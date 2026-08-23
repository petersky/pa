from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.datastructures import MutableHeaders

from pa.config import Settings, get_settings
from pa.core.context import AppContext
from pa.core.hooks import HookBus
from pa.core.logging import configure_logging
from pa.core.registry import ModuleRegistry
from pa.domain.store import get_store

if TYPE_CHECKING:
    from pa.instance.agent_session import AgentSessionManager

logger = logging.getLogger(__name__)


class _IdentityHeadersMiddleware:
    """Pure ASGI identity headers so nested owner probes are not serialized."""

    def __init__(self, app, instance_id: str) -> None:
        self.app = app
        self.instance_id = instance_id

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        correlation_id = ""
        for key, value in scope.get("headers") or []:
            if key == b"x-request-id":
                correlation_id = value.decode("latin-1")
                break
        if not correlation_id:
            from uuid import uuid4

            correlation_id = str(uuid4())
        scope.setdefault("state", {})["correlation_id"] = correlation_id
        instance_id = self.instance_id

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers["X-PA-Instance-ID"] = instance_id
                headers["X-Request-ID"] = correlation_id
            await send(message)

        await self.app(scope, receive, send_wrapper)


class _ResponsivenessMiddleware:
    """Record request timing without BaseHTTPMiddleware hitching."""

    def __init__(self, app, runtime_getter) -> None:
        self.app = app
        self.runtime_getter = runtime_getter

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        import asyncio
        import time

        started = time.perf_counter()
        status = 500
        path = str(scope.get("path") or "")

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message.get("status") or 500)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except asyncio.CancelledError:
            from pa.server.shutdown import is_shutting_down

            if is_shutting_down():
                return
            raise
        finally:
            runtime = self.runtime_getter()
            if runtime:
                runtime.record_request(
                    path, status, (time.perf_counter() - started) * 1000
                )


class _CacheControlMiddleware:
    """Assign cache headers without BaseHTTPMiddleware hitching."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        from urllib.parse import parse_qs

        path = str(scope.get("path") or "")
        query = parse_qs((scope.get("query_string") or b"").decode("latin-1"))

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                content_type = headers.get("content-type", "")
                if path.startswith("/static/"):
                    if query.get("v"):
                        headers["Cache-Control"] = (
                            "public, max-age=31536000, immutable"
                        )
                    else:
                        headers["Cache-Control"] = "no-cache"
                elif "text/html" in content_type:
                    headers["Cache-Control"] = "no-cache, must-revalidate"
                    headers["Pragma"] = "no-cache"
                elif path.startswith("/api/"):
                    headers["Cache-Control"] = "no-store"
                headers.setdefault("Vary", "Accept")
            await send(message)

        await self.app(scope, receive, send_wrapper)


class _SyncRecoveryAdmissionMiddleware:
    """Reject ordinary mutations while canonical history is incomplete."""

    ALLOWED = frozenset(
        {
            "/api/sync/get",
            "/api/sync/have",
            "/api/sync/need",
            "/api/sync/reconcile",
            "/api/sync/recovery",
        }
    )

    def __init__(self, app, ctx: AppContext) -> None:
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope, receive, send) -> None:
        recovery = self.ctx.services.get("sync_recovery")
        method = str(scope.get("method") or "GET").upper()
        path = str(scope.get("path") or "")
        blocked = (
            scope.get("type") == "http"
            and method not in {"GET", "HEAD", "OPTIONS"}
            and recovery
            and recovery.degraded()
            and path not in self.ALLOWED
        )
        if blocked:
            from starlette.responses import JSONResponse

            response = JSONResponse(
                {
                    "detail": {
                        "code": "sync_history_recovery",
                        "message": (
                            "Mutation rejected while canonical sync history "
                            "is incomplete"
                        ),
                        "recovery": recovery.public(),
                    }
                },
                status_code=503,
                headers={"Retry-After": "10"},
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)


class _DebugRequestMiddleware:
    """Debug request hooks without BaseHTTPMiddleware hitching."""

    def __init__(self, app, ctx: AppContext) -> None:
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        import time

        start = time.perf_counter()
        method = str(scope.get("method") or "")
        path = str(scope.get("path") or "")
        status = 500

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message.get("status") or 500)
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers["X-PA-Debug"] = "1"
            await send(message)

        await self.ctx.hooks.emit("request.start", method=method, path=path)
        await self.app(scope, receive, send_wrapper)
        await self.ctx.hooks.emit(
            "request.end",
            method=method,
            path=path,
            status=status,
            elapsed_ms=(time.perf_counter() - start) * 1000,
        )


SERVER_DIR = Path(__file__).resolve().parent.parent / "server"
DEFAULT_TEMPLATES = SERVER_DIR / "templates"
DEFAULT_STATIC = SERVER_DIR / "static"


class Kernel:
    """Orchestrates module loading and application assembly."""

    def __init__(self, ctx: AppContext, registry: ModuleRegistry) -> None:
        self.ctx = ctx
        self.registry = registry

    @classmethod
    def boot(
        cls,
        *,
        settings: Settings | None = None,
        load_modules: bool = True,
        claim_writer: bool = False,
    ) -> Kernel:
        settings = settings or get_settings()
        writer_lock = None
        if claim_writer:
            from pa.core.writer_lock import DataDirWriterLock

            writer_lock = DataDirWriterLock(settings.data_dir)
            writer_lock.acquire()
        try:
            configure_logging(settings)

            hooks = HookBus()
            if settings.debug:
                hooks.enable_history(True)

            ctx = AppContext(settings=settings, hooks=hooks, store=get_store(settings))
            from pa.core.async_runtime import AsyncRuntime

            async_runtime = AsyncRuntime(
                max_workers=settings.blocking_workers,
                max_queue=settings.blocking_queue_limit,
                default_timeout=settings.blocking_default_timeout,
                slow_call_seconds=settings.blocking_slow_call_seconds,
                lag_interval_seconds=settings.event_loop_probe_interval,
            )
            ctx.register_service(
                "async_runtime",
                async_runtime,
            )
            hooks.set_async_runtime(async_runtime)
            if writer_lock:
                ctx.register_service("writer_lock", writer_lock)
            from pa.core.ui.pages import PageRegistry

            ctx.register_service("pages", PageRegistry())
            from pa.core.assets import build_asset_manifest

            ctx.register_service("assets", build_asset_manifest(DEFAULT_STATIC))
            registry = ModuleRegistry(ctx)

            if load_modules:
                registry.load_all()

            kernel = cls(ctx, registry)
            return kernel
        except BaseException:
            if writer_lock:
                writer_lock.release()
            raise

    async def startup(self, app: FastAPI) -> None:
        from pa.execution.lease import LeaseManager
        from pa.execution.router import ExecutionRouter
        from pa.fleet.registry import FleetRegistry
        from pa.instance.agent_session import get_instance_agent
        from pa.network.peer_table import PeerTable
        from pa.network.registry import PeerRegistry
        from pa.server.shutdown import shutdown_event

        shutdown_event()
        async_runtime = self.ctx.require_service("async_runtime")
        await async_runtime.start()
        agent = await async_runtime.run_blocking(
            "startup.agent_manager",
            get_instance_agent,
            self.ctx.settings,
            self.ctx.store,
            self.ctx.services.get("dispatch_store"),
            timeout=120.0,
        )
        agent.async_runtime = async_runtime
        agent.browser.async_runtime = async_runtime
        agent.notification_service = self.ctx.services.get("notifications")
        agent.assigned_mcp_environment_resolver = self.ctx.services.get(
            "assigned_mcp_environment_resolver"
        )
        import os

        from pa.instance.quiesce import consume_skip_resume

        resume_env = os.environ.get("PA_ACP_RESUME", "1").strip().lower()
        resume = resume_env not in {
            "0",
            "false",
            "no",
            "off",
        } and not consume_skip_resume(self.ctx.settings.data_dir)
        agent._accepting = False
        begin_startup = getattr(agent, "begin_startup", None)
        if callable(begin_startup):
            begin_startup()
        self.ctx.register_service("instance_agent", agent)
        lifecycle = {"phase": "starting", "error": None}
        self.ctx.register_service("agent_lifecycle", lifecycle)

        async def start_agent() -> None:
            try:
                await agent.start(resume=resume)
                complete_startup = getattr(agent, "complete_startup", None)
                if callable(complete_startup):
                    complete_startup()
                lifecycle["phase"] = "ready" if agent.connected else "idle"
            except asyncio.CancelledError:
                lifecycle["phase"] = "cancelled"
                raise
            except Exception as exc:
                complete_startup = getattr(agent, "complete_startup", None)
                if callable(complete_startup):
                    complete_startup(exc)
                lifecycle.update(phase="error", error=str(exc)[:1000])
                logger.exception("Background ACP startup failed")

        import asyncio

        agent_start_task = asyncio.create_task(start_agent(), name="pa-agent-startup")
        self.ctx.register_service("agent_start_task", agent_start_task)
        self.ctx.register_service("peer_registry", PeerRegistry(self.ctx.settings))

        event_log = self.ctx.services.get("event_log")
        if event_log:
            lease_mgr = LeaseManager(
                self.ctx.store,
                event_log,
                self.ctx.settings.instance_id,
                cloud=self.ctx.services.get("cloud_coordinator"),
            )
            self.ctx.register_service("lease_manager", lease_mgr)
            fleet: FleetRegistry = self.ctx.require_service("fleet_registry")
            peer_table: PeerTable = self.ctx.require_service("peer_table")
            users = self.ctx.require_service("users")
            router = ExecutionRouter(
                self.ctx.settings,
                lease_mgr,
                fleet,
                peer_table,
                users,
                async_runtime,
            )
            self.ctx.register_service("execution_router", router)

        app.state.kernel = self
        app.state.ctx = self.ctx

        for entry in self.registry.modules:
            await entry.module.on_startup(app, self.ctx)

        await self.ctx.hooks.emit(
            "app.startup",
            app=app,
            ctx=self.ctx,
            modules=self.registry.describe(),
        )
        from pa.server.readiness import warm_ready_contract

        warm_ready_contract(app)
        if not self.ctx.settings.agent_enabled:
            # Disabled agents still run workspace housekeeping. Await it so
            # /api/ready is deterministic for tests and `pa start` without ACP.
            await agent_start_task

    async def shutdown(self, app: FastAPI) -> None:
        import asyncio

        # Fence + cancel ACP startup before module teardown. Module on_shutdown
        # can take seconds; leaving resume running there lets connect() call
        # session/new while Uvicorn is already shutting down.
        agent_start_task = self.ctx.services.get("agent_start_task")
        agent: AgentSessionManager | None = self.ctx.services.get("instance_agent")
        if agent:
            agent._accepting = False
            agent._quiescing = True
        if agent_start_task and not agent_start_task.done():
            agent_start_task.cancel()
            try:
                await asyncio.wait_for(agent_start_task, timeout=0.5)
            except asyncio.CancelledError, asyncio.TimeoutError:
                pass

        # The server grants ten seconds for the entire lifespan shutdown. Keep
        # every component inside an independent deadline so one stuck teardown
        # cannot consume the grace period needed by the remaining components.
        async def bounded(label: str, awaitable, timeout: float) -> bool:
            try:
                await asyncio.wait_for(awaitable, timeout=timeout)
                return True
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.error("Timed out %s", label)
            except Exception:
                logger.exception("Failed %s", label)
            return False

        await bounded(
            "emitting app.shutdown hooks",
            self.ctx.hooks.emit("app.shutdown", app=app, ctx=self.ctx),
            0.25,
        )

        module_deadline = asyncio.get_running_loop().time() + 1.5
        for entry in reversed(self.registry.modules):
            remaining = module_deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                logger.error(
                    "Skipped shutdown for module %s after module drain deadline",
                    entry.module.name,
                )
                continue
            await bounded(
                f"shutting down module {entry.module.name}",
                entry.module.on_shutdown(app, self.ctx),
                min(0.4, remaining),
            )

        if agent:
            import os

            from pa.instance.quiesce import consume_skip_quiesce

            skip = consume_skip_quiesce(self.ctx.settings.data_dir)
            quiesce = (not skip) and os.environ.get(
                "PA_ACP_QUIESCE", "1"
            ).strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
            has_open_sessions = any(
                not getattr(runtime, "_closed", False)
                for runtime in agent.list_runtimes()
            )
            if quiesce and has_open_sessions:
                try:
                    await asyncio.wait_for(
                        agent.quiesce(reason="shutdown", timeout=1.0), timeout=1.5
                    )
                except Exception:
                    logger.exception("ACP quiesce during shutdown failed")
            try:
                await asyncio.wait_for(agent.stop(fast=skip), timeout=1.5)
            except asyncio.TimeoutError:
                logger.error("Timed out stopping ACP/browser runtimes")
        execution_router = self.ctx.services.get("execution_router")
        if execution_router:
            await bounded("closing execution router", execution_router.close(), 0.5)
        async_runtime = self.ctx.services.get("async_runtime")
        if async_runtime:
            await bounded("closing async runtime", async_runtime.close(), 0.5)

    def build_app(self) -> FastAPI:
        from contextlib import asynccontextmanager
        from typing import AsyncIterator

        kernel = self

        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncIterator[None]:
            from pa.core.writer_lock import DataDirWriterLock

            writer_lock = kernel.ctx.services.get("writer_lock")
            if not writer_lock:
                writer_lock = DataDirWriterLock(kernel.ctx.settings.data_dir)
                writer_lock.acquire()
                kernel.ctx.register_service("writer_lock", writer_lock)
            dispatch_store = kernel.ctx.services.get("dispatch_store")
            promote_writer = getattr(dispatch_store, "promote_writer", None)
            started = False
            try:
                if callable(promote_writer):
                    promote_writer()
                await kernel.startup(app)
                started = True
                yield
            finally:
                try:
                    if started:
                        await kernel.shutdown(app)
                    elif dispatch_store and not getattr(
                        dispatch_store, "read_only", True
                    ):
                        dispatch_store.close()
                finally:
                    writer_lock.release()

        app = FastAPI(
            title="PA",
            description="Human–agent orchestration",
            version="0.1.0",
            lifespan=lifespan,
            debug=self.ctx.settings.debug,
        )

        template_dirs = [str(DEFAULT_TEMPLATES)]
        for entry in self.registry.modules:
            template_dirs.extend(entry.module.template_dirs())

        if len(template_dirs) == 1:
            app.state.templates = Jinja2Templates(directory=template_dirs[0])
        else:
            app.state.templates = Jinja2Templates(directory=template_dirs)

        assets = self.ctx.require_service("assets")
        app.state.templates.env.globals["static_url"] = assets.url
        app.state.templates.env.globals["asset_version"] = assets.version
        from pa.core.ui.work_presentation import (
            absolute_time,
            presentation_state,
            relative_time,
        )

        app.state.templates.env.globals["relative_time"] = relative_time
        app.state.templates.env.globals["absolute_time"] = absolute_time
        app.state.templates.env.globals["presentation_state"] = presentation_state
        from pa.core.ui.instance_identity import (
            canonical_instance_identities,
            resolve_instance_identity,
        )

        app.state.templates.env.globals["instance_identity_directory"] = lambda: (
            canonical_instance_identities(self.ctx)
        )
        app.state.templates.env.globals["resolve_instance_identity"] = (
            lambda instance_id: resolve_instance_identity(self.ctx, instance_id)
        )

        from fastapi.responses import FileResponse

        @app.get("/favicon.ico", include_in_schema=False)
        async def favicon() -> FileResponse:
            return FileResponse(
                DEFAULT_STATIC / "favicon.ico",
                media_type="image/x-icon",
                headers={"Cache-Control": "public, max-age=604800"},
            )

        if DEFAULT_STATIC.exists():
            app.mount(
                "/static", StaticFiles(directory=str(DEFAULT_STATIC)), name="static"
            )

        for entry in self.registry.modules:
            for url_path, fs_path in entry.module.static_mounts():
                if Path(fs_path).exists():
                    app.mount(
                        url_path,
                        StaticFiles(directory=fs_path),
                        name=url_path.strip("/"),
                    )

        for entry in self.registry.modules:
            for prefix, router, tags in entry.module.api_routers():
                app.include_router(router, prefix=prefix, tags=tags or [])

        for entry in self.registry.modules:
            for router in entry.module.ui_routers():
                app.include_router(router)

        from pa.openapi import install_openapi_contract

        install_openapi_contract(app)
        self._install_auth_middleware(app)
        app.add_middleware(_SyncRecoveryAdmissionMiddleware, ctx=self.ctx)

        if self.ctx.settings.debug:
            self._install_debug_middleware(app)

        self._install_cache_middleware(app)
        self._install_identity_middleware(app)
        self._install_responsiveness_middleware(app)
        self._install_runtime_error_handlers(app)

        return app

    def _install_runtime_error_handlers(self, app: FastAPI) -> None:
        from fastapi.exception_handlers import request_validation_exception_handler
        from fastapi.exceptions import RequestValidationError
        from starlette.requests import Request
        from starlette.responses import JSONResponse

        from pa.core.async_runtime import (
            AsyncRuntimeClosed,
            BlockingOperationTimeout,
            BlockingQueueFull,
        )
        from pa.workloads import WorkloadProfileError

        async def overloaded(
            _request: Request, exc: BlockingQueueFull | AsyncRuntimeClosed
        ) -> JSONResponse:
            return JSONResponse(
                status_code=503,
                content={"detail": str(exc), "code": "blocking_capacity_unavailable"},
                headers={"Retry-After": "1"},
            )

        async def timed_out(
            _request: Request, exc: BlockingOperationTimeout
        ) -> JSONResponse:
            return JSONResponse(
                status_code=504,
                content={"detail": str(exc), "code": "blocking_operation_timeout"},
            )

        async def invalid_request(
            request: Request, exc: RequestValidationError
        ) -> JSONResponse:
            profile_fields = {
                "profile",
                "workload_profile",
                "allowed_profiles",
                "denied_profiles",
                "hard_denied_profiles",
                "max_concurrent_by_profile",
                "max_queued_by_profile",
                "hard_max_concurrent_by_profile",
            }
            for error in exc.errors():
                cause = (error.get("ctx") or {}).get("error")
                if isinstance(cause, WorkloadProfileError):
                    return JSONResponse(
                        status_code=422, content={"detail": cause.detail()}
                    )
                if error.get("type") == "enum" and profile_fields.intersection(
                    str(part) for part in error.get("loc", ())
                ):
                    profile_error = WorkloadProfileError(error.get("input"))
                    return JSONResponse(
                        status_code=422, content={"detail": profile_error.detail()}
                    )
            return await request_validation_exception_handler(request, exc)

        app.add_exception_handler(BlockingQueueFull, overloaded)
        app.add_exception_handler(AsyncRuntimeClosed, overloaded)
        app.add_exception_handler(BlockingOperationTimeout, timed_out)
        app.add_exception_handler(RequestValidationError, invalid_request)

    def _install_identity_middleware(self, app: FastAPI) -> None:
        app.add_middleware(
            _IdentityHeadersMiddleware,
            instance_id=self.ctx.settings.instance_id,
        )

    def _install_responsiveness_middleware(self, app: FastAPI) -> None:
        app.add_middleware(
            _ResponsivenessMiddleware,
            runtime_getter=lambda: self.ctx.services.get("async_runtime"),
        )

    def _install_auth_middleware(self, app: FastAPI) -> None:
        from pa.auth.middleware import AuthMiddleware
        from pa.auth.sessions import SessionManager
        from pa.auth.users import UserDirectory

        users = self.ctx.services.get("users")
        sessions = self.ctx.services.get("sessions")
        if not users or not sessions:
            users = UserDirectory(self.ctx.settings.data_dir)
            users.ensure_default_user()
            sessions = SessionManager(self.ctx.settings.session_secret)

        app.add_middleware(
            AuthMiddleware,
            settings=self.ctx.settings,
            users=users,
            sessions=sessions,
        )

    def register_mcp(self, mcp: Any) -> None:
        from pa.core.mcp_registration import UniqueToolRegistrationProxy

        guarded = UniqueToolRegistrationProxy(mcp)
        for entry in self.registry.modules:
            entry.module.register_mcp(guarded, self.ctx)

    def _install_debug_middleware(self, app: FastAPI) -> None:
        app.add_middleware(_DebugRequestMiddleware, ctx=self.ctx)

    def _install_cache_middleware(self, app: FastAPI) -> None:
        app.add_middleware(_CacheControlMiddleware)


_kernel: Kernel | None = None


def get_kernel() -> Kernel:
    global _kernel
    if _kernel is None:
        _kernel = Kernel.boot()
    return _kernel


def reset_kernel() -> None:
    global _kernel
    _kernel = None
