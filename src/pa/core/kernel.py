from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pa.config import Settings, get_settings
from pa.core.context import AppContext
from pa.core.hooks import HookBus
from pa.core.logging import configure_logging
from pa.core.registry import ModuleRegistry
from pa.domain.store import get_store

if TYPE_CHECKING:
    from pa.instance.agent_session import AgentSessionManager

logger = logging.getLogger(__name__)

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

            ctx = AppContext(settings=settings, hooks=hooks, store=get_store())
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

        agent = get_instance_agent(self.ctx.settings, self.ctx.store)
        import os

        resume_env = os.environ.get("PA_ACP_RESUME", "1").strip().lower()
        resume = resume_env not in {"0", "false", "no", "off"}
        await agent.start(resume=resume)
        self.ctx.register_service("instance_agent", agent)
        self.ctx.register_service("peer_registry", PeerRegistry(self.ctx.settings))

        event_log = self.ctx.services.get("event_log")
        if event_log:
            lease_mgr = LeaseManager(
                self.ctx.store, event_log, self.ctx.settings.instance_id
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

    async def shutdown(self, app: FastAPI) -> None:
        import asyncio

        await self.ctx.hooks.emit("app.shutdown", app=app, ctx=self.ctx)

        for entry in reversed(self.registry.modules):
            try:
                await asyncio.wait_for(
                    entry.module.on_shutdown(app, self.ctx), timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.error("Timed out shutting down module %s", entry.module.name)

        agent: AgentSessionManager | None = self.ctx.services.get("instance_agent")
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
                        agent.quiesce(reason="shutdown", timeout=20.0), timeout=25.0
                    )
                except Exception:
                    logger.exception("ACP quiesce during shutdown failed")
            try:
                await asyncio.wait_for(agent.stop(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.error("Timed out stopping ACP/browser runtimes")

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
            started = False
            try:
                await kernel.startup(app)
                started = True
                yield
            finally:
                if started:
                    await kernel.shutdown(app)
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

        self._install_auth_middleware(app)

        if self.ctx.settings.debug:
            self._install_debug_middleware(app)

        self._install_cache_middleware(app)

        return app

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
        for entry in self.registry.modules:
            entry.module.register_mcp(mcp, self.ctx)

    def _install_debug_middleware(self, app: FastAPI) -> None:
        import time

        from starlette.requests import Request
        from starlette.responses import Response

        @app.middleware("http")
        async def debug_request_logger(request: Request, call_next) -> Response:
            start = time.perf_counter()
            await self.ctx.hooks.emit(
                "request.start",
                method=request.method,
                path=request.url.path,
            )
            response = await call_next(request)
            elapsed_ms = (time.perf_counter() - start) * 1000
            await self.ctx.hooks.emit(
                "request.end",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                elapsed_ms=elapsed_ms,
            )
            response.headers["X-PA-Debug"] = "1"
            return response

    def _install_cache_middleware(self, app: FastAPI) -> None:
        from starlette.requests import Request
        from starlette.responses import Response

        @app.middleware("http")
        async def cache_control(request: Request, call_next) -> Response:
            response = await call_next(request)
            path = request.url.path
            content_type = response.headers.get("content-type", "")

            if path.startswith("/static/"):
                if request.query_params.get("v"):
                    response.headers["Cache-Control"] = (
                        "public, max-age=31536000, immutable"
                    )
                else:
                    response.headers["Cache-Control"] = "no-cache"
            elif "text/html" in content_type:
                response.headers["Cache-Control"] = "no-cache, must-revalidate"
                response.headers["Pragma"] = "no-cache"
            elif path.startswith("/api/"):
                response.headers["Cache-Control"] = "no-store"

            response.headers.setdefault("Vary", "Accept")
            return response


_kernel: Kernel | None = None


def get_kernel() -> Kernel:
    global _kernel
    if _kernel is None:
        _kernel = Kernel.boot()
    return _kernel


def reset_kernel() -> None:
    global _kernel
    _kernel = None
