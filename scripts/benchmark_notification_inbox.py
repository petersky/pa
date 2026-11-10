"""Bounded private authenticated inbox inventory; aggregate output, no delivery."""
import asyncio
from collections import Counter, defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import time
from unittest.mock import patch

import httpx
import fastapi.routing
from starlette.responses import JSONResponse

spec = importlib.util.spec_from_file_location("notification_inventory_support",
    Path(__file__).with_name("benchmark_file_routes.py"))
shared = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared)
from pa.domain.models import AgentSession, CardCreate, Membership
from pa.domain.notifications import InteractionRequest, Notification, NotificationCreate
from pa.domain.projection import CardProjection
from pa.modules import notifications as module
from pa.notifications.service import can_view_notification


async def run(root, *, population, iterations, progress):
    settings = shared.Settings(data_dir=root/"data", workspace_root=root/"workspaces",
        agent_enabled=False, telemetry_enabled=False, auth_required=True,
        primary_realm="default", subscribed_realms=["default", "engineering"])
    users = shared.UserDirectory(settings.data_dir)
    alice = users.create_user("alice", "private-fixture-password")
    bob = users.create_user("bob", "private-fixture-password")
    original_workspace = shared.WorkspaceManager.__init__
    def guarded(manager, configured, store):
        assert Path(configured.data_dir).resolve().is_relative_to(root)
        assert Path(configured.workspace_root).expanduser().resolve().is_relative_to(root)
        return original_workspace(manager, configured, store)
    with (patch.object(shared.WorkspaceManager, "__init__", guarded),
          patch.object(shared.agent_module, "_instance_agent", None),
          patch.object(shared.GitHubCredentials, "load", return_value=shared.GitHubCredentials()),
          shared.support.fixture_logging() as (warnings, console)):
        warning_phases = Counter(); warning_observations = []
        progress["phase"] = "startup"
        original_emit = warnings.emit
        def emitted(record):
            original_emit(record)
            if record.levelno >= 30:
                warning_phases[progress["phase"]] += 1
                values = getattr(record, "performance_phases", {})
                if len(warning_observations) < 16:
                    warning_observations.append({"phase": progress["phase"],
                        "operation": values.get("operation"),
                        "numeric_phases": {k: v for k, v in values.items()
                            if isinstance(v, (int, float)) and v != 0}})
        warnings.emit = emitted
        started = time.perf_counter()
        app = shared.Kernel.boot(settings=settings).build_app()
        async with app.router.lifespan_context(app):
            startup_ms = (time.perf_counter()-started)*1000
            progress["phase"] = "seed"
            ctx = app.state.ctx; store = ctx.store
            service = ctx.require_service("notifications")
            runtime = ctx.require_service("async_runtime")
            assert not service._delivery_handlers
            def seed():
                membership = ctx.require_service("membership")
                for realm in settings.subscribed_realms:
                    for user in (alice, bob):
                        membership.add_membership(Membership(realm_id=realm, principal_type="user",
                            principal_id=user.id, role="viewer"))
                card = store.create_card(CardCreate(title="Synthetic linked card", body="x"*65536))
                store.save_session(AgentSession(id="notification-fixture", agent_name="fixture",
                    status="closed", title="Synthetic linked session", principal_id=f"user:{alice.id}",
                    card_id=card.id, metrics_json={"unused": "x"*65536}))
                records = []
                for index in range(population):
                    interaction = None
                    if index % 2 == 0:
                        recorded = index % 4 == 0
                        interaction = InteractionRequest(request_id=f"synthetic-{index}", kind="structured",
                            prompt="Synthetic bounded input", allow_freeform=True,
                            response_schema={"type": "object", "properties": {"value": {"type": "string"}}},
                            continuation_mode="none", sensitive=recorded,
                            state="delivered" if recorded else "outstanding",
                            responded_at=datetime.now(UTC) if recorded else None,
                            delivered_at=datetime.now(UTC) if recorded else None,
                            response=[{"field": i, "value": "x"*20} for i in range(1000)] if recorded else None)
                    record = service.create(NotificationCreate(id=f"notice-{index:04d}",
                        realm_id="engineering" if index % 5 == 0 else "default",
                        visibility="realm" if index % 3 == 0 else "principal",
                        principal_id=f"user:{bob.id}" if index % 3 == 1 else f"user:{alice.id}",
                        type="interaction" if interaction else "general", title=f"Synthetic notice {index}",
                        body="x"*(8192 if index % 10 == 0 else 128), summary="Synthetic inventory only",
                        priority="high" if index % 4 == 0 else "normal", interaction=interaction,
                        card_id=card.id, session_id="notification-fixture", distributable=False),
                        principal_id=f"user:{alice.id}")
                    # Private initial read/resolution metadata through the ordinary validated CAS API.
                    if index % 7 == 0 or index % 11 == 0:
                        payload = record.model_dump()
                        payload.update(version=record.version+1,
                            read_at=datetime.now(UTC) if index % 7 == 0 else None,
                            resolved_at=datetime.now(UTC) if index % 11 == 0 else None)
                        record = store.save_notification(Notification.model_validate(payload),
                            principal_id=f"user:{alice.id}", instance_id=settings.instance_id,
                            expected_version=record.version)
                    records.append(record)
                expired = service.create(NotificationCreate(id="synthetic-expiring", title="Synthetic expiry",
                    type="interaction", distributable=False, expires_at=datetime.now(UTC)-timedelta(minutes=1),
                    interaction=InteractionRequest(request_id="synthetic-expiring", kind="freeform",
                        prompt="Synthetic expiry", allow_freeform=True, continuation_mode="none")),
                    principal_id=f"user:{alice.id}")
                return records, expired
            seeded = time.perf_counter(); records, expired = await asyncio.to_thread(seed)
            seed_ms = (time.perf_counter()-seeded)*1000
            samples, sizes, phases = defaultdict(list), defaultdict(list), defaultdict(list)
            statuses, counts = Counter(), Counter()
            progress.update(samples=samples, statuses=statuses)
            active = ContextVar("notification_inventory_request", default=None)
            original_conn = CardProjection._conn
            @contextmanager
            def observed_conn(projection, *args, **kwargs):
                with original_conn(projection, *args, **kwargs) as conn:
                    selected = active.get()
                    if selected:
                        def trace(sql):
                            if sql.lstrip().upper().startswith("SELECT"):
                                counts[selected+":selects"] += 1
                        conn.set_trace_callback(trace)
                    try: yield conn
                    finally:
                        if selected: conn.set_trace_callback(None)
            def observe(name, fn):
                def wrapped(*args, **kwargs):
                    wall = time.perf_counter(); cpu = time.thread_time()
                    try:
                        value = fn(*args, **kwargs)
                        selected = active.get()
                        if selected and name == "list":
                            counts[selected+":notification_rows_decoded"] += len(value[0])
                        return value
                    finally:
                        selected = active.get()
                        if selected:
                            phases[selected+":"+name+":wall_ms"].append((time.perf_counter()-wall)*1000)
                            phases[selected+":"+name+":thread_cpu_ms"].append((time.thread_time()-cpu)*1000)
                return wrapped
            expiry = []
            original_expire = service.expire_due
            async def timed_expire(**kwargs):
                start = time.perf_counter()
                try: return await original_expire(**kwargs)
                finally: expiry.append({"phase": active.get() or "warmup", "ms": round((time.perf_counter()-start)*1000, 3)})
            variants = {
                "default": ({}, 200), "page200": ({"limit": 200}, 200),
                "offset": ({"limit": 17, "offset": 17}, 200),
                "interaction": ({"type": "interaction", "limit": 200}, 200),
                "high": ({"priority": "high", "limit": 200}, 200),
                "unread": ({"unread": "true", "limit": 200}, 200),
                "read": ({"unread": "false", "limit": 200}, 200),
                "outstanding": ({"outstanding": "true", "limit": 200}, 200),
                "not_outstanding": ({"outstanding": "false", "limit": 200}, 200),
                "resolved": ({"resolved": "true", "limit": 200}, 200),
                "engineering": ({"realm": "engineering", "limit": 200}, 200),
                "unsubscribed": ({"realm": "foreign"}, 200),
                "limit_above_max": ({"limit": 201}, 422),
                "negative_offset": ({"offset": -1}, 422),
            }
            def verify(response, params, principal):
                body = response.json(); realm = params.get("realm", "default")
                visible = [r for r in records if r.realm_id == realm and
                    can_view_notification(r, principal, {"default", "engineering"})]
                outstanding_count = sum(r.outstanding for r in visible)
                def accepted(r):
                    return (all(str(getattr(r, attr)) == params[key] for key, attr in
                        (("type", "type"), ("priority", "priority")) if key in params) and
                        all(value == (params[key] == "true") for key, value in
                        (("unread", r.read_at is None), ("resolved", r.resolved_at is not None),
                         ("outstanding", r.outstanding)) if key in params))
                selected = sorted(filter(accepted, visible), key=lambda r: (-r.updated_at.timestamp(), r.id))
                offset, limit = int(params.get("offset", 0)), int(params.get("limit", 50))
                selected = selected[offset:offset+limit]
                assert [x["id"] for x in body["items"]] == [r.id for r in selected]
                assert body["offset"] == offset and body["limit"] == limit
                assert body["next_offset"] == (offset+len(selected) if len(selected) == limit else None)
                assert body["outstanding_count"] == outstanding_count
                for public, record in zip(body["items"], selected):
                    assert {k: public[k] for k in Notification.model_fields} == record.public_dict()
                    if record.card_id:
                        assert public["context"]["card"]["label"] == (
                            "Synthetic linked card" if record.realm_id == "default" else "Linked card")
                    if record.interaction and record.interaction.sensitive:
                        assert public["interaction"]["response"] is None
                        assert public["interaction"]["response_summary"] == "Sensitive response recorded"
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture",
                    headers={"Authorization": "Bearer "+alice.cli_token}) as client:
                with patch.object(service, "expire_due", timed_expire):
                    progress["phase"] = "warmup"
                    # Normal first GET expires one synthetic notice, without any delivery handler.
                    warm = await client.get("/api/notifications")
                    assert warm.status_code == 200
                    expired = service.get_authorized(expired.id, principal_id=f"user:{alice.id}", realms={"default"})
                    assert expired.resolved_at and expired.interaction.state == "expired"
                    records.append(expired)
                    for params, expected in variants.values():
                        response = await client.get("/api/notifications", params=params)
                        assert response.status_code == expected
                        if expected == 200: verify(response, params, f"user:{alice.id}")
                    before = runtime.snapshot()
                    progress["phase"] = "measurement"
                    async def background():
                        start = time.perf_counter()
                        value = await runtime.run_blocking("sqlite.agent_session_read", store.get_session, "notification-fixture")
                        samples["background_read"].append((time.perf_counter()-start)*1000)
                        assert value.id == "notification-fixture"
                    original_serialize = fastapi.routing.serialize_response
                    async def serialized(*args, **kwargs):
                        wall = time.perf_counter(); cpu = time.thread_time()
                        try: return await original_serialize(*args, **kwargs)
                        finally:
                            selected = active.get()
                            if selected:
                                phases[selected+":fastapi_serialize:wall_ms"].append((time.perf_counter()-wall)*1000)
                                phases[selected+":fastapi_serialize:thread_cpu_ms"].append((time.thread_time()-cpu)*1000)
                    with (patch.object(CardProjection, "_conn", observed_conn),
                          patch.object(service, "list_inbox", observe("list", service.list_inbox)),
                          patch.object(module, "_present_page", observe("present", module._present_page)),
                          patch.object(fastapi.routing, "serialize_response", serialized),
                          patch.object(JSONResponse, "render", observe("render", JSONResponse.render))):
                        for iteration in range(iterations):
                            for name, (params, expected) in variants.items():
                                async def request():
                                    token = active.set(name); start = time.perf_counter()
                                    try: return await client.get("/api/notifications", params=params)
                                    finally:
                                        samples[name].append((time.perf_counter()-start)*1000)
                                        active.reset(token)
                                response, _ = await asyncio.gather(request(), background())
                                statuses[name+":"+str(response.status_code)] += 1
                                sizes[name].append(len(response.content))
                                assert response.status_code == expected
                                assert len(response.content) < 8*1024*1024
                                if expected == 200: verify(response, params, f"user:{alice.id}")
                    after = runtime.snapshot()
                    progress["phase"] = "postmeasurement_auth"
                    # Same records, different authenticated principal; no private-response effect.
                    response = await client.get("/api/notifications", params={"limit": 200},
                        headers={"Authorization": "Bearer "+bob.cli_token})
                    assert response.status_code == 200
                    verify(response, {"limit": 200}, f"user:{bob.id}")
                    details = {}
                    for name, path, token, expected in (
                        ("own_detail", "/api/notifications/notice-0002", alice.cli_token, 200),
                        ("other_principal", "/api/notifications/notice-0001", alice.cli_token, 404),
                        ("missing", "/api/notifications/synthetic-missing", alice.cli_token, 404),
                        ("unauthenticated", "/api/notifications", "invalid", 401)):
                        start = time.perf_counter()
                        response = await client.get(path, headers={"Authorization": "Bearer "+token})
                        details[name] = {"status": response.status_code, "bytes": len(response.content),
                            "ms": round((time.perf_counter()-start)*1000, 3)}
                        assert response.status_code == expected
                assert not service._delivery_handlers and not service._deliveries_active
                private = store.get_notification("notice-0000", realm_id="engineering")
                assert len(private.interaction.response) == 1000
            deltas = {op: {k: v.get(k, 0)-before.get("operations", {}).get(op, {}).get(k, 0)
                for k in ("submitted", "completed", "failed", "timed_out", "cancelled", "rejected", "slow_calls")}
                for op, v in after.get("operations", {}).items()}
            deltas = {op: v for op, v in deltas.items() if any(v.values())}
            compact = shared.support.compact_runtime(after)
            compact["operations"] = {op: v for op, v in compact["operations"].items() if op in deltas}
            result = {"population": population, "iterations": iterations, "startup_ms": round(startup_ms, 3),
                "seed_ms": round(seed_ms, 3), "request_ms": {k: shared.support.distribution(v) for k, v in samples.items()},
                "response_bytes": {k: {field.removesuffix("_ms"): value for field, value in
                    shared.support.distribution(v).items()} for k, v in sizes.items()},
                "phase_ms": {k: shared.support.distribution(v) for k, v in phases.items()},
                "status_counts": dict(statuses), "select_counts": dict(counts), "expiry_passes": expiry,
                "detail_and_auth_probes": details, "operation_deltas": deltas, "runtime": compact,
                "explicit_ge500ms": {k: sum(t >= 500 for t in v) for k, v in samples.items()},
                "private_response_retained": True, "public_redaction_verified": True}
            progress["phase"] = "shutdown"
        result.update(warning_counts=dict(warnings.counts), warning_phase_counts=dict(warning_phases),
            warning_observations=warning_observations, console_characters=console.characters)
    return result


def benchmark(*, population=480, iterations=4):
    if not 30 <= population <= 480 or not 1 <= iterations <= 4:
        raise ValueError("population30..480 iterations1..4")
    result = {"contract": "pa.notification-inbox-inventory/v1"}; progress = {}
    with tempfile.TemporaryDirectory(prefix="pa-notification-inbox-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        try: result.update(asyncio.run(run(root, population=population, iterations=iterations, progress=progress)))
        except Exception as exc:
            result.update(failed=True, **shared.support.minimum_failure(exc),
                partial_status_counts=dict(progress.get("statuses", {})),
                partial_request_ms={k: shared.support.distribution(v) for k, v in progress.get("samples", {}).items()})
        result["fixture_storage"] = shared.support.fixture_bytes(root)
        assert result["fixture_storage"]["logical_bytes"] < 1024**3
    result.update(temporary_fixture_removed=not root.exists(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Private full Kernel/authenticated API; real validated synthetic creation/CAS metadata, no delivery handlers or provider/peer. Principal/realm/filter/page ID oracle plus complete public model fields, label and redaction assertions. Detail/auth probes separate from measurement. Ordinary expiry cadence retained and separately timed. Concurrent background session reads. Query trace counts SELECTs without retaining SQL and adds overhead; render/list/present phases overlap request totals. Runtime windows include boot/warmup; measurement deltas separate. Credential wrapper stub, not native credential/deployed/fleet/startup acceptance. Postworkload logical bytes not peak or physical IO.")
    assert len(json.dumps(result).encode()) <= 128*1024
    return result


if __name__ == "__main__": print(json.dumps(benchmark(), separators=(",", ":")))
