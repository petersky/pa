"""HTTP serving, bind, and local sync health for CLI status and doctor."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from pa.config import Settings
from pa.core.context import AppContext
from pa.server.listeners import ListenerSpec, parse_listener, web_listener_specs

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
WILDCARD_HOSTS = {"0.0.0.0", "::", ""}
DEFAULT_HEALTH_TIMEOUT = 3.0


@dataclass(frozen=True)
class HealthProbe:
    url: str
    ok: bool
    elapsed_ms: float
    status_code: int | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "ok": self.ok,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "status_code": self.status_code,
            "error": self.error,
        }


@dataclass(frozen=True)
class BindReport:
    listeners: tuple[tuple[str, int], ...]
    binds_loopback: bool
    binds_non_loopback: bool
    mode: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "listeners": [
                {"host": host, "port": port} for host, port in self.listeners
            ],
            "binds_loopback": self.binds_loopback,
            "binds_non_loopback": self.binds_non_loopback,
            "mode": self.mode,
        }


@dataclass(frozen=True)
class ServingDiagnosis:
    service_running: bool
    bind: BindReport
    advertised_url: str | None
    loopback_url: str
    loopback: HealthProbe | None
    advertised: HealthProbe | None
    serving: str
    health_ok: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "service_running": self.service_running,
            "bind": self.bind.as_dict(),
            "advertised_url": self.advertised_url,
            "loopback_url": self.loopback_url,
            "loopback": self.loopback.as_dict() if self.loopback else None,
            "advertised": self.advertised.as_dict() if self.advertised else None,
            "serving": self.serving,
            "health_ok": self.health_ok,
        }


@dataclass(frozen=True)
class SyncDiagnosis:
    realm_id: str
    head: str | None
    projection_head: str | None
    consistent: bool
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "realm_id": self.realm_id,
            "head": self.head,
            "projection_head": self.projection_head,
            "consistent": self.consistent,
            "error": self.error,
        }


def loopback_base_url(settings: Settings) -> str:
    return f"http://127.0.0.1:{settings.port}"


def advertised_base_url(settings: Settings) -> str | None:
    raw = (settings.instance_url or "").strip().rstrip("/")
    if raw:
        return raw
    host = (settings.host or "").strip()
    if host and host.lower() not in LOOPBACK_HOSTS and host not in WILDCARD_HOSTS:
        return f"http://{host}:{settings.port}"
    return None


def _normalize_host(host: str) -> str:
    value = host.strip().lower()
    if value.startswith("[") and value.endswith("]"):
        return value[1:-1]
    return value


def _url_host(url: str) -> str:
    parsed = urlparse(url)
    return _normalize_host(parsed.hostname or "")


def _same_base(left: str, right: str | None) -> bool:
    if not right:
        return False
    left_parsed, right_parsed = urlparse(left), urlparse(right)
    return (
        (left_parsed.scheme or "http") == (right_parsed.scheme or "http")
        and _normalize_host(left_parsed.hostname or "")
        == _normalize_host(right_parsed.hostname or "")
        and (left_parsed.port or _url_port(left_parsed))
        == (right_parsed.port or _url_port(right_parsed))
    )


def _url_port(parsed: Any) -> int:
    if parsed.port:
        return parsed.port
    return 443 if parsed.scheme == "https" else 80


def listeners_from_environment(
    settings: Settings, environment: Mapping[str, str] | None = None
) -> list[ListenerSpec]:
    if environment:
        raw_listeners = (environment.get("PA_WEB_LISTENERS") or "").strip()
        if raw_listeners and raw_listeners not in {"[]", ""}:
            try:
                values = json.loads(raw_listeners)
            except (TypeError, ValueError):
                values = [item.strip() for item in raw_listeners.split(",") if item.strip()]
            if isinstance(values, list) and values:
                port = int(environment.get("PA_PORT") or settings.port)
                return [parse_listener(str(value), port) for value in values]
        if "PA_HOST" in environment:
            host = environment.get("PA_HOST") or ""
            port = int(environment.get("PA_PORT") or settings.port)
            return [parse_listener(host, port)]
    return web_listener_specs(settings)


def classify_bind(
    settings: Settings, environment: Mapping[str, str] | None = None
) -> BindReport:
    specs = listeners_from_environment(settings, environment)
    binds_loopback = False
    binds_non_loopback = False
    for spec in specs:
        host = _normalize_host(spec.host)
        if host in WILDCARD_HOSTS:
            binds_loopback = True
            binds_non_loopback = True
        elif host in LOOPBACK_HOSTS:
            binds_loopback = True
        else:
            binds_non_loopback = True
    if binds_loopback and binds_non_loopback:
        wildcard = any(_normalize_host(spec.host) in WILDCARD_HOSTS for spec in specs)
        mode = "wildcard" if wildcard else "mixed"
    elif binds_loopback:
        mode = "loopback"
    elif binds_non_loopback:
        mode = "specific"
    else:
        mode = "none"
    return BindReport(
        listeners=tuple((spec.host, spec.port) for spec in specs),
        binds_loopback=binds_loopback,
        binds_non_loopback=binds_non_loopback,
        mode=mode,
    )


def advertised_is_remote(settings: Settings) -> bool:
    url = advertised_base_url(settings)
    if not url:
        return False
    host = _url_host(url)
    return bool(host) and host not in LOOPBACK_HOSTS and host not in WILDCARD_HOSTS


def probe_health(
    url: str, *, token: str = "", timeout: float = DEFAULT_HEALTH_TIMEOUT
) -> HealthProbe:
    health_url = f"{url.rstrip('/')}/api/health"
    started = time.perf_counter()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(health_url, headers=headers)
        elapsed_ms = (time.perf_counter() - started) * 1000
        if response.status_code == 200:
            return HealthProbe(health_url, True, elapsed_ms, 200, None)
        return HealthProbe(health_url, False, elapsed_ms, response.status_code, "http")
    except httpx.TimeoutException:
        elapsed_ms = (time.perf_counter() - started) * 1000
        return HealthProbe(health_url, False, elapsed_ms, None, "timeout")
    except httpx.ConnectError as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000
        text = str(exc).lower()
        error = (
            "refused"
            if "refus" in text or "errno 61" in text or "errno 111" in text
            else "unreachable"
        )
        return HealthProbe(health_url, False, elapsed_ms, None, error)
    except httpx.HTTPError:
        elapsed_ms = (time.perf_counter() - started) * 1000
        return HealthProbe(health_url, False, elapsed_ms, None, "unreachable")


def _classify_serving(
    *,
    service_running: bool,
    bind: BindReport,
    remote: bool,
    loopback: HealthProbe | None,
    advertised: HealthProbe | None,
) -> str:
    if not service_running and not (loopback and loopback.ok) and not (
        advertised and advertised.ok
    ):
        return "stopped"
    loopback_ok = bool(loopback and loopback.ok)
    advertised_ok = bool(advertised and advertised.ok)
    if loopback_ok and (advertised is None or advertised_ok):
        return "ok"
    if loopback_ok and advertised and not advertised_ok:
        if advertised.error == "refused" or bind.mode == "loopback":
            return "loopback_only"
        if advertised.error == "timeout":
            return "timeout"
        return "advertised_unreachable"
    if advertised_ok and not loopback_ok:
        return "no_loopback"
    errors = [
        probe.error
        for probe in (loopback, advertised)
        if probe is not None and not probe.ok
    ]
    if "timeout" in errors:
        return "timeout"
    if errors and all(error == "refused" for error in errors):
        return "refused"
    if remote and bind.mode == "loopback":
        return "loopback_only"
    if not bind.binds_loopback and bind.mode == "specific":
        return "no_loopback"
    return "unreachable"


def diagnose_serving(
    settings: Settings,
    *,
    service_running: bool,
    token: str = "",
    environment: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_HEALTH_TIMEOUT,
    probe: bool = True,
) -> ServingDiagnosis:
    bind = classify_bind(settings, environment)
    loopback_url = loopback_base_url(settings)
    advertised_url = advertised_base_url(settings)
    if advertised_url and _same_base(loopback_url, advertised_url):
        advertised_url = None
    loopback = None
    advertised = None
    if probe:
        jobs: list[tuple[str, str]] = [("loopback", loopback_url)]
        if advertised_url:
            jobs.append(("advertised", advertised_url))
        results: dict[str, HealthProbe] = {}
        if len(jobs) == 1:
            results[jobs[0][0]] = probe_health(
                jobs[0][1], token=token, timeout=timeout
            )
        else:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {
                    name: pool.submit(probe_health, url, token=token, timeout=timeout)
                    for name, url in jobs
                }
                results = {name: future.result() for name, future in futures.items()}
        loopback = results.get("loopback")
        advertised = results.get("advertised")
    serving = _classify_serving(
        service_running=service_running,
        bind=bind,
        remote=advertised_is_remote(settings),
        loopback=loopback,
        advertised=advertised,
    )
    health_ok = serving == "ok"
    return ServingDiagnosis(
        service_running=service_running,
        bind=bind,
        advertised_url=advertised_url,
        loopback_url=loopback_url,
        loopback=loopback,
        advertised=advertised,
        serving=serving,
        health_ok=health_ok,
    )


def diagnose_sync(settings: Settings) -> SyncDiagnosis:
    from pa.sync.infrastructure import get_event_log

    realm = settings.primary_realm
    try:
        head = get_event_log(settings).get_head(realm)
        projection = None
        if settings.db_path.exists():
            try:
                conn = sqlite3.connect(
                    f"file:{settings.db_path}?mode=ro", uri=True, timeout=5
                )
            except sqlite3.OperationalError:
                conn = sqlite3.connect(settings.db_path, timeout=5)
            try:
                row = conn.execute(
                    "SELECT head_hash FROM sync_projection_heads WHERE realm_id = ?",
                    (realm,),
                ).fetchone()
                projection = row[0] if row else None
            finally:
                conn.close()
        return SyncDiagnosis(realm, head, projection, head == projection)
    except Exception as exc:
        return SyncDiagnosis(realm, None, None, False, error=str(exc))


def sync_from_context(ctx: AppContext) -> SyncDiagnosis:
    realm = ctx.settings.primary_realm
    log = ctx.services.get("event_log") or getattr(ctx.store, "event_log", None)
    try:
        head = log.get_head(realm) if log else None
        projection = ctx.store.get_projection_head(realm)
        return SyncDiagnosis(realm, head, projection, head == projection)
    except Exception as exc:
        return SyncDiagnosis(realm, None, None, False, error=str(exc))


def format_probe(probe: HealthProbe | None, *, missing: str = "not probed") -> str:
    if probe is None:
        return missing
    if probe.ok:
        return f"ok ({probe.elapsed_ms:.0f}ms)"
    if probe.error == "timeout":
        return f"timeout ({probe.elapsed_ms:.0f}ms)"
    if probe.error == "http":
        return f"HTTP {probe.status_code}"
    return probe.error or "unreachable"


def format_serving_line(diagnosis: ServingDiagnosis) -> str:
    serving = diagnosis.serving
    if serving == "ok":
        return f"ok — {format_probe(diagnosis.loopback or diagnosis.advertised)}"
    if serving == "stopped":
        return "not serving (service stopped)"
    if serving == "loopback_only":
        return (
            "loopback only — process is bound to 127.0.0.1; "
            f"advertised {format_probe(diagnosis.advertised)}"
        )
    if serving == "no_loopback":
        return (
            "no loopback — process is not listening on 127.0.0.1; "
            f"advertised {format_probe(diagnosis.advertised)}"
        )
    if serving == "timeout":
        return (
            "timeout — /api/health did not respond in time "
            f"(loopback {format_probe(diagnosis.loopback)}; "
            f"advertised {format_probe(diagnosis.advertised)})"
        )
    if serving == "refused":
        return "not serving — connection refused"
    return f"{serving} — /api/health unreachable"
