from __future__ import annotations

import os
import platform
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

from pa.telemetry.models import (
    Metric,
    MetricQuality,
    TelemetrySample,
    unavailable,
    unsupported,
)


def _metric(value: float, unit: str, source: str) -> Metric:
    return Metric(
        value=value,
        unit=unit,
        quality=MetricQuality.MEASURED,
        source=source,
    )


def _rate(current: int, previous: int | None, elapsed: float) -> float | None:
    if previous is None or elapsed <= 0 or current < previous:
        return None
    return (current - previous) / elapsed


def _counter_metric(
    current: int,
    previous: int | None,
    elapsed: float,
    unit: str,
    source: str,
) -> Metric:
    value = _rate(current, previous, elapsed)
    if value is None:
        return unavailable(unit, source, "counter is warming up or reset")
    return _metric(value, unit, source)


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    create_time: float


@dataclass
class SessionTarget:
    session_id: str
    root_pid: int
    provider_id: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    realm_id: str | None = None
    principal_id: str | None = None


class ResourceCollector:
    """Cross-platform collector with stateful counter normalization.

    No process name, command line, environment, file path, socket peer, or
    payload is inspected. Per-session ownership is based on an observed provider
    root plus exact PID/create-time descendants; previously observed descendants
    survive reparenting without making PID reuse ambiguous.
    """

    def __init__(
        self,
        *,
        instance_id: str,
        instance_name: str,
        database_path: Path,
        pa_pid: int | None = None,
    ) -> None:
        self.instance_id = instance_id
        self.instance_name = instance_name
        self.database_path = database_path
        self.pa_pid = pa_pid or os.getpid()
        self.platform = platform.system().lower()
        self._last_at: float | None = None
        self._cpu_times: Any = None
        self._disk: Any = None
        self._net: Any = None
        self._pa_cpu: float | None = None
        self._pa_io: tuple[int, int] | None = None
        self._process_cpu: dict[ProcessIdentity, float] = {}
        self._process_io: dict[ProcessIdentity, tuple[int, int]] = {}
        self._owned: dict[str, set[ProcessIdentity]] = {}
        self._roots: dict[str, ProcessIdentity] = {}

    @staticmethod
    def _cpu_total(times: Any) -> tuple[float, float]:
        values = [
            float(value)
            for key, value in times._asdict().items()
            if key not in {"guest", "guest_nice"}
        ]
        total = sum(values)
        idle = float(getattr(times, "idle", 0)) + float(getattr(times, "iowait", 0))
        return total, idle

    def _system_cpu(self, metrics: dict[str, Metric], cpu_times: Any) -> None:
        cores = psutil.cpu_count(logical=True) or 1
        metrics["cpu.logical_cores"] = _metric(cores, "cores", "psutil")
        try:
            physical = psutil.cpu_count(logical=False)
            metrics["cpu.physical_cores"] = (
                _metric(physical, "cores", "psutil")
                if physical
                else unavailable("cores", "psutil", "physical core count unavailable")
            )
        except OSError, RuntimeError, ValueError:
            metrics["cpu.physical_cores"] = unavailable(
                "cores", "psutil", "physical core count unavailable"
            )
        current_total, current_idle = self._cpu_total(cpu_times)
        if self._cpu_times is None:
            metrics["cpu.utilization"] = unavailable(
                "percent", "host_cpu_times", "counter is warming up"
            )
        else:
            previous_total, previous_idle = self._cpu_total(self._cpu_times)
            delta = current_total - previous_total
            idle_delta = current_idle - previous_idle
            if delta > 0:
                percent = max(0.0, min(100.0, (1 - idle_delta / delta) * 100))
                metrics["cpu.utilization"] = _metric(
                    percent, "percent", "host_cpu_times"
                )
            else:
                metrics["cpu.utilization"] = unavailable(
                    "percent", "host_cpu_times", "CPU counter reset"
                )
        try:
            load1, load5, load15 = os.getloadavg()
            metrics["cpu.load_1m"] = _metric(load1, "load", "os.getloadavg")
            metrics["cpu.load_5m"] = _metric(load5, "load", "os.getloadavg")
            metrics["cpu.load_15m"] = _metric(load15, "load", "os.getloadavg")
            metrics["cpu.available_capacity"] = Metric(
                value=max(0.0, cores - load1),
                unit="cores",
                quality=MetricQuality.ESTIMATED,
                source="load_1m",
                detail="logical cores minus one-minute runnable load",
            )
        except AttributeError, OSError:
            for key in ("cpu.load_1m", "cpu.load_5m", "cpu.load_15m"):
                metrics[key] = unsupported("load", "os", "load averages unsupported")
            metrics["cpu.available_capacity"] = unsupported(
                "cores", "os", "load averages unsupported"
            )

    @staticmethod
    def _memory(metrics: dict[str, Metric]) -> None:
        virtual = psutil.virtual_memory()
        swap = psutil.swap_memory()
        for name in ("total", "available", "used"):
            metrics[f"memory.{name}"] = _metric(
                int(getattr(virtual, name)), "bytes", "psutil.virtual_memory"
            )
        metrics["memory.utilization"] = _metric(
            float(virtual.percent), "percent", "psutil.virtual_memory"
        )
        metrics["swap.total"] = _metric(int(swap.total), "bytes", "psutil.swap_memory")
        metrics["swap.used"] = _metric(int(swap.used), "bytes", "psutil.swap_memory")
        metrics["swap.utilization"] = _metric(
            float(swap.percent), "percent", "psutil.swap_memory"
        )

    def _disk_metrics(
        self, metrics: dict[str, Metric], disk: Any, elapsed: float
    ) -> None:
        usage = psutil.disk_usage(str(self.database_path.parent))
        metrics["disk.capacity"] = _metric(usage.total, "bytes", "psutil.disk_usage")
        metrics["disk.free"] = _metric(usage.free, "bytes", "psutil.disk_usage")
        metrics["disk.utilization"] = _metric(
            usage.percent, "percent", "psutil.disk_usage"
        )
        source = "psutil.disk_io_counters"
        if not disk:
            for key, unit in (
                ("disk.read_throughput", "bytes/second"),
                ("disk.write_throughput", "bytes/second"),
                ("disk.read_iops", "operations/second"),
                ("disk.write_iops", "operations/second"),
                ("disk.latency", "milliseconds/operation"),
            ):
                metrics[key] = unavailable(
                    unit, source, "host disk counters unavailable"
                )
            return
        previous = self._disk
        metrics["disk.read_throughput"] = _counter_metric(
            disk.read_bytes,
            getattr(previous, "read_bytes", None),
            elapsed,
            "bytes/second",
            source,
        )
        metrics["disk.write_throughput"] = _counter_metric(
            disk.write_bytes,
            getattr(previous, "write_bytes", None),
            elapsed,
            "bytes/second",
            source,
        )
        metrics["disk.read_iops"] = _counter_metric(
            disk.read_count,
            getattr(previous, "read_count", None),
            elapsed,
            "operations/second",
            source,
        )
        metrics["disk.write_iops"] = _counter_metric(
            disk.write_count,
            getattr(previous, "write_count", None),
            elapsed,
            "operations/second",
            source,
        )
        ops = (
            disk.read_count
            + disk.write_count
            - (
                (getattr(previous, "read_count", disk.read_count))
                + (getattr(previous, "write_count", disk.write_count))
            )
        )
        busy_ms = (
            getattr(disk, "read_time", 0)
            + getattr(disk, "write_time", 0)
            - (
                getattr(previous, "read_time", getattr(disk, "read_time", 0))
                + getattr(previous, "write_time", getattr(disk, "write_time", 0))
            )
        )
        metrics["disk.latency"] = (
            Metric(
                value=max(0.0, busy_ms / ops),
                unit="milliseconds/operation",
                quality=MetricQuality.ESTIMATED,
                source=source,
                detail="aggregate device service time divided by completed operations",
            )
            if previous is not None and ops > 0 and busy_ms >= 0
            else unavailable(
                "milliseconds/operation", source, "no completed operations in interval"
            )
        )

    def _network_metrics(
        self, metrics: dict[str, Metric], net: Any, elapsed: float
    ) -> None:
        source = "psutil.net_io_counters"
        if not net:
            for key, unit in (
                ("network.ingress", "bytes/second"),
                ("network.egress", "bytes/second"),
                ("network.errors", "errors"),
                ("network.drops", "packets"),
            ):
                metrics[key] = unavailable(unit, source, "network counters unavailable")
            return
        previous = self._net
        metrics["network.ingress"] = _counter_metric(
            net.bytes_recv,
            getattr(previous, "bytes_recv", None),
            elapsed,
            "bytes/second",
            source,
        )
        metrics["network.egress"] = _counter_metric(
            net.bytes_sent,
            getattr(previous, "bytes_sent", None),
            elapsed,
            "bytes/second",
            source,
        )
        metrics["network.errors"] = _metric(net.errin + net.errout, "errors", source)
        metrics["network.drops"] = _metric(net.dropin + net.dropout, "packets", source)
        try:
            connections = psutil.net_connections(kind="inet")
            metrics["network.connections"] = _metric(
                len(connections), "connections", "psutil.net_connections"
            )
        except psutil.AccessDenied, OSError, RuntimeError:
            metrics["network.connections"] = unavailable(
                "connections", "psutil.net_connections", "permission denied"
            )

    def _pa_process(self, metrics: dict[str, Metric], elapsed: float) -> None:
        source = "psutil.Process(self)"
        try:
            process = psutil.Process(self.pa_pid)
            memory = process.memory_info()
            times = process.cpu_times()
            cpu_total = float(times.user + times.system)
            cpu_rate = _rate(
                int(cpu_total * 1_000_000),
                int(self._pa_cpu * 1_000_000) if self._pa_cpu is not None else None,
                elapsed,
            )
            metrics["pa.cpu"] = (
                _metric(cpu_rate / 10_000, "percent", source)
                if cpu_rate is not None
                else unavailable("percent", source, "counter is warming up")
            )
            metrics["pa.memory_rss"] = _metric(memory.rss, "bytes", source)
            metrics["pa.memory_virtual"] = _metric(memory.vms, "bytes", source)
            metrics["pa.threads"] = _metric(process.num_threads(), "threads", source)
            try:
                io = process.io_counters()
                previous = self._pa_io
                metrics["pa.disk_read"] = _counter_metric(
                    io.read_bytes,
                    previous[0] if previous else None,
                    elapsed,
                    "bytes/second",
                    source,
                )
                metrics["pa.disk_write"] = _counter_metric(
                    io.write_bytes,
                    previous[1] if previous else None,
                    elapsed,
                    "bytes/second",
                    source,
                )
                self._pa_io = (io.read_bytes, io.write_bytes)
            except psutil.AccessDenied, AttributeError, OSError:
                metrics["pa.disk_read"] = unavailable(
                    "bytes/second", source, "process I/O counters unavailable"
                )
                metrics["pa.disk_write"] = unavailable(
                    "bytes/second", source, "process I/O counters unavailable"
                )
            self._pa_cpu = cpu_total
        except psutil.NoSuchProcess, psutil.AccessDenied, OSError:
            for key, unit in (
                ("pa.cpu", "percent"),
                ("pa.memory_rss", "bytes"),
                ("pa.memory_virtual", "bytes"),
                ("pa.threads", "threads"),
                ("pa.disk_read", "bytes/second"),
                ("pa.disk_write", "bytes/second"),
            ):
                metrics[key] = unavailable(unit, source, "PA process unavailable")

    @staticmethod
    def _identity(process: psutil.Process) -> ProcessIdentity | None:
        try:
            return ProcessIdentity(process.pid, process.create_time())
        except psutil.NoSuchProcess, psutil.AccessDenied, OSError:
            return None

    def _owned_processes(
        self, target: SessionTarget
    ) -> tuple[list[psutil.Process], str]:
        try:
            root = psutil.Process(target.root_pid)
            root_identity = self._identity(root)
            if root_identity is None:
                raise psutil.NoSuchProcess(target.root_pid)
            previous_root = self._roots.get(target.session_id)
            if (
                previous_root
                and previous_root.pid == root_identity.pid
                and previous_root != root_identity
            ):
                # A stale provider handle must never claim a different process
                # that reused the same numeric PID.
                candidates = []
                root_identity = None
            else:
                candidates = [root, *root.children(recursive=True)]
                self._roots[target.session_id] = root_identity
        except psutil.NoSuchProcess, psutil.AccessDenied, OSError:
            candidates = []
            root_identity = None

        identities: dict[ProcessIdentity, psutil.Process] = {}
        for process in candidates:
            identity = self._identity(process)
            if identity:
                identities[identity] = process

        # Preserve exact, previously observed descendants that became orphans.
        for identity in self._owned.get(target.session_id, set()):
            if identity in identities:
                continue
            try:
                process = psutil.Process(identity.pid)
                if self._identity(process) == identity:
                    identities[identity] = process
            except psutil.NoSuchProcess, psutil.AccessDenied, OSError:
                continue

        self._owned[target.session_id] = set(identities)
        ownership = (
            "verified_root_and_process_tree"
            if root_identity and identities
            else "root_exited_retained_exact_descendants"
            if identities
            else "unavailable"
        )
        return list(identities.values()), ownership

    def _session_sample(
        self,
        target: SessionTarget,
        *,
        restart_id: str,
        timestamp: datetime,
        elapsed: float,
    ) -> TelemetrySample:
        metrics: dict[str, Metric] = {}
        processes, ownership = self._owned_processes(target)
        rss = vms = threads = 0
        cpu_rate_total = 0.0
        cpu_ready = False
        read_rate = write_rate = 0.0
        read_ready = write_ready = False
        active_identities: set[ProcessIdentity] = set()
        readable_processes = io_processes = 0
        for process in processes:
            identity = self._identity(process)
            if not identity:
                continue
            active_identities.add(identity)
            try:
                memory = process.memory_info()
                times = process.cpu_times()
                rss += memory.rss
                vms += memory.vms
                threads += process.num_threads()
                readable_processes += 1
                cpu_total = float(times.user + times.system)
                previous_cpu = self._process_cpu.get(identity)
                if (
                    previous_cpu is not None
                    and elapsed > 0
                    and cpu_total >= previous_cpu
                ):
                    cpu_rate_total += (cpu_total - previous_cpu) / elapsed * 100
                    cpu_ready = True
                self._process_cpu[identity] = cpu_total
                try:
                    io = process.io_counters()
                    previous_io = self._process_io.get(identity)
                    if previous_io and elapsed > 0:
                        if io.read_bytes >= previous_io[0]:
                            read_rate += (io.read_bytes - previous_io[0]) / elapsed
                            read_ready = True
                        if io.write_bytes >= previous_io[1]:
                            write_rate += (io.write_bytes - previous_io[1]) / elapsed
                            write_ready = True
                    self._process_io[identity] = (io.read_bytes, io.write_bytes)
                    io_processes += 1
                except psutil.AccessDenied, AttributeError, OSError:
                    pass
            except psutil.NoSuchProcess, psutil.AccessDenied, OSError:
                continue

        source = "PA-owned PID/create-time process tree"
        if ownership == "unavailable":
            metrics["session.processes"] = unavailable(
                "processes", source, "provider root and owned descendants unavailable"
            )
            metrics["session.tasks"] = unavailable(
                "threads", source, "provider root and owned descendants unavailable"
            )
            metrics["session.memory_rss"] = unavailable(
                "bytes", source, "provider root and owned descendants unavailable"
            )
            metrics["session.memory_virtual"] = unavailable(
                "bytes", source, "provider root and owned descendants unavailable"
            )
        else:
            metrics["session.processes"] = _metric(
                len(active_identities), "processes", source
            )
            partial = readable_processes < len(active_identities)
            detail = "some owned processes could not be inspected" if partial else None
            quality = MetricQuality.ESTIMATED if partial else MetricQuality.MEASURED
            for name, value, unit in (
                ("session.tasks", threads, "threads"),
                ("session.memory_rss", rss, "bytes"),
                ("session.memory_virtual", vms, "bytes"),
            ):
                metrics[name] = Metric(
                    value=value,
                    unit=unit,
                    quality=quality,
                    source=source,
                    detail=detail,
                )
        metrics["session.cpu"] = (
            Metric(
                value=cpu_rate_total,
                unit="percent_of_one_core",
                quality=(
                    MetricQuality.MEASURED
                    if readable_processes == len(active_identities)
                    else MetricQuality.ESTIMATED
                ),
                source=source,
                detail=(
                    None
                    if readable_processes == len(active_identities)
                    else "some owned processes could not be inspected"
                ),
            )
            if cpu_ready
            else unavailable("percent_of_one_core", source, "counter is warming up")
        )
        metrics["session.disk_read"] = (
            Metric(
                value=read_rate,
                unit="bytes/second",
                quality=(
                    MetricQuality.MEASURED
                    if io_processes == len(active_identities)
                    else MetricQuality.ESTIMATED
                ),
                source=source,
                detail=(
                    None
                    if io_processes == len(active_identities)
                    else "some owned process I/O counters were unavailable"
                ),
            )
            if read_ready
            else unavailable(
                "bytes/second", source, "process I/O unsupported or warming up"
            )
        )
        metrics["session.disk_write"] = (
            Metric(
                value=write_rate,
                unit="bytes/second",
                quality=(
                    MetricQuality.MEASURED
                    if io_processes == len(active_identities)
                    else MetricQuality.ESTIMATED
                ),
                source=source,
                detail=(
                    None
                    if io_processes == len(active_identities)
                    else "some owned process I/O counters were unavailable"
                ),
            )
            if write_ready
            else unavailable(
                "bytes/second", source, "process I/O unsupported or warming up"
            )
        )
        metrics["session.network_ingress"] = unsupported(
            "bytes/second",
            "operating_system",
            "portable process-tree network attribution is not technically sound",
        )
        metrics["session.network_egress"] = unsupported(
            "bytes/second",
            "operating_system",
            "portable process-tree network attribution is not technically sound",
        )
        return TelemetrySample(
            timestamp=timestamp,
            instance_id=self.instance_id,
            instance_name=self.instance_name,
            scope_type="session",
            scope_id=target.session_id,
            restart_id=restart_id,
            provider_id=target.provider_id,
            card_id=target.card_id,
            project_id=target.project_id,
            realm_id=target.realm_id,
            principal_id=target.principal_id,
            root_pid=target.root_pid,
            ownership=ownership,
            metrics=metrics,
        )

    def collect(
        self, *, restart_id: str, sessions: Iterable[SessionTarget] = ()
    ) -> list[TelemetrySample]:
        sessions = list(sessions)
        started = time.monotonic()
        timestamp = datetime.now(UTC)
        now = time.monotonic()
        elapsed = max(0.001, now - self._last_at) if self._last_at else 0
        metrics: dict[str, Metric] = {}
        cpu_times = psutil.cpu_times()
        disk = psutil.disk_io_counters()
        net = psutil.net_io_counters()
        self._system_cpu(metrics, cpu_times)
        self._memory(metrics)
        self._disk_metrics(metrics, disk, elapsed)
        self._network_metrics(metrics, net, elapsed)
        self._pa_process(metrics, elapsed)
        samples = [
            TelemetrySample(
                timestamp=timestamp,
                instance_id=self.instance_id,
                instance_name=self.instance_name,
                scope_type="instance",
                scope_id=self.instance_id,
                restart_id=restart_id,
                ownership="host",
                metrics=metrics,
            )
        ]
        samples.extend(
            self._session_sample(
                target,
                restart_id=restart_id,
                timestamp=timestamp,
                elapsed=elapsed,
            )
            for target in sessions
        )
        duration = (time.monotonic() - started) * 1000
        for sample in samples:
            sample.collection_duration_ms = duration
        self._last_at = now
        self._cpu_times = cpu_times
        self._disk = disk
        self._net = net
        active_session_ids = {target.session_id for target in sessions}
        self._owned = {
            session_id: identities
            for session_id, identities in self._owned.items()
            if session_id in active_session_ids
        }
        self._roots = {
            session_id: identity
            for session_id, identity in self._roots.items()
            if session_id in active_session_ids
        }
        active = {
            identity for identities in self._owned.values() for identity in identities
        }
        self._process_cpu = {
            key: value for key, value in self._process_cpu.items() if key in active
        }
        self._process_io = {
            key: value for key, value in self._process_io.items() if key in active
        }
        return samples


class LinuxCollector(ResourceCollector):
    """Linux collector using psutil's /proc-backed normalized counters."""


class MacOSCollector(ResourceCollector):
    """macOS collector using psutil's Mach/BSD-backed normalized counters."""


def build_collector(**kwargs) -> ResourceCollector:
    system = platform.system().lower()
    collector = MacOSCollector if system == "darwin" else LinuxCollector
    return collector(**kwargs)
