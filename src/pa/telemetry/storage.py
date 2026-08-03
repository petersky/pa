from __future__ import annotations

import math
import os
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.telemetry.models import MetricQuality, TelemetryQuery, TelemetrySample

SCHEMA_VERSION = 1
ROLLUP_BUCKET_SECONDS = 300
_QUALITY_RANK = {
    MetricQuality.MEASURED.value: 0,
    MetricQuality.ESTIMATED.value: 1,
    MetricQuality.UNAVAILABLE.value: 2,
    MetricQuality.UNSUPPORTED.value: 3,
}
_RANK_QUALITY = {value: key for key, value in _QUALITY_RANK.items()}


class TelemetryStorage:
    """Independent SQLite authority for telemetry only.

    Connections, migrations, pruning, integrity handling, and locking are
    intentionally separate from PA's metadata Store and realm EventLog.
    """

    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().resolve()
        self._lock = threading.RLock()
        self.last_error: str | None = None
        self.last_prune: dict = {"state": "never"}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._open_or_recover()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _open_or_recover(self) -> None:
        try:
            with self._connect() as conn:
                result = conn.execute("PRAGMA quick_check").fetchone()
                if result and result[0] != "ok":
                    raise sqlite3.DatabaseError(str(result[0]))
                self._migrate(conn)
        except sqlite3.DatabaseError as exc:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            quarantined = self.path.with_name(f"{self.path.name}.corrupt-{stamp}")
            if self.path.exists():
                os.replace(self.path, quarantined)
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(self.path) + suffix)
                if sidecar.exists():
                    os.replace(sidecar, Path(str(quarantined) + suffix))
            self.last_error = (
                f"corrupt telemetry database quarantined as {quarantined.name}: {exc}"
            )
            with self._connect() as conn:
                self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS telemetry_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                duration_ms REAL NOT NULL,
                instance_id TEXT NOT NULL,
                instance_name TEXT NOT NULL,
                scope_type TEXT NOT NULL CHECK(scope_type IN ('instance','session')),
                scope_id TEXT NOT NULL,
                restart_id TEXT NOT NULL,
                provider_id TEXT,
                card_id TEXT,
                project_id TEXT,
                realm_id TEXT,
                principal_id TEXT,
                ownership TEXT
            );
            CREATE TABLE IF NOT EXISTS sample_metrics (
                sample_id INTEGER NOT NULL REFERENCES samples(id) ON DELETE CASCADE,
                metric TEXT NOT NULL,
                value REAL,
                unit TEXT NOT NULL,
                quality TEXT NOT NULL,
                source TEXT NOT NULL,
                detail TEXT,
                PRIMARY KEY(sample_id, metric)
            );
            CREATE INDEX IF NOT EXISTS samples_time_idx ON samples(ts);
            CREATE INDEX IF NOT EXISTS samples_scope_idx
                ON samples(scope_type, scope_id, ts);
            CREATE INDEX IF NOT EXISTS samples_filters_idx
                ON samples(instance_id, provider_id, card_id, ts);
            CREATE INDEX IF NOT EXISTS metrics_name_idx
                ON sample_metrics(metric, sample_id);

            CREATE TABLE IF NOT EXISTS rollup_metrics (
                bucket_start REAL NOT NULL,
                bucket_seconds INTEGER NOT NULL,
                instance_id TEXT NOT NULL,
                instance_name TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                provider_id TEXT NOT NULL DEFAULT '',
                card_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                realm_id TEXT NOT NULL DEFAULT '',
                principal_id TEXT NOT NULL DEFAULT '',
                metric TEXT NOT NULL,
                unit TEXT NOT NULL,
                quality_rank INTEGER NOT NULL,
                value_sum REAL,
                value_min REAL,
                value_max REAL,
                value_last REAL,
                value_count INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                PRIMARY KEY(
                    bucket_start, bucket_seconds, instance_id, scope_type, scope_id,
                    provider_id, card_id, project_id, realm_id, principal_id, metric
                )
            );
            CREATE INDEX IF NOT EXISTS rollups_time_idx
                ON rollup_metrics(bucket_start);
            CREATE INDEX IF NOT EXISTS rollups_scope_idx
                ON rollup_metrics(scope_type, scope_id, metric, bucket_start);
            """
        )
        conn.execute(
            "INSERT INTO telemetry_meta(key,value) VALUES('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )

    def insert_samples(self, samples: Iterable[TelemetrySample]) -> int:
        samples = list(samples)
        if not samples:
            return 0
        with self._lock:
            try:
                with self._connect() as conn:
                    for sample in samples:
                        cur = conn.execute(
                            """
                            INSERT INTO samples(
                                ts,duration_ms,instance_id,instance_name,scope_type,
                                scope_id,restart_id,provider_id,card_id,project_id,
                                realm_id,principal_id,ownership
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                sample.timestamp.timestamp(),
                                sample.collection_duration_ms,
                                sample.instance_id,
                                sample.instance_name,
                                sample.scope_type,
                                sample.scope_id,
                                sample.restart_id,
                                sample.provider_id,
                                sample.card_id,
                                sample.project_id,
                                sample.realm_id,
                                sample.principal_id,
                                sample.ownership,
                            ),
                        )
                        sample_id = int(cur.lastrowid)
                        conn.executemany(
                            """
                            INSERT INTO sample_metrics(
                                sample_id,metric,value,unit,quality,source,detail
                            ) VALUES(?,?,?,?,?,?,?)
                            """,
                            [
                                (
                                    sample_id,
                                    name,
                                    metric.value,
                                    metric.unit,
                                    metric.quality.value,
                                    metric.source,
                                    metric.detail,
                                )
                                for name, metric in sample.metrics.items()
                            ],
                        )
                self.last_error = None
                return len(samples)
            except (sqlite3.Error, OSError) as exc:
                self.last_error = str(exc)[:1000]
                raise

    @staticmethod
    def _where(
        query: TelemetryQuery,
        *,
        alias: str = "s",
        time_column: str = "ts",
        start_offset_seconds: int = 0,
    ) -> tuple[str, list]:
        clauses = [
            f"{alias}.{time_column} >= ?",
            f"{alias}.{time_column} <= ?",
        ]
        values: list = [
            query.start.timestamp() - start_offset_seconds,
            query.end.timestamp(),
        ]
        for column, selected in (
            ("scope_id", query.scope_ids),
            ("instance_id", query.instance_ids),
            ("provider_id", query.provider_ids),
            ("card_id", query.card_ids),
        ):
            if selected:
                placeholders = ",".join("?" for _ in selected)
                clauses.append(f"{alias}.{column} IN ({placeholders})")
                values.extend(selected)
        if query.scope_type:
            clauses.append(f"{alias}.scope_type = ?")
            values.append(query.scope_type)
        if query.visible_principal_id:
            clauses.append(
                f"({alias}.scope_type = 'instance' OR {alias}.principal_id = ?)"
            )
            values.append(query.visible_principal_id)
        return " AND ".join(clauses), values

    def query(self, query: TelemetryQuery) -> dict:
        if query.end <= query.start:
            raise ValueError("end must be after start")
        if query.end - query.start > timedelta(days=31):
            raise ValueError("query range may not exceed 31 days")
        metric_clause = ""
        metric_values: list[str] = []
        if query.metrics:
            metric_clause = (
                " AND m.metric IN (" + ",".join("?" for _ in query.metrics) + ")"
            )
            metric_values = query.metrics
        where, values = self._where(query)
        with self._lock, self._connect() as conn:
            has_rollups = conn.execute(
                "SELECT 1 FROM rollup_metrics WHERE bucket_start>=? "
                "AND bucket_start<=? LIMIT 1",
                (
                    query.start.timestamp() - ROLLUP_BUCKET_SECONDS,
                    query.end.timestamp(),
                ),
            ).fetchone()
            bucket = max(
                query.bucket_seconds,
                ROLLUP_BUCKET_SECONDS if has_rollups else query.bucket_seconds,
            )
            raw_rows = conn.execute(
                f"""
                SELECT CAST(s.ts / ? AS INTEGER) * ? AS bucket_start,
                       s.instance_id,s.instance_name,s.scope_type,s.scope_id,
                       COALESCE(s.provider_id,'') provider_id,
                       COALESCE(s.card_id,'') card_id,
                       COALESCE(s.project_id,'') project_id,
                       COALESCE(s.realm_id,'') realm_id,
                       m.metric,m.unit,
                       AVG(m.value) value_avg,MIN(m.value) value_min,
                       MAX(m.value) value_max,COUNT(m.value) value_count,
                       COUNT(*) sample_count,
                       MAX(CASE m.quality
                         WHEN 'unsupported' THEN 3 WHEN 'unavailable' THEN 2
                         WHEN 'estimated' THEN 1 ELSE 0 END) quality_rank,
                       COUNT(DISTINCT s.restart_id) restart_count,
                       MIN(s.ts) first_ts,MAX(s.ts) last_ts
                FROM samples s JOIN sample_metrics m ON m.sample_id=s.id
                WHERE {where}{metric_clause}
                GROUP BY bucket_start,s.instance_id,s.instance_name,s.scope_type,
                         s.scope_id,provider_id,card_id,project_id,realm_id,
                         m.metric,m.unit
                ORDER BY bucket_start
                """,
                [bucket, bucket, *values, *metric_values],
            ).fetchall()

            rollup_where, rollup_values = self._where(
                query,
                alias="r",
                time_column="bucket_start",
                start_offset_seconds=ROLLUP_BUCKET_SECONDS,
            )
            rollup_metric_clause = ""
            if query.metrics:
                rollup_metric_clause = (
                    " AND r.metric IN (" + ",".join("?" for _ in query.metrics) + ")"
                )
            rollup_rows = conn.execute(
                f"""
                SELECT CAST(r.bucket_start / ? AS INTEGER) * ? AS bucket_start,
                       r.instance_id,r.instance_name,r.scope_type,r.scope_id,
                       r.provider_id,r.card_id,r.project_id,r.realm_id,
                       r.metric,r.unit,
                       SUM(r.value_sum)/NULLIF(SUM(r.value_count),0) value_avg,
                       MIN(r.value_min) value_min,MAX(r.value_max) value_max,
                       SUM(r.value_count) value_count,
                       SUM(r.sample_count) sample_count,
                       MAX(r.quality_rank) quality_rank,
                       0 restart_count,MIN(r.bucket_start) first_ts,
                       MAX(r.bucket_start+r.bucket_seconds) last_ts
                FROM rollup_metrics r
                WHERE {rollup_where}{rollup_metric_clause}
                GROUP BY bucket_start,r.instance_id,r.instance_name,r.scope_type,
                         r.scope_id,r.provider_id,r.card_id,r.project_id,r.realm_id,
                         r.metric,r.unit
                ORDER BY bucket_start
                """,
                [bucket, bucket, *rollup_values, *metric_values],
            ).fetchall()

        combined: dict[tuple, dict] = {}
        for row in [*rollup_rows, *raw_rows]:
            key = (
                row["bucket_start"],
                row["instance_id"],
                row["scope_type"],
                row["scope_id"],
                row["provider_id"],
                row["card_id"],
                row["project_id"],
                row["realm_id"],
                row["metric"],
                row["unit"],
            )
            current = combined.get(key)
            incoming = dict(row)
            if current is None:
                combined[key] = incoming
                continue
            # Raw and rollup should not overlap after deterministic pruning, but
            # combine defensively during an interrupted maintenance transaction.
            total_values = current["value_count"] + incoming["value_count"]
            weighted = (current["value_avg"] or 0) * current["value_count"] + (
                incoming["value_avg"] or 0
            ) * incoming["value_count"]
            current["value_avg"] = weighted / total_values if total_values else None
            current["value_min"] = (
                min(
                    value
                    for value in (current["value_min"], incoming["value_min"])
                    if value is not None
                )
                if any(
                    value is not None
                    for value in (current["value_min"], incoming["value_min"])
                )
                else None
            )
            current["value_max"] = (
                max(
                    value
                    for value in (current["value_max"], incoming["value_max"])
                    if value is not None
                )
                if any(
                    value is not None
                    for value in (current["value_max"], incoming["value_max"])
                )
                else None
            )
            current["value_count"] = total_values
            current["sample_count"] += incoming["sample_count"]
            current["quality_rank"] = max(
                current["quality_rank"], incoming["quality_rank"]
            )
            current["restart_count"] += incoming["restart_count"]
            current["first_ts"] = min(current["first_ts"], incoming["first_ts"])
            current["last_ts"] = max(current["last_ts"], incoming["last_ts"])

        dropped_invalid_samples = 0
        series: dict[tuple, list] = defaultdict(list)
        for row in sorted(combined.values(), key=lambda item: item["bucket_start"]):
            if any(value is not None and not math.isfinite(float(value)) for value in (row["value_avg"], row["value_min"], row["value_max"])):
                dropped_invalid_samples += 1
                continue
            series[
                (
                    row["instance_id"],
                    row["instance_name"],
                    row["scope_type"],
                    row["scope_id"],
                    row["provider_id"],
                    row["card_id"],
                    row["project_id"],
                    row["realm_id"],
                    row["metric"],
                    row["unit"],
                )
            ].append(
                {
                    "timestamp": datetime.fromtimestamp(
                        row["bucket_start"], UTC
                    ).isoformat(),
                    "avg": row["value_avg"],
                    "min": row["value_min"],
                    "max": row["value_max"],
                    "value_count": row["value_count"],
                    "sample_count": row["sample_count"],
                    "quality": _RANK_QUALITY.get(
                        row["quality_rank"], MetricQuality.UNAVAILABLE.value
                    ),
                    "restart": row["restart_count"] > 1,
                    "first_timestamp": datetime.fromtimestamp(
                        row["first_ts"], UTC
                    ).isoformat(),
                    "last_timestamp": datetime.fromtimestamp(
                        row["last_ts"], UTC
                    ).isoformat(),
                }
            )
        return {
            "start": query.start.isoformat(),
            "end": query.end.isoformat(),
            "bucket_seconds": bucket,
            "series": [
                {
                    "instance_id": key[0],
                    "instance_name": key[1],
                    "scope_type": key[2],
                    "scope_id": key[3],
                    "provider_id": key[4] or None,
                    "card_id": key[5] or None,
                    "project_id": key[6] or None,
                    "realm_id": key[7] or None,
                    "metric": key[8],
                    "unit": key[9],
                    "points": points,
                }
                for key, points in series.items()
            ],
            "diagnostics": {
                "bucket_count": len({point["timestamp"] for points in series.values() for point in points}),
                "series_count": len(series),
                "point_count": sum(len(points) for points in series.values()),
                "collection_freshness": max((point["last_timestamp"] for points in series.values() for point in points), default=None),
                "dropped_invalid_samples": dropped_invalid_samples,
            },
        }

    @staticmethod
    def _rollup_ids(conn: sqlite3.Connection, ids: list[int]) -> int:
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""
            SELECT CAST(s.ts / ? AS INTEGER) * ? bucket_start,
                   s.instance_id,s.instance_name,s.scope_type,s.scope_id,
                   COALESCE(s.provider_id,'') provider_id,
                   COALESCE(s.card_id,'') card_id,
                   COALESCE(s.project_id,'') project_id,
                   COALESCE(s.realm_id,'') realm_id,
                   COALESCE(s.principal_id,'') principal_id,
                   m.metric,m.unit,
                   MAX(CASE m.quality WHEN 'unsupported' THEN 3
                       WHEN 'unavailable' THEN 2 WHEN 'estimated' THEN 1
                       ELSE 0 END) quality_rank,
                   SUM(m.value) value_sum,MIN(m.value) value_min,
                   MAX(m.value) value_max,
                   (SELECT sm2.value FROM sample_metrics sm2
                     JOIN samples s2 ON s2.id=sm2.sample_id
                     WHERE sm2.metric=m.metric
                       AND s2.scope_id=s.scope_id
                       AND CAST(s2.ts / ? AS INTEGER)=CAST(s.ts / ? AS INTEGER)
                       AND s2.id IN ({placeholders})
                     ORDER BY s2.ts DESC LIMIT 1) value_last,
                   COUNT(m.value) value_count,COUNT(*) sample_count
            FROM samples s JOIN sample_metrics m ON m.sample_id=s.id
            WHERE s.id IN ({placeholders})
            GROUP BY bucket_start,s.instance_id,s.instance_name,s.scope_type,
                     s.scope_id,provider_id,card_id,project_id,realm_id,
                     principal_id,m.metric,m.unit
            """,
            [
                ROLLUP_BUCKET_SECONDS,
                ROLLUP_BUCKET_SECONDS,
                ROLLUP_BUCKET_SECONDS,
                ROLLUP_BUCKET_SECONDS,
                *ids,
                *ids,
            ],
        ).fetchall()
        for row in rows:
            conn.execute(
                """
                INSERT INTO rollup_metrics(
                    bucket_start,bucket_seconds,instance_id,instance_name,
                    scope_type,scope_id,provider_id,card_id,project_id,realm_id,
                    principal_id,metric,unit,quality_rank,value_sum,value_min,
                    value_max,value_last,value_count,sample_count
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(
                    bucket_start,bucket_seconds,instance_id,scope_type,scope_id,
                    provider_id,card_id,project_id,realm_id,principal_id,metric
                ) DO UPDATE SET
                    quality_rank=MAX(quality_rank,excluded.quality_rank),
                    value_sum=COALESCE(value_sum,0)+COALESCE(excluded.value_sum,0),
                    value_min=CASE
                      WHEN value_min IS NULL THEN excluded.value_min
                      WHEN excluded.value_min IS NULL THEN value_min
                      ELSE MIN(value_min,excluded.value_min) END,
                    value_max=CASE
                      WHEN value_max IS NULL THEN excluded.value_max
                      WHEN excluded.value_max IS NULL THEN value_max
                      ELSE MAX(value_max,excluded.value_max) END,
                    value_last=COALESCE(excluded.value_last,value_last),
                    value_count=value_count+excluded.value_count,
                    sample_count=sample_count+excluded.sample_count
                """,
                (
                    row["bucket_start"],
                    ROLLUP_BUCKET_SECONDS,
                    row["instance_id"],
                    row["instance_name"],
                    row["scope_type"],
                    row["scope_id"],
                    row["provider_id"],
                    row["card_id"],
                    row["project_id"],
                    row["realm_id"],
                    row["principal_id"],
                    row["metric"],
                    row["unit"],
                    row["quality_rank"],
                    row["value_sum"],
                    row["value_min"],
                    row["value_max"],
                    row["value_last"],
                    row["value_count"],
                    row["sample_count"],
                ),
            )
        conn.execute(
            f"DELETE FROM samples WHERE id IN ({placeholders})",
            ids,
        )
        return len(ids)

    def database_size(self) -> int:
        return sum(
            path.stat().st_size
            for path in (
                self.path,
                Path(str(self.path) + "-wal"),
                Path(str(self.path) + "-shm"),
            )
            if path.exists()
        )

    def prune(
        self,
        *,
        raw_retention_hours: float,
        rollup_retention_hours: float,
        max_database_bytes: int,
        now: datetime | None = None,
    ) -> dict:
        now = now or datetime.now(UTC)
        raw_cutoff = (now - timedelta(hours=raw_retention_hours)).timestamp()
        rollup_cutoff = (now - timedelta(hours=rollup_retention_hours)).timestamp()
        raw_deleted = rollup_deleted = size_batches = 0
        with self._lock, self._connect() as conn:
            while True:
                ids = [
                    int(row[0])
                    for row in conn.execute(
                        "SELECT id FROM samples WHERE ts < ? ORDER BY ts,id LIMIT 2000",
                        (raw_cutoff,),
                    ).fetchall()
                ]
                if not ids:
                    break
                raw_deleted += self._rollup_ids(conn, ids)
            cur = conn.execute(
                "DELETE FROM rollup_metrics WHERE bucket_start < ?",
                (rollup_cutoff,),
            )
            rollup_deleted += max(0, cur.rowcount)
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            # Size pressure is deterministic: oldest raw samples are rolled up
            # first, then the oldest rollup buckets are removed.
            while self.database_size() > max_database_bytes:
                ids = [
                    int(row[0])
                    for row in conn.execute(
                        "SELECT id FROM samples ORDER BY ts,id LIMIT 1000"
                    ).fetchall()
                ]
                if ids:
                    raw_deleted += self._rollup_ids(conn, ids)
                    size_batches += 1
                    conn.commit()
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    continue
                buckets = [
                    float(row[0])
                    for row in conn.execute(
                        "SELECT DISTINCT bucket_start FROM rollup_metrics "
                        "ORDER BY bucket_start LIMIT 10"
                    ).fetchall()
                ]
                if not buckets:
                    break
                placeholders = ",".join("?" for _ in buckets)
                cur = conn.execute(
                    f"DELETE FROM rollup_metrics WHERE bucket_start IN ({placeholders})",
                    buckets,
                )
                rollup_deleted += max(0, cur.rowcount)
                size_batches += 1
                conn.commit()
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result = {
            "state": "ok",
            "at": now.isoformat(),
            "raw_samples_pruned": raw_deleted,
            "rollup_rows_pruned": rollup_deleted,
            "size_pressure_batches": size_batches,
            "database_bytes": self.database_size(),
            "maximum_database_bytes": max_database_bytes,
        }
        self.last_prune = result
        return result

    def compact(self) -> dict:
        with self._lock, self._connect() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("VACUUM")
        return {"state": "ok", "database_bytes": self.database_size()}

    def status(self) -> dict:
        with self._lock, self._connect() as conn:
            sample = conn.execute(
                "SELECT COUNT(*) count,MIN(ts) oldest,MAX(ts) newest FROM samples"
            ).fetchone()
            rollup = conn.execute(
                "SELECT COUNT(*) count,MIN(bucket_start) oldest,"
                "MAX(bucket_start+bucket_seconds) newest FROM rollup_metrics"
            ).fetchone()
        oldest_values = [
            value for value in (sample["oldest"], rollup["oldest"]) if value is not None
        ]
        newest_values = [
            value for value in (sample["newest"], rollup["newest"]) if value is not None
        ]
        return {
            "database_path": str(self.path),
            "database_bytes": self.database_size(),
            "raw_samples": sample["count"],
            "rollup_rows": rollup["count"],
            "oldest_sample": (
                datetime.fromtimestamp(min(oldest_values), UTC).isoformat()
                if oldest_values
                else None
            ),
            "newest_sample": (
                datetime.fromtimestamp(max(newest_values), UTC).isoformat()
                if newest_values
                else None
            ),
            "last_error": self.last_error,
            "last_prune": self.last_prune,
            "schema_version": SCHEMA_VERSION,
            "sync_authority": "excluded",
        }

    def export(self, query: TelemetryQuery, *, max_points: int = 10_000) -> dict:
        result = self.query(query)
        count = sum(len(item["points"]) for item in result["series"])
        if count > max_points:
            raise ValueError(f"diagnostic export exceeds {max_points} points")
        result["redaction"] = (
            "resource metrics and PA-owned identifiers only; no arguments, prompts, "
            "paths, credentials, payloads, or unrelated process metadata"
        )
        return result

    def dimensions(self, *, visible_principal_id: str | None = None) -> dict:
        visibility = ""
        values: list[str] = []
        if visible_principal_id:
            visibility = " WHERE scope_type='instance' OR principal_id=?"
            values.append(visible_principal_id)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT instance_id,instance_name,scope_type,scope_id,"
                "provider_id,card_id,project_id,realm_id FROM samples"
                + visibility
                + " ORDER BY instance_name,scope_type,scope_id",
                values,
            ).fetchall()
        return {
            "instances": sorted(
                {
                    (row["instance_id"], row["instance_name"])
                    for row in rows
                    if row["instance_id"]
                }
            ),
            "sessions": sorted(
                {row["scope_id"] for row in rows if row["scope_type"] == "session"}
            ),
            "providers": sorted(
                {row["provider_id"] for row in rows if row["provider_id"]}
            ),
            "cards": sorted({row["card_id"] for row in rows if row["card_id"]}),
            "projects": sorted(
                {row["project_id"] for row in rows if row["project_id"]}
            ),
            "realms": sorted({row["realm_id"] for row in rows if row["realm_id"]}),
        }
