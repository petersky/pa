"""Numeric SQLite call observations; never retain SQL, arguments or results."""
import sqlite3
import time

from pa.core.performance import current_observation


class ObservedConnection(sqlite3.Connection):
    def _observe(self, call, *args, commit=False):
        observation = current_observation.get()
        if observation is None:
            return call(*args)
        started = time.perf_counter()
        try:
            return call(*args)
        except sqlite3.OperationalError as exc:
            if (getattr(exc, "sqlite_errorcode", 0) & 0xFF) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                observation.sqlite_busy_failures += 1
            raise
        finally:
            elapsed = (time.perf_counter() - started) * 1000
            if commit:
                observation.sqlite_commit_ms += elapsed
            else:
                observation.sqlite_statement_ms += elapsed
                observation.sqlite_calls += 1

    def execute(self, sql, parameters=(), /):
        return self._observe(super().execute, sql, parameters)

    def executemany(self, sql, parameters, /):
        return self._observe(super().executemany, sql, parameters)

    def commit(self):
        if not self.in_transaction:
            return super().commit()
        return self._observe(super().commit, commit=True)
