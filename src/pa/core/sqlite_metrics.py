"""Numeric SQLite call observations; never retain SQL, arguments or results."""
import sqlite3
import time

from pa.core.performance import current_observation


class ObservedCursor(sqlite3.Cursor):
    """Include lazy SQLite row production, not just initial execute time."""
    def execute(self, sql, parameters=(), /):
        return self.connection._observe(super().execute, sql, parameters)

    def executemany(self, sql, parameters, /):
        return self.connection._observe(super().executemany, sql, parameters)

    def _read(self, call, *args, single=False):
        result = self.connection._observe(call, *args)
        observation = current_observation.get()
        if observation is not None:
            observation.sqlite_rows += int(result is not None) if single else len(result)
        return result

    def fetchone(self):
        return self._read(super().fetchone, single=True)

    def fetchmany(self, size=None):
        if size is None:
            return self._read(super().fetchmany)
        return self._read(super().fetchmany, size)

    def fetchall(self):
        return self._read(super().fetchall)

    def __next__(self):
        return self._read(super().__next__, single=True)


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
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, parameters, /):
        return self.cursor().executemany(sql, parameters)

    def cursor(self, factory=None):
        return super().cursor(factory or ObservedCursor)

    def commit(self):
        if not self.in_transaction:
            return super().commit()
        return self._observe(super().commit, commit=True)
