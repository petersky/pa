"""Numeric SQLite call observations; never retain SQL, arguments or results."""
import sqlite3
import time
from contextlib import ExitStack, closing, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import get_ident

from pa.core.performance import current_observation


_connection_scope = ContextVar("pa_sqlite_connection_scope", default=None)


@dataclass
class _ConnectionScope:
    thread_id: int
    closers: ExitStack
    available: dict = field(default_factory=dict)
    active: bool = True


def _owned_scope():
    scope = _connection_scope.get()
    return scope if scope is not None and scope.active and scope.thread_id == get_ident() else None


@contextmanager
def sqlite_connection_scope():
    """Own derived connections until this worker finishes, including late work."""
    if _owned_scope() is not None:
        yield
        return
    with ExitStack() as closers:
        scope = _ConnectionScope(get_ident(), closers)
        token = _connection_scope.set(scope)
        try:
            yield
        finally:
            scope.active = False
            scope.available.clear()
            _connection_scope.reset(token)


@contextmanager
def derived_connection(owner, factory):
    scope = _owned_scope()
    if scope is None:
        with closing(factory()) as connection:
            yield connection
        return
    available = scope.available.setdefault(owner, [])
    if available:
        connection = available.pop()
    else:
        connection = factory()
        scope.closers.callback(connection.close)
    try:
        yield connection
    except BaseException:
        # A failed helper never returns a potentially broken handle for reuse.
        connection.close()
        raise
    else:
        available.append(connection)


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
