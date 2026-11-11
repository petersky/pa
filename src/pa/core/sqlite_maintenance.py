"""Owner-local cooperation between database mutations and full maintenance.

No SQL parsing, global connection registry or changed transaction ownership.
Only an explicitly registered maintenance connection can be interrupted.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
import threading


@dataclass(eq=False)
class _Ticket:
    requested: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None

    def reason(self, stop):
        if stop is not None and stop.is_set():
            return "stopping"
        if self.requested.is_set():
            return "foreground_write"
        return None


class DatabaseMaintenanceGate:
    def __init__(self):
        self._lock = threading.Lock()
        self._writers = 0
        self._tickets: set[_Ticket] = set()

    @contextmanager
    def writer(self):
        # Announce intent before waiting for the existing mutation/publication
        # lock. The original lock still owns ordering and all durable work.
        with self._lock:
            self._writers += 1
            for ticket in self._tickets:
                ticket.requested.set()
        try:
            yield
        finally:
            with self._lock:
                self._writers -= 1

    @contextmanager
    def maintenance(self, connection, *, stop=None):
        ticket = _Ticket()
        def interrupt():
            # Repeat a latched request: an interrupt delivered between SQLite
            # statements is not guaranteed to interrupt the next statement.
            # The observer is joined before closing this handle.
            try:
                while not ticket.done.is_set():
                    if ticket.reason(stop) is not None:
                        connection.interrupt()
                    ticket.done.wait(.001)
            except BaseException as exc:
                ticket.error = exc

        observer = threading.Thread(target=interrupt, name="pa-maintenance-interrupt")
        failed = False
        try:
            with self._lock:
                self._tickets.add(ticket)
                if self._writers:
                    ticket.requested.set()
            observer.start()
            try:
                yield ticket
            except BaseException:
                failed = True
                raise
        finally:
            with self._lock:
                self._tickets.discard(ticket)
            ticket.done.set()
            if observer.ident is not None:
                observer.join()
            if ticket.error is not None and not failed:
                raise RuntimeError("Maintenance interrupt observer failed") from ticket.error


def maintenance_writer(method):
    """Announce a mutation without altering its own lock timeout or ordering."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.maintenance.writer():
            return method(self, *args, **kwargs)
    return wrapped
