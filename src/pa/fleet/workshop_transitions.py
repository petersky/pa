"""Tiny live publication of dispatch transitions and PR gate changes.

The Workshop floor builds its skeleton from ``?view=floor`` and moves agents
from the multiplexed session event stream. Two things that stream does not
carry are dispatch stage changes (a reservation token moving through Queue and
Tooling-up) and pull-request gate changes (a card moving to or off the gantry).
Those are published here, as ids plus the new state (never snapshots), to the
existing ``live_updates`` broker and reach the browser through
``/api/cards/events`` as a ``workshop-floor`` event.

Every message carries a per-realm ``seq``. The broker drops the oldest message
for a slow subscriber, so a client that sees a gap in ``seq`` simply refetches
the skeleton once.
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from pa.fleet.workshop import pull_request_gate
from pa.fleet.workshop_stations import GANTRY_WATCH_STATUSES, gate_reasons

MESSAGE_TYPE = "workshop_floor"
SSE_EVENT = "workshop-floor"
SERVICE_NAME = "workshop_transitions"
_FINGERPRINT_LIMIT = 4096


def floor_topic(realm_id: str) -> str:
    """Broker topic for floor moves.

    Floor moves use their own topic so a burst of dispatch transitions can never
    push a ``cards-changed`` invalidation out of the small per-subscriber queue
    that the Work board and notifications rely on. ``/api/cards/events`` only
    joins this topic when asked (``?include=workshop``).
    """
    return f"workshop-floor:{realm_id}"


class WorkshopTransitionPublisher:
    def __init__(self, broker: Callable[[], Any]) -> None:
        # ``broker`` is resolved lazily: the live_updates service is registered
        # by another module and may not exist when this one loads.
        self._broker = broker
        self._sequences: dict[str, itertools.count[int]] = defaultdict(
            lambda: itertools.count(1)
        )
        self._gates: dict[str, tuple[Any, ...]] = {}

    def _publish(self, realm_id: str | None, message: dict[str, Any]) -> bool:
        broker = self._broker()
        if broker is None or not realm_id:
            return False
        broker.publish(
            floor_topic(realm_id),
            {
                "type": MESSAGE_TYPE,
                "realm_id": realm_id,
                "seq": next(self._sequences[realm_id]),
                **message,
            },
        )
        return True

    def dispatch_changed(self, previous: Any, record: Any) -> None:
        """A dispatch entered a new state or got its session."""
        self._publish(
            getattr(record, "realm_id", None),
            {
                "kind": "dispatch",
                "dispatch_id": record.dispatch_id,
                "card_id": record.card_id,
                "session_id": record.session_id,
                "target_instance_id": record.target_instance_id,
                "state": record.state,
                "previous_state": getattr(previous, "state", None),
            },
        )

    def watch_changed(self, watch: Any) -> None:
        """A PR watch was written; publish only when its gate meaningfully moved."""
        gate = pull_request_gate(
            {
                "id": watch.id,
                "repository": watch.repository,
                "pr_number": watch.pr_number,
                "status": getattr(watch.status, "value", watch.status),
                "head_sha": watch.head_sha,
                "retired_at": watch.retired_at,
                "state": watch.state,
            }
        )
        active = (
            gate["status"] in GANTRY_WATCH_STATUSES and not gate["retired_at"]
        )
        reasons = gate_reasons(gate["gate"]) if active else []
        fingerprint = (watch.card_id, gate["status"], active, tuple(reasons))
        if self._gates.get(watch.id) == fingerprint:
            return
        if len(self._gates) >= _FINGERPRINT_LIMIT:
            self._gates.clear()
        self._gates[watch.id] = fingerprint
        self._publish(
            watch.realm_id,
            {
                "kind": "gate",
                "watch_id": watch.id,
                "card_id": watch.card_id,
                "pr_number": watch.pr_number,
                "status": gate["status"],
                "active": active,
                "reasons": reasons,
            },
        )


def workshop_transition_publisher(ctx: Any) -> WorkshopTransitionPublisher:
    """The one publisher for ``ctx``, creating and registering it on first use."""
    publisher = ctx.services.get(SERVICE_NAME)
    if publisher is None:
        publisher = WorkshopTransitionPublisher(
            lambda: ctx.services.get("live_updates")
        )
        ctx.register_service(SERVICE_NAME, publisher)
    return publisher
