"""Deterministic, network-free smoke test for the remote consistency contract."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from pa.config import Settings
from pa.domain.models import Card, CardLane
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.modules.fleet import (
    DispatchCompletionBody,
    DispatchMaterializeBody,
    complete_dispatch,
    materialize_dispatch,
)
from tests.test_dispatch_consistency import (
    AUTHORITY_ID,
    CARD_ONE,
    DISPATCH_ONE,
    MUTATION_ONE,
    TARGET_ID,
)


def _request(settings, store, services, headers=None):
    ctx = MagicMock(settings=settings, store=store)
    ctx.services = services
    ctx.require_service.side_effect = services.__getitem__
    ctx.register_service.side_effect = services.__setitem__
    request = MagicMock()
    request.app.state.ctx = ctx
    request.headers = headers or {}
    return request


class MacMiniAutonomySmokeTest(unittest.TestCase):
    def test_disposable_card_session_materializes_acknowledges_and_cleans_up(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repository = root / "repository"
            worktree = root / "worktree"
            authority_data = root / "authority-data"
            target_data = root / "target-data"
            repository.mkdir()
            subprocess.run(["git", "init", "-q", str(repository)], check=True)
            subprocess.run(
                ["git", "-C", str(repository), "config", "user.email", "ci@pa.invalid"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repository), "config", "user.name", "PA CI"],
                check=True,
            )
            (repository / "README").write_text("smoke\n")
            subprocess.run(["git", "-C", str(repository), "add", "README"], check=True)
            subprocess.run(
                ["git", "-C", str(repository), "commit", "-qm", "base"], check=True
            )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repository),
                    "worktree",
                    "add",
                    "-q",
                    "-b",
                    "pa/smoke",
                    str(worktree),
                ],
                check=True,
            )

            card = Card(id=CARD_ONE, title="Autonomous smoke")
            authority_store = MagicMock()
            authority_store.get_card.return_value = card.model_copy(
                update={"lane": CardLane.ACTIVE, "preferred_instance": "target"}
            )
            target_store = MagicMock()
            # Dispatch begins only after the target projection contains the
            # authoritative card version; materialization binds, it does not
            # side-load a parallel card event.
            target_store.get_card.return_value = card
            target_log = MagicMock()
            target = _request(
                Settings(data_dir=target_data, instance_id=TARGET_ID),
                target_store,
                {"event_log": target_log},
                headers={"X-PA-Origin-Instance-ID": AUTHORITY_ID},
            )
            materialized = materialize_dispatch(
                target,
                DispatchMaterializeBody(
                    dispatch_id=DISPATCH_ONE,
                    mutation_id=MUTATION_ONE,
                    card=card.model_dump(mode="json"),
                    card_version=card.updated_at.isoformat(),
                    realm_id="default",
                    authority_instance_id=AUTHORITY_ID,
                    authority_url="http://authority.invalid",
                    target_instance_id=TARGET_ID,
                ),
            )
            self.assertTrue(materialized["resolvable"])
            target_store.apply_event.assert_not_called()

            session = {"id": "session-smoke", "card_id": card.id, "cwd": str(worktree)}
            self.assertEqual(session["card_id"], card.id)
            self.assertTrue(Path(session["cwd"]).is_dir())

            ledger = DispatchStore(authority_data)
            ledger.put(
                DispatchRecord(
                    dispatch_id=DISPATCH_ONE,
                    mutation_id=MUTATION_ONE,
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id=AUTHORITY_ID,
                    authority_url="http://authority.invalid",
                    target_instance_id=TARGET_ID,
                    session_id=session["id"],
                    state="running",
                )
            )
            authority = _request(
                Settings(data_dir=authority_data, instance_id=AUTHORITY_ID),
                authority_store,
                {"dispatch_store": ledger},
                {"idempotency-key": MUTATION_ONE},
            )
            ack = complete_dispatch(
                authority,
                DISPATCH_ONE,
                DispatchCompletionBody(
                    mutation_id=MUTATION_ONE,
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    source_instance_id=TARGET_ID,
                    session_id=session["id"],
                    result={"status": "complete"},
                ),
            )
            self.assertTrue(ack["acknowledged"])
            self.assertEqual(ledger.get(DISPATCH_ONE).state, "completed")
            self.assertEqual(ack["card_disposition"]["status"], "absent")
            authority_store.update_card.assert_not_called()

            subprocess.run(
                ["git", "-C", str(repository), "worktree", "remove", str(worktree)],
                check=True,
            )
            self.assertFalse(worktree.exists())
