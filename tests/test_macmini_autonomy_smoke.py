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

            card = Card(id="smoke-card", title="Autonomous smoke")
            authority_store = MagicMock()
            authority_store.get_card.return_value = card.model_copy(
                update={"lane": CardLane.ACTIVE, "preferred_instance": "target"}
            )
            target_store = MagicMock()
            target_store.get_card.return_value = None
            target_log = MagicMock()
            target = _request(
                Settings(data_dir=target_data, instance_id="target"),
                target_store,
                {"event_log": target_log},
            )
            materialized = materialize_dispatch(
                target,
                DispatchMaterializeBody(
                    dispatch_id="dispatch-smoke",
                    mutation_id="mutation-smoke",
                    card=card.model_dump(mode="json"),
                    card_version=card.updated_at.isoformat(),
                    realm_id="default",
                    authority_instance_id="authority",
                    authority_url="http://authority.invalid",
                    target_instance_id="target",
                ),
            )
            self.assertTrue(materialized["resolvable"])
            exact = target_store.apply_event.call_args.args[0].payload
            self.assertEqual(exact, card.model_dump(mode="json"))

            session = {"id": "session-smoke", "card_id": card.id, "cwd": str(worktree)}
            self.assertEqual(session["card_id"], card.id)
            self.assertTrue(Path(session["cwd"]).is_dir())

            ledger = DispatchStore(authority_data)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-smoke",
                    mutation_id="mutation-smoke",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority.invalid",
                    target_instance_id="target",
                    session_id=session["id"],
                    state="running",
                )
            )
            authority = _request(
                Settings(data_dir=authority_data, instance_id="authority"),
                authority_store,
                {"dispatch_store": ledger},
                {"idempotency-key": "mutation-smoke"},
            )
            ack = complete_dispatch(
                authority,
                "dispatch-smoke",
                DispatchCompletionBody(
                    mutation_id="mutation-smoke",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id=session["id"],
                    result={"status": "complete"},
                ),
            )
            self.assertTrue(ack["acknowledged"])
            self.assertEqual(ledger.get("dispatch-smoke").state, "completed")
            authority_store.update_card.assert_called_once()

            subprocess.run(
                ["git", "-C", str(repository), "worktree", "remove", str(worktree)],
                check=True,
            )
            self.assertFalse(worktree.exists())
