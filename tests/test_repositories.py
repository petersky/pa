import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from pa.domain.models import ProjectCreate, RepositoryCheckout, RepositoryCreate
from pa.domain.projection import CardProjection
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class RepositoryProjectionTests(unittest.TestCase):
    def projection(self, root: Path, instance_id: str = "instance-a") -> CardProjection:
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, instance_id)
        return CardProjection(root / "pa.db", log)

    def test_many_to_many_links_and_instance_checkouts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.projection(Path(tmp))
            first = store.create_project(ProjectCreate(title="First"))
            second = store.create_project(ProjectCreate(title="Second"))
            repo = store.create_repository(
                RepositoryCreate(url="https://example.test/org/repo.git")
            )

            self.assertTrue(
                store.link_project_repository(first.id, repo.id, branch="main")
            )
            self.assertTrue(
                store.link_project_repository(second.id, repo.id, branch="release")
            )
            store.set_repository_checkout(
                RepositoryCheckout(
                    repository_id=repo.id, instance_id="instance-a", path="/work/a"
                )
            )
            store.set_repository_checkout(
                RepositoryCheckout(
                    repository_id=repo.id, instance_id="instance-b", path="/work/b"
                )
            )

            self.assertEqual(store.get_project(first.id).repos[0].path, "/work/a")
            self.assertEqual(store.get_project(second.id).repos[0].branch, "release")
            self.assertEqual(
                store.project_working_directory(first.id, "instance-b"), "/work/b"
            )
            self.assertEqual(len(store.list_repository_checkouts(repo.id)), 2)

            store.unlink_project_repository(first.id, repo.id)
            self.assertEqual(store.get_project(first.id).repos, [])
            self.assertEqual(len(store.get_project(second.id).repos), 1)

    def test_legacy_project_repos_are_migrated_without_data_loss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "pa.db"
            now = datetime.now(UTC).isoformat()
            conn = sqlite3.connect(db)
            conn.execute(
                """CREATE TABLE projects (id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, title TEXT NOT NULL, description TEXT NOT NULL, status TEXT NOT NULL, memberships TEXT NOT NULL, repos TEXT NOT NULL, agent_prompt TEXT NOT NULL, tool_config TEXT NOT NULL, tags TEXT NOT NULL, created_by_principal TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"""
            )
            legacy = [
                {
                    "url": "https://example.test/legacy.git",
                    "branch": "main",
                    "path": "/legacy/path",
                }
            ]
            conn.execute(
                "INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "legacy",
                    "default",
                    "Legacy",
                    "",
                    "active",
                    "[]",
                    json.dumps(legacy),
                    "",
                    "{}",
                    "[]",
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
            conn.close()

            store = self.projection(root, "legacy-host")
            project = store.get_project("legacy")
            self.assertEqual(project.repos[0].url, legacy[0]["url"])
            self.assertEqual(project.repos[0].path, legacy[0]["path"])
            repository = store.list_repositories()[0]
            self.assertEqual(
                store.project_working_directory("legacy", "legacy-host"), "/legacy/path"
            )
            self.assertEqual(
                store.list_repository_checkouts(repository.id)[0].branch, "main"
            )

    def test_repository_mutations_emit_sync_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.projection(root)
            project = store.create_project(ProjectCreate(title="Synced"))
            repo = store.create_repository(
                RepositoryCreate(url="https://example.test/sync.git")
            )
            store.link_project_repository(project.id, repo.id)
            store.set_repository_checkout(
                RepositoryCheckout(
                    repository_id=repo.id, instance_id="instance-a", path="/sync"
                )
            )
            head = store.event_log.get_head("default")
            events = []
            store.event_log.apply_commit_chain(head, events.append)
            kinds = {event.type.value for event in events}
            self.assertIn("repository_created", kinds)
            self.assertIn("project_repository_linked", kinds)
            self.assertIn("repository_checkout_set", kinds)


if __name__ == "__main__":
    unittest.main()
