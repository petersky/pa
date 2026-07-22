import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from pa.domain.models import (
    CardEvent,
    EventType,
    ProjectCreate,
    RepositoryCheckout,
    RepositoryCreate,
    RepositoryRemote,
    RepositoryStatus,
    RepositoryUpdate,
    RepositoryVisibility,
)
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

    def test_noop_unlink_preserves_legacy_repos_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.projection(root)
            project = store.create_project(ProjectCreate(title="Noop unlink"))
            linked_repo = store.create_repository(
                RepositoryCreate(url="https://example.test/linked.git")
            )
            other_repo = store.create_repository(
                RepositoryCreate(url="https://example.test/other.git")
            )
            store.link_project_repository(project.id, linked_repo.id)
            stale = [
                {
                    "url": "https://example.test/ghost.git",
                    "branch": "main",
                    "path": "/ghost",
                }
            ]
            conn = sqlite3.connect(root / "pa.db")
            conn.execute(
                "UPDATE projects SET repos=? WHERE id=?",
                (json.dumps(stale), project.id),
            )
            conn.commit()
            conn.close()

            store.unlink_project_repository(project.id, other_repo.id)

            conn = sqlite3.connect(root / "pa.db")
            row = conn.execute(
                "SELECT repos FROM projects WHERE id=?", (project.id,)
            ).fetchone()
            conn.close()
            self.assertEqual(json.loads(row[0]), stale)
            self.assertEqual(len(store.get_project(project.id).repos), 1)
            self.assertEqual(
                store.get_project(project.id).repos[0].url,
                "https://example.test/linked.git",
            )

    def test_unlink_clears_stale_legacy_repos_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.projection(root)
            project = store.create_project(ProjectCreate(title="Stale unlink"))
            repo = store.create_repository(
                RepositoryCreate(url="https://example.test/stale-unlink.git")
            )
            store.link_project_repository(project.id, repo.id)
            stale = [
                {
                    "url": "https://example.test/ghost.git",
                    "branch": "main",
                    "path": "/ghost",
                }
            ]
            conn = sqlite3.connect(root / "pa.db")
            conn.execute(
                "UPDATE projects SET repos=? WHERE id=?",
                (json.dumps(stale), project.id),
            )
            conn.commit()
            conn.close()

            store.unlink_project_repository(project.id, repo.id)
            self.assertEqual(store.get_project(project.id).repos, [])

    def test_delete_repository_clears_stale_legacy_repos_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.projection(root)
            project = store.create_project(ProjectCreate(title="Stale delete"))
            repo = store.create_repository(
                RepositoryCreate(url="https://example.test/stale-delete.git")
            )
            store.link_project_repository(project.id, repo.id)
            stale = [
                {
                    "url": "https://example.test/ghost.git",
                    "branch": "main",
                    "path": "/ghost",
                }
            ]
            conn = sqlite3.connect(root / "pa.db")
            conn.execute(
                "UPDATE projects SET repos=? WHERE id=?",
                (json.dumps(stale), project.id),
            )
            conn.commit()
            conn.close()

            store.delete_repository(repo.id)
            self.assertEqual(store.get_project(project.id).repos, [])

    def test_migration_clears_stale_legacy_json_when_normalized_exists(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "pa.db"
            now = datetime.now(UTC).isoformat()
            conn = sqlite3.connect(db)
            conn.executescript(
                """
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, title TEXT NOT NULL,
                    description TEXT NOT NULL, status TEXT NOT NULL, memberships TEXT NOT NULL,
                    repos TEXT NOT NULL, agent_prompt TEXT NOT NULL, tool_config TEXT NOT NULL,
                    tags TEXT NOT NULL, created_by_principal TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE repositories (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, url TEXT NOT NULL,
                    name TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(realm_id, url)
                );
                CREATE TABLE project_repositories (
                    project_id TEXT NOT NULL, repository_id TEXT NOT NULL, branch TEXT,
                    PRIMARY KEY (project_id, repository_id)
                );
                CREATE TABLE repository_checkouts (
                    repository_id TEXT NOT NULL, instance_id TEXT NOT NULL, path TEXT NOT NULL,
                    branch TEXT, PRIMARY KEY (repository_id, instance_id)
                );
                """
            )
            legacy = [
                {
                    "url": "https://example.test/stale-json.git",
                    "branch": "main",
                    "path": "/stale",
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
            conn.execute(
                "INSERT INTO repositories VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "repo-1",
                    "default",
                    "https://example.test/normalized.git",
                    "",
                    now,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO project_repositories VALUES (?, ?, ?)",
                ("legacy", "repo-1", "main"),
            )
            conn.commit()
            conn.close()

            store = self.projection(root)
            self.assertEqual(len(store.get_project("legacy").repos), 1)
            self.assertEqual(
                store.get_project("legacy").repos[0].url,
                "https://example.test/normalized.git",
            )
            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT repos FROM projects WHERE id=?", ("legacy",)
            ).fetchone()
            conn.close()
            self.assertEqual(json.loads(row[0]), [])

    def test_project_working_directory_returns_none_for_multiple_checkouts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.projection(Path(tmp))
            project = store.create_project(ProjectCreate(title="Multi"))
            first = store.create_repository(
                RepositoryCreate(url="https://example.test/first.git")
            )
            second = store.create_repository(
                RepositoryCreate(url="https://example.test/second.git")
            )
            store.link_project_repository(project.id, first.id)
            store.link_project_repository(project.id, second.id)
            store.set_repository_checkout(
                RepositoryCheckout(
                    repository_id=first.id, instance_id="instance-a", path="/work/a"
                )
            )
            store.set_repository_checkout(
                RepositoryCheckout(
                    repository_id=second.id, instance_id="instance-a", path="/work/b"
                )
            )

            self.assertIsNone(store.project_working_directory(project.id, "instance-a"))

    def test_repository_url_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.projection(Path(tmp))
            repo = store.create_repository(
                RepositoryCreate(
                    url="https://example.test/immutable.git", name="before"
                )
            )

            with self.assertRaisesRegex(ValueError, "immutable"):
                store.update_repository(
                    repo.id,
                    RepositoryUpdate(url="https://example.test/changed.git"),
                )

            updated = store.update_repository(repo.id, RepositoryUpdate(name="after"))
            assert updated is not None
            self.assertEqual(updated.name, "after")
            self.assertEqual(updated.url, "https://example.test/immutable.git")

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

    def test_project_updated_preserves_normalized_repository_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.projection(Path(tmp))
            project = store.create_project(ProjectCreate(title="Linked"))
            repo = store.create_repository(
                RepositoryCreate(url="https://example.test/linked.git")
            )
            store.link_project_repository(project.id, repo.id, branch="main")

            store.apply_event(
                CardEvent(
                    type=EventType.PROJECT_UPDATED,
                    realm_id="default",
                    project_id=project.id,
                    author_principal="user:local",
                    author_instance="instance-a",
                    payload={
                        "repos": [
                            {
                                "url": "https://example.test/stale-legacy.git",
                                "branch": "main",
                                "path": "/stale",
                            }
                        ]
                    },
                )
            )

            linked = store.get_project(project.id).repos
            self.assertEqual(len(linked), 1)
            self.assertEqual(linked[0].url, "https://example.test/linked.git")
            conn = sqlite3.connect(Path(tmp) / "pa.db")
            row = conn.execute(
                "SELECT repos FROM projects WHERE id=?", (project.id,)
            ).fetchone()
            conn.close()
            self.assertEqual(json.loads(row[0]), [])

    def test_project_updated_empty_repos_clears_normalized_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self.projection(Path(tmp))
            project = store.create_project(ProjectCreate(title="Linked"))
            repo = store.create_repository(
                RepositoryCreate(url="https://example.test/linked.git")
            )
            store.link_project_repository(project.id, repo.id, branch="main")

            store.apply_event(
                CardEvent(
                    type=EventType.PROJECT_UPDATED,
                    realm_id="default",
                    project_id=project.id,
                    author_principal="user:local",
                    author_instance="instance-a",
                    payload={"repos": []},
                )
            )

            self.assertEqual(store.get_project(project.id).repos, [])
            conn = sqlite3.connect(Path(tmp) / "pa.db")
            count = conn.execute(
                "SELECT COUNT(*) FROM project_repositories WHERE project_id=?",
                (project.id,),
            ).fetchone()[0]
            row = conn.execute(
                "SELECT repos FROM projects WHERE id=?", (project.id,)
            ).fetchone()
            conn.close()
            self.assertEqual(count, 0)
            self.assertEqual(json.loads(row[0]), [])

    def test_replace_project_repositories_strips_url_whitespace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "pa.db"
            now = datetime.now(UTC).isoformat()
            conn = sqlite3.connect(db)
            conn.executescript(
                """
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, title TEXT NOT NULL,
                    description TEXT NOT NULL, status TEXT NOT NULL, memberships TEXT NOT NULL,
                    repos TEXT NOT NULL, agent_prompt TEXT NOT NULL, tool_config TEXT NOT NULL,
                    tags TEXT NOT NULL, created_by_principal TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE repositories (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, url TEXT NOT NULL,
                    name TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(realm_id, url)
                );
                CREATE TABLE project_repositories (
                    project_id TEXT NOT NULL, repository_id TEXT NOT NULL, branch TEXT,
                    PRIMARY KEY (project_id, repository_id)
                );
                CREATE TABLE repository_checkouts (
                    repository_id TEXT NOT NULL, instance_id TEXT NOT NULL, path TEXT NOT NULL,
                    branch TEXT, PRIMARY KEY (repository_id, instance_id)
                );
                """
            )
            conn.execute(
                "INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "legacy",
                    "default",
                    "Legacy",
                    "",
                    "active",
                    "[]",
                    "[]",
                    "",
                    "{}",
                    "[]",
                    None,
                    now,
                    now,
                ),
            )
            url = "https://example.test/trim.git"
            conn.execute(
                "INSERT INTO repositories VALUES (?, ?, ?, ?, ?, ?)",
                ("repo-1", "default", url, "", now, now),
            )
            conn.commit()
            conn.close()

            store = self.projection(root, "legacy-host")
            with store._conn() as conn:
                store._replace_project_repositories_conn(
                    conn,
                    "legacy",
                    "default",
                    [{"url": f"{url} ", "branch": "main", "path": "/trim/path"}],
                    "legacy-host",
                )

            project = store.get_project("legacy")
            self.assertEqual(len(project.repos), 1)
            self.assertEqual(project.repos[0].url, url)
            self.assertEqual(project.repos[0].path, "/trim/path")

    def test_repository_metadata_lifecycle_and_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = self.projection(root)
            repository = store.create_repository(
                RepositoryCreate(
                    url="https://example.test/org/catalog.git",
                    name="Catalog",
                    remotes=[
                        RepositoryRemote(
                            name="origin",
                            fetch_url="https://example.test/org/catalog.git",
                            push_url="ssh://git@example.test/org/catalog.git",
                        ),
                        RepositoryRemote(
                            name="mirror",
                            fetch_url="https://mirror.test/org/catalog.git",
                        ),
                    ],
                    default_branch="main",
                    provider="github",
                    provider_repository_id="R_123",
                    provider_metadata={"owner": "org", "numeric_id": 42},
                    visibility=RepositoryVisibility.PRIVATE,
                )
            )

            self.assertEqual(repository.default_branch, "main")
            self.assertEqual(repository.remotes[1].name, "mirror")
            self.assertEqual(repository.provider_metadata["numeric_id"], 42)
            self.assertEqual(repository.visibility, RepositoryVisibility.PRIVATE)
            self.assertEqual(repository.status, RepositoryStatus.ACTIVE)

            archived = store.update_repository(
                repository.id,
                RepositoryUpdate(
                    name="Catalog archived",
                    status=RepositoryStatus.ARCHIVED,
                    provider_metadata={"owner": "org", "archived": True},
                ),
            )
            assert archived is not None
            self.assertEqual(archived.status, RepositoryStatus.ARCHIVED)
            self.assertIsNotNone(archived.archived_at)

            store.rebuild_from_log("default")
            replayed = store.get_repository(repository.id)
            assert replayed is not None
            self.assertEqual(replayed.name, "Catalog archived")
            self.assertEqual(
                replayed.remotes[0].push_url, "ssh://git@example.test/org/catalog.git"
            )
            self.assertEqual(replayed.provider_repository_id, "R_123")
            self.assertTrue(replayed.provider_metadata["archived"])
            self.assertEqual(replayed.visibility, RepositoryVisibility.PRIVATE)
            self.assertEqual(replayed.status, RepositoryStatus.ARCHIVED)
            self.assertIsNotNone(replayed.archived_at)

            active = store.update_repository(
                repository.id,
                RepositoryUpdate(
                    status=RepositoryStatus.ACTIVE,
                    default_branch=None,
                    provider_repository_id=None,
                ),
            )
            assert active is not None
            self.assertEqual(active.status, RepositoryStatus.ACTIVE)
            self.assertIsNone(active.archived_at)
            self.assertIsNone(active.default_branch)
            self.assertIsNone(active.provider_repository_id)

    def test_legacy_repository_rows_gain_safe_first_class_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "pa.db"
            now = datetime.now(UTC).isoformat()
            conn = sqlite3.connect(db)
            conn.executescript(
                """
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, title TEXT NOT NULL,
                    description TEXT NOT NULL, status TEXT NOT NULL, memberships TEXT NOT NULL,
                    repos TEXT NOT NULL, agent_prompt TEXT NOT NULL, tool_config TEXT NOT NULL,
                    tags TEXT NOT NULL, created_by_principal TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE repositories (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, url TEXT NOT NULL,
                    name TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(realm_id, url)
                );
                """
            )
            conn.execute(
                "INSERT INTO repositories VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "legacy-repo",
                    "default",
                    "https://example.test/legacy.git",
                    "Legacy",
                    now,
                    now,
                ),
            )
            conn.commit()
            conn.close()

            repository = self.projection(root).get_repository("legacy-repo")
            assert repository is not None
            self.assertEqual(repository.remotes[0].name, "origin")
            self.assertEqual(repository.remotes[0].fetch_url, repository.url)
            self.assertEqual(repository.remotes[0].push_url, repository.url)
            self.assertEqual(repository.provider_metadata, {})
            self.assertEqual(repository.visibility, RepositoryVisibility.REALM)
            self.assertEqual(repository.status, RepositoryStatus.ACTIVE)
            self.assertIsNone(repository.archived_at)


class RepositoryRouteTests(unittest.TestCase):
    def test_realm_catalog_and_instance_snapshots_use_distinct_routes(self) -> None:
        from fastapi.testclient import TestClient

        from pa.config import Settings, reset_settings
        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store

        with tempfile.TemporaryDirectory() as tmp:
            reset_settings()
            reset_store()
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="test-instance",
                agent_enabled=False,
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                realm_resp = client.get("/api/realm/repositories")
                self.assertEqual(realm_resp.status_code, 200)
                self.assertIsInstance(realm_resp.json(), list)

                snapshot_resp = client.get("/api/repositories")
                self.assertEqual(snapshot_resp.status_code, 200)
                self.assertIsInstance(snapshot_resp.json(), list)

    def test_repository_api_crud_links_checkouts_and_metadata(self) -> None:
        from fastapi.testclient import TestClient

        from pa.config import Settings, reset_settings
        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store

        with tempfile.TemporaryDirectory() as tmp:
            reset_settings()
            reset_store()
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="test-instance",
                agent_enabled=False,
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                client.get("/api/health")
                csrf = client.cookies.get("pa_csrf")
                headers = {"X-CSRF-Token": csrf}

                project_response = client.post(
                    "/api/projects",
                    json={"title": "API project"},
                    headers=headers,
                )
                self.assertEqual(
                    project_response.status_code, 201, project_response.text
                )
                project_id = project_response.json()["id"]

                create_response = client.post(
                    "/api/repositories",
                    json={
                        "url": "https://example.test/api.git",
                        "name": "API repo",
                        "remotes": [
                            {
                                "name": "origin",
                                "fetch_url": "https://example.test/api.git",
                                "push_url": "ssh://git@example.test/api.git",
                            }
                        ],
                        "default_branch": "main",
                        "provider": "github",
                        "provider_repository_id": "R_api",
                        "provider_metadata": {"owner": "example"},
                        "visibility": "private",
                    },
                    headers=headers,
                )
                self.assertEqual(create_response.status_code, 201, create_response.text)
                repository_id = create_response.json()["id"]
                self.assertEqual(create_response.json()["remotes"][0]["name"], "origin")

                update_response = client.patch(
                    f"/api/repositories/{repository_id}",
                    json={
                        "name": "API repo archived",
                        "status": "archived",
                        "provider_metadata": {"owner": "example", "archived": True},
                    },
                    headers=headers,
                )
                self.assertEqual(update_response.status_code, 200, update_response.text)
                self.assertEqual(update_response.json()["status"], "archived")
                self.assertIsNotNone(update_response.json()["archived_at"])

                link_response = client.put(
                    f"/api/projects/{project_id}/repositories/{repository_id}",
                    json={"branch": "release"},
                    headers=headers,
                )
                self.assertEqual(link_response.status_code, 200, link_response.text)
                checkout_response = client.put(
                    f"/api/repositories/{repository_id}/checkouts/test-instance",
                    json={"path": "/work/api", "branch": "release"},
                    headers=headers,
                )
                self.assertEqual(
                    checkout_response.status_code, 200, checkout_response.text
                )

                links_response = client.get(f"/api/projects/{project_id}/repositories")
                self.assertEqual(links_response.status_code, 200, links_response.text)
                self.assertEqual(links_response.json()[0]["branch"], "release")
                self.assertEqual(
                    links_response.json()[0]["checkouts"][0]["path"], "/work/api"
                )
                self.assertTrue(
                    links_response.json()[0]["repository"]["provider_metadata"][
                        "archived"
                    ]
                )

                compatibility_response = client.get(f"/api/projects/{project_id}")
                self.assertEqual(
                    compatibility_response.json()["repos"],
                    [
                        {
                            "url": "https://example.test/api.git",
                            "branch": "release",
                            "path": "/work/api",
                        }
                    ],
                )

                delete_response = client.delete(
                    f"/api/repositories/{repository_id}",
                    headers=headers,
                )
                self.assertEqual(delete_response.status_code, 204, delete_response.text)
                self.assertEqual(
                    client.get(f"/api/projects/{project_id}/repositories").json(), []
                )
            reset_store()
            reset_settings()

    def test_repository_ui_manages_catalog_links_and_local_checkout(self) -> None:
        from fastapi.testclient import TestClient

        from pa.config import Settings, reset_settings
        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store

        with tempfile.TemporaryDirectory() as tmp:
            reset_settings()
            reset_store()
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="ui-instance",
                agent_enabled=False,
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                client.get("/api/health")
                csrf = client.cookies.get("pa_csrf")
                headers = {"X-CSRF-Token": csrf}
                project_id = client.post(
                    "/api/projects",
                    json={"title": "UI project"},
                    headers=headers,
                ).json()["id"]
                repository_id = client.post(
                    "/api/repositories",
                    json={
                        "url": "https://example.test/ui.git",
                        "name": "UI repo",
                        "provider": "github",
                        "provider_metadata": {"owner": "ui"},
                        "remotes": [
                            {
                                "name": "origin",
                                "fetch_url": "https://example.test/ui.git",
                                "push_url": "https://example.test/ui.git",
                            },
                            {
                                "name": "mirror",
                                "fetch_url": "https://mirror.test/ui.git",
                                "push_url": None,
                            },
                        ],
                    },
                    headers=headers,
                ).json()["id"]
                client.put(
                    f"/api/projects/{project_id}/repositories/{repository_id}",
                    json={"branch": "main"},
                    headers=headers,
                )

                page = client.get(f"/projects?realm=default&project={project_id}")
                self.assertEqual(page.status_code, 200, page.text)
                self.assertIn("Repository catalog", page.text)
                self.assertIn("Linked repositories", page.text)
                self.assertIn(
                    f"/projects/{project_id}/repositories/{repository_id}/unlink",
                    page.text,
                )
                self.assertIn(
                    f"/projects/repositories/{repository_id}/checkout",
                    page.text,
                )
                self.assertIn("Provider metadata", page.text)

                update_response = client.post(
                    f"/projects/repositories/{repository_id}?realm=default&project={project_id}",
                    data={
                        "name": "UI repo archived",
                        "default_branch": "trunk",
                        "provider": "github",
                        "provider_repository_id": "R_ui",
                        "provider_metadata": '{"owner":"ui","managed":true}',
                        "visibility": "public",
                        "status": "archived",
                        "remote_name": "upstream",
                        "fetch_url": "https://example.test/ui-mirror.git",
                        "push_url": "ssh://git@example.test/ui.git",
                    },
                    headers=headers,
                )
                self.assertEqual(update_response.status_code, 200, update_response.text)
                updated = client.get(f"/api/repositories/{repository_id}").json()
                self.assertEqual(updated["name"], "UI repo archived")
                self.assertEqual(updated["default_branch"], "trunk")
                self.assertEqual(updated["visibility"], "public")
                self.assertEqual(updated["status"], "archived")
                self.assertTrue(updated["provider_metadata"]["managed"])
                self.assertEqual(updated["remotes"][0]["name"], "upstream")
                self.assertEqual(updated["remotes"][1]["name"], "mirror")
                self.assertEqual(
                    updated["remotes"][1]["fetch_url"], "https://mirror.test/ui.git"
                )

                checkout_response = client.post(
                    f"/projects/repositories/{repository_id}/checkout?realm=default&project={project_id}",
                    data={"path": "/work/ui", "branch": "trunk"},
                    headers=headers,
                )
                self.assertEqual(
                    checkout_response.status_code, 200, checkout_response.text
                )
                self.assertEqual(
                    client.get(f"/api/repositories/{repository_id}").json()[
                        "checkouts"
                    ][0]["path"],
                    "/work/ui",
                )

                unlink_response = client.post(
                    f"/projects/{project_id}/repositories/{repository_id}/unlink?realm=default&project={project_id}",
                    headers=headers,
                )
                self.assertEqual(unlink_response.status_code, 200, unlink_response.text)
                self.assertEqual(
                    client.get(f"/api/projects/{project_id}/repositories").json(), []
                )

                delete_response = client.post(
                    f"/projects/repositories/{repository_id}/delete?realm=default&project={project_id}",
                    headers=headers,
                )
                self.assertEqual(delete_response.status_code, 200, delete_response.text)
                self.assertEqual(
                    client.get(f"/api/repositories/{repository_id}").status_code, 404
                )
            reset_store()
            reset_settings()

    def test_repository_mcp_tools_cover_first_class_workflows(self) -> None:
        from unittest.mock import MagicMock, patch

        from pa.modules.projects import ProjectsModule

        class FakeMcp:
            def __init__(self) -> None:
                self.functions: dict[str, object] = {}

            def tool(self):
                def register(fn):
                    self.functions[fn.__name__] = fn
                    return fn

                return register

        mcp = FakeMcp()
        ctx = MagicMock()
        local_api = MagicMock(return_value={"id": "repo-1"})
        with patch("pa.mcp.local_api.request_local_pa", local_api):
            ProjectsModule().register_mcp(mcp, ctx)

        expected = {
            "list_repositories",
            "get_repository",
            "create_repository",
            "update_repository",
            "delete_repository",
            "list_project_repositories",
            "link_project_repository",
            "unlink_project_repository",
            "set_repository_checkout",
            "remove_repository_checkout",
        }
        self.assertTrue(expected.issubset(mcp.functions))

        result = mcp.functions["create_repository"](
            "https://example.test/mcp.git",
            name="MCP",
            default_branch="main",
            provider="github",
            provider_metadata={"owner": "mcp"},
            visibility="private",
        )
        self.assertEqual(result, {"id": "repo-1"})
        local_api.assert_called_with(
            ctx.settings,
            "POST",
            "/api/repositories",
            json={
                "realm_id": "default",
                "url": "https://example.test/mcp.git",
                "name": "MCP",
                "remotes": [],
                "default_branch": "main",
                "provider": "github",
                "provider_repository_id": None,
                "provider_metadata": {"owner": "mcp"},
                "visibility": "private",
                "status": "active",
            },
        )

        mcp.functions["set_repository_checkout"](
            "repo-1", "instance-a", "/work/mcp", branch="main"
        )
        local_api.assert_called_with(
            ctx.settings,
            "PUT",
            "/api/repositories/repo-1/checkouts/instance-a",
            params={"realm": "default"},
            json={"path": "/work/mcp", "branch": "main"},
        )


if __name__ == "__main__":
    unittest.main()
