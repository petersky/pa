from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import FleetInstance
from pa.domain.store import get_store, reset_store
from pa.fleet.placement import (
    PlacementCandidate,
    PlacementError,
    PlacementPolicy,
    PlacementRequest,
    PlacementService,
    RoundRobinCursorStore,
)
from pa.fleet.policy import (
    MACBOOK_INSTANCE_ID,
    DispatchIntent,
    FleetPolicyService,
    InstanceGroupCreate,
    InstanceParticipationPolicy,
    ParticipationMode,
    PlacementDefault,
    compatibility_policy,
)


def _fresh(value):
    return {
        "state": "fresh",
        "value": value,
        "observed_at": datetime.now(UTC).isoformat(),
    }


def _candidate(
    instance_id: str,
    policy: InstanceParticipationPolicy,
    *,
    group_membership: str = "included",
    explicit: bool = True,
    supported: bool = True,
) -> PlacementCandidate:
    return PlacementCandidate(
        instance_id=instance_id,
        name=instance_id,
        capabilities=["browser", "capacity:4"],
        group_membership=group_membership,
        participation_policy=policy,
        participation_policy_explicit=explicit,
        participation_policy_supported=supported,
        reachability=_fresh({"health": "up"}),
        activity=_fresh(
            {
                "state": "idle",
                "active_capacity_consumers": 0,
                "queued_prompts": 0,
                "quiescing": False,
            }
        ),
        providers=_fresh(
            [
                {
                    "id": "codex",
                    "available": True,
                    "auth_state": "authenticated",
                    "models": ["gpt-5"],
                }
            ]
        ),
        repositories=_fresh(
            {
                "observations": [
                    {
                        "state": "fresh",
                        "snapshot": {"repository_id": "repo-1"},
                    }
                ],
                "workspaces": [],
            }
        ),
    )


def _request(
    policy: PlacementPolicy,
    *,
    workload: str = "repository",
    intent: DispatchIntent = DispatchIntent.AUTOMATIC,
    project_id: str | None = "project-1",
) -> PlacementRequest:
    return PlacementRequest(
        realm_id="default",
        fleet_id="fleet",
        policy=policy,
        card_id="card-1",
        provider="codex",
        model_id="gpt-5",
        required_capabilities=["browser"],
        repository_ids=["repo-1"] if workload == "repository" else [],
        workload_profile=workload,
        project_id=project_id,
        dispatch_intent=intent,
        resolved_group_id="code-workers",
        resolved_group_name="Code workers",
        group_version=1,
        policy_enforcement_active=True,
    )


@pytest.mark.parametrize("placement_policy", list(PlacementPolicy))
def test_every_automatic_policy_uses_the_same_policy_filtered_set(
    tmp_path: Path, placement_policy: PlacementPolicy
) -> None:
    macbook_policy = InstanceParticipationPolicy(
        instance_id=MACBOOK_INSTANCE_ID,
        denied_profiles=["repository"],
        allowed_profiles=["research", "operations"],
        reason="authority/UI host, not an automatic code worker",
    )
    worker_policy = InstanceParticipationPolicy(instance_id="worker")
    decision = PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
        _request(placement_policy),
        [
            _candidate(MACBOOK_INSTANCE_ID, macbook_policy),
            _candidate("worker", worker_policy),
        ],
    )

    assert decision.chosen_instance_id == "worker"
    assert [item["instance_id"] for item in decision.eligible_candidates] == [
        "worker"
    ]
    rejected = decision.rejected_candidates[0]
    assert rejected["instance_id"] == MACBOOK_INSTANCE_ID
    assert "workload_profile_denied" in rejected["rejection_codes"]
    assert rejected["policy_reason"] == macbook_policy.reason


def test_macbook_research_allowed_while_repository_denied(tmp_path: Path) -> None:
    policy = InstanceParticipationPolicy(
        instance_id=MACBOOK_INSTANCE_ID,
        denied_profiles=["repository"],
        allowed_profiles=["research", "operations"],
    )
    service = PlacementService(RoundRobinCursorStore(tmp_path))

    research = service.resolve(
        _request(PlacementPolicy.BEST_MATCH, workload="research"),
        [_candidate(MACBOOK_INSTANCE_ID, policy)],
    )
    assert research.chosen_instance_id == MACBOOK_INSTANCE_ID

    with pytest.raises(PlacementError) as denied:
        service.resolve(
            _request(PlacementPolicy.BEST_MATCH),
            [_candidate(MACBOOK_INSTANCE_ID, policy)],
        )
    assert "workload_profile_denied" in denied.value.rejected_candidates[0][
        "rejection_codes"
    ]


def test_manual_only_named_dispatch_cannot_bypass_workload_deny(
    tmp_path: Path,
) -> None:
    policy = InstanceParticipationPolicy(
        instance_id="manual",
        participation_mode=ParticipationMode.MANUAL_ONLY,
        automatic_dispatch=False,
        manual_dispatch=True,
        denied_profiles=["repository"],
    )
    service = PlacementService(RoundRobinCursorStore(tmp_path))
    candidate = _candidate("manual", policy)

    with pytest.raises(PlacementError) as automatic:
        service.resolve(
            _request(PlacementPolicy.BEST_MATCH, workload="research"),
            [candidate],
        )
    assert "automatic_participation_disabled" in automatic.value.rejected_candidates[
        0
    ]["rejection_codes"]

    manual_research = _request(
        PlacementPolicy.BEST_MATCH,
        workload="research",
        intent=DispatchIntent.MANUAL,
    )
    manual_research.policy = None
    manual_research.instance_id = "manual"
    assert service.resolve(manual_research, [candidate]).chosen_instance_id == "manual"

    manual_code = _request(
        PlacementPolicy.BEST_MATCH,
        intent=DispatchIntent.MANUAL,
    )
    manual_code.policy = None
    manual_code.instance_id = "manual"
    with pytest.raises(PlacementError) as code_denied:
        service.resolve(manual_code, [candidate])
    assert "workload_profile_denied" in code_denied.value.rejected_candidates[0][
        "rejection_codes"
    ]


def test_privileged_override_is_audited_but_hard_limits_still_win(
    tmp_path: Path,
) -> None:
    policy = InstanceParticipationPolicy(
        instance_id="restricted",
        participation_mode=ParticipationMode.DISABLED,
        automatic_dispatch=False,
        manual_dispatch=False,
        denied_profiles=["repository"],
        denied_project_ids=["project-1"],
        denied_repository_ids=["repo-1"],
    )
    request = _request(
        PlacementPolicy.BEST_MATCH,
        intent=DispatchIntent.PRIVILEGED_OVERRIDE,
    )
    request.policy = None
    request.instance_id = "restricted"
    request.participation_override_reason = "Emergency repair approved by operator"
    service = PlacementService(RoundRobinCursorStore(tmp_path))
    decision = service.resolve(request, [_candidate("restricted", policy)])
    assert decision.chosen_instance_id == "restricted"
    assert decision.dispatch_intent == "privileged_override"
    assert (
        decision.participation_override_reason
        == "Emergency repair approved by operator"
    )

    policy.hard_denied_profiles = ["repository"]
    with pytest.raises(PlacementError) as hard_denied:
        service.resolve(request, [_candidate("restricted", policy)])
    assert "self_protective_workload_denied" in hard_denied.value.rejected_candidates[
        0
    ]["rejection_codes"]


def test_project_repository_group_and_unknown_policy_denials_are_explainable(
    tmp_path: Path,
) -> None:
    policy = InstanceParticipationPolicy(
        instance_id="worker",
        denied_project_ids=["project-1"],
        denied_repository_ids=["repo-1"],
    )
    candidates = [
        _candidate("worker", policy),
        _candidate(
            "excluded",
            InstanceParticipationPolicy(instance_id="excluded"),
            group_membership="explicitly_excluded_from_group",
        ),
        _candidate(
            "mixed",
            InstanceParticipationPolicy(instance_id="mixed"),
            explicit=False,
            supported=False,
        ),
    ]
    with pytest.raises(PlacementError) as denied:
        PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
            _request(PlacementPolicy.LEAST_BUSY), candidates
        )
    codes = {
        item["instance_id"]: set(item["rejection_codes"])
        for item in denied.value.rejected_candidates
    }
    assert {"project_not_allowed", "repository_not_allowed"} <= codes["worker"]
    assert "explicitly_excluded_from_group" in codes["excluded"]
    assert "policy_unknown_on_mixed_version_peer" in codes["mixed"]
    assert "will not fall back to the authority/local instance" in denied.value.message


def test_local_compatibility_derived_instance_is_automatic_eligible(
    tmp_path: Path,
) -> None:
    local = _candidate(
        "local",
        compatibility_policy("local"),
        explicit=False,
        supported=True,
    )
    local.local = True
    decision = PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
        _request(PlacementPolicy.BEST_MATCH, workload="research"),
        [local],
    )
    assert decision.chosen_instance_id == "local"
    assert decision.eligible_candidates[0]["instance_id"] == "local"


def test_remote_compatibility_derived_peer_stays_unknown_under_enforcement(
    tmp_path: Path,
) -> None:
    remote = _candidate(
        "peer",
        compatibility_policy("peer"),
        explicit=False,
        supported=True,
    )
    with pytest.raises(PlacementError) as denied:
        PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
            _request(PlacementPolicy.BEST_MATCH, workload="research"),
            [remote],
        )
    assert "policy_unknown_on_mixed_version_peer" in denied.value.rejected_candidates[
        0
    ]["rejection_codes"]


def test_durable_groups_policies_defaults_audit_and_rebuild(
    tmp_path: Path,
) -> None:
    reset_store()
    reset_settings()
    settings = Settings(
        data_dir=tmp_path,
        instance_id="authority",
        instance_name="authority",
        instance_url="http://authority",
        subscribed_realms=["default"],
    )
    store = get_store(settings)
    try:
        group = store.create_instance_group(
            InstanceGroupCreate(
                name="PA code workers",
                included_instance_ids=["worker", MACBOOK_INSTANCE_ID],
                excluded_instance_ids=[MACBOOK_INSTANCE_ID],
            ),
            principal_id="user:admin",
            instance_id="authority",
        )
        policy = store.set_instance_participation_policy(
            InstanceParticipationPolicy(
                instance_id=MACBOOK_INSTANCE_ID,
                denied_profiles=["repository"],
            ),
            principal_id="user:admin",
            instance_id="authority",
        )
        default = store.set_placement_default(
            PlacementDefault(
                project_id="project-1",
                workload_profile="repository",
                group_id=group.id,
            ),
            principal_id="user:admin",
            instance_id="authority",
        )

        assert policy.version == 1
        assert default.scope_key == "project:project-1:profile:repository"
        assert len(store.list_fleet_policy_audit("default")) == 3

        store.rebuild_from_log("default")
        rebuilt = store.get_instance_group(group.id, "default")
        assert rebuilt and rebuilt.excluded_instance_ids == [MACBOOK_INSTANCE_ID]
        assert (
            store.get_instance_participation_policy(
                MACBOOK_INSTANCE_ID, "default"
            ).denied_profiles
            == ["repository"]
        )
        assert store.list_placement_defaults("default")[0].group_id == group.id
    finally:
        reset_store()
        reset_settings()


def test_default_precedence_group_expansion_and_migration(tmp_path: Path) -> None:
    reset_store()
    reset_settings()
    settings = Settings(
        data_dir=tmp_path,
        instance_id="authority",
        instance_name="authority",
        instance_url="http://authority",
        subscribed_realms=["default"],
    )
    store = get_store(settings)
    service = FleetPolicyService(store)
    try:
        realm_group = store.create_instance_group(
            InstanceGroupCreate(name="Realm workers", included_instance_ids=["one"]),
            principal_id="user:admin",
            instance_id="authority",
        )
        project_group = store.create_instance_group(
            InstanceGroupCreate(
                name="Project code",
                included_instance_ids=["one", "two"],
                excluded_instance_ids=["one"],
            ),
            principal_id="user:admin",
            instance_id="authority",
        )
        store.set_placement_default(
            PlacementDefault(group_id=realm_group.id),
            principal_id="user:admin",
            instance_id="authority",
        )
        store.set_placement_default(
            PlacementDefault(
                project_id="project-1",
                workload_profile="repository",
                group_id=project_group.id,
            ),
            principal_id="user:admin",
            instance_id="authority",
        )
        assert service.resolve_default_group(
            realm_id="default",
            project_id="project-1",
            workload_profile="repository",
            requested_group_id=None,
        ) == (project_group.id, "project_profile_default")
        assert service.resolve_default_group(
            realm_id="default",
            project_id="project-1",
            workload_profile="research",
            requested_group_id=None,
        ) == (realm_group.id, "realm_default")

        one = _candidate(
            "one", InstanceParticipationPolicy(instance_id="one")
        )
        two = _candidate(
            "two", InstanceParticipationPolicy(instance_id="two")
        )
        resolution = service.resolve_group(
            realm_id="default",
            project_id="project-1",
            workload_profile="repository",
            requested_group_id=None,
            candidates=[one, two],
            local_instance_id="one",
        )
        assert resolution.included_instance_ids == ["two"]
        assert (
            resolution.membership["one"]
            == "explicitly_excluded_from_group"
        )

        migration = service.migrate(
            realm_id="default",
            instances=[
                FleetInstance(
                    instance_id=MACBOOK_INSTANCE_ID,
                    name="renamed-mac",
                    url="http://mac",
                ),
                FleetInstance(instance_id="two", name="worker", url="http://two"),
            ],
            actor="user:admin",
            author_instance="authority",
            apply=True,
        )
        assert migration["applied"] is True
        macbook = store.get_instance_participation_policy(
            MACBOOK_INSTANCE_ID, "default"
        )
        assert macbook and macbook.denied_profiles == ["repository"]
        assert macbook.authority_enabled and macbook.sync_enabled
        assert store.get_instance_group(project_group.id, "default") is not None
    finally:
        reset_store()
        reset_settings()


def test_authenticated_api_and_accessible_ui_expose_configuration_and_audit(
    tmp_path: Path,
) -> None:
    reset_store()
    reset_settings()
    settings = Settings(
        data_dir=tmp_path,
        instance_id="local",
        instance_name="Local",
        instance_url="http://pa.test:8080",
        agent_enabled=False,
        subscribed_realms=["default"],
        peers=[],
    )
    app = Kernel.boot(settings=settings).build_app()
    try:
        with TestClient(app) as client:
            assert client.get("/").status_code == 200
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
            groups = client.get("/api/fleet/instance-groups").json()
            assert {"all-active", "automatic-workers", "code-workers"} <= {
                group["id"] for group in groups
            }

            created = client.post(
                "/api/fleet/instance-groups",
                headers=headers,
                json={
                    "name": "Local research",
                    "included_instance_ids": ["local"],
                },
            )
            assert created.status_code == 201, created.text
            group_id = created.json()["id"]

            policy = client.put(
                "/api/fleet/instances/local/participation-policy",
                headers=headers,
                json={
                    "denied_profiles": ["repository"],
                    "allowed_profiles": ["research", "operations"],
                    "reason": "interactive host",
                },
            )
            assert policy.status_code == 200, policy.text
            assert policy.json()["summary"] == "Custom participation"

            unconfirmed_enable = client.put(
                "/api/fleet/instances/local/participation-policy",
                headers=headers,
                json={
                    "denied_profiles": [],
                    "allowed_profiles": [
                        "repository",
                        "research",
                        "operations",
                    ],
                },
            )
            assert unconfirmed_enable.status_code == 409
            assert (
                unconfirmed_enable.json()["detail"]["code"]
                == "participation_enable_confirmation_required"
            )
            confirmed_enable = client.put(
                "/api/fleet/instances/local/participation-policy",
                headers=headers,
                json={
                    "denied_profiles": [],
                    "allowed_profiles": [
                        "repository",
                        "research",
                        "operations",
                    ],
                    "confirm_enable": True,
                    "confirmation_reason": "Approved for temporary code work",
                },
            )
            assert confirmed_enable.status_code == 200
            assert (
                confirmed_enable.json()["enablement_confirmation_reason"]
                == "Approved for temporary code work"
            )

            default = client.put(
                "/api/fleet/placement-defaults",
                headers=headers,
                json={"group_id": group_id, "workload_profile": "research"},
            )
            assert default.status_code == 200, default.text
            assert default.json()["scope_key"] == "project:*:profile:research"

            edited = client.patch(
                f"/api/fleet/instance-groups/{group_id}",
                headers=headers,
                json={
                    "excluded_instance_ids": ["local"],
                    "expected_version": created.json()["version"],
                },
            )
            assert edited.status_code == 200, edited.text
            assert edited.json()["excluded_instance_ids"] == ["local"]

            preview = client.post(
                "/api/fleet/placement/preview",
                headers=headers,
                json={
                    "placement_policy": "best_match",
                    "group_id": group_id,
                    "execution_contract": {
                        "version": 1,
                        "profile": "research",
                        "confirmed": True,
                        "requirements": {},
                    },
                },
            )
            assert preview.status_code == 409, preview.text
            detail = preview.json()["detail"]
            assert detail["code"] == "no_eligible_instance"
            assert "explicitly_excluded_from_group" in detail[
                "rejected_candidates"
            ][0]["rejection_codes"]

            audit = client.get("/api/fleet/policy-audit")
            assert audit.status_code == 200
            assert {"instance_group", "instance_participation_policy", "placement_default"} <= {
                entry["entity_type"] for entry in audit.json()
            }

            fleet_page = client.get("/fleet?section=participation")
            assert fleet_page.status_code == 200
            assert "Groups and participation" in fleet_page.text
            assert "Privileged named-instance override" not in fleet_page.text

            project = client.post(
                "/api/projects",
                headers=headers,
                json={"realm_id": "default", "title": "Policy project"},
            )
            assert project.status_code == 201, project.text
            project_page = client.get(
                f"/projects?realm=default&project={project.json()['id']}"
            )
            assert "Worker group defaults" in project_page.text
    finally:
        reset_store()
        reset_settings()
