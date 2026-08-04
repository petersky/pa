from types import SimpleNamespace

import pytest

from pa.execution.profiles import (
    ExecutionContract,
    ExecutionProfile,
    ExecutionRequirements,
    RepositoryRequirement,
    resolve_materialization_plan,
    validate_execution_contract,
)
from pa.workloads import LEGACY_CODE_PROFILE_REASON, WorkloadProfileError


def repo(identifier: str = "repo-1"):
    return SimpleNamespace(
        id=identifier,
        name="PA",
        url="https://github.com/petersky/pa",
        default_branch="main",
    )


def test_project_repository_defaults_to_code_workspace():
    repository = repo()
    plan = resolve_materialization_plan(
        requested=None,
        card=SimpleNamespace(id="card-1"),
        project=SimpleNamespace(tool_config={}),
        project_repositories=[(repository, SimpleNamespace(branch="main"))],
        explicit_repositories=[],
        target_instance_id="monica",
    )
    assert plan.admissible
    assert plan.profile == ExecutionProfile.REPOSITORY
    assert plan.profile_source == "declared_resources"
    assert plan.workspace["leased"] is True
    assert plan.repositories[0]["repository_id"] == repository.id


def test_unlinked_legacy_card_requires_visible_confirmation():
    plan = resolve_materialization_plan(
        requested=None,
        card=SimpleNamespace(id="card-1"),
        project=None,
        project_repositories=[],
        explicit_repositories=[],
        target_instance_id="monica",
    )
    assert not plan.admissible
    assert plan.confirmation_required
    assert plan.summary == "Research workspace: no Git repository required"


def test_confirmed_research_card_is_intentionally_non_git():
    plan = resolve_materialization_plan(
        requested=ExecutionContract(profile="research", confirmed=True),
        card=SimpleNamespace(id="card-1"),
        project=SimpleNamespace(tool_config={}),
        project_repositories=[(repo(), SimpleNamespace(branch="main"))],
        explicit_repositories=[],
        target_instance_id="monica",
    )
    assert plan.admissible
    assert plan.profile == ExecutionProfile.RESEARCH
    assert plan.repositories
    assert plan.workspace["kind"] == "artifact"


def test_integration_deliverable_cannot_use_non_git_profile():
    plan = resolve_materialization_plan(
        requested=ExecutionContract(
            profile="operations",
            confirmed=True,
            requirements=ExecutionRequirements(expected_deliverables=["pull_request"]),
        ),
        card=SimpleNamespace(id="card-1"),
        project=None,
        project_repositories=[],
        explicit_repositories=[],
        target_instance_id="monica",
    )
    assert not plan.admissible
    assert plan.missing_dependencies[0]["resource"] == "execution_profile"


def test_explicit_repository_without_project_is_supported():
    repository = repo()
    contract = ExecutionContract(
        profile="repository",
        requirements=ExecutionRequirements(
            repositories=[RepositoryRequirement(repository_id=repository.id)],
            expected_deliverables=["commit"],
        ),
    )
    plan = resolve_materialization_plan(
        requested=contract,
        card=SimpleNamespace(id="card-1"),
        project=None,
        project_repositories=[],
        explicit_repositories=[repository],
        target_instance_id="monica",
    )
    assert plan.admissible
    assert plan.profile_source == "dispatch_override"
    assert plan.repositories[0]["branch"] == "main"


def test_multiple_repositories_and_missing_identity_are_reported():
    repository = repo()
    contract = ExecutionContract(
        profile="repository",
        requirements=ExecutionRequirements(
            repositories=[
                RepositoryRequirement(repository_id=repository.id),
                RepositoryRequirement(repository_id="missing"),
            ]
        ),
    )
    plan = resolve_materialization_plan(
        requested=contract,
        card=None,
        project=None,
        project_repositories=[],
        explicit_repositories=[repository],
        target_instance_id="monica",
    )
    assert not plan.admissible
    assert len(plan.repositories) == 1
    assert plan.missing_dependencies[0]["resource"] == "repository:missing"


def test_legacy_code_contract_normalizes_without_rewriting_persisted_input():
    persisted = {
        "version": 1,
        "profile": "code",
        "confirmed": True,
        "requirements": {},
    }
    project = SimpleNamespace(tool_config={"execution_contract": persisted})

    plan = resolve_materialization_plan(
        requested=None,
        card=SimpleNamespace(id="legacy-card"),
        project=project,
        project_repositories=[(repo(), SimpleNamespace(branch="main"))],
        explicit_repositories=[],
        target_instance_id="worker",
    )

    assert plan.profile == ExecutionProfile.REPOSITORY
    assert plan.profile_normalization_reason == LEGACY_CODE_PROFILE_REASON
    assert persisted["profile"] == "code"


@pytest.mark.parametrize(
    ("requested", "resolved"),
    [
        ("automatic", ExecutionProfile.RESEARCH),
        ("repository", ExecutionProfile.REPOSITORY),
        ("research", ExecutionProfile.RESEARCH),
        ("operations", ExecutionProfile.OPERATIONS),
        ("code", ExecutionProfile.REPOSITORY),
    ],
)
def test_every_supported_and_legacy_contract_profile_resolves(requested, resolved):
    repositories = (
        [(repo(), SimpleNamespace(branch="main"))]
        if requested in {"repository", "code"}
        else []
    )
    plan = resolve_materialization_plan(
        requested=ExecutionContract(
            profile=requested,
            confirmed=True,
        ),
        card=None,
        project=SimpleNamespace(tool_config={}),
        project_repositories=repositories,
        explicit_repositories=[],
        target_instance_id="worker",
    )
    assert plan.profile == resolved


def test_unknown_persisted_contract_profile_has_actionable_typed_error():
    with pytest.raises(WorkloadProfileError) as raised:
        validate_execution_contract({"version": 1, "profile": "not-real"})
    assert raised.value.detail()["code"] == "invalid_workload_profile"
    assert raised.value.detail()["legacy_aliases"] == {"code": "repository"}
