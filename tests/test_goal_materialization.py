from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError

from pa.domain.models import CardAttachment
from pa.execution.dispatch import (
    DispatchRecord,
    GoalDispatchProvenance,
    goal_dispatch_execution_identity_valid,
    goal_dispatch_materialization_binding_valid,
)
from pa.execution.profiles import MaterializationPlan
from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
    GoalMaterializationResourceClaimV1,
    canonical_materialization_digest,
)
from pa.modules.fleet import (
    DispatchMaterializeBody,
    _target_goal_materialization_binding_valid,
)


class GoalMaterializationTests(unittest.TestCase):
    @staticmethod
    def _plan(target: str = "instance-b") -> dict[str, object]:
        return {
            "contract_version": 1,
            "profile": "repository",
            "profile_source": "dispatch_override",
            "requirements": {
                "repository_required": True,
                "repositories": [
                    {
                        "repository_id": "repo-a",
                        "branch": None,
                        "base_ref": None,
                        "worktree_required": True,
                    }
                ],
                "attachments": False,
                "browser": False,
                "external_tools": [],
                "required_capabilities": [],
                "writable_artifact_workspace": True,
                "network_policy": "provider-default",
                "expected_deliverables": [],
            },
            "target_instance_id": target,
            "repositories": [{"repository_id": "repo-a"}],
            "workspace": {"kind": "repository"},
            "missing_dependencies": [],
            "stale_dependencies": [],
            "confirmation_required": False,
            "summary": "Canonical repository materialization.",
        }

    @staticmethod
    def _contract() -> dict[str, object]:
        return {
            "version": 1,
            "profile": "repository",
            "requirements": {
                "repository_required": True,
                "repositories": [{"repository_id": "repo-a"}],
            },
            "confirmed": True,
        }

    @classmethod
    def _bindings(
        cls,
    ) -> tuple[
        GoalMaterializationEnvelopeV1,
        GoalMaterializationReceiptV1,
    ]:
        envelope = GoalMaterializationEnvelopeV1(
            work_package_id="package-a",
            service_role="executor",
            repository_ids=("repo-a",),
            data_scopes=("scope-b", "scope-a"),
            attachment_ids=("attachment-b", "attachment-a"),
            attachment_classes=("text/plain", "application/pdf"),
            resource_claims=(
                GoalMaterializationResourceClaimV1(key="repository:repo-a"),
                GoalMaterializationResourceClaimV1(key="fleet-dispatch:instance-b"),
            ),
            execution_contract_digest=canonical_materialization_digest(cls._contract()),
        )
        receipt = GoalMaterializationReceiptV1(
            envelope_digest=str(envelope.digest),
            target_instance_id="instance-b",
            provider_id="codex",
            model_id="gpt-5",
            mode_id="default",
            materialization_plan_digest=canonical_materialization_digest(cls._plan()),
        )
        return envelope, receipt

    @classmethod
    def _record(cls) -> DispatchRecord:
        envelope, receipt = cls._bindings()
        provenance = GoalDispatchProvenance(
            goal_id="goal-a",
            goal_version=1,
            policy_revision=1,
            authority_instance_id="instance-a",
            fencing_token=3,
            action_reservation_id="reservation-a",
            operation_key="operation-a",
            requested_placement_target="instance-b",
            placement_input_digest="a" * 64,
            resolved_target_instance_id="instance-b",
            placement_decision_digest="b" * 64,
            materialization_envelope=envelope,
            materialization_receipt=receipt,
            actor_principal="service:goal-supervisor:instance-a",
            provider_id="codex",
        )
        return DispatchRecord(
            mutation_id="mutation-a",
            idempotency_key="operation-a",
            request_payload={
                "provider": "codex",
                "model_id": "gpt-5",
                "mode_id": "default",
                "execution_contract": cls._contract(),
            },
            materialization_plan=cls._plan(),
            goal_provenance=provenance,
            authority_instance_id="instance-a",
            authority_url="http://instance-a",
            target_instance_id="instance-b",
        )

    def test_envelope_is_canonical_immutable_and_digest_verified(self) -> None:
        first, _receipt = self._bindings()
        second = GoalMaterializationEnvelopeV1(
            work_package_id="package-a",
            service_role="executor",
            repository_ids=("repo-a", "repo-a"),
            data_scopes=("scope-a", "scope-b"),
            attachment_ids=("attachment-a", "attachment-b"),
            attachment_classes=("application/pdf", "text/plain"),
            resource_claims=tuple(reversed(first.resource_claims)),
            execution_contract_digest=first.execution_contract_digest,
        )
        self.assertEqual(first, second)
        self.assertEqual(first.digest, second.digest)
        changed_role = GoalMaterializationEnvelopeV1.model_validate(
            {
                **first.model_dump(mode="python", exclude={"digest"}),
                "service_role": "verifier",
            }
        )
        self.assertNotEqual(first.digest, changed_role.digest)
        with self.assertRaisesRegex(ValidationError, "digest does not match"):
            GoalMaterializationEnvelopeV1(
                work_package_id="package-a",
                service_role="executor",
                repository_ids=("repo-a",),
                execution_contract_digest="c" * 64,
                digest="d" * 64,
            )
        with self.assertRaises(ValidationError):
            GoalMaterializationEnvelopeV1(
                work_package_id="package-a",
                service_role="executor",
                repository_ids=("   ",),
                execution_contract_digest="c" * 64,
            )
        with self.assertRaises(ValidationError):
            first.repository_ids = ("repo-b",)

    def test_plan_or_launch_selector_mutation_invalidates_receipt(self) -> None:
        record = self._record()
        self.assertTrue(goal_dispatch_materialization_binding_valid(record))
        record.materialization_plan = {
            **record.materialization_plan,
            "summary": "mutated",
        }
        self.assertFalse(goal_dispatch_materialization_binding_valid(record))

        record = self._record()
        record.request_payload["model_id"] = "different-model"
        self.assertFalse(goal_dispatch_materialization_binding_valid(record))

        record = self._record()
        record.request_payload["execution_contract"] = None
        self.assertFalse(goal_dispatch_materialization_binding_valid(record))

    def test_execution_identity_requires_exact_session_and_real_credential(
        self,
    ) -> None:
        record = self._record()
        record.session_id = "session-a"
        self.assertFalse(goal_dispatch_execution_identity_valid(record))
        envelope, receipt = self._bindings()
        identity = GoalExecutionIdentityV1(
            work_package_id=envelope.work_package_id,
            service_role=envelope.service_role,
            assigned_service_principal="service:goal-executor:principal-a",
            provider_id="codex",
            target_instance_id="instance-b",
            session_id="session-a",
            fencing_token=3,
            materialization_receipt_digest=str(receipt.digest),
        )
        assert record.goal_provenance is not None
        record.goal_provenance.execution_identity = identity
        self.assertTrue(goal_dispatch_execution_identity_valid(record))
        self.assertFalse(
            goal_dispatch_execution_identity_valid(
                record,
                require_authenticated_credential=True,
            )
        )
        self.assertFalse(identity.credential_authenticated())
        self.assertNotIn("token", identity.model_dump(mode="json"))
        with self.assertRaisesRegex(ValidationError, "canonical service role"):
            GoalExecutionIdentityV1.model_validate(
                {
                    **identity.model_dump(mode="python", exclude={"digest"}),
                    "service_role": "verifier",
                }
            )
        with self.assertRaises(ValidationError):
            GoalExecutionIdentityV1.model_validate(
                {
                    **identity.model_dump(mode="json"),
                    "credential_token": "must-never-be-persisted",
                }
            )

        authenticated = GoalExecutionIdentityV1(
            **identity.model_dump(
                mode="python",
                exclude={
                    "digest",
                    "credential_digest",
                    "credential_expires_at",
                },
            ),
            credential_digest="e" * 64,
            credential_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        record.goal_provenance.execution_identity = authenticated
        self.assertTrue(
            goal_dispatch_execution_identity_valid(
                record,
                require_authenticated_credential=True,
            )
        )
        self.assertEqual(authenticated.materialization_receipt_digest, receipt.digest)
        self.assertEqual(envelope.digest, receipt.envelope_digest)

    def test_receipt_rejects_caller_supplied_digest_and_blank_ids(self) -> None:
        envelope, receipt = self._bindings()
        with self.assertRaisesRegex(ValidationError, "digest does not match"):
            GoalMaterializationReceiptV1(
                **receipt.model_dump(mode="python", exclude={"digest"}),
                digest="f" * 64,
            )
        with self.assertRaises(ValidationError):
            GoalMaterializationReceiptV1(
                envelope_digest=str(envelope.digest),
                target_instance_id="   ",
                provider_id="codex",
                materialization_plan_digest="f" * 64,
            )

    def test_target_recomputes_the_exact_materialization_binding(self) -> None:
        envelope, receipt = self._bindings()
        provenance = GoalDispatchProvenance(
            goal_id="goal-a",
            goal_version=1,
            policy_revision=1,
            authority_instance_id="instance-a",
            fencing_token=3,
            action_reservation_id="reservation-a",
            requested_placement_target="instance-b",
            resolved_target_instance_id="instance-b",
            materialization_envelope=envelope,
            materialization_receipt=receipt,
            actor_principal="service:goal-supervisor:instance-a",
            provider_id="codex",
        )
        attachments = [
            CardAttachment(
                attachment_id="attachment-a",
                card_id="card-a",
                filename="evidence.txt",
                media_type="text/plain",
                size=1,
                sha256="a" * 64,
                blob_ref=f"sha256:{'a' * 64}",
                created_by_principal="user:operator",
                created_by_instance="instance-a",
            ),
            CardAttachment(
                attachment_id="attachment-b",
                card_id="card-a",
                filename="evidence.pdf",
                media_type="application/pdf",
                size=1,
                sha256="b" * 64,
                blob_ref=f"sha256:{'b' * 64}",
                created_by_principal="user:operator",
                created_by_instance="instance-a",
            ),
        ]
        body = DispatchMaterializeBody(
            dispatch_id="dispatch-a",
            mutation_id="mutation-a",
            realm_id="default",
            authority_instance_id="instance-a",
            authority_url="http://instance-a",
            target_instance_id="instance-b",
            provider="codex",
            model_id="gpt-5",
            mode_id="default",
            execution_contract=self._contract(),
            attachment_manifest=attachments,
            materialization_plan=self._plan(),
            goal_provenance=provenance,
        )
        plan = MaterializationPlan.model_validate(body.materialization_plan)
        self.assertTrue(_target_goal_materialization_binding_valid(body, plan))
        self.assertFalse(
            _target_goal_materialization_binding_valid(
                body.model_copy(update={"attachment_manifest": attachments[:1]}),
                plan,
            )
        )
        self.assertFalse(
            _target_goal_materialization_binding_valid(
                body.model_copy(update={"mode_id": "changed-mode"}),
                plan,
            )
        )
        changed_plan = plan.model_copy(update={"summary": "changed plan"})
        self.assertFalse(_target_goal_materialization_binding_valid(body, changed_plan))


if __name__ == "__main__":
    unittest.main()
