"""Provider-native goal invocation adapters.

Adapters only translate a bounded PA assignment. They never own canonical goal
state or gain authority from a provider's session-scoped goal loop.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pa.goals.advanced_models import (
    ProviderGoalAssignment,
    ProviderGoalCapabilities,
    ProviderGoalInvocation,
    ProviderGoalMode,
)
from pa.goals.models import Goal


@runtime_checkable
class ProviderGoalAdapter(Protocol):
    @property
    def capabilities(self) -> ProviderGoalCapabilities: ...

    def prepare(
        self, goal: Goal, assignment: ProviderGoalAssignment
    ) -> ProviderGoalInvocation: ...


def _goal_packet(goal: Goal, strategy_id: str | None) -> dict:
    return {
        "contract_version": 1,
        "goal_id": goal.id,
        "objective": goal.objective,
        "motivation": goal.motivation,
        "constraints": goal.constraints,
        "non_goals": goal.non_goals,
        "criteria": [
            {
                "id": item.id,
                "description": item.description,
                "verification_method": item.verification_method,
                "evidence_requirement": item.evidence_requirement,
            }
            for item in goal.criteria
        ],
        "strategy_id": strategy_id,
        "goal_version": goal.version,
        "policy_revision": goal.policy.revision,
        "budget": goal.budget.model_dump(mode="json"),
        "reporting": {
            "progress_contract_version": 1,
            "canonical_owner": "pa",
            "provider_completion_is_evidence_claim_only": True,
        },
    }


def _assignment_prompt(goal: Goal) -> str:
    criteria = "\n".join(
        f"- [{item.id}] {item.description}; verify with {item.verification_method}"
        for item in goal.criteria
    )
    constraints = "\n".join(f"- {item}" for item in goal.constraints) or "- none"
    return (
        f"Pursue the bounded PA goal {goal.id}: {goal.objective}\n\n"
        f"Success criteria:\n{criteria}\n\nConstraints:\n{constraints}\n\n"
        "Report progress, blockers, interactions, artifacts, usage, and completion "
        "claims through PA's canonical progress contract. Do not expand authority, "
        "change the success criteria, or treat provider completion as final audit."
    )


class CommandProviderGoalAdapter:
    def __init__(
        self,
        provider_id: str,
        *,
        native_command_candidates: tuple[str, ...] = ("goal",),
        native_capable: bool = True,
    ) -> None:
        self._capabilities = ProviderGoalCapabilities(
            provider_id=provider_id,
            native_command_candidates=list(native_command_candidates),
            supports_native_goal=native_capable,
        )

    @property
    def capabilities(self) -> ProviderGoalCapabilities:
        return self._capabilities

    def prepare(
        self, goal: Goal, assignment: ProviderGoalAssignment
    ) -> ProviderGoalInvocation:
        advertised = {item.removeprefix("/") for item in assignment.available_commands}
        command = next(
            (
                candidate
                for candidate in self.capabilities.native_command_candidates
                if candidate.removeprefix("/") in advertised
            ),
            None,
        )
        native = bool(command and self.capabilities.supports_native_goal)
        if not native and not (
            self.capabilities.supports_recoverable_turns
            and assignment.supports_session_load
        ):
            raise ValueError(
                f"provider {assignment.provider_id!r} exposes neither a native goal "
                "command nor recoverable session turns"
            )
        packet = _goal_packet(goal, assignment.strategy_id)
        return ProviderGoalInvocation(
            provider_id=assignment.provider_id,
            mode=(
                ProviderGoalMode.NATIVE if native else ProviderGoalMode.RECOVERABLE_TURN
            ),
            command_name=command if native else None,
            arguments={"goal_packet": packet} if native else {},
            prompt=_assignment_prompt(goal),
            canonical_goal_id=goal.id,
            policy_revision=goal.policy.revision,
            metadata={
                "goal_packet": packet,
                "fallback_reason": (
                    None
                    if native
                    else "native goal command was not advertised by the live provider"
                ),
            },
        )


_ADAPTERS: dict[str, ProviderGoalAdapter] = {}


def _ensure_builtins() -> None:
    if _ADAPTERS:
        return
    for provider_id in ("codex", "claude", "kimi"):
        _ADAPTERS[provider_id] = CommandProviderGoalAdapter(provider_id)
    for provider_id in ("cursor", "openinterpreter"):
        _ADAPTERS[provider_id] = CommandProviderGoalAdapter(
            provider_id, native_capable=False
        )


def register_goal_adapter(adapter: ProviderGoalAdapter) -> None:
    _ensure_builtins()
    _ADAPTERS[adapter.capabilities.provider_id] = adapter


def get_goal_adapter(provider_id: str) -> ProviderGoalAdapter:
    _ensure_builtins()
    normalized = provider_id.strip().lower()
    return _ADAPTERS.get(normalized) or CommandProviderGoalAdapter(
        normalized, native_capable=False
    )


def list_goal_adapter_capabilities() -> list[ProviderGoalCapabilities]:
    _ensure_builtins()
    return [_ADAPTERS[key].capabilities for key in sorted(_ADAPTERS)]
