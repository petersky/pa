"""Durable goal domain."""

from pa.goals.governance import GoalGovernanceService
from pa.goals.models import Goal
from pa.goals.service import GoalService

__all__ = ["Goal", "GoalGovernanceService", "GoalService"]
