"""
BackendRouter — select the best backend for a quantum job.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..contracts.backend_profile import BackendProfile, BackendKind
from ..contracts.execution_request import BackendPreference

logger = logging.getLogger(__name__)


@dataclass
class RoutingDecision:
    """The router's recommendation for a specific request."""
    backend_id: Optional[str]
    rationale: str
    rejected: List[Dict[str, Any]] = field(default_factory=list)
    fallback_to_classical: bool = False


class BackendRouter:
    """
    Selects the best backend given a request and a registry of backends.

    Selection order:
      1. Filter by capability (online, enough qubits, enough shots).
      2. Filter by preference (simulator vs. hardware).
      3. Score remaining candidates on (quality, cost, latency).
      4. Return the best, with rejected alternatives documented.
    """

    def __init__(
        self,
        *,
        prefer_simulator: bool = True,
        max_cost_per_job: Optional[float] = None,
    ):
        self.prefer_simulator = prefer_simulator
        self.max_cost_per_job = max_cost_per_job
        self._registry: Dict[str, BackendProfile] = {}

    def register(self, profile: BackendProfile) -> None:
        self._registry[profile.backend_id] = profile

    def list_backends(self) -> List[BackendProfile]:
        return list(self._registry.values())

    def select(
        self,
        *,
        required_qubits: int,
        required_shots: int,
        preference: str = BackendPreference.AUTO.value,
        max_queue_seconds: float = 300.0,
        max_cost: Optional[float] = None,
    ) -> RoutingDecision:
        """Select the best backend, or return fallback_to_classical."""
        candidates: List[BackendProfile] = []
        rejected: List[Dict[str, Any]] = []

        for profile in self._registry.values():
            if not profile.can_handle(required_qubits, required_shots):
                rejected.append({
                    "backend": profile.backend_id,
                    "reason": "insufficient capabilities",
                })
                continue
            if profile.avg_queue_seconds > max_queue_seconds:
                rejected.append({
                    "backend": profile.backend_id,
                    "reason": f"queue {profile.avg_queue_seconds}s exceeds budget",
                })
                continue
            if max_cost is not None and max_cost > 0:
                job_cost = profile.cost_per_shot * required_shots
                if job_cost > max_cost:
                    rejected.append({
                        "backend": profile.backend_id,
                        "reason": f"cost ${job_cost:.4f} exceeds budget",
                    })
                    continue
            if not self._preference_matches(profile, preference):
                rejected.append({
                    "backend": profile.backend_id,
                    "reason": f"preference '{preference}' mismatch",
                })
                continue
            candidates.append(profile)

        if not candidates:
            return RoutingDecision(
                backend_id=None,
                rationale="no backend satisfied constraints",
                rejected=rejected,
                fallback_to_classical=True,
            )

        best = max(candidates, key=self._score)
        rejected.extend({
            "backend": c.backend_id,
            "reason": f"lower score than {best.backend_id}",
        } for c in candidates if c is not best)

        return RoutingDecision(
            backend_id=best.backend_id,
            rationale=(
                f"selected '{best.backend_id}' "
                f"({best.kind}, {best.technology}) "
                f"fidelity={best.gate_fidelity}, "
                f"queue={best.avg_queue_seconds}s"
            ),
            rejected=rejected,
        )

    def _preference_matches(
        self, profile: BackendProfile, preference: str,
    ) -> bool:
        if preference == BackendPreference.AUTO.value:
            return True
        if preference == BackendPreference.SIMULATOR.value:
            return profile.kind in (
                BackendKind.SIMULATOR.value,
                BackendKind.CLOUD_SIMULATOR.value,
                BackendKind.EMULATOR.value,
            )
        if preference == BackendPreference.HARDWARE.value:
            return profile.kind == BackendKind.HARDWARE.value
        return False

    def _score(self, profile: BackendProfile) -> float:
        fidelity = profile.gate_fidelity or 0.5
        queue_penalty = min(1.0, profile.avg_queue_seconds / 600.0)
        cost_penalty = min(1.0, profile.cost_per_shot * 100.0)
        simulator_bonus = (
            0.1 if self.prefer_simulator
            and profile.kind == BackendKind.SIMULATOR.value else 0.0
        )
        return fidelity - queue_penalty - cost_penalty + simulator_bonus
