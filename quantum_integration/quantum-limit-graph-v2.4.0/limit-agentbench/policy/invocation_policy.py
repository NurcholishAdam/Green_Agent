"""
policy/invocation_policy.py — Canonical escalation logic.

Precedence (highest first):
    1. Criticality: domain in {safety, security} OR criticality_level in
       {critical, high} → escalate.
    2. Graph centrality ≥ 0.9 → escalate regardless.
    3. Sustainability: energy_wh > threshold → escalate.
    4. Confidence: confidence < threshold → escalate.
    5. RLHF feedback < 0.3 → escalate (optional signal).
    6. MODP tie-breaker — only when steps 1–5 say "no" (guarded by
       enable_modp_override).

`decide_escalation` is synchronous by design — it is a pure function.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from runtime.settings import Settings

__all__ = [
    "EscalationReason", "EscalationDecision", "SelectiveInvocationPolicy",
]


class EscalationReason(str, Enum):
    CONFIDENCE_LOW        = "confidence_low"
    SUSTAINABILITY_HIGH   = "sustainability_high"
    CRITICAL_DOMAIN       = "critical_domain"
    CRITICAL_LEVEL        = "critical_level"
    GRAPH_CENTRALITY_HIGH = "graph_centrality_high"
    RLHF_LOW              = "rlhf_low"
    MODP_OVERRIDE         = "modp_override"


_CRITICAL_DOMAINS = {"safety", "security"}
_CRITICAL_LEVELS  = {"critical", "high"}


@dataclass
class EscalationDecision:
    should_escalate: bool
    reasons: list[EscalationReason] = field(default_factory=list)
    modp_score: float | None = None

    def explain(self) -> str:
        if not self.should_escalate:
            return "not escalated"
        return "escalate: " + ", ".join(r.value for r in self.reasons)


class SelectiveInvocationPolicy:
    def __init__(
        self,
        settings: Settings,
        *,
        enable_criticality_check: bool = True,
        enable_sustainability_check: bool = True,
        enable_graph_check: bool = True,
        enable_modp_override: bool = False,
    ) -> None:
        self._s = settings
        self._crit = enable_criticality_check
        self._sust = enable_sustainability_check
        self._graph = enable_graph_check
        self._modp = enable_modp_override
        self._weights = settings.modp_weights   # validated to sum 1.0

    def decide_escalation(
        self,
        *,
        task: str,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        criticality_level: str | None = None,
        graph_metrics: dict[str, float] | None = None,
        human_feedback_score: float | None = None,
    ) -> EscalationDecision:
        reasons: list[EscalationReason] = []

        if self._crit:
            if domain in _CRITICAL_DOMAINS:
                reasons.append(EscalationReason.CRITICAL_DOMAIN)
            if criticality_level and criticality_level in _CRITICAL_LEVELS:
                reasons.append(EscalationReason.CRITICAL_LEVEL)

        centrality = (graph_metrics or {}).get("centrality", 0.0)
        if self._graph and centrality >= 0.9:
            reasons.append(EscalationReason.GRAPH_CENTRALITY_HIGH)

        if self._sust and estimated_energy_wh > self._s.sustainability_threshold_wh:
            reasons.append(EscalationReason.SUSTAINABILITY_HIGH)

        if agent_confidence < self._s.confidence_threshold:
            reasons.append(EscalationReason.CONFIDENCE_LOW)

        if human_feedback_score is not None and human_feedback_score < 0.3:
            reasons.append(EscalationReason.RLHF_LOW)

        modp_score = None
        if self._modp and not reasons:
            modp_score = self._compute_modp(
                accuracy=agent_confidence,
                energy=1.0 - min(
                    1.0,
                    estimated_energy_wh / max(0.01, self._s.sustainability_threshold_wh),
                ),
                latency=0.5,
                carbon=0.5,
            )
            if modp_score < 0.4:
                reasons.append(EscalationReason.MODP_OVERRIDE)

        return EscalationDecision(
            should_escalate=bool(reasons),
            reasons=reasons,
            modp_score=modp_score,
        )

    def _compute_modp(self, *, accuracy: float, energy: float,
                      latency: float, carbon: float) -> float:
        w = self._weights
        return max(0.0, min(1.0,
            w[0] * accuracy + w[1] * energy + w[2] * latency + w[3] * carbon,
        ))
