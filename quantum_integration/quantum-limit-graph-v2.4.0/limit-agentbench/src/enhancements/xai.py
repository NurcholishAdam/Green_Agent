"""
xai.py — Explainable AI for every decision (Enhancement 6).
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np


@dataclass
class Explanation:
    decision_id: str
    kind: str
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    """Attribution builder over a rolling metrics window."""

    def __init__(self, feature_names: Optional[Sequence[str]] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "gpu_utilization_pct",
            "gpu_memory_used_mb",
            "gpu_power_watts",
            "gpu_temp_c",
            "cpu_utilization_pct",
            "energy_gpu_joules",
            "net_recv_bandwidth_gbps",
        ]

    def _attributions(
        self,
        current: Dict[str, Any],
        baseline: Dict[str, float],
        scales: Dict[str, float],
    ) -> List[Tuple[str, float]]:
        attrs: List[Tuple[str, float]] = []
        for f in self.feature_names:
            v = float(current.get(f, 0.0))
            mu = float(baseline.get(f, 0.0))
            s = float(scales.get(f, 1.0)) + 1e-9
            attrs.append((f, (v - mu) / s))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def explain_anomaly(
        self,
        kind: str,
        current: Dict[str, Any],
        baseline: Dict[str, float],
        scales: Dict[str, float],
        counterfactuals: Optional[Dict[int, float]] = None,
        safety_ok: bool = True,
        carbon_price: float = 0.0,
        confidence: float = 0.5,
    ) -> Explanation:
        attrs = self._attributions(current, baseline, scales)
        cf_list: List[Dict[str, Any]] = []
        if counterfactuals:
            for k, v in sorted(counterfactuals.items(), key=lambda kv: kv[1], reverse=True)[:3]:
                cf_list.append({"alt_idx": int(k), "expected_reward": float(v)})
        top = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Anomaly '{kind}' at t={current.get('timestamp', 0.0):.2f}. "
            f"Top deviations vs rolling baseline: {top}. "
            f"Safety={'ok' if safety_ok else 'VIOLATION'}. "
            f"Carbon price={carbon_price:.4f}."
        )
        return Explanation(
            decision_id=f"exp-{uuid.uuid4().hex[:8]}",
            kind=kind,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=confidence,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
        )
