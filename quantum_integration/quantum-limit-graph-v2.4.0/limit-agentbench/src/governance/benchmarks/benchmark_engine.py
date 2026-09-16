"""
BenchmarkEngine — governance-side benchmark orchestration.

Defines task eligibility, evaluation rules, reproducibility
requirements, anti-gaming checks, and acceptance thresholds.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkSpec:
    """Governance-approved benchmark specification."""
    benchmark_id: str
    name: str
    task_eligibility: List[str]
    metric_definitions: Dict[str, str]
    anti_gaming_checks: List[str]
    acceptance_thresholds: Dict[str, float]
    reproducibility_rules: List[str]


@dataclass
class BenchmarkResult:
    """Immutable record of a benchmark run."""
    benchmark_id: str
    run_id: str
    system_version: str
    policy_version: str
    submitted_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    passed: bool = False
    rejection_reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


class BenchmarkEngine:
    """
    Governance engine for benchmark submissions.

    Rules enforced:
        - Task eligibility: benchmark only runs on approved tasks.
        - Anti-gaming: at least one anti-gaming check must be declared.
        - Acceptance thresholds: every threshold must be satisfied.
        - Reproducibility: reproducibility rules must be non-empty.
    """

    def __init__(self, spec: BenchmarkSpec):
        if not spec.anti_gaming_checks:
            raise ValueError("benchmark must declare anti-gaming checks")
        if not spec.reproducibility_rules:
            raise ValueError("benchmark must declare reproducibility rules")
        self.spec = spec

    def submit(
        self,
        *,
        run_id: str,
        task_type: str,
        system_version: str,
        policy_version: str,
        metrics: Dict[str, Any],
    ) -> BenchmarkResult:
        reasons: List[str] = []

        if task_type not in self.spec.task_eligibility:
            reasons.append(
                f"task type '{task_type}' not eligible for benchmark"
            )

        for name, threshold in self.spec.acceptance_thresholds.items():
            value = metrics.get(name)
            if not isinstance(value, (int, float)):
                reasons.append(f"metric '{name}' missing or non-numeric")
                continue
            if threshold >= 0 and value < threshold:
                reasons.append(
                    f"metric '{name}'={value} below threshold {threshold}"
                )
            elif threshold < 0 and value > abs(threshold):
                reasons.append(
                    f"metric '{name}'={value} above allowed "
                    f"{abs(threshold)}"
                )

        passed = not reasons
        result = BenchmarkResult(
            benchmark_id=self.spec.benchmark_id,
            run_id=run_id,
            system_version=system_version,
            policy_version=policy_version,
            passed=passed,
            rejection_reasons=reasons,
            metrics=dict(metrics),
        )
        logger.info(
            f"Benchmark '{self.spec.name}' run {run_id}: "
            f"{'PASSED' if passed else 'REJECTED'}"
        )
        return result
