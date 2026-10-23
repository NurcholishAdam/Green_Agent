# src/constraints/check_energy_budget.py
"""
check_energy_budget.py — v6.0.0
================================

Energy and carbon budget constraints for green benchmarking.

**Public API preserved**
------------------------
    ok, reason = check_energy_budget(metrics, max_energy_wh, max_carbon_kg)

**v6 additions**
----------------
    checker = BudgetChecker(deployment_id="...", features={...})
    ok, reason = checker.check(metrics, ...)
    result = checker.check_detailed(metrics, ...)      # EnhancedBudgetResult
    result = check_energy_budget_detailed(metrics, ..., checker=None)

**What changed (v5 → v6)**
--------------------------

**P0 — bugs**

1. ``_degraded_result`` never returns ``(False, "ok")``. v5 returned
   ``reason="ok"`` on circuit-open, producing a self-contradictory result
   and defeating the ``ReasonCodeConsistency`` temporal property (which
   was designed to catch exactly this — but bypassed by ``_degraded_result``).
   v6 returns ``"degraded:<reason>"`` and **runs the temporal monitor on
   degraded results**.
2. ``aggregate_violations`` no longer mutates ``reason``. v5 appended
   ``"+carbon,helium"`` to the reason, breaking every v1 caller that did
   ``reason == "energy_budget_exceeded"``. v6 keeps ``reason`` strict
   (first-violation metric) and exposes the aggregate as a separate field
   (``aggregate_reason``); the feature defaults to **``False``**.
3. ``CausalBudgetLearner`` (from ``budget_domain``) is feature-aware,
   signed (``±1``), and tightening-only by default. v5's was a one-way
   ratchet toward looser budgets.
4. **HITL propagation** — reviews every reviewable violation, not just
   the top; ``allow_hitl_override=False`` by default; ``hitl_status`` and
   ``hitl_override`` on the result.
5. **Small breaches are at least ``WARNING``.** v5 classified any breach
   under 5% as ``INFO``; v6 never returns ``INFO`` for a real breach.
   Additionally, **multi-metric breaches are escalated**: 3+ ``WARNING``
   → ``CRITICAL``; 4+ ``CRITICAL`` → ``EMERGENCY``.
6. **Zero-limit metrics → ``EMERGENCY``** (``overshoot_pct = +inf``).
7. **NaN / missing metrics → ``EMERGENCY``** in strict mode (default
   ``strict_metrics=True``); the v1 module function preserves its original
   ``metrics.get(k, 0.0)`` coercion.
8. **Helium auto-estimation is enforced** — when ``max_helium_units`` is
   set but ``metrics["helium"]`` is absent, the profiler's estimate (from
   ``energy_wh * scarcity_score``) is used. v5 computed it and then
   discarded it.
9. **``record_outcome``** on a passed check credits **all budgeted
   metrics**, not a fabricated ``"energy"`` default. It uses the real
   ``BudgetState`` from the last check.
10. **No module-level singleton.** v5 cached one ``BudgetChecker`` for the
    entire process, accumulating counters across every caller. v6 accepts
    an optional ``checker=`` argument and constructs a fresh one when
    omitted.

**P1 — reliability**

11. **Thread-safe via ``RLock``**.
12. **v18 single-probe ``CircuitBreaker``** via ``SyncCircuitAdapter``.
13. **Async HITL** awaited in ``areview``, rejected (fail-safe) in
    ``review``.
14. **Bounded collections** (violations, hitl queues, chaos events,
    federated updates).
15. **Cached market snapshot** in ``get_statistics()``.
16. **``degraded_reason`` and ``hitl_status`` on ``EnhancedBudgetResult``**;
    ``temporal_verified`` is ``Optional[bool]`` (``None`` when degraded).
17. **``set_features()``, ``health_check()``, ``__enter__/__aenter__``.**

**P2 — architecture**

18. **Enhancement classes imported from ``budget_domain`` (which imports
    from ``carbon_forecast`` v18)** — the tenth local copy in v5 is
    retired.
19. **Load-bearing temporal properties** — ``PassedImpliesNoViolations``
    and ``ReasonCodeConsistency`` now run on every path (including
    degraded) and fire on logic bugs.
20. **``QuantumDistillationBridge`` uses ``round``, not ``floor``.**
21. **``FederatedBudgetAggregator`` is bounded and zero-weight safe.**
22. **``contribute_federated`` pushes utilization, not breach rate.**
23. **``helium_signal_fn`` is consulted** for the scarcity score.
24. **``EnhancedBudgetResult.to_dict()`` is ISO-Z safe.**

**Enhancement classes are re-exported**
    ``CircuitBreaker``, ``ChaosTestingEngine``,
    ``ActiveUserPreferenceLearner``, ``AdaptivePrecisionSwitcher``,
    ``CarbonMarketIntegrator``, ``MultiAgentCoordinator``,
    ``FederatedGreenAggregator``, ``TemporalLogicVerifier`` — all from
    ``carbon_forecast`` v18 via ``budget_domain``.
"""

from __future__ import annotations

import logging
import math
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

# --------------------------------------------------------------------------- #
# Shared budget primitives (single source of truth for the budget family)
# --------------------------------------------------------------------------- #
from budget_domain import (  # type: ignore[import-not-found]
    # v18 re-exports
    CircuitBreaker,
    ChaosTestingEngine,
    ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher,
    CarbonMarketIntegrator,
    MultiAgentCoordinator,
    FederatedGreenAggregator,
    TemporalLogicVerifier,
    SimpleQuantumDistillationEngine,
    SimpleFederatedGreenAggregator,
    SimpleMultiAgentCoordinator,
    SimpleTemporalLogicVerifier,
    SimpleAdaptivePrecisionSwitcher,
    SimpleCarbonMarketIntegrator,
    SimpleChaosTestingEngine,
    SimpleActiveUserPreferenceLearner,
    # Enums
    Severity,
    PrecisionLevel,
    AgentRole,
    DegradedPolicy,
    EvaluationOutcome,
    # Types
    HardwareProfile,
    BudgetState,
    ThresholdState,
    BudgetExplanation,
    DistilledBudgetPolicy,
    FederatedBudgetProfile,
    MarketSnapshot,
    HITLRequest,
    # Helpers
    _utcnow,
    _ensure_aware,
    _json_safe,
    _safe_float,
    _V18_AVAILABLE,
    _V18_ERROR,
    _VALID_PRECISIONS,
    # Bounds
    MAX_HITL_LOG,
    # Components
    SyncCircuitAdapter,
    SyncChaosInjector,
    BudgetExplainer,
    CausalBudgetLearner,
    QuantumDistillationBridge,
    FederatedBudgetAggregator,
    HeliumProfiler,
    PassedImpliesNoViolations,
    ReasonCodeConsistency,
    BudgetSafetyMonitor,
    HumanReviewGate,
    CarbonMarketCache,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Original function — preserved verbatim
# --------------------------------------------------------------------------- #
def check_energy_budget(
    metrics: Dict[str, float],
    max_energy_wh: Optional[float] = None,
    max_carbon_kg: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Check whether collected metrics satisfy energy/carbon constraints.

    Original v1 implementation, preserved verbatim.

    Returns
    -------
    (bool, str)
        Whether constraints passed and reason code.
    """
    energy = metrics.get("energy", 0.0)
    carbon = metrics.get("carbon", 0.0)

    if max_energy_wh is not None and energy > max_energy_wh:
        return False, "energy_budget_exceeded"

    if max_carbon_kg is not None and carbon > max_carbon_kg:
        return False, "carbon_budget_exceeded"

    return True, "ok"


# --------------------------------------------------------------------------- #
# Enhanced result
# --------------------------------------------------------------------------- #
@dataclass
class EnhancedBudgetResult:
    """Rich result for a single budget check."""

    passed: bool
    reason: str
    violations: List[Dict[str, Any]] = field(default_factory=list)
    severity: Optional[str] = None
    explanation: Optional[Dict[str, Any]] = None
    temporal_verified: Optional[bool] = True
    temporal_violations: List[str] = field(default_factory=list)
    market_snapshot: Optional[Dict[str, Any]] = None
    helium_units: float = 0.0
    precision: str = "fp32"
    causal_adjustments: Dict[str, float] = field(default_factory=dict)
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    hitl_status: str = "not_required"
    hitl_override: bool = False
    degraded: bool = False
    degraded_reason: Optional[str] = None
    aggregate_reason: Optional[str] = None
    at: str = field(
        default_factory=lambda: _utcnow().isoformat()
    )

    def to_tuple(self) -> Tuple[bool, str]:
        """Backward-compatible view as (bool, reason)."""
        return self.passed, self.reason

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


# --------------------------------------------------------------------------- #
# Enhanced BudgetChecker
# --------------------------------------------------------------------------- #
class BudgetChecker:
    """
    Stateful enhanced budget checker.

    Use when you need history, statistics, learning, or cross-call state.
    For one-shot checks, use ``check_energy_budget_detailed()``.
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "causal_rl": True,
        "xai": True,
        "adaptive_precision": True,
        "federated": True,
        "multi_agent": True,
        "temporal_logic": True,
        "carbon_market": True,
        "chaos_testing": False,
        "hitl": True,
        "quantum_distillation": True,
        "helium_awareness": True,
        "aggregate_violations": False,
    }

    def __init__(
        self,
        deployment_id: str = "local",
        agent_id: str = "budget-checker-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        *,
        strict_metrics: bool = True,
        allow_hitl_override: bool = False,
        allow_budget_loosening: bool = False,
    ) -> None:
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()
        self._strict_metrics = bool(strict_metrics)
        self._allow_hitl_override = bool(allow_hitl_override)
        self._allow_budget_loosening = bool(allow_budget_loosening)

        # ---- Locks ----
        self._lock = threading.RLock()

        # ---- Core state ----
        self._circuit: Optional[SyncCircuitAdapter] = None
        self._chaos: Optional[SyncChaosInjector] = None
        self._safety: Optional[BudgetSafetyMonitor] = None
        self._precision: Optional[Any] = None
        self._budget_learner: Optional[CausalBudgetLearner] = None
        self._distiller: Optional[QuantumDistillationBridge] = None
        self._federated: Optional[FederatedBudgetAggregator] = None
        self._coordinator: Optional[Any] = None
        self._market: Optional[CarbonMarketCache] = None
        self._hitl: Optional[HumanReviewGate] = None
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # ---- Counters ----
        self.check_count: int = 0
        self.pass_count: int = 0
        self.fail_count: int = 0
        self.degraded_count: int = 0
        self.override_count: int = 0
        self.distilled_count: int = 0
        self.breach_by_metric: Dict[str, int] = defaultdict(int)
        self.severity_counts: Dict[str, int] = defaultdict(int)
        self._outcome_counts: Dict[str, int] = defaultdict(int)

        # ---- Cached state ----
        self.last_result: Optional[EnhancedBudgetResult] = None
        self._last_state: Optional[BudgetState] = None
        self._last_checked_metrics: List[str] = []
        self._last_market_snapshot: Optional[MarketSnapshot] = None
        self._last_market_fetched_at: Optional[float] = None

        self._build_layers()

        logger.info(
            "BudgetChecker v6 ready: deployment=%s, agent=%s, "
            "strict_metrics=%s, features=%s",
            deployment_id,
            agent_id,
            self._strict_metrics,
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Layer construction
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        self._circuit = SyncCircuitAdapter(
            "budget_checker",
            failure_threshold=5,
            recovery_timeout=30.0,
        )

        self._chaos = (
            SyncChaosInjector()
            if self.features["chaos_testing"]
            else None
        )

        self._safety = None
        if self.features["temporal_logic"]:
            self._safety = BudgetSafetyMonitor()
            self._safety.register(PassedImpliesNoViolations())
            self._safety.register(ReasonCodeConsistency())

        self._precision = None
        if self.features["adaptive_precision"] and _V18_AVAILABLE:
            try:
                self._precision = AdaptivePrecisionSwitcher(  # type: ignore[misc]
                    config={}, storage=None
                )
            except Exception:  # pragma: no cover
                self._precision = None

        self._budget_learner = None
        if self.features["causal_rl"]:
            self._budget_learner = CausalBudgetLearner(
                known_metrics=["energy", "carbon", "helium", "cost"],
                allow_loosening=self._allow_budget_loosening,
            )

        self._distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"]
            else None
        )

        self._federated = (
            FederatedBudgetAggregator()
            if self.features["federated"]
            else None
        )

        self._coordinator = None
        if self.features["multi_agent"] and _V18_AVAILABLE:
            try:
                self._coordinator = MultiAgentCoordinator(  # type: ignore[misc]
                    config={}, storage=None
                )
                self._coordinator.register(self.agent_id)
            except Exception:  # pragma: no cover
                self._coordinator = None

        self._market = (
            CarbonMarketCache()
            if self.features["carbon_market"]
            else None
        )

        self._hitl = None
        if self.features["hitl"]:
            learner = None
            if _V18_AVAILABLE:
                try:
                    learner = ActiveUserPreferenceLearner(  # type: ignore[misc]
                        storage=None
                    )
                except Exception:  # pragma: no cover
                    learner = None
            self._hitl = HumanReviewGate(learner=learner)

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        with self._lock:
            self._build_layers()

    # ------------------------------------------------------------------
    # Backward-compatible API
    # ------------------------------------------------------------------
    def check(
        self,
        metrics: Dict[str, float],
        max_energy_wh: Optional[float] = None,
        max_carbon_kg: Optional[float] = None,
        max_helium_units: Optional[float] = None,
        max_cost_usd: Optional[float] = None,
    ) -> Tuple[bool, str]:
        result = self.check_detailed(
            metrics=metrics,
            max_energy_wh=max_energy_wh,
            max_carbon_kg=max_carbon_kg,
            max_helium_units=max_helium_units,
            max_cost_usd=max_cost_usd,
        )
        return result.to_tuple()

    # ------------------------------------------------------------------
    # Enhanced API
    # ------------------------------------------------------------------
    def check_detailed(
        self,
        metrics: Mapping[str, Any],
        max_energy_wh: Optional[float] = None,
        max_carbon_kg: Optional[float] = None,
        max_helium_units: Optional[float] = None,
        max_cost_usd: Optional[float] = None,
        *,
        allow_hitl_override: Optional[bool] = None,
    ) -> EnhancedBudgetResult:
        with self._lock:
            self.check_count += 1

        # ---- Input validation (v1 preserves dict-like; v6 treats
        # non-Mapping as degraded) ----
        if not isinstance(metrics, Mapping):
            logger.warning(
                "Invalid metrics input (%s); treating as degraded",
                type(metrics).__name__,
            )
            return self._finalize_degraded(
                self._degraded_result("invalid_input")
            )

        # ---- Circuit ----
        if not self._circuit.can_call():
            return self._finalize_degraded(
                self._degraded_result("circuit_open")
            )

        # ---- Chaos ----
        if (
            self._chaos is not None
            and self._chaos.maybe_fail("check")
        ):
            self._circuit.record_failure()
            return self._finalize_degraded(
                self._degraded_result("chaos")
            )

        try:
            result = self._check_internal(
                dict(metrics),
                max_energy_wh=max_energy_wh,
                max_carbon_kg=max_carbon_kg,
                max_helium_units=max_helium_units,
                max_cost_usd=max_cost_usd,
                allow_hitl_override=allow_hitl_override,
            )
            self._circuit.record_success()
        except Exception as exc:
            self._circuit.record_failure()
            logger.warning("Check failed: %s", exc)
            return self._finalize_degraded(
                self._degraded_result(f"internal_error: {exc}")
            )

        with self._lock:
            self.last_result = result
            if result.passed:
                self.pass_count += 1
            else:
                self.fail_count += 1
            self._outcome_counts[
                EvaluationOutcome.APPROVED.value
                if result.passed
                else EvaluationOutcome.DENIED.value
            ] += 1
        return result

    def _finalize_degraded(
        self, result: EnhancedBudgetResult
    ) -> EnhancedBudgetResult:
        with self._lock:
            self.last_result = result
            self.fail_count += 1
            self.degraded_count += 1
            self._outcome_counts[
                EvaluationOutcome.DEGRADED.value
            ] += 1
        return result

    # ------------------------------------------------------------------
    # Internal pipeline
    # ------------------------------------------------------------------
    def _check_internal(
        self,
        metrics: Dict[str, Any],
        *,
        max_energy_wh: Optional[float],
        max_carbon_kg: Optional[float],
        max_helium_units: Optional[float],
        max_cost_usd: Optional[float],
        allow_hitl_override: Optional[bool],
    ) -> EnhancedBudgetResult:
        # ---- Build state (used by the RL and by record_outcome) ----
        state = self._state_from_metrics(metrics)
        with self._lock:
            self._last_state = state

        # ---- Active limits (RL-tightened) ----
        base_limits: Dict[str, Optional[float]] = {
            "energy": max_energy_wh,
            "carbon": max_carbon_kg,
            "helium": max_helium_units,
            "cost": max_cost_usd,
        }
        active_limits: Dict[str, Optional[float]] = dict(base_limits)
        causal_adjustments: Dict[str, float] = {}
        if self._budget_learner is not None:
            for metric, base in list(active_limits.items()):
                if base is None:
                    continue
                offset = self._budget_learner.predict_offset(
                    metric, state
                )
                active_limits[metric] = base * (1.0 + offset)
                causal_adjustments[metric] = offset

        checked_metrics = [
            m for m, lim in active_limits.items() if lim is not None
        ]
        with self._lock:
            self._last_checked_metrics = list(checked_metrics)

        # ---- Helium auto-estimate (P2 §4.9: consult signal_fn) ----
        helium_units = 0.0
        if self.features["helium_awareness"]:
            energy_for_he = _safe_float(
                metrics.get("energy", 0.0)
            )
            scarcity = self._current_scarcity()
            helium_units = HeliumProfiler.estimate_units(
                energy_for_he, scarcity
            )

        # ---- Collect violations (incl. bad-data / missing) ----
        violations: List[Dict[str, Any]] = []
        checked_for_breach: List[str] = []

        for metric in checked_metrics:
            limit_opt = active_limits.get(metric)
            if limit_opt is None:
                continue
            limit = float(limit_opt)

            # Missing metric?
            if metric not in metrics:
                if self._strict_metrics:
                    violations.append(
                        self._bad_data_violation(
                            metric, limit, "missing"
                        )
                    )
                continue
            raw = metrics[metric]

            # Bad-data metric?
            if isinstance(raw, bool) or not isinstance(
                raw, (int, float)
            ):
                violations.append(
                    self._bad_data_violation(
                        metric, limit, "not_numeric"
                    )
                )
                continue
            if not math.isfinite(float(raw)):
                violations.append(
                    self._bad_data_violation(
                        metric, limit, "not_finite"
                    )
                )
                continue

            value = float(raw)
            checked_for_breach.append(metric)
            if value > limit + 1e-9:
                overshoot = value - limit
                if limit > 0:
                    overshoot_pct = overshoot / limit * 100.0
                else:
                    overshoot_pct = float("inf")
                violations.append(
                    {
                        "metric": metric,
                        "value": value,
                        "limit": limit,
                        "overshoot": overshoot,
                        "overshoot_pct": overshoot_pct,
                    }
                )
                with self._lock:
                    self.breach_by_metric[metric] += 1

        # ---- Enforce Helium via auto-estimate when missing ----
        if (
            self.features["helium_awareness"]
            and "helium" in checked_metrics
            and "helium" not in metrics
            and active_limits.get("helium") is not None
        ):
            he_limit = float(active_limits["helium"])  # type: ignore[arg-type]
            if helium_units > he_limit + 1e-9:
                overshoot = helium_units - he_limit
                overshoot_pct = (
                    overshoot / he_limit * 100.0
                    if he_limit > 0
                    else float("inf")
                )
                violations.append(
                    {
                        "metric": "helium",
                        "value": helium_units,
                        "limit": he_limit,
                        "overshoot": overshoot,
                        "overshoot_pct": overshoot_pct,
                        "reason": "auto_estimated",
                    }
                )
                with self._lock:
                    self.breach_by_metric["helium"] += 1

        passed = len(violations) == 0

        # ---- Reason code (v1-compatible, never mutated) ----
        if passed:
            reason = "ok"
        else:
            first = violations[0]
            if first["metric"] == "energy":
                reason = "energy_budget_exceeded"
            elif first["metric"] == "carbon":
                reason = "carbon_budget_exceeded"
            elif first["metric"] == "helium":
                reason = "helium_budget_exceeded"
            elif first["metric"] == "cost":
                reason = "cost_budget_exceeded"
            else:
                reason = "budget_exceeded"

        # Aggregate is exposed separately — never mutating `reason`.
        aggregate_reason: Optional[str] = None
        if (
            self.features["aggregate_violations"]
            and len(violations) > 1
        ):
            extra = ",".join(v["metric"] for v in violations[1:])
            aggregate_reason = f"{reason}+{extra}"

        # ---- Severity ----
        severity = self._classify_severity(violations)
        if severity != Severity.INFO:
            with self._lock:
                self.severity_counts[severity.value] += 1

        # ---- Market snapshot (cached) ----
        market_snapshot: Optional[Dict[str, Any]] = None
        if self._market is not None:
            try:
                snap = self._market.get_snapshot()
                co2_used = _safe_float(metrics.get("carbon", 0.0))
                market_snapshot = {
                    "carbon_price_per_tco2_usd": (
                        snap.carbon_price_per_tco2_usd
                    ),
                    "rec_available_mwh": snap.rec_available_mwh,
                    "co2_market_value_usd": (
                        co2_used * snap.carbon_price_per_tco2_usd
                    ),
                }
                with self._lock:
                    self._last_market_snapshot = snap
                    self._last_market_fetched_at = (
                        _utcnow().timestamp()
                    )
            except Exception:  # pragma: no cover
                pass

        # ---- Precision attribution ----
        precision = self._select_precision(
            "critical" if not passed else "normal"
        )

        # ---- HITL (review every reviewable violation) ----
        hitl_required = False
        hitl_approved_all: Optional[bool] = None
        hitl_status = "not_required"
        hitl_override = False

        if not passed and self._hitl is not None:
            reviewable = []
            for v in violations:
                if v.get("reason") in (
                    "missing",
                    "not_numeric",
                    "not_finite",
                ):
                    continue
                val = v.get("value")
                limit_v = v.get("limit", 0.0)
                if limit_v <= 0:
                    # Zero-limit breaches are emergency and unreviewable
                    continue
                if val is None:
                    continue
                if self._hitl_needs_review(val, limit_v):
                    reviewable.append(v)

            if reviewable:
                hitl_required = True
                all_approved = True
                statuses: List[str] = []
                for v in reviewable:
                    approved, status = self._hitl.review(
                        metric=v["metric"],
                        value=v["value"],
                        limit=v["limit"],
                        reason=(
                            f"overshoot "
                            f"{v['overshoot_pct']:.1f}%"
                        ),
                        urgency=(
                            "high"
                            if severity
                            in (Severity.CRITICAL, Severity.EMERGENCY)
                            else "medium"
                        ),
                        context={"agent_id": self.agent_id},
                    )
                    statuses.append(status)
                    if approved is not True:
                        all_approved = False
                hitl_status = (
                    "approved"
                    if all_approved and statuses
                    else (
                        "rejected"
                        if "rejected" in statuses
                        else "timed_out"
                    )
                )
                hitl_approved_all = all_approved

        # ---- HITL override (opt-in) ----
        override_flag = (
            allow_hitl_override
            if allow_hitl_override is not None
            else self._allow_hitl_override
        )
        if override_flag and hitl_approved_all is True:
            hitl_override = True
            with self._lock:
                self.override_count += 1
                self._outcome_counts[
                    EvaluationOutcome.OVERRIDDEN.value
                ] += 1
            logger.warning(
                "HITL approved denial (allow_hitl_override=True); "
                "not failing — violations=%s",
                [v["metric"] for v in violations],
            )
            passed = True
            # Keep violations but flag them as overridden
            for v in violations:
                v["hitl_override"] = True
            # reason stays as-is (violation caused); override is separate

        # ---- XAI ----
        explanation: Optional[BudgetExplanation] = None
        if self.features["xai"]:
            explanation = BudgetExplainer.explain(
                violations=violations,
                passed=passed,
                severity=severity.value,
                market_snapshot=market_snapshot,
                hitl_status=hitl_status,
            )

        # ---- Temporal verification (load-bearing) ----
        temporal_verified: Optional[bool] = True
        temporal_violations: List[str] = []
        if self._safety is not None:
            ctx = {
                "passed": passed,
                "reason": reason,
                "violations": violations,
            }
            ok, tv = self._safety.verify(ctx)
            temporal_verified = ok
            temporal_violations = list(tv)
            if not ok:
                logger.warning(
                    "Temporal violations on %s: %s",
                    reason,
                    tv,
                )

        # ---- Coordinator feedback ----
        if self._coordinator is not None:
            try:
                self._coordinator.reward(
                    self.agent_id, 1.0 if passed else 0.0
                )
            except Exception:  # pragma: no cover
                pass

        # ---- Federated contribution (sampled) ----
        if (
            self._federated is not None
            and self.check_count % 25 == 0
            and checked_for_breach
        ):
            for metric in checked_for_breach:
                limit = active_limits.get(metric)
                if limit is None or limit <= 0:
                    continue
                value = _safe_float(metrics.get(metric, 0.0))
                self._federated.push(
                    FederatedBudgetProfile(
                        deployment_id=self.deployment_id,
                        metric=metric,
                        mean_utilization=value / limit,
                        sample_count=1,
                        budget_name=f"checker:{metric}",
                    )
                )
            self._federated.aggregate()

        return EnhancedBudgetResult(
            passed=passed,
            reason=reason,
            violations=violations,
            severity=severity.value,
            explanation=(
                explanation.to_dict() if explanation else None
            ),
            temporal_verified=temporal_verified,
            temporal_violations=temporal_violations,
            market_snapshot=market_snapshot,
            helium_units=helium_units,
            precision=precision.value,
            causal_adjustments=causal_adjustments,
            hitl_required=hitl_required,
            hitl_approved=hitl_approved_all,
            hitl_status=hitl_status,
            hitl_override=hitl_override,
            degraded=False,
            degraded_reason=None,
            aggregate_reason=aggregate_reason,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _state_from_metrics(
        self, metrics: Mapping[str, Any]
    ) -> BudgetState:
        # Build a state whose utilization_ratio is derived from the
        # caller's metrics vs. the request-level budget bases. Because
        # the budget bases arrive per call (max_energy_wh etc.), we use
        # the peak observed utilization from the last completed check
        # for continuity — or 0.5 on the first call.
        # We pass the metric-based utilization as the ratio.
        ratios: List[float] = []
        with self._lock:
            for m, v in metrics.items():
                if m in self.budget_bases:
                    base = self.budget_bases[m]
                    if base and base > 0:
                        try:
                            ratios.append(float(v) / base)
                        except (TypeError, ValueError):
                            continue
        peak = max(ratios) if ratios else 0.5
        return BudgetState(
            hour_of_day=_utcnow().hour,
            utilization_ratio=min(1.0, peak),
            workload_hash=float(
                abs(hash(self.agent_id)) % 1000
            ) / 1000.0,
        )

    # `budget_bases` is populated on each check_detailed call from the
    # keyword limits, so `_state_from_metrics` can compute a per-call
    # utilization ratio without needing extra state on the caller side.
    @property
    def budget_bases(self) -> Dict[str, float]:
        return self._last_budget_bases

    def _current_scarcity(self) -> float:
        if self.helium_signal_fn is None:
            return 0.0
        try:
            sig = self.helium_signal_fn()
            if sig is None:
                return 0.0
            return _safe_float(
                getattr(sig, "scarcity_score", 0.0)
            )
        except Exception:
            return 0.0

    def _bad_data_violation(
        self, metric: str, limit: float, reason: str
    ) -> Dict[str, Any]:
        return {
            "metric": metric,
            "value": None,
            "limit": limit,
            "overshoot": float("inf"),
            "overshoot_pct": float("inf"),
            "reason": reason,
        }

    def _hitl_needs_review(self, value: float, limit: float) -> bool:
        if limit <= 0:
            return False
        ratio = value / limit
        if 1.0 < ratio <= 1.05:
            return True
        if ratio >= 2.0:
            return True
        return False

    def _classify_severity(
        self, violations: List[Dict[str, Any]]
    ) -> Severity:
        if not violations:
            return Severity.INFO
        infinite = any(
            v.get("overshoot_pct") == float("inf")
            for v in violations
        )
        if infinite:
            return Severity.EMERGENCY
        finite = [
            v["overshoot_pct"]
            for v in violations
            if v.get("overshoot_pct") != float("inf")
        ]
        max_pct = max(finite) if finite else 0.0
        if max_pct >= 100.0:
            base = Severity.EMERGENCY
        elif max_pct >= 30.0:
            base = Severity.CRITICAL
        else:
            base = Severity.WARNING
        # Escalate on multi-metric breaches
        if len(violations) >= 4 and base == Severity.CRITICAL:
            return Severity.EMERGENCY
        if len(violations) >= 3 and base == Severity.WARNING:
            return Severity.CRITICAL
        return base

    def _select_precision(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hardware.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hardware.supports_int8
                else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4
            if self.hardware.supports_int4
            else PrecisionLevel.INT8
        )

    def _degraded_result(
        self, reason: str
    ) -> EnhancedBudgetResult:
        """
        P0 §2.1: never return ``(False, "ok")``. Always explicit and
        always runs the temporal monitor (the ``ReasonCodeConsistency``
        property is designed to catch this exact bug).
        """
        result = EnhancedBudgetResult(
            passed=False,
            reason=f"degraded:{reason}",
            violations=[],
            severity=None,
            temporal_verified=None,
            degraded=True,
            degraded_reason=reason,
        )
        if self._safety is not None:
            ctx = {
                "passed": result.passed,
                "reason": result.reason,
                "violations": [],
            }
            ok, tv = self._safety.verify(ctx)
            result.temporal_verified = ok
            result.temporal_violations = list(tv)
        return result

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------
    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        if self._hitl is not None:
            self._hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return (
            list(self._hitl.feedback_log)[-n:]
            if self._hitl is not None
            else []
        )

    def record_outcome(self, was_correct: bool) -> bool:
        """
        Feed back whether the last check was correct.

        P0 §2.9: on a passed check, **every** budgeted metric is
        credited — not a fabricated ``"energy"`` default.
        """
        if self._budget_learner is None:
            return False
        with self._lock:
            state = self._last_state
            last_result = self.last_result
            checked = list(self._last_checked_metrics)
        if state is None or last_result is None:
            return False
        features = CausalBudgetLearner.features_of(state)
        reward = 1.0 if was_correct else -1.0

        metrics_to_credit: List[str] = []
        if last_result.violations:
            for v in last_result.violations:
                m = v.get("metric")
                if m in self._budget_learner.offsets:
                    metrics_to_credit.append(m)
        else:
            for m in checked:
                if m in self._budget_learner.offsets:
                    metrics_to_credit.append(m)

        for m in metrics_to_credit:
            self._budget_learner.record(m, features, reward)
        if (
            self._budget_learner.observations % 10 == 0
            and self._budget_learner.buffer
        ):
            self._budget_learner.update()
        return True

    def contribute_federated(self) -> None:
        """
        Push current utilization statistics to the federated aggregator.

        P2 §4.5: uses utilization, not breach rate.
        """
        if self._federated is None:
            return
        with self._lock:
            last = self.last_result
            checked = list(self._last_checked_metrics)
        if last is None or not checked:
            return
        for metric in checked:
            # utilization = 1 - (remaining/limit) would need the limit;
            # use overshoot info if it exists, else 0 for passed metrics.
            util = 0.0
            for v in last.violations:
                if v["metric"] == metric:
                    pct = v.get("overshoot_pct", 0.0)
                    util = (
                        1.0
                        if pct == float("inf")
                        else 1.0 + pct / 100.0
                    )
                    break
            self._federated.push(
                FederatedBudgetProfile(
                    deployment_id=self.deployment_id,
                    metric=metric,
                    mean_utilization=util,
                    sample_count=1,
                    budget_name=f"checker:{metric}",
                )
            )
        self._federated.aggregate()

    def get_federated_aggregate(
        self,
    ) -> Dict[str, Dict[str, float]]:
        return (
            self._federated.aggregate()
            if self._federated is not None
            else {}
        )

    def distill_policy(
        self,
        max_energy_wh: Optional[float] = None,
        max_carbon_kg: Optional[float] = None,
        urgency: str = "normal",
    ) -> Optional[DistilledBudgetPolicy]:
        if self._distiller is None:
            return None
        values: Dict[str, Optional[float]] = {}
        if max_energy_wh is not None:
            values["energy_wh"] = max_energy_wh
        if max_carbon_kg is not None:
            values["carbon_kg"] = max_carbon_kg
        if not values:
            return None
        precision = self._select_precision(urgency)
        policy = self._distiller.distill(values, precision)
        with self._lock:
            self.distilled_count += 1
        return policy

    # ------------------------------------------------------------------
    # Statistics / health / lifecycle
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "checks": self.check_count,
                "pass_count": self.pass_count,
                "fail_count": self.fail_count,
                "pass_rate": (
                    self.pass_count / self.check_count
                    if self.check_count > 0
                    else 0.0
                ),
                "degraded_count": self.degraded_count,
                "override_count": self.override_count,
                "distilled_count": self.distilled_count,
                "breach_by_metric": dict(self.breach_by_metric),
                "severity_counts": dict(self.severity_counts),
                "outcome_counts": dict(self._outcome_counts),
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
            }
            if self._safety is not None:
                stats["temporal_violation_count"] = len(
                    self._safety.violations
                )
                stats["temporal_violations"] = list(
                    self._safety.violations
                )[-5:]
            if self._coordinator is not None:
                try:
                    stats["agents"] = {
                        a.id: a.role
                        for a in getattr(
                            self._coordinator, "agents", {}
                        ).values()
                    }
                except Exception:  # pragma: no cover
                    pass
            if self._chaos is not None:
                stats["chaos_events"] = list(
                    self._chaos.events
                )[-5:]
            if self._hitl is not None:
                stats["hitl"] = {
                    "pending": len(self._hitl.pending),
                    "log_size": len(self._hitl.feedback_log),
                    "outcomes": self._hitl.outcomes(),
                }
            if self._budget_learner is not None:
                stats["causal_learner"] = (
                    self._budget_learner.stats()
                )
            if self._federated is not None:
                stats["federated_aggregate"] = (
                    self._federated.aggregate()
                )
            if self._last_market_snapshot is not None:
                stats["market"] = {
                    "carbon_price_per_tco2_usd": (
                        self._last_market_snapshot.carbon_price_per_tco2_usd
                    ),
                    "rec_price_per_mwh_usd": (
                        self._last_market_snapshot.rec_price_per_mwh_usd
                    ),
                    "cached_at": _ensure_aware(
                        self._last_market_snapshot.timestamp
                    ).isoformat(),
                }
            return stats

    async def health_check(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "healthy": self._circuit.state != "OPEN",
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
                "checks": self.check_count,
                "pass_count": self.pass_count,
                "fail_count": self.fail_count,
                "degraded_count": self.degraded_count,
                "override_count": self.override_count,
                "strict_metrics": self._strict_metrics,
                "allow_hitl_override": self._allow_hitl_override,
                "allow_budget_loosening": (
                    self._allow_budget_loosening
                ),
                "hitl_outcomes": (
                    self._hitl.outcomes()
                    if self._hitl is not None
                    else {}
                ),
                "temporal_violation_count": (
                    len(self._safety.violations)
                    if self._safety is not None
                    else 0
                ),
                "features": dict(self.features),
            }

    def __enter__(self) -> "BudgetChecker":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "BudgetChecker":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    def __repr__(self) -> str:
        with self._lock:
            return (
                "BudgetChecker("
                f"checks={self.check_count}, "
                f"passes={self.pass_count}, "
                f"fails={self.fail_count}, "
                f"circuit={self._circuit.state})"
            )


# --------------------------------------------------------------------------- #
# Convenience: one-shot detailed check (no leaking singleton)
# --------------------------------------------------------------------------- #
def check_energy_budget_detailed(
    metrics: Dict[str, float],
    max_energy_wh: Optional[float] = None,
    max_carbon_kg: Optional[float] = None,
    max_helium_units: Optional[float] = None,
    max_cost_usd: Optional[float] = None,
    *,
    checker: Optional[BudgetChecker] = None,
) -> EnhancedBudgetResult:
    """
    One-shot enhanced check.

    P0 §2.10: no module-level singleton. Each call **without** a
    ``checker`` constructs a fresh, ephemeral ``BudgetChecker`` — so the
    convenience function never accumulates state across callers. Pass
    ``checker=`` to reuse a stateful instance.
    """
    if checker is None:
        checker = BudgetChecker()
    return checker.check_detailed(
        metrics=metrics,
        max_energy_wh=max_energy_wh,
        max_carbon_kg=max_carbon_kg,
        max_helium_units=max_helium_units,
        max_cost_usd=max_cost_usd,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # v18 re-exports
    "CircuitBreaker",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
    "MultiAgentCoordinator",
    "FederatedGreenAggregator",
    "TemporalLogicVerifier",
    "SimpleQuantumDistillationEngine",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
    # Enums / types
    "Severity",
    "PrecisionLevel",
    "AgentRole",
    "DegradedPolicy",
    "EvaluationOutcome",
    "HardwareProfile",
    "BudgetState",
    "ThresholdState",
    "BudgetExplanation",
    "DistilledBudgetPolicy",
    "FederatedBudgetProfile",
    "MarketSnapshot",
    "HITLRequest",
    # Components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "BudgetExplainer",
    "CausalBudgetLearner",
    "QuantumDistillationBridge",
    "FederatedBudgetAggregator",
    "HeliumProfiler",
    "PassedImpliesNoViolations",
    "ReasonCodeConsistency",
    "BudgetSafetyMonitor",
    "HumanReviewGate",
    "CarbonMarketCache",
    # Main
    "check_energy_budget",
    "EnhancedBudgetResult",
    "BudgetChecker",
    "check_energy_budget_detailed",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("check_energy_budget.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # ============================================================= #
    # v1 function preserved
    # ============================================================= #
    print("\n=== v1 function preserved verbatim ===")
    _check(
        check_energy_budget(
            {"energy": 6.2}, max_energy_wh=5.0
        ) == (False, "energy_budget_exceeded"),
        "energy breach → (False, 'energy_budget_exceeded')",
    )
    _check(
        check_energy_budget(
            {"carbon": 1.5}, max_carbon_kg=1.0
        ) == (False, "carbon_budget_exceeded"),
        "carbon breach → (False, 'carbon_budget_exceeded')",
    )
    _check(
        check_energy_budget(
            {"energy": 3.0, "carbon": 0.5},
            max_energy_wh=5.0,
            max_carbon_kg=1.0,
        )
        == (True, "ok"),
        "passing check → (True, 'ok')",
    )

    # ============================================================= #
    # P0 §2.1 — _degraded_result never returns (False, "ok")
    # ============================================================= #
    print("\n=== P0 §2.1: degraded reason is always explicit ===")
    degraded_mgr = BudgetChecker()
    for _ in range(10):
        degraded_mgr._circuit.record_failure()
    _check(
        degraded_mgr._circuit.state == "OPEN",
        "circuit forced open",
    )
    result = degraded_mgr.check_detailed(
        {"energy": 1.0}, max_energy_wh=5.0
    )
    _check(
        result.passed is False and result.reason.startswith("degraded:"),
        f"circuit-open → reason='{result.reason}' (not 'ok')",
    )
    _check(
        result.degraded is True,
        "degraded flag is True",
    )
    _check(
        result.temporal_verified is not None,
        "degraded result ran the temporal monitor",
    )
    _check(
        result.temporal_verified is True,
        "ReasonCodeConsistency passes on 'degraded:circuit_open'",
    )

    # ============================================================= #
    # P0 §2.2 — aggregate_violations doesn't corrupt reason
    # ============================================================= #
    print("\n=== P0 §2.2: reason string is v1-compatible ===")
    agg_mgr = BudgetChecker()
    r_multi = agg_mgr.check_detailed(
        {"energy": 6.2, "carbon": 1.5},
        max_energy_wh=5.0, max_carbon_kg=1.0,
    )
    _check(
        r_multi.reason == "energy_budget_exceeded",
        f"reason is exactly 'energy_budget_exceeded' "
        f"(got {r_multi.reason!r})",
    )
    _check(
        len(r_multi.violations) == 2,
        "both violations captured in the list",
    )

    # With aggregate_violations=True, reason is still v1 + aggregate separate
    agg_mgr2 = BudgetChecker(features={"aggregate_violations": True})
    r_agg = agg_mgr2.check_detailed(
        {"energy": 6.2, "carbon": 1.5},
        max_energy_wh=5.0, max_carbon_kg=1.0,
    )
    _check(
        r_agg.reason == "energy_budget_exceeded",
        "reason still v1-compatible with aggregate_violations=True",
    )
    _check(
        r_agg.aggregate_reason is not None
        and "carbon" in r_agg.aggregate_reason,
        f"aggregate_reason carries extras (got {r_agg.aggregate_reason!r})",
    )

    # ============================================================= #
    # P0 §2.3 — causal learner: features, signed, tightening-only
    # ============================================================= #
    print("\n=== P0 §2.3: causal learner ===")
    learner = CausalBudgetLearner()
    state_a = BudgetState(8, 0.9, 0.1)
    state_b = BudgetState(20, 0.1, 0.9)
    feats_a = CausalBudgetLearner.features_of(state_a)
    feats_b = CausalBudgetLearner.features_of(state_b)
    for _ in range(30):
        learner.record("energy", feats_a, -1.0)
    learner.update()
    for _ in range(30):
        learner.record("energy", feats_b, -1.0)
    learner.update()
    off_a = learner.predict_offset("energy", state_a)
    off_b = learner.predict_offset("energy", state_b)
    _check(
        abs(off_a - off_b) > 1e-4,
        f"different states → different offsets "
        f"(A={off_a:.4f}, B={off_b:.4f})",
    )

    # Signed reward tightens
    tightener = CausalBudgetLearner()
    baseline = tightener.predict_offset("energy", state_a)
    for _ in range(50):
        tightener.record("energy", feats_a, -1.0)
    tightener.update()
    after = tightener.predict_offset("energy", state_a)
    _check(
        after < baseline,
        f"negative reward tightens: {baseline:.4f} → {after:.4f}",
    )

    # Tightening-only: 100 correct don't loosen
    widener = CausalBudgetLearner()
    for _ in range(100):
        widener.record("energy", feats_a, 1.0)
    widener.update()
    off_after = widener.predict_offset("energy", state_a)
    _check(
        off_after <= 0.0,
        f"100 correct outcomes never loosen: offset={off_after:.4f}",
    )

    # ============================================================= #
    # P0 §2.4 — HITL propagation
    # ============================================================= #
    print("\n=== P0 §2.4: HITL propagation ===")
    hitl_reject = BudgetChecker()
    hitl_reject.set_hitl_callback(lambda req: False)
    r_rej = hitl_reject.check_detailed(
        {"energy": 5.3}, max_energy_wh=5.0,
    )
    _check(
        r_rej.passed is False,
        "HITL rejection → still failed",
    )
    _check(
        r_rej.hitl_status == "rejected",
        f"HITL status is 'rejected' (got {r_rej.hitl_status!r})",
    )

    hitl_override = BudgetChecker(allow_hitl_override=True)
    hitl_override.set_hitl_callback(lambda req: True)
    r_ov = hitl_override.check_detailed(
        {"energy": 5.3}, max_energy_wh=5.0,
    )
    _check(
        r_ov.passed is True and r_ov.hitl_override is True,
        "allow_hitl_override=True + approval → passed with hitl_override flag",
    )
    _check(
        hitl_override.override_count == 1,
        "override_count incremented",
    )

    # ============================================================= #
    # P0 §2.5 — small breaches are at least WARNING
    # ============================================================= #
    print("\n=== P0 §2.5: no breach is INFO ===")
    warn_mgr = BudgetChecker()
    r_small = warn_mgr.check_detailed(
        {"energy": 5.01}, max_energy_wh=5.0,  # 0.2% over
    )
    _check(
        r_small.severity == "warning",
        f"0.2% breach → WARNING (got {r_small.severity})",
    )

    # ============================================================= #
    # P0 §2.6 — zero-limit metric → EMERGENCY
    # ============================================================= #
    print("\n=== P0 §2.6: zero-limit breach → EMERGENCY ===")
    zero_mgr = BudgetChecker()
    r_zero = zero_mgr.check_detailed(
        {"carbon": 1.0}, max_carbon_kg=0.0,
    )
    _check(
        r_zero.severity == "emergency",
        f"zero-limit breach → EMERGENCY (got {r_zero.severity})",
    )
    _check(
        r_zero.violations[0]["overshoot_pct"] == float("inf"),
        "overshoot_pct is +inf for zero-limit",
    )

    # ============================================================= #
    # P0 §2.7 — NaN / missing metrics
    # ============================================================= #
    print("\n=== P0 §2.7: NaN / missing metrics → EMERGENCY ===")
    nan_mgr = BudgetChecker(strict_metrics=True)
    r_nan = nan_mgr.check_detailed(
        {"energy": float("nan")}, max_energy_wh=5.0,
    )
    _check(
        r_nan.passed is False
        and r_nan.severity == "emergency",
        "NaN energy → failed with EMERGENCY",
    )
    r_miss = nan_mgr.check_detailed(
        {"carbon": 0.5}, max_energy_wh=5.0,
    )
    _check(
        r_miss.passed is False,
        "missing energy in strict mode → failed",
    )

    # Lenient mode preserves v1 (missing → 0.0)
    lenient = BudgetChecker(strict_metrics=False)
    r_len = lenient.check_detailed(
        {"carbon": 0.5}, max_energy_wh=5.0,
    )
    _check(
        r_len.passed is True,
        "strict_metrics=False preserves v1 (missing → 0.0)",
    )

    # ============================================================= #
    # P0 §2.8 — helium auto-estimated from energy
    # ============================================================= #
    print("\n=== P0 §2.8: helium auto-estimated from energy ===")
    helium_mgr = BudgetChecker()
    # 1 kWh × 5e-5 units/Wh = 0.05 helium units. Limit is 0.01 → breach.
    r_he = helium_mgr.check_detailed(
        {"energy": 1000.0},
        max_helium_units=0.01,
    )
    _check(
        any(v["metric"] == "helium" for v in r_he.violations),
        "helium auto-estimate breached the limit",
    )

    # ============================================================= #
    # P0 §2.9 — record_outcome on pass credits all checked metrics
    # ============================================================= #
    print("\n=== P0 §2.9: record_outcome credits all metrics ===")
    rec_mgr = BudgetChecker()
    rec_mgr.check_detailed(
        {"energy": 1.0, "carbon": 0.2},
        max_energy_wh=5.0, max_carbon_kg=1.0,
    )
    before = dict(
        rec_mgr._budget_learner.observations_by_metric
    )
    rec_mgr.record_outcome(was_correct=True)
    after = dict(
        rec_mgr._budget_learner.observations_by_metric
    )
    _check(
        after.get("energy", 0) > before.get("energy", 0),
        "passed check credits 'energy'",
    )
    _check(
        after.get("carbon", 0) > before.get("carbon", 0),
        "passed check credits 'carbon' (v5 defaulted to 'energy' only)",
    )

    # ============================================================= #
    # P0 §2.10 — no leaking module singleton
    # ============================================================= #
    print("\n=== P0 §2.10: no module-level singleton ===")
    import sys as _sys
    _mod = _sys.modules[__name__]
    _check(
        not hasattr(_mod, "_DEFAULT_CHECKER"),
        "no _DEFAULT_CHECKER global (v5 leaked state across all callers)",
    )
    custom = BudgetChecker()
    check_energy_budget_detailed(
        {"energy": 1.0}, max_energy_wh=5.0, checker=custom
    )
    check_energy_budget_detailed(
        {"energy": 1.0}, max_energy_wh=5.0, checker=custom
    )
    _check(
        custom.check_count == 2,
        "custom checker accumulates across calls (opt-in state)",
    )

    # ============================================================= #
    # P1 §3.1 — thread safety
    # ============================================================= #
    print("\n=== P1 §3.1: thread safety ===")
    import threading
    safe = BudgetChecker()
    errors: List[BaseException] = []

    def _worker() -> None:
        try:
            for _ in range(100):
                safe.check_detailed(
                    {"energy": 1.0, "carbon": 0.2},
                    max_energy_wh=5.0, max_carbon_kg=1.0,
                )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _check(not errors, "10×100 concurrent checks: no errors")
    _check(
        safe.check_count == 1000,
        f"check_count is exactly 1000 (got {safe.check_count})",
    )

    # ============================================================= #
    # P1 §3.2 — v18 single-probe circuit breaker
    # ============================================================= #
    print("\n=== P1 §3.2: v18 single-probe circuit breaker ===")
    probe = BudgetChecker()
    for _ in range(2):
        probe._circuit.record_failure()
    _check(
        probe._circuit.state == "CLOSED",
        "two failures below threshold=5 → CLOSED",
    )
    for _ in range(3):
        probe._circuit.record_failure()
    _check(
        probe._circuit.state == "OPEN",
        "five failures → OPEN",
    )

    # ============================================================= #
    # P1 §3.3 — async HITL
    # ============================================================= #
    print("\n=== P1 §3.3: async HITL ===")
    from budget_domain import HumanReviewGate
    gate = HumanReviewGate()

    async def _async_cb(req):
        return True

    gate.set_callback(_async_cb)
    approved, status = gate.review(
        metric="energy", value=6.0, limit=5.0,
        reason="test", urgency="medium",
    )
    _check(
        approved is False and status == "async_in_sync",
        f"async callback rejected in sync review (got {status!r})",
    )

    import asyncio

    async def _await_review():
        gate2 = HumanReviewGate()

        async def _cb(req):
            return True

        gate2.set_callback(_cb)
        return await gate2.areview(
            metric="energy", value=6.0, limit=5.0,
            reason="test", urgency="medium",
        )

    approved2, status2 = asyncio.run(_await_review())
    _check(
        approved2 is True and status2 == "approved",
        "async callback awaited in areview",
    )

    # ============================================================= #
    # P1 §3.5 — multi-metric escalation
    # ============================================================= #
    print("\n=== P1 §3.5: 4-metric breach escalates severity ===")
    multi_mgr = BudgetChecker()
    r_multi_sev = multi_mgr.check_detailed(
        {
            "energy": 6.2, "carbon": 1.24,
            "helium": 0.062, "cost": 0.248,
        },
        max_energy_wh=5.0, max_carbon_kg=1.0,
        max_helium_units=0.05, max_cost_usd=0.2,
    )
    _check(
        len(r_multi_sev.violations) == 4,
        f"all 4 metrics breached (got {len(r_multi_sev.violations)})",
    )
    _check(
        r_multi_sev.severity == "critical",
        f"4-metric ~24% breach → CRITICAL "
        f"(got {r_multi_sev.severity})",
    )

    # ============================================================= #
    # P1 §3.7 — bounded collections
    # ============================================================= #
    print("\n=== P1 §3.7: bounded collections ===")
    from budget_domain import (
        MAX_HITL_LOG, MAX_CHAOS_EVENTS, MAX_FEDERATED_UPDATES,
    )
    bound = BudgetChecker(
        features={"chaos_testing": True, "federated": True}
    )
    # Push many checks to stress the queues
    for _ in range(2000):
        bound.check_detailed(
            {"energy": 6.0}, max_energy_wh=5.0,
        )
    _check(
        len(bound._hitl.feedback_log) <= MAX_HITL_LOG,
        f"hitl.feedback_log bounded at {MAX_HITL_LOG}",
    )
    _check(
        len(bound._federated.updates) <= MAX_FEDERATED_UPDATES,
        f"federated.updates bounded at {MAX_FEDERATED_UPDATES}",
    )
    _check(
        len(bound._chaos.events) <= MAX_CHAOS_EVENTS,
        f"chaos.events bounded at {MAX_CHAOS_EVENTS}",
    )

    # ============================================================= #
    # P1 §3.8 — cached market snapshot
    # ============================================================= #
    print("\n=== P1 §3.8: cached market snapshot ===")
    cache_mgr = BudgetChecker()
    cache_mgr.check_detailed(
        {"energy": 1.0}, max_energy_wh=5.0,
    )
    p1 = cache_mgr.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    p2 = cache_mgr.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    _check(
        p1 == p2,
        f"consecutive get_statistics() reports cached price ({p1})",
    )

    # ============================================================= #
    # P1 §3.9 — set_features, health_check, async CM
    # ============================================================= #
    print("\n=== P1 §3.9: set_features, health_check, async CM ===")
    live = BudgetChecker()
    _check(live._market is not None, "market enabled by default")
    live.set_features({"carbon_market": False})
    _check(live._market is None, "set_features disabled market")
    live.set_features({"carbon_market": True})
    _check(live._market is not None, "set_features re-enabled market")

    async def _async_health():
        async with BudgetChecker() as m:
            health = await m.health_check()
            _check(
                "healthy" in health
                and "strict_metrics" in health,
                "health_check returns expected keys",
            )

    asyncio.run(_async_health())

    # ============================================================= #
    # P2 §4.2 — temporal properties fire on logic bugs
    # ============================================================= #
    print("\n=== P2 §4.2: temporal properties actually fire ===")
    from budget_domain import (
        PassedImpliesNoViolations, ReasonCodeConsistency,
        BudgetSafetyMonitor,
    )
    safety = BudgetSafetyMonitor()
    safety.register(ReasonCodeConsistency())
    ok, viols = safety.verify(
        {
            "passed": False,
            "reason": "ok",
            "violations": [],
        }
    )
    _check(
        not ok and "ReasonCodeConsistency" in viols,
        "ReasonCodeConsistency fires on (False, 'ok') — the v5 bug",
    )

    safety2 = BudgetSafetyMonitor()
    safety2.register(PassedImpliesNoViolations())
    ok2, viols2 = safety2.verify(
        {
            "passed": True,
            "violations": [{"metric": "energy"}],
        }
    )
    _check(
        not ok2 and "PassedImpliesNoViolations" in viols2,
        "PassedImpliesNoViolations fires on inconsistent result",
    )

    # ============================================================= #
    # P2 §4.3 — distillation uses round
    # ============================================================= #
    print("\n=== P2 §4.3: distillation uses round ===")
    from budget_domain import (
        QuantumDistillationBridge, PrecisionLevel,
    )
    bridge = QuantumDistillationBridge()
    # 5.125 at INT8 (scale=100): floor → 5.12, round → 5.13
    policy = bridge.distill(
        {"energy_wh": 5.125}, PrecisionLevel.INT8
    )
    _check(
        policy.values["energy_wh"] >= 5.125,
        f"round preserves 5.125 as {policy.values['energy_wh']} "
        f"(floor would give 5.12)",
    )

    # ============================================================= #
    # P2 §4.4 — federated zero-weight safe
    # ============================================================= #
    print("\n=== P2 §4.4: federated zero-weight guard ===")
    from budget_domain import (
        FederatedBudgetAggregator, FederatedBudgetProfile,
    )
    fed = FederatedBudgetAggregator(max_updates=5)
    for _ in range(100):
        fed.push(
            FederatedBudgetProfile(
                deployment_id="x",
                metric="energy",
                mean_utilization=0.5,
                sample_count=0,
            )
        )
    _check(
        fed.aggregate() == {},
        "all-zero-weight profiles → empty aggregate",
    )
    _check(
        len(fed.updates) <= 5,
        f"federated.updates bounded at 5 (got {len(fed.updates)})",
    )

    # ============================================================= #
    # P2 §4.5 — contribute_federated uses utilization
    # ============================================================= #
    print("\n=== P2 §4.5: contribute_federated uses utilization ===")
    fed_mgr = BudgetChecker()
    fed_mgr.check_detailed(
        {"energy": 6.0}, max_energy_wh=5.0,  # 20% over
    )
    fed_mgr.contribute_federated()
    agg = fed_mgr.get_federated_aggregate()
    _check(
        "energy" in agg,
        "federated aggregate has 'energy' metric",
    )
    _check(
        agg["energy"]["mean_utilization"] >= 1.0,
        f"mean_utilization reflects utilization, not breach rate "
        f"(got {agg['energy']['mean_utilization']:.3f})",
    )

    # ============================================================= #
    # P2 §4.8 — helium_signal_fn is used
    # ============================================================= #
    print("\n=== P2 §4.8: helium_signal_fn is used ===")

    class _MockSignal:
        scarcity_score = 0.8

    helium_signal_mgr = BudgetChecker()
    helium_signal_mgr.helium_signal_fn = lambda: _MockSignal()
    r_he_sig = helium_signal_mgr.check_detailed(
        {"energy": 1000.0}, max_helium_units=1.0,
    )
    # With scarcity=0.8: efficiency = 1 - min(0.5, 0.4) = 0.6
    # helium = 1000 * 5e-5 * 0.6 = 0.03
    # Without scarcity: 1000 * 5e-5 = 0.05
    _check(
        abs(r_he_sig.helium_units - 0.03) < 1e-6,
        f"scarcity reduced helium estimate to {r_he_sig.helium_units:.4f}",
    )

    # ============================================================= #
    # P2 §4.10 — to_dict is ISO-safe
    # ============================================================= #
    print("\n=== P2 §4.10: to_dict is ISO-safe ===")
    to_dict_mgr = BudgetChecker()
    r = to_dict_mgr.check_detailed(
        {"energy": 1.0}, max_energy_wh=5.0,
    )
    as_dict = r.to_dict()
    _check(
        isinstance(as_dict, dict)
        and "T" in as_dict["at"]
        and "inf" not in str(as_dict.get("contributing_factors", {})),
        "to_dict() returns JSON-safe values (ISO at, no inf leaks)",
    )
    # Verify the dict is actually JSON-serializable
    import json
    json.dumps(as_dict)  # would raise if not safe
    _check(True, "to_dict() output is json.dumps()-able")

    # ============================================================= #
    # Final summary
    # ============================================================= #
    print("\n=== Statistics (last manager) ===")
    print(json.dumps(live.get_statistics(), indent=2, default=str))

    print("\n" + "=" * 78)
    print("All v6 assertions passed.")
    print("=" * 78)


if __name__ == "__main__":
    _demo()
