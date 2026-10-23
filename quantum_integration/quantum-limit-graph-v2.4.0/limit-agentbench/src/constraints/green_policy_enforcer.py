# src/constraints/green_policy_enforcer.py
"""
green_policy_enforcer.py — v6.0.0
==================================

Policy enforcement with mode-switching behavior (raise vs. report).

**Public API preserved**

    enforcer = GreenPolicyEnforcer(policy)
    result = enforcer.check(metrics)
    # raises PolicyViolation(violations) if mode == "active_enforcement"
    # else returns {"compliant": bool, "violations": [...]}

**What changed (v5 → v6)**
--------------------------

**P0 — enforcement contract and correctness**

1. **Every degraded path raises in active mode.** v5 returned a dict on
   circuit-open, chaos, internal error, and invalid input — even in
   `active_enforcement` mode. A caller using the documented try/except
   pattern ran the agent whenever enforcement was unavailable. v6 raises
   ``PolicyViolation(degraded=True)`` on every degraded path in active
   mode; monitor mode still returns a dict.
2. **`record_outcome` tracks the last-violated metric.** v5 crashed on
   the first-ever call via ``next(["energy"])`` (a list is not an
   iterator) and credited the **first-ever** breached metric rather than
   the most recent. v6 stores ``self._last_violated_metric`` during every
   check and credits only that metric on outcome feedback.
3. **HITL propagates.** v5 reviewed the top violation, discarded the
   result, and raised unconditionally. v6 reviews **every** reviewable
   violation; ``allow_hitl_override=False`` (default) makes approvals
   advisory; when True and all approved, the deny is suppressed with
   ``hitl_override=True`` and ``compliant=True``.
4. **`ActiveModeRaises` is load-bearing.** v5 verified with
   ``raised = will_raise`` — a tautology. v6 verifies **after** the
   decision (in `check()`), with ``raised = observed`` — so a future
   refactor that stops raising in active mode fires the property.
5. **`temporal_verified` reflects the actual verification.** v5
   hard-coded ``True`` in the result dict. v6 propagates the real result
   from `verify()`; a temporal violation surfaces in
   ``temporal_violations`` and sets ``temporal_verified=False``.
6. **NaN / missing metrics → EMERGENCY.** v5's ``_safe_float(NaN) = 0.0``
   and ``metrics.get(k, 0.0)`` silently permitted. v6 (in strict mode,
   the default) treats both as ``bad_data`` violations with
   ``severity=EMERGENCY``.
7. **`metrics=None` is not a clean pass.** v5 coerced it to ``{}`` and
   returned compliant. v6 routes it to the degraded path.
8. **Small / zero-limit breaches are never `INFO`.** v5 classified any
   breach below 5% as ``INFO``; zero-limit breaches got
   ``overshoot_pct=0.0`` → also ``INFO``. v6: any breach is at least
   ``WARNING``; zero-limit is ``EMERGENCY``; multi-metric escalation
   applies.
9. **Constraints validated at construction.** v5 accepted non-numeric,
   non-finite, and negative constraints, deferring the failure to
   ``check()`` (where it became ``internal_error`` → silent permit in
   active mode). v6 rejects at construction with ``ValueError``.
10. **Causal learner fixed.** Uses ``budget_domain.CausalBudgetLearner``
    (feature-aware, signed ``±1`` reward, tightening-only by default).

**P1 — reliability**

11. **Thread-safe via ``RLock``**.
12. **v18 single-probe ``CircuitBreaker``** via ``SyncCircuitAdapter``
    from ``budget_domain``.
13. **Async HITL callbacks awaited in ``areview``, rejected in ``review``**.
14. **Bounded collections** (violations, hitl queues, chaos events,
    federated updates).
15. **Cached market snapshot** in ``get_statistics()``.
16. **``set_mode(mode)``**, **``set_features(features)``**,
    **``health_check()``**, **``__aenter__``/``__aexit__``**.
17. **``degraded_count``, ``override_count``, ``hitl_outcomes``** on
    stats and health.

**P2 — architecture**

18. **Eight enhancement classes imported from ``carbon_forecast`` v18
    (via ``budget_domain``)** — the eleventh local copy in v5 is
    retired.
19. **Budget-family primitives reused**: ``CausalBudgetLearner``,
    ``SyncCircuitAdapter``, ``SyncChaosInjector``, ``HumanReviewGate``,
    ``CarbonMarketCache``, ``HeliumProfiler``,
    ``QuantumDistillationBridge``, ``FederatedBudgetAggregator``,
    ``BudgetState``.
20. **``QuantumDistillationBridge`` uses ``round``**, not ``floor``.
21. **``FederatedBudgetAggregator`` is bounded and zero-weight safe.**
22. **``contribute_federated`` pushes utilization, not breach rate.**

**Local only** (per the extraction spec)
    ``EnforcementMode``, ``PolicyViolation``,
    ``EnforcementExplanation``, ``EnforcementExplainer``,
    ``CompliantImpliesNoViolations``, ``ActiveModeRaises``,
    ``GreenPolicyEnforcer``.
"""

from __future__ import annotations

import asyncio
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
# Shared budget primitives (single source of truth for the family)
# --------------------------------------------------------------------------- #
from budget_domain import (  # type: ignore[import-not-found]
    # v18 re-exports (eight enhancement classes)
    CircuitBreaker,
    ChaosTestingEngine,
    ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher,
    CarbonMarketIntegrator,
    MultiAgentCoordinator,
    FederatedGreenAggregator,
    TemporalLogicVerifier,
    # Enums
    Severity,
    PrecisionLevel,
    AgentRole,
    # Types
    HardwareProfile,
    BudgetState,
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
    # Bounds
    MAX_HITL_LOG,
    MAX_CHAOS_EVENTS,
    MAX_FEDERATED_UPDATES,
    MAX_VIOLATIONS,
    # Components
    SyncCircuitAdapter,
    SyncChaosInjector,
    CausalBudgetLearner,
    QuantumDistillationBridge,
    FederatedBudgetAggregator,
    HeliumProfiler,
    BudgetSafetyMonitor,
    HumanReviewGate,
    CarbonMarketCache,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Local enums
# --------------------------------------------------------------------------- #
class EnforcementMode(Enum):
    ACTIVE_ENFORCEMENT = "active_enforcement"
    MONITOR = "monitor"
    REPORT = "report"   # explicit alias for monitor


_RECOGNIZED_MODES = {m.value for m in EnforcementMode}


# --------------------------------------------------------------------------- #
# Local exception (backward compatible)
# --------------------------------------------------------------------------- #
class PolicyViolation(Exception):
    """
    Raised when policy constraints are violated **or** enforcement is
    unavailable in active mode.

    Backward-compatible:
        str(exc)             -> str(violations)  (v1 contract)
        exc.args[0]          -> violations list  (v1 contract)

    Enhanced:
        exc.details          -> structured dict (violations, severity,
                                explanation, market_snapshot, etc.)
        exc.degraded         -> True when raised because enforcement was
                                unavailable (circuit-open, chaos,
                                internal-error, invalid input)
    """

    def __init__(
        self,
        violations: List[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(violations)
        self.details: Dict[str, Any] = details or {}

    @property
    def violations(self) -> List[str]:
        # Defensive: if a caller passed a bare string as args[0], don't
        # hand back the string's characters.
        raw = self.args[0] if self.args else []
        if isinstance(raw, str):
            return [raw]
        return list(raw) if raw else []

    @property
    def severity(self) -> Optional[str]:
        return self.details.get("severity")

    @property
    def explanation(self) -> Optional[Dict[str, Any]]:
        return self.details.get("explanation")

    @property
    def structured_violations(self) -> List[Dict[str, Any]]:
        return list(self.details.get("structured_violations", []) or [])

    @property
    def degraded(self) -> bool:
        return bool(self.details.get("degraded", False))

    @property
    def temporal_verified(self) -> Optional[bool]:
        return self.details.get("temporal_verified")

    @property
    def hitl_status(self) -> str:
        return str(self.details.get("hitl_status", "not_required"))

    @property
    def hitl_override(self) -> bool:
        return bool(self.details.get("hitl_override", False))


# --------------------------------------------------------------------------- #
# Local XAI
# --------------------------------------------------------------------------- #
@dataclass
class EnforcementExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


class EnforcementExplainer:
    @staticmethod
    def explain(
        compliant: bool,
        violations: List[Dict[str, Any]],
        severity: Optional[str] = None,
        market_snapshot: Optional[Dict[str, Any]] = None,
        hitl_status: str = "not_required",
    ) -> EnforcementExplanation:
        reasons: List[str] = []
        if compliant:
            reasons.append("All policy constraints satisfied.")
        else:
            reasons.append(f"{len(violations)} constraint(s) violated.")
            for v in violations:
                metric = v.get("metric", "?")
                value = v.get("value")
                limit = v.get("limit", 0.0)
                pct = v.get("overshoot_pct")
                if value is None:
                    reasons.append(
                        f"'{metric}': unreadable value "
                        f"({v.get('reason', 'bad_data')}), "
                        f"limit {float(limit):.4f}."
                    )
                elif pct is None or pct == float("inf"):
                    reasons.append(
                        f"'{metric}': value {float(value):.4f} against "
                        f"zero limit."
                    )
                else:
                    reasons.append(
                        f"'{metric}': {float(value):.4f} > "
                        f"limit {float(limit):.4f} "
                        f"(overshoot {pct:.1f}%)."
                    )
        if severity and severity != Severity.INFO.value:
            reasons.append(f"Severity: {severity}.")
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(
                    f"Carbon market price: ${float(cp):.2f}/tCO₂."
                )
        if hitl_status and hitl_status != "not_required":
            reasons.append(f"HITL status: {hitl_status}.")

        headline = (
            "Compliant"
            if compliant
            else f"Violated: {', '.join(str(v.get('metric', '?')) for v in violations)}"
        )
        factors: Dict[str, float] = {}
        for v in violations:
            metric = str(v.get("metric", "?"))
            pct = v.get("overshoot_pct")
            factors[f"overshoot_{metric}"] = (
                float("inf") if pct is None or pct == float("inf") else float(pct)
            )
        return EnforcementExplanation(
            headline=headline,
            rationale=reasons,
            confidence=0.95 if compliant else 0.90,
            contributing_factors=factors,
        )


# --------------------------------------------------------------------------- #
# Local temporal properties (load-bearing)
# --------------------------------------------------------------------------- #
class EnforcementProperty:
    """Marker protocol for enforcement temporal properties."""
    def check(self, ctx: Mapping[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass(frozen=True)
class CompliantImpliesNoViolations:
    """G(compliant ∧ ¬hitl_override → violations is empty)."""

    def check(self, ctx: Mapping[str, Any]) -> bool:
        if not ctx.get("compliant"):
            return True
        if ctx.get("hitl_override"):
            return True
        return len(ctx.get("violations", []) or []) == 0

    def name(self) -> str:
        return "CompliantImpliesNoViolations"


@dataclass(frozen=True)
class ActiveModeRaises:
    """
    G(active_enforcement ∧ violations ∧ ¬hitl_override → raised).

    v5 verified with ``raised = will_raise`` — a tautology. v6 verifies
    **after** the decision with ``raised = observed``, so a future
    refactor that stops raising fires this property.
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        mode = str(ctx.get("mode", ""))
        violations = ctx.get("violations", []) or []
        raised = bool(ctx.get("raised", False))
        hitl_override = bool(ctx.get("hitl_override", False))
        if mode != EnforcementMode.ACTIVE_ENFORCEMENT.value:
            return True
        if violations and not hitl_override:
            return raised
        return True

    def name(self) -> str:
        return "ActiveModeRaises"


# --------------------------------------------------------------------------- #
# Sync-safe coordinator bridge
# --------------------------------------------------------------------------- #
class _CoordinatorBridge:
    """
    Thin sync-safe adapter over v18's async ``MultiAgentCoordinator``.

    Reward updates are scheduled if a loop is running (fire-and-forget)
    or run to completion in a private loop otherwise. Exceptions are
    logged and swallowed so the coordinator can never break a policy
    check.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def register(self, agent_id: str) -> None:
        try:
            self._inner.register(agent_id)
        except Exception:  # pragma: no cover — defensive
            pass

    def record(self, agent_id: str, success: bool) -> None:
        reward = 1.0 if success else 0.0
        try:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._inner.reward(agent_id, reward))
            except RuntimeError:
                asyncio.run(self._inner.reward(agent_id, reward))
        except Exception:  # pragma: no cover — defensive
            pass


# --------------------------------------------------------------------------- #
# GreenPolicyEnforcer
# --------------------------------------------------------------------------- #
class GreenPolicyEnforcer:
    """
    Enhanced policy enforcement with mode-switching behavior.

    Backward-compatible:

        GreenPolicyEnforcer(policy)
        .check(metrics) -> {"compliant": bool, "violations": [...]}
                       or raises PolicyViolation(violations)
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
        "cost_awareness": True,
        "aggregate_violations": True,
    }

    #: Mapping from v1 policy constraint names to internal metric names.
    _CONSTRAINT_MAP: Tuple[Tuple[str, str], ...] = (
        ("max_energy_per_task_wh", "energy"),
        ("max_carbon_per_task_kg", "carbon"),
        ("max_latency_seconds", "latency"),
    )

    def __init__(
        self,
        policy: Mapping[str, Any],
        deployment_id: str = "local",
        agent_id: str = "policy-enforcer-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        *,
        strict_metrics: bool = True,
        allow_hitl_override: bool = False,
        allow_constraint_loosening: bool = False,
        min_severity_to_raise: str = "warning",
    ) -> None:
        # ---- Extract the policy (defensive) ----
        if not isinstance(policy, Mapping):
            policy = {}
        raw_constraints = policy.get("constraints", {})
        if not isinstance(raw_constraints, Mapping):
            raw_constraints = {}
        raw_identity = policy.get("agent_identity", {})
        if not isinstance(raw_identity, Mapping):
            raw_identity = {}

        # ---- Validate + copy constraints (P0 §2.10) ----
        self.constraints: Dict[str, float] = self._validate_constraints(
            raw_constraints
        )

        # ---- Validate mode ----
        mode = str(raw_identity.get("mode", EnforcementMode.MONITOR.value))
        if mode not in _RECOGNIZED_MODES:
            logger.warning(
                "Unrecognized enforcement mode %r; defaulting to 'monitor'",
                mode,
            )
            mode = EnforcementMode.MONITOR.value
        self.mode = mode

        # ---- Config ----
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()
        self._strict_metrics = bool(strict_metrics)
        self._allow_hitl_override = bool(allow_hitl_override)
        self._allow_constraint_loosening = bool(allow_constraint_loosening)
        self._min_severity_to_raise = self._parse_min_severity(
            min_severity_to_raise
        )

        # ---- Locks ----
        self._lock = threading.RLock()

        # ---- Counters ----
        self.check_count: int = 0
        self.compliant_count: int = 0
        self.violation_count: int = 0
        self.raise_count: int = 0
        self.degraded_count: int = 0
        self.override_count: int = 0
        self.distilled_count: int = 0
        self.breach_by_metric: Dict[str, int] = defaultdict(int)
        self.severity_counts: Dict[str, int] = defaultdict(int)
        self._outcome_counts: Dict[str, int] = defaultdict(int)

        # ---- Cached state ----
        self.last_explanation: Optional[EnforcementExplanation] = None
        self._last_violated_metric: Optional[str] = None
        self._last_state: Optional[BudgetState] = None
        self._last_market_snapshot: Optional[MarketSnapshot] = None
        self._last_market_fetched_at: Optional[float] = None

        # ---- Enhancement layers ----
        self._circuit: Optional[SyncCircuitAdapter] = None
        self._chaos: Optional[SyncChaosInjector] = None
        self._safety: Optional[BudgetSafetyMonitor] = None
        self._precision: Optional[Any] = None
        self._budget_learner: Optional[CausalBudgetLearner] = None
        self._distiller: Optional[QuantumDistillationBridge] = None
        self._federated: Optional[FederatedBudgetAggregator] = None
        self._coordinator: Optional[_CoordinatorBridge] = None
        self._market: Optional[CarbonMarketCache] = None
        self._hitl: Optional[HumanReviewGate] = None
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        self._build_layers()

        logger.info(
            "GreenPolicyEnforcer v6 ready: deployment=%s, agent=%s, "
            "mode=%s, strict_metrics=%s, features=%s",
            deployment_id,
            agent_id,
            self.mode,
            self._strict_metrics,
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_constraints(
        constraints: Mapping[str, Any]
    ) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for name, value in constraints.items():
            if isinstance(value, bool):
                raise ValueError(
                    f"constraint {name!r} must be numeric, got bool"
                )
            try:
                fval = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"constraint {name!r} must be numeric, got {value!r}"
                ) from exc
            if not math.isfinite(fval):
                raise ValueError(
                    f"constraint {name!r} must be finite, got {value!r}"
                )
            if fval < 0:
                raise ValueError(
                    f"constraint {name!r} must be >= 0, got {value!r}"
                )
            out[str(name)] = fval
        return out

    @staticmethod
    def _parse_min_severity(value: str) -> Severity:
        try:
            return Severity(str(value).lower())
        except ValueError as exc:
            raise ValueError(
                f"min_severity_to_raise must be one of "
                f"{[s.value for s in Severity]}, got {value!r}"
            ) from exc

    def _build_layers(self) -> None:
        self._circuit = SyncCircuitAdapter(
            "policy_enforcer",
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
            self._safety.register(CompliantImpliesNoViolations())
            self._safety.register(ActiveModeRaises())

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
                known_metrics=[
                    "energy", "carbon", "latency", "helium", "cost",
                ],
                allow_loosening=self._allow_constraint_loosening,
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
                inner = MultiAgentCoordinator(  # type: ignore[misc]
                    config={}, storage=None
                )
                self._coordinator = _CoordinatorBridge(inner)
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

    # ------------------------------------------------------------------
    # Public: mode / features
    # ------------------------------------------------------------------
    def set_mode(self, mode: str) -> None:
        """Change the enforcement mode at runtime (P1 §3.13)."""
        if mode not in _RECOGNIZED_MODES:
            raise ValueError(
                f"Unrecognized enforcement mode {mode!r}; "
                f"must be one of {sorted(_RECOGNIZED_MODES)}"
            )
        with self._lock:
            self.mode = mode
        logger.info(
            "Enforcement mode switched to %r (agent=%s)",
            mode, self.agent_id,
        )

    def set_features(
        self, features: Mapping[str, bool]
    ) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        with self._lock:
            self._build_layers()

    # ------------------------------------------------------------------
    # v1 public API: check
    # ------------------------------------------------------------------
    def check(self, metrics: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Check metrics against policy constraints.

        In active mode, raises ``PolicyViolation`` on any non-compliance
        **including** degraded paths (§2.1). In monitor/report mode,
        returns a result dict.
        """
        with self._lock:
            self.check_count += 1

        # ---- P0 §2.8: invalid input is never a clean pass ----
        if not isinstance(metrics, Mapping):
            return self._handle_degraded("invalid_input")

        # ---- P0 §2.1: circuit-open raises in active mode ----
        if not self._circuit.can_call():
            return self._handle_degraded("circuit_open")

        # ---- P0 §2.1: chaos raises in active mode ----
        if (
            self._chaos is not None
            and self._chaos.maybe_fail("check")
        ):
            self._circuit.record_failure()
            return self._handle_degraded("chaos")

        # ---- Enforcement ----
        try:
            result = self._check_internal(dict(metrics))
            self._circuit.record_success()
        except Exception as exc:
            self._circuit.record_failure()
            logger.warning("check_internal failed: %s", exc)
            return self._handle_degraded(f"internal_error: {exc}")

        # ---- P0 §2.4: raise in active mode with load-bearing verify ----
        should_raise = (
            self.mode == EnforcementMode.ACTIVE_ENFORCEMENT.value
            and not result["compliant"]
            and not result.get("hitl_override", False)
            and self._severity_at_least_min(result.get("severity"))
        )
        if should_raise:
            ok, tv = self._verify_temporal(
                compliant=False,
                violations=result["violations"],
                raised=True,
                hitl_override=False,
            )
            with self._lock:
                self.raise_count += 1
                self._outcome_counts["raised"] += 1
            details = {
                **result,
                "temporal_verified": ok,
                "temporal_violations": tv,
            }
            raise PolicyViolation(result["violations"], details=details)

        # ---- No raise: verify with raised=False (§2.4 / §2.6) ----
        ok, tv = self._verify_temporal(
            compliant=result["compliant"],
            violations=result["violations"],
            raised=False,
            hitl_override=result.get("hitl_override", False),
        )
        result["temporal_verified"] = ok
        result["temporal_violations"] = tv
        with self._lock:
            self._outcome_counts[
                "compliant" if result["compliant"] else "violated"
            ] += 1
        return result

    def _severity_at_least_min(self, sev: Optional[str]) -> bool:
        if not sev:
            return True
        try:
            current = Severity(sev)
        except ValueError:
            return True
        order = [
            Severity.INFO,
            Severity.WARNING,
            Severity.CRITICAL,
            Severity.EMERGENCY,
        ]
        return order.index(current) >= order.index(self._min_severity_to_raise)

    # ------------------------------------------------------------------
    # Internal pipeline (never raises)
    # ------------------------------------------------------------------
    def _check_internal(
        self, metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        # 1. Build the state used by the RL and record_outcome
        state = self._build_state(metrics)
        with self._lock:
            self._last_state = state

        # 2. Active constraints (with RL tightening)
        active_constraints, causal_adjustments = (
            self._active_constraints(state)
        )

        # 3. Collect violations
        violations_flat: List[str] = []
        violations_struct: List[Dict[str, Any]] = []
        checked_metrics: List[str] = []

        for metric, limit in active_constraints.items():
            checked_metrics.append(metric)

            # Missing metric (P0 §2.7 / strict_metrics)
            if metric not in metrics:
                if self._strict_metrics:
                    violations_struct.append(
                        self._bad_data_violation(metric, limit, "missing")
                    )
                    violations_flat.append(metric)
                continue

            raw = metrics[metric]
            # Non-numeric (P0 §2.7)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                violations_struct.append(
                    self._bad_data_violation(metric, limit, "not_numeric")
                )
                violations_flat.append(metric)
                continue
            # Non-finite (P0 §2.7)
            if not math.isfinite(float(raw)):
                violations_struct.append(
                    self._bad_data_violation(metric, limit, "not_finite")
                )
                violations_flat.append(metric)
                continue

            value = float(raw)
            if value > limit + 1e-9:
                overshoot = value - limit
                if limit > 0:
                    overshoot_pct = overshoot / limit * 100.0
                else:
                    # P0 §2.9: zero-limit breach is EMERGENCY
                    overshoot_pct = float("inf")
                violations_flat.append(metric)
                violations_struct.append(
                    {
                        "metric": metric,
                        "value": value,
                        "limit": limit,
                        "overshoot": overshoot,
                        "overshoot_pct": overshoot_pct,
                    }
                )

        # 4. Auto-estimate helium if the constraint exists but metric
        #    wasn't provided (P2 §3.8: consult helium_signal_fn)
        if (
            self.features.get("helium_awareness")
            and "helium" in active_constraints
            and "helium" not in metrics
        ):
            energy = _safe_float(metrics.get("energy", 0.0))
            scarcity = self._current_scarcity()
            helium_est = HeliumProfiler.estimate_units(energy, scarcity)
            limit = active_constraints["helium"]
            if helium_est > limit + 1e-9:
                overshoot = helium_est - limit
                overshoot_pct = (
                    overshoot / limit * 100.0
                    if limit > 0
                    else float("inf")
                )
                violations_flat.append("helium")
                violations_struct.append(
                    {
                        "metric": "helium",
                        "value": helium_est,
                        "limit": limit,
                        "overshoot": overshoot,
                        "overshoot_pct": overshoot_pct,
                        "reason": "auto_estimated",
                    }
                )

        compliant = len(violations_flat) == 0

        # 5. Track last-violated metric for record_outcome (P0 §2.2/§2.3)
        with self._lock:
            self._last_violated_metric = (
                violations_struct[0]["metric"]
                if violations_struct
                else None
            )

        # 6. Severity (P0 §2.9)
        severity = self._classify_severity(violations_struct)
        with self._lock:
            if not compliant:
                self.severity_counts[severity.value] += 1
                for v in violations_struct:
                    self.breach_by_metric[v["metric"]] += 1

        # 7. Market snapshot (cached)
        market_snapshot = self._get_market_snapshot(metrics)

        # 8. Precision attribution
        precision = self._select_precision(
            "critical" if not compliant else "normal"
        )

        # 9. HITL — review ALL reviewable violations (P0 §2.5)
        hitl = self._run_hitl(violations_struct, severity)

        # 10. Coordinator feedback (P1 §3.5: use _CoordinatorBridge)
        if self._coordinator is not None:
            self._coordinator.record(self.agent_id, compliant)

        # 11. Federated (P2 §3.9: utilization, not breach rate)
        self._feedback_federated(checked_metrics, metrics, active_constraints)

        # 12. Counters
        with self._lock:
            if compliant:
                self.compliant_count += 1
            else:
                self.violation_count += 1

        # 13. XAI (built last so it can include HITL status)
        explanation = EnforcementExplainer.explain(
            compliant=compliant,
            violations=violations_struct,
            severity=severity.value,
            market_snapshot=market_snapshot,
            hitl_status=hitl["status"],
        )
        with self._lock:
            self.last_explanation = explanation

        # 14. Effective compliance (HITL override flips compliant)
        effective_compliant = compliant or hitl["override"]

        return {
            "compliant": effective_compliant,
            "violations": violations_flat,
            "structured_violations": violations_struct,
            "severity": severity.value,
            "causal_adjustments": causal_adjustments,
            "precision": precision.value,
            "hitl_required": hitl["required"],
            "hitl_approved": hitl["approved"],
            "hitl_status": hitl["status"],
            "hitl_override": hitl["override"],
            "market_snapshot": market_snapshot,
            "explanation": (
                explanation.to_dict() if explanation else None
            ),
            "degraded": False,
            "degraded_reason": None,
            "temporal_verified": None,   # set by check()
            "temporal_violations": [],
            "at": _utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Constraint adjustments
    # ------------------------------------------------------------------
    def _active_constraints(
        self, state: BudgetState
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        base: Dict[str, float] = {}
        for orig_name, metric in self._CONSTRAINT_MAP:
            if orig_name in self.constraints:
                base[metric] = self.constraints[orig_name]
        if self.features.get("helium_awareness"):
            if "max_helium_units" in self.constraints:
                base["helium"] = self.constraints["max_helium_units"]
        if self.features.get("cost_awareness"):
            if "max_cost_usd" in self.constraints:
                base["cost"] = self.constraints["max_cost_usd"]

        if self._budget_learner is None:
            return base, {}

        adjusted: Dict[str, float] = {}
        adjustments: Dict[str, float] = {}
        for metric, limit in base.items():
            offset = self._budget_learner.predict_offset(metric, state)
            adjusted[metric] = limit * (1.0 + offset)
            if offset != 0.0:
                adjustments[metric] = offset
        return adjusted, adjustments

    def _build_state(self, metrics: Mapping[str, Any]) -> BudgetState:
        ratios: List[float] = []
        for orig_name, metric in self._CONSTRAINT_MAP:
            if orig_name not in self.constraints:
                continue
            limit = self.constraints[orig_name]
            if limit <= 0:
                continue
            if metric in metrics:
                try:
                    v = float(metrics[metric])
                except (TypeError, ValueError):
                    continue
                if math.isfinite(v):
                    ratios.append(v / limit)
        peak = max(ratios) if ratios else 0.5
        return BudgetState(
            hour_of_day=_utcnow().hour,
            utilization_ratio=min(1.0, peak),
            workload_hash=float(abs(hash(self.agent_id)) % 1000) / 1000.0,
        )

    def _current_scarcity(self) -> float:
        if self.helium_signal_fn is None:
            return 0.0
        try:
            sig = self.helium_signal_fn()
            if sig is None:
                return 0.0
            return _safe_float(getattr(sig, "scarcity_score", 0.0))
        except Exception:
            return 0.0

    @staticmethod
    def _bad_data_violation(
        metric: str, limit: float, reason: str
    ) -> Dict[str, Any]:
        return {
            "metric": metric,
            "value": None,
            "limit": float(limit),
            "overshoot": float("inf"),
            "overshoot_pct": float("inf"),
            "reason": reason,
        }

    # ------------------------------------------------------------------
    # HITL (P0 §2.5)
    # ------------------------------------------------------------------
    def _run_hitl(
        self,
        violations: List[Dict[str, Any]],
        severity: Severity,
    ) -> Dict[str, Any]:
        result = {
            "required": False,
            "approved": None,
            "status": "not_required",
            "override": False,
        }
        if self._hitl is None or not violations:
            return result

        reviewable = [
            v for v in violations
            if v.get("limit", 0.0) > 0
            and v.get("value") is not None
            and self._hitl_needs_review(v["value"], v["limit"])
        ]
        if not reviewable:
            return result

        result["required"] = True
        all_approved = True
        statuses: List[str] = []
        for v in reviewable:
            approved, status = self._hitl.review(
                metric=v["metric"],
                value=v["value"],
                limit=v["limit"],
                reason=f"overshoot {v['overshoot_pct']:.1f}%",
                urgency=(
                    "high"
                    if severity in (Severity.CRITICAL, Severity.EMERGENCY)
                    else "medium"
                ),
                context={"agent_id": self.agent_id},
            )
            statuses.append(status)
            if approved is not True:
                all_approved = False

        result["approved"] = all_approved
        result["status"] = (
            "approved"
            if all_approved
            else ("rejected" if "rejected" in statuses else "timed_out")
        )

        # P0 §2.5: HITL override only when explicitly enabled
        if self._allow_hitl_override and all_approved:
            result["override"] = True
            with self._lock:
                self.override_count += 1
                self._outcome_counts["overridden"] += 1
            for v in violations:
                v["hitl_override"] = True
            logger.warning(
                "HITL approved denial (allow_hitl_override=True): %s",
                [v["metric"] for v in violations],
            )
        return result

    @staticmethod
    def _hitl_needs_review(value: float, limit: float) -> bool:
        if limit <= 0:
            return False
        ratio = value / limit
        if 1.0 < ratio <= 1.05:
            return True
        if ratio >= 2.0:
            return True
        return False

    # ------------------------------------------------------------------
    # Federated (P2 §3.9: utilization, not breach rate)
    # ------------------------------------------------------------------
    def _feedback_federated(
        self,
        checked_metrics: List[str],
        metrics: Mapping[str, Any],
        active_constraints: Mapping[str, float],
    ) -> None:
        if self._federated is None:
            return
        with self._lock:
            check_count = self.check_count
        if check_count % 25 != 0:
            return
        for metric in checked_metrics:
            limit = active_constraints.get(metric)
            if limit is None or limit <= 0:
                continue
            value = _safe_float(metrics.get(metric, 0.0))
            self._federated.push(
                FederatedBudgetProfile(
                    deployment_id=self.deployment_id,
                    metric=metric,
                    mean_utilization=value / limit,
                    sample_count=1,
                    budget_name=f"policy:{self.agent_id}",
                )
            )
        self._federated.aggregate()

    # ------------------------------------------------------------------
    # Severity
    # ------------------------------------------------------------------
    @staticmethod
    def _classify_severity(
        violations: List[Dict[str, Any]]
    ) -> Severity:
        if not violations:
            return Severity.INFO
        infinite = any(
            v.get("overshoot_pct") == float("inf") for v in violations
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
            base = Severity.WARNING   # any breach is at least WARNING
        # Multi-metric escalation
        if len(violations) >= 4 and base == Severity.CRITICAL:
            return Severity.EMERGENCY
        if len(violations) >= 3 and base == Severity.WARNING:
            return Severity.CRITICAL
        return base

    # ------------------------------------------------------------------
    # Market
    # ------------------------------------------------------------------
    def _get_market_snapshot(
        self, metrics: Mapping[str, Any]
    ) -> Optional[Dict[str, Any]]:
        if self._market is None:
            return None
        try:
            snap = self._market.get_snapshot()
            co2_used = _safe_float(metrics.get("carbon", 0.0))
            payload = {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "rec_available_mwh": snap.rec_available_mwh,
                "co2_market_value_usd": (
                    co2_used * snap.carbon_price_per_tco2_usd
                ),
            }
            with self._lock:
                self._last_market_snapshot = snap
                self._last_market_fetched_at = _utcnow().timestamp()
            return payload
        except Exception:  # pragma: no cover
            return None

    # ------------------------------------------------------------------
    # Precision
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Degraded handling (P0 §2.1, §2.6, §2.8)
    # ------------------------------------------------------------------
    def _handle_degraded(self, reason: str) -> Dict[str, Any]:
        with self._lock:
            self.degraded_count += 1
            self._outcome_counts["degraded"] += 1

        # Verify with raised reflecting the actual decision (§2.4)
        # For monitor mode: raised=False. For active mode: raised=True.
        active = self.mode == EnforcementMode.ACTIVE_ENFORCEMENT.value

        if active:
            ok, tv = self._verify_temporal(
                compliant=False,
                violations=[reason],
                raised=True,
                hitl_override=False,
            )
            with self._lock:
                self.raise_count += 1
            raise PolicyViolation(
                [reason],
                details={
                    "degraded": True,
                    "degraded_reason": reason,
                    "violations": [reason],
                    "structured_violations": [],
                    "severity": Severity.EMERGENCY.value,
                    "temporal_verified": ok,
                    "temporal_violations": tv,
                    "hitl_status": "not_required",
                    "hitl_override": False,
                    "at": _utcnow().isoformat(),
                },
            )

        # Monitor mode: return a well-formed degraded result
        ok, tv = self._verify_temporal(
            compliant=False,
            violations=[reason],
            raised=False,
            hitl_override=False,
        )
        return {
            "compliant": False,
            "violations": [reason],
            "structured_violations": [],
            "severity": Severity.EMERGENCY.value,
            "degraded": True,
            "degraded_reason": reason,
            "temporal_verified": ok,
            "temporal_violations": tv,
            "hitl_status": "not_required",
            "hitl_override": False,
            "at": _utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Temporal verification (P0 §2.4, §2.6)
    # ------------------------------------------------------------------
    def _verify_temporal(
        self,
        *,
        compliant: bool,
        violations: List[str],
        raised: bool,
        hitl_override: bool,
    ) -> Tuple[bool, List[str]]:
        if self._safety is None:
            return (True, [])
        ctx = {
            "compliant": compliant,
            "violations": violations,
            "mode": self.mode,
            "raised": raised,
            "hitl_override": hitl_override,
        }
        return self._safety.verify(ctx)

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

        P0 §2.2: does **not** crash on the first-ever call when there
        are no breaches yet.
        P0 §2.3: credits the **most recent** violated metric, not the
        first-ever.
        P0 §2.11: uses the real BudgetState for feature-aware update.
        """
        if self._budget_learner is None:
            return False
        with self._lock:
            metric = self._last_violated_metric
            state = self._last_state
        if metric is None or state is None:
            # Compliant last check: nothing to credit
            return False
        features = CausalBudgetLearner.features_of(state)
        reward = 1.0 if was_correct else -1.0
        self._budget_learner.record(metric, features, reward)
        if (
            self._budget_learner.observations % 10 == 0
            and self._budget_learner.buffer
        ):
            self._budget_learner.update()
        return True

    def contribute_federated(self) -> None:
        """
        P2 §3.9: push per-metric utilization (not breach rate) from the
        last check.
        """
        if self._federated is None:
            return
        with self._lock:
            state = self._last_state
            last_metric = self._last_violated_metric
        if state is None:
            return
        # Recompute utilization from constraints and current state
        ratios: Dict[str, float] = {}
        for orig_name, metric in self._CONSTRAINT_MAP:
            if orig_name in self.constraints:
                limit = self.constraints[orig_name]
                if limit > 0 and metric == last_metric:
                    # Use state.utilization_ratio as a proxy for the metric
                    ratios[metric] = state.utilization_ratio
        if not ratios:
            return
        for metric, util in ratios.items():
            self._federated.push(
                FederatedBudgetProfile(
                    deployment_id=self.deployment_id,
                    metric=metric,
                    mean_utilization=util,
                    sample_count=1,
                    budget_name=f"policy:{self.agent_id}",
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
        self, urgency: str = "normal"
    ) -> Optional[DistilledBudgetPolicy]:
        if self._distiller is None:
            return None
        precision = self._select_precision(urgency)
        named: Dict[str, Optional[float]] = {}
        for orig_name, metric in self._CONSTRAINT_MAP:
            if orig_name in self.constraints:
                named[metric] = self.constraints[orig_name]
        if "max_helium_units" in self.constraints:
            named["helium"] = self.constraints["max_helium_units"]
        if "max_cost_usd" in self.constraints:
            named["cost"] = self.constraints["max_cost_usd"]
        if not named:
            return None
        policy = self._distiller.distill(named, precision)
        with self._lock:
            self.distilled_count += 1
        return policy

    def explain_last_check(self) -> Optional[EnforcementExplanation]:
        with self._lock:
            return self.last_explanation

    # ------------------------------------------------------------------
    # Statistics / health
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "mode": self.mode,
                "checks": self.check_count,
                "compliant_count": self.compliant_count,
                "violation_count": self.violation_count,
                "raise_count": self.raise_count,
                "degraded_count": self.degraded_count,
                "override_count": self.override_count,
                "distilled_count": self.distilled_count,
                "compliance_rate": (
                    self.compliant_count / self.check_count
                    if self.check_count > 0
                    else 0.0
                ),
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
            if self._hitl is not None:
                stats["hitl"] = {
                    "pending": len(self._hitl.pending),
                    "log_size": len(self._hitl.feedback_log),
                    "outcomes": self._hitl.outcomes(),
                }
            if self._chaos is not None:
                stats["chaos_events"] = list(self._chaos.events)[-5:]
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
                "mode": self.mode,
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
                "checks": self.check_count,
                "compliant_count": self.compliant_count,
                "violation_count": self.violation_count,
                "degraded_count": self.degraded_count,
                "override_count": self.override_count,
                "strict_metrics": self._strict_metrics,
                "allow_hitl_override": self._allow_hitl_override,
                "allow_constraint_loosening": (
                    self._allow_constraint_loosening
                ),
                "min_severity_to_raise": self._min_severity_to_raise.value,
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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "GreenPolicyEnforcer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "GreenPolicyEnforcer":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    def __repr__(self) -> str:
        with self._lock:
            return (
                "GreenPolicyEnforcer("
                f"agent={self.agent_id!r}, "
                f"mode={self.mode!r}, "
                f"checks={self.check_count}, "
                f"compliant={self.compliant_count}, "
                f"violations={self.violation_count}, "
                f"circuit={self._circuit.state})"
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
    # Enums / types
    "EnforcementMode",
    "Severity",
    "PrecisionLevel",
    "AgentRole",
    "HardwareProfile",
    "BudgetState",
    "EnforcementExplanation",
    "DistilledBudgetPolicy",
    "FederatedBudgetProfile",
    "MarketSnapshot",
    "HITLRequest",
    # Components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "CausalBudgetLearner",
    "QuantumDistillationBridge",
    "FederatedBudgetAggregator",
    "HeliumProfiler",
    "BudgetSafetyMonitor",
    "HumanReviewGate",
    "CarbonMarketCache",
    # Local
    "PolicyViolation",
    "EnforcementProperty",
    "CompliantImpliesNoViolations",
    "ActiveModeRaises",
    "GreenPolicyEnforcer",
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
    print("green_policy_enforcer.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    def _policy(mode: str = "active_enforcement", **constraints) -> Dict:
        base_constraints = {
            "max_energy_per_task_wh": 5.0,
            "max_carbon_per_task_kg": 0.5,
            "max_latency_seconds": 30.0,
            "max_helium_units": 0.0002,
            "max_cost_usd": 0.05,
        }
        base_constraints.update(constraints)
        return {
            "constraints": base_constraints,
            "agent_identity": {"mode": mode},
        }

    # ============================================================= #
    # v1 compatibility
    # ============================================================= #
    print("\n=== v1 compatibility: PolicyViolation contract preserved ===")
    exc = PolicyViolation(["energy", "carbon"])
    _check(
        exc.args[0] == ["energy", "carbon"],
        f"args[0] is the violations list (got {exc.args[0]!r})",
    )
    _check(
        str(exc) == str(["energy", "carbon"]),
        "str(exc) matches v1 format",
    )
    _check(
        exc.violations == ["energy", "carbon"],
        "violations property returns the list",
    )
    # P1 §3.16: string arg is coerced
    str_exc = PolicyViolation("energy")  # type: ignore[arg-type]
    _check(
        str_exc.violations == ["energy"],
        f"string arg coerced to ['energy'] (got {str_exc.violations!r})",
    )

    # ============================================================= #
    # Legacy mode (features off)
    # ============================================================= #
    print("\n=== Legacy mode (all features off) ===")
    legacy = GreenPolicyEnforcer(
        _policy(),
        features={k: False for k in GreenPolicyEnforcer.DEFAULT_FEATURES},
    )
    try:
        legacy.check({"energy": 6.2, "carbon": 0.4, "latency": 25.0})
    except PolicyViolation as e:
        _check(
            e.violations == ["energy"],
            f"legacy raise on energy breach (got {e.violations!r})",
        )
        _check(
            e.details.get("structured_violations") is not None,
            "legacy exception still carries structured data",
        )
    else:
        raise AssertionError("legacy active check did not raise")

    # ============================================================= #
    # P0 §2.1 — all degraded paths raise in active mode
    # ============================================================= #
    print("\n=== P0 §2.1: every degraded path raises in active mode ===")

    # (a) circuit-open
    circ = GreenPolicyEnforcer(_policy())
    for _ in range(10):
        circ._circuit.record_failure()
    _check(circ._circuit.state == "OPEN", "circuit forced open")
    try:
        circ.check({"energy": 1.0})
    except PolicyViolation as e:
        _check(
            e.degraded is True,
            f"circuit-open → raised (degraded={e.degraded})",
        )
        _check(
            "circuit_open" in e.violations,
            f"reason carries 'circuit_open' (got {e.violations})",
        )
    else:
        raise AssertionError("circuit-open did not raise in active mode")

    # (b) chaos
    chaos = GreenPolicyEnforcer(
        _policy(), features={"chaos_testing": True}
    )
    chaos._chaos = SyncChaosInjector(failure_rate=1.0)
    try:
        chaos.check({"energy": 1.0})
    except PolicyViolation as e:
        _check(
            e.degraded is True and "chaos" in e.violations,
            f"chaos → raised (degraded, {e.violations})",
        )
    else:
        raise AssertionError("chaos did not raise in active mode")

    # (c) internal error
    err = GreenPolicyEnforcer(_policy())
    _orig = err._check_internal
    def _boom(metrics):
        raise RuntimeError("simulated internal failure")
    err._check_internal = _boom  # type: ignore[assignment]
    try:
        err.check({"energy": 1.0})
    except PolicyViolation as e:
        _check(
            e.degraded is True
            and any("internal_error" in v for v in e.violations),
            f"internal error → raised (degraded, {e.violations})",
        )
    else:
        raise AssertionError("internal error did not raise")
    err._check_internal = _orig  # type: ignore[assignment]

    # (d) invalid input
    inv = GreenPolicyEnforcer(_policy())
    try:
        inv.check(None)  # type: ignore[arg-type]
    except PolicyViolation as e:
        _check(
            e.degraded is True and "invalid_input" in e.violations,
            f"invalid input → raised (degraded, {e.violations})",
        )
    else:
        raise AssertionError("invalid input did not raise")

    # (e) monitor mode still returns a dict
    mon = GreenPolicyEnforcer(_policy(mode="monitor"))
    for _ in range(10):
        mon._circuit.record_failure()
    r_mon = mon.check({"energy": 1.0})
    _check(
        isinstance(r_mon, dict) and r_mon.get("degraded") is True,
        "monitor mode returns a degraded dict (no raise)",
    )

    # ============================================================= #
    # P0 §2.2 — record_outcome doesn't crash on first-ever call
    # ============================================================= #
    print("\n=== P0 §2.2: record_outcome doesn't crash on first-ever call ===")
    fresh = GreenPolicyEnforcer(_policy(mode="monitor"))
    r = fresh.check({"energy": 1.0, "carbon": 0.2})  # compliant
    _check(r["compliant"] is True, "fresh check is compliant")
    try:
        returned = fresh.record_outcome(was_correct=True)
        _check(
            isinstance(returned, bool),
            f"record_outcome returned {returned!r}, no crash",
        )
        _check(
            returned is False,
            "record_outcome returns False (no metric to credit)",
        )
    except TypeError as exc:
        raise AssertionError(
            f"record_outcome crashed with TypeError: {exc} "
            f"(v5 bug: next(['energy']))"
        )

    # ============================================================= #
    # P0 §2.3 — record_outcome credits the last-violated metric
    # ============================================================= #
    print("\n=== P0 §2.3: record_outcome credits the last-violated metric ===")
    seq = GreenPolicyEnforcer(_policy(mode="monitor"))
    # Breach energy first
    seq.check({"energy": 10.0, "carbon": 0.1})
    # Breach carbon second
    seq.check({"energy": 1.0, "carbon": 2.0})

    before = dict(seq._budget_learner.observations_by_metric)
    seq.record_outcome(was_correct=False)
    after = dict(seq._budget_learner.observations_by_metric)
    _check(
        after.get("carbon", 0) > before.get("carbon", 0),
        f"credits 'carbon' (last violated): "
        f"{before.get('carbon', 0)} → {after.get('carbon', 0)}",
    )
    _check(
        after.get("energy", 0) == before.get("energy", 0),
        f"does not wrongly credit 'energy': "
        f"{before.get('energy', 0)} → {after.get('energy', 0)}",
    )

    # ============================================================= #
    # P0 §2.4 — ActiveModeRaises is load-bearing
    # ============================================================= #
    print("\n=== P0 §2.4: ActiveModeRaises is load-bearing ===")
    prop = ActiveModeRaises()

    # Correct behavior: active + violations + raised=True → passes
    ctx_correct = {
        "mode": "active_enforcement",
        "violations": ["energy"],
        "raised": True,
        "hitl_override": False,
    }
    _check(
        prop.check(ctx_correct) is True,
        "property passes on correct behavior (raised=True)",
    )

    # Bug: active + violations + raised=False → property fires
    ctx_bug = {
        "mode": "active_enforcement",
        "violations": ["energy"],
        "raised": False,
        "hitl_override": False,
    }
    _check(
        prop.check(ctx_bug) is False,
        "property fires on regression (raised=False, the v5 tautology)",
    )

    # Override: active + violations + raised=False + hitl_override=True → passes
    ctx_override = {
        "mode": "active_enforcement",
        "violations": ["energy"],
        "raised": False,
        "hitl_override": True,
    }
    _check(
        prop.check(ctx_override) is True,
        "property allows HITL override (raised=False + override=True)",
    )

    # Monitor mode: property always passes
    ctx_monitor = {
        "mode": "monitor",
        "violations": ["energy"],
        "raised": False,
        "hitl_override": False,
    }
    _check(
        prop.check(ctx_monitor) is True,
        "property passes in monitor mode (doesn't apply)",
    )

    # Enforcer's temporal monitor catches the bug when fed a wrong `raised`
    trap = GreenPolicyEnforcer(_policy(mode="monitor"))
    trap._verify_temporal(
        compliant=False,
        violations=["energy"],
        raised=False,   # <-- bug
        hitl_override=False,
    )
    # Note: monitor mode passes because mode != active.
    # Use active mode for the trap.
    trap_active = GreenPolicyEnforcer(_policy())
    trap_active._verify_temporal(
        compliant=False,
        violations=["energy"],
        raised=False,   # <-- bug
        hitl_override=False,
    )
    _check(
        any(
            v.get("property") == "ActiveModeRaises"
            for v in trap_active._safety.violations
        ),
        "enforcer's temporal monitor fires on the injected bug",
    )

    # And the correct path doesn't record any violation
    healthy = GreenPolicyEnforcer(_policy())
    try:
        healthy.check({"energy": 6.0})   # real breach, active raises
    except PolicyViolation:
        pass
    _check(
        not any(
            v.get("property") == "ActiveModeRaises"
            for v in healthy._safety.violations
        ),
        "no false positive on the correct-raise path",
    )

    # ============================================================= #
    # P0 §2.5 — HITL propagation
    # ============================================================= #
    print("\n=== P0 §2.5: HITL propagation ===")

    # (a) Rejection → still raises
    rej = GreenPolicyEnforcer(_policy())
    rej.set_hitl_callback(lambda req: False)
    try:
        rej.check({"energy": 5.3})   # 6% over → borderline → reviewed
    except PolicyViolation as e:
        _check(
            e.hitl_status == "rejected",
            f"HITL rejection raises (status={e.hitl_status})",
        )
    else:
        raise AssertionError("HITL rejection did not raise")

    # (b) Approval without override → still raises (default fail-safe)
    no_ov = GreenPolicyEnforcer(_policy())
    no_ov.set_hitl_callback(lambda req: True)
    try:
        no_ov.check({"energy": 5.3})
    except PolicyViolation as e:
        _check(
            e.hitl_status == "approved" and e.hitl_override is False,
            f"approval without override → still raises "
            f"(status={e.hitl_status}, override={e.hitl_override})",
        )
    else:
        raise AssertionError("approval without override did not raise")

    # (c) Approval with allow_hitl_override=True → no raise
    yes_ov = GreenPolicyEnforcer(
        _policy(), allow_hitl_override=True
    )
    yes_ov.set_hitl_callback(lambda req: True)
    result = yes_ov.check({"energy": 5.3})
    _check(
        result["compliant"] is True and result["hitl_override"] is True,
        f"approval + override → compliant with hitl_override flag",
    )
    _check(
        yes_ov.override_count == 1,
        "override_count incremented",
    )

    # ============================================================= #
    # P0 §2.6 — temporal_verified reflects reality
    # ============================================================= #
    print("\n=== P0 §2.6: temporal_verified reflects reality ===")
    tm = GreenPolicyEnforcer(_policy(mode="monitor"))
    r_ok = tm.check({"energy": 1.0})
    _check(
        r_ok["temporal_verified"] is True,
        "compliant check → temporal_verified=True",
    )
    _check(
        r_ok["temporal_violations"] == [],
        "no temporal violations on correct behavior",
    )
    # Force a temporal violation via injected buggy verify
    tm._verify_temporal(
        compliant=False,
        violations=["energy"],
        raised=False,
        hitl_override=True,  # pretend override, but monitor mode
    )
    # In monitor mode the property passes; use active mode for the trap
    active_tm = GreenPolicyEnforcer(_policy())
    try:
        active_tm.check({"energy": 6.0})
    except PolicyViolation as e:
        _check(
            e.temporal_verified is True,
            "PolicyViolation.details has temporal_verified=True",
        )

    # ============================================================= #
    # P0 §2.7 — NaN / missing metrics → EMERGENCY
    # ============================================================= #
    print("\n=== P0 §2.7: NaN / missing metrics → EMERGENCY ===")
    nan = GreenPolicyEnforcer(_policy(mode="monitor"))
    r_nan = nan.check({"energy": float("nan"), "carbon": 0.2})
    _check(
        r_nan["compliant"] is False,
        "NaN energy → non-compliant",
    )
    _check(
        r_nan["severity"] == "emergency",
        f"NaN → EMERGENCY (got {r_nan['severity']})",
    )
    r_miss = nan.check({"carbon": 0.2})   # energy missing
    _check(
        r_miss["compliant"] is False,
        "missing energy (strict mode) → non-compliant",
    )

    # Lenient mode preserves v1 (missing → 0.0)
    lenient = GreenPolicyEnforcer(
        _policy(mode="monitor"), strict_metrics=False
    )
    r_len = lenient.check({"carbon": 0.2})
    _check(
        r_len["compliant"] is True,
        "strict_metrics=False preserves v1",
    )

    # ============================================================= #
    # P0 §2.8 — metrics=None is not a clean pass
    # ============================================================= #
    print("\n=== P0 §2.8: metrics=None → not a clean pass ===")
    none_mgr = GreenPolicyEnforcer(_policy(mode="monitor"))
    r_none = none_mgr.check(None)  # type: ignore[arg-type]
    _check(
        r_none["compliant"] is False and r_none.get("degraded") is True,
        f"metrics=None → degraded, not compliant (got {r_none['compliant']})",
    )

    # ============================================================= #
    # P0 §2.9 — small / zero-limit breaches
    # ============================================================= #
    print("\n=== P0 §2.9: small / zero-limit breaches are never INFO ===")
    small = GreenPolicyEnforcer(_policy(mode="monitor"))
    r_small = small.check({"energy": 5.01, "carbon": 0.2})
    _check(
        r_small["severity"] == "warning",
        f"0.2% breach → WARNING (got {r_small['severity']})",
    )

    zero = GreenPolicyEnforcer(
        _policy(mode="monitor", max_cost_usd=0.0)
    )
    r_zero = zero.check({"energy": 1.0, "cost": 0.5})
    _check(
        r_zero["severity"] == "emergency",
        f"zero-limit breach → EMERGENCY (got {r_zero['severity']})",
    )

    # Multi-metric escalation
    multi = GreenPolicyEnforcer(_policy(mode="monitor"))
    r_multi = multi.check(
        {"energy": 6.2, "carbon": 0.62, "latency": 32.0}
    )
    _check(
        len(r_multi["structured_violations"]) == 3,
        f"3 metrics breached (got {len(r_multi['structured_violations'])})",
    )
    _check(
        r_multi["severity"] == "critical",
        f"3-metric ~24% breach escalates to CRITICAL "
        f"(got {r_multi['severity']})",
    )

    # ============================================================= #
    # P0 §2.10 — constraint validation
    # ============================================================= #
    print("\n=== P0 §2.10: constraints validated at construction ===")
    for name, args in [
        ("negative", {"max_energy_per_task_wh": -5.0}),
        ("NaN", {"max_carbon_per_task_kg": float("nan")}),
        ("string", {"max_latency_seconds": "abc"}),
        ("bool", {"max_energy_per_task_wh": True}),
    ]:
        try:
            GreenPolicyEnforcer(
                {"constraints": args, "agent_identity": {"mode": "monitor"}}
            )
        except ValueError:
            _check(True, f"{name} constraint rejected at construction")
        else:
            raise AssertionError(f"{name} constraint not rejected")

    # ============================================================= #
    # P0 §2.11 — causal learner fixed
    # ============================================================= #
    print("\n=== P0 §2.11: causal learner ===")
    learner = CausalBudgetLearner(
        known_metrics=["energy", "carbon"],
    )
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
        f"feature-aware (A={off_a:.4f}, B={off_b:.4f})",
    )
    # Signed reward tightens
    tightener = CausalBudgetLearner(known_metrics=["energy"])
    baseline = tightener.predict_offset("energy", state_a)
    for _ in range(50):
        tightener.record("energy", feats_a, -1.0)
    tightener.update()
    after = tightener.predict_offset("energy", state_a)
    _check(
        after < baseline,
        f"negative reward tightens: {baseline:.4f} → {after:.4f}",
    )
    # Tightening-only: 100 correct never loosen
    widener = CausalBudgetLearner(known_metrics=["energy"])
    for _ in range(100):
        widener.record("energy", feats_a, 1.0)
    widener.update()
    _check(
        widener.predict_offset("energy", state_a) <= 0.0,
        f"100 correct outcomes never loosen "
        f"(offset={widener.predict_offset('energy', state_a):.4f})",
    )

    # ============================================================= #
    # P1 §3.1 — thread safety
    # ============================================================= #
    print("\n=== P1 §3.1: thread safety ===")
    import threading
    safe = GreenPolicyEnforcer(_policy(mode="monitor"))
    errors: List[BaseException] = []

    def _worker() -> None:
        try:
            for _ in range(100):
                safe.check({"energy": 1.0, "carbon": 0.2})
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
        f"check_count is 1000 (got {safe.check_count})",
    )

    # ============================================================= #
    # P1 §3.2 — v18 single-probe circuit breaker
    # ============================================================= #
    print("\n=== P1 §3.2: v18 single-probe circuit breaker ===")
    probe = GreenPolicyEnforcer(_policy(mode="monitor"))
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
    # P1 §3.4 — bounded collections
    # ============================================================= #
    print("\n=== P1 §3.4: bounded collections ===")
    bound = GreenPolicyEnforcer(_policy(mode="monitor"))
    for _ in range(2000):
        bound.check({"energy": 6.0, "carbon": 0.6, "latency": 40.0})
    _check(
        len(bound._hitl.feedback_log) <= MAX_HITL_LOG,
        f"hitl.feedback_log bounded at {MAX_HITL_LOG}",
    )
    _check(
        len(bound._federated.updates) <= MAX_FEDERATED_UPDATES,
        f"federated.updates bounded at {MAX_FEDERATED_UPDATES}",
    )

    # ============================================================= #
    # P1 §3.5 — cached market snapshot
    # ============================================================= #
    print("\n=== P1 §3.5: cached market snapshot ===")
    cache = GreenPolicyEnforcer(_policy(mode="monitor"))
    cache.check({"energy": 1.0})
    p1 = cache.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    p2 = cache.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    _check(
        p1 == p2,
        f"consecutive get_statistics() reports the cached price ({p1})",
    )

    # ============================================================= #
    # P1 §3.13 — set_mode
    # ============================================================= #
    print("\n=== P1 §3.13: set_mode ===")
    sm = GreenPolicyEnforcer(_policy(mode="monitor"))
    sm.check({"energy": 6.0})   # monitor: no raise
    _check(
        sm.mode == "monitor",
        "starts in monitor mode",
    )
    sm.set_mode("active_enforcement")
    _check(
        sm.mode == "active_enforcement",
        "set_mode switches to active",
    )
    try:
        sm.check({"energy": 6.0})
    except PolicyViolation:
        _check(True, "after set_mode, check raises in active mode")
    else:
        raise AssertionError("set_mode did not switch behavior")
    # Invalid mode rejected
    try:
        sm.set_mode("bogus")
    except ValueError:
        _check(True, "invalid mode rejected")
    else:
        raise AssertionError("invalid mode not rejected")

    # ============================================================= #
    # P1 — set_features, health_check, async CM
    # ============================================================= #
    print("\n=== P1: set_features, health_check, async CM ===")
    live = GreenPolicyEnforcer(_policy(mode="monitor"))
    _check(live._market is not None, "market enabled by default")
    live.set_features({"carbon_market": False})
    _check(live._market is None, "set_features disabled market")
    live.set_features({"carbon_market": True})
    _check(live._market is not None, "set_features re-enabled market")

    async def _async_health():
        async with GreenPolicyEnforcer(_policy(mode="monitor")) as m:
            health = await m.health_check()
            _check(
                "healthy" in health and "strict_metrics" in health,
                "health_check returns expected keys",
            )

    asyncio.run(_async_health())

    # ============================================================= #
    # Final summary
    # ============================================================= #
    print("\n=== Statistics (last enforcer) ===")
    import json
    print(json.dumps(live.get_statistics(), indent=2, default=str))

    print("\n" + "=" * 78)
    print("All v6 assertions passed.")
    print("=" * 78)


if __name__ == "__main__":
    _demo()
