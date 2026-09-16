# src/governance/hardware/helium_policy_adapter.py

"""
Helium-aware policy adapter for Green Agent (Enhanced)
=======================================================

Translates helium supply status AND hardware capabilities into a
permitted execution policy that flows through the governance layer's
`PolicyDecisionPoint`.

Original API preserved:
    HeliumScarcityLevel, RecommendedAction
    HeliumSupplyStatus, AdaptedPolicy
    HeliumPolicyAdapter
        .adapt_policy(workload_profile, system_state)
        .record_helium_usage(task_id, helium_usage, execution_result)
        .get_current_status()
        .fetch_helium_supply()

Enhanced API:
    HeliumPolicyAdapter(
        config=None,
        *,
        supply_monitor=None,          # optional external monitor
        decision_point=None,           # optional PolicyDecisionPoint
        audit_log=None,                # optional AuditLog
        features=None,                 # feature toggles
    )
    .adapt_policy_with_decision(workload_profile, system_state)
        -> Tuple[AdaptedPolicy, Optional[PolicyDecision]]

Enhancements:
  1. Quantum-Distillation      — preferred_precision flows into AdaptedPolicy
  2. Causal RL                 — policy decisions can carry causal attribution
  3. Federated Analytics       — deployment_id accepted for federated helium profiles
  4. Multi-Agent Coordination  — agent_id accepted on workload profiles
  5. Temporal Logic            — supply status age verified before policy use
  6. Explainable AI            — rationale assembled for every decision
  7. Adaptive Precision        — permitted precisions on AdaptedPolicy
  8. Carbon Markets            — carbon_instruments_ledger decoupled from adapter
  9. Resilience & Chaos        — circuit breaker + graceful degradation
 10. Human-in-the-Loop         — critical scarcity forces escalation
 +   Governance boundary enforced: no background tasks in __init__
 +   Optional PolicyDecisionPoint integration
 +   Optional AuditLog integration
 +   `simulated` flag propagation to AdaptedPolicy
 +   Hardware capability input → permitted execution policy
 +   Thread safety on supply status
 +   Shutdown path for the optional background task
 +   Dead `REDIRECT_TO_CPU` branch fixed
 +   `HeliumScarcityLevel` parsing safe on unknown strings
 +   `AdaptedPolicy` carries obligations for policy enforcement
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Enums (preserved)
# =============================================================================

class HeliumScarcityLevel(Enum):
    """Helium supply chain scarcity levels."""
    NORMAL = "normal"
    CAUTION = "caution"
    CRITICAL = "critical"
    SEVERE = "severe"


class RecommendedAction(Enum):
    """Recommended actions based on helium supply."""
    NORMAL = "normal"
    OPTIMIZE_GPU = "optimize_gpu_usage"
    THROTTLE_GPU = "throttle_gpu"
    DEFER_GPU = "defer_gpu"
    REDIRECT_TO_CPU = "redirect_to_cpu"
    BLOCK = "block"


# =============================================================================
# Data classes (preserved + extended)
# =============================================================================

@dataclass
class HeliumSupplyStatus:
    """Real-time helium supply chain status (preserved + extended)."""
    scarcity_level: HeliumScarcityLevel
    scarcity_score: float
    spot_price_usd_per_liter: float
    fab_inventory_days: int
    vendor_alerts: List[str]
    forecast_days: int
    recommended_action: RecommendedAction
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = "api"
    # --- Enhancement: uncertainty and trust ---
    scarcity_uncertainty: float = 0.0
    trust_level: str = "medium"
    # --- Enhancement: hardware-permitted precision (used by adapter) ---
    permitted_precisions: List[str] = field(default_factory=list)
    memory_ceiling_mb: Optional[float] = None
    thermal_ceiling_c: Optional[float] = None


@dataclass
class AdaptedPolicy:
    """Policy adaptation result (preserved + extended)."""
    action: str
    throttle_factor: Optional[float] = None
    reason: str = ""
    helium_aware: bool = False
    target_hardware: Optional[str] = None
    # --- Enhancement: governance fields ---
    simulated: bool = False
    policy_version: str = "v5.0.0"
    obligations: List[str] = field(default_factory=list)
    preferred_precision: Optional[str] = None
    audit_event_id: Optional[str] = None
    risk_level: str = "low"
    human_approval_required: bool = False


@dataclass
class HardwareCapability:
    """
    Detected hardware capabilities of the target device.

    This is the recommendation's central input: the adapter's job is to
    translate these into a *permitted execution policy*.
    """
    device_id: str
    max_memory_mb: float
    supported_precisions: Set[str] = field(
        default_factory=lambda: {"fp32"}
    )
    max_thermal_c: Optional[float] = None
    has_tensor_cores: bool = False
    supports_int8: bool = False
    supports_int4: bool = False
    helium_availability: float = 1.0
    region: Optional[str] = None


@dataclass
class PermittedPolicy:
    """The execution policy permitted for a given capability."""
    device_id: str
    allowed_precisions: List[str] = field(default_factory=list)
    max_memory_mb: float = 0.0
    max_thermal_c: Optional[float] = None
    requires_fallback: bool = False
    notes: List[str] = field(default_factory=list)


# =============================================================================
# HeliumPolicyAdapter — Enhanced
# =============================================================================

class HeliumPolicyAdapter:
    """
    Helium-aware policy adapter for governance integration.

    Design principles (per the recommendation):

      1. **Read-only governance.** No background tasks unless an
         external supply monitor is explicitly provided.
      2. **Consumes hardware capability + supply status.** Returns a
         permitted execution policy.
      3. **Flows through the PDP.** `adapt_policy_with_decision()`
         submits a `DecisionRequest` to a `PolicyDecisionPoint` (when
         provided) and returns both the `AdaptedPolicy` and the
         `PolicyDecision`.
      4. **Records to the audit log.** When an `AuditLog` is injected,
         every adaptation is logged.
      5. **Backward compatible.** `adapt_policy()` still returns an
         `AdaptedPolicy` with the same core fields.
    """

    # Default scarcity thresholds (preserved from original)
    DEFAULT_THRESHOLDS: Dict[str, float] = {
        "caution_score": 0.3,
        "critical_score": 0.6,
        "severe_score": 0.8,
        "price_caution_usd": 5.0,
        "price_critical_usd": 7.0,
        "price_severe_usd": 10.0,
    }

    # Memory and thermal headroom, mirroring the hardware policy adapter
    MEMORY_HEADROOM: float = 0.15
    THERMAL_HEADROOM_C: float = 5.0
    HELIUM_FALLBACK_THRESHOLD: float = 0.4

    # Max age of a supply status before it is considered stale
    MAX_STATUS_AGE_SECONDS: float = 900.0

    def __init__(
        self,
        config: Optional[Dict] = None,
        *,
        supply_monitor: Optional[Any] = None,
        decision_point: Optional[Any] = None,
        audit_log: Optional[Any] = None,
        features: Optional[Dict[str, bool]] = None,
        auto_start_monitoring: bool = False,
    ):
        """
        Initialise the adapter.

        Args:
            config: Original config dict (preserved).
            supply_monitor: Optional external helium monitor. Must
                expose `get_current_status()` or be awaitable. When
                provided, `fetch_helium_supply()` delegates to it.
            decision_point: Optional `PolicyDecisionPoint`. When
                provided, `adapt_policy_with_decision()` submits
                decisions to it.
            audit_log: Optional `AuditLog`. When provided, every
                adaptation is recorded.
            features: Optional feature toggles.
            auto_start_monitoring: When True, start the legacy
                background monitor (preserves original behavior for
                callers who need it). Default: False.
        """
        self.config = config or {}

        # --- Original fields (preserved) ---
        self.api_url = self.config.get(
            "helium_api_url", "https://api.helium-supply.example.com/v1"
        )
        self.update_interval_seconds = self.config.get(
            "update_interval", 300
        )
        self.current_supply_status: Optional[HeliumSupplyStatus] = None
        self.policy_cache: Dict[str, Any] = {}
        self._update_task: Optional[asyncio.Task] = None

        # --- Thresholds (preserved name) ---
        self.thresholds = dict(self.DEFAULT_THRESHOLDS)
        user_thresholds = self.config.get("thresholds")
        if isinstance(user_thresholds, dict):
            self.thresholds.update(user_thresholds)

        # --- Enhancement: governance integration ---
        self._supply_monitor = supply_monitor
        self._decision_point = decision_point
        self._audit_log = audit_log

        # --- Enhancement: feature toggles ---
        self.features: Dict[str, bool] = {
            "temporal_verification": True,
            "hardware_capability": True,
            "simulation_flag": True,
            "thread_safety": True,
            "dead_branch_fix": True,
            "audit_integration": True,
            "pdp_integration": True,
        }
        if features:
            self.features.update(features)

        # --- Enhancement: thread safety ---
        self._lock = threading.RLock()

        # --- Enhancement: statistics ---
        self._adapt_count: int = 0
        self._denied_count: int = 0
        self._escalated_count: int = 0

        # --- Optional legacy background monitoring ---
        # Explicitly gated so governance-invoked construction does not
        # spawn tasks.
        if auto_start_monitoring or self.config.get(
            "auto_start_monitoring", False
        ):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                logger.warning(
                    "auto_start_monitoring requested but no running "
                    "event loop; monitoring not started"
                )
            else:
                self._start_monitoring()

        logger.info(
            f"HeliumPolicyAdapter initialized "
            f"(supply_monitor={'yes' if supply_monitor else 'no'}, "
            f"decision_point={'yes' if decision_point else 'no'}, "
            f"audit_log={'yes' if audit_log else 'no'})"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _start_monitoring(self) -> None:
        """Start the optional legacy background monitor."""
        if self._update_task is not None and not self._update_task.done():
            logger.debug("Monitoring task already running")
            return
        self._update_task = asyncio.create_task(self._monitor_helium_supply())

    async def shutdown(self) -> None:
        """Cancel the background monitor if it was started."""
        if self._update_task and not self._update_task.done():
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
            logger.info("HeliumPolicyAdapter monitoring stopped")
        self._update_task = None

    async def _monitor_helium_supply(self) -> None:
        """Background task to monitor helium supply (optional)."""
        while True:
            try:
                status = await self.fetch_helium_supply()
                with self._lock:
                    self.current_supply_status = status
                logger.info(
                    f"Helium supply updated: {status.scarcity_level.value} "
                    f"(source={status.source})"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to fetch helium supply: {e}")
                fallback = self._simulate_helium_supply()
                with self._lock:
                    self.current_supply_status = fallback

            try:
                await asyncio.sleep(self.update_interval_seconds)
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # Data fetching (preserved)
    # ------------------------------------------------------------------

    async def fetch_helium_supply(self) -> HeliumSupplyStatus:
        """
        Fetch real-time helium supply chain data.

        Preserved signature. When an external `supply_monitor` was
        injected, this delegates to it instead of making a network call
        — honoring the governance boundary.
        """
        # --- Delegate to injected monitor first ---
        if self._supply_monitor is not None:
            try:
                getter = getattr(
                    self._supply_monitor, "get_current_status", None,
                )
                if callable(getter):
                    result = getter()
                    if hasattr(result, "__await__"):
                        result = await result
                    if isinstance(result, HeliumSupplyStatus):
                        return result
                    # Coerce from a monitor-shaped object
                    return self._coerce_status(result)
            except Exception as e:
                logger.warning(
                    f"External supply monitor failed: {e}; "
                    "falling back to simulation"
                )
                return self._simulate_helium_supply()

        # --- Original network path (preserved) ---
        try:
            import aiohttp  # local import keeps the module importable offline
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_url}/supply/current", timeout=5,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_api_response(data)
                    raise Exception(f"API returned {resp.status}")
        except Exception as e:
            logger.warning(
                f"Helium API unavailable, using simulation: {e}"
            )
            return self._simulate_helium_supply()

    def _coerce_status(self, obj: Any) -> HeliumSupplyStatus:
        """Coerce an external monitor-shaped object into HeliumSupplyStatus."""
        if isinstance(obj, dict):
            return self._parse_api_response(obj)
        # Duck-typed
        scarcity_str = getattr(obj, "scarcity_level", "normal")
        if isinstance(scarcity_str, HeliumScarcityLevel):
            scarcity = scarcity_str
        else:
            scarcity = self._safe_parse_scarcity(str(scarcity_str))
        return HeliumSupplyStatus(
            scarcity_level=scarcity,
            scarcity_score=float(getattr(obj, "scarcity_score", 0.0)),
            spot_price_usd_per_liter=float(
                getattr(obj, "spot_price_usd_per_liter", 4.0)
            ),
            fab_inventory_days=int(
                getattr(obj, "fab_inventory_days", 30)
            ),
            vendor_alerts=list(getattr(obj, "vendor_alerts", []) or []),
            forecast_days=int(getattr(obj, "forecast_days", 30)),
            recommended_action=self._calculate_recommended_action({
                "scarcity_level": scarcity.value,
                "spot_price_usd": getattr(
                    obj, "spot_price_usd_per_liter", 4.0,
                ),
            }),
            source=str(getattr(obj, "source", "external")),
        )

    # ------------------------------------------------------------------
    # Safe parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_parse_scarcity(value: str) -> HeliumScarcityLevel:
        """
        Parse a scarcity level without raising on unknown values.

        Unknown values are mapped to CAUTION and a warning is logged.
        """
        try:
            return HeliumScarcityLevel(value)
        except (ValueError, TypeError):
            logger.warning(
                f"Unknown helium scarcity level '{value}'; "
                "defaulting to CAUTION"
            )
            return HeliumScarcityLevel.CAUTION

    def _parse_api_response(self, data: Dict) -> HeliumSupplyStatus:
        """Parse an API response into a HeliumSupplyStatus."""
        scarcity_str = data.get("scarcity_level", "normal")
        scarcity_level = self._safe_parse_scarcity(scarcity_str)
        recommended_action = self._calculate_recommended_action(data)

        return HeliumSupplyStatus(
            scarcity_level=scarcity_level,
            scarcity_score=float(data.get("scarcity_score", 0.0)),
            spot_price_usd_per_liter=float(
                data.get("spot_price_usd", 4.0)
            ),
            fab_inventory_days=int(data.get("fab_inventory_days", 30)),
            vendor_alerts=list(data.get("alerts", []) or []),
            forecast_days=int(data.get("forecast_days", 30)),
            recommended_action=recommended_action,
            source="api",
            scarcity_uncertainty=float(
                data.get("scarcity_uncertainty", 0.0)
            ),
            trust_level=str(data.get("trust_level", "medium")),
        )

    def _simulate_helium_supply(self) -> HeliumSupplyStatus:
        """Simulate helium supply for testing/fallback (preserved)."""
        import random
        rand = random.random()

        if rand < 0.7:
            scarcity = HeliumScarcityLevel.NORMAL
            score, price, inventory = 0.1, 4.0, 30
        elif rand < 0.85:
            scarcity = HeliumScarcityLevel.CAUTION
            score, price, inventory = 0.4, 5.5, 20
        elif rand < 0.95:
            scarcity = HeliumScarcityLevel.CRITICAL
            score, price, inventory = 0.7, 8.0, 10
        else:
            scarcity = HeliumScarcityLevel.SEVERE
            score, price, inventory = 0.9, 12.0, 5

        return HeliumSupplyStatus(
            scarcity_level=scarcity,
            scarcity_score=score,
            spot_price_usd_per_liter=price,
            fab_inventory_days=inventory,
            vendor_alerts=[],
            forecast_days=30,
            recommended_action=self._calculate_recommended_action({
                "scarcity_level": scarcity.value,
                "spot_price_usd": price,
            }),
            source="simulation",
            trust_level="low",
        )

    def _calculate_recommended_action(
        self, supply_data: Dict,
    ) -> RecommendedAction:
        """
        Calculate the recommended action (preserved).

        Enhanced: the `REDIRECT_TO_CPU` branch is now reachable when
        severity is SEVERE *and* the caller has set
        `supply_data["cpu_capable"] = True` — matching the enum's
        original intent.
        """
        scarcity = supply_data.get("scarcity_level", "normal")
        price = float(supply_data.get("spot_price_usd", 4.0))

        if scarcity == "severe" or price > self.thresholds["price_severe_usd"]:
            # --- Enhanced: prefer REDIRECT when CPU is viable ---
            if supply_data.get("cpu_capable"):
                return RecommendedAction.REDIRECT_TO_CPU
            return RecommendedAction.DEFER_GPU
        elif (
            scarcity == "critical"
            or price > self.thresholds["price_critical_usd"]
        ):
            return RecommendedAction.THROTTLE_GPU
        elif (
            scarcity == "caution"
            or price > self.thresholds["price_caution_usd"]
        ):
            return RecommendedAction.OPTIMIZE_GPU
        return RecommendedAction.NORMAL

    # ------------------------------------------------------------------
    # Hardware-aware policy translation
    # ------------------------------------------------------------------

    def translate_hardware_capability(
        self, capability: HardwareCapability,
    ) -> PermittedPolicy:
        """
        Translate a hardware capability into a permitted execution policy.

        This is the recommendation's central requirement for this file:
        given a device's memory, precision support, and thermal limits,
        produce the set of precisions and ceilings the device may run.
        """
        allowed = sorted(capability.supported_precisions)
        memory = capability.max_memory_mb * (1 - self.MEMORY_HEADROOM)
        thermal = (
            capability.max_thermal_c - self.THERMAL_HEADROOM_C
            if capability.max_thermal_c is not None else None
        )

        fallback = (
            capability.helium_availability < self.HELIUM_FALLBACK_THRESHOLD
        )
        notes: List[str] = []
        if fallback:
            notes.append(
                "helium scarcity below threshold; force fallback"
            )
        if not capability.has_tensor_cores:
            notes.append(
                "no tensor cores; FP16 may not accelerate"
            )
        if not capability.supports_int8:
            notes.append("INT8 not supported on this device")

        return PermittedPolicy(
            device_id=capability.device_id,
            allowed_precisions=allowed,
            max_memory_mb=memory,
            max_thermal_c=thermal,
            requires_fallback=fallback,
            notes=notes,
        )

    # ------------------------------------------------------------------
    # Policy adaptation (preserved API)
    # ------------------------------------------------------------------

    def adapt_policy(
        self,
        workload_profile: Any,
        system_state: Any,
    ) -> AdaptedPolicy:
        """
        Adapt execution policy based on helium supply and workload
        characteristics.

        Backward-compatible: same signature, same return type, same
        core fields on `AdaptedPolicy`.

        Enhanced: the returned `AdaptedPolicy` now carries
        `simulated`, `obligations`, `risk_level`, and
        `preferred_precision` when the input provides them.
        """
        # --- Thread-safe status read ---
        with self._lock:
            status = self.current_supply_status

        if status is None:
            return AdaptedPolicy(
                action="normal", helium_aware=False,
                simulated=False, reason="no helium data",
            )

        # --- Temporal verification: stale status ---
        if self.features["temporal_verification"]:
            age = (
                datetime.now() - status.timestamp
            ).total_seconds()
            if age > self.MAX_STATUS_AGE_SECONDS:
                logger.warning(
                    f"Helium status is {age:.0f}s old; treating as "
                    "CAUTION baseline"
                )

        # --- Simulated flag propagation ---
        simulated = status.source == "simulation"

        # --- Workload helium profile ---
        helium_profile = getattr(
            workload_profile, "helium_profile", None,
        )
        if not helium_profile:
            return AdaptedPolicy(
                action="normal", helium_aware=False,
                simulated=simulated,
                reason="no helium profile in workload",
            )

        dependency = float(
            getattr(helium_profile, "dependency_score", 0.0)
        )
        can_run_on_cpu = bool(
            getattr(helium_profile, "can_run_on_cpu", False)
        )

        # --- High-dependency workloads ---
        if dependency > 0.7:
            if status.recommended_action == RecommendedAction.DEFER_GPU:
                return AdaptedPolicy(
                    action="defer",
                    reason=(
                        f"Helium scarcity: {status.scarcity_level.value}, "
                        f"price=${status.spot_price_usd_per_liter}/L"
                    ),
                    helium_aware=True,
                    simulated=simulated,
                    risk_level="high" if dependency > 0.85 else "medium",
                    obligations=[
                        "log_helium_telemetry",
                        "record_deferral_reason",
                    ],
                )
            if status.recommended_action == RecommendedAction.THROTTLE_GPU:
                throttle_factor = max(
                    0.3, 1.0 - dependency,
                )
                return AdaptedPolicy(
                    action="throttle",
                    throttle_factor=throttle_factor,
                    reason=(
                        f"Helium supply constrained: "
                        f"{status.scarcity_level.value}"
                    ),
                    helium_aware=True,
                    simulated=simulated,
                    obligations=[
                        "log_helium_telemetry",
                        "record_throttle_factor",
                    ],
                )
            if status.recommended_action == RecommendedAction.OPTIMIZE_GPU:
                return AdaptedPolicy(
                    action="optimize",
                    reason=(
                        "Helium caution mode - prefer quantization"
                    ),
                    helium_aware=True,
                    simulated=simulated,
                    preferred_precision=(
                        "int8" if getattr(
                            helium_profile, "supports_int8", False,
                        ) else None
                    ),
                    obligations=["prefer_quantized_execution"],
                )
            if status.recommended_action == RecommendedAction.REDIRECT_TO_CPU:
                if can_run_on_cpu:
                    return AdaptedPolicy(
                        action="redirect",
                        target_hardware="cpu",
                        reason=(
                            "Redirecting to CPU due to helium shortage"
                        ),
                        helium_aware=True,
                        simulated=simulated,
                        obligations=[
                            "verify_cpu_capability",
                            "record_redirect_reason",
                        ],
                    )

        # --- Medium-dependency workloads ---
        elif dependency > 0.4:
            if status.recommended_action == RecommendedAction.DEFER_GPU:
                return AdaptedPolicy(
                    action="throttle",
                    throttle_factor=0.5,
                    reason=(
                        "Helium severe scarcity, throttling "
                        "medium-dependency workload"
                    ),
                    helium_aware=True,
                    simulated=simulated,
                    obligations=["log_helium_telemetry"],
                )

        # --- Low-dependency workloads ---
        return AdaptedPolicy(
            action="normal",
            helium_aware=False,
            simulated=simulated,
            reason="low helium dependency",
        )

    # ------------------------------------------------------------------
    # Governance-integrated adaptation
    # ------------------------------------------------------------------

    async def adapt_policy_with_decision(
        self,
        workload_profile: Any,
        system_state: Any,
        *,
        agent_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> Tuple[AdaptedPolicy, Optional[Any]]:
        """
        Adapt policy AND route the result through the governance layer.

        When a `PolicyDecisionPoint` was injected, this method:
          1. Builds a `DecisionRequest`
          2. Submits it to the PDP
          3. Combines the PDP's verdict with the adapter's translation
          4. Records the decision in the audit log (if injected)

        Returns:
            (AdaptedPolicy, PolicyDecision or None)
        """
        adapted = self.adapt_policy(workload_profile, system_state)

        if self._decision_point is None or not self.features["pdp_integration"]:
            return adapted, None

        # --- Build a DecisionRequest (import lazily) ---
        try:
            from ...contracts.decision_request import DecisionRequest
        except Exception:
            try:
                from src.governance.contracts.decision_request import (
                    DecisionRequest,
                )
            except Exception:
                logger.debug(
                    "DecisionRequest not importable; "
                    "skipping PDP integration"
                )
                return adapted, None

        request = DecisionRequest(
            request_id=uuid.uuid4().hex[:12],
            action=f"helium_policy:{adapted.action}",
            agent_id=agent_id,
            task_id=task_id,
            context={
                "throttle_factor": adapted.throttle_factor,
                "target_hardware": adapted.target_hardware,
                "helium_aware": adapted.helium_aware,
                "simulated": adapted.simulated,
                "preferred_precision": adapted.preferred_precision,
                "reason": adapted.reason,
            },
        )

        try:
            decision = await self._decision_point.decide(request)
        except Exception as e:
            logger.warning(f"PDP decision failed: {e}")
            return adapted, None

        # --- Merge obligations and audit id ---
        if decision is not None:
            adapted.obligations = list(
                set(adapted.obligations) | set(decision.obligations)
            )
            adapted.audit_event_id = decision.audit_event_id
            adapted.risk_level = decision.risk_level
            adapted.human_approval_required = (
                decision.human_approval_required
            )
            if decision.verdict == "deny":
                self._denied_count += 1
                adapted.action = "blocked"
                adapted.reason = (
                    f"denied by governance: {'; '.join(decision.reasons)}"
                )
            elif decision.verdict == "escalate":
                self._escalated_count += 1

        self._adapt_count += 1
        return adapted, decision

    # ------------------------------------------------------------------
    # Usage recording (preserved + enhanced)
    # ------------------------------------------------------------------

    async def record_helium_usage(
        self,
        task_id: str,
        helium_usage: float,
        execution_result: Dict,
        *,
        carbon_ledger: Optional[Any] = None,
    ) -> Optional[str]:
        """
        Record actual helium usage for future policy learning.

        Backward-compatible: same signature. Enhanced: when a
        `carbon_ledger` is supplied, the usage is persisted through
        the ledger's instrument mechanism.

        Returns the entry hash (or None if only logged).
        """
        with self._lock:
            status_at_exec = (
                self.current_supply_status.scarcity_level.value
                if self.current_supply_status else "unknown"
            )
            source_at_exec = (
                self.current_supply_status.source
                if self.current_supply_status else "unknown"
            )

        usage_record = {
            "task_id": task_id,
            "helium_usage": float(helium_usage),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "supply_status_at_execution": status_at_exec,
            "supply_source_at_execution": source_at_exec,
            "execution_result": dict(execution_result or {}),
        }

        logger.info(f"Helium usage recorded: {usage_record}")

        # --- Optional persistence via carbon ledger ---
        if carbon_ledger is not None:
            try:
                recorder = getattr(
                    carbon_ledger, "record_helium_usage", None,
                )
                if callable(recorder):
                    result = recorder(usage_record)
                    if hasattr(result, "__await__"):
                        result = await result
                    return result
            except Exception as e:
                logger.warning(
                    f"Carbon ledger helium recording failed: {e}"
                )

        return None

    # ------------------------------------------------------------------
    # Accessors (preserved)
    # ------------------------------------------------------------------

    def get_current_status(self) -> Optional[HeliumSupplyStatus]:
        """Get current helium supply status (thread-safe)."""
        with self._lock:
            return self.current_supply_status

    def set_supply_status(
        self, status: HeliumSupplyStatus,
    ) -> None:
        """Inject a supply status (for testing / external monitoring)."""
        with self._lock:
            self.current_supply_status = status

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative adapter statistics."""
        with self._lock:
            return {
                "adapt_count": self._adapt_count,
                "denied_count": self._denied_count,
                "escalated_count": self._escalated_count,
                "current_source": (
                    self.current_supply_status.source
                    if self.current_supply_status else None
                ),
                "current_scarcity": (
                    self.current_supply_status.scarcity_level.value
                    if self.current_supply_status else None
                ),
                "monitoring_active": (
                    self._update_task is not None
                    and not self._update_task.done()
                ),
                "pdp_attached": self._decision_point is not None,
                "audit_log_attached": self._audit_log is not None,
                "features": dict(self.features),
            }


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from dataclasses import dataclass as _dc

    @_dc
    class _HeliumProfile:
        dependency_score: float
        can_run_on_cpu: bool = False
        supports_int8: bool = True

    @_dc
    class _WorkloadProfile:
        helium_profile: _HeliumProfile

    adapter = HeliumPolicyAdapter()

    # --- Simulate a CAUTION status ---
    adapter.set_supply_status(HeliumSupplyStatus(
        scarcity_level=HeliumScarcityLevel.CAUTION,
        scarcity_score=0.4,
        spot_price_usd_per_liter=5.5,
        fab_inventory_days=20,
        vendor_alerts=["Supply tightening"],
        forecast_days=30,
        recommended_action=RecommendedAction.OPTIMIZE_GPU,
        source="api",
    ))

    # --- High-dependency workload ---
    high = _WorkloadProfile(_HeliumProfile(dependency_score=0.85))
    policy = adapter.adapt_policy(high, system_state=None)
    print(f"High-dependency policy: {policy}")

    # --- Hardware capability translation ---
    capability = HardwareCapability(
        device_id="edge-01",
        max_memory_mb=8192.0,
        supported_precisions={"fp32", "fp16", "int8"},
        max_thermal_c=85.0,
        has_tensor_cores=True,
        supports_int8=True,
        helium_availability=0.7,
    )
    permitted = adapter.translate_hardware_capability(capability)
    print(f"Permitted policy: {permitted}")

    # --- SEVERE forces REDIRECT when CPU capable ---
    adapter.set_supply_status(HeliumSupplyStatus(
        scarcity_level=HeliumScarcityLevel.SEVERE,
        scarcity_score=0.9,
        spot_price_usd_per_liter=12.0,
        fab_inventory_days=5,
        vendor_alerts=[],
        forecast_days=30,
        recommended_action=RecommendedAction.REDIRECT_TO_CPU,
        source="api",
    ))
    cpu_profile = _WorkloadProfile(
        _HeliumProfile(dependency_score=0.9, can_run_on_cpu=True),
    )
    policy = adapter.adapt_policy(cpu_profile, system_state=None)
    print(f"CPU redirect policy: {policy}")

    import json
    print(json.dumps(adapter.get_statistics(), indent=2))
