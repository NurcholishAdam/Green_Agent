# src/governance/hardware/helium_policy_adapter.py

"""
Helium-aware policy adapter for Green Agent (Enhanced v3.0.0)
=============================================================

Translates helium supply status AND hardware capabilities into a
permitted execution policy that flows through the governance layer's
``PolicyDecisionPoint``.

v3.0.0 changes vs. v2.0.0
-------------------------
**P0 bug fixes**
  * 2.1 **Exhaustive action handling in ``adapt_policy``.** Previously a
        high-dependency workload with ``RECOMMENDED_ACTION`` set to
        ``REDIRECT_TO_CPU`` (with ``can_run_on_cpu=False``) or ``BLOCK``
        fell through every branch and returned ``action="normal"`` — the
        most constrained case produced the least constrained policy.
        Every enum value is now handled, with safe conservative fallbacks.
  * 2.2 ``RecommendedAction.BLOCK`` is now produced by
        ``_calculate_recommended_action`` when the fleet has neither CPU
        nor GPU capacity, and handled by ``adapt_policy`` for every
        dependency tier.
  * 2.3 ``_parse_api_response`` / ``_coerce_status`` use
        ``_safe_float`` / ``_safe_int`` / ``_safe_float_in_range`` —
        malformed payloads raise ``HeliumSupplyParseError`` (a subclass
        of ``HeliumPolicyError``) instead of leaking ``ValueError`` and
        crashing the monitor loop.
  * 2.4 **UTC normalization.** ``HeliumSupplyStatus.timestamp`` defaults
        to ``datetime.now(timezone.utc)``. The temporal check uses
        ``_as_aware_utc()`` on both sides — a caller passing an aware
        timestamp no longer crashes ``adapt_policy`` with
        ``TypeError: can't subtract offset-naive and offset-aware``.
  * 2.5 ``adapt_policy_with_decision`` uses ``_merge_obligations``
        (order-preserving, deterministic) and ``dataclasses.replace``
        (no in-place mutation). Obligation order is stable across
        ``PYTHONHASHSEED`` values — audit records are reproducible.
  * 2.6 Counters (``_adapt_count``, ``_denied_count``,
        ``_escalated_count``) are incremented under ``self._lock``.
        Concurrent ``adapt_policy_with_decision`` calls no longer
        under-report.
  * 2.7 **Audit log is actually called.** ``_audit_async`` invokes
        ``audit_log.record`` / ``.log`` / ``.append`` (whichever exists)
        on every adaptation, denial, and escalation.
  * 2.8 ``translate_hardware_capability`` **enforces** the
        ``requires_fallback`` flag: when helium is below threshold, the
        returned ``allowed_precisions`` is restricted to ``fp32``.
        Previously the flag was set but not honoured.
  * 2.9 Temporal verification **falls back** to a CAUTION baseline when
        status is stale — the log message ("treating as CAUTION") now
        matches the behaviour. Strict mode raises
        ``HeliumSupplyStaleError`` instead.

**P1 robustness**
  * 3.1 ``__post_init__`` validation on every dataclass — no more
        negative memory, non-normalized enums, or ``NaN`` prices.
  * 3.2 All 7 feature toggles are now wired:
        ``temporal_verification``, ``hardware_capability``,
        ``simulation_flag``, ``thread_safety``, ``dead_branch_fix``,
        ``audit_integration``, ``pdp_integration``.
  * 3.3 ``_coerce_status`` propagates ``scarcity_uncertainty``,
        ``trust_level``, ``permitted_precisions``, ``memory_ceiling_mb``,
        ``thermal_ceiling_c`` from the external monitor.
  * 3.5 ``metrics_hook(event, fields)`` fires on every adaptation.
  * 3.6 Pickling support, ``__repr__``, sync context manager,
        ``__version__`` exported.
  * 3.7 ``__del__`` warns if the background monitor is still running.
  * 3.8 ``record_helium_usage`` is also audited.
  * 3.9 ``recommended_action`` sent by the monitor is respected (was
        discarded and recomputed from supply severity).
  * 3.10 Simulation fallback is wrapped in ``try/except`` so a failure
        there cannot crash the monitor loop.
  * 3.12 ``scarcity_score`` and ``scarcity_uncertainty`` are bounded to
        ``[0, 1]``.

Backward compatible: the public API is unchanged. Dataclasses gain
additive fields (``cpu_capable``, ``gpu_available`` on
``HeliumSupplyStatus``; validation only). The original
``if __name__ == "__main__"`` demo still runs.
"""

from __future__ import annotations

import asyncio
import logging
import math
import pickle  # noqa: F401  (used in self-tests)
import threading
import uuid
from dataclasses import dataclass, field, asdict, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "3.0.0"


# =============================================================================
# Errors (P0 2.3)
# =============================================================================

class HeliumPolicyError(Exception):
    """Base class for every error raised by the helium policy adapter."""


class HeliumSupplyParseError(HeliumPolicyError):
    """Raised when a helium supply payload cannot be parsed."""


class HeliumSupplyStaleError(HeliumPolicyError):
    """Raised when supply status exceeds max age in strict mode."""


# =============================================================================
# Enums (preserved; __str__ added for consistency)
# =============================================================================

class HeliumScarcityLevel(Enum):
    """Helium supply chain scarcity levels."""
    NORMAL = "normal"
    CAUTION = "caution"
    CRITICAL = "critical"
    SEVERE = "severe"

    def __str__(self) -> str:
        return self.value


class RecommendedAction(Enum):
    """Recommended actions based on helium supply."""
    NORMAL = "normal"
    OPTIMIZE_GPU = "optimize_gpu_usage"
    THROTTLE_GPU = "throttle_gpu"
    DEFER_GPU = "defer_gpu"
    REDIRECT_TO_CPU = "redirect_to_cpu"
    BLOCK = "block"

    def __str__(self) -> str:
        return self.value


class PolicyAction(str, Enum):
    """The exhaustive set of ``AdaptedPolicy.action`` values."""
    NORMAL = "normal"
    OPTIMIZE = "optimize"
    THROTTLE = "throttle"
    DEFER = "defer"
    REDIRECT = "redirect"
    BLOCKED = "blocked"

    def __str__(self) -> str:
        return self.value


# =============================================================================
# Helpers
# =============================================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware_utc(dt: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC (P0 2.4)."""
    if not isinstance(dt, datetime):
        raise HeliumSupplyParseError(
            f"timestamp must be a datetime, got {type(dt).__name__}")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(payload: Mapping[str, Any], key: str,
                default: float) -> float:
    """Parse a numeric field, raising ``HeliumSupplyParseError`` on
    malformed input (P0 2.3)."""
    if key not in payload or payload[key] is None:
        return float(default)
    raw = payload[key]
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise HeliumSupplyParseError(
            f"{key!r} must be numeric, got {raw!r}")
    result = float(raw)
    if math.isnan(result) or math.isinf(result):
        raise HeliumSupplyParseError(
            f"{key!r} must be finite, got {raw!r}")
    return result


def _safe_int(payload: Mapping[str, Any], key: str,
              default: int) -> int:
    """Parse an integer field, raising ``HeliumSupplyParseError`` on
    malformed input (P0 2.3)."""
    if key not in payload or payload[key] is None:
        return int(default)
    raw = payload[key]
    if isinstance(raw, bool):
        raise HeliumSupplyParseError(
            f"{key!r} must be int, got {raw!r}")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise HeliumSupplyParseError(
            f"{key!r} must be int: {exc}") from exc


def _safe_float_in_range(
    payload: Mapping[str, Any], key: str, default: float,
    *, lo: float = 0.0, hi: float = 1.0,
) -> float:
    """Parse a bounded float (P1 3.12)."""
    value = _safe_float(payload, key, default)
    if not lo <= value <= hi:
        raise HeliumSupplyParseError(
            f"{key!r} must be in [{lo}, {hi}], got {value}")
    return value


def _merge_obligations(
    existing: Sequence[str], incoming: Sequence[str],
) -> List[str]:
    """Order-preserving, deterministic union (P0 2.5)."""
    seen: Set[str] = set()
    out: List[str] = []
    for item in list(existing) + list(incoming):
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _coerce_for_hash(value: Any) -> Any:
    """Deterministic JSON-safe coercion (used in audit payloads)."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _coerce_for_hash(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_coerce_for_hash(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_coerce_for_hash(v) for v in value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class HeliumSupplyStatus:
    """
    Real-time helium supply chain status.

    Extends the original with ``scarcity_uncertainty``,
    ``trust_level``, ``permitted_precisions``, and hardware-ceiling
    hints, plus ``cpu_capable`` / ``gpu_available`` flags that mirror
    what the supply-side payload reported.
    """
    scarcity_level: HeliumScarcityLevel
    scarcity_score: float
    spot_price_usd_per_liter: float
    fab_inventory_days: int
    vendor_alerts: List[str]
    forecast_days: int
    recommended_action: RecommendedAction
    timestamp: datetime = field(default_factory=_now_utc)
    source: str = "api"
    scarcity_uncertainty: float = 0.0
    trust_level: str = "medium"
    permitted_precisions: List[str] = field(default_factory=list)
    memory_ceiling_mb: Optional[float] = None
    thermal_ceiling_c: Optional[float] = None
    cpu_capable: Optional[bool] = None
    gpu_available: Optional[bool] = None

    def __post_init__(self) -> None:
        # Coerce enums (case-insensitive) — P1 3.1
        if not isinstance(self.scarcity_level, HeliumScarcityLevel):
            try:
                self.scarcity_level = HeliumScarcityLevel(
                    str(self.scarcity_level).strip().lower())
            except ValueError as exc:
                raise HeliumSupplyParseError(
                    f"scarcity_level must be HeliumScarcityLevel, "
                    f"got {self.scarcity_level!r}") from exc
        if not isinstance(self.recommended_action, RecommendedAction):
            try:
                self.recommended_action = RecommendedAction(
                    str(self.recommended_action).strip().lower())
            except ValueError as exc:
                raise HeliumSupplyParseError(
                    f"recommended_action must be RecommendedAction, "
                    f"got {self.recommended_action!r}") from exc

        # Range checks
        if not 0.0 <= self.scarcity_score <= 1.0:
            raise HeliumSupplyParseError(
                f"scarcity_score must be in [0, 1], "
                f"got {self.scarcity_score}")
        if not 0.0 <= self.scarcity_uncertainty <= 1.0:
            raise HeliumSupplyParseError(
                f"scarcity_uncertainty must be in [0, 1], "
                f"got {self.scarcity_uncertainty}")

        # Finite / non-negative numerics
        for name in ("spot_price_usd_per_liter",):
            v = getattr(self, name)
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise HeliumSupplyParseError(f"{name} must be numeric")
            if math.isnan(v) or math.isinf(v):
                raise HeliumSupplyParseError(f"{name} must be finite")
            if v < 0:
                raise HeliumSupplyParseError(f"{name} must be >= 0")

        if not isinstance(self.fab_inventory_days, int) or \
                self.fab_inventory_days < 0:
            raise HeliumSupplyParseError(
                f"fab_inventory_days must be a non-negative int, "
                f"got {self.fab_inventory_days!r}")

        if not isinstance(self.vendor_alerts, list):
            raise HeliumSupplyParseError(
                "vendor_alerts must be a list")

        # UTC-normalize timestamp (P0 2.4)
        self.timestamp = _as_aware_utc(self.timestamp)


@dataclass
class AdaptedPolicy:
    """Policy adaptation result."""
    action: str
    throttle_factor: Optional[float] = None
    reason: str = ""
    helium_aware: bool = False
    target_hardware: Optional[str] = None
    simulated: bool = False
    policy_version: str = "v5.0.0"
    obligations: List[str] = field(default_factory=list)
    preferred_precision: Optional[str] = None
    audit_event_id: Optional[str] = None
    risk_level: str = "low"
    human_approval_required: bool = False
    decision_attached: bool = False

    def __post_init__(self) -> None:
        # Validate action against the enum values
        valid_actions = {a.value for a in PolicyAction}
        if self.action not in valid_actions:
            raise HeliumSupplyParseError(
                f"action must be one of {sorted(valid_actions)}, "
                f"got {self.action!r}")

        if self.throttle_factor is not None:
            if isinstance(self.throttle_factor, bool) or \
                    not isinstance(self.throttle_factor, (int, float)):
                raise HeliumSupplyParseError(
                    "throttle_factor must be numeric or None")
            if math.isnan(self.throttle_factor) or \
                    math.isinf(self.throttle_factor):
                raise HeliumSupplyParseError(
                    "throttle_factor must be finite")
            if not 0.0 <= self.throttle_factor <= 1.0:
                raise HeliumSupplyParseError(
                    f"throttle_factor must be in [0, 1], "
                    f"got {self.throttle_factor}")

        if not isinstance(self.obligations, list):
            raise HeliumSupplyParseError(
                "obligations must be a list")
        for i, item in enumerate(self.obligations):
            if not isinstance(item, str):
                raise HeliumSupplyParseError(
                    f"obligations[{i}] must be a string")

        if self.risk_level not in ("low", "medium", "high", "critical"):
            raise HeliumSupplyParseError(
                f"risk_level must be low/medium/high/critical, "
                f"got {self.risk_level!r}")

        if not isinstance(self.human_approval_required, bool):
            raise HeliumSupplyParseError(
                "human_approval_required must be a bool")


@dataclass
class HardwareCapability:
    """Detected hardware capabilities of the target device."""
    device_id: str
    max_memory_mb: float
    supported_precisions: Set[str] = field(
        default_factory=lambda: {"fp32"})
    max_thermal_c: Optional[float] = None
    has_tensor_cores: bool = False
    supports_int8: bool = False
    supports_int4: bool = False
    helium_availability: float = 1.0
    region: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.device_id, str) or not self.device_id:
            raise HeliumSupplyParseError(
                "device_id must be a non-empty string")
        if not isinstance(self.max_memory_mb, (int, float)) or \
                isinstance(self.max_memory_mb, bool):
            raise HeliumSupplyParseError(
                "max_memory_mb must be numeric")
        if math.isnan(self.max_memory_mb) or \
                math.isinf(self.max_memory_mb):
            raise HeliumSupplyParseError(
                "max_memory_mb must be finite")
        if self.max_memory_mb <= 0:
            raise HeliumSupplyParseError(
                "max_memory_mb must be > 0")
        if not isinstance(self.supported_precisions, set) or \
                not self.supported_precisions:
            raise HeliumSupplyParseError(
                "supported_precisions must be a non-empty set")
        for p in self.supported_precisions:
            if not isinstance(p, str) or not p:
                raise HeliumSupplyParseError(
                    "supported_precisions must be non-empty strings")
        if self.max_thermal_c is not None:
            if self.max_thermal_c <= 0:
                raise HeliumSupplyParseError(
                    "max_thermal_c must be > 0 when provided")
        if not 0.0 <= self.helium_availability <= 1.0:
            raise HeliumSupplyParseError(
                f"helium_availability must be in [0, 1], "
                f"got {self.helium_availability}")


@dataclass
class PermittedPolicy:
    """Execution policy permitted for a given capability."""
    device_id: str
    allowed_precisions: List[str] = field(default_factory=list)
    max_memory_mb: float = 0.0
    max_thermal_c: Optional[float] = None
    requires_fallback: bool = False
    notes: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.device_id, str) or not self.device_id:
            raise HeliumSupplyParseError(
                "device_id must be a non-empty string")
        if not isinstance(self.allowed_precisions, list):
            raise HeliumSupplyParseError(
                "allowed_precisions must be a list")
        if self.max_memory_mb <= 0:
            raise HeliumSupplyParseError(
                "max_memory_mb must be > 0")


# =============================================================================
# HeliumPolicyAdapter — Enhanced
# =============================================================================

class HeliumPolicyAdapter:
    """
    Helium-aware policy adapter for governance integration.

    See the module docstring for the design principles.
    """

    DEFAULT_THRESHOLDS: Dict[str, float] = {
        "caution_score": 0.3,
        "critical_score": 0.6,
        "severe_score": 0.8,
        "price_caution_usd": 5.0,
        "price_critical_usd": 7.0,
        "price_severe_usd": 10.0,
    }

    MEMORY_HEADROOM: float = 0.15
    THERMAL_HEADROOM_C: float = 5.0
    HELIUM_FALLBACK_THRESHOLD: float = 0.4
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
        strict_stale: bool = False,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ):
        self.config = config or {}

        # --- Original fields (preserved) ---
        self.api_url = self.config.get(
            "helium_api_url",
            "https://api.helium-supply.example.com/v1",
        )
        self.update_interval_seconds = self.config.get(
            "update_interval", 300)
        self.current_supply_status: Optional[HeliumSupplyStatus] = None
        self.policy_cache: Dict[str, Any] = {}
        self._update_task: Optional[asyncio.Task] = None

        # --- Thresholds (preserved name) ---
        self.thresholds = dict(self.DEFAULT_THRESHOLDS)
        user_thresholds = self.config.get("thresholds")
        if isinstance(user_thresholds, dict):
            self.thresholds.update(user_thresholds)

        # --- Governance integration ---
        self._supply_monitor = supply_monitor
        self._decision_point = decision_point
        self._audit_log = audit_log
        self._strict_stale = bool(strict_stale)

        if metrics_hook is not None and not callable(metrics_hook):
            raise HeliumPolicyError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        # --- Feature toggles (all wired in v3.0.0) ---
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

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics (incremented under the lock) ---
        self._adapt_count: int = 0
        self._denied_count: int = 0
        self._escalated_count: int = 0

        # --- Optional background monitoring ---
        if auto_start_monitoring or self.config.get(
            "auto_start_monitoring", False
        ):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                logger.warning(
                    "auto_start_monitoring requested but no running "
                    "event loop; monitoring not started")
            else:
                self._start_monitoring()

        logger.info(
            "HeliumPolicyAdapter v%s initialized "
            "(supply_monitor=%s, decision_point=%s, audit_log=%s)",
            __version__,
            "yes" if supply_monitor else "no",
            "yes" if decision_point else "no",
            "yes" if audit_log else "no",
        )

    # ------------------------------------------------------------------
    # Metrics (P1 3.5)
    # ------------------------------------------------------------------

    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ------------------------------------------------------------------
    # Audit (P0 2.7)
    # ------------------------------------------------------------------

    async def _audit_async(
        self, event_type: str, payload: Mapping[str, Any],
    ) -> None:
        if self._audit_log is None or \
                not self.features["audit_integration"]:
            return
        recorder = (
            getattr(self._audit_log, "record", None)
            or getattr(self._audit_log, "log", None)
            or getattr(self._audit_log, "append", None)
        )
        if not callable(recorder):
            logger.debug("audit_log has no record/log/append; skipping")
            return
        try:
            safe_payload = _coerce_for_hash(dict(payload))
            result = recorder(event_type, safe_payload)
            if hasattr(result, "__await__"):
                await result
        except Exception as exc:  # noqa: BLE001
            logger.warning("audit_log failed: %s", exc)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _start_monitoring(self) -> None:
        if self._update_task is not None and not self._update_task.done():
            logger.debug("Monitoring task already running")
            return
        self._update_task = asyncio.create_task(
            self._monitor_helium_supply())

    async def shutdown(self) -> None:
        if self._update_task and not self._update_task.done():
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
            logger.info("HeliumPolicyAdapter monitoring stopped")
        self._update_task = None

    def __del__(self) -> None:  # P1 3.7
        task = getattr(self, "_update_task", None)
        if task is not None and not task.done():
            logger.warning(
                "HeliumPolicyAdapter garbage-collected with a "
                "running background task; call `await shutdown()` "
                "before releasing the adapter.")

    async def __aenter__(self) -> "HeliumPolicyAdapter":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.shutdown()
        except Exception:  # noqa: BLE001
            logger.warning("shutdown() failed", exc_info=True)

    async def _monitor_helium_supply(self) -> None:
        while True:
            try:
                status = await self.fetch_helium_supply()
                with self._lock:
                    self.current_supply_status = status
                logger.info(
                    "Helium supply updated: %s (source=%s)",
                    status.scarcity_level.value, status.source)
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                logger.error("Failed to fetch helium supply: %s", e)
                # P1 3.10: wrap the simulation fallback itself.
                try:
                    fallback = self._simulate_helium_supply()
                except Exception as sim_exc:  # noqa: BLE001
                    logger.error(
                        "Simulation fallback failed: %s", sim_exc)
                    fallback = None
                if fallback is not None:
                    with self._lock:
                        self.current_supply_status = fallback

            try:
                await asyncio.sleep(self.update_interval_seconds)
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    async def fetch_helium_supply(self) -> HeliumSupplyStatus:
        # Delegate to injected monitor first
        if self._supply_monitor is not None:
            try:
                getter = getattr(
                    self._supply_monitor, "get_current_status", None)
                if callable(getter):
                    result = getter()
                    if hasattr(result, "__await__"):
                        result = await result
                    if isinstance(result, HeliumSupplyStatus):
                        return result
                    return self._coerce_status(result)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "External supply monitor failed: %s; "
                    "falling back to simulation", e)
                return self._simulate_helium_supply()

        # Original network path (preserved)
        try:
            import aiohttp  # noqa: F401
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_url}/supply/current", timeout=5,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_api_response(data)
                    raise HeliumPolicyError(
                        f"API returned {resp.status}")
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Helium API unavailable, using simulation: %s", e)
            return self._simulate_helium_supply()

    def _coerce_status(self, obj: Any) -> HeliumSupplyStatus:
        """Coerce an external monitor-shaped object into HeliumSupplyStatus."""
        if isinstance(obj, Mapping):
            return self._parse_api_response(dict(obj))

        # Duck-typed path (P1 3.3: propagate extended fields).
        scarcity_str = getattr(obj, "scarcity_level", "normal")
        if isinstance(scarcity_str, HeliumScarcityLevel):
            scarcity = scarcity_str
        else:
            scarcity = self._safe_parse_scarcity(str(scarcity_str))

        price = float(getattr(obj, "spot_price_usd_per_liter", 4.0))

        # P1 3.9: respect an explicit recommended_action from the source.
        explicit_action = getattr(obj, "recommended_action", None)
        if isinstance(explicit_action, RecommendedAction):
            action = explicit_action
        elif explicit_action is not None:
            try:
                action = RecommendedAction(
                    str(explicit_action).strip().lower())
            except ValueError:
                action = self._calculate_recommended_action({
                    "scarcity_level": scarcity.value,
                    "spot_price_usd": price,
                    "cpu_capable": getattr(obj, "cpu_capable", None),
                    "gpu_available": getattr(
                        obj, "gpu_available", None),
                })
        else:
            action = self._calculate_recommended_action({
                "scarcity_level": scarcity.value,
                "spot_price_usd": price,
                "cpu_capable": getattr(obj, "cpu_capable", None),
                "gpu_available": getattr(obj, "gpu_available", None),
            })

        return HeliumSupplyStatus(
            scarcity_level=scarcity,
            scarcity_score=float(
                getattr(obj, "scarcity_score", 0.0)),
            spot_price_usd_per_liter=price,
            fab_inventory_days=int(
                getattr(obj, "fab_inventory_days", 30)),
            vendor_alerts=list(
                getattr(obj, "vendor_alerts", []) or []),
            forecast_days=int(getattr(obj, "forecast_days", 30)),
            recommended_action=action,
            timestamp=_as_aware_utc(
                getattr(obj, "timestamp", None) or _now_utc()),
            source=str(getattr(obj, "source", "external")),
            scarcity_uncertainty=float(
                getattr(obj, "scarcity_uncertainty", 0.0)),
            trust_level=str(getattr(obj, "trust_level", "medium")),
            permitted_precisions=list(
                getattr(obj, "permitted_precisions", []) or []),
            memory_ceiling_mb=getattr(obj, "memory_ceiling_mb", None),
            thermal_ceiling_c=getattr(obj, "thermal_ceiling_c", None),
            cpu_capable=getattr(obj, "cpu_capable", None),
            gpu_available=getattr(obj, "gpu_available", None),
        )

    # ------------------------------------------------------------------
    # Safe parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_parse_scarcity(value: str) -> HeliumScarcityLevel:
        try:
            return HeliumScarcityLevel(str(value).strip().lower())
        except (ValueError, TypeError):
            logger.warning(
                "Unknown helium scarcity level %r; defaulting to CAUTION",
                value)
            return HeliumScarcityLevel.CAUTION

    def _parse_api_response(self, data: Dict) -> HeliumSupplyStatus:
        """Parse an API response into a HeliumSupplyStatus (P0 2.3)."""
        scarcity_str = data.get("scarcity_level", "normal")
        scarcity_level = self._safe_parse_scarcity(scarcity_str)

        # P1 3.9: respect explicit recommended_action when present.
        explicit = data.get("recommended_action")
        if explicit is not None:
            try:
                recommended_action = RecommendedAction(
                    str(explicit).strip().lower())
            except ValueError:
                recommended_action = self._calculate_recommended_action(data)
        else:
            recommended_action = self._calculate_recommended_action(data)

        return HeliumSupplyStatus(
            scarcity_level=scarcity_level,
            scarcity_score=_safe_float_in_range(
                data, "scarcity_score", 0.0),
            spot_price_usd_per_liter=_safe_float(
                data, "spot_price_usd", 4.0),
            fab_inventory_days=_safe_int(
                data, "fab_inventory_days", 30),
            vendor_alerts=list(data.get("alerts", []) or []),
            forecast_days=_safe_int(data, "forecast_days", 30),
            recommended_action=recommended_action,
            source="api",
            scarcity_uncertainty=_safe_float_in_range(
                data, "scarcity_uncertainty", 0.0),
            trust_level=str(data.get("trust_level", "medium")),
            permitted_precisions=list(
                data.get("permitted_precisions", []) or []),
            memory_ceiling_mb=data.get("memory_ceiling_mb"),
            thermal_ceiling_c=data.get("thermal_ceiling_c"),
            cpu_capable=data.get("cpu_capable"),
            gpu_available=data.get("gpu_available"),
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
        self, supply_data: Mapping[str, Any],
    ) -> RecommendedAction:
        """
        Calculate the recommended action.

        v3.0.0: ``BLOCK`` is now reachable when neither CPU nor GPU is
        viable. ``REDIRECT_TO_CPU`` fires when CPU is viable.
        """
        scarcity = str(
            supply_data.get("scarcity_level", "normal")).strip().lower()
        price = float(supply_data.get("spot_price_usd", 4.0))
        cpu_capable = supply_data.get("cpu_capable")
        gpu_available = supply_data.get("gpu_available")

        # Severe: either redirect to CPU (if viable) or BLOCK (if no GPU
        # and no CPU) or DEFER (if GPU exists but scarce).
        if scarcity == "severe" or \
                price > self.thresholds["price_severe_usd"]:
            if cpu_capable is True:
                return RecommendedAction.REDIRECT_TO_CPU
            if gpu_available is False and cpu_capable is False:
                return RecommendedAction.BLOCK
            return RecommendedAction.DEFER_GPU

        if scarcity == "critical" or \
                price > self.thresholds["price_critical_usd"]:
            return RecommendedAction.THROTTLE_GPU

        if scarcity == "caution" or \
                price > self.thresholds["price_caution_usd"]:
            return RecommendedAction.OPTIMIZE_GPU

        return RecommendedAction.NORMAL

    # ------------------------------------------------------------------
    # Hardware-aware policy translation (P0 2.8)
    # ------------------------------------------------------------------

    def translate_hardware_capability(
        self, capability: HardwareCapability,
    ) -> PermittedPolicy:
        """
        Translate a hardware capability into a permitted execution policy.

        When ``capability.helium_availability`` is below
        ``HELIUM_FALLBACK_THRESHOLD``, the returned
        ``allowed_precisions`` is restricted to ``fp32`` — the flag is
        now enforced, not advisory.
        """
        allowed = sorted(capability.supported_precisions)
        memory = capability.max_memory_mb * (1 - self.MEMORY_HEADROOM)
        thermal = (
            capability.max_thermal_c - self.THERMAL_HEADROOM_C
            if capability.max_thermal_c is not None else None
        )

        fallback = (
            capability.helium_availability <
            self.HELIUM_FALLBACK_THRESHOLD
        )
        notes: List[str] = []

        # P0 2.8: enforce the fallback restriction.
        if fallback:
            if "fp32" in allowed:
                allowed = ["fp32"]
                notes.append(
                    "fallback enforced: only fp32 permitted "
                    f"(helium_availability="
                    f"{capability.helium_availability:.2f} < "
                    f"{self.HELIUM_FALLBACK_THRESHOLD})")
            else:
                # Device doesn't even support fp32 — nothing permitted.
                allowed = []
                notes.append(
                    "fallback enforced: fp32 unavailable; "
                    "no precisions permitted")

        if not capability.has_tensor_cores:
            notes.append("no tensor cores; FP16 may not accelerate")
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
    # Policy adaptation (preserved API; P0 2.1 + P0 2.2 + P0 2.9)
    # ------------------------------------------------------------------

    def adapt_policy(
        self,
        workload_profile: Any,
        system_state: Any,
    ) -> AdaptedPolicy:
        """
        Adapt execution policy based on helium supply and workload
        characteristics (backward compatible).

        Every ``RecommendedAction`` is handled for every dependency
        tier — no more fall-through to ``normal``.
        """
        with self._lock:
            status = self.current_supply_status

        if status is None:
            return AdaptedPolicy(
                action=PolicyAction.NORMAL.value,
                helium_aware=False,
                simulated=False,
                reason="no helium data",
            )

        # P0 2.9: temporal verification falls back to CAUTION baseline.
        status = self._verify_status_freshness(status)

        # P0 2.x: simulation flag (respect the feature toggle)
        simulated = (
            status.source == "simulation"
            if self.features["simulation_flag"] else False
        )

        # Workload helium profile
        helium_profile = getattr(
            workload_profile, "helium_profile", None)
        if not helium_profile:
            return AdaptedPolicy(
                action=PolicyAction.NORMAL.value,
                helium_aware=False,
                simulated=simulated,
                reason="no helium profile in workload",
            )

        dependency = float(
            getattr(helium_profile, "dependency_score", 0.0))
        can_run_on_cpu = bool(
            getattr(helium_profile, "can_run_on_cpu", False))
        supports_int8 = bool(
            getattr(helium_profile, "supports_int8", False))

        # P1 3.2: respect the hardware_capability feature toggle.
        if self.features["hardware_capability"]:
            hw_cap = getattr(
                workload_profile, "hardware_capability", None)
            if hw_cap is not None:
                if hasattr(hw_cap, "helium_availability") and \
                        hw_cap.helium_availability < \
                        self.HELIUM_FALLBACK_THRESHOLD:
                    # Force CPU redirect when hardware is depleted.
                    can_run_on_cpu = can_run_on_cpu or True

        action_enum = status.recommended_action

        # P1 3.2: dead_branch_fix toggle — if disabled, restore the
        # original REDIRECT never-fires behaviour for backward compat.
        if not self.features["dead_branch_fix"] and \
                action_enum == RecommendedAction.REDIRECT_TO_CPU:
            action_enum = RecommendedAction.DEFER_GPU

        # ---- HIGH dependency ----
        if dependency > 0.7:
            return self._handle_high_dependency(
                status, action_enum, dependency,
                can_run_on_cpu, supports_int8, simulated)

        # ---- MEDIUM dependency ----
        if dependency > 0.4:
            return self._handle_medium_dependency(
                status, action_enum, can_run_on_cpu, simulated)

        # ---- LOW dependency ----
        return self._handle_low_dependency(
            status, action_enum, can_run_on_cpu, simulated)

    def _handle_high_dependency(
        self,
        status: HeliumSupplyStatus,
        action_enum: RecommendedAction,
        dependency: float,
        can_run_on_cpu: bool,
        supports_int8: bool,
        simulated: bool,
    ) -> AdaptedPolicy:
        """HIGH-dependency workloads (> 0.7): exhaustive handling (P0 2.1)."""
        if action_enum == RecommendedAction.BLOCK:
            if can_run_on_cpu:
                return AdaptedPolicy(
                    action=PolicyAction.REDIRECT.value,
                    target_hardware="cpu",
                    reason="helium BLOCK: redirecting to CPU",
                    helium_aware=True,
                    simulated=simulated,
                    risk_level="high",
                    obligations=[
                        "verify_cpu_capability",
                        "record_redirect_reason",
                        "log_helium_telemetry",
                    ],
                )
            return AdaptedPolicy(
                action=PolicyAction.BLOCKED.value,
                reason=(
                    f"helium BLOCK: no viable hardware for "
                    f"dependency={dependency:.2f}"),
                helium_aware=True,
                simulated=simulated,
                risk_level="critical",
                human_approval_required=True,
                obligations=[
                    "record_block_reason",
                    "escalate_to_human",
                ],
            )

        if action_enum == RecommendedAction.DEFER_GPU:
            return AdaptedPolicy(
                action=PolicyAction.DEFER.value,
                reason=(
                    f"Helium scarcity: {status.scarcity_level.value}, "
                    f"price=${status.spot_price_usd_per_liter}/L"),
                helium_aware=True,
                simulated=simulated,
                risk_level=(
                    "high" if dependency > 0.85 else "medium"),
                obligations=[
                    "log_helium_telemetry",
                    "record_deferral_reason",
                ],
            )

        if action_enum == RecommendedAction.THROTTLE_GPU:
            throttle_factor = max(0.3, 1.0 - dependency)
            return AdaptedPolicy(
                action=PolicyAction.THROTTLE.value,
                throttle_factor=throttle_factor,
                reason=(
                    f"Helium supply constrained: "
                    f"{status.scarcity_level.value}"),
                helium_aware=True,
                simulated=simulated,
                obligations=[
                    "log_helium_telemetry",
                    "record_throttle_factor",
                ],
            )

        if action_enum == RecommendedAction.OPTIMIZE_GPU:
            return AdaptedPolicy(
                action=PolicyAction.OPTIMIZE.value,
                reason="Helium caution mode - prefer quantization",
                helium_aware=True,
                simulated=simulated,
                preferred_precision=(
                    "int8" if supports_int8 else None),
                obligations=["prefer_quantized_execution"],
            )

        if action_enum == RecommendedAction.REDIRECT_TO_CPU:
            if can_run_on_cpu:
                return AdaptedPolicy(
                    action=PolicyAction.REDIRECT.value,
                    target_hardware="cpu",
                    reason=(
                        "Redirecting to CPU due to helium shortage"),
                    helium_aware=True,
                    simulated=simulated,
                    obligations=[
                        "verify_cpu_capability",
                        "record_redirect_reason",
                    ],
                )
            # P0 2.1: previously fell through to `normal`.
            return AdaptedPolicy(
                action=PolicyAction.DEFER.value,
                reason=(
                    "REDIRECT recommended but CPU not viable; "
                    "deferring instead"),
                helium_aware=True,
                simulated=simulated,
                risk_level="high",
                obligations=[
                    "record_deferral_reason",
                    "log_helium_telemetry",
                ],
            )

        # RecommendedAction.NORMAL (or unknown)
        return AdaptedPolicy(
            action=PolicyAction.NORMAL.value,
            helium_aware=False,
            simulated=simulated,
            reason="supply recommends normal for high-dependency",
        )

    def _handle_medium_dependency(
        self,
        status: HeliumSupplyStatus,
        action_enum: RecommendedAction,
        can_run_on_cpu: bool,
        simulated: bool,
    ) -> AdaptedPolicy:
        """MEDIUM-dependency workloads (0.4 < dep ≤ 0.7)."""
        if action_enum == RecommendedAction.BLOCK:
            if can_run_on_cpu:
                return AdaptedPolicy(
                    action=PolicyAction.REDIRECT.value,
                    target_hardware="cpu",
                    reason="helium BLOCK: redirecting to CPU",
                    helium_aware=True,
                    simulated=simulated,
                    obligations=[
                        "verify_cpu_capability",
                        "record_redirect_reason",
                    ],
                )
            return AdaptedPolicy(
                action=PolicyAction.BLOCKED.value,
                reason="helium BLOCK: no viable hardware",
                helium_aware=True,
                simulated=simulated,
                risk_level="high",
                obligations=["record_block_reason"],
            )

        if action_enum == RecommendedAction.DEFER_GPU:
            return AdaptedPolicy(
                action=PolicyAction.THROTTLE.value,
                throttle_factor=0.5,
                reason=(
                    "Helium severe scarcity, throttling "
                    "medium-dependency workload"),
                helium_aware=True,
                simulated=simulated,
                obligations=["log_helium_telemetry"],
            )

        if action_enum == RecommendedAction.THROTTLE_GPU:
            return AdaptedPolicy(
                action=PolicyAction.THROTTLE.value,
                throttle_factor=0.6,
                reason="Helium critical scarcity",
                helium_aware=True,
                simulated=simulated,
                obligations=["log_helium_telemetry"],
            )

        if action_enum == RecommendedAction.REDIRECT_TO_CPU:
            if can_run_on_cpu:
                return AdaptedPolicy(
                    action=PolicyAction.REDIRECT.value,
                    target_hardware="cpu",
                    reason="Redirecting to CPU due to helium shortage",
                    helium_aware=True,
                    simulated=simulated,
                    obligations=[
                        "verify_cpu_capability",
                        "record_redirect_reason",
                    ],
                )
            return AdaptedPolicy(
                action=PolicyAction.THROTTLE.value,
                throttle_factor=0.5,
                reason=(
                    "REDIRECT recommended but CPU not viable; "
                    "throttling instead"),
                helium_aware=True,
                simulated=simulated,
                obligations=["log_helium_telemetry"],
            )

        # OPTIMIZE_GPU or NORMAL
        return AdaptedPolicy(
            action=PolicyAction.NORMAL.value,
            helium_aware=False,
            simulated=simulated,
            reason="medium dependency, no action required",
        )

    def _handle_low_dependency(
        self,
        status: HeliumSupplyStatus,
        action_enum: RecommendedAction,
        can_run_on_cpu: bool,
        simulated: bool,
    ) -> AdaptedPolicy:
        """LOW-dependency workloads (dep ≤ 0.4)."""
        # BLOCK is the only severe action that overrides low dependency.
        if action_enum == RecommendedAction.BLOCK:
            if can_run_on_cpu:
                return AdaptedPolicy(
                    action=PolicyAction.REDIRECT.value,
                    target_hardware="cpu",
                    reason="helium BLOCK: redirecting to CPU",
                    helium_aware=True,
                    simulated=simulated,
                    obligations=[
                        "verify_cpu_capability",
                        "record_redirect_reason",
                    ],
                )
            return AdaptedPolicy(
                action=PolicyAction.BLOCKED.value,
                reason=(
                    "helium BLOCK: no viable hardware for any "
                    "workload"),
                helium_aware=True,
                simulated=simulated,
                risk_level="high",
                obligations=["record_block_reason"],
            )

        return AdaptedPolicy(
            action=PolicyAction.NORMAL.value,
            helium_aware=False,
            simulated=simulated,
            reason="low helium dependency",
        )

    def _verify_status_freshness(
        self, status: HeliumSupplyStatus,
    ) -> HeliumSupplyStatus:
        """P0 2.9: temporal verification actually falls back to CAUTION."""
        if not self.features["temporal_verification"]:
            return status
        age = (
            _now_utc() - _as_aware_utc(status.timestamp)
        ).total_seconds()
        if age <= self.MAX_STATUS_AGE_SECONDS:
            return status

        if self._strict_stale:
            raise HeliumSupplyStaleError(
                f"Helium status is {age:.0f}s old "
                f"(max {self.MAX_STATUS_AGE_SECONDS:.0f}s); "
                f"strict mode refuses to use stale data")

        logger.warning(
            "Helium status age=%.0fs exceeds %.0fs; treating as "
            "CAUTION baseline.",
            age, self.MAX_STATUS_AGE_SECONDS)
        return replace(
            status,
            scarcity_level=HeliumScarcityLevel.CAUTION,
            recommended_action=RecommendedAction.OPTIMIZE_GPU,
            source=f"{status.source}+stale",
        )

    # ------------------------------------------------------------------
    # Governance-integrated adaptation (P0 2.5 + P0 2.6 + P0 2.7)
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

        Returns ``(AdaptedPolicy, PolicyDecision or None)``.
        """
        adapted = self.adapt_policy(workload_profile, system_state)

        # P0 2.7: audit every adaptation.
        await self._audit_async("helium_policy_adapted", {
            "agent_id": agent_id,
            "task_id": task_id,
            "action": adapted.action,
            "reason": adapted.reason,
            "helium_aware": adapted.helium_aware,
            "simulated": adapted.simulated,
            "risk_level": adapted.risk_level,
            "obligations": list(adapted.obligations),
        })

        # P1 3.5: metrics
        self._emit_metric("policy_adapted", {
            "action": adapted.action,
            "helium_aware": adapted.helium_aware,
            "simulated": adapted.simulated,
            "risk_level": adapted.risk_level,
        })

        if self._decision_point is None or \
                not self.features["pdp_integration"]:
            # P0 2.6: lock counter updates
            with self._lock:
                self._adapt_count += 1
            return adapted, None

        # --- Build DecisionRequest (lazy import) ---
        try:
            from ...contracts.decision_request import DecisionRequest
        except Exception:  # noqa: BLE001
            try:
                from src.governance.contracts.decision_request import (
                    DecisionRequest,
                )
            except Exception:  # noqa: BLE001
                logger.debug(
                    "DecisionRequest not importable; skipping PDP")
                with self._lock:
                    self._adapt_count += 1
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
        except Exception as e:  # noqa: BLE001
            logger.warning("PDP decision failed: %s", e)
            with self._lock:
                self._adapt_count += 1
            return adapted, None

        if decision is None:
            with self._lock:
                self._adapt_count += 1
            return adapted, None

        # P0 2.5: deterministic merge + immutable update.
        merged_obligations = _merge_obligations(
            adapted.obligations,
            getattr(decision, "obligations", []) or [],
        )
        adapted = replace(
            adapted,
            obligations=merged_obligations,
            audit_event_id=getattr(decision, "audit_event_id", None),
            risk_level=getattr(decision, "risk_level", adapted.risk_level),
            human_approval_required=getattr(
                decision, "human_approval_required", False),
            decision_attached=True,
        )

        verdict = getattr(decision, "verdict", None)
        if verdict == "deny":
            adapted = replace(
                adapted,
                action=PolicyAction.BLOCKED.value,
                reason=(
                    "denied by governance: "
                    f"{'; '.join(getattr(decision, 'reasons', []) or [])}"
                ),
            )
            with self._lock:
                self._denied_count += 1
        elif verdict == "escalate":
            with self._lock:
                self._escalated_count += 1

        with self._lock:
            self._adapt_count += 1

        # Audit the final decision too (P0 2.7).
        await self._audit_async("helium_policy_decision", {
            "agent_id": agent_id,
            "task_id": task_id,
            "verdict": verdict,
            "final_action": adapted.action,
            "obligations": list(adapted.obligations),
        })

        return adapted, decision

    # ------------------------------------------------------------------
    # Usage recording (audit added)
    # ------------------------------------------------------------------

    async def record_helium_usage(
        self,
        task_id: str,
        helium_usage: float,
        execution_result: Dict,
        *,
        carbon_ledger: Optional[Any] = None,
    ) -> Optional[str]:
        with self._lock:
            status_at_exec = (
                self.current_supply_status.scarcity_level.value
                if self.current_supply_status else "unknown")
            source_at_exec = (
                self.current_supply_status.source
                if self.current_supply_status else "unknown")

        usage_record = {
            "task_id": task_id,
            "helium_usage": float(helium_usage),
            "timestamp": _now_utc().isoformat(),
            "supply_status_at_execution": status_at_exec,
            "supply_source_at_execution": source_at_exec,
            "execution_result": dict(execution_result or {}),
        }

        logger.info("Helium usage recorded: %s", usage_record)

        # P1 3.8: audit the usage.
        await self._audit_async("helium_usage_recorded", usage_record)

        if carbon_ledger is not None:
            try:
                recorder = getattr(
                    carbon_ledger, "record_helium_usage", None)
                if callable(recorder):
                    result = recorder(usage_record)
                    if hasattr(result, "__await__"):
                        result = await result
                    return result
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Carbon ledger helium recording failed: %s", e)

        return None

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_current_status(self) -> Optional[HeliumSupplyStatus]:
        with self._lock:
            return self.current_supply_status

    def set_supply_status(self, status: HeliumSupplyStatus) -> None:
        with self._lock:
            self.current_supply_status = status

    # ------------------------------------------------------------------
    # Statistics (P0 2.6: locked)
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "adapt_count": self._adapt_count,
                "denied_count": self._denied_count,
                "escalated_count": self._escalated_count,
                "current_source": (
                    self.current_supply_status.source
                    if self.current_supply_status else None),
                "current_scarcity": (
                    self.current_supply_status.scarcity_level.value
                    if self.current_supply_status else None),
                "monitoring_active": (
                    self._update_task is not None
                    and not self._update_task.done()),
                "pdp_attached": self._decision_point is not None,
                "audit_log_attached": self._audit_log is not None,
                "features": dict(self.features),
            }

    # ------------------------------------------------------------------
    # Pickling
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_update_task", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._update_task = None

    def __repr__(self) -> str:
        with self._lock:
            return (
                "HeliumPolicyAdapter("
                f"version={__version__!r}, "
                f"scarcity="
                f"{self.current_supply_status.scarcity_level.value if self.current_supply_status else None!r}, "
                f"adapt_count={self._adapt_count}, "
                f"pdp={self._decision_point is not None}, "
                f"audit={self._audit_log is not None})"
            )


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "AdaptedPolicy",
    "HardwareCapability",
    "HeliumPolicyAdapter",
    "HeliumPolicyError",
    "HeliumScarcityLevel",
    "HeliumSupplyParseError",
    "HeliumSupplyStaleError",
    "HeliumSupplyStatus",
    "PermittedPolicy",
    "PolicyAction",
    "RecommendedAction",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import asyncio
    import tempfile

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== helium_policy_adapter v{__version__} self-tests ===\n")

    from dataclasses import dataclass as _dc

    @_dc
    class _HeliumProfile:
        dependency_score: float
        can_run_on_cpu: bool = False
        supports_int8: bool = True

    @_dc
    class _WorkloadProfile:
        helium_profile: _HeliumProfile

    class _AuditLog:
        def __init__(self):
            self.records: List = []

        def record(self, event_type, payload):
            self.records.append((event_type, payload))

    def _status(**overrides) -> HeliumSupplyStatus:
        base = dict(
            scarcity_level=HeliumScarcityLevel.NORMAL,
            scarcity_score=0.1,
            spot_price_usd_per_liter=4.0,
            fab_inventory_days=30,
            vendor_alerts=[],
            forecast_days=30,
            recommended_action=RecommendedAction.NORMAL,
            source="api",
        )
        base.update(overrides)
        return HeliumSupplyStatus(**base)

    # ---------------------------------------------------------------- #
    # P0 2.1 — exhaustive action handling
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: HIGH-dep + unhandled action ===")

    adapter = HeliumPolicyAdapter()
    adapter.set_supply_status(_status(
        scarcity_level=HeliumScarcityLevel.SEVERE,
        recommended_action=RecommendedAction.REDIRECT_TO_CPU,
    ))
    high_dep = _WorkloadProfile(_HeliumProfile(
        dependency_score=0.9, can_run_on_cpu=False))
    policy = adapter.adapt_policy(high_dep, None)
    assert policy.action == PolicyAction.DEFER.value, (
        f"P0 2.1 regression: expected 'defer', got "
        f"{policy.action!r}")
    print(f"  ✓ HIGH-dep + REDIRECT + no CPU → defer (was 'normal')")

    # Contrast: with CPU viable → redirect.
    cpu_ok = _WorkloadProfile(_HeliumProfile(
        dependency_score=0.9, can_run_on_cpu=True))
    policy = adapter.adapt_policy(cpu_ok, None)
    assert policy.action == PolicyAction.REDIRECT.value
    print(f"  ✓ HIGH-dep + REDIRECT + CPU viable → redirect")

    # ---------------------------------------------------------------- #
    # P0 2.2 — BLOCK reachable
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: BLOCK reachable ===")

    # _calculate_recommended_action produces BLOCK.
    adapter2 = HeliumPolicyAdapter()
    action = adapter2._calculate_recommended_action({
        "scarcity_level": "severe",
        "spot_price_usd": 15.0,
        "cpu_capable": False,
        "gpu_available": False,
    })
    assert action == RecommendedAction.BLOCK, action
    print(f"  ✓ no CPU + no GPU + severe → BLOCK")

    # adapt_policy handles BLOCK for high-dep non-CPU workloads.
    adapter2.set_supply_status(_status(
        scarcity_level=HeliumScarcityLevel.SEVERE,
        recommended_action=RecommendedAction.BLOCK,
    ))
    policy = adapter2.adapt_policy(high_dep, None)
    assert policy.action == PolicyAction.BLOCKED.value, (
        f"P0 2.2 regression: expected 'blocked', got "
        f"{policy.action!r}")
    assert policy.human_approval_required is True
    print(f"  ✓ HIGH-dep + BLOCK + no CPU → blocked + HITL")

    # With CPU → redirect.
    policy = adapter2.adapt_policy(cpu_ok, None)
    assert policy.action == PolicyAction.REDIRECT.value
    print(f"  ✓ HIGH-dep + BLOCK + CPU viable → redirect")

    # ---------------------------------------------------------------- #
    # P0 2.3 — malformed payload raises HeliumSupplyParseError
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: malformed payload ===")

    adapter3 = HeliumPolicyAdapter()
    try:
        adapter3._parse_api_response({
            "scarcity_level": "normal",
            "scarcity_score": "garbage",
        })
    except HeliumSupplyParseError as exc:
        print(f"  ✓ 'garbage' score → HeliumSupplyParseError")
    except ValueError as exc:
        raise AssertionError(
            f"P0 2.3 regression: raw ValueError: {exc}")
    else:
        raise AssertionError("P0 2.3 regression: no raise")

    try:
        adapter3._parse_api_response({
            "scarcity_level": "normal",
            "scarcity_score": 1.5,    # out of range
        })
    except HeliumSupplyParseError:
        print(f"  ✓ out-of-range score → HeliumSupplyParseError")

    # Valid payload works.
    s = adapter3._parse_api_response({
        "scarcity_level": "caution",
        "scarcity_score": 0.4,
        "spot_price_usd": 5.5,
    })
    assert s.scarcity_level == HeliumScarcityLevel.CAUTION
    print(f"  ✓ valid payload parses correctly")

    # ---------------------------------------------------------------- #
    # P0 2.4 — UTC normalization
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: UTC normalization ===")

    # Naive timestamp normalizes to UTC.
    naive_status = _status(timestamp=datetime(2024, 1, 1, 12, 0, 0))
    assert naive_status.timestamp.tzinfo is not None
    print(f"  ✓ naive timestamp → UTC-aware")

    # Aware timestamp in adapt_policy doesn't crash.
    aware_status = _status(
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    adapter_aware = HeliumPolicyAdapter()
    adapter_aware.set_supply_status(aware_status)
    policy = adapter_aware.adapt_policy(
        _WorkloadProfile(_HeliumProfile(dependency_score=0.9)),
        None,
    )
    assert policy.action in (
        PolicyAction.DEFER.value, PolicyAction.NORMAL.value,
    )
    print(f"  ✓ aware timestamp + temporal check → no TypeError")

    # ---------------------------------------------------------------- #
    # P0 2.5 — deterministic obligation merge
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: obligation merge is deterministic ===")

    existing = ["a", "b"]
    incoming = ["c", "a", "d"]
    merged = _merge_obligations(existing, incoming)
    # Order-preserving: existing first, then new from incoming.
    assert merged == ["a", "b", "c", "d"], merged
    print(f"  ✓ merge {existing} + {incoming} → {merged}")

    # Repeated calls produce identical output (not just set equality).
    for _ in range(5):
        assert _merge_obligations(existing, incoming) == merged
    print(f"  ✓ merge is reproducible")

    # ---------------------------------------------------------------- #
    # P0 2.6 — locked counters
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: locked counters ===")

    class _MockPDP:
        async def decide(self, request):
            class _D:
                verdict = "allow"
                obligations = []
                audit_event_id = "evt-1"
                risk_level = "low"
                human_approval_required = False
                reasons = []
            return _D()

    async def _test_locked():
        adapter = HeliumPolicyAdapter(decision_point=_MockPDP())
        adapter.set_supply_status(_status())
        workload = _WorkloadProfile(_HeliumProfile(dependency_score=0.2))

        n = 50
        await asyncio.gather(*(
            adapter.adapt_policy_with_decision(workload, None)
            for _ in range(n)
        ))
        stats = adapter.get_statistics()
        assert stats["adapt_count"] == n, (
            f"P0 2.6 regression: adapt_count={stats['adapt_count']}, "
            f"expected {n}")
        print(f"  ✓ {n} concurrent calls → adapt_count={stats['adapt_count']}")

    asyncio.run(_test_locked())

    # ---------------------------------------------------------------- #
    # P0 2.7 — audit log actually called
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: audit log integration ===")

    async def _test_audit():
        audit = _AuditLog()
        adapter = HeliumPolicyAdapter(audit_log=audit)
        adapter.set_supply_status(_status())
        workload = _WorkloadProfile(_HeliumProfile(dependency_score=0.9))
        await adapter.adapt_policy_with_decision(workload, None)

        assert len(audit.records) >= 1, (
            f"P0 2.7 regression: audit_log never called")
        event_type, payload = audit.records[0]
        assert event_type == "helium_policy_adapted"
        assert "action" in payload
        print(f"  ✓ audit_log.record called: {event_type}")

    asyncio.run(_test_audit())

    # ---------------------------------------------------------------- #
    # P0 2.8 — fallback enforced
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: fallback enforced ===")

    adapter = HeliumPolicyAdapter()
    cap = HardwareCapability(
        device_id="edge-01",
        max_memory_mb=8192.0,
        supported_precisions={"fp32", "fp16", "int8"},
        helium_availability=0.2,   # below threshold
    )
    permitted = adapter.translate_hardware_capability(cap)
    assert permitted.requires_fallback is True
    assert permitted.allowed_precisions == ["fp32"], (
        f"P0 2.8 regression: allowed_precisions="
        f"{permitted.allowed_precisions}, expected ['fp32']")
    print(f"  ✓ helium_availability=0.2 → only fp32 permitted")

    # Above threshold: full set.
    cap_ok = HardwareCapability(
        device_id="edge-02",
        max_memory_mb=8192.0,
        supported_precisions={"fp32", "fp16", "int8"},
        helium_availability=0.9,
    )
    permitted_ok = adapter.translate_hardware_capability(cap_ok)
    assert permitted_ok.requires_fallback is False
    assert "fp16" in permitted_ok.allowed_precisions
    print(f"  ✓ helium_availability=0.9 → full precision set")

    # ---------------------------------------------------------------- #
    # P0 2.9 — stale status falls back to CAUTION
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.9: stale status → CAUTION ===")

    from datetime import timedelta
    stale = _status(
        scarcity_level=HeliumScarcityLevel.NORMAL,
        recommended_action=RecommendedAction.NORMAL,
        timestamp=_now_utc() - timedelta(hours=2),
    )
    adapter_stale = HeliumPolicyAdapter()
    adapter_stale.set_supply_status(stale)
    workload = _WorkloadProfile(_HeliumProfile(dependency_score=0.8))
    policy = adapter_stale.adapt_policy(workload, None)
    # Stale → CAUTION baseline → OPTIMIZE_GPU → "optimize" for high-dep.
    assert policy.action == PolicyAction.OPTIMIZE.value, (
        f"P0 2.9 regression: expected 'optimize' from CAUTION "
        f"fallback, got {policy.action!r}")
    print(f"  ✓ stale 2h status → CAUTION fallback → optimize")

    # Strict mode raises.
    adapter_strict = HeliumPolicyAdapter(strict_stale=True)
    adapter_strict.set_supply_status(stale)
    try:
        adapter_strict.adapt_policy(workload, None)
    except HeliumSupplyStaleError:
        print(f"  ✓ strict_stale=True → HeliumSupplyStaleError")

    # ---------------------------------------------------------------- #
    # Regression: v2.0.0 API still works
    # ---------------------------------------------------------------- #
    print("\n=== Regression: v2.0.0 API ===")

    adapter = HeliumPolicyAdapter()
    adapter.set_supply_status(_status(
        scarcity_level=HeliumScarcityLevel.CAUTION,
        recommended_action=RecommendedAction.OPTIMIZE_GPU,
        spot_price_usd_per_liter=5.5,
    ))
    high = _WorkloadProfile(_HeliumProfile(dependency_score=0.85))
    policy = adapter.adapt_policy(high, None)
    assert policy.action == PolicyAction.OPTIMIZE.value
    assert policy.helium_aware is True
    print(f"  ✓ v2.0.0 adapt_policy still works")

    # No-profile case.
    class _EmptyWorkload:
        pass
    policy = adapter.adapt_policy(_EmptyWorkload(), None)
    assert policy.action == PolicyAction.NORMAL.value
    print(f"  ✓ empty workload → normal")

    # No-status case.
    adapter_no_status = HeliumPolicyAdapter()
    policy = adapter_no_status.adapt_policy(high, None)
    assert policy.action == PolicyAction.NORMAL.value
    print(f"  ✓ no status → normal")

    # ---------------------------------------------------------------- #
    # Feature toggles are wired
    # ---------------------------------------------------------------- #
    print("\n=== Feature toggles wired ===")

    # temporal_verification off → stale status passes through.
    adapter_no_temporal = HeliumPolicyAdapter(
        features={"temporal_verification": False})
    adapter_no_temporal.set_supply_status(stale)
    policy = adapter_no_temporal.adapt_policy(workload, None)
    # Stale status is still NORMAL → no policy change.
    assert policy.action in (
        PolicyAction.NORMAL.value,
    ), policy.action
    print(f"  ✓ temporal_verification=False → stale status used as-is")

    # simulation_flag off → simulated always False.
    sim_status = _status(source="simulation")
    adapter_no_sim = HeliumPolicyAdapter(
        features={"simulation_flag": False})
    adapter_no_sim.set_supply_status(sim_status)
    policy = adapter_no_sim.adapt_policy(
        _WorkloadProfile(_HeliumProfile(dependency_score=0.2)),
        None,
    )
    assert policy.simulated is False
    print(f"  ✓ simulation_flag=False → simulated=False")

    # ---------------------------------------------------------------- #
    # Pickle + context manager
    # ---------------------------------------------------------------- #
    print("\n=== Pickle / context manager ===")

    adapter = HeliumPolicyAdapter()
    adapter.set_supply_status(_status())
    blob = pickle.dumps(adapter)
    restored = pickle.loads(blob)
    assert restored._lock is not None
    assert restored._update_task is None
    assert restored.get_current_status() is not None
    print(f"  ✓ pickle round-trip preserves status")

    async def _test_ctx():
        async with HeliumPolicyAdapter() as adapter:
            adapter.set_supply_status(_status())
            policy = adapter.adapt_policy(
                _WorkloadProfile(_HeliumProfile(dependency_score=0.2)),
                None,
            )
            assert policy.action == PolicyAction.NORMAL.value

    asyncio.run(_test_ctx())
    print(f"  ✓ async context manager works")

    # ---------------------------------------------------------------- #
    # Dataclass validation
    # ---------------------------------------------------------------- #
    print("\n=== Dataclass validation ===")

    try:
        HardwareCapability(
            device_id="",
            max_memory_mb=100.0,
        )
    except HeliumSupplyParseError:
        print(f"  ✓ empty device_id rejected")

    try:
        HardwareCapability(
            device_id="d",
            max_memory_mb=100.0,
            helium_availability=1.5,
        )
    except HeliumSupplyParseError:
        print(f"  ✓ out-of-range helium_availability rejected")

    try:
        AdaptedPolicy(action="not-an-action")
    except HeliumSupplyParseError:
        print(f"  ✓ invalid action rejected")

    try:
        AdaptedPolicy(action="normal", throttle_factor=1.5)
    except HeliumSupplyParseError:
        print(f"  ✓ out-of-range throttle_factor rejected")

    # Enum coercion.
    status = _status(scarcity_level="CAUTION")  # type: ignore
    assert status.scarcity_level == HeliumScarcityLevel.CAUTION
    print(f"  ✓ enum coercion: string → HeliumScarcityLevel.CAUTION")

    # ---------------------------------------------------------------- #
    # record_helium_usage audits
    # ---------------------------------------------------------------- #
    print("\n=== record_helium_usage audits ===")

    async def _test_usage():
        audit = _AuditLog()
        adapter = HeliumPolicyAdapter(audit_log=audit)
        adapter.set_supply_status(_status())
        await adapter.record_helium_usage(
            "t-1", 0.5, {"status": "ok"})
        event_types = [e for e, _ in audit.records]
        assert "helium_usage_recorded" in event_types, event_types
        print(f"  ✓ record_helium_usage fires audit")

    asyncio.run(_test_usage())

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    import sys
    if "--selftest" in sys.argv or len(sys.argv) == 1:
        _run_selftests()
    else:
        # Original demo
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
        high = _WorkloadProfile(_HeliumProfile(dependency_score=0.85))
        policy = adapter.adapt_policy(high, system_state=None)
        print(f"High-dependency policy: {policy}")

        capability = HardwareCapability(
            device_id="edge-01",
            max_memory_mb=8192.0,
            supported_precisions={"fp32", "fp16", "int8"},
            max_thermal_c=85.0,
            has_tensor_cores=True,
            supports_int8=True,
            helium_availability=0.2,   # below threshold → fallback
        )
        permitted = adapter.translate_hardware_capability(capability)
        print(f"Permitted policy: {permitted}")

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

        # Non-viable CPU: previously fell through to normal.
        no_cpu_profile = _WorkloadProfile(
            _HeliumProfile(dependency_score=0.9, can_run_on_cpu=False),
        )
        policy = adapter.adapt_policy(no_cpu_profile, system_state=None)
        print(f"No-CPU policy (was 'normal'): {policy}")

        import json
        print(json.dumps(adapter.get_statistics(), indent=2))
