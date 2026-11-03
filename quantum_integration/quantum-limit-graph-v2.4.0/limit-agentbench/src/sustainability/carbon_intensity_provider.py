# src/sustainability/carbon_intensity_provider.py

"""
Carbon Intensity Provider
=========================

Provides the current carbon intensity (gCO2/kWh) and classifies the grid
into CLEAN / MODERATE / DIRTY / CRITICAL zones.

Boundary semantics
------------------
- ``zone_for`` — a value equal to ``dirty_threshold`` belongs to the
  **higher** zone (``DIRTY``). Lower boundaries are strict
  (``intensity < clean_threshold`` → ``CLEAN``; at exactly
  ``clean_threshold`` → ``MODERATE``).
- ``is_grid_dirty`` — "exceeds threshold" uses **strict** ``>``. A grid at
  exactly ``dirty_threshold`` is ``zone=DIRTY`` but ``dirty=False``. This
  is intentional: "reported zone" and "actionable alarm" are distinct
  signals.

Default state
-------------
``default_intensity`` (385.0 gCO2/kWh) sits in the ``MODERATE`` zone, so a
freshly constructed provider reports ``zone=MODERATE`` before its first
``update_manual``. This is a plausible mid-grid value, not an error state.
Override ``default_intensity`` in the config if a different baseline is
appropriate for your deployment.

Enhancements
------------
- ``GridZone`` — ``StrEnum`` with a 3.8–3.10 shim, so ``str(zone)`` is
  ``"clean"`` (not ``"GridZone.CLEAN"``) on every supported Python.
- ``CarbonIntensityProviderConfig`` — frozen, ``slots``, ``__repr__``;
  every field type-checked (``bool`` rejected for ints, ``NaN``/``inf``
  rejected for floats); ``default_intensity <= max_intensity`` enforced;
  ``from_dict(..., strict=True)`` rejects unknown keys.
- ``zone_for`` **rejects non-finite intensity** instead of silently
  returning ``CRITICAL`` for ``nan``.
- ``is_grid_dirty`` / ``classify_zone`` validate their inputs as
  ``CarbonIntensityProviderError``, not leaked ``ValueError`` from
  ``float(...)``.
- **``to_json(include_history=True)`` now works** — the kwarg is routed
  to ``to_dict`` instead of being forwarded to ``json.dumps``.
- **Lifecycle counters** — ``total_updates`` and ``dropped_updates`` are
  decoupled from ``len(history)``.
- **Atomic ``statistics()``** — every field read under the lock.
- **Non-strict coercion is marked** — a sample coerced to ``0.0`` (or
  clamped) carries ``"coerced": True`` and ``"raw_repr"`` in the audit
  trail so consumers can filter it out.
- ``update_manual_batch(values)`` — validates all values first, then
  applies under one lock, so a failure mid-batch leaves the provider
  untouched.
- ``from_dict`` validates ``current_intensity`` against the ceiling and
  every history entry (finite, in range, valid zone); wraps all parse
  errors as :class:`CarbonIntensityProviderError`.
- ``to_dict(include_history=True)`` returns **copied** history entries,
  not aliased internal dicts.
- ``to_json`` no longer uses ``default=str``.
- ``__eq__`` / ``__hash__`` on config and provider.
- ``CarbonIntensityStatistics`` / ``CarbonIntensitySample`` TypedDicts.
- Class docstring documents ``region`` mutation caveat and
  ``_started_at`` uptime semantics.

Compatibility
-------------
- ``CarbonIntensityProvider(region=None, *, config=None, strict=True)``
  unchanged.
- ``update_manual(value) -> float`` unchanged in return type; non-strict
  coercion now marks the history entry (additive).
- ``get_current_intensity()`` / ``is_grid_dirty()`` / ``classify_zone()``
  / ``reset()`` unchanged in signature; input validation is tighter.
- ``zone_for(nan)`` now raises instead of returning ``CRITICAL``.
- ``CarbonIntensityProviderConfig(default_intensity=nan)`` /
  ``(max_intensity=nan)`` now raise.
- ``CarbonIntensityProviderConfig(default_intensity > max_intensity)``
  now raises.
- ``to_json`` signature is now ``to_json(*, include_history=False,
  **kwargs)`` — the kwarg routes correctly (previously it crashed).
- ``CarbonIntensityProviderConfig.from_dict(...)`` now rejects unknown
  keys by default; pass ``strict=False`` to opt out.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from enum import Enum
from typing import (
    Any, Deque, Dict, Iterable, List, Mapping, Optional, Tuple, TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# StrEnum compatibility shim
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — Python 3.11+
    from enum import StrEnum  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — 3.10 and earlier
    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """``str`` + ``Enum`` with ``__str__``/``__format__`` delegating to ``str``."""

        __str__ = str.__str__
        __format__ = str.__format__


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonIntensityProviderError(ValueError):
    """Raised for invalid provider inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed shapes
# --------------------------------------------------------------------------- #
class CarbonIntensitySample(TypedDict, total=False):
    """One entry in the audit trail."""

    value: float
    zone: str
    timestamp: float
    coerced: bool
    raw_repr: str


class CarbonIntensityStatistics(TypedDict):
    """Return shape of :meth:`CarbonIntensityProvider.statistics`."""

    region: Optional[str]
    current_intensity: float
    current_zone: str
    updates: int
    total_updates: int
    dropped_updates: int
    config: Dict[str, Any]
    strict: bool
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Zones
# --------------------------------------------------------------------------- #
class GridZone(StrEnum):
    """Carbon intensity zones."""

    CLEAN = "clean"        # intensity <  clean_threshold
    MODERATE = "moderate"  # clean_threshold  .. dirty_threshold (excl.)
    DIRTY = "dirty"        # dirty_threshold  .. critical_threshold (excl.)
    CRITICAL = "critical"  # intensity >= critical_threshold

    @property
    def description(self) -> str:
        return {
            GridZone.CLEAN: "Very low carbon intensity",
            GridZone.MODERATE: "Moderate carbon intensity",
            GridZone.DIRTY: "High carbon intensity",
            GridZone.CRITICAL: "Severe carbon intensity",
        }[self]


_VALID_ZONE_VALUES = frozenset(z.value for z in GridZone)


# --------------------------------------------------------------------------- #
# Numeric validation helpers
# --------------------------------------------------------------------------- #
def _require_finite_number(name: str, value: Any) -> float:
    """Return ``float(value)``; raise if non-numeric, ``bool``, or non-finite."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CarbonIntensityProviderError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise CarbonIntensityProviderError(
            f"{name} must be finite, got {value!r}."
        )
    return fv


def _require_int(name: str, value: Any) -> int:
    """Return ``int(value)``; raise if non-int or ``bool``."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CarbonIntensityProviderError(f"{name} must be an int.")
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class CarbonIntensityProviderConfig:
    """Tunable parameters for :class:`CarbonIntensityProvider`."""

    clean_threshold: float = 200.0
    dirty_threshold: float = 400.0
    critical_threshold: float = 600.0

    default_intensity: float = 385.0
    max_intensity: float = 1e6

    # Bounded update history.
    max_history: int = 1_000

    def __post_init__(self) -> None:
        # --- thresholds: finite, > 0, strictly increasing ------------- #
        ct = _require_finite_number("clean_threshold", self.clean_threshold)
        dt = _require_finite_number("dirty_threshold", self.dirty_threshold)
        crt = _require_finite_number(
            "critical_threshold", self.critical_threshold
        )
        for name, fv in (
            ("clean_threshold", ct),
            ("dirty_threshold", dt),
            ("critical_threshold", crt),
        ):
            if fv <= 0:
                raise CarbonIntensityProviderError(
                    f"{name} must be > 0, got {fv}."
                )
        if not (ct < dt < crt):
            raise CarbonIntensityProviderError(
                "thresholds must be strictly increasing: "
                "clean < dirty < critical."
            )

        # --- default_intensity --------------------------------------- #
        di = _require_finite_number(
            "default_intensity", self.default_intensity
        )
        if di < 0:
            raise CarbonIntensityProviderError(
                "default_intensity must be >= 0."
            )

        # --- max_intensity ------------------------------------------- #
        mi = _require_finite_number("max_intensity", self.max_intensity)
        if mi <= 0:
            raise CarbonIntensityProviderError("max_intensity must be > 0.")

        # --- default_intensity must not exceed max_intensity --------- #
        if di > mi:
            raise CarbonIntensityProviderError(
                f"default_intensity={di} must be <= max_intensity={mi}."
            )

        # --- max_history --------------------------------------------- #
        mh = _require_int("max_history", self.max_history)
        if mh <= 0:
            raise CarbonIntensityProviderError("max_history must be > 0.")

    def zone_for(self, intensity: float) -> GridZone:
        """
        Classify ``intensity`` into a :class:`GridZone`.

        Raises
        ------
        CarbonIntensityProviderError
            If ``intensity`` is non-numeric, non-finite, or negative.
            A ``NaN`` intensity is a programming error, not a valid
            classification query.
        """
        fv = _require_finite_number("intensity", intensity)
        if fv < 0:
            raise CarbonIntensityProviderError(
                f"intensity must be >= 0, got {fv}."
            )
        if fv < self.clean_threshold:
            return GridZone.CLEAN
        if fv < self.dirty_threshold:
            return GridZone.MODERATE
        if fv < self.critical_threshold:
            return GridZone.DIRTY
        return GridZone.CRITICAL

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "CarbonIntensityProviderConfig":
        if not isinstance(data, Mapping):
            raise CarbonIntensityProviderError(
                "CarbonIntensityProviderConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise CarbonIntensityProviderError(
                    f"unknown CarbonIntensityProviderConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except CarbonIntensityProviderError:
            raise
        except (TypeError, ValueError) as exc:
            raise CarbonIntensityProviderError(
                f"invalid CarbonIntensityProviderConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "CarbonIntensityProviderConfig("
            f"clean={self.clean_threshold:.3g}, "
            f"dirty={self.dirty_threshold:.3g}, "
            f"critical={self.critical_threshold:.3g}, "
            f"default={self.default_intensity:.3g}, "
            f"max={self.max_intensity:.3g}, "
            f"history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Provider
# --------------------------------------------------------------------------- #
class CarbonIntensityProvider:
    """
    Provides current carbon intensity (gCO2/kWh).

    Thread-safe for all public methods. Direct mutation of ``region`` or
    the underlying ``_current_intensity`` bypasses validation and locking —
    prefer the public API (``update_manual`` / ``update_manual_batch`` /
    ``reset``).

    ``_started_at`` lifecycle
    -------------------------
    Set at construction. :meth:`reset` resets it to ``time.time()``.
    :meth:`from_dict` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored provider reports the
    **lifetime of the serialized object**, not the current instance. Call
    :meth:`reset` after restoring if you want uptime relative to now.

    ``update_manual`` failure behaviour (non-strict mode)
    -----------------------------------------------------
    Bad samples (non-numeric, ``NaN``, ``inf``, negative, or above
    ``max_intensity``) are coerced to ``0.0`` (or clamped) **and marked**
    in the audit trail with ``"coerced": True`` and ``"raw_repr"``. The
    current intensity and history receive the coerced value, so downstream
    statistics still see the sample — filter ``history`` entries by
    ``coerced`` to exclude them.
    """

    def __init__(
        self,
        region: Optional[str] = None,
        *,
        config: Optional[CarbonIntensityProviderConfig] = None,
        strict: bool = True,
    ) -> None:
        if region is not None and (
            not isinstance(region, str) or not region
        ):
            raise CarbonIntensityProviderError(
                "region must be a non-empty string or None."
            )
        if config is not None and not isinstance(
            config, CarbonIntensityProviderConfig
        ):
            raise CarbonIntensityProviderError(
                f"config must be a CarbonIntensityProviderConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or CarbonIntensityProviderConfig()
        self._strict = bool(strict)

        # Legacy attribute; mutation not thread-safe — see class docstring.
        self.region: Optional[str] = region
        self._current_intensity: float = self._config.default_intensity

        self._lock = threading.RLock()
        self._history: Deque[CarbonIntensitySample] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        # Lifecycle counters (cumulative; not reset by reset()).
        self._total_updates: int = 0
        self._dropped_updates: int = 0

        logger.debug(
            "CarbonIntensityProvider initialized "
            "(region=%s, default=%.1f, strict=%s)",
            self.region, self._current_intensity, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CarbonIntensityProviderConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def current_intensity(self) -> float:
        with self._lock:
            return self._current_intensity

    @property
    def current_zone(self) -> GridZone:
        with self._lock:
            return self._config.zone_for(self._current_intensity)

    @property
    def total_updates(self) -> int:
        with self._lock:
            return self._total_updates

    @property
    def history(self) -> List[CarbonIntensitySample]:
        """Read-only snapshot of the audit trail."""
        with self._lock:
            return [dict(o) for o in self._history]

    # ---------------------------------------------------------- public API
    def update_manual(self, value: float) -> float:
        """
        Update the current intensity manually.

        Returns the new intensity. In strict mode, raises
        :class:`CarbonIntensityProviderError` for non-numeric,
        non-finite, negative, or above-ceiling values. In non-strict mode,
        coerces and marks the sample (see class docstring).
        """
        fv, coerced, raw_repr = self._validate_intensity(value)
        with self._lock:
            self._apply_update_locked(fv, coerced, raw_repr)
        logger.info("Carbon intensity updated: %.3f gCO2/kWh.", fv)
        return fv

    def update_manual_batch(self, values: Iterable[float]) -> int:
        """
        Update multiple readings with one lock acquisition.

        All values are validated **before** any state is mutated, so a
        strict-mode failure on the Nth value leaves the provider
        unchanged. Returns the number of samples accepted.
        """
        validated: List[Tuple[float, bool, Optional[str]]] = []
        for v in values:
            validated.append(self._validate_intensity(v))
        with self._lock:
            for fv, coerced, raw_repr in validated:
                self._apply_update_locked(fv, coerced, raw_repr)
        return len(validated)

    def get_current_intensity(self) -> float:
        """Return the current carbon intensity."""
        with self._lock:
            return self._current_intensity

    def is_grid_dirty(self, threshold: Optional[float] = None) -> bool:
        """
        Return True when the current intensity **strictly exceeds**
        ``threshold``.

        When ``threshold`` is ``None`` the provider's ``dirty_threshold``
        from the config is used. Note the boundary semantics: a grid at
        exactly ``dirty_threshold`` reports ``zone=DIRTY`` but
        ``dirty=False`` — the "reported zone" and the "actionable alarm"
        use different boundaries.

        Raises
        ------
        CarbonIntensityProviderError
            If ``threshold`` is non-numeric, non-finite, or negative.
        """
        if threshold is None:
            t = self._config.dirty_threshold
        else:
            t = _require_finite_number("threshold", threshold)
            if t < 0:
                raise CarbonIntensityProviderError(
                    "threshold must be >= 0."
                )
        with self._lock:
            return self._current_intensity > t

    def classify_zone(
        self, intensity: Optional[float] = None
    ) -> GridZone:
        """
        Return the zone for ``intensity`` (default: current intensity).

        ``intensity`` is validated as a strict-mode query regardless of
        the provider's ``_strict`` flag: a query about a malformed value
        is a programming error, not data.

        Raises
        ------
        CarbonIntensityProviderError
            If ``intensity`` is non-numeric, non-finite, or negative.
        """
        if intensity is None:
            with self._lock:
                v = self._current_intensity
        else:
            v = _require_finite_number("intensity", intensity)
        return self._config.zone_for(v)

    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset to the default intensity. Returns buffered entries removed.

        Lifecycle counters (``total_updates``, ``dropped_updates``) are
        **not** reset — they are cumulative for the object's lifetime.
        """
        with self._lock:
            removed = len(self._history)
            self._current_intensity = self._config.default_intensity
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug(
            "CarbonIntensityProvider reset (removed %d).", removed,
        )
        return removed

    # ---------------------------------------------------------- internals
    def _apply_update_locked(
        self,
        value: float,
        coerced: bool,
        raw_repr: Optional[str],
    ) -> None:
        """Commit one validated sample to state + history."""
        self._current_intensity = value
        zone = self._config.zone_for(value)

        sample: CarbonIntensitySample = {
            "value": value,
            "zone": zone.value,
            "timestamp": time.time(),
        }
        if coerced:
            sample["coerced"] = True
            if raw_repr is not None:
                sample["raw_repr"] = raw_repr

        self._total_updates += 1
        if len(self._history) == self._config.max_history:
            self._dropped_updates += 1
        self._history.append(sample)

    def _validate_intensity(
        self, value: Any
    ) -> Tuple[float, bool, Optional[str]]:
        """
        Return ``(validated_value, was_coerced, raw_repr)``.

        ``raw_repr`` is ``None`` when the value passed unchanged; otherwise
        it is ``repr(value)`` (kept JSON-safe by the caller).
        """
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon intensity must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        fv = float(value)
        if not math.isfinite(fv):
            msg = f"carbon intensity must be finite, got {value!r}."
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        if fv < 0:
            msg = f"carbon intensity must be >= 0, got {fv}."
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        if fv > self._config.max_intensity:
            msg = (
                f"carbon intensity must be <= {self._config.max_intensity}, "
                f"got {fv}."
            )
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning(
                "%s Clamping to %s.",
                msg, self._config.max_intensity,
            )
            return self._config.max_intensity, True, repr(value)
        return fv, False, None

    # ---------------------------------------------------------- statistics
    def statistics(self) -> CarbonIntensityStatistics:
        """Return a JSON-safe, atomic snapshot of the provider state."""
        with self._lock:
            zone = self._config.zone_for(self._current_intensity)
            return {
                "region": self.region,
                "current_intensity": self._current_intensity,
                "current_zone": zone.value,
                "updates": len(self._history),
                "total_updates": self._total_updates,
                "dropped_updates": self._dropped_updates,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "region": self.region,
                "current_intensity": self._current_intensity,
                "started_at": self._started_at,
                "total_updates": self._total_updates,
                "dropped_updates": self._dropped_updates,
            }
            if include_history:
                # Defensive copy — do not alias internal dicts.
                payload["history"] = [
                    dict(o) for o in self._history
                ]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CarbonIntensityProvider":
        if not isinstance(data, Mapping):
            raise CarbonIntensityProviderError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = CarbonIntensityProviderConfig.from_dict(
                dict(data.get("config") or {}), strict=True
            )
            region_raw = data.get("region")
            region = (
                None if region_raw is None
                else str(region_raw) or None
            )
            provider = cls(
                region=region, config=cfg,
                strict=bool(data.get("strict", True)),
            )
            with provider._lock:
                # ---- current_intensity ------------------------------ #
                ci_raw = data.get(
                    "current_intensity", cfg.default_intensity
                )
                ci = _require_finite_number(
                    "current_intensity", ci_raw
                )
                if ci < 0 or ci > cfg.max_intensity:
                    raise CarbonIntensityProviderError(
                        f"current_intensity={ci} must be in "
                        f"[0, {cfg.max_intensity}]."
                    )
                provider._current_intensity = ci

                # ---- started_at ------------------------------------- #
                if data.get("started_at") is not None:
                    provider._started_at = float(data["started_at"])

                # ---- counters --------------------------------------- #
                if "total_updates" in data:
                    provider._total_updates = int(
                        data["total_updates"]
                    )
                if "dropped_updates" in data:
                    provider._dropped_updates = int(
                        data["dropped_updates"]
                    )

                # ---- history ---------------------------------------- #
                for entry in data.get("history", []) or []:
                    if not isinstance(entry, Mapping):
                        raise CarbonIntensityProviderError(
                            f"history entries must be Mappings, got "
                            f"{type(entry).__name__}."
                        )
                    fv = _require_finite_number(
                        "history.value", entry.get("value")
                    )
                    if fv < 0 or fv > cfg.max_intensity:
                        raise CarbonIntensityProviderError(
                            f"history.value={fv} must be in "
                            f"[0, {cfg.max_intensity}]."
                        )
                    zone_val = entry.get("zone")
                    if zone_val is not None and (
                        zone_val not in _VALID_ZONE_VALUES
                    ):
                        raise CarbonIntensityProviderError(
                            f"history.zone={zone_val!r} is not a valid "
                            f"GridZone."
                        )
                    sample: CarbonIntensitySample = {
                        "value": fv,
                        "zone": zone_val or cfg.zone_for(fv).value,
                        "timestamp": float(entry.get("timestamp", 0.0)),
                    }
                    if entry.get("coerced"):
                        sample["coerced"] = True
                        if entry.get("raw_repr") is not None:
                            sample["raw_repr"] = str(entry["raw_repr"])
                    provider._history.append(sample)

            return provider
        except CarbonIntensityProviderError:
            raise
        except (TypeError, ValueError) as exc:
            raise CarbonIntensityProviderError(
                f"invalid CarbonIntensityProvider payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "CarbonIntensityProvider":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CarbonIntensityProviderError(
                f"Invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            zone = self._config.zone_for(self._current_intensity)
            return (
                "CarbonIntensityProvider("
                f"region={self.region!r}, "
                f"intensity={self._current_intensity:.1f}, "
                f"zone={zone.value}, "
                f"total={self._total_updates})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CarbonIntensityProvider):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonIntensityProvider",
    "CarbonIntensityProviderConfig",
    "CarbonIntensityProviderError",
    "CarbonIntensitySample",
    "CarbonIntensityStatistics",
    "GridZone",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.carbon_intensity_provider
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Happy path / zone classification ---------------------- #
    print("zones:")
    provider = CarbonIntensityProvider(region="US-CA")
    print("  repr:", provider)
    for value, expected_zone in (
        (100.0, "clean"),
        (200.0, "moderate"),   # boundary → moderate
        (250.0, "moderate"),
        (400.0, "dirty"),      # boundary → dirty
        (450.0, "dirty"),
        (600.0, "critical"),   # boundary → critical
        (700.0, "critical"),
    ):
        provider.update_manual(value)
        _check(
            f"{value:>6.1f} → {provider.current_zone.value}",
            provider.current_zone.value == expected_zone,
        )

    # ---- 2. Boundary semantics ----------------------------------- #
    print("boundary semantics:")
    p = CarbonIntensityProvider(
        config=CarbonIntensityProviderConfig(
            clean_threshold=200.0,
            dirty_threshold=400.0,
            critical_threshold=600.0,
        )
    )
    p.update_manual(400.0)
    _check("zone(400) == DIRTY", p.current_zone is GridZone.DIRTY)
    _check(
        "is_grid_dirty(400) == False (strict >)",
        p.is_grid_dirty() is False,
    )
    p.update_manual(400.001)
    _check(
        "is_grid_dirty(400.001) == True",
        p.is_grid_dirty() is True,
    )

    # ---- 3. NaN / inf / negative rejected (strict) --------------- #
    print("strict rejection:")
    strict_p = CarbonIntensityProvider()
    for bad in (
        float("nan"), float("inf"), float("-inf"),
        -1.0, "big", None, True,
    ):
        try:
            strict_p.update_manual(bad)
            _check(f"reject {bad!r}", False)
        except CarbonIntensityProviderError:
            _check(f"reject {bad!r}", True)

    # ---- 4. Non-strict coerce + mark ----------------------------- #
    print("non-strict coercion:")
    lenient = CarbonIntensityProvider(strict=False)
    lenient.update_manual(float("nan"))
    hist = lenient.history
    _check("coerced marked", hist[-1].get("coerced") is True)
    _check("raw_repr stored", "raw_repr" in hist[-1])
    _check("current == 0.0", lenient.get_current_intensity() == 0.0)

    # ---- 5. Config validation ------------------------------------ #
    print("config validation:")
    for bad_cfg in (
        dict(clean_threshold=0),
        dict(clean_threshold=float("nan")),
        dict(clean_threshold=400, dirty_threshold=200),
        dict(dirty_threshold=400, critical_threshold=200),
        dict(default_intensity=-1),
        dict(default_intensity=float("nan")),
        dict(default_intensity=1e7, max_intensity=1e6),  # > ceiling
        dict(max_intensity=0),
        dict(max_intensity=float("nan")),
        dict(max_intensity=float("inf")),
        dict(max_history=0),
        dict(max_history="100"),
    ):
        try:
            CarbonIntensityProviderConfig(**bad_cfg)  # type: ignore[arg-type]
            _check(f"reject cfg {list(bad_cfg)}", False)
        except CarbonIntensityProviderError:
            _check(f"reject cfg {list(bad_cfg)}", True)

    # ---- 6. zone_for rejects non-finite -------------------------- #
    print("zone_for rejects non-finite:")
    cfg = CarbonIntensityProviderConfig()
    for bad in (float("nan"), float("inf"), -1.0, "big"):
        try:
            cfg.zone_for(bad)  # type: ignore[arg-type]
            _check(f"zone_for({bad!r})", False)
        except CarbonIntensityProviderError:
            _check(f"zone_for({bad!r})", True)

    # ---- 7. is_grid_dirty / classify_zone input validation ------- #
    print("is_grid_dirty / classify_zone validation:")
    p = CarbonIntensityProvider()
    p.update_manual(300.0)
    try:
        p.is_grid_dirty("abc")  # type: ignore[arg-type]
        _check("is_grid_dirty('abc')", False)
    except CarbonIntensityProviderError:
        _check("is_grid_dirty('abc')", True)
    try:
        p.classify_zone("abc")  # type: ignore[arg-type]
        _check("classify_zone('abc')", False)
    except CarbonIntensityProviderError:
        _check("classify_zone('abc')", True)
    try:
        p.classify_zone(float("nan"))
        _check("classify_zone(nan)", False)
    except CarbonIntensityProviderError:
        _check("classify_zone(nan)", True)

    # ---- 8. Atomic statistics ------------------------------------ #
    print("statistics consistency:")
    p = CarbonIntensityProvider(region="EU")
    for v in (150.0, 250.0, 350.0):
        p.update_manual(v)
    stats = p.statistics()
    _check("current matches stats",
           stats["current_intensity"] == p.get_current_intensity())
    _check("zone matches stats",
           stats["current_zone"] == p.current_zone.value)
    _check("total_updates == 3", stats["total_updates"] == 3)

    # ---- 9. Round-trip with history (bug fix) -------------------- #
    print("round-trip with history:")
    payload = p.to_json(include_history=True)
    restored = CarbonIntensityProvider.from_json(payload)
    _check("current preserved",
           restored.get_current_intensity() == p.get_current_intensity())
    _check("history preserved",
           len(restored.history) == len(p.history))
    _check("region preserved", restored.region == "EU")
    _check("eq works", restored == p)

    # ---- 10. update_manual_batch -------------------------------- #
    print("batch update:")
    p = CarbonIntensityProvider()
    n = p.update_manual_batch([100.0, 200.0, 300.0])
    _check("accepted 3", n == 3)
    _check("total_updates == 3", p.total_updates == 3)
    # Strict failure mid-batch leaves state untouched.
    p2 = CarbonIntensityProvider()
    try:
        p2.update_manual_batch([100.0, float("nan")])
        _check("batch raises", False)
    except CarbonIntensityProviderError:
        _check("batch raises", True)
        _check("batch rollback", p2.total_updates == 0)

    # ---- 11. Counter invariant ----------------------------------- #
    print("counter invariant:")
    p = CarbonIntensityProvider(
        config=CarbonIntensityProviderConfig(max_history=3)
    )
    for i in range(5):
        p.update_manual(float(i * 100))
    stats = p.statistics()
    _check("total_updates == 5", stats["total_updates"] == 5)
    _check("dropped_updates == 2", stats["dropped_updates"] == 2)
    _check(
        "invariant total == buffered + dropped",
        stats["total_updates"]
        == stats["updates"] + stats["dropped_updates"],
    )

    # ---- 12. from_dict error wrapping ---------------------------- #
    print("from_dict errors:")
    try:
        CarbonIntensityProvider.from_dict(
            {"current_intensity": float("nan")}
        )
        _check("nan current_intensity rejected", False)
    except CarbonIntensityProviderError:
        _check("nan current_intensity rejected", True)

    try:
        CarbonIntensityProvider.from_dict(
            {"current_intensity": 1e9,
             "config": {"max_intensity": 1e6}}
        )
        _check("out-of-ceiling current rejected", False)
    except CarbonIntensityProviderError:
        _check("out-of-ceiling current rejected", True)

    try:
        CarbonIntensityProvider.from_dict(
            {"history": [{"value": float("nan")}]}
        )
        _check("nan history rejected", False)
    except CarbonIntensityProviderError:
        _check("nan history rejected", True)

    try:
        CarbonIntensityProvider.from_dict(
            {"history": [{"value": 100, "zone": "bogus_zone"}]}
        )
        _check("bogus zone rejected", False)
    except CarbonIntensityProviderError:
        _check("bogus zone rejected", True)

    try:
        CarbonIntensityProvider.from_dict(
            {"started_at": "not-a-number"}
        )
        _check("bad started_at rejected", False)
    except CarbonIntensityProviderError:
        _check("bad started_at rejected", True)

    # ---- 13. Config from_dict strict ----------------------------- #
    print("config from_dict strict:")
    try:
        CarbonIntensityProviderConfig.from_dict(
            {"typo": 1}, strict=True
        )
        _check("unknown key rejected", False)
    except CarbonIntensityProviderError:
        _check("unknown key rejected", True)
    cfg = CarbonIntensityProviderConfig.from_dict(
        {"typo": 1}, strict=False
    )
    _check("unknown key accepted non-strict",
           cfg.clean_threshold == 200.0)

    # ---- 14. StrEnum formatting ---------------------------------- #
    print("StrEnum formatting:")
    _check("str(CLEAN) == 'clean'", str(GridZone.CLEAN) == "clean")
    _check("str(CRITICAL) == 'critical'",
           str(GridZone.CRITICAL) == "critical")
    _check("f-string", f"{GridZone.MODERATE}" == "moderate")

    # ---- 15. default MODERATE starting state --------------------- #
    print("default state:")
    p = CarbonIntensityProvider()
    _check("default is MODERATE",
           p.current_zone is GridZone.MODERATE)
    _check("default intensity == 385",
           p.get_current_intensity() == 385.0)

    # ---- 16. __eq__ / __hash__ ----------------------------------- #
    print("eq/hash:")
    a = CarbonIntensityProvider(
        config=CarbonIntensityProviderConfig(clean_threshold=100.0)
    )
    b = CarbonIntensityProvider(
        config=CarbonIntensityProviderConfig(clean_threshold=100.0)
    )
    _check("equal configs", a == b)
    _check("hash equal", hash(a) == hash(b))
    c = CarbonIntensityProvider(
        config=CarbonIntensityProviderConfig(clean_threshold=150.0)
    )
    _check("different configs", a != c)

    # ---- 17. to_dict(include_history) defensive copy ------------- #
    print("defensive copy:")
    p = CarbonIntensityProvider()
    p.update_manual(100.0)
    snapshot = p.to_dict(include_history=True)
    snapshot["history"][0]["value"] = 99999.0
    _check(
        "internal history unchanged after external mutation",
        p.history[0]["value"] == 100.0,
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        import sys
        sys.exit(1)
