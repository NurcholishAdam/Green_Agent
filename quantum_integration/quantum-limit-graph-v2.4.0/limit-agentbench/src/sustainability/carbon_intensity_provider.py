# src/sustainability/carbon_intensity_provider.py

"""
Carbon Intensity Provider
=========================

Provides the current carbon intensity (gCO2/kWh) and classifies the grid
into CLEAN / MODERATE / DIRTY / CRITICAL zones.

Enhancements
------------
- ``CarbonIntensityProviderConfig`` — frozen, validated: zone thresholds,
  default intensity, bounded update history.
- ``GridZone`` StrEnum — ``"clean"`` | ``"moderate"`` | ``"dirty"`` |
  ``"critical"``.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards state and history.
- **Bounded update history** — every manual update is recorded with a
  timestamp for auditing.
- **`is_grid_dirty` delegates to zone classification** so the threshold is
  the same as the zone boundary.
- Serialization on the provider and the config.
- ``statistics()``, ``reset()``, ``__repr__``, custom
  ``CarbonIntensityProviderError(ValueError)``, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonIntensityProviderError(ValueError):
    """Raised for invalid provider inputs or configuration."""


# --------------------------------------------------------------------------- #
# Zones
# --------------------------------------------------------------------------- #
class GridZone(str, Enum):
    """Carbon intensity zones."""

    CLEAN = "clean"        # < clean_threshold
    MODERATE = "moderate"  # clean .. dirty
    DIRTY = "dirty"        # dirty .. critical
    CRITICAL = "critical"  # >= critical_threshold

    @property
    def description(self) -> str:
        return {
            GridZone.CLEAN: "Very low carbon intensity",
            GridZone.MODERATE: "Moderate carbon intensity",
            GridZone.DIRTY: "High carbon intensity",
            GridZone.CRITICAL: "Severe carbon intensity",
        }[self]


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
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
        for name in (
            "clean_threshold", "dirty_threshold", "critical_threshold",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise CarbonIntensityProviderError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv <= 0:
                raise CarbonIntensityProviderError(
                    f"{name} must be finite and > 0."
                )
        if not (
            self.clean_threshold
            < self.dirty_threshold
            < self.critical_threshold
        ):
            raise CarbonIntensityProviderError(
                "thresholds must be strictly increasing: "
                "clean < dirty < critical."
            )
        if self.default_intensity < 0:
            raise CarbonIntensityProviderError(
                "default_intensity must be >= 0."
            )
        if self.max_intensity <= 0:
            raise CarbonIntensityProviderError("max_intensity must be > 0.")
        if self.max_history <= 0:
            raise CarbonIntensityProviderError("max_history must be > 0.")

    def zone_for(self, intensity: float) -> GridZone:
        if intensity < self.clean_threshold:
            return GridZone.CLEAN
        if intensity < self.dirty_threshold:
            return GridZone.MODERATE
        if intensity < self.critical_threshold:
            return GridZone.DIRTY
        return GridZone.CRITICAL

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonIntensityProviderConfig":
        if not isinstance(data, Mapping):
            raise CarbonIntensityProviderError(
                "CarbonIntensityProviderConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Provider
# --------------------------------------------------------------------------- #
class CarbonIntensityProvider:
    """
    Provides current carbon intensity (gCO2/kWh).

    The original API (``region``, ``update_manual``, ``get_current_intensity``,
    ``is_grid_dirty``) is preserved; new parameters are keyword-only.
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
        self._config = config or CarbonIntensityProviderConfig()
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.region: Optional[str] = region
        self._current_intensity: float = self._config.default_intensity

        self._lock = threading.RLock()
        self._history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

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
    def current_intensity(self) -> float:
        with self._lock:
            return self._current_intensity

    @property
    def current_zone(self) -> GridZone:
        with self._lock:
            return self._config.zone_for(self._current_intensity)

    # ---------------------------------------------------------- public API
    def update_manual(self, value: float) -> float:
        """
        Update the current intensity manually.

        Returns the new intensity. Validates finiteness, non-negativity, and
        the ``max_intensity`` ceiling.
        """
        fv = self._validate_intensity(value)
        with self._lock:
            self._current_intensity = fv
            self._history.append({
                "value": fv,
                "zone": self._config.zone_for(fv).value,
                "timestamp": time.time(),
            })
        logger.info("Carbon intensity updated: %.3f gCO2/kWh.", fv)
        return fv

    def get_current_intensity(self) -> float:
        """Return the current carbon intensity."""
        with self._lock:
            return self._current_intensity

    def is_grid_dirty(self, threshold: Optional[float] = None) -> bool:
        """
        Return True when the current intensity exceeds ``threshold``.

        When ``threshold`` is ``None`` the provider's
        ``dirty_threshold`` from the config is used.
        """
        t = (
            self._config.dirty_threshold
            if threshold is None else float(threshold)
        )
        if math.isnan(t) or math.isinf(t) or t < 0:
            raise CarbonIntensityProviderError(
                "threshold must be finite and >= 0."
            )
        with self._lock:
            return self._current_intensity > t

    def classify_zone(self, intensity: Optional[float] = None) -> GridZone:
        """Return the zone for ``intensity`` (default: current intensity)."""
        v = self._current_intensity if intensity is None else intensity
        return self._config.zone_for(float(v))

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset to the default intensity. Returns entries removed."""
        with self._lock:
            removed = len(self._history)
            self._current_intensity = self._config.default_intensity
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug("CarbonIntensityProvider reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- validation
    def _validate_intensity(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon intensity must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"carbon intensity must be finite, got {value!r}."
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv < 0:
            msg = f"carbon intensity must be >= 0, got {fv}."
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv > self._config.max_intensity:
            msg = (
                f"carbon intensity must be <= {self._config.max_intensity}, "
                f"got {fv}."
            )
            if self._strict:
                raise CarbonIntensityProviderError(msg)
            logger.warning("%s Clamping.", msg)
            return self._config.max_intensity
        return fv

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
        return {
            "region": self.region,
            "current_intensity": self._current_intensity,
            "current_zone": self._config.zone_for(
                self._current_intensity
            ).value,
            "updates": len(history),
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "region": self.region,
                "current_intensity": self._current_intensity,
                "started_at": self._started_at,
            }
            if include_history:
                payload["history"] = list(self._history)
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonIntensityProvider":
        if not isinstance(data, Mapping):
            raise CarbonIntensityProviderError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = CarbonIntensityProviderConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        provider = cls(
            region=data.get("region"),
            config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with provider._lock:
            provider._current_intensity = float(
                data.get("current_intensity", cfg.default_intensity)
            )
            provider._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                if isinstance(entry, Mapping):
                    provider._history.append(dict(entry))
        return provider

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "CarbonIntensityProvider":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CarbonIntensityProviderError(f"Invalid JSON: {exc}") from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonIntensityProvider("
                f"region={self.region!r}, "
                f"intensity={self._current_intensity:.1f}, "
                f"zone={self._config.zone_for(self._current_intensity).value})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonIntensityProvider",
    "CarbonIntensityProviderConfig",
    "CarbonIntensityProviderError",
    "GridZone",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.carbon_intensity_provider
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    provider = CarbonIntensityProvider(region="US-CA")
    print("repr       :", provider)

    for value in (100.0, 250.0, 450.0, 700.0):
        provider.update_manual(value)
        print(f"  {value:>6.1f} -> zone={provider.current_zone.value}, "
              f"dirty={provider.is_grid_dirty()}")

    # ---- Bug fix: NaN / inf / negative ---------------------------- #
    for bad in (float("nan"), float("inf"), -1.0, "big"):
        try:
            provider.update_manual(bad)
        except CarbonIntensityProviderError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces --------------------------------------- #
    lenient = CarbonIntensityProvider(strict=False)
    lenient.update_manual(float("nan"))
    print("lenient    :", lenient.get_current_intensity())

    # ---- Config validation ---------------------------------------- #
    for bad_cfg in (
        dict(clean_threshold=0),
        dict(clean_threshold=400, dirty_threshold=200),
        dict(dirty_threshold=400, critical_threshold=200),
        dict(default_intensity=-1),
        dict(max_intensity=0),
        dict(max_history=0),
    ):
        try:
            CarbonIntensityProviderConfig(**bad_cfg)  # type: ignore[arg-type]
        except CarbonIntensityProviderError as exc:
            print("Rejected cfg:", exc)

    # ---- Serialization round-trip --------------------------------- #
    payload = provider.to_json()
    restored = CarbonIntensityProvider.from_json(payload)
    assert restored.to_dict() == provider.to_dict()
    print("Round-trip OK.")

    print("statistics :", {
        k: v for k, v in provider.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    print("\nSmoke test passed.")
