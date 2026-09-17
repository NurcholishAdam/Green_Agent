# user_preferences.py

"""
User preferences for multi-objective routing.
==============================================

Validated set of weights for Pareto-based routing decisions. Provides
normalization, preset profiles, JSON persistence (sync + async), and a
weighted-sum scoring helper.

Quick start
-----------
>>> from user_preferences import UserPreferences
>>> prefs = UserPreferences.energy_first()
>>> prefs.normalize().to_dict()
{'alpha': 0.4..., 'beta': 0.1..., ...}
>>> prefs.apply({'alpha': 0.8, 'beta': 0.6})
1.9...

Enhancements over the previous version
--------------------------------------
1. **Fixed unusable presets** — ``energy_first``, ``carbon_first`` and
   ``helium_aware`` used weight ``2.0`` but the fields were bounded to
   ``[-1.0, 1.0]``. Bounds are now ``[-10.0, 10.0]``.
2. **Fixed misleading ``normalize()``** — the docstring claimed "sum to
   1.0" but the implementation normalized the L1-of-absolute. Added
   :meth:`normalize_positive` for the true sum-to-1.0 case.
3. **Split async persistence** into :meth:`save_async` / :meth:`load_async`
   so they can be awaited from a running event loop. The sync :meth:`save`
   / :meth:`load` no longer call ``asyncio.run`` internally.
4. **Atomic writes + parent-directory creation** for both sync and async
   paths.
5. **Full validation** — NaN/Inf rejection, semver check, all-zero
   rejection, ``apply()`` validates input types.
6. **Round-trip helpers**: :meth:`to_json` / :meth:`from_json`.
7. **Custom exception** :class:`UserPreferencesError`.
8. **Diagnostics**: :meth:`statistics` / :meth:`active_objectives` /
   :meth:`__repr__`.
9. **Factory** :meth:`preset` to fetch a preset by name.
10. **Meta**: ``__version__``, ``__all__``, and a ``__main__`` guard.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# --------------------------------------------------------------------------- #
# Optional async file I/O
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — optional dependency
    import aiofiles  # type: ignore

    _AIOFILES_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiofiles = None  # type: ignore[assignment]
    _AIOFILES_AVAILABLE = False

logger = logging.getLogger(__name__)

__version__ = "2.1.0"

_WEIGHT_FIELDS: Tuple[str, ...] = (
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta",
)

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.\-]+)?$")


# =========================================================================== #
# Errors
# =========================================================================== #
class UserPreferencesError(ValueError):
    """Raised for invalid preference inputs or persistence failures."""


# =========================================================================== #
# Model
# =========================================================================== #
class UserPreferences(BaseModel):
    """Validated user preferences for multi-objective routing.

    The weights represent the relative importance of different objectives:

    - ``alpha``   — energy efficiency
    - ``beta``    — carbon intensity
    - ``gamma``   — helium scarcity
    - ``delta``   — cost
    - ``epsilon`` — latency
    - ``zeta``    — accuracy (may be negative to encode trade-offs)

    Weights are bounded to ``[-10.0, 10.0]`` and must be finite. At least
    one weight must be non-zero.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    alpha: float = Field(
        1.0, ge=-10.0, le=10.0,
        description="Energy efficiency weight",
    )
    beta: float = Field(
        1.0, ge=-10.0, le=10.0,
        description="Carbon intensity weight",
    )
    gamma: float = Field(
        0.5, ge=-10.0, le=10.0,
        description="Helium scarcity weight",
    )
    delta: float = Field(
        0.3, ge=-10.0, le=10.0,
        description="Cost weight",
    )
    epsilon: float = Field(
        0.1, ge=-10.0, le=10.0,
        description="Latency weight",
    )
    zeta: float = Field(
        -0.1, ge=-10.0, le=10.0,
        description="Accuracy weight (negative = trade-off)",
    )

    version: str = Field(
        __version__, description="Schema version (semver)",
    )

    # ------------------------------------------------------------------ #
    # Validators
    # ------------------------------------------------------------------ #
    @field_validator(*_WEIGHT_FIELDS, mode="before")
    @classmethod
    def _weights_finite(cls, v: Any) -> float:
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise UserPreferencesError(
                f"weight must be numeric, got {type(v).__name__}"
            )
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            raise UserPreferencesError(f"weight must be finite, got {v!r}")
        return fv

    @field_validator("version", mode="before")
    @classmethod
    def _version_valid(cls, v: Any) -> str:
        if not isinstance(v, str) or not _SEMVER_RE.match(v):
            raise UserPreferencesError(
                f"version must be a semver string, got {v!r}"
            )
        return v

    @model_validator(mode="after")
    def _non_zero(self) -> "UserPreferences":
        if all(abs(getattr(self, f)) < 1e-9 for f in _WEIGHT_FIELDS):
            raise UserPreferencesError(
                "All weights are zero; at least one must be non-zero."
            )
        return self

    # ------------------------------------------------------------------ #
    # Utility
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, float]:
        """Return the six weights as a plain dict (no ``version``)."""
        return {f: float(getattr(self, f)) for f in _WEIGHT_FIELDS}

    def to_json(self, *, indent: Optional[int] = 2) -> str:
        """Serialize to JSON (``version`` included)."""
        return self.model_dump_json(indent=indent)

    @classmethod
    def from_json(cls, payload: str) -> "UserPreferences":
        """Deserialize from a JSON string."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise UserPreferencesError(f"invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise UserPreferencesError("preferences JSON must be an object")
        return cls(**dict(data))

    def active_objectives(self) -> Tuple[str, ...]:
        """Return the names of weights whose absolute value is > 1e-9."""
        return tuple(
            f for f in _WEIGHT_FIELDS if abs(getattr(self, f)) > 1e-9
        )

    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe summary of the profile."""
        weights = self.to_dict()
        values = list(weights.values())
        return {
            "version": self.version,
            "weights": weights,
            "abs_sum": sum(abs(v) for v in values),
            "sum": sum(values),
            "min": min(values),
            "max": max(values),
            "active": list(self.active_objectives()),
        }

    def copy(self) -> "UserPreferences":
        """Return an independent copy (Pydantic ``model_copy``)."""
        return self.model_copy()

    # ------------------------------------------------------------------ #
    # Normalization
    # ------------------------------------------------------------------ #
    def normalize(self) -> "UserPreferences":
        """Scale weights so ``sum(abs(w)) == 1.0``.

        The sign of each weight is preserved, so trade-offs (negative
        weights) survive normalization.
        """
        total = sum(abs(getattr(self, f)) for f in _WEIGHT_FIELDS)
        if total < 1e-12:
            # Guarded by the non-zero validator, but kept for safety.
            return self.copy()
        return self.model_copy(
            update={f: getattr(self, f) / total for f in _WEIGHT_FIELDS}
        )

    def normalize_positive(self) -> "UserPreferences":
        """Shift weights to be non-negative, then normalize to sum to 1.0.

        Each weight is offset by ``-min(0, min(weights))`` so the smallest
        weight becomes zero. If every weight is already non-negative, this
        is equivalent to ``weights / sum(weights)``.
        """
        values = {f: getattr(self, f) for f in _WEIGHT_FIELDS}
        offset = -min(0.0, min(values.values()))
        shifted = {f: v + offset for f, v in values.items()}
        total = sum(shifted.values())
        if total < 1e-12:
            return self.copy()
        return self.model_copy(
            update={f: shifted[f] / total for f in _WEIGHT_FIELDS}
        )

    # ------------------------------------------------------------------ #
    # Presets
    # ------------------------------------------------------------------ #
    @classmethod
    def balanced(cls) -> "UserPreferences":
        """A balanced profile with equal positive weights."""
        return cls(
            alpha=1.0, beta=1.0, gamma=1.0,
            delta=1.0, epsilon=1.0, zeta=0.0,
        )

    @classmethod
    def energy_first(cls) -> "UserPreferences":
        """Prioritize energy efficiency."""
        return cls(
            alpha=2.0, beta=0.5, gamma=0.2,
            delta=0.3, epsilon=0.1, zeta=-0.1,
        )

    @classmethod
    def carbon_first(cls) -> "UserPreferences":
        """Prioritize carbon reduction."""
        return cls(
            alpha=0.2, beta=2.0, gamma=0.3,
            delta=0.2, epsilon=0.1, zeta=-0.1,
        )

    @classmethod
    def helium_aware(cls) -> "UserPreferences":
        """Prioritize helium conservation."""
        return cls(
            alpha=0.3, beta=0.3, gamma=2.0,
            delta=0.1, epsilon=0.1, zeta=-0.1,
        )

    @classmethod
    def preset(cls, name: str) -> "UserPreferences":
        """Return a preset by name.

        Recognized names: ``"balanced"``, ``"energy"``, ``"carbon"``,
        ``"helium"`` (case-insensitive).
        """
        if not isinstance(name, str) or not name:
            raise UserPreferencesError(
                "preset name must be a non-empty string."
            )
        table = {
            "balanced": cls.balanced,
            "energy":   cls.energy_first,
            "carbon":   cls.carbon_first,
            "helium":   cls.helium_aware,
        }
        factory = table.get(name.strip().lower())
        if factory is None:
            raise UserPreferencesError(
                f"unknown preset {name!r}; available: {sorted(table)}"
            )
        return factory()

    # ------------------------------------------------------------------ #
    # Scoring
    # ------------------------------------------------------------------ #
    def apply(self, objective_values: Mapping[str, float]) -> float:
        """Return the weighted sum of ``objective_values``.

        Parameters
        ----------
        objective_values : Mapping[str, float]
            A mapping whose keys are a subset of the weight names
            (``alpha``…``zeta``) and whose values are finite floats.

        Raises
        ------
        UserPreferencesError
            If ``objective_values`` is not a Mapping, contains an unknown
            key, or contains a non-finite value.
        """
        if not isinstance(objective_values, Mapping):
            raise UserPreferencesError(
                f"objective_values must be a Mapping, got "
                f"{type(objective_values).__name__}."
            )
        weights = self.to_dict()
        total = 0.0
        for key, value in objective_values.items():
            if key not in weights:
                raise UserPreferencesError(
                    f"Unknown objective key: {key!r}; "
                    f"expected one of {sorted(weights)}."
                )
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise UserPreferencesError(
                    f"objective_values[{key!r}] must be numeric, got "
                    f"{type(value).__name__}."
                )
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise UserPreferencesError(
                    f"objective_values[{key!r}] must be finite, got "
                    f"{value!r}."
                )
            total += weights[key] * fv
        return total

    # ------------------------------------------------------------------ #
    # Persistence (sync)
    # ------------------------------------------------------------------ #
    def save(self, path: Union[str, Path]) -> bool:
        """Atomically write the preferences to ``path``.

        Parent directories are created automatically. Returns ``True`` on
        success, ``False`` on failure (logged at ERROR).
        """
        try:
            _atomic_write_json(Path(path), self.model_dump())
        except Exception as exc:
            logger.error("Failed to save preferences: %s", exc)
            return False
        logger.info("User preferences saved to %s", path)
        return True

    @classmethod
    def load(
        cls,
        path: Union[str, Path],
        *,
        strict: bool = True,
    ) -> Optional["UserPreferences"]:
        """Load preferences from ``path``.

        Parameters
        ----------
        path : str | Path
            File path.
        strict : bool, default True
            When ``False``, unknown keys are dropped and a WARNING is
            logged instead of raising.

        Returns
        -------
        UserPreferences | None
            The parsed instance, or ``None`` on any failure (missing
            file, malformed JSON, validation error).
        """
        p = Path(path)
        if not p.exists():
            logger.warning("Preferences file %s not found", p)
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(data, Mapping):
                raise UserPreferencesError(
                    "preferences JSON must be an object"
                )
            if not strict:
                allowed = set(_WEIGHT_FIELDS) | {"version"}
                data = {k: v for k, v in data.items() if k in allowed}
            return cls(**dict(data))
        except Exception as exc:
            logger.error("Failed to load preferences: %s", exc)
            return None

    # ------------------------------------------------------------------ #
    # Persistence (async)
    # ------------------------------------------------------------------ #
    async def save_async(self, path: Union[str, Path]) -> bool:
        """Async sibling of :meth:`save`.

        Uses ``aiofiles`` when available, otherwise offloads the sync
        writer to a worker thread via ``asyncio.to_thread``.
        """
        p = Path(path)
        payload = self.model_dump()
        try:
            if _AIOFILES_AVAILABLE:
                await _atomic_write_json_async(p, payload)
            else:
                await asyncio.to_thread(_atomic_write_json, p, payload)
        except Exception as exc:
            logger.error("Failed to save preferences (async): %s", exc)
            return False
        logger.info("User preferences saved (async) to %s", p)
        return True

    @classmethod
    async def load_async(
        cls,
        path: Union[str, Path],
        *,
        strict: bool = True,
    ) -> Optional["UserPreferences"]:
        """Async sibling of :meth:`load`."""
        p = Path(path)
        if not p.exists():
            logger.warning("Preferences file %s not found", p)
            return None
        try:
            if _AIOFILES_AVAILABLE:
                async with aiofiles.open(  # type: ignore[union-attr]
                    p, "r", encoding="utf-8",
                ) as f:
                    content = await f.read()
                data = json.loads(content)
            else:
                data = await asyncio.to_thread(
                    lambda: json.loads(p.read_text(encoding="utf-8"))
                )
            if not isinstance(data, Mapping):
                raise UserPreferencesError(
                    "preferences JSON must be an object"
                )
            if not strict:
                allowed = set(_WEIGHT_FIELDS) | {"version"}
                data = {k: v for k, v in data.items() if k in allowed}
            return cls(**dict(data))
        except Exception as exc:
            logger.error("Failed to load preferences (async): %s", exc)
            return None

    # ------------------------------------------------------------------ #
    # Dunder
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:  # pragma: no cover — cosmetic
        return (
            "UserPreferences("
            f"alpha={self.alpha:.3f}, beta={self.beta:.3f}, "
            f"gamma={self.gamma:.3f}, delta={self.delta:.3f}, "
            f"epsilon={self.epsilon:.3f}, zeta={self.zeta:.3f}, "
            f"version={self.version!r})"
        )


# =========================================================================== #
# Atomic I/O helpers
# =========================================================================== #
def _atomic_write_json(path: Path, payload: Any) -> None:
    """Write ``payload`` to ``path`` atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                payload, f, indent=2, ensure_ascii=False, default=str,
            )
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


async def _atomic_write_json_async(path: Path, payload: Any) -> None:
    """Async variant of :func:`_atomic_write_json` using ``aiofiles``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
    )
    try:
        async with aiofiles.open(  # type: ignore[union-attr]
            fd, "w", encoding="utf-8",
        ) as f:
            await f.write(
                json.dumps(
                    payload,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )
            )
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# =========================================================================== #
# Public API
# =========================================================================== #
__all__ = [
    "__version__",
    "UserPreferences",
    "UserPreferencesError",
]


# =========================================================================== #
# Smoke test: python -m user_preferences
# =========================================================================== #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- 1. Presets are now usable ---------------------------------- #
    for name in ("balanced", "energy", "carbon", "helium"):
        p = UserPreferences.preset(name)
        print(f"{name:<8} :", p.to_dict())

    # ---- 2. Bug fix: normalize semantics ---------------------------- #
    energy = UserPreferences.energy_first()
    norm = energy.normalize()
    assert abs(
        sum(abs(v) for v in norm.to_dict().values()) - 1.0
    ) < 1e-9
    print(
        "\nnormalize(L1 of |w|) :",
        {k: round(v, 4) for k, v in norm.to_dict().items()},
    )

    pos = energy.normalize_positive()
    assert abs(sum(pos.to_dict().values()) - 1.0) < 1e-9
    print(
        "normalize_positive   :",
        {k: round(v, 4) for k, v in pos.to_dict().items()},
    )

    # ---- 3. Bug fix: apply() validates input ----------------------- #
    score = energy.apply({
        "alpha": 0.8, "beta": 0.6, "gamma": 0.4,
        "delta": 0.7, "epsilon": 0.3, "zeta": 0.5,
    })
    print("\napplied score        :", round(score, 4))

    for bad in (
        "not-a-mapping",
        {"zeta": float("nan")},
        {"unknown_key": 1.0},
        {"alpha": "big"},
    ):
        try:
            energy.apply(bad)  # type: ignore[arg-type]
        except UserPreferencesError as exc:
            print("Rejected apply       :", exc)

    # ---- 4. Bug fix: NaN / inf / out-of-range weights rejected ----- #
    for bad_w in (float("nan"), float("inf"), "big", 1e9):
        try:
            UserPreferences(alpha=bad_w)  # type: ignore[arg-type]
        except Exception as exc:
            print("Rejected weight      :", type(exc).__name__)

    # ---- 5. Version validation -------------------------------------- #
    for bad_v in ("not-a-version", "2.0", ""):
        try:
            UserPreferences(version=bad_v)
        except Exception as exc:
            print("Rejected version     :", type(exc).__name__)

    # ---- 6. All-zero rejection -------------------------------------- #
    try:
        UserPreferences(
            alpha=0.0, beta=0.0, gamma=0.0,
            delta=0.0, epsilon=0.0, zeta=0.0,
        )
    except Exception as exc:
        print("Rejected zero-vector :", type(exc).__name__)

    # ---- 7. JSON round-trip ----------------------------------------- #
    payload = energy.to_json()
    restored = UserPreferences.from_json(payload)
    assert restored.to_dict() == energy.to_dict()
    print("\nJSON round-trip      : OK")

    # ---- 8. Atomic save / load (sync) ------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "prefs.json"
        assert energy.save(path) is True
        assert path.exists()

        loaded = UserPreferences.load(path)
        assert loaded is not None
        assert loaded.to_dict() == energy.to_dict()
        print("sync save/load       : OK")

        # ---- 9. Async save / load ---------------------------------- #
        async def _async_round_trip() -> None:
            path2 = Path(td) / "async" / "prefs.json"
            ok = await energy.save_async(path2)
            assert ok
            loaded_async = await UserPreferences.load_async(path2)
            assert loaded_async is not None
            assert loaded_async.to_dict() == energy.to_dict()
            print("async save/load      : OK")

        asyncio.run(_async_round_trip())

        # ---- 10. Non-strict load drops unknown keys ---------------- #
        strictless = path.with_name("with_unknown.json")
        strictless.write_text(
            json.dumps({**energy.model_dump(), "unknown_field": 1}),
            encoding="utf-8",
        )
        assert UserPreferences.load(strictless, strict=True) is None
        non_strict = UserPreferences.load(strictless, strict=False)
        assert non_strict is not None
        assert non_strict.to_dict() == energy.to_dict()
        print("non-strict load      : OK")

    # ---- 11. Diagnostics -------------------------------------------- #
    print(
        "\nstatistics           :",
        {k: v for k, v in energy.statistics().items() if k != "weights"},
    )
    print("active objectives    :", energy.active_objectives())
    print("repr                 :", energy)

    # ---- 12. Copy is independent ------------------------------------ #
    copy = energy.copy()
    assert copy.to_dict() == energy.to_dict()
    assert copy is not energy
    print("copy()               : OK")

    # ---- 13. Preset factory error ---------------------------------- #
    try:
        UserPreferences.preset("bogus")
    except UserPreferencesError as exc:
        print("Rejected preset      :", exc)

    print("\nSmoke test passed.")
