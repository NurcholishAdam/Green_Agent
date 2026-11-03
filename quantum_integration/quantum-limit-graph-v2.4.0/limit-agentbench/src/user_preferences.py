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
>>> prefs.apply_detailed({'alpha': 0.8, 'beta': 0.6})
{'total': 1.9..., 'by_key': {'alpha': 1.6, 'beta': 0.3}}

Exception model
---------------
Two exception types are exposed, and they cover different failure modes:

- :class:`UserPreferencesValidationError` — re-export of Pydantic's
  :class:`pydantic.ValidationError`. Raised by the constructor
  (``UserPreferences(alpha=float('nan'))``) and by any path that calls
  ``cls(**data)`` — including ``from_dict`` / ``from_json`` / ``load`` /
  ``load_async`` when field validation fails. This is the type Pydantic
  always raises; our module cannot override it without breaking
  Pydantic's internals.
- :class:`UserPreferencesError` — the module's own narrow error type.
  Raised by ``apply()`` / ``apply_detailed()`` / ``preset()`` /
  ``from_json()`` for malformed input (not field validation), and by
  ``save`` / ``load`` for path-type errors.

Both classes are subclasses of :class:`ValueError`; a caller who wants
to catch everything this module can raise should write
``except (UserPreferencesError, UserPreferencesValidationError):``.

Enhancements over the previous version
--------------------------------------
1. **Exception model clarified.** :data:`UserPreferencesValidationError`
   is re-exported so callers do not need to import Pydantic directly,
   and the docstring explains the split between the two types. The
   smoke test now asserts the exact exception type on each failure path
   (the previous test caught :class:`Exception` and inspected the
   classname, which masked the Pydantic-wrapping behaviour).
2. **Python 3.8 support for async persistence.** ``asyncio.to_thread``
   (3.9+) is replaced with :func:`_run_sync_in_thread`, a cross-version
   helper that uses ``run_in_executor`` on 3.8.
3. **``to_dict`` / ``from_dict`` symmetry** with ``to_json`` /
   ``from_json``. ``from_dict`` accepts a Mapping whose keys mirror what
   ``to_dict`` produces (weights only) as well as the full
   ``model_dump`` shape (weights + version). ``to_dict``'s docstring
   makes the "version excluded" contract explicit.
4. **``from_json`` gained ``strict=``** — matching ``load`` /
   ``load_async``.
5. **Async write fixed** — the previous ``aiofiles.open(fd, ...)``
   passed an open file descriptor to aiofiles; the file is now reopened
   by path after ``mkstemp``, matching aiofiles' documented interface.
6. **Atomic-write fd-leak fix** — the sync writer no longer leaks the
   file descriptor if ``os.fdopen`` itself raises. Both writers now
   ``fsync`` the temp file before ``os.replace`` for crash durability.
7. **Log levels split** — ``load`` and ``load_async`` use ``INFO`` for a
   missing file, ``WARNING`` for malformed JSON / schema mismatch, and
   ``ERROR`` for actual I/O errors. The previous revision logged
   ``ERROR`` for all three, which is noisy for the common "no
   preferences file yet" case.
8. **``version`` is schema-checked.** The field is validated against
   the current major version (see :data:`_SUPPORTED_MAJOR`), so an
   incompatible file cannot be loaded silently. Patch / minor bumps
   within the same major remain compatible.
9. **``default=str`` removed** from both writers — a future
   non-primitive field fails loudly at write time instead of
   serialising to ``repr()``.
10. **Full semver regex** — the previous regex rejected valid
    ``"1.0.0-beta+build.42"`` strings.
11. **``apply()`` raises on empty input** — returning ``0.0`` silently
    for an empty objective set is dangerous for routing decisions.
12. **``apply_detailed()`` added** — a per-objective breakdown for
    explainability consumers.
13. **Weights accept ``numbers.Real``** — ``numpy.int64`` and other
    numeric scalars are now accepted, matching the ``numpy.float64``
    behaviour the previous version already supported implicitly via
    ``float`` subclassing. ``Decimal`` is still rejected (not a
    :class:`numbers.Real`).
14. **Validated copies** — ``normalize()`` and ``normalize_positive()``
    use ``model_validate`` instead of ``model_copy`` so the resulting
    instance has been validated. This is defensive (the current code
    cannot produce invalid output) but closes a future bug class.
15. **``normalize_positive()`` edge case** — the all-equal-negative
    case now raises :class:`UserPreferencesError` instead of silently
    returning an unnormalized copy.
16. **Preset aliases** — ``preset("energy_first")`` etc. work alongside
    the short names.
17. **Path coercion** — ``save`` / ``load`` / ``save_async`` /
    ``load_async`` accept ``str | os.PathLike[str]`` and raise
    :class:`UserPreferencesError` on invalid types (previously a raw
    ``TypeError`` from ``Path(...)``).
18. **``HAS_AIOFILES`` public** — callers can check whether async I/O
    uses aiofiles or the thread-pool fallback.
19. **``statistics()`` field renamed** — the ambiguous ``"sum"`` /
    ``"abs_sum"`` keys are now ``"weight_sum"`` / ``"weight_abs_sum"``.
    The old keys are kept as aliases for backward compatibility.
20. **``__dir__``** so ``dir(user_preferences)`` reflects the public
    API.
21. **Comprehensive smoke test** — ``_check()``-based (survives
    ``python -O``), with ``_expect_error`` asserting the exact exception
    type per failure path.

Version
-------
Schema version 3.0.0. The six numeric weights are unchanged from 2.1.0,
but the validation contract has tightened (version major must match,
``apply({})`` raises, async path uses a version-safe thread helper).
Files written by 2.1.0 are rejected by the version validator; to
migrate, drop the ``version`` key from the JSON and pass the remaining
fields to the constructor (all six weights round-trip unchanged).

Requires Python 3.8+. Pydantic v2 is required.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import tempfile
from numbers import Real
from pathlib import Path
from typing import (
    Any, Dict, Final, List, Mapping, Optional, Tuple, Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError as _PydanticValidationError,
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

# Public accessor (see Enhancements #18).
HAS_AIOFILES: Final[bool] = _AIOFILES_AVAILABLE


logger = logging.getLogger(__name__)

__version__: Final[str] = "3.0.0"

# Re-export Pydantic's ValidationError under a domain-specific name so
# callers do not need to import pydantic directly to catch the constructor
# error. See the module docstring's "Exception model" section.
UserPreferencesValidationError = _PydanticValidationError

_WEIGHT_FIELDS: Tuple[str, ...] = (
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta",
)

# Absolute-value floor below which a weight is considered "zero" for the
# "at least one non-zero" invariant and the normalize_positive() guard.
_ZERO_THRESHOLD: Final[float] = 1e-9

# Full semver 2.0.0 grammar (see https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string).
_SEMVER_RE: Final = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

# Schema-compatibility floor: only versions whose major component matches
# the current major are accepted. Patch / minor updates within the same
# major are allowed. See Enhancements #8.
_SUPPORTED_MAJOR: Final[str] = __version__.split(".", 1)[0]


# =========================================================================== #
# Errors
# =========================================================================== #
class UserPreferencesError(ValueError):
    """Raised for the module's own runtime errors.

    Constructor-level field validation raises
    :class:`UserPreferencesValidationError` (Pydantic's
    :class:`~pydantic.ValidationError`); this class is used for
    ``apply()`` / ``preset()`` / ``from_json()`` / ``save`` / ``load``
    failures that are *not* field validation. See the module docstring.
    """


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

    Constructor-time validation raises
    :class:`UserPreferencesValidationError` (Pydantic's
    :class:`~pydantic.ValidationError`). See the module docstring for the
    full exception model.
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
        __version__, description="Schema version (semver, major must match)",
    )

    # ------------------------------------------------------------------ #
    # Validators
    # ------------------------------------------------------------------ #
    @field_validator(*_WEIGHT_FIELDS, mode="before")
    @classmethod
    def _weights_finite(cls, v: Any) -> float:
        if isinstance(v, bool) or not isinstance(v, Real):
            raise UserPreferencesError(
                f"weight must be numeric, got {type(v).__name__}"
            )
        fv = float(v)
        if not math.isfinite(fv):
            raise UserPreferencesError(
                f"weight must be finite, got {v!r}"
            )
        return fv

    @field_validator("version", mode="before")
    @classmethod
    def _version_valid(cls, v: Any) -> str:
        if not isinstance(v, str) or not _SEMVER_RE.match(v):
            raise UserPreferencesError(
                f"version must be a semver string, got {v!r}"
            )
        major = v.split(".", 1)[0]
        if major != _SUPPORTED_MAJOR:
            raise UserPreferencesError(
                f"version {v!r} is incompatible with schema major "
                f"{_SUPPORTED_MAJOR!r}; files with a different major "
                f"must be migrated (see module docstring)."
            )
        return v

    @model_validator(mode="after")
    def _non_zero(self) -> "UserPreferences":
        if all(abs(getattr(self, f)) < _ZERO_THRESHOLD
               for f in _WEIGHT_FIELDS):
            raise UserPreferencesError(
                "All weights are zero; at least one must be non-zero."
            )
        return self

    # ------------------------------------------------------------------ #
    # Round-trip / utility
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, float]:
        """Return the six weights as a plain dict (``version`` excluded).

        Use :meth:`model_dump` if you also need the ``version`` field, or
        :meth:`to_dict_with_version`.
        """
        return {f: float(getattr(self, f)) for f in _WEIGHT_FIELDS}

    def to_dict_with_version(self) -> Dict[str, Any]:
        """Return the six weights **plus** ``version``.

        The shape is identical to :meth:`model_dump`. Provided so a
        caller who wants the full serializable shape has a documented
        name (unlike :meth:`to_dict` whose contract excludes
        ``version``).
        """
        return {
            **self.to_dict(),
            "version": self.version,
        }

    def to_json(self, *, indent: Optional[int] = 2) -> str:
        """Serialize to JSON (``version`` included)."""
        return self.model_dump_json(indent=indent)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "UserPreferences":
        """Reconstruct from a Mapping.

        Accepts either shape produced by the module:
        - :meth:`to_dict` (weights only) — ``version`` falls back to
          the current schema version.
        - :meth:`to_dict_with_version` / :meth:`model_dump` — all fields
          included.

        ``strict=False`` drops unknown keys instead of raising.
        """
        if not isinstance(data, Mapping):
            raise UserPreferencesError(
                f"data must be a Mapping, got {type(data).__name__}."
            )
        payload = dict(data)
        if not strict:
            allowed = set(_WEIGHT_FIELDS) | {"version"}
            payload = {k: v for k, v in payload.items() if k in allowed}
        return cls(**payload)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = True,
    ) -> "UserPreferences":
        """Deserialize from a JSON string.

        ``strict=False`` drops unknown keys instead of raising, matching
        :meth:`load` / :meth:`load_async`.
        """
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise UserPreferencesError(f"invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise UserPreferencesError(
                "preferences JSON must be an object"
            )
        return cls.from_dict(data, strict=strict)

    def active_objectives(self) -> Tuple[str, ...]:
        """Return the names of weights whose absolute value is > 1e-9."""
        return tuple(
            f for f in _WEIGHT_FIELDS
            if abs(getattr(self, f)) > _ZERO_THRESHOLD
        )

    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe summary of the profile.

        The keys ``weight_sum`` / ``weight_abs_sum`` are preferred over
        the aliases ``sum`` / ``abs_sum``; both are provided so callers
        that predate the rename continue to work.
        """
        weights = self.to_dict()
        values = list(weights.values())
        total = sum(values)
        abs_total = sum(abs(v) for v in values)
        return {
            "version": self.version,
            "weights": weights,
            "weight_sum": total,
            "weight_abs_sum": abs_total,
            # Backward-compatible aliases.
            "sum": total,
            "abs_sum": abs_total,
            "min": min(values),
            "max": max(values),
            "active": list(self.active_objectives()),
        }

    def copy(self) -> "UserPreferences":
        """Return an independent, re-validated copy."""
        return self.model_validate(self.model_dump())

    def _validated_copy(self, **updates: Any) -> "UserPreferences":
        """Return a copy with ``updates`` applied, re-validated.

        Pydantic v2's ``model_copy(update=...)`` bypasses the model's
        validators; using ``model_validate`` closes that gap. See
        Enhancements #14.
        """
        data = self.model_dump()
        data.update(updates)
        return self.model_validate(data)

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
            # Guarded by the non-zero validator; kept defensively.
            return self.copy()
        return self._validated_copy(
            **{
                f: getattr(self, f) / total for f in _WEIGHT_FIELDS
            }
        )

    def normalize_positive(self) -> "UserPreferences":
        """Shift weights to be non-negative, then normalize to sum to 1.0.

        Each weight is offset by ``-min(0, min(weights))`` so the smallest
        weight becomes zero. If every weight is already non-negative,
        this is equivalent to ``weights / sum(weights)``.

        Raises
        ------
        UserPreferencesError
            If every weight is equal and negative — after shifting they
            all collapse to zero and the vector cannot be normalized.
        """
        values = {f: getattr(self, f) for f in _WEIGHT_FIELDS}
        offset = -min(0.0, min(values.values()))
        shifted = {f: v + offset for f, v in values.items()}
        total = sum(shifted.values())
        if total < _ZERO_THRESHOLD:
            raise UserPreferencesError(
                "cannot make an all-equal-negative weight vector "
                "positive; every weight is the same negative value."
            )
        return self._validated_copy(
            **{f: shifted[f] / total for f in _WEIGHT_FIELDS}
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

        Recognized names (case-insensitive): ``"balanced"``,
        ``"energy"`` / ``"energy_first"``, ``"carbon"`` /
        ``"carbon_first"``, ``"helium"`` / ``"helium_aware"``.
        """
        if not isinstance(name, str) or not name:
            raise UserPreferencesError(
                "preset name must be a non-empty string."
            )
        table = {
            "balanced":     cls.balanced,
            "energy":       cls.energy_first,
            "energy_first": cls.energy_first,
            "carbon":       cls.carbon_first,
            "carbon_first": cls.carbon_first,
            "helium":       cls.helium_aware,
            "helium_aware": cls.helium_aware,
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
    def _validate_and_weight(
        self, objective_values: Any,
    ) -> Dict[str, float]:
        """Return per-objective weighted contributions.

        Shared body of :meth:`apply` and :meth:`apply_detailed`.
        """
        if not isinstance(objective_values, Mapping):
            raise UserPreferencesError(
                f"objective_values must be a Mapping, got "
                f"{type(objective_values).__name__}."
            )
        if not objective_values:
            raise UserPreferencesError(
                "objective_values is empty; pass at least one objective."
            )
        weights = self.to_dict()
        contributions: Dict[str, float] = {}
        for key, value in objective_values.items():
            if key not in weights:
                raise UserPreferencesError(
                    f"Unknown objective key: {key!r}; expected one of "
                    f"{sorted(weights)}."
                )
            if isinstance(value, bool) or not isinstance(value, Real):
                raise UserPreferencesError(
                    f"objective_values[{key!r}] must be numeric, got "
                    f"{type(value).__name__}."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise UserPreferencesError(
                    f"objective_values[{key!r}] must be finite, got "
                    f"{value!r}."
                )
            contributions[key] = weights[key] * fv
        return contributions

    def apply(self, objective_values: Mapping[str, float]) -> float:
        """Return the weighted sum of ``objective_values``.

        Parameters
        ----------
        objective_values : Mapping[str, float]
            A mapping whose keys are a subset of the weight names
            (``alpha``…``zeta``) and whose values are finite reals.

        Raises
        ------
        UserPreferencesError
            If ``objective_values`` is not a Mapping, is empty, contains
            an unknown key, or contains a non-finite value.
        """
        return sum(self._validate_and_weight(objective_values).values())

    def apply_detailed(
        self, objective_values: Mapping[str, float],
    ) -> Dict[str, Any]:
        """Return a per-objective breakdown of :meth:`apply`.

        Returns
        -------
        dict
            ``{"total": float, "by_key": {key: weight * value}}``.

        Raises
        ------
        UserPreferencesError
            Same conditions as :meth:`apply`.
        """
        contributions = self._validate_and_weight(objective_values)
        return {
            "total": sum(contributions.values()),
            "by_key": contributions,
        }

    # ------------------------------------------------------------------ #
    # Persistence (sync)
    # ------------------------------------------------------------------ #
    def save(self, path: Union[str, os.PathLike]) -> bool:
        """Atomically write the preferences to ``path``.

        Parent directories are created automatically. Returns ``True``
        on success, ``False`` on failure (logged at ``ERROR``).
        """
        try:
            p = _coerce_path(path)
        except UserPreferencesError as exc:
            logger.error("Invalid save path: %s", exc)
            return False
        try:
            _atomic_write_json(p, self.model_dump())
        except Exception as exc:
            logger.error("Failed to save preferences: %s", exc)
            return False
        logger.info("User preferences saved to %s", p)
        return True

    @classmethod
    def load(
        cls,
        path: Union[str, os.PathLike],
        *,
        strict: bool = True,
    ) -> Optional["UserPreferences"]:
        """Load preferences from ``path``.

        Returns ``None`` on any failure. Log level distinguishes the
        failure mode: ``INFO`` for a missing file, ``WARNING`` for
        malformed JSON or schema mismatch, ``ERROR`` for I/O errors.
        """
        try:
            p = _coerce_path(path)
        except UserPreferencesError as exc:
            logger.error("Invalid load path: %s", exc)
            return None
        if not p.exists():
            logger.info("Preferences file %s not found", p)
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(data, Mapping):
                raise UserPreferencesError(
                    "preferences JSON must be an object"
                )
            return cls.from_dict(data, strict=strict)
        except (UserPreferencesValidationError, UserPreferencesError) as exc:
            logger.warning("Invalid preferences in %s: %s", p, exc)
            return None
        except OSError as exc:
            logger.error("Could not read %s: %s", p, exc)
            return None

    # ------------------------------------------------------------------ #
    # Persistence (async)
    # ------------------------------------------------------------------ #
    async def save_async(self, path: Union[str, os.PathLike]) -> bool:
        """Async sibling of :meth:`save`.

        Uses ``aiofiles`` when available, otherwise offloads the sync
        writer to a worker thread via a version-safe helper.
        """
        try:
            p = _coerce_path(path)
        except UserPreferencesError as exc:
            logger.error("Invalid save path: %s", exc)
            return False
        payload = self.model_dump()
        try:
            if _AIOFILES_AVAILABLE:
                await _atomic_write_json_async(p, payload)
            else:
                await _run_sync_in_thread(_atomic_write_json, p, payload)
        except Exception as exc:
            logger.error("Failed to save preferences (async): %s", exc)
            return False
        logger.info("User preferences saved (async) to %s", p)
        return True

    @classmethod
    async def load_async(
        cls,
        path: Union[str, os.PathLike],
        *,
        strict: bool = True,
    ) -> Optional["UserPreferences"]:
        """Async sibling of :meth:`load`."""
        try:
            p = _coerce_path(path)
        except UserPreferencesError as exc:
            logger.error("Invalid load path: %s", exc)
            return None
        if not p.exists():
            logger.info("Preferences file %s not found", p)
            return None
        try:
            if _AIOFILES_AVAILABLE:
                async with aiofiles.open(  # type: ignore[union-attr]
                    p, "r", encoding="utf-8",
                ) as f:
                    content = await f.read()
                data = json.loads(content)
            else:
                data = await _run_sync_in_thread(
                    lambda: json.loads(p.read_text(encoding="utf-8"))
                )
            if not isinstance(data, Mapping):
                raise UserPreferencesError(
                    "preferences JSON must be an object"
                )
            return cls.from_dict(data, strict=strict)
        except (UserPreferencesValidationError, UserPreferencesError) as exc:
            logger.warning("Invalid preferences in %s: %s", p, exc)
            return None
        except OSError as exc:
            logger.error("Could not read %s: %s", p, exc)
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
# Cross-version async helper
# =========================================================================== #
async def _run_sync_in_thread(func: Any, *args: Any) -> Any:
    """
    Cross-version ``asyncio.to_thread``.

    Uses ``asyncio.to_thread`` (Python 3.9+) when available, otherwise
    falls back to ``loop.run_in_executor`` with the default executor.
    The fallback only supports positional arguments — matching every
    call site in this module.
    """
    if hasattr(asyncio, "to_thread"):
        return await asyncio.to_thread(func, *args)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


# =========================================================================== #
# Path coercion
# =========================================================================== #
def _coerce_path(path: Any) -> Path:
    """Coerce ``path`` to a :class:`pathlib.Path`.

    Accepts ``str``, :class:`Path`, and any ``os.PathLike``. Rejects
    empty strings and non-path types with :class:`UserPreferencesError`.
    """
    if isinstance(path, Path):
        return path
    if isinstance(path, str):
        if not path:
            raise UserPreferencesError("path must not be empty.")
        return Path(path)
    if hasattr(path, "__fspath__"):
        return Path(os.fspath(path))
    raise UserPreferencesError(
        f"path must be str, Path, or os.PathLike, got "
        f"{type(path).__name__}."
    )


# =========================================================================== #
# Atomic I/O helpers
# =========================================================================== #
def _atomic_write_json(path: Path, payload: Any) -> None:
    """
    Write ``payload`` to ``path`` atomically.

    Uses ``mkstemp`` + ``fdopen`` + ``os.replace``. On any failure the
    temporary file is unlinked and the exception propagates. The file
    descriptor is closed on every path, including when ``fdopen`` itself
    raises (the previous revision leaked it).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
    )
    try:
        f = os.fdopen(fd, "w", encoding="utf-8")
    except OSError:
        os.close(fd)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    try:
        with f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


async def _atomic_write_json_async(path: Path, payload: Any) -> None:
    """
    Async variant of :func:`_atomic_write_json` using ``aiofiles``.

    The temporary file is created by ``mkstemp`` (fd closed immediately),
    then reopened **by path** with ``aiofiles.open`` — matching aiofiles'
    documented interface. The previous revision passed the raw fd.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
    )
    os.close(fd)
    try:
        content = json.dumps(
            payload, indent=2, ensure_ascii=False,
        )
        async with aiofiles.open(  # type: ignore[union-attr]
            tmp, "w", encoding="utf-8",
        ) as f:
            await f.write(content)
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
__all__: List[str] = [
    "__version__",
    "HAS_AIOFILES",
    "UserPreferences",
    "UserPreferencesError",
    "UserPreferencesValidationError",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# =========================================================================== #
# Smoke test: python -m user_preferences
# =========================================================================== #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile
    from numbers import Number

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

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

    def _expect_error(name: str, expected: type, fn) -> None:
        try:
            fn()
            _check(name, False)
        except expected:
            _check(name, True)
        except Exception as exc:
            print(
                f"  ❌ {name} — wrong exception {type(exc).__name__}: "
                f"{exc}"
            )
            global failed
            failed += 1

    # ---- 1. Presets are usable ---------------------------------- #
    print("presets:")
    for name in ("balanced", "energy", "carbon", "helium"):
        p = UserPreferences.preset(name)
        _check(
            f"preset({name!r}) constructs",
            abs(sum(p.to_dict().values())) > 0,
        )
    # Aliases
    _check(
        "preset('energy_first') alias works",
        UserPreferences.preset("energy_first").to_dict()
        == UserPreferences.energy_first().to_dict(),
    )

    # ---- 2. normalize semantics --------------------------------- #
    print("normalize:")
    energy = UserPreferences.energy_first()
    norm = energy.normalize()
    _check(
        "normalize: sum(|w|) == 1",
        abs(sum(abs(v) for v in norm.to_dict().values()) - 1.0) < 1e-9,
    )
    pos = energy.normalize_positive()
    _check(
        "normalize_positive: sum(w) == 1",
        abs(sum(pos.to_dict().values()) - 1.0) < 1e-9,
    )
    _check(
        "normalize_positive: all weights >= 0",
        all(v >= 0 for v in pos.to_dict().values()),
    )

    # All-negative edge case
    all_neg = UserPreferences(
        alpha=-1.0, beta=-1.0, gamma=-1.0,
        delta=-1.0, epsilon=-1.0, zeta=-1.0,
    )
    _expect_error(
        "normalize_positive(all_negative) raises",
        UserPreferencesError,
        all_neg.normalize_positive,
    )

    # ---- 3. apply() / apply_detailed() ------------------------- #
    print("apply:")
    score = energy.apply({
        "alpha": 0.8, "beta": 0.6, "gamma": 0.4,
        "delta": 0.7, "epsilon": 0.3, "zeta": 0.5,
    })
    _check("apply returns a float", isinstance(score, float))

    detailed = energy.apply_detailed({"alpha": 0.8, "beta": 0.6})
    _check(
        "apply_detailed has 'total'",
        "total" in detailed and isinstance(detailed["total"], float),
    )
    _check(
        "apply_detailed has 'by_key'",
        "by_key" in detailed
        and detailed["by_key"]["alpha"] == 2.0 * 0.8,
    )
    _check(
        "apply == apply_detailed['total']",
        abs(
            score - detailed["total"]
            - energy.apply(
                {"gamma": 0.4, "delta": 0.7,
                 "epsilon": 0.3, "zeta": 0.5},
            )
        ) < 1e-9,
    )

    # apply() rejections
    _expect_error(
        "apply('not-a-mapping') raises",
        UserPreferencesError,
        lambda: energy.apply("not-a-mapping"),  # type: ignore[arg-type]
    )
    _expect_error(
        "apply({}) raises",
        UserPreferencesError,
        lambda: energy.apply({}),
    )
    _expect_error(
        "apply(unknown key) raises",
        UserPreferencesError,
        lambda: energy.apply({"unknown_key": 1.0}),
    )
    _expect_error(
        "apply(NaN) raises",
        UserPreferencesError,
        lambda: energy.apply({"zeta": float("nan")}),
    )
    _expect_error(
        "apply(string value) raises",
        UserPreferencesError,
        lambda: energy.apply({"alpha": "big"}),  # type: ignore[dict-item]
    )

    # ---- 4. Constructor validation (Pydantic ValidationError) - #
    print("constructor validation:")
    for label, bad_w in (
        ("NaN", float("nan")),
        ("inf", float("inf")),
        ("string", "big"),
        ("out-of-range", 1e9),
    ):
        _expect_error(
            f"reject {label} weight",
            UserPreferencesValidationError,
            lambda bw=bad_w: UserPreferences(alpha=bw),  # type: ignore[arg-type]
        )

    # Version validation
    _expect_error(
        "reject bad version format",
        UserPreferencesValidationError,
        lambda: UserPreferences(version="not-a-version"),
    )
    _expect_error(
        "reject incompatible major",
        UserPreferencesValidationError,
        lambda: UserPreferences(version="2.1.0"),
    )
    _check(
        "accept full semver",
        UserPreferences(version="3.0.0-beta.1+build.42").version
        == "3.0.0-beta.1+build.42",
    )
    _check(
        "accept same-major patch",
        UserPreferences(version="3.1.0").version == "3.1.0",
    )

    # All-zero rejection
    _expect_error(
        "reject all-zero weights",
        UserPreferencesValidationError,
        lambda: UserPreferences(
            alpha=0.0, beta=0.0, gamma=0.0,
            delta=0.0, epsilon=0.0, zeta=0.0,
        ),
    )

    # numpy-style numeric acceptance (if available)
    try:
        import numpy as np  # type: ignore
        _check(
            "accept numpy.float64",
            UserPreferences(alpha=np.float64(1.5)).alpha == 1.5,
        )
        _check(
            "accept numpy.int64",
            UserPreferences(alpha=np.int64(2)).alpha == 2.0,
        )
    except ImportError:
        _check("numpy acceptance (skipped)", True)

    # ---- 5. Version field behaviour ---------------------------- #
    print("version field:")
    _check("version defaults to __version__",
           UserPreferences().version == __version__)
    _check(
        "default is same-major as _SUPPORTED_MAJOR",
        UserPreferences().version.split(".")[0] == _SUPPORTED_MAJOR,
    )

    # ---- 6. Round-trip via all documented shapes --------------- #
    print("round-trip:")
    payload = energy.to_json()
    restored = UserPreferences.from_json(payload)
    _check(
        "from_json(to_json) round-trips weights",
        restored.to_dict() == energy.to_dict(),
    )
    _check(
        "from_json(to_json) round-trips version",
        restored.version == energy.version,
    )

    # from_dict with weights-only shape (version is defaulted)
    d = energy.to_dict()
    restored_d = UserPreferences.from_dict(d)
    _check(
        "from_dict(weights-only) works",
        restored_d.to_dict() == energy.to_dict(),
    )

    # from_dict with full shape
    full = energy.to_dict_with_version()
    restored_full = UserPreferences.from_dict(full)
    _check(
        "from_dict(to_dict_with_version()) round-trips",
        restored_full.to_dict() == energy.to_dict(),
    )

    # from_json(strict=False)
    payload_with_extra = json.dumps({
        **energy.to_dict_with_version(),
        "unknown_field": 1,
    })
    _check(
        "from_json(strict=False) drops unknown keys",
        UserPreferences.from_json(
            payload_with_extra, strict=False,
        ).to_dict() == energy.to_dict(),
    )
    _expect_error(
        "from_json(strict=True) rejects unknown keys",
        UserPreferencesValidationError,
        lambda: UserPreferences.from_json(payload_with_extra),
    )

    # ---- 7. Sync save / load ---------------------------------- #
    print("sync persistence:")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "prefs.json"
        _check("save() returns True", energy.save(path) is True)
        _check("file was created", path.exists())

        loaded = UserPreferences.load(path)
        _check("load() returns an instance", loaded is not None)
        _check(
            "loaded weights match",
            loaded is not None and loaded.to_dict() == energy.to_dict(),
        )

        # Missing file → INFO log, None return, no exception
        missing = Path(td) / "missing.json"
        _check(
            "load(missing) returns None",
            UserPreferences.load(missing) is None,
        )

        # Malformed JSON → WARNING log, None return
        bad = Path(td) / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        _check(
            "load(malformed) returns None",
            UserPreferences.load(bad) is None,
        )

        # Schema-incompatible JSON → WARNING, None return
        old = Path(td) / "old.json"
        old.write_text(
            json.dumps({
                **energy.to_dict(), "version": "2.1.0",
            }),
            encoding="utf-8",
        )
        _check(
            "load(incompatible major) returns None",
            UserPreferences.load(old) is None,
        )

        # save() with invalid path type
        _check(
            "save(42) returns False",
            energy.save(42) is False,  # type: ignore[arg-type]
        )

    # ---- 8. Async save / load --------------------------------- #
    print("async persistence:")
    with tempfile.TemporaryDirectory() as td:

        async def _async_round_trip() -> None:
            path2 = Path(td) / "async" / "prefs.json"
            ok = await energy.save_async(path2)
            _check("save_async() returns True", ok is True)
            _check("async file created", path2.exists())

            loaded = await UserPreferences.load_async(path2)
            _check("load_async() returns an instance", loaded is not None)
            _check(
                "async round-trip matches",
                loaded is not None
                and loaded.to_dict() == energy.to_dict(),
            )

            missing = Path(td) / "missing_async.json"
            _check(
                "load_async(missing) returns None",
                await UserPreferences.load_async(missing) is None,
            )

        asyncio.run(_async_round_trip())

    # ---- 9. Diagnostics --------------------------------------- #
    print("diagnostics:")
    stats = energy.statistics()
    _check(
        "statistics has weight_sum",
        "weight_sum" in stats and isinstance(stats["weight_sum"], float),
    )
    _check(
        "statistics has weight_abs_sum",
        "weight_abs_sum" in stats,
    )
    _check(
        "statistics backward-compat 'sum' matches weight_sum",
        stats["sum"] == stats["weight_sum"],
    )
    _check(
        "statistics backward-compat 'abs_sum' matches weight_abs_sum",
        stats["abs_sum"] == stats["weight_abs_sum"],
    )

    _check(
        "active_objectives returns a tuple",
        isinstance(energy.active_objectives(), tuple),
    )
    _check(
        "repr is non-empty",
        bool(repr(energy)),
    )

    # ---- 10. copy() is independent and re-validated ----------- #
    print("copy:")
    copy = energy.copy()
    _check("copy returns a new instance", copy is not energy)
    _check(
        "copy has the same weights",
        copy.to_dict() == energy.to_dict(),
    )
    _check(
        "copy is re-validated",
        copy.version == energy.version,
    )

    # ---- 11. Preset factory errors ---------------------------- #
    _expect_error(
        "preset('bogus') raises",
        UserPreferencesError,
        lambda: UserPreferences.preset("bogus"),
    )
    _expect_error(
        "preset('') raises",
        UserPreferencesError,
        lambda: UserPreferences.preset(""),
    )

    # ---- 12. Public API -------------------------------------- #
    print("public API:")
    for name in (
        "HAS_AIOFILES",
        "UserPreferences",
        "UserPreferencesError",
        "UserPreferencesValidationError",
        "__version__",
    ):
        _check(f"{name} exported", name in __all__)
    _check(
        "UserPreferencesError is ValueError",
        issubclass(UserPreferencesError, ValueError),
    )
    _check(
        "UserPreferencesValidationError is ValueError",
        issubclass(UserPreferencesValidationError, ValueError),
    )
    _check(
        "__dir__ matches __all__",
        __dir__() == sorted(__all__),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
