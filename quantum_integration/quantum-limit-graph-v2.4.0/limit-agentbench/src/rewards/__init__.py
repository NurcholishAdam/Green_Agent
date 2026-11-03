# src/rewards/__init__.py

"""
Sustainability Reward Shaping
=============================

Reward-shaping primitives for carbon-aware, helium-aware AI orchestration:

- :class:`NegawattReward` — accuracy-per-watt, negawatt score, and a
  combined RL-compatible reward signal.
- :class:`NegawattRewardConfig` — frozen, validated configuration.
- :class:`RewardBreakdown` — structured decomposition for logging / RLHF.

Design
------
- **Defensive sub-module imports** — a missing sub-module degrades the
  package instead of breaking it. ``REWARDS_AVAILABILITY`` records
  which modules loaded; ``REWARDS_LOAD_ERRORS`` records why a failed
  module failed.
- **Three parallel registries** — ``REWARDS_REGISTRY`` (components),
  ``REWARDS_CONFIG_REGISTRY`` (configs), ``REWARDS_ERROR_REGISTRY``
  (errors). All three are read-only views over internal dicts; writes
  go through :func:`register_reward_component` /
  :func:`unregister_reward_component`.
- **``RewardBreakdown`` is a result type, not a component** — it is
  exported at package level but is not registered in any of the three
  name-addressed registries. Iterating ``list_reward_components()``
  sees reward components only.
- **Name normalisation** — registration and lookup strip and lowercase
  names so ``" NEGAWATT "``, ``"negawatt"``, and ``"Negawatt"`` share a
  key.
- **Package error hierarchy** — :class:`RewardsError` (``ValueError``)
  with :class:`RewardRegistrationError` and
  :class:`UnknownRewardComponentError` subclasses. The latter is also
  a ``KeyError`` for backward compatibility.
- **CI validation** — :func:`validate_rewards_registry` returns a
  structured report. No import-time side effects.

Version: 3.0.0
"""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Type

logger = logging.getLogger(__name__)

__all__ = [
    # ---- Package metadata ---------------------------------------
    "VERSION",
    "__version__",
    # ---- Package errors -----------------------------------------
    "RewardsError",
    "RewardRegistrationError",
    "UnknownRewardComponentError",
    # ---- Registries (read-only views) ---------------------------
    "REWARDS_REGISTRY",
    "REWARDS_CONFIG_REGISTRY",
    "REWARDS_ERROR_REGISTRY",
    "REWARDS_AVAILABILITY",
    "REWARDS_LOAD_ERRORS",
    # ---- Registry helpers ---------------------------------------
    "register_reward_component",
    "unregister_reward_component",
    "get_reward_component",
    "instantiate_reward_component",
    "list_reward_components",
    "get_reward_config",
    "get_reward_error",
    "validate_rewards_registry",
    # ---- Reward components --------------------------------------
    "NegawattReward",
    "NegawattRewardConfig",
    "NegawattRewardError",
    "RewardBreakdown",
]

VERSION = "3.0.0"
__version__ = VERSION

# Guard against accidental drift between the two version strings.
assert VERSION == __version__, (
    "VERSION and __version__ must agree."
)


# --------------------------------------------------------------------------- #
# Package errors
# --------------------------------------------------------------------------- #
class RewardsError(ValueError):
    """Base error for the rewards package aggregator."""


class RewardRegistrationError(RewardsError):
    """Raised by :func:`register_reward_component` on conflict or invalid input."""


class UnknownRewardComponentError(RewardsError, KeyError):
    """
    Raised by :func:`get_reward_component` (and its siblings) when a
    name is not registered. Subclass of :class:`RewardsError`
    (``ValueError``) and :class:`KeyError` so existing callers using
    either catch clause still work.
    """


# --------------------------------------------------------------------------- #
# Availability and load-error tracking
# --------------------------------------------------------------------------- #
_REWARDS_AVAILABILITY: Dict[str, bool] = {}
_REWARDS_LOAD_ERRORS: Dict[str, str] = {}


# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
try:
    from .negawatt_reward import (
        NegawattReward,
        NegawattRewardConfig,
        NegawattRewardError,
        RewardBreakdown,
    )
    _REWARDS_AVAILABILITY["negawatt_reward"] = True
except ImportError as exc:
    logger.warning("negawatt_reward unavailable: %s", exc)
    _REWARDS_AVAILABILITY["negawatt_reward"] = False
    _REWARDS_LOAD_ERRORS["negawatt_reward"] = (
        f"{type(exc).__name__}: {exc}"
    )
    NegawattReward = None  # type: ignore[assignment]
    NegawattRewardConfig = None  # type: ignore[assignment]
    NegawattRewardError = None  # type: ignore[assignment]
    RewardBreakdown = None  # type: ignore[assignment]


# --------------------------------------------------------------------------- #
# Registries — internal mutable dicts + public read-only views
# --------------------------------------------------------------------------- #
_REWARDS_REGISTRY: Dict[str, Type[Any]] = {}
_REWARDS_CONFIG_REGISTRY: Dict[str, Type[Any]] = {}
_REWARDS_ERROR_REGISTRY: Dict[str, Type[Any]] = {}


def _register_triple(
    name: str,
    component: Optional[Type[Any]],
    config: Optional[Type[Any]],
    error: Optional[Type[Any]],
) -> None:
    """
    Populate all three registries for ``name``.

    Each entry is registered only when the class is not ``None`` — a
    missing submodule simply has no entry in the registry.
    """
    if component is None:
        logger.debug(
            "Not registering %r: component is None (import failed).",
            name,
        )
        return
    if not isinstance(component, type):
        logger.warning(
            "Not registering %r: %r is not a class.",
            name, type(component).__name__,
        )
        return
    _REWARDS_REGISTRY[name] = component
    if config is not None:
        if not isinstance(config, type):
            logger.warning(
                "Not registering config for %r: %r is not a class.",
                name, type(config).__name__,
            )
        else:
            _REWARDS_CONFIG_REGISTRY[name] = config
    if error is not None:
        if not isinstance(error, type):
            logger.warning(
                "Not registering error for %r: %r is not a class.",
                name, type(error).__name__,
            )
        else:
            _REWARDS_ERROR_REGISTRY[name] = error


# Populate registries. `RewardBreakdown` is intentionally not registered
# — it is a result dataclass, not a reward component.
_register_triple(
    "negawatt",
    NegawattReward,
    NegawattRewardConfig,
    NegawattRewardError,
)


# ---- Read-only public views ------------------------------------------------
REWARDS_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _REWARDS_REGISTRY
)
REWARDS_CONFIG_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _REWARDS_CONFIG_REGISTRY
)
REWARDS_ERROR_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _REWARDS_ERROR_REGISTRY
)
REWARDS_AVAILABILITY: Mapping[str, bool] = MappingProxyType(
    _REWARDS_AVAILABILITY
)
REWARDS_LOAD_ERRORS: Mapping[str, str] = MappingProxyType(
    _REWARDS_LOAD_ERRORS
)


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def _normalise_name(name: Any) -> str:
    """Strip and lowercase; raise on invalid input."""
    if not isinstance(name, str):
        raise RewardRegistrationError(
            f"name must be a str, got {type(name).__name__}."
        )
    key = name.strip().lower()
    if not key:
        raise RewardRegistrationError(
            "name must be a non-empty string."
        )
    return key


def register_reward_component(
    name: str,
    cls: Type[Any],
    *,
    config: Optional[Type[Any]] = None,
    error: Optional[Type[Any]] = None,
    overwrite: bool = False,
) -> None:
    """
    Register a reward component under ``name``.

    ``name`` is stripped and lowercased before storage, so
    ``register(" Negawatt ", X)`` and ``get("negawatt")`` refer to the
    same entry.

    Raises
    ------
    RewardRegistrationError
        If ``name`` is invalid or already registered to a different
        class (unless ``overwrite=True``).
    TypeError
        If ``cls`` / ``config`` / ``error`` is not a class.
    """
    key = _normalise_name(name)
    for label, value in (
        ("cls", cls), ("config", config), ("error", error),
    ):
        if value is not None and not isinstance(value, type):
            raise TypeError(
                f"{label} must be a class, got "
                f"{type(value).__name__}."
            )
    existing = _REWARDS_REGISTRY.get(key)
    if existing is not None and existing is not cls and not overwrite:
        raise RewardRegistrationError(
            f"Reward component {key!r} already registered to "
            f"{existing.__name__}."
        )
    _REWARDS_REGISTRY[key] = cls
    if config is not None:
        _REWARDS_CONFIG_REGISTRY[key] = config
    if error is not None:
        _REWARDS_ERROR_REGISTRY[key] = error
    logger.debug(
        "Registered reward component %s -> %s", key, cls.__name__,
    )


def unregister_reward_component(name: str) -> bool:
    """
    Remove a registered component from all three registries.

    Returns True if the component existed. Useful for test isolation.
    """
    try:
        key = _normalise_name(name)
    except RewardRegistrationError:
        return False
    existed = _REWARDS_REGISTRY.pop(key, None) is not None
    _REWARDS_CONFIG_REGISTRY.pop(key, None)
    _REWARDS_ERROR_REGISTRY.pop(key, None)
    return existed


def get_reward_component(name: str) -> Type[Any]:
    """
    Return the **class** registered under ``name`` (case-insensitive).

    Raises
    ------
    UnknownRewardComponentError
        If the name is not registered (also a ``KeyError``).
    """
    try:
        key = _normalise_name(name)
    except RewardRegistrationError as exc:
        raise UnknownRewardComponentError(
            f"unknown reward component {name!r}: {exc}"
        ) from None
    try:
        return _REWARDS_REGISTRY[key]
    except KeyError:
        raise UnknownRewardComponentError(
            f"unknown reward component {name!r}. "
            f"Available: {sorted(_REWARDS_REGISTRY)}"
        ) from None


def instantiate_reward_component(
    name: str, **kwargs: Any,
) -> Any:
    """Instantiate a registered component by name."""
    cls = get_reward_component(name)
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise RewardsError(
            f"could not instantiate {name!r} with kwargs "
            f"{sorted(kwargs)}: {exc}"
        ) from exc


def list_reward_components() -> Dict[str, str]:
    """
    Return ``{registered_name: "module.ClassName"}`` for all entries.

    The full qualified path lets callers see which submodule each
    component lives in.
    """
    return {
        n: f"{c.__module__}.{c.__qualname__}"
        for n, c in _REWARDS_REGISTRY.items()
    }


def get_reward_config(name: str) -> Type[Any]:
    """
    Return the ``*Config`` class for a reward component name.

    Raises
    ------
    UnknownRewardComponentError
        If the component is not registered or has no config class.
    """
    try:
        key = _normalise_name(name)
    except RewardRegistrationError as exc:
        raise UnknownRewardComponentError(
            f"unknown reward component {name!r}: {exc}"
        ) from None
    if key not in _REWARDS_REGISTRY:
        raise UnknownRewardComponentError(
            f"unknown reward component {name!r}. "
            f"Available: {sorted(_REWARDS_REGISTRY)}"
        )
    try:
        return _REWARDS_CONFIG_REGISTRY[key]
    except KeyError:
        raise UnknownRewardComponentError(
            f"component {name!r} is registered but has no config "
            f"class."
        ) from None


def get_reward_error(name: str) -> Type[Any]:
    """
    Return the ``*Error`` class for a reward component name.

    Raises
    ------
    UnknownRewardComponentError
        If the component is not registered or has no error class.
    """
    try:
        key = _normalise_name(name)
    except RewardRegistrationError as exc:
        raise UnknownRewardComponentError(
            f"unknown reward component {name!r}: {exc}"
        ) from None
    if key not in _REWARDS_REGISTRY:
        raise UnknownRewardComponentError(
            f"unknown reward component {name!r}. "
            f"Available: {sorted(_REWARDS_REGISTRY)}"
        )
    try:
        return _REWARDS_ERROR_REGISTRY[key]
    except KeyError:
        raise UnknownRewardComponentError(
            f"component {name!r} is registered but has no error "
            f"class."
        ) from None


def validate_rewards_registry() -> Dict[str, Any]:
    """
    Return a structured validation report.

    Useful as a CI check: verify that every expected submodule is
    available and that every registry entry is a class.
    """
    available = sorted(
        k for k, v in _REWARDS_AVAILABILITY.items() if v
    )
    unavailable = sorted(
        k for k, v in _REWARDS_AVAILABILITY.items() if not v
    )
    load_errors = dict(_REWARDS_LOAD_ERRORS)

    # Guard against future misuse of register_reward_component that
    # bypasses the isinstance(cls, type) check.
    for registry, label in (
        (_REWARDS_REGISTRY, "components"),
        (_REWARDS_CONFIG_REGISTRY, "configs"),
        (_REWARDS_ERROR_REGISTRY, "errors"),
    ):
        for name, cls in registry.items():
            if not isinstance(cls, type):
                raise RewardsError(
                    f"{label} registry entry {name!r} is not a "
                    f"class: {type(cls).__name__}."
                )

    return {
        "available": available,
        "unavailable": unavailable,
        "load_errors": load_errors,
        "registry_sizes": {
            "components": len(_REWARDS_REGISTRY),
            "configs": len(_REWARDS_CONFIG_REGISTRY),
            "errors": len(_REWARDS_ERROR_REGISTRY),
        },
        "registered": {
            name: f"{cls.__module__}.{cls.__qualname__}"
            for name, cls in _REWARDS_REGISTRY.items()
        },
    }


# --------------------------------------------------------------------------- #
# __all__ completeness assertion
# --------------------------------------------------------------------------- #
# A name in __all__ that doesn't resolve means a submodule rename or
# accidental deletion slipped past review. Fail-fast at import.
_missing_names = [n for n in __all__ if n not in globals()]
if _missing_names:
    raise RewardsError(
        f"rewards package __all__ references missing names: "
        f"{sorted(_missing_names)}. This usually means a submodule "
        f"renamed a public symbol without updating the aggregator."
    )


# --------------------------------------------------------------------------- #
# Smoke test: python -m rewards
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"rewards VERSION     : {VERSION}")
    print(f"rewards __version__ : {__version__}")

    print("\nAvailability:")
    report = validate_rewards_registry()
    for name in sorted(_REWARDS_AVAILABILITY.keys()):
        ok = _REWARDS_AVAILABILITY[name]
        marker = "✓" if ok else "✗"
        print(f"  {marker} {name}")
        if not ok:
            err = report["load_errors"].get(name, "unknown")
            print(f"    ↳ {err}")

    print(
        f"\nRegistry sizes: "
        f"components={report['registry_sizes']['components']}, "
        f"configs={report['registry_sizes']['configs']}, "
        f"errors={report['registry_sizes']['errors']}"
    )

    print("\nRegistered components (name → fully qualified path):")
    for name, path in sorted(report["registered"].items()):
        print(f"  {name:<12} → {path}")

    print("\nRegistry round-trip:")
    if report["registry_sizes"]["components"] > 0:
        sample = sorted(_REWARDS_REGISTRY.keys())[0]
        cls = get_reward_component(sample)
        cfg = get_reward_config(sample)
        err = get_reward_error(sample)
        print(
            f"  get_reward_component({sample!r}) → "
            f"{cls.__name__}"
        )
        print(
            f"  get_reward_config({sample!r})    → "
            f"{cfg.__name__}"
        )
        print(
            f"  get_reward_error({sample!r})     → "
            f"{err.__name__}"
        )

    print("\nData-class symbols are NOT registered as components:")
    for name in ("config", "breakdown", "reward_breakdown"):
        try:
            get_reward_component(name)
        except UnknownRewardComponentError:
            print(f"  ✓ {name!r} correctly not a component")

    print("\nRead-only enforcement:")
    for label, accessor in (
        (
            "REWARDS_REGISTRY",
            lambda: REWARDS_REGISTRY.__setitem__("evil", int),
        ),
        (
            "REWARDS_AVAILABILITY",
            lambda: REWARDS_AVAILABILITY.__setitem__(
                "evil", True,
            ),
        ),
    ):
        try:
            accessor()
        except (TypeError, AttributeError) as exc:
            print(
                f"  ✓ {label} is read-only "
                f"({type(exc).__name__})"
            )

    print("\nName normalisation:")
    if report["registry_sizes"]["components"] > 0:
        sample_upper = sorted(_REWARDS_REGISTRY.keys())[0].upper()
        try:
            normalized = get_reward_component(
                f"  {sample_upper}  "
            )
            print(
                f"  get('  {sample_upper}  ') → "
                f"{normalized.__name__}"
            )
        except Exception as exc:
            print(f"  normalisation test failed: {exc}")

    print("\nInstantiation:")
    try:
        inst = instantiate_reward_component(
            "negawatt", baseline_energy=0.01,
        )
        print(f"  instantiate_reward_component('negawatt') → {inst}")
    except UnknownRewardComponentError:
        print("  (negawatt_reward not available)")

    print("\nRegister / unregister round-trip:")

    class _MockReward:
        def __call__(self, acc, energy):
            return float(acc)

    register_reward_component("mock", _MockReward)
    print(
        f"  registered 'mock' → "
        f"{get_reward_component('mock').__name__}"
    )
    assert unregister_reward_component("mock") is True
    try:
        get_reward_component("mock")
    except UnknownRewardComponentError:
        print("  ✓ unregistered cleanly")

    print("\nSmoke test passed.")
