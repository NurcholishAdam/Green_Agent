# src/r1/__init__.py

"""
Reinforcement Learning Subsystem (R1)
=====================================

Energy-aware RL components for adaptive policy tuning.

Components
----------
- :class:`CentralCoordinator` — global sustainability ranking.
- :class:`EnergyRLAgent` — Q-learning for energy policy weights.
- :class:`PPOAgent` / :class:`PPOPolicy` — **discrete-action** PPO.
- :class:`QLearningAgent` — tabular Q-learning with persistence.
- :class:`QMemory` — JSON-backed Q-bias memory.
- :class:`GreenRewardModel` — multi-objective reward function.
- :class:`RewardNormalizer` — running mean/std normalization.
- :class:`RLStorage` — atomic JSON persistence for Q-tables.

Registry
--------
The package exposes a name-based registry so callers can discover or
instantiate components by string:

- :data:`RL_REGISTRY` — read-only ``Mapping[str, type]``.
- :func:`register_rl_component` — add or replace an entry.
- :func:`unregister_rl_component` — remove an entry (test isolation).
- :func:`get_rl_component` — look up the class by name.
- :func:`instantiate_rl_component` — look up *and* instantiate.
- :func:`list_rl_components` — ``{name: class_name}`` snapshot.
- :data:`RL_CONFIG_REGISTRY` — read-only ``Mapping[str, type]``.
- :data:`RL_ERROR_REGISTRY` — read-only ``Mapping[str, type]``.
- :func:`get_rl_config` / :func:`get_rl_error` — lookups by name.
- :func:`validate_rl_registry` — verify every entry is a class.

Torch
-----
:data:`TORCH_AVAILABLE` reports whether PyTorch is importable in the
current process. When ``False``, :class:`PPOAgent` and :class:`PPOPolicy`
cannot be constructed but the rest of the package works normally.

Migration
---------
The ``GREEN_AGENT_VALIDATE_RL`` environment variable no longer triggers
any import-time action. Callers that relied on it should call
:func:`validate_rl_registry` explicitly (e.g. from a pytest fixture).

Version
-------
:data:`VERSION` (PEP 440 string); mirrored as ``__version__``.
"""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Any

from .centralized_coordinator import (
    CentralCoordinator,
    CoordinatorConfig,
    CoordinatorError,
)
from .energy_rl_agent import (
    EnergyRLAgent,
    EnergyRLConfig,
    EnergyRLError,
)
from .ppo_agent import (
    PPOAgent,
    PPOAgentConfig,
    PPOAgentError,
    _TORCH_AVAILABLE as TORCH_AVAILABLE,
)
from .ppo_policy import (
    PPOPolicy,
    PPOPolicyConfig,
    PPOPolicyError,
)
from .q_learning import (
    QLearningAgent,
    QLearningConfig,
    QLearningError,
)
from .q_memory import QMemory, QMemoryConfig, QMemoryError
from .reward_model import (
    GreenRewardModel,
    RewardModelConfig,
    RewardModelError,
)
from .reward_normalizer import (
    RewardNormalizer,
    RewardNormalizerConfig,
    RewardNormalizerError,
)
from .rl_storage import (
    RLStorage,
    RLStorageConfig,
    RLStorageError,
)

logger = logging.getLogger(__name__)

__all__ = [
    # ---- Version ----
    "VERSION",
    "__version__",
    # ---- Torch availability ----
    "TORCH_AVAILABLE",
    # ---- Components ----
    "CentralCoordinator",
    "CoordinatorConfig",
    "CoordinatorError",
    "EnergyRLAgent",
    "EnergyRLConfig",
    "EnergyRLError",
    "PPOAgent",
    "PPOAgentConfig",
    "PPOAgentError",
    "PPOPolicy",
    "PPOPolicyConfig",
    "PPOPolicyError",
    "QLearningAgent",
    "QLearningConfig",
    "QLearningError",
    "QMemory",
    "QMemoryConfig",
    "QMemoryError",
    "GreenRewardModel",
    "RewardModelConfig",
    "RewardModelError",
    "RewardNormalizer",
    "RewardNormalizerConfig",
    "RewardNormalizerError",
    "RLStorage",
    "RLStorageConfig",
    "RLStorageError",
    # ---- Package errors ----
    "RLError",
    "RegistrationError",
    "UnknownComponentError",
    # ---- Registry API ----
    "RL_REGISTRY",
    "RL_CONFIG_REGISTRY",
    "RL_ERROR_REGISTRY",
    "register_rl_component",
    "unregister_rl_component",
    "get_rl_component",
    "instantiate_rl_component",
    "list_rl_components",
    "validate_rl_registry",
    "get_rl_config",
    "get_rl_error",
]

VERSION = "2.0.0"
__version__ = VERSION

# Deprecated private alias — prefer ``TORCH_AVAILABLE``. Not in ``__all__``.
_TORCH_AVAILABLE = TORCH_AVAILABLE


# --------------------------------------------------------------------------- #
# Package errors
# --------------------------------------------------------------------------- #
class RLError(ValueError):
    """Base error for the r1 package registry."""


class RegistrationError(RLError):
    """
    Raised by :func:`register_rl_component` on name conflict or invalid
    input. Subclass of :class:`ValueError` for backward compatibility.
    """


class UnknownComponentError(RLError, KeyError):
    """
    Raised by :func:`get_rl_component` / :func:`get_rl_config` /
    :func:`get_rl_error` when a name is not registered.

    Inherits from both :class:`RLError` (a ``ValueError``) and
    :class:`KeyError`, so either ``except RLError`` or ``except
    KeyError`` catches it.
    """


# --------------------------------------------------------------------------- #
# Internal registries (mutable) + public read-only views
# --------------------------------------------------------------------------- #
_RL_REGISTRY: dict[str, type[Any]] = {
    "coordinator": CentralCoordinator,
    "energy_rl":   EnergyRLAgent,
    "ppo_agent":   PPOAgent,
    "ppo_policy":  PPOPolicy,
    "q_learning":  QLearningAgent,
    "q_memory":    QMemory,
    "reward":      GreenRewardModel,
    "normalizer":  RewardNormalizer,
    "storage":     RLStorage,
}

_RL_CONFIG_REGISTRY: dict[str, type[Any]] = {
    "coordinator": CoordinatorConfig,
    "energy_rl":   EnergyRLConfig,
    "ppo_agent":   PPOAgentConfig,
    "ppo_policy":  PPOPolicyConfig,
    "q_learning":  QLearningConfig,
    "q_memory":    QMemoryConfig,
    "reward":      RewardModelConfig,
    "normalizer":  RewardNormalizerConfig,
    "storage":     RLStorageConfig,
}

_RL_ERROR_REGISTRY: dict[str, type[Any]] = {
    "coordinator": CoordinatorError,
    "energy_rl":   EnergyRLError,
    "ppo_agent":   PPOAgentError,
    "ppo_policy":  PPOPolicyError,
    "q_learning":  QLearningError,
    "q_memory":    QMemoryError,
    "reward":      RewardModelError,
    "normalizer":  RewardNormalizerError,
    "storage":     RLStorageError,
}

RL_REGISTRY = MappingProxyType(_RL_REGISTRY)
RL_CONFIG_REGISTRY = MappingProxyType(_RL_CONFIG_REGISTRY)
RL_ERROR_REGISTRY = MappingProxyType(_RL_ERROR_REGISTRY)


# --------------------------------------------------------------------------- #
# Name normalisation
# --------------------------------------------------------------------------- #
def _normalise_name(name: Any) -> str:
    """Strip and lowercase; raise :class:`RegistrationError` on bad input."""
    if not isinstance(name, str):
        raise RegistrationError(
            f"name must be a str, got {type(name).__name__}."
        )
    key = name.strip().lower()
    if not key:
        raise RegistrationError("name must be a non-empty string.")
    return key


# --------------------------------------------------------------------------- #
# Component registration
# --------------------------------------------------------------------------- #
def register_rl_component(
    name: str,
    cls: type[Any],
    *,
    config: type[Any] | None = None,
    error: type[Any] | None = None,
    overwrite: bool = False,
) -> None:
    """
    Register an RL component class under ``name``.

    ``name`` is stripped and lower-cased before storage, so
    ``register(" PPO ", X)`` and ``get("ppo")`` refer to the same entry.

    Parameters
    ----------
    name : str
        Unique (case-insensitive) component name.
    cls : type
        Component class.
    config : type, optional
        Corresponding ``*Config`` class for :func:`get_rl_config`.
    error : type, optional
        Corresponding ``*Error`` class for :func:`get_rl_error`.
    overwrite : bool, default False
        If True, replace an existing registration. Otherwise a
        conflicting name raises :class:`RegistrationError`.

    Raises
    ------
    TypeError
        If ``cls`` / ``config`` / ``error`` is not a class.
    RegistrationError
        If ``name`` is invalid or the entry exists and ``overwrite=False``.
    """
    key = _normalise_name(name)

    for label, value in (
        ("cls", cls), ("config", config), ("error", error),
    ):
        if value is not None and not isinstance(value, type):
            raise TypeError(
                f"{label} must be a class, got {type(value).__name__}."
            )

    existing = _RL_REGISTRY.get(key)
    if existing is not None and existing is not cls and not overwrite:
        raise RegistrationError(
            f"RL component {key!r} already registered to "
            f"{existing.__name__}."
        )

    _RL_REGISTRY[key] = cls
    if config is not None:
        _RL_CONFIG_REGISTRY[key] = config
    if error is not None:
        _RL_ERROR_REGISTRY[key] = error

    logger.debug(
        "registered rl component %s -> %s", key, cls.__name__
    )


def unregister_rl_component(name: str) -> bool:
    """
    Remove a registered component (and any associated config/error).

    Returns True if the component existed. Useful for test isolation::

        register_rl_component("mock", MockAgent)
        try:
            ...
        finally:
            unregister_rl_component("mock")
    """
    try:
        key = _normalise_name(name)
    except RegistrationError:
        return False

    existed = _RL_REGISTRY.pop(key, None) is not None
    _RL_CONFIG_REGISTRY.pop(key, None)
    _RL_ERROR_REGISTRY.pop(key, None)
    return existed


def get_rl_component(name: str) -> type[Any]:
    """
    Return the **class** registered under ``name`` (case-insensitive).

    Returns the class, not an instance. Use
    :func:`instantiate_rl_component` if you want an instance.

    Raises
    ------
    UnknownComponentError
        If the name is not registered (also a ``KeyError``).
    """
    try:
        key = _normalise_name(name)
    except RegistrationError as exc:
        raise UnknownComponentError(
            f"unknown RL component {name!r}: {exc}"
        ) from None

    try:
        return _RL_REGISTRY[key]
    except KeyError:
        raise UnknownComponentError(
            f"unknown RL component {name!r}. "
            f"Available: {sorted(_RL_REGISTRY)}"
        ) from None


def instantiate_rl_component(name: str, **kwargs: Any) -> Any:
    """
    Instantiate a registered component by name.

    ``kwargs`` are forwarded to the component's ``__init__``.
    """
    cls = get_rl_component(name)
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise RLError(
            f"could not instantiate {name!r} with kwargs "
            f"{sorted(kwargs)}: {exc}"
        ) from exc


def list_rl_components() -> dict[str, str]:
    """Return ``{registered_name: class.__name__}`` for all entries."""
    return {n: c.__name__ for n, c in _RL_REGISTRY.items()}


def validate_rl_registry() -> dict[str, str]:
    """
    Verify every registry entry is a class and return
    ``{name: "module.Class"}``.

    Call from a pytest fixture or CLI smoke test — never at import time.

    Raises
    ------
    RLError
        If any entry is not a class.
    """
    report: dict[str, str] = {}
    for name, cls in _RL_REGISTRY.items():
        if not isinstance(cls, type):
            raise RLError(
                f"registry entry {name!r} is not a class "
                f"({type(cls).__name__})."
            )
        report[name] = f"{cls.__module__}.{cls.__qualname__}"
    return report


# --------------------------------------------------------------------------- #
# Config / error lookups
# --------------------------------------------------------------------------- #
def get_rl_config(name: str) -> type[Any]:
    """
    Return the ``*Config`` class for a component name.

    Raises :class:`UnknownComponentError` if the component is not
    registered or if it was registered without a config class.
    """
    try:
        key = _normalise_name(name)
    except RegistrationError as exc:
        raise UnknownComponentError(
            f"unknown RL component {name!r}: {exc}"
        ) from None

    if key not in _RL_REGISTRY:
        raise UnknownComponentError(
            f"unknown RL component {name!r}. "
            f"Available: {sorted(_RL_REGISTRY)}"
        )

    try:
        return _RL_CONFIG_REGISTRY[key]
    except KeyError:
        raise UnknownComponentError(
            f"component {name!r} is registered but has no config class. "
            f"Register one with register_rl_component(..., config=...)."
        ) from None


def get_rl_error(name: str) -> type[Any]:
    """
    Return the ``*Error`` class for a component name.

    Raises :class:`UnknownComponentError` if the component is not
    registered or if it was registered without an error class.
    """
    try:
        key = _normalise_name(name)
    except RegistrationError as exc:
        raise UnknownComponentError(
            f"unknown RL component {name!r}: {exc}"
        ) from None

    if key not in _RL_REGISTRY:
        raise UnknownComponentError(
            f"unknown RL component {name!r}. "
            f"Available: {sorted(_RL_REGISTRY)}"
        )

    try:
        return _RL_ERROR_REGISTRY[key]
    except KeyError:
        raise UnknownComponentError(
            f"component {name!r} is registered but has no error class. "
            f"Register one with register_rl_component(..., error=...)."
        ) from None


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("VERSION    :", VERSION)
    print("__version__:", __version__)
    print("torch      :", TORCH_AVAILABLE)

    # ---- __all__ resolves ---------------------------------------- #
    missing = [n for n in __all__ if n not in globals()]
    assert not missing, f"missing from __all__: {missing}"
    print(f"__all__     : {len(__all__)} names all resolve")

    # ---- Registry shape ------------------------------------------ #
    report = validate_rl_registry()
    assert len(report) == 9, report
    print("validate_rl_registry: 9 entries")
    for name, path in sorted(report.items()):
        print(f"  {name:<14} -> {path}")

    # ---- Read-only enforcement ----------------------------------- #
    try:
        RL_REGISTRY["evil"] = int  # type: ignore[index]
    except TypeError as exc:
        print("read-only   :", exc)

    # ---- Registry lookups ---------------------------------------- #
    assert get_rl_component("ppo_agent") is PPOAgent
    assert get_rl_component(" PPO_AGENT ") is PPOAgent
    assert get_rl_config("ppo_agent") is PPOAgentConfig
    assert get_rl_error("ppo_agent") is PPOAgentError
    print("lookups     : OK (case/whitespace insensitive)")

    # ---- instantiate --------------------------------------------- #
    coord = instantiate_rl_component("coordinator")
    assert isinstance(coord, CentralCoordinator)
    reward = instantiate_rl_component("reward", strict=False)
    assert isinstance(reward, GreenRewardModel)
    print("instantiate : OK")

    # ---- Unknown name errors ------------------------------------- #
    for fn in (
        lambda: get_rl_component("nope"),
        lambda: get_rl_config("nope"),
        lambda: get_rl_error("nope"),
    ):
        try:
            fn()
        except RLError as exc:
            assert isinstance(exc, KeyError)
            print("unknown     :", exc)

    # ---- Custom registration + unregister ------------------------ #
    class MockAgent:
        def __init__(self, value: int = 0):
            self.value = value

    class MockConfig:
        pass

    class MockError(ValueError):
        pass

    register_rl_component(
        "Mock", MockAgent, config=MockConfig, error=MockError,
    )
    assert get_rl_component("mock") is MockAgent
    assert get_rl_config("mock") is MockConfig
    assert get_rl_error("mock") is MockError
    inst = instantiate_rl_component("mock", value=42)
    assert inst.value == 42
    print("custom      : registered + instantiated")

    # Overwrite guard
    try:
        register_rl_component("mock", int)
    except RegistrationError as exc:
        print("overwrite   :", exc)

    # Overwrite with flag
    register_rl_component("mock", int, overwrite=True)
    assert get_rl_component("mock") is int
    print("overwrite=  : allowed with flag")

    # Unregister cleans up all three registries
    assert unregister_rl_component("mock") is True
    try:
        get_rl_component("mock")
    except UnknownComponentError:
        pass
    else:
        raise AssertionError("mock should be gone")
    print("unregister  : removed from all registries")

    # ---- TypeError for non-class --------------------------------- #
    for label, kwargs in (
        ("cls",    dict(cls=42)),
        ("config", dict(cls=MockAgent, config=42)),
        ("error",  dict(cls=MockAgent, error="boom")),
    ):
        try:
            register_rl_component(f"bad_{label}", **kwargs)
        except TypeError as exc:
            print(f"bad {label:<6}:", exc)

    # ---- list_rl_components -------------------------------------- #
    listing = list_rl_components()
    assert "ppo_agent" in listing
    assert listing["ppo_agent"] == "PPOAgent"
    print("list        :", len(listing), "components")

    # ---- Environment variable no longer triggers validation ------ #
    import os
    os.environ["GREEN_AGENT_VALIDATE_RL"] = "1"
    try:
        # Re-running validate is a no-op call; the env var does nothing.
        # (The dead import-time block was removed entirely.)
        assert validate_rl_registry()  # non-empty
        print("env var     : no import-time side effect (removed)")
    finally:
        del os.environ["GREEN_AGENT_VALIDATE_RL"]

    print("\nSmoke test passed.")
