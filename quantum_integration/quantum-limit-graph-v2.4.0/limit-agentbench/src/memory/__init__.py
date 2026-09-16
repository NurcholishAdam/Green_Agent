# metrics/../memory/__init__.py

"""
Memory modules for sustained reflection across runs.

This package provides two complementary memory backends:

- :class:`RunMemory` — high-level, trend-aware memory across runs.
  Tracks performance metrics, generates meta-policies, and supports
  long-context reasoning. Backed by ``run_memory.json`` by default.

- :class:`EpisodicMemory` — lightweight JSON-backed store for individual
  episodes with a bounded ring-buffer. Backed by ``memory/memory_store.json``
  by default.

Both backends share the same design conventions:

- Thread-safe accumulators (``RLock``).
- Bounded history / ring-buffers.
- Strict / non-strict handling of corrupt files.
- Structured serialization (``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``).
- Context-manager support for scoped sessions.
- Custom ``ValueError`` subclasses for narrow exception handling.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Optional, Type

# --------------------------------------------------------------------------- #
# Run memory (performance across runs, meta-policies)
# --------------------------------------------------------------------------- #
from .run_memory import (
    RunMemory,
    RunMemoryError,
    RunSample,
    MemoryConfig,
)

# --------------------------------------------------------------------------- #
# Episodic memory (single-episode store)
# --------------------------------------------------------------------------- #
from .episodic_memory import (
    EpisodicMemory,
    EpisodicMemoryError,
    EpisodeEntry,
    MEMORY_FILE,
    DEFAULT_MAX_EPISODES,
    DEFAULT_RECENT_N,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # high-level run memory
    "RunMemory",
    "RunMemoryError",
    "RunSample",
    "MemoryConfig",
    # episodic memory
    "EpisodicMemory",
    "EpisodicMemoryError",
    "EpisodeEntry",
    "MEMORY_FILE",
    "DEFAULT_MAX_EPISODES",
    "DEFAULT_RECENT_N",
    # registry helpers
    "MEMORY_REGISTRY",
    "get_memory_class",
    "list_memories",
    "register_memory",
    # error taxonomy
    "MemoryErrorBase",
]

# --------------------------------------------------------------------------- #
# Unified error base
# --------------------------------------------------------------------------- #
# Convenience base so callers can catch either backend's errors with one
# ``except`` clause without importing concrete modules.
MemoryErrorBase = (RunMemoryError, EpisodicMemoryError)

# --------------------------------------------------------------------------- #
# Memory registry
# --------------------------------------------------------------------------- #
# Central lookup so benchmarking / instrumentation / dashboard layers can
# resolve a memory backend by name without importing concrete classes.
MEMORY_REGISTRY: Dict[str, Type[Any]] = {
    "run": RunMemory,
    "runs": RunMemory,
    "run_memory": RunMemory,
    "episodic": EpisodicMemory,
    "episodes": EpisodicMemory,
    "episodic_memory": EpisodicMemory,
}


def register_memory(name: str, cls: Type[Any]) -> None:
    """
    Register a memory backend class under ``name``.

    Raises
    ------
    ValueError
        If ``name`` is empty or already registered to a different class.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Memory name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = MEMORY_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Memory '{name}' already registered to {existing.__name__}."
        )
    MEMORY_REGISTRY[name] = cls
    logger.debug("Registered memory '%s' -> %s", name, cls.__name__)


def get_memory_class(name: str) -> Type[Any]:
    """
    Return the memory class registered under ``name``.

    Raises
    ------
    KeyError
        If ``name`` is not registered.
    """
    try:
        return MEMORY_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown memory '{name}'. Available: {sorted(MEMORY_REGISTRY)}"
        ) from exc


def list_memories() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {name: cls.__name__ for name, cls in MEMORY_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time sanity check (only when explicitly enabled)
# --------------------------------------------------------------------------- #
# Skips by default so imports stay cheap; set GREEN_AGENT_VALIDATE_MEMORY=1
# in CI to fail-fast if any registered backend cannot be constructed.
if _os.environ.get("GREEN_AGENT_VALIDATE_MEMORY") == "1":
    for _name, _cls in MEMORY_REGISTRY.items():
        try:
            # Validate with in-memory file paths to avoid touching the FS.
            if _cls is RunMemory:
                _instance = _cls(memory_file=":memory:", auto_load=False, autosave=False)
            elif _cls is EpisodicMemory:
                _instance = _cls(memory_file=":memory:", autosave=False)
            else:
                _instance = _cls()  # custom backend assumed constructor-safe
            logger.info("Memory '%s' (%s) instantiated OK.", _name, _cls.__name__)
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Memory '%s' (%s) failed to instantiate.", _name, _cls.__name__
            )
            raise
