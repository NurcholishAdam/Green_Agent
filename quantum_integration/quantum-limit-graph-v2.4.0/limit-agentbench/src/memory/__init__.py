# src/memory/__init__.py

"""
Memory modules for Green_Agent
==============================

This package provides two complementary memory backends plus an optional
Supermemory integration layer.

Core backends
-------------
- :class:`RunMemory` — high-level, trend-aware memory across runs.
  Tracks performance metrics, generates meta-policies, and supports
  long-context reasoning. Backed by ``run_memory.json`` by default.
- :class:`EpisodicMemory` — lightweight JSON-backed store for individual
  episodes with a bounded ring-buffer. Backed by ``memory/memory_store.json``
  by default.

Supermemory integration (optional)
----------------------------------
- :class:`SupermemoryConfig` — frozen config for the Supermemory adapter.
- :class:`SupermemoryAdapter` — bridge to the Supermemory service.
- :class:`BoundedRecall` — top-k, filtered, token-bounded recall.
- :class:`WriteGovernor` — capability-based write approval + audit log.
- :class:`MemoryBenchmark` — paired without-memory vs. with-memory harness.
- Schemas: :class:`DecisionRecord`, :class:`PolicyRecord`,
  :class:`IncidentRecord`, :class:`OutcomeRecord`, :class:`TruthLevel`.

Both core backends share the same design conventions:

- Thread-safe accumulators (``RLock``).
- Bounded history / ring-buffers.
- Strict / non-strict handling of corrupt files.
- Structured serialization (``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json``).
- Context-manager support for scoped sessions.
- Custom ``ValueError`` subclasses for narrow exception handling.

The Supermemory layer follows the same conventions and is guarded so that a
missing ``supermemory`` SDK only disables the integration, not the package.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Optional, Type

# --------------------------------------------------------------------------- #
# Core backends (always present)
# --------------------------------------------------------------------------- #
from .run_memory import (
    MemoryConfig,
    RunMemory,
    RunMemoryError,
    RunSample,
)

from .episodic_memory import (
    DEFAULT_MAX_EPISODES,
    DEFAULT_RECENT_N,
    MEMORY_FILE,
    EpisodeEntry,
    EpisodicMemory,
    EpisodicMemoryError,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Supermemory integration (optional, guarded)
# --------------------------------------------------------------------------- #
MEMORY_AVAILABILITY: Dict[str, bool] = {
    "run_memory": True,
    "episodic_memory": True,
    "supermemory": False,
}

try:
    from .supermemory_config import (
        SupermemoryConfig,
        SupermemoryConfigError,
    )
    from .memory_schemas import (
        DecisionRecord,
        IncidentRecord,
        MemorySchemaError,
        OutcomeRecord,
        PolicyRecord,
        TruthLevel,
    )
    from .supermemory_adapter import (
        SupermemoryAdapter,
        SupermemoryAdapterError,
        _SUPERMEMORY_AVAILABLE,
    )
    from .bounded_recall import (
        BoundedRecall,
        BoundedRecallConfig,
        BoundedRecallError,
        RecallBundle,
    )
    from .write_governor import (
        ApprovalDecision,
        WriteGovernor,
        WriteGovernorConfig,
        WriteGovernorError,
    )
    from .memory_benchmark import (
        BenchmarkReport,
        MemoryBenchmark,
        MemoryBenchmarkConfig,
        MemoryBenchmarkError,
    )

    MEMORY_AVAILABILITY["supermemory"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("Supermemory integration unavailable: %s", exc)

    SupermemoryConfig = None  # type: ignore[assignment,misc]
    SupermemoryConfigError = None  # type: ignore[assignment,misc]
    DecisionRecord = None  # type: ignore[assignment,misc]
    IncidentRecord = None  # type: ignore[assignment,misc]
    MemorySchemaError = None  # type: ignore[assignment,misc]
    OutcomeRecord = None  # type: ignore[assignment,misc]
    PolicyRecord = None  # type: ignore[assignment,misc]
    TruthLevel = None  # type: ignore[assignment,misc]
    SupermemoryAdapter = None  # type: ignore[assignment,misc]
    SupermemoryAdapterError = None  # type: ignore[assignment,misc]
    _SUPERMEMORY_AVAILABLE = False
    BoundedRecall = None  # type: ignore[assignment,misc]
    BoundedRecallConfig = None  # type: ignore[assignment,misc]
    BoundedRecallError = None  # type: ignore[assignment,misc]
    RecallBundle = None  # type: ignore[assignment,misc]
    ApprovalDecision = None  # type: ignore[assignment,misc]
    WriteGovernor = None  # type: ignore[assignment,misc]
    WriteGovernorConfig = None  # type: ignore[assignment,misc]
    WriteGovernorError = None  # type: ignore[assignment,misc]
    BenchmarkReport = None  # type: ignore[assignment,misc]
    MemoryBenchmark = None  # type: ignore[assignment,misc]
    MemoryBenchmarkConfig = None  # type: ignore[assignment,misc]
    MemoryBenchmarkError = None  # type: ignore[assignment,misc]

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
    # supermemory integration
    "SupermemoryConfig",
    "SupermemoryConfigError",
    "SupermemoryAdapter",
    "SupermemoryAdapterError",
    "BoundedRecall",
    "BoundedRecallConfig",
    "BoundedRecallError",
    "RecallBundle",
    "WriteGovernor",
    "WriteGovernorConfig",
    "WriteGovernorError",
    "ApprovalDecision",
    "MemoryBenchmark",
    "MemoryBenchmarkConfig",
    "MemoryBenchmarkError",
    "BenchmarkReport",
    "DecisionRecord",
    "PolicyRecord",
    "IncidentRecord",
    "OutcomeRecord",
    "TruthLevel",
    "MemorySchemaError",
    # registry helpers
    "MEMORY_REGISTRY",
    "MEMORY_AVAILABILITY",
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
_ErrorTuple: tuple = (RunMemoryError, EpisodicMemoryError)

# Extend the unified error base with the Supermemory errors when available.
if MEMORY_AVAILABILITY.get("supermemory"):
    _ErrorTuple = _ErrorTuple + (  # type: ignore[assignment]
        SupermemoryConfigError,
        SupermemoryAdapterError,
        BoundedRecallError,
        WriteGovernorError,
        MemoryBenchmarkError,
        MemorySchemaError,
    )

MemoryErrorBase = _ErrorTuple

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

# Register the Supermemory components only when they imported successfully.
if MEMORY_AVAILABILITY.get("supermemory"):
    MEMORY_REGISTRY.update({
        "supermemory": SupermemoryAdapter,
        "supermemory_adapter": SupermemoryAdapter,
        "recall": BoundedRecall,
        "bounded_recall": BoundedRecall,
        "governor": WriteGovernor,
        "write_governor": WriteGovernor,
        "benchmark": MemoryBenchmark,
        "memory_benchmark": MemoryBenchmark,
    })


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
    """Return the memory class registered under ``name``."""
    try:
        return MEMORY_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown memory '{name}'. "
            f"Available: {sorted(MEMORY_REGISTRY)}"
        ) from exc


def list_memories() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {name: cls.__name__ for name, cls in MEMORY_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_MEMORY") == "1":
    _missing = [k for k, ok in MEMORY_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Memory sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(MEMORY_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Memory component '%s' (%s) OK.", _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Memory component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
