# src/evaluation/request_loader.py

"""
Evaluation-request loader and validator.
========================================

Loads an ``eval-request-vN.json`` payload, validates it against the
published schema, and translates it into the flat, adapter-ready config
dict that :func:`src.integrations.create_agent_adapter` expects.

Beyond schema validation, this module enforces three additional
invariants that JSON Schema cannot express:

1. **No inert enhancement keys.** Every ``enhancement_config.<block>``
   must contain at least one key the target adapter actually reads.
   A block that is present but empty, or whose keys are all unknown,
   raises :class:`EvalRequestError`.
2. **No silent strategy downgrade.** For each framework where
   ``use_enhancements=True``, the corresponding ``*_strategy_aware``
   (and, for LangChain / LimitGraph, the ``*_strategy_executor``) must
   be present. Without them the adapter downgrades to legacy and the
   distillation optimizer trains on noise.
3. **4-element ``modp_weights``.** The enhanced MODP reward path
   validates ``mw.shape == (4,)``; a 5-element array raises at run
   time. This loader rejects it at load time with a clear message.

Compatibility
-------------
- ``load_request(data)`` accepts either a ``Path`` (reads the file) or
  a ``Mapping``.
- ``to_adapter_config(request, framework)`` produces the flat config
  the adapter's ``__init__`` accepts — with ``distillation_lr``,
  ``gating_lr``, top-level ``modp_weights``, and ``seed`` promoted from
  the payload's structured form.
- ``validate_no_inert_enhancement_keys(request)`` is exposed separately
  so a caller can opt out of the strict check by loading with
  ``strict=False``.

Requires Python 3.8+.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple


logger = logging.getLogger(__name__)

__version__ = "1.0.0"

_SUPPORTED_SCHEMA_VERSIONS: Tuple[int, ...] = (1,)
_KNOWN_FRAMEWORKS: Tuple[str, ...] = (
    "autogen", "langchain", "crewai", "limit_graph",
)

# Per-framework: keys the adapter actually reads from enhancement_config.
# Used by the "no inert config" check. Sourced from the enhanced adapter
# implementations (`_validate_config`, `_initialize_enhancements`).
_FRAMEWORK_READ_KEYS: Dict[str, Tuple[str, ...]] = {
    "autogen": (
        "distillation", "rlhf", "limit_graph",
    ),
    "langchain": (
        "distillation", "rlhf", "limit_graph",
    ),
    "crewai": (
        "distillation", "rlhf", "limit_graph",
    ),
    "limit_graph": (
        "distillation", "rlhf", "limit_graph", "flexgen",
    ),
}

# Keys the distillation optimizer's ``_validate_config`` reads. Anything
# else under `enhancement_config.distillation` is silently ignored.
_DISTILLATION_KNOWN_KEYS: Tuple[str, ...] = (
    "distillation_lr", "gating_lr", "epsilon",
    "distill_weight", "rl_weight",
    "train_every", "batch_size", "replay_size",
    "seed", "gate_seed",
)

_RLHF_KNOWN_KEYS: Tuple[str, ...] = ("human_feedback_score",)

_LIMIT_GRAPH_KNOWN_KEYS: Tuple[str, ...] = ("graph_id", "graph_metrics")

_FLEXGEN_KNOWN_KEYS: Tuple[str, ...] = (
    "allow_flexgen_delegation", "action_mapping",
)

# Per-block "known keys" for the inert-config check.
_BLOCK_KNOWN_KEYS: Dict[str, Tuple[str, ...]] = {
    "distillation": _DISTILLATION_KNOWN_KEYS,
    "rlhf": _RLHF_KNOWN_KEYS,
    "limit_graph": _LIMIT_GRAPH_KNOWN_KEYS,
    "flexgen": _FLEXGEN_KNOWN_KEYS,
}


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EvalRequestError(ValueError):
    """Raised for invalid evaluation-request payloads."""


# --------------------------------------------------------------------------- #
# Validated request
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EvalRequest:
    """Fully validated evaluation request."""

    query_id: str
    schema_version: int
    provenance: Mapping[str, Any]
    task: Mapping[str, Any]
    objectives: Mapping[str, Any]
    constraints: Mapping[str, Any]
    execution: Mapping[str, Any]
    reporting: Mapping[str, Any]
    framework_config: Mapping[str, Mapping[str, Any]]
    modp_weights: Tuple[float, ...]
    enhancement_config: Mapping[str, Any]
    raw: Mapping[str, Any] = field(repr=False)

    # ---------------------------------------------------------------- helpers
    @property
    def seed(self) -> Optional[int]:
        """Convenience: ``execution.seed`` or the distillation seed."""
        seed = self.execution.get("seed")
        if isinstance(seed, int) and not isinstance(seed, bool):
            return seed
        seed = self.enhancement_config.get("distillation", {}).get("seed")
        if isinstance(seed, int) and not isinstance(seed, bool):
            return seed
        return None

    @property
    def deterministic(self) -> bool:
        return bool(self.execution.get("deterministic", False))

    def enabled_frameworks(self) -> Tuple[str, ...]:
        """Return the frameworks whose ``use_enhancements`` is True."""
        return tuple(
            name
            for name, cfg in self.framework_config.items()
            if isinstance(cfg, Mapping) and cfg.get("use_enhancements")
        )


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise EvalRequestError(
            f"{name} must be a non-empty string, got {value!r}."
        )
    return value


def _require_mapping(name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvalRequestError(
            f"{name} must be a Mapping, got {type(value).__name__}."
        )
    return value


def _require_positive_number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvalRequestError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if fv <= 0.0:
        raise EvalRequestError(f"{name} must be > 0, got {fv}.")
    return fv


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvalRequestError(
            f"{name} must be an int, got {type(value).__name__}."
        )
    if value <= 0:
        raise EvalRequestError(f"{name} must be > 0, got {value}.")
    return value


def _require_unit_interval(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvalRequestError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not 0.0 <= fv <= 1.0:
        raise EvalRequestError(
            f"{name} must be in [0, 1], got {fv}."
        )
    return fv


# --------------------------------------------------------------------------- #
# Specific block validations
# --------------------------------------------------------------------------- #
def _validate_provenance(block: Any) -> Mapping[str, Any]:
    _require_mapping("provenance", block)
    allowed_sources = {
        "measured", "estimated", "simulated",
        "federated", "external", "unknown",
    }
    if block.get("source") not in allowed_sources:
        raise EvalRequestError(
            f"provenance.source must be one of {sorted(allowed_sources)}, "
            f"got {block.get('source')!r}."
        )
    allowed_trust = {"high", "medium", "low"}
    if block.get("trust_level") not in allowed_trust:
        raise EvalRequestError(
            f"provenance.trust_level must be one of "
            f"{sorted(allowed_trust)}, got {block.get('trust_level')!r}."
        )
    _require_mapping("provenance.units", block.get("units", {}))
    if not isinstance(block.get("simulated"), bool):
        raise EvalRequestError(
            "provenance.simulated must be a bool, got "
            f"{type(block.get('simulated')).__name__}."
        )
    return block


def _validate_modp_weights(block: Any) -> Tuple[float, ...]:
    if not isinstance(block, (list, tuple)):
        raise EvalRequestError(
            f"modp_weights must be a list, got {type(block).__name__}."
        )
    if len(block) != 4:
        raise EvalRequestError(
            f"modp_weights must have exactly 4 elements (carbon, energy, "
            f"latency, cost), got {len(block)}."
        )
    out: List[float] = []
    total = 0.0
    for i, v in enumerate(block):
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise EvalRequestError(
                f"modp_weights[{i}] must be numeric, got {v!r}."
            )
        fv = float(v)
        if fv < 0.0:
            raise EvalRequestError(
                f"modp_weights[{i}] must be >= 0, got {fv}."
            )
        out.append(fv)
        total += fv
    if total <= 0.0:
        raise EvalRequestError(
            "modp_weights must have a positive sum."
        )
    return tuple(out)


def _validate_framework_config(
    block: Any,
    *,
    strict: bool,
) -> Mapping[str, Mapping[str, Any]]:
    """
    Validate per-framework config and enforce strategy-aware flags when
    ``use_enhancements=True``.

    A framework not listed in the payload is absent from
    ``enabled_frameworks()`` — the caller is responsible for choosing
    which adapter to build.
    """
    block = _require_mapping("framework_config", block)
    normalised: Dict[str, Dict[str, Any]] = {}
    for name, cfg in block.items():
        if name not in _KNOWN_FRAMEWORKS:
            raise EvalRequestError(
                f"framework_config has unknown framework {name!r}; "
                f"expected one of {sorted(_KNOWN_FRAMEWORKS)}."
            )
        cfg = _require_mapping(f"framework_config.{name}", cfg)
        if not isinstance(cfg.get("use_enhancements"), bool):
            raise EvalRequestError(
                f"framework_config.{name}.use_enhancements must be a bool."
            )
        if cfg["use_enhancements"] and strict:
            _check_strategy_aware(name, cfg)
        normalised[name] = dict(cfg)
    return normalised


def _check_strategy_aware(framework: str, cfg: Mapping[str, Any]) -> None:
    """
    Enforce ``*_strategy_aware`` and (for LangChain / LimitGraph) a
    callable executor when ``use_enhancements=True``.
    """
    if framework == "autogen":
        # The AutoGen adapter *can* honour execution strategy — no guard
        # required.
        return
    if framework == "crewai":
        if not cfg.get("crewai_strategy_aware"):
            raise EvalRequestError(
                "framework_config.crewai.use_enhancements=True requires "
                "crewai_strategy_aware=True (CrewAI cannot honour the "
                "strategy on its own; without the flag the optimizer "
                "trains on noise)."
            )
        return
    if framework == "langchain":
        if not cfg.get("langchain_strategy_aware"):
            raise EvalRequestError(
                "framework_config.langchain.use_enhancements=True requires "
                "langchain_strategy_aware=True."
            )
        if not cfg.get("langchain_strategy_executor"):
            raise EvalRequestError(
                "framework_config.langchain.use_enhancements=True requires "
                "a non-empty langchain_strategy_executor (callable "
                "reference) so the requested strategy is honoured."
            )
        return
    if framework == "limit_graph":
        if not cfg.get("limit_graph_strategy_aware"):
            raise EvalRequestError(
                "framework_config.limit_graph.use_enhancements=True "
                "requires limit_graph_strategy_aware=True."
            )
        if not cfg.get("limit_graph_strategy_executor"):
            raise EvalRequestError(
                "framework_config.limit_graph.use_enhancements=True "
                "requires a non-empty limit_graph_strategy_executor "
                "(callable reference)."
            )
        return


def _validate_enhancement_config(
    block: Any, *, strict: bool,
) -> Mapping[str, Any]:
    block = _require_mapping("enhancement_config", block)
    normalised: Dict[str, Any] = {}
    for name, cfg in block.items():
        if name not in _BLOCK_KNOWN_KEYS:
            raise EvalRequestError(
                f"enhancement_config.{name} is not a recognised "
                f"enhancement block; expected one of "
                f"{sorted(_BLOCK_KNOWN_KEYS)}."
            )
        cfg = _require_mapping(f"enhancement_config.{name}", cfg)
        if strict and not cfg:
            raise EvalRequestError(
                f"enhancement_config.{name} is present but empty; "
                f"remove it or populate at least one key."
            )
        if strict:
            _check_no_inert_keys(name, cfg)
        normalised[name] = dict(cfg)

    # Per-block content validation.
    if "distillation" in normalised:
        _validate_distillation_block(normalised["distillation"])
    if "rlhf" in normalised:
        _validate_rlhf_block(normalised["rlhf"])
    if "flexgen" in normalised:
        _validate_flexgen_block(normalised["flexgen"])
    return normalised


def _check_no_inert_keys(block: str, cfg: Mapping[str, Any]) -> None:
    """Every key in ``cfg`` must be read by the pipeline for ``block``."""
    known = set(_BLOCK_KNOWN_KEYS[block])
    unknown = sorted(set(cfg) - known)
    if unknown:
        raise EvalRequestError(
            f"enhancement_config.{block} has unknown keys {unknown}; "
            f"the pipeline reads only {sorted(known)}. Remove the "
            f"unrecognised keys or add them to _BLOCK_KNOWN_KEYS if a "
            f"future adapter starts consuming them."
        )


def _validate_distillation_block(cfg: Mapping[str, Any]) -> None:
    if "distillation_lr" in cfg:
        _require_positive_number(
            "distillation.distillation_lr", cfg["distillation_lr"],
        )
    if "gating_lr" in cfg:
        _require_positive_number(
            "distillation.gating_lr", cfg["gating_lr"],
        )
    if "epsilon" in cfg:
        _require_unit_interval(
            "distillation.epsilon", cfg["epsilon"],
        )
    for name in (
        "distill_weight", "rl_weight",
    ):
        if name in cfg:
            if isinstance(cfg[name], bool) or not isinstance(
                cfg[name], (int, float)
            ):
                raise EvalRequestError(
                    f"distillation.{name} must be numeric."
                )
            if float(cfg[name]) < 0.0:
                raise EvalRequestError(
                    f"distillation.{name} must be >= 0."
                )
    if (
        "distill_weight" in cfg
        and "rl_weight" in cfg
        and float(cfg["distill_weight"]) + float(cfg["rl_weight"]) <= 0
    ):
        raise EvalRequestError(
            "distillation.distill_weight + distillation.rl_weight "
            "must be > 0."
        )
    for name in ("train_every", "batch_size", "replay_size"):
        if name in cfg:
            _require_positive_int(f"distillation.{name}", cfg[name])
    for name in ("seed", "gate_seed"):
        if name in cfg and cfg[name] is not None:
            if isinstance(cfg[name], bool) or not isinstance(
                cfg[name], int
            ):
                raise EvalRequestError(
                    f"distillation.{name} must be an int or null."
                )


def _validate_rlhf_block(cfg: Mapping[str, Any]) -> None:
    if "human_feedback_score" in cfg:
        _require_unit_interval(
            "rlhf.human_feedback_score", cfg["human_feedback_score"],
        )


def _validate_flexgen_block(cfg: Mapping[str, Any]) -> None:
    if "allow_flexgen_delegation" in cfg:
        if not isinstance(cfg["allow_flexgen_delegation"], bool):
            raise EvalRequestError(
                "flexgen.allow_flexgen_delegation must be a bool."
            )
    if "action_mapping" in cfg:
        mapping = _require_mapping(
            "flexgen.action_mapping", cfg["action_mapping"],
        )
        allowed_targets = {
            "flexgen_low_precision", "flexgen_high_precision",
        }
        for precision, target in mapping.items():
            if target not in allowed_targets:
                raise EvalRequestError(
                    f"flexgen.action_mapping[{precision!r}] = {target!r} "
                    f"is not one of {sorted(allowed_targets)}."
                )


# --------------------------------------------------------------------------- #
# Public loader
# --------------------------------------------------------------------------- #
def load_request(
    source: Any,
    *,
    strict: bool = True,
) -> EvalRequest:
    """
    Load and validate an evaluation request.

    Parameters
    ----------
    source : Path | str | Mapping
        A ``Path`` (or ``str`` path) to a JSON file, or an already-parsed
        ``Mapping``.
    strict : bool, default True
        If True, apply the "no inert config" and "strategy-aware"
        invariants. If False, only structural validation runs — useful
        for inspecting a payload from a legacy or experimental pipeline.

    Raises
    ------
    EvalRequestError
        If the payload is structurally invalid or violates a strict
        invariant.
    """
    data = _read_source(source)

    version = data.get("schema_version")
    if not isinstance(version, int) or version not in (
        _SUPPORTED_SCHEMA_VERSIONS
    ):
        raise EvalRequestError(
            f"schema_version must be one of "
            f"{list(_SUPPORTED_SCHEMA_VERSIONS)}, got {version!r}."
        )

    query_id = _require_str("query_id", data.get("query_id"))

    provenance = _validate_provenance(data.get("provenance"))
    task = _require_mapping("task", data.get("task"))
    if "input" not in task or not isinstance(task["input"], Mapping):
        raise EvalRequestError("task.input must be a Mapping.")
    _require_str("task.input.prompt", task["input"].get("prompt"))

    objectives = _require_mapping("objectives", data.get("objectives", {}))
    _validate_objectives(objectives)

    constraints = _require_mapping(
        "constraints", data.get("constraints", {}),
    )
    _validate_constraints(constraints)

    execution = _require_mapping("execution", data.get("execution", {}))
    _require_positive_int("execution.runs", execution.get("runs", 1))
    if "deterministic" in execution and not isinstance(
        execution["deterministic"], bool
    ):
        raise EvalRequestError(
            "execution.deterministic must be a bool."
        )
    if execution.get("deterministic") and not isinstance(
        execution.get("seed"), int
    ):
        # The seed may be provided under distillation.seed instead.
        seed_elsewhere = (
            isinstance(data.get("enhancement_config", {})
                       .get("distillation", {})
                       .get("seed"), int)
        )
        if not seed_elsewhere:
            raise EvalRequestError(
                "execution.deterministic=True requires execution.seed "
                "(or enhancement_config.distillation.seed) so runs are "
                "reproducible."
            )

    reporting = _require_mapping("reporting", data.get("reporting", {}))

    framework_config = _validate_framework_config(
        data.get("framework_config", {}), strict=strict,
    )
    if strict and not framework_config:
        raise EvalRequestError(
            "framework_config must list at least one framework."
        )

    modp_weights = _validate_modp_weights(data.get("modp_weights"))

    enhancement_config = _validate_enhancement_config(
        data.get("enhancement_config", {}), strict=strict,
    )

    return EvalRequest(
        query_id=query_id,
        schema_version=version,
        provenance=provenance,
        task=task,
        objectives=objectives,
        constraints=constraints,
        execution=execution,
        reporting=reporting,
        framework_config=framework_config,
        modp_weights=modp_weights,
        enhancement_config=enhancement_config,
        raw=data,
    )


def _read_source(source: Any) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        return source
    if isinstance(source, (str, Path)):
        path = Path(source)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise EvalRequestError(
                f"could not read {path}: {exc}"
            ) from exc
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise EvalRequestError(
                f"invalid JSON in {path}: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise EvalRequestError(
                f"root of {path} must be a JSON object, got "
                f"{type(data).__name__}."
            )
        return data
    raise EvalRequestError(
        f"source must be a Path, str, or Mapping, got "
        f"{type(source).__name__}."
    )


def _validate_objectives(obj: Mapping[str, Any]) -> None:
    for key in ("carbon", "energy", "latency", "cost"):
        if key in obj and obj[key] != "minimize":
            raise EvalRequestError(
                f"objectives.{key} must be 'minimize', got {obj[key]!r}."
            )
    if "success_gate" in obj and not isinstance(
        obj["success_gate"], bool
    ):
        raise EvalRequestError(
            "objectives.success_gate must be a bool."
        )


def _validate_constraints(cfg: Mapping[str, Any]) -> None:
    if "max_latency_sec" in cfg:
        _require_positive_number(
            "constraints.max_latency_sec", cfg["max_latency_sec"],
        )
    if "max_energy_wh" in cfg:
        _require_positive_number(
            "constraints.max_energy_wh", cfg["max_energy_wh"],
        )
    if "max_memory_mb" in cfg:
        _require_positive_number(
            "constraints.max_memory_mb", cfg["max_memory_mb"],
        )
    if "enforcement" in cfg:
        allowed = {"hard", "soft", "report-only"}
        if cfg["enforcement"] not in allowed:
            raise EvalRequestError(
                f"constraints.enforcement must be one of "
                f"{sorted(allowed)}, got {cfg['enforcement']!r}."
            )


# --------------------------------------------------------------------------- #
# Adapter-config translation
# --------------------------------------------------------------------------- #
def to_adapter_config(
    request: EvalRequest,
    framework: str,
) -> Dict[str, Any]:
    """
    Produce the flat config dict that
    :func:`src.integrations.create_agent_adapter` expects, for the given
    ``framework``.

    Raises
    ------
    EvalRequestError
        If ``framework`` is not enabled in the request, or is not a
        known framework name.
    """
    if framework not in _KNOWN_FRAMEWORKS:
        raise EvalRequestError(
            f"unknown framework {framework!r}; expected one of "
            f"{sorted(_KNOWN_FRAMEWORKS)}."
        )
    fw_cfg = request.framework_config.get(framework)
    if fw_cfg is None:
        raise EvalRequestError(
            f"framework {framework!r} is not present in framework_config."
        )

    # Start with the per-framework config verbatim; it already carries
    # the *_strategy_aware and *_strategy_executor flags.
    flat: Dict[str, Any] = dict(fw_cfg)

    # Promote top-level keys the adapters read directly.
    flat["modp_weights"] = list(request.modp_weights)

    # The runtime reads `seed` / `gate_seed` at the top level for
    # deterministic runs.
    seed = request.seed
    if seed is not None:
        flat.setdefault("seed", seed)
        flat.setdefault("gate_seed", seed)

    # Flatten the distillation sub-block into the top-level keys that
    # `DistillationOptimizer._validate_config` reads.
    dist = request.enhancement_config.get("distillation", {})
    for key in _DISTILLATION_KNOWN_KEYS:
        if key in dist:
            flat.setdefault(key, dist[key])

    # Pull the human-feedback prior from `enhancement_config.rlhf` to
    # the top level where the runtime reads it.
    rlhf = request.enhancement_config.get("rlhf", {})
    if "human_feedback_score" in rlhf:
        flat.setdefault(
            "human_feedback_score", rlhf["human_feedback_score"],
        )

    # Graph metrics live on `task.input.graph_metrics`; the runtime
    # reads `graph_metrics` from the top level.
    task_input = request.task.get("input", {})
    if isinstance(task_input, Mapping) and "graph_metrics" in task_input:
        flat.setdefault("graph_metrics", dict(task_input["graph_metrics"]))

    # Canonical defaults so the adapter sees a complete config.
    flat.setdefault("use_enhancements", bool(fw_cfg.get("use_enhancements")))
    flat.setdefault(
        "track_green_metrics",
        bool(request.reporting.get("return_metrics", True)),
    )
    flat.setdefault(
        "return_raw_outputs",
        bool(request.reporting.get("return_raw_outputs", False)),
    )

    return flat


def validate_no_inert_enhancement_keys(request: EvalRequest) -> None:
    """
    Verify that, for every framework whose ``use_enhancements`` is True,
    each ``enhancement_config`` block the adapter would consume is
    either absent or contains at least one key the adapter reads.

    Exposed separately so callers can enforce it even when the payload
    was loaded with ``strict=False``.
    """
    for framework in request.enabled_frameworks():
        expected_blocks = set(_FRAMEWORK_READ_KEYS.get(framework, ()))
        for block_name in expected_blocks:
            if block_name not in request.enhancement_config:
                continue
            block = request.enhancement_config[block_name]
            if not block:
                raise EvalRequestError(
                    f"framework {framework!r} has "
                    f"enhancement_config.{block_name} present but empty."
                )
            known = set(_BLOCK_KNOWN_KEYS[block_name])
            unknown = set(block) - known
            if unknown:
                raise EvalRequestError(
                    f"framework {framework!r} consumes "
                    f"enhancement_config.{block_name} but the block "
                    f"contains unknown keys {sorted(unknown)}."
                )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "EvalRequest",
    "EvalRequestError",
    "load_request",
    "to_adapter_config",
    "validate_no_inert_enhancement_keys",
    "__version__",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m evaluation.request_loader
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
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

    def _raises(exc_type, fn) -> bool:
        try:
            fn()
        except exc_type:
            return True
        except Exception:
            return False
        return False

    # ---- Minimal valid payload ----------------------------------- #
    valid_payload: Dict[str, Any] = {
        "schema_version": 1,
        "query_id": "test",
        "provenance": {
            "source": "measured",
            "trust_level": "high",
            "units": {"energy": "wh"},
            "simulated": False,
        },
        "task": {
            "type": "inference",
            "input": {"prompt": "hi", "token_count": 1000},
        },
        "objectives": {"carbon": "minimize"},
        "constraints": {"max_latency_sec": 2.0},
        "execution": {"runs": 5, "deterministic": True, "seed": 42},
        "reporting": {"return_metrics": True},
        "framework_config": {
            "autogen": {"use_enhancements": True},
        },
        "modp_weights": [0.4, 0.3, 0.2, 0.1],
        "enhancement_config": {
            "distillation": {
                "distillation_lr": 0.01,
                "gating_lr": 0.005,
                "seed": 42,
                "gate_seed": 7,
            },
            "rlhf": {"human_feedback_score": 0.7},
        },
    }

    # ---- 1. Happy path ------------------------------------------ #
    print("happy path:")
    req = load_request(valid_payload)
    _check("query_id parsed", req.query_id == "test")
    _check("schema_version parsed", req.schema_version == 1)
    _check(
        "modp_weights parsed as tuple",
        req.modp_weights == (0.4, 0.3, 0.2, 0.1),
    )
    _check("seed promoted", req.seed == 42)
    _check("deterministic parsed", req.deterministic is True)
    _check(
        "autogen enabled",
        "autogen" in req.enabled_frameworks(),
    )

    # ---- 2. to_adapter_config ---------------------------------- #
    print("to_adapter_config:")
    cfg = to_adapter_config(req, "autogen")
    _check(
        "modp_weights promoted to top level",
        cfg["modp_weights"] == [0.4, 0.3, 0.2, 0.1],
    )
    _check(
        "distillation_lr promoted",
        cfg["distillation_lr"] == 0.01,
    )
    _check(
        "gating_lr promoted",
        cfg["gating_lr"] == 0.005,
    )
    _check("seed promoted", cfg["seed"] == 42)
    _check(
        "human_feedback_score promoted",
        cfg["human_feedback_score"] == 0.7,
    )
    _check(
        "use_enhancements forwarded",
        cfg["use_enhancements"] is True,
    )
    _check(
        "return_raw_outputs forwarded",
        cfg["return_raw_outputs"] is False,
    )

    _check(
        "unknown framework rejected",
        _raises(EvalRequestError, lambda:
                to_adapter_config(req, "nope")),
    )
    _check(
        "missing framework rejected",
        _raises(EvalRequestError, lambda:
                to_adapter_config(req, "langchain")),
    )

    # ---- 3. modp_weights validation ---------------------------- #
    print("modp_weights:")
    for bad in (
        [0.4, 0.3, 0.2, 0.1, 0.0],     # 5 elements
        [0.4, 0.3, 0.2],               # 3 elements
        [0.4, -0.3, 0.2, 0.1],         # negative
        [0.0, 0.0, 0.0, 0.0],          # zero sum
        "not a list",
    ):
        bad_payload = dict(valid_payload)
        bad_payload["modp_weights"] = bad
        _check(
            f"reject {bad!r}",
            _raises(EvalRequestError, lambda:
                    load_request(bad_payload)),
        )

    # ---- 4. schema version ------------------------------------- #
    print("schema version:")
    bad = dict(valid_payload)
    bad["schema_version"] = 99
    _check(
        "unsupported version rejected",
        _raises(EvalRequestError, lambda:
                load_request(bad)),
    )

    # ---- 5. deterministic requires seed ------------------------ #
    print("deterministic seed:")
    bad = dict(valid_payload)
    bad["execution"] = {"runs": 5, "deterministic": True}
    bad = dict(bad)
    bad["enhancement_config"] = {
        "rlhf": {"human_feedback_score": 0.7},
    }
    _check(
        "deterministic without seed rejected",
        _raises(EvalRequestError, lambda:
                load_request(bad)),
    )

    # ---- 6. no inert keys ------------------------------------- #
    print("no inert keys:")
    bad = dict(valid_payload)
    bad["enhancement_config"] = {
        "distillation": {
            "distillation_learning_rate": 0.01,   # wrong name
        },
    }
    _check(
        "wrong distillation key rejected",
        _raises(EvalRequestError, lambda:
                load_request(bad)),
    )
    _check(
        "strict=False tolerates inert keys",
        load_request(bad, strict=False) is not None,
    )

    # ---- 7. strategy-aware enforcement ------------------------ #
    print("strategy-aware enforcement:")
    for fw_name, required_keys in (
        ("crewai", ("crewai_strategy_aware",)),
        ("langchain", (
            "langchain_strategy_aware",
            "langchain_strategy_executor",
        )),
        ("limit_graph", (
            "limit_graph_strategy_aware",
            "limit_graph_strategy_executor",
        )),
    ):
        bad = dict(valid_payload)
        bad["framework_config"] = {
            fw_name: {"use_enhancements": True},
        }
        _check(
            f"{fw_name}: missing strategy flags rejected",
            _raises(EvalRequestError, lambda:
                    load_request(bad)),
        )

    # ---- 8. flexgen action mapping validation ----------------- #
    print("flexgen action mapping:")
    bad = dict(valid_payload)
    bad["enhancement_config"] = {
        "flexgen": {
            "action_mapping": {"fp32": "bogus_strategy"},
        },
    }
    _check(
        "invalid strategy target rejected",
        _raises(EvalRequestError, lambda:
                load_request(bad)),
    )

    # ---- 9. File-based load ---------------------------------- #
    print("file-based load:")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "req.json"
        path.write_text(json.dumps(valid_payload), encoding="utf-8")
        req2 = load_request(path)
        _check(
            "path accepted",
            req2.query_id == "test",
        )
        bad_path = Path(tmpdir) / "bad.json"
        bad_path.write_text("{not json", encoding="utf-8")
        _check(
            "invalid JSON rejected",
            _raises(EvalRequestError, lambda:
                    load_request(bad_path)),
        )

    # ---- 10. validate_no_inert_enhancement_keys --------------- #
    print("validate_no_inert_enhancement_keys:")
    # No framework enables enhancements in this payload — no-op.
    empty = dict(valid_payload)
    empty["framework_config"] = {"autogen": {"use_enhancements": False}}
    empty_req = load_request(empty)
    try:
        validate_no_inert_enhancement_keys(empty_req)
        _check("no-op for disabled frameworks", True)
    except Exception as exc:
        _check(f"raised unexpectedly: {exc}", False)

    # Valid payload — no-op.
    try:
        validate_no_inert_enhancement_keys(req)
        _check("valid payload passes", True)
    except Exception as exc:
        _check(f"valid payload raised: {exc}", False)

    # ---- 11. __all__ / __dir__ ------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "EvalRequest", "EvalRequestError",
        "load_request", "to_adapter_config",
        "validate_no_inert_enhancement_keys",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
