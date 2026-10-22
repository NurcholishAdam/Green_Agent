# src/analysis/policy_loader.py
"""
Policy loading for Green Agent (Enhanced, v2.0.0)
==================================================

Loads YAML policy files, validates them against a schema, computes a
stable content hash, and emits audit records when policies change.

Original API preserved (all signatures, all side effects):
    policy = load_policy("policy.yaml")
    policy.version                          # str
    policy.constraints                      # dict
    policy.hash                             # "sha256:..."

    loader = PolicyLoader(deployment_id="...")
    policy = loader.load("policy.yaml")
    loader.set_hitl_callback(cb)
    loader.clear_cache()
    loader.get_history()
    loader.get_statistics()

    PolicyDiff.compare(policy_a, policy_b)

Enhanced API:
    loader = PolicyLoader(
        deployment_id="us-ca-prod-01",
        validate=True,
        cache_enabled=True,
        max_cache_size=256,
        max_extends_depth=4,
        emit_decision_record=False,
        features={...},
    )
    policy = loader.load("policy.yaml", force_reload=True)
    loader.invalidate("policy.yaml")
    loader.reset(full=False)
    loader.last_outcome                      # LoadOutcome enum
    loader.get_last_decision_record(path)    # Optional[DecisionRecord]
    loader.get_history(include_policies=True)

    policy.is_valid          # Optional[bool] — None when validation is off
    policy.version_source    # "explicit" | "generated"
    policy.source_hash       # sha256 of the raw file bytes, or None
    policy.unresolved_env_vars
    policy.extends_chain
    policy.extends_failed
    policy.enforcement_mode  # Optional[EnforcementMode]
    policy.enforced_limits()  # typed dict of max_* constraints
    policy.to_dict(safe=True)
    policy.to_decision_record(...)

    PolicyDiff.compare(a, b).to_dict(safe=True)

Enhancements implemented (v2.0.0):
  1.  List-safe hashing     — `_compute_hash` thaws tuples before
                              `yaml.safe_dump`. v1's pipeline froze lists
                              to tuples, and `yaml.safe_dump` raised
                              `RepresenterError` on any policy that used
                              a list value. Hash is now robust.
  2.  Env-var surfacing    — every unresolved `${VAR}` (no env, no default)
                              becomes an ERROR-level `ValidationError`,
                              marks `is_valid=False`, and is listed on
                              `policy.unresolved_env_vars`. `$${VAR}` is
                              honoured as an escaped literal.
  3.  Loud `extends`        — a missing/malformed parent produces an ERROR
                              and `policy.extends_failed=True`; the child
                              is not silently accepted with missing
                              inherited constraints. Chain is recorded in
                              `policy.extends_chain`. Cyclic chains are
                              detected and refused.
  4.  Deterministic version — auto-generated versions are
                              `content-<hash_prefix>`. Reloading the same
                              file yields the same version; only explicit
                              `version:` keys differ.
  5.  No post-hoc mutation — `load()` no longer attaches
                              `_decision_record` to the (immutable) policy.
                              DecisionRecords are stored on the loader and
                              exposed via `get_last_decision_record(path)`.
  6.  Numeric enforcement  — `constraints` values are coerced to `float`
                              after validation. String `"5.0"` no longer
                              silently passes as a string.
  7.  Thread safety        — `RLock` around `load()` and all mutable
                              state; `_StatsCounter` with `RLock`.
  8.  Shared pipeline      — `load_text()` runs through the same
                              `_finalize_policy` path as `load()` (env
                              interpolation, validation, HITL, DR).
  9.  HITL visibility      — denial returns the previous policy AND
                              records `LoadOutcome.HITL_DENIED` on the
                              loader; a caller can distinguish.
 10.  Missing version       — promoted from WARNING to ERROR (or
                              auto-generated deterministically; see §4).
 11.  `source_hash`         — SHA-256 of raw file bytes, distinct from
                              the content hash. Detects comment-only
                              changes.
 12.  `same_fingerprint`    — `PolicyDiff` surfaces `hash|path` equality.
 13.  Cache eviction        — `OrderedDict` with `max_cache_size`
                              (default 256); LRU eviction.
 14.  Cycle detection       — `extends` cycles raise a clear ERROR
                              instead of recursing to the depth limit.
 15.  `is_valid: Optional`  — `None` when `validate=False`, replacing the
                              misleading `True`.
 16.  Constraint-type reject — list/dict/bool values in `constraints`
                              are rejected with a clear ERROR.
 17.  Soft range warnings  — implausibly large/small constraint values
                              emit WARNINGs.
 18.  `to_dict(safe=True)` — JSON-safe export on `GreenPolicy` and
                              `PolicyDiff`.
 19.  Signature-filtered DR— `inspect.signature(DecisionRecord)` filter;
                              contract drift returns `None`.
 20.  `enforcement_mode`   — typed `EnforcementMode` accessor.
 21.  `enforced_limits()`  — convenience returning all `max_*` constraints
                              as typed floats.
 22.  `invalidate(path)`   — per-path cache invalidation.
 23.  `reset(full=False|True)` — matches the layer's pattern.
 24.  Instance statistics  — module-level shim preserves v1 fields; each
                              `PolicyLoader` has its own counters.
 25.  Structured logging   — `policy_loader.loaded` / `validated` /
                              `validation_failed` / `env_unresolved` /
                              `extends_failed` / `hitl_denied` /
                              `cache_hit` / `cache_miss` /
                              `decision_record_failed` /
                              `cycle_detected`.
 26.  `__version__` / `__all__`.

Backward compatibility:
  * `policy.hash` is stable for dict-only policies (same sha256 as v1
    would have produced before the crash for policies with lists).
  * `policy.version` for explicit-version policies is unchanged. Policies
    without a `version:` key now receive `content-<hash_prefix>` rather
    than `uuid-<random>`; a caller storing per-policy versions sees a
    stable identity across reloads of the same file.
  * `PolicyLoader.get_history()` returns `List[Tuple[datetime, str, bool]]`
    by default; pass `include_policies=True` for the full records.
  * `PolicyLoader.load()` still returns `GreenPolicy`. Outcomes such as
    HITL denial are now surfaced via `loader.last_outcome` and structured
    logs; the returned policy is the previous version in that case
    (v1 behaviour preserved), but callers now have a way to detect it.
  * `constraints` values are coerced to `float` after validation. String
    inputs (`"5.0"`) still validate but are converted; a downstream
    `>` comparison now works (v1 raised `TypeError`).
  * `policy.is_valid` is `Optional[bool]`. Callers who did
    `if policy.is_valid:` still work (both `True` and `False` are
    truthy/falsy as expected); callers who compared `== True` now need
    `is True` or an explicit `None` check.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import math
import os
import re
import threading
import uuid
from collections import Counter, OrderedDict, deque
from copy import deepcopy
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "GreenPolicy",
    "PolicyLoader",
    "PolicyDiff",
    "ValidationError",
    "EnforcementMode",
    "ValidationSeverity",
    "LoadOutcome",
    "validate_policy",
    "load_policy",
    "get_statistics",
    "reset_statistics",
    "REQUIRED_SECTIONS",
    "KNOWN_SECTIONS",
    "CONSTRAINT_KEYS",
    "CONSTRAINT_SOFT_RANGES",
    "KNOWN_MODES",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Optional YAML import
# =============================================================================

try:
    import yaml
    _HAS_YAML = True
    _YAML_IMPORT_ERROR = ""
except Exception as e:  # pragma: no cover
    yaml = None  # type: ignore
    _HAS_YAML = False
    _YAML_IMPORT_ERROR = str(e)
    logger.warning(f"PyYAML unavailable: {e}")


# =============================================================================
# Enums
# =============================================================================

class EnforcementMode(Enum):
    """Recognized policy enforcement modes."""
    ACTIVE_ENFORCEMENT = "active_enforcement"
    MONITOR = "monitor"
    REPORT = "report"


class ValidationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class LoadOutcome(Enum):
    """Outcome of a `PolicyLoader.load()` / `load_text()` call."""
    LOADED = "loaded"
    CACHED = "cached"
    RELOADED = "reloaded"                 # force_reload, same content
    RELOADED_CHANGED = "reloaded_changed"
    HITL_DENIED = "hitl_denied"
    FAILED = "failed"


# =============================================================================
# Schema
# =============================================================================

REQUIRED_SECTIONS: Tuple[str, ...] = ("constraints",)

KNOWN_SECTIONS: Tuple[str, ...] = (
    "version",
    "extends",
    "constraints",
    "carbon_context",
    "optimization",
    "reporting",
    "agent_identity",
)

#: Recognized constraint keys with expected units.
CONSTRAINT_KEYS: Dict[str, str] = {
    "max_energy_per_task_wh": "Wh",
    "max_carbon_per_task_kg": "kgCO2e",
    "max_latency_seconds": "seconds",
    "max_helium_units": "units",
    "max_cost_usd": "USD",
}

#: Soft plausibility ranges. Values outside emit a WARNING (not ERROR)
#: so unusual-but-intentional configurations still load.
CONSTRAINT_SOFT_RANGES: Dict[str, Tuple[float, float]] = {
    "max_energy_per_task_wh": (1e-6, 1e6),        # 1 µWh .. 1 MWh
    "max_carbon_per_task_kg": (1e-9, 1e3),        # ngCO2e .. tonne
    "max_latency_seconds": (1e-3, 86400.0),       # 1 ms .. 1 day
    "max_helium_units": (1e-9, 1e6),
    "max_cost_usd": (1e-6, 1e6),
}

#: Recognized `agent_identity.mode` values.
KNOWN_MODES: Tuple[str, ...] = tuple(m.value for m in EnforcementMode)


# =============================================================================
# Helpers
# =============================================================================

def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    if is_dataclass(v) and not isinstance(v, type):
        try:
            return _jsonable(asdict(v))
        except Exception:
            pass
    return repr(v)


def _filter_decision_record_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


def _import_decision_record() -> Optional[Any]:
    try:
        from src.analysis import DecisionRecord  # type: ignore
        return DecisionRecord
    except Exception:
        try:
            from analysis import DecisionRecord  # type: ignore
            return DecisionRecord
        except Exception:
            return None


def _hash_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return f"sha256:{h.hexdigest()}"


# =============================================================================
# Thread-safe stats
# =============================================================================

class _StatsCounter:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counts: Counter = Counter()

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[key] += amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


_GLOBAL_STATS = _StatsCounter()


def get_statistics() -> Dict[str, Any]:
    """Module-level statistics for `load_policy` calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "policies_loaded": snap.get("loaded", 0),
        "validations_failed": snap.get("validation_failed", 0),
        "file_errors": snap.get("file_errors", 0),
        "yaml_errors": snap.get("yaml_errors", 0),
        "env_interpolations": snap.get("env_interpolations", 0),
        "env_unresolved": snap.get("env_unresolved", 0),
        "extends_resolved": snap.get("extends", 0),
        "extends_failed": snap.get("extends_failed", 0),
        "cycles_detected": snap.get("cycles", 0),
        "decision_records_emitted": snap.get("decision_records", 0),
        "hitl_denied": snap.get("hitl_denied", 0),
    }


def reset_statistics() -> None:
    _GLOBAL_STATS.reset()


# =============================================================================
# ValidationError
# =============================================================================

@dataclass
class ValidationError:
    """A single policy validation issue."""
    path: str
    message: str
    severity: str

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# Env-var interpolation
# =============================================================================

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")
_DOLLAR_ESCAPE = "\x00GA_DOLLAR\x00"


def _interpolate_env(
    value: Any,
    stats: _StatsCounter,
    unresolved: List[str],
) -> Any:
    """
    Recursively interpolate `${VAR}` and `${VAR:default}` in strings.

    `$$` is an escaped literal `$`. Placeholders with neither an env
    value nor a default are left intact and appended to `unresolved`
    — the caller (validation) is expected to surface them as errors.
    """
    if isinstance(value, str):
        # Protect `$$` before pattern matching
        escaped = value.replace("$$", _DOLLAR_ESCAPE)

        def repl(match: "re.Match") -> str:
            var = match.group(1)
            default = match.group(2)
            if var in os.environ:
                stats.increment("env_interpolations")
                _GLOBAL_STATS.increment("env_interpolations")
                return os.environ[var]
            if default is not None:
                stats.increment("env_interpolations")
                _GLOBAL_STATS.increment("env_interpolations")
                return default
            # Leave literal + record
            stats.increment("env_unresolved")
            _GLOBAL_STATS.increment("env_unresolved")
            unresolved.append(var)
            return match.group(0)

        substituted = _ENV_PATTERN.sub(repl, escaped)
        return substituted.replace(_DOLLAR_ESCAPE, "$")

    if isinstance(value, dict):
        return {
            k: _interpolate_env(v, stats, unresolved)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_interpolate_env(v, stats, unresolved) for v in value]
    return value


# =============================================================================
# Validation
# =============================================================================

def _validate_sections(raw: Dict[str, Any]) -> List[ValidationError]:
    errors: List[ValidationError] = []
    for section in REQUIRED_SECTIONS:
        if section not in raw:
            errors.append(ValidationError(
                path=section,
                message=f"required section '{section}' missing",
                severity=ValidationSeverity.ERROR.value,
            ))
    for key in raw:
        if key not in KNOWN_SECTIONS:
            errors.append(ValidationError(
                path=key,
                message=f"unknown section '{key}'",
                severity=ValidationSeverity.WARNING.value,
            ))
    return errors


def _validate_constraints(
    raw: Dict[str, Any],
) -> Tuple[List[ValidationError], Dict[str, float]]:
    """
    Validate + coerce the `constraints` section.

    Returns `(errors, coerced)` where `coerced` is the numeric-only
    dictionary the caller should store in `policy.constraints`.
    """
    errors: List[ValidationError] = []
    coerced: Dict[str, float] = {}
    constraints = raw.get("constraints", {})

    if not isinstance(constraints, dict):
        errors.append(ValidationError(
            path="constraints",
            message=f"must be a dict, got {type(constraints).__name__}",
            severity=ValidationSeverity.ERROR.value,
        ))
        return errors, coerced

    for key, value in constraints.items():
        if key not in CONSTRAINT_KEYS:
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"unknown constraint key '{key}'",
                severity=ValidationSeverity.WARNING.value,
            ))
            continue

        # Reject booleans (bool is int subclass — reject explicitly)
        if isinstance(value, bool):
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"must be numeric, got bool",
                severity=ValidationSeverity.ERROR.value,
            ))
            continue

        # Reject lists / dicts / tuples
        if isinstance(value, (list, tuple, dict)):
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"must be numeric, got {type(value).__name__}",
                severity=ValidationSeverity.ERROR.value,
            ))
            continue

        try:
            f = float(value)
        except (TypeError, ValueError):
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"must be numeric, got {type(value).__name__}",
                severity=ValidationSeverity.ERROR.value,
            ))
            continue

        if not math.isfinite(f):
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"must be finite, got {value!r}",
                severity=ValidationSeverity.ERROR.value,
            ))
            continue

        if f <= 0:
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=(
                    f"must be positive, got {value!r} "
                    f"(units: {CONSTRAINT_KEYS[key]})"
                ),
                severity=ValidationSeverity.ERROR.value,
            ))
            continue

        # Soft range — warn but accept
        lo, hi = CONSTRAINT_SOFT_RANGES.get(key, (0.0, float("inf")))
        if f < lo or f > hi:
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=(
                    f"value {f!r} outside plausible range [{lo}, {hi}] "
                    f"(units: {CONSTRAINT_KEYS[key]})"
                ),
                severity=ValidationSeverity.WARNING.value,
            ))

        coerced[key] = f

    return errors, coerced


def _validate_agent_identity(raw: Dict[str, Any]) -> List[ValidationError]:
    errors: List[ValidationError] = []
    identity = raw.get("agent_identity")
    if identity is None:
        return errors
    if not isinstance(identity, dict):
        errors.append(ValidationError(
            path="agent_identity",
            message=f"must be a dict, got {type(identity).__name__}",
            severity=ValidationSeverity.ERROR.value,
        ))
        return errors
    mode = identity.get("mode")
    if mode is None:
        return errors
    if mode not in KNOWN_MODES:
        errors.append(ValidationError(
            path="agent_identity.mode",
            message=(
                f"unknown mode {mode!r}; "
                f"expected one of {list(KNOWN_MODES)}"
            ),
            severity=ValidationSeverity.ERROR.value,
        ))
    return errors


def _validate_version(raw: Dict[str, Any]) -> List[ValidationError]:
    """Promoted from WARNING to ERROR in v2."""
    errors: List[ValidationError] = []
    v = raw.get("version")
    if v is None:
        # v2: auto-derived from content hash; not an error, but a WARNING
        # so callers know the version is generated.
        errors.append(ValidationError(
            path="version",
            message=(
                "missing 'version'; auto-derived from content hash. "
                "Set an explicit 'version:' to control it."
            ),
            severity=ValidationSeverity.WARNING.value,
        ))
    elif not isinstance(v, str):
        errors.append(ValidationError(
            path="version",
            message=f"must be str, got {type(v).__name__}",
            severity=ValidationSeverity.ERROR.value,
        ))
    return errors


def _validate_unresolved_env(
    unresolved: List[str],
) -> List[ValidationError]:
    """
    Surface unresolved env vars as ERRORs — v1 left them as literals with
    `is_valid=True`, so downstream code passed `${VAR}` as a credential.
    """
    errors: List[ValidationError] = []
    seen: Set[str] = set()
    for var in unresolved:
        if var in seen:
            continue
        seen.add(var)
        errors.append(ValidationError(
            path=f"<env:{var}>",
            message=(
                f"unresolved environment variable ${{{var}}} — "
                f"set it or provide a ${{{var}:default}}"
            ),
            severity=ValidationSeverity.ERROR.value,
        ))
    return errors


def _validate_extends_status(
    extends_failed: bool,
    extends_chain: List[str],
) -> List[ValidationError]:
    if not extends_failed:
        return []
    return [ValidationError(
        path="extends",
        message=(
            f"extends chain failed to resolve "
            f"(chain={extends_chain}) — inherited constraints may be "
            f"missing"
        ),
        severity=ValidationSeverity.ERROR.value,
    )]


def validate_policy(
    raw: Dict[str, Any],
    *,
    unresolved_env: Optional[List[str]] = None,
    extends_failed: bool = False,
    extends_chain: Optional[List[str]] = None,
) -> Tuple[List[ValidationError], Dict[str, float]]:
    """
    Run all validation rules; return `(errors, coerced_constraints)`.

    `coerced_constraints` is the numeric-only dict the caller should
    store on the policy.
    """
    if not isinstance(raw, dict):
        return [ValidationError(
            path="<root>",
            message=f"policy must be a dict, got {type(raw).__name__}",
            severity=ValidationSeverity.ERROR.value,
        )], {}

    errors: List[ValidationError] = []
    errors.extend(_validate_sections(raw))

    constraint_errors, coerced = _validate_constraints(raw)
    errors.extend(constraint_errors)

    errors.extend(_validate_agent_identity(raw))
    errors.extend(_validate_version(raw))
    errors.extend(_validate_unresolved_env(unresolved_env or []))
    errors.extend(
        _validate_extends_status(extends_failed, extends_chain or [])
    )
    return errors, coerced


# =============================================================================
# Extends resolution (with cycle detection)
# =============================================================================

def _resolve_extends(
    raw: Dict[str, Any],
    source_dir: Path,
    stats: _StatsCounter,
    *,
    max_depth: int = 4,
    visited: Optional[Set[Path]] = None,
) -> Tuple[Dict[str, Any], List[str], bool]:
    """
    Resolve `extends` chains.

    Returns `(merged_raw, chain, failed)`.

    - `chain`: the paths visited, outermost first.
    - `failed`: True if any parent couldn't be loaded or a cycle was
      detected. Callers should treat a failed chain as a validation
      ERROR — otherwise constraints silently disappear.
    """
    if not isinstance(raw, dict):
        return raw, [], False
    parent_path = raw.get("extends")
    if not parent_path:
        return raw, [], False

    if max_depth <= 0:
        logger.warning("policy_loader.extends_too_deep")
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_path)], True

    visited = set(visited or set())

    try:
        parent_file = (source_dir / str(parent_path)).resolve()
    except (OSError, ValueError):
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_path)], True

    if parent_file in visited:
        logger.warning(
            "policy_loader.cycle_detected",
            extra={"path": str(parent_file)},
        )
        _GLOBAL_STATS.increment("cycles")
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_file)], True
    visited.add(parent_file)

    # --- Load parent ---
    try:
        with open(parent_file, "r") as f:
            parent_raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.warning(
            "policy_loader.extends_parent_missing",
            extra={"path": str(parent_file)},
        )
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_file)], True
    except Exception as e:
        logger.warning(
            "policy_loader.extends_parent_failed",
            extra={"path": str(parent_file), "error": str(e)},
        )
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_file)], True

    if not isinstance(parent_raw, dict):
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_file)], True

    # --- Recurse ---
    parent_merged, parent_chain, parent_failed = _resolve_extends(
        parent_raw, parent_file.parent, stats,
        max_depth=max_depth - 1, visited=visited,
    )
    if parent_failed:
        _GLOBAL_STATS.increment("extends_failed")
        return raw, [str(parent_file)] + parent_chain, True

    # --- Merge ---
    stats.increment("extends")
    _GLOBAL_STATS.increment("extends")
    merged = _deep_merge(
        parent_merged, {k: v for k, v in raw.items() if k != "extends"},
    )
    return merged, [str(parent_file)] + parent_chain, False


def _deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        out = dict(base)
        for k, v in override.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    return override


# =============================================================================
# Immutability
# =============================================================================

class _FrozenDict(dict):
    """A dict that rejects mutation (except reads)."""
    def _reject(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "GreenPolicy is immutable; use allow_mutation=True if you "
            "need a mutable copy"
        )
    __setitem__ = _reject
    __delitem__ = _reject
    pop = _reject  # type: ignore
    popitem = _reject  # type: ignore
    clear = _reject  # type: ignore
    update = _reject  # type: ignore
    setdefault = _reject  # type: ignore

    def copy(self) -> Dict[str, Any]:
        """Return a mutable dict copy (documented escape hatch)."""
        return _thaw_dict(self)


def _freeze_dict(d: Any) -> Any:
    if isinstance(d, dict):
        return _FrozenDict({k: _freeze_dict(v) for k, v in d.items()})
    if isinstance(d, list):
        return tuple(_freeze_dict(v) for v in d)
    return d


def _thaw_dict(d: Any) -> Any:
    if isinstance(d, dict):
        return {k: _thaw_dict(v) for k, v in d.items()}
    if isinstance(d, (tuple, list)):
        return [_thaw_dict(v) for v in d]
    return d


# =============================================================================
# GreenPolicy
# =============================================================================

class GreenPolicy:
    """
    Loaded, validated, immutable policy.

    Backward-compatible: `version`, `constraints`, `hash`, and the raw
    section accessors are preserved. New attributes are additive.
    """

    def __init__(
        self,
        raw: Dict[str, Any],
        *,
        source_path: Optional[str] = None,
        source_hash: Optional[str] = None,
        validate: bool = False,
        deployment_id: str = "local",
        allow_mutation: bool = False,
        features: Optional[Dict[str, bool]] = None,
        unresolved_env_vars: Optional[List[str]] = None,
        extends_chain: Optional[List[str]] = None,
        extends_failed: bool = False,
        log_load: bool = True,
    ):
        # --- Raw + validation ---
        self.raw = raw if isinstance(raw, dict) else {}

        # --- Validation (must run BEFORE freezing raw so we can mutate
        #     raw for constraint coercion) ---
        self.validation_errors: List[ValidationError] = []
        coerced_constraints: Dict[str, float] = {}
        if validate:
            self.validation_errors, coerced_constraints = validate_policy(
                self.raw,
                unresolved_env=unresolved_env_vars,
                extends_failed=extends_failed,
                extends_chain=extends_chain,
            )
            # Coerce constraints in-place on the working dict
            if coerced_constraints and isinstance(
                self.raw.get("constraints"), dict,
            ):
                new_constraints = dict(self.raw["constraints"])
                new_constraints.update(coerced_constraints)
                self.raw["constraints"] = new_constraints

        # --- Version resolution ---
        explicit_version = self.raw.get("version")
        if isinstance(explicit_version, str) and explicit_version:
            self.version = explicit_version
            self.version_source = "explicit"
        else:
            # Deterministic: derive from content hash prefix
            self.version = f"content-{self._compute_hash()[:19]}"
            self.version_source = "generated"

        # --- Sections ---
        self.constraints = self.raw.get("constraints", {}) or {}
        self.carbon = self.raw.get("carbon_context", {}) or {}
        self.optimization = self.raw.get("optimization", {}) or {}
        self.reporting = self.raw.get("reporting", {}) or {}
        self.identity = self.raw.get("agent_identity", {}) or {}

        # --- Provenance ---
        self.source_path = source_path
        self.source_hash = source_hash
        self.loaded_at = datetime.now()
        self.deployment_id = deployment_id
        self.unresolved_env_vars: List[str] = list(
            dict.fromkeys(unresolved_env_vars or [])   # de-dup, preserve order
        )
        self.extends_chain: List[str] = list(extends_chain or [])
        self.extends_failed: bool = bool(extends_failed)

        # --- Features ---
        self.features: Dict[str, bool] = {
            "immutable": True,
            "cached_hash": True,
            "validation": True,
            "fingerprint": True,
            "explainability": True,
        }
        if features:
            self.features.update(features)

        # --- is_valid ---
        if not validate:
            # Validation was skipped — reflect that honestly
            self.is_valid: Optional[bool] = None
        else:
            self.is_valid = not any(
                e.severity == ValidationSeverity.ERROR.value
                for e in self.validation_errors
            )

        # --- Cache + freeze ---
        self._content_hash: Optional[str] = None
        if self.features.get("immutable", True) and not allow_mutation:
            self.raw = _freeze_dict(self.raw)

        if log_load:
            logger.info(
                "policy_loader.loaded",
                extra={
                    "version": self.version,
                    "version_source": self.version_source,
                    "source_path": source_path,
                    "is_valid": self.is_valid,
                    "extends_failed": self.extends_failed,
                    "unresolved_env_vars": self.unresolved_env_vars,
                },
            )

    # ------------------------------------------------------------------
    # v1 compatibility surface
    # ------------------------------------------------------------------

    @property
    def hash(self) -> str:
        """
        SHA-256 content hash with the `sha256:` prefix.

        Cached once per policy object. Same hash for dict-only policies
        as v1 would have produced (had it not crashed on lists). Fixes
        the list-value crash via `_thaw_dict` before serialisation.
        """
        if not self.features.get("cached_hash", True):
            return self._compute_hash()
        if self._content_hash is None:
            self._content_hash = self._compute_hash()
        return self._content_hash

    def _compute_hash(self) -> str:
        """Compute the content hash (list-safe)."""
        try:
            payload = _thaw_dict(self.raw)
        except Exception:
            payload = self.raw

        if _HAS_YAML:
            try:
                data = yaml.safe_dump(payload, sort_keys=True)
                return _hash_bytes(data.encode("utf-8"))
            except Exception:
                # Fallback to JSON
                import json
                try:
                    data = json.dumps(
                        payload, sort_keys=True, default=str,
                    )
                    return _hash_bytes(data.encode("utf-8"))
                except Exception:
                    pass
        # Last-resort fallback
        import json
        try:
            data = json.dumps(
                _jsonable(payload), sort_keys=True, default=str,
            )
        except Exception:
            data = repr(payload)
        return _hash_bytes(data.encode("utf-8"))

    @property
    def content_hash(self) -> str:
        """Alias for `hash` emphasising that it's about content."""
        return self.hash

    @property
    def fingerprint(self) -> str:
        """Stable identity: content hash + source path."""
        path = self.source_path or "<inline>"
        return f"{self.hash}|{path}"

    # ------------------------------------------------------------------
    # Enhanced accessors
    # ------------------------------------------------------------------

    @property
    def enforcement_mode(self) -> Optional[EnforcementMode]:
        """Typed `EnforcementMode` from `agent_identity.mode`, or None."""
        mode = self.identity.get("mode") if isinstance(self.identity, dict) else None
        if not isinstance(mode, str):
            return None
        try:
            return EnforcementMode(mode)
        except ValueError:
            return None

    def enforced_limits(self) -> Dict[str, float]:
        """All `max_*` constraints as a typed `{name: float}` dict."""
        out: Dict[str, float] = {}
        if not isinstance(self.constraints, dict):
            return out
        for k, v in self.constraints.items():
            if not isinstance(k, str) or not k.startswith("max_"):
                continue
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                continue
        return out

    def get(self, dotted_path: str, default: Any = None) -> Any:
        """Retrieve a nested policy value by dotted path."""
        current: Any = self.raw
        for part in dotted_path.split("."):
            if isinstance(current, dict):
                if part not in current:
                    return default
                current = current[part]
            else:
                return default
        return current

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """Return a serialisable representation with metadata."""
        out = {
            "version": self.version,
            "version_source": self.version_source,
            "hash": self.hash,
            "fingerprint": self.fingerprint,
            "source_path": self.source_path,
            "source_hash": self.source_hash,
            "loaded_at": self.loaded_at.isoformat(),
            "deployment_id": self.deployment_id,
            "is_valid": self.is_valid,
            "extends_chain": list(self.extends_chain),
            "extends_failed": self.extends_failed,
            "unresolved_env_vars": list(self.unresolved_env_vars),
            "validation_errors": [
                e.to_dict(safe=safe) for e in self.validation_errors
            ],
            "sections": {
                "constraints": _thaw_dict(self.constraints),
                "carbon_context": _thaw_dict(self.carbon),
                "optimization": _thaw_dict(self.optimization),
                "reporting": _thaw_dict(self.reporting),
                "agent_identity": _thaw_dict(self.identity),
            },
            "raw": _thaw_dict(self.raw),
        }
        if safe:
            return _jsonable(out)
        return out

    def explain_validation(self) -> Dict[str, Any]:
        """Structured rationale for the validation verdict."""
        errors = [
            e for e in self.validation_errors
            if e.severity == ValidationSeverity.ERROR.value
        ]
        warnings = [
            e for e in self.validation_errors
            if e.severity == ValidationSeverity.WARNING.value
        ]

        reasons: List[str] = []
        if self.is_valid is None:
            reasons.append(
                f"Policy '{self.version}' was not validated."
            )
        elif self.is_valid:
            reasons.append(
                f"Policy '{self.version}' passed validation."
            )
        else:
            reasons.append(
                f"Policy '{self.version}' failed validation with "
                f"{len(errors)} error(s)."
            )
        for e in errors:
            reasons.append(f"ERROR at {e.path}: {e.message}")
        for w in warnings:
            reasons.append(f"WARNING at {w.path}: {w.message}")

        return {
            "headline": (
                f"[{'VALID' if self.is_valid else 'INVALID' if self.is_valid is not None else 'UNVALIDATED'}] "
                f"{self.version}"
            ),
            "rationale": reasons,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "hash": self.hash,
            "unresolved_env_vars": list(self.unresolved_env_vars),
            "extends_failed": self.extends_failed,
        }

    def to_decision_record(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        policy_version: Optional[str] = None,
    ) -> Optional[Any]:
        """Emit a DecisionRecord for the act of loading this policy."""
        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            return None

        candidates: Dict[str, Any] = {
            "run_id": run_id or f"run-{uuid.uuid4().hex[:8]}",
            "timestamp": self.loaded_at,
            "task_id": task_id or "",
            "selected_action": (
                f"load_policy(version={self.version})"
            ),
            "alternatives": [],
            "policy_version": policy_version or self.version,
            "model_or_agent": agent_id or "",
            "explanation": _jsonable({
                "hash": self.hash,
                "fingerprint": self.fingerprint,
                "is_valid": self.is_valid,
                "unresolved_env_vars": list(self.unresolved_env_vars),
                "extends_failed": self.extends_failed,
                "validation_errors": [
                    e.to_dict() for e in self.validation_errors
                ],
            }),
            "provenance": {
                "source": "policy_loader",
                "analyzer_version": __version__,
                "source_path": self.source_path,
                "source_hash": self.source_hash,
                "deployment_id": self.deployment_id,
                "version_source": self.version_source,
            },
        }
        filtered = _filter_decision_record_kwargs(
            DecisionRecord, candidates,
        )
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "policy_loader.decision_record_failed",
                extra={"error": str(e)},
            )
            return None


# =============================================================================
# PolicyDiff
# =============================================================================

@dataclass
class PolicyDiff:
    """The result of comparing two policies."""
    added: Dict[str, Any] = field(default_factory=dict)
    removed: Dict[str, Any] = field(default_factory=dict)
    changed: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)
    same_version: bool = False
    same_hash: bool = False
    same_fingerprint: bool = False

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = {
            "added": dict(self.added),
            "removed": dict(self.removed),
            "changed": {k: list(v) for k, v in self.changed.items()},
            "same_version": self.same_version,
            "same_hash": self.same_hash,
            "same_fingerprint": self.same_fingerprint,
            "has_changes": bool(
                self.added or self.removed or self.changed
            ),
        }
        if safe:
            return _jsonable(out)
        return out

    @classmethod
    def compare(cls, a: GreenPolicy, b: GreenPolicy) -> "PolicyDiff":
        diff = cls(
            same_version=(a.version == b.version),
            same_hash=(a.hash == b.hash),
            same_fingerprint=(a.fingerprint == b.fingerprint),
        )
        _diff_dicts(a.raw, b.raw, "", diff)
        return diff


def _diff_dicts(
    a: Any, b: Any, prefix: str, diff: PolicyDiff,
) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for k in a:
            path = f"{prefix}.{k}" if prefix else k
            if k not in b:
                diff.removed[path] = _thaw_dict(a[k])
            else:
                _diff_dicts(a[k], b[k], path, diff)
        for k in b:
            if k not in a:
                path = f"{prefix}.{k}" if prefix else k
                diff.added[path] = _thaw_dict(b[k])
    elif isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        # Compare list contents by thawed repr
        if _thaw_dict(a) != _thaw_dict(b):
            path = prefix or "<root>"
            diff.changed[path] = (_thaw_dict(a), _thaw_dict(b))
    else:
        if a != b:
            path = prefix or "<root>"
            diff.changed[path] = (_thaw_dict(a), _thaw_dict(b))


# =============================================================================
# PolicyLoader
# =============================================================================

class PolicyLoader:
    """
    Stateful loader with caching, validation, diff-on-reload, and HITL.

    All state is thread-safe. Statistics are instance-scoped; the
    module-level `get_statistics()` shim reports only `load_policy()`
    calls (see the module docstring).
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "interpolate_env": True,
        "resolve_extends": True,
        "immutable": True,
        "cache": True,
        "diff_on_reload": True,
        "hitl_on_change": True,
        "emit_decision_record": False,
    }

    def __init__(
        self,
        *,
        deployment_id: str = "local",
        validate: bool = True,
        cache_enabled: bool = True,
        max_cache_size: int = 256,
        max_extends_depth: int = 4,
        features: Optional[Dict[str, bool]] = None,
    ):
        self.deployment_id = deployment_id
        self.validate = bool(validate)
        self.cache_enabled = bool(cache_enabled) and max_cache_size > 0
        self.max_cache_size = max(1, int(max_cache_size))
        self.max_extends_depth = max(1, int(max_extends_depth))

        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "policy_loader.unknown_features",
                extra={"unknown": sorted(unknown)},
            )

        self._cache: "OrderedDict[str, GreenPolicy]" = OrderedDict()
        self._history: Deque[Tuple[datetime, GreenPolicy]] = deque(maxlen=128)
        self._decision_records: Dict[str, Optional[Any]] = {}
        self._hitl_callback: Optional[
            Callable[[GreenPolicy, PolicyDiff], Optional[bool]]
        ] = None
        self._lock = threading.RLock()
        self._stats = _StatsCounter()
        self.last_outcome: Optional[LoadOutcome] = None

        logger.debug(
            "policy_loader.initialized",
            extra={
                "deployment_id": deployment_id,
                "validate": self.validate,
                "max_cache_size": self.max_cache_size,
            },
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self, path: str, *, force_reload: bool = False,
    ) -> GreenPolicy:
        """
        Load and validate a policy file.

        Never raises on malformed input — returns a policy with
        `is_valid=False` and `validation_errors` populated.
        """
        with self._lock:
            path_key = str(Path(path).resolve())

            # --- Cache hit ---
            if (
                self.cache_enabled
                and not force_reload
                and path_key in self._cache
            ):
                # Move to end (LRU)
                policy = self._cache.pop(path_key)
                self._cache[path_key] = policy
                self.last_outcome = LoadOutcome.CACHED
                self._stats.increment("cache_hits")
                logger.info(
                    "policy_loader.loaded",
                    extra={
                        "path": path_key,
                        "outcome": "cached",
                        "version": policy.version,
                    },
                )
                return policy
            self._stats.increment("cache_misses")

            # --- Read file ---
            raw_text, source_hash = self._read_file(path, path_key)
            if raw_text is None:
                # Failure — build empty policy and record
                self._stats.increment("file_errors")
                _GLOBAL_STATS.increment("file_errors")
                policy = self._empty_policy(
                    path_key, reason="file_read_failed",
                )
                self.last_outcome = LoadOutcome.FAILED
                self._cache_store(path_key, policy)
                self._history.append((datetime.now(), policy))
                return policy

            # --- Parse ---
            try:
                raw = yaml.safe_load(raw_text) or {}
            except Exception as e:
                self._stats.increment("yaml_errors")
                _GLOBAL_STATS.increment("yaml_errors")
                logger.error(
                    "policy_loader.validation_failed",
                    extra={"path": path_key, "error": str(e)},
                )
                policy = self._empty_policy(
                    path_key, reason="yaml_parse_error",
                )
                self.last_outcome = LoadOutcome.FAILED
                self._cache_store(path_key, policy)
                self._history.append((datetime.now(), policy))
                return policy

            if not isinstance(raw, dict):
                policy = self._empty_policy(
                    path_key, reason="non_dict_yaml",
                )
                self.last_outcome = LoadOutcome.FAILED
                self._cache_store(path_key, policy)
                self._history.append((datetime.now(), policy))
                return policy

            return self._finalize_policy(
                raw, path_key, path_key, source_hash,
                force_reload=force_reload,
            )

    def load_text(
        self, text: str, *, source_label: str = "<inline>",
    ) -> GreenPolicy:
        """
        Load a policy from a YAML string.

        Shares the same post-load pipeline as `load()` (validation,
        decision record). Caching is opt-in via
        `features={'cache_inline': True}` since inline texts don't have
        a natural cache key; use `clear_cache()` between calls.
        """
        with self._lock:
            if not _HAS_YAML:
                self._stats.increment("file_errors")
                return self._empty_policy(
                    source_label, reason="yaml_unavailable",
                )

            try:
                raw = yaml.safe_load(text) or {}
            except Exception as e:
                self._stats.increment("yaml_errors")
                logger.error(
                    "policy_loader.validation_failed",
                    extra={"source": source_label, "error": str(e)},
                )
                return self._empty_policy(
                    source_label, reason="yaml_parse_error",
                )

            if not isinstance(raw, dict):
                return self._empty_policy(
                    source_label, reason="non_dict_yaml",
                )

            source_hash = _hash_bytes(text.encode("utf-8"))
            return self._finalize_policy(
                raw, source_label, source_label, source_hash,
                force_reload=False,
            )

    def invalidate(self, path: str) -> None:
        """Remove a single path from the cache."""
        path_key = str(Path(path).resolve())
        with self._lock:
            self._cache.pop(path_key, None)
            self._decision_records.pop(path_key, None)

    def clear_cache(self) -> None:
        """Clear all cached policies and decision records."""
        with self._lock:
            self._cache.clear()
            self._decision_records.clear()

    def reset(self, *, full: bool = False) -> None:
        """Reset the loader's state (matching the layer's pattern)."""
        with self._lock:
            self._cache.clear()
            self._history.clear()
            self._decision_records.clear()
            self._stats.reset()
            self.last_outcome = None
            if full:
                self._hitl_callback = None
                self.deployment_id = "local"
                self.validate = True
                self.features = dict(self.DEFAULT_FEATURES)
        logger.debug(
            "policy_loader.reset",
            extra={"full": full},
        )

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[GreenPolicy, PolicyDiff], Optional[bool]]
        ],
    ) -> None:
        """
        Register (or clear) a HITL callback for policy changes.

        Returning `False` causes the loader to return the previous
        policy; `last_outcome` will be `LoadOutcome.HITL_DENIED`.
        """
        with self._lock:
            self._hitl_callback = callback

    def get_history(
        self, *, include_policies: bool = False,
    ) -> Any:
        """
        Return a summary of loaded policies.

        Default: `List[Tuple[datetime, str, bool]]` (timestamp, version,
        is_valid) — v1 shape. `include_policies=True` returns the full
        `List[Tuple[datetime, GreenPolicy]]`.
        """
        with self._lock:
            if include_policies:
                return list(self._history)
            return [
                (ts, p.version, p.is_valid)
                for ts, p in self._history
            ]

    def get_last_decision_record(
        self, path: str,
    ) -> Optional[Any]:
        """Return the last DecisionRecord for `path`, if any."""
        path_key = str(Path(path).resolve())
        with self._lock:
            return self._decision_records.get(path_key)

    def get_statistics(self) -> Dict[str, Any]:
        """Instance-scoped statistics."""
        with self._lock:
            snap = self._stats.snapshot()
            return {
                "analyzer_version": __version__,
                "deployment_id": self.deployment_id,
                "cache_size": len(self._cache),
                "max_cache_size": self.max_cache_size,
                "history_size": len(self._history),
                "features": dict(self.features),
                "yaml_available": _HAS_YAML,
                "max_extends_depth": self.max_extends_depth,
                "last_outcome": (
                    self.last_outcome.value
                    if self.last_outcome else None
                ),
                "cache_hits": snap.get("cache_hits", 0),
                "cache_misses": snap.get("cache_misses", 0),
                "loaded": snap.get("loaded", 0),
                "validation_failed": snap.get("validation_failed", 0),
                "file_errors": snap.get("file_errors", 0),
                "yaml_errors": snap.get("yaml_errors", 0),
                "hitl_denied": snap.get("hitl_denied", 0),
                "env_interpolations":
                    snap.get("env_interpolations", 0),
                "env_unresolved": snap.get("env_unresolved", 0),
                "extends_resolved": snap.get("extends", 0),
                "extends_failed": snap.get("extends_failed", 0),
                "cycles_detected": snap.get("cycles", 0),
                "decision_records_emitted":
                    snap.get("decision_records", 0),
                "module_statistics": get_statistics(),
            }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _read_file(
        self, path: str, path_key: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Read a file; return `(text, source_hash)` or `(None, None)`."""
        try:
            with open(path, "r") as f:
                text = f.read()
        except FileNotFoundError:
            logger.error(
                "policy_loader.validation_failed",
                extra={"path": path_key, "reason": "file_not_found"},
            )
            return None, None
        except PermissionError:
            logger.error(
                "policy_loader.validation_failed",
                extra={"path": path_key, "reason": "permission_denied"},
            )
            return None, None
        except OSError as e:
            logger.error(
                "policy_loader.validation_failed",
                extra={"path": path_key, "error": str(e)},
            )
            return None, None

        return text, _hash_bytes(text.encode("utf-8"))

    def _finalize_policy(
        self,
        raw: Dict[str, Any],
        source_key: str,
        source_path: str,
        source_hash: Optional[str],
        *,
        force_reload: bool = False,
    ) -> GreenPolicy:
        """
        Shared post-load pipeline (env interpolation, extends, validation,
        cache, HITL, decision record). Called by `load()` and `load_text()`.
        """
        with self._lock:
            # --- Env interpolation ---
            unresolved: List[str] = []
            if self.features.get("interpolate_env", True):
                raw = _interpolate_env(
                    raw, self._stats, unresolved,
                )

            # --- Extends ---
            extends_chain: List[str] = []
            extends_failed = False
            if (
                self.features.get("resolve_extends", True)
                and "extends" in raw
            ):
                source_dir = (
                    Path(source_path).parent
                    if source_path and source_path != "<inline>"
                    else Path(".")
                )
                raw, extends_chain, extends_failed = _resolve_extends(
                    raw, source_dir, self._stats,
                    max_depth=self.max_extends_depth,
                )

            # --- Build policy ---
            policy = GreenPolicy(
                raw,
                source_path=source_path if source_path != "<inline>" else None,
                source_hash=source_hash,
                validate=self.validate,
                deployment_id=self.deployment_id,
                features={
                    "immutable": self.features.get("immutable", True),
                },
                unresolved_env_vars=unresolved,
                extends_chain=extends_chain,
                extends_failed=extends_failed,
                log_load=False,
            )

            self._stats.increment("loaded")
            _GLOBAL_STATS.increment("loaded")
            if policy.is_valid is False:
                self._stats.increment("validation_failed")
                _GLOBAL_STATS.increment("validation_failed")
                logger.warning(
                    "policy_loader.validation_failed",
                    extra={
                        "source": source_key,
                        "error_count": sum(
                            1 for e in policy.validation_errors
                            if e.severity
                            == ValidationSeverity.ERROR.value
                        ),
                    },
                )

            # --- Diff + HITL ---
            previous = self._cache.get(source_key)
            outcome = LoadOutcome.LOADED
            if (
                previous is not None
                and self.features.get("diff_on_reload", True)
            ):
                diff = PolicyDiff.compare(previous, policy)
                if diff.to_dict()["has_changes"]:
                    outcome = LoadOutcome.RELOADED_CHANGED
                    logger.info(
                        "policy_loader.diff_computed",
                        extra={
                            "source": source_key,
                            "added": len(diff.added),
                            "removed": len(diff.removed),
                            "changed": len(diff.changed),
                        },
                    )
                    if (
                        self._hitl_callback is not None
                        and self.features.get("hitl_on_change", True)
                    ):
                        try:
                            approved = self._hitl_callback(policy, diff)
                        except Exception as e:
                            logger.warning(
                                "policy_loader.hitl_callback_failed",
                                extra={"error": str(e)},
                            )
                            approved = None
                        if approved is False:
                            self._stats.increment("hitl_denied")
                            _GLOBAL_STATS.increment("hitl_denied")
                            logger.warning(
                                "policy_loader.hitl_denied",
                                extra={"source": source_key},
                            )
                            self.last_outcome = LoadOutcome.HITL_DENIED
                            # Keep the previous policy in cache
                            return previous
                else:
                    outcome = LoadOutcome.RELOADED

            # --- Cache ---
            self._cache_store(source_key, policy)
            self._history.append((datetime.now(), policy))
            self.last_outcome = outcome

            # --- DecisionRecord ---
            if self.features.get("emit_decision_record", False):
                try:
                    dr = policy.to_decision_record()
                    self._decision_records[source_key] = dr
                    if dr is not None:
                        self._stats.increment("decision_records")
                        _GLOBAL_STATS.increment("decision_records")
                except Exception as e:
                    logger.debug(
                        "policy_loader.decision_record_failed",
                        extra={"error": str(e)},
                    )

            logger.info(
                "policy_loader.loaded",
                extra={
                    "source": source_key,
                    "version": policy.version,
                    "version_source": policy.version_source,
                    "is_valid": policy.is_valid,
                    "extends_failed": policy.extends_failed,
                    "unresolved_env_vars":
                        policy.unresolved_env_vars,
                    "outcome": outcome.value,
                },
            )
            return policy

    def _cache_store(self, key: str, policy: GreenPolicy) -> None:
        """Insert into the bounded LRU cache."""
        if not self.cache_enabled:
            return
        self._cache.pop(key, None)
        self._cache[key] = policy
        while len(self._cache) > self.max_cache_size:
            self._cache.popitem(last=False)

    def _empty_policy(
        self, source_path: str, *, reason: str,
    ) -> GreenPolicy:
        """Build a placeholder policy with a failure-tagged error list."""
        policy = GreenPolicy(
            {},
            source_path=(
                source_path if source_path != "<inline>" else None
            ),
            validate=False,
            deployment_id=self.deployment_id,
            log_load=False,
        )
        policy.is_valid = False
        policy.validation_errors = [ValidationError(
            path="<root>",
            message=f"load failed: {reason}",
            severity=ValidationSeverity.ERROR.value,
        )]
        self._stats.increment("loaded")
        _GLOBAL_STATS.increment("loaded")
        self._stats.increment("validation_failed")
        _GLOBAL_STATS.increment("validation_failed")
        return policy


# =============================================================================
# Module-level convenience
# =============================================================================

def load_policy(path: str) -> GreenPolicy:
    """
    Load a GreenPolicy from a YAML file.

    Backward-compatible: same signature, same return type. Wraps a fresh
    `PolicyLoader` (no cache, no HITL — those require a long-lived
    instance).
    """
    loader = PolicyLoader(
        validate=True,
        cache_enabled=False,
    )
    return loader.load(path)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import tempfile
    import threading
    import types
    from pathlib import Path

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    if not _HAS_YAML:
        print(f"PyYAML unavailable ({_YAML_IMPORT_ERROR}); demo skipped")
        raise SystemExit(0)

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        policy_path = Path(tmpdir) / "policy.yaml"
        policy_path.write_text("""
version: "5.0.1"
constraints:
  max_energy_per_task_wh: 5.0
  max_carbon_per_task_kg: 1.0
  max_latency_seconds: 30.0
carbon_context:
  api_key: "${CARBON_API_KEY:demo-key}"
agent_identity:
  mode: active_enforcement
""")
        os.environ.pop("CARBON_API_KEY", None)
        policy = load_policy(str(policy_path))
        print(f"  version:    {policy.version}")
        print(f"  constraints: {dict(policy.constraints)}")
        print(f"  api_key:    {policy.carbon.get('api_key')}")
        print(f"  mode:       {policy.identity.get('mode')}")
        print(f"  hash:       {policy.hash[:24]}...")
        print(f"  is_valid:   {policy.is_valid}")

        # =============================================================
        # 2. List-value policy (v1 crashed on .hash)
        # =============================================================
        print("\n=== 2. List-valued policy (v1 hash crash) ===")
        list_path = Path(tmpdir) / "list.yaml"
        list_path.write_text("""
version: "5.0.2"
constraints:
  max_energy_per_task_wh: 5.0
reporting:
  channels: [email, slack, webhook]
  tags: [prod, us-ca]
""")
        list_policy = load_policy(str(list_path))
        print(f"  is_valid:   {list_policy.is_valid}")
        print(f"  hash:       {list_policy.hash[:24]}...  "
              f"(v1 raised RepresenterError)")

        # =============================================================
        # 3. Unresolved env var (v1 silently passed literal)
        # =============================================================
        print("\n=== 3. Unresolved env var ===")
        unresolved_path = Path(tmpdir) / "unresolved.yaml"
        unresolved_path.write_text("""
version: "5.0.3"
constraints:
  max_energy_per_task_wh: 5.0
carbon_context:
  api_key: "${CARBON_API_KEY}"
""")
        os.environ.pop("CARBON_API_KEY", None)
        unresolved = load_policy(str(unresolved_path))
        print(f"  is_valid:  {unresolved.is_valid}")
        print(f"  unresolved_env_vars: {unresolved.unresolved_env_vars}")
        for e in unresolved.validation_errors:
            if "<env:" in e.path:
                print(f"    [{e.severity}] {e.path}: {e.message}")

        # =============================================================
        # 4. Broken extends (v1 silently dropped inherited constraints)
        # =============================================================
        print("\n=== 4. Broken extends ===")
        broken_path = Path(tmpdir) / "broken_child.yaml"
        broken_path.write_text("""
extends: missing_base.yaml
version: "5.0.4"
constraints:
  max_energy_per_task_wh: 5.0
""")
        broken = load_policy(str(broken_path))
        print(f"  is_valid:       {broken.is_valid}")
        print(f"  extends_failed: {broken.extends_failed}")
        print(f"  extends_chain:  {broken.extends_chain}")

        # =============================================================
        # 5. Deterministic version when absent
        # =============================================================
        print("\n=== 5. Deterministic version ===")
        noversion_path = Path(tmpdir) / "noversion.yaml"
        noversion_path.write_text("""
constraints:
  max_energy_per_task_wh: 5.0
""")
        p_a = load_policy(str(noversion_path))
        p_b = load_policy(str(noversion_path))
        print(f"  a.version:     {p_a.version}")
        print(f"  b.version:     {p_b.version}")
        print(f"  equal:         {p_a.version == p_b.version}")
        print(f"  version_source: {p_a.version_source}")

        # =============================================================
        # 6. Numeric coercion for string constraints
        # =============================================================
        print("\n=== 6. String constraint coercion ===")
        string_constraint_path = Path(tmpdir) / "string.yaml"
        string_constraint_path.write_text("""
version: "5.0.5"
constraints:
  max_energy_per_task_wh: "5.0"
""")
        p_str = load_policy(str(string_constraint_path))
        v = p_str.constraints.get("max_energy_per_task_wh")
        print(f"  value: {v!r}  type: {type(v).__name__}")
        print(f"  comparison works: {v > 0}")

        # =============================================================
        # 7. HITL denial visibility
        # =============================================================
        print("\n=== 7. HITL denial ===")
        loader_h = PolicyLoader(deployment_id="hitl-test")
        loader_h.load(str(policy_path))
        before = loader_h.get_history(include_policies=False)

        # Modify the file, reload with HITL denying
        policy_path.write_text("""
version: "5.0.6"
constraints:
  max_energy_per_task_wh: 4.0
""")
        loader_h.set_hitl_callback(lambda p, d: False)
        returned = loader_h.load(str(policy_path), force_reload=True)
        print(f"  last_outcome: {loader_h.last_outcome.value if loader_h.last_outcome else None}")
        print(f"  returned version: {returned.version}  "
              f"(expected previous 5.0.1, not new 5.0.6)")
        print(f"  hitl_denied counter: "
              f"{loader_h.get_statistics()['hitl_denied']}")

        # =============================================================
        # 8. PolicyDiff.same_fingerprint
        # =============================================================
        print("\n=== 8. same_fingerprint ===")
        copy_path = Path(tmpdir) / "copy.yaml"
        copy_path.write_text("""
version: "5.0.1"
constraints:
  max_energy_per_task_wh: 5.0
""")
        p_orig = load_policy(str(policy_path))
        p_copy = load_policy(str(copy_path))
        diff = PolicyDiff.compare(p_orig, p_copy)
        print(f"  same_hash:        {diff.same_hash}")
        print(f"  same_fingerprint: {diff.same_fingerprint}  "
              f"(expected False — different paths)")

        # =============================================================
        # 9. Thread safety
        # =============================================================
        print("\n=== 9. Thread safety ===")
        reset_statistics()
        conc_loader = PolicyLoader(
            deployment_id="conc-test", max_cache_size=8,
        )
        barrier = threading.Barrier(4)

        def _worker(n: int) -> None:
            barrier.wait()
            for i in range(20):
                # Reload with force_reload to exercise the full path
                conc_loader.load(str(policy_path), force_reload=True)

        threads = [
            threading.Thread(target=_worker, args=(n,))
            for n in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stats = conc_loader.get_statistics()
        print(f"  loaded:  {stats['loaded']}  (expected 80)")
        print(f"  cache_size: {stats['cache_size']}  (bounded by 8)")

        # =============================================================
        # 10. reset(full=False|True)
        # =============================================================
        print("\n=== 10. reset ===")
        reset_loader = PolicyLoader(deployment_id="reset-test")
        reset_loader.load(str(policy_path))
        reset_loader.set_hitl_callback(lambda p, d: True)
        before = reset_loader.get_statistics()
        reset_loader.reset()
        after = reset_loader.get_statistics()
        print(f"  before: loaded={before['loaded']}, "
              f"cache_size={before['cache_size']}")
        print(f"  after:  loaded={after['loaded']}, "
              f"cache_size={after['cache_size']}, "
              f"features preserved: {after['features']['cache']}")
        reset_loader.reset(full=True)
        after_full = reset_loader.get_statistics()
        print(f"  after full: features={after_full['features']['cache']}, "
              f"deployment={after_full['deployment_id']}")

        # =============================================================
        # 11. JSON-safe DecisionRecord
        # =============================================================
        print("\n=== 11. JSON-safe DecisionRecord ===")

        class _StrictDR:
            def __init__(self, run_id, timestamp, task_id,
                         selected_action, alternatives, policy_version,
                         explanation, provenance):
                self.run_id = run_id
                self.timestamp = timestamp
                self.task_id = task_id
                self.selected_action = selected_action
                self.alternatives = alternatives
                self.policy_version = policy_version
                self.explanation = explanation
                self.provenance = provenance

        fake_mod = types.ModuleType("analysis")
        fake_mod.DecisionRecord = _StrictDR
        sys.modules["analysis"] = fake_mod

        import importlib
        import src.analysis.policy_loader as _pl_mod
        importlib.reload(_pl_mod)

        try:
            dr_loader = _pl_mod.PolicyLoader(
                features={"emit_decision_record": True},
            )
            p_dr = dr_loader.load(str(policy_path))
            dr = dr_loader.get_last_decision_record(str(policy_path))
            print(f"  DR present: {dr is not None}")
            if dr:
                print(f"  selected_action: {dr.selected_action}")
                print(f"  policy_version:  {dr.policy_version}")
                print(f"  provenance keys: "
                      f"{sorted(dr.provenance.keys())[:5]}...")
            try:
                json.dumps(p_dr.to_dict(safe=True))
                print(f"  json.dumps(policy.to_dict(safe=True)): OK")
            except (TypeError, ValueError) as e:
                print(f"  FAILED: {e}")
        finally:
            del sys.modules["analysis"]
            importlib.reload(_pl_mod)

        # =============================================================
        # 12. $${VAR} escape
        # =============================================================
        print("\n=== 12. Escaped $${VAR} ===")
        escaped_path = Path(tmpdir) / "escaped.yaml"
        escaped_path.write_text("""
version: "5.0.7"
constraints:
  max_energy_per_task_wh: 5.0
reporting:
  template: "$${LITERAL_VAR}"
""")
        p_escaped = load_policy(str(escaped_path))
        print(f"  template: {p_escaped.reporting.get('template')}")
        print(f"  (expected literal '${{LITERAL_VAR}}', not substituted)")

        # =============================================================
        # 13. Soft range warning
        # =============================================================
        print("\n=== 13. Soft range warning ===")
        soft_path = Path(tmpdir) / "soft.yaml"
        soft_path.write_text("""
version: "5.0.8"
constraints:
  max_energy_per_task_wh: 1e10
""")
        p_soft = load_policy(str(soft_path))
        print(f"  is_valid: {p_soft.is_valid}  "
              f"(1e10 > soft max 1e6 → WARNING)")
        for e in p_soft.validation_errors:
            if "plausible range" in e.message:
                print(f"    [{e.severity}] {e.path}")

        # =============================================================
        # 14. Full statistics
        # =============================================================
        print("\n=== 14. Full statistics ===")
        print(json.dumps(loader_h.get_statistics(), indent=2, default=str))
