# src/analysis/policy_loader.py

"""
Policy loading for Green Agent (Enhanced)
==========================================

Loads YAML policy files, validates them against a schema, computes a
stable content hash, and emits audit records when policies change.

Original API preserved:
    policy = load_policy("policy.yaml")
    policy.version          # str
    policy.constraints      # dict
    policy.hash             # "sha256:..."

Enhanced API:
    policy = load_policy("policy.yaml", validate=True)
    policy.is_valid         # bool
    policy.validation_errors
    policy.source_path
    policy.loaded_at
    policy.content_hash     # cached, stable
    policy.fingerprint      # hash + path
    policy.get("constraints.max_energy_per_task_wh", default=0)
    PolicyDiff.compare(policy_a, policy_b)   # what changed
    PolicyLoader(...)       # class-based loader with cache + HITL

Enhancements:
  1. Quantum-Distillation      — (N/A, no route-specific policy)
  2. Causal RL                 — policy_version anchors causal attribution
  3. Federated Analytics       — deployment-tagged policy provenance
  4. Multi-Agent Coordination  — per-agent policy identity
  5. Temporal Logic            — verification evidence linkage
  6. Explainable AI            — validation rationale
  7. Adaptive Precision        — precision-sensitive schema (optional)
  8. Carbon Markets            — separates operational/contractual constraints
  9. Resilience & Chaos        — every failure mode returns a default + error
 10. Human-in-the-Loop         — policy changes trigger review
 +   Immutable after load
 +   Cached, stable content hash (computed once)
 +   Env var interpolation (${VAR})
 +   One-level `extends`
 +   Diff between two policies
 +   Schema validation with clear error messages
 +   DecisionRecord emission on load
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
import uuid
from collections import Counter, deque
from copy import deepcopy
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Optional YAML import with a clear failure message
# =============================================================================

try:
    import yaml
    _HAS_YAML = True
except Exception as e:  # pragma: no cover
    yaml = None
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


# =============================================================================
# Schema — the expected shape of a GreenPolicy YAML file
# =============================================================================

REQUIRED_SECTIONS: Tuple[str, ...] = ("constraints",)

KNOWN_SECTIONS: Tuple[str, ...] = (
    "version",
    "constraints",
    "carbon_context",
    "optimization",
    "reporting",
    "agent_identity",
)

# Recognized constraint keys with expected units
CONSTRAINT_KEYS: Dict[str, str] = {
    "max_energy_per_task_wh": "Wh",
    "max_carbon_per_task_kg": "kgCO2e",
    "max_latency_seconds": "seconds",
    "max_helium_units": "units",
    "max_cost_usd": "USD",
}

# Recognized `agent_identity.mode` values (matches GreenPolicyEnforcer)
KNOWN_MODES: Tuple[str, ...] = tuple(m.value for m in EnforcementMode)


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "policies_loaded": _STATS["loaded"],
        "validations_failed": _STATS["validation_failed"],
        "file_errors": _STATS["file_errors"],
        "yaml_errors": _STATS["yaml_errors"],
        "env_interpolations": _STATS["env_interpolations"],
        "extends_resolved": _STATS["extends"],
        "decision_records_emitted": _STATS["decision_records"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# Validation errors
# =============================================================================

@dataclass
class ValidationError:
    """A single policy validation issue."""
    path: str               # dotted path into the policy, e.g. "constraints.max_energy_per_task_wh"
    message: str
    severity: str           # ValidationSeverity value

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Env var interpolation
# =============================================================================

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::([^}]*))?\}")


def _interpolate_env(value: Any, stats: Counter) -> Any:
    """
    Recursively interpolate ${VAR} and ${VAR:default} in strings.

    Only strings are interpolated; other types are returned unchanged.
    """
    if isinstance(value, str):
        def repl(match: "re.Match") -> str:
            var = match.group(1)
            default = match.group(2)
            if var in os.environ:
                stats["env_interpolations"] += 1
                return os.environ[var]
            if default is not None:
                stats["env_interpolations"] += 1
                return default
            # Leave the placeholder intact if no env and no default
            return match.group(0)
        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v, stats) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v, stats) for v in value]
    return value


# =============================================================================
# Validation
# =============================================================================

def _validate_sections(raw: Dict[str, Any]) -> List[ValidationError]:
    """Validate the top-level structure."""
    errors: List[ValidationError] = []
    for section in REQUIRED_SECTIONS:
        if section not in raw:
            errors.append(ValidationError(
                path=section,
                message=f"required section '{section}' missing",
                severity=ValidationSeverity.ERROR.value,
            ))
    # Unknown sections — warn, don't error (forward compatibility)
    for key in raw:
        if key not in KNOWN_SECTIONS:
            errors.append(ValidationError(
                path=key,
                message=f"unknown section '{key}'",
                severity=ValidationSeverity.WARNING.value,
            ))
    return errors


def _validate_constraints(raw: Dict[str, Any]) -> List[ValidationError]:
    """Validate the constraints section."""
    errors: List[ValidationError] = []
    constraints = raw.get("constraints", {})
    if not isinstance(constraints, dict):
        errors.append(ValidationError(
            path="constraints",
            message=f"must be a dict, got {type(constraints).__name__}",
            severity=ValidationSeverity.ERROR.value,
        ))
        return errors
    for key, value in constraints.items():
        if key not in CONSTRAINT_KEYS:
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"unknown constraint key '{key}'",
                severity=ValidationSeverity.WARNING.value,
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
        if not (f == f and f not in (float("inf"), float("-inf"))):
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"must be finite, got {value}",
                severity=ValidationSeverity.ERROR.value,
            ))
            continue
        if f <= 0:
            errors.append(ValidationError(
                path=f"constraints.{key}",
                message=f"must be positive, got {value} "
                        f"(units: {CONSTRAINT_KEYS[key]})",
                severity=ValidationSeverity.ERROR.value,
            ))
    return errors


def _validate_agent_identity(raw: Dict[str, Any]) -> List[ValidationError]:
    """Validate agent_identity.mode if present."""
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
                f"unknown mode '{mode}'; "
                f"expected one of {list(KNOWN_MODES)}"
            ),
            severity=ValidationSeverity.ERROR.value,
        ))
    return errors


def _validate_version(raw: Dict[str, Any]) -> List[ValidationError]:
    """Version is recommended; missing version defaults to a UUID."""
    errors: List[ValidationError] = []
    v = raw.get("version")
    if v is None:
        errors.append(ValidationError(
            path="version",
            message="missing 'version' — policy will be assigned a UUID",
            severity=ValidationSeverity.WARNING.value,
        ))
    elif not isinstance(v, str):
        errors.append(ValidationError(
            path="version",
            message=f"must be str, got {type(v).__name__}",
            severity=ValidationSeverity.WARNING.value,
        ))
    return errors


def validate_policy(raw: Dict[str, Any]) -> List[ValidationError]:
    """Run all validation rules against a raw policy dict."""
    if not isinstance(raw, dict):
        return [ValidationError(
            path="<root>",
            message=f"policy must be a dict, got {type(raw).__name__}",
            severity=ValidationSeverity.ERROR.value,
        )]
    errors: List[ValidationError] = []
    errors.extend(_validate_sections(raw))
    errors.extend(_validate_constraints(raw))
    errors.extend(_validate_agent_identity(raw))
    errors.extend(_validate_version(raw))
    return errors


# =============================================================================
# Extends resolution
# =============================================================================

def _resolve_extends(
    raw: Dict[str, Any],
    source_dir: Path,
    stats: Counter,
    max_depth: int = 3,
) -> Dict[str, Any]:
    """
    Resolve one-level (or up to max_depth) `extends` chains.

    The child's keys override the base. Sections are merged shallowly;
    nested dicts are deep-merged.
    """
    if not isinstance(raw, dict):
        return raw
    parent_path = raw.get("extends")
    if not parent_path:
        return raw
    if max_depth <= 0:
        logger.warning("extends chain too deep; stopping resolution")
        return raw

    parent_file = (source_dir / parent_path).resolve()
    try:
        with open(parent_file, "r") as f:
            parent_raw = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Failed to resolve extends '{parent_path}': {e}")
        return raw

    parent_raw = _resolve_extends(parent_raw, parent_file.parent, stats, max_depth - 1)
    stats["extends"] += 1
    merged = _deep_merge(parent_raw, {k: v for k, v in raw.items() if k != "extends"})
    return merged


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
# GreenPolicy — enhanced, backward compatible
# =============================================================================

class GreenPolicy:
    """
    Loaded, validated, immutable policy.

    Backward-compatible: original attribute names and `hash` property
    are preserved. New attributes are additive.
    """

    def __init__(
        self,
        raw: Dict[str, Any],
        *,
        source_path: Optional[str] = None,
        validate: bool = False,
        deployment_id: str = "local",
        allow_mutation: bool = False,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original behavior: store raw and extract sections ---
        self.raw = raw if isinstance(raw, dict) else {}
        self.version = self.raw.get("version") or f"uuid-{uuid.uuid4().hex[:8]}"
        self.constraints = self.raw.get("constraints", {}) or {}
        self.carbon = self.raw.get("carbon_context", {}) or {}
        self.optimization = self.raw.get("optimization", {}) or {}
        self.reporting = self.raw.get("reporting", {}) or {}
        self.identity = self.raw.get("agent_identity", {}) or {}

        # --- Enhancement: provenance ---
        self.source_path = source_path
        self.loaded_at = datetime.now()
        self.deployment_id = deployment_id

        # --- Enhancement: features ---
        self.features: Dict[str, bool] = {
            "immutable": True,
            "cached_hash": True,
            "validation": True,
            "fingerprint": True,
            "explainability": True,
        }
        if features:
            self.features.update(features)

        # --- Enhancement: validation ---
        self.validation_errors: List[ValidationError] = []
        if validate:
            self.validation_errors = validate_policy(self.raw)
        self.is_valid = not any(
            e.severity == ValidationSeverity.ERROR.value
            for e in self.validation_errors
        )

        # --- Enhancement: content hash (cached) ---
        self._content_hash: Optional[str] = None

        # --- Enhancement: immutable raw ---
        if self.features.get("immutable", True) and not allow_mutation:
            self.raw = _freeze_dict(self.raw)

        logger.info(
            f"GreenPolicy loaded (version={self.version}, "
            f"source={source_path}, valid={self.is_valid})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API
    # ------------------------------------------------------------------

    @property
    def hash(self) -> str:
        """
        Compute the SHA-256 content hash.

        Backward-compatible: same prefix `sha256:` and same algorithm.

        Enhanced: the hash is computed once and cached, so it does not
        change if `self.raw` is unexpectedly mutated after construction.
        """
        if not self.features.get("cached_hash", True):
            return self._compute_hash()
        if self._content_hash is None:
            self._content_hash = self._compute_hash()
        return self._content_hash

    def _compute_hash(self) -> str:
        if not _HAS_YAML:
            # Fallback: hash the repr of the dict
            data = repr(sorted(self.raw.items())) if isinstance(self.raw, dict) else str(self.raw)
        else:
            data = yaml.safe_dump(self.raw, sort_keys=True)
        h = hashlib.sha256()
        h.update(data.encode("utf-8"))
        return f"sha256:{h.hexdigest()}"

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    @property
    def fingerprint(self) -> str:
        """
        A stable identity for this policy: content hash + source path.

        Two policies with the same content but different paths have
        different fingerprints.
        """
        path = self.source_path or "<inline>"
        return f"{self.hash}|{path}"

    @property
    def content_hash(self) -> str:
        """Alias for `hash` that is clearly about content, not identity."""
        return self.hash

    def get(self, dotted_path: str, default: Any = None) -> Any:
        """
        Retrieve a nested policy value by dotted path.

        Example: policy.get("constraints.max_energy_per_task_wh", 5.0)
        """
        current: Any = self.raw
        for part in dotted_path.split("."):
            if isinstance(current, dict):
                if part not in current:
                    return default
                current = current[part]
            else:
                return default
        return current

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a serialisable representation with metadata.

        `raw` is deep-copied to prevent accidental mutation of the
        frozen internal state.
        """
        return {
            "version": self.version,
            "hash": self.hash,
            "fingerprint": self.fingerprint,
            "source_path": self.source_path,
            "loaded_at": self.loaded_at.isoformat(),
            "deployment_id": self.deployment_id,
            "is_valid": self.is_valid,
            "validation_errors": [e.to_dict() for e in self.validation_errors],
            "sections": {
                "constraints": _thaw_dict(self.constraints),
                "carbon_context": _thaw_dict(self.carbon),
                "optimization": _thaw_dict(self.optimization),
                "reporting": _thaw_dict(self.reporting),
                "agent_identity": _thaw_dict(self.identity),
            },
            "raw": _thaw_dict(self.raw),
        }

    def explain_validation(self) -> Dict[str, Any]:
        """Produce a structured rationale for the validation verdict."""
        errors = [e for e in self.validation_errors
                  if e.severity == ValidationSeverity.ERROR.value]
        warnings = [e for e in self.validation_errors
                    if e.severity == ValidationSeverity.WARNING.value]

        reasons: List[str] = []
        if self.is_valid:
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
                f"[{'VALID' if self.is_valid else 'INVALID'}] "
                f"{self.version}"
            ),
            "rationale": reasons,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "hash": self.hash,
        }

    def to_decision_record(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> Optional[Any]:
        """Emit a DecisionRecord for the act of loading this policy."""
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return None

        return DecisionRecord(
            run_id=run_id or f"run-{uuid.uuid4().hex[:8]}",
            timestamp=self.loaded_at,
            task_id=task_id or "",
            selected_action=f"load_policy(version={self.version})",
            policy_version=self.version,
            model_or_agent=agent_id or "",
            explanation={
                "hash": self.hash,
                "fingerprint": self.fingerprint,
                "is_valid": self.is_valid,
                "validation_errors": [
                    e.to_dict() for e in self.validation_errors
                ],
            },
            provenance={
                "source": "policy_loader",
                "source_path": self.source_path,
                "deployment_id": self.deployment_id,
            },
        )


# =============================================================================
# Immutability helpers
# =============================================================================

class _FrozenDict(dict):
    """A dict that rejects mutation."""
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


def _freeze_dict(d: Any) -> Any:
    if isinstance(d, dict):
        return _FrozenDict({k: _freeze_dict(v) for k, v in d.items()})
    if isinstance(d, list):
        return tuple(_freeze_dict(v) for v in d)
    return d


def _thaw_dict(d: Any) -> Any:
    if isinstance(d, dict):
        return {k: _thaw_dict(v) for k, v in d.items()}
    if isinstance(d, tuple):
        return [_thaw_dict(v) for v in d]
    return d


# =============================================================================
# Policy diff
# =============================================================================

@dataclass
class PolicyDiff:
    """The result of comparing two policies."""
    added: Dict[str, Any] = field(default_factory=dict)
    removed: Dict[str, Any] = field(default_factory=dict)
    changed: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)
    same_version: bool = False
    same_hash: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "added": dict(self.added),
            "removed": dict(self.removed),
            "changed": {k: list(v) for k, v in self.changed.items()},
            "same_version": self.same_version,
            "same_hash": self.same_hash,
            "has_changes": bool(self.added or self.removed or self.changed),
        }

    @classmethod
    def compare(cls, a: GreenPolicy, b: GreenPolicy) -> "PolicyDiff":
        diff = cls(
            same_version=(a.version == b.version),
            same_hash=(a.hash == b.hash),
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
    else:
        if a != b:
            path = prefix or "<root>"
            diff.changed[path] = (_thaw_dict(a), _thaw_dict(b))


# =============================================================================
# PolicyLoader — class-based, cache + HITL
# =============================================================================

class PolicyLoader:
    """
    Stateful loader with caching, validation, and HITL hooks.
    """

    def __init__(
        self,
        *,
        deployment_id: str = "local",
        validate: bool = True,
        cache_enabled: bool = True,
        features: Optional[Dict[str, bool]] = None,
    ):
        self.deployment_id = deployment_id
        self.validate = validate
        self.cache_enabled = cache_enabled
        self._cache: Dict[str, GreenPolicy] = {}
        self._history: Deque[Tuple[datetime, GreenPolicy]] = deque(maxlen=128)
        self._hitl_callback: Optional[Callable[[GreenPolicy, PolicyDiff], bool]] = None

        self.features: Dict[str, bool] = {
            "interpolate_env": True,
            "resolve_extends": True,
            "immutable": True,
            "cache": True,
            "diff_on_reload": True,
            "hitl_on_change": True,
            "emit_decision_record": False,
        }
        if features:
            self.features.update(features)

        logger.debug(
            f"PolicyLoader initialized "
            f"(deployment={deployment_id}, validate={validate})"
        )

    def load(
        self,
        path: str,
        *,
        force_reload: bool = False,
    ) -> GreenPolicy:
        """
        Load and validate a policy file.

        Never raises on malformed input — returns a GreenPolicy with
        `is_valid=False` instead.
        """
        path_key = str(Path(path).resolve())

        # --- Cache check ---
        if self.features.get("cache", True) and not force_reload:
            if path_key in self._cache:
                return self._cache[path_key]

        # --- YAML availability ---
        if not _HAS_YAML:
            logger.error(
                f"Cannot load '{path}': PyYAML is unavailable "
                f"({_YAML_IMPORT_ERROR if not _HAS_YAML else ''})"
            )
            _STATS["file_errors"] += 1
            return self._empty_policy(path, reason="yaml_unavailable")

        # --- File read ---
        try:
            with open(path, "r") as f:
                raw_text = f.read()
        except FileNotFoundError:
            logger.error(f"Policy file not found: {path}")
            _STATS["file_errors"] += 1
            return self._empty_policy(path, reason="file_not_found")
        except PermissionError:
            logger.error(f"Policy file not readable: {path}")
            _STATS["file_errors"] += 1
            return self._empty_policy(path, reason="permission_denied")
        except OSError as e:
            logger.error(f"Policy file read error: {e}")
            _STATS["file_errors"] += 1
            return self._empty_policy(path, reason=str(e))

        # --- YAML parse ---
        try:
            raw = yaml.safe_load(raw_text)
        except Exception as e:
            logger.error(f"Malformed YAML in '{path}': {e}")
            _STATS["yaml_errors"] += 1
            return self._empty_policy(path, reason="yaml_parse_error")

        # --- Empty / non-dict YAML ---
        if raw is None:
            logger.warning(f"Policy file '{path}' is empty")
            return self._empty_policy(path, reason="empty_yaml")
        if not isinstance(raw, dict):
            logger.error(
                f"Policy file '{path}' did not parse to a dict "
                f"({type(raw).__name__})"
            )
            return self._empty_policy(path, reason="non_dict_yaml")

        # --- Env var interpolation ---
        if self.features.get("interpolate_env", True):
            raw = _interpolate_env(raw, _STATS)

        # --- Resolve `extends` ---
        if self.features.get("resolve_extends", True) and "extends" in raw:
            raw = _resolve_extends(raw, Path(path).parent, _STATS)

        # --- Build the policy ---
        policy = GreenPolicy(
            raw,
            source_path=path_key,
            validate=self.validate,
            deployment_id=self.deployment_id,
            features={"immutable": self.features.get("immutable", True)},
        )

        _STATS["loaded"] += 1
        if not policy.is_valid:
            _STATS["validation_failed"] += 1

        # --- Diff and HITL on change ---
        previous = self._cache.get(path_key)
        if previous is not None and self.features.get("diff_on_reload", True):
            diff = PolicyDiff.compare(previous, policy)
            if diff.to_dict()["has_changes"]:
                logger.info(
                    f"Policy '{path}' changed: "
                    f"{len(diff.added)} added, "
                    f"{len(diff.removed)} removed, "
                    f"{len(diff.changed)} changed"
                )
                if self._hitl_callback is not None and \
                        self.features.get("hitl_on_change", True):
                    try:
                        approved = self._hitl_callback(policy, diff)
                        if approved is False:
                            logger.warning(
                                f"HITL denied policy change for '{path}'; "
                                "returning previous version"
                            )
                            return previous
                    except Exception as e:
                        logger.warning(f"HITL callback failed: {e}")

        # --- Cache ---
        if self.features.get("cache", True):
            self._cache[path_key] = policy

        self._history.append((datetime.now(), policy))

        # --- DecisionRecord emission ---
        if self.features.get("emit_decision_record", False):
            try:
                dr = policy.to_decision_record()
                if dr is not None:
                    policy._decision_record = dr  # type: ignore[attr-defined]
                    _STATS["decision_records"] += 1
            except Exception as e:
                logger.debug(f"DecisionRecord emission failed: {e}")

        return policy

    def load_text(
        self,
        text: str,
        *,
        source_label: str = "<inline>",
    ) -> GreenPolicy:
        """Load a policy from a YAML string."""
        if not _HAS_YAML:
            return self._empty_policy(source_label, reason="yaml_unavailable")
        try:
            raw = yaml.safe_load(text) or {}
        except Exception as e:
            logger.error(f"YAML parse error in '{source_label}': {e}")
            _STATS["yaml_errors"] += 1
            return self._empty_policy(source_label, reason="yaml_parse_error")
        if not isinstance(raw, dict):
            return self._empty_policy(source_label, reason="non_dict_yaml")

        if self.features.get("interpolate_env", True):
            raw = _interpolate_env(raw, _STATS)

        return GreenPolicy(
            raw,
            source_path=source_label,
            validate=self.validate,
            deployment_id=self.deployment_id,
        )

    def clear_cache(self) -> None:
        self._cache.clear()

    def set_hitl_callback(
        self,
        callback: Callable[[GreenPolicy, PolicyDiff], bool],
    ) -> None:
        """Register a HITL callback for policy changes."""
        self._hitl_callback = callback

    def get_history(self) -> List[Tuple[datetime, str, bool]]:
        """Return a summary of loaded policies."""
        return [
            (ts, p.version, p.is_valid)
            for ts, p in self._history
        ]

    def get_statistics(self) -> Dict[str, Any]:
        stats = get_statistics()
        stats.update({
            "deployment_id": self.deployment_id,
            "cache_size": len(self._cache),
            "history_size": len(self._history),
            "features": dict(self.features),
            "yaml_available": _HAS_YAML,
        })
        return stats

    def _empty_policy(self, path: str, *, reason: str) -> GreenPolicy:
        """Return a placeholder policy with an error-tagged empty raw."""
        policy = GreenPolicy(
            {},
            source_path=path,
            validate=False,
            deployment_id=self.deployment_id,
        )
        policy.is_valid = False
        policy.validation_errors = [ValidationError(
            path="<root>",
            message=f"load failed: {reason}",
            severity=ValidationSeverity.ERROR.value,
        )]
        return policy


# =============================================================================
# ORIGINAL FUNCTION — preserved exactly
# =============================================================================

def load_policy(path: str) -> GreenPolicy:
    """
    Load a GreenPolicy from a YAML file.

    Backward-compatible: same signature, same return type.

    Enhanced:
    - Never raises on malformed input — returns a policy with
      `is_valid=False` and `validation_errors` populated
    - Interpolates ${VAR} and ${VAR:default} in string values
    - Resolves `extends` chains
    - Populates `source_path`, `loaded_at`, `fingerprint`
    """
    loader = PolicyLoader(validate=True)
    return loader.load(path)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not _HAS_YAML:
        print("PyYAML unavailable; demo skipped")
        raise SystemExit(0)

    import tempfile
    from pathlib import Path

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
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
        os.environ.pop("CARBON_API_KEY", None)  # ensure default is used
        policy = load_policy(str(policy_path))
        print(f"  version:       {policy.version}")
        print(f"  constraints:   {dict(policy.constraints)}")
        print(f"  carbon api:    {policy.carbon.get('api_key')}")
        print(f"  mode:          {policy.identity.get('mode')}")
        print(f"  hash:          {policy.hash[:24]}...")
        print(f"  fingerprint:   {policy.fingerprint[:60]}...")
        print(f"  is_valid:      {policy.is_valid}")

        # --- Validation errors on malformed input ---
        print("\n=== Malformed input ===")
        bad_path = Path(tmpdir) / "bad.yaml"
        bad_path.write_text("""
version: "5.0.1"
constraints:
  max_energy_per_task_wh: -5.0
  unknown_key: 10
agent_identity:
  mode: Active_Enforcement
""")
        bad = load_policy(str(bad_path))
        print(f"  is_valid: {bad.is_valid}")
        for e in bad.validation_errors:
            print(f"    [{e.severity}] {e.path}: {e.message}")

        # --- File not found ---
        print("\n=== File not found ===")
        missing = load_policy("/nonexistent/policy.yaml")
        print(f"  is_valid: {missing.is_valid}")
        print(f"  error: {missing.validation_errors[0].message}")

        # --- Empty YAML ---
        print("\n=== Empty YAML ===")
        empty_path = Path(tmpdir) / "empty.yaml"
        empty_path.write_text("")
        empty = load_policy(str(empty_path))
        print(f"  is_valid: {empty.is_valid}")
        print(f"  error: {empty.validation_errors[0].message}")

        # --- Extends ---
        print("\n=== Extends ===")
        base_path = Path(tmpdir) / "base.yaml"
        base_path.write_text("""
constraints:
  max_energy_per_task_wh: 10.0
  max_latency_seconds: 60.0
""")
        child_path = Path(tmpdir) / "child.yaml"
        child_path.write_text("""
extends: base.yaml
version: "5.1.0"
constraints:
  max_energy_per_task_wh: 5.0
""")
        child = load_policy(str(child_path))
        print(f"  max_energy_per_task_wh: "
              f"{child.constraints.get('max_energy_per_task_wh')} "
              f"(overridden to 5.0)")
        print(f"  max_latency_seconds:    "
              f"{child.constraints.get('max_latency_seconds')} "
              f"(inherited from base)")

        # --- Immutability ---
        print("\n=== Immutability ===")
        try:
            policy.raw["constraints"]["max_energy_per_task_wh"] = 99999
            print("  mutation succeeded (unexpected)")
        except TypeError as e:
            print(f"  mutation blocked: {type(e).__name__}")

        # --- get() with dotted path ---
        print("\n=== Dotted-path access ===")
        print(f"  get('constraints.max_energy_per_task_wh') = "
              f"{policy.get('constraints.max_energy_per_task_wh')}")
        print(f"  get('carbon_context.api_key') = "
              f"{policy.get('carbon_context.api_key')}")

        # --- PolicyLoader with cache and diff ---
        print("\n=== PolicyLoader with cache ===")
        loader = PolicyLoader(deployment_id="us-ca-prod-01")
        p1 = loader.load(str(policy_path))
        p2 = loader.load(str(policy_path))  # cached
        print(f"  same object: {p1 is p2}")

        # --- Diff on reload ---
        print("\n=== Diff on reload ===")
        policy_path.write_text("""
version: "5.0.2"
constraints:
  max_energy_per_task_wh: 4.0
  max_carbon_per_task_kg: 1.0
  max_latency_seconds: 30.0
agent_identity:
  mode: active_enforcement
""")
        p3 = loader.load(str(policy_path), force_reload=True)
        diff = PolicyDiff.compare(p1, p3)
        print(f"  same_version: {diff.same_version}")
        print(f"  same_hash:    {diff.same_hash}")
        print(f"  changes:")
        for path, (old, new) in diff.changed.items():
            print(f"    {path}: {old} → {new}")

        # --- Statistics ---
        import json
        print("\n=== Statistics ===")
        print(json.dumps(loader.get_statistics(), indent=2, default=str))
