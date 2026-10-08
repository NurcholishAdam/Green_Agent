# src/memory/supermemory_config.py

"""
Supermemory Configuration
=========================

Frozen configuration for the Supermemory adapter, recall layer, and write
governor.

Enhancements
------------
- ``SupermemoryConfig`` — frozen, validated: mode, endpoints, budgets,
  TTLs, adapter limits (``max_recall_k``, ``write_retry_backoff_seconds``,
  ``latency_ring_size``), ``schema_version``.
- Immutable ``ttl_seconds`` mapping (``MappingProxyType``); config
  hashable. ``_DEFAULT_TTLS`` is also frozen at module level.
- ``SupermemoryMode`` enum (str-compatible) with ``coerce()`` /
  ``values()`` / ``description``.
- ``from_env()`` reads ``GREEN_AGENT_SUPERMEMORY_*`` env vars, including
  JSON-encoded ``TRUTH_LEVELS`` / ``TTL_SECONDS`` and per-kind
  ``TTL_<KIND>`` overrides; JSON parsing errors are wrapped. Integer-
  valued floats (``3600.0``) are accepted; ``NaN`` / ``inf`` / ``bool``
  are rejected.
- ``with_env()`` layered builder (defaults → env → explicit overrides)
  and ``to_env()`` exporter.
- ``to_dict()`` / ``from_dict()`` round-trip with ``strict`` mode,
  ``redact_secrets`` and ``schema_version``. ``to_json()`` supports
  ``redact_secrets`` / ``redacted_value`` / ``indent``.
- ``assert_compatible()`` classmethod.
- ``with_overrides()`` / ``merge()`` convenience constructors.
- URL and container-tag validation. The container-tag regex no longer
  accepts ``.`` or ``/`` — those aren't produced by any other module.
- ``ttl_for()`` with explicit ``default`` and warning on fallback.
- Rejects ``bool`` for numeric fields; rejects ``NaN`` / ``inf``
  timeouts.
- Custom ``SupermemoryConfigError(ValueError)``.
- ``__version__`` / ``SCHEMA_VERSION`` / ``DEFAULT_CONTAINER_TAG`` /
  ``DEFAULT_TRUTH_LEVELS`` exported via ``__all__``.
- ``__main__`` smoke test covering every new path.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

__version__ = "6.1.0"

#: Version of the config contract itself.
SCHEMA_VERSION: int = 1

#: Module-level default container tag. Kept in sync with
#: ``memory_schemas.DEFAULT_CONTAINER_TAG`` and ``SupermemoryAdapter``.
DEFAULT_CONTAINER_TAG: str = "org:green-agent"

#: Public truth-level vocabulary. Mirrors ``memory_schemas``.
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SupermemoryConfigError(ValueError):
    """Raised for invalid Supermemory configuration."""


# --------------------------------------------------------------------------- #
# Mode enum
# --------------------------------------------------------------------------- #
_MODE_DESCRIPTIONS: Mapping[str, str] = MappingProxyType({
    "local": "In-process Supermemory deployment on localhost",
    "cloud": "Hosted Supermemory service requiring an API key",
})


class SupermemoryMode(str, Enum):
    """Deployment mode for the Supermemory backend.

    Subclasses ``str`` so that ``SupermemoryMode.LOCAL == "local"`` and
    ``str(SupermemoryMode.LOCAL) == "local"``. This preserves compatibility
    with code that used the previous ``Literal["local", "cloud"]`` type.
    """

    LOCAL = "local"
    CLOUD = "cloud"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @property
    def description(self) -> str:
        """Human-readable description of this deployment mode."""
        return _MODE_DESCRIPTIONS.get(self.value, "Unknown mode")

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(member.value for member in cls)

    @classmethod
    def coerce(cls, value: Any) -> "SupermemoryMode":
        """Normalize a value to a ``SupermemoryMode`` member."""
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value.lower())
            except ValueError as exc:
                raise SupermemoryConfigError(
                    f"mode must be one of {list(cls.values())}, got "
                    f"{value!r}."
                ) from exc
        raise SupermemoryConfigError(
            f"mode must be a str or SupermemoryMode, got "
            f"{type(value).__name__}."
        )


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
#: Per-kind TTLs, frozen at module level so importers can't mutate them.
_DEFAULT_TTLS: Mapping[str, int] = MappingProxyType({
    "decision_outcome": 90 * 24 * 3600,   # 90 days
    "policy":           365 * 24 * 3600,  # 1 year
    "incident":         365 * 24 * 3600,  # 1 year
    "run":              180 * 24 * 3600,  # 180 days
    "grid_forecast":    6 * 3600,         # 6 hours
    "thermal_state":    30 * 60,          # 30 minutes
    "connectivity":     5 * 60,           # 5 minutes
})

#: Backward-compatible private alias.
_DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = DEFAULT_TRUTH_LEVELS

#: Container tags are colon-separated identifiers. The regex rejects
#: ``.`` and ``/`` because no other module produces tags containing them.
_CONTAINER_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:_\-]{0,127}$")

_ENV_PREFIX = "GREEN_AGENT_SUPERMEMORY_"

#: Adapter defaults — kept in sync with ``SupermemoryAdapter`` fallbacks.
_DEFAULT_MAX_RECALL_K: int = 1_000
_DEFAULT_RETRY_BACKOFF_SECONDS: float = 0.25
_DEFAULT_LATENCY_RING_SIZE: int = 200


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    """``True`` only for real ints (never for ``bool``)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_real_number(value: Any) -> bool:
    """``True`` for finite ints or floats (never for ``bool``)."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise SupermemoryConfigError(
            f"base_url must be a valid http(s) URL, got {url!r}."
        )


def _validate_container_tag(tag: str) -> None:
    if not _CONTAINER_TAG_RE.match(tag):
        raise SupermemoryConfigError(
            f"default_container_tag {tag!r} does not match "
            f"{_CONTAINER_TAG_RE.pattern!r}."
        )


def _coerce_int_env(name: str, raw: str) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise SupermemoryConfigError(
            f"Environment variable {name}={raw!r} is not a valid integer."
        ) from exc


def _coerce_float_env(name: str, raw: str) -> float:
    """Parse a finite float from an env var, rejecting NaN / inf."""
    try:
        fv = float(raw)
    except (TypeError, ValueError) as exc:
        raise SupermemoryConfigError(
            f"Environment variable {name}={raw!r} is not a valid float."
        ) from exc
    if not math.isfinite(fv):
        raise SupermemoryConfigError(
            f"Environment variable {name}={raw!r} must be finite."
        )
    return fv


def _coerce_json_env(name: str, raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SupermemoryConfigError(
            f"Environment variable {name}={raw!r} is not valid JSON."
        ) from exc


def _coerce_ttl_value(name: str, value: Any) -> int:
    """Coerce a TTL value from JSON; reject bool, NaN, and floats."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SupermemoryConfigError(
            f"{name} must be a number, got {type(value).__name__}."
        )
    if isinstance(value, float) and not value.is_integer():
        raise SupermemoryConfigError(
            f"{name} must be an integer, got {value!r}."
        )
    if not math.isfinite(float(value)):
        raise SupermemoryConfigError(f"{name} must be finite.")
    iv = int(value)
    if iv <= 0:
        raise SupermemoryConfigError(f"{name} must be > 0.")
    return iv


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SupermemoryConfig:
    """Frozen configuration for Supermemory integration.

    See module docstring for the full set of features.
    """

    api_key: Optional[str] = None
    base_url: str = "http://localhost:6767"
    mode: SupermemoryMode = SupermemoryMode.LOCAL
    default_container_tag: str = DEFAULT_CONTAINER_TAG

    max_history: int = 10_000

    recall_top_k: int = 5
    recall_token_budget: int = 2_048
    recall_timeout_seconds: float = 5.0

    write_retries: int = 2
    write_timeout_seconds: float = 5.0

    truth_levels: Tuple[str, ...] = DEFAULT_TRUTH_LEVELS

    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType(dict(_DEFAULT_TTLS)),
    )

    # Adapter limits (read by ``SupermemoryAdapter`` via ``getattr``).
    max_recall_k: int = _DEFAULT_MAX_RECALL_K
    write_retry_backoff_seconds: float = _DEFAULT_RETRY_BACKOFF_SECONDS
    latency_ring_size: int = _DEFAULT_LATENCY_RING_SIZE

    # Schema version — stamped on serialized payloads.
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    # Post-init: normalize + validate
    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # --- schema_version ---
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise SupermemoryConfigError(
                "schema_version must be a positive int."
            )

        # --- mode normalization (accept str or enum) ---
        object.__setattr__(
            self, "mode", SupermemoryMode.coerce(self.mode),
        )

        # --- api_key normalization ---
        if self.api_key is not None:
            if not isinstance(self.api_key, str):
                raise SupermemoryConfigError(
                    "api_key must be a string or None."
                )
            object.__setattr__(self, "api_key", self.api_key or None)

        if self.mode is SupermemoryMode.CLOUD and not self.api_key:
            raise SupermemoryConfigError(
                "api_key is required when mode='cloud'."
            )

        # --- base_url ---
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            raise SupermemoryConfigError(
                "base_url must be a non-empty string."
            )
        _validate_url(self.base_url)

        # --- container tag ---
        if (
            not isinstance(self.default_container_tag, str)
            or not self.default_container_tag
        ):
            raise SupermemoryConfigError(
                "default_container_tag must be a non-empty string."
            )
        _validate_container_tag(self.default_container_tag)

        # --- positive ints ---
        for name in (
            "max_history",
            "recall_top_k",
            "recall_token_budget",
            "max_recall_k",
            "latency_ring_size",
        ):
            value = getattr(self, name)
            if not _is_real_int(value) or value <= 0:
                raise SupermemoryConfigError(
                    f"{name} must be a positive int (got {value!r})."
                )

        # --- non-negative int ---
        if not _is_real_int(self.write_retries) or self.write_retries < 0:
            raise SupermemoryConfigError(
                "write_retries must be a non-negative int "
                f"(got {self.write_retries!r})."
            )

        # --- positive finite floats ---
        for name in (
            "recall_timeout_seconds",
            "write_timeout_seconds",
            "write_retry_backoff_seconds",
        ):
            value = getattr(self, name)
            if not _is_real_number(value) or value <= 0:
                raise SupermemoryConfigError(
                    f"{name} must be a finite number > 0 (got {value!r})."
                )

        # --- truth_levels ---
        if isinstance(self.truth_levels, str) or not isinstance(
            self.truth_levels, (tuple, list, set, frozenset)
        ):
            raise SupermemoryConfigError(
                "truth_levels must be a sequence of strings, not "
                f"{type(self.truth_levels).__name__}."
            )
        levels: Tuple[str, ...] = tuple(self.truth_levels)
        if not levels:
            raise SupermemoryConfigError(
                "truth_levels must be a non-empty sequence."
            )
        seen: set = set()
        for level in levels:
            if not isinstance(level, str) or not level:
                raise SupermemoryConfigError(
                    "truth_levels entries must be non-empty strings."
                )
            if level in seen:
                raise SupermemoryConfigError(
                    f"truth_levels contains duplicate entry {level!r}."
                )
            seen.add(level)
        object.__setattr__(self, "truth_levels", levels)

        # --- ttl_seconds: validate and freeze ---
        if not isinstance(self.ttl_seconds, ABCMapping):
            raise SupermemoryConfigError("ttl_seconds must be a Mapping.")
        ttl_copy: Dict[str, int] = {}
        for kind, seconds in self.ttl_seconds.items():
            if not isinstance(kind, str) or not kind:
                raise SupermemoryConfigError(
                    "ttl_seconds keys must be non-empty strings."
                )
            if not _is_real_int(seconds) or seconds <= 0:
                raise SupermemoryConfigError(
                    f"ttl_seconds[{kind!r}] must be a positive int "
                    f"(got {seconds!r})."
                )
            ttl_copy[kind] = int(seconds)
        object.__setattr__(
            self, "ttl_seconds", MappingProxyType(ttl_copy),
        )

    # ------------------------------------------------------------------ #
    # Convenience properties / methods
    # ------------------------------------------------------------------ #
    def is_local(self) -> bool:
        return self.mode is SupermemoryMode.LOCAL

    def is_cloud(self) -> bool:
        return self.mode is SupermemoryMode.CLOUD

    def ttl_for(self, kind: str, default: Optional[int] = None) -> int:
        """Return the TTL (seconds) for a record kind.

        Parameters
        ----------
        kind : str
            Record kind.
        default : int, optional
            Fallback if ``kind`` is not configured. If ``None`` (the
            default), a missing ``kind`` raises ``SupermemoryConfigError``.
        """
        if kind in self.ttl_seconds:
            return int(self.ttl_seconds[kind])
        if default is None:
            raise SupermemoryConfigError(
                f"No TTL configured for record kind {kind!r}."
            )
        if not _is_real_int(default) or default <= 0:
            raise SupermemoryConfigError(
                "ttl_for default must be a positive int."
            )
        logger.warning(
            "ttl_for(%r): unknown record kind, using default %ss.",
            kind, default,
        )
        return int(default)

    def with_overrides(self, **kwargs: Any) -> "SupermemoryConfig":
        """Return a new config with the given fields replaced."""
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise SupermemoryConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(self, other: "SupermemoryConfig") -> "SupermemoryConfig":
        """Return a new config where ``other``'s explicit fields win.

        Fields of ``other`` equal to the dataclass defaults are not
        considered "explicit" and are left alone.
        """
        defaults = SupermemoryConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    @classmethod
    def with_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        *,
        strict: bool = False,
        **overrides: Any,
    ) -> "SupermemoryConfig":
        """Build a config from the environment, then apply overrides.

        Layer order: defaults → environment → ``**overrides``.
        """
        base = cls.from_env(env=env, strict=strict)
        if not overrides:
            return base
        return base.with_overrides(**overrides)

    def to_env(
        self,
        *,
        include_api_key: bool = False,
        redacted_value: str = "***",
    ) -> Dict[str, str]:
        """Export this config as ``GREEN_AGENT_SUPERMEMORY_*`` env vars.

        Parameters
        ----------
        include_api_key : bool
            If True, include the real API key. Otherwise the key is
            omitted entirely (safer for logging / test fixtures).
        redacted_value : str
            Not used when ``include_api_key=False``; kept for parity
            with ``to_dict(redact_secrets=True)``.
        """
        p = _ENV_PREFIX
        out: Dict[str, str] = {
            p + "BASE_URL": self.base_url,
            p + "MODE": self.mode.value,
            p + "DEFAULT_CONTAINER_TAG": self.default_container_tag,
            p + "MAX_HISTORY": str(self.max_history),
            p + "RECALL_TOP_K": str(self.recall_top_k),
            p + "RECALL_TOKEN_BUDGET": str(self.recall_token_budget),
            p + "RECALL_TIMEOUT_SECONDS": str(self.recall_timeout_seconds),
            p + "WRITE_RETRIES": str(self.write_retries),
            p + "WRITE_TIMEOUT_SECONDS": str(self.write_timeout_seconds),
            p + "MAX_RECALL_K": str(self.max_recall_k),
            p + "WRITE_RETRY_BACKOFF_SECONDS": str(
                self.write_retry_backoff_seconds
            ),
            p + "LATENCY_RING_SIZE": str(self.latency_ring_size),
        }
        if include_api_key and self.api_key:
            out[p + "API_KEY"] = self.api_key
        return out

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(
        self,
        *,
        redact_secrets: bool = False,
        redacted_value: str = "***",
    ) -> Dict[str, Any]:
        """Return a JSON-friendly dict of this config."""
        return {
            "schema_version": self.schema_version,
            "api_key": (
                redacted_value
                if redact_secrets and self.api_key
                else self.api_key
            ),
            "base_url": self.base_url,
            "mode": self.mode.value,
            "default_container_tag": self.default_container_tag,
            "max_history": self.max_history,
            "recall_top_k": self.recall_top_k,
            "recall_token_budget": self.recall_token_budget,
            "recall_timeout_seconds": self.recall_timeout_seconds,
            "write_retries": self.write_retries,
            "write_timeout_seconds": self.write_timeout_seconds,
            "truth_levels": list(self.truth_levels),
            "ttl_seconds": dict(self.ttl_seconds),
            "max_recall_k": self.max_recall_k,
            "write_retry_backoff_seconds": self.write_retry_backoff_seconds,
            "latency_ring_size": self.latency_ring_size,
        }

    def safe_dict(self) -> Dict[str, Any]:
        """``to_dict()`` with secrets redacted."""
        return self.to_dict(redact_secrets=True)

    def to_json(
        self,
        *,
        redact_secrets: bool = False,
        redacted_value: str = "***",
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(
                redact_secrets=redact_secrets,
                redacted_value=redacted_value,
            ),
            indent=indent,
            sort_keys=True,
        )

    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        """Raise ``SupermemoryConfigError`` if ``data`` is incompatible."""
        if not isinstance(data, ABCMapping):
            raise SupermemoryConfigError(
                "SupermemoryConfig.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise SupermemoryConfigError(
                f"invalid schema_version {v!r} in config payload."
            )
        if v > SCHEMA_VERSION:
            raise SupermemoryConfigError(
                f"config payload schema_version {v} is newer than the "
                f"current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise SupermemoryConfigError(
                f"config payload schema_version {v} is older than the "
                f"current contract {SCHEMA_VERSION}."
            )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "SupermemoryConfig":
        """Build a config from a mapping."""
        if not isinstance(data, ABCMapping):
            raise SupermemoryConfigError(
                "SupermemoryConfig.from_dict expects a Mapping."
            )

        # Forward-compatibility check.
        cls.assert_compatible(data)

        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise SupermemoryConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )

        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "truth_levels":
                if isinstance(v, str) or not isinstance(
                    v, (list, tuple, set, frozenset)
                ):
                    raise SupermemoryConfigError(
                        "truth_levels must be a sequence of strings."
                    )
                kwargs[k] = tuple(v)
            elif k == "ttl_seconds":
                if not isinstance(v, ABCMapping):
                    raise SupermemoryConfigError(
                        "ttl_seconds must be a Mapping."
                    )
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v

        try:
            return cls(**kwargs)
        except SupermemoryConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise SupermemoryConfigError(
                f"Failed to build SupermemoryConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "SupermemoryConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SupermemoryConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise SupermemoryConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    # Env loading
    # ------------------------------------------------------------------ #
    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        *,
        strict: bool = False,
    ) -> "SupermemoryConfig":
        """Build a config from ``GREEN_AGENT_SUPERMEMORY_*`` env vars."""
        e: Mapping[str, str] = env if env is not None else os.environ
        p = _ENV_PREFIX

        def _raw(name: str) -> Optional[str]:
            v = e.get(name)
            if v is None:
                return None
            v = v.strip()
            return v or None

        raw: Dict[str, Any] = {}

        if (v := _raw(p + "API_KEY")) is not None:
            raw["api_key"] = v
        if (v := _raw(p + "BASE_URL")) is not None:
            raw["base_url"] = v
        if (v := _raw(p + "MODE")) is not None:
            raw["mode"] = v.lower()
        if (v := _raw(p + "DEFAULT_CONTAINER_TAG")) is not None:
            raw["default_container_tag"] = v

        for name in (
            "max_history",
            "recall_top_k",
            "recall_token_budget",
            "write_retries",
            "max_recall_k",
            "latency_ring_size",
        ):
            env_name = p + name.upper()
            if (v := _raw(env_name)) is not None:
                raw[name] = _coerce_int_env(env_name, v)

        for name in (
            "recall_timeout_seconds",
            "write_timeout_seconds",
            "write_retry_backoff_seconds",
        ):
            env_name = p + name.upper()
            if (v := _raw(env_name)) is not None:
                raw[name] = _coerce_float_env(env_name, v)

        # JSON TRUTH_LEVELS
        if (v := _raw(p + "TRUTH_LEVELS")) is not None:
            parsed = _coerce_json_env(p + "TRUTH_LEVELS", v)
            if isinstance(parsed, str) or not isinstance(
                parsed, (list, tuple, set, frozenset)
            ):
                raise SupermemoryConfigError(
                    f"{p}TRUTH_LEVELS must be a JSON array of strings."
                )
            raw["truth_levels"] = tuple(parsed)

        # TTL overrides — start from defaults, then JSON, then per-kind.
        ttl_overrides: Dict[str, int] = {}

        if (v := _raw(p + "TTL_SECONDS")) is not None:
            parsed = _coerce_json_env(p + "TTL_SECONDS", v)
            if not isinstance(parsed, ABCMapping):
                raise SupermemoryConfigError(
                    f"{p}TTL_SECONDS must be a JSON object."
                )
            for kind, seconds in parsed.items():
                if not isinstance(kind, str) or not kind:
                    raise SupermemoryConfigError(
                        f"{p}TTL_SECONDS keys must be non-empty strings."
                    )
                # Integer-valued floats (3600.0) accepted; bool / NaN
                # rejected.
                ttl_overrides[kind] = _coerce_ttl_value(
                    f"{p}TTL_SECONDS[{kind!r}]", seconds,
                )

        # Per-kind TTL_<KIND> overrides. Iterates the env mapping once.
        per_kind_prefix = p + "TTL_"
        reserved = {"TTL_SECONDS"}
        # A cheap pre-check: only iterate if any key matches the prefix.
        has_per_kind = any(
            k.startswith(per_kind_prefix) for k in e.keys()
        )
        if has_per_kind:
            for env_key, env_val in e.items():
                if not env_key.startswith(per_kind_prefix):
                    continue
                suffix = env_key[len(per_kind_prefix):]
                if not suffix or suffix in reserved:
                    continue
                if env_val is None:
                    continue
                kind = suffix.lower()
                ttl_overrides[kind] = _coerce_int_env(env_key, env_val)

        if ttl_overrides:
            merged = dict(_DEFAULT_TTLS)
            merged.update(ttl_overrides)
            raw["ttl_seconds"] = merged

        return cls.from_dict(raw, strict=strict)

    # ------------------------------------------------------------------ #
    # Dunder helpers
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:
        return (
            "SupermemoryConfig("
            f"mode={self.mode.value!r}, "
            f"base_url={self.base_url!r}, "
            f"container_tag={self.default_container_tag!r}, "
            f"recall_top_k={self.recall_top_k}, "
            f"history={self.max_history})"
        )

    def __hash__(self) -> int:
        # dataclass(frozen=True) would normally auto-generate __hash__,
        # but MappingProxyType is unhashable, so we define it explicitly.
        return hash((
            self.api_key,
            self.base_url,
            self.mode,
            self.default_container_tag,
            self.max_history,
            self.recall_top_k,
            self.recall_token_budget,
            self.recall_timeout_seconds,
            self.write_retries,
            self.write_timeout_seconds,
            self.truth_levels,
            tuple(sorted(self.ttl_seconds.items())),
            self.max_recall_k,
            self.write_retry_backoff_seconds,
            self.latency_ring_size,
            self.schema_version,
        ))


__all__ = [
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_TRUTH_LEVELS",
    "SCHEMA_VERSION",
    "SupermemoryConfig",
    "SupermemoryConfigError",
    "SupermemoryMode",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.supermemory_config
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    cfg = SupermemoryConfig()
    print("repr       :", cfg)
    print("version    :", __version__)
    print("schema ver :", SCHEMA_VERSION)
    print("default tag:", DEFAULT_CONTAINER_TAG)
    print("mode       :", cfg.mode, "| is_local:", cfg.is_local())
    print("mode desc  :", cfg.mode.description)
    print("ttl(dec)   :", cfg.ttl_for("decision_outcome"))
    print("ttl(grid)  :", cfg.ttl_for("grid_forecast"))
    print("ttl(fall)  :", cfg.ttl_for("unknown_kind", default=1234))

    # Immutability of ttl_seconds.
    try:
        cfg.ttl_seconds["policy"] = 1  # type: ignore[index]
    except TypeError as exc:
        print("frozen ttl : OK ->", type(exc).__name__)
    else:
        raise AssertionError("ttl_seconds should be immutable")

    # Module-level default TTLs are frozen.
    try:
        _DEFAULT_TTLS["policy"] = 0  # type: ignore[index]
    except TypeError:
        print("frozen defs: OK")
    else:
        raise AssertionError("_DEFAULT_TTLS should be immutable")

    # Hashability.
    hash(cfg)
    print("hashable   : OK")

    # Adapter limits are present (item #2).
    assert cfg.max_recall_k == 1_000
    assert cfg.write_retry_backoff_seconds == 0.25
    assert cfg.latency_ring_size == 200
    print("adapter limits: OK")

    # Schema version is stamped on to_dict (item #8).
    assert cfg.to_dict()["schema_version"] == SCHEMA_VERSION
    print("schema stamp   : OK")

    # SupermemoryMode.coerce / values / description (item #4).
    assert SupermemoryMode.coerce("local") is SupermemoryMode.LOCAL
    assert SupermemoryMode.coerce(SupermemoryMode.CLOUD) is SupermemoryMode.CLOUD
    assert SupermemoryMode.values() == ("local", "cloud")
    assert "In-process" in SupermemoryMode.LOCAL.description
    try:
        SupermemoryMode.coerce("bogus")
    except SupermemoryConfigError:
        print("mode coerce: OK")

    # Validation failures.
    for bad in (
        dict(mode="cloud"),                            # missing api_key
        dict(mode="bogus"),                            # bad mode
        dict(max_history=0),                           # zero
        dict(max_history=True),                        # bool not allowed
        dict(recall_top_k=-1),                         # negative
        dict(recall_timeout_seconds=0),                # zero
        dict(recall_timeout_seconds=float("inf")),     # inf
        dict(base_url="not a url"),                    # bad url
        dict(default_container_tag="bad tag"),         # bad tag (space)
        dict(default_container_tag="bad/tag"),         # bad tag (slash, item #6)
        dict(default_container_tag="bad.tag"),         # bad tag (dot, item #6)
        dict(truth_levels="measured"),                 # str not sequence
        dict(truth_levels=()),                         # empty
        dict(truth_levels=("measured", "measured")),   # duplicate
        dict(ttl_seconds={"policy": 0}),               # zero ttl
        dict(ttl_seconds={"policy": True}),            # bool ttl
        dict(max_recall_k=0),                          # zero
        dict(latency_ring_size=-1),                    # negative
        dict(write_retry_backoff_seconds=0),           # zero
        dict(schema_version=0),                        # zero
    ):
        try:
            SupermemoryConfig(**bad)  # type: ignore[arg-type]
        except SupermemoryConfigError as exc:
            print(f"Rejected   : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"Expected rejection for {bad!r}")

    # Dict round-trip (unredacted + redacted).
    payload = cfg.to_dict()
    restored = SupermemoryConfig.from_dict(payload)
    assert restored == cfg
    print("Round-trip : OK")

    payload_r = cfg.to_dict(redact_secrets=True, redacted_value="<hidden>")
    # No api_key on the default config, so redaction is a no-op.
    print("redacted   :", payload_r["api_key"])

    # Redaction with a real key.
    cfg_key = SupermemoryConfig(mode="cloud", api_key="secret-123")
    payload_key = cfg_key.to_dict(redact_secrets=True)
    assert payload_key["api_key"] == "***"
    print("redaction  : OK")

    # to_json with redacted_value (item #11).
    j_redacted = cfg_key.to_json(
        redact_secrets=True, redacted_value="<redacted>", indent=2,
    )
    assert "<redacted>" in j_redacted
    print("to_json red: OK")

    # JSON round-trip.
    j = cfg.to_json()
    assert SupermemoryConfig.from_json(j) == cfg
    print("JSON RT    : OK")

    # Strict mode.
    try:
        SupermemoryConfig.from_dict({"nope": 1}, strict=True)
    except SupermemoryConfigError as exc:
        print("strict     : OK ->", exc)

    # with_overrides / merge.
    cfg3 = cfg.with_overrides(recall_top_k=8)
    assert cfg3.recall_top_k == 8 and cfg.recall_top_k == 5
    print("overrides  : OK")

    cfg4 = cfg.merge(SupermemoryConfig(recall_top_k=9, max_history=50))
    assert cfg4.recall_top_k == 9 and cfg4.max_history == 50
    print("merge      : OK")

    # from_env — pass an explicit mapping instead of mutating os.environ.
    env = {
        "GREEN_AGENT_SUPERMEMORY_RECALL_TOP_K": "8",
        "GREEN_AGENT_SUPERMEMORY_MODE": "local",
        "GREEN_AGENT_SUPERMEMORY_TRUTH_LEVELS": '["measured","estimated"]',
        "GREEN_AGENT_SUPERMEMORY_TTL_SECONDS": '{"policy": 3600}',
        "GREEN_AGENT_SUPERMEMORY_TTL_RUN": "120",
        "GREEN_AGENT_SUPERMEMORY_MAX_RECALL_K": "500",
        "GREEN_AGENT_SUPERMEMORY_WRITE_RETRY_BACKOFF_SECONDS": "0.5",
        "GREEN_AGENT_SUPERMEMORY_LATENCY_RING_SIZE": "500",
    }
    cfg5 = SupermemoryConfig.from_env(env=env)
    assert cfg5.recall_top_k == 8
    assert cfg5.truth_levels == ("measured", "estimated")
    assert cfg5.ttl_seconds["policy"] == 3600
    assert cfg5.ttl_seconds["run"] == 120
    assert cfg5.ttl_seconds["grid_forecast"] == _DEFAULT_TTLS["grid_forecast"]
    assert cfg5.max_recall_k == 500
    assert cfg5.write_retry_backoff_seconds == 0.5
    assert cfg5.latency_ring_size == 500
    print("from_env   : OK")

    # Integer-valued floats in TTL_SECONDS JSON (item #5).
    env_float = {
        "GREEN_AGENT_SUPERMEMORY_TTL_SECONDS": '{"policy": 3600.0}',
    }
    cfg_float = SupermemoryConfig.from_env(env=env_float)
    assert cfg_float.ttl_seconds["policy"] == 3600
    print("float TTL  : OK")

    # bool / NaN / inf in TTL_SECONDS JSON rejected.
    for bad_json in (
        '{"policy": true}',
        '{"policy": null}',
        '{"policy": 3600.5}',
    ):
        try:
            SupermemoryConfig.from_env(env={
                "GREEN_AGENT_SUPERMEMORY_TTL_SECONDS": bad_json,
            })
        except SupermemoryConfigError as exc:
            print(f"reject TTL : {bad_json} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad_json!r}")

    # NaN / inf in float env vars (item #12).
    for bad_val in ("nan", "inf", "-inf"):
        try:
            SupermemoryConfig.from_env(env={
                "GREEN_AGENT_SUPERMEMORY_RECALL_TIMEOUT_SECONDS": bad_val,
            })
        except SupermemoryConfigError as exc:
            print(f"reject {bad_val}: OK -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad_val!r}")

    # Env parse failure surfaces as SupermemoryConfigError.
    try:
        SupermemoryConfig.from_env(
            env={"GREEN_AGENT_SUPERMEMORY_RECALL_TOP_K": "abc"},
        )
    except SupermemoryConfigError as exc:
        print("env error  : OK ->", exc)

    # with_env layered builder.
    cfg6 = SupermemoryConfig.with_env(
        env=env, recall_top_k=99, schema_version=SCHEMA_VERSION,
    )
    assert cfg6.recall_top_k == 99
    assert cfg6.max_recall_k == 500  # from env, not overridden
    print("with_env   : OK")

    # to_env exporter.
    exported = cfg5.to_env()
    assert exported["GREEN_AGENT_SUPERMEMORY_RECALL_TOP_K"] == "8"
    assert exported["GREEN_AGENT_SUPERMEMORY_MAX_RECALL_K"] == "500"
    assert "GREEN_AGENT_SUPERMEMORY_API_KEY" not in exported
    exported_key = cfg_key.to_env(include_api_key=True)
    assert exported_key["GREEN_AGENT_SUPERMEMORY_API_KEY"] == "secret-123"
    print("to_env     : OK")

    # assert_compatible (item #9).
    SupermemoryConfig.assert_compatible({"schema_version": SCHEMA_VERSION})
    try:
        SupermemoryConfig.assert_compatible(
            {"schema_version": SCHEMA_VERSION + 1},
        )
    except SupermemoryConfigError:
        print("assert_compat : OK")

    print("\nSmoke test passed.")
