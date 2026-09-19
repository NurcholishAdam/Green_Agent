# src/memory/supermemory_config.py

"""
Supermemory Configuration
=========================

Frozen configuration for the Supermemory adapter, recall layer, and write
governor.

Enhancements
------------
- ``SupermemoryConfig`` — frozen, validated: mode, endpoints, budgets, TTLs.
- Immutable ``ttl_seconds`` mapping (``MappingProxyType``), config hashable.
- ``SupermemoryMode`` enum (str-compatible) with normalization in post-init.
- ``from_env()`` reads ``GREEN_AGENT_SUPERMEMORY_*`` env vars, including
  JSON-encoded ``TRUTH_LEVELS`` / ``TTL_SECONDS`` and per-kind
  ``TTL_<KIND>`` overrides; JSON parsing errors are wrapped.
- ``to_dict()`` / ``from_dict()`` round-trip, with ``strict`` mode and
  ``redact_secrets`` support.
- ``to_json()`` / ``from_json()`` helpers.
- ``with_overrides()`` / ``merge()`` convenience constructors.
- URL and container-tag validation.
- ``ttl_for()`` with explicit ``default`` and warning on fallback.
- Rejects ``bool`` for numeric fields; rejects ``NaN``/``inf`` timeouts.
- Custom ``SupermemoryConfigError(ValueError)``.
- ``__main__`` smoke test.
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

__version__ = "6.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SupermemoryConfigError(ValueError):
    """Raised for invalid Supermemory configuration."""


# --------------------------------------------------------------------------- #
# Mode enum
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
_DEFAULT_TTLS: Mapping[str, int] = {
    "decision_outcome": 90 * 24 * 3600,   # 90 days
    "policy":           365 * 24 * 3600,  # 1 year
    "incident":         365 * 24 * 3600,  # 1 year
    "run":              180 * 24 * 3600,  # 180 days
    "grid_forecast":    6 * 3600,         # 6 hours
    "thermal_state":    30 * 60,          # 30 minutes
    "connectivity":     5 * 60,           # 5 minutes
}

_DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)

_CONTAINER_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:_\-./]{0,127}$")

_ENV_PREFIX = "GREEN_AGENT_SUPERMEMORY_"


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
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise SupermemoryConfigError(
            f"Environment variable {name}={raw!r} is not a valid float."
        ) from exc


def _coerce_json_env(name: str, raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SupermemoryConfigError(
            f"Environment variable {name}={raw!r} is not valid JSON."
        ) from exc


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
    default_container_tag: str = "org:green-agent"

    max_history: int = 10_000

    recall_top_k: int = 5
    recall_token_budget: int = 2_048
    recall_timeout_seconds: float = 5.0

    write_retries: int = 2
    write_timeout_seconds: float = 5.0

    truth_levels: Tuple[str, ...] = _DEFAULT_TRUTH_LEVELS

    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType(dict(_DEFAULT_TTLS)),
    )

    # ------------------------------------------------------------------ #
    # Post-init: normalize + validate
    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # --- mode normalization (accept str or enum) ---
        if isinstance(self.mode, SupermemoryMode):
            mode = self.mode
        elif isinstance(self.mode, str):
            try:
                mode = SupermemoryMode(self.mode.lower())
            except ValueError as exc:
                raise SupermemoryConfigError(
                    f"mode must be 'local' or 'cloud', got {self.mode!r}."
                ) from exc
        else:
            raise SupermemoryConfigError(
                "mode must be a str or SupermemoryMode, got "
                f"{type(self.mode).__name__}."
            )
        object.__setattr__(self, "mode", mode)

        # --- api_key normalization ---
        if self.api_key is not None:
            if not isinstance(self.api_key, str):
                raise SupermemoryConfigError(
                    "api_key must be a string or None."
                )
            object.__setattr__(self, "api_key", self.api_key or None)

        if mode is SupermemoryMode.CLOUD and not self.api_key:
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

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(
        self,
        *,
        redact_secrets: bool = False,
        redacted_value: str = "***",
    ) -> Dict[str, Any]:
        """Return a JSON-friendly dict of this config.

        Parameters
        ----------
        redact_secrets : bool
            Replace ``api_key`` with ``redacted_value`` when True.
        redacted_value : str
            Placeholder used when ``redact_secrets`` is True.
        """
        return {
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
        }

    def safe_dict(self) -> Dict[str, Any]:
        """``to_dict()`` with secrets redacted."""
        return self.to_dict(redact_secrets=True)

    def to_json(
        self,
        *,
        redact_secrets: bool = False,
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(redact_secrets=redact_secrets),
            indent=indent,
            sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "SupermemoryConfig":
        """Build a config from a mapping.

        Parameters
        ----------
        data : Mapping
            Input mapping.
        strict : bool
            If True, unknown keys raise ``SupermemoryConfigError``. If
            False (default), unknown keys are ignored for forward
            compatibility.
        """
        if not isinstance(data, ABCMapping):
            raise SupermemoryConfigError(
                "SupermemoryConfig.from_dict expects a Mapping."
            )

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
        """Build a config from ``GREEN_AGENT_SUPERMEMORY_*`` env vars.

        Recognized keys (all optional)::

            API_KEY
            BASE_URL
            MODE
            DEFAULT_CONTAINER_TAG
            MAX_HISTORY
            RECALL_TOP_K
            RECALL_TOKEN_BUDGET
            RECALL_TIMEOUT_SECONDS
            WRITE_RETRIES
            WRITE_TIMEOUT_SECONDS
            TRUTH_LEVELS          # JSON array, e.g. ["measured","estimated"]
            TTL_SECONDS           # JSON object, e.g. {"policy": 31536000}
            TTL_<KIND>            # int seconds, e.g. TTL_RUN=120

        ``TTL_SECONDS`` and ``TTL_<KIND>`` both merge on top of the
        built-in default TTLs; per-kind values win over the JSON object.
        """
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
        ):
            env_name = p + name.upper()
            if (v := _raw(env_name)) is not None:
                raw[name] = _coerce_int_env(env_name, v)

        for name in ("recall_timeout_seconds", "write_timeout_seconds"):
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
                ttl_overrides[kind] = _coerce_int_env(
                    f"{p}TTL_SECONDS[{kind!r}]", str(seconds),
                )

        per_kind_prefix = p + "TTL_"
        reserved = {"TTL_SECONDS"}
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
        ))


__all__ = [
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
    print("mode       :", cfg.mode, "| is_local:", cfg.is_local())
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

    # Hashability.
    hash(cfg)
    print("hashable   : OK")

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
        dict(default_container_tag="bad tag"),         # bad tag
        dict(truth_levels="measured"),                 # str not sequence
        dict(truth_levels=()),                         # empty
        dict(truth_levels=("measured", "measured")),   # duplicate
        dict(ttl_seconds={"policy": 0}),               # zero ttl
        dict(ttl_seconds={"policy": True}),            # bool ttl
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

    payload_r = cfg.to_dict(redact_secrets=True)
    print("redacted   :", payload_r["api_key"])

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
    }
    cfg5 = SupermemoryConfig.from_env(env=env)
    assert cfg5.recall_top_k == 8
    assert cfg5.truth_levels == ("measured", "estimated")
    assert cfg5.ttl_seconds["policy"] == 3600
    assert cfg5.ttl_seconds["run"] == 120
    # Unchanged kinds should still carry their defaults.
    assert cfg5.ttl_seconds["grid_forecast"] == _DEFAULT_TTLS["grid_forecast"]
    print("from_env   : OK")

    # Env parse failure surfaces as SupermemoryConfigError.
    try:
        SupermemoryConfig.from_env(
            env={"GREEN_AGENT_SUPERMEMORY_RECALL_TOP_K": "abc"},
        )
    except SupermemoryConfigError as exc:
        print("env error  : OK ->", exc)

    print("\nSmoke test passed.")
