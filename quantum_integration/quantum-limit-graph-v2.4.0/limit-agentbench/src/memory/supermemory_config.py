# src/memory/supermemory_config.py

"""
Supermemory Configuration
=========================

Frozen configuration for the Supermemory adapter, recall layer, and write
governor.

Enhancements
------------
- ``SupermemoryConfig`` — frozen, validated: mode, endpoints, budgets, TTLs.
- ``from_env()`` reads ``GREEN_AGENT_SUPERMEMORY_*`` environment variables.
- ``to_dict`` / ``from_dict`` round-trip.
- Custom ``SupermemoryConfigError(ValueError)``.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Literal, Mapping, Optional

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SupermemoryConfigError(ValueError):
    """Raised for invalid Supermemory configuration."""


# --------------------------------------------------------------------------- #
# Default TTLs (seconds) per record kind.
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


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SupermemoryConfig:
    """Frozen configuration for Supermemory integration.

    Parameters
    ----------
    api_key : str, optional
        API key for cloud mode. Ignored in local mode.
    base_url : str
        Base URL of the Supermemory service. Default points at the local
        deployment.
    mode : {"local", "cloud"}
        Deployment mode. ``"local"`` disables API-key requirements.
    default_container_tag : str
        Default namespace for memories written without an explicit tag.
    max_history : int
        Bounded in-process history size for adapter statistics.
    recall_top_k : int
        Default number of historical runs to retrieve.
    recall_token_budget : int
        Soft token budget for a single recall bundle.
    recall_timeout_seconds : float
        Hard timeout for one recall request.
    write_retries : int
        Number of retries for failed memory writes.
    write_timeout_seconds : float
        Hard timeout for one write request.
    truth_levels : tuple of str
        Allowed ``truth_level`` values on records.
    ttl_seconds : Mapping[str, int]
        TTL per record kind, in seconds.
    """

    api_key: Optional[str] = None
    base_url: str = "http://localhost:6767"
    mode: Literal["local", "cloud"] = "local"
    default_container_tag: str = "org:green-agent"

    max_history: int = 10_000

    recall_top_k: int = 5
    recall_token_budget: int = 2_048
    recall_timeout_seconds: float = 5.0

    write_retries: int = 2
    write_timeout_seconds: float = 5.0

    truth_levels: tuple = (
        "measured", "estimated", "simulated", "user-reported",
    )

    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: dict(_DEFAULT_TTLS),
    )

    def __post_init__(self) -> None:
        if self.mode not in ("local", "cloud"):
            raise SupermemoryConfigError(
                f"mode must be 'local' or 'cloud', got {self.mode!r}."
            )
        if self.mode == "cloud" and not self.api_key:
            raise SupermemoryConfigError(
                "api_key is required when mode='cloud'."
            )
        if not isinstance(self.base_url, str) or not self.base_url:
            raise SupermemoryConfigError("base_url must be a non-empty string.")
        if not isinstance(self.default_container_tag, str) or not self.default_container_tag:
            raise SupermemoryConfigError(
                "default_container_tag must be a non-empty string."
            )
        for name in (
            "max_history", "recall_top_k", "recall_token_budget",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise SupermemoryConfigError(f"{name} must be a positive int.")
        for name in (
            "recall_timeout_seconds", "write_timeout_seconds",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or math.isnan(value) or value <= 0:
                raise SupermemoryConfigError(f"{name} must be > 0.")
        if not isinstance(self.write_retries, int) or self.write_retries < 0:
            raise SupermemoryConfigError(
                "write_retries must be a non-negative int."
            )
        if not isinstance(self.truth_levels, tuple) or not self.truth_levels:
            raise SupermemoryConfigError(
                "truth_levels must be a non-empty tuple."
            )
        for level in self.truth_levels:
            if not isinstance(level, str) or not level:
                raise SupermemoryConfigError(
                    "truth_levels entries must be non-empty strings."
                )
        if not isinstance(self.ttl_seconds, Mapping):
            raise SupermemoryConfigError("ttl_seconds must be a Mapping.")
        for kind, seconds in self.ttl_seconds.items():
            if not isinstance(seconds, int) or seconds <= 0:
                raise SupermemoryConfigError(
                    f"ttl_seconds[{kind!r}] must be a positive int."
                )

    # ----- helpers ---------------------------------------------------
    def is_local(self) -> bool:
        return self.mode == "local"

    def ttl_for(self, kind: str) -> int:
        """Return the TTL (seconds) for a record kind, or a fallback."""
        return int(self.ttl_seconds.get(kind, 90 * 24 * 3600))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["ttl_seconds"] = dict(self.ttl_seconds)
        d["truth_levels"] = list(self.truth_levels)
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SupermemoryConfig":
        if not isinstance(data, Mapping):
            raise SupermemoryConfigError(
                "SupermemoryConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "truth_levels":
                kwargs[k] = tuple(v)
            elif k == "ttl_seconds":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)

    @classmethod
    def from_env(cls) -> "SupermemoryConfig":
        """Build a config from ``GREEN_AGENT_SUPERMEMORY_*`` env vars."""
        e = os.environ
        prefix = "GREEN_AGENT_SUPERMEMORY_"
        raw: Dict[str, Any] = {}
        if (v := e.get(prefix + "API_KEY")) is not None:
            raw["api_key"] = v
        if (v := e.get(prefix + "BASE_URL")) is not None:
            raw["base_url"] = v
        if (v := e.get(prefix + "MODE")) is not None:
            raw["mode"] = v.lower()
        if (v := e.get(prefix + "DEFAULT_CONTAINER_TAG")) is not None:
            raw["default_container_tag"] = v
        for numeric in (
            "max_history", "recall_top_k", "recall_token_budget",
            "write_retries",
        ):
            if (v := e.get(prefix + numeric.upper())) is not None:
                raw[numeric] = int(v)
        for flt in ("recall_timeout_seconds", "write_timeout_seconds"):
            if (v := e.get(prefix + flt.upper())) is not None:
                raw[flt] = float(v)
        return cls(**raw)

    def __repr__(self) -> str:
        return (
            "SupermemoryConfig("
            f"mode={self.mode!r}, "
            f"base_url={self.base_url!r}, "
            f"container_tag={self.default_container_tag!r}, "
            f"recall_top_k={self.recall_top_k}, "
            f"history={self.max_history})"
        )


__all__ = [
    "SupermemoryConfig",
    "SupermemoryConfigError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.supermemory_config
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    cfg = SupermemoryConfig()
    print("repr       :", cfg)
    print("ttl(dec)   :", cfg.ttl_for("decision_outcome"))
    print("ttl(grid)  :", cfg.ttl_for("grid_forecast"))
    print("is_local   :", cfg.is_local())

    # Validation failures.
    for bad in (
        dict(mode="cloud"),                         # missing api_key
        dict(mode="bogus"),
        dict(max_history=0),
        dict(recall_top_k=-1),
        dict(recall_timeout_seconds=0),
    ):
        try:
            SupermemoryConfig(**bad)  # type: ignore[arg-type]
        except SupermemoryConfigError as exc:
            print("Rejected   :", exc)

    # Round-trip.
    payload = cfg.to_dict()
    restored = SupermemoryConfig.from_dict(payload)
    assert restored == cfg
    print("Round-trip : OK")

    # Env vars.
    os.environ["GREEN_AGENT_SUPERMEMORY_RECALL_TOP_K"] = "8"
    os.environ["GREEN_AGENT_SUPERMEMORY_MODE"] = "local"
    cfg2 = SupermemoryConfig.from_env()
    assert cfg2.recall_top_k == 8
    print("from_env   : OK")

    print("\nSmoke test passed.")
