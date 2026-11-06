"""
runtime/settings.py — Typed, validated, secret-aware configuration.

Rules:
    * Secrets are loaded from files under $GREEN_AGENT_SECRETS_DIR, not env.
    * Env vars override YAML overrides defaults.
    * Contradictions raise SettingsError at startup — fail loud, not silent.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "SettingsError", "load_settings"]


class SettingsError(RuntimeError):
    """Raised when configuration is missing, invalid, or contradictory."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GREEN_AGENT_",
        env_file=None,
        extra="forbid",
    )

    # --- Secrets (directory only; values loaded on demand) -----------------
    secrets_dir: Path = Field(default_factory=lambda: Path("/run/secrets"))

    # --- Behavior ----------------------------------------------------------
    confidence_threshold: float = Field(default=0.7, ge=0.0, le=1.0)
    sustainability_threshold_wh: float = Field(default=0.1, gt=0.0)
    audit_log_file: Path = Field(
        default_factory=lambda: Path("/var/log/green-agent/expert_audit.jsonl")
    )

    # --- Budgets -----------------------------------------------------------
    expert_daily_usd_budget: float = Field(default=25.0, gt=0.0)
    expert_daily_call_budget: int = Field(default=5000, gt=0)

    # --- Providers ---------------------------------------------------------
    model_allowlist: list[str] = Field(default_factory=lambda: [
        "gpt-4-turbo",
        "gpt-4o-mini",
        "claude-3-opus-20240229",
        "claude-3-5-sonnet-20241022",
    ])
    provider_timeout_s: float = Field(default=30.0, gt=0.0)
    provider_max_retries: int = Field(default=3, ge=0, le=10)
    cb_failure_threshold: int = Field(default=5, ge=1)
    cb_recovery_s: float = Field(default=60.0, gt=0.0)

    # --- Enhancements ------------------------------------------------------
    enhancements_enabled: bool = True
    flexgen_enabled: bool = False
    modp_weights: list[float] = Field(default_factory=lambda: [0.4, 0.3, 0.2, 0.1])

    @field_validator("model_allowlist", mode="before")
    @classmethod
    def _parse_allowlist(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError as exc:
                raise ValueError(f"model_allowlist must be JSON array: {exc}") from exc
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            raise ValueError("model_allowlist must be a list[str]")
        return v

    @field_validator("modp_weights")
    @classmethod
    def _weights_sum_to_one(cls, v: list[float]) -> list[float]:
        if len(v) != 4:
            raise ValueError("modp_weights must have exactly 4 entries")
        if abs(sum(v) - 1.0) > 1e-6:
            raise ValueError(f"modp_weights must sum to 1.0 (got {sum(v):.6f})")
        return v

    def read_secret(self, name: str) -> str:
        """Load a secret from $secrets_dir/<name>. Never falls back to env."""
        path = self.secrets_dir / name
        if not path.exists():
            raise SettingsError(f"missing secret file: {path}")
        text = path.read_text().strip()
        if not text:
            raise SettingsError(f"empty secret file: {path}")
        return text


def load_settings(overrides: dict[str, Any] | None = None) -> Settings:
    """Load and validate. Raises SettingsError on invalid config."""
    try:
        return Settings(**(overrides or {}))
    except ValidationError as exc:
        raise SettingsError(f"invalid configuration: {exc}") from exc
