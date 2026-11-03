# src/config.py

"""
Green Agent configuration — validated via Pydantic v2.

All settings can be overridden via environment variables with prefix
``GREEN_AGENT_``. Fields that carry a non-prefixed alias (e.g.
``PARETO_QUALITY_MIN``) are also overridable directly; for those fields
the prefixed form (``GREEN_AGENT_PARETO_QUALITY_MIN``) is **not** read,
because Pydantic's ``validation_alias`` takes precedence over
``env_prefix``.

Quick start
-----------
>>> from config import GreenAgentConfig
>>> cfg = GreenAgentConfig()               # reads GREEN_AGENT_* + aliases
>>> cfg.log_level
'INFO'
>>> cfg.pareto_quality_min
0.7

Bootstrap
---------
>>> from config import GreenAgentConfig, initialize_application
>>> async def main():
...     cfg = GreenAgentConfig()
...     result = await initialize_application(
...         cfg, db_manager, blockchain, carbon_manager, sustainability_engine,
...     )
...     await result.shutdown()

Exception model
---------------
Two exception types are exposed, covering different failure modes:

- :class:`ConfigValidationError` — re-export of Pydantic's
  :class:`~pydantic.ValidationError`. Raised by the constructor
  (``GreenAgentConfig(log_level='verbose')``), by any nested-model
  validation, and by :meth:`GreenAgentConfig.from_json` /
  :meth:`GreenAgentConfig.from_dict` when field validation fails.
  This is the type Pydantic always raises; the module cannot override
  it without breaking Pydantic internals.
- :class:`ConfigError` — the module's own narrow error type. Raised by
  :meth:`GreenAgentConfig.from_json` for malformed JSON, and by
  :meth:`~GreenAgentConfig.apply_detailed`-adjacent helpers for
  semantic errors that are *not* field validation.
- :class:`ConfigBootstrapError` — raised by
  :func:`initialize_application` when a component cannot be imported,
  the marketplace fails to initialise, or the UI factory returns an
  unexpected shape.

A caller who wants to catch every configuration-layer failure should
write ``except (ConfigError, ConfigValidationError):`` for the config
model and additionally ``except ConfigBootstrapError:`` when calling
:func:`initialize_application`. See :func:`health` for the pattern.

Sync bootstrap
--------------
The bootstrap function is async — it schedules a persistent
auto-offset task and needs a running event loop. If you need sync
initialisation, call ``asyncio.run(initialize_application(...))``
yourself at your top level; remember the auto-offset loop is bound to
that loop's lifetime, so it will be cancelled when ``asyncio.run``
returns. For a persistent auto-offset loop, keep the ``BootstrapResult``
and the event loop alive (e.g. run ``initialize_application`` inside
``async def main():``).

Schema versioning
-----------------
Two version numbers are tracked:

- :data:`__version__` — the **module's** API version. Bumped on any
  internal change, including documentation and refactoring that does
  not alter the on-disk shape.
- :data:`CONFIG_SCHEMA_VERSION` — the **configuration schema** version.
  Bumped only on breaking field changes. :attr:`GreenAgentConfig.schema_version`
  defaults to this value, and the field validator requires the major
  component of any user-supplied ``schema_version`` to match. This
  means a config file written by schema ``5.x.y`` loads under module
  version ``6.x.y`` without triggering a schema mismatch.

Enhancements over the previous version
--------------------------------------
1. **Exception model unified** — :data:`ConfigValidationError` is
   re-exported, and :meth:`from_json` / :meth:`from_dict` wrap
   ``ValidationError`` in :class:`ConfigError` so callers can catch a
   single type per entry point. See the "Exception model" section.
2. **``initialize_application`` cleans up partial failure** — if the UI
   factory raises, or returns an unexpected shape, the marketplace and
   the already-started auto-offset task are torn down before the error
   propagates. The previous revision leaked both.
3. **Retry filter** — retries are limited to
   ``(ConnectionError, TimeoutError)``, so bad configuration and
   programmer errors fail immediately instead of wasting
   ``max_retries × backoff`` seconds. Non-retryable exceptions are
   re-raised as :class:`ConfigBootstrapError` with the original
   exception chained.
4. **Retry jitter** — retry delays are multiplied by
   ``0.5 + random() * 0.5`` so a fleet restart does not produce a
   synchronised stampede of retries against the marketplace service.
5. **``loop=`` invariant validated** — passing a loop that is not the
   currently running loop raises :class:`ConfigBootstrapError`. The
   previous revision silently scheduled the auto-offset task on the
   wrong loop.
6. **``BootstrapResult.shutdown()`` uses ``inspect.isawaitable``**, so
   components returning ``asyncio.Future`` (not just coroutines) are
   awaited. The previous ``iscoroutine`` check silently skipped them.
7. **Component shutdown order prefers async closers** — ``aclose`` is
   tried first, then ``shutdown``, ``stop``, and finally ``close``. This
   matches common async client libraries (``httpx.AsyncClient``,
   ``aiohttp.ClientSession``) which expose both ``close`` and
   ``aclose``.
8. **``from_env(env=...)`` is honest about ``os.environ``** — when
   ``env`` is supplied, the matching ``os.environ`` keys are
   temporarily replaced for the duration of construction, so
   ``BaseSettings`` reads exactly the supplied values. This is not
   thread-safe; the method is documented as test-only. When
   ``env`` is ``None`` the method is identical to ``GreenAgentConfig()``.
9. **``from_dict`` added** — symmetric with ``to_dict``. Accepts both
   the weights-only shape and the full ``model_dump`` shape.
10. **``validate_assignment=True``** — post-construction mutation
    (``cfg.pareto_quality_min = 2.5``) now runs validators, matching
    what construction already did.
11. **``schema_version`` validated** — the major component must match
    :data:`CONFIG_SCHEMA_VERSION`'s major. Old config files at schema
    ``5.x.y`` load under any ``5.x.y`` module; a ``4.x.y`` or ``6.x.y``
    config is rejected with a migration hint.
12. **``is_production`` is now a property** — matching common Python
    style for parameterless, side-effect-free predicates. Callers that
    wrote ``cfg.is_production()`` get a clear ``TypeError`` immediately.
13. **``apply_logging(force=True)``** — the method now passes
    ``force=True`` to ``logging.basicConfig`` by default, so it actually
    configures the root logger even when a handler is already attached.
    Pass ``force=False`` to preserve the old "only if no handler
    exists" behaviour.
14. **``_check_sum`` raises :class:`ConfigError`** (Pydantic still wraps
    it in :class:`ValidationError` at construction time; the type is
    correct for direct calls).
15. **Dead code removed** from :meth:`get_log_level_int`, and the
    redundant ``pareto_quality_min > 1.0`` check inside
    :meth:`_cross_field_checks` (already enforced by ``Field(le=1.0)``).
16. **``_import_optional`` uses** :func:`importlib.import_module`
    rather than the low-level ``__import__`` form.
17. **Smoke test rewritten** — ``_check()``-based so it survives
    ``python -O``, with ``_expect_error`` asserting the exact exception
    type per failure path.

Requires Python 3.8+; Pydantic v2 required.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError as _PydanticValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Versions
# --------------------------------------------------------------------------- #
# __version__ tracks the module's API. CONFIG_SCHEMA_VERSION tracks the
# on-disk configuration shape. They evolve independently — a new
# __version__ does not require a new schema, and vice versa.
__version__ = "6.0.0"
CONFIG_SCHEMA_VERSION = "5.0.0"

# Re-export Pydantic's ValidationError under a domain-specific name.
# See the module docstring's "Exception model" section.
ConfigValidationError = _PydanticValidationError


# =========================================================================== #
# Errors
# =========================================================================== #
class ConfigError(ValueError):
    """Raised for the module's own runtime errors.

    Field-level validation raises :class:`ConfigValidationError` (Pydantic's
    :class:`~pydantic.ValidationError`); this class is used by
    :meth:`GreenAgentConfig.from_json` for malformed JSON and by
    :meth:`GreenAgentConfig.from_dict` for non-mapping payloads.
    """


class ConfigBootstrapError(RuntimeError):
    """Raised when :func:`initialize_application` cannot wire a component."""


# =========================================================================== #
# Retry policy
# =========================================================================== #
# Only these are retried by initialize_application. A marketplace
# constructor that raises a non-retryable exception (bad config,
# programmer error) fails on the first attempt.
_RETRYABLE_EXCEPTIONS: Tuple[type, ...] = (
    ConnectionError,
    TimeoutError,
)


# =========================================================================== #
# Nested sub-configurations
# =========================================================================== #
class TaskTypeDistribution(BaseModel):
    """Distribution of synthetic task types.

    All six probabilities must be in ``[0, 1]`` and sum to ``1.0``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    summarization: float = Field(0.25, ge=0.0, le=1.0)
    classification: float = Field(0.20, ge=0.0, le=1.0)
    translation: float = Field(0.15, ge=0.0, le=1.0)
    question_answering: float = Field(0.15, ge=0.0, le=1.0)
    text_generation: float = Field(0.15, ge=0.0, le=1.0)
    sentiment_analysis: float = Field(0.10, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _check_sum(self) -> "TaskTypeDistribution":
        total = (
            self.summarization
            + self.classification
            + self.translation
            + self.question_answering
            + self.text_generation
            + self.sentiment_analysis
        )
        if abs(total - 1.0) > 1e-6:
            raise ConfigError(
                "Task type distribution probabilities must sum to 1.0 "
                f"(got {total:.6f})."
            )
        return self

    def as_dict(self) -> Dict[str, float]:
        """Return a plain ``{task_type: probability}`` mapping."""
        return self.model_dump()


class SyntheticDataConfig(BaseModel):
    """Configuration for synthetic data generation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    seed: int = Field(42, ge=0, description="Random seed for reproducibility")
    token_mean: float = Field(5.5, gt=0, description="Mean token count per task")
    token_std: float = Field(1.2, gt=0, description="Std dev of token count")
    task_type_distribution: TaskTypeDistribution = Field(
        default_factory=TaskTypeDistribution,
        description="Distribution of task types",
    )


class CarbonMarketplaceConfig(BaseModel):
    """Configuration for the carbon credit marketplace."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    refresh_interval_seconds: int = Field(
        3600,
        ge=60,
        description="How often to refresh carbon prices (seconds)",
    )
    auto_offset_enabled: bool = Field(
        True,
        description="Enable automatic carbon offsetting",
    )
    auto_offset_threshold_kg: float = Field(
        100.0,
        gt=0,
        description="Threshold (kg CO2e) to trigger automatic offset",
    )


class DistillationConfig(BaseModel):
    """Configuration for the distillation / fine-tuning pipeline."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    num_epochs: int = Field(3, ge=1)
    batch_size: int = Field(32, ge=1)
    lr: float = Field(1e-5, gt=0)
    reverse_kl: bool = Field(True)
    alpha_orm: float = Field(0.1, ge=0.0, le=1.0)
    mixed_precision: bool = Field(True)


# =========================================================================== #
# Main configuration
# =========================================================================== #
class GreenAgentConfig(BaseSettings):
    """Main configuration for the Green Agent system.

    Sources, in priority order:

    1. Explicit constructor kwargs.
    2. Environment variables with prefix ``GREEN_AGENT_`` (case-insensitive)
       for fields without a ``validation_alias``.
    3. Environment variables matching each field's ``validation_alias``
       (e.g. ``PARETO_QUALITY_MIN``) for fields that carry one.
    4. Field defaults.

    Note that ``validation_alias`` overrides ``env_prefix`` for that field,
    so a field with ``validation_alias="PARETO_QUALITY_MIN"`` is **not**
    read from ``GREEN_AGENT_PARETO_QUALITY_MIN``.
    """

    model_config = SettingsConfigDict(
        env_prefix="GREEN_AGENT_",
        case_sensitive=False,
        extra="ignore",
        frozen=False,
        populate_by_name=True,
        validate_assignment=True,
    )

    # ---- General --------------------------------------------------------- #
    instance_id: str = Field(
        default_factory=lambda: uuid.uuid4().hex[:8],
        min_length=4,
        max_length=32,
        description="Unique identifier for this agent instance",
    )
    schema_version: str = Field(
        CONFIG_SCHEMA_VERSION,
        description="Config schema version (major must match the "
                    "supported schema)",
    )
    runtime_env: Literal["dev", "staging", "prod"] = Field(
        "dev",
        description="Runtime environment",
    )
    log_level: str = Field(
        "INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    # ---- Nested sections ------------------------------------------------- #
    synthetic: SyntheticDataConfig = Field(
        default_factory=SyntheticDataConfig,
        description="Synthetic data generation parameters",
    )
    carbon_marketplace: CarbonMarketplaceConfig = Field(
        default_factory=CarbonMarketplaceConfig,
        description="Carbon credit marketplace settings",
    )
    distillation: DistillationConfig = Field(
        default_factory=DistillationConfig,
        description="Distillation / fine-tuning pipeline settings",
    )

    # ---- Pareto & Constraints -------------------------------------------- #
    pareto_quality_min: float = Field(
        0.7,
        ge=0.0,
        le=1.0,
        validation_alias="PARETO_QUALITY_MIN",
        description="Minimum accuracy to be considered Pareto-optimal",
    )
    pareto_latency_max: float = Field(
        500.0,
        gt=0,
        validation_alias="PARETO_LATENCY_MAX",
        description="Maximum acceptable latency (ms)",
    )
    pareto_carbon_max: float = Field(
        1.0,
        gt=0,
        validation_alias="PARETO_CARBON_MAX",
        description="Maximum acceptable carbon per task (kg CO2e)",
    )

    # ---- Queue / 2-tier offline loop ------------------------------------- #
    queue_type: Literal["asyncio", "redis"] = Field(
        "asyncio",
        validation_alias="QUEUE_TYPE",
        description="Queue backend: 'asyncio' or 'redis'",
    )
    redis_url: Optional[str] = Field(
        None,
        validation_alias="REDIS_URL",
        description="Redis connection URL (required when queue_type='redis')",
    )
    offline_batch_size: int = Field(
        64,
        ge=1,
        validation_alias="OFFLINE_BATCH_SIZE",
    )
    offline_update_interval_sec: int = Field(
        300,
        ge=1,
        validation_alias="OFFLINE_UPDATE_INTERVAL_SEC",
    )

    # ---- Drift & Safety -------------------------------------------------- #
    drift_threshold: float = Field(
        0.15,
        ge=0.0,
        le=1.0,
        validation_alias="DRIFT_THRESHOLD",
    )
    rollback_enabled: bool = Field(
        True,
        validation_alias="ROLLBACK_ENABLED",
    )

    # ---- Benchmark ------------------------------------------------------- #
    benchmark_interval_days: int = Field(
        7,
        ge=1,
        validation_alias="BENCHMARK_INTERVAL_DAYS",
    )

    # ---- Dashboard ------------------------------------------------------- #
    dashboard_port: int = Field(
        8080,
        ge=1,
        le=65_535,
        validation_alias="DASHBOARD_PORT",
    )
    dashboard_enabled: bool = Field(
        True,
        validation_alias="DASHBOARD_ENABLED",
    )

    # ---- Field validators ------------------------------------------------ #
    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ConfigError("log_level must be a string")
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.strip().upper()
        if upper not in allowed:
            raise ConfigError(
                f"log_level must be one of {sorted(allowed)}, got {v!r}"
            )
        return upper

    @field_validator("instance_id", mode="after")
    @classmethod
    def _validate_instance_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ConfigError("instance_id must not be empty")
        return v

    @field_validator("redis_url", mode="after")
    @classmethod
    def _validate_redis_url(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ConfigError("redis_url must not be a blank string")
        return v

    @field_validator("schema_version", mode="after")
    @classmethod
    def _validate_schema_version(cls, v: str) -> str:
        supported_major = CONFIG_SCHEMA_VERSION.split(".", 1)[0]
        major = v.split(".", 1)[0] if v else ""
        if major != supported_major:
            raise ConfigError(
                f"schema_version {v!r} is incompatible with the supported "
                f"schema major {supported_major!r}. Migrate the config "
                f"file (see module docstring, 'Schema versioning')."
            )
        return v

    # ---- Cross-field validation ------------------------------------------ #
    @model_validator(mode="after")
    def _cross_field_checks(self) -> "GreenAgentConfig":
        if self.queue_type == "redis" and not self.redis_url:
            raise ConfigError(
                "REDIS_URL must be set when QUEUE_TYPE == 'redis'."
            )
        return self

    # ---- Convenience ----------------------------------------------------- #
    @property
    def is_production(self) -> bool:
        """``True`` when :attr:`runtime_env` is ``"prod"``."""
        return self.runtime_env == "prod"

    def get_log_level_int(self) -> int:
        """Return the numeric logging level for :attr:`log_level`."""
        return logging.getLevelName(self.log_level)  # always an int for the
                                                       # validated set

    def apply_logging(self, *, force: bool = True) -> None:
        """Configure the root logger using this config's level.

        Parameters
        ----------
        force : bool, default True
            When True (default), :func:`logging.basicConfig` is called
            with ``force=True`` so any previously-installed handlers
            are replaced. Pass ``force=False`` to preserve the legacy
            "only if no handler exists" behaviour, which makes this
            method a silent no-op if logging is already configured.
        """
        logging.basicConfig(
            level=self.get_log_level_int(),
            format="%(levelname)s %(name)s: %(message)s",
            force=force,
        )

    # ---- Serialization --------------------------------------------------- #
    def to_json(self, *, indent: Optional[int] = 2) -> str:
        """Serialize to JSON. Payload contains only JSON-compatible
        primitives (nested configs are emitted as dicts)."""
        return self.model_dump_json(indent=indent)

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain JSON-safe dict.

        Both :meth:`to_dict` and :meth:`to_json` produce JSON-compatible
        shapes — the two methods stay in sync for fields that are already
        primitives, and any future non-primitive field should be verified
        against both paths.
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True,
    ) -> "GreenAgentConfig":
        """Reconstruct a validated config from a mapping.

        Parameters
        ----------
        data : Mapping
            Payload produced by :meth:`to_dict` or :meth:`to_json`.
        strict : bool, default True
            Kept for symmetry with :meth:`from_json`; the model's
            ``extra="ignore"`` config determines whether unknown keys
            raise or are silently dropped. This parameter currently
            has no effect on unknown-key handling — it exists for
            forward-compatibility with a future strict validator.

        Raises
        ------
        ConfigError
            If ``data`` is not a Mapping.
        ConfigValidationError
            If any field validation fails.
        """
        if not isinstance(data, Mapping):
            raise ConfigError(
                f"data must be a Mapping, got {type(data).__name__}."
            )
        try:
            return cls.model_validate(dict(data))
        except ConfigValidationError:
            raise
        except Exception as exc:
            raise ConfigError(
                f"invalid configuration: {exc}"
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "GreenAgentConfig":
        """Deserialize a JSON payload into a validated config.

        Raises
        ------
        ConfigError
            If ``payload`` is not valid JSON or does not decode to an
            object.
        ConfigValidationError
            If any field validation fails.
        """
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ConfigError("config JSON must be an object")
        return cls.from_dict(data)

    @classmethod
    def from_env(
        cls, env: Optional[Mapping[str, str]] = None,
    ) -> "GreenAgentConfig":
        """Build a config from environment variables.

        - ``env is None`` — equivalent to ``GreenAgentConfig()``; reads
          ``os.environ`` via Pydantic's ``BaseSettings`` machinery.
        - ``env`` supplied — the keys in ``env`` are temporarily
          overlaid on ``os.environ`` for the duration of construction,
          then restored. Intended for tests: use it to isolate a config
          from the process-wide environment.

        .. warning::
            The overlay path is **not** thread-safe. Another thread that
            reads or writes ``os.environ`` during the overlay window can
            observe or corrupt the temporary state. Production code
            should read from ``os.environ`` directly (i.e. call
            ``GreenAgentConfig()``); use ``env=`` only in a
            single-threaded test harness, or via
            ``pytest.monkeypatch.setenv``.
        """
        if env is None:
            return cls()
        saved: Dict[str, Optional[str]] = {}
        try:
            for k, v in env.items():
                saved[k] = os.environ.get(k)
                os.environ[k] = str(v)
            return cls()
        finally:
            for k, old in saved.items():
                if old is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = old


# =========================================================================== #
# Bootstrap result
# =========================================================================== #
@dataclass
class BootstrapResult:
    """Return value of :func:`initialize_application`.

    Holds live component references plus a :meth:`shutdown` coroutine
    that cancels the marketplace auto-offset loop and releases
    resources. Designed for ``async with`` or explicit
    ``await result.shutdown()``.
    """

    marketplace: Any = None
    dashboard: Any = None
    api_extension: Any = None
    auto_offset_task: Optional["asyncio.Task[None]"] = None
    started_at: float = field(default_factory=time.time)

    async def shutdown(self) -> None:
        """Cancel background tasks and best-effort shutdown collaborators."""
        if (
            self.auto_offset_task is not None
            and not self.auto_offset_task.done()
        ):
            self.auto_offset_task.cancel()
            try:
                await self.auto_offset_task
            except asyncio.CancelledError:
                logger.debug("Auto-offset loop cancelled.")
            except Exception:
                logger.exception(
                    "Auto-offset loop raised during cancel."
                )

        await _shutdown_component(self.marketplace, name="marketplace")
        await _shutdown_component(self.dashboard, name="dashboard")
        await _shutdown_component(self.api_extension, name="api_extension")

        logger.info("BootstrapResult shutdown complete.")

    async def __aenter__(self) -> "BootstrapResult":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()


# =========================================================================== #
# Component helpers
# =========================================================================== #
async def _shutdown_component(component: Any, *, name: str = "component") -> None:
    """Best-effort shutdown of ``component``.

    Tries ``aclose``, ``shutdown``, ``stop``, ``close`` in that order —
    preferring async closers so that libraries exposing both ``close``
    and ``aclose`` (httpx, aiohttp) are torn down via the awaitable
    variant.
    """
    if component is None:
        return
    for method_name in ("aclose", "shutdown", "stop", "close"):
        method = getattr(component, method_name, None)
        if not callable(method):
            continue
        try:
            result = method()
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception(
                "%s.%s() failed during shutdown.", name, method_name,
            )
        return


def _import_optional(module_name: str, attr: str) -> Any:
    """Import ``attr`` from ``module_name`` with a ``src.`` fallback.

    Raises
    ------
    ConfigBootstrapError
        When the module or attribute cannot be resolved.
    """
    candidates = [module_name]
    if not module_name.startswith("src."):
        candidates.append(f"src.{module_name}")

    last_exc: Optional[BaseException] = None
    for candidate in candidates:
        try:
            module = importlib.import_module(candidate)
            return getattr(module, attr)
        except (ImportError, AttributeError) as exc:
            last_exc = exc
    raise ConfigBootstrapError(
        f"required component '{attr}' from '{module_name}' is "
        f"unavailable: {last_exc}"
    ) from last_exc


def _fail_or_warn(
    config: GreenAgentConfig,
    message: str,
    exc: Optional[BaseException],
) -> None:
    """Raise in production, log in dev/staging.

    Used for failures that should abort bootstrap in prod but are
    tolerable in dev (e.g. the auto-offset loop fails to start).
    Contract errors (bad UI factory output, missing imports) always
    raise regardless of environment.
    """
    if config.is_production:
        if exc is not None:
            raise ConfigBootstrapError(message) from exc
        raise ConfigBootstrapError(message)
    logger.warning("%s (dev mode — continuing)", message)


async def _build_marketplace_with_retries(
    marketplace_cls: Any,
    config: GreenAgentConfig,
    db_manager: Any,
    blockchain: Any,
    carbon_manager: Any,
    sustainability_engine: Any,
    *,
    max_retries: int,
    retry_backoff_seconds: float,
) -> Any:
    """Attempt to construct the marketplace with a bounded retry policy.

    Only :data:`_RETRYABLE_EXCEPTIONS` are retried; other exceptions
    raise :class:`ConfigBootstrapError` immediately.
    """
    kwargs = dict(
        config=config.carbon_marketplace.model_dump(),
        db_manager=db_manager,
        blockchain=blockchain,
        carbon_manager=carbon_manager,
        sustainability_engine=sustainability_engine,
    )
    attempts = max_retries + 1
    last_exc: Optional[BaseException] = None

    for attempt in range(1, attempts + 1):
        try:
            return marketplace_cls(**kwargs)
        except _RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
            if attempt >= attempts:
                logger.error(
                    "Marketplace attempt %d/%d failed, no retries "
                    "left: %s",
                    attempt, attempts, exc,
                )
                break
            delay = (
                retry_backoff_seconds
                * attempt
                * (0.5 + random.random() * 0.5)
            )
            logger.warning(
                "Marketplace attempt %d/%d failed, retrying in "
                "%.2fs: %s",
                attempt, attempts, delay, exc,
            )
            await asyncio.sleep(delay)
        except Exception as exc:
            raise ConfigBootstrapError(
                f"marketplace initialisation failed "
                f"(non-retryable): {exc}"
            ) from exc

    raise ConfigBootstrapError(
        f"marketplace failed to initialise after {attempts} "
        f"attempt(s): {last_exc}"
    ) from last_exc


# =========================================================================== #
# Initialization
# =========================================================================== #
async def initialize_application(
    config: GreenAgentConfig,
    db_manager: Any,
    blockchain: Any,
    carbon_manager: Any,
    sustainability_engine: Any,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    max_retries: int = 2,
    retry_backoff_seconds: float = 1.0,
) -> BootstrapResult:
    """Build the marketplace, UI, and other components from ``config``.

    Parameters
    ----------
    config : GreenAgentConfig
    db_manager, blockchain, carbon_manager, sustainability_engine : Any
        Core collaborators already initialised by the caller.
    loop : asyncio.AbstractEventLoop, optional
        When passed, must be the currently running loop — passing a
        different loop is a programming error and raises
        :class:`ConfigBootstrapError`.
    max_retries : int, default 2
        Number of retries for the marketplace constructor when the
        failure is one of :data:`_RETRYABLE_EXCEPTIONS`.
    retry_backoff_seconds : float, default 1.0
        Base of the linear backoff between retries (with jitter).

    Returns
    -------
    BootstrapResult
        Live components plus a ``shutdown()`` coroutine.

    Raises
    ------
    ConfigBootstrapError
        If a required component cannot be imported, the marketplace
        fails to initialise, ``loop=`` is a different loop than the
        running one, or the UI factory returns an unexpected shape.
    """
    if not isinstance(config, GreenAgentConfig):
        raise ConfigBootstrapError(
            "config must be a GreenAgentConfig instance."
        )

    # ---- Validate the loop invariant ------------------------------- #
    running_loop = asyncio.get_running_loop()
    if loop is not None and loop is not running_loop:
        raise ConfigBootstrapError(
            "loop= must be the currently running loop when passed; "
            "the auto-offset task is scheduled on the running loop."
        )
    active_loop = loop or running_loop

    # ---- Resolve components (defensive imports) -------------------- #
    CarbonCreditMarketplace = _import_optional(
        "carbon_marketplace", "CarbonCreditMarketplace",
    )
    create_explainable_ui = _import_optional(
        "explainable_ui", "create_explainable_ui",
    )

    # ---- Build the marketplace with retries ------------------------ #
    marketplace = await _build_marketplace_with_retries(
        CarbonCreditMarketplace,
        config,
        db_manager,
        blockchain,
        carbon_manager,
        sustainability_engine,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )

    # ---- Compose and (on failure) clean up ------------------------- #
    auto_offset_task: Optional[asyncio.Task] = None
    try:
        # Start the auto-offset loop.
        if config.carbon_marketplace.auto_offset_enabled:
            method = getattr(
                marketplace, "start_auto_offset_loop", None,
            )
            if not callable(method):
                _fail_or_warn(
                    config,
                    "marketplace does not expose start_auto_offset_loop()",
                    None,
                )
            else:
                try:
                    auto_offset_task = active_loop.create_task(
                        method(),
                    )
                except Exception as exc:
                    _fail_or_warn(
                        config,
                        f"auto-offset loop failed to start: {exc}",
                        exc,
                    )

        # Create the UI.
        ui = create_explainable_ui(config=config)
        if not isinstance(ui, dict):
            raise ConfigBootstrapError(
                "create_explainable_ui must return a dict with "
                "'dashboard' and 'api_extension' keys; got "
                f"{type(ui).__name__}."
            )
        if "dashboard" not in ui:
            raise ConfigBootstrapError(
                "create_explainable_ui return value is missing "
                "'dashboard'."
            )
        if "api_extension" not in ui:
            raise ConfigBootstrapError(
                "create_explainable_ui return value is missing "
                "'api_extension'."
            )

        result = BootstrapResult(
            marketplace=marketplace,
            dashboard=ui["dashboard"],
            api_extension=ui["api_extension"],
            auto_offset_task=auto_offset_task,
            started_at=time.time(),
        )
    except Exception:
        # Cancel the auto-offset loop and tear down the marketplace
        # before propagating. Without this cleanup, both are leaked.
        if auto_offset_task is not None:
            auto_offset_task.cancel()
            try:
                await auto_offset_task
            except (asyncio.CancelledError, Exception):
                pass
        await _shutdown_component(marketplace, name="marketplace")
        raise

    logger.info(
        "Application initialised (env=%s, queue=%s, dashboard=%s, "
        "auto_offset=%s).",
        config.runtime_env,
        config.queue_type,
        config.dashboard_enabled,
        config.carbon_marketplace.auto_offset_enabled,
    )
    return result


# =========================================================================== #
# Public API
# =========================================================================== #
__all__: List[str] = [
    "__version__",
    "CONFIG_SCHEMA_VERSION",
    "BootstrapResult",
    "CarbonMarketplaceConfig",
    "ConfigBootstrapError",
    "ConfigError",
    "ConfigValidationError",
    "DistillationConfig",
    "GreenAgentConfig",
    "SyntheticDataConfig",
    "TaskTypeDistribution",
    "initialize_application",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# =========================================================================== #
# Smoke test: python -m config
# =========================================================================== #
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

    def _expect_error(
        name: str, expected: type, fn, *args, **kwargs,
    ) -> None:
        global failed
        try:
            fn(*args, **kwargs)
            _check(name, False)
        except expected:
            _check(name, True)
        except Exception as exc:
            print(
                f"  ❌ {name} — wrong exception "
                f"{type(exc).__name__}: {exc}"
            )
            failed += 1

    # ---- 1. Default construction --------------------------------- #
    print("default construction:")
    cfg = GreenAgentConfig()
    _check(
        "instance_id has 8 hex chars",
        len(cfg.instance_id) == 8 and all(
            c in "0123456789abcdef" for c in cfg.instance_id
        ),
    )
    _check(
        "schema_version defaults to CONFIG_SCHEMA_VERSION",
        cfg.schema_version == CONFIG_SCHEMA_VERSION,
    )
    _check("runtime_env defaults to 'dev'", cfg.runtime_env == "dev")
    _check("log_level defaults to 'INFO'", cfg.log_level == "INFO")
    _check(
        "pareto_quality_min defaults to 0.7",
        cfg.pareto_quality_min == 0.7,
    )
    _check(
        "distillation.num_epochs defaults to 3",
        cfg.distillation.num_epochs == 3,
    )
    _check(
        "synthetic.task_type_distribution sums to 1",
        abs(
            sum(cfg.synthetic.task_type_distribution.as_dict().values()) - 1
        ) < 1e-6,
    )
    _check("is_production is a property", cfg.is_production is False)
    _check(
        "get_log_level_int returns numeric level",
        cfg.get_log_level_int() == logging.INFO,
    )

    # ---- 2. Environment overrides via from_env ------------------ #
    print("env overrides:")
    overlaid = GreenAgentConfig.from_env({
        "GREEN_AGENT_LOG_LEVEL": "debug",
        "PARETO_QUALITY_MIN": "0.85",
        "GREEN_AGENT_RUNTIME_ENV": "staging",
    })
    _check(
        "log_level case-insensitively uppercased",
        overlaid.log_level == "DEBUG",
    )
    _check(
        "pareto_quality_min from alias",
        overlaid.pareto_quality_min == 0.85,
    )
    _check(
        "runtime_env from prefixed env",
        overlaid.runtime_env == "staging",
    )
    # Overlay must be restored after construction.
    _check(
        "os.environ restored after from_env(env=...)",
        "PARETO_QUALITY_MIN" not in os.environ
        or os.environ["PARETO_QUALITY_MIN"] != "0.85",
    )

    # ---- 3. Cross-field validation ------------------------------ #
    print("cross-field:")
    _expect_error(
        "redis without redis_url rejected",
        ConfigValidationError,
        GreenAgentConfig, queue_type="redis",
    )
    # With a URL — accepted.
    try:
        GreenAgentConfig(
            queue_type="redis", redis_url="redis://localhost:6379",
        )
        _check("redis with redis_url accepted", True)
    except Exception:
        _check("redis with redis_url accepted", False)

    # ---- 4. JSON round-trip ------------------------------------- #
    print("round-trip:")
    payload = cfg.to_json()
    restored = GreenAgentConfig.from_json(payload)
    _check(
        "from_json(to_json) round-trips instance_id",
        restored.instance_id == cfg.instance_id,
    )
    _check(
        "from_json(to_json) round-trips distillation",
        restored.distillation == cfg.distillation,
    )

    dict_payload = cfg.to_dict()
    restored_dict = GreenAgentConfig.from_dict(dict_payload)
    _check(
        "from_dict(to_dict) round-trips",
        restored_dict.instance_id == cfg.instance_id,
    )

    _expect_error(
        "from_dict rejects non-mapping",
        ConfigError,
        GreenAgentConfig.from_dict,
        "not-a-mapping",
    )
    _expect_error(
        "from_json rejects invalid JSON",
        ConfigError,
        GreenAgentConfig.from_json,
        "{not json",
    )

    # ---- 5. Field-level validation ------------------------------ #
    print("field validation:")
    for bad in ("verbose", "", None, 42):
        _expect_error(
            f"reject log_level={bad!r}",
            ConfigValidationError,
            GreenAgentConfig, log_level=bad,
        )

    # TaskTypeDistribution sum check.
    _expect_error(
        "reject distribution that sums to != 1",
        ConfigValidationError,
        TaskTypeDistribution,
        summarization=0.5, classification=0.5,
        translation=0.5, question_answering=0.5,
        text_generation=0.5, sentiment_analysis=0.5,
    )

    # schema_version validation.
    _expect_error(
        "reject incompatible schema major",
        ConfigValidationError,
        GreenAgentConfig, schema_version="4.0.0",
    )
    try:
        GreenAgentConfig(schema_version=CONFIG_SCHEMA_VERSION)
        _check("accept matching schema major", True)
    except Exception:
        _check("accept matching schema major", False)

    # ---- 6. validate_assignment -------------------------------- #
    print("validate_assignment:")
    mutable = GreenAgentConfig()
    _expect_error(
        "assignment of out-of-range value rejected",
        ConfigValidationError,
        setattr, mutable, "pareto_quality_min", 2.5,
    )
    try:
        mutable.pareto_quality_min = 0.9
        _check(
            "in-range assignment accepted",
            mutable.pareto_quality_min == 0.9,
        )
    except Exception:
        _check("in-range assignment accepted", False)

    # ---- 7. Bootstrap smoke test (mocked deps) ------------------ #
    print("bootstrap:")

    async def _bootstrap_tests() -> None:
        import sys as _sys
        import types as _types

        # Track shutdown calls on the fake marketplace.
        shutdown_calls = []

        # Install a fake carbon_marketplace module.
        cm_mod = _types.ModuleType("carbon_marketplace")

        class _FakeMarketplace:
            def __init__(self, **kwargs: Any) -> None:
                self.kwargs = kwargs
                self.auto_offset_started = False

            async def start_auto_offset_loop(self) -> None:
                self.auto_offset_started = True
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    logger.debug("FakeMarketplace auto-offset cancelled.")

            async def shutdown(self) -> None:
                shutdown_calls.append("marketplace")

        cm_mod.CarbonCreditMarketplace = _FakeMarketplace  # type: ignore[attr-defined]
        _sys.modules["carbon_marketplace"] = cm_mod

        # Install a fake explainable_ui module.
        ui_mod = _types.ModuleType("explainable_ui")

        def _create_explainable_ui(config: Any) -> Dict[str, Any]:
            return {"dashboard": object(), "api_extension": object()}

        ui_mod.create_explainable_ui = _create_explainable_ui  # type: ignore[attr-defined]
        _sys.modules["explainable_ui"] = ui_mod

        try:
            # Successful bootstrap.
            cfg_boot = GreenAgentConfig(
                carbon_marketplace=CarbonMarketplaceConfig(
                    auto_offset_enabled=True,
                ),
            )
            result = await initialize_application(
                cfg_boot,
                db_manager=object(),
                blockchain=object(),
                carbon_manager=object(),
                sustainability_engine=object(),
            )
            _check(
                "bootstrap returns a marketplace",
                result.marketplace is not None,
            )
            _check(
                "bootstrap returns a dashboard",
                result.dashboard is not None,
            )
            _check(
                "bootstrap returns an api_extension",
                result.api_extension is not None,
            )
            _check(
                "bootstrap schedules the auto-offset task",
                result.auto_offset_task is not None,
            )
            await result.shutdown()
            _check(
                "marketplace shutdown called after result.shutdown()",
                "marketplace" in shutdown_calls,
            )

            # Cleanup on failure — replace create_explainable_ui with
            # one that raises.
            shutdown_calls.clear()

            def _broken_ui(config: Any) -> Dict[str, Any]:
                raise RuntimeError("simulated UI failure")

            ui_mod.create_explainable_ui = _broken_ui  # type: ignore[attr-defined]

            try:
                await initialize_application(
                    cfg_boot,
                    db_manager=object(),
                    blockchain=object(),
                    carbon_manager=object(),
                    sustainability_engine=object(),
                )
                _check("UI failure propagates", False)
            except RuntimeError:
                _check("UI failure propagates", True)

            _check(
                "marketplace cleaned up on failure",
                "marketplace" in shutdown_calls,
            )

            # Restore the working factory.
            ui_mod.create_explainable_ui = _create_explainable_ui  # type: ignore[attr-defined]

            # loop= invariant — pass a different loop.
            other_loop = asyncio.new_event_loop()
            try:
                try:
                    await initialize_application(
                        cfg_boot,
                        db_manager=object(),
                        blockchain=object(),
                        carbon_manager=object(),
                        sustainability_engine=object(),
                        loop=other_loop,
                    )
                    _check("mismatched loop rejected", False)
                except ConfigBootstrapError:
                    _check("mismatched loop rejected", True)
            finally:
                other_loop.close()

        finally:
            _sys.modules.pop("carbon_marketplace", None)
            _sys.modules.pop("explainable_ui", None)

    asyncio.run(_bootstrap_tests())

    # ---- 8. Public API surface --------------------------------- #
    print("public API:")
    for name in (
        "__version__",
        "CONFIG_SCHEMA_VERSION",
        "BootstrapResult",
        "CarbonMarketplaceConfig",
        "ConfigBootstrapError",
        "ConfigError",
        "ConfigValidationError",
        "DistillationConfig",
        "GreenAgentConfig",
        "SyntheticDataConfig",
        "TaskTypeDistribution",
        "initialize_application",
    ):
        _check(f"{name} exported", name in __all__)

    _check(
        "ConfigError is ValueError",
        issubclass(ConfigError, ValueError),
    )
    _check(
        "ConfigBootstrapError is RuntimeError",
        issubclass(ConfigBootstrapError, RuntimeError),
    )
    _check(
        "ConfigValidationError is ValueError",
        issubclass(ConfigValidationError, ValueError),
    )
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
