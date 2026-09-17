# src/config.py

"""
Green Agent configuration — validated via Pydantic v2.

All settings can be overridden via environment variables with prefix
``GREEN_AGENT_``. Fields that carry a non-prefixed alias (e.g.
``PARETO_QUALITY_MIN``) are also overridable directly.

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

Enhancements over the previous version
--------------------------------------
1. All ``PARETO_*``, ``QUEUE_*``, ``DRIFT_*``, ``BENCHMARK_*`` and
   ``DASHBOARD_*`` fields are now **inside** ``GreenAgentConfig`` (previously
   they were accidentally emitted at module scope, doing nothing).
2. The orphaned ``distillation`` dict is promoted to a real
   :class:`DistillationConfig` nested model.
3. Pydantic v2 idiom throughout: ``field_validator`` /
   ``model_validator`` / ``SettingsConfigDict`` / ``validation_alias``.
4. Defensive imports for ``carbon_marketplace`` and ``explainable_ui`` so a
   missing dependency raises :class:`ConfigBootstrapError` with a clear
   message rather than a bare ``ImportError``.
5. ``initialize_application`` is now **async**, retries marketplace boot,
   and returns a :class:`BootstrapResult` with a real ``shutdown()``
   coroutine (previously it called ``asyncio.create_task`` from sync code
   and raised ``RuntimeError`` when no loop was running).
6. Cross-field validation ensures ``REDIS_URL`` is set when
   ``queue_type == "redis"``.
7. JSON round-trip helpers and a ``get_log_level_int`` convenience.
8. Structured logger, ``__version__``, ``__all__``, and a ``__main__``
   smoke test that exercises defaults, env overrides, cross-field
   validation, and JSON round-trip.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# =========================================================================== #
# Errors
# =========================================================================== #
class ConfigError(ValueError):
    """Raised for invalid configuration values."""


class ConfigBootstrapError(RuntimeError):
    """Raised when ``initialize_application`` cannot wire a component."""


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
            raise ValueError(
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
    """Configuration for the distillation / fine-tuning pipeline.

    Replaces the ``distillation`` dict that was previously nested (and
    unreachable) inside a validator body.
    """

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
    2. Environment variables with prefix ``GREEN_AGENT_`` (case-insensitive).
    3. Environment variables matching each field's ``validation_alias``
       (e.g. ``PARETO_QUALITY_MIN``).
    4. Field defaults.
    """

    model_config = SettingsConfigDict(
        env_prefix="GREEN_AGENT_",
        case_sensitive=False,
        extra="ignore",
        frozen=False,          # settings may be amended during bootstrap
        populate_by_name=True, # allow field name OR alias
    )

    # ---- General --------------------------------------------------------- #
    instance_id: str = Field(
        default_factory=lambda: uuid.uuid4().hex[:8],
        min_length=4,
        max_length=32,
        description="Unique identifier for this agent instance",
    )
    schema_version: str = Field(
        __version__,
        description="Config schema version",
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
            raise ValueError("log_level must be a string")
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.strip().upper()
        if upper not in allowed:
            raise ValueError(
                f"log_level must be one of {sorted(allowed)}, got {v!r}"
            )
        return upper

    @field_validator("instance_id", mode="after")
    @classmethod
    def _validate_instance_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("instance_id must not be empty")
        return v

    @field_validator("redis_url", mode="after")
    @classmethod
    def _validate_redis_url(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("redis_url must not be a blank string")
        return v

    # ---- Cross-field validation ------------------------------------------ #
    @model_validator(mode="after")
    def _cross_field_checks(self) -> "GreenAgentConfig":
        if self.queue_type == "redis" and not self.redis_url:
            raise ValueError(
                "REDIS_URL must be set when QUEUE_TYPE == 'redis'."
            )
        if self.pareto_quality_min > 1.0:
            # redundant given ge/le above, kept as an explicit invariant
            raise ValueError("pareto_quality_min must be <= 1.0")
        return self

    # ---- Convenience ----------------------------------------------------- #
    def get_log_level_int(self) -> int:
        """Return the numeric logging level for :attr:`log_level`."""
        level = logging.getLevelName(self.log_level)
        return level if isinstance(level, int) else logging.INFO

    def is_production(self) -> bool:
        """Return ``True`` when :attr:`runtime_env` is ``"prod"``."""
        return self.runtime_env == "prod"

    def apply_logging(self) -> None:
        """Configure the root logger using this config's level."""
        logging.basicConfig(
            level=self.get_log_level_int(),
            format="%(levelname)s %(name)s: %(message)s",
        )

    def to_json(self, *, indent: Optional[int] = 2) -> str:
        """Serialize to JSON (Pydantic v2 ``model_dump_json``)."""
        return self.model_dump_json(indent=indent)

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain JSON-safe dict."""
        return self.model_dump(mode="json")

    @classmethod
    def from_json(cls, payload: str) -> "GreenAgentConfig":
        """Deserialize a JSON payload into a validated config."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ConfigError("config JSON must be an object")
        return cls.model_validate(data)

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "GreenAgentConfig":
        """Build a config from a mapping (defaults to ``os.environ``)."""
        source = env if env is not None else dict(os.environ)

        def _get(*keys: str) -> Optional[str]:
            for k in keys:
                if k in source:
                    return source[k]
            return None

        kwargs: Dict[str, Any] = {}
        # Prefixed overrides.
        if (v := _get("GREEN_AGENT_LOG_LEVEL")) is not None:
            kwargs["log_level"] = v
        if (v := _get("GREEN_AGENT_RUNTIME_ENV")) is not None:
            kwargs["runtime_env"] = v
        # Non-prefixed aliases.
        alias_map = {
            "pareto_quality_min": ("PARETO_QUALITY_MIN",),
            "pareto_latency_max": ("PARETO_LATENCY_MAX",),
            "pareto_carbon_max": ("PARETO_CARBON_MAX",),
            "queue_type": ("QUEUE_TYPE",),
            "redis_url": ("REDIS_URL",),
            "offline_batch_size": ("OFFLINE_BATCH_SIZE",),
            "offline_update_interval_sec": ("OFFLINE_UPDATE_INTERVAL_SEC",),
            "drift_threshold": ("DRIFT_THRESHOLD",),
            "rollback_enabled": ("ROLLBACK_ENABLED",),
            "benchmark_interval_days": ("BENCHMARK_INTERVAL_DAYS",),
            "dashboard_port": ("DASHBOARD_PORT",),
            "dashboard_enabled": ("DASHBOARD_ENABLED",),
        }
        for field_name, keys in alias_map.items():
            if (v := _get(*keys)) is not None:
                kwargs[field_name] = v
        return cls(**kwargs)


# =========================================================================== #
# Bootstrap result
# =========================================================================== #
@dataclass
class BootstrapResult:
    """Return value of :func:`initialize_application`.

    Holds live component references plus a :meth:`shutdown` coroutine that
    cancels the marketplace auto-offset loop and releases resources. Designed
    to be used with ``async with`` or explicit ``await result.shutdown()``.
    """

    marketplace: Any = None
    dashboard: Any = None
    api_extension: Any = None
    auto_offset_task: Optional["asyncio.Task[None]"] = None
    started_at: float = field(default_factory=time.time)

    async def shutdown(self) -> None:
        """Cancel background tasks and best-effort shutdown collaborators."""
        # 1. Cancel the auto-offset loop, if running.
        if self.auto_offset_task is not None and not self.auto_offset_task.done():
            self.auto_offset_task.cancel()
            try:
                await self.auto_offset_task
            except asyncio.CancelledError:
                logger.debug("Auto-offset loop cancelled.")
            except Exception:
                logger.exception("Auto-offset loop raised during cancel.")

        # 2. Ask collaborators to shut down, if they expose a coroutine.
        for name in ("marketplace", "dashboard", "api_extension"):
            component = getattr(self, name, None)
            if component is None:
                continue
            for method_name in ("shutdown", "stop", "close", "aclose"):
                method = getattr(component, method_name, None)
                if method is None or not callable(method):
                    continue
                try:
                    result = method()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    logger.exception(
                        "%s.%s() failed during shutdown.",
                        name, method_name,
                    )
                break

        logger.info("BootstrapResult shutdown complete.")

    async def __aenter__(self) -> "BootstrapResult":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()


# =========================================================================== #
# Initialization
# =========================================================================== #
def _import_optional(module_name: str, attr: str) -> Any:
    """Import ``attr`` from ``module_name`` with a package-relative fallback.

    Raises
    ------
    ConfigBootstrapError
        When the module or attribute cannot be resolved.
    """
    candidates = [module_name]
    # Also try the ``src.`` prefix so this works whether the module was
    # executed as a top-level script or as part of a package.
    if not module_name.startswith("src."):
        candidates.append(f"src.{module_name}")

    last_exc: Optional[BaseException] = None
    for candidate in candidates:
        try:
            module = __import__(candidate, fromlist=[attr])
            return getattr(module, attr)
        except (ImportError, AttributeError) as exc:
            last_exc = exc
            continue
    raise ConfigBootstrapError(
        f"required component '{attr}' from '{module_name}' is "
        f"unavailable: {last_exc}"
    ) from last_exc


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
        Fully validated configuration.
    db_manager, blockchain, carbon_manager, sustainability_engine : Any
        Core collaborators already initialised by the caller.
    loop : asyncio.AbstractEventLoop, optional
        Event loop on which to schedule the auto-offset loop. Defaults to the
        running loop.
    max_retries : int, default 2
        Number of retries for the marketplace constructor.
    retry_backoff_seconds : float, default 1.0
        Base of the linear backoff between retries.

    Returns
    -------
    BootstrapResult
        Live components + a ``shutdown()`` coroutine.

    Raises
    ------
    ConfigBootstrapError
        If a required component cannot be imported, the marketplace fails to
        initialise, or the explainable-UI factory returns an unexpected
        shape.
    """
    if not isinstance(config, GreenAgentConfig):
        raise ConfigBootstrapError(
            "config must be a GreenAgentConfig instance."
        )

    # ---- Resolve components (defensive imports) ---------------------- #
    CarbonCreditMarketplace = _import_optional(
        "carbon_marketplace", "CarbonCreditMarketplace"
    )
    create_explainable_ui = _import_optional(
        "explainable_ui", "create_explainable_ui"
    )

    # ---- Build the marketplace with retries -------------------------- #
    marketplace: Any = None
    last_exc: Optional[BaseException] = None
    attempts = max_retries + 1
    for attempt in range(1, attempts + 1):
        try:
            marketplace = CarbonCreditMarketplace(
                config=config.carbon_marketplace.model_dump(),
                db_manager=db_manager,
                blockchain=blockchain,
                carbon_manager=carbon_manager,
                sustainability_engine=sustainability_engine,
            )
            break
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Marketplace boot attempt %d/%d failed: %s",
                attempt, attempts, exc,
            )
            if attempt < attempts:
                await asyncio.sleep(retry_backoff_seconds * attempt)
    if marketplace is None:
        raise ConfigBootstrapError(
            f"marketplace failed to initialise after {attempts} "
            f"attempt(s): {last_exc}"
        ) from last_exc

    # ---- Schedule the auto-offset loop ------------------------------- #
    auto_offset_task: Optional[asyncio.Task] = None
    if config.carbon_marketplace.auto_offset_enabled:
        method = getattr(marketplace, "start_auto_offset_loop", None)
        if not callable(method):
            logger.warning(
                "Marketplace does not expose start_auto_offset_loop(); "
                "auto-offset loop skipped.",
            )
        else:
            try:
                active_loop = loop or asyncio.get_running_loop()
            except RuntimeError as exc:
                raise ConfigBootstrapError(
                    "initialize_application must be called from within a "
                    "running event loop."
                ) from exc
            try:
                auto_offset_task = active_loop.create_task(method())
            except Exception as exc:
                logger.exception("Could not start auto-offset loop: %s", exc)
                if config.is_production():
                    raise ConfigBootstrapError(
                        f"auto-offset loop failed: {exc}"
                    ) from exc

    # ---- Explainable UI ---------------------------------------------- #
    ui = create_explainable_ui(config=config)
    if not isinstance(ui, dict):
        raise ConfigBootstrapError(
            "create_explainable_ui must return a dict with "
            "'dashboard' and 'api_extension' keys."
        )
    if "dashboard" not in ui:
        raise ConfigBootstrapError(
            "create_explainable_ui return value is missing 'dashboard'."
        )
    if "api_extension" not in ui:
        raise ConfigBootstrapError(
            "create_explainable_ui return value is missing 'api_extension'."
        )

    logger.info(
        "Application initialised (env=%s, queue=%s, dashboard=%s, "
        "auto_offset=%s).",
        config.runtime_env,
        config.queue_type,
        config.dashboard_enabled,
        config.carbon_marketplace.auto_offset_enabled,
    )

    return BootstrapResult(
        marketplace=marketplace,
        dashboard=ui["dashboard"],
        api_extension=ui["api_extension"],
        auto_offset_task=auto_offset_task,
        started_at=time.time(),
    )


# =========================================================================== #
# Optional synchronous shim (for callers that cannot adopt async yet)
# =========================================================================== #
def initialize_application_sync(
    config: GreenAgentConfig,
    db_manager: Any,
    blockchain: Any,
    carbon_manager: Any,
    sustainability_engine: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Synchronous compatibility wrapper around :func:`initialize_application`.

    Returns the same dict shape as the original ``initialize_application``
    (``{"marketplace", "dashboard", "api_extension"}``). The background
    auto-offset task, if scheduled, is attached to the returned dict's
    ``"auto_offset_task"`` key for the caller to await at shutdown.
    """
    result = asyncio.run(
        initialize_application(
            config,
            db_manager,
            blockchain,
            carbon_manager,
            sustainability_engine,
            **kwargs,
        )
    )
    return {
        "marketplace": result.marketplace,
        "dashboard": result.dashboard,
        "api_extension": result.api_extension,
        "auto_offset_task": result.auto_offset_task,
    }


# =========================================================================== #
# Public API
# =========================================================================== #
__all__ = [
    "__version__",
    "BootstrapResult",
    "CarbonMarketplaceConfig",
    "ConfigBootstrapError",
    "ConfigError",
    "DistillationConfig",
    "GreenAgentConfig",
    "SyntheticDataConfig",
    "TaskTypeDistribution",
    "initialize_application",
    "initialize_application_sync",
]


# =========================================================================== #
# Smoke test: python -m config
# =========================================================================== #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ---- 1. Default construction ------------------------------------- #
    cfg = GreenAgentConfig()
    print("instance_id     :", cfg.instance_id)
    print("schema_version  :", cfg.schema_version)
    print("runtime_env     :", cfg.runtime_env)
    print("log_level       :", cfg.log_level, f"({cfg.get_log_level_int()})")
    print("queue_type      :", cfg.queue_type)
    print("pareto_q_min    :", cfg.pareto_quality_min)
    print("pareto_lat_max  :", cfg.pareto_latency_max)
    print("pareto_carbon   :", cfg.pareto_carbon_max)
    print("dashboard_port  :", cfg.dashboard_port)
    print("distillation    :", cfg.distillation.model_dump())
    print("synthetic seed  :", cfg.synthetic.seed)
    print("task distrib.   :", cfg.synthetic.task_type_distribution.as_dict())

    # ---- 2. Env override demonstration ------------------------------- #
    os.environ["PARETO_QUALITY_MIN"] = "0.85"
    os.environ["GREEN_AGENT_LOG_LEVEL"] = "debug"
    os.environ["QUEUE_TYPE"] = "asyncio"
    cfg2 = GreenAgentConfig.from_env()
    print("\nafter env override:")
    print("  pareto_q_min  :", cfg2.pareto_quality_min, "(expected 0.85)")
    print("  log_level     :", cfg2.log_level, "(expected DEBUG)")

    # ---- 3. Cross-field validation ----------------------------------- #
    try:
        GreenAgentConfig(queue_type="redis")
    except Exception as exc:
        print("\nredis w/o URL  : rejected (", type(exc).__name__, ")")
    else:  # pragma: no cover
        raise AssertionError("expected cross-field validation to fail")

    try:
        GreenAgentConfig(queue_type="redis", redis_url="redis://localhost:6379")
    except Exception as exc:  # pragma: no cover
        raise AssertionError(f"redis with URL should succeed, got {exc}")
    else:
        print("redis + URL    : accepted")

    # ---- 4. JSON round-trip ------------------------------------------ #
    payload = cfg.to_json()
    restored = GreenAgentConfig.from_json(payload)
    assert restored.instance_id == cfg.instance_id
    assert restored.distillation == cfg.distillation
    print("\nRound-trip     : OK")

    # ---- 5. Field-level validation failures -------------------------- #
    for bad in ("verbose", "", None, 42):
        try:
            GreenAgentConfig(log_level=bad)  # type: ignore[arg-type]
        except Exception as exc:
            print("Rejected log   :", type(exc).__name__)
        else:  # pragma: no cover
            raise AssertionError(f"expected rejection for log_level={bad!r}")

    for bad in (0.9, 0.5, 0.3):  # sum != 1
        try:
            TaskTypeDistribution(
                summarization=bad,
                classification=bad,
                translation=bad,
                question_answering=bad,
                text_generation=bad,
                sentiment_analysis=bad,
            )
        except Exception:
            print(f"Rejected dist  : sum={bad * 6:.2f}")
            break
    else:  # pragma: no cover
        raise AssertionError("expected distribution sum check to fail")

    # ---- 6. Bootstrap smoke test (mocked dependencies) --------------- #
    async def _smoke_bootstrap() -> None:
        # Create synthetic modules on the fly so the test does not depend on
        # external packages being installed.
        import sys
        import types

        cm_mod = types.ModuleType("carbon_marketplace")

        class _FakeMarketplace:
            def __init__(self, **kwargs: Any) -> None:
                self.kwargs = kwargs

            async def start_auto_offset_loop(self) -> None:
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    logger.debug("FakeMarketplace auto-offset cancelled.")

            async def shutdown(self) -> None:
                pass

        cm_mod.CarbonCreditMarketplace = _FakeMarketplace  # type: ignore[attr-defined]
        sys.modules["carbon_marketplace"] = cm_mod

        ui_mod = types.ModuleType("explainable_ui")

        def _create_explainable_ui(config: Any) -> Dict[str, Any]:
            return {"dashboard": object(), "api_extension": object()}

        ui_mod.create_explainable_ui = _create_explainable_ui  # type: ignore[attr-defined]
        sys.modules["explainable_ui"] = ui_mod

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
        print("\nbootstrap      : marketplace=", result.marketplace is not None)
        print("bootstrap      : dashboard=", result.dashboard is not None)
        print("bootstrap      : api_extension=", result.api_extension is not None)
        print("bootstrap      : auto_offset_task=", result.auto_offset_task is not None)

        await result.shutdown()
        print("shutdown       : OK")

    asyncio.run(_smoke_bootstrap())

    print("\nSmoke test passed.")
