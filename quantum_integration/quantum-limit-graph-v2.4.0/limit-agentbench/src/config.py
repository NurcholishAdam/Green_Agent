# src/config.py
"""
Green Agent configuration – validated via Pydantic.

All settings can be overridden via environment variables with prefix GREEN_AGENT_.
"""

import uuid
from typing import Dict, Any, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict

# -----------------------------------------------------------------------------
# Nested sub‑configurations
# -----------------------------------------------------------------------------

class TaskTypeDistribution(BaseModel):
    """Distribution of synthetic task types."""
    summarization: float = Field(0.25, ge=0.0, le=1.0)
    classification: float = Field(0.20, ge=0.0, le=1.0)
    translation: float = Field(0.15, ge=0.0, le=1.0)
    question_answering: float = Field(0.15, ge=0.0, le=1.0)
    text_generation: float = Field(0.15, ge=0.0, le=1.0)
    sentiment_analysis: float = Field(0.10, ge=0.0, le=1.0)

    @field_validator('*', mode='after')
    @classmethod
    def check_sum(cls, values: Dict[str, float]) -> Dict[str, float]:
        if abs(sum(values.values()) - 1.0) > 1e-6:
            raise ValueError("Task type distribution probabilities must sum to 1.0")
        return values

class SyntheticDataConfig(BaseModel):
    """Configuration for synthetic data generation."""
    seed: int = Field(42, ge=0, description="Random seed for reproducibility")
    token_mean: float = Field(5.5, gt=0, description="Mean token count per task")
    token_std: float = Field(1.2, gt=0, description="Standard deviation of token count")
    task_type_distribution: TaskTypeDistribution = Field(
        default_factory=TaskTypeDistribution,
        description="Distribution of task types"
    )

class CarbonMarketplaceConfig(BaseModel):
    """Configuration for carbon credit marketplace."""
    refresh_interval_seconds: int = Field(3600, ge=60, description="How often to refresh carbon prices")
    auto_offset_enabled: bool = Field(True, description="Enable automatic carbon offsetting")
    auto_offset_threshold_kg: float = Field(100.0, gt=0, description="Threshold to trigger automatic offset")

# -----------------------------------------------------------------------------
# Main configuration
# -----------------------------------------------------------------------------

class GreenAgentConfig(BaseSettings):
    """
    Main configuration for the Green Agent system.
    
    All fields can be set via environment variables with the prefix GREEN_AGENT_.
    Example: GREEN_AGENT_LOG_LEVEL=DEBUG
    """

    # General
    instance_id: str = Field(
        default_factory=lambda: str(uuid.uuid4())[:8],
        description="Unique identifier for this agent instance"
    )
    log_level: str = Field("INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")

    # Synthetic data
    synthetic: SyntheticDataConfig = Field(
        default_factory=SyntheticDataConfig,
        description="Synthetic data generation parameters"
    )

    # Carbon marketplace
    carbon_marketplace: CarbonMarketplaceConfig = Field(
        default_factory=CarbonMarketplaceConfig,
        description="Carbon credit marketplace settings"
    )

    # Pydantic v2 settings configuration
    model_config = SettingsConfigDict(env_prefix="GREEN_AGENT_", case_sensitive=False)

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()

        distillation: Dict[str, Any] = {
            "num_epochs": 3, "batch_size": 32,
            "lr": 1e-5,
            "reverse_kl": True,
            "alpha_orm": 0.1,
            "mixed_precision": True,}

# -----------------------------------------------------------------------------
# Initialization function (to be used in main.py)
# -----------------------------------------------------------------------------

def initialize_application(
    config: GreenAgentConfig,
    db_manager: Any,
    blockchain: Any,
    carbon_manager: Any,
    sustainability_engine: Any,
) -> Dict[str, Any]:
    """
    Build the marketplace, UI, and other components based on the configuration.
    This function should be called after the core dependencies are ready.
    """
    from carbon_marketplace import CarbonCreditMarketplace
    from explainable_ui import create_explainable_ui

    # Create marketplace
    marketplace = CarbonCreditMarketplace(
        config=config.carbon_marketplace.model_dump(),
        db_manager=db_manager,
        blockchain=blockchain,
        carbon_manager=carbon_manager,
        sustainability_engine=sustainability_engine,
    )
    # Start auto‑offset loop if enabled
    if config.carbon_marketplace.auto_offset_enabled:
        import asyncio
        asyncio.create_task(marketplace.start_auto_offset_loop())

    # Create explainable UI
    ui = create_explainable_ui(config=config)
    dashboard = ui["dashboard"]
    api_ext = ui["api_extension"]

    # Register API routes on your FastAPI/Flask app (assumed to be passed)
    # api_ext.register_routes(app)  # app should be passed

    return {
        "marketplace": marketplace,
        "dashboard": dashboard,
        "api_extension": api_ext,
    }
