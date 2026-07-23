# src/config.py
from pydantic import BaseSettings, Field

class GreenAgentConfig(BaseSettings):
    # General
    instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    log_level: str = "INFO"

    # Synthetic data
    synthetic_seed: int = 42
    synthetic_token_mean: float = 5.5
    synthetic_token_std: float = 1.2

    synthetic_data: Dict[str, Any] = {
        'seed': 42,
        'token_mean': 5.5,
        'token_std': 1.2,
        'task_type_distribution': {
            'summarization': 0.25,
            'classification': 0.20,
            'translation': 0.15,
            'question_answering': 0.15,
            'text_generation': 0.15,
            'sentiment_analysis': 0.10
        }
    carbon_marketplace: Dict[str, Any] = {
        'refresh_interval_seconds': 3600,
        'auto_offset_enabled': True,
        'auto_offset_threshold_kg': 100.0,}
    }  
    # After core modules are created (db_manager, blockchain, carbon_manager, sustainability_engine)

    marketplace = CarbonCreditMarketplace(
        config=config.carbon_marketplace,
        db_manager=db_manager,
        blockchain=blockchain,  # your existing blockchain module
        carbon_manager=carbon_manager,
        sustainability_engine=sustainability_engine
    )
    # Start auto‑offset loop (optional)
    asyncio.create_task(marketplace.start_auto_offset_loop())

# Store marketplace instance globally for use in API or routing decisions
    
    class Config:
        env_prefix = "GREEN_AGENT_"
