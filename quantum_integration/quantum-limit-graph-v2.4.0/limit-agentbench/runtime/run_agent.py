"""
Green Agent v5.0.0 - Main Entry Point

File: runtime/run_agent.py
Status: FOUNDATIONAL - Tier 1
"""

import asyncio
import argparse
import logging
import yaml
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


async def main():
    parser = argparse.ArgumentParser(description='Green Agent v5.0.0')
    parser.add_argument('--mode', choices=['legacy', 'unified', 'compare'], default='unified')
    parser.add_argument('--config', default='config/base/green_agent_config.yaml')
    args = parser.parse_args()
    
    config_path = Path(args.config)
    if not config_path.exists():
        config_path = Path('config/green_agent_config.yaml')
    
    config = load_config(config_path) if config_path.exists() else {}
    config['system'] = config.get('system', {})
    config['system']['mode'] = args.mode
    
    from src.integration.unified_orchestrator import UnifiedGreenAgent
    
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    
    test_task = {
        'id': 'demo_001',
        'type': 'ml_inference',
        'priority': 5,
        'deferrable': True
    }
    
    result = await agent.execute_task(test_task)
    
    print(f"\n✅ Task {result.task_id}: {'Success' if result.success else 'Failed'}")
    print(f"   Energy: {result.energy_consumed:.4f} kWh")
    print(f"   Carbon: {result.carbon_emitted:.4f} kg CO2")
    print(f"   Accuracy: {result.accuracy:.2f}")
    print(f"   Negawatt: {result.negawatt_reward:.2f}")
    
    await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
