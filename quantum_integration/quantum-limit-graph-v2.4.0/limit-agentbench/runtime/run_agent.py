#!/usr/bin/env python3
"""
Green Agent v5.0.0 - Main Entry Point
File: runtime/run_agent.py
"""

import asyncio
import argparse
import logging
import yaml
from pathlib import Path
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file"""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Config not found: {config_path}, using defaults")
        return {}
    
    with open(path, 'r') as f:
        return yaml.safe_load(f) or {}


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Green Agent v5.0.0')
    parser.add_argument('--mode', choices=['legacy', 'unified', 'compare'], default='unified')
    parser.add_argument('--config', default='config/base/green_agent_config.yaml')
    parser.add_argument('--task', type=str, help='Task JSON file to execute')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    config['system'] = config.get('system', {})
    config['system']['mode'] = args.mode
    
    logger.info(f"Starting Green Agent v5.0.0 in {args.mode} mode")
    
    # Import and initialize orchestrator
    from src.integration.unified_orchestrator import UnifiedGreenAgent
    
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    
    try:
        # Execute demo task or load from file
        if args.task:
            import json
            with open(args.task, 'r') as f:
                task = json.load(f)
        else:
            task = {
                'id': 'demo_001',
                'type': 'ml_inference',
                'priority': 5,
                'deferrable': True,
                'model': 'llama-7b',
                'input_size_mb': 128
            }
        
        logger.info(f"Executing task: {task['id']}")
        result = await agent.execute_task(task)
        
        # Print results
        print(f"\n{'='*60}")
        print(f"✅ Task {result.task_id}: {'Success' if result.success else 'Failed'}")
        print(f"   Energy: {result.energy_consumed:.4f} kWh")
        print(f"   Carbon: {result.carbon_emitted:.4f} kg CO2")
        print(f"   Accuracy: {result.accuracy:.2f}")
        print(f"   Negawatt Reward: {result.negawatt_reward:.2f}")
        print(f"   Carbon Zone: {result.carbon_zone}")
        if result.errors:
            print(f"   Errors: {result.errors}")
        print(f"{'='*60}\n")
        
        return 0 if result.success else 1
        
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        return 1
        
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
