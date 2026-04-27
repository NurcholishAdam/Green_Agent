#!/usr/bin/env python3
"""
run_agent.py — Green Agent v5.0.0 Main Entry Point
===================================================
Initializes all components, wires helium monitoring, starts metrics export,
and runs the unified orchestration loop.

Usage:
    python -m src.runtime.run_agent --mode unified --config config/base/green_agent_config.yaml
"""

import asyncio
import argparse
import logging
import os
import sys
import signal
import yaml
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.graph_registry import GraphRegistry, GraphType
from carbon.helium_monitor import HeliumMonitor
from monitoring.graph_metrics_exporter import GraphMetricsExporter
from integration.unified_orchestrator import UnifiedGreenAgent

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Config not found: {config_path}, using defaults")
        return {}
    
    with open(path, 'r') as f:
        config = yaml.safe_load(f) or {}
    logger.info(f"Loaded configuration from {config_path}")
    return config


async def main():
    """Main entry point for Green Agent"""
    parser = argparse.ArgumentParser(description='Green Agent v5.0.0')
    parser.add_argument('--mode', choices=['legacy', 'unified', 'compare'], default='unified')
    parser.add_argument('--config', default='config/base/green_agent_config.yaml')
    parser.add_argument('--task', type=str, help='Task JSON file to execute')
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(f"Starting Green Agent v5.0.0 in {args.mode} mode")
    config = load_config(args.config)

    # Initialize core components
    registry = GraphRegistry()
    
    # ✅ Initialize Helium Monitor (if enabled in config)
    helium_config = config.get('helium', {})
    helium_monitor = None
    
    if helium_config.get('enabled', False):
        logger.info("Initializing HeliumMonitor...")
        try:
            # Load API key from environment if not in config
            api_key_env = helium_config.get('api_key_env_var', 'HELIUM_API_KEY')
            helium_config['api_key'] = os.getenv(api_key_env, helium_config.get('api_key'))
            
            helium_monitor = HeliumMonitor(
                config=helium_config,
                simulation_seed=helium_config.get('simulation_seed')
            )
            registry.register_helium_monitor(helium_monitor)
            logger.info("HeliumMonitor initialized and registered")
        except Exception as e:
            logger.error(f"Failed to initialize HeliumMonitor: {e}")
            logger.warning("Continuing without helium monitoring")

    # Initialize Unified Orchestrator
    logger.info("Initializing UnifiedGreenAgent...")
    agent = UnifiedGreenAgent(config)
    await agent.initialize()

    # Initialize Metrics Exporter with helium support
    monitoring_config = config.get('monitoring', {}).get('prometheus', {})
    metrics_port = monitoring_config.get('port', 8000)
    
    exporter = GraphMetricsExporter(
        registry=registry,
        job_name="green_agent",
        max_edges_export=100,
        helium_monitor=helium_monitor
    )
    
    # Start HTTP metrics server
    exporter.start_http_server(port=metrics_port)
    logger.info(f"Prometheus metrics endpoint: http://0.0.0.0:{metrics_port}/metrics")

    # Graceful shutdown handling
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        # Run agent loop or demo task
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

        # Keep running until shutdown signal (for metrics scraping)
        logger.info("Agent running. Press Ctrl+C to stop...")
        await shutdown_event.wait()

    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        # Graceful cleanup
        logger.info("Shutting down components...")
        await agent.shutdown()
        await registry.shutdown()
        exporter.stop_http_server()
        logger.info("Green Agent shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
