# File: runtime/run_agent.py

#!/usr/bin/env python3
"""
Green Agent - Main Execution Script
Supports: legacy, unified, and compare modes
"""

import asyncio
import argparse
import json
import time
from typing import Dict, Optional
from dataclasses import dataclass
from pathlib import Path
import yaml

# Import unified orchestrator
from src.integration.unified_orchestrator import UnifiedGreenAgent
from src.interpretation.workload_interpreter import WorkloadInterpreter
from src.carbon.task_carbon_profiler import CarbonProfiler
from dashboard.api_server import DashboardAPI

@dataclass
class ExecutionResult:
    """Result from agent execution"""
    mode: str
    execution_time: float
    energy_consumed: float
    carbon_emitted: float
    accuracy: float
    negawatt_reward: float
    metrics: Dict

class GreenAgentRunner:
    """Main agent runner with mode support"""
    
    def __init__(self, config_path: str = "config/green_agent_config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.mode = self.config['system']['mode']
        self.unified_agent = None
        self.legacy_components = {}
        self.dashboard = None
        
    async def initialize(self):
        """Initialize components based on mode"""
        print(f"🚀 Initializing Green Agent in '{self.mode}' mode...\n")
        
        if self.mode in ['unified', 'compare']:
            self.unified_agent = UnifiedGreenAgent(self.config)
            await self.unified_agent.initialize()
            print("✅ Unified agent initialized\n")
        
        if self.mode in ['legacy', 'compare']:
            await self._initialize_legacy_components()
            print("✅ Legacy components initialized\n")
        
        # Initialize dashboard
        if self.config.get('dashboard', {}).get('enabled', True):
            self.dashboard = DashboardAPI(self.config)
            await self.dashboard.start()
            print(f"📊 Dashboard available at: http://localhost:8000\n")
    
    async def _initialize_legacy_components(self):
        """Initialize legacy components for backward compatibility"""
        from policy.policy_engine import PolicyEngine
        from rewards.negawatt_reward import NegawattRewardCalculator
        from carbon.carbon_forecast import CarbonForecaster
        
        self.legacy_components = {
            'policy_engine': PolicyEngine(self.config),
            'negawatt_calculator': NegawattRewardCalculator(self.config),
            'carbon_forecaster': CarbonForecaster(self.config)
        }
    
    async def execute_task(self, task: Dict) -> ExecutionResult:
        """Execute task based on current mode"""
        start_time = time.time()
        
        if self.mode == 'unified':
            result = await self._execute_unified(task)
        elif self.mode == 'legacy':
            result = await self._execute_legacy(task)
        elif self.mode == 'compare':
            result = await self._execute_comparison(task)
        else:
            raise ValueError(f"Unknown mode: {self.mode}")
        
        execution_time = time.time() - start_time
        
        return ExecutionResult(
            mode=self.mode,
            execution_time=execution_time,
            **result
        )
    
    async def _execute_unified(self, task: Dict) -> Dict:
        """Execute using unified orchestrator"""
        print("🔄 Executing with Unified Green Agent...")
        
        result = await self.unified_agent.execute(task)
        
        # Log to dashboard
        if self.dashboard:
            await self.dashboard.log_execution({
                'mode': 'unified',
                'task_id': task.get('id', 'unknown'),
                'energy': result.metrics.get('energy_consumed', 0),
                'carbon': result.metrics.get('carbon_emitted', 0),
                'accuracy': result.accuracy,
                'timestamp': time.time()
            })
        
        return {
            'energy_consumed': result.metrics.get('energy_consumed', 0),
            'carbon_emitted': result.metrics.get('carbon_emitted', 0),
            'accuracy': result.accuracy,
            'negawatt_reward': result.metrics.get('negawatt_reward', 0),
            'metrics': result.metrics
        }
    
    async def _execute_legacy(self, task: Dict) -> Dict:
        """Execute using legacy components"""
        print("🔄 Executing with Legacy Components...")
        
        # Legacy execution flow
        policy_engine = self.legacy_components['policy_engine']
        negawatt_calc = self.legacy_components['negawatt_calculator']
        forecaster = self.legacy_components['carbon_forecaster']
        
        # Get carbon forecast
        carbon_intensity = await forecaster.get_current_intensity()
        
        # Apply policy
        policy_decision = await policy_engine.evaluate(task, carbon_intensity)
        
        # Execute task (simplified)
        accuracy = 0.85  # Simulated
        energy = 1.5  # kWh (simulated)
        
        # Calculate negawatt reward
        negawatt_reward = await negawatt_calc.calculate(
            energy_used=energy,
            baseline_energy=2.0,
            task_difficulty=0.7
        )
        
        carbon_emitted = energy * carbon_intensity / 1000
        
        return {
            'energy_consumed': energy,
            'carbon_emitted': carbon_emitted,
            'accuracy': accuracy,
            'negawatt_reward': negawatt_reward,
            'metrics': {
                'policy_decision': policy_decision,
                'carbon_intensity': carbon_intensity
            }
        }
    
    async def _execute_comparison(self, task: Dict) -> Dict:
        """Execute both modes and compare results"""
        print("=" * 70)
        print("📊 RUNNING COMPARISON MODE: Unified vs Legacy")
        print("=" * 70 + "\n")
        
        # Execute unified
        print("1️⃣ Running Unified Agent...")
        unified_start = time.time()
        unified_result = await self._execute_unified(task)
        unified_time = time.time() - unified_start
        
        # Execute legacy
        print("\n2️⃣ Running Legacy Components...")
        legacy_start = time.time()
        legacy_result = await self._execute_legacy(task)
        legacy_time = time.time() - legacy_start
        
        # Compare results
        print("\n" + "=" * 70)
        print("📈 COMPARISON RESULTS")
        print("=" * 70)
        
        comparison = self._generate_comparison(
            unified_result, legacy_result,
            unified_time, legacy_time
        )
        
        self._print_comparison(comparison)
        
        # Save comparison to file
        comparison_file = Path("results/comparison_results.json")
        comparison_file.parent.mkdir(exist_ok=True)
        with open(comparison_file, 'w') as f:
            json.dump(comparison, f, indent=2)
        
        print(f"\n💾 Comparison saved to: {comparison_file}")
        print("=" * 70 + "\n")
        
        return unified_result  # Return unified as primary result
    
    def _generate_comparison(self, unified: Dict, legacy: Dict, 
                            unified_time: float, legacy_time: float) -> Dict:
        """Generate comparison metrics"""
        
        energy_improvement = (
            (legacy['energy_consumed'] - unified['energy_consumed']) / 
            legacy['energy_consumed'] * 100
        )
        
        carbon_improvement = (
            (legacy['carbon_emitted'] - unified['carbon_emitted']) / 
            legacy['carbon_emitted'] * 100
        )
        
        speed_improvement = (
            (legacy_time - unified_time) / legacy_time * 100
        )
        
        return {
            'timestamp': time.time(),
            'task_id': 'comparison_run',
            'unified': {
                'execution_time': unified_time,
                'energy_kwh': unified['energy_consumed'],
                'carbon_kg': unified['carbon_emitted'],
                'accuracy': unified['accuracy'],
                'negawatt_reward': unified['negawatt_reward']
            },
            'legacy': {
                'execution_time': legacy_time,
                'energy_kwh': legacy['energy_consumed'],
                'carbon_kg': legacy['carbon_emitted'],
                'accuracy': legacy['accuracy'],
                'negawatt_reward': legacy['negawatt_reward']
            },
            'improvements': {
                'energy_reduction_percent': energy_improvement,
                'carbon_reduction_percent': carbon_improvement,
                'speed_improvement_percent': speed_improvement,
                'energy_saved_kwh': legacy['energy_consumed'] - unified['energy_consumed'],
                'carbon_saved_kg': legacy['carbon_emitted'] - unified['carbon_emitted']
            },
            'verdict': self._generate_verdict(energy_improvement, carbon_improvement)
        }
    
    def _generate_verdict(self, energy_imp: float, carbon_imp: float) -> str:
        """Generate verdict based on improvements"""
        if energy_imp > 50 and carbon_imp > 50:
            return "✅ UNIFIED MODE SIGNIFICANTLY OUTPERFORMS LEGACY"
        elif energy_imp > 20 and carbon_imp > 20:
            return "✅ UNIFIED MODE SHOWS MODERATE IMPROVEMENT"
        elif energy_imp > 0 or carbon_imp > 0:
            return "⚠️  UNIFIED MODE SHOWS SLIGHT IMPROVEMENT"
        else:
            return "❌ LEGACY MODE PERFORMS BETTER (investigation needed)"
    
    def _print_comparison(self, comparison: Dict):
        """Print comparison results in formatted way"""
        imp = comparison['improvements']
        
        print(f"\n{'Metric':<30} {'Unified':<15} {'Legacy':<15} {'Improvement':<15}")
        print("-" * 75)
        print(f"{'Execution Time (s)':<30} {comparison['unified']['execution_time']:<15.4f} "
              f"{comparison['legacy']['execution_time']:<15.4f} "
              f"{imp['speed_improvement_percent']:+.1f}%")
        print(f"{'Energy (kWh)':<30} {comparison['unified']['energy_kwh']:<15.4f} "
              f"{comparison['legacy']['energy_kwh']:<15.4f} "
              f"{imp['energy_reduction_percent']:+.1f}%")
        print(f"{'Carbon (kg CO₂)':<30} {comparison['unified']['carbon_kg']:<15.4f} "
              f"{comparison['legacy']['carbon_kg']:<15.4f} "
              f"{imp['carbon_reduction_percent']:+.1f}%")
        print(f"{'Accuracy':<30} {comparison['unified']['accuracy']:<15.4f} "
              f"{comparison['legacy']['accuracy']:<15.4f} "
              f"{'-':<15}")
        print(f"{'Negawatt Reward':<30} {comparison['unified']['negawatt_reward']:<15.4f} "
              f"{comparison['legacy']['negawatt_reward']:<15.4f} "
              f"{'-':<15}")
        
        print(f"\n🎯 VERDICT: {comparison['verdict']}")
        print(f"💾 Energy Saved: {imp['energy_saved_kwh']:.4f} kWh")
        print(f"🌱 Carbon Saved: {imp['carbon_saved_kg']:.4f} kg CO₂")
    
    async def shutdown(self):
        """Graceful shutdown"""
        print("\n🛑 Shutting down Green Agent...")
        
        if self.dashboard:
            await self.dashboard.stop()
        
        if self.unified_agent:
            await self.unified_agent.shutdown()
        
        print("✅ Shutdown complete")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Green Agent Runner')
    parser.add_argument('--mode', choices=['legacy', 'unified', 'compare'],
                       default='unified', help='Execution mode')
    parser.add_argument('--config', default='config/green_agent_config.yaml',
                       help='Path to config file')
    parser.add_argument('--task', type=str, help='Task JSON file')
    
    args = parser.parse_args()
    
    # Create runner
    runner = GreenAgentRunner(config_path=args.config)
    
    try:
        # Initialize
        await runner.initialize()
        
        # Load or create task
        if args.task:
            with open(args.task, 'r') as f:
                task = json.load(f)
        else:
            # Default test task
            task = {
                'id': 'test_task_001',
                'type': 'ml_inference',
                'model_size': '1B',
                'input_size': '1MB',
                'deadline': time.time() + 3600,
                'priority': 5
            }
        
        # Execute
        result = await runner.execute_task(task)
        
        # Print results
        print("\n" + "=" * 70)
        print("✅ EXECUTION COMPLETE")
        print("=" * 70)
        print(f"Mode: {result.mode}")
        print(f"Execution Time: {result.execution_time:.4f}s")
        print(f"Energy Consumed: {result.energy_consumed:.4f} kWh")
        print(f"Carbon Emitted: {result.carbon_emitted:.4f} kg CO₂")
        print(f"Accuracy: {result.accuracy:.4f}")
        print(f"Negawatt Reward: {result.negawatt_reward:.4f}")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await runner.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
