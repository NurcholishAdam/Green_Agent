"""
Integration Tests for Unified Green Agent v5.0
===============================================

Tests all 12 layers working together

Location: tests/integration/test_unified_system.py
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any

# Import unified orchestrator
from src.integration.unified_orchestrator import (
    UnifiedGreenAgent,
    create_unified_agent,
    UnifiedResult
)


class TestUnifiedSystem:
    """Integration tests for complete 12-layer system"""
    
    @pytest.fixture
    async def agent(self):
        """Create unified agent for testing"""
        agent = await create_unified_agent(
            enable_meta_cognitive=False,  # Optional
            enable_neuro_symbolic=False,  # Optional
            enable_quantum=False,  # Optional
            num_ray_workers=2  # Minimal for testing
        )
        yield agent
        await agent.shutdown()
    
    @pytest.mark.asyncio
    async def test_complete_workflow(self, agent):
        """Test complete 12-layer workflow"""
        
        task = {
            "task_id": "test_complete",
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 1_000,
            "num_epochs": 1,
            "batch_size": 16,
            "hardware": "V100",
            "team": "test_team"
        }
        
        result = await agent.execute(task)
        
        # Basic assertions
        assert result.status == "completed"
        assert result.task_id == "test_complete"
        assert result.accuracy > 0
        assert result.energy_kwh > 0
        assert result.carbon_kgco2e > 0
        
        # Verify carbon savings
        assert result.carbon_saved_kgco2e > 0
        assert result.carbon_savings_pct > 50.0  # Should save >50%
        
        # Verify workload parsing worked
        assert result.workload_profile is not None
        assert result.workload_profile.model_params > 0
        
        print(f"✅ Complete workflow test passed")
        print(f"   Carbon saved: {result.carbon_savings_pct:.1f}%")
    
    @pytest.mark.asyncio
    async def test_data_optimization(self, agent):
        """Test data optimization layer"""
        
        # Create mock dataset
        dataset = [
            {"id": f"sample_{i}", "text": f"Sample text {i}", "label": i % 2}
            for i in range(100)
        ]
        
        task = {
            "task_id": "test_data_opt",
            "model_name": "bert-base",
            "task_type": "fine_tuning",
            "dataset_size": len(dataset),
            "team": "test_team"
        }
        
        result = await agent.execute(task, dataset=dataset)
        
        assert result.status == "completed"
        assert result.data_optimization is not None
        
        # Verify compression happened
        data_opt = result.data_optimization
        assert data_opt["optimized_size"] < data_opt["original_size"]
        assert data_opt["estimated_energy_savings_kwh"] > 0
        
        print(f"✅ Data optimization test passed")
        print(f"   Compression: {data_opt['compression_ratio']:.1f}x")
    
    @pytest.mark.asyncio
    async def test_carbon_budget_enforcement(self, agent):
        """Test carbon budget blocking"""
        
        # Set very low budget
        agent.ledger.set_team_budget("budget_test_team", "2026-03", 0.001)
        
        task = {
            "task_id": "test_budget",
            "model_name": "bert-large",  # Large model
            "task_type": "training",  # Expensive
            "dataset_size": 100_000,  # Large dataset
            "team": "budget_test_team"
        }
        
        result = await agent.execute(task)
        
        # Should be blocked
        assert result.status == "blocked"
        assert "budget" in result.reasoning.lower()
        
        print(f"✅ Budget enforcement test passed")
    
    @pytest.mark.asyncio
    async def test_policy_enforcement(self, agent):
        """Test policy engine blocks full fine-tuning"""
        
        task = {
            "task_id": "test_policy",
            "model_name": "llama-7b",  # Large model
            "task_type": "fine_tuning",
            "dataset_size": 1_000,  # Small dataset
            "fine_tuning_method": "full_fine_tuning",  # Should be blocked
            "team": "test_team"
        }
        
        result = await agent.execute(task)
        
        # Policy should enforce LoRA instead
        assert result.status == "completed"
        if result.decision:
            # Method should be changed to LoRA
            assert result.decision.get("how") != "full_fine_tuning"
        
        print(f"✅ Policy enforcement test passed")
    
    @pytest.mark.asyncio
    async def test_benchmarking(self, agent):
        """Test benchmark intelligence recording"""
        
        task = {
            "task_id": "test_benchmark",
            "model_name": "bert-base",
            "task_type": "fine_tuning",
            "dataset_name": "test_dataset",
            "dataset_size": 500,
            "team": "test_team"
        }
        
        result = await agent.execute(task)
        
        assert result.status == "completed"
        
        # Check if benchmark was recorded
        bench_stats = agent.benchmark_intelligence.get_statistics()
        assert bench_stats["num_benchmarks"] > 0
        
        print(f"✅ Benchmarking test passed")
        print(f"   Benchmarks recorded: {bench_stats['num_benchmarks']}")
    
    @pytest.mark.asyncio
    async def test_multiple_tasks(self, agent):
        """Test executing multiple tasks"""
        
        tasks = [
            {
                "task_id": f"test_multi_{i}",
                "model_name": "bert-base",
                "task_type": "fine_tuning",
                "dataset_size": 500,
                "team": "test_team"
            }
            for i in range(3)
        ]
        
        results = []
        for task in tasks:
            result = await agent.execute(task)
            results.append(result)
        
        # All should complete
        assert all(r.status == "completed" for r in results)
        
        # Cumulative carbon savings
        total_saved = sum(r.carbon_saved_kgco2e for r in results)
        assert total_saved > 0
        
        # Statistics should reflect multiple tasks
        stats = agent.get_statistics()
        assert stats["total_tasks_executed"] >= 3
        
        print(f"✅ Multiple tasks test passed")
        print(f"   Total tasks: {stats['total_tasks_executed']}")
        print(f"   Total carbon saved: {total_saved:.4f} kgCO2e")
    
    @pytest.mark.asyncio
    async def test_error_handling(self, agent):
        """Test error handling for invalid tasks"""
        
        invalid_task = {
            "task_id": "test_invalid",
            # Missing required fields
        }
        
        result = await agent.execute(invalid_task)
        
        # Should return error result gracefully
        assert result.status in ["error", "blocked"]
        assert len(result.reasoning) > 0
        
        print(f"✅ Error handling test passed")
    
    @pytest.mark.asyncio
    async def test_statistics_tracking(self, agent):
        """Test statistics are correctly tracked"""
        
        # Execute a task
        task = {
            "task_id": "test_stats",
            "model_name": "bert-base",
            "dataset_size": 100,
            "team": "test_team"
        }
        
        await agent.execute(task)
        
        stats = agent.get_statistics()
        
        assert stats["total_tasks_executed"] > 0
        assert stats["total_carbon_saved_kgco2e"] >= 0
        assert "decision_core" in stats
        assert "benchmarks" in stats
        
        print(f"✅ Statistics tracking test passed")


class TestComponentIntegration:
    """Test individual component integrations"""
    
    @pytest.mark.asyncio
    async def test_workload_interpreter_integration(self):
        """Test workload interpreter standalone"""
        
        from src.interpretation.workload_interpreter import WorkloadInterpreter
        
        interpreter = WorkloadInterpreter()
        
        task = {
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 10_000
        }
        
        profile = interpreter.interpret(task)
        
        assert profile.model_params > 0
        assert profile.estimated_energy_kwh > 0
        assert profile.carbon_optimization_potential > 0
        assert len(profile.execution_dag) > 0
        
        print(f"✅ Workload interpreter integration passed")
    
    @pytest.mark.asyncio
    async def test_data_optimizer_integration(self):
        """Test synthetic data optimizer standalone"""
        
        from src.optimization.synthetic_data_optimizer import SyntheticDataOptimizer
        
        optimizer = SyntheticDataOptimizer()
        
        dataset = [
            {"id": f"s{i}", "text": f"Text {i}"} for i in range(100)
        ]
        
        result = optimizer.optimize(
            dataset=dataset,
            target_compression=0.5,
            baseline_energy_kwh=1.0
        )
        
        assert result.optimized_size <= result.original_size
        assert result.estimated_energy_savings_kwh > 0
        
        print(f"✅ Data optimizer integration passed")


@pytest.mark.asyncio
async def test_end_to_end_scenario():
    """
    End-to-end scenario: Submit task → Complete workflow → Verify results
    """
    
    print("\n" + "="*80)
    print("🧪 RUNNING END-TO-END SCENARIO")
    print("="*80 + "\n")
    
    # Create agent
    agent = await create_unified_agent(num_ray_workers=2)
    
    # Set budget
    agent.ledger.set_team_budget("e2e_team", "2026-03", 10.0)
    
    # Define task
    task = {
        "task_id": "e2e_bert_sentiment",
        "model_name": "bert-base-uncased",
        "task_type": "fine_tuning",
        "dataset_name": "sst2",
        "dataset_size": 5_000,
        "num_epochs": 2,
        "batch_size": 16,
        "hardware": "V100",
        "team": "e2e_team",
        "priority": 0.8,
        "deferrable": True,
        "fine_tuning_method": "full_fine_tuning",
        "target_accuracy": 0.90
    }
    
    # Create dataset
    dataset = [
        {"id": f"sample_{i}", "text": f"Training sample {i}", "label": i % 2}
        for i in range(task["dataset_size"])
    ]
    
    # Execute
    result = await agent.execute(task, dataset=dataset)
    
    # Verify complete workflow
    print("📋 Workflow Steps Verified:")
    print(f"   ✅ Workload parsed: {result.workload_profile is not None}")
    print(f"   ✅ Decision made: {result.decision is not None}")
    print(f"   ✅ Data optimized: {result.data_optimization is not None}")
    print(f"   ✅ Task executed: {result.status == 'completed'}")
    print(f"   ✅ Benchmark recorded: {result.accuracy > 0}")
    print()
    
    # Verify metrics
    print("📊 Performance Metrics:")
    print(f"   Accuracy: {result.accuracy:.1%}")
    print(f"   Energy: {result.energy_kwh:.4f} kWh")
    print(f"   Carbon: {result.carbon_kgco2e:.4f} kgCO2e")
    print(f"   Savings: {result.carbon_savings_pct:.1f}%")
    print()
    
    # Assertions
    assert result.status == "completed"
    assert result.carbon_savings_pct > 70.0, "Should save >70% carbon"
    assert result.accuracy > 0.85, "Should maintain >85% accuracy"
    
    # Cleanup
    await agent.shutdown()
    
    print("="*80)
    print("✅ END-TO-END SCENARIO PASSED")
    print("="*80)


if __name__ == "__main__":
    # Run end-to-end test
    asyncio.run(test_end_to_end_scenario())
