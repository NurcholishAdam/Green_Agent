# File: tests/integration/test_unified_system.py

import pytest
import asyncio
import time
import json
from pathlib import Path
from typing import Dict, List
import numpy as np

# Import system components
from runtime.run_agent import GreenAgentRunner, ExecutionResult
from src.integration.unified_orchestrator import UnifiedGreenAgent
from quantum_integration.test_unified_system import GreenAgentQuantumSystem
from src.carbon.task_carbon_profiler import CarbonProfiler
from src.benchmarking.benchmark_intelligence import BenchmarkEngine

class TestComparisonMode:
    """Test suite for comparison mode functionality"""
    
    @pytest.fixture
    async def runner_compare(self):
        """Create runner in compare mode"""
        config = {
            'system': {'mode': 'compare'},
            'dashboard': {'enabled': False},
            'ray': {'enabled': False}
        }
        runner = GreenAgentRunner.__new__(GreenAgentRunner)
        runner.config = config
        runner.mode = 'compare'
        runner.unified_agent = None
        runner.legacy_components = {}
        runner.dashboard = None
        yield runner
    
    @pytest.mark.asyncio
    async def test_comparison_execution(self, runner_compare):
        """Test that comparison mode runs both unified and legacy"""
        # Mock task
        task = {
            'id': 'test_comparison_001',
            'type': 'ml_inference',
            'model_size': '100M',
            'input_size': '512KB',
            'priority': 5
        }
        
        # Mock execution methods
        async def mock_unified(task):
            await asyncio.sleep(0.1)
            return {
                'energy_consumed': 0.5,
                'carbon_emitted': 0.02,
                'accuracy': 0.92,
                'negawatt_reward': 0.85,
                'metrics': {}
            }
        
        async def mock_legacy(task):
            await asyncio.sleep(0.2)
            return {
                'energy_consumed': 1.5,
                'carbon_emitted': 0.06,
                'accuracy': 0.90,
                'negawatt_reward': 0.60,
                'metrics': {}
            }
        
        runner_compare._execute_unified = mock_unified
        runner_compare._execute_legacy = mock_legacy
        
        # Execute comparison
        result = await runner_compare._execute_comparison(task)
        
        # Assertions
        assert result.energy_consumed == 0.5
        assert result.carbon_emitted == 0.02
        
        # Check comparison file was created
        comparison_file = Path("results/comparison_results.json")
        assert comparison_file.exists()
        
        with open(comparison_file, 'r') as f:
            comparison = json.load(f)
        
        # Verify improvements
        improvements = comparison['improvements']
        assert improvements['energy_reduction_percent'] > 0
        assert improvements['carbon_reduction_percent'] > 0
        
        print(f"✅ Comparison test passed: {improvements['energy_reduction_percent']:.1f}% energy saved")
    
    @pytest.mark.asyncio
    async def test_comparison_generates_report(self, runner_compare):
        """Test that comparison generates proper report"""
        unified_result = {
            'energy_consumed': 0.3,
            'carbon_emitted': 0.012,
            'accuracy': 0.95,
            'negawatt_reward': 0.90
        }
        
        legacy_result = {
            'energy_consumed': 1.0,
            'carbon_emitted': 0.04,
            'accuracy': 0.93,
            'negawatt_reward': 0.65
        }
        
        comparison = runner_compare._generate_comparison(
            unified_result, legacy_result,
            unified_time=0.5, legacy_time=1.2
        )
        
        assert 'improvements' in comparison
        assert comparison['improvements']['energy_reduction_percent'] == 70.0
        assert comparison['improvements']['carbon_reduction_percent'] == 70.0
        assert 'verdict' in comparison
        
        print(f"📊 Generated comparison: {comparison['verdict']}")


class TestQuantumIntegration:
    """Test suite for quantum components"""
    
    @pytest.fixture
    async def quantum_system(self):
        """Create quantum system"""
        config = {
            'quantum': {
                'enabled': True,
                'backend': 'simulator',
                'error_mitigation': {
                    'enabled': True,
                    'techniques': ['zero_noise_extrapolation']
                }
            }
        }
        system = GreenAgentQuantumSystem(config)
        await system.initialize()
        yield system
        await system.shutdown()
    
    @pytest.mark.asyncio
    async def test_quantum_task_execution(self, quantum_system):
        """Test quantum-enhanced task execution"""
        task_data = {
            'features': [0.1, 0.2, 0.3, 0.4],
            'task_id': 'quantum_test_001'
        }
        
        result = await quantum_system.run_quantum_task(task_data)
        
        assert 'result' in result
        assert 'execution_time' in result
        assert 'energy_consumed_kwh' in result
        assert 'carbon_emitted_kg' in result
        assert result['error_mitigation_applied'] is True
        
        print(f"⚛️ Quantum task executed in {result['execution_time']:.4f}s")
    
    @pytest.mark.asyncio
    async def test_multi_agent_coordination(self, quantum_system):
        """Test multi-agent quantum coordination"""
        carbon_data = {
            'agent_0': 30,   # Green
            'agent_1': 150,  # Yellow
            'agent_2': 250,  # Red
            'agent_3': 45    # Green
        }
        
        result = await quantum_system.run_multi_agent_coordination(carbon_data)
        
        assert 'task_distribution' in result
        assert 'entanglement_fidelity' in result
        assert result['consensus_achieved'] is True
        
        # Verify carbon-aware distribution
        distribution = result['task_distribution']
        green_agents = [aid for aid, data in distribution.items() 
                       if data['role'] == 'primary_compute']
        
        assert len(green_agents) > 0
        print(f"🤝 Multi-agent coordination: {len(green_agents)} primary compute agents")
    
    @pytest.mark.asyncio
    async def test_quantum_efficiency_metrics(self, quantum_system):
        """Test quantum efficiency calculation"""
        # Run multiple tasks
        tasks = [
            {'features': np.random.rand(4).tolist(), 'task_id': f'task_{i}'}
            for i in range(10)
        ]
        
        results = []
        for task in tasks:
            result = await quantum_system.run_quantum_task(task)
            results.append(result)
        
        # Calculate average efficiency
        total_energy = sum(r['energy_consumed_kwh'] for r in results)
        total_time = sum(r['execution_time'] for r in results)
        
        metrics = quantum_system.get_system_metrics()
        
        assert metrics['total_energy'] > 0
        assert metrics['tasks_completed'] == 10
        assert metrics['efficiency_score'] > 0
        
        print(f"📈 Quantum efficiency: {metrics['efficiency_score']:.4f}")


class TestKubernetesDeployment:
    """Test Kubernetes deployment configurations"""
    
    def test_ray_cluster_yaml_valid(self):
        """Test that Ray cluster YAML is valid"""
        import yaml
        
        yaml_file = Path("k8s/ray-cluster.yaml")
        assert yaml_file.exists(), "Ray cluster YAML not found"
        
        with open(yaml_file, 'r') as f:
            docs = list(yaml.safe_load_all(f))
        
        # Should have multiple documents
        assert len(docs) >= 3, "Expected multiple YAML documents"
        
        # Find RayCluster spec
        ray_cluster = next(d for d in docs if d.get('kind') == 'RayCluster')
        
        assert 'spec' in ray_cluster
        assert 'headGroupSpec' in ray_cluster['spec']
        assert 'workerGroupSpecs' in ray_cluster['spec']
        
        print("✅ Ray cluster YAML is valid")
    
    def test_carbon_aware_autoscaling_configured(self):
        """Test carbon-aware autoscaling is configured"""
        import yaml
        
        yaml_file = Path("k8s/ray-cluster.yaml")
        with open(yaml_file, 'r') as f:
            content = yaml.safe_load(f)
        
        # Check autoscaler options
        autoscaler = content['spec'].get('autoscalerOptions', {})
        
        assert autoscaler, "Autoscaler options not found"
        assert autoscaler.get('upscalingMode') == 'Conservative'
        
        # Check carbon metrics
        metrics = autoscaler.get('metrics', [])
        carbon_metric = next((m for m in metrics 
                            if m.get('type') == 'External' and 
                               m.get('external', {}).get('metric', {}).get('name') == 'carbon_intensity'), 
                           None)
        
        assert carbon_metric is not None, "Carbon intensity metric not configured"
        
        print("✅ Carbon-aware autoscaling is configured")


class TestDashboardMonitoring:
    """Test dashboard and monitoring functionality"""
    
    @pytest.fixture
    async def dashboard_client(self):
        """Create test dashboard client"""
        from httpx import AsyncClient
        
        async with AsyncClient(base_url="http://localhost:8000") as client:
            yield client
    
    @pytest.mark.asyncio
    async def test_dashboard_health_endpoint(self, dashboard_client):
        """Test dashboard health endpoint"""
        response = await dashboard_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert 'status' in data
        assert data['status'] == 'healthy'
        
        print("✅ Dashboard health check passed")
    
    @pytest.mark.asyncio
    async def test_real_time_metrics_endpoint(self, dashboard_client):
        """Test real-time metrics endpoint"""
        response = await dashboard_client.get("/metrics/realtime")
        
        assert response.status_code == 200
        metrics = response.json()
        
        assert 'energy_consumption' in metrics
        assert 'carbon_footprint' in metrics
        assert 'active_tasks' in metrics
        assert 'pareto_frontier' in metrics
        
        print(f"📊 Real-time metrics: {metrics['active_tasks']} active tasks")
    
    @pytest.mark.asyncio
    async def test_pareto_frontier_visualization(self, dashboard_client):
        """Test Pareto frontier data endpoint"""
        response = await dashboard_client.get("/analytics/pareto")
        
        assert response.status_code == 200
        pareto_data = response.json()
        
        assert 'frontier_points' in pareto_data
        assert len(pareto_data['frontier_points']) > 0
        
        # Verify frontier points are valid
        for point in pareto_data['frontier_points']:
            assert 'accuracy' in point
            assert 'energy' in point
            assert point['accuracy'] >= 0 and point['accuracy'] <= 1
        
        print(f"📈 Pareto frontier: {len(pareto_data['frontier_points'])} optimal points")


class TestEndToEnd:
    """End-to-end integration tests"""
    
    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """Test complete workflow from task submission to results"""
        # 1. Initialize system
        config_path = "config/green_agent_config.yaml"
        runner = GreenAgentRunner(config_path)
        await runner.initialize()
        
        try:
            # 2. Submit task
            task = {
                'id': 'e2e_test_001',
                'type': 'ml_training',
                'model_size': '500M',
                'dataset_size': '10GB',
                'deadline': time.time() + 7200,
                'priority': 7
            }
            
            # 3. Execute
            result = await runner.execute_task(task)
            
            # 4. Verify results
            assert result.execution_time > 0
            assert result.energy_consumed > 0
            assert result.carbon_emitted >= 0
            assert result.accuracy > 0
            
            # 5. Check carbon savings
            if result.mode == 'unified':
                # Unified should be more efficient
                assert result.energy_consumed < 2.0  # kWh threshold
            
            print(f"✅ End-to-end test passed:")
            print(f"   Execution time: {result.execution_time:.2f}s")
            print(f"   Energy: {result.energy_consumed:.4f} kWh")
            print(f"   Carbon: {result.carbon_emitted:.4f} kg CO₂")
            print(f"   Accuracy: {result.accuracy:.4f}")
        
        finally:
            await runner.shutdown()


# Run tests
if __name__ == "__main__":
    pytest.main([
        __file__,
        "-v",
        "--asyncio-mode=auto",
        "-s",
        "-k"
    ])
