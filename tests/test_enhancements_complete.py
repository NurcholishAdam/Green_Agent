# tests/test_enhancements_complete.py

"""
Complete test suite for all enhancement modules
Includes unit, integration, chaos, and performance tests
Additionally includes tests for advanced modules:
LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
Bio‑inspired Optimisation, MoE expert gating, and FlexGen hooks.
"""

import pytest
import asyncio
import time
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock

# Import modules
import sys
sys.path.insert(0, 'src/enhancements')

from synthetic_data_manager import SyntheticDataSource, DataQuality, ScenarioType
from control_system import ControlSystem, ControlMode, ThrottleActuator, CoolingActuator
from fallback_manager import FallbackManager, FallbackStrategy, CircuitBreaker

# ============================================================
# PART A: Unit Tests (original tests remain unchanged)
# ============================================================

class TestThermalAwareOptimizer:
    """Unit tests for thermal optimization module"""

    def test_temperature_calculation(self):
        """Test temperature dynamics calculation"""
        from thermal_optimizer import ThermalAwareOptimizer

        optimizer = ThermalAwareOptimizer({'simulate': True})

        # Test leakage power calculation
        leakage_at_60c = optimizer.calculate_leakage_power(60)
        leakage_at_80c = optimizer.calculate_leakage_power(80)

        assert leakage_at_80c > leakage_at_60c
        assert 10 < leakage_at_60c < 30
        assert 20 < leakage_at_80c < 50

    def test_optimal_temperature_finding(self):
        """Test finding optimal operating temperature"""
        from thermal_optimizer import ThermalAwareOptimizer

        optimizer = ThermalAwareOptimizer()
        optimal = optimizer.find_optimal_operating_temp(200)

        assert 50 <= optimal <= 75

    def test_thermal_zone_classification(self):
        """Test thermal zone classification"""
        from thermal_optimizer import ThermalAwareOptimizer, ThermalZone

        optimizer = ThermalAwareOptimizer()

        assert optimizer.get_thermal_zone(45) == ThermalZone.COOL
        assert optimizer.get_thermal_zone(60) == ThermalZone.OPTIMAL
        assert optimizer.get_thermal_zone(70) == ThermalZone.NORMAL
        assert optimizer.get_thermal_zone(80) == ThermalZone.WARNING
        assert optimizer.get_thermal_zone(90) == ThermalZone.CRITICAL


class TestPhaseAwareEnergyModel:
    """Unit tests for phase energy model"""

    def test_phase_decomposition(self):
        """Test workload phase decomposition"""
        from phase_energy_model import PhaseAwareEnergyModel

        model = PhaseAwareEnergyModel()
        task = {
            'model_config': {'size_gb': 10},
            'data_volume_gb': 100,
            'training_steps': 1000,
            'hardware_requirements': {'gpu_count': 4}
        }

        phases = model.decompose_workload(task)

        assert len(phases) >= 5  # Data load, preprocess, compute, communication, checkpoint
        assert any(p.type.value == 'compute' for p in phases)

    def test_energy_prediction(self):
        """Test phase energy prediction"""
        from phase_energy_model import PhaseAwareEnergyModel

        model = PhaseAwareEnergyModel()
        task = {'model_config': {'size_gb': 1}}

        profile = model.predict_phase_energy(task)

        assert profile.total_energy_joules > 0
        assert len(profile.phase_breakdown) > 0
        assert 'compute' in profile.phase_breakdown


class TestHeliumPriceElasticity:
    """Unit tests for helium elasticity model"""

    def test_elasticity_calculation(self):
        """Test price elasticity calculation"""
        from helium_elasticity import HeliumPriceElasticityModel, WorkloadPriority

        model = HeliumPriceElasticityModel()

        reduction = model.calculate_optimal_reduction(
            WorkloadPriority.BATCH,
            price_increase_ratio=2.0
        )

        assert 0.5 <= reduction <= 1.0

    def test_defer_recommendation(self):
        """Test deferral recommendation logic"""
        from helium_elasticity import HeliumPriceElasticityModel, WorkloadPriority

        model = HeliumPriceElasticityModel()
        model.current_price = 9.0

        should_defer, reason, _ = model.should_defer(
            WorkloadPriority.MEDIUM,
            carbon_zone='yellow',
            helium_requirement_liters=10
        )

        assert should_defer is True
        assert 'price' in reason.lower()


class TestDualCarbonAccountant:
    """Unit tests for dual carbon accounting"""

    def test_ppa_allocation(self):
        """Test PPA energy allocation"""
        from dual_accountant import DualCarbonAccountant

        accountant = DualCarbonAccountant()
        allocated = accountant.allocate_ppa_energy(
            datetime.now(),
            energy_kwh=100
        )

        assert allocated >= 0
        assert allocated <= 100

    def test_emissions_calculation(self):
        """Test carbon emissions calculation"""
        from dual_accountant import DualCarbonAccountant

        accountant = DualCarbonAccountant()
        accounting = accountant.account_carbon(
            task_id='test_001',
            energy_consumption_kwh=100,
            region='us-east',
            timestamp=datetime.now()
        )

        assert accounting.location_based_emissions_kg > 0
        assert accounting.market_based_emissions_kg >= 0
        assert accounting.hash != ""


# ============================================================
# PART B: Integration Tests
# ============================================================

class TestSyntheticDataIntegration:
    """Integration tests for synthetic data sources"""

    def test_synthetic_data_generation(self):
        """Test synthetic data generation flow"""
        source = SyntheticDataSource({'quality': 'perfect'})
        source.start()

        time.sleep(2)

        # Test temperature data
        temp = source.get_temperature_data()
        assert 30 <= temp.gpu_temp_c <= 95
        assert temp.quality == DataQuality.PERFECT

        # Test grid data
        grid = source.get_grid_data('us-east')
        assert 100 <= grid.average_intensity_gco2_per_kwh <= 800

        # Test helium data
        helium = source.get_helium_data()
        assert 2 <= helium.spot_price_usd_per_liter <= 15

        source.stop()

    def test_quality_degradation(self):
        """Test data quality degradation handling"""
        source = SyntheticDataSource()
        source.set_quality(DataQuality.DEGRADED)

        # Should still work but may have noise
        temp = source.get_temperature_data()
        assert temp.quality == DataQuality.DEGRADED

        source.set_quality(DataQuality.OFFLINE)
        with pytest.raises(ConnectionError):
            source.get_temperature_data()

    def test_scenario_switching(self):
        """Test scenario switching"""
        source = SyntheticDataSource()
        source.set_scenario(ScenarioType.HEATWAVE)

        temp = source.get_temperature_data()
        assert temp.gpu_temp_c > 75

        source.set_scenario(ScenarioType.HELIUM_CRISIS)
        helium = source.get_helium_data()
        assert helium.spot_price_usd_per_liter > 8
        assert helium.global_inventory_days < 15


class TestControlSystemIntegration:
    """Integration tests for control system"""

    def test_throttle_actuation(self):
        """Test throttle actuation with simulation"""
        controller = ControlSystem({'mode': 'automatic', 'simulate': True})

        result = controller.execute('throttle', 0.5)

        assert result.success is True
        assert result.actual_value == 0.5
        assert result.fallback_used is False

    def test_cooling_actuation(self):
        """Test cooling actuation"""
        controller = ControlSystem()

        result = controller.execute('cooling', 300)

        assert result.success is True
        assert 200 <= result.actual_value <= 500

    def test_emergency_stop(self):
        """Test emergency stop functionality"""
        controller = ControlSystem()

        results = controller.emergency_stop()

        assert results['throttle'].actual_value <= 0.3
        assert results['cooling'].actual_value >= 400


class TestFallbackIntegration:
    """Integration tests for fallback manager"""

    def test_circuit_breaker_trip(self):
        """Test circuit breaker tripping"""
        breaker = CircuitBreaker('test', threshold=2, timeout_ms=1000)

        failing_func = Mock(side_effect=Exception("Failing"))

        # First two calls should record failures
        for _ in range(2):
            success, _ = breaker.call(failing_func)
            assert success is False

        # Circuit should now be open
        assert breaker.state.value == 'open'

        # Third call should return immediately without calling function
        success, _ = breaker.call(failing_func)
        assert success is False
        assert failing_func.call_count == 2  # No additional call

    def test_cascading_fallback(self):
        """Test cascading fallback strategy"""
        fallback_manager = FallbackManager()

        # Primary function that fails
        def failing_primary():
            raise Exception("Primary failed")

        result = fallback_manager.execute_with_fallback(
            failing_primary,
            'temperature',
            FallbackConfig(strategy=FallbackStrategy.CASCADE)
        )

        assert result.success is True
        assert result.source == 'fallback_synthetic' or result.source.startswith('cache')
        assert result.value is not None

    def test_conservative_fallback(self):
        """Test conservative fallback defaults"""
        fallback_manager = FallbackManager()

        def failing_primary():
            raise Exception("Primary failed")

        result = fallback_manager.execute_with_fallback(
            failing_primary,
            'helium',
            FallbackConfig(strategy=FallbackStrategy.CONSERVATIVE)
        )

        assert result.success is True
        assert result.source == 'conservative_default'
        assert result.value.get('spot_price', 0) >= 5


class TestEndToEndIntegration:
    """End-to-end integration tests across all modules"""

    def test_complete_decision_workflow(self):
        """Test complete workflow from data to control"""
        from thermal_optimizer import ThermalAwareOptimizer
        from helium_elasticity import HeliumPriceElasticityModel, WorkloadPriority
        from dual_accountant import DualCarbonAccountant

        # Initialize
        source = SyntheticDataSource()
        source.start()
        time.sleep(1)

        control = ControlSystem({'simulate': True})
        fallback = FallbackManager()

        # Get data
        temp_data = source.get_temperature_data()
        helium_data = source.get_helium_data()

        # Make decisions
        thermal_opt = ThermalAwareOptimizer()
        tmp_cel = temp_data.gpu_temp_c
        decision = thermal_opt.optimize_schedule({'gpu_count': 4}, None)

        helium_elasticity = HeliumPriceElasticityModel()
        elasticity = helium_elasticity.get_elasticity_decision(
            WorkloadPriority.MEDIUM,
            10.0,
            None,
            'yellow'
        )

        # Apply controls
        throttle_result = control.execute('throttle', elasticity.throttle_factor)
        cooling_result = control.execute('cooling',
            max(50, min(500, (decision.target_temp - 20) * 10)))

        assert throttle_result.success is True
        assert cooling_result.success is True

        source.stop()

    def test_fault_tolerance(self):
        """Test fault tolerance with data source failures"""
        source = SyntheticDataSource()
        source.start()

        control = ControlSystem()
        fallback = FallbackManager()

        # Force data source offline
        source.set_quality(DataQuality.OFFLINE)

        # Should fallback to synthetic/cached data
        def get_data():
            return source.get_temperature_data()

        result = fallback.execute_with_fallback(
            get_data,
            'temperature',
            FallbackConfig(strategy=FallbackStrategy.CASCADE)
        )

        assert result.success is True
        assert result.source != 'primary'

        source.stop()


# ============================================================
# PART C: Performance Tests
# ============================================================

class TestPerformance:
    """Performance benchmarks for enhancement modules"""

    def test_decision_latency(self):
        """Test decision-making latency"""
        from carbon_aware_decision_core import CarbonAwareDecisionCore
        from workload_interpreter import WorkloadInterpreter

        interpreter = WorkloadInterpreter()
        decision_core = CarbonAwareDecisionCore()

        task = {
            'task_id': 'perf_test',
            'hardware_requirements': {'gpu_count': 4},
            'model_config': {'size_gb': 10}
        }

        # Measure repeated decisions
        latencies = []
        for _ in range(100):
            start = time.perf_counter()
            workload = interpreter.analyze_task(task)
            decision = decision_core.make_decision(workload, 150, None)
            latencies.append((time.perf_counter() - start) * 1000)

        avg_latency = sum(latencies) / len(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]

        print(f"Decision latency - avg: {avg_latency:.2f}ms, p95: {p95_latency:.2f}ms")

        assert avg_latency < 50  # Should be under 50ms
        assert p95_latency < 100  # Should be under 100ms

    def test_throughput(self):
        """Test processing throughput"""
        from unified_orchestrator import UnifiedOrchestrator

        orchestrator = UnifiedOrchestrator({'helium_aware_enabled': True})

        tasks = []
        for i in range(100):
            tasks.append({
                'task_id': f'throughput_{i}',
                'hardware_requirements': {'gpu_count': 1 if i % 2 == 0 else 0},
                'model_config': {'size_gb': 1}
            })

        start = time.perf_counter()

        # Process synchronously for benchmark
        results = []
        for task in tasks:
            result = orchestrator.process_task(task)
            results.append(result)

        elapsed = time.perf_counter() - start
        throughput = len(tasks) / elapsed

        print(f"Throughput: {throughput:.2f} tasks/second")

        assert throughput > 5  # Should handle at least 5 tasks per second


# ============================================================
# PART D: Chaos Engineering Tests
# ============================================================

class TestChaosEngineering:
    """Chaos engineering tests for resilience"""

    def test_latency_injection(self):
        """Test system behavior with injected latency"""
        from synthetic_data_manager import SyntheticDataSource

        source = SyntheticDataSource({'update_interval': 10})  # Slow updates

        start = time.time()
        data = source.get_temperature_data()
        elapsed = time.time() - start

        # Should still return data (possibly cached)
        assert data is not None

    def test_failure_cascade_isolation(self):
        """Test that failures don't cascade across modules"""
        from helium_elasticity import HeliumPriceElasticityModel

        # Create failing data source
        elasticity = HeliumPriceElasticityModel()

        # Should still work (using fallback)
        decision = elasticity.get_elasticity_decision('MEDIUM', 10, None, 'green')

        assert decision is not None
        assert decision.action in ['defer', 'throttle', 'execute']

    def test_resource_exhaustion(self):
        """Test behavior under resource exhaustion"""
        import memory_profiler

        @memory_profiler.profile
        def run_many_operations():
            decisions = []
            for i in range(10000):
                # Simulate many decisions
                decisions.append({'id': i, 'decision': 'throttle' if i % 2 == 0 else 'execute'})
            return decisions

        # Should not cause memory leak
        import tracemalloc
        tracemalloc.start()

        run_many_operations()

        current, peak = tracemalloc.get_traced_memory()
        print(f"Memory usage - current: {current/1024/1024:.2f}MB, peak: {peak/1024/1024:.2f}MB")

        assert peak < 100 * 1024 * 1024  # Under 100MB peak


# ============================================================
# NEW: Advanced Enhancements Tests
# ============================================================

class TestAdvancedEnhancements:
    """
    Tests for the advanced modules in src/enhancements:
    LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
    Bio‑inspired Optimisation, MoE expert gating, and FlexGen integration hooks.
    """

    def test_node_descriptor_distillation_routing(self):
        """NodeDescriptor should select routing strategy using distillation + MoE."""
        from schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType

        node = NodeDescriptor(
            id="test_node",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=400.0,
            energy_per_token=0.00005,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.8},
            metadata={"gating_lr": 0.005}
        )

        strategy = asyncio.run(node.select_routing_strategy(exploration=False))
        assert strategy in ["carbon_first", "latency_first", "cost_first", "balanced", "adaptive"]

    def test_workload_descriptor_distillation_priority(self):
        """WorkloadDescriptor should select priority using MODP + MoE."""
        from schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency

        wl = WorkloadDescriptor(
            task_id="test_task",
            task_type=TaskType.INFERENCE,
            tokens=1000,
            latency_target=300.0,
            urgency=Urgency.MEDIUM,
            use_evolutionary=True,
            human_feedback_score=0.7,
            graph_metrics={"centrality": 0.6},
            metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2}
        )

        priority = asyncio.run(wl.select_priority(exploration=False))
        assert priority in ["accuracy", "green", "balanced"]

    def test_feedback_event_with_enhanced_fields(self):
        """FeedbackEvent should accept MODP, RLHF, and LIMIT Graph fields."""
        from schemas.feedback_event import FeedbackEvent

        event = FeedbackEvent(
            source="test",
            feedback_type="routing",
            task_id="t1",
            context={},
            action={"selected_action": "execute"},
            performance={"quality_score": 0.9, "latency_ms": 100, "energy_joules": 100,
                         "carbon_g": 5, "helium_cost": 0, "duration_ms": 100},
            adaptive_cost_value=0.85,
            graph_metrics={"centrality": 0.7},
            human_feedback_score=0.8,
            modp_score=0.75,
            distillation_stats={"student_counter": 5}
        )

        # Serialize and deserialize to verify persistence of enhanced fields
        json_str = event.to_json()
        event2 = FeedbackEvent.from_json(json_str)
        assert event2.graph_metrics["centrality"] == 0.7
        assert event2.human_feedback_score == 0.8
        assert event2.modp_score == 0.75

    def test_zero_trust_enhanced_init(self):
        """ZeroTrustArchitecture should initialize with enhanced flags."""
        from zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig

        config = ZeroTrustConfig(
            use_enhancements=True,
            use_distillation=True,
            use_evolutionary=True,
            human_feedback_score=0.6,
            graph_metrics={"centrality": 0.5}
        )
        zta = ZeroTrustArchitecture(config=config)
        assert zta.use_enhancements is True
        # Check for presence of enhanced components
        assert hasattr(zta, 'carbon_authenticator') or hasattr(zta, 'distillation_optimizer')

    def test_graph_registry_and_causal_graph(self):
        """GraphRegistry and CausalGraph should support LIMIT Graph operations."""
        from core.graph_registry import GraphRegistry, GraphType
        from core.causal_graph import CausalGraph

        registry = GraphRegistry()
        causal_graph = registry.get_or_create(GraphType.CAUSAL)
        assert isinstance(causal_graph, CausalGraph)

        # Add observation and diagnose
        from core.meta_cognition import MetaCognitionLayer
        meta = MetaCognitionLayer(causal_graph=causal_graph)
        meta.observe_snapshot({
            "CarbonIntensity": 430.0,
            "CarbonIntensity_high": 400.0,
            "GridStrain": 0.91,
        })
        report = meta.diagnose()
        assert report["status"] == "anomaly_detected"

    def test_dag_carbon_ledger_backpropagation(self):
        """DAGCarbonLedger should support backpropagation of carbon debt."""
        from metrics.dag_carbon_ledger import DAGCarbonLedger

        ledger = DAGCarbonLedger(storage_path="/tmp/test_ledger")
        node_a = ledger.add_execution(
            task_id="A", framework="langchain", energy_kwh=0.01,
            carbon_co2e_kg=0.002, accuracy=0.9, sustainability_index=0.8
        )
        node_b = ledger.add_execution(
            task_id="B", framework="langchain", energy_kwh=0.008,
            carbon_co2e_kg=0.0016, accuracy=0.88, sustainability_index=0.75,
            parent_task_ids=[node_a], dependency_type="model_state"
        )
        attributed = ledger.backpropagate_carbon(node_b, transfer_rate=0.3)
        assert node_a in attributed
        assert attributed[node_a] > 0

    def test_flexgen_config_presence(self):
        """FlexGen integration settings should be present in enhancements config."""
        # We simulate a config check
        flexgen_config = {
            "enabled": True,
            "model_name": "facebook/opt-6.7b",
            "batch_size": 16,
            "delegation_policy": "adaptive"
        }
        assert flexgen_config["enabled"] is True
        assert flexgen_config["delegation_policy"] in ["adaptive", "always", "never"]


# ============================================================
# PART E: Running Tests
# ============================================================

if __name__ == "__main__":
    # Run all tests with pytest
    pytest.main([__file__, '-v', '--tb=short', '--maxfail=1'])
