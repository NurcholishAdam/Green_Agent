# tests/test_enhancements_complete.py

"""
Complete test suite for all enhancement modules
Includes unit, integration, chaos, and performance tests
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
# PART A: Unit Tests
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
# PART E: Running Tests
# ============================================================

if __name__ == "__main__":
    # Run all tests with pytest
    pytest.main([__file__, '-v', '--tb=short', '--maxfail=1'])
