# File: src/enhancements/tests/test_helium_integration.py

"""
Enhanced Pytest Test Suite for Helium Integration - Version 6.1

Tests all helium modules with proper fixtures, mocks, and coverage.
"""

import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import tempfile

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def sample_helium_data():
    """Sample helium market data for testing"""
    return {
        'date': '2025-01-01',
        'global_production_tonnes': 28500.0,
        'global_demand_tonnes': 29500.0,
        'price_index': 150.0,
        'shortage_severity_0_1': 0.9,
        'supply_risk_score_0_1': 0.8,
        'recycling_rate_0_1': 0.20,
        'substitution_feasibility_0_1': 0.18,
        'cooling_load_sensitivity': 1.05,
        'geopolitical_risk_index': 0.60,
        'logistics_disruption_index': 0.50
    }

@pytest.fixture
def sample_helium_csv(sample_helium_data, tmp_path):
    """Create temporary helium CSV file"""
    csv_path = tmp_path / "helium_timeseries.csv"
    
    import csv
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=sample_helium_data.keys())
        writer.writeheader()
        writer.writerow(sample_helium_data)
    
    return csv_path

@pytest.fixture
def mock_helium_collector(sample_helium_data):
    """Mock HeliumDataCollector for isolated testing"""
    with patch('helium_data_collector.HeliumDataCollector') as mock:
        collector_instance = Mock()
        collector_instance.get_latest.return_value = Mock(
            to_dict=lambda: sample_helium_data,
            price_index=150.0,
            scarcity_index=0.75,
            recycling_rate_0_1=0.20,
            substitution_feasibility_0_1=0.18,
            demand_supply_ratio=1.035,
            shortage_severity_0_1=0.9,
            supply_risk_score_0_1=0.8,
            cooling_load_sensitivity=1.05,
            geothermal_risk_index=0.60,
            logistics_disruption_index=0.50,
            circularity_potential=0.19,
            thermal_impact_factor=0.79,
            to_feature_vector=lambda: np.array([0.95, 1.035, 0.75, 0.9, 0.8, 0.20, 0.18, 1.05, 0.60, 0.50])
        )
        collector_instance.get_trends.return_value = {
            'production_change_pct': 10.0,
            'price_change_pct': 50.0,
            'scarcity_trend': 'increasing'
        }
        collector_instance.dataset = Mock(timeseries_length=10)
        mock.return_value = collector_instance
        yield collector_instance

@pytest.fixture
def elasticity_config():
    """Elasticity configuration for testing"""
    from helium_elasticity import ElasticityConfig
    return ElasticityConfig(
        enable_data_collector=False,  # Use mock
        enable_regret_integration=True,
        enable_thermal_integration=True,
        enable_sustainability_integration=True,
        enable_synthetic_integration=True
    )

@pytest.fixture
def circularity_config():
    """Circularity configuration for testing"""
    from helium_circularity import CircularityConfig
    return CircularityConfig(
        enable_data_collector=False,
        enable_elasticity_integration=False,
        enable_sustainability_integration=True,
        enable_regret_integration=True,
        enable_thermal_integration=True,
        enable_synthetic_integration=True
    )

# ============================================================
# TEST DATA COLLECTOR
# ============================================================

class TestHeliumDataCollector:
    """Tests for helium_data_collector.py"""
    
    def test_collector_initialization(self, sample_helium_csv):
        """Test collector initializes correctly with CSV"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        
        assert collector is not None
        assert collector.dataset is not None
        assert collector.dataset.timeseries_length > 0
    
    def test_collector_fallback_to_synthetic(self):
        """Test collector generates synthetic data when CSV missing"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=Path("/nonexistent/helium.csv"))
        
        assert collector is not None
        assert collector.dataset is not None
        assert collector.dataset.timeseries_length > 0
        assert collector.dataset.metadata['source'] == 'synthetic'
    
    def test_get_latest_record(self, sample_helium_csv):
        """Test getting latest helium record"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        latest = collector.get_latest()
        
        assert latest is not None
        assert latest.price_index > 0
        assert 0 <= latest.scarcity_index <= 1
        assert 0 <= latest.recycling_rate_0_1 <= 1
    
    def test_feature_vector_dimensions(self, sample_helium_csv):
        """Test feature vector has correct dimensions"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        features = collector.get_feature_vector()
        
        assert len(features) == 10
        assert np.all(features >= 0)
    
    def test_export_for_regret_optimizer(self, sample_helium_csv):
        """Test export format for regret optimizer"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        export = collector.export_for_regret_optimizer()
        
        assert 'helium_price_index' in export
        assert 'helium_scarcity_index' in export
        assert 'helium_supply_risk' in export
    
    def test_export_for_sustainability_signals(self, sample_helium_csv):
        """Test export format for sustainability signals"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        export = collector.export_for_sustainability_signals()
        
        assert 'helium_scarcity_signal' in export
        assert 'helium_circularity_signal' in export
        assert 'helium_thermal_signal' in export
    
    def test_derived_properties(self, sample_helium_csv):
        """Test derived properties are calculated correctly"""
        from helium_data_collector import HeliumDataCollector
        
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        latest = collector.get_latest()
        
        assert latest.demand_supply_ratio > 0
        assert 0 <= latest.scarcity_index <= 1
        assert 0 <= latest.circularity_potential <= 1
        assert latest.thermal_impact_factor >= 0

# ============================================================
# TEST ELASTICITY CALCULATOR
# ============================================================

class TestHeliumElasticity:
    """Tests for helium_elasticity.py"""
    
    def test_calculator_initialization(self, elasticity_config):
        """Test elasticity calculator initializes"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        assert calculator is not None
    
    def test_price_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test price elasticity is in valid range"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_price_elasticity(sample_helium_data)
        
        assert -0.8 <= elasticity <= -0.1
    
    def test_scarcity_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test scarcity elasticity is in valid range"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_scarcity_elasticity(sample_helium_data)
        
        assert 0 <= elasticity <= 1
    
    def test_cross_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test cross elasticity is in valid range"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_cross_elasticity(sample_helium_data)
        
        assert 0 <= elasticity <= 1
    
    def test_thermal_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test thermal elasticity is in valid range"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_thermal_elasticity(sample_helium_data)
        
        assert 0 <= elasticity <= 1
    
    def test_comprehensive_elasticity(self, elasticity_config, sample_helium_data):
        """Test comprehensive elasticity calculation"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        metrics = calculator.calculate_comprehensive_elasticity(sample_helium_data)
        
        assert metrics is not None
        assert 0 <= metrics.composite_elasticity <= 1
        assert metrics.migration_recommendation in [
            'stay_local', 'consider_migration', 'migrate_soon', 'migrate_immediately'
        ]
        assert 0 <= metrics.efficiency_target <= 1
    
    def test_migration_recommendation_logic(self, elasticity_config):
        """Test migration recommendation logic"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        
        # Low scarcity should recommend staying local
        low_scarcity_data = {
            'scarcity_index': 0.2,
            'shortage_severity_0_1': 0.1,
            'supply_risk_score_0_1': 0.2,
            'demand_supply_ratio': 0.95,
            'price_index': 90,
            'substitution_feasibility_0_1': 0.3,
            'recycling_rate_0_1': 0.3,
            'cooling_load_sensitivity': 0.8,
            'geopolitical_risk_index': 0.2,
            'logistics_disruption_index': 0.1
        }
        
        metrics = calculator.calculate_comprehensive_elasticity(low_scarcity_data)
        assert metrics.migration_recommendation == 'stay_local'
        
        # High scarcity should recommend migration
        high_scarcity_data = {
            'scarcity_index': 0.9,
            'shortage_severity_0_1': 0.95,
            'supply_risk_score_0_1': 0.9,
            'demand_supply_ratio': 1.2,
            'price_index': 250,
            'substitution_feasibility_0_1': 0.05,
            'recycling_rate_0_1': 0.05,
            'cooling_load_sensitivity': 1.2,
            'geopolitical_risk_index': 0.9,
            'logistics_disruption_index': 0.8
        }
        
        metrics = calculator.calculate_comprehensive_elasticity(high_scarcity_data)
        assert metrics.migration_recommendation in ['migrate_soon', 'migrate_immediately']
    
    def test_regret_optimizer_export(self, elasticity_config, sample_helium_data):
        """Test regret optimizer export format"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_regret_optimizer()
        
        assert 'decision_weights' in export
        assert 'scenario_modifiers' in export
        assert 'recommendations' in export
    
    def test_thermal_optimizer_export(self, elasticity_config, sample_helium_data):
        """Test thermal optimizer export format"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_thermal_optimizer()
        
        assert 'thermal_params' in export or 'cooling_recommendations' in export
    
    def test_sustainability_signals_export(self, elasticity_config, sample_helium_data):
        """Test sustainability signals export format"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_sustainability_signals()
        
        assert 'sustainability_signals' in export or 'esg_impact' in export
    
    def test_full_export_structure(self, elasticity_config, sample_helium_data):
        """Test full export has all required sections"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_all()
        
        assert 'regret_optimizer' in export
        assert 'thermal_optimizer' in export
        assert 'sustainability_signals' in export
        assert 'synthetic_manager' in export
        assert 'metadata' in export

# ============================================================
# TEST CIRCULARITY CALCULATOR
# ============================================================

class TestHeliumCircularity:
    """Tests for helium_circularity.py"""
    
    def test_calculator_initialization(self, circularity_config):
        """Test circularity calculator initializes"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        assert calculator is not None
    
    def test_recovery_efficiency_range(self, circularity_config, sample_helium_data):
        """Test recovery efficiency is in valid range"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        efficiency = calculator.calculate_recovery_efficiency(sample_helium_data)
        
        assert 0 <= efficiency <= 1
    
    def test_recycling_rate_range(self, circularity_config, sample_helium_data):
        """Test recycling rate is in valid range"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        rate = calculator.calculate_recycling_rate(sample_helium_data)
        
        assert 0 <= rate <= 1
    
    def test_substitution_potential_range(self, circularity_config, sample_helium_data):
        """Test substitution potential is in valid range"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        potential = calculator.calculate_substitution_potential(sample_helium_data)
        
        assert 0 <= potential <= 1
    
    def test_mci_calculation(self, circularity_config):
        """Test Material Circularity Indicator calculation"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        
        # Perfect circularity
        mci_perfect = calculator.calculate_material_circularity_indicator(1.0, 1.0, 0.0)
        assert mci_perfect == 1.0
        
        # Linear economy
        mci_linear = calculator.calculate_material_circularity_indicator(0.0, 0.0, 1.0)
        assert mci_linear == 0.0
        
        # Mixed
        mci_mixed = calculator.calculate_material_circularity_indicator(0.5, 0.5, 0.5)
        assert 0 < mci_mixed < 1
    
    def test_stage_efficiencies(self, circularity_config):
        """Test stage efficiency calculation"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        stages = calculator.calculate_stage_efficiencies()
        
        assert 'stages' in stages
        assert len(stages['stages']) == 4
        assert 'bottleneck' in stages
        assert stages['overall_throughput'] > 0
    
    def test_comprehensive_circularity(self, circularity_config, sample_helium_data):
        """Test comprehensive circularity calculation"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        metrics = calculator.calculate_comprehensive_circularity(sample_helium_data)
        
        assert metrics is not None
        assert 0 <= metrics.circularity_index <= 1
        assert 0 <= metrics.material_circularity_indicator <= 1
        assert 0 <= metrics.closed_loop_score <= 1
        assert metrics.circularity_level in [
            'highly_circular', 'circular', 'transitioning', 'mostly_linear', 'linear'
        ]
        assert metrics.certification_level in [
            'platinum', 'gold', 'silver', 'bronze', 'uncertified'
        ]
    
    def test_cost_analysis(self, circularity_config):
        """Test recovery cost analysis"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        costs = calculator.calculate_recovery_costs(10000)
        
        assert costs['total_cost'] > 0
        assert costs['total_energy_kwh'] > 0
        assert costs['carbon_footprint_kg'] > 0
        assert costs['cost_per_liter'] > 0
    
    def test_sustainability_export(self, circularity_config, sample_helium_data):
        """Test sustainability signals export format"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_for_sustainability_signals()
        
        assert 'circularity_metrics' in export
        assert 'sustainability_signals' in export
        assert 'material_flows' in export
    
    def test_regret_optimizer_export(self, circularity_config, sample_helium_data):
        """Test regret optimizer export format"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_for_regret_optimizer()
        
        assert 'decision_weights' in export
        assert 'scenario_modifiers' in export
    
    def test_full_export_structure(self, circularity_config, sample_helium_data):
        """Test full export has all required sections"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_all()
        
        assert 'sustainability_signals' in export
        assert 'regret_optimizer' in export
        assert 'thermal_optimizer' in export
        assert 'synthetic_manager' in export
        assert 'cost_analysis' in export
        assert 'metadata' in export

# ============================================================
# TEST CROSS-MODULE INTEGRATION
# ============================================================

class TestCrossModuleIntegration:
    """Tests for cross-module data flow"""
    
    def test_data_collector_to_elasticity(self, sample_helium_data):
        """Test data flow from collector to elasticity"""
        from helium_elasticity import HeliumElasticityCalculator, ElasticityConfig
        
        config = ElasticityConfig(enable_data_collector=False)
        calculator = HeliumElasticityCalculator(config)
        
        # Test with sample data directly
        metrics = calculator.calculate_comprehensive_elasticity(sample_helium_data)
        
        assert metrics is not None
        assert metrics.price_elasticity < 0  # Price elasticity should be negative
    
    def test_data_collector_to_circularity(self, sample_helium_data):
        """Test data flow from collector to circularity"""
        from helium_circularity import HeliumCircularityCalculator, CircularityConfig
        
        config = CircularityConfig(enable_data_collector=False)
        calculator = HeliumCircularityCalculator(config)
        
        # Test with sample data directly
        metrics = calculator.calculate_comprehensive_circularity(sample_helium_data)
        
        assert metrics is not None
        assert metrics.recycling_rate > 0
    
    def test_elasticity_circularity_independence(self, elasticity_config, circularity_config, sample_helium_data):
        """Test that elasticity and circularity can run independently"""
        from helium_elasticity import HeliumElasticityCalculator
        from helium_circularity import HeliumCircularityCalculator
        
        elasticity_calc = HeliumElasticityCalculator(elasticity_config)
        circularity_calc = HeliumCircularityCalculator(circularity_config)
        
        elasticity_metrics = elasticity_calc.calculate_comprehensive_elasticity(sample_helium_data)
        circularity_metrics = circularity_calc.calculate_comprehensive_circularity(sample_helium_data)
        
        # Both should produce valid results independently
        assert elasticity_metrics is not None
        assert circularity_metrics is not None
        
        # They should produce different types of metrics
        assert hasattr(elasticity_metrics, 'price_elasticity')
        assert hasattr(circularity_metrics, 'recycling_rate')

# ============================================================
# TEST EDGE CASES
# ============================================================

class TestEdgeCases:
    """Tests for edge cases and error handling"""
    
    def test_empty_data_handling(self, elasticity_config):
        """Test handling of empty/missing data"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        
        # Should handle empty dict gracefully
        metrics = calculator.calculate_comprehensive_elasticity({})
        assert metrics is not None
    
    def test_extreme_values(self, elasticity_config):
        """Test handling of extreme values"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        
        extreme_data = {
            'scarcity_index': 1.0,
            'shortage_severity_0_1': 1.0,
            'supply_risk_score_0_1': 1.0,
            'demand_supply_ratio': 10.0,
            'price_index': 1000,
            'substitution_feasibility_0_1': 0.0,
            'recycling_rate_0_1': 0.0,
            'cooling_load_sensitivity': 2.0,
            'geopolitical_risk_index': 1.0,
            'logistics_disruption_index': 1.0
        }
        
        metrics = calculator.calculate_comprehensive_elasticity(extreme_data)
        
        # All values should be clamped to valid ranges
        assert 0 <= metrics.scarcity_elasticity <= 1
        assert 0 <= metrics.thermal_elasticity <= 1
        assert 0 <= metrics.composite_elasticity <= 1
    
    def test_negative_values_handling(self, circularity_config):
        """Test handling of negative values"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        
        # MCI with negative inputs should still produce valid output
        mci = calculator.calculate_material_circularity_indicator(-1.0, -1.0, -1.0)
        assert 0 <= mci <= 1
    
    def test_zero_division_safety(self, circularity_config):
        """Test safety against division by zero"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        
        # Cost analysis with zero volume should not crash
        costs = calculator.calculate_recovery_costs(0)
        assert costs is not None

# ============================================================
# TEST PERFORMANCE
# ============================================================

class TestPerformance:
    """Performance benchmarks for helium modules"""
    
    @pytest.mark.benchmark(min_rounds=5)
    def test_elasticity_calculation_speed(self, elasticity_config, sample_helium_data, benchmark):
        """Benchmark elasticity calculation speed"""
        from helium_elasticity import HeliumElasticityCalculator
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        
        result = benchmark(
            calculator.calculate_comprehensive_elasticity,
            sample_helium_data
        )
        
        assert result is not None
    
    @pytest.mark.benchmark(min_rounds=5)
    def test_circularity_calculation_speed(self, circularity_config, sample_helium_data, benchmark):
        """Benchmark circularity calculation speed"""
        from helium_circularity import HeliumCircularityCalculator
        
        calculator = HeliumCircularityCalculator(circularity_config)
        
        result = benchmark(
            calculator.calculate_comprehensive_circularity,
            sample_helium_data
        )
        
        assert result is not None
    
    def test_bulk_calculations(self, elasticity_config, sample_helium_data):
        """Test performance with bulk calculations"""
        from helium_elasticity import HeliumElasticityCalculator
        import time
        
        calculator = HeliumElasticityCalculator(elasticity_config)
        
        start = time.time()
        for _ in range(100):
            calculator.calculate_comprehensive_elasticity(sample_helium_data)
        elapsed = time.time() - start
        
        # 100 calculations should complete in under 2 seconds
        assert elapsed < 2.0

# ============================================================
# RUNNER
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
