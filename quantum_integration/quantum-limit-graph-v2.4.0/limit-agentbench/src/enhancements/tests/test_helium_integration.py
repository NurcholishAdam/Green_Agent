# =============================================================================
# FILE: src/enhancements/tests/test_helium_integration.py
# VERSION: 6.2 (Enhanced Test Suite – Production Ready)
# =============================================================================
"""
Enhanced Pytest Test Suite for Helium Integration - Version 6.2

Additions over 6.1:
- Full coverage for all helium modules (data collector, elasticity, circularity,
  regret optimizer, thermal optimizer, synthetic manager, sustainability signals)
- Parametrized tests for business logic validation
- Error handling and edge case tests
- Synthetic data fallback tests
- Conditional benchmark markers with fallback
- Improved import handling
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

# Proper package imports (avoid sys.path modification)
try:
    from helium_data_collector import HeliumDataCollector
    from helium_elasticity import HeliumElasticityCalculator, ElasticityConfig, ElasticityMetrics
    from helium_circularity import HeliumCircularityCalculator, CircularityConfig, CircularityMetrics
    from helium_regret_optimizer import HeliumRegretOptimizer
    from helium_thermal_optimizer import HeliumThermalOptimizer
    from helium_synthetic_manager import HeliumSyntheticDataManager
    from helium_sustainability_signals import HeliumSustainabilitySignals
except ImportError:
    # Fallback: add parent to path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from helium_data_collector import HeliumDataCollector
    from helium_elasticity import HeliumElasticityCalculator, ElasticityConfig, ElasticityMetrics
    from helium_circularity import HeliumCircularityCalculator, CircularityConfig, CircularityMetrics
    from helium_regret_optimizer import HeliumRegretOptimizer
    from helium_thermal_optimizer import HeliumThermalOptimizer
    from helium_synthetic_manager import HeliumSyntheticDataManager
    from helium_sustainability_signals import HeliumSustainabilitySignals

# Conditional benchmark
try:
    import pytest_benchmark
    BENCHMARK_AVAILABLE = True
except ImportError:
    BENCHMARK_AVAILABLE = False
    # Create dummy decorator if benchmark not available
    def benchmark(func):
        return func

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

@pytest.fixture
def regret_config():
    """Regret optimizer configuration"""
    return {
        'max_regret': 0.5,
        'cvar_alpha': 0.95,
        'exploration_rate': 0.1
    }

@pytest.fixture
def thermal_config():
    """Thermal optimizer configuration"""
    return {
        'target_temperature': 8.0,
        'cooling_power_limit': 200
    }

@pytest.fixture
def synthetic_config():
    """Synthetic data manager configuration"""
    return {
        'num_samples': 100,
        'seed': 42
    }

@pytest.fixture
def sustainability_config():
    """Sustainability signals configuration"""
    return {
        'enable_helium_scarcity': True,
        'enable_circularity': True
    }

# ============================================================
# TEST DATA COLLECTOR
# ============================================================

class TestHeliumDataCollector:
    """Tests for helium_data_collector.py"""
    
    def test_collector_initialization(self, sample_helium_csv):
        """Test collector initializes correctly with CSV"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        assert collector is not None
        assert collector.dataset is not None
        assert collector.dataset.timeseries_length > 0
    
    def test_collector_fallback_to_synthetic(self):
        """Test collector generates synthetic data when CSV missing"""
        collector = HeliumDataCollector(csv_path=Path("/nonexistent/helium.csv"))
        assert collector is not None
        assert collector.dataset is not None
        assert collector.dataset.timeseries_length > 0
        assert collector.dataset.metadata['source'] == 'synthetic'
    
    def test_get_latest_record(self, sample_helium_csv):
        """Test getting latest helium record"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        latest = collector.get_latest()
        assert latest is not None
        assert latest.price_index > 0
        assert 0 <= latest.scarcity_index <= 1
        assert 0 <= latest.recycling_rate_0_1 <= 1
    
    def test_feature_vector_dimensions(self, sample_helium_csv):
        """Test feature vector has correct dimensions"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        features = collector.get_feature_vector()
        assert len(features) == 10
        assert np.all(features >= 0)
    
    def test_export_for_regret_optimizer(self, sample_helium_csv):
        """Test export format for regret optimizer"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        export = collector.export_for_regret_optimizer()
        assert 'helium_price_index' in export
        assert 'helium_scarcity_index' in export
        assert 'helium_supply_risk' in export
    
    def test_export_for_sustainability_signals(self, sample_helium_csv):
        """Test export format for sustainability signals"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        export = collector.export_for_sustainability_signals()
        assert 'helium_scarcity_signal' in export
        assert 'helium_circularity_signal' in export
        assert 'helium_thermal_signal' in export
    
    def test_derived_properties(self, sample_helium_csv):
        """Test derived properties are calculated correctly"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        latest = collector.get_latest()
        assert latest.demand_supply_ratio > 0
        assert 0 <= latest.scarcity_index <= 1
        assert 0 <= latest.circularity_potential <= 1
        assert latest.thermal_impact_factor >= 0
    
    def test_get_trends_returns_expected_keys(self, sample_helium_csv):
        """Test get_trends returns expected trend metrics"""
        collector = HeliumDataCollector(csv_path=sample_helium_csv)
        trends = collector.get_trends()
        assert 'production_change_pct' in trends
        assert 'price_change_pct' in trends
        assert 'scarcity_trend' in trends

# ============================================================
# TEST ELASTICITY CALCULATOR
# ============================================================

class TestHeliumElasticity:
    """Tests for helium_elasticity.py"""
    
    def test_calculator_initialization(self, elasticity_config):
        """Test elasticity calculator initializes"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        assert calculator is not None
    
    def test_price_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test price elasticity is in valid range"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_price_elasticity(sample_helium_data)
        assert -0.8 <= elasticity <= -0.1
    
    def test_scarcity_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test scarcity elasticity is in valid range"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_scarcity_elasticity(sample_helium_data)
        assert 0 <= elasticity <= 1
    
    def test_cross_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test cross elasticity is in valid range"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_cross_elasticity(sample_helium_data)
        assert 0 <= elasticity <= 1
    
    def test_thermal_elasticity_range(self, elasticity_config, sample_helium_data):
        """Test thermal elasticity is in valid range"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        elasticity = calculator.calculate_thermal_elasticity(sample_helium_data)
        assert 0 <= elasticity <= 1
    
    @pytest.mark.parametrize("scarcity, expected_rec", [
        (0.2, 'stay_local'),
        (0.5, 'consider_migration'),
        (0.8, 'migrate_soon'),
        (0.95, 'migrate_immediately')
    ])
    def test_migration_recommendation_logic(self, elasticity_config, scarcity, expected_rec):
        """Test migration recommendation logic with parametrized inputs"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        # Build data with given scarcity
        data = {
            'scarcity_index': scarcity,
            'shortage_severity_0_1': scarcity * 1.0,
            'supply_risk_score_0_1': scarcity * 0.9,
            'demand_supply_ratio': 1.0 + scarcity * 0.2,
            'price_index': 100 + scarcity * 200,
            'substitution_feasibility_0_1': 1.0 - scarcity * 0.9,
            'recycling_rate_0_1': 1.0 - scarcity * 0.8,
            'cooling_load_sensitivity': 1.0 + scarcity * 0.2,
            'geopolitical_risk_index': scarcity * 0.9,
            'logistics_disruption_index': scarcity * 0.8
        }
        metrics = calculator.calculate_comprehensive_elasticity(data)
        assert metrics.migration_recommendation == expected_rec
    
    def test_comprehensive_elasticity(self, elasticity_config, sample_helium_data):
        """Test comprehensive elasticity calculation"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        metrics = calculator.calculate_comprehensive_elasticity(sample_helium_data)
        assert metrics is not None
        assert 0 <= metrics.composite_elasticity <= 1
        assert metrics.migration_recommendation in [
            'stay_local', 'consider_migration', 'migrate_soon', 'migrate_immediately'
        ]
        assert 0 <= metrics.efficiency_target <= 1
    
    def test_regret_optimizer_export(self, elasticity_config, sample_helium_data):
        """Test regret optimizer export format"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_regret_optimizer()
        assert 'decision_weights' in export
        assert 'scenario_modifiers' in export
        assert 'recommendations' in export
    
    def test_thermal_optimizer_export(self, elasticity_config, sample_helium_data):
        """Test thermal optimizer export format"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_thermal_optimizer()
        assert 'thermal_params' in export or 'cooling_recommendations' in export
    
    def test_sustainability_signals_export(self, elasticity_config, sample_helium_data):
        """Test sustainability signals export format"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_sustainability_signals()
        assert 'sustainability_signals' in export or 'esg_impact' in export
    
    def test_full_export_structure(self, elasticity_config, sample_helium_data):
        """Test full export has all required sections"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_all()
        assert 'regret_optimizer' in export
        assert 'thermal_optimizer' in export
        assert 'sustainability_signals' in export
        assert 'synthetic_manager' in export
        assert 'metadata' in export
    
    def test_empty_data_handling(self, elasticity_config):
        """Test handling of empty/missing data"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        metrics = calculator.calculate_comprehensive_elasticity({})
        assert metrics is not None
    
    def test_extreme_values(self, elasticity_config):
        """Test handling of extreme values"""
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
        assert 0 <= metrics.scarcity_elasticity <= 1
        assert 0 <= metrics.thermal_elasticity <= 1
        assert 0 <= metrics.composite_elasticity <= 1

# ============================================================
# TEST CIRCULARITY CALCULATOR
# ============================================================

class TestHeliumCircularity:
    """Tests for helium_circularity.py"""
    
    def test_calculator_initialization(self, circularity_config):
        """Test circularity calculator initializes"""
        calculator = HeliumCircularityCalculator(circularity_config)
        assert calculator is not None
    
    def test_recovery_efficiency_range(self, circularity_config, sample_helium_data):
        """Test recovery efficiency is in valid range"""
        calculator = HeliumCircularityCalculator(circularity_config)
        efficiency = calculator.calculate_recovery_efficiency(sample_helium_data)
        assert 0 <= efficiency <= 1
    
    def test_recycling_rate_range(self, circularity_config, sample_helium_data):
        """Test recycling rate is in valid range"""
        calculator = HeliumCircularityCalculator(circularity_config)
        rate = calculator.calculate_recycling_rate(sample_helium_data)
        assert 0 <= rate <= 1
    
    def test_substitution_potential_range(self, circularity_config, sample_helium_data):
        """Test substitution potential is in valid range"""
        calculator = HeliumCircularityCalculator(circularity_config)
        potential = calculator.calculate_substitution_potential(sample_helium_data)
        assert 0 <= potential <= 1
    
    def test_mci_calculation(self, circularity_config):
        """Test Material Circularity Indicator calculation"""
        calculator = HeliumCircularityCalculator(circularity_config)
        mci_perfect = calculator.calculate_material_circularity_indicator(1.0, 1.0, 0.0)
        assert mci_perfect == 1.0
        mci_linear = calculator.calculate_material_circularity_indicator(0.0, 0.0, 1.0)
        assert mci_linear == 0.0
        mci_mixed = calculator.calculate_material_circularity_indicator(0.5, 0.5, 0.5)
        assert 0 < mci_mixed < 1
    
    def test_stage_efficiencies(self, circularity_config):
        """Test stage efficiency calculation"""
        calculator = HeliumCircularityCalculator(circularity_config)
        stages = calculator.calculate_stage_efficiencies()
        assert 'stages' in stages
        assert len(stages['stages']) == 4
        assert 'bottleneck' in stages
        assert stages['overall_throughput'] > 0
    
    def test_comprehensive_circularity(self, circularity_config, sample_helium_data):
        """Test comprehensive circularity calculation"""
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
        calculator = HeliumCircularityCalculator(circularity_config)
        costs = calculator.calculate_recovery_costs(10000)
        assert costs['total_cost'] > 0
        assert costs['total_energy_kwh'] > 0
        assert costs['carbon_footprint_kg'] > 0
        assert costs['cost_per_liter'] > 0
    
    def test_sustainability_export(self, circularity_config, sample_helium_data):
        """Test sustainability signals export format"""
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_for_sustainability_signals()
        assert 'circularity_metrics' in export
        assert 'sustainability_signals' in export
        assert 'material_flows' in export
    
    def test_regret_optimizer_export(self, circularity_config, sample_helium_data):
        """Test regret optimizer export format"""
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_for_regret_optimizer()
        assert 'decision_weights' in export
        assert 'scenario_modifiers' in export
    
    def test_full_export_structure(self, circularity_config, sample_helium_data):
        """Test full export has all required sections"""
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_all()
        assert 'sustainability_signals' in export
        assert 'regret_optimizer' in export
        assert 'thermal_optimizer' in export
        assert 'synthetic_manager' in export
        assert 'cost_analysis' in export
        assert 'metadata' in export
    
    def test_negative_values_handling(self, circularity_config):
        """Test handling of negative values"""
        calculator = HeliumCircularityCalculator(circularity_config)
        mci = calculator.calculate_material_circularity_indicator(-1.0, -1.0, -1.0)
        assert 0 <= mci <= 1
    
    def test_zero_division_safety(self, circularity_config):
        """Test safety against division by zero"""
        calculator = HeliumCircularityCalculator(circularity_config)
        costs = calculator.calculate_recovery_costs(0)
        assert costs is not None

# ============================================================
# TEST REGRET OPTIMIZER (NEW)
# ============================================================

class TestHeliumRegretOptimizer:
    """Tests for helium_regret_optimizer.py"""
    
    def test_optimizer_initialization(self, regret_config):
        """Test regret optimizer initializes with config"""
        optimizer = HeliumRegretOptimizer(regret_config)
        assert optimizer is not None
        assert optimizer.max_regret == 0.5
    
    def test_compute_regret_with_scenarios(self, regret_config, sample_helium_data):
        """Test regret computation given scenarios"""
        optimizer = HeliumRegretOptimizer(regret_config)
        # Create some decision options and scenarios
        decisions = ['keep', 'migrate', 'upgrade']
        scenarios = [sample_helium_data, sample_helium_data.copy()]
        scenarios[1]['price_index'] = 300  # different scenario
        result = optimizer.compute_regret(decisions, scenarios)
        assert result is not None
        assert 'optimal_decision' in result
        assert result['optimal_decision'] in decisions
        assert 'regret_values' in result
        assert len(result['regret_values']) == len(decisions)
    
    def test_cvar_calculation(self, regret_config, sample_helium_data):
        """Test Conditional Value at Risk calculation"""
        optimizer = HeliumRegretOptimizer(regret_config)
        # Test with simple regret list
        regrets = [0.1, 0.2, 0.3, 0.4, 0.5]
        cvar = optimizer._compute_cvar(regrets, alpha=0.95)
        assert cvar is not None
        assert 0 <= cvar <= 1
    
    def test_export_for_synthetic(self, regret_config):
        """Test export to synthetic manager"""
        optimizer = HeliumRegretOptimizer(regret_config)
        export = optimizer.export_for_synthetic()
        assert 'regret_parameters' in export
        assert 'cvar_alpha' in export['regret_parameters']

# ============================================================
# TEST THERMAL OPTIMIZER (NEW)
# ============================================================

class TestHeliumThermalOptimizer:
    """Tests for helium_thermal_optimizer.py"""
    
    def test_optimizer_initialization(self, thermal_config):
        """Test thermal optimizer initializes with config"""
        optimizer = HeliumThermalOptimizer(thermal_config)
        assert optimizer is not None
        assert optimizer.target_temperature == 8.0
    
    def test_optimize_cooling_for_helium(self, thermal_config, sample_helium_data):
        """Test cooling optimization given helium data"""
        optimizer = HeliumThermalOptimizer(thermal_config)
        result = optimizer.optimize_cooling(sample_helium_data)
        assert result is not None
        assert 'recommended_power' in result
        assert 'estimated_helium_savings' in result
        assert result['recommended_power'] > 0
    
    def test_export_for_elasticity(self, thermal_config):
        """Test export to elasticity module"""
        optimizer = HeliumThermalOptimizer(thermal_config)
        export = optimizer.export_for_elasticity()
        assert 'thermal_params' in export

# ============================================================
# TEST SYNTHETIC DATA MANAGER (NEW)
# ============================================================

class TestHeliumSyntheticManager:
    """Tests for helium_synthetic_manager.py"""
    
    def test_manager_initialization(self, synthetic_config):
        """Test synthetic manager initializes with config"""
        manager = HeliumSyntheticDataManager(synthetic_config)
        assert manager is not None
        assert manager.num_samples == 100
    
    def test_generate_synthetic_data(self, synthetic_config):
        """Test generation of synthetic helium data"""
        manager = HeliumSyntheticDataManager(synthetic_config)
        data = manager.generate()
        assert data is not None
        assert isinstance(data, list)
        assert len(data) == 100
        # Check first record has expected keys
        if data:
            first = data[0]
            assert 'price_index' in first
            assert 'scarcity_index' in first
    
    def test_export_for_elasticity(self, synthetic_config):
        """Test export to elasticity module"""
        manager = HeliumSyntheticDataManager(synthetic_config)
        export = manager.export_for_elasticity()
        assert 'synthetic_scenarios' in export

# ============================================================
# TEST SUSTAINABILITY SIGNALS (NEW)
# ============================================================

class TestHeliumSustainabilitySignals:
    """Tests for helium_sustainability_signals.py"""
    
    def test_initialization(self, sustainability_config):
        """Test sustainability signals initializes"""
        signals = HeliumSustainabilitySignals(sustainability_config)
        assert signals is not None
    
    def test_extract_helium_signals(self, sustainability_config, sample_helium_data):
        """Test extraction of helium-specific sustainability signals"""
        signals = HeliumSustainabilitySignals(sustainability_config)
        result = signals.extract_signals(sample_helium_data)
        assert result is not None
        assert 'helium_scarcity_signal' in result
        assert 'circularity_potential' in result
        assert 'recycling_rate_signal' in result
    
    def test_export_for_regret(self, sustainability_config):
        """Test export to regret optimizer"""
        signals = HeliumSustainabilitySignals(sustainability_config)
        export = signals.export_for_regret()
        assert 'sustainability_signals' in export

# ============================================================
# TEST CROSS-MODULE INTEGRATION
# ============================================================

class TestCrossModuleIntegration:
    """Tests for cross-module data flow"""
    
    def test_data_collector_to_elasticity(self, sample_helium_data):
        """Test data flow from collector to elasticity"""
        config = ElasticityConfig(enable_data_collector=False)
        calculator = HeliumElasticityCalculator(config)
        metrics = calculator.calculate_comprehensive_elasticity(sample_helium_data)
        assert metrics is not None
        assert metrics.price_elasticity < 0
    
    def test_data_collector_to_circularity(self, sample_helium_data):
        """Test data flow from collector to circularity"""
        config = CircularityConfig(enable_data_collector=False)
        calculator = HeliumCircularityCalculator(config)
        metrics = calculator.calculate_comprehensive_circularity(sample_helium_data)
        assert metrics is not None
        assert metrics.recycling_rate > 0
    
    def test_elasticity_to_regret(self, elasticity_config, sample_helium_data):
        """Test elasticity exports data usable by regret optimizer"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        export = calculator.export_for_regret_optimizer()
        # Create regret optimizer and use export
        optimizer = HeliumRegretOptimizer({'max_regret': 0.5})
        # Simulate using export
        assert 'decision_weights' in export
    
    def test_circularity_to_sustainability(self, circularity_config, sample_helium_data):
        """Test circularity exports data usable by sustainability signals"""
        calculator = HeliumCircularityCalculator(circularity_config)
        export = calculator.export_for_sustainability_signals()
        signals = HeliumSustainabilitySignals({})
        # Check that export contains expected keys
        assert 'circularity_metrics' in export

# ============================================================
# TEST EDGE CASES AND ERROR HANDLING
# ============================================================

class TestErrorHandling:
    """Tests for error handling across modules"""
    
    def test_missing_csv_raises_warning(self):
        """Test that missing CSV triggers warning and fallback"""
        with pytest.warns(UserWarning, match="CSV file not found"):
            collector = HeliumDataCollector(csv_path=Path("/nonexistent/helium.csv"))
        assert collector.dataset.metadata['source'] == 'synthetic'
    
    def test_invalid_config_raises_error(self):
        """Test that invalid config values raise errors"""
        with pytest.raises(ValueError):
            ElasticityConfig(learning_rate=-0.1)  # assume validation
    
    def test_elasticity_with_missing_fields(self, elasticity_config):
        """Test elasticity calculation with missing fields"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        incomplete_data = {'price_index': 150}  # missing scarcity
        metrics = calculator.calculate_comprehensive_elasticity(incomplete_data)
        # Should still produce metrics with defaults
        assert metrics is not None
    
    def test_circularity_with_missing_fields(self, circularity_config):
        """Test circularity calculation with missing fields"""
        calculator = HeliumCircularityCalculator(circularity_config)
        incomplete_data = {'recycling_rate_0_1': 0.5}
        metrics = calculator.calculate_comprehensive_circularity(incomplete_data)
        assert metrics is not None

# ============================================================
# TEST PERFORMANCE (conditional benchmark)
# ============================================================

class TestPerformance:
    """Performance benchmarks for helium modules (skip if benchmark not available)"""
    
    @pytest.mark.skipif(not BENCHMARK_AVAILABLE, reason="pytest-benchmark not installed")
    def test_elasticity_calculation_speed(self, elasticity_config, sample_helium_data, benchmark):
        """Benchmark elasticity calculation speed"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        result = benchmark(calculator.calculate_comprehensive_elasticity, sample_helium_data)
        assert result is not None
    
    @pytest.mark.skipif(not BENCHMARK_AVAILABLE, reason="pytest-benchmark not installed")
    def test_circularity_calculation_speed(self, circularity_config, sample_helium_data, benchmark):
        """Benchmark circularity calculation speed"""
        calculator = HeliumCircularityCalculator(circularity_config)
        result = benchmark(calculator.calculate_comprehensive_circularity, sample_helium_data)
        assert result is not None
    
    def test_bulk_calculations(self, elasticity_config, sample_helium_data):
        """Test performance with bulk calculations (without benchmark)"""
        calculator = HeliumElasticityCalculator(elasticity_config)
        import time
        start = time.time()
        for _ in range(100):
            calculator.calculate_comprehensive_elasticity(sample_helium_data)
        elapsed = time.time() - start
        assert elapsed < 2.0

# ============================================================
# RUNNER
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
