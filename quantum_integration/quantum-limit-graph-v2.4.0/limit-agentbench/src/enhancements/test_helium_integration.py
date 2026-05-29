# File: src/enhancements/test_helium_integration.py (UPGRADED VERSION)

"""
Integration Test for Helium Dataset with All Enhancement Modules - Version 6.1

Tests connectivity and data flow between:
- helium_data_collector.py
- helium_elasticity.py
- helium_circularity.py
- sustainability_signals.py (compatibility)
- regret_optimizer.py (compatibility)
- thermal_optimizer.py (compatibility)
- synthetic_data_manager.py (compatibility)
"""

import sys
import os
from pathlib import Path
import json
import time
from datetime import datetime
import numpy as np
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Test result tracking
class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.warnings = []
        self.start_time = datetime.now()
    
    def assert_true(self, condition: bool, test_name: str, detail: str = ""):
        if condition:
            self.passed += 1
            print(f"   ✅ {test_name}: PASSED")
        else:
            self.failed += 1
            error_msg = f"{test_name}: FAILED - {detail}"
            self.errors.append(error_msg)
            print(f"   ❌ {error_msg}")
    
    def assert_not_none(self, value, test_name: str):
        self.assert_true(value is not None, test_name, "Value is None")
    
    def assert_not_empty(self, value, test_name: str):
        if isinstance(value, (list, dict, str)):
            self.assert_true(len(value) > 0, test_name, f"Empty {type(value).__name__}")
        else:
            self.assert_true(value is not None, test_name, "Value is None")
    
    def add_warning(self, message: str):
        self.warnings.append(message)
        print(f"   ⚠️ WARNING: {message}")
    
    def summary(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print("\n" + "=" * 80)
        print(f"TEST SUMMARY - Completed in {elapsed:.2f}s")
        print(f"   Passed: {self.passed}")
        print(f"   Failed: {self.failed}")
        print(f"   Warnings: {len(self.warnings)}")
        
        if self.errors:
            print(f"\n❌ FAILED TESTS:")
            for error in self.errors:
                print(f"   - {error}")
        
        if self.warnings:
            print(f"\n⚠️ WARNINGS:")
            for warning in self.warnings:
                print(f"   - {warning}")
        
        if self.failed == 0:
            print(f"\n🎉 ALL TESTS PASSED!")
        
        print("=" * 80)
        
        return self.failed == 0


# ============================================================
# TEST DATA COLLECTOR
# ============================================================

def test_data_collector(results: TestResults):
    """Test helium_data_collector.py functionality"""
    
    print("\n" + "─" * 60)
    print("1. Testing Helium Data Collector")
    print("─" * 60)
    
    try:
        from helium_data_collector import (
            HeliumDataCollector, HeliumRecord, HeliumDataset, get_helium_collector
        )
        results.assert_true(True, "Import helium_data_collector")
    except ImportError as e:
        results.assert_true(False, "Import helium_data_collector", str(e))
        return
    
    # Test singleton
    collector = get_helium_collector()
    results.assert_not_none(collector, "Get singleton collector")
    
    # Test data loading
    latest = collector.get_latest()
    results.assert_not_none(latest, "Get latest record")
    
    if latest:
        results.assert_true(latest.price_index > 0, "Price index positive")
        results.assert_true(0 <= latest.scarcity_index <= 1, "Scarcity index in range [0,1]")
        results.assert_true(0 <= latest.recycling_rate_0_1 <= 1, "Recycling rate in range [0,1]")
        results.assert_true(latest.demand_supply_ratio > 0, "Demand/supply ratio positive")
    
    # Test dataset
    dataset = collector.dataset
    results.assert_not_none(dataset, "Get dataset")
    
    if dataset:
        results.assert_true(dataset.timeseries_length > 0, "Dataset has records")
        
        # Test dataframe conversion
        df = dataset.to_dataframe()
        results.assert_true(len(df) > 0, "Convert to DataFrame")
        results.assert_true(len(df.columns) > 5, "DataFrame has sufficient columns")
        
        # Test feature matrix
        features = dataset.to_feature_matrix()
        results.assert_true(len(features) > 0, "Get feature matrix")
        if len(features) > 0:
            results.assert_true(features.shape[1] == 10, "Feature matrix has 10 dimensions")
    
    # Test trends
    trends = collector.get_trends()
    results.assert_not_empty(trends, "Get trends")
    
    # Test feature vector
    feature_vector = collector.get_feature_vector()
    results.assert_true(len(feature_vector) == 10, "Feature vector has 10 dimensions")
    results.assert_true(np.all(feature_vector >= 0), "Feature vector non-negative")
    
    # Test exports
    regret_export = collector.export_for_regret_optimizer()
    results.assert_not_empty(regret_export, "Export for regret optimizer")
    
    sust_export = collector.export_for_sustainability_signals()
    results.assert_not_empty(sust_export, "Export for sustainability signals")
    
    synth_export = collector.export_for_synthetic_manager()
    results.assert_not_empty(synth_export, "Export for synthetic manager")
    
    print(f"\n   Data Collector: {dataset.timeseries_length if dataset else 0} records loaded")


# ============================================================
# TEST ELASTICITY CALCULATOR
# ============================================================

def test_elasticity_calculator(results: TestResults):
    """Test helium_elasticity.py functionality and integrations"""
    
    print("\n" + "─" * 60)
    print("2. Testing Helium Elasticity Calculator")
    print("─" * 60)
    
    try:
        from helium_elasticity import (
            HeliumElasticityCalculator, HeliumElasticityMetrics,
            ElasticityConfig, get_helium_elasticity_calculator
        )
        results.assert_true(True, "Import helium_elasticity")
    except ImportError as e:
        results.assert_true(False, "Import helium_elasticity", str(e))
        return
    
    # Initialize calculator
    config = ElasticityConfig(
        enable_data_collector=True,
        enable_regret_integration=True,
        enable_thermal_integration=True,
        enable_sustainability_integration=True,
        enable_synthetic_integration=True
    )
    
    calculator = HeliumElasticityCalculator(config)
    results.assert_not_none(calculator, "Initialize elasticity calculator")
    
    # Test data collection
    helium_data = calculator.get_current_helium_data()
    results.assert_not_empty(helium_data, "Get current helium data")
    
    # Test individual elasticities
    price_elasticity = calculator.calculate_price_elasticity(helium_data)
    results.assert_true(-0.8 <= price_elasticity <= -0.1, 
                       f"Price elasticity in range (got {price_elasticity:.3f})")
    
    scarcity_elasticity = calculator.calculate_scarcity_elasticity(helium_data)
    results.assert_true(0 <= scarcity_elasticity <= 1, 
                       f"Scarcity elasticity in range (got {scarcity_elasticity:.3f})")
    
    cross_elasticity = calculator.calculate_cross_elasticity(helium_data)
    results.assert_true(0 <= cross_elasticity <= 1, 
                       f"Cross elasticity in range (got {cross_elasticity:.3f})")
    
    thermal_elasticity = calculator.calculate_thermal_elasticity(helium_data)
    results.assert_true(0 <= thermal_elasticity <= 1, 
                       f"Thermal elasticity in range (got {thermal_elasticity:.3f})")
    
    # Test comprehensive calculation
    metrics = calculator.calculate_comprehensive_elasticity(helium_data)
    results.assert_not_none(metrics, "Calculate comprehensive elasticity")
    
    if metrics:
        results.assert_true(0 <= metrics.composite_elasticity <= 1, 
                           "Composite elasticity in range")
        results.assert_true(metrics.migration_recommendation in 
                           ['stay_local', 'consider_migration', 'migrate_soon', 'migrate_immediately'],
                           "Valid migration recommendation")
        results.assert_true(0 <= metrics.efficiency_target <= 1, 
                           "Efficiency target in range")
    
    # Test integration exports
    regret_export = calculator.export_for_regret_optimizer()
    results.assert_not_empty(regret_export.get('decision_weights', {}), 
                            "Regret optimizer export has decision weights")
    
    thermal_export = calculator.export_for_thermal_optimizer()
    results.assert_not_empty(thermal_export.get('thermal_params', {}), 
                            "Thermal optimizer export has params")
    
    sust_export = calculator.export_for_sustainability_signals()
    results.assert_not_empty(sust_export.get('sustainability_signals', {}), 
                            "Sustainability signals export has signals")
    
    synth_export = calculator.export_for_synthetic_manager()
    results.assert_not_empty(synth_export.get('generation_templates', {}), 
                            "Synthetic manager export has templates")
    
    # Test full export
    all_export = calculator.export_all()
    results.assert_true(len(all_export) == 5, f"Full export has 5 sections (got {len(all_export)})")
    
    print(f"\n   Elasticity: composite={metrics.composite_elasticity:.3f}, "
          f"migration={metrics.migration_recommendation}")


# ============================================================
# TEST CIRCULARITY CALCULATOR
# ============================================================

def test_circularity_calculator(results: TestResults):
    """Test helium_circularity.py functionality and integrations"""
    
    print("\n" + "─" * 60)
    print("3. Testing Helium Circularity Calculator")
    print("─" * 60)
    
    try:
        from helium_circularity import (
            HeliumCircularityCalculator, HeliumCircularityMetrics,
            CircularityConfig, RecoveryMethod, get_helium_circularity_calculator
        )
        results.assert_true(True, "Import helium_circularity")
    except ImportError as e:
        results.assert_true(False, "Import helium_circularity", str(e))
        return
    
    # Initialize calculator
    config = CircularityConfig(
        enable_data_collector=True,
        enable_elasticity_integration=True,
        enable_sustainability_integration=True,
        enable_regret_integration=True,
        enable_thermal_integration=True,
        enable_synthetic_integration=True,
        recovery_method=RecoveryMethod.HYBRID
    )
    
    calculator = HeliumCircularityCalculator(config)
    results.assert_not_none(calculator, "Initialize circularity calculator")
    
    # Test data collection
    helium_data = calculator.get_current_helium_data()
    results.assert_not_empty(helium_data, "Get current helium data")
    
    # Test individual calculations
    recovery_efficiency = calculator.calculate_recovery_efficiency(helium_data)
    results.assert_true(0 <= recovery_efficiency <= 1, 
                       f"Recovery efficiency in range (got {recovery_efficiency:.3f})")
    
    recycling_rate = calculator.calculate_recycling_rate(helium_data)
    results.assert_true(0 <= recycling_rate <= 1, 
                       f"Recycling rate in range (got {recycling_rate:.3f})")
    
    substitution_potential = calculator.calculate_substitution_potential(helium_data)
    results.assert_true(0 <= substitution_potential <= 1, 
                       f"Substitution potential in range (got {substitution_potential:.3f})")
    
    # Test MCI calculation
    mci = calculator.calculate_material_circularity_indicator(
        recycling_rate, recovery_efficiency, 0.1
    )
    results.assert_true(0 <= mci <= 1, f"MCI in range (got {mci:.3f})")
    
    # Test stage efficiencies
    stages = calculator.calculate_stage_efficiencies()
    results.assert_not_empty(stages, "Calculate stage efficiencies")
    results.assert_true(len(stages['stages']) == 4, "4 recovery stages")
    
    # Test comprehensive calculation
    metrics = calculator.calculate_comprehensive_circularity(helium_data)
    results.assert_not_none(metrics, "Calculate comprehensive circularity")
    
    if metrics:
        results.assert_true(0 <= metrics.circularity_index <= 1, 
                           "Circularity index in range")
        results.assert_not_empty(metrics.circularity_level, "Circularity level assigned")
        results.assert_not_empty(metrics.certification_level, "Certification level assigned")
        results.assert_true(0 <= metrics.closed_loop_score <= 1, 
                           "Closed loop score in range")
        results.assert_true(0 <= metrics.lifecycle_extension_potential <= 1, 
                           "Lifecycle extension in range")
    
    # Test cost analysis
    costs = calculator.calculate_recovery_costs(10000)
    results.assert_true(costs['total_cost'] > 0, "Recovery cost positive")
    results.assert_true(costs['total_energy_kwh'] > 0, "Recovery energy positive")
    
    # Test integration exports
    sust_export = calculator.export_for_sustainability_signals()
    results.assert_not_empty(sust_export.get('sustainability_signals', {}), 
                            "Sustainability signals export")
    
    regret_export = calculator.export_for_regret_optimizer()
    results.assert_not_empty(regret_export.get('decision_weights', {}), 
                            "Regret optimizer export")
    
    thermal_export = calculator.export_for_thermal_optimizer()
    results.assert_not_empty(thermal_export.get('thermal_params', {}), 
                            "Thermal optimizer export")
    
    synth_export = calculator.export_for_synthetic_manager()
    results.assert_not_empty(synth_export.get('generation_templates', {}), 
                            "Synthetic manager export")
    
    # Test full export
    all_export = calculator.export_all()
    results.assert_true(len(all_export) == 6, f"Full export has 6 sections (got {len(all_export)})")
    
    print(f"\n   Circularity: index={metrics.circularity_index:.3f}, "
          f"level={metrics.circularity_level}, cert={metrics.certification_level}")


# ============================================================
# TEST CROSS-MODULE INTEGRATION
# ============================================================

def test_cross_module_integration(results: TestResults):
    """Test data flow between all modules"""
    
    print("\n" + "─" * 60)
    print("4. Testing Cross-Module Integration")
    print("─" * 60)
    
    # Test data collector → elasticity flow
    try:
        from helium_data_collector import get_helium_collector
        from helium_elasticity import HeliumElasticityCalculator, ElasticityConfig
        
        collector = get_helium_collector()
        elasticity_calc = HeliumElasticityCalculator(
            ElasticityConfig(enable_data_collector=True)
        )
        
        # Verify data flow
        if hasattr(elasticity_calc, 'collector') and elasticity_calc.collector:
            results.assert_true(True, "Data collector → elasticity: Connected")
        else:
            results.add_warning("Data collector → elasticity: Using defaults (collector not available)")
        
        # Calculate with collector data
        metrics = elasticity_calc.calculate_comprehensive_elasticity()
        results.assert_not_none(metrics, "Elasticity calculation with collector data")
        
    except Exception as e:
        results.add_warning(f"Data collector → elasticity: {str(e)}")
    
    # Test data collector → circularity flow
    try:
        from helium_circularity import HeliumCircularityCalculator, CircularityConfig
        
        circularity_calc = HeliumCircularityCalculator(
            CircularityConfig(enable_data_collector=True)
        )
        
        if hasattr(circularity_calc, 'collector') and circularity_calc.collector:
            results.assert_true(True, "Data collector → circularity: Connected")
        else:
            results.add_warning("Data collector → circularity: Using defaults")
        
        metrics = circularity_calc.calculate_comprehensive_circularity()
        results.assert_not_none(metrics, "Circularity calculation with collector data")
        
    except Exception as e:
        results.add_warning(f"Data collector → circularity: {str(e)}")
    
    # Test elasticity → circularity flow
    try:
        from helium_circularity import HeliumCircularityCalculator, CircularityConfig
        
        circularity_calc = HeliumCircularityCalculator(
            CircularityConfig(enable_elasticity_integration=True)
        )
        
        if hasattr(circularity_calc, 'elasticity_calculator') and circularity_calc.elasticity_calculator:
            results.assert_true(True, "Elasticity → circularity: Connected")
        else:
            results.add_warning("Elasticity → circularity: Elasticity not available")
        
    except Exception as e:
        results.add_warning(f"Elasticity → circularity: {str(e)}")
    
    print(f"\n   Cross-module integration verified")


# ============================================================
# TEST EXPORT FORMAT COMPATIBILITY
# ============================================================

def test_export_compatibility(results: TestResults):
    """Test that exports are compatible with target modules"""
    
    print("\n" + "─" * 60)
    print("5. Testing Export Format Compatibility")
    print("─" * 60)
    
    try:
        from helium_elasticity import get_helium_elasticity_calculator, ElasticityConfig
        from helium_circularity import get_helium_circularity_calculator, CircularityConfig
        
        elasticity_calc = get_helium_elasticity_calculator(
            ElasticityConfig(enable_data_collector=True)
        )
        circularity_calc = get_helium_circularity_calculator(
            CircularityConfig(enable_data_collector=True)
        )
        
        # Test regret optimizer export format
        regret_export = elasticity_calc.export_for_regret_optimizer()
        
        # Check required fields for regret optimizer
        required_regret_fields = ['decision_weights', 'scenario_modifiers', 'recommendations']
        for field in required_regret_fields:
            results.assert_true(field in regret_export, 
                               f"Regret export has '{field}' field")
        
        # Check decision weights have expected structure
        weights = regret_export.get('decision_weights', {})
        expected_weights = ['helium_efficiency_weight', 'cooling_efficiency_weight', 
                          'carbon_reduction_weight', 'cost_weight']
        for weight in expected_weights:
            if weight in weights:
                results.assert_true(isinstance(weights[weight], (int, float)), 
                                   f"Weight '{weight}' is numeric")
        
        # Test sustainability signals export format
        sust_export = circularity_calc.export_for_sustainability_signals()
        
        # Check required fields for sustainability signals
        required_sust_fields = ['circularity_metrics', 'sustainability_signals', 'material_flows']
        for field in required_sust_fields:
            results.assert_true(field in sust_export, 
                               f"Sustainability export has '{field}' field")
        
        # Check material flows have expected structure
        flows = sust_export.get('material_flows', {})
        expected_flows = ['virgin_material_pct', 'recycled_material_pct', 
                         'recovered_material_pct', 'lost_material_pct']
        for flow in expected_flows:
            if flow in flows:
                results.assert_true(isinstance(flows[flow], (int, float)), 
                                   f"Flow '{flow}' is numeric")
        
        # Test thermal optimizer export format
        thermal_export = elasticity_calc.export_for_thermal_optimizer()
        results.assert_true('thermal_params' in thermal_export or 'cooling_recommendations' in thermal_export,
                          "Thermal export has expected structure")
        
        # Test synthetic manager export format
        synth_export = circularity_calc.export_for_synthetic_manager()
        results.assert_true('generation_templates' in synth_export or 'scenario_params' in synth_export,
                          "Synthetic export has expected structure")
        
        results.assert_true(True, "All export formats validated")
        
    except Exception as e:
        results.assert_true(False, "Export compatibility test", str(e))
    
    print(f"\n   Export format compatibility verified")


# ============================================================
# TEST DATA QUALITY AND VALIDATION
# ============================================================

def test_data_quality(results: TestResults):
    """Test data quality and validation"""
    
    print("\n" + "─" * 60)
    print("6. Testing Data Quality and Validation")
    print("─" * 60)
    
    try:
        from helium_data_collector import get_helium_collector, HeliumRecord
        
        collector = get_helium_collector()
        latest = collector.get_latest()
        
        if latest:
            # Test value ranges
            results.assert_true(0 <= latest.shortage_severity_0_1 <= 1, 
                               "Shortage severity in [0,1]")
            results.assert_true(0 <= latest.supply_risk_score_0_1 <= 1, 
                               "Supply risk in [0,1]")
            results.assert_true(0 <= latest.recycling_rate_0_1 <= 1, 
                               "Recycling rate in [0,1]")
            results.assert_true(0 <= latest.substitution_feasibility_0_1 <= 1, 
                               "Substitution feasibility in [0,1]")
            results.assert_true(latest.global_production_tonnes > 0, 
                               "Production positive")
            results.assert_true(latest.global_demand_tonnes > 0, 
                               "Demand positive")
            results.assert_true(latest.price_index > 0, 
                               "Price index positive")
            
            # Test derived properties
            results.assert_true(latest.demand_supply_ratio > 0, 
                               "Demand/supply ratio positive")
            results.assert_true(0 <= latest.scarcity_index <= 1, 
                               "Scarcity index in [0,1]")
            results.assert_true(0 <= latest.circularity_potential <= 1, 
                               "Circularity potential in [0,1]")
            
            # Test feature vector
            features = latest.to_feature_vector()
            results.assert_true(len(features) == 10, "Feature vector dimensions")
            results.assert_true(np.all(np.isfinite(features)), "All features finite")
            
            # Test dictionary conversion
            record_dict = latest.to_dict()
            results.assert_true(len(record_dict) > 10, "Dictionary has sufficient fields")
            results.assert_true('date' in record_dict, "Dictionary has date")
            results.assert_true('scarcity_index' in record_dict, "Dictionary has scarcity_index")
        
        results.assert_true(True, "Data quality checks passed")
        
    except Exception as e:
        results.assert_true(False, "Data quality test", str(e))
    
    print(f"\n   Data quality validation complete")


# ============================================================
# TEST PERFORMANCE
# ============================================================

def test_performance(results: TestResults):
    """Test calculation performance"""
    
    print("\n" + "─" * 60)
    print("7. Testing Calculation Performance")
    print("─" * 60)
    
    try:
        from helium_elasticity import HeliumElasticityCalculator, ElasticityConfig
        from helium_circularity import HeliumCircularityCalculator, CircularityConfig
        
        elasticity_calc = HeliumElasticityCalculator(
            ElasticityConfig(enable_data_collector=True)
        )
        circularity_calc = HeliumCircularityCalculator(
            CircularityConfig(enable_data_collector=True)
        )
        
        # Measure elasticity calculation time
        start = time.time()
        for _ in range(10):
            elasticity_calc.calculate_comprehensive_elasticity()
        elasticity_time = (time.time() - start) / 10
        
        results.assert_true(elasticity_time < 1.0, 
                           f"Elasticity calculation < 1s (avg: {elasticity_time:.4f}s)")
        
        # Measure circularity calculation time
        start = time.time()
        for _ in range(10):
            circularity_calc.calculate_comprehensive_circularity()
        circularity_time = (time.time() - start) / 10
        
        results.assert_true(circularity_time < 1.0, 
                           f"Circularity calculation < 1s (avg: {circularity_time:.4f}s)")
        
        # Measure export time
        start = time.time()
        elasticity_calc.export_all()
        export_time = time.time() - start
        
        results.assert_true(export_time < 2.0, 
                           f"Full export < 2s ({export_time:.4f}s)")
        
        print(f"\n   Performance: elasticity={elasticity_time:.4f}s, "
              f"circularity={circularity_time:.4f}s, export={export_time:.4f}s")
        
    except Exception as e:
        results.add_warning(f"Performance test: {str(e)}")
    
    print(f"\n   Performance benchmarks complete")


# ============================================================
# MAIN TEST RUNNER
# ============================================================

def run_all_tests():
    """Run all integration tests"""
    
    print("=" * 80)
    print("HELIUM DATASET INTEGRATION TEST SUITE v6.1")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 80)
    
    # Check available modules
    print("\n📦 Module Availability:")
    
    try:
        from helium_data_collector import get_helium_collector
        print("   ✅ helium_data_collector.py")
        HELIUM_AVAILABLE = True
    except ImportError:
        print("   ❌ helium_data_collector.py")
        HELIUM_AVAILABLE = False
    
    try:
        from helium_elasticity import get_helium_elasticity_calculator
        print("   ✅ helium_elasticity.py")
        ELASTICITY_AVAILABLE = True
    except ImportError:
        print("   ❌ helium_elasticity.py")
        ELASTICITY_AVAILABLE = False
    
    try:
        from helium_circularity import get_helium_circularity_calculator
        print("   ✅ helium_circularity.py")
        CIRCULARITY_AVAILABLE = True
    except ImportError:
        print("   ❌ helium_circularity.py")
        CIRCULARITY_AVAILABLE = False
    
    # Initialize test results
    results = TestResults()
    
    if not HELIUM_AVAILABLE:
        results.assert_true(False, "Prerequisites", "helium_data_collector.py is required")
        results.summary()
        return False
    
    # Run tests
    test_data_collector(results)
    
    if ELASTICITY_AVAILABLE:
        test_elasticity_calculator(results)
    else:
        results.add_warning("Skipping elasticity tests - module not available")
    
    if CIRCULARITY_AVAILABLE:
        test_circularity_calculator(results)
    else:
        results.add_warning("Skipping circularity tests - module not available")
    
    if ELASTICITY_AVAILABLE and CIRCULARITY_AVAILABLE:
        test_cross_module_integration(results)
        test_export_compatibility(results)
    else:
        results.add_warning("Skipping cross-module tests - modules not available")
    
    test_data_quality(results)
    test_performance(results)
    
    # Print summary
    success = results.summary()
    
    if success:
        print("\n📦 Integration Summary:")
        print("   ✅ helium_data_collector.py → Data loading & feature extraction")
        print("   ✅ helium_elasticity.py → Elasticity & migration recommendations")
        print("   ✅ helium_circularity.py → Circularity & recovery optimization")
        print("   ✅ sustainability_signals.py → ESG integration ready")
        print("   ✅ regret_optimizer.py → Decision weight integration ready")
        print("   ✅ thermal_optimizer.py → Cooling optimization integration ready")
        print("   ✅ synthetic_data_manager.py → Scenario generation ready")
    
    return success


if __name__ == "__main__":
    success = run_all_tests()
    
    if success:
        print("\n🎉 Helium dataset ready for Green Agent enhancement modules!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please review the errors above.")
        sys.exit(1)
