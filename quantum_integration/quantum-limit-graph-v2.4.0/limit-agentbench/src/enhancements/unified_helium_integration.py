# File: src/enhancements/unified_helium_integration.py
"""
Unified Integration Script for All Green Agent Modules
Uses enhanced dataset with 22 fields
"""

import asyncio
from helium_data_collector_enhanced import get_enhanced_helium_collector

async def run_unified_integration():
    """Run all modules with the enhanced dataset"""
    
    print("=" * 80)
    print("Unified Helium Integration - All Modules v3.0")
    print("=" * 80)
    
    # Initialize collector
    collector = get_enhanced_helium_collector()
    latest = collector.get_latest()
    
    print(f"\n📊 Current Helium Market Status:")
    print(f"   Scarcity Index: {latest.helium_scarcity_impact:.3f}")
    print(f"   Price Index: {latest.price_index:.0f}")
    print(f"   ESG Score: {latest.esg_score:.0f}/100")
    print(f"   Market Regime: {latest.market_regime}")
    
    # ============================================================
    # 1. Helium Elasticity Module
    # ============================================================
    print("\n" + "=" * 80)
    print("1. Helium Elasticity Calculator")
    print("=" * 80)
    
    elasticity_data = collector.export_for_elasticity()
    print(f"   Price Elasticity: {elasticity_data['price_elasticity']:.3f}")
    print(f"   Composite Elasticity: {elasticity_data['composite_elasticity']:.3f}")
    print(f"   Market Regime: {elasticity_data['market_regime']}")
    print(f"   Carbon Sensitivity: {elasticity_data['carbon_price_sensitivity']:.2f}")
    
    # ============================================================
    # 2. Helium Circularity Module
    # ============================================================
    print("\n" + "=" * 80)
    print("2. Helium Circularity Calculator")
    print("=" * 80)
    
    circularity_data = collector.export_for_circularity()
    print(f"   Circularity Index: {circularity_data['circularity_index']:.3f}")
    print(f"   Closed Loop Score: {circularity_data['closed_loop_score']:.3f}")
    print(f"   Waste Heat Recovery: {circularity_data['waste_heat_recovery_potential']:.1f}%")
    print(f"   Circular Economy ROI: {circularity_data['circular_economy_roi']:.1%}")
    
    # ============================================================
    # 3. Helium Forecaster Module
    # ============================================================
    print("\n" + "=" * 80)
    print("3. Helium Forecaster")
    print("=" * 80)
    
    forecaster_data = collector.export_for_forecaster()
    print(f"   Feature Matrix Shape: {len(forecaster_data['training_data']['feature_matrix'])} samples")
    print(f"   Price Trend: {forecaster_data['trends']['price_trend']}")
    print(f"   Capacity Trend: {forecaster_data['trends']['scarcity_trend']}")
    print(f"   Capacity Forecast (6m): {forecaster_data['capacity_forecast']['forecast_6m']:.0f} tonnes")
    
    # ============================================================
    # 4. Sustainability Signals Module
    # ============================================================
    print("\n" + "=" * 80)
    print("4. Sustainability Signals")
    print("=" * 80)
    
    sustainability_data = collector.export_for_sustainability()
    print(f"   ESG Score: {sustainability_data['esg_score']:.0f}/100")
    print(f"   Carbon Intensity: {sustainability_data['carbon_intensity']:.0f} gCO2/kWh")
    print(f"   Renewable Energy: {sustainability_data['renewable_energy_pct']:.0f}%")
    print(f"   Supply Chain Risk: {sustainability_data['supply_chain_risk']:.2f}")
    
    # ============================================================
    # 5. Thermal Optimizer Module
    # ============================================================
    print("\n" + "=" * 80)
    print("5. Thermal Optimizer")
    print("=" * 80)
    
    thermal_data = collector.export_for_thermal()
    print(f"   Cooling Sensitivity: {thermal_data['cooling_load_sensitivity']:.3f}")
    print(f"   Thermal Impact: {thermal_data['thermal_impact_factor']:.3f}")
    print(f"   Free Cooling Potential: {thermal_data['free_cooling_potential']:.1%}")
    print(f"   Waste Heat Recovery: {thermal_data['waste_heat_recovery']:.1%}")
    
    # ============================================================
    # 6. Regret Optimizer Module
    # ============================================================
    print("\n" + "=" * 80)
    print("6. Regret Optimizer")
    print("=" * 80)
    
    regret_data = collector.export_for_regret_optimizer()
    print(f"   Price Scenarios - Best: ${regret_data['price_scenarios']['best_case']:.0f}")
    print(f"   Price Scenarios - Worst: ${regret_data['price_scenarios']['worst_case']:.0f}")
    print(f"   Supply Risk: {regret_data['risk_metrics']['supply_risk']:.2f}")
    print(f"   Regulatory Risk: {regret_data['risk_metrics']['regulatory_risk']:.2f}")
    
    # ============================================================
    # 7. Quantum Elasticity Bridge Module
    # ============================================================
    print("\n" + "=" * 80)
    print("7. Quantum Elasticity Bridge")
    print("=" * 80)
    
    quantum_data = collector.export_for_quantum_bridge()
    print(f"   Hamiltonian Factors: {len(quantum_data['hamiltonian_factors'])}")
    print(f"   Quantum Advantage Expected: {quantum_data['quantum_advantage_expected']}")
    print(f"   Market Regime: {quantum_data['market_regime']}")
    
    # ============================================================
    # Summary
    # ============================================================
    print("\n" + "=" * 80)
    print("INTEGRATION SUMMARY")
    print("=" * 80)
    print(f"✅ 7 modules successfully integrated")
    print(f"✅ 22 data fields available")
    print(f"✅ Enhanced dataset with {len(collector.records)} records")
    print(f"✅ All modules ready for production")
    
    return True

if __name__ == "__main__":
    asyncio.run(run_unified_integration())
