# File: src/enhancements/helium_data_collector_enhanced.py
"""
Enhanced Helium Data Collector with Complete Feature Set - Version 3.0
Integrates all 22 fields from enhanced dataset
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HeliumRecordEnhanced:
    """Complete helium record with all 22 fields"""
    date: datetime
    global_production_tonnes: float
    global_demand_tonnes: float
    price_index: float
    shortage_severity_0_1: float
    supply_risk_score_0_1: float
    recycling_rate_0_1: float
    substitution_feasibility_0_1: float
    cooling_load_sensitivity: float
    geopolitical_risk_index: float
    logistics_disruption_index: float
    new_production_capacity_tonnes: float
    helium_scarcity_impact: float
    price_volatility: float
    market_regime: str
    carbon_intensity_associated: float
    renewable_energy_pct: float
    demand_supply_ratio: float
    circularity_potential: float
    thermal_impact_factor: float
    future_supply_potential_pct: float
    capacity_utilization_rate: float
    esg_score: float
    regulatory_risk_score: float
    
    @property
    def scarcity_index(self) -> float:
        """Alias for helium_scarcity_impact"""
        return self.helium_scarcity_impact
    
    @property
    def recycling_rate(self) -> float:
        """Alias for recycling_rate_0_1"""
        return self.recycling_rate_0_1
    
    def to_dict(self) -> Dict:
        return {k: v.isoformat() if isinstance(v, datetime) else v 
                for k, v in self.__dict__.items()}
    
    def to_feature_vector(self) -> np.ndarray:
        """11-dimensional feature vector for ML models"""
        return np.array([
            self.global_production_tonnes / 50000,
            self.demand_supply_ratio,
            self.price_index / 500,
            self.shortage_severity_0_1,
            self.supply_risk_score_0_1,
            self.recycling_rate_0_1,
            self.substitution_feasibility_0_1,
            self.cooling_load_sensitivity,
            self.geopolitical_risk_index,
            self.logistics_disruption_index,
            self.new_production_capacity_tonnes / 20000
        ])

class EnhancedHeliumDataCollector:
    """
    Complete helium data collector with all 22 fields
    Integrates with all Green Agent modules
    """
    
    def __init__(self, csv_path: str = "./helium_timeseries_enhanced.csv"):
        self.csv_path = Path(csv_path)
        self.records: List[HeliumRecordEnhanced] = []
        self._load_data()
    
    def _load_data(self):
        """Load enhanced dataset"""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.csv_path}")
        
        df = pd.read_csv(self.csv_path)
        df['date'] = pd.to_datetime(df['date'])
        
        for _, row in df.iterrows():
            record = HeliumRecordEnhanced(
                date=row['date'],
                global_production_tonnes=float(row['global_production_tonnes']),
                global_demand_tonnes=float(row['global_demand_tonnes']),
                price_index=float(row['price_index']),
                shortage_severity_0_1=float(row['shortage_severity_0_1']),
                supply_risk_score_0_1=float(row['supply_risk_score_0_1']),
                recycling_rate_0_1=float(row['recycling_rate_0_1']),
                substitution_feasibility_0_1=float(row['substitution_feasibility_0_1']),
                cooling_load_sensitivity=float(row['cooling_load_sensitivity']),
                geopolitical_risk_index=float(row['geopolitical_risk_index']),
                logistics_disruption_index=float(row['logistics_disruption_index']),
                new_production_capacity_tonnes=float(row['new_production_capacity_tonnes']),
                helium_scarcity_impact=float(row['helium_scarcity_impact']),
                price_volatility=float(row['price_volatility']),
                market_regime=row['market_regime'],
                carbon_intensity_associated=float(row['carbon_intensity_associated']),
                renewable_energy_pct=float(row['renewable_energy_pct']),
                demand_supply_ratio=float(row['demand_supply_ratio']),
                circularity_potential=float(row['circularity_potential']),
                thermal_impact_factor=float(row['thermal_impact_factor']),
                future_supply_potential_pct=float(row['future_supply_potential_pct']),
                capacity_utilization_rate=float(row['capacity_utilization_rate']),
                esg_score=float(row['esg_score']),
                regulatory_risk_score=float(row['regulatory_risk_score'])
            )
            self.records.append(record)
        
        logger.info(f"Loaded {len(self.records)} enhanced records with {len(self.records[0].__dict__)} fields")
    
    def get_latest(self) -> Optional[HeliumRecordEnhanced]:
        """Get most recent record"""
        return self.records[-1] if self.records else None
    
    def get_historical(self, days: int = 365) -> List[HeliumRecordEnhanced]:
        """Get historical records within date range"""
        cutoff = datetime.now() - timedelta(days=days)
        return [r for r in self.records if r.date > cutoff]
    
    def get_feature_matrix(self) -> np.ndarray:
        """Get feature matrix for ML training"""
        return np.array([r.to_feature_vector() for r in self.records])
    
    def get_timeseries_dataframe(self) -> pd.DataFrame:
        """Get complete dataset as DataFrame"""
        return pd.DataFrame([r.to_dict() for r in self.records])
    
    # ============================================================
    # EXPORT FUNCTIONS FOR ALL MODULES
    # ============================================================
    
    def export_for_elasticity(self) -> Dict:
        """Export data for helium_elasticity module"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'price_elasticity': -0.4 * (1 + latest.helium_scarcity_impact * 0.5),
            'scarcity_elasticity': 0.6 * (1 - latest.capacity_utilization_rate),
            'cross_elasticity': 0.3 * (1 - latest.substitution_feasibility_0_1),
            'thermal_elasticity': latest.thermal_impact_factor,
            'composite_elasticity': (0.4 * (1 + latest.helium_scarcity_impact * 0.3) +
                                     0.3 * latest.circularity_potential +
                                     0.3 * latest.regulatory_risk_score),
            'market_regime': latest.market_regime,
            'carbon_price_sensitivity': latest.esg_score / 100,
            'renewable_integration': latest.renewable_energy_pct / 100,
            'capacity_impact': latest.future_supply_potential_pct / 100
        }
    
    def export_for_circularity(self) -> Dict:
        """Export data for helium_circularity module"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'recycling_rate': latest.recycling_rate_0_1,
            'recovery_efficiency': 0.85,
            'circularity_index': latest.circularity_potential,
            'closed_loop_score': latest.circularity_potential * latest.recycling_rate_0_1,
            'material_circularity_indicator': (latest.recycling_rate_0_1 + latest.substitution_feasibility_0_1) / 2,
            'lifecycle_extension_potential': latest.future_supply_potential_pct / 50,
            'circular_economy_roi': (latest.esg_score / 100) * 0.15,
            'waste_heat_recovery_potential': latest.thermal_impact_factor * 100,
            'industrial_symbiosis_score': latest.capacity_utilization_rate * 0.8
        }
    
    def export_for_forecaster(self) -> Dict:
        """Export data for helium_forecaster module"""
        return {
            'training_data': {
                'feature_matrix': self.get_feature_matrix().tolist(),
                'target_prices': [r.price_index for r in self.records],
                'target_capacities': [r.new_production_capacity_tonnes for r in self.records],
                'feature_names': ['production_norm', 'demand_supply', 'price_norm', 'shortage',
                                 'supply_risk', 'recycling', 'substitution', 'cooling',
                                 'geopolitical', 'logistics', 'new_capacity_norm'],
                'market_regimes': [r.market_regime for r in self.records]
            },
            'latest_features': self.get_latest().to_feature_vector().tolist() if self.get_latest() else [],
            'trends': {
                'price_trend': 'increasing' if len(self.records) > 1 and self.records[-1].price_index > self.records[-2].price_index else 'decreasing',
                'scarcity_trend': 'increasing' if len(self.records) > 1 and self.records[-1].helium_scarcity_impact > self.records[-2].helium_scarcity_impact else 'decreasing',
                'circularity_trend': 'improving' if len(self.records) > 1 and self.records[-1].circularity_potential > self.records[-2].circularity_potential else 'worsening'
            },
            'capacity_forecast': {
                'current': self.get_latest().new_production_capacity_tonnes if self.get_latest() else 0,
                'trend': self._calculate_capacity_trend(),
                'forecast_6m': self._forecast_capacity(6),
                'forecast_12m': self._forecast_capacity(12)
            }
        }
    
    def export_for_sustainability(self) -> Dict:
        """Export data for sustainability_signals module"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'esg_score': latest.esg_score,
            'carbon_intensity': latest.carbon_intensity_associated,
            'renewable_energy_pct': latest.renewable_energy_pct,
            'circularity_score': latest.circularity_potential * 100,
            'supply_chain_risk': latest.supply_risk_score_0_1,
            'geopolitical_risk': latest.geopolitical_risk_index,
            'regulatory_risk': latest.regulatory_risk_score,
            'market_regime': latest.market_regime,
            'future_supply_potential': latest.future_supply_potential_pct,
            'capacity_utilization': latest.capacity_utilization_rate
        }
    
    def export_for_thermal(self) -> Dict:
        """Export data for thermal_optimizer module"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'cooling_load_sensitivity': latest.cooling_load_sensitivity,
            'thermal_impact_factor': latest.thermal_impact_factor,
            'helium_scarcity_impact': latest.helium_scarcity_impact,
            'carbon_intensity': latest.carbon_intensity_associated,
            'renewable_energy_pct': latest.renewable_energy_pct,
            'cooling_cost_index': latest.price_index / 100,
            'free_cooling_potential': 1 - latest.helium_scarcity_impact,
            'waste_heat_recovery': latest.thermal_impact_factor * 0.5
        }
    
    def export_for_regret_optimizer(self) -> Dict:
        """Export data for regret_optimizer module"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'price_scenarios': {
                'base': latest.price_index,
                'best_case': latest.price_index * 0.8,
                'worst_case': latest.price_index * 1.3,
                'volatility': latest.price_volatility
            },
            'carbon_scenarios': {
                'base': latest.carbon_intensity_associated,
                'best_case': latest.carbon_intensity_associated * 0.7,
                'worst_case': latest.carbon_intensity_associated * 1.5
            },
            'supply_scenarios': {
                'current': latest.global_production_tonnes,
                'with_new_capacity': latest.global_production_tonnes + latest.new_production_capacity_tonnes,
                'future_potential': latest.future_supply_potential_pct
            },
            'risk_metrics': {
                'supply_risk': latest.supply_risk_score_0_1,
                'geopolitical_risk': latest.geopolitical_risk_index,
                'regulatory_risk': latest.regulatory_risk_score,
                'price_volatility': latest.price_volatility
            }
        }
    
    def export_for_quantum_bridge(self) -> Dict:
        """Export data for quantum_elasticity_bridge module"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'hamiltonian_factors': {
                'price': latest.price_index / 500,
                'scarcity': latest.helium_scarcity_impact,
                'supply_risk': latest.supply_risk_score_0_1,
                'demand_supply': latest.demand_supply_ratio,
                'geopolitical': latest.geopolitical_risk_index,
                'logistics': latest.logistics_disruption_index,
                'new_capacity': latest.new_production_capacity_tonnes / 20000,
                'recycling': latest.recycling_rate_0_1,
                'substitution': latest.substitution_feasibility_0_1,
                'cooling': latest.cooling_load_sensitivity,
                'esg': latest.esg_score / 100
            },
            'market_regime': latest.market_regime,
            'quantum_advantage_expected': latest.price_volatility > 15
        }
    
    def _calculate_capacity_trend(self) -> str:
        """Calculate capacity trend direction"""
        if len(self.records) < 6:
            return "stable"
        
        recent = [r.new_production_capacity_tonnes for r in self.records[-6:]]
        if recent[-1] > recent[0] * 1.1:
            return "increasing"
        elif recent[-1] < recent[0] * 0.9:
            return "decreasing"
        return "stable"
    
    def _forecast_capacity(self, months_ahead: int) -> float:
        """Simple capacity forecast"""
        if len(self.records) < 12:
            return self.get_latest().new_production_capacity_tonnes if self.get_latest() else 0
        
        recent = [r.new_production_capacity_tonnes for r in self.records[-12:]]
        monthly_growth = (recent[-1] - recent[0]) / 12
        return recent[-1] + monthly_growth * months_ahead
    
    def health_check(self) -> Dict:
        """Health check for control system"""
        return {
            'data_loaded': len(self.records) > 0,
            'record_count': len(self.records),
            'date_range': {
                'start': self.records[0].date.isoformat() if self.records else None,
                'end': self.records[-1].date.isoformat() if self.records else None
            },
            'fields_available': list(HeliumRecordEnhanced.__dataclass_fields__.keys()),
            'latest_scarcity': self.get_latest().helium_scarcity_impact if self.get_latest() else None,
            'latest_esg_score': self.get_latest().esg_score if self.get_latest() else None,
            'integrations_ready': [
                'helium_elasticity', 'helium_circularity', 'helium_forecaster',
                'sustainability_signals', 'thermal_optimizer', 'regret_optimizer',
                'quantum_elasticity_bridge'
            ]
        }

# Singleton instance
_collector = None

def get_enhanced_helium_collector() -> EnhancedHeliumDataCollector:
    """Get singleton collector instance"""
    global _collector
    if _collector is None:
        _collector = EnhancedHeliumDataCollector()
    return _collector


# ============================================================
# TEST AND DEMONSTRATION
# ============================================================

def main():
    """Test the enhanced collector with all exports"""
    print("=" * 80)
    print("Enhanced Helium Data Collector v3.0 - Complete Integration Test")
    print("=" * 80)
    
    # Initialize collector
    collector = get_enhanced_helium_collector()
    
    # Display record info
    latest = collector.get_latest()
    print(f"\n📊 Latest Record ({latest.date.date()}):")
    print(f"   Production: {latest.global_production_tonnes:,.0f} tonnes")
    print(f"   Demand: {latest.global_demand_tonnes:,.0f} tonnes")
    print(f"   Price Index: {latest.price_index:.1f}")
    print(f"   Scarcity Impact: {latest.helium_scarcity_impact:.3f}")
    print(f"   ESG Score: {latest.esg_score:.1f}/100")
    print(f"   Market Regime: {latest.market_regime}")
    
    # Test all exports
    print("\n🔗 Module Exports:")
    
    elasticity_data = collector.export_for_elasticity()
    print(f"   Elasticity Module: {len(elasticity_data)} fields")
    
    circularity_data = collector.export_for_circularity()
    print(f"   Circularity Module: {len(circularity_data)} fields")
    
    forecaster_data = collector.export_for_forecaster()
    print(f"   Forecaster Module: {len(forecaster_data)} fields")
    
    sustainability_data = collector.export_for_sustainability()
    print(f"   Sustainability Module: {len(sustainability_data)} fields")
    
    thermal_data = collector.export_for_thermal()
    print(f"   Thermal Module: {len(thermal_data)} fields")
    
    regret_data = collector.export_for_regret_optimizer()
    print(f"   Regret Optimizer: {len(regret_data)} fields")
    
    quantum_data = collector.export_for_quantum_bridge()
    print(f"   Quantum Bridge: {len(quantum_data)} fields")
    
    # Feature vector
    feature_vector = latest.to_feature_vector()
    print(f"\n🧬 Feature Vector (11 dimensions):")
    for i, val in enumerate(feature_vector):
        print(f"   Dim {i+1}: {val:.4f}")
    
    # Health check
    health = collector.health_check()
    print(f"\n🏥 Health Check:")
    print(f"   Records: {health['record_count']}")
    print(f"   Integrations Ready: {len(health['integrations_ready'])}")
    
    print("\n" + "=" * 80)
    print("✅ Enhanced Helium Data Collector v3.0 - Ready for All Modules")
    print("=" * 80)

if __name__ == "__main__":
    main()
