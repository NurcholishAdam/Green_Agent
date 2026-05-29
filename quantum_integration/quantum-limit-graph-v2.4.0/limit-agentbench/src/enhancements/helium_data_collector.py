# File: src/enhancements/helium_data_collector.py

"""
Helium Data Collector for Green Agent - Version 1.0

Purpose:
- Load and manage helium market data from curated CSV
- Provide feature vectors for enhancement modules
- Support integration with synthetic data manager, sustainability signals, and regret optimizer
- Enable helium-aware decision making across the Green Agent system

Data Sources:
- USGS Helium Statistics: https://www.usgs.gov/centers/national-minerals-information-center/helium-statistics-and-information
- Helium Market Overview: https://www.helium-one.com/helium/helium-market/
- Shortage Analysis: https://www.innovationnewsnetwork.com/exploring-the-helium-shortage-in-2025/57119/
- Recycling Research: https://www.sciencedirect.com/science/article/abs/pii/S0921344923000721
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import csv
import datetime as dt
import numpy as np
import pandas as pd
import json
import logging
import hashlib
from collections import defaultdict

logger = logging.getLogger(__name__)

# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class HeliumRecord:
    """Single time-step record of helium market data"""
    date: dt.date
    global_production_tonnes: float
    global_demand_tonnes: float
    price_index: float  # Normalized to 100 in 2020
    shortage_severity_0_1: float  # 0 = normal, 1 = severe
    supply_risk_score_0_1: float  # Composite supply risk
    recycling_rate_0_1: float  # Circular economy proxy
    substitution_feasibility_0_1: float  # Material substitution potential
    cooling_load_sensitivity: float  # Impact on thermal constraints
    geopolitical_risk_index: float = 0.5  # Geopolitical risk factor
    logistics_disruption_index: float = 0.3  # Supply chain disruption
    
    @property
    def demand_supply_ratio(self) -> float:
        """Calculate demand/supply ratio"""
        return self.global_demand_tonnes / max(self.global_production_tonnes, 1e-6)
    
    @property
    def scarcity_index(self) -> float:
        """Composite scarcity index (0-1)"""
        return min(1.0, (
            self.shortage_severity_0_1 * 0.4 +
            self.supply_risk_score_0_1 * 0.3 +
            (self.demand_supply_ratio - 1) * 0.3
        ))
    
    @property
    def circularity_potential(self) -> float:
        """Combined circularity potential"""
        return (self.recycling_rate_0_1 + self.substitution_feasibility_0_1) / 2
    
    @property
    def thermal_impact_factor(self) -> float:
        """How much helium scarcity amplifies thermal constraints"""
        return self.cooling_load_sensitivity * self.scarcity_index
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'date': self.date.isoformat(),
            'global_production_tonnes': self.global_production_tonnes,
            'global_demand_tonnes': self.global_demand_tonnes,
            'price_index': self.price_index,
            'shortage_severity_0_1': self.shortage_severity_0_1,
            'supply_risk_score_0_1': self.supply_risk_score_0_1,
            'recycling_rate_0_1': self.recycling_rate_0_1,
            'substitution_feasibility_0_1': self.substitution_feasibility_0_1,
            'cooling_load_sensitivity': self.cooling_load_sensitivity,
            'demand_supply_ratio': self.demand_supply_ratio,
            'scarcity_index': self.scarcity_index,
            'circularity_potential': self.circularity_potential,
            'thermal_impact_factor': self.thermal_impact_factor
        }
    
    def to_feature_vector(self) -> np.ndarray:
        """Convert to feature vector for ML models"""
        return np.array([
            self.global_production_tonnes / 30000,  # Normalize
            self.demand_supply_ratio,
            self.price_index / 200,  # Normalize
            self.shortage_severity_0_1,
            self.supply_risk_score_0_1,
            self.recycling_rate_0_1,
            self.substitution_feasibility_0_1,
            self.cooling_load_sensitivity,
            self.geopolitical_risk_index,
            self.logistics_disruption_index
        ])

@dataclass
class HeliumDataset:
    """Complete helium dataset with derived features"""
    records: List[HeliumRecord] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    
    @property
    def latest(self) -> Optional[HeliumRecord]:
        """Get latest record"""
        return self.records[-1] if self.records else None
    
    @property
    def timeseries_length(self) -> int:
        """Number of time steps"""
        return len(self.records)
    
    def get_trends(self) -> Dict:
        """Calculate trends in helium metrics"""
        if len(self.records) < 2:
            return {}
        
        first = self.records[0]
        last = self.records[-1]
        
        return {
            'production_change_pct': ((last.global_production_tonnes - first.global_production_tonnes) / 
                                     max(first.global_production_tonnes, 1)) * 100,
            'demand_change_pct': ((last.global_demand_tonnes - first.global_demand_tonnes) / 
                                 max(first.global_demand_tonnes, 1)) * 100,
            'price_change_pct': ((last.price_index - first.price_index) / 
                                max(first.price_index, 1)) * 100,
            'scarcity_trend': 'increasing' if last.scarcity_index > first.scarcity_index else 'decreasing',
            'circularity_improvement': last.circularity_potential - first.circularity_potential,
            'thermal_risk_trend': 'increasing' if last.thermal_impact_factor > first.thermal_impact_factor else 'decreasing'
        }
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        return pd.DataFrame([r.to_dict() for r in self.records])
    
    def to_feature_matrix(self) -> np.ndarray:
        """Convert to feature matrix for ML"""
        return np.array([r.to_feature_vector() for r in self.records])

# ============================================================
# DATA COLLECTOR
# ============================================================

class HeliumDataCollector:
    """
    Collects and manages helium market data.
    
    Features:
    - Load from CSV
    - Generate synthetic data when CSV unavailable
    - Provide feature vectors for ML models
    - Export data for other modules
    - Track data quality and freshness
    """
    
    BASE_DIR = Path(__file__).resolve().parent
    DEFAULT_DATA_PATH = BASE_DIR / "data" / "helium_timeseries.csv"
    
    def __init__(self, csv_path: Optional[Path] = None):
        self.csv_path = csv_path or self.DEFAULT_DATA_PATH
        self.dataset: Optional[HeliumDataset] = None
        self._load_or_generate()
        
        logger.info(f"HeliumDataCollector initialized with {self.dataset.timeseries_length} records")
    
    def _load_or_generate(self):
        """Load CSV or generate synthetic data"""
        try:
            self.dataset = self._load_from_csv()
            logger.info(f"Loaded helium data from {self.csv_path}")
        except (FileNotFoundError, Exception) as e:
            logger.warning(f"Could not load CSV: {e}. Generating synthetic data.")
            self.dataset = self._generate_synthetic_data()
    
    def _load_from_csv(self) -> HeliumDataset:
        """Load helium data from CSV file"""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Helium data file not found: {self.csv_path}")
        
        records = []
        with open(self.csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(HeliumRecord(
                    date=dt.datetime.fromisoformat(row['date']).date(),
                    global_production_tonnes=float(row['global_production_tonnes']),
                    global_demand_tonnes=float(row['global_demand_tonnes']),
                    price_index=float(row['price_index']),
                    shortage_severity_0_1=float(row['shortage_severity_0_1']),
                    supply_risk_score_0_1=float(row['supply_risk_score_0_1']),
                    recycling_rate_0_1=float(row['recycling_rate_0_1']),
                    substitution_feasibility_0_1=float(row['substitution_feasibility_0_1']),
                    cooling_load_sensitivity=float(row['cooling_load_sensitivity']),
                    geopolitical_risk_index=float(row.get('geopolitical_risk_index', 0.5)),
                    logistics_disruption_index=float(row.get('logistics_disruption_index', 0.3))
                ))
        
        records.sort(key=lambda r: r.date)
        
        return HeliumDataset(
            records=records,
            metadata={
                'source': 'CSV',
                'file': str(self.csv_path),
                'loaded_at': dt.datetime.now().isoformat(),
                'record_count': len(records)
            }
        )
    
    def _generate_synthetic_data(self) -> HeliumDataset:
        """Generate synthetic helium data based on known trends"""
        
        # Known trends from public sources:
        # - Production ~25,000-30,000 tonnes/year
        # - Prices have risen significantly (2-3x since 2020)
        # - Shortages increasing
        # - Recycling rates slowly improving
        
        np.random.seed(42)
        
        start_date = dt.date(2020, 1, 1)
        n_periods = 24  # 2 years of monthly data
        
        records = []
        
        for i in range(n_periods):
            date = start_date + dt.timedelta(days=30 * i)
            
            # Production: slowly increasing with seasonal variation
            production = 25000 + i * 200 + np.random.normal(0, 500)
            
            # Demand: growing faster than production
            demand = 24000 + i * 250 + np.random.normal(0, 300)
            
            # Price: rising with shortages
            price = 100 + i * 2.5 + np.random.normal(0, 5)
            
            # Shortage severity: increasing
            shortage = min(1.0, 0.1 + i * 0.035 + np.random.uniform(-0.05, 0.05))
            
            # Supply risk: increasing
            supply_risk = min(1.0, 0.2 + i * 0.025 + np.random.uniform(-0.03, 0.03))
            
            # Recycling rate: slowly improving
            recycling = min(0.25, 0.10 + i * 0.005 + np.random.uniform(-0.01, 0.01))
            
            # Substitution feasibility: improving with technology
            substitution = min(0.25, 0.05 + i * 0.007 + np.random.uniform(-0.01, 0.01))
            
            # Cooling sensitivity: increasing with scarcity
            cooling = 0.8 + i * 0.012 + np.random.uniform(-0.02, 0.02)
            
            records.append(HeliumRecord(
                date=date,
                global_production_tonnes=max(0, production),
                global_demand_tonnes=max(0, demand),
                price_index=max(50, price),
                shortage_severity_0_1=np.clip(shortage, 0, 1),
                supply_risk_score_0_1=np.clip(supply_risk, 0, 1),
                recycling_rate_0_1=np.clip(recycling, 0, 1),
                substitution_feasibility_0_1=np.clip(substitution, 0, 1),
                cooling_load_sensitivity=cooling,
                geopolitical_risk_index=np.clip(0.3 + i * 0.01, 0, 1),
                logistics_disruption_index=np.clip(0.2 + i * 0.008, 0, 1)
            ))
        
        return HeliumDataset(
            records=records,
            metadata={
                'source': 'synthetic',
                'generated_at': dt.datetime.now().isoformat(),
                'record_count': len(records),
                'note': 'Synthetic data based on public helium market trends'
            }
        )
    
    def get_latest(self) -> Optional[HeliumRecord]:
        """Get latest helium record"""
        return self.dataset.latest if self.dataset else None
    
    def get_record_by_date(self, date: dt.date) -> Optional[HeliumRecord]:
        """Get record for specific date"""
        for record in self.dataset.records:
            if record.date == date:
                return record
        return None
    
    def get_feature_vector(self) -> np.ndarray:
        """Get latest feature vector for ML models"""
        latest = self.get_latest()
        if latest:
            return latest.to_feature_vector()
        return np.zeros(10)
    
    def get_timeseries_dataframe(self) -> pd.DataFrame:
        """Get complete timeseries as DataFrame"""
        if self.dataset:
            return self.dataset.to_dataframe()
        return pd.DataFrame()
    
    def get_feature_matrix(self) -> np.ndarray:
        """Get feature matrix for ML training"""
        if self.dataset:
            return self.dataset.to_feature_matrix()
        return np.array([])
    
    def get_trends(self) -> Dict:
        """Get helium market trends"""
        if self.dataset:
            return self.dataset.get_trends()
        return {}
    
    def export_for_synthetic_manager(self) -> Dict:
        """
        Export data in format compatible with synthetic_data_manager.py
        for generating extended helium scenarios.
        """
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'helium_features': latest.to_dict(),
            'timeseries': self.get_timeseries_dataframe().to_dict('records'),
            'trends': self.get_trends(),
            'feature_matrix': self.get_feature_matrix().tolist() if len(self.get_feature_matrix()) > 0 else [],
            'metadata': {
                'source': 'helium_data_collector',
                'exported_at': dt.datetime.now().isoformat(),
                'record_count': self.dataset.timeseries_length if self.dataset else 0
            }
        }
    
    def export_for_sustainability_signals(self) -> Dict:
        """
        Export data in format compatible with sustainability_signals.py.
        """
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'helium_scarcity_signal': {
                'scarcity_index': latest.scarcity_index,
                'shortage_severity': latest.shortage_severity_0_1,
                'supply_risk': latest.supply_risk_score_0_1,
                'demand_supply_ratio': latest.demand_supply_ratio
            },
            'helium_circularity_signal': {
                'recycling_rate': latest.recycling_rate_0_1,
                'substitution_feasibility': latest.substitution_feasibility_0_1,
                'circularity_potential': latest.circularity_potential
            },
            'helium_thermal_signal': {
                'cooling_load_sensitivity': latest.cooling_load_sensitivity,
                'thermal_impact_factor': latest.thermal_impact_factor,
                'price_index': latest.price_index
            },
            'metadata': {
                'source': 'helium_data_collector',
                'date': latest.date.isoformat(),
                'trends': self.get_trends()
            }
        }
    
    def export_for_regret_optimizer(self) -> Dict:
        """
        Export data in format compatible with regret_optimizer.py.
        """
        latest = self.get_latest()
        trends = self.get_trends()
        
        return {
            'helium_price_index': latest.price_index if latest else 100,
            'helium_scarcity_index': latest.scarcity_index if latest else 0.5,
            'helium_supply_risk': latest.supply_risk_score_0_1 if latest else 0.5,
            'helium_demand_supply_ratio': latest.demand_supply_ratio if latest else 1.0,
            'helium_recycling_rate': latest.recycling_rate_0_1 if latest else 0.15,
            'helium_trend': trends.get('scarcity_trend', 'stable'),
            'helium_volatility': trends.get('price_change_pct', 0) / 100,
            'metadata': {
                'source': 'helium_data_collector',
                'exported_at': dt.datetime.now().isoformat()
            }
        }

# ============================================================
# MODULE INITIALIZATION
# ============================================================

# Singleton instance for easy import
_collector_instance = None

def get_helium_collector(csv_path: Optional[Path] = None) -> HeliumDataCollector:
    """Get or create the singleton HeliumDataCollector instance"""
    global _collector_instance
    if _collector_instance is None:
        _collector_instance = HeliumDataCollector(csv_path)
    return _collector_instance

# ============================================================
# TESTING AND DEMONSTRATION
# ============================================================

if __name__ == "__main__":
    print("=" * 80)
    print("Helium Data Collector - Demonstration")
    print("=" * 80)
    
    # Initialize collector
    collector = HeliumDataCollector()
    
    # Show latest data
    latest = collector.get_latest()
    if latest:
        print(f"\n📊 Latest Helium Data ({latest.date}):")
        print(f"   Production: {latest.global_production_tonnes:,.0f} tonnes")
        print(f"   Demand: {latest.global_demand_tonnes:,.0f} tonnes")
        print(f"   Price Index: {latest.price_index:.0f}")
        print(f"   Shortage Severity: {latest.shortage_severity_0_1:.2f}")
        print(f"   Supply Risk: {latest.supply_risk_score_0_1:.2f}")
        print(f"   Recycling Rate: {latest.recycling_rate_0_1:.2f}")
        print(f"   Substitution Feasibility: {latest.substitution_feasibility_0_1:.2f}")
        print(f"   Demand/Supply Ratio: {latest.demand_supply_ratio:.2f}")
        print(f"   Scarcity Index: {latest.scarcity_index:.2f}")
        print(f"   Circularity Potential: {latest.circularity_potential:.2f}")
        print(f"   Thermal Impact Factor: {latest.thermal_impact_factor:.2f}")
    
    # Show trends
    trends = collector.get_trends()
    if trends:
        print(f"\n📈 Helium Market Trends:")
        for key, value in trends.items():
            if isinstance(value, float):
                print(f"   {key}: {value:.2f}")
            else:
                print(f"   {key}: {value}")
    
    # Show feature vector
    features = collector.get_feature_vector()
    print(f"\n🧬 Feature Vector (10 dimensions):")
    feature_names = [
        'production_norm', 'demand_supply_ratio', 'price_norm',
        'shortage_severity', 'supply_risk', 'recycling_rate',
        'substitution_feasibility', 'cooling_sensitivity',
        'geopolitical_risk', 'logistics_disruption'
    ]
    for name, value in zip(feature_names, features):
        print(f"   {name}: {value:.4f}")
    
    # Export examples
    print(f"\n🔗 Integration Exports:")
    print(f"   Regret Optimizer: {len(collector.export_for_regret_optimizer())} fields")
    print(f"   Sustainability Signals: {len(collector.export_for_sustainability_signals())} signal groups")
    print(f"   Synthetic Manager: {len(collector.export_for_synthetic_manager())} data groups")
    
    print("\n" + "=" * 80)
    print("✅ Helium Data Collector Ready")
    print("=" * 80)
