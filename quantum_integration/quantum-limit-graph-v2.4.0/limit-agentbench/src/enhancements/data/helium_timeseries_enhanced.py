# File: src/enhancements/data/helium_timeseries_enhanced.py
"""
Enhanced Helium Timeseries Dataset Generator - Version 2.0
Produces complete dataset with 22 fields for all Green Agent modules
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

class EnhancedHeliumDatasetGenerator:
    """Generate complete helium timeseries dataset for all modules"""
    
    def __init__(self, seed: int = 42):
        self.seed = seed
        np.random.seed(seed)
    
    def generate(self, n_periods: int = 120, start_date: str = "2020-01-01") -> pd.DataFrame:
        """Generate comprehensive helium dataset"""
        
        dates = pd.date_range(start=start_date, periods=n_periods, freq='M')
        t = np.arange(n_periods)
        
        # ============================================================
        # CORE ECONOMIC PARAMETERS
        # ============================================================
        
        # Production (mean-reverting with slight decline)
        production = 28000 - t * 40 + np.random.normal(0, 300, n_periods)
        production = np.clip(production, 20000, 35000)
        
        # Demand (increasing trend)
        demand = 27000 + t * 80 + np.random.normal(0, 400, n_periods)
        demand = np.clip(demand, 25000, 45000)
        
        # Price index (geometric Brownian motion with seasonality)
        price = 100 * np.exp(np.cumsum(np.random.normal(0.005, 0.1, n_periods)))
        seasonal = 1 + 0.1 * np.sin(2 * np.pi * t / 12)
        price = price * seasonal
        price = np.clip(price, 50, 500)
        
        # Demand-supply ratio
        demand_supply_ratio = demand / production
        
        # Shortage severity
        shortage = np.clip((demand_supply_ratio - 0.95) * 4, 0.05, 1.0)
        
        # Supply risk (increasing with cycles)
        supply_risk = 0.2 + t * 0.002 + 0.1 * np.sin(2 * np.pi * t / 24) + np.random.normal(0, 0.05, n_periods)
        supply_risk = np.clip(supply_risk, 0.1, 0.9)
        
        # Recycling rate (improving over time)
        recycling = 0.10 + t * 0.003 + np.random.normal(0, 0.01, n_periods)
        recycling = np.clip(recycling, 0.05, 0.40)
        
        # Substitution feasibility
        substitution = 0.08 + t * 0.004 + np.random.normal(0, 0.01, n_periods)
        substitution = np.clip(substitution, 0.05, 0.50)
        
        # Cooling load sensitivity
        cooling = 0.85 + t * 0.005 + np.random.normal(0, 0.02, n_periods)
        cooling = np.clip(cooling, 0.7, 1.3)
        
        # Geopolitical risk (cyclical)
        geo_risk = 0.3 + 0.2 * np.sin(2 * np.pi * t / 36) + np.random.normal(0, 0.05, n_periods)
        geo_risk = np.clip(geo_risk, 0.1, 0.8)
        
        # Logistics disruption
        logistics = 0.2 + t * 0.001 + np.random.normal(0, 0.05, n_periods)
        logistics = np.clip(logistics, 0.1, 0.7)
        
        # ============================================================
        # ENHANCED FIELDS (v2.0)
        # ============================================================
        
        # New production capacity
        new_capacity = 2000 + t * 100 + np.random.normal(0, 200, n_periods)
        new_capacity = np.maximum(500, new_capacity)
        
        # Helium scarcity impact
        scarcity_impact = shortage * 0.6 + supply_risk * 0.4
        scarcity_impact = np.clip(scarcity_impact, 0, 1)
        
        # Price volatility (rolling standard deviation)
        price_volatility = pd.Series(price).rolling(window=6).std().fillna(5).values
        price_volatility = np.clip(price_volatility, 1, 30)
        
        # Market regime classification
        market_regime = []
        for sc in scarcity_impact:
            if sc > 0.7:
                regime = "crisis"
            elif sc > 0.5:
                regime = "tightening"
            elif sc > 0.3:
                regime = "normal"
            else:
                regime = "stable"
            market_regime.append(regime)
        
        # Carbon intensity (gCO2/kWh)
        carbon_intensity = 300 + 200 * scarcity_impact + np.random.normal(0, 50, n_periods)
        carbon_intensity = np.clip(carbon_intensity, 50, 800)
        
        # Renewable energy percentage
        renewable_pct = 30 + 40 * (1 - scarcity_impact) + np.random.normal(0, 10, n_periods)
        renewable_pct = np.clip(renewable_pct, 5, 95)
        
        # Derived metrics
        circularity_potential = (recycling + substitution) / 2
        thermal_impact = cooling * scarcity_impact
        future_supply_potential = np.clip((new_capacity / production) * 100, 0, 50)
        capacity_utilization = production / (production + new_capacity)
        
        # ESG score
        esg_score = (recycling * 40 + (1 - supply_risk) * 30 + (1 - geo_risk) * 30) * 100
        esg_score = np.clip(esg_score, 0, 100)
        
        # Regulatory risk
        regulatory_risk = geo_risk * 0.5 + logistics * 0.5
        regulatory_risk = np.clip(regulatory_risk, 0, 1)
        
        # ============================================================
        # CREATE DATAFRAME
        # ============================================================
        
        df = pd.DataFrame({
            'date': dates,
            'global_production_tonnes': np.round(production, 0),
            'global_demand_tonnes': np.round(demand, 0),
            'price_index': np.round(price, 1),
            'shortage_severity_0_1': np.round(shortage, 3),
            'supply_risk_score_0_1': np.round(supply_risk, 3),
            'recycling_rate_0_1': np.round(recycling, 3),
            'substitution_feasibility_0_1': np.round(substitution, 3),
            'cooling_load_sensitivity': np.round(cooling, 3),
            'geopolitical_risk_index': np.round(geo_risk, 3),
            'logistics_disruption_index': np.round(logistics, 3),
            'new_production_capacity_tonnes': np.round(new_capacity, 0),
            'helium_scarcity_impact': np.round(scarcity_impact, 3),
            'price_volatility': np.round(price_volatility, 2),
            'market_regime': market_regime,
            'carbon_intensity_associated': np.round(carbon_intensity, 0),
            'renewable_energy_pct': np.round(renewable_pct, 1),
            'demand_supply_ratio': np.round(demand_supply_ratio, 3),
            'circularity_potential': np.round(circularity_potential, 3),
            'thermal_impact_factor': np.round(thermal_impact, 3),
            'future_supply_potential_pct': np.round(future_supply_potential, 1),
            'capacity_utilization_rate': np.round(capacity_utilization, 3),
            'esg_score': np.round(esg_score, 1),
            'regulatory_risk_score': np.round(regulatory_risk, 3)
        })
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, output_path: str = "./helium_timeseries_enhanced.csv"):
        """Save dataset to CSV"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"Dataset saved to {output_path}")
        return output_path

# Generate and save the dataset
if __name__ == "__main__":
    generator = EnhancedHeliumDatasetGenerator()
    df = generator.generate(n_periods=120)
    generator.save_to_csv(df)
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
