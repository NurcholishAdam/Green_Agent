# src/enhancements/marginal_carbon.py

"""
Marginal Carbon Intensity Forecasting for Green Agent
Scientific basis: Marginal emission factors (MEF) vs average emission factors (AEF)

Reference: "Marginal vs. Average Carbon Intensity in Computing" (ACM e-Energy, 2024)
"""

import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class GeneratorType(Enum):
    """Types of electricity generators"""
    COAL = "coal"
    NATURAL_GAS = "natural_gas"
    NUCLEAR = "nuclear"
    HYDRO = "hydro"
    WIND = "wind"
    SOLAR = "solar"
    BATTERY = "battery"


@dataclass
class GeneratorCharacteristics:
    """Characteristics of a generator type"""
    co2_intensity_g_per_kwh: float
    marginal_cost_usd_per_mwh: float
    ramp_rate_mw_per_min: float
    min_output_mw: float
    max_output_mw: float


@dataclass
class MarginalCarbonForecast:
    """Forecast of marginal carbon intensity"""
    timestamp: datetime
    average_intensity_g_per_kwh: float
    marginal_intensity_g_per_kwh: float
    difference_percent: float
    recommended_action: str  # 'DEFER', 'EXECUTE_NOW', 'FOLLOW_CARBON_ZONE'
    confidence: float
    marginal_generator: GeneratorType
    forecast_horizon_hours: int


class MarginalCarbonIntensityForecaster:
    """
    Marginal Carbon Intensity (MCI) forecaster.
    
    Scientific basis: Average carbon intensity ignores marginal impact.
    MCI = ΔCO2 / ΔMWh (what actually changes when demand increases)
    
    Generator characteristics based on:
    - Coal: 820 gCO2/kWh, $30/MWh, slow ramp
    - Natural Gas: 450 gCO2/kWh, $50/MWh, medium ramp
    - Hydro: 15 gCO2/kWh, $80/MWh, fast ramp
    - Solar/Wind: 0 gCO2/kWh, $100/MWh, variable output
    """
    
    GENERATOR_DATA = {
        GeneratorType.COAL: GeneratorCharacteristics(
            co2_intensity_g_per_kwh=820.0,
            marginal_cost_usd_per_mwh=30.0,
            ramp_rate_mw_per_min=10.0,
            min_output_mw=100.0,
            max_output_mw=1000.0
        ),
        GeneratorType.NATURAL_GAS: GeneratorCharacteristics(
            co2_intensity_g_per_kwh=450.0,
            marginal_cost_usd_per_mwh=50.0,
            ramp_rate_mw_per_min=30.0,
            min_output_mw=50.0,
            max_output_mw=500.0
        ),
        GeneratorType.NUCLEAR: GeneratorCharacteristics(
            co2_intensity_g_per_kwh=12.0,
            marginal_cost_usd_per_mwh=30.0,
            ramp_rate_mw_per_min=0.5,
            min_output_mw=500.0,
            max_output_mw=1200.0
        ),
        GeneratorType.HYDRO: GeneratorCharacteristics(
            co2_intensity_g_per_kwh=15.0,
            marginal_cost_usd_per_mwh=80.0,
            ramp_rate_mw_per_min=100.0,
            min_output_mw=10.0,
            max_output_mw=500.0
        ),
        GeneratorType.WIND: GeneratorCharacteristics(
            co2_intensity_g_per_kwh=0.0,
            marginal_cost_usd_per_mwh=0.0,  # Zero marginal cost
            ramp_rate_mw_per_min=50.0,
            min_output_mw=0.0,
            max_output_mw=300.0
        ),
        GeneratorType.SOLAR: GeneratorCharacteristics(
            co2_intensity_g_per_kwh=0.0,
            marginal_cost_usd_per_mwh=0.0,
            ramp_rate_mw_per_min=50.0,
            min_output_mw=0.0,
            max_output_mw=400.0
        )
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.region = self.config.get('region', 'us-east')
        self.historical_mci_data: List[Tuple[datetime, float]] = []
        
    def forecast_marginal_intensity(self, forecast_hours: int = 24) -> MarginalCarbonForecast:
        """
        Forecast marginal carbon intensity for the next horizon.
        
        Uses:
        1. Grid generation mix forecasting
        2. Merit order dispatch modeling
        3. Historical marginal emission factors
        """
        now = datetime.now()
        
        # Get forecasted generation mix
        generation_mix = self._forecast_generation_mix(forecast_hours)
        
        # Get demand forecast
        demand_forecast = self._forecast_demand(forecast_hours)
        
        # Find marginal generator (the one that would be dispatched next)
        marginal_gen, marginal_output = self._find_marginal_generator(generation_mix, demand_forecast)
        
        # Calculate marginal intensity
        marginal_intensity = self.GENERATOR_DATA[marginal_gen].co2_intensity_g_per_kwh
        
        # Calculate average intensity from generation mix
        total_generation = sum(mix['output_mw'] for mix in generation_mix)
        total_emissions = sum(mix['output_mw'] * self.GENERATOR_DATA[mix['type']].co2_intensity_g_per_kwh 
                             for mix in generation_mix)
        average_intensity = total_emissions / total_generation if total_generation > 0 else marginal_intensity
        
        # Calculate difference
        diff_percent = ((marginal_intensity - average_intensity) / average_intensity * 100 
                       if average_intensity > 0 else 0)
        
        # Determine recommended action
        recommended_action = self._determine_action(marginal_intensity, average_intensity)
        
        # Calculate confidence (higher when near real-time)
        confidence = self._calculate_confidence(forecast_hours)
        
        # Store historical data
        self.historical_mci_data.append((now, marginal_intensity))
        if len(self.historical_mci_data) > 1000:
            self.historical_mci_data = self.historical_mci_data[-1000:]
        
        logger.info(f"MCI Forecast: avg={average_intensity:.1f}, marginal={marginal_intensity:.1f} "
                   f"({diff_percent:+.1f}%), action={recommended_action}")
        
        return MarginalCarbonForecast(
            timestamp=now,
            average_intensity_g_per_kwh=average_intensity,
            marginal_intensity_g_per_kwh=marginal_intensity,
            difference_percent=diff_percent,
            recommended_action=recommended_action,
            confidence=confidence,
            marginal_generator=marginal_gen,
            forecast_horizon_hours=forecast_hours
        )
    
    def _forecast_generation_mix(self, hours: int) -> List[Dict]:
        """
        Forecast generation mix using time-of-day patterns.
        
        In production, this would call grid API or use ML models.
        """
        now = datetime.now()
        forecast = []
        
        for h in range(hours):
            forecast_time = now + timedelta(hours=h)
            hour_of_day = forecast_time.hour
            
            # Simplified model based on time of day
            if 6 <= hour_of_day <= 18:
                # Daytime: more solar
                solar_output = 300 * np.sin(np.pi * (hour_of_day - 6) / 12)
                wind_output = 150
                coal_output = 400
                gas_output = 200
            else:
                # Nighttime: more coal/gas
                solar_output = 0
                wind_output = 200
                coal_output = 500
                gas_output = 250
            
            forecast.append({
                'timestamp': forecast_time,
                'output_mw': coal_output,
                'type': GeneratorType.COAL
            })
            forecast.append({
                'timestamp': forecast_time,
                'output_mw': gas_output,
                'type': GeneratorType.NATURAL_GAS
            })
            forecast.append({
                'timestamp': forecast_time,
                'output_mw': wind_output,
                'type': GeneratorType.WIND
            })
            if solar_output > 0:
                forecast.append({
                    'timestamp': forecast_time,
                    'output_mw': solar_output,
                    'type': GeneratorType.SOLAR
                })
        
        return forecast
    
    def _forecast_demand(self, hours: int) -> List[Dict]:
        """Forecast electricity demand"""
        now = datetime.now()
        demand = []
        
        for h in range(hours):
            forecast_time = now + timedelta(hours=h)
            hour_of_day = forecast_time.hour
            
            # Typical daily demand pattern
            if hour_of_day in [9, 10, 11, 12, 13, 14, 15, 16, 17]:
                # Peak demand
                base_demand = 10000
            elif hour_of_day in [6, 7, 8, 18, 19, 20]:
                # Shoulder
                base_demand = 8000
            else:
                # Night
                base_demand = 6000
            
            demand.append({
                'timestamp': forecast_time,
                'demand_mw': base_demand
            })
        
        return demand
    
    def _find_marginal_generator(self, generation_mix: List[Dict], 
                                  demand_forecast: List[Dict]) -> Tuple[GeneratorType, float]:
        """
        Find marginal generator using merit order dispatch.
        
        Merit order: generators sorted by marginal cost.
        Marginal generator = the last generator dispatched to meet demand.
        """
        # Get current demand (first forecast point)
        current_demand = demand_forecast[0]['demand_mw'] if demand_forecast else 5000
        
        # Sort generators by marginal cost
        generators_by_cost = sorted(
            self.GENERATOR_DATA.items(),
            key=lambda x: x[1].marginal_cost_usd_per_mwh
        )
        
        # Dispatch merit order
        cumulative_output = 0
        marginal_gen = GeneratorType.COAL
        marginal_output = 0
        
        for gen_type, data in generators_by_cost:
            # Calculate available output for this generator type
            available = sum(m['output_mw'] for m in generation_mix if m['type'] == gen_type)
            
            if cumulative_output + available >= current_demand:
                # This generator is marginal
                marginal_gen = gen_type
                marginal_output = current_demand - cumulative_output
                break
            
            cumulative_output += available
        
        return marginal_gen, marginal_output
    
    def _determine_action(self, marginal_intensity: float, average_intensity: float) -> str:
        """Determine recommended scheduling action"""
        # If marginal > average by 20%, defer tasks
        if marginal_intensity > average_intensity * 1.2:
            if marginal_intensity > 600:
                return 'DEFER'
            else:
                return 'FOLLOW_CARBON_ZONE'
        elif marginal_intensity < average_intensity * 0.8:
            return 'EXECUTE_NOW'
        else:
            return 'FOLLOW_CARBON_ZONE'
    
    def _calculate_confidence(self, forecast_hours: int) -> float:
        """Calculate confidence in forecast (decays with horizon)"""
        base_confidence = 0.95
        decay_rate = 0.02
        
        confidence = base_confidence - (forecast_hours * decay_rate)
        return max(0.5, min(0.95, confidence))
    
    def get_marginal_benefit(self, workload_energy_kwh: float, 
                            forecast: MarginalCarbonForecast) -> Dict:
        """
        Calculate carbon benefit of using MCI vs ACI.
        
        Returns:
            Dictionary with carbon savings from optimal scheduling
        """
        # Carbon using marginal intensity
        marginal_carbon = workload_energy_kwh * forecast.marginal_intensity_g_per_kwh / 1000
        
        # Carbon using average intensity
        average_carbon = workload_energy_kwh * forecast.average_intensity_g_per_kwh / 1000
        
        # Benefit of MCI-aware scheduling
        if forecast.recommended_action == 'DEFER' and marginal_carbon > average_carbon:
            # Deferring avoids high marginal carbon
            saving = marginal_carbon - average_carbon
            return {
                'carbon_saving_kg': saving,
                'saving_percent': (saving / marginal_carbon) * 100 if marginal_carbon > 0 else 0,
                'recommendation': f"Defer task to avoid {saving:.2f} kg CO2"
            }
        elif forecast.recommended_action == 'EXECUTE_NOW' and marginal_carbon < average_carbon:
            # Executing now captures low marginal carbon
            saving = average_carbon - marginal_carbon
            return {
                'carbon_saving_kg': saving,
                'saving_percent': (saving / average_carbon) * 100 if average_carbon > 0 else 0,
                'recommendation': f"Execute now to save {saving:.2f} kg CO2"
            }
        else:
            return {
                'carbon_saving_kg': 0,
                'saving_percent': 0,
                'recommendation': "Follow standard carbon zones"
            }
    
    def get_mci_timeseries(self, hours: int = 24) -> List[Dict]:
        """Get MCI time series for visualization"""
        series = []
        
        for h in range(hours):
            forecast = self.forecast_marginal_intensity(h)
            series.append({
                'hour': h,
                'marginal_intensity': forecast.marginal_intensity_g_per_kwh,
                'average_intensity': forecast.average_intensity_g_per_kwh,
                'action': forecast.recommended_action
            })
        
        return series
