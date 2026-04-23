# src/governance/helium_policy_adapter.py (New Module)

from typing import Dict, Optional
from dataclasses import dataclass
import requests

@dataclass
class HeliumSupplyStatus:
    scarcity_level: str  # 'normal', 'caution', 'critical', 'severe'
    fab_reports: Dict    # Semiconductor fab helium inventory
    vendor_alerts: List[str]
    forecast_days: int
    recommended_action: str  # 'normal', 'throttle_gpu', 'defer_gpu', 'redirect_to_cpu'

class HeliumPolicyAdapter:
    def __init__(self, monitoring_api_url: str = "https://helium-monitor.example.com/api"):
        self.api_url = monitoring_api_url
        self.current_supply_status = None
        self.policy_cache = {}
    
    def get_helium_supply_status(self) -> HeliumSupplyStatus:
        """Fetch real-time helium supply chain data"""
        try:
            response = requests.get(f"{self.api_url}/v1/supply/current", timeout=5)
            data = response.json()
            
            return HeliumSupplyStatus(
                scarcity_level=data['scarcity_level'],
                fab_reports=data['fab_inventory'],
                vendor_alerts=data.get('alerts', []),
                forecast_days=data['forecast_days'],
                recommended_action=self._calculate_recommended_action(data)
            )
        except Exception as e:
            # Fallback: assume normal if API unavailable
            return HeliumSupplyStatus(
                scarcity_level='normal',
                fab_reports={},
                vendor_alerts=[],
                forecast_days=30,
                recommended_action='normal'
            )
    
    def _calculate_recommended_action(self, supply_data: dict) -> str:
        scarcity = supply_data['scarcity_level']
        helium_price = supply_data.get('spot_price_usd_per_liter', 4.0)
        
        if scarcity == 'severe' or helium_price > 10.0:
            return 'defer_gpu'
        elif scarcity == 'critical' or helium_price > 7.0:
            return 'throttle_gpu'
        elif scarcity == 'caution' or helium_price > 5.0:
            return 'optimize_gpu_usage'
        else:
            return 'normal'
    
    def adapt_policy(self, workload_profile, system_state) -> AdaptedPolicy:
        """Extension of Layer 1's policy adaptation"""
        helium_status = self.get_helium_supply_status()
        
        # Check if workload has high helium dependency
        if workload_profile.helium_profile.dependency_score > 0.7:
            if helium_status.recommended_action == 'defer_gpu':
                # Override execution decision
                return AdaptedPolicy(
                    action='defer',
                    reason=f"Helium scarcity detected: {helium_status.scarcity_level}",
                    helium_aware=True
                )
            elif helium_status.recommended_action == 'throttle_gpu':
                return AdaptedPolicy(
                    action='throttle',
                    throttle_factor=0.5,
                    reason=f"Helium supply constrained: {helium_status.scarcity_level}"
                )
        
        # Fall back to normal carbon-aware policy
        return AdaptedPolicy(action='normal', helium_aware=False)
    
    def update_helium_metrics(self, execution_result):
        """Record actual helium usage for future policy learning"""
        # Store in helium_usage_ledger (similar to CarbonLedger)
        pass
