# src/governance/helium_policy_adapter.py

from typing import Dict, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import aiohttp
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class HeliumScarcityLevel(Enum):
    """Helium supply chain scarcity levels"""
    NORMAL = "normal"
    CAUTION = "caution"
    CRITICAL = "critical"
    SEVERE = "severe"

class RecommendedAction(Enum):
    """Recommended actions based on helium supply"""
    NORMAL = "normal"
    OPTIMIZE_GPU = "optimize_gpu_usage"
    THROTTLE_GPU = "throttle_gpu"
    DEFER_GPU = "defer_gpu"
    REDIRECT_TO_CPU = "redirect_to_cpu"
    BLOCK = "block"

@dataclass
class HeliumSupplyStatus:
    """Real-time helium supply chain status"""
    scarcity_level: HeliumScarcityLevel
    scarcity_score: float  # 0.0 (normal) to 1.0 (severe)
    spot_price_usd_per_liter: float
    fab_inventory_days: int
    vendor_alerts: List[str]
    forecast_days: int
    recommended_action: RecommendedAction
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = "api"

@dataclass
class AdaptedPolicy:
    """Policy adaptation result from helium-aware meta-cognition"""
    action: str  # 'normal', 'throttle', 'defer', 'redirect'
    throttle_factor: Optional[float] = None
    reason: str = ""
    helium_aware: bool = False
    target_hardware: Optional[str] = None

class HeliumPolicyAdapter:
    """
    Helium-aware policy adapter that integrates with Layer 1 Meta-Cognition
    Dynamically adjusts workload scheduling based on helium scarcity
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.api_url = self.config.get('helium_api_url', 'https://api.helium-supply.example.com/v1')
        self.update_interval_seconds = self.config.get('update_interval', 300)  # 5 minutes
        self.current_supply_status: Optional[HeliumSupplyStatus] = None
        self.policy_cache = {}
        self._update_task = None
        
        # Scarcity thresholds
        self.thresholds = {
            'caution_score': 0.3,
            'critical_score': 0.6,
            'severe_score': 0.8,
            'price_caution_usd': 5.0,
            'price_critical_usd': 7.0,
            'price_severe_usd': 10.0
        }
        
        # Start background monitoring
        self._start_monitoring()
    
    def _start_monitoring(self):
        """Start background helium supply monitoring"""
        asyncio.create_task(self._monitor_helium_supply())
    
    async def _monitor_helium_supply(self):
        """Background task to monitor helium supply"""
        while True:
            try:
                self.current_supply_status = await self.fetch_helium_supply()
                logger.info(f"Helium supply updated: {self.current_supply_status.scarcity_level.value}")
            except Exception as e:
                logger.error(f"Failed to fetch helium supply: {e}")
                # Use fallback simulation
                self.current_supply_status = self._simulate_helium_supply()
            
            await asyncio.sleep(self.update_interval_seconds)
    
    async def fetch_helium_supply(self) -> HeliumSupplyStatus:
        """Fetch real-time helium supply chain data from external APIs"""
        
        try:
            async with aiohttp.ClientSession() as session:
                # Primary API call
                async with session.get(f"{self.api_url}/supply/current", timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_api_response(data)
                    else:
                        raise Exception(f"API returned {resp.status}")
                        
        except Exception as e:
            logger.warning(f"Helium API unavailable, using simulation: {e}")
            return self._simulate_helium_supply()
    
    def _parse_api_response(self, data: Dict) -> HeliumSupplyStatus:
        """Parse API response into HeliumSupplyStatus"""
        
        scarcity_str = data.get('scarcity_level', 'normal')
        scarcity_level = HeliumScarcityLevel(scarcity_str)
        
        # Calculate recommended action based on supply data
        recommended_action = self._calculate_recommended_action(data)
        
        return HeliumSupplyStatus(
            scarcity_level=scarcity_level,
            scarcity_score=data.get('scarcity_score', 0.0),
            spot_price_usd_per_liter=data.get('spot_price_usd', 4.0),
            fab_inventory_days=data.get('fab_inventory_days', 30),
            vendor_alerts=data.get('alerts', []),
            forecast_days=data.get('forecast_days', 30),
            recommended_action=recommended_action,
            source='api'
        )
    
    def _simulate_helium_supply(self) -> HeliumSupplyStatus:
        """Simulate helium supply for testing/fallback"""
        import random
        
        # Simulate realistic scarcity patterns
        rand = random.random()
        
        if rand < 0.7:
            scarcity_level = HeliumScarcityLevel.NORMAL
            scarcity_score = 0.1
            price = 4.0
            inventory = 30
        elif rand < 0.85:
            scarcity_level = HeliumScarcityLevel.CAUTION
            scarcity_score = 0.4
            price = 5.5
            inventory = 20
        elif rand < 0.95:
            scarcity_level = HeliumScarcityLevel.CRITICAL
            scarcity_score = 0.7
            price = 8.0
            inventory = 10
        else:
            scarcity_level = HeliumScarcityLevel.SEVERE
            scarcity_score = 0.9
            price = 12.0
            inventory = 5
        
        recommended_action = self._calculate_recommended_action({
            'scarcity_level': scarcity_level.value,
            'spot_price_usd': price
        })
        
        return HeliumSupplyStatus(
            scarcity_level=scarcity_level,
            scarcity_score=scarcity_score,
            spot_price_usd_per_liter=price,
            fab_inventory_days=inventory,
            vendor_alerts=[],
            forecast_days=30,
            recommended_action=recommended_action,
            source='simulation'
        )
    
    def _calculate_recommended_action(self, supply_data: Dict) -> RecommendedAction:
        """Calculate recommended action based on supply data"""
        
        scarcity = supply_data.get('scarcity_level', 'normal')
        price = supply_data.get('spot_price_usd', 4.0)
        
        if scarcity == 'severe' or price > self.thresholds['price_severe_usd']:
            return RecommendedAction.DEFER_GPU
        elif scarcity == 'critical' or price > self.thresholds['price_critical_usd']:
            return RecommendedAction.THROTTLE_GPU
        elif scarcity == 'caution' or price > self.thresholds['price_caution_usd']:
            return RecommendedAction.OPTIMIZE_GPU
        else:
            return RecommendedAction.NORMAL
    
    def adapt_policy(self, workload_profile, system_state) -> AdaptedPolicy:
        """
        Adapt execution policy based on helium supply and workload characteristics
        
        This is the main interface for Layer 1 Meta-Cognition integration
        """
        
        if not self.current_supply_status:
            # No data yet, return normal policy
            return AdaptedPolicy(action='normal', helium_aware=False)
        
        helium_status = self.current_supply_status
        
        # Get workload helium profile
        helium_profile = getattr(workload_profile, 'helium_profile', None)
        if not helium_profile:
            return AdaptedPolicy(action='normal', helium_aware=False)
        
        # High helium dependency workloads are most affected
        if helium_profile.dependency_score > 0.7:
            
            if helium_status.recommended_action == RecommendedAction.DEFER_GPU:
                return AdaptedPolicy(
                    action='defer',
                    reason=f"Helium scarcity: {helium_status.scarcity_level.value}, price=${helium_status.spot_price_usd_per_liter}/L",
                    helium_aware=True
                )
            
            elif helium_status.recommended_action == RecommendedAction.THROTTLE_GPU:
                # Calculate throttle factor based on dependency score
                throttle_factor = max(0.3, 1.0 - helium_profile.dependency_score)
                return AdaptedPolicy(
                    action='throttle',
                    throttle_factor=throttle_factor,
                    reason=f"Helium supply constrained: {helium_status.scarcity_level.value}",
                    helium_aware=True
                )
            
            elif helium_status.recommended_action == RecommendedAction.OPTIMIZE_GPU:
                return AdaptedPolicy(
                    action='optimize',
                    reason=f"Helium caution mode - prefer quantization",
                    helium_aware=True
                )
            
            elif helium_status.recommended_action == RecommendedAction.REDIRECT_TO_CPU:
                if helium_profile.can_run_on_cpu:
                    return AdaptedPolicy(
                        action='redirect',
                        target_hardware='cpu',
                        reason=f"Redirecting to CPU due to helium shortage",
                        helium_aware=True
                    )
        
        # Medium dependency workloads - only affected in severe scarcity
        elif helium_profile.dependency_score > 0.4:
            if helium_status.recommended_action == RecommendedAction.DEFER_GPU:
                return AdaptedPolicy(
                    action='throttle',
                    throttle_factor=0.5,
                    reason=f"Helium severe scarcity, throttling medium-dependency workload",
                    helium_aware=True
                )
        
        # Low dependency workloads - always run
        return AdaptedPolicy(action='normal', helium_aware=False)
    
    def get_current_status(self) -> Optional[HeliumSupplyStatus]:
        """Get current helium supply status"""
        return self.current_supply_status
    
    async def record_helium_usage(self, task_id: str, helium_usage: float, execution_result: Dict):
        """Record actual helium usage for future policy learning"""
        
        # Store in helium usage ledger (could be database)
        usage_record = {
            'task_id': task_id,
            'helium_usage': helium_usage,
            'timestamp': datetime.now().isoformat(),
            'supply_status_at_execution': self.current_supply_status.scarcity_level.value if self.current_supply_status else 'unknown',
            'execution_result': execution_result
        }
        
        # Log for analytics
        logger.info(f"Helium usage recorded: {usage_record}")
        
        # In production, store in TimescaleDB or similar
        # await self.db.insert_helium_usage(usage_record)
