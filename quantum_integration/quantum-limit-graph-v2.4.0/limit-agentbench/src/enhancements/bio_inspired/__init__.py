# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/__init__.py
# Complete enhanced file with protocol-based DI, economic reporting, and consolidated initialization

"""
Bio-Inspired Green Agent v5.0.0
Complete implementation with protocol-based DI, supply management, and economic reporting
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Protocol
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

# ============================================================================
# Service Protocols
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any, tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class GradientServiceProtocol(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def get_dominant_field(self) -> Tuple[str, float]: ...
    def get_field_stats(self) -> Dict[str, Any]: ...
    def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    def get_forecast_summary(self) -> Dict[str, Any]: ...

class CompartmentServiceProtocol(Protocol):
    def find_best_compartment(self, expert_type: str, task_complexity: float) -> Any: ...
    def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    def create_compartment(self, expert_type: str, expert_instance: Any, resources: Any, parent_id: str) -> Any: ...
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

class BiomassServiceProtocol(Protocol):
    def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float, guarantee: Any, deadline: Any, initial_tier: Any) -> Tuple[bool, Optional[str]]: ...
    def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]: ...
    def get_storage_stats(self) -> Dict[str, Any]: ...
    def simulate_storage_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]: ...

# ============================================================================
# Module Availability Checks
# ============================================================================

BIO_INSPIRED_AVAILABLE = True

try:
    from .eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenSupplyManager, PredictiveTokenAllocator, TokenServiceProtocol as TSP
    )
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

try:
    from .proton_gradient_fields import HierarchicalGradientManager, GradientServiceProtocol as GSP
except ImportError:
    pass

try:
    from .atp_synthase_scheduler import ATPSynthaseScheduler
except ImportError:
    pass

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager
except ImportError:
    pass

try:
    from .biomass_storage import BiomassStorage
except ImportError:
    pass

try:
    from .photosynthetic_harvester import PhotosyntheticHarvester
except ImportError:
    pass

# ============================================================================
# Enhanced Bio-Inspired Core
# ============================================================================

class EnhancedBioInspiredCore:
    """Enhanced Bio-Inspired Core with protocol-based DI and all features"""
    
    def __init__(self, enable_enhancements: bool = True):
        # Initialize exchange rate
        self.exchange_rate = DynamicExchangeRate()
        
        # Initialize core modules
        self._token_manager = EcoATPTokenManager(self.exchange_rate)
        self._gradient_manager = HierarchicalGradientManager()
        self._scheduler = ATPSynthaseScheduler(self._token_manager, self._gradient_manager)
        self._compartment_manager = HierarchicalCompartmentManager(self._token_manager)
        self._biomass_storage = BiomassStorage(self._token_manager)
        self._harvester = PhotosyntheticHarvester(self._token_manager)
        
        # Supply management and pre-allocation
        if enable_enhancements:
            self._supply_manager = TokenSupplyManager(self._token_manager)
            self._token_allocator = PredictiveTokenAllocator(self._token_manager)
        
        # Knowledge transfer
        from .knowledge_transfer import KnowledgeTransferManager
        self._knowledge_transfer = KnowledgeTransferManager()
        
        # Degradation management
        from .degradation_manager import DegradationManager
        self._degradation_manager = DegradationManager()
        
        # API
        from .api import BioInspiredAPI
        self._api = BioInspiredAPI(self)
        
        # Wire degradation manager
        self._degradation_manager.update_metrics(
            token_balance=self._token_manager.get_system_summary().get('total_balance', 500)
        )
        self._degradation_manager.register_callback(self._on_tier_change)
        
        # Start monitoring
        asyncio.create_task(self._enhanced_monitoring_loop())
        
        logger.info("Enhanced Bio-Inspired Core v5.0.0 initialized with protocol-based DI")
    
    # ========================================================================
    # Protocol-Compliant Service Accessors
    # ========================================================================
    
    @property
    def token_service(self) -> TokenServiceProtocol:
        return self._token_manager
    
    @property
    def gradient_service(self) -> GradientServiceProtocol:
        return self._gradient_manager
    
    @property
    def compartment_service(self) -> CompartmentServiceProtocol:
        return self._compartment_manager
    
    @property
    def biomass_service(self) -> BiomassServiceProtocol:
        return self._biomass_storage
    
    # Legacy accessors
    @property
    def token_manager(self): return self._token_manager
    @property
    def gradient_manager(self): return self._gradient_manager
    @property
    def scheduler(self): return self._scheduler
    @property
    def compartment_manager(self): return self._compartment_manager
    @property
    def biomass_storage(self): return self._biomass_storage
    @property
    def harvester(self): return self._harvester
    @property
    def supply_manager(self): return self._supply_manager if hasattr(self, '_supply_manager') else None
    @property
    def token_allocator(self): return self._token_allocator if hasattr(self, '_token_allocator') else None
    @property
    def knowledge_transfer(self): return self._knowledge_transfer
    @property
    def degradation_manager(self): return self._degradation_manager
    @property
    def api(self): return self._api
    
    # ========================================================================
    # Monitoring
    # ========================================================================
    
    async def _enhanced_monitoring_loop(self):
        while True:
            try:
                summary = self._token_manager.get_system_summary()
                gradients = self._gradient_manager.get_field_strengths()
                self._degradation_manager.update_metrics(
                    token_balance=summary.get('total_balance', 500),
                    carbon_gradient=gradients.get('carbon', 0.5),
                    compartment_health=self._get_avg_compartment_health(),
                    harvester_activity=self._harvester.total_harvested if self._harvester else 0
                )
                for field_id, strength in gradients.items():
                    self._gradient_manager.record_measurement(field_id, strength)
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"Monitoring error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _on_tier_change(self, old_tier, new_tier, policies):
        logger.warning(f"Tier change: {old_tier.name} → {new_tier.name}")
    
    def _get_avg_compartment_health(self) -> float:
        if not self._compartment_manager:
            return 0.5
        compartments = self._compartment_manager.compartments
        if not compartments:
            return 0.5
        return np.mean([c.health_score for c in compartments.values()])
    
    # ========================================================================
    # System Status and Reporting
    # ========================================================================
    
    def get_system_status(self) -> Dict[str, Any]:
        status = {
            'token_economy': self._token_manager.get_system_summary(),
            'gradients': self._gradient_manager.get_field_stats(),
            'gradient_forecasts': self._gradient_manager.get_forecast_summary(),
            'scheduler': self._scheduler.get_scheduler_stats() if self._scheduler else {},
            'compartments': self._compartment_manager.get_ecosystem_stats() if self._compartment_manager else {},
            'biomass': self._biomass_storage.get_storage_stats() if self._biomass_storage else {},
            'harvester': self._harvester.get_harvesting_stats() if self._harvester else {},
            'degradation': self._degradation_manager.get_tier_status() if hasattr(self, '_degradation_manager') else {},
            'knowledge': self._knowledge_transfer.get_knowledge_summary() if hasattr(self, '_knowledge_transfer') else {}
        }
        if hasattr(self, '_supply_manager'):
            status['token_economy']['supply_management'] = self._supply_manager.get_economic_indicators()
        if hasattr(self, '_token_allocator'):
            status['token_economy']['pre_allocation'] = self._token_allocator.get_cache_stats()
        return status
    
    def get_economic_report(self) -> Dict[str, Any]:
        report = {'timestamp': datetime.utcnow().isoformat(), 'token_economy': self._token_manager.get_system_summary()}
        if hasattr(self, '_supply_manager'):
            report['supply_management'] = self._supply_manager.get_economic_indicators()
        if hasattr(self, '_token_allocator'):
            report['pre_allocation'] = self._token_allocator.get_cache_stats()
        indicators = report.get('supply_management', {})
        utilization = indicators.get('utilization', 0.5)
        inflation = indicators.get('inflation_pressure', 0)
        if 0.6 < utilization < 0.9 and abs(inflation) < 0.2:
            report['health'] = 'healthy'
        elif utilization < 0.4:
            report['health'] = 'deflationary'
        elif utilization > 0.95:
            report['health'] = 'inflationary'
        else:
            report['health'] = 'stable'
        recs = []
        if utilization < 0.4: recs.append("Economy under-utilized. Increase task throughput.")
        if utilization > 0.95: recs.append("Economy over-heating. Add capacity or reduce load.")
        if inflation > 0.3: recs.append("High inflation pressure. Token burning recommended.")
        report['recommendations'] = recs if recs else ["Economy is healthy."]
        return report
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        ecoatp_required = task.get('complexity', 0.5) * 10
        
        if hasattr(self, '_token_allocator'):
            success, _ = self._token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self._token_allocator.record_demand('task_processor', ecoatp_required)
        else:
            success, _ = self._token_manager.reserve_tokens('task_processor', ecoatp_required, EcoATPConsumer.EXPERT_EXECUTION)
        
        if not success:
            if self._biomass_storage:
                stored, token_id = self._biomass_storage.store_task(task_data=task, ecoatp_cost=ecoatp_required)
                return {'success': True, 'status': 'stored', 'biomass_token': token_id}
            return {'success': False, 'reason': 'Insufficient tokens'}
        
        return {'success': True, 'task_id': task.get('task_id', 'unknown')}

# ============================================================================
# Convenience Functions
# ============================================================================

def create_metabolic_ecosystem(enable_bio: bool = True) -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(enable_enhancements=enable_bio)

def create_minimal_ecosystem() -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(enable_enhancements=False)
