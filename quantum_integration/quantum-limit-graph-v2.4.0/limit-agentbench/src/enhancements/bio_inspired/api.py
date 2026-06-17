# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/api.py

"""
Standard REST API for Bio-Inspired Green Agent Modules
Provides external access to all metabolic ecosystem components.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class BioInspiredAPI:
    """
    RESTful API wrapper for bio-inspired modules.
    
    Provides standard endpoints for:
    - Token economy queries
    - Gradient field monitoring
    - Compartment management
    - Biomass storage operations
    - Harvester status
    - System health and metrics
    """
    
    def __init__(self, bio_core=None):
        self.bio_core = bio_core
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None
        self.biomass_storage = getattr(bio_core, 'biomass_storage', None) if bio_core else None
        self.harvester = getattr(bio_core, 'harvester', None) if bio_core else None
        self.scheduler = getattr(bio_core, 'scheduler', None) if bio_core else None
        
        # Request history
        self.request_history: List[Dict] = []
        self.rate_limits: Dict[str, Dict] = {}
        
        logger.info("Bio-Inspired API initialized")
    
    # ========================================================================
    # Token Economy Endpoints
    # ========================================================================
    
    def get_token_summary(self) -> Dict[str, Any]:
        """GET /api/v1/tokens/summary"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        return self.token_manager.get_system_summary()
    
    def get_account_balance(self, account_id: str) -> Dict[str, Any]:
        """GET /api/v1/tokens/accounts/{account_id}"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        return self.token_manager.get_account_summary(account_id)
    
    def generate_tokens(self, account_id: str, carbon_saved_kg: float = 0.0,
                       helium_saved_units: float = 0.0, 
                       energy_saved_kwh: float = 0.0) -> Dict[str, Any]:
        """POST /api/v1/tokens/generate"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        
        tokens = self.token_manager.generate_tokens(
            account_id=account_id,
            source=EcoATPSource.RENEWABLE_ENERGY,
            carbon_saved_kg=carbon_saved_kg,
            helium_saved_units=helium_saved_units,
            energy_saved_kwh=energy_saved_kwh
        )
        
        return {
            'success': True,
            'tokens_generated': len(tokens),
            'total_value': sum(t.value for t in tokens) if tokens else 0,
            'account_id': account_id
        }
    
    def reserve_tokens(self, account_id: str, amount: float,
                      consumer: str = 'expert_execution',
                      priority: int = 2) -> Dict[str, Any]:
        """POST /api/v1/tokens/reserve"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        
        try:
            consumer_enum = EcoATPConsumer(consumer)
        except ValueError:
            consumer_enum = EcoATPConsumer.EXPERT_EXECUTION
        
        success, token_ids = self.token_manager.reserve_tokens(
            account_id=account_id, amount=amount,
            consumer=consumer_enum, priority=priority
        )
        
        return {
            'success': success,
            'tokens_reserved': len(token_ids) if success else 0,
            'amount': amount if success else 0,
            'account_id': account_id
        }
    
    # ========================================================================
    # Gradient Field Endpoints
    # ========================================================================
    
    def get_gradient_summary(self) -> Dict[str, Any]:
        """GET /api/v1/gradients/summary"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        return {
            'field_strengths': self.gradient_manager.get_field_strengths(),
            'detailed_stats': self.gradient_manager.get_field_stats(),
            'dominant_field': self.gradient_manager.get_dominant_field(),
            'total_potential': self.gradient_manager.get_total_potential()
        }
    
    def get_gradient_field(self, field_id: str) -> Dict[str, Any]:
        """GET /api/v1/gradients/{field_id}"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        
        field = self.gradient_manager.fields.get(field_id)
        if not field:
            return {'error': f'Field {field_id} not found'}
        
        return field.get_detailed_state()
    
    def pump_gradient(self, field_id: str, amount: float, 
                     source: str = "api") -> Dict[str, Any]:
        """POST /api/v1/gradients/{field_id}/pump"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        
        self.gradient_manager.pump_field(field_id, amount, source=source)
        
        field = self.gradient_manager.fields.get(field_id)
        return {
            'success': True,
            'field_id': field_id,
            'new_strength': field.effective_strength if field else 0,
            'amount_pumped': amount
        }
    
    # ========================================================================
    # Compartment Endpoints
    # ========================================================================
    
    def get_compartment_summary(self) -> Dict[str, Any]:
        """GET /api/v1/compartments/summary"""
        if not self.compartment_manager:
            return {'error': 'Compartment manager not available'}
        
        compartments = self.compartment_manager.compartments
        return {
            'total_compartments': len(compartments),
            'viable_compartments': sum(1 for c in compartments.values() if c.is_viable),
            'by_state': {
                state.value: sum(1 for c in compartments.values() if c.state == state)
                for state in CompartmentState
            },
            'compartments': {
                cid: {
                    'state': c.state.value,
                    'health': c.health_score,
                    'tokens': c.token_balance,
                    'membrane': c.membrane.permeability.value
                }
                for cid, c in compartments.items()
            }
        }
    
    def create_compartment(self, expert_type: str) -> Dict[str, Any]:
        """POST /api/v1/compartments/create"""
        if not self.compartment_manager:
            return {'error': 'Compartment manager not available'}
        
        compartment = self.compartment_manager.create_compartment(expert_type)
        
        return {
            'success': True,
            'compartment_id': compartment.compartment_id,
            'expert_type': expert_type,
            'state': compartment.state.value
        }
    
    # ========================================================================
    # Biomass Storage Endpoints
    # ========================================================================
    
    def get_biomass_summary(self) -> Dict[str, Any]:
        """GET /api/v1/biomass/summary"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        return self.biomass_storage.get_storage_stats()
    
    def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float,
                  guarantee: str = 'silver') -> Dict[str, Any]:
        """POST /api/v1/biomass/store"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        
        try:
            guarantee_level = GuaranteeLevel(guarantee)
        except ValueError:
            guarantee_level = GuaranteeLevel.SILVER
        
        stored, token_id = self.biomass_storage.store_task(
            task_data=task_data,
            ecoatp_cost=ecoatp_cost,
            guarantee=guarantee_level
        )
        
        return {
            'success': stored,
            'storage_token': token_id,
            'ecoatp_cost': ecoatp_cost
        }
    
    def retrieve_task(self, token_id: str) -> Dict[str, Any]:
        """POST /api/v1/biomass/retrieve"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        
        task_data, retrieval_cost = self.biomass_storage.retrieve_task(token_id)
        
        return {
            'success': task_data is not None,
            'task_data': task_data,
            'retrieval_cost': retrieval_cost
        }
    
    # ========================================================================
    # Harvester Endpoints
    # ========================================================================
    
    def get_harvester_summary(self) -> Dict[str, Any]:
        """GET /api/v1/harvester/summary"""
        if not self.harvester:
            return {'error': 'Harvester not available'}
        return self.harvester.get_harvesting_stats()
    
    def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]:
        """POST /api/v1/harvester/cycle"""
        if not self.harvester:
            return {'error': 'Harvester not available'}
        
        result = asyncio.get_event_loop().run_until_complete(
            self.harvester.harvest_cycle(environmental_data)
        )
        
        return {
            'success': True,
            'eco_atp_generated': result.get('eco_atp_generated', 0),
            'dominant_signal': result.get('dominant_signal', 'none'),
            'total_harvested': result.get('total_harvested', 0)
        }
    
    # ========================================================================
    # System Health Endpoints
    # ========================================================================
    
    def get_system_health(self) -> Dict[str, Any]:
        """GET /api/v1/health"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'components': {}
        }
        
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            health['components']['token_economy'] = {
                'status': 'emergency' if summary.get('emergency_mode') else 'healthy',
                'balance': summary.get('total_balance', 0),
                'efficiency': summary.get('system_efficiency', 0)
            }
        
        if self.gradient_manager:
            strengths = self.gradient_manager.get_field_strengths()
            health['components']['gradients'] = {
                'status': 'warning' if any(s > 0.8 for s in strengths.values()) else 'healthy',
                'fields': strengths
            }
        
        if self.compartment_manager:
            viable = sum(1 for c in self.compartment_manager.compartments.values() if c.is_viable)
            total = len(self.compartment_manager.compartments)
            health['components']['compartments'] = {
                'status': 'healthy' if viable > total * 0.5 else 'degraded',
                'viable': viable,
                'total': total
            }
        
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            health['components']['biomass'] = {
                'status': 'warning' if stats.get('total_stored', 0) > 8000 else 'healthy',
                'total_stored': stats.get('total_stored', 0)
            }
        
        if self.harvester:
            harvester_stats = self.harvester.get_harvesting_stats()
            health['components']['harvester'] = {
                'status': 'healthy' if harvester_stats.get('total_harvested', 0) > 0 else 'inactive',
                'total_harvested': harvester_stats.get('total_harvested', 0)
            }
        
        # Overall status
        statuses = [c.get('status', 'healthy') for c in health['components'].values()]
        if 'emergency' in statuses:
            health['status'] = 'emergency'
        elif 'degraded' in statuses:
            health['status'] = 'degraded'
        elif 'warning' in statuses:
            health['status'] = 'warning'
        
        return health
    
    def get_metrics(self) -> Dict[str, Any]:
        """GET /api/v1/metrics - Prometheus-compatible metrics"""
        metrics = []
        timestamp = int(datetime.utcnow().timestamp() * 1000)
        
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            metrics.append(f'green_agent_ecoatp_balance {summary.get("total_balance", 0)} {timestamp}')
            metrics.append(f'green_agent_ecoatp_efficiency {summary.get("system_efficiency", 0)} {timestamp}')
            metrics.append(f'green_agent_emergency_mode {1 if summary.get("emergency_mode") else 0} {timestamp}')
        
        if self.gradient_manager:
            for field_id, strength in self.gradient_manager.get_field_strengths().items():
                metrics.append(f'green_agent_gradient{{field="{field_id}"}} {strength} {timestamp}')
        
        if self.compartment_manager:
            viable = sum(1 for c in self.compartment_manager.compartments.values() if c.is_viable)
            total = len(self.compartment_manager.compartments)
            metrics.append(f'green_agent_compartments_viable {viable} {timestamp}')
            metrics.append(f'green_agent_compartments_total {total} {timestamp}')
        
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            metrics.append(f'green_agent_biomass_total {stats.get("total_stored", 0)} {timestamp}')
        
        if self.harvester:
            harvester_stats = self.harvester.get_harvesting_stats()
            metrics.append(f'green_agent_harvester_total {harvester_stats.get("total_harvested", 0)} {timestamp}')
        
        return '\n'.join(metrics)
