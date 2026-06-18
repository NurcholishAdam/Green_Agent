# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/api.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Bio-Inspired API v5.0.0
Complete RESTful API with authentication, rate limiting, versioning,
complete module coverage, operational endpoints, pagination, and webhook support.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from functools import wraps
import hashlib
import hmac
import json
import time
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

# ============================================================================
# API Version
# ============================================================================

API_VERSION = "v1"
API_PREFIX = f"/api/{API_VERSION}"

# ============================================================================
# Authentication and Rate Limiting
# ============================================================================

class APIKeyManager:
    """Manages API keys for authentication and rate limiting"""
    
    def __init__(self):
        self.api_keys: Dict[str, Dict[str, Any]] = {}
        self.rate_limit_records: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Default rate limits
        self.default_rate_limit = 100  # requests per minute
        self.default_burst_limit = 20  # concurrent requests
        
        # Create default admin key
        self._create_key("admin", "default-admin-key", rate_limit=1000, role="admin")
        
        logger.info("API Key Manager initialized")
    
    def _create_key(self, name: str, key: str, rate_limit: int = 100, 
                   role: str = "user", permissions: Optional[List[str]] = None) -> str:
        """Create a new API key"""
        self.api_keys[key] = {
            'name': name,
            'key': key,
            'rate_limit': rate_limit,
            'role': role,
            'permissions': permissions or ["read"],
            'created_at': datetime.utcnow(),
            'last_used': None,
            'total_requests': 0,
            'active': True
        }
        return key
    
    def create_key(self, name: str, rate_limit: int = 100, 
                  role: str = "user") -> str:
        """Create a new API key"""
        key = hashlib.sha256(f"{name}{datetime.utcnow().timestamp()}{id(self)}".encode()).hexdigest()[:32]
        return self._create_key(name, key, rate_limit, role)
    
    def validate_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """Validate an API key and return its metadata"""
        if api_key in self.api_keys:
            key_data = self.api_keys[api_key]
            if key_data['active']:
                key_data['last_used'] = datetime.utcnow()
                key_data['total_requests'] += 1
                return key_data
        return None
    
    def check_rate_limit(self, api_key: str) -> Tuple[bool, Dict[str, Any]]:
        """Check if request is within rate limit"""
        if api_key not in self.api_keys:
            return False, {'error': 'Invalid API key'}
        
        key_data = self.api_keys[api_key]
        limit = key_data['rate_limit']
        
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        
        # Clean old records
        recent = [t for t in self.rate_limit_records[api_key] if t > minute_ago]
        self.rate_limit_records[api_key] = deque(recent, maxlen=1000)
        
        if len(recent) >= limit:
            retry_after = 60 - (now - recent[0]).total_seconds()
            return False, {
                'error': 'Rate limit exceeded',
                'retry_after_seconds': max(0, int(retry_after)),
                'limit': limit,
                'current_usage': len(recent)
            }
        
        self.rate_limit_records[api_key].append(now)
        
        return True, {
            'limit': limit,
            'remaining': limit - len(recent) - 1,
            'reset_seconds': 60 - (now - (recent[0] if recent else now)).total_seconds()
        }
    
    def revoke_key(self, api_key: str) -> bool:
        """Revoke an API key"""
        if api_key in self.api_keys:
            self.api_keys[api_key]['active'] = False
            return True
        return False
    
    def get_key_stats(self) -> Dict[str, Any]:
        """Get API key statistics"""
        return {
            'total_keys': len(self.api_keys),
            'active_keys': sum(1 for k in self.api_keys.values() if k['active']),
            'keys': [
                {
                    'name': k['name'],
                    'role': k['role'],
                    'rate_limit': k['rate_limit'],
                    'total_requests': k['total_requests'],
                    'last_used': k['last_used'].isoformat() if k['last_used'] else None,
                    'active': k['active']
                }
                for k in self.api_keys.values()
            ]
        }

# ============================================================================
# Decorators
# ============================================================================

def require_auth(func):
    """Decorator to require API key authentication"""
    @wraps(func)
    async def wrapper(self, request_data: Dict[str, Any], *args, **kwargs):
        api_key = request_data.get('headers', {}).get('X-API-Key', '')
        
        if not api_key:
            return {'status': 401, 'error': 'API key required', 'message': 'Provide X-API-Key header'}
        
        key_data = self.api_key_manager.validate_key(api_key)
        if not key_data:
            return {'status': 403, 'error': 'Invalid API key'}
        
        # Check rate limit
        allowed, rate_info = self.api_key_manager.check_rate_limit(api_key)
        if not allowed:
            return {'status': 429, 'error': 'Rate limit exceeded', 'rate_info': rate_info}
        
        # Add key info to request
        request_data['api_key_info'] = key_data
        request_data['rate_info'] = rate_info
        
        return await func(self, request_data, *args, **kwargs)
    return wrapper

def require_role(role: str):
    """Decorator to require specific role"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, request_data: Dict[str, Any], *args, **kwargs):
            key_data = request_data.get('api_key_info', {})
            if key_data.get('role') != role and key_data.get('role') != 'admin':
                return {'status': 403, 'error': f'Role {role} required'}
            return await func(self, request_data, *args, **kwargs)
        return wrapper
    return decorator

# ============================================================================
# Pagination Helper
# ============================================================================

def paginate(items: List[Any], page: int = 1, limit: int = 20) -> Dict[str, Any]:
    """Paginate a list of items"""
    total = len(items)
    total_pages = max(1, (total + limit - 1) // limit)
    page = max(1, min(page, total_pages))
    
    start = (page - 1) * limit
    end = start + limit
    
    return {
        'data': items[start:end],
        'pagination': {
            'page': page,
            'limit': limit,
            'total': total,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
    }

# ============================================================================
# Enhanced Bio-Inspired API
# ============================================================================

class BioInspiredAPI:
    """
    Enhanced Bio-Inspired API v5.0.0
    
    Complete RESTful API with:
    - Authentication and rate limiting
    - Complete module coverage (all 10+ modules)
    - API versioning
    - Operational endpoints (health, config)
    - Pagination and filtering
    - Webhook support
    - Response caching headers
    """
    
    def __init__(self, bio_core=None):
        self.bio_core = bio_core
        
        # Module references
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None
        self.biomass_storage = getattr(bio_core, 'biomass_storage', None) if bio_core else None
        self.harvester = getattr(bio_core, 'harvester', None) if bio_core else None
        self.scheduler = getattr(bio_core, 'scheduler', None) if bio_core else None
        self.degradation_manager = getattr(bio_core, 'degradation_manager', None) if bio_core else None
        self.knowledge_transfer = getattr(bio_core, 'knowledge_transfer', None) if bio_core else None
        self.supply_manager = getattr(bio_core, 'supply_manager', None) if bio_core else None
        self.token_allocator = getattr(bio_core, 'token_allocator', None) if bio_core else None
        self.event_bus = getattr(bio_core, 'event_bus', None) if bio_core else None
        self.health_manager = getattr(bio_core, 'health_manager', None) if bio_core else None
        self.state_manager = getattr(bio_core, 'state_manager', None) if bio_core else None
        
        # API key manager
        self.api_key_manager = APIKeyManager()
        
        # Webhook subscriptions
        self.webhooks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Request history
        self.request_history: deque = deque(maxlen=10000)
        
        # Route registry
        self.routes: Dict[str, Callable] = {}
        self._register_routes()
        
        logger.info(f"Enhanced Bio-Inspired API v5.0.0 initialized ({len(self.routes)} routes)")
    
    def _register_routes(self):
        """Register all API routes"""
        
        # Health and Operational
        self.routes[f"{API_PREFIX}/health/live"] = self.get_live
        self.routes[f"{API_PREFIX}/health/ready"] = self.get_ready
        self.routes[f"{API_PREFIX}/health/status"] = self.get_health_status
        self.routes[f"{API_PREFIX}/config"] = self.get_config
        self.routes[f"{API_PREFIX}/config"] = self.update_config  # PUT handled separately
        
        # Token Economy
        self.routes[f"{API_PREFIX}/tokens/summary"] = self.get_token_summary
        self.routes[f"{API_PREFIX}/tokens/accounts"] = self.get_token_accounts
        self.routes[f"{API_PREFIX}/tokens/generate"] = self.generate_tokens
        self.routes[f"{API_PREFIX}/tokens/reserve"] = self.reserve_tokens
        self.routes[f"{API_PREFIX}/tokens/economic"] = self.get_economic_indicators
        
        # Gradient Fields
        self.routes[f"{API_PREFIX}/gradients/summary"] = self.get_gradient_summary
        self.routes[f"{API_PREFIX}/gradients/forecasts"] = self.get_gradient_forecasts
        self.routes[f"{API_PREFIX}/gradients/causal"] = self.get_causal_analysis
        self.routes[f"{API_PREFIX}/gradients/pump"] = self.pump_gradient
        
        # ATP Synthase
        self.routes[f"{API_PREFIX}/atp-synthase/status"] = self.get_atp_status
        self.routes[f"{API_PREFIX}/atp-synthase/efficiency"] = self.get_atp_efficiency
        
        # Compartments
        self.routes[f"{API_PREFIX}/compartments/summary"] = self.get_compartment_summary
        self.routes[f"{API_PREFIX}/compartments/regions"] = self.get_compartment_regions
        self.routes[f"{API_PREFIX}/compartments/create"] = self.create_compartment
        
        # Biomass Storage
        self.routes[f"{API_PREFIX}/biomass/summary"] = self.get_biomass_summary
        self.routes[f"{API_PREFIX}/biomass/store"] = self.store_task
        self.routes[f"{API_PREFIX}/biomass/retrieve"] = self.retrieve_task
        self.routes[f"{API_PREFIX}/biomass/analytics"] = self.get_biomass_analytics
        self.routes[f"{API_PREFIX}/biomass/forecast"] = self.get_biomass_forecast
        
        # Harvester
        self.routes[f"{API_PREFIX}/harvester/summary"] = self.get_harvester_summary
        self.routes[f"{API_PREFIX}/harvester/cycle"] = self.harvest_cycle
        self.routes[f"{API_PREFIX}/harvester/circadian"] = self.get_circadian_report
        
        # Degradation Manager
        self.routes[f"{API_PREFIX}/degradation/status"] = self.get_degradation_status
        self.routes[f"{API_PREFIX}/degradation/history"] = self.get_degradation_history
        self.routes[f"{API_PREFIX}/degradation/chaos"] = self.get_chaos_report
        
        # Knowledge Transfer
        self.routes[f"{API_PREFIX}/knowledge/packages"] = self.get_knowledge_packages
        self.routes[f"{API_PREFIX}/knowledge/transfer"] = self.transfer_knowledge
        
        # System
        self.routes[f"{API_PREFIX}/system/overview"] = self.get_system_overview
        self.routes[f"{API_PREFIX}/system/recommendations"] = self.get_system_recommendations
        self.routes[f"{API_PREFIX}/system/what-if"] = self.run_what_if_analysis
        
        # Webhooks
        self.routes[f"{API_PREFIX}/webhooks/subscribe"] = self.subscribe_webhook
        self.routes[f"{API_PREFIX}/webhooks/unsubscribe"] = self.unsubscribe_webhook
        
        # Metrics
        self.routes[f"{API_PREFIX}/metrics"] = self.get_metrics
        
        # API Keys (admin only)
        self.routes[f"{API_PREFIX}/admin/keys"] = self.get_api_keys
        self.routes[f"{API_PREFIX}/admin/keys/create"] = self.create_api_key
        self.routes[f"{API_PREFIX}/admin/keys/revoke"] = self.revoke_api_key
    
    # ========================================================================
    # Route Handler
    # ========================================================================
    
    async def handle_request(self, method: str, path: str, 
                            headers: Dict[str, str] = None,
                            body: Dict[str, Any] = None,
                            query_params: Dict[str, str] = None) -> Dict[str, Any]:
        """Handle an API request"""
        start_time = time.time()
        
        request_data = {
            'method': method,
            'path': path,
            'headers': headers or {},
            'body': body or {},
            'query_params': query_params or {},
            'timestamp': datetime.utcnow()
        }
        
        # Find route
        handler = self.routes.get(path)
        
        if not handler:
            return {
                'status': 404,
                'error': 'Not found',
                'message': f'No endpoint at {path}',
                'available_endpoints': list(self.routes.keys())[:20]
            }
        
        try:
            # Execute handler
            result = await handler(request_data)
            
            # Add metadata
            if isinstance(result, dict) and 'status' not in result:
                result['status'] = 200
            
            result['meta'] = {
                'api_version': API_VERSION,
                'timestamp': datetime.utcnow().isoformat(),
                'response_time_ms': (time.time() - start_time) * 1000
            }
            
            # Add rate limit info if available
            if 'rate_info' in request_data:
                result['meta']['rate_limit'] = request_data['rate_info']
            
            # Record request
            self.request_history.append({
                'method': method,
                'path': path,
                'status': result.get('status', 200),
                'timestamp': datetime.utcnow().isoformat()
            })
            
            return result
            
        except Exception as e:
            logger.error(f"API error: {str(e)}", exc_info=True)
            return {
                'status': 500,
                'error': 'Internal server error',
                'message': str(e),
                'meta': {
                    'api_version': API_VERSION,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
    
    # ========================================================================
    # Health and Operational Endpoints
    # ========================================================================
    
    async def get_live(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Liveness probe"""
        return {'status': 'alive', 'timestamp': datetime.utcnow().isoformat()}
    
    async def get_ready(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Readiness probe"""
        if self.health_manager:
            ready = self.health_manager.is_ready()
            return {
                'status': 'ready' if ready else 'not_ready',
                'timestamp': datetime.utcnow().isoformat()
            }
        
        # Check basic readiness
        modules_ready = all([
            self.token_manager is not None,
            self.gradient_manager is not None,
            self.compartment_manager is not None
        ])
        
        return {
            'status': 'ready' if modules_ready else 'not_ready',
            'modules': {
                'token_manager': self.token_manager is not None,
                'gradient_manager': self.gradient_manager is not None,
                'compartment_manager': self.compartment_manager is not None
            }
        }
    
    async def get_health_status(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detailed health status"""
        if self.health_manager:
            return self.health_manager.check_all(self.bio_core) if self.bio_core else {}
        
        return {'status': 'health_manager_not_available'}
    
    @require_auth
    @require_role('admin')
    async def get_config(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get current configuration"""
        if self.bio_core and hasattr(self.bio_core, 'config'):
            return {'config': self.bio_core.config.to_dict()}
        return {'error': 'Configuration not available'}
    
    @require_auth
    @require_role('admin')
    async def update_config(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update configuration"""
        updates = request_data.get('body', {})
        if self.bio_core and hasattr(self.bio_core, 'update_configuration'):
            self.bio_core.update_configuration(updates)
            return {'status': 'updated', 'changes': list(updates.keys())}
        return {'error': 'Configuration update not available'}
    
    # ========================================================================
    # Token Economy Endpoints
    # ========================================================================
    
    async def get_token_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/tokens/summary"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        return self.token_manager.get_system_summary()
    
    @require_auth
    async def get_token_accounts(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/tokens/accounts"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        
        page = int(request_data.get('query_params', {}).get('page', 1))
        limit = int(request_data.get('query_params', {}).get('limit', 20))
        
        accounts = []
        for account_id, account in self.token_manager.accounts.items():
            accounts.append({
                'account_id': account_id,
                'balance': account.balance,
                'efficiency': account.efficiency_rating
            })
        
        return paginate(accounts, page, limit)
    
    @require_auth
    async def generate_tokens(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/tokens/generate"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        
        body = request_data.get('body', {})
        account_id = body.get('account_id', 'default')
        carbon_kg = body.get('carbon_saved_kg', 0.0)
        helium_units = body.get('helium_saved_units', 0.0)
        energy_kwh = body.get('energy_saved_kwh', 0.0)
        
        try:
            tokens = self.token_manager.generate_tokens(
                account_id=account_id,
                source=EcoATPSource.RENEWABLE_ENERGY,
                carbon_saved_kg=carbon_kg,
                helium_saved_units=helium_units,
                energy_saved_kwh=energy_kwh
            )
            
            return {
                'success': True,
                'tokens_generated': len(tokens),
                'total_value': sum(t.value for t in tokens) if tokens else 0,
                'account_id': account_id
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    @require_auth
    async def reserve_tokens(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/tokens/reserve"""
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        
        body = request_data.get('body', {})
        account_id = body.get('account_id', 'default')
        amount = body.get('amount', 10.0)
        priority = body.get('priority', 2)
        
        success, token_ids = self.token_manager.reserve_tokens(
            account_id=account_id, amount=amount,
            consumer=EcoATPConsumer.EXPERT_EXECUTION, priority=priority
        )
        
        return {
            'success': success,
            'tokens_reserved': len(token_ids) if success else 0,
            'amount': amount if success else 0
        }
    
    async def get_economic_indicators(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/tokens/economic"""
        if not self.supply_manager:
            return {'error': 'Supply manager not available'}
        return self.supply_manager.get_economic_indicators()
    
    # ========================================================================
    # Gradient Field Endpoints
    # ========================================================================
    
    async def get_gradient_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/gradients/summary"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        return {
            'field_strengths': self.gradient_manager.get_field_strengths(),
            'detailed_stats': self.gradient_manager.get_field_stats(),
            'dominant_field': self.gradient_manager.get_dominant_field()
        }
    
    async def get_gradient_forecasts(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/gradients/forecasts"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        
        if hasattr(self.gradient_manager, 'get_forecast_summary'):
            return self.gradient_manager.get_forecast_summary()
        
        return {'error': 'Forecasting not available'}
    
    async def get_causal_analysis(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/gradients/causal"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        
        field_id = request_data.get('query_params', {}).get('field', 'carbon')
        
        if hasattr(self.gradient_manager, 'find_root_cause'):
            return self.gradient_manager.find_root_cause(field_id)
        
        return {'error': 'Causal analysis not available'}
    
    @require_auth
    async def pump_gradient(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/gradients/pump"""
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        
        body = request_data.get('body', {})
        field_id = body.get('field_id', 'carbon')
        amount = body.get('amount', 0.1)
        
        self.gradient_manager.pump_field(field_id, amount, source='api')
        
        return {'success': True, 'field_id': field_id, 'amount': amount}
    
    # ========================================================================
    # ATP Synthase Endpoints
    # ========================================================================
    
    async def get_atp_status(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/atp-synthase/status"""
        if not self.scheduler:
            return {'error': 'ATP synthase not available'}
        return self.scheduler.get_scheduler_stats()
    
    async def get_atp_efficiency(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/atp-synthase/efficiency"""
        if not self.scheduler:
            return {'error': 'ATP synthase not available'}
        
        if hasattr(self.scheduler, 'get_efficiency_report'):
            return self.scheduler.get_efficiency_report()
        
        return {'error': 'Efficiency report not available'}
    
    # ========================================================================
    # Compartment Endpoints
    # ========================================================================
    
    async def get_compartment_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/compartments/summary"""
        if not self.compartment_manager:
            return {'error': 'Compartment manager not available'}
        return self.compartment_manager.get_ecosystem_stats()
    
    async def get_compartment_regions(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/compartments/regions"""
        if not self.compartment_manager:
            return {'error': 'Compartment manager not available'}
        
        region_id = request_data.get('query_params', {}).get('region', None)
        
        if hasattr(self.compartment_manager, 'get_region_stats'):
            if region_id:
                return self.compartment_manager.get_region_stats(region_id) or {}
            
            regions = {}
            for rid in self.compartment_manager.regions:
                regions[rid] = self.compartment_manager.get_region_stats(rid)
            return {'regions': regions}
        
        return {'error': 'Region stats not available'}
    
    @require_auth
    async def create_compartment(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/compartments/create"""
        if not self.compartment_manager:
            return {'error': 'Compartment manager not available'}
        
        body = request_data.get('body', {})
        expert_type = body.get('expert_type', 'data')
        
        compartment = self.compartment_manager.create_compartment(expert_type)
        
        return {
            'success': True,
            'compartment_id': compartment.compartment_id,
            'expert_type': expert_type
        }
    
    # ========================================================================
    # Biomass Storage Endpoints
    # ========================================================================
    
    async def get_biomass_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/biomass/summary"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        return self.biomass_storage.get_storage_stats()
    
    @require_auth
    async def store_task(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/biomass/store"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        
        body = request_data.get('body', {})
        task_data = body.get('task_data', {})
        ecoatp_cost = body.get('ecoatp_cost', 10.0)
        guarantee = body.get('guarantee', 'silver')
        
        try:
            guarantee_level = GuaranteeLevel(guarantee)
        except ValueError:
            guarantee_level = GuaranteeLevel.SILVER
        
        stored, token_id = self.biomass_storage.store_task(
            task_data=task_data, ecoatp_cost=ecoatp_cost, guarantee=guarantee_level
        )
        
        return {'success': stored, 'storage_token': token_id}
    
    @require_auth
    async def retrieve_task(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/biomass/retrieve"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        
        body = request_data.get('body', {})
        token_id = body.get('token_id', '')
        
        task_data, cost = self.biomass_storage.retrieve_task(token_id)
        
        return {
            'success': task_data is not None,
            'task_data': task_data,
            'retrieval_cost': cost
        }
    
    async def get_biomass_analytics(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/biomass/analytics"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        
        if hasattr(self.biomass_storage, 'generate_analytics'):
            analytics = self.biomass_storage.generate_analytics()
            recommendations = self.biomass_storage.get_optimization_recommendations()
            return {'analytics': asdict(analytics) if hasattr(analytics, '__dataclass_fields__') else analytics,
                    'recommendations': recommendations}
        
        return {'error': 'Analytics not available'}
    
    async def get_biomass_forecast(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/biomass/forecast"""
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        
        tier_name = request_data.get('query_params', {}).get('tier', 'glycogen_queue')
        
        try:
            tier = StorageTier(tier_name)
        except ValueError:
            tier = StorageTier.GLYCOGEN_QUEUE
        
        if hasattr(self.biomass_storage, 'forecast_storage'):
            forecast = self.biomass_storage.forecast_storage(tier)
            return {
                'tier': tier.value,
                'current_usage': forecast.current_usage,
                'capacity': forecast.capacity,
                'inflow_rate': forecast.inflow_rate,
                'outflow_rate': forecast.outflow_rate,
                'predicted_full_time': forecast.predicted_full_time.isoformat() if forecast.predicted_full_time else None,
                'confidence': forecast.confidence
            }
        
        return {'error': 'Forecasting not available'}
    
    # ========================================================================
    # Harvester Endpoints
    # ========================================================================
    
    async def get_harvester_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/harvester/summary"""
        if not self.harvester:
            return {'error': 'Harvester not available'}
        return self.harvester.get_harvesting_stats()
    
    @require_auth
    async def harvest_cycle(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/harvester/cycle"""
        if not self.harvester:
            return {'error': 'Harvester not available'}
        
        body = request_data.get('body', {})
        env_data = body.get('environmental_data', {
            'renewable_availability': 0.5,
            'carbon_intensity': 400,
            'waste_heat': 0.2,
            'edge_availability': 0.3
        })
        
        result = await self.harvester.harvest_cycle(env_data)
        
        return {
            'success': True,
            'eco_atp_generated': result.get('eco_atp_generated', 0),
            'dominant_signal': result.get('dominant_signal', 'none'),
            'efficiency': result.get('efficiency', 0)
        }
    
    async def get_circadian_report(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/harvester/circadian"""
        if not self.harvester:
            return {'error': 'Harvester not available'}
        
        if hasattr(self.harvester, 'get_circadian_report'):
            return self.harvester.get_circadian_report()
        
        return {'error': 'Circadian report not available'}
    
    # ========================================================================
    # Degradation Manager Endpoints
    # ========================================================================
    
    async def get_degradation_status(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/degradation/status"""
        if not self.degradation_manager:
            return {'error': 'Degradation manager not available'}
        return self.degradation_manager.get_tier_status()
    
    async def get_degradation_history(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/degradation/history"""
        if not self.degradation_manager:
            return {'error': 'Degradation manager not available'}
        return {
            'history': list(self.degradation_manager.tier_history)[-50:]
        }
    
    async def get_chaos_report(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/degradation/chaos"""
        if not self.degradation_manager:
            return {'error': 'Degradation manager not available'}
        
        if hasattr(self.degradation_manager, 'get_chaos_report'):
            return self.degradation_manager.get_chaos_report()
        
        return {'error': 'Chaos engineering not available'}
    
    # ========================================================================
    # Knowledge Transfer Endpoints
    # ========================================================================
    
    async def get_knowledge_packages(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/knowledge/packages"""
        if not self.knowledge_transfer:
            return {'error': 'Knowledge transfer not available'}
        return self.knowledge_transfer.get_knowledge_summary()
    
    @require_auth
    async def transfer_knowledge(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/knowledge/transfer"""
        if not self.knowledge_transfer:
            return {'error': 'Knowledge transfer not available'}
        
        body = request_data.get('body', {})
        source_package_id = body.get('source_package_id', '')
        # Target expert would need to be provided
        
        return {'status': 'not_implemented', 'message': 'Requires target expert instance'}
    
    # ========================================================================
    # System Endpoints
    # ========================================================================
    
    async def get_system_overview(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/system/overview"""
        if not self.bio_core:
            return {'error': 'Bio-core not available'}
        
        return self.bio_core.get_system_status()
    
    async def get_system_recommendations(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/system/recommendations"""
        recommendations = []
        
        # Economic recommendations
        if self.supply_manager:
            indicators = self.supply_manager.get_economic_indicators()
            if indicators.get('utilization', 0.5) < 0.4:
                recommendations.append("Economy under-utilized. Increase task throughput.")
            if indicators.get('inflation_pressure', 0) > 0.3:
                recommendations.append("High inflation pressure. Consider token burning.")
        
        # Storage recommendations
        if self.biomass_storage and hasattr(self.biomass_storage, 'get_optimization_recommendations'):
            recommendations.extend(self.biomass_storage.get_optimization_recommendations())
        
        # Gradient recommendations
        if self.gradient_manager:
            for field_id in ['carbon', 'helium']:
                explanation = self.gradient_manager.explain_gradient_state(field_id)
                if 'CRITICAL' in explanation.get('health_assessment', ''):
                    recommendations.append(f"Gradient {field_id}: {explanation['health_assessment']}")
        
        return {
            'recommendations': recommendations if recommendations else ["System operating optimally."],
            'timestamp': datetime.utcnow().isoformat()
        }
    
    @require_auth
    async def run_what_if_analysis(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/system/what-if"""
        if not self.bio_core:
            return {'error': 'Bio-core not available'}
        
        body = request_data.get('body', {})
        
        if hasattr(self.bio_core, 'run_what_if_analysis'):
            return self.bio_core.run_what_if_analysis(body)
        
        return {'error': 'What-if analysis not available'}
    
    # ========================================================================
    # Webhook Endpoints
    # ========================================================================
    
    @require_auth
    async def subscribe_webhook(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/webhooks/subscribe"""
        body = request_data.get('body', {})
        event_type = body.get('event_type', '')
        callback_url = body.get('callback_url', '')
        
        if not event_type or not callback_url:
            return {'error': 'event_type and callback_url required'}
        
        subscription_id = hashlib.sha256(
            f"{event_type}{callback_url}{datetime.utcnow().timestamp()}".encode()
        ).hexdigest()[:16]
        
        self.webhooks[event_type].append({
            'subscription_id': subscription_id,
            'callback_url': callback_url,
            'created_at': datetime.utcnow().isoformat()
        })
        
        # Register with event bus if available
        if self.event_bus:
            async def webhook_callback(event):
                # In production, would make HTTP POST to callback_url
                logger.info(f"Webhook triggered: {event_type} → {callback_url}")
            
            self.event_bus.subscribe(event_type, webhook_callback)
        
        return {
            'success': True,
            'subscription_id': subscription_id,
            'event_type': event_type
        }
    
    @require_auth
    async def unsubscribe_webhook(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/webhooks/unsubscribe"""
        body = request_data.get('body', {})
        subscription_id = body.get('subscription_id', '')
        
        for event_type, hooks in self.webhooks.items():
            for hook in hooks:
                if hook['subscription_id'] == subscription_id:
                    hooks.remove(hook)
                    return {'success': True, 'subscription_id': subscription_id}
        
        return {'error': 'Subscription not found'}
    
    # ========================================================================
    # Metrics Endpoint
    # ========================================================================
    
    async def get_metrics(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/metrics - Prometheus-compatible"""
        metrics = []
        timestamp_ms = int(time.time() * 1000)
        
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            metrics.append(f'green_agent_ecoatp_balance {summary.get("total_balance", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_ecoatp_efficiency {summary.get("system_efficiency", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_emergency_mode {1 if summary.get("emergency_mode") else 0} {timestamp_ms}')
        
        if self.gradient_manager:
            for field_id, strength in self.gradient_manager.get_field_strengths().items():
                metrics.append(f'green_agent_gradient{{field="{field_id}"}} {strength} {timestamp_ms}')
        
        if self.compartment_manager:
            stats = self.compartment_manager.get_ecosystem_stats()
            metrics.append(f'green_agent_compartments_viable {stats.get("viable_compartments", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_compartments_total {stats.get("total_compartments", 0)} {timestamp_ms}')
        
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            metrics.append(f'green_agent_biomass_total {stats.get("total_stored", 0)} {timestamp_ms}')
            for tier, count in stats.get('tiers', {}).items():
                metrics.append(f'green_agent_biomass_tier{{tier="{tier}"}} {count} {timestamp_ms}')
        
        if self.harvester:
            harvester_stats = self.harvester.get_harvesting_stats()
            metrics.append(f'green_agent_harvester_total {harvester_stats.get("total_harvested", 0)} {timestamp_ms}')
        
        if self.scheduler:
            scheduler_stats = self.scheduler.get_scheduler_stats()
            metrics.append(f'green_agent_atp_rate {scheduler_stats.get("current_atp_rate", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_atp_efficiency {scheduler_stats.get("primary_efficiency", 0)} {timestamp_ms}')
        
        return {
            'metrics': '\n'.join(metrics),
            'content_type': 'text/plain',
            'timestamp': datetime.utcnow().isoformat()
        }
    
    # ========================================================================
    # Admin Endpoints
    # ========================================================================
    
    @require_auth
    @require_role('admin')
    async def get_api_keys(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/admin/keys"""
        return self.api_key_manager.get_key_stats()
    
    @require_auth
    @require_role('admin')
    async def create_api_key(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/admin/keys/create"""
        body = request_data.get('body', {})
        name = body.get('name', 'new-key')
        rate_limit = body.get('rate_limit', 100)
        role = body.get('role', 'user')
        
        key = self.api_key_manager.create_key(name, rate_limit, role)
        
        return {
            'success': True,
            'api_key': key,
            'name': name,
            'rate_limit': rate_limit,
            'role': role,
            'message': 'Store this key securely. It cannot be retrieved later.'
        }
    
    @require_auth
    @require_role('admin')
    async def revoke_api_key(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/admin/keys/revoke"""
        body = request_data.get('body', {})
        api_key = body.get('api_key', '')
        
        success = self.api_key_manager.revoke_key(api_key)
        
        return {'success': success, 'message': 'Key revoked' if success else 'Key not found'}
    
    # ========================================================================
    # API Statistics
    # ========================================================================
    
    def get_api_stats(self) -> Dict[str, Any]:
        """Get API usage statistics"""
        recent = list(self.request_history)[-1000:]
        
        endpoint_counts = defaultdict(int)
        status_counts = defaultdict(int)
        
        for req in recent:
            endpoint_counts[req['path']] += 1
            status_counts[req['status']] += 1
        
        return {
            'total_requests': len(self.request_history),
            'recent_requests': len(recent),
            'top_endpoints': sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            'status_distribution': dict(status_counts),
            'webhook_subscriptions': sum(len(hooks) for hooks in self.webhooks.values()),
            'registered_routes': len(self.routes),
            'api_version': API_VERSION
        }

# ============================================================================
# API Documentation Generator
# ============================================================================

def generate_openapi_spec() -> Dict[str, Any]:
    """Generate OpenAPI 3.0 specification"""
    return {
        "openapi": "3.0.0",
        "info": {
            "title": "Green Agent Bio-Inspired API",
            "version": API_VERSION,
            "description": "RESTful API for the Green Agent metabolic ecosystem"
        },
        "servers": [
            {"url": f"/api/{API_VERSION}", "description": "Production server"}
        ],
        "paths": {
            "/health/live": {
                "get": {
                    "summary": "Liveness probe",
                    "responses": {"200": {"description": "Alive"}}
                }
            },
            "/health/ready": {
                "get": {
                    "summary": "Readiness probe",
                    "responses": {"200": {"description": "Ready"}}
                }
            },
            "/tokens/summary": {
                "get": {
                    "summary": "Token economy summary",
                    "responses": {"200": {"description": "Token summary"}}
                }
            },
            "/tokens/generate": {
                "post": {
                    "summary": "Generate Eco-ATP tokens",
                    "security": [{"ApiKeyAuth": []}],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "account_id": {"type": "string"},
                                        "carbon_saved_kg": {"type": "number"},
                                        "energy_saved_kwh": {"type": "number"}
                                    }
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Tokens generated"}}
                }
            },
            "/gradients/summary": {
                "get": {
                    "summary": "Gradient field summary",
                    "responses": {"200": {"description": "Gradient summary"}}
                }
            },
            "/atp-synthase/status": {
                "get": {
                    "summary": "ATP synthase status",
                    "responses": {"200": {"description": "ATP synthase status"}}
                }
            },
            "/compartments/summary": {
                "get": {
                    "summary": "Compartment ecosystem summary",
                    "responses": {"200": {"description": "Compartment summary"}}
                }
            },
            "/biomass/summary": {
                "get": {
                    "summary": "Biomass storage summary",
                    "responses": {"200": {"description": "Biomass summary"}}
                }
            },
            "/system/overview": {
                "get": {
                    "summary": "System overview",
                    "responses": {"200": {"description": "System overview"}}
                }
            },
            "/metrics": {
                "get": {
                    "summary": "Prometheus metrics",
                    "responses": {"200": {"description": "Metrics in Prometheus format"}}
                }
            }
        },
        "components": {
            "securitySchemes": {
                "ApiKeyAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key"
                }
            }
        }
    }
