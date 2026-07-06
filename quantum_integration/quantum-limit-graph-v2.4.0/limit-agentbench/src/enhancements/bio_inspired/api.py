# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/api.py
# Complete enhanced file v6.0.0 with all improvements

"""
Enhanced Bio-Inspired API v6.0.0
Complete RESTful API with authentication, rate limiting, versioning,
complete module coverage, operational endpoints, pagination, webhook support,
OAuth2/JWT support (NEW), Adaptive rate limiting (NEW), Webhook retry with exponential backoff (NEW),
Histogram metrics for latency distribution (NEW), Swagger UI endpoint (NEW)
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
import jwt
import secrets
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ============================================================================
# API Version
# ============================================================================

API_VERSION = "v1"
API_PREFIX = f"/api/{API_VERSION}"

# ============================================================================
# OAuth2/JWT Support (NEW)
# ============================================================================

@dataclass
class OAuth2Config:
    """OAuth2/JWT configuration"""
    issuer: str = "green-agent"
    audience: str = "green-agent-api"
    secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    access_token_expiry_minutes: int = 60
    refresh_token_expiry_days: int = 7
    algorithm: str = "HS256"

class OAuth2Manager:
    """
    OAuth2/JWT authentication manager.
    
    Features:
    - JWT token generation and validation
    - Access and refresh tokens
    - Token revocation
    - Client credentials flow
    """
    
    def __init__(self, config: Optional[OAuth2Config] = None):
        self.config = config or OAuth2Config()
        self.revoked_tokens: set = set()
        self.refresh_tokens: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        
        logger.info("OAuth2 Manager initialized")
    
    def create_access_token(self, client_id: str, scopes: List[str] = None) -> str:
        """Create a JWT access token"""
        now = datetime.utcnow()
        payload = {
            'sub': client_id,
            'iss': self.config.issuer,
            'aud': self.config.audience,
            'iat': now,
            'exp': now + timedelta(minutes=self.config.access_token_expiry_minutes),
            'scopes': scopes or ['read'],
            'jti': secrets.token_hex(16)
        }
        return jwt.encode(payload, self.config.secret_key, algorithm=self.config.algorithm)
    
    def create_refresh_token(self, client_id: str) -> str:
        """Create a refresh token"""
        token = secrets.token_urlsafe(32)
        self.refresh_tokens[token] = {
            'client_id': client_id,
            'created_at': datetime.utcnow(),
            'expires_at': datetime.utcnow() + timedelta(days=self.config.refresh_token_expiry_days)
        }
        return token
    
    def validate_token(self, token: str) -> Optional[Dict]:
        """Validate a JWT token"""
        if token in self.revoked_tokens:
            return None
        
        try:
            payload = jwt.decode(
                token,
                self.config.secret_key,
                algorithms=[self.config.algorithm],
                audience=self.config.audience,
                issuer=self.config.issuer
            )
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
    
    def revoke_token(self, token: str) -> bool:
        """Revoke a token"""
        self.revoked_tokens.add(token)
        return True
    
    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        """Refresh an access token using a refresh token"""
        if refresh_token not in self.refresh_tokens:
            return None
        
        token_data = self.refresh_tokens[refresh_token]
        if datetime.utcnow() > token_data['expires_at']:
            return None
        
        # Revoke old refresh token
        del self.refresh_tokens[refresh_token]
        
        # Create new tokens
        new_access = self.create_access_token(token_data['client_id'])
        new_refresh = self.create_refresh_token(token_data['client_id'])
        
        return {
            'access_token': new_access,
            'refresh_token': new_refresh,
            'expires_in': self.config.access_token_expiry_minutes * 60
        }
    
    def get_oauth_config(self) -> Dict:
        """Get OAuth2 configuration for clients"""
        return {
            'issuer': self.config.issuer,
            'audience': self.config.audience,
            'token_endpoint': f"{API_PREFIX}/oauth/token",
            'revocation_endpoint': f"{API_PREFIX}/oauth/revoke",
            'grant_types': ['client_credentials', 'refresh_token']
        }

# ============================================================================
# Enhanced Rate Limiter with Adaptive Limits (NEW)
# ============================================================================

class AdaptiveRateLimiter:
    """
    Adaptive rate limiting based on system load.
    
    Features:
    - Dynamic rate limits based on system load
    - Adaptive thresholds
    - Load-based adjustment
    - Historical tracking
    """
    
    def __init__(self):
        self.base_limits: Dict[str, int] = {
            'read': 100,
            'write': 50,
            'admin': 20
        }
        self.current_multiplier = 1.0
        self.load_history: deque = deque(maxlen=100)
        self.rate_records: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()
        
        logger.info("Adaptive Rate Limiter initialized")
    
    def update_system_load(self, load: float):
        """Update system load for adaptive rate limiting"""
        self.load_history.append(load)
        
        # Calculate average load
        if len(self.load_history) > 10:
            avg_load = sum(self.load_history) / len(self.load_history)
            if avg_load > 0.8:
                self.current_multiplier = 0.5
            elif avg_load > 0.6:
                self.current_multiplier = 0.75
            elif avg_load < 0.3:
                self.current_multiplier = 1.5
            else:
                self.current_multiplier = 1.0
    
    def get_rate_limit(self, scope: str) -> int:
        """Get rate limit for a scope"""
        base = self.base_limits.get(scope, 50)
        return int(base * self.current_multiplier)
    
    def check_rate_limit(self, key: str, scope: str) -> Tuple[bool, Dict]:
        """Check if request is within rate limit"""
        limit = self.get_rate_limit(scope)
        
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        
        recent = [t for t in self.rate_records[key] if t > minute_ago]
        self.rate_records[key] = deque(recent, maxlen=1000)
        
        if len(recent) >= limit:
            retry_after = 60 - (now - recent[0]).total_seconds()
            return False, {
                'error': 'Rate limit exceeded',
                'retry_after_seconds': max(0, int(retry_after)),
                'limit': limit,
                'current_usage': len(recent),
                'system_load': self.load_history[-1] if self.load_history else 0.5
            }
        
        self.rate_records[key].append(now)
        
        return True, {
            'limit': limit,
            'remaining': limit - len(recent) - 1,
            'reset_seconds': 60 - (now - (recent[0] if recent else now)).total_seconds()
        }
    
    def get_limiter_stats(self) -> Dict:
        """Get rate limiter statistics"""
        return {
            'current_multiplier': self.current_multiplier,
            'base_limits': self.base_limits,
            'avg_load': sum(self.load_history) / len(self.load_history) if self.load_history else 0.5,
            'active_scopes': list(self.rate_records.keys())
        }

# ============================================================================
# Enhanced Webhook Manager with Retry (NEW)
# ============================================================================

@dataclass
class WebhookSubscription:
    """Webhook subscription with retry configuration"""
    subscription_id: str
    event_type: str
    callback_url: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0
    max_retries: int = 5
    last_delivery: Optional[datetime] = None
    last_error: Optional[str] = None
    status: str = "active"  # active, paused, failed

class WebhookManager:
    """
    Webhook manager with retry mechanism.
    
    Features:
    - Exponential backoff retry
    - Delivery tracking
    - Failure handling
    - Status monitoring
    """
    
    def __init__(self, event_broker=None, session=None):
        self.event_broker = event_broker
        self.session = session
        self.subscriptions: Dict[str, WebhookSubscription] = {}
        self.delivery_queue: deque = deque(maxlen=10000)
        self._lock = asyncio.Lock()
        self._processing = False
        
        logger.info("Webhook Manager initialized")
    
    async def subscribe(self, event_type: str, callback_url: str) -> str:
        """Subscribe a webhook to an event type"""
        subscription_id = hashlib.sha256(
            f"{event_type}{callback_url}{datetime.utcnow().timestamp()}".encode()
        ).hexdigest()[:16]
        
        subscription = WebhookSubscription(
            subscription_id=subscription_id,
            event_type=event_type,
            callback_url=callback_url
        )
        
        async with self._lock:
            self.subscriptions[subscription_id] = subscription
            
            # Register with event broker
            if self.event_broker:
                async def webhook_callback(event):
                    await self._enqueue_delivery(subscription_id, event)
                self.event_broker.subscribe(event_type, webhook_callback)
        
        logger.info(f"Webhook subscribed: {subscription_id} → {event_type}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe a webhook"""
        async with self._lock:
            if subscription_id not in self.subscriptions:
                return False
            
            subscription = self.subscriptions[subscription_id]
            subscription.status = "cancelled"
            
            # Unsubscribe from event broker
            if self.event_broker:
                # Find and remove callback
                callbacks = self.event_broker.subscribers.get(subscription.event_type, [])
                self.event_broker.subscribers[subscription.event_type] = [
                    c for c in callbacks 
                    if not (hasattr(c, '__name__') and c.__name__ == f"webhook_callback_{subscription_id}")
                ]
            
            logger.info(f"Webhook unsubscribed: {subscription_id}")
            return True
    
    async def _enqueue_delivery(self, subscription_id: str, event):
        """Queue a webhook delivery"""
        subscription = self.subscriptions.get(subscription_id)
        if not subscription or subscription.status != "active":
            return
        
        self.delivery_queue.append({
            'subscription_id': subscription_id,
            'event': event,
            'attempts': 0,
            'next_attempt': datetime.utcnow()
        })
        
        if not self._processing:
            await self._process_deliveries()
    
    async def _process_deliveries(self):
        """Process queued webhook deliveries with retry"""
        if self._processing:
            return
        
        self._processing = True
        
        try:
            while self.delivery_queue:
                delivery = self.delivery_queue[0]
                
                # Check if it's time to retry
                if datetime.utcnow() < delivery['next_attempt']:
                    await asyncio.sleep(1)
                    continue
                
                # Get subscription
                subscription = self.subscriptions.get(delivery['subscription_id'])
                if not subscription or subscription.status != "active":
                    self.delivery_queue.popleft()
                    continue
                
                # Attempt delivery with exponential backoff
                try:
                    success = await self._deliver_webhook(
                        subscription.callback_url,
                        delivery['event']
                    )
                    
                    if success:
                        subscription.last_delivery = datetime.utcnow()
                        delivery['attempts'] = 0
                        self.delivery_queue.popleft()
                        logger.debug(f"Webhook delivered: {subscription.subscription_id}")
                    else:
                        delivery['attempts'] += 1
                        
                        # Calculate backoff: 2^attempts * 1 second
                        backoff = min(60, 2 ** delivery['attempts'])
                        delivery['next_attempt'] = datetime.utcnow() + timedelta(seconds=backoff)
                        
                        if delivery['attempts'] >= subscription.max_retries:
                            subscription.status = "failed"
                            subscription.last_error = "Max retries exceeded"
                            self.delivery_queue.popleft()
                            logger.warning(f"Webhook failed: {subscription.subscription_id}")
                
                except Exception as e:
                    logger.error(f"Webhook delivery error: {e}")
                    delivery['attempts'] += 1
                    backoff = min(60, 2 ** delivery['attempts'])
                    delivery['next_attempt'] = datetime.utcnow() + timedelta(seconds=backoff)
                
                await asyncio.sleep(0.1)
        
        finally:
            self._processing = False
    
    async def _deliver_webhook(self, callback_url: str, event) -> bool:
        """Deliver a webhook payload"""
        if not self.session:
            # Simulate delivery for testing
            logger.info(f"Webhook would deliver to: {callback_url}")
            return True
        
        try:
            async with self.session.post(
                callback_url,
                json={
                    'event_type': event.event_type,
                    'timestamp': event.timestamp.isoformat(),
                    'data': event.data,
                    'correlation_id': event.correlation_id
                },
                timeout=10
            ) as response:
                return response.status in [200, 201, 202, 204]
        except Exception:
            return False
    
    def get_webhook_stats(self) -> Dict:
        """Get webhook statistics"""
        active = [s for s in self.subscriptions.values() if s.status == "active"]
        failed = [s for s in self.subscriptions.values() if s.status == "failed"]
        
        return {
            'total_subscriptions': len(self.subscriptions),
            'active_subscriptions': len(active),
            'failed_subscriptions': len(failed),
            'queue_size': len(self.delivery_queue),
            'subscriptions': [
                {
                    'id': s.subscription_id,
                    'event_type': s.event_type,
                    'status': s.status,
                    'last_delivery': s.last_delivery.isoformat() if s.last_delivery else None,
                    'last_error': s.last_error
                }
                for s in list(self.subscriptions.values())[-10:]
            ]
        }

# ============================================================================
# Authentication and Rate Limiting (Enhanced)
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
# Decorators (Enhanced)
# ============================================================================

def require_auth(func):
    """Decorator to require API key authentication"""
    @wraps(func)
    async def wrapper(self, request_data: Dict[str, Any], *args, **kwargs):
        # Check OAuth2 token first
        auth_header = request_data.get('headers', {}).get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header[7:]
            if hasattr(self, 'oauth2_manager') and self.oauth2_manager:
                payload = self.oauth2_manager.validate_token(token)
                if payload:
                    request_data['oauth_payload'] = payload
                    request_data['auth_type'] = 'oauth2'
                    return await func(self, request_data, *args, **kwargs)
        
        # Fallback to API key
        api_key = request_data.get('headers', {}).get('X-API-Key', '')
        
        if not api_key:
            return {'status': 401, 'error': 'Authentication required', 
                    'message': 'Provide X-API-Key header or Bearer token'}
        
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
        request_data['auth_type'] = 'api_key'
        
        return await func(self, request_data, *args, **kwargs)
    return wrapper

def require_role(role: str):
    """Decorator to require specific role"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, request_data: Dict[str, Any], *args, **kwargs):
            # Check OAuth2 scopes
            oauth_payload = request_data.get('oauth_payload', {})
            if oauth_payload:
                scopes = oauth_payload.get('scopes', [])
                if role in scopes or 'admin' in scopes:
                    return await func(self, request_data, *args, **kwargs)
            
            # Check API key role
            key_data = request_data.get('api_key_info', {})
            if key_data.get('role') != role and key_data.get('role') != 'admin':
                return {'status': 403, 'error': f'Role {role} required'}
            return await func(self, request_data, *args, **kwargs)
        return wrapper
    return decorator

# ============================================================================
# Pagination Helper (Enhanced)
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
    Enhanced Bio-Inspired API v6.0.0
    
    Complete RESTful API with:
    - Authentication and rate limiting
    - Complete module coverage (all 10+ modules)
    - API versioning
    - Operational endpoints (health, config)
    - Pagination and filtering
    - Webhook support
    - Response caching headers
    - OAuth2/JWT support (NEW)
    - Adaptive rate limiting (NEW)
    - Webhook retry with exponential backoff (NEW)
    - Histogram metrics for latency (NEW)
    - Swagger UI endpoint (NEW)
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
        self.event_bus = getattr(bio_core, 'event_broker', None) if bio_core else None
        self.health_manager = getattr(bio_core, 'health_manager', None) if bio_core else None
        self.state_manager = getattr(bio_core, 'state_manager', None) if bio_core else None
        
        # NEW: OAuth2 Manager
        self.oauth2_manager = OAuth2Manager()
        
        # NEW: Adaptive Rate Limiter
        self.adaptive_limiter = AdaptiveRateLimiter()
        
        # API key manager
        self.api_key_manager = APIKeyManager()
        
        # NEW: Webhook Manager
        self.webhook_manager = WebhookManager(self.event_bus)
        
        # Request history with latency tracking
        self.request_history: deque = deque(maxlen=10000)
        self.latency_histogram: Dict[str, List[float]] = defaultdict(list)
        
        # Route registry
        self.routes: Dict[str, Callable] = {}
        self._register_routes()
        
        # Start webhook processing
        asyncio.create_task(self.webhook_manager._process_deliveries())
        
        logger.info(f"Enhanced Bio-Inspired API v6.0.0 initialized ({len(self.routes)} routes)")
    
    def _register_routes(self):
        """Register all API routes"""
        
        # Health and Operational
        self.routes[f"{API_PREFIX}/health/live"] = self.get_live
        self.routes[f"{API_PREFIX}/health/ready"] = self.get_ready
        self.routes[f"{API_PREFIX}/health/status"] = self.get_health_status
        self.routes[f"{API_PREFIX}/config"] = self.get_config
        self.routes[f"{API_PREFIX}/config"] = self.update_config  # PUT handled separately
        
        # OAuth2 (NEW)
        self.routes[f"{API_PREFIX}/oauth/token"] = self.oauth_token
        self.routes[f"{API_PREFIX}/oauth/revoke"] = self.oauth_revoke
        self.routes[f"{API_PREFIX}/oauth/config"] = self.oauth_config
        
        # Swagger UI (NEW)
        self.routes[f"{API_PREFIX}/docs"] = self.get_swagger_ui
        self.routes[f"{API_PREFIX}/openapi.json"] = self.get_openapi_spec
        
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
        self.routes[f"{API_PREFIX}/system/load"] = self.get_system_load
        
        # Webhooks
        self.routes[f"{API_PREFIX}/webhooks/subscribe"] = self.subscribe_webhook
        self.routes[f"{API_PREFIX}/webhooks/unsubscribe"] = self.unsubscribe_webhook
        self.routes[f"{API_PREFIX}/webhooks/stats"] = self.get_webhook_stats
        
        # Metrics
        self.routes[f"{API_PREFIX}/metrics"] = self.get_metrics
        self.routes[f"{API_PREFIX}/metrics/histograms"] = self.get_metric_histograms
        
        # API Keys (admin only)
        self.routes[f"{API_PREFIX}/admin/keys"] = self.get_api_keys
        self.routes[f"{API_PREFIX}/admin/keys/create"] = self.create_api_key
        self.routes[f"{API_PREFIX}/admin/keys/revoke"] = self.revoke_api_key
    
    # ========================================================================
    # OAuth2 Endpoints (NEW)
    # ========================================================================
    
    async def oauth_token(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/oauth/token - OAuth2 token endpoint"""
        body = request_data.get('body', {})
        grant_type = body.get('grant_type', 'client_credentials')
        client_id = body.get('client_id', 'default')
        scopes = body.get('scope', 'read').split()
        
        if grant_type == 'client_credentials':
            access_token = self.oauth2_manager.create_access_token(client_id, scopes)
            refresh_token = self.oauth2_manager.create_refresh_token(client_id)
            
            return {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'token_type': 'Bearer',
                'expires_in': self.oauth2_manager.config.access_token_expiry_minutes * 60,
                'scope': ' '.join(scopes)
            }
        
        elif grant_type == 'refresh_token':
            refresh_token = body.get('refresh_token', '')
            result = self.oauth2_manager.refresh_access_token(refresh_token)
            if result:
                return result
            return {'error': 'invalid_grant', 'error_description': 'Invalid refresh token'}
        
        return {'error': 'unsupported_grant_type', 'error_description': 'Grant type not supported'}
    
    async def oauth_revoke(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/oauth/revoke - OAuth2 token revocation"""
        body = request_data.get('body', {})
        token = body.get('token', '')
        
        if token:
            self.oauth2_manager.revoke_token(token)
            return {'success': True}
        
        return {'error': 'invalid_token', 'error_description': 'Token required'}
    
    async def oauth_config(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/oauth/config - OAuth2 configuration"""
        return self.oauth2_manager.get_oauth_config()
    
    # ========================================================================
    # Swagger UI Endpoints (NEW)
    # ========================================================================
    
    async def get_swagger_ui(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/docs - Swagger UI"""
        return {
            'html': self._generate_swagger_html(),
            'content_type': 'text/html'
        }
    
    async def get_openapi_spec(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/openapi.json - OpenAPI specification"""
        return self._generate_openapi_spec()
    
    def _generate_swagger_html(self) -> str:
        """Generate Swagger UI HTML"""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Green Agent Bio-Inspired API - Swagger UI</title>
            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
        </head>
        <body>
            <div id="swagger-ui"></div>
            <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
            <script>
                window.onload = function() {{
                    const ui = SwaggerUIBundle({{
                        url: "{API_PREFIX}/openapi.json",
                        dom_id: '#swagger-ui',
                        presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIBundle.SwaggerUIStandalonePreset
                        ],
                        layout: "BaseLayout",
                        deepLinking: true
                    }});
                    window.ui = ui;
                }};
            </script>
        </body>
        </html>
        """
    
    def _generate_openapi_spec(self) -> Dict[str, Any]:
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
                "/oauth/token": {
                    "post": {
                        "summary": "OAuth2 token endpoint",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "grant_type": {"type": "string"},
                                            "client_id": {"type": "string"},
                                            "scope": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        },
                        "responses": {"200": {"description": "Token response"}}
                    }
                },
                "/tokens/summary": {
                    "get": {
                        "summary": "Token economy summary",
                        "security": [{"ApiKeyAuth": []}, {"OAuth2": []}],
                        "responses": {"200": {"description": "Token summary"}}
                    }
                },
                "/tokens/generate": {
                    "post": {
                        "summary": "Generate Eco-ATP tokens",
                        "security": [{"ApiKeyAuth": []}, {"OAuth2": []}],
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
                "/system/load": {
                    "get": {
                        "summary": "System load status",
                        "responses": {"200": {"description": "System load"}}
                    }
                },
                "/webhooks/stats": {
                    "get": {
                        "summary": "Webhook statistics",
                        "security": [{"ApiKeyAuth": []}, {"OAuth2": []}],
                        "responses": {"200": {"description": "Webhook stats"}}
                    }
                },
                "/metrics/histograms": {
                    "get": {
                        "summary": "Histogram metrics for latency distribution",
                        "responses": {"200": {"description": "Histogram metrics"}}
                    }
                }
            },
            "components": {
                "securitySchemes": {
                    "ApiKeyAuth": {
                        "type": "apiKey",
                        "in": "header",
                        "name": "X-API-Key"
                    },
                    "OAuth2": {
                        "type": "oauth2",
                        "flows": {
                            "clientCredentials": {
                                "tokenUrl": f"{API_PREFIX}/oauth/token",
                                "scopes": {
                                    "read": "Read access",
                                    "write": "Write access",
                                    "admin": "Admin access"
                                }
                            }
                        }
                    }
                }
            }
        }
    
    # ========================================================================
    # Route Handler (Enhanced)
    # ========================================================================
    
    async def handle_request(self, method: str, path: str, 
                            headers: Dict[str, str] = None,
                            body: Dict[str, Any] = None,
                            query_params: Dict[str, str] = None) -> Dict[str, Any]:
        """Handle an API request with latency tracking"""
        start_time = time.time()
        
        request_data = {
            'method': method,
            'path': path,
            'headers': headers or {},
            'body': body or {},
            'query_params': query_params or {},
            'timestamp': datetime.utcnow()
        }
        
        # Update adaptive rate limiter
        if self.bio_core and hasattr(self.bio_core, 'get_system_status'):
            status = self.bio_core.get_system_status()
            load = status.get('token_economy', {}).get('utilization', 0.5)
            self.adaptive_limiter.update_system_load(load)
        
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
            
            latency_ms = (time.time() - start_time) * 1000
            
            result['meta'] = {
                'api_version': API_VERSION,
                'timestamp': datetime.utcnow().isoformat(),
                'response_time_ms': latency_ms
            }
            
            # Track latency in histogram
            self.latency_histogram[path].append(latency_ms)
            if len(self.latency_histogram[path]) > 1000:
                self.latency_histogram[path] = self.latency_histogram[path][-1000:]
            
            # Add rate limit info if available
            if 'rate_info' in request_data:
                result['meta']['rate_limit'] = request_data['rate_info']
            
            # Record request
            self.request_history.append({
                'method': method,
                'path': path,
                'status': result.get('status', 200),
                'latency_ms': latency_ms,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            return result
            
        except Exception as e:
            logger.error(f"API error: {str(e)}", exc_info=True)
            latency_ms = (time.time() - start_time) * 1000
            self.latency_histogram[f"{path}_error"].append(latency_ms)
            
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
    # New Endpoint: System Load
    # ========================================================================
    
    async def get_system_load(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/system/load - System load status"""
        return {
            'system_load': self.adaptive_limiter.load_history[-1] if self.adaptive_limiter.load_history else 0.5,
            'rate_multiplier': self.adaptive_limiter.current_multiplier,
            'active_scopes': list(self.adaptive_limiter.rate_records.keys()),
            'rate_limits': self.adaptive_limiter.base_limits
        }
    
    # ========================================================================
    # New Endpoint: Histogram Metrics
    # ========================================================================
    
    async def get_metric_histograms(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/metrics/histograms - Histogram metrics for latency distribution"""
        histograms = {}
        
        for path, latencies in self.latency_histogram.items():
            if len(latencies) > 0:
                histograms[path] = {
                    'p50': np.percentile(latencies, 50),
                    'p90': np.percentile(latencies, 90),
                    'p95': np.percentile(latencies, 95),
                    'p99': np.percentile(latencies, 99),
                    'min': min(latencies),
                    'max': max(latencies),
                    'mean': sum(latencies) / len(latencies),
                    'samples': len(latencies)
                }
        
        return {
            'histograms': histograms,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    # ========================================================================
    # Enhanced Webhook Endpoints
    # ========================================================================
    
    @require_auth
    async def subscribe_webhook(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/webhooks/subscribe"""
        body = request_data.get('body', {})
        event_type = body.get('event_type', '')
        callback_url = body.get('callback_url', '')
        max_retries = body.get('max_retries', 5)
        
        if not event_type or not callback_url:
            return {'error': 'event_type and callback_url required'}
        
        subscription_id = await self.webhook_manager.subscribe(event_type, callback_url)
        
        # Update max retries if provided
        if subscription_id in self.webhook_manager.subscriptions:
            self.webhook_manager.subscriptions[subscription_id].max_retries = max_retries
        
        return {
            'success': True,
            'subscription_id': subscription_id,
            'event_type': event_type,
            'max_retries': max_retries
        }
    
    @require_auth
    async def unsubscribe_webhook(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/webhooks/unsubscribe"""
        body = request_data.get('body', {})
        subscription_id = body.get('subscription_id', '')
        
        success = await self.webhook_manager.unsubscribe(subscription_id)
        
        return {'success': success, 'subscription_id': subscription_id}
    
    async def get_webhook_stats(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """GET /api/v1/webhooks/stats"""
        return self.webhook_manager.get_webhook_stats()
    
    # ========================================================================
    # Existing Endpoints (Preserved)
    # ========================================================================
    
    # Health and Operational
    async def get_live(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        return {'status': 'alive', 'timestamp': datetime.utcnow().isoformat()}
    
    async def get_ready(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if self.health_manager:
            ready = self.health_manager.is_ready()
            return {'status': 'ready' if ready else 'not_ready', 'timestamp': datetime.utcnow().isoformat()}
        
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
        if self.health_manager:
            return self.health_manager.check_all(self.bio_core) if self.bio_core else {}
        return {'status': 'health_manager_not_available'}
    
    @require_auth
    @require_role('admin')
    async def get_config(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if self.bio_core and hasattr(self.bio_core, 'config'):
            return {'config': self.bio_core.config.to_dict()}
        return {'error': 'Configuration not available'}
    
    @require_auth
    @require_role('admin')
    async def update_config(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        updates = request_data.get('body', {})
        if self.bio_core and hasattr(self.bio_core, 'update_configuration'):
            self.bio_core.update_configuration(updates)
            return {'status': 'updated', 'changes': list(updates.keys())}
        return {'error': 'Configuration update not available'}
    
    # Token Economy
    async def get_token_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.token_manager:
            return {'error': 'Token manager not available'}
        return self.token_manager.get_system_summary()
    
    @require_auth
    async def get_token_accounts(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
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
        if not self.supply_manager:
            return {'error': 'Supply manager not available'}
        return self.supply_manager.get_economic_indicators()
    
    # Gradient Fields
    async def get_gradient_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        return {
            'field_strengths': self.gradient_manager.get_field_strengths(),
            'detailed_stats': self.gradient_manager.get_field_stats(),
            'dominant_field': self.gradient_manager.get_dominant_field()
        }
    
    async def get_gradient_forecasts(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        if hasattr(self.gradient_manager, 'get_forecast_summary'):
            return self.gradient_manager.get_forecast_summary()
        return {'error': 'Forecasting not available'}
    
    async def get_causal_analysis(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        field_id = request_data.get('query_params', {}).get('field', 'carbon')
        if hasattr(self.gradient_manager, 'find_root_cause'):
            return self.gradient_manager.find_root_cause(field_id)
        return {'error': 'Causal analysis not available'}
    
    @require_auth
    async def pump_gradient(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.gradient_manager:
            return {'error': 'Gradient manager not available'}
        body = request_data.get('body', {})
        field_id = body.get('field_id', 'carbon')
        amount = body.get('amount', 0.1)
        self.gradient_manager.pump_field(field_id, amount, source='api')
        return {'success': True, 'field_id': field_id, 'amount': amount}
    
    # ATP Synthase
    async def get_atp_status(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.scheduler:
            return {'error': 'ATP synthase not available'}
        return self.scheduler.get_scheduler_stats()
    
    async def get_atp_efficiency(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.scheduler:
            return {'error': 'ATP synthase not available'}
        if hasattr(self.scheduler, 'get_efficiency_report'):
            return self.scheduler.get_efficiency_report()
        return {'error': 'Efficiency report not available'}
    
    # Compartments
    async def get_compartment_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.compartment_manager:
            return {'error': 'Compartment manager not available'}
        return self.compartment_manager.get_ecosystem_stats()
    
    async def get_compartment_regions(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
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
    
    # Biomass Storage
    async def get_biomass_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        return self.biomass_storage.get_storage_stats()
    
    @require_auth
    async def store_task(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
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
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        body = request_data.get('body', {})
        token_id = body.get('token_id', '')
        task_data, cost = self.biomass_storage.retrieve_task(token_id)
        return {'success': task_data is not None, 'task_data': task_data, 'retrieval_cost': cost}
    
    async def get_biomass_analytics(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.biomass_storage:
            return {'error': 'Biomass storage not available'}
        if hasattr(self.biomass_storage, 'generate_analytics'):
            analytics = self.biomass_storage.generate_analytics()
            recommendations = self.biomass_storage.get_optimization_recommendations()
            return {'analytics': asdict(analytics) if hasattr(analytics, '__dataclass_fields__') else analytics,
                    'recommendations': recommendations}
        return {'error': 'Analytics not available'}
    
    async def get_biomass_forecast(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
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
    
    # Harvester
    async def get_harvester_summary(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.harvester:
            return {'error': 'Harvester not available'}
        return self.harvester.get_harvesting_stats()
    
    @require_auth
    async def harvest_cycle(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
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
        if not self.harvester:
            return {'error': 'Harvester not available'}
        if hasattr(self.harvester, 'get_circadian_report'):
            return self.harvester.get_circadian_report()
        return {'error': 'Circadian report not available'}
    
    # Degradation Manager
    async def get_degradation_status(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.degradation_manager:
            return {'error': 'Degradation manager not available'}
        return self.degradation_manager.get_tier_status()
    
    async def get_degradation_history(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.degradation_manager:
            return {'error': 'Degradation manager not available'}
        return {'history': list(self.degradation_manager.tier_history)[-50:]}
    
    async def get_chaos_report(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.degradation_manager:
            return {'error': 'Degradation manager not available'}
        if hasattr(self.degradation_manager, 'get_chaos_report'):
            return self.degradation_manager.get_chaos_report()
        return {'error': 'Chaos engineering not available'}
    
    # Knowledge Transfer
    async def get_knowledge_packages(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.knowledge_transfer:
            return {'error': 'Knowledge transfer not available'}
        return self.knowledge_transfer.get_knowledge_summary()
    
    @require_auth
    async def transfer_knowledge(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.knowledge_transfer:
            return {'error': 'Knowledge transfer not available'}
        return {'status': 'not_implemented', 'message': 'Requires target expert instance'}
    
    # System
    async def get_system_overview(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.bio_core:
            return {'error': 'Bio-core not available'}
        return self.bio_core.get_system_status()
    
    async def get_system_recommendations(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        recommendations = []
        
        if self.supply_manager:
            indicators = self.supply_manager.get_economic_indicators()
            if indicators.get('utilization', 0.5) < 0.4:
                recommendations.append("Economy under-utilized. Increase task throughput.")
            if indicators.get('inflation_pressure', 0) > 0.3:
                recommendations.append("High inflation pressure. Consider token burning.")
        
        if self.biomass_storage and hasattr(self.biomass_storage, 'get_optimization_recommendations'):
            recommendations.extend(self.biomass_storage.get_optimization_recommendations())
        
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
        if not self.bio_core:
            return {'error': 'Bio-core not available'}
        body = request_data.get('body', {})
        if hasattr(self.bio_core, 'run_what_if_analysis'):
            return self.bio_core.run_what_if_analysis(body)
        return {'error': 'What-if analysis not available'}
    
    # Metrics
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
        
        # NEW: Rate limiter metrics
        limiter_stats = self.adaptive_limiter.get_limiter_stats()
        metrics.append(f'green_agent_rate_multiplier {limiter_stats.get("current_multiplier", 1.0)} {timestamp_ms}')
        metrics.append(f'green_agent_system_load {limiter_stats.get("avg_load", 0.5)} {timestamp_ms}')
        
        return {
            'metrics': '\n'.join(metrics),
            'content_type': 'text/plain',
            'timestamp': datetime.utcnow().isoformat()
        }
    
    # Admin
    @require_auth
    @require_role('admin')
    async def get_api_keys(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        return self.api_key_manager.get_key_stats()
    
    @require_auth
    @require_role('admin')
    async def create_api_key(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
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
        avg_latency = {}
        
        for path, latencies in self.latency_histogram.items():
            if len(latencies) > 0:
                avg_latency[path] = sum(latencies) / len(latencies)
        
        for req in recent:
            endpoint_counts[req['path']] += 1
            status_counts[req['status']] += 1
        
        return {
            'total_requests': len(self.request_history),
            'recent_requests': len(recent),
            'top_endpoints': sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            'status_distribution': dict(status_counts),
            'average_latency_ms': avg_latency,
            'webhook_stats': self.webhook_manager.get_webhook_stats() if self.webhook_manager else {},
            'oauth_enabled': True,
            'registered_routes': len(self.routes),
            'api_version': API_VERSION
        }
