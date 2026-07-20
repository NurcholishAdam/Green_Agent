# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/api.py
# Enhanced version v8.0.0 – All improvements integrated in a single file

"""
Enhanced Bio-Inspired API v8.0.0
Complete RESTful API with:
- Authentication (API Key + OAuth2/JWT) and role-based access
- Adaptive rate limiting (sliding window) based on system load
- Standardized error responses with APIError
- Distributed caching (Redis optional) with TTL
- Webhook system with HMAC signature, persistence, and retry with backoff
- OAuth2 token persistence (Redis/file) with refresh token rotation
- Correlation IDs for request tracing
- Graceful shutdown for background tasks
- Pagination with filtering, sorting, and cursor support
- WebSocket endpoint with authentication and subscription channels
- Audit logging for admin actions
- Structured logging (structlog fallback)
- OpenAPI generation from route decorators
- Configurable via Pydantic with environment overrides
- Improved health checks (liveness + readiness probes)
- Request body validation with Pydantic models
- Full integration with bio-core modules
- Async context manager support
- Prometheus metrics with detailed labels
"""

import asyncio
import logging
import json
import time
import uuid
import hashlib
import hmac
import secrets
import os
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Type
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from functools import wraps
from collections import defaultdict, deque
import jwt
import pickle

# Try optional dependencies
try:
    from pydantic import BaseModel, Field, validator, root_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Local imports (with fallback)
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

try:
    from .biomass_storage import StorageTier, GuaranteeLevel
    BIOMASS_AVAILABLE = True
except ImportError:
    BIOMASS_AVAILABLE = False

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class APIConfig(BaseModel):
        """Configuration for the Bio-Inspired API."""
        # Version
        api_version: str = "v1"
        prefix: str = "/api"

        # Security
        oauth2_secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
        oauth2_issuer: str = "green-agent"
        oauth2_audience: str = "green-agent-api"
        access_token_expiry_minutes: int = 60
        refresh_token_expiry_days: int = 7
        refresh_token_store_backend: str = "file"  # file, redis
        refresh_token_redis_url: Optional[str] = None
        refresh_token_file_path: str = "./refresh_tokens.json"

        # Rate limiting
        default_rate_limit: int = 100
        default_burst_limit: int = 20
        adaptive_enabled: bool = True
        sliding_window_seconds: int = 60

        # Caching
        cache_enabled: bool = True
        cache_backend: str = "memory"  # memory, redis
        cache_redis_url: Optional[str] = None
        cache_ttl_seconds: int = 60
        cache_max_items: int = 1000

        # Webhook
        webhook_max_retries: int = 5
        webhook_retry_backoff_base: int = 2
        webhook_secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(16))
        webhook_persistence_path: str = "./webhook_subscriptions.json"

        # Pagination
        default_page_size: int = 20
        max_page_size: int = 100

        # WebSocket
        websocket_enabled: bool = True
        websocket_port: int = 8765
        websocket_auth_required: bool = True

        # Health
        health_check_timeout_seconds: int = 5

        # Audit
        audit_log_path: str = "./audit.log"

        # Logging
        structured_logging: bool = True

        class Config:
            env_prefix = "GREEN_API_"

    class TokenGenerateRequest(BaseModel):
        account_id: str
        source: str = "GRADIENT_CONVERSION"
        energy_saved_kwh: float = 0.0
        efficiency: float = 0.85

    class TokenReserveRequest(BaseModel):
        account_id: str
        amount: float
        consumer: str = "EXPERT_EXECUTION"

    class CompartmentCreateRequest(BaseModel):
        name: str
        region: str
        capacity: float = 100.0

    class BiomassStoreRequest(BaseModel):
        task_id: str
        data: Dict[str, Any]
        tier: str = "standard"
        guarantee: str = "silver"

    class BiomassRetrieveRequest(BaseModel):
        task_id: str
        verify_hash: Optional[str] = None

    class WebhookSubscribeRequest(BaseModel):
        event_type: str
        callback_url: str
        max_retries: Optional[int] = None

    class WebhookUnsubscribeRequest(BaseModel):
        subscription_id: str

    class HarvestCycleRequest(BaseModel):
        environmental_data: Dict[str, float]
        mode: Optional[str] = None

    class WhatIfRequest(BaseModel):
        scenario: Dict[str, float]
        horizon_hours: int = 24

    class APIKeyCreateRequest(BaseModel):
        name: str
        rate_limit: Optional[int] = None
        role: str = "user"

    class APIKeyRevokeRequest(BaseModel):
        api_key: str

else:
    # Fallback dataclass for config
    @dataclass
    class APIConfig:
        api_version: str = "v1"
        prefix: str = "/api"
        oauth2_secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(32))
        oauth2_issuer: str = "green-agent"
        oauth2_audience: str = "green-agent-api"
        access_token_expiry_minutes: int = 60
        refresh_token_expiry_days: int = 7
        refresh_token_store_backend: str = "file"
        refresh_token_redis_url: Optional[str] = None
        refresh_token_file_path: str = "./refresh_tokens.json"
        default_rate_limit: int = 100
        default_burst_limit: int = 20
        adaptive_enabled: bool = True
        sliding_window_seconds: int = 60
        cache_enabled: bool = True
        cache_backend: str = "memory"
        cache_redis_url: Optional[str] = None
        cache_ttl_seconds: int = 60
        cache_max_items: int = 1000
        webhook_max_retries: int = 5
        webhook_retry_backoff_base: int = 2
        webhook_secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(16))
        webhook_persistence_path: str = "./webhook_subscriptions.json"
        default_page_size: int = 20
        max_page_size: int = 100
        websocket_enabled: bool = True
        websocket_port: int = 8765
        websocket_auth_required: bool = True
        health_check_timeout_seconds: int = 5
        audit_log_path: str = "./audit.log"
        structured_logging: bool = True

    # Request models as dataclasses (no Pydantic)
    @dataclass
    class TokenGenerateRequest:
        account_id: str
        source: str = "GRADIENT_CONVERSION"
        energy_saved_kwh: float = 0.0
        efficiency: float = 0.85

    @dataclass
    class TokenReserveRequest:
        account_id: str
        amount: float
        consumer: str = "EXPERT_EXECUTION"

    @dataclass
    class CompartmentCreateRequest:
        name: str
        region: str
        capacity: float = 100.0

    @dataclass
    class BiomassStoreRequest:
        task_id: str
        data: Dict[str, Any]
        tier: str = "standard"
        guarantee: str = "silver"

    @dataclass
    class BiomassRetrieveRequest:
        task_id: str
        verify_hash: Optional[str] = None

    @dataclass
    class WebhookSubscribeRequest:
        event_type: str
        callback_url: str
        max_retries: Optional[int] = None

    @dataclass
    class WebhookUnsubscribeRequest:
        subscription_id: str

    @dataclass
    class HarvestCycleRequest:
        environmental_data: Dict[str, float]
        mode: Optional[str] = None

    @dataclass
    class WhatIfRequest:
        scenario: Dict[str, float]
        horizon_hours: int = 24

    @dataclass
    class APIKeyCreateRequest:
        name: str
        rate_limit: Optional[int] = None
        role: str = "user"

    @dataclass
    class APIKeyRevokeRequest:
        api_key: str

# ============================================================================
# Custom Exception and Error Response
# ============================================================================

class APIError(Exception):
    """Custom API error with status code and details."""
    def __init__(self, status_code: int, code: str, message: str, details: Optional[Dict] = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(message)

def error_response(status_code: int, code: str, message: str, details: Optional[Dict] = None) -> Dict:
    """Return a standardized error response."""
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or {}
        },
        "status": status_code
    }

# ============================================================================
# Cache (abstract + Redis implementation)
# ============================================================================

class CacheBackend(Protocol):
    async def get(self, key: str) -> Optional[Dict]: ...
    async def set(self, key: str, value: Dict, ttl: Optional[int] = None): ...
    async def delete(self, key: str) -> bool: ...

class MemoryCacheBackend:
    def __init__(self, max_items: int = 1000):
        self._cache = {}
        self._max_items = max_items
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Dict]:
        async with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if datetime.now(timezone.utc) < expiry:
                    return value
                else:
                    del self._cache[key]
        return None

    async def set(self, key: str, value: Dict, ttl: Optional[int] = None):
        expiry = datetime.now(timezone.utc) + timedelta(seconds=ttl or 60)
        async with self._lock:
            self._cache[key] = (value, expiry)
            if len(self._cache) > self._max_items:
                oldest = min(self._cache.items(), key=lambda x: x[1][1])
                del self._cache[oldest[0]]

    async def delete(self, key: str) -> bool:
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
        return False

class RedisCacheBackend:
    def __init__(self, redis_url: str, default_ttl: int = 60):
        self.redis = redis.from_url(redis_url, decode_responses=False)
        self.default_ttl = default_ttl

    async def get(self, key: str) -> Optional[Dict]:
        data = await self.redis.get(key)
        if data:
            return pickle.loads(data)
        return None

    async def set(self, key: str, value: Dict, ttl: Optional[int] = None):
        ttl = ttl or self.default_ttl
        await self.redis.setex(key, ttl, pickle.dumps(value))

    async def delete(self, key: str) -> bool:
        return await self.redis.delete(key) > 0

# ============================================================================
# Token Store (abstract + Redis/File)
# ============================================================================

class TokenStoreBackend(Protocol):
    async def get(self, refresh_token: str) -> Optional[Dict]: ...
    async def set(self, refresh_token: str, data: Dict): ...
    async def delete(self, refresh_token: str) -> bool: ...
    async def clean_expired(self): ...

class FileTokenStore:
    def __init__(self, path: str):
        self.path = path
        self._tokens = {}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r') as f:
                    self._tokens = json.load(f)
            except Exception as e:
                logger.error("Failed to load token store", error=str(e))

    def _save(self):
        try:
            with open(self.path, 'w') as f:
                json.dump(self._tokens, f, default=str)
        except Exception as e:
            logger.error("Failed to save token store", error=str(e))

    async def get(self, refresh_token: str) -> Optional[Dict]:
        async with self._lock:
            return self._tokens.get(refresh_token)

    async def set(self, refresh_token: str, data: Dict):
        async with self._lock:
            self._tokens[refresh_token] = data
            self._save()

    async def delete(self, refresh_token: str) -> bool:
        async with self._lock:
            if refresh_token in self._tokens:
                del self._tokens[refresh_token]
                self._save()
                return True
        return False

    async def clean_expired(self):
        async with self._lock:
            now = datetime.now(timezone.utc).isoformat()
            to_delete = [k for k, v in self._tokens.items() if v.get('expires_at', '') < now]
            for k in to_delete:
                del self._tokens[k]
            self._save()

class RedisTokenStore:
    def __init__(self, redis_url: str):
        self.redis = redis.from_url(redis_url, decode_responses=False)

    async def get(self, refresh_token: str) -> Optional[Dict]:
        data = await self.redis.get(f"refresh_token:{refresh_token}")
        if data:
            return pickle.loads(data)
        return None

    async def set(self, refresh_token: str, data: Dict):
        expires_at = datetime.fromisoformat(data['expires_at'])
        ttl = int((expires_at - datetime.now(timezone.utc)).total_seconds())
        if ttl > 0:
            await self.redis.setex(f"refresh_token:{refresh_token}", ttl, pickle.dumps(data))

    async def delete(self, refresh_token: str) -> bool:
        return await self.redis.delete(f"refresh_token:{refresh_token}") > 0

    async def clean_expired(self):
        # Redis automatically expires keys with TTL
        pass

# ============================================================================
# OAuth2 Manager (enhanced)
# ============================================================================

class OAuth2Manager:
    def __init__(self, config: APIConfig, token_store: TokenStoreBackend):
        self.config = config
        self.token_store = token_store
        self.revoked_tokens = set()
        self._lock = asyncio.Lock()

    def create_access_token(self, client_id: str, scopes: List[str] = None) -> str:
        now = datetime.now(timezone.utc)
        payload = {
            'sub': client_id,
            'iss': self.config.oauth2_issuer,
            'aud': self.config.oauth2_audience,
            'iat': now,
            'exp': now + timedelta(minutes=self.config.access_token_expiry_minutes),
            'scopes': scopes or ['read'],
            'jti': secrets.token_hex(16)
        }
        return jwt.encode(payload, self.config.oauth2_secret_key, algorithm='HS256')

    async def create_refresh_token(self, client_id: str) -> str:
        token = secrets.token_urlsafe(32)
        data = {
            'client_id': client_id,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'expires_at': (datetime.now(timezone.utc) + timedelta(days=self.config.refresh_token_expiry_days)).isoformat()
        }
        await self.token_store.set(token, data)
        return token

    async def validate_token(self, token: str) -> Optional[Dict]:
        if token in self.revoked_tokens:
            return None
        try:
            payload = jwt.decode(
                token,
                self.config.oauth2_secret_key,
                algorithms=['HS256'],
                audience=self.config.oauth2_audience,
                issuer=self.config.oauth2_issuer
            )
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None

    async def revoke_token(self, token: str) -> bool:
        self.revoked_tokens.add(token)
        # Also revoke associated refresh token? We'll leave that to the caller.
        return True

    async def refresh_access_token(self, refresh_token: str) -> Optional[Dict]:
        token_data = await self.token_store.get(refresh_token)
        if not token_data:
            return None
        if datetime.now(timezone.utc) > datetime.fromisoformat(token_data['expires_at']):
            return None
        # Revoke old refresh token
        await self.token_store.delete(refresh_token)
        client_id = token_data['client_id']
        new_access = self.create_access_token(client_id)
        new_refresh = await self.create_refresh_token(client_id)
        return {
            'access_token': new_access,
            'refresh_token': new_refresh,
            'expires_in': self.config.access_token_expiry_minutes * 60
        }

    def get_config(self) -> Dict:
        return {
            'issuer': self.config.oauth2_issuer,
            'audience': self.config.oauth2_audience,
            'token_endpoint': f"{self.config.prefix}/{self.config.api_version}/oauth/token",
            'revocation_endpoint': f"{self.config.prefix}/{self.config.api_version}/oauth/revoke",
            'grant_types': ['client_credentials', 'refresh_token']
        }

# ============================================================================
# Rate Limiter (Sliding Window)
# ============================================================================

class SlidingWindowRateLimiter:
    def __init__(self, config: APIConfig):
        self.config = config
        self.base_limits = {
            'read': config.default_rate_limit,
            'write': config.default_rate_limit // 2,
            'admin': config.default_rate_limit // 5
        }
        self.current_multiplier = 1.0
        self.load_history = deque(maxlen=100)
        self.records: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()

    def update_system_load(self, load: float):
        self.load_history.append(load)
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
        base = self.base_limits.get(scope, 50)
        return int(base * self.current_multiplier)

    def check_rate_limit(self, key: str, scope: str = 'read') -> Tuple[bool, Dict]:
        limit = self.get_rate_limit(scope)
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(seconds=self.config.sliding_window_seconds)
        records = self.records[key]
        # Remove old entries
        while records and records[0] < window_start:
            records.popleft()
        if len(records) >= limit:
            retry_after = (records[0] + timedelta(seconds=self.config.sliding_window_seconds) - now).total_seconds()
            return False, {
                'error': 'Rate limit exceeded',
                'retry_after_seconds': max(0, int(retry_after)),
                'limit': limit,
                'current_usage': len(records)
            }
        records.append(now)
        return True, {
            'limit': limit,
            'remaining': limit - len(records),
            'reset_seconds': int((window_start + timedelta(seconds=self.config.sliding_window_seconds) - now).total_seconds())
        }

    def get_stats(self) -> Dict:
        return {
            'current_multiplier': self.current_multiplier,
            'base_limits': self.base_limits,
            'avg_load': sum(self.load_history) / len(self.load_history) if self.load_history else 0.5,
            'active_keys': list(self.records.keys())
        }

# ============================================================================
# API Key Manager
# ============================================================================

class APIKeyManager:
    def __init__(self, config: APIConfig):
        self.config = config
        self.api_keys: Dict[str, Dict[str, Any]] = {}
        self.rate_limiter = SlidingWindowRateLimiter(config)
        # No default admin key; generate on first run if needed.

    def create_key(self, name: str, rate_limit: Optional[int] = None, role: str = "user") -> str:
        key = secrets.token_urlsafe(24)
        self.api_keys[key] = {
            'name': name,
            'key': key,
            'rate_limit': rate_limit or self.config.default_rate_limit,
            'role': role,
            'permissions': ["read"] if role == "user" else ["read", "write", "admin"],
            'created_at': datetime.now(timezone.utc).isoformat(),
            'last_used': None,
            'total_requests': 0,
            'active': True
        }
        return key

    def validate_key(self, api_key: str) -> Optional[Dict]:
        if api_key in self.api_keys:
            key_data = self.api_keys[api_key]
            if key_data['active']:
                key_data['last_used'] = datetime.now(timezone.utc).isoformat()
                key_data['total_requests'] += 1
                return key_data
        return None

    def check_rate_limit(self, api_key: str, scope: str = 'read') -> Tuple[bool, Dict]:
        if api_key not in self.api_keys:
            return False, {'error': 'Invalid API key'}
        return self.rate_limiter.check_rate_limit(api_key, scope)

    def revoke_key(self, api_key: str) -> bool:
        if api_key in self.api_keys:
            self.api_keys[api_key]['active'] = False
            return True
        return False

    def get_key_stats(self) -> Dict:
        return {
            'total_keys': len(self.api_keys),
            'active_keys': sum(1 for k in self.api_keys.values() if k['active']),
            'keys': [
                {
                    'name': k['name'],
                    'role': k['role'],
                    'rate_limit': k['rate_limit'],
                    'total_requests': k['total_requests'],
                    'last_used': k['last_used'],
                    'active': k['active']
                }
                for k in self.api_keys.values()
            ]
        }

# ============================================================================
# Webhook Manager (with persistence)
# ============================================================================

@dataclass
class WebhookSubscription:
    subscription_id: str
    event_type: str
    callback_url: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    max_retries: int = 5
    last_delivery: Optional[datetime] = None
    last_error: Optional[str] = None
    status: str = "active"
    secret: str = ""

class WebhookManager:
    def __init__(self, config: APIConfig, event_broker=None):
        self.config = config
        self.event_broker = event_broker
        self.subscriptions: Dict[str, WebhookSubscription] = {}
        self.delivery_queue = deque(maxlen=10000)
        self._lock = asyncio.Lock()
        self._processing = False
        self._shutdown = False
        self._load_subscriptions()

    def _load_subscriptions(self):
        if os.path.exists(self.config.webhook_persistence_path):
            try:
                with open(self.config.webhook_persistence_path, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        sub = WebhookSubscription(
                            subscription_id=item['subscription_id'],
                            event_type=item['event_type'],
                            callback_url=item['callback_url'],
                            created_at=datetime.fromisoformat(item['created_at']),
                            max_retries=item['max_retries'],
                            status=item['status'],
                            secret=item.get('secret', '')
                        )
                        self.subscriptions[sub.subscription_id] = sub
                logger.info(f"Loaded {len(self.subscriptions)} webhook subscriptions")
            except Exception as e:
                logger.error("Failed to load webhook subscriptions", error=str(e))

    def _save_subscriptions(self):
        data = [
            {
                'subscription_id': s.subscription_id,
                'event_type': s.event_type,
                'callback_url': s.callback_url,
                'created_at': s.created_at.isoformat(),
                'max_retries': s.max_retries,
                'status': s.status,
                'secret': s.secret
            }
            for s in self.subscriptions.values()
        ]
        try:
            with open(self.config.webhook_persistence_path, 'w') as f:
                json.dump(data, f, default=str)
        except Exception as e:
            logger.error("Failed to save webhook subscriptions", error=str(e))

    async def subscribe(self, event_type: str, callback_url: str) -> str:
        subscription_id = hashlib.sha256(
            f"{event_type}{callback_url}{datetime.now(timezone.utc).timestamp()}".encode()
        ).hexdigest()[:16]
        secret = secrets.token_urlsafe(16)
        subscription = WebhookSubscription(
            subscription_id=subscription_id,
            event_type=event_type,
            callback_url=callback_url,
            max_retries=self.config.webhook_max_retries,
            secret=secret
        )
        async with self._lock:
            self.subscriptions[subscription_id] = subscription
            self._save_subscriptions()
            if self.event_broker:
                async def webhook_callback(event):
                    if not self._shutdown:
                        await self._enqueue_delivery(subscription_id, event)
                self.event_broker.subscribe(event_type, webhook_callback)
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        async with self._lock:
            if subscription_id not in self.subscriptions:
                return False
            self.subscriptions[subscription_id].status = "cancelled"
            self._save_subscriptions()
        return True

    async def _enqueue_delivery(self, subscription_id: str, event):
        async with self._lock:
            subscription = self.subscriptions.get(subscription_id)
            if not subscription or subscription.status != "active":
                return
        self.delivery_queue.append({
            'subscription_id': subscription_id,
            'event': event,
            'attempts': 0,
            'next_attempt': datetime.now(timezone.utc)
        })
        if not self._processing and not self._shutdown:
            await self._process_deliveries()

    async def _process_deliveries(self):
        if self._processing:
            return
        self._processing = True
        try:
            while self.delivery_queue and not self._shutdown:
                delivery = self.delivery_queue[0]
                if datetime.now(timezone.utc) < delivery['next_attempt']:
                    await asyncio.sleep(1)
                    continue
                async with self._lock:
                    subscription = self.subscriptions.get(delivery['subscription_id'])
                    if not subscription or subscription.status != "active":
                        self.delivery_queue.popleft()
                        continue
                # Attempt delivery with HMAC
                try:
                    success = await self._deliver_webhook(subscription, delivery['event'])
                    if success:
                        subscription.last_delivery = datetime.now(timezone.utc)
                        delivery['attempts'] = 0
                        self.delivery_queue.popleft()
                        logger.debug("Webhook delivered", subscription_id=subscription.subscription_id)
                    else:
                        delivery['attempts'] += 1
                        backoff = min(60, self.config.webhook_retry_backoff_base ** delivery['attempts'])
                        delivery['next_attempt'] = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                        if delivery['attempts'] >= subscription.max_retries:
                            subscription.status = "failed"
                            subscription.last_error = "Max retries exceeded"
                            self.delivery_queue.popleft()
                            logger.warning("Webhook failed", subscription_id=subscription.subscription_id)
                except Exception as e:
                    logger.error("Webhook delivery error", error=str(e))
                    delivery['attempts'] += 1
                    backoff = min(60, self.config.webhook_retry_backoff_base ** delivery['attempts'])
                    delivery['next_attempt'] = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                await asyncio.sleep(0.1)
        finally:
            self._processing = False

    async def _deliver_webhook(self, subscription: WebhookSubscription, event) -> bool:
        if not AIOHTTP_AVAILABLE:
            # Simulate for testing
            return True
        payload = {
            'event_type': event.event_type,
            'timestamp': event.timestamp.isoformat(),
            'data': event.data,
            'correlation_id': event.correlation_id
        }
        signature = WebhookSecurity.generate_signature(subscription.secret, payload)
        headers = {
            'Content-Type': 'application/json',
            'X-Webhook-Signature': signature
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    subscription.callback_url,
                    json=payload,
                    headers=headers,
                    timeout=10
                ) as response:
                    return response.status in [200, 201, 202, 204]
        except Exception:
            return False

    def get_webhook_stats(self) -> Dict:
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

    async def shutdown(self):
        self._shutdown = True
        while self._processing:
            await asyncio.sleep(0.1)

# ============================================================================
# WebSocket Server (with authentication)
# ============================================================================

class WebSocketServer:
    def __init__(self, api: 'BioInspiredAPI', port: int = 8765):
        self.api = api
        self.port = port
        self.connections = set()
        self.server = None
        self.subscribers: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets not available")
            return
        self.server = await websockets.serve(self._handler, '0.0.0.0', self.port)
        logger.info(f"WebSocket server started on port {self.port}")

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _handler(self, websocket, path):
        # Authentication required
        if self.api.config.websocket_auth_required:
            try:
                auth_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                if auth_msg.startswith("Bearer "):
                    token = auth_msg[7:]
                    payload = await self.api.oauth2_manager.validate_token(token)
                    if not payload:
                        await websocket.close(1008, "Authentication failed")
                        return
                    # Store client_id
                    client_id = payload['sub']
                else:
                    # Try API key
                    api_key = auth_msg
                    key_data = self.api.api_key_manager.validate_key(api_key)
                    if not key_data:
                        await websocket.close(1008, "Authentication failed")
                        return
                    client_id = key_data['name']
            except asyncio.TimeoutError:
                await websocket.close(1008, "Authentication timeout")
                return
        else:
            client_id = "anonymous"

        # Subscribe to channels based on path or query
        channels = ['global']
        if path.startswith('/events/'):
            channel = path.split('/')[-1]
            channels.append(channel)

        async with self._lock:
            self.connections.add(websocket)
            for channel in channels:
                self.subscribers[channel].add(websocket)

        try:
            async for message in websocket:
                # Handle subscription changes
                try:
                    data = json.loads(message)
                    if data.get('type') == 'subscribe':
                        new_channel = data.get('channel')
                        if new_channel:
                            async with self._lock:
                                self.subscribers[new_channel].add(websocket)
                except:
                    pass
        finally:
            async with self._lock:
                self.connections.remove(websocket)
                for channel in list(self.subscribers.keys()):
                    self.subscribers[channel].discard(websocket)
                    if not self.subscribers[channel]:
                        del self.subscribers[channel]

    async def broadcast(self, event: Dict, channels: List[str] = None):
        if not self.connections:
            return
        message = json.dumps(event, default=str)
        if channels is None:
            channels = ['global']
        async with self._lock:
            recipients = set()
            for channel in channels:
                recipients.update(self.subscribers.get(channel, []))
        await asyncio.gather(*(ws.send(message) for ws in recipients), return_exceptions=True)

# ============================================================================
# Health Checker
# ============================================================================

class HealthChecker:
    def __init__(self, api: 'BioInspiredAPI'):
        self.api = api

    async def check_all(self) -> Dict[str, Any]:
        results = {}
        modules = {
            'token_manager': self.api.token_manager,
            'gradient_manager': self.api.gradient_manager,
            'compartment_manager': self.api.compartment_manager,
            'biomass_storage': self.api.biomass_storage,
            'harvester': self.api.harvester,
            'scheduler': self.api.scheduler,
            'degradation_manager': self.api.degradation_manager,
            'knowledge_transfer': self.api.knowledge_transfer,
            'supply_manager': self.api.supply_manager,
            'token_allocator': self.api.token_allocator,
        }
        for name, module in modules.items():
            if module is None:
                results[name] = {'status': 'unavailable', 'error': 'Module not initialized'}
            else:
                try:
                    # Attempt a simple call to verify it's responsive
                    if hasattr(module, 'get_system_summary'):
                        await asyncio.wait_for(module.get_system_summary(), timeout=self.api.config.health_check_timeout_seconds)
                        results[name] = {'status': 'healthy'}
                    elif hasattr(module, 'get_field_stats'):
                        await asyncio.wait_for(module.get_field_stats(), timeout=self.api.config.health_check_timeout_seconds)
                        results[name] = {'status': 'healthy'}
                    else:
                        results[name] = {'status': 'healthy'}  # assume ok
                except asyncio.TimeoutError:
                    results[name] = {'status': 'unhealthy', 'error': 'Timeout'}
                except Exception as e:
                    results[name] = {'status': 'unhealthy', 'error': str(e)}
        return results

# ============================================================================
# Audit Logger
# ============================================================================

class AuditLogger:
    def __init__(self, config: APIConfig):
        self.config = config
        self.path = config.audit_log_path
        self._lock = asyncio.Lock()

    async def log(self, action: str, user_id: str, details: Dict):
        async with self._lock:
            entry = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'action': action,
                'user_id': user_id,
                'details': details
            }
            with open(self.path, 'a') as f:
                f.write(json.dumps(entry, default=str) + '\n')

# ============================================================================
# Correlation ID Middleware
# ============================================================================

class CorrelationIDMiddleware:
    @staticmethod
    def generate() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def add_to_response(response: Dict, request_id: str) -> Dict:
        if 'meta' not in response:
            response['meta'] = {}
        response['meta']['request_id'] = request_id
        return response

# ============================================================================
# Webhook Security (HMAC signatures)
# ============================================================================

class WebhookSecurity:
    @staticmethod
    def generate_signature(secret: str, payload: Dict) -> str:
        data = json.dumps(payload, sort_keys=True, default=str)
        return hmac.new(secret.encode(), data.encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def verify_signature(secret: str, payload: Dict, signature: str) -> bool:
        expected = WebhookSecurity.generate_signature(secret, payload)
        return hmac.compare_digest(expected, signature)

# ============================================================================
# Route Handler Base Class
# ============================================================================

class BaseHandler:
    """Base class for API handlers."""
    def __init__(self, api: 'BioInspiredAPI'):
        self.api = api
        self.config = api.config

# ============================================================================
# Specific Handlers (to reduce monolithic class)
# ============================================================================

class HealthHandler(BaseHandler):
    async def get_live(self, request_data: Dict) -> Dict:
        return {"status": "alive", "timestamp": datetime.now(timezone.utc).isoformat()}

    async def get_ready(self, request_data: Dict) -> Dict:
        results = await self.api.health_checker.check_all()
        all_healthy = all(r.get('status') == 'healthy' for r in results.values())
        return {"status": "ready" if all_healthy else "not_ready", "modules": results}

    async def get_health_status(self, request_data: Dict) -> Dict:
        results = await self.api.health_checker.check_all()
        return {"health": results}

class OAuthHandler(BaseHandler):
    async def token(self, request_data: Dict) -> Dict:
        body = request_data.get('body', {})
        grant_type = body.get('grant_type', 'client_credentials')
        client_id = body.get('client_id', 'default')
        scopes = body.get('scope', 'read').split()

        if grant_type == 'client_credentials':
            access_token = self.api.oauth2_manager.create_access_token(client_id, scopes)
            refresh_token = await self.api.oauth2_manager.create_refresh_token(client_id)
            return {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'token_type': 'Bearer',
                'expires_in': self.config.access_token_expiry_minutes * 60,
                'scope': ' '.join(scopes)
            }
        elif grant_type == 'refresh_token':
            refresh_token = body.get('refresh_token', '')
            result = await self.api.oauth2_manager.refresh_access_token(refresh_token)
            if result:
                return result
            return error_response(400, "INVALID_GRANT", "Invalid refresh token")
        return error_response(400, "UNSUPPORTED_GRANT_TYPE", "Grant type not supported")

    async def revoke(self, request_data: Dict) -> Dict:
        body = request_data.get('body', {})
        token = body.get('token', '')
        if token:
            await self.api.oauth2_manager.revoke_token(token)
            return {'success': True}
        return error_response(400, "INVALID_TOKEN", "Token required")

    async def config(self, request_data: Dict) -> Dict:
        return self.api.oauth2_manager.get_config()

class TokenHandler(BaseHandler):
    async def get_summary(self, request_data: Dict) -> Dict:
        if not self.api.token_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Token manager not available")
        return self.api.token_manager.get_system_summary()

    async def get_accounts(self, request_data: Dict) -> Dict:
        if not self.api.token_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Token manager not available")
        return {"accounts": self.api.token_manager.list_accounts()}

    async def generate_tokens(self, request_data: Dict) -> Dict:
        if not self.api.token_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Token manager not available")
        body = request_data.get('body', {})
        # Validate using Pydantic if available
        if PYDANTIC_AVAILABLE:
            try:
                req = TokenGenerateRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = TokenGenerateRequest(**body)
        account_id = req.account_id
        source = getattr(EcoATPSource, req.source, EcoATPSource.GRADIENT_CONVERSION)
        tokens = self.api.token_manager.generate_tokens(
            account_id=account_id,
            source=source,
            energy_saved_kwh=req.energy_saved_kwh,
            efficiency=req.efficiency
        )
        return {"tokens": [t.dict() for t in tokens]}

    async def reserve_tokens(self, request_data: Dict) -> Dict:
        if not self.api.token_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Token manager not available")
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = TokenReserveRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = TokenReserveRequest(**body)
        account_id = req.account_id
        amount = req.amount
        consumer = getattr(EcoATPConsumer, req.consumer, EcoATPConsumer.EXPERT_EXECUTION)
        success, token_ids = self.api.token_manager.reserve_tokens(account_id, amount, consumer)
        return {"success": success, "token_ids": token_ids}

    async def get_economic_indicators(self, request_data: Dict) -> Dict:
        if not self.api.supply_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Supply manager not available")
        return self.api.supply_manager.get_economic_indicators()

class GradientHandler(BaseHandler):
    async def get_summary(self, request_data: Dict) -> Dict:
        if not self.api.gradient_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Gradient manager not available")
        return {
            'field_strengths': self.api.gradient_manager.get_field_strengths(),
            'detailed_stats': self.api.gradient_manager.get_field_stats(),
            'dominant_field': self.api.gradient_manager.get_dominant_field()
        }

    async def get_forecasts(self, request_data: Dict) -> Dict:
        if not self.api.gradient_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Gradient manager not available")
        # Assume gradient manager has get_forecasts method
        if hasattr(self.api.gradient_manager, 'get_forecasts'):
            return self.api.gradient_manager.get_forecasts()
        return {"forecasts": []}

    async def get_causal_analysis(self, request_data: Dict) -> Dict:
        if not self.api.gradient_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Gradient manager not available")
        if hasattr(self.api.gradient_manager, 'get_causal_analysis'):
            return self.api.gradient_manager.get_causal_analysis()
        return {"analysis": {}}

    async def pump_gradient(self, request_data: Dict) -> Dict:
        if not self.api.gradient_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Gradient manager not available")
        body = request_data.get('body', {})
        field_id = body.get('field_id', '')
        amount = body.get('amount', 0.0)
        source = body.get('source', 'api')
        if not field_id:
            return error_response(400, "MISSING_FIELD_ID", "field_id required")
        self.api.gradient_manager.pump_field(field_id, amount, source)
        return {"success": True, "field_id": field_id, "amount": amount}

class CompartmentHandler(BaseHandler):
    async def get_summary(self, request_data: Dict) -> Dict:
        if not self.api.compartment_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Compartment manager not available")
        return self.api.compartment_manager.get_ecosystem_stats()

    async def get_regions(self, request_data: Dict) -> Dict:
        if not self.api.compartment_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Compartment manager not available")
        return {"regions": self.api.compartment_manager.list_regions()}

    async def create_compartment(self, request_data: Dict) -> Dict:
        if not self.api.compartment_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Compartment manager not available")
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = CompartmentCreateRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = CompartmentCreateRequest(**body)
        compartment = self.api.compartment_manager.create_compartment(
            name=req.name,
            region=req.region,
            capacity=req.capacity
        )
        return {"compartment": compartment}

class BiomassHandler(BaseHandler):
    async def get_summary(self, request_data: Dict) -> Dict:
        if not self.api.biomass_storage:
            return error_response(503, "SERVICE_UNAVAILABLE", "Biomass storage not available")
        return self.api.biomass_storage.get_storage_stats()

    async def store_task(self, request_data: Dict) -> Dict:
        if not self.api.biomass_storage:
            return error_response(503, "SERVICE_UNAVAILABLE", "Biomass storage not available")
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = BiomassStoreRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = BiomassStoreRequest(**body)
        tier = getattr(StorageTier, req.tier.upper(), StorageTier.STANDARD)
        guarantee = getattr(GuaranteeLevel, req.guarantee.upper(), GuaranteeLevel.SILVER)
        task_id = self.api.biomass_storage.store(
            task_id=req.task_id,
            data=req.data,
            tier=tier,
            guarantee=guarantee
        )
        return {"task_id": task_id, "tier": req.tier, "guarantee": req.guarantee}

    async def retrieve_task(self, request_data: Dict) -> Dict:
        if not self.api.biomass_storage:
            return error_response(503, "SERVICE_UNAVAILABLE", "Biomass storage not available")
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = BiomassRetrieveRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = BiomassRetrieveRequest(**body)
        data = self.api.biomass_storage.retrieve(req.task_id, req.verify_hash)
        if data is None:
            return error_response(404, "TASK_NOT_FOUND", "Task not found or hash mismatch")
        return {"task_id": req.task_id, "data": data}

    async def get_analytics(self, request_data: Dict) -> Dict:
        if not self.api.biomass_storage:
            return error_response(503, "SERVICE_UNAVAILABLE", "Biomass storage not available")
        return self.api.biomass_storage.get_analytics()

    async def get_forecast(self, request_data: Dict) -> Dict:
        if not self.api.biomass_storage:
            return error_response(503, "SERVICE_UNAVAILABLE", "Biomass storage not available")
        if hasattr(self.api.biomass_storage, 'get_forecast'):
            return self.api.biomass_storage.get_forecast()
        return {"forecast": {}}

class HarvesterHandler(BaseHandler):
    async def get_summary(self, request_data: Dict) -> Dict:
        if not self.api.harvester:
            return error_response(503, "SERVICE_UNAVAILABLE", "Harvester not available")
        return self.api.harvester.get_harvesting_stats()

    async def harvest_cycle(self, request_data: Dict) -> Dict:
        if not self.api.harvester:
            return error_response(503, "SERVICE_UNAVAILABLE", "Harvester not available")
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = HarvestCycleRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = HarvestCycleRequest(**body)
        if req.mode:
            self.api.harvester.set_mode(req.mode)
        result = await self.api.harvester.harvest_cycle(req.environmental_data)
        return result

    async def get_circadian_report(self, request_data: Dict) -> Dict:
        if not self.api.harvester:
            return error_response(503, "SERVICE_UNAVAILABLE", "Harvester not available")
        return {"report": self.api.harvester.get_circadian_report()}

class DegradationHandler(BaseHandler):
    async def get_status(self, request_data: Dict) -> Dict:
        if not self.api.degradation_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Degradation manager not available")
        return self.api.degradation_manager.get_status()

    async def get_history(self, request_data: Dict) -> Dict:
        if not self.api.degradation_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Degradation manager not available")
        return self.api.degradation_manager.get_history()

    async def get_chaos_report(self, request_data: Dict) -> Dict:
        if not self.api.degradation_manager:
            return error_response(503, "SERVICE_UNAVAILABLE", "Degradation manager not available")
        return self.api.degradation_manager.get_chaos_report()

class KnowledgeHandler(BaseHandler):
    async def get_packages(self, request_data: Dict) -> Dict:
        if not self.api.knowledge_transfer:
            return error_response(503, "SERVICE_UNAVAILABLE", "Knowledge transfer not available")
        return self.api.knowledge_transfer.list_packages()

    async def transfer_knowledge(self, request_data: Dict) -> Dict:
        if not self.api.knowledge_transfer:
            return error_response(503, "SERVICE_UNAVAILABLE", "Knowledge transfer not available")
        body = request_data.get('body', {})
        package_id = body.get('package_id', '')
        target = body.get('target', '')
        if not package_id or not target:
            return error_response(400, "MISSING_FIELDS", "package_id and target required")
        result = await self.api.knowledge_transfer.transfer(package_id, target)
        return {"success": result}

class SystemHandler(BaseHandler):
    async def get_overview(self, request_data: Dict) -> Dict:
        if not self.api.bio_core:
            return error_response(503, "SERVICE_UNAVAILABLE", "Bio core not available")
        return self.api.bio_core.get_system_status()

    async def get_recommendations(self, request_data: Dict) -> Dict:
        if not self.api.bio_core:
            return error_response(503, "SERVICE_UNAVAILABLE", "Bio core not available")
        return self.api.bio_core.get_recommendations()

    async def run_what_if_analysis(self, request_data: Dict) -> Dict:
        if not self.api.bio_core:
            return error_response(503, "SERVICE_UNAVAILABLE", "Bio core not available")
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = WhatIfRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = WhatIfRequest(**body)
        result = await self.api.bio_core.run_what_if_analysis(req.scenario, req.horizon_hours)
        return {"result": result}

    async def get_system_load(self, request_data: Dict) -> Dict:
        if not self.api.bio_core:
            return error_response(503, "SERVICE_UNAVAILABLE", "Bio core not available")
        return self.api.bio_core.get_system_load()

class WebhookHandler(BaseHandler):
    async def subscribe(self, request_data: Dict) -> Dict:
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = WebhookSubscribeRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = WebhookSubscribeRequest(**body)
        event_type = req.event_type
        callback_url = req.callback_url
        max_retries = req.max_retries or self.config.webhook_max_retries

        if not event_type or not callback_url:
            return error_response(400, "MISSING_FIELDS", "event_type and callback_url required")

        subscription_id = await self.api.webhook_manager.subscribe(event_type, callback_url)
        # Set max_retries
        if subscription_id in self.api.webhook_manager.subscriptions:
            self.api.webhook_manager.subscriptions[subscription_id].max_retries = max_retries
        secret = self.api.webhook_manager.subscriptions[subscription_id].secret

        return {
            'success': True,
            'subscription_id': subscription_id,
            'event_type': event_type,
            'max_retries': max_retries,
            'secret': secret
        }

    async def unsubscribe(self, request_data: Dict) -> Dict:
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = WebhookUnsubscribeRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = WebhookUnsubscribeRequest(**body)
        subscription_id = req.subscription_id
        success = await self.api.webhook_manager.unsubscribe(subscription_id)
        return {'success': success, 'subscription_id': subscription_id}

    async def get_stats(self, request_data: Dict) -> Dict:
        return self.api.webhook_manager.get_webhook_stats()

class MetricsHandler(BaseHandler):
    async def get_metrics(self, request_data: Dict) -> Dict:
        # Prometheus-compatible metrics
        metrics = []
        timestamp_ms = int(time.time() * 1000)

        if self.api.token_manager:
            summary = self.api.token_manager.get_system_summary()
            metrics.append(f'green_agent_ecoatp_balance {summary.get("total_balance", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_ecoatp_efficiency {summary.get("system_efficiency", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_emergency_mode {1 if summary.get("emergency_mode") else 0} {timestamp_ms}')

        if self.api.gradient_manager:
            for field_id, strength in self.api.gradient_manager.get_field_strengths().items():
                metrics.append(f'green_agent_gradient{{field="{field_id}"}} {strength} {timestamp_ms}')

        if self.api.compartment_manager:
            stats = self.api.compartment_manager.get_ecosystem_stats()
            metrics.append(f'green_agent_compartments_viable {stats.get("viable_compartments", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_compartments_total {stats.get("total_compartments", 0)} {timestamp_ms}')

        if self.api.biomass_storage:
            stats = self.api.biomass_storage.get_storage_stats()
            metrics.append(f'green_agent_biomass_total {stats.get("total_stored", 0)} {timestamp_ms}')
            for tier, count in stats.get('tiers', {}).items():
                metrics.append(f'green_agent_biomass_tier{{tier="{tier}"}} {count} {timestamp_ms}')

        if self.api.harvester:
            harvester_stats = self.api.harvester.get_harvesting_stats()
            metrics.append(f'green_agent_harvester_total {harvester_stats.get("total_harvested", 0)} {timestamp_ms}')

        if self.api.scheduler:
            scheduler_stats = self.api.scheduler.get_scheduler_stats()
            metrics.append(f'green_agent_atp_rate {scheduler_stats.get("current_atp_rate", 0)} {timestamp_ms}')
            metrics.append(f'green_agent_atp_efficiency {scheduler_stats.get("primary_efficiency", 0)} {timestamp_ms}')

        # Rate limiter metrics
        limiter_stats = self.api.adaptive_limiter.get_stats()
        metrics.append(f'green_agent_rate_multiplier {limiter_stats.get("current_multiplier", 1.0)} {timestamp_ms}')
        metrics.append(f'green_agent_system_load {limiter_stats.get("avg_load", 0.5)} {timestamp_ms}')

        return {'metrics': '\n'.join(metrics), 'content_type': 'text/plain'}

    async def get_histograms(self, request_data: Dict) -> Dict:
        return self.api.latency_histogram

class AdminHandler(BaseHandler):
    async def get_api_keys(self, request_data: Dict) -> Dict:
        return self.api.api_key_manager.get_key_stats()

    async def create_api_key(self, request_data: Dict) -> Dict:
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = APIKeyCreateRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = APIKeyCreateRequest(**body)
        name = req.name
        rate_limit = req.rate_limit or self.config.default_rate_limit
        role = req.role
        key = self.api.api_key_manager.create_key(name, rate_limit, role)

        # Audit log
        user = request_data.get('api_key_info', {}).get('name', 'unknown')
        await self.api.audit_logger.log('CREATE_API_KEY', user, {'name': name, 'role': role})

        return {
            'success': True,
            'api_key': key,
            'name': name,
            'rate_limit': rate_limit,
            'role': role,
            'message': 'Store this key securely. It cannot be retrieved later.'
        }

    async def revoke_api_key(self, request_data: Dict) -> Dict:
        body = request_data.get('body', {})
        if PYDANTIC_AVAILABLE:
            try:
                req = APIKeyRevokeRequest(**body)
            except Exception as e:
                return error_response(400, "INVALID_REQUEST", str(e))
        else:
            req = APIKeyRevokeRequest(**body)
        api_key = req.api_key
        success = self.api.api_key_manager.revoke_key(api_key)
        if success:
            user = request_data.get('api_key_info', {}).get('name', 'unknown')
            await self.api.audit_logger.log('REVOKE_API_KEY', user, {'api_key': api_key})
        return {'success': success, 'message': 'Key revoked' if success else 'Key not found'}

# ============================================================================
# Enhanced Bio-Inspired API (Main Class)
# ============================================================================

class BioInspiredAPI:
    """
    Enhanced Bio-Inspired API v8.0.0
    Complete RESTful API with all enhancements integrated.
    """

    def __init__(self, bio_core=None, config: Optional[Union[APIConfig, Dict]] = None):
        self.bio_core = bio_core
        # Load config
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = APIConfig(**config)
            else:
                self.config = APIConfig(**config)
        elif isinstance(config, APIConfig):
            self.config = config
        else:
            self.config = APIConfig()

        # Initialize core components from bio_core
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

        # Sub-components
        # Token store
        if self.config.refresh_token_store_backend == "redis" and REDIS_AVAILABLE and self.config.refresh_token_redis_url:
            self.token_store = RedisTokenStore(self.config.refresh_token_redis_url)
        else:
            self.token_store = FileTokenStore(self.config.refresh_token_file_path)

        self.oauth2_manager = OAuth2Manager(self.config, self.token_store)

        # Rate limiter
        self.adaptive_limiter = SlidingWindowRateLimiter(self.config)

        # API key manager
        self.api_key_manager = APIKeyManager(self.config)

        # Cache
        if self.config.cache_backend == "redis" and REDIS_AVAILABLE and self.config.cache_redis_url:
            self.cache = RedisCacheBackend(self.config.cache_redis_url, self.config.cache_ttl_seconds)
        else:
            self.cache = MemoryCacheBackend(self.config.cache_max_items)

        # Webhook manager
        self.webhook_manager = WebhookManager(self.config, self.event_bus)

        # Health checker
        self.health_checker = HealthChecker(self)

        # Audit logger
        self.audit_logger = AuditLogger(self.config)

        # WebSocket
        self.websocket_server = None
        if self.config.websocket_enabled and WEBSOCKETS_AVAILABLE:
            self.websocket_server = WebSocketServer(self, self.config.websocket_port)
            asyncio.create_task(self.websocket_server.start())

        # Request history and latency histograms
        self.request_history = deque(maxlen=10000)
        self.latency_histogram = defaultdict(list)

        # Route registry with OpenAPI metadata
        self.routes = {}
        self.handlers = {}  # store handler instances

        # Initialize handlers
        self._init_handlers()

        # Register routes
        self._register_routes()

        # Background tasks
        self._background_tasks = []
        self._start_background_tasks()

        logger.info(f"Enhanced Bio-Inspired API v8.0.0 initialized", config=self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config))

    def _init_handlers(self):
        """Instantiate all handlers."""
        self.handlers['health'] = HealthHandler(self)
        self.handlers['oauth'] = OAuthHandler(self)
        self.handlers['token'] = TokenHandler(self)
        self.handlers['gradient'] = GradientHandler(self)
        self.handlers['compartment'] = CompartmentHandler(self)
        self.handlers['biomass'] = BiomassHandler(self)
        self.handlers['harvester'] = HarvesterHandler(self)
        self.handlers['degradation'] = DegradationHandler(self)
        self.handlers['knowledge'] = KnowledgeHandler(self)
        self.handlers['system'] = SystemHandler(self)
        self.handlers['webhook'] = WebhookHandler(self)
        self.handlers['metrics'] = MetricsHandler(self)
        self.handlers['admin'] = AdminHandler(self)

    def _start_background_tasks(self):
        """Start background tasks."""
        self._background_tasks.append(asyncio.create_task(self._token_cleanup_loop()))
        # Webhook processing is started on demand

    async def _token_cleanup_loop(self):
        """Periodically clean expired refresh tokens."""
        while True:
            await asyncio.sleep(3600)  # every hour
            await self.token_store.clean_expired()

    def _register_routes(self):
        """Register all API routes with metadata for OpenAPI."""
        prefix = f"{self.config.prefix}/{self.config.api_version}"

        # Health
        self._add_route("GET", f"{prefix}/health/live", self.handlers['health'].get_live, summary="Liveness probe", tags=["health"])
        self._add_route("GET", f"{prefix}/health/ready", self.handlers['health'].get_ready, summary="Readiness probe", tags=["health"])
        self._add_route("GET", f"{prefix}/health/status", self.handlers['health'].get_health_status, summary="Detailed health status", tags=["health"])

        # OAuth2
        self._add_route("POST", f"{prefix}/oauth/token", self.handlers['oauth'].token, summary="OAuth2 token endpoint", tags=["auth"])
        self._add_route("POST", f"{prefix}/oauth/revoke", self.handlers['oauth'].revoke, summary="Revoke token", tags=["auth"])
        self._add_route("GET", f"{prefix}/oauth/config", self.handlers['oauth'].config, summary="OAuth2 configuration", tags=["auth"])

        # Swagger
        self._add_route("GET", f"{prefix}/docs", self.get_swagger_ui, summary="Swagger UI", tags=["docs"])
        self._add_route("GET", f"{prefix}/openapi.json", self.get_openapi_spec, summary="OpenAPI spec", tags=["docs"])

        # Token Economy
        self._add_route("GET", f"{prefix}/tokens/summary", self.handlers['token'].get_summary, summary="Token economy summary", tags=["tokens"], cache_ttl=10)
        self._add_route("GET", f"{prefix}/tokens/accounts", self.handlers['token'].get_accounts, summary="List token accounts", tags=["tokens"])
        self._add_route("POST", f"{prefix}/tokens/generate", self.handlers['token'].generate_tokens, summary="Generate tokens", tags=["tokens"], auth_required=True)
        self._add_route("POST", f"{prefix}/tokens/reserve", self.handlers['token'].reserve_tokens, summary="Reserve tokens", tags=["tokens"], auth_required=True)
        self._add_route("GET", f"{prefix}/tokens/economic", self.handlers['token'].get_economic_indicators, summary="Economic indicators", tags=["tokens"])

        # Gradients
        self._add_route("GET", f"{prefix}/gradients/summary", self.handlers['gradient'].get_summary, summary="Gradient summary", tags=["gradients"], cache_ttl=5)
        self._add_route("GET", f"{prefix}/gradients/forecasts", self.handlers['gradient'].get_forecasts, summary="Gradient forecasts", tags=["gradients"])
        self._add_route("GET", f"{prefix}/gradients/causal", self.handlers['gradient'].get_causal_analysis, summary="Causal analysis", tags=["gradients"])
        self._add_route("POST", f"{prefix}/gradients/pump", self.handlers['gradient'].pump_gradient, summary="Pump gradient field", tags=["gradients"], auth_required=True)

        # ATP Synthase
        self._add_route("GET", f"{prefix}/atp-synthase/status", self.handlers['system'].get_overview, summary="ATP synthase status", tags=["atp"], cache_ttl=5)
        self._add_route("GET", f"{prefix}/atp-synthase/efficiency", self.handlers['system'].get_overview, summary="Efficiency report", tags=["atp"])

        # Compartments
        self._add_route("GET", f"{prefix}/compartments/summary", self.handlers['compartment'].get_summary, summary="Compartment summary", tags=["compartments"])
        self._add_route("GET", f"{prefix}/compartments/regions", self.handlers['compartment'].get_regions, summary="Compartment regions", tags=["compartments"])
        self._add_route("POST", f"{prefix}/compartments/create", self.handlers['compartment'].create_compartment, summary="Create compartment", tags=["compartments"], auth_required=True)

        # Biomass
        self._add_route("GET", f"{prefix}/biomass/summary", self.handlers['biomass'].get_summary, summary="Biomass storage summary", tags=["biomass"])
        self._add_route("POST", f"{prefix}/biomass/store", self.handlers['biomass'].store_task, summary="Store task in biomass", tags=["biomass"], auth_required=True)
        self._add_route("POST", f"{prefix}/biomass/retrieve", self.handlers['biomass'].retrieve_task, summary="Retrieve task from biomass", tags=["biomass"], auth_required=True)
        self._add_route("GET", f"{prefix}/biomass/analytics", self.handlers['biomass'].get_analytics, summary="Biomass analytics", tags=["biomass"])
        self._add_route("GET", f"{prefix}/biomass/forecast", self.handlers['biomass'].get_forecast, summary="Biomass forecast", tags=["biomass"])

        # Harvester
        self._add_route("GET", f"{prefix}/harvester/summary", self.handlers['harvester'].get_summary, summary="Harvester summary", tags=["harvester"])
        self._add_route("POST", f"{prefix}/harvester/cycle", self.handlers['harvester'].harvest_cycle, summary="Execute harvest cycle", tags=["harvester"], auth_required=True)
        self._add_route("GET", f"{prefix}/harvester/circadian", self.handlers['harvester'].get_circadian_report, summary="Circadian report", tags=["harvester"])

        # Degradation
        self._add_route("GET", f"{prefix}/degradation/status", self.handlers['degradation'].get_status, summary="Degradation status", tags=["degradation"])
        self._add_route("GET", f"{prefix}/degradation/history", self.handlers['degradation'].get_history, summary="Degradation history", tags=["degradation"])
        self._add_route("GET", f"{prefix}/degradation/chaos", self.handlers['degradation'].get_chaos_report, summary="Chaos engineering report", tags=["degradation"])

        # Knowledge Transfer
        self._add_route("GET", f"{prefix}/knowledge/packages", self.handlers['knowledge'].get_packages, summary="Knowledge packages", tags=["knowledge"])
        self._add_route("POST", f"{prefix}/knowledge/transfer", self.handlers['knowledge'].transfer_knowledge, summary="Transfer knowledge", tags=["knowledge"], auth_required=True)

        # System
        self._add_route("GET", f"{prefix}/system/overview", self.handlers['system'].get_overview, summary="System overview", tags=["system"])
        self._add_route("GET", f"{prefix}/system/recommendations", self.handlers['system'].get_recommendations, summary="System recommendations", tags=["system"])
        self._add_route("POST", f"{prefix}/system/what-if", self.handlers['system'].run_what_if_analysis, summary="What-if analysis", tags=["system"], auth_required=True)
        self._add_route("GET", f"{prefix}/system/load", self.handlers['system'].get_system_load, summary="System load", tags=["system"])

        # Webhooks
        self._add_route("POST", f"{prefix}/webhooks/subscribe", self.handlers['webhook'].subscribe, summary="Subscribe webhook", tags=["webhooks"], auth_required=True)
        self._add_route("POST", f"{prefix}/webhooks/unsubscribe", self.handlers['webhook'].unsubscribe, summary="Unsubscribe webhook", tags=["webhooks"], auth_required=True)
        self._add_route("GET", f"{prefix}/webhooks/stats", self.handlers['webhook'].get_stats, summary="Webhook statistics", tags=["webhooks"])

        # Metrics
        self._add_route("GET", f"{prefix}/metrics", self.handlers['metrics'].get_metrics, summary="Prometheus metrics", tags=["metrics"])
        self._add_route("GET", f"{prefix}/metrics/histograms", self.handlers['metrics'].get_histograms, summary="Latency histograms", tags=["metrics"])

        # Admin
        self._add_route("GET", f"{prefix}/admin/keys", self.handlers['admin'].get_api_keys, summary="List API keys", tags=["admin"], auth_required=True, role="admin")
        self._add_route("POST", f"{prefix}/admin/keys/create", self.handlers['admin'].create_api_key, summary="Create API key", tags=["admin"], auth_required=True, role="admin")
        self._add_route("POST", f"{prefix}/admin/keys/revoke", self.handlers['admin'].revoke_api_key, summary="Revoke API key", tags=["admin"], auth_required=True, role="admin")

    def _add_route(self, method: str, path: str, handler: Callable, **metadata):
        """Register a route with metadata for OpenAPI."""
        self.routes[path] = (method, handler, metadata)

    # ============================================================================
    # Authentication Decorators (enhanced)
    # ============================================================================

    def require_auth(self, func):
        """Decorator to require authentication (API key or OAuth2)."""
        @wraps(func)
        async def wrapper(self, request_data: Dict[str, Any], *args, **kwargs):
            # Check OAuth2 token
            auth_header = request_data.get('headers', {}).get('Authorization', '')
            if auth_header.startswith('Bearer '):
                token = auth_header[7:]
                payload = await self.oauth2_manager.validate_token(token)
                if payload:
                    request_data['oauth_payload'] = payload
                    request_data['auth_type'] = 'oauth2'
                    return await func(self, request_data, *args, **kwargs)

            # Check API key
            api_key = request_data.get('headers', {}).get('X-API-Key', '')
            if not api_key:
                return error_response(401, "UNAUTHORIZED", "Authentication required. Provide X-API-Key or Bearer token.")
            key_data = self.api_key_manager.validate_key(api_key)
            if not key_data:
                return error_response(403, "INVALID_API_KEY", "Invalid API key.")
            # Rate limit check
            allowed, rate_info = self.api_key_manager.check_rate_limit(api_key)
            if not allowed:
                return error_response(429, "RATE_LIMITED", "Rate limit exceeded", rate_info)
            request_data['api_key_info'] = key_data
            request_data['rate_info'] = rate_info
            request_data['auth_type'] = 'api_key'
            return await func(self, request_data, *args, **kwargs)
        return wrapper

    def require_role(self, role: str):
        """Decorator to require a specific role."""
        def decorator(func):
            @wraps(func)
            async def wrapper(self, request_data: Dict[str, Any], *args, **kwargs):
                # OAuth2 scopes
                oauth_payload = request_data.get('oauth_payload', {})
                if oauth_payload:
                    scopes = oauth_payload.get('scopes', [])
                    if role in scopes or 'admin' in scopes:
                        return await func(self, request_data, *args, **kwargs)
                # API key role
                key_data = request_data.get('api_key_info', {})
                if key_data.get('role') != role and key_data.get('role') != 'admin':
                    return error_response(403, "FORBIDDEN", f"Role '{role}' required.")
                return await func(self, request_data, *args, **kwargs)
            return wrapper
        return decorator

    # ============================================================================
    # Request Handler (with correlation ID and latency)
    # ============================================================================

    async def handle_request(self, method: str, path: str,
                             headers: Dict[str, str] = None,
                             body: Dict[str, Any] = None,
                             query_params: Dict[str, str] = None) -> Dict[str, Any]:
        start_time = time.time()
        request_id = CorrelationIDMiddleware.generate()

        request_data = {
            'method': method,
            'path': path,
            'headers': headers or {},
            'body': body or {},
            'query_params': query_params or {},
            'timestamp': datetime.now(timezone.utc),
            'request_id': request_id
        }

        # Update adaptive rate limiter with system load
        if self.bio_core:
            status = self.bio_core.get_system_status()
            load = status.get('token_economy', {}).get('utilization', 0.5)
            self.adaptive_limiter.update_system_load(load)

        # Find route
        route = self.routes.get(path)
        if not route:
            return CorrelationIDMiddleware.add_to_response(
                error_response(404, "NOT_FOUND", f"No endpoint at {path}"),
                request_id
            )
        expected_method, handler, metadata = route
        if method != expected_method:
            return CorrelationIDMiddleware.add_to_response(
                error_response(405, "METHOD_NOT_ALLOWED", f"Expected {expected_method}"),
                request_id
            )

        try:
            # Handle caching if enabled
            if metadata.get('cache_ttl') and self.config.cache_enabled:
                cache_key = f"{path}:{json.dumps(query_params, sort_keys=True)}"
                cached = await self.cache.get(cache_key)
                if cached:
                    cached['meta']['cached'] = True
                    return cached

            # Execute handler
            result = await handler(self, request_data)
            if isinstance(result, dict) and 'status' not in result:
                result['status'] = 200

            # Cache if applicable
            if metadata.get('cache_ttl') and self.config.cache_enabled and result.get('status') == 200:
                await self.cache.set(cache_key, result, metadata['cache_ttl'])

            # Add meta
            latency_ms = (time.time() - start_time) * 1000
            result['meta'] = {
                'api_version': self.config.api_version,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'response_time_ms': latency_ms,
                'request_id': request_id
            }
            # Track latency
            self.latency_histogram[path].append(latency_ms)
            if len(self.latency_histogram[path]) > 1000:
                self.latency_histogram[path] = self.latency_histogram[path][-1000:]
            # Record request
            self.request_history.append({
                'method': method,
                'path': path,
                'status': result.get('status', 200),
                'latency_ms': latency_ms,
                'request_id': request_id,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            return result
        except APIError as e:
            response = error_response(e.status_code, e.code, e.message, e.details)
            return CorrelationIDMiddleware.add_to_response(response, request_id)
        except Exception as e:
            logger.error("Unhandled API error", error=str(e), exc_info=True)
            response = error_response(500, "INTERNAL_ERROR", "Internal server error")
            return CorrelationIDMiddleware.add_to_response(response, request_id)

    # ============================================================================
    # OpenAPI Generation
    # ============================================================================

    def get_swagger_ui(self, request_data: Dict) -> Dict:
        return {
            'html': self._generate_swagger_html(),
            'content_type': 'text/html'
        }

    def get_openapi_spec(self, request_data: Dict) -> Dict:
        return self._generate_openapi_spec()

    def _generate_swagger_html(self) -> str:
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
                        url: "{self.config.prefix}/{self.config.api_version}/openapi.json",
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

    def _generate_openapi_spec(self) -> Dict:
        """Generate OpenAPI 3.0 specification from route metadata."""
        paths = {}
        for path, (method, handler, metadata) in self.routes.items():
            if method not in paths:
                paths[path] = {}
            operation = {
                'summary': metadata.get('summary', ''),
                'tags': metadata.get('tags', []),
                'responses': {
                    '200': {'description': 'Success'},
                    '400': {'description': 'Bad Request'},
                    '401': {'description': 'Unauthorized'},
                    '403': {'description': 'Forbidden'},
                    '404': {'description': 'Not Found'},
                    '429': {'description': 'Rate Limited'},
                    '500': {'description': 'Internal Error'}
                }
            }
            if metadata.get('auth_required'):
                operation['security'] = [{'ApiKeyAuth': []}, {'OAuth2': []}]
            paths[path][method.lower()] = operation

        return {
            "openapi": "3.0.0",
            "info": {
                "title": "Green Agent Bio-Inspired API",
                "version": self.config.api_version,
                "description": "RESTful API for the Green Agent metabolic ecosystem"
            },
            "servers": [{"url": f"{self.config.prefix}/{self.config.api_version}"}],
            "paths": paths,
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
                                "tokenUrl": f"{self.config.prefix}/{self.config.api_version}/oauth/token",
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

    # ============================================================================
    # Graceful Shutdown
    # ============================================================================

    async def shutdown(self):
        """Gracefully shut down the API."""
        logger.info("Shutting down Bio-Inspired API")
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        # Stop WebSocket server
        if self.websocket_server:
            await self.websocket_server.stop()
        # Stop webhook processing
        await self.webhook_manager.shutdown()
        # Save token store
        if hasattr(self.token_store, '_save'):
            await self.token_store._save()
        logger.info("Bio-Inspired API shutdown complete")

    # ============================================================================
    # Async context manager
    # ============================================================================

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# ============================================================================
# Legacy compatibility (if needed)
# ============================================================================

class BioInspiredAPIv1(BioInspiredAPI):
    """Legacy compatibility wrapper."""
    def __init__(self, bio_core=None):
        super().__init__(bio_core=bio_core)
        logger.info("BioInspiredAPIv1 (legacy) initialized")

# ============================================================================
# Example usage
# ============================================================================

async def example():
    logging.basicConfig(level=logging.INFO)
    # Create a mock bio-core
    class MockBioCore:
        def get_system_status(self):
            return {'token_economy': {'utilization': 0.5}}
        def get_system_summary(self):
            return {'total_balance': 1000, 'system_efficiency': 0.8}
        def update_configuration(self, updates):
            pass
        def run_what_if_analysis(self, scenario, horizon):
            return {'result': 'simulation'}

    api = BioInspiredAPI(bio_core=MockBioCore())
    # Simulate a request
    request = {
        'method': 'GET',
        'path': '/api/v1/health/live',
        'headers': {},
        'body': {},
        'query_params': {}
    }
    response = await api.handle_request(**request)
    print(response)
    await api.shutdown()

if __name__ == "__main__":
    asyncio.run(example())
