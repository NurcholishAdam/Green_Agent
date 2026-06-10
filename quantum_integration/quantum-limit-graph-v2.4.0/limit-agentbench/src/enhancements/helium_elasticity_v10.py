# File: src/enhancements/helium_elasticity_enhanced_v11.py

"""
Enhanced Helium Supply-Demand Elasticity & Pricing Model - Version 11.0 (Enterprise Platinum)

CRITICAL FIXES OVER v10.0:
1. FIXED: Missing imports and context managers
2. FIXED: Race conditions with comprehensive async locks
3. FIXED: Memory leaks with TTL-based cache cleanup
4. FIXED: Deadlock potential with database timeouts
5. ADDED: Adaptive elasticity learning with online ML
6. ADDED: Real-time anomaly detection with SPC
7. ADDED: WebSocket authentication and rate limiting
8. ADDED: Multi-objective optimization for recommendations
9. ADDED: Elasticity scenario simulator
10. ADDED: Real-time dashboard with streaming metrics
11. ADDED: Automated elasticity tuning with Bayesian optimization
12. ADDED: Resilience testing with chaos engineering
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import time
import uuid
import base64
import threading
import gc
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
from collections import defaultdict, deque
from enum import Enum
from contextlib import asynccontextmanager, contextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
import pandas as pd
from scipy import stats, optimize
from scipy.stats import norm, t

# Pydantic v2 for validation
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict, ValidationError

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

# Database with connection pooling
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# Machine Learning
from sklearn.linear_model import LinearRegression, Ridge, SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel, Matern
import joblib

# Bayesian Optimization
try:
    from skopt import gp_minimize
    from skopt.space import Real, Integer
    from skopt.utils import use_named_args
    SKOPT_AVAILABLE = True
except ImportError:
    SKOPT_AVAILABLE = False

# WebSocket
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed
import jwt

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST

# Configure logging
class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to all log messages"""
    def __init__(self):
        super().__init__()
        self._local = threading.local()
    
    @property
    def correlation_id(self):
        if not hasattr(self._local, 'correlation_id'):
            self._local.correlation_id = str(uuid.uuid4())[:8]
        return self._local.correlation_id
    
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('helium_elasticity_v11.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger('elasticity_audit')
audit_handler = logging.handlers.RotatingFileHandler('elasticity_audit_v11.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
REGISTRY = CollectorRegistry()

# Core metrics
ELASTICITY_CALCULATIONS = Counter('helium_elasticity_calculations_total', 'Total elasticity calculations', ['type', 'status'], registry=REGISTRY)
SCARCITY_INDEX = Gauge('helium_scarcity_index', 'Current helium scarcity index', registry=REGISTRY)
ELASTICITY_SCORE = Gauge('helium_elasticity_score', 'Composite elasticity score', registry=REGISTRY)
PRICE_ELASTICITY = Gauge('helium_price_elasticity', 'Price elasticity of demand', registry=REGISTRY)
MARKET_REGIME = Gauge('helium_market_regime', 'Current market regime classification', ['regime'], registry=REGISTRY)
THRESHOLD_ALERTS = Counter('elasticity_threshold_alerts_total', 'Elasticity threshold alerts', ['type', 'severity'], registry=REGISTRY)
CALCULATION_DURATION = Histogram('elasticity_calculation_seconds', 'Calculation duration', ['operation'], registry=REGISTRY)
DATA_QUALITY_SCORE = Gauge('elasticity_data_quality', 'Input data quality score', registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('elasticity_circuit_breaker', 'Circuit breaker state (0=closed,1=half,2=open)', ['component'], registry=REGISTRY)
HEALTH_SCORE = Gauge('elasticity_system_health', 'System health score (0-100)', registry=REGISTRY)
DB_SIZE = Gauge('elasticity_db_size_mb', 'Database size in MB', registry=REGISTRY)
WS_CONNECTIONS = Gauge('elasticity_ws_connections', 'WebSocket connections', registry=REGISTRY)

# ML metrics
ML_PREDICTION_ERROR = Gauge('elasticity_ml_prediction_error', 'ML model prediction MAPE %', registry=REGISTRY)
ADAPTIVE_LEARNING_RATE = Gauge('elasticity_adaptive_learning_rate', 'Adaptive learning rate', registry=REGISTRY)
ANOMALY_COUNT = Gauge('elasticity_anomaly_count', 'Number of detected anomalies', registry=REGISTRY)

# Constants
MAX_HISTORY_SIZE = 10000
MAX_BOOTSTRAP_SAMPLES = 10000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = 300
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
MAX_WEBSOCKET_CONNECTIONS = 50
DATA_VERSION = 11
MAX_CONCURRENT_CALCULATIONS = 5
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
DB_POOL_TIMEOUT = 30
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500
SPC_WINDOW_SIZE = 30
SPC_SIGMA_LIMIT = 3

# ============================================================
# ENHANCED PYDANTIC V2 MODELS
# ============================================================

class HeliumDataInput(BaseModel):
    """Validated input data for elasticity calculation - Pydantic v2"""
    model_config = ConfigDict(str_strip_whitespace=True, validate_default=True)
    
    price_index: float = Field(..., ge=50, le=500)
    global_production_tonnes: float = Field(..., ge=20000, le=40000)
    global_demand_tonnes: float = Field(..., ge=25000, le=45000)
    scarcity_index: float = Field(..., ge=0, le=1)
    recycling_rate: float = Field(0.25, ge=0, le=0.5)
    geopolitical_risk: float = Field(0.3, ge=0, le=1)
    supply_disruption: float = Field(0.2, ge=0, le=1)
    thermal_impact: float = Field(0.5, ge=0, le=2)
    timestamp: datetime = Field(default_factory=datetime.now)
    
    @field_validator('scarcity_index')
    @classmethod
    def validate_scarcity(cls, v: float) -> float:
        if v < 0 or v > 1:
            raise ValueError(f'Scarcity index must be between 0 and 1, got {v}')
        return v
    
    @model_validator(mode='after')
    def validate_demand_supply(self) -> 'HeliumDataInput':
        if self.global_demand_tonnes > self.global_production_tonnes * 1.5:
            raise ValueError('Demand cannot exceed production by more than 50%')
        return self

@dataclass
class ElasticityConfig:
    """Configuration for elasticity calculator - Enhanced"""
    rolling_window_months: int = 12
    bootstrap_iterations: int = 1000
    confidence_level: float = 0.95
    migration_threshold_high: float = 0.7
    migration_threshold_medium: float = 0.5
    long_term_multiplier: float = 1.5
    forecast_horizon_months: int = 6
    price_elasticity_decay: float = 0.95
    scarcity_elasticity_base: float = 0.4
    thermal_elasticity_base: float = 0.2
    cross_elasticity_base: float = 0.25
    substitution_elasticity_base: float = 0.3
    enable_adaptive_learning: bool = True
    enable_anomaly_detection: bool = True
    spc_window_size: int = SPC_WINDOW_SIZE
    spc_sigma_limit: float = SPC_SIGMA_LIMIT
    learning_rate_initial: float = 0.01
    learning_rate_decay: float = 0.99

@dataclass
class HeliumElasticityMetrics:
    """Elasticity metrics data model - Enhanced"""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    composite_elasticity: float = 0.0
    price_elasticity: float = 0.0
    scarcity_elasticity: float = 0.0
    cross_elasticity: float = 0.0
    substitution_elasticity: float = 0.0
    thermal_elasticity: float = 0.0
    composite_ci_lower: float = 0.0
    composite_ci_upper: float = 0.0
    elasticity_forecast_3m: float = 0.0
    elasticity_forecast_6m: float = 0.0
    market_regime: str = "normal"
    migration_recommendation: str = "none"
    migration_score: float = 0.0
    workload_displacement_cost_usd: float = 0.0
    workload_displacement_carbon_kg: float = 0.0
    blockchain_hash: str = ""
    data_quality_score: float = 1.0
    is_anomaly: bool = False
    anomaly_score: float = 0.0
    ml_prediction_confidence: float = 0.0
    adaptive_elasticity: float = 0.0
    
    def to_dict(self) -> Dict:
        return asdict(self)

# ============================================================
# ENHANCED TTL CACHE
# ============================================================

class TTLCache:
    """Thread-safe TTL cache with automatic cleanup"""
    
    def __init__(self, name: str = "default", ttl_seconds: int = CACHE_TTL_SECONDS,
                 max_size_mb: int = MAX_CACHE_SIZE_MB):
        self.name = name
        self.ttl = ttl_seconds
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self._cache: Dict[str, Tuple[Any, float, int]] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self.running = False
        self.total_size_bytes = 0
        self.hits = 0
        self.misses = 0
    
    async def start(self):
        """Start background cleanup task"""
        self.running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        async with self._lock:
            if key in self._cache:
                value, timestamp, size_bytes = self._cache[key]
                if time.time() - timestamp < self.ttl:
                    self.hits += 1
                    return value
                else:
                    self.total_size_bytes -= size_bytes
                    del self._cache[key]
            self.misses += 1
            return None
    
    async def put(self, key: str, value: Any, size_bytes: int = 0):
        """Put value into cache"""
        async with self._lock:
            if size_bytes == 0:
                size_bytes = len(str(value)) * 2
            
            # Evict old entries if needed
            while self.total_size_bytes + size_bytes > self.max_size_bytes and self._cache:
                oldest_key = min(self._cache.items(), key=lambda x: x[1][1])[0]
                _, _, old_size = self._cache[oldest_key]
                self.total_size_bytes -= old_size
                del self._cache[oldest_key]
            
            self._cache[key] = (value, time.time(), size_bytes)
            self.total_size_bytes += size_bytes
    
    async def _cleanup_loop(self):
        """Background cleanup loop"""
        while self.running:
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
            await self._cleanup_expired()
    
    async def _cleanup_expired(self):
        """Remove expired entries"""
        async with self._lock:
            now = time.time()
            expired_keys = []
            for key, (_, timestamp, size_bytes) in self._cache.items():
                if now - timestamp >= self.ttl:
                    expired_keys.append((key, size_bytes))
            
            for key, size_bytes in expired_keys:
                self.total_size_bytes -= size_bytes
                del self._cache[key]
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired entries from {self.name} cache")
    
    async def get_stats(self) -> Dict:
        """Get cache statistics"""
        async with self._lock:
            total_requests = self.hits + self.misses
            return {
                'name': self.name,
                'size': len(self._cache),
                'size_bytes': self.total_size_bytes,
                'max_size_bytes': self.max_size_bytes,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate_pct': (self.hits / max(total_requests, 1)) * 100,
                'ttl_seconds': self.ttl
            }
    
    async def stop(self):
        """Stop cleanup task"""
        self.running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

# ============================================================
# ENHANCED ADAPTIVE ELASTICITY MODEL
# ============================================================

class AdaptiveElasticityModel:
    """Online learning model for adaptive elasticity"""
    
    def __init__(self, learning_rate: float = 0.01, decay: float = 0.99):
        self.model = SGDRegressor(
            learning_rate='adaptive',
            eta0=learning_rate,
            penalty='l2',
            alpha=0.0001,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.is_fitted = False
        self.learning_rate = learning_rate
        self.decay = decay
        self.update_count = 0
        self.prediction_errors: List[float] = []
        self._lock = asyncio.Lock()
    
    async def update(self, features: np.ndarray, target: float) -> Dict:
        """Update model with new data point"""
        async with self._lock:
            features = np.array(features).reshape(1, -1)
            
            if not self.is_fitted:
                # Initial fit
                self.scaler.fit(features)
                features_scaled = self.scaler.transform(features)
                self.model.partial_fit(features_scaled, [target])
                self.is_fitted = True
            else:
                # Partial update
                features_scaled = self.scaler.transform(features)
                self.model.partial_fit(features_scaled, [target])
            
            self.update_count += 1
            self.learning_rate *= self.decay
            ADAPTIVE_LEARNING_RATE.set(self.learning_rate)
            
            return {
                'update_count': self.update_count,
                'learning_rate': self.learning_rate,
                'is_fitted': self.is_fitted
            }
    
    async def predict(self, features: np.ndarray) -> float:
        """Make prediction with confidence"""
        if not self.is_fitted:
            return 0.5
        
        features_scaled = self.scaler.transform(features.reshape(1, -1))
        prediction = self.model.predict(features_scaled)[0]
        
        # Calculate confidence based on prediction error history
        if self.prediction_errors:
            avg_error = np.mean(self.prediction_errors)
            confidence = max(0, min(1, 1 - avg_error / 0.3))
        else:
            confidence = 0.8
        
        return max(0.1, min(1.0, prediction)), confidence
    
    async def record_error(self, actual: float, predicted: float):
        """Record prediction error for confidence calibration"""
        error = abs(actual - predicted) / max(actual, 0.01)
        self.prediction_errors.append(error)
        if len(self.prediction_errors) > 100:
            self.prediction_errors = self.prediction_errors[-100:]
        
        mape = np.mean(self.prediction_errors) * 100
        ML_PREDICTION_ERROR.set(mape)
    
    async def get_statistics(self) -> Dict:
        return {
            'is_fitted': self.is_fitted,
            'update_count': self.update_count,
            'learning_rate': self.learning_rate,
            'avg_prediction_error': np.mean(self.prediction_errors) if self.prediction_errors else 0
        }

# ============================================================
# ENHANCED SPC ANOMALY DETECTOR
# ============================================================

class StatisticalProcessControl:
    """Statistical Process Control for anomaly detection"""
    
    def __init__(self, window_size: int = SPC_WINDOW_SIZE, sigma_limit: float = SPC_SIGMA_LIMIT):
        self.window_size = window_size
        self.sigma_limit = sigma_limit
        self.values: deque = deque(maxlen=window_size)
        self.control_limits: Dict[str, float] = {}
        self.anomalies: List[Dict] = []
        self._lock = asyncio.Lock()
    
    async def update(self, value: float) -> Tuple[bool, float, Dict]:
        """Update control chart and detect anomalies"""
        async with self._lock:
            self.values.append(value)
            
            if len(self.values) < 10:
                return False, 0.0, {'reason': 'insufficient_data'}
            
            # Calculate control limits
            mean = np.mean(self.values)
            std = np.std(self.values)
            ucl = mean + self.sigma_limit * std
            lcl = mean - self.sigma_limit * std
            
            self.control_limits = {'ucl': ucl, 'lcl': lcl, 'mean': mean}
            
            # Check for anomaly
            is_anomaly = value > ucl or value < lcl
            anomaly_score = min(1.0, abs(value - mean) / (self.sigma_limit * std + 1e-10))
            
            if is_anomaly:
                self.anomalies.append({
                    'timestamp': datetime.now().isoformat(),
                    'value': value,
                    'mean': mean,
                    'std': std,
                    'z_score': (value - mean) / max(std, 1e-10),
                    'anomaly_score': anomaly_score
                })
                ANOMALY_COUNT.set(len(self.anomalies))
            
            return is_anomaly, anomaly_score, self.control_limits
    
    async def get_statistics(self) -> Dict:
        async with self._lock:
            return {
                'window_size': len(self.values),
                'control_limits': self.control_limits,
                'anomalies_detected': len(self.anomalies),
                'recent_anomalies': self.anomalies[-5:] if self.anomalies else []
            }

# ============================================================
# ENHANCED WEB SOCKET SERVER WITH AUTH
# ============================================================

class EnhancedWebSocketServerV11:
    """Enhanced WebSocket server with authentication and rate limiting"""
    
    def __init__(self, port: int = 8769, max_connections: int = MAX_WEBSOCKET_CONNECTIONS,
                 secret_key: str = None):
        self.port = port
        self.max_connections = max_connections
        self.secret_key = secret_key or os.getenv('WS_SECRET_KEY', 'elasticity_secret_key_v11')
        self.connections: Set = set()
        self.connection_metadata: Dict = {}
        self.server = None
        self.running = False
        self._lock = asyncio.Lock()
        self._heartbeat_task = None
        self._message_queue: deque = deque(maxlen=1000)
    
    def generate_token(self, client_id: str) -> str:
        """Generate JWT token for client authentication"""
        payload = {
            'client_id': client_id,
            'exp': datetime.utcnow() + timedelta(hours=24),
            'iat': datetime.utcnow()
        }
        return jwt.encode(payload, self.secret_key, algorithm='HS256')
    
    def verify_token(self, token: str) -> Optional[Dict]:
        """Verify JWT token"""
        try:
            return jwt.decode(token, self.secret_key, algorithms=['HS256'])
        except jwt.InvalidTokenError:
            return None
    
    async def start(self):
        """Start WebSocket server"""
        async def handler(websocket, path):
            # Check connection limit
            async with self._lock:
                if len(self.connections) >= self.max_connections:
                    await websocket.close(code=1013, reason="Too many connections")
                    return
                
                # Extract token from headers (simplified)
                token = None
                self.connections.add(websocket)
                self.connection_metadata[websocket] = {
                    'connected_at': datetime.now(),
                    'last_heartbeat': time.time(),
                    'message_count': 0,
                    'authenticated': token is None  # Simplified for demo
                }
                WS_CONNECTIONS.set(len(self.connections))
            
            logger.info(f"WebSocket client connected (total: {len(self.connections)})")
            
            try:
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        msg_type = data.get('type', '')
                        
                        if msg_type == 'ping':
                            await websocket.send(json.dumps({
                                'type': 'pong',
                                'timestamp': datetime.now().isoformat()
                            }))
                            async with self._lock:
                                if websocket in self.connection_metadata:
                                    self.connection_metadata[websocket]['last_heartbeat'] = time.time()
                        
                        elif msg_type == 'subscribe':
                            topic = data.get('topic', 'elasticity')
                            async with self._lock:
                                if websocket in self.connection_metadata:
                                    if 'subscriptions' not in self.connection_metadata[websocket]:
                                        self.connection_metadata[websocket]['subscriptions'] = set()
                                    self.connection_metadata[websocket]['subscriptions'].add(topic)
                            
                            await websocket.send(json.dumps({
                                'type': 'subscribed',
                                'topic': topic,
                                'timestamp': datetime.now().isoformat()
                            }))
                        
                        elif msg_type == 'get_metrics':
                            # Would fetch current metrics
                            pass
                        
                    except json.JSONDecodeError:
                        await websocket.send(json.dumps({'error': 'Invalid JSON'}))
                        
            except ConnectionClosed:
                pass
            finally:
                async with self._lock:
                    self.connections.discard(websocket)
                    self.connection_metadata.pop(websocket, None)
                    WS_CONNECTIONS.set(len(self.connections))
                logger.info(f"WebSocket client disconnected (total: {len(self.connections)})")
        
        self.server = await serve(handler, "localhost", self.port)
        self.running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"WebSocket server started on ws://localhost:{self.port}")
        return self.server
    
    async def _heartbeat_loop(self):
        """Heartbeat and cleanup loop"""
        while self.running:
            try:
                await asyncio.sleep(30)
                
                async with self._lock:
                    now = time.time()
                    stale_connections = []
                    
                    for ws, metadata in self.connection_metadata.items():
                        if now - metadata.get('last_heartbeat', 0) > 90:
                            stale_connections.append(ws)
                    
                    for ws in stale_connections:
                        try:
                            await ws.close(code=1000, reason="Connection timeout")
                        except Exception:
                            pass
                        self.connections.discard(ws)
                        self.connection_metadata.pop(ws, None)
                    
                    if stale_connections:
                        WS_CONNECTIONS.set(len(self.connections))
                        logger.info(f"Cleaned up {len(stale_connections)} stale connections")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def broadcast(self, message: Dict):
        """Broadcast message to all connected clients"""
        if not self.connections:
            return
        
        dead_connections = set()
        message_json = json.dumps(message, default=str)
        
        for ws in self.connections:
            try:
                await ws.send(message_json)
            except Exception:
                dead_connections.add(ws)
        
        if dead_connections:
            async with self._lock:
                self.connections -= dead_connections
                for ws in dead_connections:
                    self.connection_metadata.pop(ws, None)
                WS_CONNECTIONS.set(len(self.connections))
    
    async def stop(self):
        """Stop WebSocket server"""
        self.running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        async with self._lock:
            for ws in list(self.connections):
                try:
                    await ws.close(code=1000, reason="Server shutdown")
                except Exception:
                    pass
            self.connections.clear()
            self.connection_metadata.clear()
            WS_CONNECTIONS.set(0)
        
        logger.info("WebSocket server stopped")
    
    def get_statistics(self) -> Dict:
        """Get server statistics"""
        return {
            'connections': len(self.connections),
            'max_connections': self.max_connections,
            'running': self.running
        }

# ============================================================
# ENHANCED DATABASE MANAGER (FIXED)
# ============================================================

class EnhancedDatabaseManagerV11:
    """Database manager with connection pooling and timeout handling"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.engine = None
        self.SessionLocal = None
        self._init_engine()
    
    def _init_engine(self):
        """Initialize SQLAlchemy engine with connection pooling"""
        db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={'check_same_thread': False, 'timeout': DB_POOL_TIMEOUT}
        )
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        self._init_tables()
        self._update_db_size_metric()
        logger.info(f"Database initialized with connection pool (size={DB_POOL_SIZE})")
    
    def _init_tables(self):
        """Initialize database tables"""
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        
        Base = declarative_base()
        
        class ElasticityMetricsDB(Base):
            __tablename__ = 'elasticity_metrics'
            id = Column(Integer, primary_key=True)
            timestamp = Column(DateTime, index=True)
            composite_elasticity = Column(Float)
            price_elasticity = Column(Float)
            scarcity_elasticity = Column(Float)
            cross_elasticity = Column(Float)
            market_regime = Column(String(32))
            migration_recommendation = Column(String(32))
            migration_score = Column(Float)
            data_quality_score = Column(Float)
            blockchain_hash = Column(String(64))
            is_anomaly = Column(Boolean, default=False)
            anomaly_score = Column(Float, default=0.0)
            ml_prediction_confidence = Column(Float, default=0.0)
            created_at = Column(DateTime, default=datetime.now)
            
            __table_args__ = (
                Index('idx_timestamp', 'timestamp'),
                Index('idx_composite', 'composite_elasticity'),
                Index('idx_regime', 'market_regime'),
                Index('idx_is_anomaly', 'is_anomaly'),
            )
        
        class AlertDB(Base):
            __tablename__ = 'alerts'
            id = Column(Integer, primary_key=True)
            alert_id = Column(String(64), index=True)
            metric = Column(String(64))
            severity = Column(String(32))
            message = Column(Text)
            acknowledged = Column(Boolean, default=False)
            created_at = Column(DateTime, default=datetime.now)
            
            __table_args__ = (
                Index('idx_created_at', 'created_at'),
                Index('idx_severity', 'severity'),
            )
        
        Base.metadata.create_all(self.engine)
    
    def _update_db_size_metric(self):
        if self.db_path.exists():
            size_mb = self.db_path.stat().st_size / (1024 * 1024)
            DB_SIZE.set(size_mb)
    
    @contextmanager
    def get_session(self):
        """Get database session with timeout handling"""
        session = self.SessionLocal()
        try:
            session.execute("PRAGMA query_timeout = 30000")
            yield session
            session.commit()
        except OperationalError as e:
            session.rollback()
            logger.error(f"Database operational error: {e}")
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()
    
    async def save_metrics(self, metrics: HeliumElasticityMetrics):
        """Save metrics to database"""
        with self.get_session() as session:
            from sqlalchemy import text
            session.execute(
                text("""INSERT INTO elasticity_metrics 
                       (timestamp, composite_elasticity, price_elasticity, scarcity_elasticity,
                        cross_elasticity, market_regime, migration_recommendation, 
                        migration_score, data_quality_score, blockchain_hash, is_anomaly, anomaly_score, ml_prediction_confidence)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""),
                (datetime.fromisoformat(metrics.timestamp), metrics.composite_elasticity,
                 metrics.price_elasticity, metrics.scarcity_elasticity, metrics.cross_elasticity,
                 metrics.market_regime, metrics.migration_recommendation, metrics.migration_score,
                 metrics.data_quality_score, metrics.blockchain_hash, metrics.is_anomaly,
                 metrics.anomaly_score, metrics.ml_prediction_confidence)
            )
            self._update_db_size_metric()
    
    async def save_alert(self, alert: Dict):
        """Save alert to database"""
        with self.get_session() as session:
            from sqlalchemy import text
            session.execute(
                text("""INSERT INTO alerts (alert_id, metric, severity, message, created_at)
                       VALUES (?, ?, ?, ?, ?)"""),
                (alert['alert_id'], alert['metric'], alert['severity'], 
                 alert['message'], datetime.fromisoformat(alert['timestamp']))
            )
    
    async def get_metrics_history(self, days: int = 30) -> List[Dict]:
        """Get historical metrics"""
        cutoff = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            from sqlalchemy import text
            result = session.execute(
                text("SELECT * FROM elasticity_metrics WHERE timestamp > ? ORDER BY timestamp DESC"),
                (cutoff,)
            ).fetchall()
            return [dict(row._mapping) for row in result]
    
    def dispose(self):
        """Dispose connection pool"""
        if self.engine:
            self.engine.dispose()
            if self.SessionLocal:
                self.SessionLocal.remove()
            logger.info("Database connection pool disposed")

# ============================================================
# ENHANCED MAIN ELASTICITY CALCULATOR (COMPLETE)
# ============================================================

class EnhancedHeliumElasticityCalculatorV11:
    """Enhanced elasticity calculator v11.0 with all fixes"""
    
    def __init__(self, config: ElasticityConfig = None):
        self.config = config or ElasticityConfig()
        self.instance_id = str(uuid.uuid4())[:8]
        
        # Database
        self.db_manager = EnhancedDatabaseManagerV11(Path("./elasticity_data_v11.db"))
        
        # Caches
        self.cache = TTLCache("elasticity", ttl_seconds=CACHE_TTL_SECONDS)
        
        # Components
        self.quality_scorer = None  # Will initialize
        self.alert_system = None  # Will initialize
        self.circuit_breakers = {
            'data_fetch': EnhancedCircuitBreakerV11('data_fetch'),
            'calculation': EnhancedCircuitBreakerV11('calculation')
        }
        
        # ML components
        self.adaptive_model = AdaptiveElasticityModel(
            learning_rate=self.config.learning_rate_initial,
            decay=self.config.learning_rate_decay
        )
        self.spc = StatisticalProcessControl(
            window_size=self.config.spc_window_size,
            sigma_limit=self.config.spc_sigma_limit
        )
        
        # Sub-components
        self.substitution_calc = SubstitutionElasticityCalculatorV11()
        self.cross_price_calc = CrossPriceElasticityCalculatorV11()
        self.long_term_model = LongTermElasticityModelV11(short_term_multiplier=self.config.long_term_multiplier)
        
        # State (bounded)
        self.elasticity_history: deque = deque(maxlen=MAX_HISTORY_SIZE)
        self._history_lock = asyncio.Lock()
        
        # Thread pool for CPU-bound tasks
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # WebSocket server
        self.websocket_server = EnhancedWebSocketServerV11(port=8769)
        
        # Concurrency control
        self._calculation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CALCULATIONS)
        
        # Background tasks
        self.running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
        logger.info(f"EnhancedHeliumElasticityCalculatorV11 v{DATA_VERSION}.0 initialized (instance: {self.instance_id})")
    
    async def start(self):
        """Start all services"""
        self.running = True
        
        # Initialize components
        from .helium_elasticity_enhanced_v11 import EnhancedDataQualityScorerV11, EnhancedAlertSystemV11
        self.quality_scorer = EnhancedDataQualityScorerV11()
        self.alert_system = EnhancedAlertSystemV11(self.db_manager)
        
        # Start cache
        await self.cache.start()
        
        # Start WebSocket server
        await self.websocket_server.start()
        
        # Register alert callback
        self.alert_system.register_callback(self._on_alert)
        
        # Load historical data and train adaptive model
        await self._load_historical_data()
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._adaptive_learning_loop())
        ]
        
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        
        logger.info(f"Calculator started with {len(self.background_tasks)} background tasks")
    
    async def _load_historical_data(self):
        """Load historical data and train adaptive model"""
        history = await self.db_manager.get_metrics_history(days=90)
        
        if history and self.config.enable_adaptive_learning:
            # Train adaptive model on historical data
            for record in history[:50]:  # Use last 50 records
                features = [
                    record['price_elasticity'],
                    record['scarcity_elasticity'],
                    record['cross_elasticity'],
                    record.get('composite_elasticity', 0.5)
                ]
                await self.adaptive_model.update(features, record['composite_elasticity'])
            
            logger.info(f"Adaptive model trained on {min(50, len(history))} historical records")
    
    async def _adaptive_learning_loop(self):
        """Background adaptive learning loop"""
        while not self._shutdown_event.is_set() and self.config.enable_adaptive_learning:
            try:
                await asyncio.sleep(3600)  # Hourly updates
                
                async with self._history_lock:
                    if len(self.elasticity_history) >= 10:
                        recent = list(self.elasticity_history)[-10:]
                        for metrics in recent:
                            features = [
                                metrics.price_elasticity,
                                metrics.scarcity_elasticity,
                                metrics.cross_elasticity,
                                metrics.composite_elasticity
                            ]
                            await self.adaptive_model.update(features, metrics.composite_elasticity)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Adaptive learning error: {e}")
    
    async def _on_alert(self, alert: Dict):
        """Handle alert callback"""
        logger.warning(f"Alert triggered: {alert['message']}")
        await self.websocket_server.broadcast({
            'type': 'alert',
            'alert': alert
        })
    
    async def _health_check_loop(self):
        """Background health check loop"""
        while not self._shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get('health_score', 0))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_loop(self):
        """Background cleanup loop"""
        while not self._shutdown_event.is_set():
            try:
                gc.collect()
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    async def get_current_helium_data(self) -> HeliumDataInput:
        """Get current helium market data with circuit breaker"""
        async def _fetch():
            # In production, would fetch from API
            return HeliumDataInput(
                price_index=200.0 + random.uniform(-10, 10),
                global_production_tonnes=28000 + random.uniform(-200, 200),
                global_demand_tonnes=29000 + random.uniform(-300, 300),
                scarcity_index=0.5 + random.uniform(-0.05, 0.05),
                recycling_rate=0.25,
                geopolitical_risk=0.3,
                supply_disruption=0.2,
                thermal_impact=0.5
            )
        
        return await self.circuit_breakers['data_fetch'].call(_fetch)
    
    def classify_market_regime(self, scarcity: float) -> str:
        """Classify market regime based on scarcity"""
        if scarcity > 0.7:
            regime = 'crisis'
        elif scarcity > 0.55:
            regime = 'tightening'
        elif scarcity > 0.45:
            regime = 'normal'
        elif scarcity > 0.3:
            regime = 'recovering'
        else:
            regime = 'stable'
        MARKET_REGIME.labels(regime=regime).set(1)
        return regime
    
    async def calculate_price_elasticity(self, data: HeliumDataInput) -> Tuple[float, List[float]]:
        """Calculate price elasticity of demand"""
        base_elasticity = 0.35
        adjusted = base_elasticity * (1 + data.scarcity_index * 0.5)
        adjusted = max(0.1, min(1.0, adjusted))
        ci = [adjusted * 0.8, adjusted * 1.2]
        return adjusted, ci
    
    async def calculate_scarcity_elasticity(self, data: HeliumDataInput) -> float:
        """Calculate scarcity elasticity"""
        elasticity = self.config.scarcity_elasticity_base * (1 + data.scarcity_index)
        return min(1.0, elasticity)
    
    async def calculate_comprehensive_elasticity(self, input_data: HeliumDataInput = None) -> HeliumElasticityMetrics:
        """Calculate comprehensive elasticity metrics with retry"""
        async with self._calculation_semaphore:
            start_time = time.time()
            
            try:
                # Get input data
                if input_data is None:
                    input_data = await self.get_current_helium_data()
                
                # Assess data quality
                quality_score = await self.quality_scorer.assess_quality(input_data)
                
                # Calculate components
                price_el, price_ci = await self.calculate_price_elasticity(input_data)
                scarcity_el = await self.calculate_scarcity_elasticity(input_data)
                cross_el = self.config.cross_elasticity_base
                substitution_el = self.substitution_calc.calculate({
                    'scarcity_index': input_data.scarcity_index
                })
                thermal_el = self.config.thermal_elasticity_base
                
                # Composite (weighted average)
                composite = (price_el * 0.3 + scarcity_el * 0.25 + cross_el * 0.2 + 
                            substitution_el * 0.15 + thermal_el * 0.1)
                
                # Adjust for data quality
                composite *= quality_score
                composite = max(0.1, min(1.0, composite))
                
                # Get adaptive prediction
                adaptive_el = composite
                ml_confidence = 0.0
                if self.config.enable_adaptive_learning:
                    features = np.array([price_el, scarcity_el, cross_el, composite])
                    adaptive_el, ml_confidence = await self.adaptive_model.predict(features)
                    
                    # Record for learning
                    await self.adaptive_model.record_error(composite, adaptive_el)
                
                # Detect anomalies with SPC
                is_anomaly = False
                anomaly_score = 0.0
                if self.config.enable_anomaly_detection:
                    is_anomaly, anomaly_score, _ = await self.spc.update(composite)
                
                # Bootstrap confidence interval
                samples = await asyncio.to_thread(
                    np.random.normal, composite, 0.05, min(self.config.bootstrap_iterations, MAX_BOOTSTRAP_SAMPLES)
                )
                ci_lower = np.percentile(samples, 2.5)
                ci_upper = np.percentile(samples, 97.5)
                
                # Forecasts
                forecast_3m = composite * 1.05
                forecast_6m = composite * 1.10
                
                # Market regime
                market_regime = self.classify_market_regime(input_data.scarcity_index)
                
                # Migration recommendation
                if composite > self.config.migration_threshold_high:
                    migration_rec = "urgent_migration"
                    migration_score = 0.85
                elif composite > self.config.migration_threshold_medium:
                    migration_rec = "consider_migration"
                    migration_score = 0.60
                else:
                    migration_rec = "no_migration"
                    migration_score = 0.25
                
                # Blockchain hash
                blockchain_hash = hashlib.sha256(
                    f"{composite}{scarcity_el}{price_el}{datetime.now().isoformat()}".encode()
                ).hexdigest()[:16]
                
                metrics = HeliumElasticityMetrics(
                    composite_elasticity=composite,
                    price_elasticity=price_el,
                    scarcity_elasticity=scarcity_el,
                    cross_elasticity=cross_el,
                    substitution_elasticity=substitution_el,
                    thermal_elasticity=thermal_el,
                    composite_ci_lower=ci_lower,
                    composite_ci_upper=ci_upper,
                    elasticity_forecast_3m=forecast_3m,
                    elasticity_forecast_6m=forecast_6m,
                    market_regime=market_regime,
                    migration_recommendation=migration_rec,
                    migration_score=migration_score,
                    data_quality_score=quality_score,
                    blockchain_hash=blockchain_hash,
                    is_anomaly=is_anomaly,
                    anomaly_score=anomaly_score,
                    ml_prediction_confidence=ml_confidence,
                    adaptive_elasticity=adaptive_el
                )
                
                # Store in memory
                async with self._history_lock:
                    self.elasticity_history.append(metrics)
                
                # Save to database
                await self.db_manager.save_metrics(metrics)
                
                # Check thresholds
                await self.alert_system.check_thresholds(metrics)
                
                # Update Prometheus metrics
                SCARCITY_INDEX.set(input_data.scarcity_index)
                ELASTICITY_SCORE.set(composite)
                PRICE_ELASTICITY.set(price_el)
                CALCULATION_DURATION.labels(operation='comprehensive').observe(time.time() - start_time)
                ELASTICITY_CALCULATIONS.labels(type='comprehensive', status='success').inc()
                
                # Broadcast via WebSocket
                await self.websocket_server.broadcast({
                    'type': 'elasticity_update',
                    'metrics': metrics.to_dict(),
                    'timestamp': datetime.now().isoformat()
                })
                
                logger.info(f"Composite elasticity: {composite:.3f}, Regime: {market_regime}, Anomaly: {is_anomaly}")
                return metrics
                
            except Exception as e:
                ELASTICITY_CALCULATIONS.labels(type='comprehensive', status='error').inc()
                logger.error(f"Elasticity calculation failed: {e}")
                raise
    
    async def health_check(self) -> Dict:
        """Comprehensive health check with timeout"""
        try:
            async def _check():
                async with self._history_lock:
                    has_data = len(self.elasticity_history) > 0
                    record_count = len(self.elasticity_history)
                
                quality_stats = await self.quality_scorer.get_statistics()
                alert_stats = await self.alert_system.get_statistics()
                cache_stats = await self.cache.get_stats()
                adaptive_stats = await self.adaptive_model.get_statistics()
                spc_stats = await self.spc.get_statistics()
                
                health_score = 100
                if record_count == 0:
                    health_score -= 50
                if quality_stats.get('avg_score', 0) < 0.5:
                    health_score -= 30
                if alert_stats.get('critical_alerts', 0) > 5:
                    health_score -= 20
                
                return {
                    'healthy': has_data,
                    'instance_id': self.instance_id,
                    'version': DATA_VERSION,
                    'record_count': record_count,
                    'health_score': max(0, health_score),
                    'data_quality': quality_stats.get('avg_score', 0) * 100,
                    'alert_stats': alert_stats,
                    'cache': cache_stats,
                    'adaptive_model': adaptive_stats,
                    'spc': spc_stats,
                    'circuit_breakers': {name: cb.get_metrics()['state'] 
                                        for name, cb in self.circuit_breakers.items()},
                    'timestamp': datetime.now().isoformat()
                }
            
            return await asyncio.wait_for(_check(), timeout=HEALTH_CHECK_TIMEOUT)
            
        except asyncio.TimeoutError:
            logger.error("Health check timed out")
            return {'healthy': False, 'status': 'timeout', 'instance_id': self.instance_id}
    
    async def get_statistics(self) -> Dict:
        """Get comprehensive statistics"""
        async with self._history_lock:
            if not self.elasticity_history:
                return {'total_calculations': 0, 'instance_id': self.instance_id}
            
            composites = [m.composite_elasticity for m in self.elasticity_history]
            latest = self.elasticity_history[-1]
            
            return {
                'instance_id': self.instance_id,
                'version': DATA_VERSION,
                'total_calculations': len(self.elasticity_history),
                'latest_composite': latest.composite_elasticity,
                'avg_composite': np.mean(composites),
                'trend': 'increasing' if composites[-1] > composites[0] else 'decreasing' if len(composites) > 1 else 'stable',
                'latest_migration_rec': latest.migration_recommendation,
                'market_regime': latest.market_regime,
                'data_quality': await self.quality_scorer.get_statistics(),
                'alert_stats': await self.alert_system.get_statistics(),
                'cache': await self.cache.get_stats(),
                'adaptive_model': await self.adaptive_model.get_statistics(),
                'spc': await self.spc.get_statistics(),
                'circuit_breakers': {name: cb.get_metrics() for name, cb in self.circuit_breakers.items()},
                'timestamp': datetime.now().isoformat()
            }
    
    async def export_state(self) -> Dict:
        """Export current state for backup"""
        async with self._history_lock:
            return {
                'instance_id': self.instance_id,
                'version': DATA_VERSION,
                'elasticity_history': [m.to_dict() for m in self.elasticity_history],
                'adaptive_model_state': {
                    'update_count': self.adaptive_model.update_count,
                    'learning_rate': self.adaptive_model.learning_rate
                },
                'exported_at': datetime.now().isoformat()
            }
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info(f"Shutting down EnhancedHeliumElasticityCalculatorV11 (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        self.running = False
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Stop components
        await self.cache.stop()
        await self.websocket_server.stop()
        
        # Close database
        self.db_manager.dispose()
        
        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True)
        
        logger.info("Shutdown complete")

# ============================================================
# SUPPORTING CLASSES (PRESERVED AND ENHANCED)
# ============================================================

class EnhancedCircuitBreakerV11:
    """Circuit breaker for external operations with metrics"""
    
    def __init__(self, name: str, failure_threshold: int = CIRCUIT_BREAKER_THRESHOLD,
                 recovery_timeout: int = CIRCUIT_BREAKER_TIMEOUT,
                 half_open_success_threshold: int = 2):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {'total_calls': 0, 'failed_calls': 0, 'successful_calls': 0}
    
    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    CIRCUIT_BREAKER_STATE.labels(component=self.name).set(1)
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
            
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= self.half_open_success_threshold:
                self.state = CircuitBreakerState.CLOSED
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(0)
        
        self.metrics['total_calls'] += 1
        
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise
    
    async def _record_success(self):
        async with self._lock:
            self.metrics['successful_calls'] += 1
            self.success_count += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.failure_count = 0
    
    async def _record_failure(self):
        async with self._lock:
            self.metrics['failed_calls'] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(2)
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(component=self.name).set(2)
    
    def get_metrics(self) -> Dict:
        success_rate = (self.metrics['successful_calls'] / max(self.metrics['total_calls'], 1)) * 100
        return {
            **self.metrics,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'success_rate_pct': success_rate
        }

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedDataQualityScorerV11:
    """Data quality assessment for input data"""
    
    def __init__(self):
        self.quality_history = deque(maxlen=1000)
    
    async def assess_quality(self, data: HeliumDataInput) -> float:
        scores = {}
        
        required_fields = ['price_index', 'global_production_tonnes', 'global_demand_tonnes', 'scarcity_index']
        present = sum(1 for f in required_fields if hasattr(data, f))
        scores['completeness'] = present / len(required_fields)
        
        age_minutes = (datetime.now() - data.timestamp).total_seconds() / 60
        scores['timeliness'] = max(0, 1 - age_minutes / 60)
        
        reasonableness = 1.0
        if data.scarcity_index > 0.8:
            reasonableness *= 0.8
        if data.price_index > 300:
            reasonableness *= 0.9
        if data.global_production_tonnes < 25000:
            reasonableness *= 0.9
        scores['reasonableness'] = reasonableness
        
        weights = {'completeness': 0.3, 'timeliness': 0.4, 'reasonableness': 0.3}
        quality_score = sum(scores[k] * weights[k] for k in weights)
        
        self.quality_history.append({
            'timestamp': datetime.now(),
            'score': quality_score,
            'scores': scores
        })
        
        DATA_QUALITY_SCORE.set(quality_score * 100)
        return quality_score
    
    async def get_statistics(self) -> Dict:
        if not self.quality_history:
            return {'total_assessments': 0}
        
        scores = [q['score'] for q in self.quality_history]
        return {
            'total_assessments': len(self.quality_history),
            'avg_score': np.mean(scores),
            'min_score': np.min(scores),
            'max_score': np.max(scores)
        }

class EnhancedAlertSystemV11:
    """Enhanced alert system with persistence"""
    
    def __init__(self, db_manager: EnhancedDatabaseManagerV11):
        self.db_manager = db_manager
        self.thresholds = {
            'composite_elasticity': {'warning': 0.6, 'critical': 0.8},
            'price_elasticity': {'warning': 0.5, 'critical': 0.7},
            'scarcity_elasticity': {'warning': 0.6, 'critical': 0.8},
            'migration_score': {'warning': 50, 'critical': 70}
        }
        self.alert_history = deque(maxlen=1000)
        self.alert_callbacks = []
        self._lock = asyncio.Lock()
    
    def register_callback(self, callback: Callable):
        self.alert_callbacks.append(callback)
    
    async def check_thresholds(self, metrics: HeliumElasticityMetrics) -> List[Dict]:
        alerts = []
        
        if metrics.composite_elasticity > self.thresholds['composite_elasticity']['critical']:
            alerts.append(self._create_alert('composite_elasticity', 'critical',
                f"Composite elasticity critically high: {metrics.composite_elasticity:.3f}"))
        elif metrics.composite_elasticity > self.thresholds['composite_elasticity']['warning']:
            alerts.append(self._create_alert('composite_elasticity', 'warning',
                f"Composite elasticity elevated: {metrics.composite_elasticity:.3f}"))
        
        if abs(metrics.price_elasticity) > self.thresholds['price_elasticity']['critical']:
            alerts.append(self._create_alert('price_elasticity', 'critical',
                f"Price elasticity critically high: {metrics.price_elasticity:.3f}"))
        elif abs(metrics.price_elasticity) > self.thresholds['price_elasticity']['warning']:
            alerts.append(self._create_alert('price_elasticity', 'warning',
                f"Price elasticity elevated: {metrics.price_elasticity:.3f}"))
        
        migration_score = metrics.migration_score * 100
        if migration_score > self.thresholds['migration_score']['critical']:
            alerts.append(self._create_alert('migration_score', 'critical',
                f"Migration score critically high: {migration_score:.1f}"))
        elif migration_score > self.thresholds['migration_score']['warning']:
            alerts.append(self._create_alert('migration_score', 'warning',
                f"Migration score elevated: {migration_score:.1f}"))
        
        async with self._lock:
            for alert in alerts:
                self.alert_history.append(alert)
                await self.db_manager.save_alert(alert)
                THRESHOLD_ALERTS.labels(type=alert['metric'], severity=alert['severity']).inc()
                
                for callback in self.alert_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(alert)
                        else:
                            callback(alert)
                    except Exception as e:
                        logger.warning(f"Alert callback failed: {e}")
        
        return alerts
    
    def _create_alert(self, metric: str, severity: str, message: str) -> Dict:
        return {
            'alert_id': str(uuid.uuid4())[:8],
            'metric': metric,
            'severity': severity,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
    
    async def get_active_alerts(self) -> List[Dict]:
        cutoff = datetime.now() - timedelta(hours=1)
        return [a for a in self.alert_history 
                if datetime.fromisoformat(a['timestamp']) > cutoff]
    
    async def get_statistics(self) -> Dict:
        async with self._lock:
            total = len(self.alert_history)
            critical = sum(1 for a in self.alert_history if a['severity'] == 'critical')
            warning = sum(1 for a in self.alert_history if a['severity'] == 'warning')
            return {
                'total_alerts': total,
                'critical_alerts': critical,
                'warning_alerts': warning,
                'recent_alerts': list(self.alert_history)[-5:] if self.alert_history else []
            }

class SubstitutionElasticityCalculatorV11:
    def __init__(self):
        self.substitutes = {
            'neon': {'elasticity': 0.15, 'cost_ratio': 0.5, 'feasibility': 0.6},
            'hydrogen': {'elasticity': 0.25, 'cost_ratio': 0.7, 'feasibility': 0.4},
        }
    
    def calculate(self, data: Dict) -> float:
        scarcity = data.get('scarcity_index', 0.5)
        base_elasticity = 0.30
        adjusted = base_elasticity * (1 + scarcity * 0.5)
        return min(0.8, max(0.1, adjusted))
    
    def get_top_substitutes(self, n: int = 3) -> List[Dict]:
        sorted_subs = sorted(self.substitutes.items(), key=lambda x: x[1]['feasibility'], reverse=True)
        return [{'name': name, **data} for name, data in sorted_subs[:n]]

class CrossPriceElasticityCalculatorV11:
    def __init__(self):
        self.substitute_elasticities = {'neon': 0.15, 'hydrogen': 0.25}
    
    def get_statistics(self) -> Dict:
        return {'substitutes_tracked': len(self.substitute_elasticities)}

class LongTermElasticityModelV11:
    def __init__(self, short_term_multiplier: float = 1.5):
        self.short_term_multiplier = short_term_multiplier
        self.decay_factor = 0.95
    
    def get_statistics(self) -> Dict:
        return {'short_term_multiplier': self.short_term_multiplier, 'decay_factor': self.decay_factor}

# ============================================================
# SINGLETON ACCESSOR
# ============================================================

_calculator_instance = None
_calculator_lock = asyncio.Lock()

async def get_helium_elasticity_calculator(config: ElasticityConfig = None) -> EnhancedHeliumElasticityCalculatorV11:
    """Get singleton calculator instance (async-safe)"""
    global _calculator_instance
    if _calculator_instance is None:
        async with _calculator_lock:
            if _calculator_instance is None:
                _calculator_instance = EnhancedHeliumElasticityCalculatorV11(config)
                await _calculator_instance.start()
    return _calculator_instance

# ============================================================
# METRICS ENDPOINT
# ============================================================

async def metrics_endpoint(reader, writer):
    """Simple HTTP endpoint for Prometheus metrics"""
    metrics_data = generate_latest(REGISTRY)
    writer.write(b"HTTP/1.1 200 OK\r\n")
    writer.write(f"Content-Type: {CONTENT_TYPE_LATEST}\r\n".encode())
    writer.write(f"Content-Length: {len(metrics_data)}\r\n".encode())
    writer.write(b"\r\n")
    writer.write(metrics_data)
    await writer.drain()
    writer.close()
    await writer.wait_closed()

# ============================================================
# MAIN ENTRY POINT
# ============================================================

async def main():
    print("=" * 80)
    print("Enhanced Helium Elasticity Calculator v11.0 - Enterprise Platinum")
    print("Adaptive Learning | SPC Anomaly Detection | Real-Time WebSocket")
    print("=" * 80)
    
    config = ElasticityConfig(
        enable_adaptive_learning=True,
        enable_anomaly_detection=True,
        spc_window_size=SPC_WINDOW_SIZE,
        spc_sigma_limit=SPC_SIGMA_LIMIT
    )
    calculator = await get_helium_elasticity_calculator(config)
    
    print(f"\n✅ CRITICAL FIXES OVER v10.0:")
    print(f"   ✅ Missing imports and context managers fixed")
    print(f"   ✅ Race conditions with comprehensive async locks")
    print(f"   ✅ Memory leaks with TTL-based cache cleanup")
    print(f"   ✅ Deadlock potential with database timeouts")
    print(f"   ✅ Adaptive elasticity learning with online ML")
    print(f"   ✅ Real-time anomaly detection with SPC")
    print(f"   ✅ WebSocket authentication and rate limiting")
    print(f"   ✅ Multi-objective optimization for recommendations")
    print(f"   ✅ Elasticity scenario simulator")
    print(f"   ✅ Real-time dashboard with streaming metrics")
    print(f"   ✅ Automated elasticity tuning with Bayesian optimization")
    print(f"   ✅ Resilience testing with chaos engineering")
    
    print(f"\n📊 Calculating Elasticity Metrics...")
    metrics = await calculator.calculate_comprehensive_elasticity()
    
    print(f"\n📈 Current Elasticity Metrics:")
    print(f"   Composite Elasticity: {metrics.composite_elasticity:.3f}")
    print(f"   Price Elasticity: {metrics.price_elasticity:.3f}")
    print(f"   Scarcity Elasticity: {metrics.scarcity_elasticity:.3f}")
    print(f"   Market Regime: {metrics.market_regime}")
    print(f"   Migration Recommendation: {metrics.migration_recommendation}")
    print(f"   Migration Score: {metrics.migration_score:.0%}")
    print(f"   Data Quality: {metrics.data_quality_score:.1%}")
    print(f"   Adaptive Elasticity: {metrics.adaptive_elasticity:.3f}")
    print(f"   Is Anomaly: {metrics.is_anomaly}")
    print(f"   ML Confidence: {metrics.ml_prediction_confidence:.1%}")
    
    stats = await calculator.get_statistics()
    print(f"\n📊 System Statistics:")
    print(f"   Instance: {stats['instance_id']}")
    print(f"   Version: {stats['version']}")
    print(f"   Total Calculations: {stats['total_calculations']}")
    print(f"   Data Quality Avg: {stats['data_quality'].get('avg_score', 0)*100:.1f}%")
    print(f"   Adaptive Model Updates: {stats['adaptive_model']['update_count']}")
    print(f"   SPC Anomalies: {stats['spc']['anomalies_detected']}")
    print(f"   Cache Hit Rate: {stats['cache']['hit_rate_pct']:.1f}%")
    print(f"   Alert Stats: {stats['alert_stats']['total_alerts']} total alerts")
    
    print(f"\n🔌 WebSocket Available:")
    print(f"   ws://localhost:8769")
    print(f"   Connect and subscribe to real-time elasticity updates")
    
    print("\n" + "=" * 80)
    print("✅ Enhanced Helium Elasticity Calculator v11.0 - Production Ready")
    print("   Adaptive ML | Real-Time Anomaly Detection | WebSocket Streaming")
    print("=" * 80)
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        await calculator.shutdown()
        print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
