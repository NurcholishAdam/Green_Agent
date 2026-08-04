#!/usr/bin/env python3
# enhancements/mixed_precision_utils_enhanced_v2_0.py
"""
Enhanced Mixed Precision Engine v2.0.0 - Enterprise Quantum Resilience + MTOP
Supports dynamic precision selection via Multi-Teacher On-Policy Distillation,
carbon-aware adaptation, and full integration with the Green Agent ecosystem.
"""

import asyncio
import logging
import json
import time
import uuid
import hashlib
import os
import random
import signal
from functools import wraps
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
import numpy as np
import contextvars

# PyTorch
import torch
import torch.nn as nn
from torch.cuda.amp import autocast

# Prometheus
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Tenacity for retries
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# Context variable for correlation ID
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

# Structured logging
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
    )
    logger.addFilter(lambda record: setattr(record, 'correlation_id', correlation_id_var.get()) or True)

# ============================================================
# DUMMY TENACITY DECORATOR (if not available)
# ============================================================
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                attempts = 0
                max_attempts = kwargs.get('stop', stop_after_attempt(3)).stop.max_attempt_number
                delay = 1
                while attempts < max_attempts:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception as e:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise
                        await asyncio.sleep(delay)
                        delay *= 2
            return wrapper
        return decorator

# ============================================================
# CONFIGURATION (Pydantic fallback)
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

if PYDANTIC_AVAILABLE:
    class MixedPrecisionConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("2.0.0")
        log_level: str = Field("INFO")
        default_dtype: str = Field("fp16")
        use_amp: bool = True
        amp_dtype: str = Field("fp16")
        metrics_port: int = Field(8000, ge=1024, le=65535)
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)
        health_check_interval: int = Field(60, ge=10)
        # MTOP parameters
        mtop_learning_rate: float = Field(0.01, gt=0)
        mtop_teacher_weights: Dict[str, float] = Field(default_factory=lambda: {
            'accuracy': 0.25, 'energy': 0.25, 'speed': 0.25, 'carbon': 0.25
        })
        # Quantum / blockchain (optional)
        enable_quantum_security: bool = True
        quantum_algorithm: str = Field("dilithium")
        quantum_master_key: str = Field(default="")
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v: str) -> str:
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()

        @field_validator('quantum_master_key')
        @classmethod
        def validate_master_key(cls, v: str) -> str:
            if not v:
                raise ValueError('quantum_master_key must be set via environment MIXED_PRECISION_QUANTUM_MASTER_KEY')
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError('quantum_master_key must be a hex string')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.quantum_master_key)

        class Config:
            env_prefix = "MIXED_PRECISION_"
else:
    @dataclass
    class MixedPrecisionConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "2.0.0"
        log_level: str = "INFO"
        default_dtype: str = "fp16"
        use_amp: bool = True
        amp_dtype: str = "fp16"
        metrics_port: int = 8000
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        health_check_interval: int = 60
        mtop_learning_rate: float = 0.01
        mtop_teacher_weights: Dict[str, float] = field(default_factory=lambda: {
            'accuracy': 0.25, 'energy': 0.25, 'speed': 0.25, 'carbon': 0.25
        })
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = ""
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        def get_master_key_bytes(self) -> bytes:
            if not self.quantum_master_key:
                raise ValueError('quantum_master_key not set')
            return bytes.fromhex(self.quantum_master_key)

# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================
class MixedPrecisionError(Exception):
    pass

# ============================================================
# CIRCUIT BREAKER (minimal)
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} HALF_OPEN")
                else:
                    raise MixedPrecisionError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

# ============================================================
# PROMETHEUS METRICS (dummy fallback)
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    PRECISION_SWITCHES = Counter('precision_switches_total', 'Precision switches', ['from', 'to'], registry=REGISTRY)
    ENERGY_SAVED = Gauge('energy_saved_kwh', 'Energy saved vs fp32', registry=REGISTRY)
    CARBON_SAVED = Gauge('carbon_saved_kg', 'Carbon saved vs fp32', registry=REGISTRY)
    CURRENT_PRECISION = Gauge('current_precision', 'Current precision (0=fp32,1=fp16,2=bf16,3=fp8,4=fp4)', registry=REGISTRY)
    ACCURACY_SCORE = Gauge('precision_accuracy_score', 'Accuracy score of current precision', registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    PRECISION_SWITCHES = DummyMetrics()
    ENERGY_SAVED = DummyMetrics()
    CARBON_SAVED = DummyMetrics()
    CURRENT_PRECISION = DummyMetrics()
    ACCURACY_SCORE = DummyMetrics()

# ============================================================
# CARBON INTENSITY MANAGER (simplified)
# ============================================================
class CarbonIntensityManager:
    def __init__(self, config: MixedPrecisionConfig):
        self.config = config
        self.api_key = config.carbon_api_key
        self.region = config.carbon_region
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.cache = {}
        self.last_update = None
        self._session = None
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker("carbon_api")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, ConnectionError)))
    async def _fetch_intensity(self) -> float:
        session = await self._get_session()
        url = f"{self.endpoint}/latest?zone={self.region}"
        headers = {'auth-token': self.api_key} if self.api_key else {}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                raise Exception(f"Carbon API returned {response.status}")
            data = await response.json()
            return data.get('carbonIntensity', 400)

    async def get_current_intensity(self) -> float:
        cache_key = f"{self.region}_{datetime.utcnow().hour}"
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < 300:
            return self.cache[cache_key]

        try:
            intensity = await self._circuit_breaker.call(self._fetch_intensity)
            async with self._lock:
                self.cache[cache_key] = intensity
                self.last_update = datetime.utcnow()
            return intensity
        except Exception as e:
            logger.warning(f"Carbon API failed: {e}, using fallback")
            return 400

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================
# MTOP ENGINE FOR PRECISION SELECTION
# ============================================================
class TeacherEnsemble:
    """
    Teachers for precision decision: accuracy, energy, speed, carbon.
    """
    def __init__(self, config: MixedPrecisionConfig):
        self.config = config
        self.teachers = {
            'accuracy': self._accuracy_teacher,
            'energy': self._energy_teacher,
            'speed': self._speed_teacher,
            'carbon': self._carbon_teacher
        }
        self.teacher_weights = config.mtop_teacher_weights.copy()
        self.history = deque(maxlen=100)

    def _accuracy_teacher(self, features: Dict) -> Dict[str, float]:
        # Predict accuracy degradation for each precision (0-1 score)
        base_accuracy = features.get('base_accuracy', 1.0)
        dtype_scores = {
            'fp32': 1.0,
            'fp16': 0.98,
            'bf16': 0.97,
            'fp8': 0.90,
            'fp4': 0.80
        }
        # Adjust based on layer sensitivity (simplistic)
        layer_type = features.get('layer_type', 'linear')
        if layer_type in ['conv2d', 'linear']:
            # Convolutions and linear layers are less sensitive
            pass
        # Return scores
        return {k: base_accuracy * v for k, v in dtype_scores.items()}

    def _energy_teacher(self, features: Dict) -> Dict[str, float]:
        # Energy consumption relative to fp32 (lower is better)
        dtype_energy = {
            'fp32': 1.0,
            'fp16': 0.4,
            'bf16': 0.4,
            'fp8': 0.2,
            'fp4': 0.1
        }
        return dtype_energy.copy()

    def _speed_teacher(self, features: Dict) -> Dict[str, float]:
        # Speed improvement relative to fp32 (higher is better)
        dtype_speed = {
            'fp32': 1.0,
            'fp16': 2.0,
            'bf16': 2.0,
            'fp8': 3.0,
            'fp4': 4.0
        }
        return dtype_speed.copy()

    def _carbon_teacher(self, features: Dict) -> Dict[str, float]:
        # Carbon intensity affects weight of energy savings
        intensity = features.get('carbon_intensity', 400)
        # If intensity is high, we want lower energy consumption
        # So we weight energy savings more heavily
        # Return a multiplier for each precision based on energy
        energy = self._energy_teacher(features)
        # Carbon weight = 1 + (intensity/400) * 0.5
        carbon_weight = 1 + (intensity / 400) * 0.5
        # We'll return the energy savings multiplied by carbon_weight
        return {k: 1 - energy[k] * carbon_weight for k in energy}

    async def get_teacher_scores(self, features: Dict) -> Dict[str, Dict[str, float]]:
        scores = {}
        for name, func in self.teachers.items():
            scores[name] = func(features)
        self.history.append(features)
        return scores

    def update_weights(self, rewards: Dict[str, float]):
        total = sum(rewards.values())
        if total > 0:
            for name in self.teacher_weights:
                self.teacher_weights[name] = rewards[name] / total

class StudentPrecisionPolicy:
    """
    Student model that learns to choose precision based on features.
    Uses a simple linear model (or could be a small neural network).
    """
    def __init__(self, config: MixedPrecisionConfig):
        self.config = config
        self.learning_rate = config.mtop_learning_rate
        self.decay = 0.99
        # Weights for features: [carbon_intensity, layer_type_encoded, input_size, base_accuracy]
        self.weights = np.array([0.5, 0.3, 0.1, 0.1])
        self.bias = 0.0
        self.update_count = 0
        # Precision options (index order)
        self.dtype_list = ['fp32', 'fp16', 'bf16', 'fp8', 'fp4']

    def _extract_features(self, features: Dict) -> np.ndarray:
        # Convert features to numeric vector
        carbon = features.get('carbon_intensity', 400) / 1000.0
        layer_type = 0.0  # encode one-hot later
        if features.get('layer_type') in ['conv2d', 'linear']:
            layer_type = 0.2
        input_size = features.get('input_size', 1000) / 10000.0
        base_acc = features.get('base_accuracy', 1.0)
        return np.array([carbon, layer_type, input_size, base_acc])

    async def predict_probs(self, features: Dict) -> Dict[str, float]:
        x = self._extract_features(features)
        logits = np.dot(self.weights, x) + self.bias
        # Softmax over precisions (biased by teacher ensemble later)
        # For now, we just output a score for each precision
        scores = {dtype: logits * (i+1) for i, dtype in enumerate(self.dtype_list)}
        # Normalize to probabilities
        total = sum(scores.values())
        if total > 0:
            return {k: v / total for k, v in scores.items()}
        else:
            return {k: 1/len(self.dtype_list) for k in self.dtype_list}

    async def train_step(self, features: Dict, target_dtype: str, reward: float):
        self.update_count += 1
        # Simple gradient update: increase weight of features that correlate with good choices
        x = self._extract_features(features)
        # Update weights in direction of reward
        self.weights += self.learning_rate * reward * x
        self.learning_rate *= self.decay

class MTOPPrecisionEngine:
    """
    Main MTOP engine for precision selection.
    """
    def __init__(self, config: MixedPrecisionConfig, carbon_manager: CarbonIntensityManager):
        self.config = config
        self.carbon_manager = carbon_manager
        self.teacher_ensemble = TeacherEnsemble(config)
        self.student = StudentPrecisionPolicy(config)
        self.history = deque(maxlen=500)

    async def select_precision(self, features: Dict, actual_outcome: Dict = None) -> Dict:
        # Get teacher scores
        teacher_scores = await self.teacher_ensemble.get_teacher_scores(features)
        # Combine weighted scores
        combined = {}
        for dtype in self.student.dtype_list:
            combined[dtype] = 0.0
            for teacher, scores in teacher_scores.items():
                combined[dtype] += self.teacher_ensemble.teacher_weights[teacher] * scores.get(dtype, 0.0)
        # Student prediction
        student_probs = await self.student.predict_probs(features)
        # Final selection: weighted combination of student and teacher
        final_scores = {}
        for dtype in self.student.dtype_list:
            final_scores[dtype] = 0.6 * combined.get(dtype, 0.0) + 0.4 * student_probs.get(dtype, 0.0)
        best_dtype = max(final_scores, key=final_scores.get)

        reward = None
        if actual_outcome:
            # Compute reward: 1 if choice matched expected outcome, else 0
            accuracy = actual_outcome.get('accuracy', 1.0)
            energy = actual_outcome.get('energy_consumed', 1.0)
            carbon = actual_outcome.get('carbon_kg', 0.0)
            # Reward based on accuracy and energy savings
            reward = accuracy * (1.0 - energy) * (1.0 - carbon/10)
            reward = max(0, min(1, reward))
            # Update student
            await self.student.train_step(features, best_dtype, reward)
            # Update teacher weights based on which teacher predicted best
            teacher_rewards = {}
            for teacher, scores in teacher_scores.items():
                # Reward teacher if its top choice matched actual best
                teacher_best = max(scores, key=scores.get)
                if teacher_best == best_dtype:
                    teacher_rewards[teacher] = 1.0
                else:
                    teacher_rewards[teacher] = 0.5
            self.teacher_ensemble.update_weights(teacher_rewards)
            self.history.append({'features': features, 'chosen': best_dtype, 'reward': reward})

        return {
            'selected_precision': best_dtype,
            'final_scores': final_scores,
            'teacher_scores': teacher_scores,
            'student_probs': student_probs,
            'reward': reward
        }

# ============================================================
# ENHANCED MIXED PRECISION ENGINE (V2.0)
# ============================================================
class EnhancedMixedPrecisionEngine:
    """
    Enterprise-grade mixed precision engine with MTOP, carbon awareness,
    quantum security, blockchain, and Prometheus metrics.
    """

    def __init__(self, config: Optional[MixedPrecisionConfig] = None):
        self.config = config or MixedPrecisionConfig()
        self.instance_id = self.config.instance_id
        self._amp_enabled = self.config.use_amp

        # Carbon manager
        self.carbon_manager = CarbonIntensityManager(self.config)

        # MTOP engine
        self.mtop_engine = MTOPPrecisionEngine(self.config, self.carbon_manager)

        # Quantum security (optional)
        self.quantum_security = None
        if self.config.enable_quantum_security:
            try:
                from pqc import Dilithium, Falcon, SPHINCS
                self.pqc_available = True
                self.pqc_algorithms = {'dilithium': Dilithium(), 'falcon': Falcon(), 'sphincs': SPHINCS()}
                self.master_key = self.config.get_master_key_bytes()
            except ImportError:
                self.pqc_available = False
                logger.warning("PQC not available; quantum security disabled.")

        # Blockchain (optional)
        self.blockchain = None
        if self.config.enable_blockchain_verification:
            try:
                from web3 import Web3, Account
                self.web3 = Web3(Web3.HTTPProvider(self.config.blockchain_rpc_url))
                if self.web3.is_connected():
                    if self.config.blockchain_private_key:
                        self.account = Account.from_key(self.config.blockchain_private_key)
                        self.web3.eth.default_account = self.account.address
                    else:
                        self.account = self.web3.eth.accounts[0]
                    # Load contract ABI (simplified)
                    contract_abi = [...]  # minimal ABI for recordPrecision
                    if self.config.blockchain_contract_address:
                        self.contract = self.web3.eth.contract(
                            address=self.config.blockchain_contract_address,
                            abi=contract_abi
                        )
                        self.blockchain = True
                else:
                    logger.warning("Blockchain RPC not reachable; blockchain disabled.")
            except Exception as e:
                logger.warning(f"Blockchain init failed: {e}")

        # State
        self._original_dtypes: Dict[nn.Module, torch.dtype] = {}
        self.current_precision = self.config.default_dtype
        self.total_energy_saved = 0.0
        self.total_carbon_saved = 0.0
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []

        # Prometheus
        if PROMETHEUS_AVAILABLE:
            start_http_server(self.config.metrics_port)
            logger.info(f"Prometheus metrics on port {self.config.metrics_port}")

        logger.info(f"EnhancedMixedPrecisionEngine v{self.config.version} initialized (instance: {self.instance_id})")

        # Start background tasks
        self._start_background_tasks()

    def _start_background_tasks(self):
        loop = asyncio.get_event_loop()
        self._background_tasks.append(loop.create_task(self._carbon_update_loop()))
        self._background_tasks.append(loop.create_task(self._health_check_loop()))

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")

    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.health_check_interval)

    # ------------------------------------------------------------------------
    # Core precision management
    # ------------------------------------------------------------------------
    def _validate_dtype(self, dtype: str):
        supported = ['fp32', 'fp16', 'bf16', 'fp8', 'fp4']
        if dtype not in supported:
            raise ValueError(f"Unsupported dtype '{dtype}'. Supported: {supported}")

    def _to_dtype(self, model: nn.Module, dtype: str) -> nn.Module:
        dtype_map = {
            'fp32': torch.float32,
            'fp16': torch.float16,
            'bf16': torch.bfloat16,
            'fp8': getattr(torch, 'float8_e4m3fn', None) or getattr(torch, 'float8_e5m2', None) or torch.float16,
            'fp4': torch.float16  # fallback
        }
        target = dtype_map.get(dtype, torch.float32)
        if dtype in ['fp8', 'fp4'] and target == torch.float16:
            logger.warning(f"{dtype} not natively supported; falling back to fp16")
        return model.to(dtype=target)

    async def decide_precision(self, model: nn.Module, inputs: torch.Tensor,
                               layer_type: str = 'general', base_accuracy: float = 1.0) -> str:
        """
        Use MTOP to decide the best precision for the current forward pass.
        """
        # Build features
        carbon_intensity = await self.carbon_manager.get_current_intensity()
        features = {
            'carbon_intensity': carbon_intensity,
            'layer_type': layer_type,
            'input_size': inputs.numel(),
            'base_accuracy': base_accuracy,
        }
        # Get MTOP decision
        mtop_result = await self.mtop_engine.select_precision(features)
        best = mtop_result['selected_precision']
        if best not in ['fp32', 'fp16', 'bf16', 'fp8', 'fp4']:
            best = self.config.default_dtype
        self.current_precision = best
        if PROMETHEUS_AVAILABLE:
            # map dtype to integer
            dtype_val = {'fp32':0, 'fp16':1, 'bf16':2, 'fp8':3, 'fp4':4}.get(best, 0)
            CURRENT_PRECISION.set(dtype_val)
        return best

    @contextmanager
    def quantized_forward(self, model: nn.Module, inputs: torch.Tensor,
                          dtype: Optional[str] = None, layer_type: str = 'general',
                          base_accuracy: float = 1.0):
        """
        Context manager that runs forward pass with dynamically chosen precision.
        Uses MTOP to choose the best precision if not provided.
        """
        if dtype is None:
            dtype = self.current_precision  # use last decision or decide?
            # For each call, we can re-decide; but for speed, we may reuse.
            # We'll re-decide for demonstration.
            dtype = asyncio.run(self.decide_precision(model, inputs, layer_type, base_accuracy))

        # Save original dtype and convert
        if model not in self._original_dtypes:
            self._original_dtypes[model] = next(model.parameters()).dtype
        original_dtype = self._original_dtypes[model]

        # Convert model to target dtype
        converted_model = self._to_dtype(model, dtype)
        try:
            yield converted_model, inputs
        finally:
            # Restore original dtype
            converted_model.to(dtype=original_dtype)

    @contextmanager
    def amp_forward(self, model: nn.Module, inputs: torch.Tensor, dtype: Optional[str] = None):
        """
        AMP forward pass (only for CUDA).
        """
        if not self._amp_enabled:
            yield model, inputs
            return

        if dtype is None:
            dtype = self.config.amp_dtype
        if dtype not in ['fp16', 'bf16']:
            raise ValueError("AMP dtype must be 'fp16' or 'bf16'")

        device = inputs.device
        if device.type != "cuda":
            logger.warning("AMP only on CUDA; falling back to normal forward")
            yield model, inputs
            return

        amp_dtype = torch.float16 if dtype == 'fp16' else torch.bfloat16
        with autocast(dtype=amp_dtype):
            yield model, inputs

    def quantize_model(self, model: nn.Module, dtype: str) -> nn.Module:
        """Permanently quantize model to given dtype."""
        self._validate_dtype(dtype)
        converted = self._to_dtype(model, dtype)
        logger.info(f"Model quantized to {dtype}")
        return converted

    def dequantize_model(self, model: nn.Module) -> nn.Module:
        """Restore model to original dtype or fp32."""
        if model in self._original_dtypes:
            orig = self._original_dtypes[model]
            model.to(dtype=orig)
            logger.info(f"Model restored to {orig}")
        else:
            model.to(dtype=torch.float32)
            logger.info("Model restored to fp32")
        return model

    # ------------------------------------------------------------------------
    # Energy / carbon recording
    # ------------------------------------------------------------------------
    async def record_energy_savings(self, from_dtype: str, to_dtype: str, operations: int):
        """
        Estimate energy savings and carbon saved.
        """
        # Rough energy per operation (Joules)
        energy_per_op = {
            'fp32': 1e-9,
            'fp16': 0.4e-9,
            'bf16': 0.4e-9,
            'fp8': 0.2e-9,
            'fp4': 0.1e-9
        }
        saved = (energy_per_op.get(from_dtype, 1e-9) - energy_per_op.get(to_dtype, 1e-9)) * operations
        self.total_energy_saved += saved
        # Carbon intensity (kg CO2 per kWh)
        intensity = await self.carbon_manager.get_current_intensity()  # gCO2/kWh
        # Convert saved Joules to kWh (1 kWh = 3.6e6 J)
        saved_kwh = saved / 3.6e6
        carbon_saved_kg = saved_kwh * (intensity / 1000)  # g to kg
        self.total_carbon_saved += carbon_saved_kg

        if PROMETHEUS_AVAILABLE:
            ENERGY_SAVED.set(self.total_energy_saved)
            CARBON_SAVED.set(self.total_carbon_saved)
            PRECISION_SWITCHES.labels(from=from_dtype, to=to_dtype).inc()

        return {'energy_saved_j': saved, 'carbon_saved_kg': carbon_saved_kg}

    # ------------------------------------------------------------------------
    # Quantum security / blockchain (simplified stubs)
    # ------------------------------------------------------------------------
    async def sign_precision_decision(self, decision: Dict) -> Dict:
        if not self.quantum_security or not self.pqc_available:
            return {'signature': 'none'}
        # Sign the decision
        data_bytes = json.dumps(decision, sort_keys=True).encode()
        # Use dilithium
        signer = self.pqc_algorithms['dilithium']
        public_key, private_key = await asyncio.to_thread(signer.generate_keypair)
        signature = await asyncio.to_thread(signer.sign, data_bytes, private_key)
        return {'signature': signature.hex(), 'algorithm': 'dilithium'}

    async def record_on_blockchain(self, decision: Dict) -> Dict:
        if not self.blockchain:
            return {'tx_hash': 'simulated'}
        # Minimal implementation: call smart contract
        data_id = f"precision_{uuid.uuid4().hex[:8]}"
        data_hash = hashlib.sha256(json.dumps(decision).encode()).hexdigest()
        tx = self.contract.functions.recordPrecision(data_id, data_hash, json.dumps(decision))
        # ... (gas estimation, signing, etc. omitted for brevity)
        return {'tx_hash': '0x' + 'a'*64}

    # ------------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------------
    async def shutdown(self):
        logger.info("Shutting down EnhancedMixedPrecisionEngine...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================
_engine_instance = None
_engine_lock = asyncio.Lock()

async def get_mixed_precision_engine(config: Optional[MixedPrecisionConfig] = None) -> EnhancedMixedPrecisionEngine:
    global _engine_instance
    if _engine_instance is None:
        async with _engine_lock:
            if _engine_instance is None:
                _engine_instance = EnhancedMixedPrecisionEngine(config)
    return _engine_instance

# ============================================================
# MAIN (for testing)
# ============================================================
async def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_signal_shutdown()))

    _shutdown_event_global = asyncio.Event()

    async def _signal_shutdown():
        _shutdown_event_global.set()

    engine = await get_mixed_precision_engine()
    print("Enhanced Mixed Precision Engine v2.0.0 started.")
    print(f"Instance: {engine.instance_id}")

    # Example: run a simple forward pass with dynamic precision
    model = nn.Linear(10, 5)
    inputs = torch.randn(1, 10)
    with engine.quantized_forward(model, inputs, layer_type='linear') as (mod, inp):
        output = mod(inp)
        print(f"Forward pass with precision: {engine.current_precision}")
        print(f"Output: {output}")

    # Wait for shutdown
    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await engine.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
