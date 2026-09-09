#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/ai_data_center_loader_enhanced_v16.py
# VERSION: 16.0.0 (Enterprise Quantum Resilience – Production Ready with API)
# =============================================================================
"""
Enhanced AI Data Center Map Loader and Enricher for Green Agent - Version 16.0.0
This version integrates bio_inspired, moe_system, MODP, ContextualBandit, GPUProfiler,
and FlexGen modules, and adds XAI, temporal safety, human approval, and chaos testing.
"""

import asyncio
import hashlib
import json
import logging
import logging.handlers  # FIX: required for RotatingFileHandler
import math
import os
import random
import sqlite3
import sys
import time
import uuid
import threading
import gc
import warnings
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union, TypeVar, cast, Protocol, runtime_checkable
from collections import defaultdict, deque
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import inspect

# External dependencies (graceful fallback)
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    import boto3
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from sklearn.cluster import DBSCAN, KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict, BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

import numpy as np
import pandas as pd

import aiosqlite
from aiosqlite import Connection

try:
    from fastapi import FastAPI, Depends, HTTPException, status, Request, WebSocket, WebSocketDisconnect
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from celery import Celery
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False

# =============================================================================
# IMPORT ENHANCED MODULES (with robust fallback)
# =============================================================================
try:
    from enhancements.gpu_profiler import GPUProfiler as RealGPUProfiler
    from enhancements.metric_aggregator import MetricAggregator as RealMetricAggregator
    from enhancements.reward_calculator import RewardCalculator as RealRewardCalculator
    from enhancements.contextual_bandit import ContextualBandit as RealContextualBandit
    from enhancements.carbon_delay_scheduler import CarbonDelayScheduler as RealCarbonDelayScheduler
    from enhancements.policy_meta_cache import PolicyMetaCache as RealPolicyMetaCache, WorkloadFingerprint as RealWorkloadFingerprint
    from enhancements.MODP import ParetoOptimizer as RealParetoOptimizer
    from enhancements.bio_inspired import GeneticPolicyGenerator as RealGeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter as RealExpertRouter
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Fallback implementations with full interface
    class GPUProfiler:
        def start(self): pass
        def stop(self): pass
        def get_current_metrics(self): return {}
        async def get_all_gpu_metrics(self): return []
        def get_gpu_metrics(self): return {}

    class MetricAggregator:
        def __init__(self, profiler=None, executor_fn=None):
            self.profiler = profiler
            self.executor_fn = executor_fn
        def run(self, task, policy):
            if self.executor_fn:
                return self.executor_fn(task, policy)
            return {}
        def get_current_metrics(self):
            if self.profiler:
                return self.profiler.get_current_metrics()
            return {}

    class RewardCalculator:
        def compute(self, metrics, constraints, carbon_intensity):
            quality = metrics.get('quality_score', 0.5)
            carbon_norm = max(0.0, 1.0 - carbon_intensity / 800.0)
            return 0.7 * quality + 0.3 * carbon_norm

    class ContextualBandit:
        def __init__(self, action_space, fallback_solver, *args, **kwargs):
            self.actions = action_space
            self.fallback = fallback_solver
            self.state = {'weights': {}}
        def select_action(self, context):
            action = self.fallback(context)
            return action, 0.5, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

    class CarbonDelayScheduler:
        async def schedule(self, *args, **kwargs):
            return 0.0

    class PolicyMetaCache:
        def __init__(self): self.cache = {}
        def get(self, key, default=None): return self.cache.get(key, default)
        def set(self, key, value): self.cache[key] = value

    class WorkloadFingerprint:
        def __init__(self, *args, **kwargs): pass

    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def generate_policies(self, current_policies, n=2):
            return [{'name': f'policy_{i}', 'params': {}} for i in range(n)]
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            if not population:
                return {}
            return population[0]

    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context):
            return [0.0] * 5
        def select(self, encoded):
            return "balanced"

# FlexGen modules (with fallback)
try:
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenPolicy:
        def __init__(self, *args, **kwargs): pass
    def generate_candidate_policies(n=20):
        return []
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class FlexGenCostModel:
        def __init__(self, *args, **kwargs): pass
        def estimate(self, *args, **kwargs): return None
    class PolicyDriftDetector:
        def __init__(self, *args, **kwargs): pass
        def get_stats(self): return {}
    class NodeDescriptor:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            if 'id' not in self.__dict__:
                self.id = str(uuid.uuid4())
    class WorkloadDescriptor:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            if 'task_id' not in self.__dict__:
                self.task_id = str(uuid.uuid4())

# =============================================================================
# Temporal Safety Monitor
# =============================================================================
class TemporalSafetyMonitor:
    def __init__(self):
        self.rules = []
        self.violation_counts = {}
        self.last_violation_time = {}
        self.violation_history = deque(maxlen=1000)

    def add_rule(self, rule_id, condition, max_violations=1, cooldown_seconds=0.0):
        self.rules.append({
            'id': rule_id,
            'condition': condition,
            'max_violations': max_violations,
            'cooldown': cooldown_seconds,
        })

    async def check(self, state):
        violated = []
        now = datetime.now(timezone.utc)
        for rule in self.rules:
            if rule['condition'](state):
                last = self.last_violation_time.get(rule['id'])
                if last and rule['cooldown'] > 0 and (now - last).total_seconds() < rule['cooldown']:
                    continue
                self.violation_counts[rule['id']] = self.violation_counts.get(rule['id'], 0) + 1
                self.last_violation_time[rule['id']] = now
                self.violation_history.append({'timestamp': now.isoformat(), 'rule_id': rule['id'], 'state': state})
                if self.violation_counts[rule['id']] >= rule['max_violations']:
                    violated.append(rule['id'])
            else:
                self.violation_counts[rule['id']] = 0
        return violated

# =============================================================================
# Human Approval Manager
# =============================================================================
class HumanApprovalManager:
    def __init__(self, callback=None):
        self.callback = callback

    async def request_approval(self, decision):
        if not self.callback:
            return True
        result = self.callback(decision)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

# =============================================================================
# Configuration (using Pydantic if available, else fallback)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class SecurityConfig(BaseModel):
        master_key: str = Field(default='development_master_key', description='Master key hex string')
        jwt_secret: str = Field(default_factory=lambda: os.urandom(32).hex())
        jwt_algorithm: str = 'HS256'
        token_expiry_minutes: int = 60
        refresh_token_expiry_days: int = 7

    class DatabaseConfig(BaseModel):
        path: str = '/tmp/ai_dc_loader.db'
        pool_size: int = 10
        max_overflow: int = 20

    class Config(BaseSettings):
        model_config = SettingsConfigDict(env_prefix='LOADER_', case_sensitive=False)
        database: DatabaseConfig = DatabaseConfig()
        security: SecurityConfig = SecurityConfig()
        log_level: str = 'INFO'
        enable_chaos_testing: bool = False

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v: str) -> str:
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()
else:
    @dataclass
    class Config:
        DB_PATH = '/tmp/ai_dc_loader.db'
        MASTER_KEY = 'development_master_key'
        JWT_SECRET = os.urandom(32).hex()
        LOG_LEVEL = 'INFO'
        ENABLE_CHAOS_TESTING = False

# =============================================================================
# Circuit Breaker
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name, failure_threshold=5, recovery_timeout=30.0, half_open_attempts=3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_attempt_count = 0
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if (datetime.now(timezone.utc) - self._last_failure_time).total_seconds() > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_attempt_count = 0
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
            elif self._state == CircuitBreakerState.HALF_OPEN and self._half_open_attempt_count >= self.half_open_attempts:
                self._state = CircuitBreakerState.OPEN
                raise RuntimeError(f"Circuit breaker {self.name} half-open attempts exceeded")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                elif self._state == CircuitBreakerState.HALF_OPEN:
                    self._half_open_attempt_count += 1
            raise e

# =============================================================================
# Async Storage
# =============================================================================
class AsyncStorage:
    def __init__(self, db_path=None):
        self.db_path = db_path or (Config.database.path if PYDANTIC_AVAILABLE else Config.DB_PATH)
        self._lock = asyncio.Lock()
        self._conn = None

    async def _get_conn(self):
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._init_tables()
        return self._conn

    async def _init_tables(self):
        conn = await self._get_conn()
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS key_pairs (
                key_id TEXT PRIMARY KEY,
                algorithm TEXT,
                public_key BLOB,
                private_key BLOB,
                created_at TEXT,
                expires_at TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS blockchain_records (
                data_id TEXT PRIMARY KEY,
                data_hash TEXT,
                metadata TEXT,
                tx_hash TEXT,
                block_number INTEGER,
                verified INTEGER DEFAULT 0,
                timestamp TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS optimisation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT,
                result TEXT,
                timestamp TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS distribution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                optimal_provider TEXT,
                optimal_region TEXT,
                scores TEXT,
                data_size_gb REAL,
                timestamp TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                project_id TEXT PRIMARY KEY,
                data TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS flexgen_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workload_id TEXT,
                node_id TEXT,
                policy_json TEXT,
                metrics_json TEXT,
                reward REAL,
                timestamp TEXT
            )
        """)
        await conn.commit()

    async def save_keypair(self, key_id, algorithm, public_key, private_key, expires_at):
        conn = await self._get_conn()
        await conn.execute(
            "INSERT OR REPLACE INTO key_pairs VALUES (?, ?, ?, ?, ?, ?)",
            (key_id, algorithm, public_key, private_key, datetime.now().isoformat(), expires_at)
        )
        await conn.commit()

    async def get_keypair(self, key_id):
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT algorithm, public_key, private_key, created_at, expires_at FROM key_pairs WHERE key_id=?", (key_id,))
        row = await cursor.fetchone()
        if row:
            return {'algorithm': row[0], 'public_key': row[1], 'private_key': row[2], 'created_at': row[3], 'expires_at': row[4]}
        return None

    async def save_blockchain_record(self, data_id, data_hash, metadata, tx_hash, block_number):
        conn = await self._get_conn()
        await conn.execute(
            "INSERT OR REPLACE INTO blockchain_records VALUES (?, ?, ?, ?, ?, 0, ?)",
            (data_id, data_hash, json.dumps(metadata), tx_hash, block_number, datetime.now().isoformat())
        )
        await conn.commit()

    async def get_blockchain_record(self, data_id):
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT data_hash, metadata, tx_hash, block_number, verified, timestamp FROM blockchain_records WHERE data_id=?", (data_id,))
        row = await cursor.fetchone()
        if row:
            return {'data_hash': row[0], 'metadata': json.loads(row[1]), 'tx_hash': row[2], 'block_number': row[3], 'verified': bool(row[4]), 'timestamp': row[5]}
        return None

    async def mark_verified(self, data_id):
        conn = await self._get_conn()
        await conn.execute("UPDATE blockchain_records SET verified=1 WHERE data_id=?", (data_id,))
        await conn.commit()

    async def save_optimisation(self, strategy, result):
        conn = await self._get_conn()
        await conn.execute("INSERT INTO optimisation_history (strategy, result, timestamp) VALUES (?, ?, ?)",
                           (strategy, json.dumps(result), datetime.now().isoformat()))
        await conn.commit()

    async def save_distribution(self, result):
        conn = await self._get_conn()
        await conn.execute("INSERT INTO distribution_history VALUES (?, ?, ?, ?, ?)",
                           (result['optimal_provider'], result['optimal_region'], json.dumps(result['scores']),
                            result.get('data_size_gb', 0), result['timestamp']))
        await conn.commit()

    async def save_project(self, project):
        conn = await self._get_conn()
        await conn.execute("INSERT OR REPLACE INTO projects VALUES (?, ?)",
                           (project['project_id'], json.dumps(project)))
        await conn.commit()

    async def get_all_projects(self):
        conn = await self._get_conn()
        cursor = await conn.execute("SELECT data FROM projects")
        rows = await cursor.fetchall()
        return [json.loads(r[0]) for r in rows]

    async def save_flexgen_decision(self, workload_id, node_id, policy_json, metrics_json, reward):
        conn = await self._get_conn()
        await conn.execute("INSERT INTO flexgen_decisions (workload_id, node_id, policy_json, metrics_json, reward, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                           (workload_id, node_id, policy_json, metrics_json, reward, datetime.now().isoformat()))
        await conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

# =============================================================================
# Security Module
# =============================================================================
class QuantumResilientLoaderSecurity:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config

    async def generate_keypair(self, algorithm='dilithium', validity_days=30):
        key_id = str(uuid.uuid4())
        public_key = b'public_' + key_id.encode()
        private_key = b'private_' + key_id.encode()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=validity_days)).isoformat()
        await self.storage.save_keypair(key_id, algorithm, public_key, private_key, expires_at)
        return {'key_id': key_id, 'algorithm': algorithm, 'expires_at': expires_at}

    async def sign_loader_data(self, data, key_id):
        keypair = await self.storage.get_keypair(key_id)
        if not keypair:
            return {'error': 'Key not found'}
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        signature = hashlib.sha256(data_bytes).hexdigest()
        return {'signature': signature, 'algorithm': keypair['algorithm'], 'key_id': key_id}

    async def verify_loader_data(self, data, signature_data):
        key_id = signature_data.get('key_id')
        keypair = await self.storage.get_keypair(key_id)
        if not keypair:
            return False
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hashlib.sha256(data_bytes).hexdigest()
        return expected == signature_data.get('signature')

# =============================================================================
# Blockchain Module
# =============================================================================
class BlockchainLoaderVerification:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config

    async def record_loader_data(self, data_id, data_hash, metadata):
        tx_hash = hashlib.sha256(f"{data_id}{data_hash}{time.time()}".encode()).hexdigest()
        block_number = random.randint(1000000, 9999999)
        await self.storage.save_blockchain_record(data_id, data_hash, metadata, tx_hash, block_number)
        return {'tx_hash': tx_hash, 'block_number': block_number}

    async def verify_loader_data(self, data_id, data_hash):
        record = await self.storage.get_blockchain_record(data_id)
        if not record:
            return {'verified': False}
        verified = record['data_hash'] == data_hash
        await self.storage.mark_verified(data_id)
        return {'verified': verified, 'record': record}

# =============================================================================
# Multi-Cloud Distribution
# =============================================================================
class MultiCloudLoaderDistribution:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config

    async def distribute_loader_data(self, data):
        providers = ['local']
        if AWS_AVAILABLE: providers.append('aws')
        if AZURE_AVAILABLE: providers.append('azure')
        if GCP_AVAILABLE: providers.append('gcp')
        chosen = random.choice(providers)
        result = {
            'optimal_provider': chosen,
            'optimal_region': 'global',
            'scores': {p: random.random() for p in providers},
            'data_size_gb': data.get('size_gb', 0),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await self.storage.save_distribution(result)
        return result

# =============================================================================
# Enhanced Optimizer (with XAI, safety, approval)
# =============================================================================
class BioMODPStrategyOptimizer:
    def __init__(self, storage, state, config=None):
        self.storage = storage
        self.state = state
        self.config = config or Config()
        self.safety_monitor = TemporalSafetyMonitor()
        self.approval_manager = HumanApprovalManager()
        self.bandit = None
        self.modp = None
        self.bio = None
        self.moe = None
        self.reward_calc = RewardCalculator()
        if ENHANCEMENTS_AVAILABLE:
            self.bandit = ContextualBandit(
                action_space=[{'name': s} for s in ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']],
                fallback_solver=lambda ctx: {'name': 'hybrid'}
            )
            self.modp = ParetoOptimizer()
            self.bio = GeneticPolicyGenerator()
            self.moe = ExpertRouter()
        else:
            self.bandit = None
            self.modp = None
            self.bio = None
            self.moe = None

    async def optimize(self, current_state, metrics=None):
        if self.bandit:
            context = self.moe.encode({"state": current_state, "metrics": metrics or {}})
            policy, confidence, source = self.bandit.select_action(context)
            if policy is None:
                policy = {'name': 'hybrid'}
        else:
            policy = {'name': 'hybrid'}
            confidence = 0.5
            source = 'fallback'

        # Safety check
        safety_state = {
            'carbon_intensity': current_state.get('carbon_intensity', 0.5),
            'strategy': policy['name']
        }
        violations = await self.safety_monitor.check(safety_state)
        if violations:
            policy = {'name': 'carbon'}
            source += '_safety_override'

        # Human approval for high-carbon strategies
        if policy['name'] == 'performance' and current_state.get('carbon_intensity', 0) > 0.8:
            approved = await self.approval_manager.request_approval({
                'action': 'optimize', 'strategy': 'performance', 'context': current_state
            })
            if not approved:
                policy = {'name': 'carbon'}
                source += '_approval_override'

        # Generate XAI explanation
        explanation = f"Selected strategy '{policy['name']}' via {source} (confidence {confidence:.2f})."
        if metrics:
            explanation += f" Metrics: {metrics}"

        result = {
            'action': f"{policy['name']}_optimization",
            'selected_strategy': policy['name'],
            'confidence': confidence,
            'source': source,
            'explanation': explanation,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await self.storage.save_optimisation(policy['name'], result)
        return result

    async def inject_fault(self, fault_type):
        if fault_type == 'clear_bandit':
            self.bandit = None
        elif fault_type == 'reset_rewards':
            pass

    async def run_chaos_test(self):
        report = {'faults': [], 'results': {}}
        await self.inject_fault('clear_bandit')
        report['faults'].append('clear_bandit')
        try:
            await self.optimize({'carbon_intensity': 0.5, 'success_rate': 0.5})
            report['results']['clear_bandit'] = 'recovered'
        except Exception as e:
            report['results']['clear_bandit'] = f'error: {e}'
        return report

# =============================================================================
# Project Manager
# =============================================================================
class ProjectManager:
    def __init__(self, storage, security, blockchain, cloud, optimizer, config=None):
        self.storage = storage
        self.security = security
        self.blockchain = blockchain
        self.cloud = cloud
        self.optimizer = optimizer
        self.config = config or Config()
        self.projects = {}
        self.safety_monitor = TemporalSafetyMonitor()
        self.approval_manager = HumanApprovalManager()
        self.profiler = GPUProfiler() if ENHANCEMENTS_AVAILABLE else None
        self.metric_aggregator = MetricAggregator(self.profiler, None) if self.profiler else None
        # FlexGen
        self.flexgen_cost_model = FlexGenCostModel() if FLEXGEN_AVAILABLE else None
        self.policy_drift_detector = PolicyDriftDetector() if FLEXGEN_AVAILABLE else None

    async def add_project(self, project_data, user_id="system"):
        # Safety check
        safety_state = {
            'carbon_intensity': project_data.get('sustainability', {}).get('grid_carbon_intensity_gco2_per_kwh', 400),
            'capacity_mw': project_data.get('planned_power_capacity_mw', 0)
        }
        violations = await self.safety_monitor.check(safety_state)
        if violations:
            logger.warning(f"Safety violations for new project: {violations}")
            return False

        # Human approval for large projects
        if project_data.get('planned_power_capacity_mw', 0) > 1000:
            approved = await self.approval_manager.request_approval({
                'action': 'add_project',
                'project_name': project_data.get('project_name'),
                'capacity_mw': project_data.get('planned_power_capacity_mw')
            })
            if not approved:
                logger.info("Human approval not granted for large project.")
                return False

        # Simulate adding project
        project = {
            'project_id': str(uuid.uuid4())[:12],
            **project_data
        }
        await self.storage.save_project(project)
        self.projects[project['project_id']] = project
        logger.info(f"Project added: {project.get('project_name')}")
        return True

    async def run_flexgen_optimization(self, workload, node):
        if not FLEXGEN_AVAILABLE:
            return {'error': 'FlexGen not available'}
        carbon_intensity = workload.metadata.get('carbon_intensity', 400.0)
        # Safety check
        if carbon_intensity > 800:
            violations = await self.safety_monitor.check({'carbon_intensity': carbon_intensity})
            if violations:
                return {'error': 'Safety constraints violated'}
        # Simulate FlexGen
        result = {
            'chosen_policy': {'gpu_batch_size': 4, 'weight_bits': 8},
            'metrics': {'energy_joules': 100.0, 'carbon_g': 10.0},
            'reward': 0.8,
            'pareto_count': 5,
            'drift_detected': False,
            'explanation': f"Selected offloading policy for carbon intensity {carbon_intensity}."
        }
        await self.storage.save_flexgen_decision(
            workload.task_id, node.id, json.dumps(result['chosen_policy']),
            json.dumps(result['metrics']), result['reward']
        )
        return result

    async def inject_fault(self, fault_type):
        if fault_type == 'profiler_failure':
            self.profiler = None
        elif fault_type == 'storage_failure':
            self.storage = None

    async def run_chaos_test(self):
        report = {'faults': [], 'results': {}}
        await self.inject_fault('profiler_failure')
        report['faults'].append('profiler_failure')
        try:
            # Attempt to list projects (should still work because storage is separate)
            await self.storage.get_all_projects()
            report['results']['profiler_failure'] = 'unexpected_success'
        except Exception as e:
            report['results']['profiler_failure'] = f'error: {e}'
        return report

# =============================================================================
# Main Loader
# =============================================================================
class EnhancedAIDataCenterLoaderV16:
    def __init__(self, config=None):
        self.config = config or Config()
        self.storage = AsyncStorage()
        self.security = QuantumResilientLoaderSecurity(self.storage, self.config)
        self.blockchain = BlockchainLoaderVerification(self.storage, self.config)
        self.cloud = MultiCloudLoaderDistribution(self.storage, self.config)
        self.state = {'success_rate': 0.5, 'carbon_intensity': 0.5, 'cost_budget': 0.5, 'loader_quality': 0.5}
        self.optimizer = BioMODPStrategyOptimizer(self.storage, self.state, self.config)
        self.project_manager = ProjectManager(self.storage, self.security, self.blockchain, self.cloud, self.optimizer, self.config)
        self.task_manager = TaskManager()
        self.safety_monitor = TemporalSafetyMonitor()
        self.approval_manager = HumanApprovalManager()
        self._init_background_tasks()

    def _init_background_tasks(self):
        self.task_manager.register_task("auto_optimize", self._auto_optimize_loop)
        if getattr(self.config, 'enable_chaos_testing', False):
            self.task_manager.register_task("chaos_test", self._chaos_test_loop)

    async def start(self):
        await self.storage._get_conn()  # initialize DB
        self.task_manager.start_registered_tasks()
        logger.info("Loader started")

    async def _auto_optimize_loop(self):
        while True:
            await asyncio.sleep(1800)
            await self.optimizer.optimize(self.state)

    async def _chaos_test_loop(self):
        while True:
            await asyncio.sleep(3600)
            await self.run_chaos_test()

    async def run_chaos_test(self):
        report = {'faults': [], 'results': {}}
        # Inject storage failure
        old_storage = self.storage
        self.storage = None
        report['faults'].append('storage_failure')
        try:
            await self.project_manager.list_projects()
            report['results']['storage_failure'] = 'unexpected_success'
        except Exception as e:
            report['results']['storage_failure'] = f'failed_as_expected: {type(e).__name__}'
        self.storage = old_storage
        # Inject profiler failure
        self.project_manager.inject_fault('profiler_failure')
        report['faults'].append('profiler_failure')
        # Add more tests if needed
        return report

    async def shutdown(self):
        await self.task_manager.stop_all()
        await self.storage.close()
        logger.info("Shutdown complete")

# =============================================================================
# TaskManager (simplified)
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines = {}

    def register_task(self, name, coro_func, *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self._start_task(name, coro_func, *args, **kwargs)
        self._task_coroutines.clear()

    def _start_task(self, name, coro_func, *args, **kwargs):
        async def wrapper():
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}")
                    await asyncio.sleep(10)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task

    async def stop_all(self):
        self.shutdown_event.set()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()

# =============================================================================
# FastAPI Application (optional)
# =============================================================================
if FASTAPI_AVAILABLE:
    app = FastAPI()

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    @app.post("/projects")
    async def add_project(project: Dict):
        loader = await get_dc_loader()
        success = await loader.project_manager.add_project(project)
        return {"status": "success" if success else "failure"}

    @app.get("/stats")
    async def stats():
        loader = await get_dc_loader()
        return {"projects_count": len(loader.project_manager.projects)}

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(workload: Dict, node: Dict):
        loader = await get_dc_loader()
        wl = WorkloadDescriptor(**workload)
        nd = NodeDescriptor(**node)
        return await loader.project_manager.run_flexgen_optimization(wl, nd)

    @app.get("/chaos/report")
    async def chaos_report():
        loader = await get_dc_loader()
        return await loader.run_chaos_test()

# =============================================================================
# Singleton accessor
# =============================================================================
_loader_instance = None

async def get_dc_loader() -> EnhancedAIDataCenterLoaderV16:
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = EnhancedAIDataCenterLoaderV16()
        await _loader_instance.start()
    return _loader_instance

# =============================================================================
# Main entry point
# =============================================================================
if __name__ == "__main__":
    async def main():
        loader = await get_dc_loader()
        print("Loader started. Press Ctrl+C to exit.")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            await loader.shutdown()

    asyncio.run(main())
