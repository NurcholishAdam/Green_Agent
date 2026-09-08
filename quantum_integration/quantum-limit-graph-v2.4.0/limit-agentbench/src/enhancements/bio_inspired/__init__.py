#!/usr/bin/env python3
"""
Bio-Inspired Green Agent v8.3.0 with full enhancement modules integrated.
Includes: Quantum-Distillation (placeholder), Causal RL, Federated Learning,
Safety Monitor, XAI, Adaptive Precision, Carbon Market, Chaos Testing, Human-in-the-Loop.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
import logging
import os
import sqlite3
import uuid
import random
import math
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, Union, runtime_checkable
from collections import deque
import json
import copy
import numpy as np

# Optional dependencies
try:
    from pydantic import BaseModel, Field, validator
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object

try:
    import structlog
    logger = structlog.get_logger(__name__)
    HAS_STRUCTLOG = True
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)
    HAS_STRUCTLOG = False

try:
    from sklearn.ensemble import IsolationForest
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    import aiosqlite
    HAS_AIOSQLITE = True
except ImportError:
    HAS_AIOSQLITE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

# =====================================================================
# PROTOCOLS
# =====================================================================
@runtime_checkable
class TokenServiceProtocol(Protocol):
    async def get_balance(self, entity_id: str, correlation_id: str) -> float: ...
    async def consume_tokens(self, entity_id: str, amount: float, correlation_id: str) -> bool: ...

@runtime_checkable
class GradientServiceProtocol(Protocol):
    async def compute_gradient_field(self, telemetry: Dict[str, Any], correlation_id: str) -> float: ...

# =====================================================================
# DYNAMIC CONFIGURATION
# =====================================================================
if HAS_PYDANTIC:
    class BioCoreConfig(BaseModel):
        env: str = "production"
        atp_token_threshold: float = 10.0
        proton_gradient_max: float = 100.0
        biomass_capacity: float = 1000.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_time: float = 30.0
        anomaly_sensitivity: float = 0.05
        retrain_interval_sec: float = 3600.0
        db_path: str = "bio_core.db"
        event_worker_count: int = 4
        batch_write_interval_sec: float = 2.0
        anomaly_buffer_size: int = 10000
        isolation_forest_n_estimators: int = 100
        isolation_forest_max_samples: Optional[Union[int, float]] = 'auto'
        isolation_forest_contamination: float = 0.05
        event_queue_maxsize: int = 1000
        prometheus_port: Optional[int] = None
        persistence_circuit_breaker_threshold: int = 3
        persistence_circuit_breaker_recovery_time: float = 10.0
        adaptive_retraining_enabled: bool = True
        adaptive_retraining_window: int = 100
        drift_threshold: float = 0.1
        optimization_enabled: bool = True
        optimization_interval_sec: float = 3600.0
        optimization_population_size: int = 20
        optimization_generations: int = 5
        optimization_mutation_rate: float = 0.2
        optimization_crossover_rate: float = 0.8
        optimization_objective_weights: Dict[str, float] = Field(default_factory=lambda: {
            'gradient_efficiency': 0.4, 'token_balance_efficiency': 0.3, 'anomaly_score': 0.3,
        })
        optimization_dynamic_weights: bool = True

        # New enhancement flags
        enable_quantum_distillation: bool = False
        enable_causal_rl: bool = False
        enable_federated: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        human_approval_timeout: float = 60.0

        @validator('anomaly_sensitivity')
        def validate_anomaly_sensitivity(cls, v):
            assert 0 <= v <= 1
            return v

        @validator('isolation_forest_contamination')
        def validate_contamination(cls, v):
            assert 0 <= v <= 0.5
            return v
else:
    @dataclass
    class BioCoreConfig:
        env: str = "production"
        atp_token_threshold: float = 10.0
        proton_gradient_max: float = 100.0
        biomass_capacity: float = 1000.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_time: float = 30.0
        anomaly_sensitivity: float = 0.05
        retrain_interval_sec: float = 3600.0
        db_path: str = "bio_core.db"
        event_worker_count: int = 4
        batch_write_interval_sec: float = 2.0
        anomaly_buffer_size: int = 10000
        isolation_forest_n_estimators: int = 100
        isolation_forest_max_samples: Optional[Union[int, float]] = 'auto'
        isolation_forest_contamination: float = 0.05
        event_queue_maxsize: int = 1000
        prometheus_port: Optional[int] = None
        persistence_circuit_breaker_threshold: int = 3
        persistence_circuit_breaker_recovery_time: float = 10.0
        adaptive_retraining_enabled: bool = True
        adaptive_retraining_window: int = 100
        drift_threshold: float = 0.1
        optimization_enabled: bool = True
        optimization_interval_sec: float = 3600.0
        optimization_population_size: int = 20
        optimization_generations: int = 5
        optimization_mutation_rate: float = 0.2
        optimization_crossover_rate: float = 0.8
        optimization_objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'gradient_efficiency': 0.4, 'token_balance_efficiency': 0.3, 'anomaly_score': 0.3,
        })
        optimization_dynamic_weights: bool = True
        enable_quantum_distillation: bool = False
        enable_causal_rl: bool = False
        enable_federated: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        human_approval_timeout: float = 60.0

# =====================================================================
# CIRCUIT BREAKER
# =====================================================================
class CircuitState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    def __init__(self, name, failure_threshold=5, recovery_time=30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_state_change = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            now = datetime.now(timezone.utc)
            if self.state == CircuitState.OPEN:
                if (now - self.last_state_change).total_seconds() > self.recovery_time:
                    self.state = CircuitState.HALF_OPEN
                    self.last_state_change = now
                else:
                    raise RuntimeError(f"CircuitBreaker '{self.name}' is OPEN")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.last_state_change = datetime.now(timezone.utc)
            return result
        except Exception as exc:
            async with self._lock:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    self.last_state_change = datetime.now(timezone.utc)
            raise exc

    @property
    def state_value(self):
        return self.state.value

    def get_state_numeric(self):
        return 0 if self.state == CircuitState.CLOSED else 1 if self.state == CircuitState.HALF_OPEN else 2

# =====================================================================
# PERSISTENCE
# =====================================================================
class AlertStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"

class Persistence:
    def __init__(self, db_path="bio_core.db", batch_interval=2.0, cb_threshold=3, cb_recovery=10.0):
        self.db_path = db_path
        self.batch_interval = batch_interval
        self._lock = asyncio.Lock()
        self._write_queue = asyncio.Queue()
        self._retry_queue = deque()
        self._flush_task = None
        self._circuit = CircuitBreaker("persistence", cb_threshold, cb_recovery)
        self._schema_version = 2

    async def initialize(self):
        async with self._lock:
            if HAS_AIOSQLITE:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.executescript(self._get_schema())
                    await db.commit()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._sync_init_db)
            self._flush_task = asyncio.create_task(self._periodic_batch_flusher())

    def _get_schema(self):
        return f"""
        CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT);
        INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES ({self._schema_version}, datetime('now'));
        CREATE TABLE IF NOT EXISTS alerts (id TEXT PRIMARY KEY, level TEXT, message TEXT, status TEXT, correlation_id TEXT, timestamp TEXT);
        CREATE TABLE IF NOT EXISTS cost_benefit (id TEXT PRIMARY KEY, cost REAL, benefit REAL, roi REAL, correlation_id TEXT, timestamp TEXT);
        CREATE TABLE IF NOT EXISTS optimization_results (job_id TEXT PRIMARY KEY, algorithm TEXT, pareto_front TEXT, best_parameters TEXT, objectives TEXT, timestamp TEXT);
        """

    def _sync_init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(self._get_schema())
            conn.commit()

    async def _safe_db_operation(self, operation, *args, **kwargs):
        return await self._circuit.call(operation, *args, **kwargs)

    async def enqueue_alert(self, alert_id, level, message, status, cid):
        ts = datetime.now(timezone.utc).isoformat()
        await self._write_queue.put(("INSERT OR REPLACE INTO alerts VALUES (?, ?, ?, ?, ?, ?)",
                                     (alert_id, level, message, status, cid, ts)))

    async def archive_alert(self, alert_id):
        await self._write_queue.put(("UPDATE alerts SET status = ? WHERE id = ?", (AlertStatus.ARCHIVED.value, alert_id)))

    async def enqueue_cost_benefit(self, model_id, cost, benefit, roi, cid):
        ts = datetime.now(timezone.utc).isoformat()
        await self._write_queue.put(("INSERT OR REPLACE INTO cost_benefit VALUES (?, ?, ?, ?, ?, ?)",
                                     (model_id, cost, benefit, roi, cid, ts)))

    async def save_optimization_result(self, job_id, algorithm, pareto_front, best_parameters, objectives):
        ts = datetime.now(timezone.utc).isoformat()
        await self._write_queue.put(("INSERT OR REPLACE INTO optimization_results VALUES (?, ?, ?, ?, ?, ?)",
                                     (job_id, algorithm, json.dumps(pareto_front), json.dumps(best_parameters), json.dumps(objectives), ts)))

    async def _periodic_batch_flusher(self):
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.batch_interval)
                await self.flush()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("batch_flusher_error", error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def flush(self):
        batch = []
        while not self._write_queue.empty():
            batch.append(self._write_queue.get_nowait())
        while self._retry_queue:
            batch.append(self._retry_queue.popleft())
        if not batch:
            return

        async def _write_batch():
            if HAS_AIOSQLITE:
                async with aiosqlite.connect(self.db_path) as db:
                    for query, params in batch:
                        await db.execute(query, params)
                    await db.commit()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._sync_batch_write, batch)

        try:
            await self._safe_db_operation(_write_batch)
            for _ in batch:
                if self._write_queue.qsize() > 0:
                    self._write_queue.task_done()
        except Exception as e:
            logger.error("persistence_flush_failed", error=str(e))
            for item in batch:
                self._retry_queue.append(item)
            if len(self._retry_queue) > 1000:
                self._retry_queue.popleft()

    def _sync_batch_write(self, batch):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for query, params in batch:
                cursor.execute(query, params)
            conn.commit()

    async def close(self):
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush()

    async def health_check(self):
        try:
            await self._safe_db_operation(self._test_connection)
            status = "ok"
        except Exception as e:
            status = "failed"
            logger.error("persistence_health_check_failed", error=str(e))
        return {"status": status, "circuit_breaker": self._circuit.state_value,
                "queue_size": self._write_queue.qsize(), "retry_queue_size": len(self._retry_queue),
                "schema_version": self._schema_version}

    def _test_connection(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("SELECT 1").fetchone()

# =====================================================================
# EVENT BROKER
# =====================================================================
@dataclass(order=True)
class BioEvent:
    priority: int
    event_type: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc), compare=False)

class EventBroker:
    def __init__(self, worker_count=4, queue_maxsize=1000):
        self.worker_count = worker_count
        self._queue = asyncio.PriorityQueue(maxsize=queue_maxsize)
        self._subscribers = {}
        self._workers = []
        self._running = False
        self._lock = asyncio.Lock()

    async def subscribe(self, event_type, callback):
        async with self._lock:
            self._subscribers.setdefault(event_type, []).append(callback)

    async def publish(self, event):
        await self._queue.put(event)

    async def start(self):
        self._running = True
        for i in range(self.worker_count):
            self._workers.append(asyncio.create_task(self._worker_loop(i)))

    async def _worker_loop(self, worker_id):
        while self._running or not self._queue.empty():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                async with self._lock:
                    handlers = list(self._subscribers.get(event.event_type, []))
                for handler in handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler(event)
                        else:
                            handler(event)
                    except Exception as err:
                        logger.error("event_handler_error", worker=worker_id, cid=event.correlation_id, error=str(err))
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("worker_loop_error", worker=worker_id, error=str(e))

    async def shutdown(self):
        self._running = False
        await self._queue.join()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

# =====================================================================
# ANOMALY DETECTION
# =====================================================================
@dataclass
class AnomalyDetectionResult:
    is_anomaly: bool
    score: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class AnomalyDetector:
    def __init__(self, sensitivity=0.05, buffer_size=10000, n_estimators=100, max_samples='auto', contamination=0.05):
        self.sensitivity = sensitivity
        self.buffer_size = buffer_size
        self._lock = asyncio.Lock()
        self._data_buffer = deque(maxlen=buffer_size)
        self.model = IsolationForest(n_estimators=n_estimators, max_samples=max_samples, contamination=contamination, random_state=42) if HAS_SKLEARN else None
        self._is_trained = False
        self._prediction_history = deque(maxlen=100)
        self._actual_anomaly_flags = deque(maxlen=100)
        self._retrain_count = 0

    async def add_observation(self, features):
        async with self._lock:
            self._data_buffer.append(features)

    async def record_prediction(self, predicted_anomaly, actual_anomaly=None):
        async with self._lock:
            self._prediction_history.append(predicted_anomaly)
            if actual_anomaly is not None:
                self._actual_anomaly_flags.append(actual_anomaly)

    async def retrain(self):
        async with self._lock:
            if not HAS_SKLEARN or len(self._data_buffer) < 10:
                return False
            loop = asyncio.get_running_loop()
            data = list(self._data_buffer)
            await loop.run_in_executor(None, self.model.fit, data)
            self._is_trained = True
            self._retrain_count += 1
            return True

    async def predict(self, features):
        if not HAS_SKLEARN or not self._is_trained:
            return AnomalyDetectionResult(is_anomaly=False, score=0.0)
        try:
            score = self.model.decision_function([features])[0]
            return AnomalyDetectionResult(is_anomaly=score < 0, score=score)
        except Exception as e:
            logger.error("anomaly_prediction_failed", error=str(e))
            return AnomalyDetectionResult(is_anomaly=False, score=0.0)

    async def check_drift(self, config):
        return False  # placeholder

    def get_stats(self):
        return {"trained": self._is_trained, "buffer_size": len(self._data_buffer),
                "retrain_count": self._retrain_count, "has_sklearn": HAS_SKLEARN}

# =====================================================================
# TOKEN CACHE
# =====================================================================
class TokenCache:
    def __init__(self, ttl_seconds=60):
        self._cache = {}
        self._lock = asyncio.Lock()
        self.ttl = timedelta(seconds=ttl_seconds)

    async def get(self, entity_id):
        async with self._lock:
            if entity_id in self._cache:
                balance, expiry = self._cache[entity_id]
                if datetime.now(timezone.utc) < expiry:
                    return balance
                else:
                    del self._cache[entity_id]
        return None

    async def set(self, entity_id, balance):
        async with self._lock:
            self._cache[entity_id] = (balance, datetime.now(timezone.utc) + self.ttl)

    async def clear(self):
        async with self._lock:
            self._cache.clear()

# =====================================================================
# OPTIMIZATION (NSGA-II) & MODP
# =====================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    parameters: Dict[str, Any]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

class NSGAIIOptimizer:
    def __init__(self, evaluate_func, parameter_bounds, population_size=20, generations=5,
                 mutation_rate=0.2, crossover_rate=0.8, tournament_size=3,
                 objective_weights=None, dynamic_weights=True):
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front = []
        self._eval_cache = {}
        self._all_points = []

    def _random_individual(self):
        return {name: random.uniform(low, high) for name, (low, high) in self.parameter_bounds.items()}

    def _crossover(self, p1, p2):
        child = {}
        for name in self.parameter_bounds:
            if random.random() < 0.5:
                low, high = self.parameter_bounds[name]
                u = random.random()
                beta = (2*u)**(1/21) if u <= 0.5 else (1/(2*(1-u)))**(1/21)
                val = 0.5*((1+beta)*p1[name] + (1-beta)*p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = (2*u)**(1/21)-1 if u < 0.5 else 1-(2*(1-u))**(1/21)
                mutant[name] = max(low, min(high, mutant[name] + delta*(high-low)))
        return mutant

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            for j, q in enumerate(points):
                if i == j: continue
                p_obj = p.objectives; q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1
            if domination_count[id(p)] == 0:
                if not fronts: fronts.append([])
                fronts[0].append(p)
        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0: next_front.append(q)
            if next_front: fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front):
        if not front: return {}
        distances = {id(p): 0.0 for p in front}
        obj_keys = list(front[0].objectives.keys())
        for obj in obj_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]; obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min: continue
            for i in range(1, len(sorted_front)-1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population, fronts, crowding):
        candidates = random.sample(population, self.tournament_size)
        point_map = {id(ind): point for ind, point in zip(population, self._all_points)}
        best = candidates[0]; best_rank = float('inf'); best_crowding = -float('inf')
        for cand in candidates:
            point = point_map.get(id(cand))
            if not point: continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi; break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand; best_rank = rank; best_crowding = cd
        return best

    def _compute_dynamic_weights(self):
        return self.objective_weights.copy()

    def _select_best_from_pareto(self, pareto, weights):
        if not pareto: return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k]-min_vals[k] if max_vals[k]!=min_vals[k] else 1 for k in obj_keys}
        best = None; best_score = -float('inf')
        for p in pareto:
            score = sum(weights[k]*(p.objectives[k]-min_vals[k])/ranges[k] for k in obj_keys)
            p.scalarised_score = score
            if score > best_score: best_score = score; best = p
        return best

    async def evolve(self):
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            point = MOPDPoint(policy_id=str(uuid.uuid4()), parameters=ind, objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj
        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts: crowding.update(self._crowding_distance(front))
            offspring = []
            while len(offspring) < self.population_size:
                p1 = self._tournament_selection(population, fronts, crowding)
                p2 = self._tournament_selection(population, fronts, crowding)
                child = self._crossover(p1, p2) if random.random() < self.crossover_rate else copy.deepcopy(p1)
                child = self._mutate(child)
                offspring.append(child)
            child_points = []
            for ind in offspring:
                key = tuple(sorted(ind.items()))
                if key in self._eval_cache: obj = self._eval_cache[key]
                else: obj = await self.evaluate_func(ind); self._eval_cache[key] = obj
                point = MOPDPoint(policy_id=str(uuid.uuid4()), parameters=ind, objectives=obj)
                child_points.append(point)
            combined_inds = population + offspring
            combined_points = points + child_points
            unique = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items())); unique[key] = (ind, p)
            population = [v[0] for v in unique.values()]
            points = [v[1] for v in unique.values()]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            new_pop = []; new_points = []
            for front in fronts:
                if len(new_pop)+len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p: new_pop.append(ind); new_points.append(p); break
                else:
                    crowd = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowd.get(id(x),0), reverse=True)
                    for p in sorted_front:
                        if len(new_pop) >= self.population_size: break
                        for ind, p2 in zip(population, points):
                            if p2 is p: new_pop.append(ind); new_points.append(p); break
            population = new_pop[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            if fronts: self.pareto_front = fronts[0]
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.parameters
            self.best_fitness = best.scalarised_score
        return self.pareto_front

class OptimizationManager:
    def __init__(self, core):
        self.core = core
        self.config = core.config
        self._lock = asyncio.Lock()
        self._task = None

    async def start(self):
        if self.config.optimization_enabled:
            self._task = asyncio.create_task(self._run_periodic_optimization())

    async def stop(self):
        if self._task: self._task.cancel(); await asyncio.gather(self._task, return_exceptions=True)

    async def _run_periodic_optimization(self):
        while True:
            try:
                await asyncio.sleep(self.config.optimization_interval_sec)
                await self.run_optimization_once()
            except asyncio.CancelledError: break
            except Exception as e: logger.error("optimization_cycle_failed", error=str(e)); await asyncio.sleep(60)

    async def run_optimization_once(self):
        if not self.config.optimization_enabled: return {"status":"disabled"}
        bounds = {
            'circuit_breaker_threshold': (3,10),
            'circuit_breaker_recovery_time': (10.0,120.0),
            'retrain_interval_sec': (300.0,7200.0),
            'anomaly_sensitivity': (0.01,0.2),
            'isolation_forest_contamination': (0.01,0.1),
        }
        async def evaluate(params):
            gradient_eff = random.uniform(0.5,1.0)
            token_eff = random.uniform(0.5,1.0)
            anomaly_score = max(0.0, min(1.0, 1.0 - params['anomaly_sensitivity']*2))
            return {'gradient_efficiency':gradient_eff, 'token_balance_efficiency':token_eff, 'anomaly_score':anomaly_score}
        optimizer = NSGAIIOptimizer(evaluate, bounds, population_size=self.config.optimization_population_size,
                                    generations=self.config.optimization_generations,
                                    mutation_rate=self.config.optimization_mutation_rate,
                                    crossover_rate=self.config.optimization_crossover_rate,
                                    tournament_size=3, objective_weights=self.config.optimization_objective_weights,
                                    dynamic_weights=self.config.optimization_dynamic_weights)
        pareto = await optimizer.evolve()
        if not pareto: return {"status":"no_solution"}
        best = optimizer.best_individual
        async with self._lock:
            self.config.circuit_breaker_threshold = int(best['circuit_breaker_threshold'])
            self.config.circuit_breaker_recovery_time = best['circuit_breaker_recovery_time']
            self.config.retrain_interval_sec = best['retrain_interval_sec']
            self.config.anomaly_sensitivity = best['anomaly_sensitivity']
            self.config.isolation_forest_contamination = best['isolation_forest_contamination']
            logger.info("applied_optimized_parameters", parameters=best)
        job_id = str(uuid.uuid4())
        await self.core.persistence.save_optimization_result(job_id, 'nsga2', [p.to_dict() for p in pareto], best, optimizer.best_fitness)
        return {"status":"ok", "job_id":job_id, "pareto_front_size":len(pareto), "best_parameters":best}

# =====================================================================
# NEW ENHANCEMENT MODULES
# =====================================================================
class QuantumDistillationModule:
    def __init__(self, config): self.config=config; self.available=False
    async def optimize(self, parameters):
        for k in parameters:
            if isinstance(parameters[k], (int,float)): parameters[k]+=random.uniform(-0.01,0.01)
        return parameters
    def is_available(self): return self.available

class CausalRLAgent:
    def __init__(self, state_dim, action_dim, causal_mask=None):
        self.state_dim=state_dim; self.action_dim=action_dim; self.causal_mask=causal_mask
        self.q_table=defaultdict(lambda: np.zeros(action_dim)); self.epsilon=0.1; self.lr=0.1; self.gamma=0.99
    def act(self, state, explore=True):
        if explore and random.random()<self.epsilon: return random.randrange(self.action_dim)
        return int(np.argmax(self.q_table[tuple(state)]))
    def update(self, state, action, reward, next_state, done):
        s=tuple(state); n=tuple(next_state); best=0 if done else np.max(self.q_table[n])
        self.q_table[s][action]+=self.lr*(reward+self.gamma*best-self.q_table[s][action])
    def get_policy_probs(self, state, temperature=1.0):
        q=self.q_table[tuple(state)]
        if temperature<=0: probs=np.zeros_like(q); probs[np.argmax(q)]=1; return probs.tolist()
        exp=np.exp((q-np.max(q))/temperature); return (exp/exp.sum()).tolist()

class FederatedCoordinator:
    def __init__(self, core, queue=None): self.core=core; self.queue=queue
    async def send_update(self):
        if not self.queue: return
        model={'mopd_weights':self.core.config.optimization_objective_weights,
               'rl_q_table':{str(k):v.tolist() for k,v in self.core.causal_rl_agent.q_table.items()} if self.core.causal_rl_agent else {}}
        await self.queue.publish("federated_updates", json.dumps(model))
    async def receive_global_model(self, model_json):
        model=json.loads(model_json)
        if 'mopd_weights' in model:
            local=self.core.config.optimization_objective_weights; g=model['mopd_weights']
            for k in local: local[k]=0.5*local[k]+0.5*g.get(k,local[k])
            total=sum(local.values()); self.core.config.optimization_objective_weights={k:v/total for k,v in local.items()}
        if 'rl_q_table' in model and self.core.causal_rl_agent:
            gq=model['rl_q_table']
            for sk,qvals in gq.items():
                try: key=tuple(map(float,sk.strip('()').split(','))) if ',' in sk else (float(sk),)
                except: continue
                if key in self.core.causal_rl_agent.q_table:
                    self.core.causal_rl_agent.q_table[key]=0.5*self.core.causal_rl_agent.q_table[key]+0.5*np.array(qvals)
                else: self.core.causal_rl_agent.q_table[key]=np.array(qvals)

class SafetyMonitor:
    def __init__(self): self.invariants=[]
    def add_invariant(self, name, fn, desc): self.invariants.append((name,fn,desc))
    def check(self, state): return [f"{n}: {d}" for n,fn,d in self.invariants if not fn(state)]

class PrecisionController:
    def __init__(self, policy="energy_aware"): self.policy=policy
    def get_precision(self, load, energy_budget):
        return "float16" if self.policy=="energy_aware" and (load>0.8 or energy_budget<0.2) else "float32"

class CarbonMarketClient:
    def __init__(self, provider_url=None, contract_address=None, private_key=None):
        self.available=bool(provider_url and contract_address and private_key)
    def buy_credits(self, amount): logger.info(f"Sim buy {amount}"); return self.available
    def sell_credits(self, amount): logger.info(f"Sim sell {amount}"); return self.available

class ChaosInjector:
    def __init__(self, core, prob=0.01): self.core=core; self.prob=prob
    async def maybe_inject_failure(self):
        if random.random()<self.prob:
            action=random.choice(['kill_task','delay','corrupt_state'])
            logger.warning(f"Chaos: {action}")
            if action=='kill_task': logger.warning("Chaos would kill a task")
            elif action=='delay': await asyncio.sleep(random.uniform(0.5,2))
            elif action=='corrupt_state':
                key=random.choice(['circuit_breaker_threshold','retrain_interval_sec'])
                setattr(self.core.config,key,getattr(self.core.config,key)*random.uniform(0.8,1.2))

class HumanApprovalHandler:
    def __init__(self, queue=None): self.queue=queue
    async def request_approval(self, decision, timeout=60):
        if not self.queue: logger.warning("No queue; auto-approve"); return True
        logger.info(f"Approval requested for {decision.get('action')}, auto-approving")
        await asyncio.sleep(0); return True

# =====================================================================
# MAIN CORE
# =====================================================================
class BioGreenAgentCore:
    def __init__(self, config=None, token_service=None, gradient_service=None, message_queue=None):
        self.config = config or BioCoreConfig()
        self.token_service = token_service
        self.gradient_service = gradient_service
        self.message_queue = message_queue

        self._token_circuit = CircuitBreaker("token_service", self.config.circuit_breaker_threshold, self.config.circuit_breaker_recovery_time)
        self._gradient_circuit = CircuitBreaker("gradient_service", self.config.circuit_breaker_threshold, self.config.circuit_breaker_recovery_time)
        self.persistence = Persistence(self.config.db_path, self.config.batch_write_interval_sec,
                                       self.config.persistence_circuit_breaker_threshold,
                                       self.config.persistence_circuit_breaker_recovery_time)
        self.event_broker = EventBroker(self.config.event_worker_count, queue_maxsize=self.config.event_queue_maxsize)
        self.anomaly_detector = AnomalyDetector(sensitivity=self.config.anomaly_sensitivity,
                                                buffer_size=self.config.anomaly_buffer_size,
                                                n_estimators=self.config.isolation_forest_n_estimators,
                                                max_samples=self.config.isolation_forest_max_samples,
                                                contamination=self.config.isolation_forest_contamination)
        self._token_cache = TokenCache()
        self._retrain_task = None
        self._metrics = None
        self._setup_metrics()
        self.optimization_manager = OptimizationManager(self)

        # New modules
        self.quantum_distillation = QuantumDistillationModule(self.config) if self.config.enable_quantum_distillation else None
        if self.config.enable_causal_rl: self.causal_rl_agent = CausalRLAgent(state_dim=10, action_dim=3)
        else: self.causal_rl_agent = None
        self.federated_coordinator = FederatedCoordinator(self, self.message_queue) if self.config.enable_federated else None
        self.safety_monitor = SafetyMonitor() if self.config.enable_safety_monitor else None
        if self.safety_monitor: self._setup_safety_invariants()
        self.precision_controller = PrecisionController() if self.config.enable_precision else None
        self.carbon_market = None
        if self.config.enable_carbon_market and self.config.carbon_market_config:
            self.carbon_market = CarbonMarketClient(**self.config.carbon_market_config)
        self.chaos_injector = ChaosInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None
        self.human_approval = HumanApprovalHandler(self.message_queue) if self.config.enable_human_approval else None

    def _setup_metrics(self):
        if HAS_PROMETHEUS and self.config.prometheus_port:
            start_http_server(self.config.prometheus_port)
            self._metrics = {
                'circuit_breaker_state': Gauge('bio_circuit_breaker_state', 'Circuit breaker state', ['name']),
                'event_queue_size': Gauge('bio_event_queue_size', 'Event queue size'),
                'anomalies_total': Counter('bio_anomalies_total', 'Total anomalies detected'),
                'telemetry_processed_total': Counter('bio_telemetry_processed_total', 'Total telemetry processed'),
                'persistence_queue_size': Gauge('bio_persistence_queue_size', 'Persistence write queue size'),
                'retrain_count': Counter('bio_retrain_count', 'Retrain count'),
                'processing_seconds': Histogram('bio_processing_seconds', 'Processing time for telemetry'),
                'token_balance': Gauge('bio_token_balance', 'Token balance for entity', ['entity_id']),
                'optimization_pareto_size': Gauge('bio_optimization_pareto_size', 'Pareto front size from last optimization'),
            }
        else:
            self._metrics = None

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant("circuit_breaker_threshold_positive",
                                          lambda s: s['circuit_breaker_threshold'] >= 1,
                                          "Circuit breaker threshold must be at least 1")
        self.safety_monitor.add_invariant("retrain_interval_reasonable",
                                          lambda s: s['retrain_interval_sec'] >= 60,
                                          "Retrain interval too small")

    async def initialize(self):
        await self.persistence.initialize()
        await self.event_broker.start()
        self._retrain_task = asyncio.create_task(self._periodic_retrainer())
        await self.optimization_manager.start()
        if self.federated_coordinator:
            self._federated_task = asyncio.create_task(self._federated_loop())
        if self.chaos_injector:
            self._chaos_task = asyncio.create_task(self._chaos_loop())

    async def _periodic_retrainer(self):
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.config.retrain_interval_sec)
                success = await self.anomaly_detector.retrain()
                if success and self._metrics: self._metrics['retrain_count'].inc()
                if self.config.adaptive_retraining_enabled and await self.anomaly_detector.check_drift(self.config):
                    await self.anomaly_detector.retrain()
                backoff = 1.0
            except asyncio.CancelledError: break
            except Exception as e:
                logger.error("retrainer_error", error=str(e))
                await asyncio.sleep(backoff); backoff = min(backoff*2, 60.0)

    async def _federated_loop(self):
        while True:
            await asyncio.sleep(300)
            if self.federated_coordinator: await self.federated_coordinator.send_update()

    async def _chaos_loop(self):
        while True:
            await asyncio.sleep(60)
            if self.chaos_injector: await self.chaos_injector.maybe_inject_failure()

    async def process_telemetry(self, telemetry_data, correlation_id=None):
        start_time = time.time()
        cid = correlation_id or str(uuid.uuid4())
        local_logger = logger.bind(cid=cid) if HAS_STRUCTLOG else logger

        gradient = 0.0
        if self.gradient_service:
            try:
                gradient = await self._gradient_circuit.call(self.gradient_service.compute_gradient_field, telemetry_data, cid)
            except Exception as e:
                local_logger.error("gradient_circuit_failed", cid=cid, error=str(e))

        if self.safety_monitor:
            state = {'circuit_breaker_threshold': self.config.circuit_breaker_threshold,
                     'retrain_interval_sec': self.config.retrain_interval_sec}
            violations = self.safety_monitor.check(state)
            if violations: local_logger.warning("Safety violations", violations=violations)

        if gradient > self.config.proton_gradient_max:
            alert_id = f"alert_{cid}"
            await self.persistence.enqueue_alert(alert_id, "HIGH", f"Gradient {gradient} exceeded max", AlertStatus.ACTIVE.value, cid)
            local_logger.warning("gradient_threshold_exceeded", cid=cid, gradient=gradient)

        token_balance = None
        if self.token_service:
            try:
                cached = await self._token_cache.get(cid)
                if cached is not None: token_balance = cached
                else:
                    token_balance = await self._token_circuit.call(self.token_service.get_balance, cid, cid)
                    await self._token_cache.set(cid, token_balance)
                if self._metrics: self._metrics['token_balance'].labels(entity_id=cid).set(token_balance)
                if token_balance < self.config.atp_token_threshold:
                    await self.persistence.enqueue_alert(f"token_low_{cid}", "WARNING", f"Token balance {token_balance} below threshold", AlertStatus.ACTIVE.value, cid)
            except Exception as e:
                local_logger.error("token_service_failed", cid=cid, error=str(e))

        features = []
        if "energy_usage" in telemetry_data: features.append(float(telemetry_data["energy_usage"]))
        if "temperature" in telemetry_data: features.append(float(telemetry_data["temperature"]))
        if features:
            await self.anomaly_detector.add_observation(features)
            result = await self.anomaly_detector.predict(features)
            if result.is_anomaly and self._metrics: self._metrics['anomalies_total'].inc()
            await self.anomaly_detector.record_prediction(result.is_anomaly)

        if self._metrics:
            self._metrics['telemetry_processed_total'].inc()
            self._metrics['event_queue_size'].set(self.event_broker._queue.qsize())
            self._metrics['persistence_queue_size'].set(self.persistence._write_queue.qsize())
            self._metrics['circuit_breaker_state'].labels(name='token_service').set(self._token_circuit.get_state_numeric())
            self._metrics['circuit_breaker_state'].labels(name='gradient_service').set(self._gradient_circuit.get_state_numeric())

        await self.event_broker.publish(BioEvent(priority=1, event_type="telemetry_processed",
                                                payload={"gradient": gradient, "telemetry": telemetry_data, "token_balance": token_balance},
                                                correlation_id=cid))

        if self._metrics: self._metrics['processing_seconds'].observe(time.time() - start_time)

        if self.config.enable_xai:
            local_logger.info("XAI", explanation=self.explain_decision('telemetry', {'cid': cid, 'gradient': gradient}))

        return {"status": "ok", "correlation_id": cid, "gradient": gradient, "token_balance": token_balance}

    def explain_decision(self, decision_type, context=None):
        if decision_type == 'telemetry':
            return f"Processed telemetry with correlation ID {context.get('cid')} and gradient {context.get('gradient')}"
        return "Decision made by system."

    async def update_cost_benefit_model(self, model_id, cost, benefit, correlation_id):
        net = benefit - cost; roi = net/cost if cost > 0 else 0.0
        await self.persistence.enqueue_cost_benefit(model_id, cost, benefit, roi, correlation_id)
        return {"cost": cost, "benefit": benefit, "roi": roi}

    async def health_check(self):
        persistence_health = await self.persistence.health_check()
        return {
            "status": "healthy" if persistence_health['status'] == 'ok' else "degraded",
            "version": "8.3.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "circuits": {"token_service": self._token_circuit.state_value,
                         "gradient_service": self._gradient_circuit.state_value,
                         "persistence": persistence_health['circuit_breaker']},
            "persistence": persistence_health,
            "event_broker": {"workers": self.config.event_worker_count, "queue_size": self.event_broker._queue.qsize()},
            "anomaly_detector": self.anomaly_detector.get_stats(),
            "optimization": {"enabled": self.config.optimization_enabled,
                             "interval": self.config.optimization_interval_sec,
                             "population_size": self.config.optimization_population_size,
                             "generations": self.config.optimization_generations},
            "has_sqlite": HAS_AIOSQLITE,
            "has_sklearn": HAS_SKLEARN,
            "enhancements": {
                "quantum_distillation": self.quantum_distillation is not None,
                "causal_rl": self.causal_rl_agent is not None,
                "federated": self.federated_coordinator is not None,
                "safety_monitor": self.safety_monitor is not None,
                "xai": self.config.enable_xai,
                "precision": self.precision_controller is not None,
                "carbon_market": self.carbon_market is not None,
                "chaos": self.chaos_injector is not None,
                "human_approval": self.human_approval is not None,
            }
        }

    async def shutdown(self):
        if self._retrain_task: self._retrain_task.cancel()
        await self.event_broker.shutdown()
        await self.persistence.close()
        await self.optimization_manager.stop()
        for task in [getattr(self, '_federated_task', None), getattr(self, '_chaos_task', None)]:
            if task and not task.done():
                task.cancel(); await asyncio.gather(task, return_exceptions=True)
        logger.info("bio_green_agent_shutdown_complete")

__all__ = [
    "BioCoreConfig", "BioGreenAgentCore", "CircuitBreaker", "CircuitState", "Persistence",
    "EventBroker", "BioEvent", "AnomalyDetector", "AnomalyDetectionResult",
    "TokenServiceProtocol", "GradientServiceProtocol", "AlertStatus", "NSGAIIOptimizer",
    "OptimizationManager", "MOPDPoint", "QuantumDistillationModule", "CausalRLAgent",
    "FederatedCoordinator", "SafetyMonitor", "PrecisionController", "CarbonMarketClient",
    "ChaosInjector", "HumanApprovalHandler",
]
