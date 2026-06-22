# File: carbon_nas_enhanced_v5_production.py
"""
Enhanced Carbon NAS v5.0.0 - Production Ready
Integrated with reliability features from v12.0
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import copy
import hashlib
import json
import math
from collections import defaultdict, deque
import time
import uuid
from pathlib import Path
from contextlib import contextmanager

# Add production dependencies
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
import aiosqlite  # Lightweight async SQLite

logger = logging.getLogger(__name__)

# ============================================================
# ADDED: PRODUCTION RELIABILITY COMPONENTS
# ============================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class ProductionCircuitBreaker:
    """Circuit breaker for worker failures (imported from v12.0)"""
    
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
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
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
            
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
        
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
            self.failure_count = 0
    
    async def _record_failure(self):
        async with self._lock:
            self.metrics['failed_calls'] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN

class ProductionDatabase:
    """Async database for persistence (adapted from v12.0)"""
    
    def __init__(self, db_path: Path = Path("./carbon_nas_production.db")):
        self.db_path = db_path
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS architectures (
                arch_id TEXT PRIMARY KEY,
                config_json TEXT,
                accuracy REAL,
                carbon_kg REAL,
                token_efficiency REAL,
                created_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS evolution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                generation INTEGER,
                best_fitness REAL,
                population_size INTEGER,
                carbon_spent REAL,
                created_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_packages (
                package_id TEXT PRIMARY KEY,
                config_json TEXT,
                survival_score REAL,
                domain_tags TEXT,
                created_at TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    async def save_architecture(self, gene: 'ArchitectureGene'):
        """Save architecture evaluation result"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR REPLACE INTO architectures 
                   (arch_id, config_json, accuracy, carbon_kg, token_efficiency, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (gene.config.compute_hash(),
                 json.dumps(gene.config.to_dict()),
                 gene.fitness.accuracy,
                 gene.fitness.carbon_kg,
                 gene.fitness.token_efficiency,
                 datetime.now().isoformat())
            )
            await db.commit()
    
    async def save_evolution_step(self, generation: int, best_fitness: float, 
                                   population_size: int, carbon_spent: float):
        """Save evolution history"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO evolution_history 
                   (generation, best_fitness, population_size, carbon_spent, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (generation, best_fitness, population_size, carbon_spent, datetime.now().isoformat())
            )
            await db.commit()
    
    async def get_best_architectures(self, limit: int = 10) -> List[Dict]:
        """Get best architectures from history"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """SELECT * FROM architectures 
                   ORDER BY accuracy DESC LIMIT ?""",
                (limit,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

# ============================================================
# ENHANCED MAIN CLASS WITH PRODUCTION FEATURES
# ============================================================

class EnhancedCarbonNAS:
    """Enhanced Carbon NAS v5.0.0 with production features"""
    
    def __init__(
        self, 
        expert_registry=None, 
        token_manager=None, 
        gradient_manager=None,
        knowledge_transfer=None, 
        population_size: int = 30, 
        max_generations: int = 50,
        carbon_budget_kg: float = 10.0, 
        ecoatp_budget: float = 1000.0,
        auto_register: bool = True, 
        enable_continuous: bool = True,
        # New production parameters
        enable_persistence: bool = True,
        enable_circuit_breakers: bool = True,
        db_path: Path = Path("./carbon_nas_production.db")
    ):
        # Original parameters
        self.expert_registry = expert_registry
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.knowledge_transfer = knowledge_transfer
        self.population_size = population_size
        self.max_generations = max_generations
        self.carbon_budget_kg = carbon_budget_kg
        self.ecoatp_budget = ecoatp_budget
        self.auto_register = auto_register
        self.enable_continuous = enable_continuous
        
        # New production components
        self.enable_persistence = enable_persistence
        self.enable_circuit_breakers = enable_circuit_breakers
        self.db = ProductionDatabase(db_path) if enable_persistence else None
        
        # Circuit breakers for different operations
        self.circuit_breakers = {
            'evaluation': ProductionCircuitBreaker('evaluation'),
            'registration': ProductionCircuitBreaker('registration'),
            'knowledge_capture': ProductionCircuitBreaker('knowledge_capture')
        } if enable_circuit_breakers else {}
        
        # Original state
        self.population: List[ArchitectureGene] = []
        self.generation = 0
        self.evolution_history: List[Dict] = []
        self.total_carbon_spent_kg = 0.0
        self.total_ecoatp_spent = 0.0
        self.best_by_accuracy: Optional[ArchitectureGene] = None
        self.best_by_carbon: Optional[ArchitectureGene] = None
        self.best_by_token: Optional[ArchitectureGene] = None
        
        # Health metrics
        self.health_status = {'status': 'initializing', 'last_check': datetime.now()}
        self._shutdown_event = asyncio.Event()
        
        # Search space (unchanged)
        self.search_space = {
            'families': ['cnn', 'transformer', 'efficientnet', 'mobilenet', 'resnet', 'vit', 'hybrid'],
            'num_layers': list(range(2, 21, 2)),
            'hidden_dim': [64, 128, 192, 256, 384, 512, 640, 768, 1024],
            'num_heads': [2, 4, 6, 8, 10, 12, 16],
            'pruning_rates': [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
            'quantization_bits': [32, 16, 8],
            'hardware_targets': ['cpu_x86', 'cpu_arm', 'gpu_nvidia', 'edge_tpu', 'mobile_npu']
        }
        
        self._initialize_population()
        if self.enable_continuous:
            asyncio.create_task(self._continuous_loop())
        
        logger.info(f"Enhanced Carbon NAS v5.0.0 initialized: pop={population_size}, persistence={enable_persistence}")
    
    # ... [All original methods remain the same: _initialize_population, _get_gradient_pressure, 
    # _allocate_budget, _update_bests, _register_best, _evolve_population, _crossover, 
    # _mutate, _continuous_loop, _lightweight_eval, _summary, get_nas_stats]
    
    # ADDED: Health check method from v12.0
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        try:
            health = {
                'healthy': True,
                'instance_id': id(self),
                'generation': self.generation,
                'population_size': len(self.population),
                'total_carbon_spent': self.total_carbon_spent_kg,
                'best_accuracy': self.best_by_accuracy.fitness.accuracy if self.best_by_accuracy else 0,
                'queue_size': 0,  # Not applicable in v5
                'circuit_breakers': {
                    name: cb.get_metrics()['state'] 
                    for name, cb in self.circuit_breakers.items()
                } if self.enable_circuit_breakers else {},
                'timestamp': datetime.now().isoformat()
            }
            
            # Check database health
            if self.enable_persistence and self.db:
                try:
                    async with aiosqlite.connect(self.db.db_path) as conn:
                        await conn.execute("SELECT 1")
                    health['database_healthy'] = True
                except Exception as e:
                    health['database_healthy'] = False
                    health['healthy'] = False
                    health['database_error'] = str(e)
            
            self.health_status = health
            return health
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {'healthy': False, 'error': str(e)}
    
    # ADDED: Enhanced evolve with circuit breaker protection
    async def evolve_with_retry(self, fitness_function: Callable, 
                               generations: int = None,
                               patience: int = 10) -> Dict[str, Any]:
        """Evolve with circuit breaker protection and retry logic"""
        
        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
        async def _evolve_with_retry():
            # Use circuit breaker for the entire evolution
            if self.enable_circuit_breakers:
                return await self.circuit_breakers['evaluation'].call(
                    self.evolve, fitness_function, generations, patience
                )
            else:
                return await self.evolve(fitness_function, generations, patience)
        
        try:
            return await _evolve_with_retry()
        except Exception as e:
            logger.error(f"Evolution failed after retries: {e}")
            return {'error': str(e)}
    
    # ADDED: Graceful shutdown
    async def shutdown(self):
        """Graceful shutdown with cleanup"""
        logger.info("Shutting down Enhanced Carbon NAS...")
        self._shutdown_event.set()
        
        # Cancel continuous loop
        if hasattr(self, '_continuous_task'):
            self._continuous_task.cancel()
        
        # Save final state if persistence enabled
        if self.enable_persistence and self.db:
            try:
                # Save best architectures
                for gene in self.population[:10]:  # Save top 10
                    await self.db.save_architecture(gene)
                
                # Save evolution history
                if self.evolution_history:
                    last = self.evolution_history[-1]
                    await self.db.save_evolution_step(
                        last['generation'],
                        last['best_fitness'],
                        self.population_size,
                        self.total_carbon_spent_kg
                    )
                logger.info("Final state saved to database")
            except Exception as e:
                logger.error(f"Error saving final state: {e}")
        
        logger.info("Shutdown complete")
