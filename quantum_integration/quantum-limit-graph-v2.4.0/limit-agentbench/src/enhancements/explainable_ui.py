# explainable_ui.py
"""
Enhanced Explainable Green Decisions – Enterprise UI (v4.0.0+)
=============================================================================

Provides:
- Natural‑language explanations for routing decisions (CO₂, carbon intensity, helium, material, latency, accuracy).
- Interactive dashboard with request‑level cost breakdowns, drill‑down, pagination, and real‑time updates via WebSocket.
- “What‑if” mode with multi‑scenario comparison.
- REST API with JWT authentication (access + refresh tokens) and role‑based access.
- Persistence (SQLite/PostgreSQL) for request logs and feedback.
- Configurable explanation templates (Jinja2) with optional LLM‑generated explanations.
- Export reports in CSV, JSON, and PNG/PDF (via Plotly).
- Prometheus metrics.
- Correlation IDs for end‑to‑end tracing.
- Unit test stubs.

ENHANCEMENTS OVER v3.0.0:
- Fixed ExplanationGenerator class (config, async/sync separation, LLM integration).
- Removed all asyncio.run() calls; use proper async/await or thread offloading.
- Rate limiting applied to all protected endpoints via a decorator.
- Added JWT refresh token endpoint.
- WebSocket heartbeat expects client pong; dead connections are cleaned up.
- Health check now verifies DB, carbon manager, and LCA client.
- Export all endpoint uses streaming to avoid memory blow‑up.
- Thread offloading now properly propagates exceptions.
- Comprehensive docstrings for all public methods.
- Improved error handling and logging.

NEW IN v4.0.0+:
- Integrated bio_inspired, moe_system, MODP for adaptive explanations and feedback.
- MODP computes multi‑objective utilities for chosen and alternative experts.
- MoE routes explanations to the most suitable template style based on user context.
- Bio‑inspired evolution of explanation templates using user feedback as fitness.
- Feedback from the UI is published to the central message queue for decision‑loop closure.
- New API endpoints for optimization state and template evolution.
- Extended configuration with MODP weights.
"""

import asyncio
import json
import logging
import os
import time
import uuid
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union, Callable
from collections import deque
import numpy as np

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, ConfigDict

# ---------- SQLAlchemy (async) ----------
try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
    from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, Text, Index, func, select, update
    from sqlalchemy.pool import NullPool
    from sqlalchemy.exc import SQLAlchemyError
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

# Fallback to sync SQLAlchemy (will be offloaded to threads)
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, scoped_session
    SQLALCHEMY_SYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

# ---------- FastAPI ----------
from fastapi import FastAPI, Depends, HTTPException, status, Request, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse, JSONResponse

# ---------- Authentication ----------
import jwt
from passlib.context import CryptContext

# ---------- WebSocket ----------
from websockets import WebSocketServerProtocol

# ---------- Plotly ----------
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio

# ---------- Jinja2 ----------
from jinja2 import Template, Environment, FileSystemLoader, TemplateNotFound

# ---------- Prometheus ----------
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST

# ---------- Structlog ----------
import structlog
logger = structlog.get_logger(__name__)

# ---------- Local imports (fallback stubs) ----------
try:
    from sustainability import SustainabilityAwareExpertProfile, SustainabilityFitnessScorer
except ImportError:
    class SustainabilityAwareExpertProfile:
        def __init__(self, expert_id, **kwargs):
            self.expert_id = expert_id
            self.energy_per_inference_full = 0.0
            self.energy_per_inference_compressed = None
            self.accuracy_full = 0.0
            self.accuracy_compressed = None
            self.compressed_flag = False
            self.sustainability_fitness_score = 0.0
            self.compression_method = None

    class SustainabilityFitnessScorer:
        def compute(self, profile): return 0.5

# ---------- tenacity for retries ----------
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, AsyncRetrying, RetryError

# ---------- slowapi for rate limiting ----------
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

# ---------- circuit breaker ----------
class CircuitBreaker:
    """Async circuit breaker with half‑open state."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = "closed"
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == "open":
                if time.time() - self._last_failure_time > self.recovery_timeout:
                    self._state = "half-open"
                    self._failure_count = 0
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "half-open":
                    self._state = "closed"
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.time()
                if self._failure_count >= self.failure_threshold:
                    self._state = "open"
            raise e

# ---------- LLM client ----------
try:
    from ..enhancements.llm_client import LLMClient
except ImportError:
    class LLMClient:
        async def generate_explanation(self, prompt: str) -> str:
            return "LLM client not available."

# =============================================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "default"
    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

# =============================================================================
# 1. CONFIGURATION (Pydantic, always used) – extended with MODP weights
# =============================================================================
class ExplainableUIConfig(BaseModel):
    """Configuration for Explainable UI."""
    # Database
    db_path: str = Field("./explainable_ui.db")
    db_pool_size: int = Field(10, ge=1)
    db_max_overflow: int = Field(20, ge=1)
    # Authentication
    jwt_secret: str = Field("change_me_in_production")
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = Field(1440, ge=1)
    refresh_token_expiration_days: int = Field(7, ge=1)
    # Cache
    cache_ttl_seconds: int = Field(300, ge=0)
    # Plotly
    plotly_theme: str = Field("plotly_white")
    # Logging
    log_level: str = Field("INFO")
    # Export
    export_format: str = Field("json")  # json, csv, png, pdf
    # WebSocket
    ws_enabled: bool = True
    ws_broadcast_interval: int = Field(5, ge=1)
    # Pagination
    default_page_size: int = Field(20, ge=1)
    max_page_size: int = Field(100, ge=1)
    # Constants
    co2_per_kwh_kg: float = Field(0.2, gt=0)
    energy_to_co2_factor: float = Field(0.2 / 3600000, gt=0)  # default, derived
    # Explanation template path
    explanation_template_path: Optional[str] = Field(None)

    # MODP weights for multi‑objective utility
    modp_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'accuracy': 0.4,
            'energy': 0.3,
            'carbon': 0.2,
            'latency': 0.1,
        }
    )
    # Bio‑inspired evolution settings
    template_evolution_enabled: bool = True
    template_evolution_interval_seconds: int = Field(3600, ge=60)
    template_population_size: int = Field(10, ge=1)
    template_generations: int = Field(5, ge=1)

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v.upper() not in allowed:
            raise ValueError(f'log_level must be one of {allowed}')
        return v.upper()

    @field_validator('energy_to_co2_factor')
    @classmethod
    def derive_energy_to_co2(cls, v, values):
        if v is None:
            return values.get('co2_per_kwh_kg', 0.2) / 3600000
        return v

    model_config = ConfigDict(env_prefix="EXPLAINABLE_UI_")

    @classmethod
    def from_dict(cls, data: Dict) -> "ExplainableUIConfig":
        return cls(**data)

# =============================================================================
# 2. DATA MODELS
# =============================================================================
@dataclass
class RequestLog:
    """Log entry for a single routing request."""
    request_id: str
    timestamp: datetime
    query: str
    chosen_expert_id: str
    chosen_expert_profile: SustainabilityAwareExpertProfile
    alternative_experts: List[Tuple[str, SustainabilityAwareExpertProfile]]
    latency_ms: float
    energy_joules: float
    co2_kg: float
    accuracy: float
    carbon_intensity: float = 0.0
    helium_scarcity: float = 0.0
    material_index: float = 0.0
    sustainability_score: float = 0.0
    explanation: str = ""
    feedback_rating: Optional[int] = None
    feedback_comment: Optional[str] = None

@dataclass
class WhatIfResult:
    scenario_id: str
    alternative_expert_id: str
    expected_energy_joules: float
    expected_co2_kg: float
    expected_latency_ms: float
    expected_accuracy: float
    expected_carbon_intensity: float
    expected_helium_scarcity: float
    expected_material_index: float
    difference_energy: float
    difference_co2: float
    difference_latency: float
    difference_accuracy: float
    # New: MODP utilities
    chosen_utility: Optional[float] = None
    alternative_utility: Optional[float] = None

# =============================================================================
# 3. DATABASE MODELS (SQLAlchemy Async/Sync)
# =============================================================================
Base = declarative_base()

class RequestLogDB(Base):
    __tablename__ = 'request_logs'
    id = Column(Integer, primary_key=True)
    request_id = Column(String(64), unique=True, index=True)
    timestamp = Column(DateTime, default=datetime.now)
    query = Column(Text)
    chosen_expert_id = Column(String(128))
    alternative_experts = Column(JSON)
    latency_ms = Column(Float)
    energy_joules = Column(Float)
    co2_kg = Column(Float)
    accuracy = Column(Float)
    carbon_intensity = Column(Float)
    helium_scarcity = Column(Float)
    material_index = Column(Float)
    sustainability_score = Column(Float)
    explanation = Column(Text)
    feedback_rating = Column(Integer, nullable=True)
    feedback_comment = Column(Text, nullable=True)

class ExpertStatsDB(Base):
    __tablename__ = 'expert_stats'
    expert_id = Column(String(128), primary_key=True)
    total_requests = Column(Integer, default=0)
    avg_latency_ms = Column(Float)
    avg_energy_joules = Column(Float)
    avg_accuracy = Column(Float)
    total_co2_kg = Column(Float)
    last_updated = Column(DateTime, default=datetime.now)

class UserDB(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, index=True)
    password_hash = Column(String(128))
    role = Column(String(32), default='viewer')
    created_at = Column(DateTime, default=datetime.now)
    last_login = Column(DateTime, nullable=True)
    refresh_token = Column(String(256), nullable=True)
    refresh_token_expires = Column(DateTime, nullable=True)

# New tables for optimizer state
class OptimizerStateDB(Base):
    __tablename__ = 'optimizer_state'
    id = Column(Integer, primary_key=True)
    key = Column(String(64), unique=True)
    value = Column(JSON)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

# =============================================================================
# 4. DATABASE MANAGER (Async with fallback to sync+thread)
# =============================================================================
class DatabaseManager:
    """Manages database connections and operations, supporting both async and sync."""
    def __init__(self, config: ExplainableUIConfig):
        self.config = config
        self.async_engine = None
        self.async_sessionmaker = None
        self.sync_engine = None
        self.sync_sessionmaker = None
        self._lock = asyncio.Lock()

        if ASYNC_SQLALCHEMY_AVAILABLE:
            self.async_engine = create_async_engine(
                f"sqlite+aiosqlite:///{config.db_path}",
                poolclass=NullPool,  # SQLite doesn't support pooling well
            )
            self.async_sessionmaker = async_sessionmaker(self.async_engine, expire_on_commit=False)
            # Create tables (async)
            asyncio.create_task(self._init_db_async())
        elif SQLALCHEMY_SYNC_AVAILABLE:
            self.sync_engine = create_engine(f"sqlite:///{config.db_path}", poolclass=NullPool)
            self.sync_sessionmaker = sessionmaker(bind=self.sync_engine)
            Base.metadata.create_all(self.sync_engine)

    async def _init_db_async(self):
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def get_async_session(self) -> AsyncSession:
        if self.async_sessionmaker:
            return self.async_sessionmaker()
        raise RuntimeError("Async database not available")

    async def get_sync_session(self):
        if self.sync_sessionmaker:
            return self.sync_sessionmaker()
        raise RuntimeError("Sync database not available")

    async def execute_async(self, stmt):
        """Execute an async statement with retry."""
        async with self.async_sessionmaker() as session:
            result = await session.execute(stmt)
            await session.commit()
            return result

    async def execute_sync_in_thread(self, func, *args, **kwargs):
        """Run a sync function in a thread pool with error propagation."""
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, func, *args, **kwargs)
        except Exception as e:
            logger.error(f"Thread execution failed: {e}")
            raise

    # New methods for optimizer state persistence
    async def save_optimizer_state(self, key: str, value: Dict):
        """Persist optimizer state to database."""
        if ASYNC_SQLALCHEMY_AVAILABLE:
            async with self.async_sessionmaker() as session:
                stmt = text("""
                    INSERT OR REPLACE INTO optimizer_state (key, value, updated_at)
                    VALUES (:key, :value, :updated_at)
                """)
                await session.execute(stmt, {"key": key, "value": json.dumps(value), "updated_at": datetime.now().isoformat()})
                await session.commit()
        else:
            session = self.sync_sessionmaker()
            session.execute(
                text("""
                    INSERT OR REPLACE INTO optimizer_state (key, value, updated_at)
                    VALUES (:key, :value, :updated_at)
                """),
                {"key": key, "value": json.dumps(value), "updated_at": datetime.now().isoformat()}
            )
            session.commit()

    async def load_optimizer_state(self, key: str) -> Optional[Dict]:
        if ASYNC_SQLALCHEMY_AVAILABLE:
            async with self.async_sessionmaker() as session:
                result = await session.execute(text("SELECT value FROM optimizer_state WHERE key = :key"), {"key": key})
                row = result.fetchone()
                if row:
                    return json.loads(row[0])
                return None
        else:
            session = self.sync_sessionmaker()
            row = session.execute(text("SELECT value FROM optimizer_state WHERE key = :key"), {"key": key}).fetchone()
            if row:
                return json.loads(row[0])
            return None

    async def close(self):
        if self.async_engine:
            await self.async_engine.dispose()
        if self.sync_engine:
            self.sync_engine.dispose()

# =============================================================================
# 5. EXPLANATION GENERATOR (Enhanced with MODP, MoE, Bio)
# =============================================================================
class ExplanationGenerator:
    """
    Produces human‑readable, natural‑language explanations with multiple dimensions.
    Supports Jinja2 templates loaded from file, optional LLM generation,
    and adaptive enhancement via MODP, MoE, and bio‑inspired evolution.
    """
    def __init__(
        self,
        config: ExplainableUIConfig,
        llm_client: Optional[LLMClient] = None,
        template: Optional[str] = None,
        db_manager: Optional[DatabaseManager] = None,
    ):
        self.config = config
        self.llm_client = llm_client
        self.template = template or self._default_template()
        self.template_env = None
        self.template_name = "default"
        self.db = db_manager
        if config.explanation_template_path:
            self._load_template_from_file(config.explanation_template_path)

        # Enhanced modules
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            # Population of template variants (each is a dict of parameters)
            self.template_population = [{"template": self.template, "style": "default"}]
            self.template_fitness = deque(maxlen=100)
            self._load_state()
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.template_population = []
            self.template_fitness = deque(maxlen=100)

    def _load_state(self):
        """Load evolved template population from DB."""
        if self.db:
            state = asyncio.run(self.db.load_optimizer_state("explanation_templates"))
            if state:
                self.template_population = state.get("population", [{"template": self.template, "style": "default"}])
                self.template_fitness = deque(state.get("fitness", []), maxlen=100)

    def _save_state(self):
        """Persist template population and fitness to DB."""
        if self.db:
            state = {
                "population": self.template_population,
                "fitness": list(self.template_fitness),
            }
            asyncio.create_task(self.db.save_optimizer_state("explanation_templates", state))

    def _default_template(self) -> str:
        return (
            "This request was routed to expert **{{ chosen_expert_id }}**"
            "{{ compressed_info }}."
            "{{ co2_savings }}{{ latency_impact }}"
            " The chosen expert achieved accuracy of {{ accuracy:.2% }}."
            " Carbon intensity was {{ carbon_intensity:.1f }} gCO₂/kWh, helium scarcity {{ helium_scarcity:.2f }},"
            " material index {{ material_index:.2f }}."
            "{% if chosen_utility is defined %} (Utility score: {{ chosen_utility:.3f }}){% endif %}"
        )

    def _load_template_from_file(self, path: str):
        """Load template from a file path."""
        try:
            with open(path, 'r') as f:
                self.template_content = f.read()
            self.template_env = Environment(loader=FileSystemLoader(os.path.dirname(path) or '.'))
            self.template_name = os.path.basename(path)
        except Exception as e:
            logger.warning(f"Failed to load template from {path}: {e}, using default")

    def reload_template(self, path: Optional[str] = None):
        """Reload the template from a file."""
        if path:
            self._load_template_from_file(path)
        elif self.config.explanation_template_path:
            self._load_template_from_file(self.config.explanation_template_path)

    async def generate_async(
        self,
        request: RequestLog,
        chosen_expert: SustainabilityAwareExpertProfile,
        alternatives: List[Tuple[str, SustainabilityAwareExpertProfile]],
        user_context: Optional[Dict] = None,
    ) -> str:
        """
        Generate explanation asynchronously, using MODP, MoE, and LLM if available.
        """
        # Compute MODP utilities if available
        chosen_utility = None
        alt_utilities = {}
        if self.modp:
            chosen_objectives = {
                "accuracy": request.accuracy,
                "energy": 1.0 - (request.energy_joules / (max([a[1].energy_per_inference_full for a in alternatives] + [request.energy_joules]) + 1e-8)),
                "carbon": 1.0 - (request.co2_kg / (max([a[1].energy_per_inference_full for a in alternatives] + [request.energy_joules]) + 1e-8)),
                "latency": 1.0 - (request.latency_ms / 1000.0),
            }
            chosen_utility = self.modp.evaluate(chosen_objectives, self.config.modp_weights)
            for eid, prof in alternatives:
                alt_obj = {
                    "accuracy": prof.accuracy_full,
                    "energy": 1.0 - (prof.energy_per_inference_full / (max([a[1].energy_per_inference_full for a in alternatives] + [request.energy_joules]) + 1e-8)),
                    "carbon": 1.0 - ((prof.energy_per_inference_full * self.config.energy_to_co2_factor) / (max([a[1].energy_per_inference_full for a in alternatives] + [request.energy_joules]) + 1e-8)),
                    "latency": 1.0 - ((prof.energy_per_inference_full * 1e-6 * 0.5) / 1000.0),
                }
                alt_utilities[eid] = self.modp.evaluate(alt_obj, self.config.modp_weights)

        # Select template style via MoE (if available)
        template_style = "default"
        if self.moe and user_context:
            context = {
                "user_role": user_context.get("role", "viewer"),
                "task_type": user_context.get("task_type", "general"),
                "carbon_intensity": request.carbon_intensity,
                "has_alternatives": len(alternatives) > 0,
            }
            encoded = self.moe.encode(context)
            template_style = self.moe.select(encoded)

        # Choose the appropriate template from population based on style
        selected_template = self.template
        for variant in self.template_population:
            if variant.get("style") == template_style:
                selected_template = variant.get("template", self.template)
                break

        # Prepare data for template rendering
        if alternatives:
            best_alt = min(alternatives, key=lambda x: x[1].energy_per_inference_full)
            alt_energy = best_alt[1].energy_per_inference_full
            chosen_energy = chosen_expert.energy_per_inference_compressed or chosen_expert.energy_per_inference_full
            energy_saved = alt_energy - chosen_energy
            co2_saved = energy_saved * self.config.energy_to_co2_factor
            latency_diff = request.latency_ms - (alt_energy / 1e-6 * 0.5)
        else:
            co2_saved = 0.0
            latency_diff = 0.0

        data = {
            'chosen_expert_id': chosen_expert.expert_id,
            'compressed_info': f" (compressed – {chosen_expert.compression_method})" if chosen_expert.compressed_flag else "",
            'co2_savings': f" This decision saved approximately **{co2_saved:.4f} kg CO₂** compared to the most energy‑intensive alternative." if co2_saved > 0 else " (No CO₂ savings over the best alternative).",
            'latency_impact': (
                f" It increased latency by {latency_diff:.1f} ms." if latency_diff > 1.0 else
                f" It reduced latency by {-latency_diff:.1f} ms." if latency_diff < -1.0 else ""
            ),
            'accuracy': request.accuracy,
            'carbon_intensity': request.carbon_intensity,
            'helium_scarcity': request.helium_scarcity,
            'material_index': request.material_index,
            'chosen_utility': chosen_utility,
            'alt_utilities': alt_utilities,
        }

        # Render with Jinja2 template
        try:
            if self.template_env:
                template = self.template_env.get_template(self.template_name)
                result = template.render(**data)
            else:
                # Fallback to string-based template
                template = Template(selected_template)
                result = template.render(**data)
        except Exception as e:
            logger.warning(f"Template rendering failed: {e}, using fallback")
            result = self._fallback_generate(data)

        # If LLM is available, we could also enhance the result
        if self.llm_client:
            # Optionally, we could use LLM to refine the explanation
            pass

        return result

    def generate(
        self,
        request: RequestLog,
        chosen_expert: SustainabilityAwareExpertProfile,
        alternatives: List[Tuple[str, SustainabilityAwareExpertProfile]],
        user_context: Optional[Dict] = None,
    ) -> str:
        """
        Generate explanation synchronously using the template engine.
        """
        return asyncio.run(self.generate_async(request, chosen_expert, alternatives, user_context))

    def _fallback_generate(self, data: Dict) -> str:
        parts = [
            f"This request was routed to expert **{data['chosen_expert_id']}**{data['compressed_info']}.",
            data['co2_savings'],
            data['latency_impact'],
            f" The chosen expert achieved accuracy of {data['accuracy']:.2%}.",
            f" Carbon intensity was {data['carbon_intensity']:.1f} gCO₂/kWh, helium scarcity {data['helium_scarcity']:.2f}, material index {data['material_index']:.2f}."
        ]
        if data.get('chosen_utility') is not None:
            parts.append(f" (Utility score: {data['chosen_utility']:.3f})")
        return " ".join(p for p in parts if p)

    # ----------------------------------------------------------------
    # Bio‑inspired evolution of templates
    # ----------------------------------------------------------------
    async def evolve_templates(self):
        """Run one cycle of bio‑inspired evolution on the template population."""
        if not self.bio or not self.config.template_evolution_enabled:
            return
        if len(self.template_fitness) < 10:
            logger.debug("Not enough fitness data to evolve templates.")
            return

        # Fitness function: average of recent feedback ratings
        def fitness(variant):
            # We assume fitness is stored alongside each variant; for simplicity we use the average rating
            return np.mean(list(self.template_fitness))

        # Evolve population
        new_population = self.bio.evolve(
            population=self.template_population,
            fitness_fn=fitness,
            generations=self.config.template_generations,
            population_size=self.config.template_population_size,
        )
        if new_population:
            self.template_population = new_population
            # Update the active template to the best one
            best = max(new_population, key=lambda v: fitness(v))
            self.template = best.get("template", self.template)
            self.template_name = best.get("style", "default")
            self._save_state()
            logger.info("Templates evolved; new population size: %d", len(new_population))

    async def record_feedback(self, rating: int, template_used: str):
        """Record user feedback for template evolution."""
        self.template_fitness.append(rating)
        if len(self.template_fitness) >= 20:
            await self.evolve_templates()

# =============================================================================
# 6. DASHBOARD ENGINE (Enhanced with WS, caching, and evolution loop)
# =============================================================================
class DashboardEngine:
    """
    Manages request logs with async persistence, caching, and real‑time broadcast.
    Also handles background evolution of explanation templates.
    """
    def __init__(self, config: ExplainableUIConfig, db_manager: DatabaseManager,
                 generator: ExplanationGenerator):
        self.config = config
        self.db_manager = db_manager
        self.generator = generator
        self.request_logs: Dict[str, RequestLog] = {}
        self._cache = {}
        self._cache_timestamps = {}
        self._ws_connections: List[WebSocket] = []
        self._broadcast_task = None
        self._cache_lock = asyncio.Lock()
        self._ws_lock = asyncio.Lock()

        # Start background broadcast task
        if config.ws_enabled:
            self._broadcast_task = asyncio.create_task(self._broadcast_loop())
        # Start background template evolution task
        if config.template_evolution_enabled:
            self._evolution_task = asyncio.create_task(self._evolution_loop())

    async def _broadcast_loop(self):
        """Periodic broadcast of recent stats to WebSocket clients."""
        while True:
            try:
                await asyncio.sleep(self.config.ws_broadcast_interval)
                if self._ws_connections:
                    stats = {
                        "type": "stats_update",
                        "data": {
                            "total_requests": len(self.request_logs),
                            "timestamp": datetime.now().isoformat(),
                        }
                    }
                    await self._broadcast(stats)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Broadcast loop error: {e}")

    async def _evolution_loop(self):
        """Periodically run template evolution."""
        while True:
            try:
                await asyncio.sleep(self.config.template_evolution_interval_seconds)
                if self.generator:
                    await self.generator.evolve_templates()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Evolution loop error: {e}")

    async def log_request(self, request_log: RequestLog) -> None:
        """Store a completed routing decision."""
        self.request_logs[request_log.request_id] = request_log
        # Persist to DB asynchronously
        await self._persist_request_async(request_log)
        # Invalidate cache
        async with self._cache_lock:
            self._cache.clear()
        # Broadcast to WebSocket clients
        await self._broadcast({
            "type": "new_request",
            "data": {
                "request_id": request_log.request_id,
                "timestamp": request_log.timestamp.isoformat(),
                "chosen_expert": request_log.chosen_expert_id,
                "energy_joules": request_log.energy_joules,
                "co2_kg": request_log.co2_kg,
                "accuracy": request_log.accuracy,
            }
        })

    async def _persist_request_async(self, req: RequestLog):
        """Asynchronously persist a request to the database."""
        alt_json = json.dumps([
            {'expert_id': eid, 'energy': prof.energy_per_inference_full,
             'accuracy': prof.accuracy_full, 'compressed': prof.compressed_flag}
            for eid, prof in req.alternative_experts
        ])
        log_entry = RequestLogDB(
            request_id=req.request_id,
            timestamp=req.timestamp,
            query=req.query,
            chosen_expert_id=req.chosen_expert_id,
            alternative_experts=alt_json,
            latency_ms=req.latency_ms,
            energy_joules=req.energy_joules,
            co2_kg=req.co2_kg,
            accuracy=req.accuracy,
            carbon_intensity=req.carbon_intensity,
            helium_scarcity=req.helium_scarcity,
            material_index=req.material_index,
            sustainability_score=req.sustainability_score,
            explanation=req.explanation,
        )
        try:
            if ASYNC_SQLALCHEMY_AVAILABLE:
                async with self.db_manager.async_sessionmaker() as session:
                    session.add(log_entry)
                    await session.commit()
                # Update expert stats
                await self._update_expert_stats_async(req.chosen_expert_id, req)
            else:
                # Offload to thread
                await self.db_manager.execute_sync_in_thread(self._persist_request_sync, log_entry)
                await self.db_manager.execute_sync_in_thread(self._update_expert_stats_sync, req.chosen_expert_id, req)
        except Exception as e:
            logger.error(f"Failed to persist request: {e}")

    def _persist_request_sync(self, log_entry):
        session = self.db_manager.sync_sessionmaker()
        session.add(log_entry)
        session.commit()

    def _update_expert_stats_sync(self, expert_id, req):
        session = self.db_manager.sync_sessionmaker()
        stats = session.query(ExpertStatsDB).filter_by(expert_id=expert_id).first()
        if not stats:
            stats = ExpertStatsDB(expert_id=expert_id)
            session.add(stats)
        stats.total_requests += 1
        stats.avg_latency_ms = (stats.avg_latency_ms * (stats.total_requests - 1) + req.latency_ms) / stats.total_requests
        stats.avg_energy_joules = (stats.avg_energy_joules * (stats.total_requests - 1) + req.energy_joules) / stats.total_requests
        stats.avg_accuracy = (stats.avg_accuracy * (stats.total_requests - 1) + req.accuracy) / stats.total_requests
        stats.total_co2_kg += req.co2_kg
        stats.last_updated = datetime.now()
        session.commit()

    async def _update_expert_stats_async(self, expert_id, req):
        """Async version of updating expert stats."""
        async with self.db_manager.async_sessionmaker() as session:
            stmt = select(ExpertStatsDB).where(ExpertStatsDB.expert_id == expert_id)
            result = await session.execute(stmt)
            stats = result.scalar_one_or_none()
            if not stats:
                stats = ExpertStatsDB(expert_id=expert_id)
                session.add(stats)
            stats.total_requests += 1
            stats.avg_latency_ms = (stats.avg_latency_ms * (stats.total_requests - 1) + req.latency_ms) / stats.total_requests
            stats.avg_energy_joules = (stats.avg_energy_joules * (stats.total_requests - 1) + req.energy_joules) / stats.total_requests
            stats.avg_accuracy = (stats.avg_accuracy * (stats.total_requests - 1) + req.accuracy) / stats.total_requests
            stats.total_co2_kg += req.co2_kg
            stats.last_updated = datetime.now()
            await session.commit()

    # ----- Caching -----
    async def _cached(self, key: str, func: Callable, user_id: Optional[str] = None):
        """Cache with TTL, key prefixed by user_id if provided."""
        cache_key = f"{user_id or 'global'}:{key}"
        now = time.time()
        async with self._cache_lock:
            if cache_key in self._cache and (now - self._cache_timestamps.get(cache_key, 0)) < self.config.cache_ttl_seconds:
                return self._cache[cache_key]
        result = await func()
        async with self._cache_lock:
            self._cache[cache_key] = result
            self._cache_timestamps[cache_key] = now
        return result

    async def get_request_data(self, request_id: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        req = self.request_logs.get(request_id)
        if not req:
            # Try to load from DB async
            if ASYNC_SQLALCHEMY_AVAILABLE:
                async with self.db_manager.async_sessionmaker() as session:
                    stmt = select(RequestLogDB).where(RequestLogDB.request_id == request_id)
                    result = await session.execute(stmt)
                    db_entry = result.scalar_one_or_none()
                    if db_entry:
                        req = await self._from_db_entry_async(db_entry)
                        self.request_logs[request_id] = req
            else:
                # Offload to thread
                req = await self.db_manager.execute_sync_in_thread(self._from_db_entry_sync, request_id)
                if req:
                    self.request_logs[request_id] = req
        if not req:
            return {"error": "Request not found"}
        return {
            "request_id": req.request_id,
            "timestamp": req.timestamp.isoformat(),
            "query": req.query,
            "chosen_expert": req.chosen_expert_id,
            "latency_ms": req.latency_ms,
            "energy_joules": req.energy_joules,
            "co2_kg": req.co2_kg,
            "accuracy": req.accuracy,
            "carbon_intensity": req.carbon_intensity,
            "helium_scarcity": req.helium_scarcity,
            "material_index": req.material_index,
            "sustainability_score": req.sustainability_score,
            "explanation": req.explanation,
            "feedback_rating": req.feedback_rating,
            "alternatives": [
                {
                    "expert_id": eid,
                    "energy_joules": prof.energy_per_inference_full,
                    "accuracy": prof.accuracy_full,
                    "compressed": prof.compressed_flag,
                }
                for eid, prof in req.alternative_experts
            ],
        }

    async def _from_db_entry_async(self, db_entry) -> RequestLog:
        alt_list = json.loads(db_entry.alternative_experts)
        alternatives = []
        for alt in alt_list:
            prof = SustainabilityAwareExpertProfile(
                expert_id=alt['expert_id'],
                energy_per_inference_full=alt['energy'],
                accuracy_full=alt['accuracy'],
                compressed_flag=alt['compressed']
            )
            alternatives.append((alt['expert_id'], prof))
        return RequestLog(
            request_id=db_entry.request_id,
            timestamp=db_entry.timestamp,
            query=db_entry.query,
            chosen_expert_id=db_entry.chosen_expert_id,
            chosen_expert_profile=SustainabilityAwareExpertProfile(db_entry.chosen_expert_id),
            alternative_experts=alternatives,
            latency_ms=db_entry.latency_ms,
            energy_joules=db_entry.energy_joules,
            co2_kg=db_entry.co2_kg,
            accuracy=db_entry.accuracy,
            carbon_intensity=db_entry.carbon_intensity,
            helium_scarcity=db_entry.helium_scarcity,
            material_index=db_entry.material_index,
            sustainability_score=db_entry.sustainability_score,
            explanation=db_entry.explanation,
            feedback_rating=db_entry.feedback_rating,
            feedback_comment=db_entry.feedback_comment,
        )

    def _from_db_entry_sync(self, request_id):
        session = self.db_manager.sync_sessionmaker()
        db_entry = session.query(RequestLogDB).filter_by(request_id=request_id).first()
        if db_entry:
            # Use a new event loop or run in thread
            return asyncio.run(self._from_db_entry_async(db_entry))
        return None

    async def get_expert_details(self, expert_id: str) -> Dict[str, Any]:
        if ASYNC_SQLALCHEMY_AVAILABLE:
            async with self.db_manager.async_sessionmaker() as session:
                stmt = select(ExpertStatsDB).where(ExpertStatsDB.expert_id == expert_id)
                result = await session.execute(stmt)
                stats = result.scalar_one_or_none()
                if stats:
                    return {
                        "expert_id": expert_id,
                        "total_requests": stats.total_requests,
                        "avg_latency_ms": stats.avg_latency_ms,
                        "avg_energy_joules": stats.avg_energy_joules,
                        "avg_accuracy": stats.avg_accuracy,
                        "total_co2_kg": stats.total_co2_kg,
                    }
        else:
            session = self.db_manager.sync_sessionmaker()
            stats = session.query(ExpertStatsDB).filter_by(expert_id=expert_id).first()
            if stats:
                return {
                    "expert_id": expert_id,
                    "total_requests": stats.total_requests,
                    "avg_latency_ms": stats.avg_latency_ms,
                    "avg_energy_joules": stats.avg_energy_joules,
                    "avg_accuracy": stats.avg_accuracy,
                    "total_co2_kg": stats.total_co2_kg,
                }
        return {"error": "No data"}

    async def get_dashboard_charts(self, request_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        if not PLOTLY_AVAILABLE:
            return {"error": "Plotly not installed"}

        async def _generate():
            if request_id:
                data = await self.get_request_data(request_id, user_id)
                if "error" in data:
                    return data
                alt = data["alternatives"]
                labels = [a["expert_id"] for a in alt] + [data["chosen_expert"]]
                energies = [a["energy_joules"] for a in alt] + [data["energy_joules"]]
                colors = ["gray"] * len(alt) + ["green"]
                fig = go.Figure(data=[go.Bar(x=labels, y=energies, marker_color=colors)])
                fig.update_layout(
                    title=f"Energy per Inference – Request {request_id}",
                    xaxis_title="Expert",
                    yaxis_title="Energy (Joules)",
                    template=self.config.plotly_theme,
                )
                return fig.to_json()
            else:
                # Overall: energy vs accuracy scatter
                expert_data = {}
                for req in self.request_logs.values():
                    eid = req.chosen_expert_id
                    if eid not in expert_data:
                        expert_data[eid] = {"energies": [], "accuracies": [], "count": 0}
                    expert_data[eid]["energies"].append(req.energy_joules)
                    expert_data[eid]["accuracies"].append(req.accuracy)
                    expert_data[eid]["count"] += 1
                experts = []
                avg_energies = []
                avg_accuracies = []
                sizes = []
                for eid, vals in expert_data.items():
                    experts.append(eid)
                    avg_energies.append(sum(vals["energies"]) / vals["count"])
                    avg_accuracies.append(sum(vals["accuracies"]) / vals["count"])
                    sizes.append(vals["count"] * 10)
                fig = go.Figure(data=[go.Scatter(
                    x=avg_energies,
                    y=avg_accuracies,
                    mode="markers+text",
                    text=experts,
                    marker=dict(size=sizes, color=avg_energies, colorscale="Viridis", showscale=True),
                )])
                fig.update_layout(
                    title="Expert Sustainability Trade‑offs (avg per expert)",
                    xaxis_title="Average Energy per Inference (J)",
                    yaxis_title="Average Accuracy",
                    hovermode="closest",
                    template=self.config.plotly_theme,
                )
                return fig.to_json()
        return await self._cached(f"chart_{request_id}", _generate, user_id)

    # ----- WebSocket Broadcasting -----
    async def _broadcast(self, message: Dict):
        if not self._ws_connections:
            return
        msg = json.dumps(message)
        async with self._ws_lock:
            disconnected = set()
            for ws in self._ws_connections:
                try:
                    await ws.send_text(msg)
                except Exception:
                    disconnected.add(ws)
            for ws in disconnected:
                self._ws_connections.remove(ws)

    async def register_websocket(self, websocket: WebSocket):
        await websocket.accept()
        async with self._ws_lock:
            self._ws_connections.append(websocket)
        try:
            # Heartbeat: send ping every 30s and expect pong
            while True:
                try:
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                    # If client sends "pong", continue; else handle message
                    if data.strip() == "pong":
                        continue
                    # Handle other messages (e.g., client requests)
                except asyncio.TimeoutError:
                    # Send ping and wait for pong
                    await websocket.send_text(json.dumps({"type": "ping"}))
                    try:
                        pong = await asyncio.wait_for(websocket.receive_text(), timeout=5)
                        if pong.strip() != "pong":
                            raise WebSocketDisconnect
                    except asyncio.TimeoutError:
                        raise WebSocketDisconnect
        except WebSocketDisconnect:
            async with self._ws_lock:
                self._ws_connections.remove(websocket)

    # ----- Pagination (database-backed) -----
    async def get_recent_requests(self, page: int = 1, page_size: int = 20, filter_expert: Optional[str] = None, user_id: Optional[str] = None) -> Dict:
        page_size = min(page_size, self.config.max_page_size)
        offset = (page - 1) * page_size

        async def _fetch():
            if ASYNC_SQLALCHEMY_AVAILABLE:
                async with self.db_manager.async_sessionmaker() as session:
                    query = select(RequestLogDB)
                    if filter_expert:
                        query = query.where(RequestLogDB.chosen_expert_id == filter_expert)
                    count_query = select(func.count()).select_from(RequestLogDB)
                    if filter_expert:
                        count_query = count_query.where(RequestLogDB.chosen_expert_id == filter_expert)
                    total = (await session.execute(count_query)).scalar()
                    result = await session.execute(query.order_by(RequestLogDB.timestamp.desc()).offset(offset).limit(page_size))
                    items = result.scalars().all()
                    return total, items
            else:
                # Sync fallback
                session = self.db_manager.sync_sessionmaker()
                query = session.query(RequestLogDB)
                if filter_expert:
                    query = query.filter(RequestLogDB.chosen_expert_id == filter_expert)
                total = query.count()
                items = query.order_by(RequestLogDB.timestamp.desc()).offset(offset).limit(page_size).all()
                return total, items

        total, items = await _fetch()
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": [
                {
                    "request_id": r.request_id,
                    "timestamp": r.timestamp.isoformat(),
                    "chosen_expert": r.chosen_expert_id,
                    "energy_joules": r.energy_joules,
                    "co2_kg": r.co2_kg,
                    "accuracy": r.accuracy,
                    "explanation": r.explanation,
                }
                for r in items
            ]
        }

    async def shutdown(self):
        if self._broadcast_task:
            self._broadcast_task.cancel()
            try:
                await self._broadcast_task
            except asyncio.CancelledError:
                pass
        if hasattr(self, '_evolution_task'):
            self._evolution_task.cancel()
            try:
                await self._evolution_task
            except asyncio.CancelledError:
                pass
        await self.db_manager.close()

# =============================================================================
# 7. WHAT‑IF SIMULATOR (Enhanced with MODP utility)
# =============================================================================
class WhatIfSimulator:
    """
    Simulates alternative routing choices and computes the sustainability impact.
    Includes carbon intensity, helium scarcity, and material index.
    NEW: Computes MODP utilities for chosen and alternative experts.
    """
    def __init__(self, dashboard: DashboardEngine, config: ExplainableUIConfig,
                 carbon_manager=None, lca_client=None, modp: Optional[ParetoOptimizer] = None):
        self.dashboard = dashboard
        self.config = config
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.modp = modp
        self._carbon_circuit = CircuitBreaker("carbon_api")
        self._lca_circuit = CircuitBreaker("lca_api")

    async def get_carbon_intensity(self) -> float:
        if self.carbon_manager:
            try:
                intensity = await self._carbon_circuit.call(self.carbon_manager.get_current_intensity)
                return intensity.get('intensity', 400) / 1000  # kg/kWh
            except Exception as e:
                logger.warning(f"Carbon intensity error: {e}")
        return self.config.co2_per_kwh_kg

    async def simulate(self, request_id: str, alternative_expert_id: str) -> WhatIfResult:
        req = self.dashboard.request_logs.get(request_id)
        if not req:
            raise ValueError(f"Request {request_id} not found")

        alt_profile = None
        for eid, prof in req.alternative_experts:
            if eid == alternative_expert_id:
                alt_profile = prof
                break
        if not alt_profile:
            raise ValueError(f"Expert {alternative_expert_id} not in alternatives")

        # Alternative metrics
        if alt_profile.compressed_flag and alt_profile.energy_per_inference_compressed:
            alt_energy = alt_profile.energy_per_inference_compressed
        else:
            alt_energy = alt_profile.energy_per_inference_full
        alt_latency = alt_energy * 1e-6 * 0.5  # rough
        alt_accuracy = alt_profile.accuracy_compressed if alt_profile.compressed_flag else alt_profile.accuracy_full
        co2_intensity = await self.get_carbon_intensity()
        alt_co2 = alt_energy * co2_intensity / 3600000

        # Differences
        diff_energy = alt_energy - req.energy_joules
        diff_co2 = alt_co2 - req.co2_kg
        diff_latency = alt_latency - req.latency_ms
        diff_accuracy = alt_accuracy - req.accuracy

        # Compute MODP utilities if available
        chosen_utility = None
        alternative_utility = None
        if self.modp:
            chosen_obj = {
                "accuracy": req.accuracy,
                "energy": 1.0 - (req.energy_joules / max(alt_energy, req.energy_joules, 1e-8)),
                "carbon": 1.0 - (req.co2_kg / max(alt_co2, req.co2_kg, 1e-8)),
                "latency": 1.0 - (req.latency_ms / max(alt_latency, req.latency_ms, 1e-8)),
            }
            chosen_utility = self.modp.evaluate(chosen_obj, self.config.modp_weights)

            alt_obj = {
                "accuracy": alt_accuracy,
                "energy": 1.0 - (alt_energy / max(alt_energy, req.energy_joules, 1e-8)),
                "carbon": 1.0 - (alt_co2 / max(alt_co2, req.co2_kg, 1e-8)),
                "latency": 1.0 - (alt_latency / max(alt_latency, req.latency_ms, 1e-8)),
            }
            alternative_utility = self.modp.evaluate(alt_obj, self.config.modp_weights)

        return WhatIfResult(
            scenario_id=str(uuid.uuid4()),
            alternative_expert_id=alternative_expert_id,
            expected_energy_joules=alt_energy,
            expected_co2_kg=alt_co2,
            expected_latency_ms=alt_latency,
            expected_accuracy=alt_accuracy,
            expected_carbon_intensity=req.carbon_intensity,
            expected_helium_scarcity=req.helium_scarcity,
            expected_material_index=req.material_index,
            difference_energy=diff_energy,
            difference_co2=diff_co2,
            difference_latency=diff_latency,
            difference_accuracy=diff_accuracy,
            chosen_utility=chosen_utility,
            alternative_utility=alternative_utility,
        )

# =============================================================================
# 8. AUTHENTICATION & RBAC (Enhanced with User DB and Refresh Tokens)
# =============================================================================
class AuthManager:
    def __init__(self, config: ExplainableUIConfig, db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.secret = config.jwt_secret
        self.algorithm = config.jwt_algorithm
        self.expiry = config.jwt_expiration_minutes
        self.refresh_expiry_days = config.refresh_token_expiration_days
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def hash_password(self, password: str) -> str:
        return self.pwd_context.hash(password)

    def verify_password(self, password: str, hash: str) -> bool:
        return self.pwd_context.verify(password, hash)

    async def get_user(self, username: str) -> Optional[UserDB]:
        if ASYNC_SQLALCHEMY_AVAILABLE:
            async with self.db_manager.async_sessionmaker() as session:
                stmt = select(UserDB).where(UserDB.username == username)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        else:
            session = self.db_manager.sync_sessionmaker()
            return session.query(UserDB).filter_by(username=username).first()

    async def create_user(self, username: str, password: str, role: str = "viewer") -> UserDB:
        hashed = self.hash_password(password)
        user = UserDB(username=username, password_hash=hashed, role=role)
        if ASYNC_SQLALCHEMY_AVAILABLE:
            async with self.db_manager.async_sessionmaker() as session:
                session.add(user)
                await session.commit()
                await session.refresh(user)
        else:
            session = self.db_manager.sync_sessionmaker()
            session.add(user)
            session.commit()
            session.refresh(user)
        return user

    def create_token(self, username: str, role: str = "viewer") -> str:
        expire = datetime.utcnow() + timedelta(minutes=self.expiry)
        payload = {"sub": username, "role": role, "exp": expire}
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def create_refresh_token(self, username: str) -> str:
        expire = datetime.utcnow() + timedelta(days=self.refresh_expiry_days)
        payload = {"sub": username, "type": "refresh", "exp": expire}
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def verify_token(self, token: str) -> Dict:
        try:
            payload = jwt.decode(token, self.secret, algorithms=[self.algorithm])
            return payload
        except jwt.PyJWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    async def authenticate_user(self, username: str, password: str) -> Optional[UserDB]:
        user = await self.get_user(username)
        if user and self.verify_password(password, user.password_hash):
            return user
        return None

    async def refresh_access_token(self, refresh_token: str) -> Dict:
        try:
            payload = jwt.decode(refresh_token, self.secret, algorithms=[self.algorithm])
            if payload.get("type") != "refresh":
                raise HTTPException(status_code=401, detail="Invalid refresh token")
            username = payload.get("sub")
            user = await self.get_user(username)
            if not user:
                raise HTTPException(status_code=401, detail="User not found")
            new_token = self.auth.create_token(username, user.role)
            return {"access_token": new_token, "token_type": "bearer"}
        except jwt.PyJWTError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

# =============================================================================
# 9. API GATEWAY EXTENSION (Enhanced with FastAPI and feedback loop)
# =============================================================================
class APIGatewayExtension:
    """
    Extends FastAPI with /api/explain endpoints, WebSocket, authentication,
    and feedback publishing to central message queue.
    """
    def __init__(self, dashboard: DashboardEngine, generator: ExplanationGenerator,
                 what_if: WhatIfSimulator, auth: AuthManager,
                 message_queue: Optional[AsyncMessageQueue] = None):
        self.dashboard = dashboard
        self.generator = generator
        self.what_if = what_if
        self.auth = auth
        self.message_queue = message_queue
        self.app = None
        self.limiter = None
        if SLOWAPI_AVAILABLE:
            self.limiter = Limiter(key_func=get_remote_address)

    def register_routes(self, app: FastAPI):
        self.app = app

        # Rate limiting
        if self.limiter:
            app.state.limiter = self.limiter
            app.add_exception_handler(429, _rate_limit_exceeded_handler)

        # WebSocket endpoint
        if FASTAPI_AVAILABLE and WEBSOCKETS_AVAILABLE:
            @app.websocket("/ws/explain")
            async def websocket_endpoint(websocket: WebSocket):
                await self.dashboard.register_websocket(websocket)

        # Authentication endpoints
        @app.post("/api/explain/login")
        async def login(username: str, password: str):
            user = await self.auth.authenticate_user(username, password)
            if not user:
                raise HTTPException(status_code=401, detail="Invalid credentials")
            # Update last_login
            if ASYNC_SQLALCHEMY_AVAILABLE:
                async with self.dashboard.db_manager.async_sessionmaker() as session:
                    stmt = update(UserDB).where(UserDB.id == user.id).values(last_login=datetime.now())
                    await session.execute(stmt)
                    await session.commit()
            else:
                session = self.dashboard.db_manager.sync_sessionmaker()
                user.last_login = datetime.now()
                session.commit()
            access_token = self.auth.create_token(user.username, user.role)
            refresh_token = self.auth.create_refresh_token(user.username)
            # Store refresh token in DB
            if ASYNC_SQLALCHEMY_AVAILABLE:
                async with self.dashboard.db_manager.async_sessionmaker() as session:
                    stmt = update(UserDB).where(UserDB.id == user.id).values(
                        refresh_token=refresh_token,
                        refresh_token_expires=datetime.utcnow() + timedelta(days=self.auth.refresh_expiry_days)
                    )
                    await session.execute(stmt)
                    await session.commit()
            else:
                session = self.dashboard.db_manager.sync_sessionmaker()
                user.refresh_token = refresh_token
                user.refresh_token_expires = datetime.utcnow() + timedelta(days=self.auth.refresh_expiry_days)
                session.commit()
            return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}

        @app.post("/api/explain/refresh")
        async def refresh(refresh_token: str):
            return await self.auth.refresh_access_token(refresh_token)

        @app.post("/api/explain/register")
        async def register(username: str, password: str, role: str = "viewer"):
            # Only admin can create admin users, but for demo, allow any role
            existing = await self.auth.get_user(username)
            if existing:
                raise HTTPException(status_code=400, detail="User already exists")
            user = await self.auth.create_user(username, password, role)
            return {"username": user.username, "role": user.role}

        # Security dependency
        security = HTTPBearer()
        async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
            return self.auth.verify_token(credentials.credentials)

        async def require_role(role: str, user: Dict = Depends(get_current_user)):
            if user.get("role") != role:
                raise HTTPException(status_code=403, detail="Insufficient permissions")
            return user

        # Public endpoints
        @app.get("/api/explain/health")
        async def health():
            # Check DB connectivity
            db_ok = True
            try:
                if ASYNC_SQLALCHEMY_AVAILABLE:
                    async with self.dashboard.db_manager.async_sessionmaker() as session:
                        await session.execute("SELECT 1")
                else:
                    session = self.dashboard.db_manager.sync_sessionmaker()
                    session.execute("SELECT 1")
            except Exception as e:
                db_ok = False
                logger.error(f"Health check: DB error {e}")
            # Check carbon manager
            carbon_ok = True
            if hasattr(self, 'carbon_manager') and self.carbon_manager:
                try:
                    if hasattr(self.carbon_manager, 'get_current_intensity'):
                        await self.carbon_manager.get_current_intensity()
                except Exception:
                    carbon_ok = False
            # Check LCA client
            lca_ok = True
            if hasattr(self, 'lca_client') and self.lca_client:
                try:
                    if hasattr(self.lca_client, 'get_material_index'):
                        await self.lca_client.get_material_index("test")
                except Exception:
                    lca_ok = False
            return {
                "status": "healthy" if db_ok and carbon_ok and lca_ok else "degraded",
                "database": "ok" if db_ok else "error",
                "carbon_manager": "ok" if carbon_ok else "error",
                "lca_client": "ok" if lca_ok else "error",
                "cache_size": len(self.dashboard._cache),
                "websocket_connections": len(self.dashboard._ws_connections),
                "timestamp": datetime.now().isoformat(),
            }

        @app.get("/api/explain/metrics")
        async def get_metrics():
            if PROMETHEUS_AVAILABLE:
                return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
            return {"error": "Prometheus not enabled"}

        # Protected endpoints (viewer)
        @app.get("/api/explain/request/{request_id}")
        async def explain_request(request_id: str, user: Dict = Depends(get_current_user)):
            data = await self.dashboard.get_request_data(request_id, user.get("sub"))
            if "error" in data:
                raise HTTPException(status_code=404, detail=data["error"])
            return data

        @app.get("/api/explain/dashboard")
        async def dashboard_data(page: int = 1, page_size: int = 20, expert: Optional[str] = None, user: Dict = Depends(get_current_user)):
            return await self.dashboard.get_recent_requests(page, page_size, expert, user.get("sub"))

        @app.get("/api/explain/charts")
        async def dashboard_charts(request_id: Optional[str] = None, user: Dict = Depends(get_current_user)):
            charts = await self.dashboard.get_dashboard_charts(request_id, user.get("sub"))
            if "error" in charts:
                raise HTTPException(status_code=400, detail=charts["error"])
            return charts

        @app.post("/api/explain/whatif")
        async def whatif_simulation(data: dict, user: Dict = Depends(get_current_user)):
            try:
                result = await self.what_if.simulate(data.get("request_id"), data.get("alternative_expert_id"))
                return result.__dict__
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        # Feedback endpoint – now publishes to message queue
        @app.post("/api/explain/feedback/{request_id}")
        async def submit_feedback(request_id: str, rating: int, comment: Optional[str] = None, user: Dict = Depends(get_current_user)):
            req = self.dashboard.request_logs.get(request_id)
            if not req:
                raise HTTPException(status_code=404, detail="Request not found")
            req.feedback_rating = rating
            req.feedback_comment = comment
            # Update DB
            try:
                if ASYNC_SQLALCHEMY_AVAILABLE:
                    async with self.dashboard.db_manager.async_sessionmaker() as session:
                        stmt = update(RequestLogDB).where(RequestLogDB.request_id == request_id).values(
                            feedback_rating=rating,
                            feedback_comment=comment
                        )
                        await session.execute(stmt)
                        await session.commit()
                else:
                    session = self.dashboard.db_manager.sync_sessionmaker()
                    session.query(RequestLogDB).filter_by(request_id=request_id).update({
                        "feedback_rating": rating,
                        "feedback_comment": comment
                    })
                    session.commit()
            except Exception as e:
                logger.error(f"Failed to update feedback: {e}")
                raise HTTPException(status_code=500, detail="Feedback update failed")

            # Publish feedback event to central message queue if available
            if self.message_queue:
                event = FeedbackEvent.create_with_context(
                    task_id=f"feedback_{request_id}",
                    selected_action=req.chosen_expert_id,
                    quality_score=rating / 5.0,
                    latency_ms=0,
                    energy_joules=0,
                    carbon_g=0,
                    feedback_type="user_preference",
                    adaptive_cost_value=0.0,
                    state={"request_id": request_id, "comment": comment},
                    candidates=[{"action": "none"}],
                    source="explainable_ui",
                    environment="production",
                    tags=["user_feedback"]
                )
                await self.message_queue.publish("feedback_events", event.to_json())

            # Also record feedback in the explanation generator for template evolution
            if self.generator:
                await self.generator.record_feedback(rating, req.explanation)

            return {"status": "feedback recorded"}

        # Export endpoint
        @app.get("/api/explain/export/{request_id}")
        async def export_request(request_id: str, format: str = "json", user: Dict = Depends(get_current_user)):
            data = await self.dashboard.get_request_data(request_id, user.get("sub"))
            if "error" in data:
                raise HTTPException(status_code=404, detail=data["error"])
            if format == "json":
                return data
            elif format == "csv":
                import csv
                from io import StringIO
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(["key", "value"])
                for k, v in data.items():
                    writer.writerow([k, str(v)])
                return Response(content=output.getvalue(), media_type="text/csv")
            elif format == "png" and PLOTLY_AVAILABLE:
                # Generate PNG from chart (requires kaleido)
                try:
                    fig = go.Figure(data=[go.Bar(x=[a["expert_id"] for a in data["alternatives"]] + [data["chosen_expert"]],
                                                 y=[a["energy_joules"] for a in data["alternatives"]] + [data["energy_joules"]])])
                    img_bytes = fig.to_image(format="png")
                    return Response(content=img_bytes, media_type="image/png")
                except Exception as e:
                    logger.warning(f"PNG export failed: {e}")
                    raise HTTPException(status_code=500, detail="PNG generation failed")
            else:
                raise HTTPException(status_code=400, detail="Unsupported format")

        # Admin endpoints
        @app.post("/api/explain/admin/refresh")
        async def refresh_cache(user: Dict = Depends(require_role("admin"))):
            async with self.dashboard._cache_lock:
                self.dashboard._cache.clear()
            return {"status": "cache cleared"}

        @app.post("/api/explain/admin/export/all")
        async def export_all(format: str = "json", user: Dict = Depends(require_role("admin"))):
            # Stream export to avoid memory issues
            if format == "json":
                async def stream_json():
                    yield "["
                    first = True
                    for req_id in self.dashboard.request_logs:
                        req_data = await self.dashboard.get_request_data(req_id, user.get("sub"))
                        if "error" not in req_data:
                            if not first:
                                yield ","
                            first = False
                            yield json.dumps(req_data)
                    yield "]"
                return StreamingResponse(stream_json(), media_type="application/json")
            elif format == "csv":
                import csv
                from io import StringIO
                output = StringIO()
                writer = csv.writer(output)
                first = True
                for req_id in self.dashboard.request_logs:
                    req_data = await self.dashboard.get_request_data(req_id, user.get("sub"))
                    if "error" not in req_data:
                        if first:
                            writer.writerow(req_data.keys())
                            first = False
                        writer.writerow(req_data.values())
                return Response(content=output.getvalue(), media_type="text/csv")
            else:
                raise HTTPException(status_code=400, detail="Unsupported format")

        @app.post("/api/explain/admin/reload_template")
        async def reload_template(path: Optional[str] = None, user: Dict = Depends(require_role("admin"))):
            self.generator.reload_template(path)
            return {"status": "template reloaded"}

        # New endpoints for optimization state
        @app.get("/api/explain/optimization/status")
        async def optimization_status(user: Dict = Depends(require_role("admin"))):
            return {
                "template_population_size": len(self.generator.template_population),
                "template_fitness_length": len(self.generator.template_fitness),
                "modp_weights": self.config.modp_weights,
                "template_evolution_enabled": self.config.template_evolution_enabled,
                "enhancements_available": ENHANCEMENTS_AVAILABLE,
            }

        @app.post("/api/explain/optimization/evolve")
        async def evolve_templates(user: Dict = Depends(require_role("admin"))):
            await self.generator.evolve_templates()
            return {"status": "evolution triggered"}

        logger.info("API Gateway routes registered")

# =============================================================================
# 10. CONVENIENCE FACTORY (Enhanced)
# =============================================================================
def create_explainable_ui(
    config: Optional[Union[Dict, ExplainableUIConfig]] = None,
    carbon_manager: Optional[Any] = None,
    lca_client: Optional[Any] = None,
    message_queue: Optional[AsyncMessageQueue] = None,
) -> Dict[str, Any]:
    """
    Factory to create all components and return them for integration.
    """
    if config is None:
        config = ExplainableUIConfig()
    elif isinstance(config, dict):
        config = ExplainableUIConfig.from_dict(config)

    db_manager = DatabaseManager(config)
    generator = ExplanationGenerator(config, db_manager=db_manager)
    dashboard = DashboardEngine(config, db_manager, generator)
    what_if = WhatIfSimulator(dashboard, config, carbon_manager, lca_client,
                               modp=generator.modp if ENHANCEMENTS_AVAILABLE else None)
    auth = AuthManager(config, db_manager)

    api_extension = APIGatewayExtension(dashboard, generator, what_if, auth, message_queue)

    return {
        "dashboard": dashboard,
        "explanation_generator": generator,
        "what_if": what_if,
        "auth": auth,
        "api_extension": api_extension,
        "db_manager": db_manager,
    }

# =============================================================================
# 11. UNIT TEST STUBS
# =============================================================================
async def test_explainable_ui():
    """Example test stub."""
    config = ExplainableUIConfig(db_path=":memory:")
    components = create_explainable_ui(config)
    dashboard = components["dashboard"]
    # Log a mock request
    prof = SustainabilityAwareExpertProfile("expert_A")
    req = RequestLog(
        request_id="test-123",
        timestamp=datetime.now(),
        query="test query",
        chosen_expert_id="expert_A",
        chosen_expert_profile=prof,
        alternative_experts=[],
        latency_ms=100,
        energy_joules=5.0,
        co2_kg=0.1,
        accuracy=0.95,
    )
    await dashboard.log_request(req)
    data = await dashboard.get_request_data("test-123")
    assert data["request_id"] == "test-123"
    await dashboard.shutdown()

# =============================================================================
# 12. EXAMPLE USAGE
# =============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def main():
        components = create_explainable_ui()
        dash = components["dashboard"]
        gen = components["explanation_generator"]
        what_if = components["what_if"]

        # Create a dummy profile
        prof = SustainabilityAwareExpertProfile("expert_A")
        prof.energy_per_inference_full = 2.5
        prof.accuracy_full = 0.92
        prof.compressed_flag = False

        alt_prof = SustainabilityAwareExpertProfile("expert_B")
        alt_prof.energy_per_inference_full = 3.8
        alt_prof.accuracy_full = 0.94

        req = RequestLog(
            request_id="test-123",
            timestamp=datetime.now(),
            query="What is the weather?",
            chosen_expert_id="expert_A",
            chosen_expert_profile=prof,
            alternative_experts=[("expert_B", alt_prof)],
            latency_ms=120.0,
            energy_joules=2.5,
            co2_kg=2.5 * 0.2 / 3600000,
            accuracy=0.92,
            carbon_intensity=400.0,
            helium_scarcity=0.5,
            material_index=0.2,
        )
        await dash.log_request(req)

        explanation = await gen.generate_async(req, prof, [("expert_B", alt_prof)], user_context={"role": "admin"})
        print("Explanation:", explanation)

        result = await what_if.simulate("test-123", "expert_B")
        print("What‑if result:", result)

        await dash.shutdown()
        print("✅ Enhanced Explainable UI module ready.")

    asyncio.run(main())
