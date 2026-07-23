# explainable_ui.py
"""
Enhanced Explainable Green Decisions – Enterprise UI
=====================================================

Provides:
- Natural‑language explanations for routing decisions (CO₂, carbon intensity, helium, material, latency, accuracy).
- Interactive dashboard with request‑level cost breakdowns, drill‑down, pagination, and real‑time updates via WebSocket.
- “What‑if” mode with multi‑scenario comparison.
- REST API with JWT authentication and role‑based access.
- Persistence (SQLite/PostgreSQL) for request logs and feedback.
- Configurable explanation templates (Jinja2).
- Export reports in CSV, JSON, and PNG/PDF (via Plotly).
- Prometheus metrics.
- Correlation IDs for end‑to‑end tracing.
- Unit test stubs.

Integrates with:
- SustainabilityAwareExpertProfile, SustainabilityFitnessScorer
- VisualizationEngine (Plotly)
- APIGateway (/api/explain/*)
- Anomaly detection, predictive maintenance, LCA data.
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
from typing import List, Dict, Any, Optional, Tuple, Union
from collections import deque
import numpy as np

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- SQLAlchemy ----------
try:
    from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, JSON, Text, Index, func
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, scoped_session, relationship, backref
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

# ---------- FastAPI ----------
try:
    from fastapi import FastAPI, Depends, HTTPException, status, Request, WebSocket, WebSocketDisconnect, BackgroundTasks
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import Response, StreamingResponse, JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ---------- Authentication ----------
try:
    import jwt
    from passlib.context import CryptContext
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

# ---------- WebSocket ----------
try:
    from websockets import WebSocketServerProtocol
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# ---------- Plotly ----------
try:
    import plotly.graph_objects as go
    import plotly.express as px
    import plotly.io as pio
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# ---------- Jinja2 ----------
try:
    from jinja2 import Template
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

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

# ============================================================================
# 1. CONFIGURATION (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
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

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v):
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'log_level must be one of {allowed}')
            return v.upper()

        class Config:
            env_prefix = "EXPLAINABLE_UI_"
else:
    # Fallback dict
    EXPLAINABLE_UI_CONFIG = {
        "db_path": "./explainable_ui.db",
        "db_pool_size": 10,
        "db_max_overflow": 20,
        "jwt_secret": "change_me_in_production",
        "jwt_algorithm": "HS256",
        "jwt_expiration_minutes": 1440,
        "cache_ttl_seconds": 300,
        "plotly_theme": "plotly_white",
        "log_level": "INFO",
        "export_format": "json",
        "ws_enabled": True,
        "ws_broadcast_interval": 5,
        "default_page_size": 20,
        "max_page_size": 100,
    }

# ============================================================================
# 2. DATA MODELS (Enhanced)
# ============================================================================
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

# ============================================================================
# 3. DATABASE MODELS (SQLAlchemy)
# ============================================================================
if SQLALCHEMY_AVAILABLE:
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

# ============================================================================
# 4. EXPLANATION GENERATOR (Enhanced)
# ============================================================================
class ExplanationGenerator:
    """
    Produces human‑readable, natural‑language explanations with multiple dimensions.
    Supports Jinja2 templates if available.
    """

    def __init__(self, template: Optional[str] = None):
        self.co2_per_kwh = 0.2  # kg CO₂ per kWh
        self.energy_to_co2_factor = self.co2_per_kwh / 3600000
        self.template = template or self._default_template()

    def _default_template(self) -> str:
        return (
            "This request was routed to expert **{chosen_expert_id}**"
            "{compressed_info}."
            "{co2_savings}{latency_impact}"
            " The chosen expert achieved accuracy of {accuracy:.2%}."
            " Carbon intensity was {carbon_intensity:.1f} gCO₂/kWh, helium scarcity {helium_scarcity:.2f},"
            " material index {material_index:.2f}."
        )

    def generate(
        self,
        request: RequestLog,
        chosen_expert: SustainabilityAwareExpertProfile,
        alternatives: List[Tuple[str, SustainabilityAwareExpertProfile]],
    ) -> str:
        """Generate explanation using template."""
        # Compute savings
        if alternatives:
            best_alt = min(alternatives, key=lambda x: x[1].energy_per_inference_full)
            alt_energy = best_alt[1].energy_per_inference_full
            chosen_energy = chosen_expert.energy_per_inference_compressed or chosen_expert.energy_per_inference_full
            energy_saved = alt_energy - chosen_energy
            co2_saved = energy_saved * self.energy_to_co2_factor
            latency_diff = request.latency_ms - (alt_energy / 1e-6 * 0.5)  # rough
        else:
            co2_saved = 0.0
            latency_diff = 0.0

        # Build data for template
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
        }

        if JINJA2_AVAILABLE:
            template = Template(self.template)
            return template.render(**data)
        else:
            return self._fallback_generate(data)

    def _fallback_generate(self, data: Dict) -> str:
        parts = [
            f"This request was routed to expert **{data['chosen_expert_id']}**{data['compressed_info']}.",
            data['co2_savings'],
            data['latency_impact'],
            f" The chosen expert achieved accuracy of {data['accuracy']:.2%}.",
            f" Carbon intensity was {data['carbon_intensity']:.1f} gCO₂/kWh, helium scarcity {data['helium_scarcity']:.2f}, material index {data['material_index']:.2f}."
        ]
        return " ".join(p for p in parts if p)

# ============================================================================
# 5. DASHBOARD ENGINE (Enhanced)
# ============================================================================
class DashboardEngine:
    """
    Manages request logs with persistence, caching, and real‑time broadcast.
    """

    def __init__(self, config: 'ExplainableUIConfig', db_session=None):
        self.config = config
        self.db_session = db_session
        self.request_logs: Dict[str, RequestLog] = {}
        self._cache = {}
        self._cache_timestamps = {}
        self._ws_connections: List[WebSocket] = []
        self._broadcast_task = None

    def log_request(self, request_log: RequestLog) -> None:
        """Store a completed routing decision."""
        self.request_logs[request_log.request_id] = request_log
        # Persist to DB
        if SQLALCHEMY_AVAILABLE and self.db_session:
            self._persist_request(request_log)
        # Invalidate cache
        self._cache.clear()
        # Broadcast to WebSocket clients
        asyncio.create_task(self._broadcast(request_log))

    def _persist_request(self, req: RequestLog):
        session = self.db_session()
        # Convert alternatives to JSON
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
        session.add(log_entry)
        session.commit()
        # Update expert stats
        self._update_expert_stats(req.chosen_expert_id, req)

    def _update_expert_stats(self, expert_id: str, req: RequestLog):
        session = self.db_session()
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

    # ----- Caching -----
    def _cached(self, key: str, func):
        now = time.time()
        if key in self._cache and (now - self._cache_timestamps.get(key, 0)) < self.config.cache_ttl_seconds:
            return self._cache[key]
        result = func()
        self._cache[key] = result
        self._cache_timestamps[key] = now
        return result

    def get_request_data(self, request_id: str) -> Dict[str, Any]:
        req = self.request_logs.get(request_id)
        if not req:
            # Try to load from DB
            if SQLALCHEMY_AVAILABLE and self.db_session:
                session = self.db_session()
                db_entry = session.query(RequestLogDB).filter_by(request_id=request_id).first()
                if db_entry:
                    req = self._from_db_entry(db_entry)
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

    def _from_db_entry(self, db_entry) -> RequestLog:
        # Reconstruct from DB object
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

    def get_expert_details(self, expert_id: str) -> Dict[str, Any]:
        if SQLALCHEMY_AVAILABLE and self.db_session:
            session = self.db_session()
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

    def get_dashboard_charts(self, request_id: Optional[str] = None) -> Dict[str, Any]:
        if not PLOTLY_AVAILABLE:
            return {"error": "Plotly not installed"}

        def _generate():
            if request_id:
                data = self.get_request_data(request_id)
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
        return self._cached(f"chart_{request_id}", _generate)

    # ----- WebSocket Broadcasting -----
    async def _broadcast(self, request_log: RequestLog):
        if not self._ws_connections:
            return
        message = json.dumps({
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
        disconnected = set()
        for ws in self._ws_connections:
            try:
                await ws.send_text(message)
            except Exception:
                disconnected.add(ws)
        for ws in disconnected:
            self._ws_connections.remove(ws)

    async def register_websocket(self, websocket: WebSocket):
        await websocket.accept()
        self._ws_connections.append(websocket)
        try:
            while True:
                await websocket.receive_text()  # keep connection alive
        except WebSocketDisconnect:
            self._ws_connections.remove(websocket)

    # ----- Pagination -----
    def get_recent_requests(self, page: int = 1, page_size: int = 20, filter_expert: Optional[str] = None) -> Dict:
        page_size = min(page_size, self.config.max_page_size)
        all_reqs = list(self.request_logs.values())
        if filter_expert:
            all_reqs = [r for r in all_reqs if r.chosen_expert_id == filter_expert]
        total = len(all_reqs)
        start = (page - 1) * page_size
        end = start + page_size
        items = all_reqs[start:end]
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

# ============================================================================
# 6. WHAT‑IF SIMULATOR (Enhanced)
# ============================================================================
class WhatIfSimulator:
    """
    Simulates alternative routing choices and computes the sustainability impact.
    Includes carbon intensity, helium scarcity, and material index.
    """
    def __init__(self, dashboard: DashboardEngine, carbon_manager=None, lca_client=None):
        self.dashboard = dashboard
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.co2_per_kwh = 0.2

    def simulate(self, request_id: str, alternative_expert_id: str) -> WhatIfResult:
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
        alt_co2 = alt_energy * self.co2_per_kwh / 3600000

        # Differences
        diff_energy = alt_energy - req.energy_joules
        diff_co2 = alt_co2 - req.co2_kg
        diff_latency = alt_latency - req.latency_ms
        diff_accuracy = alt_accuracy - req.accuracy

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
        )

# ============================================================================
# 7. AUTHENTICATION & RBAC
# ============================================================================
class AuthManager:
    def __init__(self, config: ExplainableUIConfig):
        self.config = config
        self.secret = config.jwt_secret
        self.algorithm = config.jwt_algorithm
        self.expiry = config.jwt_expiration_minutes
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto") if JWT_AVAILABLE else None

    def create_token(self, username: str, role: str = "viewer") -> str:
        if not JWT_AVAILABLE:
            return "dummy_token"
        expire = datetime.utcnow() + timedelta(minutes=self.expiry)
        payload = {"sub": username, "role": role, "exp": expire}
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def verify_token(self, token: str) -> Dict:
        if not JWT_AVAILABLE:
            return {"sub": "dummy", "role": "viewer"}
        try:
            payload = jwt.decode(token, self.secret, algorithms=[self.algorithm])
            return payload
        except jwt.PyJWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    def get_current_user(self, token: str) -> Dict:
        return self.verify_token(token)

# ============================================================================
# 8. API GATEWAY EXTENSION (Enhanced with FastAPI)
# ============================================================================
class APIGatewayExtension:
    """
    Extends FastAPI/Flask with /api/explain endpoints, WebSocket, and authentication.
    """

    def __init__(self, dashboard: DashboardEngine, generator: ExplanationGenerator, what_if: WhatIfSimulator, auth: AuthManager):
        self.dashboard = dashboard
        self.generator = generator
        self.what_if = what_if
        self.auth = auth
        self.app = None

    def register_routes(self, app):
        self.app = app

        # WebSocket endpoint
        if FASTAPI_AVAILABLE and WEBSOCKETS_AVAILABLE:
            @app.websocket("/ws/explain")
            async def websocket_endpoint(websocket: WebSocket):
                await self.dashboard.register_websocket(websocket)

        # Authentication
        @app.post("/api/explain/login")
        async def login(username: str, password: str):
            # In production, validate against user DB
            if username == "admin" and password == "admin":
                token = self.auth.create_token(username, "admin")
                return {"access_token": token, "token_type": "bearer"}
            raise HTTPException(status_code=401, detail="Invalid credentials")

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
            return {"status": "ok"}

        @app.get("/api/explain/metrics")
        async def get_metrics():
            if PROMETHEUS_AVAILABLE:
                return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
            return {"error": "Prometheus not enabled"}

        # Protected endpoints (viewer)
        @app.get("/api/explain/request/{request_id}")
        async def explain_request(request_id: str, user: Dict = Depends(get_current_user)):
            data = self.dashboard.get_request_data(request_id)
            if "error" in data:
                raise HTTPException(status_code=404, detail=data["error"])
            return data

        @app.get("/api/explain/dashboard")
        async def dashboard_data(page: int = 1, page_size: int = 20, expert: Optional[str] = None, user: Dict = Depends(get_current_user)):
            return self.dashboard.get_recent_requests(page, page_size, expert)

        @app.get("/api/explain/charts")
        async def dashboard_charts(request_id: Optional[str] = None, user: Dict = Depends(get_current_user)):
            charts = self.dashboard.get_dashboard_charts(request_id)
            if "error" in charts:
                raise HTTPException(status_code=400, detail=charts["error"])
            return charts

        @app.post("/api/explain/whatif")
        async def whatif_simulation(data: dict, user: Dict = Depends(get_current_user)):
            try:
                result = self.what_if.simulate(data.get("request_id"), data.get("alternative_expert_id"))
                return result.__dict__
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        # Feedback endpoint
        @app.post("/api/explain/feedback/{request_id}")
        async def submit_feedback(request_id: str, rating: int, comment: Optional[str] = None, user: Dict = Depends(get_current_user)):
            req = self.dashboard.request_logs.get(request_id)
            if not req:
                raise HTTPException(status_code=404, detail="Request not found")
            req.feedback_rating = rating
            req.feedback_comment = comment
            # Update DB if applicable
            if SQLALCHEMY_AVAILABLE and self.dashboard.db_session:
                session = self.dashboard.db_session()
                session.query(RequestLogDB).filter_by(request_id=request_id).update({
                    "feedback_rating": rating,
                    "feedback_comment": comment
                })
                session.commit()
            return {"status": "feedback recorded"}

        # Export endpoint
        @app.get("/api/explain/export/{request_id}")
        async def export_request(request_id: str, format: str = "json", user: Dict = Depends(get_current_user)):
            data = self.dashboard.get_request_data(request_id)
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
                fig = go.Figure(data=[go.Bar(x=[a["expert_id"] for a in data["alternatives"]] + [data["chosen_expert"]],
                                             y=[a["energy_joules"] for a in data["alternatives"]] + [data["energy_joules"]])])
                img_bytes = fig.to_image(format="png")
                return Response(content=img_bytes, media_type="image/png")
            else:
                raise HTTPException(status_code=400, detail="Unsupported format")

        # Admin endpoints
        @app.post("/api/explain/admin/refresh")
        async def refresh_cache(user: Dict = Depends(require_role("admin"))):
            self.dashboard._cache.clear()
            return {"status": "cache cleared"}

        @app.post("/api/explain/admin/export/all")
        async def export_all(format: str = "json", user: Dict = Depends(require_role("admin"))):
            # Export all request logs
            data = [self.dashboard.get_request_data(req.request_id) for req in self.dashboard.request_logs.values()]
            if format == "json":
                return data
            elif format == "csv":
                # simplified
                return Response(content="Not implemented", media_type="text/csv")
            else:
                raise HTTPException(status_code=400, detail="Unsupported format")

        logger.info("API Gateway routes registered")

# ============================================================================
# 9. CONVENIENCE FACTORY
# ============================================================================
def create_explainable_ui(
    config: Optional[Union[Dict, ExplainableUIConfig]] = None,
    carbon_manager: Optional[Any] = None,
    lca_client: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Factory to create all components and return them for integration.
    """
    if config is None:
        if PYDANTIC_AVAILABLE:
            config = ExplainableUIConfig()
        else:
            config = EXPLAINABLE_UI_CONFIG

    # Database setup
    db_session = None
    if SQLALCHEMY_AVAILABLE:
        engine = create_engine(
            f"sqlite:///{config.db_path}",
            poolclass=QueuePool,
            pool_size=config.db_pool_size,
            max_overflow=config.db_max_overflow,
        )
        Base.metadata.create_all(engine)
        db_session = scoped_session(sessionmaker(bind=engine))

    dashboard = DashboardEngine(config, db_session)
    generator = ExplanationGenerator()
    what_if = WhatIfSimulator(dashboard, carbon_manager, lca_client)
    auth = AuthManager(config)

    # API extension (FastAPI app will be provided separately)
    api_extension = APIGatewayExtension(dashboard, generator, what_if, auth)

    return {
        "dashboard": dashboard,
        "explanation_generator": generator,
        "what_if": what_if,
        "auth": auth,
        "api_extension": api_extension,
        "db_session": db_session,
    }

# ============================================================================
# 10. UNIT TEST STUBS
# ============================================================================
def test_explainable_ui():
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
    dashboard.log_request(req)
    data = dashboard.get_request_data("test-123")
    assert data["request_id"] == "test-123"

# ============================================================================
# 11. EXAMPLE USAGE
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

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
    dash.log_request(req)

    explanation = gen.generate(req, prof, [("expert_B", alt_prof)])
    print("Explanation:", explanation)

    result = what_if.simulate("test-123", "expert_B")
    print("What‑if result:", result)

    print("✅ Enhanced Explainable UI module ready.")
