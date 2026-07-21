#!/usr/bin/env python3
"""
Human-AI Co-Evolution Engine for Sustainability v4.0.0
Enhanced with Pydantic validation, secure JSON persistence,
transformer-based sentiment, realistic simulation, Bayesian user modeling,
and production-grade reliability.

Author: Enhanced from original v3.1.0
"""

import asyncio
import logging
import os
import re
import hashlib
import json
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, Any, List, Optional, Tuple, Union, Callable, Protocol, TypeVar, cast
from dataclasses import dataclass, field
import numpy as np

# Third-party imports (install via pip)
try:
    import aiofiles
except ImportError:
    aiofiles = None  # Fallback to sync file I/O
try:
    from pydantic import BaseModel, Field, ValidationError, validator, ConfigDict
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    raise ImportError("pydantic and pydantic-settings are required")
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    # Provide dummy retry decorator if not installed
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

try:
    from transformers import pipeline
except ImportError:
    pipeline = None

try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Pydantic Models for Configuration and Input Validation
# ============================================================================

class CoEvolutionConfig(BaseSettings):
    """Configuration with environment variable support."""
    model_config = SettingsConfigDict(env_prefix="COEVO_", case_sensitive=False)

    # Learning parameters
    learning_rate: float = Field(0.01, ge=0.0, le=1.0)
    exploration_rate: float = Field(0.1, ge=0.0, le=1.0)

    # History limits
    feedback_history_limit: int = Field(10000, gt=0)
    user_model_limit: int = Field(1000, gt=0)
    policy_suggestions_limit: int = Field(1000, gt=0)
    collaborative_decisions_limit: int = Field(100, gt=0)

    # Consensus parameters
    default_consensus_threshold: float = Field(0.7, ge=0.5, le=1.0)

    # Simulation parameters
    simulation_steps: int = Field(10, ge=1)
    num_scenarios: int = Field(5, ge=1)
    confidence_level: float = Field(0.95, ge=0.8, le=0.99)
    carbon_noise_level: float = Field(0.02, ge=0.0)
    helium_noise_level: float = Field(0.02, ge=0.0)
    energy_noise_level: float = Field(0.02, ge=0.0)

    # Persistence
    persistence_path: str = Field("co_evolution_state.json")
    persistence_auto_save_interval: int = Field(60, ge=0)  # seconds, 0 = disabled

    # Telemetry
    telemetry_export_interval: int = Field(60, ge=1)
    telemetry_enable_prometheus: bool = Field(False)
    telemetry_prometheus_port: int = Field(8000, ge=1024)

    # Sentiment model (optional; if None use rule-based)
    sentiment_model_name: Optional[str] = Field(None)
    sentiment_model_device: Optional[str] = Field(None)

    # Advanced simulation coupling
    enable_simulation_coupling: bool = Field(True)
    renewable_influence_factor: float = Field(0.3, ge=0.0, le=1.0)

    # User clustering
    enable_user_clustering: bool = Field(True)
    clustering_update_interval: int = Field(3600)  # seconds

# ============================================================================
# Pydantic Models for Data Structures
# ============================================================================

class FeedbackEntry(BaseModel):
    """Validated feedback entry."""
    user_id: str
    policy_id: str
    feedback: Dict[str, Any]
    sentiment: Optional[Dict[str, Any]] = None
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

class UserModel(BaseModel):
    """Persistent user model."""
    preferences: Dict[str, float] = Field(default_factory=dict)
    history: List[FeedbackEntry] = Field(default_factory=list)
    trust_score: float = Field(0.5, ge=0.0, le=1.0)
    feedback_count: int = 0
    sentiment_score: float = Field(0.0, ge=-1.0, le=1.0)
    engagement_level: float = Field(0.5, ge=0.0, le=1.0)
    preference_timeline: List[Dict[str, Any]] = Field(default_factory=list)
    last_active: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    cluster_id: Optional[int] = None

class PolicySuggestion(BaseModel):
    """Validated policy suggestion."""
    timestamp: str
    context: Dict[str, Any]
    actions: List[str]
    rationale: List[str]
    expected_impact: Dict[str, float]
    explanations: List[str]
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    personalized: bool = False
    alternative_actions: List[Dict[str, str]] = Field(default_factory=list)

class CollaborativeDecision(BaseModel):
    """Validated collaborative decision."""
    selected_option: Dict[str, Any]
    scores: List[float]
    participants: List[str]
    consensus_reached: bool
    consensus_score: float = Field(0.0, ge=0.0, le=1.0)
    vote_distribution: Dict[str, Dict[str, Any]]
    consensus_threshold: Optional[float] = None
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    disagreeing_users: Optional[List[str]] = None
    disagreement_analysis: Optional[Dict[str, Any]] = None

class SimulationResult(BaseModel):
    """Result of a policy simulation."""
    carbon_trajectory: List[float]
    helium_trajectory: List[float]
    energy_trajectory: List[float]
    sustainability_score: float = Field(ge=0.0, le=1.0)
    confidence_intervals: Dict[str, Tuple[float, float]] = Field(default_factory=dict)
    probabilities: Dict[str, float] = Field(default_factory=dict)
    scenario_metadata: Dict[str, Any] = Field(default_factory=dict)

class EngineState(BaseModel):
    """Full engine state for persistence."""
    version: str = "4.0.0"
    config: CoEvolutionConfig
    feedback_history: List[FeedbackEntry] = Field(default_factory=list)
    user_models: Dict[str, UserModel] = Field(default_factory=dict)
    policy_suggestions: List[PolicySuggestion] = Field(default_factory=list)
    collaborative_decisions: List[CollaborativeDecision] = Field(default_factory=list)
    behavior_history: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)
    consensus_builders: Dict[str, Dict] = Field(default_factory=dict)
    last_save: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

# ============================================================================
# Retry and Circuit Breaker Helpers
# ============================================================================

def is_retryable_exception(e: Exception) -> bool:
    """Determine if an exception is retryable."""
    return isinstance(e, (IOError, TimeoutError, ConnectionError))

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(is_retryable_exception)
)
async def retry_async(func: Callable, *args, **kwargs) -> Any:
    """Retry an async function with exponential backoff."""
    return await func(*args, **kwargs)

class CircuitBreaker:
    """Simple circuit breaker for protecting failing operations."""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half-open

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        if self.state == "open":
            if (datetime.utcnow().timestamp() - self.last_failure_time) > self.recovery_timeout:
                self.state = "half-open"
            else:
                raise RuntimeError("Circuit breaker is open")
        try:
            result = await func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow().timestamp()
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
            raise e

# ============================================================================
# Sentiment Analyzer (Enhanced)
# ============================================================================

class SentimentAnalyzer:
    """
    Sentiment analysis with pluggable transformer model or rule-based fallback.
    """
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.model = None
        self.pipeline = None

        if config.sentiment_model_name and pipeline is not None:
            try:
                self.pipeline = pipeline(
                    "sentiment-analysis",
                    model=config.sentiment_model_name,
                    device=config.sentiment_model_device,
                    truncation=True
                )
                logger.info(f"Loaded sentiment model: {config.sentiment_model_name}")
            except Exception as e:
                logger.warning(f"Failed to load sentiment model: {e}, using rule-based")

        # Enhanced rule-based keywords with multi-word phrases
        self.sentiment_keywords = {
            'positive': {
                'excellent': 1.0, 'great': 0.8, 'good': 0.6, 'nice': 0.5,
                'happy': 0.7, 'satisfied': 0.8, 'impressed': 0.9, 'love': 1.0,
                'amazing': 1.0, 'perfect': 1.0, 'awesome': 0.9, 'fantastic': 1.0,
                'helpful': 0.6, 'useful': 0.5, 'improved': 0.7, 'better': 0.6,
                'efficient': 0.7, 'sustainable': 0.8, 'innovative': 0.9,
                'very good': 0.9, 'really nice': 0.8
            },
            'negative': {
                'bad': -0.6, 'terrible': -1.0, 'awful': -0.9, 'horrible': -1.0,
                'sad': -0.5, 'disappointed': -0.7, 'frustrated': -0.8, 'angry': -0.9,
                'useless': -0.7, 'broken': -0.8, 'confusing': -0.5, 'slow': -0.5,
                'worse': -0.6, 'issue': -0.4, 'problem': -0.5, 'error': -0.6,
                'wasteful': -0.7, 'inefficient': -0.6, 'unsustainable': -0.9,
                'very bad': -0.9, 'really poor': -0.8
            }
        }
        self.emotion_keywords = {
            'joy': ['happy', 'glad', 'delighted', 'pleased', 'joy', 'wonderful'],
            'trust': ['trust', 'confident', 'reliable', 'sure', 'dependable'],
            'fear': ['worry', 'afraid', 'scared', 'anxious', 'nervous', 'concern'],
            'surprise': ['surprised', 'amazed', 'astonished', 'shocked', 'unexpected'],
            'sadness': ['sad', 'depressed', 'unhappy', 'miserable', 'disappointed'],
            'disgust': ['disgusted', 'appalled', 'horrified', 'revolted'],
            'anger': ['angry', 'furious', 'outraged', 'irritated', 'annoyed'],
            'anticipation': ['expect', 'anticipate', 'look forward', 'hope', 'eager']
        }
        self.intensifiers = ['very', 'really', 'extremely', 'absolutely', 'completely',
                             'totally', 'highly', 'incredibly', 'remarkably', 'exceptionally']
        self.downtoners = ['somewhat', 'slightly', 'a bit', 'a little', 'fairly',
                           'moderately', 'kind of', 'sort of', 'rather']
        self.negations = ['not', 'never', 'none', 'nobody', 'no', 'neither', 'nor',
                          'hardly', 'scarcely', 'barely', 'no one', 'nothing', 'nowhere']
        logger.info("Sentiment Analyzer initialized (model: %s)", 
                    config.sentiment_model_name if config.sentiment_model_name else "rule-based")

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of a text string."""
        if not text or not text.strip():
            return {'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                    'emotions': {}, 'key_phrases': []}

        if self.pipeline is not None:
            try:
                result = self.pipeline(text[:512])[0]  # truncate
                label = result['label']
                score = result['score']
                if label == 'POSITIVE':
                    sentiment_score = score
                elif label == 'NEGATIVE':
                    sentiment_score = -score
                else:
                    sentiment_score = 0.0
                return {
                    'score': sentiment_score,
                    'confidence': score,
                    'sentiment': label.lower(),
                    'emotions': {},  # transformers doesn't provide emotions
                    'key_phrases': []  # would need NER
                }
            except Exception as e:
                logger.warning(f"Transformer sentiment failed: {e}, fallback to rule-based")

        # Enhanced rule-based fallback (as before but improved)
        text_lower = text.lower()
        words = text_lower.split()
        score = 0.0
        total_weight = 0.0
        negate_next = False

        for i, word in enumerate(words):
            if word in self.negations:
                negate_next = True
                continue
            multiplier = 1.0
            if i > 0 and words[i-1] in self.intensifiers:
                multiplier = 1.5
            elif i > 0 and words[i-1] in self.downtoners:
                multiplier = 0.6

            # Check multi-word phrases first
            for phrase, value in self.sentiment_keywords['positive'].items():
                if ' ' in phrase and phrase in text_lower:
                    score += value * multiplier
                    total_weight += 1.0
            for phrase, value in self.sentiment_keywords['negative'].items():
                if ' ' in phrase and phrase in text_lower:
                    score += value * multiplier
                    total_weight += 1.0

            for sentiment_type, keywords in self.sentiment_keywords.items():
                if word in keywords:
                    sentiment_value = keywords[word] * multiplier
                    if negate_next:
                        sentiment_value = -sentiment_value
                        negate_next = False
                    score += sentiment_value
                    total_weight += 1.0
                    break

        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.0
        score = max(-1.0, min(1.0, score))

        sentiment = 'positive' if score > 0.2 else 'negative' if score < -0.2 else 'neutral'
        confidence = min(0.95, total_weight / 10.0)

        emotions = self._detect_emotions(text_lower)
        key_phrases = self._extract_key_phrases(text)

        return {'score': score, 'confidence': confidence, 'sentiment': sentiment,
                'emotions': emotions, 'key_phrases': key_phrases}

    def _detect_emotions(self, text_lower: str) -> Dict[str, float]:
        emotions = {}
        for emotion, keywords in self.emotion_keywords.items():
            count = sum(1 for keyword in keywords if keyword in text_lower)
            if count > 0:
                emotions[emotion] = min(1.0, count / 3.0)
        if emotions:
            max_emotion = max(emotions.values())
            if max_emotion > 0:
                emotions = {k: v / max_emotion for k, v in emotions.items()}
        return emotions

    def _extract_key_phrases(self, text: str) -> List[str]:
        phrases = []
        quoted = re.findall(r'"([^"]*)"', text)
        if quoted:
            phrases.extend(quoted)
        indicators = ['especially', 'particularly', 'specifically', 'mainly', 'mostly',
                      'the issue is', 'the problem is', 'suggestion', 'recommendation']
        for indicator in indicators:
            if indicator in text.lower():
                parts = text.lower().split(indicator)
                if len(parts) > 1:
                    phrase = parts[1].strip()
                    if phrase and len(phrase) > 10:
                        phrases.append(phrase[:100])
        return list(set(phrases))[:5]

# ============================================================================
# Persistence Manager (Secure JSON with Versioning)
# ============================================================================

class CoEvolutionPersistenceManager:
    """Saves and loads engine state using JSON + Pydantic, with versioning."""
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker()
        self._auto_save_task: Optional[asyncio.Task] = None

    async def start_auto_save(self, engine: 'HumanAICoEvolutionEngine'):
        """Start background auto-save if interval > 0."""
        if self.config.persistence_auto_save_interval > 0:
            async def auto_save_loop():
                while True:
                    await asyncio.sleep(self.config.persistence_auto_save_interval)
                    await self.save_state(engine)
            self._auto_save_task = asyncio.create_task(auto_save_loop())
            logger.info(f"Auto-save every {self.config.persistence_auto_save_interval}s started")

    async def stop_auto_save(self):
        if self._auto_save_task:
            self._auto_save_task.cancel()
            try:
                await self._auto_save_task
            except asyncio.CancelledError:
                pass
            self._auto_save_task = None

    async def save_state(self, engine: 'HumanAICoEvolutionEngine') -> bool:
        """Save engine state to JSON file."""
        async with self._lock:
            try:
                # Build state model
                state = EngineState(
                    config=engine.config,
                    feedback_history=list(engine.feedback_history),
                    user_models={uid: model for uid, model in engine.user_models.items()},
                    policy_suggestions=list(engine.policy_suggestions),
                    collaborative_decisions=list(engine.collaborative_decisions),
                    behavior_history={uid: list(history) for uid, history in engine.behavior_history.items()},
                    consensus_builders=engine.consensus_builders
                )
                # Serialize to JSON with indentation
                json_str = state.model_dump_json(indent=2)
                if aiofiles:
                    async with aiofiles.open(self.path, 'w') as f:
                        await f.write(json_str)
                else:
                    with open(self.path, 'w') as f:
                        f.write(json_str)
                logger.info(f"State saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                raise  # let circuit breaker handle

    async def load_state(self, engine: 'HumanAICoEvolutionEngine') -> bool:
        """Load engine state from JSON file."""
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                if aiofiles:
                    async with aiofiles.open(self.path, 'r') as f:
                        json_str = await f.read()
                else:
                    with open(self.path, 'r') as f:
                        json_str = f.read()

                state = EngineState.model_validate_json(json_str)
                # Version check
                if state.version != "4.0.0":
                    logger.warning(f"State version mismatch: {state.version} != 4.0.0; attempting to load anyway")

                # Restore engine state
                engine.feedback_history = deque(state.feedback_history, maxlen=engine.config.feedback_history_limit)
                engine.user_models = state.user_models
                engine.policy_suggestions = deque(state.policy_suggestions, maxlen=engine.config.policy_suggestions_limit)
                engine.collaborative_decisions = deque(state.collaborative_decisions, maxlen=engine.config.collaborative_decisions_limit)
                engine.behavior_history = defaultdict(list)
                for uid, history in state.behavior_history.items():
                    engine.behavior_history[uid] = history
                engine.consensus_builders = state.consensus_builders
                logger.info(f"State loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                if aiofiles:
                    await aiofiles.os.remove(self.path)
                else:
                    os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Telemetry (Prometheus-friendly)
# ============================================================================

class CoEvolutionTelemetry:
    """Collects telemetry; can export Prometheus format."""
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self._lock = asyncio.Lock()
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self.alert_thresholds: Dict[str, Tuple[float, float]] = {}  # (low, high)
        self.last_alert_time: Dict[str, float] = {}

        if config.telemetry_enable_prometheus and PROMETHEUS_AVAILABLE:
            self._init_prometheus()
            self._start_http_server()

    def _init_prometheus(self):
        self.prom_counters = {}
        self.prom_gauges = {}
        self.prom_histograms = {}

    def _start_http_server(self):
        if PROMETHEUS_AVAILABLE:
            from prometheus_client import start_http_server
            start_http_server(self.config.telemetry_prometheus_port)
            logger.info(f"Prometheus metrics server on port {self.config.telemetry_prometheus_port}")

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.counters[key] += value
        if PROMETHEUS_AVAILABLE and self.config.telemetry_enable_prometheus:
            if key not in self.prom_counters:
                self.prom_counters[key] = Counter(key, metric_name)
            self.prom_counters[key].inc(value)

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.gauges[key] = value
        if PROMETHEUS_AVAILABLE and self.config.telemetry_enable_prometheus:
            if key not in self.prom_gauges:
                self.prom_gauges[key] = Gauge(key, metric_name)
            self.prom_gauges[key].set(value)

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.histograms[key].append(value)
        if len(self.histograms[key]) > 1000:
            self.histograms[key] = self.histograms[key][-1000:]
        if PROMETHEUS_AVAILABLE and self.config.telemetry_enable_prometheus:
            if key not in self.prom_histograms:
                self.prom_histograms[key] = Histogram(key, metric_name)
            self.prom_histograms[key].observe(value)

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        if PROMETHEUS_AVAILABLE and self.config.telemetry_enable_prometheus:
            return generate_latest().decode('utf-8')
        # Custom text format
        output = []
        for key, value in self.counters.items():
            output.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self.gauges.items():
            output.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, values in self.histograms.items():
            output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
        return "\n".join(output)

    def reset(self):
        self.counters.clear()
        self.gauges.clear()
        self.histograms.clear()

    def set_alert(self, metric_name: str, low: float, high: float):
        self.alert_thresholds[metric_name] = (low, high)

    async def check_alerts(self):
        for key, (low, high) in self.alert_thresholds.items():
            value = self.gauges.get(key)
            if value is not None:
                if value < low or value > high:
                    # Throttle alerts
                    now = datetime.utcnow().timestamp()
                    if now - self.last_alert_time.get(key, 0) > 300:
                        logger.warning(f"Alert: {key} = {value} outside [{low}, {high}]")
                        self.last_alert_time[key] = now

# ============================================================================
# Enhanced Human-AI Co-Evolution Engine
# ============================================================================

class HumanAICoEvolutionEngine:
    """
    Human-AI co-evolution engine for sustainability v4.0.0.
    """
    def __init__(self, config: Optional[CoEvolutionConfig] = None):
        self.config = config or CoEvolutionConfig()
        self._lock = asyncio.Lock()
        self._user_cluster_lock = asyncio.Lock()

        # History containers
        self.feedback_history = deque(maxlen=self.config.feedback_history_limit)
        self.user_models: Dict[str, UserModel] = {}
        self.policy_suggestions = deque(maxlen=self.config.policy_suggestions_limit)
        self.collaborative_decisions = deque(maxlen=self.config.collaborative_decisions_limit)

        # User behavior history (raw data for clustering)
        self.behavior_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.consensus_builders: Dict[str, Dict] = {}

        # Sentiment analyzer
        self.sentiment_analyzer = SentimentAnalyzer(self.config)

        # Persistence and telemetry
        self.persistence = CoEvolutionPersistenceManager(self.config)
        self.telemetry = CoEvolutionTelemetry(self.config)

        # Learning parameters
        self.learning_rate = self.config.learning_rate
        self.exploration_rate = self.config.exploration_rate

        # Cache for stats (invalidated on updates)
        self._stats_cache: Optional[Dict] = None
        self._stats_cache_time: Optional[datetime] = None
        self._stats_cache_ttl = 30  # seconds

        # User clustering
        self._cluster_model: Optional[Any] = None  # placeholder for clustering model
        self._last_cluster_update: Optional[datetime] = None

        # Start background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()

        # Load persisted state
        asyncio.create_task(self._load_state())

        logger.info("Human-AI Co-Evolution Engine v4.0.0 initialized")

    def _start_background_tasks(self):
        """Start background tasks for auto-save and telemetry alerts."""
        async def auto_save_task():
            await self.persistence.start_auto_save(self)
        self._background_tasks.append(asyncio.create_task(auto_save_task()))

        async def alert_check_task():
            while True:
                await asyncio.sleep(60)
                await self.telemetry.check_alerts()
        self._background_tasks.append(asyncio.create_task(alert_check_task()))

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    async def get_telemetry_export(self) -> str:
        return await self.telemetry.export()

    async def get_health_status(self) -> Dict[str, Any]:
        """Report health of the co-evolution engine."""
        return {
            'status': 'healthy' if self._calculate_health_score() > 0.5 else 'degraded',
            'score': min(1.0, self._calculate_health_score()),
            'details': {
                'feedback_count': len(self.feedback_history),
                'user_count': len(self.user_models),
                'suggestions_count': len(self.policy_suggestions),
                'decisions_count': len(self.collaborative_decisions),
                'persistence_enabled': self.persistence is not None,
                'telemetry_active': True,
                'avg_sentiment': self._get_avg_sentiment(),
                'avg_trust': self._get_avg_trust(),
                'cluster_count': len(set(m.cluster_id for m in self.user_models.values() if m.cluster_id is not None)) if self.config.enable_user_clustering else 0
            }
        }

    def _calculate_health_score(self) -> float:
        if not self.user_models:
            return 0.5
        trust_scores = [m.trust_score for m in self.user_models.values()]
        engagement = [m.engagement_level for m in self.user_models.values()]
        avg_trust = np.mean(trust_scores)
        avg_engagement = np.mean(engagement)
        return (avg_trust * 0.6 + avg_engagement * 0.4)

    def _get_avg_sentiment(self) -> float:
        sentiments = []
        for feedback in self.feedback_history:
            if feedback.sentiment:
                sentiments.append(feedback.sentiment.get('score', 0))
        return np.mean(sentiments) if sentiments else 0.0

    def _get_avg_trust(self) -> float:
        if not self.user_models:
            return 0.0
        return np.mean([m.trust_score for m in self.user_models.values()])

    # ========================================================================
    # Feedback Recording
    # ========================================================================

    async def record_feedback(
        self,
        user_id: str,
        policy_id: str,
        feedback: Dict[str, Any]
    ):
        """Record user feedback with validation and sentiment analysis."""
        async with self._lock:
            # Validate input
            try:
                feedback_entry = FeedbackEntry(
                    user_id=user_id,
                    policy_id=policy_id,
                    feedback=feedback,
                    sentiment=None
                )
            except ValidationError as e:
                logger.error(f"Invalid feedback: {e}")
                raise ValueError(f"Invalid feedback: {e}")

            # Sentiment analysis
            sentiment = None
            if 'comment' in feedback and feedback['comment']:
                sentiment = self.sentiment_analyzer.analyze_sentiment(feedback['comment'])
                feedback_entry.sentiment = sentiment

            # Store in history
            self.feedback_history.append(feedback_entry)

            # Update user model
            if user_id not in self.user_models:
                self.user_models[user_id] = UserModel()

            user_model = self.user_models[user_id]

            # Update preferences
            if 'preferences' in feedback:
                for key, value in feedback['preferences'].items():
                    user_model.preferences[key] = value
                    user_model.preference_timeline.append({
                        'timestamp': datetime.utcnow().isoformat(),
                        'key': key,
                        'value': value
                    })

            user_model.history.append(feedback_entry)
            user_model.feedback_count += 1
            user_model.last_active = datetime.utcnow().isoformat()

            # Update trust score using Bayesian approach
            rating = feedback.get('rating', 0)
            sentiment_score = sentiment['score'] if sentiment else 0
            combined_score = (rating / 5.0) * 0.6 + (sentiment_score + 1.0) / 2.0 * 0.4

            # Bayesian update: posterior = prior + evidence
            prior = user_model.trust_score
            evidence_weight = 0.1 + 0.05 * min(user_model.feedback_count, 20) / 20
            posterior = prior * (1 - evidence_weight) + combined_score * evidence_weight
            user_model.trust_score = max(0.0, min(1.0, posterior))

            # Update sentiment tracking
            if sentiment:
                user_model.sentiment_score = (
                    user_model.sentiment_score * 0.9 + sentiment['score'] * 0.1
                )

            # Update engagement level
            engagement = user_model.feedback_count / 20.0
            user_model.engagement_level = min(1.0, engagement)

            # Store behavior history
            self.behavior_history[user_id].append({
                'timestamp': datetime.utcnow().isoformat(),
                'policy_id': policy_id,
                'rating': rating,
                'sentiment': sentiment_score if sentiment else 0,
                'trust_score': user_model.trust_score
            })

            # Telemetry
            self.telemetry.increment('feedback_received')
            self.telemetry.gauge('trust_score', user_model.trust_score, {'user_id': user_id})
            if sentiment:
                self.telemetry.gauge('sentiment_score', sentiment_score, {'user_id': user_id})

            # Invalidate stats cache
            self._stats_cache = None

            # Trigger cluster update if needed
            if self.config.enable_user_clustering:
                asyncio.create_task(self._update_clusters_if_needed())

            logger.info(f"Feedback recorded from {user_id} on {policy_id} (sentiment: {sentiment_score:.2f})")

    # ========================================================================
    # User Clustering
    # ========================================================================

    async def _update_clusters_if_needed(self):
        """Update user clusters periodically."""
        if not self.config.enable_user_clustering:
            return
        now = datetime.utcnow()
        if (self._last_cluster_update is None or
            (now - self._last_cluster_update).total_seconds() > self.config.clustering_update_interval):
            async with self._user_cluster_lock:
                await self._update_clusters()
                self._last_cluster_update = now

    async def _update_clusters(self):
        """Perform clustering of users based on preferences and trust."""
        users = list(self.user_models.values())
        if len(users) < 3:
            return

        # Extract feature vectors: preferences + trust + engagement
        features = []
        user_ids = []
        for uid, model in self.user_models.items():
            vec = [
                model.trust_score,
                model.engagement_level,
                model.sentiment_score,
                model.preferences.get('sustainability', 0.5),
                model.preferences.get('cost', 0.5),
                model.preferences.get('speed', 0.5),
                model.preferences.get('risk', 0.5)
            ]
            features.append(vec)
            user_ids.append(uid)

        from sklearn.cluster import KMeans
        k = min(5, len(users) // 2)
        if k < 2:
            k = 1
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(features)

        for uid, label in zip(user_ids, labels):
            self.user_models[uid].cluster_id = int(label)

        self._cluster_model = kmeans
        logger.info(f"Clustered {len(users)} users into {k} clusters")

    # ========================================================================
    # Policy Suggestion
    # ========================================================================

    async def generate_policy_suggestion(
        self,
        context: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate personalized policy suggestion with explanations."""
        async with self._lock:
            # Validate context
            try:
                # Could use Pydantic model for context validation
                pass
            except ValidationError as e:
                logger.error(f"Invalid context: {e}")
                raise ValueError(f"Invalid context: {e}")

            suggestion = PolicySuggestion(
                timestamp=datetime.utcnow().isoformat(),
                context=context,
                actions=[],
                rationale=[],
                expected_impact={},
                explanations=[],
                confidence=0.5,
                personalized=False,
                alternative_actions=[]
            )

            # Base recommendations (same logic but improved)
            recommendations = []
            carbon_intensity = context.get('carbon_intensity', 400)
            if carbon_intensity > 500:
                recommendations.append({
                    'action': 'Reduce carbon-intensive operations',
                    'rationale': f'High carbon intensity detected: {carbon_intensity:.0f} gCO₂/kWh',
                    'impact': {'carbon_reduction': 0.3},
                    'explanation': 'High carbon intensity indicates the current energy grid is carbon-heavy. '
                                  'Reducing operations during peak carbon periods can significantly lower your footprint.'
                })

            helium_scarcity = context.get('helium_scarcity', 0)
            if helium_scarcity > 0.7:
                recommendations.append({
                    'action': 'Conserve helium usage',
                    'rationale': f'Helium scarcity is critical: {helium_scarcity:.0%}',
                    'impact': {'helium_savings': 0.2},
                    'explanation': 'Helium is a finite resource with critical shortages. '
                                  'Conserving helium now ensures availability for future critical operations.'
                })

            energy_price = context.get('energy_price', 0)
            if energy_price > 0.15:
                recommendations.append({
                    'action': 'Optimize energy consumption',
                    'rationale': f'High energy prices: ${energy_price:.2f}/kWh',
                    'impact': {'energy_savings': 0.25},
                    'explanation': 'Energy prices are currently above average. '
                                  'Optimizing consumption can reduce operational costs and environmental impact.'
                })

            renewable_ratio = context.get('renewable_ratio', 0)
            if renewable_ratio < 0.3:
                recommendations.append({
                    'action': 'Increase renewable energy usage',
                    'rationale': f'Low renewable ratio: {renewable_ratio:.0%}',
                    'impact': {'renewable_increase': 0.2},
                    'explanation': 'Your current renewable energy usage is below target. '
                                  'Switching to renewable sources can dramatically reduce carbon emissions.'
                })

            # Personalization with cluster-aware recommendations
            if user_id and user_id in self.user_models:
                user_model = self.user_models[user_id]
                preferences = user_model.preferences
                trust = user_model.trust_score
                cluster_id = user_model.cluster_id

                # Cluster-based filtering
                if cluster_id is not None and self._cluster_model is not None:
                    # Get cluster centroid preferences
                    # Could recommend policies based on cluster average
                    pass

                # Filter based on risk tolerance
                if preferences.get('risk_tolerance', 'medium') == 'low':
                    recommendations = [r for r in recommendations if
                                     'optimize' not in r['action'].lower() or
                                     'reduce' in r['action'].lower()]

                # Sort based on sustainability preference
                if preferences.get('sustainability_focus', False):
                    recommendations = sorted(recommendations,
                                           key=lambda r: r['impact'].get('carbon_reduction', 0) +
                                                         r['impact'].get('helium_savings', 0),
                                           reverse=True)

                # Sort based on cost preference
                if preferences.get('cost_focus', False):
                    recommendations = sorted(recommendations,
                                           key=lambda r: r['impact'].get('energy_savings', 0),
                                           reverse=True)

                if trust > 0.7:
                    suggestion.personalized = True
                    suggestion.confidence = trust
                    suggestion.explanations.append(
                        f"This suggestion is personalized based on your preferences "
                        f"(trust score: {trust:.1%})"
                    )

            # Apply top recommendations
            for rec in recommendations[:3]:
                suggestion.actions.append(rec['action'])
                suggestion.rationale.append(rec['rationale'])
                for key, value in rec['impact'].items():
                    suggestion.expected_impact[key] = suggestion.expected_impact.get(key, 0) + value
                if 'explanation' in rec:
                    suggestion.explanations.append(rec['explanation'])

            # Alternative actions
            if len(recommendations) > 3:
                suggestion.alternative_actions = [
                    {'action': r['action'], 'rationale': r['rationale']}
                    for r in recommendations[3:]
                ]

            self.policy_suggestions.append(suggestion)
            self.telemetry.increment('suggestions_generated')
            self._stats_cache = None

            return suggestion.model_dump()

    # ========================================================================
    # Policy Simulation (Enhanced with Coupling)
    # ========================================================================

    async def simulate_policy_impact(
        self,
        policy: Dict[str, Any],
        simulation_steps: Optional[int] = None,
        num_scenarios: Optional[int] = None,
        confidence_level: Optional[float] = None
    ) -> SimulationResult:
        """Simulate policy impact with coupled dynamics and scenario analysis."""
        steps = simulation_steps or self.config.simulation_steps
        scenarios = num_scenarios or self.config.num_scenarios
        conf_level = confidence_level or self.config.confidence_level

        # Get policy actions
        actions = policy.get('actions', [])

        all_carbon = []
        all_helium = []
        all_energy = []
        all_scores = []

        for scenario in range(scenarios):
            np.random.seed(scenario * 12345)

            # Initial states with noise
            current_carbon = 400 + np.random.normal(0, 20)
            current_helium = 0.5 + np.random.normal(0, 0.05)
            current_energy = 0.5 + np.random.normal(0, 0.05)
            current_renewable = 0.3 + np.random.normal(0, 0.03)

            carbon_traj = []
            helium_traj = []
            energy_traj = []

            # Noise generators
            carbon_noise = np.random.normal(0, self.config.carbon_noise_level, steps)
            helium_noise = np.random.normal(0, self.config.helium_noise_level, steps)
            energy_noise = np.random.normal(0, self.config.energy_noise_level, steps)

            for step in range(steps):
                # Coupled dynamics
                if self.config.enable_simulation_coupling:
                    # Renewable energy affects carbon intensity
                    renewable_increase = 0.01 if 'increase_renewable' in actions else 0
                    current_renewable += renewable_increase + np.random.normal(0, 0.005)
                    current_renewable = max(0.1, min(0.9, current_renewable))

                    # Carbon reduction influenced by renewable ratio
                    carbon_reduction_rate = 0.95 - 0.1 * current_renewable
                    if 'reduce_carbon' in actions:
                        current_carbon *= (carbon_reduction_rate + carbon_noise[step] * 0.02)
                    else:
                        current_carbon *= (1.0 + carbon_noise[step] * 0.02)

                    # Helium conservation
                    if 'conserve_helium' in actions:
                        current_helium *= (0.97 + helium_noise[step] * 0.02)
                    else:
                        current_helium *= (1.0 + helium_noise[step] * 0.02)

                    # Energy optimization
                    if 'optimize_energy' in actions:
                        current_energy *= (0.98 + energy_noise[step] * 0.02)
                    else:
                        current_energy *= (1.0 + energy_noise[step] * 0.02)
                else:
                    # Original independent dynamics
                    if 'reduce_carbon' in actions:
                        current_carbon *= (0.95 + carbon_noise[step] * 0.02)
                    if 'conserve_helium' in actions:
                        current_helium *= (0.97 + helium_noise[step] * 0.02)
                    if 'optimize_energy' in actions:
                        current_energy *= (0.98 + energy_noise[step] * 0.02)

                carbon_traj.append(current_carbon)
                helium_traj.append(current_helium)
                energy_traj.append(current_energy)

            carbon_score = 1.0 - (carbon_traj[-1] - 300) / 700
            sustainability_score = (
                max(0, min(1, carbon_score)) * 0.4 +
                current_helium * 0.3 +
                (1.0 - current_energy) * 0.3
            )

            all_carbon.append(carbon_traj)
            all_helium.append(helium_traj)
            all_energy.append(energy_traj)
            all_scores.append(sustainability_score)

        # Mean trajectories
        mean_carbon = np.mean(all_carbon, axis=0).tolist()
        mean_helium = np.mean(all_helium, axis=0).tolist()
        mean_energy = np.mean(all_energy, axis=0).tolist()
        mean_score = np.mean(all_scores)

        # Confidence intervals
        alpha = 1.0 - conf_level
        z_score = 1.96  # For 95% confidence

        lower_carbon = (np.mean(all_carbon, axis=0) - z_score * np.std(all_carbon, axis=0)).tolist()
        upper_carbon = (np.mean(all_carbon, axis=0) + z_score * np.std(all_carbon, axis=0)).tolist()
        lower_helium = (np.mean(all_helium, axis=0) - z_score * np.std(all_helium, axis=0)).tolist()
        upper_helium = (np.mean(all_helium, axis=0) + z_score * np.std(all_helium, axis=0)).tolist()
        lower_energy = (np.mean(all_energy, axis=0) - z_score * np.std(all_energy, axis=0)).tolist()
        upper_energy = (np.mean(all_energy, axis=0) + z_score * np.std(all_energy, axis=0)).tolist()

        improvement_prob = sum(1 for s in all_scores if s > 0.5) / len(all_scores)

        return SimulationResult(
            carbon_trajectory=mean_carbon,
            helium_trajectory=mean_helium,
            energy_trajectory=mean_energy,
            sustainability_score=mean_score,
            confidence_intervals={
                'carbon': (lower_carbon[0], upper_carbon[0]) if lower_carbon and upper_carbon else (0, 0),
                'helium': (lower_helium[0], upper_helium[0]) if lower_helium and upper_helium else (0, 0),
                'energy': (lower_energy[0], upper_energy[0]) if lower_energy and upper_energy else (0, 0)
            },
            probabilities={
                'improvement': improvement_prob,
                'significant_improvement': sum(1 for s in all_scores if s > 0.7) / len(all_scores),
                'sustainability_target': sum(1 for s in all_scores if s > 0.6) / len(all_scores)
            },
            scenario_metadata={
                'num_scenarios': scenarios,
                'confidence_level': conf_level,
                'simulation_steps': steps,
                'carbon_noise_level': self.config.carbon_noise_level,
                'helium_noise_level': self.config.helium_noise_level,
                'energy_noise_level': self.config.energy_noise_level,
                'coupling_enabled': self.config.enable_simulation_coupling
            }
        )

    # ========================================================================
    # Collaborative Decision Making (with Range Voting)
    # ========================================================================

    async def collaborative_decision(
        self,
        users: List[str],
        options: List[Dict[str, Any]],
        require_consensus: bool = False,
        consensus_threshold: Optional[float] = None,
        voting_method: str = "range"  # "range" or "quadratic"
    ) -> Dict[str, Any]:
        """Facilitate collaborative decision with range/quadratic voting."""
        threshold = consensus_threshold or self.config.default_consensus_threshold

        async with self._lock:
            user_votes = {}
            user_weights = {}

            for user_id in users:
                if user_id in self.user_models:
                    user_model = self.user_models[user_id]
                    preferences = user_model.preferences
                    trust = user_model.trust_score
                    engagement = user_model.engagement_level

                    weight = trust * (0.5 + 0.5 * engagement)
                    user_weights[user_id] = weight

                    votes = []
                    for option in options:
                        score = 0
                        if 'sustainability' in preferences:
                            score += option.get('sustainability', 0.5) * preferences.get('sustainability', 0.5)
                        if 'cost' in preferences:
                            score += option.get('cost', 0.5) * preferences.get('cost', 0.5)
                        if 'speed' in preferences:
                            score += option.get('speed', 0.5) * preferences.get('speed', 0.5)
                        if 'risk' in preferences:
                            score += option.get('risk', 0.5) * (1.0 - preferences.get('risk', 0.5))
                        votes.append(score)

                    if votes and max(votes) > 0:
                        votes = [v / max(votes) for v in votes]
                    user_votes[user_id] = votes

            if not user_votes:
                decision = CollaborativeDecision(
                    selected_option=options[0],
                    scores=[1.0],
                    participants=[],
                    consensus_reached=False,
                    consensus_score=0.0,
                    vote_distribution={},
                    consensus_threshold=None
                )
                self.collaborative_decisions.append(decision)
                return decision.model_dump()

            # Weighted aggregation
            aggregated = [0.0] * len(options)
            total_weight = 0.0
            for user_id, votes in user_votes.items():
                weight = user_weights.get(user_id, 0.5)
                if voting_method == "quadratic":
                    # Quadratic voting: cost = (votes)^2, so we scale votes accordingly
                    # For simplicity, we use a quadratic transformation
                    votes = [v ** 2 if v > 0 else 0 for v in votes]
                for i, v in enumerate(votes):
                    aggregated[i] += v * weight
                total_weight += weight

            if total_weight > 0:
                aggregated = [a / total_weight for a in aggregated]

            if aggregated and max(aggregated) > 0:
                aggregated = [a / max(aggregated) for a in aggregated]

            # Consensus calculation
            if require_consensus:
                max_score = max(aggregated)
                second_max = sorted(aggregated)[-2] if len(aggregated) > 1 else 0
                margin = max_score - second_max
                consensus_score = margin / max_score if max_score > 0 else 0
                consensus_reached = consensus_score > (1.0 - threshold)
            else:
                consensus_score = 0.5
                consensus_reached = True

            best_idx = aggregated.index(max(aggregated))

            vote_distribution = {}
            for user_id, votes in user_votes.items():
                if votes:
                    vote_distribution[user_id] = {
                        'preferred_option': votes.index(max(votes)),
                        'scores': votes,
                        'weight': user_weights.get(user_id, 0.5)
                    }

            decision = CollaborativeDecision(
                selected_option=options[best_idx],
                scores=aggregated,
                participants=list(user_votes.keys()),
                consensus_reached=consensus_reached,
                consensus_score=consensus_score,
                vote_distribution=vote_distribution,
                consensus_threshold=threshold if require_consensus else None,
                timestamp=datetime.utcnow().isoformat()
            )

            if not consensus_reached:
                disagreeing_users = []
                for user_id, votes in user_votes.items():
                    if votes and votes.index(max(votes)) != best_idx:
                        disagreeing_users.append(user_id)
                decision.disagreeing_users = disagreeing_users
                decision.disagreement_analysis = self._analyze_disagreement(
                    user_votes, options, disagreeing_users
                )

            self.collaborative_decisions.append(decision)
            self.telemetry.increment('collaborative_decisions')
            self.telemetry.gauge('consensus_score', consensus_score)
            self._stats_cache = None
            return decision.model_dump()

    def _analyze_disagreement(
        self,
        user_votes: Dict[str, List[float]],
        options: List[Dict[str, Any]],
        disagreeing_users: List[str]
    ) -> Dict[str, Any]:
        """Analyze disagreement patterns among users."""
        if not disagreeing_users:
            return {'pattern': 'no_disagreement'}

        preference_patterns = {}
        for user_id in disagreeing_users:
            if user_id in self.user_models:
                prefs = self.user_models[user_id].preferences
                for key, value in prefs.items():
                    if key not in preference_patterns:
                        preference_patterns[key] = []
                    preference_patterns[key].append(value)

        patterns = {}
        for key, values in preference_patterns.items():
            if values:
                avg = np.mean(values)
                if avg > 0.6:
                    patterns[key] = {'value': avg, 'interpretation': f'Strong preference for {key}'}
                elif avg < 0.4:
                    patterns[key] = {'value': avg, 'interpretation': f'Weak preference for {key}'}

        return {
            'pattern': 'preference_divergence' if patterns else 'other',
            'preference_patterns': patterns,
            'disagreeing_users_count': len(disagreeing_users),
            'suggestion': 'Consider a compromise option that balances conflicting preferences'
        }

    # ========================================================================
    # User Behavior Modeling (Enhanced with Trend Detection)
    # ========================================================================

    def predict_user_preferences(self, user_id: str, days: int = 30) -> Dict[str, Any]:
        """Predict future preferences with trend analysis and confidence."""
        if user_id not in self.behavior_history:
            return {'status': 'insufficient_data'}

        history = self.behavior_history[user_id]
        if len(history) < 5:
            return {'status': 'insufficient_data', 'sample_size': len(history)}

        preferences = self.user_models.get(user_id, {}).preferences if user_id in self.user_models else {}
        predictions = {}

        for key, value in preferences.items():
            recent_values = []
            for entry in history[-20:]:
                if entry.get('preference_timeline'):
                    for pref in entry['preference_timeline']:
                        if pref.get('key') == key:
                            recent_values.append(pref.get('value', value))

            if recent_values:
                weights = [0.9 ** (len(recent_values) - i) for i in range(len(recent_values))]
                weighted_avg = np.average(recent_values, weights=weights)

                if len(recent_values) > 5:
                    # Use linear regression with uncertainty
                    x = np.arange(len(recent_values[-5:]))
                    y = np.array(recent_values[-5:])
                    slope, intercept = np.polyfit(x, y, 1)
                    # Compute confidence interval of slope
                    if len(x) > 2:
                        residuals = y - (slope * x + intercept)
                        std_err = np.std(residuals) / np.std(x) / np.sqrt(len(x))
                        t_val = 1.96  # 95% confidence
                        slope_ci = t_val * std_err
                        trend_confidence = max(0, 1 - slope_ci / abs(slope)) if abs(slope) > 0 else 0
                    else:
                        trend_confidence = 0.5
                    predicted_value = weighted_avg + slope * (days / 30)
                else:
                    predicted_value = weighted_avg
                    trend_confidence = 0.3

                predictions[key] = {
                    'predicted_value': max(0, min(1, predicted_value)),
                    'current_value': value,
                    'confidence': min(0.9, len(recent_values) / 20),
                    'trend': 'increasing' if predicted_value > value * 1.05 else 'decreasing' if predicted_value < value * 0.95 else 'stable',
                    'trend_confidence': trend_confidence
                }

        return {
            'user_id': user_id,
            'predictions': predictions,
            'sample_size': len(history),
            'prediction_horizon_days': days,
            'confidence': min(0.8, len(history) / 50)
        }

    # ========================================================================
    # Explanation Generation
    # ========================================================================

    def generate_explanation(
        self,
        suggestion: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate human-readable explanations for a policy suggestion."""
        # Convert to Pydantic model for safety
        suggestion_model = PolicySuggestion(**suggestion)

        explanation = {
            'summary': '',
            'detailed': [],
            'why': [],
            'impact': '',
            'uncertainty': '',
            'next_steps': []
        }

        if suggestion_model.actions:
            actions_text = ' and '.join(suggestion_model.actions[:2])
            if len(suggestion_model.actions) > 2:
                actions_text += f' and {len(suggestion_model.actions) - 2} other actions'
            explanation['summary'] = f"Based on current sustainability metrics, we recommend {actions_text}."
        else:
            explanation['summary'] = "Current sustainability metrics are within acceptable ranges. Continue monitoring."

        for action in suggestion_model.actions:
            explanation['detailed'].append(action)

        for rationale in suggestion_model.rationale:
            explanation['why'].append(rationale)

        if suggestion_model.expected_impact:
            impact_parts = []
            for key, value in suggestion_model.expected_impact.items():
                impact_parts.append(f"{key.replace('_', ' ')}: {value:.0%}")
            explanation['impact'] = f"Expected impact: {', '.join(impact_parts)}"
        else:
            explanation['impact'] = "No significant impact expected."

        confidence = suggestion_model.confidence
        if confidence > 0.8:
            explanation['uncertainty'] = "High confidence in this recommendation based on strong evidence."
        elif confidence > 0.6:
            explanation['uncertainty'] = "Moderate confidence. Additional feedback would improve accuracy."
        else:
            explanation['uncertainty'] = "Low confidence. More data and feedback are needed."

        explanation['next_steps'] = [
            "Review the suggested actions and their expected impact",
            "Provide feedback on this suggestion to improve future recommendations",
            "Monitor the metrics after implementing changes"
        ]

        if user_id and user_id in self.user_models:
            user_model = self.user_models[user_id]
            trust = user_model.trust_score
            explanation['personalized'] = {
                'trust_score': trust,
                'personalization_factors': user_model.preferences,
                'note': f"This explanation is personalized based on your historical interactions."
            }

        return explanation

    # ========================================================================
    # Statistics and Reporting (with Caching)
    # ========================================================================

    def get_coevolution_stats(self) -> Dict[str, Any]:
        """Get comprehensive co-evolution statistics with caching."""
        now = datetime.utcnow()
        if (self._stats_cache is not None and
            self._stats_cache_time is not None and
            (now - self._stats_cache_time).total_seconds() < self._stats_cache_ttl):
            return self._stats_cache

        stats = {
            'total_feedback': len(self.feedback_history),
            'unique_users': len(self.user_models),
            'policy_suggestions': len(self.policy_suggestions),
            'collaborative_decisions': len(self.collaborative_decisions),
            'average_trust': np.mean([
                model.trust_score for model in self.user_models.values()
            ]) if self.user_models else 0,
            'average_sentiment': np.mean([
                model.sentiment_score for model in self.user_models.values()
            ]) if self.user_models else 0,
            'high_trust_users': sum(1 for model in self.user_models.values()
                                   if model.trust_score > 0.7),
            'total_feedback_with_sentiment': sum(1 for f in self.feedback_history
                                                if f.sentiment is not None),
            'engagement_stats': {
                'active_users': sum(1 for model in self.user_models.values()
                                  if model.engagement_level > 0.5),
                'avg_engagement': np.mean([
                    model.engagement_level for model in self.user_models.values()
                ]) if self.user_models else 0
            },
            'cluster_stats': {
                'num_clusters': len(set(m.cluster_id for m in self.user_models.values() if m.cluster_id is not None)) if self.config.enable_user_clustering else 0,
                'cluster_distribution': self._get_cluster_distribution() if self.config.enable_user_clustering else {}
            }
        }

        self._stats_cache = stats
        self._stats_cache_time = now
        return stats

    def _get_cluster_distribution(self) -> Dict[int, int]:
        """Return count of users per cluster."""
        dist = defaultdict(int)
        for model in self.user_models.values():
            if model.cluster_id is not None:
                dist[model.cluster_id] += 1
        return dict(dist)

    def get_user_insights(self, user_id: str) -> Dict[str, Any]:
        """Get detailed insights for a specific user."""
        if user_id not in self.user_models:
            return {'status': 'user_not_found'}

        user_model = self.user_models[user_id]
        preference_prediction = self.predict_user_preferences(user_id)

        return {
            'user_id': user_id,
            'preferences': user_model.preferences,
            'trust_score': user_model.trust_score,
            'feedback_count': user_model.feedback_count,
            'sentiment_score': user_model.sentiment_score,
            'engagement_level': user_model.engagement_level,
            'cluster_id': user_model.cluster_id,
            'preference_prediction': preference_prediction,
            'last_active': user_model.last_active,
            'history_summary': {
                'recent_feedback': [f.model_dump() for f in user_model.history[-5:]] if user_model.history else [],
                'total_interactions': len(user_model.history)
            }
        }

    def get_sentiment_summary(self) -> Dict[str, Any]:
        """Get summary of sentiment analysis across all feedback."""
        if not self.feedback_history:
            return {'status': 'no_feedback'}

        sentiments = []
        for feedback in self.feedback_history:
            if feedback.sentiment:
                sentiments.append(feedback.sentiment.get('score', 0))

        if not sentiments:
            return {'status': 'no_sentiment_data'}

        policy_sentiments = defaultdict(list)
        for feedback in self.feedback_history:
            if feedback.sentiment and feedback.policy_id:
                policy_sentiments[feedback.policy_id].append(
                    feedback.sentiment.get('score', 0)
                )

        policy_summary = {}
        for policy_id, scores in policy_sentiments.items():
            policy_summary[policy_id] = {
                'average_sentiment': np.mean(scores),
                'sample_count': len(scores),
                'positive_ratio': sum(1 for s in scores if s > 0.2) / len(scores),
                'negative_ratio': sum(1 for s in scores if s < -0.2) / len(scores)
            }

        return {
            'average_sentiment': np.mean(sentiments),
            'positive_ratio': sum(1 for s in sentiments if s > 0.2) / len(sentiments),
            'negative_ratio': sum(1 for s in sentiments if s < -0.2) / len(sentiments),
            'neutral_ratio': sum(1 for s in sentiments if -0.2 <= s <= 0.2) / len(sentiments),
            'sample_count': len(sentiments),
            'trend': 'improving' if len(sentiments) > 10 and
                     np.mean(sentiments[-5:]) > np.mean(sentiments[:5]) else 'stable',
            'by_policy': policy_summary,
            'top_positive_policies': sorted(
                policy_summary.items(),
                key=lambda x: x[1]['average_sentiment'],
                reverse=True
            )[:3],
            'top_negative_policies': sorted(
                policy_summary.items(),
                key=lambda x: x[1]['average_sentiment']
            )[:3]
        }

    def get_consensus_analysis(self) -> Dict[str, Any]:
        """Analyze consensus patterns in collaborative decisions."""
        if not self.collaborative_decisions:
            return {'status': 'no_decisions'}

        decisions = list(self.collaborative_decisions)
        total_decisions = len(decisions)
        consensus_reached = sum(1 for d in decisions if d.consensus_reached)

        avg_consensus_score = np.mean([
            d.consensus_score for d in decisions
        ])

        participants_per_decision = [
            len(d.participants) for d in decisions
        ]

        return {
            'total_decisions': total_decisions,
            'consensus_rate': consensus_reached / total_decisions if total_decisions else 0,
            'average_consensus_score': avg_consensus_score,
            'average_participants': np.mean(participants_per_decision) if participants_per_decision else 0,
            'max_participants': max(participants_per_decision) if participants_per_decision else 0,
            'recent_decisions': [d.model_dump() for d in decisions[-5:]] if decisions else [],
            'recommendations': self._generate_consensus_recommendations(decisions)
        }

    def _generate_consensus_recommendations(self, decisions: List[CollaborativeDecision]) -> List[str]:
        """Generate recommendations based on consensus analysis."""
        recommendations = []
        if not decisions:
            return recommendations

        consensus_rate = sum(1 for d in decisions if d.consensus_reached) / len(decisions)
        if consensus_rate < 0.5:
            recommendations.append("Low consensus rate - consider facilitating more discussion")
            recommendations.append("Identify and address sources of disagreement")

        avg_participants = np.mean([len(d.participants) for d in decisions])
        if avg_participants < 3:
            recommendations.append("Low participation - encourage more users to join decisions")

        return recommendations

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down Human-AI Co-Evolution Engine")
        await self.persistence.stop_auto_save()
        await self.save_state()
        for task in self._background_tasks:
            task.cancel()
        logger.info("Shutdown complete")

# ============================================================================
# Example Usage (if run directly)
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main():
        config = CoEvolutionConfig()
        engine = HumanAICoEvolutionEngine(config)

        # Simulate some feedback
        await engine.record_feedback(
            user_id="user1",
            policy_id="policy1",
            feedback={
                "rating": 4,
                "comment": "Great sustainability policy, very effective!",
                "preferences": {"sustainability": 0.8, "cost": 0.3}
            }
        )

        # Generate suggestion
        suggestion = await engine.generate_policy_suggestion(
            context={"carbon_intensity": 550, "energy_price": 0.18},
            user_id="user1"
        )
        print("Suggestion:", suggestion)

        # Simulate impact
        result = await engine.simulate_policy_impact(
            policy={"actions": ["reduce_carbon", "optimize_energy"]}
        )
        print("Simulation result:", result.model_dump())

        # Collaborative decision
        decision = await engine.collaborative_decision(
            users=["user1", "user2", "user3"],
            options=[{"sustainability": 0.9, "cost": 0.2}, {"sustainability": 0.6, "cost": 0.5}],
            require_consensus=True
        )
        print("Decision:", decision)

        # Stats
        stats = engine.get_coevolution_stats()
        print("Stats:", stats)

        await engine.shutdown()

    asyncio.run(main())
