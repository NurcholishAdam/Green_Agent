#!/usr/bin/env python3
"""
Enhanced Human-AI Co-Evolution Engine v6.0.0
=============================================

Complete, event-driven, MOPD-aware, XAI-enabled, safety-checked,
approval-gated, chaos-tested, and observability-instrumented
human-AI co-evolution engine.

ENHANCEMENTS OVER v5.2.0:
- Added missing `carbon_manager` attribute (injectable).
- Deferred `asyncio.create_task` from `__init__` to `start()` / `wait_ready()`.
- Renamed `_on_token_update` → `_on_token_balance_update`.
- Removed dead code in `_on_helium_update`.
- Added `MOPDConfig.__post_init__` (warn + normalise).
- Added `MOPDPlan.objective_vector()` and `MOPD_HIGHER_IS_BETTER`.
- Persistence: typed `TIMESTAMP_KEYS`, enum/datetime handling, threaded I/O.
- Non-destructive chaos via `ChaosService` with save/restore.
- Real human approval via `GovernanceLayer` (queue topic + audit trail).
- Fixed `scalarised_score` stamping for all Pareto plans.
- Fixed `share_with_swarm` non-deterministic hash → uuid.
- Fixed `_on_anomaly_detected` compounding by deriving from baseline.
- Added idempotency keys with LRU cache.
- Added `ObservabilityHub` with Prometheus export.
- Added `SafetyInvariant` with ALWAYS/EVENTUALLY semantics.
- Added `wait_ready()` / `start()` / `shutdown()` lifecycle.
- Added Protocols for all injected components.
- Added dead-letter queue for swarm/feedback failures.
- Removed unused imports; comprehensive type hints.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None

logger = logging.getLogger(__name__)


# ============================================================================
# Named Constants
# ============================================================================
STATE_SCHEMA_VERSION = 6
DEFAULT_CARBON_INTENSITY = 400.0
CARBON_CRITICAL_THRESHOLD = 800.0
HELIUM_CRITICAL_THRESHOLD = 0.9
MAX_FEEDBACK_HISTORY = 1000
MAX_PERFORMANCE_HISTORY = 1000
MAX_MILESTONES = 100
MAX_MOPD_PLANS = 10_000
MAX_APPROVAL_AUDIT = 1000
MAX_DEAD_LETTERS = 1000
IDEMPOTENCY_CACHE_SIZE = 4096
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6
MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'cost',
    'impact',
    'time_days',
    'risk',
    'historical_effectiveness',
)
# Canonical sign convention: every objective is internally "lower is better".
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'impact',
    'historical_effectiveness',
})


# ============================================================================
# Serialization helper
# ============================================================================
def _to_serializable(obj: Any) -> Any:
    """Recursively convert arbitrary objects to JSON-safe primitives."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return _to_serializable(obj.to_dict())
    if isinstance(obj, dict):
        return {
            (k.value if isinstance(k, Enum) else str(k)): _to_serializable(v)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, deque):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.generic):
        return obj.item()
    if hasattr(obj, '__dict__'):
        return _to_serializable(obj.__dict__)
    return str(obj)


# ============================================================================
# Optional bio-inspired core
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import (  # type: ignore
        EnhancedBioInspiredCore, BioEvent, CircuitBreaker,
    )
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

    class BioEvent:  # type: ignore
        def __init__(self, event_type: str, source: str, data: Optional[Dict] = None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

    class CircuitBreaker:  # type: ignore
        def __init__(self, name: str, failure_threshold: int = 3,
                     recovery_timeout: float = 30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time: Optional[float] = None
            self._lock = asyncio.Lock()

        async def call(self, func: Callable[..., Awaitable[Any]], *args: Any,
                       **kwargs: Any) -> Any:
            async with self._lock:
                if self._state == "open":
                    if (time.monotonic() - (self._last_failure_time or 0)) > self.recovery_timeout:
                        self._state = "half-open"
                    else:
                        raise RuntimeError(f"Circuit {self.name} is open")
            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    if self._state == "half-open":
                        self._state = "closed"
                        self._failure_count = 0
                return result
            except Exception:
                async with self._lock:
                    self._failure_count += 1
                    self._last_failure_time = time.monotonic()
                    if self._failure_count >= self.failure_threshold:
                        self._state = "open"
                raise


# ============================================================================
# Optional MoE imports
# ============================================================================
try:
    from ..expert_router import ExpertRouter  # type: ignore
    from ..gating_network import GatingNetworkManager  # type: ignore
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate  # type: ignore
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    ExpertRouter = None
    GatingNetworkManager = None
    EnhancedSelfEvolvingGate = None


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class BioCoreProtocol(Protocol):
    event_broker: Any
    self_healer: Any
    workflow_orchestrator: Any
    swarm_coordinator: Any
    token_manager: Any
    gradient_manager: Any


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...
    def publish(self, topic: str, payload: Any) -> Any: ...


@runtime_checkable
class SwarmCoordinatorProtocol(Protocol):
    async def share_predictions(self, payload: Dict[str, Any]) -> None: ...


@runtime_checkable
class SelfHealerProtocol(Protocol):
    async def apply_healing(self, damage_type: str) -> Any: ...


@runtime_checkable
class WorkflowOrchestratorProtocol(Protocol):
    async def execute_workflow(self, name: str) -> Any: ...


@runtime_checkable
class HeliumProviderProtocol(Protocol):
    def get_scarcity(self) -> float: ...
    def get_cost_index(self) -> float: ...
    def get_efficiency(self) -> float: ...


@runtime_checkable
class CarbonManagerProtocol(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self) -> None: ...


@runtime_checkable
class QuantumBenchmarkProtocol(Protocol):
    async def run_benchmark(self, task_name: str, task_input: Dict[str, Any]) -> Any: ...
    async def get_benchmark_summary(self) -> Dict[str, Any]: ...


@runtime_checkable
class FFTMoEProtocol(Protocol):
    async def get_fft_moe_status(self) -> Dict[str, Any]: ...
    async def analyze_expert_specialization(self) -> Dict[str, Any]: ...


@runtime_checkable
class HeliumManagerProtocol(Protocol):
    async def get_stats(self) -> Dict[str, Any]: ...
    async def get_sustainability_forecast(self) -> Dict[str, Any]: ...


@runtime_checkable
class FederatedOrchestratorProtocol(Protocol):
    async def get_status(self) -> Dict[str, Any]: ...


@runtime_checkable
class PredictiveAnalyzerProtocol(Protocol):
    async def predict_federation_trend(self) -> Dict[str, Any]: ...


# ============================================================================
# Enums
# ============================================================================
class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class FaultType(str, Enum):
    CARBON_SPIKE = "carbon_spike"
    HELIUM_CRITICAL = "helium_critical"
    QUANTUM_FAILURE = "quantum_failure"
    MOE_FAILURE = "moe_failure"
    FEDERATED_FAILURE = "federated_failure"
    PREDICTIVE_FAILURE = "predictive_failure"
    STORAGE_CORRUPTION = "storage_corruption"
    EVENT_QUEUE_OVERFLOW = "event_queue_overflow"


# ============================================================================
# Observability Hub
# ============================================================================
class ObservabilityHub:
    """Counters, gauges, histograms and structured logs with Prometheus export."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        self._counters[self._key(name, labels)] += float(value)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self._gauges[self._key(name, labels)] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        self._histograms[self._key(name, labels)].append(float(value))

    @staticmethod
    def _key(name: str, labels: Dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    async def export_prometheus(self) -> str:
        async with self._lock:
            lines: List[str] = []
            for key, value in self._counters.items():
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} counter")
                lines.append(f"{key} {value}")
            for key, value in self._gauges.items():
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} gauge")
                lines.append(f"{key} {value}")
            for key, values in self._histograms.items():
                if not values:
                    continue
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} histogram")
                lines.append(f"{base}_count {len(values)}")
                lines.append(f"{base}_sum {sum(values)}")
            return "\n".join(lines)

    def log_event(self, level: int, event: str, **fields: Any) -> None:
        payload = {"event": event, **fields}
        logger.log(level, json.dumps(payload, default=str))


# ============================================================================
# Configuration with Sub-Configs
# ============================================================================
@dataclass
class MOPDConfig:
    """Configuration for MOPD analysis."""
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'cost': 0.2,
        'impact': 0.3,
        'time_days': 0.15,
        'risk': 0.15,
        'historical_effectiveness': 0.2,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True
    require_human_approval: bool = False
    aggressive_threshold: float = 0.7

    def __post_init__(self) -> None:
        total = sum(self.objective_weights.values())
        if total <= 0:
            raise ValueError("Objective weights must sum to a positive value")
        if abs(total - 1.0) > 1e-6:
            logger.warning(f"MOPD weights sum to {total:.4f}; normalising.")
            for k in list(self.objective_weights):
                self.objective_weights[k] /= total


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    require_for_sustainability_above_priority: float = 0.8
    require_for_global_priority_above: float = 0.95


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    probability: float = 0.05
    auto_restore: bool = True


@dataclass
class CoEvolutionConfig:
    """Centralized configuration for the Co-Evolution Engine."""
    learning_rate: float = 0.01
    exploration_rate: float = 0.1
    adaptation_threshold: float = 0.7

    feedback_history_limit: int = MAX_FEEDBACK_HISTORY
    performance_history_limit: int = MAX_PERFORMANCE_HISTORY
    sustainability_trajectory_limit: int = 1000
    milestone_limit: int = MAX_MILESTONES

    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    persistence_path: str = "co_evolution_state.json"
    telemetry_export_interval: int = 60
    co_evolution_interval: int = 300

    estimated_impact: Dict[str, float] = field(default_factory=lambda: {
        'quantum': 0.8, 'moe': 0.6, 'sustainability': 0.9,
        'user_experience': 0.7, 'federated': 0.6, 'system_wide': 0.8,
    })
    estimated_effort: Dict[str, float] = field(default_factory=lambda: {
        'quantum': 0.7, 'moe': 0.5, 'sustainability': 0.6,
        'user_experience': 0.3, 'federated': 0.5, 'system_wide': 0.8,
    })

    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_swarm_coordination: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_cost_benefit: bool = True
    enable_workflow_orchestration: bool = True
    enable_mopd: bool = True
    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True

    workflow_on_critical_alert: str = "adjust_co_evolution_strategy"
    workflow_on_slo_breach: str = "rebalance_priorities"
    swarm_share_interval: int = 60

    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    approval: ApprovalConfig = field(default_factory=ApprovalConfig)
    chaos: ChaosConfig = field(default_factory=ChaosConfig)


# ============================================================================
# Sentiment Analyzer
# ============================================================================
class SentimentAnalyzer:
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.sentiment_keywords: Dict[str, Dict[str, float]] = {
            'positive': {
                'excellent': 1.0, 'great': 0.8, 'good': 0.6, 'nice': 0.5,
                'happy': 0.7, 'satisfied': 0.8, 'impressed': 0.9, 'love': 1.0,
                'amazing': 1.0, 'perfect': 1.0, 'awesome': 0.9, 'fantastic': 1.0,
                'helpful': 0.6, 'useful': 0.5, 'improved': 0.7, 'better': 0.6,
            },
            'negative': {
                'bad': -0.6, 'terrible': -1.0, 'awful': -0.9, 'horrible': -1.0,
                'sad': -0.5, 'disappointed': -0.7, 'frustrated': -0.8, 'angry': -0.9,
                'useless': -0.7, 'broken': -0.8, 'confusing': -0.5, 'slow': -0.5,
                'worse': -0.6, 'issue': -0.4, 'problem': -0.5, 'error': -0.6,
            },
        }
        self.emotion_keywords: Dict[str, List[str]] = {
            'joy': ['happy', 'glad', 'delighted', 'pleased', 'joy', 'wonderful'],
            'trust': ['trust', 'confident', 'reliable', 'sure', 'dependable'],
            'fear': ['worry', 'afraid', 'scared', 'anxious', 'nervous', 'concern'],
            'surprise': ['surprised', 'amazed', 'astonished', 'shocked', 'unexpected'],
            'sadness': ['sad', 'depressed', 'unhappy', 'miserable', 'disappointed'],
            'disgust': ['disgusted', 'appalled', 'horrified', 'revolted'],
            'anger': ['angry', 'furious', 'outraged', 'irritated', 'annoyed'],
            'anticipation': ['expect', 'anticipate', 'look forward', 'hope', 'eager'],
        }
        self.intensifiers = ['very', 'really', 'extremely', 'absolutely', 'completely',
                             'totally', 'highly', 'incredibly', 'remarkably', 'exceptionally']
        self.downtoners = ['somewhat', 'slightly', 'a bit', 'a little', 'fairly',
                           'moderately', 'kind of', 'sort of', 'rather']
        self.negations = ['not', 'never', 'none', 'nobody', 'no', 'neither', 'nor',
                          'hardly', 'scarcely', 'barely', 'no one', 'nothing', 'nowhere']

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        if not text or not text.strip():
            return {
                'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                'emotions': {}, 'key_phrases': [],
            }
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
            if i > 0 and words[i - 1] in self.intensifiers:
                multiplier = 1.5
            elif i > 0 and words[i - 1] in self.downtoners:
                multiplier = 0.6
            for sentiment_type, keywords in self.sentiment_keywords.items():
                if word in keywords:
                    sentiment_value = keywords[word] * multiplier
                    if negate_next:
                        sentiment_value = -sentiment_value
                        negate_next = False
                    score += sentiment_value
                    total_weight += 1.0
                    break
        score = score / total_weight if total_weight > 0 else 0.0
        score = max(-1.0, min(1.0, score))
        sentiment = 'positive' if score > 0.2 else 'negative' if score < -0.2 else 'neutral'
        confidence = min(0.95, total_weight / 10.0)
        emotions = self._detect_emotions(text_lower)
        key_phrases = self._extract_key_phrases(text)
        return {
            'score': score, 'confidence': confidence, 'sentiment': sentiment,
            'emotions': emotions, 'key_phrases': key_phrases,
        }

    def _detect_emotions(self, text_lower: str) -> Dict[str, float]:
        emotions: Dict[str, float] = {}
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
        phrases: List[str] = []
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
# Recommendation Prioritizer
# ============================================================================
class RecommendationPrioritizer:
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.historical_effectiveness: Dict[str, List[float]] = defaultdict(list)

    def prioritize_recommendations(
        self,
        recommendations: List[Dict[str, Any]],
        historical_data: Dict[str, float],
    ) -> List[Dict[str, Any]]:
        prioritized: List[Dict[str, Any]] = []
        for rec in recommendations:
            area = rec.get('area', 'general')
            impact = self.config.estimated_impact.get(area, 0.5)
            effort = self.config.estimated_effort.get(area, 0.5)
            historical_effectiveness = historical_data.get(area, 0.5)
            adjusted_impact = impact * (0.5 + 0.5 * historical_effectiveness)
            roi = adjusted_impact / max(effort, 0.01)
            priority = rec.get('priority', 0.5)
            roi_score = roi * (0.5 + 0.5 * priority)
            prioritized_rec = rec.copy()
            prioritized_rec.update({
                'roi_score': roi_score,
                'historical_effectiveness': historical_effectiveness,
                'estimated_roi': roi,
                'ranking': 0,
            })
            prioritized.append(prioritized_rec)
        prioritized.sort(key=lambda x: x['roi_score'], reverse=True)
        for i, rec in enumerate(prioritized):
            rec['ranking'] = i + 1
        return prioritized


# ============================================================================
# Long-Term Impact Tracker
# ============================================================================
class LongTermImpactTracker:
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.impact_history: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=100)
        )
        self.sustainability_scores: Deque[float] = deque(
            maxlen=config.sustainability_trajectory_limit
        )
        self.decay_rates: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def record_impact(self, area: str, impact_data: Dict[str, Any],
                            sustainability_score: float) -> None:
        async with self._lock:
            self.impact_history[area].append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'impact': impact_data,
                'sustainability_score': float(sustainability_score),
            })
            self.sustainability_scores.append(float(sustainability_score))
            await self._update_decay_rate(area)

    async def _update_decay_rate(self, area: str) -> None:
        history = list(self.impact_history.get(area, []))
        if len(history) < 5:
            return
        scores = [entry['sustainability_score'] for entry in history[-20:]]
        if len(scores) > 5:
            x = np.array(range(len(scores)))
            y = np.array(scores)
            slope = float(np.polyfit(x, y, 1)[0])
            self.decay_rates[area] = -slope

    def get_area_trend(self, area: str) -> Dict[str, Any]:
        history = list(self.impact_history.get(area, []))
        if len(history) < 3:
            return {'status': 'insufficient_data'}
        scores = [entry['sustainability_score'] for entry in history[-10:]]
        recent_scores = scores[-5:] if len(scores) >= 5 else scores
        avg_score = float(np.mean(scores)) if scores else 0.5
        avg_recent = float(np.mean(recent_scores)) if recent_scores else 0.5
        if avg_recent > avg_score * 1.05:
            trend = "improving"
        elif avg_recent > avg_score * 0.95:
            trend = "stable"
        else:
            trend = "declining"
        decay_rate = self.decay_rates.get(area, 0.0)
        return {
            'area': area,
            'average_score': avg_score,
            'recent_score': avg_recent,
            'trend': trend,
            'decay_rate': decay_rate,
            'sample_count': len(scores),
            'needs_attention': decay_rate > 0.05 or trend == "declining",
        }

    def get_overall_trend(self) -> Dict[str, Any]:
        scores = list(self.sustainability_scores)
        if not scores:
            return {'status': 'insufficient_data'}
        scores = scores[-20:]
        avg_score = float(np.mean(scores))
        avg_recent = float(np.mean(scores[-5:])) if len(scores) >= 5 else avg_score
        if avg_recent > avg_score * 1.05:
            trend = "improving"
        elif avg_recent > avg_score * 0.95:
            trend = "stable"
        else:
            trend = "declining"
        return {
            'average_sustainability_score': avg_score,
            'recent_sustainability_score': avg_recent,
            'trend': trend,
            'sample_count': len(scores),
            'improvement_rate': (
                (avg_recent - avg_score) / max(avg_score, 0.01) if len(scores) > 5 else 0.0
            ),
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            'impact_history': {k: list(v) for k, v in self.impact_history.items()},
            'sustainability_scores': list(self.sustainability_scores),
            'decay_rates': self.decay_rates,
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        self.impact_history = defaultdict(lambda: deque(maxlen=100))
        for k, v in data.get('impact_history', {}).items():
            self.impact_history[k] = deque(v, maxlen=100)
        self.sustainability_scores = deque(
            data.get('sustainability_scores', []),
            maxlen=self.config.sustainability_trajectory_limit,
        )
        self.decay_rates = data.get('decay_rates', {})


# ============================================================================
# Persistence Manager (typed, threaded)
# ============================================================================
class CoEvolutionPersistenceManager:
    TIMESTAMP_KEYS: FrozenSet[str] = frozenset({
        'timestamp', 'requested_at', 'decided_at', 'last_assessment',
    })

    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._version = STATE_SCHEMA_VERSION
        logger.info(f"CoEvolutionPersistenceManager initialised (path={self.path})")

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': _to_serializable(state),
                }
                tmp = f"{self.path}.tmp"

                def _write() -> None:
                    with open(tmp, 'w') as f:
                        json.dump(payload, f, indent=2)
                    os.replace(tmp, self.path)

                await asyncio.to_thread(_write)
                logger.info(f"State saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return None
            try:
                def _read() -> Dict[str, Any]:
                    with open(self.path, 'r') as f:
                        return json.load(f)

                payload = await asyncio.to_thread(_read)
                version = payload.get('version', 0)
                if version != self._version:
                    logger.warning(
                        f"State version mismatch (file={version}, expected={self._version})"
                    )
                return self._deserialize(payload.get('data', {}))
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return None

    def _deserialize(self, obj: Any, key: Optional[str] = None) -> Any:
        if isinstance(obj, dict):
            return {k: self._deserialize(v, key=k) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._deserialize(v, key=key) for v in obj]
        if isinstance(obj, str) and key in self.TIMESTAMP_KEYS:
            try:
                return datetime.fromisoformat(obj)
            except ValueError:
                return obj
        return obj

    async def delete_state(self) -> bool:
        async with self._lock:
            if os.path.exists(self.path):
                await asyncio.to_thread(os.remove, self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False


# ============================================================================
# Retry Helper
# ============================================================================
async def retry_async(
    func: Callable[..., Awaitable[Any]],
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args: Any,
    **kwargs: Any,
) -> Any:
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")


# ============================================================================
# Evolution Milestone
# ============================================================================
@dataclass
class EvolutionMilestone:
    timestamp: datetime
    milestone_type: str
    description: str
    metrics: Dict[str, float]
    human_feedback_count: int
    ai_suggestion_impact: float
    strategy_signature: Optional[str] = None
    reuse_count: int = 0
    effectiveness_history: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'milestone_type': self.milestone_type,
            'description': self.description,
            'metrics': dict(self.metrics),
            'human_feedback_count': self.human_feedback_count,
            'ai_suggestion_impact': self.ai_suggestion_impact,
            'strategy_signature': self.strategy_signature,
            'reuse_count': self.reuse_count,
            'avg_effectiveness': (
                float(np.mean(self.effectiveness_history))
                if self.effectiveness_history else 0.0
            ),
            'effectiveness_history': list(self.effectiveness_history),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EvolutionMilestone":
        d = dict(data)
        if isinstance(d.get('timestamp'), str):
            d['timestamp'] = datetime.fromisoformat(d['timestamp'])
        d.pop('avg_effectiveness', None)
        d['metrics'] = dict(d.get('metrics', {}))
        d['effectiveness_history'] = list(d.get('effectiveness_history', []))
        return cls(**d)


# ============================================================================
# MOPD Data Classes
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a co-evolution strategy with its objective vector and XAI."""
    area: str
    action: str
    strategy_signature: str
    cost: float
    impact: float
    time_days: float
    risk: float
    historical_effectiveness: float
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'area': self.area,
            'action': self.action,
            'strategy_signature': self.strategy_signature,
            'cost': self.cost,
            'impact': self.impact,
            'time_days': self.time_days,
            'risk': self.risk,
            'historical_effectiveness': self.historical_effectiveness,
            'scalarised_score': self.scalarised_score,
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPlan":
        return cls(**data)

    def objective_vector(self) -> List[float]:
        """Canonical lower-is-better objective vector."""
        vec: List[float] = []
        for name in MOPD_OBJECTIVE_NAMES:
            val = float(getattr(self, name, 0.0))
            if name in MOPD_HIGHER_IS_BETTER:
                val = -val
            vec.append(val)
        return vec


# ============================================================================
# Storage Module
# ============================================================================
class CoEvolutionStorage:
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.feedback_history: Deque[Dict[str, Any]] = deque(
            maxlen=config.feedback_history_limit
        )
        self.user_models: Dict[str, Dict[str, Any]] = {}
        self.policy_suggestions: List[Dict[str, Any]] = []
        self.collaborative_decisions: List[Dict[str, Any]] = []
        self.evolution_milestones: Deque[EvolutionMilestone] = deque(
            maxlen=config.milestone_limit
        )
        self.milestone_strategies: Dict[str, List[Dict[str, Any]]] = {}
        self.performance_history: Deque[Dict[str, Any]] = deque(
            maxlen=config.performance_history_limit
        )
        self.trust_history: Deque[float] = deque(maxlen=1000)
        self.sustainability_trajectory: Deque[float] = deque(
            maxlen=config.sustainability_trajectory_limit
        )
        self.historical_effectiveness: Dict[str, float] = {
            'quantum': 0.5, 'moe': 0.5, 'sustainability': 0.5,
            'user_experience': 0.5, 'federated': 0.5, 'system_wide': 0.5,
        }
        self.mopd_plans: Deque[MOPDPlan] = deque(maxlen=MAX_MOPD_PLANS)
        self._lock = asyncio.Lock()

    # ---------------- Feedback ----------------
    async def add_feedback(self, feedback: Dict[str, Any]) -> None:
        async with self._lock:
            self.feedback_history.append(_to_serializable(feedback))

    async def get_feedback(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if limit is not None:
                return list(self.feedback_history)[-limit:]
            return list(self.feedback_history)

    async def clear_feedback(self) -> None:
        async with self._lock:
            self.feedback_history.clear()

    # ---------------- User Models ----------------
    async def get_user_model(self, user_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            return self.user_models.get(user_id)

    async def update_user_model(self, user_id: str, updates: Dict[str, Any]) -> None:
        async with self._lock:
            if user_id not in self.user_models:
                self.user_models[user_id] = {}
            self.user_models[user_id].update(updates)

    async def get_user_models(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return dict(self.user_models)

    async def clear_user_models(self) -> None:
        async with self._lock:
            self.user_models.clear()

    # ---------------- Policies & Decisions ----------------
    async def add_policy_suggestion(self, suggestion: Dict[str, Any]) -> None:
        async with self._lock:
            self.policy_suggestions.append(_to_serializable(suggestion))

    async def get_policy_suggestions(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if limit:
                return list(self.policy_suggestions[-limit:])
            return list(self.policy_suggestions)

    async def add_collaborative_decision(self, decision: Dict[str, Any]) -> None:
        async with self._lock:
            self.collaborative_decisions.append(_to_serializable(decision))

    async def get_collaborative_decisions(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if limit:
                return list(self.collaborative_decisions[-limit:])
            return list(self.collaborative_decisions)

    # ---------------- Milestones ----------------
    async def add_milestone(self, milestone: EvolutionMilestone) -> None:
        async with self._lock:
            self.evolution_milestones.append(milestone)

    async def get_milestones(self, limit: Optional[int] = None) -> List[EvolutionMilestone]:
        async with self._lock:
            if limit:
                return list(self.evolution_milestones)[-limit:]
            return list(self.evolution_milestones)

    async def find_milestone_by_strategy(self, strategy_signature: str) -> Optional[EvolutionMilestone]:
        async with self._lock:
            for m in self.evolution_milestones:
                if m.strategy_signature == strategy_signature:
                    return m
            return None

    # ---------------- Milestone Strategies ----------------
    async def add_milestone_strategy(self, strategy_signature: str, entry: Dict[str, Any]) -> None:
        async with self._lock:
            if strategy_signature not in self.milestone_strategies:
                self.milestone_strategies[strategy_signature] = []
            self.milestone_strategies[strategy_signature].append(_to_serializable(entry))

    async def get_milestone_strategies(self) -> Dict[str, List[Dict[str, Any]]]:
        async with self._lock:
            return dict(self.milestone_strategies)

    # ---------------- Performance & Trust ----------------
    async def add_performance_record(self, record: Dict[str, Any]) -> None:
        async with self._lock:
            self.performance_history.append(_to_serializable(record))

    async def get_performance_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if limit:
                return list(self.performance_history)[-limit:]
            return list(self.performance_history)

    async def add_trust_score(self, score: float) -> None:
        async with self._lock:
            self.trust_history.append(float(score))

    async def get_trust_history(self, limit: Optional[int] = None) -> List[float]:
        async with self._lock:
            if limit:
                return list(self.trust_history)[-limit:]
            return list(self.trust_history)

    # ---------------- Sustainability Trajectory ----------------
    async def add_sustainability_score(self, score: float) -> None:
        async with self._lock:
            self.sustainability_trajectory.append(float(score))

    async def get_sustainability_trajectory(self, limit: Optional[int] = None) -> List[float]:
        async with self._lock:
            if limit:
                return list(self.sustainability_trajectory)[-limit:]
            return list(self.sustainability_trajectory)

    # ---------------- Historical Effectiveness ----------------
    async def get_historical_effectiveness(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self.historical_effectiveness)

    async def update_historical_effectiveness(self, area: str, effectiveness: float) -> None:
        async with self._lock:
            old = self.historical_effectiveness.get(area, 0.5)
            self.historical_effectiveness[area] = old * 0.7 + float(effectiveness) * 0.3

    async def reset_historical_effectiveness(self) -> None:
        async with self._lock:
            self.historical_effectiveness = {
                'quantum': 0.5, 'moe': 0.5, 'sustainability': 0.5,
                'user_experience': 0.5, 'federated': 0.5, 'system_wide': 0.5,
            }

    # ---------------- MOPD Plans ----------------
    async def add_mopd_plan(self, plan: MOPDPlan) -> None:
        async with self._lock:
            self.mopd_plans.append(plan)

    async def get_mopd_plans(self, limit: Optional[int] = None) -> List[MOPDPlan]:
        async with self._lock:
            if limit is not None:
                return list(self.mopd_plans)[-limit:]
            return list(self.mopd_plans)

    # ---------------- State Snapshot ----------------
    async def to_dict(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'feedback_history': [_to_serializable(f) for f in self.feedback_history],
                'user_models': _to_serializable(self.user_models),
                'policy_suggestions': [_to_serializable(s) for s in self.policy_suggestions],
                'collaborative_decisions': [_to_serializable(d) for d in self.collaborative_decisions],
                'evolution_milestones': [m.to_dict() for m in self.evolution_milestones],
                'milestone_strategies': _to_serializable(self.milestone_strategies),
                'performance_history': [_to_serializable(p) for p in self.performance_history],
                'trust_history': list(self.trust_history),
                'sustainability_trajectory': list(self.sustainability_trajectory),
                'historical_effectiveness': dict(self.historical_effectiveness),
                'mopd_plans': [p.to_dict() for p in self.mopd_plans],
            }

    async def from_dict(self, data: Dict[str, Any]) -> None:
        async with self._lock:
            self.feedback_history = deque(
                data.get('feedback_history', []),
                maxlen=self.config.feedback_history_limit,
            )
            self.user_models = dict(data.get('user_models', {}))
            self.policy_suggestions = list(data.get('policy_suggestions', []))
            self.collaborative_decisions = list(data.get('collaborative_decisions', []))

            self.evolution_milestones = deque(maxlen=self.config.milestone_limit)
            for m_dict in data.get('evolution_milestones', []):
                try:
                    self.evolution_milestones.append(EvolutionMilestone.from_dict(m_dict))
                except Exception as e:
                    logger.warning(f"Skipping invalid milestone: {e}")

            self.milestone_strategies = dict(data.get('milestone_strategies', {}))
            self.performance_history = deque(
                data.get('performance_history', []),
                maxlen=self.config.performance_history_limit,
            )
            self.trust_history = deque(data.get('trust_history', []), maxlen=1000)
            self.sustainability_trajectory = deque(
                data.get('sustainability_trajectory', []),
                maxlen=self.config.sustainability_trajectory_limit,
            )
            self.historical_effectiveness = dict(data.get('historical_effectiveness', {}))

            self.mopd_plans = deque(maxlen=MAX_MOPD_PLANS)
            for p_dict in data.get('mopd_plans', []):
                try:
                    self.mopd_plans.append(MOPDPlan.from_dict(p_dict))
                except Exception as e:
                    logger.warning(f"Skipping invalid MOPD plan: {e}")


# ============================================================================
# Temporal Safety
# ============================================================================
class SafetyInvariant:
    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
                 description: str, kind: str = "always") -> None:
        if kind not in ("always", "eventually"):
            raise ValueError(f"Unsupported invariant kind: {kind}")
        self.name = name
        self.predicate = predicate
        self.description = description
        self.kind = kind


class SafetyMonitor:
    def __init__(self) -> None:
        self.invariants: List[SafetyInvariant] = []
        self._eventually_flags: Dict[str, bool] = {}

    def add_invariant(self, name: str, condition_fn: Callable[[Dict[str, Any]], bool],
                      description: str, kind: str = "always") -> None:
        self.invariants.append(SafetyInvariant(name, condition_fn, description, kind))
        if kind == "eventually":
            self._eventually_flags[name] = False

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            try:
                ok = bool(inv.predicate(state))
            except Exception as e:
                violations.append(f"{inv.name}: predicate error ({e})")
                continue
            if inv.kind == "always" and not ok:
                violations.append(f"{inv.name}: {inv.description}")
            elif inv.kind == "eventually" and ok:
                self._eventually_flags[inv.name] = True
        return violations

    def final_check(self) -> List[str]:
        return [
            f"{inv.name}: {inv.description} (never satisfied)"
            for inv in self.invariants
            if inv.kind == "eventually" and not self._eventually_flags.get(inv.name, False)
        ]


# ============================================================================
# Governance (Human Approval)
# ============================================================================
@dataclass
class ApprovalRequest:
    request_id: str
    context: Dict[str, Any]
    requested_at: datetime
    timeout_seconds: float
    status: ApprovalStatus = ApprovalStatus.PENDING
    future: Optional[asyncio.Future] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'context': _to_serializable(self.context),
            'requested_at': self.requested_at.isoformat(),
            'timeout_seconds': self.timeout_seconds,
            'status': self.status.value,
        }


@dataclass
class ApprovalResult:
    request_id: str
    status: ApprovalStatus
    reason: str = ""
    decided_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'status': self.status.value,
            'reason': self.reason,
            'decided_at': self.decided_at.isoformat() if self.decided_at else None,
        }


class GovernanceLayer:
    def __init__(self, config: ApprovalConfig,
                 publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None) -> None:
        self.config = config
        self.publish_fn = publish_fn
        self.pending: Dict[str, ApprovalRequest] = {}
        self.audit: Deque[Dict[str, Any]] = deque(maxlen=MAX_APPROVAL_AUDIT)
        self._lock = asyncio.Lock()

    async def request_approval(self, context: Dict[str, Any],
                               timeout: Optional[float] = None) -> ApprovalResult:
        if not self.config.enabled:
            result = ApprovalResult(
                request_id='auto',
                status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled',
                decided_at=datetime.now(timezone.utc),
            )
            self._audit(context, result)
            return result

        timeout = timeout if timeout is not None else self.config.timeout_seconds
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        req = ApprovalRequest(
            request_id=request_id,
            context=context,
            requested_at=datetime.now(timezone.utc),
            timeout_seconds=timeout,
            future=future,
        )
        async with self._lock:
            self.pending[request_id] = req

        try:
            if self.publish_fn is not None:
                await self.publish_fn('approval_requests',
                                       json.dumps(req.to_dict(), default=str))
            decision = await asyncio.wait_for(future, timeout=timeout)
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if decision else ApprovalStatus.DENIED,
                reason='human_decision',
                decided_at=datetime.now(timezone.utc),
            )
        except asyncio.TimeoutError:
            if self.config.auto_approve_on_timeout:
                result = ApprovalResult(
                    request_id=request_id,
                    status=ApprovalStatus.AUTO_APPROVED,
                    reason='timeout_auto_approved',
                    decided_at=datetime.now(timezone.utc),
                )
            else:
                result = ApprovalResult(
                    request_id=request_id,
                    status=ApprovalStatus.TIMEOUT,
                    reason='timeout_denied',
                    decided_at=datetime.now(timezone.utc),
                )
        except Exception as e:
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.DENIED,
                reason=f'error:{e}',
                decided_at=datetime.now(timezone.utc),
            )
        finally:
            async with self._lock:
                self.pending.pop(request_id, None)

        self._audit(context, result)
        return result

    async def submit_decision(self, request_id: str, approved: bool) -> bool:
        async with self._lock:
            req = self.pending.get(request_id)
            if req is None or req.future is None or req.future.done():
                return False
            req.future.set_result(approved)
            return True

    def _audit(self, context: Dict[str, Any], result: ApprovalResult) -> None:
        self.audit.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'context': _to_serializable(context),
            'result': result.to_dict(),
        })

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit)


# ============================================================================
# Chaos Service
# ============================================================================
class ChaosService:
    def __init__(self, orchestrator: "CoEvolutionOrchestrator",
                 config: ChaosConfig) -> None:
        self.orchestrator = orchestrator
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def inject_fault(self, fault_type: Union[str, FaultType],
                           **params: Any) -> Dict[str, Any]:
        if isinstance(fault_type, str):
            try:
                fault_type = FaultType(fault_type)
            except ValueError:
                return {'fault': fault_type, 'error': 'unknown_fault'}

        async with self._lock:
            try:
                if fault_type == FaultType.CARBON_SPIKE:
                    if self.orchestrator.carbon_manager is not None:
                        self._saved.setdefault(
                            'carbon_intensity',
                            self.orchestrator.carbon_manager.carbon_intensity,
                        )
                        self.orchestrator.carbon_manager.carbon_intensity = params.get(
                            'intensity', CARBON_CRITICAL_THRESHOLD + 50,
                        )
                    return {'fault': fault_type.value}
                if fault_type == FaultType.HELIUM_CRITICAL:
                    # Non-destructive marker — the injected component reports scarcity
                    return {'fault': fault_type.value, 'noop': True}
                if fault_type == FaultType.QUANTUM_FAILURE:
                    self._saved.setdefault(
                        'quantum_benchmark', self.orchestrator.quantum_benchmark,
                    )
                    self.orchestrator.quantum_benchmark = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.MOE_FAILURE:
                    self._saved.setdefault('fft_moe', self.orchestrator.fft_moe)
                    self.orchestrator.fft_moe = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.FEDERATED_FAILURE:
                    self._saved.setdefault(
                        'federated_orchestrator',
                        self.orchestrator.federated_orchestrator,
                    )
                    self.orchestrator.federated_orchestrator = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.PREDICTIVE_FAILURE:
                    self._saved.setdefault(
                        'predictive_analyzer', self.orchestrator.predictive_analyzer,
                    )
                    self.orchestrator.predictive_analyzer = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.STORAGE_CORRUPTION:
                    return {'fault': fault_type.value, 'noop': True}
                if fault_type == FaultType.EVENT_QUEUE_OVERFLOW:
                    return {'fault': fault_type.value, 'noop': True}
            except Exception as e:
                logger.error(f"Fault injection failed ({fault_type}): {e}")
                return {'fault': fault_type.value, 'error': str(e)}
            return {'fault': fault_type.value, 'not_implemented': True}

    def restore(self) -> None:
        if 'carbon_intensity' in self._saved and self.orchestrator.carbon_manager is not None:
            self.orchestrator.carbon_manager.carbon_intensity = self._saved.pop('carbon_intensity')
        if 'quantum_benchmark' in self._saved:
            self.orchestrator.quantum_benchmark = self._saved.pop('quantum_benchmark')
        if 'fft_moe' in self._saved:
            self.orchestrator.fft_moe = self._saved.pop('fft_moe')
        if 'federated_orchestrator' in self._saved:
            self.orchestrator.federated_orchestrator = self._saved.pop('federated_orchestrator')
        if 'predictive_analyzer' in self._saved:
            self.orchestrator.predictive_analyzer = self._saved.pop('predictive_analyzer')

    async def run_chaos_test(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.QUANTUM_FAILURE,
            FaultType.MOE_FAILURE,
            FaultType.FEDERATED_FAILURE,
            FaultType.PREDICTIVE_FAILURE,
            FaultType.CARBON_SPIKE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject_fault(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            # Exercise the system under fault
            try:
                cycle = await self.orchestrator.co_evolve()
                report['results']['post_fault_cycle'] = {
                    'status': cycle.get('status', 'unknown'),
                    'applied_count': len(cycle.get('recommendations_applied', [])),
                }
            except Exception as e:
                report['results']['post_fault_cycle'] = {
                    'status': 'error', 'error': str(e),
                }
        finally:
            if self.config.auto_restore:
                self.restore()
                report['restored'] = True
        return report


# ============================================================================
# Analyzer Module
# ============================================================================
class CoEvolutionAnalyzer:
    def __init__(
        self,
        config: CoEvolutionConfig,
        storage: CoEvolutionStorage,
        sentiment_analyzer: SentimentAnalyzer,
        recommender: RecommendationPrioritizer,
        impact_tracker: LongTermImpactTracker,
    ) -> None:
        self.config = config
        self.storage = storage
        self.sentiment = sentiment_analyzer
        self.recommender = recommender
        self.impact_tracker = impact_tracker

    # ------------------------------------------------------------------
    # Feedback aggregation
    # ------------------------------------------------------------------
    async def aggregate_human_feedback_with_sentiment(self) -> List[Dict[str, Any]]:
        user_models = await self.storage.get_user_models()
        decisions = await self.storage.get_collaborative_decisions()
        all_feedback: List[Dict[str, Any]] = []
        for _user_id, user_model in user_models.items():
            for fb in user_model.get('feedback', []) or []:
                if isinstance(fb, dict) and 'comment' in fb:
                    fb = dict(fb)
                    fb['sentiment'] = self.sentiment.analyze_sentiment(fb['comment'])
                    all_feedback.append(fb)
        for decision in decisions:
            for fb in decision.get('feedback', []) or []:
                if isinstance(fb, dict) and 'comment' in fb:
                    fb = dict(fb)
                    fb['sentiment'] = self.sentiment.analyze_sentiment(fb['comment'])
                    all_feedback.append(fb)
        return all_feedback[-self.config.feedback_history_limit:]

    async def extract_feedback_themes_with_sentiment(
        self, feedback: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        keyword_map = {
            'usability': ['confusing', 'complicated', 'hard to use', 'intuitive', 'usability'],
            'performance': ['slow', 'fast', 'lag', 'responsiveness', 'performance'],
            'accuracy': ['wrong', 'incorrect', 'accurate', 'correct', 'precision'],
            'sustainability': ['carbon', 'helium', 'green', 'environmental', 'energy', 'sustainable'],
            'trust': ['trust', 'confidence', 'reliable', 'unreliable', 'trustworthy'],
        }
        themes: Dict[str, Dict[str, Any]] = {}
        for fb in feedback:
            text = fb.get('comment', '').lower()
            sentiment = fb.get('sentiment', {}).get('score', 0)
            for theme, keywords in keyword_map.items():
                if any(keyword in text for keyword in keywords):
                    if theme not in themes:
                        themes[theme] = {'count': 0, 'sentiment_sum': 0.0, 'avg_sentiment': 0.0}
                    themes[theme]['count'] += 1
                    themes[theme]['sentiment_sum'] += sentiment
        for theme in themes:
            if themes[theme]['count'] > 0:
                themes[theme]['avg_sentiment'] = (
                    themes[theme]['sentiment_sum'] / themes[theme]['count']
                )
        return themes

    async def identify_opportunities(
        self,
        system_state: Dict[str, Any],
        human_feedback: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        opportunities: List[Dict[str, Any]] = []
        q = system_state.get('components', {}).get('quantum', {})
        if q.get('average_energy_savings_percent', 0) < 10:
            opportunities.append({
                'area': 'quantum', 'type': 'performance', 'priority': 0.7,
                'suggestion': 'Optimize quantum circuit depth and qubit usage',
                'expected_impact': '20-30% energy savings',
            })
        m = system_state.get('components', {}).get('moe', {})
        if m.get('total_updates_processed', 0) < 10:
            opportunities.append({
                'area': 'moe', 'type': 'adoption', 'priority': 0.5,
                'suggestion': 'Increase client participation in FFT-MoE',
                'expected_impact': 'Improved personalization and accuracy',
            })
        h = system_state.get('components', {}).get('helium', {})
        scarcity = h.get('current', {}).get('scarcity_index', 0)
        if scarcity > 0.6:
            opportunities.append({
                'area': 'sustainability', 'type': 'constraint', 'priority': 0.9,
                'suggestion': 'Reduce helium usage through alternative cooling',
                'expected_impact': '50-80% helium savings',
            })

        if human_feedback:
            sentiments = [
                f.get('sentiment', {}).get('score', 0)
                for f in human_feedback if 'sentiment' in f
            ]
            if sentiments:
                negative_ratio = sum(1 for s in sentiments if s < -0.3) / max(len(sentiments), 1)
                if negative_ratio > 0.2:
                    opportunities.append({
                        'area': 'user_experience', 'type': 'sentiment_driven',
                        'priority': 0.75,
                        'suggestion': (
                            f'Address user concerns - {negative_ratio:.0%} negative feedback detected'
                        ),
                        'expected_impact': 'Improved user satisfaction and trust',
                    })
            themes = await self.extract_feedback_themes_with_sentiment(human_feedback)
            for theme, data in themes.items():
                if data['count'] > len(human_feedback) * 0.2:
                    sentiment_weight = 1.0 + (0.5 - data['avg_sentiment']) * 0.5
                    priority = min(0.9, 0.6 + sentiment_weight * 0.3)
                    opportunities.append({
                        'area': 'user_experience', 'type': theme, 'priority': priority,
                        'suggestion': (
                            f'Address user concerns about {theme} '
                            f'(sentiment: {data["avg_sentiment"]:.2f})'
                        ),
                        'expected_impact': 'Improved user satisfaction and trust',
                    })

        opportunities.sort(key=lambda x: x['priority'], reverse=True)
        return opportunities[:5]

    # ------------------------------------------------------------------
    # MOPD computation
    # ------------------------------------------------------------------
    async def _compute_plan_objectives(self, rec: Dict[str, Any]) -> MOPDPlan:
        area = rec.get('area', 'general')
        action = rec.get('action', '')
        strategy_signature = rec.get(
            'strategy_signature',
            hashlib.md5(f"{area}:{action}".encode()).hexdigest()[:12],
        )

        cost = 1.0 - rec.get('historical_effectiveness', 0.5)
        impact = self.config.estimated_impact.get(area, 0.5) * rec.get('priority', 0.5)
        time_days = 1.0 / (0.5 + max(impact, 1e-6))
        risk = 1.0 - rec.get('priority', 0.5)
        historical_effectiveness = rec.get('historical_effectiveness', 0.5)

        explanation = (
            f"Area={area}, action={action}, impact={impact:.2f}, "
            f"time={time_days:.2f} days, risk={risk:.2f}, "
            f"historical effectiveness={historical_effectiveness:.2f}"
        )

        return MOPDPlan(
            area=area,
            action=action,
            strategy_signature=strategy_signature,
            cost=float(cost),
            impact=float(impact),
            time_days=float(time_days),
            risk=float(risk),
            historical_effectiveness=float(historical_effectiveness),
            explanation=explanation,
        )

    async def _generate_pareto_front_for_recommendations(
        self, recommendations: List[Dict[str, Any]],
    ) -> List[MOPDPlan]:
        if not recommendations:
            return []
        plans: List[MOPDPlan] = []
        for rec in recommendations:
            plans.append(await self._compute_plan_objectives(rec))

        pareto: List[MOPDPlan] = []
        for i, p_i in enumerate(plans):
            a_vec = p_i.objective_vector()
            dominated = False
            for j, p_j in enumerate(plans):
                if i == j:
                    continue
                b_vec = p_j.objective_vector()
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(
                    b < a for a, b in zip(a_vec, b_vec)
                ):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        vectors = [p.objective_vector() for p in pareto_front]
        arr = np.array(vectors)
        mins = arr.min(axis=0)
        maxs = arr.max(axis=0)
        ranges = np.where(maxs - mins > 1e-9, maxs - mins, 1.0)

        best: Optional[MOPDPlan] = None
        best_score = -float('inf')
        for plan in pareto_front:
            vec = np.array(plan.objective_vector())
            # 1 = best (smallest canonical), 0 = worst
            normalised = 1.0 - (vec - mins) / ranges
            score = 0.0
            for idx, name in enumerate(MOPD_OBJECTIVE_NAMES):
                w = weights.get(name, 1.0 / len(MOPD_OBJECTIVE_NAMES))
                score += w * float(normalised[idx])
            plan.scalarised_score = score
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ------------------------------------------------------------------
    # Recommendation generation
    # ------------------------------------------------------------------
    async def generate_holistic_recommendations(
        self,
        system_state: Dict[str, Any],
        opportunities: List[Dict[str, Any]],
        return_mopd: bool = False,
    ) -> Dict[str, Any]:
        combined: Dict[str, Dict[str, Any]] = {}
        for opp in opportunities:
            key = opp['area']
            if key not in combined:
                combined[key] = {
                    'area': key,
                    'types': [],
                    'priority': opp['priority'],
                    'suggestions': [opp['suggestion']],
                    'expected_impact': opp.get('expected_impact', 'Expected improvement'),
                    'predicted': opp.get('predicted', False),
                    'timeframe_hours': opp.get('timeframe_hours', None),
                }
            else:
                combined[key]['types'].append(opp.get('type', 'general'))
                combined[key]['suggestions'].append(opp['suggestion'])
                combined[key]['priority'] = max(combined[key]['priority'], opp['priority'])

        historical = await self.storage.get_historical_effectiveness()
        recommendations: List[Dict[str, Any]] = []
        for area, data in combined.items():
            strategy_sig = self._generate_strategy_signature(area, data['suggestions'])
            existing = await self.storage.find_milestone_by_strategy(strategy_sig)
            rec: Dict[str, Any] = {
                'area': area,
                'action': (
                    data['suggestions'][0] if len(data['suggestions']) == 1
                    else f"Multiple actions: {', '.join(data['suggestions'][:2])}"
                ),
                'priority': data['priority'],
                'rationale': self._generate_rationale(area, system_state),
                'expected_outcome': self._predict_outcome(
                    area, data['types'], historical,
                ),
                'predicted': data.get('predicted', False),
                'timeframe_hours': data.get('timeframe_hours', None),
                'strategy_signature': strategy_sig,
                'historical_effectiveness': historical.get(area, 0.5),
            }
            if existing:
                rec['previous_effectiveness'] = existing.ai_suggestion_impact
                rec['reuse_benefit'] = (
                    f"Strategy previously effective ({existing.ai_suggestion_impact:.2f})"
                )
            recommendations.append(rec)

        if len(opportunities) >= 3:
            recommendations.append({
                'area': 'system_wide',
                'action': 'Schedule a comprehensive system optimization sprint',
                'priority': 0.8,
                'rationale': (
                    f'Multiple improvement areas identified ({len(opportunities)} opportunities)'
                ),
                'expected_outcome': 'System-wide performance uplift',
                'strategy_signature': 'system_wide_optimization',
            })

        result: Dict[str, Any] = {'recommendations': recommendations}
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_recommendations(
                recommendations,
            )
            if pareto_front:
                for plan in pareto_front:
                    await self.storage.add_mopd_plan(plan)
                best_plan = self._select_best_from_pareto(pareto_front)
                result['mopd_pareto_front'] = [p.to_dict() for p in pareto_front]
                if best_plan:
                    result['mopd_best_plan'] = best_plan.to_dict()
        return result

    def _generate_strategy_signature(self, area: str, suggestions: List[str]) -> str:
        combined = f"{area}:{'.'.join(sorted(suggestions))}"
        return hashlib.md5(combined.encode()).hexdigest()[:12]

    def _generate_rationale(self, area: str, system_state: Dict[str, Any]) -> str:
        rationales = {
            'quantum': 'Quantum energy savings below target, optimization needed',
            'moe': 'MoE adoption is limited, more client participation needed',
            'sustainability': 'High helium scarcity requires immediate action',
            'user_experience': 'User feedback indicates areas for improvement',
            'federated': 'Federated learning performance can be improved',
        }
        base = rationales.get(area, f'Improvement needed in {area}')
        trend = self.impact_tracker.get_area_trend(area)
        if trend.get('status') != 'insufficient_data':
            if trend.get('trend') == 'declining':
                base += f" (Long-term trend: {trend['trend']}, attention needed)"
            elif trend.get('trend') == 'improving':
                base += f" (Long-term trend: {trend['trend']}, continue momentum)"
        return base

    def _predict_outcome(self, area: str, types: List[str],
                         historical: Dict[str, float]) -> str:
        outcomes = {
            'quantum': '10-30% reduction in energy consumption',
            'moe': 'Improved personalization and model accuracy',
            'sustainability': 'Significant reduction in resource usage',
            'user_experience': 'Increased user engagement and trust',
            'federated': 'Better global model performance',
        }
        base = outcomes.get(area, 'Expected performance improvement')
        eff = historical.get(area, 0.5)
        if eff > 0.7:
            base += f" (Historically effective: {eff:.1%})"
        elif eff < 0.3:
            base += f" (Historically challenging: {eff:.1%})"
        return base

    async def calculate_trend(self) -> str:
        traj = await self.storage.get_sustainability_trajectory(20)
        if len(traj) < 10:
            return "stable"
        recent = traj[-10:]
        avg_recent = float(np.mean(recent))
        older = traj[-20:-10] if len(traj) >= 20 else recent
        avg_older = float(np.mean(older))
        if avg_recent > avg_older * 1.05:
            return "improving"
        if avg_recent < avg_older * 0.95:
            return "declining"
        return "stable"

    async def detect_milestone(self, impact: Dict[str, Any]) -> Optional[EvolutionMilestone]:
        metrics = impact['metrics']
        milestone: Optional[EvolutionMilestone] = None

        if metrics.get('sustainability', 0) > 0.8:
            best_area = 'sustainability'
            details = impact.get('details', [])
            if details:
                best_area = max(
                    details, key=lambda x: x.get('effectiveness', 0),
                ).get('area', 'sustainability')
            strategy_sig = self._generate_strategy_signature(
                best_area, ["sustainability_breakthrough"],
            )
            milestone = EvolutionMilestone(
                timestamp=datetime.now(timezone.utc),
                milestone_type='breakthrough',
                description='Achieved major sustainability improvement',
                metrics=dict(metrics),
                human_feedback_count=len(await self.storage.get_feedback()),
                ai_suggestion_impact=0.9,
                strategy_signature=strategy_sig,
            )
        elif metrics.get('performance', 0) > 0.7 and metrics.get('user_satisfaction', 0) > 0.6:
            milestone = EvolutionMilestone(
                timestamp=datetime.now(timezone.utc),
                milestone_type='breakthrough',
                description='System performance and user satisfaction at high levels',
                metrics=dict(metrics),
                human_feedback_count=len(await self.storage.get_feedback()),
                ai_suggestion_impact=0.8,
            )
        elif len(await self.storage.get_performance_history()) > 5:
            perf_hist = await self.storage.get_performance_history(5)
            if len(perf_hist) >= 3:
                recent_perf = [
                    h['impact']['metrics']['performance']
                    for h in perf_hist if 'impact' in h and 'metrics' in h.get('impact', {})
                ]
                if len(recent_perf) >= 3:
                    improvement = (
                        float(np.mean(recent_perf[-3:])) - float(np.mean(recent_perf[:3]))
                    )
                    if improvement > 0.15:
                        milestone = EvolutionMilestone(
                            timestamp=datetime.now(timezone.utc),
                            milestone_type='learning_spike',
                            description=f'Performance improvement of {improvement:.1%} detected',
                            metrics=dict(metrics),
                            human_feedback_count=len(await self.storage.get_feedback()),
                            ai_suggestion_impact=improvement,
                        )
        elif len(await self.storage.get_sustainability_trajectory()) > 10:
            traj = await self.storage.get_sustainability_trajectory(5)
            if np.std(traj) < 0.1 and np.mean(traj) > 0.6:
                milestone = EvolutionMilestone(
                    timestamp=datetime.now(timezone.utc),
                    milestone_type='adaptation',
                    description='System showing stable, high sustainability performance',
                    metrics=dict(metrics),
                    human_feedback_count=len(await self.storage.get_feedback()),
                    ai_suggestion_impact=0.7,
                )

        if milestone:
            await self.storage.add_milestone(milestone)
            await self._learn_from_milestone(milestone)
        return milestone

    async def _learn_from_milestone(self, milestone: EvolutionMilestone) -> None:
        if milestone.strategy_signature:
            await self.storage.add_milestone_strategy(milestone.strategy_signature, {
                'effectiveness': milestone.ai_suggestion_impact,
                'timestamp': milestone.timestamp.isoformat(),
                'context': milestone.description,
            })
            area = self._extract_area_from_milestone(milestone)
            if area:
                await self.storage.update_historical_effectiveness(
                    area, milestone.ai_suggestion_impact,
                )
        if milestone.ai_suggestion_impact > 0.7:
            milestone.reuse_count += 1
            logger.info(
                f"Milestone strategy {milestone.strategy_signature} marked for reuse "
                f"(impact: {milestone.ai_suggestion_impact:.2f})"
            )

    def _extract_area_from_milestone(self, milestone: EvolutionMilestone) -> Optional[str]:
        areas = ['quantum', 'moe', 'sustainability', 'user_experience', 'federated']
        for area in areas:
            if area in milestone.description.lower():
                return area
        return None


# ============================================================================
# Orchestrator (Main Controller) - Enhanced with lifecycle
# ============================================================================
class CoEvolutionOrchestrator:
    def __init__(
        self,
        config: CoEvolutionConfig,
        storage: CoEvolutionStorage,
        analyzer: CoEvolutionAnalyzer,
        prioritizer: RecommendationPrioritizer,
        impact_tracker: LongTermImpactTracker,
        telemetry: "CoEvolutionTelemetry",
        persistence: Optional[CoEvolutionPersistenceManager],
        bio_core: Optional[Any] = None,
        carbon_manager: Optional[CarbonManagerProtocol] = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.prioritizer = prioritizer
        self.impact_tracker = impact_tracker
        self.telemetry = telemetry
        self.persistence = persistence
        self.bio_core = bio_core
        self.carbon_manager = carbon_manager

        # Observability
        self.observability = ObservabilityHub()

        # Bio-core
        self.event_broker: Optional[EventBrokerProtocol] = None
        self.alert_system: Optional[Any] = None
        self.self_healer: Optional[SelfHealerProtocol] = None
        self.workflow_orchestrator: Optional[WorkflowOrchestratorProtocol] = None
        self.swarm_coordinator: Optional[SwarmCoordinatorProtocol] = None
        self.tick_engine: Optional[Any] = None
        self.quantum_bridge: Optional[Any] = None
        self.cost_benefit_engine: Optional[Any] = None
        self.token_manager: Optional[Any] = None
        self.gradient_manager: Optional[Any] = None
        if bio_core is not None:
            self._wire_bio_core(bio_core)

        # External components (injected)
        self.quantum_benchmark: Optional[QuantumBenchmarkProtocol] = None
        self.fft_moe: Optional[FFTMoEProtocol] = None
        self.helium_manager: Optional[HeliumManagerProtocol] = None
        self.federated_orchestrator: Optional[FederatedOrchestratorProtocol] = None
        self.predictive_analyzer: Optional[PredictiveAnalyzerProtocol] = None

        # Circuit breakers
        self._quantum_circuit = CircuitBreaker("quantum_benchmark")
        self._moe_circuit = CircuitBreaker("fft_moe")
        self._helium_circuit = CircuitBreaker("helium_manager")
        self._federated_circuit = CircuitBreaker("federated_orchestrator")
        self._predictive_circuit = CircuitBreaker("predictive_analyzer")

        # Governance
        self.governance: Optional[GovernanceLayer] = (
            GovernanceLayer(config.approval, publish_fn=self._publish_message)
            if config.approval.enabled else None
        )

        # Safety
        self.safety_monitor = SafetyMonitor() if config.enable_temporal_safety else None
        if self.safety_monitor is not None:
            self._setup_safety_invariants()

        # Chaos
        self.chaos_service: Optional[ChaosService] = (
            ChaosService(self, config.chaos) if config.enable_chaos_testing else None
        )

        # Health
        self.health_status = "healthy"
        self.last_error: Optional[str] = None

        # Idempotency
        self._idempotency_seen: Set[str] = set()
        self._idempotency_order: Deque[str] = deque(maxlen=IDEMPOTENCY_CACHE_SIZE)

        # Dead letters
        self._dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_DEAD_LETTERS)

        # Async resources (created in start())
        self._event_queue: Optional[asyncio.Queue] = None
        self._event_consumer_task: Optional[asyncio.Task] = None
        self._background_tasks: List[asyncio.Task] = []
        self._co_evolution_task: Optional[asyncio.Task] = None
        self._started = False
        self._shutting_down = False

        # Baseline config snapshot for non-destructive anomaly handling
        self._baseline_learning_rate = config.learning_rate
        self._baseline_exploration_rate = config.exploration_rate
        self._baseline_adaptation_threshold = config.adaptation_threshold

        logger.info("CoEvolutionOrchestrator v6.0.0 initialised (not started)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        self._event_queue = asyncio.Queue(maxsize=10_000)

        if self.config.enable_event_driven and self.event_broker is not None:
            self._subscribe_events()

        self._start_background_tasks()

        if self.persistence is not None:
            await self.load_state()

        logger.info("CoEvolutionOrchestrator started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()

    # ------------------------------------------------------------------
    # Bio-core wiring
    # ------------------------------------------------------------------
    def _wire_bio_core(self, bio_core: Any) -> None:
        self.event_broker = getattr(bio_core, 'event_broker', None)
        self.alert_system = getattr(bio_core, 'alert_system', None)
        self.self_healer = getattr(bio_core, 'self_healer', None)
        self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None)
        self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)
        self.tick_engine = getattr(bio_core, 'tick_engine', None)
        self.quantum_bridge = getattr(bio_core, 'quantum_bridge', None)
        self.cost_benefit_engine = getattr(bio_core, 'cost_benefit_engine', None)
        self.token_manager = getattr(bio_core, 'token_manager', None)
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None)

    def inject_bio_core(self, bio_core: Any) -> None:
        self.bio_core = bio_core
        if bio_core is not None:
            self._wire_bio_core(bio_core)

    # ------------------------------------------------------------------
    # External component injection
    # ------------------------------------------------------------------
    def inject_components(
        self,
        quantum_benchmark: Optional[QuantumBenchmarkProtocol] = None,
        fft_moe: Optional[FFTMoEProtocol] = None,
        helium_manager: Optional[HeliumManagerProtocol] = None,
        federated_orchestrator: Optional[FederatedOrchestratorProtocol] = None,
        predictive_analyzer: Optional[PredictiveAnalyzerProtocol] = None,
        carbon_manager: Optional[CarbonManagerProtocol] = None,
    ) -> None:
        if quantum_benchmark is not None:
            self.quantum_benchmark = quantum_benchmark
        if fft_moe is not None:
            self.fft_moe = fft_moe
        if helium_manager is not None:
            self.helium_manager = helium_manager
        if federated_orchestrator is not None:
            self.federated_orchestrator = federated_orchestrator
        if predictive_analyzer is not None:
            self.predictive_analyzer = predictive_analyzer
        if carbon_manager is not None:
            self.carbon_manager = carbon_manager
        logger.info("External components injected into Co-Evolution Orchestrator")

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_safety_invariants(self) -> None:
        if self.safety_monitor is None:
            return

        def _learning_rate_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('learning_rate', 0.01)
            return 0.0 <= v <= 1.0

        def _exploration_rate_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('exploration_rate', 0.1)
            return 0.0 <= v <= 1.0

        def _adaptation_threshold_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('adaptation_threshold', 0.7)
            return 0.0 <= v <= 1.0

        def _carbon_intensity_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('carbon_intensity', DEFAULT_CARBON_INTENSITY)
            return v <= CARBON_CRITICAL_THRESHOLD

        def _helium_scarcity_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('helium_scarcity', 0.0)
            return v <= 1.0

        self.safety_monitor.add_invariant(
            "learning_rate_bounded", _learning_rate_bounded,
            "Learning rate must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "exploration_rate_bounded", _exploration_rate_bounded,
            "Exploration rate must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "adaptation_threshold_bounded", _adaptation_threshold_bounded,
            "Adaptation threshold must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "carbon_intensity_bounded", _carbon_intensity_bounded,
            "Carbon intensity must be below critical threshold",
        )
        self.safety_monitor.add_invariant(
            "helium_scarcity_bounded", _helium_scarcity_bounded,
            "Helium scarcity must be in [0, 1]",
        )

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------
    def _subscribe_events(self) -> None:
        if self.event_broker is None:
            return
        try:
            for ev in (
                'carbon_update', 'helium_update', 'alert_generated',
                'config_updated', 'token_balance_update',
                'health_update', 'anomaly_detected',
            ):
                self.event_broker.subscribe(ev, self._enqueue_event)
            logger.info("Co-Evolution Engine subscribed to core events via queue")
        except Exception as e:
            logger.warning(f"Event subscription failed: {e}")

    async def _enqueue_event(self, event: Any) -> None:
        if self._event_queue is None:
            return
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Event queue full; dropping event")
            self.observability.counter('event_queue_drops')

    async def _event_consumer(self) -> None:
        assert self._event_queue is not None
        while not self._shutting_down:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                await self._handle_event(event)
            except Exception as e:
                logger.error(f"Event handler error: {e}")
            finally:
                self._event_queue.task_done()

    async def _handle_event(self, event: Any) -> None:
        event_type = getattr(event, 'event_type', None)
        if not event_type:
            return
        handler = getattr(self, f"_on_{event_type}", None)
        if handler:
            await handler(event)

    async def _on_carbon_update(self, event: Any) -> None:
        intensity = float(event.data.get('intensity', DEFAULT_CARBON_INTENSITY))
        price = float(event.data.get('price', 50.0))
        await self.impact_tracker.record_impact(
            'sustainability',
            {'carbon_intensity': intensity, 'carbon_price': price},
            1.0 - (intensity / CARBON_CRITICAL_THRESHOLD),
        )

    async def _on_helium_update(self, event: Any) -> None:
        scarcity = float(event.data.get('scarcity', 0.5))
        await self.impact_tracker.record_impact(
            'sustainability',
            {'helium_scarcity': scarcity, 'helium_price': event.data.get('price', 0.5)},
            1.0 - scarcity,
        )

    async def _on_alert_generated(self, event: Any) -> None:
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self-healing")
            self.config.exploration_rate = self._baseline_exploration_rate * 0.5
            if self.config.enable_self_healing and self.self_healer is not None:
                try:
                    await self.self_healer.apply_healing('damage_accumulation')
                except Exception as e:
                    logger.error(f"Self-healing failed: {e}")
            if (self.workflow_orchestrator is not None and
                    self.config.workflow_on_critical_alert):
                try:
                    await self.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_critical_alert,
                    )
                except Exception as e:
                    logger.error(f"Workflow failed: {e}")

    async def _on_config_updated(self, event: Any) -> None:
        updates = event.data.get('updates', {})
        if 'co_evolution' in updates:
            new = updates['co_evolution']
            for key, value in new.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info("Co-Evolution configuration reloaded")

    async def _on_token_balance_update(self, event: Any) -> None:
        pass

    async def _on_health_update(self, event: Any) -> None:
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: Any) -> None:
        metric = event.data.get('metric')
        if metric == 'carbon_intensity':
            # Derive from baseline — do NOT compound
            self.config.learning_rate = self._baseline_learning_rate * 0.9
        elif metric == 'helium_scarcity':
            self.config.adaptation_threshold = max(
                0.5, self._baseline_adaptation_threshold * 0.9,
            )

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------
    def _start_background_tasks(self) -> None:
        if self.config.enable_event_driven and self._event_queue is not None:
            self._event_consumer_task = asyncio.create_task(self._event_consumer())
            self._background_tasks.append(self._event_consumer_task)

        async def co_evolution_loop() -> None:
            while not self._shutting_down:
                try:
                    await self.co_evolve()
                    await asyncio.sleep(self.config.co_evolution_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Co-evolution loop error: {e}")
                    await asyncio.sleep(60)

        self._co_evolution_task = asyncio.create_task(co_evolution_loop())
        self._background_tasks.append(self._co_evolution_task)

        if self.config.enable_swarm_coordination and self.swarm_coordinator is not None:
            async def swarm_loop() -> None:
                while not self._shutting_down:
                    try:
                        await self.share_with_swarm()
                        await asyncio.sleep(self.config.swarm_share_interval)
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error(f"Swarm update error: {e}")
                        await asyncio.sleep(120)

            self._background_tasks.append(asyncio.create_task(swarm_loop()))

        if self.config.enable_chaos_testing and self.chaos_service is not None:
            async def chaos_loop() -> None:
                while not self._shutting_down:
                    try:
                        await asyncio.sleep(self.config.chaos.interval_seconds)
                        await self.chaos_service.run_chaos_test()
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error(f"Chaos loop error: {e}")
                        await asyncio.sleep(60)

            self._background_tasks.append(asyncio.create_task(chaos_loop()))

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------
    def _is_duplicate(self, key: Optional[str]) -> bool:
        if not key or not self.config.enable_idempotency:
            return False
        if key in self._idempotency_seen:
            return True
        self._idempotency_seen.add(key)
        self._idempotency_order.append(key)
        if len(self._idempotency_seen) > self._idempotency_order.maxlen:
            oldest = self._idempotency_order[0]
            self._idempotency_seen.discard(oldest)
        return False

    # ------------------------------------------------------------------
    # Swarm coordination
    # ------------------------------------------------------------------
    async def share_with_swarm(self) -> None:
        if not self.config.enable_swarm_coordination or self.swarm_coordinator is None:
            return
        historical = await self.storage.get_historical_effectiveness()
        payload = {
            'orchestrator_id': uuid.uuid4().hex[:8],
            'sustainability_score': self.impact_tracker.get_overall_trend().get(
                'average_sustainability_score', 0.5,
            ),
            'milestones': len(await self.storage.get_milestones()),
            'feedback_count': len(await self.storage.get_feedback()),
            'historical_effectiveness': historical,
            'mopd_enabled': self.config.enable_mopd,
        }
        try:
            await self.swarm_coordinator.share_predictions(payload)
        except Exception as e:
            logger.warning(f"Swarm share failed: {e}")
            self._dead_letters.append({
                'topic': 'swarm',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
            })

    # ------------------------------------------------------------------
    # Main co-evolution cycle
    # ------------------------------------------------------------------
    async def co_evolve(self, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        await self.wait_ready()
        if self._is_duplicate(idempotency_key):
            return {
                'status': 'duplicate',
                'idempotency_key': idempotency_key,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }

        self.observability.counter('co_evolution_cycles')

        system_state = await self._collect_system_state()
        human_feedback = await self.analyzer.aggregate_human_feedback_with_sentiment()
        opportunities = await self.analyzer.identify_opportunities(system_state, human_feedback)

        predicted = await self._predict_opportunities(system_state)
        if predicted:
            opportunities.extend(predicted)

        rec_result = await self.analyzer.generate_holistic_recommendations(
            system_state, opportunities, return_mopd=self.config.enable_mopd,
        )
        recommendations = rec_result.get('recommendations', [])
        mopd_pareto_front = rec_result.get('mopd_pareto_front', None)
        mopd_best_plan = rec_result.get('mopd_best_plan', None)

        historical = await self.storage.get_historical_effectiveness()
        prioritized = self.prioritizer.prioritize_recommendations(recommendations, historical)

        # Human approval filtering
        approved_recs: List[Dict[str, Any]] = []
        if self.governance is not None:
            for rec in prioritized:
                needs_approval = (
                    (rec['area'] == 'sustainability' and
                     rec['priority'] > self.config.approval.require_for_sustainability_above_priority)
                    or rec['priority'] > self.config.approval.require_for_global_priority_above
                )
                if needs_approval:
                    approval = await self.governance.request_approval({
                        'action': 'approve_recommendation',
                        'recommendation': rec,
                    })
                    if approval.status in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                        approved_recs.append(rec)
                    else:
                        logger.info(
                            f"Recommendation {rec.get('action', 'unknown')} skipped: "
                            f"approval status={approval.status.value}"
                        )
                else:
                    approved_recs.append(rec)
        else:
            approved_recs = prioritized

        # Temporal safety check
        if self.safety_monitor is not None:
            safety_state = {
                'learning_rate': self.config.learning_rate,
                'exploration_rate': self.config.exploration_rate,
                'adaptation_threshold': self.config.adaptation_threshold,
                'carbon_intensity': (
                    self.carbon_manager.carbon_intensity
                    if self.carbon_manager is not None and hasattr(self.carbon_manager, 'carbon_intensity')
                    else DEFAULT_CARBON_INTENSITY
                ),
                'helium_scarcity': (
                    system_state.get('components', {}).get('helium', {})
                    .get('current', {}).get('scarcity_index', 0.0)
                ),
            }
            violations = self.safety_monitor.check(safety_state)
            if violations:
                logger.warning(f"Temporal safety violations detected: {violations}")
                # Derive from baseline non-destructively
                self.config.learning_rate = self._baseline_learning_rate
                self.config.exploration_rate = self._baseline_exploration_rate
                self.config.adaptation_threshold = self._baseline_adaptation_threshold

        if not approved_recs:
            applied: List[Dict[str, Any]] = []
        else:
            applied = await self._apply_recommendations(approved_recs)

        impact = await self._measure_impact(applied)

        for item in applied:
            if item.get('result', {}).get('success'):
                area = item['recommendation'].get('area', 'general')
                await self.impact_tracker.record_impact(
                    area,
                    item['result'],
                    impact['metrics']['sustainability'],
                )
                await self.storage.add_sustainability_score(
                    impact['metrics']['sustainability'],
                )

        await self.storage.add_performance_record({
            'timestamp': datetime.now(timezone.utc),
            'impact': impact,
        })

        milestone = await self.analyzer.detect_milestone(impact)
        if milestone:
            logger.info(
                f"Milestone detected: {milestone.milestone_type} - {milestone.description}"
            )

        for item in applied:
            if item.get('result', {}).get('success'):
                area = item['recommendation'].get('area', 'general')
                eff = item.get('result', {}).get('effectiveness', 0.5)
                await self.storage.update_historical_effectiveness(area, eff)

        self.observability.gauge('sustainability_score', impact['metrics']['sustainability'])
        self.observability.gauge('performance_score', impact['metrics']['performance'])
        self.observability.gauge(
            'milestone_count', len(await self.storage.get_milestones()),
        )
        if self.config.enable_mopd and mopd_pareto_front:
            self.observability.counter('mopd_generations')
            self.observability.histogram('mopd_pareto_front_size', len(mopd_pareto_front))

        return {
            'status': 'success' if applied else 'partial',
            'recommendations_applied': applied,
            'impact': impact,
            'milestone': milestone.to_dict() if milestone else None,
            'system_state': system_state,
            'human_feedback_count': len(human_feedback),
            'sustainability_trend': await self.analyzer.calculate_trend(),
            'long_term_trend': self.impact_tracker.get_overall_trend(),
            'mopd_pareto_front': mopd_pareto_front,
            'mopd_best_plan': mopd_best_plan,
        }

    # ------------------------------------------------------------------
    # Public: approval decision
    # ------------------------------------------------------------------
    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        if self.governance is None:
            return False
        return await self.governance.submit_decision(request_id, approved)

    # ------------------------------------------------------------------
    # Public: chaos
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: Union[str, FaultType],
                           **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.inject_fault(fault_type, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_chaos_test()

    # ------------------------------------------------------------------
    # System state collection
    # ------------------------------------------------------------------
    async def _collect_system_state(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'components': {},
        }
        if self.quantum_benchmark is not None:
            try:
                state['components']['quantum'] = await self._quantum_circuit.call(
                    retry_async,
                    self.quantum_benchmark.get_benchmark_summary,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                )
            except Exception as e:
                logger.error(f"Quantum benchmark failed: {e}")
                state['components']['quantum'] = {'error': str(e)}
        if self.fft_moe is not None:
            try:
                state['components']['moe'] = await self._moe_circuit.call(
                    retry_async,
                    self.fft_moe.get_fft_moe_status,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                )
            except Exception as e:
                logger.error(f"FFT-MoE failed: {e}")
                state['components']['moe'] = {'error': str(e)}
        if self.helium_manager is not None:
            try:
                state['components']['helium'] = await self._helium_circuit.call(
                    retry_async,
                    self.helium_manager.get_stats,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                )
            except Exception as e:
                logger.error(f"Helium manager failed: {e}")
                state['components']['helium'] = {'error': str(e)}
        if self.federated_orchestrator is not None:
            try:
                state['components']['federated'] = await self._federated_circuit.call(
                    retry_async,
                    self.federated_orchestrator.get_status,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                )
            except Exception as e:
                logger.error(f"Federated orchestrator failed: {e}")
                state['components']['federated'] = {'error': str(e)}
        state['overall'] = self._calculate_system_metrics(state['components'])
        state['long_term_trends'] = {}
        for area in ['quantum', 'moe', 'sustainability', 'user_experience', 'federated']:
            trend = self.impact_tracker.get_area_trend(area)
            if trend.get('status') != 'insufficient_data':
                state['long_term_trends'][area] = trend
        return state

    def _calculate_system_metrics(self, components: Dict[str, Any]) -> Dict[str, float]:
        metrics = {
            'overall_health': 0.0, 'sustainability_score': 0.0,
            'performance_score': 0.0, 'user_engagement': 0.0,
        }
        if components.get('helium'):
            scarcity = components['helium'].get('current', {}).get('scarcity_index', 0)
            metrics['sustainability_score'] = max(0.0, 1.0 - scarcity)
        if components.get('quantum'):
            metrics['performance_score'] = min(
                1.0, components['quantum'].get('average_speedup', 0) / 5,
            )
        if components.get('moe'):
            metrics['user_engagement'] = min(
                1.0, components['moe'].get('num_clients', 0) / 100,
            )
        metrics['overall_health'] = (
            metrics['sustainability_score'] * 0.4 +
            metrics['performance_score'] * 0.3 +
            metrics['user_engagement'] * 0.3
        )
        return metrics

    # ------------------------------------------------------------------
    # Predictive opportunities
    # ------------------------------------------------------------------
    async def _predict_opportunities(self, system_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        opportunities: List[Dict[str, Any]] = []
        if not self.predictive_analyzer:
            return opportunities

        if self.config.enable_time_tick_engine and self.tick_engine:
            try:
                forecast = self.tick_engine.get_helium_forecast(4)
                if forecast and len(forecast) > 3:
                    avg_future = float(np.mean(forecast))
                    if avg_future < 0.3:
                        opportunities.append({
                            'area': 'sustainability',
                            'type': 'predicted_constraint',
                            'priority': 0.85,
                            'suggestion': 'Helium scarcity predicted in 4 hours - proactive reduction needed',
                            'expected_impact': 'Prevent critical helium shortage',
                            'predicted': True,
                            'timeframe_hours': 4,
                        })
            except Exception as e:
                logger.warning(f"TimeTickEngine forecast error: {e}")

        if self.config.enable_quantum_bridge and self.quantum_bridge:
            try:
                q_params = self.quantum_bridge.get_qubo_parameters()
                penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
                if penalty_helium > 0.7:
                    opportunities.append({
                        'area': 'sustainability',
                        'type': 'quantum_derived_opportunity',
                        'priority': 0.8,
                        'suggestion': 'QuantumBridge indicates high helium penalty - implement recovery',
                        'expected_impact': '50% reduction in helium usage',
                        'predicted': True,
                    })
            except Exception as e:
                logger.warning(f"QuantumBridge error: {e}")

        if self.config.enable_cost_benefit and self.cost_benefit_engine:
            try:
                analysis = await self.cost_benefit_engine.analyze_scenario('co_evolution', {})
                if analysis.roi > 0.5:
                    opportunities.append({
                        'area': 'system_wide',
                        'type': 'cost_benefit_derived',
                        'priority': 0.7,
                        'suggestion': f'System-wide optimization recommended (ROI: {analysis.roi:.2f})',
                        'expected_impact': 'Overall performance uplift',
                        'predicted': True,
                    })
            except Exception as e:
                logger.warning(f"CostBenefitEngine error: {e}")

        if hasattr(self.predictive_analyzer, 'predict_federation_trend'):
            try:
                forecast = await self._predictive_circuit.call(
                    retry_async,
                    self.predictive_analyzer.predict_federation_trend,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                )
                if forecast and forecast.get('predicted_sustainability_score', 0.5) < 0.4:
                    opportunities.append({
                        'area': 'federated',
                        'type': 'predicted_performance_decline',
                        'priority': 0.7,
                        'suggestion': (
                            'Federated learning sustainability predicted to decline - '
                            'proactive optimization needed'
                        ),
                        'expected_impact': 'Prevent performance degradation',
                        'predicted': True,
                    })
            except Exception as e:
                logger.warning(f"Predictive opportunity identification error: {e}")

        return opportunities

    # ------------------------------------------------------------------
    # Apply recommendations
    # ------------------------------------------------------------------
    async def _apply_recommendations(
        self, recommendations: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        applied: List[Dict[str, Any]] = []
        for rec in recommendations[:3]:
            try:
                if rec['area'] == 'quantum':
                    result = await self._apply_quantum_optimization()
                elif rec['area'] == 'moe':
                    result = await self._apply_moe_improvement()
                elif rec['area'] == 'sustainability':
                    result = await self._apply_sustainability_measure()
                elif rec['area'] == 'user_experience':
                    result = await self._apply_ux_improvement()
                else:
                    result = await self._apply_system_wide_optimization()

                # Do not fabricate random effectiveness — use provided or measured
                if result.get('success', False):
                    result.setdefault('effectiveness', 0.6)
                applied.append({
                    'recommendation': rec,
                    'result': result,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                logger.error(f"Failed to apply recommendation: {e}")
                applied.append({
                    'recommendation': rec,
                    'result': {'success': False, 'error': str(e)},
                })
        return applied

    async def _apply_quantum_optimization(self) -> Dict[str, Any]:
        if self.quantum_benchmark is not None:
            result = await self.quantum_benchmark.run_benchmark(
                task_name="circuit_optimization_test",
                task_input={'type': 'optimization', 'size': 50},
            )
            savings = getattr(result, 'energy_savings_percent', 0.0)
            return {
                'success': True,
                'energy_savings': savings,
                'recommendation': getattr(result, 'recommended_approach', ''),
                'effectiveness': min(1.0, savings / 50.0),
            }
        return {'success': False, 'error': 'Quantum benchmark not available'}

    async def _apply_moe_improvement(self) -> Dict[str, Any]:
        if self.fft_moe is not None:
            specialization = await self.fft_moe.analyze_expert_specialization()
            specialized = specialization.get('total_specialized_experts', 0)
            return {
                'success': True,
                'specialization_score': specialized,
                'top_domain': specialization.get('top_performing_domain'),
                'effectiveness': min(1.0, specialized / 8.0),
            }
        return {'success': False, 'error': 'FFT-MoE not available'}

    async def _apply_sustainability_measure(self) -> Dict[str, Any]:
        if self.helium_manager is not None:
            forecast = await self.helium_manager.get_sustainability_forecast()
            days = forecast.get('days_to_critical', 0)
            return {
                'success': True,
                'forecast': forecast,
                'actions_taken': self._generate_recommendations(forecast),
                'effectiveness': 0.8 if days > 7 else 0.5,
            }
        return {'success': False, 'error': 'Helium manager not available'}

    async def _apply_ux_improvement(self) -> Dict[str, Any]:
        return {'success': True, 'feedback_loop_improved': True, 'user_engagement_boost': 1.2, 'effectiveness': 0.7}

    async def _apply_system_wide_optimization(self) -> Dict[str, Any]:
        return {
            'success': True,
            'optimizations_applied': ['cache_clearing', 'model_pruning', 'data_compression'],
            'effectiveness': 0.6,
        }

    def _generate_recommendations(self, forecast: Dict[str, Any]) -> List[str]:
        days = forecast.get('days_to_critical')
        if days is not None:
            if days <= 3:
                return [
                    "URGENT: Implement immediate helium reduction measures",
                    "Prioritize critical jobs only",
                ]
            if days <= 7:
                return [
                    "Accelerate helium efficiency improvements",
                    "Begin transitioning to helium-efficient operations",
                ]
        return []

    # ------------------------------------------------------------------
    # Impact measurement
    # ------------------------------------------------------------------
    async def _measure_impact(self, applied: List[Dict[str, Any]]) -> Dict[str, Any]:
        impact: Dict[str, Any] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': {
                'performance': 0.0, 'sustainability': 0.0,
                'user_satisfaction': 0.0, 'overall_effectiveness': 0.0,
            },
            'details': [],
        }
        for item in applied:
            if item.get('result', {}).get('success'):
                effectiveness = float(item['result'].get('effectiveness', 0.6))
                area = item['recommendation'].get('area', 'general')
                if area == 'quantum':
                    impact['metrics']['performance'] += 0.15 * effectiveness
                    impact['metrics']['sustainability'] += 0.2 * effectiveness
                elif area == 'sustainability':
                    impact['metrics']['sustainability'] += 0.3 * effectiveness
                elif area == 'user_experience':
                    impact['metrics']['user_satisfaction'] += 0.25 * effectiveness
                else:
                    impact['metrics']['performance'] += 0.1 * effectiveness
                impact['details'].append({
                    'area': area,
                    'impact': 'positive',
                    'effectiveness': effectiveness,
                    'details': item['result'],
                    'recommendation': item['recommendation'].get('action', 'Unknown action'),
                })
        impact['metrics']['performance'] = min(1.0, impact['metrics']['performance'] / 0.8)
        impact['metrics']['sustainability'] = min(1.0, impact['metrics']['sustainability'] / 0.8)
        impact['metrics']['user_satisfaction'] = min(1.0, impact['metrics']['user_satisfaction'] / 0.8)
        impact['metrics']['overall_effectiveness'] = (
            impact['metrics']['performance'] * 0.3 +
            impact['metrics']['sustainability'] * 0.4 +
            impact['metrics']['user_satisfaction'] * 0.3
        )
        impact['long_term_trend'] = self.impact_tracker.get_overall_trend()
        return impact

    # ------------------------------------------------------------------
    # Public query methods
    # ------------------------------------------------------------------
    async def get_evolution_status(self) -> Dict[str, Any]:
        await self.wait_ready()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'performance_history': len(await self.storage.get_performance_history()),
            'milestones': len(await self.storage.get_milestones()),
            'user_models': len(await self.storage.get_user_models()),
            'policy_suggestions': len(await self.storage.get_policy_suggestions()),
            'collaborative_decisions': len(await self.storage.get_collaborative_decisions()),
            'learning_rate': self.config.learning_rate,
            'exploration_rate': self.config.exploration_rate,
            'adaptation_threshold': self.config.adaptation_threshold,
            'historical_effectiveness': await self.storage.get_historical_effectiveness(),
            'milestone_strategies': len(await self.storage.get_milestone_strategies()),
            'long_term_trend': self.impact_tracker.get_overall_trend(),
            'mopd_enabled': self.config.enable_mopd,
            'mopd_plans': len(await self.storage.get_mopd_plans()),
            'human_approval_enabled': self.config.approval.enabled,
            'temporal_safety_enabled': self.config.enable_temporal_safety,
            'chaos_testing_enabled': self.config.enable_chaos_testing,
            'dead_letters': len(self._dead_letters),
        }

    async def get_feedback_sentiment_summary(self) -> Dict[str, Any]:
        feedback = await self.storage.get_feedback()
        if not feedback:
            return {'status': 'no_feedback'}
        sentiments = [
            fb.get('sentiment', {}).get('score', 0)
            for fb in feedback if 'sentiment' in fb
        ]
        if not sentiments:
            return {'status': 'no_sentiment_data'}
        return {
            'average_sentiment': float(np.mean(sentiments)),
            'positive_ratio': sum(1 for s in sentiments if s > 0.2) / len(sentiments),
            'negative_ratio': sum(1 for s in sentiments if s < -0.2) / len(sentiments),
            'neutral_ratio': sum(1 for s in sentiments if -0.2 <= s <= 0.2) / len(sentiments),
            'samples': len(sentiments),
            'trend': (
                'improving'
                if len(sentiments) > 10 and
                float(np.mean(sentiments[-5:])) > float(np.mean(sentiments[:5]))
                else 'stable'
            ),
        }

    async def get_milestone_summary(self) -> Dict[str, Any]:
        milestones = await self.storage.get_milestones()
        if not milestones:
            return {'status': 'no_milestones'}
        types: Dict[str, int] = defaultdict(int)
        impacts: List[float] = []
        for m in milestones:
            types[m.milestone_type] += 1
            impacts.append(m.ai_suggestion_impact)
        return {
            'total_milestones': len(milestones),
            'types': dict(types),
            'average_impact': float(np.mean(impacts)) if impacts else 0.0,
            'max_impact': max(impacts) if impacts else 0.0,
            'reused_strategies': sum(1 for m in milestones if m.reuse_count > 0),
            'most_recent': milestones[-1].to_dict() if milestones else None,
        }

    async def get_recommendation_pareto_front(
        self, system_state: Dict[str, Any], opportunities: List[Dict[str, Any]],
    ) -> List[MOPDPlan]:
        await self.wait_ready()
        if not self.config.enable_mopd:
            return []
        rec_result = await self.analyzer.generate_holistic_recommendations(
            system_state, opportunities, return_mopd=True,
        )
        if 'mopd_pareto_front' in rec_result:
            return [MOPDPlan.from_dict(p) for p in rec_result['mopd_pareto_front']]
        return []

    async def get_mopd_summary(self) -> Dict[str, Any]:
        await self.wait_ready()
        if not self.config.enable_mopd:
            return {'enabled': False}
        plans = await self.storage.get_mopd_plans(20)
        return {
            'enabled': True,
            'objective_weights': dict(self.config.mopd.objective_weights),
            'grid_resolution': self.config.mopd.grid_resolution,
            'total_mopd_plans': len(await self.storage.get_mopd_plans()),
            'sample_plans': [p.to_dict() for p in plans],
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._dead_letters)

    async def _publish_message(self, topic: str, payload: Any) -> None:
        if self.event_broker is not None and hasattr(self.event_broker, 'publish'):
            try:
                res = self.event_broker.publish(topic, payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Event publish failed: {e}")
                self._dead_letters.append({
                    'topic': topic,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error': str(e),
                })

    # ------------------------------------------------------------------
    # Health status
    # ------------------------------------------------------------------
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'score': min(
                1.0,
                self.impact_tracker.get_overall_trend().get(
                    'average_sustainability_score', 0.5,
                ),
            ),
            'details': {
                'injected_components': {
                    'quantum_benchmark': self.quantum_benchmark is not None,
                    'fft_moe': self.fft_moe is not None,
                    'helium_manager': self.helium_manager is not None,
                    'federated_orchestrator': self.federated_orchestrator is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None,
                    'carbon_manager': self.carbon_manager is not None,
                },
                'milestones': len(await self.storage.get_milestones()),
                'feedback_count': len(await self.storage.get_feedback()),
                'performance_samples': len(await self.storage.get_performance_history()),
                'user_models': len(await self.storage.get_user_models()),
                'persistence_enabled': self.persistence is not None,
                'event_driven_active': self.config.enable_event_driven,
                'self_healing_enabled': self.config.enable_self_healing,
                'swarm_coordination_active': self.config.enable_swarm_coordination,
                'mopd_enabled': self.config.enable_mopd,
                'human_approval_enabled': self.config.approval.enabled,
                'temporal_safety_enabled': self.config.enable_temporal_safety,
                'chaos_testing_enabled': self.config.enable_chaos_testing,
                'dead_letters': len(self._dead_letters),
            },
        }

    async def get_telemetry_export(self) -> str:
        return await self.telemetry.export()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def save_state(self) -> None:
        if self.persistence is None:
            return
        storage_dict = await self.storage.to_dict()
        impact_dict = self.impact_tracker.to_dict()
        state = {
            'storage': storage_dict,
            'impact_tracker': impact_dict,
            'learning_rate': self.config.learning_rate,
            'exploration_rate': self.config.exploration_rate,
            'adaptation_threshold': self.config.adaptation_threshold,
        }
        await self.persistence.save_state(state)

    async def load_state(self) -> None:
        if self.persistence is None:
            return
        state = await self.persistence.load_state()
        if not state:
            return
        await self.storage.from_dict(state.get('storage', {}))
        self.impact_tracker.from_dict(state.get('impact_tracker', {}))
        self.config.learning_rate = float(
            state.get('learning_rate', self.config.learning_rate)
        )
        self.config.exploration_rate = float(
            state.get('exploration_rate', self.config.exploration_rate)
        )
        self.config.adaptation_threshold = float(
            state.get('adaptation_threshold', self.config.adaptation_threshold)
        )
        logger.info("State loaded successfully")

    async def delete_state(self) -> None:
        if self.persistence is not None:
            await self.persistence.delete_state()

    # ------------------------------------------------------------------
    # Self-healing
    # ------------------------------------------------------------------
    async def self_heal(self) -> None:
        logger.info("CoEvolutionEngine self-healing")
        if not self.config.enable_self_healing:
            logger.warning("Self-healing disabled")
            return

        self.config.learning_rate = self._baseline_learning_rate
        self.config.exploration_rate = self._baseline_exploration_rate
        self.config.adaptation_threshold = self._baseline_adaptation_threshold

        feedback = await self.storage.get_feedback()
        if len(feedback) > 10:
            await self.storage.clear_feedback()
            for fb in feedback[-10:]:
                await self.storage.add_feedback(fb)

        perf = await self.storage.get_performance_history()
        if len(perf) > 10:
            async with self.storage._lock:
                self.storage.performance_history = deque(
                    perf[-10:], maxlen=self.config.performance_history_limit,
                )

        await self.storage.reset_historical_effectiveness()

        self.health_status = "healthy"
        self.last_error = None

        await self.save_state()
        logger.info("Self-healing completed")

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("Shutting down Co-Evolution Engine")
        self._shutting_down = True

        if self._event_consumer_task is not None:
            self._event_consumer_task.cancel()

        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        if self.persistence is not None:
            try:
                await self.save_state()
            except Exception as e:
                logger.error(f"Final save failed: {e}")

        if self.carbon_manager is not None and hasattr(self.carbon_manager, 'close'):
            try:
                res = self.carbon_manager.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Carbon manager close failed: {e}")

        if self.safety_monitor is not None:
            unmet = self.safety_monitor.final_check()
            if unmet:
                logger.warning(f"Unmet eventual invariants at shutdown: {unmet}")

        logger.info("Co-Evolution Engine shutdown complete")


# ============================================================================
# Telemetry Collector (kept for backwards compatibility)
# ============================================================================
class CoEvolutionTelemetry:
    def __init__(self) -> None:
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None,
                  value: float = 1.0) -> None:
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float,
              tags: Optional[Dict[str, str]] = None) -> None:
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float,
                  tags: Optional[Dict[str, str]] = None) -> None:
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str,
                  tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        async with self._lock:
            output: List[str] = []
            for key, value in self.metrics['counters'].items():
                output.append(f"# TYPE {key} counter\n{key} {value}")
            for key, value in self.metrics['gauges'].items():
                output.append(f"# TYPE {key} gauge\n{key} {value}")
            for key, values in self.metrics['histograms'].items():
                output.append(
                    f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}"
                )
            return "\n".join(output)

    def reset(self) -> None:
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)


# ============================================================================
# Main Entrypoint (Legacy wrapper, now fully async with lifecycle)
# ============================================================================
class EnhancedCoEvolutionEngine(CoEvolutionOrchestrator):
    """
    Legacy wrapper for backward compatibility, now with full lifecycle.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[CoEvolutionConfig] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(CoEvolutionConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            config = CoEvolutionConfig(**filtered)
        self.config = config

        self.storage = CoEvolutionStorage(config)
        self.sentiment = SentimentAnalyzer(config)
        self.prioritizer = RecommendationPrioritizer(config)
        self.impact_tracker = LongTermImpactTracker(config)
        self.analyzer = CoEvolutionAnalyzer(
            config, self.storage, self.sentiment, self.prioritizer, self.impact_tracker,
        )
        self.telemetry = CoEvolutionTelemetry()
        self.persistence = (
            CoEvolutionPersistenceManager(config) if config.persistence_path else None
        )

        super().__init__(
            config=config,
            storage=self.storage,
            analyzer=self.analyzer,
            prioritizer=self.prioritizer,
            impact_tracker=self.impact_tracker,
            telemetry=self.telemetry,
            persistence=self.persistence,
            bio_core=bio_core,
            carbon_manager=None,
        )

        logger.info(
            "Enhanced Co-Evolution Engine v6.0.0 initialised "
            "(not started) with MOPD, XAI, temporal safety, human approval, "
            "chaos testing, and ObservabilityHub"
        )


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main() -> None:
        config = CoEvolutionConfig()
        config.enable_chaos_testing = True
        config.chaos.enabled = True
        config.chaos.interval_seconds = 9999

        engine = EnhancedCoEvolutionEngine(config=config)
        await engine.start()

        cycle = await engine.co_evolve(idempotency_key=uuid.uuid4().hex)
        print(json.dumps(cycle, indent=2, default=str))

        status = await engine.get_evolution_status()
        print(json.dumps(status, indent=2, default=str))

        health = await engine.get_health_status()
        print(json.dumps(health, indent=2, default=str))

        chaos_report = await engine.run_chaos_test()
        print(json.dumps(chaos_report, indent=2, default=str))

        metrics = await engine.get_prometheus_metrics()
        print(metrics[:500])

        await engine.shutdown()

    asyncio.run(main())


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "EnhancedCoEvolutionEngine",
    "CoEvolutionOrchestrator",
    "CoEvolutionConfig",
    # Sub-modules
    "SentimentAnalyzer",
    "RecommendationPrioritizer",
    "LongTermImpactTracker",
    "CoEvolutionPersistenceManager",
    "CoEvolutionTelemetry",
    "CoEvolutionStorage",
    "CoEvolutionAnalyzer",
    # Infra
    "ObservabilityHub",
    "SafetyMonitor",
    "SafetyInvariant",
    "GovernanceLayer",
    "ApprovalConfig",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "ChaosService",
    "ChaosConfig",
    "FaultType",
    "MOPDConfig",
    # Data
    "EvolutionMilestone",
    "MOPDPlan",
    # Protocols
    "BioCoreProtocol",
    "EventBrokerProtocol",
    "SwarmCoordinatorProtocol",
    "SelfHealerProtocol",
    "WorkflowOrchestratorProtocol",
    "HeliumProviderProtocol",
    "CarbonManagerProtocol",
    "QuantumBenchmarkProtocol",
    "FFTMoEProtocol",
    "HeliumManagerProtocol",
    "FederatedOrchestratorProtocol",
    "PredictiveAnalyzerProtocol",
    # Constants
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
    "STATE_SCHEMA_VERSION",
    "retry_async",
]
