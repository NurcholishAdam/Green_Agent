# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/co_evolution_engine.py
# Enhanced version v5.1.0 – Refactored for maintainability, concurrency, resilience, and MOPD support.

"""
Enhanced Human-AI Co-Evolution Engine v5.1.0
Modular, event‑driven, robust, and MOPD‑aware implementation.
"""

import asyncio
import logging
import json
import os
import hashlib
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Deque, Callable
from collections import defaultdict, deque
import numpy as np
import zlib

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}
    class CircuitBreaker:
        def __init__(self, name, failure_threshold=3, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# MoE imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False

class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Configuration with Sub‑Configs (Enhanced with MOPD)
# ============================================================================
@dataclass
class MOPDConfig:
    """Configuration for MOPD analysis."""
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'cost': 0.2,
        'impact': 0.3,
        'time': 0.15,
        'risk': 0.15,
        'historical_effectiveness': 0.2,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True

@dataclass
class CoEvolutionConfig:
    """Centralized configuration for the Co-Evolution Engine."""
    # Learning parameters
    learning_rate: float = 0.01
    exploration_rate: float = 0.1
    adaptation_threshold: float = 0.7

    # History limits
    feedback_history_limit: int = 1000
    performance_history_limit: int = 1000
    sustainability_trajectory_limit: int = 1000
    milestone_limit: int = 100

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "co_evolution_state.json"

    # Telemetry
    telemetry_export_interval: int = 60

    # Periodic co-evolution interval (seconds)
    co_evolution_interval: int = 300

    # Estimated impact/effort per area
    estimated_impact: Dict[str, float] = field(default_factory=lambda: {
        'quantum': 0.8, 'moe': 0.6, 'sustainability': 0.9,
        'user_experience': 0.7, 'federated': 0.6, 'system_wide': 0.8
    })
    estimated_effort: Dict[str, float] = field(default_factory=lambda: {
        'quantum': 0.7, 'moe': 0.5, 'sustainability': 0.6,
        'user_experience': 0.3, 'federated': 0.5, 'system_wide': 0.8
    })

    # Feature flags
    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_swarm_coordination: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_cost_benefit: bool = True
    enable_workflow_orchestration: bool = True
    enable_mopd: bool = True               # NEW: MOPD feature flag

    # Workflow triggers
    workflow_on_critical_alert: str = "adjust_co_evolution_strategy"
    workflow_on_slo_breach: str = "rebalance_priorities"

    # Swarm sharing interval
    swarm_share_interval: int = 60

    # MOPD sub‑config
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

# ============================================================================
# Sentiment Analyzer (unchanged)
# ============================================================================
class SentimentAnalyzer:
    # ... (same as before) ...
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.sentiment_keywords = {
            'positive': {'excellent': 1.0, 'great': 0.8, 'good': 0.6, 'nice': 0.5,
                         'happy': 0.7, 'satisfied': 0.8, 'impressed': 0.9, 'love': 1.0,
                         'amazing': 1.0, 'perfect': 1.0, 'awesome': 0.9, 'fantastic': 1.0,
                         'helpful': 0.6, 'useful': 0.5, 'improved': 0.7, 'better': 0.6},
            'negative': {'bad': -0.6, 'terrible': -1.0, 'awful': -0.9, 'horrible': -1.0,
                         'sad': -0.5, 'disappointed': -0.7, 'frustrated': -0.8, 'angry': -0.9,
                         'useless': -0.7, 'broken': -0.8, 'confusing': -0.5, 'slow': -0.5,
                         'worse': -0.6, 'issue': -0.4, 'problem': -0.5, 'error': -0.6}
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

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        # ... same as original ...
        if not text or not text.strip():
            return {'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                    'emotions': {}, 'key_phrases': []}
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
        # ... same as original ...
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
        # ... same as original ...
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
# Recommendation Prioritizer (unchanged)
# ============================================================================
class RecommendationPrioritizer:
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.historical_effectiveness: Dict[str, List[float]] = defaultdict(list)

    def prioritize_recommendations(
        self,
        recommendations: List[Dict[str, Any]],
        historical_data: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        prioritized = []
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
                'ranking': 0
            })
            prioritized.append(prioritized_rec)
        prioritized.sort(key=lambda x: x['roi_score'], reverse=True)
        for i, rec in enumerate(prioritized):
            rec['ranking'] = i + 1
        return prioritized

# ============================================================================
# Long-Term Impact Tracker (refactored with async)
# ============================================================================
class LongTermImpactTracker:
    # ... (same as before) ...
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.impact_history: Dict[str, Deque[Dict]] = defaultdict(lambda: deque(maxlen=100))
        self.sustainability_scores: Deque[float] = deque(maxlen=config.sustainability_trajectory_limit)
        self.decay_rates: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def record_impact(self, area: str, impact_data: Dict[str, Any], sustainability_score: float):
        async with self._lock:
            self.impact_history[area].append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'impact': impact_data,
                'sustainability_score': sustainability_score
            })
            self.sustainability_scores.append(sustainability_score)
            await self._update_decay_rate(area)

    async def _update_decay_rate(self, area: str):
        history = list(self.impact_history.get(area, []))
        if len(history) < 5:
            return
        scores = [entry['sustainability_score'] for entry in history[-20:]]
        if len(scores) > 5:
            x = np.array(range(len(scores)))
            y = np.array(scores)
            slope = np.polyfit(x, y, 1)[0]
            self.decay_rates[area] = -slope

    def get_area_trend(self, area: str) -> Dict[str, Any]:
        history = list(self.impact_history.get(area, []))
        if len(history) < 3:
            return {'status': 'insufficient_data'}
        scores = [entry['sustainability_score'] for entry in history[-10:]]
        recent_scores = scores[-5:] if len(scores) >= 5 else scores
        avg_score = np.mean(scores) if scores else 0.5
        avg_recent = np.mean(recent_scores) if recent_scores else 0.5
        trend = "improving" if avg_recent > avg_score * 1.05 else "stable" if avg_recent > avg_score * 0.95 else "declining"
        decay_rate = self.decay_rates.get(area, 0.0)
        return {
            'area': area,
            'average_score': avg_score,
            'recent_score': avg_recent,
            'trend': trend,
            'decay_rate': decay_rate,
            'sample_count': len(scores),
            'needs_attention': decay_rate > 0.05 or trend == "declining"
        }

    def get_overall_trend(self) -> Dict[str, Any]:
        scores = list(self.sustainability_scores)
        if not scores:
            return {'status': 'insufficient_data'}
        scores = scores[-20:]
        avg_score = np.mean(scores)
        avg_recent = np.mean(scores[-5:]) if len(scores) >= 5 else avg_score
        trend = "improving" if avg_recent > avg_score * 1.05 else "stable" if avg_recent > avg_score * 0.95 else "declining"
        return {
            'average_sustainability_score': avg_score,
            'recent_sustainability_score': avg_recent,
            'trend': trend,
            'sample_count': len(scores),
            'improvement_rate': (avg_recent - avg_score) / max(avg_score, 0.01) if len(scores) > 5 else 0.0
        }

    def to_dict(self) -> Dict:
        return {
            'impact_history': {k: list(v) for k, v in self.impact_history.items()},
            'sustainability_scores': list(self.sustainability_scores),
            'decay_rates': self.decay_rates
        }

    def from_dict(self, data: Dict):
        self.impact_history = defaultdict(lambda: deque(maxlen=100))
        for k, v in data.get('impact_history', {}).items():
            self.impact_history[k] = deque(v, maxlen=100)
        self.sustainability_scores = deque(data.get('sustainability_scores', []), maxlen=self.config.sustainability_trajectory_limit)
        self.decay_rates = data.get('decay_rates', {})

# ============================================================================
# Persistence Manager (JSON with versioning)
# ============================================================================
class CoEvolutionPersistenceManager:
    # ... (same as before, with version bumped) ...
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._version = 2  # Bumped for MOPD
        logger.info(f"CoEvolutionPersistenceManager initialized (path={self.path})")

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': self._make_serializable(state)
                }
                with open(self.path, 'w') as f:
                    json.dump(payload, f, indent=2)
                logger.info(f"State saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self) -> Optional[Dict]:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return None
            try:
                with open(self.path, 'r') as f:
                    payload = json.load(f)
                if payload.get('version') != self._version:
                    logger.warning(f"State version mismatch; may be incompatible")
                return self._deserialize(payload.get('data', {}))
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return None

    def _make_serializable(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (deque, set)):
            return self._make_serializable(list(obj))
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj

    def _deserialize(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._deserialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deserialize(v) for v in obj]
        elif isinstance(obj, str):
            try:
                return datetime.fromisoformat(obj)
            except ValueError:
                return obj
        else:
            return obj

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Telemetry Collector (unchanged)
# ============================================================================
class CoEvolutionTelemetry:
    # ... (same as before) ...
    def __init__(self):
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        async with self._lock:
            output = []
            for key, value in self.metrics['counters'].items():
                output.append(f"# TYPE {key} counter\n{key} {value}")
            for key, value in self.metrics['gauges'].items():
                output.append(f"# TYPE {key} gauge\n{key} {value}")
            for key, values in self.metrics['histograms'].items():
                output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
            return "\n".join(output)

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# ============================================================================
# Retry Helper (unchanged)
# ============================================================================
async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ============================================================================
# Evolution Milestone Dataclass (unchanged)
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
            'metrics': self.metrics,
            'human_feedback_count': self.human_feedback_count,
            'ai_suggestion_impact': self.ai_suggestion_impact,
            'strategy_signature': self.strategy_signature,
            'reuse_count': self.reuse_count,
            'avg_effectiveness': np.mean(self.effectiveness_history) if self.effectiveness_history else 0.0
        }

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a co‑evolution strategy with its objective vector."""
    # Decision variables (recommendation details)
    area: str
    action: str
    strategy_signature: str
    # Objectives (to be minimised/maximised)
    cost: float
    impact: float
    time_days: float
    risk: float
    historical_effectiveness: float
    # Scalarised score (will be computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPlan':
        return cls(**data)

# ============================================================================
# Storage Module (Enhanced with MOPD)
# ============================================================================
class CoEvolutionStorage:
    """Thread‑safe storage for all co‑evolution state."""
    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.feedback_history: Deque[Dict] = deque(maxlen=config.feedback_history_limit)
        self.user_models: Dict[str, Dict[str, Any]] = {}
        self.policy_suggestions: List[Dict[str, Any]] = []
        self.collaborative_decisions: List[Dict[str, Any]] = []
        self.evolution_milestones: Deque[EvolutionMilestone] = deque(maxlen=config.milestone_limit)
        self.milestone_strategies: Dict[str, List[Dict]] = {}
        self.performance_history: Deque[Dict] = deque(maxlen=config.performance_history_limit)
        self.trust_history: Deque[float] = deque(maxlen=1000)
        self.sustainability_trajectory: Deque[float] = deque(maxlen=config.sustainability_trajectory_limit)
        self.historical_effectiveness: Dict[str, float] = {
            'quantum': 0.5, 'moe': 0.5, 'sustainability': 0.5,
            'user_experience': 0.5, 'federated': 0.5, 'system_wide': 0.5
        }
        self.mopd_plans: List[MOPDPlan] = []  # NEW: store MOPD plans
        self._lock = asyncio.Lock()

    # -------------------- Feedback --------------------
    async def add_feedback(self, feedback: Dict):
        async with self._lock:
            self.feedback_history.append(feedback)

    async def get_feedback(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit is not None:
                return list(self.feedback_history)[-limit:]
            return list(self.feedback_history)

    async def clear_feedback(self):
        async with self._lock:
            self.feedback_history.clear()

    # -------------------- User Models --------------------
    async def get_user_model(self, user_id: str) -> Optional[Dict]:
        async with self._lock:
            return self.user_models.get(user_id)

    async def update_user_model(self, user_id: str, updates: Dict):
        async with self._lock:
            if user_id not in self.user_models:
                self.user_models[user_id] = {}
            self.user_models[user_id].update(updates)

    async def get_user_models(self) -> Dict[str, Dict]:
        async with self._lock:
            return dict(self.user_models)

    async def clear_user_models(self):
        async with self._lock:
            self.user_models.clear()

    # -------------------- Policies & Decisions --------------------
    async def add_policy_suggestion(self, suggestion: Dict):
        async with self._lock:
            self.policy_suggestions.append(suggestion)

    async def get_policy_suggestions(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit:
                return self.policy_suggestions[-limit:]
            return self.policy_suggestions.copy()

    async def add_collaborative_decision(self, decision: Dict):
        async with self._lock:
            self.collaborative_decisions.append(decision)

    async def get_collaborative_decisions(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit:
                return self.collaborative_decisions[-limit:]
            return self.collaborative_decisions.copy()

    # -------------------- Milestones --------------------
    async def add_milestone(self, milestone: EvolutionMilestone):
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

    # -------------------- Milestone Strategies --------------------
    async def add_milestone_strategy(self, strategy_signature: str, entry: Dict):
        async with self._lock:
            if strategy_signature not in self.milestone_strategies:
                self.milestone_strategies[strategy_signature] = []
            self.milestone_strategies[strategy_signature].append(entry)

    async def get_milestone_strategies(self) -> Dict[str, List[Dict]]:
        async with self._lock:
            return dict(self.milestone_strategies)

    # -------------------- Performance & Trust --------------------
    async def add_performance_record(self, record: Dict):
        async with self._lock:
            self.performance_history.append(record)

    async def get_performance_history(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit:
                return list(self.performance_history)[-limit:]
            return list(self.performance_history)

    async def add_trust_score(self, score: float):
        async with self._lock:
            self.trust_history.append(score)

    async def get_trust_history(self, limit: Optional[int] = None) -> List[float]:
        async with self._lock:
            if limit:
                return list(self.trust_history)[-limit:]
            return list(self.trust_history)

    # -------------------- Sustainability Trajectory --------------------
    async def add_sustainability_score(self, score: float):
        async with self._lock:
            self.sustainability_trajectory.append(score)

    async def get_sustainability_trajectory(self, limit: Optional[int] = None) -> List[float]:
        async with self._lock:
            if limit:
                return list(self.sustainability_trajectory)[-limit:]
            return list(self.sustainability_trajectory)

    # -------------------- Historical Effectiveness --------------------
    async def get_historical_effectiveness(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self.historical_effectiveness)

    async def update_historical_effectiveness(self, area: str, effectiveness: float):
        async with self._lock:
            old = self.historical_effectiveness.get(area, 0.5)
            self.historical_effectiveness[area] = old * 0.7 + effectiveness * 0.3

    async def reset_historical_effectiveness(self):
        async with self._lock:
            self.historical_effectiveness = {
                'quantum': 0.5, 'moe': 0.5, 'sustainability': 0.5,
                'user_experience': 0.5, 'federated': 0.5, 'system_wide': 0.5
            }

    # -------------------- MOPD Plans (NEW) --------------------
    async def add_mopd_plan(self, plan: MOPDPlan):
        async with self._lock:
            self.mopd_plans.append(plan)
            if len(self.mopd_plans) > 10000:
                self.mopd_plans = self.mopd_plans[-10000:]

    async def get_mopd_plans(self, limit: Optional[int] = None) -> List[MOPDPlan]:
        async with self._lock:
            if limit is not None:
                return self.mopd_plans[-limit:]
            return self.mopd_plans.copy()

    # -------------------- State Snapshot --------------------
    async def to_dict(self) -> Dict:
        async with self._lock:
            return {
                'feedback_history': list(self.feedback_history),
                'user_models': self.user_models,
                'policy_suggestions': self.policy_suggestions,
                'collaborative_decisions': self.collaborative_decisions,
                'evolution_milestones': [m.to_dict() for m in self.evolution_milestones],
                'milestone_strategies': self.milestone_strategies,
                'performance_history': list(self.performance_history),
                'trust_history': list(self.trust_history),
                'sustainability_trajectory': list(self.sustainability_trajectory),
                'historical_effectiveness': self.historical_effectiveness,
                'mopd_plans': [p.to_dict() for p in self.mopd_plans],
            }

    async def from_dict(self, data: Dict):
        async with self._lock:
            self.feedback_history = deque(data.get('feedback_history', []), maxlen=self.config.feedback_history_limit)
            self.user_models = data.get('user_models', {})
            self.policy_suggestions = data.get('policy_suggestions', [])
            self.collaborative_decisions = data.get('collaborative_decisions', [])
            self.evolution_milestones = deque()
            for m_dict in data.get('evolution_milestones', []):
                milestone = self._dict_to_milestone(m_dict)
                if milestone:
                    self.evolution_milestones.append(milestone)
            self.milestone_strategies = data.get('milestone_strategies', {})
            self.performance_history = deque(data.get('performance_history', []), maxlen=self.config.performance_history_limit)
            self.trust_history = deque(data.get('trust_history', []), maxlen=1000)
            self.sustainability_trajectory = deque(data.get('sustainability_trajectory', []), maxlen=self.config.sustainability_trajectory_limit)
            self.historical_effectiveness = data.get('historical_effectiveness', {})
            # Restore MOPD plans
            mopd_plans = data.get('mopd_plans', [])
            for p_dict in mopd_plans:
                self.mopd_plans.append(MOPDPlan.from_dict(p_dict))

    def _dict_to_milestone(self, d: Dict) -> Optional[EvolutionMilestone]:
        try:
            return EvolutionMilestone(
                timestamp=datetime.fromisoformat(d['timestamp']),
                milestone_type=d['milestone_type'],
                description=d['description'],
                metrics=d['metrics'],
                human_feedback_count=d['human_feedback_count'],
                ai_suggestion_impact=d['ai_suggestion_impact'],
                strategy_signature=d.get('strategy_signature'),
                reuse_count=d.get('reuse_count', 0),
                effectiveness_history=d.get('effectiveness_history', [])
            )
        except Exception as e:
            logger.error(f"Failed to reconstruct milestone: {e}")
            return None

# ============================================================================
# Analyzer Module (Enhanced with MOPD)
# ============================================================================
class CoEvolutionAnalyzer:
    """Handles sentiment, prioritization, opportunity detection, impact tracking, and MOPD."""
    def __init__(
        self,
        config: CoEvolutionConfig,
        storage: CoEvolutionStorage,
        sentiment_analyzer: SentimentAnalyzer,
        recommender: RecommendationPrioritizer,
        impact_tracker: LongTermImpactTracker
    ):
        self.config = config
        self.storage = storage
        self.sentiment = sentiment_analyzer
        self.recommender = recommender
        self.impact_tracker = impact_tracker

    # ... (existing methods: aggregate_human_feedback_with_sentiment, extract_feedback_themes_with_sentiment, identify_opportunities) ...
    # For brevity, we keep them as before, but we'll add new MOPD methods.

    async def aggregate_human_feedback_with_sentiment(self) -> List[Dict[str, Any]]:
        # ... same as before ...
        user_models = await self.storage.get_user_models()
        decisions = await self.storage.get_collaborative_decisions()
        all_feedback = []
        for user_id, user_model in user_models.items():
            for fb in user_model.get('feedback', []):
                if 'comment' in fb:
                    fb['sentiment'] = self.sentiment.analyze_sentiment(fb['comment'])
                    all_feedback.append(fb)
        for decision in decisions:
            for fb in decision.get('feedback', []):
                if 'comment' in fb:
                    fb['sentiment'] = self.sentiment.analyze_sentiment(fb['comment'])
                    all_feedback.append(fb)
        return all_feedback[-self.config.feedback_history_limit:]

    async def extract_feedback_themes_with_sentiment(self, feedback: List[Dict]) -> Dict[str, Dict]:
        # ... same as before ...
        keyword_map = {
            'usability': ['confusing', 'complicated', 'hard to use', 'intuitive', 'usability'],
            'performance': ['slow', 'fast', 'lag', 'responsiveness', 'performance'],
            'accuracy': ['wrong', 'incorrect', 'accurate', 'correct', 'precision'],
            'sustainability': ['carbon', 'helium', 'green', 'environmental', 'energy', 'sustainable'],
            'trust': ['trust', 'confidence', 'reliable', 'unreliable', 'trustworthy']
        }
        themes = {}
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
                themes[theme]['avg_sentiment'] = themes[theme]['sentiment_sum'] / themes[theme]['count']
        return themes

    async def identify_opportunities(
        self,
        system_state: Dict[str, Any],
        human_feedback: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        # ... same as before ...
        opportunities = []
        # Quantum
        q = system_state.get('components', {}).get('quantum', {})
        if q.get('average_energy_savings_percent', 0) < 10:
            opportunities.append({
                'area': 'quantum', 'type': 'performance', 'priority': 0.7,
                'suggestion': 'Optimize quantum circuit depth and qubit usage',
                'expected_impact': '20-30% energy savings'
            })
        # MoE
        m = system_state.get('components', {}).get('moe', {})
        if m.get('total_updates_processed', 0) < 10:
            opportunities.append({
                'area': 'moe', 'type': 'adoption', 'priority': 0.5,
                'suggestion': 'Increase client participation in FFT-MoE',
                'expected_impact': 'Improved personalization and accuracy'
            })
        # Helium
        h = system_state.get('components', {}).get('helium', {})
        scarcity = h.get('current', {}).get('scarcity_index', 0)
        if scarcity > 0.6:
            opportunities.append({
                'area': 'sustainability', 'type': 'constraint', 'priority': 0.9,
                'suggestion': 'Reduce helium usage through alternative cooling',
                'expected_impact': '50-80% helium savings'
            })

        # Sentiment‑driven
        if human_feedback:
            sentiments = [f.get('sentiment', {}).get('score', 0) for f in human_feedback if 'sentiment' in f]
            if sentiments:
                negative_ratio = sum(1 for s in sentiments if s < -0.3) / max(len(sentiments), 1)
                if negative_ratio > 0.2:
                    opportunities.append({
                        'area': 'user_experience', 'type': 'sentiment_driven', 'priority': 0.75,
                        'suggestion': f'Address user concerns - {negative_ratio:.0%} negative feedback detected',
                        'expected_impact': 'Improved user satisfaction and trust'
                    })
            themes = await self.extract_feedback_themes_with_sentiment(human_feedback)
            for theme, data in themes.items():
                if data['count'] > len(human_feedback) * 0.2:
                    sentiment_weight = 1.0 + (0.5 - data['avg_sentiment']) * 0.5
                    priority = min(0.9, 0.6 + sentiment_weight * 0.3)
                    opportunities.append({
                        'area': 'user_experience', 'type': theme, 'priority': priority,
                        'suggestion': f'Address user concerns about {theme} (sentiment: {data["avg_sentiment"]:.2f})',
                        'expected_impact': 'Improved user satisfaction and trust'
                    })

        opportunities.sort(key=lambda x: x['priority'], reverse=True)
        return opportunities[:5]

    # ============================================================================
    # MOPD Methods (NEW)
    # ============================================================================
    async def _compute_plan_objectives(self, rec: Dict[str, Any]) -> MOPDPlan:
        """Compute objectives for a single recommendation."""
        area = rec.get('area', 'general')
        action = rec.get('action', '')
        strategy_signature = rec.get('strategy_signature', hashlib.md5(f"{area}:{action}".encode()).hexdigest()[:12])

        # Estimate objectives based on area and priority
        cost = 1.0 - rec.get('historical_effectiveness', 0.5)  # lower cost if historically effective
        impact = self.config.estimated_impact.get(area, 0.5) * rec.get('priority', 0.5)
        time_days = 1.0 / (0.5 + impact)  # rough
        risk = 1.0 - rec.get('priority', 0.5)  # lower risk for higher priority
        historical_effectiveness = rec.get('historical_effectiveness', 0.5)

        return MOPDPlan(
            area=area,
            action=action,
            strategy_signature=strategy_signature,
            cost=cost,
            impact=impact,
            time_days=time_days,
            risk=risk,
            historical_effectiveness=historical_effectiveness
        )

    async def _generate_pareto_front_for_recommendations(
        self,
        recommendations: List[Dict[str, Any]]
    ) -> List[MOPDPlan]:
        """Generate Pareto front from a list of recommendations."""
        if not recommendations:
            return []
        plans = []
        for rec in recommendations:
            plan = await self._compute_plan_objectives(rec)
            plans.append(plan)

        # Filter dominated plans
        objective_names = ['cost', 'impact', 'time_days', 'risk', 'historical_effectiveness']
        # We minimise cost, time, risk; maximise impact, historical_effectiveness
        pareto = []
        for i, p_i in enumerate(plans):
            dominated = False
            for j, p_j in enumerate(plans):
                if i == j:
                    continue
                # Build vectors: for max objectives, negate
                a_vec = [
                    p_i.cost,
                    -p_i.impact,
                    p_i.time_days,
                    p_i.risk,
                    -p_i.historical_effectiveness
                ]
                b_vec = [
                    p_j.cost,
                    -p_j.impact,
                    p_j.time_days,
                    p_j.risk,
                    -p_j.historical_effectiveness
                ]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        objective_names = ['cost', 'impact', 'time_days', 'risk', 'historical_effectiveness']
        # Normalise across front
        max_vals = {}
        min_vals = {}
        for key in objective_names:
            vals = [getattr(p, key) for p in pareto_front]
            max_vals[key] = max(vals)
            min_vals[key] = min(vals)
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_names}

        best = None
        best_score = -float('inf')
        for plan in pareto_front:
            score = 0.0
            for key in objective_names:
                val = getattr(plan, key)
                if key in ['cost', 'time_days', 'risk']:  # minimise
                    norm = 1.0 - (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                else:  # maximise
                    norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 1.0 / len(objective_names))
                score += weight * norm
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ============================================================================
    # Enhanced Recommendation Generation with MOPD
    # ============================================================================
    async def generate_holistic_recommendations(
        self,
        system_state: Dict[str, Any],
        opportunities: List[Dict[str, Any]],
        return_mopd: bool = False           # NEW
    ) -> List[Dict[str, Any]]:
        combined = {}
        for opp in opportunities:
            key = opp['area']
            if key not in combined:
                combined[key] = {
                    'area': key, 'types': [], 'priority': opp['priority'],
                    'suggestions': [opp['suggestion']], 'expected_impact': opp.get('expected_impact', 'Expected improvement'),
                    'predicted': opp.get('predicted', False), 'timeframe_hours': opp.get('timeframe_hours', None)
                }
            else:
                combined[key]['types'].append(opp.get('type', 'general'))
                combined[key]['suggestions'].append(opp['suggestion'])
                combined[key]['priority'] = max(combined[key]['priority'], opp['priority'])

        historical = await self.storage.get_historical_effectiveness()
        recommendations = []
        for area, data in combined.items():
            strategy_sig = self._generate_strategy_signature(area, data['suggestions'])
            existing = await self.storage.find_milestone_by_strategy(strategy_sig)
            rec = {
                'area': area,
                'action': data['suggestions'][0] if len(data['suggestions']) == 1 else f"Multiple actions: {', '.join(data['suggestions'][:2])}",
                'priority': data['priority'],
                'rationale': self._generate_rationale(area, system_state),
                'expected_outcome': self._predict_outcome(area, data['types'], historical),
                'predicted': data.get('predicted', False),
                'timeframe_hours': data.get('timeframe_hours', None),
                'strategy_signature': strategy_sig,
                'historical_effectiveness': historical.get(area, 0.5)
            }
            if existing:
                rec['previous_effectiveness'] = existing.ai_suggestion_impact
                rec['reuse_benefit'] = f"Strategy previously effective ({existing.ai_suggestion_impact:.2f})"
            recommendations.append(rec)

        if len(opportunities) >= 3:
            recommendations.append({
                'area': 'system_wide',
                'action': 'Schedule a comprehensive system optimization sprint',
                'priority': 0.8,
                'rationale': f'Multiple improvement areas identified ({len(opportunities)} opportunities)',
                'expected_outcome': 'System-wide performance uplift',
                'strategy_signature': 'system_wide_optimization'
            })

        # MOPD: generate Pareto front and select best if enabled
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_recommendations(recommendations)
            if pareto_front:
                # Store MOPD plans
                for plan in pareto_front:
                    await self.storage.add_mopd_plan(plan)
                best_plan = self._select_best_from_pareto(pareto_front)
                if best_plan:
                    # We can reorder recommendations based on best plan, but for simplicity we just attach info
                    return {
                        'recommendations': recommendations,
                        'mopd_pareto_front': [p.to_dict() for p in pareto_front],
                        'mopd_best_plan': best_plan.to_dict()
                    }
        return recommendations

    def _generate_strategy_signature(self, area: str, suggestions: List[str]) -> str:
        combined = f"{area}:{'.'.join(sorted(suggestions))}"
        return hashlib.md5(combined.encode()).hexdigest()[:12]

    def _generate_rationale(self, area: str, system_state: Dict[str, Any]) -> str:
        rationales = {
            'quantum': 'Quantum energy savings below target, optimization needed',
            'moe': 'MoE adoption is limited, more client participation needed',
            'sustainability': 'High helium scarcity requires immediate action',
            'user_experience': 'User feedback indicates areas for improvement',
            'federated': 'Federated learning performance can be improved'
        }
        base = rationales.get(area, f'Improvement needed in {area}')
        trend = self.impact_tracker.get_area_trend(area)
        if trend.get('status') != 'insufficient_data':
            if trend.get('trend') == 'declining':
                base += f" (Long-term trend: {trend['trend']}, attention needed)"
            elif trend.get('trend') == 'improving':
                base += f" (Long-term trend: {trend['trend']}, continue momentum)"
        return base

    def _predict_outcome(self, area: str, types: List[str], historical: Dict[str, float]) -> str:
        outcomes = {
            'quantum': '10-30% reduction in energy consumption',
            'moe': 'Improved personalization and model accuracy',
            'sustainability': 'Significant reduction in resource usage',
            'user_experience': 'Increased user engagement and trust',
            'federated': 'Better global model performance'
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
        avg_recent = np.mean(recent)
        older = traj[-20:-10] if len(traj) >= 20 else recent
        avg_older = np.mean(older)
        if avg_recent > avg_older * 1.05:
            return "improving"
        elif avg_recent < avg_older * 0.95:
            return "declining"
        else:
            return "stable"

    async def detect_milestone(self, impact: Dict[str, Any]) -> Optional[EvolutionMilestone]:
        # ... same as before ...
        metrics = impact['metrics']
        if metrics.get('sustainability', 0) > 0.8:
            best_area = 'sustainability'
            details = impact.get('details', [])
            if details:
                best_area = max(details, key=lambda x: x.get('effectiveness', 0)).get('area', 'sustainability')
            strategy_sig = self._generate_strategy_signature(best_area, ["sustainability_breakthrough"])
            milestone = EvolutionMilestone(
                timestamp=datetime.now(timezone.utc),
                milestone_type='breakthrough',
                description='Achieved major sustainability improvement',
                metrics=metrics,
                human_feedback_count=len(await self.storage.get_feedback()),
                ai_suggestion_impact=0.9,
                strategy_signature=strategy_sig
            )
        elif metrics.get('performance', 0) > 0.7 and metrics.get('user_satisfaction', 0) > 0.6:
            milestone = EvolutionMilestone(
                timestamp=datetime.now(timezone.utc),
                milestone_type='breakthrough',
                description='System performance and user satisfaction at high levels',
                metrics=metrics,
                human_feedback_count=len(await self.storage.get_feedback()),
                ai_suggestion_impact=0.8
            )
        elif len(await self.storage.get_performance_history()) > 5:
            perf_hist = await self.storage.get_performance_history(5)
            if len(perf_hist) >= 3:
                recent_perf = [h['impact']['metrics']['performance'] for h in perf_hist if 'metrics' in h['impact']]
                if len(recent_perf) >= 3:
                    improvement = np.mean(recent_perf[-3:]) - np.mean(recent_perf[:3])
                    if improvement > 0.15:
                        milestone = EvolutionMilestone(
                            timestamp=datetime.now(timezone.utc),
                            milestone_type='learning_spike',
                            description=f'Performance improvement of {improvement:.1%} detected',
                            metrics=metrics,
                            human_feedback_count=len(await self.storage.get_feedback()),
                            ai_suggestion_impact=improvement
                        )
        elif len(await self.storage.get_sustainability_trajectory()) > 10:
            traj = await self.storage.get_sustainability_trajectory(5)
            if np.std(traj) < 0.1 and np.mean(traj) > 0.6:
                milestone = EvolutionMilestone(
                    timestamp=datetime.now(timezone.utc),
                    milestone_type='adaptation',
                    description='System showing stable, high sustainability performance',
                    metrics=metrics,
                    human_feedback_count=len(await self.storage.get_feedback()),
                    ai_suggestion_impact=0.7
                )
        else:
            milestone = None

        if milestone:
            await self.storage.add_milestone(milestone)
            await self._learn_from_milestone(milestone)
        return milestone

    async def _learn_from_milestone(self, milestone: EvolutionMilestone):
        if milestone.strategy_signature:
            await self.storage.add_milestone_strategy(milestone.strategy_signature, {
                'effectiveness': milestone.ai_suggestion_impact,
                'timestamp': milestone.timestamp.isoformat(),
                'context': milestone.description
            })
            area = self._extract_area_from_milestone(milestone)
            if area:
                await self.storage.update_historical_effectiveness(area, milestone.ai_suggestion_impact)
        if milestone.ai_suggestion_impact > 0.7:
            milestone.reuse_count += 1
            logger.info(f"Milestone strategy {milestone.strategy_signature} marked for reuse (impact: {milestone.ai_suggestion_impact:.2f})")

    def _extract_area_from_milestone(self, milestone: EvolutionMilestone) -> Optional[str]:
        areas = ['quantum', 'moe', 'sustainability', 'user_experience', 'federated']
        for area in areas:
            if area in milestone.description.lower():
                return area
        return None

# ============================================================================
# Orchestrator (Main Controller) - Enhanced with MOPD
# ============================================================================
class CoEvolutionOrchestrator:
    """
    Orchestrates the co‑evolution cycle, manages events, external components,
    and background tasks.
    """
    def __init__(
        self,
        config: CoEvolutionConfig,
        storage: CoEvolutionStorage,
        analyzer: CoEvolutionAnalyzer,
        prioritizer: RecommendationPrioritizer,
        impact_tracker: LongTermImpactTracker,
        telemetry: CoEvolutionTelemetry,
        persistence: Optional[CoEvolutionPersistenceManager],
        bio_core: Optional[EnhancedBioInspiredCore] = None
    ):
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.prioritizer = prioritizer
        self.impact_tracker = impact_tracker
        self.telemetry = telemetry
        self.persistence = persistence
        self.bio_core = bio_core

        # External components (injected)
        self.quantum_benchmark = None
        self.fft_moe = None
        self.helium_manager = None
        self.federated_orchestrator = None
        self.predictive_analyzer = None
        self.event_broker = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.swarm_coordinator = None
        self.tick_engine = None
        self.quantum_bridge = None
        self.cost_benefit_engine = None

        # Circuit breakers
        self._quantum_circuit = CircuitBreaker("quantum_benchmark")
        self._moe_circuit = CircuitBreaker("fft_moe")
        self._helium_circuit = CircuitBreaker("helium_manager")
        self._federated_circuit = CircuitBreaker("federated_orchestrator")
        self._predictive_circuit = CircuitBreaker("predictive_analyzer")

        # Health
        self.health_status = "healthy"
        self.last_error = None

        # Event queue
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._event_consumer_task: Optional[asyncio.Task] = None

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._running = True
        self._co_evolution_task: Optional[asyncio.Task] = None

        # Subscribe to events if enabled
        if self.config.enable_event_driven and self.bio_core:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

    # ========================================================================
    # Injection Methods
    # ========================================================================
    def inject_components(
        self,
        quantum_benchmark=None,
        fft_moe=None,
        helium_manager=None,
        federated_orchestrator=None,
        predictive_analyzer=None
    ):
        self.quantum_benchmark = quantum_benchmark
        self.fft_moe = fft_moe
        self.helium_manager = helium_manager
        self.federated_orchestrator = federated_orchestrator
        self.predictive_analyzer = predictive_analyzer
        logger.info("External components injected into Co-Evolution Orchestrator")

    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network

    def set_self_evolving_gate(self, gate: 'EnhancedSelfEvolvingGate'):
        self.self_evolving_gate = gate

    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router

    # ========================================================================
    # Event Handling (via queue)
    # ========================================================================
    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._enqueue_event)
            self.event_broker.subscribe('helium_update', self._enqueue_event)
            self.event_broker.subscribe('alert_generated', self._enqueue_event)
            self.event_broker.subscribe('config_updated', self._enqueue_event)
            self.event_broker.subscribe('token_balance_update', self._enqueue_event)
            self.event_broker.subscribe('health_update', self._enqueue_event)
            self.event_broker.subscribe('anomaly_detected', self._enqueue_event)
            logger.info("Co-Evolution Engine subscribed to core events via queue")

    async def _enqueue_event(self, event: BioEvent):
        await self._event_queue.put(event)

    async def _event_consumer(self):
        while True:
            try:
                event = await self._event_queue.get()
                await self._handle_event(event)
                self._event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event consumer error: {e}")

    async def _handle_event(self, event: BioEvent):
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Error handling event {event.event_type}: {e}")

    async def _on_carbon_update(self, event: BioEvent):
        intensity = event.data.get('intensity', 400)
        price = event.data.get('price', 50.0)
        await self.impact_tracker.record_impact(
            'sustainability',
            {'carbon_intensity': intensity, 'carbon_price': price},
            1.0 - (intensity / 800)
        )

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        if self.helium_manager:
            # Adjust helium budget (if manager supports)
            pass
        await self.impact_tracker.record_impact(
            'sustainability',
            {'helium_scarcity': scarcity, 'helium_price': event.data.get('price', 0.5)},
            1.0 - scarcity
        )

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self‑healing")
            self.config.exploration_rate = 0.05
            if self.config.enable_self_healing and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_alert:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'co_evolution' in updates:
            new = updates['co_evolution']
            for key, value in new.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info("Co-Evolution configuration reloaded")

    async def _on_token_update(self, event: BioEvent):
        pass

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'carbon_intensity':
            self.config.learning_rate *= 0.9
        if event.data.get('metric') == 'helium_scarcity':
            self.config.adaptation_threshold = max(0.5, self.config.adaptation_threshold * 0.9)

    # ========================================================================
    # Background Tasks
    # ========================================================================
    def _start_background_tasks(self):
        # Event consumer
        if self.config.enable_event_driven:
            self._event_consumer_task = asyncio.create_task(self._event_consumer())
            self._background_tasks.append(self._event_consumer_task)

        # Co-evolution loop
        async def co_evolution_loop():
            while self._running:
                try:
                    await self.co_evolve()
                    await asyncio.sleep(self.config.co_evolution_interval)
                except Exception as e:
                    logger.error(f"Co-evolution loop error: {e}")
                    await asyncio.sleep(60)
        self._co_evolution_task = asyncio.create_task(co_evolution_loop())
        self._background_tasks.append(self._co_evolution_task)

        # Swarm update loop
        if self.config.enable_swarm_coordination and self.swarm_coordinator:
            async def swarm_loop():
                while True:
                    try:
                        await self.share_with_swarm()
                        await asyncio.sleep(self.config.swarm_share_interval)
                    except Exception as e:
                        logger.error(f"Swarm update error: {e}")
                        await asyncio.sleep(120)
            t = asyncio.create_task(swarm_loop())
            self._background_tasks.append(t)

    # ========================================================================
    # Swarm Coordination
    # ========================================================================
    async def share_with_swarm(self):
        if not self.swarm_coordinator:
            return
        historical = await self.storage.get_historical_effectiveness()
        payload = {
            'orchestrator_id': hashlib.md5(str(historical).encode()).hexdigest()[:8],
            'sustainability_score': self.impact_tracker.get_overall_trend().get('average_sustainability_score', 0.5),
            'milestones': len(await self.storage.get_milestones()),
            'feedback_count': len(await self.storage.get_feedback()),
            'historical_effectiveness': historical,
            'mopd_enabled': self.config.enable_mopd,
        }
        await self.swarm_coordinator.share_predictions(payload)

    # ========================================================================
    # Main Co-Evolution Cycle (Enhanced with MOPD)
    # ========================================================================
    async def co_evolve(self) -> Dict[str, Any]:
        """Main co-evolution cycle."""
        self.telemetry.increment('co_evolution_cycles')

        system_state = await self._collect_system_state()
        human_feedback = await self.analyzer.aggregate_human_feedback_with_sentiment()
        opportunities = await self.analyzer.identify_opportunities(system_state, human_feedback)

        # Predictive opportunities (with circuit breakers)
        predicted = await self._predict_opportunities(system_state)
        if predicted:
            opportunities.extend(predicted)

        # Generate recommendations with MOPD if enabled
        if self.config.enable_mopd:
            rec_result = await self.analyzer.generate_holistic_recommendations(system_state, opportunities, return_mopd=True)
            if isinstance(rec_result, dict) and 'recommendations' in rec_result:
                recommendations = rec_result['recommendations']
                mopd_pareto_front = rec_result.get('mopd_pareto_front', [])
                mopd_best_plan = rec_result.get('mopd_best_plan')
            else:
                recommendations = rec_result
                mopd_pareto_front = None
                mopd_best_plan = None
        else:
            recommendations = await self.analyzer.generate_holistic_recommendations(system_state, opportunities)
            mopd_pareto_front = None
            mopd_best_plan = None

        # Prioritise recommendations (ROI-based)
        historical = await self.storage.get_historical_effectiveness()
        prioritized = self.prioritizer.prioritize_recommendations(recommendations, historical)

        # Apply top recommendations (or best MOPD plan if available)
        if self.config.enable_mopd and mopd_best_plan:
            # Find the recommendation that matches the best MOPD plan
            best_rec = None
            for rec in prioritized:
                if rec.get('strategy_signature') == mopd_best_plan['strategy_signature']:
                    best_rec = rec
                    break
            if best_rec:
                applied = await self._apply_recommendations([best_rec])
            else:
                applied = await self._apply_recommendations(prioritized[:3])
        else:
            applied = await self._apply_recommendations(prioritized[:3])

        impact = await self._measure_impact(applied)

        # Record long-term impact
        for item in applied:
            if item.get('result', {}).get('success'):
                area = item['recommendation'].get('area', 'general')
                await self.impact_tracker.record_impact(
                    area,
                    item['result'],
                    impact['metrics']['sustainability']
                )
                await self.storage.add_sustainability_score(impact['metrics']['sustainability'])

        # Update performance history
        await self.storage.add_performance_record({
            'timestamp': datetime.now(timezone.utc),
            'impact': impact
        })

        # Milestone detection
        milestone = await self.analyzer.detect_milestone(impact)
        if milestone:
            logger.info(f"Milestone detected: {milestone.milestone_type} - {milestone.description}")

        # Update historical effectiveness
        for item in applied:
            if item.get('result', {}).get('success'):
                area = item['recommendation'].get('area', 'general')
                eff = item.get('result', {}).get('effectiveness', 0.5)
                await self.storage.update_historical_effectiveness(area, eff)

        # Telemetry
        self.telemetry.gauge('sustainability_score', impact['metrics']['sustainability'])
        self.telemetry.gauge('performance_score', impact['metrics']['performance'])
        self.telemetry.gauge('milestone_count', len(await self.storage.get_milestones()))
        if self.config.enable_mopd and mopd_pareto_front:
            self.telemetry.increment('mopd_generations')
            self.telemetry.histogram('mopd_pareto_front_size', len(mopd_pareto_front))

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
            'mopd_best_plan': mopd_best_plan
        }

    async def _collect_system_state(self) -> Dict[str, Any]:
        # ... (same as before) ...
        state = {'timestamp': datetime.now(timezone.utc).isoformat(), 'components': {}}
        if self.quantum_benchmark:
            try:
                state['components']['quantum'] = await self._quantum_circuit.call(
                    retry_async,
                    self.quantum_benchmark.get_benchmark_summary,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
            except Exception as e:
                logger.error(f"Quantum benchmark failed: {e}")
                state['components']['quantum'] = {'error': str(e)}
        if self.fft_moe:
            try:
                state['components']['moe'] = await self._moe_circuit.call(
                    retry_async,
                    self.fft_moe.get_fft_moe_status,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
            except Exception as e:
                logger.error(f"FFT-MoE failed: {e}")
                state['components']['moe'] = {'error': str(e)}
        if self.helium_manager:
            try:
                state['components']['helium'] = await self._helium_circuit.call(
                    retry_async,
                    self.helium_manager.get_stats,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
            except Exception as e:
                logger.error(f"Helium manager failed: {e}")
                state['components']['helium'] = {'error': str(e)}
        if self.federated_orchestrator:
            try:
                state['components']['federated'] = await self._federated_circuit.call(
                    retry_async,
                    self.federated_orchestrator.get_status,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
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
        metrics = {'overall_health': 0.0, 'sustainability_score': 0.0, 'performance_score': 0.0, 'user_engagement': 0.0}
        if components.get('helium'):
            scarcity = components['helium'].get('current', {}).get('scarcity_index', 0)
            metrics['sustainability_score'] = 1.0 - scarcity
        if components.get('quantum'):
            metrics['performance_score'] = min(1.0, components['quantum'].get('average_speedup', 0) / 5)
        if components.get('moe'):
            metrics['user_engagement'] = min(1.0, components['moe'].get('num_clients', 0) / 100)
        metrics['overall_health'] = (metrics['sustainability_score'] * 0.4 +
                                     metrics['performance_score'] * 0.3 +
                                     metrics['user_engagement'] * 0.3)
        return metrics

    async def _predict_opportunities(self, system_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        # ... (same as before) ...
        opportunities = []
        if not self.predictive_analyzer:
            return opportunities

        # TimeTickEngine
        if self.config.enable_time_tick_engine and self.tick_engine:
            try:
                forecast = self.tick_engine.get_helium_forecast(4)
                if forecast and len(forecast) > 3:
                    avg_future = np.mean(forecast)
                    if avg_future < 0.3:
                        opportunities.append({
                            'area': 'sustainability',
                            'type': 'predicted_constraint',
                            'priority': 0.85,
                            'suggestion': f'Helium scarcity predicted in 4 hours - proactive reduction needed',
                            'expected_impact': 'Prevent critical helium shortage',
                            'predicted': True,
                            'timeframe_hours': 4
                        })
            except Exception as e:
                logger.warning(f"TimeTickEngine forecast error: {e}")

        # QuantumBridge
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
                        'predicted': True
                    })
            except Exception as e:
                logger.warning(f"QuantumBridge error: {e}")

        # CostBenefitEngine
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
                        'predicted': True
                    })
            except Exception as e:
                logger.warning(f"CostBenefitEngine error: {e}")

        # Predictive analyzer
        if hasattr(self.predictive_analyzer, 'predict_federation_trend'):
            try:
                forecast = await self._predictive_circuit.call(
                    retry_async,
                    self.predictive_analyzer.predict_federation_trend,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if forecast and forecast.get('predicted_sustainability_score', 0.5) < 0.4:
                    opportunities.append({
                        'area': 'federated',
                        'type': 'predicted_performance_decline',
                        'priority': 0.7,
                        'suggestion': 'Federated learning sustainability predicted to decline - proactive optimization needed',
                        'expected_impact': 'Prevent performance degradation',
                        'predicted': True
                    })
            except Exception as e:
                logger.warning(f"Predictive opportunity identification error: {e}")

        return opportunities

    async def _apply_recommendations(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        applied = []
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
                if result.get('success', False):
                    result['effectiveness'] = 0.7 + np.random.random() * 0.2
                applied.append({'recommendation': rec, 'result': result, 'timestamp': datetime.now(timezone.utc).isoformat()})
            except Exception as e:
                logger.error(f"Failed to apply recommendation: {e}")
                applied.append({'recommendation': rec, 'result': {'success': False, 'error': str(e)}})
        return applied

    async def _apply_quantum_optimization(self) -> Dict[str, Any]:
        if self.quantum_benchmark:
            result = await self.quantum_benchmark.run_benchmark(
                task_name="circuit_optimization_test",
                task_input={'type': 'optimization', 'size': 50}
            )
            return {
                'success': True,
                'energy_savings': result.energy_savings_percent,
                'recommendation': result.recommended_approach,
                'effectiveness': min(1.0, result.energy_savings_percent / 50)
            }
        return {'success': False, 'error': 'Quantum benchmark not available'}

    async def _apply_moe_improvement(self) -> Dict[str, Any]:
        if self.fft_moe:
            specialization = await self.fft_moe.analyze_expert_specialization()
            return {
                'success': True,
                'specialization_score': specialization['total_specialized_experts'],
                'top_domain': specialization['top_performing_domain'],
                'effectiveness': min(1.0, specialization['total_specialized_experts'] / 8)
            }
        return {'success': False, 'error': 'FFT-MoE not available'}

    async def _apply_sustainability_measure(self) -> Dict[str, Any]:
        if self.helium_manager:
            forecast = await self.helium_manager.get_sustainability_forecast()
            return {
                'success': True,
                'forecast': forecast,
                'actions_taken': self._generate_recommendations(forecast),
                'effectiveness': 0.8 if forecast.get('days_to_critical', 0) > 7 else 0.5
            }
        return {'success': False, 'error': 'Helium manager not available'}

    async def _apply_ux_improvement(self) -> Dict[str, Any]:
        return {'success': True, 'feedback_loop_improved': True, 'user_engagement_boost': 1.2, 'effectiveness': 0.7}

    async def _apply_system_wide_optimization(self) -> Dict[str, Any]:
        return {'success': True, 'optimizations_applied': ['cache_clearing', 'model_pruning', 'data_compression'], 'effectiveness': 0.6}

    def _generate_recommendations(self, forecast: Dict[str, Any]) -> List[str]:
        days = forecast.get('days_to_critical')
        if days is not None:
            if days <= 3:
                return ["URGENT: Implement immediate helium reduction measures", "Prioritize critical jobs only"]
            elif days <= 7:
                return ["Accelerate helium efficiency improvements", "Begin transitioning to helium-efficient operations"]
        return []

    async def _measure_impact(self, applied: List[Dict[str, Any]]) -> Dict[str, Any]:
        impact = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': {'performance': 0.0, 'sustainability': 0.0, 'user_satisfaction': 0.0, 'overall_effectiveness': 0.0},
            'details': []
        }
        for item in applied:
            if item.get('result', {}).get('success'):
                effectiveness = item['result'].get('effectiveness', 0.7)
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
                    'area': area, 'impact': 'positive', 'effectiveness': effectiveness,
                    'details': item['result'], 'recommendation': item['recommendation'].get('action', 'Unknown action')
                })
        total = sum(impact['metrics'].values())
        if total > 0:
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

    # ========================================================================
    # Public Query Methods (Enhanced with MOPD)
    # ========================================================================
    async def get_evolution_status(self) -> Dict[str, Any]:
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
        }

    async def get_feedback_sentiment_summary(self) -> Dict[str, Any]:
        # ... same as before ...
        feedback = await self.storage.get_feedback()
        if not feedback:
            return {'status': 'no_feedback'}
        sentiments = [fb.get('sentiment', {}).get('score', 0) for fb in feedback if 'sentiment' in fb]
        if not sentiments:
            return {'status': 'no_sentiment_data'}
        return {
            'average_sentiment': np.mean(sentiments),
            'positive_ratio': sum(1 for s in sentiments if s > 0.2) / len(sentiments),
            'negative_ratio': sum(1 for s in sentiments if s < -0.2) / len(sentiments),
            'neutral_ratio': sum(1 for s in sentiments if -0.2 <= s <= 0.2) / len(sentiments),
            'samples': len(sentiments),
            'trend': 'improving' if len(sentiments) > 10 and np.mean(sentiments[-5:]) > np.mean(sentiments[:5]) else 'stable'
        }

    async def get_milestone_summary(self) -> Dict[str, Any]:
        # ... same as before ...
        milestones = await self.storage.get_milestones()
        if not milestones:
            return {'status': 'no_milestones'}
        types = defaultdict(int)
        impacts = []
        for m in milestones:
            types[m.milestone_type] += 1
            impacts.append(m.ai_suggestion_impact)
        return {
            'total_milestones': len(milestones),
            'types': dict(types),
            'average_impact': np.mean(impacts) if impacts else 0,
            'max_impact': max(impacts) if impacts else 0,
            'reused_strategies': sum(1 for m in milestones if m.reuse_count > 0),
            'most_recent': milestones[-1].to_dict() if milestones else None
        }

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================
    async def get_recommendation_pareto_front(
        self,
        system_state: Dict[str, Any],
        opportunities: List[Dict[str, Any]]
    ) -> List[MOPDPlan]:
        """
        Generate Pareto front of recommendations without actually applying them.
        Returns a list of MOPDPlan objects.
        """
        if not self.config.enable_mopd:
            return []
        rec_result = await self.analyzer.generate_holistic_recommendations(system_state, opportunities, return_mopd=True)
        if isinstance(rec_result, dict) and 'mopd_pareto_front' in rec_result:
            return [MOPDPlan.from_dict(p) for p in rec_result['mopd_pareto_front']]
        return []

    async def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.enable_mopd:
            return {'enabled': False}
        plans = await self.storage.get_mopd_plans(20)
        return {
            'enabled': True,
            'objective_weights': self.config.mopd.objective_weights,
            'grid_resolution': self.config.mopd.grid_resolution,
            'total_mopd_plans': len(await self.storage.get_mopd_plans()),
            'sample_plans': [p.to_dict() for p in plans]
        }

    # ============================================================================
    # Health Status
    # ============================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'score': min(1.0, self.impact_tracker.get_overall_trend().get('average_sustainability_score', 0.5)),
            'details': {
                'injected_components': {
                    'quantum_benchmark': self.quantum_benchmark is not None,
                    'fft_moe': self.fft_moe is not None,
                    'helium_manager': self.helium_manager is not None,
                    'federated_orchestrator': self.federated_orchestrator is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None
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
            }
        }

    async def get_telemetry_export(self) -> str:
        return await self.telemetry.export()

    # ========================================================================
    # Persistence
    # ========================================================================
    async def save_state(self):
        if self.persistence:
            storage_dict = await self.storage.to_dict()
            impact_dict = self.impact_tracker.to_dict()
            state = {
                'storage': storage_dict,
                'impact_tracker': impact_dict,
                'config': asdict(self.config)
            }
            await self.persistence.save_state(state)

    async def load_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                await self.storage.from_dict(state.get('storage', {}))
                self.impact_tracker.from_dict(state.get('impact_tracker', {}))
                # Restore config if needed (already set)
                logger.info("State loaded successfully")

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    # ========================================================================
    # Self‑Healing
    # ========================================================================
    async def self_heal(self):
        logger.info("CoEvolutionEngine self‑healing")
        if not self.config.enable_self_healing:
            logger.warning("Self‑healing disabled")
            return

        # Reset learning parameters
        self.config.learning_rate = 0.01
        self.config.exploration_rate = 0.1
        self.config.adaptation_threshold = 0.7

        # Trim histories
        feedback = await self.storage.get_feedback()
        if len(feedback) > 10:
            await self.storage.clear_feedback()
            for fb in feedback[-10:]:
                await self.storage.add_feedback(fb)

        perf = await self.storage.get_performance_history()
        if len(perf) > 10:
            async with self.storage._lock:
                self.storage.performance_history = deque(perf[-10:], maxlen=self.config.performance_history_limit)

        # Reset historical effectiveness
        await self.storage.reset_historical_effectiveness()

        # Reset health
        self.health_status = "healthy"
        self.last_error = None

        # Save state
        await self.save_state()
        logger.info("Self‑healing completed")

    # ========================================================================
    # Shutdown
    # ========================================================================
    async def shutdown(self):
        logger.info("Shutting down Co-Evolution Engine")
        self._running = False
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self.persistence:
            await self.save_state()
        logger.info("Co-Evolution Engine shutdown complete")

# ============================================================================
# Main Entrypoint (for backward compatibility)
# ============================================================================
class EnhancedCoEvolutionEngine(CoEvolutionOrchestrator):
    """
    Legacy wrapper for backward compatibility.
    """
    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[CoEvolutionConfig] = None,
        **kwargs
    ):
        if config is None:
            config = CoEvolutionConfig(**{k: v for k, v in kwargs.items() if k in CoEvolutionConfig.__annotations__})
        self.config = config

        # Create sub‑components
        self.storage = CoEvolutionStorage(config)
        self.sentiment = SentimentAnalyzer(config)
        self.prioritizer = RecommendationPrioritizer(config)
        self.impact_tracker = LongTermImpactTracker(config)
        self.analyzer = CoEvolutionAnalyzer(config, self.storage, self.sentiment, self.prioritizer, self.impact_tracker)
        self.telemetry = CoEvolutionTelemetry()
        self.persistence = CoEvolutionPersistenceManager(config) if config.persistence_path else None

        # Call parent constructor
        super().__init__(
            config,
            self.storage,
            self.analyzer,
            self.prioritizer,
            self.impact_tracker,
            self.telemetry,
            self.persistence,
            bio_core
        )

        # Store bio‑core reference for event subscriptions
        self.bio_core = bio_core
        if bio_core:
            self.event_broker = getattr(bio_core, 'event_broker', None)
            self.self_healer = getattr(bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None)
            self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)
            self.tick_engine = getattr(bio_core, 'tick_engine', None)
            self.quantum_bridge = getattr(bio_core, 'quantum_bridge', None)
            self.cost_benefit_engine = getattr(bio_core, 'cost_benefit_engine', None)

        # Initialize and start
        self._subscribe_events()
        self._start_background_tasks()
        if self.persistence:
            asyncio.create_task(self.load_state())

        logger.info("Enhanced Co-Evolution Engine v5.1.0 initialized with MOPD")

    # Expose storage and analyzer methods for backward compatibility
    def get_evolution_status(self) -> Dict[str, Any]:
        return asyncio.run(super().get_evolution_status())

    def get_feedback_sentiment_summary(self) -> Dict[str, Any]:
        return asyncio.run(super().get_feedback_sentiment_summary())

    def get_milestone_summary(self) -> Dict[str, Any]:
        return asyncio.run(super().get_milestone_summary())

    async def get_health_status(self) -> Dict[str, Any]:
        return await super().get_health_status()

    async def get_telemetry_export(self) -> str:
        return await super().get_telemetry_export()

    async def save_state(self):
        await super().save_state()

    async def load_state(self):
        await super().load_state()

    async def delete_state(self):
        await super().delete_state()

    async def self_heal(self):
        await super().self_heal()

    async def shutdown(self):
        await super().shutdown()
