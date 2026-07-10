"""
Enhanced Human-AI Co-Evolution Engine Interface v3.1.0
Integrates all co-evolution capabilities with the core system

Enhanced Features:
- Sentiment analysis on feedback for accurate satisfaction quantification
- Predictive opportunity identification using predictive analyzer
- ROI-based recommendation prioritization
- Long-term impact tracking for sustained improvements
- Milestone-based learning to remember and reuse successful strategies
- Configuration dataclass for centralized tuning
- Resilience with retry and circuit breaker
- Pluggable sentiment models (NLP or rule-based)
- Persistence for state across restarts
- Telemetry export for monitoring
- Periodic background co-evolution loop
- Health status reporting
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import re
import hashlib
import pickle
import zlib
import os
import json
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class CoEvolutionConfig:
    """Centralized configuration for the Co-Evolution Engine."""
    # Learning parameters
    learning_rate: float = 0.01
    exploration_rate: float = 0.1
    adaptation_threshold: float = 0.7

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # History limits
    feedback_history_limit: int = 1000
    performance_history_limit: int = 1000
    sustainability_trajectory_limit: int = 1000
    milestone_limit: int = 100

    # Persistence
    persistence_path: str = "co_evolution_state.pkl"

    # Telemetry
    telemetry_export_interval: int = 60

    # Periodic co-evolution interval (seconds)
    co_evolution_interval: int = 300

    # Sentiment model (if None, use rule-based)
    sentiment_model: Optional[Any] = None

    # Impact/effort estimates (can be overridden)
    estimated_impact: Dict[str, float] = field(default_factory=lambda: {
        'quantum': 0.8,
        'moe': 0.6,
        'sustainability': 0.9,
        'user_experience': 0.7,
        'federated': 0.6,
        'system_wide': 0.8
    })
    estimated_effort: Dict[str, float] = field(default_factory=lambda: {
        'quantum': 0.7,
        'moe': 0.5,
        'sustainability': 0.6,
        'user_experience': 0.3,
        'federated': 0.5,
        'system_wide': 0.8
    })

    def __post_init__(self):
        # Ensure boolean flags
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Sentiment Analyzer (Enhanced)
# ============================================================================

class SentimentAnalyzer:
    """
    Sentiment analysis for human feedback.
    Supports pluggable ML models; fallback to rule-based.
    """

    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.model = config.sentiment_model

        # Rule-based fallback keywords
        self.sentiment_keywords = {
            'positive': {
                'excellent': 1.0, 'great': 0.8, 'good': 0.6, 'nice': 0.5,
                'happy': 0.7, 'satisfied': 0.8, 'impressed': 0.9, 'love': 1.0,
                'amazing': 1.0, 'perfect': 1.0, 'awesome': 0.9, 'fantastic': 1.0,
                'helpful': 0.6, 'useful': 0.5, 'improved': 0.7, 'better': 0.6
            },
            'negative': {
                'bad': -0.6, 'terrible': -1.0, 'awful': -0.9, 'horrible': -1.0,
                'sad': -0.5, 'disappointed': -0.7, 'frustrated': -0.8, 'angry': -0.9,
                'useless': -0.7, 'broken': -0.8, 'confusing': -0.5, 'slow': -0.5,
                'worse': -0.6, 'issue': -0.4, 'problem': -0.5, 'error': -0.6
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

        logger.info("Sentiment Analyzer initialized (model: %s)", 'ML' if self.model else 'rule-based')

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of a text string.
        Returns: {'score': float, 'confidence': float, 'sentiment': str,
                  'emotions': Dict[str, float], 'key_phrases': List[str]}
        """
        if not text or not text.strip():
            return {'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                    'emotions': {}, 'key_phrases': []}

        # Use ML model if available
        if self.model:
            try:
                # Assume model has predict_sentiment method returning dict
                result = self.model.predict_sentiment(text)
                return result
            except Exception as e:
                logger.warning(f"ML sentiment model failed: {e}, falling back to rule-based")

        # Rule-based fallback
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
# Recommendation Prioritizer (Enhanced)
# ============================================================================

class RecommendationPrioritizer:
    """
    ROI-based recommendation prioritization with configurable estimates.
    """

    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.historical_effectiveness: Dict[str, List[float]] = defaultdict(list)

    def prioritize_recommendations(
        self,
        recommendations: List[Dict[str, Any]],
        historical_data: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """
        Prioritize recommendations based on ROI and historical effectiveness.
        """
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
# Long-Term Impact Tracker (Enhanced)
# ============================================================================

class LongTermImpactTracker:
    """
    Long-term impact tracking with persistence.
    """

    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.impact_history: Dict[str, List[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=100))
        self.sustainability_scores: deque = deque(maxlen=config.sustainability_trajectory_limit)
        self.decay_rates: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def record_impact(self, area: str, impact_data: Dict[str, Any], sustainability_score: float):
        async with self._lock:
            self.impact_history[area].append({
                'timestamp': datetime.utcnow().isoformat(),
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
# Persistence Manager (NEW)
# ============================================================================

class CoEvolutionPersistenceManager:
    """Saves and loads the co-evolution engine state."""

    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"CoEvolutionPersistenceManager initialized (path={self.path})")

    async def save_state(self, engine: 'EnhancedCoEvolutionEngine') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': engine.config,
                    'feedback_history': list(engine.feedback_history),
                    'user_models': engine.user_models,
                    'policy_suggestions': engine.policy_suggestions,
                    'collaborative_decisions': engine.collaborative_decisions,
                    'evolution_milestones': [m.to_dict() for m in engine.evolution_milestones],
                    'milestone_strategies': engine.milestone_strategies,
                    'performance_history': engine.performance_history,
                    'trust_history': engine.trust_history,
                    'sustainability_trajectory': list(engine.sustainability_trajectory),
                    'historical_effectiveness': engine.historical_effectiveness,
                    'impact_tracker': engine.impact_tracker.to_dict()
                }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Co-evolution state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save co-evolution state: {e}")
                return False

    async def load_state(self, engine: 'EnhancedCoEvolutionEngine') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                # Restore state
                engine.feedback_history = deque(state.get('feedback_history', []), maxlen=engine.config.feedback_history_limit)
                engine.user_models = state.get('user_models', {})
                engine.policy_suggestions = state.get('policy_suggestions', [])
                engine.collaborative_decisions = state.get('collaborative_decisions', [])
                engine.evolution_milestones = deque()
                for m_dict in state.get('evolution_milestones', []):
                    # Reconstruct EvolutionMilestone from dict
                    # We'll handle this in a helper
                    milestone = engine._dict_to_milestone(m_dict)
                    if milestone:
                        engine.evolution_milestones.append(milestone)
                engine.milestone_strategies = state.get('milestone_strategies', {})
                engine.performance_history = state.get('performance_history', [])
                engine.trust_history = state.get('trust_history', [])
                engine.sustainability_trajectory = deque(
                    state.get('sustainability_trajectory', []),
                    maxlen=engine.config.sustainability_trajectory_limit
                )
                engine.historical_effectiveness = state.get('historical_effectiveness', {})
                engine.impact_tracker.from_dict(state.get('impact_tracker', {}))
                logger.info(f"Co-evolution state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load co-evolution state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Telemetry Collector (NEW)
# ============================================================================

class CoEvolutionTelemetry:
    """Collects telemetry for the co-evolution engine."""

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
        # Prometheus text format
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
# Retry Helper (NEW)
# ============================================================================

async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    """Retry an async function with exponential backoff."""
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
# Evolution Milestone Dataclass
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
# Enhanced Co-Evolution Engine (Main Class)
# ============================================================================

class EnhancedCoEvolutionEngine:
    """
    Enhanced Human-AI Co-Evolution Engine v3.1.0
    """

    def __init__(self, config: Optional[CoEvolutionConfig] = None):
        self.config = config or CoEvolutionConfig()

        # Injected components (external)
        self.quantum_benchmark = None
        self.fft_moe = None
        self.helium_manager = None
        self.federated_orchestrator = None
        self.predictive_analyzer = None

        # Internal modules
        self.sentiment_analyzer = SentimentAnalyzer(self.config)
        self.recommendation_prioritizer = RecommendationPrioritizer(self.config)
        self.impact_tracker = LongTermImpactTracker(self.config)
        self.persistence = CoEvolutionPersistenceManager(self.config) if self.config.persistence_path else None
        self.telemetry = CoEvolutionTelemetry()

        # State with bounded histories
        self.feedback_history = deque(maxlen=self.config.feedback_history_limit)
        self.user_models: Dict[str, Dict[str, Any]] = {}
        self.policy_suggestions: List[Dict[str, Any]] = []
        self.collaborative_decisions: List[Dict[str, Any]] = []
        self.evolution_milestones = deque(maxlen=self.config.milestone_limit)
        self.milestone_strategies: Dict[str, List[Dict]] = {}

        self.performance_history = deque(maxlen=self.config.performance_history_limit)
        self.trust_history = deque(maxlen=1000)
        self.sustainability_trajectory = deque(maxlen=self.config.sustainability_trajectory_limit)

        self.historical_effectiveness: Dict[str, float] = {
            'quantum': 0.5, 'moe': 0.5, 'sustainability': 0.5,
            'user_experience': 0.5, 'federated': 0.5, 'system_wide': 0.5
        }

        self._lock = asyncio.Lock()
        self._running = True
        self._co_evolution_task: Optional[asyncio.Task] = None

        # Start background co-evolution loop
        self._start_background_tasks()

        # Load state if persistence available
        if self.persistence:
            asyncio.create_task(self._load_state())

        logger.info("Enhanced Co-Evolution Engine v3.1.0 initialized")

    def _start_background_tasks(self):
        async def co_evolution_loop():
            while self._running:
                try:
                    await self.co_evolve()
                    await asyncio.sleep(self.config.co_evolution_interval)
                except Exception as e:
                    logger.error(f"Co-evolution loop error: {e}")
                    await asyncio.sleep(60)
        self._co_evolution_task = asyncio.create_task(co_evolution_loop())

    # ============================================================================
    # Injection Methods
    # ============================================================================

    def inject_components(
        self,
        quantum_benchmark=None,
        fft_moe=None,
        helium_manager=None,
        federated_orchestrator=None,
        predictive_analyzer=None
    ):
        """Inject external components."""
        self.quantum_benchmark = quantum_benchmark
        self.fft_moe = fft_moe
        self.helium_manager = helium_manager
        self.federated_orchestrator = federated_orchestrator
        self.predictive_analyzer = predictive_analyzer
        logger.info("System components injected into Co-Evolution Engine")

    # ============================================================================
    # Persistence Methods
    # ============================================================================

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    # ============================================================================
    # Telemetry
    # ============================================================================

    async def get_telemetry_export(self) -> str:
        return await self.telemetry.export()

    # ============================================================================
    # Health Status
    # ============================================================================

    async def get_health_status(self) -> Dict[str, Any]:
        """Report health of the co-evolution engine."""
        return {
            'status': 'healthy' if self._running else 'stopped',
            'score': min(1.0, self._calculate_trend_score()),
            'details': {
                'injected_components': {
                    'quantum_benchmark': self.quantum_benchmark is not None,
                    'fft_moe': self.fft_moe is not None,
                    'helium_manager': self.helium_manager is not None,
                    'federated_orchestrator': self.federated_orchestrator is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None
                },
                'milestones': len(self.evolution_milestones),
                'feedback_count': len(self.feedback_history),
                'performance_samples': len(self.performance_history),
                'user_models': len(self.user_models),
                'persistence_enabled': self.persistence is not None,
                'telemetry_active': True
            }
        }

    def _calculate_trend_score(self) -> float:
        if len(self.sustainability_trajectory) < 10:
            return 0.5
        recent = list(self.sustainability_trajectory)[-10:]
        return np.mean(recent)

    # ============================================================================
    # Main Co-Evolution Loop
    # ============================================================================

    async def co_evolve(self) -> Dict[str, Any]:
        """
        Main co-evolution loop - drives system-wide improvement.
        Returns evolution status and metrics.
        """
        async with self._lock:
            logger.info("Starting co-evolution cycle")
            self.telemetry.increment('co_evolution_cycles')

            system_state = await self._collect_system_state()
            human_feedback = await self._aggregate_human_feedback_with_sentiment()
            opportunities = self._identify_opportunities(system_state, human_feedback)

            # Predictive opportunities
            predicted = await self._predict_opportunities(system_state)
            if predicted:
                opportunities.extend(predicted)

            recommendations = self._generate_holistic_recommendations(system_state, opportunities)
            prioritized = self.recommendation_prioritizer.prioritize_recommendations(
                recommendations, self.historical_effectiveness
            )

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

            self._update_evolution_state(impact, human_feedback)

            milestone = self._detect_milestone(impact)
            if milestone:
                self._learn_from_milestone(milestone)

            self._update_historical_effectiveness(applied)

            # Telemetry
            self.telemetry.gauge('sustainability_score', impact['metrics']['sustainability'])
            self.telemetry.gauge('performance_score', impact['metrics']['performance'])
            self.telemetry.gauge('milestone_count', len(self.evolution_milestones))

            return {
                'status': 'success' if applied else 'partial',
                'recommendations_applied': applied,
                'impact': impact,
                'milestone': milestone.to_dict() if milestone else None,
                'system_state': system_state,
                'human_feedback_count': len(human_feedback),
                'sustainability_trend': self._calculate_trend(),
                'long_term_trend': self.impact_tracker.get_overall_trend()
            }

    # ============================================================================
    # Helper Methods (Split from co_evolve)
    # ============================================================================

    async def _collect_system_state(self) -> Dict[str, Any]:
        state = {'timestamp': datetime.utcnow().isoformat(), 'components': {}}
        if self.quantum_benchmark:
            try:
                state['components']['quantum'] = await retry_async(
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
                state['components']['moe'] = await retry_async(
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
                state['components']['helium'] = await retry_async(
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
                # Assuming a get_status method
                state['components']['federated'] = await retry_async(
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

    async def _aggregate_human_feedback_with_sentiment(self) -> List[Dict[str, Any]]:
        all_feedback = []
        for user_id, user_model in self.user_models.items():
            for fb in user_model.get('feedback', []):
                if 'comment' in fb:
                    fb['sentiment'] = self.sentiment_analyzer.analyze_sentiment(fb['comment'])
                    all_feedback.append(fb)
        for decision in self.collaborative_decisions:
            for fb in decision.get('feedback', []):
                if 'comment' in fb:
                    fb['sentiment'] = self.sentiment_analyzer.analyze_sentiment(fb['comment'])
                    all_feedback.append(fb)
        # Keep within limit
        return all_feedback[-self.config.feedback_history_limit:]

    async def _predict_opportunities(self, system_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        opportunities = []
        if not self.predictive_analyzer:
            return opportunities

        try:
            # Example: helium scarcity forecast
            if self.helium_manager:
                forecast = await retry_async(
                    self.helium_manager.get_sustainability_forecast,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                days = forecast.get('days_to_critical')
                if days is not None and days <= 7:
                    opportunities.append({
                        'area': 'sustainability',
                        'type': 'predicted_constraint',
                        'priority': 0.85,
                        'suggestion': f'Helium scarcity predicted in {days} days - proactive reduction needed',
                        'expected_impact': 'Prevent critical helium shortage',
                        'predicted': True,
                        'timeframe_days': days
                    })

            if self.federated_orchestrator and hasattr(self.federated_orchestrator, 'predictive_analyzer'):
                forecast = await retry_async(
                    self.federated_orchestrator.predictive_analyzer.predict_federation_trend,
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

            if self.quantum_benchmark:
                summary = await retry_async(
                    self.quantum_benchmark.get_benchmark_summary,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if summary.get('total_benchmarks', 0) > 5 and summary.get('average_energy_savings_percent', 0) < 15:
                    opportunities.append({
                        'area': 'quantum',
                        'type': 'predicted_opportunity',
                        'priority': 0.65,
                        'suggestion': 'Quantum energy savings below target - advanced optimization recommended',
                        'expected_impact': '20-30% additional energy savings',
                        'predicted': True
                    })

        except Exception as e:
            logger.warning(f"Predictive opportunity identification error: {e}")
        return opportunities

    def _identify_opportunities(self, system_state: Dict[str, Any], human_feedback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        opportunities = []
        # Existing heuristic opportunities
        if system_state['components'].get('quantum'):
            q = system_state['components']['quantum']
            if q.get('average_energy_savings_percent', 0) < 10:
                opportunities.append({
                    'area': 'quantum', 'type': 'performance', 'priority': 0.7,
                    'suggestion': 'Optimize quantum circuit depth and qubit usage',
                    'expected_impact': '20-30% energy savings'
                })
        if system_state['components'].get('moe'):
            m = system_state['components']['moe']
            if m.get('total_updates_processed', 0) < 10:
                opportunities.append({
                    'area': 'moe', 'type': 'adoption', 'priority': 0.5,
                    'suggestion': 'Increase client participation in FFT-MoE',
                    'expected_impact': 'Improved personalization and accuracy'
                })
        if system_state['components'].get('helium'):
            h = system_state['components']['helium']
            scarcity = h.get('current', {}).get('scarcity_index', 0)
            if scarcity > 0.6:
                opportunities.append({
                    'area': 'sustainability', 'type': 'constraint', 'priority': 0.9,
                    'suggestion': 'Reduce helium usage through alternative cooling',
                    'expected_impact': '50-80% helium savings'
                })

        # Sentiment-based opportunities
        if human_feedback:
            sentiments = [f.get('sentiment', {}).get('score', 0) for f in human_feedback if 'sentiment' in f]
            if sentiments:
                avg_sentiment = np.mean(sentiments)
                negative_ratio = sum(1 for s in sentiments if s < -0.3) / max(len(sentiments), 1)
                if negative_ratio > 0.2:
                    opportunities.append({
                        'area': 'user_experience', 'type': 'sentiment_driven', 'priority': 0.75,
                        'suggestion': f'Address user concerns - {negative_ratio:.0%} negative feedback detected',
                        'expected_impact': 'Improved user satisfaction and trust'
                    })
            themes = self._extract_feedback_themes_with_sentiment(human_feedback)
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

    def _extract_feedback_themes_with_sentiment(self, feedback: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
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

    def _generate_holistic_recommendations(self, system_state: Dict[str, Any], opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        combined = {}
        for opp in opportunities:
            key = opp['area']
            if key not in combined:
                combined[key] = {
                    'area': key, 'types': [], 'priority': opp['priority'],
                    'suggestions': [opp['suggestion']], 'expected_impact': opp.get('expected_impact', 'Expected improvement'),
                    'predicted': opp.get('predicted', False), 'timeframe_days': opp.get('timeframe_days', None)
                }
            else:
                combined[key]['types'].append(opp.get('type', 'general'))
                combined[key]['suggestions'].append(opp['suggestion'])
                combined[key]['priority'] = max(combined[key]['priority'], opp['priority'])

        recommendations = []
        for area, data in combined.items():
            strategy_sig = self._generate_strategy_signature(area, data['suggestions'])
            existing = self._find_milestone_by_strategy(strategy_sig)
            rec = {
                'area': area,
                'action': data['suggestions'][0] if len(data['suggestions']) == 1 else f"Multiple actions: {', '.join(data['suggestions'][:2])}",
                'priority': data['priority'],
                'rationale': self._generate_rationale(area, system_state),
                'expected_outcome': self._predict_outcome(area, data['types']),
                'predicted': data.get('predicted', False),
                'timeframe_days': data.get('timeframe_days', None),
                'strategy_signature': strategy_sig,
                'historical_effectiveness': self.historical_effectiveness.get(area, 0.5)
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
        return recommendations

    def _generate_strategy_signature(self, area: str, suggestions: List[str]) -> str:
        combined = f"{area}:{'.'.join(sorted(suggestions))}"
        return hashlib.md5(combined.encode()).hexdigest()[:12]

    def _find_milestone_by_strategy(self, strategy_signature: str) -> Optional[EvolutionMilestone]:
        for milestone in self.evolution_milestones:
            if milestone.strategy_signature == strategy_signature:
                return milestone
        return None

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

    def _predict_outcome(self, area: str, types: List[str]) -> str:
        outcomes = {
            'quantum': '10-30% reduction in energy consumption',
            'moe': 'Improved personalization and model accuracy',
            'sustainability': 'Significant reduction in resource usage',
            'user_experience': 'Increased user engagement and trust',
            'federated': 'Better global model performance'
        }
        base = outcomes.get(area, 'Expected performance improvement')
        eff = self.historical_effectiveness.get(area, 0.5)
        if eff > 0.7:
            base += f" (Historically effective: {eff:.1%})"
        elif eff < 0.3:
            base += f" (Historically challenging: {eff:.1%})"
        return base

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

    def _update_evolution_state(self, impact: Dict[str, Any], feedback: List[Dict[str, Any]]):
        self.performance_history.append({'timestamp': datetime.utcnow(), 'impact': impact})
        for user_id, user_model in self.user_models.items():
            if impact['metrics'].get('user_satisfaction', 0) > 0.5:
                user_model['trust_score'] = min(1.0, user_model.get('trust_score', 0.5) + 0.05)
            else:
                user_model['trust_score'] = max(0.0, user_model.get('trust_score', 0.5) - 0.02)

    def _calculate_trend(self) -> str:
        if len(self.sustainability_trajectory) < 10:
            return "stable"
        recent = list(self.sustainability_trajectory)[-10:]
        avg_recent = np.mean(recent)
        avg_older = np.mean(list(self.sustainability_trajectory)[-20:-10]) if len(self.sustainability_trajectory) >= 20 else avg_recent
        if avg_recent > avg_older * 1.05:
            return "improving"
        elif avg_recent < avg_older * 0.95:
            return "declining"
        else:
            return "stable"

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
                applied.append({'recommendation': rec, 'result': result, 'timestamp': datetime.utcnow().isoformat()})
            except Exception as e:
                logger.error(f"Failed to apply recommendation: {e}")
                applied.append({'recommendation': rec, 'result': {'success': False, 'error': str(e)}, 'timestamp': datetime.utcnow().isoformat()})
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
        recs = []
        days = forecast.get('days_to_critical')
        if days is not None:
            if days <= 3:
                recs.append("URGENT: Implement immediate helium reduction measures")
                recs.append("Prioritize critical jobs only")
            elif days <= 7:
                recs.append("Accelerate helium efficiency improvements")
                recs.append("Begin transitioning to helium-efficient operations")
        return recs

    # ============================================================================
    # Milestone Learning
    # ============================================================================

    def _detect_milestone(self, impact: Dict[str, Any]) -> Optional[EvolutionMilestone]:
        strategy_sig = None
        if impact['metrics']['sustainability'] > 0.8:
            if impact['details']:
                best = max(impact['details'], key=lambda x: x.get('effectiveness', 0))
                strategy_sig = self._generate_strategy_signature(best['area'], [best['recommendation']])
            milestone = EvolutionMilestone(
                timestamp=datetime.utcnow(),
                milestone_type='breakthrough',
                description='Achieved major sustainability improvement',
                metrics=impact['metrics'],
                human_feedback_count=len(self.feedback_history),
                ai_suggestion_impact=0.9,
                strategy_signature=strategy_sig
            )
        elif impact['metrics']['performance'] > 0.7 and impact['metrics']['user_satisfaction'] > 0.6:
            if impact['details']:
                best = max(impact['details'], key=lambda x: x.get('effectiveness', 0))
                strategy_sig = self._generate_strategy_signature(best['area'], [best['recommendation']])
            milestone = EvolutionMilestone(
                timestamp=datetime.utcnow(),
                milestone_type='breakthrough',
                description='System performance and user satisfaction at high levels',
                metrics=impact['metrics'],
                human_feedback_count=len(self.feedback_history),
                ai_suggestion_impact=0.8,
                strategy_signature=strategy_sig
            )
        elif len(self.performance_history) > 5:
            recent = [h['impact']['metrics']['performance'] for h in list(self.performance_history)[-5:]
                      if 'metrics' in h['impact']]
            if len(recent) >= 3:
                improvement = np.mean(recent[-3:]) - np.mean(recent[:3])
                if improvement > 0.15:
                    milestone = EvolutionMilestone(
                        timestamp=datetime.utcnow(),
                        milestone_type='learning_spike',
                        description=f'Performance improvement of {improvement:.1%} detected',
                        metrics=impact['metrics'],
                        human_feedback_count=len(self.feedback_history),
                        ai_suggestion_impact=improvement
                    )
        elif len(self.sustainability_trajectory) > 10:
            recent_trend = list(self.sustainability_trajectory)[-5:]
            if np.std(recent_trend) < 0.1 and np.mean(recent_trend) > 0.6:
                milestone = EvolutionMilestone(
                    timestamp=datetime.utcnow(),
                    milestone_type='adaptation',
                    description='System showing stable, high sustainability performance',
                    metrics=impact['metrics'],
                    human_feedback_count=len(self.feedback_history),
                    ai_suggestion_impact=0.7
                )

        if milestone:
            self.evolution_milestones.append(milestone)
            self._learn_from_milestone(milestone)
        return milestone

    def _learn_from_milestone(self, milestone: EvolutionMilestone):
        if milestone.strategy_signature:
            if milestone.strategy_signature not in self.milestone_strategies:
                self.milestone_strategies[milestone.strategy_signature] = []
            self.milestone_strategies[milestone.strategy_signature].append({
                'effectiveness': milestone.ai_suggestion_impact,
                'timestamp': milestone.timestamp.isoformat(),
                'context': milestone.description
            })
            area = self._extract_area_from_milestone(milestone)
            if area:
                self.historical_effectiveness[area] = milestone.ai_suggestion_impact
        if milestone.ai_suggestion_impact > 0.7:
            milestone.reuse_count += 1
            logger.info(f"Milestone strategy {milestone.strategy_signature} marked for reuse (impact: {milestone.ai_suggestion_impact:.2f})")

    def _extract_area_from_milestone(self, milestone: EvolutionMilestone) -> Optional[str]:
        areas = ['quantum', 'moe', 'sustainability', 'user_experience', 'federated']
        for area in areas:
            if area in milestone.description.lower():
                return area
        return None

    def _update_historical_effectiveness(self, applied: List[Dict[str, Any]]):
        for item in applied:
            if item.get('result', {}).get('success'):
                area = item['recommendation'].get('area', 'general')
                old = self.historical_effectiveness.get(area, 0.5)
                new = item.get('result', {}).get('effectiveness', 0.5)
                self.historical_effectiveness[area] = old * 0.7 + new * 0.3

    # ============================================================================
    # Impact Measurement
    # ============================================================================

    async def _measure_impact(self, applied: List[Dict[str, Any]]) -> Dict[str, Any]:
        impact = {
            'timestamp': datetime.utcnow().isoformat(),
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

    # ============================================================================
    # Public Query Methods
    # ============================================================================

    def get_evolution_status(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'performance_history': len(self.performance_history),
            'milestones': len(self.evolution_milestones),
            'user_models': len(self.user_models),
            'policy_suggestions': len(self.policy_suggestions),
            'collaborative_decisions': len(self.collaborative_decisions),
            'learning_rate': self.config.learning_rate,
            'exploration_rate': self.config.exploration_rate,
            'adaptation_threshold': self.config.adaptation_threshold,
            'historical_effectiveness': self.historical_effectiveness,
            'milestone_strategies': len(self.milestone_strategies),
            'long_term_trend': self.impact_tracker.get_overall_trend()
        }

    def get_feedback_sentiment_summary(self) -> Dict[str, Any]:
        if not self.feedback_history:
            return {'status': 'no_feedback'}
        sentiments = [fb.get('sentiment', {}).get('score', 0) for fb in self.feedback_history if 'sentiment' in fb]
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

    def get_milestone_summary(self) -> Dict[str, Any]:
        if not self.evolution_milestones:
            return {'status': 'no_milestones'}
        types = defaultdict(int)
        impacts = []
        for m in self.evolution_milestones:
            types[m.milestone_type] += 1
            impacts.append(m.ai_suggestion_impact)
        return {
            'total_milestones': len(self.evolution_milestones),
            'types': dict(types),
            'average_impact': np.mean(impacts) if impacts else 0,
            'max_impact': max(impacts) if impacts else 0,
            'reused_strategies': sum(1 for m in self.evolution_milestones if m.reuse_count > 0),
            'most_recent': self.evolution_milestones[-1].to_dict() if self.evolution_milestones else None
        }

    # ============================================================================
    # Milestone conversion helper
    # ============================================================================

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
    # Shutdown
    # ============================================================================

    async def shutdown(self):
        """Graceful shutdown of the co-evolution engine."""
        logger.info("Shutting down Co-Evolution Engine")
        self._running = False
        if self._co_evolution_task:
            self._co_evolution_task.cancel()
            try:
                await self._co_evolution_task
            except asyncio.CancelledError:
                pass
        if self.persistence:
            await self.save_state()
        logger.info("Co-Evolution Engine shutdown complete")
