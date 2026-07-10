"""
Human-AI Co-Evolution Engine for Sustainability v3.1.0
Enhanced with configuration, persistence, telemetry, health checks,
pluggable sentiment models, improved simulation, consensus algorithms,
and better user modeling.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import numpy as np
import re
import hashlib
import os
import pickle
import zlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass
# ============================================================================

@dataclass
class CoEvolutionConfig:
    """Centralized configuration for the Co-Evolution Engine."""
    # Learning parameters
    learning_rate: float = 0.01
    exploration_rate: float = 0.1

    # History limits
    feedback_history_limit: int = 10000
    user_model_limit: int = 1000
    policy_suggestions_limit: int = 1000
    collaborative_decisions_limit: int = 100

    # Consensus parameters
    default_consensus_threshold: float = 0.7

    # Simulation parameters
    simulation_steps: int = 10
    num_scenarios: int = 5
    confidence_level: float = 0.95
    carbon_noise_level: float = 0.02
    helium_noise_level: float = 0.02
    energy_noise_level: float = 0.02

    # Persistence
    persistence_path: str = "co_evolution_state.pkl"

    # Telemetry
    telemetry_export_interval: int = 60

    # Sentiment model (if None, use rule-based)
    sentiment_model: Optional[Any] = None

# ============================================================================
# Protocol for pluggable sentiment models
# ============================================================================

class SentimentModel(Protocol):
    def predict_sentiment(self, text: str) -> Dict[str, Any]: ...

# ============================================================================
# Sentiment Analyzer (Enhanced)
# ============================================================================

class SentimentAnalyzer:
    """
    Sentiment analysis with pluggable ML model support.
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
                'helpful': 0.6, 'useful': 0.5, 'improved': 0.7, 'better': 0.6,
                'efficient': 0.7, 'sustainable': 0.8, 'innovative': 0.9
            },
            'negative': {
                'bad': -0.6, 'terrible': -1.0, 'awful': -0.9, 'horrible': -1.0,
                'sad': -0.5, 'disappointed': -0.7, 'frustrated': -0.8, 'angry': -0.9,
                'useless': -0.7, 'broken': -0.8, 'confusing': -0.5, 'slow': -0.5,
                'worse': -0.6, 'issue': -0.4, 'problem': -0.5, 'error': -0.6,
                'wasteful': -0.7, 'inefficient': -0.6, 'unsustainable': -0.9
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
        """Analyze sentiment of a text string."""
        if not text or not text.strip():
            return {'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                    'emotions': {}, 'key_phrases': []}

        # Use ML model if available
        if self.model:
            try:
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
# Retry Helper
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
# Persistence Manager
# ============================================================================

class CoEvolutionPersistenceManager:
    """Saves and loads the co-evolution engine state."""

    def __init__(self, config: CoEvolutionConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"CoEvolutionPersistenceManager initialized (path={self.path})")

    async def save_state(self, engine: 'HumanAICoEvolutionEngine') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': engine.config,
                    'feedback_history': list(engine.feedback_history),
                    'user_models': {
                        uid: {
                            'preferences': model['preferences'],
                            'history': model['history'][-100:],  # keep recent history
                            'trust_score': model['trust_score'],
                            'feedback_count': model['feedback_count'],
                            'sentiment_score': model['sentiment_score'],
                            'engagement_level': model['engagement_level'],
                            'preference_timeline': model['preference_timeline'][-100:],
                            'last_active': model['last_active']
                        }
                        for uid, model in engine.user_models.items()
                    },
                    'policy_suggestions': list(engine.policy_suggestions),
                    'collaborative_decisions': list(engine.collaborative_decisions),
                    'behavior_history': {
                        uid: list(history)
                        for uid, history in engine.behavior_history.items()
                    },
                    'consensus_builders': engine.consensus_builders
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

    async def load_state(self, engine: 'HumanAICoEvolutionEngine') -> bool:
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
                engine.policy_suggestions = deque(state.get('policy_suggestions', []), maxlen=engine.config.policy_suggestions_limit)
                engine.collaborative_decisions = deque(state.get('collaborative_decisions', []), maxlen=engine.config.collaborative_decisions_limit)
                engine.behavior_history = defaultdict(list)
                for uid, history in state.get('behavior_history', {}).items():
                    engine.behavior_history[uid] = history
                engine.consensus_builders = state.get('consensus_builders', {})
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
# Telemetry Collector
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
# Simulation Result Dataclass (unchanged)
# ============================================================================

@dataclass
class SimulationResult:
    """Result of a policy simulation with probabilistic projections."""
    carbon_trajectory: List[float]
    helium_trajectory: List[float]
    energy_trajectory: List[float]
    sustainability_score: float
    confidence_intervals: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    probabilities: Dict[str, float] = field(default_factory=dict)
    scenario_metadata: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# Enhanced Human-AI Co-Evolution Engine
# ============================================================================

class HumanAICoEvolutionEngine:
    """
    Human-AI co-evolution engine for sustainability v3.1.0.
    """

    def __init__(self, config: Optional[CoEvolutionConfig] = None):
        self.config = config or CoEvolutionConfig()

        self.feedback_history = deque(maxlen=self.config.feedback_history_limit)
        self.user_models: Dict[str, Dict[str, Any]] = {}
        self.policy_suggestions = deque(maxlen=self.config.policy_suggestions_limit)
        self.collaborative_decisions = deque(maxlen=self.config.collaborative_decisions_limit)

        # User behavior history
        self.behavior_history: Dict[str, List[Dict]] = defaultdict(list)
        self.consensus_builders: Dict[str, Dict] = {}

        # Sentiment analyzer (with pluggable model)
        self.sentiment_analyzer = SentimentAnalyzer(self.config)

        # Persistence and telemetry
        self.persistence = CoEvolutionPersistenceManager(self.config)
        self.telemetry = CoEvolutionTelemetry()

        # Learning parameters
        self.learning_rate = self.config.learning_rate
        self.exploration_rate = self.config.exploration_rate

        self._lock = asyncio.Lock()

        # Load persisted state
        asyncio.create_task(self._load_state())

        logger.info("Human-AI Co-Evolution Engine v3.1.0 initialized")

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
            'status': 'healthy',
            'score': min(1.0, self._calculate_health_score()),
            'details': {
                'feedback_count': len(self.feedback_history),
                'user_count': len(self.user_models),
                'suggestions_count': len(self.policy_suggestions),
                'decisions_count': len(self.collaborative_decisions),
                'persistence_enabled': self.persistence is not None,
                'telemetry_active': True,
                'avg_sentiment': self._get_avg_sentiment(),
                'avg_trust': self._get_avg_trust()
            }
        }

    def _calculate_health_score(self) -> float:
        if not self.user_models:
            return 0.5
        trust_scores = [m['trust_score'] for m in self.user_models.values()]
        engagement = [m.get('engagement_level', 0) for m in self.user_models.values()]
        avg_trust = np.mean(trust_scores)
        avg_engagement = np.mean(engagement)
        return (avg_trust * 0.6 + avg_engagement * 0.4)

    def _get_avg_sentiment(self) -> float:
        sentiments = []
        for feedback in self.feedback_history:
            if feedback.get('sentiment'):
                sentiments.append(feedback['sentiment'].get('score', 0))
        return np.mean(sentiments) if sentiments else 0.0

    def _get_avg_trust(self) -> float:
        if not self.user_models:
            return 0.0
        return np.mean([m['trust_score'] for m in self.user_models.values()])

    # ========================================================================
    # Feedback Recording
    # ========================================================================

    async def record_feedback(
        self,
        user_id: str,
        policy_id: str,
        feedback: Dict[str, Any]
    ):
        """Record user feedback on sustainability policies with sentiment analysis."""
        async with self._lock:
            sentiment = None
            if 'comment' in feedback and feedback['comment']:
                sentiment = self.sentiment_analyzer.analyze_sentiment(feedback['comment'])

            feedback_entry = {
                'user_id': user_id,
                'policy_id': policy_id,
                'feedback': feedback,
                'sentiment': sentiment,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.feedback_history.append(feedback_entry)

            # Update user model
            if user_id not in self.user_models:
                self.user_models[user_id] = {
                    'preferences': {},
                    'history': [],
                    'trust_score': 0.5,
                    'feedback_count': 0,
                    'sentiment_score': 0.0,
                    'engagement_level': 0.5,
                    'preference_timeline': [],
                    'last_active': datetime.utcnow().isoformat()
                }

            user_model = self.user_models[user_id]

            # Update preferences
            if 'preferences' in feedback:
                for key, value in feedback['preferences'].items():
                    user_model['preferences'][key] = value
                    user_model['preference_timeline'].append({
                        'timestamp': datetime.utcnow().isoformat(),
                        'key': key,
                        'value': value
                    })

            user_model['history'].append(feedback_entry)
            user_model['feedback_count'] += 1
            user_model['last_active'] = datetime.utcnow().isoformat()

            # Update trust score
            rating = feedback.get('rating', 0)
            sentiment_score = sentiment['score'] if sentiment else 0
            combined_score = (rating / 5.0) * 0.6 + (sentiment_score + 1.0) / 2.0 * 0.4

            if combined_score > 0.5:
                trust_delta = 0.05 * combined_score
                user_model['trust_score'] = min(1.0, user_model['trust_score'] + trust_delta)
            else:
                trust_delta = -0.05 * (1.0 - combined_score)
                user_model['trust_score'] = max(0.0, user_model['trust_score'] + trust_delta)

            # Update sentiment tracking
            if sentiment:
                user_model['sentiment_score'] = (
                    user_model['sentiment_score'] * 0.9 + sentiment['score'] * 0.1
                )

            # Update engagement level
            engagement = user_model['feedback_count'] / 20.0
            user_model['engagement_level'] = min(1.0, engagement)

            # Store behavior history
            self.behavior_history[user_id].append({
                'timestamp': datetime.utcnow().isoformat(),
                'policy_id': policy_id,
                'rating': rating,
                'sentiment': sentiment_score if sentiment else 0,
                'trust_score': user_model['trust_score']
            })

            # Telemetry
            self.telemetry.increment('feedback_received')
            self.telemetry.gauge('trust_score', user_model['trust_score'], {'user_id': user_id})
            if sentiment:
                self.telemetry.gauge('sentiment_score', sentiment_score, {'user_id': user_id})

            logger.info(f"Feedback recorded from {user_id} on {policy_id} (sentiment: {sentiment_score:.2f})")

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
            suggestion = {
                'timestamp': datetime.utcnow().isoformat(),
                'context': context,
                'actions': [],
                'rationale': [],
                'expected_impact': {},
                'explanations': [],
                'confidence': 0.5,
                'personalized': False,
                'alternative_actions': []
            }

            # Base recommendations
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

            # Personalization
            if user_id and user_id in self.user_models:
                user_model = self.user_models[user_id]
                preferences = user_model['preferences']
                trust = user_model['trust_score']

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
                    suggestion['personalized'] = True
                    suggestion['confidence'] = trust
                    suggestion['explanations'].append(
                        f"This suggestion is personalized based on your preferences "
                        f"(trust score: {trust:.1%})"
                    )

            # Apply top recommendations
            for rec in recommendations[:3]:
                suggestion['actions'].append(rec['action'])
                suggestion['rationale'].append(rec['rationale'])
                for key, value in rec['impact'].items():
                    suggestion['expected_impact'][key] = suggestion['expected_impact'].get(key, 0) + value
                if 'explanation' in rec:
                    suggestion['explanations'].append(rec['explanation'])

            # Alternative actions
            if len(recommendations) > 3:
                suggestion['alternative_actions'] = [
                    {'action': r['action'], 'rationale': r['rationale']}
                    for r in recommendations[3:]
                ]

            self.policy_suggestions.append(suggestion)
            self.telemetry.increment('suggestions_generated')

            return suggestion

    # ========================================================================
    # Policy Simulation
    # ========================================================================

    async def simulate_policy_impact(
        self,
        policy: Dict[str, Any],
        simulation_steps: Optional[int] = None,
        num_scenarios: Optional[int] = None,
        confidence_level: Optional[float] = None
    ) -> SimulationResult:
        """Simulate the impact of a policy with probabilistic projections."""
        steps = simulation_steps or self.config.simulation_steps
        scenarios = num_scenarios or self.config.num_scenarios
        conf_level = confidence_level or self.config.confidence_level

        all_carbon = []
        all_helium = []
        all_energy = []
        all_scores = []

        for scenario in range(scenarios):
            np.random.seed(scenario * 12345)

            current_carbon = 400 + np.random.normal(0, 20)
            current_helium = 0.5 + np.random.normal(0, 0.05)
            current_energy = 0.5 + np.random.normal(0, 0.05)

            carbon_traj = []
            helium_traj = []
            energy_traj = []

            actions = policy.get('actions', [])
            carbon_noise = np.random.normal(0, self.config.carbon_noise_level, steps)
            helium_noise = np.random.normal(0, self.config.helium_noise_level, steps)
            energy_noise = np.random.normal(0, self.config.energy_noise_level, steps)

            for step in range(steps):
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
                'energy_noise_level': self.config.energy_noise_level
            }
        )

    # ========================================================================
    # Collaborative Decision Making
    # ========================================================================

    async def collaborative_decision(
        self,
        users: List[str],
        options: List[Dict[str, Any]],
        require_consensus: bool = False,
        consensus_threshold: Optional[float] = None
    ) -> Dict[str, Any]:
        """Facilitate collaborative decision making with consensus building."""
        threshold = consensus_threshold or self.config.default_consensus_threshold

        async with self._lock:
            user_votes = {}
            user_weights = {}

            for user_id in users:
                if user_id in self.user_models:
                    user_model = self.user_models[user_id]
                    preferences = user_model['preferences']
                    trust = user_model['trust_score']
                    engagement = user_model.get('engagement_level', 0.5)

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
                decision = {
                    'selected_option': options[0],
                    'scores': [1.0],
                    'participants': [],
                    'consensus_reached': False,
                    'consensus_score': 0.0,
                    'vote_distribution': {},
                    'timestamp': datetime.utcnow().isoformat()
                }
                self.collaborative_decisions.append(decision)
                return decision

            # Weighted aggregation
            aggregated = [0.0] * len(options)
            total_weight = 0.0
            for user_id, votes in user_votes.items():
                weight = user_weights.get(user_id, 0.5)
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

            decision = {
                'selected_option': options[best_idx],
                'scores': aggregated,
                'participants': list(user_votes.keys()),
                'consensus_reached': consensus_reached,
                'consensus_score': consensus_score,
                'vote_distribution': vote_distribution,
                'consensus_threshold': threshold if require_consensus else None,
                'timestamp': datetime.utcnow().isoformat()
            }

            if not consensus_reached:
                disagreeing_users = []
                for user_id, votes in user_votes.items():
                    if votes and votes.index(max(votes)) != best_idx:
                        disagreeing_users.append(user_id)
                decision['disagreeing_users'] = disagreeing_users
                decision['disagreement_analysis'] = self._analyze_disagreement(
                    user_votes, options, disagreeing_users
                )

            self.collaborative_decisions.append(decision)
            self.telemetry.increment('collaborative_decisions')
            self.telemetry.gauge('consensus_score', consensus_score)
            return decision

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
                prefs = self.user_models[user_id]['preferences']
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
    # User Behavior Modeling
    # ========================================================================

    def predict_user_preferences(self, user_id: str, days: int = 30) -> Dict[str, Any]:
        """Predict future preferences based on historical behavior."""
        if user_id not in self.behavior_history:
            return {'status': 'insufficient_data'}

        history = self.behavior_history[user_id]
        if len(history) < 5:
            return {'status': 'insufficient_data', 'sample_size': len(history)}

        preferences = self.user_models.get(user_id, {}).get('preferences', {})
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
                    slope = np.polyfit(range(len(recent_values[-5:])), recent_values[-5:], 1)[0]
                    predicted_value = weighted_avg + slope * (days / 30)
                else:
                    predicted_value = weighted_avg

                predictions[key] = {
                    'predicted_value': max(0, min(1, predicted_value)),
                    'current_value': value,
                    'confidence': min(0.9, len(recent_values) / 20),
                    'trend': 'increasing' if predicted_value > value * 1.05 else 'decreasing' if predicted_value < value * 0.95 else 'stable'
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
        explanation = {
            'summary': '',
            'detailed': [],
            'why': [],
            'impact': '',
            'uncertainty': '',
            'next_steps': []
        }

        if suggestion['actions']:
            actions_text = ' and '.join(suggestion['actions'][:2])
            if len(suggestion['actions']) > 2:
                actions_text += f' and {len(suggestion["actions"]) - 2} other actions'
            explanation['summary'] = f"Based on current sustainability metrics, we recommend {actions_text}."
        else:
            explanation['summary'] = "Current sustainability metrics are within acceptable ranges. Continue monitoring."

        for action in suggestion['actions']:
            explanation['detailed'].append(action)

        for rationale in suggestion['rationale']:
            explanation['why'].append(rationale)

        if suggestion['expected_impact']:
            impact_parts = []
            for key, value in suggestion['expected_impact'].items():
                impact_parts.append(f"{key.replace('_', ' ')}: {value:.0%}")
            explanation['impact'] = f"Expected impact: {', '.join(impact_parts)}"
        else:
            explanation['impact'] = "No significant impact expected."

        confidence = suggestion.get('confidence', 0.5)
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
            trust = user_model['trust_score']
            explanation['personalized'] = {
                'trust_score': trust,
                'personalization_factors': user_model.get('preferences', {}),
                'note': f"This explanation is personalized based on your historical interactions."
            }

        return explanation

    # ========================================================================
    # Statistics and Reporting
    # ========================================================================

    def get_coevolution_stats(self) -> Dict[str, Any]:
        """Get comprehensive co-evolution statistics."""
        return {
            'total_feedback': len(self.feedback_history),
            'unique_users': len(self.user_models),
            'policy_suggestions': len(self.policy_suggestions),
            'collaborative_decisions': len(self.collaborative_decisions),
            'average_trust': np.mean([
                model['trust_score'] for model in self.user_models.values()
            ]) if self.user_models else 0,
            'average_sentiment': np.mean([
                model['sentiment_score'] for model in self.user_models.values()
            ]) if self.user_models else 0,
            'high_trust_users': sum(1 for model in self.user_models.values()
                                   if model['trust_score'] > 0.7),
            'total_feedback_with_sentiment': sum(1 for f in self.feedback_history
                                                if f.get('sentiment') is not None),
            'engagement_stats': {
                'active_users': sum(1 for model in self.user_models.values()
                                  if model.get('engagement_level', 0) > 0.5),
                'avg_engagement': np.mean([
                    model.get('engagement_level', 0) for model in self.user_models.values()
                ]) if self.user_models else 0
            }
        }

    def get_user_insights(self, user_id: str) -> Dict[str, Any]:
        """Get detailed insights for a specific user."""
        if user_id not in self.user_models:
            return {'status': 'user_not_found'}

        user_model = self.user_models[user_id]
        preference_prediction = self.predict_user_preferences(user_id)

        return {
            'user_id': user_id,
            'preferences': user_model['preferences'],
            'trust_score': user_model['trust_score'],
            'feedback_count': user_model['feedback_count'],
            'sentiment_score': user_model.get('sentiment_score', 0),
            'engagement_level': user_model.get('engagement_level', 0),
            'preference_prediction': preference_prediction,
            'last_active': user_model.get('last_active'),
            'history_summary': {
                'recent_feedback': user_model['history'][-5:] if user_model['history'] else [],
                'total_interactions': len(user_model['history'])
            }
        }

    def get_sentiment_summary(self) -> Dict[str, Any]:
        """Get summary of sentiment analysis across all feedback."""
        if not self.feedback_history:
            return {'status': 'no_feedback'}

        sentiments = []
        for feedback in self.feedback_history:
            if feedback.get('sentiment'):
                sentiments.append(feedback['sentiment'].get('score', 0))

        if not sentiments:
            return {'status': 'no_sentiment_data'}

        policy_sentiments = defaultdict(list)
        for feedback in self.feedback_history:
            if feedback.get('sentiment') and feedback.get('policy_id'):
                policy_sentiments[feedback['policy_id']].append(
                    feedback['sentiment'].get('score', 0)
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
        consensus_reached = sum(1 for d in decisions if d.get('consensus_reached', False))

        avg_consensus_score = np.mean([
            d.get('consensus_score', 0.5) for d in decisions
        ])

        participants_per_decision = [
            len(d.get('participants', [])) for d in decisions
        ]

        return {
            'total_decisions': total_decisions,
            'consensus_rate': consensus_reached / total_decisions if total_decisions else 0,
            'average_consensus_score': avg_consensus_score,
            'average_participants': np.mean(participants_per_decision) if participants_per_decision else 0,
            'max_participants': max(participants_per_decision) if participants_per_decision else 0,
            'recent_decisions': decisions[-5:] if decisions else [],
            'recommendations': self._generate_consensus_recommendations(decisions)
        }

    def _generate_consensus_recommendations(self, decisions: List[Dict]) -> List[str]:
        """Generate recommendations based on consensus analysis."""
        recommendations = []
        if not decisions:
            return recommendations

        consensus_rate = sum(1 for d in decisions if d.get('consensus_reached', False)) / len(decisions)
        if consensus_rate < 0.5:
            recommendations.append("Low consensus rate - consider facilitating more discussion")
            recommendations.append("Identify and address sources of disagreement")

        avg_participants = np.mean([len(d.get('participants', [])) for d in decisions])
        if avg_participants < 3:
            recommendations.append("Low participation - encourage more users to join decisions")

        return recommendations

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down Human-AI Co-Evolution Engine")
        await self.save_state()
        logger.info("Shutdown complete")
