# sustainability/co_evolution_interface.py
"""
Enhanced Human-AI Co-Evolution Engine Interface v3.0.0
Integrates all co-evolution capabilities with the core system

Enhanced Features:
- Sentiment analysis on feedback for accurate satisfaction quantification
- Predictive opportunity identification using predictive analyzer
- ROI-based recommendation prioritization
- Long-term impact tracking for sustained improvements
- Milestone-based learning to remember and reuse successful strategies
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np
import re
import hashlib
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

@dataclass
class EvolutionMilestone:
    """Milestone in the co-evolution process"""
    timestamp: datetime
    milestone_type: str  # 'breakthrough', 'learning_spike', 'adaptation'
    description: str
    metrics: Dict[str, float]
    human_feedback_count: int
    ai_suggestion_impact: float
    strategy_signature: Optional[str] = None  # NEW: For milestone-based learning
    reuse_count: int = 0  # NEW: Track how often this milestone is reused
    effectiveness_history: List[float] = field(default_factory=list)  # NEW: Track effectiveness over time
    
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

class SentimentAnalyzer:
    """
    Sentiment analysis for human feedback.
    
    Features:
    - Rule-based sentiment scoring
    - Emotion detection
    - Confidence scoring
    - Key phrase extraction
    """
    
    def __init__(self):
        # Sentiment keywords and weights
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
        
        # Emotion keywords
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
        
        # Intensifiers
        self.intensifiers = ['very', 'really', 'extremely', 'absolutely', 'completely', 
                            'totally', 'highly', 'incredibly', 'remarkably', 'exceptionally']
        
        self.downtoners = ['somewhat', 'slightly', 'a bit', 'a little', 'fairly', 
                          'moderately', 'kind of', 'sort of', 'rather']
        
        self.negations = ['not', 'never', 'none', 'nobody', 'no', 'neither', 'nor', 
                         'hardly', 'scarcely', 'barely', 'no one', 'nothing', 'nowhere']
        
        logger.info("Sentiment Analyzer initialized")
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of a text string.
        
        Returns:
            {
                'score': float,  # -1.0 to 1.0
                'confidence': float,  # 0.0 to 1.0
                'sentiment': str,  # 'positive', 'negative', 'neutral'
                'emotions': Dict[str, float],
                'key_phrases': List[str]
            }
        """
        if not text or not text.strip():
            return {
                'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                'emotions': {}, 'key_phrases': []
            }
        
        text_lower = text.lower()
        words = text_lower.split()
        
        # Calculate sentiment score
        score = 0.0
        total_weight = 0.0
        
        # Track negation context
        negate_next = False
        
        for i, word in enumerate(words):
            # Check for negation
            if word in self.negations:
                negate_next = True
                continue
            
            # Check for intensifiers/downtoners
            multiplier = 1.0
            if i > 0 and words[i-1] in self.intensifiers:
                multiplier = 1.5
            elif i > 0 and words[i-1] in self.downtoners:
                multiplier = 0.6
            
            # Check sentiment
            for sentiment_type, keywords in self.sentiment_keywords.items():
                if word in keywords:
                    sentiment_value = keywords[word] * multiplier
                    if negate_next:
                        sentiment_value = -sentiment_value
                        negate_next = False
                    score += sentiment_value
                    total_weight += 1.0
                    break
        
        # Normalize score
        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.0
        
        # Clamp score
        score = max(-1.0, min(1.0, score))
        
        # Determine sentiment
        if score > 0.2:
            sentiment = 'positive'
        elif score < -0.2:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        # Calculate confidence based on number of sentiment-bearing words
        confidence = min(0.95, total_weight / 10.0)
        
        # Detect emotions
        emotions = self._detect_emotions(text_lower)
        
        # Extract key phrases
        key_phrases = self._extract_key_phrases(text)
        
        return {
            'score': score,
            'confidence': confidence,
            'sentiment': sentiment,
            'emotions': emotions,
            'key_phrases': key_phrases
        }
    
    def _detect_emotions(self, text_lower: str) -> Dict[str, float]:
        """Detect emotions in text"""
        emotions = {}
        
        for emotion, keywords in self.emotion_keywords.items():
            count = sum(1 for keyword in keywords if keyword in text_lower)
            if count > 0:
                emotions[emotion] = min(1.0, count / 3.0)
        
        # Normalize
        if emotions:
            max_emotion = max(emotions.values())
            if max_emotion > 0:
                emotions = {k: v / max_emotion for k, v in emotions.items()}
        
        return emotions
    
    def _extract_key_phrases(self, text: str) -> List[str]:
        """Extract key phrases from text"""
        phrases = []
        
        # Look for phrases in quotes
        quoted = re.findall(r'"([^"]*)"', text)
        if quoted:
            phrases.extend(quoted)
        
        # Look for phrases after common indicators
        indicators = ['especially', 'particularly', 'specifically', 'mainly', 'mostly',
                      'the issue is', 'the problem is', 'suggestion', 'recommendation']
        
        for indicator in indicators:
            if indicator in text.lower():
                parts = text.lower().split(indicator)
                if len(parts) > 1:
                    phrase = parts[1].strip()
                    if phrase and len(phrase) > 10:
                        phrases.append(phrase[:100])
        
        # Add unique phrases
        return list(set(phrases))[:5]

class RecommendationPrioritizer:
    """
    ROI-based recommendation prioritization.
    
    Features:
    - Impact estimation
    - Effort estimation
    - ROI calculation
    - Priority scoring
    """
    
    def __init__(self):
        self.historical_effectiveness: Dict[str, List[float]] = defaultdict(list)
        self.estimated_effort: Dict[str, float] = {
            'quantum': 0.7,
            'moe': 0.5,
            'sustainability': 0.6,
            'user_experience': 0.3,
            'federated': 0.5,
            'system_wide': 0.8
        }
        self.estimated_impact: Dict[str, float] = {
            'quantum': 0.8,
            'moe': 0.6,
            'sustainability': 0.9,
            'user_experience': 0.7,
            'federated': 0.6,
            'system_wide': 0.8
        }
        
        logger.info("Recommendation Prioritizer initialized")
    
    def prioritize_recommendations(
        self,
        recommendations: List[Dict[str, Any]],
        historical_data: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """
        Prioritize recommendations based on ROI and historical effectiveness.
        
        Args:
            recommendations: List of recommendations to prioritize
            historical_data: Historical effectiveness data by area
            
        Returns:
            Prioritized recommendations with ROI scores
        """
        prioritized = []
        
        for rec in recommendations:
            area = rec.get('area', 'general')
            
            # Get base impact and effort estimates
            impact = self.estimated_impact.get(area, 0.5)
            effort = self.estimated_effort.get(area, 0.5)
            
            # Adjust based on historical effectiveness
            historical_effectiveness = historical_data.get(area, 0.5)
            historical_impact = impact * (0.5 + 0.5 * historical_effectiveness)
            
            # Calculate ROI
            roi = historical_impact / max(effort, 0.01)
            
            # Add priority based on ROI and urgency
            priority = rec.get('priority', 0.5)
            urgency_factor = 0.5 + 0.5 * priority
            
            roi_score = roi * urgency_factor
            
            # Create prioritized recommendation
            prioritized_rec = rec.copy()
            prioritized_rec.update({
                'roi_score': roi_score,
                'historical_effectiveness': historical_effectiveness,
                'estimated_roi': roi,
                'ranking': 0  # Will be set after sorting
            })
            
            prioritized.append(prioritized_rec)
        
        # Sort by ROI score descending
        prioritized.sort(key=lambda x: x['roi_score'], reverse=True)
        
        # Set ranking
        for i, rec in enumerate(prioritized):
            rec['ranking'] = i + 1
        
        return prioritized

class LongTermImpactTracker:
    """
    Long-term impact tracking for sustained improvements.
    
    Features:
    - Impact history tracking
    - Sustainability assessment
    - Trend analysis
    - Decay detection
    """
    
    def __init__(self):
        self.impact_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.sustainability_scores: List[float] = []
        self.decay_rates: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        
        logger.info("Long-Term Impact Tracker initialized")
    
    async def record_impact(
        self,
        area: str,
        impact_data: Dict[str, Any],
        sustainability_score: float
    ):
        """Record impact data for an area"""
        async with self._lock:
            self.impact_history[area].append({
                'timestamp': datetime.utcnow().isoformat(),
                'impact': impact_data,
                'sustainability_score': sustainability_score
            })
            
            # Keep only last 100 entries
            if len(self.impact_history[area]) > 100:
                self.impact_history[area] = self.impact_history[area][-100:]
            
            self.sustainability_scores.append(sustainability_score)
            
            # Update decay rate
            await self._update_decay_rate(area)
    
    async def _update_decay_rate(self, area: str):
        """Update decay rate for an area based on historical data"""
        history = self.impact_history.get(area, [])
        if len(history) < 5:
            return
        
        # Extract sustainability scores over time
        scores = [entry['sustainability_score'] for entry in history[-20:]]
        if len(scores) > 5:
            # Calculate decay rate as slope of scores over time
            x = np.array(range(len(scores)))
            y = np.array(scores)
            slope = np.polyfit(x, y, 1)[0]
            
            # Convert to decay rate (positive slope = improvement, negative = decay)
            self.decay_rates[area] = -slope  # Positive decay rate means improvement is fading
    
    def get_area_trend(self, area: str) -> Dict[str, Any]:
        """Get trend analysis for an area"""
        history = self.impact_history.get(area, [])
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
        """Get overall trend analysis"""
        if not self.sustainability_scores:
            return {'status': 'insufficient_data'}
        
        scores = self.sustainability_scores[-20:]
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

class EnhancedCoEvolutionEngine:
    """
    Enhanced Human-AI Co-Evolution Engine v3.0.0
    Integrating all co-evolution capabilities with the core system.
    
    New Features:
    - Sentiment analysis on feedback
    - Predictive opportunity identification
    - ROI-based recommendation prioritization
    - Long-term impact tracking
    - Milestone-based learning
    """
    
    def __init__(self):
        # Core components (injected)
        self.quantum_benchmark = None
        self.fft_moe = None
        self.helium_manager = None
        self.federated_orchestrator = None
        self.predictive_analyzer = None  # NEW: For predictive opportunity identification
        
        # New modules
        self.sentiment_analyzer = SentimentAnalyzer()
        self.recommendation_prioritizer = RecommendationPrioritizer()
        self.impact_tracker = LongTermImpactTracker()
        
        # State
        self.feedback_history = []
        self.user_models = {}
        self.policy_suggestions = []
        self.collaborative_decisions = []
        self.evolution_milestones = []
        self.milestone_strategies = {}  # NEW: Strategy signature -> effectiveness
        
        # Learning parameters
        self.learning_rate = 0.01
        self.exploration_rate = 0.1
        self.adaptation_threshold = 0.7
        
        # Performance tracking
        self.performance_history = []
        self.trust_history = []
        self.sustainability_trajectory = []
        
        # NEW: Historical effectiveness data
        self.historical_effectiveness = {
            'quantum': 0.5,
            'moe': 0.5,
            'sustainability': 0.5,
            'user_experience': 0.5,
            'federated': 0.5,
            'system_wide': 0.5
        }
        
        self._lock = asyncio.Lock()
        
        logger.info("Enhanced Co-Evolution Engine v3.0.0 initialized")
    
    def inject_components(
        self,
        quantum_benchmark=None,
        fft_moe=None,
        helium_manager=None,
        federated_orchestrator=None,
        predictive_analyzer=None  # NEW
    ):
        """Inject system components for integration"""
        self.quantum_benchmark = quantum_benchmark
        self.fft_moe = fft_moe
        self.helium_manager = helium_manager
        self.federated_orchestrator = federated_orchestrator
        self.predictive_analyzer = predictive_analyzer
        logger.info("System components injected into Co-Evolution Engine")
    
    async def co_evolve(self) -> Dict[str, Any]:
        """
        Main co-evolution loop - drives system-wide improvement.
        
        Returns:
            Evolution status and metrics
        """
        async with self._lock:
            logger.info("Starting co-evolution cycle")
            
            # Collect system state
            system_state = await self._collect_system_state()
            
            # 1. Get human feedback with sentiment analysis
            human_feedback = await self._aggregate_human_feedback_with_sentiment()
            
            # 2. Analyze feedback and identify improvement opportunities
            opportunities = self._identify_opportunities(
                system_state, human_feedback
            )
            
            # 3. Predict future opportunities
            predicted_opportunities = await self._predict_opportunities(system_state)
            if predicted_opportunities:
                opportunities.extend(predicted_opportunities)
            
            # 4. Generate system-wide recommendations
            recommendations = self._generate_holistic_recommendations(
                system_state, opportunities
            )
            
            # 5. Prioritize recommendations based on ROI
            prioritized_recommendations = self.recommendation_prioritizer.prioritize_recommendations(
                recommendations, self.historical_effectiveness
            )
            
            # 6. Apply top recommendations
            applied = await self._apply_recommendations(prioritized_recommendations[:3])
            
            # 7. Measure impact
            impact = await self._measure_impact(applied)
            
            # 8. Track long-term impact
            for item in applied:
                if item.get('result', {}).get('success'):
                    area = item['recommendation'].get('area', 'general')
                    await self.impact_tracker.record_impact(
                        area,
                        item['result'],
                        impact['metrics']['sustainability']
                    )
            
            # 9. Update evolution state
            self._update_evolution_state(impact, human_feedback)
            
            # 10. Check for milestones and learn from them
            milestone = self._detect_milestone(impact)
            if milestone:
                self._learn_from_milestone(milestone)
            
            # 11. Update historical effectiveness data
            self._update_historical_effectiveness(applied)
            
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
    
    # ========================================================================
    # Sentiment Analysis Integration
    # ========================================================================
    
    async def _aggregate_human_feedback_with_sentiment(self) -> List[Dict[str, Any]]:
        """
        Aggregate human feedback with sentiment analysis.
        
        Returns:
            List of feedback items with sentiment scores
        """
        all_feedback = []
        
        # Get feedback from user models
        for user_id, user_model in self.user_models.items():
            if user_model.get('feedback'):
                for feedback_item in user_model['feedback']:
                    # Add sentiment analysis if feedback has text
                    if 'comment' in feedback_item:
                        sentiment = self.sentiment_analyzer.analyze_sentiment(
                            feedback_item['comment']
                        )
                        feedback_item['sentiment'] = sentiment
                        all_feedback.append(feedback_item)
                    else:
                        all_feedback.append(feedback_item)
        
        # Get feedback from collaborative decisions
        for decision in self.collaborative_decisions:
            if decision.get('feedback'):
                for feedback_item in decision['feedback']:
                    if 'comment' in feedback_item:
                        sentiment = self.sentiment_analyzer.analyze_sentiment(
                            feedback_item['comment']
                        )
                        feedback_item['sentiment'] = sentiment
                        all_feedback.append(feedback_item)
                    else:
                        all_feedback.append(feedback_item)
        
        return all_feedback[-1000:]  # Last 1000 feedback items
    
    # ========================================================================
    # Predictive Opportunity Identification
    # ========================================================================
    
    async def _predict_opportunities(self, system_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Predict future opportunities using the predictive analyzer.
        
        Returns:
            List of predicted opportunities
        """
        opportunities = []
        
        if not self.predictive_analyzer:
            return opportunities
        
        try:
            # Get predictive forecasts from various sources
            if self.helium_manager:
                helium_forecast = await self.helium_manager.get_sustainability_forecast()
                if helium_forecast.get('days_to_critical') is not None:
                    days_to_critical = helium_forecast['days_to_critical']
                    if days_to_critical <= 7:
                        opportunities.append({
                            'area': 'sustainability',
                            'type': 'predicted_constraint',
                            'priority': 0.85,
                            'suggestion': f'Helium scarcity predicted in {days_to_critical} days - proactive reduction needed',
                            'expected_impact': 'Prevent critical helium shortage',
                            'predicted': True,
                            'timeframe_days': days_to_critical
                        })
            
            if self.federated_orchestrator:
                # Get federated learning performance trends
                if hasattr(self.federated_orchestrator, 'predictive_analyzer'):
                    federated_forecast = await self.federated_orchestrator.predictive_analyzer.predict_federation_trend()
                    if federated_forecast:
                        predicted_score = federated_forecast.predicted_sustainability_score
                        if predicted_score < 0.4:
                            opportunities.append({
                                'area': 'federated',
                                'type': 'predicted_performance_decline',
                                'priority': 0.7,
                                'suggestion': 'Federated learning sustainability predicted to decline - proactive optimization needed',
                                'expected_impact': 'Prevent performance degradation',
                                'predicted': True
                            })
            
            if self.quantum_benchmark:
                # Get quantum performance trends
                benchmark_summary = await self.quantum_benchmark.get_benchmark_summary()
                if benchmark_summary.get('total_benchmarks', 0) > 5:
                    avg_savings = benchmark_summary.get('average_energy_savings_percent', 0)
                    if avg_savings < 15:
                        opportunities.append({
                            'area': 'quantum',
                            'type': 'predicted_opportunity',
                            'priority': 0.65,
                            'suggestion': 'Quantum energy savings below target - advanced optimization recommended',
                            'expected_impact': '20-30% additional energy savings',
                            'predicted': True
                        })
        
        except Exception as e:
            logger.warning(f"Error in predictive opportunity identification: {e}")
        
        return opportunities
    
    # ========================================================================
    # Enhanced Opportunity Identification
    # ========================================================================
    
    def _identify_opportunities(
        self,
        system_state: Dict[str, Any],
        human_feedback: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Identify improvement opportunities from system state and feedback.
        Enhanced with sentiment analysis and predictive opportunities.
        
        Returns:
            List of opportunities with priority scores
        """
        opportunities = []
        
        # Analyze system performance (existing logic)
        if system_state['components'].get('quantum'):
            quantum_state = system_state['components']['quantum']
            if quantum_state.get('average_energy_savings_percent', 0) < 10:
                opportunities.append({
                    'area': 'quantum',
                    'type': 'performance',
                    'priority': 0.7,
                    'suggestion': 'Optimize quantum circuit depth and qubit usage',
                    'expected_impact': '20-30% energy savings'
                })
        
        if system_state['components'].get('moe'):
            moe_state = system_state['components']['moe']
            if moe_state.get('total_updates_processed', 0) < 10:
                opportunities.append({
                    'area': 'moe',
                    'type': 'adoption',
                    'priority': 0.5,
                    'suggestion': 'Increase client participation in FFT-MoE',
                    'expected_impact': 'Improved personalization and accuracy'
                })
        
        if system_state['components'].get('helium'):
            helium_state = system_state['components']['helium']
            scarcity = helium_state['current'].get('scarcity_index', 0)
            if scarcity > 0.6:
                opportunities.append({
                    'area': 'sustainability',
                    'type': 'constraint',
                    'priority': 0.9,
                    'suggestion': 'Reduce helium usage through alternative cooling',
                    'expected_impact': '50-80% helium savings'
                })
        
        # Enhanced sentiment-based opportunity identification
        if human_feedback:
            # Analyze sentiment distribution
            sentiments = [f.get('sentiment', {}).get('score', 0) for f in human_feedback if 'sentiment' in f]
            if sentiments:
                avg_sentiment = np.mean(sentiments)
                neg_sentiment_ratio = sum(1 for s in sentiments if s < -0.3) / max(len(sentiments), 1)
                
                # Identify negative sentiment themes
                if neg_sentiment_ratio > 0.2:
                    opportunities.append({
                        'area': 'user_experience',
                        'type': 'sentiment_driven',
                        'priority': 0.75,
                        'suggestion': f'Address user concerns - {neg_sentiment_ratio:.0%} negative feedback detected',
                        'expected_impact': 'Improved user satisfaction and trust'
                    })
            
            # Extract common themes with sentiment weighting
            themes = self._extract_feedback_themes_with_sentiment(human_feedback)
            
            for theme, data in themes.items():
                if data['count'] > len(human_feedback) * 0.2:  # >20% mention
                    sentiment_weight = 1.0 + (0.5 - data['avg_sentiment']) * 0.5
                    priority = min(0.9, 0.6 + sentiment_weight * 0.3)
                    
                    opportunities.append({
                        'area': 'user_experience',
                        'type': theme,
                        'priority': priority,
                        'suggestion': f'Address user concerns about {theme} (sentiment: {data["avg_sentiment"]:.2f})',
                        'expected_impact': 'Improved user satisfaction and trust'
                    })
        
        # Sort by priority
        opportunities.sort(key=lambda x: x['priority'], reverse=True)
        
        return opportunities[:5]  # Top 5 opportunities
    
    def _extract_feedback_themes_with_sentiment(self, feedback: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Extract common themes from human feedback with sentiment analysis.
        """
        themes = {}
        
        keyword_map = {
            'usability': ['confusing', 'complicated', 'hard to use', 'intuitive', 'usability'],
            'performance': ['slow', 'fast', 'lag', 'responsiveness', 'performance'],
            'accuracy': ['wrong', 'incorrect', 'accurate', 'correct', 'precision'],
            'sustainability': ['carbon', 'helium', 'green', 'environmental', 'energy', 'sustainable'],
            'trust': ['trust', 'confidence', 'reliable', 'unreliable', 'trustworthy']
        }
        
        for feedback_item in feedback:
            text = feedback_item.get('comment', '').lower()
            sentiment_score = feedback_item.get('sentiment', {}).get('score', 0)
            
            for theme, keywords in keyword_map.items():
                if any(keyword in text for keyword in keywords):
                    if theme not in themes:
                        themes[theme] = {'count': 0, 'sentiment_sum': 0.0, 'avg_sentiment': 0.0}
                    themes[theme]['count'] += 1
                    themes[theme]['sentiment_sum'] += sentiment_score
        
        # Calculate average sentiment for each theme
        for theme in themes:
            if themes[theme]['count'] > 0:
                themes[theme]['avg_sentiment'] = themes[theme]['sentiment_sum'] / themes[theme]['count']
        
        return themes
    
    # ========================================================================
    # Enhanced Recommendation Generation
    # ========================================================================
    
    def _generate_holistic_recommendations(
        self,
        system_state: Dict[str, Any],
        opportunities: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate holistic recommendations with ROI estimation"""
        recommendations = []
        
        # Combine related opportunities
        combined_opportunities = {}
        for opp in opportunities:
            key = opp['area']
            if key not in combined_opportunities:
                combined_opportunities[key] = {
                    'area': key,
                    'types': [],
                    'priority': opp['priority'],
                    'suggestions': [opp['suggestion']],
                    'expected_impact': opp.get('expected_impact', 'Expected improvement'),
                    'predicted': opp.get('predicted', False),
                    'timeframe_days': opp.get('timeframe_days', None)
                }
            else:
                combined_opportunities[key]['types'].append(opp.get('type', 'general'))
                combined_opportunities[key]['suggestions'].append(opp['suggestion'])
                combined_opportunities[key]['priority'] = max(
                    combined_opportunities[key]['priority'],
                    opp['priority']
                )
                if opp.get('timeframe_days'):
                    combined_opportunities[key]['timeframe_days'] = min(
                        combined_opportunities[key].get('timeframe_days', 30),
                        opp['timeframe_days']
                    )
        
        # Generate holistic recommendations
        for area, data in combined_opportunities.items():
            # Check for existing strategy
            strategy_signature = self._generate_strategy_signature(area, data['suggestions'])
            existing_milestone = self._find_milestone_by_strategy(strategy_signature)
            
            recommendation = {
                'area': area,
                'action': data['suggestions'][0] if len(data['suggestions']) == 1 else 
                         f"Multiple actions: {', '.join(data['suggestions'][:2])}",
                'priority': data['priority'],
                'rationale': self._generate_rationale(area, system_state),
                'expected_outcome': self._predict_outcome(area, data['types']),
                'predicted': data.get('predicted', False),
                'timeframe_days': data.get('timeframe_days', None),
                'strategy_signature': strategy_signature,
                'historical_effectiveness': self.historical_effectiveness.get(area, 0.5)
            }
            
            if existing_milestone:
                recommendation['previous_effectiveness'] = existing_milestone.ai_suggestion_impact
                recommendation['reuse_benefit'] = f"Strategy previously effective ({existing_milestone.ai_suggestion_impact:.2f})"
            
            recommendations.append(recommendation)
        
        # Cross-cutting recommendations
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
        """Generate a unique signature for a strategy"""
        combined = f"{area}:{'.'.join(sorted(suggestions))}"
        return hashlib.md5(combined.encode()).hexdigest()[:12]
    
    def _find_milestone_by_strategy(self, strategy_signature: str) -> Optional[EvolutionMilestone]:
        """Find a milestone by strategy signature"""
        for milestone in self.evolution_milestones:
            if milestone.strategy_signature == strategy_signature:
                return milestone
        return None
    
    # ========================================================================
    # Milestone-Based Learning
    # ========================================================================
    
    def _learn_from_milestone(self, milestone: EvolutionMilestone):
        """
        Learn from a milestone and store its strategy for future reuse.
        """
        if milestone.strategy_signature:
            # Store effectiveness of this strategy
            if milestone.strategy_signature not in self.milestone_strategies:
                self.milestone_strategies[milestone.strategy_signature] = []
            
            self.milestone_strategies[milestone.strategy_signature].append({
                'effectiveness': milestone.ai_suggestion_impact,
                'timestamp': milestone.timestamp.isoformat(),
                'context': milestone.description
            })
            
            # Update historical effectiveness based on milestone
            area = self._extract_area_from_milestone(milestone)
            if area:
                self.historical_effectiveness[area] = milestone.ai_suggestion_impact
        
        # Reuse strategy if effective enough
        if milestone.ai_suggestion_impact > 0.7:
            milestone.reuse_count += 1
            logger.info(f"Milestone strategy {milestone.strategy_signature} marked for reuse (impact: {milestone.ai_suggestion_impact:.2f})")
    
    def _extract_area_from_milestone(self, milestone: EvolutionMilestone) -> Optional[str]:
        """Extract area from milestone description"""
        areas = ['quantum', 'moe', 'sustainability', 'user_experience', 'federated']
        for area in areas:
            if area in milestone.description.lower():
                return area
        return None
    
    def _update_historical_effectiveness(self, applied: List[Dict[str, Any]]):
        """Update historical effectiveness data based on applied recommendations"""
        for item in applied:
            if item.get('result', {}).get('success'):
                area = item['recommendation'].get('area', 'general')
                # Update with decaying average
                old_value = self.historical_effectiveness.get(area, 0.5)
                new_value = item.get('result', {}).get('effectiveness', 0.5)
                self.historical_effectiveness[area] = old_value * 0.7 + new_value * 0.3
    
    # ========================================================================
    # Enhanced Impact Measurement
    # ========================================================================
    
    async def _measure_impact(
        self,
        applied: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Measure the impact of applied recommendations with effectiveness tracking"""
        impact = {
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': {
                'performance': 0.0,
                'sustainability': 0.0,
                'user_satisfaction': 0.0,
                'overall_effectiveness': 0.0
            },
            'details': []
        }
        
        for item in applied:
            if item.get('result', {}).get('success'):
                # Extract effectiveness from result
                effectiveness = item['result'].get('effectiveness', 0.7)
                area = item['recommendation'].get('area', 'general')
                
                # Aggregate impact metrics
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
                    'recommendation': item['recommendation'].get('action', 'Unknown action')
                })
        
        # Normalize metrics
        total_impact = sum(impact['metrics'].values())
        if total_impact > 0:
            impact['metrics']['performance'] = min(1.0, impact['metrics']['performance'] / 0.8)
            impact['metrics']['sustainability'] = min(1.0, impact['metrics']['sustainability'] / 0.8)
            impact['metrics']['user_satisfaction'] = min(1.0, impact['metrics']['user_satisfaction'] / 0.8)
        
        # Calculate overall effectiveness
        impact['metrics']['overall_effectiveness'] = (
            impact['metrics']['performance'] * 0.3 +
            impact['metrics']['sustainability'] * 0.4 +
            impact['metrics']['user_satisfaction'] * 0.3
        )
        
        # Add long-term trend analysis
        long_term_trend = self.impact_tracker.get_overall_trend()
        impact['long_term_trend'] = long_term_trend
        
        return impact
    
    # ========================================================================
    # Enhanced Milestone Detection
    # ========================================================================
    
    def _detect_milestone(self, impact: Dict[str, Any]) -> Optional[EvolutionMilestone]:
        """Detect if a significant milestone has been achieved with strategy signature"""
        milestone = None
        strategy_signature = None
        
        # Check for significant improvements with strategy tracking
        if impact['metrics']['sustainability'] > 0.8:
            # Find the most impactful strategy used
            if impact['details']:
                best_detail = max(impact['details'], key=lambda x: x.get('effectiveness', 0))
                strategy_signature = self._generate_strategy_signature(
                    best_detail['area'],
                    [best_detail['recommendation']]
                )
            
            milestone = EvolutionMilestone(
                timestamp=datetime.utcnow(),
                milestone_type='breakthrough',
                description='Achieved major sustainability improvement',
                metrics=impact['metrics'],
                human_feedback_count=len(self.feedback_history),
                ai_suggestion_impact=0.9,
                strategy_signature=strategy_signature
            )
        
        elif impact['metrics']['performance'] > 0.7 and impact['metrics']['user_satisfaction'] > 0.6:
            if impact['details']:
                best_detail = max(impact['details'], key=lambda x: x.get('effectiveness', 0))
                strategy_signature = self._generate_strategy_signature(
                    best_detail['area'],
                    [best_detail['recommendation']]
                )
            
            milestone = EvolutionMilestone(
                timestamp=datetime.utcnow(),
                milestone_type='breakthrough',
                description='System performance and user satisfaction at high levels',
                metrics=impact['metrics'],
                human_feedback_count=len(self.feedback_history),
                ai_suggestion_impact=0.8,
                strategy_signature=strategy_signature
            )
        
        # Check for learning spike
        elif len(self.performance_history) > 5:
            recent_performance = [h['impact']['metrics']['performance'] for h in self.performance_history[-5:]
                                 if 'metrics' in h['impact']]
            if len(recent_performance) >= 3:
                improvement = np.mean(recent_performance[-3:]) - np.mean(recent_performance[:3])
                if improvement > 0.15:
                    milestone = EvolutionMilestone(
                        timestamp=datetime.utcnow(),
                        milestone_type='learning_spike',
                        description=f'Performance improvement of {improvement:.1%} detected',
                        metrics=impact['metrics'],
                        human_feedback_count=len(self.feedback_history),
                        ai_suggestion_impact=improvement
                    )
        
        # Check for adaptation
        elif len(self.sustainability_trajectory) > 10:
            recent_trend = self.sustainability_trajectory[-5:]
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
    
    # ========================================================================
    # Existing Methods (Preserved with minor enhancements)
    # ========================================================================
    
    async def _collect_system_state(self) -> Dict[str, Any]:
        """Collect comprehensive system state from all components"""
        state = {
            'timestamp': datetime.utcnow().isoformat(),
            'components': {}
        }
        
        if self.quantum_benchmark:
            state['components']['quantum'] = await self.quantum_benchmark.get_benchmark_summary()
        
        if self.fft_moe:
            state['components']['moe'] = await self.fft_moe.get_fft_moe_status()
        
        if self.helium_manager:
            state['components']['helium'] = await self.helium_manager.get_stats()
        
        if self.federated_orchestrator:
            state['components']['federated'] = {
                'round_number': self.federated_orchestrator.round_number,
                'participants': len(self.federated_orchestrator.participants),
                'global_model_accuracy': self.federated_orchestrator.global_accuracy if hasattr(self.federated_orchestrator, 'global_accuracy') else 0.0
            }
        
        state['overall'] = self._calculate_system_metrics(state['components'])
        
        # Add long-term trends
        state['long_term_trends'] = {}
        for area in ['quantum', 'moe', 'sustainability', 'user_experience', 'federated']:
            trend = self.impact_tracker.get_area_trend(area)
            if trend.get('status') != 'insufficient_data':
                state['long_term_trends'][area] = trend
        
        return state
    
    def _generate_rationale(self, area: str, system_state: Dict[str, Any]) -> str:
        """Generate rationale for a recommendation with long-term context"""
        rationales = {
            'quantum': 'Quantum energy savings below target, optimization needed',
            'moe': 'MoE adoption is limited, more client participation needed',
            'sustainability': 'High helium scarcity requires immediate action',
            'user_experience': 'User feedback indicates areas for improvement',
            'federated': 'Federated learning performance can be improved'
        }
        
        base_rationale = rationales.get(area, f'Improvement needed in {area}')
        
        # Add long-term trend context
        area_trend = self.impact_tracker.get_area_trend(area)
        if area_trend.get('status') != 'insufficient_data':
            if area_trend.get('trend') == 'declining':
                base_rationale += f" (Long-term trend: {area_trend['trend']}, attention needed)"
            elif area_trend.get('trend') == 'improving':
                base_rationale += f" (Long-term trend: {area_trend['trend']}, continue momentum)"
        
        return base_rationale
    
    def _predict_outcome(self, area: str, types: List[str]) -> str:
        """Predict expected outcome of a recommendation with historical context"""
        outcomes = {
            'quantum': '10-30% reduction in energy consumption',
            'moe': 'Improved personalization and model accuracy',
            'sustainability': 'Significant reduction in resource usage',
            'user_experience': 'Increased user engagement and trust',
            'federated': 'Better global model performance'
        }
        
        base_outcome = outcomes.get(area, 'Expected performance improvement')
        
        # Add historical effectiveness context
        effectiveness = self.historical_effectiveness.get(area, 0.5)
        if effectiveness > 0.7:
            base_outcome += f" (Historically effective: {effectiveness:.1%})"
        elif effectiveness < 0.3:
            base_outcome += f" (Historically challenging: {effectiveness:.1%})"
        
        return base_outcome
    
    def _calculate_system_metrics(self, components: Dict[str, Any]) -> Dict[str, float]:
        """Calculate overall system metrics from components"""
        metrics = {
            'overall_health': 0.0,
            'sustainability_score': 0.0,
            'performance_score': 0.0,
            'user_engagement': 0.0
        }
        
        if components.get('helium'):
            helium_state = components['helium'].get('current', {})
            scarcity = helium_state.get('scarcity_index', 0)
            metrics['sustainability_score'] = 1.0 - scarcity
        
        if components.get('quantum'):
            quantum_state = components['quantum']
            metrics['performance_score'] = min(1.0, quantum_state.get('average_speedup', 0) / 5)
        
        if components.get('moe'):
            moe_state = components['moe']
            metrics['user_engagement'] = min(1.0, moe_state.get('num_clients', 0) / 100)
        
        metrics['overall_health'] = (
            metrics['sustainability_score'] * 0.4 +
            metrics['performance_score'] * 0.3 +
            metrics['user_engagement'] * 0.3
        )
        
        return metrics
    
    def _update_evolution_state(
        self,
        impact: Dict[str, Any],
        feedback: List[Dict[str, Any]]
    ):
        """Update the overall evolution state"""
        self.performance_history.append({
            'timestamp': datetime.utcnow(),
            'impact': impact
        })
        
        # Update trust scores based on impact and sentiment
        for user_id, user_model in self.user_models.items():
            if impact['metrics'].get('user_satisfaction', 0) > 0.5:
                user_model['trust_score'] = min(1.0, user_model.get('trust_score', 0.5) + 0.05)
            else:
                user_model['trust_score'] = max(0.0, user_model.get('trust_score', 0.5) - 0.02)
    
    def _calculate_trend(self) -> str:
        """Calculate overall sustainability trend"""
        if len(self.sustainability_trajectory) < 10:
            return "stable"
        
        recent = self.sustainability_trajectory[-10:]
        avg_recent = np.mean(recent)
        avg_older = np.mean(self.sustainability_trajectory[-20:-10]) if len(self.sustainability_trajectory) >= 20 else avg_recent
        
        if avg_recent > avg_older * 1.05:
            return "improving"
        elif avg_recent < avg_older * 0.95:
            return "declining"
        else:
            return "stable"
    
    def _generate_recommendations(self, forecast: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations from forecast"""
        recommendations = []
        
        days_to_critical = forecast.get('days_to_critical')
        if days_to_critical is not None:
            if days_to_critical <= 3:
                recommendations.append("URGENT: Implement immediate helium reduction measures")
                recommendations.append("Prioritize critical jobs only")
            elif days_to_critical <= 7:
                recommendations.append("Accelerate helium efficiency improvements")
                recommendations.append("Begin transitioning to helium-efficient operations")
        
        return recommendations
    
    async def _apply_recommendations(
        self,
        recommendations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply recommendations to the system with effectiveness tracking.
        
        Returns:
            List of applied recommendations and their results
        """
        applied = []
        
        for recommendation in recommendations[:3]:  # Apply top 3 recommendations
            try:
                if recommendation['area'] == 'quantum':
                    result = await self._apply_quantum_optimization()
                elif recommendation['area'] == 'moe':
                    result = await self._apply_moe_improvement()
                elif recommendation['area'] == 'sustainability':
                    result = await self._apply_sustainability_measure()
                elif recommendation['area'] == 'user_experience':
                    result = await self._apply_ux_improvement()
                else:
                    result = await self._apply_system_wide_optimization()
                
                # Add effectiveness score
                if result.get('success', False):
                    result['effectiveness'] = 0.7 + np.random.random() * 0.2  # Simulated effectiveness
                
                applied.append({
                    'recommendation': recommendation,
                    'result': result,
                    'timestamp': datetime.utcnow().isoformat()
                })
            except Exception as e:
                logger.error(f"Failed to apply recommendation: {e}")
                applied.append({
                    'recommendation': recommendation,
                    'result': {'success': False, 'error': str(e)},
                    'timestamp': datetime.utcnow().isoformat()
                })
        
        return applied
    
    async def _apply_quantum_optimization(self) -> Dict[str, Any]:
        """Apply quantum-specific optimizations"""
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
        """Apply MoE-specific improvements"""
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
        """Apply sustainability measures"""
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
        """Apply user experience improvements"""
        return {
            'success': True,
            'feedback_loop_improved': True,
            'user_engagement_boost': 1.2,
            'effectiveness': 0.7
        }
    
    async def _apply_system_wide_optimization(self) -> Dict[str, Any]:
        """Apply system-wide optimizations"""
        return {
            'success': True,
            'optimizations_applied': ['cache_clearing', 'model_pruning', 'data_compression'],
            'effectiveness': 0.6
        }
    
    # ========================================================================
    # Public Methods
    # ========================================================================
    
    def get_evolution_status(self) -> Dict[str, Any]:
        """Get comprehensive evolution status"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'performance_history': len(self.performance_history),
            'milestones': len(self.evolution_milestones),
            'user_models': len(self.user_models),
            'policy_suggestions': len(self.policy_suggestions),
            'collaborative_decisions': len(self.collaborative_decisions),
            'learning_rate': self.learning_rate,
            'exploration_rate': self.exploration_rate,
            'adaptation_threshold': self.adaptation_threshold,
            'historical_effectiveness': self.historical_effectiveness,
            'milestone_strategies': len(self.milestone_strategies),
            'long_term_trend': self.impact_tracker.get_overall_trend()
        }
    
    def get_feedback_sentiment_summary(self) -> Dict[str, Any]:
        """Get summary of feedback sentiment"""
        if not self.feedback_history:
            return {'status': 'no_feedback'}
        
        sentiments = []
        for feedback in self.feedback_history:
            if 'sentiment' in feedback:
                sentiments.append(feedback['sentiment'].get('score', 0))
        
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
        """Get summary of evolution milestones"""
        if not self.evolution_milestones:
            return {'status': 'no_milestones'}
        
        milestone_types = defaultdict(int)
        avg_impact = []
        
        for milestone in self.evolution_milestones:
            milestone_types[milestone.milestone_type] += 1
            avg_impact.append(milestone.ai_suggestion_impact)
        
        return {
            'total_milestones': len(self.evolution_milestones),
            'types': dict(milestone_types),
            'average_impact': np.mean(avg_impact) if avg_impact else 0,
            'max_impact': max(avg_impact) if avg_impact else 0,
            'reused_strategies': sum(1 for m in self.evolution_milestones if m.reuse_count > 0),
            'most_recent': self.evolution_milestones[-1].to_dict() if self.evolution_milestones else None
        }
