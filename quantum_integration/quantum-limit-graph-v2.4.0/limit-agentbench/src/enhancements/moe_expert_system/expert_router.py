# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_router.py
# Enhanced with predictive load forecasting, anomaly-based circuit breaking, shadow routing, and strategy A/B testing

"""
Enhanced Expert Router v3.0.0
- Predictive load forecasting for proactive scaling
- Anomaly-based circuit breaking with pattern recognition
- Multi-region router federation
- Real-time routing visualization data
- Shadow routing for safe strategy testing
- Cost-based routing optimization
- Adaptive timeout management
- Routing strategy A/B testing
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import time
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Predictive Load Forecasting
# ============================================================================

class PredictiveLoadForecaster:
    """
    Predicts future system load for proactive scaling.
    
    Uses time-series analysis and pattern recognition.
    """
    
    def __init__(self, history_window: int = 168):  # 1 week hourly
        self.load_history: deque = deque(maxlen=history_window * 60)  # Per-minute
        self.forecast_horizon_minutes: int = 30
        self.seasonal_patterns: Dict[int, float] = {}  # hour -> avg load
        self.trend_factor: float = 0.0
        
        # Anomaly thresholds
        self.spike_threshold: float = 2.0  # Std devs above mean
        self.drop_threshold: float = -1.5  # Std devs below mean
        
        logger.info("Predictive Load Forecaster initialized")
    
    def record_load(self, active_routes: int, max_routes: int):
        """Record current load measurement"""
        utilization = active_routes / max(max_routes, 1)
        
        self.load_history.append({
            'timestamp': datetime.utcnow(),
            'active_routes': active_routes,
            'max_routes': max_routes,
            'utilization': utilization
        })
        
        # Update seasonal patterns
        current_hour = datetime.utcnow().hour
        if current_hour not in self.seasonal_patterns:
            self.seasonal_patterns[current_hour] = utilization
        else:
            alpha = 0.1
            self.seasonal_patterns[current_hour] = (
                self.seasonal_patterns[current_hour] * (1 - alpha) +
                utilization * alpha
            )
        
        # Update trend
        if len(self.load_history) >= 10:
            recent = list(self.load_history)[-10:]
            utils = [r['utilization'] for r in recent]
            self.trend_factor = np.polyfit(range(10), utils, 1)[0] * 10  # Per 10 minutes
    
    def forecast_load(
        self,
        horizon_minutes: int = 30
    ) -> Dict[str, Any]:
        """
        Forecast future load.
        
        Returns:
            {
                'forecast': List of (timestamp, predicted_utilization),
                'confidence_interval': (lower, upper),
                'trend': 'increasing' | 'decreasing' | 'stable',
                'peak_predicted': float,
                'peak_time': datetime
            }
        """
        if len(self.load_history) < 10:
            return {'forecast': [], 'trend': 'stable', 'peak_predicted': 0.5}
        
        # Get recent utilization
        recent_utils = [r['utilization'] for r in list(self.load_history)[-30:]]
        current_util = recent_utils[-1]
        current_hour = datetime.utcnow().hour
        
        # Generate forecast
        forecast = []
        peak_util = current_util
        peak_time = datetime.utcnow()
        
        for minute in range(0, horizon_minutes, 5):
            future_time = datetime.utcnow() + timedelta(minutes=minute)
            future_hour = future_time.hour
            
            # Base prediction: current + trend
            base_pred = current_util + self.trend_factor * (minute / 10)
            
            # Seasonal adjustment
            seasonal = self.seasonal_patterns.get(future_hour, current_util)
            seasonal_adjustment = (seasonal - current_util) * 0.3
            
            # Combine
            predicted = base_pred + seasonal_adjustment
            predicted = max(0, min(1, predicted))
            
            forecast.append({
                'timestamp': future_time.isoformat(),
                'predicted_utilization': round(predicted, 3),
                'predicted_routes': int(predicted * self.load_history[-1]['max_routes'])
            })
            
            if predicted > peak_util:
                peak_util = predicted
                peak_time = future_time
        
        # Calculate confidence interval
        std_dev = np.std(recent_utils) if len(recent_utils) > 1 else 0.1
        confidence = (
            max(0, current_util - 2 * std_dev),
            min(1, current_util + 2 * std_dev)
        )
        
        # Determine trend
        if self.trend_factor > 0.01:
            trend = 'increasing'
        elif self.trend_factor < -0.01:
            trend = 'decreasing'
        else:
            trend = 'stable'
        
        return {
            'forecast': forecast,
            'confidence_interval': confidence,
            'trend': trend,
            'peak_predicted': round(peak_util, 3),
            'peak_time': peak_time.isoformat(),
            'current_utilization': round(current_util, 3)
        }
    
    def should_scale_up(self, threshold: float = 0.7) -> Tuple[bool, float]:
        """Determine if system should scale up proactively"""
        forecast = self.forecast_load(horizon_minutes=10)
        
        if forecast['trend'] == 'increasing':
            peak = forecast['peak_predicted']
            if peak > threshold:
                return True, peak
        
        return False, forecast.get('current_utilization', 0.5)
    
    def should_scale_down(self, threshold: float = 0.3) -> Tuple[bool, float]:
        """Determine if system should scale down"""
        forecast = self.forecast_load(horizon_minutes=30)
        
        if forecast['trend'] == 'decreasing':
            peak = forecast['peak_predicted']
            if peak < threshold:
                return True, peak
        
        return False, forecast.get('current_utilization', 0.5)
    
    def detect_anomalous_load(self) -> List[Dict[str, Any]]:
        """Detect anomalous load patterns"""
        if len(self.load_history) < 30:
            return []
        
        recent = list(self.load_history)[-30:]
        utils = [r['utilization'] for r in recent]
        mean = np.mean(utils)
        std = np.std(utils)
        
        anomalies = []
        current = utils[-1]
        
        # Spike detection
        if std > 0 and (current - mean) / std > self.spike_threshold:
            anomalies.append({
                'type': 'spike',
                'current': current,
                'mean': mean,
                'std': std,
                'zscore': (current - mean) / std,
                'severity': 'high' if (current - mean) / std > 3 else 'medium'
            })
        
        # Drop detection
        if std > 0 and (current - mean) / std < self.drop_threshold:
            anomalies.append({
                'type': 'drop',
                'current': current,
                'mean': mean,
                'std': std,
                'zscore': (current - mean) / std,
                'severity': 'medium'
            })
        
        return anomalies


# ============================================================================
# Anomaly-Based Circuit Breaking
# ============================================================================

class AnomalyCircuitBreaker:
    """
    Advanced anomaly-based circuit breaking.
    
    Uses pattern recognition instead of simple failure counts.
    """
    
    def __init__(self):
        self.expert_patterns: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.anomaly_models: Dict[str, Dict[str, Any]] = {}
        self.breaker_states: Dict[str, Dict[str, Any]] = {}
        
        # Pattern recognition thresholds
        self.pattern_thresholds = {
            'error_burst': {'window': 10, 'threshold': 0.5},  # 50% errors in 10 calls
            'latency_climb': {'window': 20, 'threshold': 2.0},  # 2x latency increase
            'intermittent': {'window': 50, 'threshold': 0.3},  # 30% intermittent failures
            'degradation': {'window': 100, 'threshold': -0.01}  # Health decline rate
        }
        
        logger.info("Anomaly Circuit Breaker initialized")
    
    def record_call(
        self,
        expert_id: str,
        success: bool,
        latency_ms: float,
        carbon_kg: float,
        error_type: Optional[str] = None
    ):
        """Record expert call for pattern analysis"""
        self.expert_patterns[expert_id].append({
            'success': success,
            'latency_ms': latency_ms,
            'carbon_kg': carbon_kg,
            'error_type': error_type,
            'timestamp': datetime.utcnow()
        })
        
        # Analyze patterns
        self._analyze_patterns(expert_id)
    
    def _analyze_patterns(self, expert_id: str):
        """Analyze call patterns for anomalies"""
        history = list(self.expert_patterns[expert_id])
        
        if len(history) < 10:
            return
        
        analysis = {
            'expert_id': expert_id,
            'analyzed_at': datetime.utcnow().isoformat(),
            'detected_patterns': [],
            'breaker_recommendation': None
        }
        
        # Pattern 1: Error burst detection
        if len(history) >= self.pattern_thresholds['error_burst']['window']:
            recent = history[-self.pattern_thresholds['error_burst']['window']:]
            error_rate = sum(1 for c in recent if not c['success']) / len(recent)
            
            if error_rate > self.pattern_thresholds['error_burst']['threshold']:
                analysis['detected_patterns'].append({
                    'pattern': 'error_burst',
                    'error_rate': error_rate,
                    'window': len(recent),
                    'severity': 'critical' if error_rate > 0.7 else 'high'
                })
        
        # Pattern 2: Latency climb detection
        if len(history) >= self.pattern_thresholds['latency_climb']['window']:
            recent = history[-self.pattern_thresholds['latency_climb']['window']:]
            latencies = [c['latency_ms'] for c in recent]
            
            if len(latencies) >= 10:
                baseline = np.mean(latencies[:10])
                current = np.mean(latencies[-10:])
                
                if baseline > 0 and current / baseline > self.pattern_thresholds['latency_climb']['threshold']:
                    analysis['detected_patterns'].append({
                        'pattern': 'latency_climb',
                        'baseline_ms': baseline,
                        'current_ms': current,
                        'increase_factor': current / baseline,
                        'severity': 'high' if current / baseline > 3 else 'medium'
                    })
        
        # Pattern 3: Intermittent failure detection
        if len(history) >= self.pattern_thresholds['intermittent']['window']:
            recent = history[-self.pattern_thresholds['intermittent']['window']:]
            
            # Check for alternating success/failure pattern
            success_pattern = [c['success'] for c in recent]
            transitions = sum(
                1 for i in range(1, len(success_pattern))
                if success_pattern[i] != success_pattern[i-1]
            )
            intermittency = transitions / len(success_pattern)
            
            if intermittency > self.pattern_thresholds['intermittent']['threshold']:
                analysis['detected_patterns'].append({
                    'pattern': 'intermittent',
                    'intermittency_rate': intermittency,
                    'severity': 'medium'
                })
        
        # Pattern 4: Health degradation
        if len(history) >= self.pattern_thresholds['degradation']['window']:
            recent = history[-self.pattern_thresholds['degradation']['window']:]
            success_rate = [sum(1 for c in recent[i:i+10] if c['success']) / 10 
                          for i in range(0, len(recent)-10, 10)]
            
            if len(success_rate) >= 5:
                trend = np.polyfit(range(len(success_rate)), success_rate, 1)[0]
                
                if trend < self.pattern_thresholds['degradation']['threshold']:
                    analysis['detected_patterns'].append({
                        'pattern': 'degradation',
                        'trend': trend,
                        'severity': 'high' if trend < -0.02 else 'medium'
                    })
        
        # Determine breaker recommendation
        if analysis['detected_patterns']:
            critical_patterns = [p for p in analysis['detected_patterns'] if p['severity'] == 'critical']
            high_patterns = [p for p in analysis['detected_patterns'] if p['severity'] == 'high']
            
            if critical_patterns:
                analysis['breaker_recommendation'] = 'open_immediately'
            elif len(high_patterns) >= 2:
                analysis['breaker_recommendation'] = 'open_soon'
            elif high_patterns:
                analysis['breaker_recommendation'] = 'monitor_closely'
            else:
                analysis['breaker_recommendation'] = 'monitor'
        
        self.anomaly_models[expert_id] = analysis
    
    def should_break(self, expert_id: str) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Determine if circuit should break based on anomaly patterns.
        
        Returns:
            (should_break, reason, analysis)
        """
        if expert_id not in self.anomaly_models:
            return False, "No analysis available", {}
        
        analysis = self.anomaly_models[expert_id]
        recommendation = analysis.get('breaker_recommendation')
        
        if recommendation == 'open_immediately':
            return True, "Critical anomaly patterns detected", analysis
        elif recommendation == 'open_soon':
            return True, "Multiple high-severity patterns detected", analysis
        
        return False, "Within acceptable parameters", analysis
    
    def get_expert_health_analysis(
        self,
        expert_id: str
    ) -> Dict[str, Any]:
        """Get detailed health analysis for expert"""
        return self.anomaly_models.get(expert_id, {})
    
    def get_all_analyses(self) -> Dict[str, Dict[str, Any]]:
        """Get all expert analyses"""
        return self.anomaly_models.copy()


# ============================================================================
# Shadow Routing Engine
# ============================================================================

class ShadowRoutingEngine:
    """
    Shadow routing for safe strategy testing.
    
    Runs alternative routing strategies in shadow mode
    without affecting production traffic.
    """
    
    def __init__(self):
        self.shadow_strategies: Dict[str, Dict[str, Any]] = {}
        self.shadow_results: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.comparison_metrics: Dict[str, List[Dict]] = defaultdict(list)
        
        logger.info("Shadow Routing Engine initialized")
    
    def register_shadow_strategy(
        self,
        strategy_id: str,
        strategy_function: Callable,
        description: str = ""
    ):
        """Register a shadow routing strategy"""
        self.shadow_strategies[strategy_id] = {
            'function': strategy_function,
            'description': description,
            'registered_at': datetime.utcnow(),
            'total_evaluations': 0,
            'status': 'active'
        }
        
        logger.info(f"Registered shadow strategy: {strategy_id}")
    
    async def evaluate_shadow(
        self,
        strategy_id: str,
        context: Any,
        production_routing: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Evaluate shadow strategy against production routing.
        
        Does NOT affect actual routing decisions.
        """
        if strategy_id not in self.shadow_strategies:
            return {'status': 'error', 'reason': 'Strategy not found'}
        
        strategy = self.shadow_strategies[strategy_id]
        
        try:
            # Execute shadow strategy
            shadow_start = time.time()
            shadow_result = await strategy['function'](context)
            shadow_time = (time.time() - shadow_start) * 1000
            
            # Compare with production
            comparison = self._compare_results(
                production_routing,
                shadow_result,
                shadow_time
            )
            
            # Record results
            record = {
                'strategy_id': strategy_id,
                'timestamp': datetime.utcnow().isoformat(),
                'production_routing': production_routing,
                'shadow_routing': shadow_result,
                'comparison': comparison,
                'shadow_time_ms': shadow_time
            }
            
            self.shadow_results[strategy_id].append(record)
            strategy['total_evaluations'] += 1
            
            return comparison
            
        except Exception as e:
            logger.error(f"Shadow evaluation failed for {strategy_id}: {str(e)}")
            return {'status': 'error', 'reason': str(e)}
    
    def _compare_results(
        self,
        production: Dict[str, Any],
        shadow: Dict[str, Any],
        shadow_time_ms: float
    ) -> Dict[str, Any]:
        """Compare production and shadow routing results"""
        comparison = {
            'timestamp': datetime.utcnow().isoformat(),
            'production_experts': production.get('selected_experts', []),
            'shadow_experts': shadow.get('selected_experts', []),
            'expert_overlap': len(
                set(production.get('selected_experts', [])) &
                set(shadow.get('selected_experts', []))
            ),
            'production_confidence': production.get('confidence', 0),
            'shadow_confidence': shadow.get('confidence', 0),
            'confidence_delta': shadow.get('confidence', 0) - production.get('confidence', 0),
            'production_carbon': production.get('carbon_estimate_kg', 0),
            'shadow_carbon': shadow.get('carbon_estimate_kg', 0),
            'carbon_savings': production.get('carbon_estimate_kg', 0) - shadow.get('carbon_estimate_kg', 0),
            'shadow_latency_ms': shadow_time_ms,
            'would_switch': False
        }
        
        # Determine if shadow is better
        shadow_better = (
            comparison['confidence_delta'] > 0.05 and
            comparison['carbon_savings'] > 0
        )
        comparison['would_switch'] = shadow_better
        
        return comparison
    
    def get_strategy_performance(
        self,
        strategy_id: str
    ) -> Dict[str, Any]:
        """Get performance metrics for shadow strategy"""
        results = list(self.shadow_results.get(strategy_id, []))
        
        if not results:
            return {}
        
        recent = results[-100:]
        
        return {
            'strategy_id': strategy_id,
            'total_evaluations': len(results),
            'win_rate': sum(1 for r in recent if r['comparison'].get('would_switch')) / max(len(recent), 1),
            'avg_confidence_delta': np.mean([r['comparison']['confidence_delta'] for r in recent]),
            'avg_carbon_savings': np.mean([r['comparison']['carbon_savings'] for r in recent]),
            'avg_shadow_latency': np.mean([r['shadow_time_ms'] for r in recent]),
            'recommendation': self._get_promotion_recommendation(strategy_id)
        }
    
    def _get_promotion_recommendation(self, strategy_id: str) -> str:
        """Recommend whether to promote shadow strategy to production"""
        perf = self.get_strategy_performance(strategy_id)
        
        if not perf:
            return "Insufficient data"
        
        if perf['total_evaluations'] < 100:
            return f"Need more data ({perf['total_evaluations']}/100 evaluations)"
        
        if perf['win_rate'] > 0.7 and perf['avg_carbon_savings'] > 0:
            return "PROMOTE: Strategy consistently outperforms production"
        elif perf['win_rate'] > 0.5:
            return "CONSIDER: Strategy shows promise but needs refinement"
        else:
            return "REJECT: Strategy does not outperform production"
    
    def promote_to_production(self, strategy_id: str) -> bool:
        """Promote shadow strategy to production"""
        recommendation = self._get_promotion_recommendation(strategy_id)
        
        if 'PROMOTE' in recommendation:
            strategy = self.shadow_strategies[strategy_id]
            strategy['status'] = 'promoted'
            strategy['promoted_at'] = datetime.utcnow()
            
            logger.info(f"Promoted shadow strategy {strategy_id} to production")
            return True
        
        return False


# ============================================================================
# Cost-Based Routing Optimizer
# ============================================================================

class CostBasedRouter:
    """
    Cost-based routing optimization.
    
    Considers financial costs in addition to carbon/helium.
    """
    
    def __init__(self):
        self.cost_models: Dict[str, Dict[str, float]] = {}
        self.cost_history: deque = deque(maxlen=10000)
        
        # Default cost models
        self._initialize_cost_models()
        
        logger.info("Cost-Based Router initialized")
    
    def _initialize_cost_models(self):
        """Initialize default cost models"""
        self.cost_models = {
            'energy_expert': {
                'cost_per_inference': 0.0001,
                'carbon_cost_per_kg': 50.0,  # $50/ton CO2
                'helium_cost_per_unit': 100.0
            },
            'data_expert': {
                'cost_per_inference': 0.0002,
                'carbon_cost_per_kg': 40.0,
                'helium_cost_per_unit': 80.0
            },
            'iot_expert': {
                'cost_per_inference': 0.00005,
                'carbon_cost_per_kg': 30.0,
                'helium_cost_per_unit': 50.0
            },
            'quantum_expert': {
                'cost_per_inference': 0.01,
                'carbon_cost_per_kg': 100.0,
                'helium_cost_per_unit': 200.0
            },
            'helium_expert': {
                'cost_per_inference': 0.0003,
                'carbon_cost_per_kg': 60.0,
                'helium_cost_per_unit': 150.0
            }
        }
    
    def calculate_total_cost(
        self,
        expert_id: str,
        carbon_kg: float,
        helium_units: float,
        latency_ms: float
    ) -> Dict[str, float]:
        """Calculate total cost for expert execution"""
        model = self.cost_models.get(expert_id, {
            'cost_per_inference': 0.0001,
            'carbon_cost_per_kg': 50.0,
            'helium_cost_per_unit': 100.0
        })
        
        inference_cost = model['cost_per_inference']
        carbon_cost = carbon_kg * model['carbon_cost_per_kg'] / 1000  # Convert to $
        helium_cost = helium_units * model['helium_cost_per_unit']
        
        # Latency penalty (opportunity cost)
        latency_penalty = max(0, (latency_ms - 100) * 0.00001)  # $0.00001 per ms over 100ms
        
        total_cost = inference_cost + carbon_cost + helium_cost + latency_penalty
        
        cost_breakdown = {
            'expert_id': expert_id,
            'inference_cost': inference_cost,
            'carbon_cost': carbon_cost,
            'helium_cost': helium_cost,
            'latency_penalty': latency_penalty,
            'total_cost': total_cost,
            'cost_efficiency': 1.0 / (1.0 + total_cost * 1000)  # Normalized score
        }
        
        self.cost_history.append({
            **cost_breakdown,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return cost_breakdown
    
    def optimize_for_cost(
        self,
        expert_plans: List[Dict[str, Any]],
        budget: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Optimize expert selection for minimum cost.
        
        Args:
            expert_plans: Expert execution plans
            budget: Maximum cost budget
            
        Returns:
            Cost-optimized plans
        """
        scored_plans = []
        
        for plan in expert_plans:
            expert_id = plan.get('expert_id', 'unknown')
            carbon = plan.get('estimated_carbon_kg', 0)
            helium = plan.get('estimated_helium_units', 0)
            latency = plan.get('estimated_latency_ms', 100)
            
            cost = self.calculate_total_cost(expert_id, carbon, helium, latency)
            
            if budget is None or cost['total_cost'] <= budget:
                plan['cost_breakdown'] = cost
                plan['cost_score'] = cost['cost_efficiency']
                scored_plans.append(plan)
        
        # Sort by cost efficiency (higher is better)
        scored_plans.sort(key=lambda p: p.get('cost_score', 0), reverse=True)
        
        return scored_plans
    
    def update_cost_model(
        self,
        expert_id: str,
        updates: Dict[str, float]
    ):
        """Update cost model for expert"""
        if expert_id not in self.cost_models:
            self.cost_models[expert_id] = {
                'cost_per_inference': 0.0001,
                'carbon_cost_per_kg': 50.0,
                'helium_cost_per_unit': 100.0
            }
        
        self.cost_models[expert_id].update(updates)
        
        logger.info(f"Updated cost model for {expert_id}")
    
    def get_cost_summary(self) -> Dict[str, Any]:
        """Get cost summary"""
        recent = list(self.cost_history)[-1000:]
        
        if not recent:
            return {}
        
        return {
            'total_cost': sum(r['total_cost'] for r in recent),
            'average_cost_per_call': np.mean([r['total_cost'] for r in recent]),
            'cost_by_expert': {
                eid: sum(r['total_cost'] for r in recent if r['expert_id'] == eid)
                for eid in set(r['expert_id'] for r in recent)
            },
            'carbon_cost_ratio': sum(r['carbon_cost'] for r in recent) / max(sum(r['total_cost'] for r in recent), 0.001),
            'helium_cost_ratio': sum(r['helium_cost'] for r in recent) / max(sum(r['total_cost'] for r in recent), 0.001)
        }


# ============================================================================
# Adaptive Timeout Manager
# ============================================================================

class AdaptiveTimeoutManager:
    """
    Adaptive timeout management.
    
    Dynamically adjusts timeouts based on expert performance.
    """
    
    def __init__(self):
        self.expert_latencies: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.timeout_settings: Dict[str, Dict[str, float]] = {}
        self.timeout_history: deque = deque(maxlen=10000)
        
        # Default settings
        self.default_timeout_ms = 30000  # 30 seconds
        self.min_timeout_ms = 1000       # 1 second
        self.max_timeout_ms = 120000     # 2 minutes
        self.percentile_for_timeout = 99  # P99 latency * multiplier
        
        logger.info("Adaptive Timeout Manager initialized")
    
    def record_latency(
        self,
        expert_id: str,
        latency_ms: float,
        success: bool
    ):
        """Record expert latency for timeout adaptation"""
        self.expert_latencies[expert_id].append({
            'latency_ms': latency_ms,
            'success': success,
            'timestamp': datetime.utcnow()
        })
        
        # Update timeout settings
        self._update_timeout(expert_id)
    
    def _update_timeout(self, expert_id: str):
        """Update timeout for expert based on performance"""
        latencies = list(self.expert_latencies[expert_id])
        
        if len(latencies) < 10:
            self.timeout_settings[expert_id] = {
                'timeout_ms': self.default_timeout_ms,
                'reason': 'default',
                'updated_at': datetime.utcnow().isoformat()
            }
            return
        
        # Get successful latencies only
        success_latencies = [l['latency_ms'] for l in latencies if l['success']]
        
        if not success_latencies:
            # All failures - reduce timeout
            timeout = max(self.min_timeout_ms, self.default_timeout_ms * 0.5)
            reason = 'high_failure_rate'
        else:
            # Calculate P99 of successful latencies
            p99 = np.percentile(success_latencies, self.percentile_for_timeout)
            
            # Add buffer (2x P99)
            timeout = min(self.max_timeout_ms, p99 * 2)
            timeout = max(self.min_timeout_ms, timeout)
            
            # Check if timeout is too tight
            timeout_violations = sum(
                1 for l in success_latencies if l > timeout
            )
            violation_rate = timeout_violations / len(success_latencies)
            
            if violation_rate > 0.01:  # More than 1% violations
                timeout *= 1.5  # Increase timeout
                reason = 'high_violation_rate'
            elif violation_rate < 0.001:  # Less than 0.1% violations
                timeout *= 0.9  # Decrease timeout
                reason = 'low_utilization'
            else:
                reason = 'optimal'
        
        timeout = max(self.min_timeout_ms, min(self.max_timeout_ms, timeout))
        
        self.timeout_settings[expert_id] = {
            'timeout_ms': timeout,
            'reason': reason,
            'p99_latency': np.percentile(success_latencies, 99) if success_latencies else 0,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        self.timeout_history.append({
            'expert_id': expert_id,
            'timeout_ms': timeout,
            'reason': reason,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def get_timeout(self, expert_id: str) -> float:
        """Get adaptive timeout for expert"""
        settings = self.timeout_settings.get(expert_id)
        if settings:
            return settings['timeout_ms']
        return self.default_timeout_ms
    
    def get_timeout_stats(self) -> Dict[str, Any]:
        """Get timeout statistics"""
        return {
            expert_id: {
                'timeout_ms': settings['timeout_ms'],
                'reason': settings['reason'],
                'updated_at': settings['updated_at']
            }
            for expert_id, settings in self.timeout_settings.items()
        }
    
    def reset_timeout(self, expert_id: str):
        """Reset timeout to default"""
        self.timeout_settings[expert_id] = {
            'timeout_ms': self.default_timeout_ms,
            'reason': 'reset',
            'updated_at': datetime.utcnow().isoformat()
        }


# ============================================================================
# Multi-Region Router Federation
# ============================================================================

class MultiRegionRouterFederation:
    """
    Multi-region router federation.
    
    Enables coordination between routers in different regions.
    """
    
    def __init__(self, region_id: str = "local"):
        self.region_id = region_id
        self.peer_routers: Dict[str, Dict[str, Any]] = {}
        self.routing_exchange: deque = deque(maxlen=10000)
        self.global_expert_registry: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"Multi-Region Router Federation initialized: {region_id}")
    
    def register_peer_router(
        self,
        peer_id: str,
        region: str,
        endpoint: str,
        capabilities: List[str]
    ):
        """Register peer router from another region"""
        self.peer_routers[peer_id] = {
            'peer_id': peer_id,
            'region': region,
            'endpoint': endpoint,
            'capabilities': capabilities,
            'registered_at': datetime.utcnow(),
            'last_heartbeat': datetime.utcnow(),
            'status': 'active',
            'latency_ms': 0
        }
        
        logger.info(f"Registered peer router: {peer_id} ({region})")
    
    async def route_to_peer(
        self,
        peer_id: str,
        task: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Route task to peer router in another region.
        
        Used when local experts cannot handle task.
        """
        if peer_id not in self.peer_routers:
            return None
        
        peer = self.peer_routers[peer_id]
        
        if peer['status'] != 'active':
            return None
        
        # Simulate cross-region routing
        routing_result = {
            'routed_to': peer_id,
            'region': peer['region'],
            'local_expert_id': None,
            'remote_expert_id': f"{peer_id}_expert_{np.random.randint(1, 100)}",
            'estimated_latency_ms': peer['latency_ms'] + 50,
            'routed_at': datetime.utcnow().isoformat()
        }
        
        self.routing_exchange.append({
            'type': 'outbound',
            'peer_id': peer_id,
            'task_id': task.get('task_id'),
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return routing_result
    
    def receive_routed_task(
        self,
        from_peer_id: str,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Receive task routed from peer router"""
        self.routing_exchange.append({
            'type': 'inbound',
            'peer_id': from_peer_id,
            'task_id': task.get('task_id'),
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'accepted': True,
            'from_peer': from_peer_id,
            'task_id': task.get('task_id'),
            'received_at': datetime.utcnow().isoformat()
        }
    
    def get_federation_stats(self) -> Dict[str, Any]:
        """Get federation statistics"""
        recent = list(self.routing_exchange)[-100:]
        
        return {
            'region_id': self.region_id,
            'peer_routers': len(self.peer_routers),
            'active_peers': sum(1 for p in self.peer_routers.values() if p['status'] == 'active'),
            'total_exchanges': len(self.routing_exchange),
            'outbound_routes': sum(1 for r in recent if r['type'] == 'outbound'),
            'inbound_routes': sum(1 for r in recent if r['type'] == 'inbound'),
            'peers_by_region': {
                p['region']: sum(1 for peer in self.peer_routers.values() if peer['region'] == p['region'])
                for p in self.peer_routers.values()
            }
        }
    
    def update_peer_heartbeat(self, peer_id: str):
        """Update peer heartbeat"""
        if peer_id in self.peer_routers:
            self.peer_routers[peer_id]['last_heartbeat'] = datetime.utcnow()
    
    def check_peer_health(self) -> List[str]:
        """Check health of all peers"""
        unhealthy = []
        now = datetime.utcnow()
        
        for peer_id, peer in self.peer_routers.items():
            heartbeat_age = (now - peer['last_heartbeat']).total_seconds()
            
            if heartbeat_age > 60:
                peer['status'] = 'unhealthy'
                unhealthy.append(peer_id)
            elif heartbeat_age > 30:
                peer['status'] = 'degraded'
        
        return unhealthy


# ============================================================================
# Enhanced Expert Router with All Integrations
# ============================================================================

class ExpertRouter:
    """
    Enhanced Expert Router v3.0.0
    
    New capabilities:
    - Predictive load forecasting
    - Anomaly-based circuit breaking
    - Shadow routing for safe testing
    - Cost-based routing optimization
    - Adaptive timeout management
    - Multi-region router federation
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional[Any] = None,
        enable_forecasting: bool = True,
        enable_anomaly_breaker: bool = True,
        enable_shadow_routing: bool = True,
        enable_cost_routing: bool = True,
        enable_adaptive_timeout: bool = True,
        enable_federation: bool = True,
        region_id: str = "local"
    ):
        # Feature flags
        self.enable_forecasting = enable_forecasting
        self.enable_anomaly_breaker = enable_anomaly_breaker
        self.enable_shadow_routing = enable_shadow_routing
        self.enable_cost_routing = enable_cost_routing
        self.enable_adaptive_timeout = enable_adaptive_timeout
        self.enable_federation = enable_federation
        
        # New sub-modules
        self.load_forecaster = PredictiveLoadForecaster() if enable_forecasting else None
        self.anomaly_breaker = AnomalyCircuitBreaker() if enable_anomaly_breaker else None
        self.shadow_engine = ShadowRoutingEngine() if enable_shadow_routing else None
        self.cost_router = CostBasedRouter() if enable_cost_routing else None
        self.timeout_manager = AdaptiveTimeoutManager() if enable_adaptive_timeout else None
        self.federation = MultiRegionRouterFederation(region_id) if enable_federation else None
        
        # Existing components (from v2.0.0)
        self.metrics_collector = metrics_collector
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        self.circuit_breakers: Dict[str, Any] = {}
        
        # Routing metrics
        self.metrics = RoutingMetrics()
        self.routing_history: deque = deque(maxlen=10000)
        
        # Active routes tracking
        self.active_routes = 0
        self.max_concurrent_routes = 100
        self._route_lock = asyncio.Lock()
        
        # Initialize experts
        self._initialize_experts(enable_quantum)
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced Expert Router v3.0.0 initialized: "
            f"forecasting={enable_forecasting}, anomaly_breaker={enable_anomaly_breaker}, "
            f"shadow={enable_shadow_routing}, cost={enable_cost_routing}, "
            f"adaptive_timeout={enable_adaptive_timeout}, federation={enable_federation}"
        )
    
    def _initialize_experts(self, enable_quantum: bool):
        """Initialize experts (existing logic)"""
        try:
            from enhancements.moe_expert_system.experts import (
                EnergyExpert, DataExpert, IoTExpert, HeliumExpert
            )
            
            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            
            if enable_quantum:
                from enhancements.moe_expert_system.experts import QuantumExpert
                self.experts['quantum'] = QuantumExpert()
            
            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            
            logger.info(f"Initialized {len(self.experts)} experts")
            
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        asyncio.create_task(self._load_forecasting_loop())
        asyncio.create_task(self._anomaly_analysis_loop())
        asyncio.create_task(self._shadow_evaluation_loop())
        asyncio.create_task(self._peer_health_check_loop())
    
    async def _load_forecasting_loop(self):
        """Background load forecasting loop"""
        while True:
            try:
                if self.enable_forecasting:
                    self.load_forecaster.record_load(
                        self.active_routes,
                        self.max_concurrent_routes
                    )
                    
                    # Proactive scaling check
                    should_up, peak = self.load_forecaster.should_scale_up(0.7)
                    if should_up:
                        logger.info(f"Predicted peak utilization: {peak:.1%}. Consider scaling up.")
                    
                    should_down, valley = self.load_forecaster.should_scale_down(0.3)
                    if should_down:
                        logger.info(f"Predicted low utilization: {valley:.1%}. Consider scaling down.")
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                logger.error(f"Load forecasting error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _anomaly_analysis_loop(self):
        """Background anomaly analysis loop"""
        while True:
            try:
                if self.enable_anomaly_breaker:
                    for expert_id in self.experts:
                        should_break, reason, analysis = self.anomaly_breaker.should_break(expert_id)
                        
                        if should_break:
                            logger.warning(
                                f"Anomaly breaker triggered for {expert_id}: {reason}"
                            )
                            
                            # Open circuit breaker
                            if expert_id in self.circuit_breakers:
                                breaker = self.circuit_breakers[expert_id]
                                if breaker.state == CircuitBreakerState.CLOSED:
                                    breaker.state = CircuitBreakerState.OPEN
                                    breaker.last_failure_time = datetime.utcnow()
                
                await asyncio.sleep(30)  # Every 30 seconds
                
            except Exception as e:
                logger.error(f"Anomaly analysis error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _shadow_evaluation_loop(self):
        """Background shadow routing evaluation"""
        while True:
            try:
                if self.enable_shadow_routing and self.shadow_engine.shadow_strategies:
                    for strategy_id in self.shadow_engine.shadow_strategies:
                        perf = self.shadow_engine.get_strategy_performance(strategy_id)
                        
                        if perf.get('recommendation', '').startswith('PROMOTE'):
                            logger.info(
                                f"Shadow strategy {strategy_id} ready for promotion: "
                                f"win_rate={perf['win_rate']:.1%}"
                            )
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Shadow evaluation error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _peer_health_check_loop(self):
        """Background peer health check"""
        while True:
            try:
                if self.enable_federation:
                    unhealthy = self.federation.check_peer_health()
                    
                    if unhealthy:
                        logger.warning(f"Unhealthy peer routers: {unhealthy}")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Peer health check error: {str(e)}")
                await asyncio.sleep(60)
    
    async def route_and_execute(
        self,
        workload_profile: Dict[str, Any],
        meta_cognitive_state: Dict[str, Any],
        dual_axis_context: Dict[str, Any],
        symbolic_constraints: Optional[Dict[str, Any]] = None,
        enable_shadow: bool = False,
        enable_cost_optimization: bool = False,
        cost_budget: Optional[float] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Enhanced routing and execution with all integrations.
        """
        start_time = time.time()
        route_id = hashlib.md5(
            f"{workload_profile}{start_time}".encode()
        ).hexdigest()[:12]
        
        # Record load for forecasting
        if self.enable_forecasting:
            self.load_forecaster.record_load(
                self.active_routes,
                self.max_concurrent_routes
            )
        
        # Check for anomalous load
        if self.enable_forecasting:
            anomalies = self.load_forecaster.detect_anomalous_load()
            if anomalies:
                logger.warning(f"Anomalous load detected: {len(anomalies)} anomalies")
        
        async with self._route_lock:
            self.active_routes += 1
        
        try:
            # Existing routing logic...
            gating_context = self._build_gating_context(
                workload_profile, meta_cognitive_state, dual_axis_context
            )
            
            # Get routing decisions
            routing_result = self.gating_network.route(
                gating_context,
                expert_constraints=self._apply_symbolic_constraints(symbolic_constraints)
            )
            
            # Execute experts
            expert_plans = await self._execute_experts(
                routing_result, workload_profile,
                meta_cognitive_state, dual_axis_context
            )
            
            # Cost optimization
            if self.enable_cost_routing and enable_cost_optimization:
                expert_plans = self.cost_router.optimize_for_cost(
                    expert_plans, cost_budget
                )
            
            # Aggregate plans
            final_plan = await self._aggregate_plans(
                expert_plans, dual_axis_context, gating_context
            )
            
            # Shadow routing evaluation
            if self.enable_shadow_routing and enable_shadow:
                production_routing = {
                    'selected_experts': [self.expert_index_map.get(i, 'unknown') 
                                        for i in routing_result['expert_indices']],
                    'confidence': routing_result.get('confidence', 0.5),
                    'carbon_estimate_kg': final_plan.get('aggregate_carbon_kg', 0)
                }
                
                for strategy_id in self.shadow_engine.shadow_strategies:
                    asyncio.create_task(
                        self.shadow_engine.evaluate_shadow(
                            strategy_id, gating_context, production_routing
                        )
                    )
            
            # Record anomaly breaker data
            if self.enable_anomaly_breaker:
                for plan in expert_plans:
                    expert_id = plan.get('expert_id', 'unknown')
                    self.anomaly_breaker.record_call(
                        expert_id,
                        success=plan.get('routing_weight', 0) > 0.3,
                        latency_ms=plan.get('estimated_latency_ms', 50),
                        carbon_kg=plan.get('estimated_carbon_kg', 0)
                    )
            
            # Adaptive timeout recording
            if self.enable_adaptive_timeout:
                execution_time = (time.time() - start_time) * 1000
                for plan in expert_plans:
                    expert_id = plan.get('expert_id', 'unknown')
                    self.timeout_manager.record_latency(
                        expert_id,
                        execution_time,
                        success=final_plan.get('action') != 'defer'
                    )
            
            # Build response
            response = {
                'success': True,
                'route_id': route_id,
                'plans': expert_plans,
                'final_plan': final_plan,
                'execution_time_ms': (time.time() - start_time) * 1000,
                'forecast': self.load_forecaster.forecast_load() if self.enable_forecasting else None,
                'cost_summary': self.cost_router.get_cost_summary() if self.enable_cost_routing else None,
                'metadata': {
                    'expert_count': len(expert_plans),
                    'anomaly_breaker_active': self.enable_anomaly_breaker,
                    'shadow_routing_active': self.enable_shadow_routing,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Routing failed: {str(e)}", exc_info=True)
            return self._create_fallback_response(workload_profile, str(e))
        
        finally:
            async with self._route_lock:
                self.active_routes -= 1
    
    def get_routing_stats(self) -> Dict[str, Any]:
        """Get enhanced routing statistics"""
        stats = {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'success_rate': self.metrics.success_rate,
                'active_routes': self.active_routes
            }
        }
        
        # Load forecasting
        if self.enable_forecasting:
            stats['forecast'] = self.load_forecaster.forecast_load()
            stats['anomalies'] = self.load_forecaster.detect_anomalous_load()
        
        # Anomaly breaker
        if self.enable_anomaly_breaker:
            stats['anomaly_analyses'] = self.anomaly_breaker.get_all_analyses()
        
        # Shadow routing
        if self.enable_shadow_routing:
            stats['shadow_strategies'] = {
                sid: self.shadow_engine.get_strategy_performance(sid)
                for sid in self.shadow_engine.shadow_strategies
            }
        
        # Cost routing
        if self.enable_cost_routing:
            stats['cost_summary'] = self.cost_router.get_cost_summary()
        
        # Adaptive timeout
        if self.enable_adaptive_timeout:
            stats['timeouts'] = self.timeout_manager.get_timeout_stats()
        
        # Federation
        if self.enable_federation:
            stats['federation'] = self.federation.get_federation_stats()
        
        return stats
    
    def promote_shadow_strategy(self, strategy_id: str) -> bool:
        """Promote shadow strategy to production"""
        if self.enable_shadow_routing:
            return self.shadow_engine.promote_to_production(strategy_id)
        return False
    
    def register_shadow_strategy(
        self,
        strategy_id: str,
        strategy_function: Callable,
        description: str = ""
    ):
        """Register shadow routing strategy"""
        if self.enable_shadow_routing:
            self.shadow_engine.register_shadow_strategy(
                strategy_id, strategy_function, description
            )
    
    def update_cost_model(
        self,
        expert_id: str,
        updates: Dict[str, float]
    ):
        """Update cost model for expert"""
        if self.enable_cost_routing:
            self.cost_router.update_cost_model(expert_id, updates)
    
    def register_peer_router(
        self,
        peer_id: str,
        region: str,
        endpoint: str,
        capabilities: List[str]
    ):
        """Register peer router for federation"""
        if self.enable_federation:
            self.federation.register_peer_router(
                peer_id, region, endpoint, capabilities
            )
    
    def reset_timeout(self, expert_id: str):
        """Reset adaptive timeout for expert"""
        if self.enable_adaptive_timeout:
            self.timeout_manager.reset_timeout(expert_id)
