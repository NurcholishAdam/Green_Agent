# File: src/enhancements/reasoning_engine.py
"""
Reasoning Engine for Green Agent
Implements temporal, causal, ethical, contextual, systemic, and reflexive reasoning
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import hashlib
import numpy as np
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

# ============================================================================
# Temporal Reasoning: Carbon Intensity Awareness
# ============================================================================

class CarbonIntensityAwareScheduler:
    """Schedule computations during low-carbon periods"""
    
    def __init__(self):
        self.carbon_intensity_cache = {}
        self.region = "global"
        self.forecast_hours = 24
        
        # Historical carbon intensity patterns (simplified)
        # Real implementation would query Electricity Maps API
        self.historical_patterns = {
            "global": {
                "peak_hours": [18, 19, 20, 21],  # Evening peak
                "low_hours": [1, 2, 3, 4, 5],     # Night hours
                "solar_peak": [11, 12, 13, 14]    # Solar peak
            }
        }
    
    async def get_current_intensity(self, region: str = "global") -> float:
        """Get current carbon intensity in gCO2/kWh"""
        # Simulated data - would use API in production
        hour = datetime.now().hour
        pattern = self.historical_patterns.get(region, self.historical_patterns["global"])
        
        if hour in pattern["low_hours"]:
            return 200  # Low carbon intensity
        elif hour in pattern["solar_peak"]:
            return 300  # Moderate carbon intensity
        elif hour in pattern["peak_hours"]:
            return 600  # High carbon intensity
        else:
            return 400  # Average
    
    async def get_forecast(self, region: str = "global", hours: int = 24) -> List[Dict]:
        """Get carbon intensity forecast for next 24 hours"""
        forecast = []
        current_hour = datetime.now().hour
        
        for i in range(hours):
            hour = (current_hour + i) % 24
            forecast_hour = datetime.now() + timedelta(hours=i)
            
            # Simplified forecast based on historical patterns
            pattern = self.historical_patterns.get(region, self.historical_patterns["global"])
            
            if hour in pattern["low_hours"]:
                intensity = 180 + np.random.normal(0, 20)
            elif hour in pattern["solar_peak"]:
                intensity = 280 + np.random.normal(0, 30)
            elif hour in pattern["peak_hours"]:
                intensity = 550 + np.random.normal(0, 50)
            else:
                intensity = 380 + np.random.normal(0, 40)
            
            forecast.append({
                'datetime': forecast_hour.isoformat(),
                'hour': hour,
                'intensity': max(100, intensity),
                'savings_potential': (intensity - 200) / intensity  # Relative to clean energy baseline
            })
        
        return forecast
    
    async def schedule_computation(self, 
                                   task: str, 
                                   urgency: str = "normal",
                                   compute_hours: float = 1.0) -> Dict[str, Any]:
        """
        Schedule computation based on carbon intensity and urgency.
        
        Args:
            task: Task identifier
            urgency: "critical", "normal", or "flexible"
            compute_hours: Estimated compute duration in hours
            
        Returns:
            Scheduling recommendation
        """
        current_intensity = await self.get_current_intensity(self.region)
        forecast = await self.get_forecast(self.region, 24)
        
        # Find best time in the next 24 hours
        best_time = min(forecast, key=lambda x: x['intensity'])
        
        # Calculate potential savings
        savings_percent = (current_intensity - best_time['intensity']) / current_intensity if current_intensity > 0 else 0
        savings_percent = max(0, savings_percent)
        
        # Decision logic based on urgency
        if urgency == "critical":
            # Run immediately
            recommendation = {
                'action': 'run_now',
                'reason': 'Critical task - immediate execution required',
                'schedule': datetime.now().isoformat(),
                'expected_intensity': current_intensity,
                'carbon_savings': 0
            }
        elif urgency == "normal" and savings_percent > 0.2:
            # Delay to best time
            recommendation = {
                'action': 'schedule',
                'reason': f'Delay by {best_time["datetime"]} to save {savings_percent:.1%} carbon',
                'schedule': best_time['datetime'],
                'expected_intensity': best_time['intensity'],
                'carbon_savings': savings_percent
            }
        elif urgency == "flexible":
            # Find the absolute best time
            best_time = min(forecast, key=lambda x: x['intensity'])
            recommendation = {
                'action': 'schedule_optimal',
                'reason': f'Flexible task - optimal schedule at {best_time["datetime"]}',
                'schedule': best_time['datetime'],
                'expected_intensity': best_time['intensity'],
                'carbon_savings': savings_percent
            }
        else:
            # Default: run now if savings are minimal
            recommendation = {
                'action': 'run_now',
                'reason': f'Marginal savings ({savings_percent:.1%}) - running now',
                'schedule': datetime.now().isoformat(),
                'expected_intensity': current_intensity,
                'carbon_savings': 0
            }
        
        # Add context
        recommendation.update({
            'task': task,
            'urgency': urgency,
            'compute_hours': compute_hours,
            'current_intensity': current_intensity,
            'forecast_window_hours': 24
        })
        
        logger.info(f"Computation scheduling for {task}: {recommendation['action']} ({savings_percent:.1%} savings)")
        return recommendation

# ============================================================================
# Causal Reasoning: Understanding Carbon Impact
# ============================================================================

@dataclass
class CausalExplanation:
    """Causal explanation of carbon impact"""
    primary_driver: str
    contribution: float
    pathway: List[str]
    alternatives: List[str]
    confidence: float

class CarbonCausalModel:
    """Build and maintain causal models of carbon impact"""
    
    def __init__(self):
        # Causal graph: architectural features -> carbon pathways
        self.causal_graph = {
            'num_layers': {
                'pathways': ['parameters', 'flops', 'memory_bandwidth', 'energy', 'carbon'],
                'effect_size': 0.35,
                'non_linear': True
            },
            'hidden_dim': {
                'pathways': ['parameters', 'flops', 'memory', 'energy', 'carbon'],
                'effect_size': 0.30,
                'non_linear': True
            },
            'num_heads': {
                'pathways': ['flops', 'memory_bandwidth', 'energy', 'carbon'],
                'effect_size': 0.25,
                'non_linear': True
            },
            'pruning_rate': {
                'pathways': ['parameters', 'flops', 'accuracy', 'carbon'],
                'effect_size': 0.40,
                'non_linear': True
            },
            'quantization_bits': {
                'pathways': ['memory_bandwidth', 'energy', 'carbon'],
                'effect_size': 0.30,
                'non_linear': False
            },
            'batch_size': {
                'pathways': ['memory', 'throughput', 'energy', 'carbon'],
                'effect_size': 0.20,
                'non_linear': True
            }
        }
        
        # Historical causal effects (learned from past runs)
        self.historical_effects = defaultdict(lambda: defaultdict(float))
        self.confidence_scores = defaultdict(lambda: 0.5)
        
        # Load historical data if available
        self._load_historical_data()
    
    def _load_historical_data(self):
        """Load historical causal data from database"""
        try:
            # Would load from database in production
            pass
        except Exception as e:
            logger.debug(f"Could not load historical causal data: {e}")
    
    def explain_carbon_impact(self, 
                             architecture_config: Dict[str, Any],
                             fitness_metrics: Optional[Dict[str, float]] = None) -> CausalExplanation:
        """
        Provide causal explanation of carbon impact for a given architecture.
        """
        # Analyze which features most impact carbon
        impacts = {}
        pathways = {}
        
        for feature, impact_info in self.causal_graph.items():
            if feature in architecture_config:
                value = architecture_config[feature]
                effect = self._estimate_feature_impact(feature, value, impact_info)
                impacts[feature] = effect['contribution']
                pathways[feature] = effect['pathway']
        
        # Find primary driver
        primary_driver = max(impacts, key=impacts.get) if impacts else None
        confidence = self.confidence_scores.get(primary_driver, 0.5) if primary_driver else 0.3
        
        # Generate alternatives
        alternatives = self._generate_alternatives(architecture_config, primary_driver)
        
        return CausalExplanation(
            primary_driver=primary_driver or 'unknown',
            contribution=impacts.get(primary_driver, 0.0),
            pathway=pathways.get(primary_driver, []),
            alternatives=alternatives,
            confidence=confidence
        )
    
    def _estimate_feature_impact(self, feature: str, value: Any, impact_info: Dict) -> Dict:
        """Estimate impact of a specific feature on carbon"""
        base_effect = impact_info['effect_size']
        
        # Scale effect based on value
        if isinstance(value, (int, float)):
            # Normalize value to [0, 1] range for comparison
            if feature == 'num_layers':
                normalized = min(1.0, value / 20)
            elif feature == 'hidden_dim':
                normalized = min(1.0, value / 1024)
            elif feature == 'num_heads':
                normalized = min(1.0, value / 16)
            elif feature == 'pruning_rate':
                normalized = value  # Already 0-1
            elif feature == 'quantization_bits':
                normalized = 1.0 - (value / 32)  # Higher bits = more impact
            else:
                normalized = 0.5
            
            # Apply non-linearity if needed
            if impact_info.get('non_linear', False):
                effect = base_effect * (normalized ** 0.7)  # Diminishing returns
            else:
                effect = base_effect * normalized
        else:
            effect = base_effect * 0.5
        
        contribution = min(1.0, effect)
        
        return {
            'contribution': contribution,
            'pathway': impact_info['pathways']
        }
    
    def _generate_alternatives(self, config: Dict[str, Any], primary_driver: str) -> List[str]:
        """Generate alternative configurations for carbon reduction"""
        alternatives = []
        
        if primary_driver == 'num_layers' and config.get('num_layers', 0) > 6:
            alternatives.append(f"Reduce layers from {config['num_layers']} to {config['num_layers']-2} to save ~15% carbon")
        
        if primary_driver == 'hidden_dim' and config.get('hidden_dim', 0) > 384:
            alternatives.append(f"Reduce hidden dimension from {config['hidden_dim']} to {int(config['hidden_dim']*0.7)} to save ~12% carbon")
        
        if primary_driver == 'num_heads' and config.get('num_heads', 0) > 8:
            alternatives.append(f"Reduce attention heads from {config['num_heads']} to {config['num_heads']-2} to save ~10% carbon")
        
        if config.get('pruning_rate', 0) < 0.2:
            alternatives.append("Consider 20-30% pruning to reduce carbon by 15-20%")
        
        if config.get('quantization_bits', 32) == 32:
            alternatives.append("Apply INT8 quantization to reduce memory bandwidth and carbon")
        
        return alternatives[:3]  # Top 3 alternatives

# ============================================================================
# Ethical Reasoning: Fair and Responsible Optimization
# ============================================================================

class EthicalCarbonReasoner:
    """Reason about ethical implications of carbon reduction decisions"""
    
    def __init__(self):
        self.stakeholders = ['global_climate', 'local_community', 'organization', 'end_users']
        self.ethical_frameworks = {
            'utilitarian': self._utilitarian_assessment,
            'justice': self._justice_assessment,
            'deontological': self._deontological_assessment
        }
        
        # Historical ethical assessments
        self.assessment_history = deque(maxlen=100)
    
    def assess_reduction_impact(self, 
                               architecture_config: Dict[str, Any],
                               performance: Dict[str, float]) -> Dict[str, Any]:
        """
        Assess ethical impact of carbon reduction decisions.
        """
        assessment = {}
        
        # Run each ethical framework
        for framework_name, framework_func in self.ethical_frameworks.items():
            try:
                assessment[framework_name] = framework_func(architecture_config, performance)
            except Exception as e:
                logger.error(f"Ethical assessment error in {framework_name}: {e}")
                assessment[framework_name] = {
                    'score': 0.5,
                    'concern': f'Assessment unavailable: {str(e)}'
                }
        
        # Calculate overall ethical score
        overall_score = sum(assessment.get(fw, {}).get('score', 0.5) for fw in assessment) / len(assessment)
        
        # Store for learning
        self.assessment_history.append({
            'timestamp': datetime.now().isoformat(),
            'assessment': assessment,
            'overall_score': overall_score
        })
        
        return {
            'framework_assessments': assessment,
            'overall_ethical_score': overall_score,
            'recommendations': self._generate_ethical_recommendations(assessment),
            'timestamp': datetime.now().isoformat()
        }
    
    def _utilitarian_assessment(self, config: Dict, performance: Dict) -> Dict:
        """Greatest good for greatest number"""
        # Calculate benefits to different groups
        benefits = {
            'global_climate': 0.6,  # 60% benefit to global climate
            'local_community': 0.2,  # 20% benefit to local community
            'organization': 0.1,  # 10% benefit to organization
            'end_users': 0.1  # 10% benefit to end users
        }
        
        # Adjust based on actual reduction
        carbon_reduction = 1.0 - performance.get('carbon_kg', 0.01)  # Normalized
        if carbon_reduction < 0.3:
            benefits['global_climate'] *= 0.5  # Less benefit if carbon reduction is small
        
        # Potential losses
        accuracy_loss = 1.0 - performance.get('accuracy', 0.85)
        losses = {
            'end_users': accuracy_loss * 0.5,
            'organization': accuracy_loss * 0.3
        }
        
        net_benefit = sum(benefits.values()) - sum(losses.values())
        
        return {
            'score': max(0, min(1, net_benefit)),
            'benefits': benefits,
            'losses': losses,
            'net_benefit': net_benefit
        }
    
    def _justice_assessment(self, config: Dict, performance: Dict) -> Dict:
        """Fair distribution of benefits and burdens"""
        # Check equity of reduction
        pruning_rate = config.get('pruning_rate', 0)
        accuracy_loss = 1.0 - performance.get('accuracy', 0.85)
        
        # Heavy pruning might disproportionately affect accuracy
        if pruning_rate > 0.5 and accuracy_loss > 0.05:
            equity_score = 0.3
            concern = "Heavy pruning may disproportionately reduce accuracy for critical use cases"
        elif pruning_rate > 0.3 and accuracy_loss > 0.03:
            equity_score = 0.6
            concern = "Moderate pruning with some accuracy trade-off"
        else:
            equity_score = 0.9
            concern = "Balanced approach with minimal accuracy impact"
        
        # Check hardware accessibility (different hardware has different carbon footprints)
        hardware = config.get('target_hardware', 'cpu_x86')
        hardware_accessibility = {
            'cpu_x86': 0.9,
            'gpu_nvidia': 0.7,
            'edge_tpu': 0.5,
            'mobile_npu': 0.4
        }.get(hardware, 0.5)
        
        return {
            'score': (equity_score + hardware_accessibility) / 2,
            'equity_concern': concern,
            'hardware_accessibility': hardware_accessibility
        }
    
    def _deontological_assessment(self, config: Dict, performance: Dict) -> Dict:
        """Rule-based ethical assessment"""
        rules_violated = []
        
        # Rule 1: Don't harm accuracy more than necessary
        if config.get('pruning_rate', 0) > 0.5:
            rules_violated.append("Excessive pruning may harm accuracy unnecessarily")
        
        # Rule 2: Ensure transparency
        if config.get('compression') == 'none' and config.get('pruning_rate', 0) > 0:
            rules_violated.append("Compression without proper justification - lack of transparency")
        
        # Rule 3: Consider deployment context
        if config.get('target_hardware') == 'mobile_npu' and config.get('quantization_bits', 32) > 8:
            rules_violated.append("Mobile deployment requires more aggressive quantization")
        
        return {
            'score': 1.0 - (len(rules_violated) * 0.2),
            'rules_violated': rules_violated,
            'compliant': len(rules_violated) == 0
        }
    
    def _generate_ethical_recommendations(self, assessment: Dict) -> List[str]:
        """Generate ethical recommendations"""
        recommendations = []
        
        for framework, assessment_result in assessment.items():
            if framework == 'utilitarian' and assessment_result.get('net_benefit', 0) < 0.3:
                recommendations.append("Reconsider carbon reduction approach - current strategy may not maximize net benefit")
            
            if framework == 'justice' and assessment_result.get('equity_concern', ''):
                recommendations.append(f"Address equity concerns: {assessment_result['equity_concern']}")
            
            if framework == 'deontological' and not assessment_result.get('compliant', True):
                for violation in assessment_result.get('rules_violated', []):
                    recommendations.append(f"Address ethical violation: {violation}")
        
        return recommendations[:3]  # Top 3 recommendations

# ============================================================================
# Contextual Reasoning: Deployment-Aware Optimization
# ============================================================================

class ContextAwareOptimizer:
    """Apply different optimization strategies based on deployment context"""
    
    def __init__(self):
        self.context_strategies = {
            'mobile_inference': {
                'max_size_mb': 50,
                'max_latency_ms': 10,
                'min_accuracy': 0.85,
                'max_carbon_g': 0.1,
                'priority': ['size', 'latency', 'carbon', 'accuracy'],
                'recommended_compression': ['quantization_int8', 'pruning_structured'],
                'max_pruning_rate': 0.4,
                'min_quantization_bits': 8
            },
            'cloud_inference': {
                'max_size_mb': 1000,
                'max_latency_ms': 100,
                'min_accuracy': 0.92,
                'max_carbon_g': 1.0,
                'priority': ['accuracy', 'throughput', 'carbon', 'size'],
                'recommended_compression': ['pruning_unstructured', 'quantization_fp16'],
                'max_pruning_rate': 0.3,
                'min_quantization_bits': 16
            },
            'edge_tpu': {
                'max_size_mb': 10,
                'max_latency_ms': 5,
                'min_accuracy': 0.80,
                'max_carbon_g': 0.01,
                'priority': ['size', 'latency', 'carbon'],
                'recommended_compression': ['quantization_int8', 'pruning_structured'],
                'max_pruning_rate': 0.5,
                'min_quantization_bits': 8
            },
            'batch_processing': {
                'max_size_mb': 5000,
                'max_latency_ms': 5000,
                'min_accuracy': 0.85,
                'max_carbon_g': 10.0,
                'priority': ['throughput', 'carbon', 'accuracy'],
                'recommended_compression': ['pruning_unstructured', 'quantization_fp16'],
                'max_pruning_rate': 0.4,
                'min_quantization_bits': 16
            },
            'quantum': {
                'max_size_mb': 1,
                'max_latency_ms': 1000,
                'min_accuracy': 0.70,
                'max_carbon_g': 0.001,
                'priority': ['carbon', 'size'],
                'recommended_compression': ['quantization_int8'],
                'max_pruning_rate': 0.6,
                'min_quantization_bits': 8
            }
        }
    
    def get_context_plan(self, 
                        architecture_config: Dict[str, Any],
                        context: str = 'cloud_inference') -> Dict[str, Any]:
        """
        Get context-specific optimization plan for an architecture.
        """
        strategy = self.context_strategies.get(context, self.context_strategies['cloud_inference'])
        
        # Check constraints
        constraints = {
            'size_ok': architecture_config.get('hidden_dim', 512) * architecture_config.get('num_layers', 6) / 1024 < strategy['max_size_mb'],
            'latency_ok': True,  # Would need real latency estimation
            'accuracy_ok': True,  # Would need real accuracy prediction
            'carbon_ok': True  # Would need real carbon estimation
        }
        
        # Generate optimization suggestions
        suggestions = []
        
        # Check if compression is needed
        current_pruning = architecture_config.get('pruning_rate', 0)
        if current_pruning < strategy['max_pruning_rate'] * 0.5:
            suggestions.append({
                'action': 'increase_pruning',
                'from': current_pruning,
                'to': strategy['max_pruning_rate'] * 0.7,
                'reason': f"{context} deployment requires aggressive pruning"
            })
        
        # Check quantization
        current_quantization = architecture_config.get('quantization_bits', 32)
        if current_quantization > strategy['min_quantization_bits']:
            suggestions.append({
                'action': 'quantize',
                'from': current_quantization,
                'to': strategy['min_quantization_bits'],
                'reason': f"{context} deployment benefits from lower precision"
            })
        
        # Check architecture family
        if context in ['edge_tpu', 'mobile_inference'] and architecture_config.get('family') in ['transformer', 'vit']:
            suggestions.append({
                'action': 'change_family',
                'from': architecture_config.get('family'),
                'to': 'cnn',
                'reason': f"{context} deployment favors CNN over transformer models"
            })
        
        return {
            'context': context,
            'strategy': strategy,
            'constraints_met': all(constraints.values()),
            'constraints': constraints,
            'suggestions': suggestions[:3],  # Top 3 suggestions
            'priority_order': strategy['priority'],
            'recommended_compression': strategy['recommended_compression']
        }

# ============================================================================
# Systemic Reasoning: Long-term Carbon Planning
# ============================================================================

class SystemicCarbonPlanner:
    """Plan carbon reduction across multiple NAS runs"""
    
    def __init__(self):
        self.carbon_history = deque(maxlen=100)
        self.accuracy_history = deque(maxlen=100)
        self.investment_returns = defaultdict(list)
        
        # Load historical data
        self._load_historical_data()
    
    def _load_historical_data(self):
        """Load historical planning data"""
        try:
            # Would load from database in production
            pass
        except Exception as e:
            logger.debug(f"Could not load systemic planning data: {e}")
    
    def plan_carbon_investment(self, 
                              current_accuracy: float,
                              target_accuracy: float,
                              carbon_budget: float) -> Dict[str, Any]:
        """
        Decide how much carbon to invest in exploration.
        """
        # Estimate improvement potential
        improvement_gap = max(0, target_accuracy - current_accuracy)
        
        # Estimate compute needed for improvement (diminishing returns)
        compute_need = self._estimate_compute_for_improvement(improvement_gap)
        
        # Calculate carbon cost
        carbon_cost = compute_need * 0.001  # kg per unit compute
        
        # Estimate long-term savings
        long_term_savings = self._estimate_long_term_savings(current_accuracy, target_accuracy)
        
        # Calculate ROI
        roi = (long_term_savings - carbon_cost) / carbon_cost if carbon_cost > 0 else 0
        
        # Investment decision
        if roi > 0.5:
            decision = 'invest'
            reason = f'High ROI ({roi:.2f}) - invest in exploration'
        elif roi > 0.1:
            decision = 'balanced'
            reason = f'Moderate ROI ({roi:.2f}) - balanced approach'
        else:
            decision = 'save'
            reason = f'Low ROI ({roi:.2f}) - save carbon for future'
        
        # Store for learning
        self.investment_returns[decision].append({
            'roi': roi,
            'accuracy_gain': improvement_gap,
            'carbon_cost': carbon_cost
        })
        
        return {
            'decision': decision,
            'reason': reason,
            'carbon_cost': carbon_cost,
            'long_term_savings': long_term_savings,
            'roi': roi,
            'compute_need': compute_need,
            'estimated_generations': int(compute_need / 0.5),  # 0.5 units per generation
            'recommendation': self._generate_planning_recommendation(decision, roi)
        }
    
    def _estimate_compute_for_improvement(self, improvement_gap: float) -> float:
        """Estimate compute needed for accuracy improvement"""
        # More improvement requires exponentially more compute
        # 0.05 improvement -> 5 units, 0.10 -> 10 units, 0.20 -> 30 units
        if improvement_gap <= 0:
            return 1.0
        return max(1.0, improvement_gap * 10)
    
    def _estimate_long_term_savings(self, current_accuracy: float, target_accuracy: float) -> float:
        """Estimate long-term carbon savings from better architecture"""
        # Better architecture -> less inference carbon over lifetime
        efficiency_gain = max(0, (target_accuracy - current_accuracy) * 0.1)
        lifetime_inferences = 1e6  # Million inferences
        base_inference_carbon = 0.0001  # kg per inference
        
        return efficiency_gain * lifetime_inferences * base_inference_carbon
    
    def _generate_planning_recommendation(self, decision: str, roi: float) -> str:
        """Generate planning recommendation"""
        if decision == 'invest':
            return f"Invest in exploration - ROI of {roi:.2f} justifies the carbon expenditure"
        elif decision == 'balanced':
            return f"Take a balanced approach - moderate ROI of {roi:.2f}, consider limited exploration"
        else:
            return f"Conserve carbon - low ROI of {roi:.2f}, focus on consolidating current gains"

# ============================================================================
# Reflexive Reasoning: Purpose-Aware Optimization
# ============================================================================

class PurposeAwareOptimizer:
    """Understand why carbon reduction matters for specific use cases"""
    
    def __init__(self):
        self.purposes = {
            'climate_research': {
                'priority': 'maximum_reduction',
                'accuracy_tolerance': 0.10,
                'transparency': 'high',
                'carbon_priority': 0.8,
                'description': 'Maximize carbon reduction even at cost of accuracy'
            },
            'medical_diagnosis': {
                'priority': 'accuracy_first',
                'accuracy_tolerance': 0.01,
                'transparency': 'critical',
                'carbon_priority': 0.3,
                'description': 'Maintain high accuracy, reduce carbon only where possible'
            },
            'consumer_app': {
                'priority': 'balanced',
                'accuracy_tolerance': 0.05,
                'transparency': 'medium',
                'carbon_priority': 0.5,
                'description': 'Balance between accuracy and carbon reduction'
            },
            'research_exploration': {
                'priority': 'exploration',
                'accuracy_tolerance': 0.15,
                'transparency': 'low',
                'carbon_priority': 0.6,
                'description': 'Prioritize exploration of novel architectures'
            },
            'production_deployment': {
                'priority': 'reliability',
                'accuracy_tolerance': 0.03,
                'transparency': 'high',
                'carbon_priority': 0.4,
                'description': 'Focus on reliable performance with some carbon reduction'
            }
        }
        
        self.purpose_history = deque(maxlen=100)
    
    def get_purpose_guide(self, purpose: str = 'balanced') -> Dict[str, Any]:
        """
        Get optimization guidance based on purpose.
        """
        guide = self.purposes.get(purpose, self.purposes['balanced'])
        
        # Generate specific recommendations
        recommendations = []
        
        if guide['priority'] == 'maximum_reduction':
            recommendations.append("Aggressively prune and quantize (target 40-50% reduction)")
            recommendations.append("Consider knowledge distillation for energy savings")
            recommendations.append("Prioritize carbon reduction over accuracy")
        elif guide['priority'] == 'accuracy_first':
            recommendations.append("Conservative pruning only (max 10-15%)")
            recommendations.append("Use FP16 quantization instead of INT8")
            recommendations.append("Maintain accuracy above all else")
        elif guide['priority'] == 'exploration':
            recommendations.append("Generate diverse architectures even if some are inefficient")
            recommendations.append("Balance carbon budget for maximum exploration")
            recommendations.append("Document all architecture attempts regardless of efficiency")
        elif guide['priority'] == 'reliability':
            recommendations.append("Focus on proven architecture families")
            recommendations.append("Apply moderate compression with safety margins")
            recommendations.append("Ensure reproducibility of results")
        else:  # balanced
            recommendations.append("Apply moderate compression (20-30%)")
            recommendations.append("Test both INT8 and FP16 quantization")
            recommendations.append("Balance carbon reduction with accuracy maintenance")
        
        # Add transparency requirements
        if guide['transparency'] == 'critical':
            recommendations.append("Generate detailed explainability reports for all decisions")
            recommendations.append("Document all carbon reduction choices and their impact")
            recommendations.append("Provide audit trail for all architectural decisions")
        elif guide['transparency'] == 'high':
            recommendations.append("Document key carbon reduction decisions")
            recommendations.append("Provide clear reasoning for architectural choices")
        
        return {
            'purpose': purpose,
            'guide': guide,
            'recommendations': recommendations,
            'optimization_weight': {
                'accuracy': 1.0 - guide['carbon_priority'],
                'carbon': guide['carbon_priority']
            },
            'acceptable_accuracy_loss': guide['accuracy_tolerance']
        }
    
    def reflect_on_purpose(self, 
                          purpose: str,
                          outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reflect on how well the purpose was achieved.
        """
        guide = self.purposes.get(purpose, self.purposes['balanced'])
        
        # Assess achievement
        accuracy_achieved = outcomes.get('accuracy', 0)
        carbon_achieved = outcomes.get('carbon_reduction', 0)
        
        # Compare with expectations
        accuracy_gap = max(0, guide['accuracy_tolerance'] - accuracy_achieved)
        carbon_gap = guide['carbon_priority'] - carbon_achieved
        
        # Store reflection
        reflection = {
            'timestamp': datetime.now().isoformat(),
            'purpose': purpose,
            'accuracy_achieved': accuracy_achieved,
            'carbon_achieved': carbon_achieved,
            'accuracy_gap': accuracy_gap,
            'carbon_gap': carbon_gap,
            'purpose_achieved': accuracy_gap <= 0 and carbon_gap <= 0,
            'lessons': []
        }
        
        # Generate lessons
        if accuracy_gap > 0:
            reflection['lessons'].append(f"Accuracy gap: {accuracy_gap:.2f} - consider less aggressive compression")
        if carbon_gap > 0:
            reflection['lessons'].append(f"Carbon gap: {carbon_gap:.2f} - consider more aggressive optimization")
        
        if reflection['purpose_achieved']:
            reflection['lessons'].append("Purpose achieved - continue with current strategy")
        else:
            reflection['lessons'].append("Purpose not fully achieved - adjust optimization strategy")
        
        self.purpose_history.append(reflection)
        
        return reflection

# ============================================================================
# Main Reasoning Engine
# ============================================================================

class GreenAgentReasoningEngine:
    """
    Unified reasoning engine integrating all reasoning capabilities.
    """
    
    def __init__(self):
        self.scheduler = CarbonIntensityAwareScheduler()
        self.causal_model = CarbonCausalModel()
        self.ethical_reasoner = EthicalCarbonReasoner()
        self.context_optimizer = ContextAwareOptimizer()
        self.planner = SystemicCarbonPlanner()
        self.purpose_optimizer = PurposeAwareOptimizer()
        
        self.reasoning_history = deque(maxlen=1000)
        self.enabled = True
        
        logger.info("GreenAgentReasoningEngine initialized")
    
    async def reason_about_architecture(self,
                                       architecture_config: Dict[str, Any],
                                       fitness_metrics: Dict[str, float],
                                       context: str = 'cloud_inference',
                                       purpose: str = 'balanced') -> Dict[str, Any]:
        """
        Apply all reasoning capabilities to an architecture.
        """
        if not self.enabled:
            return {'reasoning': 'disabled'}
        
        reasoning_result = {
            'timestamp': datetime.now().isoformat(),
            'architecture_hash': hashlib.md5(json.dumps(architecture_config).encode()).hexdigest()[:8],
            'context': context,
            'purpose': purpose
        }
        
        # Temporal reasoning
        scheduling = await self.scheduler.schedule_computation(
            task='architecture_evaluation',
            urgency='normal',
            compute_hours=1.0
        )
        reasoning_result['temporal'] = scheduling
        
        # Causal reasoning
        causal = self.causal_model.explain_carbon_impact(architecture_config, fitness_metrics)
        reasoning_result['causal'] = {
            'primary_driver': causal.primary_driver,
            'contribution': causal.contribution,
            'pathway': causal.pathway,
            'alternatives': causal.alternatives,
            'confidence': causal.confidence
        }
        
        # Ethical reasoning
        ethical = self.ethical_reasoner.assess_reduction_impact(architecture_config, fitness_metrics)
        reasoning_result['ethical'] = ethical
        
        # Contextual reasoning
        context_plan = self.context_optimizer.get_context_plan(architecture_config, context)
        reasoning_result['contextual'] = context_plan
        
        # Systemic planning
        systemic = self.planner.plan_carbon_investment(
            current_accuracy=fitness_metrics.get('accuracy', 0.85),
            target_accuracy=0.90,
            carbon_budget=10.0  # kg
        )
        reasoning_result['systemic'] = systemic
        
        # Reflexive reasoning
        reflexive = self.purpose_optimizer.get_purpose_guide(purpose)
        reasoning_result['reflexive'] = reflexive
        
        # Store reasoning history
        self.reasoning_history.append(reasoning_result)
        
        # Generate overall recommendations
        reasoning_result['overall_recommendations'] = self._generate_recommendations(reasoning_result)
        
        return reasoning_result
    
    def _generate_recommendations(self, reasoning_result: Dict) -> List[str]:
        """Generate overall recommendations from all reasoning"""
        recommendations = []
        
        # Temporal recommendations
        if reasoning_result.get('temporal', {}).get('action') == 'schedule':
            recommendations.append(f"Schedule evaluation for better carbon timing: {reasoning_result['temporal'].get('schedule', 'unknown')}")
        
        # Causal recommendations
        causal_alternatives = reasoning_result.get('causal', {}).get('alternatives', [])
        if causal_alternatives:
            recommendations.append(f"Causal alternative: {causal_alternatives[0]}")
        
        # Ethical recommendations
        ethical_recommendations = reasoning_result.get('ethical', {}).get('recommendations', [])
        if ethical_recommendations:
            recommendations.extend(ethical_recommendations)
        
        # Contextual recommendations
        contextual_suggestions = reasoning_result.get('contextual', {}).get('suggestions', [])
        for suggestion in contextual_suggestions[:2]:
            recommendations.append(f"Contextual suggestion: {suggestion.get('action')} ({suggestion.get('reason')})")
        
        # Systemic recommendations
        if reasoning_result.get('systemic', {}).get('decision') == 'invest':
            recommendations.append("Systemic decision: Invest in exploration - high ROI expected")
        
        # Reflexive recommendations
        reflexive_recommendations = reasoning_result.get('reflexive', {}).get('recommendations', [])
        if reflexive_recommendations:
            recommendations.extend(reflexive_recommendations[:2])
        
        return recommendations[:5]  # Top 5 recommendations
    
    async def get_reasoning_summary(self) -> Dict[str, Any]:
        """Get summary of reasoning history"""
        if not self.reasoning_history:
            return {'status': 'no_reasoning_history'}
        
        recent = list(self.reasoning_history)[-20:]
        
        # Aggregate recommendations
        all_recommendations = []
        for entry in recent:
            all_recommendations.extend(entry.get('overall_recommendations', []))
        
        return {
            'total_reasoned_architectures': len(self.reasoning_history),
            'recent_recommendations': all_recommendations[:10],
            'average_ethical_score': np.mean([
                entry.get('ethical', {}).get('overall_ethical_score', 0.5)
                for entry in recent
            ]),
            'most_common_causal_driver': self._get_most_common_causal_driver(recent),
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_most_common_causal_driver(self, recent_entries: List[Dict]) -> str:
        """Get most common causal driver from recent reasoning"""
        drivers = []
        for entry in recent_entries:
            causal = entry.get('causal', {})
            if causal.get('primary_driver'):
                drivers.append(causal['primary_driver'])
        
        if not drivers:
            return 'unknown'
        
        from collections import Counter
        return Counter(drivers).most_common(1)[0][0]
    
    async def shutdown(self):
        """Clean shutdown"""
        self.enabled = False
        logger.info("GreenAgentReasoningEngine shutdown complete")
