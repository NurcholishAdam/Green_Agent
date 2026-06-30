# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/sustainability/unified_sustainability_engine.py
"""
Unified Sustainability Valuation Engine for Green Agent v2.0.0
Creates a single, authoritative global sustainability function
that aggregates all dimensions (carbon, helium, energy, circularity, biodiversity).

Enhanced Features:
- Dynamic weight adjustment based on real-time resource scarcity
- Predictive trend analysis with ensemble forecasting
- Adaptive thresholds based on historical performance
- Circularity metrics integration
- Customizable report templates for stakeholders

Integrates with:
- expert_registry.py (FitnessScore)
- gating_network.py (expert selection)
- quantum_limit_integration.py (resource limits)
- circular_computing.py (circularity metrics)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import deque
import json
import hashlib

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes (Enhanced)
# ============================================================================

@dataclass
class SustainabilityDimension:
    """A single dimension of sustainability with enhanced tracking"""
    name: str
    current_value: float
    target_value: float
    weight: float
    units: str
    trend: str = "stable"  # improving, stable, declining
    confidence: float = 0.8
    # NEW: Enhanced tracking
    scarcity_factor: float = 1.0
    historical_weights: List[float] = field(default_factory=list)
    volatility: float = 0.0
    prediction: float = 0.0
    prediction_confidence: float = 0.0
    last_update: Optional[datetime] = None

@dataclass
class UnifiedSustainabilityScore:
    """Unified sustainability score with components"""
    total_score: float  # 0-1
    dimensions: Dict[str, SustainabilityDimension]
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    confidence: float = 0.8
    trend: str = "stable"
    risk_factors: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    # NEW: Enhanced reporting
    predicted_future_score: Optional[float] = None
    scenario_scores: Dict[str, float] = field(default_factory=dict)
    report_template: str = "standard"

@dataclass
class SustainabilityThreshold:
    """Threshold for sustainability alerts with adaptive limits"""
    dimension: str
    warning_threshold: float
    critical_threshold: float
    current_value: float = 0.0
    status: str = "unknown"  # healthy, warning, critical
    # NEW: Adaptive thresholds
    adaptive_warning: float = 0.0
    adaptive_critical: float = 0.0
    historical_avg: float = 0.0
    history_std: float = 0.0
    alert_count: int = 0

@dataclass
class ReportTemplate:
    """Customizable report template for stakeholders"""
    name: str
    description: str
    included_dimensions: List[str]
    metrics: List[str]
    format: str = "json"
    frequency: str = "daily"
    target_audience: str = "general"
    customization: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# Adaptive Threshold Manager (NEW)
# ============================================================================

class AdaptiveThresholdManager:
    """
    Adaptive thresholds based on historical performance.
    
    Features:
    - Statistical baseline calculation
    - Anomaly detection
    - Threshold adjustment based on performance trends
    - Alert escalation management
    """
    
    def __init__(self, window_size: int = 100, adaptation_rate: float = 0.1):
        self.window_size = window_size
        self.adaptation_rate = adaptation_rate
        self.historical_values: Dict[str, deque] = {}
        self.threshold_history: Dict[str, List[Dict]] = defaultdict(list)
        self._lock = asyncio.Lock()
        
        logger.info("Adaptive Threshold Manager initialized")
    
    async def update_thresholds(
        self,
        dimension: str,
        current_value: float,
        base_warning: float,
        base_critical: float
    ) -> Tuple[float, float]:
        """Update adaptive thresholds based on historical performance"""
        async with self._lock:
            if dimension not in self.historical_values:
                self.historical_values[dimension] = deque(maxlen=self.window_size)
            
            self.historical_values[dimension].append(current_value)
            
            if len(self.historical_values[dimension]) < 10:
                return base_warning, base_critical
            
            # Calculate statistical baseline
            values = list(self.historical_values[dimension])
            mean = np.mean(values)
            std = np.std(values)
            
            # Adjust thresholds based on performance
            if mean < base_warning * 0.5:
                # System is performing poorly, make thresholds stricter
                adjustment = 1.0 - self.adaptation_rate
            elif mean > base_warning * 1.2:
                # System is performing well, can be more lenient
                adjustment = 1.0 + self.adaptation_rate
            else:
                adjustment = 1.0
            
            # Calculate adaptive thresholds
            adaptive_warning = min(1.0, base_warning * adjustment)
            adaptive_critical = min(0.5, base_critical * adjustment)
            
            # Record threshold history
            self.threshold_history[dimension].append({
                'timestamp': datetime.utcnow().isoformat(),
                'current_value': current_value,
                'mean': mean,
                'std': std,
                'adaptive_warning': adaptive_warning,
                'adaptive_critical': adaptive_critical,
                'adjustment': adjustment
            })
            
            return adaptive_warning, adaptive_critical
    
    def get_anomaly_score(self, dimension: str, value: float) -> float:
        """Calculate anomaly score for a value (0-1, higher = more anomalous)"""
        if dimension not in self.historical_values or len(self.historical_values[dimension]) < 10:
            return 0.0
        
        values = list(self.historical_values[dimension])
        mean = np.mean(values)
        std = np.std(values)
        
        if std < 0.001:
            return 0.0
        
        z_score = abs(value - mean) / std
        return min(1.0, z_score / 5.0)
    
    def get_threshold_stats(self, dimension: str) -> Dict[str, Any]:
        """Get threshold statistics for a dimension"""
        if dimension not in self.historical_values:
            return {'status': 'insufficient_data'}
        
        values = list(self.historical_values[dimension])
        return {
            'sample_count': len(values),
            'mean': np.mean(values),
            'std': np.std(values),
            'min': min(values),
            'max': max(values),
            'percentile_25': np.percentile(values, 25),
            'percentile_75': np.percentile(values, 75),
            'trend': 'improving' if values[-1] > values[0] * 1.05 else 'declining' if values[-1] < values[0] * 0.95 else 'stable'
        }

# ============================================================================
# Dynamic Weight Manager (NEW)
# ============================================================================

class DynamicWeightManager:
    """
    Dynamic weight adjustment based on real-time resource scarcity.
    
    Features:
    - Scarcity-based weight adjustment
    - Historical weight tracking
    - Weight normalization
    - Priority scoring
    """
    
    def __init__(self, base_weights: Dict[str, float]):
        self.base_weights = base_weights.copy()
        self.current_weights = base_weights.copy()
        self.weight_history: Dict[str, List[float]] = defaultdict(list)
        self.scarcity_factors: Dict[str, float] = {dim: 1.0 for dim in base_weights}
        self._lock = asyncio.Lock()
        
        # Normalization factor to ensure weights sum to 1.0
        self.normalization_factor = 1.0 / sum(base_weights.values())
        
        logger.info("Dynamic Weight Manager initialized")
    
    async def update_weights(
        self,
        dimension_scores: Dict[str, float],
        scarcity_factors: Dict[str, float]
    ) -> Dict[str, float]:
        """Update weights based on scarcity and performance"""
        async with self._lock:
            # Update scarcity factors
            for dim, factor in scarcity_factors.items():
                if dim in self.scarcity_factors:
                    self.scarcity_factors[dim] = factor
            
            # Calculate new weights
            adjusted_weights = {}
            total_adjusted = 0.0
            
            for dim, base_weight in self.base_weights.items():
                scarcity = self.scarcity_factors.get(dim, 1.0)
                performance = dimension_scores.get(dim, 0.5)
                
                # Higher scarcity = higher weight
                # Lower performance = higher weight (needs attention)
                weight_factor = (scarcity * 0.7 + (1.0 - performance) * 0.3)
                adjusted_weight = base_weight * weight_factor
                
                adjusted_weights[dim] = adjusted_weight
                total_adjusted += adjusted_weight
            
            # Normalize weights to sum to 1.0
            if total_adjusted > 0:
                for dim in adjusted_weights:
                    adjusted_weights[dim] /= total_adjusted
                    self.weight_history[dim].append(adjusted_weights[dim])
            
            self.current_weights = adjusted_weights
            return adjusted_weights
    
    def get_current_weights(self) -> Dict[str, float]:
        """Get current weights"""
        return self.current_weights.copy()
    
    def get_weight_trends(self) -> Dict[str, Any]:
        """Get weight trends over time"""
        trends = {}
        for dim, history in self.weight_history.items():
            if len(history) > 5:
                slope = np.polyfit(range(len(history[-10:])), history[-10:], 1)[0] if len(history) >= 10 else 0
                trends[dim] = {
                    'current': history[-1] if history else self.base_weights.get(dim, 0),
                    'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                    'slope': slope,
                    'history_length': len(history)
                }
            else:
                trends[dim] = {'current': self.base_weights.get(dim, 0), 'trend': 'stable'}
        
        return trends

# ============================================================================
# Predictive Trend Analyzer (NEW)
# ============================================================================

class PredictiveTrendAnalyzer:
    """
    Predictive trend analysis with ensemble forecasting.
    
    Features:
    - Multiple forecasting models
    - Ensemble prediction
    - Confidence scoring
    - Scenario analysis
    """
    
    def __init__(self, window_size: int = 50):
        self.window_size = window_size
        self.historical_data: Dict[str, List[float]] = {}
        self.predictions: Dict[str, List[float]] = {}
        self.model_weights: Dict[str, float] = {'linear': 0.4, 'exponential': 0.3, 'moving_average': 0.3}
        self._lock = asyncio.Lock()
        
        logger.info("Predictive Trend Analyzer initialized")
    
    async def update_model(
        self,
        dimension: str,
        values: List[float]
    ):
        """Update predictive model with new data"""
        async with self._lock:
            if dimension not in self.historical_data:
                self.historical_data[dimension] = []
            
            self.historical_data[dimension].extend(values)
            
            # Keep only last window_size values
            if len(self.historical_data[dimension]) > self.window_size:
                self.historical_data[dimension] = self.historical_data[dimension][-self.window_size:]
    
    async def predict(
        self,
        dimension: str,
        horizon_steps: int = 10
    ) -> Tuple[float, float, float]:
        """
        Predict future values using ensemble forecasting.
        
        Returns:
            (prediction, confidence, volatility)
        """
        if dimension not in self.historical_data or len(self.historical_data[dimension]) < 10:
            return 0.5, 0.0, 0.0
        
        async with self._lock:
            data = self.historical_data[dimension]
            n = len(data)
            
            # Linear prediction
            x = np.array(range(n))
            y = np.array(data)
            linear_coeffs = np.polyfit(x, y, 1)
            linear_prediction = linear_coeffs[0] * (n + horizon_steps - 1) + linear_coeffs[1]
            
            # Exponential smoothing
            alpha = 0.3
            exp_forecast = data[-1]
            for i in range(horizon_steps):
                exp_forecast = alpha * data[-1] + (1 - alpha) * exp_forecast
            
            # Moving average
            window = min(10, n)
            ma_forecast = np.mean(data[-window:])
            
            # Ensemble prediction
            ensemble_prediction = (
                self.model_weights['linear'] * linear_prediction +
                self.model_weights['exponential'] * exp_forecast +
                self.model_weights['moving_average'] * ma_forecast
            )
            
            # Clamp to valid range
            ensemble_prediction = max(0.0, min(1.0, ensemble_prediction))
            
            # Calculate confidence based on prediction variance
            predictions = [linear_prediction, exp_forecast, ma_forecast]
            variance = np.var(predictions)
            confidence = max(0.0, min(1.0, 1.0 - variance * 10))
            
            # Calculate volatility
            volatility = np.std(data[-10:]) if len(data) >= 10 else 0.0
            
            # Store prediction
            if dimension not in self.predictions:
                self.predictions[dimension] = []
            self.predictions[dimension].append(ensemble_prediction)
            
            return ensemble_prediction, confidence, volatility
    
    async def predict_scenario(
        self,
        dimension: str,
        scenario_type: str,  # 'optimistic', 'pessimistic', 'most_likely'
        horizon_steps: int = 10
    ) -> float:
        """Predict future values under different scenarios"""
        if dimension not in self.historical_data or len(self.historical_data[dimension]) < 10:
            return 0.5
        
        data = self.historical_data[dimension]
        current = data[-1]
        
        if scenario_type == 'optimistic':
            # Assume improvement trend continues
            improvement = 0.1 * (1 + horizon_steps / 20)
            return min(1.0, current + improvement)
        elif scenario_type == 'pessimistic':
            # Assume decline continues
            decline = 0.1 * (1 + horizon_steps / 20)
            return max(0.0, current - decline)
        else:  # most_likely
            # Use ensemble prediction
            prediction, _, _ = await self.predict(dimension, horizon_steps)
            return prediction
    
    def get_prediction_accuracy(self, dimension: str) -> float:
        """Calculate prediction accuracy based on historical predictions"""
        if dimension not in self.predictions or len(self.predictions[dimension]) < 5:
            return 0.0
        
        predictions = self.predictions[dimension]
        actual = self.historical_data[dimension][-len(predictions):]
        
        errors = [abs(p - a) / max(a, 0.01) for p, a in zip(predictions, actual)]
        accuracy = 1.0 - min(1.0, np.mean(errors))
        
        return accuracy

# ============================================================================
# Report Template Manager (NEW)
# ============================================================================

class ReportTemplateManager:
    """
    Customizable report templates for different stakeholders.
    
    Features:
    - Template creation and management
    - Customizable dimensions and metrics
    - Multiple output formats
    - Scheduled report generation
    """
    
    def __init__(self):
        self.templates: Dict[str, ReportTemplate] = {}
        self.generated_reports: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        
        # Initialize default templates
        self._init_default_templates()
        
        logger.info("Report Template Manager initialized")
    
    def _init_default_templates(self):
        """Initialize default report templates"""
        default_templates = [
            ReportTemplate(
                name="executive_summary",
                description="High-level sustainability overview for executives",
                included_dimensions=["carbon", "helium", "energy", "circularity", "biodiversity"],
                metrics=["total_score", "trend", "risk_factors"],
                format="json",
                frequency="daily",
                target_audience="executives",
                customization={"show_confidence": True, "show_recommendations": True}
            ),
            ReportTemplate(
                name="technical_detailed",
                description="Detailed sustainability metrics for technical teams",
                included_dimensions=["carbon", "helium", "energy", "circularity", "biodiversity"],
                metrics=["all"],
                format="json",
                frequency="daily",
                target_audience="engineers",
                customization={"show_all_metrics": True, "include_raw_data": True}
            ),
            ReportTemplate(
                name="compliance",
                description="Compliance-focused sustainability report",
                included_dimensions=["carbon", "energy", "circularity"],
                metrics=["total_score", "trend", "threshold_status"],
                format="json",
                frequency="weekly",
                target_audience="compliance_officers",
                customization={"threshold_emphasis": True, "include_audit_trail": True}
            ),
            ReportTemplate(
                name="sustainability_summary",
                description="Summary of sustainability performance for stakeholders",
                included_dimensions=["carbon", "helium", "circularity"],
                metrics=["total_score", "trend", "recommendations"],
                format="json",
                frequency="weekly",
                target_audience="investors",
                customization={"show_roi_metrics": True, "include_benchmarks": True}
            )
        ]
        
        for template in default_templates:
            self.templates[template.name] = template
    
    def create_template(self, template: ReportTemplate) -> bool:
        """Create a new report template"""
        if template.name in self.templates:
            logger.warning(f"Template {template.name} already exists")
            return False
        
        self.templates[template.name] = template
        logger.info(f"Created report template: {template.name}")
        return True
    
    def get_template(self, name: str) -> Optional[ReportTemplate]:
        """Get a report template by name"""
        return self.templates.get(name)
    
    def list_templates(self) -> List[str]:
        """List all available report templates"""
        return list(self.templates.keys())
    
    async def generate_report(
        self,
        template_name: str,
        data: Dict[str, Any],
        output_format: str = "json"
    ) -> Dict[str, Any]:
        """Generate a report using a template"""
        template = self.get_template(template_name)
        if not template:
            return {'status': 'error', 'message': f'Template {template_name} not found'}
        
        # Filter data based on template
        filtered_data = {}
        for dim in template.included_dimensions:
            if dim in data:
                filtered_data[dim] = data[dim]
        
        # Add requested metrics
        report = {
            'template': template_name,
            'generated_at': datetime.utcnow().isoformat(),
            'target_audience': template.target_audience,
            'dimensions': filtered_data,
            'customization': template.customization,
            'status': 'generated'
        }
        
        # Add total score if available
        if 'total_score' in data:
            report['total_score'] = data['total_score']
        
        # Add recommendations if requested
        if template.customization.get('show_recommendations', False) and 'recommendations' in data:
            report['recommendations'] = data['recommendations']
        
        # Store generated report
        report_id = hashlib.md5(
            f"{template_name}_{datetime.utcnow().timestamp()}".encode()
        ).hexdigest()[:12]
        self.generated_reports[report_id] = report
        
        return report

# ============================================================================
# Unified Sustainability Engine (Enhanced)
# ============================================================================

class UnifiedSustainabilityEngine:
    """
    Unified Sustainability Valuation Engine v2.0.0.
    
    Enhanced Features:
    - Dynamic weight adjustment based on real-time resource scarcity
    - Predictive trend analysis with ensemble forecasting
    - Adaptive thresholds based on historical performance
    - Circularity metrics integration
    - Customizable report templates for stakeholders
    """
    
    def __init__(self):
        self.sustainability_score = 0.5
        self.dimensions: Dict[str, SustainabilityDimension] = {}
        self.thresholds: Dict[str, SustainabilityThreshold] = {}
        self.history: deque = deque(maxlen=10000)
        self.last_update: Optional[datetime] = None
        
        # Weights for each dimension
        self.dimension_weights = {
            'carbon': 0.25,
            'helium': 0.20,
            'energy': 0.15,
            'circularity': 0.25,
            'biodiversity': 0.15
        }
        
        # NEW: Enhanced managers
        self.adaptive_threshold_manager = AdaptiveThresholdManager()
        self.dynamic_weight_manager = DynamicWeightManager(self.dimension_weights)
        self.predictive_analyzer = PredictiveTrendAnalyzer()
        self.report_manager = ReportTemplateManager()
        
        # Scarcity factors (injected or calculated)
        self.scarcity_factors = {
            'carbon': 1.0,
            'helium': 1.0,
            'energy': 1.0,
            'circularity': 1.0,
            'biodiversity': 1.0
        }
        
        # Initialize thresholds
        self._init_thresholds()
        
        # Sub-modules (will be injected)
        self.carbon_manager = None
        self.helium_tracker = None
        self.circular_manager = None
        self.biodiversity = None
        self.expert_registry = None
        self.quantum_limits = None
        
        # NEW: Historical dimension values for prediction
        self.dimension_history: Dict[str, List[float]] = defaultdict(list)
        
        logger.info("Unified Sustainability Engine v2.0.0 initialized")
    
    # ========================================================================
    # Module Injection
    # ========================================================================
    
    def inject_modules(self, **modules):
        """Inject required system modules"""
        for name, module in modules.items():
            setattr(self, name, module)
            logger.info(f"Injected module: {name}")
    
    # ========================================================================
    # Core Methods (Enhanced)
    # ========================================================================
    
    def _init_thresholds(self):
        """Initialize sustainability thresholds with adaptive capabilities"""
        self.thresholds = {
            'carbon': SustainabilityThreshold(
                dimension='carbon',
                warning_threshold=0.3,
                critical_threshold=0.1,
                adaptive_warning=0.3,
                adaptive_critical=0.1
            ),
            'helium': SustainabilityThreshold(
                dimension='helium',
                warning_threshold=0.4,
                critical_threshold=0.2,
                adaptive_warning=0.4,
                adaptive_critical=0.2
            ),
            'energy': SustainabilityThreshold(
                dimension='energy',
                warning_threshold=0.3,
                critical_threshold=0.15,
                adaptive_warning=0.3,
                adaptive_critical=0.15
            ),
            'circularity': SustainabilityThreshold(
                dimension='circularity',
                warning_threshold=0.4,
                critical_threshold=0.25,
                adaptive_warning=0.4,
                adaptive_critical=0.25
            ),
            'biodiversity': SustainabilityThreshold(
                dimension='biodiversity',
                warning_threshold=0.3,
                critical_threshold=0.15,
                adaptive_warning=0.3,
                adaptive_critical=0.15
            )
        }
    
    async def update_sustainability_score(self) -> UnifiedSustainabilityScore:
        """
        Update the unified sustainability score with enhanced features.
        
        Returns:
            UnifiedSustainabilityScore with all dimensions
        """
        # Get current values from modules
        dimensions = {}
        risk_factors = []
        recommendations = []
        dimension_scores = {}
        
        # Carbon dimension
        carbon_value = await self._get_carbon_score()
        dimensions['carbon'] = SustainabilityDimension(
            name='carbon',
            current_value=carbon_value,
            target_value=0.8,
            weight=self.dimension_weights['carbon'],
            units='score (0-1)',
            trend=self._calculate_trend('carbon', carbon_value),
            confidence=0.8,
            scarcity_factor=self.scarcity_factors.get('carbon', 1.0)
        )
        dimension_scores['carbon'] = carbon_value
        
        # Helium dimension
        helium_value = await self._get_helium_score()
        dimensions['helium'] = SustainabilityDimension(
            name='helium',
            current_value=helium_value,
            target_value=0.8,
            weight=self.dimension_weights['helium'],
            units='score (0-1)',
            trend=self._calculate_trend('helium', helium_value),
            confidence=0.75,
            scarcity_factor=self.scarcity_factors.get('helium', 1.0)
        )
        dimension_scores['helium'] = helium_value
        
        # Energy dimension
        energy_value = await self._get_energy_score()
        dimensions['energy'] = SustainabilityDimension(
            name='energy',
            current_value=energy_value,
            target_value=0.8,
            weight=self.dimension_weights['energy'],
            units='score (0-1)',
            trend=self._calculate_trend('energy', energy_value),
            confidence=0.85,
            scarcity_factor=self.scarcity_factors.get('energy', 1.0)
        )
        dimension_scores['energy'] = energy_value
        
        # Circularity dimension
        circularity_value = await self._get_circularity_score()
        dimensions['circularity'] = SustainabilityDimension(
            name='circularity',
            current_value=circularity_value,
            target_value=0.8,
            weight=self.dimension_weights['circularity'],
            units='score (0-1)',
            trend=self._calculate_trend('circularity', circularity_value),
            confidence=0.7,
            scarcity_factor=self.scarcity_factors.get('circularity', 1.0)
        )
        dimension_scores['circularity'] = circularity_value
        
        # Biodiversity dimension
        biodiversity_value = await self._get_biodiversity_score()
        dimensions['biodiversity'] = SustainabilityDimension(
            name='biodiversity',
            current_value=biodiversity_value,
            target_value=0.8,
            weight=self.dimension_weights['biodiversity'],
            units='score (0-1)',
            trend=self._calculate_trend('biodiversity', biodiversity_value),
            confidence=0.6,
            scarcity_factor=self.scarcity_factors.get('biodiversity', 1.0)
        )
        dimension_scores['biodiversity'] = biodiversity_value
        
        # Update dimension history for prediction
        for name, dim in dimensions.items():
            self.dimension_history[name].append(dim.current_value)
            if len(self.dimension_history[name]) > 100:
                self.dimension_history[name] = self.dimension_history[name][-100:]
        
        # NEW: Update adaptive thresholds
        for name, dim in dimensions.items():
            threshold = self.thresholds.get(name)
            if threshold:
                adaptive_warning, adaptive_critical = await self.adaptive_threshold_manager.update_thresholds(
                    name,
                    dim.current_value,
                    threshold.warning_threshold,
                    threshold.critical_threshold
                )
                threshold.adaptive_warning = adaptive_warning
                threshold.adaptive_critical = adaptive_critical
                threshold.current_value = dim.current_value
                
                # Check adaptive thresholds
                if dim.current_value < adaptive_critical:
                    threshold.status = "critical"
                    risk_factors.append(f"{name} at critical level ({dim.current_value:.2f}) - threshold: {adaptive_critical:.2f}")
                    recommendations.append(f"CRITICAL: Address {name} sustainability immediately")
                elif dim.current_value < adaptive_warning:
                    threshold.status = "warning"
                    risk_factors.append(f"{name} at warning level ({dim.current_value:.2f}) - threshold: {adaptive_warning:.2f}")
                    recommendations.append(f"WARNING: Monitor {name} sustainability")
                else:
                    threshold.status = "healthy"
                
                # Check anomaly
                anomaly_score = self.adaptive_threshold_manager.get_anomaly_score(name, dim.current_value)
                if anomaly_score > 0.7:
                    risk_factors.append(f"{name} shows anomalous behavior (anomaly score: {anomaly_score:.2f})")
        
        # NEW: Dynamic weight adjustment
        scarcity_factors = {
            name: dim.scarcity_factor
            for name, dim in dimensions.items()
        }
        updated_weights = await self.dynamic_weight_manager.update_weights(
            dimension_scores,
            scarcity_factors
        )
        
        # Update dimension weights with dynamic values
        for name, weight in updated_weights.items():
            if name in dimensions:
                dimensions[name].weight = weight
                dimensions[name].historical_weights.append(weight)
        
        # NEW: Predictive analytics
        for name, dim in dimensions.items():
            if name in self.dimension_history and len(self.dimension_history[name]) > 10:
                await self.predictive_analyzer.update_model(
                    name,
                    self.dimension_history[name][-20:]
                )
                prediction, confidence, volatility = await self.predictive_analyzer.predict(name, 10)
                dim.prediction = prediction
                dim.prediction_confidence = confidence
                dim.volatility = volatility
        
        # Calculate total score with dynamic weights
        total_score = 0.0
        for name, dim in dimensions.items():
            if dim.current_value >= 0:
                total_score += dim.current_value * dim.weight
        
        # NEW: Calculate predicted future score
        predicted_total = 0.0
        scenario_scores = {}
        for name, dim in dimensions.items():
            if dim.prediction > 0:
                predicted_total += dim.prediction * dim.weight
                
                # Calculate scenario predictions
                for scenario in ['optimistic', 'pessimistic', 'most_likely']:
                    scenario_key = f"{name}_{scenario}"
                    if scenario_key not in scenario_scores:
                        scenario_scores[scenario_key] = 0.0
                    scenario_value = await self.predictive_analyzer.predict_scenario(name, scenario, 10)
                    scenario_scores[scenario_key] += scenario_value * dim.weight
        
        self.sustainability_score = total_score
        
        # Store in history
        self.history.append({
            'timestamp': datetime.now().isoformat(),
            'score': total_score,
            'dimensions': {k: v.current_value for k, v in dimensions.items()},
            'weights': {k: v.weight for k, v in dimensions.items()},
            'predictions': {k: v.prediction for k, v in dimensions.items() if v.prediction > 0}
        })
        
        # Generate global recommendations
        if total_score < 0.5:
            recommendations.insert(0, "Overall sustainability score below 0.5 - urgent action required")
        elif total_score < 0.7:
            recommendations.insert(0, "Sustainability score needs improvement")
        
        # NEW: Add predictive recommendations
        for name, dim in dimensions.items():
            if dim.prediction > 0 and dim.prediction < dim.current_value * 0.9:
                recommendations.append(
                    f"PREDICTIVE: {name} sustainability is forecasted to decline "
                    f"(current: {dim.current_value:.2f} → predicted: {dim.prediction:.2f})"
                )
        
        # Update the fitness scores in expert registry
        if self.expert_registry:
            await self._update_expert_fitness(total_score, dimensions)
        
        # Update quantum limits
        if self.quantum_limits:
            await self._update_quantum_limits(total_score, dimensions)
        
        return UnifiedSustainabilityScore(
            total_score=total_score,
            dimensions=dimensions,
            confidence=0.8,
            trend=self._calculate_global_trend(),
            risk_factors=risk_factors,
            recommendations=recommendations,
            predicted_future_score=predicted_total if predicted_total > 0 else None,
            scenario_scores=scenario_scores
        )
    
    # ========================================================================
    # Dimension Score Methods (Enhanced)
    # ========================================================================
    
    async def _get_carbon_score(self) -> float:
        """Get carbon sustainability score with scarcity awareness"""
        if self.carbon_manager:
            if hasattr(self.carbon_manager, 'get_current_intensity'):
                intensity = await self.carbon_manager.get_current_intensity()
                # Invert so lower intensity = higher score
                score = max(0, min(1, 1 - intensity / 1000))
                # Update scarcity factor (higher intensity = higher scarcity)
                self.scarcity_factors['carbon'] = min(2.0, intensity / 500)
                return score
            elif hasattr(self.carbon_manager, 'carbon_intensity'):
                intensity = self.carbon_manager.carbon_intensity
                score = max(0, min(1, 1 - intensity / 1000))
                self.scarcity_factors['carbon'] = min(2.0, intensity / 500)
                return score
        return 0.5
    
    async def _get_helium_score(self) -> float:
        """Get helium sustainability score with scarcity awareness"""
        if self.helium_tracker:
            if hasattr(self.helium_tracker, 'get_helium_position'):
                position = self.helium_tracker.get_helium_position()
                if position:
                    remaining = position.get('remaining_budget_l', 0)
                    total = position.get('budget_l', 1)
                    score = max(0, min(1, remaining / max(total, 1)))
                    # Update scarcity factor (lower remaining = higher scarcity)
                    self.scarcity_factors['helium'] = min(2.0, 2.0 - score * 2)
                    return score
            elif hasattr(self.helium_tracker, 'current_scarcity'):
                scarcity = self.helium_tracker.current_scarcity
                self.scarcity_factors['helium'] = min(2.0, 1.0 + scarcity)
                return 1.0 - scarcity
        return 0.5
    
    async def _get_energy_score(self) -> float:
        """Get energy sustainability score with scarcity awareness"""
        if self.expert_registry:
            experts = self.expert_registry.get_all_active_experts()
            if experts:
                avg_energy = np.mean([
                    getattr(e, 'energy_per_inference', 0.001) 
                    for e in experts[:10]
                ])
                score = max(0, min(1, 1 - avg_energy * 1000))
                # Update scarcity factor (higher energy = higher scarcity)
                self.scarcity_factors['energy'] = min(2.0, avg_energy * 1000)
                return score
        return 0.5
    
    async def _get_circularity_score(self) -> float:
        """Get circularity sustainability score with enhanced metrics"""
        if self.circular_manager:
            if hasattr(self.circular_manager, 'get_circularity_report'):
                report = self.circular_manager.get_circularity_report()
                if report:
                    score = report.get('circularity_score', 0.5)
                    # Update scarcity based on circularity gap
                    self.scarcity_factors['circularity'] = min(2.0, 2.0 - score * 2)
                    return score
            elif hasattr(self.circular_manager, 'circularity_score'):
                score = self.circular_manager.circularity_score
                self.scarcity_factors['circularity'] = min(2.0, 2.0 - score * 2)
                return score
        return 0.5
    
    async def _get_biodiversity_score(self) -> float:
        """Get biodiversity sustainability score"""
        if self.biodiversity:
            if hasattr(self.biodiversity, 'get_biodiversity_report'):
                report = self.biodiversity.get_biodiversity_report()
                if report:
                    biodiversity_score = report.get('local_biodiversity_score', 0.5)
                    # Invert: higher biodiversity score = higher sustainability
                    # But biodiversity loss is a negative impact
                    score = 1.0 - biodiversity_score
                    # Scarcity factor for biodiversity loss
                    self.scarcity_factors['biodiversity'] = min(2.0, biodiversity_score * 2)
                    return max(0, min(1, score))
        return 0.5
    
    # ========================================================================
    # Trend Analysis Methods (Enhanced)
    # ========================================================================
    
    def _calculate_trend(self, dimension: str, current_value: float) -> str:
        """Calculate trend for a specific dimension with enhanced detection"""
        history = list(self.history)[-20:]
        if not history:
            return "stable"
        
        values = [h['dimensions'].get(dimension, current_value) for h in history]
        if len(values) > 5:
            trend = np.polyfit(range(len(values)), values, 1)[0]
            if trend > 0.01:
                return "improving"
            elif trend < -0.01:
                return "declining"
        return "stable"
    
    def _calculate_global_trend(self) -> str:
        """Calculate global sustainability trend"""
        history = list(self.history)[-20:]
        if not history:
            return "stable"
        
        scores = [h['score'] for h in history]
        if len(scores) > 5:
            trend = np.polyfit(range(len(scores)), scores, 1)[0]
            if trend > 0.005:
                return "improving"
            elif trend < -0.005:
                return "declining"
        return "stable"
    
    # ========================================================================
    # Integration Methods (Enhanced)
    # ========================================================================
    
    async def _update_expert_fitness(self, score: float, dimensions: Dict):
        """Update expert fitness scores based on sustainability score"""
        if hasattr(self.expert_registry, 'update_sustainability_fitness'):
            await self.expert_registry.update_sustainability_fitness(score, dimensions)
    
    async def _update_quantum_limits(self, score: float, dimensions: Dict):
        """Update quantum limits based on sustainability score"""
        if hasattr(self.quantum_limits, 'update_sustainability_limits'):
            await self.quantum_limits.update_sustainability_limits(score, dimensions)
    
    # ========================================================================
    # Public Methods (Enhanced)
    # ========================================================================
    
    async def get_current_score(self) -> float:
        """Get current unified sustainability score"""
        return self.sustainability_score
    
    async def get_dimension_status(self) -> Dict[str, str]:
        """Get status of each sustainability dimension with adaptive thresholds"""
        status = {}
        for name, threshold in self.thresholds.items():
            # Use adaptive thresholds for status
            adaptive_warning = getattr(threshold, 'adaptive_warning', threshold.warning_threshold)
            adaptive_critical = getattr(threshold, 'adaptive_critical', threshold.critical_threshold)
            
            if threshold.current_value < adaptive_critical:
                status[name] = "critical"
            elif threshold.current_value < adaptive_warning:
                status[name] = "warning"
            else:
                status[name] = "healthy"
        return status
    
    async def get_historical_scores(self, n: int = 100) -> List[Dict]:
        """Get historical sustainability scores"""
        return list(self.history)[-n:]
    
    async def get_dimension_predictions(self) -> Dict[str, Any]:
        """Get predictions for all dimensions"""
        predictions = {}
        for name, history in self.dimension_history.items():
            if len(history) > 10:
                pred, conf, vol = await self.predictive_analyzer.predict(name, 10)
                predictions[name] = {
                    'prediction': pred,
                    'confidence': conf,
                    'volatility': vol,
                    'accuracy': self.predictive_analyzer.get_prediction_accuracy(name)
                }
        return predictions
    
    async def get_sustainability_report(
        self,
        template_name: str = "executive_summary"
    ) -> Dict[str, Any]:
        """Generate comprehensive sustainability report using a template"""
        score = await self.update_sustainability_score()
        status = await self.get_dimension_status()
        predictions = await self.get_dimension_predictions()
        
        # Prepare data for report
        report_data = {
            'total_score': score.total_score,
            'trend': score.trend,
            'dimensions': {
                name: {
                    'value': dim.current_value,
                    'weight': dim.weight,
                    'trend': dim.trend,
                    'status': status.get(name, 'unknown'),
                    'scarcity_factor': dim.scarcity_factor,
                    'prediction': dim.prediction,
                    'prediction_confidence': dim.prediction_confidence,
                    'volatility': dim.volatility
                }
                for name, dim in score.dimensions.items()
            },
            'risk_factors': score.risk_factors,
            'recommendations': score.recommendations,
            'predictions': predictions,
            'history': await self.get_historical_scores(10),
            'weight_trends': self.dynamic_weight_manager.get_weight_trends(),
            'threshold_stats': {
                name: self.adaptive_threshold_manager.get_threshold_stats(name)
                for name in self.thresholds
            }
        }
        
        # Generate report using template
        if template_name:
            report = await self.report_manager.generate_report(
                template_name,
                report_data
            )
            if report.get('status') == 'generated':
                report['data'] = report_data
                return report
        
        # Fallback to full report
        return report_data
    
    async def update_scarcity_factors(self, new_factors: Dict[str, float]):
        """Update scarcity factors manually"""
        for dim, factor in new_factors.items():
            if dim in self.scarcity_factors:
                self.scarcity_factors[dim] = factor
        logger.info(f"Updated scarcity factors: {new_factors}")
    
    def get_available_templates(self) -> List[str]:
        """Get list of available report templates"""
        return self.report_manager.list_templates()
    
    async def create_custom_template(self, template: ReportTemplate) -> bool:
        """Create a custom report template"""
        return self.report_manager.create_template(template)
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Unified Sustainability Engine v2.0.0")
