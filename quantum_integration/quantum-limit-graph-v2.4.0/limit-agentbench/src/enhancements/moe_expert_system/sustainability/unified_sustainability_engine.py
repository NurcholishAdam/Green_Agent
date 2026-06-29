# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/sustainability/unified_sustainability_engine.py
"""
Unified Sustainability Valuation Engine for Green Agent
Creates a single, authoritative global sustainability function
that aggregates all dimensions (carbon, helium, energy, circularity, biodiversity).

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
from datetime import datetime
import numpy as np
from collections import deque

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SustainabilityDimension:
    """A single dimension of sustainability"""
    name: str
    current_value: float
    target_value: float
    weight: float
    units: str
    trend: str = "stable"  # improving, stable, declining
    confidence: float = 0.8

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

@dataclass
class SustainabilityThreshold:
    """Threshold for sustainability alerts"""
    dimension: str
    warning_threshold: float
    critical_threshold: float
    current_value: float = 0.0
    status: str = "unknown"  # healthy, warning, critical

# ============================================================================
# Unified Sustainability Engine
# ============================================================================

class UnifiedSustainabilityEngine:
    """
    Unified Sustainability Valuation Engine.
    
    Features:
    - Single authoritative global sustainability function
    - Aggregates carbon, helium, energy, circularity, biodiversity
    - Provides feedback to expert registry and gating network
    - Tracks trends and generates recommendations
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
        
        # Initialize thresholds
        self._init_thresholds()
        
        # Sub-modules (will be injected)
        self.carbon_manager = None
        self.helium_tracker = None
        self.circular_manager = None
        self.biodiversity = None
        self.expert_registry = None
        self.quantum_limits = None
        
        logger.info("Unified Sustainability Engine initialized")
    
    # ========================================================================
    # Module Injection
    # ========================================================================
    
    def inject_modules(self, **modules):
        """Inject required system modules"""
        for name, module in modules.items():
            setattr(self, name, module)
            logger.info(f"Injected module: {name}")
    
    # ========================================================================
    # Core Methods
    # ========================================================================
    
    def _init_thresholds(self):
        """Initialize sustainability thresholds"""
        self.thresholds = {
            'carbon': SustainabilityThreshold(
                dimension='carbon',
                warning_threshold=0.3,
                critical_threshold=0.1
            ),
            'helium': SustainabilityThreshold(
                dimension='helium',
                warning_threshold=0.4,
                critical_threshold=0.2
            ),
            'energy': SustainabilityThreshold(
                dimension='energy',
                warning_threshold=0.3,
                critical_threshold=0.15
            ),
            'circularity': SustainabilityThreshold(
                dimension='circularity',
                warning_threshold=0.4,
                critical_threshold=0.25
            ),
            'biodiversity': SustainabilityThreshold(
                dimension='biodiversity',
                warning_threshold=0.3,
                critical_threshold=0.15
            )
        }
    
    async def update_sustainability_score(self) -> UnifiedSustainabilityScore:
        """
        Update the unified sustainability score.
        
        Returns:
            UnifiedSustainabilityScore with all dimensions
        """
        # Get current values from modules
        dimensions = {}
        risk_factors = []
        recommendations = []
        
        # Carbon dimension
        carbon_value = await self._get_carbon_score()
        dimensions['carbon'] = SustainabilityDimension(
            name='carbon',
            current_value=carbon_value,
            target_value=0.8,
            weight=self.dimension_weights['carbon'],
            units='score (0-1)',
            trend=self._calculate_trend('carbon', carbon_value),
            confidence=0.8
        )
        
        # Helium dimension
        helium_value = await self._get_helium_score()
        dimensions['helium'] = SustainabilityDimension(
            name='helium',
            current_value=helium_value,
            target_value=0.8,
            weight=self.dimension_weights['helium'],
            units='score (0-1)',
            trend=self._calculate_trend('helium', helium_value),
            confidence=0.75
        )
        
        # Energy dimension
        energy_value = await self._get_energy_score()
        dimensions['energy'] = SustainabilityDimension(
            name='energy',
            current_value=energy_value,
            target_value=0.8,
            weight=self.dimension_weights['energy'],
            units='score (0-1)',
            trend=self._calculate_trend('energy', energy_value),
            confidence=0.85
        )
        
        # Circularity dimension
        circularity_value = await self._get_circularity_score()
        dimensions['circularity'] = SustainabilityDimension(
            name='circularity',
            current_value=circularity_value,
            target_value=0.8,
            weight=self.dimension_weights['circularity'],
            units='score (0-1)',
            trend=self._calculate_trend('circularity', circularity_value),
            confidence=0.7
        )
        
        # Biodiversity dimension
        biodiversity_value = await self._get_biodiversity_score()
        dimensions['biodiversity'] = SustainabilityDimension(
            name='biodiversity',
            current_value=biodiversity_value,
            target_value=0.8,
            weight=self.dimension_weights['biodiversity'],
            units='score (0-1)',
            trend=self._calculate_trend('biodiversity', biodiversity_value),
            confidence=0.6
        )
        
        # Check thresholds
        for name, dim in dimensions.items():
            threshold = self.thresholds.get(name)
            if threshold:
                threshold.current_value = dim.current_value
                if dim.current_value < threshold.critical_threshold:
                    threshold.status = "critical"
                    risk_factors.append(f"{name} at critical level ({dim.current_value:.2f})")
                    recommendations.append(f"CRITICAL: Address {name} sustainability immediately")
                elif dim.current_value < threshold.warning_threshold:
                    threshold.status = "warning"
                    risk_factors.append(f"{name} at warning level ({dim.current_value:.2f})")
                    recommendations.append(f"WARNING: Monitor {name} sustainability")
                else:
                    threshold.status = "healthy"
        
        # Calculate total score
        total_score = 0.0
        for name, dim in dimensions.items():
            if dim.current_value >= 0:
                total_score += dim.current_value * dim.weight
        
        # Update thresholds
        self.sustainability_score = total_score
        
        # Store in history
        self.history.append({
            'timestamp': datetime.now().isoformat(),
            'score': total_score,
            'dimensions': {k: v.current_value for k, v in dimensions.items()}
        })
        
        # Generate global recommendations
        if total_score < 0.5:
            recommendations.insert(0, "Overall sustainability score below 0.5 - urgent action required")
        elif total_score < 0.7:
            recommendations.insert(0, "Sustainability score needs improvement")
        
        # Update thresholds with current values
        self._update_thresholds(dimensions)
        
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
            recommendations=recommendations
        )
    
    # ========================================================================
    # Dimension Score Methods
    # ========================================================================
    
    async def _get_carbon_score(self) -> float:
        """Get carbon sustainability score (0-1, higher = better)"""
        if self.carbon_manager:
            if hasattr(self.carbon_manager, 'get_current_intensity'):
                intensity = await self.carbon_manager.get_current_intensity()
                # Invert so lower intensity = higher score
                return max(0, min(1, 1 - intensity / 1000))
            elif hasattr(self.carbon_manager, 'carbon_intensity'):
                return max(0, min(1, 1 - self.carbon_manager.carbon_intensity / 1000))
        return 0.5
    
    async def _get_helium_score(self) -> float:
        """Get helium sustainability score (0-1, higher = better)"""
        if self.helium_tracker:
            position = self.helium_tracker.get_helium_position()
            if position:
                remaining_ratio = position.get('remaining_budget_l', 0) / max(position.get('budget_l', 1), 1)
                return max(0, min(1, remaining_ratio))
        return 0.5
    
    async def _get_energy_score(self) -> float:
        """Get energy sustainability score (0-1, higher = better)"""
        # Estimate from expert registry
        if self.expert_registry:
            experts = self.expert_registry.get_all_active_experts()
            if experts:
                avg_energy = np.mean([
                    getattr(e, 'energy_per_inference', 0.001) 
                    for e in experts[:10]
                ])
                # Invert so lower energy = higher score
                return max(0, min(1, 1 - avg_energy * 1000))
        return 0.5
    
    async def _get_circularity_score(self) -> float:
        """Get circularity sustainability score (0-1, higher = better)"""
        if self.circular_manager:
            report = self.circular_manager.get_circularity_report()
            if report:
                return report.get('circularity_score', 0.5)
        return 0.5
    
    async def _get_biodiversity_score(self) -> float:
        """Get biodiversity sustainability score (0-1, higher = better)"""
        if self.biodiversity:
            report = self.biodiversity.get_biodiversity_report()
            if report:
                return 1.0 - report.get('local_biodiversity_score', 0.5)
        return 0.5
    
    # ========================================================================
    # Trend Analysis Methods
    # ========================================================================
    
    def _calculate_trend(self, dimension: str, current_value: float) -> str:
        """Calculate trend for a specific dimension"""
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
    
    def _update_thresholds(self, dimensions: Dict[str, SustainabilityDimension]):
        """Update thresholds with current values"""
        for name, dim in dimensions.items():
            if name in self.thresholds:
                self.thresholds[name].current_value = dim.current_value
    
    # ========================================================================
    # Integration Methods
    # ========================================================================
    
    async def _update_expert_fitness(self, score: float, dimensions: Dict):
        """Update expert fitness scores based on sustainability score"""
        # This would integrate with expert_registry.py
        if hasattr(self.expert_registry, 'update_sustainability_fitness'):
            await self.expert_registry.update_sustainability_fitness(score, dimensions)
    
    async def _update_quantum_limits(self, score: float, dimensions: Dict):
        """Update quantum limits based on sustainability score"""
        if hasattr(self.quantum_limits, 'update_sustainability_limits'):
            await self.quantum_limits.update_sustainability_limits(score, dimensions)
    
    # ========================================================================
    # Public Methods
    # ========================================================================
    
    async def get_current_score(self) -> float:
        """Get current unified sustainability score"""
        return self.sustainability_score
    
    async def get_dimension_status(self) -> Dict[str, str]:
        """Get status of each sustainability dimension"""
        status = {}
        for name, threshold in self.thresholds.items():
            if threshold.current_value < threshold.critical_threshold:
                status[name] = "critical"
            elif threshold.current_value < threshold.warning_threshold:
                status[name] = "warning"
            else:
                status[name] = "healthy"
        return status
    
    async def get_historical_scores(self, n: int = 100) -> List[Dict]:
        """Get historical sustainability scores"""
        return list(self.history)[-n:]
    
    async def get_sustainability_report(self) -> Dict[str, Any]:
        """Generate comprehensive sustainability report"""
        score = await self.update_sustainability_score()
        status = await self.get_dimension_status()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_score': score.total_score,
            'trend': score.trend,
            'dimensions': {
                name: {
                    'value': dim.current_value,
                    'weight': dim.weight,
                    'trend': dim.trend,
                    'status': status.get(name, 'unknown')
                }
                for name, dim in score.dimensions.items()
            },
            'risk_factors': score.risk_factors,
            'recommendations': score.recommendations,
            'history': await self.get_historical_scores(10)
        }
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Unified Sustainability Engine")
