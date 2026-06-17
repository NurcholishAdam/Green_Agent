# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/degradation_manager.py

"""
Multi-Level Degradation Manager for Green Agent
Implements 5-tier operational readiness with smooth transitions.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)

class OperationalTier(Enum):
    """5-tier operational readiness levels"""
    TIER_5_FULL = 5          # All systems optimal
    TIER_4_REDUCED = 4       # Non-essential reduced
    TIER_3_CONSERVATIVE = 3  # Conservative operation
    TIER_2_CRITICAL = 2      # Critical functions only
    TIER_1_SURVIVAL = 1      # Minimal survival mode

@dataclass
class DegradationRule:
    """Rule for transitioning between operational tiers"""
    rule_id: str
    metric: str
    threshold: float
    comparison: str  # 'above' or 'below'
    target_tier: OperationalTier
    cooldown_seconds: float = 60.0
    description: str = ""

class DegradationManager:
    """
    Manages multi-level graceful degradation.
    
    Implements smooth transitions between 5 operational tiers
    based on system health metrics.
    """
    
    def __init__(self):
        self.current_tier = OperationalTier.TIER_5_FULL
        self.previous_tier = OperationalTier.TIER_5_FULL
        self.tier_history: List[Dict[str, Any]] = []
        self.last_transition_time = datetime.utcnow()
        self.transition_cooldown = timedelta(seconds=30)
        
        # Tier-specific policies
        self.tier_policies = self._initialize_policies()
        
        # Degradation rules
        self.rules = self._initialize_rules()
        
        # Module callbacks for tier changes
        self.tier_change_callbacks: List[callable] = []
        
        # Start monitoring
        asyncio.create_task(self._monitoring_loop())
        
        logger.info(f"Degradation Manager initialized at {self.current_tier.name}")
    
    def _initialize_policies(self) -> Dict[OperationalTier, Dict[str, Any]]:
        """Initialize operational policies per tier"""
        return {
            OperationalTier.TIER_5_FULL: {
                'expert_activation': 'all',
                'token_allocation': 'generous',
                'exploration_rate': 0.2,
                'cache_ttl_seconds': 120,
                'max_parallel_tasks': 100,
                'quality_threshold': 0.9,
                'biomass_storage': 'all_tiers',
                'gradient_sensitivity': 'high'
            },
            OperationalTier.TIER_4_REDUCED: {
                'expert_activation': 'all',
                'token_allocation': 'moderate',
                'exploration_rate': 0.1,
                'cache_ttl_seconds': 90,
                'max_parallel_tasks': 75,
                'quality_threshold': 0.85,
                'biomass_storage': 'hot_warm_only',
                'gradient_sensitivity': 'moderate'
            },
            OperationalTier.TIER_3_CONSERVATIVE: {
                'expert_activation': 'essential_only',
                'token_allocation': 'conservative',
                'exploration_rate': 0.05,
                'cache_ttl_seconds': 60,
                'max_parallel_tasks': 40,
                'quality_threshold': 0.8,
                'biomass_storage': 'hot_only',
                'gradient_sensitivity': 'low'
            },
            OperationalTier.TIER_2_CRITICAL: {
                'expert_activation': 'critical_only',
                'token_allocation': 'minimal',
                'exploration_rate': 0.0,
                'cache_ttl_seconds': 30,
                'max_parallel_tasks': 15,
                'quality_threshold': 0.7,
                'biomass_storage': 'emergency_only',
                'gradient_sensitivity': 'minimal'
            },
            OperationalTier.TIER_1_SURVIVAL: {
                'expert_activation': 'survival_only',
                'token_allocation': 'emergency',
                'exploration_rate': 0.0,
                'cache_ttl_seconds': 10,
                'max_parallel_tasks': 5,
                'quality_threshold': 0.5,
                'biomass_storage': 'none',
                'gradient_sensitivity': 'none'
            }
        }
    
    def _initialize_rules(self) -> List[DegradationRule]:
        """Initialize degradation transition rules"""
        return [
            # Degradation rules (worsening conditions)
            DegradationRule('R1', 'token_balance', 100, 'below', 
                           OperationalTier.TIER_4_REDUCED, 60,
                           'Low token balance triggers reduced operations'),
            DegradationRule('R2', 'token_balance', 30, 'below',
                           OperationalTier.TIER_3_CONSERVATIVE, 30,
                           'Critical token balance triggers conservative mode'),
            DegradationRule('R3', 'carbon_gradient', 0.85, 'above',
                           OperationalTier.TIER_3_CONSERVATIVE, 120,
                           'High carbon gradient triggers conservative operations'),
            DegradationRule('R4', 'compartment_health', 0.3, 'below',
                           OperationalTier.TIER_2_CRITICAL, 60,
                           'Low compartment health triggers critical mode'),
            DegradationRule('R5', 'token_balance', 10, 'below',
                           OperationalTier.TIER_1_SURVIVAL, 30,
                           'Emergency token level triggers survival mode'),
            
            # Recovery rules (improving conditions)
            DegradationRule('R6', 'token_balance', 200, 'above',
                           OperationalTier.TIER_4_REDUCED, 120,
                           'Token recovery allows reduced operations'),
            DegradationRule('R7', 'token_balance', 500, 'above',
                           OperationalTier.TIER_5_FULL, 300,
                           'Full token recovery restores full operations'),
            DegradationRule('R8', 'compartment_health', 0.6, 'above',
                           OperationalTier.TIER_3_CONSERVATIVE, 180,
                           'Health recovery allows conservative operations'),
            DegradationRule('R9', 'carbon_gradient', 0.4, 'below',
                           OperationalTier.TIER_4_REDUCED, 180,
                           'Carbon improvement allows reduced operations'),
        ]
    
    def register_callback(self, callback: callable):
        """Register callback for tier changes"""
        self.tier_change_callbacks.append(callback)
    
    async def _monitoring_loop(self):
        """Monitor system health and manage degradation"""
        while True:
            try:
                health_metrics = self._collect_health_metrics()
                new_tier = self._evaluate_tier(health_metrics)
                
                if new_tier != self.current_tier:
                    if self._can_transition():
                        await self._transition_to(new_tier, health_metrics)
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Degradation monitoring error: {str(e)}")
                await asyncio.sleep(30)
    
    def _collect_health_metrics(self) -> Dict[str, float]:
        """Collect current health metrics"""
        return {
            'token_balance': getattr(self, '_token_balance', 500),
            'carbon_gradient': getattr(self, '_carbon_gradient', 0.5),
            'compartment_health': getattr(self, '_compartment_health', 0.8),
            'harvester_activity': getattr(self, '_harvester_activity', 0.6),
            'error_rate': getattr(self, '_error_rate', 0.01),
            'queue_depth': getattr(self, '_queue_depth', 10)
        }
    
    def _evaluate_tier(self, metrics: Dict[str, float]) -> OperationalTier:
        """Evaluate which operational tier is appropriate"""
        scores = {tier: 0 for tier in OperationalTier}
        
        for rule in self.rules:
            metric_value = metrics.get(rule.metric, 0)
            
            if rule.comparison == 'above' and metric_value > rule.threshold:
                scores[rule.target_tier] += 1
            elif rule.comparison == 'below' and metric_value < rule.threshold:
                scores[rule.target_tier] += 1
        
        # Select tier with highest score (most matching rules)
        best_tier = max(scores, key=scores.get)
        
        # Ensure we don't skip tiers (smooth degradation)
        if best_tier.value < self.current_tier.value - 1:
            best_tier = OperationalTier(self.current_tier.value - 1)
        elif best_tier.value > self.current_tier.value + 1:
            best_tier = OperationalTier(self.current_tier.value + 1)
        
        return best_tier
    
    def _can_transition(self) -> bool:
        """Check if enough time has passed since last transition"""
        elapsed = datetime.utcnow() - self.last_transition_time
        return elapsed > self.transition_cooldown
    
    async def _transition_to(self, new_tier: OperationalTier, metrics: Dict[str, float]):
        """Execute transition to new operational tier"""
        old_tier = self.current_tier
        self.previous_tier = old_tier
        self.current_tier = new_tier
        self.last_transition_time = datetime.utcnow()
        
        # Record transition
        transition = {
            'from_tier': old_tier.value,
            'to_tier': new_tier.value,
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': metrics,
            'direction': 'degrading' if new_tier.value < old_tier.value else 'recovering'
        }
        self.tier_history.append(transition)
        
        # Apply new policies
        policies = self.tier_policies[new_tier]
        
        # Notify all registered callbacks
        for callback in self.tier_change_callbacks:
            try:
                await callback(old_tier, new_tier, policies)
            except Exception as e:
                logger.error(f"Tier change callback error: {str(e)}")
        
        logger.warning(
            f"OPERATIONAL TIER CHANGE: {old_tier.name} → {new_tier.name} "
            f"({transition['direction']})"
        )
    
    def get_current_policy(self) -> Dict[str, Any]:
        """Get current operational policy"""
        return self.tier_policies[self.current_tier]
    
    def get_tier_status(self) -> Dict[str, Any]:
        """Get comprehensive tier status"""
        return {
            'current_tier': self.current_tier.value,
            'current_tier_name': self.current_tier.name,
            'previous_tier': self.previous_tier.value,
            'policy': self.get_current_policy(),
            'last_transition': self.last_transition_time.isoformat(),
            'recent_history': self.tier_history[-10:]
        }
    
    def update_metrics(self, **kwargs):
        """Update health metrics from external sources"""
        for key, value in kwargs.items():
            setattr(self, f'_{key}', value)
    
    def should_execute(self, operation_type: str) -> bool:
        """Check if operation type is allowed in current tier"""
        policy = self.get_current_policy()
        
        operation_map = {
            'expert_execution': lambda p: p['expert_activation'] in ['all', 'essential_only', 'critical_only'],
            'exploration': lambda p: p['exploration_rate'] > 0,
            'biomass_storage': lambda p: p['biomass_storage'] != 'none',
            'caching': lambda p: p['cache_ttl_seconds'] > 0,
        }
        
        checker = operation_map.get(operation_type)
        if checker:
            return checker(policy)
        return True
