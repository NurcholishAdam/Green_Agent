# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/run_enhanced_agent.py
# Enhanced with dynamic pipeline selection, degradation awareness, and predictive integration

"""
Enhanced Green Agent Runner v5.0.0
Complete integration with dynamic pipeline selection, degradation awareness,
predictive homeostasis, and bio-inspired orchestration.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import numpy as np
import json
import os
import signal

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing modules
# ============================================================================
try:
    from enhancements.moe_expert_system import UnifiedMetabolicEcosystem
    from enhancements.bio_inspired import EnhancedBioInspiredCore
    from enhancements.bio_inspired.eco_atp_currency import EcoATPSource, EcoATPConsumer
    from enhancements.bio_inspired.degradation_manager import OperationalTier
    BIO_AVAILABLE = True
except ImportError:
    BIO_AVAILABLE = False

# ============================================================================
# Pipeline Selection Engine
# ============================================================================

class DynamicPipelineSelector:
    """
    Dynamically selects processing pipeline based on system state.
    
    Integrates with:
    - Degradation Manager (tier-aware selection)
    - Predictive Homeostasis (forecast-informed)
    - Token Manager (budget-aware)
    - Gradient Fields (state-aware)
    """
    
    def __init__(self):
        self.pipeline_performance: Dict[str, List[float]] = defaultdict(list)
        self.pipeline_history: deque = deque(maxlen=1000)
        
        # Pipeline suitability matrix
        self.pipeline_suitability = {
            'standard': {
                'tier_5': 1.0, 'tier_4': 0.8, 'tier_3': 0.6, 'tier_2': 0.4, 'tier_1': 0.2,
                'tokens_abundant': 0.8, 'tokens_scarce': 0.6,
                'carbon_low': 0.9, 'carbon_high': 0.5
            },
            'quantum_enhanced': {
                'tier_5': 0.9, 'tier_4': 0.7, 'tier_3': 0.4, 'tier_2': 0.2, 'tier_1': 0.0,
                'tokens_abundant': 0.9, 'tokens_scarce': 0.2,
                'carbon_low': 0.8, 'carbon_high': 0.3
            },
            'helium_optimized': {
                'tier_5': 0.9, 'tier_4': 0.9, 'tier_3': 0.7, 'tier_2': 0.5, 'tier_1': 0.3,
                'tokens_abundant': 0.7, 'tokens_scarce': 0.8,
                'carbon_low': 0.8, 'carbon_high': 0.7
            },
            'energy_efficient': {
                'tier_5': 0.8, 'tier_4': 0.9, 'tier_3': 0.9, 'tier_2': 0.7, 'tier_1': 0.5,
                'tokens_abundant': 0.6, 'tokens_scarce': 0.9,
                'carbon_low': 0.7, 'carbon_high': 0.9
            },
            'bio_optimized': {
                'tier_5': 1.0, 'tier_4': 0.9, 'tier_3': 0.8, 'tier_2': 0.6, 'tier_1': 0.3,
                'tokens_abundant': 0.9, 'tokens_scarce': 0.5,
                'carbon_low': 0.9, 'carbon_high': 0.4
            }
        }
    
    def select_pipeline(
        self, task: Dict[str, Any], system_state: Dict[str, Any]
    ) -> Tuple[str, Dict[str, float]]:
        """
        Select optimal pipeline based on system state.
        
        Returns (pipeline_name, scores).
        """
        scores = {}
        
        # Extract system conditions
        tier = system_state.get('degradation_tier', 5)
        token_balance = system_state.get('token_balance', 1000)
        carbon_gradient = system_state.get('carbon_gradient', 0.5)
        predicted_carbon = system_state.get('predicted_carbon', carbon_gradient)
        
        # Determine conditions
        tier_key = f'tier_{tier}'
        token_condition = 'tokens_abundant' if token_balance > 500 else 'tokens_scarce'
        carbon_condition = 'carbon_low' if carbon_gradient < 0.5 else 'carbon_high'
        
        for pipeline, suitability in self.pipeline_suitability.items():
            score = 0.0
            
            # Tier suitability
            score += suitability.get(tier_key, 0.5) * 0.3
            
            # Token suitability
            score += suitability.get(token_condition, 0.5) * 0.25
            
            # Carbon suitability
            score += suitability.get(carbon_condition, 0.5) * 0.25
            
            # Predictive adjustment
            if predicted_carbon > carbon_gradient:
                # Carbon worsening - prefer energy efficient
                if pipeline == 'energy_efficient':
                    score += 0.1
            elif predicted_carbon < carbon_gradient:
                # Carbon improving - can use more resources
                if pipeline in ['quantum_enhanced', 'bio_optimized']:
                    score += 0.1
            
            # Historical performance
            if pipeline in self.pipeline_performance:
                recent = self.pipeline_performance[pipeline][-20:]
                if recent:
                    score += np.mean(recent) * 0.2
            
            scores[pipeline] = score
        
        # Select best pipeline
        best_pipeline = max(scores, key=scores.get)
        
        # Record selection
        self.pipeline_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'selected': best_pipeline,
            'scores': scores,
            'conditions': {
                'tier': tier,
                'token_balance': token_balance,
                'carbon_gradient': carbon_gradient
            }
        })
        
        return best_pipeline, scores
    
    def record_performance(self, pipeline: str, success: bool, latency_ms: float):
        """Record pipeline performance for learning"""
        if pipeline not in self.pipeline_performance:
            self.pipeline_performance[pipeline] = []
        
        score = (1.0 if success else 0.0) * 0.7 + (1.0 / (1.0 + latency_ms / 100)) * 0.3
        self.pipeline_performance[pipeline].append(score)
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline selection statistics"""
        recent = list(self.pipeline_history)[-50:]
        
        pipeline_counts = defaultdict(int)
        for entry in recent:
            pipeline_counts[entry['selected']] += 1
        
        return {
            'recent_selections': dict(pipeline_counts),
            'pipeline_performance': {
                p: {
                    'avg_score': np.mean(scores[-20:]) if len(scores) >= 5 else 0.5,
                    'total_runs': len(scores)
                }
                for p, scores in self.pipeline_performance.items()
            },
            'last_selection': self.pipeline_history[-1] if self.pipeline_history else None
        }

# ============================================================================
# Enhanced Agent Runner
# ============================================================================

class EnhancedGreenAgentRunner:
    """
    Enhanced Green Agent Runner v5.0.0
    
    Features:
    - Dynamic pipeline selection based on system state
    - Degradation-aware task processing
    - Predictive-informed scheduling
    - Bio-inspired orchestration
    - Graceful shutdown
    """
    
    def __init__(self, config_path: Optional[str] = None):
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize bio-inspired core
        self.bio_core = None
        if BIO_AVAILABLE:
            self.bio_core = EnhancedBioInspiredCore()
        
        # Initialize MoE ecosystem
        self.moe_ecosystem = None
        if BIO_AVAILABLE:
            try:
                self.moe_ecosystem = UnifiedMetabolicEcosystem()
            except Exception as e:
                logger.warning(f"MoE ecosystem not available: {str(e)}")
        
        # Pipeline selector
        self.pipeline_selector = DynamicPipelineSelector()
        
        # Available pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'energy_efficient': self._energy_efficient_pipeline,
            'bio_optimized': self._bio_optimized_pipeline
        }
        
        # Task tracking
        self.total_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        
        # State
        self.running = True
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info("Enhanced Green Agent Runner v5.0.0 initialized")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load configuration"""
        default_config = {
            'enable_dynamic_pipeline': True,
            'enable_degradation_aware': True,
            'enable_predictive_informed': True,
            'max_concurrent_tasks': 10,
            'task_timeout_seconds': 300
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                default_config.update(user_config)
        
        return default_config
    
    def _register_signal_handlers(self):
        """Register signal handlers for graceful shutdown"""
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            pass
    
    # ========================================================================
    # System State Collection
    # ========================================================================
    
    def _get_system_state(self) -> Dict[str, Any]:
        """Collect current system state for pipeline selection"""
        state = {
            'degradation_tier': 5,
            'token_balance': 1000,
            'carbon_gradient': 0.5,
            'predicted_carbon': 0.5
        }
        
        if self.bio_core:
            # Get degradation tier
            if hasattr(self.bio_core, 'degradation_manager'):
                state['degradation_tier'] = self.bio_core.degradation_manager.current_tier.value
            
            # Get token balance
            if hasattr(self.bio_core, 'token_manager'):
                summary = self.bio_core.token_manager.get_system_summary()
                state['token_balance'] = summary.get('total_balance', 1000)
            
            # Get carbon gradient
            if hasattr(self.bio_core, 'gradient_manager'):
                strengths = self.bio_core.gradient_manager.get_field_strengths()
                state['carbon_gradient'] = strengths.get('carbon', 0.5)
            
            # Get predicted carbon
            if hasattr(self.bio_core, 'gradient_manager'):
                if hasattr(self.bio_core.gradient_manager, 'forecast'):
                    forecast = self.bio_core.gradient_manager.forecast('carbon', 300)
                    state['predicted_carbon'] = forecast.get('predicted', state['carbon_gradient'])
        
        return state
    
    # ========================================================================
    # Enhanced Task Processing
    # ========================================================================
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with dynamic pipeline selection"""
        start_time = datetime.utcnow()
        self.total_tasks += 1
        
        # Get system state
        system_state = self._get_system_state()
        
        # Check degradation tier
        if self.config['enable_degradation_aware']:
            tier = system_state['degradation_tier']
            if tier <= 1:
                return {
                    'success': False,
                    'reason': f'System in survival mode (tier {tier})',
                    'task_id': task.get('task_id', 'unknown')
                }
            
            # Adjust task priority based on tier
            if tier <= 2 and task.get('priority', 2) > 1:
                return {
                    'success': False,
                    'reason': f'Non-critical tasks deferred in tier {tier}',
                    'task_id': task.get('task_id', 'unknown')
                }
        
        # Select pipeline
        if self.config['enable_dynamic_pipeline']:
            pipeline_name, scores = self.pipeline_selector.select_pipeline(task, system_state)
        else:
            pipeline_name = task.get('pipeline', 'standard')
        
        # Execute pipeline
        try:
            pipeline_func = self.pipelines.get(pipeline_name, self._standard_pipeline)
            result = await pipeline_func(task)
            
            # Record performance
            success = result.get('success', False)
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            self.pipeline_selector.record_performance(pipeline_name, success, latency)
            
            if success:
                self.successful_tasks += 1
            else:
                self.failed_tasks += 1
            
            # Add metadata
            result['pipeline_used'] = pipeline_name
            result['pipeline_scores'] = scores
            result['system_state'] = {
                'tier': system_state['degradation_tier'],
                'token_balance': system_state['token_balance'],
                'carbon_gradient': system_state['carbon_gradient']
            }
            
            return result
            
        except Exception as e:
            self.failed_tasks += 1
            logger.error(f"Task processing error: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'task_id': task.get('task_id', 'unknown')
            }
    
    async def batch_process(self, tasks: List[Dict[str, Any]], max_concurrent: int = 10) -> List[Dict[str, Any]]:
        """Process batch of tasks with concurrency control"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_limit(task):
            async with semaphore:
                return await self.process_task(task)
        
        tasks_coroutines = [process_with_limit(task) for task in tasks]
        results = await asyncio.gather(*tasks_coroutines, return_exceptions=True)
        
        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed.append({'success': False, 'error': str(result), 'task_index': i})
            else:
                processed.append(result)
        
        return processed
    
    # ========================================================================
    # Pipeline Implementations
    # ========================================================================
    
    async def _standard_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Standard processing pipeline"""
        return {'success': True, 'pipeline': 'standard', 'task_id': task.get('task_id')}
    
    async def _quantum_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Quantum-enhanced pipeline"""
        if not task.get('quantum_capable'):
            return await self._standard_pipeline(task)
        return {'success': True, 'pipeline': 'quantum', 'task_id': task.get('task_id')}
    
    async def _helium_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Helium-optimized pipeline"""
        return {'success': True, 'pipeline': 'helium', 'task_id': task.get('task_id')}
    
    async def _energy_efficient_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Energy-efficient pipeline"""
        return {'success': True, 'pipeline': 'energy_efficient', 'task_id': task.get('task_id')}
    
    async def _bio_optimized_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Bio-optimized pipeline"""
        if self.moe_ecosystem:
            # Route through MoE ecosystem
            result = self.moe_ecosystem.process_task(task)
            result['pipeline'] = 'bio_optimized'
            return result
        
        return await self._standard_pipeline(task)
    
    # ========================================================================
    # Status and Shutdown
    # ========================================================================
    
    def get_status(self) -> Dict[str, Any]:
        """Get agent status"""
        return {
            'total_tasks': self.total_tasks,
            'successful_tasks': self.successful_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'pipeline_stats': self.pipeline_selector.get_pipeline_stats(),
            'system_state': self._get_system_state(),
            'running': self.running
        }
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Enhanced Green Agent Runner...")
        self.running = False
        
        if self.bio_core:
            await self.bio_core.shutdown()
        
        logger.info("Shutdown complete")
