# src/optimization/ml_optimizer.py (EXTENDED)

from typing import Dict, Optional, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class HeliumOptimizationMode(Enum):
    """Optimization modes based on helium availability"""
    AGGRESSIVE = "aggressive"   # Severe helium scarcity
    MODERATE = "moderate"        # Helium caution/critical
    LIGHT = "light"              # Normal conditions
    NONE = "none"                # Helium green

class HeliumAwareMLOptimizer:
    """
    ML optimization engine that adjusts strategies based on helium scarcity
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Optimization strategies for different helium scenarios
        self.strategies = {
            HeliumOptimizationMode.AGGRESSIVE: {
                'quantization_precision': 'int4',
                'pruning_ratio': 0.5,          # Remove 50% of weights
                'use_distillation': True,
                'distillation_temperature': 2.5,
                'target_hardware': 'cpu',
                'batch_size_multiplier': 2.0,   # Larger batches to reduce GPU calls
                'cache_ttl_hours': 72          # Cache longer during scarcity
            },
            HeliumOptimizationMode.MODERATE: {
                'quantization_precision': 'int8',
                'pruning_ratio': 0.3,
                'use_distillation': True,
                'distillation_temperature': 1.5,
                'target_hardware': 'gpu_mixed',
                'batch_size_multiplier': 1.5,
                'cache_ttl_hours': 24
            },
            HeliumOptimizationMode.LIGHT: {
                'quantization_precision': 'fp16',
                'pruning_ratio': 0.1,
                'use_distillation': False,
                'target_hardware': 'gpu',
                'batch_size_multiplier': 1.0,
                'cache_ttl_hours': 12
            },
            HeliumOptimizationMode.NONE: {
                'quantization_precision': 'fp32',
                'pruning_ratio': 0.0,
                'use_distillation': False,
                'target_hardware': 'gpu',
                'batch_size_multiplier': 1.0,
                'cache_ttl_hours': 6
            }
        }
        
        self.current_mode = HeliumOptimizationMode.NONE
    
    def set_helium_mode(self, helium_zone):
        """Set optimization mode based on helium zone"""
        
        if helium_zone in ['helium_critical']:
            self.current_mode = HeliumOptimizationMode.AGGRESSIVE
        elif helium_zone in ['helium_red']:
            self.current_mode = HeliumOptimizationMode.MODERATE
        elif helium_zone in ['helium_yellow']:
            self.current_mode = HeliumOptimizationMode.LIGHT
        else:
            self.current_mode = HeliumOptimizationMode.NONE
        
        logger.info(f"Helium optimization mode set to: {self.current_mode.value}")
        return self.current_mode
    
    def optimize_model(self, model: Any, execution_decision) -> Dict[str, Any]:
        """
        Apply helium-aware optimizations to the model
        """
        
        # Determine optimization mode from execution decision
        if execution_decision.helium_aware_flag and execution_decision.helium_zone:
            self.set_helium_mode(execution_decision.helium_zone.value)
        
        strategy = self.strategies[self.current_mode]
        
        optimization_results = {
            'original_model_size': self._get_model_size(model),
            'optimizations_applied': [],
            'estimated_helium_savings': 0.0,
            'estimated_accuracy_impact': 0.0
        }
        
        # Apply quantization if needed
        if strategy['quantization_precision'] != 'fp32':
            model = self._apply_quantization(model, strategy['quantization_precision'])
            optimization_results['optimizations_applied'].append(
                f"quantization_{strategy['quantization_precision']}"
            )
            optimization_results['estimated_helium_savings'] += 0.3
        
        # Apply pruning
        if strategy['pruning_ratio'] > 0:
            model = self._apply_pruning(model, strategy['pruning_ratio'])
            optimization_results['optimizations_applied'].append(
                f"pruning_{strategy['pruning_ratio']*100:.0f}%"
            )
            optimization_results['estimated_helium_savings'] += 0.2
            optimization_results['estimated_accuracy_impact'] += (strategy['pruning_ratio'] * 0.05)
        
        # Apply knowledge distillation
        if strategy['use_distillation']:
            model = self._apply_distillation(model, strategy['distillation_temperature'])
            optimization_results['optimizations_applied'].append(
                f"distillation_temp_{strategy['distillation_temperature']}"
            )
            optimization_results['estimated_helium_savings'] += 0.25
        
        optimization_results['optimized_model_size'] = self._get_model_size(model)
        optimization_results['model'] = model
        
        return optimization_results
    
    def _get_model_size(self, model) -> float:
        """Get model size in MB"""
        try:
            import sys
            return sys.getsizeof(model) / (1024 * 1024)  # Convert to MB
        except:
            return 0.0
    
    def _apply_quantization(self, model, precision: str):
        """Apply model quantization"""
        # Placeholder - in production, use torch.quantization or similar
        logger.info(f"Applying {precision} quantization to model")
        
        # Simulate quantization
        if precision == 'int4':
            model = f"{model}_int4_quantized"
        elif precision == 'int8':
            model = f"{model}_int8_quantized"
        elif precision == 'fp16':
            model = f"{model}_fp16_quantized"
        
        return model
    
    def _apply_pruning(self, model, ratio: float):
        """Apply weight pruning"""
        logger.info(f"Applying {ratio*100:.0f}% pruning to model")
        return f"{model}_pruned_{int(ratio*100)}"
    
    def _apply_distillation(self, model, temperature: float):
        """Apply knowledge distillation"""
        logger.info(f"Applying distillation with temperature {temperature}")
        return f"{model}_distilled_temp_{temperature}"


class HeliumAwareDataOptimizer:
    """
    Data optimization with helium-aware caching and batching
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
    
    def optimize_data_pipeline(self, dataset: Any, execution_decision) -> Dict[str, Any]:
        """
        Optimize data pipeline based on helium constraints
        """
        
        # Determine batch size based on helium zone
        if execution_decision.helium_aware_flag and execution_decision.helium_zone:
            helium_zone = execution_decision.helium_zone.value
            
            if helium_zone == 'helium_critical':
                batch_multiplier = 2.5
                cache_ttl = 3600 * 72  # 72 hours
                use_mmap = True
            elif helium_zone == 'helium_red':
                batch_multiplier = 2.0
                cache_ttl = 3600 * 48
                use_mmap = True
            elif helium_zone == 'helium_yellow':
                batch_multiplier = 1.5
                cache_ttl = 3600 * 24
                use_mmap = False
            else:
                batch_multiplier = 1.0
                cache_ttl = 3600 * 6
                use_mmap = False
        else:
            batch_multiplier = 1.0
            cache_ttl = 3600 * 6
            use_mmap = False
        
        optimization_result = {
            'batch_size': self._get_optimal_batch_size(dataset) * batch_multiplier,
            'cache_ttl_seconds': cache_ttl,
            'use_memory_mapping': use_mmap,
            'compression_enabled': batch_multiplier > 1.0,
            'estimated_savings': 0.2 if batch_multiplier > 1 else 0.0
        }
        
        return optimization_result
    
    def _get_optimal_batch_size(self, dataset) -> int:
        """Determine optimal batch size for dataset"""
        # Placeholder - in production, analyze dataset characteristics
        return 32
    
    def get_cached_result(self, key: str):
        """Retrieve from cache if available"""
        if key in self.cache:
            self.cache_hits += 1
            return self.cache[key]
        else:
            self.cache_misses += 1
            return None
    
    def cache_result(self, key: str, value: Any, ttl_seconds: int):
        """Cache result with TTL"""
        import time
        self.cache[key] = {
            'value': value,
            'expires_at': time.time() + ttl_seconds
        }
    
    def get_cache_stats(self) -> Dict:
        """Get cache hit/miss statistics"""
        total = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total if total > 0 else 0
        
        return {
            'hits': self.cache_hits,
            'misses': self.cache_misses,
            'hit_rate': hit_rate
        }
