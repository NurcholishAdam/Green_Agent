# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/integration/enhanced_work_integration.py
# Enhanced section for metrics-driven pipeline selection

"""
Enhanced Work Integration with Metrics-Driven Pipeline Selection
Version: 2.0.0

Now consumes expert_metrics.py for:
- Dynamic pipeline selection based on real-time metrics
- Automatic pipeline switching during degradation
- Health-aware work routing
"""

class MetricsDrivenPipelineSelector:
    """
    Selects pipelines based on real-time metrics from expert_metrics.py.
    
    Enables dynamic pipeline adaptation based on system health.
    """
    
    def __init__(self):
        self.metrics_collector = None  # Will be injected
        self.pipeline_performance: Dict[str, List[float]] = {}
        self.last_pipeline_selection: Dict[str, Any] = {}
        self.selection_history: List[Dict] = []
        
        # Pipeline suitability scores based on conditions
        self.pipeline_suitability = {
            'standard': {
                'healthy_experts': 1.0,
                'degraded_experts': 0.5,
                'high_carbon': 0.7,
                'low_latency': 0.9
            },
            'quantum_enhanced': {
                'healthy_experts': 0.8,
                'degraded_experts': 0.3,
                'high_carbon': 0.9,
                'low_latency': 0.4
            },
            'helium_optimized': {
                'healthy_experts': 0.9,
                'degraded_experts': 0.6,
                'high_carbon': 0.8,
                'low_latency': 0.7
            },
            'energy_efficient': {
                'healthy_experts': 0.9,
                'degraded_experts': 0.7,
                'high_carbon': 1.0,
                'low_latency': 0.5
            },
            'meta_cognitive': {
                'healthy_experts': 0.7,
                'degraded_experts': 0.8,
                'high_carbon': 0.6,
                'low_latency': 0.3
            }
        }
        
        logger.info("MetricsDrivenPipelineSelector initialized")
    
    def inject_metrics_collector(self, collector: Any):
        """Inject metrics collector"""
        self.metrics_collector = collector
        logger.info("Metrics collector injected into pipeline selector")
    
    async def select_optimal_pipeline(
        self,
        task: Dict[str, Any],
        default_pipeline: str = 'standard'
    ) -> str:
        """
        Select optimal pipeline based on real-time metrics.
        
        Returns pipeline name.
        """
        if not self.metrics_collector:
            return default_pipeline
        
        try:
            # Get current system health
            health_scores = self.metrics_collector.get_health_scores()
            
            # Get SLO status
            slo_status = {}
            if hasattr(self.metrics_collector, 'get_slo_status'):
                slo_status = self.metrics_collector.get_slo_status()
            
            # Get predictions
            predictions = {}
            if hasattr(self.metrics_collector, 'get_predictions'):
                predictions = self.metrics_collector.get_predictions()
            
            # Assess current conditions
            conditions = self._assess_conditions(
                health_scores, slo_status, predictions, task
            )
            
            # Score each pipeline
            pipeline_scores = {}
            for pipeline, suitability in self.pipeline_suitability.items():
                score = 0.0
                for condition, value in conditions.items():
                    if condition in suitability:
                        score += suitability[condition] * value
                
                pipeline_scores[pipeline] = score
            
            # Select best pipeline
            best_pipeline = max(pipeline_scores, key=pipeline_scores.get)
            
            # Record selection
            self.last_pipeline_selection = {
                'pipeline': best_pipeline,
                'conditions': conditions,
                'scores': pipeline_scores,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.selection_history.append(self.last_pipeline_selection)
            
            # Keep history manageable
            if len(self.selection_history) > 1000:
                self.selection_history = self.selection_history[-1000:]
            
            logger.debug(
                f"Metrics-driven pipeline selection: {best_pipeline} "
                f"(scores: {pipeline_scores})"
            )
            
            return best_pipeline
            
        except Exception as e:
            logger.error(f"Pipeline selection error: {str(e)}")
            return default_pipeline
    
    def _assess_conditions(
        self,
        health_scores: Dict[str, float],
        slo_status: Dict[str, Any],
        predictions: Dict[str, Any],
        task: Dict[str, Any]
    ) -> Dict[str, float]:
        """Assess current system conditions from metrics"""
        conditions = {
            'healthy_experts': 0.0,
            'degraded_experts': 0.0,
            'high_carbon': 0.0,
            'low_latency': 0.0
        }
        
        # Health assessment
        if health_scores:
            healthy_count = sum(1 for h in health_scores.values() if h > 0.7)
            degraded_count = sum(1 for h in health_scores.values() if h < 0.5)
            total = len(health_scores)
            
            conditions['healthy_experts'] = healthy_count / max(total, 1)
            conditions['degraded_experts'] = degraded_count / max(total, 1)
        
        # SLO assessment
        if slo_status:
            breached = sum(
                1 for s in slo_status.values()
                if s.get('status') == 'breached'
            )
            if breached > 0:
                conditions['high_carbon'] = 0.8
                conditions['low_latency'] = 0.9
        
        # Prediction assessment
        if predictions:
            degrading = sum(
                1 for p in predictions.values()
                if p.get('trend') == 'degrading'
            )
            if degrading > 0:
                conditions['degraded_experts'] = max(
                    conditions['degraded_experts'],
                    degrading / max(len(predictions), 1)
                )
        
        # Task-specific adjustments
        task_type = task.get('task_type', 'general')
        if task_type in ['optimization', 'simulation']:
            conditions['high_carbon'] = max(conditions['high_carbon'], 0.5)
        if task_type in ['inference', 'streaming']:
            conditions['low_latency'] = max(conditions['low_latency'], 0.7)
        
        return conditions
    
    def get_selection_stats(self) -> Dict[str, Any]:
        """Get pipeline selection statistics"""
        if not self.selection_history:
            return {}
        
        pipeline_counts = {}
        for selection in self.selection_history:
            pipeline = selection['pipeline']
            pipeline_counts[pipeline] = pipeline_counts.get(pipeline, 0) + 1
        
        total = len(self.selection_history)
        
        return {
            'total_selections': total,
            'pipeline_distribution': {
                p: c / total for p, c in pipeline_counts.items()
            },
            'last_selection': self.last_pipeline_selection,
            'recent_selections': self.selection_history[-10:]
        }


# Add to EnhancedWorkIntegrator class:

class EnhancedWorkIntegrator:
    """
    Add these methods to the existing EnhancedWorkIntegrator class.
    """
    
    def __init__(self, *args, **kwargs):
        # ... existing initialization ...
        
        # NEW: Metrics-driven pipeline selector
        self.pipeline_selector = MetricsDrivenPipelineSelector()
        
        # NEW: Auto-select pipeline based on metrics
        self.auto_select_pipeline = kwargs.get('auto_select_pipeline', True)
    
    def inject_metrics_collector(self, collector: Any):
        """Inject metrics collector for pipeline selection"""
        self.pipeline_selector.inject_metrics_collector(collector)
    
    async def process_work_with_metrics_selection(
        self,
        work_request: Dict[str, Any],
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """
        Process work with metrics-driven pipeline selection.
        
        This is the key integration point.
        """
        # Select pipeline based on metrics
        if self.auto_select_pipeline:
            pipeline_type = await self.pipeline_selector.select_optimal_pipeline(
                work_request,
                default_pipeline=work_request.get('pipeline_type', 'standard')
            )
        else:
            pipeline_type = work_request.get('pipeline_type', 'standard')
        
        logger.info(
            f"Selected pipeline '{pipeline_type}' for task "
            f"{work_request.get('task_id', 'unknown')}"
        )
        
        # Process with selected pipeline
        result = await self.process_work(
            work_request,
            pipeline_type=pipeline_type,
            tenant_id=tenant_id
        )
        
        # Add pipeline selection metadata
        result['pipeline_selection'] = {
            'selected_pipeline': pipeline_type,
            'auto_selected': self.auto_select_pipeline,
            'selection_conditions': self.pipeline_selector.last_pipeline_selection.get('conditions', {}),
            'selection_scores': self.pipeline_selector.last_pipeline_selection.get('scores', {})
        }
        
        # Record pipeline performance for future selections
        if pipeline_type not in self.pipeline_selector.pipeline_performance:
            self.pipeline_selector.pipeline_performance[pipeline_type] = []
        
        self.pipeline_selector.pipeline_performance[pipeline_type].append(
            1.0 if result.get('success') else 0.0
        )
        
        return result
    
    def get_pipeline_selection_stats(self) -> Dict[str, Any]:
        """Get pipeline selection statistics"""
        return self.pipeline_selector.get_selection_stats()
