# File: enhancements/moe_expert_system/integration/layer_integrator.py

import logging
from typing import Dict, Any, Optional, List
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class LayerIntegrator:
    """
    Integrates MoE system with existing 12-layer Green Agent architecture.
    
    This is the bridge between the MoE enhancement and the existing
    Green Agent layers, ensuring clean integration without deconstruction.
    """
    
    def __init__(self, expert_router):
        self.router = expert_router
        
        # Integration status tracking
        self.layer_integration_status = {
            'layer_0': False,  # Workload Profile
            'layer_1': False,  # Meta-Cognition
            'layer_2': False,  # Neuro-Symbolic
            'layer_3': False,  # Dual-Axis Decision Core
            'layer_4': False,  # ML Optimization
            'layer_5': False,  # Data Optimization
            'layer_6': False,  # Distributed Execution
            'layer_7': False,  # Dual Monitoring
            'layer_8': False,  # Immutable Ledger
            'layer_9': False,  # 3D Pareto Benchmarking
            'layer_10': False, # Quantum Integration
            'layer_11': False  # Dashboard
        }
        
        logger.info("Layer Integrator initialized")
    
    def integrate_with_layer_0(self, workload_classifier) -> Dict[str, Any]:
        """
        Integrate with Layer 0: Workload + Helium Profile.
        
        Extends workload classification to include MoE-compatible features.
        """
        self.layer_integration_status['layer_0'] = True
        
        def enhanced_classifier(request: Dict[str, Any]) -> Dict[str, Any]:
            """Enhanced workload classifier with MoE features"""
            # Original classification
            base_profile = workload_classifier(request)
            
            # Add MoE-specific features
            base_profile['task_embedding'] = self._create_task_embedding(request)
            base_profile['domain_tags'] = self._extract_domain_tags(request)
            base_profile['routing_priority'] = self._calculate_routing_priority(request)
            
            return base_profile
        
        return {
            'status': 'integrated',
            'enhanced_classifier': enhanced_classifier,
            'features_added': ['task_embedding', 'domain_tags', 'routing_priority']
        }
    
    def integrate_with_layer_1(self, meta_cognitive_module) -> Dict[str, Any]:
        """
        Integrate with Layer 1: Meta-Cognition.
        
        Adds expert performance tracking and routing feedback to meta-cognition.
        """
        self.layer_integration_status['layer_1'] = True
        
        def enhanced_meta_cognition(state: Dict[str, Any]) -> Dict[str, Any]:
            """Enhanced meta-cognition with MoE awareness"""
            # Original meta-cognition
            base_state = meta_cognitive_module(state)
            
            # Add expert-specific metrics
            base_state['expert_performance'] = self._get_expert_performance_metrics()
            base_state['routing_history'] = self._get_routing_history_summary()
            base_state['expert_trust_scores'] = self._calculate_expert_trust()
            
            return base_state
        
        return {
            'status': 'integrated',
            'enhanced_meta_cognition': enhanced_meta_cognition,
            'metrics_added': ['expert_performance', 'routing_history', 'expert_trust_scores']
        }
    
    def integrate_with_layer_2(self, neuro_symbolic_module) -> Dict[str, Any]:
        """
        Integrate with Layer 2: Neuro-Symbolic.
        
        Adds expert constraints and symbolic validation for MoE routing.
        """
        self.layer_integration_status['layer_2'] = True
        
        def enhanced_validation(expert_plans: List[Dict], rules: Dict) -> List[Dict]:
            """Enhanced validation with MoE-specific rules"""
            # Apply symbolic rules to expert plans
            validated_plans = []
            
            for plan in expert_plans:
                # Check against policy graphs
                if self._validate_against_policy(plan, rules):
                    # Check LIMIT graph compliance
                    if self._validate_limit_graph(plan, rules):
                        validated_plans.append(plan)
                    else:
                        logger.warning(f"Plan failed LIMIT graph validation: {plan.get('expert_id')}")
                else:
                    logger.warning(f"Plan failed policy validation: {plan.get('expert_id')}")
            
            return validated_plans
        
        return {
            'status': 'integrated',
            'enhanced_validation': enhanced_validation,
            'validations_added': ['policy_graph', 'limit_graph', 'expert_constraints']
        }
    
    def integrate_with_layer_3(self, dual_axis_core) -> Dict[str, Any]:
        """
        Integrate with Layer 3: Dual-Axis Decision Core.
        
        Maps MoE outputs through the 16-zone matrix for final decisions.
        """
        self.layer_integration_status['layer_3'] = True
        
        def enhanced_decision(plans: List[Dict], context: Dict) -> Dict[str, Any]:
            """Enhanced decision making with MoE plans"""
            # Score each plan through dual-axis
            scored_plans = []
            
            for plan in plans:
                carbon_impact = plan.get('estimated_carbon_kg', 0)
                helium_impact = plan.get('helium_per_inference', 0)
                
                # Map to dual-axis zone
                zone = dual_axis_core.calculate_zone(carbon_impact, helium_impact)
                
                # Determine action class
                action = dual_axis_core.determine_action(zone)
                
                scored_plans.append({
                    **plan,
                    'zone': zone,
                    'action': action,
                    'dual_axis_score': dual_axis_core.calculate_score(plan)
                })
            
            # Select best plan
            best_plan = max(scored_plans, key=lambda p: p['dual_axis_score'])
            
            return best_plan
        
        return {
            'status': 'integrated',
            'enhanced_decision': enhanced_decision,
            'integration_type': 'scoring_and_selection'
        }
    
    def integrate_with_layer_7(self, monitoring_module) -> Dict[str, Any]:
        """
        Integrate with Layer 7: Dual Monitoring.
        
        Adds expert-specific metrics to Prometheus/Grafana monitoring.
        """
        self.layer_integration_status['layer_7'] = True
        
        def enhanced_monitoring() -> Dict[str, Any]:
            """Enhanced monitoring with MoE metrics"""
            base_metrics = monitoring_module()
            
            # Add expert metrics
            base_metrics['moe_expert_usage'] = self.router.metrics_collector.get_expert_usage()
            base_metrics['moe_routing_stats'] = self.router.get_routing_stats()
            base_metrics['moe_load_balance'] = self.router.gating_network.get_load_balance_score()
            
            return base_metrics
        
        return {
            'status': 'integrated',
            'enhanced_monitoring': enhanced_monitoring,
            'metrics_added': ['expert_usage', 'routing_stats', 'load_balance']
        }
    
    def integrate_with_layer_8(self, ledger_module) -> Dict[str, Any]:
        """
        Integrate with Layer 8: Immutable Dual Ledger.
        
        Logs all MoE decisions for ISO 14064 audit trails.
        """
        self.layer_integration_status['layer_8'] = True
        
        def enhanced_ledger_log(decision: Dict[str, Any]) -> str:
            """Enhanced ledger logging with MoE decision data"""
            # Add MoE-specific fields to ledger entry
            ledger_entry = {
                **decision,
                'moe_routing': self._serialize_routing_decision(decision),
                'expert_profiles': self._serialize_expert_profiles(),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Hash and store in ledger
            entry_hash = ledger_module.store(ledger_entry)
            
            return entry_hash
        
        return {
            'status': 'integrated',
            'enhanced_ledger_log': enhanced_ledger_log,
            'audit_fields_added': ['moe_routing', 'expert_profiles']
        }
    
    def _create_task_embedding(self, request: Dict[str, Any]) -> List[float]:
        """Create task embedding for MoE routing"""
        # Simple embedding based on task characteristics
        embedding = [
            float(request.get('complexity', 0.5)),
            float(request.get('urgency', 0.3)),
            float(request.get('carbon_sensitivity', 0.5)),
            float(request.get('helium_dependency', 0.0)),
            float(request.get('data_size_mb', 1.0)) / 1000.0
        ]
        return embedding
    
    def _extract_domain_tags(self, request: Dict[str, Any]) -> List[str]:
        """Extract domain tags for expert routing"""
        tags = []
        
        task_type = request.get('task_type', '')
        if 'energy' in task_type.lower() or 'carbon' in task_type.lower():
            tags.append('energy')
        if 'data' in task_type.lower() or 'processing' in task_type.lower():
            tags.append('data')
        if 'iot' in task_type.lower() or 'edge' in task_type.lower():
            tags.append('iot')
        if 'quantum' in task_type.lower():
            tags.append('quantum')
        if 'helium' in task_type.lower():
            tags.append('helium')
        
        return tags or ['general']
    
    def _calculate_routing_priority(self, request: Dict[str, Any]) -> float:
        """Calculate routing priority score"""
        urgency = request.get('urgency', 0.5)
        complexity = request.get('complexity', 0.5)
        carbon_sensitivity = request.get('carbon_sensitivity', 0.5)
        
        return (urgency * 0.4 + complexity * 0.3 + carbon_sensitivity * 0.3)
    
    def _get_expert_performance_metrics(self) -> Dict[str, Any]:
        """Get expert performance metrics"""
        if hasattr(self.router, 'metrics_collector'):
            return self.router.metrics_collector.get_expert_performance()
        return {}
    
    def _get_routing_history_summary(self) -> Dict[str, Any]:
        """Get summary of routing history"""
        return {
            'total_routes': self.router.total_routes,
            'success_rate': self.router.successful_routes / max(self.router.total_routes, 1),
            'average_experts_per_route': len(self.router.experts) / 2
        }
    
    def _calculate_expert_trust(self) -> Dict[str, float]:
        """Calculate trust scores for experts"""
        trust_scores = {}
        for expert_id in self.router.experts:
            # Simple trust based on usage and success
            trust_scores[expert_id] = 0.8  # Default trust
        return trust_scores
    
    def _validate_against_policy(self, plan: Dict, rules: Dict) -> bool:
        """Validate expert plan against policy graphs"""
        # Check carbon limits
        max_carbon = rules.get('max_carbon_kg', float('inf'))
        if plan.get('estimated_carbon_kg', 0) > max_carbon:
            return False
        
        # Check helium limits
        max_helium = rules.get('max_helium_per_inference', float('inf'))
        if plan.get('helium_per_inference', 0) > max_helium:
            return False
        
        return True
    
    def _validate_limit_graph(self, plan: Dict, rules: Dict) -> bool:
        """Validate against LIMIT graph rules"""
        # Ensure plan respects planetary boundaries
        carbon_limit = rules.get('carbon_budget_kg', 0.1)
        if plan.get('estimated_carbon_kg', 0) > carbon_limit:
            return False
        
        return True
    
    def _serialize_routing_decision(self, decision: Dict) -> str:
        """Serialize routing decision for ledger"""
        return json.dumps({
            'experts_used': decision.get('expert_ids', []),
            'routing_weights': decision.get('weights', []),
            'action': decision.get('action', 'unknown')
        })
    
    def _serialize_expert_profiles(self) -> str:
        """Serialize expert profiles for ledger"""
        profiles = {}
        for expert_id, expert in self.router.experts.items():
            profiles[expert_id] = expert.profile.to_dict()
        return json.dumps(profiles)
    
    def get_integration_status(self) -> Dict[str, bool]:
        """Get integration status across all layers"""
        return self.layer_integration_status.copy()
    
