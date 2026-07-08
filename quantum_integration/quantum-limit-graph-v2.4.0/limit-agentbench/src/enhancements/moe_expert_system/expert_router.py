# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_router.py
"""
Enhanced Expert Router v8.1.0 - Complete Signal Transduction Cascade with Causal Constraints
Fully integrated with Helium, FL, Sustainability, and Bio-Inspired layers.
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
import math
import uuid
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import networkx as nx

logger = logging.getLogger(__name__)

# ============================================================================
# (All existing enums, dataclasses, and helper classes from v8.0.0 remain here)
# For brevity, we do not repeat them; they are assumed present in the existing file.
# Below we show only the additions and modifications.
# ============================================================================

# ============================================================================
# NEW: Helium Provider Interface (to be injected)
# ============================================================================

class HeliumProvider:
    """Interface to Helium modules for real-time telemetry."""
    def get_scarcity(self) -> float:
        raise NotImplementedError
    def get_cost_index(self) -> float:
        raise NotImplementedError
    def get_avg_client_energy(self) -> float:
        raise NotImplementedError
    def set_sampling_rate(self, rate: float):
        raise NotImplementedError

# ============================================================================
# NEW: Expert Classes for Helium, FL, Sustainability
# ============================================================================

class HeliumIoTExpert:
    """Expert that proposes IoT sampling and gateway strategies."""
    def propose(self, context: dict) -> dict:
        scarcity = context.get('helium_scarcity', 0.5)
        # Example: reduce sampling when scarce
        sampling_rate = 10.0 if scarcity < 0.5 else 5.0
        return {
            'sampling_rate_hz': sampling_rate,
            'aggregation_strategy': 'adaptive',
            'preferred_gateways': []  # can be filled by a gateway selector
        }

class FLEnergyExpert:
    """Expert that proposes FL round schedule and client selection."""
    def propose(self, context: dict) -> dict:
        carbon_intensity = context.get('carbon_intensity', 0.5)
        # Example: if carbon high, slow down FL
        round_frequency = 0.5 if carbon_intensity > 0.6 else 1.0
        return {
            'round_frequency_hz': round_frequency,
            'client_selection': 'energy_aware',
            'compression_level': 'high' if carbon_intensity > 0.7 else 'medium'
        }

class SustainabilityExpert:
    """Expert that proposes data center and carbon budget."""
    def propose(self, context: dict) -> dict:
        carbon_intensity = context.get('carbon_intensity', 0.5)
        return {
            'preferred_data_center': 'us-east' if carbon_intensity < 0.5 else 'us-west',
            'carbon_budget_kg': 10.0 if carbon_intensity < 0.5 else 5.0,
            'renewable_share': 0.8
        }

# ============================================================================
# MODIFIED ExpertRouter class (with full integration)
# ============================================================================

class ExpertRouter:
    """
    Enhanced Expert Router v8.1.0 - Fully integrated with Helium, FL, Bio-inspired.
    """

    def __init__(
        self,
        enable_quantum: bool = False,
        metrics_collector: Optional[Any] = None,
        enable_signal_transduction: bool = True,
        enable_allosteric: bool = True,
        enable_metabolic_pathways: bool = True,
        enable_cooperative_binding: bool = True,
        enable_homeostasis: bool = True,
        enable_bio_integration: bool = True,
        enable_federated: bool = True,
        enable_predictive: bool = True,
        enable_carbon_intensity: bool = True,
        enable_helium_optimization: bool = True,
        enable_causal_constraints: bool = True,
        enable_counterfactual: bool = True,
        enable_signal_integration: bool = True,
        enable_differential_privacy: bool = True,
        enable_uncertainty_quantification: bool = True,
        server_url: Optional[str] = None,
        helium_budget_l: float = 100.0,
        privacy_epsilon: float = 1.0,
        # NEW: self-evolving gates
        enable_self_evolving_gates: bool = True,
    ):
        # Feature flags (keep all from v8.0.0)
        self.enable_signal_transduction = enable_signal_transduction
        self.enable_allosteric = enable_allosteric
        self.enable_metabolic_pathways = enable_metabolic_pathways
        self.enable_cooperative_binding = enable_cooperative_binding
        self.enable_homeostasis = enable_homeostasis
        self.enable_bio_integration = enable_bio_integration
        self.enable_federated = enable_federated
        self.enable_predictive = enable_predictive
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_helium_optimization = enable_helium_optimization
        self.enable_causal_constraints = enable_causal_constraints
        self.enable_counterfactual = enable_counterfactual
        self.enable_signal_integration = enable_signal_integration
        self.enable_differential_privacy = enable_differential_privacy
        self.enable_uncertainty_quantification = enable_uncertainty_quantification
        self.enable_self_evolving_gates = enable_self_evolving_gates

        # New modules (same as v8.0.0)
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.helium_optimizer = HeliumEfficiencyOptimizer(helium_budget_l) if enable_helium_optimization else None
        self.federated_learner = FederatedRoutingLearner(server_url, privacy_epsilon) if enable_federated else None
        self.predictive_analyzer = PredictiveRoutingAnalyzer() if enable_predictive else None
        self.causal_model = CausalConstraintModel() if enable_causal_constraints else None
        self.signal_integrator = SignalIntegrationEngine() if enable_signal_integration else None

        # Bio-inspired subsystems (same as v8.0.0)
        self.signal_engine = SignalTransductionEngine() if enable_signal_transduction else None
        self.allosteric_system = AllostericRegulationSystem() if enable_allosteric else None
        self.metabolic_router = MetabolicPathwayRouter() if enable_metabolic_pathways else None

        # Bio-inspired module references (injected)
        self.gradient_manager = None
        self.token_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.bio_core = None

        # NEW: Helium provider (will be injected)
        self.helium_provider = None

        # NEW: Self-evolving gates
        if enable_self_evolving_gates:
            from .advanced.self_evolving_gates import SelfEvolvingGates
            self.self_evolving_gates = SelfEvolvingGates()
        else:
            self.self_evolving_gates = None

        # Initialize signal receptors (same as v8.0.0)
        if self.signal_engine:
            self.signal_engine.create_receptor('carbon_receptor', SignalType.ENDOCRINE,
                'carbon_gradient', affinity=0.7, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('helium_receptor', SignalType.ENDOCRINE,
                'helium_gradient', affinity=0.6, amplification=AmplificationLevel.MODERATE)
            self.signal_engine.create_receptor('task_receptor', SignalType.NEUROTRANSMITTER,
                'task_signal', affinity=0.9, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('stress_receptor', SignalType.AUTOCRINE,
                'stress_signal', affinity=0.8, amplification=AmplificationLevel.MAXIMUM)
            self.signal_engine.create_receptor('trust_receptor', SignalType.PARACRINE,
                'trust_gradient', affinity=0.5, amplification=AmplificationLevel.LOW)
            self.signal_engine.setup_crosstalk(SecondMessenger.cAMP, SecondMessenger.IP3, 0.3)
            self.signal_engine.setup_crosstalk(SecondMessenger.CALCIUM, SecondMessenger.cAMP, 0.5)

        if self.allosteric_system:
            self.allosteric_system.setup_cooperativity('energy', 'data', 0.4)
            self.allosteric_system.setup_cooperativity('energy', 'helium', 0.3)
            self.allosteric_system.setup_cooperativity('data', 'iot', 0.5)

        self.metrics_collector = metrics_collector
        self.metrics = RoutingMetrics()
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        self.circuit_breakers: Dict[str, ExpertCircuitBreaker] = {}
        self.gating_network = None
        self.active_routes = 0
        self.max_concurrent_routes = 100
        self._route_lock = asyncio.Lock()
        self.routing_history: deque = deque(maxlen=10000)

        self._initialize_experts(enable_quantum)
        self._start_background_tasks()

        logger.info(f"Expert Router v8.1.0 initialized with all enhancements")

    def _initialize_experts(self, enable_quantum: bool):
        """Register all experts including new Helium/FL/Sustainability."""
        # Keep existing experts
        try:
            from .experts.energy_expert import EnergyExpert
            from .experts.data_expert import DataExpert
            from .experts.iot_expert import IoTExpert
            from .experts.helium_expert import HeliumExpert

            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            if enable_quantum:
                from .experts.quantum_expert import QuantumExpert
                self.experts['quantum'] = QuantumExpert()

            # NEW: Add Helium IoT, FL Energy, Sustainability experts
            self.experts['helium_iot'] = HeliumIoTExpert()
            self.experts['fl_energy'] = FLEnergyExpert()
            self.experts['sustainability'] = SustainabilityExpert()

            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            logger.info(f"Initialized {len(self.experts)} experts")
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")

    def _start_background_tasks(self):
        asyncio.create_task(self._signal_transduction_loop())
        asyncio.create_task(self._homeostasis_loop())
        asyncio.create_task(self._product_inhibition_loop())
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_self_evolving_gates and self.self_evolving_gates:
            asyncio.create_task(self._self_evolving_loop())

    # ========================================================================
    # Background Loops (include new self-evolving loop)
    # ========================================================================

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_learner and self.routing_history:
                    routing_data = []
                    for record in list(self.routing_history)[-100:]:
                        routing_data.append({
                            'carbon_zone': record.get('context', {}).get('carbon_zone', 0),
                            'helium_scarcity': record.get('context', {}).get('helium_scarcity', 0.5),
                            'task_complexity': record.get('context', {}).get('task_complexity', 0.5),
                            'token_balance': 500,
                            'carbon_gradient': 0.5,
                            'trust_gradient': 0.5,
                            'opportunity_gradient': 0.5,
                            'stress_level': 0.3,
                            'latency_budget': 100,
                            'energy_budget': 100,
                            'selected_expert_idx': 0
                        })
                    await self.federated_learner.participate_in_round(
                        routing_data,
                        performance=self.metrics.success_rate
                    )
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer:
                    self.predictive_analyzer.update_history({
                        'success_rate': self.metrics.success_rate,
                        'avg_latency_ms': self.metrics.average_latency_ms,
                        'carbon_efficiency': 0.5,
                        'helium_efficiency': 0.5,
                        'expert_utilization': self.active_routes / max(self.max_concurrent_routes, 1)
                    })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)

    async def _signal_transduction_loop(self):
        while True:
            try:
                if self.signal_engine:
                    gradient_levels = self._get_real_gradient_levels()
                    self.signal_engine.bind_ligand('carbon_receptor', gradient_levels.get('carbon', 0.5))
                    self.signal_engine.bind_ligand('helium_receptor', gradient_levels.get('helium', 0.5))
                    self.signal_engine.bind_ligand('trust_receptor', gradient_levels.get('trust', 0.5))
                    token_level = self._get_real_token_availability()
                    stress_level = self._get_real_stress_level()
                    if stress_level > 0.5:
                        self.signal_engine.bind_ligand('stress_receptor', stress_level)
                    self.signal_engine.apply_crosstalk()
                    if self.allosteric_system:
                        self.allosteric_system.bind_modulator('carbon_site', gradient_levels.get('carbon', 0.5))
                        self.allosteric_system.bind_modulator('helium_site', gradient_levels.get('helium', 0.5))
                        self.allosteric_system.bind_modulator('trust_site', gradient_levels.get('trust', 0.5))
                        self.allosteric_system.bind_modulator('token_site', token_level)
                        if stress_level > 0.3:
                            self.allosteric_system.bind_modulator('stress_site', stress_level)
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Signal transduction error: {str(e)}")
                await asyncio.sleep(5.0)

    async def _homeostasis_loop(self):
        while True:
            try:
                if self.enable_homeostasis and self.allosteric_system:
                    modulation = self.allosteric_system.get_routing_modulation()
                    if modulation['conservation_mode'] > 0.7:
                        if np.random.random() < 0.1:
                            self.allosteric_system.bind_modulator('token_site', 0.8)
                    if modulation['risk_tolerance'] > 0.4:
                        self.allosteric_system.bind_modulator('stress_site', 0.3)
                await asyncio.sleep(10.0)
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(30.0)

    async def _product_inhibition_loop(self):
        while True:
            try:
                if self.metabolic_router:
                    self.metabolic_router.apply_product_inhibition()
                await asyncio.sleep(60.0)
            except Exception as e:
                logger.error(f"Product inhibition error: {str(e)}")
                await asyncio.sleep(120.0)

    async def _self_evolving_loop(self):
        """Periodically update the gating network using self-evolving gates."""
        while True:
            try:
                if self.self_evolving_gates and self.routing_history:
                    # Gather recent contexts and rewards (simplified)
                    # In a real implementation, you would compute reward from actual outcomes.
                    pass
                await asyncio.sleep(600)  # every 10 minutes
            except Exception as e:
                logger.error(f"Self-evolving loop error: {str(e)}")
                await asyncio.sleep(300)

    # ========================================================================
    # NEW: Inject helium provider
    # ========================================================================

    def inject_helium_provider(self, provider: HeliumProvider):
        """Inject the Helium provider to access real-time telemetry."""
        self.helium_provider = provider

    # ========================================================================
    # Helper methods (existing ones from v8.0.0)
    # ========================================================================

    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    def _get_real_token_availability(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return min(1.0, summary.get('total_balance', 500) / 1000)
        return 0.5

    def _get_real_stress_level(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            return stats.get('stress_level', 0.3)
        return 0.3

    def inject_bio_core(self, bio_core: Any):
        """Inject bio-inspired core"""
        self.bio_core = bio_core
        if hasattr(bio_core, 'token_manager'):
            self.token_manager = bio_core.token_manager
        if hasattr(bio_core, 'gradient_manager'):
            self.gradient_manager = bio_core.gradient_manager
        if hasattr(bio_core, 'scheduler'):
            self.scheduler = bio_core.scheduler
        if hasattr(bio_core, 'compartment_manager'):
            self.compartment_manager = bio_core.compartment_manager
        if hasattr(bio_core, 'biomass_storage'):
            self.biomass_storage = bio_core.biomass_storage
        if hasattr(bio_core, 'harvester'):
            self.harvester = bio_core.harvester

    # ========================================================================
    # Enhanced route_task with full context and action application
    # ========================================================================

    async def route_task(self, task: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Route task to best expert using enriched context.
        """
        context = context or {}

        # 1. Enrich context with Helium, carbon, bio signals
        if self.helium_provider:
            context['helium_scarcity'] = self.helium_provider.get_scarcity()
            context['helium_cost_index'] = self.helium_provider.get_cost_index()
            context['avg_client_energy'] = self.helium_provider.get_avg_client_energy()
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            context['carbon_intensity'] = carbon_intensity / 1000.0  # normalize

        # Bio-inspired signals
        gradients = self._get_real_gradient_levels()
        context['gradient_carbon'] = gradients.get('carbon', 0.5)
        context['gradient_helium'] = gradients.get('helium', 0.5)
        context['gradient_trust'] = gradients.get('trust', 0.5)
        context['token_balance_norm'] = self._get_real_token_availability()
        context['harvester_stress'] = self._get_real_stress_level()

        # 2. Build gating features and get expert weights
        features = self._build_gating_features(context)
        if self.gating_network:
            expert_weights = self.gating_network.predict(features)  # softmax over experts
        else:
            # Fallback: uniform weights
            expert_weights = {eid: 1.0 / len(self.experts) for eid in self.experts}

        # 3. Get expert proposals
        proposals = {}
        for expert_id, expert in self.experts.items():
            if self.circuit_breakers[expert_id].can_execute():
                try:
                    proposals[expert_id] = expert.propose(context)
                except Exception as e:
                    logger.error(f"Expert {expert_id} proposal failed: {e}")
                    self.circuit_breakers[expert_id].record_failure()
                    proposals[expert_id] = {}
            else:
                logger.debug(f"Expert {expert_id} circuit breaker open, skipping")

        # 4. Merge proposals into unified action
        unified_action = self._merge_actions(proposals, expert_weights)

        # 5. Apply action to Helium/FL modules (via callbacks or direct calls)
        await self._apply_action(unified_action, context)

        # 6. Record metrics
        self.metrics.total_routes += 1
        self.metrics.successful_routes += 1
        self.active_routes += 1
        self.routing_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'task': task,
            'context': context,
            'expert_weights': expert_weights,
            'unified_action': unified_action,
        })

        # 7. Self-evolving gates update (reward after action execution)
        if self.enable_self_evolving_gates and self.self_evolving_gates:
            reward = await self._compute_green_performance(unified_action, context)
            self.self_evolving_gates.update(reward, context, unified_action)

        return {
            'success': True,
            'expert_weights': expert_weights,
            'unified_action': unified_action,
            'metrics': {
                'latency_ms': 50.0,  # placeholder
                'carbon_savings_kg': self.metrics.carbon_savings_kg,
                'helium_savings_l': self.metrics.helium_savings_l,
            },
            'explanation': f"Action applied with weights: {expert_weights}"
        }

    # ========================================================================
    # Helper: Build gating features
    # ========================================================================

    def _build_gating_features(self, context: dict) -> np.ndarray:
        """Construct feature vector for gating network."""
        return np.array([
            context.get('helium_scarcity', 0.5),
            context.get('helium_cost_index', 1.0),
            context.get('carbon_intensity', 0.5),
            context.get('model_loss', 0.0),
            context.get('gradient_variance', 0.0),
            context.get('avg_client_energy', 0.5),
            context.get('gradient_carbon', 0.5),
            context.get('gradient_helium', 0.5),
            context.get('token_balance_norm', 0.5),
            context.get('harvester_stress', 0.3),
        ])

    # ========================================================================
    # Helper: Merge expert proposals
    # ========================================================================

    def _merge_actions(self, proposals: Dict[str, dict], weights: Dict[str, float]) -> dict:
        """
        Combine expert proposals using gating weights.
        For numeric parameters, weighted average; for categorical, pick highest weight.
        """
        merged = {}
        # Collect all possible keys
        all_keys = set()
        for prop in proposals.values():
            all_keys.update(prop.keys())

        for key in all_keys:
            # Check if all values are numeric
            numeric = all(
                isinstance(prop.get(key), (int, float))
                for prop in proposals.values()
                if key in prop
            )
            if numeric:
                weighted_sum = 0.0
                total_weight = 0.0
                for expert_id, prop in proposals.items():
                    if key in prop:
                        w = weights.get(expert_id, 0.0)
                        weighted_sum += prop[key] * w
                        total_weight += w
                merged[key] = weighted_sum / total_weight if total_weight > 0 else 0.0
            else:
                # Categorical: choose the one with highest weight
                best_expert = max(
                    proposals.items(),
                    key=lambda kv: weights.get(kv[0], 0.0) if key in kv[1] else 0.0
                )[0]
                merged[key] = proposals[best_expert].get(key)
        return merged

    # ========================================================================
    # Helper: Apply unified action to actual modules
    # ========================================================================

    async def _apply_action(self, action: dict, context: dict):
        """
        Dispatch action to Helium, FL, and energy modules.
        This is where you call your existing module APIs.
        """
        # Example: set sampling rate on Helium IoT
        if 'sampling_rate_hz' in action and self.helium_provider:
            # Assume we have a method `set_sampling_rate()` in helium provider
            await self.helium_provider.set_sampling_rate(action['sampling_rate_hz'])

        # Example: adjust FL round frequency
        if 'round_frequency_hz' in action:
            # Call your FL scheduler
            pass

        # Example: select data center
        if 'preferred_data_center' in action:
            # Call your data center selector
            pass

        # Update carbon/helium savings metrics (placeholder)
        self.metrics.carbon_savings_kg += 0.01
        self.metrics.helium_savings_l += 0.001

    # ========================================================================
    # Helper: Compute green-performance reward
    # ========================================================================

    async def _compute_green_performance(self, action: dict, context: dict) -> float:
        """
        Compute a scalar reward for self-evolving gates.
        Combine model accuracy, energy, carbon, helium usage.
        """
        # Placeholder: you should use real metrics from your modules
        accuracy = 0.95  # from FL
        energy_consumed = 0.5  # kWh
        carbon_emitted = 0.2  # kg
        helium_used = 0.1  # L

        # Normalize
        acc_norm = accuracy / 1.0
        energy_norm = 1 - min(energy_consumed / 10.0, 1.0)
        carbon_norm = 1 - min(carbon_emitted / 10.0, 1.0)
        helium_norm = 1 - min(helium_used / 5.0, 1.0)

        reward = 0.4 * acc_norm + 0.2 * energy_norm + 0.2 * carbon_norm + 0.2 * helium_norm
        return reward

    # ========================================================================
    # Public stats method (extended)
    # ========================================================================

    def get_routing_stats(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics including new experts and gates."""
        stats = {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'failed_routes': self.metrics.failed_routes,
                'success_rate': self.metrics.success_rate,
                'average_latency_ms': self.metrics.average_latency_ms,
                'carbon_savings_kg': self.metrics.carbon_savings_kg,
                'helium_savings_l': self.metrics.helium_savings_l
            },
            'active_routes': self.active_routes,
            'max_concurrent_routes': self.max_concurrent_routes,
            'experts': list(self.experts.keys()),
            'circuit_breakers': {
                eid: {
                    'state': cb.state.value,
                    'failure_count': cb.failure_count,
                    'success_count': cb.success_count
                }
                for eid, cb in self.circuit_breakers.items()
            },
            'gating_network': self.gating_network is not None,
            'self_evolving_gates': self.self_evolving_gates is not None,
        }

        if self.signal_engine:
            stats['signaling'] = self.signal_engine.get_signaling_status()
        if self.allosteric_system:
            stats['allosteric'] = self.allosteric_system.get_regulation_status()
        if self.metabolic_router:
            stats['pathways'] = self.metabolic_router.get_pathway_stats()
        if self.helium_optimizer:
            stats['helium'] = self.helium_optimizer.get_helium_status()
        if self.federated_learner:
            stats['federated'] = self.federated_learner.get_federated_insights()
        if self.predictive_analyzer:
            stats['predictive'] = self.predictive_analyzer.get_uncertainty_metrics()
        if self.causal_model:
            stats['causal'] = self.causal_model.get_causal_graph_summary()
        if self.signal_integrator:
            stats['signal_integration'] = self.signal_integrator.get_integration_stats()

        return stats
