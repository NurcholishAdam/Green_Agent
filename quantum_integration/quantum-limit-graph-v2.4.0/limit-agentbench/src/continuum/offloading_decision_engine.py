# src/continuum/offloading_decision_engine.py

from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
from datetime import datetime
import numpy as np

class PlacementTier(Enum):
    TIER_1_LOCAL = "tier_1_local"      # Edge device itself
    TIER_2_REGIONAL = "tier_2_regional"  # Regional edge node
    TIER_3_CLOUD = "tier_3_cloud"      # Cloud Green zone

@dataclass
class OffloadingCriteria:
    """Criteria for offloading decisions"""
    tdp_threshold_watts: float
    tdp_warning_buffer: float = 2.0
    carbon_green_threshold: float = 50.0    # gCO2/kWh
    carbon_yellow_threshold: float = 200.0  # gCO2/kWh
    carbon_red_threshold: float = 400.0     # gCO2/kWh
    latency_sla_ms: float = 100.0
    latency_buffer_ms: float = 20.0
    cost_weight: float = 0.3
    carbon_weight: float = 0.5
    latency_weight: float = 0.2

@dataclass
class PlacementDecision:
    """Result of offloading decision"""
    task_id: str
    device_id: str
    selected_tier: PlacementTier
    target_node: str
    confidence_score: float  # 0.0 - 1.0
    reasoning: List[str]
    estimated_latency_ms: float
    estimated_carbon_gco2: float
    estimated_cost_usd: float
    timestamp: datetime

class OffloadingDecisionEngine:
    """
    Multi-objective optimizer for edge-cloud placement
    
    Responsibilities:
    - Evaluate offloading criteria (TDP, carbon, latency, cost)
    - Calculate optimal placement score
    - Select target node from available pool
    - Provide reasoning for audit trail
    """
    
    def __init__(
        self,
        criteria: OffloadingCriteria,
        carbon_intensity_monitor: 'CarbonIntensityMonitor',
        latency_monitor: 'LatencySLAMonitor',
        node_registry: 'NodeRegistry'
    ):
        self.criteria = criteria
        self.carbon_monitor = carbon_intensity_monitor
        self.latency_monitor = latency_monitor
        self.node_registry = node_registry
        
        self._decision_cache: Dict[str, PlacementDecision] = {}
        self._cache_ttl_seconds = 60
        
    async def decide_placement(
        self,
        task_id: str,
        device_id: str,
        task_requirements: Dict
    ) -> PlacementDecision:
        """
        Make offloading decision for a task
        
        Args:
            task_id: Unique task identifier
            device_id: Source edge device
            task_requirements: {latency_sla_ms, compute_intensity, data_size_mb}
            
        Returns:
            PlacementDecision with selected tier and target node
        """
        # Check cache first
        cache_key = f"{device_id}_{task_id}"
        if cache_key in self._decision_cache:
            cached = self._decision_cache[cache_key]
            if (datetime.now() - cached.timestamp).total_seconds() < self._cache_ttl_seconds:
                return cached
                
        # Gather inputs
        tdp_reading = await self._get_tdp_reading(device_id)
        carbon_intensity = await self.carbon_monitor.get_current_intensity()
        latency_metrics = await self.latency_monitor.get_latency_to_tiers(device_id)
        available_nodes = await self.node_registry.get_available_nodes()
        
        # Evaluate placement scores for each tier
        tier_scores = {}
        for tier in PlacementTier:
            score, reasoning = await self._calculate_tier_score(
                tier=tier,
                device_id=device_id,
                tdp_reading=tdp_reading,
                carbon_intensity=carbon_intensity,
                latency_metrics=latency_metrics,
                available_nodes=available_nodes,
                task_requirements=task_requirements
            )
            tier_scores[tier] = (score, reasoning)
            
        # Select best tier
        best_tier = max(tier_scores.keys(), key=lambda t: tier_scores[t][0])
        best_score, best_reasoning = tier_scores[best_tier]
        
        # Select target node within tier
        target_node = await self._select_target_node(
            tier=best_tier,
            device_id=device_id,
            available_nodes=available_nodes,
            task_requirements=task_requirements
        )
        
        # Estimate metrics
        estimated_latency = latency_metrics.get(best_tier, 100.0)
        estimated_carbon = await self._estimate_carbon(
            tier=best_tier,
            task_requirements=task_requirements,
            carbon_intensity=carbon_intensity
        )
        estimated_cost = await self._estimate_cost(
            tier=best_tier,
            task_requirements=task_requirements
        )
        
        decision = PlacementDecision(
            task_id=task_id,
            device_id=device_id,
            selected_tier=best_tier,
            target_node=target_node,
            confidence_score=best_score,
            reasoning=best_reasoning,
            estimated_latency_ms=estimated_latency,
            estimated_carbon_gco2=estimated_carbon,
            estimated_cost_usd=estimated_cost,
            timestamp=datetime.now()
        )
        
        # Cache decision
        self._decision_cache[cache_key] = decision
        
        return decision
        
    async def _calculate_tier_score(
        self,
        tier: PlacementTier,
        device_id: str,
        tdp_reading: TDPReading,
        carbon_intensity: float,
        latency_metrics: Dict[PlacementTier, float],
        available_nodes: List[Dict],
        task_requirements: Dict
    ) -> Tuple[float, List[str]]:
        """Calculate placement score for a tier with reasoning"""
        reasoning = []
        score = 0.0
        
        # TDP score (higher = better)
        if tier == PlacementTier.TIER_1_LOCAL:
            if tdp_reading.current_power_watts > \
               tdp_reading.tdp_threshold_watts - self.criteria.tdp_warning_buffer:
                reasoning.append(f"TDP near threshold ({tdp_reading.current_power_watts:.1f}W / {tdp_reading.tdp_threshold_watts:.1f}W)")
                tdp_score = 0.0  # Penalize heavily
            else:
                reasoning.append(f"TDP within safe limits ({tdp_reading.current_power_watts:.1f}W)")
                tdp_score = 1.0
        else:
            tdp_score = 1.0  # Offloading resolves TDP concern
            reasoning.append("TDP concern resolved via offloading")
            
        # Carbon score (lower intensity = better)
        if tier == PlacementTier.TIER_3_CLOUD:
            # Cloud can select Green zone
            carbon_score = 1.0 if carbon_intensity < self.criteria.carbon_green_threshold else \
                          0.5 if carbon_intensity < self.criteria.carbon_yellow_threshold else 0.2
            reasoning.append(f"Cloud placement: carbon intensity {carbon_intensity:.1f} gCO2/kWh")
        elif tier == PlacementTier.TIER_2_REGIONAL:
            carbon_score = 0.7  # Regional edge varies
            reasoning.append("Regional edge: moderate carbon intensity")
        else:  # Local
            carbon_score = 0.5  # Local depends on grid
            reasoning.append(f"Local execution: carbon intensity {carbon_intensity:.1f} gCO2/kWh")
            
        # Latency score (lower = better)
        latency = latency_metrics.get(tier, 100.0)
        latency_sla = task_requirements.get('latency_sla_ms', self.criteria.latency_sla_ms)
        
        if latency < latency_sla - self.criteria.latency_buffer_ms:
            latency_score = 1.0
            reasoning.append(f"Latency well within SLA ({latency:.1f}ms < {latency_sla:.1f}ms)")
        elif latency < latency_sla:
            latency_score = 0.7
            reasoning.append(f"Latency within SLA ({latency:.1f}ms < {latency_sla:.1f}ms)")
        else:
            latency_score = 0.3
            reasoning.append(f"Latency exceeds SLA ({latency:.1f}ms > {latency_sla:.1f}ms)")
            
        # Cost score (edge = cheaper, cloud = more expensive)
        if tier == PlacementTier.TIER_1_LOCAL:
            cost_score = 1.0
            reasoning.append("Lowest cost (local execution)")
        elif tier == PlacementTier.TIER_2_REGIONAL:
            cost_score = 0.7
            reasoning.append("Moderate cost (regional edge)")
        else:
            cost_score = 0.4
            reasoning.append("Higher cost (cloud compute + egress)")
            
        # Weighted sum
        score = (
            tdp_score * 0.3 +  # TDP weight
            carbon_score * self.criteria.carbon_weight +
            latency_score * self.criteria.latency_weight +
            cost_score * self.criteria.cost_weight
        )
        
        return score, reasoning
        
    async def _select_target_node(
        self,
        tier: PlacementTier,
        device_id: str,
        available_nodes: List[Dict],
        task_requirements: Dict
    ) -> str:
        """Select specific target node within tier"""
        # Filter nodes by tier
        tier_nodes = [n for n in available_nodes if n['tier'] == tier.value]
        
        if not tier_nodes:
            # Fallback to next best tier
            fallback_tiers = {
                PlacementTier.TIER_1_LOCAL: PlacementTier.TIER_2_REGIONAL,
                PlacementTier.TIER_2_REGIONAL: PlacementTier.TIER_3_CLOUD,
                PlacementTier.TIER_3_CLOUD: PlacementTier.TIER_2_REGIONAL,
            }
            return await self._select_target_node(
                tier=fallback_tiers[tier],
                device_id=device_id,
                available_nodes=available_nodes,
                task_requirements=task_requirements
            )
            
        # Score nodes within tier
        scored_nodes = []
        for node in tier_nodes:
            node_score = self._score_node(node, task_requirements)
            scored_nodes.append((node_score, node))
            
        # Select highest scored node
        scored_nodes.sort(key=lambda x: x[0], reverse=True)
        return scored_nodes[0][1]['node_id']
        
    def _score_node(self, node: Dict, task_requirements: Dict) -> float:
        """Score individual node within tier"""
        score = 0.0
        
        # Available resources
        if node['available_cpu_percent'] > task_requirements.get('cpu_required', 50):
            score += 0.3
        if node['available_memory_mb'] > task_requirements.get('memory_required_mb', 1024):
            score += 0.3
            
        # Current load
        if node['current_tasks'] < node['max_tasks']:
            score += 0.2
            
        # Network quality
        if node['network_latency_ms'] < 10:
            score += 0.2
            
        return score
        
    async def _estimate_carbon(
        self,
        tier: PlacementTier,
        task_requirements: Dict,
        carbon_intensity: float
    ) -> float:
        """Estimate carbon emissions for placement"""
        # Simplified model: energy_kwh * carbon_intensity
        compute_intensity = task_requirements.get('compute_intensity', 1.0)
        
        energy_per_tier = {
            PlacementTier.TIER_1_LOCAL: 0.001,  # kWh per task
            PlacementTier.TIER_2_REGIONAL: 0.0015,
            PlacementTier.TIER_3_CLOUD: 0.002,
        }
        
        energy = energy_per_tier[tier] * compute_intensity
        carbon = energy * carbon_intensity / 1000  # Convert to kg CO2
        
        return carbon * 1000  # Return as grams
        
    async def _estimate_cost(
        self,
        tier: PlacementTier,
        task_requirements: Dict
    ) -> float:
        """Estimate cost for placement"""
        data_size_mb = task_requirements.get('data_size_mb', 1.0)
        compute_seconds = task_requirements.get('compute_seconds', 1.0)
        
        cost_per_tier = {
            PlacementTier.TIER_1_LOCAL: 0.0001,  # USD per task
            PlacementTier.TIER_2_REGIONAL: 0.0005,
            PlacementTier.TIER_3_CLOUD: 0.002,  # Includes egress
        }
        
        base_cost = cost_per_tier[tier]
        egress_cost = data_size_mb * 0.0001 if tier == PlacementTier.TIER_3_CLOUD else 0
        
        return base_cost * compute_seconds + egress_cost
        
    async def _get_tdp_reading(self, device_id: str) -> TDPReading:
        """Get TDP reading for device"""
        # Implementation: Query TDPMonitor for device
        pass

      async def _get_tdp_reading(self, device_id: str) -> TDPReading:
        """Get TDP reading for device from TDPMonitor"""
        # In production: Query the TDPMonitor instance for this device
        # For now, return a placeholder
        return TDPReading(
            timestamp=datetime.now(),
            device_id=device_id,
            device_type=DeviceType.UNKNOWN,
            current_power_watts=15.0,
            tdp_threshold_watts=28.0,
            utilization_percent=45.0,
            temperature_celsius=52.0,
            thermal_throttling=False,
            predicted_breach_seconds=None
        )

    async def _get_available_nodes(self) -> List[Dict]:
        """Get list of available nodes from NodeRegistry"""
        # Implementation: Query Kubernetes API or service discovery
        return [
            {
                'node_id': 'edge-pi-001',
                'tier': PlacementTier.TIER_1_LOCAL.value,
                'available_cpu_percent': 65,
                'available_memory_mb': 2048,
                'current_tasks': 3,
                'max_tasks': 10,
                'network_latency_ms': 2,
                'carbon_intensity': 45.0,
            },
            {
                'node_id': 'regional-edge-us-west-001',
                'tier': PlacementTier.TIER_2_REGIONAL.value,
                'available_cpu_percent': 80,
                'available_memory_mb': 8192,
                'current_tasks': 12,
                'max_tasks': 50,
                'network_latency_ms': 8,
                'carbon_intensity': 120.0,
            },
            {
                'node_id': 'cloud-gke-us-central1-001',
                'tier': PlacementTier.TIER_3_CLOUD.value,
                'available_cpu_percent': 90,
                'available_memory_mb': 32768,
                'current_tasks': 45,
                'max_tasks': 200,
                'network_latency_ms': 35,
                'carbon_intensity': 30.0,  # Green zone
            },
        ]

    def _score_node(self, node: Dict, task_requirements: Dict) -> float:
        """Score individual node within tier"""
        score = 0.0
        
        # Available resources (40% weight)
        if node['available_cpu_percent'] > task_requirements.get('cpu_required', 50):
            score += 0.2
        if node['available_memory_mb'] > task_requirements.get('memory_required_mb', 1024):
            score += 0.2
            
        # Current load (30% weight)
        load_ratio = node['current_tasks'] / node['max_tasks']
        if load_ratio < 0.5:
            score += 0.3
        elif load_ratio < 0.75:
            score += 0.15
            
        # Network quality (30% weight)
        if node['network_latency_ms'] < 10:
            score += 0.3
        elif node['network_latency_ms'] < 25:
            score += 0.15
            
        return score

    async def _estimate_carbon(
        self,
        tier: PlacementTier,
        task_requirements: Dict,
        carbon_intensity: float
    ) -> float:
        """Estimate carbon emissions for placement"""
        # Simplified model: energy_kwh * carbon_intensity
        compute_intensity = task_requirements.get('compute_intensity', 1.0)
        
        energy_per_tier = {
            PlacementTier.TIER_1_LOCAL: 0.001,    # kWh per task
            PlacementTier.TIER_2_REGIONAL: 0.0015,
            PlacementTier.TIER_3_CLOUD: 0.002,
        }
        
        energy = energy_per_tier[tier] * compute_intensity
        carbon = energy * carbon_intensity / 1000  # Convert to kg CO2
        
        return carbon * 1000  # Return as grams

    async def _estimate_cost(
        self,
        tier: PlacementTier,
        task_requirements: Dict
    ) -> float:
        """Estimate cost for placement"""
        data_size_mb = task_requirements.get('data_size_mb', 1.0)
        compute_seconds = task_requirements.get('compute_seconds', 1.0)
        
        cost_per_tier = {
            PlacementTier.TIER_1_LOCAL: 0.0001,   # USD per task
            PlacementTier.TIER_2_REGIONAL: 0.0005,
            PlacementTier.TIER_3_CLOUD: 0.002,    # Includes egress
        }
        
        base_cost = cost_per_tier[tier]
        egress_cost = data_size_mb * 0.0001 if tier == PlacementTier.TIER_3_CLOUD else 0
        
        return base_cost * compute_seconds + egress_cost
