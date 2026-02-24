# -*- coding: utf-8 -*-
"""
Multimodal Fusion Module

Fuses text and visual retrieval results with energy-aware weighting.
Inspired by VimRAG's multimodal memory graph navigation.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import numpy as np


class ModalityType(Enum):
    """Types of modalities."""
    TEXT = "text"
    VISUAL = "visual"
    AUDIO = "audio"
    MULTIMODAL = "multimodal"


@dataclass
class ModalityScore:
    """Score for a specific modality."""
    modality: ModalityType
    relevance_score: float
    confidence: float
    energy_cost: float
    token_cost: int


@dataclass
class FusedResult:
    """Result from multimodal fusion."""
    content_id: str
    modalities: List[ModalityType]
    fused_score: float
    modality_scores: Dict[ModalityType, ModalityScore]
    total_energy: float
    total_tokens: int
    fusion_method: str


class MultimodalFusion:
    """
    Multimodal fusion engine for combining retrieval results.
    
    Features:
    - Energy-aware modality weighting
    - Adaptive fusion strategies
    - Carbon-conscious modality selection
    - Token-efficient multimodal representation
    """
    
    def __init__(
        self,
        text_weight: float = 0.6,
        visual_weight: float = 0.4,
        energy_penalty_factor: float = 0.1
    ):
        """
        Initialize multimodal fusion.
        
        Args:
            text_weight: Weight for text modality
            visual_weight: Weight for visual modality
            energy_penalty_factor: Penalty factor for high energy cost
        """
        self.modality_weights = {
            ModalityType.TEXT: text_weight,
            ModalityType.VISUAL: visual_weight,
            ModalityType.AUDIO: 0.3,
            ModalityType.MULTIMODAL: 1.0
        }
        
        self.energy_penalty = energy_penalty_factor
        
        # Fusion statistics
        self.fusion_history = []
        self.modality_usage = {m: 0 for m in ModalityType}
        
    def fuse_multimodal_results(
        self,
        results: List[Dict[str, Any]],
        fusion_method: str = "weighted_sum",
        energy_budget: Optional[float] = None
    ) -> List[FusedResult]:
        """
        Fuse multimodal retrieval results.
        
        Args:
            results: List of retrieval results with modality info
            fusion_method: Fusion strategy ("weighted_sum", "max", "adaptive")
            energy_budget: Optional energy budget constraint
            
        Returns:
            List of fused results
        """
        fused_results = []
        
        for result in results:
            # Extract modality scores
            modality_scores = self._extract_modality_scores(result)
            
            # Apply fusion method
            if fusion_method == "weighted_sum":
                fused_score = self._weighted_sum_fusion(modality_scores)
            elif fusion_method == "max":
                fused_score = self._max_fusion(modality_scores)
            elif fusion_method == "adaptive":
                fused_score = self._adaptive_fusion(modality_scores, energy_budget)
            else:
                fused_score = self._weighted_sum_fusion(modality_scores)
            
            # Calculate total costs
            total_energy = sum(s.energy_cost for s in modality_scores.values())
            total_tokens = sum(s.token_cost for s in modality_scores.values())
            
            # Check energy budget
            if energy_budget and total_energy > energy_budget:
                # Skip or reduce modalities
                modality_scores = self._reduce_modalities(
                    modality_scores,
                    energy_budget
                )
                total_energy = sum(s.energy_cost for s in modality_scores.values())
                total_tokens = sum(s.token_cost for s in modality_scores.values())
            
            # Create fused result
            fused = FusedResult(
                content_id=result.get("content_id", "unknown"),
                modalities=list(modality_scores.keys()),
                fused_score=fused_score,
                modality_scores=modality_scores,
                total_energy=total_energy,
                total_tokens=total_tokens,
                fusion_method=fusion_method
            )
            
            fused_results.append(fused)
            
            # Update statistics
            for modality in modality_scores.keys():
                self.modality_usage[modality] += 1
            
            self.fusion_history.append({
                "method": fusion_method,
                "score": fused_score,
                "energy": total_energy,
                "tokens": total_tokens
            })
        
        # Sort by fused score
        fused_results.sort(key=lambda x: x.fused_score, reverse=True)
        
        return fused_results
    
    def adaptive_modality_selection(
        self,
        query: str,
        available_modalities: List[ModalityType],
        energy_budget: float,
        token_budget: int
    ) -> List[ModalityType]:
        """
        Adaptively select modalities based on query and budgets.
        
        Args:
            query: Query string
            available_modalities: Available modality types
            energy_budget: Energy budget
            token_budget: Token budget
            
        Returns:
            Selected modalities
        """
        selected = []
        remaining_energy = energy_budget
        remaining_tokens = token_budget
        
        # Estimate costs per modality
        modality_costs = self._estimate_modality_costs(available_modalities)
        
        # Score modalities for query
        modality_relevance = self._score_modality_relevance(
            query,
            available_modalities
        )
        
        # Select modalities greedily by relevance/cost ratio
        candidates = [
            (m, modality_relevance[m] / max(modality_costs[m]["energy"], 0.0001))
            for m in available_modalities
        ]
        candidates.sort(key=lambda x: x[1], reverse=True)
        
        for modality, _ in candidates:
            costs = modality_costs[modality]
            
            if (costs["energy"] <= remaining_energy and
                costs["tokens"] <= remaining_tokens):
                selected.append(modality)
                remaining_energy -= costs["energy"]
                remaining_tokens -= costs["tokens"]
        
        return selected
    
    def get_fusion_statistics(self) -> Dict[str, Any]:
        """Get statistics about multimodal fusion."""
        if not self.fusion_history:
            return {"status": "no_fusions"}
        
        avg_score = np.mean([f["score"] for f in self.fusion_history])
        avg_energy = np.mean([f["energy"] for f in self.fusion_history])
        avg_tokens = np.mean([f["tokens"] for f in self.fusion_history])
        
        return {
            "total_fusions": len(self.fusion_history),
            "avg_fused_score": avg_score,
            "avg_energy_per_fusion": avg_energy,
            "avg_tokens_per_fusion": avg_tokens,
            "modality_usage": {
                m.value: count for m, count in self.modality_usage.items()
            },
            "most_used_modality": max(
                self.modality_usage.items(),
                key=lambda x: x[1]
            )[0].value
        }
    
    def optimize_fusion_weights(self) -> Dict[ModalityType, float]:
        """
        Optimize modality weights based on fusion history.
        
        Returns:
            Optimized weights
        """
        # Simplified optimization: increase weights for frequently used modalities
        total_usage = sum(self.modality_usage.values())
        
        if total_usage == 0:
            return self.modality_weights
        
        optimized = {}
        for modality, count in self.modality_usage.items():
            usage_ratio = count / total_usage
            current_weight = self.modality_weights[modality]
            
            # Adjust weight based on usage
            optimized[modality] = current_weight * (1 + usage_ratio * 0.2)
        
        # Normalize weights
        total_weight = sum(optimized.values())
        optimized = {m: w / total_weight for m, w in optimized.items()}
        
        return optimized
    
    def _extract_modality_scores(
        self,
        result: Dict[str, Any]
    ) -> Dict[ModalityType, ModalityScore]:
        """Extract modality scores from result."""
        scores = {}
        
        for modality in ModalityType:
            key = modality.value
            if key in result:
                scores[modality] = ModalityScore(
                    modality=modality,
                    relevance_score=result[key].get("score", 0.0),
                    confidence=result[key].get("confidence", 1.0),
                    energy_cost=result[key].get("energy", 0.001),
                    token_cost=result[key].get("tokens", 100)
                )
        
        return scores
    
    def _weighted_sum_fusion(
        self,
        modality_scores: Dict[ModalityType, ModalityScore]
    ) -> float:
        """Weighted sum fusion."""
        total_score = 0.0
        total_weight = 0.0
        
        for modality, score in modality_scores.items():
            weight = self.modality_weights.get(modality, 0.5)
            
            # Apply energy penalty
            energy_penalty = 1.0 - (score.energy_cost * self.energy_penalty)
            energy_penalty = max(0.1, energy_penalty)
            
            total_score += score.relevance_score * weight * energy_penalty
            total_weight += weight
        
        return total_score / max(total_weight, 0.001)
    
    def _max_fusion(
        self,
        modality_scores: Dict[ModalityType, ModalityScore]
    ) -> float:
        """Max fusion - take highest score."""
        if not modality_scores:
            return 0.0
        return max(s.relevance_score for s in modality_scores.values())
    
    def _adaptive_fusion(
        self,
        modality_scores: Dict[ModalityType, ModalityScore],
        energy_budget: Optional[float]
    ) -> float:
        """Adaptive fusion based on confidence and energy."""
        if not modality_scores:
            return 0.0
        
        # Weight by confidence and inverse energy cost
        weighted_scores = []
        
        for modality, score in modality_scores.items():
            confidence_weight = score.confidence
            energy_weight = 1.0 / max(score.energy_cost, 0.0001)
            
            if energy_budget:
                # Penalize if over budget
                if score.energy_cost > energy_budget:
                    energy_weight *= 0.5
            
            weighted = score.relevance_score * confidence_weight * energy_weight
            weighted_scores.append(weighted)
        
        return np.mean(weighted_scores)
    
    def _reduce_modalities(
        self,
        modality_scores: Dict[ModalityType, ModalityScore],
        energy_budget: float
    ) -> Dict[ModalityType, ModalityScore]:
        """Reduce modalities to fit energy budget."""
        # Sort by relevance/energy ratio
        sorted_modalities = sorted(
            modality_scores.items(),
            key=lambda x: x[1].relevance_score / max(x[1].energy_cost, 0.0001),
            reverse=True
        )
        
        reduced = {}
        total_energy = 0.0
        
        for modality, score in sorted_modalities:
            if total_energy + score.energy_cost <= energy_budget:
                reduced[modality] = score
                total_energy += score.energy_cost
        
        return reduced
    
    def _estimate_modality_costs(
        self,
        modalities: List[ModalityType]
    ) -> Dict[ModalityType, Dict[str, float]]:
        """Estimate costs for modalities."""
        costs = {
            ModalityType.TEXT: {"energy": 0.001, "tokens": 500},
            ModalityType.VISUAL: {"energy": 0.0025, "tokens": 256},
            ModalityType.AUDIO: {"energy": 0.002, "tokens": 300},
            ModalityType.MULTIMODAL: {"energy": 0.004, "tokens": 800}
        }
        
        return {m: costs[m] for m in modalities if m in costs}
    
    def _score_modality_relevance(
        self,
        query: str,
        modalities: List[ModalityType]
    ) -> Dict[ModalityType, float]:
        """Score modality relevance for query."""
        # Simplified: check for modality keywords in query
        scores = {}
        query_lower = query.lower()
        
        for modality in modalities:
            if modality == ModalityType.TEXT:
                scores[modality] = 0.8  # Default high for text
            elif modality == ModalityType.VISUAL:
                if any(kw in query_lower for kw in ["image", "visual", "picture", "diagram"]):
                    scores[modality] = 0.9
                else:
                    scores[modality] = 0.3
            elif modality == ModalityType.AUDIO:
                if any(kw in query_lower for kw in ["audio", "sound", "speech"]):
                    scores[modality] = 0.9
                else:
                    scores[modality] = 0.2
            else:
                scores[modality] = 0.5
        
        return scores
