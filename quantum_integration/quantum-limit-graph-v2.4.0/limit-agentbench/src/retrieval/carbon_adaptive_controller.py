# -*- coding: utf-8 -*-
"""
Carbon-Adaptive Retrieval Controller

Works with the existing carbon throttler to decide whether to allow full
multimodal retrieval or restrict to lightweight sources based on sustainability.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import time


class RetrievalMode(Enum):
    """Retrieval mode based on carbon conditions."""
    FULL = "full"  # Full multimodal retrieval allowed
    RESTRICTED = "restricted"  # Text-only, limited tokens
    MINIMAL = "minimal"  # Essential retrievals only
    DEFERRED = "deferred"  # Defer non-urgent retrievals


@dataclass
class RetrievalDecision:
    """Decision about retrieval operation."""
    allowed: bool
    mode: RetrievalMode
    max_tokens: int
    allow_visual: bool
    allow_multimodal: bool
    reason: str
    carbon_intensity_g_kwh: float
    estimated_carbon_kg: float


class CarbonAdaptiveRetrievalController:
    """
    Carbon-adaptive controller for retrieval operations.
    
    Responsibilities:
    - Work with existing carbon throttler
    - Decide retrieval mode based on sustainability conditions
    - Allow full multimodal retrieval during clean grid periods
    - Restrict to lightweight sources during dirty grid periods
    - Defer non-urgent retrievals when appropriate
    - Integrate with meta-cognitive reflection
    """
    
    def __init__(
        self,
        carbon_throttler: Optional[Any] = None,
        clean_grid_threshold_g_kwh: float = 200.0,
        dirty_grid_threshold_g_kwh: float = 400.0,
        enable_deferral: bool = True
    ):
        """
        Initialize carbon-adaptive retrieval controller.
        
        Args:
            carbon_throttler: Existing carbon throttler instance
            clean_grid_threshold_g_kwh: Threshold for clean grid
            dirty_grid_threshold_g_kwh: Threshold for dirty grid
            enable_deferral: Enable deferral of non-urgent retrievals
        """
        self.carbon_throttler = carbon_throttler
        self.clean_threshold = clean_grid_threshold_g_kwh
        self.dirty_threshold = dirty_grid_threshold_g_kwh
        self.enable_deferral = enable_deferral
        
        # Current grid state
        self.current_intensity_g_kwh = 385.0  # Default moderate
        self.current_mode = RetrievalMode.RESTRICTED
        
        # Deferred retrievals queue
        self.deferred_retrievals: List[Dict[str, Any]] = []
        
        # Statistics
        self.total_decisions = 0
        self.decisions_by_mode = {mode: 0 for mode in RetrievalMode}
        self.carbon_saved_kg = 0.0
        self.retrievals_deferred = 0
        
    def update_carbon_intensity(self, intensity_g_kwh: float):
        """
        Update current carbon grid intensity.
        
        Args:
            intensity_g_kwh: Current carbon intensity
        """
        old_intensity = self.current_intensity_g_kwh
        self.current_intensity_g_kwh = intensity_g_kwh
        
        # Update mode based on new intensity
        old_mode = self.current_mode
        self.current_mode = self._determine_mode(intensity_g_kwh)
        
        # Check if we can process deferred retrievals
        if old_mode != RetrievalMode.FULL and self.current_mode == RetrievalMode.FULL:
            self._process_deferred_retrievals()
    
    def _determine_mode(self, intensity_g_kwh: float) -> RetrievalMode:
        """Determine retrieval mode based on carbon intensity."""
        if intensity_g_kwh < self.clean_threshold:
            return RetrievalMode.FULL
        elif intensity_g_kwh < self.dirty_threshold:
            return RetrievalMode.RESTRICTED
        else:
            return RetrievalMode.MINIMAL
    
    def decide_retrieval(
        self,
        query: str,
        estimated_tokens: int,
        content_type: str = "text",
        urgency: str = "normal",  # "urgent", "normal", "low"
        context: Optional[Dict[str, Any]] = None
    ) -> RetrievalDecision:
        """
        Decide whether and how to perform retrieval.
        
        Args:
            query: Retrieval query
            estimated_tokens: Estimated token count
            content_type: Type of content to retrieve
            urgency: Urgency level of retrieval
            context: Additional context
            
        Returns:
            RetrievalDecision
        """
        self.total_decisions += 1
        
        # Get current mode
        mode = self.current_mode
        
        # Estimate carbon impact
        estimated_carbon = self._estimate_carbon_impact(
            estimated_tokens,
            content_type,
            self.current_intensity_g_kwh
        )
        
        # Check if retrieval should be deferred
        if self.enable_deferral and urgency == "low" and mode == RetrievalMode.MINIMAL:
            self._defer_retrieval(query, estimated_tokens, content_type, context)
            self.retrievals_deferred += 1
            self.decisions_by_mode[RetrievalMode.DEFERRED] += 1
            
            return RetrievalDecision(
                allowed=False,
                mode=RetrievalMode.DEFERRED,
                max_tokens=0,
                allow_visual=False,
                allow_multimodal=False,
                reason="deferred_due_to_dirty_grid_and_low_urgency",
                carbon_intensity_g_kwh=self.current_intensity_g_kwh,
                estimated_carbon_kg=estimated_carbon
            )
        
        # Determine retrieval parameters based on mode
        if mode == RetrievalMode.FULL:
            decision = RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=estimated_tokens,
                allow_visual=True,
                allow_multimodal=True,
                reason="clean_grid_full_retrieval_allowed",
                carbon_intensity_g_kwh=self.current_intensity_g_kwh,
                estimated_carbon_kg=estimated_carbon
            )
        elif mode == RetrievalMode.RESTRICTED:
            # Restrict tokens and content types
            max_tokens = min(estimated_tokens, 4096)
            decision = RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=max_tokens,
                allow_visual=content_type != "visual",
                allow_multimodal=False,
                reason="moderate_grid_restricted_retrieval",
                carbon_intensity_g_kwh=self.current_intensity_g_kwh,
                estimated_carbon_kg=estimated_carbon
            )
            
            # Calculate carbon saved
            if max_tokens < estimated_tokens:
                tokens_saved = estimated_tokens - max_tokens
                self.carbon_saved_kg += tokens_saved * 0.000001 * self.current_intensity_g_kwh / 1000
        else:  # MINIMAL
            # Only allow urgent, text-only retrievals
            if urgency == "urgent":
                max_tokens = min(estimated_tokens, 2048)
                decision = RetrievalDecision(
                    allowed=True,
                    mode=mode,
                    max_tokens=max_tokens,
                    allow_visual=False,
                    allow_multimodal=False,
                    reason="dirty_grid_urgent_minimal_retrieval",
                    carbon_intensity_g_kwh=self.current_intensity_g_kwh,
                    estimated_carbon_kg=estimated_carbon
                )
                
                # Calculate carbon saved
                tokens_saved = estimated_tokens - max_tokens
                self.carbon_saved_kg += tokens_saved * 0.000001 * self.current_intensity_g_kwh / 1000
            else:
                # Deny non-urgent retrievals during dirty grid
                decision = RetrievalDecision(
                    allowed=False,
                    mode=mode,
                    max_tokens=0,
                    allow_visual=False,
                    allow_multimodal=False,
                    reason="dirty_grid_non_urgent_retrieval_denied",
                    carbon_intensity_g_kwh=self.current_intensity_g_kwh,
                    estimated_carbon_kg=estimated_carbon
                )
                
                self.carbon_saved_kg += estimated_carbon
        
        self.decisions_by_mode[decision.mode] += 1
        
        return decision
    
    def _estimate_carbon_impact(
        self,
        tokens: int,
        content_type: str,
        intensity_g_kwh: float
    ) -> float:
        """Estimate carbon impact of retrieval."""
        # Energy estimate
        base_energy_wh = tokens * 0.000001  # 1 microWh per token
        
        # Content type multiplier
        if content_type == "visual":
            base_energy_wh *= 2.5
        elif content_type == "multimodal":
            base_energy_wh *= 2.0
        
        # Carbon calculation
        carbon_kg = (base_energy_wh * intensity_g_kwh) / 1000000.0
        
        return carbon_kg
    
    def _defer_retrieval(
        self,
        query: str,
        tokens: int,
        content_type: str,
        context: Optional[Dict[str, Any]]
    ):
        """Add retrieval to deferred queue."""
        self.deferred_retrievals.append({
            "query": query,
            "tokens": tokens,
            "content_type": content_type,
            "context": context,
            "deferred_at": time.time(),
            "deferred_intensity": self.current_intensity_g_kwh
        })
    
    def _process_deferred_retrievals(self) -> List[Dict[str, Any]]:
        """Process deferred retrievals when grid is clean."""
        if not self.deferred_retrievals:
            return []
        
        processed = []
        for retrieval in self.deferred_retrievals:
            # Re-evaluate retrieval
            decision = self.decide_retrieval(
                query=retrieval["query"],
                estimated_tokens=retrieval["tokens"],
                content_type=retrieval["content_type"],
                urgency="normal",
                context=retrieval["context"]
            )
            
            if decision.allowed:
                processed.append({
                    **retrieval,
                    "processed_at": time.time(),
                    "processed_intensity": self.current_intensity_g_kwh
                })
        
        # Remove processed retrievals
        self.deferred_retrievals = [
            r for r in self.deferred_retrievals
            if r not in processed
        ]
        
        return processed
    
    def get_controller_stats(self) -> Dict[str, Any]:
        """Get controller statistics."""
        return {
            "total_decisions": self.total_decisions,
            "decisions_by_mode": {
                mode.value: count
                for mode, count in self.decisions_by_mode.items()
            },
            "carbon_saved_kg": self.carbon_saved_kg,
            "retrievals_deferred": self.retrievals_deferred,
            "pending_deferred": len(self.deferred_retrievals),
            "current_mode": self.current_mode.value,
            "current_intensity_g_kwh": self.current_intensity_g_kwh
        }
    
    def get_recommendations(self) -> List[str]:
        """Get recommendations for meta-cognitive reflection."""
        recommendations = []
        
        stats = self.get_controller_stats()
        
        if self.current_mode == RetrievalMode.MINIMAL:
            recommendations.append(
                "Operating in MINIMAL mode due to dirty grid. "
                "Only urgent text retrievals allowed."
            )
        elif self.current_mode == RetrievalMode.RESTRICTED:
            recommendations.append(
                "Operating in RESTRICTED mode. "
                "Visual and multimodal content limited."
            )
        else:
            recommendations.append(
                "Operating in FULL mode. Clean grid allows full retrieval."
            )
        
        if stats["pending_deferred"] > 0:
            recommendations.append(
                f"{stats['pending_deferred']} retrievals deferred. "
                f"Will process when grid is cleaner."
            )
        
        if stats["carbon_saved_kg"] > 0.001:
            recommendations.append(
                f"Carbon-adaptive control has saved {stats['carbon_saved_kg']:.6f} kg CO2."
            )
        
        # Suggest optimal retrieval times
        if self.current_intensity_g_kwh > self.dirty_threshold:
            recommendations.append(
                "Consider scheduling non-urgent retrievals for cleaner grid periods "
                "(typically overnight or during high renewable generation)."
            )
        
        return recommendations
    
    def get_deferred_summary(self) -> Dict[str, Any]:
        """Get summary of deferred retrievals."""
        if not self.deferred_retrievals:
            return {"pending": 0}
        
        total_tokens = sum(r["tokens"] for r in self.deferred_retrievals)
        avg_wait_time = sum(
            time.time() - r["deferred_at"]
            for r in self.deferred_retrievals
        ) / len(self.deferred_retrievals)
        
        return {
            "pending": len(self.deferred_retrievals),
            "total_tokens_deferred": total_tokens,
            "avg_wait_time_seconds": avg_wait_time,
            "oldest_deferred": min(r["deferred_at"] for r in self.deferred_retrievals),
            "deferred_by_type": self._count_by_type(self.deferred_retrievals)
        }
    
    def _count_by_type(self, retrievals: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count retrievals by content type."""
        counts = {}
        for r in retrievals:
            ctype = r.get("content_type", "unknown")
            counts[ctype] = counts.get(ctype, 0) + 1
        return counts
    
    def force_process_deferred(self) -> List[Dict[str, Any]]:
        """Force process all deferred retrievals (override carbon check)."""
        processed = []
        for retrieval in self.deferred_retrievals:
            processed.append({
                **retrieval,
                "processed_at": time.time(),
                "processed_intensity": self.current_intensity_g_kwh,
                "forced": True
            })
        
        self.deferred_retrievals = []
        return processed
