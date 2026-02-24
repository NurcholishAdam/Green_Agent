# -*- coding: utf-8 -*-
"""
Token-Aware Filtering Layer

Distinguishes between small vs. large retrieval contexts and dynamically
adjusts retrieval scope based on carbon grid intensity.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum


class ContextSize(Enum):
    """Context size categories."""
    TINY = "tiny"  # < 512 tokens
    SMALL = "small"  # 512-2048 tokens
    MEDIUM = "medium"  # 2048-8192 tokens
    LARGE = "large"  # 8192-32768 tokens
    XLARGE = "xlarge"  # > 32768 tokens


class GridIntensity(Enum):
    """Carbon grid intensity levels."""
    CLEAN = "clean"  # < 200 g CO2/kWh
    MODERATE = "moderate"  # 200-400 g CO2/kWh
    DIRTY = "dirty"  # > 400 g CO2/kWh


@dataclass
class FilteringPolicy:
    """Policy for token-aware filtering."""
    grid_intensity: GridIntensity
    max_tokens_tiny: int
    max_tokens_small: int
    max_tokens_medium: int
    max_tokens_large: int
    max_tokens_xlarge: int
    allow_visual_context: bool
    allow_multimodal: bool


class TokenAwareFilter:
    """
    Token-aware filtering layer for energy-efficient retrieval.
    
    Responsibilities:
    - Distinguish between small vs. large retrieval contexts
    - Dynamically adjust retrieval scope based on carbon grid intensity
    - Prevent wasted compute cycles on oversized or irrelevant sources
    - Provide filtering recommendations for meta-cognitive reflection
    """
    
    def __init__(
        self,
        current_grid_intensity_g_kwh: float = 385.0,
        enable_adaptive_filtering: bool = True
    ):
        """
        Initialize token-aware filter.
        
        Args:
            current_grid_intensity_g_kwh: Current carbon grid intensity
            enable_adaptive_filtering: Enable dynamic filtering based on grid
        """
        self.grid_intensity_value = current_grid_intensity_g_kwh
        self.enable_adaptive = enable_adaptive_filtering
        
        # Determine grid intensity level
        self.grid_intensity = self._classify_grid_intensity(current_grid_intensity_g_kwh)
        
        # Create filtering policy based on grid intensity
        self.policy = self._create_policy(self.grid_intensity)
        
        # Filtering statistics
        self.filtered_count = 0
        self.total_requests = 0
        self.tokens_saved = 0
        self.energy_saved_wh = 0.0
        
    def _classify_grid_intensity(self, intensity_g_kwh: float) -> GridIntensity:
        """Classify grid intensity level."""
        if intensity_g_kwh < 200:
            return GridIntensity.CLEAN
        elif intensity_g_kwh < 400:
            return GridIntensity.MODERATE
        else:
            return GridIntensity.DIRTY
    
    def _create_policy(self, grid_intensity: GridIntensity) -> FilteringPolicy:
        """Create filtering policy based on grid intensity."""
        if grid_intensity == GridIntensity.CLEAN:
            # Relaxed limits during clean grid periods
            return FilteringPolicy(
                grid_intensity=grid_intensity,
                max_tokens_tiny=512,
                max_tokens_small=2048,
                max_tokens_medium=8192,
                max_tokens_large=32768,
                max_tokens_xlarge=65536,
                allow_visual_context=True,
                allow_multimodal=True
            )
        elif grid_intensity == GridIntensity.MODERATE:
            # Moderate limits
            return FilteringPolicy(
                grid_intensity=grid_intensity,
                max_tokens_tiny=512,
                max_tokens_small=2048,
                max_tokens_medium=6144,
                max_tokens_large=16384,
                max_tokens_xlarge=32768,
                allow_visual_context=True,
                allow_multimodal=False
            )
        else:  # DIRTY
            # Strict limits during dirty grid periods
            return FilteringPolicy(
                grid_intensity=grid_intensity,
                max_tokens_tiny=256,
                max_tokens_small=1024,
                max_tokens_medium=4096,
                max_tokens_large=8192,
                max_tokens_xlarge=16384,
                allow_visual_context=False,
                allow_multimodal=False
            )
    
    def classify_context_size(self, token_count: int) -> ContextSize:
        """Classify context size based on token count."""
        if token_count < 512:
            return ContextSize.TINY
        elif token_count < 2048:
            return ContextSize.SMALL
        elif token_count < 8192:
            return ContextSize.MEDIUM
        elif token_count < 32768:
            return ContextSize.LARGE
        else:
            return ContextSize.XLARGE
    
    def should_filter(
        self,
        token_count: int,
        content_type: str,
        relevance_score: float = 0.5
    ) -> Dict[str, Any]:
        """
        Determine if content should be filtered based on policy.
        
        Args:
            token_count: Number of tokens in content
            content_type: Type of content ("text", "visual", "multimodal")
            relevance_score: Relevance score (0-1)
            
        Returns:
            Filtering decision with reasoning
        """
        self.total_requests += 1
        
        context_size = self.classify_context_size(token_count)
        
        # Check content type restrictions
        if content_type == "visual" and not self.policy.allow_visual_context:
            self.filtered_count += 1
            self.tokens_saved += token_count
            self.energy_saved_wh += token_count * 0.000001 * 2.5  # Visual multiplier
            return {
                "should_filter": True,
                "reason": "visual_content_restricted_during_dirty_grid",
                "context_size": context_size.value,
                "grid_intensity": self.grid_intensity.value
            }
        
        if content_type == "multimodal" and not self.policy.allow_multimodal:
            self.filtered_count += 1
            self.tokens_saved += token_count
            self.energy_saved_wh += token_count * 0.000001 * 2.5
            return {
                "should_filter": True,
                "reason": "multimodal_content_restricted",
                "context_size": context_size.value,
                "grid_intensity": self.grid_intensity.value
            }
        
        # Check token limits based on context size
        max_tokens = self._get_max_tokens_for_size(context_size)
        if token_count > max_tokens:
            self.filtered_count += 1
            self.tokens_saved += token_count - max_tokens
            self.energy_saved_wh += (token_count - max_tokens) * 0.000001
            return {
                "should_filter": True,
                "reason": f"exceeds_max_tokens_for_{context_size.value}",
                "max_allowed": max_tokens,
                "actual": token_count,
                "grid_intensity": self.grid_intensity.value
            }
        
        # Check relevance threshold (stricter during dirty grid)
        min_relevance = 0.7 if self.grid_intensity == GridIntensity.DIRTY else 0.5
        if relevance_score < min_relevance:
            self.filtered_count += 1
            self.tokens_saved += token_count
            self.energy_saved_wh += token_count * 0.000001
            return {
                "should_filter": True,
                "reason": "low_relevance_score",
                "min_required": min_relevance,
                "actual": relevance_score,
                "grid_intensity": self.grid_intensity.value
            }
        
        # Allow retrieval
        return {
            "should_filter": False,
            "reason": "passes_all_filters",
            "context_size": context_size.value,
            "grid_intensity": self.grid_intensity.value
        }
    
    def _get_max_tokens_for_size(self, context_size: ContextSize) -> int:
        """Get maximum tokens allowed for context size."""
        if context_size == ContextSize.TINY:
            return self.policy.max_tokens_tiny
        elif context_size == ContextSize.SMALL:
            return self.policy.max_tokens_small
        elif context_size == ContextSize.MEDIUM:
            return self.policy.max_tokens_medium
        elif context_size == ContextSize.LARGE:
            return self.policy.max_tokens_large
        else:  # XLARGE
            return self.policy.max_tokens_xlarge
    
    def update_grid_intensity(self, new_intensity_g_kwh: float):
        """
        Update grid intensity and adjust policy.
        
        Args:
            new_intensity_g_kwh: New carbon grid intensity
        """
        self.grid_intensity_value = new_intensity_g_kwh
        old_intensity = self.grid_intensity
        self.grid_intensity = self._classify_grid_intensity(new_intensity_g_kwh)
        
        # Update policy if intensity level changed
        if self.grid_intensity != old_intensity:
            self.policy = self._create_policy(self.grid_intensity)
    
    def get_filtering_stats(self) -> Dict[str, Any]:
        """Get filtering statistics."""
        filter_rate = (self.filtered_count / self.total_requests * 100) if self.total_requests > 0 else 0
        
        return {
            "total_requests": self.total_requests,
            "filtered_count": self.filtered_count,
            "filter_rate_pct": filter_rate,
            "tokens_saved": self.tokens_saved,
            "energy_saved_wh": self.energy_saved_wh,
            "current_grid_intensity": self.grid_intensity.value,
            "current_grid_intensity_g_kwh": self.grid_intensity_value,
            "current_policy": {
                "allow_visual": self.policy.allow_visual_context,
                "allow_multimodal": self.policy.allow_multimodal,
                "max_tokens_medium": self.policy.max_tokens_medium
            }
        }
    
    def get_recommendations(self) -> List[str]:
        """Get filtering recommendations for meta-cognitive reflection."""
        recommendations = []
        
        stats = self.get_filtering_stats()
        
        if stats["filter_rate_pct"] > 50:
            recommendations.append(
                "High filter rate detected. Consider adjusting relevance thresholds or query specificity."
            )
        
        if self.grid_intensity == GridIntensity.DIRTY:
            recommendations.append(
                "Operating under dirty grid conditions. Visual and multimodal content restricted."
            )
            recommendations.append(
                "Consider deferring non-urgent retrievals to cleaner grid periods."
            )
        
        if stats["tokens_saved"] > 10000:
            recommendations.append(
                f"Token-aware filtering has saved {stats['tokens_saved']} tokens "
                f"and {stats['energy_saved_wh']:.4f} Wh of energy."
            )
        
        return recommendations
    
    def reset_stats(self):
        """Reset filtering statistics."""
        self.filtered_count = 0
        self.total_requests = 0
        self.tokens_saved = 0
        self.energy_saved_wh = 0.0
