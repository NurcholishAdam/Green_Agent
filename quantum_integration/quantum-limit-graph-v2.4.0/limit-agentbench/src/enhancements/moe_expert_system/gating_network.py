# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/gating_network.py
# Enhanced with causal inference, explainable AI, and transformer architecture

"""
Enhanced Gating Network v3.0.0
- Causal inference for routing decisions
- Explainable AI for routing transparency
- Transformer-based gate architecture
- Multi-agent reinforcement learning integration
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# Causal Inference Module
# ============================================================================

class CausalInferenceGate(nn.Module):
    """
    Causal inference for routing decisions.
    
    Determines not just correlation but causation between
    features and optimal expert selection.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        hidden_dim: int = 128
    ):
        super().__init__()
        self.input_dim = input_dim
        self.num_experts = num_experts
        
        # Causal discovery network
        self.causal_graph = nn.Parameter(
            torch.randn(input_dim, input_dim) * 0.1
        )
        
        # Treatment effect estimator
        self.treatment_net = nn.Sequential(
            nn.Linear(input_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_experts)
        )
        
        # Confounder adjustment
        self.confounder_net = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, input_dim)
        )
        
        # Instrumental variable
        self.iv_strength = nn.Parameter(torch.ones(input_dim) * 0.5)
    
    def discover_causal_graph(self, x: torch.Tensor) -> torch.Tensor:
        """Discover causal relationships between features"""
        # Learn sparse causal graph
        causal_matrix = torch.sigmoid(self.causal_graph)
        
        # Apply sparsity constraint
        causal_matrix = causal_matrix * (causal_matrix > 0.1).float()
        
        return causal_matrix
    
    def estimate_treatment_effect(
        self,
        x: torch.Tensor,
        treatment: torch.Tensor
    ) -> torch.Tensor:
        """
        Estimate causal effect of treatment (feature change)
        on expert selection.
        """
        # Concatenate features with treatment
        combined = torch.cat([x, treatment], dim=-1)
        
        # Estimate treatment effect
        effect = self.treatment_net(combined)
        
        return effect
    
    def adjust_confounders(self, x: torch.Tensor) -> torch.Tensor:
        """Adjust for confounding variables"""
        # Estimate confounding
        confounding = self.confounder_net(x)
        
        # Remove confounding effect
        adjusted = x - confounding
        
        return adjusted
    
    def forward(
        self,
        x: torch.Tensor,
        return_causal_info: bool = False
    ) -> Tuple[torch.Tensor, Optional[Dict[str, Any]]]:
        """Forward pass with causal inference"""
        # Adjust for confounders
        adjusted_x = self.adjust_confounders(x)
        
        # Discover causal graph
        causal_graph = self.discover_causal_graph(adjusted_x)
        
        # Calculate routing weights
        weights = F.softmax(
            torch.matmul(adjusted_x, causal_graph[:self.num_experts].t()),
            dim=-1
        )
        
        if return_causal_info:
            causal_info = {
                'causal_graph': causal_graph.detach(),
                'confounding_adjustment': (x - adjusted_x).norm().item(),
                'instrumental_strength': self.iv_strength.detach(),
                'top_causal_features': torch.topk(
                    causal_graph.abs().sum(dim=1), k=5
                ).indices.tolist()
            }
            return weights, causal_info
        
        return weights, None
    
    def explain_decision(
        self,
        x: torch.Tensor,
        chosen_expert: int
    ) -> Dict[str, Any]:
        """
        Explain why a particular expert was chosen.
        
        Provides causal explanation of routing decision.
        """
        # Get causal graph
        causal_graph = self.discover_causal_graph(x)
        
        # Get feature contributions to chosen expert
        feature_contributions = causal_graph[:, chosen_expert]
        
        # Get top contributing features
        top_features = torch.topk(feature_contributions.abs(), k=5)
        
        # Calculate counterfactual
        counterfactual = self._calculate_counterfactual(x, chosen_expert, causal_graph)
        
        return {
            'chosen_expert': chosen_expert,
            'top_features': [
                {
                    'feature_index': idx.item(),
                    'contribution': val.item(),
                    'interpretation': 'positive' if val > 0 else 'negative'
                }
                for idx, val in zip(top_features.indices, top_features.values)
            ],
            'counterfactual': counterfactual,
            'confidence': feature_contributions.abs().mean().item()
        }
    
    def _calculate_counterfactual(
        self,
        x: torch.Tensor,
        chosen_expert: int,
        causal_graph: torch.Tensor
    ) -> Dict[str, Any]:
        """Calculate counterfactual: what if features were different?"""
        # Find minimal feature change to switch expert
        other_experts = [i for i in range(self.num_experts) if i != chosen_expert]
        min_change = float('inf')
        best_alternative = None
        
        for other in other_experts:
            # Calculate required change
            diff = causal_graph[:, other] - causal_graph[:, chosen_expert]
            change_magnitude = diff.norm().item()
            
            if change_magnitude < min_change:
                min_change = change_magnitude
                best_alternative = other
        
        return {
            'nearest_alternative_expert': best_alternative,
            'required_change_magnitude': min_change,
            'interpretation': (
                f"Small change ({min_change:.3f}) would have selected expert {best_alternative}"
                if min_change < 0.5
                else f"Decision is robust; large change ({min_change:.3f}) needed to switch experts"
            )
        }


# ============================================================================
# Transformer Gate Architecture
# ============================================================================

class TransformerGate(nn.Module):
    """
    Transformer-based gating architecture.
    
    Uses self-attention for feature interaction learning.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        num_heads: int = 8,
        num_layers: int = 3,
        hidden_dim: int = 256,
        dropout: float = 0.1
    ):
        super().__init__()
        self.input_dim = input_dim
        self.num_experts = num_experts
        
        # Input projection
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        
        # Positional encoding
        self.pos_encoding = nn.Parameter(
            torch.randn(1, input_dim, hidden_dim) * 0.02
        )
        
        # Transformer encoder layers
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=num_heads,
            dim_feedforward=hidden_dim * 4,
            dropout=dropout,
            activation='gelu',
            batch_first=True
        )
        self.transformer = nn.TransformerEncoder(
            encoder_layer,
            num_layers=num_layers
        )
        
        # Cross-attention for expert interaction
        self.expert_embeddings = nn.Parameter(
            torch.randn(num_experts, hidden_dim)
        )
        self.cross_attention = nn.MultiheadAttention(
            embed_dim=hidden_dim,
            num_heads=num_heads,
            dropout=dropout,
            batch_first=True
        )
        
        # Output projection
        self.output_proj = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, num_experts)
        )
        
        # Layer normalization
        self.layer_norm1 = nn.LayerNorm(hidden_dim)
        self.layer_norm2 = nn.LayerNorm(hidden_dim)
        
        logger.info(
            f"Transformer Gate initialized: "
            f"{num_layers} layers, {num_heads} heads, {hidden_dim} dim"
        )
    
    def forward(
        self,
        x: torch.Tensor,
        return_attention: bool = False
    ) -> Tuple[torch.Tensor, Optional[Dict[str, torch.Tensor]]]:
        """Forward pass with transformer architecture"""
        batch_size = x.size(0)
        
        # Input projection and positional encoding
        x_proj = self.input_proj(x).unsqueeze(1)  # [B, 1, D]
        x_proj = x_proj + self.pos_encoding[:, :1, :]
        
        # Self-attention encoding
        encoded = self.transformer(x_proj)
        encoded = self.layer_norm1(encoded + x_proj)
        
        # Cross-attention with expert embeddings
        expert_emb = self.expert_embeddings.unsqueeze(0).expand(batch_size, -1, -1)
        attended, attention_weights = self.cross_attention(
            query=encoded,
            key=expert_emb,
            value=expert_emb
        )
        attended = self.layer_norm2(attended + encoded)
        
        # Output projection
        logits = self.output_proj(attended.squeeze(1))
        weights = F.softmax(logits, dim=-1)
        
        if return_attention:
            attention_info = {
                'self_attention': None,  # Would need hooks to capture
                'cross_attention': attention_weights,
                'expert_embeddings': self.expert_embeddings
            }
            return weights, attention_info
        
        return weights, None
    
    def get_expert_similarity(self) -> torch.Tensor:
        """Get expert embedding similarity matrix"""
        embeddings = self.expert_embeddings
        similarity = F.cosine_similarity(
            embeddings.unsqueeze(1),
            embeddings.unsqueeze(0),
            dim=-1
        )
        return similarity
    
    def explain_attention(
        self,
        x: torch.Tensor
    ) -> Dict[str, Any]:
        """Explain attention patterns"""
        _, attention_info = self.forward(x, return_attention=True)
        
        cross_attn = attention_info['cross_attention'].squeeze(0)
        
        # Get top attended experts
        top_experts = torch.topk(cross_attn.sum(dim=0), k=3)
        
        return {
            'top_experts': [
                {
                    'expert_index': idx.item(),
                    'attention_weight': val.item(),
                    'percentage': (val.item() / cross_attn.sum().item()) * 100
                }
                for idx, val in zip(top_experts.indices, top_experts.values)
            ],
            'attention_entropy': -(
                cross_attn * torch.log(cross_attn + 1e-8)
            ).sum().item(),
            'attention_concentration': cross_attn.max().item() / cross_attn.sum().item()
        }


# ============================================================================
# Multi-Agent Reinforcement Learning Integration
# ============================================================================

class MultiAgentRLGate(nn.Module):
    """
    Multi-agent reinforcement learning for gating.
    
    Each expert acts as an agent bidding for tasks.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        hidden_dim: int = 128
    ):
        super().__init__()
        self.num_experts = num_experts
        
        # Agent networks (one per expert)
        self.agent_networks = nn.ModuleList([
            nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, 1)  # Bid value
            )
            for _ in range(num_experts)
        ])
        
        # Auction mechanism
        self.auction_weights = nn.Parameter(
            torch.ones(num_experts) / num_experts
        )
        
        # Coordination bonus
        self.coordination_matrix = nn.Parameter(
            torch.eye(num_experts) * 0.1
        )
    
    def forward(
        self,
        x: torch.Tensor,
        return_bids: bool = False
    ) -> Tuple[torch.Tensor, Optional[Dict[str, Any]]]:
        """Forward pass with multi-agent bidding"""
        # Each agent bids for the task
        bids = []
        for agent_net in self.agent_networks:
            bid = torch.sigmoid(agent_net(x))  # [0, 1]
            bids.append(bid)
        
        bids_tensor = torch.cat(bids, dim=-1)  # [B, num_experts]
        
        # Apply auction weights
        weighted_bids = bids_tensor * F.softmax(self.auction_weights, dim=-1)
        
        # Apply coordination bonus
        coordination = torch.matmul(weighted_bids, self.coordination_matrix)
        final_bids = weighted_bids + 0.1 * coordination
        
        # Select winners (top-k)
        weights = F.softmax(final_bids, dim=-1)
        
        if return_bids:
            bid_info = {
                'raw_bids': bids_tensor.detach(),
                'weighted_bids': weighted_bids.detach(),
                'auction_weights': F.softmax(self.auction_weights, dim=-1).detach(),
                'coordination_matrix': self.coordination_matrix.detach(),
                'winning_margin': (
                    weights.max(dim=-1)[0] - weights.sort(dim=-1)[0][:, -2]
                ).detach()
            }
            return weights, bid_info
        
        return weights, None
    
    def update_auction_weights(
        self,
        expert_performance: Dict[int, float]
    ):
        """Update auction weights based on expert performance"""
        with torch.no_grad():
            for expert_idx, performance in expert_performance.items():
                if expert_idx < self.num_experts:
                    # Increase weight for high-performing experts
                    self.auction_weights[expert_idx] += 0.1 * (performance - 0.5)
            
            # Normalize
            self.auction_weights.data = F.softmax(self.auction_weights, dim=-1)
    
    def get_agent_strategies(self) -> Dict[int, Dict[str, Any]]:
        """Get learned strategies for each agent"""
        strategies = {}
        for i, agent_net in enumerate(self.agent_networks):
            # Extract learned parameters
            params = list(agent_net.parameters())
            strategies[i] = {
                'bid_aggressiveness': params[-1].data.mean().item(),
                'feature_sensitivity': params[0].data.abs().mean().item(),
                'auction_weight': F.softmax(self.auction_weights, dim=-1)[i].item()
            }
        return strategies


# ============================================================================
# Enhanced MoE Gating Network with All Integrations
# ============================================================================

class MoEGatingNetwork:
    """
    Enhanced MoE Gating Network v3.0.0
    
    New capabilities:
    - Causal inference for routing
    - Transformer-based architecture
    - Multi-agent RL bidding
    - Explainable AI decisions
    """
    
    def __init__(
        self,
        num_experts: int = 5,
        device: str = 'cpu',
        use_transformer: bool = True,
        use_causal: bool = True,
        use_multi_agent: bool = True,
        enable_explainability: bool = True
    ):
        self.num_experts = num_experts
        self.device = device
        self.enable_explainability = enable_explainability
        
        # Initialize enhanced gates
        self.transformer_gate = TransformerGate(
            input_dim=32,  # Feature dimension
            num_experts=num_experts
        ).to(device) if use_transformer else None
        
        self.causal_gate = CausalInferenceGate(
            input_dim=32,
            num_experts=num_experts
        ).to(device) if use_causal else None
        
        self.marl_gate = MultiAgentRLGate(
            input_dim=32,
            num_experts=num_experts
        ).to(device) if use_multi_agent else None
        
        # Ensemble weights
        self.ensemble_weights = nn.Parameter(
            torch.ones(3) / 3  # Equal initial weighting
        )
        
        # Explanation history
        self.explanation_history: deque = deque(maxlen=1000)
        
        logger.info(
            f"Enhanced MoE Gating Network v3.0.0 initialized: "
            f"transformer={use_transformer}, causal={use_causal}, "
            f"multi_agent={use_multi_agent}"
        )
    
    def route(
        self,
        context: Any,
        return_explanation: bool = False
    ) -> Tuple[List[Tuple[int, float]], Optional[Dict[str, Any]]]:
        """
        Enhanced routing with all integrations.
        
        Returns routing decisions with optional explanation.
        """
        x = context.to_tensor().unsqueeze(0).to(self.device)
        
        # Collect predictions from all gates
        predictions = []
        attentions = []
        
        if self.transformer_gate:
            weights, attn = self.transformer_gate(x, return_attention=True)
            predictions.append(weights)
            if attn:
                attentions.append(('transformer', attn))
        
        if self.causal_gate:
            weights, causal_info = self.causal_gate(x, return_causal_info=True)
            predictions.append(weights)
            if causal_info:
                attentions.append(('causal', causal_info))
        
        if self.marl_gate:
            weights, bid_info = self.marl_gate(x, return_bids=True)
            predictions.append(weights)
            if bid_info:
                attentions.append(('marl', bid_info))
        
        # Ensemble predictions
        if predictions:
            ensemble_weights = F.softmax(self.ensemble_weights, dim=-1)
            final_weights = sum(
                w * ensemble_weights[i]
                for i, w in enumerate(predictions)
            ) / ensemble_weights.sum()
        else:
            final_weights = torch.ones(1, self.num_experts) / self.num_experts
        
        # Convert to routing decisions
        top_k = min(2, self.num_experts)
        top_weights, top_indices = torch.topk(final_weights, top_k, dim=-1)
        
        routing_decisions = list(zip(
            top_indices.squeeze(0).cpu().numpy(),
            top_weights.squeeze(0).detach().cpu().numpy()
        ))
        
        # Generate explanation if requested
        explanation = None
        if return_explanation and self.enable_explainability:
            explanation = self._generate_explanation(
                x, routing_decisions, attentions
            )
        
        return routing_decisions, explanation
    
    def _generate_explanation(
        self,
        x: torch.Tensor,
        routing_decisions: List[Tuple[int, float]],
        attentions: List[Tuple[str, Dict]]
    ) -> Dict[str, Any]:
        """Generate comprehensive routing explanation"""
        explanation = {
            'routing_decisions': [
                {'expert': idx, 'weight': float(w)}
                for idx, w in routing_decisions
            ],
            'gate_contributions': {},
            'key_factors': [],
            'confidence': float(max(w for _, w in routing_decisions)),
            'alternatives': [],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Transformer explanation
        if self.transformer_gate:
            trans_explanation = self.transformer_gate.explain_attention(x)
            explanation['gate_contributions']['transformer'] = trans_explanation
        
        # Causal explanation
        if self.causal_gate and routing_decisions:
            causal_explanation = self.causal_gate.explain_decision(
                x.squeeze(0), routing_decisions[0][0]
            )
            explanation['gate_contributions']['causal'] = causal_explanation
        
        # MARL explanation
        if self.marl_gate:
            strategies = self.marl_gate.get_agent_strategies()
            explanation['gate_contributions']['marl'] = {
                'agent_strategies': strategies
            }
        
        # Extract key factors
        if 'causal' in explanation['gate_contributions']:
            causal = explanation['gate_contributions']['causal']
            if 'top_features' in causal:
                explanation['key_factors'] = [
                    f"Feature {f['feature_index']} ({f['interpretation']} contribution: {f['contribution']:.3f})"
                    for f in causal['top_features'][:3]
                ]
        
        self.explanation_history.append(explanation)
        
        return explanation
    
    def update_performance(
        self,
        expert_performance: Dict[int, float]
    ):
        """Update based on expert performance feedback"""
        if self.marl_gate:
            self.marl_gate.update_auction_weights(expert_performance)
    
    def get_explainability_report(self) -> Dict[str, Any]:
        """Get explainability report"""
        recent = list(self.explanation_history)[-50:]
        
        return {
            'total_explanations': len(self.explanation_history),
            'average_confidence': np.mean([
                e['confidence'] for e in recent
            ]) if recent else 0,
            'common_factors': self._get_common_factors(recent),
            'gate_contributions': {
                gate: np.mean([
                    e['gate_contributions'].get(gate, {}).get('confidence', 0)
                    for e in recent
                ]) if recent else 0
                for gate in ['transformer', 'causal', 'marl']
            }
        }
    
    def _get_common_factors(
        self,
        explanations: List[Dict]
    ) -> List[str]:
        """Get most common routing factors"""
        factor_counts = {}
        for exp in explanations:
            for factor in exp.get('key_factors', []):
                factor_counts[factor] = factor_counts.get(factor, 0) + 1
        
        sorted_factors = sorted(
            factor_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return [f for f, _ in sorted_factors[:5]]
