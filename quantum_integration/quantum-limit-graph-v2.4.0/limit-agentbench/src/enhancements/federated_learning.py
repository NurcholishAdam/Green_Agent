# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/federated_learner.py
# Complete enhanced file v5.0.0

"""
Enhanced Federated Learner v5.0.0
Complete implementation with bio-inspired integration.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict, deque
import hashlib, json

logger = logging.getLogger(__name__)

BIO_AVAILABLE = False
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPSource, EcoATPConsumer
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage, StorageTier, GuaranteeLevel
    BIO_AVAILABLE = True
except ImportError:
    pass

@dataclass
class FederatedClient:
    client_id: str
    local_model: Dict[str, Any]
    data_size: int
    compute_power_flops: float
    carbon_intensity_g_per_kwh: float = 400.0
    renewable_energy_percent: float = 0.0
    token_balance: float = 0.0
    tokens_earned: float = 0.0
    trust_score: float = 0.5
    participation_count: int = 0
    success_count: int = 0
    last_participation: Optional[datetime] = None
    is_active: bool = True
    
    @property
    def success_rate(self) -> float:
        return self.success_count / max(self.participation_count, 1)
    
    @property
    def carbon_score(self) -> float:
        return min(1.0, 1.0/(1.0+self.carbon_intensity_g_per_kwh/100) + self.renewable_energy_percent*0.3)

@dataclass
class FederationRound:
    round_id: str
    round_number: int
    participants: List[str]
    tokens_distributed: float = 0.0
    gradient_trust_updates: Dict[str, float] = field(default_factory=dict)
    biomass_checkpoint_token: Optional[str] = None
    carbon_emitted_kg: float = 0.0
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    successful: bool = False

class EnhancedFederatedLearner:
    """Enhanced Federated Learner v5.0.0"""
    
    def __init__(self, token_manager=None, gradient_manager=None, biomass_storage=None,
                 min_clients: int = 3, privacy_epsilon: float = 1.0,
                 enable_incentives: bool = True, enable_gradient_trust: bool = True,
                 enable_biomass_checkpoints: bool = True):
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.biomass_storage = biomass_storage
        self.min_clients = min_clients
        self.privacy_epsilon = privacy_epsilon
        self.enable_incentives = enable_incentives
        self.enable_gradient_trust = enable_gradient_trust
        self.enable_biomass_checkpoints = enable_biomass_checkpoints
        
        self.clients: Dict[str, FederatedClient] = {}
        self.global_model: Optional[Dict[str, Any]] = None
        self.rounds: List[FederationRound] = []
        self.round_number = 0
        self.incentive_pool: float = 10000.0
        self.account_id = "federated_learner"
        
        if self.token_manager:
            self.token_manager.create_account(self.account_id)
        
        logger.info(f"Enhanced Federated Learner v5.0.0 initialized")
    
    def register_client(self, client_id: str, initial_model: Dict[str, Any],
                       data_size: int, compute_power_flops: float,
                       carbon_intensity: float = 400.0,
                       renewable_percent: float = 0.0) -> FederatedClient:
        if client_id in self.clients: return self.clients[client_id]
        
        client = FederatedClient(client_id=client_id, local_model=initial_model,
                                 data_size=data_size, compute_power_flops=compute_power_flops,
                                 carbon_intensity_g_per_kwh=carbon_intensity,
                                 renewable_energy_percent=renewable_percent)
        
        if self.token_manager:
            self.token_manager.create_account(f"federated_{client_id}")
            tokens = self.token_manager.generate_tokens(
                account_id=f"federated_{client_id}", source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=0.001, num_tokens=int(data_size/100))
            if tokens:
                client.token_balance = sum(t.value for t in tokens)
        
        if self.enable_gradient_trust and self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust: client.trust_score = trust.effective_strength
        
        self.clients[client_id] = client
        logger.info(f"Registered client: {client_id}")
        return client
    
    def _select_clients(self, num_select: int) -> List[str]:
        candidates = []
        for cid, c in self.clients.items():
            if not c.is_active: continue
            score = (c.carbon_score * 0.35 + c.trust_score * 0.30 +
                    min(1.0, c.data_size/10000) * 0.20 + min(1.0, c.participation_count/10) * 0.15)
            candidates.append((cid, score))
        candidates.sort(key=lambda x: x[1], reverse=True)
        return [c[0] for c in candidates[:num_select]]
    
    async def federated_round(self) -> Optional[Dict[str, Any]]:
        self.round_number += 1
        selected = self._select_clients(max(self.min_clients, len(self.clients)//2))
        if len(selected) < self.min_clients: return None
        
        fr = FederationRound(round_id=f"r{self.round_number}_{datetime.utcnow().timestamp()}",
                            round_number=self.round_number, participants=selected)
        
        total_carbon, total_tokens = 0.0, 0.0
        updates = {}
        
        for cid in selected:
            c = self.clients[cid]
            updates[cid] = self._apply_privacy(c.local_model)
            total_carbon += c.carbon_intensity_g_per_kwh * 0.001 / 1000
            
            if self.enable_incentives and self.token_manager:
                reward = 10.0 + c.carbon_score * 5.0 + c.trust_score * 3.0 + min(5.0, c.data_size/2000)
                tokens = self.token_manager.generate_tokens(
                    account_id=f"federated_{cid}", source=EcoATPSource.EFFICIENCY_GAIN,
                    energy_saved_kwh=reward/10000.0, num_tokens=int(reward))
                if tokens:
                    rv = sum(t.value for t in tokens)
                    c.tokens_earned += rv; c.token_balance += rv; total_tokens += rv
            
            if self.enable_gradient_trust and self.gradient_manager:
                td = 0.05 * c.success_rate
                self.gradient_manager.pump_field('trust', td, source=f"federated_{cid}")
                fr.gradient_trust_updates[cid] = td
            
            c.participation_count += 1; c.last_participation = datetime.utcnow()
        
        if updates:
            self.global_model = self._aggregate(updates)
            if self.enable_biomass_checkpoints and self.biomass_storage:
                s, t = self.biomass_storage.store_task(
                    task_data={'model': str(self.global_model)[:500], 'round': self.round_number},
                    ecoatp_cost=5.0, guarantee=GuaranteeLevel.SILVER,
                    initial_tier=StorageTier.STARCH_RESERVE)
                if s: fr.biomass_checkpoint_token = t
        
        fr.tokens_distributed = total_tokens; fr.carbon_emitted_kg = total_carbon
        fr.completed_at = datetime.utcnow(); fr.successful = True
        self.rounds.append(fr)
        
        logger.info(f"Round {self.round_number}: {len(updates)} clients, tokens={total_tokens:.1f}")
        return self.global_model
    
    def _apply_privacy(self, model: Dict[str, Any]) -> Dict[str, Any]:
        if self.privacy_epsilon <= 0: return model
        pm = {}
        for k, v in model.items():
            if isinstance(v, (int, float)):
                pm[k] = v + np.random.laplace(0, 1.0/self.privacy_epsilon)
            elif isinstance(v, np.ndarray):
                pm[k] = v + np.random.laplace(0, 1.0/self.privacy_epsilon, v.shape)
            else: pm[k] = v
        return pm
    
    def _aggregate(self, updates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        if not updates: return {}
        w = {cid: (self.clients[cid].trust_score * self.clients[cid].data_size if cid in self.clients else 1.0)
             for cid in updates}
        tw = sum(w.values())
        agg = {}
        for key in next(iter(updates.values())):
            ws = None
            for cid, u in updates.items():
                if key in u:
                    weight = w[cid]/tw
                    ws = u[key]*weight if ws is None else ws + u[key]*weight
            if ws is not None: agg[key] = ws
        return agg
    
    def get_federation_stats(self) -> Dict[str, Any]:
        recent = self.rounds[-20:] if self.rounds else []
        return {
            'total_clients': len(self.clients),
            'active_clients': sum(1 for c in self.clients.values() if c.is_active),
            'total_rounds': len(self.rounds),
            'success_rate': sum(1 for r in recent if r.successful)/max(len(recent),1),
            'total_tokens_distributed': sum(r.tokens_distributed for r in self.rounds),
            'total_carbon_emitted_kg': sum(r.carbon_emitted_kg for r in self.rounds),
            'biomass_checkpoints': sum(1 for r in self.rounds if r.biomass_checkpoint_token),
            'clients': {cid: {'trust': c.trust_score, 'carbon': c.carbon_score,
                             'tokens': c.tokens_earned, 'success_rate': c.success_rate}
                       for cid, c in self.clients.items()}
        }
