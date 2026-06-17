# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/biomass_storage.py
# Enhanced with global task index for O(1) lookup

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import uuid
import math

logger = logging.getLogger(__name__)

class StorageTier(Enum):
    ATP_CACHE = "atp_cache"
    GLYCOGEN_QUEUE = "glycogen_queue"
    STARCH_RESERVE = "starch_reserve"
    LIPID_DEPOT = "lipid_depot"
    LIGNIN_ARCHIVE = "lignin_archive"

class GuaranteeLevel(Enum):
    PLATINUM = "platinum"
    GOLD = "gold"
    SILVER = "silver"
    BRONZE = "bronze"
    BEST_EFFORT = "best_effort"

@dataclass
class StoredTask:
    task_id: str
    task_data: Dict[str, Any]
    storage_tier: StorageTier
    stored_at: datetime
    original_ecoatp_cost: float
    current_retrieval_cost: float = 0.0
    deadline: Optional[datetime] = None
    priority: int = 0
    execution_count: int = 0
    conversion_history: List[Dict] = field(default_factory=list)
    
    @property
    def age_hours(self) -> float:
        return (datetime.utcnow() - self.stored_at).total_seconds() / 3600
    
    @property
    def is_expired(self) -> bool:
        if self.deadline:
            return datetime.utcnow() > self.deadline
        return False
    
    @property
    def urgency(self) -> float:
        if not self.deadline:
            return 0.3
        remaining = (self.deadline - datetime.utcnow()).total_seconds()
        total = (self.deadline - self.stored_at).total_seconds()
        if total <= 0:
            return 1.0
        return 1.0 - (remaining / total)

@dataclass
class StorageToken:
    token_id: str
    task_id: str
    original_value: float
    guarantee: GuaranteeLevel
    collateral_amount: float
    storage_tier: StorageTier
    stored_at: datetime
    expires_at: datetime
    retrieval_cost: float = 0.0
    is_executed: bool = False
    penalty_paid: bool = False

# ============================================================================
# Enhanced Biomass Storage with Global Task Index
# ============================================================================

class BiomassStorage:
    """
    Enhanced Biomass Storage with:
    - Global task index for O(1) lookup (FIX 8)
    - Multi-tier storage management
    - Token-backed guarantees
    """
    
    def __init__(self, token_manager=None):
        self.token_manager = token_manager
        
        # Storage tiers
        self.atp_cache: deque = deque(maxlen=100)
        self.glycogen_queue: deque = deque(maxlen=1000)
        self.starch_reserve: deque = deque(maxlen=5000)
        self.lipid_depot: deque = deque(maxlen=10000)
        self.lignin_archive: deque = deque(maxlen=50000)
        
        # Storage tokens
        self.storage_tokens: Dict[str, StorageToken] = {}
        self.collateral_pool: float = 0.0
        
        # ====================================================================
        # FIX 8: Global Task Index for O(1) Lookup
        # ====================================================================
        self.task_index: Dict[str, Dict[str, Any]] = {}
        self.index_hits: int = 0
        self.index_misses: int = 0
        
        # Conversion costs
        self.conversion_costs = {
            (StorageTier.ATP_CACHE, StorageTier.GLYCOGEN_QUEUE): 0.5,
            (StorageTier.GLYCOGEN_QUEUE, StorageTier.STARCH_RESERVE): 2.0,
            (StorageTier.STARCH_RESERVE, StorageTier.LIPID_DEPOT): 5.0,
            (StorageTier.LIPID_DEPOT, StorageTier.LIGNIN_ARCHIVE): 10.0,
            (StorageTier.LIPID_DEPOT, StorageTier.STARCH_RESERVE): 8.0,
            (StorageTier.STARCH_RESERVE, StorageTier.GLYCOGEN_QUEUE): 4.0,
            (StorageTier.GLYCOGEN_QUEUE, StorageTier.ATP_CACHE): 2.0,
        }
        
        self.collateral_ratios = {
            GuaranteeLevel.PLATINUM: 2.0, GuaranteeLevel.GOLD: 1.5,
            GuaranteeLevel.SILVER: 1.2, GuaranteeLevel.BRONZE: 1.0,
            GuaranteeLevel.BEST_EFFORT: 0.5
        }
        
        asyncio.create_task(self._maintenance_loop())
        asyncio.create_task(self._mobilization_loop())
        
        logger.info("Enhanced Biomass Storage initialized with global task index")
    
    # ========================================================================
    # Global Task Index Methods
    # ========================================================================
    
    def _add_to_index(self, task_id: str, tier: StorageTier, position: int):
        """Add task to global index - O(1)"""
        self.task_index[task_id] = {
            'tier': tier,
            'position': position,
            'stored_at': datetime.utcnow(),
            'access_count': 0,
            'last_accessed': None
        }
    
    def _update_index_position(self, task_id: str, new_tier: StorageTier, new_position: int):
        """Update task position in index after tier conversion"""
        if task_id in self.task_index:
            self.task_index[task_id]['tier'] = new_tier
            self.task_index[task_id]['position'] = new_position
            self.task_index[task_id]['stored_at'] = datetime.utcnow()
    
    def _remove_from_index(self, task_id: str):
        """Remove task from index"""
        self.task_index.pop(task_id, None)
    
    def find_task(self, task_id: str) -> Optional[Tuple[StorageTier, int]]:
        """
        Find task location using global index.
        
        FIX 8: O(1) lookup instead of O(n) scanning across all tiers.
        """
        if task_id in self.task_index:
            self.index_hits += 1
            entry = self.task_index[task_id]
            entry['access_count'] += 1
            entry['last_accessed'] = datetime.utcnow()
            return entry['tier'], entry['position']
        
        self.index_misses += 1
        return None
    
    def _get_from_tier_position(self, tier: StorageTier, position: int) -> Optional[StoredTask]:
        """Get task directly from tier position - O(1)"""
        tier_map = {
            StorageTier.ATP_CACHE: self.atp_cache,
            StorageTier.GLYCOGEN_QUEUE: self.glycogen_queue,
            StorageTier.STARCH_RESERVE: self.starch_reserve,
            StorageTier.LIPID_DEPOT: self.lipid_depot,
            StorageTier.LIGNIN_ARCHIVE: self.lignin_archive
        }
        
        queue = tier_map.get(tier)
        if queue and position < len(queue):
            return queue[position]
        return None
    
    def _scan_all_tiers(self, task_id: str) -> Optional[StoredTask]:
        """Fallback: scan all tiers if index miss"""
        for tier in [StorageTier.ATP_CACHE, StorageTier.GLYCOGEN_QUEUE,
                     StorageTier.STARCH_RESERVE, StorageTier.LIPID_DEPOT,
                     StorageTier.LIGNIN_ARCHIVE]:
            queue = self._get_tier_queue(tier)
            for i, task in enumerate(queue):
                if task.task_id == task_id:
                    # Re-index for future lookups
                    self._add_to_index(task_id, tier, i)
                    return task
        return None
    
    def _get_tier_queue(self, tier: StorageTier) -> deque:
        """Get the queue for a storage tier"""
        tier_map = {
            StorageTier.ATP_CACHE: self.atp_cache,
            StorageTier.GLYCOGEN_QUEUE: self.glycogen_queue,
            StorageTier.STARCH_RESERVE: self.starch_reserve,
            StorageTier.LIPID_DEPOT: self.lipid_depot,
            StorageTier.LIGNIN_ARCHIVE: self.lignin_archive
        }
        return tier_map.get(tier, deque())
    
    # ========================================================================
    # Core Storage Methods
    # ========================================================================
    
    def store_task(
        self, task_data: Dict[str, Any], ecoatp_cost: float,
        guarantee: GuaranteeLevel = GuaranteeLevel.SILVER,
        deadline: Optional[datetime] = None,
        initial_tier: StorageTier = StorageTier.GLYCOGEN_QUEUE
    ) -> Tuple[bool, Optional[str]]:
        """Store a task as biomass with token-backed guarantee"""
        task_id = task_data.get('task_id', f"stored_{uuid.uuid4().hex[:8]}")
        
        collateral_ratio = self.collateral_ratios[guarantee]
        collateral = ecoatp_cost * collateral_ratio
        
        stored = StoredTask(
            task_id=task_id, task_data=task_data,
            storage_tier=initial_tier, stored_at=datetime.utcnow(),
            original_ecoatp_cost=ecoatp_cost,
            deadline=deadline, priority=task_data.get('priority', 0)
        )
        
        token = StorageToken(
            token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
            task_id=task_id, original_value=ecoatp_cost,
            guarantee=guarantee, collateral_amount=collateral,
            storage_tier=initial_tier, stored_at=datetime.utcnow(),
            expires_at=deadline or (datetime.utcnow() + timedelta(days=7))
        )
        
        # Add to tier
        queue = self._get_tier_queue(initial_tier)
        position = len(queue)
        queue.append(stored)
        
        # Add to global index (FIX 8)
        self._add_to_index(task_id, initial_tier, position)
        
        self.storage_tokens[token.token_id] = token
        self.collateral_pool += collateral
        
        logger.info(f"Stored task {task_id} in {initial_tier.value}: cost={ecoatp_cost:.1f}")
        return True, token.token_id
    
    def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]:
        """Enhanced retrieval using global index - O(1)"""
        if token_id not in self.storage_tokens:
            return None, 0.0
        
        token = self.storage_tokens[token_id]
        task_id = token.task_id
        
        # Try index lookup first (FIX 8)
        location = self.find_task(task_id)
        
        if location:
            tier, position = location
            stored_task = self._get_from_tier_position(tier, position)
        else:
            # Fallback to scanning
            stored_task = self._scan_all_tiers(task_id)
        
        if stored_task is None:
            return None, 0.0
        
        retrieval_cost = stored_task.current_retrieval_cost
        
        # Remove from tier
        queue = self._get_tier_queue(stored_task.storage_tier)
        try:
            queue.remove(stored_task)
        except ValueError:
            pass
        
        # Remove from index
        self._remove_from_index(task_id)
        
        token.is_executed = True
        self.collateral_pool -= token.collateral_amount
        del self.storage_tokens[token_id]
        
        logger.info(f"Retrieved task {task_id}: cost={retrieval_cost:.1f}")
        return stored_task.task_data, retrieval_cost
    
    def convert_tier(self, token_id: str, target_tier: StorageTier) -> bool:
        """Convert stored task between tiers"""
        if token_id not in self.storage_tokens:
            return False
        
        token = self.storage_tokens[token_id]
        current_tier = token.storage_tier
        
        if current_tier == target_tier:
            return True
        
        # Find task using index
        location = self.find_task(token.task_id)
        if not location:
            return False
        
        tier, position = location
        stored_task = self._get_from_tier_position(tier, position)
        if stored_task is None:
            return False
        
        # Remove from current tier
        queue = self._get_tier_queue(current_tier)
        try:
            queue.remove(stored_task)
        except ValueError:
            pass
        
        # Calculate conversion cost
        conversion_cost = self.conversion_costs.get((current_tier, target_tier), 3.0)
        stored_task.current_retrieval_cost += conversion_cost
        stored_task.conversion_history.append({
            'from_tier': current_tier.value, 'to_tier': target_tier.value,
            'cost': conversion_cost, 'timestamp': datetime.utcnow().isoformat()
        })
        
        # Update tier
        stored_task.storage_tier = target_tier
        token.storage_tier = target_tier
        token.retrieval_cost = stored_task.current_retrieval_cost
        
        # Add to new tier
        new_queue = self._get_tier_queue(target_tier)
        new_position = len(new_queue)
        new_queue.append(stored_task)
        
        # Update index
        self._update_index_position(token.task_id, target_tier, new_position)
        
        logger.info(f"Converted {token.task_id}: {current_tier.value} → {target_tier.value} (cost={conversion_cost:.1f})")
        return True
    
    async def _maintenance_loop(self):
        """Check for expired tasks and optimize storage"""
        while True:
            try:
                now = datetime.utcnow()
                for token_id in list(self.storage_tokens.keys()):
                    token = self.storage_tokens[token_id]
                    if now > token.expires_at and not token.is_executed:
                        # Pay penalty
                        penalty = token.collateral_amount * 0.5
                        self.collateral_pool -= penalty
                        token.penalty_paid = True
                        
                        # Remove from storage
                        location = self.find_task(token.task_id)
                        if location:
                            tier, position = location
                            stored = self._get_from_tier_position(tier, position)
                            if stored:
                                queue = self._get_tier_queue(tier)
                                try:
                                    queue.remove(stored)
                                except ValueError:
                                    pass
                            self._remove_from_index(token.task_id)
                        
                        del self.storage_tokens[token_id]
                
                # Auto-convert old tasks
                for stored in list(self.glycogen_queue):
                    if stored.age_hours > 6:
                        token = self._find_token(stored.task_id)
                        if token:
                            self.convert_tier(token.token_id, StorageTier.STARCH_RESERVE)
                
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Biomass maintenance error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _mobilization_loop(self):
        """Mobilize stored tasks when conditions are favorable"""
        while True:
            try:
                for _ in range(min(5, len(self.glycogen_queue))):
                    if self.glycogen_queue:
                        task = self.glycogen_queue.popleft()
                        self._remove_from_index(task.task_id)
                        self.atp_cache.append(task)
                        self._add_to_index(task.task_id, StorageTier.ATP_CACHE, len(self.atp_cache) - 1)
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Mobilization error: {str(e)}")
                await asyncio.sleep(30)
    
    def _find_token(self, task_id: str) -> Optional[StorageToken]:
        """Find storage token for task"""
        for token in self.storage_tokens.values():
            if token.task_id == task_id:
                return token
        return None
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics including index performance"""
        return {
            'tiers': {
                'atp_cache': len(self.atp_cache),
                'glycogen_queue': len(self.glycogen_queue),
                'starch_reserve': len(self.starch_reserve),
                'lipid_depot': len(self.lipid_depot),
                'lignin_archive': len(self.lignin_archive)
            },
            'total_stored': sum([
                len(self.atp_cache), len(self.glycogen_queue),
                len(self.starch_reserve), len(self.lipid_depot),
                len(self.lignin_archive)
            ]),
            'active_tokens': len(self.storage_tokens),
            'collateral_pool': self.collateral_pool,
            'index_stats': self.get_index_stats()
        }
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get index performance statistics"""
        total = self.index_hits + self.index_misses
        return {
            'total_indexed': len(self.task_index),
            'hits': self.index_hits,
            'misses': self.index_misses,
            'hit_rate': self.index_hits / max(total, 1) if total > 0 else 0.0,
            'avg_access_count': np.mean([e['access_count'] for e in self.task_index.values()]) if self.task_index else 0
        }
