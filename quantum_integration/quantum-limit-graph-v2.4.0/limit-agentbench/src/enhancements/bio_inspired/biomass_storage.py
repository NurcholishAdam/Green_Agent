# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/biomass_storage.py

"""
Biomass Storage System for Green Agent
Version: 1.0.0

Deferred computation queuing with token-backed guarantees.
Inspired by biological energy storage systems (glycogen, starch, lipids).

Storage Tiers:
- ATP Cache: Immediate (seconds-minutes)
- Glycogen Queue: Short-term (minutes-hours)  
- Starch Reserve: Medium-term (hours-days)
- Lipid Depot: Long-term (days-weeks)
- Lignin Archive: Permanent (months+)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import hashlib
import math

logger = logging.getLogger(__name__)

class StorageTier(Enum):
    """Biomass storage tiers"""
    ATP_CACHE = "atp_cache"          # Seconds-minutes, nearly free access
    GLYCOGEN_QUEUE = "glycogen_queue"  # Minutes-hours, low cost
    STARCH_RESERVE = "starch_reserve"  # Hours-days, moderate cost
    LIPID_DEPOT = "lipid_depot"        # Days-weeks, high cost
    LIGNIN_ARCHIVE = "lignin_archive"  # Months+, very high cost

class GuaranteeLevel(Enum):
    """Token-backed execution guarantees"""
    PLATINUM = "platinum"    # 200% collateral, 99.99% certainty
    GOLD = "gold"           # 150% collateral, 99.9% certainty
    SILVER = "silver"       # 120% collateral, 99% certainty
    BRONZE = "bronze"       # 100% collateral, 95% certainty
    BEST_EFFORT = "best_effort"  # 50% collateral, variable

@dataclass
class StorageToken:
    """Token representing stored computation"""
    token_id: str
    task_id: str
    original_value: float  # Eco-ATP value
    guarantee: GuaranteeLevel
    collateral_amount: float
    storage_tier: StorageTier
    stored_at: datetime
    expires_at: datetime
    retrieval_cost: float
    is_executed: bool = False
    penalty_paid: bool = False

@dataclass
class StoredTask:
    """Task stored as biomass"""
    task_id: str
    task_data: Dict[str, Any]
    storage_tier: StorageTier
    stored_at: datetime
    original_ecoatp_cost: float
    current_retrieval_cost: float
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
        """Calculate urgency (0-1, higher = more urgent)"""
        if not self.deadline:
            return 0.3
        
        remaining = (self.deadline - datetime.utcnow()).total_seconds()
        total = (self.deadline - self.stored_at).total_seconds()
        
        if total <= 0:
            return 1.0
        
        return 1.0 - (remaining / total)

class BiomassStorage:
    """
    Multi-tier biomass storage system.
    
    Manages deferred computation with token-backed guarantees.
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
        
        # Conversion costs (Eco-ATP per task)
        self.conversion_costs = {
            (StorageTier.ATP_CACHE, StorageTier.GLYCOGEN_QUEUE): 0.5,
            (StorageTier.GLYCOGEN_QUEUE, StorageTier.STARCH_RESERVE): 2.0,
            (StorageTier.STARCH_RESERVE, StorageTier.LIPID_DEPOT): 5.0,
            (StorageTier.LIPID_DEPOT, StorageTier.LIGNIN_ARCHIVE): 10.0,
            # Reverse conversions (more expensive)
            (StorageTier.LIPID_DEPOT, StorageTier.STARCH_RESERVE): 8.0,
            (StorageTier.STARCH_RESERVE, StorageTier.GLYCOGEN_QUEUE): 4.0,
            (StorageTier.GLYCOGEN_QUEUE, StorageTier.ATP_CACHE): 2.0,
        }
        
        # Guarantee collateral ratios
        self.collateral_ratios = {
            GuaranteeLevel.PLATINUM: 2.0,
            GuaranteeLevel.GOLD: 1.5,
            GuaranteeLevel.SILVER: 1.2,
            GuaranteeLevel.BRONZE: 1.0,
            GuaranteeLevel.BEST_EFFORT: 0.5
        }
        
        # Start maintenance
        asyncio.create_task(self._maintenance_loop())
        asyncio.create_task(self._mobilization_loop())
        
        logger.info("Biomass Storage System initialized")
    
    def store_task(
        self,
        task_data: Dict[str, Any],
        ecoatp_cost: float,
        guarantee: GuaranteeLevel = GuaranteeLevel.SILVER,
        deadline: Optional[datetime] = None,
        initial_tier: StorageTier = StorageTier.GLYCOGEN_QUEUE
    ) -> Tuple[bool, Optional[str]]:
        """
        Store a task as biomass with token-backed guarantee.
        
        Returns (success, storage_token_id).
        """
        task_id = task_data.get('task_id', f"stored_{uuid.uuid4().hex[:8]}")
        
        # Calculate collateral
        collateral_ratio = self.collateral_ratios[guarantee]
        collateral = ecoatp_cost * collateral_ratio
        
        # Check if sufficient collateral available
        if self.token_manager:
            main_account = "green_agent_core"
            account = self.token_manager.get_account_summary(main_account)
            if account.get('balance', 0) < collateral:
                # Reduce guarantee level
                guarantee = GuaranteeLevel.BEST_EFFORT
                collateral = ecoatp_cost * 0.5
        
        # Create stored task
        stored = StoredTask(
            task_id=task_id,
            task_data=task_data,
            storage_tier=initial_tier,
            stored_at=datetime.utcnow(),
            original_ecoatp_cost=ecoatp_cost,
            current_retrieval_cost=0.0,  # Free to retrieve from initial tier
            deadline=deadline,
            priority=task_data.get('priority', 0)
        )
        
        # Create storage token
        token = StorageToken(
            token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
            task_id=task_id,
            original_value=ecoatp_cost,
            guarantee=guarantee,
            collateral_amount=collateral,
            storage_tier=initial_tier,
            stored_at=datetime.utcnow(),
            expires_at=deadline or (datetime.utcnow() + timedelta(days=7)),
            retrieval_cost=0.0
        )
        
        # Add to appropriate tier
        self._add_to_tier(initial_tier, stored)
        
        # Track token
        self.storage_tokens[token.token_id] = token
        self.collateral_pool += collateral
        
        logger.info(
            f"Stored task {task_id} in {initial_tier.value}: "
            f"cost={ecoatp_cost:.1f}, guarantee={guarantee.value}"
        )
        
        return True, token.token_id
    
    def _add_to_tier(self, tier: StorageTier, task: StoredTask):
        """Add task to storage tier"""
        if tier == StorageTier.ATP_CACHE:
            self.atp_cache.append(task)
        elif tier == StorageTier.GLYCOGEN_QUEUE:
            self.glycogen_queue.append(task)
        elif tier == StorageTier.STARCH_RESERVE:
            self.starch_reserve.append(task)
        elif tier == StorageTier.LIPID_DEPOT:
            self.lipid_depot.append(task)
        elif tier == StorageTier.LIGNIN_ARCHIVE:
            self.lignin_archive.append(task)
    
    def _remove_from_tier(self, tier: StorageTier, task_id: str) -> Optional[StoredTask]:
        """Remove task from storage tier"""
        tier_map = {
            StorageTier.ATP_CACHE: self.atp_cache,
            StorageTier.GLYCOGEN_QUEUE: self.glycogen_queue,
            StorageTier.STARCH_RESERVE: self.starch_reserve,
            StorageTier.LIPID_DEPOT: self.lipid_depot,
            StorageTier.LIGNIN_ARCHIVE: self.lignin_archive
        }
        
        queue = tier_map.get(tier)
        if queue is None:
            return None
        
        for i, task in enumerate(queue):
            if task.task_id == task_id:
                queue.remove(task)
                return task
        
        return None
    
    def retrieve_task(
        self,
        token_id: str,
        force_retrieve: bool = False
    ) -> Tuple[Optional[Dict[str, Any]], float]:
        """
        Retrieve and execute a stored task.
        
        Returns (task_data, retrieval_cost).
        """
        if token_id not in self.storage_tokens:
            return None, 0.0
        
        token = self.storage_tokens[token_id]
        task_id = token.task_id
        
        # Find task in storage
        stored_task = None
        for tier in StorageTier:
            stored_task = self._remove_from_tier(tier, task_id)
            if stored_task:
                break
        
        if stored_task is None:
            return None, 0.0
        
        # Calculate retrieval cost
        retrieval_cost = stored_task.current_retrieval_cost
        
        if force_retrieve or retrieval_cost <= 0:
            # Execute task
            token.is_executed = True
            
            # Release collateral back to pool
            self.collateral_pool -= token.collateral_amount
            
            # Clean up
            del self.storage_tokens[token_id]
            
            logger.info(f"Retrieved task {task_id}: cost={retrieval_cost:.1f}")
            
            return stored_task.task_data, retrieval_cost
        
        return None, 0.0
    
    def convert_tier(
        self,
        token_id: str,
        target_tier: StorageTier
    ) -> bool:
        """
        Convert stored task between tiers.
        
        Higher tiers = denser storage but higher retrieval cost.
        """
        if token_id not in self.storage_tokens:
            return False
        
        token = self.storage_tokens[token_id]
        current_tier = token.storage_tier
        
        if current_tier == target_tier:
            return True
        
        # Find task
        stored_task = None
        for tier in StorageTier:
            found = self._remove_from_tier(tier, token.task_id)
            if found:
                stored_task = found
                break
        
        if stored_task is None:
            return False
        
        # Calculate conversion cost
        conversion_cost = self.conversion_costs.get(
            (current_tier, target_tier),
            3.0  # Default cost
        )
        
        # Add to retrieval cost
        stored_task.current_retrieval_cost += conversion_cost
        
        # Record conversion
        stored_task.conversion_history.append({
            'from_tier': current_tier.value,
            'to_tier': target_tier.value,
            'cost': conversion_cost,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Update tier
        stored_task.storage_tier = target_tier
        token.storage_tier = target_tier
        token.retrieval_cost = stored_task.current_retrieval_cost
        
        # Add to new tier
        self._add_to_tier(target_tier, stored_task)
        
        logger.info(
            f"Converted {token.task_id}: {current_tier.value} → {target_tier.value} "
            f"(cost={conversion_cost:.1f})"
        )
        
        return True
    
    def execute_stored_task(
        self,
        token_id: str,
        execute_func: callable
    ) -> Tuple[bool, Any]:
        """Execute a stored task and return results"""
        task_data, cost = self.retrieve_task(token_id)
        
        if task_data is None:
            return False, None
        
        # Execute the task
        result = execute_func(task_data)
        
        return True, result
    
    def handle_expired_task(self, token_id: str):
        """Handle expired task - pay penalty from collateral"""
        if token_id not in self.storage_tokens:
            return
        
        token = self.storage_tokens[token_id]
        
        if token.is_executed:
            return
        
        # Pay penalty
        penalty = token.collateral_amount * 0.5  # 50% penalty
        self.collateral_pool -= penalty
        
        # Distribute penalty as Eco-ATP generation
        if self.token_manager:
            self.token_manager.generate_tokens(
                account_id="green_agent_core",
                source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=penalty / 10000.0
            )
        
        token.penalty_paid = True
        
        # Remove task from storage
        for tier in StorageTier:
            self._remove_from_tier(tier, token.task_id)
        
        logger.info(f"Expired task {token.task_id}: penalty={penalty:.1f}")
    
    async def _maintenance_loop(self):
        """Check for expired tasks and optimize storage"""
        while True:
            try:
                now = datetime.utcnow()
                
                # Check for expired tasks
                for token_id in list(self.storage_tokens.keys()):
                    token = self.storage_tokens[token_id]
                    if now > token.expires_at and not token.is_executed:
                        self.handle_expired_task(token_id)
                
                # Auto-convert old tasks to denser storage
                for stored in list(self.glycogen_queue):
                    if stored.age_hours > 6:
                        token = self._find_token(stored.task_id)
                        if token:
                            self.convert_tier(token.token_id, StorageTier.STARCH_RESERVE)
                
                for stored in list(self.starch_reserve):
                    if stored.age_hours > 72:
                        token = self._find_token(stored.task_id)
                        if token:
                            self.convert_tier(token.token_id, StorageTier.LIPID_DEPOT)
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Biomass maintenance error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _mobilization_loop(self):
        """Mobilize stored tasks when conditions are favorable"""
        while True:
            try:
                # Check if system has capacity
                system_load = 0.5  # Would come from actual monitoring
                
                if system_load < 0.4:
                    # Mobilize from glycogen (short-term)
                    mobilized = 0
                    for _ in range(min(5, len(self.glycogen_queue))):
                        if self.glycogen_queue:
                            task = self.glycogen_queue.popleft()
                            self.atp_cache.append(task)
                            mobilized += 1
                    
                    if mobilized > 0:
                        logger.debug(f"Mobilized {mobilized} tasks from glycogen")
                
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
        """Get storage statistics"""
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
            'expired_count': sum(1 for t in self.storage_tokens.values() if t.penalty_paid),
            'by_guarantee': {
                level.value: sum(1 for t in self.storage_tokens.values() if t.guarantee == level)
                for level in GuaranteeLevel
            }
        }
