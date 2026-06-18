# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/biomass_storage.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Biomass Storage v5.0.0
Complete implementation with task deduplication, demand-based mobilization,
storage forecasting, priority-based retrieval, and storage analytics.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque, defaultdict
import uuid
import math
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing dependencies
# ============================================================================
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Enums and Data Classes
# ============================================================================

class StorageTier(Enum):
    """Storage tiers from fastest to slowest access"""
    ATP_CACHE = "atp_cache"              # Seconds-minutes, nearly free access
    GLYCOGEN_QUEUE = "glycogen_queue"    # Minutes-hours, low cost
    STARCH_RESERVE = "starch_reserve"    # Hours-days, moderate cost
    LIPID_DEPOT = "lipid_depot"          # Days-weeks, high cost
    LIGNIN_ARCHIVE = "lignin_archive"    # Months+, very high cost

class GuaranteeLevel(Enum):
    """Token-backed execution guarantees"""
    PLATINUM = "platinum"      # 200% collateral, 99.99% certainty
    GOLD = "gold"             # 150% collateral, 99.9% certainty
    SILVER = "silver"         # 120% collateral, 99% certainty
    BRONZE = "bronze"         # 100% collateral, 95% certainty
    BEST_EFFORT = "best_effort"  # 50% collateral, variable

class MobilizationTrigger(Enum):
    """Triggers for task mobilization"""
    CARBON_LOW = "carbon_low"
    ENERGY_ABUNDANT = "energy_abundant"
    DEADLINE_URGENT = "deadline_urgent"
    COMPARTMENT_AVAILABLE = "compartment_available"
    QUEUE_EMPTY = "queue_empty"
    MANUAL = "manual"

@dataclass
class StoredTask:
    """Enhanced stored task with deduplication and merging support"""
    task_id: str
    task_data: Dict[str, Any]
    task_hash: str = ""                    # Content hash for deduplication
    storage_tier: StorageTier = StorageTier.GLYCOGEN_QUEUE
    stored_at: datetime = field(default_factory=datetime.utcnow)
    original_ecoatp_cost: float = 0.0
    current_retrieval_cost: float = 0.0
    deadline: Optional[datetime] = None
    priority: int = 0
    execution_count: int = 0
    conversion_history: List[Dict] = field(default_factory=list)
    
    # Deduplication support
    reference_count: int = 1               # Number of identical tasks
    is_merged: bool = False                # Whether this is a merged task
    merged_task_ids: List[str] = field(default_factory=list)  # Original task IDs
    original_complexities: List[float] = field(default_factory=list)
    
    # Analytics
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    
    def __post_init__(self):
        if not self.task_hash:
            self.task_hash = self._compute_hash()
    
    def _compute_hash(self) -> str:
        """Compute content hash for deduplication"""
        task_str = json.dumps(self.task_data, sort_keys=True, default=str)
        return hashlib.sha256(task_str.encode()).hexdigest()
    
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
        """Calculate urgency score (0-1, higher = more urgent)"""
        if not self.deadline:
            return 0.3
        remaining = (self.deadline - datetime.utcnow()).total_seconds()
        total = (self.deadline - self.stored_at).total_seconds()
        if total <= 0:
            return 1.0
        return max(0.0, 1.0 - (remaining / total))
    
    @property
    def retrieval_priority_score(self) -> float:
        """Composite score for retrieval ordering"""
        return (
            self.priority / 5.0 * 0.3 +
            self.urgency * 0.4 +
            (1.0 - self.current_retrieval_cost / max(self.original_ecoatp_cost, 1)) * 0.3
        )

@dataclass
class StorageToken:
    """Token representing stored computation"""
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
    is_duplicate: bool = False         # Whether this references a deduplicated task

@dataclass
class StorageForecast:
    """Storage capacity forecast"""
    tier: StorageTier
    current_usage: int
    capacity: int
    inflow_rate: float                 # Tasks per second
    outflow_rate: float                # Tasks per second
    predicted_full_time: Optional[datetime]
    confidence: float

@dataclass
class StorageAnalytics:
    """Comprehensive storage analytics"""
    timestamp: datetime
    total_stored: int
    deduplication_savings: int
    merge_savings: int
    avg_retrieval_cost: float
    tier_distribution: Dict[str, int]
    conversion_efficiency: float
    expiration_rate: float
    mobilization_rate: float
    cache_hit_rate: float

# ============================================================================
# Enhanced Biomass Storage
# ============================================================================

class BiomassStorage:
    """
    Enhanced Biomass Storage v5.0.0
    
    Complete implementation with:
    - Task deduplication and merging
    - Demand-based mobilization
    - Storage forecasting
    - Priority-based retrieval
    - Storage analytics
    """
    
    def __init__(self, token_manager=None, gradient_manager=None):
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        
        # Storage tiers with dynamic capacity
        self.tier_capacities = {
            StorageTier.ATP_CACHE: 100,
            StorageTier.GLYCOGEN_QUEUE: 1000,
            StorageTier.STARCH_RESERVE: 5000,
            StorageTier.LIPID_DEPOT: 10000,
            StorageTier.LIGNIN_ARCHIVE: 50000
        }
        
        # Storage queues
        self.atp_cache: deque = deque(maxlen=self.tier_capacities[StorageTier.ATP_CACHE])
        self.glycogen_queue: deque = deque(maxlen=self.tier_capacities[StorageTier.GLYCOGEN_QUEUE])
        self.starch_reserve: deque = deque(maxlen=self.tier_capacities[StorageTier.STARCH_RESERVE])
        self.lipid_depot: deque = deque(maxlen=self.tier_capacities[StorageTier.LIPID_DEPOT])
        self.lignin_archive: deque = deque(maxlen=self.tier_capacities[StorageTier.LIGNIN_ARCHIVE])
        
        # Storage tokens
        self.storage_tokens: Dict[str, StorageToken] = {}
        self.collateral_pool: float = 0.0
        
        # Global task index for O(1) lookup
        self.task_index: Dict[str, Dict[str, Any]] = {}
        self.index_hits: int = 0
        self.index_misses: int = 0
        
        # ================================================================
        # NEW: Deduplication support
        # ================================================================
        self.task_hash_index: Dict[str, str] = {}  # hash → task_id
        self.deduplication_savings: int = 0
        self.merge_savings: int = 0
        
        # ================================================================
        # NEW: Demand-based mobilization
        # ================================================================
        self.mobilization_triggers: Dict[MobilizationTrigger, bool] = {
            MobilizationTrigger.CARBON_LOW: True,
            MobilizationTrigger.ENERGY_ABUNDANT: True,
            MobilizationTrigger.DEADLINE_URGENT: True,
            MobilizationTrigger.COMPARTMENT_AVAILABLE: True,
            MobilizationTrigger.QUEUE_EMPTY: True
        }
        self.mobilization_history: deque = deque(maxlen=500)
        self.total_mobilized: int = 0
        
        # ================================================================
        # NEW: Storage forecasting
        # ================================================================
        self.inflow_history: deque = deque(maxlen=100)
        self.outflow_history: deque = deque(maxlen=100)
        self.forecast_history: deque = deque(maxlen=50)
        
        # ================================================================
        # NEW: Storage analytics
        # ================================================================
        self.analytics_history: deque = deque(maxlen=1000)
        self.analytics_interval = 300  # Generate analytics every 5 minutes
        
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
        
        # Collateral ratios
        self.collateral_ratios = {
            GuaranteeLevel.PLATINUM: 2.0,
            GuaranteeLevel.GOLD: 1.5,
            GuaranteeLevel.SILVER: 1.2,
            GuaranteeLevel.BRONZE: 1.0,
            GuaranteeLevel.BEST_EFFORT: 0.5
        }
        
        # Start background tasks
        asyncio.create_task(self._maintenance_loop())
        asyncio.create_task(self._mobilization_loop())
        asyncio.create_task(self._forecasting_loop())
        asyncio.create_task(self._analytics_loop())
        
        logger.info("Enhanced Biomass Storage v5.0.0 initialized with all features")
    
    # ========================================================================
    # Core Storage Methods
    # ========================================================================
    
    def store_task(
        self, task_data: Dict[str, Any], ecoatp_cost: float,
        guarantee: GuaranteeLevel = GuaranteeLevel.SILVER,
        deadline: Optional[datetime] = None,
        initial_tier: StorageTier = StorageTier.GLYCOGEN_QUEUE,
        enable_dedup: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """
        Store a task with deduplication support.
        
        Returns (success, storage_token_id).
        """
        task_id = task_data.get('task_id', f"stored_{uuid.uuid4().hex[:8]}")
        
        # ================================================================
        # NEW: Task deduplication
        # ================================================================
        if enable_dedup:
            task_hash = hashlib.sha256(
                json.dumps(task_data, sort_keys=True, default=str).encode()
            ).hexdigest()
            
            # Check if identical task already exists
            if task_hash in self.task_hash_index:
                existing_task_id = self.task_hash_index[task_hash]
                existing = self._find_task_by_id(existing_task_id)
                
                if existing:
                    # Increment reference count instead of storing duplicate
                    existing.reference_count += 1
                    self.deduplication_savings += 1
                    
                    # Create a lightweight reference token
                    token = StorageToken(
                        token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
                        task_id=existing_task_id,
                        original_value=ecoatp_cost,
                        guarantee=guarantee,
                        collateral_amount=ecoatp_cost * self.collateral_ratios[guarantee],
                        storage_tier=existing.storage_tier,
                        stored_at=datetime.utcnow(),
                        expires_at=deadline or (datetime.utcnow() + timedelta(days=7)),
                        is_duplicate=True
                    )
                    
                    self.storage_tokens[token.token_id] = token
                    self.collateral_pool += token.collateral_amount
                    
                    logger.debug(f"Deduplicated task {task_id} → {existing_task_id} (refs: {existing.reference_count})")
                    return True, token.token_id
            
            # Check for mergeable tasks
            merged = self._try_merge_task(task_data, task_id, task_hash)
            if merged:
                return True, merged
        
        # Calculate collateral
        collateral_ratio = self.collateral_ratios[guarantee]
        collateral = ecoatp_cost * collateral_ratio
        
        # Create stored task
        stored = StoredTask(
            task_id=task_id,
            task_data=task_data,
            task_hash=task_hash if enable_dedup else "",
            storage_tier=initial_tier,
            stored_at=datetime.utcnow(),
            original_ecoatp_cost=ecoatp_cost,
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
            expires_at=deadline or (datetime.utcnow() + timedelta(days=7))
        )
        
        # Add to tier
        queue = self._get_tier_queue(initial_tier)
        position = len(queue)
        queue.append(stored)
        
        # Add to indexes
        self._add_to_index(task_id, initial_tier, position)
        if enable_dedup and task_hash:
            self.task_hash_index[task_hash] = task_id
        
        self.storage_tokens[token.token_id] = token
        self.collateral_pool += collateral
        
        # Track inflow
        self.inflow_history.append(datetime.utcnow())
        
        logger.info(f"Stored task {task_id} in {initial_tier.value}: cost={ecoatp_cost:.1f}")
        return True, token.token_id
    
    def _try_merge_task(self, task_data: Dict[str, Any], task_id: str, 
                       task_hash: str) -> Optional[str]:
        """
        Try to merge similar tasks for batch execution.
        
        Returns token_id if merged, None otherwise.
        """
        # Look for similar tasks in the same tier
        task_type = task_data.get('task_type', '')
        complexity = task_data.get('complexity', 0.5)
        
        for existing_id, index_entry in list(self.task_index.items())[:20]:
            existing = self._find_task_by_id(existing_id)
            if not existing:
                continue
            
            # Check if mergeable (same type, similar complexity)
            existing_type = existing.task_data.get('task_type', '')
            existing_complexity = existing.task_data.get('complexity', 0.5)
            
            if (existing_type == task_type and 
                abs(existing_complexity - complexity) < 0.2 and
                not existing.is_merged and
                len(existing.merged_task_ids) < 10):
                
                # Merge into existing task
                if not existing.is_merged:
                    existing.is_merged = True
                    existing.merged_task_ids = [existing.task_id]
                    existing.original_complexities = [existing_complexity]
                
                existing.merged_task_ids.append(task_id)
                existing.original_complexities.append(complexity)
                
                # Update complexity to combined value
                existing.task_data['complexity'] = min(1.0, sum(existing.original_complexities) * 0.7)
                existing.task_data['batch_execution'] = True
                existing.task_data['batch_size'] = len(existing.merged_task_ids)
                
                self.merge_savings += 1
                
                # Create reference token
                token = StorageToken(
                    token_id=f"stoken_{task_id}_{uuid.uuid4().hex[:6]}",
                    task_id=existing_id,
                    original_value=0,  # No additional cost
                    guarantee=GuaranteeLevel.BEST_EFFORT,
                    collateral_amount=0,
                    storage_tier=existing.storage_tier,
                    stored_at=datetime.utcnow(),
                    expires_at=existing.deadline or (datetime.utcnow() + timedelta(days=7)),
                    is_duplicate=True
                )
                
                self.storage_tokens[token.token_id] = token
                
                logger.debug(f"Merged task {task_id} into {existing_id} (batch: {len(existing.merged_task_ids)})")
                return token.token_id
        
        return None
    
    def retrieve_task(self, token_id: str, force_retrieve: bool = False) -> Tuple[Optional[Dict[str, Any]], float]:
        """
        Enhanced retrieval with priority ordering.
        
        Returns (task_data, retrieval_cost).
        """
        if token_id not in self.storage_tokens:
            return None, 0.0
        
        token = self.storage_tokens[token_id]
        
        # Handle duplicate references
        if token.is_duplicate:
            existing = self._find_task_by_id(token.task_id)
            if existing:
                existing.reference_count = max(0, existing.reference_count - 1)
            token.is_executed = True
            del self.storage_tokens[token_id]
            return existing.task_data if existing else None, 0.0
        
        task_id = token.task_id
        
        # Find task using index
        location = self.find_task(task_id)
        
        if location:
            tier, position = location
            stored_task = self._get_from_tier_position(tier, position)
        else:
            stored_task = self._scan_all_tiers(task_id)
        
        if stored_task is None:
            return None, 0.0
        
        retrieval_cost = stored_task.current_retrieval_cost
        
        # Update access tracking
        stored_task.access_count += 1
        stored_task.last_accessed = datetime.utcnow()
        
        # Remove from tier
        queue = self._get_tier_queue(stored_task.storage_tier)
        try:
            queue.remove(stored_task)
        except ValueError:
            pass
        
        # Clean up indexes
        self._remove_from_index(task_id)
        if stored_task.task_hash:
            self.task_hash_index.pop(stored_task.task_hash, None)
        
        # Handle merged tasks
        if stored_task.is_merged and stored_task.merged_task_ids:
            stored_task.task_data['merged_tasks'] = stored_task.merged_task_ids
            stored_task.task_data['total_original_tasks'] = len(stored_task.merged_task_ids)
        
        token.is_executed = True
        self.collateral_pool -= token.collateral_amount
        del self.storage_tokens[token_id]
        
        # Track outflow
        self.outflow_history.append(datetime.utcnow())
        
        logger.info(f"Retrieved task {task_id}: cost={retrieval_cost:.1f}, refs={stored_task.reference_count}")
        return stored_task.task_data, retrieval_cost
    
    def retrieve_highest_priority(self) -> Tuple[Optional[Dict[str, Any]], float, Optional[str]]:
        """
        NEW: Retrieve the highest priority task across all tiers.
        
        Returns (task_data, retrieval_cost, token_id).
        """
        best_task = None
        best_score = -1
        best_token_id = None
        
        for token_id, token in self.storage_tokens.items():
            if token.is_executed or token.is_duplicate:
                continue
            
            location = self.find_task(token.task_id)
            if not location:
                continue
            
            tier, position = location
            task = self._get_from_tier_position(tier, position)
            if not task:
                continue
            
            score = task.retrieval_priority_score
            
            if score > best_score:
                best_score = score
                best_task = task
                best_token_id = token_id
        
        if best_task and best_token_id:
            return self.retrieve_task(best_token_id)
        
        return None, 0.0, None
    
    # ========================================================================
    # Demand-Based Mobilization (NEW)
    # ========================================================================
    
    def should_mobilize(self) -> List[MobilizationTrigger]:
        """
        Check multiple signals to determine if tasks should be mobilized.
        
        Returns list of active mobilization triggers.
        """
        triggers = []
        
        # Check carbon gradient
        if (self.gradient_manager and 
            self.mobilization_triggers[MobilizationTrigger.CARBON_LOW]):
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.effective_strength < 0.3:
                triggers.append(MobilizationTrigger.CARBON_LOW)
        
        # Check if queues are empty (need more work)
        if self.mobilization_triggers[MobilizationTrigger.QUEUE_EMPTY]:
            if len(self.atp_cache) < 20:
                triggers.append(MobilizationTrigger.QUEUE_EMPTY)
        
        # Check for urgent deadlines
        if self.mobilization_triggers[MobilizationTrigger.DEADLINE_URGENT]:
            now = datetime.utcnow()
            for task in list(self.glycogen_queue)[:50]:
                if task.deadline and (task.deadline - now).total_seconds() < 3600:
                    triggers.append(MobilizationTrigger.DEADLINE_URGENT)
                    break
        
        return triggers
    
    def mobilize_tasks(self, target_tier: StorageTier = StorageTier.ATP_CACHE, 
                      max_count: int = 10) -> int:
        """
        Mobilize tasks from slower tiers to faster tiers based on demand signals.
        
        Returns number of tasks mobilized.
        """
        triggers = self.should_mobilize()
        
        if not triggers:
            return 0
        
        mobilized = 0
        
        # Mobilize from glycogen to ATP (fastest path)
        if target_tier == StorageTier.ATP_CACHE:
            source_queue = self.glycogen_queue
            
            # Prioritize urgent tasks
            urgent_tasks = []
            normal_tasks = []
            
            for task in list(source_queue)[:100]:
                if task.urgency > 0.7:
                    urgent_tasks.append(task)
                else:
                    normal_tasks.append(task)
            
            # Mobilize urgent first, then normal
            for task in urgent_tasks[:max_count]:
                if len(self.atp_cache) < self.tier_capacities[StorageTier.ATP_CACHE]:
                    source_queue.remove(task)
                    task.storage_tier = StorageTier.ATP_CACHE
                    self.atp_cache.append(task)
                    self._update_index_position(task.task_id, StorageTier.ATP_CACHE, len(self.atp_cache) - 1)
                    mobilized += 1
            
            remaining = max_count - mobilized
            for task in normal_tasks[:remaining]:
                if len(self.atp_cache) < self.tier_capacities[StorageTier.ATP_CACHE]:
                    source_queue.remove(task)
                    task.storage_tier = StorageTier.ATP_CACHE
                    self.atp_cache.append(task)
                    self._update_index_position(task.task_id, StorageTier.ATP_CACHE, len(self.atp_cache) - 1)
                    mobilized += 1
        
        if mobilized > 0:
            self.total_mobilized += mobilized
            self.mobilization_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'count': mobilized,
                'triggers': [t.value for t in triggers],
                'target_tier': target_tier.value
            })
            
            logger.info(f"Mobilized {mobilized} tasks to {target_tier.value} (triggers: {[t.value for t in triggers]})")
        
        return mobilized
    
    # ========================================================================
    # Storage Forecasting (NEW)
    # ========================================================================
    
    def forecast_storage(self, tier: StorageTier, horizon_seconds: float = 3600) -> StorageForecast:
        """
        Predict when a storage tier will reach capacity.
        
        Returns StorageForecast with predicted full time.
        """
        queue = self._get_tier_queue(tier)
        current_usage = len(queue)
        capacity = self.tier_capacities.get(tier, 1000)
        
        # Calculate inflow rate
        recent_inflow = [t for t in self.inflow_history 
                        if (datetime.utcnow() - t).total_seconds() < 3600]
        inflow_rate = len(recent_inflow) / 3600.0 if recent_inflow else 0.0
        
        # Calculate outflow rate
        recent_outflow = [t for t in self.outflow_history 
                         if (datetime.utcnow() - t).total_seconds() < 3600]
        outflow_rate = len(recent_outflow) / 3600.0 if recent_outflow else 0.0
        
        # Calculate net fill rate
        net_rate = inflow_rate - outflow_rate
        
        # Predict time to full
        if net_rate <= 0 or capacity <= current_usage:
            predicted_full_time = None  # Won't fill or already full
            confidence = 0.9
        else:
            remaining = capacity - current_usage
            seconds_to_full = remaining / net_rate
            predicted_full_time = datetime.utcnow() + timedelta(seconds=seconds_to_full)
            
            # Confidence based on data quantity
            confidence = min(0.9, len(recent_inflow) / 100)
        
        forecast = StorageForecast(
            tier=tier,
            current_usage=current_usage,
            capacity=capacity,
            inflow_rate=inflow_rate,
            outflow_rate=outflow_rate,
            predicted_full_time=predicted_full_time,
            confidence=confidence
        )
        
        self.forecast_history.append(forecast)
        
        return forecast
    
    def get_capacity_warnings(self) -> List[str]:
        """Get capacity warnings for all tiers"""
        warnings = []
        
        for tier in StorageTier:
            forecast = self.forecast_storage(tier, horizon_seconds=3600)
            
            utilization = forecast.current_usage / max(forecast.capacity, 1)
            
            if utilization > 0.9:
                warnings.append(f"CRITICAL: {tier.value} at {utilization:.0%} capacity")
            elif utilization > 0.7:
                warnings.append(f"WARNING: {tier.value} at {utilization:.0%} capacity")
            
            if forecast.predicted_full_time:
                time_to_full = (forecast.predicted_full_time - datetime.utcnow()).total_seconds()
                if time_to_full < 1800:  # Less than 30 minutes
                    warnings.append(
                        f"URGENT: {tier.value} predicted full in {time_to_full:.0f}s"
                    )
        
        return warnings
    
    # ========================================================================
    # Storage Analytics (NEW)
    # ========================================================================
    
    def generate_analytics(self) -> StorageAnalytics:
        """Generate comprehensive storage analytics"""
        total_stored = sum(len(self._get_tier_queue(t)) for t in StorageTier)
        
        tier_distribution = {
            tier.value: len(self._get_tier_queue(tier))
            for tier in StorageTier
        }
        
        # Calculate average retrieval cost
        active_tokens = [t for t in self.storage_tokens.values() if not t.is_executed]
        avg_cost = np.mean([t.retrieval_cost for t in active_tokens]) if active_tokens else 0.0
        
        # Calculate conversion efficiency
        total_conversions = sum(
            len(task.conversion_history)
            for tier in StorageTier
            for task in self._get_tier_queue(tier)
        )
        successful_retrievals = sum(
            1 for t in self.storage_tokens.values() if t.is_executed and not t.penalty_paid
        )
        conversion_efficiency = successful_retrievals / max(total_conversions, 1)
        
        # Calculate expiration rate
        total_tokens = max(len(self.storage_tokens), 1)
        expired = sum(1 for t in self.storage_tokens.values() if t.penalty_paid)
        expiration_rate = expired / total_tokens
        
        # Calculate mobilization rate
        mobilization_rate = self.total_mobilized / max(total_tokens, 1)
        
        # Calculate cache hit rate
        total_lookups = self.index_hits + self.index_misses
        cache_hit_rate = self.index_hits / max(total_lookups, 1)
        
        analytics = StorageAnalytics(
            timestamp=datetime.utcnow(),
            total_stored=total_stored,
            deduplication_savings=self.deduplication_savings,
            merge_savings=self.merge_savings,
            avg_retrieval_cost=avg_cost,
            tier_distribution=tier_distribution,
            conversion_efficiency=conversion_efficiency,
            expiration_rate=expiration_rate,
            mobilization_rate=mobilization_rate,
            cache_hit_rate=cache_hit_rate
        )
        
        self.analytics_history.append(analytics)
        
        return analytics
    
    def get_optimization_recommendations(self) -> List[str]:
        """Generate storage optimization recommendations"""
        recommendations = []
        analytics = self.generate_analytics()
        
        # Check utilization
        for tier, count in analytics.tier_distribution.items():
            tier_enum = StorageTier(tier)
            capacity = self.tier_capacities.get(tier_enum, 1000)
            utilization = count / max(capacity, 1)
            
            if utilization > 0.8:
                recommendations.append(
                    f"Increase {tier} capacity or accelerate conversion to slower tier"
                )
        
        # Check deduplication savings
        if self.deduplication_savings > 0:
            savings_pct = self.deduplication_savings / max(analytics.total_stored, 1) * 100
            recommendations.append(
                f"Deduplication saved {self.deduplication_savings} slots ({savings_pct:.1f}%)"
            )
        
        # Check conversion efficiency
        if analytics.conversion_efficiency < 0.5:
            recommendations.append(
                "Low conversion efficiency. Review tier migration schedule."
            )
        
        # Check expiration rate
        if analytics.expiration_rate > 0.1:
            recommendations.append(
                f"High expiration rate ({analytics.expiration_rate:.1%}). "
                "Consider reducing guarantee levels or extending deadlines."
            )
        
        if not recommendations:
            recommendations.append("Storage operating optimally. No changes needed.")
        
        return recommendations
    
    # ========================================================================
    # Index Methods
    # ========================================================================
    
    def _add_to_index(self, task_id: str, tier: StorageTier, position: int):
        """Add task to global index"""
        self.task_index[task_id] = {
            'tier': tier,
            'position': position,
            'stored_at': datetime.utcnow(),
            'access_count': 0,
            'last_accessed': None
        }
    
    def _update_index_position(self, task_id: str, new_tier: StorageTier, new_position: int):
        """Update task position after tier conversion"""
        if task_id in self.task_index:
            self.task_index[task_id]['tier'] = new_tier
            self.task_index[task_id]['position'] = new_position
            self.task_index[task_id]['stored_at'] = datetime.utcnow()
    
    def _remove_from_index(self, task_id: str):
        """Remove task from index"""
        self.task_index.pop(task_id, None)
    
    def find_task(self, task_id: str) -> Optional[Tuple[StorageTier, int]]:
        """Find task using global index - O(1)"""
        if task_id in self.task_index:
            self.index_hits += 1
            entry = self.task_index[task_id]
            entry['access_count'] += 1
            entry['last_accessed'] = datetime.utcnow()
            return entry['tier'], entry['position']
        self.index_misses += 1
        return None
    
    def _find_task_by_id(self, task_id: str) -> Optional[StoredTask]:
        """Find task object by ID across all tiers"""
        location = self.find_task(task_id)
        if location:
            return self._get_from_tier_position(location[0], location[1])
        return self._scan_all_tiers(task_id)
    
    def _get_from_tier_position(self, tier: StorageTier, position: int) -> Optional[StoredTask]:
        """Get task from specific tier position"""
        queue = self._get_tier_queue(tier)
        if position < len(queue):
            return queue[position]
        return None
    
    def _scan_all_tiers(self, task_id: str) -> Optional[StoredTask]:
        """Fallback: scan all tiers"""
        for tier in StorageTier:
            queue = self._get_tier_queue(tier)
            for i, task in enumerate(queue):
                if task.task_id == task_id:
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
    # Tier Conversion
    # ========================================================================
    
    def convert_tier(self, token_id: str, target_tier: StorageTier) -> bool:
        """Convert stored task between tiers"""
        if token_id not in self.storage_tokens:
            return False
        
        token = self.storage_tokens[token_id]
        
        if token.is_duplicate:
            return False
        
        current_tier = token.storage_tier
        if current_tier == target_tier:
            return True
        
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
        new_queue = self._get_tier_queue(target_tier)
        new_position = len(new_queue)
        new_queue.append(stored_task)
        
        self._update_index_position(token.task_id, target_tier, new_position)
        
        logger.info(f"Converted {token.task_id}: {current_tier.value} → {target_tier.value} (cost={conversion_cost:.1f})")
        return True
    
    # ========================================================================
    # Background Loops
    # ========================================================================
    
    async def _maintenance_loop(self):
        """Enhanced maintenance with capacity warnings"""
        while True:
            try:
                now = datetime.utcnow()
                
                # Handle expired tasks
                for token_id in list(self.storage_tokens.keys()):
                    token = self.storage_tokens[token_id]
                    if now > token.expires_at and not token.is_executed:
                        penalty = token.collateral_amount * 0.5
                        self.collateral_pool -= penalty
                        token.penalty_paid = True
                        
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
                
                # Check capacity warnings
                warnings = self.get_capacity_warnings()
                for warning in warnings:
                    if warning.startswith("CRITICAL"):
                        logger.warning(warning)
                
                # Auto-convert old tasks
                for stored in list(self.glycogen_queue):
                    if stored.age_hours > 6:
                        token = self._find_token(stored.task_id)
                        if token:
                            self.convert_tier(token.token_id, StorageTier.STARCH_RESERVE)
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Maintenance error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _mobilization_loop(self):
        """Demand-based mobilization loop"""
        while True:
            try:
                self.mobilize_tasks(StorageTier.ATP_CACHE, max_count=10)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Mobilization error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _forecasting_loop(self):
        """Storage forecasting loop"""
        while True:
            try:
                for tier in [StorageTier.GLYCOGEN_QUEUE, StorageTier.STARCH_RESERVE]:
                    self.forecast_storage(tier)
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Forecasting error: {str(e)}")
                await asyncio.sleep(600)
    
    async def _analytics_loop(self):
        """Storage analytics generation loop"""
        while True:
            try:
                self.generate_analytics()
                await asyncio.sleep(self.analytics_interval)
            except Exception as e:
                logger.error(f"Analytics error: {str(e)}")
                await asyncio.sleep(600)
    
    def _find_token(self, task_id: str) -> Optional[StorageToken]:
        """Find storage token for task"""
        for token in self.storage_tokens.values():
            if token.task_id == task_id and not token.is_duplicate:
                return token
        return None
    
    # ========================================================================
    # Statistics
    # ========================================================================
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics"""
        stats = {
            'tiers': {
                tier.value: len(self._get_tier_queue(tier))
                for tier in StorageTier
            },
            'total_stored': sum(len(self._get_tier_queue(t)) for t in StorageTier),
            'active_tokens': len([t for t in self.storage_tokens.values() if not t.is_executed]),
            'collateral_pool': self.collateral_pool,
            'index_stats': {
                'hits': self.index_hits,
                'misses': self.index_misses,
                'hit_rate': self.index_hits / max(self.index_hits + self.index_misses, 1)
            },
            'deduplication': {
                'savings': self.deduplication_savings,
                'merge_savings': self.merge_savings,
                'total_saved': self.deduplication_savings + self.merge_savings
            },
            'mobilization': {
                'total_mobilized': self.total_mobilized,
                'recent': list(self.mobilization_history)[-10:]
            },
            'capacity_warnings': self.get_capacity_warnings()
        }
        
        # Add latest forecast
        stats['forecast'] = {
            tier.value: {
                'current': self.forecast_storage(tier).current_usage,
                'capacity': self.forecast_storage(tier).capacity,
                'predicted_full': self.forecast_storage(tier).predicted_full_time.isoformat() 
                if self.forecast_storage(tier).predicted_full_time else None
            }
            for tier in [StorageTier.GLYCOGEN_QUEUE, StorageTier.STARCH_RESERVE]
        }
        
        # Add latest analytics
        if self.analytics_history:
            latest = self.analytics_history[-1]
            stats['analytics'] = {
                'deduplication_savings': latest.deduplication_savings,
                'merge_savings': latest.merge_savings,
                'avg_retrieval_cost': latest.avg_retrieval_cost,
                'conversion_efficiency': latest.conversion_efficiency,
                'expiration_rate': latest.expiration_rate,
                'mobilization_rate': latest.mobilization_rate,
                'cache_hit_rate': latest.cache_hit_rate
            }
        
        # Add recommendations
        stats['recommendations'] = self.get_optimization_recommendations()
        
        return stats
    
    def get_deduplication_report(self) -> Dict[str, Any]:
        """Get deduplication savings report"""
        total_stored = sum(len(self._get_tier_queue(t)) for t in StorageTier)
        total_saved = self.deduplication_savings + self.merge_savings
        
        return {
            'deduplication_savings': self.deduplication_savings,
            'merge_savings': self.merge_savings,
            'total_savings': total_saved,
            'savings_percentage': total_saved / max(total_stored + total_saved, 1) * 100,
            'hash_index_size': len(self.task_hash_index),
            'recommendation': (
                f"Deduplication and merging saved {total_saved} storage slots "
                f"({total_saved / max(total_stored + total_saved, 1) * 100:.1f}% reduction)"
            )
        }
    
    def get_mobilization_report(self) -> Dict[str, Any]:
        """Get mobilization activity report"""
        recent = list(self.mobilization_history)[-50:]
        
        if not recent:
            return {'status': 'No recent mobilization activity'}
        
        trigger_counts = defaultdict(int)
        for entry in recent:
            for trigger in entry.get('triggers', []):
                trigger_counts[trigger] += 1
        
        return {
            'total_mobilized': self.total_mobilized,
            'recent_activity': len(recent),
            'trigger_distribution': dict(trigger_counts),
            'most_common_trigger': max(trigger_counts, key=trigger_counts.get) if trigger_counts else 'none',
            'recommendation': (
                f"Most mobilizations triggered by: {max(trigger_counts, key=trigger_counts.get) if trigger_counts else 'none'}. "
                f"Total mobilized: {self.total_mobilized}"
            )
        }
