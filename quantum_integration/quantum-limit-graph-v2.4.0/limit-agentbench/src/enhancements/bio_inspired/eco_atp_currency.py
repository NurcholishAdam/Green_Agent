# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/eco_atp_currency.py
# Enhanced with emergency bypass, DoS protection, batch processing, and tenant quotas

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import math

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class EcoATPSource(Enum):
    RENEWABLE_ENERGY = "renewable_energy"
    CARBON_OFFSET = "carbon_offset"
    EFFICIENCY_GAIN = "efficiency_gain"
    WASTE_HEAT_RECOVERY = "waste_heat_recovery"
    COMPUTATION_SCAVENGING = "computation_scavenging"
    HELIUM_RECOVERY = "helium_recovery"
    EXTERNAL_TRADE = "external_trade"
    GRADIENT_CONVERSION = "gradient_conversion"
    EMERGENCY_SUBSTRATE = "emergency_substrate"  # NEW: Anaerobic pathway

class EcoATPConsumer(Enum):
    EXPERT_EXECUTION = "expert_execution"
    MODEL_TRAINING = "model_training"
    DATA_PROCESSING = "data_processing"
    QUANTUM_COMPUTING = "quantum_computing"
    NETWORK_TRANSFER = "network_transfer"
    COOLING_SYSTEM = "cooling_system"
    STORAGE_OPERATION = "storage_operation"
    MAINTENANCE = "maintenance"

class TokenState(Enum):
    GENERATED = "generated"
    AVAILABLE = "available"
    RESERVED = "reserved"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    RECOVERED = "recovered"
    TRADED = "traded"

@dataclass
class EcoATPToken:
    token_id: str
    value: float
    source: EcoATPSource
    generated_at: datetime
    expires_at: datetime
    state: TokenState = TokenState.AVAILABLE
    carbon_equivalent_kg: float = 0.0
    helium_equivalent_units: float = 0.0
    generation_efficiency: float = 1.0
    provenance_hash: str = ""
    
    def __post_init__(self):
        if not self.provenance_hash:
            self.provenance_hash = self._compute_hash()
    
    def _compute_hash(self) -> str:
        data = f"{self.token_id}{self.value}{self.source.value}{self.generated_at.isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def apply_decay(self, current_time: datetime) -> float:
        age_hours = (current_time - self.generated_at).total_seconds() / 3600
        half_life = 24.0
        decay_factor = math.exp(-math.log(2) * age_hours / half_life)
        return self.value * decay_factor
    
    def is_expired(self, current_time: datetime) -> bool:
        return current_time > self.expires_at

@dataclass
class EcoATPAccount:
    account_id: str
    balance: float = 0.0
    total_generated: float = 0.0
    total_consumed: float = 0.0
    total_recovered: float = 0.0
    total_expired: float = 0.0
    generation_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    consumption_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    efficiency_rating: float = 1.0
    
    @property
    def net_balance(self) -> float:
        return self.balance
    
    @property
    def utilization_rate(self) -> float:
        if self.total_generated == 0:
            return 0.0
        return self.total_consumed / self.total_generated

# ============================================================================
# Dynamic Exchange Rate Engine
# ============================================================================

class DynamicExchangeRate:
    def __init__(self):
        self.carbon_weight = 0.6
        self.helium_weight = 0.4
        self.carbon_scarcity_multiplier = 1.0
        self.helium_scarcity_multiplier = 1.0
        self.rate_history: deque = deque(maxlen=1000)
        self.base_carbon_to_ecoatp = 1000.0
        self.base_helium_to_ecoatp = 500.0
    
    def update_scarcity(self, carbon_zone: int, helium_scarcity: float, grid_carbon_intensity: float):
        self.carbon_scarcity_multiplier = 1.0 + (carbon_zone / 15.0) * 2.0
        self.helium_scarcity_multiplier = 1.0 + helium_scarcity * 3.0
        total_scarcity = self.carbon_scarcity_multiplier + self.helium_scarcity_multiplier
        self.carbon_weight = self.carbon_scarcity_multiplier / total_scarcity
        self.helium_weight = self.helium_scarcity_multiplier / total_scarcity
        self.rate_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'carbon_weight': self.carbon_weight,
            'helium_weight': self.helium_weight,
            'carbon_multiplier': self.carbon_scarcity_multiplier,
            'helium_multiplier': self.helium_scarcity_multiplier
        })
    
    def carbon_to_ecoatp(self, carbon_kg: float) -> float:
        return carbon_kg * self.base_carbon_to_ecoatp * self.carbon_scarcity_multiplier
    
    def helium_to_ecoatp(self, helium_units: float) -> float:
        return helium_units * self.base_helium_to_ecoatp * self.helium_scarcity_multiplier
    
    def get_current_rates(self) -> Dict[str, Any]:
        return {
            'carbon_to_ecoatp': self.base_carbon_to_ecoatp * self.carbon_scarcity_multiplier,
            'helium_to_ecoatp': self.base_helium_to_ecoatp * self.helium_scarcity_multiplier,
            'carbon_weight': self.carbon_weight,
            'helium_weight': self.helium_weight,
            'carbon_scarcity': self.carbon_scarcity_multiplier,
            'helium_scarcity': self.helium_scarcity_multiplier
        }

# ============================================================================
# Enhanced Eco-ATP Token Manager with All Fixes
# ============================================================================

class EcoATPTokenManager:
    """
    Enhanced Eco-ATP Token Manager with:
    - Emergency anaerobic metabolism bypass
    - Per-tenant DoS protection and rate limiting
    - Batched token operations for reduced latency
    - Suspicious activity detection
    """
    
    def __init__(self, exchange_rate: Optional[DynamicExchangeRate] = None):
        self.exchange_rate = exchange_rate or DynamicExchangeRate()
        self.accounts: Dict[str, EcoATPAccount] = {}
        self.active_tokens: Dict[str, EcoATPToken] = {}
        self.token_history: deque = deque(maxlen=10000)
        
        # Anti-hoarding configuration
        self.hoarding_threshold = 2.0
        self.tax_rate = 0.1
        self.redistribution_interval = timedelta(minutes=30)
        self.last_redistribution = datetime.utcnow()
        
        # Recovery rates
        self.recovery_rates = {
            0.0: 0.0, 0.25: 0.125, 0.5: 0.25, 0.75: 0.6, 0.9: 0.8, 1.0: 0.95
        }
        
        # ====================================================================
        # FIX 1: Emergency Anaerobic Metabolism Bypass
        # ====================================================================
        self.emergency_mode = False
        self.emergency_token_rate = 10.0
        self.emergency_reserve = 1000.0
        self.emergency_threshold = 50.0
        self.substrate_phosphorylation_active = False
        self.substrate_reserves = 500.0
        self.last_generation_time: Optional[datetime] = None
        
        # ====================================================================
        # FIX 4: DoS Protection - Per-Tenant Quotas
        # ====================================================================
        self.tenant_quotas: Dict[str, Dict[str, Any]] = {}
        self.default_quota = {
            'max_tokens_per_minute': 100.0,
            'max_concurrent_tasks': 5,
            'min_priority_for_reservation': 2,
            'reservation_cooldown_seconds': 1.0
        }
        self.tenant_usage: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.tenant_last_reservation: Dict[str, datetime] = {}
        self.suspicious_tenants: Set[str] = set()
        self.suspicious_threshold = 5
        self._failed_attempts: Dict[str, int] = defaultdict(int)
        
        # ====================================================================
        # FIX 7: Batch Processing for Reduced Latency
        # ====================================================================
        self.batch_queue: List[Dict[str, Any]] = []
        self.batch_size = 10
        self.batch_timeout = 0.005
        self._batch_lock = asyncio.Lock()
        
        # Start background tasks
        asyncio.create_task(self._emergency_monitor_loop())
        asyncio.create_task(self._batch_processor_loop())
        asyncio.create_task(self._maintenance_loop())
        
        logger.info("Enhanced Eco-ATP Token Manager initialized with all fixes")
    
    # ========================================================================
    # FIX 1: Emergency Anaerobic Metabolism
    # ========================================================================
    
    async def _emergency_monitor_loop(self):
        """Monitor for ATP synthase failure and activate emergency mode"""
        while True:
            try:
                summary = self.get_system_summary()
                balance = summary.get('total_balance', 0)
                
                if self.last_generation_time:
                    time_since_last = (datetime.utcnow() - self.last_generation_time).total_seconds()
                    
                    if time_since_last > 30 and balance < self.emergency_threshold:
                        if not self.emergency_mode:
                            self._activate_emergency_mode()
                    elif self.emergency_mode and time_since_last < 10:
                        self._deactivate_emergency_mode()
                
                if self.emergency_mode:
                    self._generate_emergency_tokens()
                
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Emergency monitor error: {str(e)}")
                await asyncio.sleep(10)
    
    def _activate_emergency_mode(self):
        """Activate anaerobic metabolism bypass"""
        self.emergency_mode = True
        self.substrate_phosphorylation_active = True
        logger.critical(
            f"EMERGENCY MODE ACTIVATED: ATP Synthase bypass engaged. "
            f"Emergency reserve: {self.emergency_reserve:.0f} Eco-ATP, "
            f"Substrate reserves: {self.substrate_reserves:.0f}"
        )
    
    def _deactivate_emergency_mode(self):
        """Deactivate emergency mode when ATP synthase recovers"""
        self.emergency_mode = False
        self.substrate_phosphorylation_active = False
        logger.info("Emergency mode deactivated - ATP Synthase recovered")
    
    def _generate_emergency_tokens(self):
        """Substrate-level phosphorylation - direct ATP from stored substrates"""
        if self.substrate_reserves <= 0:
            logger.critical("EMERGENCY RESERVES EXHAUSTED!")
            return
        
        substrate_used = min(self.emergency_token_rate, self.substrate_reserves)
        self.substrate_reserves -= substrate_used
        emergency_tokens = substrate_used * 0.5
        
        critical_accounts = ['energy_expert', 'helium_expert', 'green_agent_core']
        per_account = emergency_tokens / len(critical_accounts)
        
        for account_id in critical_accounts:
            if account_id in self.accounts:
                self.accounts[account_id].balance += per_account
        
        self.last_generation_time = datetime.utcnow()
        logger.warning(
            f"EMERGENCY: Generated {emergency_tokens:.1f} Eco-ATP via substrate phosphorylation. "
            f"Reserves remaining: {self.substrate_reserves:.1f}"
        )
    
    def replenish_substrate_reserves(self, amount: float):
        """Replenish substrate reserves from normal operation surplus"""
        self.substrate_reserves = min(1000.0, self.substrate_reserves + amount)
    
    # ========================================================================
    # FIX 4: DoS Protection - Per-Tenant Quotas
    # ========================================================================
    
    def reserve_tokens(
        self, account_id: str, amount: float,
        consumer: EcoATPConsumer, tenant_id: str = "default",
        priority: int = 2
    ) -> Tuple[bool, List[str]]:
        """Enhanced token reservation with DoS protection"""
        tenant_quota = self.tenant_quotas.get(tenant_id, self.default_quota)
        
        if tenant_id in self.suspicious_tenants:
            logger.warning(f"Suspicious tenant {tenant_id} blocked from reservation")
            return False, []
        
        if priority > tenant_quota['min_priority_for_reservation']:
            return False, []
        
        if not self._check_rate_limit(tenant_id, amount, tenant_quota):
            logger.warning(f"Tenant {tenant_id} rate limit exceeded")
            return False, []
        
        if not self._check_cooldown(tenant_id, tenant_quota):
            return False, []
        
        if not self._check_concurrent_tasks(account_id, tenant_quota):
            return False, []
        
        success, token_ids = self._do_reserve_tokens(account_id, amount, consumer)
        
        if success:
            self.tenant_usage[tenant_id].append({'amount': amount, 'timestamp': datetime.utcnow()})
            self.tenant_last_reservation[tenant_id] = datetime.utcnow()
        else:
            self._track_failed_attempt(tenant_id)
        
        return success, token_ids
    
    def _do_reserve_tokens(
        self, account_id: str, amount: float, consumer: EcoATPConsumer
    ) -> Tuple[bool, List[str]]:
        """Actual token reservation logic"""
        if account_id not in self.accounts:
            self.create_account(account_id)
        
        account = self.accounts[account_id]
        
        if self._is_hoarding(account_id):
            tax = amount * self.tax_rate
            amount += tax
        
        if account.balance < amount:
            return False, []
        
        reserved_tokens = []
        remaining = amount
        
        sorted_tokens = sorted(
            [t for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE],
            key=lambda t: t.generated_at
        )
        
        for token in sorted_tokens:
            if remaining <= 0:
                break
            effective_value = token.apply_decay(datetime.utcnow())
            if effective_value >= remaining:
                token.state = TokenState.RESERVED
                reserved_tokens.append(token.token_id)
                remaining = 0
            else:
                token.state = TokenState.RESERVED
                reserved_tokens.append(token.token_id)
                remaining -= effective_value
        
        if remaining > 0:
            for token_id in reserved_tokens:
                self.active_tokens[token_id].state = TokenState.AVAILABLE
            return False, []
        
        account.balance -= amount
        return True, reserved_tokens
    
    def _check_rate_limit(self, tenant_id: str, amount: float, quota: Dict[str, Any]) -> bool:
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        recent_usage = sum(
            u['amount'] for u in self.tenant_usage[tenant_id]
            if u['timestamp'] > minute_ago
        )
        return (recent_usage + amount) <= quota['max_tokens_per_minute']
    
    def _check_cooldown(self, tenant_id: str, quota: Dict[str, Any]) -> bool:
        if tenant_id in self.tenant_last_reservation:
            elapsed = (datetime.utcnow() - self.tenant_last_reservation[tenant_id]).total_seconds()
            if elapsed < quota['reservation_cooldown_seconds']:
                return False
        return True
    
    def _check_concurrent_tasks(self, account_id: str, quota: Dict[str, Any]) -> bool:
        active_count = sum(
            1 for token in self.active_tokens.values()
            if token.state == TokenState.RESERVED and account_id in token.token_id
        )
        return active_count < quota['max_concurrent_tasks']
    
    def _track_failed_attempt(self, tenant_id: str):
        self._failed_attempts[tenant_id] += 1
        if self._failed_attempts[tenant_id] >= self.suspicious_threshold:
            self.suspicious_tenants.add(tenant_id)
            logger.critical(
                f"Tenant {tenant_id} marked as SUSPICIOUS after "
                f"{self._failed_attempts[tenant_id]} failed attempts"
            )
    
    def clear_suspicious_status(self, tenant_id: str):
        self.suspicious_tenants.discard(tenant_id)
        self._failed_attempts[tenant_id] = 0
    
    def set_tenant_quota(self, tenant_id: str, quota: Dict[str, Any]):
        self.tenant_quotas[tenant_id] = {**self.default_quota, **quota}
    
    # ========================================================================
    # FIX 7: Batch Processing
    # ========================================================================
    
    async def _batch_processor_loop(self):
        """Process token operations in batches for efficiency"""
        while True:
            try:
                if self.batch_queue:
                    batch = self.batch_queue[:self.batch_size]
                    self.batch_queue = self.batch_queue[self.batch_size:]
                    
                    async with self._batch_lock:
                        for request in batch:
                            try:
                                result = self._do_reserve_tokens(
                                    request['account_id'],
                                    request['amount'],
                                    request['consumer']
                                )
                                if request.get('future'):
                                    request['future'].set_result(result)
                            except Exception as e:
                                if request.get('future'):
                                    request['future'].set_exception(e)
                
                await asyncio.sleep(0.001)
            except Exception as e:
                logger.error(f"Batch processor error: {str(e)}")
                await asyncio.sleep(0.01)
    
    async def reserve_tokens_async(
        self, account_id: str, amount: float, consumer: EcoATPConsumer
    ) -> Tuple[bool, List[str]]:
        """Asynchronous token reservation with batching"""
        future = asyncio.Future()
        self.batch_queue.append({
            'account_id': account_id, 'amount': amount,
            'consumer': consumer, 'future': future,
            'timestamp': datetime.utcnow()
        })
        try:
            result = await asyncio.wait_for(future, timeout=0.01)
            return result
        except asyncio.TimeoutError:
            return False, []
    
    # ========================================================================
    # Core Methods
    # ========================================================================
    
    def create_account(self, account_id: str) -> EcoATPAccount:
        if account_id not in self.accounts:
            self.accounts[account_id] = EcoATPAccount(account_id=account_id)
        return self.accounts[account_id]
    
    def generate_tokens(
        self, account_id: str, source: EcoATPSource,
        carbon_saved_kg: float = 0.0, helium_saved_units: float = 0.0,
        energy_saved_kwh: float = 0.0, efficiency: float = 1.0,
        num_tokens: Optional[int] = None
    ) -> List[EcoATPToken]:
        if account_id not in self.accounts:
            self.create_account(account_id)
        
        carbon_value = self.exchange_rate.carbon_to_ecoatp(carbon_saved_kg)
        helium_value = self.exchange_rate.helium_to_ecoatp(helium_saved_units)
        energy_value = energy_saved_kwh * 1000
        total_value = carbon_value + helium_value + energy_value
        
        if num_tokens is None:
            num_tokens = max(1, int(total_value / 10))
        
        token_value = total_value / num_tokens
        tokens = []
        
        for i in range(num_tokens):
            token = EcoATPToken(
                token_id=f"eco_{account_id}_{datetime.utcnow().timestamp()}_{i}",
                value=token_value, source=source,
                generated_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(hours=24),
                carbon_equivalent_kg=carbon_saved_kg / num_tokens,
                helium_equivalent_units=helium_saved_units / num_tokens,
                generation_efficiency=efficiency
            )
            tokens.append(token)
            self.active_tokens[token.token_id] = token
            self.accounts[account_id].balance += token_value
            self.accounts[account_id].total_generated += token_value
        
        self.last_generation_time = datetime.utcnow()
        
        # Replenish substrate reserves from surplus
        if total_value > 100 and self.substrate_reserves < 500:
            self.replenish_substrate_reserves(total_value * 0.05)
        
        return tokens
    
    def consume_tokens(
        self, token_ids: List[str], consumer: EcoATPConsumer,
        operation_success: bool = True
    ) -> float:
        consumed = 0.0
        for token_id in token_ids:
            if token_id in self.active_tokens:
                token = self.active_tokens[token_id]
                effective_value = token.apply_decay(datetime.utcnow())
                token.state = TokenState.CONSUMED
                consumed += effective_value
                for account_id, account in self.accounts.items():
                    if account_id in token_id:
                        account.total_consumed += effective_value
        return consumed
    
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float:
        recovered = 0.0
        rates = sorted(self.recovery_rates.keys())
        closest_rate = min(rates, key=lambda r: abs(r - completion_percentage))
        recovery_percentage = self.recovery_rates[closest_rate]
        
        for token_id in token_ids:
            if token_id in self.active_tokens:
                token = self.active_tokens[token_id]
                if token.state == TokenState.RESERVED:
                    recovered_value = token.value * recovery_percentage
                    token.state = TokenState.RECOVERED
                    recovered += recovered_value
                    for account_id, account in self.accounts.items():
                        if account_id in token_id:
                            account.balance += recovered_value
                            account.total_recovered += recovered_value
        return recovered
    
    def _is_hoarding(self, account_id: str) -> bool:
        if account_id not in self.accounts:
            return False
        balances = [acc.balance for acc in self.accounts.values()]
        if not balances:
            return False
        avg_balance = np.mean(balances)
        return self.accounts[account_id].balance > avg_balance * self.hoarding_threshold
    
    def get_account_summary(self, account_id: str) -> Dict[str, Any]:
        if account_id not in self.accounts:
            return {}
        account = self.accounts[account_id]
        return {
            'account_id': account_id, 'balance': account.balance,
            'total_generated': account.total_generated,
            'total_consumed': account.total_consumed,
            'total_recovered': account.total_recovered,
            'total_expired': account.total_expired,
            'utilization_rate': account.utilization_rate,
            'efficiency_rating': account.efficiency_rating
        }
    
    def get_system_summary(self) -> Dict[str, Any]:
        total_balance = sum(acc.balance for acc in self.accounts.values())
        total_generated = sum(acc.total_generated for acc in self.accounts.values())
        total_consumed = sum(acc.total_consumed for acc in self.accounts.values())
        return {
            'total_accounts': len(self.accounts),
            'total_balance': total_balance,
            'total_generated': total_generated,
            'total_consumed': total_consumed,
            'system_efficiency': total_consumed / max(total_generated, 1),
            'active_tokens': len([t for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE]),
            'emergency_mode': self.emergency_mode,
            'substrate_reserves': self.substrate_reserves,
            'suspicious_tenants': len(self.suspicious_tenants)
        }
    
    async def _maintenance_loop(self):
        """Background maintenance loop"""
        while True:
            try:
                # Expire old tokens
                now = datetime.utcnow()
                for token_id, token in list(self.active_tokens.items()):
                    if token.is_expired(now) and token.state == TokenState.AVAILABLE:
                        token.state = TokenState.EXPIRED
                
                # Redistribute wealth
                if now - self.last_redistribution > self.redistribution_interval:
                    self.last_redistribution = now
                
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Maintenance error: {str(e)}")
                await asyncio.sleep(60)
