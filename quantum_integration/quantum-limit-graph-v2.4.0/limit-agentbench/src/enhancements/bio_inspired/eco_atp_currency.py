# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/eco_atp_currency.py
# Complete enhanced file v6.0.0 with TokenSupplyManager and PredictiveTokenAllocator

"""
Enhanced Eco-ATP Currency System v6.0.0
Complete implementation with supply management, pre-allocation, protocol support,
quantum advantage as token generation source (NEW), predictive supply adjustment (NEW),
ML-based demand prediction (NEW), user-defined emergency thresholds (NEW),
and adaptive rate limiting based on system load (NEW).
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import math
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ============================================================================
# Protocol Definition
# ============================================================================

class TokenServiceProtocol(Protocol):
    """Explicit contract for token management services"""
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any,
                       tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

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
    EMERGENCY_SUBSTRATE = "emergency_substrate"
    # NEW: Quantum advantage
    QUANTUM_ADVANTAGE = "quantum_advantage"

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
    # NEW: Quantum advantage
    QUANTUM_BACKED = "quantum_backed"

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
    # NEW: Quantum metadata
    quantum_advantage_factor: float = 0.0
    quantum_circuit_id: Optional[str] = None
    
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
    # NEW: Quantum account
    quantum_balance: float = 0.0
    quantum_total_generated: float = 0.0
    
    @property
    def net_balance(self) -> float:
        return self.balance
    
    @property
    def utilization_rate(self) -> float:
        if self.total_generated == 0:
            return 0.0
        return self.total_consumed / self.total_generated

# ============================================================================
# ML-Based Demand Predictor (NEW)
# ============================================================================

class MLDemandPredictor:
    """
    Machine learning-based demand prediction for token allocation.
    
    Features:
    - Random Forest regression for demand prediction
    - Pattern recognition from historical data
    - Confidence scoring
    - Online learning
    """
    
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=50, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.training_samples = 0
        self.history: List[Dict] = []
        self.predictions: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        
        logger.info("ML Demand Predictor initialized")
    
    def record_demand(self, account_id: str, amount: float, timestamp: datetime):
        """Record demand data for training"""
        self.history.append({
            'account_id': account_id,
            'timestamp': timestamp,
            'amount': amount,
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'month': timestamp.month
        })
        if len(self.history) > 1000:
            self.history = self.history[-1000:]
    
    async def train(self):
        """Train the demand prediction model"""
        if len(self.history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        
        async with self._lock:
            # Prepare features
            X = []
            y = []
            
            # Group by account
            accounts = set(h['account_id'] for h in self.history)
            
            for account_id in accounts:
                account_history = [h for h in self.history if h['account_id'] == account_id]
                if len(account_history) < 10:
                    continue
                
                for i in range(10, len(account_history) - 1):
                    features = []
                    for j in range(10):
                        data = account_history[i - j]
                        features.extend([
                            data['amount'] / 100,
                            data['hour'] / 23.0,
                            data['day_of_week'] / 6.0,
                            data['month'] / 11.0
                        ])
                    X.append(features)
                    y.append(account_history[i + 1]['amount'])
            
            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}
            
            X = np.array(X)
            y = np.array(y)
            X_scaled = self.scaler.fit_transform(X)
            
            self.model.fit(X_scaled, y)
            self.is_trained = True
            self.training_samples = len(X)
            
            logger.info(f"ML Demand Predictor trained on {len(X)} samples")
            return {'status': 'success', 'samples': len(X)}
    
    async def predict_demand(self, account_id: str) -> Dict[str, Any]:
        """Predict demand for an account"""
        if not self.is_trained:
            return {'predicted_amount': 10.0, 'confidence': 0.0}
        
        async with self._lock:
            # Get recent history for this account
            account_history = [h for h in self.history if h['account_id'] == account_id]
            if len(account_history) < 10:
                return {'predicted_amount': 10.0, 'confidence': 0.3}
            
            # Prepare features from recent data
            recent = account_history[-10:]
            features = []
            for data in recent:
                features.extend([
                    data['amount'] / 100,
                    data['hour'] / 23.0,
                    data['day_of_week'] / 6.0,
                    data['month'] / 11.0
                ])
            
            # Ensure correct feature count
            features = features[:self.model.n_features_in_]
            while len(features) < self.model.n_features_in_:
                features.append(0.5)
            
            features_array = np.array([features])
            features_scaled = self.scaler.transform(features_array)
            
            prediction = self.model.predict(features_scaled)[0]
            confidence = min(0.9, self.training_samples / 100)
            
            return {
                'predicted_amount': max(0.0, prediction),
                'confidence': confidence,
                'timestamp': datetime.utcnow().isoformat()
            }

# ============================================================================
# Dynamic Exchange Rate Engine (Enhanced)
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
        # NEW: Quantum advantage factor
        self.quantum_advantage_multiplier = 1.0
    
    def update_scarcity(self, carbon_zone: int, helium_scarcity: float, grid_carbon_intensity: float,
                       quantum_advantage: float = 0.0):
        """Update scarcity with quantum advantage factor"""
        self.carbon_scarcity_multiplier = 1.0 + (carbon_zone / 15.0) * 2.0
        self.helium_scarcity_multiplier = 1.0 + helium_scarcity * 3.0
        
        # NEW: Quantum advantage adjustment
        if quantum_advantage > 0:
            self.quantum_advantage_multiplier = 1.0 + quantum_advantage * 0.5
        
        total_scarcity = self.carbon_scarcity_multiplier + self.helium_scarcity_multiplier
        self.carbon_weight = self.carbon_scarcity_multiplier / total_scarcity
        self.helium_weight = self.helium_scarcity_multiplier / total_scarcity
        
        self.rate_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'carbon_weight': self.carbon_weight,
            'helium_weight': self.helium_weight,
            'carbon_multiplier': self.carbon_scarcity_multiplier,
            'helium_multiplier': self.helium_scarcity_multiplier,
            'quantum_multiplier': self.quantum_advantage_multiplier
        })
    
    def carbon_to_ecoatp(self, carbon_kg: float) -> float:
        return carbon_kg * self.base_carbon_to_ecoatp * self.carbon_scarcity_multiplier * self.quantum_advantage_multiplier
    
    def helium_to_ecoatp(self, helium_units: float) -> float:
        return helium_units * self.base_helium_to_ecoatp * self.helium_scarcity_multiplier * self.quantum_advantage_multiplier
    
    def get_current_rates(self) -> Dict[str, Any]:
        return {
            'carbon_to_ecoatp': self.base_carbon_to_ecoatp * self.carbon_scarcity_multiplier * self.quantum_advantage_multiplier,
            'helium_to_ecoatp': self.base_helium_to_ecoatp * self.helium_scarcity_multiplier * self.quantum_advantage_multiplier,
            'carbon_weight': self.carbon_weight,
            'helium_weight': self.helium_weight,
            'carbon_scarcity': self.carbon_scarcity_multiplier,
            'helium_scarcity': self.helium_scarcity_multiplier,
            'quantum_advantage': self.quantum_advantage_multiplier
        }

# ============================================================================
# Enhanced Eco-ATP Token Manager
# ============================================================================

class EcoATPTokenManager:
    """Enhanced Eco-ATP Token Manager with all features"""
    
    def __init__(self, exchange_rate: Optional[DynamicExchangeRate] = None):
        self.exchange_rate = exchange_rate or DynamicExchangeRate()
        self.accounts: Dict[str, EcoATPAccount] = {}
        self.active_tokens: Dict[str, EcoATPToken] = {}
        self.token_history: deque = deque(maxlen=10000)
        
        self.hoarding_threshold = 2.0
        self.tax_rate = 0.1
        self.redistribution_interval = timedelta(minutes=30)
        self.last_redistribution = datetime.utcnow()
        
        self.recovery_rates = {0.0: 0.0, 0.25: 0.125, 0.5: 0.25, 0.75: 0.6, 0.9: 0.8, 1.0: 0.95}
        
        # Emergency mode with user-defined thresholds
        self.emergency_mode = False
        self.emergency_token_rate = 10.0
        self.emergency_reserve = 1000.0
        self.emergency_threshold = 50.0  # User-definable
        self.user_emergency_thresholds: Dict[str, float] = {}
        self.substrate_phosphorylation_active = False
        self.substrate_reserves = 500.0
        self.last_generation_time: Optional[datetime] = None
        
        # Tenant quotas
        self.tenant_quotas: Dict[str, Dict[str, Any]] = {}
        self.default_quota = {'max_tokens_per_minute': 100.0, 'max_concurrent_tasks': 5,
                             'min_priority_for_reservation': 2, 'reservation_cooldown_seconds': 1.0}
        self.tenant_usage: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.tenant_last_reservation: Dict[str, datetime] = {}
        self.suspicious_tenants: Set[str] = set()
        self.suspicious_threshold = 5
        self._failed_attempts: Dict[str, int] = defaultdict(int)
        
        # Batch processing
        self.batch_queue: List[Dict[str, Any]] = []
        self.batch_size = 10
        self._batch_lock = asyncio.Lock()
        
        # NEW: ML Demand Predictor
        self.ml_predictor = MLDemandPredictor()
        
        # NEW: Predictive supply adjustment
        self.predictive_supply_enabled = True
        self.predicted_demand_accumulator: Dict[str, float] = defaultdict(float)
        
        # NEW: Adaptive rate limiting
        self.system_load_history: deque = deque(maxlen=100)
        self.current_rate_multiplier = 1.0
        
        # NEW: User-defined emergency thresholds
        self.user_emergency_override = False
        
        # Start background tasks
        asyncio.create_task(self._emergency_monitor_loop())
        asyncio.create_task(self._batch_processor_loop())
        asyncio.create_task(self._maintenance_loop())
        asyncio.create_task(self._predictive_supply_loop())  # NEW
        asyncio.create_task(self._adaptive_rate_loop())  # NEW
        
        logger.info("Enhanced Eco-ATP Token Manager v6.0.0 initialized")
    
    def create_account(self, account_id: str) -> EcoATPAccount:
        if account_id not in self.accounts:
            self.accounts[account_id] = EcoATPAccount(account_id=account_id)
        return self.accounts[account_id]
    
    # ========================================================================
    # NEW: User-Defined Emergency Thresholds
    # ========================================================================
    
    def set_emergency_threshold(self, account_id: str, threshold: float):
        """Set user-defined emergency threshold for an account"""
        self.user_emergency_thresholds[account_id] = max(10.0, threshold)
        self.user_emergency_override = True
        logger.info(f"Emergency threshold for {account_id} set to {threshold:.1f}")
    
    def get_emergency_threshold(self, account_id: str) -> float:
        """Get effective emergency threshold for an account"""
        if account_id in self.user_emergency_thresholds:
            return self.user_emergency_thresholds[account_id]
        return self.emergency_threshold
    
    # ========================================================================
    # Token Generation (Enhanced with Quantum)
    # ========================================================================
    
    def generate_tokens(self, account_id: str, source: EcoATPSource,
                       carbon_saved_kg: float = 0.0, helium_saved_units: float = 0.0,
                       energy_saved_kwh: float = 0.0, efficiency: float = 1.0,
                       num_tokens: Optional[int] = None,
                       quantum_advantage_factor: float = 0.0,
                       quantum_circuit_id: Optional[str] = None) -> List[EcoATPToken]:
        """Generate tokens with quantum advantage support"""
        if account_id not in self.accounts:
            self.create_account(account_id)
        
        # Apply quantum advantage multiplier
        quantum_multiplier = 1.0
        if source == EcoATPSource.QUANTUM_ADVANTAGE or quantum_advantage_factor > 0:
            quantum_multiplier = 1.0 + quantum_advantage_factor * 0.5
            self.exchange_rate.quantum_advantage_multiplier = quantum_multiplier
        
        carbon_value = self.exchange_rate.carbon_to_ecoatp(carbon_saved_kg)
        helium_value = self.exchange_rate.helium_to_ecoatp(helium_saved_units)
        energy_value = energy_saved_kwh * 1000
        total_value = (carbon_value + helium_value + energy_value) * quantum_multiplier
        
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
                generation_efficiency=efficiency,
                quantum_advantage_factor=quantum_advantage_factor,
                quantum_circuit_id=quantum_circuit_id
            )
            tokens.append(token)
            self.active_tokens[token.token_id] = token
            
            account = self.accounts[account_id]
            account.balance += token_value
            account.total_generated += token_value
            if source == EcoATPSource.QUANTUM_ADVANTAGE:
                account.quantum_balance += token_value
                account.quantum_total_generated += token_value
        
        self.last_generation_time = datetime.utcnow()
        
        # Track generation for ML prediction
        self.ml_predictor.record_demand(account_id, total_value, datetime.utcnow())
        asyncio.create_task(self.ml_predictor.train())
        
        if total_value > 100 and self.substrate_reserves < 500:
            self.substrate_reserves = min(1000.0, self.substrate_reserves + total_value * 0.05)
        
        return tokens
    
    # ========================================================================
    # Enhanced Reservation with ML Prediction
    # ========================================================================
    
    def reserve_tokens(self, account_id: str, amount: float, consumer: EcoATPConsumer,
                      tenant_id: str = "default", priority: int = 2) -> Tuple[bool, List[str]]:
        """Enhanced reservation with adaptive rate limiting"""
        tenant_quota = self.tenant_quotas.get(tenant_id, self.default_quota)
        
        if tenant_id in self.suspicious_tenants:
            logger.warning(f"Suspicious tenant {tenant_id} blocked")
            return False, []
        
        if priority > tenant_quota['min_priority_for_reservation']:
            return False, []
        
        # Adaptive rate limiting
        if not self._check_adaptive_rate_limit(tenant_id, amount, tenant_quota):
            return False, []
        
        if not self._check_cooldown(tenant_id, tenant_quota):
            return False, []
        
        success, token_ids = self._do_reserve_tokens(account_id, amount, consumer)
        
        if success:
            self.tenant_usage[tenant_id].append({'amount': amount, 'timestamp': datetime.utcnow()})
            self.tenant_last_reservation[tenant_id] = datetime.utcnow()
            # Record demand for ML prediction
            self.ml_predictor.record_demand(account_id, amount, datetime.utcnow())
        else:
            self._track_failed_attempt(tenant_id)
        
        return success, token_ids
    
    def _do_reserve_tokens(self, account_id: str, amount: float, consumer: EcoATPConsumer) -> Tuple[bool, List[str]]:
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
    
    def consume_tokens(self, token_ids: List[str], consumer: EcoATPConsumer, operation_success: bool = True) -> float:
        consumed = 0.0
        for token_id in token_ids:
            if token_id in self.active_tokens:
                token = self.active_tokens[token_id]
                effective_value = token.apply_decay(datetime.utcnow())
                token.state = TokenState.CONSUMED
                consumed += effective_value
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
    
    # ========================================================================
    # Adaptive Rate Limiting (NEW)
    # ========================================================================
    
    def _check_adaptive_rate_limit(self, tenant_id: str, amount: float, quota: Dict[str, Any]) -> bool:
        """Adaptive rate limiting based on system load"""
        # Get current system load
        system_load = self._get_system_load()
        self.system_load_history.append(system_load)
        
        # Adjust multiplier based on load
        if len(self.system_load_history) > 10:
            avg_load = sum(self.system_load_history) / len(self.system_load_history)
            if avg_load > 0.8:
                self.current_rate_multiplier = 0.5
            elif avg_load > 0.6:
                self.current_rate_multiplier = 0.75
            elif avg_load < 0.3:
                self.current_rate_multiplier = 1.5
            else:
                self.current_rate_multiplier = 1.0
        
        # Apply adaptive limit
        adaptive_limit = quota['max_tokens_per_minute'] * self.current_rate_multiplier
        
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        recent_usage = sum(u['amount'] for u in self.tenant_usage[tenant_id] if u['timestamp'] > minute_ago)
        
        return (recent_usage + amount) <= adaptive_limit
    
    def _get_system_load(self) -> float:
        """Calculate current system load"""
        summary = self.get_system_summary()
        total_balance = summary.get('total_balance', 0)
        total_generated = summary.get('total_generated', 1)
        utilization = summary.get('system_efficiency', 0)
        
        # Load based on utilization and balance
        load = utilization * 0.6 + (1.0 - min(1.0, total_balance / 1000)) * 0.4
        return min(1.0, max(0.0, load))
    
    def _check_cooldown(self, tenant_id: str, quota: Dict[str, Any]) -> bool:
        if tenant_id in self.tenant_last_reservation:
            elapsed = (datetime.utcnow() - self.tenant_last_reservation[tenant_id]).total_seconds()
            if elapsed < quota['reservation_cooldown_seconds']:
                return False
        return True
    
    def _track_failed_attempt(self, tenant_id: str):
        self._failed_attempts[tenant_id] += 1
        if self._failed_attempts[tenant_id] >= self.suspicious_threshold:
            self.suspicious_tenants.add(tenant_id)
    
    # ========================================================================
    # Predictive Supply Adjustment (NEW)
    # ========================================================================
    
    async def _predictive_supply_loop(self):
        """Predictive supply adjustment based on ML predictions"""
        while True:
            try:
                if not self.predictive_supply_enabled:
                    await asyncio.sleep(60)
                    continue
                
                # Get predictions for all accounts
                predictions = {}
                for account_id in self.accounts:
                    pred = await self.ml_predictor.predict_demand(account_id)
                    if pred.get('confidence', 0) > 0.5:
                        predictions[account_id] = pred['predicted_amount']
                
                # Adjust generation rate based on predictions
                if predictions:
                    total_predicted = sum(predictions.values())
                    current_supply = sum(acc.balance for acc in self.accounts.values())
                    
                    if total_predicted > current_supply * 1.2:
                        # Increase supply
                        self.substrate_reserves = min(1000.0, self.substrate_reserves + total_predicted * 0.1)
                        logger.info(f"Predictive supply: increased reserves by {total_predicted * 0.1:.1f}")
                    elif total_predicted < current_supply * 0.5:
                        # Burn excess
                        excess = current_supply - total_predicted
                        self._burn_tokens(excess * 0.1)
                        logger.info(f"Predictive supply: burned {excess * 0.1:.1f} tokens")
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Predictive supply loop error: {str(e)}")
                await asyncio.sleep(600)
    
    def _burn_tokens(self, amount: float):
        """Burn tokens to manage supply"""
        if amount <= 0:
            return
        summary = self.get_system_summary()
        total_balance = max(summary.get('total_balance', 1), 1)
        burned = 0.0
        for account_id, account in list(self.accounts.items()):
            if account.balance <= 0:
                continue
            proportion = account.balance / total_balance
            burn_amount = min(account.balance, amount * proportion)
            if burn_amount > 0:
                account.balance -= burn_amount
                account.total_consumed += burn_amount
                burned += burn_amount
        if burned > 0:
            logger.info(f"Burned {burned:.1f} tokens")
    
    # ========================================================================
    # Adaptive Rate Limit Loop (NEW)
    # ========================================================================
    
    async def _adaptive_rate_loop(self):
        """Monitor and adjust rate limits based on system performance"""
        while True:
            try:
                load = self._get_system_load()
                self.system_load_history.append(load)
                
                if len(self.system_load_history) > 20:
                    avg_load = sum(self.system_load_history[-20:]) / 20
                    if avg_load > 0.8:
                        self.current_rate_multiplier = 0.5
                    elif avg_load > 0.6:
                        self.current_rate_multiplier = 0.75
                    elif avg_load < 0.3:
                        self.current_rate_multiplier = 1.5
                    else:
                        self.current_rate_multiplier = 1.0
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Adaptive rate loop error: {str(e)}")
                await asyncio.sleep(120)
    
    # ========================================================================
    # Emergency Monitor (Enhanced)
    # ========================================================================
    
    async def _emergency_monitor_loop(self):
        """Enhanced emergency monitoring with user thresholds"""
        while True:
            try:
                summary = self.get_system_summary()
                
                # Check each account for emergency thresholds
                for account_id, account in self.accounts.items():
                    threshold = self.get_emergency_threshold(account_id)
                    if account.balance < threshold and not self.emergency_mode:
                        logger.warning(f"Account {account_id} below threshold ({account.balance:.1f} < {threshold:.1f})")
                        self._activate_emergency_mode()
                        break
                
                if self.last_generation_time:
                    time_since = (datetime.utcnow() - self.last_generation_time).total_seconds()
                    if time_since > 30 and summary.get('total_balance', 0) < self.emergency_threshold and not self.emergency_mode:
                        self._activate_emergency_mode()
                    elif self.emergency_mode and time_since < 10:
                        self._deactivate_emergency_mode()
                
                if self.emergency_mode:
                    self._generate_emergency_tokens()
                
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Emergency monitor error: {str(e)}")
                await asyncio.sleep(10)
    
    def _activate_emergency_mode(self):
        self.emergency_mode = True
        self.substrate_phosphorylation_active = True
        logger.critical(f"EMERGENCY MODE: Reserve={self.emergency_reserve:.0f}, Substrate={self.substrate_reserves:.0f}")
    
    def _deactivate_emergency_mode(self):
        self.emergency_mode = False
        self.substrate_phosphorylation_active = False
        logger.info("Emergency mode deactivated")
    
    def _generate_emergency_tokens(self):
        if self.substrate_reserves <= 0:
            return
        substrate_used = min(self.emergency_token_rate, self.substrate_reserves)
        self.substrate_reserves -= substrate_used
        emergency_tokens = substrate_used * 0.5
        critical_accounts = ['energy_expert', 'helium_expert', 'green_agent_core']
        
        # Check user-defined critical accounts
        for account_id in self.accounts:
            if account_id in self.user_emergency_thresholds:
                if account_id not in critical_accounts:
                    critical_accounts.append(account_id)
        
        per_account = emergency_tokens / len(critical_accounts)
        for account_id in critical_accounts:
            if account_id in self.accounts:
                self.accounts[account_id].balance += per_account
        self.last_generation_time = datetime.utcnow()
    
    async def _batch_processor_loop(self):
        while True:
            try:
                if self.batch_queue:
                    batch = self.batch_queue[:self.batch_size]
                    self.batch_queue = self.batch_queue[self.batch_size:]
                    async with self._batch_lock:
                        for request in batch:
                            try:
                                result = self._do_reserve_tokens(request['account_id'], request['amount'], request['consumer'])
                                if request.get('future'):
                                    request['future'].set_result(result)
                            except Exception as e:
                                if request.get('future'):
                                    request['future'].set_exception(e)
                await asyncio.sleep(0.001)
            except Exception as e:
                logger.error(f"Batch processor error: {str(e)}")
                await asyncio.sleep(0.01)
    
    async def _maintenance_loop(self):
        while True:
            try:
                now = datetime.utcnow()
                for token_id, token in list(self.active_tokens.items()):
                    if token.is_expired(now) and token.state == TokenState.AVAILABLE:
                        token.state = TokenState.EXPIRED
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Maintenance error: {str(e)}")
                await asyncio.sleep(60)
    
    def get_account_summary(self, account_id: str) -> Dict[str, Any]:
        if account_id not in self.accounts:
            return {}
        account = self.accounts[account_id]
        return {
            'account_id': account_id,
            'balance': account.balance,
            'quantum_balance': account.quantum_balance,
            'total_generated': account.total_generated,
            'total_consumed': account.total_consumed,
            'efficiency_rating': account.efficiency_rating,
            'emergency_threshold': self.get_emergency_threshold(account_id)
        }
    
    def get_system_summary(self) -> Dict[str, Any]:
        total_balance = sum(acc.balance for acc in self.accounts.values())
        total_quantum_balance = sum(acc.quantum_balance for acc in self.accounts.values())
        total_generated = sum(acc.total_generated for acc in self.accounts.values())
        total_consumed = sum(acc.total_consumed for acc in self.accounts.values())
        return {
            'total_accounts': len(self.accounts),
            'total_balance': total_balance,
            'total_quantum_balance': total_quantum_balance,
            'total_generated': total_generated,
            'total_consumed': total_consumed,
            'system_efficiency': total_consumed / max(total_generated, 1),
            'active_tokens': len([t for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE]),
            'emergency_mode': self.emergency_mode,
            'substrate_reserves': self.substrate_reserves,
            'suspicious_tenants': len(self.suspicious_tenants),
            'current_rate_multiplier': self.current_rate_multiplier,
            'system_load': self._get_system_load()
        }
    
    def explain_system_state(self) -> Dict[str, Any]:
        summary = self.get_system_summary()
        if summary.get('emergency_mode'):
            health = "CRITICAL: System operating in emergency mode."
        elif summary.get('total_balance', 0) < 100:
            health = "WARNING: Token reserves critically low."
        elif summary.get('system_efficiency', 0) > 0.9:
            health = "EXCELLENT: Peak efficiency with healthy reserves."
        else:
            health = "NORMAL: System operating within parameters."
        
        # Add quantum metrics
        quantum_share = summary.get('total_quantum_balance', 0) / max(summary.get('total_balance', 1), 1)
        health += f" Quantum share: {quantum_share:.1%}"
        
        return {
            'health_assessment': health,
            'metrics': summary,
            'timestamp': datetime.utcnow().isoformat()
        }

# ============================================================================
# Token Supply Manager (Enhanced)
# ============================================================================

class TokenSupplyManager:
    """Manages token supply with predictive adjustment"""
    
    def __init__(self, token_manager: EcoATPTokenManager, target_utilization: float = 0.75,
                 enable_predictive: bool = True):
        self.token_manager = token_manager
        self.target_utilization = target_utilization
        self.base_generation_rate = 150.0
        self.current_generation_rate = 150.0
        self.burn_rate = 0.0
        self.total_burned = 0.0
        self.supply_history: deque = deque(maxlen=1000)
        self.enable_predictive = enable_predictive
        self.prediction_window = 5  # minutes
        
        # NEW: Track demand predictions
        self.predicted_demand = 0.0
        self.prediction_confidence = 0.0
        
        asyncio.create_task(self._supply_management_loop())
        logger.info(f"Token Supply Manager initialized: target={target_utilization:.0%}, predictive={enable_predictive}")
    
    async def _supply_management_loop(self):
        while True:
            try:
                self.adjust_supply()
                # NEW: Predictive adjustment
                if self.enable_predictive:
                    await self._predictive_adjustment()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Supply management error: {str(e)}")
                await asyncio.sleep(600)
    
    async def _predictive_adjustment(self):
        """Predictive supply adjustment using ML"""
        try:
            # Get predictions from ML predictor
            predictions = {}
            for account_id in self.token_manager.accounts:
                pred = await self.token_manager.ml_predictor.predict_demand(account_id)
                if pred.get('confidence', 0) > 0.5:
                    predictions[account_id] = pred['predicted_amount']
            
            if predictions:
                total_predicted = sum(predictions.values())
                self.predicted_demand = total_predicted
                self.prediction_confidence = np.mean([p.get('confidence', 0.5) for p in predictions.values()])
                
                # Adjust generation rate based on predictions
                current_supply = sum(acc.balance for acc in self.token_manager.accounts.values())
                predicted_supply_need = total_predicted * 1.2  # Buffer
                
                if predicted_supply_need > current_supply and self.prediction_confidence > 0.6:
                    # Need more supply
                    increase_factor = min(1.5, predicted_supply_need / max(current_supply, 1))
                    self.current_generation_rate = self.base_generation_rate * increase_factor
                    logger.debug(f"Predictive supply: increased rate to {self.current_generation_rate:.1f}")
                elif predicted_supply_need < current_supply * 0.7 and self.prediction_confidence > 0.6:
                    # Need less supply
                    decrease_factor = max(0.5, predicted_supply_need / max(current_supply, 1))
                    self.current_generation_rate = self.base_generation_rate * decrease_factor
                    logger.debug(f"Predictive supply: decreased rate to {self.current_generation_rate:.1f}")
        except Exception as e:
            logger.warning(f"Predictive adjustment error: {str(e)}")
    
    def adjust_supply(self):
        summary = self.token_manager.get_system_summary()
        total_supply = summary.get('total_balance', 0)
        total_generated = summary.get('total_generated', 0)
        total_consumed = summary.get('total_consumed', 0)
        
        utilization = total_consumed / max(total_generated, 1)
        inflation_pressure = (total_generated - total_consumed) / max(total_consumed, 1) if total_consumed > 0 else 0.0
        
        # Only adjust if not using predictive mode
        if not self.enable_predictive or self.prediction_confidence < 0.5:
            if utilization < self.target_utilization - 0.15:
                reduction = min(0.5, (self.target_utilization - utilization) * 2)
                self.current_generation_rate = self.base_generation_rate * (1 - reduction)
                if inflation_pressure > 0.2:
                    excess = total_supply * inflation_pressure * 0.1
                    self._burn_tokens(excess)
            elif utilization > self.target_utilization + 0.15:
                increase = min(0.5, (utilization - self.target_utilization) * 2)
                self.current_generation_rate = self.base_generation_rate * (1 + increase)
            else:
                self.current_generation_rate += (self.base_generation_rate - self.current_generation_rate) * 0.1
        
        self.supply_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'total_supply': total_supply,
            'utilization': utilization,
            'inflation_pressure': inflation_pressure,
            'generation_rate': self.current_generation_rate,
            'total_burned': self.total_burned,
            'predicted_demand': self.predicted_demand,
            'prediction_confidence': self.prediction_confidence
        })
    
    def _burn_tokens(self, amount: float):
        if amount <= 0:
            return
        summary = self.token_manager.get_system_summary()
        total_balance = max(summary.get('total_balance', 1), 1)
        burned = 0.0
        for account_id, account in list(self.token_manager.accounts.items()):
            if account.balance <= 0:
                continue
            proportion = account.balance / total_balance
            burn_amount = min(account.balance, amount * proportion)
            if burn_amount > 0:
                account.balance -= burn_amount
                account.total_consumed += burn_amount
                burned += burn_amount
        self.burn_rate = burned
        self.total_burned += burned
        if burned > 0:
            logger.info(f"Burned {burned:.1f} tokens (total: {self.total_burned:.1f})")
    
    def get_economic_indicators(self) -> Dict[str, Any]:
        summary = self.token_manager.get_system_summary()
        return {
            'total_supply': summary.get('total_balance', 0),
            'utilization': summary.get('system_efficiency', 0),
            'inflation_pressure': (summary.get('total_generated', 0) - summary.get('total_consumed', 1)) / max(summary.get('total_consumed', 1), 1),
            'current_generation_rate': self.current_generation_rate,
            'total_burned': self.total_burned,
            'target_utilization': self.target_utilization,
            'health': 'healthy' if 0.6 < summary.get('system_efficiency', 0) < 0.9 else 'unbalanced',
            'predicted_demand': self.predicted_demand,
            'prediction_confidence': self.prediction_confidence,
            'predictive_mode': self.enable_predictive
        }

# ============================================================================
# Predictive Token Allocator (Enhanced)
# ============================================================================

class PredictiveTokenAllocator:
    """Pre-allocates token batches based on ML demand prediction"""
    
    def __init__(self, token_manager: EcoATPTokenManager, prediction_horizon_seconds: float = 5.0,
                 enable_ml: bool = True):
        self.token_manager = token_manager
        self.prediction_horizon = prediction_horizon_seconds
        self.local_cache: Dict[str, float] = {}
        self.demand_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.cache_hits = 0
        self.cache_misses = 0
        self.pre_allocation_count = 0
        self.enable_ml = enable_ml
        
        # NEW: ML model integration
        self.ml_predictions: Dict[str, Dict] = {}
        
        asyncio.create_task(self._pre_allocation_loop())
        asyncio.create_task(self._ml_training_loop())
        
        logger.info(f"Predictive Token Allocator initialized: horizon={prediction_horizon_seconds}s, ml={enable_ml}")
    
    def record_demand(self, account_id: str, amount: float):
        self.demand_history[account_id].append({'amount': amount, 'timestamp': datetime.utcnow()})
        # Record for ML
        self.token_manager.ml_predictor.record_demand(account_id, amount, datetime.utcnow())
    
    def predict_demand(self, account_id: str) -> float:
        """Predict demand using ML if available, otherwise exponential smoothing"""
        if self.enable_ml:
            # Get ML prediction
            pred = asyncio.run(self.token_manager.ml_predictor.predict_demand(account_id))
            if pred.get('confidence', 0) > 0.5:
                self.ml_predictions[account_id] = pred
                return pred['predicted_amount']
        
        # Fallback to exponential smoothing
        history = list(self.demand_history.get(account_id, []))
        if len(history) < 5:
            return 10.0
        recent = [h['amount'] for h in history[-20:]]
        alpha = 0.3
        prediction = recent[0]
        for actual in recent[1:]:
            prediction = alpha * actual + (1 - alpha) * prediction
        return prediction * 1.2
    
    async def _ml_training_loop(self):
        """Background ML training loop"""
        while True:
            try:
                if self.enable_ml:
                    await self.token_manager.ml_predictor.train()
                await asyncio.sleep(600)  # Train every 10 minutes
            except Exception as e:
                logger.error(f"ML training loop error: {str(e)}")
                await asyncio.sleep(120)
    
    async def _pre_allocation_loop(self):
        while True:
            try:
                for account_id in list(self.demand_history.keys()):
                    predicted = self.predict_demand(account_id)
                    if predicted > 0:
                        success, _ = self.token_manager.reserve_tokens(
                            account_id=account_id, amount=predicted, consumer=EcoATPConsumer.EXPERT_EXECUTION)
                        if success:
                            self.local_cache[account_id] = self.local_cache.get(account_id, 0) + predicted
                            self.pre_allocation_count += 1
                await asyncio.sleep(self.prediction_horizon)
            except Exception as e:
                logger.error(f"Pre-allocation error: {str(e)}")
                await asyncio.sleep(10)
    
    def get_tokens(self, account_id: str, amount: float) -> Tuple[bool, float]:
        if self.local_cache.get(account_id, 0) >= amount:
            self.local_cache[account_id] -= amount
            self.cache_hits += 1
            return True, 0.0
        self.cache_misses += 1
        success, _ = self.token_manager.reserve_tokens(
            account_id=account_id, amount=amount, consumer=EcoATPConsumer.EXPERT_EXECUTION)
        return success, 1.0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        total = self.cache_hits + self.cache_misses
        return {
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': self.cache_hits / max(total, 1),
            'pre_allocations': self.pre_allocation_count,
            'active_accounts': len(self.demand_history),
            'total_cached_tokens': sum(self.local_cache.values()),
            'ml_enabled': self.enable_ml,
            'ml_predictions': len(self.ml_predictions)
        }
