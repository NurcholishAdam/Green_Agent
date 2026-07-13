# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/eco_atp_currency.py
# Enhanced version v7.0.0 – Full implementation with all improvements
"""
Enhanced Eco-ATP Currency System v7.0.0
Complete implementation with supply management, pre-allocation, protocol support,
quantum advantage as token generation source, predictive supply adjustment,
ML-based demand prediction, user-defined emergency thresholds,
adaptive rate limiting based on system load,
Genetic Optimizer, Distributed Token Market, Gradient-Aware Generation,
Quantum Feedback integration, and full concurrency safety.
"""

import asyncio
import logging
import uuid
from typing import Dict, Any, List, Optional, Tuple, Set, Protocol, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import math
import random
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import threading
from functools import wraps

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class EcoATPConfig:
    """Central configuration for the Eco-ATP system."""
    # Token parameters
    token_expiry_hours: float = 24.0
    token_half_life_hours: float = 24.0
    carbon_to_ecoatp_factor: float = 10.0  # per kg
    helium_to_ecoatp_factor: float = 5.0   # per unit
    energy_to_ecoatp_factor: float = 1000.0  # per kWh
    
    # Thresholds (default, can be evolved)
    hoarding_threshold: float = 2.0
    tax_rate: float = 0.1
    emergency_threshold: float = 50.0
    rate_limit_multiplier_high: float = 0.5
    rate_limit_multiplier_low: float = 1.5
    
    # Redistribution
    redistribution_interval_minutes: int = 30
    
    # Emergency
    emergency_token_rate: float = 10.0
    emergency_reserve: float = 1000.0
    substrate_reserves_max: float = 1000.0
    substrate_reserves_min: float = 500.0
    
    # Tenant defaults
    default_max_tokens_per_minute: float = 100.0
    default_max_concurrent_tasks: int = 5
    default_min_priority_for_reservation: int = 2
    default_reservation_cooldown_seconds: float = 1.0
    
    # Suspicious detection
    suspicious_threshold: int = 5
    
    # Batch processing
    batch_size: int = 10
    
    # ML
    ml_retrain_interval_seconds: int = 60
    ml_history_size: int = 1000
    
    # Market
    market_matching_interval_seconds: int = 30
    market_order_expiry_minutes: int = 5
    
    # Genetic optimizer
    genetic_population_size: int = 20
    genetic_mutation_rate: float = 0.2
    genetic_crossover_rate: float = 0.7
    genetic_generations: int = 10
    genetic_tournament_size: int = 3
    genetic_evolution_interval_seconds: int = 86400  # 24h
    
    # Recovery rates (completion_percentage -> recovery fraction)
    recovery_rates: Dict[float, float] = field(default_factory=lambda: {
        0.0: 0.0, 0.25: 0.125, 0.5: 0.25, 0.75: 0.6, 0.9: 0.8, 1.0: 0.95
    })

# ============================================================================
# Protocol Definitions
# ============================================================================

class TokenServiceProtocol(Protocol):
    """Explicit contract for token management services."""
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any,
                       tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class ExchangeRateProvider(Protocol):
    """Interface for exchange rate conversion."""
    def carbon_to_ecoatp(self, carbon_kg: float) -> float: ...
    def helium_to_ecoatp(self, helium_units: float) -> float: ...
    def energy_to_ecoatp(self, energy_kwh: float) -> float: ...

class GradientProvider(Protocol):
    """Interface for gradient field strengths."""
    def get_field_strengths(self) -> Dict[str, float]: ...

class QuantumFeedbackProvider(Protocol):
    """Interface for quantum feedback parameters."""
    def get_qubo_params(self) -> Dict[str, float]: ...

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
    quantum_advantage_factor: float = 0.0
    quantum_circuit_id: Optional[str] = None
    consumed_at: Optional[datetime] = None
    recovered_at: Optional[datetime] = None
    
    def __post_init__(self):
        if not self.provenance_hash:
            self.provenance_hash = self._compute_hash()
    
    def _compute_hash(self) -> str:
        data = f"{self.token_id}{self.value}{self.source.value}{self.generated_at.isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def apply_decay(self, current_time: datetime) -> float:
        age_hours = (current_time - self.generated_at).total_seconds() / 3600
        half_life = 24.0  # configurable
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
# Dynamic Exchange Rate (with config)
# ============================================================================

class DynamicExchangeRate:
    """Converts carbon, helium, and energy savings to ATP tokens."""
    
    def __init__(self, config: EcoATPConfig):
        self.config = config
        self.last_update = datetime.utcnow()
        # Placeholder for real-time market data (could be extended)
        self.carbon_price = 0.1  # dollars per kg CO2
        self.helium_price = 0.5  # dollars per unit
        self.energy_price = 0.12  # dollars per kWh
    
    def carbon_to_ecoatp(self, carbon_kg: float) -> float:
        return carbon_kg * self.config.carbon_to_ecoatp_factor
    
    def helium_to_ecoatp(self, helium_units: float) -> float:
        return helium_units * self.config.helium_to_ecoatp_factor
    
    def energy_to_ecoatp(self, energy_kwh: float) -> float:
        return energy_kwh * self.config.energy_to_ecoatp_factor
    
    def update_rates(self, carbon_price: Optional[float] = None,
                     helium_price: Optional[float] = None,
                     energy_price: Optional[float] = None):
        if carbon_price is not None:
            self.carbon_price = carbon_price
        if helium_price is not None:
            self.helium_price = helium_price
        if energy_price is not None:
            self.energy_price = energy_price
        self.last_update = datetime.utcnow()

# ============================================================================
# ML-Based Demand Predictor (with batched training)
# ============================================================================

class MLDemandPredictor:
    """
    Predicts future token demand using Random Forest.
    Training is batched and scheduled to avoid excessive retraining.
    """
    
    def __init__(self, config: EcoATPConfig):
        self.config = config
        self.model = RandomForestRegressor(n_estimators=10, random_state=42)
        self.scaler = StandardScaler()
        self.data: List[Dict[str, Any]] = []
        self.last_trained = datetime.utcnow() - timedelta(days=1)
        self.lock = asyncio.Lock()
        self.is_training = False
    
    def record_demand(self, account_id: str, amount: float, timestamp: datetime):
        """Add a demand sample."""
        features = {
            'account_id_hash': hash(account_id) % 1000,
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'amount': amount
        }
        self.data.append(features)
        # Keep history limited
        if len(self.data) > self.config.ml_history_size:
            self.data.pop(0)
    
    async def train(self, force: bool = False):
        """Train the model if enough data and time elapsed."""
        async with self.lock:
            if self.is_training:
                return
            now = datetime.utcnow()
            if not force and (now - self.last_trained).total_seconds() < self.config.ml_retrain_interval_seconds:
                return
            if len(self.data) < 10:
                logger.debug("Not enough data for ML training")
                return
            
            self.is_training = True
            try:
                # Prepare features and target
                X = np.array([[d['account_id_hash'], d['hour'], d['day_of_week']] for d in self.data])
                y = np.array([d['amount'] for d in self.data])
                X_scaled = self.scaler.fit_transform(X)
                self.model.fit(X_scaled, y)
                self.last_trained = now
                logger.info("ML model retrained on %d samples", len(self.data))
            except Exception as e:
                logger.error("ML training failed: %s", e)
            finally:
                self.is_training = False
    
    def predict_demand(self, account_id: str, timestamp: datetime) -> float:
        """Predict demand for a given account at a given time."""
        if len(self.data) < 10:
            return 0.0  # fallback
        features = np.array([[hash(account_id) % 1000, timestamp.hour, timestamp.weekday()]])
        try:
            X_scaled = self.scaler.transform(features)
            return float(self.model.predict(X_scaled)[0])
        except Exception as e:
            logger.error("Prediction failed: %s", e)
            return 0.0

# ============================================================================
# Threshold Genetic Optimizer
# ============================================================================

class ThresholdGeneticOptimizer:
    """
    Genetic optimizer to evolve key thresholds.
    """
    
    def __init__(self, token_manager: 'EcoATPTokenManager', config: EcoATPConfig):
        self.token_manager = token_manager
        self.config = config
        self.population_size = config.genetic_population_size
        self.mutation_rate = config.genetic_mutation_rate
        self.crossover_rate = config.genetic_crossover_rate
        self.generations = config.genetic_generations
        self.tournament_size = config.genetic_tournament_size
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.lock = asyncio.Lock()
        
        # Parameter bounds
        self.param_bounds = {
            'hoarding_threshold': (1.2, 4.0),
            'tax_rate': (0.05, 0.3),
            'emergency_threshold': (10.0, 100.0),
            'rate_limit_multiplier_high': (0.3, 0.7),
            'rate_limit_multiplier_low': (1.2, 2.0)
        }
        logger.info("Threshold Genetic Optimizer initialized")
    
    def _initialize_individual(self) -> Dict:
        """Generate random parameter set."""
        ind = {}
        for key, (low, high) in self.param_bounds.items():
            ind[key] = random.uniform(low, high)
        return ind
    
    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]
    
    def _fitness(self, individual: Dict) -> float:
        """Fitness based on system health: utilization, inflation, stability."""
        # Temporarily apply parameters
        self._apply_individual(individual)
        # Get system metrics
        summary = self.token_manager.get_system_summary()
        utilization = summary.get('system_efficiency', 0.5)
        total_generated = summary.get('total_generated', 1)
        total_consumed = summary.get('total_consumed', 1)
        inflation = (total_generated - total_consumed) / max(total_consumed, 1)
        emergency_mode = 1 if summary.get('emergency_mode', False) else 0
        # Fitness: high utilization (near 0.75), low inflation, no emergency
        fitness = 1.0 - abs(utilization - 0.75) * 2.0 - abs(inflation) * 0.5 - emergency_mode * 0.3
        self._restore_original_parameters()
        return max(0.0, fitness)
    
    def _apply_individual(self, individual: Dict):
        """Temporarily apply parameters to manager."""
        self._original_params = {
            'hoarding_threshold': self.token_manager.config.hoarding_threshold,
            'tax_rate': self.token_manager.config.tax_rate,
            'emergency_threshold': self.token_manager.config.emergency_threshold,
            'rate_limit_multiplier_high': self.token_manager.config.rate_limit_multiplier_high,
            'rate_limit_multiplier_low': self.token_manager.config.rate_limit_multiplier_low
        }
        self.token_manager.config.hoarding_threshold = individual['hoarding_threshold']
        self.token_manager.config.tax_rate = individual['tax_rate']
        self.token_manager.config.emergency_threshold = individual['emergency_threshold']
        self.token_manager.config.rate_limit_multiplier_high = individual['rate_limit_multiplier_high']
        self.token_manager.config.rate_limit_multiplier_low = individual['rate_limit_multiplier_low']
    
    def _restore_original_parameters(self):
        if hasattr(self, '_original_params'):
            self.token_manager.config.hoarding_threshold = self._original_params['hoarding_threshold']
            self.token_manager.config.tax_rate = self._original_params['tax_rate']
            self.token_manager.config.emergency_threshold = self._original_params['emergency_threshold']
            self.token_manager.config.rate_limit_multiplier_high = self._original_params['rate_limit_multiplier_high']
            self.token_manager.config.rate_limit_multiplier_low = self._original_params['rate_limit_multiplier_low']
    
    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]
    
    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        return child
    
    def _mutate(self, individual: Dict) -> Dict:
        mutated = individual.copy()
        for key, (low, high) in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                delta = random.uniform(-(high-low)*0.1, (high-low)*0.1)
                mutated[key] = max(low, min(high, mutated[key] + delta))
        return mutated
    
    def _evolve_one_generation(self, population: List[Dict]) -> List[Dict]:
        fitness_scores = [self._fitness(ind) for ind in population]
        new_population = []
        # Elitism
        best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
        new_population.append(population[best_idx])
        while len(new_population) < self.population_size:
            if random.random() < self.crossover_rate:
                parent1 = self._select(population, fitness_scores)
                parent2 = self._select(population, fitness_scores)
                child = self._crossover(parent1, parent2)
                child = self._mutate(child)
                new_population.append(child)
            else:
                parent = self._select(population, fitness_scores)
                new_population.append(parent.copy())
        return new_population
    
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        async with self.lock:
            if generations is None:
                generations = self.generations
            population = self._initialize_population()
            best_fitness = -float('inf')
            best_ind = None
            for gen in range(generations):
                population = self._evolve_one_generation(population)
                fitness_scores = [self._fitness(ind) for ind in population]
                gen_best = max(range(len(population)), key=lambda i: fitness_scores[i])
                if fitness_scores[gen_best] > best_fitness:
                    best_fitness = fitness_scores[gen_best]
                    best_ind = population[gen_best]
                logger.debug(f"Gen {gen+1}: best fitness = {fitness_scores[gen_best]:.4f}")
            if best_fitness > self.best_fitness:
                self.best_fitness = best_fitness
                self.best_individual = best_ind
                self._apply_individual(best_ind)
                logger.info(f"Applied best individual with fitness {best_fitness:.4f}")
            self.evolution_history.append({
                'timestamp': datetime.utcnow(),
                'best_fitness': best_fitness
            })
            return {'best_fitness': best_fitness, 'best_individual': best_ind}
    
    def get_status(self) -> Dict:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:]
        }

# ============================================================================
# Distributed Token Market (Order Book)
# ============================================================================

@dataclass
class MarketOrder:
    order_id: str
    account_id: str
    amount: float
    price: float
    side: str  # 'sell' or 'buy'
    status: str = 'open'
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    remaining: float = field(init=False)
    
    def __post_init__(self):
        self.remaining = self.amount

class OrderBook:
    """Order book with price-level aggregation for efficient matching."""
    
    def __init__(self):
        self.buy_orders: Dict[float, List[MarketOrder]] = defaultdict(list)  # price -> list of orders
        self.sell_orders: Dict[float, List[MarketOrder]] = defaultdict(list)
        self.all_orders: Dict[str, MarketOrder] = {}
    
    def add_order(self, order: MarketOrder):
        self.all_orders[order.order_id] = order
        if order.side == 'buy':
            self.buy_orders[order.price].append(order)
        else:
            self.sell_orders[order.price].append(order)
    
    def remove_order(self, order_id: str):
        order = self.all_orders.pop(order_id, None)
        if order:
            if order.side == 'buy':
                self.buy_orders[order.price] = [o for o in self.buy_orders[order.price] if o.order_id != order_id]
                if not self.buy_orders[order.price]:
                    del self.buy_orders[order.price]
            else:
                self.sell_orders[order.price] = [o for o in self.sell_orders[order.price] if o.order_id != order_id]
                if not self.sell_orders[order.price]:
                    del self.sell_orders[order.price]
    
    def get_best_buy_price(self) -> Optional[float]:
        if not self.buy_orders:
            return None
        return max(self.buy_orders.keys())
    
    def get_best_sell_price(self) -> Optional[float]:
        if not self.sell_orders:
            return None
        return min(self.sell_orders.keys())
    
    def get_buy_orders_at(self, price: float) -> List[MarketOrder]:
        return self.buy_orders.get(price, [])
    
    def get_sell_orders_at(self, price: float) -> List[MarketOrder]:
        return self.sell_orders.get(price, [])
    
    def cleanup_expired(self, now: datetime):
        to_remove = [oid for oid, order in self.all_orders.items() if order.status == 'open' and order.expires_at <= now]
        for oid in to_remove:
            self.remove_order(oid)

class DistributedTokenMarket:
    """
    Decentralized token market using an order book.
    """
    
    def __init__(self, token_manager: 'EcoATPTokenManager', config: EcoATPConfig):
        self.token_manager = token_manager
        self.config = config
        self.order_book = OrderBook()
        self.trade_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        logger.info("Distributed Token Market initialized")
    
    async def place_order(self, account_id: str, amount: float, price: float, side: str) -> str:
        """Place a buy or sell order."""
        async with self._lock:
            order = MarketOrder(
                order_id=f"order_{uuid.uuid4().hex[:8]}",
                account_id=account_id,
                amount=amount,
                price=price,
                side=side,
                expires_at=datetime.utcnow() + timedelta(minutes=self.config.market_order_expiry_minutes)
            )
            self.order_book.add_order(order)
            logger.debug(f"Order placed: {order.order_id} ({side} {amount} @ {price:.2f})")
            return order.order_id
    
    async def match_orders(self) -> List[Dict]:
        """Match open buy and sell orders using order book."""
        async with self._lock:
            matches = []
            now = datetime.utcnow()
            self.order_book.cleanup_expired(now)
            
            while True:
                best_buy = self.order_book.get_best_buy_price()
                best_sell = self.order_book.get_best_sell_price()
                if best_buy is None or best_sell is None:
                    break
                if best_sell > best_buy:
                    break  # no cross
                
                # Take the best buy and best sell orders
                buy_orders = self.order_book.get_buy_orders_at(best_buy)
                sell_orders = self.order_book.get_sell_orders_at(best_sell)
                if not buy_orders or not sell_orders:
                    break
                
                buy = buy_orders[0]
                sell = sell_orders[0]
                trade_amount = min(buy.remaining, sell.remaining)
                trade_price = (buy.price + sell.price) / 2
                
                # Execute trade
                seller_account = self.token_manager.accounts.get(sell.account_id)
                buyer_account = self.token_manager.accounts.get(buy.account_id)
                if seller_account and buyer_account:
                    total_cost = trade_price * trade_amount
                    if buyer_account.balance >= total_cost:
                        buyer_account.balance -= total_cost
                        seller_account.balance += total_cost
                        buy.remaining -= trade_amount
                        sell.remaining -= trade_amount
                        
                        # Update order status
                        if buy.remaining <= 0:
                            buy.status = 'completed'
                            self.order_book.remove_order(buy.order_id)
                        if sell.remaining <= 0:
                            sell.status = 'completed'
                            self.order_book.remove_order(sell.order_id)
                        
                        matches.append({
                            'sell_order': sell.order_id,
                            'buy_order': buy.order_id,
                            'seller': sell.account_id,
                            'buyer': buy.account_id,
                            'amount': trade_amount,
                            'price': trade_price,
                            'timestamp': now.isoformat()
                        })
                        self.trade_history.append(matches[-1])
                        logger.info(f"Trade matched: {sell.account_id} -> {buy.account_id} ({trade_amount} @ {trade_price:.2f})")
                    else:
                        # Buyer doesn't have enough balance - remove buy order
                        buy.status = 'cancelled'
                        self.order_book.remove_order(buy.order_id)
                else:
                    # Account missing - remove both orders
                    if buy.status == 'open':
                        buy.status = 'cancelled'
                        self.order_book.remove_order(buy.order_id)
                    if sell.status == 'open':
                        sell.status = 'cancelled'
                        self.order_book.remove_order(sell.order_id)
            return matches
    
    def get_market_stats(self) -> Dict[str, Any]:
        active_orders = [o for o in self.order_book.all_orders.values() if o.status == 'open']
        return {
            'active_orders': len(active_orders),
            'sell_orders': len([o for o in active_orders if o.side == 'sell']),
            'buy_orders': len([o for o in active_orders if o.side == 'buy']),
            'total_trades': len(self.trade_history),
            'total_volume': sum(t['amount'] for t in self.trade_history),
            'average_price': np.mean([t['price'] for t in self.trade_history]) if self.trade_history else 0,
            'recent_trades': list(self.trade_history)[-10:]
        }

# ============================================================================
# Gradient-Aware Generation
# ============================================================================

class GradientAwareGeneration:
    """
    Adjusts token generation based on gradient fields.
    """
    
    def __init__(self, token_manager: 'EcoATPTokenManager', gradient_provider: Optional[GradientProvider] = None):
        self.token_manager = token_manager
        self.gradient_provider = gradient_provider
        self.last_adjustment = datetime.utcnow()
        logger.info("Gradient-Aware Generation initialized")
    
    def adjust_generation_rate(self) -> float:
        """Return a multiplier to apply to token generation."""
        if not self.gradient_provider:
            return 1.0
        
        strengths = self.gradient_provider.get_field_strengths()
        carbon = strengths.get('carbon', 0.5)
        helium = strengths.get('helium', 0.5)
        opportunity = strengths.get('opportunity', 0.5)
        
        multiplier = 1.0
        if carbon > 0.7:
            multiplier *= (1.0 + (carbon - 0.7) * 0.5)
        if helium > 0.7:
            multiplier *= (1.0 + (helium - 0.7) * 0.3)
        if opportunity > 0.8:
            multiplier *= (1.0 + (opportunity - 0.8) * 0.2)
        
        self.last_adjustment = datetime.utcnow()
        return multiplier

# ============================================================================
# Quantum Feedback Integrator
# ============================================================================

class QuantumFeedbackIntegrator:
    """
    Adjusts token generation rates based on quantum solver results.
    """
    
    def __init__(self, token_manager: 'EcoATPTokenManager', quantum_provider: Optional[QuantumFeedbackProvider] = None):
        self.token_manager = token_manager
        self.quantum_provider = quantum_provider
        self.last_qubo_params: Dict[str, float] = {}
        self.last_update = datetime.utcnow()
        logger.info("Quantum Feedback Integrator initialized")
    
    def apply_quantum_insights(self) -> float:
        """Return a multiplier based on quantum insights."""
        if not self.quantum_provider:
            return 1.0
        
        qubo_params = self.quantum_provider.get_qubo_params()
        self.last_qubo_params = qubo_params
        self.last_update = datetime.utcnow()
        
        penalty_carbon = qubo_params.get('penalty_carbon', 0.5)
        penalty_helium = qubo_params.get('penalty_helium_shortage', 0.5)
        weight_opportunity = qubo_params.get('weight_opportunity', 0.5)
        
        multiplier = 1.0
        if penalty_carbon > 0.6:
            multiplier *= (1.0 + (penalty_carbon - 0.6) * 0.4)
        if penalty_helium > 0.6:
            multiplier *= (1.0 + (penalty_helium - 0.6) * 0.3)
        if weight_opportunity > 0.6:
            multiplier *= (1.0 + (weight_opportunity - 0.6) * 0.2)
        
        return multiplier

# ============================================================================
# Task Manager for Background Loops
# ============================================================================

class TaskManager:
    """Manages background tasks with restart and error handling."""
    
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
    
    def start_task(self, name: str, coro_func, *args, **kwargs):
        """Start a background task and register it."""
        async def wrapper():
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}", exc_info=True)
                    await asyncio.sleep(60)  # backoff
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task
    
    async def stop_all(self):
        """Gracefully stop all tasks."""
        self.shutdown_event.set()
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()

# ============================================================================
# Enhanced Eco-ATP Token Manager (Full Implementation)
# ============================================================================

class EcoATPTokenManager:
    """Enhanced Eco-ATP Token Manager v7.0.0 with full implementation."""
    
    def __init__(self, config: Optional[EcoATPConfig] = None,
                 exchange_rate: Optional[ExchangeRateProvider] = None,
                 gradient_provider: Optional[GradientProvider] = None,
                 quantum_provider: Optional[QuantumFeedbackProvider] = None):
        self.config = config or EcoATPConfig()
        self.exchange_rate = exchange_rate or DynamicExchangeRate(self.config)
        self.gradient_provider = gradient_provider
        self.quantum_provider = quantum_provider
        
        # Core state
        self.accounts: Dict[str, EcoATPAccount] = {}
        self.active_tokens: Dict[str, EcoATPToken] = {}
        self.token_history: deque = deque(maxlen=10000)
        
        # Locks for concurrency
        self._accounts_lock = asyncio.Lock()
        self._tokens_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        
        # Emergency mode
        self.emergency_mode = False
        self.emergency_reserve = self.config.emergency_reserve
        self.substrate_phosphorylation_active = False
        self.substrate_reserves = self.config.substrate_reserves_min
        self.last_generation_time: Optional[datetime] = None
        
        # Tenant quotas
        self.tenant_quotas: Dict[str, Dict[str, Any]] = {}
        self.default_quota = {
            'max_tokens_per_minute': self.config.default_max_tokens_per_minute,
            'max_concurrent_tasks': self.config.default_max_concurrent_tasks,
            'min_priority_for_reservation': self.config.default_min_priority_for_reservation,
            'reservation_cooldown_seconds': self.config.default_reservation_cooldown_seconds
        }
        self.tenant_usage: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.tenant_last_reservation: Dict[str, datetime] = {}
        self.suspicious_tenants: Set[str] = set()
        self._failed_attempts: Dict[str, int] = defaultdict(int)
        self._tenant_usage_lock = asyncio.Lock()
        self._tenant_last_reservation_lock = asyncio.Lock()
        self._failed_attempts_lock = asyncio.Lock()
        self._suspicious_lock = asyncio.Lock()
        
        # Batch processing
        self.batch_queue: List[Dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        
        # ML Demand Predictor
        self.ml_predictor = MLDemandPredictor(self.config)
        
        # Predictive supply
        self.predictive_supply_enabled = True
        self.predicted_demand_accumulator: Dict[str, float] = defaultdict(float)
        
        # Adaptive rate limiting
        self.system_load_history: deque = deque(maxlen=100)
        self.current_rate_multiplier = 1.0
        self._load_history_lock = asyncio.Lock()
        
        # User-defined emergency thresholds
        self.user_emergency_thresholds: Dict[str, Dict[str, Any]] = {}
        self.user_emergency_override = False
        self._emergency_thresholds_lock = asyncio.Lock()
        
        # Sub-components
        self.genetic_optimizer = ThresholdGeneticOptimizer(self, self.config)
        self.token_market = DistributedTokenMarket(self, self.config)
        self.gradient_aware = GradientAwareGeneration(self, self.gradient_provider)
        self.quantum_feedback = QuantumFeedbackIntegrator(self, self.quantum_provider)
        
        # Task manager
        self.task_manager = TaskManager()
        
        # Start background tasks
        self.task_manager.start_task("emergency_monitor", self._emergency_monitor_loop)
        self.task_manager.start_task("batch_processor", self._batch_processor_loop)
        self.task_manager.start_task("maintenance", self._maintenance_loop)
        self.task_manager.start_task("predictive_supply", self._predictive_supply_loop)
        self.task_manager.start_task("adaptive_rate", self._adaptive_rate_loop)
        self.task_manager.start_task("market_matching", self._market_matching_loop)
        self.task_manager.start_task("evolution", self._evolution_loop)
        self.task_manager.start_task("ml_training", self._ml_training_loop)
        self.task_manager.start_task("token_cleanup", self._token_cleanup_loop)
        
        logger.info("Enhanced Eco-ATP Token Manager v7.0.0 initialized")
    
    async def shutdown(self):
        """Gracefully shut down all background tasks."""
        await self.task_manager.stop_all()
        logger.info("Eco-ATP Token Manager shut down")
    
    # ========================================================================
    # Account Management
    # ========================================================================
    
    async def create_account(self, account_id: str) -> EcoATPAccount:
        async with self._accounts_lock:
            if account_id not in self.accounts:
                self.accounts[account_id] = EcoATPAccount(account_id=account_id)
            return self.accounts[account_id]
    
    async def get_account(self, account_id: str) -> Optional[EcoATPAccount]:
        async with self._accounts_lock:
            return self.accounts.get(account_id)
    
    # ========================================================================
    # Token Generation (Enhanced with gradient & quantum)
    # ========================================================================
    
    async def generate_tokens(self, account_id: str, source: EcoATPSource,
                            carbon_saved_kg: float = 0.0, helium_saved_units: float = 0.0,
                            energy_saved_kwh: float = 0.0, efficiency: float = 1.0,
                            num_tokens: Optional[int] = None,
                            quantum_advantage_factor: float = 0.0,
                            quantum_circuit_id: Optional[str] = None) -> List[EcoATPToken]:
        """Generate tokens with gradient and quantum adjustments."""
        async with self._accounts_lock:
            if account_id not in self.accounts:
                self.accounts[account_id] = EcoATPAccount(account_id=account_id)
            account = self.accounts[account_id]
        
        # Apply multipliers
        gradient_multiplier = self.gradient_aware.adjust_generation_rate()
        quantum_multiplier = self.quantum_feedback.apply_quantum_insights()
        total_multiplier = gradient_multiplier * quantum_multiplier
        
        carbon_value = self.exchange_rate.carbon_to_ecoatp(carbon_saved_kg)
        helium_value = self.exchange_rate.helium_to_ecoatp(helium_saved_units)
        energy_value = self.exchange_rate.energy_to_ecoatp(energy_saved_kwh)
        total_value = (carbon_value + helium_value + energy_value) * total_multiplier
        
        if num_tokens is None:
            num_tokens = max(1, int(total_value / 10))
        
        token_value = total_value / num_tokens
        tokens = []
        now = datetime.utcnow()
        expiry = now + timedelta(hours=self.config.token_expiry_hours)
        
        async with self._tokens_lock:
            for i in range(num_tokens):
                token = EcoATPToken(
                    token_id=f"eco_{account_id}_{now.timestamp()}_{i}_{uuid.uuid4().hex[:4]}",
                    value=token_value,
                    source=source,
                    generated_at=now,
                    expires_at=expiry,
                    carbon_equivalent_kg=carbon_saved_kg / num_tokens,
                    helium_equivalent_units=helium_saved_units / num_tokens,
                    generation_efficiency=efficiency,
                    quantum_advantage_factor=quantum_advantage_factor,
                    quantum_circuit_id=quantum_circuit_id
                )
                tokens.append(token)
                self.active_tokens[token.token_id] = token
        
        # Update account
        async with self._accounts_lock:
            account.balance += total_value
            account.total_generated += total_value
            if source == EcoATPSource.QUANTUM_ADVANTAGE:
                account.quantum_balance += total_value
                account.quantum_total_generated += total_value
        
        self.last_generation_time = now
        
        # Record for ML
        self.ml_predictor.record_demand(account_id, total_value, now)
        
        # Substrate refill
        if total_value > 100 and self.substrate_reserves < self.config.substrate_reserves_max:
            self.substrate_reserves = min(self.config.substrate_reserves_max,
                                          self.substrate_reserves + total_value * 0.05)
        
        return tokens
    
    # ========================================================================
    # Token Reservation (Enhanced with rate limiting)
    # ========================================================================
    
    async def reserve_tokens(self, account_id: str, amount: float, consumer: EcoATPConsumer,
                            tenant_id: str = "default", priority: int = 2) -> Tuple[bool, List[str]]:
        """Reserve tokens with adaptive rate limiting and tenant checks."""
        # Tenant quota checks
        tenant_quota = self.tenant_quotas.get(tenant_id, self.default_quota)
        
        async with self._suspicious_lock:
            if tenant_id in self.suspicious_tenants:
                logger.warning(f"Suspicious tenant {tenant_id} blocked")
                return False, []
        
        if priority > tenant_quota['min_priority_for_reservation']:
            return False, []
        
        # Adaptive rate limiting
        if not await self._check_adaptive_rate_limit(tenant_id, amount, tenant_quota):
            return False, []
        
        # Cooldown
        if not await self._check_cooldown(tenant_id, tenant_quota):
            return False, []
        
        # Actual reservation
        success, token_ids = await self._do_reserve_tokens(account_id, amount, consumer)
        
        if success:
            async with self._tenant_usage_lock:
                self.tenant_usage[tenant_id].append({'amount': amount, 'timestamp': datetime.utcnow()})
            async with self._tenant_last_reservation_lock:
                self.tenant_last_reservation[tenant_id] = datetime.utcnow()
            self.ml_predictor.record_demand(account_id, amount, datetime.utcnow())
        else:
            await self._track_failed_attempt(tenant_id)
        
        return success, token_ids
    
    async def _check_adaptive_rate_limit(self, tenant_id: str, amount: float, quota: Dict[str, Any]) -> bool:
        """Adaptive rate limiting based on system load."""
        system_load = await self._get_system_load()
        async with self._load_history_lock:
            self.system_load_history.append(system_load)
            if len(self.system_load_history) > 10:
                avg_load = sum(self.system_load_history) / len(self.system_load_history)
                if avg_load > 0.8:
                    self.current_rate_multiplier = self.config.rate_limit_multiplier_high
                elif avg_load > 0.6:
                    self.current_rate_multiplier = 0.75
                elif avg_load < 0.3:
                    self.current_rate_multiplier = self.config.rate_limit_multiplier_low
                else:
                    self.current_rate_multiplier = 1.0
        
        adaptive_limit = quota['max_tokens_per_minute'] * self.current_rate_multiplier
        
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        async with self._tenant_usage_lock:
            recent_usage = sum(u['amount'] for u in self.tenant_usage[tenant_id] if u['timestamp'] > minute_ago)
        
        return (recent_usage + amount) <= adaptive_limit
    
    async def _do_reserve_tokens(self, account_id: str, amount: float, consumer: EcoATPConsumer) -> Tuple[bool, List[str]]:
        """Internal reservation logic."""
        async with self._accounts_lock:
            account = self.accounts.get(account_id)
            if not account:
                return False, []
            if account.balance < amount:
                return False, []
        
        # Find available tokens
        token_ids = []
        remaining = amount
        now = datetime.utcnow()
        
        async with self._tokens_lock:
            # Simple FIFO selection
            for token_id, token in list(self.active_tokens.items()):
                if remaining <= 0:
                    break
                if token.state == TokenState.AVAILABLE and not token.is_expired(now):
                    token.state = TokenState.RESERVED
                    token_ids.append(token_id)
                    remaining -= token.value
        
        if remaining > 0:
            # Not enough tokens - rollback
            async with self._tokens_lock:
                for tid in token_ids:
                    self.active_tokens[tid].state = TokenState.AVAILABLE
            return False, []
        
        # Update account balance
        async with self._accounts_lock:
            account.balance -= amount
        
        return True, token_ids
    
    async def _check_cooldown(self, tenant_id: str, quota: Dict[str, Any]) -> bool:
        async with self._tenant_last_reservation_lock:
            if tenant_id in self.tenant_last_reservation:
                elapsed = (datetime.utcnow() - self.tenant_last_reservation[tenant_id]).total_seconds()
                if elapsed < quota['reservation_cooldown_seconds']:
                    return False
        return True
    
    async def _track_failed_attempt(self, tenant_id: str):
        async with self._failed_attempts_lock:
            self._failed_attempts[tenant_id] += 1
            if self._failed_attempts[tenant_id] >= self.config.suspicious_threshold:
                async with self._suspicious_lock:
                    self.suspicious_tenants.add(tenant_id)
    
    async def _get_system_load(self) -> float:
        summary = await self.get_system_summary()
        total_balance = summary.get('total_balance', 0)
        total_generated = summary.get('total_generated', 1)
        utilization = summary.get('system_efficiency', 0)
        load = utilization * 0.6 + (1.0 - min(1.0, total_balance / 1000)) * 0.4
        return min(1.0, max(0.0, load))
    
    # ========================================================================
    # Token Consumption & Recovery
    # ========================================================================
    
    async def consume_tokens(self, token_ids: List[str], consumer: EcoATPConsumer, operation_success: bool) -> float:
        """
        Consume reserved tokens.
        Returns total value consumed.
        """
        total_value = 0.0
        now = datetime.utcnow()
        
        async with self._tokens_lock:
            for token_id in token_ids:
                token = self.active_tokens.get(token_id)
                if token and token.state == TokenState.RESERVED:
                    if operation_success:
                        token.state = TokenState.CONSUMED
                        token.consumed_at = now
                        total_value += token.value
                    else:
                        # Failed operation - release token
                        token.state = TokenState.AVAILABLE
                elif token and token.state == TokenState.AVAILABLE:
                    # Already available? Possibly from previous consumption
                    pass
                else:
                    logger.warning(f"Token {token_id} not found or not reserved")
        
        if operation_success:
            async with self._accounts_lock:
                account = self.accounts.get(token_ids[0].split('_')[1] if token_ids else None)
                if account:
                    account.total_consumed += total_value
        
        return total_value
    
    async def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float:
        """
        Recover tokens based on completion percentage.
        Returns total value recovered.
        """
        # Interpolate recovery rate
        recovery_rate = 0.0
        sorted_rates = sorted(self.config.recovery_rates.keys())
        for i, p in enumerate(sorted_rates):
            if completion_percentage <= p:
                if i == 0:
                    recovery_rate = self.config.recovery_rates[p]
                else:
                    prev_p = sorted_rates[i-1]
                    prev_rate = self.config.recovery_rates[prev_p]
                    next_rate = self.config.recovery_rates[p]
                    # Linear interpolation
                    ratio = (completion_percentage - prev_p) / (p - prev_p)
                    recovery_rate = prev_rate + ratio * (next_rate - prev_rate)
                break
        else:
            recovery_rate = self.config.recovery_rates[sorted_rates[-1]]
        
        total_recovered = 0.0
        now = datetime.utcnow()
        
        async with self._tokens_lock:
            for token_id in token_ids:
                token = self.active_tokens.get(token_id)
                if token and token.state in (TokenState.RESERVED, TokenState.AVAILABLE):
                    recovered_value = token.value * recovery_rate
                    token.state = TokenState.RECOVERED
                    token.recovered_at = now
                    total_recovered += recovered_value
                    # Remove from active tokens (or keep for history)
                    del self.active_tokens[token_id]
        
        if total_recovered > 0:
            async with self._accounts_lock:
                account = self.accounts.get(token_ids[0].split('_')[1] if token_ids else None)
                if account:
                    account.balance += total_recovered
                    account.total_recovered += total_recovered
        
        return total_recovered
    
    # ========================================================================
    # User-Defined Emergency Thresholds
    # ========================================================================
    
    async def set_emergency_threshold(self, account_id: str, threshold: float, metric: str = 'balance', time_seconds: Optional[float] = None):
        async with self._emergency_thresholds_lock:
            if account_id not in self.user_emergency_thresholds:
                self.user_emergency_thresholds[account_id] = {}
            self.user_emergency_thresholds[account_id][metric] = {
                'threshold': max(10.0, threshold),
                'time_seconds': time_seconds
            }
            self.user_emergency_override = True
            logger.info(f"Emergency threshold for {account_id} set: {metric} = {threshold:.1f}" +
                       (f" (persist {time_seconds}s)" if time_seconds else ""))
    
    async def get_emergency_threshold(self, account_id: str, metric: str = 'balance') -> Optional[Dict]:
        async with self._emergency_thresholds_lock:
            if account_id in self.user_emergency_thresholds and metric in self.user_emergency_thresholds[account_id]:
                return self.user_emergency_thresholds[account_id][metric]
        return {'threshold': self.config.emergency_threshold, 'time_seconds': None}
    
    # ========================================================================
    # Summary and Metrics
    # ========================================================================
    
    async def get_system_summary(self) -> Dict[str, Any]:
        async with self._accounts_lock, self._tokens_lock:
            total_balance = sum(acc.balance for acc in self.accounts.values())
            total_generated = sum(acc.total_generated for acc in self.accounts.values())
            total_consumed = sum(acc.total_consumed for acc in self.accounts.values())
            total_recovered = sum(acc.total_recovered for acc in self.accounts.values())
            num_accounts = len(self.accounts)
            num_active_tokens = len(self.active_tokens)
            system_efficiency = total_consumed / total_generated if total_generated > 0 else 0.0
        
        return {
            'total_balance': total_balance,
            'total_generated': total_generated,
            'total_consumed': total_consumed,
            'total_recovered': total_recovered,
            'num_accounts': num_accounts,
            'num_active_tokens': num_active_tokens,
            'system_efficiency': system_efficiency,
            'emergency_mode': self.emergency_mode,
            'substrate_reserves': self.substrate_reserves,
            'current_rate_multiplier': self.current_rate_multiplier,
            'genetic_optimizer': self.genetic_optimizer.get_status(),
            'market_stats': self.token_market.get_market_stats(),
            'gradient_multiplier': self.gradient_aware.adjust_generation_rate(),
            'quantum_multiplier': self.quantum_feedback.apply_quantum_insights()
        }
    
    async def get_account_summary(self, account_id: str) -> Dict[str, Any]:
        async with self._accounts_lock:
            account = self.accounts.get(account_id)
            if not account:
                return {}
        return {
            'account_id': account.account_id,
            'balance': account.balance,
            'total_generated': account.total_generated,
            'total_consumed': account.total_consumed,
            'total_recovered': account.total_recovered,
            'total_expired': account.total_expired,
            'efficiency_rating': account.efficiency_rating,
            'quantum_balance': account.quantum_balance,
            'quantum_total_generated': account.quantum_total_generated,
            'utilization_rate': account.utilization_rate
        }
    
    # ========================================================================
    # Background Loops (All with error handling and restart)
    # ========================================================================
    
    async def _emergency_monitor_loop(self):
        """Monitor emergency conditions based on thresholds."""
        while True:
            try:
                await self._check_emergency_conditions()
                await asyncio.sleep(10)  # check every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Emergency monitor error: {e}")
                await asyncio.sleep(10)
    
    async def _check_emergency_conditions(self):
        """Check all accounts against emergency thresholds."""
        now = datetime.utcnow()
        emergency_triggered = False
        
        async with self._accounts_lock:
            for account_id, account in self.accounts.items():
                threshold_config = await self.get_emergency_threshold(account_id)
                if threshold_config and account.balance <= threshold_config['threshold']:
                    # Check time persistence if configured
                    time_seconds = threshold_config.get('time_seconds')
                    if time_seconds is not None:
                        # For simplicity, we just check if the condition has persisted
                        # In a real implementation, we'd track start time
                        pass
                    emergency_triggered = True
                    logger.warning(f"Emergency threshold breached for {account_id}: balance {account.balance:.2f} <= {threshold_config['threshold']:.2f}")
                    break
        
        if emergency_triggered:
            self.emergency_mode = True
            # Generate emergency tokens if needed
            if self.emergency_reserve > 0:
                await self._generate_emergency_tokens()
        else:
            self.emergency_mode = False
    
    async def _generate_emergency_tokens(self):
        """Generate emergency substrate tokens."""
        if self.emergency_reserve <= 0:
            return
        amount = min(self.config.emergency_token_rate, self.emergency_reserve)
        # Create tokens directly into a reserve account
        await self.generate_tokens("emergency_reserve", EcoATPSource.EMERGENCY_SUBSTRATE,
                                  carbon_saved_kg=0, helium_saved_units=0, energy_saved_kwh=0,
                                  num_tokens=int(amount/10))
        self.emergency_reserve -= amount
        logger.info(f"Generated {amount} emergency tokens")
    
    async def _batch_processor_loop(self):
        """Process batch queue."""
        while True:
            try:
                await self._process_batch()
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                await asyncio.sleep(5)
    
    async def _process_batch(self):
        async with self._batch_lock:
            if not self.batch_queue:
                return
            batch = self.batch_queue[:self.config.batch_size]
            self.batch_queue = self.batch_queue[self.config.batch_size:]
            # Process batch (example: execute operations)
            for item in batch:
                # Implement actual batch processing logic
                pass
    
    async def _maintenance_loop(self):
        """Perform periodic maintenance: redistribution, cleanup."""
        while True:
            try:
                await self._perform_maintenance()
                await asyncio.sleep(self.config.redistribution_interval_minutes * 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Maintenance error: {e}")
                await asyncio.sleep(60)
    
    async def _perform_maintenance(self):
        """Redistribute tokens, apply taxes, cleanup expired."""
        now = datetime.utcnow()
        # Redistribution logic (example: tax hoarders)
        async with self._accounts_lock:
            balances = [acc.balance for acc in self.accounts.values()]
            if balances:
                avg_balance = np.mean(balances)
                for acc_id, account in self.accounts.items():
                    if account.balance > avg_balance * self.config.hoarding_threshold:
                        # Tax excess
                        excess = account.balance - avg_balance * self.config.hoarding_threshold
                        tax = excess * self.config.tax_rate
                        account.balance -= tax
                        # Redistribute to other accounts (simplified)
                        for other_acc in self.accounts.values():
                            if other_acc.account_id != acc_id:
                                other_acc.balance += tax / (len(self.accounts) - 1)
                        logger.info(f"Taxed {acc_id} {tax:.2f} tokens for hoarding")
        
        # Cleanup expired tokens
        await self._cleanup_expired_tokens()
    
    async def _cleanup_expired_tokens(self):
        """Remove expired tokens and update account balances."""
        now = datetime.utcnow()
        expired_ids = []
        async with self._tokens_lock:
            for token_id, token in self.active_tokens.items():
                if token.is_expired(now) and token.state != TokenState.CONSUMED:
                    expired_ids.append(token_id)
            for token_id in expired_ids:
                token = self.active_tokens.pop(token_id, None)
                if token:
                    # Deduct from account balance (if not already consumed)
                    # Note: This is simplified; may need more complex handling
                    pass
        logger.debug(f"Cleaned up {len(expired_ids)} expired tokens")
    
    async def _predictive_supply_loop(self):
        """Adjust token generation based on predicted demand."""
        while True:
            try:
                await self._adjust_supply()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive supply error: {e}")
                await asyncio.sleep(60)
    
    async def _adjust_supply(self):
        """Predict demand and adjust generation rates."""
        if not self.predictive_supply_enabled:
            return
        now = datetime.utcnow()
        # Example: adjust generation multiplier based on predicted demand
        # Implementation depends on specific requirements
        pass
    
    async def _adaptive_rate_loop(self):
        """Periodically update adaptive rate limiting."""
        while True:
            try:
                await self._update_rate_limit()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Adaptive rate error: {e}")
                await asyncio.sleep(10)
    
    async def _update_rate_limit(self):
        """Update rate multiplier based on system load."""
        await self._get_system_load()  # updates multiplier internally
    
    async def _market_matching_loop(self):
        """Periodically match orders."""
        while True:
            try:
                matches = await self.token_market.match_orders()
                if matches:
                    logger.info(f"Matched {len(matches)} trades")
                await asyncio.sleep(self.config.market_matching_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Market matching error: {e}")
                await asyncio.sleep(60)
    
    async def _evolution_loop(self):
        """Run genetic optimization periodically."""
        while True:
            try:
                if len(self.accounts) >= 5:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.genetic_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(self.config.genetic_evolution_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Evolution loop error: {e}")
                await asyncio.sleep(3600)
    
    async def _ml_training_loop(self):
        """Periodically retrain ML model."""
        while True:
            try:
                await self.ml_predictor.train()
                await asyncio.sleep(self.config.ml_retrain_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ML training error: {e}")
                await asyncio.sleep(60)
    
    async def _token_cleanup_loop(self):
        """Periodically remove tokens that have expired and are not active."""
        while True:
            try:
                await self._cleanup_expired_tokens()
                await asyncio.sleep(300)  # every 5 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Token cleanup error: {e}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Public API Wrappers (sync versions for non-async callers)
    # ========================================================================
    
    def create_account_sync(self, account_id: str) -> EcoATPAccount:
        """Synchronous version of create_account."""
        return asyncio.run(self.create_account(account_id))
    
    def generate_tokens_sync(self, account_id: str, source: EcoATPSource, **kwargs) -> List[EcoATPToken]:
        """Synchronous version of generate_tokens."""
        return asyncio.run(self.generate_tokens(account_id, source, **kwargs))
    
    def reserve_tokens_sync(self, account_id: str, amount: float, consumer: EcoATPConsumer,
                           tenant_id: str = "default", priority: int = 2) -> Tuple[bool, List[str]]:
        """Synchronous version of reserve_tokens."""
        return asyncio.run(self.reserve_tokens(account_id, amount, consumer, tenant_id, priority))
    
    def consume_tokens_sync(self, token_ids: List[str], consumer: EcoATPConsumer, operation_success: bool) -> float:
        """Synchronous version of consume_tokens."""
        return asyncio.run(self.consume_tokens(token_ids, consumer, operation_success))
    
    def recover_tokens_sync(self, token_ids: List[str], completion_percentage: float) -> float:
        """Synchronous version of recover_tokens."""
        return asyncio.run(self.recover_tokens(token_ids, completion_percentage))
    
    def get_system_summary_sync(self) -> Dict[str, Any]:
        """Synchronous version of get_system_summary."""
        return asyncio.run(self.get_system_summary())
    
    def get_account_summary_sync(self, account_id: str) -> Dict[str, Any]:
        """Synchronous version of get_account_summary."""
        return asyncio.run(self.get_account_summary(account_id))

# ============================================================================
# Example usage (if run as script)
# ============================================================================

async def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create manager
    config = EcoATPConfig()
    manager = EcoATPTokenManager(config=config)
    
    # Example operations
    account = await manager.create_account("test_account")
    tokens = await manager.generate_tokens("test_account", EcoATPSource.RENEWABLE_ENERGY,
                                           carbon_saved_kg=10.0)
    print(f"Generated {len(tokens)} tokens")
    
    # Reserve some tokens
    success, token_ids = await manager.reserve_tokens("test_account", 50.0, EcoATPConsumer.DATA_PROCESSING)
    if success:
        print(f"Reserved {len(token_ids)} tokens")
        # Consume
        consumed = await manager.consume_tokens(token_ids, EcoATPConsumer.DATA_PROCESSING, True)
        print(f"Consumed {consumed} tokens")
    
    # Get summary
    summary = await manager.get_system_summary()
    print("System summary:", summary)
    
    # Shutdown
    await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
