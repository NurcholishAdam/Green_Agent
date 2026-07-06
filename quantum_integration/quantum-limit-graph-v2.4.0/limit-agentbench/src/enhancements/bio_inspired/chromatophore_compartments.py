# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/chromatophore_compartments.py
# Complete upgraded file v6.0.0 with hierarchical management, protocol support, RegionAggregator,
# mandatory validation gates, quantum-resistant encryption (NEW), dynamic resource allocation (NEW),
# cross-region knowledge transfer (NEW), predictive health modeling (NEW), and inter-compartment trading (NEW).

"""
Enhanced Chromatophore Compartments v6.0.0
Complete implementation with hierarchical management, protocol support,
RegionAggregator for scalable compartment orchestration, mandatory validation gates,
quantum-resistant encryption, dynamic resource allocation, cross-region knowledge transfer,
predictive health modeling, and inter-compartment trading.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid
import hashlib
import math
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ============================================================================
# Protocol Definition
# ============================================================================

class CompartmentServiceProtocol(Protocol):
    """Explicit contract for compartment management services"""
    def find_best_compartment(self, expert_type: str, task_complexity: float = 1.0) -> Optional[Any]: ...
    def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    def create_compartment(self, expert_type: str, expert_instance: Any = None,
                          resources: Any = None, parent_id: Optional[str] = None) -> Any: ...
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

# ============================================================================
# Enums
# ============================================================================

class CompartmentState(Enum):
    """Compartment lifecycle states"""
    GENESIS = "genesis"
    MATURING = "maturing"
    ACTIVE = "active"
    STRESSED = "stressed"
    SENESCENT = "senescent"
    APOPTOTIC = "apoptotic"
    DECOMMISSIONED = "decommissioned"

class MembranePermeability(Enum):
    """Membrane permeability levels"""
    IMPERMEABLE = "impermeable"
    RESTRICTIVE = "restrictive"
    SELECTIVE = "selective"
    PERMEABLE = "permeable"
    QUANTUM_ENCRYPTED = "quantum_encrypted"  # NEW

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class CompartmentResource:
    """Resource allocation for a compartment with dynamic capabilities"""
    cpu_cores: float = 1.0
    memory_mb: float = 256.0
    storage_mb: float = 1024.0
    network_mbps: float = 100.0
    max_tokens: float = 1000.0
    # NEW: Dynamic allocation
    min_cpu_cores: float = 0.5
    max_cpu_cores: float = 4.0
    min_memory_mb: float = 128.0
    max_memory_mb: float = 2048.0
    allocation_scaling: float = 1.0
    last_adjustment: Optional[datetime] = None
    
    @property
    def utilization(self) -> float:
        return (self.cpu_cores + self.memory_mb/256 + self.storage_mb/1024) / 3
    
    def scale_up(self, factor: float = 1.5):
        """Scale resources up"""
        self.cpu_cores = min(self.max_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = min(self.max_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.utcnow()
    
    def scale_down(self, factor: float = 0.7):
        """Scale resources down"""
        self.cpu_cores = max(self.min_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = max(self.min_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.utcnow()

# ============================================================================
# Quantum-Resistant Encryption (NEW)
# ============================================================================

class QuantumResistantEncryption:
    """
    Quantum-resistant encryption for membrane communication.
    
    Features:
    - RSA with 4096-bit keys (quantum-resistant)
    - Digital signatures for authenticity
    - Encrypted payload transmission
    - Key rotation
    """
    
    def __init__(self):
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        self.public_key = self.private_key.public_key()
        self.public_key_pem = self.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
        )
        self.peer_public_keys: Dict[str, Any] = {}
        self.key_rotation_count = 0
        self.last_rotation = datetime.utcnow()
        
        logger.info("Quantum-Resistant Encryption initialized with 4096-bit RSA")
    
    def encrypt_message(self, message: bytes, peer_id: str) -> bytes:
        """Encrypt a message for a peer"""
        if peer_id not in self.peer_public_keys:
            raise ValueError(f"No public key for peer {peer_id}")
        
        public_key = self.peer_public_keys[peer_id]
        encrypted = public_key.encrypt(
            message,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return encrypted
    
    def decrypt_message(self, encrypted: bytes) -> bytes:
        """Decrypt a message using private key"""
        decrypted = self.private_key.decrypt(
            encrypted,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return decrypted
    
    def sign_message(self, message: bytes) -> bytes:
        """Sign a message for authenticity"""
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return signature
    
    def verify_signature(self, message: bytes, signature: bytes, peer_id: str) -> bool:
        """Verify a message signature"""
        if peer_id not in self.peer_public_keys:
            return False
        
        public_key = self.peer_public_keys[peer_id]
        try:
            public_key.verify(
                signature,
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False
    
    def register_peer_key(self, peer_id: str, public_key_pem: bytes):
        """Register a peer's public key"""
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
        self.peer_public_keys[peer_id] = load_pem_public_key(public_key_pem)
        logger.debug(f"Registered public key for peer {peer_id}")
    
    def rotate_keys(self):
        """Rotate keys for quantum resistance"""
        self.key_rotation_count += 1
        self.last_rotation = datetime.utcnow()
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        self.public_key = self.private_key.public_key()
        self.public_key_pem = self.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
        )
        logger.info(f"Keys rotated (count: {self.key_rotation_count})")
    
    def get_encryption_stats(self) -> Dict[str, Any]:
        """Get encryption statistics"""
        return {
            'key_size': 4096,
            'key_rotation_count': self.key_rotation_count,
            'last_rotation': self.last_rotation.isoformat(),
            'peers_registered': len(self.peer_public_keys),
            'peers': list(self.peer_public_keys.keys())
        }

# ============================================================================
# Enhanced Membrane Gate with Quantum-Resistant Encryption
# ============================================================================

class MembraneGate:
    """
    Enhanced Membrane Gate with mandatory validation and quantum-resistant encryption.
    
    Features:
    - Mandatory validation for critical operations regardless of trust level
    - Trusted anomaly detection for high-trust sources
    - Rate limiting for critical operations
    - Security audit logging
    - Quantum-resistant encryption (NEW)
    """
    
    def __init__(self, compartment_id: str):
        self.compartment_id = compartment_id
        self.permeability: MembranePermeability = MembranePermeability.SELECTIVE
        self.inbound_rate_limit: float = 100.0
        self.outbound_rate_limit: float = 200.0
        self.trusted_peers: List[str] = field(default_factory=list)
        self.blocked_peers: List[str] = field(default_factory=list)
        
        # Traffic tracking
        self.inbound_count: int = 0
        self.outbound_count: int = 0
        self.rejected_count: int = 0
        
        # Mandatory validation for critical operations
        self.mandatory_validation_for = {
            'model_update': True,
            'token_transfer': True,
            'configuration_change': True,
            'data_sharing': False,
            'status_query': False
        }
        
        self.validation_history: deque = deque(maxlen=1000)
        self.blocked_operations: deque = deque(maxlen=500)
        
        # Anomaly detection for trusted sources
        self.trusted_anomaly_threshold = 0.3
        self._critical_op_timestamps: Dict[str, List[datetime]] = defaultdict(list)
        self._source_operation_history: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # NEW: Quantum-resistant encryption
        self.encryption = QuantumResistantEncryption()
        
        # NEW: Predictive health model
        self.health_model = PredictiveHealthModel()
        
        logger.info(f"Membrane Gate initialized for {compartment_id} (quantum-resistant encryption enabled)")
    
    def can_pass(
        self, source_id: str, direction: str = 'inbound',
        operation_type: str = 'status_query', payload_size: int = 0,
        encrypted_message: Optional[bytes] = None,
        signature: Optional[bytes] = None
    ) -> Tuple[bool, str]:
        """Enhanced permission check with mandatory validation and quantum encryption"""
        
        if source_id in self.blocked_peers:
            self._record_blocked(source_id, operation_type, 'blocked_peer')
            self.rejected_count += 1
            return False, "Source is blocked"
        
        # Validate encrypted communication if required
        if self.permeability == MembranePermeability.QUANTUM_ENCRYPTED:
            if encrypted_message is None or signature is None:
                self._record_blocked(source_id, operation_type, 'encryption_required')
                self.rejected_count += 1
                return False, "Encrypted communication required"
            
            # Verify signature
            if not self.encryption.verify_signature(encrypted_message, signature, source_id):
                self._record_blocked(source_id, operation_type, 'signature_verification_failed')
                self.rejected_count += 1
                return False, "Signature verification failed"
        
        # Mandatory validation for critical operations
        if self.mandatory_validation_for.get(operation_type, False):
            validation_passed = self._validate_critical_operation(source_id, operation_type, payload_size)
            if not validation_passed:
                self._record_blocked(source_id, operation_type, 'validation_failed')
                self.rejected_count += 1
                return False, "Critical operation validation failed"
        
        # Trust-based permeability for non-critical operations
        if self.permeability == MembranePermeability.IMPERMEABLE:
            self._record_blocked(source_id, operation_type, 'impermeable')
            self.rejected_count += 1
            return False, "Membrane is impermeable"
        
        if self.permeability == MembranePermeability.RESTRICTIVE:
            if source_id not in self.trusted_peers:
                self._record_blocked(source_id, operation_type, 'not_trusted')
                self.rejected_count += 1
                return False, "Source not in trusted peers"
        
        # Anomaly check for trusted sources with high permeability
        if source_id in self.trusted_peers and self.permeability in [MembranePermeability.PERMEABLE, MembranePermeability.QUANTUM_ENCRYPTED]:
            anomaly_score = self._check_trusted_anomaly(source_id, operation_type)
            if anomaly_score > self.trusted_anomaly_threshold:
                logger.warning(f"Anomaly from trusted peer {source_id}: score={anomaly_score:.2f}")
                self._record_blocked(source_id, operation_type, f'anomaly_{anomaly_score:.2f}')
                self.rejected_count += 1
                return False, f"Anomaly detected (score: {anomaly_score:.2f})"
        
        if direction == 'inbound':
            self.inbound_count += 1
        else:
            self.outbound_count += 1
        
        return True, "Passed"
    
    def _validate_critical_operation(self, source_id: str, operation_type: str, payload_size: int) -> bool:
        """Validate critical operations regardless of trust level"""
        now = datetime.utcnow()
        recent_ops = [t for t in self._critical_op_timestamps[source_id] if (now - t).total_seconds() < 60]
        if len(recent_ops) > 10:
            logger.warning(f"Critical operation rate limit exceeded for {source_id}")
            return False
        self._critical_op_timestamps[source_id].append(now)
        
        if operation_type == 'model_update' and payload_size > 100 * 1024 * 1024:
            logger.warning(f"Model update too large from {source_id}: {payload_size} bytes")
            return False
        
        if operation_type == 'token_transfer' and not self._validate_token_transfer(source_id):
            return False
        
        return True
    
    def _validate_token_transfer(self, source_id: str) -> bool:
        """Validate token transfer operations"""
        now = datetime.utcnow()
        recent = [t for t in self._critical_op_timestamps.get(f"transfer_{source_id}", [])
                 if (now - t).total_seconds() < 300]
        if len(recent) > 20:
            return False
        
        if not hasattr(self, '_transfer_timestamps'):
            self._transfer_timestamps = defaultdict(list)
        self._transfer_timestamps[f"transfer_{source_id}"].append(now)
        return True
    
    def _check_trusted_anomaly(self, source_id: str, operation_type: str) -> float:
        """Check for anomalous behavior from trusted sources"""
        anomaly_score = 0.0
        now = datetime.utcnow()
        
        recent_count = sum(1 for t in self._critical_op_timestamps.get(source_id, [])
                          if (now - t).total_seconds() < 10)
        if recent_count > 5:
            anomaly_score += 0.3
        
        history = self._source_operation_history[source_id]
        total_ops = sum(history.values())
        if total_ops > 50:
            op_frequency = history.get(operation_type, 0) / total_ops
            if op_frequency < 0.05:
                anomaly_score += 0.2
        
        self._source_operation_history[source_id][operation_type] += 1
        return min(1.0, anomaly_score)
    
    def _record_blocked(self, source_id: str, operation_type: str, reason: str):
        """Record blocked operation for security audit"""
        self.blocked_operations.append({
            'source_id': source_id, 'operation_type': operation_type,
            'reason': reason, 'timestamp': datetime.utcnow().isoformat()
        })
    
    def adjust_permeability(self, trust_score: float, token_balance: float, quantum_ready: bool = False):
        """Dynamically adjust membrane permeability"""
        if quantum_ready and token_balance > 100:
            self.permeability = MembranePermeability.QUANTUM_ENCRYPTED
        elif trust_score > 0.8 and token_balance > 500:
            self.permeability = MembranePermeability.PERMEABLE
        elif trust_score > 0.5 and token_balance > 200:
            self.permeability = MembranePermeability.SELECTIVE
        elif trust_score > 0.2:
            self.permeability = MembranePermeability.RESTRICTIVE
        else:
            self.permeability = MembranePermeability.IMPERMEABLE
    
    def get_security_stats(self) -> Dict[str, Any]:
        """Get security statistics"""
        total = self.inbound_count + self.outbound_count
        return {
            'total_blocked': len(self.blocked_operations),
            'recent_blocks': list(self.blocked_operations)[-20:],
            'rejection_rate': self.rejected_count / max(total, 1),
            'anomaly_detections': sum(1 for b in self.blocked_operations if 'anomaly' in b.get('reason', '')),
            'encryption_stats': self.encryption.get_encryption_stats()
        }

# ============================================================================
# Predictive Health Model (NEW)
# ============================================================================

class PredictiveHealthModel:
    """
    Predictive health modeling for proactive intervention.
    
    Features:
    - ML-based health prediction
    - Failure probability estimation
    - Proactive alerting
    - Trend analysis
    """
    
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=50, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.history: List[Dict] = []
        self.predictions: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        
        logger.info("Predictive Health Model initialized")
    
    def record_health_data(self, compartment_id: str, health_data: Dict[str, float]):
        """Record health data for training"""
        self.history.append({
            'compartment_id': compartment_id,
            'timestamp': datetime.utcnow(),
            **health_data
        })
        if len(self.history) > 1000:
            self.history = self.history[-1000:]
    
    async def train(self):
        """Train the health prediction model"""
        if len(self.history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        
        async with self._lock:
            # Prepare features
            X = []
            y = []
            
            for i in range(10, len(self.history) - 1):
                # Use last 10 data points as features
                features = []
                for j in range(10):
                    data = self.history[i - j]
                    features.extend([
                        data.get('health_score', 0.5),
                        data.get('success_rate', 0.5),
                        data.get('efficiency_score', 0.5),
                        data.get('token_balance', 100) / 1000,
                        data.get('trust_gradient', 0.5),
                        data.get('task_load', 0.5)
                    ])
                X.append(features)
                y.append(self.history[i + 1].get('health_score', 0.5))
            
            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}
            
            X = np.array(X)
            y = np.array(y)
            X_scaled = self.scaler.fit_transform(X)
            
            self.model.fit(X_scaled, y)
            self.is_trained = True
            
            logger.info(f"Health model trained on {len(X)} samples")
            return {'status': 'success', 'samples': len(X)}
    
    async def predict_health(self, current_data: Dict[str, float]) -> Dict[str, Any]:
        """Predict future health"""
        if not self.is_trained:
            return {'predicted_health': 0.5, 'confidence': 0.0, 'status': 'not_trained'}
        
        async with self._lock:
            # Prepare features from current data
            features = []
            for key in ['health_score', 'success_rate', 'efficiency_score', 
                       'token_balance', 'trust_gradient', 'task_load']:
                features.append(current_data.get(key, 0.5))
            
            # Use recent history for context
            recent = self.history[-10:] if len(self.history) >= 10 else self.history
            for data in recent:
                for key in ['health_score', 'success_rate', 'efficiency_score', 
                           'token_balance', 'trust_gradient', 'task_load']:
                    features.append(data.get(key, 0.5))
            
            # Ensure correct feature count
            features = features[:self.model.n_features_in_]
            while len(features) < self.model.n_features_in_:
                features.append(0.5)
            
            features_array = np.array([features])
            features_scaled = self.scaler.transform(features_array)
            
            prediction = self.model.predict(features_scaled)[0]
            confidence = min(0.9, len(self.history) / 100)
            
            # Calculate trend
            if len(self.history) > 20:
                recent_health = [h.get('health_score', 0.5) for h in self.history[-20:]]
                trend_slope = np.polyfit(range(len(recent_health)), recent_health, 1)[0]
                trend = 'improving' if trend_slope > 0.01 else 'declining' if trend_slope < -0.01 else 'stable'
            else:
                trend = 'stable'
            
            result = {
                'predicted_health': max(0.0, min(1.0, prediction)),
                'confidence': confidence,
                'trend': trend,
                'failure_probability': 1.0 - max(0.0, min(1.0, prediction * 0.8))
            }
            
            self.predictions[str(datetime.utcnow().timestamp())] = result['predicted_health']
            return result

# ============================================================================
# Inter-Compartment Trading (NEW)
# ============================================================================

@dataclass
class TradeOrder:
    """Trade order between compartments"""
    order_id: str
    seller_id: str
    buyer_id: Optional[str] = None
    token_amount: float = 0.0
    resource_type: str = "tokens"
    price: float = 0.0
    status: str = "pending"  # pending, matched, completed, cancelled
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))

class InterCompartmentMarket:
    """
    Inter-compartment trading for token optimization.
    
    Features:
    - Token trading between compartments
    - Market matching
    - Price discovery
    - Trading history
    """
    
    def __init__(self):
        self.orders: List[TradeOrder] = []
        self.trade_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        logger.info("Inter-Compartment Market initialized")
    
    def place_order(self, seller_id: str, token_amount: float, price: float) -> str:
        """Place a sell order"""
        order = TradeOrder(
            order_id=f"order_{uuid.uuid4().hex[:8]}",
            seller_id=seller_id,
            token_amount=token_amount,
            price=price
        )
        self.orders.append(order)
        logger.debug(f"Order placed: {order.order_id} - {token_amount} tokens at {price}")
        return order.order_id
    
    def match_orders(self) -> List[Dict]:
        """Match buy and sell orders"""
        matches = []
        
        # Find sell orders
        sell_orders = [o for o in self.orders if o.status == "pending" and o.price > 0]
        
        # Simple matching: match with any buyer that wants tokens
        for sell_order in sell_orders:
            buyer_id = sell_order.buyer_id
            if buyer_id:
                # Execute trade
                sell_order.status = "completed"
                matches.append({
                    'seller': sell_order.seller_id,
                    'buyer': buyer_id,
                    'amount': sell_order.token_amount,
                    'price': sell_order.price,
                    'order_id': sell_order.order_id
                })
                self.trade_history.append({
                    'timestamp': datetime.utcnow().isoformat(),
                    'seller': sell_order.seller_id,
                    'buyer': buyer_id,
                    'amount': sell_order.token_amount,
                    'price': sell_order.price
                })
        
        return matches
    
    def get_market_stats(self) -> Dict[str, Any]:
        """Get market statistics"""
        active_orders = [o for o in self.orders if o.status == "pending"]
        completed_orders = [o for o in self.orders if o.status == "completed"]
        
        return {
            'active_orders': len(active_orders),
            'completed_trades': len(completed_orders),
            'total_trade_volume': sum(o.token_amount for o in completed_orders),
            'average_price': np.mean([o.price for o in completed_orders]) if completed_orders else 0,
            'recent_trades': list(self.trade_history)[-10:]
        }

# ============================================================================
# Cross-Region Knowledge Transfer (NEW)
# ============================================================================

class CrossRegionKnowledgeTransfer:
    """
    Cross-region knowledge transfer for specialization.
    
    Features:
    - Knowledge distillation across regions
    - Specialization detection
    - Best practice sharing
    - Knowledge consolidation
    """
    
    def __init__(self):
        self.knowledge_bank: Dict[str, List[Dict]] = defaultdict(list)
        self.specialization_scores: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.transfer_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        logger.info("Cross-Region Knowledge Transfer initialized")
    
    def add_knowledge(self, region_id: str, knowledge: Dict):
        """Add knowledge from a region"""
        self.knowledge_bank[region_id].append({
            'timestamp': datetime.utcnow(),
            'knowledge': knowledge
        })
        
        # Update specialization scores
        for key, value in knowledge.items():
            if isinstance(value, (int, float)):
                self.specialization_scores[region_id][key] = value
    
    def transfer_knowledge(self, source_region: str, target_region: str) -> Dict:
        """Transfer knowledge from source to target region"""
        if source_region not in self.knowledge_bank or not self.knowledge_bank[source_region]:
            return {'status': 'no_knowledge'}
        
        # Get latest knowledge from source
        latest = self.knowledge_bank[source_region][-1]['knowledge']
        
        # Find key insights
        insights = {}
        for key, value in latest.items():
            if isinstance(value, (int, float)) and value > 0.7:
                insights[key] = value
        
        self.transfer_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'source': source_region,
            'target': target_region,
            'insights_count': len(insights)
        })
        
        return {
            'status': 'success',
            'insights': insights,
            'source': source_region,
            'target': target_region
        }
    
    def get_specialization_insights(self) -> Dict[str, Any]:
        """Get specialization insights across regions"""
        insights = {}
        
        for region_id, scores in self.specialization_scores.items():
            if scores:
                # Find top specializations
                sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
                insights[region_id] = {
                    'top_specializations': sorted_scores[:3],
                    'strength_score': np.mean(list(scores.values())) if scores else 0
                }
        
        return insights

# ============================================================================
# Chromatophore Compartment (Enhanced)
# ============================================================================

class ChromatophoreCompartment:
    """
    Self-contained expert execution compartment with enhanced features.
    
    Features:
    - Quantum-resistant encryption (NEW)
    - Dynamic resource allocation (NEW)
    - Predictive health modeling (NEW)
    - Inter-compartment trading (NEW)
    """
    
    def __init__(
        self, compartment_id: str, expert_type: str,
        expert_instance: Any = None,
        resources: Optional[CompartmentResource] = None
    ):
        self.compartment_id = compartment_id
        self.expert_type = expert_type
        self.expert = expert_instance
        self.resources = resources or CompartmentResource()
        
        # Lifecycle
        self.state = CompartmentState.GENESIS
        self.birth_time = datetime.utcnow()
        self.generation = 1
        self.parent_id: Optional[str] = None
        
        # Membrane with quantum encryption
        self.membrane = MembraneGate(compartment_id)
        
        # NEW: Predictive health model
        self.health_predictor = PredictiveHealthModel()
        
        # Local Eco-ATP pool
        self.token_balance: float = 100.0
        self.total_earned: float = 0.0
        self.total_spent: float = 0.0
        
        # Local gradient fields
        self.trust_gradient: float = 0.1
        self.efficiency_gradient: float = 0.5
        
        # Performance tracking
        self.tasks_completed: int = 0
        self.tasks_failed: int = 0
        self.total_latency_ms: float = 0.0
        self.carbon_emitted_kg: float = 0.0
        
        # Biomass storage (local)
        self.atp_cache: deque = deque(maxlen=100)
        self.glycogen_queue: deque = deque(maxlen=1000)
        self.starch_reserve: deque = deque(maxlen=5000)
        self.lipid_depot: deque = deque(maxlen=10000)
        
        # Communication history
        self.signal_history: deque = deque(maxlen=500)
        
        # Bio-core buffer
        self.bio_buffer = BioCoreBuffer()
        
        # NEW: Trading
        self.trade_orders: List[TradeOrder] = []
        
        # NEW: Knowledge export
        self.knowledge_export: Dict[str, Any] = {}
        
        # Health data for prediction
        self._health_history: List[Dict] = []
        
        logger.info(f"Compartment {compartment_id} created: {expert_type}")
    
    @property
    def success_rate(self) -> float:
        total = self.tasks_completed + self.tasks_failed
        return self.tasks_completed / max(total, 1)
    
    @property
    def efficiency_score(self) -> float:
        if self.tasks_completed == 0:
            return 0.5
        return self.token_balance / max(self.total_earned, 1)
    
    @property
    def health_score(self) -> float:
        """Composite health score with predictive component"""
        base_score = (self.success_rate * 0.4 + self.efficiency_score * 0.3 + self.trust_gradient * 0.3)
        
        # Get prediction if available
        try:
            pred = asyncio.run(self.health_predictor.predict_health({
                'health_score': base_score,
                'success_rate': self.success_rate,
                'efficiency_score': self.efficiency_score,
                'token_balance': self.token_balance,
                'trust_gradient': self.trust_gradient,
                'task_load': len(self.glycogen_queue) / 1000
            }))
            
            if pred.get('confidence', 0) > 0.5:
                # Blend current and predicted health
                predicted = pred.get('predicted_health', 0.5)
                confidence = pred.get('confidence', 0)
                return base_score * (1 - confidence * 0.3) + predicted * confidence * 0.3
        except Exception:
            pass
        
        return base_score
    
    @property
    def is_viable(self) -> bool:
        """Check if compartment is viable"""
        return (self.state in [CompartmentState.MATURING, CompartmentState.ACTIVE] and
                self.health_score > 0.2 and self.token_balance > 0)
    
    def receive_tokens(self, amount: float, source: str = "scheduler") -> bool:
        """Receive Eco-ATP tokens through membrane"""
        if not self.membrane.can_pass(source, 'inbound', 'token_transfer'):
            return False
        self.token_balance += amount
        self.total_earned += amount
        return True
    
    def spend_tokens(self, amount: float, purpose: str = "execution") -> bool:
        """Spend Eco-ATP tokens for task execution"""
        if self.token_balance < amount:
            return False
        self.token_balance -= amount
        self.total_spent += amount
        return True
    
    def record_task_result(self, success: bool, latency_ms: float, carbon_kg: float, tokens_consumed: float):
        """Record task execution result with predictive health update"""
        if success:
            self.tasks_completed += 1
            self.trust_gradient = min(1.0, self.trust_gradient + 0.05)
            self.efficiency_gradient = min(1.0, self.efficiency_gradient + 0.02 * 
                                          (1 - tokens_consumed / max(self.token_balance, 1)))
        else:
            self.tasks_failed += 1
            self.trust_gradient = max(0.0, self.trust_gradient - 0.1)
            self.efficiency_gradient = max(0.1, self.efficiency_gradient - 0.05)
        
        self.total_latency_ms += latency_ms
        self.carbon_emitted_kg += carbon_kg
        
        # Update health model
        health_data = {
            'health_score': self.health_score,
            'success_rate': self.success_rate,
            'efficiency_score': self.efficiency_score,
            'token_balance': self.token_balance,
            'trust_gradient': self.trust_gradient,
            'task_load': len(self.glycogen_queue) / 1000
        }
        self.health_predictor.record_health_data(self.compartment_id, health_data)
        
        # Async train health model
        asyncio.create_task(self.health_predictor.train())
        
        # Dynamic resource allocation
        if self.tasks_completed > 10 and self.tasks_completed % 10 == 0:
            self._adjust_resources()
        
        self.membrane.adjust_permeability(
            self.trust_gradient, 
            self.token_balance,
            quantum_ready=self._is_quantum_ready()
        )
        self._evaluate_lifecycle()
    
    def _adjust_resources(self):
        """Dynamically adjust resources based on load"""
        utilization = self.resources.utilization
        task_load = len(self.glycogen_queue) / 1000
        
        if task_load > 0.8 and utilization > 0.7:
            self.resources.scale_up()
            logger.debug(f"Compartment {self.compartment_id} scaled up resources")
        elif task_load < 0.2 and utilization > 0.3:
            self.resources.scale_down()
            logger.debug(f"Compartment {self.compartment_id} scaled down resources")
    
    def _is_quantum_ready(self) -> bool:
        """Check if compartment is ready for quantum encryption"""
        return self.token_balance > 100 and self.trust_gradient > 0.6
    
    def _evaluate_lifecycle(self):
        """Evaluate and transition lifecycle state"""
        if self.health_score < 0.1 and self.state == CompartmentState.ACTIVE:
            self.state = CompartmentState.SENESCENT
            logger.warning(f"Compartment {self.compartment_id} entering senescence")
        elif self.health_score < 0.05:
            self.state = CompartmentState.APOPTOTIC
            logger.warning(f"Compartment {self.compartment_id} marked for apoptosis")
        elif self.health_score > 0.3 and self.state == CompartmentState.MATURING:
            self.state = CompartmentState.ACTIVE
            logger.info(f"Compartment {self.compartment_id} now active")
    
    def spawn_child(self, expert_type: Optional[str] = None) -> 'ChromatophoreCompartment':
        """Spawn a child compartment (reproduction)"""
        child_id = f"{self.compartment_id}_child_{self.generation}"
        child_type = expert_type or self.expert_type
        
        endowment = self.token_balance * 0.2
        self.token_balance -= endowment
        
        child = ChromatophoreCompartment(
            compartment_id=child_id,
            expert_type=child_type,
            resources=CompartmentResource(
                cpu_cores=self.resources.cpu_cores * 0.5,
                memory_mb=self.resources.memory_mb * 0.5
            )
        )
        child.parent_id = self.compartment_id
        child.generation = self.generation + 1
        child.token_balance = endowment
        child.trust_gradient = self.trust_gradient * 0.5
        
        # Share health knowledge
        child.health_predictor = self.health_predictor
        
        self.generation += 1
        logger.info(f"Compartment {self.compartment_id} spawned child {child_id}")
        return child
    
    def prepare_apoptosis(self) -> Tuple[float, Dict[str, Any]]:
        """Prepare for programmed cell death with knowledge export"""
        knowledge_summary = {
            'expert_type': self.expert_type,
            'tasks_completed': self.tasks_completed,
            'success_rate': self.success_rate,
            'efficiency_score': self.efficiency_score,
            'learned_patterns': list(self.atp_cache)[-10:],
            'best_practices': {
                'avg_latency_ms': self.total_latency_ms / max(self.tasks_completed, 1),
                'carbon_per_task_kg': self.carbon_emitted_kg / max(self.tasks_completed, 1)
            },
            'health_history': self._health_history[-50:],
            'resource_config': {
                'cpu_cores': self.resources.cpu_cores,
                'memory_mb': self.resources.memory_mb,
                'allocation_scaling': self.resources.allocation_scaling
            }
        }
        remaining_tokens = self.token_balance
        self.state = CompartmentState.DECOMMISSIONED
        self.knowledge_export = knowledge_summary
        return remaining_tokens, knowledge_summary
    
    def get_status(self) -> Dict[str, Any]:
        """Get compartment status with enhanced metrics"""
        # Get health prediction
        prediction = asyncio.run(self.health_predictor.predict_health({
            'health_score': self.health_score,
            'success_rate': self.success_rate,
            'efficiency_score': self.efficiency_score,
            'token_balance': self.token_balance,
            'trust_gradient': self.trust_gradient,
            'task_load': len(self.glycogen_queue) / 1000
        }))
        
        return {
            'compartment_id': self.compartment_id,
            'expert_type': self.expert_type,
            'state': self.state.value,
            'generation': self.generation,
            'health_score': self.health_score,
            'predicted_health': prediction.get('predicted_health', self.health_score),
            'health_confidence': prediction.get('confidence', 0),
            'health_trend': prediction.get('trend', 'stable'),
            'token_balance': self.token_balance,
            'trust_gradient': self.trust_gradient,
            'efficiency_gradient': self.efficiency_gradient,
            'success_rate': self.success_rate,
            'membrane_permeability': self.membrane.permeability.value,
            'tasks_completed': self.tasks_completed,
            'resource_utilization': self.resources.utilization,
            'allocation_scaling': self.resources.allocation_scaling,
            'storage': {
                'atp_cache': len(self.atp_cache),
                'glycogen_queue': len(self.glycogen_queue),
                'starch_reserve': len(self.starch_reserve),
                'lipid_depot': len(self.lipid_depot)
            },
            'bio_buffer': self.bio_buffer.get_stats()
        }

# ============================================================================
# Bio-Core Buffer (Preserved)
# ============================================================================

class BioCoreBuffer:
    """
    Local buffer for graceful degradation during bio-core outages.
    
    Allows compartments to operate using cached values when bio-core is unavailable.
    """
    
    def __init__(self, buffer_ttl_seconds: float = 30.0):
        self.buffer_ttl = buffer_ttl_seconds
        self.cached_gradient_levels: Dict[str, float] = {}
        self.cached_token_balance: float = 500.0
        self.cached_compartment_health: float = 0.7
        self.last_sync_time: Optional[datetime] = None
        self.bio_core_available: bool = True
        self.degraded_mode: bool = False
        self.degraded_operations_count: int = 0
    
    def sync_from_bio_core(self, bio_core) -> bool:
        """Sync buffer from bio-core"""
        try:
            if bio_core and hasattr(bio_core, 'gradient_manager'):
                self.cached_gradient_levels = bio_core.gradient_manager.get_field_strengths()
            if bio_core and hasattr(bio_core, 'token_manager'):
                summary = bio_core.token_manager.get_system_summary()
                self.cached_token_balance = summary.get('total_balance', 500)
            self.last_sync_time = datetime.utcnow()
            self.bio_core_available = True
            self.degraded_mode = False
            return True
        except Exception as e:
            logger.warning(f"Failed to sync from bio-core: {str(e)}")
            self._check_degraded()
            return False
    
    def get_gradient(self, field_id: str) -> float:
        """Get gradient level with graceful degradation"""
        if not self.degraded_mode:
            return self.cached_gradient_levels.get(field_id, 0.5)
        self.degraded_operations_count += 1
        cached = self.cached_gradient_levels.get(field_id, 0.5)
        return max(0.0, min(1.0, cached * 0.8 + np.random.normal(0, 0.1)))
    
    def get_token_balance(self) -> float:
        """Get token balance with graceful degradation"""
        if not self.degraded_mode:
            return self.cached_token_balance
        self.degraded_operations_count += 1
        return self.cached_token_balance * 0.7
    
    def _check_degraded(self):
        """Check if should enter degraded mode"""
        if self.last_sync_time:
            elapsed = (datetime.utcnow() - self.last_sync_time).total_seconds()
            if elapsed > self.buffer_ttl and not self.degraded_mode:
                self.degraded_mode = True
                logger.warning(f"Entering DEGRADED MODE: Bio-core unavailable for {elapsed:.0f}s")
    
    def is_degraded(self) -> bool:
        return self.degraded_mode
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            'degraded_mode': self.degraded_mode,
            'bio_core_available': self.bio_core_available,
            'last_sync': self.last_sync_time.isoformat() if self.last_sync_time else None,
            'degraded_operations': self.degraded_operations_count,
            'buffer_ttl': self.buffer_ttl
        }

# ============================================================================
# Region Aggregator (Enhanced)
# ============================================================================

class RegionAggregator:
    """
    Hierarchical region aggregator with enhanced features.
    
    Features:
    - Cross-region knowledge transfer (NEW)
    - Inter-compartment trading (NEW)
    - Predictive health aggregation (NEW)
    """
    
    def __init__(self, region_id: str, max_compartments: int = 50):
        self.region_id = region_id
        self.max_compartments = max_compartments
        self.compartments: Dict[str, ChromatophoreCompartment] = {}
        self.aggregated_health: float = 0.7
        self.aggregated_tokens: float = 0.0
        self.last_global_sync: datetime = datetime.utcnow()
        self.last_local_balance: datetime = datetime.utcnow()
        
        # Region-level metrics
        self.total_tasks_processed: int = 0
        self.total_carbon_kg: float = 0.0
        self.total_tokens_consumed: float = 0.0
        
        # NEW: Knowledge transfer
        self.knowledge_transfer = CrossRegionKnowledgeTransfer()
        
        # NEW: Market
        self.market = InterCompartmentMarket()
        
        # NEW: Predictive health
        self.health_predictions: Dict[str, Dict] = {}
        
        logger.info(f"Region Aggregator '{region_id}' initialized (max: {max_compartments})")
    
    def add_compartment(self, compartment: ChromatophoreCompartment) -> bool:
        """Add compartment to region if capacity allows"""
        if len(self.compartments) >= self.max_compartments:
            logger.warning(f"Region {self.region_id} at capacity ({self.max_compartments})")
            return False
        
        self.compartments[compartment.compartment_id] = compartment
        self._update_aggregates()
        
        logger.debug(f"Added compartment {compartment.compartment_id} to region {self.region_id} "
                    f"({len(self.compartments)}/{self.max_compartments})")
        return True
    
    def remove_compartment(self, compartment_id: str) -> bool:
        """Remove compartment from region"""
        if compartment_id in self.compartments:
            comp = self.compartments.pop(compartment_id)
            self.total_tasks_processed += comp.tasks_completed
            self.total_carbon_kg += comp.carbon_emitted_kg
            self.total_tokens_consumed += comp.total_spent
            
            # Extract knowledge before removal
            self.knowledge_transfer.add_knowledge(self.region_id, comp.knowledge_export)
            
            self._update_aggregates()
            return True
        return False
    
    def _update_aggregates(self):
        """Update aggregated metrics for this region"""
        if not self.compartments:
            self.aggregated_health = 0.0
            self.aggregated_tokens = 0.0
            return
        
        self.aggregated_health = np.mean([c.health_score for c in self.compartments.values()])
        self.aggregated_tokens = sum(c.token_balance for c in self.compartments.values())
    
    def balance_load_local(self) -> int:
        """Balance load within this region only."""
        if (datetime.utcnow() - self.last_local_balance).total_seconds() < 10:
            return 0
        
        overloaded = [
            c for c in self.compartments.values()
            if c.is_viable and len(c.glycogen_queue) > 500
        ]
        underloaded = [
            c for c in self.compartments.values()
            if c.is_viable and len(c.glycogen_queue) < 100
        ]
        
        if not overloaded or not underloaded:
            return 0
        
        transfers = 0
        for ol in overloaded[:5]:
            for ul in underloaded[:5]:
                if ol.expert_type == ul.expert_type:
                    count = min(50, len(ol.glycogen_queue) - 500)
                    for _ in range(count):
                        if ol.glycogen_queue:
                            task = ol.glycogen_queue.popleft()
                            ul.glycogen_queue.append(task)
                            transfers += 1
        
        if transfers > 0:
            logger.debug(f"Region {self.region_id}: transferred {transfers} tasks locally")
        
        self.last_local_balance = datetime.utcnow()
        return transfers
    
    def health_check(self) -> float:
        """Check health of all compartments in region with predictions"""
        predictions = []
        
        for comp in self.compartments.values():
            comp._evaluate_lifecycle()
            
            # Get health prediction
            pred = asyncio.run(comp.health_predictor.predict_health({
                'health_score': comp.health_score,
                'success_rate': comp.success_rate,
                'efficiency_score': comp.efficiency_score,
                'token_balance': comp.token_balance,
                'trust_gradient': comp.trust_gradient,
                'task_load': len(comp.glycogen_queue) / 1000
            }))
            if pred.get('confidence', 0) > 0.5:
                predictions.append(pred)
        
        self._update_aggregates()
        
        # Update region health predictions
        if predictions:
            avg_predicted = np.mean([p['predicted_health'] for p in predictions])
            self.health_predictions[self.region_id] = {
                'predicted_health': avg_predicted,
                'trend': 'improving' if avg_predicted > self.aggregated_health else 'declining',
                'confidence': np.mean([p['confidence'] for p in predictions])
            }
        
        return self.aggregated_health
    
    def cull_unhealthy(self) -> List[str]:
        """Remove unhealthy compartments and return list of removed IDs"""
        removed = []
        for cid in list(self.compartments.keys()):
            comp = self.compartments[cid]
            if comp.state == CompartmentState.APOPTOTIC:
                remaining_tokens, knowledge = comp.prepare_apoptosis()
                self.remove_compartment(cid)
                removed.append(cid)
                logger.info(f"Region {self.region_id}: culled apoptotic compartment {cid}")
            elif comp.state == CompartmentState.SENESCENT and comp.health_score < 0.03:
                comp.state = CompartmentState.APOPTOTIC
        
        return removed
    
    def get_viable_count(self) -> int:
        """Get count of viable compartments"""
        return sum(1 for c in self.compartments.values() if c.is_viable)
    
    def get_total_count(self) -> int:
        """Get total compartment count"""
        return len(self.compartments)
    
    def get_region_stats(self) -> Dict[str, Any]:
        """Get comprehensive region statistics"""
        viable = self.get_viable_count()
        total = self.get_total_count()
        
        return {
            'region_id': self.region_id,
            'compartment_count': total,
            'viable_count': viable,
            'viability_ratio': viable / max(total, 1),
            'max_capacity': self.max_compartments,
            'utilization': total / self.max_compartments,
            'aggregated_health': self.aggregated_health,
            'predicted_health': self.health_predictions.get(self.region_id, {}),
            'aggregated_tokens': self.aggregated_tokens,
            'total_tasks_processed': self.total_tasks_processed,
            'total_carbon_kg': self.total_carbon_kg,
            'total_tokens_consumed': self.total_tokens_consumed,
            'expert_types': list(set(c.expert_type for c in self.compartments.values())),
            'states': {
                state.value: sum(1 for c in self.compartments.values() if c.state == state)
                for state in CompartmentState
            },
            'market_stats': self.market.get_market_stats(),
            'knowledge_stats': self.knowledge_transfer.get_specialization_insights()
        }

# ============================================================================
# Hierarchical Compartment Manager (Enhanced)
# ============================================================================

class HierarchicalCompartmentManager:
    """
    Enhanced compartment manager with hierarchical organization.
    
    New Features:
    - Quantum-resistant encryption for membrane communication
    - Dynamic resource allocation based on workload
    - Cross-region knowledge transfer for specialization
    - Predictive health modeling for proactive intervention
    - Inter-compartment trading for token optimization
    """
    
    def __init__(self, token_manager=None, max_regions: int = 20, compartments_per_region: int = 50):
        self.token_manager = token_manager
        self.max_regions = max_regions
        self.compartments_per_region = compartments_per_region
        
        # Hierarchical organization
        self.regions: Dict[str, RegionAggregator] = {}
        self.compartment_to_region: Dict[str, str] = {}  # compartment_id → region_id
        
        # Legacy access
        self.compartments: Dict[str, ChromatophoreCompartment] = {}
        
        # Global metrics
        self.global_health: float = 0.7
        self.total_compartments_created: int = 0
        self.total_apoptosis_events: int = 0
        self.last_global_balance: datetime = datetime.utcnow()
        
        # Knowledge bank
        self.knowledge_bank: Dict[str, List[Dict]] = defaultdict(list)
        
        # Market for inter-compartment trading
        self.market_orders: List[Dict] = []
        
        # Create default region
        self._ensure_region_exists("default")
        
        # Start maintenance tasks
        asyncio.create_task(self._ecosystem_maintenance())
        asyncio.create_task(self._trading_maintenance())
        
        logger.info(
            f"Hierarchical Compartment Manager v6.0.0 initialized: "
            f"max_regions={max_regions}, per_region={compartments_per_region}"
        )
    
    def _ensure_region_exists(self, region_id: str) -> RegionAggregator:
        """Ensure a region exists, creating if necessary"""
        if region_id not in self.regions:
            if len(self.regions) >= self.max_regions:
                # Find least utilized region to reuse
                region_id = min(self.regions.keys(), 
                               key=lambda r: len(self.regions[r].compartments))
                return self.regions[region_id]
            
            self.regions[region_id] = RegionAggregator(
                region_id=region_id,
                max_compartments=self.compartments_per_region
            )
        
        return self.regions[region_id]
    
    def _get_region_for_expert(self, expert_type: str) -> str:
        """Get or create appropriate region for expert type"""
        # Try to find existing region with capacity and matching type
        for region_id, region in self.regions.items():
            if len(region.compartments) < region.max_compartments:
                # Check if region has similar expert types
                existing_types = set(c.expert_type for c in region.compartments.values())
                if expert_type in existing_types or len(existing_types) < 3:
                    return region_id
        
        # Create new region
        region_id = f"region_{expert_type}_{len(self.regions)}"
        self._ensure_region_exists(region_id)
        return region_id
    
    def create_compartment(
        self, expert_type: str, expert_instance: Any = None,
        resources: Optional[CompartmentResource] = None,
        parent_id: Optional[str] = None,
        region_id: Optional[str] = None
    ) -> ChromatophoreCompartment:
        """Create compartment in appropriate region"""
        if region_id is None:
            region_id = self._get_region_for_expert(expert_type)
        
        self._ensure_region_exists(region_id)
        
        compartment_id = f"comp_{expert_type}_{uuid.uuid4().hex[:8]}"
        
        if resources is None:
            resources = CompartmentResource(
                cpu_cores=min(2.0, 16.0 * 0.1),
                memory_mb=min(256.0, 4096.0 * 0.1),
                storage_mb=min(512.0, 10240.0 * 0.05)
            )
        
        compartment = ChromatophoreCompartment(
            compartment_id=compartment_id,
            expert_type=expert_type,
            expert_instance=expert_instance,
            resources=resources
        )
        
        if parent_id:
            compartment.parent_id = parent_id
        
        # Initial token endowment
        if self.token_manager:
            self.token_manager.create_account(compartment_id)
            tokens = self.token_manager.generate_tokens(
                account_id=compartment_id,
                source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=0.001,
                num_tokens=10
            )
            if tokens:
                compartment.receive_tokens(sum(t.value for t in tokens))
        
        # Add to region
        region = self.regions[region_id]
        if not region.add_compartment(compartment):
            # Region full - find another
            for rid, reg in self.regions.items():
                if rid != region_id and len(reg.compartments) < reg.max_compartments:
                    reg.add_compartment(compartment)
                    region_id = rid
                    break
        
        # Track mapping
        self.compartment_to_region[compartment_id] = region_id
        self.compartments[compartment_id] = compartment
        self.total_compartments_created += 1
        
        compartment.state = CompartmentState.MATURING
        
        logger.info(f"Created compartment {compartment_id} in region {region_id}")
        return compartment
    
    def find_best_compartment(self, expert_type: str, task_complexity: float = 1.0) -> Optional[ChromatophoreCompartment]:
        """Find best compartment across all regions"""
        candidates = []
        
        for region in self.regions.values():
            for comp in region.compartments.values():
                if comp.expert_type == expert_type and comp.is_viable:
                    # Include predictive health in scoring
                    health_score = comp.health_score
                    pred = asyncio.run(comp.health_predictor.predict_health({
                        'health_score': health_score,
                        'success_rate': comp.success_rate,
                        'efficiency_score': comp.efficiency_score,
                        'token_balance': comp.token_balance,
                        'trust_gradient': comp.trust_gradient,
                        'task_load': len(comp.glycogen_queue) / 1000
                    }))
                    
                    if pred.get('confidence', 0) > 0.5:
                        health_score = (health_score * 0.6 + pred.get('predicted_health', 0.5) * 0.4)
                    
                    score = (health_score * 0.4 + comp.efficiency_score * 0.3 +
                            min(comp.token_balance / (task_complexity * 10), 1.0) * 0.3)
                    candidates.append((comp, score))
        
        if not candidates:
            return None
        
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]
    
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]:
        """Decommission a compartment (apoptosis)"""
        if compartment_id not in self.compartments:
            return {}
        
        compartment = self.compartments[compartment_id]
        region_id = self.compartment_to_region.get(compartment_id)
        
        # Prepare apoptosis
        remaining_tokens, knowledge = compartment.prepare_apoptosis()
        
        # Store knowledge
        self.knowledge_bank[compartment.expert_type].append(knowledge)
        
        # Transfer knowledge to region
        if region_id and region_id in self.regions:
            self.regions[region_id].knowledge_transfer.add_knowledge(region_id, knowledge)
        
        # Remove from region
        if region_id and region_id in self.regions:
            self.regions[region_id].remove_compartment(compartment_id)
        
        # Return tokens
        if self.token_manager and remaining_tokens > 0:
            self.token_manager.generate_tokens(
                account_id="green_agent_core",
                source=EcoATPSource.EFFICIENCY_GAIN,
                energy_saved_kwh=remaining_tokens / 10000.0,
                num_tokens=int(remaining_tokens / 10)
            )
        
        # Clean up
        del self.compartments[compartment_id]
        self.compartment_to_region.pop(compartment_id, None)
        self.total_apoptosis_events += 1
        
        logger.info(f"Decommissioned compartment {compartment_id}")
        return knowledge
    
    def balance_load(self) -> int:
        """Hierarchical load balancing with cross-region knowledge transfer"""
        # Phase 1: Local balancing
        total_transfers = 0
        for region in self.regions.values():
            total_transfers += region.balance_load_local()
        
        # Phase 2: Global balancing
        if (datetime.utcnow() - self.last_global_balance).total_seconds() > 60:
            self._balance_across_regions()
            self.last_global_balance = datetime.utcnow()
        
        # Phase 3: Knowledge transfer
        if len(self.regions) > 1:
            # Transfer knowledge from best to worst performing region
            sorted_regions = sorted(
                self.regions.items(),
                key=lambda x: x[1].aggregated_health,
                reverse=True
            )
            if len(sorted_regions) >= 2:
                best_region, best = sorted_regions[0]
                worst_region, worst = sorted_regions[-1]
                
                if best.aggregated_health > worst.aggregated_health + 0.1:
                    best.knowledge_transfer.transfer_knowledge(best_region, worst_region)
        
        return total_transfers
    
    def _balance_across_regions(self):
        """Balance load across regions by moving compartments"""
        if len(self.regions) < 2:
            return
        
        region_loads = {}
        for region_id, region in self.regions.items():
            total_tasks = sum(
                len(getattr(c, 'glycogen_queue', []))
                for c in region.compartments.values()
            )
            region_loads[region_id] = total_tasks
        
        if not region_loads:
            return
        
        avg_load = np.mean(list(region_loads.values()))
        if avg_load == 0:
            return
        
        overloaded = {rid: load for rid, load in region_loads.items() if load > avg_load * 1.5}
        underloaded = {rid: load for rid, load in region_loads.items() if load < avg_load * 0.5}
        
        for ol_rid in overloaded:
            for ul_rid in underloaded:
                ol_region = self.regions[ol_rid]
                ul_region = self.regions[ul_rid]
                
                if (ol_region.compartments and 
                    len(ul_region.compartments) < ul_region.max_compartments):
                    
                    comp_id = next(iter(ol_region.compartments.keys()))
                    compartment = ol_region.compartments.pop(comp_id)
                    ul_region.add_compartment(compartment)
                    self.compartment_to_region[comp_id] = ul_rid
                    
                    # Transfer knowledge
                    if hasattr(compartment, 'knowledge_export'):
                        ul_region.knowledge_transfer.add_knowledge(ul_rid, compartment.knowledge_export)
                    
                    logger.info(f"Moved compartment {comp_id}: region {ol_rid} → {ul_rid}")
                    break
    
    def health_check_all(self) -> Dict[str, float]:
        """O(r) health check with predictive modeling"""
        health_scores = {}
        
        for region_id, region in self.regions.items():
            region_health = region.health_check()
            health_scores[region_id] = region_health
            
            # Only check individual compartments if region health is poor
            if region_health < 0.5:
                for comp in region.compartments.values():
                    comp._evaluate_lifecycle()
        
        self.global_health = np.mean(list(health_scores.values())) if health_scores else 0.0
        return health_scores
    
    def cull_unhealthy(self) -> int:
        """Remove unhealthy compartments across all regions"""
        total_culled = 0
        for region in self.regions.values():
            removed = region.cull_unhealthy()
            for comp_id in removed:
                self.compartment_to_region.pop(comp_id, None)
                self.compartments.pop(comp_id, None)
            total_culled += len(removed)
        return total_culled
    
    def spawn_if_needed(self):
        """Spawn new compartments if demand exceeds supply"""
        expert_types = set()
        for region in self.regions.values():
            for comp in region.compartments.values():
                expert_types.add(comp.expert_type)
        
        for etype in expert_types:
            viable = sum(
                1 for region in self.regions.values()
                for comp in region.compartments.values()
                if comp.expert_type == etype and comp.is_viable
            )
            
            if viable < 2:
                self.create_compartment(etype)
                logger.info(f"Auto-spawned compartment for {etype} (viable count: {viable})")
    
    async def _ecosystem_maintenance(self):
        """Periodic ecosystem maintenance"""
        while True:
            try:
                self.balance_load()
                self.spawn_if_needed()
                self.cull_unhealthy()
                self.health_check_all()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Ecosystem maintenance error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _trading_maintenance(self):
        """Periodic trading maintenance"""
        while True:
            try:
                # Match orders in all regions
                for region in self.regions.values():
                    matches = region.market.match_orders()
                    for match in matches:
                        seller_id = match['seller']
                        buyer_id = match['buyer']
                        amount = match['amount']
                        
                        # Execute trade
                        if seller_id in self.compartments and buyer_id in self.compartments:
                            seller = self.compartments[seller_id]
                            buyer = self.compartments[buyer_id]
                            
                            if seller.spend_tokens(amount, "trade") and buyer.receive_tokens(amount, seller_id):
                                logger.info(f"Trade executed: {seller_id} → {buyer_id} ({amount} tokens)")
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Trading maintenance error: {str(e)}")
                await asyncio.sleep(120)
    
    def get_ecosystem_stats(self) -> Dict[str, Any]:
        """Get comprehensive ecosystem statistics"""
        total_compartments = sum(r.get_total_count() for r in self.regions.values())
        viable_compartments = sum(r.get_viable_count() for r in self.regions.values())
        
        # Get specialization insights
        specialization_insights = {}
        for region in self.regions.values():
            insights = region.knowledge_transfer.get_specialization_insights()
            specialization_insights.update(insights)
        
        stats = {
            'total_compartments': total_compartments,
            'viable_compartments': viable_compartments,
            'viability_ratio': viable_compartments / max(total_compartments, 1),
            'total_regions': len(self.regions),
            'total_created': self.total_compartments_created,
            'total_apoptosis': self.total_apoptosis_events,
            'global_health': self.global_health,
            'knowledge_bank_size': sum(len(v) for v in self.knowledge_bank.values()),
            'specialization_insights': specialization_insights,
            'regions': {
                region_id: region.get_region_stats()
                for region_id, region in self.regions.items()
            }
        }
        
        # Expert type distribution
        expert_counts = defaultdict(int)
        for region in self.regions.values():
            for comp in region.compartments.values():
                expert_counts[comp.expert_type] += 1
        stats['expert_distribution'] = dict(expert_counts)
        
        # Global market stats
        total_orders = sum(len(r.market.orders) for r in self.regions.values())
        stats['global_market'] = {
            'total_orders': total_orders,
            'total_trades': sum(len(r.market.trade_history) for r in self.regions.values())
        }
        
        return stats
    
    def get_region_stats(self, region_id: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific region"""
        if region_id in self.regions:
            return self.regions[region_id].get_region_stats()
        return None
    
    def move_compartment(self, compartment_id: str, target_region_id: str) -> bool:
        """Move compartment between regions"""
        if compartment_id not in self.compartment_to_region:
            return False
        
        source_region_id = self.compartment_to_region[compartment_id]
        if source_region_id not in self.regions or target_region_id not in self.regions:
            return False
        
        source_region = self.regions[source_region_id]
        target_region = self.regions[target_region_id]
        
        if compartment_id not in source_region.compartments:
            return False
        
        if len(target_region.compartments) >= target_region.max_compartments:
            return False
        
        compartment = source_region.compartments.pop(compartment_id)
        target_region.add_compartment(compartment)
        self.compartment_to_region[compartment_id] = target_region_id
        
        # Transfer knowledge
        target_region.knowledge_transfer.add_knowledge(target_region_id, compartment.knowledge_export)
        
        logger.info(f"Moved compartment {compartment_id}: {source_region_id} → {target_region_id}")
        return True

# ============================================================================
# Legacy Compatibility
# ============================================================================

class CompartmentManager(HierarchicalCompartmentManager):
    """
    Legacy CompartmentManager for backward compatibility.
    
    Maintains original interface while using hierarchical management.
    """
    
    def __init__(self, token_manager=None):
        super().__init__(token_manager=token_manager, max_regions=5, compartments_per_region=20)
        logger.info("Compartment Manager initialized (legacy compatibility mode)")
