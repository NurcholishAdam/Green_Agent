# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/chromatophore_compartments.py
# Enhanced with mandatory validation gates and trusted anomaly detection

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid
import hashlib

logger = logging.getLogger(__name__)

# ============================================================================
# Enums
# ============================================================================

class CompartmentState(Enum):
    GENESIS = "genesis"
    MATURING = "maturing"
    ACTIVE = "active"
    STRESSED = "stressed"
    SENESCENT = "senescent"
    APOPTOTIC = "apoptotic"
    DECOMMISSIONED = "decommissioned"

class MembranePermeability(Enum):
    IMPERMEABLE = "impermeable"
    RESTRICTIVE = "restrictive"
    SELECTIVE = "selective"
    PERMEABLE = "permeable"

# ============================================================================
# Enhanced Membrane Gate with Mandatory Validation
# ============================================================================

class MembraneGate:
    """
    Enhanced Membrane Gate with:
    - Mandatory validation for critical operations
    - Trusted anomaly detection
    - Rate limiting for critical operations
    - Security audit logging
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
        
        # ====================================================================
        # FIX 5: Mandatory Validation for Critical Operations
        # ====================================================================
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
    
    def can_pass(
        self, source_id: str, direction: str = 'inbound',
        operation_type: str = 'status_query', payload_size: int = 0
    ) -> Tuple[bool, str]:
        """Enhanced permission check with mandatory validation for critical operations"""
        
        # Blocked peers always denied
        if source_id in self.blocked_peers:
            self._record_blocked(source_id, operation_type, 'blocked_peer')
            self.rejected_count += 1
            return False, "Source is blocked"
        
        # Mandatory validation for critical operations
        if self.mandatory_validation_for.get(operation_type, False):
            validation_passed = self._validate_critical_operation(
                source_id, operation_type, payload_size
            )
            if not validation_passed:
                self._record_blocked(source_id, operation_type, 'validation_failed')
                self.rejected_count += 1
                return False, "Critical operation validation failed"
        
        # For non-critical operations, use trust-based permeability
        if self.permeability == MembranePermeability.IMPERMEABLE:
            self._record_blocked(source_id, operation_type, 'impermeable')
            self.rejected_count += 1
            return False, "Membrane is impermeable"
        
        if self.permeability == MembranePermeability.RESTRICTIVE:
            if source_id not in self.trusted_peers:
                self._record_blocked(source_id, operation_type, 'not_trusted')
                self.rejected_count += 1
                return False, "Source not in trusted peers"
        
        # Even for trusted sources, check for anomalies
        if source_id in self.trusted_peers and self.permeability == MembranePermeability.PERMEABLE:
            anomaly_score = self._check_trusted_anomaly(source_id, operation_type)
            if anomaly_score > self.trusted_anomaly_threshold:
                logger.warning(
                    f"Anomaly detected from trusted peer {source_id}: "
                    f"score={anomaly_score:.2f}, operation={operation_type}"
                )
                self._record_blocked(source_id, operation_type, f'anomaly_{anomaly_score:.2f}')
                self.rejected_count += 1
                return False, f"Anomaly detected (score: {anomaly_score:.2f})"
        
        # Track successful passage
        if direction == 'inbound':
            self.inbound_count += 1
        else:
            self.outbound_count += 1
        
        return True, "Passed"
    
    def _validate_critical_operation(
        self, source_id: str, operation_type: str, payload_size: int
    ) -> bool:
        """Validate critical operations regardless of trust level"""
        now = datetime.utcnow()
        
        # Rate limiting for critical operations
        recent_ops = [
            t for t in self._critical_op_timestamps[source_id]
            if (now - t).total_seconds() < 60
        ]
        
        if len(recent_ops) > 10:
            logger.warning(f"Critical operation rate limit exceeded for {source_id}")
            return False
        
        self._critical_op_timestamps[source_id].append(now)
        
        # Size validation for model updates
        if operation_type == 'model_update' and payload_size > 100 * 1024 * 1024:
            logger.warning(f"Model update too large from {source_id}: {payload_size} bytes")
            return False
        
        # Token transfer validation
        if operation_type == 'token_transfer':
            if not self._validate_token_transfer(source_id):
                return False
        
        return True
    
    def _validate_token_transfer(self, source_id: str) -> bool:
        """Validate token transfer operations"""
        # Check transfer frequency
        now = datetime.utcnow()
        recent_transfers = [
            t for t in self._critical_op_timestamps.get(f"transfer_{source_id}", [])
            if (now - t).total_seconds() < 300  # 5 minutes
        ]
        
        if len(recent_transfers) > 20:  # Max 20 transfers per 5 minutes
            logger.warning(f"Token transfer rate limit exceeded for {source_id}")
            return False
        
        if not hasattr(self, '_transfer_timestamps'):
            self._transfer_timestamps = defaultdict(list)
        self._transfer_timestamps[f"transfer_{source_id}"].append(now)
        
        return True
    
    def _check_trusted_anomaly(self, source_id: str, operation_type: str) -> float:
        """Check for anomalous behavior from trusted sources"""
        anomaly_score = 0.0
        
        # Check frequency anomaly
        now = datetime.utcnow()
        recent_count = sum(
            1 for t in self._critical_op_timestamps.get(source_id, [])
            if (now - t).total_seconds() < 10
        )
        if recent_count > 5:
            anomaly_score += 0.3
        
        # Check operation type anomaly
        history = self._source_operation_history[source_id]
        total_ops = sum(history.values())
        
        if total_ops > 50:
            op_frequency = history.get(operation_type, 0) / total_ops
            if op_frequency < 0.05:
                anomaly_score += 0.2
        
        # Check payload size anomaly
        if total_ops > 100:
            if hasattr(self, '_payload_history'):
                avg_payload = np.mean(self._payload_history.get(source_id, [1000]))
                if operation_type == 'model_update' and hasattr(self, '_current_payload'):
                    if self._current_payload > avg_payload * 10:
                        anomaly_score += 0.3
        
        self._source_operation_history[source_id][operation_type] += 1
        
        return min(1.0, anomaly_score)
    
    def _record_blocked(self, source_id: str, operation_type: str, reason: str):
        """Record blocked operation for security audit"""
        self.blocked_operations.append({
            'source_id': source_id,
            'operation_type': operation_type,
            'reason': reason,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def adjust_permeability(self, trust_score: float, token_balance: float):
        """Dynamically adjust membrane permeability"""
        if trust_score > 0.8 and token_balance > 500:
            self.permeability = MembranePermeability.PERMEABLE
        elif trust_score > 0.5 and token_balance > 200:
            self.permeability = MembranePermeability.SELECTIVE
        elif trust_score > 0.2:
            self.permeability = MembranePermeability.RESTRICTIVE
        else:
            self.permeability = MembranePermeability.IMPERMEABLE
    
    def get_security_stats(self) -> Dict[str, Any]:
        """Get security statistics"""
        return {
            'total_blocked': len(self.blocked_operations),
            'recent_blocks': list(self.blocked_operations)[-20:],
            'rejection_rate': self.rejected_count / max(self.inbound_count + self.outbound_count, 1),
            'anomaly_detections': sum(
                1 for b in self.blocked_operations if 'anomaly' in b.get('reason', '')
            )
        }

# ============================================================================
# Bio-Core Buffer for Graceful Degradation
# ============================================================================

class BioCoreBuffer:
    """
    Local buffer that caches bio-core data for graceful degradation.
    
    FIX 6: Allows experts to operate during bio-core outages using cached values.
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
        """Sync buffer from bio-core. Returns True if successful."""
        try:
            if bio_core and hasattr(bio_core, 'gradient_manager'):
                self.cached_gradient_levels = bio_core.gradient_manager.get_field_strengths()
            if bio_core and hasattr(bio_core, 'token_manager'):
                summary = bio_core.token_manager.get_system_summary()
                self.cached_token_balance = summary.get('total_balance', 500)
            if bio_core and hasattr(bio_core, 'compartment_manager'):
                compartment = bio_core.compartment_manager.find_best_compartment('data')
                if compartment:
                    self.cached_compartment_health = compartment.health_score
            
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
        else:
            self.degraded_operations_count += 1
            cached = self.cached_gradient_levels.get(field_id, 0.5)
            # Conservative estimate with uncertainty noise
            return max(0.0, min(1.0, cached * 0.8 + np.random.normal(0, 0.1)))
    
    def get_token_balance(self) -> float:
        """Get token balance with graceful degradation"""
        if not self.degraded_mode:
            return self.cached_token_balance
        else:
            self.degraded_operations_count += 1
            return self.cached_token_balance * 0.7  # Conservative 30% reduction
    
    def get_compartment_health(self) -> float:
        """Get compartment health with graceful degradation"""
        if not self.degraded_mode:
            return self.cached_compartment_health
        else:
            return max(0.1, self.cached_compartment_health * 0.8)
    
    def _check_degraded(self):
        """Check if should enter degraded mode"""
        if self.last_sync_time:
            elapsed = (datetime.utcnow() - self.last_sync_time).total_seconds()
            if elapsed > self.buffer_ttl:
                if not self.degraded_mode:
                    self.degraded_mode = True
                    logger.warning(
                        f"Entering DEGRADED MODE: Bio-core unavailable for {elapsed:.0f}s"
                    )
    
    def is_degraded(self) -> bool:
        """Check if operating in degraded mode"""
        return self.degraded_mode
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            'degraded_mode': self.degraded_mode,
            'bio_core_available': self.bio_core_available,
            'last_sync': self.last_sync_time.isoformat() if self.last_sync_time else None,
            'degraded_operations': self.degraded_operations_count,
            'buffer_ttl': self.buffer_ttl
        }
