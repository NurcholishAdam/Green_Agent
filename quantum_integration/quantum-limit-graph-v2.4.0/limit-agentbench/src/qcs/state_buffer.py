# src/qcs/state_buffer.py

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np
import hashlib
import json

@dataclass
class QuantumState:
    """Represents a quantum state vector"""
    state_id: str
    amplitudes: np.ndarray  # Complex amplitudes
    qubit_count: int
    created_at: datetime
    coherence_time_ms: float
    thought_signature: str
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class BufferedState:
    """State in buffer waiting for quantum execution"""
    buffer_id: str
    quantum_state: QuantumState
    classical_context: Dict[str, Any]
    queued_at: datetime
    expected_execution_time: Optional[datetime] = None
    status: str = "queued"  # queued, executing, completed, failed, decohered

class StateBuffer:
    """
    Quantum-classical state buffer
    
    Responsibilities:
    - Buffer thoughtSignature during quantum state collapse
    - Translate quantum measurement outcomes to classical tokens
    - Preserve meta-cognitive context across boundary
    - Manage buffer queue with priority scheduling
    """
    
    def __init__(
        self,
        max_buffer_size: int = 1000,
        coherence_timeout_ms: float = 100.0,
        priority_queue: bool = True
    ):
        self.max_buffer_size = max_buffer_size
        self.coherence_timeout = coherence_timeout_ms
        self.priority_queue = priority_queue
        
        self._buffer: Dict[str, BufferedState] = {}
        self._queue: List[str] = []  # Buffer IDs in priority order
        self._completed: Dict[str, BufferedState] = {}
        self._max_completed_cache = 10000
        
    async def buffer_state(
        self,
        thought_signature: str,
        quantum_state: np.ndarray,
        classical_context: Dict[str, Any]
    ) -> str:
        """
        Buffer a quantum state with classical context
        
        Args:
            thought_signature: Meta-cognitive signature from Layer 3
            quantum_state: State vector amplitudes
            classical_context: Classical metadata and parameters
            
        Returns:
            buffer_id for tracking
        """
        buffer_id = self._generate_buffer_id()
        
        # Create quantum state object
        q_state = QuantumState(
            state_id=self._generate_state_id(),
            amplitudes=quantum_state,
            qubit_count=int(np.log2(len(quantum_state))),
            created_at=datetime.now(),
            coherence_time_ms=self.coherence_timeout,
            thought_signature=thought_signature,
            metadata=classical_context
        )
        
        # Create buffered state
        buffered = BufferedState(
            buffer_id=buffer_id,
            quantum_state=q_state,
            classical_context=classical_context,
            queued_at=datetime.now(),
            expected_execution_time=datetime.now()
        )
        
        # Add to buffer
        self._buffer[buffer_id] = buffered
        self._queue.append(buffer_id)
        
        # Enforce max buffer size
        if len(self._buffer) > self.max_buffer_size:
            await self._evict_oldest()
            
        return buffer_id
        
    async def get_buffered_state(self, buffer_id: str) -> Optional[BufferedState]:
        """Get buffered state by ID"""
        return self._buffer.get(buffer_id)
        
    async def complete_state(
        self,
        buffer_id: str,
        measurement_outcomes: np.ndarray,
        classical_tokens: List[str]
    ):
        """Mark state as completed with measurement results"""
        if buffer_id not in self._buffer:
            raise ValueError(f"Buffer ID {buffer_id} not found")
            
        buffered = self._buffer[buffer_id]
        buffered.status = "completed"
        buffered.measurement_outcomes = measurement_outcomes
        buffered.classical_tokens = classical_tokens
        buffered.completed_at = datetime.now()
        
        # Move to completed cache
        self._completed[buffer_id] = buffered
        del self._buffer[buffer_id]
        
        # Enforce completed cache size
        if len(self._completed) > self._max_completed_cache:
            oldest = min(self._completed.keys(), 
                        key=lambda k: self._completed[k].completed_at)
            del self._completed[oldest]
            
    async def mark_decohered(self, buffer_id: str):
        """Mark state as decohered (lost coherence)"""
        if buffer_id in self._buffer:
            self._buffer[buffer_id].status = "decohered"
            del self._buffer[buffer_id]
            
    async def get_next_for_execution(self) -> Optional[BufferedState]:
        """Get next state from queue for quantum execution"""
        if not self._queue:
            return None
            
        buffer_id = self._queue.pop(0)
        if buffer_id in self._buffer:
            buffered = self._buffer[buffer_id]
            buffered.status = "executing"
            return buffered
        return None
        
    async def _evict_oldest(self):
        """Evict oldest buffered state if buffer is full"""
        if not self._buffer:
            return
            
        oldest_id = min(
            self._buffer.keys(),
            key=lambda k: self._buffer[k].queued_at
        )
        del self._buffer[oldest_id]
        if oldest_id in self._queue:
            self._queue.remove(oldest_id)
            
    def _generate_buffer_id(self) -> str:
        """Generate unique buffer ID"""
        return hashlib.sha256(
            f"{datetime.now().isoformat()}:{np.random.random()}".encode()
        ).hexdigest()[:16]
        
    def _generate_state_id(self) -> str:
        """Generate unique quantum state ID"""
        return hashlib.sha256(
            f"qs:{datetime.now().isoformat()}:{np.random.random()}".encode()
        ).hexdigest()[:16]
        
    async def get_buffer_stats(self) -> Dict:
        """Get buffer statistics"""
        return {
            'total_buffered': len(self._buffer),
            'total_completed': len(self._completed),
            'queue_length': len(self._queue),
            'by_status': {
                'queued': sum(1 for b in self._buffer.values() if b.status == 'queued'),
                'executing': sum(1 for b in self._buffer.values() if b.status == 'executing'),
                'completed': len(self._completed),
                'decohered': sum(1 for b in self._buffer.values() if b.status == 'decohered'),
            },
            'avg_coherence_time_ms': np.mean([
                b.quantum_state.coherence_time_ms
                for b in self._buffer.values()
            ]) if self._buffer else 0
        }
