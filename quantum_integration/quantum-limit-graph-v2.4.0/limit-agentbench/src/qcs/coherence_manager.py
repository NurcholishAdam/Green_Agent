# src/qcs/coherence_manager.py

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import numpy as np

@dataclass
class CoherenceWindow:
    """Quantum coherence window tracking"""
    window_id: str
    qpu_id: str
    start_time: datetime
    expected_duration_ms: float
    remaining_duration_ms: float
    current_fidelity: float
    decoherence_rate: float  # Per millisecond
    status: str = "active"  # active, expiring, decohered, completed

@dataclass
class CoherenceAlert:
    """Alert for coherence threshold breach"""
    alert_id: str
    window_id: str
    alert_type: str  # low_fidelity, expiring_soon, decohered
    severity: str  # warning, critical
    remaining_time_ms: float
    current_fidelity: float
    timestamp: datetime
    recommended_action: str

class CoherenceManager:
    """
    Quantum state coherence tracker
    
    Responsibilities:
    - Track qubit coherence windows
    - Schedule VQC execution within coherence time
    - Fallback to classical simulation on decoherence
    - Alert on coherence threshold breaches
    """
    
    def __init__(
        self,
        qpu_id: str,
        coherence_threshold_ms: float = 100.0,
        fidelity_threshold: float = 0.95,
        warning_buffer_ms: float = 10.0
    ):
        self.qpu_id = qpu_id
        self.coherence_threshold = coherence_threshold_ms
        self.fidelity_threshold = fidelity_threshold
        self.warning_buffer = warning_buffer_ms
        
        self._active_windows: Dict[str, CoherenceWindow] = {}
        self._alert_callbacks: List[callable] = []
        self._running = False
        
    async def start(self):
        """Start coherence monitoring loop"""
        self._running = True
        asyncio.create_task(self._monitoring_loop())
        
    async def stop(self):
        """Stop coherence monitoring"""
        self._running = False
        
    def register_alert_callback(self, callback: callable):
        """Register callback for coherence alerts"""
        self._alert_callbacks.append(callback)
        
    async def start_coherence_window(
        self,
        expected_duration_ms: float = 100.0,
        initial_fidelity: float = 0.99
    ) -> str:
        """
        Start a new coherence window
        
        Args:
            expected_duration_ms: Expected coherence time
            initial_fidelity: Initial state fidelity
            
        Returns:
            window_id for tracking
        """
        window_id = self._generate_window_id()
        
        window = CoherenceWindow(
            window_id=window_id,
            qpu_id=self.qpu_id,
            start_time=datetime.now(),
            expected_duration_ms=expected_duration_ms,
            remaining_duration_ms=expected_duration_ms,
            current_fidelity=initial_fidelity,
            decoherence_rate=(1.0 - initial_fidelity) / expected_duration_ms
        )
        
        self._active_windows[window_id] = window
        return window_id
        
    async def update_coherence(
        self,
        window_id: str,
        elapsed_ms: float,
        measured_fidelity: Optional[float] = None
    ) -> CoherenceWindow:
        """
        Update coherence window with elapsed time
        
        Args:
            window_id: Window to update
            elapsed_ms: Time elapsed since window start
            measured_fidelity: Optional measured fidelity (vs. estimated)
            
        Returns:
            Updated CoherenceWindow
        """
        if window_id not in self._active_windows:
            raise ValueError(f"Window {window_id} not found")
            
        window = self._active_windows[window_id]
        
        # Update remaining time
        window.remaining_duration_ms = max(0, window.expected_duration_ms - elapsed_ms)
        
        # Update fidelity (measured or estimated)
        if measured_fidelity is not None:
            window.current_fidelity = measured_fidelity
        else:
            # Estimate from decoherence rate
            window.current_fidelity = max(0, 1.0 - (window.decoherence_rate * elapsed_ms))
            
        # Check status
        if window.current_fidelity < self.fidelity_threshold:
            window.status = "expiring"
            await self._trigger_alert(window, "low_fidelity", "warning")
        elif window.remaining_duration_ms < self.warning_buffer:
            window.status = "expiring"
            await self._trigger_alert(window, "expiring_soon", "critical")
        elif window.remaining_duration_ms <= 0 or window.current_fidelity < 0.5:
            window.status = "decohered"
            await self._trigger_alert(window, "decohered", "critical")
            
        return window
        
    async def end_coherence_window(
        self,
        window_id: str,
        final_fidelity: float,
        successful: bool = True
    ):
        """End coherence window (successful completion or failure)"""
        if window_id in self._active_windows:
            window = self._active_windows[window_id]
            window.current_fidelity = final_fidelity
            window.status = "completed" if successful else "failed"
            del self._active_windows[window_id]
            
    async def get_coherence_status(self, window_id: str) -> Dict:
        """Get current coherence status for a window"""
        if window_id not in self._active_windows:
            return {'status': 'not_found'}
            
        window = self._active_windows[window_id]
        elapsed = (datetime.now() - window.start_time).total_seconds() * 1000
        
        return {
            'window_id': window_id,
            'qpu_id': window.qpu_id,
            'status': window.status,
            'elapsed_ms': elapsed,
            'remaining_ms': window.remaining_duration_ms,
            'expected_duration_ms': window.expected_duration_ms,
            'current_fidelity': window.current_fidelity,
            'fidelity_threshold': self.fidelity_threshold,
            'utilization_percent': (elapsed / window.expected_duration_ms) * 100
        }
        
    async def _trigger_alert(
        self,
        window: CoherenceWindow,
        alert_type: str,
        severity: str
    ):
        """Trigger coherence alert"""
        alert = CoherenceAlert(
            alert_id=self._generate_alert_id(),
            window_id=window.window_id,
            alert_type=alert_type,
            severity=severity,
            remaining_time_ms=window.remaining_duration_ms,
            current_fidelity=window.current_fidelity,
            timestamp=datetime.now(),
            recommended_action=self._get_recommended_action(alert_type)
        )
        
        for callback in self._alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")
                
    def _get_recommended_action(self, alert_type: str) -> str:
        """Get recommended action for alert type"""
        actions = {
            "low_fidelity": "Consider error mitigation or fallback to classical",
            "expiring_soon": "Complete current operation immediately",
            "decohered": "Fallback to classical simulation, reinitialize quantum state"
        }
        return actions.get(alert_type, "Monitor closely")
        
    def _generate_window_id(self) -> str:
        """Generate unique window ID"""
        import hashlib
        return hashlib.sha256(
            f"cw:{self.qpu_id}:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]
        
    def _generate_alert_id(self) -> str:
        """Generate unique alert ID"""
        import hashlib
        return hashlib.sha256(
            f"ca:{datetime.now().isoformat()}:{np.random.random()}".encode()
        ).hexdigest()[:16]
        
    async def _monitoring_loop(self):
        """Background loop for coherence monitoring"""
        while self._running:
            try:
                for window_id in list(self._active_windows.keys()):
                    window = self._active_windows[window_id]
                    elapsed = (datetime.now() - window.start_time).total_seconds() * 1000
                    await self.update_coherence(window_id, elapsed)
                await asyncio.sleep(1)  # Check every second
            except Exception as e:
                logger.error(f"Coherence monitoring error: {e}")
                await asyncio.sleep(5)
