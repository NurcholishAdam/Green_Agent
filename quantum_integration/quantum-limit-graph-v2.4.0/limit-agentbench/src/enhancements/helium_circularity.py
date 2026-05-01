# src/enhancements/helium_circularity.py

"""
Helium Circularity Tracker for Green Agent
Scientific basis: Circular economy metrics (material circularity indicator)

Reference: "Circular Economy Metrics for Critical Materials" (Resources, Conservation & Recycling, 2024)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
from datetime import datetime
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class RecoveryMethod(Enum):
    """Methods for helium recovery"""
    CAPTURE = "capture"
    RECYCLE = "recycle"
    PURIFICATION = "purification"
    LIQUEFACTION = "liquefaction"
    REUSE = "reuse"


class HardwareType(Enum):
    """Hardware types with different recovery potentials"""
    GPU_CLUSTER = "gpu_cluster"
    SINGLE_GPU = "single_gpu"
    TPU = "tpu"
    QUANTUM = "quantum"
    CPU = "cpu"


@dataclass
class CircularityEntry:
    """Entry in helium circularity ledger"""
    task_id: str
    timestamp: datetime
    hardware_type: HardwareType
    helium_used_liters: float
    helium_recovered_liters: float
    recovery_method: RecoveryMethod
    circularity_score: float
    recovery_efficiency: float
    hash: str = ""


@dataclass
class CircularityMetrics:
    """Aggregated circularity metrics"""
    total_helium_used_liters: float
    total_helium_recovered_liters: float
    average_circularity_score: float
    recovery_rate_percent: float
    virgin_helium_saved_liters: float
    carbon_credits_earned: float
    recommendations: List[str]


class HeliumCircularityTracker:
    """
    Track helium recovery, recycling, and reuse across tasks.
    
    Circularity = (Recycled + Reused + Repaired) / Total Used
    """
    
    # Recovery rates by hardware type (percentage of helium recoverable)
    RECOVERY_RATES = {
        HardwareType.GPU_CLUSTER: 0.85,
        HardwareType.SINGLE_GPU: 0.70,
        HardwareType.TPU: 0.75,
        HardwareType.QUANTUM: 0.60,
        HardwareType.CPU: 0.95
    }
    
    # Recovery method characteristics
    RECOVERY_METHODS = {
        RecoveryMethod.CAPTURE: {'efficiency': 0.70, 'cost_per_liter': 0.50, 'carbon_per_liter': 0.1},
        RecoveryMethod.RECYCLE: {'efficiency': 0.80, 'cost_per_liter': 0.80, 'carbon_per_liter': 0.2},
        RecoveryMethod.PURIFICATION: {'efficiency': 0.90, 'cost_per_liter': 1.50, 'carbon_per_liter': 0.3},
        RecoveryMethod.LIQUEFACTION: {'efficiency': 0.95, 'cost_per_liter': 2.00, 'carbon_per_liter': 0.5},
        RecoveryMethod.REUSE: {'efficiency': 0.98, 'cost_per_liter': 0.10, 'carbon_per_liter': 0.05}
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.circularity_ledger: List[CircularityEntry] = []
        self.cumulative_metrics = CircularityMetrics(
            total_helium_used_liters=0,
            total_helium_recovered_liters=0,
            average_circularity_score=0,
            recovery_rate_percent=0,
            virgin_helium_saved_liters=0,
            carbon_credits_earned=0,
            recommendations=[]
        )
        
    def calculate_recoverable_helium(self, helium_used_liters: float,
                                      hardware_type: HardwareType) -> float:
        """Calculate how much helium can be recovered"""
        recovery_rate = self.RECOVERY_RATES.get(hardware_type, 0.70)
        return helium_used_liters * recovery_rate
    
    def calculate_circularity_score(self, helium_used_liters: float,
                                     helium_recovered_liters: float) -> float:
        """Calculate circularity score (0-1)"""
        if helium_used_liters == 0:
            return 1.0
        return min(1.0, helium_recovered_liters / helium_used_liters)
    
    def determine_recovery_method(self, hardware_type: HardwareType,
                                   recovery_amount_liters: float) -> RecoveryMethod:
        """Determine optimal recovery method based on hardware and volume"""
        if recovery_amount_liters > 1000:
            return RecoveryMethod.LIQUEFACTION
        elif recovery_amount_liters > 100:
            return RecoveryMethod.PURIFICATION
        elif recovery_amount_liters > 10:
            return RecoveryMethod.RECYCLE
        elif recovery_amount_liters > 1:
            return RecoveryMethod.CAPTURE
        else:
            return RecoveryMethod.REUSE
    
    def track_helium_usage(self, task_id: str, helium_used_liters: float,
                          hardware_type: HardwareType,
                          recovery_enabled: bool = True) -> CircularityEntry:
        """
        Track helium usage and calculate circularity.
        
        Main interface for Layer 8 integration.
        """
        # Calculate recoverable amount
        recoverable = self.calculate_recoverable_helium(helium_used_liters, hardware_type)
        
        # Determine recovery method
        recovery_method = self.determine_recovery_method(hardware_type, recoverable)
        method_data = self.RECOVERY_METHODS[recovery_method]
        
        # Actual recovered (based on efficiency)
        if recovery_enabled:
            helium_recovered = recoverable * method_data['efficiency']
        else:
            helium_recovered = 0
        
        # Calculate circularity score
        circularity_score = self.calculate_circularity_score(helium_used_liters, helium_recovered)
        
        # Create entry
        entry = CircularityEntry(
            task_id=task_id,
            timestamp=datetime.now(),
            hardware_type=hardware_type,
            helium_used_liters=helium_used_liters,
            helium_recovered_liters=helium_recovered,
            recovery_method=recovery_method,
            circularity_score=circularity_score,
            recovery_efficiency=method_data['efficiency']
        )
        
        # Calculate hash for immutability
        entry.hash = self._calculate_hash(entry)
        
        # Add to ledger
        self.circularity_ledger.append(entry)
        
        # Update cumulative metrics
        self._update_cumulative_metrics()
        
        logger.info(f"Helium circularity for {task_id}: used={helium_used_liters:.2f}L, "
                   f"recovered={helium_recovered:.2f}L, score={circularity_score:.2f}")
        
        return entry
    
    def _calculate_hash(self, entry: CircularityEntry) -> str:
        """Calculate SHA-256 hash for immutability"""
        data = {
            'task_id': entry.task_id,
            'timestamp': entry.timestamp.isoformat(),
            'helium_used': entry.helium_used_liters,
            'helium_recovered': entry.helium_recovered_liters,
            'circularity_score': entry.circularity_score
        }
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def _update_cumulative_metrics(self):
        """Update cumulative circularity metrics"""
        total_used = sum(e.helium_used_liters for e in self.circularity_ledger)
        total_recovered = sum(e.helium_recovered_liters for e in self.circularity_ledger)
        
        self.cumulative_metrics.total_helium_used_liters = total_used
        self.cumulative_metrics.total_helium_recovered_liters = total_recovered
        
        if total_used > 0:
            self.cumulative_metrics.recovery_rate_percent = (total_recovered / total_used) * 100
            self.cumulative_metrics.average_circularity_score = total_recovered / total_used
        
        self.cumulative_metrics.virgin_helium_saved_liters = total_recovered
        
        # Carbon credits: each liter of helium recovered avoids production emissions
        # Helium production emits ~2 kg CO2 per liter
        self.cumulative_metrics.carbon_credits_earned = total_recovered * 2
        
        # Generate recommendations
        self.cumulative_metrics.recommendations = self._generate_recommendations()
    
    def _generate_recommendations(self) -> List[str]:
        """Generate circularity improvement recommendations"""
        recommendations = []
        
        if self.cumulative_metrics.recovery_rate_percent < 50:
            recommendations.append("Improve helium recovery infrastructure (target >70%)")
        
        # Check hardware types with low recovery
        for hardware in HardwareType:
            entries = [e for e in self.circularity_ledger if e.hardware_type == hardware]
            if entries:
                avg_score = sum(e.circularity_score for e in entries) / len(entries)
                if avg_score < 0.6:
                    recommendations.append(f"Improve recovery for {hardware.value} (current {avg_score:.0%})")
        
        # Check if better recovery methods available
        for entry in self.circularity_ledger[-10:]:  # Last 10 entries
            if entry.recovery_efficiency < 0.8 and entry.helium_recovered_liters > 10:
                recommendations.append(f"Consider upgrading recovery method from {entry.recovery_method.value} for large volumes")
        
        if not recommendations:
            recommendations.append("Helium circularity metrics are healthy. Maintain current recovery practices.")
        
        return recommendations[:5]  # Top 5
    
    def get_circularity_certificate(self, task_id: str) -> Optional[Dict]:
        """Generate circularity certificate for a task"""
        entries = [e for e in self.circularity_ledger if e.task_id == task_id]
        
        if not entries:
            return None
        
        entry = entries[-1]
        
        return {
            'task_id': task_id,
            'circularity_score': entry.circularity_score,
            'helium_saved_liters': entry.helium_recovered_liters,
            'carbon_offset_kg': entry.helium_recovered_liters * 2,
            'certificate_hash': entry.hash,
            'issuance_date': entry.timestamp.isoformat(),
            'valid_until': (entry.timestamp.replace(year=entry.timestamp.year + 1)).isoformat()
        }
    
    def get_circularity_metrics(self) -> CircularityMetrics:
        """Get current circularity metrics"""
        return self.cumulative_metrics
    
    def verify_integrity(self) -> bool:
        """Verify ledger integrity"""
        for entry in self.circularity_ledger:
            expected_hash = self._calculate_hash(entry)
            if entry.hash != expected_hash:
                logger.error(f"Integrity check failed for task {entry.task_id}")
                return False
        return True
    
    def get_circularity_trend(self, days: int = 30) -> List[Dict]:
        """Get circularity trend over time"""
        cutoff = datetime.now().timestamp() - (days * 86400)
        recent = [e for e in self.circularity_ledger if e.timestamp.timestamp() > cutoff]
        
        # Group by day
        trend = {}
        for entry in recent:
            day = entry.timestamp.date().isoformat()
            if day not in trend:
                trend[day] = {'total_used': 0, 'total_recovered': 0, 'count': 0}
            trend[day]['total_used'] += entry.helium_used_liters
            trend[day]['total_recovered'] += entry.helium_recovered_liters
            trend[day]['count'] += 1
        
        # Calculate daily scores
        result = []
        for day, data in sorted(trend.items()):
            score = data['total_recovered'] / data['total_used'] if data['total_used'] > 0 else 0
            result.append({
                'date': day,
                'circularity_score': score,
                'helium_used': data['total_used'],
                'helium_recovered': data['total_recovered'],
                'task_count': data['count']
            })
        
        return result
