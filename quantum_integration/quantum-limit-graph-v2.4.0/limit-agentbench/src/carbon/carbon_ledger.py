# src/carbon/carbon_ledger.py (EXTENDED)

import hashlib
import json
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict

@dataclass
class LedgerEntry:
    """Enhanced ledger entry with helium metrics"""
    timestamp: datetime
    task_id: str
    energy_kwh: float
    carbon_kg: float
    helium_zone: Optional[str]
    helium_usage: float
    helium_supply_at_execution: str
    helium_spot_price: float
    hardware_type: str
    power_budget: float
    fallback_used: bool
    hash: str = ""

class ExtendedCarbonLedger:
    """
    Extended carbon ledger with helium accounting
    Implements immutable ledger with cryptographic hashing
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.ledger: List[LedgerEntry] = []
        self.ledger_file = self.config.get('ledger_file', 'carbon_ledger.json')
        
        # Load existing ledger if available
        self._load_ledger()
    
    def add_entry(self, unified_result, execution_decision, helium_signal) -> LedgerEntry:
        """
        Add entry to ledger with helium metrics
        """
        
        entry = LedgerEntry(
            timestamp=datetime.now(),
            task_id=unified_result.task_id,
            energy_kwh=unified_result.energy_consumed_kwh,
            carbon_kg=unified_result.carbon_emitted_kg,
            helium_zone=execution_decision.helium_zone.value if execution_decision.helium_zone else None,
            helium_usage=getattr(unified_result, 'helium_usage', 0.0),
            helium_supply_at_execution=helium_signal.scarcity_level.value if helium_signal else 'unknown',
            helium_spot_price=helium_signal.spot_price_usd_per_liter if helium_signal else 4.0,
            hardware_type=unified_result.worker_type,
            power_budget=execution_decision.power_budget,
            fallback_used=getattr(unified_result, 'fallback_used', False)
        )
        
        # Calculate cryptographic hash
        entry.hash = self._calculate_hash(entry)
        
        # Add to ledger
        self.ledger.append(entry)
        
        # Persist to disk
        self._save_ledger()
        
        return entry
    
    def _calculate_hash(self, entry: LedgerEntry) -> str:
        """Calculate SHA-256 hash of entry for immutability"""
        # Create dictionary without hash field
        entry_dict = asdict(entry)
        entry_dict.pop('hash', None)
        
        # Convert to JSON string and hash
        json_str = json.dumps(entry_dict, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def _save_ledger(self):
        """Save ledger to disk"""
        try:
            with open(self.ledger_file, 'w') as f:
                json.dump([asdict(entry) for entry in self.ledger], f, default=str, indent=2)
        except Exception as e:
            logger.error(f"Failed to save ledger: {e}")
    
    def _load_ledger(self):
        """Load ledger from disk"""
        try:
            with open(self.ledger_file, 'r') as f:
                data = json.load(f)
                for entry_dict in data:
                    entry_dict['timestamp'] = datetime.fromisoformat(entry_dict['timestamp'])
                    self.ledger.append(LedgerEntry(**entry_dict))
        except FileNotFoundError:
            logger.info("No existing ledger found, starting fresh")
        except Exception as e:
            logger.error(f"Failed to load ledger: {e}")
    
    def get_helium_efficiency_report(self, task_id: Optional[str] = None) -> Dict:
        """Generate helium efficiency report"""
        
        if task_id:
            entries = [e for e in self.ledger if e.task_id == task_id]
        else:
            entries = self.ledger
        
        if not entries:
            return {'error': 'No entries found'}
        
        total_helium_usage = sum(e.helium_usage for e in entries)
        total_energy = sum(e.energy_kwh for e in entries)
        
        return {
            'total_entries': len(entries),
            'total_helium_usage': total_helium_usage,
            'total_energy_kwh': total_energy,
            'helium_per_energy_ratio': total_helium_usage / total_energy if total_energy > 0 else 0,
            'tasks_by_helium_zone': {
                zone: len([e for e in entries if e.helium_zone == zone])
                for zone in set(e.helium_zone for e in entries if e.helium_zone)
            },
            'fallback_rate': len([e for e in entries if e.fallback_used]) / len(entries) if entries else 0
        }
    
    def verify_integrity(self) -> bool:
        """Verify ledger integrity by checking all hashes"""
        for i, entry in enumerate(self.ledger):
            expected_hash = self._calculate_hash(entry)
            if entry.hash != expected_hash:
                logger.error(f"Integrity check failed at index {i}")
                return False
        return True
