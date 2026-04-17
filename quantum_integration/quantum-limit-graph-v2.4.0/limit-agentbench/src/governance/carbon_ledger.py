"""
Green Agent v5.0.0 - Carbon Ledger
Layer 8: Immutable carbon accounting and compliance
File: src/governance/carbon_ledger.py
"""

from typing import Dict, List
from datetime import datetime
import logging
import json
import hashlib

logger = logging.getLogger(__name__)


class CarbonLedger:
    """
    Immutable ledger for carbon accounting and compliance reporting
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.entries: List[Dict] = []
        self.total_carbon = 0.0
        self.total_energy = 0.0
        self._ledger_file = config.get('governance', {}).get('ledger_file', 'data/carbon_ledger.json')
    
    async def initialize(self):
        """Initialize the ledger"""
        logger.info("CarbonLedger initialized")
        # Load existing entries if file exists
        # (Implementation omitted for brevity)
    
    async def shutdown(self):
        """Persist ledger and shutdown"""
        logger.info(f"CarbonLedger shutdown - Total: {self.total_carbon:.4f} kg CO2")
        # Persist to file
        # (Implementation omitted for brevity)
    
    async def record(self, result, decision) -> str:
        """
        Record a carbon accounting entry
        
        Args:
            result: UnifiedResult from execution
            decision: ExecutionDecision that was applied
            
        Returns:
            Hash of the ledger entry for verification
        """
        entry = {
            'timestamp': datetime.now().isoformat(),
            'task_id': result.task_id,
            'energy_kwh': result.energy_consumed,
            'carbon_kg': result.carbon_emitted,
            'carbon_zone': decision.carbon_zone,
            'action': decision.action,
            'negawatt_reward': result.negawatt_reward,
            'power_budget': decision.power_budget
        }
        
        # Calculate cryptographic hash for integrity
        entry['hash'] = self._calculate_hash(entry)
        
        # Add to in-memory ledger
        self.entries.append(entry)
        self.total_carbon += result.carbon_emitted
        self.total_energy += result.energy_consumed
        
        logger.info(f"Carbon ledger entry: {result.carbon_emitted:.4f} kg CO2 (total: {self.total_carbon:.4f})")
        return entry['hash']
    
    def _calculate_hash(self, entry: Dict) -> str:
        """Calculate SHA-256 hash of entry for integrity verification"""
        # Exclude hash field from hash calculation
        data = {k: v for k, v in entry.items() if k != 'hash'}
        serialized = json.dumps(data, sort_keys=True)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
    
    async def get_report(self, start_date: datetime = None, end_date: datetime = None) -> Dict:
        """
        Generate carbon accounting report
        
        Args:
            start_date: Start of reporting period
            end_date: End of reporting period
            
        Returns:
            Report dictionary with totals and entries
        """
        # Filter entries by date range
        filtered = self.entries
        if start_date:
            filtered = [e for e in filtered if datetime.fromisoformat(e['timestamp']) >= start_date]
        if end_date:
            filtered = [e for e in filtered if datetime.fromisoformat(e['timestamp']) <= end_date]
        
        return {
            'report_period': {
                'start': start_date.isoformat() if start_date else None,
                'end': end_date.isoformat() if end_date else None
            },
            'total_entries': len(filtered),
            'total_energy_kwh': sum(e['energy_kwh'] for e in filtered),
            'total_carbon_kg': sum(e['carbon_kg'] for e in filtered),
            'total_negawatt': sum(e['negawatt_reward'] for e in filtered),
            'average_carbon_intensity': self._calculate_average_intensity(filtered),
            'entries': filtered[-100:]  # Return last 100 entries
        }
    
    def _calculate_average_intensity(self, entries: List[Dict]) -> float:
        """Calculate average carbon intensity from entries"""
        if not entries:
            return 0.0
        total_energy = sum(e['energy_kwh'] for e in entries)
        total_carbon = sum(e['carbon_kg'] for e in entries)
        if total_energy == 0:
            return 0.0
        return (total_carbon / total_energy) * 1000  # Convert to gCO2/kWh
