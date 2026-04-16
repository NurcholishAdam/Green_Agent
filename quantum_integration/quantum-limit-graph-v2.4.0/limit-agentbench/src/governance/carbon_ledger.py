"""
Green Agent v5.0.0 - Carbon Ledger
Layer 8: Immutable carbon accounting and compliance

File: src/governance/carbon_ledger.py
Status: FOUNDATIONAL - Tier 1
"""

from typing import Dict, List
from datetime import datetime
import logging
import json
import hashlib

logger = logging.getLogger(__name__)


class CarbonLedger:
    def __init__(self, config: Dict):
        self.config = config
        self.entries: List[Dict] = []
        self.total_carbon = 0.0
        self.total_energy = 0.0
    
    async def initialize(self):
        logger.info("CarbonLedger initialized")
    
    async def shutdown(self):
        logger.info(f"CarbonLedger shutdown - Total: {self.total_carbon:.4f} kg CO2")
    
    async def record(self, result, decision) -> str:
        entry = {
            'timestamp': datetime.now().isoformat(),
            'task_id': result.task_id,
            'energy_kwh': result.energy_consumed,
            'carbon_kg': result.carbon_emitted,
            'carbon_zone': decision.carbon_zone,
            'action': decision.action,
            'negawatt_reward': result.negawatt_reward
        }
        
        entry['hash'] = self._calculate_hash(entry)
        self.entries.append(entry)
        self.total_carbon += result.carbon_emitted
        self.total_energy += result.energy_consumed
        
        logger.info(f"Carbon ledger entry: {result.carbon_emitted:.4f} kg CO2 (total: {self.total_carbon:.4f})")
        return entry['hash']
    
    def _calculate_hash(self, entry: Dict) -> str:
        data = json.dumps({k: v for k, v in entry.items() if k != 'hash'}, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    async def get_report(self, start_date: datetime = None, end_date: datetime = None) -> Dict:
        filtered = self.entries
        if start_date:
            filtered = [e for e in filtered if datetime.fromisoformat(e['timestamp']) >= start_date]
        if end_date:
            filtered = [e for e in filtered if datetime.fromisoformat(e['timestamp']) <= end_date]
        
        return {
            'total_entries': len(filtered),
            'total_energy_kwh': sum(e['energy_kwh'] for e in filtered),
            'total_carbon_kg': sum(e['carbon_kg'] for e in filtered),
            'total_negawatt': sum(e['negawatt_reward'] for e in filtered),
            'entries': filtered[-100:]
        }
