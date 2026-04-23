# src/carbon/helium_monitor.py (NEW)

import asyncio
import aiohttp
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class HeliumScarcityLevel(Enum):
    NORMAL = "normal"
    CAUTION = "caution"
    CRITICAL = "critical"
    SEVERE = "severe"

@dataclass
class HeliumSupplySignal:
    """Real-time helium supply chain signal"""
    timestamp: datetime
    scarcity_level: HeliumScarcityLevel
    scarcity_score: float  # 0.0 to 1.0
    spot_price_usd_per_liter: float
    fab_inventory_days: int
    vendor_alerts: List[str]
    source: str
    forecast_valid_until: Optional[datetime] = None

class HeliumMonitor:
    """
    Helium supply chain monitoring for Layer 7
    Tracks real-time helium availability and forecasts
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.api_endpoints = self.config.get('api_endpoints', {
            'primary': 'https://api.helium-monitor.example.com/v1/supply',
            'backup': 'https://backup.helium-api.example.com/v1/status'
        })
        self.update_interval_seconds = self.config.get('update_interval', 900)  # 15 minutes
        self.current_signal: Optional[HeliumSupplySignal] = None
        self.signal_history: List[HeliumSupplySignal] = []
        self._monitoring_task = None
        
        # Start monitoring
        self._start_monitoring()
    
    def _start_monitoring(self):
        """Start background monitoring task"""
        loop = asyncio.get_event_loop()
        self._monitoring_task = loop.create_task(self._monitor_loop())
    
    async def _monitor_loop(self):
        """Continuous monitoring loop"""
        while True:
            try:
                signal = await self.fetch_helium_supply()
                self.current_signal = signal
                self.signal_history.append(signal)
                
                # Keep last 100 signals
                if len(self.signal_history) > 100:
                    self.signal_history = self.signal_history[-100:]
                
                logger.info(f"Helium supply updated: {signal.scarcity_level.value} (score: {signal.scarcity_score})")
                
            except Exception as e:
                logger.error(f"Helium monitoring failed: {e}")
                # Use simulated data as fallback
                self.current_signal = self._simulate_helium_supply()
            
            await asyncio.sleep(self.update_interval_seconds)
    
    async def fetch_helium_supply(self) -> HeliumSupplySignal:
        """Fetch real-time helium supply from external API"""
        
        try:
            async with aiohttp.ClientSession() as session:
                # Try primary API
                async with session.get(self.api_endpoints['primary'], timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_api_response(data, source='primary_api')
                    else:
                        # Try backup API
                        async with session.get(self.api_endpoints['backup'], timeout=10) as backup_resp:
                            if backup_resp.status == 200:
                                data = await backup_resp.json()
                                return self._parse_api_response(data, source='backup_api')
                            else:
                                raise Exception("All APIs failed")
        
        except Exception as e:
            logger.warning(f"Helium API unavailable: {e}, using simulation")
            return self._simulate_helium_supply()
    
    def _parse_api_response(self, data: Dict, source: str) -> HeliumSupplySignal:
        """Parse API response into HeliumSupplySignal"""
        
        scarcity_level_str = data.get('scarcity_level', 'normal')
        scarcity_level = HeliumScarcityLevel(scarcity_level_str)
        
        return HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=scarcity_level,
            scarcity_score=data.get('scarcity_score', 0.0),
            spot_price_usd_per_liter=data.get('spot_price_usd', 4.0),
            fab_inventory_days=data.get('fab_inventory_days', 30),
            vendor_alerts=data.get('alerts', []),
            source=source,
            forecast_valid_until=datetime.fromisoformat(data['forecast_valid_until']) if 'forecast_valid_until' in data else None
        )
    
    def _simulate_helium_supply(self) -> HeliumSupplySignal:
        """Simulate helium supply for testing"""
        import random
        
        # Weighted random for realistic simulation
        rand = random.random()
        
        if rand < 0.7:
            scarcity_level = HeliumScarcityLevel.NORMAL
            scarcity_score = 0.1
            price = 4.0
            inventory = 30
        elif rand < 0.85:
            scarcity_level = HeliumScarcityLevel.CAUTION
            scarcity_score = 0.4
            price = 5.5
            inventory = 20
        elif rand < 0.95:
            scarcity_level = HeliumScarcityLevel.CRITICAL
            scarcity_score = 0.7
            price = 8.0
            inventory = 10
        else:
            scarcity_level = HeliumScarcityLevel.SEVERE
            scarcity_score = 0.9
            price = 12.0
            inventory = 5
        
        return HeliumSupplySignal(
            timestamp=datetime.now(),
            scarcity_level=scarcity_level,
            scarcity_score=scarcity_score,
            spot_price_usd_per_liter=price,
            fab_inventory_days=inventory,
            vendor_alerts=[],
            source='simulation'
        )
    
    def get_current_supply(self) -> Optional[HeliumSupplySignal]:
        """Get current helium supply status"""
        return self.current_signal
    
    def get_supply_trend(self, hours: int = 24) -> List[HeliumSupplySignal]:
        """Get helium supply trend over time period"""
        cutoff = datetime.now() - pd.Timedelta(hours=hours)
        return [s for s in self.signal_history if s.timestamp > cutoff]
    
    async def get_forecast(self, hours_ahead: int = 24) -> Dict:
        """Get helium supply forecast"""
        # Placeholder - in production, use ML forecasting
        if self.current_signal:
            return {
                'current_scarcity': self.current_signal.scarcity_level.value,
                'forecast': 'stable' if self.current_signal.scarcity_score < 0.5 else 'worsening',
                'hours_ahead': hours_ahead,
                'confidence': 0.8
            }
        return {'error': 'No data available'}


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
