# src/enhancements/dual_accountant.py

"""
Dual Carbon Accounting for Green Agent
Scientific basis: GHG Protocol Scope 2 guidance (location-based vs market-based)

Reference: "GHG Protocol Scope 2 Guidance" (World Resources Institute, 2015)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class PPAContract:
    """Power Purchase Agreement contract"""
    contract_id: str
    renewable_type: str  # 'solar', 'wind', 'hydro'
    capacity_mw: float
    start_date: date
    end_date: date
    hourly_allocation: Dict[int, float]  # hour_of_day -> allocated MWh


@dataclass
class RECertificate:
    """Renewable Energy Certificate"""
    cert_id: str
    vintage_year: int
    renewable_type: str
    mwh_volume: float
    is_additional: bool  # Additionality: would project exist without REC?
    retired: bool = False


@dataclass
class CarbonAccounting:
    """Complete carbon accounting result"""
    task_id: str
    timestamp: datetime
    energy_consumption_kwh: float
    location_based_emissions_kg: float
    market_based_emissions_kg: float
    ppa_coverage_percent: float
    rec_coverage_percent: float
    residual_emissions_kg: float
    reporting_recommendation: str
    hash: str = ""


class DualCarbonAccountant:
    """
    Dual carbon accounting with PPA and REC tracking.
    
    Location-based: Uses grid average emissions
    Market-based: Uses contractual instruments (PPAs, RECs)
    
    GHG Protocol requires reporting both, but decisions can use market-based
    if RECs have additionality.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.ppa_contracts: List[PPAContract] = []
        self.rec_portfolio: List[RECertificate] = []
        self.accounting_ledger: List[CarbonAccounting] = []
        
        # Load initial data
        self._load_contracts()
        self._load_recs()
    
    def _load_contracts(self):
        """Load PPA contracts from configuraion"""
        # Example PPA
        self.ppa_contracts.append(PPAContract(
            contract_id='PPA-001',
            renewable_type='solar',
            capacity_mw=50.0,
            start_date=date(2024, 1, 1),
            end_date=date(2034, 12, 31),
            hourly_allocation={h: 50.0 / 24 for h in range(24)}
        ))
        
        self.ppa_contracts.append(PPAContract(
            contract_id='PPA-002',
            renewable_type='wind',
            capacity_mw=30.0,
            start_date=date(2023, 6, 1),
            end_date=date(2033, 5, 31),
            hourly_allocation={h: 30.0 / 24 for h in range(24)}
        ))
    
    def _load_recs(self):
        """Load REC portfolio from configuration"""
        self.rec_portfolio.append(RECertificate(
            cert_id='REC-2024-001',
            vintage_year=2024,
            renewable_type='solar',
            mwh_volume=1000.0,
            is_additional=True,
            retired=False
        ))
        
        self.rec_portfolio.append(RECertificate(
            cert_id='REC-2024-002',
            vintage_year=2024,
            renewable_type='wind',
            mwh_volume=500.0,
            is_additional=False,  # Non-additional (would exist anyway)
            retired=False
        ))
    
    def allocate_ppa_energy(self, timestamp: datetime, energy_kwh: float) -> float:
        """
        Allocate PPA energy to a task based on time of day.
        
        Returns allocated PPA energy in kWh.
        """
        hour_of_day = timestamp.hour
        total_ppa_kw = 0
        
        for contract in self.ppa_contracts:
            if contract.start_date <= timestamp.date() <= contract.end_date:
                # Get hourly allocation in kW
                hourly_mw = contract.hourly_allocation.get(hour_of_day, 0)
                total_ppa_kw += hourly_mw * 1000  # Convert MW to kW
        
        # PPA can't exceed actual consumption
        allocated = min(energy_kwh, total_ppa_kw * 1)  # Assume 1 hour allocation
        
        return allocated
    
    def allocate_rec_energy(self, energy_kwh: float, require_additionality: bool = True) -> float:
        """
        Allocate RECs to energy consumption.
        
        Args:
            energy_kwh: Energy consumed in kWh
            require_additionality: If True, only use additional RECs
            
        Returns allocated REC energy in kWh.
        """
        # Get available RECs
        available_recs = [r for r in self.rec_portfolio if not r.retired]
        
        if require_additionality:
            available_recs = [r for r in available_recs if r.is_additional]
        
        total_rec_mwh = sum(r.mwh_volume for r in available_recs)
        total_rec_kwh = total_rec_mwh * 1000
        
        # Allocate up to consumption
        allocated = min(energy_kwh, total_rec_kwh)
        
        # Mark RECs as retired (simplified)
        remaining = allocated / 1000  # Convert to MWh for retirement
        for rec in available_recs:
            if remaining <= 0:
                break
            retire_amount = min(rec.mwh_volume, remaining)
            rec.mwh_volume -= retire_amount
            remaining -= retire_amount
            if rec.mwh_volume <= 0:
                rec.retired = True
        
        return allocated
    
    def get_residual_mix_intensity(self, region: str, timestamp: datetime) -> float:
        """
        Get residual grid mix intensity after removing PPAs and RECs.
        
        Residual = Grid average adjusted for contractual instruments.
        """
        # Base grid intensity for region (example values)
        grid_intensity = self._get_grid_intensity(region, timestamp)
        
        # Adjust for PPA and REC coverage if needed
        # In practice, residual mix is reported by grid operators
        
        return grid_intensity
    
    def _get_grid_intensity(self, region: str, timestamp: datetime) -> float:
        """Get location-based grid intensity (gCO2/kWh)"""
        # Simplified regional intensities (gCO2/kWh)
        regional_intensities = {
            'us-east': 380.0,
            'us-west': 250.0,
            'us-central': 450.0,
            'eu-north': 80.0,
            'eu-west': 220.0,
            'asia-pacific': 550.0
        }
        
        return regional_intensities.get(region, 400.0)
    
    def account_carbon(self, task_id: str, energy_consumption_kwh: float,
                      region: str, timestamp: datetime) -> CarbonAccounting:
        """
        Perform dual carbon accounting for a task.
        
        Returns both location-based and market-based emissions.
        """
        # Location-based accounting
        grid_intensity = self._get_grid_intensity(region, timestamp)
        location_emissions = energy_consumption_kwh * grid_intensity / 1000
        
        # Market-based accounting
        ppa_allocated = self.allocate_ppa_energy(timestamp, energy_consumption_kwh)
        rec_allocated = self.allocate_rec_energy(energy_consumption_kwh - ppa_allocated)
        
        # Residual energy
        residual_energy = energy_consumption_kwh - ppa_allocated - rec_allocated
        residual_intensity = self.get_residual_mix_intensity(region, timestamp)
        residual_emissions = residual_energy * residual_intensity / 1000
        
        # Market-based emissions = residual emissions only
        market_emissions = residual_emissions
        
        # Coverage percentages
        ppa_coverage = (ppa_allocated / energy_consumption_kwh * 100) if energy_consumption_kwh > 0 else 0
        rec_coverage = (rec_allocated / energy_consumption_kwh * 100) if energy_consumption_kwh > 0 else 0
        
        # Determine reporting recommendation
        reporting_recommendation = self._select_reporting_method(
            location_emissions, market_emissions, self._check_rec_quality()
        )
        
        # Create accounting record
        accounting = CarbonAccounting(
            task_id=task_id,
            timestamp=timestamp,
            energy_consumption_kwh=energy_consumption_kwh,
            location_based_emissions_kg=location_emissions,
            market_based_emissions_kg=market_emissions,
            ppa_coverage_percent=ppa_coverage,
            rec_coverage_percent=rec_coverage,
            residual_emissions_kg=residual_emissions,
            reporting_recommendation=reporting_recommendation
        )
        
        # Calculate cryptographic hash for immutability
        accounting.hash = self._calculate_hash(accounting)
        
        # Store in ledger
        self.accounting_ledger.append(accounting)
        
        logger.info(f"Carbon accounting for {task_id}: location={location_emissions:.2f}kg, "
                   f"market={market_emissions:.2f}kg, PPA={ppa_coverage:.1f}%, REC={rec_coverage:.1f}%")
        
        return accounting
    
    def _select_reporting_method(self, location_emissions: float, 
                                  market_emissions: float,
                                  recs_are_additional: bool) -> str:
        """Select appropriate reporting method for decisions"""
        if recs_are_additional and market_emissions < location_emissions:
            return 'MARKET_BASED'
        else:
            return 'LOCATION_BASED'
    
    def _check_rec_quality(self) -> bool:
        """Check if RECs in portfolio have additionality"""
        additional_recs = [r for r in self.rec_portfolio if r.is_additional and not r.retired]
        return len(additional_recs) > 0
    
    def _calculate_hash(self, accounting: CarbonAccounting) -> str:
        """Calculate SHA-256 hash for immutability"""
        data = {
            'task_id': accounting.task_id,
            'timestamp': accounting.timestamp.isoformat(),
            'energy_kwh': accounting.energy_consumption_kwh,
            'location_emissions': accounting.location_based_emissions_kg,
            'market_emissions': accounting.market_based_emissions_kg
        }
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def get_emissions_ledger(self, task_id: Optional[str] = None) -> List[Dict]:
        """Get emissions ledger entries"""
        if task_id:
            entries = [a for a in self.accounting_ledger if a.task_id == task_id]
        else:
            entries = self.accounting_ledger
        
        return [
            {
                'task_id': e.task_id,
                'timestamp': e.timestamp.isoformat(),
                'location_emissions_kg': e.location_based_emissions_kg,
                'market_emissions_kg': e.market_based_emissions_kg,
                'ppa_coverage': e.ppa_coverage_percent,
                'rec_coverage': e.rec_coverage_percent,
                'hash': e.hash
            }
            for e in entries
        ]
    
    def verify_integrity(self) -> bool:
        """Verify integrity of accounting ledger"""
        for entry in self.accounting_ledger:
            expected_hash = self._calculate_hash(entry)
            if entry.hash != expected_hash:
                logger.error(f"Integrity check failed for task {entry.task_id}")
                return False
        return True
    
    def get_carbon_credit_eligible(self) -> float:
        """
        Calculate carbon credits earned from renewable energy.
        
        Only additional RECs that reduce emissions beyond baseline qualify.
        """
        baseline_emissions = sum(e.location_based_emissions_kg for e in self.accounting_ledger)
        actual_emissions = sum(e.market_based_emissions_kg for e in self.accounting_ledger)
        
        credits = max(0, baseline_emissions - actual_emissions)
        
        return credits
