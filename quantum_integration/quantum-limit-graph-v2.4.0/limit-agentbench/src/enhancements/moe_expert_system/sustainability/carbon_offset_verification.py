# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/sustainability/carbon_offset_verification.py

"""
Automated Carbon Offset Verification System
Version: 1.0.0

Features:
- Blockchain-based offset registry integration
- Satellite imagery verification pipeline
- IoT sensor network validation
- Additionality assessment engine
- Permanence risk scoring
- Real-time carbon accounting
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import hashlib
import json
import requests
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class OffsetRegistry(Enum):
    """Supported carbon offset registries"""
    VERRA = "verra"
    GOLD_STANDARD = "gold_standard"
    CLIMATE_ACTION_RESERVE = "climate_action_reserve"
    AMERICAN_CARBON_REGISTRY = "american_carbon_registry"
    PLAN_VIVO = "plan_vivo"
    PURO_EARTH = "puro_earth"
    CUSTOM_BLOCKCHAIN = "custom_blockchain"

class ProjectType(Enum):
    """Carbon offset project types"""
    REFORESTATION = "reforestation"
    AVOIDED_DEFORESTATION = "avoided_deforestation"
    RENEWABLE_ENERGY = "renewable_energy"
    METHANE_CAPTURE = "methane_capture"
    DIRECT_AIR_CAPTURE = "direct_air_capture"
    BIOCHAR = "biochar"
    SOIL_CARBON = "soil_carbon"
    BLUE_CARBON = "blue_carbon"
    ENHANCED_WEATHERING = "enhanced_weathering"
    OCEAN_ALKALINIZATION = "ocean_alkalinization"

class VerificationStatus(Enum):
    """Offset verification status"""
    PENDING = "pending"
    VERIFIED = "verified"
    FAILED = "failed"
    DISPUTED = "disputed"
    REVOKED = "revoked"
    EXPIRED = "expired"

class AdditionalityLevel(Enum):
    """Additionality assessment levels"""
    NOT_ASSESSED = "not_assessed"
    LIKELY_ADDITIONAL = "likely_additional"
    PROVEN_ADDITIONAL = "proven_additional"
    NOT_ADDITIONAL = "not_additional"
    UNCERTAIN = "uncertain"

class PermanenceRisk(Enum):
    """Permanence risk levels"""
    VERY_LOW = "very_low"       # 1000+ years (DAC, mineralization)
    LOW = "low"                 # 100+ years (biochar, enhanced weathering)
    MODERATE = "moderate"       # 30-100 years (reforestation)
    HIGH = "high"               # 10-30 years (soil carbon)
    VERY_HIGH = "very_high"     # <10 years (some agricultural)

@dataclass
class CarbonCredit:
    """Verified carbon credit"""
    credit_id: str
    registry: OffsetRegistry
    project_type: ProjectType
    amount_kg: float
    vintage_year: int
    verification_status: VerificationStatus
    additionality: AdditionalityLevel
    permanence_risk: PermanenceRisk
    project_location: Dict[str, float]  # lat, lon
    verification_date: datetime
    expiry_date: datetime
    blockchain_tx_hash: Optional[str] = None
    satellite_verified: bool = False
    sensor_verified: bool = False
    retirement_date: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def effective_amount(self) -> float:
        """Calculate effective carbon offset considering permanence risk"""
        risk_discounts = {
            PermanenceRisk.VERY_LOW: 1.0,
            PermanenceRisk.LOW: 0.95,
            PermanenceRisk.MODERATE: 0.85,
            PermanenceRisk.HIGH: 0.70,
            PermanenceRisk.VERY_HIGH: 0.50
        }
        discount = risk_discounts.get(self.permanence_risk, 0.85)
        
        # Additional discount for non-additional projects
        if self.additionality == AdditionalityLevel.NOT_ADDITIONAL:
            discount *= 0.5
        elif self.additionality == AdditionalityLevel.UNCERTAIN:
            discount *= 0.75
        
        return self.amount_kg * discount

@dataclass
class SatelliteVerification:
    """Satellite imagery verification result"""
    verification_id: str
    project_id: str
    satellite_source: str  # Sentinel-2, Landsat, Planet
    image_date: datetime
    ndvi_mean: float
    ndvi_change: float
    forest_cover_percent: float
    deforestation_detected: bool
    project_boundary_violation: bool
    carbon_sequestration_estimate_kg: float
    confidence_score: float
    anomaly_detected: bool
    verification_timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class SensorValidation:
    """IoT sensor validation result"""
    validation_id: str
    project_id: str
    sensor_id: str
    sensor_type: str  # CO2, CH4, temperature, humidity
    measurements: List[Dict[str, Any]]
    mean_value: float
    standard_deviation: float
    expected_range: Tuple[float, float]
    within_expected_range: bool
    data_quality_score: float
    cryptographic_signature: str
    validation_timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class AdditionalityAssessment:
    """Additionality assessment result"""
    assessment_id: str
    project_id: str
    financial_additionality: bool
    regulatory_additionality: bool
    barrier_analysis: Dict[str, bool]
    common_practice_analysis: bool
    counterfactual_scenario: str
    overall_assessment: AdditionalityLevel
    confidence_score: float
    assessor: str
    assessment_date: datetime = field(default_factory=datetime.utcnow)

@dataclass
class RealTimeCarbonAccount:
    """Real-time carbon accounting entry"""
    account_id: str
    timestamp: datetime
    scope1_emissions_kg: float  # Direct emissions
    scope2_emissions_kg: float  # Energy emissions
    scope3_emissions_kg: float  # Supply chain emissions
    verified_offsets_kg: float
    pending_offsets_kg: float
    net_position_kg: float
    carbon_budget_remaining_kg: float
    budget_status: str  # compliant, warning, exceeded

# ============================================================================
# Blockchain Registry Integration
# ============================================================================

class BlockchainRegistryConnector:
    """
    Connects to blockchain-based carbon credit registries.
    
    Supports:
    - Verra, Gold Standard, Climate Action Reserve
    - Custom blockchain registries
    - Credit verification and retirement
    - Double-counting prevention
    """
    
    def __init__(self):
        self.registry_endpoints = {
            OffsetRegistry.VERRA: "https://api.verra.org/v2",
            OffsetRegistry.GOLD_STANDARD: "https://api.goldstandard.org/v1",
            OffsetRegistry.CLIMATE_ACTION_RESERVE: "https://api.climateactionreserve.org/v1",
            OffsetRegistry.AMERICAN_CARBON_REGISTRY: "https://api.americancarbonregistry.org/v1",
            OffsetRegistry.PURO_EARTH: "https://api.puro.earth/v1"
        }
        
        self.verified_credits: Dict[str, CarbonCredit] = {}
        self.retired_credits: Dict[str, CarbonCredit] = {}
        self.verification_cache: Dict[str, Dict] = {}
        
        # Local blockchain for audit trail
        self.audit_chain: List[Dict] = []
        self.chain_hash = "0" * 64
        
        logger.info("Blockchain Registry Connector initialized")
    
    async def verify_credit(
        self,
        credit_id: str,
        registry: OffsetRegistry
    ) -> Tuple[bool, Optional[CarbonCredit]]:
        """
        Verify carbon credit against blockchain registry.
        
        Returns (is_valid, carbon_credit)
        """
        # Check cache
        cache_key = f"{registry.value}_{credit_id}"
        if cache_key in self.verification_cache:
            cached = self.verification_cache[cache_key]
            if datetime.utcnow() < cached['expires_at']:
                return True, cached['credit']
        
        try:
            # Query registry (simulated)
            registry_data = await self._query_registry(credit_id, registry)
            
            if not registry_data or not registry_data.get('valid'):
                return False, None
            
            # Create credit object
            credit = CarbonCredit(
                credit_id=credit_id,
                registry=registry,
                project_type=ProjectType(registry_data.get('project_type', 'reforestation')),
                amount_kg=registry_data.get('amount_kg', 0),
                vintage_year=registry_data.get('vintage_year', datetime.utcnow().year),
                verification_status=VerificationStatus.VERIFIED,
                additionality=AdditionalityLevel(
                    registry_data.get('additionality', 'likely_additional')
                ),
                permanence_risk=PermanenceRisk(
                    registry_data.get('permanence_risk', 'moderate')
                ),
                project_location=registry_data.get('location', {'lat': 0, 'lon': 0}),
                verification_date=datetime.fromisoformat(
                    registry_data.get('verification_date', datetime.utcnow().isoformat())
                ),
                expiry_date=datetime.fromisoformat(
                    registry_data.get('expiry_date', 
                        (datetime.utcnow() + timedelta(days=365)).isoformat())
                ),
                blockchain_tx_hash=registry_data.get('tx_hash')
            )
            
            # Cache result
            self.verification_cache[cache_key] = {
                'credit': credit,
                'expires_at': datetime.utcnow() + timedelta(hours=24)
            }
            
            # Store verified credit
            self.verified_credits[credit_id] = credit
            
            # Record to audit chain
            self._record_audit_entry('verify_credit', {
                'credit_id': credit_id,
                'registry': registry.value,
                'valid': True,
                'amount_kg': credit.amount_kg
            })
            
            logger.info(f"Verified credit {credit_id}: {credit.amount_kg:.2f} kg CO2")
            
            return True, credit
            
        except Exception as e:
            logger.error(f"Credit verification failed: {str(e)}")
            return False, None
    
    async def _query_registry(
        self,
        credit_id: str,
        registry: OffsetRegistry
    ) -> Optional[Dict[str, Any]]:
        """Query blockchain registry for credit information"""
        # Simulated registry query
        # In production, would make API calls to actual registries
        
        # Check for double-counting in our ledger
        if credit_id in self.retired_credits:
            logger.warning(f"Credit {credit_id} already retired - double counting detected!")
            return None
        
        # Simulate valid credit
        return {
            'valid': True,
            'credit_id': credit_id,
            'project_type': np.random.choice([
                'reforestation', 'renewable_energy', 'direct_air_capture', 'biochar'
            ]),
            'amount_kg': np.random.uniform(100, 10000),
            'vintage_year': datetime.utcnow().year,
            'additionality': np.random.choice([
                'proven_additional', 'likely_additional'
            ]),
            'permanence_risk': np.random.choice([
                'low', 'moderate'
            ]),
            'location': {
                'lat': np.random.uniform(-30, 30),
                'lon': np.random.uniform(-180, 180)
            },
            'verification_date': datetime.utcnow().isoformat(),
            'expiry_date': (datetime.utcnow() + timedelta(days=365)).isoformat(),
            'tx_hash': hashlib.sha256(f"{credit_id}{datetime.utcnow()}".encode()).hexdigest()
        }
    
    async def retire_credit(
        self,
        credit_id: str,
        amount_kg: Optional[float] = None
    ) -> Tuple[bool, str]:
        """
        Retire carbon credit on blockchain.
        
        Prevents double-counting by marking credit as retired.
        """
        if credit_id not in self.verified_credits:
            return False, "Credit not verified"
        
        credit = self.verified_credits[credit_id]
        
        if credit.retirement_date:
            return False, "Credit already retired"
        
        retire_amount = amount_kg or credit.amount_kg
        
        if retire_amount > credit.amount_kg:
            return False, f"Retirement amount {retire_amount} exceeds credit {credit.amount_kg}"
        
        # Simulate blockchain retirement transaction
        retirement_tx = hashlib.sha256(
            f"retire_{credit_id}_{datetime.utcnow().timestamp()}".encode()
        ).hexdigest()
        
        credit.retirement_date = datetime.utcnow()
        self.retired_credits[credit_id] = credit
        
        # Record to audit chain
        self._record_audit_entry('retire_credit', {
            'credit_id': credit_id,
            'amount_kg': retire_amount,
            'retirement_tx': retirement_tx
        })
        
        logger.info(f"Retired credit {credit_id}: {retire_amount:.2f} kg CO2")
        
        return True, retirement_tx
    
    def _record_audit_entry(self, action: str, data: Dict[str, Any]):
        """Record action to audit chain"""
        entry = {
            'entry_id': len(self.audit_chain) + 1,
            'timestamp': datetime.utcnow().isoformat(),
            'previous_hash': self.chain_hash,
            'action': action,
            'data': data
        }
        
        entry_hash = hashlib.sha256(
            json.dumps(entry, sort_keys=True, default=str).encode()
        ).hexdigest()
        
        entry['entry_hash'] = entry_hash
        self.chain_hash = entry_hash
        self.audit_chain.append(entry)
    
    def verify_chain_integrity(self) -> bool:
        """Verify audit chain integrity"""
        for i in range(1, len(self.audit_chain)):
            current = self.audit_chain[i]
            previous = self.audit_chain[i-1]
            
            if current['previous_hash'] != previous['entry_hash']:
                return False
            
            computed = hashlib.sha256(
                json.dumps(
                    {k: v for k, v in current.items() if k != 'entry_hash'},
                    sort_keys=True, default=str
                ).encode()
            ).hexdigest()
            
            if computed != current['entry_hash']:
                return False
        
        return True
    
    def get_retired_credits_summary(self) -> Dict[str, Any]:
        """Get summary of retired credits"""
        total_retired = sum(c.amount_kg for c in self.retired_credits.values())
        total_effective = sum(c.effective_amount for c in self.retired_credits.values())
        
        return {
            'total_credits_retired': len(self.retired_credits),
            'total_amount_kg': total_retired,
            'total_effective_amount_kg': total_effective,
            'average_permanence_discount': 1 - (total_effective / max(total_retired, 1)),
            'retired_by_project_type': {
                pt.value: sum(
                    c.amount_kg for c in self.retired_credits.values()
                    if c.project_type == pt
                )
                for pt in ProjectType
            }
        }

# ============================================================================
# Satellite Imagery Verification
# ============================================================================

class SatelliteVerificationEngine:
    """
    Satellite imagery analysis for carbon offset verification.
    
    Features:
    - Multi-satellite support (Sentinel-2, Landsat, Planet)
    - NDVI calculation and change detection
    - Forest cover monitoring
    - Deforestation detection
    - Project boundary compliance
    """
    
    def __init__(self):
        self.satellite_sources = {
            'sentinel-2': {
                'resolution_m': 10,
                'revisit_days': 5,
                'bands': ['B2', 'B3', 'B4', 'B8'],
                'api_endpoint': 'https://api.sentinel-hub.com/v2'
            },
            'landsat-8': {
                'resolution_m': 30,
                'revisit_days': 16,
                'bands': ['B4', 'B5'],
                'api_endpoint': 'https://api.landsat.org/v1'
            },
            'planet': {
                'resolution_m': 3,
                'revisit_days': 1,
                'bands': ['red', 'nir'],
                'api_endpoint': 'https://api.planet.com/v1'
            }
        }
        
        self.verification_history: List[SatelliteVerification] = []
        self.project_baselines: Dict[str, Dict] = {}
        
        logger.info("Satellite Verification Engine initialized")
    
    async def verify_project(
        self,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        baseline_year: int = 2020
    ) -> SatelliteVerification:
        """
        Verify carbon offset project using satellite imagery.
        
        Args:
            project_id: Project identifier
            project_location: {'lat': float, 'lon': float}
            project_area_km2: Project area in square kilometers
            baseline_year: Baseline year for comparison
            
        Returns:
            SatelliteVerification result
        """
        # Select best satellite source based on project size
        satellite_source = self._select_satellite(project_area_km2)
        
        # Fetch satellite imagery (simulated)
        imagery = await self._fetch_imagery(
            project_location, project_area_km2, satellite_source
        )
        
        # Calculate NDVI
        ndvi_current = self._calculate_ndvi(imagery)
        
        # Get baseline NDVI
        baseline_imagery = await self._fetch_baseline_imagery(
            project_location, project_area_km2, baseline_year
        )
        ndvi_baseline = self._calculate_ndvi(baseline_imagery)
        
        # Calculate changes
        ndvi_change = ndvi_current - ndvi_baseline
        
        # Detect forest cover
        forest_cover = self._calculate_forest_cover(ndvi_current)
        
        # Check for deforestation
        deforestation_detected = ndvi_change < -0.1
        
        # Check project boundaries
        boundary_violation = self._check_boundary_compliance(
            project_location, project_area_km2
        )
        
        # Estimate carbon sequestration
        carbon_estimate = self._estimate_sequestration(
            ndvi_change, project_area_km2
        )
        
        # Calculate confidence
        confidence = self._calculate_confidence(
            satellite_source, project_area_km2, ndvi_change
        )
        
        verification = SatelliteVerification(
            verification_id=f"sat_{project_id}_{datetime.utcnow().timestamp()}",
            project_id=project_id,
            satellite_source=satellite_source,
            image_date=datetime.utcnow(),
            ndvi_mean=ndvi_current,
            ndvi_change=ndvi_change,
            forest_cover_percent=forest_cover,
            deforestation_detected=deforestation_detected,
            project_boundary_violation=boundary_violation,
            carbon_sequestration_estimate_kg=carbon_estimate,
            confidence_score=confidence,
            anomaly_detected=deforestation_detected or boundary_violation
        )
        
        self.verification_history.append(verification)
        
        logger.info(
            f"Satellite verification for {project_id}: "
            f"NDVI change={ndvi_change:.3f}, "
            f"carbon={carbon_estimate:.0f}kg, "
            f"confidence={confidence:.2f}"
        )
        
        return verification
    
    def _select_satellite(self, area_km2: float) -> str:
        """Select appropriate satellite based on project size"""
        if area_km2 < 1:
            return 'planet'  # High resolution for small areas
        elif area_km2 < 100:
            return 'sentinel-2'  # Medium resolution
        else:
            return 'landsat-8'  # Wide coverage
    
    async def _fetch_imagery(
        self,
        location: Dict[str, float],
        area_km2: float,
        source: str
    ) -> Dict[str, np.ndarray]:
        """Fetch satellite imagery (simulated)"""
        # Simulate imagery data
        resolution = self.satellite_sources[source]['resolution_m']
        pixels = int(np.sqrt(area_km2 * 1e6) / resolution)
        
        # Generate realistic NDVI-like data
        red_band = np.random.beta(2, 5, (pixels, pixels))
        nir_band = red_band + np.random.normal(0.3, 0.1, (pixels, pixels))
        nir_band = np.clip(nir_band, 0, 1)
        
        return {
            'red': red_band,
            'nir': nir_band,
            'pixels': pixels,
            'resolution_m': resolution
        }
    
    async def _fetch_baseline_imagery(
        self,
        location: Dict[str, float],
        area_km2: float,
        year: int
    ) -> Dict[str, np.ndarray]:
        """Fetch baseline imagery from historical data"""
        # Simulate baseline with lower NDVI (less vegetation historically)
        imagery = await self._fetch_imagery(location, area_km2, 'landsat-8')
        
        # Reduce NDVI for baseline (assuming improvement over time)
        imagery['red'] = imagery['red'] * 0.8 + 0.2
        imagery['nir'] = imagery['nir'] * 0.7
        
        return imagery
    
    def _calculate_ndvi(self, imagery: Dict[str, np.ndarray]) -> float:
        """Calculate Normalized Difference Vegetation Index"""
        red = imagery['red']
        nir = imagery['nir']
        
        ndvi = (nir - red) / (nir + red + 1e-8)
        
        return float(np.mean(ndvi))
    
    def _calculate_forest_cover(self, ndvi: float) -> float:
        """Estimate forest cover percentage from NDVI"""
        # NDVI > 0.6 typically indicates dense vegetation
        cover = min(100, max(0, (ndvi - 0.2) / 0.6 * 100))
        return cover
    
    def _check_boundary_compliance(
        self,
        location: Dict[str, float],
        area_km2: float
    ) -> bool:
        """Check if project stays within declared boundaries"""
        # Simulate boundary check
        # In production, would use GIS polygon containment
        return np.random.random() < 0.95  # 95% compliance
    
    def _estimate_sequestration(
        self,
        ndvi_change: float,
        area_km2: float
    ) -> float:
        """Estimate carbon sequestration from NDVI change"""
        # Simplified estimation
        # 1 unit NDVI increase ≈ 50 tC/ha/year for tropical forests
        carbon_per_ha = ndvi_change * 50 * 3.67  # Convert C to CO2
        area_ha = area_km2 * 100
        
        return carbon_per_ha * area_ha * 1000  # Convert to kg
    
    def _calculate_confidence(
        self,
        source: str,
        area_km2: float,
        ndvi_change: float
    ) -> float:
        """Calculate verification confidence score"""
        resolution_factor = {
            'planet': 1.0,
            'sentinel-2': 0.9,
            'landsat-8': 0.8
        }.get(source, 0.7)
        
        area_factor = min(1.0, area_km2 / 10)  # Larger areas = more confidence
        change_factor = min(1.0, abs(ndvi_change) * 5)  # Larger changes = easier to detect
        
        confidence = resolution_factor * 0.4 + area_factor * 0.3 + change_factor * 0.3
        
        return confidence
    
    def get_verification_summary(self) -> Dict[str, Any]:
        """Get verification history summary"""
        if not self.verification_history:
            return {}
        
        recent = self.verification_history[-50:]
        
        return {
            'total_verifications': len(self.verification_history),
            'average_ndvi_change': np.mean([v.ndvi_change for v in recent]),
            'deforestation_detected_count': sum(1 for v in recent if v.deforestation_detected),
            'average_confidence': np.mean([v.confidence_score for v in recent]),
            'total_sequestration_estimated_kg': sum(v.carbon_sequestration_estimate_kg for v in recent),
            'anomaly_rate': sum(1 for v in recent if v.anomaly_detected) / max(len(recent), 1)
        }

# ============================================================================
# IoT Sensor Validation
# ============================================================================

class IoTSensorValidator:
    """
    IoT sensor network validation for carbon offset verification.
    
    Features:
    - Multi-sensor data collection
    - Statistical validation
    - Cryptographic signing
    - Anomaly detection
    """
    
    def __init__(self):
        self.registered_sensors: Dict[str, Dict] = {}
        self.sensor_readings: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.validation_history: List[SensorValidation] = []
        
        logger.info("IoT Sensor Validator initialized")
    
    def register_sensor(
        self,
        sensor_id: str,
        sensor_type: str,
        location: Dict[str, float],
        public_key: str
    ):
        """Register IoT sensor for validation"""
        self.registered_sensors[sensor_id] = {
            'sensor_type': sensor_type,
            'location': location,
            'public_key': public_key,
            'registered_at': datetime.utcnow(),
            'last_reading': None,
            'is_active': True
        }
        
        logger.info(f"Registered sensor {sensor_id} ({sensor_type})")
    
    async def collect_readings(
        self,
        sensor_id: str,
        duration_seconds: float = 60.0,
        sampling_rate_hz: float = 1.0
    ) -> List[Dict[str, Any]]:
        """Collect readings from IoT sensor"""
        if sensor_id not in self.registered_sensors:
            return []
        
        sensor = self.registered_sensors[sensor_id]
        num_samples = int(duration_seconds * sampling_rate_hz)
        
        readings = []
        for _ in range(num_samples):
            # Simulate sensor reading
            base_value = {
                'CO2': 420,
                'CH4': 1.9,
                'temperature': 25,
                'humidity': 60
            }.get(sensor['sensor_type'], 0)
            
            value = base_value + np.random.normal(0, base_value * 0.02)
            
            reading = {
                'sensor_id': sensor_id,
                'timestamp': datetime.utcnow().isoformat(),
                'value': value,
                'unit': {
                    'CO2': 'ppm',
                    'CH4': 'ppb',
                    'temperature': 'celsius',
                    'humidity': 'percent'
                }.get(sensor['sensor_type'], 'unknown')
            }
            
            # Cryptographically sign reading
            reading['signature'] = self._sign_reading(reading, sensor['public_key'])
            
            readings.append(reading)
            self.sensor_readings[sensor_id].append(reading)
        
        sensor['last_reading'] = datetime.utcnow()
        
        return readings
    
    def _sign_reading(self, reading: Dict[str, Any], public_key: str) -> str:
        """Cryptographically sign sensor reading"""
        data = json.dumps({
            'sensor_id': reading['sensor_id'],
            'timestamp': reading['timestamp'],
            'value': reading['value']
        }, sort_keys=True)
        
        # Simulate signing
        return hashlib.sha256(
            f"{data}{public_key}".encode()
        ).hexdigest()
    
    async def validate_sensor_data(
        self,
        sensor_id: str,
        expected_range: Optional[Tuple[float, float]] = None
    ) -> SensorValidation:
        """Validate sensor data quality"""
        if sensor_id not in self.sensor_readings:
            return None
        
        readings = list(self.sensor_readings[sensor_id])[-100:]
        
        if not readings:
            return None
        
        values = [r['value'] for r in readings]
        mean_value = np.mean(values)
        std_value = np.std(values)
        
        # Determine expected range if not provided
        if expected_range is None:
            expected_range = (mean_value * 0.8, mean_value * 1.2)
        
        within_range = expected_range[0] <= mean_value <= expected_range[1]
        
        # Calculate data quality
        quality = self._calculate_data_quality(values)
        
        sensor = self.registered_sensors[sensor_id]
        
        validation = SensorValidation(
            validation_id=f"val_{sensor_id}_{datetime.utcnow().timestamp()}",
            project_id=sensor.get('project_id', 'unknown'),
            sensor_id=sensor_id,
            sensor_type=sensor['sensor_type'],
            measurements=readings[-10:],
            mean_value=mean_value,
            standard_deviation=std_value,
            expected_range=expected_range,
            within_expected_range=within_range,
            data_quality_score=quality,
            cryptographic_signature=readings[-1]['signature'] if readings else ''
        )
        
        self.validation_history.append(validation)
        
        return validation
    
    def _calculate_data_quality(self, values: List[float]) -> float:
        """Calculate data quality score"""
        if len(values) < 10:
            return 0.5
        
        # Check for missing values
        completeness = 1.0
        
        # Check for outliers using IQR
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        outliers = sum(1 for v in values if v < q1 - 1.5 * iqr or v > q3 + 1.5 * iqr)
        outlier_score = 1.0 - (outliers / len(values))
        
        # Check for excessive variance
        cv = np.std(values) / (np.mean(values) + 1e-8)
        variance_score = max(0, 1.0 - cv)
        
        quality = completeness * 0.3 + outlier_score * 0.4 + variance_score * 0.3
        
        return quality
    
    def verify_sensor_signature(self, reading: Dict[str, Any]) -> bool:
        """Verify cryptographic signature of sensor reading"""
        sensor_id = reading['sensor_id']
        if sensor_id not in self.registered_sensors:
            return False
        
        public_key = self.registered_sensors[sensor_id]['public_key']
        expected_signature = self._sign_reading(reading, public_key)
        
        return reading.get('signature') == expected_signature
    
    def get_sensor_status(self) -> Dict[str, Any]:
        """Get status of all sensors"""
        return {
            sensor_id: {
                'type': info['sensor_type'],
                'is_active': info['is_active'],
                'last_reading': info['last_reading'].isoformat() if info['last_reading'] else None,
                'readings_count': len(self.sensor_readings[sensor_id])
            }
            for sensor_id, info in self.registered_sensors.items()
        }

# ============================================================================
# Additionality Assessment Engine
# ============================================================================

class AdditionalityAssessor:
    """
    Automated additionality assessment for carbon offset projects.
    
    Evaluates whether emission reductions would have occurred
    without carbon credit revenue.
    """
    
    def __init__(self):
        self.assessments: List[AdditionalityAssessment] = []
        self.counterfactual_models: Dict[str, Any] = {}
        
        logger.info("Additionality Assessor initialized")
    
    async def assess_project(
        self,
        project_id: str,
        project_type: ProjectType,
        project_location: Dict[str, float],
        financial_data: Optional[Dict[str, Any]] = None,
        regulatory_context: Optional[Dict[str, Any]] = None
    ) -> AdditionalityAssessment:
        """
        Assess additionality of carbon offset project.
        
        Returns:
            AdditionalityAssessment with detailed analysis
        """
        # Financial additionality
        financial_additional = await self._assess_financial_additionality(
            project_type, financial_data
        )
        
        # Regulatory additionality
        regulatory_additional = await self._assess_regulatory_additionality(
            project_type, project_location, regulatory_context
        )
        
        # Barrier analysis
        barriers = await self._analyze_barriers(project_type)
        
        # Common practice analysis
        common_practice = await self._analyze_common_practice(
            project_type, project_location
        )
        
        # Build counterfactual scenario
        counterfactual = self._build_counterfactual(
            project_type, financial_additional, regulatory_additional
        )
        
        # Overall assessment
        if financial_additional and regulatory_additional and common_practice:
            overall = AdditionalityLevel.PROVEN_ADDITIONAL
            confidence = 0.9
        elif financial_additional or regulatory_additional:
            overall = AdditionalityLevel.LIKELY_ADDITIONAL
            confidence = 0.7
        else:
            overall = AdditionalityLevel.NOT_ADDITIONAL
            confidence = 0.8
        
        assessment = AdditionalityAssessment(
            assessment_id=f"add_{project_id}_{datetime.utcnow().timestamp()}",
            project_id=project_id,
            financial_additionality=financial_additional,
            regulatory_additionality=regulatory_additional,
            barrier_analysis=barriers,
            common_practice_analysis=common_practice,
            counterfactual_scenario=counterfactual,
            overall_assessment=overall,
            confidence_score=confidence,
            assessor="automated_additionality_engine"
        )
        
        self.assessments.append(assessment)
        
        return assessment
    
    async def _assess_financial_additionality(
        self,
        project_type: ProjectType,
        financial_data: Optional[Dict[str, Any]]
    ) -> bool:
        """Assess financial additionality"""
        # Projects that are financially viable without carbon credits
        # are NOT additional
        inherently_profitable = [
            ProjectType.RENEWABLE_ENERGY,  # Often profitable without credits
        ]
        
        if project_type in inherently_profitable:
            # Check if carbon revenue is essential
            if financial_data:
                carbon_revenue_ratio = financial_data.get('carbon_revenue_ratio', 0)
                return carbon_revenue_ratio > 0.3  # Carbon revenue > 30% of total
            return False
        
        return True  # Most other types require carbon finance
    
    async def _assess_regulatory_additionality(
        self,
        project_type: ProjectType,
        location: Dict[str, float],
        regulatory_context: Optional[Dict[str, Any]]
    ) -> bool:
        """Assess regulatory additionality"""
        # Check if project is required by law
        # Projects that are legally required are NOT additional
        
        if regulatory_context:
            legally_required = regulatory_context.get('legally_required', False)
            if legally_required:
                return False
        
        # Simulate regulatory check
        # Most carbon offset projects exceed regulatory requirements
        return True    
    async def _analyze_barriers(
        self,
        project_type: ProjectType
    ) -> Dict[str, bool]:
        """Analyze barriers to project implementation"""
        barriers = {
            'financial_barrier': True,      # Most projects face financial barriers
            'technological_barrier': project_type in [
                ProjectType.DIRECT_AIR_CAPTURE,
                ProjectType.ENHANCED_WEATHERING
            ],
            'institutional_barrier': False,
            'social_barrier': False,
            'market_barrier': True
        }
        
        return barriers
    
    async def _analyze_common_practice(
        self,
        project_type: ProjectType,
        location: Dict[str, float]
    ) -> bool:
        """Analyze if project goes beyond common practice"""
        # Check if similar projects exist without carbon credits
        common_without_credits = [
            ProjectType.RENEWABLE_ENERGY,
        ]
        
        return project_type not in common_without_credits
    
    def _build_counterfactual(
        self,
        project_type: ProjectType,
        financial_additional: bool,
        regulatory_additional: bool
    ) -> str:
        """Build counterfactual scenario description"""
        if not financial_additional and not regulatory_additional:
            return "Project would have occurred without carbon credit revenue"
        elif financial_additional:
            return "Without carbon credits, project would not be financially viable"
        else:
            return "Without carbon credits, project would not exceed regulatory requirements"
    
    def get_additionality_summary(self) -> Dict[str, Any]:
        """Get additionality assessment summary"""
        if not self.assessments:
            return {}
        
        return {
            'total_assessments': len(self.assessments),
            'proven_additional': sum(
                1 for a in self.assessments
                if a.overall_assessment == AdditionalityLevel.PROVEN_ADDITIONAL
            ),
            'likely_additional': sum(
                1 for a in self.assessments
                if a.overall_assessment == AdditionalityLevel.LIKELY_ADDITIONAL
            ),
            'not_additional': sum(
                1 for a in self.assessments
                if a.overall_assessment == AdditionalityLevel.NOT_ADDITIONAL
            ),
            'average_confidence': np.mean([a.confidence_score for a in self.assessments])
        }

# ============================================================================
# Real-Time Carbon Accounting
# ============================================================================

class RealTimeCarbonAccountant:
    """
    Real-time carbon accounting system.
    
    Tracks emissions and offsets on per-second basis.
    """
    
    def __init__(
        self,
        carbon_budget_kg: float = 1000.0,
        accounting_interval_seconds: float = 1.0
    ):
        self.carbon_budget_kg = carbon_budget_kg
        self.accounting_interval = accounting_interval_seconds
        
        self.scope1_emissions: deque = deque(maxlen=86400)  # 24 hours at 1/sec
        self.scope2_emissions: deque = deque(maxlen=86400)
        self.scope3_emissions: deque = deque(maxlen=86400)
        
        self.verified_offsets: float = 0.0
        self.pending_offsets: float = 0.0
        
        self.account_history: deque = deque(maxlen=10000)
        
        self._running_total_scope1 = 0.0
        self._running_total_scope2 = 0.0
        self._running_total_scope3 = 0.0
        
        # Start accounting loop
        asyncio.create_task(self._accounting_loop())
        
        logger.info(f"Real-Time Carbon Accountant initialized: budget={carbon_budget_kg}kg")
    
    def record_emission(
        self,
        scope: int,
        amount_kg: float,
        source: str = "unknown"
    ):
        """Record carbon emission"""
        emission = {
            'scope': scope,
            'amount_kg': amount_kg,
            'source': source,
            'timestamp': datetime.utcnow()
        }
        
        if scope == 1:
            self.scope1_emissions.append(emission)
            self._running_total_scope1 += amount_kg
        elif scope == 2:
            self.scope2_emissions.append(emission)
            self._running_total_scope2 += amount_kg
        elif scope == 3:
            self.scope3_emissions.append(emission)
            self._running_total_scope3 += amount_kg
    
    def record_offset(
        self,
        amount_kg: float,
        verified: bool = False
    ):
        """Record carbon offset"""
        if verified:
            self.verified_offsets += amount_kg
        else:
            self.pending_offsets += amount_kg
    
    async def _accounting_loop(self):
        """Background accounting loop"""
        while True:
            try:
                account = self._generate_account()
                self.account_history.append(account)
                
                # Alert if budget exceeded
                if account['budget_status'] == 'exceeded':
                    logger.critical(
                        f"Carbon budget exceeded! Net position: {account['net_position_kg']:.2f} kg"
                    )
                elif account['budget_status'] == 'warning':
                    logger.warning(
                        f"Carbon budget warning: {account['carbon_budget_remaining_kg']:.2f} kg remaining"
                    )
                
                await asyncio.sleep(self.accounting_interval)
                
            except Exception as e:
                logger.error(f"Accounting loop error: {str(e)}")
                await asyncio.sleep(5)
    
    def _generate_account(self) -> RealTimeCarbonAccount:
        """Generate current carbon account"""
        total_scope1 = self._running_total_scope1
        total_scope2 = self._running_total_scope2
        total_scope3 = self._running_total_scope3
        
        total_emissions = total_scope1 + total_scope2 + total_scope3
        total_offsets = self.verified_offsets + self.pending_offsets * 0.5  # Discount pending
        
        net_position = total_emissions - total_offsets
        remaining_budget = self.carbon_budget_kg - net_position
        
        if remaining_budget < 0:
            budget_status = 'exceeded'
        elif remaining_budget < self.carbon_budget_kg * 0.2:
            budget_status = 'warning'
        else:
            budget_status = 'compliant'
        
        return RealTimeCarbonAccount(
            account_id=f"acct_{datetime.utcnow().timestamp()}",
            timestamp=datetime.utcnow(),
            scope1_emissions_kg=total_scope1,
            scope2_emissions_kg=total_scope2,
            scope3_emissions_kg=total_scope3,
            verified_offsets_kg=self.verified_offsets,
            pending_offsets_kg=self.pending_offsets,
            net_position_kg=net_position,
            carbon_budget_remaining_kg=remaining_budget,
            budget_status=budget_status
        )
    
    def get_current_position(self) -> RealTimeCarbonAccount:
        """Get current carbon position"""
        return self._generate_account()
    
    def get_emissions_breakdown(self) -> Dict[str, float]:
        """Get emissions breakdown by scope"""
        return {
            'scope1': self._running_total_scope1,
            'scope2': self._running_total_scope2,
            'scope3': self._running_total_scope3,
            'total': self._running_total_scope1 + self._running_total_scope2 + self._running_total_scope3
        }
    
    def get_historical_accounts(
        self,
        hours: float = 24.0
    ) -> List[RealTimeCarbonAccount]:
        """Get historical carbon accounts"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [
            a for a in self.account_history
            if a.timestamp > cutoff
        ]

# ============================================================================
# Unified Carbon Offset Verification System
# ============================================================================

class AutomatedCarbonOffsetVerification:
    """
    Complete automated carbon offset verification system.
    
    Integrates:
    - Blockchain registry verification
    - Satellite imagery analysis
    - IoT sensor validation
    - Additionality assessment
    - Real-time accounting
    """
    
    def __init__(
        self,
        carbon_budget_kg: float = 1000.0,
        enable_blockchain: bool = True,
        enable_satellite: bool = True,
        enable_sensors: bool = True,
        enable_additionality: bool = True
    ):
        self.enable_blockchain = enable_blockchain
        self.enable_satellite = enable_satellite
        self.enable_sensors = enable_sensors
        self.enable_additionality = enable_additionality
        
        # Sub-modules
        self.blockchain = BlockchainRegistryConnector() if enable_blockchain else None
        self.satellite = SatelliteVerificationEngine() if enable_satellite else None
        self.sensors = IoTSensorValidator() if enable_sensors else None
        self.additionality = AdditionalityAssessor() if enable_additionality else None
        
        # Carbon accountant
        self.accountant = RealTimeCarbonAccountant(carbon_budget_kg)
        
        # Verification history
        self.verification_records: List[Dict] = []
        
        logger.info(
            "Automated Carbon Offset Verification System initialized: "
            f"blockchain={enable_blockchain}, satellite={enable_satellite}, "
            f"sensors={enable_sensors}, additionality={enable_additionality}"
        )
    
    async def verify_and_retire_offset(
        self,
        credit_id: str,
        registry: OffsetRegistry,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        amount_to_retire_kg: float,
        project_type: Optional[ProjectType] = None
    ) -> Dict[str, Any]:
        """
        Complete verification and retirement workflow.
        
        1. Verify credit on blockchain
        2. Verify project via satellite
        3. Validate via IoT sensors
        4. Assess additionality
        5. Retire credit
        6. Update carbon accounting
        """
        result = {
            'credit_id': credit_id,
            'timestamp': datetime.utcnow().isoformat(),
            'verification_steps': {},
            'overall_success': False
        }
        
        # Step 1: Blockchain verification
        if self.enable_blockchain:
            is_valid, credit = await self.blockchain.verify_credit(credit_id, registry)
            result['verification_steps']['blockchain'] = {
                'success': is_valid,
                'amount_kg': credit.amount_kg if credit else 0,
                'effective_amount_kg': credit.effective_amount if credit else 0
            }
            
            if not is_valid:
                result['overall_success'] = False
                return result
        else:
            credit = None
        
        # Step 2: Satellite verification
        if self.enable_satellite:
            sat_verification = await self.satellite.verify_project(
                project_id, project_location, project_area_km2
            )
            result['verification_steps']['satellite'] = {
                'success': not sat_verification.anomaly_detected,
                'ndvi_change': sat_verification.ndvi_change,
                'sequestration_estimate_kg': sat_verification.carbon_sequestration_estimate_kg,
                'confidence': sat_verification.confidence_score
            }
        
        # Step 3: IoT sensor validation
        if self.enable_sensors:
            sensor_validation = await self.sensors.validate_sensor_data(
                f"sensor_{project_id}"
            )
            if sensor_validation:
                result['verification_steps']['sensors'] = {
                    'success': sensor_validation.within_expected_range,
                    'data_quality': sensor_validation.data_quality_score
                }
        
        # Step 4: Additionality assessment
        if self.enable_additionality:
            assessment = await self.additionality.assess_project(
                project_id,
                project_type or ProjectType.REFORESTATION,
                project_location
            )
            result['verification_steps']['additionality'] = {
                'success': assessment.overall_assessment in [
                    AdditionalityLevel.PROVEN_ADDITIONAL,
                    AdditionalityLevel.LIKELY_ADDITIONAL
                ],
                'level': assessment.overall_assessment.value,
                'confidence': assessment.confidence_score
            }
        
        # Step 5: Retire credit
        if self.enable_blockchain and credit:
            success, tx_hash = await self.blockchain.retire_credit(
                credit_id, amount_to_retire_kg
            )
            result['verification_steps']['retirement'] = {
                'success': success,
                'transaction_hash': tx_hash,
                'amount_retired_kg': amount_to_retire_kg
            }
            
            if success:
                # Step 6: Update carbon accounting
                effective_amount = credit.effective_amount if credit else amount_to_retire_kg
                self.accountant.record_offset(effective_amount, verified=True)
        
        # Determine overall success
        steps = result['verification_steps']
        result['overall_success'] = all(
            step.get('success', False)
            for step in steps.values()
        )
        
        self.verification_records.append(result)
        
        logger.info(
            f"Offset verification complete: {credit_id} - "
            f"success={result['overall_success']}"
        )
        
        return result
    
    def get_verification_summary(self) -> Dict[str, Any]:
        """Get comprehensive verification summary"""
        return {
            'total_verifications': len(self.verification_records),
            'successful_verifications': sum(
                1 for r in self.verification_records if r['overall_success']
            ),
            'success_rate': sum(
                1 for r in self.verification_records if r['overall_success']
            ) / max(len(self.verification_records), 1),
            'carbon_position': self.accountant.get_current_position().__dict__,
            'emissions_breakdown': self.accountant.get_emissions_breakdown(),
            'blockchain_summary': self.blockchain.get_retired_credits_summary() if self.blockchain else {},
            'satellite_summary': self.satellite.get_verification_summary() if self.satellite else {},
            'sensor_status': self.sensors.get_sensor_status() if self.sensors else {},
            'additionality_summary': self.additionality.get_additionality_summary() if self.additionality else {}
        }
    
    def verify_blockchain_integrity(self) -> bool:
        """Verify blockchain audit chain integrity"""
        if self.blockchain:
            return self.blockchain.verify_chain_integrity()
        return True
