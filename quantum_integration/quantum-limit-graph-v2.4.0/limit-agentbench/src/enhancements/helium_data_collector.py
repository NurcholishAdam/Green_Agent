# File: src/enhancements/helium_data_collector.py (ENHANCED VERSION v2.2)

"""
Helium Data Collector for Green Agent - Version 2.2

ENHANCED WITH:
- New production capacity tracking
- Future supply potential calculations
- Enhanced feature vector (11 dimensions)
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any, Union
from pathlib import Path
import csv
import datetime as dt
import numpy as np
import pandas as pd
import json
import logging
import hashlib
import time
import uuid
import threading
import asyncio
import aiohttp
import pickle
import copy
from collections import defaultdict, deque
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

# Production dependencies
from pydantic import BaseSettings, Field, validator
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import plotly.graph_objects as go
import plotly.express as px
from fastapi import FastAPI, WebSocket
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.FileHandler('helium_collector_v2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.correlation_id = str(uuid.uuid4())[:8]
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger("audit")
audit_handler = logging.FileHandler('helium_audit.log')
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
REGISTRY = CollectorRegistry()
COLLECTOR_LOADS = Counter('helium_collector_loads_total', 'Total data loads', ['source', 'status'], registry=REGISTRY)
DATA_FRESHNESS = Gauge('helium_data_freshness_seconds', 'Age of latest data point', registry=REGISTRY)
RECORD_COUNT = Gauge('helium_record_count', 'Number of records in dataset', registry=REGISTRY)
DATA_QUALITY_SCORE = Gauge('helium_data_quality_score', 'Data quality score (0-100)', registry=REGISTRY)
SCARCITY_INDEX_GAUGE = Gauge('helium_scarcity_index_gauge', 'Current helium scarcity index', registry=REGISTRY)
PRICE_INDEX_GAUGE = Gauge('helium_price_index_gauge', 'Current helium price index', registry=REGISTRY)
RECYCLING_RATE_GAUGE = Gauge('helium_recycling_rate_gauge', 'Current helium recycling rate', registry=REGISTRY)
INTEGRATION_STATUS = Gauge('helium_collector_integration_status', 'Integration status', ['module'], registry=REGISTRY)
CACHE_HITS = Counter('helium_collector_cache_hits_total', 'Cache hit count', ['cache_type'], registry=REGISTRY)
FEATURE_VECTOR_GAUGE = Gauge('helium_feature_vector', 'Feature vector values', ['dimension'], registry=REGISTRY)
API_CALLS = Counter('helium_api_calls_total', 'API calls', ['source', 'status'], registry=REGISTRY)
ANOMALY_COUNT = Gauge('helium_anomaly_count', 'Number of detected anomalies', registry=REGISTRY)
# NEW metrics
FUTURE_SUPPLY_POTENTIAL = Gauge('helium_future_supply_potential_pct', 'Future supply potential percentage', registry=REGISTRY)
NEW_CAPACITY_TRACKED = Gauge('helium_new_capacity_tracked_tonnes', 'New production capacity tracked', registry=REGISTRY)

# ============================================================
# CONFIGURATION MANAGEMENT
# ============================================================

class HeliumCollectorSettings(BaseSettings):
    """Configuration settings for helium collector with new capacity options"""
    csv_path: Path = Field(default=Path("./data/helium_timeseries.csv"))
    cache_ttl: int = Field(default=3600, description="Cache TTL in seconds")
    max_data_age_hours: float = Field(default=24, description="Maximum data age before warning")
    enable_synthetic_fallback: bool = Field(default=True)
    anomaly_detection_enabled: bool = Field(default=True)
    refresh_interval_hours: int = Field(default=24)
    enable_api_integration: bool = Field(default=True)
    api_timeout_seconds: int = Field(default=30)
    usgs_api_key: str = Field(default="", env="USGS_API_KEY")
    commodity_api_key: str = Field(default="", env="COMMODITY_API_KEY")
    supply_chain_api_key: str = Field(default="", env="SUPPLY_CHAIN_API_KEY")
    dashboard_port: int = Field(default=8501)
    websocket_port: int = Field(default=8765)
    # NEW: Capacity tracking
    enable_capacity_tracking: bool = Field(default=True)
    capacity_forecast_months: int = Field(default=12)
    
    class Config:
        env_prefix = "HELIUM_COLLECTOR_"
        case_sensitive = False

# ============================================================
# ENHANCED DATA MODELS
# ============================================================

@dataclass
class HeliumRecord:
    """Enhanced helium record with new production capacity"""
    date: dt.date
    global_production_tonnes: float
    global_demand_tonnes: float
    price_index: float
    shortage_severity_0_1: float
    supply_risk_score_0_1: float
    recycling_rate_0_1: float
    substitution_feasibility_0_1: float
    cooling_load_sensitivity: float
    geopolitical_risk_index: float = 0.5
    logistics_disruption_index: float = 0.3
    new_production_capacity_tonnes: float = 0.0  # NEW FIELD
    
    # NEW derived fields
    price_volatility: float = 0.0
    market_regime: str = "normal"
    anomaly_score: float = 0.0
    is_anomaly: bool = False
    
    @property
    def demand_supply_ratio(self) -> float:
        return self.global_demand_tonnes / max(self.global_production_tonnes, 1e-6)
    
    @property
    def scarcity_index(self) -> float:
        return min(1.0, (
            self.shortage_severity_0_1 * 0.4 +
            self.supply_risk_score_0_1 * 0.3 +
            (self.demand_supply_ratio - 1) * 0.3
        ))
    
    @property
    def circularity_potential(self) -> float:
        return (self.recycling_rate_0_1 + self.substitution_feasibility_0_1) / 2
    
    @property
    def thermal_impact_factor(self) -> float:
        return self.cooling_load_sensitivity * self.scarcity_index
    
    # NEW derived properties
    @property
    def future_supply_potential(self) -> float:
        """Calculate future supply potential based on new capacity"""
        # Ratio of new capacity to current production (as percentage)
        return (self.new_production_capacity_tonnes / max(self.global_production_tonnes, 1)) * 100
    
    @property
    def supply_demand_gap_projection(self) -> float:
        """Projected supply-demand gap considering new capacity"""
        projected_supply = self.global_production_tonnes + self.new_production_capacity_tonnes * 0.5
        return self.global_demand_tonnes - projected_supply
    
    @property
    def capacity_utilization_rate(self) -> float:
        """Calculate capacity utilization rate"""
        total_capacity = self.global_production_tonnes + self.new_production_capacity_tonnes
        return self.global_production_tonnes / max(total_capacity, 1)
    
    def to_dict(self) -> Dict:
        return {
            'date': self.date.isoformat(),
            'global_production_tonnes': self.global_production_tonnes,
            'global_demand_tonnes': self.global_demand_tonnes,
            'price_index': self.price_index,
            'shortage_severity_0_1': self.shortage_severity_0_1,
            'supply_risk_score_0_1': self.supply_risk_score_0_1,
            'recycling_rate_0_1': self.recycling_rate_0_1,
            'substitution_feasibility_0_1': self.substitution_feasibility_0_1,
            'cooling_load_sensitivity': self.cooling_load_sensitivity,
            'demand_supply_ratio': self.demand_supply_ratio,
            'scarcity_index': self.scarcity_index,
            'circularity_potential': self.circularity_potential,
            'thermal_impact_factor': self.thermal_impact_factor,
            'price_volatility': self.price_volatility,
            'market_regime': self.market_regime,
            'is_anomaly': self.is_anomaly,
            'new_production_capacity_tonnes': self.new_production_capacity_tonnes,
            'future_supply_potential_pct': self.future_supply_potential,
            'supply_demand_gap_projection': self.supply_demand_gap_projection,
            'capacity_utilization_rate': self.capacity_utilization_rate
        }
    
    def to_feature_vector(self) -> np.ndarray:
        """Enhanced feature vector (11 dimensions)"""
        return np.array([
            self.global_production_tonnes / 50000,  # Normalized production
            self.demand_supply_ratio,
            self.price_index / 500,
            self.shortage_severity_0_1,
            self.supply_risk_score_0_1,
            self.recycling_rate_0_1,
            self.substitution_feasibility_0_1,
            self.cooling_load_sensitivity,
            self.geopolitical_risk_index,
            self.logistics_disruption_index,
            self.new_production_capacity_tonnes / 20000  # NEW normalized capacity
        ])

@dataclass
class HeliumDataset:
    """Enhanced dataset with versioning and metadata"""
    records: List[HeliumRecord] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    version: str = field(default_factory=lambda: dt.datetime.now().strftime("%Y%m%d%H%M%S"))
    
    @property
    def latest(self) -> Optional[HeliumRecord]:
        return self.records[-1] if self.records else None
    
    @property
    def timeseries_length(self) -> int:
        return len(self.records)
    
    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([r.to_dict() for r in self.records])
    
    def to_feature_matrix(self) -> np.ndarray:
        return np.array([r.to_feature_vector() for r in self.records])
    
    def get_trends(self) -> Dict:
        if len(self.records) < 2:
            return {}
        first, last = self.records[0], self.records[-1]
        return {
            'production_change_pct': ((last.global_production_tonnes - first.global_production_tonnes) / max(first.global_production_tonnes, 1)) * 100,
            'demand_change_pct': ((last.global_demand_tonnes - first.global_demand_tonnes) / max(first.global_demand_tonnes, 1)) * 100,
            'price_change_pct': ((last.price_index - first.price_index) / max(first.price_index, 1)) * 100,
            'scarcity_trend': 'increasing' if last.scarcity_index > first.scarcity_index else 'decreasing',
            'circularity_improvement': last.circularity_potential - first.circularity_potential,
            'capacity_growth_pct': ((last.new_production_capacity_tonnes - first.new_production_capacity_tonnes) / max(first.new_production_capacity_tonnes, 1)) * 100 if first.new_production_capacity_tonnes > 0 else 0,
            'future_supply_potential': last.future_supply_potential
        }

# ============================================================
# ENHANCED SYNTHETIC DATA GENERATOR WITH CAPACITY
# ============================================================

class EnhancedSyntheticDataGenerator:
    """Generate synthetic helium data with realistic new capacity trends"""
    
    def __init__(self, seed: int = 42):
        self.rng = np.random.RandomState(seed)
    
    def generate(self, n_periods: int = 48, start_date: dt.date = None) -> List[HeliumRecord]:
        """Generate synthetic data with new production capacity"""
        if start_date is None:
            start_date = dt.date(2020, 1, 1)
        
        records = []
        
        # Geometric Brownian Motion parameters
        mu = 0.05  # drift
        sigma = 0.15  # volatility
        
        # Generate price path
        price_path = self._generate_price_path(n_periods, mu, sigma)
        
        # New capacity growth parameters
        base_capacity = 2000
        capacity_growth_rate = 0.025  # 2.5% per month
        
        for i in range(n_periods):
            date = start_date + dt.timedelta(days=30 * i)
            
            # Production (mean-reverting with slight decline)
            production = 28000 - i * 40 + self.rng.normal(0, 300)
            production = max(20000, min(35000, production))
            
            # Demand (increasing with growth)
            demand = 27000 + i * 80 + self.rng.normal(0, 400)
            demand = max(25000, min(45000, demand))
            
            # NEW: Production capacity (ramping up)
            new_capacity = base_capacity * (1 + capacity_growth_rate) ** i + self.rng.normal(0, 200)
            new_capacity = max(500, min(15000, new_capacity))
            
            # Effective supply with new capacity
            effective_supply = production + new_capacity * 0.3
            demand_supply_ratio = demand / max(effective_supply, 1)
            
            # Shortage severity
            shortage = min(1.0, max(0.05, (demand_supply_ratio - 0.95) * 4))
            
            # Supply risk (reduced by new capacity)
            supply_risk = max(0.1, min(0.9, 0.2 + i * 0.012 - (new_capacity / 15000) + self.rng.uniform(-0.05, 0.05)))
            
            # Recycling rate (increasing)
            recycling = min(0.35, 0.10 + i * 0.005 + self.rng.uniform(-0.01, 0.01))
            
            # Substitution feasibility (increasing)
            substitution = min(0.40, 0.08 + i * 0.007 + self.rng.uniform(-0.01, 0.01))
            
            # Cooling sensitivity (slightly increasing)
            cooling = 0.85 + i * 0.006 + self.rng.uniform(-0.02, 0.02)
            
            # Geopolitical risk (cyclic)
            geo_risk = 0.3 + 0.2 * np.sin(2 * np.pi * i / 24) + self.rng.uniform(-0.05, 0.05)
            
            # Logistics disruption (random)
            logistics = 0.2 + 0.15 * self.rng.random() + i * 0.002
            
            record = HeliumRecord(
                date=date,
                global_production_tonnes=production,
                global_demand_tonnes=demand,
                price_index=price_path[i],
                shortage_severity_0_1=np.clip(shortage, 0, 1),
                supply_risk_score_0_1=np.clip(supply_risk, 0, 1),
                recycling_rate_0_1=np.clip(recycling, 0, 1),
                substitution_feasibility_0_1=np.clip(substitution, 0, 1),
                cooling_load_sensitivity=cooling,
                geopolitical_risk_index=np.clip(geo_risk, 0, 1),
                logistics_disruption_index=np.clip(logistics, 0, 1),
                new_production_capacity_tonnes=new_capacity
            )
            records.append(record)
        
        return records
    
    def _generate_price_path(self, n_periods: int, mu: float, sigma: float) -> np.ndarray:
        """Generate realistic price path with seasonality"""
        dt_days = 1/12
        shocks = self.rng.normal(0, sigma * np.sqrt(dt_days), n_periods)
        
        price_path = [100]
        for shock in shocks:
            price_path.append(price_path[-1] * np.exp((mu - 0.5 * sigma**2) * dt_days + shock))
        price_path = np.array(price_path[1:])
        
        # Add seasonal component
        seasonality = 1 + 0.1 * np.sin(2 * np.pi * np.arange(n_periods) / 12)
        price_path = price_path * seasonality
        
        return price_path

# ============================================================
# MAIN HELIUM DATA COLLECTOR (ENHANCED)
# ============================================================

class HeliumDataCollector:
    """
    ENHANCED Helium Data Collector v2.2
    
    Complete helium data management with:
    - Real API integration (USGS, commodity, supply chain)
    - New production capacity tracking
    - Future supply potential calculations
    - Data refresh scheduling
    - Anomaly detection with Isolation Forest
    - Time series feature engineering
    - Configuration management
    - Data versioning
    - Export to multiple formats
    """
    
    BASE_DIR = Path(__file__).resolve().parent
    DEFAULT_DATA_PATH = BASE_DIR / "data" / "helium_timeseries.csv"
    
    def __init__(self, settings: HeliumCollectorSettings = None):
        self.settings = settings or HeliumCollectorSettings()
        self.csv_path = self.settings.csv_path or self.DEFAULT_DATA_PATH
        
        # Core components
        self.api_collector = None
        self.anomaly_detector = None
        self.feature_engineer = None
        self.regime_detector = None
        self.version_manager = None
        self.data_augmenter = None
        self._init_components()
        
        # Dataset
        self.dataset: Optional[HeliumDataset] = None
        
        # Cache management
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._cache_ttl = self.settings.cache_ttl
        self._lock = threading.RLock()
        
        # Data lineage
        self._lineage: List[Dict] = []
        
        # Load or generate data
        self._load_or_generate()
        
        # Train anomaly detector
        if self.settings.anomaly_detection_enabled and self.dataset:
            feature_matrix = self.dataset.to_feature_matrix()
            if len(feature_matrix) > 10:
                self._train_anomaly_detector(feature_matrix)
        
        # Update metrics
        self._update_all_metrics()
        self._record_lineage('initialize', {'source': 'csv' if self.csv_path.exists() else 'synthetic'})
        
        logger.info(f"HeliumDataCollector v2.2 initialized with {self.dataset.timeseries_length if self.dataset else 0} records")
    
    def _init_components(self):
        """Initialize all components with lazy imports"""
        # Real API connector
        if self.settings.enable_api_integration:
            from .helium_api_collector import RealAPICollector
            self.api_collector = RealAPICollector()
        
        # Anomaly detection
        if self.settings.anomaly_detection_enabled:
            self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
            self.anomaly_scaler = StandardScaler()
        
        # Feature engineering
        self.feature_engineer = TimeSeriesFeatureEngineer()
        
        # Market regime detector
        self.regime_detector = MarketRegimeDetector()
        
        # Data versioning
        self.version_manager = DataVersionManager()
        
        # Data augmenter
        self.data_augmenter = DataAugmenter()
    
    def _train_anomaly_detector(self, feature_matrix: np.ndarray):
        """Train anomaly detection model"""
        if len(feature_matrix) < 10:
            return
        features_scaled = self.anomaly_scaler.fit_transform(feature_matrix)
        self.anomaly_detector.fit(features_scaled)
        logger.info(f"Anomaly detector trained on {len(feature_matrix)} samples")
    
    def _load_or_generate(self):
        """Load CSV or generate synthetic data"""
        try:
            self.dataset = self._load_from_csv()
            COLLECTOR_LOADS.labels(source='csv', status='success').inc()
            logger.info(f"Loaded helium data from {self.csv_path}")
        except Exception as e:
            logger.warning(f"Could not load CSV: {e}")
            if self.settings.enable_synthetic_fallback:
                self.dataset = self._generate_enhanced_synthetic_data()
                COLLECTOR_LOADS.labels(source='synthetic', status='success').inc()
            else:
                raise
    
    def _load_from_csv(self) -> HeliumDataset:
        """Load and validate CSV data with new capacity column"""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Helium data file not found: {self.csv_path}")
        
        df = pd.read_csv(self.csv_path)
        
        # Parse dates
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Add time series features
        df = self.feature_engineer.add_features(df) if self.feature_engineer else df
        
        # Detect market regimes
        if self.regime_detector and 'price_volatility' in df.columns:
            df['market_regime'] = df.apply(lambda row: self.regime_detector.detect_regime(df), axis=1)
        
        # Create records
        records = []
        for _, row in df.iterrows():
            record = HeliumRecord(
                date=row['date'],
                global_production_tonnes=float(row['global_production_tonnes']),
                global_demand_tonnes=float(row['global_demand_tonnes']),
                price_index=float(row['price_index']),
                shortage_severity_0_1=self._validate_range(float(row['shortage_severity_0_1']), 0, 1),
                supply_risk_score_0_1=self._validate_range(float(row['supply_risk_score_0_1']), 0, 1),
                recycling_rate_0_1=self._validate_range(float(row['recycling_rate_0_1']), 0, 1),
                substitution_feasibility_0_1=self._validate_range(float(row['substitution_feasibility_0_1']), 0, 1),
                cooling_load_sensitivity=float(row['cooling_load_sensitivity']),
                geopolitical_risk_index=float(row.get('geopolitical_risk_index', 0.5)),
                logistics_disruption_index=float(row.get('logistics_disruption_index', 0.3)),
                new_production_capacity_tonnes=float(row.get('new_production_capacity_tonnes', 0.0)),
                price_volatility=float(row.get('price_volatility', 0)),
                market_regime=row.get('market_regime', 'normal')
            )
            records.append(record)
        
        records.sort(key=lambda r: r.date)
        
        return HeliumDataset(
            records=records,
            metadata={'source': 'CSV', 'file': str(self.csv_path), 'loaded_at': dt.datetime.now().isoformat()}
        )
    
    def _generate_enhanced_synthetic_data(self) -> HeliumDataset:
        """Generate enhanced synthetic data with new capacity"""
        generator = EnhancedSyntheticDataGenerator(seed=self.settings.seed if hasattr(self.settings, 'seed') else 42)
        records = generator.generate(n_periods=48)
        
        return HeliumDataset(
            records=records,
            metadata={
                'source': 'enhanced_synthetic',
                'generated_at': dt.datetime.now().isoformat(),
                'model': 'geometric_brownian_motion_with_seasonality_and_capacity'
            }
        )
    
    def _validate_range(self, value: float, min_val: float, max_val: float) -> float:
        """Validate and clip value to range"""
        if value < min_val or value > max_val:
            logger.debug(f"Value {value} outside range [{min_val}, {max_val}], clipping")
            return max(min_val, min(max_val, value))
        return value
    
    def _update_all_metrics(self):
        """Update all Prometheus metrics"""
        if not self.dataset:
            return
        
        RECORD_COUNT.set(self.dataset.timeseries_length)
        latest = self.get_latest()
        
        if latest:
            DATA_FRESHNESS.set((dt.date.today() - latest.date).days * 86400)
            SCARCITY_INDEX_GAUGE.set(latest.scarcity_index)
            PRICE_INDEX_GAUGE.set(latest.price_index)
            RECYCLING_RATE_GAUGE.set(latest.recycling_rate_0_1)
            FUTURE_SUPPLY_POTENTIAL.set(latest.future_supply_potential)
            NEW_CAPACITY_TRACKED.set(latest.new_production_capacity_tonnes)
            
            features = latest.to_feature_vector()
            for i, value in enumerate(features):
                FEATURE_VECTOR_GAUGE.labels(dimension=str(i)).set(value)
        
        DATA_QUALITY_SCORE.set(self._calculate_data_quality())
    
    def _calculate_data_quality(self) -> float:
        """Calculate data quality score including new capacity field"""
        if not self.dataset or not self.dataset.records:
            return 0.0
        
        records = self.dataset.records
        score = 100.0
        
        # Check for gaps in time series
        if len(records) > 1:
            for i in range(1, len(records)):
                day_diff = (records[i].date - records[i-1].date).days
                if day_diff > 35:
                    score -= 5
        
        # Check for unrealistic values
        for record in records:
            if record.global_production_tonnes < 20000 or record.global_production_tonnes > 40000:
                score -= 2
            if record.price_index < 50 or record.price_index > 500:
                score -= 2
            if record.recycling_rate_0_1 > 0.5:
                score -= 1
            if record.new_production_capacity_tonnes < 0 or record.new_production_capacity_tonnes > 20000:
                score -= 1
        
        return max(0, min(100, score))
    
    def _record_lineage(self, action: str, details: Dict):
        """Record data lineage for audit trail"""
        self._lineage.append({
            'action': action,
            'details': details,
            'timestamp': dt.datetime.now().isoformat(),
            'correlation_id': str(uuid.uuid4())[:8]
        })
    
    # ============================================================
    # CACHE MANAGEMENT
    # ============================================================
    
    def _get_cached(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if time.time() - timestamp < self._cache_ttl:
                    CACHE_HITS.labels(cache_type=key).inc()
                    return value
                del self._cache[key]
        return None
    
    def _set_cache(self, key: str, value: Any):
        """Set value in cache"""
        with self._lock:
            self._cache[key] = (value, time.time())
    
    # ============================================================
    # PUBLIC METHODS
    # ============================================================
    
    def get_latest(self) -> Optional[HeliumRecord]:
        """Get latest helium record"""
        cached = self._get_cached('latest')
        if cached is not None:
            return cached
        
        result = self.dataset.latest if self.dataset else None
        if result:
            self._set_cache('latest', result)
        return result
    
    def get_feature_vector(self) -> np.ndarray:
        """Get latest feature vector (11 dimensions) for ML models"""
        cached = self._get_cached('feature_vector')
        if cached is not None:
            return cached
        
        latest = self.get_latest()
        result = latest.to_feature_vector() if latest else np.zeros(11)
        self._set_cache('feature_vector', result)
        return result
    
    def get_timeseries_dataframe(self) -> pd.DataFrame:
        """Get complete timeseries as DataFrame"""
        return self.dataset.to_dataframe() if self.dataset else pd.DataFrame()
    
    def get_feature_matrix(self) -> np.ndarray:
        """Get feature matrix for ML training"""
        return self.dataset.to_feature_matrix() if self.dataset else np.array([])
    
    def get_trends(self) -> Dict:
        """Get helium market trends including capacity growth"""
        return self.dataset.get_trends() if self.dataset else {}
    
    def is_data_fresh(self, max_age_hours: float = None) -> bool:
        """Check if data is fresh"""
        max_age = max_age_hours or self.settings.max_data_age_hours
        latest = self.get_latest()
        if not latest:
            return False
        
        age = (dt.date.today() - latest.date).days * 24
        return age <= max_age
    
    # ============================================================
    # EXPORT FUNCTIONS FOR INTEGRATIONS
    # ============================================================
    
    def export_for_synthetic_manager(self) -> Dict:
        """Export data for synthetic data manager"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'helium_features': latest.to_dict(),
            'timeseries': self.get_timeseries_dataframe().to_dict('records'),
            'trends': self.get_trends(),
            'feature_matrix': self.get_feature_matrix().tolist(),
            'metadata': {
                'source': 'helium_data_collector_v2.2',
                'exported_at': dt.datetime.now().isoformat(),
                'record_count': self.dataset.timeseries_length if self.dataset else 0,
                'data_quality': self._calculate_data_quality(),
                'anomaly_detection_enabled': self.settings.anomaly_detection_enabled,
                'capacity_tracking_enabled': self.settings.enable_capacity_tracking
            }
        }
    
    def export_for_sustainability_signals(self) -> Dict:
        """Export data for sustainability signals with new capacity"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'helium_scarcity_signal': {
                'scarcity_index': latest.scarcity_index,
                'shortage_severity': latest.shortage_severity_0_1,
                'supply_risk': latest.supply_risk_score_0_1,
                'demand_supply_ratio': latest.demand_supply_ratio,
                'new_production_capacity_tonnes': latest.new_production_capacity_tonnes,
                'future_supply_potential_pct': latest.future_supply_potential,
                'capacity_utilization_rate': latest.capacity_utilization_rate
            },
            'helium_circularity_signal': {
                'recycling_rate': latest.recycling_rate_0_1,
                'substitution_feasibility': latest.substitution_feasibility_0_1,
                'circularity_potential': latest.circularity_potential
            },
            'helium_thermal_signal': {
                'cooling_load_sensitivity': latest.cooling_load_sensitivity,
                'thermal_impact_factor': latest.thermal_impact_factor,
                'price_index': latest.price_index
            },
            'anomaly_signal': {
                'is_anomaly': latest.is_anomaly,
                'anomaly_score': latest.anomaly_score,
                'market_regime': latest.market_regime
            },
            'capacity_signal': {
                'new_capacity_tonnes': latest.new_production_capacity_tonnes,
                'future_supply_potential_pct': latest.future_supply_potential,
                'supply_demand_gap': latest.supply_demand_gap_projection
            },
            'metadata': {
                'source': 'helium_data_collector_v2.2',
                'date': latest.date.isoformat(),
                'trends': self.get_trends()
            }
        }
    
    def export_for_regret_optimizer(self) -> Dict:
        """Export data for regret optimizer with capacity impact"""
        latest = self.get_latest()
        trends = self.get_trends()
        
        return {
            'helium_price_index': latest.price_index if latest else 100,
            'helium_scarcity_index': latest.scarcity_index if latest else 0.5,
            'helium_supply_risk': latest.supply_risk_score_0_1 if latest else 0.5,
            'helium_demand_supply_ratio': latest.demand_supply_ratio if latest else 1.0,
            'helium_recycling_rate': latest.recycling_rate_0_1 if latest else 0.15,
            'helium_trend': trends.get('scarcity_trend', 'stable'),
            'helium_volatility': latest.price_volatility / 100 if latest else 0,
            'market_regime': latest.market_regime if latest else 'normal',
            'capacity_impact': {
                'new_capacity_tonnes': latest.new_production_capacity_tonnes if latest else 0,
                'future_supply_potential_pct': latest.future_supply_potential if latest else 0,
                'capacity_growth_trend': trends.get('capacity_growth_pct', 0) if trends else 0
            },
            'metadata': {
                'source': 'helium_data_collector_v2.2',
                'exported_at': dt.datetime.now().isoformat(),
                'data_quality': self._calculate_data_quality()
            }
        }
    
    def export_for_thermal_optimizer(self) -> Dict:
        """Export data for thermal optimizer with capacity adjustment"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        # Capacity adjustment factor for cooling
        capacity_adjustment = 1 - (latest.new_production_capacity_tonnes / 20000) * 0.2
        
        return {
            'helium_thermal_impact': {
                'cooling_load_sensitivity': latest.cooling_load_sensitivity,
                'thermal_impact_factor': latest.thermal_impact_factor,
                'scarcity_index': latest.scarcity_index,
                'capacity_adjustment_factor': capacity_adjustment
            },
            'helium_cooling_adjustment': {
                'price_index': latest.price_index,
                'demand_supply_ratio': latest.demand_supply_ratio,
                'shortage_severity': latest.shortage_severity_0_1,
                'market_regime': latest.market_regime,
                'new_capacity_impact': latest.new_production_capacity_tonnes / 10000
            },
            'forecast_adjustment': {
                'volatility': latest.price_volatility,
                'anomaly_detected': latest.is_anomaly,
                'future_supply_adjustment': 1 - latest.future_supply_potential / 100
            },
            'metadata': {
                'source': 'helium_data_collector_v2.2',
                'exported_at': dt.datetime.now().isoformat()
            }
        }
    
    def export_for_blockchain(self) -> Dict:
        """Export data for blockchain verification with capacity proof"""
        latest = self.get_latest()
        if not latest:
            return {}
        
        return {
            'helium_provenance_data': {
                'production_tonnes': latest.global_production_tonnes,
                'demand_tonnes': latest.global_demand_tonnes,
                'price_index': latest.price_index,
                'scarcity_index': latest.scarcity_index,
                'recycling_rate': latest.recycling_rate_0_1,
                'new_capacity_tonnes': latest.new_production_capacity_tonnes,
                'future_supply_potential': latest.future_supply_potential,
                'date': latest.date.isoformat(),
                'market_regime': latest.market_regime
            },
            'verification_payload': {
                'data_hash': hashlib.sha256(
                    json.dumps(latest.to_dict(), sort_keys=True, default=str).encode()
                ).hexdigest(),
                'timestamp': dt.datetime.now().isoformat(),
                'version': self.dataset.version if self.dataset else 'unknown'
            },
            'metadata': {
                'source': 'helium_data_collector_v2.2',
                'exported_at': dt.datetime.now().isoformat(),
                'record_count': self.dataset.timeseries_length if self.dataset else 0,
                'capacity_verified': self.settings.enable_capacity_tracking
            }
        }
    
    def export_for_forecaster(self) -> Dict:
        """Export data for helium forecaster with capacity features"""
        return {
            'training_data': {
                'feature_matrix': self.get_feature_matrix().tolist(),
                'timeseries': self.get_timeseries_dataframe().to_dict('records'),
                'record_count': self.dataset.timeseries_length if self.dataset else 0,
                'feature_names': ['production_norm', 'demand_supply', 'price_norm', 'shortage',
                                 'supply_risk', 'recycling', 'substitution', 'cooling',
                                 'geopolitical', 'logistics', 'new_capacity_norm']
            },
            'latest_features': self.get_feature_vector().tolist(),
            'trends': self.get_trends(),
            'anomaly_info': {
                'detection_enabled': self.settings.anomaly_detection_enabled,
                'latest_anomaly': self.get_latest().is_anomaly if self.get_latest() else False
            },
            'capacity_info': {
                'current_new_capacity': self.get_latest().new_production_capacity_tonnes if self.get_latest() else 0,
                'capacity_trend': self.get_trends().get('capacity_growth_pct', 0) if self.get_trends() else 0,
                'forecast_horizon_months': self.settings.capacity_forecast_months
            },
            'metadata': {
                'source': 'helium_data_collector_v2.2',
                'exported_at': dt.datetime.now().isoformat(),
                'data_quality': self._calculate_data_quality(),
                'version': self.dataset.version if self.dataset else 'unknown'
            }
        }
    
    def health_check(self) -> Dict:
        """Health check for control system integration"""
        latest = self.get_latest()
        
        return {
            'healthy': self.dataset is not None and self.dataset.timeseries_length > 0,
            'status': 'operational' if self.dataset and self.dataset.timeseries_length > 0 else 'degraded',
            'data_loaded': self.dataset is not None,
            'record_count': self.dataset.timeseries_length if self.dataset else 0,
            'data_source': self.dataset.metadata.get('source', 'unknown') if self.dataset else 'none',
            'data_fresh': self.is_data_fresh(),
            'data_quality_score': self._calculate_data_quality(),
            'latest_date': latest.date.isoformat() if latest else None,
            'latest_scarcity': latest.scarcity_index if latest else 0,
            'latest_regime': latest.market_regime if latest else 'unknown',
            'latest_capacity_tonnes': latest.new_production_capacity_tonnes if latest else 0,
            'future_supply_potential': latest.future_supply_potential if latest else 0,
            'anomaly_detection_enabled': self.settings.anomaly_detection_enabled,
            'capacity_tracking_enabled': self.settings.enable_capacity_tracking,
            'api_integration_enabled': self.settings.enable_api_integration,
            'cache_size': len(self._cache),
            'lineage_entries': len(self._lineage),
            'versions_available': len(self.version_manager.list_versions()) if self.version_manager else 0,
            'timestamp': dt.datetime.now().isoformat()
        }
    
    def get_statistics(self) -> Dict:
        """Get comprehensive statistics"""
        latest = self.get_latest()
        trends = self.get_trends()
        versions = self.version_manager.list_versions() if self.version_manager else []
        
        return {
            'dataset': {
                'record_count': self.dataset.timeseries_length if self.dataset else 0,
                'data_source': self.dataset.metadata.get('source', 'unknown') if self.dataset else 'none',
                'version': self.dataset.version if self.dataset else None,
                'date_range': {
                    'first': self.dataset.records[0].date.isoformat() if self.dataset and self.dataset.records else None,
                    'last': self.dataset.records[-1].date.isoformat() if self.dataset and self.dataset.records else None
                } if self.dataset else {}
            },
            'latest_metrics': latest.to_dict() if latest else {},
            'trends': trends,
            'capacity_metrics': {
                'current_new_capacity_tonnes': latest.new_production_capacity_tonnes if latest else 0,
                'future_supply_potential_pct': latest.future_supply_potential if latest else 0,
                'capacity_growth_trend_pct': trends.get('capacity_growth_pct', 0) if trends else 0,
                'capacity_utilization_rate': latest.capacity_utilization_rate if latest else 0
            },
            'quality': {
                'score': self._calculate_data_quality(),
                'data_fresh': self.is_data_fresh(),
                'csv_available': self.csv_path.exists()
            },
            'anomaly_detection': {
                'model_trained': self.anomaly_detector is not None,
                'anomalies_detected': ANOMALY_COUNT._value.get() if hasattr(ANOMALY_COUNT, '_value') else 0
            },
            'version_management': {
                'versions_available': len(versions),
                'latest_version': versions[-1] if versions else None,
                'versions': versions
            },
            'cache': {
                'size': len(self._cache),
                'ttl_seconds': self._cache_ttl
            },
            'lineage': {
                'entries': len(self._lineage),
                'last_action': self._lineage[-1]['action'] if self._lineage else None
            },
            'api_integration': {
                'enabled': self.settings.enable_api_integration
            },
            'feature_vector_dimensions': len(self.get_feature_vector()),
            'export_functions': 6,
            'timestamp': dt.datetime.now().isoformat()
        }
    
    def get_active_integrations(self) -> List[str]:
        """Get list of active integration capabilities"""
        integrations = [
            'synthetic_manager',
            'sustainability_signals',
            'regret_optimizer',
            'thermal_optimizer',
            'blockchain',
            'forecaster'
        ]
        
        if self.settings.enable_api_integration:
            integrations.append('real_api')
        
        if self.settings.anomaly_detection_enabled:
            integrations.append('anomaly_detection')
        
        if self.settings.enable_capacity_tracking:
            integrations.append('capacity_tracking')
        
        return integrations


# ============================================================
# HELPER CLASSES (SIMPLIFIED)
# ============================================================

class TimeSeriesFeatureEngineer:
    """Add time series features to the dataset"""
    
    @staticmethod
    def add_features(df: pd.DataFrame) -> pd.DataFrame:
        """Add rolling windows, lag features, and volatility indicators"""
        if len(df) < 6:
            return df
        
        df_copy = df.copy()
        
        # Rolling averages
        df_copy['price_ma_3'] = df_copy['price_index'].rolling(3).mean()
        df_copy['price_ma_6'] = df_copy['price_index'].rolling(6).mean()
        df_copy['price_ma_12'] = df_copy['price_index'].rolling(12).mean()
        
        # Year-over-year changes
        if len(df_copy) >= 13:
            df_copy['price_yoy_pct'] = df_copy['price_index'].pct_change(12) * 100
        
        # Volatility (rolling std)
        df_copy['price_volatility'] = df_copy['price_index'].rolling(6).std()
        
        # Momentum
        df_copy['price_momentum_1m'] = df_copy['price_index'] - df_copy['price_index'].shift(1)
        df_copy['price_momentum_3m'] = df_copy['price_index'] - df_copy['price_index'].shift(3)
        
        return df_copy

class MarketRegimeDetector:
    """Detect market regimes"""
    
    def __init__(self):
        self.regimes = ['normal', 'high_volatility', 'crisis', 'recovery']
        self.current_regime = 'normal'
    
    def detect_regime(self, df: pd.DataFrame) -> str:
        """Detect current market regime"""
        if len(df) < 30:
            return 'normal'
        
        volatility = df['price_volatility'].iloc[-1] if 'price_volatility' in df.columns else 5
        price_change = df['price_index'].pct_change(12).iloc[-1] if len(df) > 12 else 0
        
        if volatility > 15 and price_change < -0.1:
            regime = 'crisis'
        elif volatility > 10:
            regime = 'high_volatility'
        elif price_change > 0.05:
            regime = 'recovery'
        else:
            regime = 'normal'
        
        self.current_regime = regime
        return regime

class DataVersionManager:
    """Manage versioned datasets"""
    
    def __init__(self, storage_dir: Path = Path("./data/versions")):
        self.storage_dir = storage_dir
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def save_version(self, dataset: HeliumDataset, tag: str = None) -> str:
        """Save versioned dataset"""
        version = tag or dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.storage_dir / f"helium_data_{version}.pkl"
        
        with open(path, 'wb') as f:
            pickle.dump(dataset, f)
        
        audit_logger.info(f"Saved dataset version: {version}")
        return version
    
    def load_version(self, version: str) -> Optional[HeliumDataset]:
        """Load specific version"""
        path = self.storage_dir / f"helium_data_{version}.pkl"
        if path.exists():
            with open(path, 'rb') as f:
                return pickle.load(f)
        return None
    
    def list_versions(self) -> List[str]:
        """List all available versions"""
        versions = []
        for path in self.storage_dir.glob("helium_data_*.pkl"):
            version = path.stem.replace("helium_data_", "")
            versions.append(version)
        return sorted(versions)

class DataAugmenter:
    """Generate augmented versions of existing data"""
    
    def __init__(self, noise_std: float = 0.02):
        self.noise_std = noise_std
    
    def augment_dataset(self, dataset: HeliumDataset, factor: int = 2) -> HeliumDataset:
        """Generate augmented dataset"""
        if not dataset.records:
            return dataset
        
        augmented_records = []
        
        for record in dataset.records:
            augmented_records.append(record)
            for _ in range(factor - 1):
                augmented = self._augment_record(record)
                augmented_records.append(augmented)
        
        return HeliumDataset(
            records=sorted(augmented_records, key=lambda r: r.date),
            metadata={
                'original_size': len(dataset.records),
                'augmented_size': len(augmented_records),
                'augmentation_factor': factor,
                'augmented_at': dt.datetime.now().isoformat()
            },
            version=dataset.version
        )
    
    def _augment_record(self, record: HeliumRecord) -> HeliumRecord:
        """Add realistic noise to a record"""
        augmented = copy.deepcopy(record)
        
        augmented.global_production_tonnes *= (1 + np.random.normal(0, self.noise_std))
        augmented.global_demand_tonnes *= (1 + np.random.normal(0, self.noise_std))
        augmented.price_index *= (1 + np.random.normal(0, self.noise_std))
        augmented.new_production_capacity_tonnes *= (1 + np.random.normal(0, self.noise_std * 1.5))
        
        augmented.global_production_tonnes = max(0, augmented.global_production_tonnes)
        augmented.global_demand_tonnes = max(0, augmented.global_demand_tonnes)
        augmented.price_index = max(50, augmented.price_index)
        augmented.new_production_capacity_tonnes = max(0, augmented.new_production_capacity_tonnes)
        
        date_offset = np.random.randint(-5, 6)
        augmented.date = record.date + dt.timedelta(days=date_offset)
        
        return augmented


# ============================================================
# SINGLETON INSTANCE
# ============================================================

_collector_instance = None

def get_helium_collector(settings: HeliumCollectorSettings = None) -> HeliumDataCollector:
    """Get or create the singleton HeliumDataCollector instance"""
    global _collector_instance
    if _collector_instance is None:
        _collector_instance = HeliumDataCollector(settings)
    return _collector_instance

async def quick_collect() -> HeliumRecord:
    """Quick data collection"""
    collector = get_helium_collector()
    return collector.get_latest()

# ============================================================
# ENHANCED MAIN DEMO
# ============================================================

async def main_v2_enhanced():
    """Enhanced v2.2 demonstration with new capacity features"""
    print("=" * 80)
    print("Helium Data Collector v2.2 - Enhanced with Production Capacity Tracking")
    print("=" * 80)
    
    settings = HeliumCollectorSettings(
        enable_api_integration=False,
        anomaly_detection_enabled=True,
        enable_synthetic_fallback=True,
        enable_capacity_tracking=True,
        capacity_forecast_months=12
    )
    
    collector = get_helium_collector(settings)
    
    print(f"\n✅ v2.2 Enhancements Active:")
    print(f"   Capacity Tracking: {'✅' if collector.settings.enable_capacity_tracking else '❌'}")
    print(f"   Feature Vector: 11 dimensions (was 10)")
    print(f"   Future Supply Potential: Calculated")
    print(f"   Supply-Demand Gap Projection: Available")
    
    latest = collector.get_latest()
    if latest:
        print(f"\n📊 Latest Helium Data ({latest.date}):")
        print(f"   Production: {latest.global_production_tonnes:,.0f} tonnes")
        print(f"   Demand: {latest.global_demand_tonnes:,.0f} tonnes")
        print(f"   Price Index: {latest.price_index:.0f}")
        print(f"   Scarcity Index: {latest.scarcity_index:.3f}")
        print(f"   New Production Capacity: {latest.new_production_capacity_tonnes:,.0f} tonnes")
        print(f"   Future Supply Potential: {latest.future_supply_potential:.1f}%")
        print(f"   Supply-Demand Gap Projection: {latest.supply_demand_gap_projection:,.0f} tonnes")
        print(f"   Recycling Rate: {latest.recycling_rate_0_1:.2%}")
        print(f"   Market Regime: {latest.market_regime}")
    
    trends = collector.get_trends()
    if trends:
        print(f"\n📈 Market Trends:")
        for key, value in trends.items():
            if isinstance(value, float):
                print(f"   {key}: {value:.2f}")
            else:
                print(f"   {key}: {value}")
    
    features = collector.get_feature_vector()
    print(f"\n🧬 Feature Vector (11 dimensions):")
    names = ['production', 'demand_supply', 'price', 'shortage', 'supply_risk', 
             'recycling', 'substitution', 'cooling', 'geopolitical', 'logistics', 'new_capacity']
    for name, value in zip(names, features):
        print(f"   {name}: {value:.4f}")
    
    print(f"\n🔗 Integration Exports (6 modules + capacity tracking):")
    regret_export = collector.export_for_regret_optimizer()
    print(f"   Regret Optimizer: {len(regret_export)} fields (capacity_impact included)")
    
    thermal_export = collector.export_for_thermal_optimizer()
    print(f"   Thermal Optimizer: {len(thermal_export)} fields (capacity_adjustment_factor included)")
    
    # Health check
    health = collector.health_check()
    print(f"\n🏥 Health Check:")
    print(f"   Status: {health['status']}")
    print(f"   Record Count: {health['record_count']}")
    print(f"   Data Quality: {health['data_quality_score']:.0f}%")
    print(f"   Latest Capacity: {health['latest_capacity_tonnes']:,.0f} tonnes")
    print(f"   Future Supply Potential: {health['future_supply_potential']:.1f}%")
    
    print("\n" + "=" * 80)
    print("✅ Helium Data Collector v2.2 - Demo Complete")
    print("=" * 80)
    
    return collector

if __name__ == "__main__":
    asyncio.run(main_v2_enhanced())
