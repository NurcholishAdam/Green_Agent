# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/photosynthetic_harvester.py
# Complete enhanced file v6.1.0 with:
# - Genetic Algorithm to evolve conversion costs, sensitivity thresholds, and repair rates
# - Competition among child harvesters (predator‑prey dynamics)
# - Swarm coordination: child harvesters share predictions and coordinate harvesting windows

"""
Enhanced Photosynthetic Harvester v6.1.0
Complete implementation with:
- Demand-responsive harvesting
- Photoinhibition protection and repair
- Predictive harvesting windows
- Circadian rhythm integration
- Multi-harvester scaling support
- Direct gradient field coupling
- ATP synthase feedback
- Advanced state persistence & recovery
- Machine learning predictions (LSTM-based)
- Vectorized processing for performance
- Comprehensive health monitoring & self-healing
- WebSocket streaming for real-time monitoring
- Enhanced circadian model with seasonal/geographic components
- Genetic Algorithm for parameter evolution
- Competition among child harvesters
- Swarm coordination for prediction sharing
"""

import asyncio
import logging
import json
import pickle
import hashlib
from typing import Dict, Any, List, Optional, Tuple, Union, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
import numpy as np
from collections import deque
import math
import random
import time
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing dependencies with enhanced error handling
# ============================================================================
try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    logger.warning("TensorFlow not available - using fallback prediction models")

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis not available - using in-memory persistence")

try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPSource
    TOKEN_MANAGER_AVAILABLE = True
except ImportError:
    TOKEN_MANAGER_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Enhanced Enums and Data Classes
# ============================================================================

class PigmentState(Enum):
    """Pigment operational states with enhanced tracking"""
    ACTIVE = "active"
    PHOTOINHIBITED = "photoinhibited"
    REPAIRING = "repairing"
    QUIESCENT = "quiescent"
    DAMAGED = "damaged"
    OVERLOADED = "overloaded"
    CALIBRATING = "calibrating"

class HarvestingMode(Enum):
    """Harvesting operational modes with additional states"""
    FULL = "full"
    MODULATED = "modulated"
    CONSERVATIVE = "conservative"
    MINIMAL = "minimal"
    DORMANT = "dormant"
    ADAPTIVE = "adaptive"
    SURVIVAL = "survival"

@dataclass
class PigmentHealth:
    """Enhanced health tracking for individual pigments"""
    pigment_name: str
    state: PigmentState = PigmentState.ACTIVE
    efficiency: float = 1.0
    damage_accumulation: float = 0.0
    repair_progress: float = 0.0
    total_excitations: int = 0
    total_conversions: int = 0
    last_repair: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    protection_level: float = 0.0
    recovery_rate: float = 0.01
    stress_history: deque = field(default_factory=lambda: deque(maxlen=100))
    peak_performance: float = 1.0
    degradation_trend: float = 0.0

@dataclass
class CircadianProfile:
    """Enhanced circadian rhythm profile with seasonal/geographic components"""
    hour_efficiency: Dict[int, float] = field(default_factory=lambda: {
        0: 0.1, 1: 0.05, 2: 0.05, 3: 0.05, 4: 0.1, 5: 0.2,
        6: 0.4, 7: 0.6, 8: 0.8, 9: 0.9, 10: 0.95, 11: 1.0,
        12: 1.0, 13: 0.95, 14: 0.9, 15: 0.85, 16: 0.75,
        17: 0.6, 18: 0.4, 19: 0.25, 20: 0.15, 21: 0.1,
        22: 0.1, 23: 0.1
    })
    learned_patterns: Dict[str, Dict[int, float]] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    latitude: float = 0.0
    longitude: float = 0.0
    seasonal_shift: float = 0.0
    daylight_savings: bool = False
    solar_noon_adjustment: float = 0.0

@dataclass
class ExcitationRecord:
    """Enhanced record of excitation events for predictive modeling"""
    timestamp: datetime
    pigment_name: str
    excitation_level: float
    converted_energy: float
    environmental_context: Dict[str, float]
    prediction_accuracy: Optional[float] = None
    anomaly_flag: bool = False

@dataclass
class HarvesterState:
    """Complete harvester state for persistence"""
    harvester_id: str
    timestamp: datetime
    mode: str
    total_harvested: float
    peak_harvest_rate: float
    harvest_cycles: int
    circadian_patterns: Dict[str, Dict[int, float]]
    prediction_models: Dict[str, Dict[str, Any]]
    pigment_health: Dict[str, Dict[str, Any]]
    reaction_center_state: Dict[str, Any]
    child_harvester_states: Dict[str, Dict[str, Any]]
    version: str = "6.0.0"

# ============================================================================
# State Persistence & Recovery Module
# ============================================================================

class PersistentHarvesterState:
    """
    Advanced state persistence with support for Redis, file, and memory backends.
    Implements checkpointing, versioning, and rollback capabilities.
    """
    
    def __init__(self, harvester_id: str, storage_backend: str = "memory"):
        self.harvester_id = harvester_id
        self.storage_backend = storage_backend
        self.redis_client = None
        self.state_cache = {}
        self.last_checkpoint = None
        self.checkpoint_interval = 300  # 5 minutes
        self.retention_days = 30
        
        # Setup backend
        if storage_backend == "redis" and REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(
                    host='localhost', 
                    port=6379, 
                    db=0,
                    decode_responses=True
                )
                logger.info("Redis persistence backend initialized")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.storage_backend = "memory"
        elif storage_backend == "file":
            self.state_dir = f"./harvester_states/{harvester_id}/"
            import os
            os.makedirs(self.state_dir, exist_ok=True)
            logger.info(f"File persistence backend initialized: {self.state_dir}")
        else:
            logger.info("Memory persistence backend initialized")
    
    async def checkpoint(self, state: Dict[str, Any]) -> bool:
        """
        Save current state with timestamp and versioning.
        
        Args:
            state: Complete harvester state dictionary
            
        Returns:
            bool: Success status
        """
        try:
            # Add metadata
            checkpoint_data = {
                'harvester_id': self.harvester_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'version': '6.0.0',
                'checksum': self._calculate_checksum(state),
                'state': state
            }
            
            checkpoint_id = f"checkpoint_{int(time.time())}"
            
            if self.storage_backend == "redis" and self.redis_client:
                key = f"harvester:{self.harvester_id}:checkpoint:{checkpoint_id}"
                await self.redis_client.setex(
                    key, 
                    self.retention_days * 86400,  # TTL in seconds
                    json.dumps(checkpoint_data, default=str)
                )
                # Update latest pointer
                latest_key = f"harvester:{self.harvester_id}:latest"
                await self.redis_client.set(latest_key, checkpoint_id)
                
            elif self.storage_backend == "file":
                import os
                filename = f"{self.state_dir}{checkpoint_id}.json"
                with open(filename, 'w') as f:
                    json.dump(checkpoint_data, f, default=str, indent=2)
                # Update latest pointer
                with open(f"{self.state_dir}latest.txt", 'w') as f:
                    f.write(checkpoint_id)
            
            else:  # memory
                self.state_cache[checkpoint_id] = checkpoint_data
                self.last_checkpoint = checkpoint_id
            
            self.last_checkpoint = checkpoint_id
            return True
            
        except Exception as e:
            logger.error(f"Checkpoint save failed: {e}")
            return False
    
    async def restore_latest(self) -> Optional[Dict[str, Any]]:
        """
        Restore the latest checkpoint.
        
        Returns:
            Dict or None: Restored state or None if not found
        """
        try:
            if self.storage_backend == "redis" and self.redis_client:
                latest_key = f"harvester:{self.harvester_id}:latest"
                checkpoint_id = await self.redis_client.get(latest_key)
                if not checkpoint_id:
                    return None
                    
                key = f"harvester:{self.harvester_id}:checkpoint:{checkpoint_id}"
                data = await self.redis_client.get(key)
                if data:
                    checkpoint_data = json.loads(data)
                    return checkpoint_data['state']
                    
            elif self.storage_backend == "file":
                import os
                latest_file = f"{self.state_dir}latest.txt"
                if not os.path.exists(latest_file):
                    return None
                    
                with open(latest_file, 'r') as f:
                    checkpoint_id = f.read().strip()
                    
                filename = f"{self.state_dir}{checkpoint_id}.json"
                if not os.path.exists(filename):
                    return None
                    
                with open(filename, 'r') as f:
                    checkpoint_data = json.load(f)
                return checkpoint_data['state']
            
            else:  # memory
                if self.last_checkpoint and self.last_checkpoint in self.state_cache:
                    return self.state_cache[self.last_checkpoint]['state']
            
            return None
            
        except Exception as e:
            logger.error(f"State restoration failed: {e}")
            return None
    
    async def list_checkpoints(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List available checkpoints with metadata.
        
        Returns:
            List of checkpoint metadata
        """
        checkpoints = []
        
        if self.storage_backend == "redis" and self.redis_client:
            pattern = f"harvester:{self.harvester_id}:checkpoint:*"
            keys = await self.redis_client.keys(pattern)
            keys = keys[-limit:] if len(keys) > limit else keys
            
            for key in keys:
                data = await self.redis_client.get(key)
                if data:
                    checkpoint = json.loads(data)
                    checkpoints.append({
                        'id': key.split(':')[-1],
                        'timestamp': checkpoint['timestamp'],
                        'version': checkpoint['version']
                    })
                    
        elif self.storage_backend == "file":
            import os, glob
            pattern = f"{self.state_dir}checkpoint_*.json"
            files = glob.glob(pattern)
            files = sorted(files, reverse=True)[:limit]
            
            for file in files:
                with open(file, 'r') as f:
                    checkpoint = json.load(f)
                    checkpoints.append({
                        'id': file.split('/')[-1].replace('.json', ''),
                        'timestamp': checkpoint['timestamp'],
                        'version': checkpoint['version']
                    })
        
        else:  # memory
            keys = list(self.state_cache.keys())[-limit:]
            for key in keys:
                checkpoint = self.state_cache[key]
                checkpoints.append({
                    'id': key,
                    'timestamp': checkpoint['timestamp'],
                    'version': checkpoint['version']
                })
        
        return checkpoints
    
    def _calculate_checksum(self, data: Dict) -> str:
        """Calculate MD5 checksum for state validation"""
        import hashlib
        state_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(state_str.encode()).hexdigest()

# ============================================================================
# Enhanced Circadian Model
# ============================================================================

class AdvancedCircadianModel:
    """
    Seasonal and location-aware circadian rhythm model.
    Calculates solar position and adjusts efficiency based on geographic location.
    """
    
    def __init__(self, latitude: float = 0.0, longitude: float = 0.0, timezone_offset: float = 0.0):
        self.latitude = latitude
        self.longitude = longitude
        self.timezone_offset = timezone_offset
        
        # Base efficiency profile
        self.base_profile = CircadianProfile()
        self.base_profile.latitude = latitude
        self.base_profile.longitude = longitude
        
        # Initialize seasonal factors
        self.seasonal_factors = self._calculate_seasonal_factors()
        
        # Cache for performance
        self._efficiency_cache = {}
        self._last_cache_clear = datetime.now(timezone.utc)
    
    def _calculate_seasonal_factors(self) -> Dict[str, float]:
        """
        Calculate seasonal modulation factors based on latitude.
        
        Returns:
            Dict with seasonal adjustment factors
        """
        # Latitude-based seasonal variation
        lat_factor = abs(self.latitude) / 90.0
        
        return {
            'summer_amplification': 1.0 + (lat_factor * 0.3),
            'winter_attenuation': 1.0 - (lat_factor * 0.4),
            'spring_acceleration': 0.15,
            'autumn_deceleration': -0.15,
            'equatorial_stability': 1.0 - lat_factor * 0.2
        }
    
    def get_efficiency_at_time(self, dt: datetime) -> float:
        """
        Calculate circadian efficiency at a specific datetime.
        
        Args:
            dt: Datetime for efficiency calculation
            
        Returns:
            float: Efficiency factor (0.0 - 1.0)
        """
        # Convert to local time
        local_hour = (dt.hour + self.timezone_offset) % 24
        
        # Get base efficiency from profile
        base_efficiency = self.base_profile.hour_efficiency.get(int(local_hour), 0.5)
        
        # Apply seasonal modulation
        day_of_year = dt.timetuple().tm_yday
        seasonal_mod = self._calculate_seasonal_modulation(day_of_year, local_hour)
        
        # Apply solar position adjustment
        solar_angle = self._calculate_solar_elevation(dt)
        solar_mod = self._calculate_solar_modulation(solar_angle)
        
        # Apply daylight savings adjustment
        dst_mod = 1.05 if self.base_profile.daylight_savings else 1.0
        
        # Combine factors
        final_efficiency = base_efficiency * seasonal_mod * solar_mod * dst_mod
        
        # Clamp to valid range
        return max(0.05, min(1.0, final_efficiency))
    
    def _calculate_seasonal_modulation(self, day_of_year: int, hour: int) -> float:
        """Calculate seasonal modulation factor"""
        # Sine wave for seasonal variation
        seasonal_position = (day_of_year - 80) / 365.25 * 2 * math.pi
        seasonal_factor = 1.0 + 0.2 * math.sin(seasonal_position)
        
        # Day length factor (shorter/longer days)
        day_length = self._calculate_day_length(day_of_year)
        if 6 <= hour <= 18:  # Daytime
            modulation = seasonal_factor * (day_length / 12.0)
        else:  # Nighttime
            modulation = seasonal_factor * ((24 - day_length) / 12.0)
        
        # Apply latitude-based extremes
        if abs(self.latitude) > 60:
            # High latitude extreme seasons
            if 6 <= hour <= 18:
                modulation *= 1.3 if seasonal_factor > 1 else 0.7
        
        return max(0.1, min(1.5, modulation))
    
    def _calculate_day_length(self, day_of_year: int) -> float:
        """Calculate day length in hours based on latitude"""
        if abs(self.latitude) < 1:
            return 12.0  # Equator
        
        # Simplified day length calculation
        declination = -23.44 * math.cos((day_of_year + 10) / 365.25 * 2 * math.pi)
        lat_rad = math.radians(self.latitude)
        dec_rad = math.radians(declination)
        
        cos_hour_angle = -math.tan(lat_rad) * math.tan(dec_rad)
        cos_hour_angle = max(-1, min(1, cos_hour_angle))
        
        hour_angle = math.acos(cos_hour_angle)
        day_length = 2 * math.degrees(hour_angle) / 15.0
        
        return max(0, min(24, day_length))
    
    def _calculate_solar_elevation(self, dt: datetime) -> float:
        """Calculate solar elevation angle in degrees"""
        # Simplified solar elevation calculation
        day_of_year = dt.timetuple().tm_yday
        hour_angle = (dt.hour - 12) * 15 + self.longitude
        
        declination = -23.44 * math.cos((day_of_year + 10) / 365.25 * 2 * math.pi)
        
        lat_rad = math.radians(self.latitude)
        dec_rad = math.radians(declination)
        hour_rad = math.radians(hour_angle)
        
        sin_alt = (math.sin(lat_rad) * math.sin(dec_rad) + 
                   math.cos(lat_rad) * math.cos(dec_rad) * math.cos(hour_rad))
        
        return math.degrees(math.asin(max(-1, min(1, sin_alt))))
    
    def _calculate_solar_modulation(self, solar_elevation: float) -> float:
        """Calculate modulation factor based on solar elevation"""
        if solar_elevation <= -18:  # Astronomical twilight
            return 0.05
        elif solar_elevation <= 0:  # Civil twilight
            return 0.2 + (solar_elevation + 18) / 18 * 0.3
        elif solar_elevation <= 10:  # Low angle
            return 0.5 + solar_elevation / 10 * 0.4
        else:  # High angle
            return min(1.0, 0.9 + (solar_elevation - 10) / 80 * 0.1)
    
    def update_from_harvester_learning(self, patterns: Dict[str, Dict[int, float]]):
        """Update circadian model with learned patterns from harvester"""
        for pigment, pattern in patterns.items():
            if pigment not in self.base_profile.learned_patterns:
                self.base_profile.learned_patterns[pigment] = {}
            
            for hour, value in pattern.items():
                old_value = self.base_profile.learned_patterns[pigment].get(hour, value)
                self.base_profile.learned_patterns[pigment][hour] = old_value * 0.7 + value * 0.3
        
        self.base_profile.last_updated = datetime.now(timezone.utc)

# ============================================================================
# Enhanced Pigment Array with Vectorized Processing
# ============================================================================

class EnhancedPigmentArray:
    """
    Enhanced multi-spectral sensor array with adaptive sensitivity,
    photoinhibition protection, health tracking, and vectorized processing.
    """
    
    def __init__(self, latitude: float = 0.0, longitude: float = 0.0):
        # Pigment definitions with enhanced properties
        self.pigments = {
            'chlorophyll_a': {
                'target': 'renewable_availability',
                'sensitivity': 1.0,
                'base_sensitivity': 1.0,
                'response_time_ms': 100,
                'saturation_threshold': 0.9,
                'noise_floor': 0.05,
                'photoinhibition_rate': 0.001,
                'safe_excitation_level': 0.7,
                'repair_rate': 0.01,
                'circadian_peak_hours': [10, 11, 12, 13, 14],
                'specialization': 'solar',
                'energy_conversion_factor': 0.01,
                'critical_threshold': 0.85
            },
            'chlorophyll_b': {
                'target': 'carbon_intensity',
                'sensitivity': 0.8,
                'base_sensitivity': 0.8,
                'response_time_ms': 200,
                'saturation_threshold': 0.7,
                'noise_floor': 0.03,
                'photoinhibition_rate': 0.0005,
                'safe_excitation_level': 0.8,
                'repair_rate': 0.015,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'carbon',
                'energy_conversion_factor': 0.001,
                'critical_threshold': 0.75
            },
            'carotenoids': {
                'target': 'waste_heat',
                'sensitivity': 0.6,
                'base_sensitivity': 0.6,
                'response_time_ms': 500,
                'saturation_threshold': 0.8,
                'noise_floor': 0.1,
                'photoinhibition_rate': 0.0002,
                'safe_excitation_level': 0.9,
                'repair_rate': 0.02,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'thermal',
                'energy_conversion_factor': 0.01,
                'critical_threshold': 0.9
            },
            'phycobilins': {
                'target': 'edge_availability',
                'sensitivity': 0.7,
                'base_sensitivity': 0.7,
                'response_time_ms': 300,
                'saturation_threshold': 0.6,
                'noise_floor': 0.08,
                'photoinhibition_rate': 0.0003,
                'safe_excitation_level': 0.85,
                'repair_rate': 0.012,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'edge',
                'energy_conversion_factor': 0.005,
                'critical_threshold': 0.8
            },
            'xanthophylls': {
                'target': 'system_overload',
                'sensitivity': 0.9,
                'base_sensitivity': 0.9,
                'response_time_ms': 50,
                'saturation_threshold': 1.0,
                'noise_floor': 0.01,
                'photoinhibition_rate': 0.0001,
                'safe_excitation_level': 0.95,
                'repair_rate': 0.025,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'protection',
                'energy_conversion_factor': 0.02,
                'critical_threshold': 0.95
            }
        }
        
        # Vectorized storage for performance
        self._pigment_names = list(self.pigments.keys())
        self._targets = np.array([self.pigments[p]['target'] for p in self._pigment_names])
        self._sensitivities = np.array([self.pigments[p]['sensitivity'] for p in self._pigment_names])
        self._safe_levels = np.array([self.pigments[p]['safe_excitation_level'] for p in self._pigment_names])
        self._saturation_thresholds = np.array([self.pigments[p]['saturation_threshold'] for p in self._pigment_names])
        self._noise_floors = np.array([self.pigments[p]['noise_floor'] for p in self._pigment_names])
        
        # Pigment health tracking
        self.pigment_health: Dict[str, PigmentHealth] = {
            name: PigmentHealth(pigment_name=name)
            for name in self._pigment_names
        }
        
        # Excitation history for predictive modeling
        self.excitation_history: Dict[str, deque] = {
            name: deque(maxlen=500) for name in self._pigment_names
        }
        
        # Enhanced circadian model
        self.circadian_model = AdvancedCircadianModel(latitude, longitude)
        
        # Predictive models
        self.prediction_models: Dict[str, Dict[str, Any]] = {}
        self.lstm_predictors = {} if TENSORFLOW_AVAILABLE else {}
        
        # Anomaly detection
        self.anomaly_detector = EnvironmentalAnomalyDetector()
        
        # Start repair and adaptation loops
        self._repair_task = asyncio.create_task(self._repair_loop())
        self._adaptation_task = asyncio.create_task(self._adaptation_loop())
        self._anomaly_task = asyncio.create_task(self._anomaly_detection_loop())
        
        # Thread pool for parallel processing
        self._thread_pool = ThreadPoolExecutor(max_workers=4)
        
        logger.info(f"Enhanced Pigment Array initialized with {len(self.pigments)} pigment types")
        if TENSORFLOW_AVAILABLE:
            logger.info("TensorFlow-based prediction models available")
    
    def sense_environment(self, environmental_data: Dict[str, float]) -> Dict[str, float]:
        """
        Enhanced environmental sensing with adaptive sensitivity,
        circadian modulation, photoinhibition protection, and vectorized processing.
        """
        # Extract target values in vectorized form
        target_values = np.array([environmental_data.get(t, 0.0) for t in self._targets])
        
        # Vectorized processing for all pigments
        current_hour = datetime.now(timezone.utc).hour
        
        # Get health states
        health_efficiencies = np.array([self.pigment_health[p].efficiency for p in self._pigment_names])
        health_protection = np.array([self.pigment_health[p].protection_level for p in self._pigment_names])
        health_states = np.array([1.0 if self.pigment_health[p].state != PigmentState.DAMAGED else 0.0 
                                   for p in self._pigment_names])
        
        # Calculate effective sensitivities
        circadian_factors = np.array([
            self._get_circadian_factor(p, current_hour) for p in self._pigment_names
        ])
        
        effective_sensitivity = (
            self._sensitivities * 
            circadian_factors * 
            health_efficiencies * 
            (1.0 - health_protection) * 
            health_states
        )
        
        # Calculate excitations (vectorized)
        excitations = target_values * effective_sensitivity
        
        # Apply noise floor and saturation (vectorized)
        excitations = np.where(excitations < self._noise_floors, 0, excitations)
        excitations = np.minimum(excitations, self._saturation_thresholds)
        
        # Convert to dictionary
        result = dict(zip(self._pigment_names, excitations))
        
        # Track photoinhibition and record history
        for i, pigment_name in enumerate(self._pigment_names):
            excitation = excitations[i]
            config = self.pigments[pigment_name]
            health = self.pigment_health[pigment_name]
            
            # Track photoinhibition
            if excitation > config['safe_excitation_level']:
                damage = (excitation - config['safe_excitation_level']) * config['photoinhibition_rate']
                health.damage_accumulation += damage
                health.efficiency = max(0.1, 1.0 - health.damage_accumulation)
                health.protection_level = min(0.9, health.damage_accumulation * 2)
                
                if health.damage_accumulation > 0.3 and health.state == PigmentState.ACTIVE:
                    health.state = PigmentState.PHOTOINHIBITED
                    logger.warning(f"{pigment_name} photoinhibited (damage: {health.damage_accumulation:.2f})")
                elif health.damage_accumulation > 0.7:
                    health.state = PigmentState.DAMAGED
                    logger.error(f"{pigment_name} DAMAGED (damage: {health.damage_accumulation:.2f})")
            else:
                # Gradual recovery when below safe level
                if health.damage_accumulation > 0:
                    health.damage_accumulation = max(0, health.damage_accumulation - 0.001)
                    health.efficiency = max(0.1, 1.0 - health.damage_accumulation)
                    health.protection_level = max(0, health.protection_level - 0.005)
            
            # Record for predictive modeling
            self.excitation_history[pigment_name].append(
                ExcitationRecord(
                    timestamp=datetime.now(timezone.utc),
                    pigment_name=pigment_name,
                    excitation_level=float(excitation),
                    converted_energy=0.0,
                    environmental_context=environmental_data,
                    anomaly_flag=self.anomaly_detector.is_anomaly(pigment_name, float(excitation))
                )
            )
            
            health.total_excitations += 1
        
        # Update predictive models
        if len(self.excitation_history['chlorophyll_a']) % 30 == 0:
            self._update_predictive_models()
        
        # Vectorized anomaly detection
        if len(self.excitation_history['chlorophyll_a']) % 60 == 0:
            self._update_anomaly_thresholds()
        
        return result
    
    def _get_circadian_factor(self, pigment_name: str, hour: int) -> float:
        """Get circadian modulation factor for pigment at given hour"""
        # Use learned patterns if available
        if pigment_name in self.circadian_model.base_profile.learned_patterns:
            patterns = self.circadian_model.base_profile.learned_patterns[pigment_name]
            if hour in patterns:
                # Blend with solar-based calculation
                solar_factor = self.circadian_model.get_efficiency_at_time(datetime.now(timezone.utc))
                learned_factor = patterns[hour]
                return 0.6 * learned_factor + 0.4 * solar_factor
        
        # Use solar-based calculation
        return self.circadian_model.get_efficiency_at_time(datetime.now(timezone.utc))
    
    def get_antenna_amplification(self, excitations: Dict[str, float]) -> Dict[str, float]:
        """
        Enhanced cooperative amplification with learned correlations.
        Uses vectorized operations for performance.
        """
        amplified = excitations.copy()
        
        # Get excitation array
        names = list(excitations.keys())
        values = np.array([excitations[n] for n in names])
        
        # Calculate correlation matrix (vectorized)
        n_pigments = len(names)
        correlation_matrix = np.eye(n_pigments)
        
        for i in range(n_pigments):
            for j in range(i + 1, n_pigments):
                corr = self._calculate_correlation_vectorized(names[i], names[j])
                correlation_matrix[i, j] = corr
                correlation_matrix[j, i] = corr
        
        # Apply amplification (vectorized)
        for i, name in enumerate(names):
            if i < n_pigments:
                amplification = 1.0
                for j in range(n_pigments):
                    if i != j and values[j] > 0:
                        corr = correlation_matrix[i, j]
                        amp_factor = 0.3 * values[i] * values[j] * corr
                        amplification += amp_factor
                
                amplified[name] = min(
                    amplified[name] * amplification,
                    self.pigments[name]['saturation_threshold']
                )
        
        return amplified
    
    def _calculate_correlation_vectorized(self, pigment_a: str, pigment_b: str) -> float:
        """Calculate correlation between two pigments using vectorized operations"""
        history_a = np.array([h.excitation_level for h in list(self.excitation_history.get(pigment_a, []))[-100:]])
        history_b = np.array([h.excitation_level for h in list(self.excitation_history.get(pigment_b, []))[-100:]])
        
        if len(history_a) < 10 or len(history_b) < 10:
            return 0.5
        
        min_len = min(len(history_a), len(history_b))
        if min_len < 10:
            return 0.5
        
        correlation = np.corrcoef(history_a[:min_len], history_b[:min_len])[0, 1]
        return max(0, correlation) if not np.isnan(correlation) else 0.5
    
    async def _repair_loop(self):
        """Background repair loop for photoinhibited pigments"""
        while True:
            try:
                for name, health in self.pigment_health.items():
                    if health.state in [PigmentState.PHOTOINHIBITED, PigmentState.REPAIRING]:
                        config = self.pigments[name]
                        health.repair_progress += config['repair_rate']
                        
                        if health.repair_progress >= 1.0:
                            health.state = PigmentState.ACTIVE
                            health.damage_accumulation = max(0, health.damage_accumulation - 0.2)
                            health.efficiency = 1.0 - health.damage_accumulation
                            health.repair_progress = 0.0
                            health.protection_level = 0.0
                            health.last_repair = datetime.now(timezone.utc)
                            logger.info(f"{name} repaired and reactivated")
                        else:
                            health.state = PigmentState.REPAIRING
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Repair loop error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _adaptation_loop(self):
        """Background adaptation loop for learning circadian patterns"""
        while True:
            try:
                for pigment_name in self._pigment_names:
                    history = list(self.excitation_history.get(pigment_name, []))
                    if len(history) < 100:
                        continue
                    
                    hour_excitations = {}
                    for record in history[-500:]:
                        hour = record.timestamp.hour
                        if hour not in hour_excitations:
                            hour_excitations[hour] = []
                        hour_excitations[hour].append(record.excitation_level)
                    
                    if pigment_name not in self.circadian_model.base_profile.learned_patterns:
                        self.circadian_model.base_profile.learned_patterns[pigment_name] = {}
                    
                    for hour, excitations in hour_excitations.items():
                        if len(excitations) >= 5:
                            avg = np.mean(excitations)
                            old = self.circadian_model.base_profile.learned_patterns[pigment_name].get(hour, avg)
                            self.circadian_model.base_profile.learned_patterns[pigment_name][hour] = old * 0.7 + avg * 0.3
                
                self.circadian_model.base_profile.last_updated = datetime.now(timezone.utc)
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Adaptation loop error: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _anomaly_detection_loop(self):
        """Background loop for anomaly detection and response"""
        while True:
            try:
                for name in self._pigment_names:
                    history = list(self.excitation_history.get(name, []))
                    if len(history) > 20:
                        values = [h.excitation_level for h in history[-50:]]
                        self.anomaly_detector.update_baseline(name, values)
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Anomaly detection loop error: {str(e)}")
                await asyncio.sleep(600)
    
    def _update_anomaly_thresholds(self):
        """Update anomaly detection thresholds"""
        # Implementation updates baseline models
        pass
    
    def _update_predictive_models(self):
        """Update predictive models for each pigment"""
        for pigment_name in self._pigment_names:
            history = list(self.excitation_history.get(pigment_name, []))
            if len(history) < 20:
                continue
            
            values = np.array([h.excitation_level for h in history[-100:]])
            
            # Use LSTM if available
            if TENSORFLOW_AVAILABLE and len(values) >= 50:
                self._update_lstm_model(pigment_name, values)
            
            # Simple exponential smoothing (fallback)
            alpha = 0.3
            level = values[0]
            trend = 0
            
            for i in range(1, len(values)):
                new_level = alpha * values[i] + (1 - alpha) * (level + trend)
                new_trend = 0.1 * (new_level - level) + 0.9 * trend
                level, trend = new_level, new_trend
            
            self.prediction_models[pigment_name] = {
                'level': float(level),
                'trend': float(trend),
                'last_updated': datetime.now(timezone.utc),
                'confidence': 0.8 if len(values) > 50 else 0.5
            }
    
    def _update_lstm_model(self, pigment_name: str, values: np.ndarray):
        """Update LSTM prediction model"""
        if not TENSORFLOW_AVAILABLE:
            return
        
        try:
            # Prepare training data
            sequence_length = 20
            X, y = [], []
            for i in range(len(values) - sequence_length - 1):
                X.append(values[i:i + sequence_length])
                y.append(values[i + sequence_length])
            
            if len(X) < 10:
                return
            
            X = np.array(X).reshape(-1, sequence_length, 1)
            y = np.array(y)
            
            # Build or update model
            if pigment_name not in self.lstm_predictors:
                model = tf.keras.Sequential([
                    tf.keras.layers.LSTM(32, return_sequences=True, input_shape=(sequence_length, 1)),
                    tf.keras.layers.Dropout(0.2),
                    tf.keras.layers.LSTM(16, return_sequences=False),
                    tf.keras.layers.Dropout(0.2),
                    tf.keras.layers.Dense(8, activation='relu'),
                    tf.keras.layers.Dense(1)
                ])
                model.compile(optimizer='adam', loss='mse')
                self.lstm_predictors[pigment_name] = model
            else:
                model = self.lstm_predictors[pigment_name]
            
            # Train incrementally
            model.fit(X, y, epochs=5, batch_size=16, verbose=0)
            
        except Exception as e:
            logger.error(f"LSTM model update failed for {pigment_name}: {e}")
    
    def predict_excitation(self, pigment_name: str, horizon_seconds: float) -> float:
        """Predict future excitation for a pigment using best available model"""
        # Try LSTM prediction first
        if TENSORFLOW_AVAILABLE and pigment_name in self.lstm_predictors:
            try:
                model = self.lstm_predictors[pigment_name]
                history = list(self.excitation_history.get(pigment_name, []))[-20:]
                if len(history) >= 20:
                    sequence = np.array([h.excitation_level for h in history[-20:]]).reshape(1, 20, 1)
                    prediction = model.predict(sequence, verbose=0)[0, 0]
                    return float(max(0, min(1, prediction)))
            except Exception as e:
                logger.error(f"LSTM prediction failed for {pigment_name}: {e}")
        
        # Fallback to exponential smoothing
        if pigment_name in self.prediction_models:
            model = self.prediction_models[pigment_name]
            predicted = model['level'] + model['trend'] * (horizon_seconds / 60.0)
            return float(max(0, min(1, predicted)))
        
        return 0.5
    
    def get_dominant_signal(self, excitations: Dict[str, float]) -> Tuple[str, float]:
        """Get the dominant environmental signal"""
        if not excitations:
            return 'none', 0.0
        return max(excitations.items(), key=lambda x: x[1])
    
    def get_pigment_health_summary(self) -> Dict[str, Any]:
        """Get health summary for all pigments"""
        return {
            name: {
                'state': health.state.value,
                'efficiency': health.efficiency,
                'damage': health.damage_accumulation,
                'repair_progress': health.repair_progress,
                'protection_level': health.protection_level,
                'total_excitations': health.total_excitations,
                'recovery_rate': health.recovery_rate
            }
            for name, health in self.pigment_health.items()
        }
    
    def get_circadian_summary(self) -> Dict[str, Any]:
        """Get circadian rhythm summary"""
        return {
            'latitude': self.circadian_model.latitude,
            'longitude': self.circadian_model.longitude,
            'current_efficiency': self.circadian_model.get_efficiency_at_time(datetime.now(timezone.utc)),
            'learned_patterns': self.circadian_model.base_profile.learned_patterns,
            'last_updated': self.circadian_model.base_profile.last_updated.isoformat()
        }
    
    def get_predictions(self) -> Dict[str, Dict[str, Any]]:
        """Get predictions for all pigments"""
        predictions = {}
        for name in self._pigment_names:
            short = self.predict_excitation(name, 60)
            medium = self.predict_excitation(name, 300)
            long = self.predict_excitation(name, 1800)
            predictions[name] = {
                'short_term_60s': short,
                'medium_term_300s': medium,
                'long_term_1800s': long,
                'trend': 'rising' if medium > short * 1.05 else 'falling' if medium < short * 0.95 else 'stable',
                'confidence': self.prediction_models.get(name, {}).get('confidence', 0.5)
            }
        return predictions

# ============================================================================
# Environmental Anomaly Detector
# ============================================================================

class EnvironmentalAnomalyDetector:
    """
    Detect and respond to unusual environmental patterns.
    Uses statistical methods and ML for anomaly detection.
    """
    
    def __init__(self, anomaly_threshold: float = 3.0):
        self.baseline_models: Dict[str, Dict[str, Any]] = {}
        self.anomaly_threshold = anomaly_threshold
        self.anomaly_history: Dict[str, deque] = {}
        self.alert_callbacks = []
    
    def update_baseline(self, pigment_name: str, values: List[float]):
        """Update baseline model for anomaly detection"""
        if len(values) < 10:
            return
        
        mean = np.mean(values)
        std = np.std(values)
        
        self.baseline_models[pigment_name] = {
            'mean': mean,
            'std': std,
            'last_updated': datetime.now(timezone.utc),
            'count': len(values)
        }
    
    def is_anomaly(self, pigment_name: str, value: float) -> bool:
        """Check if a value is anomalous"""
        if pigment_name not in self.baseline_models:
            return False
        
        model = self.baseline_models[pigment_name]
        z_score = abs(value - model['mean']) / (model['std'] + 1e-6)
        
        is_anomaly = z_score > self.anomaly_threshold
        
        if is_anomaly:
            if pigment_name not in self.anomaly_history:
                self.anomaly_history[pigment_name] = deque(maxlen=20)
            self.anomaly_history[pigment_name].append({
                'timestamp': datetime.now(timezone.utc),
                'value': value,
                'z_score': z_score
            })
        
        return is_anomaly
    
    def get_anomaly_stats(self) -> Dict[str, Any]:
        """Get anomaly detection statistics"""
        stats = {}
        for pigment_name, history in self.anomaly_history.items():
            stats[pigment_name] = {
                'count': len(history),
                'recent': list(history)[-5:] if history else []
            }
        return stats
    
    def register_alert_callback(self, callback):
        """Register callback for anomaly alerts"""
        self.alert_callbacks.append(callback)
    
    async def trigger_alert(self, pigment_name: str, value: float):
        """Trigger anomaly alert"""
        for callback in self.alert_callbacks:
            try:
                await callback(pigment_name, value)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

# ============================================================================
# Health Dashboard & Monitoring
# ============================================================================

class HealthMonitor:
    """
    Comprehensive health monitoring and visualization.
    Provides real-time metrics and automated recommendations.
    """
    
    def __init__(self, harvester_id: str, prometheus_enabled: bool = False):
        self.harvester_id = harvester_id
        self.prometheus_enabled = prometheus_enabled
        self.metrics: Dict[str, Any] = {}
        self.recommendations: List[Dict[str, Any]] = []
        self.alert_history: deque = deque(maxlen=100)
        
        # Initialize health thresholds
        self.thresholds = {
            'efficiency_warning': 0.6,
            'efficiency_critical': 0.3,
            'damage_warning': 0.4,
            'damage_critical': 0.7,
            'harvest_rate_min': 0.1,
            'prediction_accuracy_min': 0.7
        }
        
        # Prometheus integration placeholder
        if prometheus_enabled:
            try:
                from prometheus_client import Gauge, Counter, Histogram
                self.prometheus_metrics = {
                    'harvesting_rate': Gauge('harvester_rate', 'Harvesting rate'),
                    'pigment_health': Gauge('pigment_health', 'Pigment health', ['pigment']),
                    'mode_transitions': Counter('mode_transitions', 'Mode transitions'),
                    'prediction_accuracy': Histogram('prediction_accuracy', 'Prediction accuracy')
                }
            except ImportError:
                logger.warning("Prometheus client not available")
                self.prometheus_enabled = False
    
    def collect_metrics(self, harvester_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect comprehensive health metrics from harvester state.
        
        Args:
            harvester_state: Complete harvester state dictionary
            
        Returns:
            Dict with health metrics
        """
        health_metrics = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'harvester_id': self.harvester_id,
            'overall_health': 1.0,
            'metrics': {},
            'alerts': [],
            'recommendations': []
        }
        
        # Analyze pigment health
        pigment_health = harvester_state.get('pigment_health', {})
        pigment_scores = []
        for pigment, health in pigment_health.items():
            score = health.get('efficiency', 1.0)
            pigment_scores.append(score)
            
            # Check thresholds
            if score < self.thresholds['efficiency_critical']:
                health_metrics['alerts'].append({
                    'level': 'critical',
                    'component': pigment,
                    'message': f'Critically low efficiency: {score:.2f}'
                })
            elif score < self.thresholds['efficiency_warning']:
                health_metrics['alerts'].append({
                    'level': 'warning',
                    'component': pigment,
                    'message': f'Low efficiency: {score:.2f}'
                })
        
        # Calculate overall health
        if pigment_scores:
            health_metrics['overall_health'] = np.mean(pigment_scores)
        
        # Check harvesting rate
        rate = harvester_state.get('peak_harvest_rate', 0)
        if rate < self.thresholds['harvest_rate_min']:
            health_metrics['alerts'].append({
                'level': 'warning',
                'component': 'harvester',
                'message': f'Low harvesting rate: {rate:.2f}'
            })
        
        # Generate recommendations
        health_metrics['recommendations'] = self.generate_recommendations(health_metrics)
        
        # Update Prometheus if enabled
        if self.prometheus_enabled:
            self._update_prometheus(health_metrics)
        
        self.metrics = health_metrics
        return health_metrics
    
    def generate_recommendations(self, health_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate automated recommendations based on health metrics"""
        recommendations = []
        
        # Check for photoinhibition
        for alert in health_metrics.get('alerts', []):
            if 'efficiency' in alert['message']:
                recommendations.append({
                    'priority': 'high' if alert['level'] == 'critical' else 'medium',
                    'action': f"Reduce harvesting intensity for {alert['component']}",
                    'reason': alert['message'],
                    'implementation': 'Switch to CONSERVATIVE mode or enable repair cycle'
                })
        
        # Check circadian patterns
        if 'circadian' in health_metrics:
            current_efficiency = health_metrics.get('current_efficiency', 0.5)
            if current_efficiency > 0.8:
                recommendations.append({
                    'priority': 'low',
                    'action': 'Optimize harvesting schedule',
                    'reason': f'High circadian efficiency ({current_efficiency:.2f}) detected',
                    'implementation': 'Consider increasing harvesting activity now'
                })
        
        # Add general recommendations
        if health_metrics['overall_health'] < 0.5:
            recommendations.append({
                'priority': 'critical',
                'action': 'Perform system reset',
                'reason': 'System health critically low',
                'implementation': 'Initiate emergency recovery protocol'
            })
        
        return recommendations
    
    def _update_prometheus(self, metrics: Dict[str, Any]):
        """Update Prometheus metrics (placeholder)"""
        if not self.prometheus_enabled:
            return
        
        try:
            for name, value in metrics.get('metrics', {}).items():
                if name in self.prometheus_metrics:
                    self.prometheus_metrics[name].set(value)
        except Exception as e:
            logger.error(f"Prometheus update failed: {e}")

# ============================================================================
# Self-Healing Mechanisms
# ============================================================================

class SelfHealer:
    """
    Automated recovery from degradation states.
    Implements multiple healing strategies with validation.
    """
    
    def __init__(self, harvester):
        self.harvester = harvester
        self.healing_attempts: Dict[str, int] = {}
        self.max_attempts = 3
        self.cooldown_period = 300  # 5 minutes
        
        self.healing_strategies = {
            'photoinhibition': self._apply_photoinhibition_healing,
            'prediction_drift': self._recalibrate_predictions,
            'gradient_stagnation': self._stimulate_gradients,
            'efficiency_collapse': self._restore_efficiency
        }
    
    async def diagnose_and_heal(self, health_report: Dict[str, Any]) -> bool:
        """
        Identify and attempt to resolve issues.
        
        Returns:
            bool: True if healing was successful
        """
        healed = False
        
        # Analyze health report
        for alert in health_report.get('alerts', []):
            component = alert.get('component', 'unknown')
            
            # Check if already attempted too many times
            attempt_key = f"{component}_{alert['level']}"
            if self.healing_attempts.get(attempt_key, 0) >= self.max_attempts:
                logger.warning(f"Max healing attempts reached for {attempt_key}")
                continue
            
            # Apply appropriate strategy
            strategy = self._determine_strategy(alert)
            if strategy and strategy in self.healing_strategies:
                logger.info(f"Applying healing strategy: {strategy} for {component}")
                success = await self.healing_strategies[strategy](alert)
                
                if success:
                    healed = True
                    self.healing_attempts[attempt_key] = self.healing_attempts.get(attempt_key, 0) + 1
                    
                    # Cooldown
                    await asyncio.sleep(self.cooldown_period)
        
        return healed
    
    def _determine_strategy(self, alert: Dict[str, Any]) -> Optional[str]:
        """Determine appropriate healing strategy based on alert"""
        message = alert.get('message', '').lower()
        
        if 'photoinhibit' in message or 'damage' in message:
            return 'photoinhibition'
        elif 'prediction' in message or 'drift' in message:
            return 'prediction_drift'
        elif 'gradient' in message or 'stagnat' in message:
            return 'gradient_stagnation'
        elif 'efficiency' in message:
            return 'efficiency_collapse'
        
        return None
    
    async def _apply_photoinhibition_healing(self, alert: Dict) -> bool:
        """Heal photoinhibited pigments"""
        try:
            # Reduce harvesting intensity
            if hasattr(self.harvester, 'set_mode'):
                self.harvester.set_mode(HarvestingMode.CONSERVATIVE)
            
            # Force repair cycle
            if hasattr(self.harvester, 'pigments'):
                for pigment, health in self.harvester.pigments.pigment_health.items():
                    if health.state in [PigmentState.PHOTOINHIBITED, PigmentState.DAMAGED]:
                        health.state = PigmentState.REPAIRING
                        health.repair_progress = 0.5  # Boost repair
            
            return True
        except Exception as e:
            logger.error(f"Photoinhibition healing failed: {e}")
            return False
    
    async def _recalibrate_predictions(self, alert: Dict) -> bool:
        """Recalibrate prediction models"""
        try:
            # Reset predictions and force relearning
            if hasattr(self.harvester, 'pigments'):
                for pigment in self.harvester.pigments._pigment_names:
                    if pigment in self.harvester.pigments.prediction_models:
                        # Reset model with current data
                        history = list(self.harvester.pigments.excitation_history.get(pigment, []))
                        if len(history) > 10:
                            values = [h.excitation_level for h in history[-50:]]
                            alpha = 0.5  # Higher learning rate
                            level = values[0]
                            for value in values[1:]:
                                level = alpha * value + (1 - alpha) * level
                            
                            self.harvester.pigments.prediction_models[pigment] = {
                                'level': level,
                                'trend': 0,
                                'last_updated': datetime.now(timezone.utc),
                                'confidence': 0.3
                            }
            
            return True
        except Exception as e:
            logger.error(f"Prediction recalibration failed: {e}")
            return False
    
    async def _stimulate_gradients(self, alert: Dict) -> bool:
        """Stimulate gradient fields"""
        try:
            if hasattr(self.harvester, 'gradient_manager') and self.harvester.gradient_manager:
                # Pump fields to stimulate activity
                self.harvester.gradient_manager.pump_field('opportunity', 0.01, source='self_healer')
                self.harvester.gradient_manager.pump_field('carbon', -0.005, source='self_healer')
            
            return True
        except Exception as e:
            logger.error(f"Gradient stimulation failed: {e}")
            return False
    
    async def _restore_efficiency(self, alert: Dict) -> bool:
        """Restore collapsed efficiency"""
        try:
            # Reset reaction center
            if hasattr(self.harvester, 'reaction_center'):
                self.harvester.reaction_center.cumulative_damage = max(
                    0, self.harvester.reaction_center.cumulative_damage - 0.2
                )
                self.harvester.reaction_center.current_efficiency = max(
                    self.harvester.reaction_center.min_efficiency,
                    self.harvester.reaction_center.base_quantum_efficiency
                )
            
            return True
        except Exception as e:
            logger.error(f"Efficiency restoration failed: {e}")
            return False

# ============================================================================
# WebSocket Server for Real-Time Monitoring
# ============================================================================

class HarvesterWebSocketServer:
    """
    WebSocket server for real-time harvester status streaming.
    Enables external monitoring and control.
    """
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8765):
        self.host = host
        self.port = port
        self.connections: Set = set()
        self.stream_interval = 1.0
        self.is_running = False
        self.server = None
    
    async def start(self):
        """Start WebSocket server"""
        try:
            import websockets
            self.server = await websockets.serve(self._handle_connection, self.host, self.port)
            self.is_running = True
            logger.info(f"WebSocket server started on {self.host}:{self.port}")
        except ImportError:
            logger.warning("WebSocket support not available")
        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
    
    async def _handle_connection(self, websocket, path):
        """Handle individual WebSocket connection"""
        self.connections.add(websocket)
        try:
            async for message in websocket:
                await self._handle_message(websocket, message)
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            self.connections.remove(websocket)
    
    async def _handle_message(self, websocket, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            # Process control messages
            if data.get('type') == 'subscribe':
                await websocket.send(json.dumps({
                    'type': 'subscribed',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }))
        except Exception as e:
            logger.error(f"Message handling error: {e}")
    
    async def broadcast_update(self, update: Dict[str, Any]):
        """Broadcast update to all connected clients"""
        if not self.connections:
            return
        
        try:
            message = json.dumps(update, default=str)
            disconnected = set()
            
            for connection in self.connections:
                try:
                    await connection.send(message)
                except Exception:
                    disconnected.add(connection)
            
            # Clean up disconnected clients
            self.connections -= disconnected
            
        except Exception as e:
            logger.error(f"Broadcast failed: {e}")
    
    async def stop(self):
        """Stop WebSocket server"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.is_running = False
            logger.info("WebSocket server stopped")

# ============================================================================
# Enhanced Reaction Center
# ============================================================================

class EnhancedReactionCenter:
    """
    Enhanced reaction center with demand-responsive conversion,
    direct gradient coupling, efficiency tracking, and self-healing.
    """
    
    def __init__(self, token_manager=None, gradient_manager=None):
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        
        # Conversion efficiency (dynamic)
        self.base_quantum_efficiency = 0.85
        self.current_efficiency = 0.85
        self.min_efficiency = 0.3
        self.max_efficiency = 0.98
        
        # Energy thresholds
        self.activation_threshold = 0.1
        self.saturation_point = 0.9
        
        # Demand-responsive parameters
        self.demand_modulation_enabled = True
        self.token_abundance_threshold = 50000
        self.token_scarcity_threshold = 5000
        self.demand_response_factor = 0.5
        
        # Photoinhibition tracking
        self.cumulative_damage = 0.0
        self.repair_rate = 0.005
        self.damage_threshold = 0.8
        
        # Conversion history
        self.conversion_history: deque = deque(maxlen=2000)
        
        # Efficiency tracking
        self.efficiency_history: deque = deque(maxlen=100)
        self.performance_metrics = {
            'peak_efficiency': 0.85,
            'avg_conversion_rate': 0.0,
            'total_conversions': 0
        }
        
        # Start maintenance
        self._maintenance_task = asyncio.create_task(self._maintenance_loop())
        self._performance_task = asyncio.create_task(self._performance_loop())
        
        logger.info(f"Enhanced Reaction Center initialized: base_efficiency={self.base_quantum_efficiency}")
    
    def modulate_efficiency(self) -> float:
        """
        Modulate conversion efficiency based on system demand.
        Includes dynamic response factor and performance history.
        """
        if not self.demand_modulation_enabled or not self.token_manager:
            return self.base_quantum_efficiency
        
        summary = self.token_manager.get_system_summary()
        balance = summary.get('total_balance', 10000)
        
        # Calculate demand-based modulation
        if balance > self.token_abundance_threshold:
            excess_ratio = (balance - self.token_abundance_threshold) / self.token_abundance_threshold
            modulation = 1.0 / (1.0 + excess_ratio * self.demand_response_factor)
        elif balance < self.token_scarcity_threshold:
            scarcity_ratio = (self.token_scarcity_threshold - balance) / self.token_scarcity_threshold
            modulation = 1.0 + scarcity_ratio * self.demand_response_factor * 0.5
        else:
            modulation = 1.0
        
        efficiency = self.base_quantum_efficiency * modulation
        
        # Apply photoinhibition damage
        efficiency *= (1.0 - self.cumulative_damage * 0.5)
        
        # Apply recent performance trend
        if self.efficiency_history:
            recent_avg = np.mean(self.efficiency_history)
            performance_factor = 1.0 + (recent_avg - self.current_efficiency) * 0.1
            efficiency *= performance_factor
        
        self.current_efficiency = max(self.min_efficiency, min(self.max_efficiency, efficiency))
        self.efficiency_history.append(self.current_efficiency)
        
        return self.current_efficiency
    
    def convert_excitation(self, excitations: Dict[str, float], account_id: str) -> float:
        """
        Enhanced conversion with demand modulation, gradient coupling, and performance tracking.
        
        Returns amount of Eco-ATP generated.
        """
        total_excitation = sum(excitations.values())
        
        if total_excitation < self.activation_threshold:
            return 0.0
        
        effective_excitation = min(total_excitation, self.saturation_point)
        
        # Modulate efficiency based on demand
        efficiency = self.modulate_efficiency()
        
        # Apply efficiency
        convertible_energy = effective_excitation * efficiency
        
        # Track photoinhibition
        if effective_excitation > 0.8:
            self.cumulative_damage += 0.0005
        elif effective_excitation < 0.3:
            self.cumulative_damage = max(0, self.cumulative_damage - 0.0001)
        
        # Check for critical damage
        if self.cumulative_damage > self.damage_threshold:
            logger.warning(f"Reaction center critically damaged: {self.cumulative_damage:.2f}")
            self.current_efficiency = self.min_efficiency
        
        # Calculate savings
        savings = self._calculate_savings(excitations)
        
        # Generate Eco-ATP tokens
        total_generated = self._generate_tokens(account_id, convertible_energy, savings, efficiency)
        
        # Direct gradient coupling
        self._couple_gradients(total_generated, excitations)
        
        # Record conversion
        self._record_conversion(total_excitation, effective_excitation, efficiency, 
                              convertible_energy, total_generated, savings)
        
        # Update performance metrics
        self.performance_metrics['total_conversions'] += 1
        if total_generated > 0:
            self.performance_metrics['avg_conversion_rate'] = (
                self.performance_metrics['avg_conversion_rate'] * 0.9 + total_generated * 0.1
            )
        
        if total_generated > 0:
            logger.debug(
                f"Reaction Center: {total_excitation:.3f} excitation → "
                f"{total_generated:.1f} Eco-ATP (efficiency: {efficiency:.2f})"
            )
        
        return total_generated
    
    def _calculate_savings(self, excitations: Dict[str, float]) -> Dict[str, float]:
        """Calculate savings in carbon, helium, and energy"""
        return {
            'carbon_saved': excitations.get('chlorophyll_b', 0) * 0.001,
            'helium_saved': excitations.get('carotenoids', 0) * 0.01,
            'energy_saved': excitations.get('chlorophyll_a', 0) * 0.01
        }
    
    def _generate_tokens(self, account_id: str, convertible_energy: float, 
                         savings: Dict[str, float], efficiency: float) -> float:
        """Generate Eco-ATP tokens with appropriate source"""
        if not self.token_manager:
            return convertible_energy * efficiency * 0.5  # Simulated
        
        source = EcoATPSource.RENEWABLE_ENERGY
        if savings['carbon_saved'] > savings['energy_saved']:
            source = EcoATPSource.EFFICIENCY_GAIN
        
        tokens = self.token_manager.generate_tokens(
            account_id=account_id,
            source=source,
            carbon_saved_kg=savings['carbon_saved'],
            helium_saved_units=savings['helium_saved'],
            energy_saved_kwh=savings['energy_saved'],
            efficiency=efficiency
        )
        
        return sum(t.value for t in tokens) if tokens else 0
    
    def _couple_gradients(self, total_generated: float, excitations: Dict[str, float]):
        """Direct gradient field coupling"""
        if not self.gradient_manager:
            return
        
        try:
            if total_generated > 0:
                self.gradient_manager.pump_field('opportunity', total_generated / 1000.0, source='harvester')
            
            if excitations.get('chlorophyll_b', 0) > 0.5:
                self.gradient_manager.pump_field('carbon', -0.01, source='harvester_carbon_detection')
        except Exception as e:
            logger.error(f"Gradient coupling failed: {e}")
    
    def _record_conversion(self, total_excitation: float, effective_excitation: float,
                           efficiency: float, convertible_energy: float,
                           total_generated: float, savings: Dict[str, float]):
        """Record conversion details for analytics"""
        self.conversion_history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'total_excitation': total_excitation,
            'effective_excitation': effective_excitation,
            'efficiency': efficiency,
            'convertible_energy': convertible_energy,
            'eco_atp_generated': total_generated,
            'carbon_saved': savings['carbon_saved'],
            'helium_saved': savings['helium_saved'],
            'energy_saved': savings['energy_saved'],
            'cumulative_damage': self.cumulative_damage
        })
    
    async def _maintenance_loop(self):
        """Background maintenance for reaction center repair"""
        while True:
            try:
                if self.cumulative_damage > 0:
                    self.cumulative_damage = max(0, self.cumulative_damage - self.repair_rate)
                    self.current_efficiency = max(
                        self.min_efficiency,
                        self.base_quantum_efficiency * (1.0 - self.cumulative_damage * 0.5)
                    )
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Reaction center maintenance error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _performance_loop(self):
        """Track and optimize performance metrics"""
        while True:
            try:
                # Calculate performance trend
                if len(self.efficiency_history) > 50:
                    recent = list(self.efficiency_history)[-50:]
                    trend = (recent[-1] - recent[0]) / len(recent)
                    
                    # Adjust if performance is declining
                    if trend < -0.001:
                        logger.info(f"Performance declining: {trend:.4f}")
                        self.demand_response_factor = min(0.8, self.demand_response_factor + 0.01)
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Performance loop error: {e}")
                await asyncio.sleep(600)
    
    def get_efficiency_stats(self) -> Dict[str, Any]:
        """Get efficiency statistics"""
        recent = list(self.conversion_history)[-50:]
        return {
            'current_efficiency': self.current_efficiency,
            'base_efficiency': self.base_quantum_efficiency,
            'cumulative_damage': self.cumulative_damage,
            'demand_modulation': self.demand_modulation_enabled,
            'average_efficiency': np.mean([c['efficiency'] for c in recent]) if recent else 0,
            'total_conversions': len(self.conversion_history),
            'performance_metrics': self.performance_metrics
        }

# ============================================================================
# NEW: Genetic Optimizer for Harvester Parameters
# ============================================================================

class HarvesterGeneticOptimizer:
    """
    Genetic algorithm to evolve harvester parameters:
    - Conversion costs (energy_conversion_factor per pigment)
    - Sensitivity multipliers (scaling of base sensitivity)
    - Repair rates for each pigment
    """
    
    def __init__(self, harvester: 'EnhancedPhotosyntheticHarvester'):
        self.harvester = harvester
        self.population_size = 20
        self.mutation_rate = 0.2
        self.crossover_rate = 0.7
        self.generations = 10
        self.tournament_size = 3
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        
        self.param_bounds = {
            'conversion_factors': (0.001, 0.1),   # for each pigment
            'sensitivity_multipliers': (0.5, 2.0),
            'repair_rates': (0.005, 0.05)
        }
        logger.info("Harvester Genetic Optimizer initialized")
    
    def _initialize_individual(self) -> Dict:
        """Generate random parameter set."""
        ind = {
            'conversion_factors': {},
            'sensitivity_multipliers': {},
            'repair_rates': {}
        }
        pigments = self.harvester.pigments.pigments.keys()
        for p in pigments:
            ind['conversion_factors'][p] = random.uniform(*self.param_bounds['conversion_factors'])
            ind['sensitivity_multipliers'][p] = random.uniform(*self.param_bounds['sensitivity_multipliers'])
            ind['repair_rates'][p] = random.uniform(*self.param_bounds['repair_rates'])
        return ind
    
    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]
    
    def _fitness(self, individual: Dict) -> float:
        """Fitness based on average token generation rate and system health."""
        # Temporarily apply parameters
        self._apply_individual(individual)
        # Evaluate fitness
        stats = self.harvester.get_harvesting_stats()
        total_harvested = stats.get('total_harvested', 0)
        harvest_cycles = stats.get('harvest_cycles', 1)
        avg_rate = total_harvested / max(harvest_cycles, 1)
        efficiency = stats.get('efficiency', 0.5)
        health = stats.get('health_metrics', {}).get('overall_health', 0.5)
        fitness = 0.5 * avg_rate + 0.3 * efficiency + 0.2 * health
        self._restore_original_parameters()
        return fitness
    
    def _apply_individual(self, individual: Dict):
        """Temporarily apply parameters to harvester."""
        self._original_params = {
            'conversion_factors': {},
            'sensitivity_multipliers': {},
            'repair_rates': {}
        }
        pigments = self.harvester.pigments.pigments
        for p in pigments:
            self._original_params['conversion_factors'][p] = pigments[p]['energy_conversion_factor']
            self._original_params['sensitivity_multipliers'][p] = pigments[p]['sensitivity']
            self._original_params['repair_rates'][p] = self.harvester.pigments.pigment_health[p].recovery_rate
            # Apply new values
            pigments[p]['energy_conversion_factor'] = individual['conversion_factors'][p]
            pigments[p]['sensitivity'] = individual['sensitivity_multipliers'][p] * pigments[p]['base_sensitivity']
            self.harvester.pigments.pigment_health[p].recovery_rate = individual['repair_rates'][p]
    
    def _restore_original_parameters(self):
        if hasattr(self, '_original_params'):
            pigments = self.harvester.pigments.pigments
            for p in pigments:
                pigments[p]['energy_conversion_factor'] = self._original_params['conversion_factors'][p]
                pigments[p]['sensitivity'] = self._original_params['sensitivity_multipliers'][p] * pigments[p]['base_sensitivity']
                self.harvester.pigments.pigment_health[p].recovery_rate = self._original_params['repair_rates'][p]
    
    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]
    
    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {'conversion_factors': {}, 'sensitivity_multipliers': {}, 'repair_rates': {}}
        pigments = self.harvester.pigments.pigments.keys()
        for p in pigments:
            if random.random() < 0.5:
                child['conversion_factors'][p] = parent1['conversion_factors'][p]
                child['sensitivity_multipliers'][p] = parent1['sensitivity_multipliers'][p]
                child['repair_rates'][p] = parent1['repair_rates'][p]
            else:
                child['conversion_factors'][p] = parent2['conversion_factors'][p]
                child['sensitivity_multipliers'][p] = parent2['sensitivity_multipliers'][p]
                child['repair_rates'][p] = parent2['repair_rates'][p]
            if random.random() < 0.3:
                child['conversion_factors'][p] = (parent1['conversion_factors'][p] + parent2['conversion_factors'][p]) / 2
                child['sensitivity_multipliers'][p] = (parent1['sensitivity_multipliers'][p] + parent2['sensitivity_multipliers'][p]) / 2
                child['repair_rates'][p] = (parent1['repair_rates'][p] + parent2['repair_rates'][p]) / 2
        return child
    
    def _mutate(self, individual: Dict) -> Dict:
        mutated = {'conversion_factors': {}, 'sensitivity_multipliers': {}, 'repair_rates': {}}
        pigments = self.harvester.pigments.pigments.keys()
        for p in pigments:
            mutated['conversion_factors'][p] = individual['conversion_factors'][p]
            mutated['sensitivity_multipliers'][p] = individual['sensitivity_multipliers'][p]
            mutated['repair_rates'][p] = individual['repair_rates'][p]
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.01, 0.01)
                mutated['conversion_factors'][p] = max(0.001, min(0.1, mutated['conversion_factors'][p] + delta))
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.1, 0.1)
                mutated['sensitivity_multipliers'][p] = max(0.5, min(2.0, mutated['sensitivity_multipliers'][p] + delta))
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.002, 0.002)
                mutated['repair_rates'][p] = max(0.005, min(0.05, mutated['repair_rates'][p] + delta))
        return mutated
    
    def _evolve_one_generation(self, population: List[Dict]) -> List[Dict]:
        fitness_scores = [self._fitness(ind) for ind in population]
        new_population = []
        # Elitism
        best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
        new_population.append(population[best_idx])
        while len(new_population) < self.population_size:
            if random.random() < self.crossover_rate:
                parent1 = self._select(population, fitness_scores)
                parent2 = self._select(population, fitness_scores)
                child = self._crossover(parent1, parent2)
                child = self._mutate(child)
                new_population.append(child)
            else:
                parent = self._select(population, fitness_scores)
                new_population.append(parent.copy())
        return new_population
    
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        if generations is None:
            generations = self.generations
        population = self._initialize_population()
        best_fitness = -float('inf')
        best_ind = None
        for gen in range(generations):
            population = self._evolve_one_generation(population)
            fitness_scores = [self._fitness(ind) for ind in population]
            gen_best = max(range(len(population)), key=lambda i: fitness_scores[i])
            if fitness_scores[gen_best] > best_fitness:
                best_fitness = fitness_scores[gen_best]
                best_ind = population[gen_best]
            logger.debug(f"Gen {gen+1}: best fitness = {fitness_scores[gen_best]:.4f}")
        if best_fitness > self.best_fitness:
            self.best_fitness = best_fitness
            self.best_individual = best_ind
            self._apply_individual(best_ind)
            logger.info(f"Applied best individual with fitness {best_fitness:.4f}")
        self.evolution_history.append({
            'timestamp': datetime.now(timezone.utc),
            'best_fitness': best_fitness
        })
        return {'best_fitness': best_fitness, 'best_individual': best_ind}
    
    def get_status(self) -> Dict:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:]
        }

# ============================================================================
# NEW: Competition Engine for Child Harvesters
# ============================================================================

class ChildHarvesterCompetition:
    """
    Manages competition among child harvesters.
    Underperforming children are replaced by mutated copies of top performers.
    """
    
    def __init__(self, parent_harvester: 'EnhancedPhotosyntheticHarvester'):
        self.parent = parent_harvester
        self.competition_interval = 3600  # 1 hour
        self.performance_window = 100  # cycles to consider for performance
        self.replacement_threshold = 0.3  # bottom % of performers to replace
        self._lock = asyncio.Lock()
        logger.info("Child Harvester Competition initialized")
    
    async def run_competition(self):
        """Evaluate child harvesters and replace underperformers."""
        async with self._lock:
            children = list(self.parent.child_harvesters.values())
            if len(children) < 2:
                return
            
            # Compute average token generation per cycle for each child
            performance = {}
            for child in children:
                cycles = child.harvest_cycles
                if cycles > 0:
                    avg = child.total_harvested / cycles
                else:
                    avg = 0
                performance[child.harvester_id] = avg
            
            if not performance:
                return
            
            # Sort by performance
            sorted_perf = sorted(performance.items(), key=lambda x: x[1])
            # Identify bottom performers
            bottom_count = max(1, int(len(sorted_perf) * self.replacement_threshold))
            bottom = [child_id for child_id, _ in sorted_perf[:bottom_count]]
            
            # Identify top performers
            top = [child_id for child_id, _ in sorted_perf[-bottom_count:]]
            if not top:
                return
            
            # For each bottom performer, replace with a mutated copy of a random top performer
            for child_id in bottom:
                # Choose a top performer to replicate
                top_id = random.choice(top)
                top_child = self.parent.child_harvesters.get(top_id)
                if not top_child:
                    continue
                # Create a mutated copy
                new_child = self.parent.spawn_child(top_child.pigments._pigment_names[0])  # placeholder specialization
                # We'll mutate some parameters of the new child
                # For simplicity, mutate sensitivity of its pigments randomly
                for pigment_name, config in new_child.pigments.pigments.items():
                    if random.random() < 0.3:
                        config['sensitivity'] = config['base_sensitivity'] * random.uniform(0.8, 1.2)
                
                # Remove the old child
                self.parent.remove_child(child_id)
                # Add the new child
                self.parent.child_harvesters[new_child.harvester_id] = new_child
                logger.info(f"Replaced child {child_id} with mutated copy {new_child.harvester_id}")
    
    def get_stats(self) -> Dict:
        return {
            'competition_interval': self.competition_interval,
            'replacement_threshold': self.replacement_threshold,
            'children_count': len(self.parent.child_harvesters)
        }

# ============================================================================
# NEW: Swarm Coordinator for Prediction Sharing
# ============================================================================

class SwarmCoordinator:
    """
    Coordinates sharing of predicted peaks among harvesters.
    Allows children to adjust harvesting schedules based on swarm predictions.
    """
    
    def __init__(self, parent_harvester: 'EnhancedPhotosyntheticHarvester'):
        self.parent = parent_harvester
        self.shared_predictions: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        logger.info("Swarm Coordinator initialized")
    
    async def share_predictions(self):
        """Collect predictions from all harvesters and broadcast them."""
        async with self._lock:
            all_predictions = {}
            # Get predictions from parent
            parent_preds = self.parent.pigments.get_predictions()
            all_predictions[self.parent.harvester_id] = parent_preds
            # Get predictions from children
            for child_id, child in self.parent.child_harvesters.items():
                child_preds = child.pigments.get_predictions()
                all_predictions[child_id] = child_preds
            self.shared_predictions = all_predictions
            
            # For each harvester, adjust mode based on aggregated predictions
            # (simplified: if majority predicts high excitation, switch to FULL)
            high_count = sum(
                1 for preds in all_predictions.values()
                for p in preds.values()
                if p.get('medium_term_300s', 0) > 0.7
            )
            total = sum(len(p) for p in all_predictions.values())
            if total > 0 and high_count / total > 0.5:
                # Set mode to FULL for parent (children will inherit via parent's mode)
                self.parent.set_mode(HarvestingMode.FULL)
            elif high_count / total < 0.2:
                self.parent.set_mode(HarvestingMode.CONSERVATIVE)
            else:
                self.parent.set_mode(HarvestingMode.MODULATED)
    
    def get_shared_predictions(self) -> Dict:
        return self.shared_predictions

# ============================================================================
# Enhanced Photosynthetic Harvester (Main Class) – Extended with new features
# ============================================================================

class EnhancedPhotosyntheticHarvester:
    """
    Enhanced Photosynthetic Harvester v6.1.0
    Includes all original features plus:
    - Genetic algorithm for parameter evolution
    - Competition among child harvesters
    - Swarm coordination for prediction sharing
    """
    
    def __init__(self, token_manager=None, gradient_manager=None, 
                 harvester_id: str = "primary", latitude: float = 0.0, 
                 longitude: float = 0.0, enable_persistence: bool = True,
                 enable_websocket: bool = False, websocket_port: int = 8765):
        self.harvester_id = harvester_id
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.latitude = latitude
        self.longitude = longitude
        
        # Enhanced sub-modules
        self.pigments = EnhancedPigmentArray(latitude, longitude)
        self.reaction_center = EnhancedReactionCenter(token_manager, gradient_manager)
        
        # Harvesting mode
        self.mode = HarvestingMode.FULL
        
        # Harvesting statistics
        self.total_harvested = 0.0
        self.harvesting_efficiency = 0.0
        self.peak_harvest_rate = 0.0
        self.harvest_cycles = 0
        
        # Account for harvested energy
        self.account_id = f"photosynthetic_{harvester_id}"
        if token_manager:
            token_manager.create_account(self.account_id)
        
        # Predictive window tracking
        self.predicted_peaks: Dict[str, datetime] = {}
        
        # Child harvesters (multi-harvester scaling)
        self.child_harvesters: Dict[str, 'EnhancedPhotosyntheticHarvester'] = {}
        self.is_child = harvester_id != "primary"
        
        # State persistence
        self.persistence = PersistentHarvesterState(harvester_id) if enable_persistence else None
        
        # Health monitoring
        self.health_monitor = HealthMonitor(harvester_id)
        
        # Self-healing
        self.self_healer = SelfHealer(self)
        
        # WebSocket server
        self.websocket_server = None
        if enable_websocket:
            self.websocket_server = HarvesterWebSocketServer(port=websocket_port)
            asyncio.create_task(self.websocket_server.start())
        
        # Performance tracking
        self.performance_metrics = {
            'start_time': datetime.now(timezone.utc),
            'uptime': 0.0,
            'harvest_rate_avg': 0.0,
            'harvest_rate_peak': 0.0,
            'successful_cycles': 0,
            'failed_cycles': 0
        }
        
        # NEW: Genetic optimizer
        self.genetic_optimizer = HarvesterGeneticOptimizer(self)
        
        # NEW: Competition engine
        self.competition_engine = ChildHarvesterCompetition(self)
        
        # NEW: Swarm coordinator
        self.swarm_coordinator = SwarmCoordinator(self)
        
        # Restore previous state if available
        if self.persistence:
            asyncio.create_task(self._restore_state())
        
        # Start maintenance loops (including new ones)
        self._maintenance_task = asyncio.create_task(self._predictive_window_loop())
        self._metrics_task = asyncio.create_task(self._metrics_loop())
        self._websocket_task = asyncio.create_task(self._websocket_broadcast_loop())
        asyncio.create_task(self._genetic_evolution_loop())
        asyncio.create_task(self._competition_loop())
        asyncio.create_task(self._swarm_coordination_loop())
        
        logger.info(f"Enhanced Photosynthetic Harvester '{harvester_id}' initialized v6.1.0")
    
    # ========================================================================
    # New background loops
    # ========================================================================
    
    async def _genetic_evolution_loop(self):
        """Run genetic optimization periodically."""
        while True:
            try:
                if self.harvest_cycles > 50:
                    logger.info("Starting genetic evolution cycle...")
                    result = await self.genetic_optimizer.evolve(generations=10)
                    logger.info(f"Evolution complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(86400)  # every 24 hours
            except Exception as e:
                logger.error(f"Genetic evolution loop error: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _competition_loop(self):
        """Run child harvester competition periodically."""
        while True:
            try:
                if not self.is_child and len(self.child_harvesters) >= 2:
                    await self.competition_engine.run_competition()
                await asyncio.sleep(self.competition_engine.competition_interval)
            except Exception as e:
                logger.error(f"Competition loop error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _swarm_coordination_loop(self):
        """Share predictions and coordinate modes among harvesters."""
        while True:
            try:
                await self.swarm_coordinator.share_predictions()
                await asyncio.sleep(120)  # every 2 minutes
            except Exception as e:
                logger.error(f"Swarm coordination error: {str(e)}")
                await asyncio.sleep(300)
    
    # ========================================================================
    # Override spawn_child to use the new competition & coordination
    # ========================================================================
    
    def spawn_child(self, specialization: str) -> 'EnhancedPhotosyntheticHarvester':
        """
        Spawn a child harvester specialized in a particular pigment type.
        Enables multi-harvester scaling for high-demand scenarios.
        """
        child_id = f"{self.harvester_id}_child_{specialization}_{len(self.child_harvesters)}"
        
        child = EnhancedPhotosyntheticHarvester(
            token_manager=self.token_manager,
            gradient_manager=self.gradient_manager,
            harvester_id=child_id,
            latitude=self.latitude,
            longitude=self.longitude,
            enable_persistence=bool(self.persistence),
            enable_websocket=False  # Child harvesters don't have their own websocket
        )
        child.is_child = True
        
        # Specialize the child's pigments
        for pigment_name, pigment_config in child.pigments.pigments.items():
            if pigment_config['specialization'] != specialization:
                # Reduce sensitivity for non-specialized pigments
                pigment_config['sensitivity'] *= 0.3
            else:
                # Increase sensitivity for specialized pigment
                pigment_config['sensitivity'] *= 1.5
        
        self.child_harvesters[child_id] = child
        
        logger.info(f"Spawned child harvester '{child_id}' specialized in {specialization}")
        return child
    
    # ========================================================================
    # New public API for enhancements
    # ========================================================================
    
    def get_genetic_status(self) -> Dict:
        return self.genetic_optimizer.get_status()
    
    def get_competition_status(self) -> Dict:
        return self.competition_engine.get_stats()
    
    def get_swarm_status(self) -> Dict:
        return {
            'shared_predictions': self.swarm_coordinator.get_shared_predictions(),
            'children_count': len(self.child_harvesters)
        }
    
    # ========================================================================
    # Override get_harvesting_stats to include new metrics
    # ========================================================================
    
    def get_harvesting_stats(self) -> Dict[str, Any]:
        stats = {
            'harvester_id': self.harvester_id,
            'total_harvested': self.total_harvested,
            'harvest_cycles': self.harvest_cycles,
            'peak_harvest_rate': self.peak_harvest_rate,
            'mode': self.mode.value,
            'efficiency': self.reaction_center.current_efficiency,
            'account_balance': self.token_manager.get_account_summary(self.account_id).get('balance', 0) if self.token_manager else 0,
            'pigment_health': self.pigments.get_pigment_health_summary(),
            'circadian': self.pigments.get_circadian_summary(),
            'predictions': self.pigments.get_predictions(),
            'reaction_center': self.reaction_center.get_efficiency_stats(),
            'predicted_peaks': {k: v.isoformat() for k, v in self.predicted_peaks.items()},
            'child_harvesters': len(self.child_harvesters),
            'is_child': self.is_child,
            'performance_metrics': self.performance_metrics,
            'health_metrics': self.health_monitor.metrics,
            # New
            'genetic_optimizer': self.genetic_optimizer.get_status(),
            'competition': self.competition_engine.get_stats(),
            'swarm': self.get_swarm_status()
        }
        
        # Add recent conversions
        recent = list(self.reaction_center.conversion_history)[-10:]
        stats['recent_conversions'] = recent
        
        return stats

# ============================================================================
# Legacy Compatibility (unchanged)
# ============================================================================

class PhotosyntheticHarvester(EnhancedPhotosyntheticHarvester):
    """
    Legacy Photosynthetic Harvester for backward compatibility.
    Maintains original interface while using enhanced functionality.
    """
    
    def __init__(self, token_manager=None):
        super().__init__(token_manager=token_manager, harvester_id="primary")
        logger.info("Photosynthetic Harvester initialized (legacy compatibility mode)")
    
    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]:
        """Legacy harvest cycle (simplified interface)"""
        result = await super().harvest_cycle(environmental_data)
        
        # Return simplified result for backward compatibility
        return {
            'eco_atp_generated': result.get('eco_atp_generated', 0.0),
            'total_harvested': result.get('total_harvested', 0.0),
            'dominant_signal': result.get('dominant_signal', 'none'),
            'recent_conversions': result.get('recent_conversions', [])
        }

# ============================================================================
# Additional Utility Functions (unchanged)
# ============================================================================

def create_harvester(config: Dict[str, Any]) -> EnhancedPhotosyntheticHarvester:
    """Factory function to create a configured harvester"""
    return EnhancedPhotosyntheticHarvester(
        token_manager=config.get('token_manager'),
        gradient_manager=config.get('gradient_manager'),
        harvester_id=config.get('harvester_id', 'primary'),
        latitude=config.get('latitude', 0.0),
        longitude=config.get('longitude', 0.0),
        enable_persistence=config.get('enable_persistence', True),
        enable_websocket=config.get('enable_websocket', False),
        websocket_port=config.get('websocket_port', 8765)
    )

# ============================================================================
# Example Usage (unchanged)
# ============================================================================

async def example_usage():
    """Example demonstrating the enhanced harvester"""
    
    # Create token manager (placeholder)
    class SimpleTokenManager:
        def create_account(self, account_id):
            pass
        def get_account_summary(self, account_id):
            return {'balance': 10000}
        def generate_tokens(self, **kwargs):
            return []
    
    token_manager = SimpleTokenManager()
    
    # Create harvester with all enhancements
    harvester = EnhancedPhotosyntheticHarvester(
        token_manager=token_manager,
        harvester_id="example_harvester",
        latitude=40.7128,  # New York City
        longitude=-74.0060,
        enable_persistence=True,
        enable_websocket=True
    )
    
    # Simulate environmental data
    environmental_data = {
        'renewable_availability': 0.8,
        'carbon_intensity': 200.0,
        'waste_heat': 0.3,
        'edge_availability': 0.6,
        'system_overload': 0.1
    }
    
    # Run harvest cycles
    for i in range(10):
        result = await harvester.harvest_cycle(environmental_data)
        print(f"Cycle {i}: Generated {result['eco_atp_generated']:.2f} Eco-ATP")
        await asyncio.sleep(1)
    
    # Get statistics
    stats = harvester.get_harvesting_stats()
    print(f"Total harvested: {stats['total_harvested']:.2f}")
    print(f"Peak rate: {stats['peak_harvest_rate']:.2f}")
    print(f"Mode: {stats['mode']}")
    
    # Get circadian report
    report = harvester.get_circadian_report()
    print("Optimal harvesting hours:")
    for hour_info in report['optimal_hours'][:3]:
        print(f"  {hour_info['hour']}:00 ({hour_info['efficiency']:.0%} efficiency)")
    
    # Cleanup
    await harvester.cleanup()

if __name__ == "__main__":
    asyncio.run(example_usage())
