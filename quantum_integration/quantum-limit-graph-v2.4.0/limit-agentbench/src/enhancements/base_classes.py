# File: src/enhancements/base_classes.py (ENHANCED v6.2)

"""
Green Agent Base Classes - Version 6.2

ENHANCEMENTS OVER v6.1:
1. ADDED: Default configuration fallback (no crash on missing config)
2. FIXED: Shared Prometheus registry for metric aggregation
3. ADDED: BaseCollector class for data collectors
4. ADDED: BaseForecaster class for forecasting modules
5. ADDED: BaseVerifier class for verification modules
6. ADDED: Thread-safe ModuleRegistry with unregister/clear
7. ADDED: Missing configuration properties (ai_datacenter, forecaster, api)
8. ADDED: Configuration validation on load
9. ADDED: Configuration hot-reload capability
10. ADDED: Comprehensive docstrings for all classes
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
import logging
import json
import yaml
import os
import uuid
import threading
from datetime import datetime
from pathlib import Path
from collections import defaultdict, deque
from functools import lru_cache
import warnings
import copy

# Production dependencies
from pydantic import BaseModel, Field, validator, root_validator
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# ============================================================
# SHARED PROMETHEUS REGISTRY
# ============================================================

_SHARED_REGISTRY = CollectorRegistry()

def get_shared_registry() -> CollectorRegistry:
    """Get the shared Prometheus registry for all modules"""
    return _SHARED_REGISTRY

# ============================================================
# CONFIGURATION LOADER (ENHANCED)
# ============================================================

class GreenAgentConfig:
    """
    Unified configuration loader for all Green Agent modules.
    
    ENHANCEMENTS:
    - Default configuration fallback
    - Configuration validation
    - Hot-reload capability
    - Missing property additions
    """
    
    _instance = None
    _config = None
    _lock = threading.RLock()
    
    def __new__(cls, config_path: str = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: str = None):
        if self._config is None:
            with self._lock:
                if self._config is None:
                    self.config_path = config_path or self._find_config()
                    self._config = self._load_config()
                    self._resolve_env_vars()
                    self._validate_config()
    
    def _find_config(self) -> Optional[str]:
        """Find configuration file with fallback"""
        search_paths = [
            Path(__file__).parent / "green_agent_config.yaml",
            Path.cwd() / "green_agent_config.yaml",
            Path(os.environ.get("GREEN_AGENT_CONFIG", ""))
        ]
        
        for path in search_paths:
            if path.exists():
                return str(path)
        
        logger = logging.getLogger(__name__)
        logger.warning("No configuration file found. Using embedded defaults.")
        return None
    
    def _load_config(self) -> Dict:
        """Load YAML configuration or use defaults"""
        if self.config_path and Path(self.config_path).exists():
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Return sensible default configuration"""
        return {
            'system': {
                'name': 'Green Agent',
                'version': '6.2.0',
                'environment': 'development',
                'log_level': 'INFO',
                'correlation_id_enabled': True
            },
            'helium': {
                'data_collector': {
                    'csv_path': 'src/enhancements/data/helium_timeseries.csv',
                    'use_synthetic_fallback': True,
                    'cache_ttl_seconds': 3600
                },
                'forecaster': {
                    'seq_length': 60,
                    'output_horizon': 12,
                    'epochs': 100,
                    'early_stopping': True
                }
            },
            'quantum': {
                'provider': 'pennylane',
                'backend': 'default.qubit',
                'n_qubits': 8,
                'shots': 1000,
                'error_mitigation': True
            },
            'blockchain': {
                'provider': 'ethereum',
                'network': 'sepolia',
                'chain_id': 11155111
            },
            'regret_optimizer': {
                'n_scenarios': 1000,
                'confidence_level': 0.95,
                'optimization_method': 'minimax'
            },
            'sustainability': {
                'sector': 'technology',
                'reporting_framework': 'GRI'
            },
            'thermal': {
                'data_center': {
                    'chiller_cop': 4.5,
                    'ambient_temp_c': 25.0,
                    'safety_margin_c': 5.0
                }
            },
            'synthetic_data': {
                'seed': 42,
                'n_samples_default': 1000,
                'parallel_workers': 4
            },
            'ai_datacenter': {
                'default_capacity_mw': 100,
                'default_pue': 1.3,
                'default_gpu_count': 10000
            },
            'carbon': {
                'price_usd_per_tonne': 75.0,
                'grid_carbon_intensity': 0.5,
                'renewable_energy_pct': 30.0
            },
            'api': {
                'host': '0.0.0.0',
                'port': 8000,
                'rate_limit_per_minute': 60
            },
            'monitoring': {
                'prometheus': {'enabled': True, 'port': 9090},
                'logging': {'level': 'INFO', 'format': 'structured'}
            }
        }
    
    def _resolve_env_vars(self):
        """Resolve ${ENV_VAR} patterns in configuration"""
        import re
        
        def resolve_value(value):
            if isinstance(value, str):
                pattern = r'\$\{([^}]+)\}'
                matches = re.findall(pattern, value)
                for match in matches:
                    env_value = os.environ.get(match, '')
                    value = value.replace(f'${{{match}}}', env_value)
                return value
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(v) for v in value]
            return value
        
        self._config = resolve_value(self._config)
    
    def _validate_config(self):
        """Validate configuration completeness"""
        required_sections = ['system', 'helium', 'quantum', 'blockchain', 
                           'sustainability', 'thermal', 'synthetic_data', 'carbon']
        
        logger = logging.getLogger(__name__)
        missing = [s for s in required_sections if s not in self._config]
        
        if missing:
            logger.warning(f"Missing configuration sections: {missing}")
        
        logger.info(f"Configuration loaded from {'file' if self.config_path else 'defaults'}")
    
    def reload(self):
        """Hot-reload configuration from file"""
        with self._lock:
            old_config = copy.deepcopy(self._config)
            self._config = self._load_config()
            self._resolve_env_vars()
            self._validate_config()
            
            logger = logging.getLogger(__name__)
            logger.info("Configuration reloaded")
            
            return old_config != self._config  # Return True if changed
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value by dot-separated path"""
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    # Enhanced property accessors
    @property
    def helium_config(self) -> Dict:
        return self._config.get('helium', {})
    
    @property
    def quantum_config(self) -> Dict:
        return self._config.get('quantum', {})
    
    @property
    def blockchain_config(self) -> Dict:
        return self._config.get('blockchain', {})
    
    @property
    def regret_config(self) -> Dict:
        return self._config.get('regret_optimizer', {})
    
    @property
    def sustainability_config(self) -> Dict:
        return self._config.get('sustainability', {})
    
    @property
    def thermal_config(self) -> Dict:
        return self._config.get('thermal', {})
    
    @property
    def synthetic_config(self) -> Dict:
        return self._config.get('synthetic_data', {})
    
    @property
    def carbon_config(self) -> Dict:
        return self._config.get('carbon', {})
    
    @property
    def ai_datacenter_config(self) -> Dict:
        return self._config.get('ai_datacenter', {})
    
    @property
    def api_config(self) -> Dict:
        return self._config.get('api', {})
    
    @property
    def monitoring_config(self) -> Dict:
        return self._config.get('monitoring', {})
    
    @property
    def system_config(self) -> Dict:
        return self._config.get('system', {})
    
    def to_dict(self) -> Dict:
        return copy.deepcopy(self._config)

# ============================================================
# BASE METRICS CLASS
# ============================================================

@dataclass
class BaseMetrics:
    """Base class for all metrics objects with serialization"""
    calculation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source_module: str = "base"
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str, indent=2)
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.calculation_id}, source={self.source_module})"

# ============================================================
# BASE CALCULATOR CLASS
# ============================================================

class BaseCalculator(ABC):
    """Abstract base class for all calculators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.calculation_history: List[BaseMetrics] = []
        self.cache = {}
        
        # Use shared Prometheus registry
        self.calculations_counter = Counter(
            f'{self.__class__.__name__.lower()}_calculations_total',
            'Total calculations',
            ['status'],
            registry=get_shared_registry()
        )
        
        self.calculation_duration = Histogram(
            f'{self.__class__.__name__.lower()}_duration_seconds',
            'Calculation duration',
            registry=get_shared_registry()
        )
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def calculate(self, *args, **kwargs) -> BaseMetrics:
        """Calculate metrics"""
        pass
    
    def validate_input(self, data: Any) -> bool:
        """Validate input data"""
        return True
    
    def get_history(self, limit: int = 10) -> List[BaseMetrics]:
        """Get calculation history"""
        return self.calculation_history[-limit:]
    
    def clear_cache(self):
        """Clear calculation cache"""
        self.cache.clear()
    
    def get_statistics(self) -> Dict:
        """Get calculator statistics"""
        return {
            'total_calculations': len(self.calculation_history),
            'cache_size': len(self.cache),
            'class_name': self.__class__.__name__
        }

# ============================================================
# BASE COLLECTOR CLASS (NEW)
# ============================================================

class BaseCollector(ABC):
    """Abstract base class for data collectors"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.collection_history: List[Dict] = []
        self.last_collection_time: Optional[datetime] = None
        self.cache = {}
        self.cache_ttl = 3600
        self._lock = threading.RLock()
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def collect(self, *args, **kwargs) -> Any:
        """Collect data"""
        pass
    
    @abstractmethod
    def get_latest(self) -> Any:
        """Get latest collected data"""
        pass
    
    def get_collection_status(self) -> Dict:
        """Get collection status"""
        return {
            'last_collection': self.last_collection_time.isoformat() if self.last_collection_time else None,
            'total_collections': len(self.collection_history),
            'cache_size': len(self.cache)
        }
    
    def is_data_fresh(self, max_age_seconds: float = 3600) -> bool:
        """Check if data is fresh"""
        if self.last_collection_time is None:
            return False
        return (datetime.now() - self.last_collection_time).total_seconds() < max_age_seconds
    
    def clear_cache(self):
        """Clear collection cache"""
        with self._lock:
            self.cache.clear()

# ============================================================
# BASE GENERATOR CLASS
# ============================================================

class BaseGenerator(ABC):
    """Abstract base class for data generators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.generation_history = []
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def generate(self, *args, **kwargs) -> Any:
        """Generate data"""
        pass
    
    @abstractmethod
    def get_domain_name(self) -> str:
        """Get domain name"""
        pass
    
    def validate_output(self, data: Any) -> float:
        """Validate generated data quality (0-1)"""
        return 1.0
    
    def get_statistics(self) -> Dict:
        """Get generator statistics"""
        return {
            'total_generations': len(self.generation_history),
            'domain': self.get_domain_name(),
            'class_name': self.__class__.__name__
        }

# ============================================================
# BASE FORECASTER CLASS (NEW)
# ============================================================

class BaseForecaster(ABC):
    """Abstract base class for forecasting modules"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.models = {}
        self.forecast_history: List[Dict] = []
        self.models_trained = False
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def train(self, historical_data: Any, **kwargs) -> Dict:
        """Train forecasting model"""
        pass
    
    @abstractmethod
    def forecast(self, recent_data: Any, horizon: int) -> Dict:
        """Generate forecast"""
        pass
    
    def get_forecast_accuracy(self) -> Dict:
        """Get forecast accuracy metrics"""
        if not self.forecast_history:
            return {'error': 'No forecasts available'}
        
        return {
            'total_forecasts': len(self.forecast_history),
            'last_forecast': self.forecast_history[-1] if self.forecast_history else None,
            'models_trained': self.models_trained,
            'class_name': self.__class__.__name__
        }
    
    def is_model_ready(self) -> bool:
        """Check if model is trained and ready"""
        return self.models_trained

# ============================================================
# BASE OPTIMIZER CLASS
# ============================================================

class BaseOptimizer(ABC):
    """Abstract base class for optimizers"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.optimization_history = []
        self.convergence_history = []
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def optimize(self, *args, **kwargs) -> Dict:
        """Run optimization"""
        pass
    
    @abstractmethod
    def get_optimal_solution(self) -> Dict:
        """Get optimal solution"""
        pass
    
    def get_convergence_metrics(self) -> Dict:
        """Get convergence metrics"""
        return {
            'iterations': len(self.convergence_history),
            'converged': len(self.convergence_history) > 0 and 
                        self.convergence_history[-1].get('converged', False),
            'class_name': self.__class__.__name__
        }
    
    def get_statistics(self) -> Dict:
        """Get optimizer statistics"""
        return {
            'total_optimizations': len(self.optimization_history),
            **self.get_convergence_metrics()
        }

# ============================================================
# BASE INTEGRATOR CLASS
# ============================================================

class BaseIntegrator(ABC):
    """Abstract base class for module integrators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.integration_registry = {}
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def integrate(self, source_data: Dict, target_module: str) -> Dict:
        """Integrate data with target module"""
        pass
    
    def register_integration(self, module_name: str, integration_fn: Callable):
        """Register integration function"""
        self.integration_registry[module_name] = integration_fn
    
    def get_integration_status(self) -> Dict:
        """Get integration status"""
        return {
            'registered_modules': list(self.integration_registry.keys()),
            'total_integrations': len(self.integration_registry),
            'class_name': self.__class__.__name__
        }

# ============================================================
# BASE VALIDATOR CLASS
# ============================================================

class BaseValidator(ABC):
    """Abstract base class for data validators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.validation_history = []
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def validate(self, data: Any) -> Tuple[bool, List[str]]:
        """Validate data, returns (is_valid, list_of_issues)"""
        pass
    
    def get_validation_score(self) -> float:
        """Get overall validation score"""
        if not self.validation_history:
            return 0.0
        
        passed = sum(1 for v in self.validation_history if v['passed'])
        return passed / len(self.validation_history)
    
    def get_statistics(self) -> Dict:
        """Get validator statistics"""
        return {
            'total_validations': len(self.validation_history),
            'validation_score': self.get_validation_score(),
            'class_name': self.__class__.__name__
        }

# ============================================================
# BASE VERIFIER CLASS (NEW)
# ============================================================

class BaseVerifier(ABC):
    """Abstract base class for verification modules"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.verification_history: List[Dict] = []
        self._lock = threading.RLock()
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def verify(self, claims: Dict) -> Dict:
        """Verify claims, returns verification result"""
        pass
    
    def get_verification_rate(self) -> float:
        """Get verification success rate"""
        if not self.verification_history:
            return 0.0
        passed = sum(1 for v in self.verification_history if v.get('verified', False))
        return passed / len(self.verification_history)
    
    def get_statistics(self) -> Dict:
        """Get verifier statistics"""
        return {
            'total_verifications': len(self.verification_history),
            'verification_rate': self.get_verification_rate(),
            'class_name': self.__class__.__name__
        }

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def load_module_config(module_name: str) -> Dict:
    """Load configuration for a specific module"""
    config = GreenAgentConfig()
    
    config_map = {
        'helium': config.helium_config,
        'quantum': config.quantum_config,
        'blockchain': config.blockchain_config,
        'regret_optimizer': config.regret_config,
        'sustainability': config.sustainability_config,
        'thermal': config.thermal_config,
        'synthetic_data': config.synthetic_config,
        'ai_datacenter': config.ai_datacenter_config,
        'carbon': config.carbon_config,
        'api': config.api_config,
        'monitoring': config.monitoring_config,
        'system': config.system_config
    }
    
    return config_map.get(module_name, {})

def get_system_config() -> Dict:
    """Get system configuration"""
    return GreenAgentConfig().system_config

def get_api_config() -> Dict:
    """Get API configuration"""
    return GreenAgentConfig().api_config

def get_monitoring_config() -> Dict:
    """Get monitoring configuration"""
    return GreenAgentConfig().monitoring_config

def reload_all_config() -> bool:
    """Hot-reload configuration"""
    return GreenAgentConfig().reload()

# ============================================================
# MODULE REGISTRY (THREAD-SAFE)
# ============================================================

class ModuleRegistry:
    """Thread-safe registry for all Green Agent modules"""
    
    _modules = {}
    _lock = threading.RLock()
    
    @classmethod
    def register(cls, module_name: str, module_instance: Any):
        """Register a module (thread-safe)"""
        with cls._lock:
            cls._modules[module_name] = module_instance
            logging.getLogger(__name__).info(f"Module registered: {module_name}")
    
    @classmethod
    def get(cls, module_name: str) -> Any:
        """Get a registered module (thread-safe)"""
        with cls._lock:
            return cls._modules.get(module_name)
    
    @classmethod
    def list_modules(cls) -> List[str]:
        """List all registered modules"""
        with cls._lock:
            return list(cls._modules.keys())
    
    @classmethod
    def get_status(cls) -> Dict:
        """Get status of all modules"""
        with cls._lock:
            return {
                name: {
                    'type': type(instance).__name__,
                    'available': True
                }
                for name, instance in cls._modules.items()
            }
    
    @classmethod
    def unregister(cls, module_name: str):
        """Remove a module from registry"""
        with cls._lock:
            cls._modules.pop(module_name, None)
            logging.getLogger(__name__).info(f"Module unregistered: {module_name}")
    
    @classmethod
    def clear(cls):
        """Clear all registered modules"""
        with cls._lock:
            cls._modules.clear()
            logging.getLogger(__name__).info("All modules unregistered")

# Add to src/enhancements/base_classes.py (GPU-aware base class)

class GPUBaseCalculator(BaseCalculator):
    """Base calculator with GPU acceleration support"""
    
    def __init__(self, config=None):
        super().__init__(config)
        self._gpu_accelerator = None
        self._init_gpu()
    
    def _init_gpu(self):
        """Initialize GPU acceleration"""
        try:
            from .gpu_acceleration import get_gpu_accelerator
            self._gpu_accelerator = get_gpu_accelerator()
            if self._gpu_accelerator.cuda_available:
                logger.info(f"{self.__class__.__name__} GPU-ready: "
                           f"{self._gpu_accelerator.device_name}")
        except ImportError:
            self._gpu_accelerator = None
    
    def to_gpu(self, data):
        """Move data to GPU if available"""
        if self._gpu_accelerator:
            return self._gpu_accelerator.to_gpu(data)
        return data
    
    def to_cpu(self, data):
        """Move data back to CPU"""
        if self._gpu_accelerator:
            return self._gpu_accelerator.to_cpu(data)
        return data
    
    def get_gpu_memory_info(self):
        """Get GPU memory information"""
        if self._gpu_accelerator:
            return self._gpu_accelerator.get_memory_info()
        return {'cuda_available': False}

# ============================================================
# CONVENIENCE IMPORTS
# ============================================================

__all__ = [
    # Configuration
    'GreenAgentConfig',
    'load_module_config',
    'get_system_config',
    'get_api_config',
    'get_monitoring_config',
    'reload_all_config',
    
    # Base Classes
    'BaseMetrics',
    'BaseCalculator',
    'BaseCollector',
    'BaseGenerator',
    'BaseForecaster',
    'BaseOptimizer',
    'BaseIntegrator',
    'BaseValidator',
    'BaseVerifier',
    
    # Registry
    'ModuleRegistry',
    
    # Utilities
    'get_shared_registry',
]
