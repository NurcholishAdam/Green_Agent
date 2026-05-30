# File: src/enhancements/base_classes.py

"""
Green Agent Base Classes - Version 6.1

Provides shared base classes for all enhancement modules.
Resolves inheritance issues across the project.
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
from datetime import datetime
from pathlib import Path
from collections import defaultdict, deque
from functools import lru_cache
import warnings

# Production dependencies
from pydantic import BaseModel, Field, validator, root_validator
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# ============================================================
# CONFIGURATION LOADER
# ============================================================

class GreenAgentConfig:
    """
    Unified configuration loader for all Green Agent modules.
    Loads from green_agent_config.yaml with environment variable substitution.
    """
    
    _instance = None
    _config = None
    
    def __new__(cls, config_path: str = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: str = None):
        if self._config is None:
            self.config_path = config_path or self._find_config()
            self._config = self._load_config()
            self._resolve_env_vars()
    
    def _find_config(self) -> str:
        """Find configuration file"""
        search_paths = [
            Path(__file__).parent / "green_agent_config.yaml",
            Path.cwd() / "green_agent_config.yaml",
            Path(os.environ.get("GREEN_AGENT_CONFIG", ""))
        ]
        
        for path in search_paths:
            if path.exists():
                return str(path)
        
        raise FileNotFoundError("green_agent_config.yaml not found")
    
    def _load_config(self) -> Dict:
        """Load YAML configuration"""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
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
    
    def to_dict(self) -> Dict:
        return self._config.copy()

# ============================================================
# BASE METRICS CLASS
# ============================================================

@dataclass
class BaseMetrics:
    """Base class for all metrics objects"""
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

# ============================================================
# BASE CALCULATOR CLASS
# ============================================================

class BaseCalculator(ABC):
    """Abstract base class for all calculators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.calculation_history: List[BaseMetrics] = []
        self.cache = {}
        
        # Prometheus metrics
        self.calculations_counter = Counter(
            f'{self.__class__.__name__.lower()}_calculations_total',
            'Total calculations',
            ['status'],
            registry=CollectorRegistry()
        )
        
        self.calculation_duration = Histogram(
            f'{self.__class__.__name__.lower()}_duration_seconds',
            'Calculation duration',
            registry=CollectorRegistry()
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
        """Validate generated data quality"""
        return 1.0

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
                        self.convergence_history[-1].get('converged', False)
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
            'total_integrations': len(self.integration_registry)
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
        """Validate data"""
        pass
    
    def get_validation_score(self) -> float:
        """Get overall validation score"""
        if not self.validation_history:
            return 0.0
        
        passed = sum(1 for v in self.validation_history if v['passed'])
        return passed / len(self.validation_history)

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
        'carbon': config.carbon_config
    }
    
    return config_map.get(module_name, {})

def get_system_config() -> Dict:
    """Get system configuration"""
    config = GreenAgentConfig()
    return config.get('system', {})

def get_api_config() -> Dict:
    """Get API configuration"""
    config = GreenAgentConfig()
    return config.get('api', {})

def get_monitoring_config() -> Dict:
    """Get monitoring configuration"""
    config = GreenAgentConfig()
    return config.get('monitoring', {})

# ============================================================
# MODULE REGISTRY
# ============================================================

class ModuleRegistry:
    """Registry for all Green Agent modules"""
    
    _modules = {}
    
    @classmethod
    def register(cls, module_name: str, module_instance: Any):
        """Register a module"""
        cls._modules[module_name] = module_instance
        logging.getLogger(__name__).info(f"Module registered: {module_name}")
    
    @classmethod
    def get(cls, module_name: str) -> Any:
        """Get a registered module"""
        return cls._modules.get(module_name)
    
    @classmethod
    def list_modules(cls) -> List[str]:
        """List all registered modules"""
        return list(cls._modules.keys())
    
    @classmethod
    def get_status(cls) -> Dict:
        """Get status of all modules"""
        return {
            name: {
                'type': type(instance).__name__,
                'available': True
            }
            for name, instance in cls._modules.items()
        }

# ============================================================
# CONVENIENCE IMPORTS
# ============================================================

__all__ = [
    'GreenAgentConfig',
    'BaseMetrics',
    'BaseCalculator',
    'BaseGenerator',
    'BaseOptimizer',
    'BaseIntegrator',
    'BaseValidator',
    'ModuleRegistry',
    'load_module_config',
    'get_system_config',
    'get_api_config',
    'get_monitoring_config'
]
