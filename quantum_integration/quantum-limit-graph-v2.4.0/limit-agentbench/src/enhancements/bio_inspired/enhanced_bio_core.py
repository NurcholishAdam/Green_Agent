# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/enhanced_bio_core.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Bio-Inspired Core v5.0.0
Complete implementation with graceful shutdown, module registry, lifecycle management,
health dashboard, configuration validation, and module isolation.
"""

import asyncio
import logging
import signal
import time
from typing import Dict, Any, List, Optional, Tuple, Protocol, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import json
import os

logger = logging.getLogger(__name__)

# ============================================================================
# Service Protocols
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any, tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class GradientServiceProtocol(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def get_dominant_field(self) -> Tuple[str, float]: ...
    def get_field_stats(self) -> Dict[str, Any]: ...
    def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    def get_forecast_summary(self) -> Dict[str, Any]: ...

class CompartmentServiceProtocol(Protocol):
    def find_best_compartment(self, expert_type: str, task_complexity: float) -> Any: ...
    def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    def create_compartment(self, expert_type: str, expert_instance: Any, resources: Any, parent_id: str) -> Any: ...
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

class BiomassServiceProtocol(Protocol):
    def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float, guarantee: Any, deadline: Any, initial_tier: Any) -> Tuple[bool, Optional[str]]: ...
    def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]: ...
    def get_storage_stats(self) -> Dict[str, Any]: ...
    def simulate_storage_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]: ...

# ============================================================================
# Lifecycle Management
# ============================================================================

class LifecyclePhase(Enum):
    """Module lifecycle phases"""
    UNREGISTERED = "unregistered"
    REGISTERED = "registered"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    HEALTH_CHECKING = "health_checking"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

@dataclass
class ModuleEntry:
    """Module registry entry with lifecycle management"""
    name: str
    module: Any = None
    phase: LifecyclePhase = LifecyclePhase.REGISTERED
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    health_check: Optional[Callable] = None
    init_timeout: float = 30.0
    shutdown_timeout: float = 10.0
    init_started: Optional[datetime] = None
    init_completed: Optional[datetime] = None
    error_message: Optional[str] = None
    health_status: str = "unknown"
    circuit_breaker_state: str = "closed"
    failure_count: int = 0
    last_failure: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# Module Registry
# ============================================================================

class ModuleRegistry:
    """
    Dynamic module registry with lifecycle management, health checking,
    and circuit breaker protection.
    """
    
    def __init__(self):
        self.modules: Dict[str, ModuleEntry] = {}
        self.startup_order: List[str] = []
        self.shutdown_order: List[str] = []
        self._initialized = False
        self._init_lock = asyncio.Lock()
        
        logger.info("Module Registry initialized")
    
    def register(self, name: str, module: Any = None, dependencies: List[str] = None,
                health_check: Callable = None, init_timeout: float = 30.0,
                shutdown_timeout: float = 10.0) -> 'ModuleEntry':
        """Register a module with the registry"""
        if name in self.modules:
            logger.warning(f"Module {name} already registered, updating")
        
        entry = ModuleEntry(
            name=name,
            module=module,
            dependencies=dependencies or [],
            health_check=health_check,
            init_timeout=init_timeout,
            shutdown_timeout=shutdown_timeout
        )
        
        self.modules[name] = entry
        
        # Update dependency graph
        for dep in entry.dependencies:
            if dep in self.modules:
                self.modules[dep].dependents.append(name)
        
        logger.info(f"Module registered: {name} (deps: {entry.dependencies})")
        return entry
    
    def unregister(self, name: str) -> bool:
        """Unregister a module"""
        if name in self.modules:
            # Check if other modules depend on this
            if self.modules[name].dependents:
                logger.warning(f"Cannot unregister {name}: depended on by {self.modules[name].dependents}")
                return False
            
            del self.modules[name]
            
            # Remove from dependency lists
            for module in self.modules.values():
                if name in module.dependencies:
                    module.dependencies.remove(name)
            
            logger.info(f"Module unregistered: {name}")
            return True
        return False
    
    def get(self, name: str) -> Optional[Any]:
        """Get module instance by name"""
        entry = self.modules.get(name)
        return entry.module if entry else None
    
    def get_entry(self, name: str) -> Optional[ModuleEntry]:
        """Get module entry by name"""
        return self.modules.get(name)
    
    def list_modules(self) -> List[str]:
        """List all registered modules"""
        return list(self.modules.keys())
    
    def get_dependency_graph(self) -> Dict[str, List[str]]:
        """Get module dependency graph"""
        return {
            name: entry.dependencies
            for name, entry in self.modules.items()
        }
    
    def get_startup_order(self) -> List[str]:
        """Calculate topological startup order"""
        visited = set()
        order = []
        
        def visit(name):
            if name in visited:
                return
            visited.add(name)
            entry = self.modules.get(name)
            if entry:
                for dep in entry.dependencies:
                    if dep in self.modules:
                        visit(dep)
            order.append(name)
        
        for name in self.modules:
            visit(name)
        
        self.startup_order = order
        return order
    
    def get_shutdown_order(self) -> List[str]:
        """Calculate reverse topological shutdown order"""
        startup = self.get_startup_order()
        self.shutdown_order = list(reversed(startup))
        return self.shutdown_order
    
    async def initialize_all(self, parallel: bool = False) -> Dict[str, bool]:
        """Initialize all modules in dependency order"""
        async with self._init_lock:
            if self._initialized:
                logger.warning("Modules already initialized")
                return {}
            
            order = self.get_startup_order()
            results = {}
            
            for name in order:
                entry = self.modules[name]
                
                if entry.phase == LifecyclePhase.INITIALIZED:
                    results[name] = True
                    continue
                
                try:
                    entry.phase = LifecyclePhase.INITIALIZING
                    entry.init_started = datetime.utcnow()
                    
                    # Initialize with timeout
                    if hasattr(entry.module, 'initialize'):
                        await asyncio.wait_for(
                            entry.module.initialize(),
                            timeout=entry.init_timeout
                        )
                    
                    entry.phase = LifecyclePhase.INITIALIZED
                    entry.init_completed = datetime.utcnow()
                    
                    # Run health check
                    if entry.health_check:
                        try:
                            is_healthy = entry.health_check()
                            entry.health_status = "healthy" if is_healthy else "degraded"
                        except Exception:
                            entry.health_status = "unknown"
                    
                    results[name] = True
                    logger.info(f"Module {name} initialized successfully")
                    
                except asyncio.TimeoutError:
                    entry.phase = LifecyclePhase.ERROR
                    entry.error_message = f"Initialization timeout ({entry.init_timeout}s)"
                    results[name] = False
                    logger.error(f"Module {name} initialization timed out")
                    
                except Exception as e:
                    entry.phase = LifecyclePhase.ERROR
                    entry.error_message = str(e)
                    results[name] = False
                    logger.error(f"Module {name} initialization failed: {str(e)}")
            
            # Verify all critical modules initialized
            all_ok = all(results.values())
            if all_ok:
                self._initialized = True
                logger.info("All modules initialized successfully")
            else:
                failed = [name for name, ok in results.items() if not ok]
                logger.warning(f"Some modules failed to initialize: {failed}")
            
            return results
    
    async def shutdown_all(self) -> Dict[str, bool]:
        """Shutdown all modules in reverse dependency order"""
        order = self.get_shutdown_order()
        results = {}
        
        for name in order:
            entry = self.modules[name]
            
            if entry.phase == LifecyclePhase.STOPPED:
                results[name] = True
                continue
            
            try:
                entry.phase = LifecyclePhase.STOPPING
                
                # Shutdown with timeout
                if hasattr(entry.module, 'shutdown'):
                    await asyncio.wait_for(
                        entry.module.shutdown(),
                        timeout=entry.shutdown_timeout
                    )
                
                entry.phase = LifecyclePhase.STOPPED
                results[name] = True
                logger.info(f"Module {name} shutdown successfully")
                
            except asyncio.TimeoutError:
                entry.phase = LifecyclePhase.ERROR
                results[name] = False
                logger.error(f"Module {name} shutdown timed out")
                
            except Exception as e:
                entry.phase = LifecyclePhase.ERROR
                results[name] = False
                logger.error(f"Module {name} shutdown failed: {str(e)}")
        
        self._initialized = False
        return results
    
    def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """Run health checks on all modules"""
        results = {}
        
        for name, entry in self.modules.items():
            if entry.health_check:
                try:
                    is_healthy = entry.health_check()
                    entry.health_status = "healthy" if is_healthy else "degraded"
                except Exception as e:
                    entry.health_status = "error"
                    entry.error_message = str(e)
            else:
                entry.health_status = "unknown"
            
            results[name] = {
                'status': entry.health_status,
                'phase': entry.phase.value,
                'error': entry.error_message,
                'circuit_breaker': entry.circuit_breaker_state,
                'uptime': (datetime.utcnow() - entry.init_completed).total_seconds() if entry.init_completed else 0
            }
        
        return results
    
    def record_failure(self, name: str):
        """Record a module failure for circuit breaker"""
        entry = self.modules.get(name)
        if not entry:
            return
        
        entry.failure_count += 1
        entry.last_failure = datetime.utcnow()
        
        if entry.failure_count >= 5 and entry.circuit_breaker_state == "closed":
            entry.circuit_breaker_state = "open"
            logger.warning(f"Circuit breaker OPEN for module {name} ({entry.failure_count} failures)")
    
    def record_success(self, name: str):
        """Record a module success for circuit breaker"""
        entry = self.modules.get(name)
        if not entry:
            return
        
        if entry.circuit_breaker_state == "half_open":
            entry.circuit_breaker_state = "closed"
            entry.failure_count = 0
            logger.info(f"Circuit breaker CLOSED for module {name}")
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        return {
            'total_modules': len(self.modules),
            'initialized': sum(1 for m in self.modules.values() if m.phase == LifecyclePhase.INITIALIZED),
            'running': sum(1 for m in self.modules.values() if m.phase == LifecyclePhase.RUNNING),
            'error': sum(1 for m in self.modules.values() if m.phase == LifecyclePhase.ERROR),
            'circuit_breakers_open': sum(1 for m in self.modules.values() if m.circuit_breaker_state == "open"),
            'modules': {
                name: {
                    'phase': entry.phase.value,
                    'health': entry.health_status,
                    'circuit_breaker': entry.circuit_breaker_state,
                    'dependencies': entry.dependencies,
                    'dependents': entry.dependents
                }
                for name, entry in self.modules.items()
            }
        }

# ============================================================================
# Configuration Manager
# ============================================================================

@dataclass
class CoreConfig:
    """Core configuration with validation"""
    # Token economy
    token_base_generation_rate: float = 150.0
    token_hoarding_threshold: float = 2.0
    token_emergency_threshold: float = 50.0
    token_target_utilization: float = 0.75
    
    # Compartments
    compartments_per_expert_type: int = 2
    max_total_compartments: int = 100
    compartment_health_threshold: float = 0.2
    
    # Gradient fields
    carbon_leakage_rate: float = 0.03
    helium_leakage_rate: float = 0.08
    trust_leakage_rate: float = 0.10
    
    # ATP Synthase
    atp_c_ring_size: int = 12
    atp_max_rotation_speed: float = 6000
    enable_multi_synthase: bool = True
    
    # Expert types
    enable_quantum_expert: bool = False
    enable_helium_expert: bool = False
    
    # Features
    enable_degradation_manager: bool = True
    enable_predictive_homeostasis: bool = True
    enable_knowledge_transfer: bool = True
    enable_supply_management: bool = True
    enable_token_preallocation: bool = True
    enable_chaos_engineering: bool = False
    
    # State persistence
    enable_state_persistence: bool = True
    state_save_interval_seconds: int = 300
    state_directory: str = "./agent_state"
    
    # Health checks
    health_check_interval_seconds: int = 30
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate configuration and return (is_valid, issues)"""
        issues = []
        
        if self.token_base_generation_rate <= 0:
            issues.append("token_base_generation_rate must be positive")
        if self.token_hoarding_threshold < 1.0:
            issues.append("token_hoarding_threshold should be at least 1.0")
        if self.compartments_per_expert_type < 1:
            issues.append("compartments_per_expert_type must be at least 1")
        if self.carbon_leakage_rate <= 0:
            issues.append("carbon_leakage_rate must be positive")
        if self.atp_c_ring_size < 8 or self.atp_c_ring_size > 17:
            issues.append("atp_c_ring_size should be between 8 and 17")
        if self.state_save_interval_seconds < 60:
            issues.append("state_save_interval_seconds should be at least 60")
        
        return len(issues) == 0, issues
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'token_base_generation_rate': self.token_base_generation_rate,
            'token_hoarding_threshold': self.token_hoarding_threshold,
            'token_emergency_threshold': self.token_emergency_threshold,
            'token_target_utilization': self.token_target_utilization,
            'compartments_per_expert_type': self.compartments_per_expert_type,
            'max_total_compartments': self.max_total_compartments,
            'compartment_health_threshold': self.compartment_health_threshold,
            'carbon_leakage_rate': self.carbon_leakage_rate,
            'helium_leakage_rate': self.helium_leakage_rate,
            'trust_leakage_rate': self.trust_leakage_rate,
            'atp_c_ring_size': self.atp_c_ring_size,
            'atp_max_rotation_speed': self.atp_max_rotation_speed,
            'enable_multi_synthase': self.enable_multi_synthase,
            'enable_quantum_expert': self.enable_quantum_expert,
            'enable_helium_expert': self.enable_helium_expert,
            'enable_degradation_manager': self.enable_degradation_manager,
            'enable_predictive_homeostasis': self.enable_predictive_homeostasis,
            'enable_knowledge_transfer': self.enable_knowledge_transfer,
            'enable_supply_management': self.enable_supply_management,
            'enable_token_preallocation': self.enable_token_preallocation,
            'enable_chaos_engineering': self.enable_chaos_engineering,
            'enable_state_persistence': self.enable_state_persistence,
            'state_save_interval_seconds': self.state_save_interval_seconds,
            'state_directory': self.state_directory,
            'health_check_interval_seconds': self.health_check_interval_seconds
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CoreConfig':
        """Create from dictionary"""
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)
    
    @classmethod
    def from_file(cls, path: str) -> 'CoreConfig':
        """Load configuration from JSON file"""
        with open(path, 'r') as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    def save_to_file(self, path: str):
        """Save configuration to JSON file"""
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

# ============================================================================
# Enhanced Bio-Inspired Core
# ============================================================================

class EnhancedBioInspiredCore:
    """
    Enhanced Bio-Inspired Core v5.0.0
    
    Complete implementation with:
    - Graceful shutdown and lifecycle management
    - Module registry with dependency management
    - Health dashboard and performance monitoring
    - Configuration validation and hot-reload
    - Module isolation with circuit breakers
    - Protocol-based dependency injection
    """
    
    def __init__(self, config: Optional[CoreConfig] = None, config_path: Optional[str] = None):
        # Load configuration
        if config_path:
            self.config = CoreConfig.from_file(config_path)
        else:
            self.config = config or CoreConfig()
        
        # Validate configuration
        is_valid, issues = self.config.validate()
        if not is_valid:
            logger.warning(f"Configuration issues: {issues}")
        
        # Module registry
        self.registry = ModuleRegistry()
        
        # Module references (populated during init)
        self._token_manager = None
        self._gradient_manager = None
        self._scheduler = None
        self._compartment_manager = None
        self._biomass_storage = None
        self._harvester = None
        self._supply_manager = None
        self._token_allocator = None
        self._knowledge_transfer = None
        self._degradation_manager = None
        self._api = None
        self._event_bus = None
        
        # Exchange rate
        self.exchange_rate = None
        
        # Lifecycle state
        self._lifecycle_phase = LifecyclePhase.UNREGISTERED
        self._start_time: Optional[datetime] = None
        self._shutdown_requested = False
        
        # Performance metrics
        self._perf_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info("Enhanced Bio-Inspired Core v5.0.0 created")
    
    # ========================================================================
    # Lifecycle Management
    # ========================================================================
    
    async def initialize(self) -> bool:
        """Initialize all modules with health verification"""
        if self._lifecycle_phase == LifecyclePhase.RUNNING:
            logger.warning("Core already initialized")
            return True
        
        self._lifecycle_phase = LifecyclePhase.INITIALIZING
        self._start_time = datetime.utcnow()
        
        try:
            # Step 1: Validate configuration
            is_valid, issues = self.config.validate()
            if not is_valid:
                logger.error(f"Configuration invalid: {issues}")
                self._lifecycle_phase = LifecyclePhase.ERROR
                return False
            
            # Step 2: Initialize exchange rate
            from .eco_atp_currency import DynamicExchangeRate
            self.exchange_rate = DynamicExchangeRate()
            self.registry.register('exchange_rate', self.exchange_rate, 
                                  health_check=lambda: True)
            
            # Step 3: Initialize token manager
            from .eco_atp_currency import EcoATPTokenManager, TokenSupplyManager, PredictiveTokenAllocator
            self._token_manager = EcoATPTokenManager(self.exchange_rate)
            self.registry.register('token_manager', self._token_manager,
                                  dependencies=['exchange_rate'],
                                  health_check=lambda: self._token_manager.get_system_summary().get('total_balance', 0) > 0)
            
            # Step 4: Initialize gradient manager
            from .proton_gradient_fields import HierarchicalGradientManager
            self._gradient_manager = HierarchicalGradientManager()
            self.registry.register('gradient_manager', self._gradient_manager,
                                  health_check=lambda: len(self._gradient_manager.get_field_strengths()) > 0)
            
            # Step 5: Initialize ATP synthase scheduler
            from .atp_synthase_scheduler import ATPSynthaseScheduler, SynthaseConfig
            synthase_config = SynthaseConfig(
                protons_per_rotation=self.config.atp_c_ring_size,
                max_rotation_speed_rpm=self.config.atp_max_rotation_speed
            )
            self._scheduler = ATPSynthaseScheduler(
                self._token_manager, self._gradient_manager, synthase_config,
                enable_multi_synthase=self.config.enable_multi_synthase
            )
            self.registry.register('atp_synthase', self._scheduler,
                                  dependencies=['token_manager', 'gradient_manager'],
                                  health_check=lambda: self._scheduler.calculate_gradient_driving_force() >= 0)
            
            # Step 6: Initialize compartment manager
            from .chromatophore_compartments import HierarchicalCompartmentManager
            self._compartment_manager = HierarchicalCompartmentManager(
                self._token_manager,
                max_regions=10,
                compartments_per_region=20
            )
            self.registry.register('compartment_manager', self._compartment_manager,
                                  dependencies=['token_manager'],
                                  health_check=lambda: self._compartment_manager.get_ecosystem_stats().get('viable_compartments', 0) > 0)
            
            # Step 7: Initialize biomass storage
            from .biomass_storage import BiomassStorage
            self._biomass_storage = BiomassStorage(self._token_manager, self._gradient_manager)
            self.registry.register('biomass_storage', self._biomass_storage,
                                  dependencies=['token_manager', 'gradient_manager'],
                                  health_check=lambda: self._biomass_storage.get_storage_stats().get('total_stored', -1) >= 0)
            
            # Step 8: Initialize harvester
            from .photosynthetic_harvester import EnhancedPhotosyntheticHarvester
            self._harvester = EnhancedPhotosyntheticHarvester(
                self._token_manager, self._gradient_manager
            )
            self.registry.register('harvester', self._harvester,
                                  dependencies=['token_manager', 'gradient_manager'],
                                  health_check=lambda: self._harvester.get_harvesting_stats().get('total_harvested', -1) >= 0)
            
            # Wire harvester to scheduler
            if self._scheduler:
                self._scheduler.inject_harvester(self._harvester)
            
            # Step 9: Initialize supply management
            if self.config.enable_supply_management:
                self._supply_manager = TokenSupplyManager(self._token_manager)
                self.registry.register('supply_manager', self._supply_manager,
                                      dependencies=['token_manager'])
            
            # Step 10: Initialize token pre-allocation
            if self.config.enable_token_preallocation:
                self._token_allocator = PredictiveTokenAllocator(self._token_manager)
                self.registry.register('token_allocator', self._token_allocator,
                                      dependencies=['token_manager'])
            
            # Step 11: Initialize knowledge transfer
            if self.config.enable_knowledge_transfer:
                try:
                    from .knowledge_transfer import KnowledgeTransferManager
                    self._knowledge_transfer = KnowledgeTransferManager()
                    self.registry.register('knowledge_transfer', self._knowledge_transfer)
                except ImportError:
                    logger.warning("Knowledge transfer not available")
            
            # Step 12: Initialize degradation manager
            if self.config.enable_degradation_manager:
                try:
                    from .degradation_manager import DegradationManager
                    self._degradation_manager = DegradationManager(event_bus=self._event_bus)
                    self.registry.register('degradation_manager', self._degradation_manager)
                    
                    # Wire initial metrics
                    self._degradation_manager.update_metrics(
                        token_balance=self._token_manager.get_system_summary().get('total_balance', 500)
                    )
                except ImportError:
                    logger.warning("Degradation manager not available")
            
            # Step 13: Run health checks on all modules
            health_results = self.registry.health_check_all()
            unhealthy = [name for name, status in health_results.items() if status['status'] not in ('healthy', 'unknown')]
            
            if unhealthy:
                logger.warning(f"Some modules unhealthy after init: {unhealthy}")
            
            # Step 14: Start background monitoring
            asyncio.create_task(self._health_monitoring_loop())
            asyncio.create_task(self._performance_monitoring_loop())
            
            self._lifecycle_phase = LifecyclePhase.RUNNING
            
            init_time = (datetime.utcnow() - self._start_time).total_seconds()
            logger.info(f"Bio-Inspired Core initialized successfully in {init_time:.1f}s")
            logger.info(f"Registered modules: {self.registry.list_modules()}")
            
            return True
            
        except Exception as e:
            self._lifecycle_phase = LifecyclePhase.ERROR
            logger.error(f"Initialization failed: {str(e)}", exc_info=True)
            return False
    
    async def shutdown(self) -> bool:
        """Graceful shutdown of all modules"""
        if self._lifecycle_phase == LifecyclePhase.STOPPED:
            return True
        
        self._lifecycle_phase = LifecyclePhase.STOPPING
        self._shutdown_requested = True
        logger.info("Initiating graceful shutdown...")
        
        # Save state if enabled
        if self.config.enable_state_persistence:
            self._save_state()
        
        # Shutdown all modules in reverse order
        results = await self.registry.shutdown_all()
        
        all_ok = all(results.values())
        if all_ok:
            self._lifecycle_phase = LifecyclePhase.STOPPED
            logger.info("Graceful shutdown complete")
        else:
            failed = [name for name, ok in results.items() if not ok]
            logger.warning(f"Some modules failed to shutdown: {failed}")
        
        return all_ok
    
    def _save_state(self):
        """Save system state for recovery"""
        try:
            state_dir = self.config.state_directory
            os.makedirs(state_dir, exist_ok=True)
            
            state = {
                'timestamp': datetime.utcnow().isoformat(),
                'config': self.config.to_dict(),
                'token_summary': self._token_manager.get_system_summary() if self._token_manager else {},
                'gradient_strengths': self._gradient_manager.get_field_strengths() if self._gradient_manager else {},
                'compartment_stats': self._compartment_manager.get_ecosystem_stats() if self._compartment_manager else {},
                'biomass_stats': self._biomass_storage.get_storage_stats() if self._biomass_storage else {}
            }
            
            path = os.path.join(state_dir, f"state_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")
            with open(path, 'w') as f:
                json.dump(state, f, indent=2, default=str)
            
            logger.info(f"State saved to {path}")
            
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}")
    
    def _register_signal_handlers(self):
        """Register OS signal handlers for graceful shutdown"""
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.shutdown())
                )
            logger.info("Signal handlers registered")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")
    
    # ========================================================================
    # Health Monitoring
    # ========================================================================
    
    async def _health_monitoring_loop(self):
        """Periodic health monitoring loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                health_results = self.registry.health_check_all()
                
                # Log unhealthy modules
                unhealthy = [
                    name for name, status in health_results.items()
                    if status['status'] not in ('healthy', 'unknown')
                ]
                
                if unhealthy:
                    logger.warning(f"Unhealthy modules: {unhealthy}")
                
                # Update degradation manager
                if self._degradation_manager and self._token_manager:
                    summary = self._token_manager.get_system_summary()
                    gradients = self._gradient_manager.get_field_strengths() if self._gradient_manager else {}
                    
                    self._degradation_manager.update_metrics(
                        token_balance=summary.get('total_balance', 500),
                        carbon_gradient=gradients.get('carbon', 0.5),
                        compartment_health=self._get_avg_compartment_health()
                    )
                
                await asyncio.sleep(self.config.health_check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Health monitoring error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _performance_monitoring_loop(self):
        """Periodic performance monitoring loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                # Record token metrics
                if self._token_manager:
                    summary = self._token_manager.get_system_summary()
                    self._perf_metrics['token_balance'].append(summary.get('total_balance', 0))
                    self._perf_metrics['token_efficiency'].append(summary.get('system_efficiency', 0))
                
                # Record gradient metrics
                if self._gradient_manager:
                    strengths = self._gradient_manager.get_field_strengths()
                    for field_id, strength in strengths.items():
                        self._perf_metrics[f'gradient_{field_id}'].append(strength)
                
                # Record compartment metrics
                if self._compartment_manager:
                    stats = self._compartment_manager.get_ecosystem_stats()
                    self._perf_metrics['viable_compartments'].append(stats.get('viable_compartments', 0))
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Performance monitoring error: {str(e)}")
                await asyncio.sleep(60)
    
    def _get_avg_compartment_health(self) -> float:
        """Get average compartment health"""
        if not self._compartment_manager:
            return 0.5
        compartments = self._compartment_manager.compartments
        if not compartments:
            return 0.5
        return np.mean([c.health_score for c in compartments.values()])
    
    # ========================================================================
    # Protocol-Compliant Service Accessors
    # ========================================================================
    
    @property
    def token_service(self) -> Optional[TokenServiceProtocol]:
        return self._token_manager
    
    @property
    def gradient_service(self) -> Optional[GradientServiceProtocol]:
        return self._gradient_manager
    
    @property
    def compartment_service(self) -> Optional[CompartmentServiceProtocol]:
        return self._compartment_manager
    
    @property
    def biomass_service(self) -> Optional[BiomassServiceProtocol]:
        return self._biomass_storage
    
    # Legacy accessors (backward compatibility)
    @property
    def token_manager(self): return self._token_manager
    @property
    def gradient_manager(self): return self._gradient_manager
    @property
    def scheduler(self): return self._scheduler
    @property
    def compartment_manager(self): return self._compartment_manager
    @property
    def biomass_storage(self): return self._biomass_storage
    @property
    def harvester(self): return self._harvester
    @property
    def supply_manager(self): return self._supply_manager
    @property
    def token_allocator(self): return self._token_allocator
    @property
    def knowledge_transfer(self): return self._knowledge_transfer
    @property
    def degradation_manager(self): return self._degradation_manager
    
    # ========================================================================
    # System Status and Reporting
    # ========================================================================
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        status = {
            'lifecycle_phase': self._lifecycle_phase.value,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            'timestamp': datetime.utcnow().isoformat(),
            'config': self.config.to_dict(),
            'modules': self.registry.get_registry_stats()
        }
        
        # Module-specific status
        if self._token_manager:
            status['token_economy'] = self._token_manager.get_system_summary()
        
        if self._gradient_manager:
            status['gradients'] = self._gradient_manager.get_field_stats()
            status['gradient_forecasts'] = self._gradient_manager.get_forecast_summary()
        
        if self._compartment_manager:
            status['compartments'] = self._compartment_manager.get_ecosystem_stats()
        
        if self._biomass_storage:
            status['biomass'] = self._biomass_storage.get_storage_stats()
        
        if self._harvester:
            status['harvester'] = self._harvester.get_harvesting_stats()
        
        if self._scheduler:
            status['atp_synthase'] = self._scheduler.get_scheduler_stats()
        
        if self._supply_manager:
            status['token_economy']['supply_management'] = self._supply_manager.get_economic_indicators()
        
        if self._token_allocator:
            status['token_economy']['pre_allocation'] = self._token_allocator.get_cache_stats()
        
        if self._degradation_manager:
            status['degradation'] = self._degradation_manager.get_tier_status()
        
        # Performance metrics
        status['performance'] = {
            name: {
                'current': list(values)[-1] if values else None,
                'avg_1min': np.mean(list(values)[-60:]) if len(values) >= 10 else None,
                'trend': 'stable'
            }
            for name, values in self._perf_metrics.items()
        }
        
        return status
    
    def get_health_dashboard(self) -> Dict[str, Any]:
        """Get health dashboard for all modules"""
        health = self.registry.health_check_all()
        
        # Calculate overall health
        healthy_count = sum(1 for s in health.values() if s['status'] == 'healthy')
        total = len(health)
        
        return {
            'overall_health': 'healthy' if healthy_count == total else 'degraded' if healthy_count > total // 2 else 'unhealthy',
            'healthy_modules': healthy_count,
            'total_modules': total,
            'modules': health,
            'circuit_breakers': {
                name: entry.circuit_breaker_state
                for name, entry in self.registry.modules.items()
            },
            'dependency_graph': self.registry.get_dependency_graph(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_economic_report(self) -> Dict[str, Any]:
        """Get economic health report"""
        report = {
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if self._token_manager:
            report['token_economy'] = self._token_manager.get_system_summary()
        
        if self._supply_manager:
            report['supply_management'] = self._supply_manager.get_economic_indicators()
        
        if self._token_allocator:
            report['pre_allocation'] = self._token_allocator.get_cache_stats()
        
        # Health assessment
        indicators = report.get('supply_management', {})
        utilization = indicators.get('utilization', 0.5)
        inflation = indicators.get('inflation_pressure', 0)
        
        if 0.6 < utilization < 0.9 and abs(inflation) < 0.2:
            report['health'] = 'healthy'
        elif utilization < 0.4:
            report['health'] = 'deflationary'
        elif utilization > 0.95:
            report['health'] = 'inflationary'
        else:
            report['health'] = 'stable'
        
        recs = []
        if utilization < 0.4:
            recs.append("Economy under-utilized. Increase task throughput.")
        if utilization > 0.95:
            recs.append("Economy over-heating. Add capacity or reduce load.")
        if inflation > 0.3:
            recs.append("High inflation pressure. Token burning recommended.")
        report['recommendations'] = recs if recs else ["Economy is healthy."]
        
        return report
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get performance metrics report"""
        report = {'timestamp': datetime.utcnow().isoformat()}
        
        for name, values in self._perf_metrics.items():
            if values:
                arr = np.array(list(values))
                report[name] = {
                    'current': float(arr[-1]) if len(arr) > 0 else None,
                    'mean': float(np.mean(arr[-60:])) if len(arr) >= 10 else None,
                    'min': float(np.min(arr[-60:])) if len(arr) >= 10 else None,
                    'max': float(np.max(arr[-60:])) if len(arr) >= 10 else None,
                    'std': float(np.std(arr[-60:])) if len(arr) >= 10 else None,
                    'trend': 'improving' if len(arr) >= 10 and arr[-1] > np.mean(arr[-10:-5]) else 'stable'
                }
        
        return report
    
    # ========================================================================
    # Configuration Management
    # ========================================================================
    
    def update_configuration(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        """Update configuration with validation"""
        # Create temporary config with updates
        temp_config = CoreConfig.from_dict({**self.config.to_dict(), **updates})
        
        # Validate
        is_valid, issues = temp_config.validate()
        if not is_valid:
            return False, f"Invalid configuration: {'; '.join(issues)}"
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        logger.info(f"Configuration updated: {list(updates.keys())}")
        return True, "Configuration updated successfully"
    
    def reload_configuration(self, path: str) -> Tuple[bool, str]:
        """Reload configuration from file"""
        try:
            new_config = CoreConfig.from_file(path)
            is_valid, issues = new_config.validate()
            
            if not is_valid:
                return False, f"Invalid configuration: {'; '.join(issues)}"
            
            self.config = new_config
            logger.info(f"Configuration reloaded from {path}")
            return True, "Configuration reloaded successfully"
            
        except Exception as e:
            return False, f"Failed to reload configuration: {str(e)}"
    
    # ========================================================================
    # Task Processing
    # ========================================================================
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task through the bio-inspired system"""
        if self._lifecycle_phase != LifecyclePhase.RUNNING:
            return {'success': False, 'reason': f'System not running (phase: {self._lifecycle_phase.value})'}
        
        ecoatp_required = task.get('complexity', 0.5) * 10
        
        # Try token pre-allocation first
        if self._token_allocator:
            success, _ = self._token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self._token_allocator.record_demand('task_processor', ecoatp_required)
        elif self._token_manager:
            success, _ = self._token_manager.reserve_tokens(
                'task_processor', ecoatp_required, None
            )
        else:
            success = True
        
        if not success:
            # Store in biomass
            if self._biomass_storage:
                stored, token_id = self._biomass_storage.store_task(
                    task_data=task, ecoatp_cost=ecoatp_required
                )
                return {'success': True, 'status': 'stored', 'biomass_token': token_id}
            
            return {'success': False, 'reason': 'Insufficient tokens'}
        
        return {'success': True, 'task_id': task.get('task_id', 'unknown'), 'ecoatp_cost': ecoatp_required}
    
    # ========================================================================
    # Lifecycle Status
    # ========================================================================
    
    @property
    def is_running(self) -> bool:
        return self._lifecycle_phase == LifecyclePhase.RUNNING
    
    @property
    def is_healthy(self) -> bool:
        health = self.registry.health_check_all()
        return all(s['status'] != 'error' for s in health.values())
    
    @property
    def lifecycle_phase(self) -> LifecyclePhase:
        return self._lifecycle_phase
    
    def get_lifecycle_status(self) -> Dict[str, Any]:
        """Get lifecycle status"""
        return {
            'phase': self._lifecycle_phase.value,
            'is_running': self.is_running,
            'is_healthy': self.is_healthy,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            'start_time': self._start_time.isoformat() if self._start_time else None,
            'shutdown_requested': self._shutdown_requested,
            'module_count': len(self.registry.modules)
        }

# ============================================================================
# Convenience Functions
# ============================================================================

def create_core(config: Optional[CoreConfig] = None, config_path: Optional[str] = None) -> EnhancedBioInspiredCore:
    """Create an enhanced bio-inspired core"""
    return EnhancedBioInspiredCore(config=config, config_path=config_path)

async def create_and_initialize(config: Optional[CoreConfig] = None) -> EnhancedBioInspiredCore:
    """Create and initialize a bio-inspired core"""
    core = EnhancedBioInspiredCore(config=config)
    success = await core.initialize()
    
    if not success:
        raise RuntimeError("Failed to initialize Bio-Inspired Core")
    
    return core
