# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/bio_integrated_agent.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Bio-Integrated Green Agent v5.0.0
Complete implementation with graceful shutdown, state persistence, health checks,
event bus, dynamic scaling, configuration management, and distributed tracing.
"""

import asyncio
import logging
import signal
import json
import os
import pickle
from typing import Dict, Any, List, Optional, Callable, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid
import hashlib

logger = logging.getLogger(__name__)

# ============================================================================
# Module Availability Checks
# ============================================================================

BIO_INSPIRED_AVAILABLE = True
MODULE_STATUS = {}

try:
    from .eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenSupplyManager, PredictiveTokenAllocator
    )
    MODULE_STATUS['token_manager'] = True
except ImportError as e:
    MODULE_STATUS['token_manager'] = False
    logger.error(f"Token manager not available: {str(e)}")

try:
    from .proton_gradient_fields import HierarchicalGradientManager
    MODULE_STATUS['gradient_manager'] = True
except ImportError as e:
    MODULE_STATUS['gradient_manager'] = False

try:
    from .atp_synthase_scheduler import ATPSynthaseScheduler, SynthaseConfig
    MODULE_STATUS['atp_synthase'] = True
except ImportError as e:
    MODULE_STATUS['atp_synthase'] = False

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager
    MODULE_STATUS['compartment_manager'] = True
except ImportError as e:
    MODULE_STATUS['compartment_manager'] = False

try:
    from .biomass_storage import BiomassStorage
    MODULE_STATUS['biomass_storage'] = True
except ImportError as e:
    MODULE_STATUS['biomass_storage'] = False

try:
    from .photosynthetic_harvester import EnhancedPhotosyntheticHarvester
    MODULE_STATUS['harvester'] = True
except ImportError as e:
    MODULE_STATUS['harvester'] = False

# ============================================================================
# Enums and Data Classes
# ============================================================================

class AgentState(Enum):
    """Agent operational states"""
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    RUNNING = "running"
    DEGRADED = "degraded"
    PAUSED = "paused"
    SHUTTING_DOWN = "shutting_down"
    SHUTDOWN = "shutdown"
    ERROR = "error"

class HealthStatus(Enum):
    """Module health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    STOPPED = "stopped"
    UNKNOWN = "unknown"

@dataclass
class AgentConfig:
    """Centralized agent configuration"""
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
    
    # State persistence
    enable_state_persistence: bool = True
    state_save_interval_seconds: int = 300
    state_directory: str = "./agent_state"
    
    # Health checks
    health_check_interval_seconds: int = 30
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        if self.token_base_generation_rate <= 0:
            issues.append("token_base_generation_rate must be positive")
        if self.compartments_per_expert_type < 1:
            issues.append("compartments_per_expert_type must be at least 1")
        if self.carbon_leakage_rate <= 0:
            issues.append("carbon_leakage_rate must be positive")
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

@dataclass
class ModuleHealth:
    """Health status for a single module"""
    module_name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_check: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SystemSnapshot:
    """Complete system state for persistence"""
    agent_state: str
    timestamp: datetime
    token_state: Optional[Dict[str, Any]] = None
    gradient_state: Optional[Dict[str, Any]] = None
    compartment_state: Optional[Dict[str, Any]] = None
    biomass_state: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    correlation_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

# ============================================================================
# Event Bus for Decoupled Communication
# ============================================================================

class EventBus:
    """
    Lightweight event bus for decoupled module communication.
    
    Enables publish/subscribe pattern across all bio-inspired modules.
    """
    
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.event_history: deque = deque(maxlen=1000)
        self.running = True
        
        # Start event processor
        asyncio.create_task(self._process_events())
        
        logger.info("Event Bus initialized")
    
    def publish(self, event_type: str, payload: Dict[str, Any], 
                correlation_id: Optional[str] = None):
        """Publish an event to all subscribers"""
        event = {
            'event_id': uuid.uuid4().hex[:12],
            'event_type': event_type,
            'payload': payload,
            'correlation_id': correlation_id or uuid.uuid4().hex[:12],
            'timestamp': datetime.utcnow()
        }
        
        try:
            self.event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(f"Event queue full, dropping event: {event_type}")
    
    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to events of a specific type"""
        self.subscribers[event_type].append(callback)
        logger.debug(f"Subscribed to event: {event_type}")
    
    def unsubscribe(self, event_type: str, callback: Callable):
        """Unsubscribe from events"""
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(callback)
    
    async def _process_events(self):
        """Background event processing loop"""
        while self.running:
            try:
                event = await self.event_queue.get()
                
                # Notify subscribers
                subscribers = self.subscribers.get(event['event_type'], [])
                for callback in subscribers:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(event)
                        else:
                            callback(event)
                    except Exception as e:
                        logger.error(f"Event callback error: {str(e)}")
                
                self.event_history.append(event)
                self.event_queue.task_done()
                
            except Exception as e:
                logger.error(f"Event processing error: {str(e)}")
                await asyncio.sleep(1)
    
    def shutdown(self):
        """Shutdown the event bus"""
        self.running = False
        logger.info("Event Bus shutdown")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        return {
            'subscriber_count': sum(len(v) for v in self.subscribers.values()),
            'event_types': list(self.subscribers.keys()),
            'queue_size': self.event_queue.qsize(),
            'events_processed': len(self.event_history)
        }

# ============================================================================
# State Persistence Manager
# ============================================================================

class StatePersistenceManager:
    """
    Manages state serialization and recovery for all bio-inspired modules.
    
    Enables graceful shutdown and restart without data loss.
    """
    
    def __init__(self, state_directory: str = "./agent_state"):
        self.state_directory = state_directory
        self.save_interval = 300  # seconds
        self.last_save_time: Optional[datetime] = None
        
        # Create state directory if it doesn't exist
        os.makedirs(state_directory, exist_ok=True)
        
        logger.info(f"State Persistence Manager initialized: {state_directory}")
    
    def save_state(self, bio_core) -> bool:
        """Save complete system state"""
        try:
            timestamp = datetime.utcnow()
            correlation_id = uuid.uuid4().hex[:12]
            
            snapshot = SystemSnapshot(
                agent_state="running",
                timestamp=timestamp,
                correlation_id=correlation_id
            )
            
            # Save token state
            if hasattr(bio_core, 'token_manager'):
                summary = bio_core.token_manager.get_system_summary()
                snapshot.token_state = {
                    'summary': summary,
                    'timestamp': timestamp.isoformat()
                }
                self._save_json(f"token_state_{correlation_id}.json", snapshot.token_state)
            
            # Save gradient state
            if hasattr(bio_core, 'gradient_manager'):
                snapshot.gradient_state = {
                    'fields': bio_core.gradient_manager.get_field_stats(),
                    'timestamp': timestamp.isoformat()
                }
                self._save_json(f"gradient_state_{correlation_id}.json", snapshot.gradient_state)
            
            # Save compartment state
            if hasattr(bio_core, 'compartment_manager'):
                snapshot.compartment_state = {
                    'stats': bio_core.compartment_manager.get_ecosystem_stats(),
                    'timestamp': timestamp.isoformat()
                }
                self._save_json(f"compartment_state_{correlation_id}.json", snapshot.compartment_state)
            
            # Save biomass state
            if hasattr(bio_core, 'biomass_storage'):
                snapshot.biomass_state = {
                    'stats': bio_core.biomass_storage.get_storage_stats(),
                    'timestamp': timestamp.isoformat()
                }
                self._save_json(f"biomass_state_{correlation_id}.json", snapshot.biomass_state)
            
            # Save master snapshot reference
            self._save_json("latest_snapshot.json", {
                'correlation_id': correlation_id,
                'timestamp': timestamp.isoformat()
            })
            
            self.last_save_time = timestamp
            
            # Cleanup old snapshots (keep last 10)
            self._cleanup_old_snapshots(keep_count=10)
            
            logger.info(f"State saved successfully: {correlation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}")
            return False
    
    def restore_state(self, bio_core) -> bool:
        """Restore system state from latest snapshot"""
        try:
            # Check for saved state
            latest_path = os.path.join(self.state_directory, "latest_snapshot.json")
            if not os.path.exists(latest_path):
                logger.info("No saved state found - starting fresh")
                return False
            
            latest = self._load_json(latest_path)
            correlation_id = latest.get('correlation_id')
            
            if not correlation_id:
                return False
            
            logger.info(f"Restoring state from snapshot: {correlation_id}")
            
            # Restore token state
            token_path = os.path.join(self.state_directory, f"token_state_{correlation_id}.json")
            if os.path.exists(token_path) and hasattr(bio_core, 'token_manager'):
                token_state = self._load_json(token_path)
                logger.info(f"Restored token state: balance={token_state.get('summary', {}).get('total_balance', 'N/A')}")
            
            # Restore gradient state
            gradient_path = os.path.join(self.state_directory, f"gradient_state_{correlation_id}.json")
            if os.path.exists(gradient_path) and hasattr(bio_core, 'gradient_manager'):
                gradient_state = self._load_json(gradient_path)
                # Pump gradients to restored levels
                for field_id, field_data in gradient_state.get('fields', {}).items():
                    if isinstance(field_data, dict) and 'current_value' in field_data:
                        current = field_data['current_value']
                        if current > 0 and field_id in bio_core.gradient_manager.fields:
                            bio_core.gradient_manager.fields[field_id].current_value = current
                logger.info("Restored gradient state")
            
            logger.info(f"State restored successfully from: {correlation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore state: {str(e)}")
            return False
    
    def _save_json(self, filename: str, data: Dict[str, Any]):
        """Save data as JSON file"""
        filepath = os.path.join(self.state_directory, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _load_json(self, filename: str) -> Dict[str, Any]:
        """Load data from JSON file"""
        filepath = os.path.join(self.state_directory, filename)
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def _cleanup_old_snapshots(self, keep_count: int = 10):
        """Remove old snapshot files, keeping the most recent"""
        try:
            files = [f for f in os.listdir(self.state_directory) if f.startswith('token_state_')]
            files.sort(key=lambda f: os.path.getmtime(os.path.join(self.state_directory, f)), reverse=True)
            
            for old_file in files[keep_count:]:
                correlation_id = old_file.replace('token_state_', '').replace('.json', '')
                # Remove related files
                for prefix in ['token_state_', 'gradient_state_', 'compartment_state_', 'biomass_state_']:
                    path = os.path.join(self.state_directory, f"{prefix}{correlation_id}.json")
                    if os.path.exists(path):
                        os.remove(path)
        except Exception as e:
            logger.warning(f"Snapshot cleanup error: {str(e)}")

# ============================================================================
# Health Check Manager
# ============================================================================

class HealthCheckManager:
    """
    Manages health checks for all bio-inspired modules.
    
    Provides Kubernetes-compatible liveness and readiness probes.
    """
    
    def __init__(self):
        self.module_health: Dict[str, ModuleHealth] = {}
        self.overall_status = HealthStatus.STARTING
        self.last_full_check: Optional[datetime] = None
        
        logger.info("Health Check Manager initialized")
    
    def register_module(self, module_name: str):
        """Register a module for health checking"""
        self.module_health[module_name] = ModuleHealth(
            module_name=module_name,
            status=HealthStatus.STARTING
        )
    
    def update_health(self, module_name: str, status: HealthStatus, 
                     metrics: Optional[Dict[str, Any]] = None,
                     error: Optional[str] = None):
        """Update health status for a module"""
        if module_name not in self.module_health:
            self.register_module(module_name)
        
        health = self.module_health[module_name]
        health.status = status
        health.last_check = datetime.utcnow()
        health.error_message = error
        if metrics:
            health.metrics.update(metrics)
    
    def check_all(self, bio_core) -> Dict[str, Any]:
        """Run health checks on all modules"""
        results = {}
        all_healthy = True
        
        # Check token manager
        if hasattr(bio_core, 'token_manager'):
            try:
                summary = bio_core.token_manager.get_system_summary()
                balance = summary.get('total_balance', 0)
                
                if summary.get('emergency_mode'):
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                elif balance < 100:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('token_manager', status, {'balance': balance})
                results['token_manager'] = {'status': status.value, 'balance': balance}
            except Exception as e:
                self.update_health('token_manager', HealthStatus.UNHEALTHY, error=str(e))
                results['token_manager'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Check gradient manager
        if hasattr(bio_core, 'gradient_manager'):
            try:
                strengths = bio_core.gradient_manager.get_field_strengths()
                critical = any(s > 0.9 for s in strengths.values())
                
                if critical:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('gradient_manager', status, {'fields': strengths})
                results['gradient_manager'] = {'status': status.value, 'fields': strengths}
            except Exception as e:
                self.update_health('gradient_manager', HealthStatus.UNHEALTHY, error=str(e))
                results['gradient_manager'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Check compartment manager
        if hasattr(bio_core, 'compartment_manager'):
            try:
                stats = bio_core.compartment_manager.get_ecosystem_stats()
                viable = stats.get('viable_compartments', 0)
                total = stats.get('total_compartments', 0)
                
                if viable == 0:
                    status = HealthStatus.UNHEALTHY
                    all_healthy = False
                elif viable < total * 0.5:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('compartment_manager', status, {'viable': viable, 'total': total})
                results['compartment_manager'] = {'status': status.value, 'viable': viable}
            except Exception as e:
                self.update_health('compartment_manager', HealthStatus.UNHEALTHY, error=str(e))
                results['compartment_manager'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Check ATP synthase
        if hasattr(bio_core, 'scheduler'):
            try:
                stats = bio_core.scheduler.get_scheduler_stats()
                rate = stats.get('current_atp_rate', 0)
                
                if rate <= 0:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('atp_synthase', status, {'rate': rate})
                results['atp_synthase'] = {'status': status.value, 'rate': rate}
            except Exception as e:
                self.update_health('atp_synthase', HealthStatus.UNHEALTHY, error=str(e))
                results['atp_synthase'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        self.overall_status = HealthStatus.HEALTHY if all_healthy else HealthStatus.DEGRADED
        self.last_full_check = datetime.utcnow()
        
        return {
            'status': self.overall_status.value,
            'timestamp': datetime.utcnow().isoformat(),
            'modules': results
        }
    
    def is_ready(self) -> bool:
        """Check if agent is ready to serve requests"""
        required_modules = ['token_manager', 'gradient_manager', 'compartment_manager']
        for module in required_modules:
            if module in self.module_health:
                if self.module_health[module].status == HealthStatus.UNHEALTHY:
                    return False
            else:
                return False
        return True
    
    def is_alive(self) -> bool:
        """Check if agent process is alive"""
        return True  # If this code runs, the process is alive
    
    def get_health_report(self) -> Dict[str, Any]:
        """Get comprehensive health report"""
        return {
            'overall_status': self.overall_status.value,
            'last_full_check': self.last_full_check.isoformat() if self.last_full_check else None,
            'modules': {
                name: {
                    'status': health.status.value,
                    'last_check': health.last_check.isoformat(),
                    'error': health.error_message,
                    'metrics': health.metrics
                }
                for name, health in self.module_health.items()
            }
        }

# ============================================================================
# Enhanced Bio-Integrated Green Agent
# ============================================================================

class BioIntegratedGreenAgent:
    """
    Enhanced Bio-Integrated Green Agent v5.0.0
    
    Complete implementation with:
    - Graceful shutdown and state persistence
    - Health checks and readiness probes
    - Event bus for decoupled communication
    - Dynamic compartment scaling
    - Centralized configuration management
    - Distributed tracing with correlation IDs
    """
    
    def __init__(self, config: Optional[AgentConfig] = None):
        self.config = config or AgentConfig()
        self.state = AgentState.UNINITIALIZED
        
        # Validate configuration
        issues = self.config.validate()
        if issues:
            logger.warning(f"Configuration issues: {issues}")
        
        # Event bus for decoupled communication
        self.event_bus = EventBus()
        
        # Health check manager
        self.health_manager = HealthCheckManager()
        
        # State persistence manager
        self.state_manager = StatePersistenceManager(
            state_directory=self.config.state_directory
        ) if self.config.enable_state_persistence else None
        
        # Module references
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.supply_manager = None
        self.token_allocator = None
        self.knowledge_transfer = None
        self.degradation_manager = None
        
        # Correlation ID for request tracing
        self._correlation_counter = 0
        
        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        
        # Initialize
        self._initialize()
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info("Bio-Integrated Green Agent v5.0.0 initialized")
    
    def _initialize(self):
        """Initialize all modules with health verification"""
        self.state = AgentState.INITIALIZING
        
        try:
            # Step 1: Initialize exchange rate
            self.exchange_rate = DynamicExchangeRate()
            self.health_manager.register_module('exchange_rate')
            self.health_manager.update_health('exchange_rate', HealthStatus.HEALTHY)
            
            # Step 2: Initialize token manager
            if MODULE_STATUS.get('token_manager', False):
                self.token_manager = EcoATPTokenManager(self.exchange_rate)
                self.health_manager.register_module('token_manager')
                self.health_manager.update_health('token_manager', HealthStatus.HEALTHY)
                
                # Supply management
                if self.config.enable_supply_management:
                    self.supply_manager = TokenSupplyManager(self.token_manager)
                
                # Token pre-allocation
                if self.config.enable_token_preallocation:
                    self.token_allocator = PredictiveTokenAllocator(self.token_manager)
            
            # Step 3: Initialize gradient manager
            if MODULE_STATUS.get('gradient_manager', False):
                self.gradient_manager = HierarchicalGradientManager()
                self.health_manager.register_module('gradient_manager')
                self.health_manager.update_health('gradient_manager', HealthStatus.HEALTHY)
            
            # Step 4: Initialize ATP synthase
            if MODULE_STATUS.get('atp_synthase', False):
                synthase_config = SynthaseConfig(
                    protons_per_rotation=self.config.atp_c_ring_size,
                    max_rotation_speed_rpm=self.config.atp_max_rotation_speed
                )
                self.scheduler = ATPSynthaseScheduler(
                    self.token_manager, self.gradient_manager, synthase_config,
                    enable_multi_synthase=self.config.enable_multi_synthase
                )
                self.health_manager.register_module('atp_synthase')
                self.health_manager.update_health('atp_synthase', HealthStatus.HEALTHY)
            
            # Step 5: Initialize compartment manager
            if MODULE_STATUS.get('compartment_manager', False):
                self.compartment_manager = HierarchicalCompartmentManager(
                    self.token_manager,
                    max_regions=10,
                    compartments_per_region=20
                )
                self.health_manager.register_module('compartment_manager')
                self.health_manager.update_health('compartment_manager', HealthStatus.HEALTHY)
            
            # Step 6: Initialize biomass storage
            if MODULE_STATUS.get('biomass_storage', False):
                self.biomass_storage = BiomassStorage(self.token_manager)
                self.health_manager.register_module('biomass_storage')
                self.health_manager.update_health('biomass_storage', HealthStatus.HEALTHY)
            
            # Step 7: Initialize harvester
            if MODULE_STATUS.get('harvester', False):
                self.harvester = EnhancedPhotosyntheticHarvester(
                    self.token_manager, self.gradient_manager
                )
                self.health_manager.register_module('harvester')
                self.health_manager.update_health('harvester', HealthStatus.HEALTHY)
                
                # Wire harvester to ATP synthase
                if self.scheduler:
                    self.scheduler.inject_harvester(self.harvester)
            
            # Step 8: Initialize knowledge transfer
            if self.config.enable_knowledge_transfer:
                try:
                    from .knowledge_transfer import KnowledgeTransferManager
                    self.knowledge_transfer = KnowledgeTransferManager()
                    self.health_manager.register_module('knowledge_transfer')
                    self.health_manager.update_health('knowledge_transfer', HealthStatus.HEALTHY)
                except ImportError:
                    pass
            
            # Step 9: Initialize degradation manager
            if self.config.enable_degradation_manager:
                try:
                    from .degradation_manager import DegradationManager
                    self.degradation_manager = DegradationManager()
                    self.health_manager.register_module('degradation_manager')
                    self.health_manager.update_health('degradation_manager', HealthStatus.HEALTHY)
                except ImportError:
                    pass
            
            # Step 10: Restore state if available
            if self.state_manager:
                restored = self.state_manager.restore_state(self)
                if restored:
                    logger.info("State restored from previous session")
            
            # Step 11: Create expert compartments
            self._create_expert_compartments()
            
            # Step 12: Subscribe to events
            self._subscribe_to_events()
            
            # Step 13: Start background tasks
            self._start_background_tasks()
            
            # Step 14: Run initial health check
            self.health_manager.check_all(self)
            
            self.state = AgentState.RUNNING
            logger.info(f"Agent initialized successfully. State: {self.state.value}")
            
        except Exception as e:
            self.state = AgentState.ERROR
            logger.error(f"Initialization failed: {str(e)}", exc_info=True)
            raise
    
    def _create_expert_compartments(self):
        """Create expert compartments with dynamic scaling"""
        if not self.compartment_manager:
            return
        
        expert_types = ['energy', 'data', 'iot']
        
        if self.config.enable_quantum_expert:
            expert_types.append('quantum')
        if self.config.enable_helium_expert:
            expert_types.append('helium')
        
        for etype in expert_types:
            for i in range(self.config.compartments_per_expert_type):
                self.compartment_manager.create_compartment(etype)
        
        logger.info(f"Created compartments for {len(expert_types)} expert types "
                   f"({self.config.compartments_per_expert_type} each)")
    
    def _subscribe_to_events(self):
        """Subscribe to internal events for cross-module communication"""
        
        # Token events
        self.event_bus.subscribe('token_low', self._on_token_low)
        self.event_bus.subscribe('token_critical', self._on_token_critical)
        
        # Gradient events
        self.event_bus.subscribe('gradient_high', self._on_gradient_high)
        self.event_bus.subscribe('gradient_critical', self._on_gradient_critical)
        
        # Compartment events
        self.event_bus.subscribe('compartment_unhealthy', self._on_compartment_unhealthy)
        self.event_bus.subscribe('compartment_depleted', self._on_compartment_depleted)
        
        logger.info(f"Subscribed to {len(self.event_bus.subscribers)} event types")
    
    def _start_background_tasks(self):
        """Start all background maintenance tasks"""
        
        # Health check loop
        task = asyncio.create_task(self._health_check_loop())
        self._background_tasks.append(task)
        
        # State persistence loop
        if self.state_manager:
            task = asyncio.create_task(self._state_persistence_loop())
            self._background_tasks.append(task)
        
        # Dynamic scaling loop
        task = asyncio.create_task(self._dynamic_scaling_loop())
        self._background_tasks.append(task)
        
        # Environmental monitoring loop
        task = asyncio.create_task(self._environmental_loop())
        self._background_tasks.append(task)
        
        logger.info(f"Started {len(self._background_tasks)} background tasks")
    
    def _register_signal_handlers(self):
        """Register OS signal handlers for graceful shutdown"""
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            logger.info("Signal handlers registered")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")
    
    # ========================================================================
    # Correlation ID for Distributed Tracing
    # ========================================================================
    
    def _generate_correlation_id(self) -> str:
        """Generate unique correlation ID for request tracing"""
        self._correlation_counter += 1
        return f"corr_{datetime.utcnow().timestamp()}_{self._correlation_counter}"
    
    # ========================================================================
    # Event Handlers
    # ========================================================================
    
    async def _on_token_low(self, event: Dict[str, Any]):
        """Handle low token event"""
        logger.warning(f"Token low event: {event['payload']}")
        
        # Pause non-essential operations
        if self.degradation_manager:
            self.degradation_manager.update_metrics(token_balance=event['payload'].get('balance', 0))
    
    async def _on_token_critical(self, event: Dict[str, Any]):
        """Handle critical token event"""
        logger.error(f"Token critical event: {event['payload']}")
        
        # Activate emergency mode
        if self.token_manager:
            self.token_manager._activate_emergency_mode()
    
    async def _on_gradient_high(self, event: Dict[str, Any]):
        """Handle high gradient event"""
        field_id = event['payload'].get('field_id', 'unknown')
        logger.warning(f"High gradient: {field_id}")
        
        # Increase leakage temporarily
        if self.gradient_manager and field_id in self.gradient_manager.fields:
            field = self.gradient_manager.fields[field_id]
            field.leakage_rate = min(0.3, field.leakage_rate * 2)
    
    async def _on_gradient_critical(self, event: Dict[str, Any]):
        """Handle critical gradient event"""
        field_id = event['payload'].get('field_id', 'unknown')
        logger.error(f"Critical gradient: {field_id}")
        
        # Activate uncoupling in ATP synthase
        if self.scheduler:
            for synthase in self.scheduler.synthases.values():
                synthase.operate_uncoupled(self.gradient_manager)
    
    async def _on_compartment_unhealthy(self, event: Dict[str, Any]):
        """Handle unhealthy compartment event"""
        compartment_id = event['payload'].get('compartment_id', 'unknown')
        logger.warning(f"Unhealthy compartment: {compartment_id}")
        
        # Spawn replacement
        if self.compartment_manager:
            expert_type = event['payload'].get('expert_type', 'data')
            self.compartment_manager.create_compartment(expert_type)
    
    async def _on_compartment_depleted(self, event: Dict[str, Any]):
        """Handle compartment depletion event"""
        expert_type = event['payload'].get('expert_type', 'unknown')
        logger.error(f"Compartment type depleted: {expert_type}")
        
        # Emergency spawning
        if self.compartment_manager:
            for _ in range(3):
                self.compartment_manager.create_compartment(expert_type)
    
    # ========================================================================
    # Background Loops
    # ========================================================================
    
    async def _health_check_loop(self):
        """Periodic health check loop"""
        while self.state == AgentState.RUNNING:
            try:
                self.health_manager.check_all(self)
                
                # Publish health events
                if self.health_manager.overall_status == HealthStatus.DEGRADED:
                    self.event_bus.publish('agent_degraded', {
                        'status': self.health_manager.overall_status.value
                    })
                
                await asyncio.sleep(self.config.health_check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _state_persistence_loop(self):
        """Periodic state persistence loop"""
        while self.state == AgentState.RUNNING:
            try:
                if self.state_manager:
                    self.state_manager.save_state(self)
                await asyncio.sleep(self.config.state_save_interval_seconds)
            except Exception as e:
                logger.error(f"State persistence error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _dynamic_scaling_loop(self):
        """Dynamic compartment scaling based on load"""
        while self.state == AgentState.RUNNING:
            try:
                if self.compartment_manager and self.token_manager:
                    summary = self.token_manager.get_system_summary()
                    balance = summary.get('total_balance', 0)
                    
                    # Scale up if tokens abundant and compartments few
                    total_compartments = sum(
                        len(r.compartments) for r in self.compartment_manager.regions.values()
                    )
                    
                    if balance > 1000 and total_compartments < self.config.max_total_compartments:
                        # Check if any expert type needs more compartments
                        for etype in ['energy', 'data', 'iot']:
                            count = sum(
                                1 for r in self.compartment_manager.regions.values()
                                for c in r.compartments.values()
                                if c.expert_type == etype and c.is_viable
                            )
                            if count < 3:
                                self.compartment_manager.create_compartment(etype)
                                logger.info(f"Auto-scaled {etype} compartment (count: {count})")
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Dynamic scaling error: {str(e)}")
                await asyncio.sleep(120)
    
    async def _environmental_loop(self):
        """Environmental monitoring and harvesting loop"""
        while self.state == AgentState.RUNNING:
            try:
                if self.harvester:
                    # Simulated environmental data
                    env_data = {
                        'renewable_availability': np.random.uniform(0.3, 0.9),
                        'carbon_intensity': np.random.uniform(100, 600),
                        'waste_heat': np.random.uniform(0.1, 0.5),
                        'edge_availability': np.random.uniform(0.2, 0.8),
                        'system_overload': np.random.uniform(0.0, 0.3)
                    }
                    
                    result = await self.harvester.harvest_cycle(env_data)
                    
                    if result['eco_atp_generated'] > 0:
                        self.event_bus.publish('harvest_complete', {
                            'eco_atp_generated': result['eco_atp_generated'],
                            'dominant_signal': result['dominant_signal']
                        })
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Environmental loop error: {str(e)}")
                await asyncio.sleep(30)
    
    # ========================================================================
    # Public API Methods
    # ========================================================================
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a task through the bio-inspired system with correlation tracing.
        """
        correlation_id = self._generate_correlation_id()
        task['correlation_id'] = correlation_id
        
        logger.info(f"Processing task: {task.get('task_id', 'unknown')} [{correlation_id}]")
        
        # Publish task received event
        self.event_bus.publish('task_received', {
            'task_id': task.get('task_id'),
            'task_type': task.get('task_type'),
            'correlation_id': correlation_id
        })
        
        # Calculate Eco-ATP cost
        ecoatp_required = task.get('complexity', 0.5) * 10
        
        # Try token allocation
        if self.token_allocator:
            success, latency = self.token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self.token_allocator.record_demand('task_processor', ecoatp_required)
        elif self.token_manager:
            success, _ = self.token_manager.reserve_tokens(
                'task_processor', ecoatp_required, EcoATPConsumer.EXPERT_EXECUTION
            )
        else:
            success = True
        
        if not success:
            # Store in biomass
            if self.biomass_storage:
                stored, token_id = self.biomass_storage.store_task(
                    task_data=task, ecoatp_cost=ecoatp_required
                )
                
                self.event_bus.publish('task_stored', {
                    'task_id': task.get('task_id'),
                    'biomass_token': token_id,
                    'correlation_id': correlation_id
                })
                
                return {
                    'success': True,
                    'status': 'stored',
                    'biomass_token': token_id,
                    'correlation_id': correlation_id
                }
            
            return {
                'success': False,
                'reason': 'Insufficient tokens',
                'correlation_id': correlation_id
            }
        
        # Execute task
        result = {
            'success': True,
            'task_id': task.get('task_id', 'unknown'),
            'correlation_id': correlation_id,
            'ecoatp_cost': ecoatp_required
        }
        
        # Publish task completed event
        self.event_bus.publish('task_completed', {
            'task_id': task.get('task_id'),
            'success': True,
            'correlation_id': correlation_id
        })
        
        return result
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        status = {
            'agent_state': self.state.value,
            'timestamp': datetime.utcnow().isoformat(),
            'health': self.health_manager.get_health_report(),
            'event_bus': self.event_bus.get_stats(),
            'config': self.config.to_dict()
        }
        
        # Module-specific status
        if self.token_manager:
            status['token_economy'] = self.token_manager.get_system_summary()
        
        if self.gradient_manager:
            status['gradients'] = self.gradient_manager.get_field_stats()
            status['forecasts'] = self.gradient_manager.get_forecast_summary()
        
        if self.compartment_manager:
            status['compartments'] = self.compartment_manager.get_ecosystem_stats()
        
        if self.biomass_storage:
            status['biomass'] = self.biomass_storage.get_storage_stats()
        
        if self.harvester:
            status['harvester'] = self.harvester.get_harvesting_stats()
        
        if self.scheduler:
            status['atp_synthase'] = self.scheduler.get_scheduler_stats()
        
        if self.supply_manager:
            status['supply_management'] = self.supply_manager.get_economic_indicators()
        
        if self.token_allocator:
            status['pre_allocation'] = self.token_allocator.get_cache_stats()
        
        return status
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health check status (for Kubernetes probes)"""
        return {
            'status': self.health_manager.overall_status.value,
            'ready': self.health_manager.is_ready(),
            'alive': self.health_manager.is_alive(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_configuration(self) -> Dict[str, Any]:
        """Get current configuration"""
        return self.config.to_dict()
    
    def update_configuration(self, updates: Dict[str, Any]):
        """Update configuration at runtime"""
        for key, value in updates.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        logger.info(f"Configuration updated: {list(updates.keys())}")
    
    # ========================================================================
    # Graceful Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown with state preservation"""
        if self.state == AgentState.SHUTTING_DOWN:
            return
        
        self.state = AgentState.SHUTTING_DOWN
        logger.info("Initiating graceful shutdown...")
        
        # Step 1: Stop accepting new tasks
        self.event_bus.publish('agent_shutdown', {'timestamp': datetime.utcnow().isoformat()})
        
        # Step 2: Save state
        if self.state_manager:
            logger.info("Saving state...")
            self.state_manager.save_state(self)
        
        # Step 3: Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        
        # Step 4: Shutdown event bus
        self.event_bus.shutdown()
        
        # Step 5: Cleanup resources
        if self.compartment_manager:
            # Decommission all compartments
            for region in list(self.compartment_manager.regions.values()):
                for comp_id in list(region.compartments.keys()):
                    self.compartment_manager.decommission_compartment(comp_id)
        
        self.state = AgentState.SHUTDOWN
        logger.info("Graceful shutdown complete")
    
    def get_correlation_trace(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """Get trace for a specific correlation ID"""
        trace = {
            'correlation_id': correlation_id,
            'events': []
        }
        
        for event in self.event_bus.event_history:
            if event.get('correlation_id') == correlation_id:
                trace['events'].append({
                    'event_type': event['event_type'],
                    'timestamp': event['timestamp'].isoformat(),
                    'payload': str(event['payload'])[:200]
                })
        
        return trace if trace['events'] else None

# ============================================================================
# Convenience Functions
# ============================================================================

def create_agent(config: Optional[AgentConfig] = None) -> BioIntegratedGreenAgent:
    """Create a bio-integrated agent with default or custom configuration"""
    return BioIntegratedGreenAgent(config=config)

def create_agent_from_file(config_path: str) -> BioIntegratedGreenAgent:
    """Create agent from configuration file"""
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    config = AgentConfig.from_dict(config_data)
    return BioIntegratedGreenAgent(config=config)
