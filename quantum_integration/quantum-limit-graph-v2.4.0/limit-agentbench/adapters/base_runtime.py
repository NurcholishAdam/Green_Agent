"""
Base adapter for framework runtimes.
Defines the interface that all runtime adapters must implement.
Enhanced with optional integration hooks for LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, and MoE expert gating.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging

# Optional imports for enhancements (graceful degradation if not available)
try:
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Dummy classes to avoid NameError if not used
    class WorkloadDescriptor: pass
    class NodeDescriptor: pass
    class ZeroTrustArchitecture: pass
    class FeedbackEvent: pass

logger = logging.getLogger(__name__)


class BaseRuntimeAdapter(ABC):
    """
    Abstract base class for runtime adapters.

    Provides a unified interface for running agents on different frameworks.
    When ``use_enhancements`` is True and the enhanced modules are available,
    the adapter can optionally:
      - Use a distillation optimizer (with MoE gating) to select execution strategy
        (e.g., native framework vs FlexGen low/high precision).
      - Compute multi‑objective rewards (MODP) after execution.
      - Incorporate RLHF feedback via state features.
      - Integrate LIMIT Graph metrics into the decision state.
      - Emit `FeedbackEvent` for cross‑module learning.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the runtime adapter.

        Args:
            config: Optional configuration dictionary for enhancements.
        """
        self.config = config or {}
        self.use_enhancements = (
            self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        )
        self.track_green_metrics = self.config.get('track_green_metrics', True)

        # Enhanced components (lazy initialized)
        self.workload_descriptor = None
        self.node_descriptor = None
        self.zero_trust = None
        self.distillation_optimizer = None

        if self.use_enhancements:
            self._init_enhancements()

        logger.info(
            f"BaseRuntimeAdapter initialized (enhancements={'on' if self.use_enhancements else 'off'})"
        )

    def _init_enhancements(self):
        """Initialize optional enhanced components."""
        try:
            # Distillation optimizer for execution strategy
            from src.enhancements.schemas.node_descriptor import DistillationRoutingOptimizer
            self.distillation_optimizer = DistillationRoutingOptimizer(self.config)

            # Workload descriptor (optional)
            if self.config.get('use_workload_descriptor', False) and WorkloadDescriptor is not None:
                self.workload_descriptor = WorkloadDescriptor(
                    task_id=self.config.get('task_id', 'default_task'),
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get('default_tokens', 1000),
                    latency_target=self.config.get('default_latency', 500.0),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id="base_runtime",
                    metadata={"adapter": "base_runtime"}
                )

            # Node descriptor (optional)
            if self.config.get('use_node_descriptor', False) and NodeDescriptor is not None:
                self.node_descriptor = NodeDescriptor(
                    id="runtime_node",
                    type=NodeType.EDGE,
                    region=self.config.get('region', 'us-east'),
                    region_carbon_intensity=self.config.get('carbon_intensity', 400.0),
                    energy_per_token=0.00005,
                    helium_connectivity_score=0.8,
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type="air",
                    hardware_model="cpu"
                )

            # Zero trust (optional)
            if self.config.get('enable_zero_trust', False):
                self.zero_trust = ZeroTrustArchitecture()

            logger.info("Enhanced components initialized")
        except Exception as e:
            logger.error(f"Failed to initialize enhancements: {e}")
            self.use_enhancements = False
            self.distillation_optimizer = None

    @abstractmethod
    def init(self, config: Dict[str, Any]) -> None:
        """Initialize the runtime with configuration. Subclasses may call this and also set self.config."""
        # Store config for later use
        self.config = config

    @abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single query and return raw result metrics."""
        pass

    @abstractmethod
    def finalize(self) -> None:
        """Clean up resources."""
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """Return metadata about the runtime adapter and enhancement status."""
        meta = {
            "adapter_type": self.__class__.__name__,
            "enhancements_enabled": self.use_enhancements,
        }
        if self.use_enhancements:
            meta["use_workload_descriptor"] = self.workload_descriptor is not None
            meta["use_node_descriptor"] = self.node_descriptor is not None
            meta["use_zero_trust"] = self.zero_trust is not None
            meta["distillation_optimizer"] = (
                self.distillation_optimizer is not None
            )
        return meta
