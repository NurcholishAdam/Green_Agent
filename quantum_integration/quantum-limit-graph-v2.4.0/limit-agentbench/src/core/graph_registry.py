"""
graph_registry.py — Central Graph & Helium Monitor Registry
============================================================
Manages lifecycle, registration, and health reporting for all graph types
and the optional Helium supply chain monitor.

Thread Safety:
- Registry operations are thread-safe for read/write graph access.
- Helium monitor access is read-only after registration.
"""

import logging
import threading
from typing import Dict, Optional, List, Any, TYPE_CHECKING
from datetime import datetime
from enum import Enum

# Type checking imports to avoid circular dependencies
if TYPE_CHECKING:
    from carbon.helium_monitor import HeliumMonitor

logger = logging.getLogger(__name__)


class GraphType(Enum):
    """Supported graph types in the system"""
    CAUSAL = "causal"
    POLICY = "policy"
    SIMILARITY = "similarity"
    CARBON_LEDGER = "carbon_ledger"


class GraphRegistry:
    """
    Central registry for graph instances and helium monitor.
    Provides health reporting, type-safe access, and lifecycle management.
    """

    def __init__(self):
        self._graphs: Dict[GraphType, Any] = {}
        self._metadata: Dict[str, Any] = {
            "execution_count": 0,
            "singletons": {},
            "last_updated": datetime.now().isoformat()
        }
        self._helium_monitor: Optional['HeliumMonitor'] = None
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        
        logger.info("GraphRegistry initialized")

    # ------------------------------------------------------------------
    # Graph Management
    # ------------------------------------------------------------------

    def register(self, graph_type: GraphType, graph: Any, metadata: Optional[Dict] = None) -> None:
        """
        Register a graph instance with the registry
        
        Args:
            graph_type: Type of graph (causal, policy, etc.)
            graph: Graph instance
            metadata: Optional metadata dict (node_count, edge_count, etc.)
        """
        with self._lock:
            self._graphs[graph_type] = graph
            
            if metadata:
                self._metadata["singletons"][graph_type.value] = metadata
                
            self._metadata["last_updated"] = datetime.now().isoformat()
            logger.info(f"Registered {graph_type.value} graph")

    def get(self, graph_type: GraphType) -> Optional[Any]:
        """
        Retrieve a graph instance by type
        
        Args:
            graph_type: Type of graph to retrieve
            
        Returns:
            Graph instance or None if not registered
        """
        with self._lock:
            return self._graphs.get(graph_type)

    def list_graphs(self) -> List[GraphType]:
        """Return list of registered graph types"""
        with self._lock:
            return list(self._graphs.keys())

    def health(self) -> Dict[str, Any]:
        """
        Get registry health and graph statistics
        
        Returns:
            Dict with execution count, singleton stats, and helium status
        """
        with self._lock:
            health_data = dict(self._metadata)
            health_data["helium_monitor_registered"] = self._helium_monitor is not None
            return health_data

    # ------------------------------------------------------------------
    # Helium Monitor Integration (NEW)
    # ------------------------------------------------------------------

    def register_helium_monitor(self, monitor: 'HeliumMonitor') -> None:
        """
        Register HeliumMonitor for metrics collection and decision making
        
        Args:
            monitor: Initialized HeliumMonitor instance
        """
        with self._lock:
            self._helium_monitor = monitor
            logger.info("HeliumMonitor registered with GraphRegistry")

    def get_helium_monitor(self) -> Optional['HeliumMonitor']:
        """
        Get registered HeliumMonitor instance
        
        Returns:
            HeliumMonitor instance or None if not registered
        """
        with self._lock:
            return self._helium_monitor

    # ------------------------------------------------------------------
    # Utility & Cleanup
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Reset registry state (useful for testing)"""
        with self._lock:
            self._graphs.clear()
            self._helium_monitor = None
            self._metadata = {
                "execution_count": 0,
                "singletons": {},
                "last_updated": datetime.now().isoformat()
            }
            logger.info("GraphRegistry reset")

    async def shutdown(self) -> None:
        """Gracefully shutdown registered components"""
        logger.info("Shutting down GraphRegistry...")
        with self._lock:
            if self._helium_monitor:
                try:
                    await self._helium_monitor.shutdown()
                except Exception as e:
                    logger.error(f"Error shutting down helium monitor: {e}")
            self._graphs.clear()
            logger.info("GraphRegistry shutdown complete")
