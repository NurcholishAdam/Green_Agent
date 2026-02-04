"""
Base adapter for framework runtimes.
Defines the interface that all runtime adapters must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseRuntimeAdapter(ABC):
    @abstractmethod
    def init(self, config: Dict[str, Any]) -> None:
        """Initialize the runtime with configuration."""
        pass

    @abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single query and return raw result metrics."""
        pass

    @abstractmethod
    def finalize(self) -> None:
        """Clean up resources."""
        pass
