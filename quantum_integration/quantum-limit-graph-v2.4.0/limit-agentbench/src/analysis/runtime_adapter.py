# src/analysis/runtime_adapter.py

from abc import ABC, abstractmethod
from typing import Dict, Any


class AgentRuntime(ABC):
    """
    Framework-agnostic execution interface.
    """

    @abstractmethod
    def init(self, config: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes one query.
        Must return at least:
        {
            "accuracy": float,
            "output": Any
        }
        """
        pass

    @abstractmethod
    def finalize(self) -> None:
        pass
