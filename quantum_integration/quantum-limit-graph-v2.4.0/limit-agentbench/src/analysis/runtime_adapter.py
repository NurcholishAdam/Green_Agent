# src/analysis/runtime_adapter.py

from abc import ABC, abstractmethod
from typing import Dict, Any


class AgentRuntime(ABC):
    """
    Framework-agnostic interface for agent execution.
    """

    @abstractmethod
    def init(self, config: Dict[str, Any]) -> None:
        """Initialize agent with configuration."""
        pass

    @abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single query.

        Must return:
        {
            "accuracy": float,
            "output": Any
        }
        """
        pass

    @abstractmethod
    def finalize(self) -> None:
        """Cleanup resources if needed."""
        pass


class NativeAgentRuntime(AgentRuntime):
    """
    Default runtime for simple / baseline agents.
    """

    def init(self, config: Dict[str, Any]) -> None:
        self.config = config

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        # Placeholder logic — replace with real agent inference
        output = query.get("input", "")
        accuracy = 1.0 if output else 0.0
        return {"accuracy": accuracy, "output": output}

    def finalize(self) -> None:
        pass
