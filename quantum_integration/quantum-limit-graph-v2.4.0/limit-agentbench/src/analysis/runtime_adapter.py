# src/analysis/runtime_adapter.py
from abc import ABC, abstractmethod
from typing import Dict, Any

class AgentRuntime(ABC):
    @abstractmethod
    def init(self, config: Dict[str, Any]):
        pass

    @abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Must return:
        {
          accuracy: float,
          tool_calls: int,
          conversation_depth: int
        }
        """
        pass

    def finalize(self):
        pass
