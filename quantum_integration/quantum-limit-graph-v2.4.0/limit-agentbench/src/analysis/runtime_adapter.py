from abc import ABC, abstractmethod
from typing import Dict


class AgentRuntime(ABC):
    @abstractmethod
    def run(self, query: Dict) -> Dict:
        """
        Must return:
        accuracy, tool_calls, conversation_depth
        """
        pass
