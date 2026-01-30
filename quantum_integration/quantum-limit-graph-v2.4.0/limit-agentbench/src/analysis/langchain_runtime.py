# src/analysis/langchain_runtime.py
from typing import Dict, Any
from langchain.callbacks import get_openai_callback
from .runtime_adapter import AgentRuntime

class LangChainRuntime(AgentRuntime):
    def init(self, config: Dict[str, Any]):
        self.agent = config["agent"]  # already constructed upstream

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        with get_openai_callback() as cb:
            response = self.agent.run(query["input"])

        # LangChain exposes this natively
        tool_calls = cb.tool_calls
        tokens = cb.total_tokens

        # Conversation depth = reasoning turns
        conversation_depth = max(1, cb.successful_requests)

        return {
            "accuracy": self._score(response),
            "tool_calls": tool_calls,
            "conversation_depth": conversation_depth,
            "tokens": tokens,
        }

    def _score(self, response) -> float:
        return 1.0 if response else 0.0
