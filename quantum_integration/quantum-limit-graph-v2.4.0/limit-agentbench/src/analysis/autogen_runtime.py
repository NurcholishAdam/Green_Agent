# src/analysis/autogen_runtime.py
from typing import Dict, Any
from .runtime_adapter import AgentRuntime

class AutoGenRuntime(AgentRuntime):
    def init(self, config: Dict[str, Any]):
        self.agent = config["agent"]

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        chat_result = self.agent.run(query["input"])

        # AutoGen stores conversation graph
        messages = chat_result.chat_history

        tool_calls = sum(
            1 for m in messages if m.get("tool_call") is not None
        )

        conversation_depth = len(messages)

        return {
            "accuracy": self._score(chat_result),
            "tool_calls": tool_calls,
            "conversation_depth": conversation_depth,
        }

    def _score(self, result) -> float:
        return 1.0 if result.success else 0.0
