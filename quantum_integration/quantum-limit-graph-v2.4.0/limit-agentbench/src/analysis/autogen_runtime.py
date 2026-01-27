# src/analysis/autogen_runtime.py

from typing import Dict, Any
from .runtime_adapter import AgentRuntime


class AutoGenRuntime(AgentRuntime):
    """
    Adapter for AutoGen with conversation-depth measurement.
    """

    def init(self, config: Dict[str, Any]) -> None:
        try:
            import autogen
        except ImportError:
            raise RuntimeError(
                "AutoGen not installed. "
                "pip install pyautogen"
            )

        self.agent = config.get("agent")
        if self.agent is None:
            raise ValueError("AutoGenRuntime requires `agent` in config")

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        prompt = query.get("input")

        messages = [{"role": "user", "content": prompt}]

        reply = self.agent.generate_reply(messages=messages)

        # Conversation depth = user + agent replies
        conversation_depth = len(messages) + 1

        accuracy = (
            query.get("expected") == reply
            if "expected" in query
            else 0.0
        )

        return {
            "output": reply,
            "accuracy": float(accuracy),
            "conversation_depth": conversation_depth,
        }

    def finalize(self) -> None:
        pass
