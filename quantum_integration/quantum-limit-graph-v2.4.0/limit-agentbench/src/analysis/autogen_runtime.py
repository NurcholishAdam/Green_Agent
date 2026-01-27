# src/analysis/autogen_runtime.py

from typing import Dict, Any

from .runtime_adapter import AgentRuntime


class AutoGenRuntime(AgentRuntime):
    """
    Adapter for AutoGen conversational agents.

    Measures one conversation turn as a benchmark unit.
    """

    def init(self, config: Dict[str, Any]) -> None:
        try:
            import autogen
        except ImportError:
            raise RuntimeError(
                "AutoGen is not installed. "
                "Install with `pip install pyautogen` to use AutoGenRuntime."
            )

        self.agent = config.get("agent")
        if self.agent is None:
            raise ValueError("AutoGenRuntime requires `agent` in config")

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a single AutoGen interaction.
        """
        prompt = query.get("input")

        reply = self.agent.generate_reply(
            messages=[{"role": "user", "content": prompt}]
        )

        accuracy = (
            query.get("expected") == reply
            if "expected" in query
            else 0.0
        )

        return {
            "output": reply,
            "accuracy": float(accuracy),
        }

    def finalize(self) -> None:
        pass
