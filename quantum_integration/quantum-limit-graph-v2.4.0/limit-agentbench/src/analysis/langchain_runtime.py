# src/analysis/langchain_runtime.py

from typing import Dict, Any
from .runtime_adapter import AgentRuntime


class ToolCallCounter:
    """
    LangChain callback handler to count tool calls.
    """

    def __init__(self):
        self.count = 0

    def on_tool_start(self, *args, **kwargs):
        self.count += 1


class LangChainRuntime(AgentRuntime):
    """
    Adapter for LangChain chains with tool-call counting.
    """

    def init(self, config: Dict[str, Any]) -> None:
        try:
            from langchain_core.callbacks import BaseCallbackManager
        except ImportError:
            raise RuntimeError(
                "LangChain not installed. "
                "pip install langchain"
            )

        self.chain = config.get("chain")
        if self.chain is None:
            raise ValueError("LangChainRuntime requires `chain` in config")

        self.counter = ToolCallCounter()

        # Attach callback if supported
        if hasattr(self.chain, "callbacks"):
            self.chain.callbacks.append(self.counter)

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        input_data = query.get("input")

        output = self.chain.invoke(input_data)

        accuracy = (
            query.get("expected") == output
            if "expected" in query
            else 0.0
        )

        return {
            "output": output,
            "accuracy": float(accuracy),
            "tool_calls": self.counter.count,
        }

    def finalize(self) -> None:
        pass
