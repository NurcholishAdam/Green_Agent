# src/analysis/langchain_runtime.py

from typing import Dict, Any

from .runtime_adapter import AgentRuntime


class LangChainRuntime(AgentRuntime):
    """
    Adapter for LangChain chains / runnables.

    This module is OPTIONAL.
    LangChain is imported lazily to avoid hard dependency.
    """

    def init(self, config: Dict[str, Any]) -> None:
        try:
            from langchain_core.runnables import Runnable
        except ImportError:
            raise RuntimeError(
                "LangChain is not installed. "
                "Install with `pip install langchain` to use LangChainRuntime."
            )

        self.chain = config.get("chain")
        if self.chain is None:
            raise ValueError("LangChainRuntime requires `chain` in config")

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes one LangChain invocation.
        """
        input_data = query.get("input")

        result = self.chain.invoke(input_data)

        # Accuracy must be user-defined or proxy-based
        accuracy = query.get("expected") == result if "expected" in query else 0.0

        return {
            "output": result,
            "accuracy": float(accuracy),
        }

    def finalize(self) -> None:
        pass
