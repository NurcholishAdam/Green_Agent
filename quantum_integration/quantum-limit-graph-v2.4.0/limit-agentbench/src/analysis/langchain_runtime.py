"""
LangChain runtime adapter for Green Agent.

Uses real LangChain callbacks when available.
Falls back gracefully if LangChain is not installed.
"""

from typing import Dict, Any
from src.analysis.runtime_adapter import AgentRuntime


class LangChainRuntime(AgentRuntime):
    """
    Executes queries using LangChain and collects real metrics.
    """

    def init(self, config: Dict[str, Any]):
        self.enabled = False
        self.metrics = {
            "tool_calls": 0,
            "llm_calls": 0,
        }

        try:
            from langchain.callbacks.base import BaseCallbackHandler
            from langchain.chains import LLMChain
            from langchain.prompts import PromptTemplate

            class GreenCallback(BaseCallbackHandler):
                def __init__(self, outer):
                    self.outer = outer

                def on_tool_start(self, *args, **kwargs):
                    self.outer.metrics["tool_calls"] += 1

                def on_llm_start(self, *args, **kwargs):
                    self.outer.metrics["llm_calls"] += 1

            self.callback = GreenCallback(self)

            llm = config.get("llm")
            if llm is None:
                raise ValueError("LangChain LLM not provided")

            prompt = PromptTemplate(
                input_variables=["input"],
                template="{input}"
            )

            self.chain = LLMChain(
                llm=llm,
                prompt=prompt,
                callbacks=[self.callback],
            )

            self.enabled = True

        except Exception as e:
            self.init_error = str(e)
            self.enabled = False

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            return {
                "output": None,
                "tool_calls": 0,
                "conversation_depth": 0,
                "error": "LangChain unavailable",
            }

        output = self.chain.run(query.get("input", ""))

        return {
            "output": output,
            "tool_calls": self.metrics["tool_calls"],
            "conversation_depth": self.metrics["llm_calls"],
        }

    def finalize(self):
        pass
