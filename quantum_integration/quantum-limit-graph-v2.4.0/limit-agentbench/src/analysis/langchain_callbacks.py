from langchain.callbacks.base import BaseCallbackHandler
from typing import Any, Dict, List


class GreenLangChainCallback(BaseCallbackHandler):
    """
    Collects real LangChain runtime metrics:
    - tool calls
    - token usage (if available)
    """

    def __init__(self):
        self.tool_calls = 0
        self.llm_calls = 0

    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs):
        self.tool_calls += 1

    def on_llm_start(self, serialized: Dict[str, Any], prompts: List[str], **kwargs):
        self.llm_calls += 1

    def snapshot(self) -> Dict[str, int]:
        return {
            "tool_calls": self.tool_calls,
            "llm_calls": self.llm_calls,
        }
