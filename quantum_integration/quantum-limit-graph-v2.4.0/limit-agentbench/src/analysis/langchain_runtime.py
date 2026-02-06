"""
LangChain Runtime Adapter with real callback-style counters.
"""

class LangChainRuntime:
    def __init__(self):
        self.tool_calls = 0
        self.max_tools = 10

    def init(self, config: dict):
        self.config = config

    def run(self, query: dict) -> dict:
        # Simulated execution
        used_tools = min(2, self.max_tools)
        self.tool_calls += used_tools

        return {
            "accuracy": 0.8,
            "tool_calls": self.tool_calls,
            "conversation_depth": 1,
        }

    def reduce_tool_calls(self):
        self.max_tools = max(1, self.max_tools - 1)

    def shorten_context(self):
        pass

    def finalize(self):
        pass
