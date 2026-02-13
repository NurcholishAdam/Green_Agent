class LangChainRuntime:
    def __init__(self):
        self.tool_calls = 0
        self.depth = 0
        self.max_tools = 5

    def init(self, config):
        self.config = config

    def run(self, query):
        used = min(2, self.max_tools)
        self.tool_calls += used
        self.depth += 1

        return {
            "accuracy": 0.82,
            "tool_calls": self.tool_calls,
            "conversation_depth": self.depth,
        }

    def reduce_tool_calls(self):
        self.max_tools = max(1, self.max_tools - 1)

    def shorten_context(self):
        pass

    def finalize(self):
        pass
