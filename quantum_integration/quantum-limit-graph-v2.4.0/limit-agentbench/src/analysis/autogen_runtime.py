from src.analysis.runtime_adapter import AgentRuntime


class AutoGenRuntime(AgentRuntime):
    def run(self, query):
        return {
            "accuracy": 0.85,
            "tool_calls": 6,
            "conversation_depth": 3,
        }
