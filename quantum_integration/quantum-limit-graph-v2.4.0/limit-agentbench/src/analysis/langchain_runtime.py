from src.analysis.runtime_adapter import AgentRuntime


class LangChainRuntime(AgentRuntime):
    def run(self, query):
        return {
            "accuracy": 0.82,
            "tool_calls": 4,
            "conversation_depth": 2,
        }
