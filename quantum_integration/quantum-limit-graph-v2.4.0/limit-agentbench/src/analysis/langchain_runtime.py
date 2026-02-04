from src.analysis.langchain_callbacks import GreenLangChainCallback

class LangChainRuntime(AgentRuntime):
    def init(self, config):
        self.callback = GreenLangChainCallback()
        self.llm = config["llm"](
            callbacks=[self.callback]
        )

    def run(self, query):
        output = self.llm.invoke(query["input"])
        metrics = self.callback.snapshot()
        return {
            "output": output,
            **metrics
        }
