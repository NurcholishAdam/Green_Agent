from src.analysis.autogen_graph import AutoGenConversationGraph

class AutoGenRuntime(AgentRuntime):
    def init(self, config):
        self.graph = AutoGenConversationGraph()
        self.agents = config["agents"]

    def run(self, query):
        for msg in self.agents.run(query["input"]):
            self.graph.record(
                sender=msg.sender,
                recipient=msg.recipient,
                content=msg.content
            )

        return self.graph.metrics()
