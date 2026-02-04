class AutoGenConversationGraph:
    """
    Extracts message graph metrics from AutoGen runs.
    """

    def __init__(self):
        self.messages = []

    def record(self, sender: str, recipient: str, content: str):
        self.messages.append({
            "from": sender,
            "to": recipient,
            "content": content
        })

    def metrics(self):
        nodes = set()
        for m in self.messages:
            nodes.add(m["from"])
            nodes.add(m["to"])

        return {
            "conversation_depth": len(self.messages),
            "agent_nodes": len(nodes)
        }
