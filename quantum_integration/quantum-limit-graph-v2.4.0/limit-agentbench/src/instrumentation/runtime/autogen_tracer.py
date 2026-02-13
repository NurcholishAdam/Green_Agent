class AutoGenTracer:
    """
    Extracts message graph from AutoGen conversation.
    """

    def __init__(self):
        self.edges = []

    def record(self, sender, receiver, message):
        self.edges.append((sender, receiver, len(message)))

    def get_graph(self):
        return self.edges
