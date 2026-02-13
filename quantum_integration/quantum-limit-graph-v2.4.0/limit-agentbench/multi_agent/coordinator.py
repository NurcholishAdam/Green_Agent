class MultiAgentCoordinator:

    def __init__(self, agents):
        self.agents = agents

    def distribute(self, task):
        results = []

        for agent in self.agents:
            res = agent.execute(task)
            results.append(res)

        return results
