"""
Wraps GreenAgentRunner into distributed-ready node.
"""

from run_agent import GreenAgentRunner


class AgentNode:
    """
    Represents a distributed agent instance.
    """

    def __init__(self, runtime, policy):
        self.runner = GreenAgentRunner(runtime, policy)

    def run(self, task_input):
        return self.runner.run(task_input)
