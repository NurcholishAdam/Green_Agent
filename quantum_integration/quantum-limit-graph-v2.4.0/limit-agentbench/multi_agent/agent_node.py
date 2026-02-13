class AgentNode:

    def __init__(self, name, runner):
        self.name = name
        self.runner = runner

    def execute(self, task):
        result = self.runner.run(task)
        return result
