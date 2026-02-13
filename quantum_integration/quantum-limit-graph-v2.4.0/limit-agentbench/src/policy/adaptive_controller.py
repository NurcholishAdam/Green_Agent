class AdaptiveController:
    """
    Adjust runtime strategy dynamically.
    """

    def __init__(self):
        self.mode = "normal"

    def evaluate(self, trend):
        if trend.get("memory_delta", 0) > 25:
            self.mode = "low_memory"
        elif trend.get("cpu_delta", 0) > 15:
            self.mode = "low_energy"
        else:
            self.mode = "normal"
        return self.mode

    def apply(self, runtime):
        if self.mode == "low_energy":
            runtime.reduce_tool_calls()
        elif self.mode == "low_memory":
            runtime.shorten_context()
