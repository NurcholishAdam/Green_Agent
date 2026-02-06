# src/policy/adaptive_controller.py

class AdaptiveController:
    """
    Adjusts agent behavior based on self-monitor signals.
    """

    def __init__(self, policy):
        self.policy = policy
        self.mode = "normal"

    def evaluate(self, monitor_trend):
        if monitor_trend.get("mem_delta", 0) > self.policy.memory_soft_limit_mb:
            self.mode = "low_memory"
        if monitor_trend.get("cpu_delta", 0) > self.policy.cpu_soft_limit:
            self.mode = "low_energy"
        return self.mode

    def apply(self, runtime):
        if self.mode == "low_energy":
            runtime.reduce_tool_calls()
        elif self.mode == "low_memory":
            runtime.shorten_context()
