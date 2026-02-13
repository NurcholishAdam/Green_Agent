import time
import psutil

class SelfMonitor:
    """
    Internal meta-cognitive introspection.
    Tracks CPU, memory, tool usage, and depth over time.
    """

    def __init__(self):
        self.start_time = time.time()
        self.snapshots = []

    def snapshot(self, tool_calls=0, depth=0):
        process = psutil.Process()
        snap = {
            "elapsed": time.time() - self.start_time,
            "cpu_percent": process.cpu_percent(interval=None),
            "memory_mb": process.memory_info().rss / 1024**2,
            "tool_calls": tool_calls,
            "conversation_depth": depth,
        }
        self.snapshots.append(snap)
        return snap

    def last_trend(self):
        if len(self.snapshots) < 2:
            return {}
        prev, curr = self.snapshots[-2], self.snapshots[-1]
        return {
            "cpu_delta": curr["cpu_percent"] - prev["cpu_percent"],
            "memory_delta": curr["memory_mb"] - prev["memory_mb"],
        }
