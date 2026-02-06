# src/analysis/self_monitor.py

import time
import psutil

class SelfMonitor:
    """
    Internal introspection layer.
    Tracks agent-side resource usage trends in real time.
    """

    def __init__(self):
        self.start_time = time.time()
        self.snapshots = []

    def snapshot(self, tool_calls=0, conversation_depth=0):
        process = psutil.Process()
        snap = {
            "t": time.time() - self.start_time,
            "cpu_pct": process.cpu_percent(),
            "mem_mb": process.memory_info().rss / 1024**2,
            "tool_calls": tool_calls,
            "conversation_depth": conversation_depth,
        }
        self.snapshots.append(snap)
        return snap

    def trend(self):
        if len(self.snapshots) < 2:
            return {}
        a, b = self.snapshots[-2], self.snapshots[-1]
        return {
            "cpu_delta": b["cpu_pct"] - a["cpu_pct"],
            "mem_delta": b["mem_mb"] - a["mem_mb"],
        }
