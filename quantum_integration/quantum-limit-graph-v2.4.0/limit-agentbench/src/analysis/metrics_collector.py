"""
Collects core execution metrics like CPU time and memory.
Also integrates counters for tool calls and reasoning depth.
"""

import os
import time
import psutil
from typing import Dict

class MetricsCollector:
    def __init__(self):
        self.start = time.time()
        self.tool_calls = 0
        self.conversation_depth = 0

    def record_tool(self) -> None:
        self.tool_calls += 1

    def record_depth(self, depth: int) -> None:
        self.conversation_depth = max(self.conversation_depth, depth)

    def finalize(self) -> Dict[str, float]:
        info = psutil.Process().memory_info()
        latency = time.time() - self.start
        return {
            "latency_s": latency,
            "memory_mb": info.rss / (1024**2),
            "tool_calls": self.tool_calls,
            "conversation_depth": self.conversation_depth,
        }
