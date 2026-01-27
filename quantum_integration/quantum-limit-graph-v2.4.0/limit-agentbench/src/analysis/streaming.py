# src/analysis/streaming.py

import json
import sys
import time
from typing import Dict

class MetricsStreamer:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def emit(self, event: str, payload: Dict):
        if not self.enabled:
            return

        msg = {
            "event": event,
            "timestamp": time.time(),
            "payload": payload
        }
        print(json.dumps(msg), file=sys.stdout, flush=True)
