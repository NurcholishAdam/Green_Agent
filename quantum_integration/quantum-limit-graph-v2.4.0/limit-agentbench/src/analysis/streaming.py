import time
import json


class MetricsStreamer:
    def __init__(self, interval=5):
        self.interval = interval
        self.last = time.time()

    def heartbeat(self, payload):
        now = time.time()
        if now - self.last >= self.interval:
            print("[HEARTBEAT]", json.dumps(payload))
            self.last = now
