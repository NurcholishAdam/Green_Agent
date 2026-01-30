import json
import time


class PolicyFeedback:
    def emit(self, run_id, verdict):
        print("[POLICY]",
              json.dumps({
                  "run_id": run_id,
                  "time": int(time.time()),
                  "verdict": verdict
              }))
