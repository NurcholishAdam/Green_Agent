import json
import time
from typing import Dict


class PolicyFeedback:
    """
    Emits human-readable and machine-readable policy feedback.
    Safe for enterprise audit logs.
    """

    def emit(
        self,
        run_id: str,
        verdict: Dict,
        policy_hash: str,
        framework: str,
    ) -> None:
        payload = {
            "timestamp": int(time.time()),
            "run_id": run_id,
            "policy_hash": policy_hash,
            "framework": framework,
            "compliant": verdict["compliant"],
            "violations": verdict["violations"],
        }

        # Always print — AgentBeats captures stdout safely
        print("[POLICY_FEEDBACK]")
        print(json.dumps(payload, indent=2))
