"""
Policy Feedback

Produces reflective, human-readable explanations.
"""

class PolicyFeedback:
    def explain(self, decision: str, before: dict, after: dict) -> dict:
        return {
            "decision": decision,
            "explanation": (
                f"I chose '{decision}' because it reduced energy "
                f"from {before.get('energy')} to {after.get('energy')} "
                f"while increasing latency by "
                f"{after.get('latency', 0) - before.get('latency', 0):.2f}s."
            ),
            "tradeoff": {
                "energy_delta": after.get("energy", 0) - before.get("energy", 0),
                "latency_delta": after.get("latency", 0) - before.get("latency", 0),
            },
            "confidence": 0.7,
        }
