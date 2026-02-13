class PolicyFeedback:
    """
    Generates reflective explanations.
    """

    def generate(self, decision, before, after):
        return {
            "decision": decision,
            "explanation": (
                f"I selected mode '{decision}' because energy shifted "
                f"from {before.get('energy')} to {after.get('energy')}."
            ),
            "tradeoff": {
                "latency_delta": after.get("latency", 0) - before.get("latency", 0),
                "energy_delta": after.get("energy", 0) - before.get("energy", 0),
            },
            "confidence": 0.75
        }
