"""
Human-readable feedback based on energy efficiency.
"""

def generate_energy_feedback(metrics: dict) -> str:
    energy = metrics.get("energy", 0.0)
    latency = metrics.get("latency", 0.0)
    memory = metrics.get("memory", 0.0)

    messages = []

    if energy > 0.1:
        messages.append("High energy usage detected.")

    if latency > 2.0:
        messages.append("Latency may impact user experience.")

    if memory > 1024:
        messages.append("Large memory footprint observed.")

    if not messages:
        return "Run is efficient and within green thresholds."

    return " ".join(messages)
