class CentralCoordinator:

    def __init__(self):
        self.global_metrics = []

    def register_agent_result(self, agent_id, accuracy, energy, reward):

        self.global_metrics.append({
            "agent": agent_id,
            "accuracy": accuracy,
            "energy": energy,
            "reward": reward
        })

    def compute_global_sustainability_rank(self):

        ranked = sorted(
            self.global_metrics,
            key=lambda x: x["accuracy"] / x["energy"],
            reverse=True
        )

        return ranked
