import os
import json


class AgentRegistry:

    def __init__(self, base_path="metrics"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def register(self, agent_id, metrics):
        path = os.path.join(
            self.base_path,
            f"agent_{agent_id}_latest.json"
        )

        temp_path = path + ".tmp"

        with open(temp_path, "w") as f:
            json.dump(metrics, f)

        os.replace(temp_path, path)

    def aggregate(self):
        results = []
        for file in os.listdir(self.base_path):
            if file.endswith(".json"):
                with open(os.path.join(self.base_path, file)) as f:
                    results.append(json.load(f))
        return results
