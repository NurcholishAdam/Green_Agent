# Proposed src/memory/supermemory_adapter.py

from supermemory import Supermemory

class GreenMemoryAdapter:
    """Adapter between Green Agent and Supermemory."""
    
    def __init__(self, api_key: str = None, base_url: str = "http://localhost:6767"):
        self.client = Supermemory(apiKey=api_key, baseURL=base_url)
    
    def remember_decision(self, decision: dict, container_tag: str):
        """Store a completed routing decision."""
        self.client.add(content=decision["content"], containerTag=container_tag)
    
    def recall_similar_runs(self, workload_type: str, device_class: str, k: int = 5):
        """Retrieve comparable historical runs."""
        return self.client.search({
            "q": f"{workload_type} {device_class}",
            "containerTag": "org:green-agent",
            "searchMode": "hybrid",
            "limit": k,
        })
    
    def remember_policy(self, policy: dict, version: str):
        """Store an approved policy."""
        self.client.add(
            content=json.dumps(policy),
            containerTag=f"policy:{version}",
        )
    
    def record_outcome(self, run_id: str, predicted: dict, measured: dict):
        """Record the outcome of a run for feedback learning."""
        self.client.add(
            content=f"Run {run_id}: predicted={predicted}, measured={measured}",
            containerTag=f"run:{run_id}",
        )
