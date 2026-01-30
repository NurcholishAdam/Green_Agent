import yaml
import hashlib
from typing import Dict


class GreenPolicy:
    def __init__(self, raw: Dict):
        self.raw = raw
        self.version = raw.get("version", "unknown")
        self.constraints = raw.get("constraints", {})
        self.carbon = raw.get("carbon_context", {})
        self.optimization = raw.get("optimization", {})
        self.reporting = raw.get("reporting", {})
        self.identity = raw.get("agent_identity", {})

    @property
    def hash(self) -> str:
        h = hashlib.sha256()
        h.update(yaml.dump(self.raw).encode())
        return f"sha256:{h.hexdigest()}"


def load_policy(path: str) -> GreenPolicy:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return GreenPolicy(data)
