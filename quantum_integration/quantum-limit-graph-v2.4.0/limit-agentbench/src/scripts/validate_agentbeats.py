import json
import sys
import subprocess

with open("agentbeats.json") as f:
    data = json.load(f)

assert isinstance(data["queries"], list), "queries must be array"

for q in data["queries"]:
    assert isinstance(q["command"], list), "command must be array"
    assert isinstance(q.get("environment", {}), dict)

image = data["image"]
subprocess.run(["docker", "manifest", "inspect", image], check=True)

print("✅ AgentBeats validation passed")
