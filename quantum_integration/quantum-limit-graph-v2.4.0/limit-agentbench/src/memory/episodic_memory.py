import json
import os
from typing import Dict, List

MEMORY_FILE = "memory/memory_store.json"


class EpisodicMemory:

    def __init__(self):
        if not os.path.exists(MEMORY_FILE):
            with open(MEMORY_FILE, "w") as f:
                json.dump([], f)

    def store(self, episode: Dict):
        memory = self.load_all()
        memory.append(episode)

        with open(MEMORY_FILE, "w") as f:
            json.dump(memory[-1000:], f, indent=4)  # Keep last 1000

    def load_all(self) -> List[Dict]:
        with open(MEMORY_FILE, "r") as f:
            return json.load(f)

    def get_recent(self, n=10):
        return self.load_all()[-n:]
