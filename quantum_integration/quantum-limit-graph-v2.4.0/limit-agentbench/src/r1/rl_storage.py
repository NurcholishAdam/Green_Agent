import json
import os

QTABLE_FILE = "rl/qtable.json"


class RLStorage:

    def save(self, q_table):
        with open(QTABLE_FILE, "w") as f:
            json.dump(q_table, f)

    def load(self):
        if not os.path.exists(QTABLE_FILE):
            return {}
        with open(QTABLE_FILE, "r") as f:
            return json.load(f)
