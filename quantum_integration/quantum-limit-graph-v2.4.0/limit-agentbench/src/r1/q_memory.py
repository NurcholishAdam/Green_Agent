import json
import os


class QMemory:

    def __init__(self, path="q_table.json"):
        self.path = path
        self.q_table = self.load()

    def load(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                return json.load(f)
        return {}

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.q_table, f)

    def update(self, state, action, reward, alpha=0.1):

        state = str(state)
        if state not in self.q_table:
            self.q_table[state] = {}

        old_value = self.q_table[state].get(str(action), 0.0)

        new_value = old_value + alpha * (reward - old_value)

        self.q_table[state][str(action)] = new_value

    def get_bias(self, state, action):
        return self.q_table.get(str(state), {}).get(str(action), 0.0)
