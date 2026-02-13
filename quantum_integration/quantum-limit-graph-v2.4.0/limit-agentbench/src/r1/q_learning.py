import random
from rl.rl_storage import RLStorage


class QLearningAgent:

    def __init__(self, alpha=0.1, gamma=0.9, epsilon=0.1):
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon

        self.storage = RLStorage()
        self.q_table = self.storage.load()

        self.actions = ["balanced", "energy_saver", "latency_optimized"]

    def choose_action(self, state):
        if random.random() < self.epsilon:
            return random.choice(self.actions)

        if state not in self.q_table:
            self.q_table[state] = {a: 0 for a in self.actions}

        return max(self.q_table[state], key=self.q_table[state].get)

    def update(self, state, action, reward, next_state):
        if state not in self.q_table:
            self.q_table[state] = {a: 0 for a in self.actions}

        if next_state not in self.q_table:
            self.q_table[next_state] = {a: 0 for a in self.actions}

        best_next = max(self.q_table[next_state].values())

        self.q_table[state][action] += self.alpha * (
            reward + self.gamma * best_next - self.q_table[state][action]
        )

        self.storage.save(self.q_table)
