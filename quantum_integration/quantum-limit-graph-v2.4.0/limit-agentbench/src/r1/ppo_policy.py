import torch
import torch.nn as nn
import torch.optim as optim
import torch.distributions as dist


class PPOPolicy(nn.Module):

    def __init__(self, state_dim=3, action_dim=3):
        super().__init__()

        self.fc = nn.Sequential(
            nn.Linear(state_dim, 64),
            nn.ReLU(),
            nn.Linear(64, action_dim)
        )

        self.optimizer = optim.Adam(self.parameters(), lr=0.001)

    def forward(self, state):
        return self.fc(state)

    def select_action(self, state):
        logits = self.forward(state)
        probs = torch.softmax(logits, dim=-1)
        m = dist.Categorical(probs)
        action = m.sample()
        return action.item(), m.log_prob(action)

    def update(self, loss):
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
