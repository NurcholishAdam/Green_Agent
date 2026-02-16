import torch
import torch.nn as nn
import torch.optim as optim


class PPOAgent(nn.Module):

    def __init__(self, state_dim, action_dim, lr=3e-4):
        super().__init__()

        self.actor = nn.Sequential(
            nn.Linear(state_dim, 128),
            nn.ReLU(),
            nn.Linear(128, action_dim),
            nn.Softmax(dim=-1)
        )

        self.critic = nn.Sequential(
            nn.Linear(state_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 1)
        )

        self.optimizer = optim.Adam(self.parameters(), lr=lr)
        self.clip_epsilon = 0.2

    def forward(self, state):
        policy = self.actor(state)
        value = self.critic(state)
        return policy, value

    def compute_loss(
        self,
        states,
        actions,
        old_log_probs,
        returns,
        advantages
    ):

        policy, values = self.forward(states)

        dist = torch.distributions.Categorical(policy)
        new_log_probs = dist.log_prob(actions)

        ratio = torch.exp(new_log_probs - old_log_probs)

        clipped_ratio = torch.clamp(
            ratio,
            1 - self.clip_epsilon,
            1 + self.clip_epsilon
        )

        actor_loss = -torch.min(
            ratio * advantages,
            clipped_ratio * advantages
        ).mean()

        critic_loss = nn.MSELoss()(values.squeeze(), returns)

        entropy = dist.entropy().mean()

        total_loss = actor_loss + 0.5 * critic_loss - 0.01 * entropy

        return total_loss
