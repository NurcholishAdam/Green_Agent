# src/r1/ppo_agent.py

"""
PPO Agent
=========

Proximal Policy Optimization (PPO-Clip) agent for continuous or discrete
control of energy-aware policies.

Enhancements
------------
- Optional ``torch`` import guarded by ``_TORCH_AVAILABLE``. When PyTorch is
  not installed, the module still imports and a clear error is raised only
  on construction.
- ``PPOAgentConfig`` — frozen, validated: hidden size, learning rate, clip
  epsilon, value coefficient, entropy coefficient, max grad norm, target KL.
- **Fixed the double-softmax bug** — the actor now returns logits and every
  distribution uses ``Categorical(logits=...)``.
- **Fixed the value-loss shape bug** — ``values.squeeze(-1)``.
- **Value clipping** — PPO-style value loss clipping.
- ``get_action`` / ``evaluate_actions`` helpers.
- ``compute_loss`` returns both the loss tensor and a diagnostic dict
  (actor / critic / entropy / approx-KL / clip fraction).
- Optional gradient clipping helper ``update(...)``.
- Configurable entropy coefficient with linear annealing.
- Serialization of hyperparameters and state dict.
- ``statistics()``, ``__repr__``, custom ``PPOAgentError``, lazy ``%s``
  logging, and a ``__main__`` smoke test that auto-skips when PyTorch is
  absent.
"""

from __future__ import annotations

import json
import logging
import math
import threading
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional PyTorch import
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import torch  # type: ignore
    import torch.nn as nn  # type: ignore
    import torch.optim as optim  # type: ignore
    from torch.distributions import Categorical  # type: ignore

    _TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore[assignment]
    nn = None      # type: ignore[assignment]
    optim = None   # type: ignore[assignment]
    Categorical = None  # type: ignore[assignment]
    _TORCH_AVAILABLE = False
    logger.debug("torch not importable; PPOAgent disabled.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PPOAgentError(ValueError):
    """Raised for invalid PPO agent inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PPOAgentConfig:
    """Tunable parameters for the PPO agent."""

    # Architecture.
    hidden_dim: int = 128

    # Optimization.
    lr: float = 3e-4
    clip_epsilon: float = 0.2
    value_coef: float = 0.5
    entropy_coef: float = 0.01
    entropy_coef_min: float = 0.0
    entropy_coef_decay: float = 1.0      # 1.0 = no decay
    max_grad_norm: Optional[float] = 0.5  # None disables clipping
    target_kl: Optional[float] = 0.015    # None disables KL early stop

    # Value loss clipping (PPO2).
    value_clip_epsilon: Optional[float] = 0.2

    # Numerical stability.
    log_prob_epsilon: float = 1e-8

    def __post_init__(self) -> None:
        if not isinstance(self.hidden_dim, int) or self.hidden_dim <= 0:
            raise PPOAgentError("hidden_dim must be a positive int.")
        if self.lr <= 0:
            raise PPOAgentError("lr must be > 0.")
        if not 0.0 < self.clip_epsilon < 1.0:
            raise PPOAgentError("clip_epsilon must be in (0, 1).")
        if self.value_coef < 0:
            raise PPOAgentError("value_coef must be >= 0.")
        if self.entropy_coef < 0:
            raise PPOAgentError("entropy_coef must be >= 0.")
        if self.entropy_coef_min < 0:
            raise PPOAgentError("entropy_coef_min must be >= 0.")
        if not 0.0 < self.entropy_coef_decay <= 1.0:
            raise PPOAgentError("entropy_coef_decay must be in (0, 1].")
        if self.entropy_coef_min > self.entropy_coef:
            raise PPOAgentError(
                "entropy_coef_min must be <= entropy_coef."
            )
        if self.max_grad_norm is not None and self.max_grad_norm <= 0:
            raise PPOAgentError("max_grad_norm must be > 0 or None.")
        if self.target_kl is not None and self.target_kl <= 0:
            raise PPOAgentError("target_kl must be > 0 or None.")
        if (
            self.value_clip_epsilon is not None
            and self.value_clip_epsilon <= 0
        ):
            raise PPOAgentError(
                "value_clip_epsilon must be > 0 or None."
            )
        if self.log_prob_epsilon <= 0:
            raise PPOAgentError("log_prob_epsilon must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PPOAgentConfig":
        if not isinstance(data, Mapping):
            raise PPOAgentError(
                f"PPOAgentConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            hidden_dim=int(data.get("hidden_dim", 128)),
            lr=float(data.get("lr", 3e-4)),
            clip_epsilon=float(data.get("clip_epsilon", 0.2)),
            value_coef=float(data.get("value_coef", 0.5)),
            entropy_coef=float(data.get("entropy_coef", 0.01)),
            entropy_coef_min=float(data.get("entropy_coef_min", 0.0)),
            entropy_coef_decay=float(data.get("entropy_coef_decay", 1.0)),
            max_grad_norm=data.get("max_grad_norm", 0.5),
            target_kl=data.get("target_kl", 0.015),
            value_clip_epsilon=data.get("value_clip_epsilon", 0.2),
            log_prob_epsilon=float(data.get("log_prob_epsilon", 1e-8)),
        )


# --------------------------------------------------------------------------- #
# Torch-optional module base
# --------------------------------------------------------------------------- #
_ModuleBase = nn.Module if _TORCH_AVAILABLE else object  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# PPO agent
# --------------------------------------------------------------------------- #
class PPOAgent(_ModuleBase):  # type: ignore[misc,valid-type]
    """
    PPO-Clip agent with an actor-critic architecture.

    Thread-safe with respect to configuration reads; training calls should be
    serialized by the caller (standard for a single optimizer).

    Parameters
    ----------
    state_dim : int
        Observation dimensionality.
    action_dim : int
        Number of discrete actions.
    lr : float, default 3e-4
        Learning rate (kept positional for backward compatibility).
    config : PPOAgentConfig, optional
        Typed configuration. Overrides ``lr`` when provided.
    strict : bool, default True
        If True, invalid inputs raise :class:`PPOAgentError`.

    Notes
    -----
    ``forward`` returns **logits**, not probabilities. This is the correct
    PPO behavior — the caller (or :meth:`compute_loss`) applies
    ``Categorical(logits=...)`` which handles normalization internally.
    Use :meth:`policy_probs` if you need explicit probabilities.
    """

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        lr: float = 3e-4,
        *,
        config: Optional[PPOAgentConfig] = None,
        strict: bool = True,
    ) -> None:
        if not _TORCH_AVAILABLE:
            raise PPOAgentError(
                "PyTorch is not installed; PPOAgent is unavailable. "
                "Install torch or use EnergyRLAgent / QLearningAgent instead."
            )
        if not isinstance(state_dim, int) or state_dim <= 0:
            raise PPOAgentError("state_dim must be a positive int.")
        if not isinstance(action_dim, int) or action_dim <= 0:
            raise PPOAgentError("action_dim must be a positive int.")

        super().__init__()

        if config is not None:
            self._config = config
        else:
            self._config = PPOAgentConfig(lr=float(lr))
        self._strict = bool(strict)

        # Legacy attributes preserved for backward compatibility.
        self.clip_epsilon: float = self._config.clip_epsilon
        self.state_dim: int = state_dim
        self.action_dim: int = action_dim

        # ---- Networks ------------------------------------------------
        # NOTE: the actor returns *logits* (no softmax); see class docstring.
        self.actor = nn.Sequential(
            nn.Linear(state_dim, self._config.hidden_dim),
            nn.ReLU(),
            nn.Linear(self._config.hidden_dim, action_dim),
            # intentionally no softmax — Categorical(logits=...) applies it
        )
        self.critic = nn.Sequential(
            nn.Linear(state_dim, self._config.hidden_dim),
            nn.ReLU(),
            nn.Linear(self._config.hidden_dim, 1),
        )

        self.optimizer = optim.Adam(self.parameters(), lr=self._config.lr)

        # Cached loss function to avoid per-call allocations.
        self._mse = nn.MSELoss(reduction="mean")

        # Internal counters.
        self._lock = threading.RLock()
        self._updates: int = 0
        self._entropy_coef: float = self._config.entropy_coef

        logger.debug(
            "PPOAgent initialized "
            "(state_dim=%d, action_dim=%d, hidden=%d, lr=%.1e, "
            "clip=%.3f, value_coef=%.3f, entropy_coef=%.4f)",
            state_dim, action_dim, self._config.hidden_dim,
            self._config.lr, self._config.clip_epsilon,
            self._config.value_coef, self._config.entropy_coef,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> PPOAgentConfig:
        return self._config

    @property
    def entropy_coef(self) -> float:
        with self._lock:
            return self._entropy_coef

    @property
    def updates(self) -> int:
        with self._lock:
            return self._updates

    # ---------------------------------------------------------- forward
    def forward(self, state: Any) -> Tuple[Any, Any]:
        """
        Run the actor and critic networks.

        Returns
        -------
        (logits, value)
            ``logits`` are unnormalized action scores (shape
            ``[..., action_dim]``); ``value`` is the state-value estimate
            (shape ``[..., 1]``).
        """
        logits = self.actor(state)
        value = self.critic(state)
        return logits, value

    def policy_probs(self, state: Any) -> Any:
        """Return the softmax-normalized action probabilities."""
        logits, _ = self.forward(state)
        return torch.softmax(logits, dim=-1)

    # ---------------------------------------------------------- action selection
    def get_action(
        self, state: Any, *, deterministic: bool = False
    ) -> Tuple[int, float, float]:
        """
        Sample (or greedily select) an action for a single state.

        Parameters
        ----------
        state : tensor
            A single state, shape ``[state_dim]`` or ``[1, state_dim]``.
        deterministic : bool, default False
            If True, return the argmax action instead of sampling.

        Returns
        -------
        (action, log_prob, value)
            ``action`` is an int, ``log_prob`` and ``value`` are floats.
        """
        if not _TORCH_AVAILABLE:  # pragma: no cover — guarded in __init__
            raise PPOAgentError("PyTorch is not available.")

        with torch.no_grad():
            state_t = state if torch.is_tensor(state) else torch.as_tensor(state, dtype=torch.float32)
            if state_t.dim() == 1:
                state_t = state_t.unsqueeze(0)
            logits, value = self.forward(state_t)
            dist = Categorical(logits=logits)
            if deterministic:
                action = torch.argmax(logits, dim=-1)
            else:
                action = dist.sample()
            log_prob = dist.log_prob(action)
            action_int = int(action.item())
            log_prob_f = float(log_prob.item())
            value_f = float(value.squeeze(-1).item())
        return action_int, log_prob_f, value_f

    # ---------------------------------------------------------- evaluation
    def evaluate_actions(
        self, states: Any, actions: Any
    ) -> Tuple[Any, Any, Any]:
        """
        Evaluate a batch of ``(states, actions)``.

        Returns
        -------
        (log_probs, values, entropy)
        """
        logits, values = self.forward(states)
        dist = Categorical(logits=logits)
        log_probs = dist.log_prob(actions)
        entropy = dist.entropy()
        return log_probs, values.squeeze(-1), entropy

    # ---------------------------------------------------------- loss
    def compute_loss(
        self,
        states: Any,
        actions: Any,
        old_log_probs: Any,
        returns: Any,
        advantages: Any,
        *,
        old_values: Optional[Any] = None,
        return_diagnostics: bool = False,
    ) -> Any:
        """
        Compute the PPO-Clip loss.

        Parameters
        ----------
        states : tensor
            Batch of observations.
        actions : tensor
            Batch of actions taken under the old policy.
        old_log_probs : tensor
            Log-probabilities of ``actions`` under the old policy.
        returns : tensor
            Target returns (``[batch]`` or ``[batch, 1]``).
        advantages : tensor
            Advantage estimates (same shape as ``returns``).
        old_values : tensor, optional
            Value predictions under the old policy, for value clipping.
        return_diagnostics : bool, default False
            If True, return ``(loss, diagnostics_dict)`` instead of the loss.

        Returns
        -------
        loss : tensor  — or ``(loss, diagnostics)`` when
        ``return_diagnostics`` is True.
        """
        cfg = self._config

        # ---- Normalize shapes defensively ------------------------------
        returns = self._as_flat(returns)
        advantages = self._as_flat(advantages)
        old_log_probs = self._as_flat(old_log_probs)

        # ---- Current policy evaluation ---------------------------------
        log_probs, values, entropy = self.evaluate_actions(states, actions)

        # ---- PPO-Clip actor loss ---------------------------------------
        ratio = torch.exp(log_probs - old_log_probs)
        clipped_ratio = torch.clamp(
            ratio,
            1.0 - cfg.clip_epsilon,
            1.0 + cfg.clip_epsilon,
        )
        actor_loss = -torch.min(
            ratio * advantages,
            clipped_ratio * advantages,
        ).mean()

        # ---- Critic loss (with optional value clipping) -----------------
        if old_values is not None and cfg.value_clip_epsilon is not None:
            old_values = self._as_flat(old_values)
            clipped_values = old_values + torch.clamp(
                values - old_values,
                -cfg.value_clip_epsilon,
                cfg.value_clip_epsilon,
            )
            critic_loss = torch.max(
                self._mse(values, returns),
                self._mse(clipped_values, returns),
            )
        else:
            critic_loss = self._mse(values, returns)

        # ---- Entropy bonus ---------------------------------------------
        with self._lock:
            entropy_coef = self._entropy_coef
        entropy_term = entropy.mean()

        total_loss = (
            actor_loss
            + cfg.value_coef * critic_loss
            - entropy_coef * entropy_term
        )

        if return_diagnostics:
            with torch.no_grad():
                approx_kl = ((old_log_probs - log_probs) ** 2).mean().item() / 2.0
                clip_fraction = (
                    (torch.abs(ratio - 1.0) > cfg.clip_epsilon)
                    .float()
                    .mean()
                    .item()
                )
            diagnostics = {
                "actor_loss": float(actor_loss.item()),
                "critic_loss": float(critic_loss.item()),
                "entropy": float(entropy_term.item()),
                "entropy_coef": float(entropy_coef),
                "approx_kl": float(approx_kl),
                "clip_fraction": float(clip_fraction),
                "mean_ratio": float(ratio.mean().item()),
                "mean_value": float(values.mean().item()),
                "mean_return": float(returns.mean().item()),
            }
            return total_loss, diagnostics

        return total_loss

    @staticmethod
    def _as_flat(tensor: Any) -> Any:
        """Return a 1-D tensor view — fixes the ``values.squeeze()`` bug."""
        if tensor is None:  # pragma: no cover — callers guard
            return tensor
        if tensor.dim() == 2 and tensor.size(-1) == 1:
            return tensor.squeeze(-1)
        return tensor

    # ---------------------------------------------------------- training step
    def update(
        self,
        states: Any,
        actions: Any,
        old_log_probs: Any,
        returns: Any,
        advantages: Any,
        *,
        old_values: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        One full optimization step: zero grad, compute loss, backward,
        clip gradients, optimizer step, and decay entropy coefficient.

        Returns a diagnostics dictionary (see :meth:`compute_loss`).
        """
        cfg = self._config
        self.optimizer.zero_grad(set_to_none=True)

        loss, diagnostics = self.compute_loss(
            states, actions, old_log_probs, returns, advantages,
            old_values=old_values,
            return_diagnostics=True,
        )
        loss.backward()

        if cfg.max_grad_norm is not None:
            grad_norm = torch.nn.utils.clip_grad_norm_(
                self.parameters(), cfg.max_grad_norm
            )
            diagnostics["grad_norm"] = float(grad_norm.item())

        self.optimizer.step()

        with self._lock:
            self._updates += 1
            if cfg.entropy_coef_decay < 1.0:
                self._entropy_coef = max(
                    cfg.entropy_coef_min,
                    self._entropy_coef * cfg.entropy_coef_decay,
                )
            diagnostics["update_index"] = self._updates

        # Optional KL early-stop signal for the training loop.
        if cfg.target_kl is not None and diagnostics["approx_kl"] > cfg.target_kl:
            diagnostics["early_stop"] = True
        else:
            diagnostics["early_stop"] = False

        logger.debug(
            "PPO update #%d: loss=%.4f actor=%.4f critic=%.4f entropy=%.4f "
            "kl=%.5f clip_frac=%.3f grad_norm=%s early_stop=%s",
            self._updates,
            float(loss.item()),
            diagnostics["actor_loss"],
            diagnostics["critic_loss"],
            diagnostics["entropy"],
            diagnostics["approx_kl"],
            diagnostics["clip_fraction"],
            f"{diagnostics.get('grad_norm', 0.0):.4f}",
            diagnostics["early_stop"],
        )
        return diagnostics

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the agent configuration and counters."""
        return {
            "state_dim": self.state_dim,
            "action_dim": self.action_dim,
            "hidden_dim": self._config.hidden_dim,
            "lr": self._config.lr,
            "clip_epsilon": self._config.clip_epsilon,
            "value_coef": self._config.value_coef,
            "entropy_coef": self.entropy_coef,
            "max_grad_norm": self._config.max_grad_norm,
            "target_kl": self._config.target_kl,
            "updates": self.updates,
            "torch_available": _TORCH_AVAILABLE,
            "parameter_count": sum(
                p.numel() for p in self.parameters()
            ) if _TORCH_AVAILABLE else 0,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self._config.to_dict(),
            "state_dim": self.state_dim,
            "action_dim": self.action_dim,
            "strict": self._strict,
            "entropy_coef": self.entropy_coef,
            "updates": self.updates,
            "torch_available": _TORCH_AVAILABLE,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        load_state: Optional[Mapping[str, Any]] = None,
    ) -> "PPOAgent":
        """Rebuild a PPOAgent from a config dict (weights are not serialized)."""
        if not isinstance(data, Mapping):
            raise PPOAgentError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = PPOAgentConfig.from_dict(dict(data.get("config", {}) or {}))
        agent = cls(
            state_dim=int(data["state_dim"]),
            action_dim=int(data["action_dim"]),
            config=cfg,
            strict=bool(data.get("strict", True)) if strict is None else strict,
        )
        with agent._lock:
            agent._entropy_coef = float(
                data.get("entropy_coef", cfg.entropy_coef)
            )
            agent._updates = int(data.get("updates", 0))
        if load_state is not None:
            agent.load_state(load_state)
        return agent

    # ---------------------------------------------------------- weights
    def save_state(self) -> Dict[str, Any]:
        """Return the state dict (weights + optimizer) for checkpointing."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOAgentError("PyTorch is not available.")
        return {
            "model": self.state_dict(),
            "optimizer": self.optimizer.state_dict(),
        }

    def load_state(self, checkpoint: Mapping[str, Any]) -> None:
        """Restore model + optimizer weights from ``save_state`` output."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOAgentError("PyTorch is not available.")
        if not isinstance(checkpoint, Mapping):
            raise PPOAgentError("checkpoint must be a Mapping.")
        if "model" in checkpoint:
            self.load_state_dict(checkpoint["model"])
        if "optimizer" in checkpoint and checkpoint["optimizer"] is not None:
            self.optimizer.load_state_dict(checkpoint["optimizer"])
        logger.debug("PPOAgent state restored from checkpoint.")

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PPOAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "PPOAgent scope exited with %s.", exc_type.__name__
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "PPOAgent("
            f"state_dim={self.state_dim}, "
            f"action_dim={self.action_dim}, "
            f"hidden={self._config.hidden_dim}, "
            f"lr={self._config.lr:.1e}, "
            f"clip={self._config.clip_epsilon:.2f}, "
            f"entropy_coef={self.entropy_coef:.4f}, "
            f"updates={self._updates})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "PPOAgent",
    "PPOAgentConfig",
    "PPOAgentError",
    "_TORCH_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.ppo_agent
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    if not _TORCH_AVAILABLE:
        print("PyTorch is not installed; skipping PPOAgent smoke test.")
    else:
        torch.manual_seed(0)

        # ---- Happy path ---------------------------------------------------- #
        agent = PPOAgent(state_dim=4, action_dim=3, lr=3e-4)
        print("repr       :", agent)

        # Get an action for a random state.
        state = torch.randn(4)
        action, log_prob, value = agent.get_action(state)
        print(f"action     : {action}, log_prob={log_prob:.4f}, value={value:.4f}")

        # Deterministic selection.
        det_action, _, _ = agent.get_action(state, deterministic=True)
        print("deterministic:", det_action)

        # ---- Batch training step ----------------------------------------- #
        B = 64
        states = torch.randn(B, 4)
        actions = torch.randint(0, 3, (B,))
        old_log_probs, old_values, _ = agent.evaluate_actions(states, actions)
        old_log_probs = old_log_probs.detach()
        old_values = old_values.detach()
        returns = torch.randn(B)
        advantages = torch.randn(B)

        diagnostics = agent.update(
            states, actions, old_log_probs, returns, advantages,
            old_values=old_values,
        )
        print("diagnostics:", {
            k: (round(v, 5) if isinstance(v, float) else v)
            for k, v in diagnostics.items()
        })

        # ---- Double-softmax proof ---------------------------------------- #
        # The actor's raw output should NOT sum to 1 (it's logits).
        logits, _ = agent.forward(states[:1])
        print("logits sum :", float(logits.sum().item()),
              "(should NOT be 1.0 — the fix)")
        probs = agent.policy_probs(states[:1])
        print("probs sum  :", float(probs.sum().item()),
              "(should be ~1.0)")

        # ---- Multiple training steps ------------------------------------- #
        for step in range(5):
            agent.update(
                states, actions, old_log_probs, returns, advantages,
                old_values=old_values,
            )
        print("after 5    :", agent.statistics())

        # ---- Checkpoint round-trip --------------------------------------- #
        checkpoint = agent.save_state()
        agent2 = PPOAgent(state_dim=4, action_dim=3)
        agent2.load_state(checkpoint)
        # Verify identical outputs.
        with torch.no_grad():
            l1, _ = agent.forward(states[:1])
            l2, _ = agent2.forward(states[:1])
        assert torch.allclose(l1, l2), "checkpoint restore failed"
        print("checkpoint : OK")

        # ---- Serialization ----------------------------------------------- #
        payload = agent.to_json()
        restored = PPOAgent.from_dict(json.loads(payload))
        assert restored.to_dict() == agent.to_dict()
        print("Round-trip : OK")

        # ---- Config validation ------------------------------------------- #
        for bad_cfg in (
            dict(hidden_dim=0),
            dict(lr=0),
            dict(clip_epsilon=0),
            dict(clip_epsilon=1.5),
            dict(value_coef=-0.1),
            dict(entropy_coef=-0.1),
            dict(entropy_coef_decay=0),
            dict(max_grad_norm=-0.5),
            dict(target_kl=-0.1),
            dict(value_clip_epsilon=-0.1),
        ):
            try:
                PPOAgentConfig(**bad_cfg)  # type: ignore[arg-type]
            except PPOAgentError as exc:
                print("Rejected cfg:", exc)

        # ---- Input validation -------------------------------------------- #
        for bad_call in (
            lambda: PPOAgent(state_dim=0, action_dim=3),
            lambda: PPOAgent(state_dim=4, action_dim=0),
        ):
            try:
                bad_call()
            except PPOAgentError as exc:
                print("Rejected   :", exc)

        # ---- Entropy decay ----------------------------------------------- #
        decaying = PPOAgent(
            state_dim=4, action_dim=3,
            config=PPOAgentConfig(entropy_coef=0.1, entropy_coef_decay=0.9,
                                   entropy_coef_min=0.001),
        )
        start_e = decaying.entropy_coef
        for _ in range(20):
            decaying.update(
                states, actions, old_log_probs, returns, advantages,
            )
        print(f"entropy    : {start_e:.4f} → {decaying.entropy_coef:.4f}")

        print("\nSmoke test passed.")
