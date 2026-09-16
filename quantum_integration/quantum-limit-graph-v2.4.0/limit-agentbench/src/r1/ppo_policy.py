# src/r1/ppo_policy.py

"""
PPO Policy
==========

Actor-only policy network for PPO. Smaller and simpler than
:class:`r1.ppo_agent.PPOAgent`, useful as a standalone stochastic policy.

Enhancements
------------
- Optional ``torch`` import guarded by ``_TORCH_AVAILABLE``.
- ``PPOPolicyConfig`` — frozen, validated: hidden size, learning rate,
  max grad norm, logit temperature.
- **Fixed the undetached-log-prob bug** — ``select_action`` now returns
  detached log-probs so callers can safely store them in replay buffers.
- **Numeric stability** — ``Categorical(logits=...)`` replaces
  ``Categorical(softmax(...))``.
- **Validation** of every input tensor and of ``loss`` in ``update``.
- Optional gradient clipping via ``max_grad_norm``.
- Batch helpers: ``get_action`` and ``evaluate_actions``.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` and
  ``save_state`` / ``load_state`` for weights.
- ``statistics()``, ``__repr__``, custom ``PPOPolicyError``, lazy ``%s``
  logging, and a ``__main__`` smoke test that auto-skips when torch is
  absent.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional PyTorch import
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import torch  # type: ignore
    import torch.nn as nn  # type: ignore
    import torch.optim as optim  # type: ignore
    import torch.distributions as dist  # type: ignore

    _TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore[assignment]
    nn = None      # type: ignore[assignment]
    optim = None   # type: ignore[assignment]
    dist = None    # type: ignore[assignment]
    _TORCH_AVAILABLE = False
    logger.debug("torch not importable; PPOPolicy disabled.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PPOPolicyError(ValueError):
    """Raised for invalid PPOPolicy inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PPOPolicyConfig:
    """Tunable parameters for the PPOPolicy network."""

    hidden_dim: int = 64
    lr: float = 1e-3
    max_grad_norm: Optional[float] = 0.5   # None disables clipping
    logit_temperature: float = 1.0          # Divide logits before softmax

    def __post_init__(self) -> None:
        if not isinstance(self.hidden_dim, int) or self.hidden_dim <= 0:
            raise PPOPolicyError("hidden_dim must be a positive int.")
        if self.lr <= 0:
            raise PPOPolicyError("lr must be > 0.")
        if self.max_grad_norm is not None and self.max_grad_norm <= 0:
            raise PPOPolicyError("max_grad_norm must be > 0 or None.")
        if self.logit_temperature <= 0:
            raise PPOPolicyError("logit_temperature must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PPOPolicyConfig":
        if not isinstance(data, Mapping):
            raise PPOPolicyError(
                f"PPOPolicyConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            hidden_dim=int(data.get("hidden_dim", 64)),
            lr=float(data.get("lr", 1e-3)),
            max_grad_norm=data.get("max_grad_norm", 0.5),
            logit_temperature=float(data.get("logit_temperature", 1.0)),
        )


# --------------------------------------------------------------------------- #
# Torch-optional module base
# --------------------------------------------------------------------------- #
_ModuleBase = nn.Module if _TORCH_AVAILABLE else object  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
class PPOPolicy(_ModuleBase):  # type: ignore[misc,valid-type]
    """
    Actor-only policy network for PPO.

    The network outputs **logits**. Callers that need probabilities should
    use :meth:`policy_probs`. Sampling uses ``Categorical(logits=...)``
    internally for numerical stability.

    Parameters
    ----------
    state_dim : int, default 3
        Observation dimensionality.
    action_dim : int, default 3
        Number of discrete actions.
    config : PPOPolicyConfig, optional
        Typed configuration.
    strict : bool, default True
        If True, invalid inputs raise :class:`PPOPolicyError`.
    """

    def __init__(
        self,
        state_dim: int = 3,
        action_dim: int = 3,
        *,
        config: Optional[PPOPolicyConfig] = None,
        strict: bool = True,
    ) -> None:
        if not _TORCH_AVAILABLE:
            raise PPOPolicyError(
                "PyTorch is not installed; PPOPolicy is unavailable. "
                "Install torch or use EnergyRLAgent / QLearningAgent instead."
            )
        if not isinstance(state_dim, int) or state_dim <= 0:
            raise PPOPolicyError("state_dim must be a positive int.")
        if not isinstance(action_dim, int) or action_dim <= 0:
            raise PPOPolicyError("action_dim must be a positive int.")

        super().__init__()

        self._config = config or PPOPolicyConfig()
        self._strict = bool(strict)
        self.state_dim = state_dim
        self.action_dim = action_dim

        # NOTE: no trailing softmax — the network outputs logits.
        self.fc = nn.Sequential(
            nn.Linear(state_dim, self._config.hidden_dim),
            nn.ReLU(),
            nn.Linear(self._config.hidden_dim, action_dim),
        )

        self.optimizer = optim.Adam(self.parameters(), lr=self._config.lr)

        self._updates: int = 0

        logger.debug(
            "PPOPolicy initialized "
            "(state_dim=%d, action_dim=%d, hidden=%d, lr=%.1e, "
            "max_grad_norm=%s)",
            state_dim, action_dim, self._config.hidden_dim,
            self._config.lr, self._config.max_grad_norm,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> PPOPolicyConfig:
        return self._config

    @property
    def updates(self) -> int:
        return self._updates

    # ---------------------------------------------------------- forward
    def forward(self, state: Any) -> Any:
        """
        Return raw action **logits** for ``state``.

        Shape: ``[..., action_dim]``. Use :meth:`policy_probs` for
        probabilities or pass to :meth:`select_action` for sampling.
        """
        return self.fc(state)

    def policy_probs(self, state: Any) -> Any:
        """Return softmax-normalized action probabilities."""
        logits = self.forward(state)
        if self._config.logit_temperature != 1.0:
            logits = logits / self._config.logit_temperature
        return torch.softmax(logits, dim=-1)

    # ---------------------------------------------------------- action selection
    def select_action(
        self, state: Any
    ) -> Tuple[int, Any]:
        """
        Sample an action for a single state.

        Parameters
        ----------
        state : tensor
            A single state, shape ``[state_dim]`` or ``[1, state_dim]``.

        Returns
        -------
        (action, log_prob)
            ``action`` is an int. ``log_prob`` is a **detached** scalar
            tensor — safe to store in a replay buffer.
        """
        with torch.no_grad():
            state_t = self._as_batched_tensor(state)
            logits = self.forward(state_t)
            if self._config.logit_temperature != 1.0:
                logits = logits / self._config.logit_temperature
            m = dist.Categorical(logits=logits)
            action = m.sample()
            log_prob = m.log_prob(action)
        return int(action.item()), log_prob.detach()

    def get_action(self, state: Any) -> Tuple[int, float]:
        """
        Same as :meth:`select_action`, but returns a Python float for the
        log-prob (convenience for callers that don't want a tensor).
        """
        action, log_prob = self.select_action(state)
        return action, float(log_prob.item())

    def evaluate_actions(
        self, states: Any, actions: Any
    ) -> Tuple[Any, Any]:
        """
        Evaluate a batch of ``(states, actions)``.

        Returns
        -------
        (log_probs, entropy)
            ``log_probs`` shape ``[batch]``; ``entropy`` shape ``[batch]``.
        """
        logits = self.forward(states)
        if self._config.logit_temperature != 1.0:
            logits = logits / self._config.logit_temperature
        m = dist.Categorical(logits=logits)
        return m.log_prob(actions), m.entropy()

    # ---------------------------------------------------------- update
    def update(self, loss: Any) -> Optional[float]:
        """
        Apply one optimizer step.

        Parameters
        ----------
        loss : tensor
            A scalar loss tensor with ``requires_grad``. A Python float or
            ``None`` is rejected in strict mode; in non-strict mode a warning
            is logged and the update is skipped.

        Returns
        -------
        float | None
            The loss value (as a Python float) after the step, or ``None``
            when the update was skipped.
        """
        if not _TORCH_AVAILABLE:  # pragma: no cover — guarded in __init__
            raise PPOPolicyError("PyTorch is not available.")

        if loss is None or isinstance(loss, (int, float)):
            msg = (
                f"loss must be a torch.Tensor, got "
                f"{type(loss).__name__}."
            )
            if self._strict:
                raise PPOPolicyError(msg)
            logger.warning("%s Skipping update.", msg)
            return None

        if not torch.is_tensor(loss):
            msg = f"loss must be a torch.Tensor, got {type(loss).__name__}."
            if self._strict:
                raise PPOPolicyError(msg)
            logger.warning("%s Skipping update.", msg)
            return None

        if loss.dim() != 0:
            msg = f"loss must be a scalar tensor, got shape {tuple(loss.shape)}."
            if self._strict:
                raise PPOPolicyError(msg)
            logger.warning("%s Skipping update.", msg)
            return None

        self.optimizer.zero_grad(set_to_none=True)
        loss.backward()

        if self._config.max_grad_norm is not None:
            grad_norm = torch.nn.utils.clip_grad_norm_(
                self.parameters(), self._config.max_grad_norm
            )
            logger.debug(
                "Gradient norm clipped to %.4f (max=%.4f).",
                float(grad_norm.item()),
                self._config.max_grad_norm,
            )

        self.optimizer.step()
        self._updates += 1

        loss_value = float(loss.detach().item())
        logger.debug(
            "PPOPolicy update #%d: loss=%.6f", self._updates, loss_value
        )
        return loss_value

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _as_batched_tensor(state: Any) -> Any:
        """Promote a 1-D state to a ``[1, state_dim]`` tensor."""
        state_t = (
            state
            if torch.is_tensor(state)
            else torch.as_tensor(state, dtype=torch.float32)
        )
        if state_t.dim() == 1:
            state_t = state_t.unsqueeze(0)
        return state_t

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the policy."""
        return {
            "state_dim": self.state_dim,
            "action_dim": self.action_dim,
            "hidden_dim": self._config.hidden_dim,
            "lr": self._config.lr,
            "max_grad_norm": self._config.max_grad_norm,
            "logit_temperature": self._config.logit_temperature,
            "updates": self._updates,
            "torch_available": _TORCH_AVAILABLE,
            "parameter_count": (
                sum(p.numel() for p in self.parameters())
                if _TORCH_AVAILABLE else 0
            ),
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self._config.to_dict(),
            "state_dim": self.state_dim,
            "action_dim": self.action_dim,
            "strict": self._strict,
            "updates": self._updates,
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
    ) -> "PPOPolicy":
        if not isinstance(data, Mapping):
            raise PPOPolicyError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = PPOPolicyConfig.from_dict(dict(data.get("config", {}) or {}))
        policy = cls(
            state_dim=int(data["state_dim"]),
            action_dim=int(data["action_dim"]),
            config=cfg,
            strict=bool(data.get("strict", True)) if strict is None else strict,
        )
        policy._updates = int(data.get("updates", 0))
        if load_state is not None:
            policy.load_state(load_state)
        return policy

    # ---------------------------------------------------------- weights
    def save_state(self) -> Dict[str, Any]:
        """Return the model + optimizer state dict for checkpointing."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOPolicyError("PyTorch is not available.")
        return {
            "model": self.state_dict(),
            "optimizer": self.optimizer.state_dict(),
            "updates": self._updates,
        }

    def load_state(self, checkpoint: Mapping[str, Any]) -> None:
        """Restore model + optimizer weights from ``save_state`` output."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOPolicyError("PyTorch is not available.")
        if not isinstance(checkpoint, Mapping):
            raise PPOPolicyError("checkpoint must be a Mapping.")
        if "model" in checkpoint:
            self.load_state_dict(checkpoint["model"])
        if checkpoint.get("optimizer") is not None:
            self.optimizer.load_state_dict(checkpoint["optimizer"])
        self._updates = int(checkpoint.get("updates", self._updates))
        logger.debug("PPOPolicy state restored.")

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PPOPolicy":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "PPOPolicy scope exited with %s.", exc_type.__name__
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "PPOPolicy("
            f"state_dim={self.state_dim}, "
            f"action_dim={self.action_dim}, "
            f"hidden={self._config.hidden_dim}, "
            f"lr={self._config.lr:.1e}, "
            f"temp={self._config.logit_temperature:.2f}, "
            f"updates={self._updates})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "PPOPolicy",
    "PPOPolicyConfig",
    "PPOPolicyError",
    "_TORCH_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.ppo_policy
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    if not _TORCH_AVAILABLE:
        print("PyTorch is not installed; skipping PPOPolicy smoke test.")
    else:
        torch.manual_seed(0)

        # ---- Happy path --------------------------------------------- #
        policy = PPOPolicy(state_dim=3, action_dim=4)
        print("repr       :", policy)

        state = torch.randn(3)
        action, log_prob = policy.select_action(state)
        print(f"action     : {action}, log_prob={float(log_prob.item()):.4f}")
        print(f"log_prob is Tensor : {torch.is_tensor(log_prob)}")
        print(f"log_prob requires_grad : {log_prob.requires_grad}")
        assert not log_prob.requires_grad, "log_prob must be detached"

        # Convenience float-returning variant.
        action_f, log_prob_f = policy.get_action(state)
        print(f"get_action : action={action_f}, log_prob={log_prob_f:.4f}")

        # ---- Probabilities ------------------------------------------ #
        probs = policy.policy_probs(state)
        print("probs      :", [round(p, 4) for p in probs.tolist()])
        print("probs sum  :", float(probs.sum().item()), "(should be ~1.0)")
        assert abs(float(probs.sum().item()) - 1.0) < 1e-5

        # ---- Batch evaluation --------------------------------------- #
        B = 32
        states = torch.randn(B, 3)
        actions = torch.randint(0, 4, (B,))
        log_probs, entropy = policy.evaluate_actions(states, actions)
        print(f"batch log_probs: shape={tuple(log_probs.shape)}, "
              f"mean={float(log_probs.mean().item()):.4f}")
        print(f"batch entropy  : shape={tuple(entropy.shape)}, "
              f"mean={float(entropy.mean().item()):.4f}")

        # ---- Update ------------------------------------------------- #
        # Simple policy-gradient loss: -E[log_prob * advantage]
        advantages = torch.randn(B)
        loss = -(log_probs * advantages).mean()
        result = policy.update(loss)
        print(f"update     : loss={result:.6f}, updates={policy.updates}")
        assert policy.updates == 1

        # ---- Multiple updates -------------------------------------- #
        for _ in range(5):
            log_probs, _ = policy.evaluate_actions(states, actions)
            loss = -(log_probs * advantages).mean()
            policy.update(loss)
        print("after 5    :", policy.statistics())

        # ---- Validation: non-tensor loss --------------------------- #
        for bad_loss in (0.5, None, "not-a-loss"):
            try:
                policy.update(bad_loss)  # type: ignore[arg-type]
            except PPOPolicyError as exc:
                print("Rejected   :", exc)

        # ---- Non-tensor loss in non-strict mode -------------------- #
        lenient = PPOPolicy(state_dim=3, action_dim=4, strict=False)
        result = lenient.update(0.5)  # type: ignore[arg-type]
        print("lenient    :", result, "(skipped)")
        assert result is None

        # ---- Non-scalar loss --------------------------------------- #
        non_scalar = torch.randn(4, requires_grad=True)
        try:
            policy.update(non_scalar)
        except PPOPolicyError as exc:
            print("Rejected   :", exc)

        # ---- Checkpoint round-trip --------------------------------- #
        checkpoint = policy.save_state()
        fresh = PPOPolicy(state_dim=3, action_dim=4)
        fresh.load_state(checkpoint)
        with torch.no_grad():
            l1 = policy.forward(state)
            l2 = fresh.forward(state)
        assert torch.allclose(l1, l2), "checkpoint restore failed"
        print("checkpoint : OK")

        # ---- Serialization ----------------------------------------- #
        payload = policy.to_json()
        restored = PPOPolicy.from_dict(json.loads(payload))
        assert restored.to_dict() == policy.to_dict()
        print("Round-trip : OK")

        # ---- Config validation ------------------------------------- #
        for bad_cfg in (
            dict(hidden_dim=0),
            dict(lr=0),
            dict(max_grad_norm=-0.5),
            dict(logit_temperature=0),
        ):
            try:
                PPOPolicyConfig(**bad_cfg)  # type: ignore[arg-type]
            except PPOPolicyError as exc:
                print("Rejected cfg:", exc)

        # ---- Input validation -------------------------------------- #
        for bad_call in (
            lambda: PPOPolicy(state_dim=0, action_dim=4),
            lambda: PPOPolicy(state_dim=3, action_dim=0),
        ):
            try:
                bad_call()
            except PPOPolicyError as exc:
                print("Rejected   :", exc)

        # ---- Temperature scaling ----------------------------------- #
        hot = PPOPolicy(state_dim=3, action_dim=4,
                        config=PPOPolicyConfig(logit_temperature=2.0))
        cold = PPOPolicy(state_dim=3, action_dim=4,
                         config=PPOPolicyConfig(logit_temperature=0.5))
        # Copy weights so the comparison is meaningful.
        cold.load_state_dict(hot.state_dict())
        with torch.no_grad():
            ph = hot.policy_probs(state)
            pc = cold.policy_probs(state)
        print(f"temp=2.0 entropy: {-torch.sum(ph * torch.log(ph)).item():.4f}")
        print(f"temp=0.5 entropy: {-torch.sum(pc * torch.log(pc)).item():.4f}")
        # Cold temperature should be peakier (lower entropy).
        assert float(-torch.sum(pc * torch.log(pc))) < float(-torch.sum(ph * torch.log(ph)))

        # ---- Context manager --------------------------------------- #
        with PPOPolicy(state_dim=3, action_dim=4) as scoped:
            scoped.select_action(state)
        print("Context    : OK")

        print("\nSmoke test passed.")
