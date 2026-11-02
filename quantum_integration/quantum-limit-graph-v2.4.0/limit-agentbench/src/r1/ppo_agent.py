# src/r1/ppo_agent.py

"""
PPO Agent
=========

Proximal Policy Optimization (PPO-Clip) agent for continuous or discrete
control of energy-aware policies.

Design
------
- Optional ``torch`` import guarded by ``_TORCH_AVAILABLE``. Imports without
  torch; construction raises ``PPOAgentError``.
- Frozen, validated :class:`PPOAgentConfig`.
- Actor returns **logits**; every distribution uses ``Categorical(logits=...)``.
- Value and old-log-prob tensors are **detached inside** ``compute_loss`` —
  PPO treats them as constants of the previous policy; callers need not
  remember.
- PPO-style value clipping (clipped-value and unclipped take max).
- Optional advantage normalization (default on) — the canonical recipe.
- KL estimator configurable (``k1`` / ``k3``; default ``k3``).
- ``train_epochs`` helper: K optimizer epochs per rollout with early stop.
- Device-aware: ``config.device`` puts the model wherever the caller wants;
  ``get_action`` / ``evaluate_actions`` coerce states to the model's device.
- Serialization contract:
    * ``to_dict(include_state=False)`` → hyperparameters only (JSON-safe).
    * ``to_dict(include_state=True)``  → hyperparameters **plus** CPU-pinned
      model + optimizer state dicts.
    * ``from_dict(data, load_state=...)`` accepts either shape.
- Diagnostics from ``update`` / ``compute_loss`` carry a **fixed** key set
  (``grad_norm`` and ``early_stop`` are ``None`` / ``False`` when disabled).

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import threading
from dataclasses import asdict, dataclass
from types import TracebackType
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "PPOAgent",
    "PPOAgentConfig",
    "PPOAgentError",
    "_TORCH_AVAILABLE",
]

VERSION = "2.0.0"


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
    # Geometric decay per optimizer step (1.0 = no decay). NOT linear.
    entropy_coef_decay: float = 1.0
    max_grad_norm: Optional[float] = 0.5
    target_kl: Optional[float] = 0.015

    # PPO2 value clipping.
    value_clip_epsilon: Optional[float] = 0.2

    # Advantage normalization (canonical PPO).
    normalize_advantage: bool = True

    # KL estimator: "k3" (default, canonical) or "k1" (signed, may be < 0).
    kl_estimator: Literal["k1", "k3"] = "k3"

    # Reproducibility. Note: torch.manual_seed is GLOBAL. Setting this seed
    # affects every torch consumer in the process.
    seed: Optional[int] = None

    # Device. ``None`` → leave on the default device.
    device: Optional[str] = None

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
        if self.kl_estimator not in ("k1", "k3"):
            raise PPOAgentError(
                f"kl_estimator must be 'k1' or 'k3', "
                f"got {self.kl_estimator!r}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PPOAgentConfig":
        if not isinstance(data, Mapping):
            raise PPOAgentError(
                f"PPOAgentConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                hidden_dim=int(data.get("hidden_dim", 128)),
                lr=float(data.get("lr", 3e-4)),
                clip_epsilon=float(data.get("clip_epsilon", 0.2)),
                value_coef=float(data.get("value_coef", 0.5)),
                entropy_coef=float(data.get("entropy_coef", 0.01)),
                entropy_coef_min=float(data.get("entropy_coef_min", 0.0)),
                entropy_coef_decay=float(
                    data.get("entropy_coef_decay", 1.0)
                ),
                max_grad_norm=data.get("max_grad_norm", 0.5),
                target_kl=data.get("target_kl", 0.015),
                value_clip_epsilon=data.get("value_clip_epsilon", 0.2),
                normalize_advantage=bool(
                    data.get("normalize_advantage", True)
                ),
                kl_estimator=str(data.get("kl_estimator", "k3")),
                seed=data.get("seed"),
                device=data.get("device"),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PPOAgentError):
                raise
            raise PPOAgentError(
                f"invalid PPOAgentConfig payload: {exc}"
            ) from exc


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

    ``forward`` returns **logits** (not probabilities). Use
    :meth:`policy_probs` if you need the softmax.
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

        self._config = config if config is not None else PPOAgentConfig(
            lr=float(lr)
        )
        self._strict = bool(strict)
        self.state_dim = state_dim
        self.action_dim = action_dim
        # Legacy attribute (mirrors config; do not mutate).
        self.clip_epsilon: float = self._config.clip_epsilon

        # ---- Networks (logits, not probabilities) --------------------
        self.actor = nn.Sequential(
            nn.Linear(state_dim, self._config.hidden_dim),
            nn.ReLU(),
            nn.Linear(self._config.hidden_dim, action_dim),
        )
        self.critic = nn.Sequential(
            nn.Linear(state_dim, self._config.hidden_dim),
            nn.ReLU(),
            nn.Linear(self._config.hidden_dim, 1),
        )

        self.optimizer = optim.Adam(self.parameters(), lr=self._config.lr)
        self._mse = nn.MSELoss(reduction="mean")

        self._lock = threading.RLock()
        self._updates: int = 0
        self._entropy_coef: float = self._config.entropy_coef

        # Reproducibility — global torch seed. Documented limitation.
        if self._config.seed is not None:
            torch.manual_seed(self._config.seed)
            if torch.cuda.is_available():
                torch.cuda.manual_seed_all(self._config.seed)

        # Device placement.
        if self._config.device is not None:
            try:
                self.to(self._config.device)
            except (RuntimeError, AssertionError) as exc:
                raise PPOAgentError(
                    f"cannot move model to device "
                    f"{self._config.device!r}: {exc}"
                ) from exc

        logger.debug(
            "PPOAgent initialized "
            "(state_dim=%d, action_dim=%d, hidden=%d, lr=%.1e, "
            "clip=%.3f, value_coef=%.3f, entropy_coef=%.4f, "
            "device=%s)",
            state_dim, action_dim, self._config.hidden_dim,
            self._config.lr, self._config.clip_epsilon,
            self._config.value_coef, self._config.entropy_coef,
            next(self.parameters()).device,
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

    @property
    def device(self) -> Any:
        return next(self.parameters()).device

    # ---------------------------------------------------------- forward
    def forward(self, state: Any) -> Tuple[Any, Any]:
        """Return ``(logits, value)`` with shapes ``[..., action_dim]`` and ``[..., 1]``."""
        return self.actor(state), self.critic(state)

    def policy_probs(self, state: Any, *, no_grad: bool = True) -> Any:
        """Return softmax-normalised action probabilities."""
        if no_grad:
            with torch.no_grad():
                logits, _ = self.forward(state)
                return torch.softmax(logits, dim=-1)
        logits, _ = self.forward(state)
        return torch.softmax(logits, dim=-1)

    # ---------------------------------------------------------- actions
    def _to_tensor_on_device(self, x: Any) -> Any:
        """Coerce to float32 tensor on the model's device."""
        dev = self.device
        if torch.is_tensor(x):
            return x.to(device=dev, dtype=torch.float32)
        return torch.as_tensor(x, dtype=torch.float32, device=dev)

    def _apply_action_mask(
        self, logits: Any, action_mask: Any
    ) -> Any:
        mask_t = torch.as_tensor(
            action_mask, dtype=torch.bool, device=logits.device
        )
        if mask_t.dim() != logits.dim():
            raise PPOAgentError(
                f"action_mask dim {mask_t.dim()} != logits dim {logits.dim()}."
            )
        if mask_t.shape[-1] != logits.shape[-1]:
            raise PPOAgentError(
                f"action_mask last dim {mask_t.shape[-1]} != "
                f"action_dim {logits.shape[-1]}."
            )
        return logits.masked_fill(~mask_t, float("-inf"))

    def get_action(
        self,
        state: Any,
        *,
        deterministic: bool = False,
        action_mask: Optional[Any] = None,
    ) -> Tuple[int, float, float]:
        """Sample (or argmax) one action. Returns ``(action, log_prob, value)``."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOAgentError("PyTorch is not available.")

        with torch.no_grad():
            state_t = self._to_tensor_on_device(state)
            if state_t.dim() == 1:
                state_t = state_t.unsqueeze(0)
            logits, value = self.forward(state_t)
            if action_mask is not None:
                logits = self._apply_action_mask(logits, action_mask)
            dist = Categorical(logits=logits)
            if deterministic:
                action = torch.argmax(logits, dim=-1)
            else:
                action = dist.sample()
            log_prob = dist.log_prob(action)
            return (
                int(action.item()),
                float(log_prob.item()),
                float(value.squeeze(-1).item()),
            )

    # ---------------------------------------------------------- evaluation
    def evaluate_actions(
        self,
        states: Any,
        actions: Any,
        *,
        action_mask: Optional[Any] = None,
    ) -> Tuple[Any, Any, Any]:
        """Return ``(log_probs, values, entropy)`` for a batch."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOAgentError("PyTorch is not available.")
        states_t = self._to_tensor_on_device(states)
        actions_t = actions if torch.is_tensor(actions) else torch.as_tensor(
            actions, dtype=torch.long, device=states_t.device
        )
        logits, values = self.forward(states_t)
        if action_mask is not None:
            logits = self._apply_action_mask(logits, action_mask)
        dist = Categorical(logits=logits)
        return dist.log_prob(actions_t), values.squeeze(-1), dist.entropy()

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
        action_mask: Optional[Any] = None,
        normalize_advantage: Optional[bool] = None,
    ) -> Any:
        """
        Compute the PPO-Clip loss.

        ``old_log_probs``, ``old_values``, ``advantages`` are treated as
        constants of the previous policy and are **detached inside** this
        method — callers need not remember.

        Returns ``loss`` or ``(loss, diagnostics)`` when
        ``return_diagnostics=True``.
        """
        cfg = self._config
        device = self.device

        # ---- Shapes and detach the previous-policy quantities ---------
        returns = self._as_flat(self._to_tensor_on_device(returns))
        advantages = self._as_flat(
            self._to_tensor_on_device(advantages)
        ).detach()
        old_log_probs = self._as_flat(
            self._to_tensor_on_device(old_log_probs)
        ).detach()
        if old_values is not None:
            old_values = self._as_flat(
                self._to_tensor_on_device(old_values)
            ).detach()

        # ---- Advantage normalization (canonical PPO) ------------------
        do_norm = (
            cfg.normalize_advantage
            if normalize_advantage is None
            else normalize_advantage
        )
        if do_norm and advantages.numel() > 1:
            advantages = (
                (advantages - advantages.mean())
                / (advantages.std(unbiased=False) + 1e-8)
            )

        # ---- Current policy -------------------------------------------
        log_probs, values, entropy = self.evaluate_actions(
            states, actions, action_mask=action_mask,
        )

        # ---- Actor loss -----------------------------------------------
        ratio = torch.exp(log_probs - old_log_probs)
        clipped_ratio = torch.clamp(
            ratio, 1.0 - cfg.clip_epsilon, 1.0 + cfg.clip_epsilon,
        )
        actor_loss = -torch.min(
            ratio * advantages, clipped_ratio * advantages,
        ).mean()

        # ---- Critic loss with value clipping --------------------------
        critic_clip_fraction: Optional[float] = None
        if old_values is not None and cfg.value_clip_epsilon is not None:
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

        # ---- Entropy bonus --------------------------------------------
        with self._lock:
            entropy_coef = self._entropy_coef
        entropy_term = entropy.mean()

        total_loss = (
            actor_loss
            + cfg.value_coef * critic_loss
            - entropy_coef * entropy_term
        )

        if not return_diagnostics:
            return total_loss

        # ---- Diagnostics (fixed key set) ------------------------------
        with torch.no_grad():
            log_ratio = old_log_probs - log_probs
            if cfg.kl_estimator == "k1":
                approx_kl = log_ratio.mean().item()
            else:  # "k3"
                approx_kl = (
                    torch.exp(log_ratio) - 1.0 - log_ratio
                ).mean().item()

            clip_fraction = (
                (torch.abs(ratio - 1.0) > cfg.clip_epsilon)
                .float().mean().item()
            )
            if old_values is not None and cfg.value_clip_epsilon is not None:
                critic_clip_fraction = float(
                    (torch.abs(values - old_values)
                     > cfg.value_clip_epsilon)
                    .float().mean().item()
                )

        diagnostics: Dict[str, Any] = {
            "total_loss": float(total_loss.item()),
            "actor_loss": float(actor_loss.item()),
            "critic_loss": float(critic_loss.item()),
            "entropy": float(entropy_term.item()),
            "entropy_coef": float(entropy_coef),
            "approx_kl": float(approx_kl),
            "clip_fraction": float(clip_fraction),
            "critic_clip_fraction": critic_clip_fraction,
            "mean_ratio": float(ratio.mean().item()),
            "mean_value": float(values.mean().item()),
            "mean_return": float(returns.mean().item()),
            # Always present; filled by update():
            "grad_norm": None,
            "early_stop": False,
            "update_index": self.updates,
        }
        return total_loss, diagnostics

    @staticmethod
    def _as_flat(tensor: Any) -> Any:
        """Return a 1-D view; reject sequences to surface shape bugs."""
        if tensor is None:  # pragma: no cover
            return tensor
        if tensor.dim() == 0:
            return tensor.unsqueeze(0)
        if tensor.dim() > 2:
            raise PPOAgentError(
                f"expected 1-D or 2-D tensor, got shape "
                f"{tuple(tensor.shape)}."
            )
        if tensor.dim() == 2 and tensor.size(-1) == 1:
            return tensor.squeeze(-1)
        return tensor

    # ---------------------------------------------------------- training
    def update(
        self,
        states: Any,
        actions: Any,
        old_log_probs: Any,
        returns: Any,
        advantages: Any,
        *,
        old_values: Optional[Any] = None,
        action_mask: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """One optimizer step. Returns diagnostics."""
        cfg = self._config
        self.optimizer.zero_grad(set_to_none=True)

        loss, diagnostics = self.compute_loss(
            states, actions, old_log_probs, returns, advantages,
            old_values=old_values,
            action_mask=action_mask,
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

        if cfg.target_kl is not None and diagnostics["approx_kl"] > cfg.target_kl:
            diagnostics["early_stop"] = True

        logger.debug(
            "PPO update #%d: loss=%.4f actor=%.4f critic=%.4f entropy=%.4f "
            "kl=%.5f clip_frac=%.3f grad_norm=%s early_stop=%s",
            self._updates,
            diagnostics["total_loss"],
            diagnostics["actor_loss"],
            diagnostics["critic_loss"],
            diagnostics["entropy"],
            diagnostics["approx_kl"],
            diagnostics["clip_fraction"],
            f"{diagnostics['grad_norm']:.4f}"
            if diagnostics["grad_norm"] is not None else "n/a",
            diagnostics["early_stop"],
        )
        return diagnostics

    def train_epochs(
        self,
        states: Any,
        actions: Any,
        old_log_probs: Any,
        returns: Any,
        advantages: Any,
        *,
        old_values: Optional[Any] = None,
        action_mask: Optional[Any] = None,
        n_epochs: int = 4,
        target_kl: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Run up to ``n_epochs`` optimizer steps over the same batch.

        ``old_log_probs`` / ``old_values`` are the **rollout-time**
        references (do not re-evaluate between epochs — that is the whole
        point of PPO). Early-stops when ``approx_kl`` exceeds ``target_kl``.
        """
        if n_epochs < 1:
            raise PPOAgentError("n_epochs must be >= 1.")
        kl_threshold = (
            target_kl if target_kl is not None else self._config.target_kl
        )

        results: List[Dict[str, Any]] = []
        for _ in range(n_epochs):
            diag = self.update(
                states, actions, old_log_probs, returns, advantages,
                old_values=old_values, action_mask=action_mask,
            )
            results.append(diag)
            if kl_threshold is not None and diag["approx_kl"] > kl_threshold:
                diag["early_stop"] = True
                break
        return results

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """JSON-safe configuration and counter snapshot."""
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
            "kl_estimator": self._config.kl_estimator,
            "normalize_advantage": self._config.normalize_advantage,
            "updates": self.updates,
            "torch_available": _TORCH_AVAILABLE,
            "device": str(self.device) if _TORCH_AVAILABLE else None,
            "parameter_count": (
                sum(p.numel() for p in self.parameters())
                if _TORCH_AVAILABLE else 0
            ),
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_state: bool = False) -> Dict[str, Any]:
        """
        Return a JSON-safe hyperparameter snapshot.

        When ``include_state=True``, the model and optimizer state dicts are
        also included, pinned to CPU. Use :meth:`save_state` +
        ``torch.save`` for real binary checkpoints (optimizer state contains
        nested dicts that JSON cannot round-trip cleanly).
        """
        out: Dict[str, Any] = {
            "version": VERSION,
            "config": self._config.to_dict(),
            "state_dim": self.state_dim,
            "action_dim": self.action_dim,
            "strict": self._strict,
            "entropy_coef": self.entropy_coef,
            "updates": self.updates,
            "torch_available": _TORCH_AVAILABLE,
            "device": str(self.device) if _TORCH_AVAILABLE else None,
        }
        if include_state and _TORCH_AVAILABLE:
            out["model"] = {
                k: v.detach().cpu()
                for k, v in self.state_dict().items()
            }
            opt_state = self.optimizer.state_dict()
            out["optimizer"] = {
                "state": opt_state.get("state", {}),
                "param_groups": opt_state.get("param_groups", []),
            }
        return out

    def to_json(self, *, include_state: bool = False, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        if include_state:
            raise PPOAgentError(
                "to_json(include_state=True) is not supported — optimizer "
                "state contains nested dicts that JSON cannot round-trip "
                "cleanly. Use save_state() + torch.save() for binary "
                "checkpointing."
            )
        return json.dumps(self.to_dict(include_state=False), **kwargs)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        load_state: Optional[Mapping[str, Any]] = None,
    ) -> "PPOAgent":
        """
        Rebuild a ``PPOAgent`` from a dict produced by :meth:`to_dict`.

        If ``data`` carries ``model`` / ``optimizer`` (i.e.
        ``to_dict(include_state=True)``) those are used as ``load_state``
        unless overridden by the explicit ``load_state=`` kwarg.
        """
        if not isinstance(data, Mapping):
            raise PPOAgentError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            state_dim = int(data["state_dim"])
            action_dim = int(data["action_dim"])
        except KeyError as exc:
            raise PPOAgentError(
                f"missing required field {exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            raise PPOAgentError(f"invalid dimension: {exc}") from exc

        cfg = PPOAgentConfig.from_dict(dict(data.get("config", {}) or {}))

        agent = cls(
            state_dim=state_dim,
            action_dim=action_dim,
            config=cfg,
            strict=(
                bool(data.get("strict", True)) if strict is None else strict
            ),
        )

        # ---- Validate restored live hyperparameters ------------------
        try:
            restored_entropy = float(
                data.get("entropy_coef", cfg.entropy_coef)
            )
        except (TypeError, ValueError) as exc:
            raise PPOAgentError(
                f"invalid entropy_coef: {exc}"
            ) from exc
        if not 0.0 <= restored_entropy <= cfg.entropy_coef:
            raise PPOAgentError(
                f"restored entropy_coef {restored_entropy} outside "
                f"[0, {cfg.entropy_coef}]."
            )

        try:
            restored_updates = int(data.get("updates", 0))
        except (TypeError, ValueError) as exc:
            raise PPOAgentError(f"invalid updates counter: {exc}") from exc
        if restored_updates < 0:
            raise PPOAgentError(
                f"invalid updates counter: {restored_updates}."
            )

        with agent._lock:
            agent._entropy_coef = restored_entropy
            agent._updates = restored_updates

        # ---- Load weights (inline or via kwarg) ----------------------
        inline_state = None
        if "model" in data or "optimizer" in data:
            inline_state = {
                "model": data.get("model"),
                "optimizer": data.get("optimizer"),
            }
        payload = load_state if load_state is not None else inline_state
        if payload is not None:
            agent.load_state(payload)

        return agent

    @classmethod
    def from_json(cls, payload: str) -> "PPOAgent":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PPOAgentError(
                f"invalid JSON payload: {exc}"
            ) from exc
        try:
            return cls.from_dict(raw)
        except PPOAgentError:
            raise
        except (TypeError, ValueError) as exc:
            raise PPOAgentError(
                f"invalid agent payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- weights
    def save_state(self) -> Dict[str, Any]:
        """Return ``{"model": ..., "optimizer": ...}`` for torch.save."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOAgentError("PyTorch is not available.")
        return {
            "model": self.state_dict(),
            "optimizer": self.optimizer.state_dict(),
        }

    def load_state(self, checkpoint: Mapping[str, Any]) -> None:
        """Restore model + optimizer weights from :meth:`save_state` output."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOAgentError("PyTorch is not available.")
        if not isinstance(checkpoint, Mapping):
            raise PPOAgentError("checkpoint must be a Mapping.")
        try:
            if checkpoint.get("model") is not None:
                self.load_state_dict(checkpoint["model"])
            if checkpoint.get("optimizer") is not None:
                self.optimizer.load_state_dict(checkpoint["optimizer"])
        except (RuntimeError, ValueError, KeyError) as exc:
            raise PPOAgentError(
                f"state_dict restore failed: {exc}"
            ) from exc
        logger.debug("PPOAgent state restored from checkpoint.")

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PPOAgent":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "PPOAgent scope exited with %s.", exc_type.__name__
            )
        return None

    # ------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            updates = self._updates
            entropy_coef = self._entropy_coef
        return (
            "PPOAgent("
            f"state_dim={self.state_dim}, "
            f"action_dim={self.action_dim}, "
            f"hidden={self._config.hidden_dim}, "
            f"lr={self._config.lr:.1e}, "
            f"clip={self._config.clip_epsilon:.2f}, "
            f"entropy_coef={entropy_coef:.4f}, "
            f"updates={updates})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.ppo_agent
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    if not _TORCH_AVAILABLE:
        print("PyTorch is not installed; skipping PPOAgent smoke test.")
    else:
        torch.manual_seed(0)

        agent = PPOAgent(
            state_dim=4, action_dim=3, lr=3e-4,
            config=PPOAgentConfig(seed=0),
        )
        print("repr       :", agent)
        print("device     :", agent.device)

        # ---- Single-step action -------------------------------------
        state = torch.randn(4)
        action, log_prob, value = agent.get_action(state)
        print(f"action     : {action}, log_prob={log_prob:.4f}, "
              f"value={value:.4f}")
        det_action, _, _ = agent.get_action(state, deterministic=True)
        print("deterministic:", det_action)

        # ---- Batch training with train_epochs -----------------------
        B = 64
        states = torch.randn(B, 4)
        actions = torch.randint(0, 3, (B,))
        old_log_probs, old_values, _ = agent.evaluate_actions(
            states, actions,
        )
        # NOTE: caller need *not* detach — compute_loss does it internally.
        returns = torch.randn(B)
        advantages = torch.randn(B) * 5.0   # deliberately unscaled

        diagnostics = agent.train_epochs(
            states, actions, old_log_probs, returns, advantages,
            old_values=old_values, n_epochs=4, target_kl=0.05,
        )
        for i, d in enumerate(diagnostics):
            print(f"epoch {i}: kl={d['approx_kl']:.5f} "
                  f"clip_frac={d['clip_fraction']:.3f} "
                  f"early_stop={d['early_stop']}")

        # ---- Advantage normalization proof --------------------------
        # With raw advantages of std ~5, the ratio would explode without
        # normalization. Assert it stays close to 1 after normalization.
        _, diag = agent.compute_loss(
            states, actions, old_log_probs, returns, advantages,
            old_values=old_values, return_diagnostics=True,
            normalize_advantage=True,
        )
        print("normalized kl:", diag["approx_kl"])

        # ---- Fixed diagnostics key set ------------------------------
        expected_keys = {
            "total_loss", "actor_loss", "critic_loss", "entropy",
            "entropy_coef", "approx_kl", "clip_fraction",
            "critic_clip_fraction", "mean_ratio", "mean_value",
            "mean_return", "grad_norm", "early_stop", "update_index",
        }
        assert set(diag.keys()) == expected_keys, (
            f"diagnostics schema drift: "
            f"missing={expected_keys - diag.keys()}, "
            f"extra={diag.keys() - expected_keys}"
        )
        print("diagnostics schema: OK")

        # ---- Double-softmax proof ----------------------------------
        logits, _ = agent.forward(states[:1])
        probs = agent.policy_probs(states[:1])
        print("logits sum :", float(logits.sum().item()), "≠ 1.0")
        print("probs sum  :", float(probs.sum().item()), "≈ 1.0")

        # ---- Action masking ----------------------------------------
        mask = torch.tensor([True, False, True])
        masked_action, _, _ = agent.get_action(
            state, deterministic=True, action_mask=mask,
        )
        assert masked_action != 1, "masked action selected"
        print("action mask: OK, selected", masked_action)

        # ---- Checkpoint round-trip ---------------------------------
        checkpoint = agent.save_state()
        agent2 = PPOAgent(state_dim=4, action_dim=3)
        agent2.load_state(checkpoint)
        with torch.no_grad():
            l1, _ = agent.forward(states[:1])
            l2, _ = agent2.forward(states[:1])
        assert torch.allclose(l1, l2)
        print("checkpoint : OK")

        # ---- Full state serialization ------------------------------
        agent3 = PPOAgent.from_dict(agent.to_dict(include_state=True))
        with torch.no_grad():
            l3, _ = agent3.forward(states[:1])
        assert torch.allclose(l1, l3)
        print("full serialization: OK")

        # ---- Hyperparameter-only serialization ---------------------
        payload = agent.to_json()
        restored = PPOAgent.from_json(payload)
        assert restored.to_dict() == agent.to_dict()
        print("hp-only round-trip: OK")

        # ---- Config validation -------------------------------------
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
            dict(kl_estimator="bogus"),
        ):
            try:
                PPOAgentConfig(**bad_cfg)  # type: ignore[arg-type]
            except PPOAgentError as exc:
                print("Rejected cfg:", exc)

        # ---- Serialization validation ------------------------------
        try:
            PPOAgent.from_dict({"state_dim": 4})  # missing action_dim
        except PPOAgentError as exc:
            print("missing field:", exc)

        try:
            PPOAgent.from_dict({
                "state_dim": 4,
                "action_dim": 3,
                "entropy_coef": 5.0,
            })
        except PPOAgentError as exc:
            print("bad entropy_coef:", exc)

        try:
            PPOAgent.from_dict({
                "state_dim": 4,
                "action_dim": 3,
                "updates": -1,
            })
        except PPOAgentError as exc:
            print("bad updates:", exc)

        # ---- Shape rejection --------------------------------------
        try:
            agent.compute_loss(
                states.unsqueeze(1).expand(-1, 2, -1),
                actions, old_log_probs, returns, advantages,
            )
        except PPOAgentError as exc:
            print("bad shape rejected:", exc)

        # ---- Entropy decay ----------------------------------------
        decaying = PPOAgent(
            state_dim=4, action_dim=3,
            config=PPOAgentConfig(
                entropy_coef=0.1, entropy_coef_decay=0.9,
                entropy_coef_min=0.001,
            ),
        )
        start_e = decaying.entropy_coef
        for _ in range(20):
            decaying.update(
                states, actions, old_log_probs, returns, advantages,
            )
        print(f"entropy    : {start_e:.4f} → {decaying.entropy_coef:.4f}")

        print("\nSmoke test passed.")
