# src/r1/ppo_policy.py

"""
PPO Policy
==========

Actor-only policy network for PPO. Smaller and simpler than
:class:`r1.ppo_agent.PPOAgent`, useful as a standalone stochastic policy.

Design
------
- Optional ``torch`` import guarded by ``_TORCH_AVAILABLE``.
- Frozen, validated :class:`PPOPolicyConfig` — includes ``seed`` and
  ``device`` for reproducibility and placement.
- Actor returns **logits**; ``Categorical(logits=...)`` is used throughout.
- ``policy_probs`` defaults to ``no_grad=True``; opt out with
  ``no_grad=False`` if you need gradient flow through probabilities.
- Device-aware state coercion; a CUDA policy accepts CPU numpy/torch states.
- ``update`` pre-validates the loss: must be a scalar tensor with
  ``requires_grad=True`` on the same device-type as the policy.
- Serialization contract:

    * ``to_dict`` / ``to_json`` → hyperparameters only (JSON-safe).
    * ``save_state`` / ``load_state`` → weights (use ``torch.save`` for
      binary; the returned dict contains optimizer state nested dicts).

- ``evaluate_actor`` returns ``(log_probs, None, entropy)`` so it can be
  dropped in for :meth:`PPOAgent.evaluate_actions` without destructuring
  changes.

Temperature semantics
---------------------
``forward`` returns **raw logits from the network** and does NOT apply
``logit_temperature``. The three sampling/evaluation methods —
``policy_probs``, ``select_action``, and ``evaluate_actions`` — DO apply
it. If you need temperature-scaled logits, divide manually::

    logits = policy.forward(s) / policy.config.logit_temperature

Strict mode
-----------
``strict`` affects only :meth:`update`. In non-strict mode, invalid
losses are logged and the update is skipped (``update`` returns None).
All other methods raise ``PPOPolicyError`` unconditionally on invalid
inputs — those are programming errors, not runtime data. Wrap in
try/except if you need graceful degradation there.

Threading
---------
``update`` is **not** thread-safe — serialize training calls. Read-only
methods (``select_action``, ``policy_probs``, ``evaluate_actions``) are
safe to run concurrently with each other but not with ``update``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from types import TracebackType
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "PPOPolicy",
    "PPOPolicyConfig",
    "PPOPolicyError",
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
    logit_temperature: float = 1.0          # Applied by *_probs/select/evaluate

    # Reproducibility. Note: torch.manual_seed is GLOBAL — setting this
    # seed affects every torch consumer in the process. Two PPOPolicy
    # instances created with the same seed will share initial weights.
    seed: Optional[int] = None

    # Device placement. ``None`` → leave on default.
    device: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.hidden_dim, bool)
            or not isinstance(self.hidden_dim, int)
            or self.hidden_dim <= 0
        ):
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
        try:
            return cls(
                hidden_dim=int(data.get("hidden_dim", 64)),
                lr=float(data.get("lr", 1e-3)),
                max_grad_norm=data.get("max_grad_norm", 0.5),
                logit_temperature=float(
                    data.get("logit_temperature", 1.0)
                ),
                seed=data.get("seed"),
                device=data.get("device"),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PPOPolicyError):
                raise
            raise PPOPolicyError(
                f"invalid PPOPolicyConfig payload: {exc}"
            ) from exc


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

    The network outputs **logits**. See the module docstring for the
    temperature and strict-mode contracts.
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
        if (
            isinstance(state_dim, bool)
            or not isinstance(state_dim, int)
            or state_dim <= 0
        ):
            raise PPOPolicyError("state_dim must be a positive int.")
        if (
            isinstance(action_dim, bool)
            or not isinstance(action_dim, int)
            or action_dim <= 0
        ):
            raise PPOPolicyError("action_dim must be a positive int.")

        super().__init__()

        self._config = config if config is not None else PPOPolicyConfig()
        self._strict = bool(strict)
        self.state_dim = state_dim
        self.action_dim = action_dim

        # ---- Reproducibility (before network creation) ----------------
        if self._config.seed is not None:
            torch.manual_seed(self._config.seed)
            if torch.cuda.is_available():
                torch.cuda.manual_seed_all(self._config.seed)

        # ---- Network (logits, no softmax) -----------------------------
        self.fc = nn.Sequential(
            nn.Linear(state_dim, self._config.hidden_dim),
            nn.ReLU(),
            nn.Linear(self._config.hidden_dim, action_dim),
        )

        self.optimizer = optim.Adam(self.parameters(), lr=self._config.lr)

        self._updates: int = 0

        # ---- Device placement ----------------------------------------
        if self._config.device is not None:
            try:
                self.to(self._config.device)
            except (RuntimeError, AssertionError) as exc:
                raise PPOPolicyError(
                    f"cannot move policy to device "
                    f"{self._config.device!r}: {exc}"
                ) from exc

        logger.debug(
            "PPOPolicy initialized "
            "(state_dim=%d, action_dim=%d, hidden=%d, lr=%.1e, "
            "max_grad_norm=%s, device=%s, seed=%s)",
            state_dim, action_dim, self._config.hidden_dim,
            self._config.lr, self._config.max_grad_norm,
            next(self.parameters()).device, self._config.seed,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> PPOPolicyConfig:
        return self._config

    @property
    def updates(self) -> int:
        return self._updates

    @property
    def device(self) -> Any:
        return next(self.parameters()).device

    # ---------------------------------------------------------- forward
    def forward(self, state: Any) -> Any:
        """Return raw action **logits** (shape ``[..., action_dim]``)."""
        return self.fc(state)

    def _temperature_scale(self, logits: Any) -> Any:
        t = self._config.logit_temperature
        return logits if t == 1.0 else logits / t

    def policy_probs(self, state: Any, *, no_grad: bool = True) -> Any:
        """
        Return softmax-normalised action probabilities.

        Parameters
        ----------
        no_grad : bool, default True
            Wrap in ``torch.no_grad()``. Set to False if you intend to
            differentiate through the probabilities (rare — typically you
            want log-probs, which ``select_action`` returns).
        """
        logits = self._temperature_scale(self.forward(state))
        if no_grad:
            with torch.no_grad():
                return torch.softmax(logits, dim=-1)
        return torch.softmax(logits, dim=-1)

    # ---------------------------------------------------------- tensor prep
    def _as_batched_tensor(self, state: Any) -> Any:
        """
        Coerce ``state`` to ``[B, state_dim]`` on the policy's device.

        Accepts a 1-D state (promoted to ``[1, state_dim]``) or a 2-D batch.
        Raises :class:`PPOPolicyError` on shape mismatches.
        """
        dev = self.device
        if torch.is_tensor(state):
            state_t = state.to(device=dev, dtype=torch.float32)
        else:
            state_t = torch.as_tensor(
                state, dtype=torch.float32, device=dev
            )
        if state_t.dim() == 1:
            state_t = state_t.unsqueeze(0)
        if state_t.dim() != 2:
            raise PPOPolicyError(
                f"state must be 1-D or 2-D, got shape "
                f"{tuple(state_t.shape)}."
            )
        if state_t.size(-1) != self.state_dim:
            raise PPOPolicyError(
                f"state last dim {state_t.size(-1)} != "
                f"state_dim {self.state_dim}."
            )
        return state_t

    # ---------------------------------------------------------- action selection
    def select_action(self, state: Any) -> Tuple[int, Any]:
        """
        Sample an action for a single state.

        Returns ``(action: int, log_prob: tensor)`` — the log-prob is a
        detached scalar tensor (safe to store in a replay buffer).
        """
        with torch.no_grad():
            state_t = self._as_batched_tensor(state)
            logits = self._temperature_scale(self.forward(state_t))
            m = dist.Categorical(logits=logits)
            action = m.sample()
            log_prob = m.log_prob(action)
        return int(action.item()), log_prob.detach()

    def get_action(self, state: Any) -> Tuple[int, float]:
        """Same as :meth:`select_action` but returns ``log_prob`` as a float."""
        action, log_prob = self.select_action(state)
        return action, float(log_prob.item())

    def evaluate_actions(
        self, states: Any, actions: Any
    ) -> Tuple[Any, Any]:
        """
        Evaluate ``(states, actions)``. Returns ``(log_probs, entropy)``,
        each shape ``[batch]``.
        """
        states_t = self._as_batched_tensor(states)

        if not torch.is_tensor(actions):
            actions_t = torch.as_tensor(
                actions, dtype=torch.long, device=states_t.device
            )
        else:
            if actions.dtype != torch.long:
                raise PPOPolicyError(
                    f"actions must be torch.long, got {actions.dtype}."
                )
            actions_t = actions.to(device=states_t.device)

        if actions_t.size(0) != states_t.size(0):
            raise PPOPolicyError(
                f"batch size mismatch: {states_t.size(0)} states, "
                f"{actions_t.size(0)} actions."
            )

        logits = self._temperature_scale(self.forward(states_t))
        m = dist.Categorical(logits=logits)
        return m.log_prob(actions_t), m.entropy()

    def evaluate_actor(
        self, states: Any, actions: Any
    ) -> Tuple[Any, Optional[Any], Any]:
        """
        Drop-in for ``PPOAgent.evaluate_actions`` when using an actor-only
        policy: returns ``(log_probs, None, entropy)``.
        """
        log_probs, entropy = self.evaluate_actions(states, actions)
        return log_probs, None, entropy

    # ---------------------------------------------------------- update
    def _validate_loss(self, loss: Any) -> Optional[str]:
        """Return an error message, or None if ``loss`` is valid."""
        if loss is None or isinstance(loss, (int, float)):
            return (
                f"loss must be a torch.Tensor, got {type(loss).__name__}."
            )
        if not torch.is_tensor(loss):
            return (
                f"loss must be a torch.Tensor, got {type(loss).__name__}."
            )
        if loss.dim() != 0:
            return (
                f"loss must be a scalar tensor, got shape "
                f"{tuple(loss.shape)}."
            )
        if not loss.requires_grad:
            return (
                "loss does not require grad; the tensor passed to update() "
                "must have requires_grad=True and be part of a computation "
                "graph. (Did you forget to disable torch.no_grad()?)"
            )
        if loss.device.type != self.device.type:
            return (
                f"loss on {loss.device} but policy on {self.device}; "
                f"the graph is disconnected."
            )
        return None

    def update(self, loss: Any) -> Optional[float]:
        """
        One optimizer step.

        Returns the loss value as a float after the step, or ``None`` when
        the update was skipped (non-strict mode with an invalid loss).
        """
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOPolicyError("PyTorch is not available.")

        error = self._validate_loss(loss)
        if error is not None:
            if self._strict:
                raise PPOPolicyError(error)
            logger.warning("%s Skipping update.", error)
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
            "device": str(self.device) if _TORCH_AVAILABLE else None,
            "parameter_count": (
                sum(p.numel() for p in self.parameters())
                if _TORCH_AVAILABLE else 0
            ),
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": VERSION,
            "config": self._config.to_dict(),
            "state_dim": self.state_dim,
            "action_dim": self.action_dim,
            "strict": self._strict,
            "updates": self._updates,
            "torch_available": _TORCH_AVAILABLE,
            "device": str(self.device) if _TORCH_AVAILABLE else None,
        }

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

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
        try:
            state_dim = int(data["state_dim"])
            action_dim = int(data["action_dim"])
        except KeyError as exc:
            raise PPOPolicyError(
                f"missing required field {exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            raise PPOPolicyError(f"invalid dimension: {exc}") from exc

        cfg = PPOPolicyConfig.from_dict(dict(data.get("config", {}) or {}))
        policy = cls(
            state_dim=state_dim,
            action_dim=action_dim,
            config=cfg,
            strict=(
                bool(data.get("strict", True)) if strict is None else strict
            ),
        )

        try:
            restored_updates = int(data.get("updates", 0))
        except (TypeError, ValueError) as exc:
            raise PPOPolicyError(
                f"invalid updates counter: {exc}"
            ) from exc
        if restored_updates < 0:
            raise PPOPolicyError(
                f"invalid updates counter: {restored_updates}."
            )
        policy._updates = restored_updates

        if load_state is not None:
            policy.load_state(load_state)
        return policy

    # ---------------------------------------------------------- weights
    def save_state(self) -> Dict[str, Any]:
        """Return ``{"model", "optimizer", "updates"}`` for torch.save."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOPolicyError("PyTorch is not available.")
        return {
            "model": self.state_dict(),
            "optimizer": self.optimizer.state_dict(),
            "updates": self._updates,
        }

    def load_state(self, checkpoint: Mapping[str, Any]) -> None:
        """Restore model + optimizer weights from :meth:`save_state` output."""
        if not _TORCH_AVAILABLE:  # pragma: no cover
            raise PPOPolicyError("PyTorch is not available.")
        if not isinstance(checkpoint, Mapping):
            raise PPOPolicyError("checkpoint must be a Mapping.")
        try:
            if checkpoint.get("model") is not None:
                self.load_state_dict(checkpoint["model"])
            if checkpoint.get("optimizer") is not None:
                self.optimizer.load_state_dict(checkpoint["optimizer"])
            if "updates" in checkpoint:
                restored = int(checkpoint["updates"])
                if restored < 0:
                    raise PPOPolicyError(
                        f"invalid updates counter: {restored}."
                    )
                self._updates = restored
        except (RuntimeError, ValueError, KeyError) as exc:
            if isinstance(exc, PPOPolicyError):
                raise
            raise PPOPolicyError(
                f"checkpoint restore failed: {exc}"
            ) from exc
        logger.debug("PPOPolicy state restored.")

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PPOPolicy":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
        print("device     :", policy.device)

        state = torch.randn(3)
        action, log_prob = policy.select_action(state)
        print(f"action     : {action}, log_prob={float(log_prob.item()):.4f}")
        print(f"log_prob requires_grad : {log_prob.requires_grad}")
        assert not log_prob.requires_grad, "log_prob must be detached"

        action_f, log_prob_f = policy.get_action(state)
        print(f"get_action : action={action_f}, log_prob={log_prob_f:.4f}")

        # ---- Probabilities (no_grad default) ----------------------- #
        probs = policy.policy_probs(state)
        print("probs sum  :", float(probs.sum().item()))
        print("probs requires_grad:", probs.requires_grad)
        assert not probs.requires_grad, (
            "policy_probs should default to no_grad=True"
        )
        assert abs(float(probs.sum().item()) - 1.0) < 1e-5

        probs_grad = policy.policy_probs(state, no_grad=False)
        print("probs (grad enabled) requires_grad:",
              probs_grad.requires_grad)
        assert probs_grad.requires_grad

        # ---- Batch evaluation --------------------------------------- #
        B = 32
        states = torch.randn(B, 3)
        actions = torch.randint(0, 4, (B,))
        log_probs, entropy = policy.evaluate_actions(states, actions)
        print(f"batch log_probs: shape={tuple(log_probs.shape)}, "
              f"mean={float(log_probs.mean().item()):.4f}")

        # ---- evaluate_actor shim ----------------------------------- #
        lp, val, ent = policy.evaluate_actor(states, actions)
        assert val is None
        assert torch.allclose(lp, log_probs)
        print("evaluate_actor: OK (returns (log_probs, None, entropy))")

        # ---- Update ------------------------------------------------- #
        advantages = torch.randn(B)
        loss = -(log_probs * advantages).mean()
        result = policy.update(loss)
        print(f"update     : loss={result:.6f}, updates={policy.updates}")
        assert policy.updates == 1

        for _ in range(5):
            log_probs, _ = policy.evaluate_actions(states, actions)
            loss = -(log_probs * advantages).mean()
            policy.update(loss)
        print("after 5    :", policy.statistics())

        # ---- Reject non-tensor / no-grad / non-scalar losses -------- #
        for bad_loss in (0.5, None, "not-a-loss"):
            try:
                policy.update(bad_loss)  # type: ignore[arg-type]
            except PPOPolicyError as exc:
                print("Rejected   :", exc)

        # requires_grad=False is a NEW check
        try:
            policy.update(torch.tensor(0.5))
        except PPOPolicyError as exc:
            print("Rejected   :", exc)

        non_scalar = torch.randn(4, requires_grad=True)
        try:
            policy.update(non_scalar)
        except PPOPolicyError as exc:
            print("Rejected   :", exc)

        # ---- Non-strict mode: bad loss skipped --------------------- #
        lenient = PPOPolicy(state_dim=3, action_dim=4, strict=False)
        result = lenient.update(0.5)  # type: ignore[arg-type]
        print("lenient    :", result, "(skipped)")
        assert result is None

        # ---- Bad state / batch / action dtype ----------------------- #
        for label, fn in (
            ("bad state shape",
                lambda: policy.select_action(torch.randn(5))),
            ("bad batch size",
                lambda: policy.evaluate_actions(
                    torch.randn(4, 3), torch.randint(0, 4, (3,)))),
            ("float actions",
                lambda: policy.evaluate_actions(
                    torch.randn(4, 3), torch.randint(0, 4, (4,)).float())),
        ):
            try:
                fn()
            except PPOPolicyError as exc:
                print(f"Rejected ({label}):", exc)

        # ---- Checkpoint round-trip --------------------------------- #
        checkpoint = policy.save_state()
        fresh = PPOPolicy(state_dim=3, action_dim=4)
        fresh.load_state(checkpoint)
        with torch.no_grad():
            l1 = policy.forward(state)
            l2 = fresh.forward(state)
        assert torch.allclose(l1, l2), "checkpoint restore failed"
        print("checkpoint : OK")

        # ---- Bad checkpoint shapes -------------------------------- #
        bad_ckpt = {
            "model": {"fc.0.weight": torch.zeros(1, 1)},
            "optimizer": None,
        }
        try:
            fresh.load_state(bad_ckpt)
        except PPOPolicyError as exc:
            print("Rejected ckpt:", exc)

        # ---- Serialization ----------------------------------------- #
        payload = policy.to_json()
        restored = PPOPolicy.from_dict(json.loads(payload))
        assert restored.to_dict() == policy.to_dict()
        print("Round-trip : OK")

        # ---- from_dict error wrapping ------------------------------ #
        for bad in (
            {"state_dim": 3},                      # missing action_dim
            {"state_dim": 3, "action_dim": 4,
             "updates": -1},                        # negative counter
            {"state_dim": "x", "action_dim": 4},    # non-int dim
        ):
            try:
                PPOPolicy.from_dict(bad)
            except PPOPolicyError as exc:
                print("Rejected   :", exc)

        # ---- Config validation ------------------------------------- #
        for bad_cfg in (
            dict(hidden_dim=0),
            dict(hidden_dim=True),    # bool rejected
            dict(lr=0),
            dict(max_grad_norm=-0.5),
            dict(logit_temperature=0),
        ):
            try:
                PPOPolicyConfig(**bad_cfg)  # type: ignore[arg-type]
            except PPOPolicyError as exc:
                print("Rejected cfg:", exc)

        # ---- Seed reproducibility ---------------------------------- #
        p1 = PPOPolicy(state_dim=4, action_dim=3,
                       config=PPOPolicyConfig(seed=7))
        p2 = PPOPolicy(state_dim=4, action_dim=3,
                       config=PPOPolicyConfig(seed=7))
        s = torch.randn(4)
        with torch.no_grad():
            a = p1.forward(s)
            b = p2.forward(s)
        assert torch.allclose(a, b), "seeded policies should match"
        print("seed reproducibility: OK")

        # ---- Bad device -------------------------------------------- #
        try:
            PPOPolicy(state_dim=3, action_dim=4,
                      config=PPOPolicyConfig(device="bogus"))
        except PPOPolicyError as exc:
            print("Rejected device:", exc)

        # ---- Temperature scaling ----------------------------------- #
        hot = PPOPolicy(state_dim=3, action_dim=4,
                        config=PPOPolicyConfig(logit_temperature=2.0))
        cold = PPOPolicy(state_dim=3, action_dim=4,
                         config=PPOPolicyConfig(logit_temperature=0.5))
        cold.load_state_dict(hot.state_dict())
        with torch.no_grad():
            ph = hot.policy_probs(state)
            pc = cold.policy_probs(state)
        h_ent = float(-torch.sum(ph * torch.log(ph)))
        c_ent = float(-torch.sum(pc * torch.log(pc)))
        print(f"temp=2.0 entropy: {h_ent:.4f}")
        print(f"temp=0.5 entropy: {c_ent:.4f}")
        assert c_ent < h_ent, "cold temperature should be peakier"

        # ---- Context manager --------------------------------------- #
        with PPOPolicy(state_dim=3, action_dim=4) as scoped:
            scoped.select_action(state)
        print("Context    : OK")

        print("\nSmoke test passed.")
