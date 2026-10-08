# src/quantum_integration/digital_twin/digital_twin_moe.py

"""Mixture-of-Experts gating with expert-specific action distributions.

Overview
--------
Each :class:`Expert` owns a linear policy over ``ACTION_SPACE``. A
gating network produces per-expert weights from the state features, and
the combined action distribution is the gate-weighted mixture of the
experts' distributions.

Key design points
-----------------
* ``expert_name`` in :class:`MoEResult` is the **routing expert**
  (``argmax(gate)``), preserving the original semantics.
  ``contributing_expert_name`` is the expert whose weighted distribution
  contributed most to the *selected* action — usually the more
  informative label for training signals.
* :meth:`MoEGatingNetwork.add_training_sample` takes the action that was
  actually taken. The legacy 3-argument form
  ``add_training_sample(state, expert, reward)`` still works but is
  deprecated and falls back to the expert's argmax action.
* ``select_expert`` supports exploration via ``epsilon`` (uniform random
  action) and ``temperature`` (softmax scaling when sampling).
* All inputs are validated; ``reward`` must be finite; feature values
  must be finite numeric.
* All state mutations are guarded by a re-entrant lock
  (``thread_safe=True``, default).

Serialization
-------------
``to_dict`` / ``from_dict`` round-trip the full training state:
expert names, weights, per-expert learning rates, counters, reward
aggregates, and the NumPy RNG state. Shapes are validated on load and
``schema_version`` newer than the module's is rejected.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import uuid
import warnings
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import numpy as np

from .digital_twin_distillation import (
    ACTION_SPACE,
    TwinOptimizationState,
    _FEATURE_NAMES,
)
from .digital_twin_errors import DigitalTwinInputError

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

_DEFAULT_EXPERT_NAMES: Tuple[str, ...] = (
    "performance", "carbon", "cost", "adaptive",
)

_EPS: float = 1e-12

_DEPRECATION_MESSAGE = (
    "add_training_sample(state, expert, reward) is deprecated; "
    "pass the action explicitly: "
    "add_training_sample(state, expert, action, reward). "
    "The legacy form falls back to the expert's argmax action and will "
    "be removed in a future release."
)


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #

def _validate_finite_float(
    name: str, value: Any, *, allow_none: bool = False,
) -> Optional[float]:
    """Return ``value`` as a finite ``float`` (or ``None`` when allowed)."""
    if value is None:
        if allow_none:
            return None
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    if not math.isfinite(value):
        raise DigitalTwinInputError(
            f"{name} must be finite (no NaN or infinity)."
        )
    return float(value)


def _validate_positive_float(name: str, value: Any) -> float:
    """Return ``value`` as a strictly positive finite float."""
    v = _validate_finite_float(name, value)
    assert v is not None
    if v <= 0:
        raise DigitalTwinInputError(f"{name} must be > 0.")
    return v


def _validate_non_negative_float(name: str, value: Any) -> float:
    """Return ``value`` as a non-negative finite float."""
    v = _validate_finite_float(name, value)
    assert v is not None
    if v < 0:
        raise DigitalTwinInputError(f"{name} must be >= 0.")
    return v


def _validate_action_idx(name: str, value: Any, n_actions: int) -> int:
    """Return ``value`` as an action index in ``[0, n_actions)``."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise DigitalTwinInputError(f"{name} must be an integer.")
    if not (0 <= value < n_actions):
        raise DigitalTwinInputError(
            f"{name} must be in [0, {n_actions}), got {value}."
        )
    return int(value)


def _validate_expert_names(
    names: Sequence[str], *, min_count: int = 1,
) -> Tuple[str, ...]:
    """Return ``names`` as a tuple of unique non-empty strings."""
    if not names:
        raise DigitalTwinInputError(
            f"at least {min_count} expert name(s) required."
        )
    if len(names) < min_count:
        raise DigitalTwinInputError(
            f"at least {min_count} expert name(s) required, got {len(names)}."
        )
    out: List[str] = []
    seen: set = set()
    for n in names:
        if not isinstance(n, str) or not n.strip():
            raise DigitalTwinInputError(
                "expert names must be non-empty, non-whitespace strings."
            )
        if n in seen:
            raise DigitalTwinInputError(f"duplicate expert name {n!r}.")
        seen.add(n)
        out.append(n)
    return tuple(out)


def _softmax(x: np.ndarray) -> np.ndarray:
    """Numerically stable softmax; non-finite inputs are treated as 0."""
    x = np.asarray(x, dtype=np.float64).reshape(-1)
    if x.size == 0:
        return x
    x = np.where(np.isfinite(x), x, 0.0)
    x = x - np.max(x)
    e = np.exp(x)
    s = float(e.sum())
    return e / s if s > 0 else np.full_like(e, 1.0 / e.size)


# --------------------------------------------------------------------------- #
# Expert
# --------------------------------------------------------------------------- #

@dataclass
class Expert:
    """Linear expert with a dedicated strategy distribution over actions.

    The ``weights`` array has shape ``(feature_dim, num_actions)``. The
    array is copied at construction so external mutation of the input
    does not affect the expert's state.
    """

    name: str
    weights: np.ndarray  # (feature_dim, num_actions)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise DigitalTwinInputError(
                "expert name must be a non-empty, non-whitespace string."
            )
        w = np.asarray(self.weights, dtype=np.float64)
        if w.ndim != 2:
            raise DigitalTwinInputError(
                f"expert {self.name!r} weights must be 2-D, got "
                f"shape {w.shape}."
            )
        if not np.all(np.isfinite(w)):
            raise DigitalTwinInputError(
                f"expert {self.name!r} weights contain non-finite values."
            )
        self.weights = w.copy()

    def predict(self, state_vec: np.ndarray) -> np.ndarray:
        """Return a softmax distribution over actions for ``state_vec``."""
        logits = np.asarray(state_vec, dtype=np.float64) @ self.weights
        return _softmax(logits)


# --------------------------------------------------------------------------- #
# Result
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class MoEResult:
    """Result of a single :meth:`MoEGatingNetwork.select_expert` call."""

    expert_name: str
    gate_probs: Tuple[float, ...]
    action_probs: Tuple[float, ...]
    selected_action: str
    selected_action_idx: int

    # New fields (defaulted for backward compatibility with old pickles).
    contributing_expert_name: Optional[str] = None
    gate_entropy: float = 0.0
    explored: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expert_name": self.expert_name,
            "gate_probs": list(self.gate_probs),
            "action_probs": list(self.action_probs),
            "selected_action": self.selected_action,
            "selected_action_idx": self.selected_action_idx,
            "contributing_expert_name": self.contributing_expert_name,
            "gate_entropy": self.gate_entropy,
            "explored": self.explored,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MoEResult":
        return cls(
            expert_name=str(data["expert_name"]),
            gate_probs=tuple(float(x) for x in data.get("gate_probs", ())),
            action_probs=tuple(
                float(x) for x in data.get("action_probs", ())
            ),
            selected_action=str(data["selected_action"]),
            selected_action_idx=int(data["selected_action_idx"]),
            contributing_expert_name=data.get("contributing_expert_name"),
            gate_entropy=float(data.get("gate_entropy", 0.0)),
            explored=bool(data.get("explored", False)),
        )


# --------------------------------------------------------------------------- #
# Fallback no-op lock
# --------------------------------------------------------------------------- #

class _NullLock:
    """A no-op context manager used when ``thread_safe=False``."""

    def __enter__(self) -> "_NullLock":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


# --------------------------------------------------------------------------- #
# RNG serialization helpers
# --------------------------------------------------------------------------- #

def _serialize_rng_state(rng: np.random.Generator) -> Dict[str, Any]:
    """Extract a JSON-serializable RNG state dict."""
    try:
        return dict(rng.bit_generator.state)
    except Exception:  # noqa: BLE001 - defensive
        logger.warning("Could not serialize RNG state.")
        return {}


def _restore_rng_state(
    rng: np.random.Generator, state: Mapping[str, Any],
) -> None:
    """Restore a previously-serialized RNG state (best-effort)."""
    if not state:
        return
    try:
        rng.bit_generator.state = dict(state)
    except Exception as exc:  # noqa: BLE001 - best-effort
        logger.warning("Could not restore RNG state: %s", exc)


def _state_payload(
    state: Union[Mapping[str, Any], TwinOptimizationState],
) -> Any:
    """Return a JSON-friendly representation of ``state`` for hashing."""
    if isinstance(state, TwinOptimizationState):
        return state.to_feature_vector().astype(float).tolist()
    if isinstance(state, ABCMapping):
        return dict(state)
    return repr(state)


# --------------------------------------------------------------------------- #
# MoE gating network
# --------------------------------------------------------------------------- #

class MoEGatingNetwork:
    """Mixture-of-experts gating.

    Parameters
    ----------
    storage:
        Optional backend. If it exposes ``log_routing_decision`` it is
        called on every :meth:`select_expert` (best-effort).
    num_experts:
        Number of experts. Must be ``>= 1``. Values ``>= 2`` are typical.
    seed:
        Seed for the NumPy RNG used for exploration and initialization.
    lr:
        Default learning rate used for both gate and expert updates
        (overridable via ``gate_lr`` / ``expert_lr``).
    gate_lr, expert_lr:
        Optional per-component learning rates. Default to ``lr``.
    expert_names:
        Sequence of unique non-empty names. If shorter than
        ``num_experts`` it is extended with ``expert_{i}``; if longer it
        is truncated.
    grad_clip:
        L2 norm clip applied to each gradient tensor (``0`` disables).
    weight_decay:
        Multiplicative shrink applied after each update (``0`` disables).
    thread_safe:
        When ``True`` (default), mutations are guarded by an
        ``RLock``. Set to ``False`` only if the caller guarantees
        single-threaded access.
    feature_names:
        Override for the feature name list. Defaults to the canonical
        ``_FEATURE_NAMES`` from the distillation module.
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        num_experts: int = 4,
        seed: int = 0,
        lr: float = 0.1,
        gate_lr: Optional[float] = None,
        expert_lr: Optional[float] = None,
        expert_names: Optional[Sequence[str]] = None,
        grad_clip: float = 1.0,
        weight_decay: float = 0.0,
        thread_safe: bool = True,
        feature_names: Optional[Sequence[str]] = None,
    ) -> None:
        if isinstance(num_experts, bool) or not isinstance(num_experts, int):
            raise DigitalTwinInputError("num_experts must be an integer.")
        if num_experts < 1:
            raise DigitalTwinInputError("num_experts must be >= 1.")

        lr_v = _validate_positive_float("lr", lr)
        self._gate_lr = (
            _validate_positive_float("gate_lr", gate_lr)
            if gate_lr is not None else lr_v
        )
        self._expert_lr = (
            _validate_positive_float("expert_lr", expert_lr)
            if expert_lr is not None else lr_v
        )
        self._lr = lr_v

        self._grad_clip = _validate_non_negative_float("grad_clip", grad_clip)
        self._weight_decay = _validate_non_negative_float(
            "weight_decay", weight_decay,
        )

        # Names: extend / truncate to exactly num_experts.
        base_names = list(expert_names or _DEFAULT_EXPERT_NAMES)
        if len(base_names) < num_experts:
            for i in range(len(base_names), num_experts):
                base_names.append(f"expert_{i}")
        elif len(base_names) > num_experts:
            base_names = base_names[:num_experts]
        self._expert_names: Tuple[str, ...] = _validate_expert_names(
            base_names, min_count=num_experts,
        )
        self._expert_names_set = frozenset(self._expert_names)
        self.num_experts = int(num_experts)

        # Feature configuration.
        self._feature_names: Tuple[str, ...] = tuple(
            feature_names or _FEATURE_NAMES
        )
        if not self._feature_names:
            raise DigitalTwinInputError(
                "feature_names must contain at least one name."
            )
        self.feature_dim = len(self._feature_names)
        self.num_actions = len(ACTION_SPACE)

        # External collaborators.
        self.storage = storage

        # RNG + threading.
        self._rng = np.random.default_rng(seed)
        self._lock: Any = threading.RLock() if thread_safe else _NullLock()
        self._thread_safe = bool(thread_safe)

        # Weights: gating matrix + experts.
        self._gating_weights: np.ndarray = self._rng.standard_normal(
            (self.num_experts, self.feature_dim),
        )
        self._experts: List[Expert] = [
            Expert(
                name=name,
                weights=(
                    self._rng.standard_normal(
                        (self.feature_dim, self.num_actions),
                    ) * 0.1
                ),
            )
            for name in self._expert_names
        ]

        # Per-expert metrics.
        self._updates: int = 0
        self._expert_update_counts: np.ndarray = np.zeros(
            self.num_experts, dtype=np.float64,
        )
        self._expert_reward_sums: np.ndarray = np.zeros(
            self.num_experts, dtype=np.float64,
        )
        self._expert_reward_counts: np.ndarray = np.zeros(
            self.num_experts, dtype=np.float64,
        )

    # ------------------------------------------------------------------ #
    # Properties
    # ------------------------------------------------------------------ #

    @property
    def expert_names(self) -> Tuple[str, ...]:
        """Tuple of expert names in index order."""
        return self._expert_names

    @property
    def experts(self) -> Tuple[Expert, ...]:
        """Tuple of expert instances (read-only view)."""
        return tuple(self._experts)

    @property
    def gating_weights(self) -> np.ndarray:
        """Snapshot of the gating weight matrix."""
        with self._lock:
            return self._gating_weights.copy()

    @property
    def expert_weights(self) -> np.ndarray:
        """Stacked snapshot ``(num_experts, feature_dim, num_actions)``.

        The returned array is a fresh copy; mutating it does **not**
        affect the model. To update an expert's weights, mutate
        ``self.experts[i].weights`` under the lock (or use
        :meth:`add_training_sample`).
        """
        with self._lock:
            return np.stack([e.weights for e in self._experts], axis=0)

    # ------------------------------------------------------------------ #
    # State encoding
    # ------------------------------------------------------------------ #

    def _encode_state(
        self, state: Union[Mapping[str, Any], TwinOptimizationState],
    ) -> np.ndarray:
        """Return a validated ``(feature_dim,)`` float64 feature vector."""
        # Fast path: the canonical dataclass with the canonical features.
        if (
            isinstance(state, TwinOptimizationState)
            and self._feature_names == _FEATURE_NAMES
        ):
            vec = np.asarray(state.to_feature_vector(), dtype=np.float64)
            if not np.all(np.isfinite(vec)):
                raise DigitalTwinInputError(
                    "state contains non-finite values."
                )
            return vec

        if isinstance(state, ABCMapping):
            getter = state.get
        elif hasattr(state, "__dict__") or hasattr(
            state, "__dataclass_fields__",
        ):
            getter = lambda name, _s=state: getattr(_s, name, 0.0)  # noqa: E731
        else:
            raise DigitalTwinInputError(
                "state must be a Mapping, a TwinOptimizationState, or a "
                f"dataclass-like object; got {type(state).__name__}."
            )

        values = np.empty(self.feature_dim, dtype=np.float64)
        for i, name in enumerate(self._feature_names):
            v = getter(name, 0.0) if callable(getter) is False else getter(name)
            try:
                fv = float(v)
            except (TypeError, ValueError) as exc:
                raise DigitalTwinInputError(
                    f"feature {name!r} is not numeric: {v!r}."
                ) from exc
            if not math.isfinite(fv):
                raise DigitalTwinInputError(
                    f"feature {name!r} must be finite, got {fv!r}."
                )
            values[i] = fv
        return values

    # ------------------------------------------------------------------ #
    # Internal inference
    # ------------------------------------------------------------------ #

    def _gate(self, state_vec: np.ndarray) -> np.ndarray:
        """Compute gating probabilities for ``state_vec``."""
        logits = self._gating_weights @ state_vec
        return _softmax(logits)

    def _expert_probs_all(self, state_vec: np.ndarray) -> np.ndarray:
        """Stacked ``(num_experts, num_actions)`` expert distributions."""
        return np.stack(
            [e.predict(state_vec) for e in self._experts], axis=0,
        )

    def _pick_action_idx(
        self,
        combined: np.ndarray,
        *,
        temperature: float,
        sampling: bool,
    ) -> int:
        """Return an action index from ``combined`` with optional sampling."""
        if temperature != 1.0 and temperature > 0:
            logits = np.log(np.clip(combined, _EPS, None)) / temperature
            probs = _softmax(logits)
        else:
            probs = combined
        if sampling:
            # Generator.choice requires float64 and sum-to-1.
            probs = np.asarray(probs, dtype=np.float64)
            probs = probs / probs.sum()
            return int(self._rng.choice(self.num_actions, p=probs))
        return int(np.argmax(probs))

    # ------------------------------------------------------------------ #
    # Public: select_expert
    # ------------------------------------------------------------------ #

    async def select_expert(
        self,
        state: Union[Mapping[str, Any], TwinOptimizationState],
        *,
        log_decision: bool = True,
        exploration: bool = False,
        epsilon: float = 0.0,
        temperature: float = 1.0,
    ) -> MoEResult:
        """Select an expert and an action for ``state``.

        Parameters
        ----------
        state:
            Feature mapping or :class:`TwinOptimizationState`.
        log_decision:
            When ``True`` (default) and ``storage`` exposes
            ``log_routing_decision``, log the routing decision.
        exploration:
            When ``True``, sample from the action distribution (and,
            with probability ``epsilon``, from the uniform distribution).
            Default ``False`` preserves the original deterministic
            ``argmax`` behavior.
        epsilon:
            Probability of a uniform-random action when
            ``exploration=True``. Must be in ``[0, 1]``.
        temperature:
            Softmax temperature applied to the combined distribution
            when sampling. ``1.0`` means no rescaling. Must be ``> 0``.

        Returns
        -------
        MoEResult
            See class docstring for the routing / contributing expert
            distinction.
        """
        if not (0.0 <= epsilon <= 1.0):
            raise DigitalTwinInputError("epsilon must be in [0, 1].")
        if not (temperature > 0.0) or not math.isfinite(temperature):
            raise DigitalTwinInputError(
                "temperature must be a positive finite number."
            )

        state_vec = self._encode_state(state)

        # Snapshot the shared tensors under the lock.
        with self._lock:
            gate = self._gate(state_vec)
            expert_probs = self._expert_probs_all(state_vec)

        combined = (gate[:, None] * expert_probs).sum(axis=0)
        combined = combined / max(float(combined.sum()), _EPS)

        explored = False
        if exploration and epsilon > 0 and self._rng.random() < epsilon:
            selected_idx = int(self._rng.integers(0, self.num_actions))
            explored = True
        else:
            selected_idx = self._pick_action_idx(
                combined,
                temperature=temperature,
                sampling=bool(exploration),
            )

        # Routing expert: highest gate weight.
        routing_idx = int(np.argmax(gate))
        # Contributing expert: highest weighted contribution to the
        # *selected* action. This is the more informative label for
        # training signals.
        contributions = gate * expert_probs[:, selected_idx]
        contributing_idx = int(np.argmax(contributions))

        gate_entropy = float(
            -np.sum(gate * np.log(np.clip(gate, _EPS, None)))
        )

        result = MoEResult(
            expert_name=self._expert_names[routing_idx],
            gate_probs=tuple(float(x) for x in gate),
            action_probs=tuple(float(x) for x in combined),
            selected_action=ACTION_SPACE[selected_idx],
            selected_action_idx=selected_idx,
            contributing_expert_name=self._expert_names[contributing_idx],
            gate_entropy=gate_entropy,
            explored=explored,
        )

        if log_decision:
            self._log_routing_decision(state, result, gate)

        return result

    def _log_routing_decision(
        self,
        state: Union[Mapping[str, Any], TwinOptimizationState],
        result: MoEResult,
        gate: np.ndarray,
    ) -> None:
        """Best-effort routing-decision logging via ``self.storage``."""
        if self.storage is None:
            return
        fn = getattr(self.storage, "log_routing_decision", None)
        if fn is None:
            return
        try:
            payload = _state_payload(state)
            sample_id = hashlib.sha256(
                json.dumps(
                    payload, sort_keys=True, default=str,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()[:16]
            # Log the routing expert (argmax gate) and its gate value.
            routing_idx = self._expert_names.index(result.expert_name)
            fn(
                str(uuid.uuid4()),
                sample_id,
                result.expert_name,
                float(gate[routing_idx]),
            )
        except Exception as exc:  # noqa: BLE001 - best-effort
            logger.warning("Failed to log routing decision: %s", exc)

    # ------------------------------------------------------------------ #
    # Public: add_training_sample
    # ------------------------------------------------------------------ #

    async def add_training_sample(
        self,
        state: Union[Mapping[str, Any], TwinOptimizationState],
        selected_expert: str,
        *args: Any,
        action: Optional[int] = None,
        reward: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Online update for the gate and one expert.

        New signature (preferred)::

            await net.add_training_sample(state, expert, action, reward)

        or with keyword arguments::

            await net.add_training_sample(
                state, expert, action=3, reward=0.5,
            )

        Deprecated signature (still accepted, emits ``DeprecationWarning``)::

            await net.add_training_sample(state, expert, reward)

        When the deprecated form is used, the expert's argmax action is
        used as the training target, matching the original behavior.

        Returns
        -------
        dict
            Diagnostics: ``reward``, ``action_idx``, ``used_argmax``
            (``True`` iff the action was inferred), ``gate_lr``,
            ``expert_lr``.
        """
        # ---- Argument normalization (deprecation shim) --------------- #
        used_argmax = False
        if len(args) == 1 and action is None and reward is None:
            warnings.warn(
                _DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2,
            )
            reward = args[0]
            action = None
            used_argmax = True
        elif len(args) == 2 and action is None and reward is None:
            action, reward = args
        elif len(args) == 0 and (action is not None or reward is not None):
            if action is None or reward is None:
                raise TypeError(
                    "add_training_sample requires both 'action' and "
                    "'reward' when using keyword arguments."
                )
        else:
            raise TypeError(
                "add_training_sample expects "
                "(state, expert, reward) [deprecated] or "
                "(state, expert, action, reward)."
            )

        if reward is None:
            raise TypeError("add_training_sample is missing 'reward'.")

        reward_v = _validate_finite_float("reward", reward)
        assert reward_v is not None  # for type checkers

        if selected_expert not in self._expert_names_set:
            raise DigitalTwinInputError(
                f"unknown expert {selected_expert!r}."
            )
        expert_idx = self._expert_names.index(selected_expert)
        state_vec = self._encode_state(state)

        # ---- Resolve action ------------------------------------------ #
        with self._lock:
            if action is None:
                # Deprecated path: use the expert's current argmax.
                expert_probs = self._experts[expert_idx].predict(state_vec)
                action_idx = int(np.argmax(expert_probs))
                used_argmax = True
            else:
                action_idx = _validate_action_idx(
                    "action", action, self.num_actions,
                )

        # ---- Gradient computation ------------------------------------ #
        with self._lock:
            # Gating gradient: push gate toward the selected expert on
            # positive reward, away on negative reward.
            gate = self._gate(state_vec)
            target = np.zeros(self.num_experts, dtype=np.float64)
            target[expert_idx] = 1.0
            grad_gate = (
                reward_v * (gate - target)[:, None] * state_vec[None, :]
            )

            # Expert gradient: policy-gradient-like step toward the
            # *actual* action taken (or the argmax in the deprecated
            # path).
            expert_probs = self._experts[expert_idx].predict(state_vec)
            one_hot = np.zeros(self.num_actions, dtype=np.float64)
            one_hot[action_idx] = 1.0
            grad_expert = -reward_v * np.outer(
                state_vec, (one_hot - expert_probs),
            )

            # Gradient clipping.
            if self._grad_clip > 0:
                gn = float(np.linalg.norm(grad_gate))
                if gn > self._grad_clip:
                    grad_gate = grad_gate * (self._grad_clip / gn)
                gn = float(np.linalg.norm(grad_expert))
                if gn > self._grad_clip:
                    grad_expert = grad_expert * (self._grad_clip / gn)

            # Apply updates.
            self._gating_weights -= self._gate_lr * grad_gate
            self._experts[expert_idx].weights -= (
                self._expert_lr * grad_expert
            )

            # Optional weight decay.
            if self._weight_decay > 0:
                factor = 1.0 - self._weight_decay
                self._gating_weights *= factor
                self._experts[expert_idx].weights *= factor

            # Per-expert metrics.
            self._updates += 1
            self._expert_update_counts[expert_idx] += 1
            self._expert_reward_sums[expert_idx] += reward_v
            self._expert_reward_counts[expert_idx] += 1

        return {
            "expert": selected_expert,
            "action_idx": action_idx,
            "reward": reward_v,
            "used_argmax": used_argmax,
            "gate_lr": self._gate_lr,
            "expert_lr": self._expert_lr,
        }

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the full training state."""
        with self._lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "expert_names": list(self._expert_names),
                "feature_names": list(self._feature_names),
                "lr": self._lr,
                "gate_lr": self._gate_lr,
                "expert_lr": self._expert_lr,
                "grad_clip": self._grad_clip,
                "weight_decay": self._weight_decay,
                "gating_weights": self._gating_weights.tolist(),
                "expert_weights": [
                    e.weights.tolist() for e in self._experts
                ],
                "updates": self._updates,
                "expert_update_counts": (
                    self._expert_update_counts.tolist()
                ),
                "expert_reward_sums": self._expert_reward_sums.tolist(),
                "expert_reward_counts": (
                    self._expert_reward_counts.tolist()
                ),
                "rng_state": _serialize_rng_state(self._rng),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        seed: int = 0,
        storage: Optional[Any] = None,
    ) -> "MoEGatingNetwork":
        """Rebuild a network from :meth:`to_dict` output.

        Raises
        ------
        DigitalTwinInputError
            On an unsupported ``schema_version``, shape mismatch, or
            malformed payload.
        """
        version = int(data.get("schema_version", 1))
        if version > SCHEMA_VERSION:
            raise DigitalTwinInputError(
                f"schema_version {version} is newer than supported "
                f"({SCHEMA_VERSION})."
            )

        names = list(data.get("expert_names") or _DEFAULT_EXPERT_NAMES)
        feature_names = data.get("feature_names")

        obj = cls(
            storage=storage,
            num_experts=len(names),
            seed=seed,
            lr=float(data.get("lr", 0.1)),
            gate_lr=data.get("gate_lr"),
            expert_lr=data.get("expert_lr"),
            expert_names=names,
            grad_clip=float(data.get("grad_clip", 1.0)),
            weight_decay=float(data.get("weight_decay", 0.0)),
            feature_names=feature_names,
        )

        # Gating weights shape check.
        gw = data.get("gating_weights")
        if gw is not None:
            arr = np.asarray(gw, dtype=np.float64)
            expected = (obj.num_experts, obj.feature_dim)
            if arr.shape != expected:
                raise DigitalTwinInputError(
                    f"gating_weights shape {arr.shape} != expected "
                    f"{expected}."
                )
            if not np.all(np.isfinite(arr)):
                raise DigitalTwinInputError(
                    "gating_weights contains non-finite values."
                )
            obj._gating_weights = arr.copy()

        # Expert weights shape check.
        ew = data.get("expert_weights")
        if ew is not None:
            arr = np.asarray(ew, dtype=np.float64)
            expected = (
                obj.num_experts, obj.feature_dim, obj.num_actions,
            )
            if arr.shape != expected:
                raise DigitalTwinInputError(
                    f"expert_weights shape {arr.shape} != expected "
                    f"{expected}."
                )
            if not np.all(np.isfinite(arr)):
                raise DigitalTwinInputError(
                    "expert_weights contains non-finite values."
                )
            for i, expert in enumerate(obj._experts):
                expert.weights = arr[i].copy()

        # Optional RNG state.
        _restore_rng_state(obj._rng, data.get("rng_state") or {})

        # Counters / metrics (shape-checked, defaulting to zero).
        obj._updates = int(data.get("updates", 0))
        for key, target in (
            ("expert_update_counts", obj._expert_update_counts),
            ("expert_reward_sums", obj._expert_reward_sums),
            ("expert_reward_counts", obj._expert_reward_counts),
        ):
            src = data.get(key)
            if src is None:
                continue
            arr = np.asarray(src, dtype=np.float64)
            if arr.shape == target.shape:
                target[...] = arr
            else:
                logger.warning(
                    "Ignoring %s with wrong shape %s (expected %s).",
                    key, arr.shape, target.shape,
                )

        return obj

    # ------------------------------------------------------------------ #
    # Statistics / lifecycle
    # ------------------------------------------------------------------ #

    def statistics(self) -> Dict[str, Any]:
        """Return an enriched snapshot of the network's state."""
        with self._lock:
            counts = self._expert_reward_counts.copy()
            sums = self._expert_reward_sums.copy()
            updates = int(self._updates)
            update_counts = self._expert_update_counts.copy()

        avg_reward = np.where(
            counts > 0, sums / np.maximum(counts, 1.0), 0.0,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "num_experts": self.num_experts,
            "expert_names": list(self._expert_names),
            "num_actions": self.num_actions,
            "feature_dim": self.feature_dim,
            "updates": updates,
            "expert_update_counts": update_counts.tolist(),
            "expert_avg_reward": avg_reward.tolist(),
            "gate_lr": self._gate_lr,
            "expert_lr": self._expert_lr,
            "grad_clip": self._grad_clip,
            "weight_decay": self._weight_decay,
            "thread_safe": self._thread_safe,
            "uses_storage": self.storage is not None,
            "storage_type": (
                type(self.storage).__name__
                if self.storage is not None else None
            ),
        }

    def close(self, *, flush: bool = True) -> None:
        """Flush and/or close the storage backend, if any."""
        if self.storage is None:
            return
        if flush:
            fn = getattr(self.storage, "flush", None)
            if fn is not None:
                fn()
        fn = getattr(self.storage, "close", None)
        if fn is not None:
            fn()

    # ------------------------------------------------------------------ #
    # Dunders
    # ------------------------------------------------------------------ #

    def __contains__(self, expert_name: object) -> bool:
        return (
            isinstance(expert_name, str)
            and expert_name in self._expert_names_set
        )

    def __len__(self) -> int:
        return self.num_experts

    def __repr__(self) -> str:
        storage_name = (
            type(self.storage).__name__ if self.storage is not None else None
        )
        return (
            f"{type(self).__name__}(num_experts={self.num_experts}, "
            f"feature_dim={self.feature_dim}, "
            f"num_actions={self.num_actions}, "
            f"storage={storage_name})"
        )


__all__ = [
    "SCHEMA_VERSION",
    "Expert",
    "MoEGatingNetwork",
    "MoEResult",
]
