# src/quantum_integration/digital_twin/digital_twin_rlhf.py

"""RLHF: preference pairs and a small Bradley-Terry linear reward model.

Overview
--------
* :class:`PreferencePair` — a frozen, hashable record of one preference
  observation.
* :class:`RewardModel` — a linear Bradley-Terry reward model with a
  deterministic, process-stable feature encoder.
* :class:`RLHFTrainer` — collects preference pairs, persists them
  (best-effort) via an optional storage backend, and trains the reward
  model with a numerically stable BT gradient step.

Determinism
-----------
Feature indices are computed with BLAKE2b, so a reward model trained in
one process produces the same predictions when reloaded in another.
The legacy ``hash()``-based encoder was process-randomized and is not
compatible; see :data:`SCHEMA_VERSION`.

Thread safety
-------------
Pair storage is guarded by an ``RLock`` when ``thread_safe=True``
(default). Concurrent ``record_pair`` calls cannot race on the
duplicate check.

Cancellation and finiteness
---------------------------
All numeric inputs are validated to be finite; ``NaN`` / ``inf`` cannot
enter the model or corrupt training.
"""

from __future__ import annotations

import hashlib
import logging
import math
import random
import threading
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

from .digital_twin_errors import DigitalTwinInputError
from .digital_twin_helpers import _parse_iso_datetime

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

#: Sentinel returned by ``_try_storage`` when the backend lacks a hook.
_NO_STORAGE: Any = object()

_EPS: float = 1e-12


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #

def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DigitalTwinInputError(
            f"{name} must be a non-empty, non-whitespace string."
        )
    return value


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DigitalTwinInputError(f"{name} must be an integer.")
    if value < 1:
        raise DigitalTwinInputError(f"{name} must be >= 1.")
    return int(value)


def _require_finite_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    v = float(value)
    if not math.isfinite(v):
        raise DigitalTwinInputError(f"{name} must be finite.")
    return v


def _require_positive_finite(name: str, value: Any) -> float:
    v = _require_finite_float(name, value)
    if v <= 0:
        raise DigitalTwinInputError(f"{name} must be > 0.")
    return v


# --------------------------------------------------------------------------- #
# Numerically stable sigmoid
# --------------------------------------------------------------------------- #

def _sigmoid(x: float) -> float:
    """Numerically stable logistic sigmoid.

    Avoids ``OverflowError`` for large negative ``x`` (the naive
    ``1 / (1 + exp(-x))`` overflows when ``-x > ~709``).
    """
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


# --------------------------------------------------------------------------- #
# Storage protocol
# --------------------------------------------------------------------------- #

@runtime_checkable
class _RLHFStorage(Protocol):
    """Duck-typed protocol for RLHF storage backends.

    Every method is optional. The trainer probes for individual methods
    at runtime so a backend may implement only a subset.
    """

    def save_preference_pair(
        self,
        pair_id: str,
        prompt: str,
        chosen: str,
        rejected: str,
        reward_diff: float,
        metadata: Mapping[str, Any],
    ) -> None: ...

    def get_preference_pairs(self) -> List[Dict[str, Any]]: ...

    def remove_preference_pair(self, pair_id: str) -> bool: ...

    def flush(self) -> None: ...
    def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# PreferencePair
# --------------------------------------------------------------------------- #

@dataclass(frozen=True, eq=True)
class PreferencePair:
    """A frozen record of one preference observation.

    ``metadata`` is excluded from equality and hashing because arbitrary
    mappings are not hashable. Two pairs with the same identifying
    fields but different metadata compare equal.
    """

    pair_id: str
    prompt: str
    chosen: str
    rejected: str
    reward_diff: float
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_nonempty_str("pair_id", self.pair_id)
        _require_nonempty_str("prompt", self.prompt)
        _require_nonempty_str("chosen", self.chosen)
        _require_nonempty_str("rejected", self.rejected)
        if self.chosen == self.rejected:
            raise DigitalTwinInputError(
                "chosen and rejected must differ."
            )
        rd = _require_finite_float("reward_diff", self.reward_diff)
        object.__setattr__(self, "reward_diff", rd)
        if not isinstance(self.observed_at, datetime):
            raise DigitalTwinInputError(
                "observed_at must be a datetime."
            )
        if self.observed_at.tzinfo is None:
            object.__setattr__(
                self,
                "observed_at",
                self.observed_at.replace(tzinfo=timezone.utc),
            )
        if not isinstance(self.metadata, ABCMapping):
            raise DigitalTwinInputError("metadata must be a Mapping.")
        object.__setattr__(self, "metadata", dict(self.metadata))

    def __hash__(self) -> int:
        # Exclude ``metadata`` (unhashable) and ``observed_at`` (datetime
        # equality is well-defined but two equal instants with different
        # tz offsets should hash the same).
        return hash((
            self.pair_id,
            self.prompt,
            self.chosen,
            self.rejected,
            self.reward_diff,
        ))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "prompt": self.prompt,
            "chosen": self.chosen,
            "rejected": self.rejected,
            "reward_diff": float(self.reward_diff),
            "observed_at": self.observed_at.isoformat(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> "PreferencePair":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinInputError(
                "PreferencePair.from_dict expects a Mapping."
            )
        raw_ts = data.get("observed_at")
        ts = _parse_iso_datetime(raw_ts)
        if ts is None:
            if strict and raw_ts is not None:
                raise DigitalTwinInputError(
                    f"invalid observed_at: {raw_ts!r}."
                )
            if raw_ts is not None:
                logger.warning(
                    "PreferencePair.from_dict: unparseable observed_at "
                    "%r; substituting current time.", raw_ts,
                )
            ts = datetime.now(timezone.utc)
        return cls(
            pair_id=str(data["pair_id"]),
            prompt=str(data["prompt"]),
            chosen=str(data["chosen"]),
            rejected=str(data["rejected"]),
            reward_diff=float(data.get("reward_diff", 0.0)),
            observed_at=ts,
            metadata=dict(data.get("metadata") or {}),
        )


# --------------------------------------------------------------------------- #
# RewardModel
# --------------------------------------------------------------------------- #

class RewardModel:
    """Small linear Bradley-Terry reward model.

    Features are derived from text by hashing tokens with BLAKE2b so
    feature indices are stable across processes and runs. This is a
    stand-in for a real tokenizer; pass ``encoder`` to override.

    Parameters
    ----------
    feature_dim:
        Number of features. Must be a positive integer.
    lr:
        Learning rate. Must be positive and finite.
    seed:
        Seed for the deterministic RNG. Kept for API compatibility;
        training is currently deterministic given the input order.
    encoder:
        Optional ``Callable[[str], Sequence[float]]``. When provided, it
        must return a vector of length ``feature_dim``.
    """

    def __init__(
        self,
        feature_dim: int = 32,
        lr: float = 0.01,
        seed: int = 0,
        *,
        encoder: Optional[Any] = None,
    ) -> None:
        self.feature_dim = _require_positive_int("feature_dim", feature_dim)
        self.lr = _require_positive_finite("lr", lr)
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise DigitalTwinInputError("seed must be an integer.")
        self.weights: List[float] = [0.0] * self.feature_dim
        self._rng = random.Random(int(seed))
        self.trained_pairs = 0
        self._encoder = encoder

        # Diagnostic counters.
        self._last_loss: Optional[float] = None
        self._last_grad_norm: Optional[float] = None

    # -- encoding ------------------------------------------------------- #

    def _token_index(self, token: str) -> int:
        """Return a process-stable feature index for ``token``."""
        h = hashlib.blake2b(
            token.encode("utf-8"), digest_size=8,
        ).digest()
        return int.from_bytes(h, "big") % self.feature_dim

    def _encode(self, prompt: str) -> List[float]:
        """Bag-of-tokens encoder with L2 normalization."""
        if not isinstance(prompt, str):
            raise DigitalTwinInputError("prompt must be a string.")
        if self._encoder is not None:
            vec = list(self._encoder(prompt))
            if len(vec) != self.feature_dim:
                raise DigitalTwinInputError(
                    f"custom encoder returned length {len(vec)}, "
                    f"expected {self.feature_dim}."
                )
            return [float(v) for v in vec]
        vec = [0.0] * self.feature_dim
        for token in prompt.lower().split():
            idx = self._token_index(token)
            vec[idx] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    # -- inference ------------------------------------------------------ #

    def predict(self, prompt: str) -> float:
        """Return the scalar reward for ``prompt``."""
        x = self._encode(prompt)
        return sum(w * xi for w, xi in zip(self.weights, x))

    def predict_many(self, prompts: List[str]) -> List[float]:
        """Batched prediction. Convenience wrapper around ``predict``."""
        return [self.predict(p) for p in prompts]

    def pairwise_accuracy(self, pairs: List[PreferencePair]) -> float:
        """Fraction of pairs where ``reward(chosen) > reward(rejected)``."""
        if not pairs:
            return 0.0
        correct = 0
        for p in pairs:
            if self.predict(p.chosen) > self.predict(p.rejected):
                correct += 1
        return correct / len(pairs)

    # -- training ------------------------------------------------------- #

    def train_step(self, pair: PreferencePair) -> float:
        """One Bradley-Terry gradient step. Returns the loss.

        The update is ``w -= lr * grad_coef * (φ(chosen) - φ(rejected))``
        where ``grad_coef = -(1 - sigmoid(w·(φ(c) - φ(r))))``. The
        gradient norm is recorded for diagnostics.
        """
        c = self._encode(pair.chosen)
        r = self._encode(pair.rejected)
        diff = [ci - ri for ci, ri in zip(c, r)]
        score = sum(w * d for w, d in zip(self.weights, diff))
        sig = _sigmoid(score)
        loss = -math.log(max(sig, _EPS))
        grad_coef = -(1.0 - sig)

        grad = [grad_coef * d for d in diff]
        grad_norm = math.sqrt(sum(g * g for g in grad))
        for i in range(self.feature_dim):
            self.weights[i] -= self.lr * grad[i]

        self.trained_pairs += 1
        self._last_loss = loss
        self._last_grad_norm = grad_norm
        return loss

    # -- serialization -------------------------------------------------- #

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "feature_dim": self.feature_dim,
            "lr": self.lr,
            "weights": list(self.weights),
            "trained_pairs": self.trained_pairs,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, seed: int = 0,
    ) -> "RewardModel":
        version = int(data.get("schema_version", 1))
        if version > SCHEMA_VERSION:
            raise DigitalTwinInputError(
                f"schema_version {version} is newer than supported "
                f"({SCHEMA_VERSION})."
            )
        obj = cls(
            feature_dim=int(data.get("feature_dim", 32)),
            lr=float(data.get("lr", 0.01)),
            seed=seed,
        )
        w = data.get("weights")
        if isinstance(w, list):
            w_parsed = [float(x) for x in w]
            if len(w_parsed) != obj.feature_dim:
                raise DigitalTwinInputError(
                    f"weights length {len(w_parsed)} != feature_dim "
                    f"{obj.feature_dim}."
                )
            if not all(math.isfinite(x) for x in w_parsed):
                raise DigitalTwinInputError(
                    "weights contain non-finite values."
                )
            obj.weights = w_parsed
        obj.trained_pairs = int(data.get("trained_pairs", 0))
        return obj

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(feature_dim={self.feature_dim}, "
            f"lr={self.lr}, trained_pairs={self.trained_pairs})"
        )


# --------------------------------------------------------------------------- #
# RLHFTrainer
# --------------------------------------------------------------------------- #

class RLHFTrainer:
    """Collects preference pairs and trains a linear reward model.

    Parameters
    ----------
    storage:
        Optional backend implementing :class:`_RLHFStorage` hooks. Only
        ``save_preference_pair`` is probed on ``record_pair``.
    seed:
        Seed for the trainer RNG (used for shuffling).
    feature_dim, lr:
        Passed through to :class:`RewardModel`.
    min_pairs_for_training:
        Minimum number of pairs required before training runs.
    thread_safe:
        When ``True`` (default), pair storage is guarded by an
        ``RLock``.
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        seed: int = 0,
        feature_dim: int = 32,
        lr: float = 0.01,
        min_pairs_for_training: int = 5,
        thread_safe: bool = True,
    ) -> None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise DigitalTwinInputError("seed must be an integer.")

        self.storage = storage
        self.seed = int(seed)
        self.feature_dim = _require_positive_int("feature_dim", feature_dim)
        self.lr = _require_positive_finite("lr", lr)
        self.min_pairs_for_training = _require_positive_int(
            "min_pairs_for_training", min_pairs_for_training,
        )

        self._rng = random.Random(self.seed)
        self._pairs: Dict[str, PreferencePair] = {}
        self._lock: Any = (
            threading.RLock() if thread_safe else _NullLock()
        )
        self._thread_safe = bool(thread_safe)

        self.reward_model = RewardModel(
            feature_dim=self.feature_dim, lr=self.lr, seed=self.seed,
        )

        # Diagnostics.
        self.last_persist_error: Optional[Exception] = None
        self._last_train_metrics: Dict[str, Any] = {}

    # -- storage dispatch ----------------------------------------------- #

    def _try_storage(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        if self.storage is None:
            return _NO_STORAGE
        fn = getattr(self.storage, method_name, None)
        if fn is None:
            return _NO_STORAGE
        return fn(*args, **kwargs)

    # -- pair management ------------------------------------------------ #

    def record_pair(
        self,
        pair_id: str,
        prompt: str,
        chosen: str,
        rejected: str,
        reward_diff: float,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> PreferencePair:
        """Record a preference pair.

        Raises
        ------
        DigitalTwinInputError
            On duplicate ``pair_id`` or invalid input.
        """
        pair = PreferencePair(
            pair_id=pair_id,
            prompt=prompt,
            chosen=chosen,
            rejected=rejected,
            reward_diff=reward_diff,
            metadata=dict(metadata or {}),
        )
        with self._lock:
            if pair.pair_id in self._pairs:
                raise DigitalTwinInputError(
                    f"preference pair {pair.pair_id!r} already recorded."
                )
            self._pairs[pair.pair_id] = pair

        # Best-effort persistence outside the lock.
        if self.storage is not None:
            try:
                saved = self._try_storage(
                    "save_preference_pair",
                    pair.pair_id,
                    pair.prompt,
                    pair.chosen,
                    pair.rejected,
                    float(pair.reward_diff),
                    dict(pair.metadata),
                )
                if saved is not _NO_STORAGE:
                    self.last_persist_error = None
            except Exception as exc:  # noqa: BLE001 - best-effort
                self.last_persist_error = exc
                logger.warning(
                    "Failed to persist preference pair %r: %s",
                    pair.pair_id, exc,
                )
        return pair

    def get_pairs(self, limit: Optional[int] = 100) -> List[PreferencePair]:
        """Return up to ``limit`` most recently recorded pairs.

        Pass ``limit=None`` for all pairs.
        """
        with self._lock:
            pairs = list(self._pairs.values())
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise DigitalTwinInputError(
                    "limit must be an integer or None."
                )
            if limit < 0:
                raise DigitalTwinInputError("limit must be >= 0.")
            pairs = pairs[-limit:] if limit > 0 else []
        return pairs

    def pair_count(self) -> int:
        with self._lock:
            return len(self._pairs)

    def clear_pairs(self) -> int:
        with self._lock:
            removed = len(self._pairs)
            self._pairs.clear()
        return removed

    def remove_pair(self, pair_id: str) -> bool:
        """Remove a single pair. Returns ``True`` if it existed."""
        _require_nonempty_str("pair_id", pair_id)
        with self._lock:
            existed = self._pairs.pop(pair_id, None) is not None
        if existed:
            try:
                result = self._try_storage("remove_preference_pair", pair_id)
                if result is not _NO_STORAGE:
                    self.last_persist_error = None
            except Exception as exc:  # noqa: BLE001 - best-effort
                self.last_persist_error = exc
                logger.warning(
                    "Failed to remove persisted pair %r: %s",
                    pair_id, exc,
                )
        return existed

    def __contains__(self, pair_id: object) -> bool:
        if not isinstance(pair_id, str):
            return False
        with self._lock:
            return pair_id in self._pairs

    def __len__(self) -> int:
        return self.pair_count()

    # -- training ------------------------------------------------------- #

    def train_reward_model(self, *, epochs: int = 1) -> Dict[str, float]:
        """Train the reward model on the recorded pairs.

        Returns
        -------
        dict
            Keys: ``trained`` (number of gradient steps),
            ``mean_loss``, ``min_loss``, ``max_loss``,
            ``final_loss``, ``mean_grad_norm``, ``pairwise_accuracy``,
            ``epochs``. All values are floats.
        """
        if isinstance(epochs, bool) or not isinstance(epochs, int):
            raise DigitalTwinInputError("epochs must be an integer.")
        epochs = max(1, int(epochs))

        pairs = self.get_pairs(limit=None)
        if len(pairs) < self.min_pairs_for_training:
            logger.info(
                "Not enough pairs (%d < %d) for RLHF training.",
                len(pairs), self.min_pairs_for_training,
            )
            metrics = {
                "trained": 0.0,
                "mean_loss": 0.0,
                "min_loss": 0.0,
                "max_loss": 0.0,
                "final_loss": 0.0,
                "mean_grad_norm": 0.0,
                "pairwise_accuracy": 0.0,
                "epochs": float(epochs),
            }
            self._last_train_metrics = metrics
            return metrics

        losses: List[float] = []
        grad_norms: List[float] = []
        for _ in range(epochs):
            self._rng.shuffle(pairs)
            for p in pairs:
                before = self.reward_model.trained_pairs
                loss = self.reward_model.train_step(p)
                losses.append(loss)
                if self.reward_model.trained_pairs > before:
                    gn = self.reward_model._last_grad_norm
                    if gn is not None:
                        grad_norms.append(gn)

        accuracy = self.reward_model.pairwise_accuracy(pairs)
        metrics = {
            "trained": float(len(losses)),
            "mean_loss": (sum(losses) / len(losses)) if losses else 0.0,
            "min_loss": min(losses) if losses else 0.0,
            "max_loss": max(losses) if losses else 0.0,
            "final_loss": losses[-1] if losses else 0.0,
            "mean_grad_norm": (
                sum(grad_norms) / len(grad_norms) if grad_norms else 0.0
            ),
            "pairwise_accuracy": accuracy,
            "epochs": float(epochs),
        }
        self._last_train_metrics = metrics
        return metrics

    # -- serialization -------------------------------------------------- #

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            pairs = [p.to_dict() for p in self._pairs.values()]
        return {
            "schema_version": SCHEMA_VERSION,
            "seed": self.seed,
            "feature_dim": self.feature_dim,
            "lr": self.lr,
            "min_pairs_for_training": self.min_pairs_for_training,
            "pairs": pairs,
            "reward_model": self.reward_model.to_dict(),
            "last_train_metrics": dict(self._last_train_metrics),
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        storage: Optional[Any] = None,
        strict: bool = False,
    ) -> "RLHFTrainer":
        version = int(data.get("schema_version", 1))
        if version > SCHEMA_VERSION:
            raise DigitalTwinInputError(
                f"schema_version {version} is newer than supported "
                f"({SCHEMA_VERSION})."
            )
        obj = cls(
            storage=storage,
            seed=int(data.get("seed", 0)),
            feature_dim=int(data.get("feature_dim", 32)),
            lr=float(data.get("lr", 0.01)),
            min_pairs_for_training=int(
                data.get("min_pairs_for_training", 5),
            ),
        )
        for pd in data.get("pairs") or []:
            pair = PreferencePair.from_dict(pd, strict=strict)
            with obj._lock:
                obj._pairs[pair.pair_id] = pair
        rm = data.get("reward_model")
        if isinstance(rm, ABCMapping):
            obj.reward_model = RewardModel.from_dict(
                rm, seed=obj.seed,
            )
        metrics = data.get("last_train_metrics")
        if isinstance(metrics, ABCMapping):
            obj._last_train_metrics = dict(metrics)
        return obj

    # -- introspection / lifecycle -------------------------------------- #

    def statistics(self) -> Dict[str, Any]:
        """Return a snapshot of the trainer's state."""
        with self._lock:
            pair_count = len(self._pairs)
        return {
            "schema_version": SCHEMA_VERSION,
            "pair_count": pair_count,
            "trained_pairs": self.reward_model.trained_pairs,
            "feature_dim": self.feature_dim,
            "lr": self.lr,
            "seed": self.seed,
            "min_pairs_for_training": self.min_pairs_for_training,
            "thread_safe": self._thread_safe,
            "reward_model": self.reward_model.to_dict(),
            "last_train_metrics": dict(self._last_train_metrics),
            "uses_storage": self.storage is not None,
            "storage_type": (
                type(self.storage).__name__
                if self.storage is not None else None
            ),
            "last_persist_error_type": (
                type(self.last_persist_error).__name__
                if self.last_persist_error is not None else None
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

    def __repr__(self) -> str:
        with self._lock:
            n = len(self._pairs)
        storage_name = (
            type(self.storage).__name__ if self.storage is not None else None
        )
        return (
            f"{type(self).__name__}(pairs={n}, "
            f"feature_dim={self.feature_dim}, "
            f"trained_pairs={self.reward_model.trained_pairs}, "
            f"storage={storage_name})"
        )


# --------------------------------------------------------------------------- #
# Null lock
# --------------------------------------------------------------------------- #

class _NullLock:
    """No-op context manager used when ``thread_safe=False``."""

    def __enter__(self) -> "_NullLock":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


__all__ = [
    "SCHEMA_VERSION",
    "PreferencePair",
    "RLHFTrainer",
    "RewardModel",
    "_RLHFStorage",
]
