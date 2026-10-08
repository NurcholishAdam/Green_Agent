# src/quantum_integration/digital_twin/digital_twin_rlhf.py

"""RLHF: preference pairs and a small Bradley-Terry linear reward model."""

from __future__ import annotations

import json
import logging
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .digital_twin_errors import DigitalTwinInputError
from .digital_twin_helpers import _LazyLock, _iso_now, _parse_iso_datetime

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1


@dataclass(frozen=True)
class PreferencePair:
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
        for name in ("pair_id", "prompt", "chosen", "rejected"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise DigitalTwinInputError(
                    f"{name} must be a non-empty string."
                )
        if not isinstance(self.reward_diff, (int, float)) or isinstance(
            self.reward_diff, bool,
        ):
            raise DigitalTwinInputError("reward_diff must be numeric.")
        if self.chosen == self.rejected:
            raise DigitalTwinInputError(
                "chosen and rejected must differ."
            )

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
    def from_dict(cls, data: Mapping[str, Any]) -> "PreferencePair":
        if not isinstance(data, Mapping):
            raise DigitalTwinInputError(
                "PreferencePair.from_dict expects a Mapping."
            )
        ts = _parse_iso_datetime(data.get("observed_at")) or datetime.now(timezone.utc)
        return cls(
            pair_id=str(data["pair_id"]),
            prompt=str(data["prompt"]),
            chosen=str(data["chosen"]),
            rejected=str(data["rejected"]),
            reward_diff=float(data.get("reward_diff", 0.0)),
            observed_at=ts,
            metadata=dict(data.get("metadata") or {}),
        )


class RewardModel:
    """Small linear Bradley-Terry reward model.

    Features are derived from a prompt by hashing key tokens (a stand-in
    for a real tokenizer). The model is intentionally simple — the point
    is to demonstrate a working training loop, not to be competitive.
    """

    def __init__(
        self,
        feature_dim: int = 32,
        lr: float = 0.01,
        seed: int = 0,
    ) -> None:
        self.feature_dim = feature_dim
        self.lr = lr
        self.weights = [0.0] * feature_dim
        self._rng = random.Random(seed)
        self.trained_pairs = 0

    def _encode(self, prompt: str) -> List[float]:
        vec = [0.0] * self.feature_dim
        for token in prompt.lower().split():
            idx = hash(token) % self.feature_dim
            vec[idx] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def predict(self, prompt: str) -> float:
        x = self._encode(prompt)
        return sum(w * xi for w, xi in zip(self.weights, x))

    def train_step(self, pair: PreferencePair) -> float:
        """One Bradley-Terry gradient step. Returns the loss."""
        c = self._encode(pair.chosen)
        r = self._encode(pair.rejected)
        diff = [ci - ri for ci, ri in zip(c, r)]
        score = sum(w * d for w, d in zip(self.weights, diff))
        sig = 1.0 / (1.0 + math.exp(-score))
        # BT loss: -log(sigmoid(score)).
        loss = -math.log(max(sig, 1e-12))
        # Gradient: -(1 - sig) * diff.
        grad_coef = -(1.0 - sig)
        for i in range(self.feature_dim):
            self.weights[i] -= self.lr * grad_coef * diff[i]
        self.trained_pairs += 1
        return loss

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feature_dim": self.feature_dim,
            "lr": self.lr,
            "weights": list(self.weights),
            "trained_pairs": self.trained_pairs,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, seed: int = 0,
    ) -> "RewardModel":
        obj = cls(
            feature_dim=int(data.get("feature_dim", 32)),
            lr=float(data.get("lr", 0.01)),
            seed=seed,
        )
        w = data.get("weights")
        if isinstance(w, list):
            obj.weights = [float(x) for x in w]
        obj.trained_pairs = int(data.get("trained_pairs", 0))
        return obj


class RLHFTrainer:
    """Collects preference pairs and trains a linear reward model."""

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        seed: int = 0,
        feature_dim: int = 32,
        lr: float = 0.01,
        min_pairs_for_training: int = 5,
    ) -> None:
        self.storage = storage
        self._rng = random.Random(seed)
        self._pairs: Dict[str, PreferencePair] = {}
        self.reward_model = RewardModel(
            feature_dim=feature_dim, lr=lr, seed=seed,
        )
        self.min_pairs_for_training = max(1, int(min_pairs_for_training))

    def record_pair(
        self,
        pair_id: str,
        prompt: str,
        chosen: str,
        rejected: str,
        reward_diff: float,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> PreferencePair:
        if pair_id in self._pairs:
            raise DigitalTwinInputError(
                f"preference pair {pair_id!r} already recorded."
            )
        pair = PreferencePair(
            pair_id=pair_id,
            prompt=prompt,
            chosen=chosen,
            rejected=rejected,
            reward_diff=float(reward_diff),
            metadata=dict(metadata or {}),
        )
        self._pairs[pair_id] = pair
        if self.storage and hasattr(self.storage, "save_preference_pair"):
            try:
                self.storage.save_preference_pair(
                    pair_id, prompt, chosen, rejected,
                    float(reward_diff), dict(metadata or {}),
                )
            except Exception as exc:  # noqa: BLE001 - best-effort
                logger.warning("Failed to persist preference pair: %s", exc)
        return pair

    def get_pairs(self, limit: int = 100) -> List[PreferencePair]:
        pairs = list(self._pairs.values())
        if limit is not None:
            pairs = pairs[-int(limit):]
        return pairs

    def pair_count(self) -> int:
        return len(self._pairs)

    def clear_pairs(self) -> int:
        removed = len(self._pairs)
        self._pairs.clear()
        return removed

    def train_reward_model(self, *, epochs: int = 1) -> Dict[str, float]:
        pairs = self.get_pairs()
        if len(pairs) < self.min_pairs_for_training:
            logger.info(
                "Not enough pairs (%d < %d) for RLHF training.",
                len(pairs), self.min_pairs_for_training,
            )
            return {"trained": 0.0, "mean_loss": 0.0}

        losses: List[float] = []
        for _ in range(max(1, epochs)):
            self._rng.shuffle(pairs)
            for p in pairs:
                losses.append(self.reward_model.train_step(p))
        return {
            "trained": float(len(losses)),
            "mean_loss": sum(losses) / len(losses) if losses else 0.0,
        }

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "pair_count": len(self._pairs),
            "trained_pairs": self.reward_model.trained_pairs,
            "reward_model": self.reward_model.to_dict(),
        }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = [
    "SCHEMA_VERSION",
    "PreferencePair",
    "RLHFTrainer",
    "RewardModel",
]
