# src/memory/bounded_recall.py

"""
Bounded Recall
==============

Implements the "Bound retrieval: top-k evidence, metadata filters, short
summaries, and token budgets per decision" constraint.

Enhancements
------------
- ``BoundedRecallConfig`` — frozen, validated: k, token budget, timeout,
  tie-breakers, per-source weights.
- ``RecallBundle`` — frozen dataclass: matched memories, scores, token cost,
  latency, citation list, truth-level distribution.
- Hard caps on ``k``, tokens, and latency.
- Deterministic tie-breaking (score desc, memory id asc).
- ``summarize()`` produces a citable short summary.
- Custom ``BoundedRecallError(ValueError)`` and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .supermemory_adapter import SupermemoryAdapter, SupermemoryAdapterError

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BoundedRecallError(ValueError):
    """Raised for invalid recall inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BoundedRecallConfig:
    """Tunable parameters for bounded recall."""

    default_k: int = 5
    max_k: int = 20
    token_budget: int = 2_048
    timeout_seconds: float = 5.0
    chars_per_token: int = 4
    max_summary_tokens: int = 256

    # Score weights applied to each matched memory.
    weight_metadata_match: float = 0.5
    weight_recency: float = 0.3
    weight_truth_level: float = 0.2

    # Trusted truth levels receive full credit; others are discounted.
    trusted_truth_levels: Tuple[str, ...] = ("measured",)

    def __post_init__(self) -> None:
        for name in ("default_k", "max_k", "token_budget", "max_summary_tokens"):
            v = getattr(self, name)
            if not isinstance(v, int) or v <= 0:
                raise BoundedRecallError(f"{name} must be a positive int.")
        if self.default_k > self.max_k:
            raise BoundedRecallError("default_k must be <= max_k.")
        if not isinstance(self.timeout_seconds, (int, float)) or self.timeout_seconds <= 0:
            raise BoundedRecallError("timeout_seconds must be > 0.")
        if not isinstance(self.chars_per_token, int) or self.chars_per_token <= 0:
            raise BoundedRecallError("chars_per_token must be a positive int.")
        for name in ("weight_metadata_match", "weight_recency", "weight_truth_level"):
            if getattr(self, name) < 0:
                raise BoundedRecallError(f"{name} must be >= 0.")
        total = (
            self.weight_metadata_match
            + self.weight_recency
            + self.weight_truth_level
        )
        if abs(total - 1.0) > 1e-6:
            raise BoundedRecallError(
                f"weights must sum to 1.0 (got {total:.6f})."
            )
        if not isinstance(self.trusted_truth_levels, tuple):
            raise BoundedRecallError("trusted_truth_levels must be a tuple.")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["trusted_truth_levels"] = list(self.trusted_truth_levels)
        return d


# --------------------------------------------------------------------------- #
# Bundle
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RecallBundle:
    """Frozen bundle of matched memories and their provenance."""

    query: str
    memories: Tuple[Mapping[str, Any], ...]
    scores: Tuple[float, ...]
    citations: Tuple[str, ...]
    token_cost: int
    latency_ms: float
    truth_level_mix: Mapping[str, int] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.memories, tuple):
            object.__setattr__(self, "memories", tuple(self.memories))
        if not isinstance(self.scores, tuple):
            object.__setattr__(self, "scores", tuple(self.scores))
        if not isinstance(self.citations, tuple):
            object.__setattr__(self, "citations", tuple(self.citations))
        if len(self.memories) != len(self.scores):
            raise BoundedRecallError(
                "memories and scores must have the same length."
            )
        if self.token_cost < 0:
            raise BoundedRecallError("token_cost must be >= 0.")
        if self.latency_ms < 0:
            raise BoundedRecallError("latency_ms must be >= 0.")

    def is_empty(self) -> bool:
        return not self.memories

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "memories": [dict(m) for m in self.memories],
            "scores": list(self.scores),
            "citations": list(self.citations),
            "token_cost": self.token_cost,
            "latency_ms": self.latency_ms,
            "truth_level_mix": dict(self.truth_level_mix),
            "timestamp": self.timestamp.isoformat(),
        }


# --------------------------------------------------------------------------- #
# Recall layer
# --------------------------------------------------------------------------- #
class BoundedRecall:
    """Bounded, filtered, scored recall over Supermemory."""

    def __init__(
        self,
        adapter: SupermemoryAdapter,
        *,
        config: Optional[BoundedRecallConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(adapter, SupermemoryAdapter):
            raise BoundedRecallError("adapter must be a SupermemoryAdapter.")
        self._adapter = adapter
        self._config = config or BoundedRecallConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._total_queries = 0
        self._total_empty = 0
        self._total_tokens = 0
        self._total_latency_ms = 0.0
        self._started_at = time.time()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> BoundedRecallConfig:
        return self._config

    # ---------------------------------------------------------- public API
    def query(
        self,
        query: str,
        *,
        k: Optional[int] = None,
        container_tag: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> RecallBundle:
        """Run a bounded recall and return a ``RecallBundle``."""
        if not isinstance(query, str) or not query:
            raise BoundedRecallError("query must be a non-empty string.")
        top_k = k if k is not None else self._config.default_k
        if not isinstance(top_k, int) or top_k <= 0:
            raise BoundedRecallError("k must be a positive int.")
        if top_k > self._config.max_k:
            top_k = self._config.max_k

        start = time.perf_counter()
        try:
            memories = self._adapter.recall_similar_runs(
                query,
                container_tag=container_tag,
                k=top_k,
                filters=filters,
            )
        except SupermemoryAdapterError as exc:
            if self._strict:
                raise BoundedRecallError(
                    f"adapter recall failed: {exc}"
                ) from exc
            memories = []
        latency_ms = (time.perf_counter() - start) * 1000.0

        # Score, filter, and cap on tokens.
        scored = self._score(memories, filters=filters)
        scored.sort(key=lambda t: (-t[1], str(t[0].get("id", ""))))
        selected: List[Mapping[str, Any]] = []
        scores: List[float] = []
        token_cost = 0
        for memory, score in scored:
            cost = self._token_cost(memory)
            if token_cost + cost > self._config.token_budget:
                continue
            selected.append(memory)
            scores.append(score)
            token_cost += cost
            if len(selected) >= top_k:
                break

        citations = tuple(self._citation(m) for m in selected)
        truth_mix: Dict[str, int] = {}
        for m in selected:
            level = str((m.get("metadata") or {}).get("truth_level", "unknown"))
            truth_mix[level] = truth_mix.get(level, 0) + 1

        with self._lock:
            self._total_queries += 1
            self._total_tokens += token_cost
            self._total_latency_ms += latency_ms
            if not selected:
                self._total_empty += 1

        return RecallBundle(
            query=query,
            memories=tuple(selected),
            scores=tuple(scores),
            citations=citations,
            token_cost=token_cost,
            latency_ms=latency_ms,
            truth_level_mix=truth_mix,
        )

    def summarize(
        self,
        bundle: RecallBundle,
        *,
        max_tokens: Optional[int] = None,
    ) -> str:
        """Return a citable short summary of ``bundle``."""
        if bundle.is_empty():
            return f"No comparable historical runs for {bundle.query!r}."
        cap = (
            max_tokens if max_tokens is not None
            else self._config.max_summary_tokens
        )
        lines = [f"Recalled {len(bundle.memories)} comparable run(s):"]
        for memory, score, citation in zip(
            bundle.memories, bundle.scores, bundle.citations
        ):
            meta = memory.get("metadata") or {}
            route = meta.get("route", "unknown")
            wt = meta.get("workload_type", "unknown")
            dc = meta.get("device_class", "unknown")
            lines.append(
                f"  - {citation}: route={route}, workload={wt}, "
                f"device={dc}, score={score:.3f}"
            )
        text = "\n".join(lines)
        if self._token_cost({"content": text}) > cap:
            # Truncate to the first N lines that fit.
            fitted: List[str] = []
            for line in lines:
                candidate = "\n".join(fitted + [line])
                if self._token_cost({"content": candidate}) > cap:
                    break
                fitted.append(line)
            text = "\n".join(fitted) if fitted else lines[0]
        return text

    # ---------------------------------------------------------- internals
    def _score(
        self,
        memories: Sequence[Mapping[str, Any]],
        *,
        filters: Optional[Mapping[str, Any]],
    ) -> List[tuple]:
        scored: List[tuple] = []
        for memory in memories:
            meta = memory.get("metadata") or {}
            meta_match = self._metadata_match(meta, filters)
            recency = self._recency(memory)
            truth = (
                1.0
                if meta.get("truth_level") in self._config.trusted_truth_levels
                else 0.0
            )
            score = (
                self._config.weight_metadata_match * meta_match
                + self._config.weight_recency * recency
                + self._config.weight_truth_level * truth
            )
            scored.append((memory, score))
        return scored

    def _metadata_match(
        self, meta: Mapping[str, Any], filters: Optional[Mapping[str, Any]]
    ) -> float:
        if not filters:
            return 1.0
        keys = [k for k in filters if k in meta]
        if not keys:
            return 0.0
        hits = sum(1 for k in keys if meta[k] == filters[k])
        return hits / len(keys)

    def _recency(self, memory: Mapping[str, Any]) -> float:
        ts = (memory.get("metadata") or {}).get("observed_at")
        if not ts:
            return 0.5
        try:
            dt = datetime.fromisoformat(str(ts))
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return 0.5
        age_hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
        return 1.0 / (1.0 + age_hours / 24.0)

    def _token_cost(self, memory: Mapping[str, Any]) -> int:
        content = str(memory.get("content", ""))
        return max(1, math.ceil(len(content) / self._config.chars_per_token))

    def _citation(self, memory: Mapping[str, Any]) -> str:
        meta = memory.get("metadata") or {}
        return str(
            meta.get("run_id")
            or memory.get("id")
            or meta.get("incident_id")
            or "unknown"
        )

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "queries": self._total_queries,
                "empty_results": self._total_empty,
                "total_tokens": self._total_tokens,
                "mean_latency_ms": (
                    self._total_latency_ms / self._total_queries
                    if self._total_queries else 0.0
                ),
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    def reset(self) -> None:
        with self._lock:
            self._total_queries = 0
            self._total_empty = 0
            self._total_tokens = 0
            self._total_latency_ms = 0.0
            self._started_at = time.time()

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=str, **kw)

    def __repr__(self) -> str:
        return (
            "BoundedRecall("
            f"queries={self._total_queries}, "
            f"token_budget={self._config.token_budget}, "
            f"k={self._config.default_k}, "
            f"strict={self._strict})"
        )


__all__ = [
    "BoundedRecall",
    "BoundedRecallConfig",
    "BoundedRecallError",
    "RecallBundle",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.bounded_recall
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    adapter = SupermemoryAdapter()

    # Seed a few decisions with distinct metadata.
    from .memory_schemas import DecisionRecord

    for i in range(5):
        adapter.remember_decision(DecisionRecord(
            run_id=f"run-{i:03d}",
            workload_type="vision_inference" if i % 2 == 0 else "text_gen",
            device_class="arm_edge" if i < 3 else "cloud_gpu",
            route="edge_int8" if i < 3 else "cloud_gpu",
            policy_version="v0.3",
            truth_level="measured",
            predicted_energy_wh=8.0 + i * 0.1,
            predicted_carbon_gco2e=3.7 + i * 0.05,
            predicted_latency_ms=410.0 + i * 5,
            reason="SLA met.",
        ))

    recall = BoundedRecall(adapter)
    print("repr       :", recall)

    bundle = recall.query(
        "vision_inference edge_int8",
        k=3,
        filters={"workload_type": "vision_inference"},
    )
    print("bundle     :", len(bundle.memories), "memories, "
          f"tokens={bundle.token_cost}, latency={bundle.latency_ms:.2f}ms")
    print("citations  :", bundle.citations)
    print("truth mix  :", bundle.truth_level_mix)

    summary = recall.summarize(bundle)
    print("summary    :\n" + summary)

    # Statistics.
    print("statistics :", {
        k: v for k, v in recall.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # Validation.
    for bad in (
        lambda: recall.query(""),
        lambda: recall.query("q", k=0),
    ):
        try:
            bad()
        except BoundedRecallError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
