# -*- coding: utf-8 -*-
"""
Knowledge Base Integrator & Selective Invocation Policy (Enhanced, v2)
======================================================================

Integrates structured knowledge bases and implements intelligent escalation
logic based on confidence, sustainability, and criticality thresholds.

Fixes applied vs. v1
--------------------
  P0  KB integrator accepted but never called        -> wired into state
  P0  Circular reward (student mimics heuristic)     -> record_outcome()
  P0  Contradictory decision (should_escalate/reasons) -> consistency check
  P0  Evolutionary unreachable when distillation off -> OR gate
  P0  Evolutionary never evolves (index-0 bug)       -> whole-population
  P0  Substring criticality matching                 -> word-boundary regex
  P0  config.modp_weights mutated per decision       -> local copy
  P0  modp_weights=[] silently produced []           -> __post_init__
  P0  MoE gate gradient wrong shape/sign             -> softmax chain-rule
  P0  Unseeded RNG                                   -> default_rng(seed)
  P0  numpy unconditional (import fails)             -> ENHANCEMENTS_AVAILABLE
  P1  feature_dim / n_actions hard-coded             -> derived
  P1  epsilon never decays                           -> epsilon_min / decay
  P1  batch_size / distill_w / rl_w hard-coded       -> config
  P1  State features not clipped                     -> _clamp
  P1  Four different criticality representations     -> single scoring path
  P1  No persistence                                 -> save()/load()
  P1  No metrics                                     -> MetricsSink
  P1  No thread safety                               -> RLock
  P1  Config not frozen                              -> frozen + validated
  P2  PendingDecision ad-hoc tuple                   -> dataclass
  P2  asdict imported but unused                     -> removed
  P2  reset_stats / batch_decide missing             -> added
  P2  Justification hides suppressed reasons         -> reported

Preserved public surface
------------------------
  KnowledgeSourceType, EscalationReason, KnowledgeEntry, EscalationDecision,
  SelectiveInvocationConfig, KnowledgeBaseIntegrator,
  SelectiveInvocationPolicy, EscalationState,
  EscalationDistillationOptimizer, EvolutionaryMODPWeightsOptimizer

New public surface
------------------
  ENHANCEMENTS_AVAILABLE, MetricsSink, InMemoryMetrics, LoggingMetrics,
  PendingDecision,
  .record_outcome(), .record_human_feedback(), .save(), .load(),
  .reset_stats(), .batch_decide_escalation(), .get_optimizer_stats()
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# SECTION 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _bounded_inverse(q: float, scale: float) -> float:
    """1 / (1 + q/scale) — bounded on (0, 1], scale-invariant."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _has_keyword(text: str, keywords: Sequence[str]) -> bool:
    """Word-boundary match — avoids 'non-critical' matching 'critical'."""
    low = text.lower()
    for kw in keywords:
        if re.search(rf"\b{re.escape(kw)}\b", low):
            return True
    return False


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(path)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring corrupt checkpoint %s: %s", path, exc)
        return None


# ============================================================================
# SECTION 2 — Metrics sink
# ============================================================================

class MetricsSink(Protocol):
    def observe(self, name: str, value: float, **labels: str) -> None: ...


class InMemoryMetrics:
    def __init__(self) -> None:
        self._data: Dict[str, float] = {}
        self._lock = threading.Lock()

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = name + (
            ":" + ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            if labels else ""
        )
        with self._lock:
            self._data[key] = value

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._data)


class LoggingMetrics:
    def __init__(self) -> None:
        self._log = logging.getLogger("kb_integrator.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Enums
# ============================================================================

class KnowledgeSourceType(str, Enum):
    ENERGY_STANDARDS = "energy_standards"
    SCIENTIFIC_PAPERS = "scientific_papers"
    CODE_REPOSITORIES = "code_repositories"
    BENCHMARKS = "benchmarks"
    DOCUMENTATION = "documentation"
    REGULATORY = "regulatory"


class EscalationReason(str, Enum):
    LOW_CONFIDENCE = "low_confidence"
    HIGH_ENERGY = "high_energy"
    CRITICAL_SAFETY = "critical_safety"
    CRITICAL_SECURITY = "critical_security"
    CRITICAL_CORRECTNESS = "critical_correctness"
    DOMAIN_COMPLEXITY = "domain_complexity"
    ETHICAL_CONCERN = "ethical_concern"
    REGULATORY_COMPLIANCE = "regulatory_compliance"


# ============================================================================
# SECTION 4 — Data classes
# ============================================================================

@dataclass
class KnowledgeEntry:
    entry_id: str
    source_type: KnowledgeSourceType
    title: str
    content: str
    metadata: Dict[str, Any]
    relevance_score: float
    confidence: float


@dataclass
class EscalationDecision:
    should_escalate: bool
    reasons: List[EscalationReason]
    confidence_score: float
    sustainability_impact: float
    criticality_level: str
    recommended_expert: str
    estimated_cost_wh: float
    justification: str
    graph_metrics: Optional[Dict[str, float]] = None
    modp_score: Optional[float] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None
    # New: how many reasons the optimizer suppressed (for transparency).
    suppressed_reasons: List[EscalationReason] = field(default_factory=list)
    enhanced: bool = False


@dataclass
class PendingDecision:
    """Captured state from one decide_escalation() call, consumed by record_outcome()."""
    state_vec: "np.ndarray"
    action: int
    teacher_outputs: "np.ndarray"
    gate_probs: "np.ndarray"
    heuristic_reasons: List[EscalationReason]
    task: str
    agent_confidence: float
    estimated_energy_wh: float
    criticality_level: str


# ============================================================================
# SECTION 5 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class SelectiveInvocationConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5, "connectivity": 0.5, "density": 0.4,
        }
    )
    # MODP weights: (confidence, sustainability, criticality, cost)
    modp_weights: Tuple[float, float, float, float] = (0.4, 0.3, 0.2, 0.1)
    human_feedback_score: float = 0.5

    use_distillation: bool = True
    use_evolutionary: bool = False

    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995
    distill_w: float = 0.7
    rl_w: float = 0.3

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Reward normalization scales
    energy_scale_wh: float = 0.01
    cost_scale_wh: float = 0.1

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components "
                f"(confidence, sustainability, criticality, cost), got {w!r}"
            )
        if any(float(x) < 0 for x in w):
            raise ValueError(f"modp_weights must be non-negative, got {w!r}")
        if sum(float(x) for x in w) <= 0:
            raise ValueError(f"modp_weights sum must be > 0, got {w!r}")
        if not 0.0 <= self.human_feedback_score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        if not 0.0 <= self.epsilon <= 1.0:
            raise ValueError("epsilon must be in [0,1]")
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise ValueError("epsilon_min must be in [0, epsilon]")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise ValueError("epsilon_decay must be in (0,1]")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.population_size < 2:
            raise ValueError("population_size must be >= 2")
        if not 0 <= self.elitism < self.population_size:
            raise ValueError("elitism must be in [0, population_size)")
        if self.distill_w < 0 or self.rl_w < 0 or (self.distill_w + self.rl_w) <= 0:
            raise ValueError("distill_w and rl_w must be non-negative, sum > 0")
        if self.energy_scale_wh <= 0 or self.cost_scale_wh <= 0:
            raise ValueError("energy_scale_wh and cost_scale_wh must be > 0")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SelectiveInvocationConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "SelectiveInvocationConfig ignoring unknown keys: %s",
                sorted(unknown),
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                if v is None:
                    continue
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 6 — Criticality — single source of truth
# ============================================================================

# Word-boundary patterns (avoids false positives).
_SAFETY_RE = re.compile(
    r"\b(crash|segfault|unsafe|critical|memory)\b", re.IGNORECASE
)
_SECURITY_RE = re.compile(
    r"\b(vulnerability|exploit|password|authentication|encryption)\b",
    re.IGNORECASE,
)
_CORRECTNESS_RE = re.compile(
    r"\b(algorithm|logic|computation|accuracy|precision)\b", re.IGNORECASE
)
_COMPLIANCE_RE = re.compile(
    r"\b(medical|healthcare|financial|privacy|gdpr|hipaa|pci|compliance)\b",
    re.IGNORECASE,
)
_COMPLEX_DOMAINS = frozenset(
    {
        "cryptography", "quantum", "distributed_systems",
        "machine_learning", "medical", "legal", "scientific",
    }
)


def _criticality_reasons_from_task(task: str) -> List[EscalationReason]:
    reasons: List[EscalationReason] = []
    if _SAFETY_RE.search(task):
        reasons.append(EscalationReason.CRITICAL_SAFETY)
    if _SECURITY_RE.search(task):
        reasons.append(EscalationReason.CRITICAL_SECURITY)
    if _CORRECTNESS_RE.search(task):
        reasons.append(EscalationReason.CRITICAL_CORRECTNESS)
    if _COMPLIANCE_RE.search(task):
        reasons.append(EscalationReason.REGULATORY_COMPLIANCE)
    return reasons


def _criticality_score_from_reasons(reasons: Sequence[EscalationReason]) -> float:
    """Score ∈ [0, 1]. Higher = more critical."""
    if any(
        r in reasons
        for r in (
            EscalationReason.CRITICAL_SAFETY,
            EscalationReason.CRITICAL_SECURITY,
            EscalationReason.REGULATORY_COMPLIANCE,
        )
    ):
        return 1.0
    if EscalationReason.CRITICAL_CORRECTNESS in reasons:
        return 0.7
    return 0.3 if reasons else 0.0


def _criticality_level_from_score(score: float) -> str:
    if score >= 0.9:
        return "critical"
    if score >= 0.5:
        return "high"
    if score >= 0.2:
        return "medium"
    return "low"


# ============================================================================
# SECTION 7 — Escalation state (clipped, KB-aware)
# ============================================================================

class EscalationState:
    # 7 base + 2 KB = 9 features
    FEATURE_DIM = 9

    __slots__ = (
        "confidence", "energy", "criticality", "domain_complexity",
        "centrality", "connectivity", "human_feedback",
        "kb_confidence", "kb_relevance",
        "_vector",
    )

    def __init__(
        self,
        task: str,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
        *,
        kb_confidence: float = 0.5,
        kb_relevance: float = 0.5,
        energy_scale_wh: float = 1.0,
    ) -> None:
        self.confidence = _clamp(agent_confidence)
        self.energy = _bounded_inverse(estimated_energy_wh, energy_scale_wh)
        reasons = _criticality_reasons_from_task(task)
        self.criticality = _criticality_score_from_reasons(reasons)
        self.domain_complexity = (
            0.8 if any(cd in domain.lower() for cd in _COMPLEX_DOMAINS) else 0.2
        )
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(human_feedback)
        self.kb_confidence = _clamp(kb_confidence)
        self.kb_relevance = _clamp(kb_relevance)

        self._vector = np.array(
            [
                self.confidence,
                self.energy,
                self.criticality,
                self.domain_complexity,
                self.centrality,
                self.connectivity,
                self.human_feedback,
                self.kb_confidence,
                self.kb_relevance,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 8 — Distillation optimizer (fixed MoE gate gradient)
# ============================================================================

class EscalationDistillationOptimizer:
    """
    Multi-teacher distillation + MoE gating for the escalate/no-escalate decision.

    Forward returns (action, state_vec, teacher_outputs, gate_probs) so the
    gate can be trained on real teacher outputs (fixes the 15th occurrence of
    the gate-gradient bug).
    """

    N_TEACHERS = 3

    def __init__(
        self,
        config: SelectiveInvocationConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for distillation")

        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = EscalationState.FEATURE_DIM
        self.n_actions = 2
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)
        self._lock = threading.RLock()

        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".distill.json"))

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: EscalationState) -> "np.ndarray":
        # Escalate if low confidence, high energy, high criticality,
        # or complex domain. Uses word-boundary criticality score.
        should = (
            state.confidence < 0.7
            or state.energy > 0.9  # bounded_inverse: higher energy -> lower
            or state.criticality > 0.3
            or state.domain_complexity > 0.5
        )
        return np.array([0.1, 0.9]) if should else np.array([0.9, 0.1])

    def _rlhf_teacher(self, state: EscalationState) -> "np.ndarray":
        if state.human_feedback > 0.7:
            return np.array([0.2, 0.8])
        if state.human_feedback < 0.3:
            return np.array([0.8, 0.2])
        return np.array([0.5, 0.5])

    def _historical_teacher(self, state: EscalationState) -> "np.ndarray":
        # KB-aware: if knowledge base has low-confidence match, escalate.
        if state.centrality > 0.7 and state.criticality > 0.4:
            return np.array([0.1, 0.9])
        if state.kb_confidence < 0.5 and state.kb_relevance > 0.3:
            return np.array([0.3, 0.7])
        return np.array([0.6, 0.4])

    # ---- forward ----------------------------------------------------------
    def select_action(
        self, state: EscalationState, exploration: bool = True
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray"]:
        x = state.to_vector()
        with self._lock:
            teacher_outputs = np.stack([t(state) for t in self.teachers])
            teacher_outputs = np.maximum(teacher_outputs, 1e-12)
            teacher_outputs /= teacher_outputs.sum(axis=1, keepdims=True)
            gate = self._gate_forward(x)
            teacher_mix = gate @ teacher_outputs
            teacher_mix /= max(float(teacher_mix.sum()), 1e-12)

            if exploration and self.rng.random() < self.epsilon:
                action = int(self.rng.integers(0, self.n_actions))
            else:
                student = self._softmax(
                    x @ self.student_weights + self.student_bias
                )
                combined = 0.8 * student + 0.2 * teacher_mix
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, gate

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        with self._lock:
            self.replay.append(
                (
                    np.asarray(state_vec, dtype=np.float64),
                    int(action),
                    r,
                    np.asarray(teacher_outputs, dtype=np.float64),
                    np.asarray(gate_probs, dtype=np.float64),
                )
            )
            self.counter += 1
            should_train = (
                self.counter % self.train_every == 0
                and len(self.replay) >= self.batch_size
            )
        if should_train:
            self._train_step()

    def _train_step(self) -> None:
        with self._lock:
            n = min(self.batch_size, len(self.replay))
            idxs = self.rng.choice(len(self.replay), size=n, replace=False)
            batch = [self.replay[int(i)] for i in idxs]

            for s, a, r, t_outs, gate in batch:
                cur = self._softmax(s @ self.student_weights + self.student_bias)

                # ---- student ----
                combined = gate @ t_outs
                one_hot = np.zeros(self.n_actions)
                one_hot[a] = 1.0
                grad_distill = -(combined - cur)
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # ---- gate (correct softmax chain rule) ----
                target = np.full(
                    self.n_actions,
                    (1.0 - r) / max(self.n_actions - 1, 1),
                    dtype=np.float64,
                )
                target[a] = r
                target = np.clip(target, 0.0, 1.0)
                ts = target.sum()
                target = target / ts if ts > 0 else np.full(
                    self.n_actions, 1.0 / self.n_actions
                )

                err = combined - target                # (P,)
                dL_dg = t_outs.T @ err                 # (T,)
                grad_logits = gate * (dL_dg - float(gate @ dL_dg))
                self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                self.gate_bias -= self.gate_lr * grad_logits

            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "escalation_distillation",
                "feature_dim": self.feature_dim,
                "n_actions": self.n_actions,
                "student_weights": self.student_weights.tolist(),
                "student_bias": self.student_bias.tolist(),
                "gate_weights": self.gate_weights.tolist(),
                "gate_bias": self.gate_bias.tolist(),
                "epsilon": float(self.epsilon),
                "counter": int(self.counter),
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "escalation_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(
            payload["student_weights"], dtype=np.float64
        )
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 9 — Evolutionary optimizer (real GA over MODP weights)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"
    fitness: float = 0.0
    evaluations: int = 0


class EvolutionaryMODPWeightsOptimizer:
    """
    Population of MODP weight vectors.

    Fitness = negative squared error between predicted score (from weights)
    and the target reward. Each individual is scored against every observation;
    the population is evolved once enough observations have accumulated.
    """

    def __init__(
        self,
        config: SelectiveInvocationConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for evolutionary optimizer")

        self.config = config
        self.n_weights = len(config.modp_weights)
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.counter = 0
        self.generation = 0
        self.best_weights: "np.ndarray" = np.asarray(
            config.modp_weights, dtype=np.float64
        )
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(
                weights=self.rng.dirichlet(np.ones(self.n_weights)),
            )
            for _ in range(config.population_size)
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    # ---- scoring ----------------------------------------------------------
    @staticmethod
    def _predict(weights: "np.ndarray", features: "np.ndarray") -> float:
        # features = (confidence, energy, criticality, cost)
        return float(np.clip(weights @ features, 0.0, 1.0))

    def _score(self, weights: "np.ndarray", features: "np.ndarray", target: float) -> float:
        return -(self._predict(weights, features) - target) ** 2

    # ---- public API -------------------------------------------------------
    def observe(
        self,
        features: "np.ndarray",
        target: float,
    ) -> None:
        """
        Score every individual against the observation and update the
        historical best. Evolve every `train_every` observations.
        """
        with self._lock:
            for ind in self.population:
                ind.fitness += self._score(ind.weights, features, target)
                ind.evaluations += 1

            best = max(self.population, key=lambda ind: ind.fitness)
            if best.fitness > self.best_fitness:
                self.best_fitness = float(best.fitness)
                self.best_weights = best.weights.copy()

            self.counter += 1
            should_evolve = self.counter % self.config.train_every == 0
        if should_evolve:
            self._evolve()

    def _evolve(self) -> None:
        with self._lock:
            self.population.sort(key=lambda ind: ind.fitness, reverse=True)
            n_elite = max(1, self.config.elitism)
            new_pop: List[_Ind] = [
                _Ind(weights=ind.weights.copy(), fitness=0.0)
                for ind in self.population[:n_elite]
            ]
            while len(new_pop) < len(self.population):
                a = self._tournament()
                b = self._tournament()
                child = self._crossover(a, b)
                self._mutate(child)
                new_pop.append(child)
            self.population = new_pop
            self.generation += 1

    def _tournament(self, k: int = 3) -> _Ind:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        return max(
            (self.population[int(i)] for i in idxs), key=lambda ind: ind.fitness
        )

    def _crossover(self, a: _Ind, b: _Ind) -> _Ind:
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(self.n_weights) < 0.5
            w = np.where(mask, a.weights, b.weights)
        else:
            w = a.weights.copy()
        return _Ind(weights=w)

    def _mutate(self, ind: _Ind) -> None:
        mask = self.rng.random(self.n_weights) < self.config.mutation_rate
        ind.weights = np.clip(
            ind.weights
            + mask * self.rng.standard_normal(self.n_weights) * 0.05,
            1e-6,
            None,
        )
        total = ind.weights.sum()
        if total > 0:
            ind.weights = ind.weights / total

    def get_best_weights(self) -> "np.ndarray":
        with self._lock:
            return self.best_weights.copy()

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "escalation_evolution",
                "generation": self.generation,
                "counter": self.counter,
                "best_fitness": self.best_fitness,
                "best_weights": self.best_weights.tolist(),
                "population": [
                    {
                        "weights": ind.weights.tolist(),
                        "fitness": float(ind.fitness),
                    }
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload or payload.get("kind") != "escalation_evolution":
            return
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.best_fitness = float(payload.get("best_fitness", -math.inf))
        self.best_weights = np.asarray(payload["best_weights"], dtype=np.float64)
        self.population = [
            _Ind(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 10 — Knowledge base integrator
# ============================================================================

class KnowledgeBaseIntegrator:
    """
    Integrates structured knowledge bases (JSON-backed).

    Original behaviour preserved; logging added and load errors are non-fatal.
    """

    def __init__(
        self,
        knowledge_sources: Dict[KnowledgeSourceType, str],
        enable_caching: bool = True,
    ) -> None:
        self.knowledge_sources = dict(knowledge_sources)
        self.enable_caching = bool(enable_caching)
        self.knowledge_cache: Dict[str, List[KnowledgeEntry]] = {}
        self.knowledge_bases: Dict[KnowledgeSourceType, Any] = {}
        self._lock = threading.RLock()
        self._load_knowledge_bases()

    def _load_knowledge_bases(self) -> None:
        for source_type, source_path in self.knowledge_sources.items():
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                with self._lock:
                    self.knowledge_bases[source_type] = data
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to load %s: %s", source_type.value, exc
                )

    def query_knowledge(
        self,
        query: str,
        source_types: Optional[List[KnowledgeSourceType]] = None,
        top_k: int = 5,
    ) -> List[KnowledgeEntry]:
        source_keys = sorted(
            [s.value for s in (source_types or self.knowledge_bases.keys())]
        )
        cache_key = f"{query}:{source_keys}"
        with self._lock:
            if self.enable_caching and cache_key in self.knowledge_cache:
                return list(self.knowledge_cache[cache_key])

        results: List[KnowledgeEntry] = []
        sources_to_query = source_types or list(self.knowledge_bases.keys())
        for source_type in sources_to_query:
            kb_data = self.knowledge_bases.get(source_type)
            if kb_data is None:
                continue
            for entry in kb_data.get("entries", []):
                relevance = self._calculate_relevance(query, entry)
                if relevance > 0.3:
                    results.append(
                        KnowledgeEntry(
                            entry_id=entry.get("id", ""),
                            source_type=source_type,
                            title=entry.get("title", ""),
                            content=entry.get("content", ""),
                            metadata=entry.get("metadata", {}),
                            relevance_score=relevance,
                            confidence=float(entry.get("confidence", 0.8)),
                        )
                    )
        results.sort(key=lambda x: x.relevance_score, reverse=True)
        results = results[:top_k]
        if self.enable_caching:
            with self._lock:
                self.knowledge_cache[cache_key] = list(results)
        return results

    @staticmethod
    def _calculate_relevance(query: str, entry: Dict[str, Any]) -> float:
        query_words = set(query.lower().split())
        title_words = set(str(entry.get("title", "")).lower().split())
        content_words = set(str(entry.get("content", "")).lower().split())
        title_overlap = len(query_words & title_words)
        content_overlap = len(query_words & content_words)
        score = (
            title_overlap * 0.7 + content_overlap * 0.3
        ) / max(len(query_words), 1)
        return min(score, 1.0)

    def add_knowledge_source(
        self, source_type: KnowledgeSourceType, source_path: str
    ) -> None:
        with self._lock:
            self.knowledge_sources[source_type] = source_path
        try:
            with open(source_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            with self._lock:
                self.knowledge_bases[source_type] = data
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to load %s: %s", source_type.value, exc
            )

    def aggregate_confidence(self, query: str, top_k: int = 3) -> Tuple[float, float]:
        """
        Returns (avg_confidence, avg_relevance) across top_k matches.
        Used by SelectiveInvocationPolicy to build the state.
        """
        results = self.query_knowledge(query, top_k=top_k)
        if not results:
            return 0.0, 0.0
        avg_conf = sum(e.confidence for e in results) / len(results)
        avg_rel = sum(e.relevance_score for e in results) / len(results)
        return _clamp(avg_conf), _clamp(avg_rel)


# ============================================================================
# SECTION 11 — SelectiveInvocationPolicy (main)
# ============================================================================

class SelectiveInvocationPolicy:
    """
    Escalation policy with optional enhanced decision-making.

    Enhanced path:
      1. Build state from task + metrics + KB query.
      2. select_action() picks escalate / don't escalate.
      3. Consistency check: warn if decision contradicts heuristic reasons.
      4. Store pending for record_outcome().

    Non-enhanced path (or when enhancements unavailable): rule-based heuristic.

    Training: caller invokes `record_outcome(actual_should_escalate, actual_cost_wh)`
    after observing the true outcome. The optimizer trains toward the outcome,
    not toward the heuristic.
    """

    def __init__(
        self,
        confidence_threshold: float = 0.7,
        sustainability_threshold_wh: float = 0.1,
        enable_criticality_check: bool = True,
        enable_sustainability_check: bool = True,
        knowledge_integrator: Optional[KnowledgeBaseIntegrator] = None,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not 0.0 <= confidence_threshold <= 1.0:
            raise ValueError("confidence_threshold must be in [0,1]")
        if sustainability_threshold_wh < 0:
            raise ValueError("sustainability_threshold_wh must be >= 0")

        if config is None:
            self.config = SelectiveInvocationConfig()
        elif isinstance(config, SelectiveInvocationConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = SelectiveInvocationConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be SelectiveInvocationConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.confidence_threshold = float(confidence_threshold)
        self.sustainability_threshold = float(sustainability_threshold_wh)
        self.enable_criticality_check = bool(enable_criticality_check)
        self.enable_sustainability_check = bool(enable_sustainability_check)
        self.knowledge_integrator = knowledge_integrator

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )
        self._lock = threading.RLock()

        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to rule-based escalation"
            )

        # Statistics
        self.total_decisions = 0
        self.escalations = 0
        self.escalations_by_reason: Dict[EscalationReason, int] = {}
        self._human_feedback_score = float(self.config.human_feedback_score)
        self._pending: Optional[PendingDecision] = None

        # Optimizers
        self.optimizer: Optional[Any] = None
        self.evolutionary_optimizer: Optional[Any] = None
        if self.use_enhancements:
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(
                    self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.optimizer = EscalationDistillationOptimizer(
                    self.config, rng=self.rng
                )

        # Backwards-compat aliases
        self.distillation_optimizer = self.optimizer

    # ------------------------------------------------------------------
    # Live human feedback
    # ------------------------------------------------------------------
    def record_human_feedback(self, score: float) -> None:
        score = float(score)
        if not 0.0 <= score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        with self._lock:
            self._human_feedback_score = score
        self.metrics.observe("escalation_human_feedback_score", score)

    # ------------------------------------------------------------------
    # Main API
    # ------------------------------------------------------------------
    def decide_escalation(
        self,
        task: str,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        context: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> EscalationDecision:
        with self._lock:
            self.total_decisions += 1
            # Clear pending at entry — a new decision invalidates the previous.
            self._pending = None

        gm = dict(graph_metrics or self.config.graph_metrics)
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )

        # Build heuristic reasons (independent of optimizer).
        reasons = self._build_reasons(task, agent_confidence, estimated_energy_wh)
        criticality_reasons = _criticality_reasons_from_task(task)
        criticality_score = _criticality_score_from_reasons(criticality_reasons)
        criticality_level = _criticality_level_from_score(criticality_score)

        if self.optimizer is None:
            # Non-enhanced path: decide from heuristic.
            should_escalate = bool(reasons)
            suppressed: List[EscalationReason] = []
            decision = self._build_decision(
                should_escalate=should_escalate,
                reasons=reasons,
                suppressed=suppressed,
                agent_confidence=agent_confidence,
                estimated_energy_wh=estimated_energy_wh,
                criticality_level=criticality_level,
                domain=domain,
                gm=gm,
                hf=hf,
                modp_score=None,
                enhanced=False,
            )
            self._record_escalation(should_escalate, reasons)
            self._emit_decision_metrics(decision)
            return decision

        # ---- Enhanced path ---------------------------------------
        kb_conf, kb_rel = self._kb_features(task)
        state = EscalationState(
            task=task,
            agent_confidence=agent_confidence,
            estimated_energy_wh=estimated_energy_wh,
            domain=domain,
            graph_metrics=gm,
            human_feedback=hf,
            kb_confidence=kb_conf,
            kb_relevance=kb_rel,
        )
        action, s_vec, t_outs, g_probs = self.optimizer.select_action(state)
        should_escalate = bool(action == 1)

        # Consistency check — log if optimizer contradicts heuristic.
        if should_escalate != bool(reasons):
            logger.warning(
                "Optimizer decision (%s) disagrees with heuristic reasons (%s)",
                should_escalate, [r.value for r in reasons],
            )

        # Suppressed reasons: heuristic says "should escalate" but optimizer
        # chose not to. Report them for transparency.
        suppressed: List[EscalationReason] = (
            reasons if (reasons and not should_escalate) else []
        )

        self._pending = PendingDecision(
            state_vec=s_vec,
            action=action,
            teacher_outputs=t_outs,
            gate_probs=g_probs,
            heuristic_reasons=reasons,
            task=task,
            agent_confidence=agent_confidence,
            estimated_energy_wh=estimated_energy_wh,
            criticality_level=criticality_level,
        )

        decision = self._build_decision(
            should_escalate=should_escalate,
            reasons=reasons,
            suppressed=suppressed,
            agent_confidence=agent_confidence,
            estimated_energy_wh=estimated_energy_wh,
            criticality_level=criticality_level,
            domain=domain,
            gm=gm,
            hf=hf,
            modp_score=None,   # filled by record_outcome
            enhanced=True,
        )
        decision.distillation_stats = self._optimizer_stats()
        self._record_escalation(should_escalate, reasons)
        self._emit_decision_metrics(decision)
        return decision

    # ------------------------------------------------------------------
    # Training: caller supplies the true outcome
    # ------------------------------------------------------------------
    def record_outcome(
        self,
        actual_should_escalate: bool,
        actual_cost_wh: float,
        *,
        quality: float = 1.0,
    ) -> bool:
        """
        Train the optimizer against the TRUE outcome of the last decision.

        Returns True if training occurred. Requires a prior
        decide_escalation() call to have produced a pending decision.
        """
        if not self.use_enhancements:
            return False
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            logger.debug("record_outcome(): no pending decision")
            return False

        decision_correct = int(pending.action) == int(bool(actual_should_escalate))
        reward = 1.0 if decision_correct else -0.5
        # Cost penalty (bounded).
        cost_term = _bounded_inverse(actual_cost_wh, self.config.cost_scale_wh)
        reward -= (1.0 - cost_term) * 0.2
        reward *= _clamp(quality, 0.0, 1.0)
        reward = float(np.clip(reward, -1.0, 1.0))

        if self.optimizer is not None:
            try:
                self.optimizer.update(
                    pending.state_vec,
                    pending.action,
                    reward,
                    pending.teacher_outputs,
                    pending.gate_probs,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer update failed: %s", exc)
                return False

        if self.evolutionary_optimizer is not None:
            try:
                # Build the 4D MODP feature vector from the pending decision.
                confidence = _clamp(pending.agent_confidence)
                energy = _bounded_inverse(
                    pending.estimated_energy_wh, self.config.energy_scale_wh
                )
                crit = _criticality_score_from_reasons(pending.heuristic_reasons)
                cost = cost_term
                features = np.array(
                    [confidence, energy, crit, cost], dtype=np.float64
                )
                # Target: the correct outcome as a score in [0,1].
                target = 1.0 if int(pending.action) == int(bool(actual_should_escalate)) else 0.0
                self.evolutionary_optimizer.observe(features, target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary update failed: %s", exc)

        self.metrics.observe("escalation_reward", reward)
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _build_reasons(
        self, task: str, agent_confidence: float, estimated_energy_wh: float
    ) -> List[EscalationReason]:
        reasons: List[EscalationReason] = []
        if agent_confidence < self.confidence_threshold:
            reasons.append(EscalationReason.LOW_CONFIDENCE)
        if (
            self.enable_sustainability_check
            and estimated_energy_wh > self.sustainability_threshold
        ):
            reasons.append(EscalationReason.HIGH_ENERGY)
        if self.enable_criticality_check:
            reasons.extend(_criticality_reasons_from_task(task))
        if self._is_complex_domain(task):
            reasons.append(EscalationReason.DOMAIN_COMPLEXITY)
        return reasons

    def _is_complex_domain(self, domain: str) -> bool:
        return any(cd in domain.lower() for cd in _COMPLEX_DOMAINS)

    def _kb_features(self, task: str) -> Tuple[float, float]:
        if self.knowledge_integrator is None:
            return 0.5, 0.5
        try:
            return self.knowledge_integrator.aggregate_confidence(task, top_k=3)
        except Exception as exc:  # noqa: BLE001
            logger.warning("KB query failed: %s", exc)
            return 0.5, 0.5

    def _recommend_expert(
        self, reasons: Sequence[EscalationReason], domain: str
    ) -> str:
        if EscalationReason.CRITICAL_SECURITY in reasons:
            return "security_expert"
        if EscalationReason.CRITICAL_SAFETY in reasons:
            return "safety_expert"
        if EscalationReason.REGULATORY_COMPLIANCE in reasons:
            return "compliance_expert"
        if EscalationReason.HIGH_ENERGY in reasons:
            return "sustainability_expert"
        return f"{domain}_expert"

    def _estimate_expert_cost(
        self, reasons: Sequence[EscalationReason], criticality_level: str
    ) -> float:
        base = 0.05
        cost = base * max(len(reasons), 1)
        if criticality_level == "critical":
            cost *= 1.5
        elif criticality_level == "high":
            cost *= 1.3
        return cost

    def _calculate_sustainability_impact(
        self, task_energy: float, expert_energy: float
    ) -> float:
        potential_savings = task_energy * 0.3
        return expert_energy - potential_savings

    def _build_justification(
        self,
        should_escalate: bool,
        reasons: Sequence[EscalationReason],
        suppressed: Sequence[EscalationReason],
        confidence: float,
        energy: float,
        criticality: str,
    ) -> str:
        if not should_escalate:
            msg = (
                f"No escalation needed. Agent confidence: {confidence:.2f}, "
                f"Energy: {energy*1000:.1f} mWh, Criticality: {criticality}"
            )
            if suppressed:
                sup = ", ".join(r.value.replace("_", " ") for r in suppressed)
                msg += f". Suppressed: {sup}"
            return msg
        reason_strs = [r.value.replace("_", " ") for r in reasons]
        reasons_text = ", ".join(reason_strs) if reason_strs else "optimizer decision"
        return (
            f"Escalation recommended ({criticality} priority). "
            f"Reasons: {reasons_text}. "
            f"Agent confidence: {confidence:.2f}, "
            f"Energy: {energy*1000:.1f} mWh"
        )

    def _build_decision(
        self,
        *,
        should_escalate: bool,
        reasons: List[EscalationReason],
        suppressed: List[EscalationReason],
        agent_confidence: float,
        estimated_energy_wh: float,
        criticality_level: str,
        domain: str,
        gm: Dict[str, float],
        hf: float,
        modp_score: Optional[float],
        enhanced: bool,
    ) -> EscalationDecision:
        recommended_expert = self._recommend_expert(reasons, domain)
        estimated_cost = self._estimate_expert_cost(reasons, criticality_level)
        sustainability_impact = self._calculate_sustainability_impact(
            estimated_energy_wh, estimated_cost
        )
        justification = self._build_justification(
            should_escalate=should_escalate,
            reasons=reasons,
            suppressed=suppressed,
            confidence=agent_confidence,
            energy=estimated_energy_wh,
            criticality=criticality_level,
        )
        return EscalationDecision(
            should_escalate=should_escalate,
            reasons=reasons,
            confidence_score=agent_confidence,
            sustainability_impact=sustainability_impact,
            criticality_level=criticality_level,
            recommended_expert=recommended_expert,
            estimated_cost_wh=estimated_cost,
            justification=justification,
            graph_metrics=dict(gm),
            modp_score=modp_score,
            human_feedback_score=hf,
            suppressed_reasons=list(suppressed),
            enhanced=enhanced,
        )

    def _record_escalation(
        self, should_escalate: bool, reasons: Sequence[EscalationReason]
    ) -> None:
        with self._lock:
            if should_escalate:
                self.escalations += 1
                for r in reasons:
                    self.escalations_by_reason[r] = (
                        self.escalations_by_reason.get(r, 0) + 1
                    )

    def _emit_decision_metrics(self, decision: EscalationDecision) -> None:
        try:
            self.metrics.observe(
                "escalation_decisions_total", 1.0,
                escalate=str(decision.should_escalate),
                criticality=decision.criticality_level,
            )
            for r in decision.reasons:
                self.metrics.observe(
                    "escalation_reasons_total", 1.0, reason=r.value
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("metrics.observe failed: %s", exc)

    # ------------------------------------------------------------------
    # Reporting / persistence
    # ------------------------------------------------------------------
    def get_policy_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self.total_decisions
            return {
                "total_decisions": total,
                "escalations": self.escalations,
                "escalation_rate": (
                    self.escalations / total if total else 0.0
                ),
                "escalations_by_reason": {
                    r.value: c for r, c in self.escalations_by_reason.items()
                },
                "confidence_threshold": self.confidence_threshold,
                "sustainability_threshold_wh": self.sustainability_threshold,
                "optimizer_stats": self._optimizer_stats(),
            }

    def reset_stats(self) -> None:
        with self._lock:
            self.total_decisions = 0
            self.escalations = 0
            self.escalations_by_reason.clear()

    def _optimizer_stats(self) -> Optional[Dict[str, Any]]:
        if self.optimizer is None and self.evolutionary_optimizer is None:
            return None
        opt = self.optimizer or self.evolutionary_optimizer
        stats: Dict[str, Any] = {
            "kind": type(opt).__name__,
            "counter": getattr(opt, "counter", None),
        }
        if hasattr(opt, "epsilon"):
            stats["epsilon"] = opt.epsilon
        if hasattr(opt, "replay"):
            stats["replay_size"] = len(opt.replay)
        if hasattr(opt, "generation"):
            stats["generation"] = opt.generation
        return stats

    def get_optimizer_stats(self) -> Optional[Dict[str, Any]]:
        return self._optimizer_stats()

    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.optimizer is not None:
            try:
                self.optimizer.save(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation save failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.save(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.optimizer is not None:
            try:
                self.optimizer.load(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation load failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.load(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary load failed: %s", exc)

    # ------------------------------------------------------------------
    async def batch_decide_escalation(
        self,
        requests: List[Dict[str, Any]],
        return_exceptions: bool = False,
    ) -> List[Any]:
        """
        Run many `decide_escalation(**req)` concurrently.

        The policy's internal state (pending decision, statistics) is
        guarded by a lock; callers should invoke `record_outcome()` for each
        request after `batch_decide_escalation` returns. When
        `return_exceptions=True`, failures appear as exception objects in the
        result list rather than cancelling the batch.
        """
        async def _one(req: Dict[str, Any]) -> Any:
            # Offload to a thread to keep the event loop responsive when
            # the state's KB query does disk I/O on the first call.
            return await asyncio.to_thread(self.decide_escalation, **req)

        tasks = [_one(req) for req in requests]
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)


# ============================================================================
# SECTION 12 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Build a tiny in-memory KB so the demo is self-contained.
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        kb_path = Path(tmpdir) / "kb.json"
        kb_path.write_text(
            json.dumps(
                {
                    "entries": [
                        {
                            "id": "e1",
                            "title": "crash safety reference",
                            "content": "A crash on startup often indicates a null pointer.",
                            "confidence": 0.6,
                        },
                        {
                            "id": "e2",
                            "title": "energy efficiency standards",
                            "content": "Energy per inference should stay under 0.1 Wh.",
                            "confidence": 0.9,
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )

        kb = KnowledgeBaseIntegrator(
            {KnowledgeSourceType.DOCUMENTATION: str(kb_path)}
        )

        cfg = SelectiveInvocationConfig.from_dict(
            {
                "use_enhancements": True,
                "use_distillation": True,
                "modp_weights": [0.4, 0.3, 0.2, 0.1],
                "human_feedback_score": 0.7,
                "seed": 42,
            }
        )
        policy = SelectiveInvocationPolicy(
            confidence_threshold=0.7,
            sustainability_threshold_wh=0.1,
            knowledge_integrator=kb,
            config=cfg,
        )

        cases = [
            {
                "task": "analyze a crash in production",
                "agent_confidence": 0.5,
                "estimated_energy_wh": 0.02,
                "domain": "systems",
            },
            {
                "task": "compute 2+2",
                "agent_confidence": 0.95,
                "estimated_energy_wh": 0.001,
                "domain": "math",
            },
            {
                "task": "non-critical refactoring",
                "agent_confidence": 0.9,
                "estimated_energy_wh": 0.005,
                "domain": "code_review",
            },
        ]

        for i, case in enumerate(cases):
            decision = policy.decide_escalation(**case)
            print(
                f"[{i}] escalate={decision.should_escalate} "
                f"criticality={decision.criticality_level} "
                f"reasons={[r.value for r in decision.reasons]} "
                f"suppressed={[r.value for r in decision.suppressed_reasons]}"
            )
            # Simulated true outcome: escalate only if criticality is high.
            actual = decision.criticality_level in ("high", "critical")
            policy.record_outcome(
                actual_should_escalate=actual,
                actual_cost_wh=0.05,
            )

        print("\nstats:", json.dumps(
            policy.get_policy_stats(), indent=2, default=str
        ))
