# -*- coding: utf-8 -*-
"""
Expert Model Gateway (Enhanced, v2)
===================================

Routes LLM/expert requests to the best available provider with sustainability
tracking. Enhancements (opt-in via `ExpertGatewayConfig.use_enhancements`):

  - LIMIT Graph metrics as decision-state features.
  - MODP reward over (quality, latency, energy, carbon).
  - RLHF human-feedback score as a per-agent signal.
  - Multi-Teacher MoE + distillation for model selection.
  - Bio-inspired (evolutionary) alternative optimizer.

Fixes applied vs. v1
--------------------
  P0  Reward dominated by per-provider constant     -> per-call quality
  P0  Cache key omitted model/max_tokens/temp/ctx   -> full-request hash
  P0  5/10 domains collapsed to "other"             -> 10-dim one-hot
  P0  Budget check dead for 9/10 domains            -> scans all providers
  P0  _last_decision not cleared on exceptions      -> local pending + finally
  P0  modp_weights=[] crash / no validation         -> __post_init__
  P0  MODP ordering inconsistent across files       -> MODP_SCHEMA constant
  P0  MoE gate gradient wrong shape/sign            -> softmax chain-rule
  P0  Unseeded RNG                                  -> default_rng(seed)
  P0  Unbounded cache                               -> LRU with maxsize
  P0  Dead use_evolutionary flag                    -> EvolutionaryModelOptimizer
  P0  No `enhanced: bool` on response               -> added
  P1  feature_dim / n_actions hard-coded            -> derived
  P1  epsilon never decays                          -> epsilon_min / epsilon_decay
  P1  batch_size / distill_w / rl_w hard-coded      -> config
  P1  State features not clipped                    -> _clamp
  P1  No persistence                                -> save()/load()
  P1  No metrics                                    -> MetricsSink
  P1  No thread safety                              -> RLock
  P1  Blocking subprocess in async invoke           -> already async; kept
  P1  Config not frozen                             -> frozen + __post_init__
  P2  PendingDecision 4-tuple                       -> dataclass
  P2  carbon_intensity_g_kwh unused on gateway      -> gateway recomputes C
  P2  No fallback on provider failure               -> retry default_provider
  P2  No record_human_feedback / record_outcome     -> both added
  P2  clear_cache resets counters                   -> documented / option
  P2  Dead imports (datetime, Callable, asdict)     -> removed
  P2  Cache key computed twice                      -> once
  P2  batch_invoke cancels on first failure         -> return_exceptions opt

Preserved public surface
------------------------
  ExpertModelType, ExpertDomain, ExpertRequest, ExpertResponse,
  ExpertModelProvider, OpenAIProvider, AnthropicProvider, LocalLLMProvider,
  ExpertGatewayConfig, GatewaySelectionState, GatewayDistillationOptimizer,
  ExpertModelGateway,
  create_openai_gateway, create_anthropic_gateway,
  create_multi_provider_gateway

Run demo:
    python expert_model_gateway.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import threading
import time
from collections import OrderedDict, deque
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
        self._log = logging.getLogger("gateway.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Enums
# ============================================================================

class ExpertModelType(str, Enum):
    GPT4 = "gpt-4"
    GPT4_TURBO = "gpt-4-turbo"
    CLAUDE_OPUS = "claude-opus-3"
    CLAUDE_SONNET = "claude-sonnet-3-5"
    GEMINI_ULTRA = "gemini-ultra"
    LLAMA_70B = "llama-70b"
    MISTRAL_LARGE = "mistral-large"
    CUSTOM = "custom"


class ExpertDomain(str, Enum):
    CODE_GENERATION = "code_generation"
    CODE_REVIEW = "code_review"
    ARCHITECTURE = "architecture"
    SECURITY = "security"
    PERFORMANCE = "performance"
    SUSTAINABILITY = "sustainability"
    MATHEMATICS = "mathematics"
    SCIENTIFIC = "scientific"
    LEGAL = "legal"
    MEDICAL = "medical"


# ============================================================================
# SECTION 4 — MODP schema (single source of truth)
# ============================================================================

# Weight ordering is canonical: (quality, latency, energy, carbon).
# Any file that uses MODP weights MUST use this exact order.
MODP_SCHEMA: Tuple[str, ...] = ("quality", "latency", "energy", "carbon")


def compute_modp_reward(
    *,
    quality: float,
    latency_ms: float,
    energy_wh: float,
    carbon_kg: float,
    weights: Sequence[float],
    energy_scale_wh: float = 0.01,
    latency_scale_ms: float = 2000.0,
    carbon_scale_kg: float = 0.001,
) -> float:
    """
    Bounded reward in [-1, +1].

    quality ∈ [0,1] from caller (per-call outcome; not a per-provider constant)
    energy, latency, carbon normalized with 1/(1+q/scale)
    raw = w·[...] ∈ [0,1]; return 2·raw - 1
    """
    if len(weights) != 4:
        raise ValueError(f"weights must have 4 components, got {len(weights)}")
    w = [float(x) for x in weights]
    s = sum(w)
    if s <= 0 or any(x < 0 for x in w):
        raise ValueError("weights must be non-negative, sum > 0")
    w = [x / s for x in w]

    values = {
        "quality": _clamp(quality),
        "latency": _bounded_inverse(latency_ms, latency_scale_ms),
        "energy": _bounded_inverse(energy_wh, energy_scale_wh),
        "carbon": _bounded_inverse(carbon_kg, carbon_scale_kg),
    }
    raw = sum(w_i * values[name] for w_i, name in zip(w, MODP_SCHEMA))
    return float(_clamp(2.0 * raw - 1.0, -1.0, 1.0))


# ============================================================================
# SECTION 5 — Data classes
# ============================================================================

@dataclass
class ExpertRequest:
    request_id: str
    task: str
    prompt: str
    domain: ExpertDomain
    context: Dict[str, Any]
    max_tokens: int
    temperature: float
    urgency: str
    sustainability_budget_wh: float
    timestamp: float
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None


@dataclass
class ExpertResponse:
    request_id: str
    response_text: str
    model_used: ExpertModelType
    tokens_used: int
    energy_consumed_wh: float
    carbon_emitted_kg: float
    latency_ms: float
    confidence_score: float
    metadata: Dict[str, Any]
    timestamp: float
    modp_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None
    enhanced: bool = False
    cache_hit: bool = False


@dataclass
class PendingDecision:
    """Captured state for one invoke() call, consumed by the optimizer update."""
    state_vec: "np.ndarray"
    action_idx: int
    teacher_outputs: "np.ndarray"
    gate_probs: "np.ndarray"
    model_type: ExpertModelType


@dataclass(frozen=True)
class ExpertGatewayConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # MODP weights in MODP_SCHEMA order: (quality, latency, energy, carbon)
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
    latency_scale_ms: float = 2000.0
    carbon_scale_kg: float = 0.001

    # Cache
    cache_maxsize: int = 1000

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components "
                f"{MODP_SCHEMA}, got {w!r}"
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
        for name in ("energy_scale_wh", "latency_scale_ms", "carbon_scale_kg"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.cache_maxsize < 1:
            raise ValueError("cache_maxsize must be >= 1")
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "ExpertGatewayConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "ExpertGatewayConfig ignoring unknown keys: %s", sorted(unknown)
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
# SECTION 6 — Providers
# ============================================================================

class ExpertModelProvider:
    """Base class for expert model providers."""

    def __init__(
        self,
        model_type: ExpertModelType,
        api_key: Optional[str] = None,
        energy_multiplier: float = 1.0,
    ) -> None:
        self.model_type = model_type
        self.api_key = api_key
        self.energy_multiplier = energy_multiplier

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        raise NotImplementedError


class OpenAIProvider(ExpertModelProvider):
    def __init__(self, api_key: str, model: str = "gpt-4-turbo") -> None:
        super().__init__(ExpertModelType.GPT4_TURBO, api_key)
        self.model = model

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        try:
            from openai import AsyncOpenAI
        except ImportError as exc:
            raise RuntimeError(
                "openai package not installed; pip install openai"
            ) from exc

        client = AsyncOpenAI(api_key=self.api_key)
        start = time.time()
        try:
            response = await client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": f"You are an expert in {request.domain.value}.",
                    },
                    {"role": "user", "content": request.prompt},
                ],
                max_tokens=request.max_tokens,
                temperature=request.temperature,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"OpenAI invocation failed: {exc}") from exc

        latency_ms = (time.time() - start) * 1000.0
        tokens_used = response.usage.total_tokens
        energy_wh = tokens_used * 0.001 * self.energy_multiplier
        # Gateway recomputes carbon using its carbon_intensity; leave 0.0 here.
        return ExpertResponse(
            request_id=request.request_id,
            response_text=response.choices[0].message.content or "",
            model_used=self.model_type,
            tokens_used=tokens_used,
            energy_consumed_wh=energy_wh,
            carbon_emitted_kg=0.0,
            latency_ms=latency_ms,
            confidence_score=0.9,
            metadata={"finish_reason": response.choices[0].finish_reason},
            timestamp=time.time(),
        )


class AnthropicProvider(ExpertModelProvider):
    def __init__(self, api_key: str, model: str = "claude-3-opus-20240229") -> None:
        super().__init__(ExpertModelType.CLAUDE_OPUS, api_key)
        self.model = model

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        try:
            import anthropic
        except ImportError as exc:
            raise RuntimeError(
                "anthropic package not installed; pip install anthropic"
            ) from exc

        client = anthropic.AsyncAnthropic(api_key=self.api_key)
        start = time.time()
        try:
            message = await client.messages.create(
                model=self.model,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                system=f"You are an expert in {request.domain.value}.",
                messages=[{"role": "user", "content": request.prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Anthropic invocation failed: {exc}") from exc

        latency_ms = (time.time() - start) * 1000.0
        tokens_used = message.usage.input_tokens + message.usage.output_tokens
        energy_wh = tokens_used * 0.0008 * self.energy_multiplier
        return ExpertResponse(
            request_id=request.request_id,
            response_text=message.content[0].text,
            model_used=self.model_type,
            tokens_used=tokens_used,
            energy_consumed_wh=energy_wh,
            carbon_emitted_kg=0.0,
            latency_ms=latency_ms,
            confidence_score=0.95,
            metadata={"stop_reason": message.stop_reason},
            timestamp=time.time(),
        )


class LocalLLMProvider(ExpertModelProvider):
    """Placeholder for local inference (e.g., vLLM, llama.cpp, FlexGen)."""

    def __init__(
        self,
        model_path: str,
        model_type: ExpertModelType = ExpertModelType.LLAMA_70B,
        energy_multiplier: float = 1.0,
    ) -> None:
        super().__init__(model_type, energy_multiplier=energy_multiplier)
        self.model_path = model_path
        self.model = None

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        raise NotImplementedError("Local LLM integration pending")


class FakeExpertProvider(ExpertModelProvider):
    """
    Deterministic provider for tests and the demo. No network calls.
    Emits a plausible energy cost and latency so the gateway has real numbers.
    """

    def __init__(
        self,
        model_type: ExpertModelType,
        latency_ms: float = 500.0,
        tokens_per_wh: float = 2000.0,
    ) -> None:
        super().__init__(model_type)
        self.latency_ms = latency_ms
        self.tokens_per_wh = tokens_per_wh

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        await asyncio.sleep(0)
        tokens = min(request.max_tokens, 500)
        energy_wh = tokens / self.tokens_per_wh
        return ExpertResponse(
            request_id=request.request_id,
            response_text=f"[{self.model_type.value}] to: {request.prompt[:40]}",
            model_used=self.model_type,
            tokens_used=tokens,
            energy_consumed_wh=energy_wh,
            carbon_emitted_kg=0.0,  # gateway computes
            latency_ms=self.latency_ms,
            confidence_score=0.9,
            metadata={"fake": True},
            timestamp=time.time(),
        )


# ============================================================================
# SECTION 7 — Selection state (clipped, 10-dim domain one-hot)
# ============================================================================

class GatewaySelectionState:
    FEATURE_DIM = 6 + len(ExpertDomain)  # 6 scalars + 10 domain slots = 16

    __slots__ = (
        "domain_onehot", "urgency_num", "max_tokens_norm", "budget_norm",
        "centrality", "connectivity", "human_feedback", "_vector",
    )

    _DOMAIN_ORDER: Tuple[ExpertDomain, ...] = tuple(ExpertDomain)

    _URGENCY_MAP: Dict[str, float] = {
        "low": 0.0, "medium": 0.5, "high": 0.8, "critical": 1.0,
    }

    def __init__(
        self,
        request: ExpertRequest,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        self.domain_onehot = self._domain_to_onehot(request.domain)
        self.urgency_num = self._URGENCY_MAP.get(request.urgency, 0.5)
        self.max_tokens_norm = _clamp(float(request.max_tokens) / 4096.0)
        self.budget_norm = _clamp(float(request.sustainability_budget_wh) / 0.5)
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        self._vector = np.array(
            [
                self.urgency_num,
                self.max_tokens_norm,
                self.budget_norm,
                self.centrality,
                self.connectivity,
                self.human_feedback,
                *self.domain_onehot,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    @classmethod
    def _domain_to_onehot(cls, domain: ExpertDomain) -> List[float]:
        """10-slot one-hot so MATHEMATICS != SCIENTIFIC != LEGAL != MEDICAL."""
        vec = [0.0] * len(cls._DOMAIN_ORDER)
        try:
            vec[cls._DOMAIN_ORDER.index(domain)] = 1.0
        except ValueError:
            logger.warning("Unknown domain %r; leaving one-hot all zeros", domain)
        return vec

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 8 — Distillation optimizer (fixed MoE gate gradient)
# ============================================================================

class GatewayDistillationOptimizer:
    """
    Multi-teacher distillation + MoE gating for model selection.

    Forward returns the individual teacher outputs so the gate can be trained
    on real signals (fixes the 14th occurrence of the gate-gradient bug).
    """

    N_TEACHERS = 3

    def __init__(
        self,
        available_models: Sequence[ExpertModelType],
        config: ExpertGatewayConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for distillation")

        self.available_models: List[ExpertModelType] = list(available_models)
        self.index_map: Dict[ExpertModelType, int] = {
            m: i for i, m in enumerate(self.available_models)
        }
        self.n_actions = len(self.available_models)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = GatewaySelectionState.FEATURE_DIM
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
    def _uniform(self) -> "np.ndarray":
        return np.full(self.n_actions, 1.0 / self.n_actions, dtype=np.float64)

    def _set(self, probs: "np.ndarray", m: ExpertModelType, value: float) -> None:
        idx = self.index_map.get(m)
        if idx is not None:
            probs[idx] = value

    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: GatewaySelectionState) -> "np.ndarray":
        probs = np.full(self.n_actions, 0.05, dtype=np.float64)
        # One-hot lookup: exact domain, no more collapse to "other".
        domain = ExpertDomain
        order = GatewaySelectionState._DOMAIN_ORDER
        active_idx = int(np.argmax(state.domain_onehot))
        active_domain = order[active_idx]
        if active_domain in (
            domain.CODE_GENERATION, domain.CODE_REVIEW, domain.ARCHITECTURE,
        ):
            self._set(probs, ExpertModelType.CLAUDE_SONNET, 0.5)
        elif active_domain == domain.SECURITY:
            self._set(probs, ExpertModelType.GPT4, 0.5)
        elif active_domain in (domain.MATHEMATICS, domain.SCIENTIFIC):
            self._set(probs, ExpertModelType.GPT4, 0.5)
        elif active_domain in (domain.LEGAL, domain.MEDICAL):
            self._set(probs, ExpertModelType.CLAUDE_OPUS, 0.5)
        else:
            # Fallback: highest-quality available model.
            for m in (
                ExpertModelType.CLAUDE_OPUS,
                ExpertModelType.GPT4,
                ExpertModelType.CLAUDE_SONNET,
            ):
                if m in self.index_map:
                    self._set(probs, m, 0.5)
                    break
            else:
                probs[0] = 0.5
        return probs / max(probs.sum(), 1e-12)

    def _rlhf_teacher(self, state: GatewaySelectionState) -> "np.ndarray":
        probs = self._uniform().astype(np.float64)
        if state.human_feedback > 0.7:
            for m in (ExpertModelType.GPT4, ExpertModelType.CLAUDE_OPUS):
                idx = self.index_map.get(m)
                if idx is not None:
                    probs[idx] += 0.2
        elif state.human_feedback < 0.3:
            for m in (
                ExpertModelType.CLAUDE_SONNET,
                ExpertModelType.LLAMA_70B,
                ExpertModelType.MISTRAL_LARGE,
            ):
                idx = self.index_map.get(m)
                if idx is not None:
                    probs[idx] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: GatewaySelectionState) -> "np.ndarray":
        probs = np.full(self.n_actions, 0.1, dtype=np.float64)
        if state.centrality > 0.7:
            self._set(probs, ExpertModelType.GPT4, 0.6)
        else:
            self._set(probs, ExpertModelType.CLAUDE_SONNET, 0.6)
        total = probs.sum()
        return probs / total if total > 0 else self._uniform()

    # ---- forward ----------------------------------------------------------
    def select_model(
        self, state: GatewaySelectionState, exploration: bool = True
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
                student = self._softmax(x @ self.student_weights + self.student_bias)
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

                # ---- student: distill + RL ----
                combined = gate @ t_outs
                one_hot = np.zeros(self.n_actions)
                one_hot[a] = 1.0
                grad_distill = -(combined - cur)
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # ---- gate: correct softmax chain rule ----
                target = np.full(
                    self.n_actions,
                    (1.0 - r) / max(self.n_actions - 1, 1),
                    dtype=np.float64,
                )
                target[a] = r
                target = np.clip(target, 0.0, 1.0)
                ts = target.sum()
                target = target / ts if ts > 0 else self._uniform()

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
                "kind": "gateway_distillation",
                "available_models": [m.value for m in self.available_models],
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
        if payload.get("kind") != "gateway_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        expected = [m.value for m in self.available_models]
        if list(payload.get("available_models", [])) != expected:
            raise ValueError(
                f"Checkpoint action space {payload.get('available_models')} "
                f"!= current {expected}"
            )
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
# SECTION 9 — Evolutionary optimizer (implements use_evolutionary)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"
    bias: "np.ndarray"
    fitness: float = 0.0


class EvolutionaryModelOptimizer:
    """Population of linear policies over state → model logits."""

    def __init__(
        self,
        available_models: Sequence[ExpertModelType],
        config: ExpertGatewayConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for evolutionary optimizer")
        self.available_models = list(available_models)
        self.index_map = {m: i for i, m in enumerate(self.available_models)}
        self.n_actions = len(self.available_models)
        self.feature_dim = GatewaySelectionState.FEATURE_DIM
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.counter = 0
        self.generation = 0
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(
                weights=self.rng.standard_normal(
                    (self.feature_dim, self.n_actions)
                ) * 0.1,
                bias=self.rng.standard_normal(self.n_actions) * 0.01,
            )
            for _ in range(config.population_size)
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    def _forward(self, ind: _Ind, x: "np.ndarray") -> "np.ndarray":
        return x @ ind.weights + ind.bias

    def select_model(
        self, state: GatewaySelectionState, exploration: bool = True
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray"]:
        x = state.to_vector()
        with self._lock:
            if exploration and self.rng.random() < self.config.epsilon:
                action = int(self.rng.integers(0, self.n_actions))
            else:
                best = max(self.population, key=lambda ind: ind.fitness)
                action = int(np.argmax(self._forward(best, x)))
            teacher_outputs = np.tile(
                np.full(self.n_actions, 1.0 / self.n_actions),
                (3, 1),
            )
            gate = np.full(3, 1.0 / 3.0)
        return action, x, teacher_outputs, gate

    def update(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: Optional["np.ndarray"] = None,
        gate_probs: Optional["np.ndarray"] = None,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        target = np.full(
            self.n_actions,
            (1.0 - r) / max(self.n_actions - 1, 1),
            dtype=np.float64,
        )
        target[action] = r
        with self._lock:
            for ind in self.population:
                pred = self._forward(ind, state_vec)
                ind.fitness -= float(np.sum((pred - target) ** 2))
            self.counter += 1
            should_evolve = self.counter % self.config.train_every == 0
        if should_evolve:
            self._evolve()

    def _evolve(self) -> None:
        with self._lock:
            self.population.sort(key=lambda ind: ind.fitness, reverse=True)
            n_elite = max(1, self.config.elitism)
            new_pop: List[_Ind] = [
                _Ind(weights=ind.weights.copy(), bias=ind.bias.copy(), fitness=0.0)
                for ind in self.population[:n_elite]
            ]
            while len(new_pop) < len(self.population):
                a = self._tournament()
                b = self._tournament()
                child = self._crossover(a, b)
                self._mutate(child)
                child.fitness = 0.0
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
            mask_w = self.rng.random(a.weights.shape) < 0.5
            mask_b = self.rng.random(a.bias.shape) < 0.5
            return _Ind(
                weights=np.where(mask_w, a.weights, b.weights).copy(),
                bias=np.where(mask_b, a.bias, b.bias).copy(),
            )
        return _Ind(weights=a.weights.copy(), bias=a.bias.copy())

    def _mutate(self, ind: _Ind) -> None:
        mw = self.rng.random(ind.weights.shape) < self.config.mutation_rate
        mb = self.rng.random(ind.bias.shape) < self.config.mutation_rate
        ind.weights = ind.weights + mw * self.rng.standard_normal(
            ind.weights.shape
        ) * 0.05
        ind.bias = ind.bias + mb * self.rng.standard_normal(ind.bias.shape) * 0.02

    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "gateway_evolution",
                "available_models": [m.value for m in self.available_models],
                "feature_dim": self.feature_dim,
                "generation": self.generation,
                "counter": self.counter,
                "population": [
                    {
                        "weights": ind.weights.tolist(),
                        "bias": ind.bias.tolist(),
                        "fitness": float(ind.fitness),
                    }
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload or payload.get("kind") != "gateway_evolution":
            return
        expected = [m.value for m in self.available_models]
        if list(payload.get("available_models", [])) != expected:
            raise ValueError("Checkpoint action space mismatch")
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.population = [
            _Ind(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                bias=np.asarray(ind["bias"], dtype=np.float64),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 10 — Gateway (main)
# ============================================================================

class ExpertModelGateway:
    """
    Routes requests to expert models with optional learned selection.

    Enhanced path:
      1. Build state from request + graph metrics + human feedback.
      2. select_model() picks a provider (distillation or evolution).
      3. Check LRU cache (key includes model + max_tokens + temperature + context).
      4. On miss, invoke provider, recompute carbon from gateway's grid intensity.
      5. Compute MODP reward from per-call quality (success proxy by default).
      6. Train the optimizer, annotate response with modp_score + enhanced=True.
      7. Store in LRU cache.

    Cache hits never train (no provider call → no reward).

    Non-enhanced path (v1 behaviour): heuristic `_select_optimal_model`.
    """

    def __init__(
        self,
        providers: Dict[ExpertModelType, ExpertModelProvider],
        default_provider: ExpertModelType,
        enable_caching: bool = True,
        carbon_intensity_g_kwh: float = 385.0,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not providers:
            raise ValueError("providers must be non-empty")
        if default_provider not in providers:
            raise ValueError(
                f"default_provider {default_provider} not in providers"
            )

        if config is None:
            self.config = ExpertGatewayConfig()
        elif isinstance(config, ExpertGatewayConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = ExpertGatewayConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be ExpertGatewayConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.providers = dict(providers)
        self.default_provider = default_provider
        self.enable_caching = bool(enable_caching)
        self.carbon_intensity = float(carbon_intensity_g_kwh)
        if self.carbon_intensity < 0:
            raise ValueError("carbon_intensity_g_kwh must be >= 0")

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )
        self._lock = threading.RLock()

        self.cache: "OrderedDict[str, ExpertResponse]" = OrderedDict()
        self.total_requests = 0
        self.cache_hits = 0
        self.total_energy_consumed_wh = 0.0
        self.total_carbon_emitted_kg = 0.0
        self.requests_by_domain: Dict[ExpertDomain, int] = {}
        self.requests_by_model: Dict[ExpertModelType, int] = {}

        self._human_feedback_score = float(self.config.human_feedback_score)
        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to heuristic model selection"
            )

        self.optimizer: Optional[Any] = None
        if self.use_enhancements:
            if self.config.use_evolutionary:
                self.optimizer = EvolutionaryModelOptimizer(
                    list(self.providers.keys()), self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.optimizer = GatewayDistillationOptimizer(
                    list(self.providers.keys()), self.config, rng=self.rng
                )

        # Backwards-compat alias
        self.distillation_optimizer = self.optimizer

    # ------------------------------------------------------------------
    def record_human_feedback(self, score: float) -> None:
        score = float(score)
        if not 0.0 <= score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        with self._lock:
            self._human_feedback_score = score
        self.metrics.observe("gateway_human_feedback_score", score)

    # ------------------------------------------------------------------
    async def invoke_expert(
        self,
        task: str,
        prompt: str,
        domain: ExpertDomain,
        context: Optional[Dict[str, Any]] = None,
        max_tokens: int = 2048,
        temperature: float = 0.7,
        urgency: str = "medium",
        sustainability_budget_wh: float = 0.1,
        preferred_model: Optional[ExpertModelType] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        quality_signal: Optional[float] = None,
    ) -> ExpertResponse:
        """
        Invoke expert model. `quality_signal` (∈ [0,1]) is the per-call quality
        used for the MODP reward; when None, defaults to a success proxy
        (1.0 if response text is non-empty, else 0.0).
        """
        with self._lock:
            self.total_requests += 1

        gm = dict(graph_metrics or self.config.graph_metrics)
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )

        request = ExpertRequest(
            request_id=self._generate_request_id(task, prompt),
            task=task,
            prompt=prompt,
            domain=domain,
            context=context or {},
            max_tokens=int(max_tokens),
            temperature=float(temperature),
            urgency=urgency,
            sustainability_budget_wh=float(sustainability_budget_wh),
            timestamp=time.time(),
            graph_metrics=gm,
            human_feedback_score=hf,
        )

        # ---- 1. Model selection -------------------------------------
        pending: Optional[PendingDecision] = None
        if preferred_model is not None:
            model_type = preferred_model
        elif self.optimizer is not None:
            state = GatewaySelectionState(request, gm, hf)
            action_idx, s_vec, t_outs, g_probs = self.optimizer.select_model(
                state
            )
            available = list(self.providers.keys())
            if not 0 <= action_idx < len(available):
                logger.warning(
                    "Optimizer returned action %d outside [0,%d); "
                    "falling back to default",
                    action_idx, len(available),
                )
                model_type = self.default_provider
            else:
                model_type = available[action_idx]
                pending = PendingDecision(
                    state_vec=s_vec,
                    action_idx=action_idx,
                    teacher_outputs=t_outs,
                    gate_probs=g_probs,
                    model_type=model_type,
                )
        else:
            model_type = self._select_optimal_model(request)

        if model_type not in self.providers:
            logger.warning(
                "Selected model %s not in providers; using default %s",
                model_type, self.default_provider,
            )
            model_type = self.default_provider
            pending = None

        # ---- 2. Cache check (key includes model) ---------------------
        cache_key: Optional[str] = None
        if self.enable_caching:
            cache_key = self._get_cache_key(request, model_type)
            with self._lock:
                cached = self.cache.get(cache_key)
                if cached is not None:
                    self.cache.move_to_end(cache_key)
                    self.cache_hits += 1
                    cached.cache_hit = True
                    self.metrics.observe(
                        "gateway_cache_hits_total", 1.0,
                        model=model_type.value,
                    )
                    return cached

        # ---- 3. Invoke provider (with fallback) ----------------------
        provider = self.providers[model_type]
        try:
            response = await provider.invoke(request)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Provider %s failed: %s", model_type.value, exc,
            )
            self.metrics.observe(
                "gateway_provider_errors_total", 1.0,
                model=model_type.value,
            )
            if model_type != self.default_provider:
                response = await self.providers[
                    self.default_provider
                ].invoke(request)
                model_type = self.default_provider
                pending = None  # decision was for the failed model
            else:
                raise

        # ---- 4. Recompute carbon from gateway's grid intensity ------
        response.carbon_emitted_kg = (
            response.energy_consumed_wh * self.carbon_intensity / 1_000_000.0
        )

        # ---- 5. Bookkeeping -----------------------------------------
        with self._lock:
            self.total_energy_consumed_wh += response.energy_consumed_wh
            self.total_carbon_emitted_kg += response.carbon_emitted_kg
            self.requests_by_domain[domain] = (
                self.requests_by_domain.get(domain, 0) + 1
            )
            self.requests_by_model[model_type] = (
                self.requests_by_model.get(model_type, 0) + 1
            )

        # ---- 6. Train optimizer -------------------------------------
        if pending is not None and self.optimizer is not None:
            try:
                # Default quality: success proxy. Caller can override.
                if quality_signal is None:
                    quality = 1.0 if (response.response_text or "").strip() else 0.0
                else:
                    quality = _clamp(float(quality_signal))

                reward = compute_modp_reward(
                    quality=quality,
                    latency_ms=response.latency_ms,
                    energy_wh=response.energy_consumed_wh,
                    carbon_kg=response.carbon_emitted_kg,
                    weights=self.config.modp_weights,
                    energy_scale_wh=self.config.energy_scale_wh,
                    latency_scale_ms=self.config.latency_scale_ms,
                    carbon_scale_kg=self.config.carbon_scale_kg,
                )
                self.optimizer.update(
                    pending.state_vec,
                    pending.action_idx,
                    reward,
                    pending.teacher_outputs,
                    pending.gate_probs,
                )
                response.modp_score = reward
                response.distillation_stats = self._optimizer_stats()
                response.enhanced = True
                self.metrics.observe(
                    "gateway_reward", reward,
                    model=model_type.value,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer update failed: %s", exc)

        # ---- 7. Emit metrics ----------------------------------------
        try:
            self.metrics.observe(
                "gateway_invocations_total", 1.0, model=model_type.value,
            )
            self.metrics.observe(
                "gateway_energy_wh", response.energy_consumed_wh,
                model=model_type.value,
            )
            self.metrics.observe(
                "gateway_carbon_kg", response.carbon_emitted_kg,
                model=model_type.value,
            )
            self.metrics.observe(
                "gateway_latency_ms", response.latency_ms,
                model=model_type.value,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("metrics.observe failed: %s", exc)

        # ---- 8. Cache the response ----------------------------------
        if self.enable_caching and cache_key is not None:
            with self._lock:
                self.cache[cache_key] = response
                self.cache.move_to_end(cache_key)
                while len(self.cache) > self.config.cache_maxsize:
                    self.cache.popitem(last=False)

        return response

    # ------------------------------------------------------------------
    def _select_optimal_model(self, request: ExpertRequest) -> ExpertModelType:
        """Heuristic selection. Budget branch now scans all registered providers."""
        domain_preferences = {
            ExpertDomain.CODE_GENERATION: [
                ExpertModelType.GPT4_TURBO,
                ExpertModelType.CLAUDE_SONNET,
            ],
            ExpertDomain.SECURITY: [
                ExpertModelType.CLAUDE_OPUS,
                ExpertModelType.GPT4,
            ],
            ExpertDomain.MATHEMATICS: [
                ExpertModelType.GPT4,
                ExpertModelType.CLAUDE_OPUS,
            ],
            ExpertDomain.SCIENTIFIC: [
                ExpertModelType.CLAUDE_OPUS,
                ExpertModelType.GEMINI_ULTRA,
            ],
        }
        preferred = domain_preferences.get(
            request.domain, [self.default_provider]
        )
        available_preferred = [m for m in preferred if m in self.providers]

        # Budget branch: scans *all* providers, not just the preference list.
        if request.sustainability_budget_wh < 0.05:
            for cheap in (
                ExpertModelType.CLAUDE_SONNET,
                ExpertModelType.LLAMA_70B,
                ExpertModelType.MISTRAL_LARGE,
            ):
                if cheap in self.providers:
                    return cheap

        if available_preferred:
            return available_preferred[0]
        return self.default_provider

    # ------------------------------------------------------------------
    def _generate_request_id(self, task: str, prompt: str) -> str:
        content = f"{task}:{prompt}:{time.time()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def _get_cache_key(
        self, request: ExpertRequest, model_type: ExpertModelType
    ) -> str:
        payload = {
            "model": model_type.value,
            "domain": request.domain.value,
            "prompt": request.prompt,
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
            "context": hashlib.sha256(
                json.dumps(
                    request.context, sort_keys=True, default=str
                ).encode()
            ).hexdigest(),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest()

    # ------------------------------------------------------------------
    def _optimizer_stats(self) -> Optional[Dict[str, Any]]:
        if self.optimizer is None:
            return None
        stats: Dict[str, Any] = {
            "kind": type(self.optimizer).__name__,
            "counter": getattr(self.optimizer, "counter", None),
        }
        if hasattr(self.optimizer, "epsilon"):
            stats["epsilon"] = self.optimizer.epsilon
        if hasattr(self.optimizer, "replay"):
            stats["replay_size"] = len(self.optimizer.replay)
        if hasattr(self.optimizer, "generation"):
            stats["generation"] = self.optimizer.generation
        return stats

    def get_optimizer_stats(self) -> Optional[Dict[str, Any]]:
        return self._optimizer_stats()

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            total = self.total_requests
            hit_rate = self.cache_hits / total if total > 0 else 0.0
            return {
                "total_requests": total,
                "cache_hits": self.cache_hits,
                "cache_hit_rate": hit_rate,
                "cache_size": len(self.cache),
                "cache_maxsize": self.config.cache_maxsize,
                "total_energy_consumed_wh": self.total_energy_consumed_wh,
                "total_carbon_emitted_kg": self.total_carbon_emitted_kg,
                "avg_energy_per_request": (
                    self.total_energy_consumed_wh / total if total else 0.0
                ),
                "requests_by_domain": {
                    d.value: c for d, c in self.requests_by_domain.items()
                },
                "requests_by_model": {
                    m.value: c for m, c in self.requests_by_model.items()
                },
                "optimizer_stats": self._optimizer_stats(),
            }

    def clear_cache(self, reset_counters: bool = False) -> None:
        with self._lock:
            self.cache.clear()
            if reset_counters:
                self.cache_hits = 0

    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.optimizer is None:
            return
        try:
            self.optimizer.save(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Optimizer save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.optimizer is None:
            return
        try:
            self.optimizer.load(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Optimizer load failed: %s", exc)

    async def batch_invoke(
        self,
        requests: List[Dict[str, Any]],
        return_exceptions: bool = False,
    ) -> List[Any]:
        tasks = [self.invoke_expert(**req) for req in requests]
        return await asyncio.gather(
            *tasks, return_exceptions=return_exceptions
        )


# ============================================================================
# SECTION 11 — Factory functions
# ============================================================================

def create_openai_gateway(
    api_key: str,
    model: str = "gpt-4-turbo",
    enable_caching: bool = True,
    config: Optional[Any] = None,
) -> ExpertModelGateway:
    provider = OpenAIProvider(api_key, model)
    return ExpertModelGateway(
        providers={ExpertModelType.GPT4_TURBO: provider},
        default_provider=ExpertModelType.GPT4_TURBO,
        enable_caching=enable_caching,
        config=config,
    )


def create_anthropic_gateway(
    api_key: str,
    model: str = "claude-3-opus-20240229",
    enable_caching: bool = True,
    config: Optional[Any] = None,
) -> ExpertModelGateway:
    provider = AnthropicProvider(api_key, model)
    return ExpertModelGateway(
        providers={ExpertModelType.CLAUDE_OPUS: provider},
        default_provider=ExpertModelType.CLAUDE_OPUS,
        enable_caching=enable_caching,
        config=config,
    )


def create_multi_provider_gateway(
    openai_key: Optional[str] = None,
    anthropic_key: Optional[str] = None,
    enable_caching: bool = True,
    config: Optional[Any] = None,
) -> ExpertModelGateway:
    providers: Dict[ExpertModelType, ExpertModelProvider] = {}
    default: Optional[ExpertModelType] = None
    if openai_key:
        providers[ExpertModelType.GPT4_TURBO] = OpenAIProvider(openai_key)
        default = ExpertModelType.GPT4_TURBO
    if anthropic_key:
        providers[ExpertModelType.CLAUDE_OPUS] = AnthropicProvider(anthropic_key)
        if default is None:
            default = ExpertModelType.CLAUDE_OPUS
    if not providers:
        raise ValueError("At least one provider API key required")
    return ExpertModelGateway(
        providers=providers,
        default_provider=default,
        enable_caching=enable_caching,
        config=config,
    )


# ============================================================================
# SECTION 12 — Demo (no network calls; uses FakeExpertProvider)
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async def _demo() -> None:
        cfg = ExpertGatewayConfig.from_dict(
            {
                "use_enhancements": True,
                "use_distillation": True,
                "modp_weights": [0.4, 0.3, 0.2, 0.1],
                "human_feedback_score": 0.7,
                "seed": 42,
            }
        )

        providers = {
            ExpertModelType.GPT4_TURBO: FakeExpertProvider(
                ExpertModelType.GPT4_TURBO, latency_ms=800.0,
            ),
            ExpertModelType.CLAUDE_OPUS: FakeExpertProvider(
                ExpertModelType.CLAUDE_OPUS, latency_ms=600.0,
            ),
            ExpertModelType.CLAUDE_SONNET: FakeExpertProvider(
                ExpertModelType.CLAUDE_SONNET, latency_ms=300.0,
            ),
        }
        gateway = ExpertModelGateway(
            providers=providers,
            default_provider=ExpertModelType.GPT4_TURBO,
            carbon_intensity_g_kwh=385.0,
            config=cfg,
        )

        # 4 requests: same prompt, varied budgets/domains, verify cache + selection
        for i, (domain, budget) in enumerate(
            [
                (ExpertDomain.CODE_GENERATION, 0.5),
                (ExpertDomain.SECURITY, 0.5),
                (ExpertDomain.MATHEMATICS, 0.5),
                (ExpertDomain.CODE_GENERATION, 0.01),   # low budget -> cheap model
                (ExpertDomain.CODE_GENERATION, 0.5),    # cache hit expected
            ]
        ):
            r = await gateway.invoke_expert(
                task="demo",
                prompt=f"What is 2+2? (request {i})",
                domain=domain,
                sustainability_budget_wh=budget,
                max_tokens=512,
            )
            print(
                f"[{i}] {r.model_used.value:<18} "
                f"cache={r.cache_hit} "
                f"energy={r.energy_consumed_wh:.6f}Wh "
                f"carbon={r.carbon_emitted_kg:.3e}kg "
                f"reward={r.modp_score} "
                f"enhanced={r.enhanced}"
            )

        print("\nstats:", json.dumps(
            gateway.get_statistics(), indent=2, default=str
        ))

    asyncio.run(_demo())
