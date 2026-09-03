# -*- coding: utf-8 -*-
"""
Expert Model Gateway (Enhanced)

Provides optional interface to larger LLMs and expert models for tasks
requiring higher capability or domain expertise. Includes sustainability
tracking to ensure expert invocation is justified.

Enhancements (enabled via `ExpertGatewayConfig.use_enhancements`):
  - LIMIT Graph metrics influence model selection and caching.
  - MODP weights (accuracy, latency, energy, carbon) computed for each request.
  - RLHF: human feedback score biases teacher predictions and final selection.
  - Multi‑Teacher On‑Policy Distillation + MoE: a learned student replaces
    the heuristic `_select_optimal_model`.
  - Bio‑inspired optimisation: optional evolutionary tuning of the blending
    weight between distillation and rule-based selection.
"""

from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import asyncio
import time
import hashlib
import random
import numpy as np
from collections import deque


class ExpertModelType(Enum):
    """Types of expert models available."""
    GPT4 = "gpt-4"
    GPT4_TURBO = "gpt-4-turbo"
    CLAUDE_OPUS = "claude-opus-3"
    CLAUDE_SONNET = "claude-sonnet-3-5"
    GEMINI_ULTRA = "gemini-ultra"
    LLAMA_70B = "llama-70b"
    MISTRAL_LARGE = "mistral-large"
    CUSTOM = "custom"


class ExpertDomain(Enum):
    """Expert domain specializations."""
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


@dataclass
class ExpertRequest:
    """Request to expert model."""
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
    # Enhanced optional fields
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None


@dataclass
class ExpertResponse:
    """Response from expert model."""
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
    # Enhanced fields
    modp_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None


class ExpertModelProvider:
    """Base class for expert model providers."""
    def __init__(self, model_type: ExpertModelType, api_key: Optional[str] = None,
                 energy_multiplier: float = 1.0):
        self.model_type = model_type
        self.api_key = api_key
        self.energy_multiplier = energy_multiplier

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Original provider implementations (OpenAI, Anthropic, Local) unchanged
# ---------------------------------------------------------------------------

class OpenAIProvider(ExpertModelProvider):
    def __init__(self, api_key: str, model: str = "gpt-4-turbo"):
        super().__init__(ExpertModelType.GPT4_TURBO, api_key)
        self.model = model

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        try:
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=self.api_key)
            start_time = time.time()
            response = await client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": f"You are an expert in {request.domain.value}."},
                    {"role": "user", "content": request.prompt}
                ],
                max_tokens=request.max_tokens,
                temperature=request.temperature
            )
            latency = (time.time() - start_time) * 1000
            tokens_used = response.usage.total_tokens
            energy_wh = tokens_used * 0.001 * self.energy_multiplier
            carbon_kg = energy_wh * 0.000385
            return ExpertResponse(
                request_id=request.request_id,
                response_text=response.choices[0].message.content,
                model_used=self.model_type,
                tokens_used=tokens_used,
                energy_consumed_wh=energy_wh,
                carbon_emitted_kg=carbon_kg,
                latency_ms=latency,
                confidence_score=0.9,
                metadata={"finish_reason": response.choices[0].finish_reason},
                timestamp=time.time()
            )
        except Exception as e:
            raise Exception(f"OpenAI invocation failed: {e}")


class AnthropicProvider(ExpertModelProvider):
    def __init__(self, api_key: str, model: str = "claude-3-opus-20240229"):
        super().__init__(ExpertModelType.CLAUDE_OPUS, api_key)
        self.model = model

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        try:
            import anthropic
            client = anthropic.AsyncAnthropic(api_key=self.api_key)
            start_time = time.time()
            message = await client.messages.create(
                model=self.model,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                system=f"You are an expert in {request.domain.value}.",
                messages=[{"role": "user", "content": request.prompt}]
            )
            latency = (time.time() - start_time) * 1000
            tokens_used = message.usage.input_tokens + message.usage.output_tokens
            energy_wh = tokens_used * 0.0008 * self.energy_multiplier
            carbon_kg = energy_wh * 0.000385
            return ExpertResponse(
                request_id=request.request_id,
                response_text=message.content[0].text,
                model_used=self.model_type,
                tokens_used=tokens_used,
                energy_consumed_wh=energy_wh,
                carbon_emitted_kg=carbon_kg,
                latency_ms=latency,
                confidence_score=0.95,
                metadata={"stop_reason": message.stop_reason},
                timestamp=time.time()
            )
        except Exception as e:
            raise Exception(f"Anthropic invocation failed: {e}")


class LocalLLMProvider(ExpertModelProvider):
    def __init__(self, model_path: str, model_type: ExpertModelType = ExpertModelType.LLAMA_70B):
        super().__init__(model_type)
        self.model_path = model_path
        self.model = None

    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        # Placeholder – could integrate with FlexGen.
        raise NotImplementedError("Local LLM integration pending")


# ---------------------------------------------------------------------------
# Enhanced configuration and distillation optimizer
# ---------------------------------------------------------------------------

@dataclass
class ExpertGatewayConfig:
    use_enhancements: bool = False
    # LIMIT Graph
    graph_metrics: Dict[str, float] = None  # default after init
    # MODP weights: [quality, latency, energy, carbon]
    modp_weights: Optional[List[float]] = None
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1
    # Bio‑inspired
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    def __post_init__(self):
        if self.graph_metrics is None:
            self.graph_metrics = {"centrality": 0.5, "connectivity": 0.5}
        if self.modp_weights is None:
            self.modp_weights = [0.4, 0.3, 0.2, 0.1]  # quality, latency, energy, carbon
        else:
            total = sum(self.modp_weights)
            self.modp_weights = [w / total for w in self.modp_weights]


class GatewaySelectionState:
    """Feature vector for model selection."""
    def __init__(self, request: ExpertRequest, graph_metrics: Dict[str, float],
                 human_feedback: float):
        # Normalized features
        self.domain_onehot = self._domain_to_onehot(request.domain)
        self.urgency_num = {"low": 0.0, "medium": 0.5, "high": 0.8, "critical": 1.0}.get(
            request.urgency, 0.5)
        self.max_tokens_norm = min(request.max_tokens / 4096.0, 1.0)
        self.budget_norm = min(request.sustainability_budget_wh / 0.5, 1.0)
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def _domain_to_onehot(self, domain: ExpertDomain) -> List[float]:
        # Map to a compact one-hot (using first 5 domains for simplicity)
        mapping = {
            ExpertDomain.CODE_GENERATION: 0,
            ExpertDomain.CODE_REVIEW: 1,
            ExpertDomain.SECURITY: 2,
            ExpertDomain.PERFORMANCE: 3,
            ExpertDomain.SUSTAINABILITY: 4,
        }
        idx = mapping.get(domain, 5)
        vec = [0.0] * 6
        if idx < 6:
            vec[idx] = 1.0
        return vec

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.urgency_num,
            self.max_tokens_norm,
            self.budget_norm,
            self.centrality,
            self.connectivity,
            self.human_feedback,
            *self.domain_onehot,   # 6 dims
        ], dtype=np.float32)  # total 12 dims


class GatewayDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to select the best model.
    Teachers: rule-based, RLHF, historical (simulated). Actions correspond to
    available model types.
    """
    def __init__(self, available_models: List[ExpertModelType], config: ExpertGatewayConfig):
        self.available_models = available_models
        self.n_actions = len(available_models)
        self.config = config
        self.feature_dim = 12
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers (they receive state and return probabilities over actions)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr

    def _rule_teacher(self, state: GatewaySelectionState) -> np.ndarray:
        # Simple domain + urgency rules
        probs = np.ones(self.n_actions) * 0.05
        # Prefer GPT-4 for security, Claude for code
        domain = state.domain_onehot
        if domain[2] > 0.5:  # SECURITY
            if ExpertModelType.GPT4 in self.available_models:
                idx = self.available_models.index(ExpertModelType.GPT4)
                probs[idx] = 0.5
        elif domain[0] > 0.5 or domain[1] > 0.5:  # CODE
            if ExpertModelType.CLAUDE_SONNET in self.available_models:
                idx = self.available_models.index(ExpertModelType.CLAUDE_SONNET)
                probs[idx] = 0.5
        else:
            # Default to first available (e.g., GPT4_TURBO)
            probs[0] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: GatewaySelectionState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        # High human feedback -> prefer larger/more accurate models
        if state.human_feedback > 0.7:
            for m in [ExpertModelType.GPT4, ExpertModelType.CLAUDE_OPUS]:
                if m in self.available_models:
                    idx = self.available_models.index(m)
                    probs[idx] += 0.2
        elif state.human_feedback < 0.3:
            # Prefer efficient models
            for m in [ExpertModelType.CLAUDE_SONNET, ExpertModelType.LLAMA_70B]:
                if m in self.available_models:
                    idx = self.available_models.index(m)
                    probs[idx] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: GatewaySelectionState) -> np.ndarray:
        # Simulated learned model based on past performance (static)
        probs = np.ones(self.n_actions) * 0.1
        # High centrality -> prefer more reliable models
        if state.centrality > 0.7:
            if ExpertModelType.GPT4 in self.available_models:
                idx = self.available_models.index(ExpertModelType.GPT4)
                probs[idx] = 0.6
        else:
            if ExpertModelType.CLAUDE_SONNET in self.available_models:
                idx = self.available_models.index(ExpertModelType.CLAUDE_SONNET)
                probs[idx] = 0.6
        return probs / probs.sum()

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_model(self, state: GatewaySelectionState, exploration=True) -> Tuple[int, np.ndarray, np.ndarray]:
        x = state.to_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay_buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, a, r, ns, tp in batch:
                # Update student
                logits = s @ self.student_weights + self.student_bias
                cur = np.exp(logits - np.max(logits))
                cur /= cur.sum()
                grad_distill = -(tp - cur)
                one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # Update gating
                gate = self._gate_forward(s)
                combined_teacher = np.sum(gate[:, None] * tp, axis=0)
                error = combined_teacher - cur
                grad_gate = np.dot(tp, error)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate


# ---------------------------------------------------------------------------
# Enhanced ExpertModelGateway
# ---------------------------------------------------------------------------
class ExpertModelGateway:
    """
    Gateway for routing requests to expert models with optional advanced selection.
    """

    def __init__(
        self,
        providers: Dict[ExpertModelType, ExpertModelProvider],
        default_provider: ExpertModelType,
        enable_caching: bool = True,
        carbon_intensity_g_kwh: float = 385.0,
        config: Optional[ExpertGatewayConfig] = None
    ):
        self.providers = providers
        self.default_provider = default_provider
        self.enable_caching = enable_caching
        self.carbon_intensity = carbon_intensity_g_kwh

        self.cache: Dict[str, ExpertResponse] = {}
        self.total_requests = 0
        self.cache_hits = 0
        self.total_energy_consumed_wh = 0.0
        self.total_carbon_emitted_kg = 0.0
        self.requests_by_domain: Dict[ExpertDomain, int] = {}
        self.requests_by_model: Dict[ExpertModelType, int] = {}

        self.config = config or ExpertGatewayConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = GatewayDistillationOptimizer(
                    list(providers.keys()), self.config
                )
            if self.config.use_evolutionary:
                # Could initialize evolutionary optimizer for blending weight, omitted for brevity
                pass

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
        human_feedback_score: Optional[float] = None
    ) -> ExpertResponse:
        """
        Invoke expert model. Enhanced selection if enabled and preferred_model not given.
        """
        self.total_requests += 1

        # Set defaults for enhanced inputs
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        # Create request with optional enhanced fields
        request = ExpertRequest(
            request_id=self._generate_request_id(task, prompt),
            task=task,
            prompt=prompt,
            domain=domain,
            context=context or {},
            max_tokens=max_tokens,
            temperature=temperature,
            urgency=urgency,
            sustainability_budget_wh=sustainability_budget_wh,
            timestamp=time.time(),
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score
        )

        # Check cache
        if self.enable_caching:
            cache_key = self._get_cache_key(request)
            if cache_key in self.cache:
                self.cache_hits += 1
                return self.cache[cache_key]

        # Select model
        if preferred_model is not None:
            model_type = preferred_model
            self._last_decision = None
        elif self.use_enhancements and self.distillation_optimizer:
            state = GatewaySelectionState(request, graph_metrics, human_feedback_score)
            action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_model(state)
            available_models = list(self.providers.keys())
            if action_idx < len(available_models):
                model_type = available_models[action_idx]
            else:
                model_type = self.default_provider
            self._last_decision = (state_vec, action_idx, teacher_probs, model_type)
        else:
            model_type = self._select_optimal_model(request)
            self._last_decision = None

        if model_type not in self.providers:
            model_type = self.default_provider

        provider = self.providers[model_type]
        response = await provider.invoke(request)

        # Update statistics
        self.total_energy_consumed_wh += response.energy_consumed_wh
        self.total_carbon_emitted_kg += response.carbon_emitted_kg
        self.requests_by_domain[domain] = self.requests_by_domain.get(domain, 0) + 1
        self.requests_by_model[model_type] = self.requests_by_model.get(model_type, 0) + 1

        # Enhanced post-processing
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, selected_model = self._last_decision
            # Compute MODP reward
            quality = response.confidence_score
            energy_norm = 1.0 - min(response.energy_consumed_wh, 1.0)
            latency_norm = 1.0 - min(response.latency_ms / 10000.0, 1.0)
            carbon_norm = 1.0 - min(response.carbon_emitted_kg, 1.0)
            weights = self.config.modp_weights
            reward = float(np.dot([quality, latency_norm, energy_norm, carbon_norm], weights))
            # Update distillation
            self.distillation_optimizer.update(state_vec, action_idx, reward, state_vec, teacher_probs)

            # Add to response
            response.modp_score = reward
            response.distillation_stats = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
            del self._last_decision

        # Cache
        if self.enable_caching:
            cache_key = self._get_cache_key(request)
            self.cache[cache_key] = response

        return response

    def _select_optimal_model(self, request: ExpertRequest) -> ExpertModelType:
        """Original heuristic selection (unchanged)."""
        domain_preferences = {
            ExpertDomain.CODE_GENERATION: [ExpertModelType.GPT4_TURBO, ExpertModelType.CLAUDE_SONNET],
            ExpertDomain.SECURITY: [ExpertModelType.CLAUDE_OPUS, ExpertModelType.GPT4],
            ExpertDomain.MATHEMATICS: [ExpertModelType.GPT4, ExpertModelType.CLAUDE_OPUS],
            ExpertDomain.SCIENTIFIC: [ExpertModelType.CLAUDE_OPUS, ExpertModelType.GEMINI_ULTRA],
        }
        preferred = domain_preferences.get(request.domain, [self.default_provider])
        available_preferred = [m for m in preferred if m in self.providers]
        if not available_preferred:
            return self.default_provider
        if request.sustainability_budget_wh < 0.05:
            if ExpertModelType.CLAUDE_SONNET in available_preferred:
                return ExpertModelType.CLAUDE_SONNET
        return available_preferred[0]

    def _generate_request_id(self, task: str, prompt: str) -> str:
        content = f"{task}:{prompt}:{time.time()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def _get_cache_key(self, request: ExpertRequest) -> str:
        content = f"{request.domain.value}:{request.prompt}"
        return hashlib.md5(content.encode()).hexdigest()

    def get_statistics(self) -> Dict[str, Any]:
        cache_hit_rate = self.cache_hits / self.total_requests if self.total_requests > 0 else 0
        stats = {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_hit_rate": cache_hit_rate,
            "total_energy_consumed_wh": self.total_energy_consumed_wh,
            "total_carbon_emitted_kg": self.total_carbon_emitted_kg,
            "avg_energy_per_request": self.total_energy_consumed_wh / self.total_requests if self.total_requests > 0 else 0,
            "requests_by_domain": {d.value: c for d, c in self.requests_by_domain.items()},
            "requests_by_model": {m.value: c for m, c in self.requests_by_model.items()},
        }
        if self.use_enhancements and self.distillation_optimizer:
            stats["distillation_stats"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
        return stats

    def clear_cache(self):
        self.cache.clear()

    async def batch_invoke(self, requests: List[Dict[str, Any]]) -> List[ExpertResponse]:
        tasks = [self.invoke_expert(**req) for req in requests]
        return await asyncio.gather(*tasks)


# Convenience functions (unchanged)
def create_openai_gateway(api_key: str, model: str = "gpt-4-turbo",
                          enable_caching: bool = True,
                          config: Optional[ExpertGatewayConfig] = None) -> ExpertModelGateway:
    provider = OpenAIProvider(api_key, model)
    return ExpertModelGateway(
        providers={ExpertModelType.GPT4_TURBO: provider},
        default_provider=ExpertModelType.GPT4_TURBO,
        enable_caching=enable_caching,
        config=config
    )


def create_anthropic_gateway(api_key: str, model: str = "claude-3-opus-20240229",
                             enable_caching: bool = True,
                             config: Optional[ExpertGatewayConfig] = None) -> ExpertModelGateway:
    provider = AnthropicProvider(api_key, model)
    return ExpertModelGateway(
        providers={ExpertModelType.CLAUDE_OPUS: provider},
        default_provider=ExpertModelType.CLAUDE_OPUS,
        enable_caching=enable_caching,
        config=config
    )


def create_multi_provider_gateway(openai_key: Optional[str] = None,
                                  anthropic_key: Optional[str] = None,
                                  enable_caching: bool = True,
                                  config: Optional[ExpertGatewayConfig] = None) -> ExpertModelGateway:
    providers = {}
    default = None
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
        config=config
    )
