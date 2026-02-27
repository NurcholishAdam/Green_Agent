# -*- coding: utf-8 -*-
"""
Expert Model Gateway

Provides optional interface to larger LLMs and expert models for tasks
requiring higher capability or domain expertise. Includes sustainability
tracking to ensure expert invocation is justified.
"""

from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import asyncio
import time
import hashlib


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
    urgency: str  # "low", "medium", "high", "critical"
    sustainability_budget_wh: float
    timestamp: float


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


class ExpertModelProvider:
    """Base class for expert model providers."""
    
    def __init__(
        self,
        model_type: ExpertModelType,
        api_key: Optional[str] = None,
        energy_multiplier: float = 1.0
    ):
        self.model_type = model_type
        self.api_key = api_key
        self.energy_multiplier = energy_multiplier
        
    async def invoke(
        self,
        request: ExpertRequest
    ) -> ExpertResponse:
        """Invoke expert model (override in subclasses)."""
        raise NotImplementedError


class OpenAIProvider(ExpertModelProvider):
    """OpenAI GPT-4 provider."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo"):
        super().__init__(ExpertModelType.GPT4_TURBO, api_key)
        self.model = model
        
    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        """Invoke OpenAI API."""
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
            
            # Estimate energy (GPT-4: ~0.001 Wh per token)
            tokens_used = response.usage.total_tokens
            energy_wh = tokens_used * 0.001 * self.energy_multiplier
            carbon_kg = energy_wh * 0.000385  # Default grid intensity
            
            return ExpertResponse(
                request_id=request.request_id,
                response_text=response.choices[0].message.content,
                model_used=self.model_type,
                tokens_used=tokens_used,
                energy_consumed_wh=energy_wh,
                carbon_emitted_kg=carbon_kg,
                latency_ms=latency,
                confidence_score=0.9,  # Could extract from model
                metadata={"finish_reason": response.choices[0].finish_reason},
                timestamp=time.time()
            )
            
        except Exception as e:
            raise Exception(f"OpenAI invocation failed: {e}")


class AnthropicProvider(ExpertModelProvider):
    """Anthropic Claude provider."""
    
    def __init__(self, api_key: str, model: str = "claude-3-opus-20240229"):
        super().__init__(ExpertModelType.CLAUDE_OPUS, api_key)
        self.model = model
        
    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        """Invoke Anthropic API."""
        try:
            import anthropic
            client = anthropic.AsyncAnthropic(api_key=self.api_key)
            
            start_time = time.time()
            
            message = await client.messages.create(
                model=self.model,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                system=f"You are an expert in {request.domain.value}.",
                messages=[
                    {"role": "user", "content": request.prompt}
                ]
            )
            
            latency = (time.time() - start_time) * 1000
            
            # Estimate energy (Claude: ~0.0008 Wh per token)
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
    """Local LLM provider (for Llama, Mistral, etc.)."""
    
    def __init__(
        self,
        model_path: str,
        model_type: ExpertModelType = ExpertModelType.LLAMA_70B
    ):
        super().__init__(model_type)
        self.model_path = model_path
        self.model = None
        
    async def invoke(self, request: ExpertRequest) -> ExpertResponse:
        """Invoke local LLM."""
        # Placeholder for local LLM integration
        # Would integrate with llama.cpp, vLLM, or similar
        raise NotImplementedError("Local LLM integration pending")


class ExpertModelGateway:
    """
    Gateway for routing requests to expert models.
    
    Features:
    - Multiple provider support (OpenAI, Anthropic, local)
    - Automatic provider selection based on domain
    - Sustainability tracking
    - Cost optimization
    - Caching for repeated queries
    - Rate limiting and quota management
    """
    
    def __init__(
        self,
        providers: Dict[ExpertModelType, ExpertModelProvider],
        default_provider: ExpertModelType,
        enable_caching: bool = True,
        carbon_intensity_g_kwh: float = 385.0
    ):
        """
        Initialize expert model gateway.
        
        Args:
            providers: Dictionary of available providers
            default_provider: Default provider to use
            enable_caching: Enable response caching
            carbon_intensity_g_kwh: Grid carbon intensity
        """
        self.providers = providers
        self.default_provider = default_provider
        self.enable_caching = enable_caching
        self.carbon_intensity = carbon_intensity_g_kwh
        
        # Cache
        self.cache: Dict[str, ExpertResponse] = {}
        
        # Statistics
        self.total_requests = 0
        self.cache_hits = 0
        self.total_energy_consumed_wh = 0.0
        self.total_carbon_emitted_kg = 0.0
        self.requests_by_domain: Dict[ExpertDomain, int] = {}
        self.requests_by_model: Dict[ExpertModelType, int] = {}
        
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
        preferred_model: Optional[ExpertModelType] = None
    ) -> ExpertResponse:
        """
        Invoke expert model.
        
        Args:
            task: Task description
            prompt: Prompt for expert
            domain: Domain specialization
            context: Additional context
            max_tokens: Maximum tokens to generate
            temperature: Temperature for generation
            urgency: Task urgency level
            sustainability_budget_wh: Energy budget in Wh
            preferred_model: Preferred model (optional)
            
        Returns:
            Expert response
        """
        self.total_requests += 1
        
        # Create request
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
            timestamp=time.time()
        )
        
        # Check cache
        if self.enable_caching:
            cache_key = self._get_cache_key(request)
            if cache_key in self.cache:
                self.cache_hits += 1
                return self.cache[cache_key]
        
        # Select provider
        model_type = preferred_model or self._select_optimal_model(request)
        
        if model_type not in self.providers:
            model_type = self.default_provider
        
        provider = self.providers[model_type]
        
        # Invoke expert
        response = await provider.invoke(request)
        
        # Update statistics
        self.total_energy_consumed_wh += response.energy_consumed_wh
        self.total_carbon_emitted_kg += response.carbon_emitted_kg
        self.requests_by_domain[domain] = self.requests_by_domain.get(domain, 0) + 1
        self.requests_by_model[model_type] = self.requests_by_model.get(model_type, 0) + 1
        
        # Cache response
        if self.enable_caching:
            cache_key = self._get_cache_key(request)
            self.cache[cache_key] = response
        
        return response
    
    def _select_optimal_model(self, request: ExpertRequest) -> ExpertModelType:
        """
        Select optimal model based on request characteristics.
        
        Considers:
        - Domain expertise
        - Sustainability budget
        - Urgency
        - Cost
        """
        # Domain-specific model preferences
        domain_preferences = {
            ExpertDomain.CODE_GENERATION: [ExpertModelType.GPT4_TURBO, ExpertModelType.CLAUDE_SONNET],
            ExpertDomain.SECURITY: [ExpertModelType.CLAUDE_OPUS, ExpertModelType.GPT4],
            ExpertDomain.MATHEMATICS: [ExpertModelType.GPT4, ExpertModelType.CLAUDE_OPUS],
            ExpertDomain.SCIENTIFIC: [ExpertModelType.CLAUDE_OPUS, ExpertModelType.GEMINI_ULTRA],
        }
        
        # Get preferred models for domain
        preferred = domain_preferences.get(request.domain, [self.default_provider])
        
        # Filter by available providers
        available_preferred = [m for m in preferred if m in self.providers]
        
        if not available_preferred:
            return self.default_provider
        
        # If low sustainability budget, prefer efficient model
        if request.sustainability_budget_wh < 0.05:
            # Claude Sonnet is typically more efficient than Opus/GPT-4
            if ExpertModelType.CLAUDE_SONNET in available_preferred:
                return ExpertModelType.CLAUDE_SONNET
        
        # Return first available preferred model
        return available_preferred[0]
    
    def _generate_request_id(self, task: str, prompt: str) -> str:
        """Generate unique request ID."""
        content = f"{task}:{prompt}:{time.time()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _get_cache_key(self, request: ExpertRequest) -> str:
        """Generate cache key for request."""
        # Cache based on prompt and domain (ignore temperature for caching)
        content = f"{request.domain.value}:{request.prompt}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        cache_hit_rate = (
            self.cache_hits / self.total_requests
            if self.total_requests > 0 else 0
        )
        
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_hit_rate": cache_hit_rate,
            "total_energy_consumed_wh": self.total_energy_consumed_wh,
            "total_carbon_emitted_kg": self.total_carbon_emitted_kg,
            "avg_energy_per_request": (
                self.total_energy_consumed_wh / self.total_requests
                if self.total_requests > 0 else 0
            ),
            "requests_by_domain": {
                domain.value: count
                for domain, count in self.requests_by_domain.items()
            },
            "requests_by_model": {
                model.value: count
                for model, count in self.requests_by_model.items()
            }
        }
    
    def clear_cache(self):
        """Clear response cache."""
        self.cache.clear()
    
    async def batch_invoke(
        self,
        requests: List[Dict[str, Any]]
    ) -> List[ExpertResponse]:
        """
        Batch invoke multiple expert requests.
        
        Args:
            requests: List of request parameters
            
        Returns:
            List of expert responses
        """
        tasks = [
            self.invoke_expert(**req)
            for req in requests
        ]
        
        return await asyncio.gather(*tasks)


# Convenience functions

def create_openai_gateway(
    api_key: str,
    model: str = "gpt-4-turbo",
    enable_caching: bool = True
) -> ExpertModelGateway:
    """Create gateway with OpenAI provider."""
    provider = OpenAIProvider(api_key, model)
    
    return ExpertModelGateway(
        providers={ExpertModelType.GPT4_TURBO: provider},
        default_provider=ExpertModelType.GPT4_TURBO,
        enable_caching=enable_caching
    )


def create_anthropic_gateway(
    api_key: str,
    model: str = "claude-3-opus-20240229",
    enable_caching: bool = True
) -> ExpertModelGateway:
    """Create gateway with Anthropic provider."""
    provider = AnthropicProvider(api_key, model)
    
    return ExpertModelGateway(
        providers={ExpertModelType.CLAUDE_OPUS: provider},
        default_provider=ExpertModelType.CLAUDE_OPUS,
        enable_caching=enable_caching
    )


def create_multi_provider_gateway(
    openai_key: Optional[str] = None,
    anthropic_key: Optional[str] = None,
    enable_caching: bool = True
) -> ExpertModelGateway:
    """Create gateway with multiple providers."""
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
        enable_caching=enable_caching
    )
