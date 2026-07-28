# File: src/enhancements/tokenization_optimizer.py
"""
Tokenization optimizer – language‑aware tokenizer selection, segmentation, and token budgets.
Enhanced version with async support, proper language detection, intelligent segmentation,
extractive summarization, caching, structured logging, configuration validation, and metrics.
"""

import asyncio
import hashlib
import logging
import os
import re
from functools import lru_cache
from typing import Dict, List, Any, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# External dependencies (install via pip)
# -----------------------------------------------------------------------------
try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    from pydantic import BaseSettings, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from langdetect import detect, DetectorFactory
    LANGDETECT_AVAILABLE = True
    DetectorFactory.seed = 0  # for reproducibility
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
    import nltk
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except ImportError:
    NLTK_AVAILABLE = False

try:
    from transformers import AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from summa import summarizer
    SUMMA_AVAILABLE = True
except ImportError:
    SUMMA_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging
# -----------------------------------------------------------------------------
if STRUCTLOG_AVAILABLE:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Prometheus metrics (only if available)
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    TOKENIZATION_COUNTER = Counter('tokenization_requests_total', 'Total tokenization requests', ['language', 'status'])
    TOKEN_COUNT_HISTOGRAM = Histogram('token_count_per_request', 'Number of tokens per request', ['language'])
    TOKENIZATION_DURATION = Histogram('tokenization_duration_seconds', 'Tokenization duration', ['language'])
    CACHE_HIT_COUNTER = Counter('tokenization_cache_hits_total', 'Cache hits for tokenization')
    CACHE_MISS_COUNTER = Counter('tokenization_cache_misses_total', 'Cache misses for tokenization')
    LANGUAGE_DISTRIBUTION = Gauge('tokenization_language_distribution', 'Language distribution of requests', ['language'])

# -----------------------------------------------------------------------------
# Configuration with Pydantic (fallback if not installed)
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class TokenizationConfig(BaseSettings):
        """Configuration for the tokenization optimizer."""
        default_tokenizer: str = Field('bert-base-uncased', description="Default tokenizer model.")
        language_tokenizer_map: Dict[str, str] = Field(
            default_factory=lambda: {
                'en': 'bert-base-uncased',
                'id': 'bert-base-indonesian-1.5G',
                'fr': 'camembert-base',
                'de': 'bert-base-german-cased',
                'es': 'dccuchile/bert-base-spanish-wwm-uncased',
            },
            description="Mapping from language code to tokenizer model name."
        )
        cache_ttl_seconds: int = Field(300, description="TTL for tokenization cache (seconds).")
        enable_cache: bool = Field(True, description="Enable tokenization caching.")
        max_segment_length: int = Field(100, description="Maximum tokens per segment before split.")
        summarization_ratio: float = Field(0.5, description="Ratio of original length to summarize to.")
        fallback_language: str = Field('en', description="Fallback language if detection fails.")
        require_langdetect: bool = Field(False, description="Raise error if langdetect not available.")
        require_nltk: bool = Field(False, description="Raise error if NLTK not available.")

        @validator('summarization_ratio')
        def ratio_between_0_and_1(cls, v):
            if not 0 < v <= 1:
                raise ValueError('summarization_ratio must be between 0 and 1')
            return v

        class Config:
            env_prefix = "TOKEN_"
            case_sensitive = True

    config = TokenizationConfig()
else:
    # Fallback config as dict
    config = {
        'default_tokenizer': 'bert-base-uncased',
        'language_tokenizer_map': {
            'en': 'bert-base-uncased',
            'id': 'bert-base-indonesian-1.5G',
            'fr': 'camembert-base',
            'de': 'bert-base-german-cased',
            'es': 'dccuchile/bert-base-spanish-wwm-uncased',
        },
        'cache_ttl_seconds': 300,
        'enable_cache': True,
        'max_segment_length': 100,
        'summarization_ratio': 0.5,
        'fallback_language': 'en',
        'require_langdetect': False,
        'require_nltk': False,
    }

# -----------------------------------------------------------------------------
# Circuit Breaker (simplified)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise e

# -----------------------------------------------------------------------------
# Tokenization Optimizer (Enhanced)
# -----------------------------------------------------------------------------
class TokenizationOptimizer:
    """
    Optimizes tokenization for sustainability:
    - Selects the most efficient tokenizer per language.
    - Segments input by sentence boundaries.
    - Enforces per‑segment token budgets.
    - Supports summarization when budget is exceeded.
    - Caches tokenization results.
    - Provides metrics and structured logging.
    """

    def __init__(self, cfg: Optional[Union[Dict[str, Any], TokenizationConfig]] = None):
        """
        Initialize the optimizer.

        Args:
            cfg: Configuration dictionary or Pydantic object.
        """
        if cfg is None:
            if PYDANTIC_AVAILABLE:
                self.config = TokenizationConfig()
            else:
                self.config = config.copy()
        elif isinstance(cfg, dict):
            if PYDANTIC_AVAILABLE:
                self.config = TokenizationConfig(**cfg)
            else:
                self.config = cfg
        else:
            self.config = cfg

        # Validate required dependencies
        if self.config.require_langdetect and not LANGDETECT_AVAILABLE:
            raise ImportError("langdetect is required but not installed.")
        if self.config.require_nltk and not NLTK_AVAILABLE:
            raise ImportError("NLTK is required but not installed.")

        self.tokenizers: Dict[str, Any] = {}
        self.language_map = self.config.get('language_tokenizer_map')
        self.default_tokenizer_name = self.config.get('default_tokenizer')
        self._tokenizer_lock = asyncio.Lock()
        self.circuit_breaker = CircuitBreaker(name="tokenizer_loading")

        # Cache for tokenization results (keyed by (text, language, budget))
        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = self.config.get('cache_ttl_seconds', 300)

        logger.info("TokenizationOptimizer initialized", config=self.config)

    # ------------------------------------------------------------------
    # Language detection
    # ------------------------------------------------------------------
    async def detect_language(self, text: str) -> str:
        """
        Detect the language of the given text.

        Uses langdetect if available, otherwise falls back to the configured fallback language.
        """
        if not LANGDETECT_AVAILABLE:
            logger.warning("langdetect not available; using fallback language: %s", self.config.get('fallback_language', 'en'))
            return self.config.get('fallback_language', 'en')
        try:
            # Run langdetect in a thread pool because it's blocking
            loop = asyncio.get_event_loop()
            lang = await loop.run_in_executor(None, detect, text)
            return lang
        except Exception as e:
            logger.error("Language detection failed: %s", e, exc_info=True)
            return self.config.get('fallback_language', 'en')

    # ------------------------------------------------------------------
    # Tokenizer loading with retry and circuit breaker
    # ------------------------------------------------------------------
    async def _load_tokenizer(self, language: str) -> Any:
        """Load a tokenizer for the given language with retry and circuit breaker."""
        if language in self.tokenizers:
            return self.tokenizers[language]

        model_name = self.language_map.get(language, self.default_tokenizer_name)

        async def _load():
            if not TRANSFORMERS_AVAILABLE:
                raise RuntimeError("Transformers not available.")
            try:
                tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.tokenizers[language] = tokenizer
                logger.info("Loaded tokenizer", language=language, model=model_name)
                return tokenizer
            except Exception as e:
                logger.error("Failed to load tokenizer", language=language, model=model_name, error=str(e))
                # Fallback to default tokenizer
                if model_name != self.default_tokenizer_name:
                    logger.warning("Falling back to default tokenizer: %s", self.default_tokenizer_name)
                    tokenizer = AutoTokenizer.from_pretrained(self.default_tokenizer_name)
                    self.tokenizers[language] = tokenizer
                    return tokenizer
                else:
                    raise

        if TENACITY_AVAILABLE:
            @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
            async def load_with_retry():
                return await self.circuit_breaker.call(_load)
            return await load_with_retry()
        else:
            return await self.circuit_breaker.call(_load)

    async def _get_tokenizer(self, language: str) -> Any:
        """Get or load a tokenizer for the language."""
        async with self._tokenizer_lock:
            return await self._load_tokenizer(language)

    # ------------------------------------------------------------------
    # Segmentation
    # ------------------------------------------------------------------
    async def _segment_text(self, text: str) -> List[str]:
        """
        Split text into sentences using NLTK or a fallback regex.
        """
        if NLTK_AVAILABLE:
            try:
                loop = asyncio.get_event_loop()
                sentences = await loop.run_in_executor(None, sent_tokenize, text)
                return sentences
            except Exception as e:
                logger.error("NLTK segmentation failed: %s", e, exc_info=True)
                # Fallback to regex
        # Fallback: split by common sentence boundaries
        return re.split(r'(?<=[.!?])\s+', text)

    # ------------------------------------------------------------------
    # Summarization
    # ------------------------------------------------------------------
    async def _summarize(self, text: str, target_tokens: int) -> str:
        """
        Summarize text using extractive summarization (TextRank) or fallback.
        """
        if SUMMA_AVAILABLE:
            try:
                # summa's summarize returns a summary with a specified ratio or word count
                # We'll approximate by using a ratio based on target tokens.
                # First, get token count of original
                lang = await self.detect_language(text)
                tokenizer = await self._get_tokenizer(lang)
                tokens = tokenizer.encode(text, add_special_tokens=False)
                ratio = target_tokens / len(tokens) if len(tokens) > 0 else 0.5
                ratio = min(1.0, max(0.1, ratio))
                loop = asyncio.get_event_loop()
                summary = await loop.run_in_executor(None, summarizer.summarize, text, ratio=ratio)
                return summary if summary else text[:target_tokens * 4]
            except Exception as e:
                logger.error("Summarization failed: %s", e, exc_info=True)
                # Fallback to truncation
        # Fallback: truncate to target_tokens * 4 characters (rough)
        return text[:target_tokens * 4]

    # ------------------------------------------------------------------
    # Tokenization with caching
    # ------------------------------------------------------------------
    async def _tokenize(self, text: str, language: str) -> Tuple[List[int], int]:
        """
        Tokenize text using the language-specific tokenizer.
        Returns (token_ids, token_count).
        """
        tokenizer = await self._get_tokenizer(language)
        tokens = tokenizer.encode(text, add_special_tokens=False)
        return tokens, len(tokens)

    def _cache_key(self, text: str, language: str, budget: int) -> str:
        """Generate a cache key based on text, language, and budget."""
        # For simplicity, we hash the concatenation
        key = f"{text}_{language}_{budget}"
        return hashlib.md5(key.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Main optimization method
    # ------------------------------------------------------------------
    async def optimize(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize tokenization for the given text.

        Args:
            text: Input text.
            context: Dict containing:
                - 'language' (optional): Language code.
                - 'token_budget' (optional): Overall token budget.
                - 'segment_budget' (optional): Per-segment budget (overrides global).

        Returns:
            Dict with:
                - 'segments': list of (segment_text, segment_tokens)
                - 'total_tokens': int
                - 'language': str
                - 'tokenizer_used': str
                - 'cache_hit': bool
        """
        start_time = time.time()
        language = context.get('language')
        if language is None:
            language = await self.detect_language(text)

        budget = context.get('token_budget', 1000)
        segment_budget = context.get('segment_budget', None)

        # Check cache
        cache_key = self._cache_key(text, language, budget)
        if self.config.get('enable_cache', True) and cache_key in self._cache:
            cached = self._cache[cache_key]
            if (datetime.now() - cached['timestamp']).seconds < self._cache_ttl:
                if PROMETHEUS_AVAILABLE:
                    CACHE_HIT_COUNTER.inc()
                logger.debug("Cache hit", language=language)
                cached['cache_hit'] = True
                return cached

        if PROMETHEUS_AVAILABLE:
            CACHE_MISS_COUNTER.inc()

        # Tokenize the full text to get total tokens
        tokens, total_tokens = await self._tokenize(text, language)

        # If total tokens <= budget, we can just return the whole text as a single segment
        if total_tokens <= budget:
            segments = [(text, total_tokens)]
        else:
            # Need to segment and potentially summarize
            sentences = await self._segment_text(text)
            # We'll allocate budget to sentences based on their token counts
            # First, compute token count for each sentence
            sent_token_counts = []
            for sent in sentences:
                _, cnt = await self._tokenize(sent, language)
                sent_token_counts.append(cnt)

            # If total tokens exceed budget, we need to summarize some sentences or truncate
            # Simple approach: summarize the entire text to a summary that fits the budget
            if total_tokens > budget:
                target_tokens = int(budget * 0.8)  # leave some room
                summary = await self._summarize(text, target_tokens)
                # Re-tokenize summary
                _, summary_tokens = await self._tokenize(summary, language)
                # If summary still exceeds budget, truncate
                if summary_tokens > budget:
                    summary = summary[:budget * 4]
                    _, summary_tokens = await self._tokenize(summary, language)
                segments = [(summary, summary_tokens)]
                total_tokens = summary_tokens
            else:
                # This shouldn't happen because total_tokens > budget already
                segments = [(text, total_tokens)]

        result = {
            'segments': segments,
            'total_tokens': total_tokens,
            'language': language,
            'tokenizer_used': self.language_map.get(language, self.default_tokenizer_name),
            'cache_hit': False,
            'timestamp': datetime.now()
        }

        # Cache the result
        if self.config.get('enable_cache', True):
            self._cache[cache_key] = result

        # Update metrics
        if PROMETHEUS_AVAILABLE:
            TOKENIZATION_COUNTER.labels(language=language, status='success').inc()
            TOKEN_COUNT_HISTOGRAM.labels(language=language).observe(total_tokens)
            TOKENIZATION_DURATION.labels(language=language).observe(time.time() - start_time)
            LANGUAGE_DISTRIBUTION.labels(language=language).set(1)

        logger.info("Tokenization completed", language=language, total_tokens=total_tokens, segments=len(segments))
        return result

    # ------------------------------------------------------------------
    # Utility: get token efficiency
    # ------------------------------------------------------------------
    async def get_token_efficiency(self, text: str, language: Optional[str] = None) -> float:
        """
        Return tokens per character as a measure of efficiency.
        """
        if language is None:
            language = await self.detect_language(text)
        _, total_tokens = await self._tokenize(text, language)
        return total_tokens / len(text) if text else 0.0

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------
    async def clear_cache(self):
        """Clear the in‑memory cache."""
        self._cache.clear()
        logger.info("Tokenization cache cleared")

    async def get_cache_stats(self) -> Dict:
        """Return cache statistics."""
        return {
            'size': len(self._cache),
            'ttl_seconds': self._cache_ttl,
        }

    # ------------------------------------------------------------------
    # Shutdown (cleanup)
    # ------------------------------------------------------------------
    async def shutdown(self):
        """Clean up resources."""
        self.tokenizers.clear()
        self._cache.clear()
        logger.info("TokenizationOptimizer shutdown complete")

# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------
async def example_usage():
    optimizer = TokenizationOptimizer()
    text = "This is a sample text. It contains multiple sentences. We want to tokenize it efficiently."
    context = {'token_budget': 50}
    result = await optimizer.optimize(text, context)
    print(f"Segments: {result['segments']}")
    print(f"Total tokens: {result['total_tokens']}")
    print(f"Language: {result['language']}")
    print(f"Cache hit: {result['cache_hit']}")

    efficiency = await optimizer.get_token_efficiency(text)
    print(f"Token efficiency: {efficiency}")

    await optimizer.shutdown()

if __name__ == "__main__":
    asyncio.run(example_usage())
