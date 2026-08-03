"""
Lightweight LLM client for generating natural language explanations.
Enhanced with retries, circuit breaker, persistent session, and fallback.
"""

import asyncio
import logging
import json
from typing import Dict, Any, Optional, Callable
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tenacity import RetryError

# ---------- Circuit Breaker ----------
class CircuitBreaker:
    """Simple async circuit breaker to protect against repeated failures."""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half-open
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "half-open"
                else:
                    raise RuntimeError("Circuit breaker is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
            raise e

# ---------- LLM Client ----------
class LLMClient:
    """
    Lightweight LLM client with retry, circuit breaker, persistent session, and fallback.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:8000/generate",
        model: str = "small",
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        retry_attempts: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0,
        fallback_generator: Optional[Callable[[str], str]] = None,
    ):
        """
        Args:
            endpoint: LLM API endpoint.
            model: Model identifier.
            headers: Optional HTTP headers (e.g., for authentication).
            timeout: Request timeout in seconds.
            retry_attempts: Number of retry attempts on failure.
            circuit_breaker_threshold: Failures before opening circuit.
            circuit_breaker_timeout: Seconds to wait before trying half-open.
            fallback_generator: Function to generate a fallback explanation if LLM fails.
        """
        self.endpoint = endpoint
        self.model = model
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.fallback_generator = fallback_generator

        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=circuit_breaker_timeout,
        )
        self._logger = logging.getLogger(__name__)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Create or return a persistent ClientSession."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        """Close the persistent session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError)
        ),
    )
    async def _do_request(self, payload: Dict) -> Dict:
        """Perform the HTTP request with retry."""
        session = await self._get_session()
        async with session.post(
            self.endpoint,
            json=payload,
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        ) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"LLM API returned {resp.status}"
                )
            return await resp.json()

    async def generate_explanation(
        self,
        prompt: str,
        max_tokens: int = 150,
        temperature: float = 0.7,
        **kwargs,
    ) -> str:
        """
        Send prompt to LLM and return generated explanation.
        Supports custom generation parameters via kwargs.

        Args:
            prompt: The prompt to send.
            max_tokens: Maximum tokens to generate.
            temperature: Sampling temperature.
            **kwargs: Additional parameters to pass to the API.

        Returns:
            Generated text as string.

        Raises:
            RuntimeError: If all retries fail and no fallback is provided.
        """
        payload = {
            "prompt": prompt,
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            **kwargs,
        }

        self._logger.debug(f"LLM request: {payload}")

        try:
            result = await self._circuit_breaker.call(self._do_request, payload)
            self._logger.debug(f"LLM response: {result}")
            # Extract text; adjust based on your API response structure.
            text = result.get("text")
            if text is None:
                # Try alternative keys if needed
                text = result.get("generated_text", result.get("response", ""))
            return text
        except Exception as e:
            self._logger.warning(f"LLM request failed: {e}")
            if self.fallback_generator:
                return self.fallback_generator(prompt)
            else:
                raise RuntimeError(f"LLM generation failed after retries: {e}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
