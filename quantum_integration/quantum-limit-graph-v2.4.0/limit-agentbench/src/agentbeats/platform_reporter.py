# src/agentbeats/platform_reporter.py

"""
AgentBeats Platform Reporter

Bridges the Green Agent runtime to the AgentBeats platform by:

- Submitting structured assessment results (accuracy, latency, energy, carbon,
  sustainability index, feedback, artifacts).
- Emitting real-time trace updates for long-running tasks (progress, warnings,
  and final status).
- Recording every submission / emission in a bounded, auditable history for
  offline replay, dashboards, and post-hoc debugging.

Original behaviour preserved
----------------------------
- ``AgentBeatsPlatformReporter()`` — constructs a reporter bound to a platform
  API client.
- ``submit_result(result: dict)`` — submits a normalized assessment payload.
- ``emit_trace(step: dict)`` — sends a single ``progress`` update.

Enhancements
------------
- Thread-safe via ``RLock``.
- Configurable retries / backoff / timeouts via :class:`ReporterConfig`.
- Immutable ``ReportRecord`` history with bounded ring-buffer.
- Full validation of every field on both ``submit_result`` and ``emit_trace``.
- Strict / non-strict modes (missing fields raise or are defaulted).
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Automatic retry with exponential backoff + jitter for transient failures.
- Context-manager support for scoped reporting batches.
- Custom :class:`PlatformReporterError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
- Graceful degradation if the ``agentbeats`` platform SDK is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PlatformReporterError(ValueError):
    """Raised for invalid inputs, configuration, or submission failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReporterConfig:
    """
    Tunable parameters for the platform reporter.

    Centralizes retry policy, timeouts, and field mappings so deployments can
    calibrate without editing the reporter.
    """

    # Retry policy for transient network failures.
    max_retries: int = 3
    base_backoff_seconds: float = 0.25
    max_backoff_seconds: float = 5.0
    jitter_fraction: float = 0.2

    # Per-call timeout for platform API invocations.
    call_timeout_seconds: float = 15.0

    # Bounded history ring-buffer.
    max_history: Optional[int] = 1000

    # Default artifact MIME type when callers omit it.
    default_artifact_mime: str = "application/octet-stream"

    # Emit trace messages in batches? (batch size > 1 enables buffering.)
    trace_batch_size: int = 1

    # Reporter identity (useful for multi-tenant deployments).
    reporter_id: str = "green-agent"

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise PlatformReporterError("max_retries must be >= 0.")
        if self.base_backoff_seconds < 0:
            raise PlatformReporterError("base_backoff_seconds must be >= 0.")
        if self.max_backoff_seconds < self.base_backoff_seconds:
            raise PlatformReporterError(
                "max_backoff_seconds must be >= base_backoff_seconds."
            )
        if not 0.0 <= self.jitter_fraction <= 1.0:
            raise PlatformReporterError("jitter_fraction must be in [0, 1].")
        if self.call_timeout_seconds <= 0:
            raise PlatformReporterError("call_timeout_seconds must be > 0.")
        if self.max_history is not None and self.max_history <= 0:
            raise PlatformReporterError("max_history must be > 0 or None.")
        if self.trace_batch_size <= 0:
            raise PlatformReporterError("trace_batch_size must be > 0.")


# --------------------------------------------------------------------------- #
# Outcome enum
# --------------------------------------------------------------------------- #
class SubmissionOutcome(str, Enum):
    OK = "ok"
    RETRIED = "retried"
    FAILED = "failed"


# --------------------------------------------------------------------------- #
# Immutable sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReportRecord:
    """Immutable snapshot of one platform interaction."""

    timestamp: float
    kind: str                       # "result" | "trace" | "batch_trace"
    outcome: str                    # SubmissionOutcome value
    assessment_id: Optional[str]
    agent_id: Optional[str]
    attempts: int
    duration_ms: float
    error: Optional[str] = None
    payload_size: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Reporter
# --------------------------------------------------------------------------- #
class AgentBeatsPlatformReporter:
    """
    Reports results to the AgentBeats platform.

    Wraps a ``platform_api`` client (any object exposing async
    ``submit_result(payload)`` and ``send_task_update(payload)`` methods) with
    validation, retries, history, and observability.

    Parameters
    ----------
    platform_api : object, optional
        Client with async ``submit_result`` / ``send_task_update`` methods.
        If omitted, a no-op stub is used (so the reporter can be constructed
        and tested without a live platform connection).
    config : ReporterConfig, optional
        Retry / timeout / history configuration.
    strict : bool, default True
        If True, missing or malformed fields raise
        :class:`PlatformReporterError`. If False, they are logged and
        coerced to defaults.
    """

    # Required top-level keys on ``submit_result``.
    _REQUIRED_RESULT_FIELDS = (
        "assessment_id",
        "agent_id",
        "accuracy",
        "latency",
        "energy_kwh",
        "carbon_co2e_kg",
        "sustainability_index",
        "feedback",
        "artifacts",
    )

    def __init__(
        self,
        platform_api: Any = None,
        *,
        config: Optional[ReporterConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config: ReporterConfig = config or ReporterConfig()
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._history: List[ReportRecord] = []
        self._trace_buffer: List[Dict[str, Any]] = []
        self._ctx_start: Optional[float] = None
        self._started_at: float = time.time()

        self.platform_api = platform_api or _NoopPlatformAPI()

        logger.debug(
            "AgentBeatsPlatformReporter initialized "
            "(reporter_id=%s, strict=%s, retries=%d, batch=%d)",
            self._config.reporter_id,
            self._strict,
            self._config.max_retries,
            self._config.trace_batch_size,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ReporterConfig:
        return self._config

    @property
    def history(self) -> List[ReportRecord]:
        with self._lock:
            return list(self._history)

    @property
    def pending_trace_count(self) -> int:
        with self._lock:
            return len(self._trace_buffer)

    # --------------------------------------------------------- public API
    async def submit_result(
        self,
        result: Mapping[str, Any],
        *,
        label: Optional[str] = None,
        record: bool = True,
    ) -> Dict[str, Any]:
        """
        Submit a normalized assessment result to the platform.

        Parameters
        ----------
        result : Mapping
            Must contain the fields listed in
            :attr:`_REQUIRED_RESULT_FIELDS`. Values are validated and
            normalized before being sent.
        label : str, optional
            Optional label stored alongside the history record.
        record : bool, default True
            If True, append a :class:`ReportRecord` to history.

        Returns
        -------
        dict
            ``{"status": "ok", "attempts": N, "assessment_id": ..., "response": ...}``
            or, in non-strict mode on failure,
            ``{"status": "failed", "error": ...}``.
        """
        payload = self._build_result_payload(result)

        return await self._call_with_retry(
            kind="result",
            api_method_name="submit_result",
            payload=payload,
            assessment_id=payload["assessment_id"],
            agent_id=payload["purple_agent_id"],
            label=label,
            record=record,
        )

    async def emit_trace(
        self,
        step: Mapping[str, Any],
        *,
        label: Optional[str] = None,
        record: bool = True,
        flush: bool = False,
    ) -> Dict[str, Any]:
        """
        Emit a real-time trace update.

        Parameters
        ----------
        step : Mapping
            Must contain ``description`` and ``metrics``. An optional
            ``type`` defaults to ``"progress"``.
        label : str, optional
            Optional label stored alongside the history record.
        record : bool, default True
            If True, append a :class:`ReportRecord` to history.
        flush : bool, default False
            If True, force-flush the trace buffer even when the configured
            ``trace_batch_size`` has not been reached.

        Returns
        -------
        dict
            ``{"status": "ok" | "buffered" | "failed", ...}``.
        """
        normalized = self._build_trace_payload(step)

        if self._config.trace_batch_size <= 1:
            return await self._call_with_retry(
                kind="trace",
                api_method_name="send_task_update",
                payload=normalized,
                assessment_id=normalized.get("assessment_id"),
                agent_id=normalized.get("agent_id"),
                label=label,
                record=record,
            )

        # Batched mode: buffer and flush when full (or on explicit flush).
        with self._lock:
            self._trace_buffer.append(normalized)
            should_flush = (
                flush or len(self._trace_buffer) >= self._config.trace_batch_size
            )
            batch = list(self._trace_buffer) if should_flush else []
            if should_flush:
                self._trace_buffer.clear()

        if not should_flush:
            logger.debug(
                "Buffered trace update (%d/%d).",
                len(self._trace_buffer),
                self._config.trace_batch_size,
            )
            return {"status": "buffered", "buffered": len(self._trace_buffer)}

        return await self._call_with_retry(
            kind="batch_trace",
            api_method_name="send_task_update",
            payload={"type": "batch_progress", "updates": batch},
            assessment_id=None,
            agent_id=None,
            label=label,
            record=record,
        )

    async def flush_traces(self) -> Dict[str, Any]:
        """Flush any buffered trace updates immediately."""
        with self._lock:
            batch = list(self._trace_buffer)
            self._trace_buffer.clear()
        if not batch:
            return {"status": "empty"}
        return await self._call_with_retry(
            kind="batch_trace",
            api_method_name="send_task_update",
            payload={"type": "batch_progress", "updates": batch},
            assessment_id=None,
            agent_id=None,
            label="flush",
            record=True,
        )

    # -------------------------------------------------------- payload building
    def _build_result_payload(self, result: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(result, Mapping):
            raise PlatformReporterError(
                f"result must be a Mapping, got {type(result).__name__}."
            )

        missing = [k for k in self._REQUIRED_RESULT_FIELDS if k not in result]
        if missing:
            msg = f"result missing required field(s): {missing}"
            if self._strict:
                raise PlatformReporterError(msg)
            logger.warning(msg + " Defaulting them.")

        def _get(key: str, default: Any) -> Any:
            return result.get(key, default)

        assessment_id = _get("assessment_id", uuid.uuid4().hex)
        if not isinstance(assessment_id, str) or not assessment_id:
            raise PlatformReporterError("assessment_id must be a non-empty string.")

        agent_id = _get("agent_id", None)
        if not isinstance(agent_id, str) or not agent_id:
            raise PlatformReporterError("agent_id must be a non-empty string.")

        accuracy = self._validate_number(
            "accuracy", _get("accuracy", 0.0), low=0.0, high=1.0
        )
        latency = self._validate_number("latency", _get("latency", 0.0), low=0.0)
        energy = self._validate_number(
            "energy_kwh", _get("energy_kwh", 0.0), low=0.0
        )
        carbon = self._validate_number(
            "carbon_co2e_kg", _get("carbon_co2e_kg", 0.0), low=0.0
        )
        sustain = self._validate_number(
            "sustainability_index", _get("sustainability_index", 0.0), low=0.0
        )

        feedback = _get("feedback", "")
        if not isinstance(feedback, str):
            raise PlatformReporterError("feedback must be a string.")

        artifacts = _get("artifacts", [])
        if not isinstance(artifacts, (list, tuple)):
            raise PlatformReporterError("artifacts must be a list or tuple.")
        artifacts = [self._normalize_artifact(a) for a in artifacts]

        payload: Dict[str, Any] = {
            "assessment_id": assessment_id,
            "purple_agent_id": agent_id,
            "reporter_id": self._config.reporter_id,
            "metrics": {
                "accuracy": accuracy,
                "latency_ms": latency,
                "energy_kwh": energy,
                "carbon_co2e_kg": carbon,
                "sustainability_index": sustain,
            },
            "feedback": feedback,
            "artifacts": artifacts,
        }
        return payload

    def _build_trace_payload(self, step: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(step, Mapping):
            raise PlatformReporterError(
                f"step must be a Mapping, got {type(step).__name__}."
            )

        description = step.get("description")
        if not isinstance(description, str) or not description:
            raise PlatformReporterError("step.description must be a non-empty string.")

        metrics = step.get("metrics", {})
        if not isinstance(metrics, Mapping):
            raise PlatformReporterError("step.metrics must be a Mapping.")
        # Coerce metrics values to JSON-safe primitives.
        safe_metrics = {str(k): self._json_safe(v) for k, v in metrics.items()}

        payload: Dict[str, Any] = {
            "type": str(step.get("type", "progress")),
            "message": description,
            "metadata": safe_metrics,
            "reporter_id": self._config.reporter_id,
            "emitted_at": time.time(),
        }
        # Propagate optional correlation fields if provided.
        for opt in ("assessment_id", "agent_id", "task_id"):
            if opt in step and step[opt] is not None:
                payload[opt] = step[opt]
        return payload

    # ------------------------------------------------------ normalization
    @staticmethod
    def _normalize_artifact(artifact: Any) -> Dict[str, Any]:
        if isinstance(artifact, Mapping):
            return {
                "name": str(artifact.get("name", "artifact")),
                "mime_type": str(artifact.get("mime_type", "application/octet-stream")),
                "size_bytes": int(artifact.get("size_bytes", 0))
                if isinstance(artifact.get("size_bytes", 0), int)
                else 0,
                "url": str(artifact.get("url", "")),
            }
        # Bare strings treated as URLs.
        if isinstance(artifact, str):
            return {
                "name": "artifact",
                "mime_type": "application/octet-stream",
                "size_bytes": 0,
                "url": artifact,
            }
        raise PlatformReporterError(
            f"artifact must be a Mapping or str, got {type(artifact).__name__}."
        )

    @staticmethod
    def _validate_number(
        name: str, value: Any, *, low: float, high: Optional[float] = None
    ) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PlatformReporterError(
                f"{name} must be numeric, got {type(value).__name__}."
            )
        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            raise PlatformReporterError(f"{name} must be finite, got {value!r}.")
        if fvalue < low:
            raise PlatformReporterError(f"{name} must be >= {low}, got {fvalue}.")
        if high is not None and fvalue > high:
            raise PlatformReporterError(f"{name} must be <= {high}, got {fvalue}.")
        return fvalue

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, Mapping):
            return {str(k): cls._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._json_safe(v) for v in value]
        return str(value)

    # -------------------------------------------------------- retry wrapper
    async def _call_with_retry(
        self,
        *,
        kind: str,
        api_method_name: str,
        payload: Mapping[str, Any],
        assessment_id: Optional[str],
        agent_id: Optional[str],
        label: Optional[str],
        record: bool,
    ) -> Dict[str, Any]:
        cfg = self._config
        start = time.perf_counter()
        attempts = 0
        last_exc: Optional[BaseException] = None

        # Serialize once so we can estimate payload size cheaply.
        try:
            payload_size = len(json.dumps(dict(payload)))
        except (TypeError, ValueError):
            payload_size = None

        method = getattr(self.platform_api, api_method_name, None)
        if not callable(method):
            raise PlatformReporterError(
                f"platform_api is missing required method '{api_method_name}'."
            )

        while attempts <= cfg.max_retries:
            attempts += 1
            try:
                result = await asyncio.wait_for(
                    method(dict(payload)),
                    timeout=cfg.call_timeout_seconds,
                )
                duration_ms = (time.perf_counter() - start) * 1000.0
                outcome = (
                    SubmissionOutcome.OK.value if attempts == 1
                    else SubmissionOutcome.RETRIED.value
                )
                if record:
                    self._record(
                        kind=kind,
                        outcome=outcome,
                        assessment_id=assessment_id,
                        agent_id=agent_id,
                        attempts=attempts,
                        duration_ms=duration_ms,
                        error=None,
                        payload_size=payload_size,
                    )
                logger.info(
                    "Submitted %s in %d attempt(s) in %.2fms (assessment=%s).",
                    kind, attempts, duration_ms, assessment_id,
                )
                return {
                    "status": "ok",
                    "kind": kind,
                    "attempts": attempts,
                    "assessment_id": assessment_id,
                    "agent_id": agent_id,
                    "response": result,
                }
            except asyncio.TimeoutError as exc:
                last_exc = exc
                logger.warning(
                    "%s call to %s timed out after %.2fs (attempt %d/%d).",
                    kind, api_method_name, cfg.call_timeout_seconds,
                    attempts, cfg.max_retries + 1,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "%s call to %s raised %s (attempt %d/%d): %s",
                    kind, api_method_name, type(exc).__name__,
                    attempts, cfg.max_retries + 1, exc,
                )

            if attempts > cfg.max_retries:
                break
            await asyncio.sleep(self._backoff_for(attempts))

        # All attempts failed.
        duration_ms = (time.perf_counter() - start) * 1000.0
        error_repr = (
            f"{type(last_exc).__name__}: {last_exc}"
            if last_exc is not None
            else "unknown error"
        )
        if record:
            self._record(
                kind=kind,
                outcome=SubmissionOutcome.FAILED.value,
                assessment_id=assessment_id,
                agent_id=agent_id,
                attempts=attempts,
                duration_ms=duration_ms,
                error=error_repr,
                payload_size=payload_size,
            )
        logger.error(
            "Permanently failed %s after %d attempt(s): %s",
            kind, attempts, error_repr,
        )

        if self._strict:
            raise PlatformReporterError(
                f"{kind} submission failed after {attempts} attempt(s): "
                f"{error_repr}"
            ) from last_exc
        return {
            "status": "failed",
            "kind": kind,
            "attempts": attempts,
            "error": error_repr,
        }

    def _backoff_for(self, attempt: int) -> float:
        cfg = self._config
        raw = cfg.base_backoff_seconds * (2 ** (attempt - 1))
        capped = min(raw, cfg.max_backoff_seconds)
        jitter = capped * cfg.jitter_fraction * (random.random() * 2 - 1)
        return max(0.0, capped + jitter)

    # ------------------------------------------------------------ history
    def _record(
        self,
        *,
        kind: str,
        outcome: str,
        assessment_id: Optional[str],
        agent_id: Optional[str],
        attempts: int,
        duration_ms: float,
        error: Optional[str],
        payload_size: Optional[int],
    ) -> None:
        sample = ReportRecord(
            timestamp=time.time(),
            kind=kind,
            outcome=outcome,
            assessment_id=assessment_id,
            agent_id=agent_id,
            attempts=attempts,
            duration_ms=duration_ms,
            error=error,
            payload_size=payload_size,
        )
        with self._lock:
            self._history.append(sample)
            if (
                self._config.max_history is not None
                and len(self._history) > self._config.max_history
            ):
                del self._history[0]

    def statistics(self) -> Dict[str, Any]:
        """Return aggregate stats over the recorded submission history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "count": 0,
                "ok": 0,
                "retried": 0,
                "failed": 0,
                "mean_attempts": None,
                "mean_duration_ms": None,
                "success_rate": None,
            }
        ok = sum(1 for s in history if s.outcome == SubmissionOutcome.OK.value)
        retried = sum(1 for s in history if s.outcome == SubmissionOutcome.RETRIED.value)
        failed = sum(1 for s in history if s.outcome == SubmissionOutcome.FAILED.value)
        attempts = [s.attempts for s in history]
        durations = [s.duration_ms for s in history]
        return {
            "count": len(history),
            "ok": ok,
            "retried": retried,
            "failed": failed,
            "mean_attempts": sum(attempts) / len(attempts),
            "mean_duration_ms": sum(durations) / len(durations),
            "success_rate": (ok + retried) / len(history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the reporter state; optionally clear history."""
        with self._lock:
            if clear_history:
                self._history.clear()
            self._trace_buffer.clear()
        logger.debug("PlatformReporter reset (clear_history=%s)", clear_history)

    # ------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "started_at": self._started_at,
                "buffered_traces": len(self._trace_buffer),
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        platform_api: Any = None,
    ) -> "AgentBeatsPlatformReporter":
        if not isinstance(data, Mapping):
            raise PlatformReporterError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = ReporterConfig(
            max_retries=int(cfg_data.get("max_retries", 3)),
            base_backoff_seconds=float(cfg_data.get("base_backoff_seconds", 0.25)),
            max_backoff_seconds=float(cfg_data.get("max_backoff_seconds", 5.0)),
            jitter_fraction=float(cfg_data.get("jitter_fraction", 0.2)),
            call_timeout_seconds=float(cfg_data.get("call_timeout_seconds", 15.0)),
            max_history=cfg_data.get("max_history", 1000),
            default_artifact_mime=cfg_data.get(
                "default_artifact_mime", "application/octet-stream"
            ),
            trace_batch_size=int(cfg_data.get("trace_batch_size", 1)),
            reporter_id=str(cfg_data.get("reporter_id", "green-agent")),
        )
        reporter = cls(
            platform_api=platform_api,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with reporter._lock:
            reporter._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                reporter._history.append(
                    ReportRecord(
                        timestamp=float(entry["timestamp"]),
                        kind=str(entry["kind"]),
                        outcome=str(entry["outcome"]),
                        assessment_id=entry.get("assessment_id"),
                        agent_id=entry.get("agent_id"),
                        attempts=int(entry["attempts"]),
                        duration_ms=float(entry["duration_ms"]),
                        error=entry.get("error"),
                        payload_size=entry.get("payload_size"),
                    )
                )
        return reporter

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, platform_api: Any = None
    ) -> "AgentBeatsPlatformReporter":
        try:
            return cls.from_dict(json.loads(payload), platform_api=platform_api)
        except json.JSONDecodeError as exc:
            raise PlatformReporterError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    async def __aenter__(self) -> "AgentBeatsPlatformReporter":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped platform-reporting session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None

        # Best-effort flush of buffered traces on clean exit.
        if exc_type is None and self._trace_buffer:
            try:
                await self.flush_traces()
            except PlatformReporterError:
                logger.exception("Failed to flush traces on context exit.")

        if exc_type is not None:
            logger.warning(
                "Platform-reporting scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Platform-reporting scope closed in %.4fs (%d submission(s)).",
            elapsed, len(self._history),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "AgentBeatsPlatformReporter("
            f"reporter_id={self._config.reporter_id!r}, "
            f"strict={self._strict}, "
            f"submissions={len(self._history)}, "
            f"buffered_traces={len(self._trace_buffer)})"
        )


# --------------------------------------------------------------------------- #
# No-op platform API (used when no client is supplied)
# --------------------------------------------------------------------------- #
class _NoopPlatformAPI:
    """
    Silent stand-in for the real AgentBeats client.

    Useful for tests, dry runs, and local development where no live platform
    is available. Logs every call at DEBUG level so payloads can be inspected.
    """

    async def submit_result(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        logger.debug("noop submit_result: %s", payload)
        return {"accepted": True, "dry_run": True}

    async def send_task_update(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        logger.debug("noop send_task_update: %s", payload)
        return {"accepted": True, "dry_run": True}


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentBeatsPlatformReporter",
    "PlatformReporterError",
    "ReporterConfig",
    "ReportRecord",
    "SubmissionOutcome",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m agentbeats.platform_reporter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    class _FlakyAPI:
        """Fails the first 2 calls per method, then succeeds."""
        def __init__(self) -> None:
            self.calls: Dict[str, int] = {}

        async def submit_result(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
            self.calls["submit"] = self.calls.get("submit", 0) + 1
            if self.calls["submit"] < 3:
                raise ConnectionError(f"transient failure #{self.calls['submit']}")
            return {"accepted": True, "id": payload.get("assessment_id")}

        async def send_task_update(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
            self.calls["trace"] = self.calls.get("trace", 0) + 1
            return {"accepted": True, "kind": payload.get("type")}

    async def main() -> None:
        # ---- Basic flow (no-op API) ------------------------------------
        reporter = AgentBeatsPlatformReporter(
            config=ReporterConfig(max_retries=2, base_backoff_seconds=0.01)
        )

        result_payload = {
            "assessment_id": "assess-001",
            "agent_id": "purple-42",
            "accuracy": 0.93,
            "latency": 145.0,
            "energy_kwh": 0.0042,
            "carbon_co2e_kg": 0.0012,
            "sustainability_index": 0.87,
            "feedback": "Strong accuracy; energy profile acceptable.",
            "artifacts": [
                {"name": "trace.json", "mime_type": "application/json",
                 "size_bytes": 2048, "url": "s3://bucket/trace.json"},
                "s3://bucket/extra.log",
            ],
        }
        print("submit_result  :", await reporter.submit_result(result_payload))

        # ---- Trace emission -------------------------------------------
        print("emit_trace     :", await reporter.emit_trace({
            "description": "Evaluation step 1/3",
            "metrics": {"progress": 0.33, "elapsed_ms": 420},
            "assessment_id": "assess-001",
            "agent_id": "purple-42",
        }))

        # ---- Retry behaviour ------------------------------------------
        flaky = _FlakyAPI()
        retry_reporter = AgentBeatsPlatformReporter(
            platform_api=flaky,
            config=ReporterConfig(max_retries=5, base_backoff_seconds=0.01),
        )
        print("retried submit :", await retry_reporter.submit_result(result_payload))

        # ---- Batched traces -------------------------------------------
        batched = AgentBeatsPlatformReporter(
            config=ReporterConfig(trace_batch_size=3, max_retries=1)
        )
        for i in range(2):
            print("buffered       :", await batched.emit_trace({
                "description": f"step-{i}",
                "metrics": {"i": i},
            }))
        print("flushed        :", await batched.emit_trace(
            {"description": "step-2", "metrics": {"i": 2}}
        ))

        # ---- Statistics -----------------------------------------------
        print("stats          :", reporter.statistics())
        print("retry stats    :", retry_reporter.statistics())

        # ---- Serialization round-trip ---------------------------------
        payload = reporter.to_json()
        restored = AgentBeatsPlatformReporter.from_json(payload)
        assert restored.to_dict() == reporter.to_dict()
        print("Serialization round-trip OK.")

        # ---- Context manager ------------------------------------------
        async with AgentBeatsPlatformReporter(
            config=ReporterConfig(trace_batch_size=2)
        ) as scoped:
            await scoped.emit_trace({"description": "s1", "metrics": {}})
            await scoped.emit_trace({"description": "s2", "metrics": {}})
        print("Context-managed session OK.")

        # ---- Validation failures --------------------------------------
        for bad in (
            {"assessment_id": "x"},                          # many missing
            {**result_payload, "accuracy": 1.5},             # out of range
            {**result_payload, "energy_kwh": float("nan")},  # non-finite
            {**result_payload, "artifacts": 123},            # wrong type
        ):
            try:
                await reporter.submit_result(bad)
            except PlatformReporterError as exc:
                print("Rejected as expected:", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected rejection for {bad!r}")

        print("\nSmoke test passed.")

    asyncio.run(main())
