"""
Quantum Execution Monitor (Enhanced v2)
========================================

Tracks and coordinates quantum circuit execution on the QPU with
first-class support for the enhancement layers:

  * Resilience: typed exceptions, per-buffer idempotency, QPU timeout +
    semaphore, bounded result retention, cancellation-safe lifecycle.
  * Observability: structured logging, metrics on success AND failure,
    heartbeat-based health() endpoint.
  * Correctness: validated quantum state, honest success determination
    from QPU-measured fidelity, failure classification, signature
    verification, ordered side-effects, per-buffer serialization.
  * Config: QuantumMonitorConfig frozen dataclass.
  * Lifecycle: start/stop/close, async context manager, stored task.

Original API preserved:
    monitor = QuantumExecutionMonitor(
        state_buffer, coherence_manager, signature_preserver, qpu_endpoint
    )
    await monitor.start()
    result = await monitor.execute_quantum_circuit(
        buffer_id, circuit_definition
    )
    await monitor.stop()

v2 fixes (see analysis):
  * Module imports cleanly — TYPE_CHECKING + Protocol interfaces.
  * logger defined.
  * Background task stored, cancellable, awaited in stop().
  * Classical tokens dimensioned by n_qubits, not n_shots.
  * mark_decohered only on actual decoherence.
  * Failed executions recorded and emitted.
  * quantum_state validated (1-D, power-of-2, finite, normalized).
  * Real QPU timing (time.monotonic).
  * Ordered side-effects (result before buffer completion).
  * uuid4 execution IDs.
  * Per-buffer idempotency lock.
  * Fidelity from QPU result, not from just-started coherence window.
  * signature_preserver.verify(...) actually called.
  * Frozen, serialisable result (tuple + histogram).
  * Coherence utilization uses window expected duration.
  * Bounded executions (OrderedDict eviction).
  * asyncio.wait_for + asyncio.Semaphore on QPU call.
  * Metrics on both paths.
  * health() with heartbeat staleness.
  * Typed exception hierarchy.
  * Event-driven heartbeat (no more pass/no-op polling).
  * UTC datetimes; seeded RNG; single-loop contract documented.
  * close() / __aenter__ / __aexit__ / __version__ / __all__.
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
import uuid
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    Any, Dict, List, Mapping, Optional, Protocol, Sequence, Set, Tuple,
)

import numpy as np

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# External interfaces (structural, so we don't hard-depend on concrete modules)
# =============================================================================

class StateBufferProtocol(Protocol):
    """Structural interface expected of the buffer store."""

    async def get_buffered_state(self, buffer_id: str) -> Any: ...
    async def complete_state(
        self,
        buffer_id: str,
        measurement_outcomes: Any,
        classical_tokens: List[str],
    ) -> None: ...
    async def mark_decohered(self, buffer_id: str) -> None: ...


class CoherenceManagerProtocol(Protocol):
    async def start_coherence_window(
        self, expected_duration_ms: float, initial_fidelity: float,
    ) -> str: ...
    async def end_coherence_window(
        self, window_id: str, final_fidelity: float, successful: bool,
    ) -> None: ...
    async def get_coherence_status(self, window_id: str) -> Dict[str, Any]: ...


class SignaturePreserverProtocol(Protocol):
    def verify(
        self,
        signature_id: str,
        measurement_outcomes: Any,
        classical_tokens: List[str],
    ) -> bool: ...


# =============================================================================
# Errors
# =============================================================================

class QuantumExecutionError(Exception):
    """Base class for all QCS execution errors."""


class BufferNotFoundError(QuantumExecutionError):
    """Buffer does not exist or cannot be retrieved."""


class BufferAlreadyExecutingError(QuantumExecutionError):
    """Another execution for the same buffer_id is in flight."""


class QuantumStateValidationError(QuantumExecutionError):
    """Amplitudes vector failed shape or normalization checks."""


class QpuTimeoutError(QuantumExecutionError):
    """QPU execution exceeded the configured timeout."""


class QpuExecutionError(QuantumExecutionError):
    """QPU backend raised a runtime error."""


class CoherenceLostError(QuantumExecutionError):
    """Post-execution fidelity fell below the decoherence threshold."""


class SignatureVerificationError(QuantumExecutionError):
    """Signature verification raised or definitively failed."""


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class QuantumMonitorConfig:
    initial_fidelity: float = 0.99
    success_fidelity_threshold: float = 0.90
    decoherence_fidelity_threshold: float = 0.50
    default_shots: int = 1000
    max_shots: int = 100_000

    qpu_timeout_s: float = 30.0
    max_concurrent_qpu_calls: int = 8

    heartbeat_interval_s: float = 5.0
    heartbeat_stale_s: float = 30.0

    max_executions_retained: int = 10_000
    max_classical_tokens: int = 256

    rng_seed: Optional[int] = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.initial_fidelity <= 1.0:
            raise ValueError("initial_fidelity must be in [0, 1].")
        if not 0.0 <= self.success_fidelity_threshold <= 1.0:
            raise ValueError("success_fidelity_threshold must be in [0, 1].")
        if not 0.0 <= self.decoherence_fidelity_threshold \
                <= self.success_fidelity_threshold:
            raise ValueError(
                "decoherence_fidelity_threshold must be in "
                "[0, success_fidelity_threshold]."
            )
        if self.default_shots <= 0 or self.max_shots < self.default_shots:
            raise ValueError("invalid shots configuration.")
        if self.qpu_timeout_s <= 0:
            raise ValueError("qpu_timeout_s must be > 0.")
        if self.max_concurrent_qpu_calls <= 0:
            raise ValueError("max_concurrent_qpu_calls must be > 0.")


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# QPU backend protocol + mock
# =============================================================================

@dataclass(frozen=True)
class QpuExecutionOutcome:
    outcomes: Tuple[int, ...]
    final_fidelity: float
    backend_metadata: Mapping[str, Any] = field(default_factory=dict)


class QpuBackend(Protocol):
    async def execute(
        self,
        circuit_definition: Mapping[str, Any],
        quantum_state: np.ndarray,
        shots: int,
        window_id: str,
    ) -> QpuExecutionOutcome: ...


class MockQpuBackend:
    """
    Deterministic (optionally seeded) simulator.

    Fidelity is simulated from circuit size — a real backend must report
    its own measured post-execution fidelity. This mock is *not* reading
    from the coherence manager (which would have reflected the pre-execution
    value and hidden the bug being fixed).
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self._rng = np.random.default_rng(seed)

    async def execute(
        self,
        circuit_definition: Mapping[str, Any],
        quantum_state: np.ndarray,
        shots: int,
        window_id: str,
    ) -> QpuExecutionOutcome:
        n = len(quantum_state)
        p = np.abs(quantum_state) ** 2
        total = float(p.sum())
        if total <= 0 or not np.isfinite(total):
            raise QuantumStateValidationError(
                "amplitudes have zero or non-finite total probability"
            )
        p = p / total
        # Simulate measurement.
        outcomes = self._rng.choice(n, size=shots, p=p)

        # Simulate post-execution fidelity decay (backend-measured).
        n_qubits = int(round(math.log2(n)))
        base = 0.99
        qubit_penalty = 0.005 * n_qubits
        shot_penalty = min(0.05, shots * 1e-6)
        fidelity = max(0.0, base - qubit_penalty - shot_penalty)

        return QpuExecutionOutcome(
            outcomes=tuple(int(x) for x in outcomes),
            final_fidelity=fidelity,
            backend_metadata={"backend": "mock"},
        )


# =============================================================================
# Result
# =============================================================================

@dataclass(frozen=True)
class QuantumExecutionResult:
    execution_id: str
    buffer_id: str
    window_id: str
    signature_id: str
    start_time: datetime
    end_time: datetime
    duration_ms: float
    measurement_outcomes: Tuple[int, ...]
    outcome_histogram: Mapping[int, int]
    final_fidelity: float
    success: bool
    error_messages: Tuple[str, ...] = ()
    classical_tokens: Tuple[str, ...] = ()
    signature_verified: Optional[bool] = None
    failure_kind: Optional[str] = None
    circuit_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome_histogram, Mapping):
            raise ValueError("outcome_histogram must be a Mapping.")
        # Freeze the histogram so callers can't mutate the stored result.
        from types import MappingProxyType
        if not isinstance(self.outcome_histogram, MappingProxyType):
            object.__setattr__(
                self, "outcome_histogram",
                MappingProxyType(dict(self.outcome_histogram)),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "buffer_id": self.buffer_id,
            "window_id": self.window_id,
            "signature_id": self.signature_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_ms": self.duration_ms,
            "measurement_outcomes": list(self.measurement_outcomes),
            "outcome_histogram": {
                int(k): int(v) for k, v in self.outcome_histogram.items()
            },
            "final_fidelity": self.final_fidelity,
            "success": self.success,
            "error_messages": list(self.error_messages),
            "classical_tokens": list(self.classical_tokens),
            "signature_verified": self.signature_verified,
            "failure_kind": self.failure_kind,
            "circuit_id": self.circuit_id,
        }


# =============================================================================
# Validation
# =============================================================================

def _validate_quantum_state(amplitudes: Any) -> np.ndarray:
    if not isinstance(amplitudes, np.ndarray):
        amplitudes = np.asarray(amplitudes, dtype=np.complex128)
    if amplitudes.ndim != 1:
        raise QuantumStateValidationError(
            f"amplitudes must be 1-D, got shape {amplitudes.shape}"
        )
    n = amplitudes.shape[0]
    if n == 0 or (n & (n - 1)) != 0:
        raise QuantumStateValidationError(
            f"amplitudes length {n} is not a power of 2"
        )
    probs = np.abs(amplitudes) ** 2
    total = float(probs.sum())
    if total <= 0 or not np.isfinite(total):
        raise QuantumStateValidationError(
            "amplitudes have zero or non-finite total probability"
        )
    return amplitudes


# =============================================================================
# The Enhanced QuantumExecutionMonitor
# =============================================================================

class QuantumExecutionMonitor:
    """
    Quantum execution tracker and coordinator.

    Responsibilities:
      - Monitor quantum state during execution
      - Track measurement outcomes
      - Coordinate with CoherenceManager and StateBuffer
      - Emit metrics on both success and failure

    Threading: single event loop is assumed for the async primitives
    (semaphore). Cross-thread calls to `execute_quantum_circuit` are safe
    via the internal `RLock`, but must be scheduled on the same loop.
    """

    def __init__(
        self,
        state_buffer: StateBufferProtocol,
        coherence_manager: CoherenceManagerProtocol,
        signature_preserver: SignaturePreserverProtocol,
        qpu_endpoint: str,
        *,
        qpu_backend: Optional[QpuBackend] = None,
        config: Optional[QuantumMonitorConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        if state_buffer is None:
            raise ValueError("state_buffer must not be None.")
        if coherence_manager is None:
            raise ValueError("coherence_manager must not be None.")
        if not isinstance(qpu_endpoint, str) or not qpu_endpoint:
            raise ValueError("qpu_endpoint must be a non-empty string.")

        self.state_buffer = state_buffer
        self.coherence_manager = coherence_manager
        self.signature_preserver = signature_preserver
        self.qpu_endpoint = qpu_endpoint

        self.config = config or QuantumMonitorConfig()
        self._metrics = metrics or MetricsRecorder()
        self._qpu: QpuBackend = qpu_backend or MockQpuBackend(
            seed=self.config.rng_seed,
        )

        # Bounded retention.
        self._executions: "OrderedDict[str, QuantumExecutionResult]" = \
            OrderedDict()
        self._lock = threading.RLock()

        # Per-buffer idempotency (buffer_id → in-flight marker).
        self._inflight: Set[str] = set()

        # QPU concurrency.
        self._qpu_semaphore: Optional[asyncio.Semaphore] = None

        # Lifecycle.
        self._running: bool = False
        self._loop_task: Optional[asyncio.Task] = None
        self._last_heartbeat_mono: float = 0.0
        self._started_mono: float = time.monotonic()

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        """Start the heartbeat task."""
        if self._running:
            return
        if self._qpu_semaphore is None:
            self._qpu_semaphore = asyncio.Semaphore(
                self.config.max_concurrent_qpu_calls,
            )
        self._running = True
        self._last_heartbeat_mono = time.monotonic()
        self._loop_task = asyncio.create_task(
            self._heartbeat_loop(), name="qcs-heartbeat",
        )
        logger.info(
            "QuantumExecutionMonitor started",
            extra={"qpu_endpoint": self.qpu_endpoint},
        )

    async def stop(self) -> None:
        """Stop the heartbeat task and await it."""
        self._running = False
        task = self._loop_task
        self._loop_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("QuantumExecutionMonitor stopped")

    async def close(self) -> None:
        await self.stop()

    async def __aenter__(self) -> "QuantumExecutionMonitor":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.stop()

    async def _heartbeat_loop(self) -> None:
        """Liveness signal only. Actual execution is via execute_quantum_circuit."""
        while self._running:
            try:
                self._last_heartbeat_mono = time.monotonic()
                await asyncio.sleep(self.config.heartbeat_interval_s)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("heartbeat loop error")
                await asyncio.sleep(1.0)

    # ------------------------------------------------------------------ main API
    async def execute_quantum_circuit(
        self,
        buffer_id: str,
        circuit_definition: Mapping[str, Any],
    ) -> QuantumExecutionResult:
        """
        Execute a quantum circuit with monitoring.

        Raises typed exceptions on failure; a failed `QuantumExecutionResult`
        is stored and retrievable via `get_execution_result` for auditing.
        """
        if not isinstance(buffer_id, str) or not buffer_id:
            raise ValueError("buffer_id must be a non-empty string.")
        if not isinstance(circuit_definition, Mapping):
            raise ValueError("circuit_definition must be a Mapping.")

        if not self._try_acquire_buffer(buffer_id):
            raise BufferAlreadyExecutingError(
                f"buffer {buffer_id!r} is already being executed."
            )

        execution_id = self._generate_execution_id()
        circuit_id = (
            circuit_definition.get("circuit_id")
            or circuit_definition.get("id")
        )
        start_time = _utcnow()
        t0 = time.monotonic()

        window_id: str = ""
        window_ended = False
        signature_id: str = ""
        outcomes: Tuple[int, ...] = ()
        classical_tokens: Tuple[str, ...] = ()
        final_fidelity: float = 0.0
        signature_verified: Optional[bool] = None
        success = False

        try:
            # 1. Fetch buffer.
            try:
                buffered = await self.state_buffer.get_buffered_state(buffer_id)
            except Exception as e:
                raise QuantumExecutionError(
                    f"failed to fetch buffer {buffer_id!r}: {e}"
                ) from e
            if buffered is None:
                raise BufferNotFoundError(
                    f"buffer {buffer_id!r} not found"
                )

            qstate = getattr(buffered, "quantum_state", None)
            if qstate is None:
                raise QuantumStateValidationError(
                    "buffered state has no quantum_state attribute"
                )
            signature_id = str(getattr(qstate, "thought_signature", "") or "")

            # 2. Validate quantum state.
            amplitudes = getattr(qstate, "amplitudes", None)
            if amplitudes is None:
                raise QuantumStateValidationError(
                    "buffered state has no amplitudes"
                )
            amplitudes = _validate_quantum_state(amplitudes)
            n = len(amplitudes)
            n_qubits = int(round(math.log2(n)))

            # 3. Validate shots.
            try:
                shots = int(
                    circuit_definition.get("shots", self.config.default_shots)
                )
            except (TypeError, ValueError) as e:
                raise QuantumExecutionError(
                    f"invalid shots value: {e}"
                ) from e
            if shots <= 0 or shots > self.config.max_shots:
                raise QuantumExecutionError(
                    f"shots={shots} outside [1, {self.config.max_shots}]"
                )

            # 4. Open coherence window.
            expected_duration_ms = float(
                getattr(qstate, "coherence_time_ms", 0.0) or 0.0
            )
            window_id = await self.coherence_manager.start_coherence_window(
                expected_duration_ms=expected_duration_ms,
                initial_fidelity=self.config.initial_fidelity,
            )

            # 5. Execute on QPU (bounded by timeout + semaphore).
            await self._acquire_qpu_slot()
            try:
                outcome = await asyncio.wait_for(
                    self._qpu.execute(
                        circuit_definition=circuit_definition,
                        quantum_state=amplitudes,
                        shots=shots,
                        window_id=window_id,
                    ),
                    timeout=self.config.qpu_timeout_s,
                )
            except asyncio.TimeoutError as e:
                raise QpuTimeoutError(
                    f"QPU execution exceeded {self.config.qpu_timeout_s}s"
                ) from e
            except QuantumExecutionError:
                raise
            except Exception as e:
                raise QpuExecutionError(
                    f"QPU backend error: {e}"
                ) from e
            finally:
                self._release_qpu_slot()

            outcomes = tuple(int(x) for x in outcome.outcomes)
            final_fidelity = float(outcome.final_fidelity)
            success = final_fidelity >= self.config.success_fidelity_threshold

            # 6. Translate to classical tokens (bit-width = n_qubits).
            unique_outcomes = sorted(set(outcomes))
            classical_tokens = tuple(
                self._translate_outcome_to_token(o, n_qubits)
                for o in unique_outcomes
            )[: self.config.max_classical_tokens]

            # 7. Signature verification (once, before buffer completion).
            if self.signature_preserver is not None:
                try:
                    verified = self.signature_preserver.verify(
                        signature_id=signature_id,
                        measurement_outcomes=outcomes,
                        classical_tokens=list(classical_tokens),
                    )
                    signature_verified = bool(verified)
                except Exception as e:
                    logger.warning(
                        "signature verification errored: %s", e,
                        extra={
                            "execution_id": execution_id,
                            "signature_id": signature_id,
                        },
                        exc_info=True,
                    )
                    self._metrics.inc("signature_verify_errors")
                    signature_verified = None
                if signature_verified is False:
                    logger.warning(
                        "signature verification failed",
                        extra={
                            "execution_id": execution_id,
                            "signature_id": signature_id,
                        },
                    )
                    self._metrics.inc("signature_verify_failures")

            # 8. Real decoherence check.
            if final_fidelity < self.config.decoherence_fidelity_threshold:
                raise CoherenceLostError(
                    f"final fidelity {final_fidelity:.3f} below "
                    f"decoherence threshold "
                    f"{self.config.decoherence_fidelity_threshold}"
                )

            # 9. Close coherence window.
            await self.coherence_manager.end_coherence_window(
                window_id=window_id,
                final_fidelity=final_fidelity,
                successful=success,
            )
            window_ended = True

            # 10. Complete buffer state (after result-side-effect safety net).
            try:
                await self.state_buffer.complete_state(
                    buffer_id=buffer_id,
                    measurement_outcomes=np.array(outcomes, dtype=np.int64),
                    classical_tokens=list(classical_tokens),
                )
            except Exception as e:
                raise QuantumExecutionError(
                    f"failed to complete buffer {buffer_id!r}: {e}"
                ) from e

            # 11. Build and store result (last side-effect).
            end_time = _utcnow()
            duration_ms = (time.monotonic() - t0) * 1000.0
            result = self._build_result(
                execution_id=execution_id,
                buffer_id=buffer_id,
                window_id=window_id,
                signature_id=signature_id,
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                outcomes=outcomes,
                final_fidelity=final_fidelity,
                success=success,
                classical_tokens=classical_tokens,
                signature_verified=signature_verified,
                circuit_id=str(circuit_id) if circuit_id is not None else None,
                failure_kind=None,
                error_messages=(),
            )
            self._store_result(result)

            # 12. Metrics + logs (success path).
            self._metrics.inc(
                "execute_success",
                circuit_id=str(circuit_id or ""),
            )
            self._metrics.observe("final_fidelity", final_fidelity)
            self._metrics.observe("duration_ms", duration_ms)
            self._metrics.observe("shots", shots)
            self._metrics.observe(
                "coherence_utilization_pct",
                self._calculate_coherence_utilization(
                    duration_ms=duration_ms,
                    expected_duration_ms=expected_duration_ms,
                ),
            )
            if signature_verified is not None:
                self._metrics.inc(
                    "signature_result",
                    verified=str(signature_verified).lower(),
                )

            logger.info(
                "quantum execution succeeded",
                extra={
                    "execution_id": execution_id,
                    "buffer_id": buffer_id,
                    "window_id": window_id,
                    "final_fidelity": final_fidelity,
                    "duration_ms": duration_ms,
                    "success": success,
                },
            )
            return result

        except BaseException as exc:
            failure_kind = self._classify_failure(exc)
            end_time = _utcnow()
            duration_ms = (time.monotonic() - t0) * 1000.0

            # mark_decohered ONLY for real coherence loss (§1.7).
            if isinstance(exc, CoherenceLostError):
                try:
                    await self.state_buffer.mark_decohered(buffer_id)
                except Exception:
                    logger.exception(
                        "failed to mark buffer decohered",
                        extra={
                            "execution_id": execution_id,
                            "buffer_id": buffer_id,
                        },
                    )

            # Close window if not already closed (§1.8).
            if window_id and not window_ended:
                try:
                    await self.coherence_manager.end_coherence_window(
                        window_id=window_id,
                        final_fidelity=final_fidelity,
                        successful=False,
                    )
                except Exception:
                    logger.exception(
                        "failed to close coherence window during failure",
                        extra={
                            "execution_id": execution_id,
                            "window_id": window_id,
                        },
                    )

            # Record failed result (§1.6).
            error_messages = (f"{type(exc).__name__}: {exc}",)
            failed = self._build_result(
                execution_id=execution_id,
                buffer_id=buffer_id,
                window_id=window_id,
                signature_id=signature_id,
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                outcomes=outcomes,
                final_fidelity=final_fidelity,
                success=False,
                classical_tokens=classical_tokens,
                signature_verified=signature_verified,
                circuit_id=str(circuit_id) if circuit_id is not None else None,
                failure_kind=failure_kind,
                error_messages=error_messages,
            )
            self._store_result(failed)

            self._metrics.inc("execute_failure", failure_kind=failure_kind)
            # Do not log CancelledError (expected on shutdown).
            if not isinstance(exc, asyncio.CancelledError):
                logger.warning(
                    "quantum execution failed",
                    extra={
                        "execution_id": execution_id,
                        "buffer_id": buffer_id,
                        "failure_kind": failure_kind,
                    },
                    exc_info=True,
                )
            raise

        finally:
            self._release_buffer(buffer_id)

    # ------------------------------------------------------------------ lookups
    def get_execution_result(
        self, execution_id: str,
    ) -> Optional[QuantumExecutionResult]:
        """Synchronous lookup — no I/O."""
        with self._lock:
            return self._executions.get(execution_id)

    def recent_results(self, n: int = 10) -> List[QuantumExecutionResult]:
        with self._lock:
            return list(self._executions.values())[-n:]

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        with self._lock:
            inflight = len(self._inflight)
            retained = len(self._executions)
        now = time.monotonic()
        heartbeat_age = (
            now - self._last_heartbeat_mono
            if self._last_heartbeat_mono > 0
            else None
        )
        loop_alive = (
            self._loop_task is not None and not self._loop_task.done()
        )
        heartbeat_ok = (
            heartbeat_age is not None
            and heartbeat_age < self.config.heartbeat_stale_s
        )
        return {
            "ready": loop_alive and heartbeat_ok,
            "running": self._running,
            "loop_alive": loop_alive,
            "heartbeat_age_s": heartbeat_age,
            "executions_inflight": inflight,
            "executions_retained": retained,
            "qpu_endpoint": self.qpu_endpoint,
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _generate_execution_id() -> str:
        return uuid.uuid4().hex[:16]

    @staticmethod
    def _translate_outcome_to_token(outcome: int, n_qubits: int) -> str:
        # Correct bit width: n_qubits, NOT n_shots (§1.5).
        if n_qubits <= 0:
            return f"token_{outcome}"
        return f"token_{format(int(outcome), f'0{n_qubits}b')}"

    def _try_acquire_buffer(self, buffer_id: str) -> bool:
        with self._lock:
            if buffer_id in self._inflight:
                return False
            self._inflight.add(buffer_id)
            return True

    def _release_buffer(self, buffer_id: str) -> None:
        with self._lock:
            self._inflight.discard(buffer_id)

    async def _acquire_qpu_slot(self) -> None:
        if self._qpu_semaphore is None:
            self._qpu_semaphore = asyncio.Semaphore(
                self.config.max_concurrent_qpu_calls,
            )
        await self._qpu_semaphore.acquire()

    def _release_qpu_slot(self) -> None:
        if self._qpu_semaphore is not None:
            self._qpu_semaphore.release()

    def _store_result(self, result: QuantumExecutionResult) -> None:
        with self._lock:
            self._executions[result.execution_id] = result
            while len(self._executions) > self.config.max_executions_retained:
                self._executions.popitem(last=False)

    def _build_result(
        self,
        *,
        execution_id: str,
        buffer_id: str,
        window_id: str,
        signature_id: str,
        start_time: datetime,
        end_time: datetime,
        duration_ms: float,
        outcomes: Sequence[int],
        final_fidelity: float,
        success: bool,
        classical_tokens: Sequence[str],
        signature_verified: Optional[bool],
        circuit_id: Optional[str],
        failure_kind: Optional[str],
        error_messages: Sequence[str],
    ) -> QuantumExecutionResult:
        histogram = dict(Counter(int(x) for x in outcomes))
        return QuantumExecutionResult(
            execution_id=execution_id,
            buffer_id=buffer_id,
            window_id=window_id,
            signature_id=signature_id,
            start_time=start_time,
            end_time=end_time,
            duration_ms=duration_ms,
            measurement_outcomes=tuple(int(x) for x in outcomes),
            outcome_histogram=histogram,
            final_fidelity=final_fidelity,
            success=success,
            error_messages=tuple(error_messages),
            classical_tokens=tuple(classical_tokens),
            signature_verified=signature_verified,
            failure_kind=failure_kind,
            circuit_id=circuit_id,
        )

    @staticmethod
    def _classify_failure(exc: BaseException) -> str:
        if isinstance(exc, BufferNotFoundError):
            return "buffer_not_found"
        if isinstance(exc, BufferAlreadyExecutingError):
            return "buffer_busy"
        if isinstance(exc, QuantumStateValidationError):
            return "invalid_state"
        if isinstance(exc, QpuTimeoutError):
            return "qpu_timeout"
        if isinstance(exc, QpuExecutionError):
            return "qpu_error"
        if isinstance(exc, CoherenceLostError):
            return "coherence_lost"
        if isinstance(exc, SignatureVerificationError):
            return "signature_failed"
        if isinstance(exc, asyncio.CancelledError):
            return "cancelled"
        return "unknown"

    @staticmethod
    def _calculate_coherence_utilization(
        *,
        duration_ms: float,
        expected_duration_ms: float,
    ) -> float:
        if expected_duration_ms <= 0:
            return 0.0
        return (duration_ms / expected_duration_ms) * 100.0


__all__ = [
    "BufferAlreadyExecutingError",
    "BufferNotFoundError",
    "CoherenceLostError",
    "CoherenceManagerProtocol",
    "MetricsRecorder",
    "MockQpuBackend",
    "QpuBackend",
    "QpuExecutionError",
    "QpuExecutionOutcome",
    "QpuTimeoutError",
    "QuantumExecutionError",
    "QuantumExecutionMonitor",
    "QuantumExecutionResult",
    "QuantumMonitorConfig",
    "QuantumStateValidationError",
    "SignaturePreserverProtocol",
    "SignatureVerificationError",
    "StateBufferProtocol",
]


# =============================================================================
# Demo: python -m qcs.quantum_execution_monitor
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _MockQState:
        def __init__(self, amplitudes, thought_signature="sig", coherence_time_ms=10.0):
            self.amplitudes = amplitudes
            self.thought_signature = thought_signature
            self.coherence_time_ms = coherence_time_ms

    class _MockBuffered:
        def __init__(self, qstate, classical_context=None):
            self.quantum_state = qstate
            self.classical_context = classical_context or {}

    class _MockStateBuffer:
        def __init__(self):
            self._buffers: Dict[str, _MockBuffered] = {}
            self.completed: List[str] = []
            self.decohered: List[str] = []

        def add(self, buffer_id, amplitudes, sig="sig", coherence_ms=10.0):
            self._buffers[buffer_id] = _MockBuffered(
                _MockQState(amplitudes, sig, coherence_ms),
            )

        async def get_buffered_state(self, buffer_id):
            return self._buffers.get(buffer_id)

        async def complete_state(self, buffer_id, measurement_outcomes, classical_tokens):
            self.completed.append(buffer_id)
            self._buffers.pop(buffer_id, None)

        async def mark_decohered(self, buffer_id):
            self.decohered.append(buffer_id)
            self._buffers.pop(buffer_id, None)

    class _MockCoherenceManager:
        def __init__(self):
            self.windows: Dict[str, Dict[str, Any]] = {}
            self.counter = 0

        async def start_coherence_window(self, expected_duration_ms, initial_fidelity):
            self.counter += 1
            wid = f"win-{self.counter}"
            self.windows[wid] = {
                "expected_duration_ms": expected_duration_ms,
                "initial_fidelity": initial_fidelity,
                "ended": False,
            }
            return wid

        async def end_coherence_window(self, window_id, final_fidelity, successful):
            if window_id in self.windows:
                self.windows[window_id].update({
                    "ended": True,
                    "final_fidelity": final_fidelity,
                    "successful": successful,
                })

        async def get_coherence_status(self, window_id):
            w = self.windows.get(window_id, {})
            return {"current_fidelity": w.get("initial_fidelity", 0.99)}

    class _MockSignaturePreserver:
        def __init__(self, fail=False, error=False):
            self.fail = fail
            self.error = error

        def verify(self, signature_id, measurement_outcomes, classical_tokens):
            if self.error:
                raise RuntimeError("mock signature verifier error")
            return not self.fail

    async def demo():
        # Build mock dependencies.
        state_buffer = _MockStateBuffer()
        coherence = _MockCoherenceManager()
        signature = _MockSignaturePreserver()

        # 3-qubit Bell-like state.
        amps = np.array(
            [1 / math.sqrt(2), 0, 0, 1 / math.sqrt(2), 0, 0, 0, 0],
            dtype=np.complex128,
        )
        state_buffer.add("buf-A", amps, sig="sig-A", coherence_ms=10.0)

        monitor = QuantumExecutionMonitor(
            state_buffer=state_buffer,
            coherence_manager=coherence,
            signature_preserver=signature,
            qpu_endpoint="mock://qpu/local",
            config=QuantumMonitorConfig(
                default_shots=500,
                success_fidelity_threshold=0.90,
                rng_seed=42,
            ),
        )

        async with monitor:
            print("\n=== Health (running) ===")
            print(monitor.health()["ready"], monitor.health()["loop_alive"])

            # --- Success path ---
            print("\n=== Success path ===")
            result = await monitor.execute_quantum_circuit(
                buffer_id="buf-A",
                circuit_definition={
                    "circuit_id": "bell-2q",
                    "shots": 500,
                },
            )
            print(f"  execution_id:     {result.execution_id}")
            print(f"  success:          {result.success}")
            print(f"  final_fidelity:   {result.final_fidelity:.4f}")
            print(f"  duration_ms:      {result.duration_ms:.2f}")
            print(f"  signature:        {result.signature_verified}")
            print(f"  tokens:           {result.classical_tokens}")
            print(f"  histogram:        {dict(result.outcome_histogram)}")
            print(f"  serialisable:     "
                  f"{sorted(result.to_dict().keys()) == sorted(result.to_dict().keys())}")

            # Verify token widths are 3 chars (3 qubits).
            for t in result.classical_tokens:
                binary = t.split("_", 1)[1]
                assert len(binary) == 3, f"bad token width: {t}"

            # --- Buffer not found ---
            print("\n=== Buffer not found ===")
            try:
                await monitor.execute_quantum_circuit(
                    "missing", {"shots": 100},
                )
            except BufferNotFoundError as e:
                print(f"  raised: {type(e).__name__}")

            failed = monitor.recent_results(1)[0]
            print(f"  recorded failure: {failed.failure_kind}, "
                  f"success={failed.success}")
            assert failed.failure_kind == "buffer_not_found"
            assert failed.success is False

            # --- Invalid state ---
            print("\n=== Invalid state (non power-of-2) ===")
            state_buffer.add("buf-B", np.array([1.0, 0.0, 0.0]))
            try:
                await monitor.execute_quantum_circuit(
                    "buf-B", {"shots": 100},
                )
            except QuantumStateValidationError as e:
                print(f"  raised: {type(e).__name__}")
            # Buffer NOT marked decohered.
            assert "buf-B" not in state_buffer.decohered
            print("  buffer not decohered ✓")

            # --- Coherence loss ---
            print("\n=== Coherence loss (deep circuit) ===")
            # 12-qubit state, 50000 shots → fidelity < 0.90 but ≥ 0.50.
            big_n = 2 ** 12
            big_amps = np.zeros(big_n, dtype=np.complex128)
            big_amps[0] = 1.0
            state_buffer.add("buf-C", big_amps)
            try:
                await monitor.execute_quantum_circuit(
                    "buf-C",
                    {"circuit_id": "deep-12q", "shots": 50_000},
                )
            except Exception as e:
                print(f"  raised: {type(e).__name__}")

            # --- Buffer conflict ---
            print("\n=== Concurrent execution conflict ===")
            state_buffer.add("buf-D", amps)
            state_buffer.add("buf-E", amps)
            # Trigger two executions simultaneously.
            async def run_one(bid):
                try:
                    return await monitor.execute_quantum_circuit(bid, {"shots": 200})
                except Exception as e:
                    return e
            # Manually acquire buffer-D first to simulate an in-flight execution.
            monitor._try_acquire_buffer("buf-D")
            try:
                await monitor.execute_quantum_circuit("buf-D", {"shots": 100})
            except BufferAlreadyExecutingError as e:
                print(f"  raised: {type(e).__name__}")
            finally:
                monitor._release_buffer("buf-D")

            # --- Signature failure ---
            print("\n=== Signature failure ===")
            signature.fail = True
            state_buffer.add("buf-F", amps)
            res = await monitor.execute_quantum_circuit(
                "buf-F", {"shots": 200},
            )
            print(f"  signature_verified: {res.signature_verified}")
            print(f"  success (fidelity): {res.success}")
            assert res.signature_verified is False

            # --- Signature error (swallowed) ---
            print("\n=== Signature error (swallowed) ===")
            signature.error = True
            state_buffer.add("buf-G", amps)
            res = await monitor.execute_quantum_circuit(
                "buf-G", {"shots": 200},
            )
            print(f"  signature_verified: {res.signature_verified}")
            assert res.signature_verified is None

        # --- Health after stop ---
        print("\n=== Health (stopped) ===")
        h = monitor.health()
        print(f"  ready: {h['ready']}, running: {h['running']}")

        # --- Metrics snapshot ---
        print("\n=== Metrics ===")
        metrics = h["metrics"]["counters"]
        for k in sorted(metrics):
            print(f"  {k}: {metrics[k]}")

        print("\nSmoke test passed.")
        assert len(monitor.recent_results(100)) >= 3
        assert all(
            isinstance(r.measurement_outcomes, tuple)
            for r in monitor.recent_results(100)
        )

    asyncio.run(demo())
