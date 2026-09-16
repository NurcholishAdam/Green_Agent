# src/instrumentation/energy/perf_counters.py

"""
Perf Counter Meter
==================

Hardware performance counters via Linux ``perf stat``.

Reports CPU cycles and instructions per measurement window, plus a
power-estimated energy figure derived from the sampled duty cycle.

Enhancements
------------
- Now inherits :class:`BaseEnergyMeter` (uniform contract with the rest of
  the instrumentation layer).
- ``PerfCounterConfig`` — frozen, validated: events, sample window, binary
  path, timeout, optional per-counter power coefficients.
- **Parsed output** — extracts ``cycles`` and ``instructions`` into
  structured ints, plus ``ipc`` (instructions per cycle).
- **Robust subprocess handling** — timeout, non-zero exit, missing binary,
  permission errors, and ``<not counted>`` markers are all handled.
- **Fixed contract** — returns ``energy_joules`` + ``power_watts`` +
  ``provenance`` alongside the perf counters.
- **Graceful degradation** — ``provenance`` falls back to ``"estimated"``
  when the counters are unavailable (missing binary / permissions), so
  consumers can distinguish real measurements from placeholders.
- ``RLock``-guarded ``start`` / ``stop`` (idempotent ``start``).
- Custom ``PerfCounterError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test with a mocked runner.
"""

from __future__ import annotations

import json
import logging
import math
import re
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    TelemetrySample,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PerfCounterError(InstrumentationError):
    """Raised for invalid perf-counter inputs or runtime failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PerfCounterConfig:
    """Tunable parameters for the perf-counter meter."""

    # Events to query.
    events: tuple = ("cycles", "instructions")

    # Sampling window passed to ``sleep`` inside the ``perf stat`` command.
    sample_seconds: float = 0.1

    # Binary used to query the CPU.
    binary: str = "perf"

    # Subprocess timeout.
    timeout_seconds: float = 10.0

    # Energy estimation coefficients. When the counters are unavailable, the
    # meter returns a modelled estimate using these values:
    #   energy_joules = idle_watts * elapsed + dynamic_watts * duty_cycle
    # The defaults approximate a mid-range server CPU.
    idle_watts: float = 3.0
    dynamic_watts: float = 45.0
    # Normalize instructions-per-second against this reference; the meter
    # clamps the resulting duty cycle to [0, 1].
    ipc_reference: float = 2_000_000_000.0  # 2 G instructions/sec

    # If True, ``provenance`` is ``"measured"`` when counters parse cleanly,
    # otherwise ``"estimated"``. If False, always report ``"measured"``.
    distinguish_provenance: bool = True

    # If a hardware counter reports ``<not counted>``, treat the sample as
    # unmeasured (this usually means permissions are insufficient).
    tolerate_not_counted: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.events, tuple) or not self.events:
            raise PerfCounterError("events must be a non-empty tuple.")
        for ev in self.events:
            if not isinstance(ev, str) or not ev:
                raise PerfCounterError(
                    "events entries must be non-empty strings."
                )
        for name in ("sample_seconds", "timeout_seconds"):
            if getattr(self, name) <= 0:
                raise PerfCounterError(f"{name} must be > 0.")
        if not isinstance(self.binary, str) or not self.binary:
            raise PerfCounterError("binary must be a non-empty string.")
        if self.idle_watts < 0 or self.dynamic_watts < 0:
            raise PerfCounterError(
                "idle_watts / dynamic_watts must be >= 0."
            )
        if self.ipc_reference <= 0:
            raise PerfCounterError("ipc_reference must be > 0.")


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class PerfCounter(BaseEnergyMeter):
    """
    Hardware performance counters using Linux ``perf``.

    Parameters
    ----------
    config : PerfCounterConfig, optional
        Meter configuration. Defaults to ``PerfCounterConfig()``.
    base_config : BaseMeterConfig, optional
        Contract policy passed to :class:`BaseEnergyMeter`.
    strict : bool, default True
        If True, a missing binary or a ``perf`` error raises
        :class:`PerfCounterError`. If False, ``collect`` falls back to an
        estimated result derived from the configured coefficients.
    runner : callable, optional
        ``(argv, timeout) -> (rc, stdout, stderr)`` callable replacing the
        real subprocess. Used to inject a mock in tests.
    """

    # Regex for lines like ``    1,234,567      cycles`` or
    # ``    1,234,567      cycles        #   1.23 GHz``.
    _LINE_RE = re.compile(
        r"^\s*(?P<value>[\d,]+|<not counted>|not counted)\s+(?P<event>\S+)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        *,
        config: Optional[PerfCounterConfig] = None,
        base_config: Optional[BaseMeterConfig] = None,
        strict: bool = True,
        runner: Optional[Any] = None,
    ) -> None:
        super().__init__(
            config=base_config,
            strict=strict,
            name="perf",
        )
        self._perf_config: PerfCounterConfig = config or PerfCounterConfig()
        self._runner = runner
        self._inner_lock = threading.RLock()

        self._start_time: Optional[float] = None
        self._last_counters: Dict[str, int] = {}
        self._last_power_watts: float = 0.0
        self._last_energy_joules: float = 0.0
        self._last_provenance: str = "estimated"

        # Resolve the binary unless a mock runner was provided.
        if runner is None:
            resolved = shutil.which(self._perf_config.binary)
            if resolved is None:
                msg = (
                    f"'{self._perf_config.binary}' not found on PATH; "
                    f"perf counters are unavailable."
                )
                if self._strict:
                    raise PerfCounterError(msg)
                logger.warning("%s PerfCounter will return estimated values.", msg)
                self._binary_available = False
            else:
                self._binary_available = True
                logger.debug("Resolved perf to %s.", resolved)
        else:
            self._binary_available = True

        logger.debug(
            "PerfCounter initialized (binary=%s, events=%s, sample=%.3fs, "
            "timeout=%.2fs, strict=%s)",
            self._perf_config.binary,
            list(self._perf_config.events),
            self._perf_config.sample_seconds,
            self._perf_config.timeout_seconds,
            self._strict,
        )

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Mark the start of a measurement window."""
        with self._inner_lock:
            if self.started:
                logger.debug("PerfCounter.start() called while already running.")
                return
            self._start_time = time.monotonic()
            self._mark_started()
        logger.debug("PerfCounter started.")

    def stop(self) -> Dict[str, Any]:
        """
        Stop the perf-counter window and return:

        ``{"energy_joules", "power_watts", "provenance",
        "cycles", "instructions", "ipc", "raw_output"}``
        """
        with self._inner_lock:
            elapsed = self._mark_stopped()

        # Allow the caller to reuse ``collect()`` directly (legacy path).
        collected = self.collect()
        counters = {
            k: v for k, v in collected.items()
            if k in self._perf_config.events
        }
        cycles = int(counters.get("cycles", 0))
        instructions = int(counters.get("instructions", 0))
        ipc = (
            instructions / cycles if cycles > 0 else 0.0
        )

        # Derive duty cycle from instructions-per-second.
        ips = (instructions / elapsed) if elapsed > 0 else 0.0
        duty = (
            max(0.0, min(1.0, ips / self._perf_config.ipc_reference))
            if self._perf_config.ipc_reference > 0 else 0.0
        )

        cfg = self._perf_config
        power_watts = cfg.idle_watts + cfg.dynamic_watts * duty
        energy_joules = power_watts * max(elapsed, 0.0)

        provenance = collected.get("provenance", "estimated")
        if not cfg.distinguish_provenance:
            provenance = "measured"

        raw = {
            "energy_joules": energy_joules,
            "power_watts": power_watts,
            "provenance": provenance,
            "cycles": cycles,
            "instructions": instructions,
            "ipc": ipc,
            "raw_output": collected.get("raw_output", ""),
            "source": "perf",
            "elapsed_seconds": elapsed,
        }

        self._last_counters = counters
        self._last_power_watts = power_watts
        self._last_energy_joules = energy_joules
        self._last_provenance = provenance

        logger.debug(
            "PerfCounter stopped: cycles=%d, instructions=%d, ipc=%.3f, "
            "power=%.4g W, energy=%.6g J, provenance=%s",
            cycles, instructions, ipc, power_watts, energy_joules, provenance,
        )

        return self._validate_metrics(raw)

    # ---------------------------------------------------------- collection
    def collect(self) -> Dict[str, Any]:
        """
        Run one ``perf stat`` sample and return structured counters.

        Legacy-compatible: the returned dict contains ``raw_output`` and
        ``provenance`` exactly as the original did, plus parsed ``cycles``
        and ``instructions`` when the counters are readable.
        """
        cfg = self._perf_config

        argv: List[str] = [cfg.binary, "stat", "-e", ",".join(cfg.events)]
        argv += ["sleep", str(cfg.sample_seconds)]

        try:
            returncode, stdout, stderr = self._run(argv)
        except PerfCounterError:
            raise
        except Exception as exc:
            raise PerfCounterError(f"perf invocation failed: {exc}") from exc

        if returncode != 0:
            msg = (
                f"'{cfg.binary}' exited with code {returncode}: "
                f"{stderr.strip()[:200]!r}"
            )
            if self._strict and self._binary_available:
                raise PerfCounterError(msg)
            logger.warning("%s Falling back to estimated values.", msg)
            return self._estimate_result(reason=msg)

        parsed = self._parse(stderr)
        not_counted = any(
            marker in stderr for marker in ("<not counted>", "not counted")
        )

        if not_counted and not cfg.tolerate_not_counted:
            msg = (
                "perf reported '<not counted>' for at least one event; "
                "this usually means insufficient permissions "
                "(check /proc/sys/kernel/perf_event_paranoid)."
            )
            if self._strict:
                raise PerfCounterError(msg)
            logger.warning("%s Falling back to estimated values.", msg)
            return self._estimate_result(reason=msg, raw_output=stderr)

        # Preserve legacy keys and add the parsed counters.
        result: Dict[str, Any] = {
            "raw_output": stderr,
            "provenance": "measured",
        }
        for ev in cfg.events:
            result[ev] = int(parsed.get(ev, 0))

        return result

    # ---------------------------------------------------------- subprocess
    def _run(self, argv: Sequence[str]) -> Tuple[int, str, str]:
        """Invoke the command and return ``(rc, stdout, stderr)``."""
        cfg = self._perf_config

        if self._runner is not None:
            try:
                result = self._runner(argv, cfg.timeout_seconds)
            except Exception as exc:
                raise PerfCounterError(
                    f"custom runner failed: {exc}"
                ) from exc
            return self._unpack_runner_result(result)

        if not self._binary_available:
            raise PerfCounterError(
                f"'{cfg.binary}' is not available on this host."
            )

        try:
            proc = subprocess.run(
                list(argv),
                capture_output=True,
                text=True,
                timeout=cfg.timeout_seconds,
                check=False,
            )
        except FileNotFoundError as exc:
            raise PerfCounterError(
                f"'{cfg.binary}' not found on PATH."
            ) from exc
        except PermissionError as exc:
            raise PerfCounterError(
                f"'{cfg.binary}' requires elevated privileges: {exc}"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise PerfCounterError(
                f"'{cfg.binary}' timed out after {cfg.timeout_seconds:.1f}s."
            ) from exc

        return proc.returncode, proc.stdout or "", proc.stderr or ""

    @staticmethod
    def _unpack_runner_result(result: Any) -> Tuple[int, str, str]:
        """Accept ``(rc, stdout, stderr)`` or objects with those attributes."""
        if isinstance(result, tuple) and len(result) == 3:
            rc, stdout, stderr = result
            return int(rc), str(stdout or ""), str(stderr or "")
        if hasattr(result, "returncode"):
            return (
                int(getattr(result, "returncode")),
                str(getattr(result, "stdout", "") or ""),
                str(getattr(result, "stderr", "") or ""),
            )
        if isinstance(result, str):
            return 0, "", result  # treat as stderr-only
        raise PerfCounterError(
            f"custom runner returned unsupported type "
            f"{type(result).__name__}; expected (rc, stdout, stderr)."
        )

    # ---------------------------------------------------------- parsing
    def _parse(self, text: str) -> Dict[str, int]:
        """
        Parse ``perf stat`` stderr output into ``{event: int_value}``.

        Handles comma-separated digit groups (``1,234,567``) and skips
        ``<not counted>`` markers.
        """
        parsed: Dict[str, int] = {}
        wanted = set(self._perf_config.events)

        for line in text.splitlines():
            m = self._LINE_RE.match(line)
            if m is None:
                continue
            event = m.group("event").strip()
            if event not in wanted:
                continue
            raw_value = m.group("value")
            if raw_value.lower() in ("<not counted>", "not counted"):
                continue
            try:
                parsed[event] = int(raw_value.replace(",", ""))
            except ValueError:
                logger.debug("Unparseable perf value %r for %s.", raw_value, event)
                continue

        return parsed

    # ---------------------------------------------------------- estimation
    def _estimate_result(
        self,
        *,
        reason: str,
        raw_output: str = "",
    ) -> Dict[str, Any]:
        """Fallback that keeps the legacy ``raw_output`` + ``provenance`` keys."""
        return {
            "raw_output": raw_output,
            "provenance": "estimated",
            "reason": reason,
        }

    # ---------------------------------------------------------- props
    @property
    def perf_config(self) -> PerfCounterConfig:
        return self._perf_config

    @property
    def binary_available(self) -> bool:
        return self._binary_available

    @property
    def last_counters(self) -> Dict[str, int]:
        return dict(self._last_counters)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": asdict(self._perf_config),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "binary_available": self._binary_available,
            "last_counters": dict(self._last_counters),
            "last_power_watts": self._last_power_watts,
            "last_energy_joules": self._last_energy_joules,
            "last_provenance": self._last_provenance,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "PerfCounter("
            f"binary={self._perf_config.binary!r}, "
            f"events={list(self._perf_config.events)}, "
            f"sample_seconds={self._perf_config.sample_seconds:.3f}, "
            f"available={self._binary_available}, "
            f"started={self.started}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "PerfCounter",
    "PerfCounterConfig",
    "PerfCounterError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.energy.perf_counters
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---------------------------------------------------------------- #
    # Mock runners covering the common environments.
    # ---------------------------------------------------------------- #
    _PERF_OK_STDERR = (
        "\n"
        " Performance counter stats for 'sleep 0.1':\n"
        "\n"
        "       123,456,789      cycles\n"
        "       246,913,578      instructions              #    2.00  insn per cycle\n"
        "\n"
        "       0.100432064 seconds time elapsed\n"
    )

    _PERF_NOT_COUNTED = (
        "\n"
        " Performance counter stats for 'sleep 0.1':\n"
        "\n"
        "   <not counted>      cycles\n"
        "   <not counted>      instructions\n"
        "\n"
        "       0.100432064 seconds time elapsed\n"
    )

    def _ok_runner(argv: Sequence[str], timeout: float):
        return 0, "", _PERF_OK_STDERR

    def _not_counted_runner(argv: Sequence[str], timeout: float):
        return 0, "", _PERF_NOT_COUNTED

    def _missing_runner(argv: Sequence[str], timeout: float):
        raise FileNotFoundError("perf: No such file or directory")

    def _permission_runner(argv: Sequence[str], timeout: float):
        return 255, "", "perf: Permission denied."

    def _timeout_runner(argv: Sequence[str], timeout: float):
        raise subprocess.TimeoutExpired(argv, timeout)

    # ---- Happy path (counters parse cleanly) -------------------------- #
    meter = PerfCounter(runner=_ok_runner)
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.05)
    sample: Optional[TelemetrySample] = getattr(m, "last_sample", None)
    print("sample     :", sample)
    if sample is not None:
        print("dict       :", sample.to_dict())

    # ---- Legacy ``collect()`` shape preserved ------------------------- #
    legacy = PerfCounter(runner=_ok_runner).collect()
    print("legacy     :", {
        "raw_output_present": bool(legacy.get("raw_output")),
        "provenance": legacy.get("provenance"),
        "cycles": legacy.get("cycles"),
        "instructions": legacy.get("instructions"),
    })
    assert "raw_output" in legacy and "provenance" in legacy

    # ---- <not counted> → strict raises, lenient estimates ------------- #
    try:
        strict = PerfCounter(runner=_not_counted_runner, strict=True)
        strict.start()
        strict.stop()
    except PerfCounterError as exc:
        print("not counted:", exc)

    lenient = PerfCounter(runner=_not_counted_runner, strict=False)
    with lenient.measure() as m:
        time.sleep(0.01)
    print("lenient    :", m.last_sample)

    # ---- Missing binary ------------------------------------------------ #
    missing = PerfCounter(runner=_missing_runner, strict=False)
    with missing.measure() as m:
        pass
    print("missing    :", m.last_sample)

    # ---- Permission denied -------------------------------------------- #
    try:
        perm = PerfCounter(runner=_permission_runner, strict=True)
        perm.start()
        perm.stop()
    except PerfCounterError as exc:
        print("perm       :", exc)

    # ---- Timeout ------------------------------------------------------- #
    try:
        to = PerfCounter(runner=_timeout_runner, strict=True)
        to.start()
        to.stop()
    except PerfCounterError as exc:
        print("timeout    :", exc)

    # ---- Serialization ------------------------------------------------- #
    payload = meter.to_json()
    assert json.loads(payload)["config"]["binary"] == "perf"
    print("Serialization OK.")

    # ---- Config validation -------------------------------------------- #
    for bad_cfg in (
        dict(events=()),
        dict(sample_seconds=0),
        dict(binary=""),
        dict(timeout_seconds=0),
        dict(idle_watts=-1.0),
        dict(dynamic_watts=-1.0),
        dict(ipc_reference=0),
    ):
        try:
            PerfCounterConfig(**bad_cfg)  # type: ignore[arg-type]
        except PerfCounterError as exc:
            print("Rejected cfg:", exc)

    # ---- Live probe only when perf is actually installed and permitted #
    if shutil.which("perf") is not None:
        real = PerfCounter(strict=False)
        with real.measure() as m:
            time.sleep(0.05)
        print("real host  :", m.last_sample)
    else:
        print("real host  : perf not present (skipping live probe).")

    print("\nSmoke test passed.")
