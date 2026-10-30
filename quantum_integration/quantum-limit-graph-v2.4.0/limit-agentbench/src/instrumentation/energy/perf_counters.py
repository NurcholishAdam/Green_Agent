# src/instrumentation/energy/perf_counters.py

"""
Perf Counter Meter
==================

Hardware performance counters via Linux ``perf stat``.

Reports CPU cycles and instructions per measurement window, plus a
power-estimated energy figure derived from the sampled duty cycle.

Enhancements
------------
- :class:`Provenance` (from ``base_meter``) replaces the raw string.
- **Fixed duty-cycle window** — ``ips = instructions / sample_seconds``
  (the *measurement* window) rather than ``instructions / elapsed``.
  The previous formula off-by-``sample_seconds/elapsed`` underestimated
  IPS by that ratio, and hence underestimated energy.
- **Sanitized propagation** — every fallback (missing binary, permissions,
  timeout, ``<not counted>``, non-zero exit, empty parse) sets
  ``self._last_sanitized = True``. Downstream consumers can distinguish
  fabricated estimates from measured readings.
- **Fail-closed on empty parse** — if ``perf`` returns exit 0 but no
  counters parsed, the sample is treated as fallback rather than
  reported as ``measured`` with zeros.
- **Lenient ``_run()`` failures** — ``FileNotFoundError``,
  ``PermissionError``, and ``TimeoutExpired`` are now caught by
  ``collect()`` in lenient mode and degrade to an estimate, matching the
  docstring's promise.
- **`distinguish_provenance=False` still sets `sanitized=True`** — the
  flag lets legacy callers see ``"measured"`` on fabricated data, but the
  ``sanitized`` signal is preserved regardless.
- **Numeric config strictness** — rejects ``bool`` and ``NaN`` for
  ``sample_seconds``, ``timeout_seconds``, ``idle_watts``,
  ``dynamic_watts``, ``ipc_reference``.
- **Reset state on `stop()`** — ``_last_sanitized`` is cleared at the
  start of every ``stop()`` so direct callers don't inherit stale flags.
- ``_start_time`` removed (was set, never read).
- ``_estimate_result`` returns the same shape as the measured path (with
  event keys defaulting to ``0``).
- ``last_sanitized`` property exposed on the meter.
- ``__main__`` smoke test extended with fallback-window, empty-parse, and
  lenient-``_run`` regression cases.

Modeling note
-------------
The power figure is a **model** derived from the sample's duty cycle::

    ips   = instructions / sample_seconds
    duty  = clamp(ips / ipc_reference, 0, 1)
    power = idle_watts + dynamic_watts * duty
    energy = power * elapsed

The model assumes the CPU's busy fraction during ``sample_seconds`` is
representative of the entire ``elapsed`` window. When ``perf`` cannot
run, the estimate collapses to ``duty = 0`` → ``power = idle_watts`` →
``energy = idle_watts * elapsed`` and is marked ``sanitized=True``.

Note on blocking
----------------
``stop()`` blocks for approximately ``sample_seconds`` (plus subprocess
overhead) because it synchronously runs ``perf stat sleep
{sample_seconds}``.
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import re
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    Provenance,
    TelemetrySample,
)

logger = logging.getLogger(__name__)

__all__ = [
    "PerfCounter",
    "PerfCounterConfig",
    "PerfCounterError",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PerfCounterError(InstrumentationError):
    """Raised for invalid perf-counter inputs or runtime failures."""


# --------------------------------------------------------------------------- #
# Config helpers
# --------------------------------------------------------------------------- #
def _require_positive_finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise PerfCounterError(f"{label} must be a number, not bool")
    fv = float(value)
    if not math.isfinite(fv):
        raise PerfCounterError(f"{label} must be finite")
    if fv <= 0:
        raise PerfCounterError(f"{label} must be > 0")
    return fv


def _require_non_negative_finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise PerfCounterError(f"{label} must be a number, not bool")
    fv = float(value)
    if not math.isfinite(fv):
        raise PerfCounterError(f"{label} must be finite")
    if fv < 0:
        raise PerfCounterError(f"{label} must be >= 0")
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PerfCounterConfig:
    """Tunable parameters for the perf-counter meter."""

    events: tuple = ("cycles", "instructions")
    sample_seconds: float = 0.1
    binary: str = "perf"
    timeout_seconds: float = 10.0

    # Energy model coefficients. See the module docstring.
    idle_watts: float = 3.0
    dynamic_watts: float = 45.0
    # Reference instructions-per-second; a CPU at this rate is 100% duty.
    ipc_reference: float = 2_000_000_000.0

    distinguish_provenance: bool = True
    tolerate_not_counted: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.events, tuple) or not self.events:
            raise PerfCounterError(
                "events must be a non-empty tuple"
            )
        for ev in self.events:
            if not isinstance(ev, str) or not ev:
                raise PerfCounterError(
                    "events entries must be non-empty strings"
                )

        if not isinstance(self.binary, str) or not self.binary:
            raise PerfCounterError(
                "binary must be a non-empty string"
            )

        object.__setattr__(
            self, "sample_seconds",
            _require_positive_finite(
                self.sample_seconds, "sample_seconds",
            ),
        )
        object.__setattr__(
            self, "timeout_seconds",
            _require_positive_finite(
                self.timeout_seconds, "timeout_seconds",
            ),
        )
        object.__setattr__(
            self, "idle_watts",
            _require_non_negative_finite(
                self.idle_watts, "idle_watts",
            ),
        )
        object.__setattr__(
            self, "dynamic_watts",
            _require_non_negative_finite(
                self.dynamic_watts, "dynamic_watts",
            ),
        )
        object.__setattr__(
            self, "ipc_reference",
            _require_positive_finite(
                self.ipc_reference, "ipc_reference",
            ),
        )

        if not isinstance(self.distinguish_provenance, bool):
            raise PerfCounterError(
                "distinguish_provenance must be a bool"
            )
        if not isinstance(self.tolerate_not_counted, bool):
            raise PerfCounterError(
                "tolerate_not_counted must be a bool"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class PerfCounter(BaseEnergyMeter):
    """
    Hardware performance counters using Linux ``perf``.

    See the module docstring for the modeling assumption, the blocking
    behavior of ``stop()``, and the fallback/sanitization rules.
    """

    _LINE_RE = re.compile(
        r"^\s*(?P<value>[\d,]+|<not counted>|not counted)\s+"
        r"(?P<event>\S+)",
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
        if config is not None and not isinstance(
            config, PerfCounterConfig
        ):
            raise PerfCounterError(
                "config must be a PerfCounterConfig instance"
            )
        self._perf_config: PerfCounterConfig = (
            config or PerfCounterConfig()
        )
        self._runner = runner
        self._inner_lock = threading.RLock()

        self._last_counters: Dict[str, int] = {}
        self._last_power_watts: float = 0.0
        self._last_energy_joules: float = 0.0
        self._last_provenance: Provenance = Provenance.ESTIMATED
        self._last_collect_fell_back: bool = False

        if runner is None:
            resolved = shutil.which(self._perf_config.binary)
            if resolved is None:
                msg = (
                    f"'{self._perf_config.binary}' not found on PATH; "
                    f"perf counters are unavailable."
                )
                if self._strict:
                    raise PerfCounterError(msg)
                logger.warning(
                    "%s PerfCounter will estimate values; samples "
                    "will be marked sanitized.", msg,
                )
                self._binary_available = False
            else:
                self._binary_available = True
                logger.debug(
                    "Resolved perf to %s.", resolved,
                )
        else:
            self._binary_available = True

        logger.debug(
            "PerfCounter initialized (binary=%s, events=%s, "
            "sample=%.3fs, timeout=%.2fs, strict=%s)",
            self._perf_config.binary,
            list(self._perf_config.events),
            self._perf_config.sample_seconds,
            self._perf_config.timeout_seconds,
            self._strict,
        )

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        with self._inner_lock:
            if self.started:
                logger.debug(
                    "PerfCounter.start() called while already running."
                )
                return
            self._mark_started()
        logger.debug("PerfCounter started.")

    def stop(self) -> Dict[str, Any]:
        """
        Stop the window and return the enriched metrics dict.

        ``stop()`` blocks for approximately ``sample_seconds`` because
        it synchronously runs ``perf stat sleep {sample_seconds}``.
        """
        with self._inner_lock:
            # Reset state so direct callers don't inherit stale flags.
            self._last_sanitized = False
            elapsed = self._mark_stopped()

        collected = self.collect()

        cfg = self._perf_config
        counters = {
            k: int(v) for k, v in collected.items()
            if k in cfg.events
        }
        cycles = counters.get("cycles", 0)
        instructions = counters.get("instructions", 0)

        # -- Corrected duty-cycle calculation -------------------------
        # ``instructions`` was measured over ``sample_seconds`` — NOT
        # over the caller's elapsed window. Use the measurement window
        # to derive IPS; then integrate power over elapsed for energy.
        sample_seconds = cfg.sample_seconds
        ips = (
            instructions / sample_seconds
            if sample_seconds > 0
            else 0.0
        )
        duty = max(
            0.0, min(1.0, ips / cfg.ipc_reference)
        ) if cfg.ipc_reference > 0 else 0.0

        power_watts = cfg.idle_watts + cfg.dynamic_watts * duty
        energy_joules = power_watts * max(elapsed, 0.0)

        ipc = (instructions / cycles) if cycles > 0 else 0.0

        # -- Provenance ------------------------------------------------
        raw_prov = collected.get("provenance", "estimated")
        try:
            provenance = Provenance(raw_prov)
        except (ValueError, TypeError):
            provenance = Provenance.ESTIMATED

        if not cfg.distinguish_provenance:
            provenance = Provenance.MEASURED
            # …but the sample is still fabricated if the fallback ran.

        # -- Sanitization ---------------------------------------------
        if self._last_collect_fell_back or not self._binary_available:
            self._last_sanitized = True

        raw = {
            "energy_joules": energy_joules,
            "power_watts": power_watts,
            "provenance": provenance.value,
            "cycles": cycles,
            "instructions": instructions,
            "ipc": ipc,
            "raw_output": collected.get("raw_output", ""),
            "source": "perf",
            "elapsed_seconds": elapsed,
            "sanitized": self._last_sanitized,
        }

        self._last_counters = counters
        self._last_power_watts = power_watts
        self._last_energy_joules = energy_joules
        self._last_provenance = provenance

        logger.debug(
            "PerfCounter stopped: cycles=%d, instructions=%d, "
            "ipc=%.3f, power=%.4g W, energy=%.6g J, "
            "provenance=%s, sanitized=%s",
            cycles, instructions, ipc,
            power_watts, energy_joules,
            provenance.value, self._last_sanitized,
        )

        return self._validate_metrics(raw)

    # ---------------------------------------------------------- collection
    def collect(self) -> Dict[str, Any]:
        """
        Run one ``perf stat`` sample and return structured counters.

        The returned dict has ``raw_output`` and ``provenance`` — plus
        the configured event keys — in both success and fallback paths.
        """
        cfg = self._perf_config
        self._last_collect_fell_back = False

        argv: List[str] = [
            cfg.binary, "stat", "-e", ",".join(cfg.events),
        ]
        argv += ["sleep", str(cfg.sample_seconds)]

        # -- Run the subprocess (or mock) -----------------------------
        try:
            returncode, stdout, stderr = self._run(argv)
        except PerfCounterError as exc:
            # Missing binary, permissions, timeout, or mock failure.
            if self._strict:
                raise
            logger.warning(
                "%s Falling back to estimated values.", exc,
            )
            self._last_collect_fell_back = True
            return self._estimate_result(reason=str(exc))
        except Exception as exc:
            raise PerfCounterError(
                f"perf invocation failed: {exc}"
            ) from exc

        if returncode != 0:
            msg = (
                f"'{cfg.binary}' exited with code {returncode}: "
                f"{stderr.strip()[:200]!r}"
            )
            if self._strict and self._binary_available:
                raise PerfCounterError(msg)
            logger.warning(
                "%s Falling back to estimated values.", msg,
            )
            self._last_collect_fell_back = True
            return self._estimate_result(
                reason=msg, raw_output=stderr,
            )

        parsed = self._parse(stderr)
        not_counted = any(
            marker in stderr
            for marker in ("<not counted>", "not counted")
        )

        if not_counted and not cfg.tolerate_not_counted:
            msg = (
                "perf reported '<not counted>' for at least one "
                "event; this usually means insufficient permissions "
                "(check /proc/sys/kernel/perf_event_paranoid)."
            )
            if self._strict:
                raise PerfCounterError(msg)
            logger.warning(
                "%s Falling back to estimated values.", msg,
            )
            self._last_collect_fell_back = True
            return self._estimate_result(
                reason=msg, raw_output=stderr,
            )

        # -- Fail-closed on empty parse ------------------------------
        if not parsed and cfg.events:
            msg = (
                "perf produced no usable counters for the configured "
                f"events {list(cfg.events)}"
            )
            if self._strict:
                raise PerfCounterError(msg)
            logger.warning(
                "%s Falling back to estimated values.", msg,
            )
            self._last_collect_fell_back = True
            return self._estimate_result(
                reason=msg, raw_output=stderr,
            )

        result: Dict[str, Any] = {
            "raw_output": stderr,
            "provenance": "measured",
        }
        for ev in cfg.events:
            result[ev] = int(parsed.get(ev, 0))
        return result

    # ---------------------------------------------------------- subprocess
    def _run(self, argv: Sequence[str]) -> Tuple[int, str, str]:
        cfg = self._perf_config

        if self._runner is not None:
            try:
                result = self._runner(
                    argv, cfg.timeout_seconds,
                )
            except PerfCounterError:
                raise
            except Exception as exc:
                raise PerfCounterError(
                    f"custom runner failed: {exc}"
                ) from exc
            return self._unpack_runner_result(result)

        if not self._binary_available:
            raise PerfCounterError(
                f"'{cfg.binary}' is not available on this host"
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
                f"'{cfg.binary}' requires elevated privileges: "
                f"{exc}"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise PerfCounterError(
                f"'{cfg.binary}' timed out after "
                f"{cfg.timeout_seconds:.1f}s."
            ) from exc

        return proc.returncode, proc.stdout or "", proc.stderr or ""

    @staticmethod
    def _unpack_runner_result(result: Any) -> Tuple[int, str, str]:
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
            return 0, "", result
        raise PerfCounterError(
            f"custom runner returned unsupported type "
            f"{type(result).__name__}; expected (rc, stdout, stderr)."
        )

    # ---------------------------------------------------------- parsing
    def _parse(self, text: str) -> Dict[str, int]:
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
                logger.debug(
                    "Unparseable perf value %r for %s.",
                    raw_value, event,
                )
                continue

        return parsed

    # ---------------------------------------------------------- estimation
    def _estimate_result(
        self,
        *,
        reason: str,
        raw_output: str = "",
    ) -> Dict[str, Any]:
        """Fallback result with the same shape as the measured path."""
        result: Dict[str, Any] = {
            "raw_output": raw_output,
            "provenance": "estimated",
            "reason": reason,
        }
        for ev in self._perf_config.events:
            result[ev] = 0
        return result

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

    @property
    def last_sanitized(self) -> bool:
        return self._last_sanitized

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": self._perf_config.to_dict(),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "binary_available": self._binary_available,
            "last_counters": dict(self._last_counters),
            "last_power_watts": self._last_power_watts,
            "last_energy_joules": self._last_energy_joules,
            "last_provenance": self._last_provenance.value,
            "last_sanitized": self._last_sanitized,
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
            f"strict={self._strict}, "
            f"sanitized={self._last_sanitized})"
        )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    _PERF_OK_STDERR = (
        "\n"
        " Performance counter stats for 'sleep 0.1':\n"
        "\n"
        "       123,456,789      cycles\n"
        "       246,913,578      instructions              "
        "#    2.00  insn per cycle\n"
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

    _PERF_MALFORMED = "garbage output that does not match\n"

    def _ok_runner(argv, timeout):
        return 0, "", _PERF_OK_STDERR

    def _not_counted_runner(argv, timeout):
        return 0, "", _PERF_NOT_COUNTED

    def _malformed_runner(argv, timeout):
        return 0, "", _PERF_MALFORMED

    def _missing_runner(argv, timeout):
        raise FileNotFoundError("perf: No such file or directory")

    def _permission_runner(argv, timeout):
        return 255, "", "perf: Permission denied."

    def _timeout_runner(argv, timeout):
        raise subprocess.TimeoutExpired(argv, timeout)

    # -- Happy path -------------------------------------------------- #
    meter = PerfCounter(runner=_ok_runner)
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.05)
    sample: Optional[TelemetrySample] = getattr(
        m, "last_sample", None,
    )
    print("sample     :", sample)
    assert sample is not None
    assert sample.sanitized is False
    assert sample.provenance is Provenance.MEASURED

    # -- Legacy collect() shape ------------------------------------- #
    legacy = PerfCounter(runner=_ok_runner).collect()
    assert "raw_output" in legacy and "provenance" in legacy
    assert legacy["provenance"] == "measured"
    assert legacy["cycles"] == 123_456_789
    print("legacy     :", {
        "cycles": legacy["cycles"],
        "instructions": legacy["instructions"],
    })

    # -- Duty-cycle window regression -------------------------------- #
    # If instructions == sample_seconds * ipc_reference, duty == 1.0.
    def _full_load_runner(argv, timeout):
        # 0.1 s * 2e9 IPS = 200M instructions.
        text = (
            "       200,000,000      cycles\n"
            "       200,000,000      instructions\n"
        )
        return 0, "", text

    full = PerfCounter(
        config=PerfCounterConfig(
            sample_seconds=0.1,
            ipc_reference=2_000_000_000.0,
            idle_watts=3.0, dynamic_watts=45.0,
        ),
        runner=_full_load_runner,
    )
    full.start()
    time.sleep(0.2)  # elapsed ~0.2s; sample window is 0.1s
    metrics = full.stop()
    # duty should be 1.0 (200M / 0.1s == 2e9 IPS == reference)
    assert abs(
        metrics["power_watts"] - (3.0 + 45.0)
    ) < 0.01, metrics["power_watts"]
    print(
        "duty       : power=", metrics["power_watts"],
        "W (should be 48 W)",
    )

    # -- <not counted> → strict raises, lenient falls back ----------- #
    try:
        strict = PerfCounter(
            runner=_not_counted_runner, strict=True,
        )
        strict.start()
        strict.stop()
    except PerfCounterError as exc:
        print("not counted:", exc)
    else:
        raise AssertionError(
            "strict should have raised on <not counted>"
        )

    lenient = PerfCounter(
        runner=_not_counted_runner,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with lenient.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    assert m.last_sample.provenance is Provenance.ESTIMATED
    print("lenient    :", m.last_sample, "(sanitized)")

    # -- Malformed output (empty parse) ------------------------------ #
    malformed = PerfCounter(
        runner=_malformed_runner,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with malformed.measure() as m:
        pass
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    print("malformed  :", m.last_sample, "(sanitized)")

    # -- Missing binary in lenient mode → falls back ----------------- #
    missing = PerfCounter(
        runner=_missing_runner,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with missing.measure() as m:
        pass
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    print("missing    :", m.last_sample, "(sanitized)")

    # -- Permission denied ------------------------------------------- #
    try:
        perm = PerfCounter(
            runner=_permission_runner, strict=True,
        )
        perm.start()
        perm.stop()
    except PerfCounterError as exc:
        print("perm       :", exc)

    # -- Timeout ----------------------------------------------------- #
    try:
        to = PerfCounter(runner=_timeout_runner, strict=True)
        to.start()
        to.stop()
    except PerfCounterError as exc:
        print("timeout    :", exc)

    # -- distinguish_provenance=False still sets sanitized ---------- #
    class_forced = PerfCounter(
        runner=_not_counted_runner,
        config=PerfCounterConfig(
            distinguish_provenance=False,
        ),
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with class_forced.measure() as m:
        pass
    assert m.last_sample is not None
    assert m.last_sample.provenance is Provenance.MEASURED
    assert m.last_sample.sanitized is True, (
        "distinguish_provenance=False must not hide sanitization"
    )
    print(
        "forced     : provenance=", m.last_sample.provenance,
        " sanitized=", m.last_sample.sanitized,
    )

    # -- Serialization ----------------------------------------------- #
    payload = meter.to_json()
    assert json.loads(payload)["config"]["binary"] == "perf"
    print("Serialization OK.")

    # -- Config validation ------------------------------------------- #
    bad_configs = (
        dict(events=()),
        dict(events=("x", 42)),
        dict(sample_seconds=0),
        dict(sample_seconds=True),
        dict(sample_seconds=float("nan")),
        dict(binary=""),
        dict(timeout_seconds=0),
        dict(timeout_seconds=True),
        dict(idle_watts=-1.0),
        dict(idle_watts=float("nan")),
        dict(dynamic_watts=-1.0),
        dict(dynamic_watts=True),
        dict(ipc_reference=0),
        dict(ipc_reference=float("nan")),
        dict(distinguish_provenance="yes"),
        dict(tolerate_not_counted=1),
    )
    for bad in bad_configs:
        try:
            PerfCounterConfig(**bad)  # type: ignore[arg-type]
        except PerfCounterError as exc:
            print("Rejected cfg:", exc)
        else:
            raise AssertionError(
                f"config should have rejected {bad}"
            )

    # -- Live probe -------------------------------------------------- #
    if shutil.which("perf") is not None:
        real = PerfCounter(
            strict=False,
            base_config=BaseMeterConfig(enforce_contract=False),
        )
        with real.measure() as m:
            time.sleep(0.05)
        print("real host  :", m.last_sample)
    else:
        print("real host  : perf not present (skipping live probe).")

    print("\nSmoke test passed.")
