# src/instrumentation/energy/dcgm_gpu_meter.py

"""
DCGM GPU Meter
==============

Measures GPU power draw via NVIDIA DCGM (through ``nvidia-smi``).

Enhancements
------------
- ``DCGMMeterConfig`` — frozen, fully validated (bool/NaN rejection,
  non-empty ``power_field``, ``power_field`` must be ``query_fields[0]``).
- :class:`Provenance` (from ``base_meter``) replaces the raw string.
- **Sampling loop implemented** — ``stop()`` takes ``sample_count``
  samples spaced ``sample_interval_seconds`` apart (in addition to the
  initial sample from ``start()``). ``stop()`` therefore **blocks** for
  ``(sample_count - 1) * sample_interval_seconds`` when both are
  non-zero; document this at call sites.
- **Provenance is honest** — ``MEASURED`` requires the binary to be
  available *and* every sample to have succeeded. Any failure (missing
  binary, one or more sample failures, or zero samples obtained)
  downgrades to ``ESTIMATED`` and sets ``_last_sanitized`` so the
  emitted :class:`TelemetrySample` carries ``sanitized=True``.
- Per-sample success/failure counters exposed on ``extras``
  (``samples_taken``, ``samples_failed``).
- ``_last_per_gpu_watts`` and ``_start_time`` initialized in
  ``__init__``.
- ``power_field`` config field — the parser's "first column is
  power.draw" assumption is now explicit and validated.
- ``__main__`` smoke test with sampling-loop, partial-failure, and
  all-N/A regression cases.

Note on lenient mode
--------------------
To suppress contract violations the meter must be constructed with
**both** ``strict=False`` **and**
``base_config=BaseMeterConfig(enforce_contract=False)``.

Note on the ``runner`` callable
-------------------------------
A custom runner is ``(argv, timeout) -> (returncode, stdout)``. It is
the runner's responsibility to honor ``timeout``; the meter calls the
runner synchronously and cannot interrupt a hang.
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    Provenance,
    TelemetrySample,
)

logger = logging.getLogger(__name__)

__all__ = [
    "DCGMMeter",
    "DCGMMeterConfig",
    "DCGMMeterError",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DCGMMeterError(InstrumentationError):
    """Raised for invalid DCGM meter inputs or runtime failures."""


# --------------------------------------------------------------------------- #
# Config helpers
# --------------------------------------------------------------------------- #
def _reject_bool_and_nan(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise DCGMMeterError(f"{label} must be a number, not bool")
    fv = float(value)
    if not math.isfinite(fv):
        raise DCGMMeterError(f"{label} must be finite")
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DCGMMeterConfig:
    """Tunable parameters for the DCGM GPU meter."""

    binary: str = "nvidia-smi"

    # The parser assumes ``query_fields[0] == power_field``; this is
    # validated in ``__post_init__``.
    power_field: str = "power.draw"
    query_fields: tuple = ("power.draw",)

    gpu_index: Optional[int] = None
    timeout_seconds: float = 5.0

    # Sampling loop applied inside ``stop()``:
    # * ``sample_count`` total samples are taken (in addition to the
    #   initial sample from ``start()``).
    # * Consecutive samples are spaced ``sample_interval_seconds`` apart.
    # * ``stop()`` blocks for ``(sample_count - 1) * interval`` seconds
    #   when both are non-zero.
    sample_interval_seconds: float = 0.0
    sample_count: int = 1

    # Warn (but don't sanitize) when real 0 W readings are obtained —
    # useful for detecting MIG slices where power.draw isn't exposed.
    zero_power_warning: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.binary, str) or not self.binary:
            raise DCGMMeterError("binary must be a non-empty string")

        if (
            not isinstance(self.power_field, str)
            or not self.power_field.strip()
        ):
            raise DCGMMeterError(
                "power_field must be a non-empty string"
            )

        if (
            not isinstance(self.query_fields, tuple)
            or not self.query_fields
        ):
            raise DCGMMeterError(
                "query_fields must be a non-empty tuple of strings"
            )
        for field_name in self.query_fields:
            if not isinstance(field_name, str) or not field_name:
                raise DCGMMeterError(
                    "query_fields entries must be non-empty strings"
                )
        if self.query_fields[0] != self.power_field:
            raise DCGMMeterError(
                f"query_fields[0] must be {self.power_field!r} "
                f"(the parser reads the first column as power); "
                f"got {self.query_fields[0]!r}"
            )

        if self.gpu_index is not None:
            if isinstance(self.gpu_index, bool) or not isinstance(
                self.gpu_index, int
            ):
                raise DCGMMeterError(
                    "gpu_index must be a non-negative int or None"
                )
            if self.gpu_index < 0:
                raise DCGMMeterError(
                    "gpu_index must be a non-negative int or None"
                )

        timeout = _reject_bool_and_nan(
            self.timeout_seconds, "timeout_seconds",
        )
        if timeout <= 0:
            raise DCGMMeterError("timeout_seconds must be > 0")
        object.__setattr__(self, "timeout_seconds", timeout)

        interval = _reject_bool_and_nan(
            self.sample_interval_seconds,
            "sample_interval_seconds",
        )
        if interval < 0:
            raise DCGMMeterError(
                "sample_interval_seconds must be >= 0"
            )
        object.__setattr__(
            self, "sample_interval_seconds", interval,
        )

        if isinstance(self.sample_count, bool) or not isinstance(
            self.sample_count, int
        ):
            raise DCGMMeterError(
                "sample_count must be a positive int, not bool"
            )
        if self.sample_count <= 0:
            raise DCGMMeterError("sample_count must be > 0")

        if not isinstance(self.zero_power_warning, bool):
            raise DCGMMeterError("zero_power_warning must be a bool")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class DCGMMeter(BaseEnergyMeter):
    """
    GPU power draw via NVIDIA DCGM (through ``nvidia-smi``).

    See the module docstring for the sampling-loop blocking behavior
    and for the lenient-mode contract.
    """

    def __init__(
        self,
        *,
        config: Optional[DCGMMeterConfig] = None,
        base_config: Optional[BaseMeterConfig] = None,
        strict: bool = True,
        runner: Optional[Any] = None,
    ) -> None:
        super().__init__(
            config=base_config,
            strict=strict,
            name="dcgm",
        )
        self._gpu_config: DCGMMeterConfig = config or DCGMMeterConfig()
        self._runner = runner
        self._inner_lock = threading.RLock()

        self._start_time: Optional[float] = None
        self._samples: List[float] = []
        self._samples_succeeded: int = 0
        self._samples_failed: int = 0
        self._last_per_gpu_watts: List[float] = []

        if runner is None:
            resolved = shutil.which(self._gpu_config.binary)
            if resolved is None:
                msg = (
                    f"'{self._gpu_config.binary}' not found on PATH; "
                    f"DCGM GPU metering is unavailable."
                )
                if self._strict:
                    raise DCGMMeterError(msg)
                logger.warning(
                    "%s DCGMMeter will return fabricated zeros; "
                    "samples will be marked sanitized.", msg,
                )
                self._binary_available = False
            else:
                self._binary_available = True
                logger.debug(
                    "Resolved nvidia-smi to %s.", resolved,
                )
        else:
            self._binary_available = True

        logger.debug(
            "DCGMMeter initialized (binary=%s, fields=%s, gpu_index=%s, "
            "timeout=%.2fs, sample_count=%d, interval=%.2fs, strict=%s)",
            self._gpu_config.binary,
            list(self._gpu_config.query_fields),
            self._gpu_config.gpu_index,
            self._gpu_config.timeout_seconds,
            self._gpu_config.sample_count,
            self._gpu_config.sample_interval_seconds,
            self._strict,
        )

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        with self._inner_lock:
            if self.started:
                logger.debug(
                    "DCGMMeter.start() called while already running."
                )
                return
            self._start_time = time.monotonic()
            self._samples = []
            self._samples_succeeded = 0
            self._samples_failed = 0
            self._last_per_gpu_watts = []
            self._mark_started()

            try:
                self._samples.append(self._sample_power_watts())
                self._samples_succeeded += 1
            except DCGMMeterError:
                self._samples_failed += 1
                if self._strict:
                    raise
                logger.warning(
                    "Initial DCGM sample failed; continuing."
                )
        logger.debug("DCGMMeter started.")

    def stop(self) -> Dict[str, Any]:
        """
        Stop the DCGM measurement window and return::

            {"energy_joules": float, "power_watts": float,
             "per_gpu_watts": [float, ...], "gpu_count": int,
             "provenance": "measured" | "estimated"}

        ``stop()`` takes ``sample_count`` additional samples spaced
        ``sample_interval_seconds`` apart, so it **blocks** for
        ``(sample_count - 1) * sample_interval_seconds`` when both are
        non-zero.

        ``provenance`` is ``"measured"`` only when the binary was
        available AND every sample succeeded. Any failure downgrades
        to ``"estimated"`` and marks ``self._last_sanitized = True``.
        """
        with self._inner_lock:
            elapsed = self._mark_stopped()

            interval = self._gpu_config.sample_interval_seconds
            count = self._gpu_config.sample_count

            # Sampling loop (in addition to the sample from start()).
            for i in range(count):
                if i > 0 and interval > 0:
                    time.sleep(interval)
                try:
                    self._samples.append(self._sample_power_watts())
                    self._samples_succeeded += 1
                except DCGMMeterError:
                    self._samples_failed += 1
                    if self._strict:
                        raise
                    logger.warning(
                        "DCGM stop() sample %d failed; continuing.", i,
                    )

            if self._samples:
                mean_power = sum(self._samples) / len(self._samples)
                per_gpu = list(self._last_per_gpu_watts)
            else:
                mean_power = 0.0
                per_gpu = []
                self._last_sanitized = True

            # Any failure downgrades the reading.
            if (
                not self._binary_available
                or self._samples_failed > 0
                or not self._samples
            ):
                self._last_sanitized = True

            provenance = (
                Provenance.MEASURED
                if (
                    self._binary_available
                    and self._samples_failed == 0
                    and self._samples
                )
                else Provenance.ESTIMATED
            )

            energy_joules = max(0.0, mean_power * max(elapsed, 0.0))

            raw = {
                "energy_joules": energy_joules,
                "power_watts": max(0.0, mean_power),
                "per_gpu_watts": per_gpu,
                "gpu_count": len(per_gpu),
                "samples_taken": self._samples_succeeded,
                "samples_failed": self._samples_failed,
                "provenance": provenance.value,
                "source": "dcgm",
                "elapsed_seconds": elapsed,
            }

        if (
            self._gpu_config.zero_power_warning
            and raw["power_watts"] == 0.0
            and self._binary_available
        ):
            logger.warning(
                "DCGMMeter reported 0 W (samples_taken=%d, "
                "samples_failed=%d); this may indicate an idle GPU, "
                "a MIG slice, or a driver that does not expose "
                "power.draw.",
                raw["samples_taken"],
                raw["samples_failed"],
            )

        logger.debug(
            "DCGMMeter stopped: power=%.4g W, energy=%.6g J, "
            "elapsed=%.3fs, gpus=%d, samples_taken=%d, "
            "samples_failed=%d, sanitized=%s",
            raw["power_watts"],
            raw["energy_joules"],
            elapsed,
            raw["gpu_count"],
            raw["samples_taken"],
            raw["samples_failed"],
            self._last_sanitized,
        )

        return self._validate_metrics(raw)

    # ---------------------------------------------------------- sampling
    def _sample_power_watts(self) -> float:
        try:
            per_gpu = self._run_query()
        except DCGMMeterError:
            raise
        except Exception as exc:
            raise DCGMMeterError(
                f"nvidia-smi sample failed: {exc}"
            ) from exc

        self._last_per_gpu_watts = list(per_gpu)
        if not per_gpu:
            return 0.0
        return sum(per_gpu) / len(per_gpu)

    def _run_query(self) -> List[float]:
        cfg = self._gpu_config

        argv = [cfg.binary]
        if cfg.gpu_index is not None:
            argv += ["-i", str(cfg.gpu_index)]
        argv += [
            f"--query-gpu={','.join(cfg.query_fields)}",
            "--format=csv,noheader,nounits",
        ]

        if self._runner is not None:
            try:
                result = self._runner(argv, cfg.timeout_seconds)
            except Exception as exc:
                raise DCGMMeterError(
                    f"custom runner failed: {exc}"
                ) from exc
            returncode, stdout = self._unpack_runner_result(result)
        else:
            if not self._binary_available:
                return []
            try:
                proc = subprocess.run(
                    argv,
                    capture_output=True,
                    text=True,
                    timeout=cfg.timeout_seconds,
                    check=False,
                )
            except FileNotFoundError as exc:
                raise DCGMMeterError(
                    f"'{cfg.binary}' not found on PATH."
                ) from exc
            except PermissionError as exc:
                raise DCGMMeterError(
                    f"'{cfg.binary}' is not executable by this user: "
                    f"{exc}"
                ) from exc
            except subprocess.TimeoutExpired as exc:
                raise DCGMMeterError(
                    f"'{cfg.binary}' timed out after "
                    f"{cfg.timeout_seconds:.1f}s."
                ) from exc

            returncode = proc.returncode
            stdout = proc.stdout or ""

        if returncode != 0:
            raise DCGMMeterError(
                f"'{cfg.binary}' exited with code {returncode}."
            )

        return self._parse_output(stdout)

    @staticmethod
    def _unpack_runner_result(result: Any) -> Any:
        if isinstance(result, tuple) and len(result) == 2:
            rc, stdout = result
            return int(rc), str(stdout)
        if hasattr(result, "returncode") and hasattr(result, "stdout"):
            return int(result.returncode), str(result.stdout or "")
        if isinstance(result, str):
            return 0, result
        raise DCGMMeterError(
            f"custom runner returned unsupported type "
            f"{type(result).__name__}; expected (rc, stdout)."
        )

    def _parse_output(self, stdout: str) -> List[float]:
        per_gpu: List[float] = []
        gpu_index = self._gpu_config.gpu_index

        for line_no, raw_line in enumerate(
            stdout.splitlines(), start=1,
        ):
            line = raw_line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",")]
            first = parts[0]
            if first == "" or first.lower() in ("[n/a]", "n/a"):
                logger.warning(
                    "nvidia-smi line %d reported n/a power for GPU; "
                    "skipping.",
                    line_no,
                )
                continue
            try:
                value = float(first)
            except ValueError:
                msg = (
                    f"nvidia-smi line {line_no} power value not "
                    f"numeric: {first!r}."
                )
                if self._strict:
                    raise DCGMMeterError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            if not math.isfinite(value) or value < 0:
                msg = (
                    f"nvidia-smi line {line_no} reported invalid "
                    f"power {value!r}."
                )
                if self._strict:
                    raise DCGMMeterError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            per_gpu.append(value)

        if not per_gpu:
            msg = (
                "nvidia-smi returned no usable power samples"
                + (
                    f" for GPU index {gpu_index}."
                    if gpu_index is not None
                    else "."
                )
            )
            if self._strict:
                raise DCGMMeterError(msg)
            logger.warning(
                "%s Marking sample sanitized.", msg,
            )
        return per_gpu

    # ---------------------------------------------------------- props
    @property
    def gpu_config(self) -> DCGMMeterConfig:
        return self._gpu_config

    @property
    def binary_available(self) -> bool:
        return self._binary_available

    @property
    def last_sanitized(self) -> bool:
        return self._last_sanitized

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": self._gpu_config.to_dict(),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "binary_available": self._binary_available,
            "last_sanitized": self._last_sanitized,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "DCGMMeter("
            f"binary={self._gpu_config.binary!r}, "
            f"fields={list(self._gpu_config.query_fields)}, "
            f"gpu_index={self._gpu_config.gpu_index}, "
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

    def _single_gpu_runner(argv: Sequence[str], timeout: float):
        return 0, "112.5\n"

    def _multi_gpu_runner(argv: Sequence[str], timeout: float):
        return 0, "112.5\n95.0\n120.0\n101.5\n"

    def _flaky_runner(argv: Sequence[str], timeout: float):
        return 1, ""

    def _nan_runner(argv: Sequence[str], timeout: float):
        return 0, "[N/A]\n95.0\n"

    def _all_na_runner(argv: Sequence[str], timeout: float):
        return 0, "[N/A]\n[N/A]\n"

    # Single-GPU happy path
    meter = DCGMMeter(runner=_single_gpu_runner)
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.05)
    sample: Optional[TelemetrySample] = getattr(m, "last_sample", None)
    print("sample     :", sample)
    assert sample is not None
    assert sample.provenance is Provenance.MEASURED
    assert sample.sanitized is False
    assert sample.extras["samples_taken"] == 2
    assert sample.extras["samples_failed"] == 0
    print("extras     :", dict(sample.extras))

    # Multi-GPU
    multi = DCGMMeter(runner=_multi_gpu_runner)
    with multi.measure() as m:
        time.sleep(0.05)
    assert m.last_sample is not None
    assert m.last_sample.sanitized is False
    print("multi      :", m.last_sample)
    print("per-GPU    :", m.last_sample.extras.get("per_gpu_watts"))

    # Sampling loop: sample_count > 1
    loop_meter = DCGMMeter(
        config=DCGMMeterConfig(
            sample_count=3, sample_interval_seconds=0.01,
        ),
        runner=_multi_gpu_runner,
    )
    with loop_meter.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    # 1 from start() + 3 from stop() = 4 samples
    assert m.last_sample.extras["samples_taken"] == 4
    print("loop       :", m.last_sample)

    # Non-zero exit → strict raises
    try:
        bad = DCGMMeter(runner=_flaky_runner, strict=True)
        bad.start()
        bad.stop()
    except DCGMMeterError as exc:
        print("non-zero rc:", exc)
    else:
        raise AssertionError("strict flaky should have raised")

    # Non-zero exit → lenient returns 0 with sanitized=True
    lenient = DCGMMeter(
        runner=_flaky_runner,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with lenient.measure() as m:
        pass
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    assert m.last_sample.provenance is Provenance.ESTIMATED
    print("lenient    :", m.last_sample, "(sanitized=True)")

    # Partial [N/A] handling — still measured because the other sample was valid
    partial = DCGMMeter(
        runner=_nan_runner,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with partial.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    print("partial    :", m.last_sample)

    # All [N/A] → no usable samples → sanitized
    all_na = DCGMMeter(
        runner=_all_na_runner,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with all_na.measure() as m:
        pass
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    assert m.last_sample.provenance is Provenance.ESTIMATED
    print("all-N/A    :", m.last_sample, "(sanitized=True)")

    # Serialization
    payload = meter.to_json()
    assert json.loads(payload)["config"]["binary"] == "nvidia-smi"
    print("Serialization OK.")

    # Config validation
    bad_configs = (
        dict(binary=""),
        dict(query_fields=()),
        dict(query_fields=("temperature.gpu",), power_field="power.draw"),
        dict(gpu_index=-1),
        dict(gpu_index=True),
        dict(timeout_seconds=0),
        dict(timeout_seconds=True),
        dict(timeout_seconds=float("nan")),
        dict(sample_interval_seconds=-1.0),
        dict(sample_interval_seconds=float("nan")),
        dict(sample_count=0),
        dict(sample_count=True),
    )
    for bad_cfg in bad_configs:
        try:
            DCGMMeterConfig(**bad_cfg)  # type: ignore[arg-type]
        except DCGMMeterError as exc:
            print("Rejected cfg:", exc)
        else:
            raise AssertionError(
                f"config should have rejected {bad_cfg}"
            )

    # Real environment probe (skip on CI without GPU)
    if shutil.which("nvidia-smi") is not None:
        real = DCGMMeter(
            strict=False,
            base_config=BaseMeterConfig(enforce_contract=False),
        )
        with real.measure() as m:
            time.sleep(0.1)
        print("real host  :", m.last_sample)
    else:
        print("real host  : nvidia-smi not present (skipping live probe).")

    print("\nSmoke test passed.")
