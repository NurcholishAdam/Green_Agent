# src/instrumentation/energy/dcgm_gpu_meter.py

"""
DCGM GPU Meter
==============

Measures GPU power draw via NVIDIA DCGM (through ``nvidia-smi``).

Enhancements
------------
- ``DCGMMeterConfig`` — frozen, validated, centralizes binary path, query
  fields, timeout, and GPU index filtering.
- **Fixed multi-GPU crash** — parses every ``nvidia-smi`` output line into a
  per-GPU list instead of collapsing to a single ``float()``.
- **Fixed contract violation** — ``stop()`` now returns ``energy_joules`` in
  addition to ``power_watts`` and ``provenance``.
- Lifecycle tracking via ``_mark_started`` / ``_mark_stopped``.
- ``stop()`` result routed through ``BaseEnergyMeter._validate_metrics``.
- Robust subprocess handling: timeout, ``check=False``, decode errors,
  ``FileNotFoundError``, ``PermissionError``.
- ``RLock``-guarded ``start`` / ``stop`` with idempotent ``start()``.
- Custom ``DCGMMeterError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test with a mocked subprocess.
"""

from __future__ import annotations

import json
import logging
import math
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

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
class DCGMMeterError(InstrumentationError):
    """Raised for invalid DCGM meter inputs or runtime failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DCGMMeterConfig:
    """Tunable parameters for the DCGM GPU meter."""

    # Binary used to query the GPU. Discovered via ``shutil.which`` when
    # constructing the meter; this field records the resolved path.
    binary: str = "nvidia-smi"

    # Fields to query. Order matters: the parser assumes the CSV columns
    # come back in this exact order.
    query_fields: tuple = ("power.draw",)

    # Optional GPU index filter. ``None`` queries all GPUs.
    gpu_index: Optional[int] = None

    # Subprocess timeout (seconds). NVIDIA driver stalls are rare but real.
    timeout_seconds: float = 5.0

    # Sampling cadence for the mean-power derivation inside ``stop()``.
    # If < 0, power is taken directly from the last sample.
    sample_interval_seconds: float = 0.0

    # Number of samples to average when ``sample_interval_seconds > 0``.
    sample_count: int = 1

    # If a single-GPU host reports 0 W, treat the reading as suspect.
    # Measured provenance is still returned, but a warning is logged.
    zero_power_warning: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.binary, str) or not self.binary:
            raise DCGMMeterError("binary must be a non-empty string.")
        if not isinstance(self.query_fields, tuple) or not self.query_fields:
            raise DCGMMeterError(
                "query_fields must be a non-empty tuple of strings."
            )
        for field_name in self.query_fields:
            if not isinstance(field_name, str) or not field_name:
                raise DCGMMeterError(
                    "query_fields entries must be non-empty strings."
                )
        if self.gpu_index is not None and (
            not isinstance(self.gpu_index, int) or self.gpu_index < 0
        ):
            raise DCGMMeterError("gpu_index must be a non-negative int or None.")
        if self.timeout_seconds <= 0:
            raise DCGMMeterError("timeout_seconds must be > 0.")
        if self.sample_interval_seconds < 0:
            raise DCGMMeterError("sample_interval_seconds must be >= 0.")
        if self.sample_count <= 0:
            raise DCGMMeterError("sample_count must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class DCGMMeter(BaseEnergyMeter):
    """
    GPU power draw via NVIDIA DCGM (through ``nvidia-smi``).

    Parameters
    ----------
    config : DCGMMeterConfig, optional
        Meter configuration. Defaults to ``DCGMMeterConfig()``.
    base_config : BaseMeterConfig, optional
        Contract policy passed to :class:`BaseEnergyMeter`.
    strict : bool, default True
        If True, a missing binary or a query failure raises
        :class:`DCGMMeterError`. If False, ``stop()`` returns zeros.
    runner : callable, optional
        ``(argv, timeout) -> (returncode, stdout)`` callable replacing the
        real subprocess. Used to inject a mock in tests.
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

        # Resolve the binary unless a mock runner was provided.
        if runner is None:
            resolved = shutil.which(self._gpu_config.binary)
            if resolved is None:
                msg = (
                    f"'{self._gpu_config.binary}' not found on PATH; "
                    f"DCGM GPU metering is unavailable."
                )
                if self._strict:
                    raise DCGMMeterError(msg)
                logger.warning("%s DCGMMeter will return zeros.", msg)
                self._binary_available = False
            else:
                self._binary_available = True
                logger.debug("Resolved nvidia-smi to %s.", resolved)
        else:
            self._binary_available = True

        logger.debug(
            "DCGMMeter initialized (binary=%s, fields=%s, gpu_index=%s, "
            "timeout=%.2fs, strict=%s)",
            self._gpu_config.binary,
            list(self._gpu_config.query_fields),
            self._gpu_config.gpu_index,
            self._gpu_config.timeout_seconds,
            self._strict,
        )

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Mark the start of a measurement window."""
        with self._inner_lock:
            if self.started:
                logger.debug("DCGMMeter.start() called while already running.")
                return
            self._start_time = time.monotonic()
            self._samples = []
            self._mark_started()

            # Take an initial sample so ``stop()`` has at least one data
            # point even if the sampling loop is skipped.
            try:
                self._samples.append(self._sample_power_watts())
            except DCGMMeterError:
                if self._strict:
                    raise
                logger.warning("Initial DCGM sample failed; continuing.")
        logger.debug("DCGMMeter started.")

    def stop(self) -> Dict[str, Any]:
        """
        Stop the DCGM measurement window and return:

        ``{"energy_joules": float, "power_watts": float,
        "per_gpu_watts": [float, ...], "provenance": "measured"}``
        """
        with self._inner_lock:
            elapsed = self._mark_stopped()

            # Take one more sample so single-shot start/stop calls have data.
            try:
                self._samples.append(self._sample_power_watts())
            except DCGMMeterError:
                if self._strict:
                    raise
                logger.warning("Final DCGM sample failed; continuing.")

            # Average over the collected samples (may be a single value).
            if self._samples:
                mean_power = sum(self._samples) / len(self._samples)
                # The last raw sample is retained for per-GPU reporting.
                per_gpu = self._last_per_gpu_watts
            else:
                mean_power = 0.0
                per_gpu = []

            # Derive cumulative energy from the time-integrated power.
            energy_joules = max(0.0, mean_power * max(elapsed, 0.0))

            provenance = (
                "measured" if self._binary_available else "estimated"
            )

            raw = {
                "energy_joules": energy_joules,
                "power_watts": max(0.0, mean_power),
                "per_gpu_watts": list(per_gpu),
                "gpu_count": len(per_gpu),
                "provenance": provenance,
                "source": "dcgm",
                "elapsed_seconds": elapsed,
            }

        if (
            self._gpu_config.zero_power_warning
            and raw["power_watts"] == 0.0
            and self._binary_available
        ):
            logger.warning(
                "DCGMMeter reported 0 W; this may indicate an idle GPU, a "
                "MIG slice, or a driver that does not expose power.draw."
            )

        logger.debug(
            "DCGMMeter stopped: power=%.4g W, energy=%.6g J, elapsed=%.3fs, "
            "gpus=%d",
            raw["power_watts"],
            raw["energy_joules"],
            elapsed,
            raw["gpu_count"],
        )

        return self._validate_metrics(raw)

    # ---------------------------------------------------------- sampling
    def _sample_power_watts(self) -> float:
        """
        Take one ``nvidia-smi`` sample, parse it, store per-GPU values, and
        return the mean power across the queried GPUs.
        """
        try:
            per_gpu = self._run_query()
        except DCGMMeterError:
            raise
        except Exception as exc:
            raise DCGMMeterError(f"nvidia-smi sample failed: {exc}") from exc

        self._last_per_gpu_watts = per_gpu
        if not per_gpu:
            return 0.0
        return sum(per_gpu) / len(per_gpu)

    def _run_query(self) -> List[float]:
        """Invoke ``nvidia-smi`` and parse per-GPU power draws."""
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
                    check=False,  # inspect the return code ourselves
                )
            except FileNotFoundError as exc:
                raise DCGMMeterError(
                    f"'{cfg.binary}' not found on PATH."
                ) from exc
            except PermissionError as exc:
                raise DCGMMeterError(
                    f"'{cfg.binary}' is not executable by this user: {exc}"
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
        """Accept ``(rc, stdout)`` tuple, an object with those attrs, or a string."""
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
        """
        Parse ``nvidia-smi`` CSV output. One line per GPU; each line contains
        one or more comma-separated numeric fields.
        """
        per_gpu: List[float] = []
        gpu_index = self._gpu_config.gpu_index

        for line_no, raw_line in enumerate(stdout.splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",")]
            # First query field is the power draw.
            first = parts[0]
            if first == "" or first.lower() == "[n/a]" or first.lower() == "n/a":
                logger.warning(
                    "nvidia-smi line %d reported n/a power for GPU; skipping.",
                    line_no,
                )
                continue
            try:
                value = float(first)
            except ValueError:
                msg = (
                    f"nvidia-smi line {line_no} power value not numeric: "
                    f"{first!r}."
                )
                if self._strict:
                    raise DCGMMeterError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            if math.isnan(value) or math.isinf(value) or value < 0:
                msg = (
                    f"nvidia-smi line {line_no} reported invalid power "
                    f"{value!r}."
                )
                if self._strict:
                    raise DCGMMeterError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            per_gpu.append(value)

        if not per_gpu:
            msg = (
                "nvidia-smi returned no usable power samples"
                + (f" for GPU index {gpu_index}." if gpu_index is not None else ".")
            )
            if self._strict:
                raise DCGMMeterError(msg)
            logger.warning("%s Returning zeros.", msg)
        return per_gpu

    # ---------------------------------------------------------- props
    @property
    def gpu_config(self) -> DCGMMeterConfig:
        return self._gpu_config

    @property
    def binary_available(self) -> bool:
        return self._binary_available

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": asdict(self._gpu_config),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "binary_available": self._binary_available,
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
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DCGMMeter",
    "DCGMMeterConfig",
    "DCGMMeterError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.energy.dcgm_gpu_meter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---------------------------------------------------------------- #
    # Mock runner — simulates a single-GPU and multi-GPU host.
    # ---------------------------------------------------------------- #
    def _single_gpu_runner(argv: Sequence[str], timeout: float):
        return 0, "112.5\n"

    def _multi_gpu_runner(argv: Sequence[str], timeout: float):
        # Four GPUs, one line each.
        return 0, "112.5\n95.0\n120.0\n101.5\n"

    def _flaky_runner(argv: Sequence[str], timeout: float):
        return 1, ""  # non-zero exit

    def _nan_runner(argv: Sequence[str], timeout: float):
        # Driver sometimes reports [N/A] when the GPU is in a low-power state.
        return 0, "[N/A]\n95.0\n"

    # ---- Single-GPU happy path ---------------------------------------- #
    meter = DCGMMeter(runner=_single_gpu_runner)
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.05)
    sample: Optional[TelemetrySample] = getattr(m, "last_sample", None)
    print("sample     :", sample)
    if sample is not None:
        print("dict       :", sample.to_dict())

    # ---- Multi-GPU (the original bug) --------------------------------- #
    multi = DCGMMeter(runner=_multi_gpu_runner)
    with multi.measure() as m:
        time.sleep(0.05)
    print("multi      :", m.last_sample)
    if m.last_sample is not None:
        print("per-GPU    :", m.last_sample.extras.get("per_gpu_watts"))

    # ---- Explicit start / stop ---------------------------------------- #
    legacy = DCGMMeter(runner=_multi_gpu_runner)
    legacy.start()
    time.sleep(0.02)
    metrics = legacy.stop()
    print("legacy stop:", {k: metrics[k] for k in (
        "energy_joules", "power_watts", "provenance"
    )})
    assert "energy_joules" in metrics, "contract requires energy_joules"
    assert "power_watts" in metrics

    # ---- Non-zero exit → strict raises -------------------------------- #
    try:
        bad = DCGMMeter(runner=_flaky_runner, strict=True)
        bad.start()
        bad.stop()
    except DCGMMeterError as exc:
        print("non-zero rc:", exc)

    # ---- Non-zero exit → lenient returns zeros ----------------------- #
    lenient = DCGMMeter(runner=_flaky_runner, strict=False)
    with lenient.measure() as m:
        pass
    print("lenient    :", m.last_sample)

    # ---- Partial [N/A] handling --------------------------------------- #
    partial = DCGMMeter(runner=_nan_runner, strict=False)
    with partial.measure() as m:
        time.sleep(0.01)
    print("partial    :", m.last_sample)

    # ---- Serialization round-trip ------------------------------------- #
    payload = meter.to_json()
    assert json.loads(payload)["config"]["binary"] == "nvidia-smi"
    print("Serialization OK.")

    # ---- Config validation -------------------------------------------- #
    for bad_cfg in (
        dict(binary=""),
        dict(query_fields=()),
        dict(gpu_index=-1),
        dict(timeout_seconds=0),
        dict(sample_interval_seconds=-1.0),
        dict(sample_count=0),
    ):
        try:
            DCGMMeterConfig(**bad_cfg)  # type: ignore[arg-type]
        except DCGMMeterError as exc:
            print("Rejected cfg:", exc)

    # ---- Real environment probe (only if nvidia-smi is installed) ----- #
    if shutil.which("nvidia-smi") is not None:
        real = DCGMMeter(strict=False)  # tolerate a missing/blocked GPU
        with real.measure() as m:
            time.sleep(0.1)
        print("real host  :", m.last_sample)
    else:
        print("real host  : nvidia-smi not present (skipping live probe).")

    print("\nSmoke test passed.")
