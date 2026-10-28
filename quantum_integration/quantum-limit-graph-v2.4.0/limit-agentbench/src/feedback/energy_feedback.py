# src/feedback/energy_feedback.py

"""
Energy Feedback — v2.0.0
========================

Human-readable feedback based on energy efficiency.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``_extract_number`` rejects negative values. Previously
        ``analyze_energy_metrics({"energy": -5.0})`` returned
        ``energy=-5.0`` in the result — corrupted for downstream consumers
        and invisible to ``is_efficient``.
  * 2.2 ``EnergyFeedbackConfig`` rejects ``bool`` for numeric fields
        (``isinstance(True, (int, float))`` is ``True`` — the recurring
        pattern from every prior DPQ file).
  * 2.3 ``EnergyFeedbackConfig`` rejects ``NaN`` / ``Inf``. Previously
        ``high_energy_threshold=float("nan")`` passed validation and the
        threshold silently never fired (``x > nan`` is ``False``).
  * 2.4 ``EnergyFeedback`` validates every field via ``__post_init__``.
        Previously ``messages="not-a-tuple"`` produced
        ``['n','o','t',...]`` in ``to_dict()`` and
        ``"n o t - a - t u p l e"`` in ``to_text()``.
  * 2.5 ``analyze_energy_metrics({})`` no longer reports
        ``is_efficient=True`` as if it were a verified efficient run. The
        result carries ``evaluated=()``, ``coerced=("energy","latency","memory")``,
        ``has_data=False``, and ``to_text()`` returns
        ``"No metrics available for evaluation."``.

**P1 robustness / observability**
  * 3.1 ``EnergyFeedbackConfig.from_legacy_dict`` — the recurring
        config-drift fix used in every prior DPQ / distributed file.
  * 3.2 ``EnergyFeedbackConfig.to_dict`` / ``from_dict`` — symmetry.
  * 3.3 ``EnergyFeedback.from_dict`` mirrors ``to_dict``.
  * 3.4 Per-metric ``energy_ok`` / ``latency_ok`` / ``memory_ok`` flags
        so callers don't have to inspect ``messages`` for substrings.
  * 3.5 ``has_data`` property — the honest "is there any signal here?".
  * 3.6 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.7 ``__version__`` exported.
  * 3.8 ``analyze_batch(metrics_list, ...)`` for batch inputs.
  * 3.9 ``messages: Tuple[str, ...]`` (was bare ``tuple``).
  * 3.10 ``evaluated`` / ``coerced`` fields make defaulting visible.

**P2 polish**
  * ``__str__`` returns ``to_text()`` — ``print(feedback)`` is readable.
  * Message strings exported as module constants (for tests and i18n).
  * ``__all__`` includes ``__version__`` and the new symbols.

Backward compatible: every v1.0.0 field, method, and payload works
unchanged. ``to_text()`` preserves the v1.0.0 "efficient" message when
the caller directly constructs ``EnergyFeedback(messages=())`` — the
"No data" message only appears when ``analyze_energy_metrics`` coerced
every metric.
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass, fields
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Message constants (P2 — exported for tests and i18n)
# --------------------------------------------------------------------------- #
MSG_HIGH_ENERGY = "High energy usage detected."
MSG_HIGH_LATENCY = "Latency may impact user experience."
MSG_HIGH_MEMORY = "Large memory footprint observed."
MSG_EFFICIENT = "Run is efficient and within green thresholds."
MSG_NO_DATA = "No metrics available for evaluation."


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EnergyFeedbackError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EnergyFeedbackConfig:
    """Tunable thresholds for energy feedback generation."""

    high_energy_threshold: float = 0.1     # kWh
    high_latency_threshold: float = 2.0    # seconds
    high_memory_threshold: float = 1024.0  # MB

    def __post_init__(self) -> None:
        for name in (
            "high_energy_threshold",
            "high_latency_threshold",
            "high_memory_threshold",
        ):
            value = getattr(self, name)
            # P0 2.2: reject bool (passes isinstance(x, (int, float))).
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise EnergyFeedbackError(
                    f"{name} must be numeric, "
                    f"got {type(value).__name__}.")
            # P0 2.3: reject NaN/Inf.
            if math.isnan(value) or math.isinf(value):
                raise EnergyFeedbackError(
                    f"{name} must be finite, got {value!r}.")
            if value < 0:
                raise EnergyFeedbackError(
                    f"{name} must be >= 0, got {value}.")

    # ---------------------------------------------------------------- helpers
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "EnergyFeedbackConfig":
        """P1 3.1: build from a flat mapping; unknown keys ignored."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})

    def to_dict(self) -> Dict[str, Any]:
        """P1 3.2: mirror of ``from_dict``."""
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Optional[Mapping[str, Any]]
    ) -> "EnergyFeedbackConfig":
        return cls.from_legacy_dict(data)


# --------------------------------------------------------------------------- #
# Structured result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EnergyFeedback:
    """Structured energy feedback result."""

    messages: Tuple[str, ...]
    is_efficient: bool
    energy: float
    latency: float
    memory: float

    # v2.0.0 additions (defaults preserve v1.0.0 shape for callers who
    # construct the dataclass directly).
    evaluated: Tuple[str, ...] = ()
    coerced: Tuple[str, ...] = ()
    energy_ok: bool = True
    latency_ok: bool = True
    memory_ok: bool = True

    def __post_init__(self) -> None:
        # P0 2.4: full structural validation.
        if not isinstance(self.messages, tuple):
            raise EnergyFeedbackError(
                f"messages must be a tuple, "
                f"got {type(self.messages).__name__}.")
        for i, m in enumerate(self.messages):
            if not isinstance(m, str):
                raise EnergyFeedbackError(
                    f"messages[{i}] must be a string, "
                    f"got {type(m).__name__}.")
        if not isinstance(self.is_efficient, bool):
            raise EnergyFeedbackError(
                f"is_efficient must be a bool, "
                f"got {type(self.is_efficient).__name__}.")
        for name in ("energy", "latency", "memory"):
            value = getattr(self, name)
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise EnergyFeedbackError(
                    f"{name} must be numeric, "
                    f"got {type(value).__name__}.")
            if math.isnan(value) or math.isinf(value):
                raise EnergyFeedbackError(f"{name} must be finite.")
            if value < 0:
                raise EnergyFeedbackError(f"{name} must be >= 0.")
        for name in ("evaluated", "coerced"):
            val = getattr(self, name)
            if not isinstance(val, tuple):
                raise EnergyFeedbackError(
                    f"{name} must be a tuple, "
                    f"got {type(val).__name__}.")
            for i, item in enumerate(val):
                if not isinstance(item, str):
                    raise EnergyFeedbackError(
                        f"{name}[{i}] must be a string, "
                        f"got {type(item).__name__}.")
        for name in ("energy_ok", "latency_ok", "memory_ok"):
            val = getattr(self, name)
            if not isinstance(val, bool):
                raise EnergyFeedbackError(
                    f"{name} must be a bool, "
                    f"got {type(val).__name__}.")

    # ---------------------------------------------------------------- props
    @property
    def has_data(self) -> bool:
        """P1 3.5: True iff at least one metric was validated (not
        defaulted by ``analyze_energy_metrics``)."""
        return len(self.evaluated) > 0

    # ---------------------------------------------------------------- output
    def to_dict(self) -> Dict[str, Any]:
        return {
            "messages": list(self.messages),
            "is_efficient": self.is_efficient,
            "energy": self.energy,
            "latency": self.latency,
            "memory": self.memory,
            "evaluated": list(self.evaluated),
            "coerced": list(self.coerced),
            "energy_ok": self.energy_ok,
            "latency_ok": self.latency_ok,
            "memory_ok": self.memory_ok,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnergyFeedback":
        """P1 3.3: mirror of ``to_dict``."""
        if not isinstance(data, Mapping):
            raise EnergyFeedbackError(
                f"EnergyFeedback.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        return cls(
            messages=tuple(data.get("messages", ())),
            is_efficient=bool(data["is_efficient"]),
            energy=float(data["energy"]),
            latency=float(data["latency"]),
            memory=float(data["memory"]),
            evaluated=tuple(data.get("evaluated", ())),
            coerced=tuple(data.get("coerced", ())),
            energy_ok=bool(data.get("energy_ok", True)),
            latency_ok=bool(data.get("latency_ok", True)),
            memory_ok=bool(data.get("memory_ok", True)),
        )

    def to_text(self) -> str:
        """
        Return the human-readable feedback string (legacy format).

        P0 2.5: ``analyze_energy_metrics`` populates ``coerced`` when a
        metric had to be defaulted. If every metric was coerced, we have
        no measurements to certify — return ``MSG_NO_DATA`` rather than
        ``MSG_EFFICIENT``. Direct construction with ``coerced=()``
        preserves v1.0.0's "efficient" behaviour for backward
        compatibility.
        """
        if self.messages:
            return " ".join(self.messages)
        if self.has_data:
            return MSG_EFFICIENT
        if self.coerced:
            return MSG_NO_DATA
        # Backward-compatible path: caller constructed directly with
        # no data flags — treat as v1.0.0 "efficient" for compat.
        return MSG_EFFICIENT

    def __str__(self) -> str:
        """P2: ``print(feedback)`` is human-readable."""
        return self.to_text()

    def __repr__(self) -> str:
        return (
            "EnergyFeedback("
            f"efficient={self.is_efficient}, "
            f"has_data={self.has_data}, "
            f"energy={self.energy:.4f}, latency={self.latency:.2f}, "
            f"memory={self.memory:.1f})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def _extract_number(
    metrics: Mapping[str, Any],
    key: str,
    default: float,
) -> Tuple[float, bool]:
    """
    Return ``(value, present)``.

    ``present`` is True iff ``key`` was present, numeric (rejecting
    ``bool``), finite (rejecting ``NaN``/``Inf``), and non-negative
    (P0 2.1). Otherwise returns ``(default, False)`` and logs the reason
    at DEBUG.
    """
    value = metrics.get(key, default)
    if isinstance(value, bool) or \
            not isinstance(value, (int, float)):
        logger.debug(
            "metrics[%r] is not numeric; using default %.2f.",
            key, default)
        return default, False
    f = float(value)
    if math.isnan(f) or math.isinf(f):
        logger.debug(
            "metrics[%r] is non-finite; using default %.2f.",
            key, default)
        return default, False
    # P0 2.1: reject negatives (previously slipped through).
    if f < 0:
        logger.debug(
            "metrics[%r] is negative; using default %.2f.",
            key, default)
        return default, False
    return f, True


def analyze_energy_metrics(
    metrics: Mapping[str, Any],
    *,
    config: Optional[EnergyFeedbackConfig] = None,
    strict: bool = True,
    metrics_hook: Optional[
        Callable[[str, Mapping[str, Any]], None]
    ] = None,
) -> EnergyFeedback:
    """
    Build a structured :class:`EnergyFeedback` from a metrics mapping.

    Parameters
    ----------
    metrics : Mapping
        Recognized optional keys: ``energy``, ``latency``, ``memory``.
        Missing / non-numeric / negative values are coerced to ``0.0``
        and flagged in the returned ``coerced`` tuple.
    config : EnergyFeedbackConfig, optional
        Thresholds. Defaults to ``EnergyFeedbackConfig()``.
    strict : bool, default True
        If True, a non-Mapping ``metrics`` raises
        ``EnergyFeedbackError``.
    metrics_hook : callable, optional
        Called as ``hook(event, fields_dict)`` after analysis.
        Exceptions from the hook are swallowed.
    """
    cfg = config or EnergyFeedbackConfig()

    if not isinstance(metrics, Mapping):
        msg = (
            f"metrics must be a Mapping, got "
            f"{type(metrics).__name__}.")
        if strict:
            raise EnergyFeedbackError(msg)
        logger.warning("%s Using empty mapping.", msg)
        metrics = {}

    energy, energy_present = _extract_number(metrics, "energy", 0.0)
    latency, latency_present = _extract_number(metrics, "latency", 0.0)
    memory, memory_present = _extract_number(metrics, "memory", 0.0)

    evaluated: List[str] = []
    coerced: List[str] = []
    for name, present in (
        ("energy", energy_present),
        ("latency", latency_present),
        ("memory", memory_present),
    ):
        if present:
            evaluated.append(name)
        else:
            coerced.append(name)

    energy_ok = energy <= cfg.high_energy_threshold
    latency_ok = latency <= cfg.high_latency_threshold
    memory_ok = memory <= cfg.high_memory_threshold

    messages: List[str] = []
    if not energy_ok:
        messages.append(MSG_HIGH_ENERGY)
    if not latency_ok:
        messages.append(MSG_HIGH_LATENCY)
    if not memory_ok:
        messages.append(MSG_HIGH_MEMORY)

    result = EnergyFeedback(
        messages=tuple(messages),
        is_efficient=not messages,
        energy=energy,
        latency=latency,
        memory=memory,
        evaluated=tuple(evaluated),
        coerced=tuple(coerced),
        energy_ok=energy_ok,
        latency_ok=latency_ok,
        memory_ok=memory_ok,
    )

    # P1 3.6: metrics hook (best-effort).
    if metrics_hook is not None:
        try:
            metrics_hook("feedback_generated", {
                "is_efficient": result.is_efficient,
                "has_data": result.has_data,
                "evaluated_count": len(result.evaluated),
                "coerced_count": len(result.coerced),
                "messages_count": len(result.messages),
                "energy": result.energy,
                "latency": result.latency,
                "memory": result.memory,
            })
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    return result


def generate_energy_feedback(
    metrics: Mapping[str, Any],
    *,
    config: Optional[EnergyFeedbackConfig] = None,
    strict: bool = True,
    metrics_hook: Optional[
        Callable[[str, Mapping[str, Any]], None]
    ] = None,
) -> str:
    """
    Return a human-readable energy feedback string
    (backward-compatible).

    Parameters match :func:`analyze_energy_metrics`.
    """
    result = analyze_energy_metrics(
        metrics, config=config, strict=strict, metrics_hook=metrics_hook)
    text = result.to_text()
    logger.debug("Energy feedback: %s", text)
    return text


def analyze_batch(
    metrics_list: List[Mapping[str, Any]],
    *,
    config: Optional[EnergyFeedbackConfig] = None,
    strict: bool = True,
    metrics_hook: Optional[
        Callable[[str, Mapping[str, Any]], None]
    ] = None,
) -> List[EnergyFeedback]:
    """P1 3.8: analyze a list of metrics mappings."""
    if not isinstance(metrics_list, list):
        raise EnergyFeedbackError(
            f"metrics_list must be a list, "
            f"got {type(metrics_list).__name__}.")
    return [
        analyze_energy_metrics(
            m, config=config, strict=strict, metrics_hook=metrics_hook)
        for m in metrics_list
    ]


__all__ = [
    "__version__",
    "EnergyFeedback",
    "EnergyFeedbackConfig",
    "EnergyFeedbackError",
    "MSG_EFFICIENT",
    "MSG_HIGH_ENERGY",
    "MSG_HIGH_LATENCY",
    "MSG_HIGH_MEMORY",
    "MSG_NO_DATA",
    "analyze_batch",
    "analyze_energy_metrics",
    "generate_energy_feedback",
]


# --------------------------------------------------------------------------- #
# Self-tests
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== energy_feedback v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — negative energy coerced, not returned
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: negative energy coerced ===")
    fb = analyze_energy_metrics({"energy": -5.0})
    assert fb.energy == 0.0, (
        f"P0 2.1 regression: energy={fb.energy}, expected 0.0")
    assert "energy" in fb.coerced, (
        f"P0 2.1 regression: 'energy' not in coerced={fb.coerced}")
    assert "energy" not in fb.evaluated, (
        f"P0 2.1 regression: 'energy' in evaluated={fb.evaluated}")
    print(f"  ✓ energy=-5.0 → 0.0 (coerced), "
          f"evaluated={fb.evaluated}, coerced={fb.coerced}")

    # Negative latency and memory also caught.
    fb2 = analyze_energy_metrics({"latency": -1.0, "memory": -100.0})
    assert fb2.latency == 0.0
    assert fb2.memory == 0.0
    assert set(fb2.coerced) == {"energy", "latency", "memory"}
    print(f"  ✓ negative latency/memory also coerced")

    # ---------------------------------------------------------------- #
    # P0 2.2 — bool rejected for config
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: bool rejected for config ===")
    try:
        EnergyFeedbackConfig(high_energy_threshold=True)
    except EnergyFeedbackError as exc:
        print(f"  ✓ True rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.2 regression: high_energy_threshold=True accepted")

    try:
        EnergyFeedbackConfig(high_latency_threshold=False)
    except EnergyFeedbackError:
        print("  ✓ False rejected")
    else:
        raise AssertionError(
            "P0 2.2 regression: high_latency_threshold=False accepted")

    # ---------------------------------------------------------------- #
    # P0 2.3 — NaN/Inf rejected for config
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: NaN/Inf rejected for config ===")
    for bad in (float("nan"), float("inf"), float("-inf")):
        try:
            EnergyFeedbackConfig(high_energy_threshold=bad)
        except EnergyFeedbackError as exc:
            print(f"  ✓ {bad!r} rejected: {exc}")
        else:
            raise AssertionError(f"P0 2.3 regression: {bad!r} accepted")

    # ---------------------------------------------------------------- #
    # P0 2.4 — EnergyFeedback structural validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: EnergyFeedback structural validation ===")

    # messages="not-a-tuple" rejected
    try:
        EnergyFeedback(
            messages="not-a-tuple",   # type: ignore[arg-type]
            is_efficient=True,
            energy=0.0, latency=0.0, memory=0.0,
        )
    except EnergyFeedbackError as exc:
        print(f"  ✓ messages as string rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.4 regression: string messages accepted")

    # is_efficient="yes" rejected
    try:
        EnergyFeedback(
            messages=("msg",),
            is_efficient="yes",   # type: ignore[arg-type]
            energy=0.0, latency=0.0, memory=0.0,
        )
    except EnergyFeedbackError as exc:
        print(f"  ✓ is_efficient as string rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.4 regression: string is_efficient accepted")

    # energy=nan rejected
    try:
        EnergyFeedback(
            messages=(),
            is_efficient=True,
            energy=float("nan"),
            latency=0.0, memory=0.0,
        )
    except EnergyFeedbackError:
        print("  ✓ NaN energy rejected")
    else:
        raise AssertionError(
            "P0 2.4 regression: NaN energy accepted")

    # Negative energy rejected
    try:
        EnergyFeedback(
            messages=(),
            is_efficient=True,
            energy=-1.0,
            latency=0.0, memory=0.0,
        )
    except EnergyFeedbackError:
        print("  ✓ negative energy rejected")
    else:
        raise AssertionError(
            "P0 2.4 regression: negative energy accepted")

    # Valid construction succeeds.
    valid = EnergyFeedback(
        messages=("test",),
        is_efficient=False,
        energy=0.5, latency=1.0, memory=100.0,
    )
    assert valid.messages == ("test",)
    print("  ✓ valid construction accepted")

    # ---------------------------------------------------------------- #
    # P0 2.5 — empty metrics has_data=False, to_text() = MSG_NO_DATA
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: empty metrics has_data=False ===")

    empty = analyze_energy_metrics({})
    assert empty.evaluated == (), (
        f"P0 2.5 regression: evaluated={empty.evaluated}, expected ()")
    assert empty.coerced == ("energy", "latency", "memory")
    assert empty.has_data is False, (
        f"P0 2.5 regression: has_data={empty.has_data}")
    assert empty.to_text() == MSG_NO_DATA, (
        f"P0 2.5 regression: to_text()={empty.to_text()!r}, "
        f"expected {MSG_NO_DATA!r}")
    print(f"  ✓ {{}}: evaluated=(), has_data=False, "
          f"to_text()={empty.to_text()!r}")

    # Genuinely-zero metrics still evaluated.
    zeros = analyze_energy_metrics(
        {"energy": 0.0, "latency": 0.0, "memory": 0.0})
    assert zeros.evaluated == ("energy", "latency", "memory"), (
        f"P0 2.5 regression: evaluated={zeros.evaluated}")
    assert zeros.coerced == ()
    assert zeros.has_data is True, (
        f"P0 2.5 regression: has_data={zeros.has_data}")
    assert zeros.is_efficient is True, (
        f"P0 2.5 regression: is_efficient={zeros.is_efficient}")
    assert zeros.to_text() == MSG_EFFICIENT
    print(f"  ✓ {{energy:0, latency:0, memory:0}}: "
          f"evaluated=('energy','latency','memory'), "
          f"has_data=True, is_efficient=True")

    # String API matches.
    assert generate_energy_feedback({}) == MSG_NO_DATA
    assert generate_energy_feedback(
        {"energy": 0.0, "latency": 0.0, "memory": 0.0}
    ) == MSG_EFFICIENT
    print(f"  ✓ generate_energy_feedback matches analyze path")

    # ---------------------------------------------------------------- #
    # P1 3.1 — EnergyFeedbackConfig.from_legacy_dict
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: EnergyFeedbackConfig.from_legacy_dict ===")
    cfg = EnergyFeedbackConfig.from_legacy_dict(
        {"high_energy_threshold": 0.5})
    assert cfg.high_energy_threshold == 0.5, (
        f"P1 3.1 regression: high_energy_threshold="
        f"{cfg.high_energy_threshold}")
    assert cfg.high_latency_threshold == 2.0, (
        f"P1 3.1 regression: high_latency_threshold="
        f"{cfg.high_latency_threshold}, expected 2.0 (default)")
    assert cfg.high_memory_threshold == 1024.0
    print(f"  ✓ explicit 0.5 preserved; other two default")

    # None / empty / unknown keys.
    assert EnergyFeedbackConfig.from_legacy_dict(None) == \
        EnergyFeedbackConfig()
    assert EnergyFeedbackConfig.from_legacy_dict({}) == \
        EnergyFeedbackConfig()
    cfg2 = EnergyFeedbackConfig.from_legacy_dict({
        "high_energy_threshold": 0.5,
        "unknown_field": 999,
    })
    assert cfg2.high_energy_threshold == 0.5
    print(f"  ✓ None / {{}} → defaults; unknown keys ignored")

    # ---------------------------------------------------------------- #
    # P1 3.2 — config to_dict / from_dict round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: config to_dict / from_dict ===")
    cfg3 = EnergyFeedbackConfig(high_energy_threshold=0.5)
    assert cfg3.to_dict() == {
        "high_energy_threshold": 0.5,
        "high_latency_threshold": 2.0,
        "high_memory_threshold": 1024.0,
    }
    assert EnergyFeedbackConfig.from_dict(cfg3.to_dict()) == cfg3
    print(f"  ✓ config round-trip OK")

    # ---------------------------------------------------------------- #
    # P1 3.3 — EnergyFeedback.from_dict round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: EnergyFeedback.from_dict round-trip ===")
    original = analyze_energy_metrics(
        {"energy": 0.5, "latency": 0.0, "memory": 100.0})
    payload = original.to_dict()
    restored = EnergyFeedback.from_dict(payload)
    assert restored == original, (
        f"P1 3.3 regression: round-trip mismatch\n"
        f"original: {original}\n"
        f"restored: {restored}")
    assert restored.evaluated == original.evaluated
    assert restored.coerced == original.coerced
    assert restored.energy_ok == original.energy_ok
    assert restored.latency_ok == original.latency_ok
    assert restored.memory_ok == original.memory_ok
    print(f"  ✓ round-trip: evaluated={restored.evaluated}, "
          f"coerced={restored.coerced}, "
          f"flags=(energy_ok={restored.energy_ok}, "
          f"latency_ok={restored.latency_ok}, "
          f"memory_ok={restored.memory_ok})")

    # ---------------------------------------------------------------- #
    # P1 3.4 — individual *_ok flags
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: individual *_ok flags ===")
    fb = analyze_energy_metrics(
        {"energy": 0.5, "latency": 0.0, "memory": 0.0})
    assert fb.energy_ok is False, (
        f"P1 3.4 regression: energy_ok={fb.energy_ok}, "
        f"expected False (0.5 > 0.1)")
    assert fb.latency_ok is True, (
        f"P1 3.4 regression: latency_ok={fb.latency_ok}")
    assert fb.memory_ok is True, (
        f"P1 3.4 regression: memory_ok={fb.memory_ok}")
    print(f"  ✓ energy=0.5: energy_ok=False, "
          f"latency_ok=True, memory_ok=True")

    # Multiple violations.
    fb2 = analyze_energy_metrics(
        {"energy": 1.0, "latency": 5.0, "memory": 2000.0})
    assert fb2.energy_ok is False
    assert fb2.latency_ok is False
    assert fb2.memory_ok is False
    assert len(fb2.messages) == 3
    print(f"  ✓ all three violated: all *_ok=False, "
          f"{len(fb2.messages)} messages")

    # ---------------------------------------------------------------- #
    # Bonus: __str__ returns to_text()
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: __str__ == to_text() ===")
    fb = analyze_energy_metrics({"energy": 0.5})
    assert str(fb) == fb.to_text(), (
        f"__str__ regression: {str(fb)!r} != {fb.to_text()!r}")
    assert MSG_HIGH_ENERGY in str(fb)
    print(f"  ✓ str(feedback) = {str(fb)!r}")

    # ---------------------------------------------------------------- #
    # Bonus: metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: metrics_hook ===")
    events = []
    analyze_energy_metrics(
        {"energy": 0.5},
        metrics_hook=lambda ev, f: events.append((ev, f)))
    assert len(events) == 1
    event_name, event_fields = events[0]
    assert event_name == "feedback_generated"
    assert "is_efficient" in event_fields
    assert event_fields["has_data"] is True
    print(f"  ✓ metrics_hook captured {event_name} with "
          f"{len(event_fields)} fields")

    # ---------------------------------------------------------------- #
    # Bonus: analyze_batch
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: analyze_batch ===")
    results = analyze_batch([
        {"energy": 0.05},
        {"energy": 0.5},
        {},
    ])
    assert len(results) == 3
    assert results[0].is_efficient is True
    assert results[1].is_efficient is False
    assert results[2].has_data is False
    assert results[2].to_text() == MSG_NO_DATA
    print(f"  ✓ batch: 3 results — efficient / violation / no-data")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 style construction still works
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat: v1.0.0 construction ===")

    # A caller who only knows about v1.0.0 fields.
    legacy_feedback = EnergyFeedback(
        messages=("custom message",),
        is_efficient=False,
        energy=0.5,
        latency=1.0,
        memory=100.0,
    )
    # New fields defaulted.
    assert legacy_feedback.evaluated == ()
    assert legacy_feedback.coerced == ()
    assert legacy_feedback.energy_ok is True
    assert legacy_feedback.has_data is False
    # Messages explicitly provided → preserved.
    assert legacy_feedback.to_text() == "custom message"
    print(f"  ✓ legacy construction: new fields defaulted, "
          f"messages preserved")

    # Legacy construction with empty messages (v1.0.0 default "efficient").
    legacy_empty = EnergyFeedback(
        messages=(),
        is_efficient=True,
        energy=0.0, latency=0.0, memory=0.0,
    )
    # coerced=() distinguishes this from analyze_energy_metrics({}).
    assert legacy_empty.to_text() == MSG_EFFICIENT, (
        f"Backward-compat regression: direct empty construction "
        f"should return MSG_EFFICIENT (v1.0.0 behaviour), "
        f"got {legacy_empty.to_text()!r}")
    print(f"  ✓ direct empty construction → MSG_EFFICIENT "
          f"(v1.0.0 behaviour preserved)")

    # Old payload (no new fields).
    old_payload = {
        "messages": ["High energy usage detected."],
        "is_efficient": False,
        "energy": 0.5,
        "latency": 1.0,
        "memory": 100.0,
    }
    restored_old = EnergyFeedback.from_dict(old_payload)
    assert restored_old.evaluated == ()
    assert restored_old.coerced == ()
    assert restored_old.has_data is False
    # messages present → returned.
    assert restored_old.to_text() == "High energy usage detected."
    print(f"  ✓ old payload loads with new fields defaulted")

    print("\n✓ All self-tests passed.")
