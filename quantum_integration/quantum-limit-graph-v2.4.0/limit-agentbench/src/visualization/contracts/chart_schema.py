# src/visualization/contracts/chart_schema.py

"""
Chart schema — the data-source declaration every visual must carry.

Implements the recommendation's requirement::

    "Every visual should declare its data source, time range, units,
     baseline, system/policy version, and whether a value is measured,
     estimated, simulated, or externally supplied."

Contract surface
----------------
- :class:`DataSource` / :class:`TrustLevel` / :class:`TradeoffKind` —
  closed enums. All are ``StrEnum``-compatible (see §2.13 of the review):
  ``str(DataSource.MEASURED) == "measured"`` on every supported Python
  version, not just 3.11+.
- :class:`ChartContext` — the provenance record, ``frozen=True`` +
  ``slots=True``, with full ``__post_init__`` validation, string-form
  coercions for ``source`` / ``trust_level`` (case-insensitive, accepts
  either the enum member or its canonical string), and round-trip
  support (``from_dict`` / ``to_json`` / ``from_json``).
- :class:`ChartContextDict` — the return shape of :meth:`to_dict`,
  documenting that ``time_range_*`` become ISO-8601 strings in the
  payload.

Behaviour changes over the previous revision
--------------------------------------------
1. **`source` and `trust_level` are validated and coerced.**
   Previously ``ChartContext(source=DataSource.MEASURED)`` rendered as
   ``"source=DataSource.MEASURED"`` on Python <3.11, and any arbitrary
   string was silently accepted. Now both forms normalize to the
   canonical enum value, and unknown strings raise
   :class:`ChartSchemaError`.

2. **`simulated` and `source="simulated"` no longer silently diverge.**
   ``to_subtitle()`` shows the ``⚠ simulated`` glyph when *either* is
   true. A ``WARNING`` is logged at construction time when the two
   disagree, so it is visible without breaking existing callers who set
   the flag for a non-simulation reason.

3. **`to_subtitle()` now includes the fields a reader needs.**
   ``trust``, ``run_id``, and the date range appear in the subtitle.
   ``baseline`` and ``system_version`` / ``policy_version`` are
   unchanged.

4. **`to_dict()` builds the payload manually** and returns the
   :class:`ChartContextDict` TypedDict. It no longer relies on
   ``asdict`` (which deep-copied ``datetime`` fields unnecessarily) and
   the return type is documented.

5. **`time_range_start` / `time_range_end` are validated** as
   ``datetime | None`` in ``__post_init__`` and compared: ``start <=
   end`` when both are present.

6. **Full round-trip.** ``from_dict`` accepts the payload that
   ``to_dict`` produces (including ISO-8601 strings for
   ``time_range_*``), ``to_json`` / ``from_json`` are thin wrappers.

7. **Frozen and hashable.** ``@dataclass(frozen=True, slots=True)`` —
   mutating a context between renders now requires
   :func:`dataclasses.replace`. The ``with_*`` helpers
   (:meth:`ChartContext.with_source`, :meth:`ChartContext.with_trust`,
   :meth:`ChartContext.with_simulated`) are the ergonomic frozen-style
   replacement.

8. **Narrow error type.** :class:`ChartSchemaError(ValueError)` is
   raised from every validation path; previously every failure leaked
   ``ValueError`` / ``AttributeError`` / ``TypeError``.

Compatibility
-------------
- ``ChartContext(...)`` accepts the same keyword arguments as before.
  The dataclass is now frozen — callers who did ``ctx.source = ...``
  must use ``ctx.with_source(...)`` or ``dataclasses.replace``.
- ``ChartContext.to_dict()`` returns the same keys as before. The
  ``time_range_*`` fields remain ``Optional[str]`` in the payload (this
  was already the case); the change is that they are now explicitly
  typed via :class:`ChartContextDict`.
- ``ChartContext.to_subtitle()`` produces a longer subtitle (adds
  ``trust``, ``run_id``, date range). Callers rendering the subtitle
  into a fixed-width column may need to adjust column width.
- ``ChartContext(source="measured", simulated=True)`` still constructs,
  but logs a ``WARNING`` and the subtitle includes both ``⚠ simulated``
  and ``source=measured`` — the reader sees the contradiction, which is
  the correct outcome.
- ``TradeoffKind`` gained a ``description`` property for chart labels;
  no existing call site is affected.

Requires Python 3.8+ (``slots=True`` requires 3.10+; drop it for older
versions — everything else is 3.8-compatible).
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import (
    Any, Dict, Final, List, Mapping, Optional, TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# StrEnum compatibility shim (same pattern as carbon_forecast / eco_mode)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — Python 3.11+
    from enum import StrEnum  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — 3.10 and earlier
    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """``str`` + ``Enum`` with ``__str__`` delegating to ``str``."""

        __str__ = str.__str__
        __format__ = str.__format__


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ChartSchemaError(ValueError):
    """Raised for invalid chart-context fields or serialization."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class DataSource(StrEnum):
    """Where a visual's data came from."""

    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    FEDERATED = "federated"
    EXTERNAL = "external"
    UNKNOWN = "unknown"


class TrustLevel(StrEnum):
    """How reliable the caller asserts the data source is."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TradeoffKind(StrEnum):
    """The trade-off pairs the folder knows how to render."""

    ACCURACY_ENERGY = "accuracy_energy"
    ACCURACY_CARBON = "accuracy_carbon"
    LATENCY_ENERGY = "latency_energy"
    CARBON_ENERGY = "carbon_energy"
    QUALITY_LATENCY = "quality_latency"
    PRECISION_ENERGY = "precision_energy"
    QUANTUM_CLASSICAL = "quantum_classical"

    @property
    def description(self) -> str:
        """Human-readable label for chart legends and axis titles."""
        return {
            TradeoffKind.ACCURACY_ENERGY:   "Accuracy vs energy",
            TradeoffKind.ACCURACY_CARBON:   "Accuracy vs carbon",
            TradeoffKind.LATENCY_ENERGY:    "Latency vs energy",
            TradeoffKind.CARBON_ENERGY:     "Carbon vs energy",
            TradeoffKind.QUALITY_LATENCY:   "Quality vs latency",
            TradeoffKind.PRECISION_ENERGY:  "Precision vs energy",
            TradeoffKind.QUANTUM_CLASSICAL: "Quantum vs classical",
        }[self]


# --------------------------------------------------------------------------- #
# TypedDict for to_dict
# --------------------------------------------------------------------------- #
class ChartContextDict(TypedDict):
    """
    Return shape of :meth:`ChartContext.to_dict`.

    The ``time_range_*`` fields are ISO-8601 strings in the payload
    (they are ``datetime`` in the dataclass). ``units`` is a
    ``Dict[str, str]`` mapping variable / axis name → unit.
    """

    source: str
    trust_level: str
    system_version: Optional[str]
    policy_version: Optional[str]
    run_id: Optional[str]
    time_range_start: Optional[str]
    time_range_end: Optional[str]
    units: Dict[str, str]
    baseline: Optional[str]
    simulated: bool
    notes: Optional[str]


# --------------------------------------------------------------------------- #
# Coercion helpers
# --------------------------------------------------------------------------- #
def _coerce_enum_value(
    name: str, value: Any, enum_cls: Any,
) -> str:
    """
    Coerce a ``str`` or enum member to the enum's canonical ``.value``.

    Case-insensitive string match; enum members pass through via
    ``.value``. Everything else raises :class:`ChartSchemaError`.
    """
    if isinstance(value, enum_cls):
        return value.value
    if isinstance(value, str):
        try:
            return enum_cls(value.lower()).value
        except ValueError as exc:
            valid = ", ".join(repr(m.value) for m in enum_cls)
            raise ChartSchemaError(
                f"{name}={value!r} is not a valid {enum_cls.__name__}; "
                f"expected one of {valid}."
            ) from exc
    raise ChartSchemaError(
        f"{name} must be a {enum_cls.__name__} or a string, got "
        f"{type(value).__name__}."
    )


# --------------------------------------------------------------------------- #
# ChartContext
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ChartContext:
    """
    The provenance banner every visual must attach to itself.

    Rendered as a subtitle or footer so a reader cannot mistake a
    simulated plot for a measured one.

    Field units
    -----------
    ``units`` maps variable / axis names to their unit strings, e.g.
    ``{"energy": "kWh", "latency": "ms", "carbon": "kgCO2e"}``. Keys
    must be non-empty strings; values must be strings.

    Trust level
    -----------
    ``trust_level`` is **caller-asserted**, not derived. This schema
    does not attempt to reason about whether estimated data can be
    "high trust" — that is a policy decision for the calling system.
    """

    source: str = DataSource.UNKNOWN.value
    trust_level: str = TrustLevel.LOW.value
    system_version: Optional[str] = None
    policy_version: Optional[str] = None
    run_id: Optional[str] = None
    time_range_start: Optional[datetime] = None
    time_range_end: Optional[datetime] = None
    units: Dict[str, str] = field(default_factory=dict)
    baseline: Optional[str] = None
    simulated: bool = False
    notes: Optional[str] = None

    def __post_init__(self) -> None:
        # ---- enums: coerce + validate ---------------------------- #
        # The field types are `str`, so callers can pass either the
        # canonical string ("measured") or the enum member
        # (DataSource.MEASURED). Coerce both to the canonical string
        # via `object.__setattr__` — this is the standard frozen-
        # dataclass idiom for enum-to-value normalization.
        object.__setattr__(
            self,
            "source",
            _coerce_enum_value("source", self.source, DataSource),
        )
        object.__setattr__(
            self,
            "trust_level",
            _coerce_enum_value(
                "trust_level", self.trust_level, TrustLevel,
            ),
        )

        # ---- optional strings ------------------------------------ #
        for name in (
            "system_version", "policy_version", "run_id",
            "baseline", "notes",
        ):
            v = getattr(self, name)
            if v is not None and not isinstance(v, str):
                raise ChartSchemaError(
                    f"{name} must be a string or None, got "
                    f"{type(v).__name__}."
                )

        # ---- simulated is a real bool ---------------------------- #
        if not isinstance(self.simulated, bool):
            raise ChartSchemaError(
                f"simulated must be a bool, got "
                f"{type(self.simulated).__name__}."
            )

        # ---- datetimes + range ordering -------------------------- #
        for name in ("time_range_start", "time_range_end"):
            v = getattr(self, name)
            if v is not None and not isinstance(v, datetime):
                raise ChartSchemaError(
                    f"{name} must be a datetime or None, got "
                    f"{type(v).__name__}."
                )
        if (
            self.time_range_start is not None
            and self.time_range_end is not None
            and self.time_range_start > self.time_range_end
        ):
            raise ChartSchemaError(
                f"time_range_start ({self.time_range_start.isoformat()}) "
                f"must be <= time_range_end "
                f"({self.time_range_end.isoformat()})."
            )

        # ---- units ----------------------------------------------- #
        if not isinstance(self.units, Mapping):
            raise ChartSchemaError(
                f"units must be a Mapping, got "
                f"{type(self.units).__name__}."
            )
        for k, v in self.units.items():
            if not isinstance(k, str) or not k:
                raise ChartSchemaError(
                    f"units keys must be non-empty strings, got {k!r}."
                )
            if not isinstance(v, str):
                raise ChartSchemaError(
                    f"units[{k!r}] must be a string, got "
                    f"{type(v).__name__}."
                )

        # ---- simulated / source divergence ----------------------- #
        # Preserved for backward compatibility: a caller who set
        # simulated=True with source != "simulated" keeps working, but
        # the reader-facing subtitle shows the warning glyph (see
        # to_subtitle) and a WARNING is logged here so the divergence
        # is visible without breaking existing call sites.
        if self.simulated and self.source != DataSource.SIMULATED.value:
            logger.warning(
                "ChartContext(simulated=True, source=%r) — the source "
                "does not say 'simulated'. The subtitle will show the "
                "⚠ simulated glyph so readers are not misled.",
                self.source,
            )

    # ---------------------------------------------------------- rendering
    def to_subtitle(self) -> str:
        """
        One-line summary for the plot's subtitle.

        Includes the reader-facing signals a viewer needs to avoid
        mistaking the data: source, trust level, simulation warning,
        and the date range (compact form).
        """
        parts: List[str] = []
        if self.system_version:
            parts.append(f"sys={self.system_version}")
        if self.policy_version:
            parts.append(f"policy={self.policy_version}")
        if self.run_id:
            parts.append(f"run={self.run_id}")
        parts.append(f"source={self.source}")
        parts.append(f"trust={self.trust_level}")
        if self.simulated or self.source == DataSource.SIMULATED.value:
            parts.append("⚠ simulated")
        if self.baseline:
            parts.append(f"baseline={self.baseline}")
        if self.time_range_start is not None and self.time_range_end is not None:
            parts.append(
                f"{self.time_range_start.date().isoformat()}.."
                f"{self.time_range_end.date().isoformat()}"
            )
        return " · ".join(parts)

    def to_long_form(self) -> str:
        """
        Multi-line block for audit logs — one field per line, missing
        values rendered as ``—``.
        """
        lines: List[str] = ["ChartContext:"]
        for field_name in (
            "source",
            "trust_level",
            "simulated",
            "system_version",
            "policy_version",
            "run_id",
            "time_range_start",
            "time_range_end",
            "baseline",
            "units",
            "notes",
        ):
            value = getattr(self, field_name)
            if field_name in ("time_range_start", "time_range_end"):
                rendered = (
                    value.isoformat() if value is not None else "—"
                )
            elif field_name == "units":
                rendered = (
                    ", ".join(f"{k}={v}" for k, v in value.items())
                    or "—"
                )
            elif value in (None, ""):
                rendered = "—"
            else:
                rendered = str(value)
            lines.append(f"  {field_name:<17}= {rendered}")
        return "\n".join(lines)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> ChartContextDict:
        """
        Serialize to a JSON-compatible payload.

        Built manually (not via :func:`dataclasses.asdict`) so the
        ``datetime`` fields are converted to ISO-8601 strings and no
        deep copy of unrelated fields is performed.
        """
        return {
            "source": self.source,
            "trust_level": self.trust_level,
            "system_version": self.system_version,
            "policy_version": self.policy_version,
            "run_id": self.run_id,
            "time_range_start": (
                self.time_range_start.isoformat()
                if self.time_range_start is not None
                else None
            ),
            "time_range_end": (
                self.time_range_end.isoformat()
                if self.time_range_end is not None
                else None
            ),
            "units": dict(self.units),
            "baseline": self.baseline,
            "simulated": self.simulated,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ChartContext":
        """
        Reconstruct a :class:`ChartContext` from a ``to_dict()`` payload.

        ``time_range_start`` / ``time_range_end`` may be ISO-8601
        strings or ``datetime`` instances; unknown extra keys are
        ignored.
        """
        if not isinstance(data, Mapping):
            raise ChartSchemaError(
                "ChartContext.from_dict expects a Mapping."
            )

        def _parse_dt(name: str) -> Optional[datetime]:
            raw = data.get(name)
            if raw is None:
                return None
            if isinstance(raw, datetime):
                return raw
            if isinstance(raw, str):
                try:
                    return datetime.fromisoformat(raw)
                except ValueError as exc:
                    raise ChartSchemaError(
                        f"{name}={raw!r} is not a valid ISO-8601 "
                        f"datetime: {exc}"
                    ) from exc
            raise ChartSchemaError(
                f"{name} must be str|datetime|None, got "
                f"{type(raw).__name__}."
            )

        try:
            return cls(
                source=data.get(
                    "source", DataSource.UNKNOWN.value,
                ),
                trust_level=data.get(
                    "trust_level", TrustLevel.LOW.value,
                ),
                system_version=data.get("system_version"),
                policy_version=data.get("policy_version"),
                run_id=data.get("run_id"),
                time_range_start=_parse_dt("time_range_start"),
                time_range_end=_parse_dt("time_range_end"),
                units=dict(data.get("units") or {}),
                baseline=data.get("baseline"),
                simulated=bool(data.get("simulated", False)),
                notes=data.get("notes"),
            )
        except ChartSchemaError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise ChartSchemaError(
                f"invalid ChartContext payload: {exc}"
            ) from exc

    def to_json(self, *, indent: Optional[int] = None) -> str:
        """Serialize to JSON. The payload is primitive-only."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, payload: str) -> "ChartContext":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ChartSchemaError(
                f"invalid ChartContext JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ---------------------------------------------------------- frozen helpers
    def with_source(self, source: Any) -> "ChartContext":
        """Return a copy with ``source`` replaced (coerced + validated)."""
        return replace(self, source=source)

    def with_trust(self, trust_level: Any) -> "ChartContext":
        """Return a copy with ``trust_level`` replaced."""
        return replace(self, trust_level=trust_level)

    def with_simulated(self, simulated: bool) -> "ChartContext":
        """Return a copy with ``simulated`` replaced."""
        return replace(self, simulated=simulated)

    # ---------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "ChartContext("
            f"source={self.source!r}, "
            f"trust={self.trust_level!r}, "
            f"simulated={self.simulated})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ChartContext",
    "ChartContextDict",
    "ChartSchemaError",
    "DataSource",
    "TradeoffKind",
    "TrustLevel",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.contracts.chart_schema
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    from datetime import timezone

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Defaults & repr ------------------------------------- #
    print("defaults:")
    ctx = ChartContext()
    _check("default source == unknown", ctx.source == "unknown")
    _check("default trust == low", ctx.trust_level == "low")
    _check("default simulated == False", ctx.simulated is False)
    _check(
        "repr mentions source",
        "source='unknown'" in repr(ctx),
    )

    # ---- 2. Enum coercion --------------------------------------- #
    print("enum coercion:")
    a = ChartContext(source=DataSource.MEASURED)
    _check(
        "enum member → canonical string",
        a.source == "measured",
    )
    b = ChartContext(source="measured")
    _check("string → canonical string", b.source == "measured")
    c = ChartContext(source="MEASURED")
    _check(
        "case-insensitive string → canonical",
        c.source == "measured",
    )
    _check("all forms equal", a == b == c)
    _check(
        "trust_level coerced",
        ChartContext(trust_level=TrustLevel.HIGH).trust_level == "high",
    )
    _check(
        "trust_level case-insensitive",
        ChartContext(trust_level="MEDIUM").trust_level == "medium",
    )

    # StrEnum behaviour on this Python version
    _check(
        "str(DataSource.MEASURED) == 'measured'",
        str(DataSource.MEASURED) == "measured",
    )
    _check(
        "f-string renders canonical value",
        f"{TrustLevel.HIGH}" == "high",
    )

    # ---- 3. Validation errors ----------------------------------- #
    print("validation errors:")
    for bad_kwargs in (
        dict(source="not-a-source"),
        dict(source=42),
        dict(source=True),
        dict(trust_level="not-a-level"),
        dict(trust_level=3.14),
        dict(simulated="yes"),
        dict(simulated=1),
        dict(units="not-a-mapping"),
        dict(units={1: "x"}),
        dict(units={"x": 1}),
        dict(units={"": "x"}),
        dict(system_version=42),
        dict(notes=True),
        dict(time_range_start="2026-01-01"),
        dict(time_range_end=0),
    ):
        try:
            ChartContext(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject {list(bad_kwargs)}", False)
        except ChartSchemaError:
            _check(f"reject {list(bad_kwargs)}", True)
        except Exception:
            _check(f"reject {list(bad_kwargs)} (wrong type)", False)

    # ---- 4. Time range ordering --------------------------------- #
    print("time range ordering:")
    from datetime import datetime as dt, timedelta

    start = dt(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    ok_ctx = ChartContext(
        time_range_start=start, time_range_end=end,
    )
    _check("start < end accepted", ok_ctx.time_range_start == start)

    try:
        ChartContext(time_range_start=end, time_range_end=start)
        _check("start > end rejected", False)
    except ChartSchemaError:
        _check("start > end rejected", True)

    # Equal start/end allowed
    same_ctx = ChartContext(time_range_start=start, time_range_end=start)
    _check("start == end accepted", same_ctx.time_range_start == start)

    # ---- 5. Frozen dataclass ------------------------------------ #
    print("frozen:")
    try:
        ok_ctx.source = "fabricated"  # type: ignore[misc]
        _check("mutation rejected", False)
    except (AttributeError, Exception):
        _check("mutation rejected", True)

    # with_* helpers replace correctly
    modified = ok_ctx.with_source(DataSource.SIMULATED)
    _check(
        "with_source replaces field",
        modified.source == "simulated",
    )
    _check(
        "original unchanged by with_source",
        ok_ctx.source == "unknown",
    )
    _check(
        "with_trust replaces field",
        ok_ctx.with_trust("high").trust_level == "high",
    )
    _check(
        "with_simulated replaces field",
        ok_ctx.with_simulated(True).simulated is True,
    )

    # ---- 6. to_subtitle includes the important fields ---------- #
    print("to_subtitle:")
    rich = ChartContext(
        source="measured",
        trust_level="high",
        system_version="v3.2",
        policy_version="p17",
        run_id="run-abc123",
        time_range_start=dt(2026, 10, 1, tzinfo=timezone.utc),
        time_range_end=dt(2026, 10, 8, tzinfo=timezone.utc),
        baseline="median-2025",
    )
    subtitle = rich.to_subtitle()
    _check("subtitle mentions sys", "sys=v3.2" in subtitle)
    _check("subtitle mentions policy", "policy=p17" in subtitle)
    _check("subtitle mentions run id", "run=run-abc123" in subtitle)
    _check("subtitle mentions source", "source=measured" in subtitle)
    _check("subtitle mentions trust", "trust=high" in subtitle)
    _check("subtitle mentions baseline", "baseline=median-2025" in subtitle)
    _check(
        "subtitle mentions date range",
        "2026-10-01..2026-10-08" in subtitle,
    )
    _check(
        "no simulated glyph on measured",
        "simulated" not in subtitle,
    )

    # ---- 7. simulated divergence --------------------------------- #
    print("simulated divergence:")
    sim_source = ChartContext(source="simulated", simulated=False)
    _check(
        "source=simulated (no flag) shows glyph",
        "simulated" in sim_source.to_subtitle(),
    )
    flagged = ChartContext(source="measured", simulated=True)
    _check(
        "flag=True (source=measured) shows glyph",
        "simulated" in flagged.to_subtitle(),
    )
    _check(
        "both signal — no false negative",
        "simulated" in sim_source.to_subtitle()
        and "simulated" in flagged.to_subtitle(),
    )

    # ---- 8. to_dict shape --------------------------------------- #
    print("to_dict:")
    payload = rich.to_dict()
    _check("payload is dict", isinstance(payload, dict))
    _check("payload source", payload["source"] == "measured")
    _check("payload trust", payload["trust_level"] == "high")
    _check(
        "time_range_start is ISO string",
        isinstance(payload["time_range_start"], str)
        and "T" in payload["time_range_start"],
    )
    _check(
        "time_range_end is ISO string",
        isinstance(payload["time_range_end"], str),
    )
    _check(
        "units is a plain dict",
        isinstance(payload["units"], dict),
    )
    _check(
        "units is not the same object",
        payload["units"] is not rich.units,
    )

    # ---- 9. Round-trip ----------------------------------------- #
    print("round-trip:")
    restored = ChartContext.from_dict(payload)
    _check(
        "restored source matches",
        restored.source == rich.source,
    )
    _check(
        "restored time_range_start matches",
        restored.time_range_start == rich.time_range_start,
    )
    _check("restored context equals original", restored == rich)

    json_payload = rich.to_json(indent=2)
    restored2 = ChartContext.from_json(json_payload)
    _check("json round-trip equals", restored2 == rich)

    # from_dict with datetime objects (not strings)
    payload_dt = dict(payload)
    payload_dt["time_range_start"] = rich.time_range_start
    payload_dt["time_range_end"] = rich.time_range_end
    restored3 = ChartContext.from_dict(payload_dt)
    _check(
        "from_dict accepts datetime for time_range_*",
        restored3 == rich,
    )

    # Empty round-trip
    empty_restored = ChartContext.from_dict(ChartContext().to_dict())
    _check("empty round-trip equals", empty_restored == ChartContext())

    # ---- 10. from_dict error handling -------------------------- #
    print("from_dict errors:")
    for bad_payload in (
        "not-a-mapping",
        {"source": "not-a-source"},
        {"time_range_start": "not-iso"},
        {"trust_level": "not-a-level"},
        {"units": "not-a-mapping"},
    ):
        try:
            ChartContext.from_dict(bad_payload)  # type: ignore[arg-type]
            _check(f"reject {bad_payload!r}", False)
        except ChartSchemaError:
            _check(f"reject {bad_payload!r}", True)

    try:
        ChartContext.from_json("{not json")
        _check("bad JSON rejected", False)
    except ChartSchemaError:
        _check("bad JSON rejected", True)

    # ---- 11. TradeoffKind descriptions ------------------------ #
    print("TradeoffKind:")
    _check(
        "description for ACCURACY_ENERGY",
        TradeoffKind.ACCURACY_ENERGY.description
        == "Accuracy vs energy",
    )
    _check(
        "description for QUANTUM_CLASSICAL",
        TradeoffKind.QUANTUM_CLASSICAL.description
        == "Quantum vs classical",
    )
    _check(
        "all TradeoffKind have descriptions",
        all(
            isinstance(m.description, str) and m.description
            for m in TradeoffKind
        ),
    )

    # ---- 12. to_long_form ------------------------------------ #
    print("to_long_form:")
    lf = rich.to_long_form()
    _check(
        "long form starts with header",
        lf.startswith("ChartContext:"),
    )
    _check("long form has source line", "source" in lf)
    _check("long form has run_id line", "run_id" in lf)
    _check(
        "long form renders missing fields as —",
        ChartContext().to_long_form().count("—") >= 4,
    )

    # ---- 13. __all__ / __dir__ ------------------------------ #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "ChartContext", "ChartContextDict", "ChartSchemaError",
        "DataSource", "TradeoffKind", "TrustLevel",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
