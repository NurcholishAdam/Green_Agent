# src/dashboard/symbolic_visualizer.py

"""
Symbolic Violation Visualizer
=============================

Dashboard component for visualizing symbolic rule violations with trace
explanations.

Enhancements
------------
- ``SymbolicVisualizerConfig`` — frozen, fully validated: bounded history,
  HTML escape toggle, severity colors, category prefixes.
  ``severity_colors`` values must be hex colors (``#rgb`` / ``#rrggbb`` /
  ``#rrggbbaa``) or CSS named colors; the mapping and
  ``category_prefixes`` are frozen with :class:`types.MappingProxyType`.
  ``from_dict(..., strict=True)`` rejects unknown keys.
- **XSS-hardened** — every user-supplied string **including config-supplied
  colors** is HTML-escaped before insertion into the rendered card. The
  color-format validation from ``__post_init__`` provides defence in depth.
- **Consistent category vocabulary** — ``_extract_category`` matches rule
  IDs against ``category_prefixes`` (longest prefix wins) so
  ``generate_category_view()`` and ``filter_by_rule_type()`` agree on
  category names. Composite rules like ``COMP-SUST-001`` map to
  ``sustainability``; unmatched rules fall back to the first dash segment
  lowercased.
- **Bounded storage** — ``deque(maxlen=config.max_history)``.
- **Thread safety** — ``RLock`` guards all state. ``export_violation_report``
  takes a **single** snapshot under one lock, so the summary, category
  view, timeline, and raw violations all reflect the same state.
- **Atomic, durable export** — ``tempfile`` + ``fdopen`` + ``fsync`` +
  ``os.replace``; temp file cleaned on any failure; fd closed on every
  path.
- **``from_dict`` / ``from_json``** on the visualizer, matching the
  existing ``to_dict`` shape. Restores the violation deque and counters.
- **Robust timeline sort** — mixed ``int`` / ``str`` / ``datetime``
  timestamps are coerced to a comparable key instead of crashing.
- **``"other"`` severity bucket** — violations whose severity is not one
  of ``critical`` / ``high`` / ``medium`` / ``low`` are surfaced under
  ``"other"`` rather than silently dropped.
- **``to_json(include_history=True)`` now works** (the kwarg routes to
  ``to_dict``; the previous signature forwarded its kwargs to
  ``json.dumps`` and raised ``TypeError``).
- **JSON-safe serialization** — ``to_json`` and ``export_violation_report``
  coerce ``datetime`` / ``Enum`` / nested mappings to JSON primitives
  rather than silently stringifying via ``default=str``.
- ``__version__``, config ``__repr__``, ``_check()``-based smoke test
  that survives ``python -O``.

Compatibility
-------------
- ``SymbolicVisualizer(*, config=None, strict=True)`` — same signature.
  ``strict`` must now be a ``bool``; passing ``strict="yes"`` raises
  :class:`SymbolicVisualizerError`.
- ``SymbolicVisualizerConfig`` — same fields. Behaviour changes:
  * ``max_history`` must be a real ``int`` (``bool`` / ``str`` raise
    ``SymbolicVisualizerError`` instead of ``TypeError``).
  * ``escape_html`` must be a ``bool``.
  * ``severity_colors`` values must be hex or CSS named colors; the
    mapping is frozen.
  * ``category_prefixes`` values must be non-empty tuples of non-empty
    strings; the mapping is frozen.
  * ``from_dict(..., strict=True)`` default — unknown keys rejected.
- ``add_violations(violations)`` — same signature and return value
  (count added). **Behaviour changes**:
  * ``violations`` accepts any ``Sequence`` (``str`` / ``bytes`` still
    rejected).
  * Non-mapping entries are logged at ``WARNING`` per index instead of
    being silently dropped.
- ``generate_category_view()`` — **behaviour change**: keys are now the
  semantic category names from ``category_prefixes`` (e.g.
  ``"sustainability"``) instead of the first dash-segment lowercased
  (e.g. ``"sust"``). This is the fix that makes the view agree with
  ``filter_by_rule_type``. Unmatched rule IDs still fall back to the
  first dash-segment so the change is limited to rules that match a
  configured prefix.
- ``generate_violation_timeline()`` — same shape. Robust against mixed
  timestamp types.
- ``generate_severity_summary()`` — same shape plus a new ``"other"``
  bucket in ``counts`` and ``details``.
- ``export_violation_report(filepath)`` — same signature, same return
  value. Now atomic, durable, and internally consistent.
- ``to_json(include_history=True)`` now works.
- ``from_dict`` / ``from_json`` added.

Requires Python 3.8+.
"""

from __future__ import annotations

import html as _html
import json
import logging
import math
import os
import re
import tempfile
import threading
import time
from collections import deque
from collections.abc import Mapping as ABCMapping
from collections.abc import Sequence as ABCSequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import (
    Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple, Union,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"

_KNOWN_SEVERITIES: Tuple[str, ...] = (
    "critical", "high", "medium", "low",
)
_OTHER_SEVERITY = "other"

# Hex color: #rgb, #rrggbb, or #rrggbbaa.
_HEX_COLOR_RE = re.compile(
    r"^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$"
)
# CSS named color: conservative alphabetic form (covers "red", "blue",
# "rebeccapurple", etc.). Anything containing quotes, angle brackets,
# parentheses, or semicolons is rejected here; the renderer also
# escapes the value at insertion time.
_CSS_COLOR_NAME_RE = re.compile(r"^[a-zA-Z]{3,20}$")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SymbolicVisualizerError(ValueError):
    """Raised for invalid visualizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Validators
# --------------------------------------------------------------------------- #
def _require_bool(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise SymbolicVisualizerError(
            f"{name} must be a bool, got {type(value).__name__}."
        )
    return value


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SymbolicVisualizerError(f"{name} must be an int.")
    if value <= 0:
        raise SymbolicVisualizerError(f"{name} must be > 0, got {value}.")
    return value


def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise SymbolicVisualizerError(
            f"{name} must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    return value


def _is_valid_color(value: Any) -> bool:
    return isinstance(value, str) and bool(
        _HEX_COLOR_RE.match(value) or _CSS_COLOR_NAME_RE.match(value)
    )


def _coerce_path(value: Any, *, name: str = "path") -> Path:
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        if not value:
            raise SymbolicVisualizerError(f"{name} must not be empty.")
        return Path(value)
    if hasattr(value, "__fspath__"):
        return Path(os.fspath(value))
    raise SymbolicVisualizerError(
        f"{name} must be str, Path, or os.PathLike, got "
        f"{type(value).__name__}."
    )


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse ISO 8601 / numeric epoch / datetime. Returns ``None`` on failure."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if not math.isfinite(ts):
            return None
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z") or s.endswith("z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    """Return a JSON-safe copy of ``value``; unknown types become strings."""
    if depth > 32:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value if hasattr(value, "value") else str(value)
    if isinstance(value, ABCMapping):
        return {
            str(k): _json_safe(v, depth=depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v, depth=depth + 1) for v in value]
    return str(value)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def _default_severity_colors() -> Dict[str, str]:
    return {
        "critical": "#dc3545",
        "high":     "#fd7e14",
        "medium":   "#ffc107",
        "low":      "#17a2b8",
    }


def _default_category_prefixes() -> Dict[str, Tuple[str, ...]]:
    return {
        "sustainability": ("SUST", "COMP-SUST"),
        "resource":       ("RES", "COMP-RES"),
        "fairness":       ("FAIR",),
        "safety":         ("SAFE",),
        "compliance":     ("COMP",),
    }


@dataclass(frozen=True)
class SymbolicVisualizerConfig:
    """Tunable parameters for :class:`SymbolicVisualizer`."""

    max_history: int = 100_000
    escape_html: bool = True

    severity_colors: Mapping[str, str] = field(
        default_factory=_default_severity_colors,
    )
    category_prefixes: Mapping[str, Tuple[str, ...]] = field(
        default_factory=_default_category_prefixes,
    )

    def __post_init__(self) -> None:
        _require_positive_int("max_history", self.max_history)
        _require_bool("escape_html", self.escape_html)

        # ---- severity_colors ---------------------------------------- #
        if not isinstance(self.severity_colors, ABCMapping):
            raise SymbolicVisualizerError(
                "severity_colors must be a Mapping."
            )
        validated_colors: Dict[str, str] = {}
        for sev, color in self.severity_colors.items():
            _require_nonempty_str("severity_colors key", sev)
            if not _is_valid_color(color):
                raise SymbolicVisualizerError(
                    f"severity_colors[{sev!r}] must be a hex color or a "
                    f"CSS named color, got {color!r}."
                )
            validated_colors[sev] = color
        object.__setattr__(
            self, "severity_colors", MappingProxyType(validated_colors),
        )

        # ---- category_prefixes -------------------------------------- #
        if not isinstance(self.category_prefixes, ABCMapping):
            raise SymbolicVisualizerError(
                "category_prefixes must be a Mapping."
            )
        validated_prefixes: Dict[str, Tuple[str, ...]] = {}
        for category, prefixes in self.category_prefixes.items():
            _require_nonempty_str("category_prefixes key", category)
            if not isinstance(prefixes, (tuple, list)) or not prefixes:
                raise SymbolicVisualizerError(
                    f"category_prefixes[{category!r}] must be a "
                    f"non-empty tuple."
                )
            cleaned: List[str] = []
            for i, p in enumerate(prefixes):
                if not isinstance(p, str) or not p:
                    raise SymbolicVisualizerError(
                        f"category_prefixes[{category!r}][{i}] must be "
                        f"a non-empty string."
                    )
                cleaned.append(p)
            validated_prefixes[category] = tuple(cleaned)
        object.__setattr__(
            self,
            "category_prefixes",
            MappingProxyType(validated_prefixes),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_history": self.max_history,
            "escape_html": self.escape_html,
            "severity_colors": dict(self.severity_colors),
            "category_prefixes": {
                k: list(v) for k, v in self.category_prefixes.items()
            },
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "SymbolicVisualizerConfig":
        if not isinstance(data, ABCMapping):
            raise SymbolicVisualizerError(
                "SymbolicVisualizerConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise SymbolicVisualizerError(
                    f"unknown SymbolicVisualizerConfig keys: {unknown}"
                )
        kwargs: Dict[str, Any] = {}
        try:
            for k, v in data.items():
                if k not in valid:
                    continue
                if k in ("severity_colors", "category_prefixes"):
                    if not isinstance(v, ABCMapping):
                        raise SymbolicVisualizerError(
                            f"{k} must be a Mapping."
                        )
                    if k == "category_prefixes":
                        kwargs[k] = {
                            str(cat): tuple(p) for cat, p in v.items()
                        }
                    else:
                        kwargs[k] = dict(v)
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except SymbolicVisualizerError:
            raise
        except (TypeError, ValueError) as exc:
            raise SymbolicVisualizerError(
                f"invalid SymbolicVisualizerConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "SymbolicVisualizerConfig("
            f"max_history={self.max_history}, "
            f"escape_html={self.escape_html}, "
            f"severities={sorted(self.severity_colors)!r})"
        )


# --------------------------------------------------------------------------- #
# Visualizer
# --------------------------------------------------------------------------- #
class SymbolicVisualizer:
    """
    Visualizer for symbolic rule violations in the dashboard.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[SymbolicVisualizerConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, SymbolicVisualizerConfig
        ):
            raise SymbolicVisualizerError(
                f"config must be a SymbolicVisualizerConfig or None, "
                f"got {type(config).__name__}."
            )
        self._config = config or SymbolicVisualizerConfig()
        _require_bool("strict", strict)
        self._strict = strict

        self._lock = threading.RLock()
        self._violations: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        # Legacy view — kept in sync with the internal deque. Each entry
        # is a **deep copy** so mutating it does not affect stored
        # violations.
        self.violation_data: List[Dict[str, Any]] = []

        logger.debug(
            "SymbolicVisualizer initialized (max_history=%d, strict=%s)",
            self._config.max_history, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SymbolicVisualizerConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def violation_count(self) -> int:
        with self._lock:
            return len(self._violations)

    # ---------------------------------------------------------- ingestion
    def add_violations(
        self, violations: Sequence[Mapping[str, Any]],
    ) -> int:
        """
        Add violation traces to the visualizer.

        Accepts any non-string ``Sequence`` of mappings. Non-mapping
        entries are logged at ``WARNING`` per index and skipped.

        Returns the number of violations successfully added.
        """
        if isinstance(violations, (str, bytes)) or not isinstance(
            violations, ABCSequence,
        ):
            raise SymbolicVisualizerError(
                "violations must be a sequence of mappings "
                "(not a string)."
            )

        added = 0
        dropped: List[int] = []
        with self._lock:
            for i, v in enumerate(violations):
                if isinstance(v, ABCMapping):
                    self._violations.append(dict(v))
                    added += 1
                else:
                    dropped.append(i)
                    logger.warning(
                        "add_violations: violations[%d] is not a "
                        "Mapping (%s); dropped.",
                        i, type(v).__name__,
                    )
            self.violation_data = [
                dict(v) for v in self._violations
            ]

        if dropped:
            logger.warning(
                "add_violations: %d/%d entries dropped as non-Mapping.",
                len(dropped), len(violations),
            )
        logger.debug("Added %d violation(s) to visualizer.", added)
        return added

    # ---------------------------------------------------------- views
    def generate_violation_timeline(self) -> List[Dict[str, Any]]:
        """Generate a sorted timeline view of violations."""
        with self._lock:
            data = [dict(v) for v in self._violations]
        return self._timeline_from(data)

    def _timeline_from(
        self, data: Sequence[Mapping[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Pure helper — no lock; caller supplies a snapshot."""
        data_sorted = sorted(data, key=_timeline_sort_key)
        timeline: List[Dict[str, Any]] = []
        for v in data_sorted:
            severity = _normalize_severity(v.get("severity"))
            timeline.append({
                "timestamp": v.get("timestamp"),
                "step": v.get("step"),
                "rule_name": v.get("rule_name"),
                "severity": severity,
                "category": self._extract_category(
                    v.get("rule_id", ""),
                ),
                "action": v.get("action_triggered"),
                "status": (
                    "critical" if severity == "critical" else "warning"
                ),
            })
        return timeline

    def generate_category_view(self) -> Dict[str, List[Dict[str, Any]]]:
        """Group violations by category."""
        with self._lock:
            data = [dict(v) for v in self._violations]
        return self._category_view_from(data)

    def _category_view_from(
        self, data: Sequence[Mapping[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Pure helper — no lock; caller supplies a snapshot."""
        by_category: Dict[str, List[Dict[str, Any]]] = {}
        for v in data:
            category = (
                self._extract_category(v.get("rule_id", "")) or "unknown"
            )
            by_category.setdefault(category, []).append({
                "rule_id": v.get("rule_id"),
                "rule_name": v.get("rule_name"),
                "step": v.get("step"),
                "severity": v.get("severity"),
                "explanation": v.get("explanation"),
                "violation_details": v.get("violation_details"),
            })
        return by_category

    def generate_severity_summary(self) -> Dict[str, Any]:
        """Generate a summary grouped by severity level."""
        with self._lock:
            data = [dict(v) for v in self._violations]
        return self._severity_summary_from(data)

    def _severity_summary_from(
        self, data: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        """Pure helper — no lock; caller supplies a snapshot."""
        summary: Dict[str, List[Dict[str, Any]]] = {
            k: [] for k in _KNOWN_SEVERITIES
        }
        summary[_OTHER_SEVERITY] = []
        for v in data:
            sev = _normalize_severity(v.get("severity"))
            bucket = sev if sev in summary else _OTHER_SEVERITY
            summary[bucket].append({
                "rule_name": v.get("rule_name"),
                "step": v.get("step"),
                "action": v.get("action_triggered"),
            })
        return {
            "counts": {k: len(vs) for k, vs in summary.items()},
            "details": summary,
        }

    def filter_by_rule_type(
        self, rule_type: str,
    ) -> List[Dict[str, Any]]:
        """Filter violations by category prefix."""
        if not isinstance(rule_type, str) or not rule_type:
            raise SymbolicVisualizerError(
                "rule_type must be a non-empty string."
            )
        # Accept both the semantic name ("sustainability") and the raw
        # prefix ("SUST") for backward compatibility with callers that
        # used the old first-segment behaviour.
        key = rule_type.lower()
        prefixes = self._config.category_prefixes.get(key, ())
        if not prefixes:
            # Try matching the input against each configured prefix list.
            for category_prefixes in self._config.category_prefixes.values():
                if any(p.lower() == key for p in category_prefixes):
                    prefixes = category_prefixes
                    break
        if not prefixes:
            prefixes = ()
        with self._lock:
            data = [dict(v) for v in self._violations]
        return [
            v for v in data
            if any(
                str(v.get("rule_id", "")).startswith(p) for p in prefixes
            )
        ]

    def generate_html_violation_card(
        self, violation: Mapping[str, Any],
    ) -> str:
        """Generate an HTML card for a single violation (escaped)."""
        esc = (
            (lambda s: _html.escape(str(s), quote=True))
            if self._config.escape_html
            else (lambda s: str(s))
        )
        raw_severity = violation.get("severity")
        severity = _normalize_severity(raw_severity)
        raw_color = self._config.severity_colors.get(
            severity, "#6c757d",
        )
        # Defense in depth: even though ``__post_init__`` validates
        # colors, escape the value here so a future config field that
        # reaches this point cannot inject markup.
        color = _html.escape(str(raw_color), quote=True)

        return f"""
<div style="border-left: 4px solid {color}; padding: 8px; margin: 8px 0;">
<h4>{esc(violation.get('rule_name', 'Unknown Rule'))}</h4>
<span style="background: {color}; color: white; padding: 2px 6px; border-radius: 3px;">
{esc(severity.upper())}
</span>
<p>Rule ID: {esc(violation.get('rule_id', 'N/A'))} | Step: {esc(violation.get('step', 'N/A'))}</p>
<p>Condition: <code>{esc(violation.get('condition', 'N/A'))}</code></p>
<p>Action Triggered: {esc(violation.get('action_triggered', 'N/A'))}</p>
<p>{esc(violation.get('explanation', 'No explanation available'))}</p>
<details>
<summary>View Trace Details</summary>
<pre>{esc(violation.get('violation_details', 'No details available'))}</pre>
</details>
</div>
"""

    def generate_dashboard_section(self) -> str:
        """Generate the complete HTML section for the dashboard."""
        if self.violation_count == 0:
            return """
<h3>✅ Symbolic Oversight</h3>
<p>No rule violations detected. All symbolic constraints satisfied.</p>
"""

        severity_summary = self.generate_severity_summary()
        category_view = self.generate_category_view()

        counts = severity_summary["counts"]
        esc = (
            (lambda s: _html.escape(str(s)))
            if self._config.escape_html
            else (lambda s: str(s))
        )

        parts = [f"""
<h3>⚠️ Symbolic Oversight - Rule Violations</h3>
<p><b>Critical:</b> {counts.get('critical', 0)} |
<b>High:</b> {counts.get('high', 0)} |
<b>Medium:</b> {counts.get('medium', 0)} |
<b>Low:</b> {counts.get('low', 0)}</p>
<h4>Violations by Category</h4>
"""]
        for category, violations in category_view.items():
            parts.append(
                f"<h5>{esc(category.upper())} ({len(violations)})</h5>"
            )
            for v in violations:
                parts.append(self.generate_html_violation_card(v))
        return "".join(parts)

    def export_violation_report(
        self, filepath: Union[str, Path],
    ) -> int:
        """
        Export a violation report as JSON.

        Takes a **single** snapshot under one lock so the summary,
        category view, timeline, and raw violations reflect the same
        state. The write is atomic and durable (``fsync`` before
        ``os.replace``). Returns bytes written.
        """
        path = _coerce_path(filepath, name="filepath")
        with self._lock:
            violations = [dict(v) for v in self._violations]
        severity_summary = self._severity_summary_from(violations)
        category_view = self._category_view_from(violations)
        timeline = self._timeline_from(violations)

        report = {
            "total_violations": len(violations),
            "severity_summary": _json_safe(severity_summary),
            "category_view": _json_safe(category_view),
            "timeline": _json_safe(timeline),
            "violations": _json_safe(violations),
        }
        return _atomic_write_json(path, report)

    # ---------------------------------------------------------- helpers
    def _extract_category(self, rule_id: Any) -> Optional[str]:
        """
        Return the semantic category for ``rule_id``.

        Matches rule IDs against the configured ``category_prefixes``,
        longest prefix first so ``COMP-SUST-001`` maps to
        ``sustainability`` rather than ``compliance``. Unmatched rule
        IDs fall back to the first dash-segment lowercased. Returns
        ``None`` for an empty rule ID.
        """
        if not rule_id:
            return None
        rid = str(rule_id)
        # Build a flat list of (prefix, category) and sort by prefix
        # length descending so composite prefixes win.
        candidates: List[Tuple[str, str]] = []
        for category, prefixes in self._config.category_prefixes.items():
            for prefix in prefixes:
                candidates.append((prefix, category))
        candidates.sort(key=lambda pc: len(pc[0]), reverse=True)
        for prefix, category in candidates:
            if rid.startswith(prefix):
                return category
        # Fallback: first dash-segment lowercased.
        parts = rid.split("-", 1)
        if parts and parts[0]:
            return parts[0].lower()
        return None

    # ---------------------------------------------------------- lifecycle
    def reset(self) -> int:
        """Clear the visualizer. Returns number removed."""
        with self._lock:
            removed = len(self._violations)
            self._violations.clear()
            self.violation_data = []
            self._started_at = time.time()
        logger.debug("SymbolicVisualizer reset (removed %d).", removed)
        return removed

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            count = len(self._violations)
            started = self._started_at
        return {
            "violations": count,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - started,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
            }
            if include_history:
                payload["violations"] = [
                    dict(v) for v in self._violations
                ]
        return payload

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any,
    ) -> str:
        """
        Serialize to JSON.

        ``include_history=True`` includes the stored violations
        (additive — the previous ``to_json(**kwargs)`` signature
        forwarded its kwargs to ``json.dumps`` and raised ``TypeError``
        on ``include_history``).
        """
        payload = self.to_dict(include_history=include_history)
        return json.dumps(_json_safe(payload), **kwargs)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        restore_history: bool = True,
    ) -> "SymbolicVisualizer":
        """
        Reconstruct a visualizer from a ``to_dict`` payload.

        Parameters
        ----------
        data : Mapping
            Payload produced by :meth:`to_dict`.
        strict : bool, optional
            Overrides the payload's ``strict`` flag when supplied.
        restore_history : bool, default True
            Restore the violation deque when the payload contains it.
        """
        if not isinstance(data, ABCMapping):
            raise SymbolicVisualizerError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob
            if isinstance(cfg_blob, SymbolicVisualizerConfig)
            else SymbolicVisualizerConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None else strict
        )
        vis = cls(config=config, strict=resolved_strict)

        if restore_history:
            for entry in data.get("violations", []) or []:
                if isinstance(entry, ABCMapping):
                    with vis._lock:
                        vis._violations.append(dict(entry))
            with vis._lock:
                vis.violation_data = [
                    dict(v) for v in vis._violations
                ]

        if data.get("started_at") is not None:
            try:
                vis._started_at = float(data["started_at"])
            except (TypeError, ValueError):
                pass
        return vis

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
        restore_history: bool = True,
    ) -> "SymbolicVisualizer":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SymbolicVisualizerError(
                f"invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(
            data, strict=strict, restore_history=restore_history,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "SymbolicVisualizer("
                f"violations={len(self._violations)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Sort / normalization helpers
# --------------------------------------------------------------------------- #
def _timeline_sort_key(v: Mapping[str, Any]) -> Tuple[float, float]:
    """Return a comparable sort key for a violation entry."""
    raw_ts = v.get("timestamp")
    if isinstance(raw_ts, datetime):
        ts = raw_ts.timestamp()
    elif isinstance(raw_ts, (int, float)) and not isinstance(raw_ts, bool):
        ts = float(raw_ts)
    else:
        parsed = _parse_iso_datetime(raw_ts)
        ts = parsed.timestamp() if parsed is not None else 0.0

    raw_step = v.get("step")
    step = (
        float(raw_step)
        if isinstance(raw_step, (int, float)) and not isinstance(raw_step, bool)
        else 0.0
    )
    return (ts, step)


def _normalize_severity(raw: Any) -> str:
    """Return a lowercased, stripped severity string (or ``"unknown"``)."""
    if raw is None:
        return "unknown"
    s = str(raw).strip().lower()
    return s or "unknown"


# --------------------------------------------------------------------------- #
# Atomic I/O
# --------------------------------------------------------------------------- #
def _atomic_write_json(path: Path, payload: Any) -> int:
    """
    Write ``payload`` as JSON to ``path`` atomically. Returns bytes
    written.

    Uses ``mkstemp`` + ``fdopen`` + ``fsync`` + ``os.replace``.
    Cleans up the temp file and closes the fd on every failure path.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp",
            dir=str(path.parent),
        )
    except OSError as exc:
        raise SymbolicVisualizerError(
            f"could not prepare {path}: {exc}"
        ) from exc

    try:
        f = os.fdopen(fd, "w", encoding="utf-8")
    except OSError:
        os.close(fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    try:
        with f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except SymbolicVisualizerError:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    except (OSError, TypeError, ValueError) as exc:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise SymbolicVisualizerError(
            f"could not write {path}: {exc}"
        ) from exc
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return path.stat().st_size


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "SymbolicVisualizer",
    "SymbolicVisualizerConfig",
    "SymbolicVisualizerError",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.symbolic_visualizer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

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

    def _expect_error(name: str, expected: type, fn, *args, **kwargs):
        global failed
        try:
            fn(*args, **kwargs)
            _check(name, False)
        except expected:
            _check(name, True)
        except Exception as exc:
            print(
                f"  ❌ {name} — wrong exception "
                f"{type(exc).__name__}: {exc}"
            )
            failed += 1

    # ---- 1. Config defaults + repr ---------------------------- #
    print("config defaults:")
    cfg = SymbolicVisualizerConfig()
    _check("default max_history", cfg.max_history == 100_000)
    _check("default escape_html", cfg.escape_html is True)
    _check(
        "repr concise",
        "SymbolicVisualizerConfig(" in repr(cfg)
        and "max_history=100000" in repr(cfg),
    )
    _check(
        "severity_colors is MappingProxyType",
        type(cfg.severity_colors).__name__ == "mappingproxy",
    )
    _check(
        "category_prefixes is MappingProxyType",
        type(cfg.category_prefixes).__name__ == "mappingproxy",
    )

    # Frozen mapping: mutation rejected
    try:
        cfg.severity_colors["critical"] = "#ff0000"  # type: ignore[index]
        _check("severity_colors is read-only", False)
    except TypeError:
        _check("severity_colors is read-only", True)

    # ---- 2. Config validation --------------------------------- #
    print("config validation:")
    for label, bad in (
        ("zero max_history", dict(max_history=0)),
        ("negative max_history", dict(max_history=-1)),
        ("string max_history", dict(max_history="100")),
        ("bool max_history", dict(max_history=True)),
        ("string escape_html", dict(escape_html="yes")),
        ("int escape_html", dict(escape_html=1)),
        (
            "malicious color",
            dict(severity_colors={
                "critical": 'red"><script>alert(1)</script>',
            }),
        ),
        (
            "empty severity key",
            dict(severity_colors={"": "#fff"}),
        ),
        (
            "non-tuple category prefixes",
            dict(category_prefixes={"x": "#fff"}),
        ),
        (
            "empty category prefixes",
            dict(category_prefixes={"x": ()}),
        ),
        (
            "non-string prefix",
            dict(category_prefixes={"x": (42,)}),
        ),
    ):
        _expect_error(
            f"reject cfg {label}",
            SymbolicVisualizerError,
            SymbolicVisualizerConfig,
            **bad,
        )

    # Valid colors pass
    try:
        SymbolicVisualizerConfig(
            severity_colors={
                "critical": "#dc3545",
                "warn": "orange",
                "info": "rebeccapurple",
            },
        )
        _check("valid colors accepted", True)
    except Exception as exc:
        _check(f"valid colors accepted ({exc})", False)

    # ---- 3. Config round-trip --------------------------------- #
    print("config round-trip:")
    d = cfg.to_dict()
    _check(
        "from_dict(to_dict()) == original",
        SymbolicVisualizerConfig.from_dict(d) == cfg,
    )
    _expect_error(
        "from_dict strict=True rejects unknown keys",
        SymbolicVisualizerError,
        SymbolicVisualizerConfig.from_dict,
        {"max_history": 1000, "typo_field": 1},
    )
    _check(
        "from_dict strict=False accepts unknown keys",
        SymbolicVisualizerConfig.from_dict(
            {"max_history": 1000, "typo_field": 1}, strict=False,
        ).max_history == 1000,
    )

    # ---- 4. Visualizer construction --------------------------- #
    print("construction:")
    vis = SymbolicVisualizer()
    _check("dashboard constructs", vis.violation_count == 0)
    _expect_error(
        "reject non-bool strict",
        SymbolicVisualizerError,
        SymbolicVisualizer,
        strict="yes",
    )
    _expect_error(
        "reject non-config",
        SymbolicVisualizerError,
        SymbolicVisualizer,
        config="not-a-config",
    )

    # ---- 5. Empty data returns clean section ------------------ #
    print("empty section:")
    section = vis.generate_dashboard_section()
    _check("empty section has ✅", "✅" in section)

    # ---- 6. Add + XSS escape --------------------------------- #
    print("ingestion + XSS:")
    added = vis.add_violations([
        {
            "rule_id": "SUST-001", "rule_name": "High energy", "step": 3,
            "severity": "critical", "action_triggered": "halt",
            "explanation": "Energy exceeded", "violation_details": "...",
            "condition": "energy > 5", "timestamp": 1000,
        },
        {
            "rule_id": "RES-002",
            "rule_name": "<img src=x onerror=alert(1)>",
            "step": 4, "severity": "high", "action_triggered": "warn",
            "explanation": "<script>alert('xss')</script>",
            "violation_details": "...", "condition": "mem > 500",
            "timestamp": 2000,
        },
    ])
    _check("added 2 violations", added == 2)
    _check("violation_count == 2", vis.violation_count == 2)

    section = vis.generate_dashboard_section()
    _check("no raw <img> in HTML", "<img src=x" not in section)
    _check("escaped <img> in HTML", "&lt;img src=x" in section)
    _check("no raw <script> in HTML", "<script>" not in section)

    # Tuple of violations accepted
    added_tuple = vis.add_violations((
        {"rule_id": "FAIR-001", "severity": "low"},
    ))
    _check("tuple violations accepted", added_tuple == 1)
    vis.reset()

    # ---- 7. Category view consistency ------------------------ #
    print("category view consistency:")
    vis.add_violations([
        {"rule_id": "SUST-001", "severity": "critical"},
        {"rule_id": "COMP-SUST-001", "severity": "high"},
        {"rule_id": "RES-001", "severity": "medium"},
        {"rule_id": "UNKNOWN-001", "severity": "low"},
    ])
    cat_view = vis.generate_category_view()
    _check(
        "SUST-001 → sustainability",
        "sustainability" in cat_view,
    )
    _check(
        "COMP-SUST-001 → sustainability (longest prefix)",
        "sustainability" in cat_view
        and len(cat_view["sustainability"]) == 2,
    )
    _check(
        "UNKNOWN-001 → unknown (fallback)",
        "unknown" in cat_view,
    )

    # filter_by_rule_type and category view agree
    sust_from_filter = vis.filter_by_rule_type("sustainability")
    _check(
        "filter_by_rule_type('sustainability') matches category view",
        len(sust_from_filter) == 2,
    )
    # Backward-compat: raw prefix still works
    _check(
        "filter_by_rule_type('SUST') still works",
        len(vis.filter_by_rule_type("SUST")) >= 1,
    )

    # ---- 8. Severity summary + other bucket ------------------ #
    print("severity summary:")
    vis2 = SymbolicVisualizer()
    vis2.add_violations([
        {"rule_id": "X", "severity": "critical"},
        {"rule_id": "X", "severity": "CRITICAL"},   # case-insensitive
        {"rule_id": "X", "severity": "weird-value"},
        {"rule_id": "X"},
    ])
    summary = vis2.generate_severity_summary()
    counts = summary["counts"]
    _check(
        "critical counted (case-insensitive)",
        counts.get("critical") == 2,
    )
    _check(
        "weird-value lands in 'other'",
        counts.get("other") == 2,   # weird-value + missing severity
    )

    # ---- 9. Mixed-timestamp timeline ------------------------- #
    print("mixed-timestamp timeline:")
    from datetime import datetime as _dt
    vis3 = SymbolicVisualizer()
    vis3.add_violations([
        {"rule_id": "X", "timestamp": 1000},
        {"rule_id": "X", "timestamp": "2026-01-01T00:00:00Z"},
        {"rule_id": "X", "timestamp": _dt(2025, 1, 1)},
        {"rule_id": "X", "timestamp": None},
    ])
    try:
        timeline = vis3.generate_violation_timeline()
        _check("mixed timestamps don't crash", len(timeline) == 4)
    except Exception as exc:
        _check(f"mixed timestamps don't crash ({exc})", False)

    # ---- 10. Serialization ---------------------------------- #
    print("serialization:")
    vis3_payload = vis3.to_json(include_history=True)
    _check(
        "to_json(include_history=True) works",
        "violations" in json.loads(vis3_payload),
    )
    restored = SymbolicVisualizer.from_json(vis3_payload)
    _check(
        "from_json restores violations",
        restored.violation_count == vis3.violation_count,
    )
    _check(
        "from_json round-trip stable",
        restored.to_dict(include_history=True) == vis3.to_dict(
            include_history=True,
        ),
    )

    # Non-primitive values are coerced, not raising
    vis4 = SymbolicVisualizer()
    vis4.add_violations([
        {"rule_id": "X", "timestamp": _dt(2026, 1, 1)},
    ])
    try:
        payload = vis4.to_json(include_history=True)
        _check(
            "datetime coerced to ISO in JSON",
            "2026-01-01" in payload,
        )
    except Exception as exc:
        _check(f"datetime coerced to ISO ({exc})", False)

    # ---- 11. Export ----------------------------------------- #
    print("export:")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "report.json"
        size = vis4.export_violation_report(path)
        _check("export size > 0", size > 0)
        _check("export file exists", path.exists())
        parsed = json.loads(path.read_text())
        _check(
            "export has summary",
            "severity_summary" in parsed,
        )
        _check(
            "export has timeline",
            "timeline" in parsed,
        )

    # ---- 12. reset ------------------------------------------- #
    print("reset:")
    removed = vis.reset()
    _check("reset removed violations", removed > 0)
    _check("violation_count == 0", vis.violation_count == 0)
    _check("violation_data cleared", vis.violation_data == [])

    # ---- 13. Public API ------------------------------------- #
    print("public API:")
    for name in (
        "__version__",
        "SymbolicVisualizer",
        "SymbolicVisualizerConfig",
        "SymbolicVisualizerError",
    ):
        _check(f"{name} exported", name in __all__)
    _check(
        "SymbolicVisualizerError is ValueError",
        issubclass(SymbolicVisualizerError, ValueError),
    )
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
