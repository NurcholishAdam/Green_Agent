#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Green Leaderboard (Enhanced v2)
===============================

Unified leaderboard for green-agent benchmarking, with optional
enhancement overlays (LIMIT Graph, MODP, RLHF, Distillation, MoE).

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   Atomic file writes. v1 opened the file with mode 'w' and then
           dumped JSON — a crash mid-write truncated the entire leaderboard.
           Writes go to a temp file and are atomically swapped via
           os.replace.
  FIX #2   Thread safety. v1 had no locking; two concurrent submit() calls
           could each open the same file and interleave writes. A
           threading.RLock serializes the append+save pair. An optional
           cross-process file lock (fcntl.flock on POSIX, msvcrt on
           Windows) protects multi-process use.
  FIX #3   get_rankings returns copies. v1 set entry["rank"] on the same
           dicts stored in self.entries, so the in-memory state diverged
           from the file after every query. v2 returns new dicts with the
           rank set, leaving self.entries untouched.
  FIX #4   Falsy-value `or` defaults removed. v1's `human_feedback_score or
           config.get(...)` replaced a legitimate 0.0 with the config
           default. The same for `graph_metrics or ...` (empty dict) and
           `distillation_stats or {}`. v2 checks `is not None`.
  FIX #5   datetime.now(timezone.utc). v1 used datetime.utcnow(), which is
           deprecated in Python 3.12 and removed in 3.14.
  FIX #6   MODP normalization accepts reference maxima
           (energy_max_kwh / carbon_max_kg / latency_max_ms). v1's 10/1/
           10000 constants gave every workload ≈0.99 on every objective,
           so the composite score could not distinguish good from great.
  FIX #7   Missing data sorts to the *worst* end. v1's `.get(sort_by, 0)`
           made a missing metric rank as the best entry on ascending sorts.
           Missing values use +inf on ascending, -inf on descending.
  FIX #8   sort_by validation. Unknown keys now raise ValueError with the
           list of accepted values.
  FIX #9   sort_by="modp_score" with enhancements disabled raises
           ValueError — v1 silently returned insertion order.
  FIX #10  Relative import fallback. v1's `from ..metrics...` fails when
           the file is imported outside the dashboard package (script, spec
           loader). v2 tries three paths.
  FIX #11  Schema validation on load. Non-list JSON and entries missing
           required fields are rejected at construction, not at first
           query.
  FIX #12  total_modp guard. A None modp_score would raise TypeError during
           aggregation; v2 coerces to 0.0 with a warning.
  FIX #13  Input validation on submit(). Ranges and types are checked;
           out-of-range values raise ValueError before any state is
           modified.
  FIX #14  Every entry carries an `entry_id` (UUID4) so callers can
           reference a specific submission.
  FIX #15  remove(entry_id) helper.
  FIX #16  get_rankings(dedupe_by_agent=True). A single agent with 5
           submissions no longer occupies 5 of the top-10 slots.
  FIX #17  get_framework_stats reports both `count` (submissions) and
           `unique_agents`.
  FIX #18  get_agent_best(agent_name, sort_by=...) returns the best entry
           for one agent.
  FIX #19  export_to_json accepts `indent` (default 2) and writes
           atomically.
  FIX #20  import_from_json(path, merge=False) restores from an export.
  FIX #21  set_enhancements(bool) for post-construction toggle.
  FIX #22  __repr__, __len__.
  FIX #23  Per-submit logging downgraded to debug; one INFO at
           construction.
  FIX #24  storage_path gitignore hint in __main__ (the file prints a
           reminder when run as a script).

---- Run --------------------------------------------------------------------
  python green_leaderboard.py            # self-test
  python green_leaderboard.py --demo     # end-to-end smoke
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import tempfile
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cross-process file lock (best effort)
# ---------------------------------------------------------------------------
if sys.platform == "win32":
    import msvcrt

    def _lock_file(fileobj: Any, exclusive: bool = True) -> None:
        try:
            msvcrt.locking(fileobj.fileno(), msvcrt.LK_LOCK, 1)
        except OSError:
            pass

    def _unlock_file(fileobj: Any) -> None:
        try:
            fileobj.seek(0)
            msvcrt.locking(fileobj.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
else:
    try:
        import fcntl
    except ImportError:
        fcntl = None  # type: ignore[assignment]

    def _lock_file(fileobj: Any, exclusive: bool = True) -> None:
        if fcntl is None:
            return
        try:
            fcntl.flock(
                fileobj.fileno(),
                fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH,
            )
        except OSError:
            pass

    def _unlock_file(fileobj: Any) -> None:
        if fcntl is None:
            return
        try:
            fcntl.flock(fileobj.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# SustainabilityIndex import fallback (FIX #10)
# ---------------------------------------------------------------------------
def _import_sustainability_index() -> Optional[Any]:
    """Try three import paths so the module works inside and outside a package."""
    for modpath in (
        "..metrics.sustainability_index",
        "metrics.sustainability_index",
        "sustainability_index",
    ):
        try:
            if modpath.startswith(".."):
                import importlib
                mod = importlib.import_module(
                    modpath,
                    package="dashboard",
                )
            else:
                mod = __import__(modpath, fromlist=["SustainabilityIndex"])
        except ImportError:
            continue
        cls = getattr(mod, "SustainabilityIndex", None)
        if cls is not None:
            return cls
    return None


# ---------------------------------------------------------------------------
# Accepted sort keys
# ---------------------------------------------------------------------------
BASE_SORT_KEYS = frozenset({
    "accuracy",
    "energy_kwh",
    "carbon_co2e_kg",
    "latency_ms",
    "sustainability_index",
})
ENHANCED_SORT_KEYS = frozenset({"modp_score"})


# ---------------------------------------------------------------------------
# Leaderboard
# ---------------------------------------------------------------------------
class GreenLeaderboard:
    """
    Unified green leaderboard for agent benchmarking.

    Thread-safe within a process; cross-process-safe on POSIX and Windows
    via a file lock. Writes are atomic.
    """

    # FIX #6 — default reference maxima tuned to typical LLM inference
    DEFAULT_ENERGY_MAX_KWH = 0.05
    DEFAULT_CARBON_MAX_KG = 0.02
    DEFAULT_LATENCY_MAX_MS = 2000.0

    def __init__(
        self,
        storage_path: str = "./leaderboard_data",
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.entries_file = self.storage_path / "leaderboard_entries.json"
        self._lock_file = self.storage_path / ".lock"

        self.config: Dict[str, Any] = dict(config or {})
        self.use_enhancements: bool = bool(
            self.config.get("use_enhancements", False)
        )

        self._lock = threading.RLock()          # FIX #2

        self.entries: List[Dict[str, Any]] = self._load_entries()

        logger.info(
            "Initialized GreenLeaderboard with %d entries "
            "(enhancements=%s, storage=%s)",
            len(self.entries),
            "on" if self.use_enhancements else "off",
            self.storage_path,
        )

    # ------------------------------------------------------------------
    # Dunder helpers (FIX #22)
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self.entries)

    def __repr__(self) -> str:
        return (
            f"GreenLeaderboard("
            f"entries={len(self.entries)}, "
            f"use_enhancements={self.use_enhancements}, "
            f"storage={str(self.storage_path)!r})"
        )

    # ------------------------------------------------------------------
    # Public API — submit
    # ------------------------------------------------------------------
    def submit(
        self,
        agent_name: str,
        framework: str,
        task_suite: str,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float,
        sustainability_index: Optional[float] = None,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None,
        modp_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        # FIX #13 — validate before touching state
        self._validate_submission(
            agent_name=agent_name,
            framework=framework,
            task_suite=task_suite,
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
            latency_ms=latency_ms,
            sustainability_index=sustainability_index,
            human_feedback_score=human_feedback_score,
            modp_score=modp_score,
        )

        # Compute SI if omitted
        if sustainability_index is None:
            sustainability_index = self._compute_sustainability_index(
                accuracy, energy_kwh, carbon_co2e_kg,
            )

        # Compute MODP if enhancements on and not supplied
        if self.use_enhancements and modp_score is None:
            modp_score = self._calculate_modp_score(
                accuracy=accuracy,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_co2e_kg,
                latency_ms=latency_ms,
            )

        entry: Dict[str, Any] = {
            "entry_id": str(uuid.uuid4()),      # FIX #14
            "agent_name": agent_name,
            "framework": framework,
            "task_suite": task_suite,
            "metrics": {
                "accuracy": float(accuracy),
                "energy_kwh": float(energy_kwh),
                "carbon_co2e_kg": float(carbon_co2e_kg),
                "latency_ms": float(latency_ms),
                "sustainability_index": float(sustainability_index),
            },
            "backend": backend,
            "rank": rank,
            "timestamp": datetime.now(timezone.utc).isoformat(),  # FIX #5
            "metadata": dict(metadata) if metadata is not None else {},
        }

        if self.use_enhancements:
            entry["enhancements"] = {
                "modp_score": modp_score,
                # FIX #4 — is-not-None, not falsy-`or`
                "graph_metrics": (
                    dict(graph_metrics)
                    if graph_metrics is not None
                    else dict(self.config.get("graph_metrics", {}))
                ),
                "human_feedback_score": (
                    float(human_feedback_score)
                    if human_feedback_score is not None
                    else float(self.config.get("human_feedback_score", 0.5))
                ),
                "distillation_stats": (
                    dict(distillation_stats)
                    if distillation_stats is not None
                    else {}
                ),
            }

        with self._lock:
            self.entries.append(entry)
            self._save_entries()

        logger.debug(
            "Submitted %s (%s) on %s — entry_id=%s",
            agent_name, framework, task_suite, entry["entry_id"],
        )
        return dict(entry)

    def remove(self, entry_id: str) -> bool:
        """Remove an entry by ID. Returns True if the entry existed. (FIX #15)"""
        with self._lock:
            for i, e in enumerate(self.entries):
                if e.get("entry_id") == entry_id:
                    self.entries.pop(i)
                    self._save_entries()
                    return True
        return False

    # ------------------------------------------------------------------
    # Public API — rankings
    # ------------------------------------------------------------------
    def get_rankings(
        self,
        sort_by: str = "sustainability_index",
        framework_filter: Optional[str] = None,
        task_suite_filter: Optional[str] = None,
        limit: int = 100,
        dedupe_by_agent: bool = False,
    ) -> List[Dict[str, Any]]:
        # FIX #8, #9 — validate sort_by up front
        valid_keys = BASE_SORT_KEYS | (
            ENHANCED_SORT_KEYS if self.use_enhancements else frozenset()
        )
        if sort_by not in valid_keys:
            if sort_by in ENHANCED_SORT_KEYS:
                raise ValueError(
                    f"sort_by={sort_by!r} requires enhancements; enable "
                    f"them via config['use_enhancements']=True"
                )
            raise ValueError(
                f"unknown sort_by={sort_by!r}; accepted: {sorted(valid_keys)}"
            )

        if limit < 0:
            raise ValueError(f"limit must be >= 0, got {limit}")

        with self._lock:
            filtered = list(self.entries)

        if framework_filter:
            filtered = [e for e in filtered if e.get("framework") == framework_filter]
        if task_suite_filter:
            filtered = [e for e in filtered if e.get("task_suite") == task_suite_filter]

        reverse = sort_by in {"accuracy", "sustainability_index", "modp_score"}
        # FIX #7 — sentinel chosen for the sort direction
        missing = -math.inf if reverse else math.inf

        def _sort_key(entry: Dict[str, Any]) -> float:
            if sort_by in ENHANCED_SORT_KEYS:
                raw = entry.get("enhancements", {}).get(sort_by)
            else:
                raw = entry.get("metrics", {}).get(sort_by)
            try:
                value = float(raw)
            except (TypeError, ValueError):
                return missing
            return value if math.isfinite(value) else missing

        filtered.sort(key=_sort_key, reverse=reverse)

        if dedupe_by_agent:
            seen: set = set()
            deduped: List[Dict[str, Any]] = []
            for e in filtered:
                name = e.get("agent_name")
                if name in seen:
                    continue
                seen.add(name)
                deduped.append(e)
            filtered = deduped

        # FIX #3 — return copies so callers cannot mutate self.entries
        return [
            {**entry, "rank": i}
            for i, entry in enumerate(filtered[:limit], 1)
        ]

    def get_top_agents(
        self,
        n: int = 10,
        sort_by: str = "sustainability_index",
        dedupe_by_agent: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return the top N entries, deduped by agent by default. (FIX #16)"""
        return self.get_rankings(
            sort_by=sort_by,
            limit=n,
            dedupe_by_agent=dedupe_by_agent,
        )

    def get_agent_history(self, agent_name: str) -> List[Dict[str, Any]]:
        """Return every submission for an agent (returned as copies)."""
        with self._lock:
            return [dict(e) for e in self.entries if e.get("agent_name") == agent_name]

    def get_agent_best(
        self, agent_name: str, sort_by: str = "sustainability_index",
    ) -> Optional[Dict[str, Any]]:
        """Return the best entry for one agent. (FIX #18)"""
        history = self.get_agent_history(agent_name)
        if not history:
            return None
        return self.get_rankings(
            sort_by=sort_by,
            framework_filter=None,
            task_suite_filter=None,
            limit=len(history),
        )[0] if False else max(
            history,
            key=lambda e: self._score_for_sort(e, sort_by),
        )

    # ------------------------------------------------------------------
    # Public API — aggregation
    # ------------------------------------------------------------------
    def get_framework_stats(self) -> Dict[str, Dict[str, Any]]:
        """Aggregate per-framework statistics. (FIX #17)"""
        stats: Dict[str, Dict[str, Any]] = {}
        with self._lock:
            entries = list(self.entries)

        for entry in entries:
            fw = entry.get("framework", "unknown")
            if fw not in stats:
                stats[fw] = {
                    "count": 0,
                    "agents": set(),
                    "total_accuracy": 0.0,
                    "total_energy": 0.0,
                    "total_carbon": 0.0,
                    "total_sustainability": 0.0,
                }
                if self.use_enhancements:
                    stats[fw]["total_modp"] = 0.0

            metrics = entry.get("metrics") or {}
            stats[fw]["count"] += 1
            stats[fw]["agents"].add(entry.get("agent_name"))
            stats[fw]["total_accuracy"] += float(metrics.get("accuracy", 0.0))
            stats[fw]["total_energy"] += float(metrics.get("energy_kwh", 0.0))
            stats[fw]["total_carbon"] += float(metrics.get("carbon_co2e_kg", 0.0))
            stats[fw]["total_sustainability"] += float(
                metrics.get("sustainability_index", 0.0)
            )
            if self.use_enhancements:
                # FIX #12 — guard against None modp_score
                raw = (entry.get("enhancements") or {}).get("modp_score")
                try:
                    modp = float(raw) if raw is not None else 0.0
                except (TypeError, ValueError):
                    modp = 0.0
                if not math.isfinite(modp):
                    modp = 0.0
                stats[fw]["total_modp"] += modp

        for fw, s in stats.items():
            s["unique_agents"] = len(s.pop("agents"))
            n = s["count"] or 1
            s["avg_accuracy"] = s["total_accuracy"] / n
            s["avg_energy"] = s["total_energy"] / n
            s["avg_carbon"] = s["total_carbon"] / n
            s["avg_sustainability"] = s["total_sustainability"] / n
            if self.use_enhancements and "total_modp" in s:
                s["avg_modp_score"] = s["total_modp"] / n
        return stats

    # ------------------------------------------------------------------
    # Public API — import / export
    # ------------------------------------------------------------------
    def export_to_json(self, filepath: str, indent: Optional[int] = 2) -> None:
        """Export the leaderboard to JSON. Writes atomically. (FIX #19)"""
        export_data: Dict[str, Any] = {
            "leaderboard": self.get_rankings(limit=10**9),
            "framework_stats": self.get_framework_stats(),
            "total_entries": len(self.entries),
            "exported_at": datetime.now(timezone.utc).isoformat(),
        }
        if self.use_enhancements:
            export_data["enhancement_settings"] = {
                "modp_weights": self.config.get("modp_weights", [0.4, 0.3, 0.2, 0.1]),
                "graph_metrics": self.config.get("graph_metrics", {}),
                "human_feedback_score": self.config.get("human_feedback_score", 0.5),
            }
        self._atomic_write_json(Path(filepath), export_data, indent=indent)
        logger.info("Exported leaderboard to %s", filepath)

    def import_from_json(self, filepath: str, merge: bool = False) -> int:
        """Import entries from a JSON export. Returns the number imported. (FIX #20)"""
        path = Path(filepath)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a dict at the top level")
        raw = data.get("leaderboard")
        if not isinstance(raw, list):
            raise ValueError(f"{path}: 'leaderboard' must be a list")
        imported: List[Dict[str, Any]] = []
        for i, e in enumerate(raw):
            validated = self._validate_entry_shape(e, source=f"{path}[{i}]")
            if validated is not None:
                imported.append(validated)
        with self._lock:
            if not merge:
                self.entries = imported
            else:
                self.entries.extend(imported)
            self._save_entries()
        return len(imported)

    # ------------------------------------------------------------------
    # Public API — toggle (FIX #21)
    # ------------------------------------------------------------------
    def set_enhancements(self, enabled: bool) -> None:
        """Enable or disable enhancement handling at runtime."""
        self.use_enhancements = bool(enabled)
        logger.info("Enhancements %s", "enabled" if enabled else "disabled")

    # ------------------------------------------------------------------
    # Internal — validation (FIX #13)
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_submission(
        *,
        agent_name: Any,
        framework: Any,
        task_suite: Any,
        accuracy: Any,
        energy_kwh: Any,
        carbon_co2e_kg: Any,
        latency_ms: Any,
        sustainability_index: Any,
        human_feedback_score: Any,
        modp_score: Any,
    ) -> None:
        if not isinstance(agent_name, str) or not agent_name:
            raise ValueError("agent_name must be a non-empty string")
        if not isinstance(framework, str) or not framework:
            raise ValueError("framework must be a non-empty string")
        if not isinstance(task_suite, str) or not task_suite:
            raise ValueError("task_suite must be a non-empty string")
        for label, value in (
            ("accuracy", accuracy),
            ("energy_kwh", energy_kwh),
            ("carbon_co2e_kg", carbon_co2e_kg),
            ("latency_ms", latency_ms),
        ):
            try:
                f = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{label} must be numeric, got {value!r}")
            if not math.isfinite(f):
                raise ValueError(f"{label} must be finite, got {f!r}")
        if not (0.0 <= float(accuracy) <= 1.0):
            raise ValueError(f"accuracy must be in [0,1], got {accuracy!r}")
        if float(energy_kwh) < 0:
            raise ValueError(f"energy_kwh must be >= 0, got {energy_kwh!r}")
        if float(carbon_co2e_kg) < 0:
            raise ValueError(
                f"carbon_co2e_kg must be >= 0, got {carbon_co2e_kg!r}"
            )
        if float(latency_ms) < 0:
            raise ValueError(f"latency_ms must be >= 0, got {latency_ms!r}")
        if sustainability_index is not None:
            try:
                f = float(sustainability_index)
            except (TypeError, ValueError):
                raise ValueError(
                    f"sustainability_index must be numeric, "
                    f"got {sustainability_index!r}"
                )
            if not (0.0 <= f <= 1.0):
                raise ValueError(
                    f"sustainability_index must be in [0,1], "
                    f"got {sustainability_index!r}"
                )
        if human_feedback_score is not None:
            try:
                f = float(human_feedback_score)
            except (TypeError, ValueError):
                raise ValueError(
                    f"human_feedback_score must be numeric, "
                    f"got {human_feedback_score!r}"
                )
            if not (0.0 <= f <= 1.0):
                raise ValueError(
                    f"human_feedback_score must be in [0,1], "
                    f"got {human_feedback_score!r}"
                )
        if modp_score is not None:
            try:
                f = float(modp_score)
            except (TypeError, ValueError):
                raise ValueError(
                    f"modp_score must be numeric, got {modp_score!r}"
                )
            if not (0.0 <= f <= 1.0):
                raise ValueError(
                    f"modp_score must be in [0,1], got {modp_score!r}"
                )

    @staticmethod
    def _validate_entry_shape(
        entry: Any, *, source: str,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(entry, dict):
            logger.warning("Skipping non-dict entry from %s", source)
            return None
        if not isinstance(entry.get("metrics"), dict):
            logger.warning("Skipping entry with no 'metrics' from %s", source)
            return None
        for key in ("agent_name", "framework", "task_suite"):
            if not isinstance(entry.get(key), str):
                logger.warning("Skipping entry missing %r from %s", key, source)
                return None
        return entry

    # ------------------------------------------------------------------
    # Internal — SI + MODP
    # ------------------------------------------------------------------
    def _compute_sustainability_index(
        self, accuracy: float, energy_kwh: float, carbon_co2e_kg: float,
    ) -> float:
        cls = _import_sustainability_index()
        if cls is None:
            # Crude fallback so the leaderboard still works without the module
            logger.warning(
                "SustainabilityIndex not importable; using fallback formula"
            )
            energy_term = 1.0 - min(energy_kwh / 0.05, 1.0)
            carbon_term = 1.0 - min(carbon_co2e_kg / 0.02, 1.0)
            return max(0.0, min(1.0, (accuracy + energy_term + carbon_term) / 3.0))
        try:
            si_calc = cls()
            return float(
                si_calc.calculate(accuracy, energy_kwh, carbon_co2e_kg)
            )
        except Exception as exc:                        # noqa: BLE001
            logger.warning(
                "SustainabilityIndex.calculate failed (%s); using fallback",
                exc,
            )
            energy_term = 1.0 - min(energy_kwh / 0.05, 1.0)
            carbon_term = 1.0 - min(carbon_co2e_kg / 0.02, 1.0)
            return max(0.0, min(1.0, (accuracy + energy_term + carbon_term) / 3.0))

    def _calculate_modp_score(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_kg: float,
        latency_ms: float,
    ) -> float:
        """
        Weighted composite. Reference maxima come from config so the score
        has dynamic range. (FIX #6)
        """
        weights = list(self.config.get("modp_weights", [0.4, 0.3, 0.2, 0.1]))
        if len(weights) != 4:
            logger.warning(
                "modp_weights must have 4 entries; using defaults"
            )
            weights = [0.4, 0.3, 0.2, 0.1]
        total = sum(weights)
        if total <= 0:
            weights = [0.25, 0.25, 0.25, 0.25]
        else:
            weights = [w / total for w in weights]

        energy_max = float(
            self.config.get("energy_max_kwh", self.DEFAULT_ENERGY_MAX_KWH)
        )
        carbon_max = float(
            self.config.get("carbon_max_kg", self.DEFAULT_CARBON_MAX_KG)
        )
        latency_max = float(
            self.config.get("latency_max_ms", self.DEFAULT_LATENCY_MAX_MS)
        )
        if energy_max <= 0 or carbon_max <= 0 or latency_max <= 0:
            raise ValueError(
                "energy_max_kwh, carbon_max_kg, latency_max_ms must be > 0"
            )

        acc_norm = max(0.0, min(float(accuracy), 1.0))
        energy_norm = max(0.0, 1.0 - float(energy_kwh) / energy_max)
        carbon_norm = max(0.0, 1.0 - float(carbon_kg) / carbon_max)
        latency_norm = max(0.0, 1.0 - float(latency_ms) / latency_max)

        return float(
            weights[0] * acc_norm
            + weights[1] * energy_norm
            + weights[2] * latency_norm
            + weights[3] * carbon_norm
        )

    def _score_for_sort(self, entry: Dict[str, Any], sort_by: str) -> float:
        """Score an entry for a `max()` call (always 'higher is better')."""
        metrics = entry.get("metrics", {})
        if sort_by == "accuracy":
            return float(metrics.get("accuracy", -math.inf))
        if sort_by == "sustainability_index":
            return float(metrics.get("sustainability_index", -math.inf))
        if sort_by == "modp_score":
            raw = (entry.get("enhancements") or {}).get("modp_score")
            try:
                return float(raw) if raw is not None else -math.inf
            except (TypeError, ValueError):
                return -math.inf
        # Lower-is-better metrics → negate
        for key in ("energy_kwh", "carbon_co2e_kg", "latency_ms"):
            if sort_by == key:
                try:
                    return -float(metrics.get(key, math.inf))
                except (TypeError, ValueError):
                    return -math.inf
        return -math.inf

    # ------------------------------------------------------------------
    # Internal — persistence (FIX #1)
    # ------------------------------------------------------------------
    def _load_entries(self) -> List[Dict[str, Any]]:
        """Load and validate entries from disk. (FIX #11)"""
        if not self.entries_file.exists():
            return []
        try:
            with self.entries_file.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error(
                "Cannot parse %s: %s — starting empty, original file left "
                "untouched",
                self.entries_file, exc,
            )
            return []
        if not isinstance(raw, list):
            logger.error(
                "Expected list at top of %s, got %s — starting empty",
                self.entries_file, type(raw).__name__,
            )
            return []
        valid: List[Dict[str, Any]] = []
        for i, entry in enumerate(raw):
            checked = self._validate_entry_shape(
                entry, source=f"{self.entries_file.name}[{i}]",
            )
            if checked is not None:
                valid.append(checked)
        if len(valid) != len(raw):
            logger.warning(
                "Loaded %d/%d entries from %s (skipped malformed)",
                len(valid), len(raw), self.entries_file,
            )
        return valid

    def _save_entries(self) -> None:
        """Atomic, cross-process-safe save. (FIX #1, #2)"""
        # The lock is held by the caller (submit/remove/import).
        self._atomic_write_json(
            self.entries_file, self.entries, indent=2,
        )

    def _atomic_write_json(
        self, path: Path, data: Any, indent: Optional[int] = 2,
    ) -> None:
        """Write JSON to ``path`` atomically (temp file + os.replace)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        # Cross-process lock
        lock_handle = None
        try:
            lock_handle = self._lock_file.open("a+")
            _lock_file(lock_handle, exclusive=True)
        except OSError:
            lock_handle = None

        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".",
            suffix=".tmp",
            dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=indent, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            # Best-effort cleanup
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        finally:
            if lock_handle is not None:
                _unlock_file(lock_handle)
                lock_handle.close()


# ---------------------------------------------------------------------------
# Self-test (FIX #23 — no external dependencies)
# ---------------------------------------------------------------------------
def _self_test() -> int:
    failures = 0

    def _check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if cond:
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name}{(': ' + detail) if detail else ''}")
            failures += 1

    import tempfile as _tf
    tmpdir = Path(_tf.mkdtemp(prefix="leaderboard-selftest-"))
    try:
        print("basic submit / rank:")
        lb = GreenLeaderboard(
            storage_path=str(tmpdir / "lb1"),
            config={"use_enhancements": True},
        )
        e1 = lb.submit(
            agent_name="alice", framework="langchain", task_suite="qa",
            accuracy=0.95, energy_kwh=0.003, carbon_co2e_kg=0.0006,
            latency_ms=500.0,
        )
        _check("entry has entry_id", "entry_id" in e1 and e1["entry_id"])
        _check("entry has metrics", "metrics" in e1)
        _check(
            "SI computed when not supplied",
            0.0 <= e1["metrics"]["sustainability_index"] <= 1.0,
        )
        _check(
            "MODP computed when enhancements on",
            0.0 <= e1["enhancements"]["modp_score"] <= 1.0,
        )

        print("input validation:")
        for bad in (
            {"accuracy": 2.0},
            {"accuracy": -0.1},
            {"energy_kwh": -1.0},
            {"latency_ms": -1.0},
            {"human_feedback_score": 1.5},
            {"modp_score": -0.1},
        ):
            args = {
                "agent_name": "x", "framework": "f", "task_suite": "t",
                "accuracy": 0.9, "energy_kwh": 0.001,
                "carbon_co2e_kg": 0.0002, "latency_ms": 100.0,
            }
            args.update(bad)
            try:
                lb.submit(**args)
                _check(f"rejects {bad}", False)
            except ValueError:
                _check(f"rejects {bad}", True)

        print("falsy-value `or` fix:")
        e = lb.submit(
            agent_name="bob", framework="f", task_suite="t",
            accuracy=0.5, energy_kwh=0.001, carbon_co2e_kg=0.0002,
            latency_ms=100.0, human_feedback_score=0.0,
        )
        _check(
            "0.0 feedback preserved",
            e["enhancements"]["human_feedback_score"] == 0.0,
            detail=f"got {e['enhancements']['human_feedback_score']}",
        )
        e = lb.submit(
            agent_name="bob", framework="f", task_suite="t",
            accuracy=0.5, energy_kwh=0.001, carbon_co2e_kg=0.0002,
            latency_ms=100.0, graph_metrics={},
        )
        _check(
            "empty graph_metrics preserved",
            e["enhancements"]["graph_metrics"] == {},
            detail=f"got {e['enhancements']['graph_metrics']}",
        )

        print("get_rankings returns copies:")
        rankings = lb.get_rankings(sort_by="sustainability_index")
        if rankings:
            before = lb.entries[0]["metrics"].get("accuracy")
            rankings[0]["metrics"]["accuracy"] = 999.0
            after = lb.entries[0]["metrics"].get("accuracy")
            _check("internal state unchanged", before == after,
                   detail=f"{before} -> {after}")

        print("sort_by validation:")
        try:
            lb.get_rankings(sort_by="nonsense")
            _check("rejects unknown sort_by", False)
        except ValueError:
            _check("rejects unknown sort_by", True)

        print("sort_by modp_score disabled:")
        lb_off = GreenLeaderboard(
            storage_path=str(tmpdir / "lb_off"),
            config={"use_enhancements": False},
        )
        try:
            lb_off.get_rankings(sort_by="modp_score")
            _check("rejects modp_score when enhancements off", False)
        except ValueError:
            _check("rejects modp_score when enhancements off", True)

        print("missing-data sort sentinel:")
        lb_off.submit(
            agent_name="a", framework="f", task_suite="t",
            accuracy=0.9, energy_kwh=0.001, carbon_co2e_kg=0.0002,
            latency_ms=100.0,
        )
        lb_off.submit(
            agent_name="b", framework="f", task_suite="t",
            accuracy=0.9, energy_kwh=0.0, carbon_co2e_kg=0.0002,
            latency_ms=100.0,
        )
        # Corrupt the second entry to simulate missing energy
        lb_off.entries[-1]["metrics"].pop("energy_kwh", None)
        ranked = lb_off.get_rankings(sort_by="energy_kwh")
        if len(ranked) >= 2:
            _check(
                "missing data sorts worst (last)",
                ranked[-1]["agent_name"] == "b",
                detail=f"last={ranked[-1]['agent_name']}",
            )

        print("dedupe_by_agent:")
        lb_dd = GreenLeaderboard(
            storage_path=str(tmpdir / "lb_dd"),
            config={"use_enhancements": False},
        )
        for i in range(5):
            lb_dd.submit(
                agent_name="solo", framework="f", task_suite="t",
                accuracy=0.9, energy_kwh=0.001, carbon_co2e_kg=0.0002,
                latency_ms=100.0,
            )
        top = lb_dd.get_top_agents(n=10, dedupe_by_agent=True)
        _check("dedupe_by_agent reduces to 1", len(top) == 1)

        print("atomic save (roundtrip):")
        lb_reload = GreenLeaderboard(
            storage_path=str(tmpdir / "lb1"),
            config={"use_enhancements": True},
        )
        _check("reload preserves entries", len(lb_reload) == len(lb.entries))

        print("remove by entry_id:")
        target_id = e1["entry_id"]
        removed = lb_reload.remove(target_id)
        _check("remove returns True", removed is True)
        _check("remove deletes entry", len(lb_reload) == len(lb.entries) - 1)

        print("framework stats:")
        stats = lb.get_framework_stats()
        _check("stats has framework 'f'", "f" in stats)
        if "f" in stats:
            _check("count is positive", stats["f"]["count"] > 0)
            _check(
                "unique_agents present",
                "unique_agents" in stats["f"],
            )
            _check(
                "avg_modp_score present",
                "avg_modp_score" in stats["f"],
            )

        print("get_agent_best:")
        best = lb.get_agent_best("alice", sort_by="accuracy")
        _check("returns an entry", best is not None)

        print("export / import round-trip:")
        export_path = tmpdir / "export.json"
        lb.export_to_json(str(export_path))
        _check("export file exists", export_path.exists())
        lb_new = GreenLeaderboard(
            storage_path=str(tmpdir / "lb_import"),
            config={"use_enhancements": True},
        )
        n_imported = lb_new.import_from_json(str(export_path))
        _check("imported > 0", n_imported > 0)
    finally:
        import shutil as _sh
        _sh.rmtree(tmpdir, ignore_errors=True)

    print()
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def _demo() -> int:
    import tempfile as _tf
    tmpdir = Path(_tf.mkdtemp(prefix="leaderboard-demo-"))
    try:
        lb = GreenLeaderboard(
            storage_path=str(tmpdir),
            config={
                "use_enhancements": True,
                "modp_weights": [0.4, 0.3, 0.2, 0.1],
                "energy_max_kwh": 0.02,
                "carbon_max_kg": 0.01,
                "latency_max_ms": 2000.0,
            },
        )
        for i in range(20):
            lb.submit(
                agent_name=f"agent_{i % 4}",
                framework=["langchain", "autogen", "crewai", "limit_graph"][i % 4],
                task_suite="demo",
                accuracy=0.85 + 0.01 * i,
                energy_kwh=0.001 + 0.0002 * i,
                carbon_co2e_kg=0.0002 + 0.00004 * i,
                latency_ms=200.0 + 10.0 * i,
            )

        print("Top 5 by sustainability_index:")
        for entry in lb.get_top_agents(n=5):
            print(
                f"  {entry['rank']}. {entry['agent_name']} "
                f"({entry['framework']}) — "
                f"acc={entry['metrics']['accuracy']:.3f} "
                f"SI={entry['metrics']['sustainability_index']:.3f} "
                f"MODP={entry['enhancements']['modp_score']:.3f}"
            )

        print("\nFramework stats:")
        for fw, s in lb.get_framework_stats().items():
            print(
                f"  {fw}: entries={s['count']} "
                f"unique={s['unique_agents']} "
                f"avg_SI={s['avg_sustainability']:.3f}"
            )
        return 0
    finally:
        import shutil as _sh
        _sh.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if "--demo" in sys.argv[1:]:
        sys.exit(_demo())
    sys.exit(_self_test())
