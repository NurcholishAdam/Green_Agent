"""
ExperimentRegistry — persistent index of quantum experiments.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExperimentRecord:
    """A single experiment registry entry."""
    experiment_id: str
    task_id: str
    request_id: str
    algorithm: str
    backend: Optional[str]
    status: str
    quality: Optional[float] = None
    shots_used: int = 0
    queue_seconds: float = 0.0
    runtime_seconds: float = 0.0
    energy_kwh: float = 0.0
    co2e_kg: float = 0.0
    signature: Optional[str] = None
    policy_version: Optional[str] = None
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["created_at"] = self.created_at.isoformat()
        return out


class ExperimentRegistry:
    """
    Thread-safe, optionally persistent index of experiments.

    When `path` is provided, records are persisted to JSON so
    experiments survive process restarts.
    """

    def __init__(self, *, path: Optional[Path] = None):
        self._records: List[ExperimentRecord] = []
        self._path = Path(path) if path else None
        self._lock = threading.RLock()
        self._load()

    def record(self, rec: ExperimentRecord) -> None:
        with self._lock:
            self._records.append(rec)
        self._persist()

    def list(
        self,
        *,
        task_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            selected = list(self._records)
        if task_id:
            selected = [r for r in selected if r.task_id == task_id]
        if status:
            selected = [r for r in selected if r.status == status]
        return [r.to_dict() for r in selected[-limit:]]

    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def _load(self) -> None:
        if self._path is None or not self._path.exists():
            return
        try:
            with open(self._path, "r") as f:
                payload = json.load(f)
            for entry in payload.get("records", []):
                try:
                    entry["created_at"] = datetime.fromisoformat(
                        entry["created_at"]
                    )
                    self._records.append(ExperimentRecord(**entry))
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"Failed to load experiment registry: {e}")

    def _persist(self) -> None:
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                payload = {
                    "records": [r.to_dict() for r in self._records],
                }
            with open(self._path, "w") as f:
                json.dump(payload, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Failed to persist experiment registry: {e}")
