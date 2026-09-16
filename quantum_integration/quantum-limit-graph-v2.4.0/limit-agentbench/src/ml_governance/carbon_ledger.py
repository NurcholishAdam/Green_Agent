# src/ml_governance/carbon_ledger.py

"""
Carbon Ledger Service
=====================

Tracks carbon budgets and expenditures per team / model / experiment.

Enhancements
------------
- ``LedgerConfig`` — frozen, validated, centralizes path / capacity / strict.
- ``RLock``-guarded transactions and budgets; bounded ``transactions``.
- UTC timestamps.
- ``_load_ledger`` / ``_save_ledger`` honor ``strict`` mode; atomic writes.
- ``record_transaction`` auto-creates a budget if missing.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Custom ``CarbonLedgerError``; ``__repr__``; ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


class CarbonLedgerError(ValueError):
    """Raised for invalid ledger inputs, configuration, or I/O failures."""


@dataclass(frozen=True)
class LedgerConfig:
    """Tunable parameters for the carbon ledger."""
    ledger_path: str = "data/carbon_ledger.json"
    max_transactions: int = 100_000

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_path, str) or not self.ledger_path:
            raise CarbonLedgerError("ledger_path must be a non-empty string.")
        if self.max_transactions <= 0:
            raise CarbonLedgerError("max_transactions must be > 0.")


@dataclass
class CarbonTransaction:
    """Single carbon transaction."""
    transaction_id: str
    timestamp: datetime
    team: str
    task_id: str
    energy_kwh: float
    carbon_kgco2e: float
    cost_usd: float

    def __post_init__(self) -> None:
        for name in ("energy_kwh", "carbon_kgco2e", "cost_usd"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or math.isnan(float(value)):
                raise CarbonLedgerError(f"{name} must be a finite number.")
            if value < 0:
                raise CarbonLedgerError(f"{name} must be >= 0.")
        if not self.timestamp.tzinfo:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonTransaction":
        ts = data["timestamp"]
        timestamp = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
        if not timestamp.tzinfo:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return cls(
            transaction_id=str(data["transaction_id"]),
            timestamp=timestamp,
            team=str(data["team"]),
            task_id=str(data["task_id"]),
            energy_kwh=float(data["energy_kwh"]),
            carbon_kgco2e=float(data["carbon_kgco2e"]),
            cost_usd=float(data["cost_usd"]),
        )


@dataclass
class TeamBudget:
    """Team carbon budget."""
    team: str
    period: str
    budget_kgco2e: float
    used_kgco2e: float
    remaining_kgco2e: float
    num_transactions: int

    def __post_init__(self) -> None:
        if self.budget_kgco2e < 0:
            raise CarbonLedgerError("budget_kgco2e must be >= 0.")
        if self.used_kgco2e < 0:
            raise CarbonLedgerError("used_kgco2e must be >= 0.")
        if self.num_transactions < 0:
            raise CarbonLedgerError("num_transactions must be >= 0.")

    @property
    def utilization_pct(self) -> float:
        return (
            (self.used_kgco2e / self.budget_kgco2e * 100)
            if self.budget_kgco2e > 0 else 0.0
        )

    def to_dict(self) -> Dict[str, Any]:
        return {**asdict(self), "utilization_pct": self.utilization_pct}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TeamBudget":
        return cls(
            team=str(data["team"]),
            period=str(data["period"]),
            budget_kgco2e=float(data["budget_kgco2e"]),
            used_kgco2e=float(data["used_kgco2e"]),
            remaining_kgco2e=float(data.get("remaining_kgco2e", 0.0)),
            num_transactions=int(data.get("num_transactions", 0)),
        )


class CarbonLedgerService:
    """Tracks carbon budgets and transactions."""

    def __init__(
        self,
        ledger_path: Optional[Path] = None,
        *,
        config: Optional[LedgerConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = LedgerConfig(
                ledger_path=str(ledger_path) if ledger_path else "data/carbon_ledger.json"
            )
        self._strict = bool(strict)
        self.ledger_path = Path(self._config.ledger_path)

        self._lock = threading.RLock()
        self.transactions: Deque[CarbonTransaction] = deque(
            maxlen=self._config.max_transactions
        )
        self.team_budgets: Dict[str, TeamBudget] = {}

        self._load_ledger()
        logger.debug(
            "Carbon ledger initialized with %d transaction(s).",
            len(self.transactions),
        )

    # ---------------------------------------------------------- persistence
    def _load_ledger(self) -> None:
        if not self.ledger_path.exists():
            return
        try:
            with self.ledger_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to load ledger: %s", exc)
            if self._strict:
                raise CarbonLedgerError(f"Failed to load ledger: {exc}") from exc
            return

        with self._lock:
            for t in data.get("transactions", []):
                try:
                    self.transactions.append(CarbonTransaction.from_dict(t))
                except (CarbonLedgerError, KeyError, TypeError) as exc:
                    if self._strict:
                        raise
                    logger.warning("Skipping malformed transaction: %s", exc)
            for key, b in (data.get("budgets") or {}).items():
                try:
                    self.team_budgets[key] = TeamBudget.from_dict(b)
                except (CarbonLedgerError, KeyError, TypeError) as exc:
                    if self._strict:
                        raise
                    logger.warning("Skipping malformed budget %s: %s", key, exc)

    def _save_ledger(self) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            payload = {
                "transactions": [t.to_dict() for t in self.transactions],
                "budgets": {
                    k: b.to_dict() for k, b in self.team_budgets.items()
                },
            }
        try:
            fd, tmp = tempfile.mkstemp(
                prefix=self.ledger_path.name + ".", suffix=".tmp",
                dir=str(self.ledger_path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, default=str)
                os.replace(tmp, self.ledger_path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except OSError as exc:
            logger.error("Failed to save ledger: %s", exc)
            if self._strict:
                raise CarbonLedgerError(f"Failed to save ledger: {exc}") from exc

    # ---------------------------------------------------------- public API
    def set_team_budget(
        self, team: str, period: str, budget_kgco2e: float
    ) -> TeamBudget:
        """Set carbon budget for team."""
        if not isinstance(team, str) or not team:
            raise CarbonLedgerError("team must be a non-empty string.")
        if not isinstance(period, str) or not period:
            raise CarbonLedgerError("period must be a non-empty string.")
        if not isinstance(budget_kgco2e, (int, float)) or budget_kgco2e < 0:
            raise CarbonLedgerError("budget_kgco2e must be a non-negative number.")

        with self._lock:
            key = f"{team}_{period}"
            budget = TeamBudget(
                team=team, period=period, budget_kgco2e=float(budget_kgco2e),
                used_kgco2e=0.0, remaining_kgco2e=float(budget_kgco2e),
                num_transactions=0,
            )
            self.team_budgets[key] = budget
        self._save_ledger()
        return budget

    def record_transaction(
        self, team: str, task_id: str,
        energy_kwh: float, carbon_kgco2e: float, cost_usd: float,
    ) -> CarbonTransaction:
        """Record a carbon transaction and update the team budget."""
        if not isinstance(team, str) or not team:
            raise CarbonLedgerError("team must be a non-empty string.")
        if not isinstance(task_id, str) or not task_id:
            raise CarbonLedgerError("task_id must be a non-empty string.")

        now = datetime.now(timezone.utc)
        with self._lock:
            transaction = CarbonTransaction(
                transaction_id=f"txn_{len(self.transactions)}",
                timestamp=now,
                team=team, task_id=task_id,
                energy_kwh=float(energy_kwh),
                carbon_kgco2e=float(carbon_kgco2e),
                cost_usd=float(cost_usd),
            )
            self.transactions.append(transaction)

            period = now.strftime("%Y-%m")
            budget_key = f"{team}_{period}"
            budget = self.team_budgets.get(budget_key)
            if budget is None:
                # Auto-create a zero-usage budget slot so tracking is not lost.
                budget = TeamBudget(
                    team=team, period=period, budget_kgco2e=0.0,
                    used_kgco2e=0.0, remaining_kgco2e=0.0, num_transactions=0,
                )
                self.team_budgets[budget_key] = budget

            budget.used_kgco2e += float(carbon_kgco2e)
            budget.remaining_kgco2e = max(
                0.0, budget.budget_kgco2e - budget.used_kgco2e
            )
            budget.num_transactions += 1

        self._save_ledger()
        return transaction

    def get_team_budget(self, team: str, period: str) -> Optional[TeamBudget]:
        with self._lock:
            return self.team_budgets.get(f"{team}_{period}")

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            txs = list(self.transactions)
            budgets = list(self.team_budgets.values())
        if not txs:
            return {"transactions": 0, "budgets": len(budgets)}
        return {
            "transactions": len(txs),
            "budgets": len(budgets),
            "total_energy_kwh": sum(t.energy_kwh for t in txs),
            "total_carbon_kgco2e": sum(t.carbon_kgco2e for t in txs),
            "total_cost_usd": sum(t.cost_usd for t in txs),
            "teams": sorted({t.team for t in txs}),
        }

    def reset(self, *, clear_transactions: bool = False) -> None:
        with self._lock:
            if clear_transactions:
                self.transactions.clear()
        logger.debug("CarbonLedgerService reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "transactions": [t.to_dict() for t in self.transactions],
                "budgets": {k: b.to_dict() for k, b in self.team_budgets.items()},
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, ledger_path: Optional[Path] = None
    ) -> "CarbonLedgerService":
        if not isinstance(data, Mapping):
            raise CarbonLedgerError("from_dict expects a Mapping.")
        cfg_data = dict(data.get("config", {}) or {})
        cfg = LedgerConfig(
            ledger_path=str(cfg_data.get("ledger_path", "data/carbon_ledger.json")),
            max_transactions=int(cfg_data.get("max_transactions", 100_000)),
        )
        svc = cls(
            ledger_path=ledger_path, config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with svc._lock:
            for t in data.get("transactions", []):
                svc.transactions.append(CarbonTransaction.from_dict(t))
            for k, b in (data.get("budgets") or {}).items():
                svc.team_budgets[k] = TeamBudget.from_dict(b)
        return svc

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, ledger_path: Optional[Path] = None
    ) -> "CarbonLedgerService":
        try:
            return cls.from_dict(json.loads(payload), ledger_path=ledger_path)
        except json.JSONDecodeError as exc:
            raise CarbonLedgerError(f"Invalid JSON: {exc}") from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonLedgerService("
                f"path={str(self.ledger_path)!r}, "
                f"transactions={len(self.transactions)}, "
                f"budgets={len(self.team_budgets)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "CarbonLedgerError",
    "CarbonLedgerService",
    "CarbonTransaction",
    "LedgerConfig",
    "TeamBudget",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile
    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "ledger.json"
        ledger = CarbonLedgerService(ledger_path=path)
        print("repr       :", ledger)

        ledger.set_team_budget("nlp_research", "2026-03", 10.0)
        t1 = ledger.record_transaction(
            "nlp_research", "task-1", 0.05, 0.02, 0.01
        )
        t2 = ledger.record_transaction(
            "nlp_research", "task-2", 0.08, 0.03, 0.02
        )
        print("tx1        :", t1.to_dict())
        print("budget     :", ledger.get_team_budget("nlp_research", "2026-03").to_dict())
        print("stats      :", ledger.statistics())

        # Round-trip.
        payload = ledger.to_json()
        restored = CarbonLedgerService.from_json(payload, ledger_path=Path(td) / "restored.json")
        assert restored.to_dict() == ledger.to_dict()
        print("Round-trip OK.")

        # Auto-budget creation for a new team.
        ledger.record_transaction("vision_team", "task-x", 0.01, 0.005, 0.001)
        b = ledger.get_team_budget("vision_team", datetime.now(timezone.utc).strftime("%Y-%m"))
        print("auto-budget:", b.to_dict() if b else None)
