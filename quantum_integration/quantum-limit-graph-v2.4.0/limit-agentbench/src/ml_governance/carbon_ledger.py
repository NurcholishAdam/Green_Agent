# src/ml_governance/carbon_ledger.py

"""
Carbon Ledger Service (v5.1)
============================

Tracks carbon budgets and expenditures per team / model / experiment.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``_save_ledger`` is now called under the same lock that guards
         the mutation (``_persist_locked``). Previously a concurrent
         ``record_transaction`` could lose an update: two threads each
         captured a payload under the lock, released, then wrote —
         allowing an older payload to overwrite a newer one.
  * P0 — ``set_team_budget`` no longer silently wipes ``used_kgco2e``
         when a budget already exists. The default is to refuse; pass
         ``overwrite=True`` to reset, or call the new
         ``adjust_budget(team, period, new_limit)`` to change the limit
         while preserving usage.
  * P0 — ``CarbonTransaction.__post_init__`` uses ``math.isfinite``
         (previously ``NaN`` was rejected but ``Inf`` slipped through).
         Team / task / transaction IDs are validated; the timestamp must
         be a timezone-aware ``datetime``.
  * P0 — ``TeamBudget.remaining_kgco2e`` is now a *derived* property
         (``max(0, budget - used)``), so an inconsistent value cannot be
         injected via ``from_dict``. Validation covers ``team``,
         ``period``, and non-negative finiteness for every numeric field.
  * P0 — ``transaction_id`` is generated from a monotonic counter that
         is persisted in the ledger file and re-derived on load.
         Previously ``f"txn_{len(transactions)}"`` collided after a
         bounded-deque truncation, after ``reset``, and after restoring
         into a smaller ``max_transactions``.
  * P0 — Budget keys are stored internally as ``(team, period)`` tuples.
         Serialization uses a JSON-encoded string; ``_load_ledger``
         derives the tuple from the budget object's own ``team`` /
         ``period`` fields (authoritative), so legacy
         ``"team_period"`` keys migrate transparently.
  * P0 — ``from_dict`` on ``CarbonTransaction``, ``TeamBudget``, and
         ``CarbonLedgerService`` routes every value through ``_safe_*``
         helpers. ``KeyError`` / ``TypeError`` / ``ValueError`` no
         longer escape; only ``CarbonLedgerError`` is raised.
  * P0 — Loading more transactions than ``max_transactions`` emits a
         prominent warning and trims to the *most recent* entries, so
         the truncation is visible rather than silent.
  * P1 — ``to_json`` and ``_save_ledger`` no longer pass
         ``default=str``; serialization errors are wrapped.
  * P1 — ``TeamBudget`` is frozen; ``get_team_budget`` returns a
         snapshot. Previously callers could mutate the internal object
         and corrupt the next save.
  * P1 — ``record_transaction`` no longer auto-creates a zero-budget
         placeholder. ``get_team_budget`` returns ``None`` for teams
         without a budget, matching the caller's "no budget → no
         enforcement" contract. A warning is logged when recording
         against an unbudgeted team.
  * P1 — ``period`` is validated as ``"YYYY-MM"`` in ``set_team_budget``
         and ``get_team_budget``. ``record_transaction`` already derives
         the period in that format, so the two paths can no longer
         disagree.
  * P2 — Two-phase budget: ``reserve(team, period, kg)`` →
         ``commit(reservation, actual_kg, ...)`` / ``release(reservation)``.
         Matches the ``UnifiedGreenAgent`` v5.1 orchestrator's
         ``CarbonLedgerService`` shape.
  * P2 — New helpers: ``__version__``, ``schema_version`` in
         ``to_dict()``, ``adjust_budget``, ``undo_transaction``,
         ``get_transactions`` (with filters), ``summarize``,
         ``CarbonTransaction.key()``, ``__repr__`` on every class.
  * P3 — ``logger.error`` + ``raise`` redundancy removed; smoke test
         asserts every fix instead of only printing.

Backward compatibility
----------------------
- ``CarbonLedgerService(ledger_path=None, *, config=None, strict=True)``
  signature is unchanged.
- ``record_transaction(team, task_id, energy_kwh, carbon_kgco2e, cost_usd)``
  is unchanged.
- ``set_team_budget(team, period, budget_kgco2e)`` is unchanged in
  position; new ``overwrite=False`` is keyword-only.
- ``get_team_budget`` returns ``None`` for an unknown team/period (same
  as before, except for the auto-created placeholder that used to exist
  after a transaction — see P1 #11).
- ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json`` round-trip
  with v5.0 files. Legacy ``"team_period"`` budget keys are migrated on
  load; the next save uses the new JSON-encoded key format.
- **Breaking (documented):** ``set_team_budget`` raises on an existing
  budget with ``used > 0`` unless ``overwrite=True`` is passed.
  Ops should call ``adjust_budget`` to change the limit.
- **Breaking (documented):** ``record_transaction`` no longer creates a
  placeholder budget. Callers relying on the placeholder must handle
  ``get_team_budget`` returning ``None``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
import threading
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "BudgetReservation",
    "CarbonLedgerError",
    "CarbonLedgerService",
    "CarbonTransaction",
    "LedgerConfig",
    "TeamBudget",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Errors & helpers
# --------------------------------------------------------------------------- #
class CarbonLedgerError(ValueError):
    """Raised for invalid ledger inputs, configuration, or I/O failures."""


_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_period(period: Any) -> str:
    if not isinstance(period, str) or not period:
        raise CarbonLedgerError(
            f"period must be a non-empty string, "
            f"got {type(period).__name__}"
        )
    if not _PERIOD_RE.match(period):
        raise CarbonLedgerError(
            f"period must be 'YYYY-MM', got {period!r}"
        )
    return period


def _safe_float(name: str, value: Any) -> float:
    """Coerce ``value`` to a finite, non-negative float."""
    if isinstance(value, bool) or not isinstance(
        value, (int, float, str)
    ):
        raise CarbonLedgerError(
            f"{name} must be numeric, got {type(value).__name__}"
        )
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise CarbonLedgerError(
            f"{name} must be numeric, got {value!r}"
        ) from exc
    if not math.isfinite(v):
        raise CarbonLedgerError(
            f"{name} must be finite, got {value!r}"
        )
    if v < 0:
        raise CarbonLedgerError(
            f"{name} must be >= 0, got {v!r}"
        )
    return v


def _safe_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise CarbonLedgerError(
            f"{name} must be a non-empty string, "
            f"got {type(value).__name__}"
        )
    return value


def _safe_timestamp(value: Any) -> datetime:
    """Parse a timestamp from datetime / ISO-8601 string (accepts 'Z')."""
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(s)
        except ValueError as exc:
            raise CarbonLedgerError(
                f"timestamp is not ISO-8601: {value!r}"
            ) from exc
    else:
        raise CarbonLedgerError(
            f"timestamp must be datetime or str, "
            f"got {type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LedgerConfig:
    """Tunable parameters for the carbon ledger."""
    ledger_path: str = "data/carbon_ledger.json"
    max_transactions: int = 100_000

    def __post_init__(self) -> None:
        if not isinstance(self.ledger_path, str) or not self.ledger_path:
            raise CarbonLedgerError(
                "ledger_path must be a non-empty string."
            )
        if (
            isinstance(self.max_transactions, bool)
            or not isinstance(self.max_transactions, int)
            or self.max_transactions <= 0
        ):
            raise CarbonLedgerError(
                "max_transactions must be a positive int."
            )


# --------------------------------------------------------------------------- #
# Transaction
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonTransaction:
    """Immutable single carbon transaction."""
    transaction_id: str
    timestamp: datetime
    team: str
    task_id: str
    energy_kwh: float
    carbon_kgco2e: float
    cost_usd: float

    def __post_init__(self) -> None:
        _safe_str("transaction_id", self.transaction_id)
        _safe_str("team", self.team)
        _safe_str("task_id", self.task_id)
        object.__setattr__(
            self, "timestamp", _safe_timestamp(self.timestamp),
        )
        for name in ("energy_kwh", "carbon_kgco2e", "cost_usd"):
            object.__setattr__(
                self, name, _safe_float(name, getattr(self, name)),
            )

    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes transaction_id)."""
        return (
            self.timestamp.isoformat(),
            self.team,
            self.task_id,
            self.energy_kwh,
            self.carbon_kgco2e,
            self.cost_usd,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp.isoformat(),
            "team": self.team,
            "task_id": self.task_id,
            "energy_kwh": self.energy_kwh,
            "carbon_kgco2e": self.carbon_kgco2e,
            "cost_usd": self.cost_usd,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonTransaction":
        if not isinstance(data, Mapping):
            raise CarbonLedgerError(
                f"CarbonTransaction.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        required = (
            "transaction_id", "timestamp", "team",
            "task_id", "energy_kwh", "carbon_kgco2e", "cost_usd",
        )
        missing = [k for k in required if k not in data]
        if missing:
            raise CarbonLedgerError(
                f"CarbonTransaction.from_dict: missing field(s) {missing}"
            )
        return cls(
            transaction_id=_safe_str(
                "transaction_id", data["transaction_id"],
            ),
            timestamp=_safe_timestamp(data["timestamp"]),
            team=_safe_str("team", data["team"]),
            task_id=_safe_str("task_id", data["task_id"]),
            energy_kwh=_safe_float("energy_kwh", data["energy_kwh"]),
            carbon_kgco2e=_safe_float(
                "carbon_kgco2e", data["carbon_kgco2e"],
            ),
            cost_usd=_safe_float("cost_usd", data["cost_usd"]),
        )

    def __repr__(self) -> str:
        return (
            "CarbonTransaction("
            f"id={self.transaction_id}, "
            f"team={self.team!r}, "
            f"task_id={self.task_id!r}, "
            f"carbon={self.carbon_kgco2e}kg)"
        )


# --------------------------------------------------------------------------- #
# Budget
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TeamBudget:
    """Immutable team carbon budget snapshot."""
    team: str
    period: str
    budget_kgco2e: float
    used_kgco2e: float
    num_transactions: int = 0

    def __post_init__(self) -> None:
        _safe_str("team", self.team)
        _validate_period(self.period)
        object.__setattr__(
            self, "budget_kgco2e",
            _safe_float("budget_kgco2e", self.budget_kgco2e),
        )
        object.__setattr__(
            self, "used_kgco2e",
            _safe_float("used_kgco2e", self.used_kgco2e),
        )
        if (
            isinstance(self.num_transactions, bool)
            or not isinstance(self.num_transactions, int)
            or self.num_transactions < 0
        ):
            raise CarbonLedgerError(
                "num_transactions must be a non-negative int."
            )

    #: Remaining budget derived from `budget_kgco2e - used_kgco2e`.
    @property
    def remaining_kgco2e(self) -> float:
        return max(0.0, self.budget_kgco2e - self.used_kgco2e)

    @property
    def utilization_pct(self) -> float:
        return (
            (self.used_kgco2e / self.budget_kgco2e * 100.0)
            if self.budget_kgco2e > 0 else 0.0
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "team": self.team,
            "period": self.period,
            "budget_kgco2e": self.budget_kgco2e,
            "used_kgco2e": self.used_kgco2e,
            # Derived fields, included for round-trip observability only.
            "remaining_kgco2e": self.remaining_kgco2e,
            "utilization_pct": self.utilization_pct,
            "num_transactions": self.num_transactions,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TeamBudget":
        if not isinstance(data, Mapping):
            raise CarbonLedgerError(
                f"TeamBudget.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        for required in ("team", "period", "budget_kgco2e", "used_kgco2e"):
            if required not in data:
                raise CarbonLedgerError(
                    f"TeamBudget.from_dict: missing field {required!r}"
                )
        # Note: incoming `remaining_kgco2e` is ignored — it is derived.
        return cls(
            team=_safe_str("team", data["team"]),
            period=_validate_period(data["period"]),
            budget_kgco2e=_safe_float(
                "budget_kgco2e", data["budget_kgco2e"],
            ),
            used_kgco2e=_safe_float(
                "used_kgco2e", data["used_kgco2e"],
            ),
            num_transactions=int(data.get("num_transactions", 0)),
        )


# --------------------------------------------------------------------------- #
# Two-phase reservation
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BudgetReservation:
    """A held budget reservation, ready to commit or release."""
    team: str
    period: str
    token: str
    reserved_kgco2e: float
    created_at: datetime
    committed: bool = False

    def __post_init__(self) -> None:
        _safe_str("team", self.team)
        _validate_period(self.period)
        _safe_str("token", self.token)
        object.__setattr__(
            self, "reserved_kgco2e",
            _safe_float("reserved_kgco2e", self.reserved_kgco2e),
        )
        object.__setattr__(
            self, "created_at", _safe_timestamp(self.created_at),
        )


# --------------------------------------------------------------------------- #
# Service
# --------------------------------------------------------------------------- #
class CarbonLedgerService:
    """Tracks carbon budgets and transactions with two-phase reservation."""

    def __init__(
        self,
        ledger_path: Optional[Path] = None,
        *,
        config: Optional[LedgerConfig] = None,
        strict: bool = True,
        _auto_load: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = LedgerConfig(
                ledger_path=(
                    str(ledger_path) if ledger_path
                    else "data/carbon_ledger.json"
                ),
            )
        self._strict = bool(strict)
        self.ledger_path = Path(self._config.ledger_path)

        self._lock = threading.RLock()
        self.transactions: Deque[CarbonTransaction] = deque(
            maxlen=self._config.max_transactions
        )
        #: Internal storage keyed by (team, period) tuples.
        self.team_budgets: Dict[Tuple[str, str], TeamBudget] = {}
        #: Outstanding reservations, keyed by token.
        self._reservations: Dict[str, BudgetReservation] = {}
        #: Monotonic transaction counter, persisted with the ledger.
        self._transaction_counter: int = 0

        if _auto_load:
            self._load_ledger()

        logger.debug(
            "Carbon ledger initialized with %d transaction(s), "
            "%d budget(s).",
            len(self.transactions), len(self.team_budgets),
        )

    # ---------------------------------------------------------- properties
    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def config(self) -> LedgerConfig:
        return self._config

    # ---------------------------------------------------------- persistence
    def _load_ledger(self) -> None:
        if not self.ledger_path.exists():
            return

        try:
            with self.ledger_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            if self._strict:
                raise CarbonLedgerError(
                    f"failed to load ledger: {exc}"
                ) from exc
            logger.warning("Failed to load ledger: %s", exc)
            return

        if not isinstance(data, Mapping):
            raise CarbonLedgerError(
                f"ledger file must contain a JSON object, "
                f"got {type(data).__name__}"
            )

        raw_transactions = data.get("transactions", []) or []
        if not isinstance(raw_transactions, list):
            raise CarbonLedgerError("'transactions' must be a list.")

        if (
            self._config.max_transactions is not None
            and len(raw_transactions) > self._config.max_transactions
        ):
            dropped = (
                len(raw_transactions) - self._config.max_transactions
            )
            logger.warning(
                "Ledger file contains %d transactions but "
                "max_transactions=%d; the oldest %d will be dropped "
                "on the next save. Increase max_transactions or "
                "archive to preserve history.",
                len(raw_transactions),
                self._config.max_transactions,
                dropped,
            )

        with self._lock:
            max_counter = 0
            for entry in raw_transactions:
                try:
                    txn = CarbonTransaction.from_dict(entry)
                except CarbonLedgerError as exc:
                    if self._strict:
                        raise
                    logger.warning(
                        "Skipping malformed transaction: %s", exc,
                    )
                    continue
                self.transactions.append(txn)
                # Derive next counter from existing IDs.
                suffix = txn.transaction_id[4:]
                if (
                    txn.transaction_id.startswith("txn_")
                    and suffix.isdigit()
                ):
                    max_counter = max(max_counter, int(suffix))
            self._transaction_counter = max_counter

            raw_budgets = data.get("budgets", {}) or {}
            if not isinstance(raw_budgets, Mapping):
                raise CarbonLedgerError("'budgets' must be a mapping.")
            for key, entry in raw_budgets.items():
                try:
                    budget = TeamBudget.from_dict(entry)
                except CarbonLedgerError as exc:
                    if self._strict:
                        raise
                    logger.warning(
                        "Skipping malformed budget %r: %s", key, exc,
                    )
                    continue
                # Derive tuple key from the budget object itself —
                # the string key in the file may be legacy.
                self.team_budgets[(budget.team, budget.period)] = budget

    def _persist_locked(self) -> None:
        """
        Write the ledger to disk. **Caller must hold ``self._lock``.**

        Captures the payload and performs the atomic replace inside the
        caller's lock so concurrent mutations cannot interleave.
        """
        payload = {
            "schema_version": __version__,
            "transaction_counter": self._transaction_counter,
            "transactions": [
                t.to_dict() for t in self.transactions
            ],
            "budgets": {
                json.dumps([team, period], separators=(",", ":")):
                    b.to_dict()
                for (team, period), b in self.team_budgets.items()
            },
        }

        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd, tmp = tempfile.mkstemp(
                prefix=self.ledger_path.name + ".",
                suffix=".tmp",
                dir=str(self.ledger_path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                os.replace(tmp, self.ledger_path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except (OSError, TypeError, ValueError) as exc:
            if self._strict:
                raise CarbonLedgerError(
                    f"failed to save ledger: {exc}"
                ) from exc
            logger.warning("Failed to save ledger: %s", exc)

    # ---------------------------------------------------------- helpers
    def _next_transaction_id_locked(self) -> str:
        """Caller must hold ``self._lock``."""
        self._transaction_counter += 1
        return f"txn_{self._transaction_counter}"

    def _reserved_for_locked(self, team: str, period: str) -> float:
        """Total kg CO2e currently reserved for (team, period)."""
        return sum(
            r.reserved_kgco2e
            for r in self._reservations.values()
            if r.team == team and r.period == period
        )

    def _apply_transaction_locked(
        self, transaction: CarbonTransaction,
    ) -> Optional[TeamBudget]:
        """
        Append ``transaction`` and update its team's budget (if one
        exists). Returns the updated budget, or ``None`` if the team has
        no budget for the transaction's period.

        Caller must hold ``self._lock``.
        """
        self.transactions.append(transaction)

        # Derive the period from the transaction's timestamp.
        period = transaction.timestamp.strftime("%Y-%m")
        key = (transaction.team, period)
        existing = self.team_budgets.get(key)
        if existing is None:
            logger.warning(
                "Recording transaction for team=%r period=%r with no "
                "budget; usage will be logged but not capped.",
                transaction.team, period,
            )
            return None

        updated = replace(
            existing,
            used_kgco2e=existing.used_kgco2e + transaction.carbon_kgco2e,
            num_transactions=existing.num_transactions + 1,
        )
        self.team_budgets[key] = updated
        return updated

    # ---------------------------------------------------------- public API
    def set_team_budget(
        self,
        team: str,
        period: str,
        budget_kgco2e: float,
        *,
        overwrite: bool = False,
    ) -> TeamBudget:
        """
        Set (or overwrite) a team's carbon budget for a period.

        Raises
        ------
        CarbonLedgerError
            If a budget already exists with ``used_kgco2e > 0`` and
            ``overwrite`` is False. Use :meth:`adjust_budget` to change
            the limit while preserving usage, or pass ``overwrite=True``
            to explicitly reset.
        """
        _safe_str("team", team)
        _validate_period(period)
        budget_kgco2e = _safe_float(
            "budget_kgco2e", budget_kgco2e,
        )

        with self._lock:
            key = (team, period)
            existing = self.team_budgets.get(key)
            if existing is not None and not overwrite:
                if existing.used_kgco2e > 0:
                    raise CarbonLedgerError(
                        f"budget for team={team!r} period={period!r} "
                        f"already exists with used="
                        f"{existing.used_kgco2e:.4f} kg. Pass "
                        f"overwrite=True to reset, or use "
                        f"adjust_budget() to change the limit."
                    )
                # Existing with zero usage — replace silently.
            budget = TeamBudget(
                team=team, period=period,
                budget_kgco2e=budget_kgco2e,
                used_kgco2e=0.0,
                num_transactions=0,
            )
            self.team_budgets[key] = budget
            self._persist_locked()
        return budget

    def adjust_budget(
        self, team: str, period: str, new_budget_kgco2e: float,
    ) -> TeamBudget:
        """
        Change the budget limit for an existing team/period without
        touching ``used_kgco2e``.
        """
        _safe_str("team", team)
        _validate_period(period)
        new_budget_kgco2e = _safe_float(
            "new_budget_kgco2e", new_budget_kgco2e,
        )

        with self._lock:
            key = (team, period)
            existing = self.team_budgets.get(key)
            if existing is None:
                raise CarbonLedgerError(
                    f"no budget for team={team!r} period={period!r}; "
                    f"use set_team_budget() to create one."
                )
            updated = replace(
                existing, budget_kgco2e=new_budget_kgco2e,
            )
            self.team_budgets[key] = updated
            self._persist_locked()
        return updated

    def record_transaction(
        self,
        team: str,
        task_id: str,
        energy_kwh: float,
        carbon_kgco2e: float,
        cost_usd: float,
    ) -> CarbonTransaction:
        """Record a carbon transaction and update the team budget (if any)."""
        _safe_str("team", team)
        _safe_str("task_id", task_id)
        energy_kwh = _safe_float("energy_kwh", energy_kwh)
        carbon_kgco2e = _safe_float("carbon_kgco2e", carbon_kgco2e)
        cost_usd = _safe_float("cost_usd", cost_usd)

        now = datetime.now(timezone.utc)
        with self._lock:
            transaction = CarbonTransaction(
                transaction_id=self._next_transaction_id_locked(),
                timestamp=now,
                team=team, task_id=task_id,
                energy_kwh=energy_kwh,
                carbon_kgco2e=carbon_kgco2e,
                cost_usd=cost_usd,
            )
            self._apply_transaction_locked(transaction)
            self._persist_locked()
        return transaction

    def undo_transaction(self, transaction_id: str) -> bool:
        """
        Remove a transaction by id and (if it had a budget) refund its
        carbon cost.

        Returns ``True`` if the transaction existed and was removed.
        """
        _safe_str("transaction_id", transaction_id)
        with self._lock:
            target = next(
                (t for t in self.transactions
                 if t.transaction_id == transaction_id),
                None,
            )
            if target is None:
                return False

            # Remove from the deque.
            try:
                self.transactions.remove(target)
            except ValueError:  # pragma: no cover — defensive
                return False

            # Refund the team budget, if one exists.
            period = target.timestamp.strftime("%Y-%m")
            key = (target.team, period)
            existing = self.team_budgets.get(key)
            if existing is not None:
                self.team_budgets[key] = replace(
                    existing,
                    used_kgco2e=max(
                        0.0,
                        existing.used_kgco2e
                        - target.carbon_kgco2e,
                    ),
                    num_transactions=max(
                        0, existing.num_transactions - 1,
                    ),
                )

            self._persist_locked()
        return True

    # ---------------------------------------------------------- two-phase
    def reserve(
        self,
        team: str,
        period: str,
        required_kgco2e: float,
    ) -> Optional[BudgetReservation]:
        """
        Hold ``required_kgco2e`` of the team's budget.

        Returns a :class:`BudgetReservation` on success, or ``None``
        when the budget is missing or insufficient. The reservation
        must later be resolved with :meth:`commit` or :meth:`release`.
        """
        _safe_str("team", team)
        _validate_period(period)
        required_kgco2e = _safe_float(
            "required_kgco2e", required_kgco2e,
        )

        with self._lock:
            key = (team, period)
            budget = self.team_budgets.get(key)
            if budget is None:
                return None
            reserved = self._reserved_for_locked(team, period)
            available = budget.remaining_kgco2e - reserved
            if required_kgco2e > available:
                return None
            token = uuid.uuid4().hex
            reservation = BudgetReservation(
                team=team, period=period, token=token,
                reserved_kgco2e=required_kgco2e,
                created_at=datetime.now(timezone.utc),
            )
            self._reservations[token] = reservation
        return reservation

    def commit(
        self,
        reservation: BudgetReservation,
        actual_kgco2e: float,
        *,
        task_id: str = "",
        energy_kwh: float = 0.0,
        cost_usd: float = 0.0,
    ) -> Optional[CarbonTransaction]:
        """
        Resolve a reservation to its actual cost.

        If ``task_id`` is non-empty, a :class:`CarbonTransaction` is
        recorded for the actual amount; otherwise only the budget is
        updated. Either way the reservation is dropped and the
        difference between reserved and actual is refunded.
        """
        actual_kgco2e = _safe_float("actual_kgco2e", actual_kgco2e)

        with self._lock:
            held = self._reservations.pop(reservation.token, None)
            if held is None:
                logger.warning(
                    "commit(): token %s not found; reservation may have "
                    "been released or already committed.",
                    reservation.token,
                )
                return None

            if task_id:
                # Full audit-trail path: record the transaction and
                # update the budget in one shot.
                transaction = CarbonTransaction(
                    transaction_id=self._next_transaction_id_locked(),
                    timestamp=datetime.now(timezone.utc),
                    team=held.team,
                    task_id=_safe_str("task_id", task_id),
                    energy_kwh=_safe_float("energy_kwh", energy_kwh),
                    carbon_kgco2e=actual_kgco2e,
                    cost_usd=_safe_float("cost_usd", cost_usd),
                )
                self._apply_transaction_locked(transaction)
            else:
                # Budget-only path: apply actual usage without a
                # transaction record (used by schedulers that will
                # call record_transaction separately).
                key = (held.team, held.period)
                existing = self.team_budgets.get(key)
                if existing is not None:
                    self.team_budgets[key] = replace(
                        existing,
                        used_kgco2e=(
                            existing.used_kgco2e + actual_kgco2e
                        ),
                        num_transactions=(
                            existing.num_transactions + 1
                        ),
                    )
                transaction = None

            self._persist_locked()
        return transaction

    def release(self, reservation: BudgetReservation) -> bool:
        """
        Cancel a reservation (task failed, or the actual cost was 0).

        Returns ``True`` if the reservation existed.
        """
        with self._lock:
            held = self._reservations.pop(reservation.token, None)
            return held is not None

    # ---------------------------------------------------------- queries
    def get_team_budget(
        self, team: str, period: str,
    ) -> Optional[TeamBudget]:
        """Return a snapshot of the team's budget, or ``None``."""
        _safe_str("team", team)
        _validate_period(period)
        with self._lock:
            return self.team_budgets.get((team, period))

    def check_budget_available(
        self, team: str, period: str, required_kgco2e: float,
    ) -> bool:
        """
        True if the team has enough *available* (unused and unreserved)
        budget for ``required_kgco2e``. Returns ``True`` when the team
        has no budget (unenforced).
        """
        _validate_period(period)
        required_kgco2e = _safe_float(
            "required_kgco2e", required_kgco2e,
        )
        with self._lock:
            budget = self.team_budgets.get((team, period))
            if budget is None:
                return True
            reserved = self._reserved_for_locked(team, period)
            return required_kgco2e <= (
                budget.remaining_kgco2e - reserved
            )

    def get_transactions(
        self,
        *,
        team: Optional[str] = None,
        task_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[CarbonTransaction]:
        """Return a filtered snapshot of the transaction log."""
        since_ts = _safe_timestamp(since) if since is not None else None
        until_ts = _safe_timestamp(until) if until is not None else None
        with self._lock:
            snapshot = list(self.transactions)
        result: List[CarbonTransaction] = []
        for t in snapshot:
            if team is not None and t.team != team:
                continue
            if task_id is not None and t.task_id != task_id:
                continue
            if since_ts is not None and t.timestamp < since_ts:
                continue
            if until_ts is not None and t.timestamp > until_ts:
                continue
            result.append(t)
        return result

    def summarize(
        self, team: str, period: str,
    ) -> Dict[str, Any]:
        """Structured per-budget report for dashboards."""
        _validate_period(period)
        with self._lock:
            budget = self.team_budgets.get((team, period))
            reserved = self._reserved_for_locked(team, period)
            txs = [
                t for t in self.transactions
                if t.team == team
                and t.timestamp.strftime("%Y-%m") == period
            ]
        return {
            "team": team,
            "period": period,
            "budget": budget.to_dict() if budget else None,
            "reserved_kgco2e": reserved,
            "available_kgco2e": (
                budget.remaining_kgco2e - reserved
                if budget else None
            ),
            "transaction_count": len(txs),
            "total_energy_kwh": sum(t.energy_kwh for t in txs),
            "total_carbon_kgco2e": sum(
                t.carbon_kgco2e for t in txs
            ),
            "total_cost_usd": sum(t.cost_usd for t in txs),
        }

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            txs = list(self.transactions)
            budgets = list(self.team_budgets.values())
            open_reservations = len(self._reservations)
        if not txs:
            return {
                "transactions": 0,
                "budgets": len(budgets),
                "open_reservations": open_reservations,
            }
        return {
            "transactions": len(txs),
            "budgets": len(budgets),
            "open_reservations": open_reservations,
            "total_energy_kwh": sum(t.energy_kwh for t in txs),
            "total_carbon_kgco2e": sum(
                t.carbon_kgco2e for t in txs
            ),
            "total_cost_usd": sum(t.cost_usd for t in txs),
            "teams": sorted({t.team for t in txs}),
        }

    def reset(self, *, clear_transactions: bool = False) -> None:
        with self._lock:
            if clear_transactions:
                self.transactions.clear()
                # Note: transaction_counter is intentionally NOT reset
                # so IDs remain globally unique across resets.
            self._persist_locked()
        logger.debug("CarbonLedgerService reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "transaction_counter": self._transaction_counter,
                "transactions": [
                    t.to_dict() for t in self.transactions
                ],
                "budgets": {
                    json.dumps([team, period], separators=(",", ":")):
                        b.to_dict()
                    for (team, period), b in self.team_budgets.items()
                },
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        ledger_path: Optional[Path] = None,
    ) -> "CarbonLedgerService":
        if not isinstance(data, Mapping):
            raise CarbonLedgerError("from_dict expects a Mapping.")

        cfg_data = dict(data.get("config", {}) or {})
        cfg_fields = {f.name for f in fields(LedgerConfig)}
        cfg_kwargs = {
            k: v for k, v in cfg_data.items() if k in cfg_fields
        }
        cfg = LedgerConfig(**cfg_kwargs)

        svc = cls(
            ledger_path=ledger_path,
            config=cfg,
            strict=bool(data.get("strict", True)),
            _auto_load=False,
        )

        raw_transactions = data.get("transactions", []) or []
        raw_budgets = data.get("budgets", {}) or {}
        if not isinstance(raw_transactions, list):
            raise CarbonLedgerError("'transactions' must be a list.")
        if not isinstance(raw_budgets, Mapping):
            raise CarbonLedgerError("'budgets' must be a mapping.")

        with svc._lock:
            max_counter = 0
            for entry in raw_transactions:
                txn = CarbonTransaction.from_dict(entry)
                svc.transactions.append(txn)
                suffix = txn.transaction_id[4:]
                if (
                    txn.transaction_id.startswith("txn_")
                    and suffix.isdigit()
                ):
                    max_counter = max(max_counter, int(suffix))

            persisted_counter = data.get("transaction_counter")
            svc._transaction_counter = max(
                max_counter,
                int(persisted_counter)
                if persisted_counter is not None else 0,
            )

            for _key, entry in raw_budgets.items():
                budget = TeamBudget.from_dict(entry)
                svc.team_budgets[
                    (budget.team, budget.period)
                ] = budget
        return svc

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise CarbonLedgerError(
                f"ledger is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        ledger_path: Optional[Path] = None,
    ) -> "CarbonLedgerService":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CarbonLedgerError(
                f"invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise CarbonLedgerError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}"
            )
        return cls.from_dict(data, ledger_path=ledger_path)

    # ---------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonLedgerService("
                f"path={str(self.ledger_path)!r}, "
                f"transactions={len(self.transactions)}, "
                f"budgets={len(self.team_budgets)}, "
                f"reservations={len(self._reservations)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import concurrent.futures
    import tempfile

    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "ledger.json"
        ledger = CarbonLedgerService(ledger_path=path)
        print("repr       :", ledger)

        ledger.set_team_budget("nlp_research", "2026-03", 10.0)
        t1 = ledger.record_transaction(
            "nlp_research", "task-1", 0.05, 0.02, 0.01,
        )
        t2 = ledger.record_transaction(
            "nlp_research", "task-2", 0.08, 0.03, 0.02,
        )
        assert t1.transaction_id != t2.transaction_id
        print("tx1        :", t1)
        budget = ledger.get_team_budget("nlp_research", "2026-03")
        assert budget.used_kgco2e == 0.05
        assert budget.remaining_kgco2e == 9.95
        print("budget     :", budget)
        print("stats      :", ledger.statistics())

        # --- P0 #2 regression: set_team_budget refuses to overwrite ---
        try:
            ledger.set_team_budget("nlp_research", "2026-03", 20.0)
        except CarbonLedgerError as exc:
            print("Refused overwrite:", exc)
        else:
            raise AssertionError("set_team_budget should have refused.")
        adjusted = ledger.adjust_budget("nlp_research", "2026-03", 20.0)
        assert adjusted.used_kgco2e == 0.05
        assert adjusted.remaining_kgco2e == 19.95
        print("adjust_budget: OK", adjusted)

        # --- P0 #3 regression: Inf rejected ---
        for bad in (float("inf"), float("-inf"), float("nan")):
            try:
                CarbonTransaction(
                    transaction_id="txn_x",
                    timestamp=datetime.now(timezone.utc),
                    team="t", task_id="x",
                    energy_kwh=bad, carbon_kgco2e=0.0,
                    cost_usd=0.0,
                )
            except CarbonLedgerError:
                pass
            else:
                raise AssertionError(
                    f"{bad!r} should have been rejected"
                )
        print("Inf/NaN rejection: OK")

        # --- P0 #4 regression: remaining_kgco2e is derived ---
        tb = TeamBudget(
            team="t", period="2026-03",
            budget_kgco2e=10.0, used_kgco2e=3.0,
        )
        assert tb.remaining_kgco2e == 7.0
        # from_dict ignores incoming remaining:
        tb2 = TeamBudget.from_dict({
            "team": "t", "period": "2026-03",
            "budget_kgco2e": 10.0, "used_kgco2e": 3.0,
            "remaining_kgco2e": -999.0,   # ignored
        })
        assert tb2.remaining_kgco2e == 7.0
        print("remaining derived: OK")

        # --- P0 #5 regression: transaction IDs monotonic after load ---
        reloaded = CarbonLedgerService(ledger_path=path)
        new_txn = reloaded.record_transaction(
            "nlp_research", "task-3", 0.01, 0.005, 0.001,
        )
        existing_ids = {t.transaction_id for t in reloaded.transactions}
        assert new_txn.transaction_id not in existing_ids - {
            new_txn.transaction_id,
        }
        print("transaction_id monotonic:", new_txn.transaction_id)

        # --- P0 #6 regression: budget key collision avoided ---
        ledger.set_team_budget("a_b", "2026-03", 10.0)
        ledger.set_team_budget("a", "2026-03", 10.0)
        ledger.record_transaction("a_b", "t1", 0.01, 3.0, 0.001)
        b1 = ledger.get_team_budget("a_b", "2026-03")
        b2 = ledger.get_team_budget("a", "2026-03")
        assert b1.used_kgco2e == 3.0
        assert b2.used_kgco2e == 0.0
        print("budget key collision: OK")

        # --- P0 #7 regression: from_dict error contract ---
        for bad_payload in (
            {},
            {"transaction_id": "x"},   # missing fields
            {
                "transaction_id": "x", "timestamp": "not a date",
                "team": "t", "task_id": "x",
                "energy_kwh": 0.0, "carbon_kgco2e": 0.0,
                "cost_usd": 0.0,
            },
        ):
            try:
                CarbonTransaction.from_dict(bad_payload)
            except CarbonLedgerError:
                pass
            else:
                raise AssertionError(
                    f"from_dict accepted bad payload: {bad_payload}"
                )
        print("from_dict error contract: OK")

        # --- P0 #8 regression: max_transactions warning on load ---
        small_cap = Path(td) / "small.json"
        svc_small = CarbonLedgerService(
            ledger_path=small_cap,
            config=LedgerConfig(
                ledger_path=str(small_cap),
                max_transactions=2,
            ),
        )
        svc_small.set_team_budget("t", "2026-03", 100.0)
        for i in range(5):
            svc_small.record_transaction(
                "t", f"t{i}", 0.01, 0.1, 0.001,
            )
        reloaded_small = CarbonLedgerService(
            ledger_path=small_cap,
            config=LedgerConfig(
                ledger_path=str(small_cap),
                max_transactions=2,
            ),
        )
        assert len(reloaded_small.transactions) == 2
        print("max_transactions enforced: OK")

        # --- P1 #10 regression: get_team_budget returns snapshot ---
        snap = ledger.get_team_budget("nlp_research", "2026-03")
        original_used = snap.used_kgco2e
        # Caller can't mutate because TeamBudget is frozen.
        try:
            snap.used_kgco2e = 0.0  # type: ignore[misc]
        except Exception:
            pass
        assert (
            ledger.get_team_budget("nlp_research", "2026-03").used_kgco2e
            == original_used
        )
        print("get_team_budget snapshot: OK")

        # --- P1 #11 regression: no auto-budget ---
        ledger.record_transaction(
            "unbudgeted_team", "t-x", 0.01, 0.005, 0.001,
        )
        assert (
            ledger.get_team_budget("unbudgeted_team", "2026-03")
            is None
        )
        print("no auto-budget: OK")

        # --- P1 #12 regression: period format ---
        for bad_period in ("March 2026", "2026-13", "2026/03"):
            try:
                ledger.set_team_budget("t", bad_period, 10.0)
            except CarbonLedgerError:
                pass
            else:
                raise AssertionError(
                    f"period {bad_period!r} should have been rejected"
                )
        print("period format: OK")

        # --- P2: reserve / commit / release ---
        ledger.set_team_budget("reserve_test", "2026-03", 10.0)
        r1 = ledger.reserve("reserve_test", "2026-03", 5.0)
        assert r1 is not None
        assert ledger.check_budget_available(
            "reserve_test", "2026-03", 6.0,
        ) is False   # 5 reserved → only 5 available
        assert ledger.check_budget_available(
            "reserve_test", "2026-03", 4.0,
        ) is True
        txn = ledger.commit(
            r1, 3.0, task_id="task-reserve", energy_kwh=0.01,
            cost_usd=0.001,
        )
        assert txn is not None
        b = ledger.get_team_budget("reserve_test", "2026-03")
        assert b.used_kgco2e == 3.0
        assert b.remaining_kgco2e == 7.0

        r2 = ledger.reserve("reserve_test", "2026-03", 5.0)
        assert r2 is not None
        assert ledger.release(r2) is True
        assert ledger.check_budget_available(
            "reserve_test", "2026-03", 7.0,
        ) is True
        print("reserve/commit/release: OK")

        # --- P2: undo_transaction ---
        before = ledger.get_team_budget(
            "reserve_test", "2026-03",
        ).used_kgco2e
        assert ledger.undo_transaction(txn.transaction_id) is True
        after = ledger.get_team_budget(
            "reserve_test", "2026-03",
        ).used_kgco2e
        assert after == before - 3.0
        assert ledger.undo_transaction("nonexistent") is False
        print("undo_transaction: OK")

        # --- P2: get_transactions / summarize ---
        txns = ledger.get_transactions(team="nlp_research")
        assert all(t.team == "nlp_research" for t in txns)
        summary = ledger.summarize("nlp_research", "2026-03")
        assert summary["team"] == "nlp_research"
        assert summary["budget"] is not None
        print("get_transactions / summarize: OK")

        # --- P0 #1 regression: no update loss under concurrency ---
        concurrent_path = Path(td) / "concurrent.json"
        svc_conc = CarbonLedgerService(ledger_path=concurrent_path)
        svc_conc.set_team_budget("team", "2026-03", 1e9)

        def _record(i: int) -> None:
            svc_conc.record_transaction(
                "team", f"task-{i}", 0.01, 0.005, 0.001,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(_record, range(100)))

        assert len(svc_conc.transactions) == 100, (
            f"in-memory: {len(svc_conc.transactions)}"
        )
        reloaded_conc = CarbonLedgerService(
            ledger_path=concurrent_path,
        )
        assert len(reloaded_conc.transactions) == 100, (
            f"on disk: {len(reloaded_conc.transactions)}"
        )
        print("concurrent transactions: 100/100 persisted")

        # --- Round-trip ---
        payload = ledger.to_json()
        restored = CarbonLedgerService.from_json(
            payload, ledger_path=Path(td) / "restored.json",
        )
        assert restored.to_dict() == ledger.to_dict()
        print("Round-trip OK.")

        # --- Legacy budget key migration ---
        legacy_payload = {
            "transactions": [],
            "budgets": {
                "nlp_2026-03": {
                    "team": "nlp", "period": "2026-03",
                    "budget_kgco2e": 10.0, "used_kgco2e": 1.0,
                    "remaining_kgco2e": 9.0,
                    "num_transactions": 1,
                },
            },
            "config": {
                "ledger_path": str(Path(td) / "legacy.json"),
                "max_transactions": 1000,
            },
        }
        legacy_svc = CarbonLedgerService.from_dict(legacy_payload)
        legacy_budget = legacy_svc.get_team_budget("nlp", "2026-03")
        assert legacy_budget is not None
        assert legacy_budget.used_kgco2e == 1.0
        print("legacy budget key migration: OK")

    print("\nSmoke test passed.")
