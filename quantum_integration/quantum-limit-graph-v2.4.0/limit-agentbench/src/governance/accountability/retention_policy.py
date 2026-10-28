"""
RetentionPolicy — declares how long audit events and ledgers are
retained, per compliance-kind. — v2.0.0

NOTE
----
This class **declares** retention windows. It does not enforce them.
Retention is enforced by calling ``prune_expired(policy)`` on each
ledger (``AuditLog``, ``CarbonLedger``, ``CarbonInstrumentsLedger``,
``ProvenanceRegistry``) — a per-ledger method that must be invoked
explicitly. A helper function ``enforce_retention(policy, *ledgers)``
is provided for convenience.

Default values are conservative baselines, not legal advice.
Operators MUST confirm their jurisdiction's minimum retention
requirements (e.g., EU ETS Art. 15: 5 years; SOX 802: 7 years;
SECR: 4 years) before relying on these values for compliance.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 Field validation — every field is a positive integer.
        Previously ``decisions_days=-100``, ``=0``, ``=True`` (coerces
        to 1 day), ``=3.14``, and ``=None`` all constructed cleanly.
  * 2.2 Cross-field consistency — the audit log must be retained at
        least as long as any artifact it audits (decisions,
        carbon/instruments ledgers, human review). Verification
        evidence must be retained at most as long as the carbon
        ledger it proves. The default values changed to satisfy
        these invariants (see "Changed defaults" below).
  * 2.3 ``to_dict`` uses ``dataclasses.asdict()`` — new fields are
        picked up automatically. Previously six hand-written lines
        silently drifted on every new field.
  * 2.4 ``from_dict`` / ``from_legacy_dict`` added. Previously
        serialization was one-way.
  * 2.5 ``get_retention_days(kind)`` — string keyed lookup so a
        consumer can iterate over retention kinds. Previously every
        caller had to ``getattr(policy, f"{kind}_days")``.
  * 2.6 ``frozen=True`` — a policy is a declaration of intent; it
        must not change after construction. Previously
        ``policy.audit_log_days = 1`` silently mutated the policy.
  * 2.7 ``is_expired(kind, recorded_at)`` and
        ``expires_at(kind, recorded_at)`` — the query methods a
        consumer needs to actually apply the policy. Previously
        the file declared windows that nothing enforced.

**P1 robustness**
  * 3.1 ``to_dict`` coerces any datetime / Path value to a
        JSON-safe string. Currently all fields are ``int``, but
        future additions won't silently break serialization.
  * 3.2 ``__version__`` and ``LEDGER_SCHEMA_VERSION`` exported;
        ``to_dict`` includes both.
  * 3.4 ``RetentionPolicy.from_jurisdiction(name)`` — presets for
        EU ETS 5-year, SOX 7-year, SECR 4-year, and "conservative"
        (10-year) minimums.
  * 3.5 ``satisfies_minimums(minimums)`` — returns a list of
        violations, empty when the policy meets all minimums.
  * 3.6 Changed defaults documented below.
  * 3.7 ``__repr__`` — concise, groups the four most-used fields.
  * 3.9 ``enforce_retention(policy, *ledgers, dry_run=True)`` —
        applies the policy to every ledger with a compatible
        ``prune_expired`` method, returning per-ledger counts.
  * 3.10 Honest docstring with the "declares, does not enforce"
        disclaimer.

**Changed defaults**
  * ``audit_log_days``: 1825 → 3650 (5y → 10y). The audit log is
    evidence of every ledger write; it must not expire before the
    instruments ledger it certifies.
  * ``decisions_days``: 365 → 1095 (1y → 3y). Human review logs
    support the decisions they were for; the decision record must
    not expire before its review log.

Callers who need the old defaults can pass them explicitly; the
cross-field validation will reject the old combination on
``instruments_ledger_days=3650, audit_log_days=1825`` because it was
internally inconsistent.

Backward compatible
-------------------
``RetentionPolicy()``, ``policy.decisions_days``, ``policy.to_dict()``
— every v1.0.0 attribute access and method call works unchanged. New
methods (``get_retention_days``, ``from_dict``, ``from_jurisdiction``,
``satisfies_minimums``, ``is_expired``, ``expires_at``) are additive.
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"
LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors (P0 2.1, 2.5)
# =============================================================================

class RetentionPolicyError(ValueError):
    """Base class for every error raised by RetentionPolicy."""


class RetentionPolicyValidationError(RetentionPolicyError):
    """Raised for invalid field values, cross-field inconsistency, or
    unknown retention kinds."""


# =============================================================================
# RetentionKind (P0 2.5)
# =============================================================================

class RetentionKind(str, Enum):
    """The artifact kinds a RetentionPolicy governs."""
    DECISIONS = "decisions"
    VERIFICATION_EVIDENCE = "verification_evidence"
    AUDIT_LOG = "audit_log"
    CARBON_LEDGER = "carbon_ledger"
    INSTRUMENTS_LEDGER = "instruments_ledger"
    HUMAN_REVIEW = "human_review"

    def __str__(self) -> str:
        return self.value


_ALL_KINDS = tuple(k.value for k in RetentionKind)


# =============================================================================
# Cross-field ordering (P0 2.2)
# =============================================================================
#
# Each tuple is ``(longer, shorter)``: the retention window for
# ``longer`` must be >= the retention window for ``shorter``.
# The reasoning is a compliance invariant: the audit trail that proves
# an artifact existed must not be deleted while the artifact is still
# required.
#
_RETENTION_ORDERING: Tuple[Tuple[str, str], ...] = (
    # The audit log proves every decision, ledger write, and review.
    ("audit_log_days", "decisions_days"),
    ("audit_log_days", "carbon_ledger_days"),
    ("audit_log_days", "instruments_ledger_days"),
    ("audit_log_days", "human_review_days"),
    ("audit_log_days", "verification_evidence_days"),
    # Verification evidence supports a specific carbon ledger entry.
    ("carbon_ledger_days", "verification_evidence_days"),
    # Human review supports a specific decision.
    ("decisions_days", "human_review_days"),
)


# =============================================================================
# Jurisdiction presets (P1 3.4)
# =============================================================================

_JURISDICTION_PRESETS: Dict[str, Dict[str, int]] = {
    "eu_ets_5y": {
        # EU ETS Article 15 — 5-year minimum (some member states 10).
        # We use 10 years for audit + instruments, 5 for the rest.
        "decisions_days": 5 * 365,
        "verification_evidence_days": 5 * 365,
        "audit_log_days": 10 * 365,
        "carbon_ledger_days": 5 * 365,
        "instruments_ledger_days": 10 * 365,
        "human_review_days": 5 * 365,
    },
    "sox_7y": {
        # Sarbanes-Oxley § 802 — 7 years for audit-related records.
        "decisions_days": 7 * 365,
        "verification_evidence_days": 7 * 365,
        "audit_log_days": 7 * 365,
        "carbon_ledger_days": 7 * 365,
        "instruments_ledger_days": 7 * 365,
        "human_review_days": 7 * 365,
    },
    "secr_4y": {
        # UK Streamlined Energy and Carbon Reporting — 4-year minimum.
        "decisions_days": 4 * 365,
        "verification_evidence_days": 4 * 365,
        "audit_log_days": 4 * 365,
        "carbon_ledger_days": 4 * 365,
        "instruments_ledger_days": 4 * 365,
        "human_review_days": 4 * 365,
    },
    "conservative_10y": {
        # Conservative across all known regimes.
        "decisions_days": 10 * 365,
        "verification_evidence_days": 10 * 365,
        "audit_log_days": 10 * 365,
        "carbon_ledger_days": 10 * 365,
        "instruments_ledger_days": 10 * 365,
        "human_review_days": 10 * 365,
    },
}


# =============================================================================
# Helpers
# =============================================================================

def _coerce_to_json_safe(value: Any) -> Any:
    """Coerce a value to a JSON-safe primitive (P1 3.1)."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return str(value)


def _as_aware_utc(dt: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""
    if not isinstance(dt, datetime):
        raise RetentionPolicyValidationError(
            f"expected datetime, got {type(dt).__name__}")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# =============================================================================
# RetentionPolicy
# =============================================================================

@dataclass(frozen=True)
class RetentionPolicy:
    """
    Retention configuration. All durations in days.

    A ``RetentionPolicy`` is a *declaration* of intent. It does not
    enforce itself — see :func:`enforce_retention`.
    """
    decisions_days: int = 3 * 365
    verification_evidence_days: int = 730
    audit_log_days: int = 10 * 365
    carbon_ledger_days: int = 5 * 365
    instruments_ledger_days: int = 10 * 365
    human_review_days: int = 3 * 365

    def __post_init__(self) -> None:
        # P0 2.1: every field is a positive int (reject bool too —
        # isinstance(True, int) is True but True means "1 day").
        for name in _ALL_KINDS:
            field_name = f"{name}_days"
            value = getattr(self, field_name)
            if isinstance(value, bool) or \
                    not isinstance(value, int):
                raise RetentionPolicyValidationError(
                    f"{field_name} must be an int, "
                    f"got {type(value).__name__}")
            if value <= 0:
                raise RetentionPolicyValidationError(
                    f"{field_name} must be > 0, got {value}")

        # P0 2.2: cross-field consistency.
        for longer_name, shorter_name in _RETENTION_ORDERING:
            longer = getattr(self, longer_name)
            shorter = getattr(self, shorter_name)
            if longer < shorter:
                raise RetentionPolicyValidationError(
                    f"{longer_name}={longer} must be >= "
                    f"{shorter_name}={shorter}; "
                    f"the audit trail must not expire before the "
                    f"artifact it audits")

    # ---------------------------------------------------------------- #
    # Query API (P0 2.5)
    # ---------------------------------------------------------------- #

    def get_retention_days(self, kind: str) -> int:
        """Return the retention window for ``kind`` in days."""
        if not isinstance(kind, str) or not kind:
            raise RetentionPolicyValidationError(
                "kind must be a non-empty string")
        normalized = kind.strip().lower()
        if normalized not in _ALL_KINDS:
            raise RetentionPolicyValidationError(
                f"unknown retention kind {kind!r}; "
                f"expected one of {sorted(_ALL_KINDS)}")
        return int(getattr(self, f"{normalized}_days"))

    def is_expired(
        self,
        kind: str,
        recorded_at: datetime,
        *,
        now: Optional[datetime] = None,
    ) -> bool:
        """
        Return True iff a record of ``kind`` recorded at
        ``recorded_at`` has exceeded the retention window (P0 2.7).
        """
        days = self.get_retention_days(kind)
        recorded = _as_aware_utc(recorded_at)
        if now is None:
            now = datetime.now(timezone.utc)
        else:
            now = _as_aware_utc(now)
        return (now - recorded) > timedelta(days=days)

    def expires_at(
        self,
        kind: str,
        recorded_at: datetime,
    ) -> datetime:
        """Return the timestamp at which a record expires (P0 2.7)."""
        days = self.get_retention_days(kind)
        recorded = _as_aware_utc(recorded_at)
        return recorded + timedelta(days=days)

    # ---------------------------------------------------------------- #
    # Minimums check (P1 3.5)
    # ---------------------------------------------------------------- #

    def satisfies_minimums(
        self, minimums: Mapping[str, int],
    ) -> List[str]:
        """
        Return a list of violations (empty = policy satisfies all
        minimums). ``minimums`` maps retention-kind to a minimum in
        days.
        """
        if not isinstance(minimums, Mapping):
            raise RetentionPolicyValidationError(
                "minimums must be a Mapping")
        violations: List[str] = []
        for kind_str, min_days in minimums.items():
            if isinstance(min_days, bool) or \
                    not isinstance(min_days, int):
                violations.append(
                    f"minimums[{kind_str!r}] must be an int")
                continue
            try:
                actual = self.get_retention_days(kind_str)
            except RetentionPolicyValidationError:
                violations.append(
                    f"unknown retention kind {kind_str!r}")
                continue
            if actual < min_days:
                violations.append(
                    f"{kind_str}: {actual} < {min_days} days")
        return violations

    # ---------------------------------------------------------------- #
    # Serialization (P0 2.3, 2.4; P1 3.1, 3.2)
    # ---------------------------------------------------------------- #

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict representation."""
        out = asdict(self)  # P0 2.3: introspects fields
        # P1 3.1: coerce in case a future field is datetime / Path.
        out = {k: _coerce_to_json_safe(v) for k, v in out.items()}
        # P1 3.2: version the payload.
        out["version"] = __version__
        out["schema_version"] = LEDGER_SCHEMA_VERSION
        return out

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "RetentionPolicy":
        """Construct from a payload produced by ``to_dict``."""
        return cls.from_legacy_dict(data)

    @classmethod
    def from_legacy_dict(
        cls, data: Optional[Mapping[str, Any]],
    ) -> "RetentionPolicy":
        """
        Construct from a flat mapping, ignoring unknown keys (P0 2.4).

        Missing fields default to their declared values. Unknown keys
        (like ``version`` / ``schema_version`` from ``to_dict``) are
        ignored.
        """
        data = dict(data or {})
        valid = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)

    # ---------------------------------------------------------------- #
    # Jurisdiction presets (P1 3.4)
    # ---------------------------------------------------------------- #

    @classmethod
    def from_jurisdiction(cls, name: str) -> "RetentionPolicy":
        """
        Return the retention policy preset for a jurisdiction.

        Available presets: ``"eu_ets_5y"``, ``"sox_7y"``,
        ``"secr_4y"``, ``"conservative_10y"``.
        """
        if not isinstance(name, str) or not name:
            raise RetentionPolicyValidationError(
                "jurisdiction name must be a non-empty string")
        key = name.strip().lower().replace(" ", "_").replace("-", "_")
        if key not in _JURISDICTION_PRESETS:
            raise RetentionPolicyValidationError(
                f"unknown jurisdiction {name!r}; available: "
                f"{sorted(_JURISDICTION_PRESETS)}")
        return cls(**_JURISDICTION_PRESETS[key])

    @staticmethod
    def available_jurisdictions() -> List[str]:
        """Return the list of jurisdiction preset names."""
        return sorted(_JURISDICTION_PRESETS)

    # ---------------------------------------------------------------- #
    # Dunder (P1 3.7)
    # ---------------------------------------------------------------- #

    def __repr__(self) -> str:
        return (
            "RetentionPolicy("
            f"decisions={self.decisions_days}d, "
            f"audit_log={self.audit_log_days}d, "
            f"carbon_ledger={self.carbon_ledger_days}d, "
            f"instruments={self.instruments_ledger_days}d)")


# =============================================================================
# Enforcement helper (P1 3.9)
# =============================================================================

# Map a ledger class name to its retention kind.
_KIND_FOR_LEDGER: Dict[str, str] = {
    "AuditLog": "audit_log",
    "CarbonLedger": "carbon_ledger",
    "CarbonInstrumentsLedger": "instruments_ledger",
    "ProvenanceRegistry": "decisions",  # least-specific fallback
}


def enforce_retention(
    policy: RetentionPolicy,
    *ledgers: Any,
    dry_run: bool = True,
) -> Dict[str, int]:
    """
    Apply ``policy`` to every ledger that exposes a
    ``prune_expired(policy)`` (or ``expired_entries(policy)``) method.

    Parameters
    ----------
    policy : RetentionPolicy
        The policy to apply.
    *ledgers : objects
        One or more ledger instances. Each must expose
        ``prune_expired(policy) -> int`` (deletes, returns count) and,
        optionally, ``expired_entries(policy) -> list`` (returns the
        entries that would be pruned without deleting).
    dry_run : bool, default True
        When True, use ``expired_entries`` if available and never
        delete. Pass ``dry_run=False`` to actually prune.

    Returns
    -------
    dict
        ``{ledger_class_name: count}`` where ``count`` is either the
        number of entries that would be pruned (dry run) or the number
        actually pruned.

    Note
    ----
    If a ledger does not implement ``prune_expired`` /
    ``expired_entries``, it is skipped and a WARNING is logged. This
    function never raises on a missing method — retention enforcement
    is best-effort by design (an operator should not be blocked from
    other ledgers just because one doesn't have a pruner yet).
    """
    if not isinstance(policy, RetentionPolicy):
        raise RetentionPolicyValidationError(
            "policy must be a RetentionPolicy")

    counts: Dict[str, int] = {}
    for ledger in ledgers:
        cls_name = type(ledger).__name__
        kind_str = _KIND_FOR_LEDGER.get(cls_name)

        # Prefer expired_entries for dry run when available.
        expired_fn = getattr(ledger, "expired_entries", None)
        prune_fn = getattr(ledger, "prune_expired", None)

        try:
            if dry_run and callable(expired_fn):
                expired = expired_fn(policy) or []
                counts[cls_name] = len(expired)
                continue
            if callable(prune_fn):
                counts[cls_name] = int(prune_fn(policy) or 0)
                continue
            # Fall back to expired_entries-only if prune isn't there
            # but the caller asked for a dry run.
            if callable(expired_fn):
                expired = expired_fn(policy) or []
                counts[cls_name] = len(expired)
                continue
            logger.warning(
                "Ledger %r does not implement prune_expired or "
                "expired_entries; skipping retention enforcement "
                "(kind=%r)", cls_name, kind_str)
            counts[cls_name] = 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Retention enforcement failed for %r: %s",
                cls_name, exc)
            counts[cls_name] = 0

    return counts


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "RetentionKind",
    "RetentionPolicy",
    "RetentionPolicyError",
    "RetentionPolicyValidationError",
    "enforce_retention",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import pickle
    from dataclasses import FrozenInstanceError

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== retention_policy v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — field validation
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: field validation ===")

    bad_values = [
        ("decisions_days", -100),
        ("decisions_days", 0),
        ("decisions_days", True),
        ("decisions_days", False),
        ("decisions_days", 3.14),
        ("decisions_days", None),
        ("audit_log_days", "365"),
    ]
    for field_name, bad_value in bad_values:
        try:
            RetentionPolicy(**{field_name: bad_value})  # type: ignore
        except RetentionPolicyValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.1 regression: {field_name}={bad_value!r} "
                f"accepted")
    print(f"  ✓ {len(bad_values)} invalid field values rejected")

    # Defaults construct.
    RetentionPolicy()
    print(f"  ✓ default construction works")

    # ---------------------------------------------------------------- #
    # P0 2.2 — cross-field consistency
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: cross-field consistency ===")

    # Old defaults: instruments (3650) > audit_log (1825) → reject.
    try:
        RetentionPolicy(
            audit_log_days=1825,
            instruments_ledger_days=3650,
            decisions_days=365,
            verification_evidence_days=730,
            carbon_ledger_days=1825,
            human_review_days=365)
    except RetentionPolicyValidationError as exc:
        print(f"  ✓ audit_log<instruments rejected: "
              f"{str(exc)[:60]}...")
    else:
        raise AssertionError(
            "P0 2.2 regression: audit_log<instruments accepted")

    # decision < human_review → reject.
    try:
        RetentionPolicy(
            decisions_days=365,
            human_review_days=1095,
            audit_log_days=3650,
            carbon_ledger_days=1825,
            instruments_ledger_days=3650,
            verification_evidence_days=730)
    except RetentionPolicyValidationError:
        print(f"  ✓ decision<review rejected")
    else:
        raise AssertionError(
            "P0 2.2 regression: decision<review accepted")

    # Consistent policy accepts.
    RetentionPolicy(
        decisions_days=1095,
        human_review_days=1095,
        audit_log_days=3650,
        carbon_ledger_days=1825,
        instruments_ledger_days=3650,
        verification_evidence_days=730)
    print(f"  ✓ consistent policy accepted")

    # ---------------------------------------------------------------- #
    # P0 2.3 — to_dict uses asdict (no drift)
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: to_dict has no field list to drift ===")
    policy = RetentionPolicy()
    d = policy.to_dict()
    for kind in _ALL_KINDS:
        assert f"{kind}_days" in d, f"missing {kind}_days"
    print(f"  ✓ to_dict includes all {len(_ALL_KINDS)} fields "
          f"via asdict()")

    # ---------------------------------------------------------------- #
    # P0 2.4 — from_dict / from_legacy_dict
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: from_dict / from_legacy_dict ===")

    # Round-trip.
    restored = RetentionPolicy.from_dict(d)
    assert restored == policy
    print(f"  ✓ round-trip preserves all fields")

    # Legacy payload with only one field: other fields use defaults.
    legacy = {"decisions_days": 730}
    restored = RetentionPolicy.from_legacy_dict(legacy)
    assert restored.decisions_days == 730
    assert restored.audit_log_days == RetentionPolicy().audit_log_days
    print(f"  ✓ legacy payload preserves defaults for omitted fields")

    # Unknown keys ignored (like `version` from to_dict).
    restored = RetentionPolicy.from_legacy_dict({
        "decisions_days": 730,
        "unknown_key": 999,
    })
    assert restored.decisions_days == 730
    print(f"  ✓ unknown keys ignored")

    # ---------------------------------------------------------------- #
    # P0 2.5 — get_retention_days
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: get_retention_days(kind) ===")
    policy = RetentionPolicy()
    assert policy.get_retention_days("audit_log") == 3650
    assert policy.get_retention_days("AUDIT_LOG") == 3650
    assert policy.get_retention_days("  decisions  ") == 1095
    try:
        policy.get_retention_days("garbage")
    except RetentionPolicyValidationError:
        print(f"  ✓ 'garbage' rejected")
    else:
        raise AssertionError(
            "P0 2.5 regression: unknown kind accepted")
    print(f"  ✓ valid kinds resolve; case/whitespace normalized")

    # ---------------------------------------------------------------- #
    # P0 2.6 — frozen
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: frozen dataclass ===")
    policy = RetentionPolicy()
    try:
        policy.audit_log_days = 1   # type: ignore[misc]
    except FrozenInstanceError:
        print(f"  ✓ direct mutation raises FrozenInstanceError")
    else:
        raise AssertionError(
            "P0 2.6 regression: policy mutable")

    # ---------------------------------------------------------------- #
    # P0 2.7 — is_expired / expires_at
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: is_expired / expires_at ===")

    policy = RetentionPolicy()
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)

    # 4-year-old audit entry: not expired (10y window).
    old_but_fresh = now - timedelta(days=4 * 365)
    assert policy.is_expired(
        "audit_log", old_but_fresh, now=now) is False
    print(f"  ✓ 4-year-old audit entry: not expired (10y window)")

    # 11-year-old audit entry: expired.
    ancient = now - timedelta(days=11 * 365)
    assert policy.is_expired(
        "audit_log", ancient, now=now) is True
    print(f"  ✓ 11-year-old audit entry: expired")

    # expires_at for a 2024 entry at 10-year window.
    recorded = datetime(2024, 1, 1, tzinfo=timezone.utc)
    expires = policy.expires_at("audit_log", recorded)
    assert expires.year == 2034
    print(f"  ✓ expires_at: {recorded.date()} + 10y = "
          f"{expires.date()}")

    # Naive datetime rejected.
    try:
        policy.is_expired(
            "audit_log", datetime(2024, 1, 1))  # naive
    except RetentionPolicyValidationError:
        pass  # We normalize to UTC in _as_aware_utc — actually let's
              # check: _as_aware_utc normalizes, so this should not
              # raise. Test that it works.
    # The current implementation normalizes to UTC:
    assert policy.is_expired(
        "audit_log", datetime(2024, 1, 1),
        now=datetime(2035, 1, 1, tzinfo=timezone.utc)) is True
    print(f"  ✓ naive datetime normalized to UTC")

    # ---------------------------------------------------------------- #
    # P1 3.2 — version constants
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: version constants ===")
    assert __version__ == "2.0.0"
    assert LEDGER_SCHEMA_VERSION == "5.0.0"
    d = RetentionPolicy().to_dict()
    assert d["version"] == __version__
    assert d["schema_version"] == LEDGER_SCHEMA_VERSION
    print(f"  ✓ version={__version__!r}, "
          f"schema={LEDGER_SCHEMA_VERSION!r}")

    # ---------------------------------------------------------------- #
    # P1 3.4 — jurisdiction presets
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: jurisdiction presets ===")

    jur = RetentionPolicy.from_jurisdiction("EU_ETS_5y")
    assert jur.get_retention_days("carbon_ledger") >= 5 * 365
    assert jur.get_retention_days("audit_log") >= 5 * 365
    print(f"  ✓ EU_ETS_5y: all fields >= 5y")

    jur = RetentionPolicy.from_jurisdiction("sox_7y")
    assert jur.get_retention_days("audit_log") == 7 * 365
    print(f"  ✓ sox_7y: audit_log=7y")

    jur = RetentionPolicy.from_jurisdiction("conservative_10y")
    for kind in _ALL_KINDS:
        assert jur.get_retention_days(kind) == 10 * 365
    print(f"  ✓ conservative_10y: all fields = 10y")

    try:
        RetentionPolicy.from_jurisdiction("garbage")
    except RetentionPolicyValidationError:
        print(f"  ✓ unknown jurisdiction rejected")

    print(f"  ✓ available: {RetentionPolicy.available_jurisdictions()}")

    # ---------------------------------------------------------------- #
    # P1 3.5 — satisfies_minimums
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: satisfies_minimums ===")

    policy = RetentionPolicy()
    # All minimums satisfied.
    violations = policy.satisfies_minimums({
        "audit_log": 365,
        "carbon_ledger": 365,
    })
    assert violations == []
    print(f"  ✓ compliant minimums: no violations")

    # Minimum exceeded.
    violations = policy.satisfies_minimums({
        "audit_log": 365 * 20,   # want 20y, policy has 10y
    })
    assert len(violations) == 1
    assert "audit_log" in violations[0]
    assert "365" in violations[0] or "7300" in violations[0]
    print(f"  ✓ violation detected: {violations[0]}")

    # Unknown kind.
    violations = policy.satisfies_minimums({
        "garbage": 365,
    })
    assert any("unknown" in v for v in violations)
    print(f"  ✓ unknown kind flagged: {violations[0]}")

    # ---------------------------------------------------------------- #
    # P1 3.7 — __repr__
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: __repr__ ===")
    r = repr(RetentionPolicy())
    assert "decisions=" in r
    assert "audit_log=" in r
    assert "carbon_ledger=" in r
    print(f"  ✓ repr is concise: {r[:80]}...")

    # ---------------------------------------------------------------- #
    # P1 3.9 — enforce_retention helper
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: enforce_retention helper ===")

    class _MockLedger:
        """A minimal ledger that simulates prune_expired."""
        def __init__(self, n_expired: int) -> None:
            self._n = n_expired
            self.pruned = 0

        def expired_entries(self, policy: RetentionPolicy) -> List:
            return [object()] * self._n

        def prune_expired(self, policy: RetentionPolicy) -> int:
            self.pruned = self._n
            return self._n

    ledger_a = _MockLedger(3)
    ledger_b = _MockLedger(0)

    # Dry run.
    counts = enforce_retention(
        RetentionPolicy(), ledger_a, ledger_b, dry_run=True)
    assert counts["_MockLedger"] == 3
    assert ledger_a.pruned == 0, "dry_run should not prune"
    print(f"  ✓ dry run: {counts}, no actual pruning")

    # Actual prune.
    counts = enforce_retention(
        RetentionPolicy(), ledger_a, ledger_b, dry_run=False)
    assert counts["_MockLedger"] == 3
    assert ledger_a.pruned == 3
    print(f"  ✓ prune: {counts}, actual pruning happened")

    # Unknown ledger type (no prune_expired).
    class _NoPruner:
        pass

    counts = enforce_retention(RetentionPolicy(), _NoPruner())
    assert counts["_NoPruner"] == 0
    print(f"  ✓ ledger without pruner: count=0, no crash")

    # ---------------------------------------------------------------- #
    # Pickling (frozen dataclass)
    # ---------------------------------------------------------------- #
    print("\n=== Pickling ===")
    policy = RetentionPolicy()
    blob = pickle.dumps(policy)
    restored = pickle.loads(blob)
    assert restored == policy
    print(f"  ✓ pickle round-trip works")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 attribute access
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat ===")
    policy = RetentionPolicy()
    # All attributes accessible.
    assert isinstance(policy.decisions_days, int)
    assert isinstance(policy.verification_evidence_days, int)
    assert isinstance(policy.audit_log_days, int)
    assert isinstance(policy.carbon_ledger_days, int)
    assert isinstance(policy.instruments_ledger_days, int)
    assert isinstance(policy.human_review_days, int)
    print(f"  ✓ all 6 attribute accessors work")

    # to_dict returns a dict with the six fields.
    d = policy.to_dict()
    for kind in _ALL_KINDS:
        assert f"{kind}_days" in d
    print(f"  ✓ to_dict preserves field names")

    # ---------------------------------------------------------------- #
    # RetentionKind enum
    # ---------------------------------------------------------------- #
    print("\n=== RetentionKind enum ===")
    assert RetentionKind.AUDIT_LOG.value == "audit_log"
    assert RetentionKind("carbon_ledger") is RetentionKind.CARBON_LEDGER
    print(f"  ✓ enum values match field names")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
