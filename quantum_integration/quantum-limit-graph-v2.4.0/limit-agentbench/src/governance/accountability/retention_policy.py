"""
RetentionPolicy — how long audit events and ledgers are retained.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RetentionPolicy:
    """
    Retention configuration. All durations in days.
    """
    decisions_days: int = 365
    verification_evidence_days: int = 730
    audit_log_days: int = 1825
    carbon_ledger_days: int = 1825
    instruments_ledger_days: int = 3650
    human_review_days: int = 1095

    def to_dict(self) -> dict:
        return {
            "decisions_days": self.decisions_days,
            "verification_evidence_days": self.verification_evidence_days,
            "audit_log_days": self.audit_log_days,
            "carbon_ledger_days": self.carbon_ledger_days,
            "instruments_ledger_days": self.instruments_ledger_days,
            "human_review_days": self.human_review_days,
        }
