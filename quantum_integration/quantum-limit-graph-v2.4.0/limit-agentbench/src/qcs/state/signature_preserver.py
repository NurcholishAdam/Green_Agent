"""
SignaturePreserver — deterministic signatures for reproducibility.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class SignatureRecord:
    """A preserved signature for reproducibility."""
    signature: str
    payload_hash: str
    algorithm: str = "sha256"
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["created_at"] = self.created_at.isoformat()
        return out


class SignaturePreserver:
    """
    Preserves SHA-256 signatures for quantum experiments.

    Signatures cover the request, backend, algorithm, seed, and any
    measurement settings — so a third party can reproduce the exact
    execution context.
    """

    ALGORITHM = "sha256"

    def __init__(self):
        self._records: Dict[str, SignatureRecord] = {}

    def sign(
        self,
        *,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SignatureRecord:
        canonical = json.dumps(payload, sort_keys=True, default=str)
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        signature = f"{self.ALGORITHM}:{digest}"
        record = SignatureRecord(
            signature=signature,
            payload_hash=digest,
            algorithm=self.ALGORITHM,
            metadata=dict(metadata or {}),
        )
        self._records[signature] = record
        return record

    def verify(self, payload: Dict[str, Any], signature: str) -> bool:
        canonical = json.dumps(payload, sort_keys=True, default=str)
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        return signature == f"{self.ALGORITHM}:{digest}"

    def get(self, signature: str) -> Optional[SignatureRecord]:
        return self._records.get(signature)

    def count(self) -> int:
        return len(self._records)
