"""
SignaturePreserver — deterministic reproducibility hashes for experiments.

**This module produces hashes, not cryptographic signatures.**

A "signature" here is a digest (SHA-256 by default) of a canonicalized
payload. Anyone who knows the payload can produce the same signature;
there is no key, no authentication, no non-repudiation. Use it to detect
payload changes and to make an experiment reproducible.

For *attestation* — signatures a third party can verify without knowing
the payload — use the Ed25519-capable preserver at
``qcs.state.signature_preserver.SignaturePreserver``. The two classes
are intentionally different: this one answers "does this payload hash
to this digest?"; that one answers "did this signer attest to this
payload?".

Canonicalization
----------------
The digest is taken over a **byte-stable canonical form** so that two
processes on different machines and different Python versions produce
the same signature for the same logical payload:

  * ``None``           → ``b"null"``
  * ``bool``           → ``b"true"`` / ``b"false"``
  * ``int``            → ``b"i:<decimal>``
  * ``float``          → ``b"f:" + struct.pack(">d", x)`` (raw IEEE-754)
  * ``str``            → ``b"s:" + UTF-8 bytes``
  * ``bytes``          → ``b"b:" + base64``
  * ``datetime``       → ``b"t:" + ISO 8601 in UTC``
  * ``Decimal``        → ``b"d:" + normalized decimal string``
  * ``Enum``           → ``b"e:ClassName.MEMBER``
  * ``Mapping``        → ``b"m:{k1=v1,k2=v2,...}`` (sorted by canonical key)
  * ``list``/``tuple`` → ``b"a:[...]"``
  * ``set``/``frozenset`` → ``b"set:[sorted]``
  * anything else      → ``TypeError`` (fail loudly, never stringify)

The canonicalization scheme is versioned (``SignatureConfig.canonicalization_version``,
default ``"v1"``) and included in the signature string by default
(``"sha256:v1:<hex>"``), so a future scheme change does not invalidate
old signatures — a verifier can inspect the version and pick the right
canonicalizer.

Original API preserved
----------------------
``SignaturePreserver`` (deprecated in favor of ``SignatureHasher``):

    preserver = SignaturePreserver()
    record = preserver.sign(payload={...}, metadata={...})
    ok = preserver.verify({...}, record.signature)
    record2 = preserver.get(record.signature)   # accepts signature string or id
    n = preserver.count()

New API:

    hasher = SignatureHasher()
    record = hasher.sign(payload={...}, metadata={...})
    ok = hasher.verify({...}, record.signature)          # reproducibility
    issued = hasher.verify_known({...}, record.signature) # + issuance check
    hasher.delete(record.signature_id)
    hasher.records()
    hasher.clear()
    hasher.health()

v2 fixes (see changelog in commit message):
  * Renamed primary class to ``SignatureHasher`` (honest); the old
    ``SignaturePreserver`` name is a deprecated subclass.
  * Canonical byte form — no more ``json.dumps(default=str)``
    non-determinism (datetimes, Decimals, floats, sets, unknown types).
  * Versioned canonicalization (``v1``) with ``verify`` accepting both
    old and new formats.
  * ``sign()`` rejects empty/garbage payloads.
  * ``SignatureRecord`` frozen; ``__post_init__`` validates
    ``signature == "<alg>:<ver>:<hash>"`` consistency.
  * ``metadata`` frozen and bounded.
  * ``to_dict`` / ``from_dict`` / ``checksum`` on the record.
  * Thread-safe registry; bounded by ``max_records``.
  * ``verify_known`` distinguishes reproducibility from issuance.
  * Metrics, ``health()``, structured logging.
  * ``delete`` / ``records`` / ``clear`` added.
  * ``SignatureConfig``; no class-level mutable constants.
  * ``__version__``, ``__all__``, demo.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import secrets
import struct
import threading
import time
import warnings
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

MAX_METADATA_ENTRIES: int = 64
MAX_METADATA_VALUE_LEN: int = 4096
MAX_PAYLOAD_KEYS: int = 1000
DEFAULT_MAX_RECORDS: int = 10_000

_ALLOWED_ALGORITHMS: frozenset = frozenset(
    a for a in hashlib.algorithms_guaranteed
    if a not in {"md5", "sha1"}   # reject weak hashes by default
)


# =============================================================================
# Errors
# =============================================================================

class SignatureError(Exception):
    """Base class for SignaturePreserver/SignatureHasher errors."""


class SignatureInputError(SignatureError, ValueError):
    """Invalid input to the constructor or a method."""


class DuplicateSignatureError(SignatureError):
    """A record with this ``signature_id`` already exists."""


# =============================================================================
# Canonicalization (the heart of the module)
# =============================================================================

def _canonical_bytes(obj: Any) -> bytes:
    """
    Recursively convert ``obj`` to a byte-stable canonical form.

    Two callers on any two machines running any two supported Python
    versions must produce the same bytes for the same logical value.
    Anything we cannot canonicalize raises ``TypeError`` — silently
    stringifying would break reproducibility.
    """
    if obj is None:
        return b"null"
    # bool BEFORE int — bool is a subclass of int
    if isinstance(obj, bool):
        return b"true" if obj else b"false"
    if isinstance(obj, int):
        return b"i:" + str(obj).encode("ascii")
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise SignatureInputError(
                f"cannot canonicalize non-finite float {obj!r}"
            )
        return b"f:" + struct.pack(">d", float(obj))
    if isinstance(obj, str):
        return b"s:" + obj.encode("utf-8")
    if isinstance(obj, (bytes, bytearray)):
        return b"b:" + base64.b64encode(bytes(obj))
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            raise SignatureInputError(
                f"cannot canonicalize naive datetime {obj!r}; "
                f"use a timezone-aware datetime"
            )
        return b"t:" + obj.astimezone(timezone.utc).isoformat().encode("ascii")
    if isinstance(obj, Decimal):
        if not obj.is_finite():
            raise SignatureInputError(
                f"cannot canonicalize non-finite Decimal {obj!r}"
            )
        # Normalize strips trailing zeros; format(_, "f") avoids scientific
        return b"d:" + format(obj.normalize(), "f").encode("ascii")
    if isinstance(obj, Enum):
        return b"e:" + (
            f"{type(obj).__name__}.{obj.name}".encode("utf-8")
        )
    if isinstance(obj, Mapping):
        items = []
        for k, v in obj.items():
            kb = _canonical_bytes(k)
            vb = _canonical_bytes(v)
            items.append((kb, vb))
        items.sort(key=lambda kv: kv[0])
        body = b",".join(k + b"=" + v for k, v in items)
        return b"m:{" + body + b"}"
    if isinstance(obj, (list, tuple)):
        body = b",".join(_canonical_bytes(x) for x in obj)
        return b"a:[" + body + b"]"
    if isinstance(obj, (set, frozenset)):
        body = b",".join(
            sorted(_canonical_bytes(x) for x in obj)
        )
        return b"set:[" + body + b"]"
    raise TypeError(
        f"cannot canonicalize type {type(obj).__name__!r}; "
        f"sign() accepts only JSON-shaped values, datetimes, "
        f"Decimals, and enums"
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise SignatureInputError(
            f"metadata must be a Mapping; got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return format(obj, "f")
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return repr(obj)


def _new_signature_id() -> str:
    return "sig-" + secrets.token_hex(16)   # 128 bits


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class SignatureConfig:
    """Frozen, validated configuration for ``SignatureHasher``."""

    algorithm: str = "sha256"
    canonicalization_version: str = "v1"
    include_version_in_signature: bool = True
    max_records: int = DEFAULT_MAX_RECORDS

    def __post_init__(self) -> None:
        if not isinstance(self.algorithm, str) \
                or self.algorithm not in _ALLOWED_ALGORITHMS:
            raise SignatureInputError(
                f"algorithm must be one of "
                f"{sorted(_ALLOWED_ALGORITHMS)}; "
                f"got {self.algorithm!r}"
            )
        if not isinstance(self.canonicalization_version, str) \
                or not self.canonicalization_version.strip():
            raise SignatureInputError(
                "canonicalization_version must be a non-empty string"
            )
        if not isinstance(self.include_version_in_signature, bool):
            raise SignatureInputError(
                "include_version_in_signature must be a bool"
            )
        if isinstance(self.max_records, bool) \
                or not isinstance(self.max_records, int) \
                or self.max_records <= 0:
            raise SignatureInputError(
                "max_records must be a positive int"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {"counters": dict(self.counters), "histograms": hist}


# =============================================================================
# Record
# =============================================================================

@dataclass(frozen=True)
class SignatureRecord:
    """
    Immutable record of a reproducibility hash.

    Fields
    ------
    signature : str
        Full signature string: ``"<algorithm>:<version>:<hex>"`` in v2
        format, or ``"<algorithm>:<hex>"`` in v1 (legacy) format.
    payload_hash : str
        The raw digest hex. Must match the digest part of ``signature``.
    algorithm : str
        Digest algorithm; must match the algorithm part of ``signature``.
    canonicalization_version : str
        Canonicalization scheme version; must match the version part of
        ``signature`` (empty string for legacy signatures).
    signature_id : str
        128-bit random identifier. Lookups accept either this or
        ``signature``.
    created_at : datetime
        UTC.
    metadata : Mapping[str, Any]
        Frozen caller-supplied data.
    """

    signature: str
    payload_hash: str
    algorithm: str = "sha256"
    canonicalization_version: str = ""
    signature_id: str = field(default_factory=_new_signature_id)
    created_at: datetime = field(default_factory=_utcnow)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # --- Parse signature and validate field consistency ---
        if not isinstance(self.signature, str) or not self.signature:
            raise SignatureInputError(
                "signature must be a non-empty string"
            )
        parts = self.signature.split(":")
        if len(parts) == 2:
            sig_alg, digest = parts
            sig_version = ""
        elif len(parts) == 3:
            sig_alg, sig_version, digest = parts
        else:
            raise SignatureInputError(
                f"malformed signature {self.signature!r}; "
                f"expected '<alg>:<hex>' or '<alg>:<ver>:<hex>'"
            )

        if self.payload_hash != digest:
            raise SignatureInputError(
                f"payload_hash={self.payload_hash!r} does not match "
                f"the digest in signature={self.signature!r}"
            )
        if self.algorithm != sig_alg:
            raise SignatureInputError(
                f"algorithm={self.algorithm!r} does not match "
                f"the algorithm in signature={self.signature!r}"
            )
        if not self.canonicalization_version:
            # Infer from signature (empty means legacy format)
            object.__setattr__(
                self, "canonicalization_version", sig_version,
            )
        elif self.canonicalization_version != sig_version:
            raise SignatureInputError(
                f"canonicalization_version="
                f"{self.canonicalization_version!r} does not match "
                f"the version in signature={self.signature!r}"
            )

        # --- ID ---
        if not isinstance(self.signature_id, str) \
                or not self.signature_id.strip():
            raise SignatureInputError(
                "signature_id must be a non-empty string"
            )

        # --- UTC datetime ---
        object.__setattr__(
            self, "created_at", _ensure_utc(self.created_at),
        )

        # --- Freeze metadata + bounds ---
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata", _freeze_mapping(self.metadata),
            )
        if len(self.metadata) > MAX_METADATA_ENTRIES:
            raise SignatureInputError(
                f"metadata has {len(self.metadata)} entries "
                f"(max {MAX_METADATA_ENTRIES})"
            )
        for k, v in self.metadata.items():
            if not isinstance(k, str):
                raise SignatureInputError(
                    "metadata keys must be strings"
                )
            if isinstance(v, str) \
                    and len(v) > MAX_METADATA_VALUE_LEN:
                raise SignatureInputError(
                    f"metadata[{k!r}] exceeds "
                    f"{MAX_METADATA_VALUE_LEN} chars"
                )

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "signature": self.signature,
            "payload_hash": self.payload_hash,
            "algorithm": self.algorithm,
            "canonicalization_version": self.canonicalization_version,
            "signature_id": self.signature_id,
            "created_at": self.created_at.isoformat(),
            "metadata": _to_jsonable(dict(self.metadata)),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "SignatureRecord":
        if not isinstance(d, Mapping):
            raise SignatureInputError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known
        if unknown:
            raise SignatureInputError(
                f"unknown fields in SignatureRecord dict: "
                f"{sorted(unknown)}"
            )
        cleaned: Dict[str, Any] = dict(d)
        ts = cleaned.get("created_at")
        if isinstance(ts, str):
            cleaned["created_at"] = datetime.fromisoformat(ts)
        elif ts is None:
            cleaned["created_at"] = _utcnow()
        if "signature_id" not in cleaned:
            cleaned["signature_id"] = _new_signature_id()
        if not validate:
            # Skip __post_init__ validation by temporarily bypassing.
            # We rebuild via object.__new__ — simpler and safer than
            # shipping a _skip_validation field.
            obj = object.__new__(cls)
            for f in fields(cls):
                if f.name in cleaned:
                    object.__setattr__(obj, f.name, cleaned[f.name])
            return obj
        return cls(**cleaned)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"SignatureRecord("
            f"id={self.signature_id!r}, "
            f"algorithm={self.algorithm!r}, "
            f"version={self.canonicalization_version!r}, "
            f"payload_hash={self.payload_hash[:12]}…)"
        )


# =============================================================================
# The Hasher
# =============================================================================

class SignatureHasher:
    """
    Produce and verify reproducibility hashes for experiment payloads.

    See the module docstring for the canonicalization scheme and the
    difference between ``verify`` (hash match — reproducibility) and
    ``verify_known`` (hash match + record exists — issuance).
    """

    def __init__(
        self,
        *,
        config: Optional[SignatureConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        self.config = config or SignatureConfig()
        self._metrics = metrics or MetricsRecorder()
        self._lock = threading.RLock()
        self._records_by_id: Dict[str, SignatureRecord] = {}
        self._id_by_signature: Dict[str, str] = {}
        self._order: Deque[str] = deque()
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ signing
    def sign(
        self,
        *,
        payload: Mapping[str, Any],
        metadata: Optional[Mapping[str, Any]] = None,
        signature_id: Optional[str] = None,
    ) -> SignatureRecord:
        """
        Compute a reproducibility hash over ``payload``.

        ``payload`` must be a non-empty mapping. Values inside it may be
        primitives, datetimes, Decimals, enums, bytes, or nested
        mappings/lists/sets of these. Unknown types raise
        ``TypeError`` (never silently stringify).
        """
        self._validate_payload(payload)
        sig_id = (
            signature_id.strip() if isinstance(signature_id, str)
            else _new_signature_id()
        )
        if not sig_id:
            raise SignatureInputError(
                "signature_id must be a non-empty string when provided"
            )

        digest_hex = self._digest(payload)
        if self.config.include_version_in_signature:
            signature = (
                f"{self.config.algorithm}:"
                f"{self.config.canonicalization_version}:"
                f"{digest_hex}"
            )
        else:
            signature = f"{self.config.algorithm}:{digest_hex}"

        record = SignatureRecord(
            signature=signature,
            payload_hash=digest_hex,
            algorithm=self.config.algorithm,
            canonicalization_version=self.config.canonicalization_version,
            signature_id=sig_id,
            metadata=metadata or {},
        )

        with self._lock:
            if sig_id in self._records_by_id:
                raise DuplicateSignatureError(sig_id)
            self._records_by_id[sig_id] = record
            self._id_by_signature[signature] = sig_id
            self._order.append(sig_id)
            self._evict_locked()

        self._metrics.inc("signatures_created_total")
        logger.debug(
            "signature created",
            extra={
                "signature_id": sig_id,
                "algorithm": self.config.algorithm,
                "version": self.config.canonicalization_version,
            },
        )
        return record

    # ------------------------------------------------------------------ verification
    def verify(
        self, payload: Mapping[str, Any], signature: str,
    ) -> bool:
        """
        Reproducibility check: does ``payload`` hash to ``signature``?

        Accepts both v2 (``"<alg>:<ver>:<hex>"``) and v1 (legacy,
        ``"<alg>:<hex>"``) formats. Does not require the signature to
        exist in this hasher's registry — any party with the same
        canonicalization can verify.
        """
        if not isinstance(signature, str) or not signature:
            return False
        try:
            self._validate_payload(payload)
            digest_hex = self._digest(payload)
        except (SignatureInputError, TypeError):
            return False
        candidates = {
            f"{self.config.algorithm}:{digest_hex}",
            f"{self.config.algorithm}:"
            f"{self.config.canonicalization_version}:{digest_hex}",
        }
        return signature in candidates

    def verify_known(
        self, payload: Mapping[str, Any], signature: str,
    ) -> bool:
        """
        Issuance check: does ``payload`` hash to ``signature`` **and**
        was ``signature`` created by this hasher instance?
        """
        with self._lock:
            if signature not in self._id_by_signature:
                return False
        return self.verify(payload, signature)

    # ------------------------------------------------------------------ lookups
    def get(self, key: str) -> Optional[SignatureRecord]:
        """
        Look up a record by either ``signature`` (the full string) or
        ``signature_id``.
        """
        if not isinstance(key, str):
            return None
        with self._lock:
            rec = self._records_by_id.get(key)
            if rec is not None:
                return rec
            sig_id = self._id_by_signature.get(key)
            if sig_id is None:
                return None
            return self._records_by_id.get(sig_id)

    def records(self) -> Tuple[SignatureRecord, ...]:
        with self._lock:
            return tuple(self._records_by_id.values())

    def count(self) -> int:
        with self._lock:
            return len(self._records_by_id)

    # ------------------------------------------------------------------ mutation
    def delete(self, key: str) -> bool:
        """Delete by ``signature_id`` or ``signature`` string."""
        if not isinstance(key, str):
            return False
        with self._lock:
            rec = self._records_by_id.get(key)
            if rec is None:
                sig_id = self._id_by_signature.get(key)
                if sig_id is None:
                    return False
                rec = self._records_by_id.get(sig_id)
                if rec is None:
                    return False
            self._records_by_id.pop(rec.signature_id, None)
            self._id_by_signature.pop(rec.signature, None)
            try:
                self._order.remove(rec.signature_id)
            except ValueError:
                pass
        self._metrics.inc("signatures_deleted_total")
        return True

    def clear(self) -> None:
        with self._lock:
            self._records_by_id.clear()
            self._id_by_signature.clear()
            self._order.clear()
        self._metrics.inc("clears_total")

    # ------------------------------------------------------------------ observability
    def health(self) -> Dict[str, Any]:
        with self._lock:
            n = len(self._records_by_id)
            cap = self.config.max_records
        return {
            "ready": True,
            "records": n,
            "capacity": cap,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _digest(self, payload: Mapping[str, Any]) -> str:
        canonical = _canonical_bytes(dict(payload))
        h = hashlib.new(self.config.algorithm)
        h.update(canonical)
        return h.hexdigest()

    def _validate_payload(self, payload: Any) -> None:
        if not isinstance(payload, Mapping):
            raise SignatureInputError(
                f"payload must be a Mapping; "
                f"got {type(payload).__name__}"
            )
        if not payload:
            raise SignatureInputError(
                "payload must not be empty"
            )
        if len(payload) > MAX_PAYLOAD_KEYS:
            raise SignatureInputError(
                f"payload has {len(payload)} keys "
                f"(max {MAX_PAYLOAD_KEYS})"
            )

    def _evict_locked(self) -> None:
        while len(self._records_by_id) > self.config.max_records:
            oldest_id = self._order.popleft()
            old = self._records_by_id.pop(oldest_id, None)
            if old is not None:
                self._id_by_signature.pop(old.signature, None)
                self._metrics.inc("evictions_total")

    def __repr__(self) -> str:
        return (
            f"SignatureHasher("
            f"algorithm={self.config.algorithm!r}, "
            f"version={self.config.canonicalization_version!r}, "
            f"records={self.count()})"
        )


# =============================================================================
# Deprecated compatibility shim
# =============================================================================

class SignaturePreserver(SignatureHasher):
    """
    Deprecated alias for ``SignatureHasher``.

    This class **does not sign** — it produces reproducibility hashes.
    For cryptographic attestation (Ed25519), use
    ``qcs.state.signature_preserver.SignaturePreserver``.

    Preserves the pre-2.0 behavior: signatures are the v1 format
    (``"<alg>:<hex>"``) unless explicitly opted into versioning via
    ``config=SignatureConfig(include_version_in_signature=True)``.
    """

    def __init__(
        self,
        *,
        config: Optional[SignatureConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        warnings.warn(
            "SignaturePreserver is deprecated and does NOT sign "
            "(it hashes). Use SignatureHasher for reproducibility "
            "hashes, or qcs.state.signature_preserver.SignaturePreserver "
            "for Ed25519 attestation.",
            DeprecationWarning,
            stacklevel=2,
        )
        if config is None:
            config = SignatureConfig(include_version_in_signature=False)
        super().__init__(config=config, metrics=metrics)


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "DuplicateSignatureError",
    "DEFAULT_MAX_RECORDS",
    "MetricsRecorder",
    "SignatureConfig",
    "SignatureError",
    "SignatureHasher",
    "SignatureInputError",
    "SignaturePreserver",
    "SignatureRecord",
)


# =============================================================================
# Demo: python -m qcs.signature.signature_preserver
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import warnings
    from datetime import timedelta
    from decimal import Decimal

    logging.basicConfig(level=logging.INFO)

    print("=== Basic signing ===")
    hasher = SignatureHasher()
    rec = hasher.sign(
        payload={"request_id": "r1", "backend": "ibmq_quito", "shots": 1000},
        metadata={"created_by": "demo"},
    )
    print(f"  {rec!r}")
    print(f"  signature: {rec.signature}")
    print(f"  signature_id: {rec.signature_id}")
    print(f"  payload_hash: {rec.payload_hash}")
    assert rec.algorithm == "sha256"
    assert rec.canonicalization_version == "v1"
    assert "v1" in rec.signature

    print("\n=== Verification: reproducibility vs issuance ===")
    payload = {"request_id": "r1", "backend": "ibmq_quito", "shots": 1000}
    print(f"  verify(payload): {hasher.verify(payload, rec.signature)}")
    print(f"  verify_known(payload): "
          f"{hasher.verify_known(payload, rec.signature)}")
    assert hasher.verify(payload, rec.signature)
    assert hasher.verify_known(payload, rec.signature)

    print("\n=== External signature verifies but is not 'known' ===")
    # Same payload, different hasher instance
    external_hasher = SignatureHasher()
    external_rec = external_hasher.sign(payload=payload)
    # Our hasher can verify (reproducibility), but doesn't know the record
    print(f"  verify(external): {hasher.verify(payload, external_rec.signature)}")
    print(f"  verify_known(external): "
          f"{hasher.verify_known(payload, external_rec.signature)}")
    assert hasher.verify(payload, external_rec.signature)
    assert not hasher.verify_known(payload, external_rec.signature)

    print("\n=== §1.2 Canonicalization: datetimes, Decimals, floats, sets ===")
    # Same logical value, different representations → same hash
    h1 = hasher.sign(payload={
        "when": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "price": Decimal("1.23"),
        "ratio": 0.5,
        "tags": {"a", "b", "c"},
    })
    h2 = hasher.sign(payload={
        "tags": ["c", "b", "a"],           # list not set — DIFFERENT type
        "ratio": 0.5,
        "price": Decimal("1.230"),          # same Decimal, different string
        "when": datetime(2024, 1, 1, 7, 0, 0,
                         tzinfo=timezone(timedelta(hours=-5))),  # same instant
    })
    print(f"  h1 hash: {h1.payload_hash[:16]}…")
    print(f"  h2 hash: {h2.payload_hash[:16]}…")
    print(f"  h1 == h2 (tags list vs set differ)? "
          f"{h1.payload_hash == h2.payload_hash}")
    # Note: set vs list is a type difference, so hashes SHOULD differ.
    # But datetime and Decimal normalizations should match.
    h3 = hasher.sign(payload={
        "when": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "price": Decimal("1.23"),
        "ratio": 0.5,
        "tags": {"a", "b", "c"},
    })
    assert h1.payload_hash == h3.payload_hash, "identical payloads must match"
    print(f"  identical payloads match: {h1.payload_hash == h3.payload_hash}")

    # Different timezone, same instant → same hash
    h4 = hasher.sign(payload={
        "when": datetime(2024, 1, 1, 7, 0, 0,
                         tzinfo=timezone(timedelta(hours=-5))),
        "price": Decimal("1.23"),
        "ratio": 0.5,
        "tags": {"a", "b", "c"},
    })
    print(f"  same instant, diff timezone → same hash: "
          f"{h1.payload_hash == h4.payload_hash}")
    assert h1.payload_hash == h4.payload_hash

    print("\n=== §1.5 Empty/garbage payloads rejected ===")
    for label, payload in [
        ("empty dict", {}),
        ("None", None),
        ("string", "hello"),
    ]:
        try:
            hasher.sign(payload=payload)  # type: ignore[arg-type]
            print(f"  {label}: NOT rejected")
        except SignatureInputError:
            print(f"  {label}: rejected")

    print("\n=== §1.2 Unknown types raise (never silently stringify) ===")
    try:
        hasher.sign(payload={"obj": object()})
    except TypeError as e:
        print(f"  object() rejected: {type(e).__name__}")

    print("\n=== §1.4 Old and new formats both verify ===")
    old_hasher = SignatureHasher(
        config=SignatureConfig(include_version_in_signature=False),
    )
    old_rec = old_hasher.sign(payload={"x": 1})
    print(f"  old format signature: {old_rec.signature}")
    print(f"  old verifies on new hasher: "
          f"{hasher.verify({'x': 1}, old_rec.signature)}")
    assert hasher.verify({"x": 1}, old_rec.signature)

    print("\n=== §2.1 Immutability ===")
    try:
        rec.signature = "fake"  # type: ignore[misc]
    except Exception as e:
        print(f"  signature mutation → {type(e).__name__}")
    try:
        rec.metadata["x"] = 1  # type: ignore[index]
    except TypeError:
        print(f"  metadata mutation → TypeError")

    print("\n=== §1.7 signature/payload_hash consistency enforced ===")
    try:
        SignatureRecord(
            signature="sha256:v1:0000",
            payload_hash="1234",
        )
    except SignatureInputError:
        print(f"  mismatched record rejected")

    print("\n=== §2.3 JSON-safe serialization ===")
    rec_meta = hasher.sign(
        payload={"x": 1},
        metadata={"ts": datetime.now(), "price": Decimal("9.99")},
    )
    print(f"  to_json: {rec_meta.to_json()[:80]}…")
    assert "ts" in rec_meta.to_dict()["metadata"]

    print("\n=== §2.4 Round-trip via from_dict ===")
    d = rec.to_dict()
    restored = SignatureRecord.from_dict(d)
    print(f"  round-trip: {restored.signature == rec.signature}")
    print(f"  checksum: {rec.checksum()[:16]}…")

    print("\n=== §3.5 get() accepts signature_id OR signature ===")
    assert hasher.get(rec.signature_id) is rec
    assert hasher.get(rec.signature) is rec
    print(f"  get by id: {hasher.get(rec.signature_id).signature_id}")
    print(f"  get by signature: {hasher.get(rec.signature).signature_id}")

    print("\n=== §3.5 delete / clear ===")
    n = hasher.count()
    print(f"  before: {n}")
    hasher.delete(rec.signature_id)
    print(f"  after delete: {hasher.count()}")
    hasher.clear()
    print(f"  after clear: {hasher.count()}")
    assert hasher.count() == 0

    print("\n=== §3.1 Bounded registry (FIFO eviction) ===")
    small = SignatureHasher(
        config=SignatureConfig(max_records=3),
    )
    for i in range(10):
        small.sign(payload={"i": i})
    print(f"  after 10 signs, max 3: {small.count()}")
    assert small.count() == 3

    print("\n=== §4.2 Algorithm configurable ===")
    sha512 = SignatureHasher(config=SignatureConfig(algorithm="sha512"))
    r512 = sha512.sign(payload={"x": 1})
    print(f"  sha512 signature: {r512.signature[:24]}…")
    assert r512.signature.startswith("sha512:")
    # Reject weak algorithms
    for alg in ("md5", "sha1"):
        try:
            SignatureHasher(config=SignatureConfig(algorithm=alg))
            print(f"  {alg}: NOT rejected")
        except SignatureInputError:
            print(f"  {alg}: rejected")

    print("\n=== §4.3 signature_id can be supplied ===")
    pre_id = "sig-custom-fixed-id"
    r = hasher.sign(payload={"x": 1}, signature_id=pre_id)
    print(f"  id: {r.signature_id}")
    assert r.signature_id == pre_id
    try:
        hasher.sign(payload={"x": 2}, signature_id=pre_id)
    except DuplicateSignatureError:
        print(f"  duplicate id → DuplicateSignatureError")

    print("\n=== §1.1 Deprecated SignaturePreserver ===")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        legacy = SignaturePreserver()
        assert any(
            issubclass(w.category, DeprecationWarning) for w in caught
        )
    print(f"  emitted DeprecationWarning ✓")
    legacy_rec = legacy.sign(payload={"x": 1})
    # Old format: no version segment
    print(f"  legacy format signature: {legacy_rec.signature}")
    assert legacy_rec.signature.count(":") == 1  # "sha256:<hex>"

    print("\n=== §3.2 Thread safety ===")
    import threading
    ts_hasher = SignatureHasher()
    def worker(idx: int) -> None:
        for j in range(50):
            ts_hasher.sign(payload={"worker": idx, "j": j})
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  final count: {ts_hasher.count()} (expected 250)")
    assert ts_hasher.count() == 250

    print("\n=== §3.4 health ===")
    print(f"  {ts_hasher.health()['config']}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
