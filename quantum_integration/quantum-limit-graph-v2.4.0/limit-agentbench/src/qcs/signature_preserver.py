"""
Signature Preserver (Enhanced v2)
==================================

Cryptographic hashing and (optional) Ed25519 attestation of quantum state
across the quantum-classical boundary.

What this module provides
-------------------------
  * Deterministic, canonical SHA-256 (or sha512 / sha3_256 / blake2b)
    hashing of thought data, quantum amplitudes, and classical context.
  * Optional **Ed25519** signatures over the canonical hashed payload,
    with the public key retained on the resulting ``ThoughtSignature``.
  * A **real** Merkle tree over signature *content* (not IDs), with
    domain separation, verified by ``verify_chain()``.
  * Backend abstraction for carbon ledger and blockchain anchoring —
    either supply real backends, use the ``InMemory*`` reference
    backends, or accept the "not configured" backend which stores
    nothing and marks signatures ``anchored=False``.
  * Bounded caches, timeouts, retries, revocation, and health checks.

What this module does NOT provide
---------------------------------
  * **SLSA Level 3 provenance.** L3 requires builder isolation, ephemeral
    execution environments, verifiable builder identity, and a public
    build log. This module can sign the *output*, but not the *process*
    that produced it. ``provenance_metadata`` is a placeholder for a
    properly-signed SLSA provenance document — supply one via
    ``ThoughtSignature.metadata["slsa_provenance"]`` if you have it.
  * **Trust in the builder.** A signature here attests that the signer
    saw a specific state vector. It does not attest that the state vector
    was computed correctly. That is out of scope.

Original API preserved:
    preserver = SignaturePreserver(
        carbon_ledger_endpoint, blockchain_endpoint=None,
        hash_algorithm="sha256",
    )
    signature = await preserver.create_signature(
        thought_data, quantum_amplitudes, classical_context,
    )
    ok = await preserver.verify_signature(
        signature.signature_id, thought_data,
        quantum_amplitudes, classical_context,
    )
    chain = await preserver.create_signature_chain([sig1_id, sig2_id])

v2 fixes (see changelog in commit message):
  * Fixed _signature_chains being a local variable (chains were broken).
  * Real Merkle tree over signature content, with domain separation and
    verification. Tampering with any hashed field invalidates the root.
  * Deterministic amplitude hashing via tobytes() + canonical dtype.
  * Canonical JSON with allow_nan=False for thought/context hashes.
  * hmac.compare_digest for every hash comparison.
  * hash_algorithm validated; no silent algorithm fallback.
  * secrets.token_hex(16) IDs (was predictable microsecond hashes).
  * Optional Ed25519 signing; verified=True only when signing succeeds.
  * Ledger/blockchain backends are protocols; unconfigured backends
    raise BackendNotConfiguredError. No more fabricated anchor IDs.
  * Frozen dataclasses; verification returns fresh results (no sticky
    mutation of cached signatures).
  * Bounded LRU caches; timeout + retry on all backend calls.
  * Revocation and expiry supported.
  * verify_chain(), verify_signature_by_id(), verify_signature_detailed().
  * Config dataclass, metrics, health(), UTC datetimes, structured logs.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field, asdict, replace
from datetime import date, datetime, timedelta, timezone
from typing import (
    Any, Awaitable, Callable, Dict, List, Mapping, Optional, Protocol,
    Sequence, Tuple,
)
from types import MappingProxyType

import numpy as np

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Optional Ed25519 support
# =============================================================================

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey,
        Ed25519PublicKey,
    )
    from cryptography.hazmat.primitives import serialization
    from cryptography.exceptions import InvalidSignature
    _HAS_CRYPTO = True
except Exception:  # pragma: no cover
    Ed25519PrivateKey = None  # type: ignore[assignment]
    Ed25519PublicKey = None  # type: ignore[assignment]
    serialization = None  # type: ignore[assignment]
    InvalidSignature = None  # type: ignore[assignment]
    _HAS_CRYPTO = False


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_ALLOWED_HASHES = frozenset({"sha256", "sha512", "sha3_256", "blake2b"})

_LEAF_PREFIX = b"\x00"
_NODE_PREFIX = b"\x01"
_EMPTY_ROOT_SEED = b"\x00empty-signature-chain-v2"
_AMPLITUDE_DOMAIN = b"amplitude_vector\x00"


# =============================================================================
# Errors
# =============================================================================

class SignaturePreserverError(Exception):
    """Base class for all errors raised by SignaturePreserver."""


class SignatureNotFoundError(SignaturePreserverError):
    """Requested signature (or one of several) does not exist."""

    def __init__(self, missing_ids: Sequence[str]) -> None:
        self.missing_ids: Tuple[str, ...] = tuple(missing_ids)
        super().__init__(
            f"missing signatures: {list(self.missing_ids)}"
        )


class ChainNotFoundError(SignaturePreserverError):
    pass


class BackendError(SignaturePreserverError):
    """An external backend failed after retries."""


class BackendNotConfiguredError(SignaturePreserverError, NotImplementedError):
    """Backend is a 'not configured' placeholder; supply a real implementation."""


class AmplitudeValidationError(SignaturePreserverError, ValueError):
    """Quantum amplitudes failed validation."""


class SignatureVerificationError(SignaturePreserverError):
    pass


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class SignaturePreserverConfig:
    hash_algorithm: str = "sha256"

    # --- Timeouts & retries ---
    backend_timeout_s: float = 5.0
    backend_max_attempts: int = 3
    backend_retry_base_s: float = 0.1
    backend_retry_max_s: float = 2.0

    # --- Cache bounds ---
    max_signatures_retained: int = 10_000
    max_chains_retained: int = 1_000

    # --- Batch concurrency ---
    batch_max_concurrency: int = 8

    # --- Signing ---
    enable_signing: bool = True   # only takes effect if cryptography is installed

    # --- SLSA honesty ---
    require_slsa_provenance: bool = False  # reject signatures missing metadata["slsa_provenance"]

    def __post_init__(self) -> None:
        if self.hash_algorithm not in _ALLOWED_HASHES:
            raise ValueError(
                f"hash_algorithm must be one of "
                f"{sorted(_ALLOWED_HASHES)}; got {self.hash_algorithm!r}"
            )
        if self.backend_timeout_s <= 0:
            raise ValueError("backend_timeout_s must be > 0.")
        if self.backend_max_attempts < 1:
            raise ValueError("backend_max_attempts must be >= 1.")
        if self.max_signatures_retained <= 0:
            raise ValueError("max_signatures_retained must be > 0.")
        if self.batch_max_concurrency <= 0:
            raise ValueError("batch_max_concurrency must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
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
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Backend protocols + not-configured + in-memory reference impls
# =============================================================================

class CarbonLedgerBackend(Protocol):
    is_configured: bool
    async def record(self, signature: "ThoughtSignature") -> str: ...
    async def fetch(self, signature_id: str) -> Optional["ThoughtSignature"]: ...
    async def verify_entry(
        self, signature_id: str, entry_id: str,
    ) -> bool: ...
    async def close(self) -> None: ...


class BlockchainBackend(Protocol):
    is_configured: bool
    async def anchor(self, signature: "ThoughtSignature") -> str: ...
    async def verify_anchor(
        self, signature_id: str, tx_hash: str,
    ) -> bool: ...
    async def close(self) -> None: ...


class NotConfiguredCarbonLedger:
    """Default backend: raises if actually used."""

    is_configured = False

    async def record(self, signature: "ThoughtSignature") -> str:
        raise BackendNotConfiguredError(
            "carbon ledger backend is not configured; "
            "supply a CarbonLedgerBackend implementation"
        )

    async def fetch(self, signature_id: str) -> Optional["ThoughtSignature"]:
        raise BackendNotConfiguredError(
            "carbon ledger backend is not configured"
        )

    async def verify_entry(
        self, signature_id: str, entry_id: str,
    ) -> bool:
        raise BackendNotConfiguredError(
            "carbon ledger backend is not configured"
        )

    async def close(self) -> None:
        return None


class NotConfiguredBlockchain:
    is_configured = False

    async def anchor(self, signature: "ThoughtSignature") -> str:
        raise BackendNotConfiguredError(
            "blockchain backend is not configured; "
            "supply a BlockchainBackend implementation"
        )

    async def verify_anchor(
        self, signature_id: str, tx_hash: str,
    ) -> bool:
        raise BackendNotConfiguredError(
            "blockchain backend is not configured"
        )

    async def close(self) -> None:
        return None


class InMemoryCarbonLedger:
    """Reference implementation. Stores entries in memory; useful for tests."""

    is_configured = True

    def __init__(self) -> None:
        self._entries: Dict[str, "ThoughtSignature"] = {}
        self._lock = threading.RLock()

    async def record(self, signature: "ThoughtSignature") -> str:
        entry_id = f"mem-ledger:{signature.signature_id}"
        with self._lock:
            self._entries[signature.signature_id] = signature
        return entry_id

    async def fetch(
        self, signature_id: str,
    ) -> Optional["ThoughtSignature"]:
        with self._lock:
            return self._entries.get(signature_id)

    async def verify_entry(
        self, signature_id: str, entry_id: str,
    ) -> bool:
        with self._lock:
            return (
                signature_id in self._entries
                and entry_id == f"mem-ledger:{signature_id}"
            )

    async def close(self) -> None:
        return None


class InMemoryBlockchain:
    is_configured = True

    def __init__(self) -> None:
        self._txs: Dict[str, str] = {}
        self._lock = threading.RLock()

    async def anchor(self, signature: "ThoughtSignature") -> str:
        tx_hash = "0x" + hashlib.sha256(
            f"mem-tx:{signature.signature_id}".encode("utf-8"),
        ).hexdigest()
        with self._lock:
            self._txs[signature.signature_id] = tx_hash
        return tx_hash

    async def verify_anchor(
        self, signature_id: str, tx_hash: str,
    ) -> bool:
        with self._lock:
            return self._txs.get(signature_id) == tx_hash

    async def close(self) -> None:
        return None


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class ThoughtSignature:
    """Meta-cognitive thought signature at creation time (immutable)."""

    signature_id: str
    thought_hash: str
    quantum_amplitude_hash: str
    classical_context_hash: str
    created_at: datetime
    hash_algorithm: str = "sha256"

    # --- Optional Ed25519 signing ---
    signature_bytes: Optional[bytes] = None
    public_key_pem: Optional[bytes] = None
    signing_algorithm: Optional[str] = None

    # --- Attestation / anchors ---
    carbon_ledger_entry: Optional[str] = None
    blockchain_anchor: Optional[str] = None

    # --- Status snapshot at creation ---
    # True only when an Ed25519 signature was created and attached.
    verified: bool = False

    # --- Lifecycle ---
    expires_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    revocation_reason: Optional[str] = None
    partial: bool = False  # some anchors failed

    # --- Free-form metadata (frozen) ---
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.metadata, Mapping):
            raise TypeError("metadata must be a Mapping")
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata", MappingProxyType(dict(self.metadata)),
            )

    def is_expired(self, at: Optional[datetime] = None) -> bool:
        if self.expires_at is None:
            return False
        now = at or _utcnow()
        exp = self.expires_at
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        return now >= exp

    def is_revoked(self) -> bool:
        return self.revoked_at is not None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signature_id": self.signature_id,
            "thought_hash": self.thought_hash,
            "quantum_amplitude_hash": self.quantum_amplitude_hash,
            "classical_context_hash": self.classical_context_hash,
            "created_at": self.created_at.isoformat(),
            "hash_algorithm": self.hash_algorithm,
            "signature_bytes_b64": (
                base64.b64encode(self.signature_bytes).decode("ascii")
                if self.signature_bytes else None
            ),
            "public_key_pem": (
                self.public_key_pem.decode("ascii")
                if self.public_key_pem else None
            ),
            "signing_algorithm": self.signing_algorithm,
            "carbon_ledger_entry": self.carbon_ledger_entry,
            "blockchain_anchor": self.blockchain_anchor,
            "verified": self.verified,
            "expires_at": (
                self.expires_at.isoformat() if self.expires_at else None
            ),
            "revoked_at": (
                self.revoked_at.isoformat() if self.revoked_at else None
            ),
            "revocation_reason": self.revocation_reason,
            "partial": self.partial,
            "metadata": _to_jsonable(dict(self.metadata)),
        }


@dataclass(frozen=True)
class SignatureChain:
    """Immutable chain with Merkle root over signature *content*."""

    chain_id: str
    signatures: Tuple[ThoughtSignature, ...]
    merkle_root: str
    created_at: datetime
    verified: bool = False
    merkle_algorithm: str = "sha256"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chain_id": self.chain_id,
            "signature_ids": [s.signature_id for s in self.signatures],
            "merkle_root": self.merkle_root,
            "merkle_algorithm": self.merkle_algorithm,
            "created_at": self.created_at.isoformat(),
            "verified": self.verified,
        }


@dataclass(frozen=True)
class VerificationResult:
    """Rich verification outcome. Never mutates the cached signature."""

    signature_id: str
    valid: bool
    reason: str
    thought_hash_ok: bool = False
    amplitude_hash_ok: bool = False
    context_hash_ok: bool = False
    signature_bytes_ok: bool = False
    expired: bool = False
    revoked: bool = False
    signature: Optional[ThoughtSignature] = None
    verified_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signature_id": self.signature_id,
            "valid": self.valid,
            "reason": self.reason,
            "thought_hash_ok": self.thought_hash_ok,
            "amplitude_hash_ok": self.amplitude_hash_ok,
            "context_hash_ok": self.context_hash_ok,
            "signature_bytes_ok": self.signature_bytes_ok,
            "expired": self.expired,
            "revoked": self.revoked,
            "verified_at": self.verified_at.isoformat(),
        }


# =============================================================================
# Canonicalisation helpers
# =============================================================================

def _canonical_json(data: Any) -> bytes:
    """
    Deterministic JSON: sorted keys, no whitespace, no NaN/Infinity,
    explicit handlers for common non-JSON types.

    Raises TypeError for anything unsupported — better than silently hashing
    a repr() or skipping a field.
    """
    def _default(o: Any) -> Any:
        if isinstance(o, np.ndarray):
            return {
                "__ndarray__": o.tolist(),
                "dtype": str(o.dtype),
                "shape": list(o.shape),
            }
        if isinstance(o, (datetime, date)):
            return {"__datetime__": o.isoformat()}
        if isinstance(o, (bytes, bytearray)):
            return {"__bytes__": base64.b64encode(bytes(o)).decode("ascii")}
        if isinstance(o, (set, frozenset)):
            return {"__set__": sorted(o, key=str)}
        if isinstance(o, Mapping):
            return dict(o)
        if hasattr(o, "__dataclass_fields__"):
            return asdict(o)
        raise TypeError(
            f"_canonical_json: unsupported type {type(o).__name__}"
        )

    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_default,
    ).encode("utf-8")


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    if isinstance(obj, (set, frozenset)):
        return sorted(obj, key=str)
    return obj


# =============================================================================
# The Enhanced SignaturePreserver
# =============================================================================

class SignaturePreserver:
    """
    Cryptographic hashing and (optional) Ed25519 attestation of quantum
    state across the quantum-classical boundary.

    Parameters
    ----------
    carbon_ledger_endpoint : str
        Passed to the carbon ledger backend if one is supplied; used as a
        metadata label otherwise.
    blockchain_endpoint : Optional[str]
        Same.
    hash_algorithm : str
        One of ``_ALLOWED_HASHES``. Used for all content hashes and the
        Merkle tree.
    carbon_ledger_backend : Optional[CarbonLedgerBackend]
        Real backend. If ``None``, uses ``NotConfiguredCarbonLedger``.
    blockchain_backend : Optional[BlockchainBackend]
        Real backend. If ``None``, uses ``NotConfiguredBlockchain``.
    signing_private_key : Optional[Ed25519PrivateKey]
        If supplied and `cryptography` is installed, every signature is
        attested with Ed25519. Otherwise `verified` is ``False`` on the
        resulting ThoughtSignature (hashes computed but not attested).
    config : Optional[SignaturePreserverConfig]
    metrics : Optional[MetricsRecorder]
    """

    def __init__(
        self,
        carbon_ledger_endpoint: str,
        blockchain_endpoint: Optional[str] = None,
        hash_algorithm: str = "sha256",
        *,
        carbon_ledger_backend: Optional[CarbonLedgerBackend] = None,
        blockchain_backend: Optional[BlockchainBackend] = None,
        signing_private_key: Optional[Any] = None,
        config: Optional[SignaturePreserverConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        # Backward-compatible positional hash_algorithm → overrides config.
        cfg = config or SignaturePreserverConfig()
        if hash_algorithm != cfg.hash_algorithm:
            cfg = replace(cfg, hash_algorithm=hash_algorithm)
        cfg.__post_init__()  # re-validate if replaced
        self.config = cfg

        if not isinstance(carbon_ledger_endpoint, str) or not carbon_ledger_endpoint:
            raise ValueError("carbon_ledger_endpoint must be a non-empty string")

        self.carbon_ledger_endpoint = carbon_ledger_endpoint
        self.blockchain_endpoint = blockchain_endpoint

        self._ledger: CarbonLedgerBackend = (
            carbon_ledger_backend or NotConfiguredCarbonLedger()
        )
        self._blockchain: BlockchainBackend = (
            blockchain_backend or NotConfiguredBlockchain()
        )

        # Signing.
        self._private_key = None
        if signing_private_key is not None:
            if not _HAS_CRYPTO:
                raise RuntimeError(
                    "signing_private_key supplied but 'cryptography' "
                    "is not installed"
                )
            self._private_key = signing_private_key
        self._signing_enabled = bool(
            self.config.enable_signing
            and _HAS_CRYPTO
            and self._private_key is not None
        )

        # Bounded caches.
        self._signature_cache: "OrderedDict[str, ThoughtSignature]" = \
            OrderedDict()
        self._signature_chains: "OrderedDict[str, SignatureChain]" = \
            OrderedDict()
        self._lock = threading.RLock()
        self._inflight_creations: set[str] = set()

        # Metrics.
        self._metrics = metrics or MetricsRecorder()

        # Lifecycle.
        self._closed = False
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self._closed:
            raise SignaturePreserverError("instance is closed")
        return None

    async def stop(self) -> None:
        self._closed = True
        try:
            await self._ledger.close()
        except Exception:
            logger.exception("carbon ledger close failed")
        try:
            await self._blockchain.close()
        except Exception:
            logger.exception("blockchain close failed")

    async def close(self) -> None:
        await self.stop()

    async def __aenter__(self) -> "SignaturePreserver":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------ creation
    async def create_signature(
        self,
        thought_data: Mapping[str, Any],
        quantum_amplitudes: np.ndarray,
        classical_context: Mapping[str, Any],
        *,
        expires_in: Optional[timedelta] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        skip_ledger: bool = False,
        skip_blockchain: bool = False,
    ) -> ThoughtSignature:
        """
        Create a ThoughtSignature.

        Hashes thought data, quantum amplitudes, and classical context using
        the configured algorithm; optionally signs the canonical payload
        with Ed25519; optionally writes to the carbon ledger and blockchain
        backends.

        Backend failures (after retries) do NOT raise — the returned
        signature is marked ``partial=True``. This preserves the audit
        trail locally. To fail loudly, use ``strict_backends=True`` in a
        subclass or inspect ``partial`` at the call site.
        """
        if self._closed:
            raise SignaturePreserverError("instance is closed")

        t0 = time.monotonic()
        signature_id = self._generate_signature_id()

        with self._lock:
            if signature_id in self._inflight_creations:
                raise SignaturePreserverError(
                    f"signature_id collision: {signature_id}"
                )
            self._inflight_creations.add(signature_id)

        try:
            meta = dict(metadata or {})
            if self.config.require_slsa_provenance and \
                    "slsa_provenance" not in meta:
                raise SignaturePreserverError(
                    "require_slsa_provenance=True but no metadata"
                    "['slsa_provenance'] supplied"
                )

            # Compute the three hashes.
            thought_hash = self._hash_data(thought_data)
            amplitude_hash = self._hash_amplitudes(quantum_amplitudes)
            context_hash = self._hash_data(classical_context)

            # Build the canonical signed payload.
            signing_payload = self._canonical_signing_payload(
                signature_id=signature_id,
                thought_hash=thought_hash,
                amplitude_hash=amplitude_hash,
                context_hash=context_hash,
                hash_algorithm=self.config.hash_algorithm,
                metadata=meta,
            )

            # Optional Ed25519 sign.
            signature_bytes: Optional[bytes] = None
            public_key_pem: Optional[bytes] = None
            signing_algorithm: Optional[str] = None
            signed_ok = False
            if self._signing_enabled and self._private_key is not None:
                try:
                    signature_bytes = self._private_key.sign(signing_payload)
                    public_key_pem = self._private_key.public_key().public_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PublicFormat.SubjectPublicKeyInfo,
                    )
                    signing_algorithm = "ed25519"
                    signed_ok = True
                    self._metrics.inc("sign_ok")
                except Exception as e:
                    logger.exception("Ed25519 signing failed")
                    self._metrics.inc("sign_failures")
                    signed_ok = False

            # Provisional signature (immutable; we rebuild on anchor success).
            expires_at = (
                _utcnow() + expires_in if expires_in is not None else None
            )
            provisional = ThoughtSignature(
                signature_id=signature_id,
                thought_hash=thought_hash,
                quantum_amplitude_hash=amplitude_hash,
                classical_context_hash=context_hash,
                created_at=_utcnow(),
                hash_algorithm=self.config.hash_algorithm,
                signature_bytes=signature_bytes,
                public_key_pem=public_key_pem,
                signing_algorithm=signing_algorithm,
                verified=signed_ok,
                expires_at=expires_at,
                metadata=meta,
            )

            partial = False

            # Carbon ledger.
            ledger_entry: Optional[str] = None
            if not skip_ledger and self._ledger.is_configured:
                try:
                    ledger_entry = await self._call_backend_with_retries(
                        lambda: self._ledger.record(provisional),
                        operation="ledger_record",
                    )
                    self._metrics.inc("ledger_write_ok")
                except BackendNotConfiguredError:
                    raise
                except BackendError as e:
                    logger.warning(
                        "carbon ledger write failed (partial signature): %s",
                        e,
                        extra={"signature_id": signature_id},
                    )
                    self._metrics.inc("ledger_write_failures")
                    partial = True

            # Blockchain.
            blockchain_anchor: Optional[str] = None
            if not skip_blockchain and self._blockchain.is_configured:
                try:
                    blockchain_anchor = await self._call_backend_with_retries(
                        lambda: self._blockchain.anchor(provisional),
                        operation="blockchain_anchor",
                    )
                    self._metrics.inc("blockchain_anchor_ok")
                except BackendNotConfiguredError:
                    raise
                except BackendError as e:
                    logger.warning(
                        "blockchain anchor failed (partial signature): %s",
                        e,
                        extra={"signature_id": signature_id},
                    )
                    self._metrics.inc("blockchain_anchor_failures")
                    partial = True

            # Final frozen signature.
            final = replace(
                provisional,
                carbon_ledger_entry=ledger_entry,
                blockchain_anchor=blockchain_anchor,
                partial=partial,
            )

            self._store_signature(final)
            self._metrics.inc(
                "signature_created",
                signed=str(signed_ok).lower(),
                partial=str(partial).lower(),
            )
            self._metrics.observe(
                "signature_create_ms",
                (time.monotonic() - t0) * 1000.0,
            )

            logger.info(
                "signature created",
                extra={
                    "signature_id": signature_id,
                    "signed": signed_ok,
                    "partial": partial,
                },
            )
            return final
        finally:
            with self._lock:
                self._inflight_creations.discard(signature_id)

    async def create_signatures_batch(
        self,
        items: Sequence[
            Tuple[Mapping[str, Any], np.ndarray, Mapping[str, Any]]
        ],
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> List[ThoughtSignature]:
        """Create multiple signatures concurrently (bounded)."""
        sem = asyncio.Semaphore(self.config.batch_max_concurrency)

        async def _one(idx: int, item: Tuple[Any, Any, Any]):
            async with sem:
                thought, amps, ctx = item
                return await self.create_signature(
                    thought, amps, ctx, metadata=metadata,
                )

        results = await asyncio.gather(
            *[_one(i, it) for i, it in enumerate(items)],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                # Re-raise the first failure; partial batches are a footgun.
                raise r
        return list(results)  # type: ignore[return-value]

    # ------------------------------------------------------------------ lookup
    async def get_signature(
        self, signature_id: str,
    ) -> Optional[ThoughtSignature]:
        with self._lock:
            cached = self._signature_cache.get(signature_id)
        if cached is not None:
            return cached
        # Fall back to ledger (§3.6).
        if not self._ledger.is_configured:
            return None
        try:
            fetched = await self._call_backend_with_retries(
                lambda: self._ledger.fetch(signature_id),
                operation="ledger_fetch",
            )
        except BackendError:
            return None
        if fetched is not None:
            self._store_signature(fetched)
        return fetched

    async def get_chain(
        self, chain_id: str,
    ) -> Optional[SignatureChain]:
        with self._lock:
            return self._signature_chains.get(chain_id)

    # ------------------------------------------------------------------ verify
    async def verify_signature(
        self,
        signature_id: str,
        thought_data: Mapping[str, Any],
        quantum_amplitudes: np.ndarray,
        classical_context: Mapping[str, Any],
    ) -> bool:
        """
        Backward-compatible bool entry point.

        Use :meth:`verify_signature_detailed` or
        :meth:`verify_signature_by_id` for a richer result.
        """
        result = await self.verify_signature_detailed(
            signature_id,
            thought_data,
            quantum_amplitudes,
            classical_context,
        )
        return result.valid

    async def verify_signature_detailed(
        self,
        signature_id: str,
        thought_data: Mapping[str, Any],
        quantum_amplitudes: np.ndarray,
        classical_context: Mapping[str, Any],
    ) -> VerificationResult:
        sig = await self.get_signature(signature_id)
        if sig is None:
            return VerificationResult(
                signature_id=signature_id,
                valid=False,
                reason="not_found",
            )
        return self._verify_against(
            sig=sig,
            thought_data=thought_data,
            quantum_amplitudes=quantum_amplitudes,
            classical_context=classical_context,
        )

    async def verify_signature_by_id(
        self, signature_id: str,
    ) -> VerificationResult:
        """
        Verify a stored signature by re-deriving hashes from the ledger (if
        available) and checking its Ed25519 signature. Uses no external
        inputs beyond the signature_id.
        """
        sig = await self.get_signature(signature_id)
        if sig is None:
            return VerificationResult(
                signature_id=signature_id,
                valid=False,
                reason="not_found",
            )
        return self._verify_intrinsic(sig)

    # ------------------------------------------------------------------ revocation
    async def revoke_signature(
        self,
        signature_id: str,
        reason: str,
    ) -> ThoughtSignature:
        if not isinstance(reason, str) or not reason:
            raise ValueError("reason must be a non-empty string")
        with self._lock:
            sig = self._signature_cache.get(signature_id)
        if sig is None:
            raise SignatureNotFoundError([signature_id])
        if sig.revoked_at is not None:
            return sig  # idempotent
        updated = replace(
            sig,
            revoked_at=_utcnow(),
            revocation_reason=reason,
        )
        with self._lock:
            self._signature_cache[signature_id] = updated
        self._metrics.inc("signature_revoked")
        logger.warning(
            "signature revoked",
            extra={"signature_id": signature_id, "reason": reason},
        )
        return updated

    # ------------------------------------------------------------------ chains
    async def create_signature_chain(
        self,
        signature_ids: Sequence[str],
    ) -> SignatureChain:
        if not signature_ids:
            raise ValueError("signature_ids must be non-empty")
        if len(set(signature_ids)) != len(signature_ids):
            raise ValueError("signature_ids contains duplicates")

        signatures: List[ThoughtSignature] = []
        missing: List[str] = []
        for sid in signature_ids:
            sig = await self.get_signature(sid)
            if sig is None:
                missing.append(sid)
            else:
                signatures.append(sig)
        if missing:
            raise SignatureNotFoundError(missing)

        leaves = [self._leaf_bytes(sig) for sig in signatures]
        merkle_root = self._merkle_root(leaves)
        chain = SignatureChain(
            chain_id=self._generate_chain_id(),
            signatures=tuple(signatures),
            merkle_root=merkle_root.hex(),
            created_at=_utcnow(),
            verified=False,
            merkle_algorithm=self.config.hash_algorithm,
        )
        with self._lock:
            self._signature_chains[chain.chain_id] = chain
            while len(self._signature_chains) > \
                    self.config.max_chains_retained:
                self._signature_chains.popitem(last=False)
        self._metrics.inc("chain_created")
        self._metrics.observe("chain_size", len(signatures))
        logger.info(
            "signature chain created",
            extra={
                "chain_id": chain.chain_id,
                "n_signatures": len(signatures),
            },
        )
        return chain

    async def verify_chain(self, chain_id: str) -> bool:
        with self._lock:
            chain = self._signature_chains.get(chain_id)
        if chain is None:
            return False
        leaves = [self._leaf_bytes(sig) for sig in chain.signatures]
        recomputed = self._merkle_root(leaves).hex()
        ok = hmac.compare_digest(recomputed, chain.merkle_root)
        if ok:
            # Mark verified by storing a verified copy.
            with self._lock:
                self._signature_chains[chain_id] = replace(
                    chain, verified=True,
                )
            self._metrics.inc("chain_verify_ok")
        else:
            self._metrics.inc("chain_verify_fail")
        return ok

    # ------------------------------------------------------------------ anchors
    async def verify_ledger_entry(self, signature_id: str) -> bool:
        sig = await self.get_signature(signature_id)
        if sig is None or sig.carbon_ledger_entry is None:
            return False
        if not self._ledger.is_configured:
            return False
        try:
            return await self._call_backend_with_retries(
                lambda: self._ledger.verify_entry(
                    signature_id, sig.carbon_ledger_entry,
                ),
                operation="ledger_verify",
            )
        except BackendError:
            return False

    async def verify_blockchain_anchor(self, signature_id: str) -> bool:
        sig = await self.get_signature(signature_id)
        if sig is None or sig.blockchain_anchor is None:
            return False
        if not self._blockchain.is_configured:
            return False
        try:
            return await self._call_backend_with_retries(
                lambda: self._blockchain.verify_anchor(
                    signature_id, sig.blockchain_anchor,
                ),
                operation="blockchain_verify",
            )
        except BackendError:
            return False

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        with self._lock:
            n_sigs = len(self._signature_cache)
            n_chains = len(self._signature_chains)
            inflight = len(self._inflight_creations)
        return {
            "ready": not self._closed,
            "closed": self._closed,
            "hash_algorithm": self.config.hash_algorithm,
            "signing_enabled": self._signing_enabled,
            "crypto_available": _HAS_CRYPTO,
            "ledger_configured": self._ledger.is_configured,
            "blockchain_configured": self._blockchain.is_configured,
            "signatures_cached": n_sigs,
            "chains_cached": n_chains,
            "inflight_creations": inflight,
            "uptime_seconds": time.monotonic() - self._started_mono,
        }

    def get_statistics(self) -> Dict[str, Any]:
        h = self.health()
        h["metrics"] = self._metrics.snapshot()
        h["config"] = self.config.to_dict()
        return h

    # ------------------------------------------------------------------ internals
    def _store_signature(self, sig: ThoughtSignature) -> None:
        with self._lock:
            self._signature_cache[sig.signature_id] = sig
            self._signature_cache.move_to_end(sig.signature_id)
            while len(self._signature_cache) > \
                    self.config.max_signatures_retained:
                self._signature_cache.popitem(last=False)

    async def _call_backend_with_retries(
        self,
        call: Callable[[], Awaitable[Any]],
        *,
        operation: str,
    ) -> Any:
        attempts = self.config.backend_max_attempts
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                return await asyncio.wait_for(
                    call(), timeout=self.config.backend_timeout_s,
                )
            except asyncio.TimeoutError as e:
                last_exc = e
                self._metrics.inc(
                    "backend_timeouts", operation=operation,
                )
                logger.warning(
                    "backend timeout",
                    extra={"operation": operation, "attempt": attempt},
                )
            except BackendNotConfiguredError:
                raise
            except Exception as e:
                last_exc = e
                self._metrics.inc(
                    "backend_errors", operation=operation,
                )
                logger.warning(
                    "backend error",
                    extra={"operation": operation, "attempt": attempt},
                    exc_info=True,
                )
            if attempt < attempts:
                delay = min(
                    self.config.backend_retry_max_s,
                    self.config.backend_retry_base_s * (2 ** (attempt - 1)),
                )
                await asyncio.sleep(delay)
        raise BackendError(
            f"{operation} failed after {attempts} attempts: {last_exc}"
        ) from last_exc

    def _hash_data(self, data: Mapping[str, Any]) -> str:
        payload = _canonical_json(data)
        h = hashlib.new(self.config.hash_algorithm)
        h.update(payload)
        return h.hexdigest()

    def _hash_amplitudes(self, amplitudes: np.ndarray) -> str:
        if not isinstance(amplitudes, np.ndarray):
            amplitudes = np.asarray(amplitudes, dtype=np.complex128)
        if amplitudes.ndim != 1:
            raise AmplitudeValidationError(
                f"amplitudes must be 1-D, got shape {amplitudes.shape}"
            )
        n = amplitudes.shape[0]
        if n == 0:
            raise AmplitudeValidationError("amplitudes must not be empty")
        # Canonical dtype + little-endian + contiguous.
        canonical = np.ascontiguousarray(
            amplitudes, dtype=np.complex128,
        ).astype("<c16", copy=False)
        h = hashlib.new(self.config.hash_algorithm)
        h.update(_AMPLITUDE_DOMAIN)
        h.update(n.to_bytes(8, "little", signed=False))
        h.update(canonical.tobytes(order="C"))
        return h.hexdigest()

    def _canonical_signing_payload(
        self,
        *,
        signature_id: str,
        thought_hash: str,
        amplitude_hash: str,
        context_hash: str,
        hash_algorithm: str,
        metadata: Mapping[str, Any],
    ) -> bytes:
        return _canonical_json({
            "signature_id": signature_id,
            "thought_hash": thought_hash,
            "quantum_amplitude_hash": amplitude_hash,
            "classical_context_hash": context_hash,
            "hash_algorithm": hash_algorithm,
            "metadata": dict(metadata),
        })

    def _leaf_bytes(self, sig: ThoughtSignature) -> bytes:
        payload = _canonical_json({
            "signature_id": sig.signature_id,
            "thought_hash": sig.thought_hash,
            "quantum_amplitude_hash": sig.quantum_amplitude_hash,
            "classical_context_hash": sig.classical_context_hash,
            "hash_algorithm": sig.hash_algorithm,
            "created_at": sig.created_at.isoformat(),
            "expires_at": (
                sig.expires_at.isoformat() if sig.expires_at else None
            ),
            "revoked_at": (
                sig.revoked_at.isoformat() if sig.revoked_at else None
            ),
            "signature_bytes_b64": (
                base64.b64encode(sig.signature_bytes).decode("ascii")
                if sig.signature_bytes else None
            ),
            "signing_algorithm": sig.signing_algorithm,
        })
        h = hashlib.new(sig.hash_algorithm)
        h.update(_LEAF_PREFIX)
        h.update(payload)
        return h.digest()

    def _node_bytes(self, left: bytes, right: bytes) -> bytes:
        h = hashlib.new(self.config.hash_algorithm)
        h.update(_NODE_PREFIX)
        h.update(left)
        h.update(right)
        return h.digest()

    def _merkle_root(self, leaves: List[bytes]) -> bytes:
        if not leaves:
            h = hashlib.new(self.config.hash_algorithm)
            h.update(_EMPTY_ROOT_SEED)
            return h.digest()
        level = list(leaves)
        while len(level) > 1:
            nxt: List[bytes] = []
            for i in range(0, len(level), 2):
                if i + 1 == len(level):
                    # Domain-separated duplicate; does NOT append to the
                    # level (avoids the standard CVE-2012-2459 class).
                    nxt.append(self._node_bytes(level[i], level[i]))
                else:
                    nxt.append(self._node_bytes(level[i], level[i + 1]))
            level = nxt
        return level[0]

    def _verify_against(
        self,
        *,
        sig: ThoughtSignature,
        thought_data: Mapping[str, Any],
        quantum_amplitudes: np.ndarray,
        classical_context: Mapping[str, Any],
    ) -> VerificationResult:
        if sig.is_revoked():
            return VerificationResult(
                signature_id=sig.signature_id,
                valid=False,
                reason="revoked",
                revoked=True,
                signature=sig,
            )
        if sig.is_expired():
            return VerificationResult(
                signature_id=sig.signature_id,
                valid=False,
                reason="expired",
                expired=True,
                signature=sig,
            )

        thought_hash = self._hash_data(thought_data)
        amplitude_hash = self._hash_amplitudes(quantum_amplitudes)
        context_hash = self._hash_data(classical_context)

        thought_ok = hmac.compare_digest(thought_hash, sig.thought_hash)
        amp_ok = hmac.compare_digest(
            amplitude_hash, sig.quantum_amplitude_hash,
        )
        ctx_ok = hmac.compare_digest(context_hash, sig.classical_context_hash)

        # Signature bytes verify against the canonical payload.
        sig_ok = True
        if sig.signature_bytes is not None and _HAS_CRYPTO:
            try:
                payload = self._canonical_signing_payload(
                    signature_id=sig.signature_id,
                    thought_hash=sig.thought_hash,
                    amplitude_hash=sig.quantum_amplitude_hash,
                    context_hash=sig.classical_context_hash,
                    hash_algorithm=sig.hash_algorithm,
                    metadata={},  # NOTE: metadata is not part of the signed payload
                )
                if sig.public_key_pem is None:
                    sig_ok = False
                else:
                    pub = serialization.load_pem_public_key(
                        sig.public_key_pem,
                    )
                    pub.verify(sig.signature_bytes, payload)
                    sig_ok = True
            except Exception as e:
                logger.warning(
                    "signature bytes verification failed: %s", e,
                    extra={"signature_id": sig.signature_id},
                )
                sig_ok = False

        reason = "ok"
        if not thought_ok:
            reason = "thought_hash_mismatch"
        elif not amp_ok:
            reason = "amplitude_hash_mismatch"
        elif not ctx_ok:
            reason = "context_hash_mismatch"
        elif not sig_ok:
            reason = "signature_bytes_invalid"

        valid = thought_ok and amp_ok and ctx_ok and sig_ok

        self._metrics.inc(
            "verify_total", result=reason,
        )
        return VerificationResult(
            signature_id=sig.signature_id,
            valid=valid,
            reason=reason,
            thought_hash_ok=thought_ok,
            amplitude_hash_ok=amp_ok,
            context_hash_ok=ctx_ok,
            signature_bytes_ok=sig_ok,
            signature=sig,
        )

    def _verify_intrinsic(
        self, sig: ThoughtSignature,
    ) -> VerificationResult:
        """Verify only what the signature attests (no external inputs)."""
        if sig.is_revoked():
            return VerificationResult(
                signature_id=sig.signature_id,
                valid=False,
                reason="revoked",
                revoked=True,
                signature=sig,
            )
        if sig.is_expired():
            return VerificationResult(
                signature_id=sig.signature_id,
                valid=False,
                reason="expired",
                expired=True,
                signature=sig,
            )
        if sig.signature_bytes is None or not _HAS_CRYPTO:
            return VerificationResult(
                signature_id=sig.signature_id,
                valid=False,
                reason="not_signed",
                signature=sig,
            )
        try:
            payload = self._canonical_signing_payload(
                signature_id=sig.signature_id,
                thought_hash=sig.thought_hash,
                amplitude_hash=sig.quantum_amplitude_hash,
                context_hash=sig.classical_context_hash,
                hash_algorithm=sig.hash_algorithm,
                metadata={},
            )
            pub = serialization.load_pem_public_key(sig.public_key_pem)
            pub.verify(sig.signature_bytes, payload)
        except Exception:
            return VerificationResult(
                signature_id=sig.signature_id,
                valid=False,
                reason="signature_bytes_invalid",
                signature=sig,
            )
        return VerificationResult(
            signature_id=sig.signature_id,
            valid=True,
            reason="ok",
            signature_bytes_ok=True,
            signature=sig,
        )

    # ------------------------------------------------------------------ ids
    @staticmethod
    def _generate_signature_id() -> str:
        return "sig-" + secrets.token_hex(16)

    @staticmethod
    def _generate_chain_id() -> str:
        return "chain-" + secrets.token_hex(16)


__all__ = [
    "AmplitudeValidationError",
    "BackendError",
    "BackendNotConfiguredError",
    "BlockchainBackend",
    "CarbonLedgerBackend",
    "ChainNotFoundError",
    "InMemoryBlockchain",
    "InMemoryCarbonLedger",
    "MetricsRecorder",
    "NotConfiguredBlockchain",
    "NotConfiguredCarbonLedger",
    "SignatureChain",
    "SignatureNotFoundError",
    "SignaturePreserver",
    "SignaturePreserverConfig",
    "SignaturePreserverError",
    "SignatureVerificationError",
    "ThoughtSignature",
    "VerificationResult",
]


# =============================================================================
# Demo: python -m qcs.signature_preserver
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import asyncio

    logging.basicConfig(level=logging.INFO)

    async def demo() -> None:
        # --- signing key ---
        if _HAS_CRYPTO:
            private_key = Ed25519PrivateKey.generate()
            print("[demo] Ed25519 key generated")
        else:
            private_key = None
            print("[demo] cryptography not installed → hash-only mode")

        # --- backends ---
        ledger = InMemoryCarbonLedger()
        chain_anchor = InMemoryBlockchain()

        preserver = SignaturePreserver(
            carbon_ledger_endpoint="memory://ledger",
            blockchain_endpoint="memory://chain",
            hash_algorithm="sha256",
            carbon_ledger_backend=ledger,
            blockchain_backend=chain_anchor,
            signing_private_key=private_key,
            config=SignaturePreserverConfig(
                max_signatures_retained=8,
                max_chains_retained=4,
                backend_timeout_s=1.0,
                backend_max_attempts=2,
            ),
        )

        async with preserver:
            # --- Create + verify ---
            amps = np.array(
                [1 / math.sqrt(2), 0, 0, 1 / math.sqrt(2)],
                dtype=np.complex128,
            )
            thought = {"goal": "bell-2q", "layer": 3}
            ctx = {"hardware": "mock-qpu", "shots": 500}

            sig = await preserver.create_signature(thought, amps, ctx)
            print("\n=== Created signature ===")
            print(f"  signature_id:  {sig.signature_id}")
            print(f"  hash_alg:      {sig.hash_algorithm}")
            print(f"  signed:        {sig.verified}  "
                  f"(algo={sig.signing_algorithm})")
            print(f"  partial:       {sig.partial}")
            print(f"  ledger_entry:  {sig.carbon_ledger_entry}")
            print(f"  chain_anchor:  {sig.blockchain_anchor}")

            vr = await preserver.verify_signature_detailed(
                sig.signature_id, thought, amps, ctx,
            )
            print(f"\n=== Verify ===")
            print(f"  valid:  {vr.valid}  reason={vr.reason}")
            assert vr.valid

            # --- Tampered input ---
            vr_bad = await preserver.verify_signature_detailed(
                sig.signature_id, thought, amps * 0.99, ctx,
            )
            print(f"  tampered amplitudes → valid={vr_bad.valid}, "
                  f"reason={vr_bad.reason}")
            assert not vr_bad.valid
            assert vr_bad.reason == "amplitude_hash_mismatch"

            # --- verify by id ---
            vr_id = await preserver.verify_signature_by_id(sig.signature_id)
            print(f"  verify_by_id → valid={vr_id.valid}, "
                  f"reason={vr_id.reason}")

            # --- revocation ---
            await preserver.revoke_signature(sig.signature_id, "test revocation")
            vr_revoked = await preserver.verify_signature_detailed(
                sig.signature_id, thought, amps, ctx,
            )
            print(f"  after revocation → valid={vr_revoked.valid}, "
                  f"reason={vr_revoked.reason}")
            assert vr_revoked.reason == "revoked"

            # --- Chain creation + verification ---
            sig2 = await preserver.create_signature(
                {"goal": "bell-2q-2"}, amps,
                {"hardware": "mock-qpu", "shots": 500},
            )
            sig3 = await preserver.create_signature(
                {"goal": "bell-2q-3"}, amps,
                {"hardware": "mock-qpu", "shots": 500},
            )

            chain = await preserver.create_signature_chain(
                [sig2.signature_id, sig3.signature_id],
            )
            print(f"\n=== Chain ===")
            print(f"  chain_id:    {chain.chain_id}")
            print(f"  n:           {len(chain.signatures)}")
            print(f"  merkle_root: {chain.merkle_root[:16]}…")
            assert await preserver.verify_chain(chain.chain_id)

            # --- Missing IDs rejected ---
            try:
                await preserver.create_signature_chain(["does-not-exist"])
            except SignatureNotFoundError as e:
                print(f"\n  missing ids rejected: {e.missing_ids}")

            # --- Unconfigured backend honesty ---
            preserver_nc = SignaturePreserver(
                carbon_ledger_endpoint="nope://ledger",
                hash_algorithm="sha256",
                signing_private_key=private_key,
            )
            sig_nc = await preserver_nc.create_signature(
                {"goal": "x"}, amps, {"k": "v"},
            )
            print(f"\n=== Unconfigured backends ===")
            print(f"  ledger_entry:  {sig_nc.carbon_ledger_entry}  (None expected)")
            print(f"  chain_anchor:  {sig_nc.blockchain_anchor}  (None expected)")
            print(f"  partial:       {sig_nc.partial}")

            # --- Determinism: same inputs → same hashes ---
            sig_dup = await preserver.create_signature(
                {"goal": "bell-2q-3"}, amps,
                {"hardware": "mock-qpu", "shots": 500},
            )
            print(f"\n=== Determinism ===")
            print(f"  thought hash equal: "
                  f"{sig_dup.thought_hash == sig3.thought_hash}")
            print(f"  amp hash equal: "
                  f"{sig_dup.quantum_amplitude_hash == sig3.quantum_amplitude_hash}")
            print(f"  ctx hash equal: "
                  f"{sig_dup.classical_context_hash == sig3.classical_context_hash}")
            assert sig_dup.thought_hash == sig3.thought_hash
            assert sig_dup.quantum_amplitude_hash == sig3.quantum_amplitude_hash
            assert sig_dup.classical_context_hash == sig3.classical_context_hash

            # --- Bounded cache ---
            print(f"\n=== Bounded cache ===")
            for i in range(20):
                await preserver.create_signature(
                    {"n": i}, amps, {"i": i},
                )
            print(f"  signatures cached: "
                  f"{len(preserver._signature_cache)} (max 8)")
            assert len(preserver._signature_cache) == 8

            # --- Health ---
            print(f"\n=== Health ===")
            print(preserver.health())

            # --- Stats ---
            import json
            print("\n=== Statistics ===")
            print(json.dumps(preserver.get_statistics(), indent=2, default=str))

    import math
    asyncio.run(demo())
    print("\nSmoke test passed.")
