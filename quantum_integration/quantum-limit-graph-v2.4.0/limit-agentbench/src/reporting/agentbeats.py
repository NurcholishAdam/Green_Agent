# src/r1/agentbeats_submission_builder.py

"""
AgentBeats Submission Builder for Green Agent
=============================================

Builds AgentBeats benchmark submissions from EvidenceBundles.
Backward-compatible with the original ``build_agentbeats_submission``.

Public API
----------
- :func:`build_agentbeats_submission` — write a submission JSON.
- :func:`verify_submission` — return ``True`` iff the file's
  ``content_hash`` matches its canonical payload.
- :func:`verify_submission_detailed` — same, with a reason dict.
- :class:`ReproducibilityBundle` — captures everything needed to
  reproduce a submission; supports ``build`` / ``to_dict`` /
  ``from_dict`` / ``compute_hash``.

Design
------
- **Single canonical form.** :func:`_canonical_payload` is the only
  serialisation used by :meth:`ReproducibilityBundle.compute_hash`,
  the writer, and the verifier. The enhanced on-disk file *is*
  ``ReproducibilityBundle.to_dict()`` — no flattened re-assembly, so
  the hash round-trips by construction.
- **Atomic writes** — ``tempfile.mkstemp`` in the target directory +
  ``os.replace``. ``allow_nan=False`` rejects ``NaN`` / ``Inf`` at
  write time; the file is always RFC-8259-valid JSON.
- **Enhanced shape is triggered by ``bundle is not None``**, not by
  ``system_version``. A caller that only bumps the version keeps the
  minimal shape and gets ``system_version`` recorded additively.
- **``strict`` mode** — write failures, non-serializable queries, and
  self-verification failures raise :class:`AgentBeatsError`. In
  non-strict mode, ``build_agentbeats_submission`` returns ``None``
  on failure (instead of returning a path that may not exist).
- **Self-verification** — after a successful write of an enhanced
  submission, :func:`verify_submission` runs against the file on
  disk. A mismatch (writer / hasher divergence) is reported as an
  error, not swallowed.

Version: 2.0.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "AgentBeatsError",
    "ReproducibilityBundle",
    "build_agentbeats_submission",
    "verify_submission",
    "verify_submission_detailed",
]

VERSION = "2.0.0"

try:
    from .layered_reporter import EvidenceBundle  # noqa: F401
except ImportError:
    EvidenceBundle = None  # type: ignore[assignment,misc]


# =============================================================================
# Errors
# =============================================================================

class AgentBeatsError(ValueError):
    """Raised for invalid AgentBeats inputs or write failures."""


# =============================================================================
# Canonical serialisation
# =============================================================================

def _canonical_payload(
    data: Mapping[str, Any], *, strip_hash: bool
) -> str:
    """
    Return a canonical JSON string for hashing.

    Deterministic across key order (``sort_keys``), whitespace
    (compact separators), unicode (ASCII-only), and number formatting.
    ``strip_hash=True`` removes ``content_hash`` before serialising —
    the hash covers everything except itself.

    Raises
    ------
    TypeError, ValueError
        If any value is not JSON-serializable or is ``NaN`` / ``Inf``.
    """
    payload = dict(data)
    if strip_hash:
        payload.pop("content_hash", None)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash_payload(data: Mapping[str, Any]) -> str:
    """SHA-256 of ``data`` (excluding ``content_hash``)."""
    canonical = _canonical_payload(data, strip_hash=True)
    return "sha256:" + hashlib.sha256(
        canonical.encode("ascii")
    ).hexdigest()


# =============================================================================
# ReproducibilityBundle
# =============================================================================

@dataclass
class ReproducibilityBundle:
    """
    Captures everything a third party needs to reproduce a submission.

    ``compute_hash`` / ``to_dict`` / ``from_dict`` round-trip exactly:
    ``ReproducibilityBundle.from_dict(b.to_dict()).compute_hash()
    == b.compute_hash()``.
    """

    submission_id: str
    image: str
    queries: List[Any]
    system_version: str
    policy_version: Optional[str] = None
    dataset_id: Optional[str] = None
    region: Optional[str] = None
    precision: Optional[str] = None
    hardware: Optional[str] = None
    simulated: bool = False
    verification_summary: Optional[Dict[str, Any]] = None
    provenance: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    content_hash: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Explicit field list — no ``asdict`` ambiguity, stable shape."""
        return {
            "submission_id": self.submission_id,
            "image": self.image,
            "queries": list(self.queries),
            "system_version": self.system_version,
            "policy_version": self.policy_version,
            "dataset_id": self.dataset_id,
            "region": self.region,
            "precision": self.precision,
            "hardware": self.hardware,
            "simulated": bool(self.simulated),
            "verification_summary": self.verification_summary,
            "provenance": dict(self.provenance),
            "created_at": self.created_at.isoformat(),
            "content_hash": self.content_hash,
        }

    def compute_hash(self) -> str:
        """SHA-256 over :meth:`to_dict` minus ``content_hash``."""
        return _hash_payload(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReproducibilityBundle":
        if not isinstance(data, Mapping):
            raise AgentBeatsError(
                f"ReproducibilityBundle.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )

        ts = data.get("created_at")
        if isinstance(ts, str):
            try:
                created_at = datetime.fromisoformat(ts)
            except ValueError as exc:
                raise AgentBeatsError(
                    f"invalid created_at {ts!r}: {exc}"
                ) from exc
            if not created_at.tzinfo:
                created_at = created_at.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            created_at = ts
        else:
            created_at = datetime.now(timezone.utc)

        try:
            return cls(
                submission_id=str(data["submission_id"]),
                image=str(data["image"]),
                queries=list(data.get("queries") or []),
                system_version=str(data["system_version"]),
                policy_version=data.get("policy_version"),
                dataset_id=data.get("dataset_id"),
                region=data.get("region"),
                precision=data.get("precision"),
                hardware=data.get("hardware"),
                simulated=bool(data.get("simulated", False)),
                verification_summary=data.get("verification_summary"),
                provenance=dict(data.get("provenance") or {}),
                created_at=created_at,
                content_hash=data.get("content_hash"),
            )
        except KeyError as exc:
            raise AgentBeatsError(
                f"missing required field {exc.args[0]!r}."
            ) from exc

    @classmethod
    def build(
        cls,
        *,
        image: str,
        queries: List[Any],
        bundle: Optional[Any] = None,
        system_version: str = "5.0.0",
    ) -> "ReproducibilityBundle":
        """
        Construct a bundle (with ``content_hash`` computed) from
        optional ``EvidenceBundle``-shaped context.

        Extraction failures (``getattr`` on a broken property) surface
        to the caller rather than being swallowed — a caller who passes
        a bad bundle wants to know.
        """
        normalised_queries = _validate_queries(
            queries, strict=True, allow_raise=True,
        )
        rb = cls(
            submission_id=f"ab-{uuid.uuid4().hex[:16]}",
            image=str(image),
            queries=normalised_queries,
            system_version=str(system_version),
        )

        if bundle is not None:
            # Older EvidenceBundle exposed ``verification``; newer
            # versions expose ``verification_summary``. Prefer the
            # specific name, fall back to the alias.
            verification = getattr(
                bundle, "verification_summary", None
            )
            if verification is None:
                verification = getattr(bundle, "verification", None)

            rb.policy_version = getattr(
                bundle, "policy_version", None
            )
            rb.dataset_id = getattr(
                bundle, "dataset_or_workload_id", None
            )
            rb.region = getattr(bundle, "region", None)
            rb.precision = getattr(bundle, "precision", None)
            rb.hardware = getattr(bundle, "hardware", None)
            rb.simulated = bool(getattr(bundle, "simulated", False))
            rb.verification_summary = verification
            rb.provenance = {
                "run_id": getattr(bundle, "run_id", None),
                "content_hash": getattr(bundle, "content_hash", None),
            }

        rb.content_hash = rb.compute_hash()
        return rb


# =============================================================================
# Query validation
# =============================================================================

def _validate_queries(
    queries: Any,
    *,
    strict: bool,
    allow_raise: bool = False,
) -> List[Any]:
    """
    Normalise ``queries`` to a ``list`` of JSON-serializable items.

    In strict mode (or ``allow_raise=True``), non-list / non-serializable
    inputs raise :class:`AgentBeatsError`. In non-strict mode,
    non-serializable entries are dropped with a warning.
    """
    def _fail(msg: str) -> None:
        if strict or allow_raise:
            raise AgentBeatsError(msg)
        logger.warning("%s Dropping.", msg)

    if queries is None:
        return []
    if not isinstance(queries, (list, tuple)):
        msg = (
            f"queries must be a list or tuple, "
            f"got {type(queries).__name__}."
        )
        if strict or allow_raise:
            raise AgentBeatsError(msg)
        logger.warning("%s Treating as single-item.", msg)
        queries = [queries]

    out: List[Any] = []
    for i, q in enumerate(queries):
        try:
            json.dumps(q, allow_nan=False, ensure_ascii=True)
        except (TypeError, ValueError) as exc:
            _fail(f"queries[{i}] is not JSON-serializable: {exc}")
            continue
        out.append(q)
    return out


# =============================================================================
# build_agentbeats_submission
# =============================================================================

def build_agentbeats_submission(
    image: str,
    queries: List[Any],
    output_path: str = "agentbeats_submission.json",
    *,
    bundle: Optional[Any] = None,
    system_version: str = "5.0.0",
    strict: bool = True,
) -> Optional[str]:
    """
    Write an AgentBeats submission JSON.

    Backward-compatible: ``build_agentbeats_submission(image, queries)``
    writes ``{"image": ..., "queries": ...}`` unchanged.

    When ``bundle`` is provided, the file on disk is exactly
    ``ReproducibilityBundle.to_dict()`` — the same dict
    :meth:`ReproducibilityBundle.compute_hash` hashes and
    :func:`verify_submission` verifies.

    Parameters
    ----------
    image, queries :
        Same as the original signature.
    output_path : str
        Destination path. Parent directories are created on demand.
    bundle : optional
        An ``EvidenceBundle``-shaped object. Triggers the enhanced shape.
    system_version : str
        Recorded on the submission; does **not** flip the shape.
    strict : bool, default True
        If True, any failure raises :class:`AgentBeatsError`. If False,
        returns ``None`` and logs the reason.

    Returns
    -------
    Optional[str]
        The ``output_path`` on success, ``None`` on failure in
        non-strict mode.
    """
    if not isinstance(image, str) or not image:
        msg = f"image must be a non-empty string, got {image!r}."
        if strict:
            raise AgentBeatsError(msg)
        logger.error("%s", msg)
        return None

    # ---- Compose payload -----------------------------------------
    if bundle is not None:
        try:
            rb = ReproducibilityBundle.build(
                image=image,
                queries=queries,
                bundle=bundle,
                system_version=system_version,
            )
        except AgentBeatsError as exc:
            if strict:
                raise
            logger.error("bundle extraction failed: %s", exc)
            return None
        submission: Dict[str, Any] = rb.to_dict()
    else:
        normalised = _validate_queries(queries, strict=strict)
        submission = {
            "image": image,
            "queries": normalised,
        }
        if system_version != "5.0.0":
            # Additive: caller bumped version but didn't pass a bundle.
            submission["system_version"] = system_version

    # ---- Serialization guard -------------------------------------
    try:
        _canonical_payload(submission, strip_hash=False)
    except (TypeError, ValueError) as exc:
        msg = f"submission is not JSON-serializable: {exc}"
        logger.error("%s", msg)
        if strict:
            raise AgentBeatsError(msg) from exc
        return None

    # ---- Atomic write --------------------------------------------
    out = Path(output_path)
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=out.name + ".",
            suffix=".tmp",
            dir=str(out.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(
                    submission, f,
                    indent=2,
                    ensure_ascii=True,
                    allow_nan=False,
                )
            os.replace(tmp, out)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
    except (OSError, ValueError) as exc:
        msg = f"could not write submission to {output_path}: {exc}"
        logger.error("%s", msg)
        if strict:
            raise AgentBeatsError(msg) from exc
        return None

    logger.info("AgentBeats submission written to %s", output_path)

    # ---- Self-verification (paranoid, cheap) ---------------------
    if bundle is not None:
        check = verify_submission_detailed(output_path)
        if not check["ok"]:
            msg = (
                f"submission written to {output_path} failed "
                f"self-verification: {check['reason']}"
            )
            logger.error("%s", msg)
            if strict:
                raise AgentBeatsError(msg)
            return None

    return output_path


# =============================================================================
# Verification
# =============================================================================

def verify_submission_detailed(path: str) -> Dict[str, Any]:
    """
    Verify a submission's ``content_hash`` against its payload.

    Returns ``{ok, reason, expected, computed}``. Never raises;
    failures are reported in ``reason``.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "reason": f"unreadable: {exc}",
            "expected": None,
            "computed": None,
        }

    if not isinstance(data, dict):
        return {
            "ok": False,
            "reason": (
                f"root is not a mapping "
                f"({type(data).__name__})"
            ),
            "expected": None,
            "computed": None,
        }

    expected = data.get("content_hash")
    if not expected:
        return {
            "ok": False,
            "reason": "no content_hash field",
            "expected": None,
            "computed": None,
        }

    try:
        computed = _hash_payload(data)
    except (TypeError, ValueError) as exc:
        return {
            "ok": False,
            "reason": f"payload not hashable: {exc}",
            "expected": expected,
            "computed": None,
        }

    if computed == expected:
        return {
            "ok": True,
            "reason": "match",
            "expected": expected,
            "computed": computed,
        }
    return {
        "ok": False,
        "reason": "hash mismatch",
        "expected": expected,
        "computed": computed,
    }


def verify_submission(path: str) -> bool:
    """
    Return ``True`` iff the file's ``content_hash`` matches its
    recomputed payload.

    Minimal submissions (no ``content_hash``) return ``False`` — use
    :func:`verify_submission_detailed` to distinguish "no hash" from
    "hash mismatch".
    """
    return verify_submission_detailed(path)["ok"]


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("VERSION   :", VERSION)

    # ---- Original behaviour --------------------------------------
    print("\n=== Original behaviour (minimal submission) ===")
    minimal = build_agentbeats_submission(
        image="green-agent:v5",
        queries=[{"task": "summarize", "input": "text"}],
        output_path="/tmp/ab_minimal.json",
    )
    assert minimal is not None
    with open(minimal) as f:
        print(json.load(f))
    assert verify_submission(minimal) is False  # no content_hash
    print("verify minimal:", verify_submission_detailed(minimal))

    # ---- Enhanced with bundle -----------------------------------
    print("\n=== Enhanced with EvidenceBundle ===")
    try:
        from .layered_reporter import BundleBuilder
    except (ImportError, SystemError):
        try:
            from layered_reporter import BundleBuilder  # type: ignore
        except ImportError:
            print(
                "BundleBuilder unavailable; skipping enhanced demo. "
                "Run via `python -m r1.agentbeats_submission_builder`."
            )
            raise SystemExit(0)

    result = {
        "run_id": "run-001",
        "accuracy": 0.92,
        "energy_kwh": 0.045,
        "carbon_kg": 0.018,
        "latency_ms": 120.0,
        "policy_version": "v5.0.1",
        "region": "US-CA",
        "precision": "int8",
        "hardware": "A100-80GB",
    }
    bundle = BundleBuilder.build(result)

    full_path = build_agentbeats_submission(
        image="green-agent:v5",
        queries=[{"task": "summarize"}],
        output_path="/tmp/ab_full.json",
        bundle=bundle,
        system_version="5.0.0",
    )
    assert full_path is not None
    with open(full_path) as f:
        full = json.load(f)
    print(json.dumps(
        {
            k: full[k] for k in (
                "image", "system_version", "policy_version",
                "hardware", "content_hash", "provenance",
            )
        },
        indent=2,
    ))

    # ---- Self-verification round-trip ---------------------------
    assert verify_submission(full_path) is True
    check = verify_submission_detailed(full_path)
    print("\nverify full:", check["reason"])

    # ---- Tamper detection --------------------------------------
    tampered = json.loads(json.dumps(full))
    tampered["policy_version"] = "v5.9.9"
    with open("/tmp/ab_tampered.json", "w") as f:
        json.dump(tampered, f)
    assert verify_submission("/tmp/ab_tampered.json") is False
    print(
        "tampered  :", verify_submission_detailed(
            "/tmp/ab_tampered.json"
        )["reason"],
    )

    # ---- ReproducibilityBundle.from_dict round-trip ------------
    restored = ReproducibilityBundle.from_dict(full)
    assert restored.compute_hash() == full["content_hash"]
    print("from_dict round-trip: OK")

    # ---- Query validation --------------------------------------
    try:
        build_agentbeats_submission(
            image="x",
            queries={"a": 1},   # dict, not list
            output_path="/tmp/ab_bad.json",
            strict=True,
        )
    except AgentBeatsError as exc:
        print("Rejected queries:", exc)

    # Non-strict: dict is treated as single item.
    out = build_agentbeats_submission(
        image="x",
        queries={"a": 1},
        output_path="/tmp/ab_lenient.json",
        strict=False,
    )
    assert out is not None
    print("non-strict queries: OK")

    # ---- Non-serializable query → strict raises ----------------
    from decimal import Decimal
    try:
        build_agentbeats_submission(
            image="x",
            queries=[Decimal("1.5")],
            output_path="/tmp/ab_dec.json",
            strict=True,
        )
    except AgentBeatsError as exc:
        print("Rejected Decimal:", exc)

    # ---- Atomic write: deep parent path ------------------------
    nested = build_agentbeats_submission(
        image="x",
        queries=[{"q": 1}],
        output_path="/tmp/ab_nested/a/b/c/sub.json",
    )
    assert nested is not None and Path(nested).exists()
    print("nested write: OK")

    # ---- strict=False on unwritable path -----------------------
    unwritable = build_agentbeats_submission(
        image="x",
        queries=[],
        output_path="/proc/1/sub.json",   # unwritable
        strict=False,
    )
    assert unwritable is None
    print("unwritable : returned None")

    print("\nSmoke test passed.")
