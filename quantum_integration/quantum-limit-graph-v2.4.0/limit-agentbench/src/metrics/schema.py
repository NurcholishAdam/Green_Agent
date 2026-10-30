# metrics/episode_record.py

"""
Episode Record Factory (v5.1)
=============================

Builds a strongly-typed, validated record of a single episode's outcome:
accuracy, energy, carbon, reward, and the provenance of the energy / carbon
values (which instrument, meter, or estimator produced them).

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``metadata`` is now validated in ``__post_init__``. Previously any
         value was accepted, and a corrupt record crashed later at
         ``to_dict()`` with a raw ``TypeError`` — outside the
         ``EpisodeRecordError`` contract that every other method honours.
  * P0 — ``to_dict()`` builds the output field by field. It no longer goes
         through ``dataclasses.asdict``, which calls ``deepcopy`` on every
         field and fails on ``MappingProxyType`` and any custom mapping —
         even though ``_validate_provenance`` accepted them at construction
         time.
  * P0 — ``from_dict`` no longer leaks ``TypeError`` / ``ValueError`` for a
         ``timestamp`` that is ``None``, a string, or a ``datetime``. Every
         external value goes through a helper that raises
         ``EpisodeRecordError``.
  * P0 — ``EpisodeRecord`` defines an explicit ``__hash__`` over the scalar
         fields only (episode, accuracy, energy, carbon, reward, timestamp).
         The auto-generated hash raised ``TypeError`` on any populated
         provenance / metadata field.
  * P0 — Numeric fields are coerced to ``float`` in ``__post_init__`` via
         ``object.__setattr__``. Previously the return value of each
         ``_validate_*`` call was discarded, so
         ``EpisodeRecord(accuracy=1)`` retained an ``int``.
  * P1 — ``create_episode_record`` passes through without double-validating.
         Error messages now reference the class field names
         (``energy_joules`` / ``carbon_grams``) instead of the factory
         parameter names (``energy`` / ``carbon``).
  * P1 — ``from_dict`` evaluates ``time.time()`` only when the payload
         lacks a ``timestamp``. Previously it was evaluated eagerly as the
         default argument of ``dict.get``.
  * P1 — ``to_json`` falls back to ``default=str`` and wraps serialization
         errors in ``EpisodeRecordError``.
  * P1 — ``episode`` accepts ``uuid.UUID`` as documented. Serializes as its
         string form; a round-trip yields ``str``.
  * P1 — ``schema_version`` embedded in ``to_dict`` via a class-level
         ``SCHEMA_VERSION`` constant.
  * P2 — New helpers: ``key()``, ``with_updates()``, ``age_seconds()``,
         ``is_recent()``, ``merge_metadata()``, ``__version__``.
  * P3 — ``__repr__`` uses ``!r`` (no precision loss); ``math.isfinite``
         used consistently; ``logger.exception`` removed from the
         validation path (callers see the exception directly).

Backward compatibility
----------------------
- ``EpisodeRecord`` remains a frozen dataclass with the same field set.
- ``create_episode_record`` signature unchanged.
- ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json`` unchanged.
- **Breaking:** error messages from ``create_episode_record`` now reference
  ``energy_joules`` / ``carbon_grams`` instead of ``energy`` / ``carbon``.
- **Breaking:** ``hash(record)`` now succeeds on records with populated
  provenance or metadata. Two records that differ only in context hash
  identically (scalar identity).
"""

from __future__ import annotations

import json
import logging
import math
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import (
    Any, ClassVar, Dict, Mapping, Optional, Tuple, Union,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "EpisodeRecord",
    "EpisodeRecordError",
    "create_episode_record",
    "__version__",
]


#: Episode identifier type. UUID is serialized as its string representation.
EpisodeId = Union[int, str, uuid.UUID]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EpisodeRecordError(ValueError):
    """Raised when an episode record is constructed with invalid inputs."""


# --------------------------------------------------------------------------- #
# Validation / coercion helpers
# --------------------------------------------------------------------------- #
def _safe_float(name: str, value: Any) -> float:
    """Coerce ``value`` to a finite float, raising ``EpisodeRecordError``."""
    if isinstance(value, bool):
        raise EpisodeRecordError(
            f"{name} must be numeric, got bool {value!r}"
        )
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise EpisodeRecordError(
            f"{name} must be numeric, got {value!r} "
            f"({type(value).__name__})"
        ) from exc
    if not math.isfinite(v):
        raise EpisodeRecordError(
            f"{name} must be finite, got {value!r}"
        )
    return v


def _validate_finite(name: str, value: Any) -> float:
    return _safe_float(name, value)


def _validate_non_negative(name: str, value: Any) -> float:
    v = _safe_float(name, value)
    if v < 0:
        raise EpisodeRecordError(
            f"{name} must be non-negative, got {v!r}"
        )
    return v


def _validate_accuracy(value: Any) -> float:
    v = _safe_float("accuracy", value)
    if not 0.0 <= v <= 1.0:
        raise EpisodeRecordError(
            f"accuracy must be in [0, 1], got {v!r}"
        )
    return v


def _validate_episode(value: Any) -> None:
    if isinstance(value, bool):
        raise EpisodeRecordError(
            f"episode must not be bool, got {value!r}"
        )
    if not isinstance(value, (int, str, uuid.UUID)):
        raise EpisodeRecordError(
            f"episode must be int, str, or UUID, "
            f"got {type(value).__name__}"
        )
    if isinstance(value, str) and not value:
        raise EpisodeRecordError("episode must be a non-empty string.")


def _validate_provenance(name: str, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise EpisodeRecordError(
            f"{name} must be a Mapping or None, "
            f"got {type(value).__name__}"
        )


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EpisodeRecord:
    """
    Immutable, validated record of a single episode.

    Attributes
    ----------
    episode : int | str | uuid.UUID
        Episode identifier. Any int, non-empty string, or ``UUID``.
        UUIDs are serialized as their string representation (round-trip
        yields a ``str``).
    accuracy : float
        Task accuracy, in ``[0, 1]``.
    energy_joules : float
        Energy consumed during the episode, in joules (non-negative, finite).
    carbon_grams : float
        Carbon emitted during the episode, in grams CO2e (non-negative, finite).
    reward : float
        Reward signal for the episode (finite; may be negative).
    energy_provenance : Mapping | None
        Structured description of how energy was measured.
    carbon_provenance : Mapping | None
        Structured description of how carbon was measured.
    timestamp : float
        Epoch seconds when the record was created (auto-populated if not
        supplied).
    metadata : Mapping | None
        Optional free-form extras (tags, run_id, host, etc.).

    Notes
    -----
    ``EpisodeRecord`` is hashable via its **scalar fields only** —
    provenance and metadata are context, not identity. Two records that
    differ only in provenance or metadata compare unequal but hash
    identically.
    """

    #: Serialization schema version emitted by ``to_dict``.
    SCHEMA_VERSION: ClassVar[str] = __version__

    episode: EpisodeId
    accuracy: float
    energy_joules: float
    carbon_grams: float
    reward: float
    energy_provenance: Optional[Mapping[str, Any]] = None
    carbon_provenance: Optional[Mapping[str, Any]] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Optional[Mapping[str, Any]] = None

    # --------------------------------------------------------------- dunders
    def __post_init__(self) -> None:
        _validate_episode(self.episode)
        # Coerce numeric fields to float via object.__setattr__ so
        # isinstance(rec.accuracy, float) holds regardless of input type.
        object.__setattr__(
            self, "accuracy", _validate_accuracy(self.accuracy),
        )
        object.__setattr__(
            self, "energy_joules",
            _validate_non_negative("energy_joules", self.energy_joules),
        )
        object.__setattr__(
            self, "carbon_grams",
            _validate_non_negative("carbon_grams", self.carbon_grams),
        )
        object.__setattr__(
            self, "reward", _validate_finite("reward", self.reward),
        )
        object.__setattr__(
            self, "timestamp",
            _validate_finite("timestamp", self.timestamp),
        )
        _validate_provenance("energy_provenance", self.energy_provenance)
        _validate_provenance("carbon_provenance", self.carbon_provenance)
        _validate_provenance("metadata", self.metadata)

    def __hash__(self) -> int:
        # Scalars only — provenance and metadata are context, not identity.
        return hash((
            self.episode,
            self.accuracy,
            self.energy_joules,
            self.carbon_grams,
            self.reward,
            self.timestamp,
        ))

    def __repr__(self) -> str:
        return (
            "EpisodeRecord("
            f"episode={self.episode!r}, "
            f"accuracy={self.accuracy!r}, "
            f"energy_joules={self.energy_joules!r}, "
            f"carbon_grams={self.carbon_grams!r}, "
            f"reward={self.reward!r})"
        )

    # --------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "episode": (
                str(self.episode)
                if isinstance(self.episode, uuid.UUID)
                else self.episode
            ),
            "accuracy": float(self.accuracy),
            "energy_joules": float(self.energy_joules),
            "carbon_grams": float(self.carbon_grams),
            "reward": float(self.reward),
            "energy_provenance": (
                dict(self.energy_provenance)
                if self.energy_provenance is not None else None
            ),
            "carbon_provenance": (
                dict(self.carbon_provenance)
                if self.carbon_provenance is not None else None
            ),
            "timestamp": float(self.timestamp),
            "metadata": (
                dict(self.metadata)
                if self.metadata is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EpisodeRecord":
        if not isinstance(data, Mapping):
            raise EpisodeRecordError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )

        required = (
            "episode", "accuracy", "energy_joules",
            "carbon_grams", "reward",
        )
        missing = [k for k in required if k not in data]
        if missing:
            raise EpisodeRecordError(
                f"Missing required field(s): {missing}"
            )

        timestamp_raw = data.get("timestamp")
        timestamp = (
            time.time()
            if timestamp_raw is None
            else _safe_float("timestamp", timestamp_raw)
        )

        return cls(
            episode=data["episode"],
            accuracy=data["accuracy"],
            energy_joules=data["energy_joules"],
            carbon_grams=data["carbon_grams"],
            reward=data["reward"],
            energy_provenance=data.get("energy_provenance"),
            carbon_provenance=data.get("carbon_provenance"),
            timestamp=timestamp,
            metadata=data.get("metadata"),
        )

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("default", str)
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise EpisodeRecordError(
                f"Record is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "EpisodeRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EpisodeRecordError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise EpisodeRecordError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------ identity / helpers
    def key(self) -> Tuple[Any, ...]:
        """
        Deterministic identity tuple (excludes timestamp and context).

        Use this to deduplicate across runs where provenance or metadata
        may differ but the measurements are identical.
        """
        return (
            self.episode,
            self.accuracy,
            self.energy_joules,
            self.carbon_grams,
            self.reward,
        )

    def with_updates(self, **kwargs: Any) -> "EpisodeRecord":
        """
        Return a new record with some fields replaced.

        Replacement goes through ``__init__``, so validation applies and
        the returned record is guaranteed consistent.
        """
        return replace(self, **kwargs)

    def age_seconds(self, now: Optional[float] = None) -> float:
        """Seconds elapsed since ``timestamp`` (relative to ``now``)."""
        reference = time.time() if now is None else float(now)
        return reference - self.timestamp

    def is_recent(
        self, max_age_seconds: float, now: Optional[float] = None,
    ) -> bool:
        """True if the record is no older than ``max_age_seconds``."""
        if max_age_seconds < 0:
            raise EpisodeRecordError(
                "max_age_seconds must be >= 0."
            )
        return self.age_seconds(now) <= max_age_seconds

    def merge_metadata(
        self, extra: Mapping[str, Any],
    ) -> "EpisodeRecord":
        """
        Return a new record whose ``metadata`` merges ``self.metadata``
        and ``extra`` (``extra`` wins on collision).
        """
        if not isinstance(extra, Mapping):
            raise EpisodeRecordError(
                f"extra must be a Mapping, "
                f"got {type(extra).__name__}"
            )
        merged = dict(self.metadata or {})
        merged.update(extra)
        return replace(self, metadata=merged)


# --------------------------------------------------------------------------- #
# Public factory (backward-compatible)
# --------------------------------------------------------------------------- #
def create_episode_record(
    episode: EpisodeId,
    accuracy: float,
    energy: float,
    carbon: float,
    reward: float,
    provenance_energy: Optional[Mapping[str, Any]] = None,
    provenance_carbon: Optional[Mapping[str, Any]] = None,
    *,
    as_dict: bool = True,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Union[Dict[str, Any], EpisodeRecord]:
    """
    Build a validated episode record.

    Parameters
    ----------
    episode : int | str | uuid.UUID
        Episode identifier.
    accuracy : float
        Task accuracy in ``[0, 1]``.
    energy : float
        Energy in joules (>= 0).
    carbon : float
        Carbon in grams CO2e (>= 0).
    reward : float
        Reward signal (may be negative, must be finite).
    provenance_energy, provenance_carbon : Mapping | None
        Structured provenance info
        (e.g. ``{"source": "nvml", "host": "gpu-01"}``).
    as_dict : bool, default True
        If True, return a plain ``dict`` (matches the original API).
        If False, return the underlying :class:`EpisodeRecord` dataclass.
    metadata : Mapping | None
        Optional free-form extras stored alongside the record.

    Returns
    -------
    dict | EpisodeRecord
        The episode record, either as a plain dictionary (default) or as
        the richer ``EpisodeRecord`` instance.

    Raises
    ------
    EpisodeRecordError
        If any field fails validation. Error messages reference the
        class field names (``energy_joules`` / ``carbon_grams``), not
        the factory parameter names.
    """
    # Pass-through: EpisodeRecord.__post_init__ performs all validation.
    # Avoids double validation and keeps error messages consistent.
    record = EpisodeRecord(
        episode=episode,
        accuracy=accuracy,
        energy_joules=energy,
        carbon_grams=carbon,
        reward=reward,
        energy_provenance=provenance_energy,
        carbon_provenance=provenance_carbon,
        metadata=metadata,
    )
    logger.debug(
        "Created episode record: episode=%r accuracy=%.3f "
        "energy=%.6gJ carbon=%.6g g",
        record.episode,
        record.accuracy,
        record.energy_joules,
        record.carbon_grams,
    )
    return record.to_dict() if as_dict else record


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metrics.episode_record
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # --- backward-compatible call (returns dict) ---
    d = create_episode_record(
        episode=1,
        accuracy=0.92,
        energy=0.5,
        carbon=0.12,
        reward=1.0,
        provenance_energy={"source": "nvml", "host": "gpu-01"},
        provenance_carbon={"source": "grid-mix", "region": "eu-west-1"},
    )
    print("dict record:", d)

    # --- dataclass form ---
    rec = create_episode_record(
        episode="ep-002",
        accuracy=0.75,
        energy=1.2,
        carbon=0.3,
        reward=0.4,
        as_dict=False,
    )
    print("dataclass   :", rec)
    print("json        :", rec.to_json())

    # --- round-trip ---
    assert EpisodeRecord.from_json(rec.to_json()).to_dict() == rec.to_dict()
    print("Serialization round-trip OK.")

    # --- P0 #1 regression: metadata must be a Mapping or None ---
    for bad_metadata in ("string", [1, 2, 3], 42):
        try:
            EpisodeRecord(
                episode=1, accuracy=0.9, energy_joules=1.0,
                carbon_grams=0.1, reward=0.5,
                metadata=bad_metadata,
            )
        except EpisodeRecordError:
            pass
        else:
            raise AssertionError(
                f"metadata={bad_metadata!r} should have been rejected."
            )
    print("metadata validation: OK")

    # --- P0 #2 regression: MappingProxyType round-trips through to_dict ---
    from types import MappingProxyType
    rec_proxy = EpisodeRecord(
        episode=1, accuracy=0.9, energy_joules=1.0,
        carbon_grams=0.1, reward=0.5,
        energy_provenance=MappingProxyType({"source": "nvml"}),
        metadata=MappingProxyType({"tag": "test"}),
    )
    d = rec_proxy.to_dict()
    assert d["energy_provenance"] == {"source": "nvml"}
    assert d["metadata"] == {"tag": "test"}
    print("MappingProxyType to_dict: OK")

    # --- P0 #3 regression: from_dict rejects bad timestamps ---
    for bad_ts in ("2024-01-01", object()):
        try:
            EpisodeRecord.from_dict({
                "episode": 1, "accuracy": 0.9,
                "energy_joules": 1.0, "carbon_grams": 0.1,
                "reward": 0.5, "timestamp": bad_ts,
            })
        except EpisodeRecordError:
            pass
        else:
            raise AssertionError(
                f"timestamp={bad_ts!r} should have been rejected."
            )
    # None means "use default"
    restored_none_ts = EpisodeRecord.from_dict({
        "episode": 1, "accuracy": 0.9,
        "energy_joules": 1.0, "carbon_grams": 0.1,
        "reward": 0.5, "timestamp": None,
    })
    assert restored_none_ts.timestamp > 0
    print("from_dict timestamp validation: OK")

    # --- P0 #4 regression: hash works with populated context ---
    fixed_ts = 1234567890.0
    rec_no_meta = EpisodeRecord(
        episode=1, accuracy=0.9, energy_joules=1.0,
        carbon_grams=0.1, reward=0.5, timestamp=fixed_ts,
    )
    rec_with_meta = EpisodeRecord(
        episode=1, accuracy=0.9, energy_joules=1.0,
        carbon_grams=0.1, reward=0.5, timestamp=fixed_ts,
        metadata={"different": "tag"},
    )
    assert hash(rec_no_meta) == hash(rec_with_meta)   # scalar identity
    assert rec_no_meta != rec_with_meta               # eq considers all fields
    print("hash() on records with mappings: OK")

    # --- P0 #5 regression: field coercion ---
    rec_int = EpisodeRecord(
        episode=1, accuracy=1, energy_joules=1,
        carbon_grams=0, reward=0,
    )
    for field_name in (
        "accuracy", "energy_joules", "carbon_grams", "reward", "timestamp",
    ):
        assert isinstance(getattr(rec_int, field_name), float), (
            f"{field_name} was not coerced to float"
        )
    print("field coercion: OK")

    # --- P1 #9 regression: UUID episode support ---
    uid = uuid.uuid4()
    rec_uuid = EpisodeRecord(
        episode=uid, accuracy=0.9, energy_joules=1.0,
        carbon_grams=0.1, reward=0.5,
    )
    serialized = rec_uuid.to_dict()
    assert serialized["episode"] == str(uid)
    restored_uuid = EpisodeRecord.from_dict(serialized)
    assert restored_uuid.episode == str(uid)   # round-trip yields str
    print("UUID episode: OK")

    # --- P1 #6 regression: factory field-name consistency in errors ---
    try:
        create_episode_record(
            episode=1, accuracy=0.9, energy=-1.0,
            carbon=0.1, reward=0.5,
        )
    except EpisodeRecordError as exc:
        assert "energy_joules" in str(exc), (
            f"error should reference energy_joules, got: {exc}"
        )
    else:
        raise AssertionError("negative energy should have been rejected.")
    print("factory error message: OK")

    # --- P1 #7 regression: from_dict uses supplied timestamp ---
    payload_with_ts = {
        "episode": 1, "accuracy": 0.9, "energy_joules": 1.0,
        "carbon_grams": 0.1, "reward": 0.5, "timestamp": 1234567890.0,
    }
    restored = EpisodeRecord.from_dict(payload_with_ts)
    assert restored.timestamp == 1234567890.0
    print("from_dict uses supplied timestamp: OK")

    # --- P1 #8 regression: to_json stringifies non-JSON-safe metadata ---
    from datetime import datetime
    rec_dt = EpisodeRecord(
        episode=1, accuracy=0.9, energy_joules=1.0,
        carbon_grams=0.1, reward=0.5,
        metadata={"when": datetime(2024, 1, 1, 12, 0, 0)},
    )
    json_str = rec_dt.to_json()
    assert "2024-01-01" in json_str
    print("non-JSON-safe metadata stringified: OK")

    # --- P2 regression: helpers ---
    now = time.time()
    rec_recent = EpisodeRecord(
        episode=1, accuracy=0.9, energy_joules=1.0,
        carbon_grams=0.1, reward=0.5, timestamp=now,
    )
    assert rec_recent.is_recent(60.0)
    assert rec_recent.age_seconds() < 1.0
    rec_merged = rec_recent.merge_metadata({"tag": "new"})
    assert rec_merged.metadata == {"tag": "new"}
    assert rec_recent.key() == (
        1, 0.9, 1.0, 0.1, 0.5,
    )
    rec_updated = rec_recent.with_updates(accuracy=0.95)
    assert rec_updated.accuracy == 0.95
    assert rec_updated.episode == rec_recent.episode
    assert rec_updated.timestamp == rec_recent.timestamp
    print("helpers (key/age/is_recent/merge_metadata/with_updates): OK")

    # --- unchanged validation failures ---
    for bad_kwargs in (
        dict(episode=1, accuracy=1.5, energy=1.0, carbon=0.1, reward=0.0),
        dict(episode=1, accuracy=0.5, energy=-1.0, carbon=0.1, reward=0.0),
        dict(episode=1, accuracy=0.5, energy=1.0, carbon=0.1,
             reward=float("nan")),
        dict(episode="", accuracy=0.5, energy=1.0, carbon=0.1, reward=0.0),
        dict(episode=True, accuracy=0.5, energy=1.0, carbon=0.1, reward=0.0),
    ):
        try:
            create_episode_record(**bad_kwargs)
        except EpisodeRecordError as exc:
            print("Rejected as expected:", exc)
        else:
            raise AssertionError(
                f"Expected rejection for {bad_kwargs!r}"
            )

    print("\nSmoke test passed.")
