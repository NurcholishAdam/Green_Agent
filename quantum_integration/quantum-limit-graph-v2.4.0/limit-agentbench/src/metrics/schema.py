# metrics/episode_record.py

"""
Episode Record Factory

Builds a strongly-typed, validated record of a single episode's outcome:
accuracy, energy, carbon, reward, and the provenance of the energy / carbon
values (which instrument, meter, or estimator produced them).

Enhancements
------------
- Frozen ``EpisodeRecord`` dataclass (immutable, hashable, typed).
- ``create_episode_record(...)`` kept as a backward-compatible factory.
- Validation: finite non-negative numerics; provenance must be a mapping or None.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Custom :class:`EpisodeRecordError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Mapping, Optional, Union

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EpisodeRecordError(ValueError):
    """Raised when an episode record is constructed with invalid inputs."""


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EpisodeRecord:
    """
    Immutable, validated record of a single episode.

    Attributes
    ----------
    episode : str | int
        Episode identifier. Any hashable scalar (int, str, UUID, etc.).
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
        Epoch seconds when the record was created (auto-populated).
    metadata : Mapping | None
        Optional free-form extras (tags, run_id, host, etc.).
    """

    episode: Union[int, str]
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
        # Defensive re-validation in case the dataclass is instantiated directly.
        _validate_episode(self.episode)
        _validate_accuracy(self.accuracy)
        _validate_non_negative("energy_joules", self.energy_joules)
        _validate_non_negative("carbon_grams", self.carbon_grams)
        _validate_finite("reward", self.reward)
        _validate_provenance("energy_provenance", self.energy_provenance)
        _validate_provenance("carbon_provenance", self.carbon_provenance)
        _validate_finite("timestamp", self.timestamp)

    def __repr__(self) -> str:
        return (
            "EpisodeRecord("
            f"episode={self.episode!r}, "
            f"accuracy={self.accuracy:.3f}, "
            f"energy_joules={self.energy_joules:.6g}, "
            f"carbon_grams={self.carbon_grams:.6g}, "
            f"reward={self.reward:.6g})"
        )

    # --------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # ``asdict`` deep-copies mappings into plain dicts already; normalize
        # the optional Mapping fields to None when absent.
        d["energy_provenance"] = (
            dict(self.energy_provenance) if self.energy_provenance is not None else None
        )
        d["carbon_provenance"] = (
            dict(self.carbon_provenance) if self.carbon_provenance is not None else None
        )
        d["metadata"] = dict(self.metadata) if self.metadata is not None else None
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EpisodeRecord":
        if not isinstance(data, Mapping):
            raise EpisodeRecordError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            return cls(
                episode=data["episode"],
                accuracy=data["accuracy"],
                energy_joules=data["energy_joules"],
                carbon_grams=data["carbon_grams"],
                reward=data["reward"],
                energy_provenance=data.get("energy_provenance"),
                carbon_provenance=data.get("carbon_provenance"),
                timestamp=float(data.get("timestamp", time.time())),
                metadata=data.get("metadata"),
            )
        except KeyError as exc:
            raise EpisodeRecordError(f"Missing required field: {exc.args[0]!r}") from exc

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "EpisodeRecord":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise EpisodeRecordError(f"Invalid JSON payload: {exc}") from exc


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _validate_finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EpisodeRecordError(f"{name} must be a real number, got {type(value).__name__}.")
    v = float(value)
    if math.isnan(v) or math.isinf(v):
        raise EpisodeRecordError(f"{name} must be finite, got {value!r}.")
    return v


def _validate_non_negative(name: str, value: Any) -> float:
    v = _validate_finite(name, value)
    if v < 0:
        raise EpisodeRecordError(f"{name} must be non-negative, got {v!r}.")
    return v


def _validate_accuracy(value: Any) -> float:
    v = _validate_finite("accuracy", value)
    if not 0.0 <= v <= 1.0:
        raise EpisodeRecordError(f"accuracy must be in [0, 1], got {v!r}.")
    return v


def _validate_episode(value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise EpisodeRecordError(
            f"episode must be an int or str, got {type(value).__name__}."
        )
    if isinstance(value, str) and not value:
        raise EpisodeRecordError("episode must be a non-empty string.")


def _validate_provenance(name: str, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise EpisodeRecordError(
            f"{name} must be a Mapping or None, got {type(value).__name__}."
        )


# --------------------------------------------------------------------------- #
# Public factory (backward-compatible)
# --------------------------------------------------------------------------- #
def create_episode_record(
    episode: Union[int, str],
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
    episode : int | str
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
        Structured provenance info (e.g. ``{"source": "nvml", "host": "gpu-01"}``).
    as_dict : bool, default True
        If True, return a plain ``dict`` (matches the original API).
        If False, return the underlying :class:`EpisodeRecord` dataclass.
    metadata : Mapping | None
        Optional free-form extras stored alongside the record.

    Returns
    -------
    dict | EpisodeRecord
        The episode record, either as a plain dictionary (default) or as the
        richer ``EpisodeRecord`` instance.

    Raises
    ------
    EpisodeRecordError
        If any field fails validation.
    """
    try:
        record = EpisodeRecord(
            episode=episode,
            accuracy=_validate_accuracy(accuracy),
            energy_joules=_validate_non_negative("energy", energy),
            carbon_grams=_validate_non_negative("carbon", carbon),
            reward=_validate_finite("reward", reward),
            energy_provenance=provenance_energy,
            carbon_provenance=provenance_carbon,
            metadata=metadata,
        )
    except EpisodeRecordError:
        logger.exception("Failed to create episode record for episode=%r", episode)
        raise

    logger.debug(
        "Created episode record: episode=%r accuracy=%.3f energy=%.6gJ carbon=%.6g g",
        record.episode,
        record.accuracy,
        record.energy_joules,
        record.carbon_grams,
    )

    return record.to_dict() if as_dict else record


# --------------------------------------------------------------------------- #
# Convenience re-exports
# --------------------------------------------------------------------------- #
__all__ = [
    "EpisodeRecord",
    "EpisodeRecordError",
    "create_episode_record",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metrics.episode_record
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s"
    )

    # Backward-compatible call (returns dict)
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

    # Dataclass form
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

    # Round-trip
    assert EpisodeRecord.from_json(rec.to_json()).to_dict() == rec.to_dict()
    print("Serialization round-trip OK.")

    # Validation failure path
    for bad in (
        dict(episode=1, accuracy=1.5, energy=1.0, carbon=0.1, reward=0.0),
        dict(episode=1, accuracy=0.5, energy=-1.0, carbon=0.1, reward=0.0),
        dict(episode=1, accuracy=0.5, energy=1.0, carbon=0.1, reward=float("nan")),
        dict(episode="", accuracy=0.5, energy=1.0, carbon=0.1, reward=0.0),
    ):
        try:
            create_episode_record(**bad)
        except EpisodeRecordError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection for {bad!r}")
