# community_data.py
"""
Community-driven data sharing for Green Agent (v2.0.0)
=======================================================

Users can *opt in* to contribute anonymized observations that improve
downstream simulations:

  - Grid carbon observations (kg CO2/kWh)
  - Helium price observations (USD per litre)
  - Recovery efficiency data (0..1)

Design principles
-----------------
- **Opt-in only.**  Nothing is written unless ``GREEN_AGENT_COMMUNITY_OPT_IN=1``
  is set in the environment (or ``opt_in()`` is called explicitly).
- **Privacy first.**  Contributors are stored as a salted hash, not raw
  identifiers.  Regions and sources are validated against a fixed vocabulary.
- **Append-only, atomic, bounded.**  Writes use ``os.open(..., O_APPEND)``
  with a single ``os.write`` call.  Files rotate once they exceed
  ``MAX_FILE_BYTES``.  Retention pruning is available on demand.
- **Corruption tolerant.**  Malformed JSONL lines are skipped with a
  warning; they never abort a query.
- **Local first.**  Everything lives under a user-configurable directory
  (``GREEN_AGENT_DATA_DIR``), defaulting to ``~/.green_agent``.

CHANGES (v2.0.0 over the previous snippet):
- Fixed missing ``Optional`` import and the string-vs-float timestamp bug.
- Numeric ``ts`` field for cheap window filtering; ISO string kept for humans.
- Input validation for ``region``, ``intensity``, ``source``, ``days``.
- Opt-in gate via env var or explicit ``opt_in()`` call.
- Atomic append via ``os.open`` + ``os.write``.
- Bounded file size with rotation (``.1``, ``.2``, ...) and pruning.
- Corrupt-line tolerance with structured logging.
- Configurable data directory (env var + classmethod).
- Salted contributor hash instead of a bare ``'anonymous': True`` flag.
- Privacy controls: ``forget_me()`` and ``export_all()``.
- Region normalization to lowercase-slug.
- Schema version field (``"v": 1``) for forward compatibility.
- UTC-aware timestamps everywhere.
- Built-in test suite (``python community_data.py``).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import secrets
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "2.0.0"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION: int = 1
OBSERVATIONS_FILE: str = "carbon_observations.jsonl"
SALT_FILE: str = "contributor_salt"
OPT_IN_FLAG_FILE: str = "opt_in"

MAX_FILE_BYTES: int = 50 * 1024 * 1024    # 50 MB per file before rotation
MAX_ROTATED_FILES: int = 5               # keep up to 5 rotated files
MAX_LINE_BYTES: int = 1024               # reject oversized observations

MIN_INTENSITY: float = 0.0
MAX_INTENSITY: float = 1000.0
MIN_DAYS: int = 1
MAX_DAYS: int = 3650

_REGION_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,63}$")
_SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,31}$")

# Fixed vocabularies.  Add entries here as needed; unknown values are rejected.
KNOWN_REGIONS: Optional[frozenset] = None
"""If set, only these region slugs are accepted.  ``None`` = any valid slug."""

KNOWN_SOURCES: frozenset = frozenset({
    "api", "invoice", "sensor", "manual", "import", "derived",
})


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class CarbonObservation:
    """A single anonymized carbon-intensity observation."""
    v: int
    ts: float                 # POSIX UTC seconds, for cheap filtering
    timestamp: str            # ISO 8601 UTC, for humans
    region: str               # normalized slug
    intensity: float          # kg CO2 / kWh
    source: str               # validated slug
    contributor_hash: str     # salted SHA-256 of the caller identifier

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, line: str) -> "CarbonObservation":
        data = json.loads(line)
        if not isinstance(data, dict):
            raise ValueError("observation must be a JSON object")
        # Tolerate missing ts by parsing the ISO string.
        ts = data.get("ts")
        if ts is None and "timestamp" in data:
            ts = datetime.fromisoformat(data["timestamp"]).timestamp()
        return cls(
            v=int(data.get("v", SCHEMA_VERSION)),
            ts=float(ts) if ts is not None else 0.0,
            timestamp=str(data.get("timestamp", "")),
            region=str(data.get("region", "")).lower(),
            intensity=float(data.get("intensity", 0.0)),
            source=str(data.get("source", "unknown")),
            contributor_hash=str(data.get("contributor_hash", "")),
        )


@dataclass
class CommunityAverage:
    """Result of a community-average query."""
    region: str
    mean: float
    count: int
    window_days: int
    min_ts: Optional[float] = None
    max_ts: Optional[float] = None


# ---------------------------------------------------------------------------
# CommunityDataHub
# ---------------------------------------------------------------------------

class CommunityDataHub:
    """
    Optional, opt-in community data sharing.

    Usage::

        CommunityDataHub.opt_in()                         # persist opt-in
        CommunityDataHub.contribute_carbon_observation(
            region="us-east", intensity=0.42, source="api",
            contributor="user-42",
        )
        avg = CommunityDataHub.get_community_average("us-east", days=30)

    All writes are no-ops unless the user has opted in.
    """

    # ------------------------------------------------------------------
    # Directory + opt-in
    # ------------------------------------------------------------------
    @classmethod
    def data_dir(cls) -> Path:
        """
        Return the base data directory.  Honours ``GREEN_AGENT_DATA_DIR``.
        """
        root = os.environ.get("GREEN_AGENT_DATA_DIR")
        base = Path(root) if root else Path.home() / ".green_agent"
        return base / "community_data"

    @classmethod
    def _ensure_dir(cls) -> Path:
        d = cls.data_dir()
        d.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(d, 0o700)
        except OSError:
            pass
        return d

    @classmethod
    def is_opted_in(cls) -> bool:
        """True if the user has opted in via env var or the flag file."""
        if os.environ.get("GREEN_AGENT_COMMUNITY_OPT_IN", "0") == "1":
            return True
        flag = cls.data_dir() / OPT_IN_FLAG_FILE
        try:
            return flag.exists()
        except OSError:
            return False

    @classmethod
    def opt_in(cls) -> None:
        """Persist the opt-in flag to disk (idempotent)."""
        cls._ensure_dir()
        flag = cls.data_dir() / OPT_IN_FLAG_FILE
        try:
            flag.write_text(
                f"opted-in-at={datetime.now(timezone.utc).isoformat()}\n",
                encoding="utf-8",
            )
        except OSError as e:
            logger.error(f"Failed to write opt-in flag: {e}")

    @classmethod
    def opt_out(cls) -> None:
        """Remove the opt-in flag.  Existing observations are kept."""
        flag = cls.data_dir() / OPT_IN_FLAG_FILE
        try:
            flag.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(f"Failed to remove opt-in flag: {e}")

    # ------------------------------------------------------------------
    # Contributor salt (for pseudonymization)
    # ------------------------------------------------------------------
    @classmethod
    def _contributor_salt(cls) -> bytes:
        d = cls._ensure_dir()
        salt_path = d / SALT_FILE
        try:
            if salt_path.exists():
                return salt_path.read_bytes()
        except OSError as e:
            logger.warning(f"Failed to read contributor salt: {e}")

        salt = secrets.token_bytes(32)
        try:
            # Write atomically so concurrent first-callers don't race.
            fd = os.open(salt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.write(fd, salt)
            finally:
                os.close(fd)
        except FileExistsError:
            try:
                return salt_path.read_bytes()
            except OSError:
                pass
        except OSError as e:
            logger.warning(f"Failed to write contributor salt: {e}")
        return salt

    @classmethod
    def _hash_contributor(cls, contributor: Optional[str]) -> str:
        if not contributor:
            contributor = "anonymous"
        salt = cls._contributor_salt()
        h = hashlib.sha256(salt)
        h.update(contributor.encode("utf-8"))
        return h.hexdigest()[:32]

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    @classmethod
    def _validate_region(cls, region: str) -> str:
        if not isinstance(region, str):
            raise ValueError("region must be a string")
        slug = region.strip().lower()
        if not _REGION_RE.match(slug):
            raise ValueError(
                "region must be a lowercase slug matching [a-z0-9][a-z0-9_-]{0,63}"
            )
        if KNOWN_REGIONS is not None and slug not in KNOWN_REGIONS:
            raise ValueError(f"region {slug!r} is not in the known set")
        return slug

    @classmethod
    def _validate_source(cls, source: str) -> str:
        if not isinstance(source, str):
            raise ValueError("source must be a string")
        slug = source.strip().lower()
        if not _SOURCE_RE.match(slug):
            raise ValueError(
                "source must be a lowercase slug matching [a-z0-9][a-z0-9_-]{0,31}"
            )
        if KNOWN_SOURCES and slug not in KNOWN_SOURCES:
            raise ValueError(f"source {slug!r} is not in {sorted(KNOWN_SOURCES)}")
        return slug

    @classmethod
    def _validate_intensity(cls, intensity: float) -> float:
        if isinstance(intensity, bool):
            raise ValueError("intensity must be a finite number, not a bool")
        try:
            f = float(intensity)
        except (TypeError, ValueError):
            raise ValueError("intensity must be a number")
        if math.isnan(f) or math.isinf(f):
            raise ValueError("intensity must be finite")
        if not (MIN_INTENSITY <= f <= MAX_INTENSITY):
            raise ValueError(
                f"intensity must be in [{MIN_INTENSITY}, {MAX_INTENSITY}]"
            )
        return f

    @classmethod
    def _validate_days(cls, days: int) -> int:
        if isinstance(days, bool) or not isinstance(days, int):
            raise ValueError("days must be an integer")
        if not (MIN_DAYS <= days <= MAX_DAYS):
            raise ValueError(f"days must be in [{MIN_DAYS}, {MAX_DAYS}]")
        return days

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------
    @classmethod
    def contribute_carbon_observation(
        cls,
        region: str,
        intensity: float,
        source: str,
        contributor: Optional[str] = None,
    ) -> bool:
        """
        Append an anonymized carbon-intensity observation.

        Returns ``True`` if the observation was written, ``False`` if the
        user has not opted in or the write failed.  Raises ``ValueError`` on
        invalid inputs.
        """
        if not cls.is_opted_in():
            logger.info("Community data opt-in disabled; skipping contribution.")
            return False

        region_slug = cls._validate_region(region)
        source_slug = cls._validate_source(source)
        intensity_val = cls._validate_intensity(intensity)

        now = datetime.now(timezone.utc)
        obs = CarbonObservation(
            v=SCHEMA_VERSION,
            ts=now.timestamp(),
            timestamp=now.isoformat(),
            region=region_slug,
            intensity=intensity_val,
            source=source_slug,
            contributor_hash=cls._hash_contributor(contributor),
        )
        line = (obs.to_json() + "\n").encode("utf-8")
        if len(line) > MAX_LINE_BYTES:
            logger.warning("Refusing to write oversized observation line.")
            return False

        return cls._atomic_append(line)

    @classmethod
    def _atomic_append(cls, line: bytes) -> bool:
        d = cls._ensure_dir()
        path = d / OBSERVATIONS_FILE
        cls._rotate_if_needed(path)

        try:
            fd = os.open(path, os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
        except OSError as e:
            logger.error(f"Failed to open observations file: {e}")
            return False
        try:
            # A single os.write of a small buffer is atomic on POSIX.
            os.write(fd, line)
        except OSError as e:
            logger.error(f"Failed to write observation: {e}")
            return False
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
        return True

    @classmethod
    def _rotate_if_needed(cls, path: Path) -> None:
        try:
            size = path.stat().st_size if path.exists() else 0
        except OSError:
            return
        if size < MAX_FILE_BYTES:
            return
        try:
            # Shift .N -> .N+1, oldest dropped.
            for i in range(MAX_ROTATED_FILES, 0, -1):
                src = path.with_suffix(path.suffix + f".{i}")
                dst = path.with_suffix(path.suffix + f".{i + 1}")
                if src.exists():
                    if i == MAX_ROTATED_FILES:
                        try:
                            src.unlink()
                        except OSError:
                            pass
                    else:
                        try:
                            src.replace(dst)
                        except OSError:
                            pass
            path.replace(path.with_suffix(path.suffix + ".1"))
            logger.info("Rotated community observations file.")
        except OSError as e:
            logger.warning(f"Failed to rotate observations file: {e}")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    @classmethod
    def _iter_observations(cls, include_rotated: bool = True) -> Iterator[CarbonObservation]:
        d = cls.data_dir()
        base = d / OBSERVATIONS_FILE
        paths: List[Path] = []
        if base.exists():
            paths.append(base)
        if include_rotated:
            for i in range(1, MAX_ROTATED_FILES + 1):
                p = base.with_suffix(base.suffix + f".{i}")
                if p.exists():
                    paths.append(p)

        for p in paths:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            yield CarbonObservation.from_json(line)
                        except (json.JSONDecodeError, ValueError, TypeError) as e:
                            logger.warning(f"Skipping malformed line in {p.name}: {e}")
            except OSError as e:
                logger.warning(f"Failed to read {p}: {e}")

    @classmethod
    def get_community_average(
        cls,
        region: str,
        days: int = 30,
        include_rotated: bool = True,
    ) -> Optional[float]:
        """
        Return the mean intensity for ``region`` over the last ``days`` days,
        or ``None`` if there is no matching data.
        """
        result = cls.get_community_stats(region, days, include_rotated)
        return result.mean if result is not None else None

    @classmethod
    def get_community_stats(
        cls,
        region: str,
        days: int = 30,
        include_rotated: bool = True,
    ) -> Optional[CommunityAverage]:
        """
        Return a :class:`CommunityAverage` for ``region`` over the last
        ``days`` days, or ``None`` if there is no matching data.
        """
        region_slug = cls._validate_region(region)
        days = cls._validate_days(days)

        cutoff = time.time() - days * 86400
        total = 0.0
        count = 0
        min_ts: Optional[float] = None
        max_ts: Optional[float] = None

        for obs in cls._iter_observations(include_rotated=include_rotated):
            if obs.region != region_slug:
                continue
            if obs.ts < cutoff:
                continue
            total += obs.intensity
            count += 1
            min_ts = obs.ts if min_ts is None else min(min_ts, obs.ts)
            max_ts = obs.ts if max_ts is None else max(max_ts, obs.ts)

        if count == 0:
            return None
        return CommunityAverage(
            region=region_slug,
            mean=total / count,
            count=count,
            window_days=days,
            min_ts=min_ts,
            max_ts=max_ts,
        )

    # ------------------------------------------------------------------
    # Privacy controls
    # ------------------------------------------------------------------
    @classmethod
    def export_all(cls) -> List[Dict[str, Any]]:
        """Return every stored observation as a list of dicts (for the user)."""
        return [asdict(obs) for obs in cls._iter_observations(include_rotated=True)]

    @classmethod
    def forget_me(cls, contributor: Optional[str] = None) -> int:
        """
        Delete observations matching the contributor hash.

        If ``contributor`` is ``None``, all observations are deleted.
        Returns the number of observations removed.
        """
        d = cls.data_dir()
        if not d.exists():
            return 0

        target_hash = cls._hash_contributor(contributor) if contributor else None

        removed = 0
        base = d / OBSERVATIONS_FILE
        paths: List[Path] = [base] + [
            base.with_suffix(base.suffix + f".{i}")
            for i in range(1, MAX_ROTATED_FILES + 1)
        ]
        for p in paths:
            if not p.exists():
                continue
            kept_lines: List[str] = []
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        try:
                            obs = CarbonObservation.from_json(stripped)
                        except Exception:
                            # Keep unparseable lines? No: they can't be
                            # attributed.  Drop them.
                            removed += 1
                            continue
                        if target_hash is None or obs.contributor_hash == target_hash:
                            removed += 1
                            continue
                        kept_lines.append(stripped)
            except OSError as e:
                logger.warning(f"Failed to read {p} during forget_me: {e}")
                continue

            # Rewrite atomically.
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix=p.name + ".", dir=str(p.parent)
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmpf:
                    for line in kept_lines:
                        tmpf.write(line + "\n")
                os.replace(tmp_name, p)
            except OSError as e:
                logger.error(f"Failed to rewrite {p}: {e}")
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
        logger.info(f"forget_me removed {removed} observation(s).")
        return removed

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------
    @classmethod
    def prune_older_than(cls, days: int) -> int:
        """
        Delete observations older than ``days`` days.  Returns the number
        of observations removed.
        """
        days = cls._validate_days(days)
        cutoff = time.time() - days * 86400
        d = cls.data_dir()
        if not d.exists():
            return 0
        base = d / OBSERVATIONS_FILE
        paths = [base] + [
            base.with_suffix(base.suffix + f".{i}")
            for i in range(1, MAX_ROTATED_FILES + 1)
        ]
        removed = 0
        for p in paths:
            if not p.exists():
                continue
            kept: List[str] = []
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        try:
                            obs = CarbonObservation.from_json(stripped)
                        except Exception:
                            removed += 1
                            continue
                        if obs.ts < cutoff:
                            removed += 1
                            continue
                        kept.append(stripped)
            except OSError as e:
                logger.warning(f"Failed to read {p} during prune: {e}")
                continue
            tmp_fd, tmp_name = tempfile.mkstemp(prefix=p.name + ".", dir=str(p.parent))
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmpf:
                    for line in kept:
                        tmpf.write(line + "\n")
                os.replace(tmp_name, p)
            except OSError as e:
                logger.error(f"Failed to rewrite {p}: {e}")
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
        logger.info(f"prune_older_than removed {removed} observation(s).")
        return removed


__all__ = [
    "SCHEMA_VERSION",
    "OBSERVATIONS_FILE",
    "CarbonObservation",
    "CommunityAverage",
    "CommunityDataHub",
]


# ============================================================================
# Tests
# ============================================================================

def _reset_env(tmpdir: Path) -> None:
    os.environ["GREEN_AGENT_DATA_DIR"] = str(tmpdir)
    os.environ["GREEN_AGENT_COMMUNITY_OPT_IN"] = "0"


def _test_import_and_version() -> None:
    assert __version__ == "2.0.0"
    assert CommunityDataHub is not None


def _test_opt_in_gate(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    assert not CommunityDataHub.is_opted_in()
    assert CommunityDataHub.contribute_carbon_observation("us-east", 0.42, "api") is False

    CommunityDataHub.opt_in()
    assert CommunityDataHub.is_opted_in()
    assert CommunityDataHub.contribute_carbon_observation("us-east", 0.42, "api") is True


def _test_validation(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()

    for bad in ["", "  ", "US East", "region!" * 20]:
        try:
            CommunityDataHub.contribute_carbon_observation(bad, 0.4, "api")
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for region {bad!r}")

    for bad in [float("nan"), float("inf"), -1.0, 10_000.0, True, "abc"]:
        try:
            CommunityDataHub.contribute_carbon_observation("us-east", bad, "api")  # type: ignore[arg-type]
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for intensity {bad!r}")

    for bad in ["", "API KEY", "bad source!"]:
        try:
            CommunityDataHub.contribute_carbon_observation("us-east", 0.4, bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for source {bad!r}")

    for bad in [0, -5, 999999, "30", True]:
        try:
            CommunityDataHub.get_community_average("us-east", days=bad)  # type: ignore[arg-type]
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for days {bad!r}")


def _test_timestamp_types(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    CommunityDataHub.contribute_carbon_observation("us-east", 0.4, "api")

    # Manually read the file to inspect the stored schema.
    fp = CommunityDataHub.data_dir() / OBSERVATIONS_FILE
    with open(fp, "r", encoding="utf-8") as f:
        line = f.readline().strip()
    data = json.loads(line)
    assert isinstance(data["ts"], float)
    assert isinstance(data["timestamp"], str)
    # ISO string should be parseable back to the same UTC second.
    parsed = datetime.fromisoformat(data["timestamp"])
    assert abs(parsed.timestamp() - data["ts"]) < 1.0


def _test_average(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    for v in (0.3, 0.5, 0.7):
        CommunityDataHub.contribute_carbon_observation("us-east", v, "api")
    CommunityDataHub.contribute_carbon_observation("eu-west", 0.9, "sensor")

    avg = CommunityDataHub.get_community_average("us-east", days=30)
    assert avg is not None
    assert abs(avg - 0.5) < 1e-9

    stats = CommunityDataHub.get_community_stats("eu-west", days=30)
    assert stats is not None
    assert stats.count == 1
    assert abs(stats.mean - 0.9) < 1e-9

    assert CommunityDataHub.get_community_average("asia-east") is None


def _test_corrupt_line_tolerance(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    CommunityDataHub.contribute_carbon_observation("us-east", 0.4, "api")

    fp = CommunityDataHub.data_dir() / OBSERVATIONS_FILE
    with open(fp, "a", encoding="utf-8") as f:
        f.write("this is not json\n")
        f.write('{"v":1,"ts":123,"region":"us-east","intensity":0.6,"source":"api"}\n')

    avg = CommunityDataHub.get_community_average("us-east")
    assert avg is not None
    # Two valid observations: 0.4 and 0.6.
    assert abs(avg - 0.5) < 1e-9


def _test_missing_file(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    # No directory, no file.
    assert CommunityDataHub.get_community_average("us-east") is None


def _test_region_normalization(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    CommunityDataHub.contribute_carbon_observation("US-East", 0.4, "api")
    CommunityDataHub.contribute_carbon_observation("us-east", 0.6, "api")

    stats = CommunityDataHub.get_community_stats("us-east")
    assert stats is not None
    assert stats.count == 2


def _test_forget_me(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    CommunityDataHub.contribute_carbon_observation("us-east", 0.4, "api", contributor="alice")
    CommunityDataHub.contribute_carbon_observation("us-east", 0.5, "api", contributor="bob")
    CommunityDataHub.contribute_carbon_observation("us-east", 0.6, "api", contributor="alice")

    removed = CommunityDataHub.forget_me(contributor="alice")
    assert removed == 2

    stats = CommunityDataHub.get_community_stats("us-east")
    assert stats is not None
    assert stats.count == 1

    removed_all = CommunityDataHub.forget_me()
    assert removed_all == 1
    assert CommunityDataHub.get_community_stats("us-east") is None


def _test_prune(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    # Manually write an old observation.
    fp = CommunityDataHub.data_dir() / OBSERVATIONS_FILE
    fp.parent.mkdir(parents=True, exist_ok=True)
    old_ts = time.time() - 90 * 86400
    old_obs = CarbonObservation(
        v=1, ts=old_ts,
        timestamp=datetime.fromtimestamp(old_ts, tz=timezone.utc).isoformat(),
        region="us-east", intensity=0.2, source="api", contributor_hash="x",
    )
    with open(fp, "a", encoding="utf-8") as f:
        f.write(old_obs.to_json() + "\n")

    CommunityDataHub.contribute_carbon_observation("us-east", 0.5, "api")

    removed = CommunityDataHub.prune_older_than(30)
    assert removed == 1
    stats = CommunityDataHub.get_community_stats("us-east")
    assert stats is not None
    assert stats.count == 1


def _test_export_all(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    CommunityDataHub.contribute_carbon_observation("us-east", 0.4, "api")
    CommunityDataHub.contribute_carbon_observation("eu-west", 0.5, "sensor")
    exported = CommunityDataHub.export_all()
    assert len(exported) == 2
    for obs in exported:
        assert "contributor_hash" in obs
        assert "region" in obs


def _test_opt_out(tmpdir: Path) -> None:
    _reset_env(tmpdir)
    CommunityDataHub.opt_in()
    assert CommunityDataHub.is_opted_in()
    CommunityDataHub.opt_out()
    assert not CommunityDataHub.is_opted_in()


def _run_all_tests() -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _test_import_and_version()
        _test_opt_in_gate(tmp)
        _test_validation(tmp)
        _test_timestamp_types(tmp)
        _test_average(tmp)
        _test_corrupt_line_tolerance(tmp)
        _test_missing_file(tmp)
        _test_region_normalization(tmp)
        _test_forget_me(tmp)
        _test_prune(tmp)
        _test_export_all(tmp)
        _test_opt_out(tmp)
    print("All CommunityDataHub tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
