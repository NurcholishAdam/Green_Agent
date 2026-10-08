# src/quantum_integration/digital_twin/digital_twin_persistence.py

"""Atomic gzip-compressed persistence for the digital twin.

File format
-----------
New writes use a self-describing container::

    b"DTW1" | format_byte | payload

where ``format_byte`` is one of ``_FORMAT_RAW`` (uncompressed JSON),
``_FORMAT_ZLIB`` (raw DEFLATE with a zlib header), or ``_FORMAT_GZIP``
(a gzip stream). Legacy files written by v1 of this module — raw zlib
bytes with no header — are still readable; the loader sniffs the first
four bytes to tell the two apart.

Schema migration
----------------
Checkpoints carry a ``schema_version``. Older payloads are migrated
through :data:`_MIGRATIONS` before deserialization. When you bump
:data:`SCHEMA_VERSION`, register a migration callable for the previous
version. In non-strict mode, a missing migration is a load failure; in
strict mode, it raises.

Durability
----------
When ``durable=True`` (default), the temp file and its parent directory
are ``fsync``-ed before and after the atomic rename, so a crash cannot
leave a truncated or stale destination. Set ``durable=False`` for
faster writes on network filesystems where the trade-off is acceptable.

Event-loop friendliness
-----------------------
All blocking I/O (file read/write, fsync, decompression) runs on
``asyncio.to_thread`` so the event loop is never blocked. The
``_LazyLock`` serializes access across coroutines.

Round-trip caveats
------------------
``json.dumps(..., default=str)`` is used for compatibility with
non-JSON-native values. Prefer storing JSON-native types; complex
objects will round-trip as their ``str`` form.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import json
import logging
import os
import tempfile
import zlib
from collections.abc import Mapping as ABCMapping
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .digital_twin_errors import (
    DigitalTwinParseError,
    DigitalTwinPersistenceError,
)
from .digital_twin_helpers import _LazyLock, _iso_now

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

# --------------------------------------------------------------------------- #
# Container format
# --------------------------------------------------------------------------- #

_MAGIC: bytes = b"DTW1"
_FORMAT_RAW: int = 0x00
_FORMAT_ZLIB: int = 0x01
_FORMAT_GZIP: int = 0x02
_VALID_FORMATS = frozenset({_FORMAT_RAW, _FORMAT_ZLIB, _FORMAT_GZIP})

_DEFAULT_MAX_COMPRESSED_BYTES: int = 64 * 1024 * 1024      # 64 MiB
_DEFAULT_MAX_DECOMPRESSED_BYTES: int = 512 * 1024 * 1024   # 512 MiB
_READ_STEP: int = 65536


# --------------------------------------------------------------------------- #
# Container helpers
# --------------------------------------------------------------------------- #

def _sniff_and_unpack(data: bytes) -> Tuple[int, bytes]:
    """Return ``(format_byte, payload)`` from a checkpoint blob.

    Legacy v1 files (raw zlib, no header) are detected by the absence of
    the :data:`_MAGIC` prefix and reported as ``_FORMAT_ZLIB``.
    """
    if data.startswith(_MAGIC):
        if len(data) < len(_MAGIC) + 1:
            raise DigitalTwinParseError(
                "checkpoint is truncated (magic header only)."
            )
        fmt = data[len(_MAGIC)]
        if fmt not in _VALID_FORMATS:
            raise DigitalTwinParseError(
                f"unknown format byte 0x{fmt:02x}."
            )
        return fmt, data[len(_MAGIC) + 1:]
    # Legacy v1 checkpoint: raw zlib stream, no header.
    return _FORMAT_ZLIB, data


def _pack_payload(
    state: Mapping[str, Any], *, compression: str, level: int,
) -> bytes:
    """Serialize ``state`` and wrap it in the DTW1 container."""
    json_str = json.dumps(
        state, separators=(",", ":"), default=str,
    )
    raw = json_str.encode("utf-8")
    if compression == "none":
        fmt = _FORMAT_RAW
        payload = raw
    elif compression == "gzip":
        fmt = _FORMAT_GZIP
        payload = gzip.compress(raw, compresslevel=level)
    else:  # "zlib"
        fmt = _FORMAT_ZLIB
        payload = zlib.compress(raw, level)
    return _MAGIC + bytes([fmt]) + payload


def _decompress_zlib_guarded(data: bytes, max_bytes: int) -> bytes:
    """Decompress a zlib stream with a hard output-size cap."""
    d = zlib.decompressobj()
    out = bytearray()
    for i in range(0, len(data), _READ_STEP):
        chunk = d.decompress(
            data[i:i + _READ_STEP], max_bytes - len(out),
        )
        out.extend(chunk)
        if len(out) >= max_bytes:
            raise DigitalTwinParseError(
                f"decompressed payload exceeds {max_bytes} bytes."
            )
    # Drain the internal buffer.
    while True:
        chunk = d.decompress(b"", max_bytes - len(out))
        if not chunk:
            break
        out.extend(chunk)
        if len(out) >= max_bytes:
            raise DigitalTwinParseError(
                f"decompressed payload exceeds {max_bytes} bytes."
            )
    tail = d.flush()
    if tail:
        if len(out) + len(tail) > max_bytes:
            raise DigitalTwinParseError(
                f"decompressed payload exceeds {max_bytes} bytes."
            )
        out.extend(tail)
    return bytes(out)


def _decompress_gzip_guarded(data: bytes, max_bytes: int) -> bytes:
    """Decompress a gzip stream with a hard output-size cap."""
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            out = bytearray()
            while True:
                chunk = gz.read(_READ_STEP)
                if not chunk:
                    break
                out.extend(chunk)
                if len(out) > max_bytes:
                    raise DigitalTwinParseError(
                        f"decompressed payload exceeds {max_bytes} bytes."
                    )
            return bytes(out)
    except (OSError, EOFError) as exc:
        raise DigitalTwinParseError(
            "gzip payload is corrupt or truncated."
        ) from exc


def _decompress_guarded(
    fmt: int, data: bytes, max_bytes: int,
) -> bytes:
    """Dispatch to the right decompressor with a shared size budget."""
    if fmt == _FORMAT_RAW:
        if len(data) > max_bytes:
            raise DigitalTwinParseError(
                f"raw payload exceeds {max_bytes} bytes."
            )
        return data
    if fmt == _FORMAT_ZLIB:
        return _decompress_zlib_guarded(data, max_bytes)
    if fmt == _FORMAT_GZIP:
        return _decompress_gzip_guarded(data, max_bytes)
    raise DigitalTwinParseError(f"unsupported format byte 0x{fmt:02x}.")


def _get_or(state: Mapping[str, Any], key: str, default: Any) -> Any:
    """Return ``state[key]`` unless it is missing or ``None``."""
    v = state.get(key)
    return default if v is None else v


# --------------------------------------------------------------------------- #
# Schema migrations
# --------------------------------------------------------------------------- #
#
# Register ``{from_version: callable}`` when bumping ``SCHEMA_VERSION``.
# A callable receives the deserialized dict and returns the migrated
# dict, with ``schema_version`` bumped to ``from_version + 1``.

_MIGRATIONS: Dict[int, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}


def _migrate(state: Dict[str, Any], from_version: int) -> Dict[str, Any]:
    """Apply registered migrations until ``SCHEMA_VERSION`` is reached."""
    v = from_version
    while v < SCHEMA_VERSION:
        fn = _MIGRATIONS.get(v)
        if fn is None:
            raise DigitalTwinParseError(
                f"no migration registered from schema_version {v}."
            )
        state = fn(state)
        v = int(state.get("schema_version", v + 1))
    return state


def _assert_compatible(data: Any) -> int:
    """Validate the container and return the declared schema version."""
    if not isinstance(data, dict):
        raise DigitalTwinParseError("state root must be a Mapping.")
    v = data.get("schema_version", SCHEMA_VERSION)
    if not isinstance(v, int) or isinstance(v, bool) or v <= 0:
        raise DigitalTwinParseError(f"invalid schema_version {v!r}.")
    if v > SCHEMA_VERSION:
        raise DigitalTwinParseError(
            f"state schema_version {v} is newer than {SCHEMA_VERSION}."
        )
    return v


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #

class DigitalTwinPersistenceManager:
    """Persists a twin's state as atomically-written compressed JSON.

    Parameters
    ----------
    path:
        Destination file path. Parent directories are created on demand.
    atomic_writes:
        When ``True`` (default), writes go through a temp file and
        ``os.replace``. When ``False``, writes are direct (no crash
        safety, but faster).
    durable:
        When ``True`` (default), ``fsync`` the temp file and its parent
        directory around the rename. Requires ``atomic_writes`` for full
        effect.
    compression:
        One of ``"zlib"`` (default), ``"gzip"``, or ``"none"``.
    compression_level:
        ``1``–``9`` for ``zlib``/``gzip``; ignored for ``"none"``.
    max_compressed_bytes:
        Refuse to read files larger than this before decompressing.
    max_decompressed_bytes:
        Hard cap on the decompressed size (decompression-bomb guard).
    """

    def __init__(
        self,
        path: str,
        *,
        atomic_writes: bool = True,
        durable: bool = True,
        compression: str = "zlib",
        compression_level: int = 6,
        max_compressed_bytes: int = _DEFAULT_MAX_COMPRESSED_BYTES,
        max_decompressed_bytes: int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
    ) -> None:
        if not isinstance(path, str) or not path.strip():
            raise DigitalTwinPersistenceError(
                "path must be a non-empty string.",
            )
        if compression not in ("zlib", "gzip", "none"):
            raise DigitalTwinPersistenceError(
                f"compression must be 'zlib', 'gzip', or 'none'; "
                f"got {compression!r}.",
            )
        if not isinstance(compression_level, int) or isinstance(
            compression_level, bool,
        ):
            raise DigitalTwinPersistenceError(
                "compression_level must be an integer.",
            )
        if not (1 <= compression_level <= 9):
            raise DigitalTwinPersistenceError(
                "compression_level must be in [1, 9].",
            )
        if max_compressed_bytes <= 0 or max_decompressed_bytes <= 0:
            raise DigitalTwinPersistenceError(
                "size limits must be positive.",
            )

        self.path = path
        self.atomic_writes = bool(atomic_writes)
        self.durable = bool(durable)
        self.compression = compression
        self.compression_level = int(compression_level)
        self.max_compressed_bytes = int(max_compressed_bytes)
        self.max_decompressed_bytes = int(max_decompressed_bytes)
        self._lock = _LazyLock()

        # Typed diagnostics for the most recent operation.
        self.last_error: Optional[Exception] = None
        # One of: "ok", "missing", "incompatible", "corrupt", "failed".
        self.last_status: str = "ok"

    # ------------------------------------------------------------------ #
    # State collection / deserialization
    # ------------------------------------------------------------------ #

    def _extract_q_teacher(self, twin: Any) -> Optional[Dict[str, Any]]:
        """Return the Q-teacher's serialized state via the distillation API.

        Inverts the dependency: instead of reaching into
        ``twin.distillation.q_teacher.weights`` (which silently returns
        ``None`` if the internals are renamed), we ask the distillation
        module to serialize itself.
        """
        distill = getattr(twin, "distillation", None)
        if distill is None:
            return None
        to_dict = getattr(distill, "to_dict", None)
        if not callable(to_dict):
            return None
        try:
            payload = to_dict()
        except Exception as exc:  # noqa: BLE001 - best-effort
            logger.warning("distillation.to_dict() failed: %s", exc)
            return None
        if not isinstance(payload, ABCMapping):
            return None
        q = payload.get("q_teacher")
        return dict(q) if isinstance(q, ABCMapping) else None

    def _collect_state(self, twin: Any) -> Dict[str, Any]:
        """Build the JSON-serializable payload for ``twin``."""
        config = getattr(twin, "config", None)
        if config is not None and hasattr(config, "to_dict"):
            config_payload = config.to_dict()
        elif config is not None:
            config_payload = {
                k: v for k, v in vars(config).items()
                if not k.startswith("_")
            }
        else:
            config_payload = {}

        return {
            "schema_version": SCHEMA_VERSION,
            "twin_version": getattr(twin, "__version__", None),
            "config": config_payload,
            "scenario_results": [
                r.to_dict() if hasattr(r, "to_dict") else dict(vars(r))
                for r in getattr(twin, "scenario_results", []) or []
            ],
            "resource_projections": {
                k: (v.to_dict() if hasattr(v, "to_dict") else dict(vars(v)))
                for k, v in (
                    getattr(twin, "resource_projections", {}) or {}
                ).items()
            },
            "priority_weights": dict(
                getattr(twin, "priority_weights", {}) or {}
            ),
            "resource_correlation": {
                k: dict(v) for k, v in (
                    getattr(twin, "resource_correlation", {}) or {}
                ).items()
            },
            "substitution_options": {
                k: list(v) for k, v in (
                    getattr(twin, "substitution_options", {}) or {}
                ).items()
            },
            "last_save": _iso_now(),
            "q_teacher": self._extract_q_teacher(twin),
        }

    async def _deserialize_into(
        self, twin: Any, state: Mapping[str, Any],
    ) -> None:
        """Apply a loaded (and migrated) payload to ``twin``."""
        from .digital_twin_schemas import (
            DigitalTwinResult,
            ResourceProjection,
        )

        # Empty-but-saved values must survive the round-trip. Only
        # fall back to the twin's current value when the key is truly
        # absent (``None``), never when it is ``{}`` or ``[]``.
        if "priority_weights" in state and state["priority_weights"] is not None:
            twin.priority_weights = dict(state["priority_weights"])
        if (
            "resource_correlation" in state
            and state["resource_correlation"] is not None
        ):
            twin.resource_correlation = {
                k: dict(v) for k, v in state["resource_correlation"].items()
            }
        if (
            "substitution_options" in state
            and state["substitution_options"] is not None
        ):
            twin.substitution_options = {
                k: list(v) for k, v in state["substitution_options"].items()
            }
        if "scenario_results" in state and state["scenario_results"] is not None:
            twin.scenario_results = [
                DigitalTwinResult.from_dict(d)
                for d in state["scenario_results"]
            ]
        if (
            "resource_projections" in state
            and state["resource_projections"] is not None
        ):
            twin.resource_projections = {
                k: ResourceProjection.from_dict(v)
                for k, v in state["resource_projections"].items()
            }

        # Q-teacher: prefer the decoupled dict form; accept the legacy
        # ``q_teacher_weights`` list-of-floats for v1 checkpoints.
        q_payload: Optional[Mapping[str, Any]] = None
        raw_q = state.get("q_teacher")
        if isinstance(raw_q, ABCMapping) and raw_q:
            q_payload = raw_q
        else:
            legacy = state.get("q_teacher_weights")
            if legacy is not None:
                q_payload = {"weights": list(legacy)}
        if q_payload is not None:
            distill = getattr(twin, "distillation", None)
            if distill is not None:
                try:
                    from .digital_twin_distillation import (
                        TwinStatefulQTeacher,
                    )
                    distill.q_teacher = TwinStatefulQTeacher.from_dict(
                        dict(q_payload),
                    )
                except Exception as exc:  # noqa: BLE001 - best-effort
                    logger.warning("Failed to restore Q-teacher: %s", exc)

        # Update the live twin's last_save so callers see the same
        # timestamp the file was written with.
        last_save = state.get("last_save")
        if isinstance(last_save, str) and hasattr(twin, "last_save"):
            twin.last_save = last_save

    # ------------------------------------------------------------------ #
    # Atomic write
    # ------------------------------------------------------------------ #

    def _atomic_write(
        self, path: Path, payload: bytes, *, durable: bool,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

        if not self.atomic_writes:
            if durable:
                with open(path, "wb") as f:
                    f.write(payload)
                    f.flush()
                    os.fsync(f.fileno())
            else:
                path.write_bytes(payload)
            return

        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
                if durable:
                    f.flush()
                    os.fsync(f.fileno())
            os.replace(tmp_path, path)
            if durable:
                self._fsync_dir(path.parent)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        """Best-effort fsync of a directory entry (POSIX only)."""
        flags = getattr(os, "O_DIRECTORY", 0)
        if not flags:
            return  # Windows / unsupported platforms.
        try:
            fd = os.open(str(directory), flags)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def save_state(self, twin: Any, *, flush: bool = True) -> bool:
        """Serialize ``twin`` to disk. Returns ``True`` on success."""
        async with self._lock:
            path = Path(self.path)
            self.last_error = None
            try:
                state = self._collect_state(twin)
                payload = _pack_payload(
                    state,
                    compression=self.compression,
                    level=self.compression_level,
                )
                await asyncio.to_thread(
                    self._atomic_write, path, payload, self.durable,
                )
                self.last_status = "ok"
                return True
            except Exception as exc:  # noqa: BLE001 - defensive
                self.last_error = exc
                self.last_status = "failed"
                logger.exception("Failed to save state")
                if getattr(twin, "strict", False):
                    raise DigitalTwinPersistenceError(
                        "failed to save state",
                        path=str(path),
                        errtype=type(exc).__name__,
                    ) from exc
                return False

    async def load_state(self, twin: Any) -> bool:
        """Load state into ``twin``. Returns ``True`` on success.

        Sets :attr:`last_status` to one of ``"ok"``, ``"missing"``,
        ``"incompatible"``, ``"corrupt"``, or ``"failed"`` and stores
        the originating exception on :attr:`last_error` for anything
        other than ``"ok"``.
        """
        async with self._lock:
            path = Path(self.path)
            self.last_error = None
            try:
                exists = await asyncio.to_thread(path.exists)
                if not exists:
                    self.last_status = "missing"
                    return False

                size = await asyncio.to_thread(
                    lambda p=path: p.stat().st_size,
                )
                if size > self.max_compressed_bytes:
                    raise DigitalTwinParseError(
                        f"compressed payload is {size} bytes, exceeds "
                        f"limit {self.max_compressed_bytes}.",
                    )

                compressed = await asyncio.to_thread(path.read_bytes)
                fmt, body = _sniff_and_unpack(compressed)
                raw = await asyncio.to_thread(
                    _decompress_guarded, fmt, body,
                    self.max_decompressed_bytes,
                )
                state = json.loads(raw.decode("utf-8"))
                declared_v = _assert_compatible(state)
                if declared_v < SCHEMA_VERSION:
                    state = _migrate(state, declared_v)
                await self._deserialize_into(twin, state)
                self.last_status = "ok"
                return True
            except DigitalTwinParseError as exc:
                self.last_error = exc
                self.last_status = "incompatible"
                logger.warning("Checkpoint incompatible: %s", exc)
                if getattr(twin, "strict", False):
                    raise
                return False
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                self.last_error = exc
                self.last_status = "corrupt"
                logger.warning("Checkpoint corrupt: %s", exc)
                if getattr(twin, "strict", False):
                    raise DigitalTwinParseError(
                        "checkpoint is corrupt",
                        path=str(path),
                        errtype=type(exc).__name__,
                    ) from exc
                return False
            except Exception as exc:  # noqa: BLE001 - defensive
                self.last_error = exc
                self.last_status = "failed"
                logger.exception("Failed to load state")
                if getattr(twin, "strict", False):
                    raise DigitalTwinPersistenceError(
                        "failed to load state",
                        path=str(path),
                        errtype=type(exc).__name__,
                    ) from exc
                return False

    async def delete_state(self) -> bool:
        """Delete the checkpoint file. Returns ``True`` if removed."""
        async with self._lock:
            path = Path(self.path)
            try:
                await asyncio.to_thread(path.unlink)
                return True
            except FileNotFoundError:
                return False
            except OSError as exc:
                logger.warning(
                    "Failed to delete state: %s (errno=%s)",
                    exc, getattr(exc, "errno", None),
                )
                return False

    def exists(self) -> bool:
        """Return ``True`` if the checkpoint file exists."""
        try:
            return Path(self.path).exists()
        except OSError:
            return False

    # ------------------------------------------------------------------ #
    # Introspection / lifecycle
    # ------------------------------------------------------------------ #

    def statistics(self) -> Dict[str, Any]:
        """Return a snapshot of the manager's configuration and file state."""
        path = Path(self.path)
        exists = False
        size = 0
        try:
            st = path.stat()
            exists = True
            size = st.st_size
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning("statistics(): stat failed: %s", exc)
            size = -1

        return {
            "schema_version": SCHEMA_VERSION,
            "path": self.path,
            "exists": exists,
            "size_bytes": size,
            "atomic_writes": self.atomic_writes,
            "durable": self.durable,
            "compression": self.compression,
            "compression_level": self.compression_level,
            "max_compressed_bytes": self.max_compressed_bytes,
            "max_decompressed_bytes": self.max_decompressed_bytes,
            "last_status": self.last_status,
            "last_error_type": (
                type(self.last_error).__name__
                if self.last_error is not None else None
            ),
        }

    def close(self, *, flush: bool = True) -> None:
        """No-op; accepted for API symmetry with other managers."""
        return None

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(path={self.path!r}, "
            f"compression={self.compression!r}, "
            f"atomic_writes={self.atomic_writes}, "
            f"durable={self.durable})"
        )


__all__ = ["SCHEMA_VERSION", "DigitalTwinPersistenceManager"]
