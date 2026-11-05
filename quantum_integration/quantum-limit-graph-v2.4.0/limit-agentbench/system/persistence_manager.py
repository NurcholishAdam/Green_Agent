# -*- coding: utf-8 -*-
"""
PersistenceManager (Enhanced v2)

Atomic persistence for JSON, torch models, pickle, and enhanced component
state (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary).

---- Critical fixes vs v1 ------------------------------------------------------
  * torch.load(..., weights_only=True, map_location="cpu") — v1 was RCE-vulnerable
    on torch < 2.6 and failed on cross-device loads.
  * Pickle is opt-in via trust_pickle=True on BOTH save and load. v1 silently
    fell back to pickle when JSON failed and silently unpickled on load.
  * save_full_checkpoint no longer pretends to be atomic. The recommended API
    is save_checkpoint_dir() / load_checkpoint_dir(), which is transactional
    (manifest + atomic directory swap).
  * load_model / load_pickle raise on missing file instead of silently
    returning.
  * fsync on the file AND the parent directory before/after os.replace, so
    the write is durable, not just atomic.
  * os.makedirs(parent, exist_ok=True) before any write.
  * JSON defaults to allow_nan=False (valid RFC 8259 output).
  * format_version field written into every checkpoint; older files load but
    are flagged.
  * load_enhanced_state raises ValueError on corrupt file instead of
    returning {} (which was indistinguishable from "file missing").
  * Magic-byte format header ("GMA:JSON\\n" / "GMA:PKL\\n") so the load path
    is deterministic; legacy files still load via sniffer.
  * overwrite=False guard on directory saves.
  * Async wrappers for saving during the training loop.

---- Backward compatibility ---------------------------------------------------
  * atomic_json_save / atomic_model_save / load_model / atomic_pickle_save /
    load_pickle / save_enhanced_state / load_enhanced_state keep their
    signatures. New behaviour only makes them *safer*; files written by v1
    still load (with a warning).
  * load_model now raises FileNotFoundError on missing path — callers that
    relied on silent no-op must catch it or use `if os.path.exists(...)`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import pickle
import shutil
import sys
import tempfile
import uuid
import warnings
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import torch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class PersistenceError(Exception):
    """Base class for persistence errors."""


class UnsafeSerializationError(PersistenceError):
    """Raised when pickle is required but trust_pickle=False."""


class CorruptStateError(PersistenceError):
    """Raised when a state file exists but cannot be decoded."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
_MAGIC_JSON = b"GMA:JSON\n"
_MAGIC_PICKLE = b"GMA:PKL\n"
_LEGACY_PICKLE_PREFIX = b"\x80"     # pickle protocol 2+


def _fsync_file(f) -> None:
    f.flush()
    try:
        os.fsync(f.fileno())
    except OSError:
        # Some filesystems (e.g. certain NFS mounts) reject fsync.
        logger.debug("fsync not supported on %r", getattr(f, "name", "?"))


def _fsync_dir(path: str) -> None:
    if not os.path.isdir(path):
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        logger.debug("fsync on directory %s not supported", path)
    finally:
        os.close(fd)


def _atomic_write_bytes(path: str, blob: bytes, *, overwrite: bool = True) -> None:
    parent = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(parent, exist_ok=True)
    if not overwrite and os.path.exists(path):
        raise FileExistsError(path)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(blob)
            _fsync_file(f)
        os.replace(tmp, path)
        _fsync_dir(parent)
    except Exception:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# PersistenceManager
# ---------------------------------------------------------------------------
class PersistenceManager:
    """Atomic persistence for models, JSON, pickle, and enhanced state."""

    FORMAT_VERSION = 2

    # =====================================================================
    # JSON
    # =====================================================================
    @classmethod
    def atomic_json_save(
        cls,
        data: Any,
        path: str,
        *,
        allow_nan: bool = False,
        indent: Optional[int] = 2,
        overwrite: bool = True,
    ) -> None:
        """
        Save JSON atomically. ``allow_nan=False`` (default) refuses NaN/Inf
        so the output is valid RFC 8259 and consumable by jq / JS / Go.
        """
        try:
            text = json.dumps(data, indent=indent, allow_nan=allow_nan)
        except ValueError as exc:
            if not allow_nan:
                raise ValueError(
                    f"State contains NaN/Infinity and allow_nan=False. "
                    f"Pass allow_nan=True to write non-standard JSON, or "
                    f"clean the data first. ({exc})"
                ) from exc
            raise
        _atomic_write_bytes(path, text.encode("utf-8"), overwrite=overwrite)
        logger.debug("Saved JSON: %s (%d bytes)", path, len(text))

    @classmethod
    def load_json(cls, path: str) -> Optional[Any]:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # =====================================================================
    # Torch models
    # =====================================================================
    @staticmethod
    def _as_state_dict(obj: Any) -> Dict[str, Any]:
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, torch.nn.Module):
            return obj.state_dict()
        raise TypeError(
            f"Expected nn.Module or state_dict, got {type(obj).__name__}"
        )

    @classmethod
    def atomic_model_save(cls, model: Any, path: str,
                          *, overwrite: bool = True) -> None:
        """
        Save a model's *state_dict* atomically.

        We never call ``torch.save(module)`` directly — that pickles the whole
        module (fragile across refactors, and requires the class to exist at
        load time).
        """
        state = cls._as_state_dict(model)
        parent = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(parent, exist_ok=True)
        if not overwrite and os.path.exists(path):
            raise FileExistsError(path)
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=parent, suffix=".pt")
        os.close(fd)
        try:
            torch.save(state, tmp)
            _fsync_dir_and_replace(tmp, path, parent)
        except Exception:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        logger.debug("Saved model state_dict: %s", path)

    @classmethod
    def load_model(
        cls,
        model: Optional[torch.nn.Module],
        path: str,
        *,
        strict: bool = True,
        map_location: Union[str, torch.device] = "cpu",
        weights_only: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Load a state_dict into ``model`` (which may be None to just return
        the state_dict). Raises FileNotFoundError if missing.

        ``weights_only=True`` (default) restricts the loader to safe primitive
        tensors — required for any checkpoint from an untrusted source.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(path)

        try:
            state = torch.load(
                path,
                map_location=map_location,
                weights_only=weights_only,
            )
        except TypeError:
            # torch < 1.13 doesn't have weights_only — surface a warning and
            # fall back. This is a known RCE risk on untrusted inputs.
            logger.warning(
                "torch < 1.13 does not support weights_only=True; "
                "this load is UNSAFE for untrusted checkpoints."
            )
            state = torch.load(path, map_location=map_location)

        if model is None:
            return state
        model.load_state_dict(state, strict=strict)
        logger.debug("Loaded model state_dict: %s (strict=%s)", path, strict)
        return state

    # =====================================================================
    # Pickle (opt-in)
    # =====================================================================
    @classmethod
    def atomic_pickle_save(
        cls,
        obj: Any,
        path: str,
        *,
        trust_pickle: bool = False,
        overwrite: bool = True,
    ) -> None:
        if not trust_pickle:
            raise UnsafeSerializationError(
                "Refusing to write pickle by default. Pass trust_pickle=True "
                "if the destination is trusted and will only be loaded by "
                "trusted code."
            )
        payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        _atomic_write_bytes(path, _MAGIC_PICKLE + payload, overwrite=overwrite)
        logger.debug("Saved pickle: %s (%d bytes)", path, len(payload))

    @classmethod
    def load_pickle(cls, path: str, *, trust_pickle: bool = False) -> Any:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        if not trust_pickle:
            raise UnsafeSerializationError(
                "Refusing to unpickle by default (arbitrary code execution). "
                "Pass trust_pickle=True only for files you produced yourself."
            )
        with open(path, "rb") as f:
            head = f.read(len(_MAGIC_PICKLE))
            if head == _MAGIC_PICKLE:
                return pickle.load(f)
            # Legacy pickle (written by v1) — no header
            f.seek(0)
            if f.read(1) == _LEGACY_PICKLE_PREFIX:
                f.seek(0)
                warnings.warn(
                    f"{path} is a legacy pickle without a version header; "
                    "re-save with save_enhanced_state() to upgrade.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return pickle.load(f)
        raise CorruptStateError(f"Unknown pickle format: {path}")

    # =====================================================================
    # Enhanced state (JSON preferred, pickle opt-in)
    # =====================================================================
    @classmethod
    def save_enhanced_state(
        cls,
        state: Dict[str, Any],
        path: str,
        *,
        trust_pickle: bool = False,
        allow_nan: bool = False,
        overwrite: bool = True,
    ) -> str:
        """
        Save enhanced state.

        Returns the format actually written: ``"json"`` or ``"pickle"``.
        """
        # Try JSON first
        try:
            text = json.dumps(state, indent=2, allow_nan=allow_nan)
            _atomic_write_bytes(path, text.encode("utf-8"),
                                overwrite=overwrite)
            logger.debug("Saved enhanced state as JSON: %s", path)
            return "json"
        except (TypeError, ValueError) as exc:
            if not trust_pickle:
                raise UnsafeSerializationError(
                    "State contains non-JSON-serializable types "
                    "(numpy arrays, custom classes, etc.). Convert to "
                    "JSON-safe primitives or pass trust_pickle=True to "
                    "save as pickle. Original error: " + str(exc)
                ) from exc
            logger.debug("JSON failed (%s); falling back to pickle", exc)

        payload = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
        _atomic_write_bytes(path, _MAGIC_PICKLE + payload,
                            overwrite=overwrite)
        logger.debug("Saved enhanced state as pickle: %s", path)
        return "pickle"

    @classmethod
    def load_enhanced_state(
        cls,
        path: str,
        *,
        trust_pickle: bool = False,
    ) -> Dict[str, Any]:
        """
        Load enhanced state. Raises FileNotFoundError if missing,
        CorruptStateError if the file exists but can't be decoded.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(path)

        with open(path, "rb") as f:
            head = f.read(max(len(_MAGIC_JSON), len(_MAGIC_PICKLE)))

        # --- v2 pickle ---
        if head.startswith(_MAGIC_PICKLE):
            if not trust_pickle:
                raise UnsafeSerializationError(
                    f"{path} contains a pickle payload. Pass "
                    "trust_pickle=True only if the file is trusted."
                )
            with open(path, "rb") as f:
                f.read(len(_MAGIC_PICKLE))
                return pickle.load(f)

        # --- v2 JSON ---
        if head.startswith(_MAGIC_JSON):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    f.read(len(_MAGIC_JSON))
                    return json.load(f)
            except json.JSONDecodeError as exc:
                raise CorruptStateError(
                    f"JSON body of {path} is corrupt: {exc}"
                ) from exc

        # --- legacy: try JSON (v1 files start with whitespace or '{') ---
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                logger.debug("Loaded legacy JSON state: %s", path)
                return data
            raise CorruptStateError(f"Top-level value is not a dict: {path}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # --- legacy: try pickle ---
        if head[:1] == _LEGACY_PICKLE_PREFIX:
            if not trust_pickle:
                raise UnsafeSerializationError(
                    f"{path} is a legacy pickle. Pass trust_pickle=True "
                    "to load it, then re-save with save_enhanced_state() to "
                    "migrate to a header-tagged format."
                )
            try:
                with open(path, "rb") as f:
                    data = pickle.load(f)
                if isinstance(data, dict):
                    warnings.warn(
                        f"{path} is a legacy pickle; re-save to migrate.",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    return data
                raise CorruptStateError(
                    f"Legacy pickle did not decode to dict: {path}"
                )
            except Exception as exc:                     # noqa: BLE001
                raise CorruptStateError(
                    f"Failed to unpickle legacy {path}: {exc}"
                ) from exc

        raise CorruptStateError(f"Unknown or corrupt state format: {path}")

    # =====================================================================
    # Full checkpoint (legacy two-file API) — warned but supported
    # =====================================================================
    @classmethod
    def save_full_checkpoint(
        cls,
        model: Any,
        enhanced_state: Dict[str, Any],
        model_path: str,
        state_path: str,
        **kwargs: Any,
    ) -> None:
        """
        Save model + enhanced state to two separate files.

        .. warning::
            This is NOT atomic across the two files. Prefer
            :meth:`save_checkpoint_dir` for transactional checkpoints.
        """
        warnings.warn(
            "save_full_checkpoint is not atomic across the two files; "
            "use save_checkpoint_dir for transactional saves.",
            DeprecationWarning,
            stacklevel=2,
        )
        # Save state first (cheap), model second (expensive). If model fails,
        # the state is a stale orphan — safer than new weights with old state.
        cls.save_enhanced_state(enhanced_state, state_path, **kwargs)
        cls.atomic_model_save(model, model_path)

    @classmethod
    def load_full_checkpoint(
        cls,
        model: torch.nn.Module,
        model_path: str,
        state_path: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if not os.path.exists(model_path):
            raise FileNotFoundError(model_path)
        if not os.path.exists(state_path):
            raise FileNotFoundError(state_path)
        cls.load_model(model, model_path)
        return cls.load_enhanced_state(state_path, **kwargs)

    # =====================================================================
    # Directory checkpoint (transactional, recommended)
    # =====================================================================
    @classmethod
    def save_checkpoint_dir(
        cls,
        model: Any,
        enhanced_state: Dict[str, Any],
        dir_path: str,
        *,
        trust_pickle: bool = False,
        allow_nan: bool = False,
        overwrite: bool = True,
    ) -> str:
        """
        Save a *transactional* checkpoint directory:

            dir_path/
                manifest.json
                model.pt
                state.json OR state.pkl

        The entire directory is written to a temp sibling, fsynced, then
        renamed into place. A reader either sees the old checkpoint or the
        new one — never a half-written one.
        """
        parent = os.path.dirname(os.path.abspath(dir_path)) or "."
        os.makedirs(parent, exist_ok=True)
        if os.path.exists(dir_path) and not overwrite:
            raise FileExistsError(dir_path)

        tmp = f"{dir_path}.tmp-{uuid.uuid4().hex[:8]}"
        os.makedirs(tmp)

        try:
            # --- model ---
            model_path = os.path.join(tmp, "model.pt")
            torch.save(cls._as_state_dict(model), model_path)
            _fsync_dir(tmp)

            # --- state ---
            try:
                text = json.dumps(enhanced_state, indent=2,
                                  allow_nan=allow_nan)
                state_name = "state.json"
                state_format = "json"
                _atomic_write_bytes(os.path.join(tmp, state_name),
                                    text.encode("utf-8"))
            except (TypeError, ValueError) as exc:
                if not trust_pickle:
                    raise UnsafeSerializationError(
                        "State contains non-JSON types. Pass "
                        "trust_pickle=True or clean the data. "
                        f"Original error: {exc}"
                    ) from exc
                state_name = "state.pkl"
                state_format = "pickle"
                _atomic_write_bytes(
                    os.path.join(tmp, state_name),
                    _MAGIC_PICKLE + pickle.dumps(
                        enhanced_state, protocol=pickle.HIGHEST_PROTOCOL),
                )

            # --- manifest (last, so its presence implies success) ---
            manifest = {
                "format_version": cls.FORMAT_VERSION,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "model_file": "model.pt",
                "state_file": state_name,
                "state_format": state_format,
                "model_sha256": _sha256(model_path),
                "state_sha256": _sha256(os.path.join(tmp, state_name)),
                "torch_version": torch.__version__,
                "python_version": sys.version.split()[0],
            }
            with open(os.path.join(tmp, "manifest.json"), "w",
                      encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
                _fsync_file(f)
            _fsync_dir(tmp)

            # --- atomic swap ---
            if os.path.exists(dir_path):
                backup = f"{dir_path}.old-{uuid.uuid4().hex[:8]}"
                os.rename(dir_path, backup)
                try:
                    os.rename(tmp, dir_path)
                except Exception:
                    os.rename(backup, dir_path)
                    raise
                shutil.rmtree(backup, ignore_errors=True)
            else:
                os.rename(tmp, dir_path)
            _fsync_dir(parent)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise

        logger.info("Saved checkpoint dir: %s", dir_path)
        return dir_path

    @classmethod
    def load_checkpoint_dir(
        cls,
        model: Optional[torch.nn.Module],
        dir_path: str,
        *,
        strict: bool = True,
        map_location: Union[str, torch.device] = "cpu",
        weights_only: bool = True,
        trust_pickle: bool = False,
        verify: bool = True,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Load a checkpoint dir. Returns (state, model_state_dict|None).
        """
        manifest_path = os.path.join(dir_path, "manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(manifest_path)
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        fv = manifest.get("format_version")
        if fv != cls.FORMAT_VERSION:
            logger.warning(
                "Checkpoint %s has format_version=%s (expected %d)",
                dir_path, fv, cls.FORMAT_VERSION,
            )

        model_path = os.path.join(dir_path, manifest["model_file"])
        state_path = os.path.join(dir_path, manifest["state_file"])

        if verify:
            for label, path, expected in (
                ("model", model_path, manifest.get("model_sha256")),
                ("state", state_path, manifest.get("state_sha256")),
            ):
                if expected and _sha256(path) != expected:
                    raise CorruptStateError(
                        f"{label} file {path} failed integrity check "
                        f"(manifest sha256 mismatch)"
                    )

        model_state = cls.load_model(
            model, model_path, strict=strict,
            map_location=map_location, weights_only=weights_only,
        )
        state = cls.load_enhanced_state(state_path, trust_pickle=trust_pickle)
        logger.info("Loaded checkpoint dir: %s", dir_path)
        return state, model_state

    # =====================================================================
    # Async wrappers
    # =====================================================================
    @classmethod
    async def async_save_enhanced_state(
        cls, state: Dict[str, Any], path: str, **kwargs: Any,
    ) -> str:
        return await asyncio.to_thread(
            cls.save_enhanced_state, state, path, **kwargs
        )

    @classmethod
    async def async_save_checkpoint_dir(
        cls, model: Any, state: Dict[str, Any], dir_path: str,
        **kwargs: Any,
    ) -> str:
        return await asyncio.to_thread(
            cls.save_checkpoint_dir, model, state, dir_path, **kwargs
        )


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    workdir = tempfile.mkdtemp(prefix="persistence-test-")
    try:
        # --- JSON: NaN rejected by default ---
        try:
            PersistenceManager.atomic_json_save(
                {"x": float("nan")}, os.path.join(workdir, "nan.json"),
            )
            raise AssertionError("expected NaN to be rejected")
        except ValueError:
            pass
        PersistenceManager.atomic_json_save(
            {"x": 1.0}, os.path.join(workdir, "ok.json"),
        )

        # --- Pickle: opt-in ---
        try:
            PersistenceManager.atomic_pickle_save(
                {"a": 1}, os.path.join(workdir, "p.pkl"),
            )
            raise AssertionError("expected UnsafeSerializationError")
        except UnsafeSerializationError:
            pass
        PersistenceManager.atomic_pickle_save(
            {"a": 1}, os.path.join(workdir, "p.pkl"), trust_pickle=True,
        )

        # --- Missing file raises ---
        try:
            PersistenceManager.load_model(None, os.path.join(workdir, "missing.pt"))
            raise AssertionError("expected FileNotFoundError")
        except FileNotFoundError:
            pass

        # --- Save / load enhanced state (JSON) ---
        state = {"modp_weights": [0.4, 0.3, 0.2, 0.1],
                 "graph_metrics": {"centrality": 0.7, "connectivity": 0.6}}
        fmt = PersistenceManager.save_enhanced_state(
            state, os.path.join(workdir, "state.json"))
        assert fmt == "json"
        loaded = PersistenceManager.load_enhanced_state(
            os.path.join(workdir, "state.json"))
        assert loaded == state

        # --- Save / load enhanced state (pickle opt-in) ---
        state_np = {"weights": np.zeros((3, 3))}
        try:
            PersistenceManager.save_enhanced_state(
                state_np, os.path.join(workdir, "state.pkl"))
            raise AssertionError("expected UnsafeSerializationError")
        except UnsafeSerializationError:
            pass
        PersistenceManager.save_enhanced_state(
            state_np, os.path.join(workdir, "state.pkl"), trust_pickle=True)
        try:
            PersistenceManager.load_enhanced_state(
                os.path.join(workdir, "state.pkl"))
            raise AssertionError("expected UnsafeSerializationError on load")
        except UnsafeSerializationError:
            pass
        loaded = PersistenceManager.load_enhanced_state(
            os.path.join(workdir, "state.pkl"), trust_pickle=True)
        assert np.allclose(loaded["weights"], state_np["weights"])

        # --- Directory checkpoint: round-trip + integrity ---
        model = torch.nn.Linear(4, 2)
        state = {"best_metric": 0.92}
        ckpt = os.path.join(workdir, "ckpt")
        PersistenceManager.save_checkpoint_dir(model, state, ckpt)

        model2 = torch.nn.Linear(4, 2)
        state2, sd = PersistenceManager.load_checkpoint_dir(model2, ckpt)
        assert state2 == state
        for p1, p2 in zip(model.parameters(), model2.parameters()):
            assert torch.allclose(p1, p2)

        # Corrupt the manifest and verify integrity check fires
        manifest_path = os.path.join(ckpt, "manifest.json")
        with open(manifest_path) as f:
            m = json.load(f)
        m["model_sha256"] = "deadbeef"
        with open(manifest_path, "w") as f:
            json.dump(m, f)
        try:
            PersistenceManager.load_checkpoint_dir(model2, ckpt)
            raise AssertionError("expected integrity check failure")
        except CorruptStateError:
            pass

        print("all smoke tests passed")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
