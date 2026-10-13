"""
federated_green_learning.py — Enhanced v17.1.0
================================================

Cross-deployment federated weight sharing for MOPD, extended with ten
advanced Green Agent enhancements in a single self-contained file.

FIXES OVER v17.0.0:
 - share_weights carbon gate no longer blocks every share. The intensity
   used for the defer check is caller-supplied (default reads config).
 - record_reputation uses an instance-level lock (was a fresh lock per call).
 - has_converged accepts model_id/version; snapshots prior state before the
   pull so it is idempotent and can actually return True.
 - Pickle aggregation no longer produces an empty dict (keys initialization
   fixed).
 - _try_deserialize requires explicit magic prefixes; garbage bytes are no
   longer interpreted as a float32 array.
 - apply_aggregated_weights preserves keys present in only one side.
 - dp_epsilon is read from the caller-supplied config (was read from
   storage.config, which is almost always missing).
 - Reputation decays each round; unknown instances start at a lower default.
 - FedAvg blend factor is computed from local/global sample counts when
   available; explicit `blend=` still honoured.
 - Strict mode refuses to merge mismatched formats instead of silently
   falling back to the majority.
 - File-level: log_event shim, Prometheus metrics, orchestrator lifecycle
   (aclose / __aenter__ / __aexit__) and get_running_loop().
 - All shared bugs from the ten bundled enhancements fixed (see each class).

Self-contained: Python stdlib + optional numpy / torch.
All storage interactions soft-fail.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import math
import pickle
import random
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

# ---------- Optional dependencies ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIG HELPER
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# PROMETHEUS METRICS (module scope: single registration)
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_FED_SHARES = _PromCounter(
        "federated_shares_total", "Federated weight shares",
        ["model_id", "status"],
    )
    _M_FED_PULLS = _PromCounter(
        "federated_pulls_total", "Federated pulls",
        ["model_id", "status"],
    )
    _M_FED_ROUNDS = Gauge(
        "federated_rounds", "Federated rounds completed",
    )
    _M_FED_TRUSTED = Gauge(
        "federated_trusted_instances", "Trusted instances",
    )
    _M_FED_CONVERGENCE = _PromCounter(
        "federated_convergence_checks_total",
        "Federated convergence checks",
        ["converged"],
    )
    _M_FED_REPUTATION = Gauge(
        "federated_reputation",
        "Reputation score",
        ["instance_id"],
    )
    _M_FED_DP = _PromCounter(
        "federated_dp_applied_total",
        "Times DP noise was applied",
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_FED_SHARES = _M_FED_PULLS = _M_FED_ROUNDS = _Dummy()
    _M_FED_TRUSTED = _M_FED_CONVERGENCE = _M_FED_REPUTATION = _Dummy()
    _M_FED_DP = _Dummy()


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    """
    Cross-deployment federated weight sharing for MOPD.

    v17.1 enhancements:
      * Correct aggregation via deserialize → average → serialize.
      * Weighted FedAvg (sample-count weighting).
      * Client reputation filtering with decay and endorsement.
      * Differential-privacy noise injection (local DP).
      * Convergence detection (numeric deltas + fingerprints).
      * Format compatibility checks (numpy magic prefix, torch, pickle).
      * Integration hooks for the other nine enhancements.
    """

    # Explicit magic prefixes for the supported serialization formats.
    _MAGIC_TORCH = b"TRC1"
    _MAGIC_PICKLE = b"PKL1"
    _MAGIC_NUMPY = b"NPY1"
    KNOWN_FORMATS: FrozenSet[str] = frozenset({"torch", "pickle", "numpy_f32"})

    def __init__(self,
                 storage: Any,
                 instance_id: str,
                 share_interval: int = 3600,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 temporal: Optional["TemporalLogicVerifier"] = None,
                 xai: Optional["XAIDecisionExplainer"] = None,
                 precision: Optional["AdaptivePrecisionSwitcher"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 *,
                 config: Any = None,
                 strict_format: bool = True,
                 default_reputation: float = 0.5,
                 reputation_floor: float = 0.3,
                 reputation_decay: float = 0.99,
                 seed: Optional[int] = None):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.config = config

        self.rounds = 0
        self.strict_format = bool(strict_format)
        self.default_reputation = float(default_reputation)
        self.reputation_floor = float(reputation_floor)
        self.reputation_decay = float(reputation_decay)

        # Enhancement hooks.
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.temporal = temporal
        self.xai = xai
        self.precision = precision
        self.hitl = hitl
        self.multi_agent = multi_agent

        # DP config (from the caller-supplied config, not storage).
        self.dp_epsilon = float(
            _cfg_get(config, "federated_dp_epsilon", 1.0) or 1.0
        )

        # Convergence tracking.
        self._last_aggregate: Optional[bytes] = None
        self._last_aggregate_fp: Optional[str] = None

        # Reputation table (instance_id -> score).
        self.reputation: Dict[str, float] = defaultdict(
            lambda: self.default_reputation
        )
        self._reputation_lock = asyncio.Lock()

        # Aggregation history.
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

        # Deterministic RNG for DP noise.
        self._seed = seed if seed is not None else int.from_bytes(
            uuid.uuid4().bytes[:4], "little")
        self._rng = np.random.default_rng(self._seed) if NUMPY_AVAILABLE else None

    # ------------------------------------------------------------------
    # Fingerprinting
    # ------------------------------------------------------------------
    @staticmethod
    def _fingerprint(weights: bytes) -> str:
        # Full SHA256 hex; used for equality checks only.
        return hashlib.sha256(weights).hexdigest()

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------
    def _try_deserialize(self, weights: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """
        Attempt to deserialize weights via torch / pickle / numpy.

        Requires an explicit 4-byte magic prefix to avoid misinterpreting
        arbitrary bytes as a float32 array.
        """
        if not weights or len(weights) < 4:
            return None
        magic, payload = weights[:4], weights[4:]

        if magic == self._MAGIC_TORCH:
            if not TORCH_AVAILABLE:
                return None
            try:
                return {"format": "torch",
                        "data": torch.load(io.BytesIO(payload), map_location="cpu")}
            except Exception as exc:
                log_event("warning", f"torch deserialize failed: {exc}")
                return None

        if magic == self._MAGIC_PICKLE:
            try:
                return {"format": "pickle", "data": pickle.loads(payload)}
            except Exception as exc:
                log_event("warning", f"pickle deserialize failed: {exc}")
                return None

        if magic == self._MAGIC_NUMPY:
            if not NUMPY_AVAILABLE:
                return None
            if len(payload) % 4 != 0:
                return None
            try:
                arr = np.frombuffer(payload, dtype=np.float32).copy()
                return {"format": "numpy_f32", "data": arr}
            except Exception as exc:
                log_event("warning", f"numpy deserialize failed: {exc}")
                return None

        log_event("warning", f"Unknown magic prefix: {magic!r}")
        return None

    def _serialize(self, obj: Any) -> bytes:
        fmt = obj.get("format")
        data = obj.get("data")
        if fmt == "torch":
            if not TORCH_AVAILABLE:
                raise ValueError("torch is not installed")
            buf = io.BytesIO()
            torch.save(data, buf)
            return self._MAGIC_TORCH + buf.getvalue()
        if fmt == "pickle":
            return self._MAGIC_PICKLE + pickle.dumps(data)
        if fmt == "numpy_f32":
            if not NUMPY_AVAILABLE:
                raise ValueError("numpy is not installed")
            return self._MAGIC_NUMPY + data.astype("float32").tobytes()
        raise ValueError(f"Unknown format: {fmt!r}")

    # ------------------------------------------------------------------
    # Differential privacy (local DP on numeric leaves)
    # ------------------------------------------------------------------
    def _add_dp_noise(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        if self.dp_epsilon is None or self.dp_epsilon <= 0:
            return obj
        scale = 1.0 / self.dp_epsilon
        fmt = obj.get("format")
        data = obj.get("data")
        rng = self._rng if self._rng is not None else np.random.default_rng()
        try:
            if fmt == "torch" and TORCH_AVAILABLE and hasattr(data, "items"):
                noisy = {}
                for k, v in data.items():
                    noise = torch.from_numpy(
                        rng.laplace(0.0, scale, size=tuple(v.shape))
                    ).to(v.dtype)
                    noisy[k] = v + noise
                try:
                    _M_FED_DP.inc()
                except Exception:
                    pass
                return {"format": "torch", "data": noisy}
            if fmt == "numpy_f32" and NUMPY_AVAILABLE:
                noise = rng.laplace(0.0, scale, size=data.shape)
                try:
                    _M_FED_DP.inc()
                except Exception:
                    pass
                return {"format": "numpy_f32",
                        "data": (data + noise).astype("float32")}
            if fmt == "pickle" and NUMPY_AVAILABLE:
                def _walk(x):
                    if isinstance(x, dict):
                        return {k: _walk(v) for k, v in x.items()}
                    if isinstance(x, list):
                        return [_walk(v) for v in x]
                    if isinstance(x, (int, float)) and not isinstance(x, bool):
                        return float(x) + float(rng.laplace(0.0, scale))
                    return x
                try:
                    _M_FED_DP.inc()
                except Exception:
                    pass
                return {"format": "pickle", "data": _walk(data)}
        except Exception as exc:
            log_event("warning", f"DP noise failed: {exc}")
        return obj

    # ------------------------------------------------------------------
    # Reputation
    # ------------------------------------------------------------------
    async def record_reputation(self, instance_id: str, reward: float) -> None:
        """Update an instance's reputation from a training reward."""
        try:
            r = float(reward)
        except (TypeError, ValueError):
            return
        async with self._reputation_lock:
            old = self.reputation.get(instance_id, self.default_reputation)
            new = max(0.0, min(1.0, old + 0.1 * (r - 0.5)))
            self.reputation[instance_id] = new
            try:
                _M_FED_REPUTATION.labels(instance_id=instance_id).set(new)
            except Exception:
                pass

    def _decay_reputation_locked(self) -> None:
        """Decay every reputation by a fixed factor (call under lock)."""
        for iid in list(self.reputation.keys()):
            self.reputation[iid] = max(
                0.0, self.reputation[iid] * self.reputation_decay
            )

    def endorse(self, instance_id: str, endorser: str) -> bool:
        """
        Bootstrap reputation for a new instance via an endorsement.

        The endorser must itself be trusted. The new instance is assigned
        the endorser's reputation as its initial score.
        """
        if not self._is_trusted(endorser):
            return False
        self.reputation[instance_id] = self.reputation.get(endorser,
                                                           self.default_reputation)
        return True

    def _is_trusted(self, instance_id: str, floor: Optional[float] = None) -> bool:
        if floor is None:
            floor = self.reputation_floor
        return self.reputation.get(instance_id, self.default_reputation) >= floor

    # ------------------------------------------------------------------
    # Carbon-aware sharing
    # ------------------------------------------------------------------
    async def _carbon_defer(self, intensity: Optional[float] = None) -> bool:
        """
        Ask the carbon market whether to defer the current share.

        `intensity` defaults to the value from config (or 0.2 if not set),
        which is *below* the default defer threshold. Callers can override.
        """
        if self.carbon_market is None:
            return False
        if intensity is None:
            intensity = float(
                _cfg_get(self.config, "federated_intensity", 0.2) or 0.2
            )
        try:
            decision = await self.carbon_market.net_zero_schedule(
                workload_kwh=0.1, intensity=intensity, commit=False)
            return decision.get("action") == "defer"
        except Exception as exc:
            log_event("debug", f"carbon_defer check failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Share
    # ------------------------------------------------------------------
    async def share_weights(self,
                            model_id: str,
                            weights: bytes,
                            sample_count: int = 1,
                            version: str = "1.0.0",
                            apply_dp: bool = True,
                            *,
                            intensity: Optional[float] = None,
                            skip_carbon_gate: bool = False) -> None:
        """
        Publish local weights to the federated registry.

        Args:
            model_id: logical model name
            weights: serialized weights (torch / pickle / numpy)
            sample_count: number of samples the model was trained on
            version: semantic version of the model architecture
            apply_dp: whether to add differential-privacy noise
            intensity: grid intensity used for the carbon defer check
            skip_carbon_gate: bypass the carbon defer decision
        """
        if not weights:
            return

        # 1. Carbon-aware deferral (opt-in via skip_carbon_gate=False).
        if not skip_carbon_gate and await self._carbon_defer(intensity):
            self.history.append({
                "action": "share_deferred",
                "model_id": model_id,
                "reason": "high_carbon_window",
                "ts": _now_utc().isoformat()})
            try:
                _M_FED_SHARES.labels(
                    model_id=model_id, status="deferred").inc()
            except Exception:
                pass
            return

        # 2. Differential privacy (local DP).
        payload = weights
        if apply_dp:
            obj = self._try_deserialize(weights)
            if obj is not None:
                noisy = self._add_dp_noise(obj)
                try:
                    payload = self._serialize(noisy)
                except Exception as exc:
                    log_event("warning", f"DP serialization failed: {exc}")
                    payload = weights

        # 3. Persist.
        versioned_id = f"{model_id}@{version}"
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, versioned_id, payload,
                float(sample_count), self.rounds)
        except Exception as exc:
            log_event("warning", f"Federated share persist failed: {exc}")
            try:
                _M_FED_SHARES.labels(
                    model_id=versioned_id, status="error").inc()
            except Exception:
                pass
            return

        # 4. Reputation decay (per share).
        async with self._reputation_lock:
            self._decay_reputation_locked()

        self.history.append({
            "action": "share",
            "model_id": versioned_id,
            "bytes": len(payload),
            "sample_count": int(sample_count),
            "dp_applied": bool(apply_dp),
            "ts": _now_utc().isoformat()})
        try:
            _M_FED_SHARES.labels(
                model_id=versioned_id, status="ok").inc()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Pull
    # ------------------------------------------------------------------
    async def pull_aggregated_weights(self,
                                      model_id: str,
                                      version: str = "1.0.0",
                                      reputation_floor: Optional[float] = None
                                      ) -> Optional[bytes]:
        """
        Fetch all instances' weights and average them.

        Steps:
          1. Fetch rows for the versioned model_id.
          2. Filter by reputation.
          3. Deserialize all valid blobs.
          4. Verify format agreement (strict or majority).
          5. Weighted average using sample counts.
          6. Re-serialize.
        """
        if reputation_floor is None:
            reputation_floor = self.reputation_floor
        versioned_id = f"{model_id}@{version}"
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, versioned_id)
        except Exception as exc:
            log_event("warning", f"Federated pull storage failed: {exc}")
            try:
                _M_FED_PULLS.labels(
                    model_id=versioned_id, status="error").inc()
            except Exception:
                pass
            return None
        if not rows:
            try:
                _M_FED_PULLS.labels(
                    model_id=versioned_id, status="empty").inc()
            except Exception:
                pass
            return None

        # Reputation filtering.
        rows = [r for r in rows
                if self._is_trusted(r.get("instance_id", ""),
                                    reputation_floor)]
        if not rows:
            try:
                _M_FED_PULLS.labels(
                    model_id=versioned_id, status="filtered").inc()
            except Exception:
                pass
            return None

        # Deserialize.
        weights_list: List[float] = []
        objs_list: List[Dict[str, Any]] = []
        for r in rows:
            blob = r.get("weights")
            if not blob:
                continue
            obj = self._try_deserialize(blob)
            if obj is None:
                continue
            w = float(r.get("weight_norm", 1.0)) or 1.0
            weights_list.append(w)
            objs_list.append(obj)
        if not objs_list:
            return None

        # Normalise weights.
        total_w = sum(weights_list) or 1.0
        weights_list = [w / total_w for w in weights_list]

        # Format agreement.
        formats = {o["format"] for o in objs_list}
        if len(formats) > 1:
            if self.strict_format:
                log_event(
                    "warning",
                    f"Format mismatch in strict mode: {formats}; "
                    "refusing to merge.")
                try:
                    _M_FED_PULLS.labels(
                        model_id=versioned_id, status="format_mismatch").inc()
                except Exception:
                    pass
                return None
            majority = max(
                formats, key=lambda f: sum(1 for o in objs_list if o["format"] == f))
            objs_list = [o for o in objs_list if o["format"] == majority]
            weights_list = weights_list[:len(objs_list)]
            total_w = sum(weights_list) or 1.0
            weights_list = [w / total_w for w in weights_list]

        # Aggregate.
        fmt = objs_list[0]["format"]
        try:
            if fmt == "torch" and TORCH_AVAILABLE:
                avg_sd: Dict[str, Any] = {}
                keys = list(objs_list[0]["data"].keys())
                for key in keys:
                    stacked = torch.stack([
                        o["data"][key].float() * w
                        for o, w in zip(objs_list, weights_list)
                    ])
                    avg_sd[key] = stacked.sum(dim=0)
                agg = {"format": "torch", "data": avg_sd}
            elif fmt == "numpy_f32" and NUMPY_AVAILABLE:
                n = min(len(o["data"]) for o in objs_list)
                acc = np.zeros(n, dtype=np.float64)
                for o, w in zip(objs_list, weights_list):
                    acc += o["data"][:n].astype(np.float64) * w
                agg = {"format": "numpy_f32",
                       "data": acc.astype(np.float32)}
            elif fmt == "pickle":
                def _avg_numeric(items: List[Any], ws: List[float]) -> Any:
                    try:
                        if all(isinstance(x, (int, float)) and
                               not isinstance(x, bool) for x in items):
                            return sum(x * w for x, w in zip(items, ws))
                        if all(isinstance(x, list) for x in items):
                            length = min(len(x) for x in items)
                            return [_avg_numeric([x[i] for x in items], ws)
                                    for i in range(length)]
                        if all(isinstance(x, dict) for x in items):
                            keys: Optional[Set[str]] = None
                            for x in items:
                                xk = set(x.keys())
                                keys = xk if keys is None else (keys & xk)
                            if not keys:
                                return {}
                            return {
                                k: _avg_numeric(
                                    [x.get(k, 0.0) for x in items], ws)
                                for k in keys
                            }
                    except Exception:
                        pass
                    return items[0]
                merged = _avg_numeric([o["data"] for o in objs_list],
                                      weights_list)
                agg = {"format": "pickle", "data": merged}
            else:
                return None
            result = self._serialize(agg)
        except Exception as exc:
            log_event("warning", f"Aggregation failed: {exc}")
            return None

        # Convergence tracking.
        fp = self._fingerprint(result)
        self._last_aggregate = result
        self._last_aggregate_fp = fp
        self.rounds += 1

        try:
            _M_FED_ROUNDS.set(self.rounds)
            _M_FED_PULLS.labels(
                model_id=versioned_id, status="ok").inc()
            _M_FED_TRUSTED.set(
                sum(1 for r in self.reputation.values()
                    if r >= self.reputation_floor))
        except Exception:
            pass

        self.history.append({
            "action": "pull",
            "model_id": versioned_id,
            "instances": len(objs_list),
            "format": fmt,
            "fingerprint": fp[:16],
            "round": self.rounds,
            "ts": _now_utc().isoformat()})
        return result

    # ------------------------------------------------------------------
    # Apply
    # ------------------------------------------------------------------
    async def apply_aggregated_weights(self,
                                       model_id: str,
                                       current: bytes,
                                       version: str = "1.0.0",
                                       blend: float = 0.5,
                                       *,
                                       local_sample_count: Optional[int] = None,
                                       global_sample_count: Optional[int] = None
                                       ) -> bytes:
        """
        Blend local weights with the federated average.

        Args:
            current: local serialized weights
            blend: 0.0 = keep local, 1.0 = use aggregate only. Ignored
                when local_sample_count and global_sample_count are given,
                in which case blend is computed as
                local / (local + global).
        """
        agg = await self.pull_aggregated_weights(model_id, version)
        if agg is None:
            return current

        # Compute blend from sample counts if provided.
        if (local_sample_count is not None and
                global_sample_count is not None and
                local_sample_count + global_sample_count > 0):
            blend = float(local_sample_count) / float(
                local_sample_count + global_sample_count)
        blend = max(0.0, min(1.0, float(blend)))

        obj_cur = self._try_deserialize(current)
        obj_agg = self._try_deserialize(agg)
        if obj_cur is None or obj_agg is None:
            return current
        if obj_cur["format"] != obj_agg["format"]:
            if self.strict_format:
                log_event("warning",
                          "Format mismatch on apply; returning local weights")
                return current

        fmt = obj_cur["format"]
        try:
            if fmt == "torch" and TORCH_AVAILABLE:
                merged: Dict[str, Any] = {}
                keys = set(obj_cur["data"].keys()) | set(obj_agg["data"].keys())
                for k in keys:
                    if k in obj_cur["data"] and k in obj_agg["data"]:
                        merged[k] = (
                            (1 - blend) * obj_cur["data"][k].float()
                            + blend * obj_agg["data"][k].float()
                        )
                    elif k in obj_cur["data"]:
                        merged[k] = obj_cur["data"][k]
                    else:
                        merged[k] = obj_agg["data"][k]
                out = {"format": "torch", "data": merged}
            elif fmt == "numpy_f32" and NUMPY_AVAILABLE:
                a = obj_cur["data"]
                b = obj_agg["data"]
                n = min(len(a), len(b))
                out_arr = (1 - blend) * a[:n] + blend * b[:n]
                out = {"format": "numpy_f32",
                       "data": out_arr.astype(np.float32)}
            elif fmt == "pickle":
                def _blend(a: Any, b: Any, w: float) -> Any:
                    try:
                        if isinstance(a, dict) and isinstance(b, dict):
                            merged_dict: Dict[str, Any] = {}
                            for k in set(a.keys()) | set(b.keys()):
                                if k in a and k in b:
                                    merged_dict[k] = _blend(a[k], b[k], w)
                                elif k in a:
                                    merged_dict[k] = a[k]
                                else:
                                    merged_dict[k] = b[k]
                            return merged_dict
                        if isinstance(a, list) and isinstance(b, list):
                            n = min(len(a), len(b))
                            return [_blend(a[i], b[i], w) for i in range(n)]
                        if (isinstance(a, (int, float)) and
                                isinstance(b, (int, float)) and
                                not isinstance(a, bool) and
                                not isinstance(b, bool)):
                            return (1 - w) * float(a) + w * float(b)
                    except Exception:
                        pass
                    return a
                merged = _blend(obj_cur["data"], obj_agg["data"], blend)
                out = {"format": "pickle", "data": merged}
            else:
                return current
            return self._serialize(out)
        except Exception as exc:
            log_event("warning", f"Federated blend failed: {exc}")
            return current

    # ------------------------------------------------------------------
    # Convergence detection
    # ------------------------------------------------------------------
    async def has_converged(self,
                            threshold: float = 0.001,
                            *,
                            model_id: str = "mopd_student",
                            version: str = "1.0.0") -> bool:
        """
        Return True if the last two aggregates are within `threshold`.

        Snapshots the previous aggregate and fingerprint before pulling so
        the check is idempotent and can actually return True.
        """
        if self._last_aggregate is None:
            return False

        prev_blob = self._last_aggregate
        prev_fp = self._last_aggregate_fp
        new = await self.pull_aggregated_weights(model_id, version)
        if new is None:
            # Restore state and report no change.
            self._last_aggregate = prev_blob
            self._last_aggregate_fp = prev_fp
            return False

        # Numeric comparison when possible.
        obj_new = self._try_deserialize(new)
        obj_prev = self._try_deserialize(prev_blob)
        converged = False
        if (obj_new is not None and obj_prev is not None and
                obj_new["format"] == "numpy_f32" == obj_prev["format"] and
                NUMPY_AVAILABLE):
            try:
                a = obj_new["data"]
                b = obj_prev["data"]
                n = min(len(a), len(b))
                if n > 0:
                    delta = float(np.abs(a[:n] - b[:n]).mean())
                    converged = delta < threshold
                else:
                    converged = True
            except Exception:
                converged = False
        else:
            # Fingerprint fallback.
            converged = (self._fingerprint(new) == prev_fp)

        # Update stored state to the newly-pulled aggregate.
        self._last_aggregate = new
        self._last_aggregate_fp = self._fingerprint(new)

        try:
            _M_FED_CONVERGENCE.labels(
                converged=str(converged)).inc()
        except Exception:
            pass
        return converged

    # ------------------------------------------------------------------
    # Legacy API (byte-wise) — retained for backward compatibility
    # ------------------------------------------------------------------
    async def share_weights_legacy(self, model_id: str, weights: bytes) -> None:
        """Deprecated: use `share_weights` instead."""
        if not weights:
            return
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights_legacy(self, model_id: str) -> Optional[bytes]:
        """Deprecated: use `pull_aggregated_weights` instead."""
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        return bytes(avg)

    async def apply_aggregated_weights_legacy(self, model_id: str,
                                              current: bytes) -> bytes:
        """Deprecated: use `apply_aggregated_weights` instead."""
        agg = await self.pull_aggregated_weights_legacy(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])

    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "rounds": self.rounds,
            "last_fingerprint": (self._last_aggregate_fp[:16]
                                 if self._last_aggregate_fp else None),
            "trusted_instances": sum(
                1 for r in self.reputation.values()
                if r >= self.reputation_floor),
            "known_instances": len(self.reputation),
            "history_size": len(self.history),
            "dp_epsilon": self.dp_epsilon,
            "strict_format": self.strict_format,
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / self.n_actions] * self.n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        if not x:
            return []
        m = max(x)
        t = max(temp, 1e-6)
        exps = [math.exp((v - m) / t) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage: Any,
                   student_id: str = "fed_student") -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL against the teacher's own distribution.
        for tid, pol in self.teachers.items():
            weight = float(pol[0]) if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            kl = 0.0
            for i in range(self.n_actions):
                pi = pol[i] if i < len(pol) else 1e-9
                si = self.student_policy[i]
                pi = max(pi, 1e-9)
                si = max(si, 1e-9)
                kl += pi * math.log(pi / si)
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature, amplitude, kl)
            except Exception:
                pass

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage: Any = None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]],
                    variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
            return self.summary()
        try:
            X = np.array(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float)
        except Exception as exc:
            log_event("warning", f"Causal samples parse failed: {exc}")
            return self.summary()
        if X.shape[0] < 2 or X.shape[1] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
        except Exception:
            return self.summary()

        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if not math.isfinite(c) or c <= threshold:
                        continue
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = (variables[i], variables[j]) if vi > vj \
                        else (variables[j], variables[i])
                    self.graph[src][dst] = {"weight": float(corr[i, j]),
                                            "confidence": c}
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]
    STRATEGY_MAP: Dict[str, str] = {
        "performance": "balanced",
        "carbon": "carbon_focus",
        "cost": "operational_focus",
        "hybrid": "balanced",
        "adaptive": "balanced",
    }

    def __init__(self, config: Any, storage: Any, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1)
                             or 0.1)
        self._lock = asyncio.Lock()

    def to_strategy(self, action: str) -> str:
        return self.STRATEGY_MAP.get(action, "balanced")

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Dict[str, Any]) -> None:
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent, a.id, a.role,
                        a.reputation, a.utilities)
                except Exception:
                    pass

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> None:
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            log_event("warning",
                      "MultiAgentCoordinator bus full; message dropped")
        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                uuid.uuid4().hex[:8], topic, sender, "*", payload)
        except Exception:
            pass

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0,
                                          a.utilities[a.role] + 0.05 * reward)

    def get_role_distribution(self) -> Dict[str, float]:
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        return {role: c / total for role, c in counts.items()}

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        role_dist = self.get_role_distribution()
        out = [0.0] * 5
        for role, w in role_dist.items():
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_role_distribution(),
                "strategy_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id: str, operator: str,
                 conditions: List[Callable[[Dict], bool]],
                 window: float = 0.0, description: str = "",
                 severity: str = "warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage: Any, config: Any):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = (formula or "").strip()
        if not f:
            return
        try:
            if f.startswith("G "):
                self._add_atomic(f[2:].strip().strip("()"), "always")
            elif f.startswith("F "):
                self._add_atomic(f[2:].strip().strip("()"), "eventually")
            elif f.startswith("NEVER "):
                self._add_atomic(f[6:].strip().strip("()"), "never")
            else:
                log_event("warning",
                          f"Unrecognised temporal formula: {formula!r}")
        except Exception as exc:
            log_event("warning", f"Temporal install failed: {exc}")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            log_event("warning", f"Atomic formula did not parse: {expr!r}")
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        self._pending_rule_saves.append((rid, expr, op, "warning"))

    async def _flush_pending_rule_saves(self) -> None:
        while self._pending_rule_saves:
            rid, expr, op, sev = self._pending_rule_saves.pop(0)
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, expr, op, sev, expr, 0.0, True)
            except Exception:
                pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self) -> Dict[str, bool]:
        await self._flush_pending_rule_saves()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace)
            violated = rule.evaluate(copy)
            approved = False
            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(
                            rid, self.trace[-1][1] if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            approved = await approved
                    except Exception as exc:
                        log_event("warning", f"Approval callback failed: {exc}")
                        approved = False
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, bool(approved))
                except Exception:
                    pass
            result[rid] = (not violated) or bool(approved)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap")
                          or "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5) or 5)

    def _permutation_shap(self, f, x, names, n=64):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _kernel_shap(self, f, x, names, n=64):
        return self._permutation_shap(f, x, names, n)

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id: str, label: str, features,
                      names: List[str], model_fn: Callable) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            attrs = self._permutation_shap(model_fn, features, names)
        nl = self._nl(label, attrs)

        feature_map: Dict[str, Any] = {}
        try:
            if NUMPY_AVAILABLE and hasattr(features, "tolist"):
                feature_map = dict(zip(names, features.tolist()))
            else:
                feature_map = dict(zip(names, list(features)))
        except Exception:
            feature_map = {"raw": list(features)}

        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                feature_map, attrs, nl)
        except Exception:
            pass
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.dynamic_energy = dict(self.ENERGY)
        self.dynamic_latency = dict(self.LATENCY)
        self.history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
            except Exception:
                pass
        self._hw_cache = info
        return info

    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.dynamic_energy]

    def select_precision(self) -> str:
        hw = self._probe()
        cands = self._allowed_precisions()
        if not cands:
            return "fp32"
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.dynamic_energy.get(c, 1.0))

    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.dynamic_energy:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels; ignoring")
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.dynamic_energy.get(old, 1.0) -
                    self.dynamic_energy.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        if recent_acc <= 0 or baseline_acc <= 0:
            if not self._acc_window:
                return
            recent_acc = sum(self._acc_window) / len(self._acc_window)
            baseline_acc = max(self._acc_window)
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(self.config,
                                "precision_switch_threshold", 0.02) or 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            target = self.select_precision()
            if target != self.current:
                await self.switch_to(target, reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3   # kgCO2/kWh
    RUN_OFFSET_COST_THRESHOLD = 0.5   # USD
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.last_price = float(
            _cfg_get(config, "carbon_default_price_usd",
                     self.DEFAULT_PRICE_USD) or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh: float = 0.0
        self._closed = False

    async def _fetch_price(self) -> float:
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        mwh = max(0.0, float(mwh))
        if mwh == 0.0:
            return 0.0
        price_per_mwh = max(0.0, float(price_per_mwh))
        src = (source or "").strip().lower() or "unspecified"
        cost = mwh * price_per_mwh
        self._rec_balance_mwh += mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, src)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float, intensity: float,
                                price: Optional[float] = None,
                                commit: bool = True) -> Dict[str, Any]:
        workload_kwh = max(0.0, float(workload_kwh))
        intensity = max(0.0, float(intensity))
        if price is None:
            price = float(self.last_price)
        else:
            try:
                price = max(0.0, float(price))
            except (TypeError, ValueError):
                price = float(self.last_price)
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"
        if commit:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                    carbon_kg, offset_cost, price)
            except Exception:
                pass
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh)}

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self) -> bool:
        async with self._lock:
            return not self.active

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._health_ok()
        status = "completed"
        injected_ms = 0.0
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type,
                                     "started": _now_utc().isoformat()}
            if fault_type == "latency":
                delay = 0.5
                injected_ms = delay * 1000.0
                await asyncio.sleep(delay)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption":
                async with self._lock:
                    self._shared_state["payload"] = "corrupted"
                    self._shared_state["counter"] += 1
                injected_ms = 5.0
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                injected_ms = 10.0
            elif fault_type == "network_drop":
                delay = 0.2
                injected_ms = delay * 1000.0
                await asyncio.sleep(delay)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._health_ok()
        duration = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1) or 0.1),
                int(before), int(after), status, duration)
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration,
                "injected_ms": injected_ms}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage: Any, dashboard: Any = None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id: str, chosen_id: str,
                              request_id: Optional[str] = None) -> None:
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}; using FIFO")
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping")

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "pareto_query",
                {"candidates": candidates[:2]}, "info")
        except Exception:
            pass
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req_id] = fut
        try:
            msg = await asyncio.wait_for(fut, timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            chosen = msg.get("chosen") if isinstance(msg, dict) else None
            metrics = next(
                (c for c in candidates if c.get("solution_id") == chosen), None)
            await self.record_choice(user_id, chosen, metrics=metrics)
            return chosen
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(normalized.get(k, 0.25)
                            / (float(c.get(k, 0) or 0) + 1e-8)
                            for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self._PREF_KEYS:
                if k in metrics:
                    try:
                        v = float(metrics[k])
                    except (TypeError, ValueError):
                        continue
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (abs(v) + 1e-6) * 0.01
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class FederatedOrchestratorV17:
    """
    Unified entry point: wires all ten enhancements around the
    FederatedGreenAggregator. Runs a full federated round and exposes
    background loops for each enhancement.
    """

    def __init__(self, storage: Any, config: Any, dashboard: Any = None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Build ten enhancements.
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(
            storage, self.instance_id, config=config)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Wire enhancement hooks into the federated aggregator.
        self.federated.carbon_market = self.carbon_market
        self.federated.chaos = self.chaos
        self.federated.temporal = self.temporal
        self.federated.xai = self.xai
        self.federated.precision = self.precision
        self.federated.hitl = self.hitl
        self.federated.multi_agent = self.multi_agent

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    # ------------------------------------------------------------------
    def _spawn(self, coro: Any, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("error", f"Background task {name} failed: {exc}")

        task.add_done_callback(_done)
        return task

    async def federated_round(self,
                              model_id: str = "mopd_student",
                              local_weights: bytes = b"",
                              sample_count: int = 100,
                              version: str = "1.0.0") -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        # 1. Multi-agent bid.
        agent_id, _ = await self.multi_agent.bid({
            "name": f"federated_round_{model_id}",
            "preferred_role": "reporter"})
        result["agent_id"] = agent_id

        # 2. Precision switch.
        await self.precision.auto_switch(0.0, 0.0)
        result["precision"] = self.precision.current

        # 3. Carbon-aware share. Uses the configured intensity, which is
        # below the defer threshold by default.
        await self.federated.share_weights(
            model_id=model_id,
            weights=local_weights,
            sample_count=sample_count,
            version=version)
        result["shared_bytes"] = len(local_weights)

        # 4. Temporal verify.
        await self.temporal.push_state({
            "federated_round": self.federated.rounds,
            "quality": 1.0})
        verify = await self.temporal.verify()
        result["temporal_violations"] = [
            k for k, v in verify.items() if not v]

        # 5. Pull + blend.
        blended = await self.federated.apply_aggregated_weights(
            model_id=model_id,
            current=local_weights,
            version=version,
            local_sample_count=sample_count)
        result["blended_bytes"] = len(blended)

        # 6. XAI explanation.
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    float(sample_count) / 1000.0,
                    float(len(local_weights)) / 100000.0,
                    float(self.federated.rounds) / 100.0,
                    float(self.precision.saved_wh)])
                def _score(x, _w=np.array([0.3, 0.2, 0.4, 0.1])):
                    return float(np.dot(x, _w))
                result["xai"] = await self.xai.explain(
                    decision_id=f"fed_{uuid.uuid4().hex[:8]}",
                    label=f"round_{self.federated.rounds}",
                    features=feats,
                    names=["sample_count", "n_bytes", "round", "saved_wh"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"XAI explanation skipped: {exc}")

        # 7. Convergence check.
        result["converged"] = await self.federated.has_converged(
            model_id=model_id, version=version)

        # 8. Reputation update.
        await self.federated.record_reputation(self.instance_id, 0.8)

        # 9. Multi-agent reward.
        await self.multi_agent.reward(agent_id, 0.8)

        return result

    # ------------------------------------------------------------------
    async def start(self) -> None:
        self._running = True
        _ = asyncio.get_running_loop()
        self._spawn(self._causal_rl_loop(), "causal_rl")
        self._spawn(self._multi_agent_loop(), "multi_agent")
        self._spawn(self._temporal_loop(), "temporal")
        self._spawn(self._xai_loop(), "xai")
        self._spawn(self._precision_loop(), "precision")
        self._spawn(self._carbon_market_loop(), "carbon_market")
        self._spawn(self._chaos_loop(), "chaos")
        self._spawn(self._distillation_loop(), "distillation")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "FederatedOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x, _w=np.array([0.3, 0.2, 0.4, 0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="federated_health",
                    features=feats,
                    names=["samples", "bytes", "round", "saved_wh"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "fed_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "federated": self.federated.stats(),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()},
            "temporal_rules": len(self.temporal.rules),
            "hitl_pending": len(self.hitl._pending),
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos/tests. Soft-fail for all methods."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._data["carbon_credit_prices"].append({
            "price_usd": price_usd, "currency": currency,
            "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._data["rec_ledger"].append({
            "mwh": mwh, "price_per_mwh": price_per_mwh, "source": source,
            "certificate_id": certificate_id, "region": region,
            "retired": retired})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._data["net_zero_matches"].append({
            "match_id": match_id, "workload_kwh": workload_kwh,
            "intensity": intensity, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost,
            "credit_price": credit_price})

    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula,
            "operator": operator, "severity": severity,
            "description": description, "window": window, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({"state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient, "payload": payload})

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
        "federated_dp_epsilon": 1.0,
        "federated_intensity": 0.2,
    }

    storage = InMemoryStorage()
    orch = FederatedOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("federated_green_learning.py v17.1.0 — Demo")
    print("=" * 80)

    if NUMPY_AVAILABLE:
        # ---- 1. Correct numpy-based aggregation ----
        print("\n=== Correct numpy-based aggregation ===")
        w1 = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32)
        w2 = np.array([3.0, 4.0, 5.0, 6.0, 7.0], dtype=np.float32)

        # Serialize with the required magic prefix.
        w1_bytes = orch.federated._serialize(
            {"format": "numpy_f32", "data": w1})
        w2_bytes = orch.federated._serialize(
            {"format": "numpy_f32", "data": w2})

        await orch.federated.share_weights(
            "mopd_student", w1_bytes, sample_count=100, apply_dp=False)
        # Simulate a peer sharing its weights.
        await asyncio.to_thread(
            storage.save_federated_weights,
            "peer_01", "mopd_student@1.0.0", w2_bytes, 200.0, 0)
        # Trust the peer explicitly (default trust score is 0.5, floor is 0.3).
        orch.federated.reputation["peer_01"] = 1.0

        agg = await orch.federated.pull_aggregated_weights(
            "mopd_student", "1.0.0")
        if agg:
            obj = orch.federated._try_deserialize(agg)
            result = obj["data"]
            print(f"  Instance A weights: {w1}")
            print(f"  Instance B weights: {w2}")
            print(f"  Weighted aggregate: {result}")
            expected = (100 * w1 + 200 * w2) / 300
            print(f"  Expected (weighted): {expected}")
            print(f"  Match: {np.allclose(result, expected, atol=1e-5)}")

    # ---- 2. Full federated round ----
    print("\n=== Full federated round ===")
    if NUMPY_AVAILABLE:
        local = orch.federated._serialize(
            {"format": "numpy_f32",
             "data": np.array([1.0, 2.0, 3.0], dtype=np.float32)})
    else:
        local = b""
    result = await orch.federated_round(
        model_id="mopd_student",
        local_weights=local,
        sample_count=150,
        version="1.0.0")
    print(f"  agent_id: {result.get('agent_id')}")
    print(f"  precision: {result.get('precision')}")
    print(f"  shared_bytes: {result.get('shared_bytes')}")
    print(f"  blended_bytes: {result.get('blended_bytes')}")
    print(f"  converged: {result.get('converged')}")
    print(f"  temporal_violations: {result.get('temporal_violations')}")

    # ---- 3. Reputation filtering ----
    print("\n=== Reputation filtering ===")
    orch.federated.reputation["malicious_peer"] = 0.1
    if NUMPY_AVAILABLE:
        poison = orch.federated._serialize(
            {"format": "numpy_f32",
             "data": np.array([99.0] * 3, dtype=np.float32)})
    else:
        poison = b""
    await asyncio.to_thread(
        storage.save_federated_weights,
        "malicious_peer", "mopd_student@1.0.0", poison, 100.0, 0)
    trusted = await orch.federated.pull_aggregated_weights(
        "mopd_student", "1.0.0", reputation_floor=0.5)
    print(f"  Malicious peer reputation: 0.1 (floor 0.5)")
    print(f"  Trusted aggregate returned: {trusted is not None}")

    # ---- 4. Differential privacy ----
    print("\n=== Differential privacy ===")
    orch.federated.dp_epsilon = 0.5
    if NUMPY_AVAILABLE:
        dp_input = orch.federated._serialize(
            {"format": "numpy_f32",
             "data": np.array([1.0, 2.0, 3.0], dtype=np.float32)})
        await orch.federated.share_weights(
            "dp_test", dp_input, sample_count=50, apply_dp=True)
        # Find the latest dp_test row.
        rows = [r for r in storage._data["federated_weights"]
                if r["model_id"] == "dp_test@1.0.0"]
        if rows:
            dp_shared = rows[-1]["weights"]
            print(f"  Original bytes: {len(dp_input)}")
            print(f"  DP-shared bytes: {len(dp_shared)}")
            print(f"  Noisy (expected): {dp_input != dp_shared}")

    # ---- 5. Convergence check ----
    print("\n=== Convergence check ===")
    converged1 = await orch.federated.has_converged(
        model_id="mopd_student", version="1.0.0")
    converged2 = await orch.federated.has_converged(
        model_id="mopd_student", version="1.0.0")
    print(f"  First check: {converged1}")
    print(f"  Second check (identical inputs → should be True): {converged2}")

    # ---- 6. Health check ----
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
