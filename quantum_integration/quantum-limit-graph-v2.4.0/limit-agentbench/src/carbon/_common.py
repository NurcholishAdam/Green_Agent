"""Shared value and timestamp helpers for carbon modules."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return _ensure_aware(raw)
    if isinstance(raw, str):
        value = raw.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            return _ensure_aware(datetime.fromisoformat(value))
        except ValueError:
            return _utcnow()
    return _utcnow()


def _json_safe(obj: Any) -> Any:
    """Make values JSON-compatible, converting non-finite floats to null."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, datetime):
        return _ensure_aware(obj).isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(key): _json_safe(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(value) for value in obj]
    return obj


def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    """Read a key from mapping, Pydantic, or object-based configuration."""
    if config is None:
        return default
    if isinstance(config, Mapping):
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


def _safe_mean(
    values: Iterable[Optional[float]], default: float = 0.0
) -> float:
    """Mean of finite numeric values, ignoring invalid values."""
    items: List[float] = []
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            items.append(number)
    if not items:
        return default
    try:
        return statistics.mean(items)
    except statistics.StatisticsError:
        return default
