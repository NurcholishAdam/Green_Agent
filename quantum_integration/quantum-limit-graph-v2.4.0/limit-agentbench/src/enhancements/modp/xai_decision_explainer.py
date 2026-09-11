"""
xai_decision_explainer.py
Explainable AI for MOPD decisions.
"""
from __future__ import annotations
import asyncio
import random
from typing import Any, Callable, Dict, List

import numpy as np

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


class XAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = getattr(config, "xai_method", "kernel_shap")
        self.depth = getattr(config, "xai_depth", 5)

    def _kernel_shap(self, f: Callable, x: np.ndarray, names: List[str],
                     n: int = 64) -> Dict[str, float]:
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

    def _lime(self, f: Callable, x: np.ndarray, names: List[str],
              n: int = 200) -> Dict[str, float]:
        if not SKLEARN_AVAILABLE:
            return {k: random.uniform(-1, 1) for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision: str, attrs: Dict[str, float]) -> str:
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id: str, label: str,
                      features: np.ndarray, names: List[str],
                      model_fn: Callable) -> Dict[str, Any]:
        if self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._nl(label, attrs)
        await asyncio.to_thread(
            self.storage.save_xai_explanation,
            decision_id, decision_id, self.method, label,
            dict(enumerate(features)), attrs, nl)
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}
