"""
active_rlhf.py
Human-in-the-loop active learning for MOPD.
"""
from __future__ import annotations
import asyncio
import uuid
from typing import Any, Dict, List, Optional


class ActiveUserPreferenceLearner:
    def __init__(self, storage, pareto_gating, dashboard=None):
        self.storage = storage
        self.pareto = pareto_gating
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id: str, chosen_id: str) -> None:
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        try:
            q = [c.get("quality_score", 0) for c in candidates[:2]]
            if abs(q[0] - q[1]) / max(q) > 0.05:
                return None
        except Exception:
            return None
        req_id = uuid.uuid4().hex[:8]
        await asyncio.to_thread(
            self.storage.enqueue_hitl_request,
            req_id, "pareto_query", {"candidates": candidates[:2]}, "info")
        if self.dashboard:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query", "user_id": user_id,
                    "options": [{"id": c.get("solution_id"),
                                 "quality": c.get("quality_score")}
                                for c in candidates[:2]]})
            except Exception:
                pass
        try:
            msg = await asyncio.wait_for(self._responses.get(), timeout=timeout)
            await asyncio.to_thread(
                self.storage.resolve_hitl_request, req_id, "approved")
            return msg.get("chosen")
        except asyncio.TimeoutError:
            await asyncio.to_thread(
                self.storage.resolve_hitl_request, req_id, "timeout")
            weights = self.preferences.get(user_id, {})
            if weights:
                scored = []
                for c in candidates:
                    s = sum(weights.get(k, 0.25) / (c.get(k, 0) + 1e-8)
                            for k in ("quality_score", "carbon_g",
                                      "cost_usd", "latency_ms"))
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k, v in metrics.items():
                prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        await asyncio.to_thread(
            self.storage.save_user_preference, user_id, prefs)
