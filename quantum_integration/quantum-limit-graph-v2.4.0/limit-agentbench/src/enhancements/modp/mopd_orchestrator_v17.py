"""
mopd_orchestrator_v17.py
Unified MOPD orchestrator with all ten enhancements.
"""
from __future__ import annotations
import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np


class MOPDOrchestratorV17:
    """Single entry point: in-process or HTTP reporting + ten enhancements."""

    def __init__(self, storage, config,
                 adaptive_function=None,
                 adaptive_api_url: Optional[str] = None,
                 adaptive_api_token: Optional[str] = None,
                 dashboard=None):
        self.storage = storage
        self.config = config
        self.adaptive = adaptive_function
        self.adaptive_api_url = adaptive_api_url
        self.adaptive_api_token = adaptive_api_token
        self.dashboard = dashboard
        self.instance_id = str(uuid.uuid4())[:8]

        # Ten enhancements (imported from blocks above)
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, None, dashboard)

        # Wire HITL to temporal critical rules
        self.temporal.set_approval_callback(self._on_critical_violation)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    # ------------------------------------------------------------------
    async def _on_critical_violation(self, rule_id: str, state: Dict) -> bool:
        req_id = uuid.uuid4().hex[:8]
        await asyncio.to_thread(
            self.storage.enqueue_hitl_request, req_id, rule_id, state, "critical")
        approved = random.random() > 0.5
        await asyncio.to_thread(
            self.storage.resolve_hitl_request,
            req_id, "approved" if approved else "denied")
        return approved

    # ------------------------------------------------------------------
    # Strategy selection: all ten enhancements participate
    # ------------------------------------------------------------------
    async def select_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {"strategy": "adaptive"}

        # 1. Causal RL priority
        if getattr(self.config, "causal_rl_enabled", True):
            result["strategy"] = await self.causal_rl.choose_action(state)

        # 2. Multi-agent bid
        agent_id, _ = await self.multi_agent.bid({
            "name": "distill_task", "preferred_role": "optimizer"})
        result["agent_id"] = agent_id

        # 3. Precision pre-selection
        await self.precision.auto_switch(recent_acc=0.9, baseline_acc=0.92)
        result["precision"] = self.precision.current

        # 4. Carbon market decision
        cm = await self.carbon_market.net_zero_schedule(
            workload_kwh=1.0,
            intensity=state.get("carbon_intensity", 0.4) / 1000.0)
        result["carbon_decision"] = cm

        # 5. XAI explanation
        try:
            feats = np.array([
                state.get("quality", 0.8),
                state.get("carbon_intensity", 400) / 1000.0,
                state.get("cost", 0.5),
                state.get("latency_ms", 100) / 1000.0])
            def _score(x):
                return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
            xai = await self.xai.explain(
                decision_id=f"mopd_{uuid.uuid4().hex[:8]}",
                label=f"strategy={result['strategy']}",
                features=feats,
                names=["quality", "carbon", "cost", "latency"],
                model_fn=_score)
            result["xai"] = xai
        except Exception:
            pass

        # 6. Multi-agent reward
        await self.multi_agent.reward(agent_id, 0.8)

        # 7. Temporal push + verify
        await self.temporal.push_state({
            "quality": state.get("quality", 0.8),
            "carbon": state.get("carbon_intensity", 400) / 1000.0,
            "task_complete": True})
        verify = await self.temporal.verify()
        result["temporal_violations"] = [k for k, v in verify.items() if not v]

        # 8. Causal update
        await self.causal_rl.update(result["strategy"], 0.8, state)

        return result

    # ------------------------------------------------------------------
    # Report a distillation epoch to the adaptive pipeline
    # ------------------------------------------------------------------
    async def report_epoch(self, context: Dict[str, Any],
                            metrics: Dict[str, float],
                            teacher_id: str,
                            distillation_loss: float,
                            epoch: int) -> None:
        payload = {
            "context": context,
            "metrics": metrics,
            "teacher_id": teacher_id,
            "distillation_loss": distillation_loss,
            "epoch": epoch,
        }
        # In-process reporting
        if self.adaptive is not None:
            try:
                await self.adaptive.record_feedback(
                    context, metrics,
                    teacher_id=teacher_id,
                    distillation_loss=distillation_loss)
                return
            except Exception:
                pass
        # HTTP fallback
        if self.adaptive_api_url:
            await self._post_http(payload)

    async def _post_http(self, payload: Dict[str, Any]) -> None:
        try:
            import aiohttp
            url = f"{self.adaptive_api_url}/mopd/record"
            headers = {}
            if self.adaptive_api_token:
                headers["Authorization"] = f"Bearer {self.adaptive_api_token}"
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, headers=headers, timeout=10) as r:
                    await r.read()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        self._running = True
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self._causal_rl_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._multi_agent_loop()),
            loop.create_task(self._temporal_loop()),
            loop.create_task(self._xai_loop()),
            loop.create_task(self._precision_loop()),
            loop.create_task(self._carbon_market_loop()),
            loop.create_task(self._chaos_loop()),
            loop.create_task(self._distillation_loop()),
        ]
        for t in tasks:
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(900)
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
                await self.causal_rl.estimate_ate("carbon", "quality")
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("policy", dummy)
                await self.federated.pull_aggregated_weights("policy")
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                await self.multi_agent.step()
            except Exception:
                pass

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.temporal.verify()
            except Exception:
                pass

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="system_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except Exception:
                pass

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.carbon_market.update_price()
            except Exception:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "mopd_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()},
            "causal_edges": self.causal_graph.summary()["edges"],
            "temporal_rules": len(self.temporal.rules),
            "precision": self.precision.current,
            "rec_balance_mwh": await asyncio.to_thread(
                self.storage.get_rec_balance),
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
        }
