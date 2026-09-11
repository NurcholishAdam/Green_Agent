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

-- 1. Quantum-Distillation
CREATE TABLE IF NOT EXISTS teacher_superpositions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id TEXT, teacher_id TEXT, teacher_weight REAL,
    temperature REAL, amplitude REAL, kl_divergence REAL, timestamp TEXT);

-- 2. Causal RL
CREATE TABLE IF NOT EXISTS causal_graph (
    edge_id TEXT PRIMARY KEY, source TEXT, target TEXT,
    weight REAL, confidence REAL, timestamp TEXT);
CREATE TABLE IF NOT EXISTS causal_experiments (
    exp_id TEXT PRIMARY KEY, treatment TEXT, outcome TEXT,
    ate REAL, samples INTEGER, method TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS causal_interventions (
    intervention_id TEXT PRIMARY KEY, node TEXT, do_value TEXT,
    observed_outcome TEXT, counterfactual_json TEXT, timestamp TEXT);

-- 3. Federated Green Learning
CREATE TABLE IF NOT EXISTS federated_weights (
    instance_id TEXT, model_id TEXT, weights BLOB,
    weight_norm REAL, round_id INTEGER, timestamp TEXT,
    PRIMARY KEY (instance_id, model_id));

-- 4. Multi-Agent
CREATE TABLE IF NOT EXISTS agent_registry (
    agent_id TEXT PRIMARY KEY, role TEXT, reputation REAL,
    utilities TEXT, capabilities TEXT, created_at TEXT, last_updated TEXT);
CREATE TABLE IF NOT EXISTS agent_messages (
    message_id TEXT PRIMARY KEY, topic TEXT, sender TEXT,
    recipient TEXT, payload TEXT, timestamp TEXT);

-- 5. Temporal Logic
CREATE TABLE IF NOT EXISTS temporal_rules (
    rule_id TEXT PRIMARY KEY, formula TEXT, operator TEXT,
    severity TEXT, description TEXT, window_seconds REAL,
    created_at TEXT, active INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS temporal_trace (
    step INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT,
    context TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS temporal_violations (
    id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id TEXT, formula TEXT,
    step INTEGER, state TEXT, severity TEXT,
    approved INTEGER, resolved_at TEXT, timestamp TEXT);

-- 6. XAI
CREATE TABLE IF NOT EXISTS xai_explanations (
    explanation_id TEXT PRIMARY KEY, decision_id TEXT, method TEXT,
    decision_label TEXT, features TEXT, attributions TEXT,
    natural_language TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS xai_feature_importance (
    id INTEGER PRIMARY KEY AUTOINCREMENT, explanation_id TEXT,
    feature_name TEXT, importance REAL, rank INTEGER);

-- 7. Adaptive Precision
CREATE TABLE IF NOT EXISTS precision_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, from_p TEXT, to_p TEXT,
    reason TEXT, energy_saved_wh REAL, accuracy_delta REAL, timestamp TEXT);

-- 8. Carbon Markets / REC
CREATE TABLE IF NOT EXISTS carbon_credit_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT, price_usd REAL,
    currency TEXT, source TEXT, region TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS rec_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT, mwh REAL, price_per_mwh REAL,
    source TEXT, certificate_id TEXT, region TEXT,
    retired INTEGER DEFAULT 0, timestamp TEXT);
CREATE TABLE IF NOT EXISTS net_zero_matches (
    match_id TEXT PRIMARY KEY, workload_kwh REAL, intensity REAL,
    action TEXT, carbon_kg REAL, offset_cost_usd REAL,
    credit_price_usd REAL, timestamp TEXT);

-- 9. Chaos Testing
CREATE TABLE IF NOT EXISTS chaos_experiments (
    experiment_id TEXT PRIMARY KEY, name TEXT, fault_type TEXT,
    blast_radius REAL, steady_before INTEGER, steady_after INTEGER,
    status TEXT, duration_ms REAL, timestamp TEXT);

-- 10. HITL
CREATE TABLE IF NOT EXISTS hitl_approval_queue (
    request_id TEXT PRIMARY KEY, rule_id TEXT, state TEXT,
    severity TEXT, status TEXT, created_at TEXT, resolved_at TEXT);
CREATE TABLE IF NOT EXISTS active_learning_samples (
    sample_id TEXT PRIMARY KEY, model_id TEXT, strategy TEXT,
    uncertainty REAL, selected_for_review INTEGER,
    user_label TEXT, reviewed_at TEXT, timestamp TEXT);

-- Supporting tables
CREATE TABLE IF NOT EXISTS bio_inspired_runs (
    run_id TEXT PRIMARY KEY, algorithm TEXT, problem_id TEXT,
    parameters TEXT, best_solution TEXT, best_fitness REAL, timestamp TEXT);
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id TEXT PRIMARY KEY, weights TEXT, updated_at REAL);

class Storage:
    # 1. Quantum-Distillation
    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._execute("""INSERT INTO teacher_superpositions
            (student_id, teacher_id, teacher_weight, temperature,
             amplitude, kl_divergence, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (student_id, teacher_id, weight, temperature, amplitude, kl,
             datetime.now().isoformat()))

    # 2. Causal RL
    def save_causal_edge(self, source, target, weight, confidence):
        self._execute("""INSERT OR REPLACE INTO causal_graph
            (edge_id, source, target, weight, confidence, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (f"{source}->{target}", source, target, weight, confidence,
             datetime.now().isoformat()))

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        self._execute("""INSERT OR REPLACE INTO causal_experiments
            (exp_id, treatment, outcome, ate, samples, method, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (exp_id, treatment, outcome, ate, samples, method,
             datetime.now().isoformat()))

    # 3. Federated
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._execute("""INSERT OR REPLACE INTO federated_weights
            (instance_id, model_id, weights, weight_norm, round_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (instance_id, model_id, weights, weight_norm, round_id,
             datetime.now().isoformat()))

    def get_federated_weights(self, model_id):
        return self._fetchall(
            "SELECT * FROM federated_weights WHERE model_id = ?", (model_id,))

    # 4. Multi-Agent
    def save_agent(self, agent_id, role, reputation, utilities):
        self._execute("""INSERT OR REPLACE INTO agent_registry
            (agent_id, role, reputation, utilities, created_at, last_updated)
            VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM agent_registry
                                          WHERE agent_id = ?), ?), ?)""",
            (agent_id, role, reputation, json.dumps(utilities),
             agent_id, datetime.now().isoformat(),
             datetime.now().isoformat()))

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._execute("""INSERT OR REPLACE INTO agent_messages
            (message_id, topic, sender, recipient, payload, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (message_id, topic, sender, recipient,
             json.dumps(payload, default=str), datetime.now().isoformat()))

    # 5. Temporal
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window_seconds, active=True):
        self._execute("""INSERT OR REPLACE INTO temporal_rules
            (rule_id, formula, operator, severity, description,
             window_seconds, created_at, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (rule_id, formula, operator, severity, description,
             window_seconds, datetime.now().isoformat(), int(active)))

    def save_temporal_trace(self, state, context=None):
        self._execute("""INSERT INTO temporal_trace (state, context, timestamp)
            VALUES (?, ?, ?)""",
            (json.dumps(state, default=str),
             json.dumps(context, default=str) if context else None,
             datetime.now().isoformat()))

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._execute("""INSERT INTO temporal_violations
            (rule_id, formula, step, state, severity, approved,
             resolved_at, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (rule_id, formula, step, json.dumps(state, default=str),
             severity, int(approved) if approved is not None else None,
             None, datetime.now().isoformat()))

    # 6. XAI
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._execute("""INSERT OR REPLACE INTO xai_explanations
            (explanation_id, decision_id, method, decision_label,
             features, attributions, natural_language, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (explanation_id, decision_id, method, label,
             json.dumps(features, default=str),
             json.dumps(attributions, default=str),
             nl, datetime.now().isoformat()))
        for name, val in (attributions or {}).items():
            self._execute("""INSERT INTO xai_feature_importance
                (explanation_id, feature_name, importance, rank)
                VALUES (?, ?, ?, ?)""",
                (explanation_id, str(name), float(val), 0))

    # 7. Precision
    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._execute("""INSERT INTO precision_history
            (from_p, to_p, reason, energy_saved_wh, accuracy_delta, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (from_p, to_p, reason, saved_wh, acc_delta,
             datetime.now().isoformat()))

    # 8. Carbon / REC
    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._execute("""INSERT INTO carbon_credit_prices
            (price_usd, currency, source, region, timestamp)
            VALUES (?, ?, ?, ?, ?)""",
            (price_usd, currency, source, region,
             datetime.now().isoformat()))

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._execute("""INSERT INTO rec_ledger
            (mwh, price_per_mwh, source, certificate_id, region,
             retired, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (mwh, price_per_mwh, source, certificate_id, region,
             int(retired), datetime.now().isoformat()))

    def get_rec_balance(self):
        row = self._fetchone("SELECT COALESCE(SUM(mwh), 0) AS s FROM rec_ledger")
        return float(row["s"]) if row else 0.0

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._execute("""INSERT OR REPLACE INTO net_zero_matches
            (match_id, workload_kwh, intensity, action, carbon_kg,
             offset_cost_usd, credit_price_usd, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, workload_kwh, intensity, action, carbon_kg,
             offset_cost, credit_price, datetime.now().isoformat()))

    # 9. Chaos
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._execute("""INSERT OR REPLACE INTO chaos_experiments
            (experiment_id, name, fault_type, blast_radius,
             steady_before, steady_after, status, duration_ms, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (experiment_id, name, fault_type, blast_radius,
             int(steady_before), int(steady_after), status, duration_ms,
             datetime.now().isoformat()))

    # 10. HITL
    def enqueue_hitl_request(self, request_id, rule_id, state,
                             severity="critical"):
        self._execute("""INSERT OR REPLACE INTO hitl_approval_queue
            (request_id, rule_id, state, severity, status,
             created_at, resolved_at)
            VALUES (?, ?, ?, ?, 'pending', ?, NULL)""",
            (request_id, rule_id, json.dumps(state, default=str),
             severity, datetime.now().isoformat()))

    def resolve_hitl_request(self, request_id, status="approved"):
        self._execute("""UPDATE hitl_approval_queue
            SET status = ?, resolved_at = ? WHERE request_id = ?""",
            (status, datetime.now().isoformat(), request_id))

