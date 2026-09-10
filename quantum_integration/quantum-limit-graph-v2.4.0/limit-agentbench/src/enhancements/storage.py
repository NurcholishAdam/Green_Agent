#!/usr/bin/env python3
"""
Central Storage module for Green Agent enhancements – Version 5.0.0

Provides persistent SQLite storage with tables for:
- Model weights (BLOB)
- Feedback events (canonical MOPD feedback)
- Drift states (BLOBs for online/offline weights)
- Benchmark runs
- Power readings
- Elasticity metrics
- Substitution results
- Federated rounds
- Circularity records
- Emission records
- Optimisation history
- Distribution history
- Thermal optimizations
- Generic key-value state
- Post‑quantum cryptography keys (with AES‑GCM encryption)
- LIMIT Graph nodes, edges, and constraints
- RLHF feedback buffer and reward model metadata
- Multi‑Teacher Policy Distillation student policy and teacher history

NEW IN v5.0.0 (over v4.0.0):
- Quantum‑Distillation state (QAOA parameters, QUBO penalties, VQE energy)
- Causal RL (structural causal graphs, counterfactual estimates, ATE history)
- Federated Green Learning (global models, client contributions, staleness)
- Multi‑Agent Coordination (agent registry, role assignments, agent messages)
- Temporal Logic Verification (formulas, history, violations)
- Explainable AI (feature attributions, narratives, weights used)
- Adaptive Precision Switching (selections, policy state, energy savings)
- Carbon Markets + RECs (trades, certificates, market prices, retirement records)
- Chaos Testing (schedules, results, recovery actions)
- Human‑in‑the‑Loop (pending/resolved queries, audit log, active learning samples)
- Full async wrappers for all new methods
- Retention policy support for all new tables
- Statistics for the expanded schema
"""

from __future__ import annotations

import json
import sqlite3
import time
import os
import secrets
import asyncio
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import asdict, dataclass

# Attempt to import cryptography for encryption
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# Optional aiosqlite for true async
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# Try to reuse repo configuration
try:
    from . import config as _config
    DEFAULT_DB = getattr(_config, "DB_PATH", "enhancements_storage.db")
except Exception:
    try:
        import config as _config
        DEFAULT_DB = getattr(_config, "DB_PATH", "enhancements_storage.db")
    except Exception:
        DEFAULT_DB = "enhancements_storage.db"


class Storage:
    """
    Persistent SQLite storage for enhancements with async wrappers.

    Features (v5.0.0):
    - Connection pooling (thread-local) and optional aiosqlite for async.
    - WAL mode and foreign keys.
    - Schema versioning with incremental migrations (v1..v5).
    - Optional AES-GCM encryption for sensitive BLOB fields.
    - Comprehensive error handling and logging.
    - Async methods using aiosqlite (if available) or thread-pool.
    - Dedicated tables for every Green Agent enhancement area:
        * MODP, MoE, GA, Pareto, RLHF, Distillation, LIMIT Graph (v3–v4)
        * Quantum Distillation, Causal RL, Federated, Multi-Agent, Temporal Logic,
          XAI, Adaptive Precision, Carbon Markets/RECs, Chaos Testing, HITL (v5)
    - Configurable data retention policies for all tables.
    - Storage statistics.
    """

    # Schema version – increment when tables change
    SCHEMA_VERSION = 5

    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        self.db_path = Path(db_path or DEFAULT_DB)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Encryption master key (optional)
        self.master_key: Optional[bytes] = None
        key_hex = os.getenv("STORAGE_MASTER_KEY")
        if key_hex:
            try:
                self.master_key = bytes.fromhex(key_hex)
                if len(self.master_key) != 32:
                    raise ValueError("Master key must be 32 bytes")
            except Exception as e:
                print(f"WARNING: Invalid master key – encryption disabled: {e}")
                self.master_key = None

        # Connection pool: thread-local storage for sync mode
        self._local = threading.local()

        # Async connection (if aiosqlite available) – created per call
        self._async_conn = None

        # Statistics
        self._stats: Dict[str, Any] = {
            "table_sizes": {},
            "last_cleanup": None,
            "total_queries": 0,
        }

        self._init_db()

    # --------------------------------------------------------------------------
    # Connection management
    # --------------------------------------------------------------------------
    def _get_connection(self) -> sqlite3.Connection:
        """Return a thread-local SQLite connection (pooled)."""
        if not hasattr(self._local, 'conn'):
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=5000;")
            self._local.conn = conn
        return self._local.conn

    async def _get_async_connection(self) -> "aiosqlite.Connection":
        """Return an async connection (aiosqlite)."""
        if AIOSQLITE_AVAILABLE:
            conn = await aiosqlite.connect(self.db_path)
            await conn.execute("PRAGMA journal_mode=WAL;")
            await conn.execute("PRAGMA foreign_keys=ON;")
            await conn.execute("PRAGMA busy_timeout=5000;")
            return conn
        else:
            raise RuntimeError("aiosqlite not available")

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute SQL with error handling."""
        try:
            conn = self._get_connection()
            cursor = conn.execute(sql, params)
            conn.commit()
            self._stats["total_queries"] += 1
            return cursor
        except sqlite3.Error as e:
            print(f"Storage SQL error: {e} (sql: {sql}, params: {params})")
            raise

    async def _execute_async(self, sql: str, params: tuple = ()):
        """Async execute using aiosqlite or thread pool."""
        if AIOSQLITE_AVAILABLE:
            async with await self._get_async_connection() as conn:
                cursor = await conn.execute(sql, params)
                await conn.commit()
                return cursor
        else:
            return await asyncio.to_thread(self._execute, sql, params)

    async def _fetchone_async(self, sql: str, params: tuple = ()):
        if AIOSQLITE_AVAILABLE:
            async with await self._get_async_connection() as conn:
                cursor = await conn.execute(sql, params)
                row = await cursor.fetchone()
                return dict(row) if row else None
        else:
            return await asyncio.to_thread(self._fetchone, sql, params)

    async def _fetchall_async(self, sql: str, params: tuple = ()):
        if AIOSQLITE_AVAILABLE:
            async with await self._get_async_connection() as conn:
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        else:
            return await asyncio.to_thread(self._fetchall, sql, params)

    def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        cursor = self._execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        cursor = self._execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    # --------------------------------------------------------------------------
    # Schema initialisation and versioning (incremental migrations)
    # --------------------------------------------------------------------------
    def _init_db(self) -> None:
        """Create tables and apply migrations incrementally."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                )
            """)
            cur = conn.execute("SELECT version FROM schema_version")
            row = cur.fetchone()
            current_version = row[0] if row else 0

            if current_version < self.SCHEMA_VERSION:
                for v in range(current_version + 1, self.SCHEMA_VERSION + 1):
                    migration_method = getattr(self, f"_migrate_to_v{v}", None)
                    if migration_method:
                        migration_method(conn)
                conn.execute("DELETE FROM schema_version")
                conn.execute("INSERT INTO schema_version (version) VALUES (?)",
                             (self.SCHEMA_VERSION,))
                conn.commit()

    def _migrate_to_v1(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS model_weights (
            model_id TEXT PRIMARY KEY, weights BLOB, timestamp REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS feedback_events (
            event_id TEXT PRIMARY KEY, timestamp REAL NOT NULL, task_id TEXT NOT NULL,
            model_id TEXT, teacher_id TEXT, selected_action TEXT NOT NULL,
            quality_score REAL NOT NULL, latency_ms REAL NOT NULL,
            energy_joules REAL NOT NULL, carbon_g REAL NOT NULL,
            helium_cost REAL, resource_usage TEXT, distillation_loss REAL,
            feedback_type TEXT NOT NULL, adaptive_cost_value REAL NOT NULL,
            metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS drift_states (
            snapshot_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
            online_weights BLOB, offline_weights BLOB, cost_score REAL, reason TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS benchmark_runs (
            run_id TEXT PRIMARY KEY, timestamp REAL NOT NULL, policy_name TEXT NOT NULL,
            avg_quality REAL, avg_carbon REAL, avg_latency REAL, avg_cost REAL,
            total_energy REAL, sample_count INTEGER)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS pqc_keys (
            key_id TEXT PRIMARY KEY, algorithm TEXT NOT NULL,
            public_key BLOB NOT NULL, public_nonce BLOB NOT NULL,
            private_key BLOB NOT NULL, private_nonce BLOB NOT NULL,
            created_at TEXT NOT NULL, expires_at TEXT NOT NULL)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_task ON feedback_events(task_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_time ON feedback_events(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_policy ON benchmark_runs(policy_name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pqc_expires ON pqc_keys(expires_at);")

    def _migrate_to_v2(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS power_readings (
            reading_id TEXT PRIMARY KEY, power_watts REAL, carbon_intensity REAL,
            timestamp TEXT, metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS elasticity_metrics (
            metric_id TEXT PRIMARY KEY, price_elasticity REAL, scarcity_elasticity REAL,
            cross_elasticity REAL, substitution_elasticity REAL, thermal_elasticity REAL,
            composite_elasticity REAL, scarcity_index REAL, quality_score REAL,
            data_quality_score REAL, market_regime TEXT, migration_urgency TEXT,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS substitution_results (
            analysis_id TEXT PRIMARY KEY, base_material TEXT, substitute TEXT,
            topsis_score REAL, carbon_reduction_pct REAL, cost_savings_pct REAL,
            sustainability_score REAL, confidence_score REAL, quality_score REAL,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_rounds (
            round_id INTEGER PRIMARY KEY, num_clients INTEGER, global_accuracy REAL,
            aggregated_loss REAL, strategy TEXT, carbon_footprint REAL,
            energy_used REAL, tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS circularity_records (
            record_id TEXT PRIMARY KEY, circularity_index REAL, circularity_level TEXT,
            recycling_rate REAL, recovery_efficiency REAL, collection_efficiency REAL,
            purification_efficiency REAL, data_quality_score REAL,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS emission_records (
            record_id TEXT PRIMARY KEY, scope TEXT, amount_kg REAL, source TEXT,
            location TEXT, verified INTEGER, region TEXT, user_id TEXT,
            timestamp TEXT, metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS optimisation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT NOT NULL,
            result TEXT, timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS distribution_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, optimal_provider TEXT NOT NULL,
            optimal_region TEXT NOT NULL, scores TEXT, data_size_gb REAL,
            timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS thermal_optimizations (
            id TEXT PRIMARY KEY, data TEXT, timestamp TEXT)""")
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_power_time ON power_readings(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_elasticity_time ON elasticity_metrics(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_substitution_time ON substitution_results(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_federated_time ON federated_rounds(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_circularity_time ON circularity_records(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_emission_time ON emission_records(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_opt_time ON optimisation_history(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_dist_time ON distribution_history(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_thermal_time ON thermal_optimizations(timestamp);",
        ]:
            conn.execute(idx_sql)

    def _migrate_to_v3(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_populations (
            generation INTEGER, individual_id TEXT, attributes TEXT, fitness REAL,
            timestamp TEXT, PRIMARY KEY (generation, individual_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_fitness_history (
            generation INTEGER PRIMARY KEY, best_fitness REAL, avg_fitness REAL,
            diversity REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS moe_gating_training (
            sample_id TEXT PRIMARY KEY, features TEXT, expert_label INTEGER,
            reward REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS moe_expert_metadata (
            expert_id TEXT PRIMARY KEY, name TEXT, description TEXT,
            performance_score REAL, last_updated TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS pareto_front (
            solution_id TEXT PRIMARY KEY, decision_attributes TEXT,
            accuracy REAL, carbon REAL, cost REAL, robustness REAL,
            is_current INTEGER, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS user_preferences (
            user_id TEXT, weights TEXT, chosen_solution_id TEXT, timestamp TEXT,
            PRIMARY KEY (user_id, timestamp))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scenarios (
            scenario_id TEXT PRIMARY KEY, carbon_price REAL, discount_rate REAL,
            demand_growth_rate REAL, technology_cost_reduction REAL,
            regulatory_risk REAL, renewable_energy_share REAL,
            energy_efficiency REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS decision_catalogue (
            option_id TEXT PRIMARY KEY, name TEXT, attributes TEXT, timestamp TEXT)""")
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_pareto_current ON pareto_front(is_current);",
            "CREATE INDEX IF NOT EXISTS idx_ga_generation ON ga_populations(generation);",
            "CREATE INDEX IF NOT EXISTS idx_moe_sample_time ON moe_gating_training(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_preferences_user ON user_preferences(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_scenario_time ON scenarios(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_decision_time ON decision_catalogue(timestamp);",
        ]:
            conn.execute(idx_sql)

    def _migrate_to_v4(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS limit_graph_nodes (
            node_id TEXT PRIMARY KEY, node_type TEXT, attributes TEXT, created_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS limit_graph_edges (
            edge_id TEXT PRIMARY KEY, source_node TEXT NOT NULL,
            target_node TEXT NOT NULL, weight REAL, created_at TEXT,
            FOREIGN KEY (source_node) REFERENCES limit_graph_nodes(node_id),
            FOREIGN KEY (target_node) REFERENCES limit_graph_nodes(node_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS limit_graph_constraints (
            constraint_id TEXT PRIMARY KEY, node_id TEXT NOT NULL,
            value REAL, timestamp TEXT,
            FOREIGN KEY (node_id) REFERENCES limit_graph_nodes(node_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS rlhf_feedback (
            feedback_id TEXT PRIMARY KEY, state TEXT, action TEXT,
            reward REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS rlhf_reward_model (
            model_id TEXT PRIMARY KEY, version TEXT, last_trained TEXT, metrics TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS distillation_student_policy (
            policy_id TEXT PRIMARY KEY, policy_vector TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS distillation_teacher_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT, policy_vector TEXT,
            source TEXT, timestamp TEXT)""")
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_limit_edges_source ON limit_graph_edges(source_node);",
            "CREATE INDEX IF NOT EXISTS idx_limit_edges_target ON limit_graph_edges(target_node);",
            "CREATE INDEX IF NOT EXISTS idx_limit_constraints_node ON limit_graph_constraints(node_id);",
            "CREATE INDEX IF NOT EXISTS idx_rlhf_feedback_time ON rlhf_feedback(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_distillation_teacher_time ON distillation_teacher_policies(timestamp);",
        ]:
            conn.execute(idx_sql)

    # =========================================================================
    # NEW: v5.0.0 MIGRATION – All ten enhancement areas
    # =========================================================================
    def _migrate_to_v5(self, conn: sqlite3.Connection) -> None:
        """New tables for v5.0.0:
        Quantum Distillation, Causal RL, Federated Green Learning,
        Multi-Agent Coordination, Temporal Logic, XAI, Adaptive Precision,
        Carbon Markets + RECs, Chaos Testing, Human-in-the-Loop.
        """

        # ---------------- 1. Quantum‑Distillation Integration ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS quantum_distillation_state (
            state_id TEXT PRIMARY KEY,
            qaoa_parameters TEXT,             -- JSON array of ansatz angles
            qubo_penalties TEXT,              -- JSON dict (penalty_carbon, ...)
            vqe_energy REAL,
            n_qubits INTEGER,
            n_layers INTEGER,
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS quantum_teacher_weights (
            teacher_id TEXT PRIMARY KEY,
            weight REAL,
            source TEXT,                      -- 'qaoa' / 'vqe' / 'quantum_bridge'
            updated_at TEXT)""")

        # ---------------- 2. Causal RL for Policy Adaptation ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS causal_graphs (
            graph_id TEXT PRIMARY KEY,
            edges TEXT,                       -- JSON list of {source, target, weight}
            metadata TEXT,
            created_at TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS counterfactual_estimates (
            estimate_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            potential_outcome REAL,
            propensity_score REAL,
            confidence REAL,
            context TEXT,                     -- JSON
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS ate_history (
            ate_id TEXT PRIMARY KEY,
            action TEXT,
            average_treatment_effect REAL,
            samples INTEGER,
            method TEXT,                      -- 'IPW' / 'doubly_robust'
            timestamp TEXT)""")

        # ---------------- 3. Federated Green Learning ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_global_models (
            round_id INTEGER PRIMARY KEY,
            global_weights TEXT,              -- JSON array
            num_clients INTEGER,
            aggregation_strategy TEXT,
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS federated_client_contributions (
            contribution_id TEXT PRIMARY KEY,
            round_id INTEGER,
            client_id TEXT NOT NULL,
            weights TEXT,                     -- JSON array
            samples INTEGER,
            loss REAL,
            timestamp TEXT,
            FOREIGN KEY (round_id) REFERENCES federated_global_models(round_id))""")

        conn.execute("""CREATE TABLE IF NOT EXISTS federated_staleness (
            client_id TEXT PRIMARY KEY,
            last_contribution_round INTEGER,
            staleness INTEGER,
            last_seen TEXT)""")

        # ---------------- 4. Advanced Multi-Agent Coordination ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_registry (
            agent_id TEXT PRIMARY KEY,
            capabilities TEXT,                -- JSON dict
            current_role TEXT,
            is_active INTEGER,
            last_active TEXT,
            explanation TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS role_assignments (
            assignment_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            role TEXT NOT NULL,
            softmax_weights TEXT,             -- JSON dict of role -> prob
            explanation TEXT,
            xai_explanation TEXT,             -- JSON (feature contributions)
            timestamp TEXT,
            FOREIGN KEY (agent_id) REFERENCES agent_registry(agent_id))""")

        conn.execute("""CREATE TABLE IF NOT EXISTS agent_messages (
            message_id TEXT PRIMARY KEY,
            sender_id TEXT,
            recipient_id TEXT,
            payload TEXT,                     -- JSON
            status TEXT,                      -- 'pending' / 'read'
            created_at TEXT)""")

        # ---------------- 5. Temporal Logic & Formal Verification ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_logic_formulas (
            formula_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            ltl_expression TEXT NOT NULL,
            description TEXT,
            created_at TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_logic_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formula_name TEXT,
            state_snapshot TEXT,              -- JSON
            result INTEGER,                   -- 1=pass, 0=fail
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_logic_violations (
            violation_id TEXT PRIMARY KEY,
            formula_name TEXT NOT NULL,
            expression TEXT,
            state_snapshot TEXT,              -- JSON
            severity TEXT,
            timestamp TEXT)""")

        # ---------------- 6. Explainable AI (XAI) ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS xai_explanations (
            explanation_id TEXT PRIMARY KEY,
            decision_type TEXT NOT NULL,      -- 'modp' / 'moe' / 'reward' ...
            reference_id TEXT,                -- optional link to other records
            feature_contributions TEXT,       -- JSON dict
            top_features TEXT,                -- JSON array
            narrative TEXT,                   -- human-readable
            weights_used TEXT,                -- JSON dict
            timestamp TEXT)""")

        # ---------------- 7. Adaptive Precision Switching ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS precision_selections (
            selection_id TEXT PRIMARY KEY,
            precision_level TEXT NOT NULL,    -- fp32/fp16/bf16/fp8/fp4
            carbon_intensity REAL,
            gpu_available INTEGER,
            gpu_utilization REAL,
            memory_gb REAL,
            accuracy_required REAL,
            energy_savings REAL,
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS precision_policy_state (
            state_id TEXT PRIMARY KEY,
            policy_weights TEXT,              -- JSON array
            last_updated TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS precision_energy_savings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            precision_level TEXT,
            energy_saved_joules REAL,
            carbon_saved_kg REAL,
            timestamp TEXT)""")

        # ---------------- 8. Carbon Markets + RECs ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_credit_trades (
            trade_id TEXT PRIMARY KEY,
            trade_type TEXT NOT NULL,         -- 'buy' / 'sell' / 'retire'
            amount_kg REAL NOT NULL,
            price_usd REAL,
            beneficiary TEXT,
            market TEXT,                      -- 'verra' / 'gold_standard' / ...
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS rec_certificates (
            cert_id TEXT PRIMARY KEY,
            energy_mwh REAL NOT NULL,
            source TEXT,                      -- 'solar' / 'wind' / 'hydro'
            vintage TEXT,
            serial_number TEXT,
            retired INTEGER,
            beneficiary TEXT,
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_market_prices (
            price_id TEXT PRIMARY KEY,
            carbon_price_usd_per_ton REAL,
            rec_price_usd_per_mwh REAL,
            grid_intensity_kg_per_mwh REAL,
            region TEXT,
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_retirement_records (
            retirement_id TEXT PRIMARY KEY,
            amount_kg REAL,
            beneficiary TEXT,
            purpose TEXT,
            certificate TEXT,
            timestamp TEXT)""")

        # ---------------- 9. Chaos Testing ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_test_schedules (
            schedule_id TEXT PRIMARY KEY,
            fault_type TEXT NOT NULL,
            scheduled_at TEXT,
            duration_seconds REAL,
            target_module TEXT,
            enabled INTEGER)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_test_results (
            test_id TEXT PRIMARY KEY,
            fault_type TEXT NOT NULL,
            duration_seconds REAL,
            passed INTEGER,
            error_message TEXT,
            recovery_time_ms REAL,
            target_module TEXT,
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS recovery_actions (
            action_id TEXT PRIMARY KEY,
            trigger_type TEXT,                -- 'drift' / 'chaos' / 'anomaly'
            action TEXT,
            outcome TEXT,
            timestamp TEXT)""")

        # ---------------- 10. Human-in-the-Loop (Active Learning) ----------------
        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_pending_queries (
            query_id TEXT PRIMARY KEY,
            context TEXT,                     -- JSON
            options TEXT,                     -- JSON array
            uncertainty REAL,
            status TEXT,                      -- 'pending' / 'resolved' / 'timed_out'
            created_at TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_resolved_queries (
            query_id TEXT PRIMARY KEY,
            chosen_action TEXT,
            rating REAL,
            resolution_source TEXT,           -- 'human' / 'auto_fallback'
            resolved_at TEXT,
            FOREIGN KEY (query_id) REFERENCES hitl_pending_queries(query_id))""")

        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_audit_log (
            log_id TEXT PRIMARY KEY,
            decision TEXT NOT NULL,           -- 'auto' / 'escalated'
            chosen TEXT,
            confidence REAL,
            query_id TEXT,
            context TEXT,                     -- JSON
            timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS active_learning_samples (
            sample_id TEXT PRIMARY KEY,
            state TEXT,                       -- JSON
            uncertainty REAL,
            selected_for_review INTEGER,
            outcome TEXT,
            timestamp TEXT)""")

        # ---------------- Indexes for v5 tables ----------------
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_quantum_distill_time ON quantum_distillation_state(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_causal_graph_time ON causal_graphs(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_cf_estimates_action ON counterfactual_estimates(action);",
            "CREATE INDEX IF NOT EXISTS idx_ate_action ON ate_history(action);",
            "CREATE INDEX IF NOT EXISTS idx_fed_global_time ON federated_global_models(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_fed_client_round ON federated_client_contributions(round_id);",
            "CREATE INDEX IF NOT EXISTS idx_fed_client_id ON federated_client_contributions(client_id);",
            "CREATE INDEX IF NOT EXISTS idx_agent_role ON agent_registry(current_role);",
            "CREATE INDEX IF NOT EXISTS idx_role_agent ON role_assignments(agent_id);",
            "CREATE INDEX IF NOT EXISTS idx_agent_msg_recipient ON agent_messages(recipient_id);",
            "CREATE INDEX IF NOT EXISTS idx_tl_history_formula ON temporal_logic_history(formula_name);",
            "CREATE INDEX IF NOT EXISTS idx_tl_violation_formula ON temporal_logic_violations(formula_name);",
            "CREATE INDEX IF NOT EXISTS idx_xai_decision ON xai_explanations(decision_type);",
            "CREATE INDEX IF NOT EXISTS idx_xai_ref ON xai_explanations(reference_id);",
            "CREATE INDEX IF NOT EXISTS idx_precision_time ON precision_selections(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_precision_level ON precision_selections(precision_level);",
            "CREATE INDEX IF NOT EXISTS idx_carbon_trade_time ON carbon_credit_trades(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_rec_time ON rec_certificates(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_market_price_time ON carbon_market_prices(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_chaos_schedule_time ON chaos_test_schedules(scheduled_at);",
            "CREATE INDEX IF NOT EXISTS idx_chaos_result_time ON chaos_test_results(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_recovery_time ON recovery_actions(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_hitl_status ON hitl_pending_queries(status);",
            "CREATE INDEX IF NOT EXISTS idx_hitl_resolved_time ON hitl_resolved_queries(resolved_at);",
            "CREATE INDEX IF NOT EXISTS idx_hitl_audit_time ON hitl_audit_log(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_al_selected ON active_learning_samples(selected_for_review);",
        ]:
            conn.execute(idx_sql)

    # --------------------------------------------------------------------------
    # Encryption helpers
    # --------------------------------------------------------------------------
    def _encrypt_blob(self, data: bytes) -> Tuple[bytes, bytes]:
        if self.master_key and CRYPTO_AVAILABLE:
            nonce = secrets.token_bytes(12)
            aesgcm = AESGCM(self.master_key)
            ciphertext = aesgcm.encrypt(nonce, data, None)
            return ciphertext, nonce
        return data, b''

    def _decrypt_blob(self, ciphertext: bytes, nonce: bytes) -> bytes:
        if self.master_key and CRYPTO_AVAILABLE and nonce:
            aesgcm = AESGCM(self.master_key)
            return aesgcm.decrypt(nonce, ciphertext, None)
        return ciphertext

    # =========================================================================
    # CORE METHODS (v1 – model_weights, feedback, drift, benchmark, kv, pqc)
    # =========================================================================
    def save_model_weights(self, model_id: str, weights_bytes: bytes) -> None:
        self._execute("INSERT OR REPLACE INTO model_weights VALUES (?, ?, ?)",
                      (model_id, weights_bytes, time.time()))

    def load_model_weights(self, model_id: str) -> Optional[bytes]:
        row = self._fetchone("SELECT weights FROM model_weights WHERE model_id = ?",
                             (model_id,))
        return row["weights"] if row else None

    def store_feedback_event(self, event: Dict) -> None:
        self._execute("""
            INSERT OR REPLACE INTO feedback_events (
                event_id, timestamp, task_id, model_id, teacher_id, selected_action,
                quality_score, latency_ms, energy_joules, carbon_g, helium_cost,
                resource_usage, distillation_loss, feedback_type, adaptive_cost_value,
                metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event["event_id"], float(event["timestamp"]), event["task_id"],
             event.get("model_id"), event.get("teacher_id"),
             event["selected_action"], float(event["quality_score"]),
             float(event["latency_ms"]), float(event["energy_joules"]),
             float(event["carbon_g"]), event.get("helium_cost"),
             json.dumps(event.get("resource_usage", {})),
             event.get("distillation_loss"), event["feedback_type"],
             float(event["adaptive_cost_value"]),
             json.dumps(event.get("metadata", {}))))

    def get_feedback_events(self, limit: int = 1000) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM feedback_events ORDER BY timestamp DESC LIMIT ?", (limit,))
        for r in rows:
            for k in ("resource_usage", "metadata"):
                try: r[k] = json.loads(r.get(k) or "{}")
                except Exception: pass
        return rows

    def save_drift_snapshot(self, snapshot_id: str, online_w: Optional[bytes],
                            offline_w: Optional[bytes], cost: Optional[float],
                            reason: Optional[str]) -> None:
        self._execute("INSERT OR REPLACE INTO drift_states VALUES (?, ?, ?, ?, ?, ?)",
                      (snapshot_id, time.time(), online_w, offline_w, cost, reason))

    def get_last_snapshot(self) -> Optional[Dict]:
        return self._fetchone("SELECT * FROM drift_states ORDER BY timestamp DESC LIMIT 1")

    def store_benchmark_result(self, run_id: str, policy: str,
                               metrics: Dict[str, float], count: int) -> None:
        self._execute("""INSERT OR REPLACE INTO benchmark_runs
            (run_id, timestamp, policy_name, avg_quality, avg_carbon, avg_latency,
             avg_cost, total_energy, sample_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, time.time(), policy, metrics.get("quality"),
             metrics.get("carbon"), metrics.get("latency"), metrics.get("cost"),
             metrics.get("energy"), int(count)))

    def get_benchmark_results(self, days_back: int = 7) -> List[Dict]:
        cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()
        return self._fetchall(
            "SELECT * FROM benchmark_runs WHERE timestamp >= ? ORDER BY timestamp DESC",
            (cutoff,))

    def save_state(self, key: str, value: str) -> None:
        self._execute("INSERT OR REPLACE INTO kv_store VALUES (?, ?, ?)",
                      (key, value, datetime.now().isoformat()))

    def get_state(self, key: str) -> Optional[str]:
        row = self._fetchone("SELECT value FROM kv_store WHERE key = ?", (key,))
        return row["value"] if row else None

    def delete_state(self, key: str) -> None:
        self._execute("DELETE FROM kv_store WHERE key = ?", (key,))

    def save_pqc_key(self, key_id: str, algorithm: str, public_key: bytes,
                     private_key: bytes, expires_at: str) -> None:
        pub_cipher, pub_nonce = self._encrypt_blob(public_key)
        priv_cipher, priv_nonce = self._encrypt_blob(private_key)
        self._execute("""INSERT OR REPLACE INTO pqc_keys
            (key_id, algorithm, public_key, public_nonce, private_key,
             private_nonce, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (key_id, algorithm, pub_cipher, pub_nonce, priv_cipher, priv_nonce,
             datetime.now().isoformat(), expires_at))

    def get_pqc_key(self, key_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM pqc_keys WHERE key_id = ?", (key_id,))
        if row:
            pub = self._decrypt_blob(row["public_key"], row["public_nonce"])
            priv = self._decrypt_blob(row["private_key"], row["private_nonce"])
            return {"key_id": row["key_id"], "algorithm": row["algorithm"],
                    "public_key": pub, "private_key": priv,
                    "created_at": row["created_at"], "expires_at": row["expires_at"]}
        return None

    def list_pqc_keys(self) -> List[str]:
        return [r["key_id"] for r in self._fetchall("SELECT key_id FROM pqc_keys")]

    def delete_pqc_key(self, key_id: str) -> None:
        self._execute("DELETE FROM pqc_keys WHERE key_id = ?", (key_id,))

    # =========================================================================
    # v2 METHODS (power, elasticity, substitution, federated, circularity,
    # emission, optimisation, distribution, thermal)
    # =========================================================================
    def store_power_reading(self, reading: Dict) -> None:
        self._execute("INSERT OR REPLACE INTO power_readings VALUES (?, ?, ?, ?, ?)",
                      (reading["reading_id"], reading["power_watts"],
                       reading.get("carbon_intensity"), reading["timestamp"],
                       json.dumps(reading.get("metadata", {}))))

    def store_elasticity_metrics(self, metrics) -> None:
        data = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        self._execute("""INSERT OR REPLACE INTO elasticity_metrics (
            metric_id, price_elasticity, scarcity_elasticity, cross_elasticity,
            substitution_elasticity, thermal_elasticity, composite_elasticity,
            scarcity_index, quality_score, data_quality_score, market_regime,
            migration_urgency, tx_hash, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (data["metric_id"], data["price_elasticity"], data["scarcity_elasticity"],
             data["cross_elasticity"], data["substitution_elasticity"],
             data["thermal_elasticity"], data["composite_elasticity"],
             data["scarcity_index"], data["quality_score"], data["data_quality_score"],
             data["market_regime"], data["migration_urgency"],
             data.get("blockchain_tx_hash") or "",
             data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat")
             else data["timestamp"]))

    def store_substitution_result(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        self._execute("""INSERT OR REPLACE INTO substitution_results (
            analysis_id, base_material, substitute, topsis_score,
            carbon_reduction_pct, cost_savings_pct, sustainability_score,
            confidence_score, quality_score, tx_hash, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (data["calculation_id"], data["base_material"],
             data["recommended_substitute"], data["topsis_score"],
             data["carbon_reduction_pct"], data["cost_savings_pct"],
             data["sustainability_score"], data["confidence_score"],
             data["data_quality_score"], data.get("blockchain_tx_hash") or "",
             data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat")
             else data["timestamp"]))

    def store_federated_round(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        self._execute("""INSERT OR REPLACE INTO federated_rounds (
            round_id, num_clients, global_accuracy, aggregated_loss, strategy,
            carbon_footprint, energy_used, tx_hash, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (data["round_id"], data["num_clients"], data["global_accuracy"],
             data["aggregated_loss"], data["strategy"], data["carbon_footprint"],
             data["energy_used"], data.get("blockchain_tx_hash") or "",
             data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat")
             else data["timestamp"]))

    def store_circularity_record(self, metrics) -> None:
        data = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        self._execute("""INSERT OR REPLACE INTO circularity_records (
            record_id, circularity_index, circularity_level, recycling_rate,
            recovery_efficiency, collection_efficiency, purification_efficiency,
            data_quality_score, tx_hash, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (data["record_id"], data["circularity_index"], data["circularity_level"],
             data["recycling_rate"], data["recovery_efficiency"],
             data["collection_efficiency"], data["purification_efficiency"],
             data["data_quality_score"], data.get("blockchain_tx_hash") or "",
             data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat")
             else data["timestamp"]))

    def store_emission_record(self, record: Dict) -> None:
        self._execute("""INSERT OR REPLACE INTO emission_records (
            record_id, scope, amount_kg, source, location, verified,
            region, user_id, timestamp, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (record["record_id"], record["scope"], record["amount_kg"],
             record["source"], record["location"],
             1 if record.get("verified") else 0,
             record["region"], record["user_id"], record["timestamp"],
             json.dumps(record.get("metadata", {}))))

    def save_optimisation(self, strategy: str, result: Dict) -> None:
        self._execute("INSERT INTO optimisation_history VALUES (NULL, ?, ?, ?)",
                      (strategy, json.dumps(result), datetime.now().isoformat()))

    def get_recent_optimisations(self, limit: int = 10) -> List[Dict]:
        rows = self._fetchall(
            "SELECT strategy, result, timestamp FROM optimisation_history "
            "ORDER BY id DESC LIMIT ?", (limit,))
        return [{"strategy": r["strategy"], "result": json.loads(r["result"]),
                 "timestamp": r["timestamp"]} for r in rows]

    def save_distribution(self, result: Dict) -> None:
        self._execute("""INSERT INTO distribution_history
            (optimal_provider, optimal_region, scores, data_size_gb, timestamp)
            VALUES (?, ?, ?, ?, ?)""",
            (result["optimal_provider"], result["optimal_region"],
             json.dumps(result["scores"]), result.get("data_size_gb", 0),
             result["timestamp"]))

    def get_recent_distributions(self, limit: int = 10) -> List[Dict]:
        rows = self._fetchall("""SELECT optimal_provider, optimal_region, scores,
            data_size_gb, timestamp FROM distribution_history
            ORDER BY id DESC LIMIT ?""", (limit,))
        return [{"optimal_provider": r["optimal_provider"],
                 "optimal_region": r["optimal_region"],
                 "scores": json.loads(r["scores"]),
                 "data_size_gb": r["data_size_gb"],
                 "timestamp": r["timestamp"]} for r in rows]

    def store_thermal_optimization(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        self._execute("INSERT OR REPLACE INTO thermal_optimizations VALUES (?, ?, ?)",
                      (data.get("id", f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
                       json.dumps(data, default=str),
                       datetime.now().isoformat()))

    # =========================================================================
    # v3 METHODS (GA, MoE, Pareto, user prefs, scenarios, decision catalogue)
    # =========================================================================
    def save_ga_population(self, generation: int, individuals: List[Dict]) -> None:
        for ind in individuals:
            self._execute("""INSERT OR REPLACE INTO ga_populations
                (generation, individual_id, attributes, fitness, timestamp)
                VALUES (?, ?, ?, ?, ?)""",
                (generation, ind["individual_id"], json.dumps(ind["attributes"]),
                 ind["fitness"], datetime.now().isoformat()))

    def get_ga_population(self, generation: int) -> List[Dict]:
        rows = self._fetchall("""SELECT individual_id, attributes, fitness
            FROM ga_populations WHERE generation = ?""", (generation,))
        return [{"individual_id": r["individual_id"],
                 "attributes": json.loads(r["attributes"]),
                 "fitness": r["fitness"]} for r in rows]

    def save_ga_fitness_history(self, generation: int, best_fitness: float,
                                 avg_fitness: float, diversity: float) -> None:
        self._execute("""INSERT OR REPLACE INTO ga_fitness_history
            VALUES (?, ?, ?, ?, ?)""",
            (generation, best_fitness, avg_fitness, diversity,
             datetime.now().isoformat()))

    def save_moe_training_sample(self, sample_id: str, features: List[float],
                                 expert_label: int, reward: float) -> None:
        self._execute("""INSERT OR REPLACE INTO moe_gating_training
            VALUES (?, ?, ?, ?, ?)""",
            (sample_id, json.dumps(features), expert_label, reward,
             datetime.now().isoformat()))

    def get_moe_training_samples(self, limit: int = 1000) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM moe_gating_training "
                              "ORDER BY timestamp DESC LIMIT ?", (limit,))
        for r in rows:
            r["features"] = json.loads(r["features"])
        return rows

    def save_moe_expert_metadata(self, expert_id: str, name: str,
                                  description: str, performance_score: float) -> None:
        self._execute("""INSERT OR REPLACE INTO moe_expert_metadata
            VALUES (?, ?, ?, ?, ?)""",
            (expert_id, name, description, performance_score,
             datetime.now().isoformat()))

    def list_moe_experts(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM moe_expert_metadata "
                              "ORDER BY performance_score DESC")

    def save_pareto_front(self, solutions: List[Dict]) -> None:
        self._execute("UPDATE pareto_front SET is_current = 0")
        for sol in solutions:
            self._execute("""INSERT OR REPLACE INTO pareto_front
                (solution_id, decision_attributes, accuracy, carbon, cost,
                 robustness, is_current, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (sol["solution_id"], json.dumps(sol["decision_attributes"]),
                 sol["accuracy"], sol["carbon"], sol["cost"], sol["robustness"],
                 1, datetime.now().isoformat()))

    def get_current_pareto_front(self) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM pareto_front WHERE is_current = 1 "
                              "ORDER BY accuracy DESC")
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    def save_user_preference(self, user_id: str, weights: Dict[str, float],
                              chosen_solution_id: Optional[str] = None) -> None:
        self._execute("""INSERT INTO user_preferences
            (user_id, weights, chosen_solution_id, timestamp)
            VALUES (?, ?, ?, ?)""",
            (user_id, json.dumps(weights), chosen_solution_id,
             datetime.now().isoformat()))

    def get_user_preferences(self, user_id: str, limit: int = 10) -> List[Dict]:
        rows = self._fetchall("""SELECT weights, chosen_solution_id, timestamp
            FROM user_preferences WHERE user_id = ?
            ORDER BY timestamp DESC LIMIT ?""", (user_id, limit))
        for r in rows:
            r["weights"] = json.loads(r["weights"])
        return rows

    def save_scenario(self, scenario_id: str, scenario: Dict) -> None:
        self._execute("""INSERT OR REPLACE INTO scenarios
            (scenario_id, carbon_price, discount_rate, demand_growth_rate,
             technology_cost_reduction, regulatory_risk, renewable_energy_share,
             energy_efficiency, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (scenario_id, scenario.get("carbon_price", 50.0),
             scenario.get("discount_rate", 0.05),
             scenario.get("demand_growth_rate", 0.02),
             scenario.get("technology_cost_reduction", 0.1),
             scenario.get("regulatory_risk", 0.3),
             scenario.get("renewable_energy_share", 0.3),
             scenario.get("energy_efficiency", 0.7),
             datetime.now().isoformat()))

    def save_decision_option(self, option_id: str, name: str,
                              attributes: Dict) -> None:
        self._execute("""INSERT OR REPLACE INTO decision_catalogue
            VALUES (?, ?, ?, ?)""",
            (option_id, name, json.dumps(attributes), datetime.now().isoformat()))

    def list_decision_options(self) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM decision_catalogue")
        for r in rows:
            r["attributes"] = json.loads(r["attributes"])
        return rows

    # =========================================================================
    # v4 METHODS (LIMIT Graph, RLHF, Distillation)
    # =========================================================================
    def save_limit_graph_node(self, node_id: str, node_type: str,
                               attributes: Dict) -> None:
        self._execute("""INSERT OR REPLACE INTO limit_graph_nodes
            VALUES (?, ?, ?, ?)""",
            (node_id, node_type, json.dumps(attributes),
             datetime.now().isoformat()))

    def get_limit_graph_node(self, node_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM limit_graph_nodes WHERE node_id = ?",
                             (node_id,))
        if row:
            row["attributes"] = json.loads(row["attributes"])
        return row

    def save_limit_graph_edge(self, edge_id: str, source_node: str,
                               target_node: str, weight: float) -> None:
        self._execute("""INSERT OR REPLACE INTO limit_graph_edges
            VALUES (?, ?, ?, ?, ?)""",
            (edge_id, source_node, target_node, weight,
             datetime.now().isoformat()))

    def get_limit_graph_edges(self, source_node: Optional[str] = None) -> List[Dict]:
        if source_node:
            return self._fetchall(
                "SELECT * FROM limit_graph_edges WHERE source_node = ?",
                (source_node,))
        return self._fetchall("SELECT * FROM limit_graph_edges")

    def save_limit_graph_constraint(self, constraint_id: str, node_id: str,
                                     value: float) -> None:
        self._execute("""INSERT OR REPLACE INTO limit_graph_constraints
            VALUES (?, ?, ?, ?)""",
            (constraint_id, node_id, value, datetime.now().isoformat()))

    def save_rlhf_feedback(self, feedback_id: str, state: Dict,
                            action: str, reward: float) -> None:
        self._execute("""INSERT OR REPLACE INTO rlhf_feedback
            VALUES (?, ?, ?, ?, ?)""",
            (feedback_id, json.dumps(state), action, reward,
             datetime.now().isoformat()))

    def get_rlhf_feedback(self, limit: int = 1000) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM rlhf_feedback "
                              "ORDER BY timestamp DESC LIMIT ?", (limit,))
        for r in rows:
            r["state"] = json.loads(r["state"])
        return rows

    def save_rlhf_model_metadata(self, model_id: str, version: str,
                                  metrics: Dict) -> None:
        self._execute("""INSERT OR REPLACE INTO rlhf_reward_model
            VALUES (?, ?, ?, ?)""",
            (model_id, version, datetime.now().isoformat(),
             json.dumps(metrics)))

    def save_distillation_student_policy(self, policy_id: str,
                                          policy_vector: List[float]) -> None:
        self._execute("""INSERT OR REPLACE INTO distillation_student_policy
            VALUES (?, ?, ?)""",
            (policy_id, json.dumps(policy_vector), datetime.now().isoformat()))

    def get_distillation_student_policy(self, policy_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM distillation_student_policy WHERE policy_id = ?",
            (policy_id,))
        if row:
            row["policy_vector"] = json.loads(row["policy_vector"])
        return row

    def save_distillation_teacher_policy(self, policy_vector: List[float],
                                          source: str) -> None:
        self._execute("""INSERT INTO distillation_teacher_policies
            (policy_vector, source, timestamp) VALUES (?, ?, ?)""",
            (json.dumps(policy_vector), source, datetime.now().isoformat()))

    def get_distillation_teacher_policies(self, limit: int = 100) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM distillation_teacher_policies
            ORDER BY id DESC LIMIT ?""", (limit,))
        for r in rows:
            r["policy_vector"] = json.loads(r["policy_vector"])
        return rows

    # =========================================================================
    # NEW v5.0.0 METHODS
    # =========================================================================

    # ---------------- 1. Quantum‑Distillation Integration ----------------
    def save_quantum_distillation_state(self, state_id: str,
                                        qaoa_parameters: List[float],
                                        qubo_penalties: Dict,
                                        vqe_energy: float,
                                        n_qubits: int, n_layers: int) -> None:
        self._execute("""INSERT OR REPLACE INTO quantum_distillation_state
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (state_id, json.dumps(qaoa_parameters), json.dumps(qubo_penalties),
             vqe_energy, n_qubits, n_layers, datetime.now().isoformat()))

    def get_quantum_distillation_state(self, state_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM quantum_distillation_state WHERE state_id = ?",
            (state_id,))
        if row:
            row["qaoa_parameters"] = json.loads(row["qaoa_parameters"])
            row["qubo_penalties"] = json.loads(row["qubo_penalties"])
        return row

    def list_quantum_distillation_states(self, limit: int = 50) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM quantum_distillation_state
            ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["qaoa_parameters"] = json.loads(r["qaoa_parameters"])
            r["qubo_penalties"] = json.loads(r["qubo_penalties"])
        return rows

    def save_quantum_teacher_weight(self, teacher_id: str, weight: float,
                                     source: str) -> None:
        self._execute("""INSERT OR REPLACE INTO quantum_teacher_weights
            VALUES (?, ?, ?, ?)""",
            (teacher_id, weight, source, datetime.now().isoformat()))

    def list_quantum_teacher_weights(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM quantum_teacher_weights "
                              "ORDER BY weight DESC")

    # ---------------- 2. Causal RL ----------------
    def save_causal_graph(self, graph_id: str, edges: List[Dict],
                           metadata: Optional[Dict] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO causal_graphs
            VALUES (?, ?, ?, ?)""",
            (graph_id, json.dumps(edges), json.dumps(metadata or {}),
             datetime.now().isoformat()))

    def get_causal_graph(self, graph_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM causal_graphs WHERE graph_id = ?",
                             (graph_id,))
        if row:
            row["edges"] = json.loads(row["edges"])
            row["metadata"] = json.loads(row["metadata"])
        return row

    def save_counterfactual_estimate(self, estimate_id: str, action: str,
                                      potential_outcome: float,
                                      propensity_score: float,
                                      confidence: float,
                                      context: Optional[Dict] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO counterfactual_estimates
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (estimate_id, action, potential_outcome, propensity_score,
             confidence, json.dumps(context or {}), datetime.now().isoformat()))

    def get_counterfactual_estimates(self, action: Optional[str] = None,
                                      limit: int = 100) -> List[Dict]:
        if action:
            rows = self._fetchall("""SELECT * FROM counterfactual_estimates
                WHERE action = ? ORDER BY timestamp DESC LIMIT ?""",
                (action, limit))
        else:
            rows = self._fetchall("""SELECT * FROM counterfactual_estimates
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["context"] = json.loads(r["context"])
        return rows

    def save_ate(self, ate_id: str, action: str, ate: float,
                 samples: int, method: str) -> None:
        self._execute("""INSERT OR REPLACE INTO ate_history
            VALUES (?, ?, ?, ?, ?, ?)""",
            (ate_id, action, ate, samples, method, datetime.now().isoformat()))

    def get_latest_ate(self, action: str) -> Optional[Dict]:
        return self._fetchone("""SELECT * FROM ate_history WHERE action = ?
            ORDER BY timestamp DESC LIMIT 1""", (action,))

    # ---------------- 3. Federated Green Learning ----------------
    def save_federated_global_model(self, round_id: int,
                                     global_weights: List[float],
                                     num_clients: int,
                                     aggregation_strategy: str) -> None:
        self._execute("""INSERT OR REPLACE INTO federated_global_models
            VALUES (?, ?, ?, ?, ?)""",
            (round_id, json.dumps(global_weights), num_clients,
             aggregation_strategy, datetime.now().isoformat()))

    def get_federated_global_model(self, round_id: int) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM federated_global_models WHERE round_id = ?",
            (round_id,))
        if row:
            row["global_weights"] = json.loads(row["global_weights"])
        return row

    def save_federated_client_contribution(self, contribution_id: str,
                                            round_id: int, client_id: str,
                                            weights: List[float],
                                            samples: int,
                                            loss: Optional[float] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO federated_client_contributions
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (contribution_id, round_id, client_id, json.dumps(weights),
             samples, loss, datetime.now().isoformat()))

    def get_federated_client_contributions(self, round_id: Optional[int] = None,
                                            client_id: Optional[str] = None,
                                            limit: int = 100) -> List[Dict]:
        if round_id is not None:
            rows = self._fetchall("""SELECT * FROM federated_client_contributions
                WHERE round_id = ? ORDER BY timestamp DESC LIMIT ?""",
                (round_id, limit))
        elif client_id is not None:
            rows = self._fetchall("""SELECT * FROM federated_client_contributions
                WHERE client_id = ? ORDER BY timestamp DESC LIMIT ?""",
                (client_id, limit))
        else:
            rows = self._fetchall("""SELECT * FROM federated_client_contributions
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["weights"] = json.loads(r["weights"])
        return rows

    def update_federated_staleness(self, client_id: str,
                                    last_contribution_round: int,
                                    staleness: int) -> None:
        self._execute("""INSERT OR REPLACE INTO federated_staleness
            VALUES (?, ?, ?, ?)""",
            (client_id, last_contribution_round, staleness,
             datetime.now().isoformat()))

    def get_federated_staleness(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM federated_staleness")

    # ---------------- 4. Multi-Agent Coordination ----------------
    def save_agent(self, agent_id: str, capabilities: Dict,
                    current_role: str, is_active: bool = True,
                    explanation: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO agent_registry
            VALUES (?, ?, ?, ?, ?, ?)""",
            (agent_id, json.dumps(capabilities), current_role,
             1 if is_active else 0, datetime.now().isoformat(), explanation))

    def get_agent(self, agent_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM agent_registry WHERE agent_id = ?",
                             (agent_id,))
        if row:
            row["capabilities"] = json.loads(row["capabilities"])
            row["is_active"] = bool(row["is_active"])
        return row

    def list_agents(self, active_only: bool = False) -> List[Dict]:
        if active_only:
            rows = self._fetchall(
                "SELECT * FROM agent_registry WHERE is_active = 1")
        else:
            rows = self._fetchall("SELECT * FROM agent_registry")
        for r in rows:
            r["capabilities"] = json.loads(r["capabilities"])
            r["is_active"] = bool(r["is_active"])
        return rows

    def save_role_assignment(self, assignment_id: str, agent_id: str,
                              role: str, softmax_weights: Dict,
                              explanation: str = "",
                              xai_explanation: Optional[Dict] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO role_assignments
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (assignment_id, agent_id, role, json.dumps(softmax_weights),
             explanation, json.dumps(xai_explanation or {}),
             datetime.now().isoformat()))

    def get_role_history(self, agent_id: str, limit: int = 20) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM role_assignments
            WHERE agent_id = ? ORDER BY timestamp DESC LIMIT ?""",
            (agent_id, limit))
        for r in rows:
            r["softmax_weights"] = json.loads(r["softmax_weights"])
            r["xai_explanation"] = json.loads(r["xai_explanation"])
        return rows

    def send_agent_message(self, message_id: str, sender_id: str,
                            recipient_id: str, payload: Dict) -> None:
        self._execute("""INSERT OR REPLACE INTO agent_messages
            VALUES (?, ?, ?, ?, ?, ?)""",
            (message_id, sender_id, recipient_id, json.dumps(payload),
             'pending', datetime.now().isoformat()))

    def get_agent_messages(self, recipient_id: str,
                            pending_only: bool = True) -> List[Dict]:
        if pending_only:
            rows = self._fetchall("""SELECT * FROM agent_messages
                WHERE recipient_id = ? AND status = 'pending'
                ORDER BY created_at""", (recipient_id,))
        else:
            rows = self._fetchall("""SELECT * FROM agent_messages
                WHERE recipient_id = ? ORDER BY created_at DESC""",
                (recipient_id,))
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows

    def mark_agent_message_read(self, message_id: str) -> None:
        self._execute("UPDATE agent_messages SET status = 'read' WHERE message_id = ?",
                      (message_id,))

    # ---------------- 5. Temporal Logic ----------------
    def save_temporal_formula(self, formula_id: str, name: str,
                               ltl_expression: str,
                               description: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO temporal_logic_formulas
            VALUES (?, ?, ?, ?, ?)""",
            (formula_id, name, ltl_expression, description,
             datetime.now().isoformat()))

    def list_temporal_formulas(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM temporal_logic_formulas")

    def save_temporal_evaluation(self, formula_name: str,
                                  state_snapshot: Dict,
                                  result: bool) -> None:
        self._execute("""INSERT INTO temporal_logic_history
            (formula_name, state_snapshot, result, timestamp)
            VALUES (?, ?, ?, ?)""",
            (formula_name, json.dumps(state_snapshot),
             1 if result else 0, datetime.now().isoformat()))

    def save_temporal_violation(self, violation_id: str, formula_name: str,
                                 expression: str, state_snapshot: Dict,
                                 severity: str = "warning") -> None:
        self._execute("""INSERT OR REPLACE INTO temporal_logic_violations
            VALUES (?, ?, ?, ?, ?, ?)""",
            (violation_id, formula_name, expression,
             json.dumps(state_snapshot), severity,
             datetime.now().isoformat()))

    def get_temporal_violations(self, formula_name: Optional[str] = None,
                                 limit: int = 100) -> List[Dict]:
        if formula_name:
            rows = self._fetchall("""SELECT * FROM temporal_logic_violations
                WHERE formula_name = ? ORDER BY timestamp DESC LIMIT ?""",
                (formula_name, limit))
        else:
            rows = self._fetchall("""SELECT * FROM temporal_logic_violations
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["state_snapshot"] = json.loads(r["state_snapshot"])
        return rows

    # ---------------- 6. Explainable AI (XAI) ----------------
    def save_xai_explanation(self, explanation_id: str, decision_type: str,
                              feature_contributions: Dict,
                              top_features: List[str],
                              narrative: List[str],
                              weights_used: Dict,
                              reference_id: Optional[str] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO xai_explanations
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (explanation_id, decision_type, reference_id,
             json.dumps(feature_contributions), json.dumps(top_features),
             json.dumps(narrative), json.dumps(weights_used),
             datetime.now().isoformat()))

    def get_xai_explanation(self, explanation_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM xai_explanations WHERE explanation_id = ?",
            (explanation_id,))
        if row:
            for k in ("feature_contributions", "top_features",
                      "narrative", "weights_used"):
                row[k] = json.loads(row[k])
        return row

    def get_xai_explanations_by_type(self, decision_type: str,
                                      limit: int = 50) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM xai_explanations
            WHERE decision_type = ? ORDER BY timestamp DESC LIMIT ?""",
            (decision_type, limit))
        for r in rows:
            for k in ("feature_contributions", "top_features",
                      "narrative", "weights_used"):
                r[k] = json.loads(r[k])
        return rows

    # ---------------- 7. Adaptive Precision Switching ----------------
    def save_precision_selection(self, selection_id: str,
                                  precision_level: str,
                                  carbon_intensity: float,
                                  gpu_available: bool,
                                  gpu_utilization: float,
                                  memory_gb: float,
                                  accuracy_required: float,
                                  energy_savings: Optional[float] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO precision_selections
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (selection_id, precision_level, carbon_intensity,
             1 if gpu_available else 0, gpu_utilization, memory_gb,
             accuracy_required, energy_savings, datetime.now().isoformat()))

    def get_precision_selections(self, precision_level: Optional[str] = None,
                                  limit: int = 100) -> List[Dict]:
        if precision_level:
            rows = self._fetchall("""SELECT * FROM precision_selections
                WHERE precision_level = ? ORDER BY timestamp DESC LIMIT ?""",
                (precision_level, limit))
        else:
            rows = self._fetchall("""SELECT * FROM precision_selections
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["gpu_available"] = bool(r["gpu_available"])
        return rows

    def save_precision_policy_state(self, state_id: str,
                                     policy_weights: List[float]) -> None:
        self._execute("""INSERT OR REPLACE INTO precision_policy_state
            VALUES (?, ?, ?)""",
            (state_id, json.dumps(policy_weights),
             datetime.now().isoformat()))

    def get_precision_policy_state(self, state_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM precision_policy_state WHERE state_id = ?",
            (state_id,))
        if row:
            row["policy_weights"] = json.loads(row["policy_weights"])
        return row

    def save_precision_energy_savings(self, precision_level: str,
                                       energy_saved_joules: float,
                                       carbon_saved_kg: float) -> None:
        self._execute("""INSERT INTO precision_energy_savings
            (precision_level, energy_saved_joules, carbon_saved_kg, timestamp)
            VALUES (?, ?, ?, ?)""",
            (precision_level, energy_saved_joules, carbon_saved_kg,
             datetime.now().isoformat()))

    def get_precision_energy_savings_total(self) -> Dict:
        row = self._fetchone("""SELECT
            COALESCE(SUM(energy_saved_joules), 0) AS total_energy,
            COALESCE(SUM(carbon_saved_kg), 0) AS total_carbon
            FROM precision_energy_savings""")
        return row or {"total_energy": 0.0, "total_carbon": 0.0}

    # ---------------- 8. Carbon Markets + RECs ----------------
    def save_carbon_credit_trade(self, trade_id: str, trade_type: str,
                                  amount_kg: float, price_usd: float,
                                  beneficiary: str = "",
                                  market: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO carbon_credit_trades
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (trade_id, trade_type, amount_kg, price_usd, beneficiary,
             market, datetime.now().isoformat()))

    def get_carbon_credit_trades(self, trade_type: Optional[str] = None,
                                  limit: int = 100) -> List[Dict]:
        if trade_type:
            return self._fetchall("""SELECT * FROM carbon_credit_trades
                WHERE trade_type = ? ORDER BY timestamp DESC LIMIT ?""",
                (trade_type, limit))
        return self._fetchall("""SELECT * FROM carbon_credit_trades
            ORDER BY timestamp DESC LIMIT ?""", (limit,))

    def save_rec_certificate(self, cert_id: str, energy_mwh: float,
                              source: str, vintage: str,
                              serial_number: str,
                              beneficiary: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO rec_certificates
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (cert_id, energy_mwh, source, vintage, serial_number, 0,
             beneficiary, datetime.now().isoformat()))

    def retire_rec_certificate(self, cert_id: str) -> None:
        self._execute("UPDATE rec_certificates SET retired = 1 WHERE cert_id = ?",
                      (cert_id,))

    def get_rec_certificates(self, retired: Optional[bool] = None,
                              limit: int = 100) -> List[Dict]:
        if retired is True:
            rows = self._fetchall("""SELECT * FROM rec_certificates
                WHERE retired = 1 ORDER BY timestamp DESC LIMIT ?""", (limit,))
        elif retired is False:
            rows = self._fetchall("""SELECT * FROM rec_certificates
                WHERE retired = 0 ORDER BY timestamp DESC LIMIT ?""", (limit,))
        else:
            rows = self._fetchall("""SELECT * FROM rec_certificates
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["retired"] = bool(r["retired"])
        return rows

    def save_carbon_market_price(self, price_id: str,
                                  carbon_price_usd_per_ton: float,
                                  rec_price_usd_per_mwh: float,
                                  grid_intensity_kg_per_mwh: float,
                                  region: str = "global") -> None:
        self._execute("""INSERT OR REPLACE INTO carbon_market_prices
            VALUES (?, ?, ?, ?, ?, ?)""",
            (price_id, carbon_price_usd_per_ton, rec_price_usd_per_mwh,
             grid_intensity_kg_per_mwh, region, datetime.now().isoformat()))

    def get_latest_market_price(self, region: str = "global") -> Optional[Dict]:
        return self._fetchone("""SELECT * FROM carbon_market_prices
            WHERE region = ? ORDER BY timestamp DESC LIMIT 1""", (region,))

    def save_carbon_retirement(self, retirement_id: str, amount_kg: float,
                                beneficiary: str, purpose: str = "",
                                certificate: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO carbon_retirement_records
            VALUES (?, ?, ?, ?, ?, ?)""",
            (retirement_id, amount_kg, beneficiary, purpose, certificate,
             datetime.now().isoformat()))

    # ---------------- 9. Chaos Testing ----------------
    def schedule_chaos_test(self, schedule_id: str, fault_type: str,
                             scheduled_at: str, duration_seconds: float,
                             target_module: str = "",
                             enabled: bool = True) -> None:
        self._execute("""INSERT OR REPLACE INTO chaos_test_schedules
            VALUES (?, ?, ?, ?, ?, ?)""",
            (schedule_id, fault_type, scheduled_at, duration_seconds,
             target_module, 1 if enabled else 0))

    def save_chaos_test_result(self, test_id: str, fault_type: str,
                                duration_seconds: float, passed: bool,
                                error_message: str = "",
                                recovery_time_ms: float = 0.0,
                                target_module: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO chaos_test_results
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (test_id, fault_type, duration_seconds, 1 if passed else 0,
             error_message, recovery_time_ms, target_module,
             datetime.now().isoformat()))

    def get_chaos_test_results(self, limit: int = 100) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM chaos_test_results
            ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["passed"] = bool(r["passed"])
        return rows

    def save_recovery_action(self, action_id: str, trigger_type: str,
                              action: str, outcome: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO recovery_actions
            VALUES (?, ?, ?, ?, ?)""",
            (action_id, trigger_type, action, outcome,
             datetime.now().isoformat()))

    def get_recovery_actions(self, limit: int = 100) -> List[Dict]:
        return self._fetchall("""SELECT * FROM recovery_actions
            ORDER BY timestamp DESC LIMIT ?""", (limit,))

    # ---------------- 10. Human-in-the-Loop ----------------
    def save_hitl_query(self, query_id: str, context: Dict,
                         options: List[str], uncertainty: float,
                         status: str = "pending") -> None:
        self._execute("""INSERT OR REPLACE INTO hitl_pending_queries
            VALUES (?, ?, ?, ?, ?, ?)""",
            (query_id, json.dumps(context), json.dumps(options),
             uncertainty, status, datetime.now().isoformat()))

    def resolve_hitl_query(self, query_id: str, chosen_action: str,
                            rating: float = 1.0,
                            resolution_source: str = "human") -> None:
        self._execute("""INSERT OR REPLACE INTO hitl_resolved_queries
            VALUES (?, ?, ?, ?, ?)""",
            (query_id, chosen_action, rating, resolution_source,
             datetime.now().isoformat()))
        self._execute("""UPDATE hitl_pending_queries SET status = 'resolved'
            WHERE query_id = ?""", (query_id,))

    def get_pending_hitl_queries(self) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM hitl_pending_queries
            WHERE status = 'pending' ORDER BY created_at""")
        for r in rows:
            r["context"] = json.loads(r["context"])
            r["options"] = json.loads(r["options"])
        return rows

    def save_hitl_audit(self, log_id: str, decision: str, chosen: str,
                         confidence: float, query_id: str = "",
                         context: Optional[Dict] = None) -> None:
        self._execute("""INSERT OR REPLACE INTO hitl_audit_log
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (log_id, decision, chosen, confidence, query_id,
             json.dumps(context or {}), datetime.now().isoformat()))

    def get_hitl_audit_log(self, limit: int = 100) -> List[Dict]:
        rows = self._fetchall("""SELECT * FROM hitl_audit_log
            ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["context"] = json.loads(r["context"])
        return rows

    def save_active_learning_sample(self, sample_id: str, state: Dict,
                                     uncertainty: float,
                                     selected_for_review: bool = False,
                                     outcome: str = "") -> None:
        self._execute("""INSERT OR REPLACE INTO active_learning_samples
            VALUES (?, ?, ?, ?, ?, ?)""",
            (sample_id, json.dumps(state), uncertainty,
             1 if selected_for_review else 0, outcome,
             datetime.now().isoformat()))

    def get_active_learning_samples(self, selected_only: bool = False,
                                     limit: int = 100) -> List[Dict]:
        if selected_only:
            rows = self._fetchall("""SELECT * FROM active_learning_samples
                WHERE selected_for_review = 1
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        else:
            rows = self._fetchall("""SELECT * FROM active_learning_samples
                ORDER BY timestamp DESC LIMIT ?""", (limit,))
        for r in rows:
            r["state"] = json.loads(r["state"])
            r["selected_for_review"] = bool(r["selected_for_review"])
        return rows

    # =========================================================================
    # DATA RETENTION POLICIES
    # =========================================================================
    def apply_retention_policy(self, policies: Dict[str, int]) -> None:
        """Apply retention policy to tables.

        `policies` is a dict of table_name -> retention_days.
        Use 0 or negative to skip a table.
        """
        # Map of table -> (timestamp_column, cleanup_sql)
        retention_map = {
            "power_readings": ("timestamp", None),
            "elasticity_metrics": ("timestamp", None),
            "substitution_results": ("timestamp", None),
            "federated_rounds": ("timestamp", None),
            "circularity_records": ("timestamp", None),
            "emission_records": ("timestamp", None),
            "optimisation_history": ("timestamp", None),
            "distribution_history": ("timestamp", None),
            "thermal_optimizations": ("timestamp", None),
            "ga_populations": ("timestamp", None),
            "ga_fitness_history": ("timestamp", None),
            "moe_gating_training": ("timestamp", None),
            "pareto_front": ("timestamp", None),
            "user_preferences": ("timestamp", None),
            "scenarios": ("timestamp", None),
            "decision_catalogue": ("timestamp", None),
            "limit_graph_edges": ("created_at", None),
            "limit_graph_constraints": ("timestamp", None),
            "rlhf_feedback": ("timestamp", None),
            "distillation_teacher_policies": ("timestamp", None),
            # v5.0.0 tables
            "quantum_distillation_state": ("timestamp", None),
            "causal_graphs": ("created_at", None),
            "counterfactual_estimates": ("timestamp", None),
            "ate_history": ("timestamp", None),
            "federated_global_models": ("timestamp", None),
            "federated_client_contributions": ("timestamp", None),
            "role_assignments": ("timestamp", None),
            "agent_messages": ("created_at", None),
            "temporal_logic_history": ("timestamp", None),
            "temporal_logic_violations": ("timestamp", None),
            "xai_explanations": ("timestamp", None),
            "precision_selections": ("timestamp", None),
            "precision_energy_savings": ("timestamp", None),
            "carbon_credit_trades": ("timestamp", None),
            "rec_certificates": ("timestamp", None),
            "carbon_market_prices": ("timestamp", None),
            "carbon_retirement_records": ("timestamp", None),
            "chaos_test_results": ("timestamp", None),
            "recovery_actions": ("timestamp", None),
            "hitl_pending_queries": ("created_at", None),
            "hitl_resolved_queries": ("resolved_at", None),
            "hitl_audit_log": ("timestamp", None),
            "active_learning_samples": ("timestamp", None),
        }

        for table, days in policies.items():
            if days <= 0 or table not in retention_map:
                continue
            ts_col, _ = retention_map[table]
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            try:
                self._execute(f"DELETE FROM {table} WHERE {ts_col} < ?", (cutoff,))
            except Exception as e:
                print(f"WARNING: Failed to apply retention for {table}: {e}")

        self._stats["last_cleanup"] = datetime.now().isoformat()

    # =========================================================================
    # STORAGE STATISTICS
    # =========================================================================
    def update_statistics(self) -> None:
        tables = [
            # v1
            "model_weights", "feedback_events", "drift_states", "benchmark_runs",
            "kv_store", "pqc_keys",
            # v2
            "power_readings", "elasticity_metrics", "substitution_results",
            "federated_rounds", "circularity_records", "emission_records",
            "optimisation_history", "distribution_history", "thermal_optimizations",
            # v3
            "ga_populations", "ga_fitness_history", "moe_gating_training",
            "moe_expert_metadata", "pareto_front", "user_preferences",
            "scenarios", "decision_catalogue",
            # v4
            "limit_graph_nodes", "limit_graph_edges", "limit_graph_constraints",
            "rlhf_feedback", "rlhf_reward_model",
            "distillation_student_policy", "distillation_teacher_policies",
            # v5 – Quantum / Causal
            "quantum_distillation_state", "quantum_teacher_weights",
            "causal_graphs", "counterfactual_estimates", "ate_history",
            # v5 – Federated / Multi-Agent
            "federated_global_models", "federated_client_contributions",
            "federated_staleness", "agent_registry", "role_assignments",
            "agent_messages",
            # v5 – Temporal / XAI / Precision
            "temporal_logic_formulas", "temporal_logic_history",
            "temporal_logic_violations", "xai_explanations",
            "precision_selections", "precision_policy_state",
            "precision_energy_savings",
            # v5 – Carbon Markets / Chaos / HITL
            "carbon_credit_trades", "rec_certificates",
            "carbon_market_prices", "carbon_retirement_records",
            "chaos_test_schedules", "chaos_test_results", "recovery_actions",
            "hitl_pending_queries", "hitl_resolved_queries",
            "hitl_audit_log", "active_learning_samples",
        ]
        for table in tables:
            try:
                row = self._fetchone(f"SELECT COUNT(*) as cnt FROM {table}")
                self._stats["table_sizes"][table] = row["cnt"] if row else 0
            except Exception:
                self._stats["table_sizes"][table] = -1
        self._stats["last_updated"] = datetime.now().isoformat()

    def get_statistics(self) -> Dict[str, Any]:
        self.update_statistics()
        return {
            "schema_version": self.SCHEMA_VERSION,
            "table_sizes": self._stats["table_sizes"],
            "total_queries": self._stats["total_queries"],
            "last_updated": self._stats.get("last_updated"),
            "last_cleanup": self._stats.get("last_cleanup"),
        }

    # =========================================================================
    # ASYNC WRAPPERS FOR NEW v5.0.0 METHODS
    # =========================================================================
    # Pattern: async def save_<x>_async(...) → await self._execute_async(...)
    #          async def get_<x>_async(...)   → await self._fetchone_async(...)

    # ---------------- Quantum Distillation ----------------
    async def save_quantum_distillation_state_async(self, state_id, qaoa_parameters,
                                                     qubo_penalties, vqe_energy,
                                                     n_qubits, n_layers):
        await self._execute_async("""INSERT OR REPLACE INTO quantum_distillation_state
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (state_id, json.dumps(qaoa_parameters), json.dumps(qubo_penalties),
             vqe_energy, n_qubits, n_layers, datetime.now().isoformat()))

    async def get_quantum_distillation_state_async(self, state_id):
        row = await self._fetchone_async(
            "SELECT * FROM quantum_distillation_state WHERE state_id = ?",
            (state_id,))
        if row:
            row["qaoa_parameters"] = json.loads(row["qaoa_parameters"])
            row["qubo_penalties"] = json.loads(row["qubo_penalties"])
        return row

    # ---------------- Causal RL ----------------
    async def save_causal_graph_async(self, graph_id, edges, metadata=None):
        await self._execute_async("""INSERT OR REPLACE INTO causal_graphs
            VALUES (?, ?, ?, ?)""",
            (graph_id, json.dumps(edges), json.dumps(metadata or {}),
             datetime.now().isoformat()))

    async def save_counterfactual_estimate_async(self, estimate_id, action,
                                                  potential_outcome,
                                                  propensity_score, confidence,
                                                  context=None):
        await self._execute_async("""INSERT OR REPLACE INTO counterfactual_estimates
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (estimate_id, action, potential_outcome, propensity_score,
             confidence, json.dumps(context or {}), datetime.now().isoformat()))

    # ---------------- Federated ----------------
    async def save_federated_global_model_async(self, round_id, global_weights,
                                                 num_clients, aggregation_strategy):
        await self._execute_async("""INSERT OR REPLACE INTO federated_global_models
            VALUES (?, ?, ?, ?, ?)""",
            (round_id, json.dumps(global_weights), num_clients,
             aggregation_strategy, datetime.now().isoformat()))

    async def save_federated_client_contribution_async(self, contribution_id,
                                                        round_id, client_id,
                                                        weights, samples, loss=None):
        await self._execute_async("""INSERT OR REPLACE INTO federated_client_contributions
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (contribution_id, round_id, client_id, json.dumps(weights),
             samples, loss, datetime.now().isoformat()))

    # ---------------- Multi-Agent ----------------
    async def save_agent_async(self, agent_id, capabilities, current_role,
                                is_active=True, explanation=""):
        await self._execute_async("""INSERT OR REPLACE INTO agent_registry
            VALUES (?, ?, ?, ?, ?, ?)""",
            (agent_id, json.dumps(capabilities), current_role,
             1 if is_active else 0, datetime.now().isoformat(), explanation))

    async def save_role_assignment_async(self, assignment_id, agent_id, role,
                                          softmax_weights, explanation="",
                                          xai_explanation=None):
        await self._execute_async("""INSERT OR REPLACE INTO role_assignments
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (assignment_id, agent_id, role, json.dumps(softmax_weights),
             explanation, json.dumps(xai_explanation or {}),
             datetime.now().isoformat()))

    # ---------------- Temporal Logic ----------------
    async def save_temporal_formula_async(self, formula_id, name, ltl_expression,
                                           description=""):
        await self._execute_async("""INSERT OR REPLACE INTO temporal_logic_formulas
            VALUES (?, ?, ?, ?, ?)""",
            (formula_id, name, ltl_expression, description,
             datetime.now().isoformat()))

    async def save_temporal_violation_async(self, violation_id, formula_name,
                                             expression, state_snapshot,
                                             severity="warning"):
        await self._execute_async("""INSERT OR REPLACE INTO temporal_logic_violations
            VALUES (?, ?, ?, ?, ?, ?)""",
            (violation_id, formula_name, expression,
             json.dumps(state_snapshot), severity,
             datetime.now().isoformat()))

    # ---------------- XAI ----------------
    async def save_xai_explanation_async(self, explanation_id, decision_type,
                                          feature_contributions, top_features,
                                          narrative, weights_used,
                                          reference_id=None):
        await self._execute_async("""INSERT OR REPLACE INTO xai_explanations
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (explanation_id, decision_type, reference_id,
             json.dumps(feature_contributions), json.dumps(top_features),
             json.dumps(narrative), json.dumps(weights_used),
             datetime.now().isoformat()))

    async def get_xai_explanations_by_type_async(self, decision_type, limit=50):
        rows = await self._fetchall_async("""SELECT * FROM xai_explanations
            WHERE decision_type = ? ORDER BY timestamp DESC LIMIT ?""",
            (decision_type, limit))
        for r in rows:
            for k in ("feature_contributions", "top_features",
                      "narrative", "weights_used"):
                r[k] = json.loads(r[k])
        return rows

    # ---------------- Adaptive Precision ----------------
    async def save_precision_selection_async(self, selection_id, precision_level,
                                              carbon_intensity, gpu_available,
                                              gpu_utilization, memory_gb,
                                              accuracy_required,
                                              energy_savings=None):
        await self._execute_async("""INSERT OR REPLACE INTO precision_selections
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (selection_id, precision_level, carbon_intensity,
             1 if gpu_available else 0, gpu_utilization, memory_gb,
             accuracy_required, energy_savings, datetime.now().isoformat()))

    async def save_precision_policy_state_async(self, state_id, policy_weights):
        await self._execute_async("""INSERT OR REPLACE INTO precision_policy_state
            VALUES (?, ?, ?)""",
            (state_id, json.dumps(policy_weights), datetime.now().isoformat()))

    # ---------------- Carbon Markets ----------------
    async def save_carbon_credit_trade_async(self, trade_id, trade_type,
                                              amount_kg, price_usd,
                                              beneficiary="", market=""):
        await self._execute_async("""INSERT OR REPLACE INTO carbon_credit_trades
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (trade_id, trade_type, amount_kg, price_usd, beneficiary,
             market, datetime.now().isoformat()))

    async def save_rec_certificate_async(self, cert_id, energy_mwh, source,
                                          vintage, serial_number, beneficiary=""):
        await self._execute_async("""INSERT OR REPLACE INTO rec_certificates
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (cert_id, energy_mwh, source, vintage, serial_number, 0,
             beneficiary, datetime.now().isoformat()))

    async def save_carbon_market_price_async(self, price_id, carbon_price_usd_per_ton,
                                              rec_price_usd_per_mwh,
                                              grid_intensity_kg_per_mwh,
                                              region="global"):
        await self._execute_async("""INSERT OR REPLACE INTO carbon_market_prices
            VALUES (?, ?, ?, ?, ?, ?)""",
            (price_id, carbon_price_usd_per_ton, rec_price_usd_per_mwh,
             grid_intensity_kg_per_mwh, region, datetime.now().isoformat()))

    # ---------------- Chaos ----------------
    async def save_chaos_test_result_async(self, test_id, fault_type,
                                            duration_seconds, passed,
                                            error_message="",
                                            recovery_time_ms=0.0,
                                            target_module=""):
        await self._execute_async("""INSERT OR REPLACE INTO chaos_test_results
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (test_id, fault_type, duration_seconds, 1 if passed else 0,
             error_message, recovery_time_ms, target_module,
             datetime.now().isoformat()))

    async def save_recovery_action_async(self, action_id, trigger_type,
                                          action, outcome=""):
        await self._execute_async("""INSERT OR REPLACE INTO recovery_actions
            VALUES (?, ?, ?, ?, ?)""",
            (action_id, trigger_type, action, outcome,
             datetime.now().isoformat()))

    # ---------------- HITL ----------------
    async def save_hitl_query_async(self, query_id, context, options,
                                     uncertainty, status="pending"):
        await self._execute_async("""INSERT OR REPLACE INTO hitl_pending_queries
            VALUES (?, ?, ?, ?, ?, ?)""",
            (query_id, json.dumps(context), json.dumps(options),
             uncertainty, status, datetime.now().isoformat()))

    async def resolve_hitl_query_async(self, query_id, chosen_action,
                                        rating=1.0, resolution_source="human"):
        await self._execute_async("""INSERT OR REPLACE INTO hitl_resolved_queries
            VALUES (?, ?, ?, ?, ?)""",
            (query_id, chosen_action, rating, resolution_source,
             datetime.now().isoformat()))
        await self._execute_async(
            "UPDATE hitl_pending_queries SET status='resolved' WHERE query_id = ?",
            (query_id,))

    async def save_hitl_audit_async(self, log_id, decision, chosen,
                                     confidence, query_id="", context=None):
        await self._execute_async("""INSERT OR REPLACE INTO hitl_audit_log
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (log_id, decision, chosen, confidence, query_id,
             json.dumps(context or {}), datetime.now().isoformat()))

    async def save_active_learning_sample_async(self, sample_id, state,
                                                 uncertainty,
                                                 selected_for_review=False,
                                                 outcome=""):
        await self._execute_async("""INSERT OR REPLACE INTO active_learning_samples
            VALUES (?, ?, ?, ?, ?, ?)""",
            (sample_id, json.dumps(state), uncertainty,
             1 if selected_for_review else 0, outcome,
             datetime.now().isoformat()))

    # =========================================================================
    # SYNC-TO-ASYNC BRIDGE FOR EXISTING METHODS
    # =========================================================================
    async def save_model_weights_async(self, model_id, weights_bytes):
        await self._execute_async(
            "INSERT OR REPLACE INTO model_weights VALUES (?, ?, ?)",
            (model_id, weights_bytes, time.time()))

    async def load_model_weights_async(self, model_id):
        row = await self._fetchone_async(
            "SELECT weights FROM model_weights WHERE model_id = ?", (model_id,))
        return row["weights"] if row else None

    async def save_state_async(self, key, value):
        await self._execute_async("INSERT OR REPLACE INTO kv_store VALUES (?, ?, ?)",
                                   (key, value, datetime.now().isoformat()))

    async def get_state_async(self, key):
        row = await self._fetchone_async("SELECT value FROM kv_store WHERE key = ?",
                                          (key,))
        return row["value"] if row else None

    async def save_rlhf_feedback_async(self, feedback_id, state, action, reward):
        await self._execute_async("""INSERT OR REPLACE INTO rlhf_feedback
            VALUES (?, ?, ?, ?, ?)""",
            (feedback_id, json.dumps(state), action, reward,
             datetime.now().isoformat()))

    async def save_limit_graph_node_async(self, node_id, node_type, attributes):
        await self._execute_async("""INSERT OR REPLACE INTO limit_graph_nodes
            VALUES (?, ?, ?, ?)""",
            (node_id, node_type, json.dumps(attributes),
             datetime.now().isoformat()))

    async def get_limit_graph_node_async(self, node_id):
        row = await self._fetchone_async(
            "SELECT * FROM limit_graph_nodes WHERE node_id = ?", (node_id,))
        if row:
            row["attributes"] = json.loads(row["attributes"])
        return row

    # =========================================================================
    # Close / cleanup
    # =========================================================================
    def close(self):
        if hasattr(self, '_local') and hasattr(self._local, 'conn'):
            self._local.conn.close()
            del self._local.conn

    async def close_async(self):
        if AIOSQLITE_AVAILABLE and self._async_conn:
            await self._async_conn.close()
            self._async_conn = None
