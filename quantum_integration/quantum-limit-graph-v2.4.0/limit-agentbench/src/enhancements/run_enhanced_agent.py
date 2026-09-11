#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/storage_v5_0_0.py
# VERSION: 5.0.0 – Central Storage with full ten-enhancement support
# =============================================================================
"""
Central Storage module for Green Agent enhancements – Version 5.0.0

v5.0.0 additions (schema migration v4 → v5) — full support for the ten
advanced Green Agent enhancements:

  1. Quantum-Distillation            → teacher_superpositions
  2. Causal RL                       → causal_graph, causal_experiments,
                                       causal_interventions
  3. Federated Green Learning        → federated_weights, federated_clients,
                                       federated_aggregation_log
  4. Multi-Agent Coordination        → agent_registry, agent_messages,
                                       agent_bids, agent_reputation_history
  5. Temporal Logic                  → temporal_rules, temporal_trace,
                                       temporal_violations
  6. Explainable AI                  → xai_explanations,
                                       xai_feature_importance
  7. Adaptive Precision              → precision_history,
                                       hardware_profiles
  8. Carbon Markets / REC            → carbon_credit_prices, rec_ledger,
                                       net_zero_matches, carbon_market_trades
  9. Chaos Testing                   → chaos_experiments, chaos_steady_states,
                                       chaos_rollbacks
 10. HITL Active Learning            → hitl_approval_queue, hitl_decisions,
                                       active_learning_samples

All v1–v4 tables are preserved. All methods remain synchronous with
async wrappers via `asyncio.to_thread` or `aiosqlite` if available.
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

    v5.0.0 features:
    - Schema versioning with incremental migrations (v1 → v5).
    - Full support for the ten advanced Green Agent enhancements.
    - Connection pooling (thread-local) + optional aiosqlite for async.
    - WAL mode, foreign keys, busy timeout.
    - Optional AES-GCM encryption for sensitive BLOB fields.
    - Configurable data retention policies and storage statistics.
    """

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

        # Async connection (if aiosqlite available)
        self._async_conn = None

        # Statistics
        self._stats: Dict[str, Any] = {
            "table_sizes": {},
            "last_cleanup": None,
            "total_queries": 0,
        }

        self._init_db()

    # =========================================================================
    # Connection management
    # =========================================================================
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn'):
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=5000;")
            self._local.conn = conn
        return self._local.conn

    async def _get_async_connection(self) -> aiosqlite.Connection:
        if AIOSQLITE_AVAILABLE:
            conn = await aiosqlite.connect(self.db_path)
            await conn.execute("PRAGMA journal_mode=WAL;")
            await conn.execute("PRAGMA foreign_keys=ON;")
            await conn.execute("PRAGMA busy_timeout=5000;")
            return conn
        raise RuntimeError("aiosqlite not available")

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
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
        if AIOSQLITE_AVAILABLE:
            async with await self._get_async_connection() as conn:
                cursor = await conn.execute(sql, params)
                await conn.commit()
                return cursor
        return await asyncio.to_thread(self._execute, sql, params)

    async def _fetchone_async(self, sql: str, params: tuple = ()):
        if AIOSQLITE_AVAILABLE:
            async with await self._get_async_connection() as conn:
                cursor = await conn.execute(sql, params)
                row = await cursor.fetchone()
                return dict(row) if row else None
        return await asyncio.to_thread(self._fetchone, sql, params)

    async def _fetchall_async(self, sql: str, params: tuple = ()):
        if AIOSQLITE_AVAILABLE:
            async with await self._get_async_connection() as conn:
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        return await asyncio.to_thread(self._fetchall, sql, params)

    def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        cursor = self._execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        cursor = self._execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    # =========================================================================
    # Schema initialisation and incremental migrations
    # =========================================================================
    def _init_db(self) -> None:
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

    # -------------------------------------------------------------------------
    # v1 — core tables
    # -------------------------------------------------------------------------
    def _migrate_to_v1(self, conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS model_weights (
                model_id TEXT PRIMARY KEY, weights BLOB, timestamp REAL)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback_events (
                event_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
                task_id TEXT NOT NULL, model_id TEXT, teacher_id TEXT,
                selected_action TEXT NOT NULL, quality_score REAL NOT NULL,
                latency_ms REAL NOT NULL, energy_joules REAL NOT NULL,
                carbon_g REAL NOT NULL, helium_cost REAL, resource_usage TEXT,
                distillation_loss REAL, feedback_type TEXT NOT NULL,
                adaptive_cost_value REAL NOT NULL, metadata TEXT)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drift_states (
                snapshot_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
                online_weights BLOB, offline_weights BLOB,
                cost_score REAL, reason TEXT)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_runs (
                run_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
                policy_name TEXT NOT NULL, avg_quality REAL, avg_carbon REAL,
                avg_latency REAL, avg_cost REAL, total_energy REAL,
                sample_count INTEGER)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP)
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_task ON feedback_events(task_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_time ON feedback_events(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_policy ON benchmark_runs(policy_name);")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pqc_keys (
                key_id TEXT PRIMARY KEY, algorithm TEXT NOT NULL,
                public_key BLOB NOT NULL, public_nonce BLOB NOT NULL,
                private_key BLOB NOT NULL, private_nonce BLOB NOT NULL,
                created_at TEXT NOT NULL, expires_at TEXT NOT NULL)
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pqc_expires ON pqc_keys(expires_at);")

    # -------------------------------------------------------------------------
    # v2 — power / elasticity / substitution / federated / circularity /
    #       emission / optimisation / distribution / thermal
    # -------------------------------------------------------------------------
    def _migrate_to_v2(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS power_readings (
            reading_id TEXT PRIMARY KEY, power_watts REAL,
            carbon_intensity REAL, timestamp TEXT, metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS elasticity_metrics (
            metric_id TEXT PRIMARY KEY, price_elasticity REAL,
            scarcity_elasticity REAL, cross_elasticity REAL,
            substitution_elasticity REAL, thermal_elasticity REAL,
            composite_elasticity REAL, scarcity_index REAL,
            quality_score REAL, data_quality_score REAL,
            market_regime TEXT, migration_urgency TEXT,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS substitution_results (
            analysis_id TEXT PRIMARY KEY, base_material TEXT,
            substitute TEXT, topsis_score REAL, carbon_reduction_pct REAL,
            cost_savings_pct REAL, sustainability_score REAL,
            confidence_score REAL, quality_score REAL,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_rounds (
            round_id INTEGER PRIMARY KEY, num_clients INTEGER,
            global_accuracy REAL, aggregated_loss REAL, strategy TEXT,
            carbon_footprint REAL, energy_used REAL,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS circularity_records (
            record_id TEXT PRIMARY KEY, circularity_index REAL,
            circularity_level TEXT, recycling_rate REAL,
            recovery_efficiency REAL, collection_efficiency REAL,
            purification_efficiency REAL, data_quality_score REAL,
            tx_hash TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS emission_records (
            record_id TEXT PRIMARY KEY, scope TEXT, amount_kg REAL,
            source TEXT, location TEXT, verified INTEGER, region TEXT,
            user_id TEXT, timestamp TEXT, metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS optimisation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL, result TEXT, timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS distribution_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            optimal_provider TEXT NOT NULL, optimal_region TEXT NOT NULL,
            scores TEXT, data_size_gb REAL, timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS thermal_optimizations (
            id TEXT PRIMARY KEY, data TEXT, timestamp TEXT)""")
        for t, c in [
            ("power_readings", "timestamp"), ("elasticity_metrics", "timestamp"),
            ("substitution_results", "timestamp"), ("federated_rounds", "timestamp"),
            ("circularity_records", "timestamp"), ("emission_records", "timestamp"),
            ("optimisation_history", "timestamp"), ("distribution_history", "timestamp"),
            ("thermal_optimizations", "timestamp"),
        ]:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_time ON {t}({c});")

    # -------------------------------------------------------------------------
    # v3 — GA / MoE / Pareto / user prefs / scenarios / decision catalogue
    # -------------------------------------------------------------------------
    def _migrate_to_v3(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_populations (
            generation INTEGER, individual_id TEXT, attributes TEXT,
            fitness REAL, timestamp TEXT,
            PRIMARY KEY (generation, individual_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS ga_fitness_history (
            generation INTEGER PRIMARY KEY, best_fitness REAL,
            avg_fitness REAL, diversity REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS moe_gating_training (
            sample_id TEXT PRIMARY KEY, features TEXT,
            expert_label INTEGER, reward REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS moe_expert_metadata (
            expert_id TEXT PRIMARY KEY, name TEXT, description TEXT,
            performance_score REAL, last_updated TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS pareto_front (
            solution_id TEXT PRIMARY KEY, decision_attributes TEXT,
            accuracy REAL, carbon REAL, cost REAL, robustness REAL,
            is_current INTEGER, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pareto_current ON pareto_front(is_current);")
        conn.execute("""CREATE TABLE IF NOT EXISTS user_preferences (
            user_id TEXT, weights TEXT, chosen_solution_id TEXT,
            timestamp TEXT, PRIMARY KEY (user_id, timestamp))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scenarios (
            scenario_id TEXT PRIMARY KEY, carbon_price REAL,
            discount_rate REAL, demand_growth_rate REAL,
            technology_cost_reduction REAL, regulatory_risk REAL,
            renewable_energy_share REAL, energy_efficiency REAL,
            timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS decision_catalogue (
            option_id TEXT PRIMARY KEY, name TEXT,
            attributes TEXT, timestamp TEXT)""")
        for t, c in [
            ("ga_populations", "generation"),
            ("moe_gating_training", "timestamp"),
            ("user_preferences", "user_id"),
            ("scenarios", "timestamp"),
            ("decision_catalogue", "timestamp"),
        ]:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_k ON {t}({c});")

    # -------------------------------------------------------------------------
    # v4 — LIMIT Graph / MODP / RLHF / distillation / bio / MoE models
    # -------------------------------------------------------------------------
    def _migrate_to_v4(self, conn: sqlite3.Connection) -> None:
        conn.execute("""CREATE TABLE IF NOT EXISTS limit_graph_nodes (
            node_id TEXT PRIMARY KEY, graph_id TEXT NOT NULL,
            node_type TEXT, attributes TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS limit_graph_edges (
            edge_id TEXT PRIMARY KEY, graph_id TEXT NOT NULL,
            source_node TEXT NOT NULL, target_node TEXT NOT NULL,
            weight REAL, attributes TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS limit_graph_metadata (
            graph_id TEXT PRIMARY KEY, description TEXT,
            configuration TEXT, created_at TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_limit_graph_nodes_graph ON limit_graph_nodes(graph_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_limit_graph_edges_graph ON limit_graph_edges(graph_id);")

        conn.execute("""CREATE TABLE IF NOT EXISTS modp_states (
            state_id TEXT PRIMARY KEY, problem_id TEXT NOT NULL,
            state_attributes TEXT, objective_values TEXT,
            stage INTEGER, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS modp_transitions (
            transition_id TEXT PRIMARY KEY, problem_id TEXT NOT NULL,
            from_state TEXT NOT NULL, to_state TEXT NOT NULL,
            action TEXT, cost REAL, objective_deltas TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS modp_policies (
            policy_id TEXT PRIMARY KEY, problem_id TEXT NOT NULL,
            state_id TEXT NOT NULL, action TEXT,
            expected_objectives TEXT, timestamp TEXT)""")
        for t in ["modp_states", "modp_transitions", "modp_policies"]:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_problem ON {t}(problem_id);")

        conn.execute("""CREATE TABLE IF NOT EXISTS rlhf_preference_pairs (
            pair_id TEXT PRIMARY KEY, prompt TEXT, chosen_response TEXT,
            rejected_response TEXT, reward_difference REAL,
            metadata TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rlhf_time ON rlhf_preference_pairs(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS teacher_policies (
            teacher_id TEXT PRIMARY KEY, policy_name TEXT, architecture TEXT,
            parameters BLOB, performance_score REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS distillation_episodes (
            episode_id TEXT PRIMARY KEY, student_policy_id TEXT,
            teacher_policy_ids TEXT, state_features TEXT,
            teacher_actions TEXT, student_action TEXT,
            loss REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS student_policy_updates (
            update_id TEXT PRIMARY KEY, student_policy_id TEXT,
            parameters_before BLOB, parameters_after BLOB,
            loss_before REAL, loss_after REAL, timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS bio_inspired_runs (
            run_id TEXT PRIMARY KEY, algorithm TEXT NOT NULL,
            problem_id TEXT, parameters TEXT, best_solution TEXT,
            best_fitness REAL, timestamp TEXT)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS moe_expert_models (
            expert_id TEXT PRIMARY KEY, model_type TEXT,
            parameters BLOB, version TEXT, training_timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS moe_routing_history (
            routing_id TEXT PRIMARY KEY, sample_id TEXT,
            routed_expert_id TEXT, gating_score REAL, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_moe_routing_time ON moe_routing_history(timestamp);")

    # =========================================================================
    # v5 — the ten advanced Green Agent enhancements
    # =========================================================================
    def _migrate_to_v5(self, conn: sqlite3.Connection) -> None:
        """
        v5 introduces 17 new tables covering the ten advanced enhancements:
          1. Quantum-Distillation           → teacher_superpositions
          2. Causal RL                      → causal_graph, causal_experiments,
                                              causal_interventions
          3. Federated Green Learning       → federated_weights, federated_clients,
                                              federated_aggregation_log
          4. Multi-Agent Coordination       → agent_registry, agent_bids,
                                              agent_messages,
                                              agent_reputation_history
          5. Temporal Logic                 → temporal_rules, temporal_trace,
                                              temporal_violations
          6. Explainable AI                 → xai_explanations,
                                              xai_feature_importance
          7. Adaptive Precision             → precision_history,
                                              hardware_profiles
          8. Carbon Markets / REC           → carbon_credit_prices, rec_ledger,
                                              net_zero_matches,
                                              carbon_market_trades
          9. Chaos Testing                  → chaos_experiments,
                                              chaos_steady_states,
                                              chaos_rollbacks
         10. HITL Active Learning           → hitl_approval_queue,
                                              hitl_decisions,
                                              active_learning_samples
        """
        # --- 1. Quantum-Distillation: teacher superposition weights ---
        conn.execute("""CREATE TABLE IF NOT EXISTS teacher_superpositions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id TEXT, teacher_id TEXT, teacher_weight REAL,
            temperature REAL, superposition_amplitude REAL,
            kl_divergence REAL, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ts_student ON teacher_superpositions(student_id);")

        # --- 2. Causal RL ---
        conn.execute("""CREATE TABLE IF NOT EXISTS causal_graph (
            edge_id TEXT PRIMARY KEY, source TEXT NOT NULL,
            target TEXT NOT NULL, weight REAL, confidence REAL,
            timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_causal_edge ON causal_graph(source, target);")

        conn.execute("""CREATE TABLE IF NOT EXISTS causal_experiments (
            exp_id TEXT PRIMARY KEY, treatment TEXT, outcome TEXT,
            ate REAL, samples INTEGER, method TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_causal_exp_pair ON causal_experiments(treatment, outcome);")

        conn.execute("""CREATE TABLE IF NOT EXISTS causal_interventions (
            intervention_id TEXT PRIMARY KEY, node TEXT, do_value TEXT,
            observed_outcome TEXT, counterfactual_json TEXT,
            timestamp TEXT)""")

        # --- 3. Federated Green Learning ---
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_weights (
            instance_id TEXT, model_id TEXT, weights BLOB,
            weight_norm REAL, round_id INTEGER, timestamp TEXT,
            PRIMARY KEY (instance_id, model_id))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fed_w_model ON federated_weights(model_id);")

        conn.execute("""CREATE TABLE IF NOT EXISTS federated_clients (
            instance_id TEXT PRIMARY KEY, last_seen TEXT,
            reputation REAL, capabilities TEXT, region TEXT,
            carbon_intensity REAL, weights_shared INTEGER DEFAULT 0)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS federated_aggregation_log (
            aggregation_id TEXT PRIMARY KEY, round_id INTEGER,
            instance_ids TEXT, weights_snapshot TEXT,
            aggregation_method TEXT, global_accuracy REAL,
            carbon_footprint REAL, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fed_agg_round ON federated_aggregation_log(round_id);")

        # --- 4. Multi-Agent Coordination ---
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_registry (
            agent_id TEXT PRIMARY KEY, role TEXT, reputation REAL,
            utilities TEXT, capabilities TEXT, created_at TEXT,
            last_updated TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_role ON agent_registry(role);")

        conn.execute("""CREATE TABLE IF NOT EXISTS agent_bids (
            bid_id TEXT PRIMARY KEY, task_id TEXT, agent_id TEXT,
            bid_score REAL, preferred_role TEXT, awarded INTEGER,
            timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_bid_task ON agent_bids(task_id);")

        conn.execute("""CREATE TABLE IF NOT EXISTS agent_messages (
            message_id TEXT PRIMARY KEY, topic TEXT, sender TEXT,
            recipient TEXT, payload TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_msg_topic ON agent_messages(topic);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_msg_time ON agent_messages(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS agent_reputation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT,
            reputation REAL, reason TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_rep_id ON agent_reputation_history(agent_id);")

        # --- 5. Temporal Logic ---
        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_rules (
            rule_id TEXT PRIMARY KEY, formula TEXT, operator TEXT,
            severity TEXT, description TEXT, window_seconds REAL,
            created_at TEXT, active INTEGER DEFAULT 1)""")

        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_trace (
            step INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT,
            context TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_temporal_trace_time ON temporal_trace(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_violations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id TEXT,
            formula TEXT, step INTEGER, state TEXT,
            severity TEXT, approved INTEGER, resolved_at TEXT,
            timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_temporal_viol_rule ON temporal_violations(rule_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_temporal_viol_time ON temporal_violations(timestamp);")

        # --- 6. Explainable AI ---
        conn.execute("""CREATE TABLE IF NOT EXISTS xai_explanations (
            explanation_id TEXT PRIMARY KEY, decision_id TEXT,
            method TEXT, decision_label TEXT, features TEXT,
            attributions TEXT, natural_language TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_xai_decision ON xai_explanations(decision_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_xai_time ON xai_explanations(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS xai_feature_importance (
            id INTEGER PRIMARY KEY AUTOINCREMENT, explanation_id TEXT,
            feature_name TEXT, importance REAL, rank INTEGER)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_xai_fi_exp ON xai_feature_importance(explanation_id);")

        # --- 7. Adaptive Precision ---
        conn.execute("""CREATE TABLE IF NOT EXISTS precision_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, from_p TEXT, to_p TEXT,
            reason TEXT, energy_saved_wh REAL, accuracy_delta REAL,
            timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_precision_time ON precision_history(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS hardware_profiles (
            device_id TEXT PRIMARY KEY, device_name TEXT,
            cuda_available INTEGER, bf16_supported INTEGER,
            max_precision TEXT, memory_gb REAL, last_probed TEXT)""")

        # --- 8. Carbon Markets / REC ---
        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_credit_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT, price_usd REAL,
            currency TEXT, source TEXT, region TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cc_price_time ON carbon_credit_prices(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS rec_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT, mwh REAL,
            price_per_mwh REAL, source TEXT, certificate_id TEXT,
            region TEXT, retired INTEGER DEFAULT 0, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rec_time ON rec_ledger(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS net_zero_matches (
            match_id TEXT PRIMARY KEY, workload_kwh REAL, intensity REAL,
            action TEXT, carbon_kg REAL, offset_cost_usd REAL,
            credit_price_usd REAL, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nz_time ON net_zero_matches(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_market_trades (
            trade_id TEXT PRIMARY KEY, action TEXT, amount REAL,
            price REAL, currency TEXT, executed_at TEXT, tx_hash TEXT)""")

        # --- 9. Chaos Testing ---
        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_experiments (
            experiment_id TEXT PRIMARY KEY, name TEXT, fault_type TEXT,
            blast_radius REAL, steady_before INTEGER, steady_after INTEGER,
            status TEXT, duration_ms REAL, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chaos_time ON chaos_experiments(timestamp);")

        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_steady_states (
            check_id TEXT PRIMARY KEY, experiment_id TEXT, kpi_name TEXT,
            kpi_value REAL, ok INTEGER, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chaos_ss_exp ON chaos_steady_states(experiment_id);")

        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_rollbacks (
            rollback_id TEXT PRIMARY KEY, experiment_id TEXT,
            trigger_reason TEXT, rolled_back_at TEXT)""")

        # --- 10. HITL Active Learning ---
        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_approval_queue (
            request_id TEXT PRIMARY KEY, rule_id TEXT, state TEXT,
            severity TEXT, status TEXT, created_at TEXT, resolved_at TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hitl_status ON hitl_approval_queue(status);")

        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_decisions (
            decision_id TEXT PRIMARY KEY, request_id TEXT, user_id TEXT,
            approved INTEGER, rationale TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hitl_decision_req ON hitl_decisions(request_id);")

        conn.execute("""CREATE TABLE IF NOT EXISTS active_learning_samples (
            sample_id TEXT PRIMARY KEY, model_id TEXT,
            strategy TEXT, uncertainty REAL, selected_for_review INTEGER,
            user_label TEXT, reviewed_at TEXT, timestamp TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_als_model ON active_learning_samples(model_id);")

    # =========================================================================
    # Encryption helpers
    # =========================================================================
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

    def encrypt_existing_blobs(self, table: str, column: str,
                               nonce_column: str, where_clause: str = "1") -> int:
        if not self.master_key or not CRYPTO_AVAILABLE:
            raise RuntimeError("Encryption not available")
        rows = self._fetchall(
            f"SELECT rowid, {column} FROM {table} WHERE {nonce_column} = b'' AND {where_clause}"
        )
        count = 0
        for row in rows:
            ciphertext, nonce = self._encrypt_blob(row[column])
            self._execute(
                f"UPDATE {table} SET {column} = ?, {nonce_column} = ? WHERE rowid = ?",
                (ciphertext, nonce, row["rowid"])
            )
            count += 1
        print(f"Encrypted {count} existing BLOBs in {table}.{column}")
        return count

    # =========================================================================
    # v1 core methods
    # =========================================================================
    def save_model_weights(self, model_id: str, weights_bytes: bytes) -> None:
        self._execute(
            "INSERT OR REPLACE INTO model_weights (model_id, weights, timestamp) VALUES (?, ?, ?)",
            (model_id, weights_bytes, time.time()))

    def load_model_weights(self, model_id: str) -> Optional[bytes]:
        row = self._fetchone("SELECT weights FROM model_weights WHERE model_id = ?", (model_id,))
        return row["weights"] if row else None

    def store_feedback_event(self, event: Dict) -> None:
        self._execute("""
            INSERT OR REPLACE INTO feedback_events (
                event_id, timestamp, task_id, model_id, teacher_id, selected_action,
                quality_score, latency_ms, energy_joules, carbon_g, helium_cost,
                resource_usage, distillation_loss, feedback_type, adaptive_cost_value, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["event_id"], float(event["timestamp"]), event["task_id"],
            event.get("model_id"), event.get("teacher_id"), event["selected_action"],
            float(event["quality_score"]), float(event["latency_ms"]),
            float(event["energy_joules"]), float(event["carbon_g"]),
            event.get("helium_cost"), json.dumps(event.get("resource_usage", {})),
            event.get("distillation_loss"), event["feedback_type"],
            float(event["adaptive_cost_value"]),
            json.dumps(event.get("metadata", {})),
        ))

    def get_feedback_events(self, limit: int = 1000) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM feedback_events ORDER BY timestamp DESC LIMIT ?", (limit,))
        for r in rows:
            for k in ("resource_usage", "metadata"):
                try:
                    r[k] = json.loads(r.get(k) or "{}")
                except Exception:
                    pass
        return rows

    def save_drift_snapshot(self, snapshot_id, online_w, offline_w, cost, reason) -> None:
        self._execute("""
            INSERT OR REPLACE INTO drift_states
            (snapshot_id, timestamp, online_weights, offline_weights, cost_score, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (snapshot_id, time.time(), online_w, offline_w, cost, reason))

    def get_last_snapshot(self) -> Optional[Dict]:
        return self._fetchone("SELECT * FROM drift_states ORDER BY timestamp DESC LIMIT 1")

    def store_benchmark_result(self, run_id, policy, metrics, count) -> None:
        self._execute("""
            INSERT OR REPLACE INTO benchmark_runs
            (run_id, timestamp, policy_name, avg_quality, avg_carbon, avg_latency,
             avg_cost, total_energy, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, time.time(), policy,
              metrics.get("quality"), metrics.get("carbon"),
              metrics.get("latency"), metrics.get("cost"),
              metrics.get("energy"), int(count)))

    def get_benchmark_results(self, days_back: int = 7) -> List[Dict]:
        cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()
        return self._fetchall(
            "SELECT * FROM benchmark_runs WHERE timestamp >= ? ORDER BY timestamp DESC",
            (cutoff,))

    # ---- power / elasticity / substitution / circularity / emissions ----
    def store_power_reading(self, reading: Dict) -> None:
        self._execute("""
            INSERT OR REPLACE INTO power_readings
            (reading_id, power_watts, carbon_intensity, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, (reading["reading_id"], reading["power_watts"],
              reading.get("carbon_intensity"), reading["timestamp"],
              json.dumps(reading.get("metadata", {}))))

    def store_elasticity_metrics(self, metrics) -> None:
        d = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        self._execute("""
            INSERT OR REPLACE INTO elasticity_metrics (
                metric_id, price_elasticity, scarcity_elasticity, cross_elasticity,
                substitution_elasticity, thermal_elasticity, composite_elasticity,
                scarcity_index, quality_score, data_quality_score, market_regime,
                migration_urgency, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["metric_id"], d["price_elasticity"], d["scarcity_elasticity"],
              d["cross_elasticity"], d["substitution_elasticity"],
              d["thermal_elasticity"], d["composite_elasticity"],
              d["scarcity_index"], d["quality_score"], d["data_quality_score"],
              d["market_regime"], d["migration_urgency"],
              d.get("blockchain_tx_hash") or "",
              d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else d["timestamp"]))

    def store_substitution_result(self, result) -> None:
        d = asdict(result) if hasattr(result, "asdict") else result
        self._execute("""
            INSERT OR REPLACE INTO substitution_results (
                analysis_id, base_material, substitute, topsis_score,
                carbon_reduction_pct, cost_savings_pct, sustainability_score,
                confidence_score, quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["calculation_id"], d["base_material"], d["recommended_substitute"],
              d["topsis_score"], d["carbon_reduction_pct"], d["cost_savings_pct"],
              d["sustainability_score"], d["confidence_score"],
              d["data_quality_score"], d.get("blockchain_tx_hash") or "",
              d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else d["timestamp"]))

    def store_federated_round(self, result) -> None:
        d = asdict(result) if hasattr(result, "asdict") else result
        self._execute("""
            INSERT OR REPLACE INTO federated_rounds (
                round_id, num_clients, global_accuracy, aggregated_loss,
                strategy, carbon_footprint, energy_used, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["round_id"], d["num_clients"], d["global_accuracy"],
              d["aggregated_loss"], d["strategy"], d["carbon_footprint"],
              d["energy_used"], d.get("blockchain_tx_hash") or "",
              d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else d["timestamp"]))

    def store_circularity_record(self, metrics) -> None:
        d = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        self._execute("""
            INSERT OR REPLACE INTO circularity_records (
                record_id, circularity_index, circularity_level, recycling_rate,
                recovery_efficiency, collection_efficiency,
                purification_efficiency, data_quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["record_id"], d["circularity_index"], d["circularity_level"],
              d["recycling_rate"], d["recovery_efficiency"], d["collection_efficiency"],
              d["purification_efficiency"], d["data_quality_score"],
              d.get("blockchain_tx_hash") or "",
              d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else d["timestamp"]))

    def store_emission_record(self, record: Dict) -> None:
        self._execute("""
            INSERT OR REPLACE INTO emission_records
            (record_id, scope, amount_kg, source, location, verified,
             region, user_id, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (record["record_id"], record["scope"], record["amount_kg"],
              record["source"], record["location"],
              1 if record.get("verified") else 0,
              record["region"], record["user_id"], record["timestamp"],
              json.dumps(record.get("metadata", {}))))

    def save_optimisation(self, strategy: str, result: Dict) -> None:
        self._execute("INSERT INTO optimisation_history (strategy, result, timestamp) VALUES (?, ?, ?)",
                      (strategy, json.dumps(result), datetime.now().isoformat()))

    def save_distribution(self, result: Dict) -> None:
        self._execute("""
            INSERT INTO distribution_history
            (optimal_provider, optimal_region, scores, data_size_gb, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (result["optimal_provider"], result["optimal_region"],
              json.dumps(result["scores"]), result.get("data_size_gb", 0),
              result["timestamp"]))

    def store_thermal_optimization(self, result) -> None:
        d = asdict(result) if hasattr(result, "asdict") else result
        self._execute("""
            INSERT OR REPLACE INTO thermal_optimizations (id, data, timestamp)
            VALUES (?, ?, ?)
        """, (d.get("id", f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
              json.dumps(d, default=str), datetime.now().isoformat()))

    # ---- KV + PQC ----
    def save_state(self, key: str, value: str) -> None:
        self._execute("INSERT OR REPLACE INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
                      (key, value, datetime.now().isoformat()))

    def get_state(self, key: str) -> Optional[str]:
        row = self._fetchone("SELECT value FROM kv_store WHERE key = ?", (key,))
        return row["value"] if row else None

    def delete_state(self, key: str) -> None:
        self._execute("DELETE FROM kv_store WHERE key = ?", (key,))

    def save_pqc_key(self, key_id, algorithm, public_key, private_key, expires_at) -> None:
        pub_c, pub_n = self._encrypt_blob(public_key)
        priv_c, priv_n = self._encrypt_blob(private_key)
        self._execute("""
            INSERT OR REPLACE INTO pqc_keys
            (key_id, algorithm, public_key, public_nonce, private_key,
             private_nonce, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (key_id, algorithm, pub_c, pub_n, priv_c, priv_n,
              datetime.now().isoformat(), expires_at))

    def get_pqc_key(self, key_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM pqc_keys WHERE key_id = ?", (key_id,))
        if row:
            return {
                "key_id": row["key_id"], "algorithm": row["algorithm"],
                "public_key": self._decrypt_blob(row["public_key"], row["public_nonce"]),
                "private_key": self._decrypt_blob(row["private_key"], row["private_nonce"]),
                "created_at": row["created_at"], "expires_at": row["expires_at"],
            }
        return None

    def list_pqc_keys(self) -> List[str]:
        return [r["key_id"] for r in self._fetchall("SELECT key_id FROM pqc_keys")]

    def delete_pqc_key(self, key_id: str) -> None:
        self._execute("DELETE FROM pqc_keys WHERE key_id = ?", (key_id,))

    # =========================================================================
    # v3 methods (GA / MoE / Pareto / preferences / scenarios / catalogue)
    # =========================================================================
    def save_ga_population(self, generation: int, individuals: List[Dict]) -> None:
        for ind in individuals:
            self._execute("""
                INSERT OR REPLACE INTO ga_populations
                (generation, individual_id, attributes, fitness, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (generation, ind["individual_id"], json.dumps(ind["attributes"]),
                  ind["fitness"], datetime.now().isoformat()))

    def get_ga_population(self, generation: int) -> List[Dict]:
        rows = self._fetchall(
            "SELECT individual_id, attributes, fitness FROM ga_populations WHERE generation = ?",
            (generation,))
        return [{"individual_id": r["individual_id"],
                 "attributes": json.loads(r["attributes"]),
                 "fitness": r["fitness"]} for r in rows]

    def save_ga_fitness_history(self, generation, best_fitness, avg_fitness, diversity) -> None:
        self._execute("""
            INSERT OR REPLACE INTO ga_fitness_history
            (generation, best_fitness, avg_fitness, diversity, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (generation, best_fitness, avg_fitness, diversity, datetime.now().isoformat()))

    def save_moe_training_sample(self, sample_id, features, expert_label, reward) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_gating_training
            (sample_id, features, expert_label, reward, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (sample_id, json.dumps(features), expert_label, reward, datetime.now().isoformat()))

    def save_moe_expert_metadata(self, expert_id, name, description, performance_score) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_expert_metadata
            (expert_id, name, description, performance_score, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, (expert_id, name, description, performance_score, datetime.now().isoformat()))

    def save_pareto_front(self, solutions: List[Dict]) -> None:
        self._execute("UPDATE pareto_front SET is_current = 0")
        for s in solutions:
            self._execute("""
                INSERT OR REPLACE INTO pareto_front
                (solution_id, decision_attributes, accuracy, carbon, cost,
                 robustness, is_current, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (s["solution_id"], json.dumps(s["decision_attributes"]),
                  s["accuracy"], s["carbon"], s["cost"], s["robustness"], 1,
                  datetime.now().isoformat()))

    def get_current_pareto_front(self) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM pareto_front WHERE is_current = 1 ORDER BY accuracy DESC")
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    def save_user_preference(self, user_id, weights, chosen_solution_id=None) -> None:
        self._execute("""
            INSERT INTO user_preferences (user_id, weights, chosen_solution_id, timestamp)
            VALUES (?, ?, ?, ?)
        """, (user_id, json.dumps(weights), chosen_solution_id, datetime.now().isoformat()))

    def get_latest_user_preference(self, user_id: str) -> Optional[Dict]:
        row = self._fetchone("""
            SELECT weights, chosen_solution_id, timestamp FROM user_preferences
            WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1
        """, (user_id,))
        if row:
            row["weights"] = json.loads(row["weights"])
        return row

    def save_scenario(self, scenario_id: str, scenario: Dict) -> None:
        self._execute("""
            INSERT OR REPLACE INTO scenarios
            (scenario_id, carbon_price, discount_rate, demand_growth_rate,
             technology_cost_reduction, regulatory_risk, renewable_energy_share,
             energy_efficiency, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (scenario_id, scenario.get("carbon_price", 50.0),
              scenario.get("discount_rate", 0.05),
              scenario.get("demand_growth_rate", 0.02),
              scenario.get("technology_cost_reduction", 0.1),
              scenario.get("regulatory_risk", 0.3),
              scenario.get("renewable_energy_share", 0.3),
              scenario.get("energy_efficiency", 0.7),
              datetime.now().isoformat()))

    def save_decision_option(self, option_id, name, attributes) -> None:
        self._execute("""
            INSERT OR REPLACE INTO decision_catalogue
            (option_id, name, attributes, timestamp)
            VALUES (?, ?, ?, ?)
        """, (option_id, name, json.dumps(attributes), datetime.now().isoformat()))

    # =========================================================================
    # v4 methods (LIMIT Graph / MODP / RLHF / distillation / bio / MoE)
    # =========================================================================
    def save_limit_graph_node(self, node_id, graph_id, node_type, attributes) -> None:
        self._execute("""
            INSERT OR REPLACE INTO limit_graph_nodes
            (node_id, graph_id, node_type, attributes, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (node_id, graph_id, node_type, json.dumps(attributes), datetime.now().isoformat()))

    def get_limit_graph_nodes(self, graph_id: str) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM limit_graph_nodes WHERE graph_id = ?", (graph_id,))
        for r in rows:
            r["attributes"] = json.loads(r["attributes"]) if r["attributes"] else {}
        return rows

    def save_limit_graph_edge(self, edge_id, graph_id, source, target, weight, attributes) -> None:
        self._execute("""
            INSERT OR REPLACE INTO limit_graph_edges
            (edge_id, graph_id, source_node, target_node, weight, attributes, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (edge_id, graph_id, source, target, weight,
              json.dumps(attributes), datetime.now().isoformat()))

    def get_limit_graph_edges(self, graph_id: str) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM limit_graph_edges WHERE graph_id = ?", (graph_id,))
        for r in rows:
            r["attributes"] = json.loads(r["attributes"]) if r["attributes"] else {}
        return rows

    def save_limit_graph_metadata(self, graph_id, description, configuration) -> None:
        self._execute("""
            INSERT OR REPLACE INTO limit_graph_metadata
            (graph_id, description, configuration, created_at)
            VALUES (?, ?, ?, ?)
        """, (graph_id, description, json.dumps(configuration), datetime.now().isoformat()))

    def save_modp_state(self, state_id, problem_id, state_attributes, objective_values, stage) -> None:
        self._execute("""
            INSERT OR REPLACE INTO modp_states
            (state_id, problem_id, state_attributes, objective_values, stage, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (state_id, problem_id, json.dumps(state_attributes),
              json.dumps(objective_values), stage, datetime.now().isoformat()))

    def save_modp_transition(self, transition_id, problem_id, from_state, to_state,
                             action, cost, objective_deltas) -> None:
        self._execute("""
            INSERT OR REPLACE INTO modp_transitions
            (transition_id, problem_id, from_state, to_state, action, cost,
             objective_deltas, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (transition_id, problem_id, from_state, to_state, action, cost,
              json.dumps(objective_deltas), datetime.now().isoformat()))

    def save_modp_policy(self, policy_id, problem_id, state_id, action, expected_objectives) -> None:
        self._execute("""
            INSERT OR REPLACE INTO modp_policies
            (policy_id, problem_id, state_id, action, expected_objectives, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (policy_id, problem_id, state_id, action,
              json.dumps(expected_objectives), datetime.now().isoformat()))

    def save_preference_pair(self, pair_id, prompt, chosen, rejected,
                             reward_diff, metadata=None) -> None:
        self._execute("""
            INSERT OR REPLACE INTO rlhf_preference_pairs
            (pair_id, prompt, chosen_response, rejected_response,
             reward_difference, metadata, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pair_id, prompt, chosen, rejected, reward_diff,
              json.dumps(metadata) if metadata else None,
              datetime.now().isoformat()))

    def get_preference_pairs(self, limit: int = 100) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM rlhf_preference_pairs ORDER BY timestamp DESC LIMIT ?", (limit,))
        for r in rows:
            r["metadata"] = json.loads(r["metadata"]) if r["metadata"] else {}
        return rows

    def save_teacher_policy(self, teacher_id, policy_name, architecture,
                            parameters, performance_score) -> None:
        self._execute("""
            INSERT OR REPLACE INTO teacher_policies
            (teacher_id, policy_name, architecture, parameters,
             performance_score, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (teacher_id, policy_name, architecture, parameters,
              performance_score, datetime.now().isoformat()))

    def save_distillation_episode(self, episode_id, student_policy_id,
                                  teacher_ids, state_features, teacher_actions,
                                  student_action, loss) -> None:
        self._execute("""
            INSERT OR REPLACE INTO distillation_episodes
            (episode_id, student_policy_id, teacher_policy_ids,
             state_features, teacher_actions, student_action, loss, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (episode_id, student_policy_id, json.dumps(teacher_ids),
              json.dumps(state_features), json.dumps(teacher_actions),
              student_action, loss, datetime.now().isoformat()))

    def save_student_update(self, update_id, student_policy_id,
                            parameters_before, parameters_after,
                            loss_before, loss_after) -> None:
        self._execute("""
            INSERT OR REPLACE INTO student_policy_updates
            (update_id, student_policy_id, parameters_before,
             parameters_after, loss_before, loss_after, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (update_id, student_policy_id, parameters_before,
              parameters_after, loss_before, loss_after, datetime.now().isoformat()))

    def save_bio_run(self, run_id, algorithm, problem_id,
                     parameters, best_solution, best_fitness) -> None:
        self._execute("""
            INSERT OR REPLACE INTO bio_inspired_runs
            (run_id, algorithm, problem_id, parameters,
             best_solution, best_fitness, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (run_id, algorithm, problem_id, json.dumps(parameters),
              json.dumps(best_solution), best_fitness, datetime.now().isoformat()))

    def save_expert_model(self, expert_id, model_type, parameters, version) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_expert_models
            (expert_id, model_type, parameters, version, training_timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (expert_id, model_type, parameters, version, datetime.now().isoformat()))

    def log_routing_decision(self, routing_id, sample_id, routed_expert_id, gating_score) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_routing_history
            (routing_id, sample_id, routed_expert_id, gating_score, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (routing_id, sample_id, routed_expert_id, gating_score, datetime.now().isoformat()))

    # =========================================================================
    # v5 methods (ten advanced enhancements)
    # =========================================================================

    # --- 1. Quantum-Distillation: teacher superpositions ------------------
    def save_teacher_superposition(self, student_id, teacher_id, teacher_weight,
                                   temperature, amplitude, kl_divergence) -> None:
        self._execute("""
            INSERT INTO teacher_superpositions
            (student_id, teacher_id, teacher_weight, temperature,
             superposition_amplitude, kl_divergence, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (student_id, teacher_id, teacher_weight, temperature,
              amplitude, kl_divergence, datetime.now().isoformat()))

    def get_teacher_superpositions(self, student_id: str, limit: int = 100) -> List[Dict]:
        return self._fetchall("""
            SELECT * FROM teacher_superpositions
            WHERE student_id = ? ORDER BY timestamp DESC LIMIT ?
        """, (student_id, limit))

    # --- 2. Causal RL -----------------------------------------------------
    def save_causal_edge(self, source: str, target: str, weight: float, confidence: float) -> None:
        edge_id = f"{source}->{target}"
        self._execute("""
            INSERT OR REPLACE INTO causal_graph
            (edge_id, source, target, weight, confidence, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (edge_id, source, target, weight, confidence, datetime.now().isoformat()))

    def get_causal_graph(self) -> List[Dict]:
        return self._fetchall("SELECT source, target, weight, confidence FROM causal_graph")

    def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples, method="") -> None:
        self._execute("""
            INSERT OR REPLACE INTO causal_experiments
            (exp_id, treatment, outcome, ate, samples, method, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (exp_id, treatment, outcome, ate, samples, method, datetime.now().isoformat()))

    def get_causal_experiments(self, treatment: Optional[str] = None,
                               outcome: Optional[str] = None,
                               limit: int = 100) -> List[Dict]:
        if treatment and outcome:
            return self._fetchall("""
                SELECT * FROM causal_experiments
                WHERE treatment = ? AND outcome = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (treatment, outcome, limit))
        return self._fetchall(
            "SELECT * FROM causal_experiments ORDER BY timestamp DESC LIMIT ?", (limit,))

    def save_causal_intervention(self, intervention_id, node, do_value,
                                 observed_outcome, counterfactual) -> None:
        self._execute("""
            INSERT OR REPLACE INTO causal_interventions
            (intervention_id, node, do_value, observed_outcome,
             counterfactual_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (intervention_id, node, str(do_value), str(observed_outcome),
              json.dumps(counterfactual, default=str), datetime.now().isoformat()))

    # --- 3. Federated Green Learning --------------------------------------
    def save_federated_weights(self, instance_id, model_id, weights_bytes,
                                weight_norm=0.0, round_id=0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO federated_weights
            (instance_id, model_id, weights, weight_norm, round_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (instance_id, model_id, weights_bytes, weight_norm,
              round_id, datetime.now().isoformat()))

    def get_federated_weights(self, model_id: str) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM federated_weights WHERE model_id = ?", (model_id,))

    def register_federated_client(self, instance_id, capabilities=None,
                                  region="global", carbon_intensity=0.0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO federated_clients
            (instance_id, last_seen, reputation, capabilities,
             region, carbon_intensity, weights_shared)
            VALUES (?, ?, COALESCE((SELECT reputation FROM federated_clients
                                    WHERE instance_id = ?), 0.5),
                    ?, ?, ?,
                    COALESCE((SELECT weights_shared FROM federated_clients
                              WHERE instance_id = ?), 0))
        """, (instance_id, datetime.now().isoformat(), instance_id,
              json.dumps(capabilities or {}), region, carbon_intensity, instance_id))

    def list_federated_clients(self) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM federated_clients ORDER BY last_seen DESC")
        for r in rows:
            r["capabilities"] = json.loads(r["capabilities"]) if r["capabilities"] else {}
        return rows

    def save_federated_aggregation(self, aggregation_id, round_id, instance_ids,
                                   weights_snapshot, method,
                                   global_accuracy=0.0, carbon_footprint=0.0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO federated_aggregation_log
            (aggregation_id, round_id, instance_ids, weights_snapshot,
             aggregation_method, global_accuracy, carbon_footprint, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (aggregation_id, round_id, json.dumps(instance_ids),
              json.dumps(weights_snapshot, default=str), method,
              global_accuracy, carbon_footprint, datetime.now().isoformat()))

    # --- 4. Multi-Agent Coordination --------------------------------------
    def save_agent(self, agent_id, role, reputation, utilities, capabilities=None) -> None:
        self._execute("""
            INSERT OR REPLACE INTO agent_registry
            (agent_id, role, reputation, utilities, capabilities,
             created_at, last_updated)
            VALUES (?, ?, ?, ?,
                    COALESCE((SELECT capabilities FROM agent_registry
                              WHERE agent_id = ?), '{}'),
                    COALESCE((SELECT created_at FROM agent_registry
                              WHERE agent_id = ?), ?),
                    ?)
        """, (agent_id, role, reputation, json.dumps(utilities),
              agent_id, agent_id, datetime.now().isoformat(),
              datetime.now().isoformat()))

    def get_agent(self, agent_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM agent_registry WHERE agent_id = ?", (agent_id,))
        if row:
            row["utilities"] = json.loads(row["utilities"]) if row["utilities"] else {}
            row["capabilities"] = json.loads(row["capabilities"]) if row["capabilities"] else {}
        return row

    def list_agents(self) -> List[Dict]:
        rows = self._fetchall("SELECT * FROM agent_registry ORDER BY reputation DESC")
        for r in rows:
            r["utilities"] = json.loads(r["utilities"]) if r["utilities"] else {}
            r["capabilities"] = json.loads(r["capabilities"]) if r["capabilities"] else {}
        return rows

    def save_agent_bid(self, bid_id, task_id, agent_id, bid_score,
                       preferred_role="", awarded=False) -> None:
        self._execute("""
            INSERT OR REPLACE INTO agent_bids
            (bid_id, task_id, agent_id, bid_score, preferred_role,
             awarded, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (bid_id, task_id, agent_id, bid_score, preferred_role,
              int(awarded), datetime.now().isoformat()))

    def save_agent_message(self, message_id, topic, sender, recipient, payload) -> None:
        self._execute("""
            INSERT OR REPLACE INTO agent_messages
            (message_id, topic, sender, recipient, payload, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (message_id, topic, sender, recipient,
              json.dumps(payload, default=str), datetime.now().isoformat()))

    def get_agent_messages(self, topic: Optional[str] = None,
                           limit: int = 100) -> List[Dict]:
        if topic:
            return self._fetchall("""
                SELECT * FROM agent_messages WHERE topic = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (topic, limit))
        return self._fetchall(
            "SELECT * FROM agent_messages ORDER BY timestamp DESC LIMIT ?", (limit,))

    def save_reputation_change(self, agent_id, reputation, reason) -> None:
        self._execute("""
            INSERT INTO agent_reputation_history
            (agent_id, reputation, reason, timestamp)
            VALUES (?, ?, ?, ?)
        """, (agent_id, reputation, reason, datetime.now().isoformat()))

    # --- 5. Temporal Logic -------------------------------------------------
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window_seconds, active=True) -> None:
        self._execute("""
            INSERT OR REPLACE INTO temporal_rules
            (rule_id, formula, operator, severity, description,
             window_seconds, created_at, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, formula, operator, severity, description,
              window_seconds, datetime.now().isoformat(), int(active)))

    def list_temporal_rules(self, active_only: bool = True) -> List[Dict]:
        if active_only:
            return self._fetchall("SELECT * FROM temporal_rules WHERE active = 1")
        return self._fetchall("SELECT * FROM temporal_rules")

    def save_temporal_trace(self, state, context=None) -> None:
        self._execute("""
            INSERT INTO temporal_trace (state, context, timestamp)
            VALUES (?, ?, ?)
        """, (json.dumps(state, default=str),
              json.dumps(context, default=str) if context else None,
              datetime.now().isoformat()))

    def get_temporal_trace(self, limit: int = 1000) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM temporal_trace ORDER BY step DESC LIMIT ?", (limit,))
        for r in rows:
            r["state"] = json.loads(r["state"]) if r["state"] else {}
        return rows

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None) -> None:
        self._execute("""
            INSERT INTO temporal_violations
            (rule_id, formula, step, state, severity, approved,
             resolved_at, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, formula, step, json.dumps(state, default=str),
              severity, int(approved) if approved is not None else None,
              None, datetime.now().isoformat()))

    def get_temporal_violations(self, rule_id: Optional[str] = None,
                                limit: int = 100) -> List[Dict]:
        if rule_id:
            return self._fetchall("""
                SELECT * FROM temporal_violations WHERE rule_id = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (rule_id, limit))
        return self._fetchall(
            "SELECT * FROM temporal_violations ORDER BY timestamp DESC LIMIT ?", (limit,))

    # --- 6. Explainable AI -------------------------------------------------
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             decision_label, features, attributions,
                             natural_language) -> None:
        self._execute("""
            INSERT OR REPLACE INTO xai_explanations
            (explanation_id, decision_id, method, decision_label,
             features, attributions, natural_language, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (explanation_id, decision_id, method, decision_label,
              json.dumps(features, default=str),
              json.dumps(attributions, default=str),
              natural_language, datetime.now().isoformat()))
        # Also write feature importance rows
        for name, val in (attributions or {}).items():
            self._execute("""
                INSERT INTO xai_feature_importance
                (explanation_id, feature_name, importance, rank)
                VALUES (?, ?, ?, ?)
            """, (explanation_id, str(name), float(val), 0))

    def get_xai_explanation(self, explanation_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM xai_explanations WHERE explanation_id = ?", (explanation_id,))
        if row:
            row["attributions"] = json.loads(row["attributions"]) if row["attributions"] else {}
            row["features"] = json.loads(row["features"]) if row["features"] else {}
        return row

    def get_xai_explanations_for_decision(self, decision_id: str) -> List[Dict]:
        return self._fetchall("""
            SELECT * FROM xai_explanations WHERE decision_id = ?
            ORDER BY timestamp DESC
        """, (decision_id,))

    # --- 7. Adaptive Precision --------------------------------------------
    def save_precision_switch(self, from_p, to_p, reason,
                              energy_saved_wh=0.0, accuracy_delta=0.0) -> None:
        self._execute("""
            INSERT INTO precision_history
            (from_p, to_p, reason, energy_saved_wh, accuracy_delta, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (from_p, to_p, reason, energy_saved_wh, accuracy_delta,
              datetime.now().isoformat()))

    def get_precision_history(self, limit: int = 100) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM precision_history ORDER BY timestamp DESC LIMIT ?", (limit,))

    def save_hardware_profile(self, device_id, device_name, cuda_available,
                              bf16_supported, max_precision,
                              memory_gb=0.0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO hardware_profiles
            (device_id, device_name, cuda_available, bf16_supported,
             max_precision, memory_gb, last_probed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (device_id, device_name, int(cuda_available),
              int(bf16_supported), max_precision, memory_gb,
              datetime.now().isoformat()))

    def get_hardware_profile(self, device_id: str) -> Optional[Dict]:
        return self._fetchone(
            "SELECT * FROM hardware_profiles WHERE device_id = ?", (device_id,))

    # --- 8. Carbon Markets / REC ------------------------------------------
    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global") -> None:
        self._execute("""
            INSERT INTO carbon_credit_prices
            (price_usd, currency, source, region, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (price_usd, currency, source, region, datetime.now().isoformat()))

    def get_latest_credit_price(self) -> Optional[Dict]:
        return self._fetchone(
            "SELECT * FROM carbon_credit_prices ORDER BY timestamp DESC LIMIT 1")

    def save_rec(self, mwh, price_per_mwh, source, certificate_id="",
                 region="global", retired=False) -> None:
        self._execute("""
            INSERT INTO rec_ledger
            (mwh, price_per_mwh, source, certificate_id, region,
             retired, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (mwh, price_per_mwh, source, certificate_id, region,
              int(retired), datetime.now().isoformat()))

    def get_rec_balance(self, retired_only: bool = False) -> float:
        if retired_only:
            row = self._fetchone(
                "SELECT COALESCE(SUM(mwh), 0) AS s FROM rec_ledger WHERE retired = 1")
        else:
            row = self._fetchone("SELECT COALESCE(SUM(mwh), 0) AS s FROM rec_ledger")
        return float(row["s"]) if row else 0.0

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost_usd, credit_price_usd) -> None:
        self._execute("""
            INSERT OR REPLACE INTO net_zero_matches
            (match_id, workload_kwh, intensity, action, carbon_kg,
             offset_cost_usd, credit_price_usd, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (match_id, workload_kwh, intensity, action, carbon_kg,
              offset_cost_usd, credit_price_usd, datetime.now().isoformat()))

    def save_carbon_trade(self, trade_id, action, amount, price,
                          currency="USD", tx_hash="") -> None:
        self._execute("""
            INSERT OR REPLACE INTO carbon_market_trades
            (trade_id, action, amount, price, currency, executed_at, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (trade_id, action, amount, price, currency,
              datetime.now().isoformat(), tx_hash))

    # --- 9. Chaos Testing --------------------------------------------------
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO chaos_experiments
            (experiment_id, name, fault_type, blast_radius,
             steady_before, steady_after, status, duration_ms, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (experiment_id, name, fault_type, blast_radius,
              int(steady_before), int(steady_after), status, duration_ms,
              datetime.now().isoformat()))

    def get_chaos_experiments(self, limit: int = 100) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM chaos_experiments ORDER BY timestamp DESC LIMIT ?", (limit,))

    def save_chaos_steady_state(self, check_id, experiment_id, kpi_name,
                                kpi_value, ok) -> None:
        self._execute("""
            INSERT OR REPLACE INTO chaos_steady_states
            (check_id, experiment_id, kpi_name, kpi_value, ok, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (check_id, experiment_id, kpi_name, kpi_value, int(ok),
              datetime.now().isoformat()))

    def save_chaos_rollback(self, rollback_id, experiment_id, trigger_reason) -> None:
        self._execute("""
            INSERT OR REPLACE INTO chaos_rollbacks
            (rollback_id, experiment_id, trigger_reason, rolled_back_at)
            VALUES (?, ?, ?, ?)
        """, (rollback_id, experiment_id, trigger_reason, datetime.now().isoformat()))

    # --- 10. HITL Active Learning -----------------------------------------
    def enqueue_hitl_request(self, request_id, rule_id, state, severity="critical") -> None:
        self._execute("""
            INSERT OR REPLACE INTO hitl_approval_queue
            (request_id, rule_id, state, severity, status, created_at, resolved_at)
            VALUES (?, ?, ?, ?, 'pending', ?, NULL)
        """, (request_id, rule_id, json.dumps(state, default=str),
              severity, datetime.now().isoformat()))

    def list_pending_hitl_requests(self) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM hitl_approval_queue WHERE status = 'pending' "
            "ORDER BY created_at ASC")
        for r in rows:
            r["state"] = json.loads(r["state"]) if r["state"] else {}
        return rows

    def resolve_hitl_request(self, request_id: str, status: str = "approved") -> None:
        self._execute("""
            UPDATE hitl_approval_queue
            SET status = ?, resolved_at = ?
            WHERE request_id = ?
        """, (status, datetime.now().isoformat(), request_id))

    def save_hitl_decision(self, decision_id, request_id, user_id,
                           approved, rationale="") -> None:
        self._execute("""
            INSERT OR REPLACE INTO hitl_decisions
            (decision_id, request_id, user_id, approved, rationale, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (decision_id, request_id, user_id, int(approved), rationale,
              datetime.now().isoformat()))

    def save_active_learning_sample(self, sample_id, model_id, strategy,
                                    uncertainty, selected_for_review,
                                    user_label=None) -> None:
        self._execute("""
            INSERT OR REPLACE INTO active_learning_samples
            (sample_id, model_id, strategy, uncertainty,
             selected_for_review, user_label, reviewed_at, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (sample_id, model_id, strategy, uncertainty,
              int(selected_for_review), user_label,
              datetime.now().isoformat() if user_label else None,
              datetime.now().isoformat()))

    # =========================================================================
    # Cleanup / retention policies
    # =========================================================================
    def clean_power_readings(self, days: int) -> None:
        self._execute("DELETE FROM power_readings WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_elasticity_records(self, days: int) -> None:
        self._execute("DELETE FROM elasticity_metrics WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_substitution_results(self, days: int) -> None:
        self._execute("DELETE FROM substitution_results WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_federated_rounds(self, days: int) -> None:
        self._execute("DELETE FROM federated_rounds WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_circularity_records(self, days: int) -> None:
        self._execute("DELETE FROM circularity_records WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_emission_records(self, days: int) -> None:
        self._execute("DELETE FROM emission_records WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_optimisation_history(self, days: int) -> None:
        self._execute("DELETE FROM optimisation_history WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_thermal_records(self, days: int) -> None:
        self._execute("DELETE FROM thermal_optimizations WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_ga_populations(self, days: int) -> None:
        self._execute("DELETE FROM ga_populations WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_ga_fitness_history(self, days: int) -> None:
        self._execute("DELETE FROM ga_fitness_history WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_moe_training_samples(self, days: int) -> None:
        self._execute("DELETE FROM moe_gating_training WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_moe_routing(self, days: int) -> None:
        self._execute("DELETE FROM moe_routing_history WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_pareto_front(self, days: int) -> None:
        self._execute("DELETE FROM pareto_front WHERE timestamp < ? AND is_current = 0",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_user_preferences(self, days: int) -> None:
        self._execute("DELETE FROM user_preferences WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_scenarios(self, days: int) -> None:
        self._execute("DELETE FROM scenarios WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_decision_catalogue(self, days: int) -> None:
        self._execute("DELETE FROM decision_catalogue WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_limit_graph(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM limit_graph_nodes WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM limit_graph_edges WHERE timestamp < ?", (cutoff,))

    def clean_modp(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        for t in ("modp_states", "modp_transitions", "modp_policies"):
            self._execute(f"DELETE FROM {t} WHERE timestamp < ?", (cutoff,))

    def clean_rlhf_preference_pairs(self, days: int) -> None:
        self._execute("DELETE FROM rlhf_preference_pairs WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_distillation(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM distillation_episodes WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM student_policy_updates WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM teacher_superpositions WHERE timestamp < ?", (cutoff,))

    def clean_bio_runs(self, days: int) -> None:
        self._execute("DELETE FROM bio_inspired_runs WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    # v5 cleanups
    def clean_causal_graph(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM causal_graph WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM causal_experiments WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM causal_interventions WHERE timestamp < ?", (cutoff,))

    def clean_agent_messages(self, days: int) -> None:
        self._execute("DELETE FROM agent_messages WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_agent_bids(self, days: int) -> None:
        self._execute("DELETE FROM agent_bids WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_temporal_trace(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM temporal_trace WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM temporal_violations WHERE timestamp < ?", (cutoff,))

    def clean_xai_explanations(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM xai_explanations WHERE timestamp < ?", (cutoff,))

    def clean_precision_history(self, days: int) -> None:
        self._execute("DELETE FROM precision_history WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    def clean_carbon_market(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM carbon_credit_prices WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM net_zero_matches WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM carbon_market_trades WHERE executed_at < ?", (cutoff,))

    def clean_chaos_experiments(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM chaos_experiments WHERE timestamp < ?", (cutoff,))
        self._execute("DELETE FROM chaos_steady_states WHERE timestamp < ?", (cutoff,))

    def clean_hitl_approval_queue(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("""
            DELETE FROM hitl_approval_queue
            WHERE status != 'pending' AND created_at < ?
        """, (cutoff,))

    def clean_active_learning_samples(self, days: int) -> None:
        self._execute("DELETE FROM active_learning_samples WHERE timestamp < ?",
                      ((datetime.now() - timedelta(days=days)).isoformat(),))

    # =========================================================================
    # Retention policy dispatcher
    # =========================================================================
    def apply_retention_policy(self, policies: Dict[str, int]) -> None:
        for table, days in policies.items():
            if days <= 0:
                continue
            method = getattr(self, f"clean_{table}", None)
            if method:
                method(days)
            else:
                print(f"WARNING: No clean method for table '{table}'")

    # =========================================================================
    # Storage statistics
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
            "limit_graph_nodes", "limit_graph_edges", "limit_graph_metadata",
            "modp_states", "modp_transitions", "modp_policies",
            "rlhf_preference_pairs", "teacher_policies",
            "distillation_episodes", "student_policy_updates",
            "bio_inspired_runs", "moe_expert_models", "moe_routing_history",
            # v5
            "teacher_superpositions",
            "causal_graph", "causal_experiments", "causal_interventions",
            "federated_weights", "federated_clients", "federated_aggregation_log",
            "agent_registry", "agent_bids", "agent_messages",
            "agent_reputation_history",
            "temporal_rules", "temporal_trace", "temporal_violations",
            "xai_explanations", "xai_feature_importance",
            "precision_history", "hardware_profiles",
            "carbon_credit_prices", "rec_ledger", "net_zero_matches",
            "carbon_market_trades",
            "chaos_experiments", "chaos_steady_states", "chaos_rollbacks",
            "hitl_approval_queue", "hitl_decisions",
            "active_learning_samples",
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
    # Async wrappers — v5 additions
    # =========================================================================
    async def save_teacher_superposition_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_teacher_superposition, *args, **kwargs)

    async def get_teacher_superpositions_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_teacher_superpositions, *args, **kwargs)

    async def save_causal_edge_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_causal_edge, *args, **kwargs)

    async def get_causal_graph_async(self):
        return await asyncio.to_thread(self.get_causal_graph)

    async def save_causal_experiment_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_causal_experiment, *args, **kwargs)

    async def get_causal_experiments_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_causal_experiments, *args, **kwargs)

    async def save_causal_intervention_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_causal_intervention, *args, **kwargs)

    async def save_federated_weights_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_federated_weights, *args, **kwargs)

    async def get_federated_weights_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_federated_weights, *args, **kwargs)

    async def register_federated_client_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.register_federated_client, *args, **kwargs)

    async def list_federated_clients_async(self):
        return await asyncio.to_thread(self.list_federated_clients)

    async def save_federated_aggregation_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_federated_aggregation, *args, **kwargs)

    async def save_agent_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_agent, *args, **kwargs)

    async def get_agent_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_agent, *args, **kwargs)

    async def list_agents_async(self):
        return await asyncio.to_thread(self.list_agents)

    async def save_agent_bid_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_agent_bid, *args, **kwargs)

    async def save_agent_message_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_agent_message, *args, **kwargs)

    async def get_agent_messages_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_agent_messages, *args, **kwargs)

    async def save_reputation_change_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_reputation_change, *args, **kwargs)

    async def save_temporal_rule_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_temporal_rule, *args, **kwargs)

    async def list_temporal_rules_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.list_temporal_rules, *args, **kwargs)

    async def save_temporal_trace_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_temporal_trace, *args, **kwargs)

    async def get_temporal_trace_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_temporal_trace, *args, **kwargs)

    async def save_temporal_violation_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_temporal_violation, *args, **kwargs)

    async def get_temporal_violations_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_temporal_violations, *args, **kwargs)

    async def save_xai_explanation_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_xai_explanation, *args, **kwargs)

    async def get_xai_explanation_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_xai_explanation, *args, **kwargs)

    async def get_xai_explanations_for_decision_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_xai_explanations_for_decision, *args, **kwargs)

    async def save_precision_switch_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_precision_switch, *args, **kwargs)

    async def get_precision_history_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_precision_history, *args, **kwargs)

    async def save_hardware_profile_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_hardware_profile, *args, **kwargs)

    async def get_hardware_profile_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_hardware_profile, *args, **kwargs)

    async def save_credit_price_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_credit_price, *args, **kwargs)

    async def get_latest_credit_price_async(self):
        return await asyncio.to_thread(self.get_latest_credit_price)

    async def save_rec_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_rec, *args, **kwargs)

    async def get_rec_balance_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_rec_balance, *args, **kwargs)

    async def save_net_zero_match_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_net_zero_match, *args, **kwargs)

    async def save_carbon_trade_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_carbon_trade, *args, **kwargs)

    async def save_chaos_experiment_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_chaos_experiment, *args, **kwargs)

    async def get_chaos_experiments_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.get_chaos_experiments, *args, **kwargs)

    async def save_chaos_steady_state_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_chaos_steady_state, *args, **kwargs)

    async def save_chaos_rollback_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_chaos_rollback, *args, **kwargs)

    async def enqueue_hitl_request_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.enqueue_hitl_request, *args, **kwargs)

    async def list_pending_hitl_requests_async(self):
        return await asyncio.to_thread(self.list_pending_hitl_requests)

    async def resolve_hitl_request_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.resolve_hitl_request, *args, **kwargs)

    async def save_hitl_decision_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_hitl_decision, *args, **kwargs)

    async def save_active_learning_sample_async(self, *args, **kwargs):
        return await asyncio.to_thread(self.save_active_learning_sample, *args, **kwargs)

    # =========================================================================
    # Close
    # =========================================================================
    def close(self):
        if hasattr(self, "_local") and hasattr(self._local, "conn"):
            self._local.conn.close()
            del self._local.conn

    async def close_async(self):
        if AIOSQLITE_AVAILABLE and self._async_conn:
            await self._async_conn.close()
            self._async_conn = None


# =============================================================================
# Self-test / demo
# =============================================================================
async def _demo():
    s = Storage(db_path="/tmp/storage_v5_demo.db")
    print("Schema version:", Storage.SCHEMA_VERSION)
    stats = s.get_statistics()
    print("Registered tables:", len(stats["table_sizes"]))

    # Exercise each v5 module briefly
    s.save_causal_edge("carbon_intensity", "quality", 0.7, 0.9)
    s.save_causal_experiment("exp1", "carbon_intensity", "quality", 0.7, 100)
    print("Causal edges:", len(s.get_causal_graph()))

    s.save_agent("agent_00", "orchestrator", 0.7, {"task_selection": 0.8})
    s.save_agent_message("m1", "task_bid", "agent_00", "*", {"task": "t1", "score": 0.9})
    print("Agents:", len(s.list_agents()))

    s.save_temporal_violation("never_extreme_carbon", "G (carbon <= 0.7)",
                              step=42, state={"carbon": 0.9})
    print("Temporal violations:", len(s.get_temporal_violations()))

    s.save_xai_explanation("x1", "d1", "kernel_shap", "strategy=carbon",
                            {"pue": 1.5, "carbon": 0.4},
                            {"pue": 0.3, "carbon": -0.5},
                            "Decision 'strategy=carbon' driven by carbon (-0.5)")
    print("XAI explanations:", len(s.get_xai_explanations_for_decision("d1")))

    s.save_precision_switch("fp32", "bf16", "headroom", 0.45, 0.005)
    print("Precision switches:", len(s.get_precision_history()))

    s.save_credit_price(28.5)
    s.save_rec(10.0, 5.0, "wind")
    print("REC balance (MWh):", s.get_rec_balance())

    s.save_chaos_experiment("ch1", "auto_chaos", "latency", 0.1, 1, 1, "completed")
    print("Chaos experiments:", len(s.get_chaos_experiments()))

    s.enqueue_hitl_request("r1", "never_extreme_carbon", {"carbon": 0.9})
    print("Pending HITL:", len(s.list_pending_hitl_requests()))

    s.save_federated_weights("inst_A", "policy", b"\x00\x01\x02",
                             weight_norm=1.0, round_id=1)
    print("Federated weight rows:", len(s.get_federated_weights("policy")))

    s.save_teacher_superposition("student_1", "teacher_a", 0.5, 2.0, 0.707, 0.12)
    print("Superpositions:", len(s.get_teacher_superpositions("student_1")))

    print("\nFinal statistics:")
    stats = s.get_statistics()
    for k, v in stats["table_sizes"].items():
        if v > 0:
            print(f"  {k}: {v}")
    s.close()


if __name__ == "__main__":
    asyncio.run(_demo())
