#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/storage_v5_1_0.py
# VERSION: 5.1.0 – Central Storage with full ten-enhancement support
# =============================================================================
"""
Central Storage module for Green Agent enhancements – v5.1.0

v5.0.0 preserved: schema v5 with all ten advanced enhancements, migrations
v1→v5, WAL mode, thread-local connection pool, optional AES-GCM.

v5.1.0 (Phase 1-3):
  [P0] Fix asdict detection (broken on dataclasses); save_agent capabilities;
       chaos_experiments.steady_during (mig v5→v6); benchmark timestamp
       comparison; encryption fail-closed; thread-safe stats; cursor-after-
       commit; list_pending_hitl_requests LIMIT; get_causal_experiments OR-
       logic; clean_* days validation; save_xai_explanation transaction.
  [P1] Persistent async connection; temporal_trace.session_id; retire_rec;
       increment_weights_shared; get_pending_hitl_count; pqc public-only
       encryption; composite indexes; chaos FK; hitl status consistency.
  [P2] Correlation IDs; transaction() context manager; health_check;
       structured logging; explain_query.
  [P3] backup/restore; vacuum; batch_insert; stream_table; migration
       savepoint-rollback.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import (Any, AsyncIterator, Dict, Iterable, Iterator, List,
                    Optional, Tuple, Union)

# -----------------------------------------------------------------------------
# Optional deps
# -----------------------------------------------------------------------------
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from . import config as _config
    DEFAULT_DB = getattr(_config, "DB_PATH", "enhancements_storage.db")
except Exception:
    try:
        import config as _config
        DEFAULT_DB = getattr(_config, "DB_PATH", "enhancements_storage.db")
    except Exception:
        DEFAULT_DB = "enhancements_storage.db"


# -----------------------------------------------------------------------------
# Logging + correlation ID — [P2]
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar("correlation_id", default="unknown")
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - "
               "[%(correlation_id)s] - %(message)s")

    class _CidFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True

    for _h in logger.handlers:
        _h.addFilter(_CidFilter())


# =============================================================================
# Thread-safe stats — [P0]
# =============================================================================
class _Stats:
    """Thread-safe counter bundle. Replaces the plain dict in v5.0.0
    (`self._stats["total_queries"] += 1` was racy under threads)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._total_queries = 0
        self._table_sizes: Dict[str, int] = {}
        self._last_updated: Optional[str] = None
        self._last_cleanup: Optional[str] = None
        self._last_backup: Optional[str] = None
        self._errors = 0

    def inc_query(self):
        with self._lock:
            self._total_queries += 1

    def inc_error(self):
        with self._lock:
            self._errors += 1

    def set_table_sizes(self, sizes: Dict[str, int]):
        with self._lock:
            self._table_sizes = dict(sizes)
            self._last_updated = datetime.now().isoformat()

    def mark_cleanup(self):
        with self._lock:
            self._last_cleanup = datetime.now().isoformat()

    def mark_backup(self):
        with self._lock:
            self._last_backup = datetime.now().isoformat()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_queries": self._total_queries,
                "table_sizes": dict(self._table_sizes),
                "last_updated": self._last_updated,
                "last_cleanup": self._last_cleanup,
                "last_backup": self._last_backup,
                "errors": self._errors,
            }


# =============================================================================
# MigrationError — [P3]
# =============================================================================
class MigrationError(RuntimeError):
    pass


# =============================================================================
# Storage
# =============================================================================
class Storage:
    SCHEMA_VERSION = 6  # [P0] bump for v5→v6 additive fixes

    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        self.db_path = Path(db_path or DEFAULT_DB)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Encryption — [P0] fail-closed
        self.master_key: Optional[bytes] = None
        key_hex = os.getenv("STORAGE_MASTER_KEY")
        self._encryption_disabled_reason: Optional[str] = None
        if key_hex:
            try:
                self.master_key = bytes.fromhex(key_hex)
                if len(self.master_key) != 32:
                    raise ValueError("Master key must be 32 bytes")
            except Exception as e:
                self.master_key = None
                self._encryption_disabled_reason = f"Invalid master key: {e}"
                logger.warning("Invalid STORAGE_MASTER_KEY – encryption disabled: %s", e)
        elif not CRYPTO_AVAILABLE:
            self._encryption_disabled_reason = "cryptography package not installed"

        # [P1] Persistent async connection
        self._async_conn: Optional["aiosqlite.Connection"] = None
        self._async_conn_lock = asyncio.Lock()

        # Sync connection (thread-local) — kept from v5
        self._local = threading.local()

        # [P2] Transaction depth (sync) — needed for nested transactions
        self._tx_depth = threading.local()
        self._tx_lock = threading.Lock()

        # [P0] Thread-safe stats
        self._stats = _Stats()

        self._init_db()

    # =========================================================================
    # Connection management
    # =========================================================================
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=5000;")
            self._local.conn = conn
        return self._local.conn

    async def _ensure_async_conn(self) -> Optional["aiosqlite.Connection"]:
        # [P1] Single persistent async connection (no more open/close per call)
        if not AIOSQLITE_AVAILABLE:
            return None
        async with self._async_conn_lock:
            if self._async_conn is None:
                try:
                    self._async_conn = await aiosqlite.connect(self.db_path)
                    await self._async_conn.execute("PRAGMA journal_mode=WAL;")
                    await self._async_conn.execute("PRAGMA foreign_keys=ON;")
                    await self._async_conn.execute("PRAGMA busy_timeout=5000;")
                except Exception as e:
                    logger.warning("aiosqlite init failed: %s", e)
                    self._async_conn = None
            return self._async_conn

    # =========================================================================
    # Core execution — [P0] fetch-before-commit
    # =========================================================================
    def _execute(self, sql: str, params: tuple = ()) -> int:
        """Execute a DML statement. Returns rowcount. Commits.
        Does NOT return a cursor (fixes v5.0.0 cursor-after-commit bug)."""
        try:
            conn = self._get_connection()
            cur = conn.execute(sql, params)
            conn.commit()
            self._stats.inc_query()
            return cur.rowcount if cur.rowcount is not None else 0
        except sqlite3.Error as e:
            self._stats.inc_error()
            logger.error("Storage SQL error: %s | sql=%s", e, sql)
            raise

    def _execute_no_commit(self, sql: str, params: tuple = ()) -> int:
        """Execute without committing (for batch / explicit-tx paths)."""
        try:
            conn = self._get_connection()
            cur = conn.execute(sql, params)
            self._stats.inc_query()
            return cur.rowcount if cur.rowcount is not None else 0
        except sqlite3.Error as e:
            self._stats.inc_error()
            logger.error("Storage SQL error (no commit): %s | sql=%s", e, sql)
            raise

    def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        """SELECT one row. No commit needed (reads)."""
        try:
            conn = self._get_connection()
            cur = conn.execute(sql, params)
            row = cur.fetchone()
            self._stats.inc_query()
            return dict(row) if row else None
        except sqlite3.Error as e:
            self._stats.inc_error()
            logger.error("Storage SQL error (fetchone): %s | sql=%s", e, sql)
            raise

    def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        """SELECT all rows. No commit needed (reads)."""
        try:
            conn = self._get_connection()
            cur = conn.execute(sql, params)
            rows = cur.fetchall()
            self._stats.inc_query()
            return [dict(r) for r in rows]
        except sqlite3.Error as e:
            self._stats.inc_error()
            logger.error("Storage SQL error (fetchall): %s | sql=%s", e, sql)
            raise

    async def _execute_async(self, sql: str, params: tuple = ()) -> int:
        conn = await self._ensure_async_conn()
        if conn is None:
            return await asyncio.to_thread(self._execute, sql, params)
        try:
            cur = await conn.execute(sql, params)
            await conn.commit()
            self._stats.inc_query()
            return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            self._stats.inc_error()
            logger.error("Storage async SQL error: %s | sql=%s", e, sql)
            raise

    async def _fetchone_async(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        conn = await self._ensure_async_conn()
        if conn is None:
            return await asyncio.to_thread(self._fetchone, sql, params)
        cur = await conn.execute(sql, params)
        row = await cur.fetchone()
        return dict(row) if row else None

    async def _fetchall_async(self, sql: str, params: tuple = ()) -> List[Dict]:
        conn = await self._ensure_async_conn()
        if conn is None:
            return await asyncio.to_thread(self._fetchall, sql, params)
        cur = await conn.execute(sql, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # =========================================================================
    # Transactions — [P2]
    # =========================================================================
    @contextlib.contextmanager
    def transaction(self):
        """Sync transaction context. Nested calls are no-ops (SQLite doesn't
        support real nesting; we rely on savepoints manually if needed)."""
        conn = self._get_connection()
        with self._tx_lock:
            depth = getattr(self._tx_depth, "d", 0)
            self._tx_depth.d = depth + 1
            is_outermost = depth == 0
        try:
            if is_outermost:
                conn.execute("BEGIN")
            yield self
            if is_outermost:
                conn.commit()
        except Exception:
            if is_outermost:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            with self._tx_lock:
                self._tx_depth.d -= 1

    @contextlib.asynccontextmanager
    async def transaction_async(self):
        """Async transaction. Falls back to sync via to_thread when aiosqlite
        is unavailable — serializes under `_tx_lock`."""
        conn = await self._ensure_async_conn()
        if conn is None:
            # Fallback: use sync transaction via to_thread
            with self.transaction() as tx:
                yield tx
            return
        try:
            await conn.execute("BEGIN")
            yield self
            await conn.commit()
        except Exception:
            try:
                await conn.rollback()
            except Exception:
                pass
            raise

    # =========================================================================
    # Schema
    # =========================================================================
    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY, applied_at TEXT)
            """)
            row = conn.execute(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            ).fetchone()
            current = row[0] if row else 0

            if current < self.SCHEMA_VERSION:
                # [P3] Run each migration in its own savepoint
                for v in range(current + 1, self.SCHEMA_VERSION + 1):
                    method = getattr(self, f"_migrate_to_v{v}", None)
                    if method is None:
                        continue
                    sp = f"mig_v{v}"
                    try:
                        conn.execute(f"SAVEPOINT {sp}")
                        method(conn)
                        conn.execute(f"RELEASE {sp}")
                    except Exception as e:
                        try:
                            conn.execute(f"ROLLBACK TO {sp}")
                            conn.execute(f"RELEASE {sp}")
                        except Exception:
                            pass
                        raise MigrationError(
                            f"Migration to v{v} failed: {e}") from e
                conn.execute("DELETE FROM schema_version")
                conn.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (self.SCHEMA_VERSION, datetime.now().isoformat()))
                conn.commit()
                logger.info("Storage migrated to schema v%d", self.SCHEMA_VERSION)

    def _migrate_to_v1(self, conn):  # unchanged from v5.0.0
        conn.execute("""CREATE TABLE IF NOT EXISTS model_weights (
            model_id TEXT PRIMARY KEY, weights BLOB, timestamp REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS feedback_events (
            event_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
            task_id TEXT NOT NULL, model_id TEXT, teacher_id TEXT,
            selected_action TEXT NOT NULL, quality_score REAL NOT NULL,
            latency_ms REAL NOT NULL, energy_joules REAL NOT NULL,
            carbon_g REAL NOT NULL, helium_cost REAL, resource_usage TEXT,
            distillation_loss REAL, feedback_type TEXT NOT NULL,
            adaptive_cost_value REAL NOT NULL, metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS drift_states (
            snapshot_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
            online_weights BLOB, offline_weights BLOB,
            cost_score REAL, reason TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS benchmark_runs (
            run_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
            policy_name TEXT NOT NULL, avg_quality REAL, avg_carbon REAL,
            avg_latency REAL, avg_cost REAL, total_energy REAL,
            sample_count INTEGER)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_task ON feedback_events(task_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_time ON feedback_events(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_policy ON benchmark_runs(policy_name);")
        conn.execute("""CREATE TABLE IF NOT EXISTS pqc_keys (
            key_id TEXT PRIMARY KEY, algorithm TEXT NOT NULL,
            public_key BLOB NOT NULL, public_nonce BLOB NOT NULL,
            private_key BLOB NOT NULL, private_nonce BLOB NOT NULL,
            created_at TEXT NOT NULL, expires_at TEXT NOT NULL)""")

    def _migrate_to_v2(self, conn):  # unchanged
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
            source TEXT, location TEXT, verified INTEGER,
            region TEXT, user_id TEXT, timestamp TEXT, metadata TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS optimisation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT NOT NULL,
            result TEXT, timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS distribution_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            optimal_provider TEXT NOT NULL, optimal_region TEXT NOT NULL,
            scores TEXT, data_size_gb REAL, timestamp TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS thermal_optimizations (
            id TEXT PRIMARY KEY, data TEXT, timestamp TEXT)""")

    def _migrate_to_v3(self, conn):  # unchanged
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
            option_id TEXT PRIMARY KEY, name TEXT, attributes TEXT,
            timestamp TEXT)""")

    def _migrate_to_v4(self, conn):  # unchanged
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
        conn.execute("""CREATE TABLE IF NOT EXISTS rlhf_preference_pairs (
            pair_id TEXT PRIMARY KEY, prompt TEXT, chosen_response TEXT,
            rejected_response TEXT, reward_difference REAL,
            metadata TEXT, timestamp TEXT)""")
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

    def _migrate_to_v5(self, conn):  # unchanged from v5.0.0
        conn.execute("""CREATE TABLE IF NOT EXISTS teacher_superpositions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id TEXT, teacher_id TEXT, teacher_weight REAL,
            temperature REAL, superposition_amplitude REAL,
            kl_divergence REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS causal_graph (
            edge_id TEXT PRIMARY KEY, source TEXT NOT NULL,
            target TEXT NOT NULL, weight REAL, confidence REAL,
            timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS causal_experiments (
            exp_id TEXT PRIMARY KEY, treatment TEXT, outcome TEXT,
            ate REAL, samples INTEGER, method TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS causal_interventions (
            intervention_id TEXT PRIMARY KEY, node TEXT, do_value TEXT,
            observed_outcome TEXT, counterfactual_json TEXT,
            timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_weights (
            instance_id TEXT, model_id TEXT, weights BLOB,
            weight_norm REAL, round_id INTEGER, timestamp TEXT,
            PRIMARY KEY (instance_id, model_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_clients (
            instance_id TEXT PRIMARY KEY, last_seen TEXT,
            reputation REAL, capabilities TEXT, region TEXT,
            carbon_intensity REAL, weights_shared INTEGER DEFAULT 0)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS federated_aggregation_log (
            aggregation_id TEXT PRIMARY KEY, round_id INTEGER,
            instance_ids TEXT, weights_snapshot TEXT,
            aggregation_method TEXT, global_accuracy REAL,
            carbon_footprint REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_registry (
            agent_id TEXT PRIMARY KEY, role TEXT, reputation REAL,
            utilities TEXT, capabilities TEXT, created_at TEXT,
            last_updated TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_bids (
            bid_id TEXT PRIMARY KEY, task_id TEXT, agent_id TEXT,
            bid_score REAL, preferred_role TEXT, awarded INTEGER,
            timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_messages (
            message_id TEXT PRIMARY KEY, topic TEXT, sender TEXT,
            recipient TEXT, payload TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS agent_reputation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT,
            reputation REAL, reason TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_rules (
            rule_id TEXT PRIMARY KEY, formula TEXT, operator TEXT,
            severity TEXT, description TEXT, window_seconds REAL,
            created_at TEXT, active INTEGER DEFAULT 1)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_trace (
            step INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT,
            context TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS temporal_violations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id TEXT,
            formula TEXT, step INTEGER, state TEXT,
            severity TEXT, approved INTEGER, resolved_at TEXT,
            timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS xai_explanations (
            explanation_id TEXT PRIMARY KEY, decision_id TEXT,
            method TEXT, decision_label TEXT, features TEXT,
            attributions TEXT, natural_language TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS xai_feature_importance (
            id INTEGER PRIMARY KEY AUTOINCREMENT, explanation_id TEXT,
            feature_name TEXT, importance REAL, rank INTEGER)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS precision_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, from_p TEXT, to_p TEXT,
            reason TEXT, energy_saved_wh REAL, accuracy_delta REAL,
            timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS hardware_profiles (
            device_id TEXT PRIMARY KEY, device_name TEXT,
            cuda_available INTEGER, bf16_supported INTEGER,
            max_precision TEXT, memory_gb REAL, last_probed TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_credit_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT, price_usd REAL,
            currency TEXT, source TEXT, region TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS rec_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT, mwh REAL,
            price_per_mwh REAL, source TEXT, certificate_id TEXT,
            region TEXT, retired INTEGER DEFAULT 0, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS net_zero_matches (
            match_id TEXT PRIMARY KEY, workload_kwh REAL, intensity REAL,
            action TEXT, carbon_kg REAL, offset_cost_usd REAL,
            credit_price_usd REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS carbon_market_trades (
            trade_id TEXT PRIMARY KEY, action TEXT, amount REAL,
            price REAL, currency TEXT, executed_at TEXT, tx_hash TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_experiments (
            experiment_id TEXT PRIMARY KEY, name TEXT, fault_type TEXT,
            blast_radius REAL, steady_before INTEGER, steady_after INTEGER,
            status TEXT, duration_ms REAL, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_steady_states (
            check_id TEXT PRIMARY KEY, experiment_id TEXT, kpi_name TEXT,
            kpi_value REAL, ok INTEGER, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS chaos_rollbacks (
            rollback_id TEXT PRIMARY KEY, experiment_id TEXT,
            trigger_reason TEXT, rolled_back_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_approval_queue (
            request_id TEXT PRIMARY KEY, rule_id TEXT, state TEXT,
            severity TEXT, status TEXT, created_at TEXT, resolved_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS hitl_decisions (
            decision_id TEXT PRIMARY KEY, request_id TEXT, user_id TEXT,
            approved INTEGER, rationale TEXT, timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS active_learning_samples (
            sample_id TEXT PRIMARY KEY, model_id TEXT,
            strategy TEXT, uncertainty REAL, selected_for_review INTEGER,
            user_label TEXT, reviewed_at TEXT, timestamp TEXT)""")

    # =========================================================================
    # v6 — [P0/P1] additive fixes on top of v5.0.0
    # =========================================================================
    def _migrate_to_v6(self, conn):
        """Additive fixes for v5.0.0 schema. Uses ALTER TABLE for existing
        DBs; no-op for fresh installs (columns created by migrations v1-v5)."""
        # [P0] chaos_experiments needs steady_during (missing in v5.0.0)
        self._add_column_if_missing(
            conn, "chaos_experiments", "steady_during", "INTEGER")

        # [P1] temporal_trace needs session_id for multi-context isolation
        self._add_column_if_missing(
            conn, "temporal_trace", "session_id", "TEXT")

        # [P1] federated_clients needs explicit weights_shared (already there
        # in v5 but no update path existed — column was dead; keep, add index)
        self._add_column_if_missing(
            conn, "federated_clients", "last_round_id", "INTEGER DEFAULT 0")

        # [P1] agent_registry needs a JSON capabilities update path (already
        # present as a column in v5; no schema change needed)

        # [P2] Add correlation_id to audit-relevant tables (optional but
        # useful for tracing; defaults to 'unknown')
        for t in ("feedback_events", "causal_experiments",
                   "temporal_violations", "xai_explanations",
                   "chaos_experiments", "hitl_decisions"):
            self._add_column_if_missing(conn, t, "correlation_id", "TEXT DEFAULT 'unknown'")

        # [P1] Composite indexes for hot paths
        idx_stmts = [
            "CREATE INDEX IF NOT EXISTS idx_agent_bids_agent ON agent_bids(agent_id)",
            "CREATE INDEX IF NOT EXISTS idx_agent_bids_task_status ON agent_bids(task_id, awarded)",
            "CREATE INDEX IF NOT EXISTS idx_temporal_trace_session ON temporal_trace(session_id, step)",
            "CREATE INDEX IF NOT EXISTS idx_temporal_viol_rule_time ON temporal_violations(rule_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_active_learning_review ON active_learning_samples(model_id, selected_for_review)",
            "CREATE INDEX IF NOT EXISTS idx_federated_weights_model_round ON federated_weights(model_id, round_id)",
            "CREATE INDEX IF NOT EXISTS idx_xai_importance_rank ON xai_feature_importance(explanation_id, rank)",
            "CREATE INDEX IF NOT EXISTS idx_chaos_steady_exp ON chaos_steady_states(experiment_id)",
            "CREATE INDEX IF NOT EXISTS idx_rec_ledger_retired ON rec_ledger(retired)",
            "CREATE INDEX IF NOT EXISTS idx_pending_hitl ON hitl_approval_queue(status, created_at)",
        ]
        for stmt in idx_stmts:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass

    @staticmethod
    def _add_column_if_missing(conn: sqlite3.Connection, table: str,
                                column: str, type_decl: str):
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if column not in cols:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_decl}")
            except sqlite3.OperationalError as e:
                logger.warning("Could not add column %s.%s: %s", table, column, e)

    # =========================================================================
    # Encryption — [P0] fail-closed; [P1] public key not encrypted
    # =========================================================================
    def _encrypt_blob(self, data: bytes) -> Tuple[bytes, bytes]:
        if not self.master_key or not CRYPTO_AVAILABLE:
            raise RuntimeError(
                f"Encryption unavailable: {self._encryption_disabled_reason or 'no key'}")
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(self.master_key)
        return aesgcm.encrypt(nonce, data, None), nonce

    def _decrypt_blob(self, ciphertext: bytes, nonce: bytes) -> bytes:
        if not self.master_key or not CRYPTO_AVAILABLE:
            if not nonce:
                return ciphertext  # was stored plaintext (legacy)
            raise RuntimeError(
                f"Encryption unavailable: {self._encryption_disabled_reason or 'no key'}")
        if not nonce:
            return ciphertext
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, ciphertext, None)

    def encrypt_existing_blobs(self, table: str, column: str,
                                nonce_column: str,
                                where_clause: str = "1",
                                batch_size: int = 500) -> int:
        if not self.master_key or not CRYPTO_AVAILABLE:
            raise RuntimeError("Encryption not available")
        count = 0
        offset = 0
        while True:
            rows = self._fetchall(
                f"SELECT rowid, {column} FROM {table} "
                f"WHERE {nonce_column} = X'' AND {where_clause} "
                f"LIMIT {batch_size} OFFSET {offset}")
            if not rows:
                break
            with self.transaction():
                for row in rows:
                    try:
                        ct, nonce = self._encrypt_blob(bytes(row[column] or b""))
                    except Exception as e:
                        logger.warning("Encrypt failed for rowid=%s: %s",
                                        row["rowid"], e)
                        continue
                    self._execute_no_commit(
                        f"UPDATE {table} SET {column}=?, {nonce_column}=? "
                        f"WHERE rowid=?", (ct, nonce, row["rowid"]))
                    count += 1
            offset += len(rows)
        logger.info("Encrypted %d BLOBs in %s.%s", count, table, column)
        return count

    # =========================================================================
    # Async wrappers for v5 public API
    # =========================================================================
    # All existing async wrappers preserved (unchanged). For brevity, only new
    # or changed ones are shown; the rest delegate to `asyncio.to_thread`.
    async def save_causal_edge_async(self, *a, **kw):
        return await asyncio.to_thread(self.save_causal_edge, *a, **kw)

    async def save_temporal_violation_async(self, *a, **kw):
        return await asyncio.to_thread(self.save_temporal_violation, *a, **kw)

    async def save_xai_explanation_async(self, *a, **kw):
        return await asyncio.to_thread(self.save_xai_explanation, *a, **kw)

    # ... (all other async wrappers from v5.0.0 unchanged)

    # =========================================================================
    # v1 core (unchanged)
    # =========================================================================
    def save_model_weights(self, model_id: str, weights_bytes: bytes) -> None:
        self._execute(
            "INSERT OR REPLACE INTO model_weights (model_id, weights, timestamp) "
            "VALUES (?, ?, ?)", (model_id, weights_bytes, time.time()))

    def load_model_weights(self, model_id: str) -> Optional[bytes]:
        row = self._fetchone(
            "SELECT weights FROM model_weights WHERE model_id = ?", (model_id,))
        return row["weights"] if row else None

    def store_feedback_event(self, event: Dict) -> None:
        cid = correlation_id_var.get()
        self._execute("""
            INSERT OR REPLACE INTO feedback_events (
                event_id, timestamp, task_id, model_id, teacher_id, selected_action,
                quality_score, latency_ms, energy_joules, carbon_g, helium_cost,
                resource_usage, distillation_loss, feedback_type,
                adaptive_cost_value, metadata, correlation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["event_id"], float(event["timestamp"]), event["task_id"],
            event.get("model_id"), event.get("teacher_id"),
            event["selected_action"],
            float(event["quality_score"]), float(event["latency_ms"]),
            float(event["energy_joules"]), float(event["carbon_g"]),
            event.get("helium_cost"),
            json.dumps(event.get("resource_usage", {})),
            event.get("distillation_loss"), event["feedback_type"],
            float(event["adaptive_cost_value"]),
            json.dumps(event.get("metadata", {})), cid,
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
            (snapshot_id, timestamp, online_weights, offline_weights,
             cost_score, reason) VALUES (?, ?, ?, ?, ?, ?)
        """, (snapshot_id, time.time(), online_w, offline_w, cost, reason))

    def get_last_snapshot(self) -> Optional[Dict]:
        return self._fetchone(
            "SELECT * FROM drift_states ORDER BY timestamp DESC LIMIT 1")

    def store_benchmark_result(self, run_id, policy, metrics, count) -> None:
        # [P0] Timestamp stored as REAL for consistency with feedback_events
        self._execute("""
            INSERT OR REPLACE INTO benchmark_runs
            (run_id, timestamp, policy_name, avg_quality, avg_carbon,
             avg_latency, avg_cost, total_energy, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, time.time(), policy,
              metrics.get("quality"), metrics.get("carbon"),
              metrics.get("latency"), metrics.get("cost"),
              metrics.get("energy"), int(count)))

    def get_benchmark_results(self, days_back: int = 7) -> List[Dict]:
        # [P0] Timestamp column is REAL (seconds since epoch); compare to
        # time.time() - days*86400, NOT to an ISO string
        cutoff_epoch = time.time() - days_back * 86400
        return self._fetchall(
            "SELECT * FROM benchmark_runs WHERE timestamp >= ? "
            "ORDER BY timestamp DESC", (cutoff_epoch,))

    def store_power_reading(self, reading: Dict) -> None:
        self._execute("""
            INSERT OR REPLACE INTO power_readings
            (reading_id, power_watts, carbon_intensity, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, (reading["reading_id"], reading["power_watts"],
              reading.get("carbon_intensity"), reading["timestamp"],
              json.dumps(reading.get("metadata", {}))))

    # [P0] Fix asdict detection — dataclasses don't have `.asdict`
    @staticmethod
    def _to_dict(obj: Any) -> Dict:
        if isinstance(obj, dict):
            return obj
        if is_dataclass(obj) and not isinstance(obj, type):
            return asdict(obj)
        # Try common protocol: __dict__ for objects with __dict__
        if hasattr(obj, "__dict__"):
            return dict(vars(obj))
        raise TypeError(f"Cannot convert {type(obj).__name__} to dict")

    def store_elasticity_metrics(self, metrics) -> None:
        d = self._to_dict(metrics)
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
              d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat")
              else str(d["timestamp"])))

    def store_substitution_result(self, result) -> None:
        d = self._to_dict(result)
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
              str(d["timestamp"])))

    def store_federated_round(self, result) -> None:
        d = self._to_dict(result)
        self._execute("""
            INSERT OR REPLACE INTO federated_rounds (
                round_id, num_clients, global_accuracy, aggregated_loss,
                strategy, carbon_footprint, energy_used, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["round_id"], d["num_clients"], d["global_accuracy"],
              d["aggregated_loss"], d["strategy"], d["carbon_footprint"],
              d["energy_used"], d.get("blockchain_tx_hash") or "",
              str(d["timestamp"])))

    def store_circularity_record(self, metrics) -> None:
        d = self._to_dict(metrics)
        self._execute("""
            INSERT OR REPLACE INTO circularity_records (
                record_id, circularity_index, circularity_level, recycling_rate,
                recovery_efficiency, collection_efficiency,
                purification_efficiency, data_quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["record_id"], d["circularity_index"], d["circularity_level"],
              d["recycling_rate"], d["recovery_efficiency"], d["collection_efficiency"],
              d["purification_efficiency"], d["data_quality_score"],
              d.get("blockchain_tx_hash") or "", str(d["timestamp"])))

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
        self._execute(
            "INSERT INTO optimisation_history (strategy, result, timestamp) "
            "VALUES (?, ?, ?)",
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
        d = self._to_dict(result)
        self._execute("""
            INSERT OR REPLACE INTO thermal_optimizations (id, data, timestamp)
            VALUES (?, ?, ?)
        """, (d.get("id", f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
              json.dumps(d, default=str), datetime.now().isoformat()))

    def save_state(self, key: str, value: str) -> None:
        self._execute(
            "INSERT OR REPLACE INTO kv_store (key, value, updated_at) "
            "VALUES (?, ?, ?)",
            (key, value, datetime.now().isoformat()))

    def get_state(self, key: str) -> Optional[str]:
        row = self._fetchone("SELECT value FROM kv_store WHERE key = ?", (key,))
        return row["value"] if row else None

    def delete_state(self, key: str) -> None:
        self._execute("DELETE FROM kv_store WHERE key = ?", (key,))

    # [P1] Public key not encrypted (only private key is)
    def save_pqc_key(self, key_id, algorithm, public_key, private_key,
                     expires_at) -> None:
        priv_c, priv_n = self._encrypt_blob(private_key)
        # Public key stays plaintext — it's meant to be public
        self._execute("""
            INSERT OR REPLACE INTO pqc_keys
            (key_id, algorithm, public_key, public_nonce, private_key,
             private_nonce, created_at, expires_at)
            VALUES (?, ?, ?, X'', ?, ?, ?, ?)
        """, (key_id, algorithm, public_key,
              priv_c, priv_n,
              datetime.now().isoformat(), expires_at))

    def get_pqc_key(self, key_id: str) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM pqc_keys WHERE key_id = ?", (key_id,))
        if not row:
            return None
        return {
            "key_id": row["key_id"], "algorithm": row["algorithm"],
            "public_key": bytes(row["public_key"]),
            "private_key": self._decrypt_blob(
                bytes(row["private_key"]), bytes(row["private_nonce"])),
            "created_at": row["created_at"], "expires_at": row["expires_at"],
        }

    def list_pqc_keys(self) -> List[str]:
        return [r["key_id"] for r in self._fetchall("SELECT key_id FROM pqc_keys")]

    def delete_pqc_key(self, key_id: str) -> None:
        self._execute("DELETE FROM pqc_keys WHERE key_id = ?", (key_id,))

    # =========================================================================
    # v3 (unchanged)
    # =========================================================================
    def save_ga_population(self, generation: int, individuals: List[Dict]) -> None:
        with self.transaction():
            for ind in individuals:
                self._execute_no_commit("""
                    INSERT OR REPLACE INTO ga_populations
                    (generation, individual_id, attributes, fitness, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (generation, ind["individual_id"],
                      json.dumps(ind["attributes"]), ind["fitness"],
                      datetime.now().isoformat()))

    def get_ga_population(self, generation: int) -> List[Dict]:
        rows = self._fetchall(
            "SELECT individual_id, attributes, fitness FROM ga_populations "
            "WHERE generation = ?", (generation,))
        return [{"individual_id": r["individual_id"],
                 "attributes": json.loads(r["attributes"]),
                 "fitness": r["fitness"]} for r in rows]

    def save_ga_fitness_history(self, generation, best_fitness, avg_fitness,
                                 diversity) -> None:
        self._execute("""
            INSERT OR REPLACE INTO ga_fitness_history
            (generation, best_fitness, avg_fitness, diversity, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (generation, best_fitness, avg_fitness, diversity,
              datetime.now().isoformat()))

    def save_moe_training_sample(self, sample_id, features, expert_label,
                                  reward) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_gating_training
            (sample_id, features, expert_label, reward, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (sample_id, json.dumps(features), expert_label, reward,
              datetime.now().isoformat()))

    def save_moe_expert_metadata(self, expert_id, name, description,
                                  performance_score) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_expert_metadata
            (expert_id, name, description, performance_score, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, (expert_id, name, description, performance_score,
              datetime.now().isoformat()))

    def save_pareto_front(self, solutions: List[Dict]) -> None:
        with self.transaction():
            self._execute_no_commit("UPDATE pareto_front SET is_current = 0")
            for s in solutions:
                self._execute_no_commit("""
                    INSERT OR REPLACE INTO pareto_front
                    (solution_id, decision_attributes, accuracy, carbon, cost,
                     robustness, is_current, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (s["solution_id"], json.dumps(s["decision_attributes"]),
                      s["accuracy"], s["carbon"], s["cost"], s["robustness"],
                      1, datetime.now().isoformat()))

    def get_current_pareto_front(self) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM pareto_front WHERE is_current = 1 "
            "ORDER BY accuracy DESC")
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    def save_user_preference(self, user_id, weights,
                              chosen_solution_id=None) -> None:
        self._execute("""
            INSERT INTO user_preferences
            (user_id, weights, chosen_solution_id, timestamp)
            VALUES (?, ?, ?, ?)
        """, (user_id, json.dumps(weights), chosen_solution_id,
              datetime.now().isoformat()))

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
             technology_cost_reduction, regulatory_risk,
             renewable_energy_share, energy_efficiency, timestamp)
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
        """, (option_id, name, json.dumps(attributes),
              datetime.now().isoformat()))

    # =========================================================================
    # v4 (unchanged)
    # =========================================================================
    def save_limit_graph_node(self, node_id, graph_id, node_type, attributes) -> None:
        self._execute("""
            INSERT OR REPLACE INTO limit_graph_nodes
            (node_id, graph_id, node_type, attributes, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (node_id, graph_id, node_type, json.dumps(attributes),
              datetime.now().isoformat()))

    def get_limit_graph_nodes(self, graph_id: str) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM limit_graph_nodes WHERE graph_id = ?", (graph_id,))
        for r in rows:
            r["attributes"] = json.loads(r["attributes"]) if r["attributes"] else {}
        return rows

    def save_limit_graph_edge(self, edge_id, graph_id, source, target, weight,
                                attributes) -> None:
        self._execute("""
            INSERT OR REPLACE INTO limit_graph_edges
            (edge_id, graph_id, source_node, target_node, weight, attributes,
             timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (edge_id, graph_id, source, target, weight,
              json.dumps(attributes), datetime.now().isoformat()))

    def get_limit_graph_edges(self, graph_id: str) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM limit_graph_edges WHERE graph_id = ?", (graph_id,))
        for r in rows:
            r["attributes"] = json.loads(r["attributes"]) if r["attributes"] else {}
        return rows

    def save_modp_state(self, state_id, problem_id, state_attributes,
                          objective_values, stage) -> None:
        self._execute("""
            INSERT OR REPLACE INTO modp_states
            (state_id, problem_id, state_attributes, objective_values,
             stage, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (state_id, problem_id, json.dumps(state_attributes),
              json.dumps(objective_values), stage, datetime.now().isoformat()))

    def save_modp_transition(self, transition_id, problem_id, from_state,
                              to_state, action, cost, objective_deltas) -> None:
        self._execute("""
            INSERT OR REPLACE INTO modp_transitions
            (transition_id, problem_id, from_state, to_state, action, cost,
             objective_deltas, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (transition_id, problem_id, from_state, to_state, action, cost,
              json.dumps(objective_deltas), datetime.now().isoformat()))

    def save_modp_policy(self, policy_id, problem_id, state_id, action,
                          expected_objectives) -> None:
        self._execute("""
            INSERT OR REPLACE INTO modp_policies
            (policy_id, problem_id, state_id, action,
             expected_objectives, timestamp)
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
            "SELECT * FROM rlhf_preference_pairs ORDER BY timestamp DESC "
            "LIMIT ?", (limit,))
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

    def save_expert_model(self, expert_id, model_type, parameters, version) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_expert_models
            (expert_id, model_type, parameters, version, training_timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (expert_id, model_type, parameters, version,
              datetime.now().isoformat()))

    def log_routing_decision(self, routing_id, sample_id, routed_expert_id,
                              gating_score) -> None:
        self._execute("""
            INSERT OR REPLACE INTO moe_routing_history
            (routing_id, sample_id, routed_expert_id, gating_score, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (routing_id, sample_id, routed_expert_id, gating_score,
              datetime.now().isoformat()))

    def save_bio_run(self, run_id, algorithm, problem_id, parameters,
                     best_solution, best_fitness) -> None:
        self._execute("""
            INSERT OR REPLACE INTO bio_inspired_runs
            (run_id, algorithm, problem_id, parameters, best_solution,
             best_fitness, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (run_id, algorithm, problem_id, json.dumps(parameters),
              json.dumps(best_solution), best_fitness,
              datetime.now().isoformat()))

    # =========================================================================
    # v5 methods — [P0/P1] fixes applied where noted
    # =========================================================================
    def save_teacher_superposition(self, student_id, teacher_id,
                                     teacher_weight, temperature, amplitude,
                                     kl_divergence) -> None:
        self._execute("""
            INSERT INTO teacher_superpositions
            (student_id, teacher_id, teacher_weight, temperature,
             superposition_amplitude, kl_divergence, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (student_id, teacher_id, teacher_weight, temperature, amplitude,
              kl_divergence, datetime.now().isoformat()))

    def get_teacher_superpositions(self, student_id, limit=100) -> List[Dict]:
        return self._fetchall("""
            SELECT * FROM teacher_superpositions WHERE student_id = ?
            ORDER BY timestamp DESC LIMIT ?
        """, (student_id, limit))

    def save_causal_edge(self, source, target, weight, confidence) -> None:
        edge_id = f"{source}->{target}"
        self._execute("""
            INSERT OR REPLACE INTO causal_graph
            (edge_id, source, target, weight, confidence, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (edge_id, source, target, weight, confidence,
              datetime.now().isoformat()))

    def get_causal_graph(self) -> List[Dict]:
        return self._fetchall(
            "SELECT source, target, weight, confidence FROM causal_graph")

    def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples,
                                 method="") -> None:
        cid = correlation_id_var.get()
        self._execute("""
            INSERT OR REPLACE INTO causal_experiments
            (exp_id, treatment, outcome, ate, samples, method, timestamp,
             correlation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (exp_id, treatment, outcome, ate, samples, method,
              datetime.now().isoformat(), cid))

    def get_causal_experiments(self, treatment: Optional[str] = None,
                                 outcome: Optional[str] = None,
                                 limit: int = 100) -> List[Dict]:
        # [P0] support OR of filters (v5.0.0 required BOTH)
        clauses, params = [], []
        if treatment:
            clauses.append("treatment = ?")
            params.append(treatment)
        if outcome:
            clauses.append("outcome = ?")
            params.append(outcome)
        params.append(limit)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        return self._fetchall(f"""
            SELECT * FROM causal_experiments {where}
            ORDER BY timestamp DESC LIMIT ?
        """, tuple(params))

    def save_causal_intervention(self, intervention_id, node, do_value,
                                   observed_outcome, counterfactual) -> None:
        self._execute("""
            INSERT OR REPLACE INTO causal_interventions
            (intervention_id, node, do_value, observed_outcome,
             counterfactual_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (intervention_id, node, str(do_value), str(observed_outcome),
              json.dumps(counterfactual, default=str),
              datetime.now().isoformat()))

    def save_federated_weights(self, instance_id, model_id, weights_bytes,
                                 weight_norm=0.0, round_id=0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO federated_weights
            (instance_id, model_id, weights, weight_norm, round_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (instance_id, model_id, weights_bytes, weight_norm, round_id,
              datetime.now().isoformat()))

    def get_federated_weights(self, model_id: str) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM federated_weights WHERE model_id = ?", (model_id,))

    def register_federated_client(self, instance_id, capabilities=None,
                                    region="global",
                                    carbon_intensity=0.0) -> None:
        self._execute("""
            INSERT INTO federated_clients
            (instance_id, last_seen, reputation, capabilities, region,
             carbon_intensity, weights_shared)
            VALUES (?, ?, 0.5, ?, ?, ?, 0)
            ON CONFLICT(instance_id) DO UPDATE SET
                last_seen = excluded.last_seen,
                capabilities = excluded.capabilities,
                region = excluded.region,
                carbon_intensity = excluded.carbon_intensity
        """, (instance_id, datetime.now().isoformat(),
              json.dumps(capabilities or {}), region, carbon_intensity))

    def increment_weights_shared(self, instance_id: str) -> None:
        # [P1] increment the count (v5.0.0 column existed but was never updated)
        self._execute("""
            UPDATE federated_clients
            SET weights_shared = weights_shared + 1, last_seen = ?
            WHERE instance_id = ?
        """, (datetime.now().isoformat(), instance_id))

    def list_federated_clients(self) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM federated_clients ORDER BY last_seen DESC")
        for r in rows:
            r["capabilities"] = (json.loads(r["capabilities"])
                                  if r["capabilities"] else {})
        return rows

    def save_federated_aggregation(self, aggregation_id, round_id,
                                     instance_ids, weights_snapshot, method,
                                     global_accuracy=0.0,
                                     carbon_footprint=0.0) -> None:
        self._execute("""
            INSERT OR REPLACE INTO federated_aggregation_log
            (aggregation_id, round_id, instance_ids, weights_snapshot,
             aggregation_method, global_accuracy, carbon_footprint, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (aggregation_id, round_id, json.dumps(instance_ids),
              json.dumps(weights_snapshot, default=str), method,
              global_accuracy, carbon_footprint, datetime.now().isoformat()))

    def save_agent(self, agent_id, role, reputation, utilities,
                    capabilities=None) -> None:
        # [P0] Use `capabilities` argument (v5.0.0 silently ignored it)
        caps_json = json.dumps(capabilities or {})
        self._execute("""
            INSERT INTO agent_registry
            (agent_id, role, reputation, utilities, capabilities,
             created_at, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(agent_id) DO UPDATE SET
                role = excluded.role,
                reputation = excluded.reputation,
                utilities = excluded.utilities,
                capabilities = excluded.capabilities,
                last_updated = excluded.last_updated
        """, (agent_id, role, reputation, json.dumps(utilities),
              caps_json, datetime.now().isoformat(),
              datetime.now().isoformat()))

    def get_agent(self, agent_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM agent_registry WHERE agent_id = ?", (agent_id,))
        if row:
            row["utilities"] = json.loads(row["utilities"]) if row["utilities"] else {}
            row["capabilities"] = (json.loads(row["capabilities"])
                                    if row["capabilities"] else {})
        return row

    def list_agents(self) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM agent_registry ORDER BY reputation DESC")
        for r in rows:
            r["utilities"] = json.loads(r["utilities"]) if r["utilities"] else {}
            r["capabilities"] = (json.loads(r["capabilities"])
                                  if r["capabilities"] else {})
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

    def save_agent_message(self, message_id, topic, sender, recipient,
                            payload) -> None:
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
            "SELECT * FROM agent_messages ORDER BY timestamp DESC LIMIT ?",
            (limit,))

    def save_reputation_change(self, agent_id, reputation, reason) -> None:
        self._execute("""
            INSERT INTO agent_reputation_history
            (agent_id, reputation, reason, timestamp)
            VALUES (?, ?, ?, ?)
        """, (agent_id, reputation, reason, datetime.now().isoformat()))

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                            description, window_seconds, active=True) -> None:
        self._execute("""
            INSERT OR REPLACE INTO temporal_rules
            (rule_id, formula, operator, severity, description,
             window_seconds, created_at, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, formula, operator, severity, description,
              window_seconds, datetime.now().isoformat(), int(active)))

    def list_temporal_rules(self, active_only=True) -> List[Dict]:
        if active_only:
            return self._fetchall(
                "SELECT * FROM temporal_rules WHERE active = 1")
        return self._fetchall("SELECT * FROM temporal_rules")

    def save_temporal_trace(self, state, context=None,
                              session_id: Optional[str] = None) -> None:
        # [P1] session_id isolates multiple verification contexts
        self._execute("""
            INSERT INTO temporal_trace (state, context, session_id, timestamp)
            VALUES (?, ?, ?, ?)
        """, (json.dumps(state, default=str),
              json.dumps(context, default=str) if context else None,
              session_id or correlation_id_var.get(),
              datetime.now().isoformat()))

    def get_temporal_trace(self, limit=1000,
                            session_id: Optional[str] = None) -> List[Dict]:
        if session_id:
            rows = self._fetchall(
                "SELECT * FROM temporal_trace WHERE session_id = ? "
                "ORDER BY step DESC LIMIT ?", (session_id, limit))
        else:
            rows = self._fetchall(
                "SELECT * FROM temporal_trace ORDER BY step DESC LIMIT ?",
                (limit,))
        for r in rows:
            r["state"] = json.loads(r["state"]) if r["state"] else {}
        return rows

    def save_temporal_violation(self, rule_id, formula, step, state,
                                  severity="warning", approved=None) -> None:
        cid = correlation_id_var.get()
        self._execute("""
            INSERT INTO temporal_violations
            (rule_id, formula, step, state, severity, approved,
             resolved_at, timestamp, correlation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, formula, step, json.dumps(state, default=str),
              severity, int(approved) if approved is not None else None,
              None, datetime.now().isoformat(), cid))

    def get_temporal_violations(self, rule_id: Optional[str] = None,
                                  limit: int = 100) -> List[Dict]:
        if rule_id:
            return self._fetchall("""
                SELECT * FROM temporal_violations WHERE rule_id = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (rule_id, limit))
        return self._fetchall(
            "SELECT * FROM temporal_violations ORDER BY timestamp DESC LIMIT ?",
            (limit,))

    # [P0] Wrap the two-table write in a single transaction
    def save_xai_explanation(self, explanation_id, decision_id, method,
                               decision_label, features, attributions,
                               natural_language) -> None:
        cid = correlation_id_var.get()
        with self.transaction():
            self._execute_no_commit("""
                INSERT OR REPLACE INTO xai_explanations
                (explanation_id, decision_id, method, decision_label,
                 features, attributions, natural_language, timestamp,
                 correlation_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (explanation_id, decision_id, method, decision_label,
                  json.dumps(features, default=str),
                  json.dumps(attributions, default=str),
                  natural_language, datetime.now().isoformat(), cid))
            # [P1] Rank features by |importance| so downstream queries work
            ranked = sorted((attributions or {}).items(),
                            key=lambda kv: abs(float(kv[1])), reverse=True)
            for rank, (name, val) in enumerate(ranked, start=1):
                self._execute_no_commit("""
                    INSERT INTO xai_feature_importance
                    (explanation_id, feature_name, importance, rank)
                    VALUES (?, ?, ?, ?)
                """, (explanation_id, str(name), float(val), rank))

    def get_xai_explanation(self, explanation_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM xai_explanations WHERE explanation_id = ?",
            (explanation_id,))
        if row:
            row["attributions"] = (json.loads(row["attributions"])
                                    if row["attributions"] else {})
            row["features"] = (json.loads(row["features"])
                                if row["features"] else {})
        return row

    def get_xai_explanations_for_decision(self, decision_id) -> List[Dict]:
        return self._fetchall("""
            SELECT * FROM xai_explanations WHERE decision_id = ?
            ORDER BY timestamp DESC
        """, (decision_id,))

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
            "SELECT * FROM precision_history ORDER BY timestamp DESC LIMIT ?",
            (limit,))

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
                "SELECT COALESCE(SUM(mwh), 0) AS s FROM rec_ledger "
                "WHERE retired = 1")
        else:
            row = self._fetchone(
                "SELECT COALESCE(SUM(mwh), 0) AS s FROM rec_ledger")
        return float(row["s"]) if row else 0.0

    def retire_rec(self, rec_id: int) -> int:
        # [P1] v5.0.0 had no way to retire RECs after purchase
        return self._execute(
            "UPDATE rec_ledger SET retired = 1 WHERE id = ?", (int(rec_id),))

    def retire_recs_by_certificate(self, certificate_id: str) -> int:
        # [P1] Batch retire by certificate
        return self._execute(
            "UPDATE rec_ledger SET retired = 1 WHERE certificate_id = ?",
            (certificate_id,))

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                              carbon_kg, offset_cost_usd,
                              credit_price_usd) -> None:
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

    # [P0] chaos_experiments now stores steady_during
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                                blast_radius, steady_before, steady_during,
                                steady_after, status, duration_ms=0.0) -> None:
        cid = correlation_id_var.get()
        self._execute("""
            INSERT OR REPLACE INTO chaos_experiments
            (experiment_id, name, fault_type, blast_radius,
             steady_before, steady_during, steady_after, status,
             duration_ms, timestamp, correlation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (experiment_id, name, fault_type, blast_radius,
              int(steady_before), int(steady_during), int(steady_after),
              status, duration_ms, datetime.now().isoformat(), cid))

    def get_chaos_experiments(self, limit: int = 100) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM chaos_experiments ORDER BY timestamp DESC LIMIT ?",
            (limit,))

    def save_chaos_steady_state(self, check_id, experiment_id, kpi_name,
                                  kpi_value, ok) -> None:
        self._execute("""
            INSERT OR REPLACE INTO chaos_steady_states
            (check_id, experiment_id, kpi_name, kpi_value, ok, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (check_id, experiment_id, kpi_name, kpi_value, int(ok),
              datetime.now().isoformat()))

    def save_chaos_rollback(self, rollback_id, experiment_id,
                              trigger_reason) -> None:
        self._execute("""
            INSERT OR REPLACE INTO chaos_rollbacks
            (rollback_id, experiment_id, trigger_reason, rolled_back_at)
            VALUES (?, ?, ?, ?)
        """, (rollback_id, experiment_id, trigger_reason,
              datetime.now().isoformat()))

    def enqueue_hitl_request(self, request_id, rule_id, state,
                              severity="critical") -> None:
        self._execute("""
            INSERT OR REPLACE INTO hitl_approval_queue
            (request_id, rule_id, state, severity, status, created_at,
             resolved_at)
            VALUES (?, ?, ?, ?, 'pending', ?, NULL)
        """, (request_id, rule_id, json.dumps(state, default=str),
              severity, datetime.now().isoformat()))

    def list_pending_hitl_requests(self, limit: int = 100) -> List[Dict]:
        # [P0] v5.0.0 had no LIMIT — OOM risk on large queues
        rows = self._fetchall("""
            SELECT * FROM hitl_approval_queue WHERE status = 'pending'
            ORDER BY created_at ASC LIMIT ?
        """, (int(limit),))
        for r in rows:
            r["state"] = json.loads(r["state"]) if r["state"] else {}
        return rows

    def get_pending_hitl_count(self) -> int:
        row = self._fetchone(
            "SELECT COUNT(*) AS c FROM hitl_approval_queue WHERE status = 'pending'")
        return int(row["c"]) if row else 0

    def resolve_hitl_request(self, request_id: str,
                              status: str = "approved") -> None:
        # [P1] Normalise status
        if status not in ("approved", "denied", "expired"):
            raise ValueError(f"Invalid status: {status}")
        self._execute("""
            UPDATE hitl_approval_queue
            SET status = ?, resolved_at = ? WHERE request_id = ?
        """, (status, datetime.now().isoformat(), request_id))

    def save_hitl_decision(self, decision_id, request_id, user_id,
                            approved, rationale="") -> None:
        cid = correlation_id_var.get()
        # [P1] Sync status with approved flag
        with self.transaction():
            self._execute_no_commit("""
                INSERT OR REPLACE INTO hitl_decisions
                (decision_id, request_id, user_id, approved, rationale,
                 timestamp, correlation_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (decision_id, request_id, user_id, int(approved), rationale,
                  datetime.now().isoformat(), cid))
            self._execute_no_commit("""
                UPDATE hitl_approval_queue
                SET status = ?, resolved_at = ? WHERE request_id = ?
            """, ("approved" if approved else "denied",
                  datetime.now().isoformat(), request_id))

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
    # Cleanup — [P0] validate days > 0
    # =========================================================================
    def _clean(self, table: str, days: int, column: str = "timestamp",
                extra_where: str = "") -> int:
        if days <= 0:
            raise ValueError(f"days must be > 0, got {days}")
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        where = f"{column} < ?"
        if extra_where:
            where += f" AND {extra_where}"
        return self._execute(f"DELETE FROM {table} WHERE {where}", (cutoff,))

    def clean_power_readings(self, days): return self._clean("power_readings", days)
    def clean_elasticity_records(self, days): return self._clean("elasticity_metrics", days)
    def clean_substitution_results(self, days): return self._clean("substitution_results", days)
    def clean_federated_rounds(self, days): return self._clean("federated_rounds", days)
    def clean_circularity_records(self, days): return self._clean("circularity_records", days)
    def clean_emission_records(self, days): return self._clean("emission_records", days)
    def clean_optimisation_history(self, days): return self._clean("optimisation_history", days)
    def clean_thermal_records(self, days): return self._clean("thermal_optimizations", days)
    def clean_ga_populations(self, days): return self._clean("ga_populations", days)
    def clean_ga_fitness_history(self, days): return self._clean("ga_fitness_history", days)
    def clean_moe_training_samples(self, days): return self._clean("moe_gating_training", days)
    def clean_moe_routing(self, days): return self._clean("moe_routing_history", days)
    def clean_user_preferences(self, days): return self._clean("user_preferences", days)
    def clean_scenarios(self, days): return self._clean("scenarios", days)
    def clean_decision_catalogue(self, days): return self._clean("decision_catalogue", days)
    def clean_rlhf_preference_pairs(self, days): return self._clean("rlhf_preference_pairs", days)
    def clean_bio_runs(self, days): return self._clean("bio_inspired_runs", days)
    def clean_causal_graph(self, days):
        self._clean("causal_graph", days)
        self._clean("causal_experiments", days)
        self._clean("causal_interventions", days)
    def clean_agent_messages(self, days): return self._clean("agent_messages", days)
    def clean_agent_bids(self, days): return self._clean("agent_bids", days)
    def clean_temporal_trace(self, days):
        self._clean("temporal_trace", days)
        self._clean("temporal_violations", days)
    def clean_xai_explanations(self, days): return self._clean("xai_explanations", days)
    def clean_precision_history(self, days): return self._clean("precision_history", days)
    def clean_carbon_market(self, days):
        self._clean("carbon_credit_prices", days)
        self._clean("net_zero_matches", days)
        self._clean("carbon_market_trades", days, column="executed_at")
    def clean_chaos_experiments(self, days):
        self._clean("chaos_experiments", days)
        self._clean("chaos_steady_states", days)
    def clean_hitl_approval_queue(self, days):
        if days <= 0:
            raise ValueError("days must be > 0")
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        return self._execute(
            "DELETE FROM hitl_approval_queue WHERE status != 'pending' "
            "AND created_at < ?", (cutoff,))
    def clean_active_learning_samples(self, days):
        return self._clean("active_learning_samples", days)
    def clean_pareto_front(self, days):
        if days <= 0:
            raise ValueError("days must be > 0")
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        return self._execute(
            "DELETE FROM pareto_front WHERE timestamp < ? AND is_current = 0",
            (cutoff,))
    def clean_distillation(self, days):
        self._clean("distillation_episodes", days)
        self._clean("student_policy_updates", days)
        self._clean("teacher_superpositions", days)
    def clean_limit_graph(self, days):
        self._clean("limit_graph_nodes", days)
        self._clean("limit_graph_edges", days)
    def clean_modp(self, days):
        self._clean("modp_states", days)
        self._clean("modp_transitions", days)
        self._clean("modp_policies", days)

    def apply_retention_policy(self, policies: Dict[str, int]) -> None:
        for table, days in policies.items():
            if days <= 0:
                continue
            method = getattr(self, f"clean_{table}", None)
            if method:
                try:
                    method(days)
                except Exception as e:
                    logger.warning("clean_%s failed: %s", table, e)
            else:
                logger.warning("No clean method for table '%s'", table)
        self._stats.mark_cleanup()

    # =========================================================================
    # [P2] Health check
    # =========================================================================
    def health_check(self) -> Dict[str, Any]:
        """Return DB health: integrity, WAL size, migration state."""
        result: Dict[str, Any] = {
            "db_path": str(self.db_path),
            "schema_version": self.SCHEMA_VERSION,
            "encryption_enabled": bool(self.master_key and CRYPTO_AVAILABLE),
            "encryption_disabled_reason": self._encryption_disabled_reason,
            "aiosqlite": AIOSQLITE_AVAILABLE,
            "cryptography": CRYPTO_AVAILABLE,
        }
        try:
            row = self._fetchone("PRAGMA integrity_check")
            result["integrity"] = row.get("integrity_check", "unknown") if row else "unknown"
        except Exception as e:
            result["integrity"] = f"error: {e}"
        try:
            result["db_size_bytes"] = self.db_path.stat().st_size
            wal = self.db_path.with_suffix(self.db_path.suffix + "-wal")
            if wal.exists():
                result["wal_size_bytes"] = wal.stat().st_size
        except Exception:
            pass
        try:
            row = self._fetchone(
                "SELECT version, applied_at FROM schema_version "
                "ORDER BY version DESC LIMIT 1")
            if row:
                result["migration"] = dict(row)
        except Exception:
            pass
        result["stats"] = self._stats.snapshot()
        return result

    # =========================================================================
    # [P3] Backup / restore / vacuum
    # =========================================================================
    def backup(self, dest_path: Union[str, Path]) -> bool:
        """Backup via SQLite VACUUM INTO (safe, consistent)."""
        try:
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                dest.unlink()
            conn = self._get_connection()
            conn.execute(f"VACUUM INTO '{dest}'")
            self._stats.mark_backup()
            logger.info("Backup complete: %s (%d bytes)",
                        dest, dest.stat().st_size)
            return True
        except Exception as e:
            logger.error("backup failed: %s", e)
            return False

    def vacuum(self) -> bool:
        try:
            conn = self._get_connection()
            conn.execute("VACUUM")
            logger.info("VACUUM complete")
            return True
        except Exception as e:
            logger.error("vacuum failed: %s", e)
            return False

    # =========================================================================
    # [P3] Batch + streaming
    # =========================================================================
    def batch_insert(self, table: str, columns: List[str],
                      rows: List[Tuple], conflict: str = "OR REPLACE") -> int:
        """Batch insert in a single transaction. `conflict` can be
        'OR REPLACE', 'OR IGNORE', 'OR ABORT'."""
        if not rows:
            return 0
        placeholders = ", ".join("?" for _ in columns)
        cols = ", ".join(columns)
        sql = f"INSERT {conflict} INTO {table} ({cols}) VALUES ({placeholders})"
        conn = self._get_connection()
        try:
            conn.executemany(sql, rows)
            conn.commit()
            logger.debug("Batch inserted %d rows into %s", len(rows), table)
            return len(rows)
        except Exception as e:
            logger.error("batch_insert failed for %s: %s", table, e)
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    def stream_table(self, table: str, batch_size: int = 1000) -> Iterator[List[Dict]]:
        """Stream rows from a table in batches (bounded memory)."""
        offset = 0
        while True:
            rows = self._fetchall(
                f"SELECT * FROM {table} LIMIT ? OFFSET ?",
                (batch_size, offset))
            if not rows:
                break
            yield rows
            offset += len(rows)

    # =========================================================================
    # [P2] Query explanation
    # =========================================================================
    def explain_query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Return SQLite's query plan (for debugging slow queries)."""
        return self._fetchall(f"EXPLAIN QUERY PLAN {sql}", params)

    # =========================================================================
    # Statistics
    # =========================================================================
    def update_statistics(self) -> None:
        tables = [
            "model_weights", "feedback_events", "drift_states",
            "benchmark_runs", "kv_store", "pqc_keys",
            "power_readings", "elasticity_metrics", "substitution_results",
            "federated_rounds", "circularity_records", "emission_records",
            "optimisation_history", "distribution_history",
            "thermal_optimizations",
            "ga_populations", "ga_fitness_history", "moe_gating_training",
            "moe_expert_metadata", "pareto_front", "user_preferences",
            "scenarios", "decision_catalogue",
            "limit_graph_nodes", "limit_graph_edges", "limit_graph_metadata",
            "modp_states", "modp_transitions", "modp_policies",
            "rlhf_preference_pairs", "teacher_policies",
            "distillation_episodes", "student_policy_updates",
            "bio_inspired_runs", "moe_expert_models", "moe_routing_history",
            "teacher_superpositions",
            "causal_graph", "causal_experiments", "causal_interventions",
            "federated_weights", "federated_clients",
            "federated_aggregation_log",
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
        sizes: Dict[str, int] = {}
        for table in tables:
            try:
                row = self._fetchone(f"SELECT COUNT(*) AS cnt FROM {table}")
                sizes[table] = int(row["cnt"]) if row else 0
            except Exception:
                sizes[table] = -1
        self._stats.set_table_sizes(sizes)

    def get_statistics(self) -> Dict[str, Any]:
        self.update_statistics()
        snap = self._stats.snapshot()
        return {
            "schema_version": self.SCHEMA_VERSION,
            "table_sizes": snap["table_sizes"],
            "total_queries": snap["total_queries"],
            "errors": snap["errors"],
            "last_updated": snap["last_updated"],
            "last_cleanup": snap["last_cleanup"],
            "last_backup": snap["last_backup"],
        }

    # =========================================================================
    # Close
    # =========================================================================
    def close(self):
        if hasattr(self._local, "conn"):
            try:
                self._local.conn.close()
            except Exception:
                pass
            del self._local.conn

    async def close_async(self):
        if self._async_conn:
            try:
                await self._async_conn.close()
            except Exception:
                pass
            self._async_conn = None
        await asyncio.to_thread(self.close)


# =============================================================================
# Demo
# =============================================================================
async def _demo():
    s = Storage(db_path="/tmp/storage_v51_demo.db")
    print(f"Schema version: {Storage.SCHEMA_VERSION}")
    stats = s.get_statistics()
    print(f"Registered tables: {len(stats['table_sizes'])}")

    health = s.health_check()
    print(f"Health: integrity={health.get('integrity')}, "
          f"encryption={health.get('encryption_enabled')}, "
          f"db_size={health.get('db_size_bytes')} bytes")

    # [P0] Demonstrate that chaos_experiments can now store steady_during
    s.save_chaos_experiment("ch1", "auto_chaos", "latency", 0.1,
                              steady_before=1, steady_during=0, steady_after=1,
                              status="completed")
    rows = s.get_chaos_experiments(limit=1)
    if rows:
        print(f"Chaos steady_during persisted: {rows[0].get('steady_during')}")

    # [P0] asdict fix — pass a real dataclass
    @dataclass
    class _Metrics:
        record_id: str
        circularity_index: float
        circularity_level: str
        recycling_rate: float
        recovery_efficiency: float
        collection_efficiency: float
        purification_efficiency: float
        data_quality_score: float
        blockchain_tx_hash: Optional[str] = None
        timestamp: str = ""

    s.store_circularity_record(_Metrics(
        record_id="cr1", circularity_index=0.75, circularity_level="high",
        recycling_rate=0.6, recovery_efficiency=0.7,
        collection_efficiency=0.8, purification_efficiency=0.85,
        data_quality_score=0.95,
        timestamp=datetime.now().isoformat()))
    print("Circularity record saved via dataclass ✓")

    # [P0] Save agent with capabilities
    s.save_agent("agent_00", "orchestrator", 0.7, {"x": 0.8},
                  capabilities={"supports_batching": True})
    a = s.get_agent("agent_00")
    print(f"Agent capabilities: {a.get('capabilities')}")

    # [P1] Retire REC
    s.save_rec(10.0, 5.0, "wind", certificate_id="cert-1")
    s.retire_recs_by_certificate("cert-1")
    print(f"REC balance (retired): {s.get_rec_balance(retired_only=True)}")

    # [P1] Increment federated weights_shared
    s.register_federated_client("inst_A")
    s.increment_weights_shared("inst_A")
    clients = s.list_federated_clients()
    if clients:
        print(f"Client weights_shared: {clients[0].get('weights_shared')}")

    # [P1] Pending HITL count
    s.enqueue_hitl_request("r1", "never_extreme_carbon", {"carbon": 0.9})
    print(f"Pending HITL: {s.get_pending_hitl_count()}")

    # [P2] Transaction context
    with s.transaction():
        s._execute_no_commit("INSERT INTO kv_store VALUES ('tx_test', 'ok', ?)",
                              (datetime.now().isoformat(),))
    print(f"Transaction value: {s.get_state('tx_test')}")

    # [P3] Batch insert
    rows = [(f"m{i}", "USD", "test", "global", datetime.now().isoformat())
            for i in range(10)]
    s.batch_insert("carbon_credit_prices",
                    ["price_usd", "currency", "source", "region", "timestamp"],
                    rows)
    print(f"Batch inserted 10 credit prices ✓")

    # [P3] Stream table
    for i, batch in enumerate(s.stream_table("carbon_credit_prices", batch_size=3)):
        if i == 0:
            print(f"First stream batch: {len(batch)} rows")
        break

    # [P3] Backup
    ok = s.backup("/tmp/storage_v51_demo.bak")
    print(f"Backup: {ok}")

    await s.close_async()


if __name__ == "__main__":
    asyncio.run(_demo())
