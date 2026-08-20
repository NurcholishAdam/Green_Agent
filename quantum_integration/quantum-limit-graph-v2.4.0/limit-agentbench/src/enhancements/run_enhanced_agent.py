#!/usr/bin/env python3
"""
Central Storage module for Green Agent enhancements – Version 3.0.0

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

NEW IN v3.0.0:
- Genetic Algorithm (GA) populations and fitness history.
- Mixture‑of‑Experts (MoE) gating training samples and expert metadata.
- Pareto front and user preferences for multi‑objective optimisation.
- Scenario definitions and decision option catalogue.
- Optional aiosqlite for true async I/O (fallback to thread pool).
- Incremental schema migration framework.
- Configurable data retention policies.
- Storage statistics tracking.
- High‑level query methods for all new tables.

All methods are synchronous; async wrappers are provided via `asyncio.to_thread`
or via `aiosqlite` if available.
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

    Features:
    - Connection pooling (thread-local) and optional aiosqlite for async.
    - WAL mode and foreign keys.
    - Schema versioning with incremental migrations.
    - Optional AES-GCM encryption for sensitive BLOB fields.
    - Comprehensive error handling and logging.
    - Async methods using aiosqlite (if available) or thread-pool.
    - Dedicated tables for GA, MoE, Pareto, user preferences, scenarios, etc.
    - Configurable data retention policies.
    - Storage statistics.

    Note: This class is intended to be used as a singleton or shared across modules.
    """

    # Schema version – increment when tables change
    SCHEMA_VERSION = 3

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

        # Async connection (if aiosqlite available) – will be created per call
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

    async def _get_async_connection(self) -> aiosqlite.Connection:
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
            # Schema version table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                )
            """)
            cur = conn.execute("SELECT version FROM schema_version")
            row = cur.fetchone()
            current_version = row[0] if row else 0

            if current_version < self.SCHEMA_VERSION:
                # Apply migrations incrementally
                for v in range(current_version + 1, self.SCHEMA_VERSION + 1):
                    migration_method = getattr(self, f"_migrate_to_v{v}", None)
                    if migration_method:
                        migration_method(conn)
                # Update version
                conn.execute("DELETE FROM schema_version")
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (self.SCHEMA_VERSION,))
                conn.commit()

    def _migrate_to_v1(self, conn: sqlite3.Connection) -> None:
        """Initial schema (v1)."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS model_weights (
                model_id TEXT PRIMARY KEY,
                weights BLOB,
                timestamp REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback_events (
                event_id TEXT PRIMARY KEY,
                timestamp REAL NOT NULL,
                task_id TEXT NOT NULL,
                model_id TEXT,
                teacher_id TEXT,
                selected_action TEXT NOT NULL,
                quality_score REAL NOT NULL,
                latency_ms REAL NOT NULL,
                energy_joules REAL NOT NULL,
                carbon_g REAL NOT NULL,
                helium_cost REAL,
                resource_usage TEXT,
                distillation_loss REAL,
                feedback_type TEXT NOT NULL,
                adaptive_cost_value REAL NOT NULL,
                metadata TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drift_states (
                snapshot_id TEXT PRIMARY KEY,
                timestamp REAL NOT NULL,
                online_weights BLOB,
                offline_weights BLOB,
                cost_score REAL,
                reason TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_runs (
                run_id TEXT PRIMARY KEY,
                timestamp REAL NOT NULL,
                policy_name TEXT NOT NULL,
                avg_quality REAL,
                avg_carbon REAL,
                avg_latency REAL,
                avg_cost REAL,
                total_energy REAL,
                sample_count INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP
            )
        """)
        # Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_task ON feedback_events(task_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_time ON feedback_events(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_policy ON benchmark_runs(policy_name);")

        # v1 also includes PQC keys table (for backward compatibility)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pqc_keys (
                key_id TEXT PRIMARY KEY,
                algorithm TEXT NOT NULL,
                public_key BLOB NOT NULL,
                public_nonce BLOB NOT NULL,
                private_key BLOB NOT NULL,
                private_nonce BLOB NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pqc_expires ON pqc_keys(expires_at);")

    def _migrate_to_v2(self, conn: sqlite3.Connection) -> None:
        """Additional tables for v2 (power, elasticity, substitution, federated, circularity, emission, optimisation, distribution, thermal)."""
        # Power readings
        conn.execute("""
            CREATE TABLE IF NOT EXISTS power_readings (
                reading_id TEXT PRIMARY KEY,
                power_watts REAL,
                carbon_intensity REAL,
                timestamp TEXT,
                metadata TEXT
            )
        """)
        # Elasticity metrics
        conn.execute("""
            CREATE TABLE IF NOT EXISTS elasticity_metrics (
                metric_id TEXT PRIMARY KEY,
                price_elasticity REAL,
                scarcity_elasticity REAL,
                cross_elasticity REAL,
                substitution_elasticity REAL,
                thermal_elasticity REAL,
                composite_elasticity REAL,
                scarcity_index REAL,
                quality_score REAL,
                data_quality_score REAL,
                market_regime TEXT,
                migration_urgency TEXT,
                tx_hash TEXT,
                timestamp TEXT
            )
        """)
        # Substitution results
        conn.execute("""
            CREATE TABLE IF NOT EXISTS substitution_results (
                analysis_id TEXT PRIMARY KEY,
                base_material TEXT,
                substitute TEXT,
                topsis_score REAL,
                carbon_reduction_pct REAL,
                cost_savings_pct REAL,
                sustainability_score REAL,
                confidence_score REAL,
                quality_score REAL,
                tx_hash TEXT,
                timestamp TEXT
            )
        """)
        # Federated rounds
        conn.execute("""
            CREATE TABLE IF NOT EXISTS federated_rounds (
                round_id INTEGER PRIMARY KEY,
                num_clients INTEGER,
                global_accuracy REAL,
                aggregated_loss REAL,
                strategy TEXT,
                carbon_footprint REAL,
                energy_used REAL,
                tx_hash TEXT,
                timestamp TEXT
            )
        """)
        # Circularity records
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circularity_records (
                record_id TEXT PRIMARY KEY,
                circularity_index REAL,
                circularity_level TEXT,
                recycling_rate REAL,
                recovery_efficiency REAL,
                collection_efficiency REAL,
                purification_efficiency REAL,
                data_quality_score REAL,
                tx_hash TEXT,
                timestamp TEXT
            )
        """)
        # Emission records
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emission_records (
                record_id TEXT PRIMARY KEY,
                scope TEXT,
                amount_kg REAL,
                source TEXT,
                location TEXT,
                verified INTEGER,
                region TEXT,
                user_id TEXT,
                timestamp TEXT,
                metadata TEXT
            )
        """)
        # Optimisation history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS optimisation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                result TEXT,
                timestamp TEXT NOT NULL
            )
        """)
        # Distribution history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS distribution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                optimal_provider TEXT NOT NULL,
                optimal_region TEXT NOT NULL,
                scores TEXT,
                data_size_gb REAL,
                timestamp TEXT NOT NULL
            )
        """)
        # Thermal optimizations
        conn.execute("""
            CREATE TABLE IF NOT EXISTS thermal_optimizations (
                id TEXT PRIMARY KEY,
                data TEXT,
                timestamp TEXT
            )
        """)
        # Indexes for new tables
        conn.execute("CREATE INDEX IF NOT EXISTS idx_power_time ON power_readings(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_elasticity_time ON elasticity_metrics(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_substitution_time ON substitution_results(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_federated_time ON federated_rounds(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_circularity_time ON circularity_records(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emission_time ON emission_records(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_opt_time ON optimisation_history(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dist_time ON distribution_history(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_thermal_time ON thermal_optimizations(timestamp);")

    def _migrate_to_v3(self, conn: sqlite3.Connection) -> None:
        """New tables for v3: GA, MoE, Pareto, user preferences, scenarios, decision catalogue."""
        # Genetic Algorithm populations
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ga_populations (
                generation INTEGER,
                individual_id TEXT,
                attributes TEXT,          -- JSON of decision attributes
                fitness REAL,
                timestamp TEXT,
                PRIMARY KEY (generation, individual_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ga_fitness_history (
                generation INTEGER PRIMARY KEY,
                best_fitness REAL,
                avg_fitness REAL,
                diversity REAL,
                timestamp TEXT
            )
        """)
        # MoE gating training samples
        conn.execute("""
            CREATE TABLE IF NOT EXISTS moe_gating_training (
                sample_id TEXT PRIMARY KEY,
                features TEXT,            -- JSON array
                expert_label INTEGER,
                reward REAL,
                timestamp TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS moe_expert_metadata (
                expert_id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                performance_score REAL,
                last_updated TEXT
            )
        """)
        # Pareto front
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pareto_front (
                solution_id TEXT PRIMARY KEY,
                decision_attributes TEXT, -- JSON of decision option
                accuracy REAL,
                carbon REAL,
                cost REAL,
                robustness REAL,
                is_current INTEGER,       -- 0 or 1
                timestamp TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pareto_current ON pareto_front(is_current);")
        # User preferences
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id TEXT,
                weights TEXT,             -- JSON of weight vector
                chosen_solution_id TEXT,
                timestamp TEXT,
                PRIMARY KEY (user_id, timestamp)
            )
        """)
        # Scenarios
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scenarios (
                scenario_id TEXT PRIMARY KEY,
                carbon_price REAL,
                discount_rate REAL,
                demand_growth_rate REAL,
                technology_cost_reduction REAL,
                regulatory_risk REAL,
                renewable_energy_share REAL,
                energy_efficiency REAL,
                timestamp TEXT
            )
        """)
        # Decision catalogue
        conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_catalogue (
                option_id TEXT PRIMARY KEY,
                name TEXT,
                attributes TEXT,          -- JSON of attributes
                timestamp TEXT
            )
        """)
        # Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ga_generation ON ga_populations(generation);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_moe_sample_time ON moe_gating_training(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_preferences_user ON user_preferences(user_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scenario_time ON scenarios(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_time ON decision_catalogue(timestamp);")

    # --------------------------------------------------------------------------
    # Encryption helpers (optional)
    # --------------------------------------------------------------------------
    def _encrypt_blob(self, data: bytes) -> Tuple[bytes, bytes]:
        """Encrypt data using AES-GCM if master key is available.
        Returns (ciphertext, nonce). If no key, returns (data, b'')."""
        if self.master_key and CRYPTO_AVAILABLE:
            nonce = secrets.token_bytes(12)
            aesgcm = AESGCM(self.master_key)
            ciphertext = aesgcm.encrypt(nonce, data, None)
            return ciphertext, nonce
        return data, b''

    def _decrypt_blob(self, ciphertext: bytes, nonce: bytes) -> bytes:
        """Decrypt data using AES-GCM if master key is available.
        If nonce is empty, returns ciphertext unchanged."""
        if self.master_key and CRYPTO_AVAILABLE and nonce:
            aesgcm = AESGCM(self.master_key)
            return aesgcm.decrypt(nonce, ciphertext, None)
        return ciphertext

    # --------------------------------------------------------------------------
    # Migration helper: encrypt existing plaintext BLOBs
    # --------------------------------------------------------------------------
    def encrypt_existing_blobs(self, table: str, column: str, nonce_column: str,
                               where_clause: str = "1") -> int:
        """
        Encrypt existing BLOB data in a table column that was stored in plaintext.
        This should be run after the master key is set.
        Returns the number of rows updated.
        """
        if not self.master_key or not CRYPTO_AVAILABLE:
            raise RuntimeError("Encryption not available (master key or cryptography missing)")

        # Fetch rows where nonce is empty (meaning not encrypted)
        rows = self._fetchall(
            f"SELECT rowid, {column} FROM {table} WHERE {nonce_column} = b'' AND {where_clause}"
        )
        count = 0
        for row in rows:
            rowid = row['rowid']
            plaintext = row[column]
            ciphertext, nonce = self._encrypt_blob(plaintext)
            self._execute(
                f"UPDATE {table} SET {column} = ?, {nonce_column} = ? WHERE rowid = ?",
                (ciphertext, nonce, rowid)
            )
            count += 1
        print(f"Encrypted {count} existing BLOBs in {table}.{column}")
        return count

    # --------------------------------------------------------------------------
    # Core methods (model_weights, feedback_events, drift_states, benchmark_runs)
    # --------------------------------------------------------------------------
    def save_model_weights(self, model_id: str, weights_bytes: bytes) -> None:
        """Store serialised model weights (BLOB)."""
        self._execute(
            "INSERT OR REPLACE INTO model_weights (model_id, weights, timestamp) VALUES (?, ?, ?)",
            (model_id, weights_bytes, time.time())
        )

    def load_model_weights(self, model_id: str) -> Optional[bytes]:
        """Retrieve serialised model weights (BLOB)."""
        row = self._fetchone(
            "SELECT weights FROM model_weights WHERE model_id = ?",
            (model_id,)
        )
        return row["weights"] if row else None

    def store_feedback_event(self, event: Dict) -> None:
        """Store a feedback event."""
        self._execute(
            """
            INSERT OR REPLACE INTO feedback_events (
                event_id, timestamp, task_id, model_id, teacher_id, selected_action,
                quality_score, latency_ms, energy_joules, carbon_g, helium_cost,
                resource_usage, distillation_loss, feedback_type, adaptive_cost_value, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                float(event["timestamp"]),
                event["task_id"],
                event.get("model_id"),
                event.get("teacher_id"),
                event["selected_action"],
                float(event["quality_score"]),
                float(event["latency_ms"]),
                float(event["energy_joules"]),
                float(event["carbon_g"]),
                event.get("helium_cost"),
                json.dumps(event.get("resource_usage", {})),
                event.get("distillation_loss"),
                event["feedback_type"],
                float(event["adaptive_cost_value"]),
                json.dumps(event.get("metadata", {})),
            )
        )

    def get_feedback_events(self, limit: int = 1000) -> List[Dict]:
        """Retrieve recent feedback events."""
        rows = self._fetchall(
            "SELECT * FROM feedback_events ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        for r in rows:
            # Parse JSON fields
            try:
                r["resource_usage"] = json.loads(r.get("resource_usage") or "{}")
            except Exception:
                r["resource_usage"] = r.get("resource_usage")
            try:
                r["metadata"] = json.loads(r.get("metadata") or "{}")
            except Exception:
                r["metadata"] = r.get("metadata")
        return rows

    def save_drift_snapshot(self, snapshot_id: str, online_w: Optional[bytes],
                            offline_w: Optional[bytes], cost: Optional[float],
                            reason: Optional[str]) -> None:
        """Store drift snapshot (BLOBs)."""
        self._execute(
            """
            INSERT OR REPLACE INTO drift_states (snapshot_id, timestamp, online_weights, offline_weights, cost_score, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, time.time(), online_w, offline_w, cost, reason)
        )

    def get_last_snapshot(self) -> Optional[Dict]:
        """Retrieve most recent drift snapshot."""
        row = self._fetchone(
            "SELECT * FROM drift_states ORDER BY timestamp DESC LIMIT 1"
        )
        if row:
            row["online_weights"] = row.get("online_weights")
            row["offline_weights"] = row.get("offline_weights")
        return row

    def store_benchmark_result(self, run_id: str, policy: str, metrics: Dict[str, float],
                               count: int) -> None:
        """Store benchmark run result."""
        self._execute(
            """
            INSERT OR REPLACE INTO benchmark_runs
            (run_id, timestamp, policy_name, avg_quality, avg_carbon, avg_latency, avg_cost, total_energy, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, time.time(), policy,
                metrics.get("quality"), metrics.get("carbon"),
                metrics.get("latency"), metrics.get("cost"),
                metrics.get("energy"), int(count)
            )
        )

    def get_benchmark_results(self, days_back: int = 7) -> List[Dict]:
        """Retrieve benchmark runs from the last N days."""
        cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()
        return self._fetchall(
            "SELECT * FROM benchmark_runs WHERE timestamp >= ? ORDER BY timestamp DESC",
            (cutoff,)
        )

    # --------------------------------------------------------------------------
    # Power readings (unchanged)
    # --------------------------------------------------------------------------
    def store_power_reading(self, reading: Dict) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO power_readings (reading_id, power_watts, carbon_intensity, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                reading["reading_id"],
                reading["power_watts"],
                reading.get("carbon_intensity"),
                reading["timestamp"],
                json.dumps(reading.get("metadata", {}))
            )
        )

    def clean_power_readings(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM power_readings WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Elasticity metrics (unchanged)
    # --------------------------------------------------------------------------
    def store_elasticity_metrics(self, metrics) -> None:
        # metrics is an instance of HeliumElasticityMetrics (or similar)
        data = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        self._execute(
            """
            INSERT OR REPLACE INTO elasticity_metrics (
                metric_id, price_elasticity, scarcity_elasticity, cross_elasticity,
                substitution_elasticity, thermal_elasticity, composite_elasticity,
                scarcity_index, quality_score, data_quality_score, market_regime,
                migration_urgency, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["metric_id"],
                data["price_elasticity"],
                data["scarcity_elasticity"],
                data["cross_elasticity"],
                data["substitution_elasticity"],
                data["thermal_elasticity"],
                data["composite_elasticity"],
                data["scarcity_index"],
                data["quality_score"],
                data["data_quality_score"],
                data["market_regime"],
                data["migration_urgency"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    def clean_elasticity_records(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM elasticity_metrics WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Substitution results (unchanged)
    # --------------------------------------------------------------------------
    def store_substitution_result(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        self._execute(
            """
            INSERT OR REPLACE INTO substitution_results (
                analysis_id, base_material, substitute, topsis_score,
                carbon_reduction_pct, cost_savings_pct, sustainability_score,
                confidence_score, quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["calculation_id"],
                data["base_material"],
                data["recommended_substitute"],
                data["topsis_score"],
                data["carbon_reduction_pct"],
                data["cost_savings_pct"],
                data["sustainability_score"],
                data["confidence_score"],
                data["data_quality_score"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    def clean_substitution_results(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM substitution_results WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Federated rounds (unchanged)
    # --------------------------------------------------------------------------
    def store_federated_round(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        self._execute(
            """
            INSERT OR REPLACE INTO federated_rounds (
                round_id, num_clients, global_accuracy, aggregated_loss,
                strategy, carbon_footprint, energy_used, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["round_id"],
                data["num_clients"],
                data["global_accuracy"],
                data["aggregated_loss"],
                data["strategy"],
                data["carbon_footprint"],
                data["energy_used"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    def clean_federated_rounds(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM federated_rounds WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Circularity records (unchanged)
    # --------------------------------------------------------------------------
    def store_circularity_record(self, metrics) -> None:
        data = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        self._execute(
            """
            INSERT OR REPLACE INTO circularity_records (
                record_id, circularity_index, circularity_level, recycling_rate,
                recovery_efficiency, collection_efficiency, purification_efficiency,
                data_quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["record_id"],
                data["circularity_index"],
                data["circularity_level"],
                data["recycling_rate"],
                data["recovery_efficiency"],
                data["collection_efficiency"],
                data["purification_efficiency"],
                data["data_quality_score"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    def clean_circularity_records(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM circularity_records WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Emission records (unchanged)
    # --------------------------------------------------------------------------
    def store_emission_record(self, record: Dict) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO emission_records (
                record_id, scope, amount_kg, source, location, verified,
                region, user_id, timestamp, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["record_id"],
                record["scope"],
                record["amount_kg"],
                record["source"],
                record["location"],
                1 if record.get("verified") else 0,
                record["region"],
                record["user_id"],
                record["timestamp"],
                json.dumps(record.get("metadata", {}))
            )
        )

    def clean_emission_records(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM emission_records WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Optimisation history (unchanged)
    # --------------------------------------------------------------------------
    def save_optimisation(self, strategy: str, result: Dict) -> None:
        self._execute(
            "INSERT INTO optimisation_history (strategy, result, timestamp) VALUES (?, ?, ?)",
            (strategy, json.dumps(result), datetime.now().isoformat())
        )

    def get_recent_optimisations(self, limit: int = 10) -> List[Dict]:
        rows = self._fetchall(
            "SELECT strategy, result, timestamp FROM optimisation_history ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return [{"strategy": r["strategy"], "result": json.loads(r["result"]), "timestamp": r["timestamp"]} for r in rows]

    def clean_optimisation_history(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM optimisation_history WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Distribution history (unchanged)
    # --------------------------------------------------------------------------
    def save_distribution(self, result: Dict) -> None:
        self._execute(
            """
            INSERT INTO distribution_history (optimal_provider, optimal_region, scores, data_size_gb, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                result["optimal_provider"],
                result["optimal_region"],
                json.dumps(result["scores"]),
                result.get("data_size_gb", 0),
                result["timestamp"]
            )
        )

    def get_recent_distributions(self, limit: int = 10) -> List[Dict]:
        rows = self._fetchall(
            """
            SELECT optimal_provider, optimal_region, scores, data_size_gb, timestamp
            FROM distribution_history ORDER BY id DESC LIMIT ?
            """,
            (limit,)
        )
        return [
            {
                "optimal_provider": r["optimal_provider"],
                "optimal_region": r["optimal_region"],
                "scores": json.loads(r["scores"]),
                "data_size_gb": r["data_size_gb"],
                "timestamp": r["timestamp"]
            }
            for r in rows
        ]

    # --------------------------------------------------------------------------
    # Thermal optimizations (unchanged)
    # --------------------------------------------------------------------------
    def store_thermal_optimization(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        self._execute(
            """
            INSERT OR REPLACE INTO thermal_optimizations (id, data, timestamp)
            VALUES (?, ?, ?)
            """,
            (
                data.get("id", f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
                json.dumps(data, default=str),
                datetime.now().isoformat()
            )
        )

    def clean_thermal_records(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM thermal_optimizations WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Generic key-value state (kv_store)
    # --------------------------------------------------------------------------
    def save_state(self, key: str, value: str) -> None:
        self._execute(
            "INSERT OR REPLACE INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, datetime.now().isoformat())
        )

    def get_state(self, key: str) -> Optional[str]:
        row = self._fetchone("SELECT value FROM kv_store WHERE key = ?", (key,))
        return row["value"] if row else None

    def delete_state(self, key: str) -> None:
        self._execute("DELETE FROM kv_store WHERE key = ?", (key,))

    # --------------------------------------------------------------------------
    # Post‑quantum cryptography keys (stored with encryption)
    # --------------------------------------------------------------------------
    def save_pqc_key(self, key_id: str, algorithm: str,
                     public_key: bytes, private_key: bytes,
                     expires_at: str) -> None:
        """Store a PQC keypair with optional encryption."""
        pub_cipher, pub_nonce = self._encrypt_blob(public_key)
        priv_cipher, priv_nonce = self._encrypt_blob(private_key)
        self._execute(
            """
            INSERT OR REPLACE INTO pqc_keys
            (key_id, algorithm, public_key, public_nonce, private_key, private_nonce, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key_id,
                algorithm,
                pub_cipher,
                pub_nonce,
                priv_cipher,
                priv_nonce,
                datetime.now().isoformat(),
                expires_at
            )
        )

    def get_pqc_key(self, key_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM pqc_keys WHERE key_id = ?",
            (key_id,)
        )
        if row:
            pub = self._decrypt_blob(row["public_key"], row["public_nonce"])
            priv = self._decrypt_blob(row["private_key"], row["private_nonce"])
            return {
                "key_id": row["key_id"],
                "algorithm": row["algorithm"],
                "public_key": pub,
                "private_key": priv,
                "created_at": row["created_at"],
                "expires_at": row["expires_at"]
            }
        return None

    def list_pqc_keys(self) -> List[str]:
        rows = self._fetchall("SELECT key_id FROM pqc_keys")
        return [r["key_id"] for r in rows]

    def delete_pqc_key(self, key_id: str) -> None:
        self._execute("DELETE FROM pqc_keys WHERE key_id = ?", (key_id,))

    # =========================================================================
    # NEW v3.0.0 METHODS: Genetic Algorithm
    # =========================================================================
    def save_ga_population(self, generation: int, individuals: List[Dict[str, Any]]) -> None:
        """
        Save a GA population.
        individuals: list of dict with keys 'individual_id', 'attributes', 'fitness'
        """
        for ind in individuals:
            self._execute(
                """
                INSERT OR REPLACE INTO ga_populations (generation, individual_id, attributes, fitness, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    generation,
                    ind["individual_id"],
                    json.dumps(ind["attributes"]),
                    ind["fitness"],
                    datetime.now().isoformat()
                )
            )

    def get_ga_population(self, generation: int) -> List[Dict]:
        """Retrieve all individuals for a given generation."""
        rows = self._fetchall(
            "SELECT individual_id, attributes, fitness FROM ga_populations WHERE generation = ?",
            (generation,)
        )
        return [
            {
                "individual_id": r["individual_id"],
                "attributes": json.loads(r["attributes"]),
                "fitness": r["fitness"]
            }
            for r in rows
        ]

    def get_ga_population_generations(self) -> List[int]:
        """Return sorted list of all generations that have data."""
        rows = self._fetchall("SELECT DISTINCT generation FROM ga_populations ORDER BY generation")
        return [r["generation"] for r in rows]

    def save_ga_fitness_history(self, generation: int, best_fitness: float,
                                 avg_fitness: float, diversity: float) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO ga_fitness_history (generation, best_fitness, avg_fitness, diversity, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (generation, best_fitness, avg_fitness, diversity, datetime.now().isoformat())
        )

    def get_ga_fitness_history(self, limit: int = 100) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM ga_fitness_history ORDER BY generation DESC LIMIT ?",
            (limit,)
        )
        return rows

    def clean_ga_populations(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM ga_populations WHERE timestamp < ?", (cutoff,))

    def clean_ga_fitness_history(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM ga_fitness_history WHERE timestamp < ?", (cutoff,))

    # =========================================================================
    # NEW v3.0.0 METHODS: Mixture-of-Experts (MoE)
    # =========================================================================
    def save_moe_training_sample(self, sample_id: str, features: List[float],
                                 expert_label: int, reward: float) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO moe_gating_training (sample_id, features, expert_label, reward, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (sample_id, json.dumps(features), expert_label, reward, datetime.now().isoformat())
        )

    def get_moe_training_samples(self, limit: int = 1000) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM moe_gating_training ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        for r in rows:
            r["features"] = json.loads(r["features"])
        return rows

    def clean_moe_training_samples(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM moe_gating_training WHERE timestamp < ?", (cutoff,))

    def save_moe_expert_metadata(self, expert_id: str, name: str,
                                  description: str, performance_score: float) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO moe_expert_metadata (expert_id, name, description, performance_score, last_updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            (expert_id, name, description, performance_score, datetime.now().isoformat())
        )

    def get_moe_expert_metadata(self, expert_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM moe_expert_metadata WHERE expert_id = ?",
            (expert_id,)
        )
        return row

    def list_moe_experts(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM moe_expert_metadata ORDER BY performance_score DESC")

    # =========================================================================
    # NEW v3.0.0 METHODS: Pareto front
    # =========================================================================
    def save_pareto_front(self, solutions: List[Dict]) -> None:
        """
        solutions: list of dict with keys:
            solution_id, decision_attributes (dict), accuracy, carbon, cost, robustness
        This replaces the current Pareto front.
        """
        # Mark all existing as not current
        self._execute("UPDATE pareto_front SET is_current = 0")
        for sol in solutions:
            self._execute(
                """
                INSERT OR REPLACE INTO pareto_front
                (solution_id, decision_attributes, accuracy, carbon, cost, robustness, is_current, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sol["solution_id"],
                    json.dumps(sol["decision_attributes"]),
                    sol["accuracy"],
                    sol["carbon"],
                    sol["cost"],
                    sol["robustness"],
                    1,
                    datetime.now().isoformat()
                )
            )

    def get_current_pareto_front(self) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM pareto_front WHERE is_current = 1 ORDER BY accuracy DESC"
        )
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    def get_pareto_front_history(self, limit: int = 10) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM pareto_front WHERE is_current = 0 ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    def clean_pareto_front(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM pareto_front WHERE timestamp < ? AND is_current = 0", (cutoff,))

    # =========================================================================
    # NEW v3.0.0 METHODS: User preferences
    # =========================================================================
    def save_user_preference(self, user_id: str, weights: Dict[str, float],
                              chosen_solution_id: Optional[str] = None) -> None:
        self._execute(
            """
            INSERT INTO user_preferences (user_id, weights, chosen_solution_id, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, json.dumps(weights), chosen_solution_id, datetime.now().isoformat())
        )

    def get_user_preferences(self, user_id: str, limit: int = 10) -> List[Dict]:
        rows = self._fetchall(
            "SELECT weights, chosen_solution_id, timestamp FROM user_preferences WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
            (user_id, limit)
        )
        for r in rows:
            r["weights"] = json.loads(r["weights"])
        return rows

    def get_latest_user_preference(self, user_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT weights, chosen_solution_id, timestamp FROM user_preferences WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1",
            (user_id,)
        )
        if row:
            row["weights"] = json.loads(row["weights"])
        return row

    def clean_user_preferences(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM user_preferences WHERE timestamp < ?", (cutoff,))

    # =========================================================================
    # NEW v3.0.0 METHODS: Scenarios
    # =========================================================================
    def save_scenario(self, scenario_id: str, scenario: Dict[str, Any]) -> None:
        """Store a scenario definition."""
        self._execute(
            """
            INSERT OR REPLACE INTO scenarios
            (scenario_id, carbon_price, discount_rate, demand_growth_rate,
             technology_cost_reduction, regulatory_risk, renewable_energy_share,
             energy_efficiency, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scenario_id,
                scenario.get("carbon_price", 50.0),
                scenario.get("discount_rate", 0.05),
                scenario.get("demand_growth_rate", 0.02),
                scenario.get("technology_cost_reduction", 0.1),
                scenario.get("regulatory_risk", 0.3),
                scenario.get("renewable_energy_share", 0.3),
                scenario.get("energy_efficiency", 0.7),
                datetime.now().isoformat()
            )
        )

    def get_scenario(self, scenario_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM scenarios WHERE scenario_id = ?",
            (scenario_id,)
        )
        return row

    def list_scenarios(self) -> List[Dict]:
        return self._fetchall("SELECT * FROM scenarios ORDER BY timestamp DESC")

    def clean_scenarios(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM scenarios WHERE timestamp < ?", (cutoff,))

    # =========================================================================
    # NEW v3.0.0 METHODS: Decision catalogue
    # =========================================================================
    def save_decision_option(self, option_id: str, name: str, attributes: Dict[str, Any]) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO decision_catalogue (option_id, name, attributes, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (option_id, name, json.dumps(attributes), datetime.now().isoformat())
        )

    def get_decision_option(self, option_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT option_id, name, attributes, timestamp FROM decision_catalogue WHERE option_id = ?",
            (option_id,)
        )
        if row:
            row["attributes"] = json.loads(row["attributes"])
        return row

    def list_decision_options(self) -> List[Dict]:
        rows = self._fetchall("SELECT option_id, name, attributes, timestamp FROM decision_catalogue ORDER BY timestamp DESC")
        for r in rows:
            r["attributes"] = json.loads(r["attributes"])
        return rows

    def clean_decision_catalogue(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self._execute("DELETE FROM decision_catalogue WHERE timestamp < ?", (cutoff,))

    # =========================================================================
    # CONFIGURABLE DATA RETENTION POLICIES
    # =========================================================================
    def apply_retention_policy(self, policies: Dict[str, int]) -> None:
        """
        Apply retention policies for various tables.
        policies: dict mapping table name to retention days (e.g., {"feedback_events": 30})
        Tables not listed will not be cleaned.
        """
        for table, days in policies.items():
            if days <= 0:
                continue
            method = getattr(self, f"clean_{table}", None)
            if method:
                method(days)
            else:
                print(f"WARNING: No clean method for table '{table}'")

    # =========================================================================
    # STORAGE STATISTICS
    # =========================================================================
    def update_statistics(self) -> None:
        """Update table size statistics."""
        tables = [
            "model_weights", "feedback_events", "drift_states", "benchmark_runs",
            "kv_store", "pqc_keys", "power_readings", "elasticity_metrics",
            "substitution_results", "federated_rounds", "circularity_records",
            "emission_records", "optimisation_history", "distribution_history",
            "thermal_optimizations", "ga_populations", "ga_fitness_history",
            "moe_gating_training", "moe_expert_metadata", "pareto_front",
            "user_preferences", "scenarios", "decision_catalogue"
        ]
        for table in tables:
            row = self._fetchone(f"SELECT COUNT(*) as cnt FROM {table}")
            self._stats["table_sizes"][table] = row["cnt"] if row else 0
        self._stats["last_updated"] = datetime.now().isoformat()

    def get_statistics(self) -> Dict[str, Any]:
        self.update_statistics()
        return {
            "table_sizes": self._stats["table_sizes"],
            "total_queries": self._stats["total_queries"],
            "last_updated": self._stats.get("last_updated"),
            "last_cleanup": self._stats.get("last_cleanup"),
        }

    # --------------------------------------------------------------------------
    # Async wrappers (for use in async modules)
    # --------------------------------------------------------------------------
    async def save_model_weights_async(self, model_id: str, weights_bytes: bytes) -> None:
        await self._execute_async(
            "INSERT OR REPLACE INTO model_weights (model_id, weights, timestamp) VALUES (?, ?, ?)",
            (model_id, weights_bytes, time.time())
        )

    async def load_model_weights_async(self, model_id: str) -> Optional[bytes]:
        row = await self._fetchone_async(
            "SELECT weights FROM model_weights WHERE model_id = ?",
            (model_id,)
        )
        return row["weights"] if row else None

    async def store_feedback_event_async(self, event: Dict) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO feedback_events (
                event_id, timestamp, task_id, model_id, teacher_id, selected_action,
                quality_score, latency_ms, energy_joules, carbon_g, helium_cost,
                resource_usage, distillation_loss, feedback_type, adaptive_cost_value, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                float(event["timestamp"]),
                event["task_id"],
                event.get("model_id"),
                event.get("teacher_id"),
                event["selected_action"],
                float(event["quality_score"]),
                float(event["latency_ms"]),
                float(event["energy_joules"]),
                float(event["carbon_g"]),
                event.get("helium_cost"),
                json.dumps(event.get("resource_usage", {})),
                event.get("distillation_loss"),
                event["feedback_type"],
                float(event["adaptive_cost_value"]),
                json.dumps(event.get("metadata", {})),
            )
        )

    async def get_feedback_events_async(self, limit: int = 1000) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT * FROM feedback_events ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        for r in rows:
            try:
                r["resource_usage"] = json.loads(r.get("resource_usage") or "{}")
            except Exception:
                r["resource_usage"] = r.get("resource_usage")
            try:
                r["metadata"] = json.loads(r.get("metadata") or "{}")
            except Exception:
                r["metadata"] = r.get("metadata")
        return rows

    async def save_drift_snapshot_async(self, snapshot_id: str, online_w: Optional[bytes],
                                        offline_w: Optional[bytes], cost: Optional[float],
                                        reason: Optional[str]) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO drift_states (snapshot_id, timestamp, online_weights, offline_weights, cost_score, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, time.time(), online_w, offline_w, cost, reason)
        )

    async def get_last_snapshot_async(self) -> Optional[Dict]:
        row = await self._fetchone_async(
            "SELECT * FROM drift_states ORDER BY timestamp DESC LIMIT 1"
        )
        if row:
            row["online_weights"] = row.get("online_weights")
            row["offline_weights"] = row.get("offline_weights")
        return row

    async def store_benchmark_result_async(self, run_id: str, policy: str,
                                           metrics: Dict[str, float], count: int) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO benchmark_runs
            (run_id, timestamp, policy_name, avg_quality, avg_carbon, avg_latency, avg_cost, total_energy, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, time.time(), policy,
                metrics.get("quality"), metrics.get("carbon"),
                metrics.get("latency"), metrics.get("cost"),
                metrics.get("energy"), int(count)
            )
        )

    async def get_benchmark_results_async(self, days_back: int = 7) -> List[Dict]:
        cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()
        return await self._fetchall_async(
            "SELECT * FROM benchmark_runs WHERE timestamp >= ? ORDER BY timestamp DESC",
            (cutoff,)
        )

    async def store_power_reading_async(self, reading: Dict) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO power_readings (reading_id, power_watts, carbon_intensity, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                reading["reading_id"],
                reading["power_watts"],
                reading.get("carbon_intensity"),
                reading["timestamp"],
                json.dumps(reading.get("metadata", {}))
            )
        )

    async def clean_power_readings_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM power_readings WHERE timestamp < ?", (cutoff,))

    async def store_elasticity_metrics_async(self, metrics) -> None:
        data = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        await self._execute_async(
            """
            INSERT OR REPLACE INTO elasticity_metrics (
                metric_id, price_elasticity, scarcity_elasticity, cross_elasticity,
                substitution_elasticity, thermal_elasticity, composite_elasticity,
                scarcity_index, quality_score, data_quality_score, market_regime,
                migration_urgency, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["metric_id"],
                data["price_elasticity"],
                data["scarcity_elasticity"],
                data["cross_elasticity"],
                data["substitution_elasticity"],
                data["thermal_elasticity"],
                data["composite_elasticity"],
                data["scarcity_index"],
                data["quality_score"],
                data["data_quality_score"],
                data["market_regime"],
                data["migration_urgency"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    async def clean_elasticity_records_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM elasticity_metrics WHERE timestamp < ?", (cutoff,))

    async def store_substitution_result_async(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        await self._execute_async(
            """
            INSERT OR REPLACE INTO substitution_results (
                analysis_id, base_material, substitute, topsis_score,
                carbon_reduction_pct, cost_savings_pct, sustainability_score,
                confidence_score, quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["calculation_id"],
                data["base_material"],
                data["recommended_substitute"],
                data["topsis_score"],
                data["carbon_reduction_pct"],
                data["cost_savings_pct"],
                data["sustainability_score"],
                data["confidence_score"],
                data["data_quality_score"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    async def clean_substitution_results_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM substitution_results WHERE timestamp < ?", (cutoff,))

    async def store_federated_round_async(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        await self._execute_async(
            """
            INSERT OR REPLACE INTO federated_rounds (
                round_id, num_clients, global_accuracy, aggregated_loss,
                strategy, carbon_footprint, energy_used, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["round_id"],
                data["num_clients"],
                data["global_accuracy"],
                data["aggregated_loss"],
                data["strategy"],
                data["carbon_footprint"],
                data["energy_used"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    async def clean_federated_rounds_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM federated_rounds WHERE timestamp < ?", (cutoff,))

    async def store_circularity_record_async(self, metrics) -> None:
        data = asdict(metrics) if hasattr(metrics, "asdict") else metrics
        await self._execute_async(
            """
            INSERT OR REPLACE INTO circularity_records (
                record_id, circularity_index, circularity_level, recycling_rate,
                recovery_efficiency, collection_efficiency, purification_efficiency,
                data_quality_score, tx_hash, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["record_id"],
                data["circularity_index"],
                data["circularity_level"],
                data["recycling_rate"],
                data["recovery_efficiency"],
                data["collection_efficiency"],
                data["purification_efficiency"],
                data["data_quality_score"],
                data.get("blockchain_tx_hash") or "",
                data["timestamp"].isoformat() if hasattr(data["timestamp"], "isoformat") else data["timestamp"]
            )
        )

    async def clean_circularity_records_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM circularity_records WHERE timestamp < ?", (cutoff,))

    async def store_emission_record_async(self, record: Dict) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO emission_records (
                record_id, scope, amount_kg, source, location, verified,
                region, user_id, timestamp, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["record_id"],
                record["scope"],
                record["amount_kg"],
                record["source"],
                record["location"],
                1 if record.get("verified") else 0,
                record["region"],
                record["user_id"],
                record["timestamp"],
                json.dumps(record.get("metadata", {}))
            )
        )

    async def clean_emission_records_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM emission_records WHERE timestamp < ?", (cutoff,))

    async def save_optimisation_async(self, strategy: str, result: Dict) -> None:
        await self._execute_async(
            "INSERT INTO optimisation_history (strategy, result, timestamp) VALUES (?, ?, ?)",
            (strategy, json.dumps(result), datetime.now().isoformat())
        )

    async def get_recent_optimisations_async(self, limit: int = 10) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT strategy, result, timestamp FROM optimisation_history ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return [{"strategy": r["strategy"], "result": json.loads(r["result"]), "timestamp": r["timestamp"]} for r in rows]

    async def clean_optimisation_history_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM optimisation_history WHERE timestamp < ?", (cutoff,))

    async def save_distribution_async(self, result: Dict) -> None:
        await self._execute_async(
            """
            INSERT INTO distribution_history (optimal_provider, optimal_region, scores, data_size_gb, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                result["optimal_provider"],
                result["optimal_region"],
                json.dumps(result["scores"]),
                result.get("data_size_gb", 0),
                result["timestamp"]
            )
        )

    async def get_recent_distributions_async(self, limit: int = 10) -> List[Dict]:
        rows = await self._fetchall_async(
            """
            SELECT optimal_provider, optimal_region, scores, data_size_gb, timestamp
            FROM distribution_history ORDER BY id DESC LIMIT ?
            """,
            (limit,)
        )
        return [
            {
                "optimal_provider": r["optimal_provider"],
                "optimal_region": r["optimal_region"],
                "scores": json.loads(r["scores"]),
                "data_size_gb": r["data_size_gb"],
                "timestamp": r["timestamp"]
            }
            for r in rows
        ]

    async def store_thermal_optimization_async(self, result) -> None:
        data = asdict(result) if hasattr(result, "asdict") else result
        await self._execute_async(
            """
            INSERT OR REPLACE INTO thermal_optimizations (id, data, timestamp)
            VALUES (?, ?, ?)
            """,
            (
                data.get("id", f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
                json.dumps(data, default=str),
                datetime.now().isoformat()
            )
        )

    async def clean_thermal_records_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM thermal_optimizations WHERE timestamp < ?", (cutoff,))

    async def save_state_async(self, key: str, value: str) -> None:
        await self._execute_async(
            "INSERT OR REPLACE INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, datetime.now().isoformat())
        )

    async def get_state_async(self, key: str) -> Optional[str]:
        row = await self._fetchone_async("SELECT value FROM kv_store WHERE key = ?", (key,))
        return row["value"] if row else None

    async def delete_state_async(self, key: str) -> None:
        await self._execute_async("DELETE FROM kv_store WHERE key = ?", (key,))

    async def save_pqc_key_async(self, key_id: str, algorithm: str,
                                 public_key: bytes, private_key: bytes,
                                 expires_at: str) -> None:
        pub_cipher, pub_nonce = self._encrypt_blob(public_key)
        priv_cipher, priv_nonce = self._encrypt_blob(private_key)
        await self._execute_async(
            """
            INSERT OR REPLACE INTO pqc_keys
            (key_id, algorithm, public_key, public_nonce, private_key, private_nonce, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key_id,
                algorithm,
                pub_cipher,
                pub_nonce,
                priv_cipher,
                priv_nonce,
                datetime.now().isoformat(),
                expires_at
            )
        )

    async def get_pqc_key_async(self, key_id: str) -> Optional[Dict]:
        row = await self._fetchone_async(
            "SELECT * FROM pqc_keys WHERE key_id = ?",
            (key_id,)
        )
        if row:
            pub = self._decrypt_blob(row["public_key"], row["public_nonce"])
            priv = self._decrypt_blob(row["private_key"], row["private_nonce"])
            return {
                "key_id": row["key_id"],
                "algorithm": row["algorithm"],
                "public_key": pub,
                "private_key": priv,
                "created_at": row["created_at"],
                "expires_at": row["expires_at"]
            }
        return None

    async def list_pqc_keys_async(self) -> List[str]:
        rows = await self._fetchall_async("SELECT key_id FROM pqc_keys")
        return [r["key_id"] for r in rows]

    async def delete_pqc_key_async(self, key_id: str) -> None:
        await self._execute_async("DELETE FROM pqc_keys WHERE key_id = ?", (key_id,))

    # --------------------------------------------------------------------------
    # NEW v3.0.0 ASYNC WRAPPERS FOR GA, MoE, Pareto, etc.
    # --------------------------------------------------------------------------
    async def save_ga_population_async(self, generation: int, individuals: List[Dict[str, Any]]) -> None:
        for ind in individuals:
            await self._execute_async(
                """
                INSERT OR REPLACE INTO ga_populations (generation, individual_id, attributes, fitness, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    generation,
                    ind["individual_id"],
                    json.dumps(ind["attributes"]),
                    ind["fitness"],
                    datetime.now().isoformat()
                )
            )

    async def get_ga_population_async(self, generation: int) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT individual_id, attributes, fitness FROM ga_populations WHERE generation = ?",
            (generation,)
        )
        return [
            {
                "individual_id": r["individual_id"],
                "attributes": json.loads(r["attributes"]),
                "fitness": r["fitness"]
            }
            for r in rows
        ]

    async def get_ga_population_generations_async(self) -> List[int]:
        rows = await self._fetchall_async("SELECT DISTINCT generation FROM ga_populations ORDER BY generation")
        return [r["generation"] for r in rows]

    async def save_ga_fitness_history_async(self, generation: int, best_fitness: float,
                                            avg_fitness: float, diversity: float) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO ga_fitness_history (generation, best_fitness, avg_fitness, diversity, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (generation, best_fitness, avg_fitness, diversity, datetime.now().isoformat())
        )

    async def get_ga_fitness_history_async(self, limit: int = 100) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT * FROM ga_fitness_history ORDER BY generation DESC LIMIT ?",
            (limit,)
        )
        return rows

    async def clean_ga_populations_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM ga_populations WHERE timestamp < ?", (cutoff,))

    async def clean_ga_fitness_history_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM ga_fitness_history WHERE timestamp < ?", (cutoff,))

    async def save_moe_training_sample_async(self, sample_id: str, features: List[float],
                                             expert_label: int, reward: float) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO moe_gating_training (sample_id, features, expert_label, reward, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (sample_id, json.dumps(features), expert_label, reward, datetime.now().isoformat())
        )

    async def get_moe_training_samples_async(self, limit: int = 1000) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT * FROM moe_gating_training ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        for r in rows:
            r["features"] = json.loads(r["features"])
        return rows

    async def clean_moe_training_samples_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM moe_gating_training WHERE timestamp < ?", (cutoff,))

    async def save_moe_expert_metadata_async(self, expert_id: str, name: str,
                                              description: str, performance_score: float) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO moe_expert_metadata (expert_id, name, description, performance_score, last_updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            (expert_id, name, description, performance_score, datetime.now().isoformat())
        )

    async def get_moe_expert_metadata_async(self, expert_id: str) -> Optional[Dict]:
        row = await self._fetchone_async(
            "SELECT * FROM moe_expert_metadata WHERE expert_id = ?",
            (expert_id,)
        )
        return row

    async def list_moe_experts_async(self) -> List[Dict]:
        return await self._fetchall_async("SELECT * FROM moe_expert_metadata ORDER BY performance_score DESC")

    async def save_pareto_front_async(self, solutions: List[Dict]) -> None:
        await self._execute_async("UPDATE pareto_front SET is_current = 0")
        for sol in solutions:
            await self._execute_async(
                """
                INSERT OR REPLACE INTO pareto_front
                (solution_id, decision_attributes, accuracy, carbon, cost, robustness, is_current, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sol["solution_id"],
                    json.dumps(sol["decision_attributes"]),
                    sol["accuracy"],
                    sol["carbon"],
                    sol["cost"],
                    sol["robustness"],
                    1,
                    datetime.now().isoformat()
                )
            )

    async def get_current_pareto_front_async(self) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT * FROM pareto_front WHERE is_current = 1 ORDER BY accuracy DESC"
        )
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    async def get_pareto_front_history_async(self, limit: int = 10) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT * FROM pareto_front WHERE is_current = 0 ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        for r in rows:
            r["decision_attributes"] = json.loads(r["decision_attributes"])
        return rows

    async def clean_pareto_front_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM pareto_front WHERE timestamp < ? AND is_current = 0", (cutoff,))

    async def save_user_preference_async(self, user_id: str, weights: Dict[str, float],
                                          chosen_solution_id: Optional[str] = None) -> None:
        await self._execute_async(
            """
            INSERT INTO user_preferences (user_id, weights, chosen_solution_id, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, json.dumps(weights), chosen_solution_id, datetime.now().isoformat())
        )

    async def get_user_preferences_async(self, user_id: str, limit: int = 10) -> List[Dict]:
        rows = await self._fetchall_async(
            "SELECT weights, chosen_solution_id, timestamp FROM user_preferences WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
            (user_id, limit)
        )
        for r in rows:
            r["weights"] = json.loads(r["weights"])
        return rows

    async def get_latest_user_preference_async(self, user_id: str) -> Optional[Dict]:
        row = await self._fetchone_async(
            "SELECT weights, chosen_solution_id, timestamp FROM user_preferences WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1",
            (user_id,)
        )
        if row:
            row["weights"] = json.loads(row["weights"])
        return row

    async def clean_user_preferences_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM user_preferences WHERE timestamp < ?", (cutoff,))

    async def save_scenario_async(self, scenario_id: str, scenario: Dict[str, Any]) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO scenarios
            (scenario_id, carbon_price, discount_rate, demand_growth_rate,
             technology_cost_reduction, regulatory_risk, renewable_energy_share,
             energy_efficiency, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scenario_id,
                scenario.get("carbon_price", 50.0),
                scenario.get("discount_rate", 0.05),
                scenario.get("demand_growth_rate", 0.02),
                scenario.get("technology_cost_reduction", 0.1),
                scenario.get("regulatory_risk", 0.3),
                scenario.get("renewable_energy_share", 0.3),
                scenario.get("energy_efficiency", 0.7),
                datetime.now().isoformat()
            )
        )

    async def get_scenario_async(self, scenario_id: str) -> Optional[Dict]:
        row = await self._fetchone_async(
            "SELECT * FROM scenarios WHERE scenario_id = ?",
            (scenario_id,)
        )
        return row

    async def list_scenarios_async(self) -> List[Dict]:
        return await self._fetchall_async("SELECT * FROM scenarios ORDER BY timestamp DESC")

    async def clean_scenarios_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM scenarios WHERE timestamp < ?", (cutoff,))

    async def save_decision_option_async(self, option_id: str, name: str, attributes: Dict[str, Any]) -> None:
        await self._execute_async(
            """
            INSERT OR REPLACE INTO decision_catalogue (option_id, name, attributes, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (option_id, name, json.dumps(attributes), datetime.now().isoformat())
        )

    async def get_decision_option_async(self, option_id: str) -> Optional[Dict]:
        row = await self._fetchone_async(
            "SELECT option_id, name, attributes, timestamp FROM decision_catalogue WHERE option_id = ?",
            (option_id,)
        )
        if row:
            row["attributes"] = json.loads(row["attributes"])
        return row

    async def list_decision_options_async(self) -> List[Dict]:
        rows = await self._fetchall_async("SELECT option_id, name, attributes, timestamp FROM decision_catalogue ORDER BY timestamp DESC")
        for r in rows:
            r["attributes"] = json.loads(r["attributes"])
        return rows

    async def clean_decision_catalogue_async(self, days: int) -> None:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        await self._execute_async("DELETE FROM decision_catalogue WHERE timestamp < ?", (cutoff,))

    # --------------------------------------------------------------------------
    # Cleanup / close (optional)
    # --------------------------------------------------------------------------
    def close(self):
        """Close all connections in the thread-local pool."""
        if hasattr(self, '_local') and hasattr(self._local, 'conn'):
            self._local.conn.close()
            del self._local.conn

    async def close_async(self):
        """Close async connections if any (aiosqlite)."""
        if AIOSQLITE_AVAILABLE and self._async_conn:
            await self._async_conn.close()
            self._async_conn = None
