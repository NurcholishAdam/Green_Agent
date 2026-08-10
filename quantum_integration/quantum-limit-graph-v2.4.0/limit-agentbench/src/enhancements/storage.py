"""
Central Storage module for Green Agent enhancements.

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

All methods are synchronous; async wrappers are provided via `asyncio.to_thread`.
Optional AES-GCM encryption for sensitive BLOB fields (e.g., PQC keys) is available
if a master key is provided via environment variable `STORAGE_MASTER_KEY`.
"""

from __future__ import annotations

import json
import sqlite3
import time
import os
import secrets
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import asdict

# Attempt to import cryptography for encryption
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

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
    - Connection pooling (thread-local).
    - WAL mode and foreign keys.
    - Schema versioning (table `schema_version`).
    - Optional AES-GCM encryption for sensitive BLOB fields.
    - Comprehensive error handling and logging.
    - Async methods for all sync operations.

    Note: This class is intended to be used as a singleton or shared across modules.
    """

    # Schema version – increment when tables change
    SCHEMA_VERSION = 2

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

        # Connection pool: thread-local storage
        self._local = threading.local() if 'threading' in globals() else None

        self._init_db()

    # --------------------------------------------------------------------------
    # Connection management
    # --------------------------------------------------------------------------
    def _get_connection(self) -> sqlite3.Connection:
        """Return a thread-local SQLite connection (pooled)."""
        if hasattr(self, '_local'):
            if not hasattr(self._local, 'conn'):
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA foreign_keys=ON;")
                conn.execute("PRAGMA busy_timeout=5000;")
                self._local.conn = conn
            return self._local.conn
        else:
            # Fallback (no threading)
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            return conn

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute SQL with error handling."""
        try:
            conn = self._get_connection()
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor
        except sqlite3.Error as e:
            print(f"Storage SQL error: {e} (sql: {sql}, params: {params})")
            raise

    def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        cursor = self._execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        cursor = self._execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    # --------------------------------------------------------------------------
    # Schema initialisation and versioning
    # --------------------------------------------------------------------------
    def _init_db(self) -> None:
        """Create tables and set schema version."""
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
                # Run migrations incrementally
                if current_version < 1:
                    self._create_tables_v1(conn)
                if current_version < 2:
                    self._create_tables_v2(conn)
                # Update version
                conn.execute("DELETE FROM schema_version")
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (self.SCHEMA_VERSION,))
                conn.commit()

    def _create_tables_v1(self, conn: sqlite3.Connection) -> None:
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

    def _create_tables_v2(self, conn: sqlite3.Connection) -> None:
        """Additional tables for v2."""
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
        # State table (key-value) – already created as kv_store in v1
        # But we'll also create a dedicated state table for clarity (or use kv_store)
        # We'll keep kv_store and add a view if needed; for simplicity we just use kv_store.

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
    # Power readings
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
    # Elasticity metrics
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
    # Substitution results
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
    # Federated rounds
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
    # Circularity records
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
    # Emission records
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
    # Optimisation history
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
    # Distribution history
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
    # Thermal optimizations
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
        # We need to ensure the table exists – create it if not.
        # But we'll create it in schema v3 if needed. For now, we'll create on demand.
        self._ensure_table_pqc()

    def _ensure_table_pqc(self) -> None:
        with self._get_connection() as conn:
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
            conn.commit()

    def get_pqc_key(self, key_id: str) -> Optional[Dict]:
        row = self._fetchone(
            "SELECT * FROM pqc_keys WHERE key_id = ?",
            (key_id,)
        )
        if row:
            # Decrypt
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

    # --------------------------------------------------------------------------
    # Async wrappers (for use in async modules)
    # --------------------------------------------------------------------------
    async def save_model_weights_async(self, model_id: str, weights_bytes: bytes) -> None:
        await asyncio.to_thread(self.save_model_weights, model_id, weights_bytes)

    async def load_model_weights_async(self, model_id: str) -> Optional[bytes]:
        return await asyncio.to_thread(self.load_model_weights, model_id)

    async def store_feedback_event_async(self, event: Dict) -> None:
        await asyncio.to_thread(self.store_feedback_event, event)

    async def get_feedback_events_async(self, limit: int = 1000) -> List[Dict]:
        return await asyncio.to_thread(self.get_feedback_events, limit)

    async def save_drift_snapshot_async(self, snapshot_id: str, online_w: Optional[bytes],
                                       offline_w: Optional[bytes], cost: Optional[float],
                                       reason: Optional[str]) -> None:
        await asyncio.to_thread(self.save_drift_snapshot, snapshot_id, online_w, offline_w, cost, reason)

    async def get_last_snapshot_async(self) -> Optional[Dict]:
        return await asyncio.to_thread(self.get_last_snapshot)

    async def store_benchmark_result_async(self, run_id: str, policy: str,
                                          metrics: Dict[str, float], count: int) -> None:
        await asyncio.to_thread(self.store_benchmark_result, run_id, policy, metrics, count)

    async def get_benchmark_results_async(self, days_back: int = 7) -> List[Dict]:
        return await asyncio.to_thread(self.get_benchmark_results, days_back)

    async def store_power_reading_async(self, reading: Dict) -> None:
        await asyncio.to_thread(self.store_power_reading, reading)

    async def clean_power_readings_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_power_readings, days)

    async def store_elasticity_metrics_async(self, metrics) -> None:
        await asyncio.to_thread(self.store_elasticity_metrics, metrics)

    async def clean_elasticity_records_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_elasticity_records, days)

    async def store_substitution_result_async(self, result) -> None:
        await asyncio.to_thread(self.store_substitution_result, result)

    async def clean_substitution_results_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_substitution_results, days)

    async def store_federated_round_async(self, result) -> None:
        await asyncio.to_thread(self.store_federated_round, result)

    async def clean_federated_rounds_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_federated_rounds, days)

    async def store_circularity_record_async(self, metrics) -> None:
        await asyncio.to_thread(self.store_circularity_record, metrics)

    async def clean_circularity_records_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_circularity_records, days)

    async def store_emission_record_async(self, record: Dict) -> None:
        await asyncio.to_thread(self.store_emission_record, record)

    async def clean_emission_records_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_emission_records, days)

    async def save_optimisation_async(self, strategy: str, result: Dict) -> None:
        await asyncio.to_thread(self.save_optimisation, strategy, result)

    async def get_recent_optimisations_async(self, limit: int = 10) -> List[Dict]:
        return await asyncio.to_thread(self.get_recent_optimisations, limit)

    async def clean_optimisation_history_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_optimisation_history, days)

    async def save_distribution_async(self, result: Dict) -> None:
        await asyncio.to_thread(self.save_distribution, result)

    async def get_recent_distributions_async(self, limit: int = 10) -> List[Dict]:
        return await asyncio.to_thread(self.get_recent_distributions, limit)

    async def store_thermal_optimization_async(self, result) -> None:
        await asyncio.to_thread(self.store_thermal_optimization, result)

    async def clean_thermal_records_async(self, days: int) -> None:
        await asyncio.to_thread(self.clean_thermal_records, days)

    async def save_state_async(self, key: str, value: str) -> None:
        await asyncio.to_thread(self.save_state, key, value)

    async def get_state_async(self, key: str) -> Optional[str]:
        return await asyncio.to_thread(self.get_state, key)

    async def save_pqc_key_async(self, key_id: str, algorithm: str,
                                 public_key: bytes, private_key: bytes,
                                 expires_at: str) -> None:
        await asyncio.to_thread(self.save_pqc_key, key_id, algorithm, public_key, private_key, expires_at)

    async def get_pqc_key_async(self, key_id: str) -> Optional[Dict]:
        return await asyncio.to_thread(self.get_pqc_key, key_id)

    async def list_pqc_keys_async(self) -> List[str]:
        return await asyncio.to_thread(self.list_pqc_keys)

    async def delete_pqc_key_async(self, key_id: str) -> None:
        await asyncio.to_thread(self.delete_pqc_key, key_id)

    # --------------------------------------------------------------------------
    # Cleanup / close (optional)
    # --------------------------------------------------------------------------
    def close(self):
        """Close all connections in the thread-local pool."""
        if hasattr(self, '_local') and hasattr(self._local, 'conn'):
            self._local.conn.close()
            del self._local.conn
