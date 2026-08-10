"""Central Storage module for enhancements.

This Storage class provides persistent SQLite storage and includes tables for
feedback_events, drift_states (with BLOB weights), benchmark_runs, and
model_weights (BLOB). Additions are implemented to avoid interfering with other
Storage classes in the repo — this file is intended as a canonical storage
helper that enhancement modules can import.

If you'd prefer integrating these changes into an existing Storage class in a
specific file instead of adding a new module, tell me which file and I'll
patch it directly.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Dict, List, Optional

# Try to reuse repo configuration if available
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
    """Persistent SQLite storage for enhancements.

    Notes:
    - drift_states.online_weights / offline_weights are stored as BLOBs.
    - model weights are stored in model_weights as BLOB.
    - resource_usage and metadata fields are JSON-encoded strings.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or DEFAULT_DB
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        # enable WAL for concurrency
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # model weights table (BLOB)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_weights (
                    model_id TEXT PRIMARY KEY,
                    weights BLOB,
                    timestamp REAL
                )
            """)

            # New tables requested: feedback_events, drift_states (BLOBs), benchmark_runs
            cursor.execute("""
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
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS drift_states (
                    snapshot_id TEXT PRIMARY KEY,
                    timestamp REAL NOT NULL,
                    online_weights BLOB,
                    offline_weights BLOB,
                    cost_score REAL,
                    reason TEXT
                );
            """)

            cursor.execute("""
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
                );
            """)

            # Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_feedback_task ON feedback_events(task_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_feedback_time ON feedback_events(timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_policy ON benchmark_runs(policy_name);")

            conn.commit()

    # ----- Model weight methods (BLOB) -----
    def save_model_weights(self, model_id: str, weights_bytes: bytes) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO model_weights (model_id, weights, timestamp) VALUES (?, ?, ?)",
                (model_id, weights_bytes, time.time()),
            )
            conn.commit()

    def load_model_weights(self, model_id: str) -> Optional[bytes]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT weights FROM model_weights WHERE model_id = ?",
                (model_id,),
            ).fetchone()
            return row[0] if row else None

    def store_power_reading(self, reading: Dict) -> None:
    with self._get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS power_readings (
                reading_id TEXT PRIMARY KEY,
                power_watts REAL,
                carbon_intensity REAL,
                timestamp TEXT,
                metadata TEXT
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO power_readings VALUES (?, ?, ?, ?, ?)",
            (reading['reading_id'], reading['power_watts'], reading['carbon_intensity'],
             reading['timestamp'], json.dumps(reading.get('metadata', {})))
        )
        conn.commit()

def clean_power_readings(self, days: int) -> None:
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with self._get_connection() as conn:
        conn.execute("DELETE FROM power_readings WHERE timestamp < ?", (cutoff,))
        conn.commit()

def save_pqc_key(self, key_id: str, algorithm: str, public_key: bytes, private_key: bytes, expires_at: str) -> None:
    # implement as in previous integrations

def store_substitution_result(self, result: SubstitutionResult) -> None:
    with self._get_connection() as conn:
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
        conn.execute(
            "INSERT OR REPLACE INTO substitution_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (result.calculation_id, result.base_material, result.recommended_substitute,
             result.topsis_score, result.carbon_reduction_pct, result.cost_savings_pct,
             result.sustainability_score, result.confidence_score, result.data_quality_score,
             result.blockchain_tx_hash or '', result.timestamp.isoformat())
        )
        conn.commit()

def clean_old_substitution_results(self, days: int) -> None:
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with self._get_connection() as conn:
        conn.execute("DELETE FROM substitution_results WHERE timestamp < ?", (cutoff,))
        conn.commit()

    # ----- Feedback Event Methods -----
    def store_feedback_event(self, event: Dict) -> None:
        """
        event: dict
          required: event_id, timestamp, task_id, selected_action, quality_score,
                    latency_ms, energy_joules, carbon_g, feedback_type, adaptive_cost_value
          optional: model_id, teacher_id, helium_cost, resource_usage (dict),
                    distillation_loss, metadata (dict)
        """
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO feedback_events (\
                    event_id, timestamp, task_id, model_id, teacher_id, selected_action,\
                    quality_score, latency_ms, energy_joules, carbon_g, helium_cost,\
                    resource_usage, distillation_loss, feedback_type, adaptive_cost_value, metadata\
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                ),
            )
            conn.commit()

    def save_pqc_key(self, key_id: str, algorithm: str, public_key: bytes, private_key: bytes, expires_at: str) -> None:
    # implement as in previous integrations

def store_circularity_record(self, metrics: HeliumCircularityMetrics) -> None:
    with self._get_connection() as conn:
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
        conn.execute(
            "INSERT OR REPLACE INTO circularity_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (metrics.record_id, metrics.circularity_index, metrics.circularity_level,
             metrics.recycling_rate, metrics.recovery_efficiency, metrics.collection_efficiency,
             metrics.purification_efficiency, metrics.data_quality_score,
             metrics.blockchain_tx_hash or '', metrics.timestamp.isoformat())
        )
        conn.commit()

def clean_old_circularity_records(self, days: int) -> None:
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with self._get_connection() as conn:
        conn.execute("DELETE FROM circularity_records WHERE timestamp < ?", (cutoff,))
        conn.commit()
        
    def store_emission_record(self, record: Dict) -> None:
    with self._get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emission_records (
                record_id TEXT PRIMARY KEY,
                scope TEXT, amount_kg REAL, source TEXT,
                location TEXT, verified INTEGER, region TEXT,
                user_id TEXT, timestamp TEXT, metadata TEXT
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO emission_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (record['record_id'], record['scope'], record['amount_kg'], record['source'],
             record['location'], 1 if record['verified'] else 0, record['region'],
             record['user_id'], record['timestamp'], json.dumps(record.get('metadata', {})))
        )
        conn.commit()

def clean_emission_records(self, days: int) -> None:
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with self._get_connection() as conn:
        conn.execute("DELETE FROM emission_records WHERE timestamp < ?", (cutoff,))
        conn.commit()
    def get_feedback_events(self, limit: int = 1000) -> List[Dict]:
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM feedback_events ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            results: List[Dict] = []
            for row in rows:
                r = dict(row)
                try:
                    r["resource_usage"] = json.loads(r.get("resource_usage") or "{}")
                except Exception:
                    r["resource_usage"] = r.get("resource_usage")
                try:
                    r["metadata"] = json.loads(r.get("metadata") or "{}")
                except Exception:
                    r["metadata"] = r.get("metadata")
                results.append(r)
            return results

    # ----- Drift Methods (store BLOBs) -----
    def save_drift_snapshot(self, snapshot_id: str, online_w: Optional[bytes], offline_w: Optional[bytes], cost: Optional[float], reason: Optional[str]) -> None:
        """
        online_w and offline_w should be bytes (will be stored as BLOB). Pass None if not available.
        """
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO drift_states (snapshot_id, timestamp, online_weights, offline_weights, cost_score, reason) VALUES (?, ?, ?, ?, ?, ?)",
                (snapshot_id, time.time(), online_w, offline_w, cost, reason),
            )
            conn.commit()

    def get_last_snapshot(self) -> Optional[Dict]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM drift_states ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            r = dict(row)
            # row['online_weights'] and 'offline_weights' are bytes or None
            r["online_weights"] = row["online_weights"]
            r["offline_weights"] = row["offline_weights"]
            return r

    # ----- Benchmark Methods -----
    def store_benchmark_result(self, run_id: str, policy: str, metrics: Dict[str, float], count: int) -> None:
        avg_quality = metrics.get("quality")
        avg_carbon = metrics.get("carbon")
        avg_latency = metrics.get("latency")
        avg_cost = metrics.get("cost")
        total_energy = metrics.get("energy")
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO benchmark_runs (run_id, timestamp, policy_name, avg_quality, avg_carbon, avg_latency, avg_cost, total_energy, sample_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, time.time(), policy, avg_quality, avg_carbon, avg_latency, avg_cost, total_energy, int(count)),
            )
            conn.commit()


# End of file
