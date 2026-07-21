import os
import sqlite3
import pytest
from unittest.mock import MagicMock, patch
from cryptography.exceptions import InvalidTag

# Import components from src.enhancements
from src.enhancements import (
    Storage,
    AutonomousEnhancementsOptimizer,
    QuantumResilientEnhancementsSecurity,
)


# =====================================================================
# Fixtures
# =====================================================================

@pytest.fixture
def master_key():
    """Generates a valid 256-bit (32-byte) hex-encoded master key."""
    return os.urandom(32).hex()


@pytest.fixture
def set_env_master_key(monkeypatch, master_key):
    """Sets the master encryption key in environment variables."""
    monkeypatch.setenv("MASTER_ENCRYPTION_KEY", master_key)
    return master_key


@pytest.fixture
def temp_db_path(tmp_path):
    """Provides a temporary SQLite database file path."""
    return str(tmp_path / "test_enhancements.db")


@pytest.fixture
def storage(set_env_master_key, temp_db_path):
    """Initializes a Storage instance with a clean temporary DB and master key."""
    return Storage(db_path=temp_db_path)


@pytest.fixture
def optimizer(set_env_master_key, temp_db_path):
    """Initializes AutonomousEnhancementsOptimizer with storage backed by temp DB."""
    return AutonomousEnhancementsOptimizer(db_path=temp_db_path)


# =====================================================================
# Key Storage & Encryption Tests
# =====================================================================

class TestKeyStorageEncryption:
    """Unit tests for AES-256-GCM key storage encryption and security."""

    def test_encryption_decryption_roundtrip(self, storage):
        """Verifies plaintext keys can be encrypted and decrypted accurately."""
        secret_key = "sk_live_51NxExampleKey123456789"
        key_alias = "api_provider_key"

        # Encrypt & store
        storage.store_key(alias=key_alias, plaintext_key=secret_key)

        # Retrieve & decrypt
        decrypted_key = storage.get_key(alias=key_alias)
        assert decrypted_key == secret_key

    def test_unique_nonce_per_encryption(self, storage):
        """Ensures consecutive encryptions of the same key produce distinct ciphertexts (96-bit nonce)."""
        secret_key = "static_secret_value"
        
        storage.store_key(alias="key_v1", plaintext_key=secret_key)
        storage.store_key(alias="key_v2", plaintext_key=secret_key)

        raw_c1 = storage.get_raw_encrypted_entry("key_v1")
        raw_c2 = storage.get_raw_encrypted_entry("key_v2")

        # Nonce / ciphertext should differ even for identical plaintexts
        assert raw_c1["nonce"] != raw_c2["nonce"]
        assert raw_c1["ciphertext"] != raw_c2["ciphertext"]

    def test_tampered_ciphertext_rejection(self, storage, temp_db_path):
        """Ensures AES-GCM authentication fails if ciphertext or tag is tampered with."""
        key_alias = "sensitive_token"
        storage.store_key(alias=key_alias, plaintext_key="super_secret")

        # Directly tamper with ciphertext in SQLite
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT ciphertext FROM keys WHERE alias = ?", (key_alias,))
        original_ct = bytearray(cursor.fetchone()[0])
        
        # Flip a bit in the ciphertext
        original_ct[0] ^= 0xFF
        cursor.execute("UPDATE keys SET ciphertext = ? WHERE alias = ?", (bytes(original_ct), key_alias))
        conn.commit()
        conn.close()

        # Decryption must raise an authentication / tag failure exception
        with pytest.raises((ValueError, InvalidTag, Exception)):
            storage.get_key(alias=key_alias)

    def test_missing_master_key_raises_error(self, monkeypatch, temp_db_path):
        """Verifies Storage initialization fails if MASTER_ENCRYPTION_KEY is unset."""
        monkeypatch.delenv("MASTER_ENCRYPTION_KEY", raising=False)
        
        with pytest.raises(EnvironmentError, match="MASTER_ENCRYPTION_KEY"):
            Storage(db_path=temp_db_path)

    def test_sqlite_wal_mode_enabled(self, storage, temp_db_path):
        """Verifies SQLite storage initializes with Write-Ahead Logging (WAL) for concurrency."""
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode;")
        journal_mode = cursor.fetchone()[0]
        conn.close()

        assert journal_mode.lower() == "wal"


# =====================================================================
# AutonomousEnhancementsOptimizer Tests
# =====================================================================

class TestAutonomousEnhancementsOptimizer:
    """Unit tests for state management, optimization runs, and storage integration."""

    def test_optimizer_initialization(self, optimizer):
        """Verifies the optimizer correctly initializes internal components and storage."""
        assert optimizer.storage is not None
        assert isinstance(optimizer.security, QuantumResilientEnhancementsSecurity)

    def test_save_and_retrieve_optimization_state(self, optimizer):
        """Tests recording optimization metrics while keeping sensitive keys encrypted."""
        state_payload = {
            "iteration": 42,
            "best_score": 0.945,
            "hyperparameters": {"learning_rate": 0.001, "batch_size": 64},
        }
        
        optimizer.save_state(state=state_payload)
        retrieved_state = optimizer.load_latest_state()

        assert retrieved_state["iteration"] == 42
        assert retrieved_state["best_score"] == 0.945
        assert retrieved_state["hyperparameters"]["learning_rate"] == 0.001

    def test_optimizer_key_rotation_flow(self, optimizer, set_env_master_key, monkeypatch):
        """Tests re-encrypting stored keys when rotating the master key."""
        old_key = set_env_master_key
        new_key = os.urandom(32).hex()

        # Store key under old master key
        optimizer.storage.store_key("service_api", "secret_value_123")

        # Perform re-encryption/rotation
        optimizer.rotate_master_key(new_master_key=new_key)
        monkeypatch.setenv("MASTER_ENCRYPTION_KEY", new_key)

        # Ensure key is still correctly readable
        retrieved = optimizer.storage.get_key("service_api")
        assert retrieved == "secret_value_123"

    @pytest.mark.concurrent
    def test_concurrent_state_updates(self, optimizer):
        """Validates thread safety under concurrent optimization state logging."""
        import concurrent.futures

        def worker(thread_id):
            optimizer.record_step(
                step_id=thread_id,
                metrics={"loss": 1.0 / (thread_id + 1)}
            )
            return thread_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert len(results) == 10
        history = optimizer.get_history()
        assert len(history) == 10
