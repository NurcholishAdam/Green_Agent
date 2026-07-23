# anomaly_detection.py (or anomaly_detection/__init__.py)
"""
Anomaly Detection for Sustainability Metrics
=============================================

Uses Isolation Forest or autoencoders to detect anomalies in real‑time telemetry
(energy, carbon, helium usage, etc.). Raises alerts, triggers automated responses,
and provides anomaly scores to the EvolutionaryEngine for pruning.

Integrates with:
- TelemetryCollector (time‑series metrics)
- AlertEscalationSystem (operator notifications)
- EvolutionaryEngine (expert pruning)
"""

import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple, Union
import numpy as np

# ---------- Optional ML libraries ----------
try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
ANOMALY_CONFIG = {
    # Model type: "isolation_forest", "autoencoder", "threshold"
    "model_type": os.environ.get("ANOMALY_MODEL", "isolation_forest"),
    # Window size for time‑series (number of samples)
    "window_size": 100,
    # Contamination factor for Isolation Forest (expected anomaly fraction)
    "contamination": 0.05,
    # Autoencoder hidden dimensions
    "autoencoder_hidden": [16, 8, 16],
    # Alert thresholds (if using threshold model)
    "energy_spike_threshold": 2.0,  # multiple of rolling mean
    "carbon_spike_threshold": 2.0,
    # Alert cooldown (seconds between similar alerts)
    "alert_cooldown": 300,
    # Auto‑response triggers
    "auto_reroute_on_anomaly": True,
    "auto_restart_on_persistent": True,
    # Persistent anomaly count before restart
    "persistent_anomaly_threshold": 3,
    # How often to retrain models (seconds)
    "retrain_interval": 3600,
    # Feature names expected from TelemetryCollector
    "metrics_features": ["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"],
}


# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
@dataclass
class AnomalyEvent:
    """Represents a detected anomaly."""
    timestamp: datetime
    node_id: str
    metric_name: str
    metric_value: float
    anomaly_score: float  # -1 for normal, 1 for anomaly (Isolation Forest)
    description: str
    alert_sent: bool = False
    auto_response_taken: str = ""  # "reroute", "restart", "none"


class TelemetryBuffer:
    """Maintains a rolling window of telemetry data for each node."""
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.buffers: Dict[str, Dict[str, deque]] = {}  # node_id -> {metric: deque}

    def add_sample(self, node_id: str, metrics: Dict[str, float]) -> None:
        """Add a new sample for a node."""
        if node_id not in self.buffers:
            self.buffers[node_id] = {}
        for name, value in metrics.items():
            if name not in self.buffers[node_id]:
                self.buffers[node_id][name] = deque(maxlen=self.window_size)
            self.buffers[node_id][name].append(value)

    def get_data(self, node_id: str, metric_names: List[str]) -> np.ndarray:
        """
        Return a 2D array of shape (samples, features) for the given node.
        Samples are the most recent up to window_size.
        """
        if node_id not in self.buffers:
            return np.empty((0, len(metric_names)))
        data = []
        for name in metric_names:
            if name in self.buffers[node_id]:
                data.append(list(self.buffers[node_id][name]))
            else:
                data.append([])
        # Transpose to (samples, features)
        samples = np.array(data).T
        return samples

    def get_latest(self, node_id: str, metric_names: List[str]) -> np.ndarray:
        """Return the latest sample (1D array) for the node."""
        if node_id not in self.buffers:
            return np.zeros(len(metric_names))
        latest = []
        for name in metric_names:
            if name in self.buffers[node_id] and len(self.buffers[node_id][name]) > 0:
                latest.append(self.buffers[node_id][name][-1])
            else:
                latest.append(0.0)
        return np.array(latest)

    def has_enough_data(self, node_id: str, metric_names: List[str]) -> bool:
        """Check if we have at least some data for all metrics."""
        if node_id not in self.buffers:
            return False
        for name in metric_names:
            if name not in self.buffers[node_id] or len(self.buffers[node_id][name]) < 10:
                return False
        return True


# ============================================================================
# 3. ANOMALY DETECTION MODELS
# ============================================================================
class BaseAnomalyModel:
    """Abstract base for anomaly detection models."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_trained = False

    def train(self, data: np.ndarray) -> None:
        raise NotImplementedError

    def predict(self, data: np.ndarray) -> np.ndarray:
        """Return anomaly scores (1 for anomaly, -1 for normal) or probabilities."""
        raise NotImplementedError


class IsolationForestModel(BaseAnomalyModel):
    """Isolation Forest wrapper."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model = None
        self.contamination = config.get("contamination", 0.05)

    def train(self, data: np.ndarray) -> None:
        if data.shape[0] < 10:
            logger.warning("Not enough data to train Isolation Forest (need at least 10 samples).")
            self.is_trained = False
            return
        if not SKLEARN_AVAILABLE:
            logger.error("scikit-learn not installed; cannot use IsolationForest.")
            self.is_trained = False
            return
        self.model = IsolationForest(contamination=self.contamination, random_state=42)
        self.model.fit(data)
        self.is_trained = True

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)  # assume normal
        return self.model.predict(data)  # returns -1 for normal, 1 for anomaly


class AutoencoderModel(BaseAnomalyModel):
    """Simple autoencoder using PyTorch."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model = None
        self.input_dim = None
        self.hidden_dims = config.get("autoencoder_hidden", [16, 8, 16])
        self.reconstruction_threshold = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def _build_network(self, input_dim: int) -> nn.Module:
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch not installed; cannot use autoencoder.")
        layers = []
        dims = [input_dim] + self.hidden_dims
        for i in range(len(dims) - 1):
            layers.append(nn.Linear(dims[i], dims[i+1]))
            layers.append(nn.ReLU())
        # Decoder (symmetric)
        for i in range(len(dims) - 2, 0, -1):
            layers.append(nn.Linear(dims[i+1], dims[i]))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(dims[1], input_dim))
        return nn.Sequential(*layers)

    def train(self, data: np.ndarray) -> None:
        if data.shape[0] < 10:
            logger.warning("Not enough data to train autoencoder (need at least 10 samples).")
            self.is_trained = False
            return
        if not TORCH_AVAILABLE:
            logger.error("PyTorch not installed; cannot use autoencoder.")
            self.is_trained = False
            return

        self.input_dim = data.shape[1]
        self.model = self._build_network(self.input_dim).to(self.device)
        data_tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        criterion = nn.MSELoss()

        # Train for a few epochs
        self.model.train()
        for epoch in range(50):
            optimizer.zero_grad()
            reconstructed = self.model(data_tensor)
            loss = criterion(reconstructed, data_tensor)
            loss.backward()
            optimizer.step()

        # Compute reconstruction errors on training data to set threshold
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(data_tensor)
            errors = torch.mean((reconstructed - data_tensor) ** 2, dim=1).cpu().numpy()
        # Set threshold as 95th percentile
        self.reconstruction_threshold = np.percentile(errors, 95)
        self.is_trained = True

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)  # normal
        data_tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(data_tensor)
            errors = torch.mean((reconstructed - data_tensor) ** 2, dim=1).cpu().numpy()
        # Anomaly if reconstruction error > threshold
        return np.where(errors > self.reconstruction_threshold, 1, -1)


class ThresholdModel(BaseAnomalyModel):
    """Simple threshold‑based anomaly detection using rolling mean and std."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.threshold_multiplier = config.get("energy_spike_threshold", 2.0)
        self.means = {}
        self.stds = {}

    def train(self, data: np.ndarray) -> None:
        # Compute mean and std per feature from the window
        if data.shape[0] == 0:
            self.is_trained = False
            return
        self.means = np.mean(data, axis=0)
        self.stds = np.std(data, axis=0)
        self.stds[self.stds == 0] = 1e-6  # avoid division by zero
        self.is_trained = True

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained:
            return np.full(data.shape[0], -1)
        # Z‑score per feature
        z_scores = np.abs((data - self.means) / self.stds)
        # Anomaly if any feature exceeds threshold
        anomalies = np.any(z_scores > self.threshold_multiplier, axis=1)
        return np.where(anomalies, 1, -1)


# ============================================================================
# 4. ANOMALY DETECTOR ORCHESTRATOR
# ============================================================================
class AnomalyDetector:
    """
    Main anomaly detection engine. Maintains per‑node models, ingests telemetry,
    and raises events.
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or ANOMALY_CONFIG
        self.buffer = TelemetryBuffer(self.config["window_size"])
        self.models: Dict[str, BaseAnomalyModel] = {}  # node_id -> model
        self.last_training: Dict[str, float] = {}  # node_id -> timestamp
        self.anomaly_history: Dict[str, List[AnomalyEvent]] = {}
        self.alert_cooldown: Dict[str, float] = {}  # node_id -> last alert time
        self.persistent_anomaly_count: Dict[str, int] = {}

        # Model factory
        self.model_type = self.config["model_type"]
        if self.model_type == "isolation_forest":
            self.ModelClass = IsolationForestModel
        elif self.model_type == "autoencoder":
            self.ModelClass = AutoencoderModel
        else:
            self.ModelClass = ThresholdModel

        # External integration hooks (set by user)
        self.alert_callback: Optional[Callable[[AnomalyEvent], None]] = None
        self.auto_response_callback: Optional[Callable[[AnomalyEvent], None]] = None
        self.evolutionary_engine_callback: Optional[Callable[[str, float], None]] = None

    def register_alert_callback(self, callback: Callable[[AnomalyEvent], None]):
        self.alert_callback = callback

    def register_auto_response_callback(self, callback: Callable[[AnomalyEvent], None]):
        self.auto_response_callback = callback

    def register_evolutionary_engine_callback(self, callback: Callable[[str, float], None]):
        self.evolutionary_engine_callback = callback

    def _ensure_model(self, node_id: str) -> BaseAnomalyModel:
        """Create or retrieve a model for a node."""
        if node_id not in self.models:
            self.models[node_id] = self.ModelClass(self.config)
            self.last_training[node_id] = 0.0
            self.anomaly_history[node_id] = []
        return self.models[node_id]

    def _should_retrain(self, node_id: str) -> bool:
        """Check if enough time has passed since last training."""
        if node_id not in self.last_training:
            return True
        elapsed = time.time() - self.last_training[node_id]
        return elapsed > self.config["retrain_interval"]

    def _update_model(self, node_id: str, data: np.ndarray) -> None:
        """Train or retrain the model for a node if conditions met."""
        model = self._ensure_model(node_id)
        if self._should_retrain(node_id) and data.shape[0] >= 10:
            model.train(data)
            self.last_training[node_id] = time.time()

    def ingest(self, node_id: str, metrics: Dict[str, float]) -> Optional[AnomalyEvent]:
        """
        Process a new telemetry sample for a node.
        Returns an AnomalyEvent if an anomaly is detected, else None.
        """
        # Filter metrics to configured features
        features = self.config["metrics_features"]
        filtered_metrics = {k: v for k, v in metrics.items() if k in features}
        if len(filtered_metrics) < len(features):
            # Some features missing; we can still proceed with available ones
            pass

        # Update buffer
        self.buffer.add_sample(node_id, filtered_metrics)

        # Check if we have enough data
        if not self.buffer.has_enough_data(node_id, features):
            return None

        # Get data window
        data_window = self.buffer.get_data(node_id, features)
        if data_window.shape[0] < 10:
            return None

        # Update model
        self._update_model(node_id, data_window)

        # Get latest sample
        latest = self.buffer.get_latest(node_id, features)
        if latest.size == 0:
            return None

        # Predict on latest sample (need to reshape)
        latest_reshaped = latest.reshape(1, -1)
        model = self._ensure_model(node_id)
        if not model.is_trained:
            return None
        prediction = model.predict(latest_reshaped)[0]

        if prediction == 1:  # anomaly
            event = self._create_event(node_id, filtered_metrics, model)
            self._handle_anomaly(event)
            return event
        else:
            # Reset persistent count if normal
            if node_id in self.persistent_anomaly_count:
                self.persistent_anomaly_count[node_id] = 0
            return None

    def _create_event(self, node_id: str, metrics: Dict[str, float], model: BaseAnomalyModel) -> AnomalyEvent:
        """Create an AnomalyEvent object."""
        # Find which metric(s) likely caused anomaly (by z‑score)
        # For simplicity, pick the metric with highest deviation from its mean
        features = self.config["metrics_features"]
        # Get the model's internal stats if available (ThresholdModel has means/stds)
        if isinstance(model, ThresholdModel):
            means = model.means
            stds = model.stds
            if means is not None:
                metric_values = np.array([metrics.get(f, 0.0) for f in features])
                z_scores = np.abs((metric_values - means) / (stds + 1e-6))
                idx = np.argmax(z_scores)
                metric_name = features[idx]
                metric_value = metrics.get(metric_name, 0.0)
            else:
                metric_name = features[0]
                metric_value = metrics.get(metric_name, 0.0)
        else:
            # For other models, just pick first feature
            metric_name = features[0]
            metric_value = metrics.get(metric_name, 0.0)

        # Anomaly score: for Isolation Forest, -1 normal, 1 anomaly
        # Convert to 0-1 probability if possible (here we just use 0.9 as high)
        score = 0.9 if prediction == 1 else 0.1

        desc = f"Anomaly detected on {node_id}: {metric_name} = {metric_value:.4f} (above expected range)."
        return AnomalyEvent(
            timestamp=datetime.now(),
            node_id=node_id,
            metric_name=metric_name,
            metric_value=metric_value,
            anomaly_score=score,
            description=desc,
            alert_sent=False,
            auto_response_taken="none",
        )

    def _handle_anomaly(self, event: AnomalyEvent) -> None:
        """Process an anomaly: alert, auto‑response, evolutionary feedback."""
        node_id = event.node_id
        # Persistent count
        self.persistent_anomaly_count[node_id] = self.persistent_anomaly_count.get(node_id, 0) + 1

        # Send alert (respect cooldown)
        now = time.time()
        if node_id in self.alert_cooldown and (now - self.alert_cooldown[node_id]) < self.config["alert_cooldown"]:
            event.alert_sent = False
        else:
            event.alert_sent = True
            self.alert_cooldown[node_id] = now
            if self.alert_callback:
                self.alert_callback(event)
            else:
                logger.warning(f"ALERT: {event.description}")

        # Trigger auto‑response if configured
        if self.config["auto_reroute_on_anomaly"]:
            event.auto_response_taken = "reroute"
            if self.auto_response_callback:
                self.auto_response_callback(event)
            else:
                logger.info(f"AUTO‑REROUTE for {node_id} due to anomaly.")
        elif (self.config["auto_restart_on_persistent"] and
              self.persistent_anomaly_count[node_id] >= self.config["persistent_anomaly_threshold"]):
            event.auto_response_taken = "restart"
            if self.auto_response_callback:
                self.auto_response_callback(event)
            else:
                logger.info(f"AUTO‑RESTART for {node_id} due to persistent anomalies.")
            # Reset count after restart
            self.persistent_anomaly_count[node_id] = 0

        # Feed anomaly information to EvolutionaryEngine (pruning)
        if self.evolutionary_engine_callback:
            # Provide a negative score (anomaly severity) to encourage pruning
            severity = event.anomaly_score  # 0-1, we can use directly
            self.evolutionary_engine_callback(node_id, severity)
        else:
            logger.debug(f"EvolutionaryEngine feedback for {node_id}: severity={event.anomaly_score}")

        # Store in history
        if node_id not in self.anomaly_history:
            self.anomaly_history[node_id] = []
        self.anomaly_history[node_id].append(event)

        # Keep history limited
        if len(self.anomaly_history[node_id]) > 100:
            self.anomaly_history[node_id] = self.anomaly_history[node_id][-100:]


# ============================================================================
# 5. INTEGRATION WITH TELEMETRYCOLLECTOR (mock)
# ============================================================================
class TelemetryCollector:
    """Mock TelemetryCollector that emits metrics."""
    def __init__(self, anomaly_detector: AnomalyDetector):
        self.detector = anomaly_detector
        self.is_running = False

    def start(self):
        self.is_running = True
        logger.info("TelemetryCollector started.")

    def stop(self):
        self.is_running = False
        logger.info("TelemetryCollector stopped.")

    def receive_telemetry(self, node_id: str, metrics: Dict[str, float]):
        """Simulate receiving a telemetry sample."""
        if not self.is_running:
            logger.warning("TelemetryCollector not running; ignoring sample.")
            return
        event = self.detector.ingest(node_id, metrics)
        return event


# ============================================================================
# 6. INTEGRATION WITH ALERTESCALATIONSYSTEM (mock)
# ============================================================================
class AlertEscalationSystem:
    """Mock alert system that can escalate alerts to operators."""
    def __init__(self):
        self.alerts = []

    def send_alert(self, event: AnomalyEvent):
        self.alerts.append(event)
        logger.info(f"AlertEscalationSystem: {event.description}")
        # Here you would integrate with real notification services (Slack, email, etc.)


# ============================================================================
# 7. INTEGRATION WITH EVOLUTIONARYENGINE (mock)
# ============================================================================
class EvolutionaryEngine:
    """Mock evolutionary engine that prunes experts based on anomaly scores."""
    def __init__(self):
        self.pruned_experts = []

    def receive_anomaly_feedback(self, node_id: str, severity: float):
        """
        Receives anomaly severity for a node. Higher severity increases
        likelihood of pruning the expert.
        """
        # Simple logic: if severity > 0.7, prune
        if severity > 0.7:
            self.pruned_experts.append((node_id, severity))
            logger.info(f"EvolutionaryEngine pruned expert {node_id} due to anomaly severity {severity:.2f}")
        else:
            logger.debug(f"EvolutionaryEngine noted anomaly for {node_id} (severity {severity:.2f}) but not pruning.")


# ============================================================================
# 8. CONVENIENCE FACTORY
# ============================================================================
def create_anomaly_detection_system(config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Factory to create the entire anomaly detection pipeline with all integrations.
    """
    if config is None:
        config = ANOMALY_CONFIG

    detector = AnomalyDetector(config)
    telemetry_collector = TelemetryCollector(detector)
    alert_system = AlertEscalationSystem()
    evolutionary_engine = EvolutionaryEngine()

    # Wire callbacks
    detector.register_alert_callback(alert_system.send_alert)
    detector.register_evolutionary_engine_callback(evolutionary_engine.receive_anomaly_feedback)

    # Auto‑response callback (can be a separate module)
    def auto_response_callback(event: AnomalyEvent):
        logger.info(f"Auto‑response triggered: {event.auto_response_taken} on {event.node_id}")
        # Here you would call actual orchestration (e.g., reroute tasks, restart service)
    detector.register_auto_response_callback(auto_response_callback)

    return {
        "detector": detector,
        "telemetry_collector": telemetry_collector,
        "alert_system": alert_system,
        "evolutionary_engine": evolutionary_engine,
    }


# ============================================================================
# 9. EXAMPLE USAGE (if run directly)
# ============================================================================
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Create system
    system = create_anomaly_detection_system()
    detector = system["detector"]
    telemetry = system["telemetry_collector"]
    alert_system = system["alert_system"]
    engine = system["evolutionary_engine"]

    # Start telemetry
    telemetry.start()

    # Simulate some normal data
    for i in range(200):
        metrics = {
            "energy_joules": 10 + np.random.normal(0, 1),
            "carbon_kg": 0.5 + np.random.normal(0, 0.1),
            "helium_usage": 0.02 + np.random.normal(0, 0.005),
            "latency_ms": 50 + np.random.normal(0, 5),
            "accuracy": 0.95 + np.random.normal(0, 0.02),
        }
        telemetry.receive_telemetry("node-001", metrics)
        time.sleep(0.01)

    # Inject an anomaly (energy spike)
    anomaly_metrics = {
        "energy_joules": 100,
        "carbon_kg": 0.5,
        "helium_usage": 0.02,
        "latency_ms": 50,
        "accuracy": 0.95,
    }
    event = telemetry.receive_telemetry("node-001", anomaly_metrics)
    if event:
        logger.info(f"Detected anomaly: {event.description}")
        logger.info(f"Alerts sent: {len(alert_system.alerts)}")
        logger.info(f"EvolutionaryEngine pruned: {len(engine.pruned_experts)} experts")

    # Stop telemetry
    telemetry.stop()
