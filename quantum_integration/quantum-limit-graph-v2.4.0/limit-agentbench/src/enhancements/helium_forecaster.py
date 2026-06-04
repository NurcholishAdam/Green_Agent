# File: src/enhancements/helium_forecaster.py (ENHANCED VERSION v6.3)

"""
Helium Market Forecaster with Deep Learning - Version 6.3

ENHANCEMENTS:
- 11-dimensional feature vectors (added new_production_capacity)
- Capacity-aware forecasting
- Future supply potential predictions
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import pandas as pd
import logging
import time
import json
import uuid
import threading
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque, defaultdict
import warnings
import math
import hashlib

# Deep learning imports
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Scikit-learn imports
try:
    from sklearn.preprocessing import StandardScaler, RobustScaler
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.ensemble import GradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Import base classes
try:
    from .base_classes import BaseMetrics, GreenAgentConfig, load_module_config
except ImportError:
    from base_classes import BaseMetrics, GreenAgentConfig, load_module_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.FileHandler('helium_forecaster_v6.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.correlation_id = str(uuid.uuid4())[:8]
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logger.addFilter(CorrelationIdFilter())

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
REGISTRY = CollectorRegistry()
FORECAST_GENERATIONS = Counter('helium_forecast_generations_total', 'Total forecasts generated', ['status'], registry=REGISTRY)
FORECAST_DURATION = Histogram('helium_forecast_duration_seconds', 'Forecast generation time', ['model'], registry=REGISTRY)
MODEL_ACCURACY = Gauge('helium_forecaster_model_accuracy', 'Model accuracy metrics', ['model', 'metric'], registry=REGISTRY)
PREDICTION_CONFIDENCE = Gauge('helium_forecaster_confidence', 'Prediction confidence score', ['horizon'], registry=REGISTRY)
INTEGRATION_STATUS = Gauge('helium_forecaster_integration_status', 'Integration status', ['module'], registry=REGISTRY)
FORECAST_HORIZON = Gauge('helium_forecast_horizon_value', 'Forecast horizon values', ['horizon', 'type'], registry=REGISTRY)
MODEL_VERSION = Gauge('helium_forecaster_model_version', 'Current model version', ['model_type'], registry=REGISTRY)
DATA_FRESHNESS = Gauge('helium_forecaster_data_freshness', 'Training data freshness (hours)', registry=REGISTRY)
CAPACITY_FORECAST = Gauge('helium_capacity_forecast', 'New capacity forecast', ['horizon'], registry=REGISTRY)

# Try to import helium data collector
try:
    from .helium_data_collector import get_helium_collector
    HELIUM_COLLECTOR_AVAILABLE = True
except ImportError:
    try:
        from helium_data_collector import get_helium_collector
        HELIUM_COLLECTOR_AVAILABLE = True
    except ImportError:
        HELIUM_COLLECTOR_AVAILABLE = False

# Try to import blockchain verifier
try:
    from .blockchain_helium_verification import HeliumProvenanceTracker
    BLOCKCHAIN_AVAILABLE = True
except ImportError:
    try:
        from blockchain_helium_verification import HeliumProvenanceTracker
        BLOCKCHAIN_AVAILABLE = True
    except ImportError:
        BLOCKCHAIN_AVAILABLE = False

# ============================================================
# ENHANCED NEURAL NETWORK MODELS
# ============================================================

class PositionalEncoding(nn.Module):
    """Sinusoidal positional encoding for Transformer"""
    def __init__(self, d_model: int, max_len: int = 500):
        super().__init__()
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)
        self.register_buffer('pe', pe)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + self.pe[:, :x.size(1), :]

class HeliumLSTMForecaster(nn.Module):
    """LSTM-based helium market forecaster with attention and MC Dropout"""
    
    def __init__(self, input_dim: int = 11, hidden_dim: int = 256, 
                 n_layers: int = 3, output_horizon: int = 12, dropout: float = 0.2):
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_horizon = output_horizon
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        self.lstm_layers = nn.ModuleList([
            nn.LSTM(hidden_dim if i > 0 else hidden_dim, hidden_dim, batch_first=True,
                   dropout=dropout if i < n_layers - 1 else 0)
            for i in range(n_layers)
        ])
        self.attention = nn.MultiheadAttention(hidden_dim, num_heads=8, dropout=dropout, batch_first=True)
        self.output_net = nn.Sequential(
            nn.Linear(hidden_dim, 128), nn.ReLU(), nn.Dropout(dropout),
            nn.Linear(128, 64), nn.ReLU(), nn.Dropout(dropout),
            nn.Linear(64, output_horizon)
        )
        self.capacity_output = nn.Sequential(
            nn.Linear(hidden_dim, 64), nn.ReLU(),
            nn.Linear(64, 32), nn.ReLU(),
            nn.Linear(32, output_horizon)
        )
        self.uncertainty_net = nn.Sequential(
            nn.Linear(hidden_dim, 64), nn.ReLU(), nn.Linear(64, output_horizon), nn.Softplus()
        )
        self.layer_norms = nn.ModuleList([nn.LayerNorm(hidden_dim) for _ in range(n_layers)])
        self.dropout = nn.Dropout(dropout)
        self.mc_dropout = True
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        x = self.input_proj(x)
        for i, (lstm, norm) in enumerate(zip(self.lstm_layers, self.layer_norms)):
            residual = x
            x, _ = lstm(x)
            x = norm(x + residual)
            x = self.dropout(x)
        attended, _ = self.attention(x, x, x)
        x = x + attended
        pool_len = max(1, x.size(1) // 10)
        context = x[:, -pool_len:, :].mean(dim=1)
        if self.mc_dropout and self.training:
            forecasts = [self.output_net(self.dropout(context)) for _ in range(10)]
            forecast = torch.stack(forecasts).mean(dim=0)
            capacity_forecasts = [self.capacity_output(self.dropout(context)) for _ in range(10)]
            capacity_forecast = torch.stack(capacity_forecasts).mean(dim=0)
        else:
            forecast = self.output_net(context)
            capacity_forecast = self.capacity_output(context)
        uncertainty = self.uncertainty_net(context)
        return forecast, capacity_forecast, uncertainty
    
    def predict_with_intervals(self, x: torch.Tensor, confidence: float = 0.95) -> Dict:
        """Predict with Monte Carlo Dropout for uncertainty quantification"""
        self.eval()
        self.mc_dropout = True
        forecasts, capacity_forecasts, uncertainties = [], [], []
        with torch.no_grad():
            for _ in range(50):
                fc, cap_fc, unc = self.forward(x)
                forecasts.append(fc.cpu().numpy())
                capacity_forecasts.append(cap_fc.cpu().numpy())
                uncertainties.append(unc.cpu().numpy())
        forecasts = np.array(forecasts)
        capacity_forecasts = np.array(capacity_forecasts)
        uncertainties = np.array(uncertainties)
        mean_fc = forecasts.mean(axis=0)
        std_fc = forecasts.std(axis=0)
        mean_cap_fc = capacity_forecasts.mean(axis=0)
        std_cap_fc = capacity_forecasts.std(axis=0)
        z = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}.get(confidence, 1.96)
        self.mc_dropout = False
        return {
            'price_forecast': mean_fc,
            'capacity_forecast': mean_cap_fc,
            'price_lower_bound': mean_fc - z * std_fc,
            'price_upper_bound': mean_fc + z * std_fc,
            'capacity_lower_bound': mean_cap_fc - z * std_cap_fc,
            'capacity_upper_bound': mean_cap_fc + z * std_cap_fc,
            'price_uncertainty': std_fc,
            'capacity_uncertainty': std_cap_fc,
            'aleatoric_uncertainty': uncertainties.mean(axis=0),
            'epistemic_uncertainty': std_fc
        }

class HeliumTransformerForecaster(nn.Module):
    """Transformer-based helium market forecaster with capacity prediction"""
    
    def __init__(self, input_dim: int = 11, d_model: int = 256, 
                 n_heads: int = 8, n_layers: int = 4, output_horizon: int = 12):
        super().__init__()
        self.d_model = d_model
        self.pos_encoder = PositionalEncoding(d_model)
        self.input_embedding = nn.Linear(input_dim, d_model)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=512,
            dropout=0.1, activation='gelu', batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        self.price_proj = nn.Sequential(nn.Linear(d_model, 128), nn.GELU(), nn.Linear(128, output_horizon))
        self.capacity_proj = nn.Sequential(nn.Linear(d_model, 64), nn.GELU(), nn.Linear(64, output_horizon))
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        x = self.input_embedding(x) * math.sqrt(self.d_model)
        x = self.pos_encoder(x)
        x = self.transformer(x)
        context = x.mean(dim=1)
        return self.price_proj(context), self.capacity_proj(context)

# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class ForecastResult(BaseMetrics):
    """Helium market forecast result with capacity predictions"""
    source_module: str = "helium_forecaster"
    horizon_months: int = 12
    forecast_horizons: List[int] = field(default_factory=lambda: [1, 3, 6, 12])
    price_forecast: List[float] = field(default_factory=list)
    capacity_forecast: List[float] = field(default_factory=list)  # NEW
    scarcity_forecast: List[float] = field(default_factory=list)
    production_forecast: List[float] = field(default_factory=list)
    demand_forecast: List[float] = field(default_factory=list)
    price_confidence_intervals: Dict[str, List[float]] = field(default_factory=dict)
    capacity_confidence_intervals: Dict[str, List[float]] = field(default_factory=dict)  # NEW
    forecast_uncertainty: List[float] = field(default_factory=list)
    model_name: str = "lstm_transformer_ensemble"
    training_loss: float = 0.0
    validation_mae: float = 0.0
    r2_score: float = 0.0
    best_case_scenario: Dict = field(default_factory=dict)
    worst_case_scenario: Dict = field(default_factory=dict)
    market_outlook: str = "stable"
    price_trend: str = "stable"
    risk_level: str = "moderate"
    recommended_actions: List[str] = field(default_factory=list)
    blockchain_verified: bool = False
    blockchain_transaction_hash: str = ""
    forecast_confidence: float = 0.0
    
    def to_dict(self) -> Dict:
        return asdict(self)

# ============================================================
# MAIN HELIUM FORECASTER
# ============================================================

class HeliumForecaster:
    """
    ENHANCED Helium Market Forecaster v6.3
    
    Complete forecasting with:
    - 11-dimensional feature vectors (includes new production capacity)
    - LSTM + Transformer ensemble
    - Capacity forecasting
    - Monte Carlo Dropout uncertainty
    - HeliumDataCollector integration
    - Blockchain verification
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or (load_module_config('helium') if load_module_config else {})
        
        # Initialize models
        self.lstm_model = None
        self.transformer_model = None
        self.gradient_boosting_model = None
        
        # Scalers
        self.feature_scaler = RobustScaler() if SKLEARN_AVAILABLE else None
        self.target_scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        
        # Model parameters (updated for 11 features)
        self.input_dim = 11  # Was 10, now includes new_production_capacity
        self.seq_length = 60
        self.output_horizon = 12
        
        # Training history
        self.training_history: List[Dict] = []
        self.forecast_history: List[ForecastResult] = []
        self.model_version = 1
        
        # Integration modules
        self.collector = None
        self.blockchain_verifier = None
        self._init_integrations()
        
        # Performance tracking
        self.performance_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Initialize models if PyTorch available
        if TORCH_AVAILABLE:
            self.lstm_model = HeliumLSTMForecaster(
                input_dim=self.input_dim, 
                output_horizon=self.output_horizon
            )
            self.transformer_model = HeliumTransformerForecaster(
                input_dim=self.input_dim, 
                output_horizon=self.output_horizon
            )
        
        if SKLEARN_AVAILABLE:
            self.gradient_boosting_model = GradientBoostingRegressor(
                n_estimators=200, learning_rate=0.05, max_depth=5, random_state=42
            )
        
        self.models_trained = False
        
        # Update metrics
        self._update_integration_metrics()
        
        logger.info(f"HeliumForecaster v6.3 initialized with LSTM={self.lstm_model is not None}, "
                   f"Transformer={self.transformer_model is not None}, "
                   f"input_dim={self.input_dim}, Collector={self.collector is not None}")
    
    def _init_integrations(self):
        """Initialize integrations"""
        if HELIUM_COLLECTOR_AVAILABLE:
            try:
                self.collector = get_helium_collector()
                logger.info("✅ HeliumDataCollector integrated")
            except Exception as e:
                logger.warning(f"HeliumDataCollector init failed: {e}")
        
        if BLOCKCHAIN_AVAILABLE:
            try:
                self.blockchain_verifier = HeliumProvenanceTracker()
                logger.info("✅ Blockchain verifier integrated")
            except Exception as e:
                logger.warning(f"Blockchain verifier init failed: {e}")
    
    def _update_integration_metrics(self):
        """Update integration status metrics"""
        integrations = {
            'helium_collector': self.collector is not None,
            'blockchain': self.blockchain_verifier is not None,
            'pytorch': TORCH_AVAILABLE,
            'sklearn': SKLEARN_AVAILABLE
        }
        for module, status in integrations.items():
            INTEGRATION_STATUS.labels(module=module).set(1 if status else 0)
    
    def _count_active_integrations(self) -> int:
        """Count active integrations"""
        return sum([self.collector is not None, self.blockchain_verifier is not None, 
                   TORCH_AVAILABLE, SKLEARN_AVAILABLE])
    
    def get_active_integrations(self) -> List[str]:
        """Get list of active integrations"""
        integrations = []
        if self.collector:
            integrations.append('helium_collector')
        if self.blockchain_verifier:
            integrations.append('blockchain')
        if TORCH_AVAILABLE:
            integrations.append('pytorch')
        if SKLEARN_AVAILABLE:
            integrations.append('sklearn')
        return integrations
    
    def fetch_training_data(self) -> Optional[np.ndarray]:
        """Automatically fetch training data from helium collector"""
        if not self.collector:
            return None
        
        try:
            feature_matrix = self.collector.get_feature_matrix()
            if len(feature_matrix) > 0:
                DATA_FRESHNESS.set(0)
                logger.info(f"Fetched {len(feature_matrix)} training samples from collector")
                return feature_matrix
        except Exception as e:
            logger.warning(f"Data fetch failed: {e}")
        
        DATA_FRESHNESS.set(999)
        return None
    
    def prepare_data(self, historical_data: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Prepare data for training (price and capacity targets)"""
        if SKLEARN_AVAILABLE and self.feature_scaler:
            historical_data = self.feature_scaler.fit_transform(historical_data)
        
        X, y_price, y_capacity = [], [], []
        for i in range(len(historical_data) - self.seq_length - self.output_horizon + 1):
            X.append(historical_data[i:i + self.seq_length])
            y_price.append(historical_data[i + self.seq_length:i + self.seq_length + self.output_horizon, 2])  # price_index is at index 2
            y_capacity.append(historical_data[i + self.seq_length:i + self.seq_length + self.output_horizon, 10])  # capacity is at index 10
        return np.array(X), np.array(y_price), np.array(y_capacity)
    
    def _train_model(self, model, X_train, y_train_price, y_train_capacity, 
                     X_val, y_val_price, y_val_capacity, epochs, model_name, early_stopping):
        """Train individual model with dual outputs (price and capacity)"""
        if not TORCH_AVAILABLE:
            return {'error': 'PyTorch not available'}
        
        optimizer = optim.Adam(model.parameters(), lr=0.001)
        scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=10, factor=0.5)
        criterion_price = nn.HuberLoss(delta=1.0)
        criterion_capacity = nn.MSELoss()
        
        best_val_loss = float('inf')
        patience_counter = 0
        train_losses, val_losses = [], []
        
        for epoch in range(epochs):
            model.train()
            optimizer.zero_grad()
            forecast_price, forecast_capacity = model(X_train)
            loss_price = criterion_price(forecast_price, y_train_price)
            loss_capacity = criterion_capacity(forecast_capacity, y_train_capacity)
            loss = loss_price + 0.3 * loss_capacity  # Weight capacity loss lower
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            train_losses.append(loss.item())
            
            model.eval()
            with torch.no_grad():
                val_price, val_capacity = model(X_val)
                val_loss_price = criterion_price(val_price, y_val_price)
                val_loss_capacity = criterion_capacity(val_capacity, y_val_capacity)
                val_loss = val_loss_price + 0.3 * val_loss_capacity
                val_losses.append(val_loss.item())
            
            scheduler.step(val_loss)
            
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
            else:
                patience_counter += 1
            
            if early_stopping and patience_counter >= 20:
                break
            
            if (epoch + 1) % 10 == 0:
                logger.debug(f"{model_name} Epoch {epoch+1}: loss={loss.item():.4f}, val_loss={val_loss.item():.4f}")
        
        return {
            'final_train_loss': train_losses[-1],
            'final_val_loss': val_losses[-1],
            'best_val_loss': best_val_loss,
            'epochs_completed': len(train_losses)
        }
    
    def train(self, historical_data: np.ndarray = None, epochs: int = 100,
             validation_split: float = 0.2, early_stopping: bool = True) -> Dict:
        """Train all forecasting models"""
        if not TORCH_AVAILABLE:
            return {'error': 'PyTorch required for training'}
        
        if historical_data is None:
            historical_data = self.fetch_training_data()
            if historical_data is None:
                return {'error': 'No training data available'}
        
        # Validate input dimension
        if historical_data.shape[1] != self.input_dim:
            logger.warning(f"Expected {self.input_dim} features, got {historical_data.shape[1]}. Adjusting...")
            self.input_dim = historical_data.shape[1]
        
        logger.info(f"Training forecaster on {len(historical_data)} data points with {self.input_dim} features...")
        
        X, y_price, y_capacity = self.prepare_data(historical_data)
        split_idx = int(len(X) * (1 - validation_split))
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_price_train, y_price_val = y_price[:split_idx], y_price[split_idx:]
        y_capacity_train, y_capacity_val = y_capacity[:split_idx], y_capacity[split_idx:]
        
        X_train_t = torch.FloatTensor(X_train)
        y_price_train_t = torch.FloatTensor(y_price_train)
        y_capacity_train_t = torch.FloatTensor(y_capacity_train)
        X_val_t = torch.FloatTensor(X_val)
        y_price_val_t = torch.FloatTensor(y_price_val)
        y_capacity_val_t = torch.FloatTensor(y_capacity_val)
        
        lstm_results = self._train_model(
            self.lstm_model, X_train_t, y_price_train_t, y_capacity_train_t,
            X_val_t, y_price_val_t, y_capacity_val_t, epochs, "LSTM", early_stopping
        )
        transformer_results = self._train_model(
            self.transformer_model, X_train_t, y_price_train_t, y_capacity_train_t,
            X_val_t, y_price_val_t, y_capacity_val_t, epochs, "Transformer", early_stopping
        )
        
        # Train GBM on flattened data
        gbm_score = None
        if self.gradient_boosting_model:
            X_flat = X.reshape(X.shape[0], -1)
            y_flat = y_price[:, 0]
            split_idx = int(len(X_flat) * (1 - validation_split))
            self.gradient_boosting_model.fit(X_flat[:split_idx], y_flat[:split_idx])
            gbm_score = self.gradient_boosting_model.score(X_flat[split_idx:], y_flat[split_idx:])
        
        self.models_trained = True
        self.model_version += 1
        
        training_result = {
            'lstm': lstm_results,
            'transformer': transformer_results,
            'gbm_score': gbm_score,
            'models_trained': True
        }
        self.training_history.append(training_result)
        
        MODEL_VERSION.labels(model_type='lstm').set(self.model_version)
        if lstm_results:
            MODEL_ACCURACY.labels(model='lstm', metric='val_loss').set(lstm_results.get('final_val_loss', 0))
        
        logger.info(f"Training complete: LSTM loss={lstm_results.get('final_val_loss', 0):.4f}, "
                   f"Transformer loss={transformer_results.get('final_val_loss', 0):.4f}")
        
        return training_result
    
    def forecast(self, recent_data: np.ndarray = None, horizon_months: int = 12) -> ForecastResult:
        """Generate helium market forecast with capacity predictions"""
        start_time = time.time()
        
        if recent_data is None:
            fetched = self.fetch_training_data()
            if fetched is not None and len(fetched) >= self.seq_length:
                recent_data = fetched[-self.seq_length:]
        
        if not self.models_trained or recent_data is None:
            logger.warning("Models not trained or no data. Returning baseline forecast.")
            result = self._baseline_forecast(recent_data, horizon_months)
            FORECAST_GENERATIONS.labels(status='baseline').inc()
            return result
        
        if SKLEARN_AVAILABLE and self.feature_scaler:
            recent_data = self.feature_scaler.transform(recent_data)
        
        X = torch.FloatTensor(recent_data[-self.seq_length:]).unsqueeze(0)
        
        lstm_result = None
        transformer_price = None
        transformer_capacity = None
        
        if self.lstm_model:
            with FORECAST_DURATION.labels(model='lstm').time():
                self.lstm_model.eval()
                lstm_result = self.lstm_model.predict_with_intervals(X)
                lstm_price = lstm_result['price_forecast'][0]
                lstm_capacity = lstm_result['capacity_forecast'][0]
        
        if self.transformer_model:
            with FORECAST_DURATION.labels(model='transformer').time():
                self.transformer_model.eval()
                with torch.no_grad():
                    transformer_price, transformer_capacity = self.transformer_model(X)
                    transformer_price = transformer_price.cpu().numpy()[0]
                    transformer_capacity = transformer_capacity.cpu().numpy()[0]
        
        # Ensemble average
        if lstm_price is not None and transformer_price is not None:
            ensemble_price = (lstm_price + transformer_price) / 2
            ensemble_capacity = (lstm_capacity + transformer_capacity) / 2
        elif lstm_price is not None:
            ensemble_price = lstm_price
            ensemble_capacity = lstm_capacity
        elif transformer_price is not None:
            ensemble_price = transformer_price
            ensemble_capacity = transformer_capacity
        else:
            return self._baseline_forecast(recent_data, horizon_months)
        
        # Calculate confidence
        if lstm_result is not None:
            uncertainty = lstm_result['price_uncertainty'][0]
            avg_uncertainty = float(np.mean(uncertainty))
            confidence = 1.0 / (1.0 + avg_uncertainty)
        else:
            confidence = 0.7
        
        # Generate confidence intervals
        confidence_intervals = {
            'price': {
                'lower': lstm_result['price_lower_bound'][0].tolist() if lstm_result else [p * 0.85 for p in ensemble_price],
                'upper': lstm_result['price_upper_bound'][0].tolist() if lstm_result else [p * 1.15 for p in ensemble_price]
            },
            'capacity': {
                'lower': lstm_result['capacity_lower_bound'][0].tolist() if lstm_result else [c * 0.85 for c in ensemble_capacity],
                'upper': lstm_result['capacity_upper_bound'][0].tolist() if lstm_result else [c * 1.15 for c in ensemble_capacity]
            }
        }
        
        # Update capacity forecast metric
        for i, h in enumerate([1, 3, 6, 12]):
            if i < len(ensemble_capacity):
                CAPACITY_FORECAST.labels(horizon=f'{h}m').set(ensemble_capacity[i])
        
        result = ForecastResult(
            horizon_months=horizon_months,
            price_forecast=ensemble_price.tolist(),
            capacity_forecast=ensemble_capacity.tolist(),
            scarcity_forecast=[min(1.0, p / 200) for p in ensemble_price],
            production_forecast=[28500 * (1 + 0.005 * i) for i in range(horizon_months)],
            demand_forecast=[29500 * (1 - 0.3 * (p - ensemble_price[0]) / max(ensemble_price[0], 1)) for p in ensemble_price],
            price_confidence_intervals=confidence_intervals['price'],
            capacity_confidence_intervals=confidence_intervals['capacity'],
            forecast_uncertainty=lstm_result['price_uncertainty'][0].tolist() if lstm_result is not None else [],
            model_name="lstm_transformer_ensemble",
            price_trend=self._determine_trend(ensemble_price),
            market_outlook=self._determine_outlook(ensemble_price),
            risk_level=self._assess_risk(ensemble_price),
            recommended_actions=self._generate_recommendations(ensemble_price, ensemble_capacity),
            forecast_confidence=confidence
        )
        
        # Blockchain verification
        if self.blockchain_verifier:
            blockchain_result = self._verify_forecast_on_blockchain(result)
            result.blockchain_verified = blockchain_result['verified']
            result.blockchain_transaction_hash = blockchain_result.get('transaction_hash', '')
        
        self.forecast_history.append(result)
        
        elapsed = time.time() - start_time
        FORECAST_GENERATIONS.labels(status='success').inc()
        PREDICTION_CONFIDENCE.labels(horizon='1m').set(confidence)
        for i, h in enumerate([1, 3, 6, 12]):
            if i < len(ensemble_price):
                FORECAST_HORIZON.labels(horizon=f'{h}m', type='price').set(ensemble_price[i])
        
        self.performance_metrics['forecast_times'].append(elapsed)
        self.performance_metrics['confidence_scores'].append(confidence)
        
        logger.info(f"Forecast generated: price_trend={result.price_trend}, risk={result.risk_level}, "
                   f"confidence={confidence:.3f}, capacity_2030={ensemble_capacity[11] if len(ensemble_capacity) > 11 else 0:.0f}, "
                   f"time={elapsed:.2f}s")
        
        return result
    
    def _verify_forecast_on_blockchain(self, forecast: ForecastResult) -> Dict:
        """Record forecast on blockchain for provenance"""
        result = {'verified': False, 'transaction_hash': '', 'method': 'none'}
        
        if not self.blockchain_verifier:
            result['method'] = 'blockchain_unavailable'
            return result
        
        try:
            forecast_hash = hashlib.sha256(
                json.dumps(forecast.to_dict(), sort_keys=True, default=str).encode()
            ).hexdigest()
            
            record = self.blockchain_verifier.register_helium_batch(
                source=f"forecast_{forecast.calculation_id}",
                volume_liters=forecast.price_forecast[0] if forecast.price_forecast else 100,
                purity=0.99,
                certification_level='verified'
            )
            if record:
                result['verified'] = True
                result['transaction_hash'] = getattr(record, 'transaction_hash', 'local_' + forecast_hash[:16])
                result['method'] = 'blockchain_onchain'
                BLOCKCHAIN_AUDIT.labels(type='forecast').inc()
        except Exception as e:
            logger.warning(f"Blockchain verification failed: {e}")
            result['method'] = 'failed'
        
        return result
    
    def _baseline_forecast(self, recent_data, horizon):
        """Generate baseline forecast when models unavailable"""
        if recent_data is not None and hasattr(recent_data, '__getitem__'):
            if recent_data.ndim > 1 and len(recent_data) > 0:
                last_price = float(recent_data[-1, 2]) if recent_data.shape[1] > 2 else 150.0
                last_capacity = float(recent_data[-1, 10]) if recent_data.shape[1] > 10 else 5000.0
            else:
                last_price = 150.0
                last_capacity = 5000.0
        else:
            last_price = 150.0
            last_capacity = 5000.0
        
        price_forecast = [last_price * (1 + 0.01 * i) for i in range(horizon)]
        capacity_forecast = [last_capacity * (1 + 0.02 * i) for i in range(horizon)]
        
        return ForecastResult(
            horizon_months=horizon,
            price_forecast=price_forecast,
            capacity_forecast=capacity_forecast,
            scarcity_forecast=[min(1.0, p / 200) for p in price_forecast],
            production_forecast=[28500 * (1 + 0.005 * i) for i in range(horizon)],
            demand_forecast=[29500 * (1 - 0.3 * (p - price_forecast[0]) / max(price_forecast[0], 1)) for p in price_forecast],
            model_name="baseline",
            market_outlook=self._determine_outlook(price_forecast),
            price_trend=self._determine_trend(price_forecast),
            risk_level=self._assess_risk(price_forecast),
            recommended_actions=self._generate_recommendations(price_forecast, capacity_forecast),
            forecast_confidence=0.5
        )
    
    def _determine_trend(self, forecast):
        if len(forecast) < 2:
            return "stable"
        change = (forecast[-1] - forecast[0]) / max(forecast[0], 0.001) * 100
        if change > 15: return "strongly_increasing"
        elif change > 5: return "increasing"
        elif change > -5: return "stable"
        elif change > -15: return "decreasing"
        return "strongly_decreasing"
    
    def _determine_outlook(self, forecast):
        trend = self._determine_trend(forecast)
        return {"strongly_increasing": "tightening", "increasing": "cautious", 
                "stable": "stable", "decreasing": "improving", "strongly_decreasing": "easing"}.get(trend, "stable")
    
    def _assess_risk(self, forecast):
        if len(forecast) < 3:
            return "moderate"
        volatility = np.std(forecast) / max(np.mean(forecast), 0.001)
        if forecast[-1] > 300 or volatility > 0.3:
            return "critical"
        elif forecast[-1] > 200 or volatility > 0.15:
            return "high"
        elif volatility > 0.08:
            return "moderate"
        return "low"
    
    def _generate_recommendations(self, price_forecast, capacity_forecast):
        trend = self._determine_trend(price_forecast)
        risk = self._assess_risk(price_forecast)
        capacity_trend = "increasing" if len(capacity_forecast) > 1 and capacity_forecast[-1] > capacity_forecast[0] else "stable"
        recs = []
        
        if risk == "critical":
            recs.extend(["URGENT: Secure long-term helium supply contracts immediately", "Activate emergency helium conservation protocols"])
        if trend in ["strongly_increasing", "increasing"]:
            recs.extend(["Increase helium recycling investments by 50%", "Accelerate substitution technology research"])
        if capacity_trend == "increasing":
            recs.extend(["New production capacity coming online - monitor for supply relief", "Adjust procurement strategy based on capacity timeline"])
        if risk in ["high", "critical"]:
            recs.extend(["Build strategic helium reserve (6-month supply)", "Diversify supply sources across 3+ countries"])
        
        return recs[:10] if recs else ["Maintain current helium management strategy", "Continue monitoring market conditions"]
    
    def export_forecast(self) -> Dict:
        """Export forecast for all integrations"""
        if not self.forecast_history:
            return {'error': 'No forecasts available'}
        
        latest = self.forecast_history[-1]
        
        return {
            'forecast': latest.to_dict(),
            'scenarios': self._generate_scenarios(latest),
            'integration_data': {
                'regret_optimizer': {
                    'price_scenarios': self._generate_scenarios(latest),
                    'capacity_scenarios': {
                        'base': latest.capacity_forecast,
                        'best': [c * 0.85 for c in latest.capacity_forecast],
                        'worst': [c * 1.15 for c in latest.capacity_forecast]
                    },
                    'scarcity_trajectory': latest.scarcity_forecast,
                    'risk_level': latest.risk_level
                },
                'sustainability_signals': {
                    'helium_outlook': latest.market_outlook,
                    'scarcity_forecast': latest.scarcity_forecast,
                    'price_trend': latest.price_trend,
                    'capacity_forecast': latest.capacity_forecast,
                    'recommended_actions': latest.recommended_actions
                },
                'thermal_optimizer': {
                    'cooling_cost_forecast': [p * 0.01 for p in latest.price_forecast],
                    'scarcity_impact': latest.scarcity_forecast,
                    'capacity_adjustment': [1 - c / 20000 for c in latest.capacity_forecast],
                    'confidence_intervals': latest.price_confidence_intervals
                }
            },
            'blockchain': {
                'verified': latest.blockchain_verified,
                'transaction_hash': latest.blockchain_transaction_hash[:16] if latest.blockchain_transaction_hash else 'N/A'
            },
            'metadata': {
                'model_name': latest.model_name,
                'forecast_confidence': latest.forecast_confidence,
                'generated_at': latest.timestamp,
                'input_dimension': self.input_dim
            }
        }
    
    def _generate_scenarios(self, base_forecast: ForecastResult, n_scenarios: int = 100) -> Dict:
        """Generate scenario analysis"""
        if not base_forecast.price_forecast:
            return {'base_case': [], 'best_case': [], 'worst_case': []}
        
        best_case = []
        worst_case = []
        
        for i in range(len(base_forecast.price_forecast)):
            base = base_forecast.price_forecast[i]
            if i < len(base_forecast.forecast_uncertainty):
                uncertainty = base_forecast.forecast_uncertainty[i]
            else:
                uncertainty = base * 0.1
            
            samples = np.random.normal(base, uncertainty, n_scenarios)
            best_case.append(float(np.percentile(samples, 10)))
            worst_case.append(float(np.percentile(samples, 90)))
        
        return {
            'base_case': base_forecast.price_forecast,
            'best_case': best_case,
            'worst_case': worst_case,
            'n_scenarios': n_scenarios,
            'timestamp': datetime.now().isoformat()
        }
    
    def health_check(self) -> Dict:
        """Health check for control system integration"""
        integrations_status = {
            'helium_collector': self.collector is not None,
            'blockchain': self.blockchain_verifier is not None,
            'pytorch': TORCH_AVAILABLE,
            'sklearn': SKLEARN_AVAILABLE
        }
        healthy = sum(1 for v in integrations_status.values() if v)
        total = len(integrations_status)
        
        recent_forecast = False
        if self.forecast_history:
            last = self.forecast_history[-1]
            recent_forecast = (datetime.now() - datetime.fromisoformat(last.timestamp)).total_seconds() < 3600
        
        return {
            'healthy': self.models_trained and healthy >= 2,
            'status': 'fully_operational' if self.models_trained and healthy >= 3 else 'degraded' if self.models_trained else 'not_trained',
            'integrations': integrations_status,
            'healthy_integrations': healthy,
            'total_integrations': total,
            'integration_health_pct': (healthy / max(total, 1)) * 100,
            'models_trained': self.models_trained,
            'model_version': self.model_version,
            'total_forecasts': len(self.forecast_history),
            'recent_forecast': recent_forecast,
            'latest_risk_level': self.forecast_history[-1].risk_level if self.forecast_history else 'unknown',
            'latest_confidence': self.forecast_history[-1].forecast_confidence if self.forecast_history else 0,
            'input_dimension': self.input_dim,
            'avg_forecast_time_ms': np.mean(list(self.performance_metrics['forecast_times'])) * 1000 if self.performance_metrics['forecast_times'] else 0,
            'blockchain_enabled': BLOCKCHAIN_AVAILABLE,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_statistics(self) -> Dict:
        """Get comprehensive statistics"""
        return {
            'model_info': {
                'version': self.model_version,
                'trained': self.models_trained,
                'lstm_available': self.lstm_model is not None,
                'transformer_available': self.transformer_model is not None,
                'input_dimension': self.input_dim
            },
            'training': {
                'total_trainings': len(self.training_history),
                'latest_results': self.training_history[-1] if self.training_history else None
            },
            'forecasts': {
                'total_forecasts': len(self.forecast_history),
                'latest_trend': self.forecast_history[-1].price_trend if self.forecast_history else 'N/A',
                'latest_risk': self.forecast_history[-1].risk_level if self.forecast_history else 'N/A',
                'avg_confidence': np.mean(list(self.performance_metrics['confidence_scores'])) if self.performance_metrics['confidence_scores'] else 0,
                'avg_generation_time_ms': np.mean(list(self.performance_metrics['forecast_times'])) * 1000 if self.performance_metrics['forecast_times'] else 0
            },
            'integrations': {
                'active_count': self._count_active_integrations(),
                'active_list': self.get_active_integrations(),
                'blockchain_verifications': sum(1 for f in self.forecast_history if f.blockchain_verified)
            },
            'latest_forecast': self.forecast_history[-1].to_dict() if self.forecast_history else None
        }

# ============================================================
# SINGLETON INSTANCE
# ============================================================

_forecaster = None

def get_helium_forecaster() -> HeliumForecaster:
    """Get singleton forecaster"""
    global _forecaster
    if _forecaster is None:
        _forecaster = HeliumForecaster()
    return _forecaster

def quick_forecast(historical_data: np.ndarray = None) -> ForecastResult:
    """Quick forecast with default settings"""
    forecaster = get_helium_forecaster()
    if not forecaster.models_trained:
        forecaster.train(historical_data, epochs=50)
    return forecaster.forecast(historical_data[-60:] if historical_data is not None and len(historical_data) >= 60 else None)

# ============================================================
# ENHANCED MAIN DEMO
# ============================================================

def main():
    """Enhanced v6.3 demonstration"""
    print("=" * 80)
    print("Helium Market Forecaster v6.3 - Enhanced Demo")
    print("=" * 80)
    
    forecaster = HeliumForecaster()
    
    print(f"\n✅ v6.3 Enhancements Active:")
    print(f"   Input Dimension: {forecaster.input_dim} (11 features)")
    print(f"   Capacity Forecasting: ✅")
    print(f"   Helium Collector: {'✅' if HELIUM_COLLECTOR_AVAILABLE else '❌'}")
    print(f"   Active Integrations: {forecaster._count_active_integrations()}")
    
    # Generate sample data with 11 features
    np.random.seed(42)
    sample_data = np.random.randn(200, forecaster.input_dim) * 0.1 + np.arange(200).reshape(-1, 1) * 0.01
    # Ensure capacity column (index 10) has realistic values
    sample_data[:, 10] = 5000 + np.cumsum(np.random.randn(200) * 100)
    
    # Train models
    print(f"\n🧠 Training Models...")
    training_result = forecaster.train(sample_data, epochs=30)
    print(f"   LSTM Val Loss: {training_result.get('lstm', {}).get('final_val_loss', 0):.4f}")
    print(f"   Transformer Val Loss: {training_result.get('transformer', {}).get('final_val_loss', 0):.4f}")
    
    # Generate forecast
    print(f"\n🔮 Generating Forecast...")
    forecast = forecaster.forecast(sample_data[-60:])
    print(f"   Price Trend: {forecast.price_trend}")
    print(f"   Risk Level: {forecast.risk_level}")
    print(f"   Confidence: {forecast.forecast_confidence:.3f}")
    print(f"   Blockchain Verified: {'✅' if forecast.blockchain_verified else '❌'}")
    
    if forecast.price_forecast:
        print(f"   Price Forecast (12m): {[f'{p:.0f}' for p in forecast.price_forecast[:6]]}...")
    if forecast.capacity_forecast:
        print(f"   Capacity Forecast (12m): {[f'{c:.0f}' for c in forecast.capacity_forecast[:6]]}...")
    
    # Recommendations
    if forecast.recommended_actions:
        print(f"\n💡 Recommendations:")
        for i, rec in enumerate(forecast.recommended_actions[:5], 1):
            print(f"   {i}. {rec}")
    
    # Health check
    health = forecaster.health_check()
    print(f"\n🏥 Health Check:")
    print(f"   Status: {health['status']}")
    print(f"   Integration Health: {health['integration_health_pct']:.0f}%")
    print(f"   Input Dimension: {health['input_dimension']}")
    
    print("\n" + "=" * 80)
    print("✅ Helium Forecaster v6.3 - Demo Complete")
    print("=" * 80)
    
    return forecaster

if __name__ == "__main__":
    forecaster = main()
