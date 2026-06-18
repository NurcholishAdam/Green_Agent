# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/predictive_homeostasis.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Predictive Homeostasis v5.0.0
Complete implementation with seasonal decomposition, ensemble forecasting,
accuracy tracking, model selection, consumer integration, anomaly filtering,
and multi-field coordination.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import deque
from enum import Enum
import math

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class ForecastModel(Enum):
    """Available forecasting models"""
    HOLT = "holt"
    HOLT_WINTERS = "holt_winters"
    MOVING_AVERAGE = "moving_average"
    ENSEMBLE = "ensemble"
    AUTO_ARIMA = "auto_arima"

class SeasonalityType(Enum):
    """Types of seasonality detected"""
    NONE = "none"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"

@dataclass
class ForecastResult:
    """Enhanced forecast result with confidence intervals"""
    field_id: str
    current_value: float
    predicted_value: float
    prediction_horizon_seconds: float
    confidence: float
    confidence_interval: Tuple[float, float]  # (lower, upper)
    trend: str
    anomaly_probability: float
    recommended_action: str
    model_used: ForecastModel
    forecast_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class ModelPerformance:
    """Track forecasting model performance"""
    model: ForecastModel
    field_id: str
    mae: float = 0.0        # Mean Absolute Error
    rmse: float = 0.0       # Root Mean Square Error
    mape: float = 0.0       # Mean Absolute Percentage Error
    predictions_made: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True

@dataclass
class SeasonalPattern:
    """Detected seasonal pattern"""
    pattern_type: SeasonalityType
    period: int             # Number of data points per cycle
    strength: float         # 0-1, how strong the seasonality is
    peak_hours: List[int]   # Hours with peak values
    trough_hours: List[int] # Hours with minimum values
    confidence: float

@dataclass
class FieldForecast:
    """Complete forecast state for a single field"""
    field_id: str
    current_value: float
    short_term: ForecastResult
    medium_term: ForecastResult
    long_term: ForecastResult
    seasonal_pattern: Optional[SeasonalPattern]
    best_model: ForecastModel
    anomaly_probability: float

# ============================================================================
# Enhanced Predictive Homeostasis
# ============================================================================

class PredictiveHomeostasis:
    """
    Enhanced Predictive Homeostasis v5.0.0
    
    Complete implementation with:
    - Seasonal decomposition (daily/weekly patterns)
    - Ensemble forecasting (Holt, Holt-Winters, MA, Ensemble)
    - Accuracy tracking and model selection
    - Consumer integration via event bus
    - Anomaly filtering for clean forecasts
    - Multi-field coordinated forecasting
    - Prediction intervals with confidence
    - Adaptive model parameters
    """
    
    def __init__(self, gradient_manager=None, event_bus=None):
        self.gradient_manager = gradient_manager
        self.event_bus = event_bus
        
        # Forecasting models per field
        self.models: Dict[str, Dict[str, Any]] = {}
        self.model_performance: Dict[str, Dict[ForecastModel, ModelPerformance]] = defaultdict(dict)
        
        # Historical data for training
        self.history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self.raw_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Prediction horizons
        self.short_term_horizon = 60      # 1 minute
        self.medium_term_horizon = 300    # 5 minutes
        self.long_term_horizon = 1800     # 30 minutes
        
        # Thresholds
        self.warning_threshold = 0.7
        self.critical_threshold = 0.85
        
        # Forecast history for accuracy tracking
        self.forecast_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
        self.accuracy_history: Dict[str, List[Dict]] = defaultdict(list)
        
        # Seasonal patterns
        self.seasonal_patterns: Dict[str, SeasonalPattern] = {}
        
        # Anomaly detection
        self.anomaly_threshold = 3.0  # Z-score threshold
        self.anomaly_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Adaptive parameters
        self.adaptive_params: Dict[str, Dict[str, float]] = defaultdict(lambda: {
            'alpha': 0.3, 'beta': 0.1, 'gamma': 0.1
        })
        
        # Active consumers (modules that receive forecasts)
        self.forecast_consumers: Dict[str, List[Callable]] = defaultdict(list)
        
        # Start forecasting and maintenance loops
        asyncio.create_task(self._forecasting_loop())
        asyncio.create_task(self._accuracy_tracking_loop())
        asyncio.create_task(self._seasonal_detection_loop())
        
        logger.info("Enhanced Predictive Homeostasis v5.0.0 initialized")
    
    # ========================================================================
    # Consumer Registration (NEW)
    # ========================================================================
    
    def register_consumer(self, field_id: str, callback: Callable):
        """Register a consumer for forecast events on a specific field"""
        self.forecast_consumers[field_id].append(callback)
        logger.debug(f"Consumer registered for {field_id} forecasts")
    
    def unregister_consumer(self, field_id: str, callback: Callable):
        """Unregister a consumer"""
        if field_id in self.forecast_consumers:
            self.forecast_consumers[field_id].remove(callback)
    
    def _notify_consumers(self, field_id: str, forecast: FieldForecast):
        """Notify all registered consumers of new forecast"""
        for callback in self.forecast_consumers.get(field_id, []):
            try:
                callback(forecast)
            except Exception as e:
                logger.error(f"Consumer callback error: {str(e)}")
        
        # Also publish via event bus if available
        if self.event_bus:
            self.event_bus.publish('gradient_forecast', {
                'field_id': field_id,
                'current': forecast.current_value,
                'short_term': forecast.short_term.predicted_value,
                'medium_term': forecast.medium_term.predicted_value,
                'trend': forecast.short_term.trend,
                'confidence': forecast.short_term.confidence,
                'anomaly_probability': forecast.anomaly_probability,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            # Critical forecast event
            if forecast.short_term.predicted_value > self.critical_threshold:
                self.event_bus.publish('gradient_forecast_critical', {
                    'field_id': field_id,
                    'predicted_value': forecast.short_term.predicted_value,
                    'time_to_critical': self.short_term_horizon,
                    'action': forecast.short_term.recommended_action,
                    'timestamp': datetime.utcnow().isoformat()
                })
    
    # ========================================================================
    # Anomaly Filtering (NEW)
    # ========================================================================
    
    def _is_anomaly(self, field_id: str, value: float) -> Tuple[bool, float]:
        """
        Detect if a value is anomalous using modified Z-score.
        
        Returns (is_anomaly, zscore).
        """
        history = list(self.raw_history.get(field_id, []))
        
        if len(history) < 20:
            return False, 0.0
        
        values = [h['value'] for h in history[-50:]]
        median = np.median(values)
        
        # Median Absolute Deviation
        mad = np.median([abs(v - median) for v in values])
        if mad == 0:
            return False, 0.0
        
        # Modified Z-score
        zscore = 0.6745 * (value - median) / mad
        
        is_anomaly = abs(zscore) > self.anomaly_threshold
        
        if is_anomaly:
            self.anomaly_history[field_id].append({
                'value': value,
                'zscore': zscore,
                'timestamp': datetime.utcnow()
            })
            logger.debug(f"Anomaly detected in {field_id}: value={value:.3f}, zscore={zscore:.2f}")
        
        return is_anomaly, zscore
    
    def _clean_value(self, field_id: str, value: float) -> float:
        """
        Clean anomalous values by replacing with interpolated estimate.
        """
        is_anomaly, zscore = self._is_anomaly(field_id, value)
        
        if not is_anomaly:
            return value
        
        # Replace with last valid value or moving average
        history = list(self.history.get(field_id, []))
        if len(history) >= 5:
            recent_valid = [h['value'] for h in history[-10:]
                          if not self._is_anomaly(field_id, h['value'])[0]]
            if recent_valid:
                return np.mean(recent_valid)
        
        return value  # Fallback to original
    
    # ========================================================================
    # Data Recording
    # ========================================================================
    
    def record_measurement(self, field_id: str, value: float):
        """Record gradient measurement with anomaly filtering"""
        # Store raw value
        self.raw_history[field_id].append({
            'value': value,
            'timestamp': datetime.utcnow()
        })
        
        # Clean anomalous values before storing in model history
        cleaned_value = self._clean_value(field_id, value)
        
        self.history[field_id].append({
            'value': cleaned_value,
            'raw_value': value,
            'is_cleaned': cleaned_value != value,
            'timestamp': datetime.utcnow()
        })
        
        # Update models when enough data
        if len(self.history[field_id]) >= 10:
            self._update_all_models(field_id)
    
    # ========================================================================
    # Model Updates (Enhanced with Multiple Methods)
    # ========================================================================
    
    def _update_all_models(self, field_id: str):
        """Update all forecasting models for a field"""
        self._update_holt_model(field_id)
        self._update_holt_winters_model(field_id)
        self._update_moving_average_model(field_id)
    
    def _update_holt_model(self, field_id: str):
        """Update Holt's exponential smoothing model"""
        history = list(self.history.get(field_id, []))
        if len(history) < 10:
            return
        
        values = [h['value'] for h in history]
        params = self.adaptive_params[field_id]
        
        alpha = params['alpha']
        beta = params['beta']
        
        level = values[0]
        trend = 0
        
        for i in range(1, len(values)):
            new_level = alpha * values[i] + (1 - alpha) * (level + trend)
            new_trend = beta * (new_level - level) + (1 - beta) * trend
            level, trend = new_level, new_trend
        
        if field_id not in self.models:
            self.models[field_id] = {}
        
        self.models[field_id]['holt'] = {
            'level': level,
            'trend': trend,
            'last_updated': datetime.utcnow(),
            'data_points': len(values)
        }
    
    def _update_holt_winters_model(self, field_id: str):
        """Update Holt-Winters model with seasonal component"""
        history = list(self.history.get(field_id, []))
        if len(history) < 30:  # Need more data for seasonal
            return
        
        values = [h['value'] for h in history]
        params = self.adaptive_params[field_id]
        
        # Detect seasonality period
        pattern = self.seasonal_patterns.get(field_id)
        period = pattern.period if pattern else 24  # Default to 24 for hourly
        
        if len(values) < period * 2:
            return
        
        alpha = params['alpha']
        beta = params['beta']
        gamma = params.get('gamma', 0.1)
        
        # Initialize
        level = sum(values[:period]) / period
        trend = (sum(values[period:2*period]) - sum(values[:period])) / (period * period)
        seasonal = [values[i] - level for i in range(period)]
        
        # Triple exponential smoothing
        for i in range(period, len(values)):
            old_level = level
            level = alpha * (values[i] - seasonal[i % period]) + (1 - alpha) * (level + trend)
            trend = beta * (level - old_level) + (1 - beta) * trend
            seasonal[i % period] = gamma * (values[i] - level) + (1 - gamma) * seasonal[i % period]
        
        if field_id not in self.models:
            self.models[field_id] = {}
        
        self.models[field_id]['holt_winters'] = {
            'level': level,
            'trend': trend,
            'seasonal': seasonal,
            'period': period,
            'last_updated': datetime.utcnow(),
            'data_points': len(values)
        }
    
    def _update_moving_average_model(self, field_id: str):
        """Update simple moving average model"""
        history = list(self.history.get(field_id, []))
        if len(history) < 5:
            return
        
        values = [h['value'] for h in history[-50:]]
        
        if field_id not in self.models:
            self.models[field_id] = {}
        
        self.models[field_id]['moving_average'] = {
            'ma_short': np.mean(values[-5:]) if len(values) >= 5 else values[-1],
            'ma_medium': np.mean(values[-20:]) if len(values) >= 20 else np.mean(values),
            'ma_long': np.mean(values[-50:]) if len(values) >= 50 else np.mean(values),
            'last_updated': datetime.utcnow(),
            'data_points': len(values)
        }
    
    # ========================================================================
    # Ensemble Forecasting (NEW)
    # ========================================================================
    
    def forecast(self, field_id: str, horizon_seconds: float,
                model: ForecastModel = ForecastModel.ENSEMBLE) -> ForecastResult:
        """
        Enhanced forecasting with model selection and ensemble support.
        
        Args:
            field_id: Gradient field to forecast
            horizon_seconds: Prediction horizon
            model: Forecasting model to use (default: ensemble)
            
        Returns:
            ForecastResult with confidence intervals
        """
        if model == ForecastModel.ENSEMBLE:
            return self._ensemble_forecast(field_id, horizon_seconds)
        elif model == ForecastModel.HOLT:
            return self._holt_forecast(field_id, horizon_seconds)
        elif model == ForecastModel.HOLT_WINTERS:
            return self._holt_winters_forecast(field_id, horizon_seconds)
        elif model == ForecastModel.MOVING_AVERAGE:
            return self._moving_average_forecast(field_id, horizon_seconds)
        else:
            return self._ensemble_forecast(field_id, horizon_seconds)
    
    def _ensemble_forecast(self, field_id: str, horizon_seconds: float) -> ForecastResult:
        """Combine multiple models for robust forecast"""
        forecasts = []
        weights = []
        
        # Get forecast from each available model
        for model_type in [ForecastModel.HOLT, ForecastModel.HOLT_WINTERS, ForecastModel.MOVING_AVERAGE]:
            model_key = model_type.value
            
            if field_id in self.models and model_key in self.models[field_id]:
                if model_type == ForecastModel.HOLT:
                    result = self._holt_forecast(field_id, horizon_seconds)
                elif model_type == ForecastModel.HOLT_WINTERS:
                    result = self._holt_winters_forecast(field_id, horizon_seconds)
                else:
                    result = self._moving_average_forecast(field_id, horizon_seconds)
                
                # Weight by model performance
                perf = self.model_performance.get(field_id, {}).get(model_type)
                weight = 1.0 / (1.0 + perf.rmse) if perf and perf.rmse > 0 else 1.0
                
                forecasts.append(result)
                weights.append(weight)
        
        if not forecasts:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        # Weighted average of predictions
        total_weight = sum(weights)
        ensemble_prediction = sum(f.predicted_value * w for f, w in zip(forecasts, weights)) / total_weight
        
        # Weighted confidence
        ensemble_confidence = sum(f.confidence * w for f, w in zip(forecasts, weights)) / total_weight
        
        # Confidence interval
        predictions = [f.predicted_value for f in forecasts]
        std_pred = np.std(predictions) if len(predictions) > 1 else 0.05
        lower = max(0, ensemble_prediction - 2 * std_pred)
        upper = min(1, ensemble_prediction + 2 * std_pred)
        
        # Best individual trend
        trends = [f.trend for f in forecasts]
        ensemble_trend = max(set(trends), key=trends.count) if trends else 'stable'
        
        # Best recommendation
        best_forecast = max(forecasts, key=lambda f: f.confidence)
        
        # Select best model for this field
        best_model = self._select_best_model(field_id)
        
        return ForecastResult(
            field_id=field_id,
            current_value=forecasts[0].current_value,
            predicted_value=ensemble_prediction,
            prediction_horizon_seconds=horizon_seconds,
            confidence=ensemble_confidence,
            confidence_interval=(lower, upper),
            trend=ensemble_trend,
            anomaly_probability=forecasts[0].anomaly_probability,
            recommended_action=best_forecast.recommended_action,
            model_used=best_model
        )
    
    def _holt_forecast(self, field_id: str, horizon_seconds: float) -> ForecastResult:
        """Holt's exponential smoothing forecast"""
        if field_id not in self.models or 'holt' not in self.models[field_id]:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        model = self.models[field_id]['holt']
        history = list(self.history.get(field_id, []))
        
        if not history:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        current = history[-1]['value']
        predicted = model['level'] + model['trend'] * horizon_seconds
        predicted = max(0, min(1, predicted))
        
        confidence = self._calculate_confidence(field_id, ForecastModel.HOLT)
        trend = 'rising' if model['trend'] > 0.001 else 'falling' if model['trend'] < -0.001 else 'stable'
        anomaly_prob = self._calculate_anomaly_probability(field_id, current)
        
        # Confidence interval
        std = np.std([h['value'] for h in history[-20:]]) if len(history) >= 20 else 0.05
        lower = max(0, predicted - 2 * std)
        upper = min(1, predicted + 2 * std)
        
        return ForecastResult(
            field_id=field_id, current_value=current, predicted_value=predicted,
            prediction_horizon_seconds=horizon_seconds, confidence=confidence,
            confidence_interval=(lower, upper), trend=trend,
            anomaly_probability=anomaly_prob,
            recommended_action=self._generate_recommendation(field_id, current, predicted, trend),
            model_used=ForecastModel.HOLT
        )
    
    def _holt_winters_forecast(self, field_id: str, horizon_seconds: float) -> ForecastResult:
        """Holt-Winters forecast with seasonality"""
        if field_id not in self.models or 'holt_winters' not in self.models[field_id]:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        model = self.models[field_id]['holt_winters']
        history = list(self.history.get(field_id, []))
        
        if not history:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        current = history[-1]['value']
        
        # Predict with seasonal component
        steps = int(horizon_seconds / 60)  # Convert to steps
        period = model['period']
        seasonal_idx = (len(history) + steps) % period
        seasonal_component = model['seasonal'][seasonal_idx] if seasonal_idx < len(model['seasonal']) else 0
        
        predicted = model['level'] + model['trend'] * horizon_seconds + seasonal_component * 0.3
        predicted = max(0, min(1, predicted))
        
        confidence = self._calculate_confidence(field_id, ForecastModel.HOLT_WINTERS) * 0.9
        trend = 'rising' if model['trend'] > 0.001 else 'falling' if model['trend'] < -0.001 else 'stable'
        anomaly_prob = self._calculate_anomaly_probability(field_id, current)
        
        std = np.std([h['value'] for h in history[-20:]]) if len(history) >= 20 else 0.05
        lower = max(0, predicted - 2 * std)
        upper = min(1, predicted + 2 * std)
        
        return ForecastResult(
            field_id=field_id, current_value=current, predicted_value=predicted,
            prediction_horizon_seconds=horizon_seconds, confidence=confidence,
            confidence_interval=(lower, upper), trend=trend,
            anomaly_probability=anomaly_prob,
            recommended_action=self._generate_recommendation(field_id, current, predicted, trend),
            model_used=ForecastModel.HOLT_WINTERS
        )
    
    def _moving_average_forecast(self, field_id: str, horizon_seconds: float) -> ForecastResult:
        """Moving average forecast"""
        if field_id not in self.models or 'moving_average' not in self.models[field_id]:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        model = self.models[field_id]['moving_average']
        history = list(self.history.get(field_id, []))
        
        if not history:
            return self._create_default_forecast(field_id, horizon_seconds)
        
        current = history[-1]['value']
        
        # Use appropriate MA based on horizon
        if horizon_seconds <= 60:
            predicted = model['ma_short']
        elif horizon_seconds <= 300:
            predicted = model['ma_medium']
        else:
            predicted = model['ma_long']
        
        predicted = max(0, min(1, predicted))
        
        confidence = self._calculate_confidence(field_id, ForecastModel.MOVING_AVERAGE) * 0.8
        trend = 'stable'
        anomaly_prob = self._calculate_anomaly_probability(field_id, current)
        
        std = np.std([h['value'] for h in history[-20:]]) if len(history) >= 20 else 0.05
        lower = max(0, predicted - 2 * std)
        upper = min(1, predicted + 2 * std)
        
        return ForecastResult(
            field_id=field_id, current_value=current, predicted_value=predicted,
            prediction_horizon_seconds=horizon_seconds, confidence=confidence,
            confidence_interval=(lower, upper), trend=trend,
            anomaly_probability=anomaly_prob,
            recommended_action=self._generate_recommendation(field_id, current, predicted, trend),
            model_used=ForecastModel.MOVING_AVERAGE
        )
    
    def _create_default_forecast(self, field_id: str, horizon_seconds: float) -> ForecastResult:
        """Create default forecast when no model available"""
        return ForecastResult(
            field_id=field_id, current_value=0.5, predicted_value=0.5,
            prediction_horizon_seconds=horizon_seconds, confidence=0.3,
            confidence_interval=(0.3, 0.7), trend='stable',
            anomaly_probability=0.0,
            recommended_action='MONITOR: Insufficient data for forecast',
            model_used=ForecastModel.ENSEMBLE
        )
    
    # ========================================================================
    # Model Selection (NEW)
    # ========================================================================
    
    def _select_best_model(self, field_id: str) -> ForecastModel:
        """Select the best performing model for a field"""
        performances = self.model_performance.get(field_id, {})
        
        if not performances:
            return ForecastModel.ENSEMBLE
        
        # Find model with lowest RMSE
        best_model = None
        best_rmse = float('inf')
        
        for model, perf in performances.items():
            if perf.rmse < best_rmse and perf.predictions_made >= 10:
                best_rmse = perf.rmse
                best_model = model
        
        return best_model or ForecastModel.ENSEMBLE
    
    def _calculate_confidence(self, field_id: str, model: ForecastModel) -> float:
        """Calculate confidence based on model performance"""
        perf = self.model_performance.get(field_id, {}).get(model)
        
        if not perf or perf.predictions_made < 10:
            history = list(self.history.get(field_id, []))
            data_confidence = min(1.0, len(history) / 50)
            return data_confidence * 0.7
        
        # Confidence based on accuracy
        accuracy_confidence = 1.0 / (1.0 + perf.rmse * 5)
        data_confidence = min(1.0, perf.predictions_made / 100)
        
        return accuracy_confidence * 0.7 + data_confidence * 0.3
    
    # ========================================================================
    # Accuracy Tracking (NEW)
    # ========================================================================
    
    def _record_prediction(self, field_id: str, predicted: float, actual: float, 
                          model: ForecastModel):
        """Record prediction for accuracy tracking"""
        error = abs(predicted - actual)
        
        if field_id not in self.model_performance:
            self.model_performance[field_id] = {}
        
        if model not in self.model_performance[field_id]:
            self.model_performance[field_id][model] = ModelPerformance(
                model=model, field_id=field_id
            )
        
        perf = self.model_performance[field_id][model]
        n = perf.predictions_made + 1
        
        # Update metrics with exponential moving average
        alpha = 0.1
        perf.mae = perf.mae * (1 - alpha) + error * alpha
        perf.rmse = math.sqrt(perf.rmse**2 * (1 - alpha) + error**2 * alpha)
        
        if actual > 0:
            percentage_error = error / actual
            perf.mape = perf.mape * (1 - alpha) + percentage_error * alpha
        
        perf.predictions_made = n
        perf.last_updated = datetime.utcnow()
    
    async def _accuracy_tracking_loop(self):
        """Track and validate forecast accuracy"""
        while True:
            try:
                # Check past predictions against actual values
                for field_id in list(self.forecast_history.keys()):
                    predictions = list(self.forecast_history[field_id])
                    history = list(self.history.get(field_id, []))
                    
                    if not predictions or not history:
                        continue
                    
                    # Match predictions with actual outcomes
                    for pred in predictions[-10:]:
                        pred_time = pred.forecast_at
                        horizon = pred.prediction_horizon_seconds
                        
                        # Find actual value at prediction time + horizon
                        target_time = pred_time + timedelta(seconds=horizon)
                        actual_values = [
                            h['value'] for h in history
                            if abs((h['timestamp'] - target_time).total_seconds()) < 30
                        ]
                        
                        if actual_values:
                            actual = np.mean(actual_values)
                            self._record_prediction(
                                field_id, pred.predicted_value, actual, pred.model_used
                            )
                
                # Log accuracy summary periodically
                for field_id, performances in self.model_performance.items():
                    for model, perf in performances.items():
                        if perf.predictions_made > 0 and perf.predictions_made % 50 == 0:
                            logger.debug(
                                f"Model {model.value} on {field_id}: "
                                f"RMSE={perf.rmse:.4f}, MAE={perf.mae:.4f}, n={perf.predictions_made}"
                            )
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Accuracy tracking error: {str(e)}")
                await asyncio.sleep(120)
    
    # ========================================================================
    # Seasonal Detection (NEW)
    # ========================================================================
    
    async def _seasonal_detection_loop(self):
        """Detect seasonal patterns in gradient data"""
        while True:
            try:
                for field_id in list(self.history.keys()):
                    history = list(self.history[field_id])
                    
                    if len(history) < 48:  # Need at least 2 days of hourly data
                        continue
                    
                    values = [h['value'] for h in history[-200:]]
                    
                    # Simple autocorrelation for daily pattern (24-hour period)
                    if len(values) >= 48:
                        # Check 24-hour autocorrelation
                        autocorr_24 = self._autocorrelation(values, 24)
                        
                        if autocorr_24 > 0.3:
                            # Detect peak and trough hours
                            hourly_avg = defaultdict(list)
                            for i, h in enumerate(history[-168:]):  # Last week
                                hour = h['timestamp'].hour
                                hourly_avg[hour].append(h['value'])
                            
                            hour_means = {h: np.mean(v) for h, v in hourly_avg.items() if v}
                            
                            if hour_means:
                                sorted_hours = sorted(hour_means.items(), key=lambda x: x[1])
                                trough_hours = [h for h, _ in sorted_hours[:4]]
                                peak_hours = [h for h, _ in sorted_hours[-4:]]
                                
                                self.seasonal_patterns[field_id] = SeasonalPattern(
                                    pattern_type=SeasonalityType.DAILY,
                                    period=24,
                                    strength=autocorr_24,
                                    peak_hours=peak_hours,
                                    trough_hours=trough_hours,
                                    confidence=min(0.9, autocorr_24)
                                )
                                
                                logger.debug(
                                    f"Daily pattern detected in {field_id}: "
                                    f"peaks at {peak_hours}, troughs at {trough_hours}"
                                )
                
                await asyncio.sleep(3600)  # Check every hour
                
            except Exception as e:
                logger.error(f"Seasonal detection error: {str(e)}")
                await asyncio.sleep(3600)
    
    def _autocorrelation(self, values: List[float], lag: int) -> float:
        """Calculate autocorrelation at specified lag"""
        if len(values) <= lag:
            return 0.0
        
        mean = np.mean(values)
        var = np.var(values)
        
        if var == 0:
            return 0.0
        
        n = len(values)
        cov = sum((values[i] - mean) * (values[i - lag] - mean) for i in range(lag, n))
        
        return cov / ((n - lag) * var)
    
    # ========================================================================
    # Main Forecasting Loop
    # ========================================================================
    
    async def _forecasting_loop(self):
        """Continuous forecasting with pre-emptive adjustments"""
        while True:
            try:
                if not self.gradient_manager:
                    await asyncio.sleep(30)
                    continue
                
                for field_id in self.gradient_manager.fields:
                    field = self.gradient_manager.fields[field_id]
                    
                    # Record measurement
                    self.record_measurement(field_id, field.effective_strength)
                    
                    # Generate forecasts
                    short_term = self.forecast(field_id, self.short_term_horizon)
                    medium_term = self.forecast(field_id, self.medium_term_horizon)
                    long_term = self.forecast(field_id, self.long_term_horizon)
                    
                    # Store forecast
                    self.forecast_history[field_id].append(short_term)
                    
                    # Build field forecast
                    field_forecast = FieldForecast(
                        field_id=field_id,
                        current_value=short_term.current_value,
                        short_term=short_term,
                        medium_term=medium_term,
                        long_term=long_term,
                        seasonal_pattern=self.seasonal_patterns.get(field_id),
                        best_model=self._select_best_model(field_id),
                        anomaly_probability=short_term.anomaly_probability
                    )
                    
                    # Notify consumers
                    self._notify_consumers(field_id, field_forecast)
                    
                    # Apply pre-emptive adjustments
                    if short_term.trend == 'rising' and short_term.predicted_value > self.warning_threshold:
                        # Graduated response based on prediction magnitude
                        if short_term.predicted_value > self.critical_threshold:
                            # Critical - aggressive dampening
                            field.leakage_rate = min(0.5, field.leakage_rate * 3)
                            logger.warning(
                                f"CRITICAL PRE-EMPTIVE: {field_id} predicted at "
                                f"{short_term.predicted_value:.2f} in {self.short_term_horizon}s"
                            )
                        else:
                            # Warning - moderate dampening
                            field.leakage_rate = min(0.3, field.leakage_rate * 1.5)
                            logger.info(
                                f"Pre-emptive dampening on {field_id}: "
                                f"predicted {short_term.predicted_value:.2f}"
                            )
                        
                        # Schedule restoration
                        asyncio.create_task(self._restore_leakage(field, 120))
                    
                    # Publish critical forecast event
                    if short_term.predicted_value > self.critical_threshold and self.event_bus:
                        self.event_bus.publish('gradient_forecast_critical', {
                            'field_id': field_id,
                            'predicted_value': short_term.predicted_value,
                            'time_to_critical': self.short_term_horizon,
                            'confidence': short_term.confidence,
                            'action': short_term.recommended_action,
                            'timestamp': datetime.utcnow().isoformat()
                        })
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Forecasting loop error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _restore_leakage(self, field, delay_seconds: float):
        """Restore original leakage rate after delay"""
        original = field.leakage_rate
        await asyncio.sleep(delay_seconds)
        field.leakage_rate = max(0.01, original / 1.5)
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def _calculate_anomaly_probability(self, field_id: str, current_value: float) -> float:
        """Calculate probability that current value is anomalous"""
        history = list(self.history.get(field_id, []))
        if len(history) < 20:
            return 0.0
        
        values = [h['value'] for h in history[-50:]]
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            return 0.0
        
        zscore = abs(current_value - mean) / std
        return min(1.0, zscore / 3.0)
    
    def _generate_recommendation(self, field_id: str, current: float,
                                 predicted: float, trend: str) -> str:
        """Generate graduated pre-emptive recommendation"""
        if predicted > self.critical_threshold and trend == 'rising':
            return (f"CRITICAL: {field_id} predicted at {predicted:.2f} (critical). "
                   f"Apply aggressive dampening immediately.")
        elif predicted > self.warning_threshold and trend == 'rising':
            return (f"WARNING: {field_id} predicted at {predicted:.2f} (warning). "
                   f"Apply moderate dampening.")
        elif current > self.critical_threshold:
            return (f"CRITICAL: {field_id} currently at {current:.2f}. "
                   f"Increase leakage rate significantly.")
        elif trend == 'falling' and predicted < 0.2:
            return (f"RECOVERY: {field_id} predicted to recover to {predicted:.2f}. "
                   f"Prepare to reduce dampening.")
        elif trend == 'stable':
            return f"STABLE: {field_id} forecast stable at ~{predicted:.2f}."
        else:
            return f"MONITOR: {field_id} at {current:.2f}, predicted {predicted:.2f}."
    
    # ========================================================================
    # Statistics and Reporting
    # ========================================================================
    
    def get_forecast_summary(self) -> Dict[str, Any]:
        """Get forecast summary for all fields"""
        summary = {}
        for field_id in (self.gradient_manager.fields if self.gradient_manager else []):
            short = self.forecast(field_id, self.short_term_horizon)
            medium = self.forecast(field_id, self.medium_term_horizon)
            
            best_model = self._select_best_model(field_id)
            
            summary[field_id] = {
                'current': short.current_value,
                'short_term_60s': short.predicted_value,
                'medium_term_300s': medium.predicted_value,
                'trend': short.trend,
                'confidence': short.confidence,
                'confidence_interval': short.confidence_interval,
                'best_model': best_model.value,
                'seasonal_pattern': (
                    self.seasonal_patterns[field_id].pattern_type.value
                    if field_id in self.seasonal_patterns else 'none'
                ),
                'anomaly_probability': short.anomaly_probability,
                'recommendation': short.recommended_action
            }
        return summary
    
    def get_accuracy_report(self) -> Dict[str, Any]:
        """Get forecasting accuracy report"""
        report = {}
        
        for field_id, performances in self.model_performance.items():
            field_report = {}
            for model, perf in performances.items():
                if perf.predictions_made > 0:
                    field_report[model.value] = {
                        'mae': perf.mae,
                        'rmse': perf.rmse,
                        'mape': perf.mape,
                        'predictions': perf.predictions_made,
                        'is_active': perf.is_active
                    }
            
            if field_report:
                report[field_id] = {
                    'models': field_report,
                    'best_model': self._select_best_model(field_id).value,
                    'data_points': len(self.history.get(field_id, []))
                }
        
        return report
    
    def get_seasonal_report(self) -> Dict[str, Any]:
        """Get seasonal pattern report"""
        return {
            field_id: {
                'pattern_type': pattern.pattern_type.value,
                'period': pattern.period,
                'strength': pattern.strength,
                'peak_hours': pattern.peak_hours,
                'trough_hours': pattern.trough_hours,
                'confidence': pattern.confidence
            }
            for field_id, pattern in self.seasonal_patterns.items()
        }
    
    def get_anomaly_report(self) -> Dict[str, Any]:
        """Get anomaly detection report"""
        report = {}
        for field_id, anomalies in self.anomaly_history.items():
            recent = list(anomalies)[-50:]
            report[field_id] = {
                'total_anomalies': len(anomalies),
                'recent_anomalies': len(recent),
                'anomaly_rate': len(recent) / max(len(self.history.get(field_id, [])), 1),
                'latest_zscore': recent[-1]['zscore'] if recent else 0
            }
        return report
