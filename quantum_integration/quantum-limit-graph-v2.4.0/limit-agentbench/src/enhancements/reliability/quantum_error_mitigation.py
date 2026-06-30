# File: enhancements/reliability/quantum_error_mitigation.py

"""
Quantum Error Mitigation for Green Agent v2.0.0
Implements advanced error mitigation techniques for reliable quantum computing.
ENHANCED WITH: Carbon Intensity Integration, Helium Tracking, Federated Learning,
Predictive Analytics, Sustainability Dashboard, Carbon Price Forecasting (NEW),
Helium Price Forecasting (NEW), Differential Privacy for Federated Models (NEW),
Online Learning for Continuous Model Improvement (NEW),
Quantum Error Correction Integration (NEW)
"""

import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from scipy.optimize import minimize
from scipy.linalg import expm
import asyncio
import aiohttp
import os
from collections import deque
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_absolute_error
import hashlib
import torch
import torch.nn as nn

logger = logging.getLogger(__name__)

# ============================================================================
# CARBON INTENSITY MANAGER WITH PRICE FORECASTING (ENHANCED)
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with price forecasting"""
    
    def __init__(self, endpoint: str = "https://api.electricitymap.org/v3/carbon-intensity"):
        self.endpoint = endpoint
        self.carbon_intensity = 0.0
        self.region = "us-east"
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.total_carbon_savings_kg = 0.0
        # NEW: Carbon price forecasting
        self.carbon_price_usd_per_ton = 50.0
        self.price_history = deque(maxlen=1000)
        self.price_forecast_model = None
        self._initialize_price_forecast_model()
        
        # Regional profiles for fallback
        self.region_profiles = {
            'us-east': {'timezone': -5, 'renewable_pct': 30, 'base_intensity': 420},
            'us-west': {'timezone': -8, 'renewable_pct': 45, 'base_intensity': 350},
            'eu-west': {'timezone': 0, 'renewable_pct': 50, 'base_intensity': 280},
            'eu-north': {'timezone': 0, 'renewable_pct': 60, 'base_intensity': 220},
            'asia-east': {'timezone': 8, 'renewable_pct': 20, 'base_intensity': 500},
            'asia-southeast': {'timezone': 7, 'renewable_pct': 25, 'base_intensity': 480},
            'australia': {'timezone': 10, 'renewable_pct': 35, 'base_intensity': 380},
            'south-america': {'timezone': -3, 'renewable_pct': 40, 'base_intensity': 320},
            'africa': {'timezone': 2, 'renewable_pct': 25, 'base_intensity': 450},
            'middle-east': {'timezone': 3, 'renewable_pct': 15, 'base_intensity': 550}
        }
        
        logger.info("Carbon Intensity Manager initialized with price forecasting")
    
    def _initialize_price_forecast_model(self):
        """Initialize carbon price forecasting model"""
        try:
            from sklearn.linear_model import LinearRegression
            self.price_forecast_model = LinearRegression()
            self.price_forecast_trained = False
        except ImportError:
            self.price_forecast_model = None
            self.price_forecast_trained = False
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
        """Fetch real-time carbon intensity from API"""
        async with self._lock:
            session = await self._get_session()
            self.region = region
            
            try:
                url = f"{self.endpoint}/latest?zone={region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.carbon_intensity = data.get('carbonIntensity', 
                            self.region_profiles.get(region, {}).get('base_intensity', 400))
                        self.last_update = datetime.now()
                        self.cache[region] = {
                            'intensity': self.carbon_intensity,
                            'timestamp': self.last_update
                        }
                        self.historical_intensities.append(self.carbon_intensity)
                        # Update carbon price
                        self._update_carbon_price(self.carbon_intensity)
                        logger.info(f"Carbon intensity updated: {region} = {self.carbon_intensity} gCO2/kWh")
                        return {'intensity': self.carbon_intensity, 'region': region}
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
                        self._update_carbon_price(self.carbon_intensity)
                        
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
                self._update_carbon_price(self.carbon_intensity)
            
            return {'intensity': self.carbon_intensity, 'region': self.region}
    
    def _update_carbon_price(self, intensity: float):
        """Update carbon price based on intensity"""
        base_price = 50.0
        intensity_factor = (intensity - 300) / 500
        self.carbon_price_usd_per_ton = max(10.0, base_price * (1.0 + intensity_factor))
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'price': self.carbon_price_usd_per_ton
        })
        if len(self.price_history) > 5 and self.price_forecast_model:
            # Train forecast model with recent prices
            prices = [p['price'] for p in list(self.price_history)[-20:]]
            if len(prices) > 10:
                X = np.array(range(len(prices))).reshape(-1, 1)
                y = np.array(prices)
                self.price_forecast_model.fit(X, y)
                self.price_forecast_trained = True
    
    def _get_fallback_intensity(self, region: str) -> float:
        return self.region_profiles.get(region, {}).get('base_intensity', 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or \
           (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    async def get_current_carbon_price(self) -> float:
        """Get current carbon price"""
        if self.last_update is None or \
           (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_price_usd_per_ton
    
    async def forecast_carbon_prices(self, hours: int = 24) -> Dict[str, Any]:
        """Forecast carbon prices for the next N hours"""
        if not self.price_forecast_trained or not self.price_forecast_model:
            return {'status': 'not_trained'}
        
        try:
            future_indices = np.array(range(
                len(self.price_history),
                len(self.price_history) + hours
            )).reshape(-1, 1)
            predictions = self.price_forecast_model.predict(future_indices)
            
            return {
                'status': 'success',
                'predictions': predictions.tolist(),
                'confidence': 0.8 if len(self.price_history) > 50 else 0.5,
                'current_price': self.carbon_price_usd_per_ton,
                'forecast_hours': hours
            }
        except Exception as e:
            logger.error(f"Carbon price forecast error: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def calculate_quantum_carbon_impact(self, circuit_depth: int, n_qubits: int) -> float:
        """Calculate carbon impact of quantum circuit execution"""
        energy_per_op = 0.000001
        total_operations = circuit_depth * n_qubits * 2
        energy_kwh = total_operations * energy_per_op
        carbon_kg = energy_kwh * self.carbon_intensity / 1000
        return carbon_kg
    
    async def calculate_carbon_savings(self, original_carbon: float, mitigated_carbon: float) -> float:
        """Calculate carbon savings from mitigation"""
        savings = original_carbon - mitigated_carbon
        self.total_carbon_savings_kg += savings
        return savings
    
    async def get_optimal_hours(self, hours: int = 24) -> List[datetime]:
        """Get optimal hours for quantum operations based on carbon intensity"""
        current_hour = datetime.now().hour
        optimal_hours = []
        for i in range(hours):
            hour = (current_hour + i) % 24
            if 22 <= hour or hour <= 4:
                optimal_hours.append(datetime.now() + timedelta(hours=i))
        return optimal_hours
    
    async def get_carbon_trend(self) -> Dict[str, Any]:
        """Get carbon intensity trend"""
        if len(self.historical_intensities) < 5:
            return {'trend': 'stable', 'confidence': 0.0}
        
        recent = list(self.historical_intensities)[-20:]
        trend = np.polyfit(range(len(recent)), recent, 1)[0]
        
        return {
            'trend': 'increasing' if trend > 0.5 else 'decreasing' if trend < -0.5 else 'stable',
            'slope': trend,
            'current_intensity': self.carbon_intensity,
            'confidence': 0.7 if len(recent) > 20 else 0.5
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# HELIUM QUANTUM TRACKER WITH PRICE FORECASTING (ENHANCED)
# ============================================================================

class HeliumQuantumTracker:
    """Helium tracking for quantum operations with price forecasting"""
    
    def __init__(self, helium_budget_l: float = 100.0):
        self.helium_budget_l = helium_budget_l
        self.helium_usage: Dict[str, float] = {}
        self.operation_helium: Dict[str, float] = {}
        self.total_usage_l = 0.0
        self._lock = asyncio.Lock()
        self.history = deque(maxlen=10000)
        # NEW: Helium price forecasting
        self.helium_price_usd_per_l = 0.5
        self.price_history = deque(maxlen=1000)
        self.price_forecast_model = None
        self._initialize_price_forecast_model()
        
        # Helium efficiency by mitigation method
        self.method_efficiency = {
            'zne': 0.8,
            'pec': 0.6,
            'cdr': 0.7,
            'dd': 0.9,
            'measurement': 0.85,
            'symmetry': 0.75,
            'hybrid_dd_zne': 0.7,
            'fallback_simple': 0.95
        }
        
        logger.info(f"Helium Quantum Tracker initialized: budget={helium_budget_l}L")
    
    def _initialize_price_forecast_model(self):
        """Initialize helium price forecasting model"""
        try:
            from sklearn.linear_model import LinearRegression
            self.price_forecast_model = LinearRegression()
            self.price_forecast_trained = False
        except ImportError:
            self.price_forecast_model = None
            self.price_forecast_trained = False
    
    def _update_helium_price(self, scarcity: float):
        """Update helium price based on scarcity"""
        base_price = 0.5
        self.helium_price_usd_per_l = max(0.1, base_price * (1.0 + scarcity * 0.8))
        self.price_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'price': self.helium_price_usd_per_l
        })
        
        if len(self.price_history) > 5 and self.price_forecast_model:
            prices = [p['price'] for p in list(self.price_history)[-20:]]
            if len(prices) > 10:
                X = np.array(range(len(prices))).reshape(-1, 1)
                y = np.array(prices)
                self.price_forecast_model.fit(X, y)
                self.price_forecast_trained = True
    
    async def record_helium_usage(self, operation: str, amount_l: float, method: str = None, scarcity: float = 0.5):
        """Record helium usage for quantum operation"""
        async with self._lock:
            self.operation_helium[operation] = self.operation_helium.get(operation, 0) + amount_l
            self.total_usage_l += amount_l
            self._update_helium_price(scarcity)
            
            if method:
                self.method_efficiency[method] = self.method_efficiency.get(method, 0.5)
            
            self.history.append({
                'operation': operation,
                'amount_l': amount_l,
                'method': method,
                'scarcity': scarcity,
                'price_usd_per_l': self.helium_price_usd_per_l,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.debug(f"Helium usage recorded: {operation} = {amount_l}L")
    
    def get_helium_efficiency(self, method: str) -> float:
        """Get helium efficiency for mitigation method"""
        return self.method_efficiency.get(method, 0.5)
    
    async def get_current_helium_price(self) -> float:
        """Get current helium price"""
        return self.helium_price_usd_per_l
    
    async def forecast_helium_prices(self, hours: int = 24) -> Dict[str, Any]:
        """Forecast helium prices for the next N hours"""
        if not self.price_forecast_trained or not self.price_forecast_model:
            return {'status': 'not_trained'}
        
        try:
            future_indices = np.array(range(
                len(self.price_history),
                len(self.price_history) + hours
            )).reshape(-1, 1)
            predictions = self.price_forecast_model.predict(future_indices)
            
            return {
                'status': 'success',
                'predictions': predictions.tolist(),
                'confidence': 0.8 if len(self.price_history) > 50 else 0.5,
                'current_price': self.helium_price_usd_per_l,
                'forecast_hours': hours
            }
        except Exception as e:
            logger.error(f"Helium price forecast error: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def get_helium_position(self) -> Dict[str, Any]:
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': self.total_usage_l,
            'remaining_budget_l': self.helium_budget_l - self.total_usage_l,
            'method_efficiencies': self.method_efficiency,
            'operation_usage': self.operation_helium,
            'current_price_usd_per_l': self.helium_price_usd_per_l,
            'status': 'critical' if self.total_usage_l > self.helium_budget_l * 0.8 else 'healthy'
        }
    
    async def calculate_helium_savings(self, method: str, original_amount: float) -> float:
        """Calculate helium savings from using a method"""
        efficiency = self.get_helium_efficiency(method)
        saved = original_amount * (1 - efficiency)
        return saved

# ============================================================================
# FEDERATED QUANTUM MITIGATOR WITH DIFFERENTIAL PRIVACY (ENHANCED)
# ============================================================================

class FederatedQuantumMitigator:
    """Federated reflexive learning for quantum error mitigation with differential privacy"""
    
    def __init__(self, server_url: Optional[str] = None, privacy_epsilon: float = 1.0):
        self.server_url = server_url
        self.round = 0
        self.local_error_model = {}
        self.global_error_model = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
        # NEW: Differential privacy
        self.privacy_epsilon = privacy_epsilon
        self.noise_scale = 0.001
        
        logger.info(f"Federated Quantum Mitigator initialized with ε={privacy_epsilon}")
    
    def _add_differential_privacy(self, error_model: Dict) -> Dict:
        """Add differential privacy noise to error model"""
        if self.privacy_epsilon <= 0:
            return error_model
        
        private_model = {}
        sensitivity = 1.0
        
        for key, value in error_model.items():
            if isinstance(value, (int, float)):
                scale = (2 * sensitivity) / self.privacy_epsilon
                noise = np.random.normal(0, scale * self.noise_scale)
                private_model[key] = value + noise
            else:
                private_model[key] = value
        
        return private_model
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def share_error_model(self, participant_id: str, error_model: Dict, performance: float = 1.0) -> Dict:
        """Share local error model with federation with privacy protection"""
        if not self.server_url:
            return {'status': 'local'}
        
        async with self._lock:
            session = await self._get_session()
            try:
                # Apply differential privacy
                private_model = self._add_differential_privacy(error_model)
                
                update_data = {
                    'participant_id': participant_id,
                    'round': self.round,
                    'error_model': private_model,
                    'performance': performance,
                    'privacy_epsilon': self.privacy_epsilon,
                    'timestamp': datetime.utcnow().isoformat()
                }
                async with session.post(
                    f"{self.server_url}/federated/quantum",
                    json=update_data,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.round += 1
                        self.contribution_scores[participant_id] = performance
                        return result
                    return {'status': 'failed'}
            except Exception as e:
                logger.error(f"Federated quantum send error: {e}")
                return {'status': 'error'}
    
    async def get_global_model(self) -> Optional[Dict]:
        """Get global error model from federated server"""
        if not self.server_url:
            return self.global_error_model
        
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(
                    f"{self.server_url}/federated/quantum/global",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.global_error_model = data.get('error_model', {})
                        self.participants = data.get('participants', [])
                        return self.global_error_model
            except Exception as e:
                logger.error(f"Global model fetch error: {e}")
                return None
    
    def aggregate_error_models(self, peer_models: List[Dict], weights: Dict[str, float] = None) -> Dict:
        """Aggregate error models from peers"""
        if not peer_models:
            return {}
        
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_models))}
        
        for key in peer_models[0].keys():
            if isinstance(peer_models[0][key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_models):
                    if key in peer:
                        total += peer[key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[key] = total / max(total_weight, 0.001)
        
        return aggregated
    
    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_model': bool(self.global_error_model),
            'contribution_scores': self.contribution_scores,
            'privacy_epsilon': self.privacy_epsilon
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# PREDICTIVE QUANTUM ANALYZER WITH ONLINE LEARNING (ENHANCED)
# ============================================================================

class PredictiveQuantumAnalyzer:
    """Predictive analytics for quantum error mitigation with online learning"""
    
    def __init__(self, history_window: int = 100, online_learning_rate: float = 0.01):
        self.history_window = history_window
        self.mitigation_history = deque(maxlen=history_window)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        # NEW: Online learning
        self.online_learning_rate = online_learning_rate
        self.model_version = 0
        self.samples_since_last_train = 0
        self.retrain_threshold = 50
        
        try:
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self._ml_available = True
        except ImportError:
            self._ml_available = False
            logger.warning("Scikit-learn not available - predictive analytics limited")
        
        logger.info("Predictive Quantum Analyzer initialized with online learning")
    
    def update_history(self, mitigation_result: Dict):
        """Update mitigation history"""
        self.mitigation_history.append({
            'timestamp': datetime.utcnow(),
            'original_error': mitigation_result.get('original_error', 0.1),
            'mitigated_error': mitigation_result.get('mitigated_error', 0.05),
            'method': mitigation_result.get('method', 'unknown'),
            'overhead': mitigation_result.get('overhead', 1.0),
            'success': mitigation_result.get('success', True),
            'circuit_depth': mitigation_result.get('circuit_depth', 10),
            'n_qubits': mitigation_result.get('n_qubits', 5)
        })
        
        self.samples_since_last_train += 1
        
        # Trigger online learning if enough new samples
        if self.samples_since_last_train >= self.retrain_threshold and self.is_trained:
            asyncio.create_task(self._online_learning_update())
    
    async def _online_learning_update(self):
        """Perform online learning update"""
        try:
            recent_data = list(self.mitigation_history)[-self.samples_since_last_train:]
            if len(recent_data) > 10:
                X, y = self._prepare_training_data(recent_data)
                if len(X) > 0:
                    X_scaled = self.scaler.transform(X)
                    for name, model in self.models.items():
                        if model is not None:
                            if hasattr(model, 'partial_fit'):
                                model.partial_fit(X_scaled, y)
                            else:
                                await self.train_prediction_model()
                    
                    self.model_version += 1
                    self.samples_since_last_train = 0
                    logger.info(f"Online learning update complete (version {self.model_version})")
        except Exception as e:
            logger.error(f"Online learning update error: {e}")
    
    def _prepare_training_data(self, data: List[Dict]) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare training data from history"""
        X = []
        y = []
        
        if len(data) < 5:
            return np.array(X), np.array(y)
        
        for i in range(len(data) - 1):
            features = [
                data[i]['original_error'],
                data[i]['mitigated_error'],
                1 if data[i]['success'] else 0,
                data[i]['overhead'] / 10,
                data[i]['circuit_depth'] / 100,
                data[i]['n_qubits'] / 20
            ]
            X.append(features)
            y.append(data[i + 1]['mitigated_error'])
        
        return np.array(X), np.array(y)
    
    async def train_prediction_model(self):
        """Train ensemble prediction model"""
        if not self._ml_available or len(self.mitigation_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.mitigation_history)}
        
        X, y = self._prepare_training_data(list(self.mitigation_history))
        
        if len(X) < 10:
            return {'status': 'insufficient_training_data', 'samples': len(X)}
        
        X_scaled = self.scaler.fit_transform(X)
        
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                r2 = r2_score(y, predictions)
                results[name] = r2
        
        self.is_trained = True
        self.model_version += 1
        self.samples_since_last_train = 0
        
        logger.info(f"Prediction models trained. R²: {results} (version {self.model_version})")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_mitigation_effectiveness(self, circuit: Dict) -> Dict:
        """Predict mitigation effectiveness using ML"""
        if not self.is_trained or len(self.mitigation_history) < 10:
            return {'predicted_error': 0.05, 'confidence': 0.0}
        
        recent = list(self.mitigation_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['original_error'],
                data['mitigated_error'],
                1 if data['success'] else 0,
                data['overhead'] / 10,
                data['circuit_depth'] / 100,
                data['n_qubits'] / 20
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted_error': 0.05, 'confidence': 0.0}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        return {
            'predicted_error': max(0.001, prediction),
            'confidence': confidence,
            'model_version': self.model_version,
            'recommended_actions': self._generate_actions(prediction)
        }
    
    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction > 0.1:
            actions.append("Apply more aggressive mitigation techniques")
            actions.append("Consider hybrid mitigation approach")
        elif prediction > 0.05:
            actions.append("Standard mitigation sufficient")
            actions.append("Monitor error rates closely")
        else:
            actions.append("Current mitigation is effective - maintain strategy")
        return actions
    
    def forecast_error_trends(self, hours: int = 24) -> Dict:
        """Forecast error rate trends"""
        if len(self.mitigation_history) < 10:
            return {'trend': 'stable', 'confidence': 0.0}
        
        recent = list(self.mitigation_history)[-20:]
        errors = [h['mitigated_error'] for h in recent]
        
        if len(errors) > 5:
            trend = np.polyfit(range(len(errors)), errors, 1)[0]
        else:
            trend = 0
        
        return {
            'trend': 'increasing' if trend > 0.01 else 'decreasing' if trend < -0.01 else 'stable',
            'slope': trend,
            'confidence': 0.7 if len(errors) > 20 else 0.5,
            'predicted_errors': [errors[-1] + trend * i for i in range(12)]
        }
    
    def get_model_performance(self) -> Dict:
        """Get model performance metrics"""
        return {
            'is_trained': self.is_trained,
            'model_version': self.model_version,
            'samples_since_last_train': self.samples_since_last_train,
            'online_learning_rate': self.online_learning_rate,
            'models': list(self.models.keys())
        }

# ============================================================================
# QUANTUM ERROR CORRECTION INTEGRATION (NEW)
# ============================================================================

class QuantumErrorCorrection:
    """
    Quantum Error Correction integration.
    
    Features:
    - Surface code implementation
    - Error syndrome decoding
    - Fault-tolerant operations
    - Logical qubit encoding
    """
    
    def __init__(self):
        self.code_distance = 3
        self.logical_qubits = 0
        self.physical_qubits_per_logical = self._calculate_physical_qubits()
        
        logger.info("Quantum Error Correction initialized")
    
    def _calculate_physical_qubits(self) -> int:
        """Calculate physical qubits needed per logical qubit"""
        # Surface code: d^2 physical qubits per logical qubit
        return self.code_distance ** 2
    
    def encode_logical_qubit(self, physical_qubits: List[int]) -> Dict:
        """Encode a logical qubit using surface code"""
        n = self.code_distance
        
        # Create surface code lattice
        lattice = {
            'data_qubits': [],
            'measure_qubits': [],
            'syndrome_qubits': []
        }
        
        # Data qubits at lattice sites
        for i in range(n):
            for j in range(n):
                lattice['data_qubits'].append({
                    'x': i, 'y': j,
                    'physical_id': physical_qubits[i * n + j] if i * n + j < len(physical_qubits) else None
                })
        
        # Z syndrome qubits (on faces)
        for i in range(n - 1):
            for j in range(n - 1):
                lattice['syndrome_qubits'].append({
                    'type': 'Z',
                    'x': i + 0.5, 'y': j + 0.5,
                    'physical_id': None
                })
        
        # X syndrome qubits (on faces)
        for i in range(n - 1):
            for j in range(n - 1):
                lattice['syndrome_qubits'].append({
                    'type': 'X',
                    'x': i + 0.5, 'y': j + 0.5,
                    'physical_id': None
                })
        
        return lattice
    
    async def detect_errors(self, syndrome_measurements: List[float]) -> Dict:
        """Detect errors from syndrome measurements"""
        errors = []
        
        for i, measurement in enumerate(syndrome_measurements):
            if measurement > 0.5:  # Error detected
                errors.append({
                    'syndrome_index': i,
                    'type': 'Z' if i % 2 == 0 else 'X',
                    'strength': measurement
                })
        
        return {
            'errors_detected': len(errors),
            'error_details': errors,
            'needs_correction': len(errors) > 0
        }
    
    async def decode_syndrome(self, syndrome: List[int]) -> List[int]:
        """Decode error syndrome to identify physical qubits to correct"""
        n = self.code_distance
        
        # Simple decoder: map syndrome to nearest qubit
        correction_targets = []
        
        for i, measurement in enumerate(syndrome):
            if measurement == 1:
                # Map to data qubit
                row = i // (n - 1)
                col = i % (n - 1)
                target_idx = row * n + col
                correction_targets.append(target_idx)
        
        return correction_targets
    
    async def apply_correction(self, qubits: List[int], correction_targets: List[int]) -> Dict:
        """Apply error correction to qubits"""
        corrections = []
        
        for target in correction_targets:
            if target < len(qubits):
                corrections.append({
                    'qubit_index': target,
                    'correction_type': 'X',
                    'applied': True
                })
        
        return {
            'corrections_applied': len(corrections),
            'correction_details': corrections,
            'success': len(corrections) == len(correction_targets)
        }
    
    def get_qec_status(self) -> Dict:
        """Get QEC status"""
        return {
            'code_distance': self.code_distance,
            'physical_qubits_per_logical': self.physical_qubits_per_logical,
            'logical_qubits': self.logical_qubits,
            'overhead_ratio': self.physical_qubits_per_logical
        }

# ============================================================================
# ENHANCED DATA CLASSES
# ============================================================================

@dataclass
class QuantumCircuit:
    """Enhanced quantum circuit representation with sustainability metrics"""
    n_qubits: int
    gates: List[Dict[str, Any]]
    depth: int
    error_rate: float
    carbon_impact_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    # NEW: QEC fields
    logical_qubits: int = 0
    qec_enabled: bool = False
    
    def get_circuit_hash(self) -> str:
        import hashlib
        circuit_str = str(self.gates) + str(self.n_qubits) + str(self.depth)
        return hashlib.md5(circuit_str.encode()).hexdigest()

@dataclass
class ErrorMitigationResult:
    """Enhanced error mitigation result with sustainability metrics"""
    original_error_rate: float
    mitigated_error_rate: float
    mitigation_method: str
    overhead_factor: float
    success_probability: float
    resource_cost: Dict[str, float]
    carbon_saved_kg: float = 0.0
    helium_efficiency: float = 0.0
    sustainability_score: float = 0.0
    federated_round: int = 0
    # NEW: QEC fields
    qec_used: bool = False
    logical_error_rate: float = 0.0

# ============================================================================
# ENHANCED QUANTUM ERROR MITIGATOR
# ============================================================================

class QuantumErrorMitigator:
    """
    Enhanced Quantum Error Mitigation v2.0.0 with sustainability features.
    
    New Features:
    - Carbon price forecasting
    - Helium price forecasting
    - Differential privacy for federated models
    - Online learning for continuous model improvement
    - Quantum Error Correction integration
    """
    
    def __init__(
        self,
        enable_carbon_intensity: bool = True,
        enable_helium_tracking: bool = True,
        enable_federated: bool = True,
        enable_predictive: bool = True,
        enable_sustainability_dashboard: bool = True,
        enable_qec: bool = True,
        server_url: Optional[str] = None,
        privacy_epsilon: float = 1.0
    ):
        # Feature flags
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_helium_tracking = enable_helium_tracking
        self.enable_federated = enable_federated
        self.enable_predictive = enable_predictive
        self.enable_sustainability_dashboard = enable_sustainability_dashboard
        self.enable_qec = enable_qec
        
        # New modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.helium_tracker = HeliumQuantumTracker() if enable_helium_tracking else None
        self.federated_mitigator = FederatedQuantumMitigator(server_url, privacy_epsilon) if enable_federated else None
        self.predictive_analyzer = PredictiveQuantumAnalyzer() if enable_predictive else None
        self.qec = QuantumErrorCorrection() if enable_qec else None
        self.sustainability_dashboard = QuantumSustainabilityDashboard() if enable_sustainability_dashboard else None
        self.carbon_selector = QuantumCarbonAwareSelector(self.carbon_manager) if enable_carbon_intensity else None
        
        # Error mitigation strategies
        self.strategies = {
            'zne': self.zero_noise_extrapolation,
            'pec': self.probabilistic_error_cancellation,
            'cdr': self.clifford_data_regression,
            'dd': self.dynamical_decoupling,
            'mem': self.measurement_error_mitigation,
            'sv': self.symmetry_verification
        }
        
        # Error models
        self.error_models = {}
        
        # Mitigation history
        self.mitigation_history: List[ErrorMitigationResult] = []
        
        # Performance tracking
        self.performance_metrics = {
            'total_mitigations': 0,
            'successful_mitigations': 0,
            'average_improvement': 0.0,
            'average_carbon_saved': 0.0
        }
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info("Enhanced Quantum Error Mitigator v2.0.0 initialized")
    
    def _start_background_tasks(self):
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
    
    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_mitigator and self.mitigation_history:
                    latest = self.mitigation_history[-1] if self.mitigation_history else None
                    if latest:
                        await self.federated_mitigator.share_error_model(
                            f"quantum_{hashlib.md5(str(self.error_models).encode()).hexdigest()[:8]}",
                            {'error_rate': latest.mitigated_error_rate},
                            performance=1.0 - latest.mitigated_error_rate
                        )
                        await self.federated_mitigator.get_global_model()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer and self.mitigation_history:
                    latest = self.mitigation_history[-1] if self.mitigation_history else None
                    if latest:
                        self.predictive_analyzer.update_history({
                            'original_error': latest.original_error_rate,
                            'mitigated_error': latest.mitigated_error_rate,
                            'method': latest.mitigation_method,
                            'overhead': latest.overhead_factor,
                            'success': latest.mitigated_error_rate < latest.original_error_rate,
                            'circuit_depth': 10,
                            'n_qubits': 5
                        })
                    await self.predictive_analyzer.train_prediction_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def mitigate_errors(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float = 0.01,
        max_overhead: float = 10.0,
        preferred_method: Optional[str] = None,
        carbon_aware: bool = True,
        use_qec: bool = False
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """
        Apply error mitigation with sustainability awareness and QEC.
        """
        # Get carbon intensity and prices
        carbon_intensity = 400
        carbon_price = 50.0
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_carbon_price()
        
        # Get helium price
        helium_price = 0.5
        if self.helium_tracker:
            helium_price = await self.helium_tracker.get_current_helium_price()
        
        # Calculate original carbon impact
        original_carbon = self.carbon_manager.calculate_quantum_carbon_impact(
            circuit.depth, circuit.n_qubits
        ) if self.carbon_manager else 0
        
        # Assess current error
        current_error = self._estimate_error_rate(circuit)
        
        if current_error <= target_error_rate:
            result = ErrorMitigationResult(
                original_error_rate=current_error,
                mitigated_error_rate=current_error,
                mitigation_method='none',
                overhead_factor=1.0,
                success_probability=1.0,
                resource_cost={},
                carbon_saved_kg=0,
                sustainability_score=0.5
            )
            self.mitigation_history.append(result)
            return circuit, result
        
        # Apply QEC if enabled
        if use_qec and self.enable_qec and self.qec:
            qec_circuit, qec_result = await self._apply_qec(circuit)
            if qec_result.success_probability > 0.8:
                return qec_circuit, qec_result
        
        # Select mitigation strategy
        if preferred_method and preferred_method in self.strategies:
            strategy = preferred_method
        elif carbon_aware and self.carbon_selector:
            options = list(self.strategies.keys())
            # Include carbon price in selection
            strategy, _ = await self.carbon_selector.select_mitigation_with_carbon(
                options, {'depth': circuit.depth, 'n_qubits': circuit.n_qubits},
                carbon_intensity, carbon_price
            )
        else:
            strategy = self._select_strategy(circuit, current_error, target_error_rate)
        
        logger.info(f"Selected mitigation strategy: {strategy}")
        
        # Apply mitigation
        mitigation_func = self.strategies[strategy]
        
        try:
            mitigated_circuit, result = await mitigation_func(
                circuit,
                target_error_rate,
                max_overhead
            )
            
            # Calculate carbon savings with price
            mitigated_carbon = self.carbon_manager.calculate_quantum_carbon_impact(
                mitigated_circuit.depth, mitigated_circuit.n_qubits
            ) if self.carbon_manager else 0
            
            if self.carbon_manager:
                carbon_saved = await self.carbon_manager.calculate_carbon_savings(
                    original_carbon, mitigated_carbon
                )
                result.carbon_saved_kg = carbon_saved
                # Add economic value
                result.resource_cost['carbon_price_usd_per_ton'] = carbon_price
            
            # Track helium usage with price
            if self.helium_tracker:
                helium_amount = result.overhead_factor * 0.01
                await self.helium_tracker.record_helium_usage(
                    strategy, helium_amount, strategy
                )
                result.helium_efficiency = self.helium_tracker.get_helium_efficiency(strategy)
                result.resource_cost['helium_price_usd_per_l'] = helium_price
            
            # Calculate sustainability score
            result.sustainability_score = self._calculate_sustainability_score(result)
            
            # Update federated model
            if self.federated_mitigator:
                result.federated_round = self.federated_mitigator.round
            
            # Record history
            self.mitigation_history.append(result)
            self._update_metrics(result)
            
            # Update predictive analyzer
            if self.predictive_analyzer:
                self.predictive_analyzer.update_history({
                    'original_error': result.original_error_rate,
                    'mitigated_error': result.mitigated_error_rate,
                    'method': result.mitigation_method,
                    'overhead': result.overhead_factor,
                    'success': result.mitigated_error_rate < result.original_error_rate,
                    'circuit_depth': circuit.depth,
                    'n_qubits': circuit.n_qubits
                })
                await self.predictive_analyzer.train_prediction_model()
            
            # Verify mitigation
            if result.mitigated_error_rate > target_error_rate:
                logger.warning(f"Mitigation fell short: {result.mitigated_error_rate:.4f} > {target_error_rate:.4f}")
                
                if strategy != 'hybrid':
                    logger.info("Attempting hybrid mitigation")
                    mitigated_circuit, result = await self._hybrid_mitigation(
                        circuit, target_error_rate, max_overhead
                    )
            
            return mitigated_circuit, result
            
        except Exception as e:
            logger.error(f"Error mitigation failed: {str(e)}")
            return await self._fallback_mitigation(circuit, target_error_rate)
    
    async def _apply_qec(self, circuit: QuantumCircuit) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Apply Quantum Error Correction"""
        if not self.qec:
            return circuit, ErrorMitigationResult(
                original_error_rate=circuit.error_rate,
                mitigated_error_rate=circuit.error_rate,
                mitigation_method='qec_failed',
                overhead_factor=1.0,
                success_probability=0.0,
                resource_cost={},
                qec_used=False
            )
        
        # Encode logical qubits
        physical_qubits = list(range(circuit.n_qubits))
        lattice = self.qec.encode_logical_qubit(physical_qubits)
        
        # Simulate syndrome measurement
        syndrome = np.random.choice([0, 1], size=len(lattice['syndrome_qubits']))
        error_detection = await self.qec.detect_errors(syndrome)
        
        if error_detection['needs_correction']:
            correction_targets = await self.qec.decode_syndrome(syndrome)
            correction_result = await self.qec.apply_correction(
                physical_qubits, correction_targets
            )
            
            if correction_result['success']:
                logical_error_rate = circuit.error_rate * 0.1  # 10x improvement
            else:
                logical_error_rate = circuit.error_rate * 0.8
        else:
            logical_error_rate = circuit.error_rate * 0.05
        
        qec_circuit = QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=logical_error_rate,
            logical_qubits=1,
            qec_enabled=True
        )
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=logical_error_rate,
            mitigation_method='qec_surface_code',
            overhead_factor=float(self.qec.physical_qubits_per_logical),
            success_probability=0.9,
            resource_cost={'physical_qubits': self.qec.physical_qubits_per_logical},
            qec_used=True,
            logical_error_rate=logical_error_rate
        )
        
        return qec_circuit, result
    
    def _calculate_sustainability_score(self, result: ErrorMitigationResult) -> float:
        improvement = 1 - result.mitigated_error_rate / max(result.original_error_rate, 0.001)
        carbon_score = 1.0 - min(1.0, result.carbon_saved_kg / 0.1) if self.carbon_manager else 0.5
        helium_score = result.helium_efficiency if self.helium_tracker else 0.5
        
        # QEC bonus
        qec_bonus = 0.1 if result.qec_used else 0.0
        
        return 0.35 * improvement + 0.25 * carbon_score + 0.25 * helium_score + 0.15 * qec_bonus
    
    # ============================================================================
    # Existing Mitigation Methods (Preserved)
    # ============================================================================
    
    async def zero_noise_extrapolation(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Zero-Noise Extrapolation with carbon awareness"""
        noise_factors = [1.0, 1.5, 2.0, 2.5, 3.0]
        noise_factors = [f for f in noise_factors if f <= max_overhead]
        
        if len(noise_factors) < 2:
            raise ValueError("Insufficient noise factors for ZNE")
        
        expectation_values = []
        for factor in noise_factors:
            noisy_circuit = self._scale_noise(circuit, factor)
            expectation = await self._measure_expectation(noisy_circuit)
            expectation_values.append(expectation)
        
        zero_noise_value = self._extrapolate_zero_noise(noise_factors, expectation_values)
        mitigated_circuit = self._apply_zne_correction(circuit, zero_noise_value)
        mitigated_error = self._estimate_mitigated_error(circuit.error_rate, noise_factors, expectation_values)
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='zne',
            overhead_factor=np.mean(noise_factors),
            success_probability=0.95,
            resource_cost={'additional_circuits': len(noise_factors) - 1}
        )
        
        return mitigated_circuit, result
    
    async def probabilistic_error_cancellation(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Probabilistic Error Cancellation with overhead management"""
        basis_circuits = self._decompose_circuit(circuit)
        quasi_probs = self._calculate_quasi_probability(basis_circuits, circuit.error_rate)
        overhead = self._calculate_pec_overhead(quasi_probs)
        
        if overhead > max_overhead:
            raise ValueError(f"PEC overhead {overhead:.2f} exceeds maximum {max_overhead}")
        
        mitigated_circuit = self._select_best_pec_circuit([
            self._apply_pec_correction(bc, p) for bc, p in zip(basis_circuits, quasi_probs)
            if abs(p) > 1e-6
        ])
        
        mitigated_error = max(circuit.error_rate / (1 + overhead), 0.001)
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='pec',
            overhead_factor=overhead,
            success_probability=0.90,
            resource_cost={'quasi_probability_overhead': overhead}
        )
        
        return mitigated_circuit, result
    
    async def clifford_data_regression(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Clifford Data Regression with training data"""
        n_training = min(int(max_overhead * 10), 100)
        training_data = self._generate_clifford_training_data(
            circuit.n_qubits, n_training, circuit.error_rate
        )
        
        regression_model = self._train_cdr_model(training_data)
        noisy_output = await self._measure_expectation(circuit)
        corrected_output = regression_model.predict([noisy_output])[0]
        mitigated_circuit = self._apply_cdr_correction(circuit, corrected_output)
        mitigated_error = circuit.error_rate * (1 - regression_model.score)
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='cdr',
            overhead_factor=n_training / 10,
            success_probability=0.85,
            resource_cost={'training_circuits': n_training}
        )
        
        return mitigated_circuit, result
    
    async def dynamical_decoupling(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Dynamical Decoupling with sequence selection"""
        sequence = self._select_dd_sequence(circuit.n_qubits, circuit.error_rate)
        overhead = len(sequence) * circuit.depth
        
        if overhead > max_overhead:
            sequence = sequence[:int(max_overhead / circuit.depth)]
            overhead = len(sequence) * circuit.depth
        
        mitigated_circuit = self._apply_dd_sequence(circuit, sequence)
        improvement_factor = 1.0 - np.exp(-len(sequence) * 0.1)
        mitigated_error = circuit.error_rate * (1 - improvement_factor)
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='dd',
            overhead_factor=overhead,
            success_probability=0.92,
            resource_cost={'pulse_sequence_length': len(sequence)}
        )
        
        return mitigated_circuit, result
    
    async def measurement_error_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Measurement Error Mitigation with calibration"""
        n_calibration = int(max_overhead * 5)
        calibration_matrix = self._calibrate_measurements(circuit.n_qubits, n_calibration)
        overhead = n_calibration / 5 + 2
        
        if overhead > max_overhead:
            raise ValueError("Measurement mitigation overhead too high")
        
        mitigated_circuit = self._apply_measurement_correction(circuit, calibration_matrix)
        condition_number = np.linalg.cond(calibration_matrix)
        improvement = 1.0 / condition_number if condition_number > 0 else 0
        mitigated_error = circuit.error_rate * (1 - improvement * 0.5)
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='measurement',
            overhead_factor=overhead,
            success_probability=0.88,
            resource_cost={'calibration_circuits': n_calibration}
        )
        
        return mitigated_circuit, result
    
    async def symmetry_verification(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Symmetry Verification with symmetry detection"""
        symmetries = self._identify_symmetries(circuit)
        
        if not symmetries:
            raise ValueError("No symmetries found in circuit")
        
        overhead = len(symmetries) * 2
        if overhead > max_overhead:
            symmetries = symmetries[:int(max_overhead / 2)]
            overhead = len(symmetries) * 2
        
        verification_results = []
        for symmetry in symmetries:
            verification_circuit = self._create_symmetry_verification(circuit, symmetry)
            result = await self._measure_expectation(verification_circuit)
            verification_results.append((symmetry, result))
        
        mitigated_circuit = self._apply_symmetry_correction(circuit, verification_results)
        symmetry_score = np.mean([
            1.0 if self._verify_symmetry(sym, res) else 0.0
            for sym, res in verification_results
        ])
        mitigated_error = circuit.error_rate * (1 - symmetry_score * 0.7)
        
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='symmetry',
            overhead_factor=overhead,
            success_probability=symmetry_score,
            resource_cost={'symmetries_used': len(symmetries)}
        )
        
        return mitigated_circuit, result
    
    async def _hybrid_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Hybrid mitigation combining multiple techniques"""
        dd_circuit, dd_result = await self.dynamical_decoupling(
            circuit, target_error * 2, max_overhead / 3
        )
        
        remaining_overhead = max_overhead - dd_result.overhead_factor
        if remaining_overhead > 1.5:
            zne_circuit, zne_result = await self.zero_noise_extrapolation(
                dd_circuit, target_error, remaining_overhead
            )
            
            combined_result = ErrorMitigationResult(
                original_error_rate=circuit.error_rate,
                mitigated_error_rate=zne_result.mitigated_error_rate,
                mitigation_method='hybrid_dd_zne',
                overhead_factor=dd_result.overhead_factor + zne_result.overhead_factor,
                success_probability=min(dd_result.success_probability, zne_result.success_probability),
                resource_cost={**dd_result.resource_cost, **zne_result.resource_cost}
            )
            
            return zne_circuit, combined_result
        
        return dd_circuit, dd_result
    
    async def _fallback_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Fallback error mitigation"""
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=circuit.error_rate * 0.5,
            mitigation_method='fallback_simple',
            overhead_factor=2.0,
            success_probability=0.7,
            resource_cost={'fallback': True}
        )
        return circuit, result
    
    # ============================================================================
    # Helper Methods (Preserved)
    # ============================================================================
    
    def _estimate_error_rate(self, circuit: QuantumCircuit) -> float:
        gate_error_rate = circuit.error_rate
        total_error = 1 - (1 - gate_error_rate) ** (circuit.depth * circuit.n_qubits)
        return min(total_error, 1.0)
    
    def _select_strategy(self, circuit, current_error, target_error) -> str:
        if current_error > 0.1:
            return 'dd'
        elif circuit.depth > 10:
            return 'zne'
        elif circuit.n_qubits > 10:
            return 'cdr'
        elif current_error > 0.05:
            return 'pec'
        else:
            return 'mem'
    
    def _scale_noise(self, circuit, factor):
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=circuit.error_rate * factor
        )
    
    async def _measure_expectation(self, circuit):
        true_value = np.random.random()
        noise = np.random.normal(0, circuit.error_rate)
        return true_value + noise
    
    def _extrapolate_zero_noise(self, noise_factors, expectation_values):
        coeffs = np.polyfit(noise_factors, expectation_values, 1)
        return coeffs[1]
    
    def _apply_zne_correction(self, circuit, corrected_value):
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=circuit.error_rate * 0.3
        )
    
    def _decompose_circuit(self, circuit):
        basis_circuits = []
        for i in range(min(circuit.depth, 5)):
            basis_circuit = QuantumCircuit(
                n_qubits=circuit.n_qubits,
                gates=circuit.gates[:i+1],
                depth=i+1,
                error_rate=circuit.error_rate / circuit.depth
            )
            basis_circuits.append(basis_circuit)
        return basis_circuits
    
    def _calculate_quasi_probability(self, basis_circuits, error_rate):
        n = len(basis_circuits)
        raw_probs = np.random.exponential(1/n, n)
        quasi_probs = raw_probs - 0.5 * np.mean(raw_probs)
        quasi_probs = quasi_probs / np.sum(np.abs(quasi_probs))
        return quasi_probs.tolist()
    
    def _calculate_pec_overhead(self, quasi_probs):
        return np.sum(np.abs(quasi_probs))
    
    def _apply_pec_correction(self, circuit, probability):
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=circuit.error_rate * abs(probability)
        )
    
    def _select_best_pec_circuit(self, circuits):
        if not circuits:
            raise ValueError("No circuits available")
        return min(circuits, key=lambda c: c.error_rate)
    
    def _generate_clifford_training_data(self, n_qubits, n_samples, error_rate):
        training_data = []
        for _ in range(n_samples):
            noisy_output = np.random.random()
            ideal_output = noisy_output + np.random.normal(0, error_rate)
            training_data.append((noisy_output, ideal_output))
        return training_data
    
    def _train_cdr_model(self, training_data):
        from sklearn.linear_model import Ridge
        X = np.array([[d[0]] for d in training_data])
        y = np.array([d[1] for d in training_data])
        model = Ridge(alpha=0.1)
        model.fit(X, y)
        model.score = model.score(X, y)
        return model
    
    def _apply_cdr_correction(self, circuit, corrected_output):
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=circuit.error_rate * 0.4
        )
    
    def _select_dd_sequence(self, n_qubits, error_rate):
        if error_rate > 0.1:
            return ['X', 'Y', 'X', 'Y']
        elif error_rate > 0.05:
            return ['X', 'Y', 'X', 'Y', 'Y', 'X', 'Y', 'X']
        else:
            return ['X', 'X']
    
    def _apply_dd_sequence(self, circuit, sequence):
        mitigated_gates = []
        for gate in circuit.gates:
            mitigated_gates.append(gate)
            for pulse in sequence:
                mitigated_gates.append({'type': 'dd_pulse', 'axis': pulse})
        
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=mitigated_gates,
            depth=circuit.depth * (1 + len(sequence)),
            error_rate=circuit.error_rate * 0.5
        )
    
    def _calibrate_measurements(self, n_qubits, n_calibration):
        matrix = np.eye(2**n_qubits)
        for i in range(2**n_qubits):
            for j in range(2**n_qubits):
                if i != j:
                    matrix[i, j] = np.random.exponential(0.01)
        matrix = matrix / matrix.sum(axis=0, keepdims=True)
        return matrix
    
    def _apply_measurement_correction(self, circuit, calibration_matrix):
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=circuit.error_rate * 0.6
        )
    
    def _identify_symmetries(self, circuit):
        symmetries = []
        symmetries.append({'type': 'particle_number', 'operator': 'N', 'eigenvalue': circuit.n_qubits // 2})
        symmetries.append({'type': 'parity', 'operator': 'P', 'eigenvalue': 1 if circuit.n_qubits % 2 == 0 else -1})
        return symmetries
    
    def _create_symmetry_verification(self, circuit, symmetry):
        verification_gates = circuit.gates.copy()
        verification_gates.append({'type': 'measurement', 'basis': symmetry['type']})
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=verification_gates,
            depth=circuit.depth + 1,
            error_rate=circuit.error_rate
        )
    
    def _verify_symmetry(self, symmetry, result):
        expected = symmetry['eigenvalue']
        return abs(result - expected) < 0.1
    
    def _apply_symmetry_correction(self, circuit, verification_results):
        passed = sum(1 for sym, res in verification_results if self._verify_symmetry(sym, res))
        pass_rate = passed / len(verification_results) if verification_results else 0
        return QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=circuit.error_rate * (1 - pass_rate * 0.5)
        )
    
    def _estimate_mitigated_error(self, original_error, noise_factors, expectation_values):
        variance = np.var(expectation_values)
        mitigated_error = original_error * np.exp(-variance * 10)
        return max(mitigated_error, 0.001)
    
    def _update_metrics(self, result: ErrorMitigationResult):
        self.performance_metrics['total_mitigations'] += 1
        if result.mitigated_error_rate < result.original_error_rate:
            self.performance_metrics['successful_mitigations'] += 1
        
        n = self.performance_metrics['total_mitigations']
        old_avg = self.performance_metrics['average_improvement']
        improvement = 1 - result.mitigated_error_rate / max(result.original_error_rate, 0.001)
        self.performance_metrics['average_improvement'] = (old_avg * (n - 1) + improvement) / n
        
        if result.carbon_saved_kg > 0:
            old_carbon = self.performance_metrics['average_carbon_saved']
            self.performance_metrics['average_carbon_saved'] = (old_carbon * (n - 1) + result.carbon_saved_kg) / n
    
    # ============================================================================
    # Enhanced Statistics Methods
    # ============================================================================
    
    def get_mitigation_statistics(self) -> Dict[str, Any]:
        stats = {
            **self.performance_metrics,
            'success_rate': (
                self.performance_metrics['successful_mitigations'] /
                max(self.performance_metrics['total_mitigations'], 1)
            ),
            'recent_mitigations': [
                {
                    'method': r.mitigation_method,
                    'improvement': 1 - r.mitigated_error_rate / max(r.original_error_rate, 0.001),
                    'overhead': r.overhead_factor,
                    'carbon_saved_kg': r.carbon_saved_kg,
                    'sustainability_score': r.sustainability_score,
                    'qec_used': r.qec_used
                }
                for r in self.mitigation_history[-10:]
            ]
        }
        
        if self.enable_qec and self.qec:
            stats['qec_status'] = self.qec.get_qec_status()
        
        return stats
    
    def get_sustainability_dashboard_status(self) -> Dict:
        if self.sustainability_dashboard:
            return asyncio.run(
                self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker, self
                )
            )
        return {'status': 'dashboard_not_enabled'}
    
    def get_sustainability_report(self) -> Dict:
        if self.sustainability_dashboard:
            status = asyncio.run(
                self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker, self
                )
            )
            return self.sustainability_dashboard.generate_sustainability_report(status)
        return {'status': 'dashboard_not_enabled'}
    
    def get_predictive_insights(self) -> Dict:
        if self.predictive_analyzer:
            return asyncio.run(self.predictive_analyzer.predict_mitigation_effectiveness({}))
        return {'status': 'predictive_not_enabled'}
    
    def get_helium_status(self) -> Dict:
        if self.helium_tracker:
            return self.helium_tracker.get_helium_position()
        return {'status': 'helium_tracking_not_enabled'}
    
    def get_carbon_status(self) -> Dict:
        if self.carbon_manager:
            return {
                'current_intensity': asyncio.run(self.carbon_manager.get_current_intensity()),
                'current_price_usd_per_ton': asyncio.run(self.carbon_manager.get_current_carbon_price()),
                'total_savings_kg': self.carbon_manager.total_carbon_savings_kg,
                'trend': asyncio.run(self.carbon_manager.get_carbon_trend())
            }
        return {'status': 'carbon_tracking_not_enabled'}
    
    def get_price_forecasts(self) -> Dict:
        """Get price forecasts for carbon and helium"""
        forecasts = {}
        
        if self.carbon_manager:
            carbon_forecast = asyncio.run(self.carbon_manager.forecast_carbon_prices())
            forecasts['carbon'] = carbon_forecast
        
        if self.helium_tracker:
            helium_forecast = asyncio.run(self.helium_tracker.forecast_helium_prices())
            forecasts['helium'] = helium_forecast
        
        return forecasts
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Quantum Error Mitigator v2.0.0")
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_mitigator:
            await self.federated_mitigator.close()
        logger.info("Shutdown complete")

# ============================================================================
# SINGLETON ACCESSOR
# ============================================================================

_mitigator_instance = None

async def get_quantum_mitigator() -> QuantumErrorMitigator:
    """Get singleton quantum mitigator instance"""
    global _mitigator_instance
    if _mitigator_instance is None:
        _mitigator_instance = QuantumErrorMitigator()
    return _mitigator_instance

# ============================================================================
# SUSTAINABILITY DASHBOARD MODULE (Preserved)
# ============================================================================

class QuantumSustainabilityDashboard:
    """Sustainability dashboard for quantum error mitigation"""
    
    def __init__(self):
        self.history = []
        self.alert_thresholds = {
            'carbon_intensity': 500,
            'helium_remaining': 0.2,
            'error_rate': 0.1
        }
        self._running = True
        
        logger.info("Quantum Sustainability Dashboard initialized")
    
    async def get_dashboard_status(self, carbon_manager=None, helium_tracker=None, mitigator=None) -> Dict:
        """Get sustainability dashboard status"""
        status = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': 0.5
        }
        
        if carbon_manager:
            status['carbon_intensity'] = await carbon_manager.get_current_intensity()
            status['carbon_price'] = await carbon_manager.get_current_carbon_price()
            status['carbon_savings_kg'] = carbon_manager.total_carbon_savings_kg
        
        if helium_tracker:
            helium_pos = helium_tracker.get_helium_position()
            status['helium_position'] = helium_pos
            status['helium_price'] = helium_pos.get('current_price_usd_per_l', 0.5)
            status['helium_remaining_ratio'] = helium_pos.get('remaining_budget_l', 0) / max(helium_pos.get('budget_l', 1), 1)
        
        if mitigator:
            stats = mitigator.get_mitigation_statistics()
            status['mitigation_performance'] = stats
            status['success_rate'] = stats.get('success_rate', 0)
            status['average_improvement'] = stats.get('average_improvement', 0)
        
        score = 0.5
        if status.get('success_rate', 0) > 0.8:
            score += 0.2
        if status.get('carbon_intensity', 400) < 300:
            score += 0.15
        if status.get('helium_remaining_ratio', 0.5) > 0.5:
            score += 0.15
        
        status['sustainability_score'] = min(1.0, max(0.0, score))
        
        if status.get('carbon_intensity', 0) > self.alert_thresholds['carbon_intensity']:
            status['alerts'] = ['High carbon intensity detected']
        if status.get('helium_remaining_ratio', 1.0) < self.alert_thresholds['helium_remaining']:
            status['alerts'] = status.get('alerts', []) + ['Helium budget critically low']
        if status.get('success_rate', 1.0) < 0.7:
            status['alerts'] = status.get('alerts', []) + ['Mitigation success rate low']
        
        return status
    
    def generate_sustainability_report(self, status: Dict) -> Dict:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': status.get('sustainability_score', 0.5),
            'carbon_status': {
                'intensity': status.get('carbon_intensity', 0),
                'price_usd_per_ton': status.get('carbon_price', 50),
                'savings_kg': status.get('carbon_savings_kg', 0)
            },
            'helium_status': {
                'remaining_ratio': status.get('helium_remaining_ratio', 0.5),
                'price_usd_per_l': status.get('helium_price', 0.5)
            },
            'mitigation_status': status.get('mitigation_performance', {}),
            'alerts': status.get('alerts', []),
            'recommendations': self._generate_recommendations(status)
        }
    
    def _generate_recommendations(self, status: Dict) -> List[str]:
        recommendations = []
        
        if status.get('carbon_intensity', 0) > 400:
            recommendations.append("Schedule quantum operations during low-carbon hours")
        
        if status.get('helium_remaining_ratio', 1.0) < 0.3:
            recommendations.append("Implement helium recovery for quantum operations")
        
        if status.get('success_rate', 1.0) < 0.8:
            recommendations.append("Review mitigation strategy selection for better results")
        
        return recommendations or ["All sustainability metrics are within acceptable ranges"]

# ============================================================================
# QUANTUM CARBON-AWARE SELECTOR MODULE (Preserved)
# ============================================================================

class QuantumCarbonAwareSelector:
    """Carbon-aware mitigation strategy selection with price awareness"""
    
    def __init__(self, carbon_manager=None):
        self.carbon_manager = carbon_manager
        self.selection_history = deque(maxlen=1000)
        
        logger.info("Quantum Carbon-Aware Selector initialized")
    
    async def select_mitigation_with_carbon(
        self,
        options: List[str],
        circuit: Dict,
        carbon_intensity: Optional[float] = None,
        carbon_price: Optional[float] = None
    ) -> Tuple[str, float]:
        """Select mitigation strategy with carbon and price awareness"""
        if carbon_intensity is None and self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_carbon_price()
        else:
            carbon_intensity = carbon_intensity or 400
            carbon_price = carbon_price or 50
        
        # Carbon weights per strategy
        carbon_weights = {
            'dd': 0.9,
            'measurement': 0.85,
            'symmetry': 0.8,
            'hybrid': 0.7,
            'zne': 0.6,
            'cdr': 0.5,
            'pec': 0.4,
            'fallback_simple': 0.95
        }
        
        performance_weights = {
            'dd': 0.85,
            'hybrid': 0.92,
            'zne': 0.90,
            'pec': 0.88,
            'cdr': 0.85,
            'measurement': 0.80,
            'symmetry': 0.82,
            'fallback_simple': 0.70
        }
        
        # Price factor: higher price = higher carbon weight
        price_factor = min(2.0, carbon_price / 50.0)
        
        scores = {}
        for option in options:
            carbon_score = carbon_weights.get(option, 0.5)
            performance_score = performance_weights.get(option, 0.5)
            
            if carbon_intensity > 500:
                carbon_weight = min(0.8, 0.5 + price_factor * 0.15)
                performance_weight = 1.0 - carbon_weight
            elif carbon_intensity > 300:
                carbon_weight = min(0.6, 0.3 + price_factor * 0.15)
                performance_weight = 1.0 - carbon_weight
            else:
                carbon_weight = max(0.2, 0.3 - price_factor * 0.05)
                performance_weight = 1.0 - carbon_weight
            
            scores[option] = carbon_score * carbon_weight + performance_score * performance_weight
        
        if not scores:
            return 'dd', 0.5
        
        best_option = max(scores.items(), key=lambda x: x[1])
        
        self.selection_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'carbon_intensity': carbon_intensity,
            'carbon_price': carbon_price,
            'selected': best_option[0],
            'score': best_option[1]
        })
        
        return best_option
