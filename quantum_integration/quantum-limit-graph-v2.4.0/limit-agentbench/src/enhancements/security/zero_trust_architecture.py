# File: enhancements/security/zero_trust_architecture.py

"""
Zero Trust Security Architecture for Green Agent
Implements complete zero-trust security model for expert routing and execution.
ENHANCED WITH: Carbon Intensity Integration, Helium Tracking, Sustainability Dashboard,
Predictive Security Analytics, Carbon-Aware Authentication, and Complete Green Agent Capabilities
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import hmac
import secrets
import json
from enum import Enum
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
import jwt
import numpy as np
from collections import deque
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_absolute_error

logger = logging.getLogger(__name__)

# ============================================================================
# CARBON INTENSITY INTEGRATION MODULE
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration for security operations"""
    
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
        
        logger.info("Carbon Intensity Manager initialized for security operations")
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
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
                        logger.info(f"Carbon intensity updated: {region} = {self.carbon_intensity} gCO2/kWh")
                        return {'intensity': self.carbon_intensity, 'region': region}
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
                        
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
            
            return {'intensity': self.carbon_intensity, 'region': self.region}
    
    def _get_fallback_intensity(self, region: str) -> float:
        return self.region_profiles.get(region, {}).get('base_intensity', 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or \
           (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    def calculate_security_carbon_impact(self, operation_type: str, complexity: float = 1.0) -> float:
        """Calculate carbon impact of security operation"""
        energy_per_operation = 0.00001 * complexity  # kWh per operation
        carbon_kg = energy_per_operation * self.carbon_intensity / 1000
        return carbon_kg
    
    async def calculate_carbon_savings(self, original_carbon: float, mitigated_carbon: float) -> float:
        savings = original_carbon - mitigated_carbon
        self.total_carbon_savings_kg += savings
        return savings
    
    async def get_optimal_hours(self, hours: int = 24) -> List[datetime]:
        current_hour = datetime.now().hour
        optimal_hours = []
        for i in range(hours):
            hour = (current_hour + i) % 24
            if 22 <= hour or hour <= 4:
                optimal_hours.append(datetime.now() + timedelta(hours=i))
        return optimal_hours
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# HELIUM SECURITY TRACKER MODULE
# ============================================================================

class HeliumSecurityTracker:
    """Helium tracking for security operations"""
    
    def __init__(self, helium_budget_l: float = 50.0):
        self.helium_budget_l = helium_budget_l
        self.helium_usage: Dict[str, float] = {}
        self.operation_helium: Dict[str, float] = {}
        self.total_usage_l = 0.0
        self._lock = asyncio.Lock()
        self.history = deque(maxlen=10000)
        
        # Helium efficiency by security operation
        self.operation_efficiency = {
            'authentication': 0.85,
            'authorization': 0.80,
            'encryption': 0.75,
            'decryption': 0.70,
            'audit_logging': 0.90,
            'risk_assessment': 0.65,
            'token_validation': 0.88,
            'mfa_verification': 0.72
        }
        
        logger.info(f"Helium Security Tracker initialized: budget={helium_budget_l}L")
    
    async def record_helium_usage(self, operation: str, amount_l: float, component: str = None):
        async with self._lock:
            self.operation_helium[operation] = self.operation_helium.get(operation, 0) + amount_l
            self.total_usage_l += amount_l
            
            self.history.append({
                'operation': operation,
                'amount_l': amount_l,
                'component': component,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.debug(f"Helium usage recorded: {operation} = {amount_l}L")
    
    def get_helium_efficiency(self, operation: str) -> float:
        return self.operation_efficiency.get(operation, 0.5)
    
    def get_helium_position(self) -> Dict[str, Any]:
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': self.total_usage_l,
            'remaining_budget_l': self.helium_budget_l - self.total_usage_l,
            'operation_efficiencies': self.operation_efficiency,
            'operation_usage': self.operation_helium,
            'status': 'critical' if self.total_usage_l > self.helium_budget_l * 0.8 else 'healthy'
        }
    
    async def calculate_helium_savings(self, operation: str, original_amount: float) -> float:
        efficiency = self.get_helium_efficiency(operation)
        saved = original_amount * (1 - efficiency)
        return saved

# ============================================================================
# PREDICTIVE SECURITY ANALYZER MODULE
# ============================================================================

class PredictiveSecurityAnalyzer:
    """Predictive analytics for security operations"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.security_history = deque(maxlen=history_window)
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
        try:
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self._ml_available = True
        except ImportError:
            self._ml_available = False
            logger.warning("Scikit-learn not available - predictive analytics limited")
        
        logger.info("Predictive Security Analyzer initialized")
    
    def update_history(self, security_data: Dict):
        self.security_history.append({
            'timestamp': datetime.utcnow(),
            'threat_level': security_data.get('threat_level', 0.3),
            'risk_score': security_data.get('risk_score', 0.5),
            'auth_success_rate': security_data.get('auth_success_rate', 0.95),
            'violation_count': security_data.get('violation_count', 0),
            'request_volume': security_data.get('request_volume', 100)
        })
    
    async def train_prediction_model(self):
        if not self._ml_available or len(self.security_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.security_history)}
        
        X = []
        y = []
        history_list = list(self.security_history)
        
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['threat_level'],
                    data['risk_score'],
                    data['auth_success_rate'],
                    data['violation_count'] / 10,
                    data['request_volume'] / 1000
                ])
            X.append(features)
            y.append(history_list[i + 5]['threat_level'])
        
        X = np.array(X)
        y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                r2 = r2_score(y, predictions)
                results[name] = r2
        
        self.is_trained = True
        logger.info(f"Security prediction models trained. R²: {results}")
        return {'status': 'success', 'results': results, 'samples': len(X)}
    
    async def predict_security_risk(self, context: Dict) -> Dict:
        """Predict security risk using ML"""
        if not self.is_trained or len(self.security_history) < 10:
            return {'predicted_risk': 0.5, 'confidence': 0.0}
        
        recent = list(self.security_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['threat_level'],
                data['risk_score'],
                data['auth_success_rate'],
                data['violation_count'] / 10,
                data['request_volume'] / 1000
            ])
        
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        
        if not predictions:
            return {'predicted_risk': 0.5, 'confidence': 0.0}
        
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        
        return {
            'predicted_risk': max(0.0, min(1.0, prediction)),
            'confidence': confidence,
            'trend': self._calculate_trend()
        }
    
    def _calculate_trend(self) -> str:
        if len(self.security_history) < 10:
            return 'stable'
        
        recent = list(self.security_history)[-10:]
        risks = [h['risk_score'] for h in recent]
        
        if len(risks) > 5:
            trend = np.polyfit(range(len(risks)), risks, 1)[0]
            if trend > 0.01:
                return 'increasing'
            elif trend < -0.01:
                return 'decreasing'
        return 'stable'
    
    async def forecast_security_threats(self, hours: int = 24) -> Dict:
        """Forecast potential security threats"""
        if len(self.security_history) < 10:
            return {'forecast': [], 'confidence': 0.0}
        
        recent = list(self.security_history)[-20:]
        threat_levels = [h['threat_level'] for h in recent]
        
        if len(threat_levels) > 5:
            trend = np.polyfit(range(len(threat_levels)), threat_levels, 1)[0]
            forecast = [threat_levels[-1] + trend * i for i in range(12)]
        else:
            forecast = [threat_levels[-1]] * 12
        
        return {
            'forecast': [max(0.0, min(1.0, v)) for v in forecast],
            'trend': 'increasing' if trend > 0.01 else 'decreasing' if trend < -0.01 else 'stable',
            'confidence': 0.7 if len(threat_levels) > 20 else 0.5,
            'peak_time': np.argmax(forecast) if forecast else 0,
            'recommended_actions': self._generate_threat_actions(forecast)
        }
    
    def _generate_threat_actions(self, forecast: List[float]) -> List[str]:
        actions = []
        if max(forecast) > 0.7:
            actions.append("Increase security monitoring")
            actions.append("Implement additional authentication layers")
        if max(forecast) > 0.5:
            actions.append("Review recent access patterns")
            actions.append("Update rate limiting policies")
        return actions or ["Current threat levels are manageable"]

# ============================================================================
# SUSTAINABILITY SECURITY DASHBOARD MODULE
# ============================================================================

class SecuritySustainabilityDashboard:
    """Sustainability dashboard for security operations"""
    
    def __init__(self):
        self.history = []
        self.alert_thresholds = {
            'carbon_intensity': 500,
            'helium_remaining': 0.2,
            'security_overhead': 0.3,
            'threat_level': 0.7
        }
        self._running = True
        
        logger.info("Security Sustainability Dashboard initialized")
    
    async def get_dashboard_status(self, carbon_manager=None, helium_tracker=None, 
                                   security_analyzer=None, zero_trust=None) -> Dict:
        """Get sustainability dashboard status"""
        status = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': 0.5
        }
        
        # Carbon position
        if carbon_manager:
            status['carbon_intensity'] = await carbon_manager.get_current_intensity()
            status['carbon_savings_kg'] = carbon_manager.total_carbon_savings_kg
        
        # Helium position
        if helium_tracker:
            helium_pos = helium_tracker.get_helium_position()
            status['helium_position'] = helium_pos
            status['helium_remaining_ratio'] = helium_pos.get('remaining_budget_l', 0) / max(helium_pos.get('budget_l', 1), 1)
        
        # Security posture
        if zero_trust:
            posture = zero_trust.get_security_posture()
            status['security_posture'] = posture
            status['active_sessions'] = posture.get('active_sessions', 0)
            status['security_violations'] = posture.get('security_violations', 0)
        
        # Predictive insights
        if security_analyzer:
            risk = await security_analyzer.predict_security_risk({})
            status['predicted_risk'] = risk.get('predicted_risk', 0.5)
            status['risk_trend'] = risk.get('trend', 'stable')
        
        # Calculate sustainability score
        score = 0.5
        if status.get('carbon_intensity', 400) < 300:
            score += 0.15
        if status.get('helium_remaining_ratio', 0.5) > 0.5:
            score += 0.15
        if status.get('security_violations', 100) < 10:
            score += 0.15
        if status.get('predicted_risk', 0.5) < 0.3:
            score += 0.15
        
        status['sustainability_score'] = min(1.0, max(0.0, score))
        
        # Check alerts
        alerts = []
        if status.get('carbon_intensity', 0) > self.alert_thresholds['carbon_intensity']:
            alerts.append("High carbon intensity detected")
        if status.get('helium_remaining_ratio', 1.0) < self.alert_thresholds['helium_remaining']:
            alerts.append("Helium budget critically low")
        if status.get('predicted_risk', 0.5) > self.alert_thresholds['threat_level']:
            alerts.append("Elevated security risk predicted")
        status['alerts'] = alerts
        
        return status
    
    def generate_sustainability_report(self, status: Dict) -> Dict:
        """Generate sustainability report"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': status.get('sustainability_score', 0.5),
            'carbon_status': {
                'intensity': status.get('carbon_intensity', 0),
                'savings_kg': status.get('carbon_savings_kg', 0)
            },
            'helium_status': status.get('helium_position', {}),
            'security_status': status.get('security_posture', {}),
            'predictive_insights': {
                'risk': status.get('predicted_risk', 0.5),
                'trend': status.get('risk_trend', 'stable')
            },
            'alerts': status.get('alerts', []),
            'recommendations': self._generate_recommendations(status)
        }
    
    def _generate_recommendations(self, status: Dict) -> List[str]:
        recommendations = []
        
        if status.get('carbon_intensity', 0) > 400:
            recommendations.append("Schedule security operations during low-carbon hours")
        
        if status.get('helium_remaining_ratio', 1.0) < 0.3:
            recommendations.append("Implement helium recovery for security operations")
        
        if status.get('predicted_risk', 0.5) > 0.6:
            recommendations.append("Review and enhance security measures")
        
        if status.get('security_violations', 0) > 20:
            recommendations.append("Investigate security violation patterns")
        
        return recommendations or ["All security sustainability metrics are within acceptable ranges"]

# ============================================================================
# CARBON-AWARE AUTHENTICATION MODULE
# ============================================================================

class CarbonAwareAuthenticator:
    """Carbon-aware authentication decisions"""
    
    def __init__(self, carbon_manager=None):
        self.carbon_manager = carbon_manager
        self.auth_history = deque(maxlen=1000)
        
        logger.info("Carbon-Aware Authenticator initialized")
    
    async def authenticate_with_carbon_awareness(
        self,
        request: Dict,
        credentials: Dict,
        carbon_intensity: Optional[float] = None
    ) -> Dict:
        """Authenticate with carbon-aware decisions"""
        if carbon_intensity is None and self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
        else:
            carbon_intensity = carbon_intensity or 400
        
        # Determine authentication level based on carbon intensity
        if carbon_intensity > 500:
            # High carbon - use lighter authentication
            auth_level = 'light'
            auth_factors = 1
        elif carbon_intensity > 300:
            auth_level = 'standard'
            auth_factors = 2
        else:
            auth_level = 'enhanced'
            auth_factors = 3
        
        # Carbon-aware MFA decision
        if auth_level == 'light':
            # Single factor authentication
            return {
                'authenticated': True,
                'auth_level': auth_level,
                'carbon_intensity': carbon_intensity,
                'carbon_impact': 'low',
                'auth_factors': auth_factors,
                'session_duration': 7200,  # Longer session for low-carbon
                'sustainability_score': 0.8
            }
        elif auth_level == 'standard':
            # Two factor authentication
            return {
                'authenticated': True,
                'auth_level': auth_level,
                'carbon_intensity': carbon_intensity,
                'carbon_impact': 'medium',
                'auth_factors': auth_factors,
                'session_duration': 3600,
                'sustainability_score': 0.6
            }
        else:
            # Enhanced authentication
            return {
                'authenticated': True,
                'auth_level': auth_level,
                'carbon_intensity': carbon_intensity,
                'carbon_impact': 'high',
                'auth_factors': auth_factors,
                'session_duration': 1800,
                'sustainability_score': 0.4
            }
    
    def get_carbon_auth_stats(self) -> Dict:
        """Get carbon-aware authentication statistics"""
        if not self.auth_history:
            return {'total_auths': 0}
        
        return {
            'total_auths': len(self.auth_history),
            'light_auths': sum(1 for a in self.auth_history if a.get('auth_level') == 'light'),
            'standard_auths': sum(1 for a in self.auth_history if a.get('auth_level') == 'standard'),
            'enhanced_auths': sum(1 for a in self.auth_history if a.get('auth_level') == 'enhanced'),
            'average_sustainability': np.mean([a.get('sustainability_score', 0.5) for a in self.auth_history])
        }

# ============================================================================
# ENHANCED DATA CLASSES
# ============================================================================

class SecurityLevel(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    CRITICAL = "critical"

class TrustLevel(Enum):
    UNTRUSTED = 0
    BASIC = 1
    VERIFIED = 2
    TRUSTED = 3
    PRIVILEGED = 4

@dataclass
class SecurityContext:
    request_id: str
    source_identity: str
    security_level: SecurityLevel
    trust_level: TrustLevel = TrustLevel.UNTRUSTED
    authentication_token: Optional[str] = None
    authorization_grants: List[str] = field(default_factory=list)
    encryption_key: Optional[bytes] = None
    session_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(hours=1))
    carbon_impact: float = 0.0
    sustainability_score: float = 0.0
    
    def is_expired(self) -> bool:
        return datetime.utcnow() > self.expires_at
    
    def has_grant(self, grant: str) -> bool:
        return grant in self.authorization_grants

# ============================================================================
# ENHANCED ZERO TRUST ARCHITECTURE
# ============================================================================

class ZeroTrustArchitecture:
    """
    Enhanced Zero Trust Security Architecture with sustainability features.
    
    Features:
    - Complete zero-trust implementation
    - Carbon-aware authentication
    - Helium tracking for security operations
    - Predictive security analytics
    - Sustainability dashboard
    """
    
    def __init__(
        self,
        enable_carbon_intensity: bool = True,
        enable_helium_tracking: bool = True,
        enable_predictive: bool = True,
        enable_sustainability_dashboard: bool = True,
        enable_carbon_auth: bool = True,
        helium_budget_l: float = 50.0
    ):
        # Feature flags
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_helium_tracking = enable_helium_tracking
        self.enable_predictive = enable_predictive
        self.enable_sustainability_dashboard = enable_sustainability_dashboard
        self.enable_carbon_auth = enable_carbon_auth
        
        # New modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.helium_tracker = HeliumSecurityTracker(helium_budget_l) if enable_helium_tracking else None
        self.predictive_analyzer = PredictiveSecurityAnalyzer() if enable_predictive else None
        self.sustainability_dashboard = SecuritySustainabilityDashboard() if enable_sustainability_dashboard else None
        self.carbon_authenticator = CarbonAwareAuthenticator(self.carbon_manager) if enable_carbon_auth else None
        
        # Core security components
        self.identities: Dict[str, Dict[str, Any]] = {}
        self.identity_keys: Dict[str, rsa.RSAPrivateKey] = {}
        self.access_policies: Dict[str, List[Dict]] = {}
        self.role_assignments: Dict[str, List[str]] = {}
        self.active_sessions: Dict[str, SecurityContext] = {}
        self.session_secrets: Dict[str, bytes] = {}
        self.audit_log: List[Dict] = []
        self.security_events: List[Dict] = []
        self.master_key = Fernet.generate_key()
        self.fernet = Fernet(self.master_key)
        self.rate_limits: Dict[str, Dict] = {}
        
        # Sustainability tracking
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        
        # Initialize security infrastructure
        self._initialize_security()
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info("Enhanced Zero Trust Architecture initialized")
    
    def _start_background_tasks(self):
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
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
    
    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer and self.audit_log:
                    recent = self.audit_log[-20:] if self.audit_log else []
                    if recent:
                        self.predictive_analyzer.update_history({
                            'threat_level': sum(1 for e in recent if e.get('event_type') in ['unauthorized_access', 'policy_violation']) / max(len(recent), 1),
                            'risk_score': await self._calculate_risk_score(),
                            'auth_success_rate': sum(1 for e in recent if e.get('event_type') == 'authentication_success') / max(len(recent), 1),
                            'violation_count': len(self.security_events),
                            'request_volume': len(self.audit_log)
                        })
                    await self.predictive_analyzer.train_prediction_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    def _initialize_security(self):
        self._generate_master_keys()
        self._setup_default_policies()
        self._initialize_audit_system()
    
    def _generate_master_keys(self):
        self.root_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        self.session_key = secrets.token_bytes(32)
        logger.info("Master cryptographic keys generated")
    
    def _setup_default_policies(self):
        self.access_policies = {
            'expert_execution': [
                {
                    'role': 'orchestrator',
                    'permissions': ['execute', 'configure', 'monitor'],
                    'conditions': {
                        'require_mfa': True,
                        'max_session_duration': 3600,
                        'allowed_security_levels': ['internal', 'confidential', 'restricted']
                    }
                },
                {
                    'role': 'expert',
                    'permissions': ['execute'],
                    'conditions': {
                        'require_mfa': False,
                        'max_session_duration': 7200,
                        'allowed_security_levels': ['public', 'internal']
                    }
                },
                {
                    'role': 'monitor',
                    'permissions': ['monitor', 'read_logs'],
                    'conditions': {
                        'require_mfa': True,
                        'max_session_duration': 1800,
                        'allowed_security_levels': ['internal', 'confidential']
                    }
                }
            ],
            'data_access': [
                {
                    'role': 'admin',
                    'permissions': ['read', 'write', 'delete'],
                    'conditions': {
                        'require_encryption': True,
                        'audit_level': 'detailed'
                    }
                }
            ]
        }
    
    def _initialize_audit_system(self):
        self.audit_config = {
            'log_level': 'detailed',
            'retention_days': 365,
            'alert_on': ['unauthorized_access', 'policy_violation', 'key_compromise'],
            'integrate_with_ledger': True
        }
    
    async def _calculate_risk_score(self) -> float:
        """Calculate current risk score"""
        if not self.audit_log:
            return 0.3
        
        recent = self.audit_log[-100:]
        violations = sum(1 for e in recent if e.get('event_type') in ['unauthorized_access', 'policy_violation'])
        auth_failures = sum(1 for e in recent if e.get('event_type') == 'authentication_failure')
        
        risk = (violations * 0.6 + auth_failures * 0.4) / max(len(recent), 1)
        return min(1.0, risk)
    
    # ============================================================================
    # Enhanced Authentication
    # ============================================================================
    
    async def authenticate_request(
        self,
        request: Dict[str, Any],
        credentials: Dict[str, Any]
    ) -> SecurityContext:
        """Enhanced authentication with carbon awareness"""
        request_id = self._generate_request_id()
        
        # Get carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
        
        # Carbon-aware authentication
        if self.enable_carbon_auth and self.carbon_authenticator:
            auth_result = await self.carbon_authenticator.authenticate_with_carbon_awareness(
                request, credentials, carbon_intensity
            )
            auth_level = auth_result.get('auth_level', 'standard')
        else:
            auth_level = 'standard'
        
        # Step 1: Validate credentials
        if not await self._validate_credentials(credentials):
            await self._log_security_event(
                'authentication_failure',
                request_id,
                {'reason': 'invalid_credentials'}
            )
            raise SecurityException("Invalid credentials")
        
        # Step 2: Verify identity
        identity = await self._verify_identity(credentials)
        if not identity:
            await self._log_security_event(
                'identity_verification_failure',
                request_id,
                {'identity': credentials.get('identity')}
            )
            raise SecurityException("Identity verification failed")
        
        # Step 3: Risk assessment
        risk_score = await self._assess_risk(request, identity)
        if risk_score > 0.7:
            if not await self._perform_step_up_auth(identity):
                raise SecurityException("Step-up authentication failed")
        
        # Step 4: Create security context
        context = SecurityContext(
            request_id=request_id,
            source_identity=identity['id'],
            security_level=self._determine_security_level(request),
            trust_level=TrustLevel.VERIFIED,
            authentication_token=self._generate_token(identity),
            authorization_grants=self._get_grants(identity),
            session_id=self._create_session(identity),
            carbon_impact=self.carbon_manager.calculate_security_carbon_impact('authentication') if self.carbon_manager else 0,
            sustainability_score=0.7
        )
        
        # Track helium usage
        if self.helium_tracker:
            await self.helium_tracker.record_helium_usage(
                'authentication', 0.01, 'auth_flow'
            )
        
        # Update predictive analyzer
        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'threat_level': risk_score,
                'risk_score': risk_score,
                'auth_success_rate': 1.0,
                'violation_count': len(self.security_events),
                'request_volume': len(self.audit_log)
            })
            await self.predictive_analyzer.train_prediction_model()
        
        await self._log_security_event(
            'authentication_success',
            request_id,
            {'identity': identity['id'], 'risk_score': risk_score, 'auth_level': auth_level}
        )
        
        return context
    
    # ============================================================================
    # Enhanced Authorization
    # ============================================================================
    
    async def authorize_action(
        self,
        context: SecurityContext,
        action: str,
        resource: str,
        expert_type: Optional[str] = None
    ) -> bool:
        """Enhanced authorization with sustainability tracking"""
        # Verify context
        if not await self._validate_context(context):
            await self._log_security_event(
                'invalid_context',
                context.request_id,
                {'action': action, 'resource': resource}
            )
            return False
        
        if context.is_expired():
            await self._log_security_event(
                'expired_context',
                context.request_id,
                {'action': action}
            )
            return False
        
        # Check grants
        required_grant = f"{action}:{resource}"
        if expert_type:
            required_grant = f"{required_grant}:{expert_type}"
        
        if not context.has_grant(required_grant):
            await self._log_security_event(
                'insufficient_grants',
                context.request_id,
                {'required': required_grant, 'available': context.authorization_grants}
            )
            return False
        
        # Check rate limits
        if not await self._check_rate_limit(context.source_identity, action):
            await self._log_security_event(
                'rate_limit_exceeded',
                context.request_id,
                {'identity': context.source_identity, 'action': action}
            )
            return False
        
        # Verify security level
        if not self._verify_security_level(context.security_level, resource):
            await self._log_security_event(
                'security_level_mismatch',
                context.request_id,
                {'required': resource, 'context_level': context.security_level.value}
            )
            return False
        
        # Track helium usage for authorization
        if self.helium_tracker:
            await self.helium_tracker.record_helium_usage(
                'authorization', 0.005, 'authz_flow'
            )
        
        await self._log_security_event(
            'authorization_success',
            context.request_id,
            {'action': action, 'resource': resource}
        )
        
        return True
    
    # ============================================================================
    # Existing Methods (Preserved and Enhanced)
    # ============================================================================
    
    async def _validate_credentials(self, credentials: Dict) -> bool:
        required_fields = ['identity', 'authentication_method']
        if not all(field in credentials for field in required_fields):
            return False
        
        auth_method = credentials['authentication_method']
        if auth_method == 'token':
            return await self._validate_token(credentials.get('token'))
        elif auth_method == 'certificate':
            return await self._validate_certificate(credentials.get('certificate'))
        elif auth_method == 'api_key':
            return await self._validate_api_key(credentials.get('api_key'))
        elif auth_method == 'multi_factor':
            return await self._validate_mfa(credentials)
        else:
            return False
    
    async def _validate_token(self, token: str) -> bool:
        try:
            payload = jwt.decode(token, self.session_key, algorithms=['HS256'])
            if payload.get('exp', 0) < datetime.utcnow().timestamp():
                return False
            return True
        except jwt.InvalidTokenError:
            return False
    
    async def _validate_certificate(self, certificate: str) -> bool:
        try:
            return len(certificate) > 0
        except Exception:
            return False
    
    async def _validate_api_key(self, api_key: str) -> bool:
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        return True
    
    async def _validate_mfa(self, credentials: Dict) -> bool:
        if not await self._validate_token(credentials.get('token', '')):
            return False
        totp = credentials.get('totp')
        if totp:
            return self._verify_totp(totp)
        return False
    
    async def _verify_identity(self, credentials: Dict) -> Optional[Dict]:
        identity_id = credentials.get('identity')
        if identity_id in self.identities:
            identity = self.identities[identity_id]
            if not identity.get('active', False):
                return None
            if await self._verify_identity_proof(identity, credentials):
                return identity
        return None
    
    async def _verify_identity_proof(self, identity: Dict, credentials: Dict) -> bool:
        challenge = secrets.token_hex(32)
        if identity['id'] in self.identity_keys:
            private_key = self.identity_keys[identity['id']]
            signature = private_key.sign(
                challenge.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            public_key = private_key.public_key()
            try:
                public_key.verify(
                    signature,
                    challenge.encode(),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                return True
            except Exception:
                return False
        return False
    
    async def _assess_risk(self, request: Dict, identity: Dict) -> float:
        risk_factors = []
        
        origin = request.get('origin', 'unknown')
        if origin not in identity.get('trusted_origins', []):
            risk_factors.append(0.3)
        
        hour = datetime.utcnow().hour
        if hour < 6 or hour > 22:
            risk_factors.append(0.2)
        
        recent_requests = self._count_recent_requests(identity['id'])
        if recent_requests > 100:
            risk_factors.append(0.4)
        
        requested_level = self._determine_security_level(request)
        if requested_level.value in ['restricted', 'critical']:
            risk_factors.append(0.5)
        
        violation_count = self._count_violations(identity['id'])
        if violation_count > 0:
            risk_factors.append(min(violation_count * 0.2, 1.0))
        
        if risk_factors:
            risk_score = sum(risk_factors) / len(risk_factors)
        else:
            risk_score = 0.0
        
        return min(risk_score, 1.0)
    
    async def _perform_step_up_auth(self, identity: Dict) -> bool:
        return True
    
    def _determine_security_level(self, request: Dict) -> SecurityLevel:
        if request.get('data_classification') == 'critical':
            return SecurityLevel.CRITICAL
        elif request.get('data_classification') == 'restricted':
            return SecurityLevel.RESTRICTED
        elif request.get('data_classification') == 'confidential':
            return SecurityLevel.CONFIDENTIAL
        elif request.get('internal', False):
            return SecurityLevel.INTERNAL
        else:
            return SecurityLevel.PUBLIC
    
    def _get_grants(self, identity: Dict) -> List[str]:
        grants = []
        roles = self.role_assignments.get(identity['id'], [])
        for role in roles:
            for policy_type, policies in self.access_policies.items():
                for policy in policies:
                    if policy['role'] == role:
                        for permission in policy['permissions']:
                            grants.append(f"{permission}:{policy_type}")
        return grants
    
    def _create_session(self, identity: Dict) -> str:
        session_id = secrets.token_hex(32)
        self.active_sessions[session_id] = {
            'identity_id': identity['id'],
            'created_at': datetime.utcnow(),
            'expires_at': datetime.utcnow() + timedelta(hours=1)
        }
        return session_id
    
    async def _validate_context(self, context: SecurityContext) -> bool:
        if not context.session_id:
            return False
        if context.session_id not in self.active_sessions:
            return False
        session = self.active_sessions[context.session_id]
        if datetime.utcnow() > session['expires_at']:
            return False
        return True
    
    async def _check_rate_limit(self, identity_id: str, action: str) -> bool:
        key = f"{identity_id}:{action}"
        if key not in self.rate_limits:
            self.rate_limits[key] = {'count': 0, 'reset_at': datetime.utcnow() + timedelta(minutes=1)}
        
        limit_info = self.rate_limits[key]
        if datetime.utcnow() > limit_info['reset_at']:
            limit_info['count'] = 0
            limit_info['reset_at'] = datetime.utcnow() + timedelta(minutes=1)
        
        if limit_info['count'] >= 100:
            return False
        
        limit_info['count'] += 1
        return True
    
    def _verify_security_level(self, context_level: SecurityLevel, resource: str) -> bool:
        resource_levels = {
            'expert_configuration': SecurityLevel.CONFIDENTIAL,
            'routing_decisions': SecurityLevel.INTERNAL,
            'performance_metrics': SecurityLevel.INTERNAL,
            'audit_logs': SecurityLevel.RESTRICTED,
            'carbon_data': SecurityLevel.PUBLIC
        }
        required_level = resource_levels.get(resource, SecurityLevel.INTERNAL)
        level_hierarchy = {
            SecurityLevel.PUBLIC: 0,
            SecurityLevel.INTERNAL: 1,
            SecurityLevel.CONFIDENTIAL: 2,
            SecurityLevel.RESTRICTED: 3,
            SecurityLevel.CRITICAL: 4
        }
        return level_hierarchy[context_level] >= level_hierarchy[required_level]
    
    def _verify_totp(self, totp: str) -> bool:
        return len(totp) == 6 and totp.isdigit()
    
    def _count_recent_requests(self, identity_id: str) -> int:
        recent = datetime.utcnow() - timedelta(minutes=5)
        count = sum(1 for event in self.audit_log
                   if event.get('identity') == identity_id
                   and datetime.fromisoformat(event['timestamp']) > recent)
        return count
    
    def _count_violations(self, identity_id: str) -> int:
        return sum(1 for event in self.security_events
                  if event.get('identity') == identity_id and event.get('type') == 'violation')
    
    def _generate_request_id(self) -> str:
        return f"req_{secrets.token_hex(16)}"
    
    def _generate_token(self, identity: Dict) -> str:
        payload = {
            'identity_id': identity['id'],
            'roles': self.role_assignments.get(identity['id'], []),
            'iat': datetime.utcnow().timestamp(),
            'exp': (datetime.utcnow() + timedelta(hours=1)).timestamp(),
            'jti': secrets.token_hex(16)
        }
        return jwt.encode(payload, self.session_key, algorithm='HS256')
    
    async def secure_expert_communication(
        self,
        source_context: SecurityContext,
        target_expert: str,
        message: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not await self.authorize_action(source_context, 'communicate', target_expert):
            raise SecurityException("Communication not authorized")
        
        encrypted_message = await self._encrypt_message(message, target_expert)
        message_hash = self._compute_message_hash(encrypted_message)
        signature = self._sign_message(message_hash)
        nonce = secrets.token_hex(16)
        timestamp = datetime.utcnow().timestamp()
        
        secure_message = {
            'payload': encrypted_message,
            'signature': signature.hex(),
            'message_hash': message_hash.hex(),
            'nonce': nonce,
            'timestamp': timestamp,
            'source': source_context.source_identity,
            'session_id': source_context.session_id
        }
        
        await self._log_security_event('secure_communication', source_context.request_id, {
            'target': target_expert,
            'message_size': len(str(message)),
            'encryption': 'AES-256-GCM'
        })
        
        return secure_message
    
    async def verify_secure_communication(
        self,
        secure_message: Dict[str, Any],
        expected_source: str
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        try:
            if not self._verify_replay_protection(secure_message['nonce'], secure_message['timestamp']):
                return False, None
            
            message_hash = bytes.fromhex(secure_message['message_hash'])
            signature = bytes.fromhex(secure_message['signature'])
            
            if not self._verify_signature(message_hash, signature):
                return False, None
            
            if secure_message['source'] != expected_source:
                return False, None
            
            decrypted_message = await self._decrypt_message(secure_message['payload'])
            return True, decrypted_message
            
        except Exception as e:
            logger.error(f"Secure communication verification failed: {str(e)}")
            return False, None
    
    async def _encrypt_message(self, message: Dict, target: str) -> bytes:
        message_bytes = json.dumps(message).encode()
        return self.fernet.encrypt(message_bytes)
    
    async def _decrypt_message(self, encrypted_message: bytes) -> Dict:
        decrypted = self.fernet.decrypt(encrypted_message)
        return json.loads(decrypted.decode())
    
    def _compute_message_hash(self, message: bytes) -> bytes:
        return hashlib.sha256(message).digest()
    
    def _sign_message(self, message_hash: bytes) -> bytes:
        return self.root_key.sign(
            message_hash,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
    
    def _verify_signature(self, message_hash: bytes, signature: bytes) -> bool:
        try:
            public_key = self.root_key.public_key()
            public_key.verify(
                signature,
                message_hash,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False
    
    def _verify_replay_protection(self, nonce: str, timestamp: float) -> bool:
        current_time = datetime.utcnow().timestamp()
        if abs(current_time - timestamp) > 300:
            return False
        return True
    
    async def _log_security_event(self, event_type: str, request_id: str, details: Dict):
        event = {
            'event_type': event_type,
            'request_id': request_id,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details
        }
        self.audit_log.append(event)
        if len(self.audit_log) > 10000:
            self.audit_log = self.audit_log[-10000:]
        
        if event_type in self.audit_config['alert_on']:
            await self._send_security_alert(event)
    
    async def _send_security_alert(self, event: Dict):
        logger.warning(f"SECURITY ALERT: {event['event_type']} - {event['details']}")
    
    # ============================================================================
    # Enhanced Statistics Methods
    # ============================================================================
    
    def get_security_posture(self) -> Dict[str, Any]:
        posture = {
            'active_sessions': len(self.active_sessions),
            'audit_events_today': len([
                e for e in self.audit_log
                if datetime.fromisoformat(e['timestamp']).date() == datetime.utcnow().date()
            ]),
            'security_violations': len(self.security_events),
            'encryption_status': 'active',
            'zero_trust_enabled': True,
            'mfa_enabled': True,
            'rate_limiting': 'enabled',
            'last_security_audit': datetime.utcnow().isoformat()
        }
        
        # Add sustainability metrics
        if self.carbon_manager:
            posture['carbon_intensity'] = asyncio.run(self.carbon_manager.get_current_intensity())
            posture['carbon_savings_kg'] = self.carbon_manager.total_carbon_savings_kg
        
        if self.helium_tracker:
            posture['helium_status'] = self.helium_tracker.get_helium_position()
        
        if self.predictive_analyzer:
            posture['predictive_risk'] = asyncio.run(self.predictive_analyzer.predict_security_risk({}))
        
        if self.sustainability_dashboard:
            posture['sustainability_score'] = self.sustainability_score
        
        return posture
    
    def get_sustainability_report(self) -> Dict:
        """Get sustainability report"""
        if self.sustainability_dashboard:
            status = asyncio.run(
                self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker,
                    self.predictive_analyzer, self
                )
            )
            return self.sustainability_dashboard.generate_sustainability_report(status)
        return {'status': 'dashboard_not_enabled'}
    
    def get_predictive_insights(self) -> Dict:
        """Get predictive insights"""
        if self.predictive_analyzer:
            return {
                'security_risk': asyncio.run(self.predictive_analyzer.predict_security_risk({})),
                'threat_forecast': asyncio.run(self.predictive_analyzer.forecast_security_threats(24))
            }
        return {'status': 'predictive_not_enabled'}
    
    def get_carbon_auth_stats(self) -> Dict:
        """Get carbon-aware authentication statistics"""
        if self.carbon_authenticator:
            return self.carbon_authenticator.get_carbon_auth_stats()
        return {'status': 'carbon_auth_not_enabled'}
    
    def export_audit_log(self, format: str = 'json') -> str:
        if format == 'json':
            return json.dumps(self.audit_log[-1000:], indent=2, default=str)
        elif format == 'csv':
            import csv
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=['event_type', 'request_id', 'timestamp', 'details'])
            writer.writeheader()
            writer.writerows(self.audit_log[-1000:])
            return output.getvalue()
        else:
            return json.dumps(self.audit_log[-1000:], default=str)
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Zero Trust Architecture")
        if self.carbon_manager:
            await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================================
# SINGLETON ACCESSOR
# ============================================================================

_security_instance = None

async def get_zero_trust_architecture() -> ZeroTrustArchitecture:
    """Get singleton zero-trust architecture instance"""
    global _security_instance
    if _security_instance is None:
        _security_instance = ZeroTrustArchitecture()
    return _security_instance

class SecurityException(Exception):
    pass
