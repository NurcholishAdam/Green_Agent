# enhancements/helium_scarcity_manager.py
"""
Helium Scarcity Manager v1.0.0
Real-time helium monitoring and constraint enforcement for sustainable scheduling
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import aiohttp
import numpy as np

logger = logging.getLogger(__name__)

@dataclass
class HeliumData:
    """Real-time helium market and scarcity data"""
    timestamp: datetime
    price_per_liter_usd: float
    scarcity_index: float  # 0-1, 1 = extreme scarcity
    supply_confidence: float  # 0-1
    projected_shortage_days: int
    region: str = "global"
    
    # Historical trends
    price_trend: str = "stable"  # increasing, decreasing, stable
    scarcity_trend: str = "stable"
    
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class HeliumConstraint:
    """Scheduling constraint based on helium availability"""
    constraint_id: str
    severity: str  # 'info', 'warning', 'critical', 'emergency'
    scarcity_threshold: float
    max_helium_usage_l: float
    recommended_actions: List[str]
    valid_until: datetime
    is_active: bool = True

class HeliumScarcityManager:
    """
    Manages helium scarcity tracking and constraint enforcement.
    
    Features:
    - Real-time price and scarcity monitoring
    - Predictive shortage forecasting
    - Automatic constraint generation
    - Job scheduling veto
    - Historical trend analysis
    - Alert system integration
    """
    
    def __init__(
        self,
        api_endpoint: str = "https://api.heliumprice.com/v1",
        update_interval: int = 300,  # 5 minutes
        scarcity_thresholds: Dict[str, float] = None
    ):
        self.api_endpoint = api_endpoint
        self.update_interval = update_interval
        
        # Default thresholds
        if scarcity_thresholds is None:
            scarcity_thresholds = {
                'info': 0.3,
                'warning': 0.5,
                'critical': 0.7,
                'emergency': 0.85
            }
        self.scarcity_thresholds = scarcity_thresholds
        
        # State
        self.current_helium_data: Optional[HeliumData] = None
        self.historical_data: deque = deque(maxlen=10000)
        self.active_constraints: List[HeliumConstraint] = []
        self.constraint_history: List[HeliumConstraint] = []
        
        # Predictive model (simplified)
        self.prediction_confidence = 0.0
        self.shortage_predictions: deque = deque(maxlen=100)
        
        # Alert system
        self.alerts: List[Dict] = []
        self._alert_callbacks = []
        
        self._lock = asyncio.Lock()
        self._session = None
        
        # Background update task
        self._update_task: Optional[asyncio.Task] = None
        
        logger.info("Helium Scarcity Manager initialized")
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def start_background_updates(self):
        """Start background monitoring of helium data"""
        if self._update_task is None:
            self._update_task = asyncio.create_task(self._background_update_loop())
            logger.info("Started background helium monitoring")
    
    async def _background_update_loop(self):
        """Background update loop for helium data"""
        while True:
            try:
                await self.update_helium_data()
                await self._update_constraints()
                await self._check_alerts()
            except Exception as e:
                logger.error(f"Error in background helium update: {e}")
            
            await asyncio.sleep(self.update_interval)
    
    async def update_helium_data(self, region: str = "global") -> HeliumData:
        """Fetch latest helium market data"""
        async with self._lock:
            session = await self._get_session()
            
            try:
                # Simulated API call - replace with real endpoint
                url = f"{self.api_endpoint}/current"
                params = {'region': region}
                
                async with session.get(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        helium_data = self._parse_helium_data(data)
                    else:
                        # Fallback: generate simulated data
                        helium_data = self._generate_simulated_data(region)
                        
                        # Record API failure
                        logger.warning(f"Helium API returned {response.status}, using simulation")
            except Exception as e:
                logger.error(f"Error fetching helium data: {e}")
                helium_data = self._generate_simulated_data(region)
            
            # Store data
            self.current_helium_data = helium_data
            self.historical_data.append(helium_data)
            
            # Update prediction confidence
            self._update_predictions()
            
            logger.info(f"Updated helium data: scarcity={helium_data.scarcity_index:.3f}, "
                       f"price=${helium_data.price_per_liter_usd:.2f}/L")
            
            return helium_data
    
    def _parse_helium_data(self, api_data: Dict[str, Any]) -> HeliumData:
        """Parse API response into HeliumData object"""
        return HeliumData(
            timestamp=datetime.fromisoformat(api_data.get('timestamp', datetime.utcnow().isoformat())),
            price_per_liter_usd=api_data.get('price', 0.5),
            scarcity_index=api_data.get('scarcity_index', 0.4),
            supply_confidence=api_data.get('confidence', 0.8),
            projected_shortage_days=api_data.get('shortage_days', 30),
            region=api_data.get('region', 'global'),
            price_trend=api_data.get('price_trend', 'stable'),
            scarcity_trend=api_data.get('scarcity_trend', 'stable'),
            metadata=api_data.get('metadata', {})
        )
    
    def _generate_simulated_data(self, region: str = "global") -> HeliumData:
        """Generate simulated helium data when API is unavailable"""
        # Simulate realistic fluctuations
        base_scarcity = 0.3
        base_price = 0.5
        
        # Add daily and weekly patterns
        hour = datetime.utcnow().hour
        day = datetime.utcnow().weekday()
        
        # Higher scarcity during business hours (more demand)
        time_factor = 0.1 * (1 + np.sin(hour / 12 * np.pi))
        
        # Seasonal variation
        season_factor = 0.05 * np.sin(datetime.utcnow().timetuple().tm_yday / 365 * 2 * np.pi)
        
        # Random noise
        noise = np.random.normal(0, 0.02)
        
        scarcity = min(1.0, max(0.0, base_scarcity + time_factor + season_factor + noise))
        price = base_price * (1 + scarcity * 0.8)
        
        return HeliumData(
            timestamp=datetime.utcnow(),
            price_per_liter_usd=price,
            scarcity_index=scarcity,
            supply_confidence=0.75 + np.random.random() * 0.2,
            projected_shortage_days=int(30 + scarcity * 60),
            region=region,
            price_trend=self._calculate_trend('price'),
            scarcity_trend=self._calculate_trend('scarcity')
        )
    
    def _calculate_trend(self, field: str) -> str:
        """Calculate trend from historical data"""
        if len(self.historical_data) < 5:
            return "stable"
        
        recent = list(self.historical_data)[-5:]
        values = [getattr(d, field) for d in recent]
        
        slope = np.polyfit(range(len(values)), values, 1)[0]
        
        if abs(slope) < 0.01:
            return "stable"
        elif slope > 0:
            return "increasing"
        else:
            return "decreasing"
    
    def _update_predictions(self):
        """Update shortage predictions based on historical data"""
        if len(self.historical_data) < 10:
            self.prediction_confidence = 0.0
            return
        
        # Simple autoregressive model for shortage prediction
        recent = list(self.historical_data)[-10:]
        scarcity_values = [d.scarcity_index for d in recent]
        
        # Fit simple AR(2) model
        if len(scarcity_values) >= 3:
            # y_t = a1*y_{t-1} + a2*y_{t-2} + c
            Y = np.array(scarcity_values[2:])
            X = np.column_stack([scarcity_values[1:-1], scarcity_values[:-2], np.ones(len(scarcity_values[2:]))])
            
            try:
                coeffs = np.linalg.lstsq(X, Y, rcond=None)[0]
                next_prediction = coeffs[0] * scarcity_values[-1] + coeffs[1] * scarcity_values[-2] + coeffs[2]
                
                self.shortage_predictions.append({
                    'predicted_scarcity': min(1.0, max(0.0, next_prediction)),
                    'timestamp': datetime.utcnow()
                })
                
                # Calculate confidence based on recent accuracy
                if len(self.shortage_predictions) > 5:
                    recent_predictions = list(self.shortage_predictions)[-5:]
                    errors = []
                    for i, pred in enumerate(recent_predictions[:-1]):
                        actual = recent_predictions[i+1].get('predicted_scarcity', 0)
                        predicted = pred.get('predicted_scarcity', 0)
                        errors.append(abs(actual - predicted) / (actual + 0.01))
                    
                    self.prediction_confidence = 1.0 - min(0.5, np.mean(errors))
                else:
                    self.prediction_confidence = 0.5
            except Exception:
                self.prediction_confidence = 0.0
    
    async def _update_constraints(self):
        """Update helium-based scheduling constraints"""
        if not self.current_helium_data:
            return
        
        async with self._lock:
            # Clear expired constraints
            self.active_constraints = [
                c for c in self.active_constraints
                if c.valid_until > datetime.utcnow()
            ]
            
            # Generate new constraints based on current scarcity
            scarcity = self.current_helium_data.scarcity_index
            
            # Determine severity
            severity = "info"
            if scarcity >= self.scarcity_thresholds['emergency']:
                severity = "emergency"
            elif scarcity >= self.scarcity_thresholds['critical']:
                severity = "critical"
            elif scarcity >= self.scarcity_thresholds['warning']:
                severity = "warning"
            
            # Generate constraint if severe enough
            if severity in ['warning', 'critical', 'emergency']:
                max_usage = self._calculate_max_helium_usage(severity)
                
                constraint = HeliumConstraint(
                    constraint_id=f"helium_{datetime.utcnow().timestamp()}",
                    severity=severity,
                    scarcity_threshold=self.scarcity_thresholds[severity],
                    max_helium_usage_l=max_usage,
                    recommended_actions=self._generate_recommendations(severity),
                    valid_until=datetime.utcnow() + timedelta(hours=1)
                )
                
                # Add if not already active (avoid duplicates)
                if not any(c.constraint_id == constraint.constraint_id for c in self.active_constraints):
                    self.active_constraints.append(constraint)
                    self.constraint_history.append(constraint)
                    
                    logger.warning(f"New helium constraint: {severity.upper()} - max {max_usage:.3f}L")
    
    def _calculate_max_helium_usage(self, severity: str) -> float:
        """Calculate maximum allowed helium usage based on severity"""
        if severity == "emergency":
            return 0.05  # 50mL
        elif severity == "critical":
            return 0.2   # 200mL
        elif severity == "warning":
            return 0.5   # 500mL
        else:
            return 1.0   # 1L
    
    def _generate_recommendations(self, severity: str) -> List[str]:
        """Generate recommended actions based on severity"""
        if severity == "emergency":
            return [
                "HALT ALL HELIUM-INTENSIVE OPERATIONS",
                "Switch to classical computation where possible",
                "Activate helium recovery systems",
                "Notify all operators of emergency"
            ]
        elif severity == "critical":
            return [
                "Reduce helium usage by 80%",
                "Schedule helium-intensive tasks for off-peak hours",
                "Increase recycling and recovery efficiency",
                "Consider alternative cooling methods"
            ]
        elif severity == "warning":
            return [
                "Reduce helium usage by 50%",
                "Optimize existing helium workflows",
                "Monitor helium consumption closely",
                "Prepare for potential shortages"
            ]
        else:
            return []
    
    async def _check_alerts(self):
        """Check if alerts need to be triggered"""
        if not self.current_helium_data:
            return
        
        scarcity = self.current_helium_data.scarcity_index
        
        # Check against thresholds
        for level, threshold in self.scarcity_thresholds.items():
            if scarcity >= threshold:
                # Check if alert already exists
                alert_exists = any(
                    a['level'] == level and 
                    a['timestamp'] > datetime.utcnow() - timedelta(minutes=30)
                    for a in self.alerts
                )
                
                if not alert_exists:
                    alert = {
                        'level': level.upper(),
                        'scarcity': scarcity,
                        'timestamp': datetime.utcnow(),
                        'message': f"Helium scarcity reached {level.upper()} level: {scarcity:.2f}",
                        'constraints': [c.constraint_id for c in self.active_constraints if c.severity == level]
                    }
                    self.alerts.append(alert)
                    
                    # Trigger callbacks
                    for callback in self._alert_callbacks:
                        try:
                            await callback(alert)
                        except Exception as e:
                            logger.error(f"Error in alert callback: {e}")
                    
                    logger.warning(f"Helium alert: {alert['level']} - {alert['message']}")
    
    def register_alert_callback(self, callback):
        """Register a callback for helium alerts"""
        self._alert_callbacks.append(callback)
    
    async def check_job_eligibility(
        self,
        job_id: str,
        helium_requirement_l: float,
        job_priority: str = "normal"
    ) -> Tuple[bool, List[str]]:
        """
        Check if a job can be scheduled based on helium constraints.
        
        Args:
            job_id: Job identifier
            helium_requirement_l: Required helium in liters
            job_priority: 'critical', 'normal', or 'low'
            
        Returns:
            (allowed, rejection_reasons)
        """
        if not self.current_helium_data:
            # If no data, be conservative
            return False, ["No helium data available - scheduling blocked"]
        
        scarcity = self.current_helium_data.scarcity_index
        reasons = []
        
        # Check active constraints
        for constraint in self.active_constraints:
            if not constraint.is_active:
                continue
            
            if helium_requirement_l > constraint.max_helium_usage_l:
                reasons.append(
                    f"Helium usage {helium_requirement_l:.3f}L exceeds "
                    f"{constraint.severity} limit {constraint.max_helium_usage_l:.3f}L"
                )
        
        # Critical jobs may bypass some constraints
        if job_priority == "critical" and scarcity < 0.9:
            # Allow critical jobs with higher limits
            if helium_requirement_l < 5.0:  # 5L emergency limit for critical
                return True, []
        
        if reasons:
            logger.info(f"Job {job_id} blocked: {', '.join(reasons)}")
            return False, reasons
        
        return True, []
    
    async def get_sustainability_forecast(self, days: int = 7) -> Dict[str, Any]:
        """Get sustainability forecast for helium usage"""
        if len(self.historical_data) < 5:
            return {'status': 'insufficient_data'}
        
        # Project future scarcity
        recent_data = list(self.historical_data)[-30:]
        scarcity_trend = np.polyfit(
            range(len(recent_data)),
            [d.scarcity_index for d in recent_data],
            1
        )[0]
        
        current_scarcity = self.current_helium_data.scarcity_index if self.current_helium_data else 0.3
        
        # Simple projection
        projections = []
        for i in range(days):
            projected = current_scarcity + scarcity_trend * (i + 1)
            projections.append(min(1.0, max(0.0, projected)))
        
        # Determine when critical threshold will be reached
        critical_threshold = self.scarcity_thresholds.get('critical', 0.7)
        days_to_critical = 0
        for i, projection in enumerate(projections):
            if projection >= critical_threshold:
                days_to_critical = i + 1
                break
        
        return {
            'current_scarcity': current_scarcity,
            'projected_trend': scarcity_trend,
            'days_to_critical': days_to_critical if days_to_critical > 0 else None,
            'projections': projections,
            'confidence': self.prediction_confidence,
            'recommendations': self._generate_forecast_recommendations(
                projections, days_to_critical
            )
        }
    
    def _generate_forecast_recommendations(
        self,
        projections: List[float],
        days_to_critical: int
    ) -> List[str]:
        """Generate recommendations based on forecast"""
        recommendations = []
        
        if days_to_critical is None:
            recommendations.append("Helium supply appears stable for the forecast period")
        elif days_to_critical <= 1:
            recommendations.append("IMMEDIATE ACTION REQUIRED: Critical helium shortage imminent")
            recommendations.append("Halt all non-essential helium-consuming operations")
        elif days_to_critical <= 3:
            recommendations.append("URGENT: Helium shortage expected within 3 days")
            recommendations.append("Reduce helium usage by at least 50%")
            recommendations.append("Optimize all helium-consuming processes")
        elif days_to_critical <= 7:
            recommendations.append("Helium shortage expected within 7 days")
            recommendations.append("Begin transitioning to helium-efficient operations")
            recommendations.append("Increase helium recovery and recycling")
        else:
            recommendations.append("Monitor helium trends - moderate shortage risk")
        
        return recommendations
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive helium statistics"""
        return {
            'current': {
                'scarcity_index': self.current_helium_data.scarcity_index if self.current_helium_data else None,
                'price_usd_per_l': self.current_helium_data.price_per_liter_usd if self.current_helium_data else None,
                'supply_confidence': self.current_helium_data.supply_confidence if self.current_helium_data else None,
                'projected_shortage_days': self.current_helium_data.projected_shortage_days if self.current_helium_data else None,
                'price_trend': self.current_helium_data.price_trend if self.current_helium_data else None,
                'scarcity_trend': self.current_helium_data.scarcity_trend if self.current_helium_data else None
            },
            'constraints': {
                'active': len(self.active_constraints),
                'history': len(self.constraint_history),
                'active_constraints': [
                    {
                        'severity': c.severity,
                        'max_usage_l': c.max_helium_usage_l,
                        'valid_until': c.valid_until.isoformat()
                    }
                    for c in self.active_constraints
                ]
            },
            'alerts': {
                'total': len(self.alerts),
                'recent': [
                    {
                        'level': a['level'],
                        'scarcity': a['scarcity'],
                        'timestamp': a['timestamp'].isoformat()
                    }
                    for a in self.alerts[-5:]
                ]
            },
            'prediction': {
                'confidence': self.prediction_confidence,
                'samples': len(self.shortage_predictions)
            },
            'historical': {
                'samples': len(self.historical_data),
                'min_scarcity': min([d.scarcity_index for d in self.historical_data]) if self.historical_data else None,
                'max_scarcity': max([d.scarcity_index for d in self.historical_data]) if self.historical_data else None,
                'avg_scarcity': np.mean([d.scarcity_index for d in self.historical_data]) if self.historical_data else None
            }
        }
    
    async def close(self):
        """Clean up resources"""
        if self._session:
            await self._session.close()
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
        logger.info("Helium Scarcity Manager closed")
