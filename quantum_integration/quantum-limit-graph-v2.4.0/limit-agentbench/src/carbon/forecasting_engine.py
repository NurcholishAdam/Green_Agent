"""
Carbon Forecasting Engine
==========================

Predicts future carbon intensity using time-series models.

Location: src/carbon/forecasting_engine.py
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import pickle
from pathlib import Path

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    logging.warning("Prophet not available. Install with: pip install prophet")

logger = logging.getLogger(__name__)


@dataclass
class CarbonForecast:
    """Single carbon intensity forecast point"""
    timestamp: datetime
    predicted_intensity: float  # gCO2/kWh
    lower_bound: float  # 5th percentile
    upper_bound: float  # 95th percentile
    confidence: float  # 0-1


@dataclass
class OptimalWindow:
    """Optimal execution window with lowest carbon"""
    start_time: datetime
    end_time: datetime
    avg_intensity: float  # gCO2/kWh
    min_intensity: float
    max_intensity: float
    carbon_savings_percent: float  # vs. immediate execution


class CarbonForecaster:
    """
    Forecasts grid carbon intensity using time-series models
    
    Supports:
    - Prophet (Facebook's time-series library)
    - ARIMA (fallback)
    - Persistence (simple baseline)
    """
    
    def __init__(
        self,
        region: str = "US-CA",
        model_type: str = "prophet",  # "prophet", "arima", "persistence"
        model_path: Optional[Path] = None
    ):
        self.region = region
        self.model_type = model_type
        self.model_path = model_path
        self.model = None
        self.historical_data: List[Tuple[datetime, float]] = []
        self.last_train_time: Optional[datetime] = None
        self.training_mae: float = 0.0
        
        # Model hyperparameters
        self.prophet_params = {
            "yearly_seasonality": True,
            "weekly_seasonality": True,
            "daily_seasonality": True,
            "changepoint_prior_scale": 0.05,
            "seasonality_prior_scale": 10.0
        }
        
        logger.info(f"Initialized carbon forecaster for region {region}")
    
    async def fetch_historical_data(
        self,
        days: int = 90,
        api_client = None
    ) -> pd.DataFrame:
        """
        Fetch historical carbon intensity data
        
        Args:
            days: Number of days of historical data
            api_client: Grid API client (WattTime, ElectricityMap, etc.)
        
        Returns:
            DataFrame with 'ds' (datetime) and 'y' (carbon intensity)
        """
        
        if api_client:
            # Fetch from API
            data = await api_client.get_historical_intensity(
                region=self.region,
                start=datetime.now() - timedelta(days=days),
                end=datetime.now()
            )
        else:
            # Generate synthetic data for demo
            logger.warning("No API client provided, generating synthetic data")
            data = self._generate_synthetic_data(days)
        
        # Convert to Prophet format
        df = pd.DataFrame(data, columns=['timestamp', 'intensity'])
        df['ds'] = pd.to_datetime(df['timestamp'])
        df['y'] = df['intensity']
        
        # Store historical data
        self.historical_data = list(zip(df['ds'], df['y']))
        
        return df[['ds', 'y']]
    
    def _generate_synthetic_data(self, days: int) -> List[Tuple[datetime, float]]:
        """Generate synthetic carbon intensity data for testing"""
        
        data = []
        start = datetime.now() - timedelta(days=days)
        
        for hour in range(days * 24):
            timestamp = start + timedelta(hours=hour)
            
            # Simulate daily pattern (higher during day, lower at night)
            hour_of_day = timestamp.hour
            daily_pattern = 100 * np.sin((hour_of_day - 6) * np.pi / 12) + 300
            
            # Add weekly pattern (higher on weekdays)
            day_of_week = timestamp.weekday()
            weekly_pattern = 50 if day_of_week < 5 else -50
            
            # Add random noise
            noise = np.random.normal(0, 30)
            
            intensity = max(50, daily_pattern + weekly_pattern + noise)
            data.append((timestamp, intensity))
        
        return data
    
    async def train(
        self,
        historical_df: Optional[pd.DataFrame] = None,
        days: int = 90
    ):
        """Train forecasting model on historical data"""
        
        if historical_df is None:
            historical_df = await self.fetch_historical_data(days=days)
        
        if len(historical_df) < 24:
            raise ValueError(f"Insufficient data: {len(historical_df)} points (need >= 24)")
        
        if self.model_type == "prophet":
            if not PROPHET_AVAILABLE:
                raise ImportError("Prophet not installed")
            
            self.model = Prophet(**self.prophet_params)
            self.model.fit(historical_df)
            
        elif self.model_type == "arima":
            # Placeholder for ARIMA
            from statsmodels.tsa.arima.model import ARIMA
            self.model = ARIMA(historical_df['y'], order=(2, 1, 2))
            self.model = self.model.fit()
            
        elif self.model_type == "persistence":
            # Simple persistence model (use last value)
            self.model = historical_df['y'].iloc[-1]
        
        self.last_train_time = datetime.now()
        
        # Calculate training accuracy
        self.training_mae = self._calculate_mae(historical_df)
        
        logger.info(
            f"Model trained on {len(historical_df)} data points "
            f"(MAE: {self.training_mae:.1f} gCO2/kWh)"
        )
    
    def _calculate_mae(self, df: pd.DataFrame) -> float:
        """Calculate mean absolute error on training data"""
        
        if self.model_type == "prophet":
            predictions = self.model.predict(df[['ds']])
            return np.mean(np.abs(predictions['yhat'] - df['y']))
        
        return 0.0
    
    async def predict(
        self,
        horizon: str = "24h",
        interval_minutes: int = 60
    ) -> List[CarbonForecast]:
        """
        Predict future carbon intensity
        
        Args:
            horizon: Prediction horizon (e.g., "1h", "6h", "24h", "7d")
            interval_minutes: Time interval between predictions
        
        Returns:
            List of carbon forecasts
        """
        
        if self.model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        
        # Parse horizon
        periods = self._parse_horizon(horizon, interval_minutes)
        
        if self.model_type == "prophet":
            # Create future dataframe
            future = self.model.make_future_dataframe(
                periods=periods,
                freq=f'{interval_minutes}T'
            )
            
            # Predict
            forecast_df = self.model.predict(future)
            
            # Extract predictions (only future, not historical)
            future_forecast = forecast_df.tail(periods)
            
            # Convert to CarbonForecast objects
            forecasts = []
            for _, row in future_forecast.iterrows():
                forecasts.append(CarbonForecast(
                    timestamp=row['ds'],
                    predicted_intensity=max(0, row['yhat']),
                    lower_bound=max(0, row['yhat_lower']),
                    upper_bound=max(0, row['yhat_upper']),
                    confidence=self._calculate_confidence(row)
                ))
            
            return forecasts
        
        elif self.model_type == "persistence":
            # Simple persistence: repeat last value
            last_value = self.model
            last_time = self.historical_data[-1][0]
            
            forecasts = []
            for i in range(1, periods + 1):
                timestamp = last_time + timedelta(minutes=i * interval_minutes)
                forecasts.append(CarbonForecast(
                    timestamp=timestamp,
                    predicted_intensity=last_value,
                    lower_bound=last_value * 0.8,
                    upper_bound=last_value * 1.2,
                    confidence=0.5
                ))
            
            return forecasts
        
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")
    
    def _calculate_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on prediction interval width"""
        
        interval_width = row['yhat_upper'] - row['yhat_lower']
        
        # Normalize: narrower interval = higher confidence
        # Assume interval_width of 100 gCO2/kWh = 0.5 confidence
        confidence = 1.0 - (interval_width / 200.0)
        
        return np.clip(confidence, 0.1, 0.95)
    
    def _parse_horizon(self, horizon: str, interval_minutes: int) -> int:
        """Convert horizon string to number of periods"""
        
        horizon = horizon.lower().strip()
        
        if horizon.endswith('h'):
            hours = int(horizon[:-1])
            return (hours * 60) // interval_minutes
        elif horizon.endswith('d'):
            days = int(horizon[:-1])
            return (days * 24 * 60) // interval_minutes
        else:
            raise ValueError(f"Invalid horizon format: {horizon}")
    
    async def get_current_intensity(self, region: Optional[str] = None) -> float:
        """Get current carbon intensity (real-time)"""
        
        region = region or self.region
        
        # In production, fetch from real-time API
        # For now, use last historical value + noise
        if self.historical_data:
            last_intensity = self.historical_data[-1][1]
            noise = np.random.normal(0, 10)
            return max(50, last_intensity + noise)
        
        return 400.0  # Default fallback
    
    async def find_optimal_execution_window(
        self,
        duration_hours: float,
        deadline: datetime,
        min_carbon_threshold: float = 200  # gCO2/kWh
    ) -> OptimalWindow:
        """
        Find lowest-carbon execution window before deadline
        
        Args:
            duration_hours: Required execution time (hours)
            deadline: Task deadline
            min_carbon_threshold: Target carbon intensity
        
        Returns:
            Optimal execution window
        """
        
        # Predict until deadline
        horizon_hours = max(1, (deadline - datetime.now()).total_seconds() / 3600)
        forecasts = await self.predict(
            horizon=f"{int(horizon_hours)}h",
            interval_minutes=60
        )
        
        if len(forecasts) < int(duration_hours):
            raise ValueError("Deadline too soon for requested duration")
        
        # Sliding window to find minimum average intensity
        window_size = int(duration_hours)
        min_avg_intensity = float('inf')
        best_window = None
        
        for i in range(len(forecasts) - window_size + 1):
            window = forecasts[i:i+window_size]
            avg_intensity = sum(f.predicted_intensity for f in window) / window_size
            
            if avg_intensity < min_avg_intensity:
                min_avg_intensity = avg_intensity
                best_window = window
        
        if best_window is None:
            raise ValueError("No suitable execution window found")
        
        # Calculate carbon savings vs. immediate execution
        immediate_intensity = await self.get_current_intensity()
        carbon_savings_percent = (
            (immediate_intensity - min_avg_intensity) / immediate_intensity * 100
        )
        
        return OptimalWindow(
            start_time=best_window[0].timestamp,
            end_time=best_window[-1].timestamp,
            avg_intensity=min_avg_intensity,
            min_intensity=min(f.predicted_intensity for f in best_window),
            max_intensity=max(f.predicted_intensity for f in best_window),
            carbon_savings_percent=carbon_savings_percent
        )
    
    def save_model(self, path: Path):
        """Save trained model to disk"""
        
        if self.model is None:
            raise RuntimeError("No model to save")
        
        with open(path, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'model_type': self.model_type,
                'region': self.region,
                'last_train_time': self.last_train_time,
                'training_mae': self.training_mae
            }, f)
        
        logger.info(f"Model saved to {path}")
    
    def load_model(self, path: Path):
        """Load trained model from disk"""
        
        with open(path, 'rb') as f:
            data = pickle.load(f)
        
        self.model = data['model']
        self.model_type = data['model_type']
        self.region = data['region']
        self.last_train_time = data['last_train_time']
        self.training_mae = data['training_mae']
        
        logger.info(f"Model loaded from {path}")
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get model information and statistics"""
        
        return {
            "model_type": self.model_type,
            "region": self.region,
            "last_train_time": self.last_train_time.isoformat() if self.last_train_time else None,
            "training_mae": self.training_mae,
            "num_historical_points": len(self.historical_data),
            "historical_date_range": {
                "start": self.historical_data[0][0].isoformat() if self.historical_data else None,
                "end": self.historical_data[-1][0].isoformat() if self.historical_data else None
            }
        }


# Convenience function
async def create_forecaster(
    region: str = "US-CA",
    train_days: int = 90
) -> CarbonForecaster:
    """Create and train carbon forecaster"""
    
    forecaster = CarbonForecaster(region=region, model_type="prophet")
    await forecaster.train(days=train_days)
    
    return forecaster


if __name__ == "__main__":
    # Example usage
    import asyncio
    
    async def main():
        # Create forecaster
        forecaster = await create_forecaster(region="US-CA", train_days=30)
        
        # Predict next 24 hours
        forecasts = await forecaster.predict(horizon="24h", interval_minutes=60)
        
        print(f"24-hour forecast ({len(forecasts)} points):")
        for f in forecasts[:5]:
            print(f"  {f.timestamp}: {f.predicted_intensity:.1f} gCO2/kWh "
                  f"(confidence: {f.confidence:.2f})")
        
        # Find optimal window
        deadline = datetime.now() + timedelta(hours=48)
        optimal = await forecaster.find_optimal_execution_window(
            duration_hours=2.0,
            deadline=deadline
        )
        
        print(f"\nOptimal execution window:")
        print(f"  Start: {optimal.start_time}")
        print(f"  End: {optimal.end_time}")
        print(f"  Avg intensity: {optimal.avg_intensity:.1f} gCO2/kWh")
        print(f"  Carbon savings: {optimal.carbon_savings_percent:.1f}%")
        
        # Model info
        print(f"\nModel info: {forecaster.get_model_info()}")
    
    asyncio.run(main())
