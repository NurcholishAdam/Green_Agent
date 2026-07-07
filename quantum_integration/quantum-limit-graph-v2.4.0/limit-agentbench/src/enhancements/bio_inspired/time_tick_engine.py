"""
TimeTickEngine v1.0
Loads the helium CSV, interpolates to daily frequency, and drives the simulation
by feeding daily environmental data to the Photosynthetic Harvester.
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable
import logging

logger = logging.getLogger(__name__)

class TimeTickEngine:
    """
    Simulation driver that:
      - Loads the helium CSV (monthly data)
      - Interpolates to daily ticks (linear)
      - For each tick, calls the Harvester's harvest_cycle() with translated data
      - Optionally runs a callback after each tick (e.g., to update the quantum graph)
    """
    
    def __init__(self, csv_path: str, harvester, translator_class,
                 start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Args:
            csv_path: Path to the CSV file (monthly data)
            harvester: The EnhancedPhotosyntheticHarvester instance
            translator_class: The HeliumEnvironmentTranslator class (or instance)
            start_date, end_date: Optional date strings to limit simulation range.
        """
        self.harvester = harvester
        self.translator_class = translator_class
        
        # Load and prepare data
        self.df = pd.read_csv(csv_path)
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df = self.df.sort_values('date')
        
        # Filter dates if provided
        if start_date:
            start = pd.to_datetime(start_date)
            self.df = self.df[self.df['date'] >= start]
        if end_date:
            end = pd.to_datetime(end_date)
            self.df = self.df[self.df['date'] <= end]
        
        # Interpolate to daily frequency
        self._interpolate_daily()
        
        logger.info(f"TimeTickEngine loaded {len(self.df)} monthly rows, interpolated to {len(self.daily_df)} daily ticks.")
    
    def _interpolate_daily(self):
        """Convert monthly data to daily using linear interpolation."""
        # Set date as index
        df_monthly = self.df.set_index('date')
        # Create daily index covering the full range
        daily_index = pd.date_range(start=df_monthly.index.min(),
                                    end=df_monthly.index.max(),
                                    freq='D')
        # Interpolate all numeric columns
        self.daily_df = df_monthly.reindex(daily_index).interpolate(method='linear').reset_index()
        self.daily_df.rename(columns={'index': 'date'}, inplace=True)
    
    async def run_simulation(self, tick_interval_seconds: float = 0.1,
                            post_tick_callback: Optional[Callable] = None):
        """
        Run the simulation over all daily ticks.
        
        Args:
            tick_interval_seconds: Time to sleep between each tick (simulate real‑time).
            post_tick_callback: Async function called after each tick, with the tick data.
        """
        logger.info(f"Starting simulation over {len(self.daily_df)} days...")
        
        for idx, row in self.daily_df.iterrows():
            # Convert the row to the format expected by the Harvester
            env_data = self.translator_class.translate_row(row)
            if env_data is None:
                continue
            
            # Run the harvest cycle
            result = await self.harvester.harvest_cycle(env_data)
            
            # Optional callback (e.g., update quantum graph)
            if post_tick_callback:
                await post_tick_callback(idx, row, result)
            
            # Log every 30 days
            if idx % 30 == 0:
                logger.info(f"Day {idx}: harvested {result.get('eco_atp_generated',0):.2f} Eco‑ATP, balance {result.get('account_balance',0):.2f}")
            
            await asyncio.sleep(tick_interval_seconds)
        
        logger.info("Simulation completed.")
    
    def get_daily_data(self) -> pd.DataFrame:
        """Return the interpolated daily DataFrame."""
        return self.daily_df
