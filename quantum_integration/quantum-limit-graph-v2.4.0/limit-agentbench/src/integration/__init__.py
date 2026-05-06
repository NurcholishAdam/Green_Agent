# src/integration/__init__.py

"""
Integration module for Green Agent
Provides free API integrations and community data sharing
"""

from .free_apis import (
    FreeAPIManager,
    StaticDataProvider,
    FreeWeatherAPI,
    FreeGridCarbonAPI,
    SelfHostedGridCarbon,
    HeliumMarketSimulator,
    CommunityDataHub,
    CarbonData,
    WeatherData,
    HeliumData,
    GridMixData
)

__all__ = [
    'FreeAPIManager',
    'StaticDataProvider',
    'FreeWeatherAPI',
    'FreeGridCarbonAPI',
    'SelfHostedGridCarbon',
    'HeliumMarketSimulator',
    'CommunityDataHub',
    'CarbonData',
    'WeatherData',
    'HeliumData',
    'GridMixData'
]
