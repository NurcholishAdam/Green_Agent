# test_graph_metrics_exporter_helium.py
import pytest
from unittest.mock import Mock
from src.monitoring.graph_metrics_exporter import GraphMetricsExporter
from carbon.helium_monitor import HeliumMonitor, HeliumScarcityLevel, HeliumSupplySignal
from datetime import datetime

def test_helium_metrics_integration():
    """Test that helium metrics are collected when monitor is configured"""
    # Mock registry
    registry = Mock()
    registry.health.return_value = {
        "execution_count": 5,
        "singletons": {}
    }
    
    # Mock helium monitor with test signal
    helium_monitor = Mock(spec=HeliumMonitor)
    helium_monitor.get_current_supply.return_value = HeliumSupplySignal(
        timestamp=datetime.now(),
        scarcity_level=HeliumScarcityLevel.CRITICAL,
        scarcity_score=0.7,
        spot_price_usd_per_liter=8.0,
        fab_inventory_days=10,
        vendor_alerts=['Test alert'],
        source='test'
    )
    
    # Create exporter with helium monitor
    exporter = GraphMetricsExporter(
        registry=registry,
        helium_monitor=helium_monitor
    )
    
    # Collect metrics
    metrics = exporter.collect()
    
    # Verify helium metrics present
    assert 'green_agent_helium_scarcity_level' in metrics
    assert metrics['green_agent_helium_scarcity_level'][0] == 2  # CRITICAL=2
    assert 'green_agent_helium_spot_price_usd' in metrics
    assert metrics['green_agent_helium_spot_price_usd'][0] == 8.0
    assert 'green_agent_helium_price_premium_usd' in metrics
    assert metrics['green_agent_helium_price_premium_usd'][0] == 4.0  # 8.0 - 4.0 baseline
    
    # Verify Prometheus format
    text = exporter.render()
    assert 'green_agent_helium_scarcity_level{source="test",job="green_agent"} 2' in text
    assert 'green_agent_helium_spot_price_usd{job="green_agent"} 8.0' in text

def test_no_helium_metrics_when_disabled():
    """Test that no helium metrics are collected when monitor is None"""
    registry = Mock()
    registry.health.return_value = {"execution_count": 0, "singletons": {}}
    
    exporter = GraphMetricsExporter(registry, helium_monitor=None)
    metrics = exporter.collect()
    
    # Should have base metrics but no helium metrics
    assert 'green_agent_execution_graphs_active' in metrics
    assert 'green_agent_helium_scarcity_level' not in metrics
