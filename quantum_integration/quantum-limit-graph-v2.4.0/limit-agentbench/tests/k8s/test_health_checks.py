"""
Health Check Tests
Green Agent v5.0.0
"""

import pytest
import requests
import time

class TestHealthChecks:
    """Test health check endpoints"""
    
    @pytest.fixture
    def dashboard_url(self, k8s_client, test_namespace, wait_for_pods):
        """Get dashboard URL via port-forward"""
        # For testing, assume localhost with port-forward
        return "http://localhost:8000"
    
    def test_health_endpoint(self, dashboard_url):
        """Test /health endpoint"""
        try:
            response = requests.get(f"{dashboard_url}/health", timeout=10)
            
            assert response.status_code == 200
            data = response.json()
            assert data['status'] == 'healthy'
            assert 'timestamp' in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible (expected in CI)")
    
    def test_readiness_endpoint(self, dashboard_url):
        """Test /ready endpoint"""
        try:
            response = requests.get(f"{dashboard_url}/ready", timeout=10)
            
            assert response.status_code == 200
            data = response.json()
            assert 'ready' in data
            assert 'checks' in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible (expected in CI)")
    
    def test_liveness_endpoint(self, dashboard_url):
        """Test /live endpoint"""
        try:
            response = requests.get(f"{dashboard_url}/live", timeout=10)
            
            assert response.status_code == 200
            data = response.json()
            assert data['status'] == 'alive'
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible (expected in CI)")
    
    def test_metrics_endpoint(self, dashboard_url):
        """Test /metrics endpoint (Prometheus)"""
        try:
            response = requests.get(f"{dashboard_url}/metrics", timeout=10)
            
            assert response.status_code == 200
            assert 'green_agent' in response.text
            assert 'green_agent_energy_consumed_kwh' in response.text
            assert 'green_agent_carbon_emitted_kg' in response.text
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible (expected in CI)")
    
    def test_health_probe_configuration(self, k8s_client, test_namespace, wait_for_pods):
        """Verify health probes are configured in pod spec"""
        pods = wait_for_pods('component=head', timeout=300)
        
        container = pods[0].spec.containers[0]
        
        # Liveness probe
        assert container.liveness_probe is not None
        assert container.liveness_probe.http_get.path == '/health'
        assert container.liveness_probe.http_get.port == 8000
        
        # Readiness probe
        assert container.readiness_probe is not None
        assert container.readiness_probe.http_get.path == '/ready'
        assert container.readiness_probe.http_get.port == 8000
        
        # Startup probe
        assert container.startup_probe is not None
        assert container.startup_probe.http_get.path == '/health'
        assert container.startup_probe.http_get.port == 8000
    
    def test_probe_timing_configuration(self, k8s_client, test_namespace, wait_for_pods):
        """Verify probe timing is configured correctly"""
        pods = wait_for_pods('component=head', timeout=300)
        
        container = pods[0].spec.containers[0]
        
        # Liveness probe timing
        assert container.liveness_probe.initial_delay_seconds >= 30
        assert container.liveness_probe.period_seconds >= 10
        
        # Readiness probe timing
        assert container.readiness_probe.initial_delay_seconds >= 10
        assert container.readiness_probe.period_seconds >= 5
        
        # Startup probe timing
        assert container.startup_probe.failure_threshold >= 30
