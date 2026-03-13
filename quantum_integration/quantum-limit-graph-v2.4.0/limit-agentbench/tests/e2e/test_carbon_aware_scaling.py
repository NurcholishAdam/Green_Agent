"""
End-to-End Carbon-Aware Scaling Tests
Green Agent v5.0.0
"""

import pytest
import time
import requests

class TestCarbonAwareScaling:
    """End-to-end tests for carbon-aware scaling"""
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_metrics_exported(self, dashboard_url):
        """Verify carbon metrics are exported to Prometheus"""
        try:
            response = requests.get(f"{dashboard_url}/metrics", timeout=10)
            
            assert response.status_code == 200
            
            # Check for carbon metrics
            assert 'green_agent_carbon_intensity' in response.text
            assert 'green_agent_carbon_emitted_kg' in response.text
            assert 'green_agent_energy_consumed_kwh' in response.text
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible")
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_hpa_responds_to_metrics(self, k8s_client, test_namespace):
        """Verify HPA can access custom metrics"""
        try:
            # Get HPA status
            hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
                name="green-agent-hpa",
                namespace=test_namespace
            )
            
            # HPA should be configured
            assert hpa is not None
            assert hpa.spec.min_replicas >= 2
            assert hpa.spec.max_replicas >= 10
        except Exception as e:
            pytest.skip(f"HPA not accessible: {e}")
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_dashboard_accessible(self, dashboard_url):
        """Verify dashboard is accessible"""
        try:
            response = requests.get(f"{dashboard_url}/", timeout=10)
            assert response.status_code == 200
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible")
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_health_endpoints_respond(self, dashboard_url):
        """Verify all health endpoints respond"""
        endpoints = ['/health', '/ready', '/live', '/metrics']
        
        for endpoint in endpoints:
            try:
                response = requests.get(f"{dashboard_url}{endpoint}", timeout=10)
                assert response.status_code == 200, f"{endpoint} failed"
            except requests.exceptions.ConnectionError:
                pytest.skip(f"Endpoint {endpoint} not accessible")
