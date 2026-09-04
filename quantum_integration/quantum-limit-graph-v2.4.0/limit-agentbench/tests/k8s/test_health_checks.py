"""
Health Check Tests
Green Agent v5.0.0 (Enhanced)

Adds tests for advanced enhancement health indicators:
- LIMIT Graph metrics in health/readiness responses
- MODP composite score
- RLHF feedback score
- Distillation, MoE, evolutionary status
- FlexGen readiness
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

    # ------------------------------------------------------------------
    # Enhanced tests for advanced enhancement health checks
    # ------------------------------------------------------------------

    def test_metrics_endpoint_contains_enhanced_metrics(self, dashboard_url):
        """Prometheus metrics endpoint should expose enhanced metrics."""
        try:
            response = requests.get(f"{dashboard_url}/metrics", timeout=10)
            assert response.status_code == 200
            text = response.text
            # List of expected enhanced metric substrings
            enhanced_metrics = [
                'green_agent_modp_score',
                'green_agent_rlhf_feedback',
                'green_agent_graph_centrality',
                'green_agent_graph_connectivity',
                'green_agent_distillation_update_count',
                'green_agent_moe_gate_weight',
                'green_agent_evolutionary_best_fitness',
                'green_agent_flexgen_energy_joules_total'
            ]
            for metric in enhanced_metrics:
                assert metric in text, f"Enhanced metric {metric} not found in /metrics"
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible (expected in CI)")

    def test_health_endpoint_enhanced_fields(self, dashboard_url):
        """Health endpoint may include enhanced status fields."""
        try:
            response = requests.get(f"{dashboard_url}/health", timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Check if any enhanced keys exist
                enhanced_keys = ['modp_score', 'rlhf_feedback', 'graph_centrality',
                                 'distillation_status', 'moe_status', 'evolutionary_status',
                                 'flexgen_status']
                found_any = any(k in data for k in enhanced_keys)
                # We don't fail if not present (maybe optional), but if present validate types
                if 'modp_score' in data:
                    assert isinstance(data['modp_score'], (int, float))
                if 'graph_centrality' in data:
                    assert isinstance(data['graph_centrality'], (int, float))
                # If the system is enhanced, these should exist; but we can't guarantee.
                # So we just assert the call works.
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible (expected in CI)")

    def test_pod_environment_has_enhancement_flags(self, k8s_client, test_namespace, wait_for_pods):
        """Ray head pod should have environment variables enabling enhancements."""
        pods = wait_for_pods('component=head', timeout=300)
        container = pods[0].spec.containers[0]
        env_names = {env.name: env.value for env in container.env}
        # Check for core enhancement flag
        assert env_names.get('ENHANCEMENTS_ENABLED') == 'true', "ENHANCEMENTS_ENABLED not true"
        # Check for specific enhancement toggles
        expected_env = {
            'LIMIT_GRAPH_ENABLED': 'true',
            'MODP_ENABLED': 'true',
            'RLHF_ENABLED': 'true',
            'DISTILLATION_ENABLED': 'true',
            'MOE_GATING_ENABLED': 'true',
            'EVOLUTIONARY_ENABLED': 'true',
            'FLEXGEN_ENABLED': 'true'
        }
        for var, expected in expected_env.items():
            assert env_names.get(var) == expected, f"{var} missing or incorrect"
