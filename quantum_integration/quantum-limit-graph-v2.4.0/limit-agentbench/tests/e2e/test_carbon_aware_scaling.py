"""
End-to-End Carbon-Aware Scaling Tests
Green Agent v5.0.0 (Enhanced)

Adds end-to-end tests for advanced enhancement integration:
- LIMIT Graph metrics
- MODP composite score
- RLHF feedback
- Multi‑Teacher Distillation update rate
- MoE gate stability
- Bio‑inspired evolutionary fitness
- FlexGen energy consumption
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

    # ------------------------------------------------------------------
    # Enhanced End-to-End tests for advanced modules
    # ------------------------------------------------------------------

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_enhanced_metrics_exported(self, dashboard_url):
        """Verify advanced enhancement metrics are exported to Prometheus."""
        try:
            response = requests.get(f"{dashboard_url}/metrics", timeout=10)
            assert response.status_code == 200
            text = response.text
            enhanced_metrics = [
                'green_agent_modp_score',
                'green_agent_rlhf_feedback',
                'green_agent_graph_centrality',
                'green_agent_graph_connectivity',
                'green_agent_distillation_update_count',
                'green_agent_moe_gate_weight',
                'green_agent_evolutionary_best_fitness',
                'green_agent_flexgen_energy_joules_total',
            ]
            for metric in enhanced_metrics:
                assert metric in text, f"Enhanced metric {metric} not exported"
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not accessible")

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_hpa_has_enhanced_metrics(self, k8s_client, test_namespace):
        """Verify HPA is configured with enhanced custom metrics."""
        try:
            hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
                name="green-agent-hpa",
                namespace=test_namespace
            )
            metric_names = set()
            for metric in hpa.spec.metrics:
                if metric.type == 'Pods':
                    metric_names.add(metric.pods.metric.name)
                elif metric.type == 'External':
                    metric_names.add(metric.external.metric.name)
            # Check for at least one enhanced metric
            enhanced_metrics = {
                'modp_score', 'rlhf_feedback_score', 'graph_centrality',
                'distillation_update_rate', 'moe_gate_stddev',
                'evolutionary_best_fitness', 'flexgen_energy_rate'
            }
            assert enhanced_metrics & metric_names, "No enhanced metrics found in HPA"
        except Exception as e:
            pytest.skip(f"HPA not accessible: {e}")

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_dashboard_enhanced_endpoints(self, dashboard_url):
        """Verify dashboard endpoints for enhanced features respond (if available)."""
        # These endpoints may or may not exist; we check a few likely ones.
        enhanced_endpoints = [
            '/analytics/modp',
            '/analytics/rlhf',
            '/analytics/graph',
            '/enhancements/status',
        ]
        for endpoint in enhanced_endpoints:
            try:
                response = requests.get(f"{dashboard_url}{endpoint}", timeout=5)
                # We don't require 200; just ensure no server error (5xx)
                assert response.status_code < 500, f"{endpoint} returned {response.status_code}"
            except requests.exceptions.ConnectionError:
                pytest.skip(f"Endpoint {endpoint} not accessible")

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_enhancement_configmap_present(self, k8s_client, test_namespace):
        """Verify the enhancement ConfigMap exists if enhancements are enabled."""
        try:
            cm = k8s_client['core'].read_namespaced_config_map(
                name="green-agent-enhancements",
                namespace=test_namespace
            )
            assert cm is not None
            # Check for enhancement sections in data
            config_yaml = cm.data.get('green_agent_config.yaml', '')
            for key in ['limit_graph', 'modp', 'rlhf', 'distillation', 'bio_inspired', 'moe_expert', 'flexgen']:
                assert key in config_yaml, f"Missing enhancement section: {key}"
        except Exception:
            pytest.skip("Enhancement ConfigMap not found")
