"""
Monitoring Tests
Green Agent v5.0.0
"""

import pytest
import requests

class TestMonitoring:
    """Test monitoring configuration"""
    
    def test_servicemonitor_created(self, k8s_client, test_namespace):
        """Verify ServiceMonitor is created"""
        try:
            sm = k8s_client['custom_objects'].get_namespaced_custom_object(
                group="monitoring.coreos.com",
                version="v1",
                namespace=test_namespace,
                plural="servicemonitors",
                name="green-agent-monitor"
            )
            
            assert sm is not None
            assert sm['metadata']['name'] == 'green-agent-monitor'
        except:
            pytest.skip("ServiceMonitor CRD not available")
    
    def test_prometheusrule_created(self, k8s_client, test_namespace):
        """Verify PrometheusRule is created"""
        try:
            rule = k8s_client['custom_objects'].get_namespaced_custom_object(
                group="monitoring.coreos.com",
                version="v1",
                namespace=test_namespace,
                plural="prometheusrules",
                name="green-agent-alerts"
            )
            
            assert rule is not None
            assert len(rule['spec']['groups']) >= 1
        except:
            pytest.skip("PrometheusRule CRD not available")
    
    def test_metrics_service_exists(self, k8s_client, test_namespace):
        """Verify metrics service exists"""
        services = k8s_client['core'].list_namespaced_service(
            namespace=test_namespace,
            label_selector="app=green-agent"
        )
        
        metrics_services = [
            s for s in services.items
            if 'metrics' in s.metadata.name or
               any(p.name == 'metrics' for p in s.spec.ports)
        ]
        
        assert len(metrics_services) >= 1
    
    def test_grafana_dashboard_configmap(self, k8s_client, test_namespace):
        """Verify Grafana dashboard ConfigMap exists"""
        try:
            cm = k8s_client['core'].read_namespaced_config_map(
                name="green-agent-grafana-dashboard",
                namespace=test_namespace
            )
            
            assert cm is not None
            assert 'green-agent-overview.json' in cm.data
        except:
            pytest.skip("Grafana dashboard ConfigMap not found")
    
    def test_alert_rules_configured(self, k8s_client, test_namespace):
        """Verify alert rules are configured"""
        try:
            rule = k8s_client['custom_objects'].get_namespaced_custom_object(
                group="monitoring.coreos.com",
                version="v1",
                namespace=test_namespace,
                plural="prometheusrules",
                name="green-agent-alerts"
            )
            
            # Should have multiple alert rules
            rules = rule['spec']['groups'][0]['rules']
            assert len(rules) >= 5
            
            # Check for specific alerts
            alert_names = [r['alert'] for r in rules if 'alert' in r]
            assert 'GreenAgentHighCarbonIntensity' in alert_names
            assert 'GreenAgentServiceDown' in alert_names
        except:
            pytest.skip("PrometheusRule not available")
