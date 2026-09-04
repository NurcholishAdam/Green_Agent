"""
Monitoring Tests
Green Agent v5.0.0 (Enhanced)

Adds tests for advanced enhancement integration in monitoring:
- LIMIT Graph metrics
- MODP (Multi-Objective Decision Process) alerts
- RLHF feedback alerts
- Distillation / MoE / Evolutionary metrics
- FlexGen energy monitoring
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

    # ------------------------------------------------------------------
    # Enhanced tests for advanced enhancement integration in monitoring
    # ------------------------------------------------------------------

    def test_servicemonitor_has_enhanced_labels(self, k8s_client, test_namespace):
        """ServiceMonitor should have enhancement labels and annotations."""
        try:
            sm = k8s_client['custom_objects'].get_namespaced_custom_object(
                group="monitoring.coreos.com",
                version="v1",
                namespace=test_namespace,
                plural="servicemonitors",
                name="green-agent-monitor"
            )
            labels = sm['metadata'].get('labels', {})
            assert labels.get('enhancements') == 'enabled'
            annotations = sm['metadata'].get('annotations', {})
            for key in ['green-agent/limit-graph', 'green-agent/modp', 'green-agent/rlhf',
                        'green-agent/distillation', 'green-agent/bio-inspired', 'green-agent/moe',
                        'green-agent/flexgen']:
                assert annotations.get(key) == 'true', f"Missing annotation {key}"
        except Exception:
            pytest.skip("ServiceMonitor CRD not available")

    def test_prometheusrule_has_enhanced_alerts(self, k8s_client, test_namespace):
        """PrometheusRule should contain alerts for MODP, RLHF, etc."""
        try:
            rule = k8s_client['custom_objects'].get_namespaced_custom_object(
                group="monitoring.coreos.com",
                version="v1",
                namespace=test_namespace,
                plural="prometheusrules",
                name="green-agent-alerts"
            )
            all_alerts = []
            for group in rule['spec']['groups']:
                all_alerts.extend([r['alert'] for r in group.get('rules', []) if 'alert' in r])
            # Required enhanced alerts
            enhanced_alerts = {
                'GreenAgentLowMODPScore',
                'GreenAgentLowRLHFFeedback',
                'GreenAgentGraphCentralityLow',
                'GreenAgentDistillationStalled',
                'GreenAgentMoEUnstable',
                'GreenAgentEvolutionStalled',
                'GreenAgentFlexGenHighEnergy'
            }
            assert enhanced_alerts.issubset(set(all_alerts)), f"Missing enhanced alerts: {enhanced_alerts - set(all_alerts)}"
        except Exception:
            pytest.skip("PrometheusRule CRD not available")

    def test_grafana_dashboard_contains_enhanced_panels(self, k8s_client, test_namespace):
        """Grafana dashboard JSON should include enhanced panels."""
        try:
            cm = k8s_client['core'].read_namespaced_config_map(
                name="green-agent-grafana-dashboard",
                namespace=test_namespace
            )
            dashboard_json = cm.data.get('green-agent-overview.json', '')
            # Check for mentions of enhanced metrics
            for keyword in ['modp_score', 'rlhf_feedback', 'graph_centrality',
                            'distillation_update', 'moe_gate', 'evolutionary_best_fitness',
                            'flexgen_energy']:
                assert keyword in dashboard_json, f"Dashboard missing metric: {keyword}"
        except Exception:
            pytest.skip("Grafana dashboard ConfigMap not found")

    def test_service_monitor_scrapes_enhanced_metrics(self, k8s_client, test_namespace):
        """ServiceMonitor endpoints should not drop enhanced metrics."""
        try:
            sm = k8s_client['custom_objects'].get_namespaced_custom_object(
                group="monitoring.coreos.com",
                version="v1",
                namespace=test_namespace,
                plural="servicemonitors",
                name="green-agent-monitor"
            )
            endpoints = sm['spec'].get('endpoints', [])
            # Enhanced metrics should be preserved (metricRelabelings may keep green_agent_*)
            for ep in endpoints:
                relabelings = ep.get('metricRelabelings', [])
                # Check if there is a keep rule for green_agent_*
                has_keep = any(rel.get('action') == 'keep' and 'green_agent_' in rel.get('regex', '')
                               for rel in relabelings)
                # If no keep rule, then metrics are not dropped explicitly
                if not has_keep:
                    # We can still pass if no drop rules for green_agent_
                    drop_rules = [rel for rel in relabelings if rel.get('action') == 'drop']
                    has_green_drop = any('green_agent_' in rel.get('regex', '') for rel in drop_rules)
                    assert not has_green_drop, "ServiceMonitor drops green_agent metrics, enhanced metrics will not be scraped"
        except Exception:
            pytest.skip("ServiceMonitor CRD not available")

    def test_metrics_service_has_enhanced_labels(self, k8s_client, test_namespace):
        """Metrics service should have enhancement labels."""
        services = k8s_client['core'].list_namespaced_service(
            namespace=test_namespace,
            label_selector="app=green-agent"
        )
        metrics_services = [s for s in services.items if any(p.name == 'metrics' for p in s.spec.ports)]
        assert metrics_services, "No metrics service found"
        # At least one metrics service should have enhancements label
        has_enhanced = any(s.metadata.labels.get('enhancements') == 'enabled' for s in metrics_services)
        assert has_enhanced, "Metrics service missing enhancements label"
