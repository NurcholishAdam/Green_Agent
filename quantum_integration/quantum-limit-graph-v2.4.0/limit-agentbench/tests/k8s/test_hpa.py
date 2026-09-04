"""
Horizontal Pod Autoscaler Tests
Green Agent v5.0.0 (Enhanced)

Adds tests for advanced enhancement integration in HPA:
- LIMIT Graph metrics
- MODP (Multi-Objective Decision Process)
- RLHF feedback
- Distillation update rate
- MoE gate stability
- Bio‑inspired evolutionary fitness
- FlexGen energy rate
"""

import pytest
import time

class TestHorizontalPodAutoscaler:
    """Test HPA configuration and behavior"""
    
    def test_hpa_created(self, k8s_client, test_namespace):
        """Verify HPA is created"""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        
        assert hpa is not None
        assert hpa.spec.min_replicas == 2 or hpa.spec.min_replicas == 4
        assert hpa.spec.max_replicas == 20
    
    def test_hpa_metrics_configured(self, k8s_client, test_namespace):
        """Verify HPA metrics are configured"""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        
        # Should have multiple metrics (CPU, Memory, Carbon, and enhanced)
        assert len(hpa.spec.metrics) >= 3
        
        metric_types = [m.type for m in hpa.spec.metrics]
        assert 'Resource' in metric_types
        assert 'Pods' in metric_types  # For pod-based enhanced metrics
        # Could also be 'External' for carbon intensity if external
        assert 'External' in metric_types or any(m.type == 'Pods' for m in hpa.spec.metrics)
    
    def test_hpa_scaling_behavior(self, k8s_client, test_namespace):
        """Verify HPA scaling behavior is configured"""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        
        assert hpa.spec.behavior is not None
        assert hpa.spec.behavior.scale_down.stabilization_window_seconds >= 300
        assert hpa.spec.behavior.scale_up.stabilization_window_seconds >= 30
    
    def test_hpa_target_reference(self, k8s_client, test_namespace):
        """Verify HPA targets correct RayCluster"""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        
        assert hpa.spec.scale_target_ref.kind == 'RayCluster'
        assert hpa.spec.scale_target_ref.name == 'green-agent-cluster'
    
    @pytest.mark.slow
    def test_hpa_status_available(self, k8s_client, test_namespace, deploy_green_agent):
        """Verify HPA status is being populated"""
        # Wait for metrics to be collected
        time.sleep(60)
        
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        
        # Status should be available (may not have current metrics yet)
        assert hpa.status is not None

    # ------------------------------------------------------------------
    # Enhanced tests for advanced enhancement integration
    # ------------------------------------------------------------------

    def test_hpa_has_enhanced_labels(self, k8s_client, test_namespace):
        """HPA should have enhancement labels and annotations."""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        assert hpa.metadata.labels.get('enhancements') == 'enabled'
        annotations = hpa.metadata.annotations or {}
        expected_annotations = {
            'green-agent/limit-graph': 'true',
            'green-agent/modp': 'true',
            'green-agent/rlhf': 'true',
            'green-agent/distillation': 'true',
            'green-agent/bio-inspired': 'true',
            'green-agent/moe': 'true',
            'green-agent/flexgen': 'true',
        }
        for key, value in expected_annotations.items():
            assert annotations.get(key) == value, f"Missing annotation {key}"

    def test_hpa_enhanced_metrics_present(self, k8s_client, test_namespace):
        """HPA should include enhanced custom metrics for MODP, RLHF, graph, etc."""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        # Extract metric names from pods/external metrics
        metric_names = set()
        for metric in hpa.spec.metrics:
            if metric.type == 'Pods':
                metric_names.add(metric.pods.metric.name)
            elif metric.type == 'External':
                metric_names.add(metric.external.metric.name)
            elif metric.type == 'Resource':
                metric_names.add(metric.resource.name)  # cpu, memory
        # Required enhanced metrics
        required_enhanced = {
            'modp_score',
            'rlhf_feedback_score',
            'graph_centrality',
            'distillation_update_rate',
            'moe_gate_stddev',
            'evolutionary_best_fitness',
            'flexgen_energy_rate'
        }
        assert required_enhanced.issubset(metric_names), f"Missing enhanced metrics: {required_enhanced - metric_names}"

    def test_hpa_evolutionary_metric_average_value(self, k8s_client, test_namespace):
        """Evolutionary best fitness metric should have a reasonable target."""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        # Find the evolutionary_best_fitness metric target
        for metric in hpa.spec.metrics:
            if metric.type == 'Pods' and metric.pods.metric.name == 'evolutionary_best_fitness':
                avg_val = metric.pods.target.average_value
                # Should be a float > 0 and <= 1 (fitness normalized)
                assert avg_val is not None
                assert 0.0 < float(avg_val) <= 1.0

    def test_hpa_moe_stability_metric_present(self, k8s_client, test_namespace):
        """MoE gate stability metric should be a stddev-based metric."""
        hpa = k8s_client['autoscaling'].read_namespaced_horizontal_pod_autoscaler(
            name="green-agent-hpa",
            namespace=test_namespace
        )
        moe_found = False
        for metric in hpa.spec.metrics:
            if metric.type == 'Pods' and metric.pods.metric.name == 'moe_gate_stddev':
                moe_found = True
                # The metric query should likely be a stddev_over_time, but we can't verify that here
                break
        assert moe_found, "MoE gate stddev metric not present in HPA"
