"""
Horizontal Pod Autoscaler Tests
Green Agent v5.0.0
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
        
        # Should have multiple metrics (CPU, Memory, Carbon)
        assert len(hpa.spec.metrics) >= 3
        
        metric_types = [m.type for m in hpa.spec.metrics]
        assert 'Resource' in metric_types
    
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
