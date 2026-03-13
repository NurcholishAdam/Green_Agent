"""
Kubernetes Deployment Tests
Green Agent v5.0.0
"""

import pytest
from kubernetes.client.rest import ApiException
import time

class TestRayClusterDeployment:
    """Test Ray Cluster deployment"""
    
    def test_ray_cluster_created(self, k8s_client, test_namespace):
        """Verify RayCluster resource is created"""
        ray_cluster = k8s_client['custom_objects'].get_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=test_namespace,
            plural="rayclusters",
            name="green-agent-cluster"
        )
        
        assert ray_cluster is not None
        assert ray_cluster['metadata']['name'] == 'green-agent-cluster'
        assert ray_cluster['metadata']['labels']['app'] == 'green-agent'
    
    def test_head_pod_running(self, k8s_client, test_namespace, wait_for_pods):
        """Verify head pod is running"""
        pods = wait_for_pods('component=head', timeout=300)
        
        assert len(pods) >= 1
        assert pods[0].status.phase == 'Running'
    
    def test_worker_pods_running(self, k8s_client, test_namespace, wait_for_pods):
        """Verify worker pods are running"""
        pods = wait_for_pods('component=worker', timeout=300)
        
        assert len(pods) >= 2  # minReplicas
    
    def test_resource_limits_applied(self, k8s_client, test_namespace, wait_for_pods):
        """Verify resource limits are applied"""
        pods = wait_for_pods('component=head', timeout=300)
        
        container = pods[0].spec.containers[0]
        
        assert container.resources.requests['cpu'] == '2' or container.resources.requests['cpu'] == '4'
        assert container.resources.requests['memory'] in ['4Gi', '8Gi']
    
    def test_configmap_mounted(self, k8s_client, test_namespace, wait_for_pods):
        """Verify ConfigMap is mounted"""
        pods = wait_for_pods('component=head', timeout=300)
        
        volume_mounts = pods[0].spec.containers[0].volume_mounts
        config_mount = [vm for vm in volume_mounts if vm.mount_path == '/app/config']
        
        assert len(config_mount) == 1
    
    def test_persistent_volume_mounted(self, k8s_client, test_namespace, wait_for_pods):
        """Verify Persistent Volume is mounted"""
        pods = wait_for_pods('component=head', timeout=300)
        
        volume_mounts = pods[0].spec.containers[0].volume_mounts
        data_mount = [vm for vm in volume_mounts if vm.mount_path == '/app/data']
        
        assert len(data_mount) == 1
    
    def test_image_pull_policy(self, k8s_client, test_namespace, wait_for_pods):
        """Verify image pull policy is set correctly"""
        pods = wait_for_pods('component=head', timeout=300)
        
        container = pods[0].spec.containers[0]
        assert container.image_pull_policy == 'Always'
    
    def test_labels_applied(self, k8s_client, test_namespace, wait_for_pods):
        """Verify labels are applied correctly"""
        pods = wait_for_pods('app=green-agent', timeout=300)
        
        for pod in pods:
            assert 'app' in pod.metadata.labels
            assert pod.metadata.labels['app'] == 'green-agent'
