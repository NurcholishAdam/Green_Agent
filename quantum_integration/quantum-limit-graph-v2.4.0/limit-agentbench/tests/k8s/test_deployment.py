"""Kubernetes Deployment Tests"""

import pytest

class TestRayClusterDeployment:
    
    def test_ray_cluster_created(self, k8s_client, namespace):
        """Verify RayCluster resource is created"""
        ray_cluster = k8s_client['custom_objects'].get_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=namespace,
            plural="rayclusters",
            name="green-agent-cluster"
        )
        assert ray_cluster is not None
        assert ray_cluster['metadata']['name'] == 'green-agent-cluster'
    
    def test_head_pod_running(self, k8s_client, namespace, wait_for_pods):
        """Verify head pod is running"""
        pods = wait_for_pods('component=head', timeout=300)
        assert len(pods) >= 1
        assert pods[0].status.phase == 'Running'
    
    def test_worker_pods_running(self, k8s_client, namespace, wait_for_pods):
        """Verify worker pods are running"""
        pods = wait_for_pods('component=worker', timeout=300)
        assert len(pods) >= 1
