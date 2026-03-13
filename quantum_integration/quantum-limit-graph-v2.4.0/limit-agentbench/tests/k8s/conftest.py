"""Pytest fixtures for Kubernetes integration tests"""

import pytest
import kubernetes
from kubernetes import client, config
import time
import yaml

@pytest.fixture(scope="session")
def k8s_client():
    """Initialize Kubernetes client"""
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    
    return {
        'core': client.CoreV1Api(),
        'apps': client.AppsV1Api(),
        'autoscaling': client.AutoscalingV2Api(),
        'custom_objects': client.CustomObjectsApi()
    }

@pytest.fixture(scope="session")
def namespace():
    """Test namespace"""
    return "green-agent-test"

@pytest.fixture
def wait_for_pods(k8s_client, namespace):
    """Wait for pods to be ready"""
    def _wait(label_selector, timeout=300):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pods = k8s_client['core'].list_namespaced_pod(
                    namespace=namespace,
                    label_selector=label_selector
                )
                ready_pods = [
                    p for p in pods.items
                    if p.status.phase == 'Running'
                ]
                if len(ready_pods) > 0:
                    return ready_pods
                time.sleep(5)
            except:
                time.sleep(5)
        raise TimeoutError(f"Pods not ready within {timeout}s")
    return _wait
