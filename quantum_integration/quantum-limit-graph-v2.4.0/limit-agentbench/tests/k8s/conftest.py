"""
Pytest fixtures for Kubernetes integration tests
Green Agent v5.0.0
"""

import pytest
import kubernetes
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import time
import yaml
from pathlib import Path
import subprocess
import os

@pytest.fixture(scope="session")
def k8s_client():
    """Initialize Kubernetes client"""
    try:
        # Try to load in-cluster config (for CI/CD)
        config.load_incluster_config()
    except:
        # Fall back to kubeconfig (for local testing)
        config.load_kube_config()
    
    return {
        'core': client.CoreV1Api(),
        'apps': client.AppsV1Api(),
        'autoscaling': client.AutoscalingV2Api(),
        'networking': client.NetworkingV1Api(),
        'custom_objects': client.CustomObjectsApi()
    }

@pytest.fixture(scope="session")
def namespace():
    """Test namespace"""
    return os.environ.get('TEST_NAMESPACE', 'green-agent-test')

@pytest.fixture(scope="session")
def test_namespace(k8s_client, namespace):
    """Create test namespace"""
    ns = client.V1Namespace(
        metadata=client.V1ObjectMeta(name=namespace)
    )
    
    try:
        k8s_client['core'].create_namespace(ns)
        print(f"✓ Created namespace: {namespace}")
        yield namespace
    finally:
        # Cleanup
        try:
            k8s_client['core'].delete_namespace(namespace)
            print(f"✓ Deleted namespace: {namespace}")
        except:
            pass

@pytest.fixture
def deploy_green_agent(k8s_client, test_namespace):
    """Deploy Green Agent for testing"""
    # Load manifests
    with open('config/base/ray-cluster.yaml') as f:
        ray_cluster = yaml.safe_load(f)
    
    with open('config/base/service.yaml') as f:
        service = yaml.safe_load(f)
    
    with open('config/base/hpa.yaml') as f:
        hpa = yaml.safe_load(f)
    
    # Deploy RayCluster
    k8s_client['custom_objects'].create_namespaced_custom_object(
        group="ray.io",
        version="v1",
        namespace=test_namespace,
        plural="rayclusters",
        body=ray_cluster
    )
    print("✓ Deployed RayCluster")
    
    # Deploy Service
    k8s_client['core'].create_namespaced_service(
        namespace=test_namespace,
        body=service
    )
    print("✓ Deployed Service")
    
    # Deploy HPA
    k8s_client['autoscaling'].create_namespaced_horizontal_pod_autoscaler(
        namespace=test_namespace,
        body=hpa
    )
    print("✓ Deployed HPA")
    
    # Wait for deployment
    print("⏳ Waiting for deployment...")
    time.sleep(30)
    
    yield test_namespace
    
    # Cleanup
    try:
        k8s_client['custom_objects'].delete_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=test_namespace,
            plural="rayclusters",
            name="green-agent-cluster"
        )
        print("✓ Cleaned up RayCluster")
    except:
        pass

@pytest.fixture
def wait_for_pods(k8s_client, test_namespace):
    """Wait for pods to be ready"""
    def _wait(label_selector, timeout=300):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pods = k8s_client['core'].list_namespaced_pod(
                    namespace=test_namespace,
                    label_selector=label_selector
                )
                
                ready_pods = [
                    p for p in pods.items
                    if p.status.phase == 'Running' and
                       all(c.ready for c in (p.status.conditions or []))
                ]
                
                if len(ready_pods) > 0:
                    return ready_pods
                
                time.sleep(5)
            except Exception as e:
                time.sleep(5)
        
        raise TimeoutError(f"Pods not ready within {timeout}s")
    
    return _wait

@pytest.fixture
def get_service_url(k8s_client, test_namespace):
    """Get service URL for testing"""
    def _get(service_name, port):
        try:
            service = k8s_client['core'].read_namespaced_service(
                name=service_name,
                namespace=test_namespace
            )
            
            if service.spec.type == 'LoadBalancer':
                # Wait for load balancer IP
                for _ in range(30):
                    service = k8s_client['core'].read_namespaced_service(
                        name=service_name,
                        namespace=test_namespace
                    )
                    if service.status.load_balancer.ingress:
                        ip = service.status.load_balancer.ingress[0].ip
                        return f"http://{ip}:{port}"
                    time.sleep(5)
            
            # Fallback to port-forward URL
            return f"http://localhost:{port}"
        
        except Exception as e:
            return f"http://localhost:{port}"
    
    return _get
