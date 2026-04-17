import pytest
import subprocess

def test_ray_cluster_exists():
    """Test RayCluster CRD is installed"""
    result = subprocess.run(
        ['kubectl', 'get', 'crd', 'rayclusters.ray.io'],
        capture_output=True, text=True
    )
    assert result.returncode == 0

def test_pods_running():
    """Test pods are running"""
    result = subprocess.run(
        ['kubectl', 'get', 'pods', '-n', 'green-agent-test', '-l', 'app=green-agent'],
        capture_output=True, text=True
    )
    assert 'Running' in result.stdout

def test_service_exists():
    """Test service exists"""
    result = subprocess.run(
        ['kubectl', 'get', 'svc', '-n', 'green-agent-test', 'dev-green-agent'],
        capture_output=True, text=True
    )
    assert result.returncode == 0
