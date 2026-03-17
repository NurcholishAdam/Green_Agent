"""Kubernetes manifest tests"""
import pytest
import yaml
import os

def test_kustomize_base_valid():
    """Test that base kustomization is valid"""
    kustomization_path = "config/base/kustomization.yaml"
    
    if not os.path.exists(kustomization_path):
        pytest.skip(f"{kustomization_path} does not exist")
    
    with open(kustomization_path, 'r') as f:
        config = yaml.safe_load(f)
    
    assert config is not None
    assert config.get('apiVersion') == 'kustomize.config.k8s.io/v1beta1'
    assert config.get('kind') == 'Kustomization'
    print("✅ Base kustomization is valid")

def test_namespace_manifest_valid():
    """Test that namespace manifest is valid"""
    namespace_path = "config/base/namespace.yaml"
    
    if not os.path.exists(namespace_path):
        pytest.skip(f"{namespace_path} does not exist")
    
    with open(namespace_path, 'r') as f:
        config = yaml.safe_load(f)
    
    assert config is not None
    assert config.get('apiVersion') == 'v1'
    assert config.get('kind') == 'Namespace'
    assert config.get('metadata', {}).get('name') is not None
    print("✅ Namespace manifest is valid")

def test_deployment_manifest_valid():
    """Test that deployment manifest is valid"""
    deployment_path = "config/base/deployment.yaml"
    
    if not os.path.exists(deployment_path):
        pytest.skip(f"{deployment_path} does not exist")
    
    with open(deployment_path, 'r') as f:
        config = yaml.safe_load(f)
    
    assert config is not None
    assert config.get('apiVersion') == 'apps/v1'
    assert config.get('kind') == 'Deployment'
    assert config.get('spec', {}).get('replicas') is not None
    print("✅ Deployment manifest is valid")
