"""Kubernetes manifest tests (Enhanced)

Additional tests for manifests that configure advanced enhancements:
- LIMIT Graph
- MODP (Multi‑Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi‑Teacher On‑Policy Distillation with MoE gating
- Bio‑inspired Optimisation
- FlexGen execution backend

These tests are optional; they skip gracefully if the corresponding manifest files are not present.
"""

import pytest
import yaml
import os


# ------------------------------------------------------------------------------
# Existing tests (unchanged)
# ------------------------------------------------------------------------------

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


# ------------------------------------------------------------------------------
# Enhanced tests (new)
# ------------------------------------------------------------------------------

def test_enhancement_configmap_exists_and_has_keys():
    """Verify that the enhancement ConfigMap exists and contains advanced module settings."""
    cm_path = "config/base/enhancements-configmap.yaml"
    if not os.path.exists(cm_path):
        pytest.skip(f"{cm_path} does not exist – skipping enhancement ConfigMap test")

    with open(cm_path, 'r') as f:
        cm = yaml.safe_load(f)

    assert cm is not None
    assert cm.get('kind') == 'ConfigMap'
    data = cm.get('data', {})
    # The ConfigMap should contain a YAML config with the 'enhancements' section
    assert 'green_agent_config.yaml' in data, "Enhancement ConfigMap missing 'green_agent_config.yaml'"
    enh_config = yaml.safe_load(data['green_agent_config.yaml'])
    assert 'enhancements' in enh_config, "No 'enhancements' section in ConfigMap"
    enhancements = enh_config['enhancements']
    # Check for key sub-sections
    for key in ['limit_graph', 'modp', 'rlhf', 'distillation', 'bio_inspired', 'moe_expert']:
        assert key in enhancements, f"Missing enhancement section: {key}"
    print("✅ Enhancement ConfigMap is valid and contains all advanced module sections")


def test_raycluster_has_enhancement_env_vars():
    """Verify that the RayCluster manifest includes environment variables for enhancements."""
    ray_path = "config/base/ray-cluster-enhanced.yaml"
    if not os.path.exists(ray_path):
        pytest.skip(f"{ray_path} does not exist – skipping enhanced RayCluster test")

    with open(ray_path, 'r') as f:
        ray_cluster = yaml.safe_load(f)

    assert ray_cluster is not None
    assert ray_cluster.get('kind') == 'RayCluster'
    head_env = ray_cluster['spec']['headGroupSpec']['template']['spec']['containers'][0].get('env', [])
    worker_env = ray_cluster['spec']['workerGroupSpecs'][0]['template']['spec']['containers'][0].get('env', [])

    env_map_head = {e['name']: e['value'] for e in head_env}
    env_map_worker = {e['name']: e['value'] for e in worker_env}

    # Required enhancement flags
    required_env = {
        'ENHANCEMENTS_ENABLED': 'true',
        'LIMIT_GRAPH_ENABLED': 'true',
        'MODP_ENABLED': 'true',
        'RLHF_ENABLED': 'true',
        'DISTILLATION_ENABLED': 'true',
        'MOE_GATING_ENABLED': 'true',
        'EVOLUTIONARY_ENABLED': 'true',
        'FLEXGEN_ENABLED': 'true',
    }
    for var, expected in required_env.items():
        assert env_map_head.get(var) == expected, f"Head container missing {var}={expected}"
        assert env_map_worker.get(var) == expected, f"Worker container missing {var}={expected}"
    print("✅ Enhanced RayCluster has all required environment variables for head and workers")


def test_hpa_contains_enhanced_metrics():
    """Verify that the HPA manifest includes custom metrics for MODP, RLHF, etc."""
    hpa_path = "config/base/hpa-enhanced.yaml"
    if not os.path.exists(hpa_path):
        pytest.skip(f"{hpa_path} does not exist – skipping enhanced HPA test")

    with open(hpa_path, 'r') as f:
        hpa = yaml.safe_load(f)

    assert hpa is not None
    assert hpa.get('kind') == 'HorizontalPodAutoscaler'
    metrics = hpa['spec']['metrics']
    metric_names = set()
    for metric in metrics:
        if metric['type'] == 'Pods':
            metric_names.add(metric['pods']['metric']['name'])
        elif metric['type'] == 'External':
            metric_names.add(metric['external']['metric']['name'])
        elif metric['type'] == 'Resource':
            metric_names.add(metric['resource']['name'])

    required_metrics = {
        'modp_score',
        'rlhf_feedback_score',
        'graph_centrality',
        'distillation_update_rate',
        'moe_gate_stddev',
        'evolutionary_best_fitness',
        'flexgen_energy_rate',
    }
    assert required_metrics.issubset(metric_names), f"Missing enhanced metrics: {required_metrics - metric_names}"
    print("✅ Enhanced HPA contains all required custom metrics")


def test_flexgen_config_present_in_base_config():
    """Verify that FlexGen settings appear in the base green_agent_config.yaml."""
    base_config_path = "config/base/green_agent_config.yaml"
    if not os.path.exists(base_config_path):
        pytest.skip(f"{base_config_path} does not exist – skipping FlexGen test")

    with open(base_config_path, 'r') as f:
        config = yaml.safe_load(f)

    assert config is not None
    assert 'flexgen' in config, "FlexGen section missing in base config"
    flexgen = config['flexgen']
    assert flexgen.get('enabled') is not None
    assert flexgen.get('model_name') is not None
    assert flexgen.get('delegation_policy') in ['adaptive', 'always', 'never']
    print("✅ FlexGen configuration is present in base config")


def test_network_policy_allows_enhanced_ports():
    """Verify that the network policy includes ports needed by enhanced modules."""
    np_path = "config/base/network-policy-enhanced.yaml"
    if not os.path.exists(np_path):
        pytest.skip(f"{np_path} does not exist – skipping enhanced NetworkPolicy test")

    with open(np_path, 'r') as f:
        netpol = yaml.safe_load(f)

    assert netpol is not None
    assert netpol.get('kind') == 'NetworkPolicy'

    # Check ingress and egress ports
    ingress_ports = set()
    for rule in netpol['spec'].get('ingress', []):
        for port in rule.get('ports', []):
            ingress_ports.add(port.get('port'))

    egress_ports = set()
    for rule in netpol['spec'].get('egress', []):
        for port in rule.get('ports', []):
            egress_ports.add(port.get('port'))

    # These ports are typical for enhanced modules (adjust as needed)
    enhanced_ingress = {8080, 50051, 7687, 7474}
    enhanced_egress = {7687, 9443, 443}
    assert enhanced_ingress & ingress_ports, "Enhanced ingress ports missing"
    assert enhanced_egress & egress_ports, "Enhanced egress ports missing"
    print("✅ Enhanced NetworkPolicy includes ports for advanced modules")


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
