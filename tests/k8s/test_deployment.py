import pytest
import subprocess
import json
import os

NAMESPACE = os.getenv("GREEN_AGENT_NAMESPACE", "green-agent-test")

def _kubectl_get(resource_type, resource_name, namespace=NAMESPACE, output="json"):
    """Helper to run kubectl get and return parsed JSON or raw output."""
    cmd = ["kubectl", "get", resource_type, resource_name, "-n", namespace]
    if output == "json":
        cmd += ["-o", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

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
        ['kubectl', 'get', 'pods', '-n', NAMESPACE, '-l', 'app=green-agent'],
        capture_output=True, text=True
    )
    assert 'Running' in result.stdout

def test_service_exists():
    """Test service exists"""
    result = subprocess.run(
        ['kubectl', 'get', 'svc', '-n', NAMESPACE, 'dev-green-agent'],
        capture_output=True, text=True
    )
    assert result.returncode == 0


# ------------------------------------------------------------------------------
# Enhanced tests for advanced modules
# ------------------------------------------------------------------------------

def _get_pod_env(component_label="component=head"):
    """Retrieve environment variables of the first pod matching the label."""
    # Get pod name
    get_pod = subprocess.run(
        ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", component_label, "-o", "jsonpath={.items[0].metadata.name}"],
        capture_output=True, text=True
    )
    if get_pod.returncode != 0 or not get_pod.stdout.strip():
        pytest.skip(f"No pod found with label {component_label}")
    pod_name = get_pod.stdout.strip()
    # Get env vars
    get_env = subprocess.run(
        ["kubectl", "get", "pod", pod_name, "-n", NAMESPACE, "-o", "jsonpath={.spec.containers[0].env}"],
        capture_output=True, text=True
    )
    if get_env.returncode != 0:
        pytest.skip("Failed to retrieve environment variables")
    # Parse JSON
    try:
        env_list = json.loads(get_env.stdout)
    except json.JSONDecodeError:
        pytest.skip("Environment variables not in JSON format")
    env_dict = {item.get("name"): item.get("value", "") for item in env_list}
    return env_dict

def test_head_pod_has_enhancement_env_vars():
    """Head pod should have enhancement environment variables set."""
    env = _get_pod_env("component=head")
    required_vars = {
        "ENHANCEMENTS_ENABLED": "true",
        "LIMIT_GRAPH_ENABLED": "true",
        "MODP_ENABLED": "true",
        "RLHF_ENABLED": "true",
        "DISTILLATION_ENABLED": "true",
        "MOE_GATING_ENABLED": "true",
        "EVOLUTIONARY_ENABLED": "true",
        "FLEXGEN_ENABLED": "true",
    }
    for var, expected in required_vars.items():
        assert env.get(var) == expected, f"Missing or incorrect {var} in head pod"

def test_worker_pod_has_enhancement_env_vars():
    """Worker pods should have enhancement environment variables set."""
    env = _get_pod_env("component=worker")
    required_vars = {
        "ENHANCEMENTS_ENABLED": "true",
        "LIMIT_GRAPH_ENABLED": "true",
        "MODP_ENABLED": "true",
        "RLHF_ENABLED": "true",
        "DISTILLATION_ENABLED": "true",
        "MOE_GATING_ENABLED": "true",
        "EVOLUTIONARY_ENABLED": "true",
        "FLEXGEN_ENABLED": "true",
    }
    for var, expected in required_vars.items():
        assert env.get(var) == expected, f"Missing or incorrect {var} in worker pod"

def test_enhancement_configmap_exists():
    """Enhancement ConfigMap should be present and contain advanced settings."""
    result = _kubectl_get("configmap", "green-agent-enhancements", output="json")
    if result.returncode != 0:
        pytest.skip("green-agent-enhancements ConfigMap not found")
    cm = json.loads(result.stdout)
    data = cm.get("data", {})
    config_yaml = data.get("green_agent_config.yaml", "")
    assert "limit_graph" in config_yaml
    assert "modp" in config_yaml
    assert "rlhf" in config_yaml
    assert "distillation" in config_yaml
    assert "bio_inspired" in config_yaml
    assert "moe_expert" in config_yaml
    assert "flexgen" in config_yaml

def test_flexgen_env_var_in_deployment():
    """Deployment/Pod should have FlexGen environment variables."""
    env = _get_pod_env("component=head")
    # FlexGen may not be enabled in all environments; we check if present, not mandatory.
    # If not present, the test passes but prints a warning.
    if "FLEXGEN_ENABLED" not in env:
        pytest.skip("FlexGen not enabled in this deployment")
    assert env.get("FLEXGEN_ENABLED") == "true"
    assert "FLEXGEN_MODEL_NAME" in env
    assert "FLEXGEN_DEFAULT_PRECISION" in env

def test_hpa_has_enhanced_metrics():
    """HPA should include custom metrics for MODP, RLHF, etc."""
    result = _kubectl_get("hpa", "green-agent-hpa", output="json")
    if result.returncode != 0:
        pytest.skip("HPA not found")
    hpa = json.loads(result.stdout)
    metrics = hpa.get("spec", {}).get("metrics", [])
    metric_names = set()
    for metric in metrics:
        if metric.get("type") == "Pods":
            metric_names.add(metric["pods"]["metric"]["name"])
        elif metric.get("type") == "External":
            metric_names.add(metric["external"]["metric"]["name"])
        elif metric.get("type") == "Resource":
            metric_names.add(metric["resource"]["name"])
    required_metrics = {
        "modp_score",
        "rlhf_feedback_score",
        "graph_centrality",
        "distillation_update_rate",
        "moe_gate_stddev",
        "evolutionary_best_fitness",
        "flexgen_energy_rate",
    }
    assert required_metrics.issubset(metric_names), f"Missing enhanced metrics: {required_metrics - metric_names}"

def test_prometheus_adapter_has_enhanced_metrics_config():
    """Prometheus adapter custom metrics config should include enhanced metrics."""
    # Check ConfigMap in monitoring namespace (or wherever prometheus-adapter is installed)
    # Adjust namespace as needed; we'll try 'monitoring' first.
    result = _kubectl_get("configmap", "custom-metrics-config", namespace="monitoring", output="json")
    if result.returncode != 0:
        # Maybe in same namespace? skip if not found
        pytest.skip("custom-metrics-config ConfigMap not found in monitoring namespace")
    cm = json.loads(result.stdout)
    config_yaml = cm.get("data", {}).get("config.yaml", "")
    for metric in ["green_agent_modp_score", "green_agent_rlhf_feedback", "green_agent_graph_centrality"]:
        assert metric in config_yaml, f"{metric} not found in custom metrics config"
