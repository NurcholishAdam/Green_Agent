import pytest
import subprocess
import json
import os
import yaml

NAMESPACE = os.getenv("GREEN_AGENT_NAMESPACE", "green-agent")
RESOURCE_PREFIX = os.getenv("GREEN_AGENT_RESOURCE_PREFIX", "")
SERVICE_NAME = f"{RESOURCE_PREFIX}green-agent"
HPA_NAME = f"{RESOURCE_PREFIX}green-agent"

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
        ['kubectl', 'get', 'pods', '-n', NAMESPACE, '-l', 'app.kubernetes.io/name=green-agent'],
        capture_output=True, text=True
    )
    assert 'Running' in result.stdout

def test_service_exists():
    """Test service exists"""
    result = subprocess.run(
        ['kubectl', 'get', 'svc', '-n', NAMESPACE, SERVICE_NAME],
        capture_output=True, text=True
    )
    assert result.returncode == 0


# ------------------------------------------------------------------------------
# Enhanced tests for advanced modules
# ------------------------------------------------------------------------------

def _get_pod_env(component_label="app.kubernetes.io/component=head"):
    """Retrieve effective environment variables from the first matching pod."""
    # Get pod name
    get_pod = subprocess.run(
        ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", component_label, "-o", "jsonpath={.items[0].metadata.name}"],
        capture_output=True, text=True
    )
    if get_pod.returncode != 0 or not get_pod.stdout.strip():
        pytest.skip(f"No pod found with label {component_label}")
    pod_name = get_pod.stdout.strip()
    get_env = subprocess.run(
        ["kubectl", "exec", pod_name, "-n", NAMESPACE, "--", "env"],
        capture_output=True, text=True
    )
    if get_env.returncode != 0:
        pytest.fail(f"Failed to retrieve environment variables: {get_env.stderr}")
    return dict(line.split("=", 1) for line in get_env.stdout.splitlines() if "=" in line)

def test_head_pod_has_enhancement_env_vars():
    """Head pod should have enhancement environment variables set."""
    env = _get_pod_env("app.kubernetes.io/component=head")
    required_vars = {
        "ENHANCEMENTS_ENABLED": "true",
        "LIMIT_GRAPH_ENABLED": "true",
        "MODP_ENABLED": "true",
        "RLHF_ENABLED": "true",
        "DISTILLATION_ENABLED": "true",
        "MOE_GATING_ENABLED": "true",
        "EVOLUTIONARY_ENABLED": "true",
        "FLEXGEN_ENABLED": "false",
    }
    for var, expected in required_vars.items():
        assert env.get(var) == expected, f"Missing or incorrect {var} in head pod"

def test_worker_pod_has_enhancement_env_vars():
    """Worker pods should have enhancement environment variables set."""
    env = _get_pod_env("app.kubernetes.io/component=worker")
    required_vars = {
        "ENHANCEMENTS_ENABLED": "true",
        "LIMIT_GRAPH_ENABLED": "true",
        "MODP_ENABLED": "true",
        "RLHF_ENABLED": "true",
        "DISTILLATION_ENABLED": "true",
        "MOE_GATING_ENABLED": "true",
        "EVOLUTIONARY_ENABLED": "true",
        "FLEXGEN_ENABLED": "false",
    }
    for var, expected in required_vars.items():
        assert env.get(var) == expected, f"Missing or incorrect {var} in worker pod"

def test_enhancement_configmap_exists():
    """Enhancement ConfigMap should be present and contain advanced settings."""
    result = _kubectl_get("configmap", f"{RESOURCE_PREFIX}green-agent-config", output="json")
    if result.returncode != 0:
        pytest.skip("green-agent-config ConfigMap not found")
    cm = json.loads(result.stdout)
    data = cm.get("data", {})
    config_yaml = data.get("green_agent.yaml", "")
    config = yaml.safe_load(config_yaml)
    enhancements = config.get("enhancements", {})
    assert {"limit_graph", "modp", "rlhf", "distillation", "evolutionary", "moe", "flexgen"} <= set(enhancements)

def test_flexgen_env_var_in_deployment():
    """Deployment/Pod should have FlexGen environment variables."""
    env = _get_pod_env("app.kubernetes.io/component=head")
    # FlexGen is intentionally disabled in the default deployment profile.
    if "FLEXGEN_ENABLED" not in env:
        pytest.skip("FlexGen not enabled in this deployment")
    assert env.get("FLEXGEN_ENABLED") == "false"
    assert "FLEXGEN_MODEL_NAME" in env
    assert "FLEXGEN_DEFAULT_PRECISION" in env

def test_hpa_has_resource_and_agent_metrics():
    """HPA should include CPU and the Green Agent workload metrics."""
    result = _kubectl_get("hpa", HPA_NAME, output="json")
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
    required_metrics = {"cpu", "green_agent_carbon_intensity_gco2_kwh", "green_agent_queue_depth"}
    assert required_metrics <= metric_names, f"Missing HPA metrics: {required_metrics - metric_names}"

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
