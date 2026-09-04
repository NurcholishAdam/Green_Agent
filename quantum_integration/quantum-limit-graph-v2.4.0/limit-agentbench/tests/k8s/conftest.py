"""Pytest fixtures for Kubernetes integration tests (Enhanced)

Adds fixtures for advanced enhancements:
- LIMIT Graph metrics
- MODP (Multi‑Objective Decision Process) weights
- RLHF (human feedback)
- Multi‑Teacher On‑Policy Distillation with MoE gating
- Bio‑inspired Optimisation parameters
- FlexGen execution backend
"""

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
        'custom_objects': client.CustomObjectsApi(),
        'rbac': client.RbacAuthorizationV1Api(),
        'networking': client.NetworkingV1Api(),  # Added for network policy checks
        'storage': client.StorageV1Api()          # Added for PVC/StorageClass checks
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

# ------------------------------------------------------------------------------
# Enhanced fixtures for advanced modules
# ------------------------------------------------------------------------------

@pytest.fixture(scope="session")
def enhancement_config():
    """Return a dictionary with default enhancement settings.

    This configuration can be used to create ConfigMaps, env vars, or
    to verify that the deployed resources match expected values.
    """
    return {
        "enabled": True,
        "limit_graph": {
            "enabled": True,
            "centrality": 0.7,
            "connectivity": 0.6,
            "density": 0.5,
            "update_interval_seconds": 60
        },
        "modp": {
            "enabled": True,
            "objective_weights": [0.4, 0.3, 0.2, 0.1],
            "normalize_weights": True
        },
        "rlhf": {
            "enabled": True,
            "human_feedback_score": 0.6,
            "collect_feedback": True,
            "feedback_source": "dashboard"
        },
        "distillation": {
            "enabled": True,
            "use_moe_gating": True,
            "distillation_lr": 0.01,
            "gating_lr": 0.005,
            "replay_size": 2000,
            "train_every": 10,
            "epsilon": 0.1,
            "teachers": ["rule_based", "historical_ml", "q_learning", "rlhf"]
        },
        "bio_inspired": {
            "enabled": True,
            "use_evolutionary": True,
            "population_size": 20,
            "mutation_rate": 0.1,
            "crossover_rate": 0.7,
            "elitism": 2
        },
        "moe_expert": {
            "enabled": True,
            "n_experts": 4,
            "gating_lr": 0.005
        },
        "flexgen": {
            "enabled": True,
            "model_name": "facebook/opt-6.7b",
            "batch_size": 16,
            "memory_limit_mb": 4096,
            "precision_options": ["fp32", "fp16", "int8"],
            "default_precision": "fp16",
            "delegation_policy": "adaptive"
        }
    }

@pytest.fixture
def create_enhancement_configmap(k8s_client, namespace, enhancement_config):
    """
    Create a ConfigMap named 'green-agent-enhancements' with the enhancement
    configuration YAML. Yields the ConfigMap name and cleans up afterwards.
    """
    configmap_name = "green-agent-enhancements"
    config_yaml = yaml.dump({"enhancements": enhancement_config})
    body = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(
            name=configmap_name,
            namespace=namespace,
            labels={"app": "green-agent", "enhancements": "enabled"},
            annotations={
                "green-agent/limit-graph": "true",
                "green-agent/modp": "true",
                "green-agent/rlhf": "true",
                "green-agent/distillation": "true",
                "green-agent/bio-inspired": "true",
                "green-agent/moe": "true",
                "green-agent/flexgen": "true"
            }
        ),
        data={"green_agent_config.yaml": config_yaml}
    )
    k8s_client['core'].create_namespaced_config_map(
        namespace=namespace, body=body
    )
    yield configmap_name
    # Cleanup
    try:
        k8s_client['core'].delete_namespaced_config_map(
            name=configmap_name, namespace=namespace
        )
    except kubernetes.client.exceptions.ApiException:
        pass  # Already deleted

@pytest.fixture
def apply_enhancement_env_to_raycluster(k8s_client, namespace, enhancement_config):
    """
    Patch the RayCluster head and worker containers with enhancement environment
    variables. Yields nothing; used for tests that require env vars to be set.
    """
    ray_cluster_name = "green-agent-cluster"
    # Build env list from enhancement_config
    env_vars = [
        {"name": "ENHANCEMENTS_ENABLED", "value": "true"},
        {"name": "LIMIT_GRAPH_ENABLED", "value": str(enhancement_config['limit_graph']['enabled']).lower()},
        {"name": "LIMIT_GRAPH_CENTRALITY", "value": str(enhancement_config['limit_graph']['centrality'])},
        {"name": "LIMIT_GRAPH_CONNECTIVITY", "value": str(enhancement_config['limit_graph']['connectivity'])},
        {"name": "MODP_ENABLED", "value": str(enhancement_config['modp']['enabled']).lower()},
        {"name": "MODP_WEIGHTS", "value": json.dumps(enhancement_config['modp']['objective_weights'])},
        {"name": "RLHF_ENABLED", "value": str(enhancement_config['rlhf']['enabled']).lower()},
        {"name": "HUMAN_FEEDBACK_SCORE", "value": str(enhancement_config['rlhf']['human_feedback_score'])},
        {"name": "DISTILLATION_ENABLED", "value": str(enhancement_config['distillation']['enabled']).lower()},
        {"name": "MOE_GATING_ENABLED", "value": str(enhancement_config['distillation']['use_moe_gating']).lower()},
        {"name": "EVOLUTIONARY_ENABLED", "value": str(enhancement_config['bio_inspired']['use_evolutionary']).lower()},
        {"name": "POPULATION_SIZE", "value": str(enhancement_config['bio_inspired']['population_size'])},
        {"name": "MUTATION_RATE", "value": str(enhancement_config['bio_inspired']['mutation_rate'])},
        {"name": "FLEXGEN_ENABLED", "value": str(enhancement_config['flexgen']['enabled']).lower()},
        {"name": "FLEXGEN_MODEL_NAME", "value": enhancement_config['flexgen']['model_name']},
        {"name": "FLEXGEN_DEFAULT_PRECISION", "value": enhancement_config['flexgen']['default_precision']},
    ]
    # Patch head and worker containers via strategic merge
    patch_body = {
        "spec": {
            "headGroupSpec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "ray-head",
                            "env": env_vars
                        }]
                    }
                }
            },
            "workerGroupSpecs": [{
                "groupName": "standard-workers",
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "ray-worker",
                            "env": env_vars
                        }]
                    }
                }
            }]
        }
    }
    k8s_client['custom_objects'].patch_namespaced_custom_object(
        group="ray.io",
        version="v1",
        namespace=namespace,
        plural="rayclusters",
        name=ray_cluster_name,
        body=patch_body
    )
    yield
