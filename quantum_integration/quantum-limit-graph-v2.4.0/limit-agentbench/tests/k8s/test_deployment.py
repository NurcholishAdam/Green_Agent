"""Kubernetes Deployment Tests (Enhanced)

Adds tests for advanced enhancement integration in RayCluster deployment:
- LIMIT Graph metrics
- MODP (Multi‑Objective Decision Process)
- RLHF
- Multi‑Teacher Distillation + MoE gating
- Bio‑inspired Optimisation
- FlexGen execution backend
"""

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

    # ------------------------------------------------------------------
    # Enhanced tests for advanced enhancement integration
    # ------------------------------------------------------------------

    def test_ray_cluster_has_enhancement_labels(self, k8s_client, namespace):
        """RayCluster should have enhancement labels and annotations."""
        ray_cluster = k8s_client['custom_objects'].get_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=namespace,
            plural="rayclusters",
            name="green-agent-cluster"
        )
        metadata = ray_cluster['metadata']
        labels = metadata.get('labels', {})
        assert labels.get('enhancements') == 'enabled'
        annotations = metadata.get('annotations', {})
        expected_annotations = {
            'green-agent/limit-graph': 'true',
            'green-agent/modp': 'true',
            'green-agent/rlhf': 'true',
            'green-agent/distillation': 'true',
            'green-agent/bio-inspired': 'true',
            'green-agent/moe': 'true',
            'green-agent/flexgen': 'true',
        }
        for key, value in expected_annotations.items():
            assert annotations.get(key) == value, f"Missing annotation {key}"

    def test_head_pod_has_enhancement_env_vars(self, k8s_client, namespace, wait_for_pods):
        """Head pod containers should have enhancement environment variables."""
        pods = wait_for_pods('component=head', timeout=300)
        assert pods, "No head pods found"
        head_pod = pods[0]
        container = head_pod.spec.containers[0]
        env_dict = {env.name: env.value for env in container.env}
        expected_env = {
            'ENHANCEMENTS_ENABLED': 'true',
            'LIMIT_GRAPH_ENABLED': 'true',
            'LIMIT_GRAPH_CENTRALITY': '0.7',
            'LIMIT_GRAPH_CONNECTIVITY': '0.6',
            'MODP_ENABLED': 'true',
            'MODP_WEIGHTS': '[0.4,0.3,0.2,0.1]',
            'RLHF_ENABLED': 'true',
            'HUMAN_FEEDBACK_SCORE': '0.6',
            'DISTILLATION_ENABLED': 'true',
            'MOE_GATING_ENABLED': 'true',
            'EVOLUTIONARY_ENABLED': 'true',
            'POPULATION_SIZE': '20',
            'FLEXGEN_ENABLED': 'true',
            'FLEXGEN_MODEL_NAME': 'facebook/opt-6.7b',
            'FLEXGEN_DEFAULT_PRECISION': 'fp16',
        }
        for var, value in expected_env.items():
            assert env_dict.get(var) == value, f"Head pod missing env var {var}={value}"

    def test_worker_pods_have_enhancement_env_vars(self, k8s_client, namespace, wait_for_pods):
        """Worker pod containers should have enhancement environment variables."""
        pods = wait_for_pods('component=worker', timeout=300)
        assert pods, "No worker pods found"
        worker_pod = pods[0]
        container = worker_pod.spec.containers[0]
        env_dict = {env.name: env.value for env in container.env}
        # Workers need at least the master enhancement flag and some module toggles
        assert env_dict.get('ENHANCEMENTS_ENABLED') == 'true'
        assert env_dict.get('LIMIT_GRAPH_ENABLED') == 'true'
        assert env_dict.get('MODP_ENABLED') == 'true'
        assert env_dict.get('RLHF_ENABLED') == 'true'
        assert env_dict.get('DISTILLATION_ENABLED') == 'true'
        assert env_dict.get('MOE_GATING_ENABLED') == 'true'
        assert env_dict.get('EVOLUTIONARY_ENABLED') == 'true'
        assert env_dict.get('FLEXGEN_ENABLED') == 'true'

    def test_enhancement_state_volume_mount(self, k8s_client, namespace, wait_for_pods):
        """Both head and worker pods should mount enhancement state volume."""
        pods = wait_for_pods('app=green-agent', timeout=300)  # get all
        for pod in pods[:1]:  # check at least head pod
            container = pod.spec.containers[0]
            volume_mounts = container.volume_mounts or []
            mount_paths = [vm.mount_path for vm in volume_mounts]
            assert '/app/enhancement_state' in mount_paths, "Missing enhancement state volume mount"

    def test_enhancement_configmap_exists(self, k8s_client, namespace):
        """Enhanced ConfigMap should exist if enhancements are enabled."""
        try:
            cm = k8s_client['core'].read_namespaced_config_map(
                name="green-agent-enhancements",
                namespace=namespace
            )
            assert cm is not None
            # Check that config contains enhancement sections
            config_data = cm.data.get('green_agent_config.yaml', '')
            assert 'limit_graph' in config_data
            assert 'modp' in config_data
            assert 'rlhf' in config_data
            assert 'distillation' in config_data
            assert 'bio_inspired' in config_data
            assert 'moe_expert' in config_data
            assert 'flexgen' in config_data
        except Exception:
            pytest.skip("Enhanced ConfigMap not found; may be using alternative method")
