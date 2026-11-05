# -*- coding: utf-8 -*-
"""
Pytest fixtures for Kubernetes integration tests (Enhanced v2).

Adds fixtures for advanced enhancements:
    - LIMIT Graph metrics
    - MODP (Multi-Objective Decision Process) weights
    - RLHF (human feedback)
    - Multi-Teacher On-Policy Distillation with MoE gating
    - Bio-inspired Optimisation parameters
    - FlexGen execution backend

---- Fixes vs v1 ---------------------------------------------------------------
  FIX #1   import json  (v1 used json.dumps without importing json → NameError
           at fixture setup time).
  FIX #2,3 RayCluster patch: read → mutate → replace → restore. v1 used a
           strategic-merge patch on a CRD with no merge key, which replaced
           the entire containers list and destroyed ports/resources/probes/
           volumes. v1 also had no teardown.
  FIX #4,5,7,16
           wait_for_pods: Ready condition (not just Running), supports
           expected_count, uses pytest.fail with diagnostics, and does not
           swallow 401/403/404 with a bare `except:`.
  FIX #6,9 k8s_client: validates the cluster is reachable; skips (or fails
           under STRICT_INTEGRATION=1) with a clean message.
  FIX #8,13 namespace: env-configurable via GREEN_AGENT_TEST_NAMESPACE
           (defaults to green-agent-development — green-agent-test does not
           exist in v2) and existence-checked.
  FIX #10  create_enhancement_configmap: idempotent (create-or-replace), and
           returns a dict the test can inspect (name, namespace, data).
  FIX #11  enhancement_config: fresh deep copy per test — mutations do not
           leak into other tests.
  FIX #14  _request_timeout on every Kubernetes API call.
  FIX #17,18
           apply_enhancement_env_to_raycluster: returns the env it set, and
           restores the original spec on teardown.
  FIX #21  pytest_configure registers requires_cluster / k8s / enhancements
           markers.
  FIX #22  yaml.safe_dump(sort_keys=False) instead of yaml.dump.
  FIX #25  wait_for_raycluster_ready helper — head + N workers.
  FIX #26  STRICT_INTEGRATION=1 flips skips into failures for CI gating.

---- Backward compatibility ---------------------------------------------------
  Fixture names and signatures are unchanged. Tests that use
  ``k8s_client``, ``namespace``, ``wait_for_pods``, ``enhancement_config``,
  ``create_enhancement_configmap`` and ``apply_enhancement_env_to_raycluster``
  continue to work — they just skip cleanly when the cluster is unreachable,
  and never corrupt the RayCluster.

---- Environment variables ----------------------------------------------------
  GREEN_AGENT_TEST_NAMESPACE  target namespace (default: green-agent-development)
  GREEN_AGENT_RAY_CLUSTER     RayCluster name (default: green-agent-cluster)
  GREEN_AGENT_K8S_TIMEOUT     per-API-call timeout in seconds (default: 10)
  STRICT_INTEGRATION          set to "1" to fail instead of skip on missing
                              infrastructure (use in CI)
"""

from __future__ import annotations

import copy
import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

import pytest
import yaml
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
STRICT_INTEGRATION: bool = os.environ.get("STRICT_INTEGRATION", "0") == "1"
DEFAULT_NAMESPACE: str = os.environ.get(
    "GREEN_AGENT_TEST_NAMESPACE", "green-agent-development",
)
RAY_CLUSTER_NAME: str = os.environ.get(
    "GREEN_AGENT_RAY_CLUSTER", "green-agent-cluster",
)
API_TIMEOUT: float = float(os.environ.get("GREEN_AGENT_K8S_TIMEOUT", "10"))


# ---------------------------------------------------------------------------
# pytest configuration
# ---------------------------------------------------------------------------
def pytest_configure(config: pytest.Config) -> None:
    """Register markers used by these fixtures and the tests that use them."""
    for marker, desc in [
        ("requires_cluster", "requires a live Kubernetes cluster"),
        ("k8s", "Kubernetes API interaction"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
        ("slow", "long-running test"),
    ]:
        config.addinivalue_line("markers", f"{marker}: {desc}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _skip_or_fail(msg: str) -> None:
    """Skip normally; fail under STRICT_INTEGRATION=1 (CI gating)."""
    if STRICT_INTEGRATION:
        pytest.fail(msg)
    pytest.skip(msg)


def _is_pod_ready(pod: Any) -> bool:
    """Return True iff the pod is Running *and* every container is Ready."""
    if pod is None or pod.status is None:
        return False
    if pod.status.phase != "Running":
        return False
    statuses = pod.status.container_statuses or []
    if not statuses:
        return False
    return all(getattr(cs, "ready", False) for cs in statuses)


def _pod_diagnostics(pods: List[Any]) -> str:
    """Compact one-line summary for pytest.fail messages."""
    if not pods:
        return "no pods matched"
    lines = []
    for p in pods:
        name = getattr(getattr(p, "metadata", None), "name", "?")
        phase = getattr(getattr(p, "status", None), "phase", "?")
        statuses = getattr(getattr(p, "status", None),
                           "container_statuses", None) or []
        ready = sum(1 for cs in statuses if getattr(cs, "ready", False))
        total = len(statuses)
        lines.append(f"{name}(phase={phase}, ready={ready}/{total})")
    return "; ".join(lines)


def _default_enhancement_config() -> Dict[str, Any]:
    """Canonical enhancement config — deep-copied for each test."""
    return {
        "enabled": True,
        "limit_graph": {
            "enabled": True,
            "centrality": 0.7,
            "connectivity": 0.6,
            "density": 0.5,
            "update_interval_seconds": 60,
        },
        "modp": {
            "enabled": True,
            "objective_weights": [0.4, 0.3, 0.2, 0.1],
            "normalize_weights": True,
        },
        "rlhf": {
            "enabled": True,
            "human_feedback_score": 0.6,
            "collect_feedback": True,
            "feedback_source": "dashboard",
        },
        "distillation": {
            "enabled": True,
            "use_moe_gating": True,
            "distillation_lr": 0.01,
            "gating_lr": 0.005,
            "replay_size": 2000,
            "train_every": 10,
            "epsilon": 0.1,
            "teachers": [
                "rule_based", "historical_ml", "q_learning", "rlhf",
            ],
        },
        "bio_inspired": {
            "enabled": True,
            "use_evolutionary": True,
            "population_size": 20,
            "mutation_rate": 0.1,
            "crossover_rate": 0.7,
            "elitism": 2,
        },
        "moe_expert": {
            "enabled": True,
            "n_experts": 4,
            "gating_lr": 0.005,
        },
        "flexgen": {
            "enabled": True,
            "model_name": "facebook/opt-6.7b",
            "batch_size": 16,
            "memory_limit_mb": 4096,
            "precision_options": ["fp32", "fp16", "int8"],
            "default_precision": "fp16",
            "delegation_policy": "adaptive",
        },
    }


def _build_env_vars(cfg: Dict[str, Any]) -> List[Dict[str, str]]:
    """Translate enhancement_config into a flat, mergeable env-var list."""
    return [
        {"name": "ENHANCEMENTS_ENABLED",
         "value": str(bool(cfg.get("enabled", False))).lower()},

        {"name": "LIMIT_GRAPH_ENABLED",
         "value": str(bool(cfg["limit_graph"]["enabled"])).lower()},
        {"name": "LIMIT_GRAPH_CENTRALITY",
         "value": str(cfg["limit_graph"]["centrality"])},
        {"name": "LIMIT_GRAPH_CONNECTIVITY",
         "value": str(cfg["limit_graph"]["connectivity"])},

        {"name": "MODP_ENABLED",
         "value": str(bool(cfg["modp"]["enabled"])).lower()},
        {"name": "MODP_WEIGHTS",
         "value": json.dumps(cfg["modp"]["objective_weights"])},

        {"name": "RLHF_ENABLED",
         "value": str(bool(cfg["rlhf"]["enabled"])).lower()},
        {"name": "HUMAN_FEEDBACK_SCORE",
         "value": str(cfg["rlhf"]["human_feedback_score"])},

        {"name": "DISTILLATION_ENABLED",
         "value": str(bool(cfg["distillation"]["enabled"])).lower()},
        {"name": "MOE_GATING_ENABLED",
         "value": str(bool(cfg["distillation"]["use_moe_gating"])).lower()},
        {"name": "DISTILLATION_TEACHERS",
         "value": json.dumps(cfg["distillation"]["teachers"])},

        {"name": "EVOLUTIONARY_ENABLED",
         "value": str(bool(cfg["bio_inspired"]["use_evolutionary"])).lower()},
        {"name": "POPULATION_SIZE",
         "value": str(cfg["bio_inspired"]["population_size"])},
        {"name": "MUTATION_RATE",
         "value": str(cfg["bio_inspired"]["mutation_rate"])},

        {"name": "FLEXGEN_ENABLED",
         "value": str(bool(cfg["flexgen"]["enabled"])).lower()},
        {"name": "FLEXGEN_MODEL_NAME",
         "value": str(cfg["flexgen"]["model_name"])},
        {"name": "FLEXGEN_DEFAULT_PRECISION",
         "value": str(cfg["flexgen"]["default_precision"])},
    ]


def _merge_env_into_container(container: Dict[str, Any],
                              new_env: List[Dict[str, str]]) -> None:
    """Merge env vars by name, preserving unrelated entries already present."""
    existing = {e.get("name"): e for e in (container.get("env") or [])}
    for entry in new_env:
        existing[entry["name"]] = entry
    container["env"] = list(existing.values())


# ---------------------------------------------------------------------------
# Session-scoped core fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def k8s_client() -> Dict[str, Any]:
    """
    Initialized and validated Kubernetes API clients.

    Probes ``list_namespace`` once — cheap, always allowed for any kubeconfig
    with cluster-read — so tests fail fast with a clear message instead of
    crashing deep inside an unrelated call.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except Exception as exc:                       # noqa: BLE001
            _skip_or_fail(f"No Kubernetes configuration available: {exc}")

    clients: Dict[str, Any] = {
        "core": client.CoreV1Api(),
        "apps": client.AppsV1Api(),
        "autoscaling": client.AutoscalingV2Api(),
        "custom_objects": client.CustomObjectsApi(),
        "rbac": client.RbacAuthorizationV1Api(),
        "networking": client.NetworkingV1Api(),
        "storage": client.StorageV1Api(),
    }

    try:
        clients["core"].list_namespace(limit=1, _request_timeout=API_TIMEOUT)
    except ApiException as exc:
        _skip_or_fail(
            f"Kubernetes API unreachable "
            f"({exc.status} {exc.reason})"
        )
    except Exception as exc:                           # noqa: BLE001
        _skip_or_fail(f"Kubernetes API unreachable: {exc}")
    return clients


@pytest.fixture(scope="session")
def namespace(k8s_client) -> str:
    """
    Target namespace. Env-configurable and existence-checked so downstream
    tests get a clean skip instead of a wall of 404s.
    """
    ns = DEFAULT_NAMESPACE
    try:
        k8s_client["core"].read_namespace(
            ns, _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        if exc.status == 404:
            _skip_or_fail(
                f"Namespace '{ns}' does not exist. "
                f"Deploy it first, or set GREEN_AGENT_TEST_NAMESPACE."
            )
        _skip_or_fail(f"Cannot read namespace '{ns}': {exc.reason}")
    except Exception as exc:                           # noqa: BLE001
        _skip_or_fail(f"Cannot read namespace '{ns}': {exc}")
    return ns


# ---------------------------------------------------------------------------
# Function-scoped polling helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def wait_for_pods(k8s_client, namespace) -> Callable[..., List[Any]]:
    """
    Return a function that polls pods matching ``label_selector`` until at
    least ``expected_count`` are Ready (phase Running + every container
    ready).

    Raises via ``pytest.fail`` with a pod diagnostic line on timeout.
    """
    def _wait(label_selector: str,
              timeout: float = 300.0,
              expected_count: int = 1) -> List[Any]:
        deadline = time.time() + timeout
        last_seen: List[Any] = []
        while time.time() < deadline:
            try:
                last_seen = k8s_client["core"].list_namespaced_pod(
                    namespace=namespace,
                    label_selector=label_selector,
                    _request_timeout=API_TIMEOUT,
                ).items
            except ApiException as exc:
                if exc.status in (401, 403, 404):
                    pytest.fail(
                        f"Cannot list pods in '{namespace}' "
                        f"({exc.status} {exc.reason})"
                    )
                time.sleep(2)
                continue

            ready = [p for p in last_seen if _is_pod_ready(p)]
            if len(ready) >= expected_count:
                return ready
            time.sleep(2)

        pytest.fail(
            f"Pods matching '{label_selector}' not Ready within {timeout}s "
            f"(wanted {expected_count}). Last seen: {_pod_diagnostics(last_seen)}"
        )
    return _wait


@pytest.fixture
def wait_for_raycluster_ready(k8s_client, namespace, wait_for_pods
                              ) -> Callable[..., List[Any]]:
    """Wait for the Green Agent RayCluster pods (head + N workers) to be Ready."""
    def _wait(timeout: float = 300.0, expected_workers: int = 2) -> List[Any]:
        return wait_for_pods(
            f"ray.io/cluster={RAY_CLUSTER_NAME}",
            timeout=timeout,
            expected_count=expected_workers + 1,
        )
    return _wait


# ---------------------------------------------------------------------------
# Enhancement fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def enhancement_config() -> Dict[str, Any]:
    """
    Fresh, deep-copied enhancement config per test.

    v1 returned a shared session-scoped dict; a single mutation in any test
    silently changed the config for every subsequent test.
    """
    return copy.deepcopy(_default_enhancement_config())


@pytest.fixture
def create_enhancement_configmap(
    k8s_client, namespace, enhancement_config,
) -> Dict[str, Any]:
    """
    Idempotently create ``green-agent-enhancements`` ConfigMap.

    Yields ``{"name", "namespace", "data"}`` so tests can inspect what was
    written. Deletes the ConfigMap on teardown (idempotent — a 404 is
    ignored). v1 used ``create_namespaced_config_map`` unconditionally and
    raised 409 on the second run in the same cluster.
    """
    cm_name = "green-agent-enhancements"
    payload = {"enhancements": enhancement_config}
    cm_yaml = yaml.safe_dump(payload, sort_keys=False)

    body = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(
            name=cm_name,
            namespace=namespace,
            labels={"app": "green-agent", "enhancements": "enabled"},
            annotations={
                f"green-agent/{k}": "true"
                for k in (
                    "limit-graph", "modp", "rlhf", "distillation",
                    "bio-inspired", "moe", "flexgen",
                )
            },
        ),
        data={"green_agent_config.yaml": cm_yaml},
    )

    # Idempotent: replace if exists, create otherwise.
    try:
        k8s_client["core"].replace_namespaced_config_map(
            name=cm_name, namespace=namespace, body=body,
            _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        if exc.status == 404:
            k8s_client["core"].create_namespaced_config_map(
                namespace=namespace, body=body, _request_timeout=API_TIMEOUT,
            )
        else:
            raise

    try:
        yield {"name": cm_name, "namespace": namespace, "data": payload}
    finally:
        try:
            k8s_client["core"].delete_namespaced_config_map(
                name=cm_name, namespace=namespace,
                _request_timeout=API_TIMEOUT,
            )
        except ApiException as exc:
            if exc.status != 404:
                logger.warning(
                    "Failed to delete ConfigMap %s/%s: %s",
                    namespace, cm_name, exc.reason,
                )


@pytest.fixture
def apply_enhancement_env_to_raycluster(
    k8s_client, namespace, enhancement_config,
) -> Dict[str, Any]:
    """
    Patch the RayCluster head and worker containers with enhancement env vars.

    Uses **read → mutate → replace** so existing env vars, ports, resources,
    probes, and volumes are preserved. Ray's CRD has no merge key, so a
    strategic-merge patch (v1) would replace the containers list entirely.

    The original spec is restored on teardown.
    """
    co = k8s_client["custom_objects"]
    try:
        original = co.get_namespaced_custom_object(
            group="ray.io", version="v1", namespace=namespace,
            plural="rayclusters", name=RAY_CLUSTER_NAME,
            _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        if exc.status == 404:
            _skip_or_fail(
                f"RayCluster '{RAY_CLUSTER_NAME}' not found in '{namespace}'"
            )
        _skip_or_fail(f"Cannot read RayCluster: {exc.reason}")

    patched = copy.deepcopy(original)
    env_vars = _build_env_vars(enhancement_config)

    spec = patched.setdefault("spec", {})

    # Head containers
    head_containers = (
        spec.get("headGroupSpec", {})
            .get("template", {})
            .get("spec", {})
            .get("containers") or []
    )
    for c in head_containers:
        _merge_env_into_container(c, env_vars)

    # Worker containers (all groups)
    for group in spec.get("workerGroupSpecs") or []:
        for c in (group.get("template", {})
                       .get("spec", {})
                       .get("containers") or []):
            _merge_env_into_container(c, env_vars)

    try:
        co.replace_namespaced_custom_object(
            group="ray.io", version="v1", namespace=namespace,
            plural="rayclusters", name=RAY_CLUSTER_NAME, body=patched,
            _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        _skip_or_fail(f"Failed to patch RayCluster: {exc.reason}")

    try:
        yield {
            "env": env_vars,
            "ray_cluster": RAY_CLUSTER_NAME,
            "namespace": namespace,
        }
    finally:
        try:
            co.replace_namespaced_custom_object(
                group="ray.io", version="v1", namespace=namespace,
                plural="rayclusters", name=RAY_CLUSTER_NAME, body=original,
                _request_timeout=API_TIMEOUT,
            )
        except ApiException as exc:
            logger.warning(
                "Failed to restore RayCluster %s/%s: %s",
                namespace, RAY_CLUSTER_NAME, exc.reason,
            )


# ---------------------------------------------------------------------------
# Diagnostic fixtures (for failure investigation)
# ---------------------------------------------------------------------------
@pytest.fixture
def cluster_events(k8s_client, namespace) -> Callable[[str], List[Any]]:
    """
    Return a function ``events(kind_or_name)`` that lists recent events in
    the namespace, optionally filtered by involved-object name.

    Useful inside a test's failure path or as a pytest hook.
    """
    def _events(filter_name: Optional[str] = None) -> List[Any]:
        try:
            events = k8s_client["core"].list_namespaced_event(
                namespace=namespace, _request_timeout=API_TIMEOUT,
            ).items
        except ApiException:
            return []
        if filter_name:
            events = [
                e for e in events
                if getattr(getattr(e, "involved_object", None),
                           "name", "") == filter_name
            ]
        return sorted(
            events,
            key=lambda e: getattr(e, "last_timestamp", None) or "",
            reverse=True,
        )
    return _events


@pytest.fixture
def describe_pod(k8s_client, namespace) -> Callable[[str], str]:
    """
    Return a function ``describe(pod_name)`` that produces a compact,
    log-friendly summary of a pod (phase, node, restart count, last state).
    """
    def _describe(pod_name: str) -> str:
        try:
            pod = k8s_client["core"].read_namespaced_pod(
                name=pod_name, namespace=namespace, _request_timeout=API_TIMEOUT,
            )
        except ApiException as exc:
            return f"pod {pod_name}: cannot read ({exc.status} {exc.reason})"
        lines = [
            f"name={pod.metadata.name}",
            f"node={getattr(pod.spec, 'node_name', '?')}",
            f"phase={getattr(pod.status, 'phase', '?')}",
        ]
        for cs in (pod.status.container_statuses or []):
            state = cs.state or object()
            state_kind = next(
                (k for k in ("waiting", "running", "terminated")
                 if getattr(state, k, None)), "unknown",
            )
            reason = ""
            if state_kind == "waiting":
                reason = getattr(state.waiting, "reason", "")
            elif state_kind == "terminated":
                reason = getattr(state.terminated, "reason", "")
            lines.append(
                f"container={cs.name} ready={cs.ready} "
                f"restarts={cs.restart_count} state={state_kind} "
                f"reason={reason or '-'}"
            )
        return " | ".join(lines)
    return _describe


# ---------------------------------------------------------------------------
# Smoke test — validates the module loads and the helpers work standalone.
# Run: python conftest.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    failures = 0

    # 1. Default config round-trip
    cfg = _default_enhancement_config()
    cfg["modp"]["enabled"] = False
    fresh = _default_enhancement_config()
    assert fresh["modp"]["enabled"] is True, (
        "default config is not deep-copied (mutation leaked)"
    )
    print("ok  deep copy of default config")

    # 2. Env-var generation includes every documented field
    env = _build_env_vars(fresh)
    names = {e["name"] for e in env}
    for required in (
        "ENHANCEMENTS_ENABLED", "MODP_WEIGHTS", "HUMAN_FEEDBACK_SCORE",
        "DISTILLATION_TEACHERS", "EVOLUTIONARY_ENABLED",
        "FLEXGEN_MODEL_NAME",
    ):
        if required not in names:
            print(f"FAIL missing env var: {required}")
            failures += 1
    print(f"ok  env-var list has {len(env)} entries")

    # 3. Env merge preserves existing entries
    container = {"env": [{"name": "PRE_EXISTING", "value": "1"}]}
    _merge_env_into_container(container, env)
    merged_names = {e["name"] for e in container["env"]}
    assert "PRE_EXISTING" in merged_names, "merge dropped existing env var"
    assert "MODP_ENABLED" in merged_names, "merge did not add new env var"
    print("ok  env merge preserves pre-existing entries")

    # 4. Pod readiness heuristic
    class _FakeStatus:
        phase = "Running"

    class _FakeCS:
        ready = True

    class _FakePod:
        status = _FakeStatus()
        metadata = None

    _FakePod.status.container_statuses = [_FakeCS()]
    assert _is_pod_ready(_FakePod()), "Ready pod detected as not ready"

    class _CrashPod(_FakePod):
        class status:
            phase = "Running"
            container_statuses = [type("CS", (), {"ready": False})()]

    assert not _is_pod_ready(_CrashPod()), (
        "Running-but-not-Ready pod detected as ready (v1 behaviour)"
    )
    print("ok  pod readiness heuristic")

    # 5. Diagnostics render without crashing on a fake list
    print("ok  pod diagnostics:", _pod_diagnostics([_FakePod()]))

    # 6. yaml.safe_dump keeps stable key order
    dumped = yaml.safe_dump({"enhancements": fresh}, sort_keys=False)
    assert dumped.index("limit_graph") < dumped.index("modp"), (
        "safe_dump reordered keys (sort_keys=False must preserve order)"
    )
    print("ok  yaml.safe_dump preserves key order")

    print()
    if failures:
        print(f"{failures} failure(s)")
        raise SystemExit(1)
    print("all smoke tests passed")
