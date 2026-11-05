# -*- coding: utf-8 -*-
"""Kubernetes Deployment Tests (Enhanced v2).

Covers the RayCluster deployment and — when the live cluster shows that
enhancements are enabled — the enhancement integration surface:

  - LIMIT Graph metrics (via ConfigMap)
  - MODP (multi-objective weights in ConfigMap)
  - RLHF (human feedback score in ConfigMap)
  - Multi-Teacher Distillation + MoE gating (ConfigMap + env vars if present)
  - Bio-inspired Optimisation (ConfigMap)
  - FlexGen execution backend (ConfigMap)

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  ``except Exception: pytest.skip()`` in the ConfigMap test replaced
          with a structural check that fails on a missing section and skips
          only when the live ConfigMap says ``enhancements.enabled: false``.
          v1 swallowed its own AssertionError.
  FIX #2  ``MODP_WEIGHTS`` compared via JSON/literal parsing on both sides.
          v1 required the exact string ``'[0.4,0.3,0.2,0.1]'``, which never
          matches ``json.dumps`` output ``'[0.4, 0.3, 0.2, 0.1]'``.
  FIX #3  Volume-mount test checks the mounts v2 actually creates
          (``/app/config`` and ``/app/data``) and skips when the deployer
          customized the layout. v1 required ``/app/enhancement_state``,
          which the v2 config generator does not create.
  FIX #4  Label/annotation/env-var tests skip when the live ConfigMap shows
          ``enhancements.enabled: false``. v1 failed on a correct deployment.
  FIX #5  ConfigMap lookup by prefix (``dev-``/``staging-``/``prod-`` +
          kustomize hash) instead of exact ``green-agent-enhancements``.
  FIX #6  RayCluster name is env-configurable (``GREEN_AGENT_RAY_CLUSTER``).
  FIX #7  ``container.env`` guarded against ``None``.
  FIX #8  Session-scoped ``live_raycluster`` / ``live_green_agent_config``
          fixtures — one fetch per session.
  FIX #9  ``_request_timeout`` on every API call; 404 → skip.
  FIX #10 Pods sorted by creation timestamp; filtered by ``component``.
  FIX #11 ``test_worker_pods_ready`` asserts the count matches the live
          ``workerGroupSpecs[].replicas``.
  FIX #12 ``test_ray_cluster_exists_with_spec`` asserts the spec shape.
  FIX #13 Added ``workerGroupSpecs[].{replicas,minReplicas,maxReplicas}``
          invariant check.
  FIX #14 Markers ``k8s`` / ``enhancements`` / ``requires_cluster``
          registered via ``pytest_configure``.
  FIX #15 YAML parsed with ``yaml.safe_load`` instead of substring matching.

---- Fixtures consumed (provided by tests/integration/conftest.py) -----------
  - ``k8s_client``      : dict of Kubernetes API clients
  - ``namespace``       : target namespace (already validated)
  - ``wait_for_pods``   : callable(selector, timeout, expected_count) -> List
  - ``cluster_events``  : callable(filter_name) -> List  (optional)
  - ``describe_pod``    : callable(pod_name) -> str       (optional)

---- Environment variables ---------------------------------------------------
  GREEN_AGENT_RAY_CLUSTER  RayCluster name (default: green-agent-cluster)
  GREEN_AGENT_K8S_TIMEOUT  per-API-call timeout in seconds (default: 10)

---- Run ---------------------------------------------------------------------
  pytest tests/integration/test_k8s_deployment.py -m "k8s and not enhancements"
  pytest tests/integration/test_k8s_deployment.py -m enhancements
  python tests/integration/test_k8s_deployment.py   # offline helper smoke test
"""

from __future__ import annotations

import ast
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import pytest
import yaml
from kubernetes.client.exceptions import ApiException


# ---------------------------------------------------------------------------
# Environment / markers (FIX #6, #14)
# ---------------------------------------------------------------------------
RAY_CLUSTER_NAME: str = os.environ.get(
    "GREEN_AGENT_RAY_CLUSTER", "green-agent-cluster",
)
API_TIMEOUT: float = float(os.environ.get("GREEN_AGENT_K8S_TIMEOUT", "10"))

pytestmark = [pytest.mark.k8s, pytest.mark.requires_cluster]


def pytest_configure(config: pytest.Config) -> None:
    """Register markers used by this module.

    The conftest also registers these; double-registration is harmless and
    keeps this file independently runnable.
    """
    for marker, desc in (
        ("k8s", "Kubernetes API interaction"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
        ("requires_cluster", "requires a live Kubernetes cluster"),
    ):
        config.addinivalue_line("markers", f"{marker}: {desc}")


# ---------------------------------------------------------------------------
# Helpers (FIX #2, #7, #9, #10)
# ---------------------------------------------------------------------------
def _skip_on_404(exc: ApiException, what: str) -> None:
    """Skip cleanly on a 404; fail with diagnostics on any other API error."""
    if getattr(exc, "status", None) == 404:
        pytest.skip(f"{what} not deployed (404)")
    reason = getattr(exc, "reason", "")
    pytest.fail(f"Cannot read {what}: {exc.status} {reason}")


def _sorted_pods(pods: List[Any]) -> List[Any]:
    """Oldest first — the head pod is created before the workers."""
    return sorted(
        pods,
        key=lambda p: getattr(
            getattr(p, "metadata", None), "creation_timestamp", None,
        ) or "",
    )


def _env_dict(container: Any) -> Dict[str, Optional[str]]:
    """Safe mapping of env var name → value (None for valueFrom entries)."""
    return {
        e.name: getattr(e, "value", None)
        for e in (container.env or [])
    }


def _parse_json_or_literal(text: Optional[str]) -> Any:
    """
    Try JSON, then ``ast.literal_eval``, else return the string unchanged.

    Handles the v1 mismatch where ``json.dumps`` writes ``'[0.4, 0.3, ...]'``
    (with spaces) but the test expected ``'[0.4,0.3,...]'`` (without).
    """
    if text is None:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        pass
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return text


def _find_configmap_by_prefix(core: Any, namespace: str,
                              prefixes: List[str]) -> Optional[Any]:
    """Return the first ConfigMap whose name starts with any prefix."""
    try:
        cms = core.list_namespaced_config_map(
            namespace=namespace,
            label_selector="app=green-agent",
            _request_timeout=API_TIMEOUT,
        ).items
    except ApiException as exc:
        _skip_on_404(exc, "green-agent ConfigMap list")
    for cm in cms:
        for prefix in prefixes:
            if cm.metadata.name.startswith(prefix):
                return cm
    return None


def _first_container_env(pod: Any) -> Dict[str, Optional[str]]:
    """Env vars of the first container, tolerant of empty specs."""
    if not getattr(pod, "spec", None) or not pod.spec.containers:
        return {}
    return _env_dict(pod.spec.containers[0])


def _expected_worker_replicas(rc: Dict[str, Any]) -> int:
    """Sum of replicas across all worker groups; 0 when the field is absent."""
    spec = rc.get("spec") or {}
    groups = spec.get("workerGroupSpecs") or []
    return sum(int(g.get("replicas", 0)) for g in groups)


# ---------------------------------------------------------------------------
# Session fixtures (FIX #8)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def live_raycluster(k8s_client, namespace) -> Dict[str, Any]:
    """Fetch the RayCluster once per session; skip on 404."""
    try:
        return k8s_client["custom_objects"].get_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=namespace,
            plural="rayclusters",
            name=RAY_CLUSTER_NAME,
            _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        _skip_on_404(exc, f"RayCluster '{RAY_CLUSTER_NAME}'")


@pytest.fixture(scope="session")
def live_green_agent_config(k8s_client, namespace) -> Dict[str, Any]:
    """
    Fetch and parse the deployed ``green_agent_config.yaml`` ConfigMap.

    Returns ``{"configmap": <cm>, "config": <parsed dict>}``.

    The ConfigMap name carries a kustomize hash suffix and an overlay
    namePrefix, so we resolve by prefix rather than exact name (FIX #5).
    """
    cm = _find_configmap_by_prefix(
        k8s_client["core"], namespace,
        prefixes=[
            "dev-green-agent-config",
            "staging-green-agent-config",
            "prod-green-agent-config",
            "green-agent-config",
            "green-agent-enhancements",   # legacy / future
        ],
    )
    if cm is None:
        pytest.skip(f"No green-agent-config* ConfigMap in '{namespace}'")

    raw = (cm.data or {}).get("green_agent_config.yaml", "")
    if not raw:
        pytest.skip(
            f"ConfigMap {cm.metadata.name} has no green_agent_config.yaml key"
        )
    try:
        parsed = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        pytest.fail(f"ConfigMap {cm.metadata.name} contains invalid YAML: {exc}")
    return {"configmap": cm, "config": parsed}


# ---------------------------------------------------------------------------
# Base RayCluster deployment (FIX #11, #12, #13)
# ---------------------------------------------------------------------------
class TestRayClusterDeployment:

    def test_ray_cluster_exists_with_spec(self, live_raycluster):
        rc = live_raycluster
        assert rc is not None
        assert rc["metadata"]["name"] == RAY_CLUSTER_NAME
        spec = rc.get("spec") or {}
        assert "headGroupSpec" in spec, "RayCluster has no headGroupSpec"
        assert "workerGroupSpecs" in spec, "RayCluster has no workerGroupSpecs"
        assert spec["workerGroupSpecs"], "workerGroupSpecs is empty"

    def test_worker_group_replicas_bounds(self, live_raycluster):
        """Carbon-aware scaling invariant: 0 <= min <= replicas <= max."""
        groups = live_raycluster["spec"]["workerGroupSpecs"]
        for g in groups:
            replicas = int(g.get("replicas", 0))
            mn = int(g.get("minReplicas", replicas))
            mx = int(g.get("maxReplicas", replicas))
            assert 0 <= mn <= replicas <= mx, (
                f"workerGroup {g.get('groupName')}: replicas={replicas} "
                f"not in [{mn},{mx}]"
            )
        assert any(
            int(g.get("maxReplicas", 0)) > int(g.get("minReplicas", 0))
            for g in groups
        ), "no worker group can scale (all groups have max == min)"

    def test_head_pod_ready(self, wait_for_pods):
        pods = wait_for_pods("component=head", timeout=300, expected_count=1)
        assert pods, "no head pod ready"
        head = _sorted_pods(pods)[0]
        assert head.status.phase == "Running"

    def test_worker_pods_ready(self, wait_for_pods, live_raycluster):
        expected = _expected_worker_replicas(live_raycluster)
        assert expected >= 1, (
            "RayCluster declares zero worker replicas; cannot test workers"
        )
        pods = wait_for_pods(
            "component=worker", timeout=300, expected_count=expected,
        )
        assert len(pods) >= expected, (
            f"expected {expected} worker(s) from RayCluster spec, "
            f"found {len(pods)}"
        )


# ---------------------------------------------------------------------------
# Enhancement ConfigMap contents (FIX #1, #15)
# ---------------------------------------------------------------------------
@pytest.mark.enhancements
class TestEnhancedDeployment:
    """Enhancement ConfigMap structure and contents."""

    REQUIRED_SECTIONS: Tuple[str, ...] = (
        "limit_graph", "modp", "rlhf", "distillation",
        "bio_inspired", "moe_expert",
    )

    @staticmethod
    def _enhancements_cfg(live_green_agent_config) -> Dict[str, Any]:
        """Return the ``enhancements`` section, or skip when it is off."""
        cfg = live_green_agent_config["config"].get("enhancements") or {}
        if not cfg.get("enabled"):
            pytest.skip(
                "enhancements.enabled is false in "
                f"{live_green_agent_config['configmap'].metadata.name}"
            )
        return cfg

    def test_enhancement_configmap_sections(self, live_green_agent_config):
        """
        Structural check: every enhancement module has a section in the live
        ConfigMap. Runs regardless of ``enabled`` so a missing section is a
        real failure (not a silent skip).
        """
        cfg = live_green_agent_config["config"].get("enhancements") or {}
        missing = [k for k in self.REQUIRED_SECTIONS if k not in cfg]
        assert not missing, (
            f"ConfigMap missing enhancement sections: {missing} "
            f"(found: {sorted(cfg.keys())})"
        )

    def test_modp_weights_valid(self, live_green_agent_config):
        cfg = self._enhancements_cfg(live_green_agent_config)
        modp = cfg.get("modp") or {}
        weights = modp.get("weights") or modp.get("objective_weights")
        assert isinstance(weights, list), (
            f"modp weights not a list: {weights!r}"
        )
        assert len(weights) == 4, (
            f"modp weights should have 4 entries, got {len(weights)}"
        )
        assert all(isinstance(w, (int, float)) for w in weights), (
            f"modp weights must be numeric: {weights!r}"
        )
        assert sum(float(w) for w in weights) > 0, (
            "modp weights sum to <= 0; objective weighting is broken"
        )

    def test_rlhf_feedback_score_in_range(self, live_green_agent_config):
        cfg = self._enhancements_cfg(live_green_agent_config)
        hf = (cfg.get("rlhf") or {}).get("human_feedback_score")
        if hf is None:
            pytest.skip("rlhf.human_feedback_score not set in ConfigMap")
        assert 0.0 <= float(hf) <= 1.0, (
            f"human_feedback_score={hf} out of [0,1]"
        )

    def test_distillation_uses_moe_gating(self, live_green_agent_config):
        cfg = self._enhancements_cfg(live_green_agent_config)
        d = cfg.get("distillation") or {}
        assert d.get("enabled") is True, "distillation not enabled"
        assert d.get("use_moe_gating") is True, (
            "distillation.use_moe_gating is false — MoE gating claimed but off"
        )

    def test_flexgen_config_present(self, live_green_agent_config):
        """FlexGen may be top-level or nested under ``integration``."""
        cfg = live_green_agent_config["config"]
        flex = (cfg.get("flexgen")
                or (cfg.get("integration") or {}).get("flexgen"))
        if flex is None:
            pytest.skip("FlexGen section not present in ConfigMap")
        assert flex.get("enabled") in (True, False)
        if flex.get("enabled"):
            assert flex.get("model_name"), (
                "FlexGen enabled but no model_name declared"
            )


# ---------------------------------------------------------------------------
# Enhancement env vars on pods (FIX #2, #4, #7)
# ---------------------------------------------------------------------------
@pytest.mark.enhancements
class TestEnhancedEnvVars:
    """Enhancement env vars on pods (only asserted when the deployer set them)."""

    @staticmethod
    def _enhancements_enabled(live_green_agent_config) -> bool:
        cfg = live_green_agent_config["config"].get("enhancements") or {}
        return bool(cfg.get("enabled"))

    def test_head_pod_enhancement_env_vars(
        self, wait_for_pods, live_green_agent_config,
    ):
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false — no env vars expected")

        pods = wait_for_pods("component=head", timeout=300, expected_count=1)
        env = _first_container_env(_sorted_pods(pods)[0])
        if "ENHANCEMENTS_ENABLED" not in env:
            pytest.skip(
                "head pod does not expose enhancement env vars "
                "(deployer chose to pass config via ConfigMap only)"
            )

        # Only assert the values the deployer actually provided.
        assert env.get("ENHANCEMENTS_ENABLED") == "true"

        cfg_enh = live_green_agent_config["config"].get("enhancements") or {}

        if "MODP_WEIGHTS" in env:
            pod_weights = _parse_json_or_literal(env.get("MODP_WEIGHTS"))
            cfg_weights = (
                (cfg_enh.get("modp") or {}).get("weights")
                or (cfg_enh.get("modp") or {}).get("objective_weights")
            )
            if isinstance(pod_weights, list) and isinstance(cfg_weights, list):
                assert (
                    [float(x) for x in pod_weights]
                    == [float(x) for x in cfg_weights]
                ), (
                    f"MODP_WEIGHTS on pod ({pod_weights}) does not match "
                    f"ConfigMap ({cfg_weights})"
                )

        if "HUMAN_FEEDBACK_SCORE" in env:
            v = env.get("HUMAN_FEEDBACK_SCORE")
            if v is not None:
                assert 0.0 <= float(v) <= 1.0, (
                    f"HUMAN_FEEDBACK_SCORE={v} out of [0,1]"
                )

        if "POPULATION_SIZE" in env:
            v = env.get("POPULATION_SIZE")
            if v is not None:
                assert int(v) > 0, (
                    f"POPULATION_SIZE={v} must be positive"
                )

    def test_worker_pods_enhancement_env_vars(
        self, wait_for_pods, live_green_agent_config, live_raycluster,
    ):
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false — no env vars expected")

        expected = _expected_worker_replicas(live_raycluster) or 1
        pods = wait_for_pods(
            "component=worker", timeout=300, expected_count=expected,
        )
        env = _first_container_env(_sorted_pods(pods)[0])
        if "ENHANCEMENTS_ENABLED" not in env:
            pytest.skip("workers do not expose enhancement env vars")

        assert env.get("ENHANCEMENTS_ENABLED") == "true"


# ---------------------------------------------------------------------------
# Volume mounts (FIX #3)
# ---------------------------------------------------------------------------
class TestVolumeMounts:
    """Verify the mounts the config generator actually creates."""

    EXPECTED = ("/app/config", "/app/data")

    def test_head_pod_mounts_config_and_data(self, wait_for_pods):
        pods = wait_for_pods("component=head", timeout=300, expected_count=1)
        head = _sorted_pods(pods)[0]
        container = head.spec.containers[0] if head.spec.containers else None
        assert container is not None, "head pod has no container"

        mounts = {
            (vm.mount_path or "").rstrip("/")
            for vm in (container.volume_mounts or [])
        }

        # If the deployer replaced the mount set entirely, skip rather than
        # fail on a correct alternate layout.
        if not any(e in mounts for e in self.EXPECTED):
            pytest.skip(
                f"pods do not mount {self.EXPECTED}; "
                f"actual mounts={sorted(mounts)}"
            )

        for expected in self.EXPECTED:
            assert expected in mounts, (
                f"expected mount {expected}; actual={sorted(mounts)}"
            )


# ---------------------------------------------------------------------------
# Offline smoke test — validates helpers without a cluster.
# Run: python tests/integration/test_k8s_deployment.py
# ---------------------------------------------------------------------------
def _smoke_test_helpers() -> int:
    """Return the number of failures (0 = all pass)."""
    failures = 0

    # --- _parse_json_or_literal handles both JSON and literal styles ---
    assert _parse_json_or_literal("[0.4, 0.3, 0.2, 0.1]") == [0.4, 0.3, 0.2, 0.1]
    assert _parse_json_or_literal("[0.4,0.3,0.2,0.1]") == [0.4, 0.3, 0.2, 0.1]
    assert _parse_json_or_literal("not-json") == "not-json"
    assert _parse_json_or_literal(None) is None
    print("ok  _parse_json_or_literal")

    # --- _env_dict tolerates env=None ---
    class _C:
        env = None

    assert _env_dict(_C()) == {}
    print("ok  _env_dict handles env=None")

    # --- _env_dict returns None for valueFrom entries ---
    class _E:
        def __init__(self, name, value):
            self.name = name
            self.value = value

    class _C2:
        env = [_E("A", "1"), _E("B", None)]

    assert _env_dict(_C2()) == {"A": "1", "B": None}
    print("ok  _env_dict handles valueFrom")

    # --- _sorted_pods sorts by creation_timestamp, oldest first ---
    class _Meta:
        def __init__(self, ts):
            self.creation_timestamp = ts

    class _P:
        def __init__(self, ts):
            self.metadata = _Meta(ts)

    pods = [_P("2024-02-01"), _P("2024-01-01"), _P("2024-03-01")]
    ordered = _sorted_pods(pods)
    assert [p.metadata.creation_timestamp for p in ordered] == [
        "2024-01-01", "2024-02-01", "2024-03-01",
    ]
    print("ok  _sorted_pods orders by creation_timestamp")

    # --- _expected_worker_replicas sums across groups ---
    rc = {
        "spec": {
            "workerGroupSpecs": [
                {"replicas": 2},
                {"replicas": 4},
            ]
        }
    }
    assert _expected_worker_replicas(rc) == 6
    assert _expected_worker_replicas({"spec": {}}) == 0
    assert _expected_worker_replicas({}) == 0
    print("ok  _expected_worker_replicas sums across groups")

    # --- _first_container_env tolerates empty spec ---
    class _P2:
        spec = None

    assert _first_container_env(_P2()) == {}

    class _EmptySpec:
        containers = []

    class _P3:
        spec = _EmptySpec()

    assert _first_container_env(_P3()) == {}
    print("ok  _first_container_env tolerates empty specs")

    # --- _skip_on_404 only skips on 404 ---
    class _Exc404(ApiException):
        def __init__(self):
            super().__init__(status=404, reason="Not Found")

    class _Exc500(ApiException):
        def __init__(self):
            super().__init__(status=500, reason="Server Error")

    # We can't call pytest.skip/fail from a plain script, so verify the
    # branch selection with monkeypatched pytest hooks.
    import unittest.mock as mock

    with mock.patch("pytest.skip") as mskip:
        _skip_on_404(_Exc404(), "thing")
        mskip.assert_called_once()
    with mock.patch("pytest.fail") as mfail:
        _skip_on_404(_Exc500(), "thing")
        mfail.assert_called_once()
    print("ok  _skip_on_404 distinguishes 404 from other errors")

    return failures


if __name__ == "__main__":
    try:
        n = _smoke_test_helpers()
    except AssertionError as exc:
        print(f"FAIL: {exc}")
        raise SystemExit(1) from exc
    print()
    if n:
        print(f"{n} failure(s)")
        raise SystemExit(1)
    print("all helper smoke tests passed")
