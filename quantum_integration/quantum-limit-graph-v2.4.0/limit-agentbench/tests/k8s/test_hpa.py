# -*- coding: utf-8 -*-
"""
Horizontal Pod Autoscaler + Carbon-Aware Scaling Tests (Enhanced v2).

The v2 config generator does **not** deploy an HPA — ``RayCluster`` has no
``scale`` subresource, so HPA-on-RayCluster fails permanently with
``the server could not find the requested resource``. Horizontal scaling in
v2 is expressed via ``workerGroupSpecs[].{minReplicas, maxReplicas, replicas}``
on the RayCluster itself.

This file therefore tests the mechanism v2 actually uses, and treats the
legacy HPA as an *optional* deployment surface:

  1. ``TestRayClusterScaling``     — the v2 scaling mechanism (always runs).
  2. ``TestHPALegacy``             — skips cleanly when no HPA exists.
  3. ``TestHPALegacyEnhancements`` — skips when enhancements are off.

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  ``TestRayClusterScaling`` added — tests the mechanism v2 actually
          uses. The old file only tested an HPA that v2 does not create, so
          all ten tests failed with 404 on a correct v2 deployment.
  FIX #2  Every HPA test depends on the session-scoped ``live_hpa`` fixture,
          which calls ``pytest.skip`` on 404 and ``pytest.fail`` on any other
          API error.
  FIX #3  ``test_hpa_evolutionary_metric_configured`` fails if the metric is
          declared in a Pods metric but cannot be read back; it skips only
          when the metric is not referenced at all. v1's loop body never
          ran when the metric was absent, so a completely broken HPA
          passed.
  FIX #4  ``test_hpa_metrics_configured`` dead ``or`` branch removed; the
          test now asserts what it claims (a usable metric type is present).
  FIX #5  ``time.sleep(60)`` and ``assert hpa.status is not None``
          (trivially true) removed; ``test_hpa_status_is_populated`` checks
          ``status.current_replicas`` when it is populated.
  FIX #6  ``test_hpa_bounds_are_sane`` asserts ``min <= replicas <= max``
          and ``max > min`` instead of hardcoded 2/4/20 values.
  FIX #7  ``test_hpa_targets_a_scalable_resource`` *fails* when the HPA
          targets a RayCluster, surfacing the permanently-broken
          configuration instead of asserting it.
  FIX #8  ``test_hpa_scaling_behavior`` guards ``scale_down`` / ``scale_up``
          being ``None`` (v1 crashed with AttributeError on a valid HPA).
  FIX #9  Undefined ``deploy_green_agent`` and ``test_namespace`` fixtures
          removed. Uses ``namespace`` from the shared conftest.
  FIX #10 ``_request_timeout`` on every API call; ``ApiException(404)``
          routes to ``_skip_or_fail`` in every fetch fixture.
  FIX #11 Markers ``hpa`` / ``scaling`` / ``enhancements`` / ``slow`` /
          ``requires_cluster`` registered via ``pytest_configure``.
  FIX #12 Hardcoded HPA / RayCluster names made env-configurable
          (``GREEN_AGENT_HPA_NAME``, ``GREEN_AGENT_RAY_CLUSTER``).
  FIX #13 Enhancement tests gated on ``enhancements.enabled`` from the live
          ConfigMap.
  FIX #14 ``test_hpa_enhanced_metrics_exist_in_prometheus`` checks the
          metrics are *exported* on ``/metrics`` (the correct boundary);
          it does not pretend the HPA consumes them.
  FIX #15 Added ``test_worker_pods_match_spec_replicas`` — a behavioural
          check that actual pod count matches ``workerGroupSpecs[].replicas``.
  FIX #16 ``__main__`` smoke test validates helpers offline — no cluster
          required.

---- Fixtures consumed (from tests/integration/conftest.py) ------------------
  - ``k8s_client``      : dict of Kubernetes API clients
  - ``namespace``       : target namespace (already validated)
  - ``wait_for_pods``   : callable(selector, timeout, expected_count) -> List

---- Environment variables ---------------------------------------------------
  GREEN_AGENT_HPA_NAME         HPA name (default: green-agent-hpa)
  GREEN_AGENT_RAY_CLUSTER      RayCluster name (default: green-agent-cluster)
  GREEN_AGENT_URL              dashboard URL (default: http://localhost:8000)
  GREEN_AGENT_K8S_TIMEOUT      per-API-call timeout in seconds (default: 10)
  STRICT_INTEGRATION           "1" fails instead of skipping on missing infra

---- Run ---------------------------------------------------------------------
  pytest tests/integration/test_hpa.py -m "scaling and not slow"
  pytest tests/integration/test_hpa.py -m hpa            # legacy HPA only
  pytest tests/integration/test_hpa.py -m enhancements   # enhancement surface
  GREEN_AGENT_URL=http://green.example.com pytest -m enhancements
  python tests/integration/test_hpa.py                   # offline helper checks
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest
from kubernetes.client.exceptions import ApiException

try:
    from prometheus_client.parser import text_string_to_metric_families
    _PROM = True
except ImportError:                                     # pragma: no cover
    _PROM = False
    text_string_to_metric_families = None


# ---------------------------------------------------------------------------
# Environment / markers
# ---------------------------------------------------------------------------
HPA_NAME: str = os.environ.get("GREEN_AGENT_HPA_NAME", "green-agent-hpa")
RAY_CLUSTER_NAME: str = os.environ.get(
    "GREEN_AGENT_RAY_CLUSTER", "green-agent-cluster",
)
API_TIMEOUT: float = float(os.environ.get("GREEN_AGENT_K8S_TIMEOUT", "10"))
STRICT_INTEGRATION: bool = os.environ.get("STRICT_INTEGRATION", "0") == "1"
DEFAULT_URL: str = os.environ.get("GREEN_AGENT_URL", "http://localhost:8000")

pytestmark = [pytest.mark.scaling, pytest.mark.requires_cluster]


def pytest_configure(config: pytest.Config) -> None:
    """Register markers used by this module.

    The shared conftest also registers ``requires_cluster`` and
    ``enhancements``; double registration is harmless and keeps this file
    independently runnable.
    """
    for marker, desc in (
        ("hpa", "legacy Horizontal Pod Autoscaler tests"),
        ("scaling", "carbon-aware scaling tests (RayCluster, HPA)"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
        ("slow", "long-running test"),
        ("requires_cluster", "requires a live Kubernetes cluster"),
    ):
        config.addinivalue_line("markers", f"{marker}: {desc}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _skip_or_fail(msg: str) -> None:
    """Skip normally; fail under STRICT_INTEGRATION=1 (CI gating)."""
    if STRICT_INTEGRATION:
        pytest.fail(msg)
    pytest.skip(msg)


def _parse_metric_names_from_hpa(hpa: Any) -> Set[str]:
    """
    Extract metric names from a V2 HPA spec, tolerant of malformed entries.

    v1 crashed or silently produced wrong sets when ``metric.pods`` was
    ``None`` — a common shape for partially-configured HPAs.
    """
    names: Set[str] = set()
    for metric in (getattr(getattr(hpa, "spec", None), "metrics", None) or []):
        mtype = getattr(metric, "type", None)
        name: Optional[str] = None
        try:
            if mtype == "Pods":
                name = metric.pods.metric.name
            elif mtype == "External":
                name = metric.external.metric.name
            elif mtype == "Object":
                name = metric.object.metric.name
            elif mtype == "Resource":
                name = metric.resource.name
        except AttributeError:
            name = None
        if name:
            names.add(name)
    return names


def _replicas_bounds(g: Dict[str, Any]) -> Tuple[int, int, int]:
    """Return (min, replicas, max) with the fields defaulted safely."""
    replicas = int(g.get("replicas", 0))
    mn = int(g.get("minReplicas", replicas))
    mx = int(g.get("maxReplicas", replicas))
    return mn, replicas, mx


# ---------------------------------------------------------------------------
# Session fixtures — one fetch per session
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
        if exc.status == 404:
            _skip_or_fail(
                f"RayCluster '{RAY_CLUSTER_NAME}' not found in '{namespace}'"
            )
        pytest.fail(f"Cannot read RayCluster: {exc.status} {exc.reason}")


@pytest.fixture(scope="session")
def live_hpa(k8s_client, namespace) -> Any:
    """
    Fetch the HPA once per session.

    Skips on 404 (v2 does not deploy an HPA). Fails on any other API error so
    RBAC / connectivity problems surface as real failures, not skips.
    """
    try:
        return k8s_client["autoscaling"].read_namespaced_horizontal_pod_autoscaler(
            name=HPA_NAME,
            namespace=namespace,
            _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        if exc.status == 404:
            _skip_or_fail(
                f"HPA '{HPA_NAME}' not deployed in '{namespace}'. "
                "v2 uses workerGroupSpecs on the RayCluster instead — see "
                "TestRayClusterScaling."
            )
        pytest.fail(f"Cannot read HPA: {exc.status} {exc.reason}")


@pytest.fixture(scope="session")
def live_green_agent_config(k8s_client, namespace) -> Dict[str, Any]:
    """
    Fetch and parse the deployed ``green_agent_config.yaml`` ConfigMap.

    Returns ``{"configmap": <cm>, "config": <parsed dict>}``. Skips when no
    ``green-agent-config*`` ConfigMap is present in the namespace.
    """
    import yaml

    try:
        cms = k8s_client["core"].list_namespaced_config_map(
            namespace=namespace,
            label_selector="app=green-agent",
            _request_timeout=API_TIMEOUT,
        ).items
    except ApiException as exc:
        if exc.status == 404:
            _skip_or_fail(f"No ConfigMaps visible in '{namespace}'")
        pytest.fail(f"Cannot list ConfigMaps: {exc.status} {exc.reason}")

    prefixes = (
        "dev-green-agent-config",
        "staging-green-agent-config",
        "prod-green-agent-config",
        "green-agent-config",
    )
    cm = None
    for candidate in cms:
        if any(candidate.metadata.name.startswith(p) for p in prefixes):
            cm = candidate
            break
    if cm is None:
        _skip_or_fail(f"No green-agent-config* ConfigMap in '{namespace}'")

    raw = (cm.data or {}).get("green_agent_config.yaml", "")
    if not raw:
        _skip_or_fail(
            f"ConfigMap {cm.metadata.name} has no green_agent_config.yaml key"
        )
    try:
        parsed = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        pytest.fail(
            f"ConfigMap {cm.metadata.name} contains invalid YAML: {exc}"
        )
    return {"configmap": cm, "config": parsed}


@pytest.fixture(scope="session")
def prometheus_metrics() -> Dict[str, Any]:
    """Parse ``/metrics`` once per session; skip when unreachable."""
    if not _PROM:
        _skip_or_fail(
            "prometheus_client not installed — cannot parse /metrics rigorously"
        )
    import requests
    url = DEFAULT_URL.rstrip("/")
    try:
        r = requests.get(f"{url}/metrics", timeout=(3, 10))
    except requests.exceptions.RequestException as exc:
        _skip_or_fail(
            f"/metrics unreachable at {url} ({exc}). "
            "Open a port-forward or set GREEN_AGENT_URL."
        )
    if r.status_code != 200:
        _skip_or_fail(f"/metrics returned {r.status_code}")
    families = list(text_string_to_metric_families(r.text))
    samples = [s for fam in families for s in fam.samples]
    return {
        "families": {fam.name: fam for fam in families},
        "family_names": {fam.name for fam in families},
        "sample_names": {s.name for s in samples},
        "samples": samples,
        "raw": r.text,
    }


# ---------------------------------------------------------------------------
# The actual v2 scaling mechanism
# ---------------------------------------------------------------------------
class TestRayClusterScaling:
    """
    Carbon-aware scaling in v2 is expressed via
    ``RayCluster.spec.workerGroupSpecs[].{minReplicas, maxReplicas, replicas}``.
    The Ray autoscaler respects these bounds; an HPA cannot scale a
    RayCluster (no scale subresource).
    """

    def test_raycluster_has_worker_groups(self, live_raycluster):
        spec = live_raycluster.get("spec") or {}
        groups = spec.get("workerGroupSpecs") or []
        assert groups, "RayCluster has no workerGroupSpecs — no scaling"

    def test_worker_group_bounds(self, live_raycluster):
        """min <= replicas <= max must hold for every group."""
        groups = live_raycluster["spec"]["workerGroupSpecs"]
        for g in groups:
            mn, replicas, mx = _replicas_bounds(g)
            assert 0 <= mn <= replicas <= mx, (
                f"workerGroup {g.get('groupName')}: replicas={replicas} "
                f"not in [{mn},{mx}]"
            )

    def test_at_least_one_group_can_scale(self, live_raycluster):
        """A group with max == min cannot scale — carbon-aware behaviour breaks."""
        groups = live_raycluster["spec"]["workerGroupSpecs"]
        scalable = [
            g.get("groupName") for g in groups
            if int(g.get("maxReplicas", 0)) > int(g.get("minReplicas", 0))
        ]
        assert scalable, (
            "no workerGroup can scale (all groups have max == min); "
            "carbon-aware scaling cannot operate"
        )

    def test_worker_pods_match_spec_replicas(self, live_raycluster, wait_for_pods):
        """The number of Ready workers should match the declared replicas."""
        expected = sum(
            int(g.get("replicas", 0))
            for g in live_raycluster["spec"]["workerGroupSpecs"]
        )
        assert expected >= 1, "RayCluster declares zero worker replicas"
        pods = wait_for_pods(
            "component=worker", timeout=300, expected_count=expected,
        )
        assert len(pods) >= expected, (
            f"expected {expected} worker(s) from RayCluster spec, "
            f"found {len(pods)}"
        )

    def test_min_replicas_within_max(self, live_raycluster):
        """Every group's declared minimum must not exceed its maximum."""
        for g in live_raycluster["spec"]["workerGroupSpecs"]:
            mn, _, mx = _replicas_bounds(g)
            assert mn <= mx, (
                f"workerGroup {g.get('groupName')}: minReplicas={mn} > "
                f"maxReplicas={mx}"
            )


# ---------------------------------------------------------------------------
# Legacy HPA tests (skip when absent — v2 has none)
# ---------------------------------------------------------------------------
@pytest.mark.hpa
class TestHPALegacy:
    """Legacy HPA tests — every test depends on ``live_hpa`` (skip on 404)."""

    def test_hpa_bounds_are_sane(self, live_hpa):
        """min <= replicas <= max; max must be strictly greater than min."""
        mn = int(live_hpa.spec.min_replicas)
        mx = int(live_hpa.spec.max_replicas)
        current = int(live_hpa.spec.replicas or mn)
        assert 0 <= mn <= current <= mx, (
            f"replicas={current} not in [{mn},{mx}]"
        )
        assert mx > mn, (
            f"max_replicas ({mx}) == min_replicas ({mn}); "
            "no scaling possible"
        )

    def test_hpa_targets_a_scalable_resource(self, live_hpa):
        """
        The HPA target must support the ``scale`` subresource.

        A RayCluster does not — the HPA would fail permanently. Fail (not
        skip) so the misconfiguration surfaces instead of the test asserting
        it as if it were correct.
        """
        target = live_hpa.spec.scale_target_ref
        assert target.kind != "RayCluster", (
            f"HPA '{HPA_NAME}' targets a RayCluster, which has no scale "
            "subresource. The HPA will fail permanently. Use "
            "workerGroupSpecs on the RayCluster instead."
        )

    def test_hpa_metrics_configured(self, live_hpa):
        """
        HPA must have at least one metric of a *usable* type.

        Custom metrics are exposed via Pods / External / Object; built-in
        resource metrics via Resource. Any of the four is acceptable; what is
        not acceptable is zero metrics or an unknown type alone.
        """
        metrics = live_hpa.spec.metrics or []
        assert metrics, "HPA has no metrics configured"
        types = {getattr(m, "type", None) for m in metrics}
        assert types & {"Resource", "Pods", "External", "Object"}, (
            f"HPA has no usable metric type; types={sorted(t for t in types if t)}"
        )

    def test_hpa_scaling_behavior(self, live_hpa):
        """
        If a behavior block is configured, its scale_down / scale_up windows
        must be sane. Both sub-blocks are optional, so absence is not a
        failure — it just means the HPA uses controller defaults.
        """
        behavior = getattr(live_hpa.spec, "behavior", None)
        if behavior is None:
            pytest.skip("HPA has no behavior block configured")

        sd = getattr(behavior, "scale_down", None)
        if sd is not None:
            window = getattr(sd, "stabilization_window_seconds", None)
            if window is not None:
                assert int(window) >= 60, (
                    f"scale_down.stabilization_window_seconds={window} "
                    "is too short for stable scaling"
                )

        su = getattr(behavior, "scale_up", None)
        if su is not None:
            window = getattr(su, "stabilization_window_seconds", None)
            if window is not None:
                assert int(window) >= 0, (
                    f"scale_up.stabilization_window_seconds={window} "
                    "must be non-negative"
                )

    def test_hpa_status_is_populated(self, live_hpa):
        """
        If the HPA has synced at least once, ``status.current_replicas`` is a
        non-negative integer.

        ``status`` itself is never None on a retrieved HPA object, so the
        only meaningful assertion is on its *content*.
        """
        status = live_hpa.status
        if status is None:
            pytest.skip("HPA status has not been populated yet")
        current = getattr(status, "current_replicas", None)
        if current is not None:
            assert int(current) >= 0


# ---------------------------------------------------------------------------
# Enhancement surface (skip when enhancements.enabled is false)
# ---------------------------------------------------------------------------
@pytest.mark.hpa
@pytest.mark.enhancements
class TestHPALegacyEnhancements:
    """Enhanced HPA surface — gated on the live ConfigMap flag."""

    @staticmethod
    def _enhancements_enabled(live_green_agent_config) -> bool:
        enh = live_green_agent_config["config"].get("enhancements") or {}
        return bool(enh.get("enabled"))

    def test_hpa_has_enhancement_annotations(
        self, live_hpa, live_green_agent_config,
    ):
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        annotations = live_hpa.metadata.annotations or {}
        required = (
            "green-agent/limit-graph",
            "green-agent/modp",
            "green-agent/rlhf",
            "green-agent/distillation",
            "green-agent/bio-inspired",
            "green-agent/moe",
            "green-agent/flexgen",
        )
        missing = [k for k in required if annotations.get(k) != "true"]
        assert not missing, (
            f"HPA missing enhancement annotations: {missing}"
        )

    def test_hpa_enhanced_metrics_exist_in_prometheus(
        self, live_green_agent_config, prometheus_metrics,
    ):
        """
        When enhancements are enabled, the metric families the HPA would use
        must be exported on ``/metrics``.

        This checks metric *export*, not HPA *configuration*. Export is
        decided by ``run_agent.py``; HPA configuration is a deployment
        concern and is checked separately by ``test_hpa_evolutionary_metric_
        configured``.
        """
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        required = (
            "green_agent_modp_score",
            "green_agent_rlhf_feedback",
            "green_agent_graph_centrality",
            "green_agent_graph_connectivity",
            "green_agent_distillation_update_count",
            "green_agent_moe_gate_weight",
            "green_agent_evolutionary_best_fitness",
            "green_agent_flexgen_energy_joules_total",
        )
        missing = [
            name for name in required
            if name not in prometheus_metrics["family_names"]
            and name not in prometheus_metrics["sample_names"]
        ]
        assert not missing, (
            f"Enhanced metrics claimed enabled but not exported on /metrics: "
            f"{missing}"
        )

    def test_hpa_evolutionary_metric_configured(self, live_hpa):
        """
        If the HPA references ``evolutionary_best_fitness``, its target value
        must be a positive finite number. If the metric is not referenced,
        skip — the evolutionary HPA metric is optional.

        v1's version of this test passed vacuously when the metric was
        absent (the loop body never ran and no assertion executed).
        """
        target_metric = "evolutionary_best_fitness"
        names = _parse_metric_names_from_hpa(live_hpa)
        if target_metric not in names:
            pytest.skip(
                f"HPA does not reference {target_metric}; "
                "evolutionary HPA metric is optional"
            )

        # The metric is declared — find and validate it.
        for metric in (live_hpa.spec.metrics or []):
            if getattr(metric, "type", None) != "Pods":
                continue
            try:
                name = metric.pods.metric.name
            except AttributeError:
                continue
            if name != target_metric:
                continue

            target = getattr(metric.pods, "target", None)
            avg = getattr(target, "average_value", None)
            assert avg is not None, (
                f"{target_metric} has no average_value target"
            )
            value = float(avg)
            assert value > 0, (
                f"{target_metric} average_value={value} must be positive"
            )
            return

        pytest.fail(
            f"HPA declares {target_metric} in a Pods metric but it could "
            "not be read back — metric configuration is malformed"
        )


# ---------------------------------------------------------------------------
# Offline smoke test — validates helpers without a cluster.
# Run: python tests/integration/test_hpa.py
# ---------------------------------------------------------------------------
def _smoke_test_helpers() -> int:
    from types import SimpleNamespace
    import unittest.mock as mock

    # --- _replicas_bounds ---
    assert _replicas_bounds(
        {"replicas": 3, "minReplicas": 1, "maxReplicas": 10}
    ) == (1, 3, 10)
    assert _replicas_bounds({"replicas": 2}) == (2, 2, 2)
    assert _replicas_bounds({}) == (0, 0, 0)
    print("ok  _replicas_bounds")

    # --- _parse_metric_names_from_hpa with a well-formed HPA ---
    def _pods_metric(name):
        return SimpleNamespace(
            type="Pods",
            pods=SimpleNamespace(
                metric=SimpleNamespace(name=name),
                target=SimpleNamespace(average_value="0.5"),
            ),
        )

    def _external_metric(name):
        return SimpleNamespace(
            type="External",
            external=SimpleNamespace(metric=SimpleNamespace(name=name)),
        )

    def _object_metric(name):
        return SimpleNamespace(
            type="Object",
            object=SimpleNamespace(metric=SimpleNamespace(name=name)),
        )

    def _resource_metric(name):
        return SimpleNamespace(
            type="Resource",
            resource=SimpleNamespace(name=name),
        )

    hpa = SimpleNamespace(
        spec=SimpleNamespace(metrics=[
            _pods_metric("modp_score"),
            _external_metric("carbon_intensity"),
            _object_metric("latency_p99"),
            _resource_metric("cpu"),
        ]),
    )
    names = _parse_metric_names_from_hpa(hpa)
    assert names == {"modp_score", "carbon_intensity", "latency_p99", "cpu"}, names
    print("ok  _parse_metric_names_from_hpa (well-formed)")

    # --- _parse_metric_names_from_hpa tolerates malformed entries ---
    malformed = SimpleNamespace(
        spec=SimpleNamespace(metrics=[
            SimpleNamespace(type="Pods", pods=None),
            SimpleNamespace(type="External",
                            external=SimpleNamespace(metric=None)),
            SimpleNamespace(type="Object"),
            SimpleNamespace(type="Unknown"),
        ]),
    )
    assert _parse_metric_names_from_hpa(malformed) == set()
    print("ok  _parse_metric_names_from_hpa (malformed)")

    # --- _parse_metric_names_from_hpa tolerates no metrics at all ---
    assert _parse_metric_names_from_hpa(
        SimpleNamespace(spec=SimpleNamespace(metrics=None))
    ) == set()
    assert _parse_metric_names_from_hpa(
        SimpleNamespace(spec=SimpleNamespace(metrics=[]))
    ) == set()
    print("ok  _parse_metric_names_from_hpa (empty)")

    # --- _skip_or_fail honours STRICT_INTEGRATION ---
    with mock.patch("pytest.skip") as mskip:
        _skip_or_fail("not reachable")
        mskip.assert_called_once()

    global STRICT_INTEGRATION
    saved = STRICT_INTEGRATION
    STRICT_INTEGRATION = True
    try:
        with mock.patch("pytest.fail") as mfail:
            _skip_or_fail("not reachable")
            mfail.assert_called_once()
    finally:
        STRICT_INTEGRATION = saved
    print("ok  _skip_or_fail honours STRICT_INTEGRATION")

    # --- _replicas_bounds is consistent with the RayCluster invariant ---
    g_ok = {"replicas": 4, "minReplicas": 2, "maxReplicas": 10}
    mn, reps, mx = _replicas_bounds(g_ok)
    assert mn <= reps <= mx, f"bounds invariant violated: {mn},{reps},{mx}"
    g_bad = {"replicas": 15, "minReplicas": 2, "maxReplicas": 10}
    mn, reps, mx = _replicas_bounds(g_bad)
    assert not (mn <= reps <= mx), (
        "bad bounds should be detectable: 15 not in [2,10]"
    )
    print("ok  _replicas_bounds invariant detection")

    # --- env-var defaults are stable ---
    assert isinstance(HPA_NAME, str) and HPA_NAME
    assert isinstance(RAY_CLUSTER_NAME, str) and RAY_CLUSTER_NAME
    assert API_TIMEOUT > 0
    print("ok  env-var defaults")

    return 0


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
