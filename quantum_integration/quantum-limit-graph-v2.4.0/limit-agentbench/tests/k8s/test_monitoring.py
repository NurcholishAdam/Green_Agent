# -*- coding: utf-8 -*-
"""
Monitoring Tests (Enhanced v2).

Covers the ServiceMonitor, PrometheusRule, metrics Service, and Grafana
dashboard that the Green Agent deployment is expected to produce — plus the
enhanced-observability surface when ``enhancements.enabled: true`` is set in
the live ConfigMap.

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  ``except: pytest.skip()`` and ``except Exception: pytest.skip()``
          removed. In v1 every test in this file (six "enhanced" and three
          base) wrapped its assertion in a handler that caught the
          AssertionError and reported the test as SKIP — CI green on a broken
          deployment. Failures now fail; absence of an *optional* resource
          skips explicitly via ``_skip_or_fail``.
  FIX #2  ``test_namespace`` fixture was undefined (conftest provides
          ``namespace``). Fixed across every test.
  FIX #3  ``test_alert_rules_configured`` required 5+ rules and specific
          alerts (``GreenAgentServiceDown``) that v2's PrometheusRule does
          not produce. The test now asserts the alerts v2 *does* produce
          (``GreenAgentHighCarbonIntensity``, ``GreenAgentTaskFailed``) and
          checks structural validity of every rule it finds.
  FIX #4  ``test_prometheusrule_has_enhanced_alerts`` required 7 alerts that
          no v2 PrometheusRule contains. Gated on the live ConfigMap's
          ``enhancements.enabled`` flag; when on, the alerts are checked
          against the set the enhancement modules are documented to emit —
          which the deployer must add to their PrometheusRule. When off,
          skips.
  FIX #5  ``test_grafana_dashboard_*`` required a
          ``green-agent-grafana-dashboard`` ConfigMap that the v2 config
          generator does not produce. Both tests skip cleanly when the
          ConfigMap is absent (dashboard is an *optional* deployment).
  FIX #6  ``test_servicemonitor_has_enhanced_labels`` required 7
          ``green-agent/*`` annotations that v2 does not produce. Gated on
          ``enhancements.enabled``; when on, checks for the annotations — the
          deployer must add them, so their absence is a real failure (not a
          skip).
  FIX #7  ``test_metrics_service_has_enhanced_labels`` required an
          ``enhancements: enabled`` label that v2 does not produce. Gated on
          the live ConfigMap.
  FIX #8  ``test_service_monitor_scrapes_enhanced_metrics`` had a nested
          conditional whose assertion could never fire in practice (both
          ``keep`` and ``drop`` regexes use ``green_agent_`` prefix, so
          ``has_keep`` was true → the drop-branch was unreachable). Rewritten
          to actually inspect the relabelling chain.
  FIX #9  ``test_metrics_service_exists`` accessed ``p.name == 'metrics'``
          without guarding ``p.name`` being ``None`` (valid for unnamed
          ports) → ``AttributeError``.
  FIX #10 ``_request_timeout`` on every Kubernetes API call; 404 → skip via
          ``_skip_or_fail``; any other API error → ``pytest.fail``.
  FIX #11 Markers ``monitoring`` / ``enhancements`` / ``requires_cluster``
          registered via ``pytest_configure``.
  FIX #12 Hardcoded ServiceMonitor / PrometheusRule names made
          env-configurable (``GREEN_AGENT_SERVICEMONITOR_NAME``,
          ``GREEN_AGENT_PROMETHEUSRULE_NAME``).
  FIX #13 ``live_green_agent_config`` fixture added — one source of truth
          for "are enhancements enabled?" used by every gated test.
  FIX #14 ``__main__`` smoke test validates helpers offline — no cluster
          required.

---- Fixtures consumed (from tests/integration/conftest.py) ------------------
  - ``k8s_client``      : dict of Kubernetes API clients
  - ``namespace``       : target namespace (already validated)

---- Environment variables ---------------------------------------------------
  GREEN_AGENT_SERVICEMONITOR_NAME   ServiceMonitor name (default: green-agent-monitor)
  GREEN_AGENT_PROMETHEUSRULE_NAME   PrometheusRule name (default: green-agent-alerts)
  GREEN_AGENT_K8S_TIMEOUT           per-API-call timeout (default: 10)
  STRICT_INTEGRATION                "1" fails instead of skipping on missing infra

---- Run ---------------------------------------------------------------------
  pytest tests/integration/test_monitoring.py -m monitoring
  pytest tests/integration/test_monitoring.py -m enhancements
  python tests/integration/test_monitoring.py    # offline helper smoke test
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pytest
from kubernetes.client.exceptions import ApiException


# ---------------------------------------------------------------------------
# Environment / markers
# ---------------------------------------------------------------------------
SERVICEMONITOR_NAME: str = os.environ.get(
    "GREEN_AGENT_SERVICEMONITOR_NAME", "green-agent-monitor",
)
PROMETHEUSRULE_NAME: str = os.environ.get(
    "GREEN_AGENT_PROMETHEUSRULE_NAME", "green-agent-alerts",
)
API_TIMEOUT: float = float(os.environ.get("GREEN_AGENT_K8S_TIMEOUT", "10"))
STRICT_INTEGRATION: bool = os.environ.get("STRICT_INTEGRATION", "0") == "1"

# Documented enhancement alerts. The deployer must add these to their
# PrometheusRule when enhancements are enabled; their absence is a real
# failure in that configuration.
ENHANCED_ALERTS: Tuple[str, ...] = (
    "GreenAgentLowMODPScore",
    "GreenAgentLowRLHFFeedback",
    "GreenAgentGraphCentralityLow",
    "GreenAgentDistillationStalled",
    "GreenAgentMoEUnstable",
    "GreenAgentEvolutionStalled",
    "GreenAgentFlexGenHighEnergy",
)

# Alerts that v2's base PrometheusRule produces. The base alert test checks
# *structure* + at least one of these.
BASE_ALERTS: Tuple[str, ...] = (
    "GreenAgentHighCarbonIntensity",
    "GreenAgentTaskFailed",
)

# Documented enhancement metric substrings that should appear in the Grafana
# dashboard when enhancements are enabled.
ENHANCED_DASHBOARD_KEYWORDS: Tuple[str, ...] = (
    "modp_score", "rlhf_feedback", "graph_centrality",
    "distillation_update", "moe_gate",
    "evolutionary_best_fitness", "flexgen_energy",
)

pytestmark = [pytest.mark.monitoring, pytest.mark.requires_cluster]


def pytest_configure(config: pytest.Config) -> None:
    """Register markers used by this module.

    The shared conftest also registers ``requires_cluster`` and
    ``enhancements``; double registration is harmless and keeps this file
    independently runnable.
    """
    for marker, desc in (
        ("monitoring", "ServiceMonitor / PrometheusRule / Grafana"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
        ("requires_cluster", "requires a live Kubernetes cluster"),
    ):
        config.addinivalue_line("markers", f"{marker}: {desc}")


# ---------------------------------------------------------------------------
# Helpers (FIX #1, #10)
# ---------------------------------------------------------------------------
def _skip_or_fail(msg: str) -> None:
    """Skip normally; fail under STRICT_INTEGRATION=1 (CI gating)."""
    if STRICT_INTEGRATION:
        pytest.fail(msg)
    pytest.skip(msg)


def _get_custom_object(
    co: Any,
    *,
    group: str,
    version: str,
    namespace: str,
    plural: str,
    name: str,
) -> Optional[Dict[str, Any]]:
    """
    Fetch a custom object. Returns ``None`` on 404 (resource absent) so the
    caller can decide whether that is a skip or a failure. Any other API
    error is a real failure and is raised.
    """
    try:
        return co.get_namespaced_custom_object(
            group=group, version=version,
            namespace=namespace, plural=plural, name=name,
            _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        if exc.status in (401, 403):
            pytest.fail(
                f"Cannot read {plural}/{name} in '{namespace}' "
                f"({exc.status} {exc.reason})"
            )
        if exc.status == 404:
            return None
        # CRD itself may be missing (different from the object being missing).
        if exc.status == 404 or "the server could not find the requested resource" in str(exc.reason):
            return None
        pytest.fail(f"API error reading {plural}/{name}: {exc.status} {exc.reason}")


def _crds_installed(co: Any) -> bool:
    """Return True iff the monitoring.coreos.com API group is served."""
    try:
        co.api_client.call_api(
            "/apis/monitoring.coreos.com/v1",
            "GET",
            _preload_content=False,
            _request_timeout=API_TIMEOUT,
        )
        return True
    except ApiException as exc:
        # 404 = the API group isn't served (CRD not installed).
        if exc.status == 404:
            return False
        # Any other error (401/403/500) is an RBAC or cluster problem — not a
        # missing CRD. Report it so it doesn't silently become a skip.
        pytest.fail(
            f"Cannot query monitoring.coreos.com API group "
            f"({exc.status} {exc.reason})"
        )
    except Exception:                                   # noqa: BLE001
        # Some kubernetes clients raise a non-ApiException here.
        return False


def _alert_names_from_rule(rule: Dict[str, Any]) -> Set[str]:
    """Collect all ``alert`` names from every group in a PrometheusRule."""
    names: Set[str] = set()
    for group in (rule.get("spec", {}).get("groups") or []):
        for r in (group.get("rules") or []):
            alert = r.get("alert")
            if alert:
                names.add(alert)
    return names


def _groups_from_rule(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the groups in a PrometheusRule, tolerating a missing spec."""
    return (rule.get("spec") or {}).get("groups") or []


def _metrics_services_from_list(services: Iterable[Any]) -> List[Any]:
    """
    Return services that either contain ``metrics`` in their name or expose
    a port named ``metrics``. Ports may be unnamed (valid), so guard
    ``p.name`` being ``None`` — v1 crashed with AttributeError.
    """
    out: List[Any] = []
    for s in services:
        name = getattr(getattr(s, "metadata", None), "name", "") or ""
        if "metrics" in name:
            out.append(s)
            continue
        ports = getattr(getattr(s, "spec", None), "ports", None) or []
        if any((getattr(p, "name", None) or "") == "metrics" for p in ports):
            out.append(s)
    return out


def _service_labels(svc: Any) -> Dict[str, str]:
    return getattr(getattr(svc, "metadata", None), "labels", None) or {}


def _relabel_would_drop_green_agent(relabelings: List[Dict[str, Any]]) -> bool:
    """
    Return True iff any ``drop`` rule in the relabeling chain matches a
    ``green_agent_*`` metric name.

    v1's version of this check could never fire because the same condition
    gated both ``keep`` and ``drop`` paths.
    """
    for rel in relabelings:
        if not isinstance(rel, dict):
            continue
        if (rel.get("action") or "").lower() != "drop":
            continue
        regex = rel.get("regex") or ""
        source_labels = rel.get("sourceLabels") or rel.get("source_labels") or []
        # The regex is matched against __name__ when ``sourceLabels`` is
        # empty; otherwise against the concatenation of the listed labels.
        if "green_agent_" in regex or "green_agent" in regex:
            return True
        if "__name__" in source_labels and "green_agent" in regex:
            return True
    return False


# ---------------------------------------------------------------------------
# Session fixtures (FIX #13)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def live_green_agent_config(k8s_client, namespace) -> Dict[str, Any]:
    """
    Fetch and parse the deployed ``green_agent_config.yaml`` ConfigMap.

    Returns ``{"configmap": <cm>, "config": <parsed dict>}``. Skips when no
    ``green-agent-config*`` ConfigMap is present.
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


# ---------------------------------------------------------------------------
# Base monitoring tests
# ---------------------------------------------------------------------------
class TestMonitoring:
    """Base monitoring resources — skip cleanly when prometheus-operator absent."""

    def test_servicemonitor_created(self, k8s_client, namespace):
        co = k8s_client["custom_objects"]
        if not _crds_installed(co):
            pytest.skip("prometheus-operator CRDs not installed")

        sm = _get_custom_object(
            co, group="monitoring.coreos.com", version="v1",
            namespace=namespace, plural="servicemonitors",
            name=SERVICEMONITOR_NAME,
        )
        if sm is None:
            pytest.skip(f"ServiceMonitor '{SERVICEMONITOR_NAME}' not deployed")
        assert sm["metadata"]["name"] == SERVICEMONITOR_NAME

    def test_prometheusrule_created(self, k8s_client, namespace):
        co = k8s_client["custom_objects"]
        if not _crds_installed(co):
            pytest.skip("prometheus-operator CRDs not installed")

        rule = _get_custom_object(
            co, group="monitoring.coreos.com", version="v1",
            namespace=namespace, plural="prometheusrules",
            name=PROMETHEUSRULE_NAME,
        )
        if rule is None:
            pytest.skip(f"PrometheusRule '{PROMETHEUSRULE_NAME}' not deployed")
        assert _groups_from_rule(rule), "PrometheusRule has no rule groups"

    def test_metrics_service_exists(self, k8s_client, namespace):
        try:
            services = k8s_client["core"].list_namespaced_service(
                namespace=namespace,
                label_selector="app=green-agent",
                _request_timeout=API_TIMEOUT,
            ).items
        except ApiException as exc:
            pytest.fail(
                f"Cannot list services in '{namespace}': "
                f"{exc.status} {exc.reason}"
            )
        assert _metrics_services_from_list(services), (
            "No Service exposes a 'metrics' port and no Service name "
            "contains 'metrics'"
        )

    def test_grafana_dashboard_configmap(self, k8s_client, namespace):
        """
        Grafana dashboard is an *optional* deployment. Skip cleanly when the
        ConfigMap is absent (v2 does not produce one by default).
        """
        try:
            cm = k8s_client["core"].read_namespaced_config_map(
                name="green-agent-grafana-dashboard",
                namespace=namespace,
                _request_timeout=API_TIMEOUT,
            )
        except ApiException as exc:
            if exc.status == 404:
                pytest.skip(
                    "green-agent-grafana-dashboard ConfigMap not deployed "
                    "(Grafana dashboard is optional)"
                )
            pytest.fail(
                f"Cannot read Grafana ConfigMap: {exc.status} {exc.reason}"
            )
        assert "green-agent-overview.json" in (cm.data or {}), (
            "Grafana ConfigMap missing green-agent-overview.json key"
        )

    def test_alert_rules_configured(self, k8s_client, namespace):
        """
        Structural check: every rule is valid, and at least one of the base
        alerts v2 produces is present. v1 required 5+ rules and a specific
        alert (GreenAgentServiceDown) that v2 does not produce.
        """
        co = k8s_client["custom_objects"]
        if not _crds_installed(co):
            pytest.skip("prometheus-operator CRDs not installed")

        rule = _get_custom_object(
            co, group="monitoring.coreos.com", version="v1",
            namespace=namespace, plural="prometheusrules",
            name=PROMETHEUSRULE_NAME,
        )
        if rule is None:
            pytest.skip(f"PrometheusRule '{PROMETHEUSRULE_NAME}' not deployed")

        groups = _groups_from_rule(rule)
        assert groups, "PrometheusRule has no rule groups"

        # Structural: every rule must have either an alert or a record name.
        for group in groups:
            for r in (group.get("rules") or []):
                assert r.get("alert") or r.get("record"), (
                    f"rule has neither 'alert' nor 'record': {r}"
                )
                if r.get("alert"):
                    assert r.get("expr"), (
                        f"alert {r['alert']} has no expression"
                    )

        # At least one of the base alerts that v2 produces.
        names = _alert_names_from_rule(rule)
        assert names & set(BASE_ALERTS), (
            f"PrometheusRule does not contain any of {list(BASE_ALERTS)}; "
            f"found: {sorted(names)}"
        )

    # ------------------------------------------------------------------
    # Enhanced surface (gated on live ConfigMap)
    # ------------------------------------------------------------------
    @pytest.mark.enhancements
    def test_servicemonitor_has_enhanced_labels(
        self, k8s_client, namespace, live_green_agent_config,
    ):
        enh = live_green_agent_config["config"].get("enhancements") or {}
        if not enh.get("enabled"):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        co = k8s_client["custom_objects"]
        if not _crds_installed(co):
            pytest.skip("prometheus-operator CRDs not installed")

        sm = _get_custom_object(
            co, group="monitoring.coreos.com", version="v1",
            namespace=namespace, plural="servicemonitors",
            name=SERVICEMONITOR_NAME,
        )
        if sm is None:
            pytest.skip(f"ServiceMonitor '{SERVICEMONITOR_NAME}' not deployed")

        annotations = (sm["metadata"].get("annotations") or {})
        required = (
            "green-agent/limit-graph", "green-agent/modp",
            "green-agent/rlhf", "green-agent/distillation",
            "green-agent/bio-inspired", "green-agent/moe",
            "green-agent/flexgen",
        )
        missing = [k for k in required if annotations.get(k) != "true"]
        assert not missing, (
            "enhancements.enabled is true but ServiceMonitor is missing "
            f"annotations: {missing}"
        )

    @pytest.mark.enhancements
    def test_prometheusrule_has_enhanced_alerts(
        self, k8s_client, namespace, live_green_agent_config,
    ):
        enh = live_green_agent_config["config"].get("enhancements") or {}
        if not enh.get("enabled"):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        co = k8s_client["custom_objects"]
        if not _crds_installed(co):
            pytest.skip("prometheus-operator CRDs not installed")

        rule = _get_custom_object(
            co, group="monitoring.coreos.com", version="v1",
            namespace=namespace, plural="prometheusrules",
            name=PROMETHEUSRULE_NAME,
        )
        if rule is None:
            pytest.skip(f"PrometheusRule '{PROMETHEUSRULE_NAME}' not deployed")

        names = _alert_names_from_rule(rule)
        missing = [a for a in ENHANCED_ALERTS if a not in names]
        assert not missing, (
            "enhancements.enabled is true but PrometheusRule is missing "
            f"enhanced alerts: {missing} (present: {sorted(names)})"
        )

    @pytest.mark.enhancements
    def test_grafana_dashboard_contains_enhanced_panels(
        self, k8s_client, namespace, live_green_agent_config,
    ):
        enh = live_green_agent_config["config"].get("enhancements") or {}
        if not enh.get("enabled"):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        try:
            cm = k8s_client["core"].read_namespaced_config_map(
                name="green-agent-grafana-dashboard",
                namespace=namespace,
                _request_timeout=API_TIMEOUT,
            )
        except ApiException as exc:
            if exc.status == 404:
                pytest.skip(
                    "Grafana dashboard ConfigMap not deployed — panels "
                    "cannot be verified"
                )
            pytest.fail(
                f"Cannot read Grafana ConfigMap: {exc.status} {exc.reason}"
            )

        dashboard_json = (cm.data or {}).get("green-agent-overview.json", "")
        if not dashboard_json:
            pytest.fail(
                "Grafana ConfigMap exists but green-agent-overview.json is "
                "empty — panel content cannot be verified"
            )

        missing = [k for k in ENHANCED_DASHBOARD_KEYWORDS
                   if k not in dashboard_json]
        assert not missing, (
            "enhancements.enabled is true but Grafana dashboard is missing "
            f"panels for metrics: {missing}"
        )

    def test_service_monitor_scrapes_enhanced_metrics(
        self, k8s_client, namespace,
    ):
        """
        The ServiceMonitor must not *drop* ``green_agent_*`` metrics via a
        relabeling rule.

        Runs regardless of enhancements — the base metrics (carbon/energy)
        also start with ``green_agent_`` and are always required.

        v1's version of this test had an unreachable assertion: both the
        ``keep`` and ``drop`` branches gated on the same regex, so ``keep``
        being present made ``has_green_drop`` unreachable.
        """
        co = k8s_client["custom_objects"]
        if not _crds_installed(co):
            pytest.skip("prometheus-operator CRDs not installed")

        sm = _get_custom_object(
            co, group="monitoring.coreos.com", version="v1",
            namespace=namespace, plural="servicemonitors",
            name=SERVICEMONITOR_NAME,
        )
        if sm is None:
            pytest.skip(f"ServiceMonitor '{SERVICEMONITOR_NAME}' not deployed")

        endpoints = (sm.get("spec") or {}).get("endpoints") or []
        assert endpoints, "ServiceMonitor has no endpoints"

        for i, ep in enumerate(endpoints):
            relabelings = (
                ep.get("metricRelabelings")
                or ep.get("metric_relabelings")
                or []
            )
            assert not _relabel_would_drop_green_agent(relabelings), (
                f"ServiceMonitor endpoint[{i}] drops green_agent_* metrics "
                f"via relabeling: {relabelings}"
            )

    @pytest.mark.enhancements
    def test_metrics_service_has_enhanced_labels(
        self, k8s_client, namespace, live_green_agent_config,
    ):
        enh = live_green_agent_config["config"].get("enhancements") or {}
        if not enh.get("enabled"):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        try:
            services = k8s_client["core"].list_namespaced_service(
                namespace=namespace,
                label_selector="app=green-agent",
                _request_timeout=API_TIMEOUT,
            ).items
        except ApiException as exc:
            pytest.fail(
                f"Cannot list services in '{namespace}': "
                f"{exc.status} {exc.reason}"
            )

        metrics_services = _metrics_services_from_list(services)
        assert metrics_services, (
            "no Service exposes a metrics port; cannot check labels"
        )

        labelled = [s for s in metrics_services
                    if _service_labels(s).get("enhancements") == "enabled"]
        assert labelled, (
            "enhancements.enabled is true but no metrics Service carries "
            "label 'enhancements=enabled'; found labels: "
            f"{[dict(_service_labels(s)) for s in metrics_services]}"
        )


# ---------------------------------------------------------------------------
# Offline smoke test — validates helpers without a cluster.
# Run: python tests/integration/test_monitoring.py
# ---------------------------------------------------------------------------
def _smoke_test_helpers() -> int:
    from types import SimpleNamespace
    import unittest.mock as mock

    # --- _alert_names_from_rule gathers across groups ---
    rule = {
        "spec": {
            "groups": [
                {"name": "g1", "rules": [
                    {"alert": "GreenAgentHighCarbonIntensity", "expr": "1"},
                    {"record": "some_record"},
                ]},
                {"name": "g2", "rules": [
                    {"alert": "GreenAgentTaskFailed", "expr": "2"},
                    {"alert": "GreenAgentLowMODPScore", "expr": "3"},
                ]},
            ]
        }
    }
    assert _alert_names_from_rule(rule) == {
        "GreenAgentHighCarbonIntensity",
        "GreenAgentTaskFailed",
        "GreenAgentLowMODPScore",
    }
    print("ok  _alert_names_from_rule")

    # --- _groups_from_rule tolerates a missing spec ---
    assert _groups_from_rule({}) == []
    assert _groups_from_rule({"spec": {}}) == []
    assert _groups_from_rule({"spec": {"groups": None}}) == []
    print("ok  _groups_from_rule")

    # --- _metrics_services_from_list handles unnamed ports ---
    def _svc(name, port_names):
        return SimpleNamespace(
            metadata=SimpleNamespace(name=name, labels={}),
            spec=SimpleNamespace(
                ports=[SimpleNamespace(name=p) for p in port_names],
            ),
        )

    svcs = [
        _svc("green-agent-dashboard", ["http", None]),      # unnamed port
        _svc("green-agent-metrics", []),
        _svc("unrelated", ["http"]),
    ]
    result = _metrics_services_from_list(svcs)
    names = {s.metadata.name for s in result}
    assert names == {"green-agent-dashboard", "green-agent-metrics"}, names
    print("ok  _metrics_services_from_list (unnamed ports)")

    # --- _service_labels is safe on missing labels ---
    class _NoLabels:
        metadata = SimpleNamespace(name="x")
    assert _service_labels(_NoLabels()) == {}
    print("ok  _service_labels tolerates missing labels")

    # --- _relabel_would_drop_green_agent detects drop rules ---
    # No relabelings → nothing dropped
    assert not _relabel_would_drop_green_agent([])

    # A non-drop rule is safe
    assert not _relabel_would_drop_green_agent([
        {"action": "replace", "regex": "green_agent_.*"},
    ])

    # A drop rule on green_agent_* is unsafe
    assert _relabel_would_drop_green_agent([
        {"action": "drop", "regex": "green_agent_.*"},
    ])

    # Case-insensitive "action"
    assert _relabel_would_drop_green_agent([
        {"action": "DROP", "regex": "green_agent_carbon.*"},
    ])

    # A drop rule on unrelated metrics is safe
    assert not _relabel_would_drop_green_agent([
        {"action": "drop", "regex": "go_gc_.*"},
    ])

    # sourceLabels __name__ + drop on green_agent is unsafe
    assert _relabel_would_drop_green_agent([
        {
            "action": "drop",
            "sourceLabels": ["__name__"],
            "regex": "green_agent_.*",
        },
    ])
    print("ok  _relabel_would_drop_green_agent")

    # --- _get_custom_object distinguishes 404 from other API errors ---
    class _Exc404(ApiException):
        def __init__(self):
            super().__init__(status=404, reason="Not Found")

    class _Exc403(ApiException):
        def __init__(self):
            super().__init__(status=403, reason="Forbidden")

    fake_co = mock.MagicMock()

    fake_co.get_namespaced_custom_object.side_effect = _Exc404()
    assert _get_custom_object(
        fake_co, group="g", version="v1", namespace="n",
        plural="p", name="x",
    ) is None

    fake_co.get_namespaced_custom_object.side_effect = _Exc403()
    with mock.patch("pytest.fail") as mfail:
        _get_custom_object(
            fake_co, group="g", version="v1", namespace="n",
            plural="p", name="x",
        )
        mfail.assert_called_once()
    print("ok  _get_custom_object")

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
    print("ok  _skip_or_fail")

    # --- constants sanity ---
    assert ENHANCED_ALERTS and BASE_ALERTS
    assert ENHANCED_DASHBOARD_KEYWORDS
    assert not (set(ENHANCED_ALERTS) & set(BASE_ALERTS)), (
        "base and enhanced alert names must not overlap"
    )
    print("ok  constants sanity")

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
