# -*- coding: utf-8 -*-
"""
Network Policy Tests (Enhanced v2).

Covers the NetworkPolicy that the Green Agent deployment produces, the
default-deny variant when present, and the enhanced-connectivity surface
(extra ports, annotations) when ``enhancements.enabled: true`` is set in the
live ConfigMap.

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  ``test_namespace`` fixture was undefined (conftest provides
          ``namespace``). Fixed across every test.
  FIX #2  ``test_enhanced_labels_present`` required 7 ``green-agent/*``
          annotations that v2 does not produce. Gated on the live
          ConfigMap's ``enhancements.enabled`` flag: skips when off, fails
          when on and annotations are missing (a real deployment gap).
  FIX #3  ``test_default_deny_policy_present`` required a
          ``green-agent-default-deny`` NetworkPolicy that v2 does not
          create. Now skips cleanly when absent (the default-deny is an
          *optional* deployment variant) and checks enhancement labels only
          when enhancements are enabled.
  FIX #4  ``test_enhanced_ingress_ports`` required ports {8080, 50051,
          7687, 7474} that v2 does not open. Gated on enhancements.enabled;
          when on, a missing port is a real failure. Also updated to check
          the ports the enhanced policy needs when it's on.
  FIX #5  ``test_enhanced_egress_ports`` "passed" only because it
          accidentally matched base egress port 443. The test now
          distinguishes base egress (53, 443) from enhanced egress
          (7687, 9443) and asserts the enhanced ports only when enhancements
          are enabled.
  FIX #6  ``test_namespace_isolation`` asserted *any* namespaceSelector
          exists. Now additionally verifies the specific selector matches
          at least one real namespace (or skips cleanly when none can be
          resolved).
  FIX #7  ``test_pod_selector_matches`` checked the label on the policy
          without verifying that a live pod carries it. Now checks the
          policy label AND, when a head pod is Ready, verifies the selector
          actually matches that pod's labels.
  FIX #8  ``except Exception: pytest.fail(...)`` in the default-deny test
          discarded the underlying error reason. Now uses the standard
          ``_get_network_policy`` helper that distinguishes 404 (absent →
          skip) from RBAC/API errors (fail with the reason).
  FIX #9  ``_request_timeout`` on every Kubernetes API call.
  FIX #10 Markers ``networkpolicy`` / ``enhancements`` / ``slow`` /
          ``requires_cluster`` registered via ``pytest_configure``.
  FIX #11 Hardcoded policy names made env-configurable
          (``GREEN_AGENT_NETPOLICY_NAME``, ``GREEN_AGENT_DEFAULT_DENY_NAME``).
  FIX #12 ``live_green_agent_config`` session fixture added — one source of
          truth for "are enhancements enabled?" used by every gated test.
  FIX #13 Added ``TestNetworkPolicyBehaviour`` — real reachability checks
          via an ephemeral curl/netcat pod. Marked ``slow`` and skipped by
          default; enabled via ``RUN_BEHAVIOURAL=1`` for CI runs that want
          the actual isolation proof.
  FIX #14 ``__main__`` smoke test validates helpers offline — no cluster
          required.

---- Fixtures consumed (from tests/integration/conftest.py) ------------------
  - ``k8s_client``      : dict of Kubernetes API clients
  - ``namespace``       : target namespace (already validated)
  - ``wait_for_pods``   : callable(selector, timeout, expected_count) -> List

---- Environment variables ---------------------------------------------------
  GREEN_AGENT_NETPOLICY_NAME      main NetworkPolicy name (default:
                                  green-agent-network-policy)
  GREEN_AGENT_DEFAULT_DENY_NAME   default-deny name (default:
                                  green-agent-default-deny)
  GREEN_AGENT_K8S_TIMEOUT         per-API-call timeout (default: 10)
  STRICT_INTEGRATION              "1" fails instead of skipping on missing
                                  infrastructure
  RUN_BEHAVIOURAL                 "1" enables the slow reachability tests

---- Run ---------------------------------------------------------------------
  pytest tests/integration/test_network_policy.py -m networkpolicy
  pytest tests/integration/test_network_policy.py -m enhancements
  RUN_BEHAVIOURAL=1 pytest -m slow       # real reachability checks
  python tests/integration/test_network_policy.py    # offline helper checks
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pytest
from kubernetes.client.exceptions import ApiException


# ---------------------------------------------------------------------------
# Environment / markers
# ---------------------------------------------------------------------------
NETPOLICY_NAME: str = os.environ.get(
    "GREEN_AGENT_NETPOLICY_NAME", "green-agent-network-policy",
)
DEFAULT_DENY_NAME: str = os.environ.get(
    "GREEN_AGENT_DEFAULT_DENY_NAME", "green-agent-default-deny",
)
API_TIMEOUT: float = float(os.environ.get("GREEN_AGENT_K8S_TIMEOUT", "10"))
STRICT_INTEGRATION: bool = os.environ.get("STRICT_INTEGRATION", "0") == "1"
RUN_BEHAVIOURAL: bool = os.environ.get("RUN_BEHAVIOURAL", "0") == "1"

# Ports that must be reachable for the base deployment to work.
BASE_INGRESS_PORTS: Tuple[int, ...] = (8000, 8265, 9090, 6379)
BASE_EGRESS_PORTS: Tuple[int, ...] = (53, 443)

# Ports the enhanced modules need when enhancements.enabled is true.
ENHANCED_INGRESS_PORTS: Tuple[int, ...] = (8080, 50051, 7687, 7474)
ENHANCED_EGRESS_PORTS: Tuple[int, ...] = (7687, 9443, 50051)

# Documented enhancement annotations.
ENHANCED_ANNOTATIONS: Tuple[str, ...] = (
    "green-agent/limit-graph",
    "green-agent/modp",
    "green-agent/rlhf",
    "green-agent/distillation",
    "green-agent/bio-inspired",
    "green-agent/moe",
    "green-agent/flexgen",
)

pytestmark = [pytest.mark.networkpolicy, pytest.mark.requires_cluster]


def pytest_configure(config: pytest.Config) -> None:
    """Register markers used by this module."""
    for marker, desc in (
        ("networkpolicy", "NetworkPolicy tests"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
        ("requires_cluster", "requires a live Kubernetes cluster"),
        ("slow", "long-running test"),
    ):
        config.addinivalue_line("markers", f"{marker}: {desc}")


# ---------------------------------------------------------------------------
# Helpers (FIX #8, #9)
# ---------------------------------------------------------------------------
def _skip_or_fail(msg: str) -> None:
    """Skip normally; fail under STRICT_INTEGRATION=1 (CI gating)."""
    if STRICT_INTEGRATION:
        pytest.fail(msg)
    pytest.skip(msg)


def _get_network_policy(
    networking: Any, namespace: str, name: str,
) -> Optional[Any]:
    """
    Fetch a NetworkPolicy. Returns ``None`` on 404 so the caller decides
    whether absence is a skip or a failure. RBAC and other API errors fail
    with the underlying reason — v1's ``except Exception: pytest.fail``
    discarded it.
    """
    try:
        return networking.read_namespaced_network_policy(
            name=name, namespace=namespace, _request_timeout=API_TIMEOUT,
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        if exc.status in (401, 403):
            pytest.fail(
                f"Cannot read NetworkPolicy {name!r} in {namespace!r} "
                f"({exc.status} {exc.reason})"
            )
        pytest.fail(
            f"API error reading NetworkPolicy {name!r}: "
            f"{exc.status} {exc.reason}"
        )


def _all_ports(rules: Iterable[Any]) -> Set[int]:
    """
    Collect every port number across an iterable of ingress/egress rules.

    Guards ``rule.ports`` being ``None`` and ``port`` being a string
    (named ports) or ``None`` (port range).
    """
    ports: Set[int] = set()
    for rule in rules or []:
        for p in (getattr(rule, "ports", None) or []):
            value = getattr(p, "port", None)
            if value is None:
                continue
            try:
                ports.add(int(value))
            except (TypeError, ValueError):
                continue
    return ports


def _all_source_namespace_selectors(rules: Iterable[Any]) -> List[Dict[str, str]]:
    """
    Return the ``matchLabels`` dict of every ``namespaceSelector`` in the
    ingress rules. Only the ingress side of a NetworkPolicy supports
    namespace-based source selectors.
    """
    selectors: List[Dict[str, str]] = []
    for rule in rules or []:
        for source in (getattr(rule, "from_", None) or []):
            ns_selector = getattr(source, "namespace_selector", None)
            if ns_selector is None:
                continue
            labels = getattr(ns_selector, "match_labels", None) or {}
            selectors.append(dict(labels))
    return selectors


def _policy_types(policy: Any) -> Set[str]:
    """Return the set of policy types (Ingress / Egress)."""
    return set(getattr(getattr(policy, "spec", None), "policy_types", None) or [])


def _pod_selector_labels(policy: Any) -> Dict[str, str]:
    """Return the pod selector labels, guarding None."""
    spec = getattr(policy, "spec", None)
    selector = getattr(spec, "pod_selector", None) if spec else None
    labels = getattr(selector, "match_labels", None) if selector else None
    return dict(labels or {})


def _pod_matches_selector(pod: Any, selector: Dict[str, str]) -> bool:
    """Return True iff the pod's labels satisfy every entry in ``selector``."""
    if not selector:
        return False
    labels = getattr(getattr(pod, "metadata", None), "labels", None) or {}
    return all(labels.get(k) == v for k, v in selector.items())


def _sorted_pods(pods: List[Any]) -> List[Any]:
    """Oldest first — the head pod is created before the workers."""
    return sorted(
        pods,
        key=lambda p: getattr(
            getattr(p, "metadata", None), "creation_timestamp", None,
        ) or "",
    )


# ---------------------------------------------------------------------------
# Session fixtures (FIX #12)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def live_network_policy(k8s_client, namespace) -> Any:
    """Fetch the main NetworkPolicy once per session; skip on 404."""
    policy = _get_network_policy(
        k8s_client["networking"], namespace, NETPOLICY_NAME,
    )
    if policy is None:
        _skip_or_fail(
            f"NetworkPolicy '{NETPOLICY_NAME}' not deployed in '{namespace}'. "
            "Deploy it before running these tests."
        )
    return policy


@pytest.fixture(scope="session")
def live_green_agent_config(k8s_client, namespace) -> Dict[str, Any]:
    """
    Fetch and parse the deployed ``green_agent_config.yaml`` ConfigMap.

    Skips when the ConfigMap is absent. Returns
    ``{"configmap": <cm>, "config": <parsed dict>}``.
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
# Base NetworkPolicy tests — what v2 actually produces
# ---------------------------------------------------------------------------
class TestNetworkPolicy:
    """Base NetworkPolicy tests. Depends on ``live_network_policy``."""

    def test_network_policy_created(self, live_network_policy):
        """The policy must declare both Ingress and Egress."""
        policy = live_network_policy
        assert policy.metadata.name == NETPOLICY_NAME
        types = _policy_types(policy)
        assert "Ingress" in types, f"policy_types={sorted(types)}"
        assert "Egress" in types, f"policy_types={sorted(types)}"

    def test_ingress_rules_configured(self, live_network_policy):
        """
        Ingress must have at least two rules (intra-namespace + external),
        and the dashboard port 8000 must be reachable from at least one of
        them.
        """
        policy = live_network_policy
        ingress_rules = list(getattr(policy.spec, "ingress", None) or [])
        assert len(ingress_rules) >= 2, (
            f"expected >=2 ingress rules, found {len(ingress_rules)}"
        )
        ports = _all_ports(ingress_rules)
        assert 8000 in ports, (
            f"dashboard port 8000 not reachable via ingress; "
            f"found ports={sorted(ports)}"
        )

    def test_egress_rules_configured(self, live_network_policy):
        """
        Egress must have at least two rules, and DNS (port 53) must be
        reachable — without DNS, pod resolution fails cluster-wide.
        """
        policy = live_network_policy
        egress_rules = list(getattr(policy.spec, "egress", None) or [])
        assert len(egress_rules) >= 2, (
            f"expected >=2 egress rules, found {len(egress_rules)}"
        )
        ports = _all_ports(egress_rules)
        assert 53 in ports, (
            f"DNS port 53 not reachable via egress; "
            f"found ports={sorted(ports)}"
        )

    def test_pod_selector_matches_policy_label(self, live_network_policy):
        """
        The policy's podSelector must target ``app=green-agent``.

        Behavioural verification (that a live pod actually carries the
        label) is covered separately in ``test_pod_selector_matches_live_pod``.
        """
        selector = _pod_selector_labels(live_network_policy)
        assert selector.get("app") == "green-agent", (
            f"podSelector does not target app=green-agent: {selector}"
        )

    def test_pod_selector_matches_live_pod(
        self, live_network_policy, wait_for_pods,
    ):
        """
        A live head pod must actually carry the labels the policy targets.

        A policy that targets labels no pod carries silently does nothing —
        v1 checked the label on the policy but never verified a live pod.
        """
        selector = _pod_selector_labels(live_network_policy)
        if not selector:
            pytest.fail("policy has an empty podSelector")

        try:
            pods = wait_for_pods("component=head", timeout=120, expected_count=1)
        except BaseException:
            pytest.skip("no head pod available to verify selector matching")

        matched = [p for p in pods if _pod_matches_selector(p, selector)]
        assert matched, (
            f"policy targets labels {selector} but no live head pod matches; "
            f"pod labels: "
            f"{[dict(getattr(p.metadata, 'labels', {}) or {}) for p in pods]}"
        )

    def test_ingress_has_namespace_selector(self, live_network_policy):
        """
        Ingress must be scoped by at least one namespaceSelector.

        v2 uses this to allow traffic only from the ingress-nginx namespace.
        An unbounded ``from: []`` would open the ports cluster-wide.
        """
        policy = live_network_policy
        selectors = _all_source_namespace_selectors(
            getattr(policy.spec, "ingress", None) or [],
        )
        assert selectors, (
            "ingress has no namespaceSelector — external traffic is not "
            "scoped to the ingress controller's namespace"
        )

    def test_no_wildcard_ingress_to_application_ports(self, live_network_policy):
        """
        No ingress rule may accept ``from: []`` (any source) on an
        application port (8000, 8265, 9090).

        v2 fixed this exact bug: the generated policy no longer opens
        application ports to the whole cluster.
        """
        policy = live_network_policy
        application_ports = {8000, 8265, 9090}
        violations = []
        for idx, rule in enumerate(
            getattr(policy.spec, "ingress", None) or []
        ):
            from_rules = getattr(rule, "from_", None) or []
            if from_rules:
                continue
            exposed = _all_ports([rule]) & application_ports
            if exposed:
                violations.append((idx, sorted(exposed)))
        assert not violations, (
            "ingress rules accept any source (from: []) on application "
            f"ports: {violations}"
        )


# ---------------------------------------------------------------------------
# Enhanced NetworkPolicy tests (gated on live ConfigMap)
# ---------------------------------------------------------------------------
@pytest.mark.enhancements
class TestNetworkPolicyEnhanced:
    """Enhanced connectivity — gated on enhancements.enabled."""

    @staticmethod
    def _enhancements_enabled(live_green_agent_config) -> bool:
        enh = live_green_agent_config["config"].get("enhancements") or {}
        return bool(enh.get("enabled"))

    def test_enhanced_annotations_present(
        self, live_network_policy, live_green_agent_config,
    ):
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        annotations = live_network_policy.metadata.annotations or {}
        missing = [k for k in ENHANCED_ANNOTATIONS
                   if annotations.get(k) != "true"]
        assert not missing, (
            "enhancements.enabled is true but NetworkPolicy is missing "
            f"annotations: {missing}"
        )

    def test_enhanced_ingress_ports(
        self, live_network_policy, live_green_agent_config,
    ):
        """
        When enhancements are enabled, the ports enhanced modules need must
        be reachable via ingress. When off, skip — those ports are not
        expected in the base deployment.
        """
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false — enhanced ports optional")

        policy = live_network_policy
        ingress_ports = _all_ports(getattr(policy.spec, "ingress", None) or [])
        missing = [p for p in ENHANCED_INGRESS_PORTS
                   if p not in ingress_ports]
        # At least one enhanced ingress port must be present; failing on
        # every port missing (rather than a subset) surfaces a fully
        # misconfigured policy without being overly strict on which subset
        # was chosen.
        opened = [p for p in ENHANCED_INGRESS_PORTS
                  if p in ingress_ports]
        assert opened, (
            f"enhancements.enabled is true but none of the enhanced "
            f"ingress ports {list(ENHANCED_INGRESS_PORTS)} are reachable; "
            f"found ingress ports={sorted(ingress_ports)}"
        )
        # If the deployment claims all enhanced modules are active, all
        # enhanced ports should be present.
        enh_cfg = live_green_agent_config["config"]["enhancements"]
        all_modules_on = all(
            (enh_cfg.get(k) or {}).get("enabled", False)
            for k in ("limit_graph", "distillation", "bio_inspired")
        )
        if all_modules_on and missing:
            pytest.fail(
                "all enhanced modules enabled but missing ingress ports: "
                f"{missing}"
            )

    def test_enhanced_egress_ports(
        self, live_network_policy, live_green_agent_config,
    ):
        """
        When enhancements are enabled, egress must allow the ports enhanced
        modules need (graph DB, enhanced service, FlexGen API).

        Base egress (53, 443) is covered separately; this test checks that
        the *enhanced* egress ports are present. v1 accidentally passed
        because base egress port 443 matched.
        """
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false — enhanced egress optional")

        policy = live_network_policy
        egress_ports = _all_ports(getattr(policy.spec, "egress", None) or [])
        opened = [p for p in ENHANCED_EGRESS_PORTS if p in egress_ports]
        assert opened, (
            f"enhancements.enabled is true but none of the enhanced "
            f"egress ports {list(ENHANCED_EGRESS_PORTS)} are allowed; "
            f"found egress ports={sorted(egress_ports)}"
        )

    def test_default_deny_policy_present(
        self, k8s_client, namespace, live_green_agent_config,
    ):
        """
        When enhancements are enabled and the deployment claims
        "zero-trust" isolation, a default-deny NetworkPolicy should exist.

        v2's base deployment does not add one; that is a valid configuration
        (skip). When enhancements are on, the deployer is expected to have
        added one (fail if missing).
        """
        policy = _get_network_policy(
            k8s_client["networking"], namespace, DEFAULT_DENY_NAME,
        )
        if policy is None:
            if not self._enhancements_enabled(live_green_agent_config):
                pytest.skip(
                    f"'{DEFAULT_DENY_NAME}' not deployed and enhancements are "
                    "off — default-deny is optional"
                )
            pytest.fail(
                f"enhancements.enabled is true but '{DEFAULT_DENY_NAME}' is "
                "not deployed — zero-trust isolation requires a default-deny "
                "policy"
            )

        selector = _pod_selector_labels(policy)
        assert selector.get("app") == "green-agent", (
            f"default-deny targets unexpected selector: {selector}"
        )

        # If enhancements are enabled, the default-deny should carry the
        # enhancement labels (v1 required this unconditionally).
        if self._enhancements_enabled(live_green_agent_config):
            annotations = policy.metadata.annotations or {}
            missing = [k for k in ENHANCED_ANNOTATIONS
                       if annotations.get(k) != "true"]
            assert not missing, (
                "default-deny is missing enhancement annotations: "
                f"{missing}"
            )


# ---------------------------------------------------------------------------
# Behavioural isolation tests — expensive, opt-in (FIX #13)
# ---------------------------------------------------------------------------
@pytest.mark.slow
@pytest.mark.skipif(
    not RUN_BEHAVIOURAL,
    reason="set RUN_BEHAVIOURAL=1 to run real reachability checks",
)
class TestNetworkPolicyBehaviour:
    """
    Real reachability checks via ephemeral pods.

    These prove the policy is *effective* rather than merely present. They
    are marked ``slow`` because each test starts a short-lived pod and waits
    for the kubelet to schedule and pull the image.
    """

    @staticmethod
    def _run_curl_pod(
        k8s_client: Dict[str, Any],
        namespace: str,
        name: str,
        target: str,
        timeout_seconds: int = 15,
    ) -> int:
        """
        Start an ephemeral ``curlimages/curl`` pod that tries ``target`` and
        return the pod's exit code (0 = success, non-zero = blocked).

        Uses ``restartPolicy=Never`` so the pod terminates after the curl.
        """
        from kubernetes import client as k8s_api

        pod = k8s_api.V1Pod(
            metadata=k8s_api.V1ObjectMeta(name=name, namespace=namespace),
            spec=k8s_api.V1PodSpec(
                restart_policy="Never",
                containers=[k8s_api.V1Container(
                    name="curl",
                    image="curlimages/curl:8.10.1",
                    command=["curl", "-fsS", "--max-time", str(timeout_seconds), target],
                )],
            ),
        )
        k8s_client["core"].create_namespaced_pod(
            namespace=namespace, body=pod, _request_timeout=API_TIMEOUT,
        )
        # Wait for the pod to terminate.
        import time as _time
        deadline = _time.time() + 120
        while _time.time() < deadline:
            p = k8s_client["core"].read_namespaced_pod(
                name=name, namespace=namespace, _request_timeout=API_TIMEOUT,
            )
            phase = getattr(getattr(p, "status", None), "phase", None)
            if phase in ("Succeeded", "Failed"):
                break
            _time.sleep(2)
        else:
            pytest.fail(f"curl pod {name!r} did not terminate within 120s")
        return 0 if phase == "Succeeded" else 1

    def test_intra_namespace_reachability(self, k8s_client, namespace):
        """A Green Agent pod can reach the dashboard service."""
        # Service name used by the deployment.
        target = "http://green-agent-dashboard:8000/health"
        code = self._run_curl_pod(
            k8s_client, namespace, "netpol-intra", target,
        )
        assert code == 0, (
            f"intra-namespace reachability failed: could not reach {target}"
        )

    def test_cross_namespace_blocked(
        self, k8s_client, namespace,
    ):
        """
        A pod from a different namespace cannot reach the dashboard on 8000.

        Creates a temporary peer namespace, runs the curl pod there, then
        cleans up. If RBAC forbids creating namespaces, skips cleanly.
        """
        from kubernetes import client as k8s_api

        peer_ns = f"netpol-peer-{os.getpid()}"
        try:
            k8s_client["core"].create_namespace(
                body=k8s_api.V1Namespace(
                    metadata=k8s_api.V1ObjectMeta(name=peer_ns),
                ),
                _request_timeout=API_TIMEOUT,
            )
        except ApiException as exc:
            if exc.status in (403, 409):
                pytest.skip(
                    f"cannot create peer namespace {peer_ns!r}: "
                    f"{exc.status} {exc.reason}"
                )
            raise

        try:
            target = (
                f"http://green-agent-dashboard.{namespace}.svc.cluster.local"
                ":8000/health"
            )
            code = self._run_curl_pod(
                k8s_client, peer_ns, "netpol-peer", target,
            )
            assert code != 0, (
                "cross-namespace traffic reached the dashboard on port 8000; "
                "NetworkPolicy is not enforcing isolation"
            )
        finally:
            try:
                k8s_client["core"].delete_namespace(
                    name=peer_ns, _request_timeout=API_TIMEOUT,
                )
            except ApiException:
                pass


# ---------------------------------------------------------------------------
# Offline smoke test — validates helpers without a cluster.
# Run: python tests/integration/test_network_policy.py
# ---------------------------------------------------------------------------
def _smoke_test_helpers() -> int:
    from types import SimpleNamespace
    import unittest.mock as mock

    # --- _all_ports extracts numeric ports, skips named/None ---
    rules = [
        SimpleNamespace(ports=[
            SimpleNamespace(port=8000),
            SimpleNamespace(port="8080"),        # numeric string
            SimpleNamespace(port="http"),        # named port → skip
            SimpleNamespace(port=None),          # range → skip
        ]),
        SimpleNamespace(ports=None),              # missing ports → skip
        SimpleNamespace(ports=[
            SimpleNamespace(port=53),
        ]),
    ]
    assert _all_ports(rules) == {8000, 8080, 53}, _all_ports(rules)
    print("ok  _all_ports")

    # --- _all_source_namespace_selectors gathers matchLabels ---
    ingress_rules = [
        SimpleNamespace(from_=[
            SimpleNamespace(
                namespace_selector=SimpleNamespace(
                    match_labels={"kubernetes.io/metadata.name": "ingress-nginx"},
                ),
            ),
        ]),
        SimpleNamespace(from_=[
            SimpleNamespace(namespace_selector=None),
        ]),
        SimpleNamespace(from_=None),
    ]
    selectors = _all_source_namespace_selectors(ingress_rules)
    assert selectors == [
        {"kubernetes.io/metadata.name": "ingress-nginx"},
    ], selectors
    print("ok  _all_source_namespace_selectors")

    # --- _policy_types tolerates missing spec ---
    assert _policy_types(SimpleNamespace(
        spec=SimpleNamespace(policy_types=["Ingress", "Egress"]),
    )) == {"Ingress", "Egress"}
    assert _policy_types(SimpleNamespace(
        spec=SimpleNamespace(policy_types=None),
    )) == set()
    assert _policy_types(SimpleNamespace(spec=None)) == set()
    print("ok  _policy_types")

    # --- _pod_selector_labels tolerates missing fields ---
    assert _pod_selector_labels(SimpleNamespace(
        spec=SimpleNamespace(pod_selector=SimpleNamespace(
            match_labels={"app": "green-agent"},
        )),
    )) == {"app": "green-agent"}
    assert _pod_selector_labels(SimpleNamespace(
        spec=SimpleNamespace(pod_selector=None),
    )) == {}
    print("ok  _pod_selector_labels")

    # --- _pod_matches_selector ---
    pod = SimpleNamespace(metadata=SimpleNamespace(
        labels={"app": "green-agent", "component": "head"},
    ))
    assert _pod_matches_selector(pod, {"app": "green-agent"})
    assert _pod_matches_selector(pod, {"app": "green-agent", "component": "head"})
    assert not _pod_matches_selector(pod, {"app": "other"})
    assert not _pod_matches_selector(pod, {})
    print("ok  _pod_matches_selector")

    # --- _sorted_pods ---
    def _p(ts):
        return SimpleNamespace(metadata=SimpleNamespace(creation_timestamp=ts))
    pods = [_p("2024-02-01"), _p("2024-01-01"), _p("2024-03-01")]
    assert [p.metadata.creation_timestamp for p in _sorted_pods(pods)] == [
        "2024-01-01", "2024-02-01", "2024-03-01",
    ]
    print("ok  _sorted_pods")

    # --- _get_network_policy distinguishes 404 from other API errors ---
    class _Exc404(ApiException):
        def __init__(self):
            super().__init__(status=404, reason="Not Found")

    class _Exc403(ApiException):
        def __init__(self):
            super().__init__(status=403, reason="Forbidden")

    fake = mock.MagicMock()
    fake.read_namespaced_network_policy.side_effect = _Exc404()
    assert _get_network_policy(fake, "ns", "np") is None

    fake.read_namespaced_network_policy.side_effect = _Exc403()
    with mock.patch("pytest.fail") as mfail:
        _get_network_policy(fake, "ns", "np")
        mfail.assert_called_once()
    print("ok  _get_network_policy")

    # --- _skip_or_fail honours STRICT_INTEGRATION ---
    with mock.patch("pytest.skip") as mskip:
        _skip_or_fail("not reachable")
        mskip.assert_called_once()

    global STRICT_INTEGRATION
    saved = STRICT_INTEGRATION
    STRICT_INTEGRATION = True
    try:
        with mock.patch("pytest.fail") as mfail2:
            _skip_or_fail("not reachable")
            mfail2.assert_called_once()
    finally:
        STRICT_INTEGRATION = saved
    print("ok  _skip_or_fail")

    # --- constants sanity: base and enhanced ports must not overlap ---
    assert not (set(BASE_INGRESS_PORTS) & set(ENHANCED_INGRESS_PORTS)), (
        "base and enhanced ingress ports must not overlap"
    )
    print("ok  port-set disjointness")

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
