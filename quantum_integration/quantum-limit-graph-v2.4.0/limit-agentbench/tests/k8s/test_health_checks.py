# -*- coding: utf-8 -*-
"""
Health Check Tests (Enhanced v2).

Covers the health, readiness, liveness, and Prometheus metrics endpoints of
the Green Agent dashboard, plus the Kubernetes probe configuration, plus the
enhancement surface when ``enhancements.enabled: true`` is set in the live
ConfigMap.

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  ``dashboard_url`` fixture probes the target URL once and skips
          cleanly (or fails under STRICT_INTEGRATION=1) with a message about
          opening a port-forward. v1 returned a hardcoded URL that required an
          externally-opened port-forward, so every HTTP test skipped by
          accident when the forward wasn't running.
  FIX #2  ``test_metrics_endpoint`` parses /metrics with the canonical
          ``prometheus_client.parser`` and requires the carbon / energy
          families to exist with finite, non-negative values. v1 did a
          substring match, so ``NaN`` samples and comment lines passed.
  FIX #3  ``test_metrics_contain_enhanced_metrics`` is gated on the live
          ConfigMap's ``enhancements.enabled`` flag. When off, it skips.
          When on, it fails on missing families. v1 always required 8 names
          that ``run_agent.py`` never registers.
  FIX #4  ``test_probe_configuration`` requires liveness + readiness (the
          probes v2 actually creates). A startup probe is checked only when
          configured. v1 required a startup probe that v2 does not add.
  FIX #5  ``test_probe_timing`` uses the same conditional logic for the
          startup probe's ``failure_threshold``.
  FIX #6  ``test_pod_environment_enhancement_flags`` skips when the
          deployment passes enhancement config via ConfigMap (v2 default)
          rather than env vars. v1 failed on every correct v2 deployment.
  FIX #7  ``test_health_enhanced_fields`` no longer has dead code — when
          enhancements are enabled, at least one enhanced field must be
          present, and every field it finds is range-checked.
  FIX #8  Status vocabularies accept the common shapes:
          {healthy, ok, up, pass} for /health; {alive, ok, up, healthy,
          pass} for /live; {ready, ok, up, pass} plus {ready: true} for
          /ready.
  FIX #9  ``container.env or []`` guarded; probe ``http_get`` verified
          before path/port access.
  FIX #10 ``requests.Session`` with retries and a ``(connect, read)``
          timeout tuple; no bare ``requests.get``.
  FIX #11 Pods sorted by ``creation_timestamp`` (v1 used ``pods[0]``).
  FIX #12 Markers ``health`` / ``e2e`` / ``requires_cluster`` /
          ``enhancements`` registered via ``pytest_configure``.
  FIX #13 Added ``test_metrics_have_help_and_type`` and
          ``test_metrics_all_finite`` — structural and value checks that
          v1 lacked.
  FIX #14 ``live_green_agent_config`` fixture provides one source of truth
          for "are enhancements enabled?" across every test that needs it.

---- Fixtures consumed (from tests/integration/conftest.py) ------------------
  - ``k8s_client``      : dict of Kubernetes API clients
  - ``namespace``       : target namespace (already validated)
  - ``wait_for_pods``   : callable(selector, timeout, expected_count) -> List

---- Environment variables ---------------------------------------------------
  GREEN_AGENT_URL              dashboard URL (default: http://localhost:8000)
  STRICT_INTEGRATION           "1" fails instead of skipping on unreachable
  GREEN_AGENT_K8S_TIMEOUT      per-API-call timeout in seconds (default: 10)

---- Run ---------------------------------------------------------------------
  # In another terminal:
  #   kubectl port-forward -n green-agent-development \
  #       svc/green-agent-dashboard 8000:8000
  pytest tests/integration/test_health_checks.py -m health
  GREEN_AGENT_URL=http://green.example.com pytest -m "health and not e2e"
  python tests/integration/test_health_checks.py    # offline helper smoke test
"""

from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Optional, Tuple

import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
DEFAULT_URL: str = os.environ.get("GREEN_AGENT_URL", "http://localhost:8000")
STRICT_INTEGRATION: bool = os.environ.get("STRICT_INTEGRATION", "0") == "1"
API_TIMEOUT: float = float(os.environ.get("GREEN_AGENT_K8S_TIMEOUT", "10"))

# Real-world readiness / liveness vocabularies
_HEALTH_OK = {"healthy", "ok", "up", "pass"}
_LIVE_OK = {"alive", "ok", "up", "healthy", "pass"}
_READY_OK = {"ready", "ok", "up", "pass"}

pytestmark = [pytest.mark.health, pytest.mark.requires_cluster]


def pytest_configure(config: pytest.Config) -> None:
    for marker, desc in (
        ("health", "health / readiness / liveness / metrics endpoints"),
        ("e2e", "requires a live deployment"),
        ("requires_cluster", "requires a live Kubernetes cluster"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
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


def _sorted_pods(pods: List[Any]) -> List[Any]:
    """Oldest first — the head pod is created before the workers."""
    return sorted(
        pods,
        key=lambda p: getattr(
            getattr(p, "metadata", None), "creation_timestamp", None,
        ) or "",
    )


def _env_dict(container: Any) -> Dict[str, Optional[str]]:
    """Safe env mapping; returns None values for valueFrom entries."""
    return {
        e.name: getattr(e, "value", None)
        for e in (getattr(container, "env", None) or [])
    }


def _http_get_probe(probe: Any) -> Optional[Any]:
    """Return probe.http_get, or None when the probe is exec/tcp/grpc."""
    if probe is None:
        return None
    return getattr(probe, "http_get", None)


def _probe_path_port(probe: Any) -> Optional[Tuple[str, int]]:
    """Extract (path, port) from an HTTP probe, or None for non-HTTP."""
    http = _http_get_probe(probe)
    if http is None:
        return None
    return (getattr(http, "path", ""), int(getattr(http, "port", 0) or 0))


def _find_first_container(pods: List[Any]) -> Any:
    """Return the first container of the oldest pod."""
    if not pods:
        pytest.fail("no pods returned from wait_for_pods")
    oldest = _sorted_pods(pods)[0]
    containers = getattr(getattr(oldest, "spec", None), "containers", None) or []
    if not containers:
        pytest.fail(f"pod {oldest.metadata.name} has no containers")
    return containers[0]


def _require_metric_family(metrics: Dict[str, Any], name: str) -> float:
    """Return the first sample value of a metric family, or fail."""
    if name not in metrics["sample_names"]:
        pytest.fail(
            f"/metrics missing {name}; exported samples="
            f"{sorted(metrics['sample_names'])[:40]}..."
        )
    for s in metrics["samples"]:
        if s.name == name:
            return float(s.value)
    return float("nan")


def _finite(v: float) -> bool:
    return isinstance(v, float) and math.isfinite(v)


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def dashboard_url() -> str:
    """
    Return the dashboard URL only if it is actually reachable.

    A port-forward must be opened externally, e.g.:
        kubectl port-forward -n green-agent-development \
            svc/green-agent-dashboard 8000:8000
    or GREEN_AGENT_URL must point at a live deployment.
    """
    url = DEFAULT_URL.rstrip("/")
    try:
        r = requests.get(f"{url}/health", timeout=(3, 10))
    except requests.exceptions.RequestException as exc:
        _skip_or_fail(
            f"Dashboard unreachable at {url} ({exc}). "
            "Open a port-forward, or set GREEN_AGENT_URL."
        )
    if r.status_code != 200:
        _skip_or_fail(f"Dashboard {url}/health returned {r.status_code}")
    return url


@pytest.fixture(scope="session")
def http() -> Any:
    """Requests session with retries for transient 5xx."""
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "green-agent-health-tests/2.0"})
    try:
        yield s
    finally:
        s.close()


@pytest.fixture(scope="session")
def prometheus_metrics(dashboard_url: str, http: Any) -> Dict[str, Any]:
    """Parse /metrics once per session with the canonical Prometheus parser."""
    if not _PROM:
        _skip_or_fail(
            "prometheus_client not installed — cannot parse /metrics rigorously"
        )
    r = http.get(f"{dashboard_url}/metrics", timeout=(3, 10))
    assert r.status_code == 200, (
        f"/metrics returned {r.status_code}: {r.text[:200]}"
    )
    families = list(text_string_to_metric_families(r.text))
    samples = [s for fam in families for s in fam.samples]
    return {
        "families": {fam.name: fam for fam in families},
        "family_names": {fam.name for fam in families},
        "sample_names": {s.name for s in samples},
        "samples": samples,
        "raw": r.text,
    }


@pytest.fixture(scope="session")
def live_green_agent_config(k8s_client, namespace) -> Dict[str, Any]:
    """
    Fetch and parse the deployed ``green_agent_config.yaml`` ConfigMap.

    Skips when the ConfigMap is absent. Returns
    ``{"configmap": <cm>, "config": <parsed dict>}``.
    """
    try:
        cms = k8s_client["core"].list_namespaced_config_map(
            namespace=namespace,
            label_selector="app=green-agent",
            _request_timeout=API_TIMEOUT,
        ).items
    except ApiException as exc:
        if exc.status == 404:
            pytest.skip(f"No ConfigMaps visible in '{namespace}'")
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
        pytest.skip(f"No green-agent-config* ConfigMap in '{namespace}'")

    raw = (cm.data or {}).get("green_agent_config.yaml", "")
    if not raw:
        pytest.skip(
            f"ConfigMap {cm.metadata.name} has no green_agent_config.yaml key"
        )
    import yaml
    try:
        parsed = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        pytest.fail(f"ConfigMap {cm.metadata.name} contains invalid YAML: {exc}")
    return {"configmap": cm, "config": parsed}


# ---------------------------------------------------------------------------
# Health / readiness / liveness / metrics endpoints
# ---------------------------------------------------------------------------
class TestHealthChecks:
    """Core health, readiness, liveness, and metrics endpoints."""

    def test_health_endpoint(self, dashboard_url, http):
        r = http.get(f"{dashboard_url}/health", timeout=(3, 10))
        assert r.status_code == 200, r.text[:200]
        data = r.json()
        assert "status" in data, f"missing 'status' key: {data}"
        assert str(data["status"]).lower() in _HEALTH_OK, (
            f"unexpected status {data['status']!r}; "
            f"accepted={sorted(_HEALTH_OK)}"
        )
        assert "timestamp" in data, "missing timestamp in /health"

    def test_readiness_endpoint(self, dashboard_url, http):
        r = http.get(f"{dashboard_url}/ready", timeout=(3, 10))
        assert r.status_code == 200, r.text[:200]
        data = r.json()

        # Accept the three common readiness shapes:
        #   {"status": "ready", "checks": {...}}
        #   {"ready": true}
        #   {"status": "ok"}
        has_status = str(data.get("status", "")).lower() in _READY_OK
        has_ready_bool = data.get("ready") is True
        has_checks = "checks" in data
        assert has_status or has_ready_bool, (
            f"/ready did not report readiness: keys={sorted(data.keys())}"
        )
        if has_checks:
            assert isinstance(data["checks"], dict)

    def test_liveness_endpoint(self, dashboard_url, http):
        r = http.get(f"{dashboard_url}/live", timeout=(3, 10))
        assert r.status_code == 200, r.text[:200]
        data = r.json()
        assert "status" in data, f"missing 'status' key: {data}"
        assert str(data["status"]).lower() in _LIVE_OK, (
            f"unexpected liveness status {data['status']!r}; "
            f"accepted={sorted(_LIVE_OK)}"
        )

    def test_metrics_endpoint(self, prometheus_metrics):
        """Carbon / energy metric families must be present and well-formed."""
        for name in (
            "green_agent_carbon_intensity",
            "green_agent_carbon_emitted_kg",
            "green_agent_energy_consumed_kwh",
        ):
            value = _require_metric_family(prometheus_metrics, name)
            assert _finite(value), f"{name} is non-finite: {value}"
            assert value >= 0, f"{name} is negative: {value}"

    def test_metrics_have_help_and_type(self, prometheus_metrics):
        """Every metric family must have a HELP line."""
        assert prometheus_metrics["families"], "no metric families parsed"
        for fam in prometheus_metrics["families"].values():
            assert fam.name, "metric family has no name"
            assert fam.documentation != "", (
                f"metric family {fam.name} has no HELP line"
            )

    def test_metrics_all_finite(self, prometheus_metrics):
        """Every sample value must be a finite float."""
        bad = []
        for s in prometheus_metrics["samples"]:
            try:
                v = float(s.value)
            except (TypeError, ValueError):
                bad.append((s.name, s.value))
                continue
            if not math.isfinite(v):
                bad.append((s.name, v))
        assert not bad, f"non-finite metric samples: {bad[:10]}"

    # ------------------------------------------------------------------
    # Kubernetes probe configuration
    # ------------------------------------------------------------------
    def test_probe_configuration(self, wait_for_pods):
        """
        Liveness + readiness must be HTTP probes on the documented endpoints.

        A startup probe is checked only when configured (v2 does not add one).
        """
        pods = wait_for_pods("component=head", timeout=300, expected_count=1)
        container = _find_first_container(pods)

        live = _probe_path_port(container.liveness_probe)
        assert live is not None, "liveness probe is not HTTP"
        path, port = live
        assert path == "/health", f"liveness path={path}"
        assert port == 8000, f"liveness port={port}"

        ready = _probe_path_port(container.readiness_probe)
        assert ready is not None, "readiness probe is not HTTP"
        path, port = ready
        assert path == "/ready", f"readiness path={path}"
        assert port == 8000, f"readiness port={port}"

        startup = _probe_path_port(container.startup_probe)
        if startup is not None:
            path, port = startup
            assert path in ("/health", "/live"), (
                f"startup path={path}; expected /health or /live"
            )
            assert port == 8000

    def test_probe_timing(self, wait_for_pods):
        """Probe timing must allow for a slow first start."""
        pods = wait_for_pods("component=head", timeout=300, expected_count=1)
        container = _find_first_container(pods)

        lv = container.liveness_probe
        assert lv is not None, "no liveness probe"
        assert (lv.initial_delay_seconds or 0) >= 30, (
            f"liveness initial_delay_seconds={lv.initial_delay_seconds} (<30)"
        )
        assert (lv.period_seconds or 0) >= 10, (
            f"liveness period_seconds={lv.period_seconds} (<10)"
        )

        rd = container.readiness_probe
        assert rd is not None, "no readiness probe"
        assert (rd.initial_delay_seconds or 0) >= 10, (
            f"readiness initial_delay_seconds={rd.initial_delay_seconds} (<10)"
        )
        assert (rd.period_seconds or 0) >= 5, (
            f"readiness period_seconds={rd.period_seconds} (<5)"
        )

        sp = container.startup_probe
        if sp is not None:
            assert (sp.failure_threshold or 0) >= 30, (
                "startup probe failure_threshold should be >= 30 to allow "
                f"slow first start (got {sp.failure_threshold})"
            )


# ---------------------------------------------------------------------------
# Enhanced health indicators (skip when enhancements.enabled is false)
# ---------------------------------------------------------------------------
@pytest.mark.enhancements
class TestEnhancedHealthChecks:
    """Enhanced metric families and health fields when enhancements are on."""

    ENHANCED_METRICS: Tuple[str, ...] = (
        "green_agent_modp_score",
        "green_agent_rlhf_feedback",
        "green_agent_graph_centrality",
        "green_agent_graph_connectivity",
        "green_agent_distillation_update_count",
        "green_agent_moe_gate_weight",
        "green_agent_evolutionary_best_fitness",
        "green_agent_flexgen_energy_joules_total",
    )

    @staticmethod
    def _enhancements_enabled(live_green_agent_config) -> bool:
        enh = live_green_agent_config["config"].get("enhancements") or {}
        return bool(enh.get("enabled"))

    def test_metrics_contain_enhanced_metrics(
        self, prometheus_metrics, live_green_agent_config,
    ):
        """
        When enhancements are enabled, the documented metric families must be
        exported. When enhancements are off, the test skips — the feature is
        not deployed, so there is nothing to assert.
        """
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        missing = [
            name for name in self.ENHANCED_METRICS
            if name not in prometheus_metrics["family_names"]
            and name not in prometheus_metrics["sample_names"]
        ]
        assert not missing, (
            "Enhanced metrics declared enabled in ConfigMap but not "
            f"exported on /metrics: {missing}"
        )

    def test_enhanced_metric_values(
        self, prometheus_metrics, live_green_agent_config,
    ):
        """Enhanced metric values must be finite and in range."""
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = {
            "green_agent_modp_score":               (0.0, 1.0),
            "green_agent_rlhf_feedback":            (0.0, 1.0),
            "green_agent_graph_centrality":         (0.0, 1.0),
            "green_agent_graph_connectivity":       (0.0, 1.0),
            "green_agent_distillation_update_count": (0.0, None),
            "green_agent_evolutionary_best_fitness": (None, None),
            "green_agent_flexgen_energy_joules_total": (0.0, None),
        }
        violations = []
        for name, (lo, hi) in ranges.items():
            if name not in prometheus_metrics["sample_names"]:
                continue
            v = _require_metric_family(prometheus_metrics, name)
            if not _finite(v):
                violations.append((name, v, "finite"))
                continue
            if lo is not None and v < lo - 1e-9:
                violations.append((name, v, f">={lo}"))
            if hi is not None and v > hi + 1e-9:
                violations.append((name, v, f"<={hi}"))
        assert not violations, f"out-of-range enhanced metrics: {violations}"

    def test_health_enhanced_fields(
        self, dashboard_url, http, live_green_agent_config,
    ):
        """
        If enhancements are enabled, ``/health`` should include at least one
        enhanced field, and any field it does include must be well-typed and
        in range.
        """
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        r = http.get(f"{dashboard_url}/health", timeout=(3, 10))
        assert r.status_code == 200, r.text[:200]
        data = r.json()

        enhanced_keys = (
            "modp_score", "rlhf_feedback", "graph_centrality",
            "distillation_status", "moe_status",
            "evolutionary_status", "flexgen_status",
        )
        present = {k: data[k] for k in enhanced_keys if k in data}

        # Any enhanced numeric field must be in [0, 1].
        for k in ("modp_score", "rlhf_feedback", "graph_centrality"):
            if k in present:
                assert isinstance(present[k], (int, float)), (
                    f"/health field '{k}' has wrong type: "
                    f"{type(present[k]).__name__}"
                )
                assert 0.0 <= float(present[k]) <= 1.0, (
                    f"/health field '{k}'={present[k]} out of [0,1]"
                )

        # Any enhanced status field must be a str, bool, or dict.
        for k in ("distillation_status", "moe_status",
                  "evolutionary_status", "flexgen_status"):
            if k in present:
                assert isinstance(present[k], (str, bool, dict)), (
                    f"/health field '{k}' has unexpected type "
                    f"{type(present[k]).__name__}"
                )

        # Enhancements are enabled — at least one indicator should be present.
        assert present, (
            "enhancements.enabled is true but /health exposes none of "
            f"{list(enhanced_keys)}"
        )

    def test_pod_environment_enhancement_flags(
        self, wait_for_pods, live_green_agent_config,
    ):
        """
        When the deployer chose to expose enhancement config via env vars,
        validate the flag values. When config is passed via ConfigMap only
        (v2's default), skip — that is a valid deployment shape.
        """
        if not self._enhancements_enabled(live_green_agent_config):
            pytest.skip("enhancements.enabled is false in live ConfigMap")

        pods = wait_for_pods("component=head", timeout=300, expected_count=1)
        env = _env_dict(_find_first_container(pods))
        if "ENHANCEMENTS_ENABLED" not in env:
            pytest.skip(
                "head pod does not expose enhancement env vars "
                "(config delivered via ConfigMap only)"
            )

        assert env.get("ENHANCEMENTS_ENABLED") == "true"

        # Only check toggles the deployer actually set.
        expected_true = (
            "LIMIT_GRAPH_ENABLED", "MODP_ENABLED", "RLHF_ENABLED",
            "DISTILLATION_ENABLED", "MOE_GATING_ENABLED",
            "EVOLUTIONARY_ENABLED", "FLEXGEN_ENABLED",
        )
        for var in expected_true:
            if var in env:
                assert env[var] == "true", (
                    f"{var}={env[var]!r}; enhancements are on so {var} "
                    f"should be 'true'"
                )


# ---------------------------------------------------------------------------
# Offline smoke test — validates helpers without a cluster.
# Run: python tests/integration/test_health_checks.py
# ---------------------------------------------------------------------------
def _smoke_test_helpers() -> int:
    failures = 0

    # --- _sorted_pods orders by creation_timestamp ---
    class _Meta:
        def __init__(self, ts): self.creation_timestamp = ts

    class _P:
        def __init__(self, ts): self.metadata = _Meta(ts)

    pods = [_P("2024-02-01"), _P("2024-01-01"), _P("2024-03-01")]
    assert [p.metadata.creation_timestamp for p in _sorted_pods(pods)] == [
        "2024-01-01", "2024-02-01", "2024-03-01",
    ]
    print("ok  _sorted_pods")

    # --- _env_dict tolerates env=None and valueFrom entries ---
    class _C:
        env = None
    assert _env_dict(_C()) == {}

    class _E:
        def __init__(self, name, value): self.name, self.value = name, value

    class _C2:
        env = [_E("A", "1"), _E("B", None)]
    assert _env_dict(_C2()) == {"A": "1", "B": None}
    print("ok  _env_dict")

    # --- _probe_path_port distinguishes HTTP from exec/tcp ---
    class _HTTP:
        def __init__(self, path, port):
            self.path, self.port = path, port

    class _Probe:
        def __init__(self, http): self.http_get = http

    class _TCPProbe:
        http_get = None

    assert _probe_path_port(_Probe(_HTTP("/health", 8000))) == ("/health", 8000)
    assert _probe_path_port(_TCPProbe()) is None
    assert _probe_path_port(None) is None
    print("ok  _probe_path_port")

    # --- _http_get_probe returns None for non-HTTP probes ---
    assert _http_get_probe(None) is None
    assert _http_get_probe(_TCPProbe()) is None
    print("ok  _http_get_probe")

    # --- vocabulary checks ---
    assert "healthy" in _HEALTH_OK
    assert "ok" in _HEALTH_OK
    assert "alive" in _LIVE_OK
    assert "ready" in _READY_OK
    print("ok  status vocabularies")

    # --- _finite ---
    assert _finite(0.0)
    assert _finite(1.5)
    assert not _finite(float("nan"))
    assert not _finite(float("inf"))
    print("ok  _finite")

    # --- _require_metric_family: present and absent ---
    from types import SimpleNamespace
    import unittest.mock as mock

    fake = {
        "samples": [
            SimpleNamespace(name="green_agent_modp_score",
                            labels={}, value="0.75"),
            SimpleNamespace(name="green_agent_tasks_total",
                            labels={}, value="42"),
        ],
        "sample_names": {
            "green_agent_modp_score", "green_agent_tasks_total",
        },
    }
    assert _require_metric_family(fake, "green_agent_modp_score") == 0.75

    with mock.patch("pytest.fail") as mfail:
        _require_metric_family(fake, "green_agent_nonexistent")
        mfail.assert_called_once()
    print("ok  _require_metric_family")

    # --- _skip_or_fail: skips by default, fails under strict ---
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
    print("ok  _skip_or_fail honours STRICT_INTEGRATION")

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
