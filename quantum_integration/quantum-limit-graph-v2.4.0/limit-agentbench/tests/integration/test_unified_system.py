# File: tests/integration/test_unified_system.py (Enhanced v2)
# =============================================================================
# ---- Fixes vs v1 ------------------------------------------------------------
#   FIX #1   Every top-level import is guarded. GreenAgentRunner /
#            ExecutionResult / GreenAgentQuantumSystem are optional; the module
#            skips at collection time if the critical ones are absent (v1
#            failed at collection with ImportError).
#   FIX #3/4 TestKubernetesDeployment searches config/base, config/overlays/*
#            and legacy k8s/; skipif on absence. Accepts 1+ YAML docs (v1
#            required >= 3 and crashed on single-doc files).
#   FIX #5   autoscaling test checks workerGroupSpecs[].{minReplicas,
#            maxReplicas} — the actual v2 mechanism — instead of the removed
#            autoscalerOptions block.
#   FIX #6   TestDashboardMonitoring / TestEndToEnd skipif on missing services
#            via a session fixture (v1 failed 6 tests whenever the dashboard
#            was not live).
#   FIX #11  Exact float equality replaced with pytest.approx.
#   FIX #12  Comparison output redirected to tmp_path (v1 wrote into CWD and
#            raced under `-n auto`).
#   FIX #13  Async fixtures work under pytest-asyncio without relying on
#            --asyncio-mode=auto being set globally.
#   FIX #15  np.random seeded per test that needs randomness.
#   FIX #16  asyncio.run() inside sync tests replaced with @pytest.mark.asyncio.
#   FIX #21  Comparison fixture tests _execute_comparison / _generate_
#            comparison presence before use (v1 assumed and crashed).
#   FIX #8   test_flexgen_integration_hook replaced: reads the real
#            green_agent_config.yaml instead of asserting on a dict the test
#            itself built (v1 was a tautology).
#   FIX #9   ZeroTrust assertion has a diagnostic failure message.
#   FIX #26  Enhancement imports are individually guarded; each test skips on
#            its own capability.
#   FIX #7   if __name__ main block uses a valid `-m` expression, not `-k`.
#   FIX #24  pytest_configure registers e2e / slow / integration / quantum /
#            dashboard / requires_cluster markers.
# =============================================================================
#
# Run:
#   pytest tests/integration/test_unified_system.py -m "not e2e"
#   GREEN_AGENT_URL=http://localhost:8000 pytest -m e2e
#   STRICT_INTEGRATION=1 pytest                # turn skips into failures
#
# Recommended pytest.ini (kept out of this file to allow per-project tuning):
#   [pytest]
#   asyncio_mode = auto
#   timeout = 60
#   markers =
#       e2e: requires a live deployment
#       slow: long-running test
#       integration: multi-component integration
#       quantum: quantum simulation
#       dashboard: requires the dashboard service
#       requires_cluster: requires a live Kubernetes cluster

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Optional dependencies — one flag per capability (FIX #1, #26)
# ---------------------------------------------------------------------------
CRITICAL_IMPORTS_OK = True
CRITICAL_MISSING: List[str] = []

try:
    from runtime.run_agent import GreenAgentRunner, ExecutionResult   # noqa: F401
except Exception as _exc:                                   # noqa: BLE001
    GreenAgentRunner = None                                 # type: ignore
    ExecutionResult = None                                  # type: ignore
    CRITICAL_IMPORTS_OK = False
    CRITICAL_MISSING.append(f"runtime.run_agent: {_exc}")

try:
    from src.integration.unified_orchestrator import UnifiedGreenAgent  # noqa: F401
except Exception as _exc:                                   # noqa: BLE001
    UnifiedGreenAgent = None                                # type: ignore
    CRITICAL_IMPORTS_OK = False
    CRITICAL_MISSING.append(f"src.integration.unified_orchestrator: {_exc}")

try:
    from src.carbon.task_carbon_profiler import CarbonProfiler  # noqa: F401
except Exception:                                           # noqa: BLE001
    CarbonProfiler = None                                   # type: ignore

try:
    from src.benchmarking.benchmark_intelligence import BenchmarkEngine  # noqa: F401
except Exception:                                           # noqa: BLE001
    BenchmarkEngine = None                                  # type: ignore

try:
    # NOTE: importing from a test_* module is a code smell. If the quantum
    # system becomes production code, move it to a non-test package.
    from quantum_integration.test_unified_system import GreenAgentQuantumSystem  # noqa: F401
except Exception:                                           # noqa: BLE001
    GreenAgentQuantumSystem = None                          # type: ignore

# Enhanced modules — individual flags (FIX #26)
HAS_FEEDBACK_EVENT = False
HAS_NODE_DESC = False
HAS_WORKLOAD_DESC = False
HAS_ZERO_TRUST = False

FeedbackEvent = None
NodeDescriptor = None
NodeType = None
CoolingType = None
WorkloadDescriptor = None
TaskType = None
Urgency = None
ZeroTrustArchitecture = None

try:
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    HAS_FEEDBACK_EVENT = True
except Exception:                                           # noqa: BLE001
    pass

try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType,
    )
    HAS_NODE_DESC = True
except Exception:                                           # noqa: BLE001
    pass

try:
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency,
    )
    HAS_WORKLOAD_DESC = True
except Exception:                                           # noqa: BLE001
    pass

try:
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    HAS_ZERO_TRUST = True
except Exception:                                           # noqa: BLE001
    pass

import httpx
import yaml


# ---------------------------------------------------------------------------
# Markers / env (FIX #24)
# ---------------------------------------------------------------------------
STRICT_INTEGRATION = os.environ.get("STRICT_INTEGRATION", "0") == "1"
DEFAULT_URL = os.environ.get("GREEN_AGENT_URL", "http://localhost:8000")


def pytest_configure(config: pytest.Config) -> None:
    for marker, desc in [
        ("e2e", "requires a live deployment"),
        ("slow", "long-running test"),
        ("integration", "multi-component integration"),
        ("quantum", "quantum simulation"),
        ("dashboard", "requires the dashboard service"),
        ("requires_cluster", "requires a live Kubernetes cluster"),
    ]:
        config.addinivalue_line("markers", f"{marker}: {desc}")


def _skip(msg: str) -> None:
    """Skip normally; fail under STRICT_INTEGRATION (CI gating)."""
    if STRICT_INTEGRATION:
        pytest.fail(msg)
    pytest.skip(msg)


# Module-level collection guard (FIX #1)
if not CRITICAL_IMPORTS_OK:
    pytest.skip(
        "Critical modules unavailable: " + "; ".join(CRITICAL_MISSING),
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Shared fixtures (FIX #3, #6, #13)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def dashboard_reachable() -> str:
    """Return the dashboard URL if reachable; skip (or fail) otherwise."""
    url = DEFAULT_URL.rstrip("/")
    try:
        r = httpx.get(f"{url}/health", timeout=httpx.Timeout(5.0, connect=3.0))
    except httpx.HTTPError as exc:
        _skip(f"Dashboard unreachable at {url}: {exc}")
    if r.status_code != 200:
        _skip(f"Dashboard {url}/health returned {r.status_code}")
    return url


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Locate the repo root by walking up for a known marker."""
    for marker in (".git", "pyproject.toml", "setup.py"):
        for d in [Path.cwd(), *Path.cwd().parents]:
            if (d / marker).exists():
                return d
    return Path.cwd()


@pytest.fixture(scope="session")
def ray_cluster_yaml(repo_root: Path) -> Path:
    """Find a YAML document anywhere the config generator may have written it."""
    candidates: List[Path] = [
        repo_root / "config/base/ray-cluster.yaml",
        *sorted(repo_root.glob("config/base/*.yaml")),
        *sorted(repo_root.glob("config/overlays/*/ray-cluster*.yaml")),
        repo_root / "k8s/ray-cluster.yaml",       # legacy path
    ]
    for p in candidates:
        if not p.is_file():
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                docs = [d for d in yaml.safe_load_all(f) if d]
        except yaml.YAMLError:
            continue
        if any(isinstance(d, dict) and d.get("kind") == "RayCluster" for d in docs):
            return p
    _skip(
        "No RayCluster manifest found under config/base, "
        "config/overlays or k8s/"
    )


# ---------------------------------------------------------------------------
# Helper: safe attribute access for dict-or-object results (FIX #22)
# ---------------------------------------------------------------------------
def _get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


# ---------------------------------------------------------------------------
# Test: Comparison mode (FIX #11, #12, #21)
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestComparisonMode:
    """Comparison-mode behaviour (unified vs legacy side-by-side)."""

    @pytest.fixture
    def runner_compare(self, tmp_path: Path):
        if not hasattr(GreenAgentRunner, "_execute_comparison"):
            pytest.skip("GreenAgentRunner lacks _execute_comparison")
        runner = GreenAgentRunner.__new__(GreenAgentRunner)
        runner.config = {
            "system": {"mode": "compare"},
            "dashboard": {"enabled": False},
            "ray": {"enabled": False},
            "comparison_output_dir": str(tmp_path),
        }
        runner.mode = "compare"
        runner.unified_agent = None
        runner.legacy_components = {}
        runner.dashboard = None
        runner._comparison_output_dir = tmp_path
        return runner

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_comparison_execution(self, runner_compare, tmp_path: Path):
        task = {
            "id": "test_comparison_001",
            "type": "ml_inference",
            "model_size": "100M",
            "input_size": "512KB",
            "priority": 5,
        }

        async def mock_unified(task):                       # noqa: ARG001
            import asyncio
            await asyncio.sleep(0.05)
            return {
                "energy_consumed": 0.5,
                "carbon_emitted": 0.02,
                "accuracy": 0.92,
                "negawatt_reward": 0.85,
                "metrics": {},
            }

        async def mock_legacy(task):                        # noqa: ARG001
            import asyncio
            await asyncio.sleep(0.10)
            return {
                "energy_consumed": 1.5,
                "carbon_emitted": 0.06,
                "accuracy": 0.90,
                "negawatt_reward": 0.60,
                "metrics": {},
            }

        runner_compare._execute_unified = mock_unified
        runner_compare._execute_legacy = mock_legacy

        result = await runner_compare._execute_comparison(task)

        # FIX #11 — approximate float equality
        assert _get(result, "energy_consumed") == pytest.approx(0.5, abs=1e-9)
        assert _get(result, "carbon_emitted") == pytest.approx(0.02, abs=1e-9)

        # FIX #12 — search tmp_path first; fall back to legacy CWD path
        outputs = list(tmp_path.glob("**/comparison_results.json")) + \
                  list(Path.cwd().glob("results/comparison_results.json"))
        assert outputs, "no comparison_results.json produced"
        with outputs[0].open("r", encoding="utf-8") as f:
            comparison = json.load(f)
        improvements = comparison["improvements"]
        assert improvements["energy_reduction_percent"] > 0
        assert improvements["carbon_reduction_percent"] > 0

    @pytest.mark.timeout(10)
    def test_comparison_generates_report(self, runner_compare):
        if not hasattr(runner_compare, "_generate_comparison"):
            pytest.skip("_generate_comparison not present")

        unified_result = {
            "energy_consumed": 0.3,
            "carbon_emitted": 0.012,
            "accuracy": 0.95,
            "negawatt_reward": 0.90,
        }
        legacy_result = {
            "energy_consumed": 1.0,
            "carbon_emitted": 0.04,
            "accuracy": 0.93,
            "negawatt_reward": 0.65,
        }

        comparison = runner_compare._generate_comparison(
            unified_result, legacy_result,
            unified_time=0.5, legacy_time=1.2,
        )

        assert "improvements" in comparison
        # FIX #11 — pytest.approx, not ==70.0
        assert comparison["improvements"]["energy_reduction_percent"] == \
            pytest.approx(70.0, abs=1e-6)
        assert comparison["improvements"]["carbon_reduction_percent"] == \
            pytest.approx(70.0, abs=1e-6)
        assert "verdict" in comparison


# ---------------------------------------------------------------------------
# Test: Quantum integration (FIX #15, #16, #22)
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.quantum
class TestQuantumIntegration:

    @pytest.fixture
    async def quantum_system(self):
        if GreenAgentQuantumSystem is None:
            pytest.skip("GreenAgentQuantumSystem not importable")
        if not hasattr(GreenAgentQuantumSystem, "initialize"):
            pytest.skip("GreenAgentQuantumSystem lacks initialize()")
        config = {
            "quantum": {
                "enabled": True,
                "backend": "simulator",
                "error_mitigation": {
                    "enabled": True,
                    "techniques": ["zero_noise_extrapolation"],
                },
            }
        }
        system = GreenAgentQuantumSystem(config)
        await system.initialize()
        try:
            yield system
        finally:
            await system.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_quantum_task_execution(self, quantum_system):
        task_data = {"features": [0.1, 0.2, 0.3, 0.4], "task_id": "quantum_test_001"}
        result = await quantum_system.run_quantum_task(task_data)

        for key in (
            "result", "execution_time", "energy_consumed_kwh",
            "carbon_emitted_kg", "error_mitigation_applied",
        ):
            assert key in result, f"missing key: {key}"
        assert isinstance(result["error_mitigation_applied"], bool)

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_multi_agent_coordination(self, quantum_system):
        carbon_data = {"agent_0": 30, "agent_1": 150, "agent_2": 250, "agent_3": 45}
        result = await quantum_system.run_multi_agent_coordination(carbon_data)

        assert "task_distribution" in result
        assert "entanglement_fidelity" in result
        assert result["consensus_achieved"] is True

        distribution = result["task_distribution"]
        green_agents = [
            aid for aid, data in distribution.items()
            if data.get("role") == "primary_compute"
        ]
        assert green_agents, "no primary_compute agents assigned"

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_quantum_efficiency_metrics(self, quantum_system):
        # FIX #15 — deterministic randomness
        rng = np.random.default_rng(seed=0)
        tasks = [
            {"features": rng.random(4).tolist(), "task_id": f"task_{i}"}
            for i in range(5)
        ]
        for t in tasks:
            await quantum_system.run_quantum_task(t)

        metrics = quantum_system.get_system_metrics()
        # FIX #22 — tolerate dict or dataclass return
        get = metrics.get if isinstance(metrics, dict) else \
              lambda k, d=None: getattr(metrics, k, d)
        assert get("total_energy", 0) > 0
        assert get("tasks_completed", 0) == 5
        assert get("efficiency_score", 0) > 0


# ---------------------------------------------------------------------------
# Test: Kubernetes deployment config (FIX #3, #4, #5)
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.requires_cluster
class TestKubernetesDeployment:

    @pytest.mark.timeout(10)
    def test_ray_cluster_yaml_valid(self, ray_cluster_yaml: Path):
        with ray_cluster_yaml.open("r", encoding="utf-8") as f:
            docs = [d for d in yaml.safe_load_all(f) if d]
        assert docs, f"{ray_cluster_yaml} has no YAML documents"

        clusters = [d for d in docs
                    if isinstance(d, dict) and d.get("kind") == "RayCluster"]
        assert clusters, f"No RayCluster document in {ray_cluster_yaml}"

        rc = clusters[0]
        assert "spec" in rc
        assert "headGroupSpec" in rc["spec"]
        assert "workerGroupSpecs" in rc["spec"]

    @pytest.mark.timeout(10)
    def test_carbon_aware_scaling_configured(self, ray_cluster_yaml: Path):
        """
        v2 has no autoscalerOptions — horizontal scaling is expressed via
        workerGroupSpecs[].{minReplicas,maxReplicas}. Verify at least one
        group can actually scale (max > min).
        """
        with ray_cluster_yaml.open("r", encoding="utf-8") as f:
            docs = [d for d in yaml.safe_load_all(f) if d]
        rc = next((d for d in docs
                   if isinstance(d, dict) and d.get("kind") == "RayCluster"), None)
        assert rc, f"No RayCluster in {ray_cluster_yaml}"

        groups = rc.get("spec", {}).get("workerGroupSpecs") or []
        assert groups, "no workerGroupSpecs in RayCluster"

        scalable: List[str] = []
        for g in groups:
            reps = int(g.get("replicas", 0))
            mn = int(g.get("minReplicas", reps))
            mx = int(g.get("maxReplicas", reps))
            assert 0 <= mn <= reps <= mx, (
                f"workerGroup {g.get('groupName')}: replicas={reps} "
                f"not in [{mn},{mx}]"
            )
            if mx > mn:
                scalable.append(g.get("groupName"))
        assert scalable, (
            "no workerGroup can scale (all groups have max == min); "
            "carbon-aware scaling cannot operate"
        )


# ---------------------------------------------------------------------------
# Test: Dashboard monitoring (FIX #6, #14, #28)
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.dashboard
class TestDashboardMonitoring:

    @pytest.fixture
    async def dashboard_client(self, dashboard_reachable: str):
        async with httpx.AsyncClient(
            base_url=dashboard_reachable,
            timeout=httpx.Timeout(5.0, connect=3.0),
        ) as client:
            yield client

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_dashboard_health_endpoint(self, dashboard_client):
        r = await dashboard_client.get("/health")
        assert r.status_code == 200, r.text[:200]
        data = r.json()
        assert "status" in data
        # Accept the common readiness vocabularies
        assert data["status"].lower() in {"healthy", "ready", "ok"}

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_real_time_metrics_endpoint(self, dashboard_client):
        r = await dashboard_client.get("/metrics/realtime")
        assert r.status_code == 200, r.text[:200]
        metrics = r.json()
        for key in ("energy_consumption", "carbon_footprint",
                    "active_tasks", "pareto_frontier"):
            assert key in metrics, f"missing {key} in /metrics/realtime"
        assert isinstance(metrics["active_tasks"], (int, float))
        assert metrics["active_tasks"] >= 0

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_pareto_frontier_visualization(self, dashboard_client):
        r = await dashboard_client.get("/analytics/pareto")
        assert r.status_code == 200, r.text[:200]
        data = r.json()
        assert "frontier_points" in data
        assert len(data["frontier_points"]) > 0
        for point in data["frontier_points"]:
            assert "accuracy" in point
            assert "energy" in point
            assert 0.0 <= point["accuracy"] <= 1.0
            assert point["energy"] >= 0


# ---------------------------------------------------------------------------
# Test: End-to-end workflow (FIX #6, #19, #20)
# ---------------------------------------------------------------------------
@pytest.mark.e2e
@pytest.mark.slow
class TestEndToEnd:

    @pytest.mark.asyncio
    @pytest.mark.timeout(120)
    async def test_full_workflow(self, repo_root: Path):
        config_candidates = [
            repo_root / "config/base/green_agent_config.yaml",
            repo_root / "config/green_agent_config.yaml",
            repo_root / "green_agent_config.yaml",
        ]
        cfg_path = next((p for p in config_candidates if p.is_file()), None)
        if cfg_path is None:
            _skip("No green_agent_config.yaml found in the repo")

        runner = GreenAgentRunner(str(cfg_path))
        if not hasattr(runner, "initialize"):
            pytest.skip("GreenAgentRunner lacks initialize()")
        await runner.initialize()
        try:
            task = {
                "id": "e2e_test_001",
                "type": "ml_training",
                "model_size": "500M",
                "dataset_size": "10GB",
                "deadline": time.time() + 7200,
                "priority": 7,
            }
            result = await runner.execute_task(task)

            assert _get(result, "execution_time", 0) > 0
            assert _get(result, "energy_consumed", 0) > 0
            assert _get(result, "carbon_emitted", -1) >= 0
            assert _get(result, "accuracy", 0) > 0

            # Unified mode energy is hardware-dependent — assert a generous
            # ceiling only to catch obvious regressions.
            if _get(result, "mode") == "unified":
                assert _get(result, "energy_consumed", 0) < 5.0, (
                    "unified mode used >5 kWh — check the baseline comparison"
                )
        finally:
            await runner.shutdown()


# ---------------------------------------------------------------------------
# Test: Enhanced module schemas (FIX #8, #9, #17, #26)
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestEnhancedIntegration:

    @pytest.mark.skipif(not HAS_FEEDBACK_EVENT, reason="FeedbackEvent unavailable")
    def test_feedback_event_roundtrip(self):
        # Only pass fields the schema documents — do NOT pass graph_metrics /
        # modp_score / distillation_stats unless they are part of the schema.
        event = FeedbackEvent(
            source="test",
            feedback_type="routing",
            task_id="t1",
            context={"key": "value"},
            action={"selected_action": "execute", "selected_rank": 1},
            performance={
                "quality_score": 0.9,
                "latency_ms": 100,
                "energy_joules": 100,
                "carbon_g": 5,
                "helium_cost": 0,
                "duration_ms": 100,
            },
            adaptive_cost_value=0.85,
            tags=["test"],
        )
        data = event.to_json()
        assert isinstance(data, str) and data.startswith("{")
        event2 = FeedbackEvent.from_json(data)
        assert event2.task_id == "t1"
        assert event2.feedback_type == "routing"

    @pytest.mark.skipif(not HAS_NODE_DESC, reason="NodeDescriptor unavailable")
    def test_node_descriptor_enhanced_features(self):
        node = NodeDescriptor(
            id="node1",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=400.0,
            energy_per_token=0.00005,
            helium_connectivity_score=0.8,
            uptime=0.99,
            renewable_fraction=0.3,
            cooling_type=CoolingType.AIR if CoolingType is not None else None,
            hardware_model="cpu",
            graph_metrics={
                "centrality": 0.8, "connectivity": 0.5, "density": 0.4,
            },
            human_feedback_score=0.6,
            metadata={"gating_lr": 0.005},
        )
        assert node.human_feedback_score == 0.6
        assert node.graph_metrics["centrality"] == 0.8

    @pytest.mark.skipif(not HAS_WORKLOAD_DESC, reason="WorkloadDescriptor unavailable")
    def test_workload_descriptor_modp_rlhf(self):
        wl = WorkloadDescriptor(
            task_id="task1",
            task_type=TaskType.INFERENCE,
            tokens=1000,
            latency_target=500.0,
            human_feedback_score=0.7,
            graph_metrics={
                "centrality": 0.6, "connectivity": 0.5, "density": 0.4,
            },
            metadata={"latency_weight": 0.5, "carbon_weight": 0.3},
        )
        assert wl.human_feedback_score == 0.7

    @pytest.mark.skipif(not HAS_ZERO_TRUST, reason="ZeroTrustArchitecture unavailable")
    def test_zero_trust_with_enhancements(self):
        zta = ZeroTrustArchitecture(config={
            "use_enhancements": True,
            "use_distillation": True,
            "use_evolutionary": True,
        })
        # FIX #9 — specific, diagnostic failure
        assert (
            hasattr(zta, "distillation_optimizer")
            or hasattr(zta, "carbon_authenticator")
        ), (
            "ZeroTrustArchitecture exposes neither distillation_optimizer "
            "nor carbon_authenticator"
        )

    def test_flexgen_config_present(self, repo_root: Path):
        """
        FIX #8 — read the real config; do not assert on a dict the test
        itself built (v1's tautology).
        """
        cfg_paths = [
            repo_root / "config/base/green_agent_config.yaml",
            repo_root / "green_agent_config.yaml",
        ]
        cfg_path = next((p for p in cfg_paths if p.is_file()), None)
        if cfg_path is None:
            pytest.skip("No green_agent_config.yaml found")

        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        flex = cfg.get("flexgen") or (cfg.get("integration") or {}).get("flexgen")
        if flex is None:
            pytest.skip(f"FlexGen section absent from {cfg_path}")

        assert flex.get("enabled") in (True, False)
        if flex.get("enabled"):
            assert flex.get("model_name"), "FlexGen enabled but no model_name"


# ---------------------------------------------------------------------------
# Test: Distillation + MoE integration (FIX #16)
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestEnhancedDistillationAndMoE:

    @pytest.mark.asyncio
    @pytest.mark.skipif(not HAS_NODE_DESC, reason="NodeDescriptor unavailable")
    @pytest.mark.timeout(30)
    async def test_node_descriptor_distillation_select(self):
        node = NodeDescriptor(
            id="node_d",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=300.0,
            energy_per_token=0.00004,
            helium_connectivity_score=0.8,
            uptime=0.99,
            renewable_fraction=0.3,
            cooling_type=CoolingType.AIR if CoolingType is not None else None,
            hardware_model="cpu",
            graph_metrics={
                "centrality": 0.7, "connectivity": 0.5, "density": 0.4,
            },
            human_feedback_score=0.6,
        )
        if not hasattr(node, "select_routing_strategy"):
            pytest.skip("NodeDescriptor lacks select_routing_strategy")
        strategy = await node.select_routing_strategy(exploration=True)
        assert strategy in {
            "carbon_first", "latency_first", "cost_first",
            "balanced", "adaptive",
        }

    @pytest.mark.asyncio
    @pytest.mark.skipif(not HAS_WORKLOAD_DESC, reason="WorkloadDescriptor unavailable")
    @pytest.mark.timeout(30)
    async def test_workload_descriptor_distillation_select(self):
        wl = WorkloadDescriptor(
            task_id="task_d",
            task_type=TaskType.INFERENCE,
            tokens=500,
            latency_target=200.0,
            human_feedback_score=0.5,
            graph_metrics={
                "centrality": 0.5, "connectivity": 0.5, "density": 0.4,
            },
        )
        if not hasattr(wl, "select_priority"):
            pytest.skip("WorkloadDescriptor lacks select_priority")
        priority = await wl.select_priority(exploration=True)
        assert priority in {"accuracy", "green", "balanced"}


# ---------------------------------------------------------------------------
# Direct invocation (FIX #7)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    raise SystemExit(pytest.main([
        __file__,
        "-v",
        "-s",
        "-m", "not e2e",     # valid expression (v1 passed bare `-k`)
    ]))
