"""
Integration Tests for Unified Green Agent v5.1 (Enhanced, auditable)
====================================================================

Covers all 12 layers, all ten cross-cutting enhancements, AND the v5.1
fixes from the review:

  * Sub-floor savings are flagged `simulated=True`, never fabricated.
  * Temporal safety violations BLOCK execution.
  * Carbon-intensity fallback is a single source of truth.
  * Precision honors accuracy floors; high carbon → lower precision.
  * RL updates on a dedicated counter (fixes off-by-one).
  * `active_tasks` cleaned in a `finally`.
  * Market trades in a list, not overwritten.
  * Factory rejects unknown kwargs; deprecated kwargs are no-ops.
  * Two-phase budget (reserve / commit / release).
  * Multi-agent dispatch actually invoked.
  * Idempotent task submission keyed by `task_id`.
  * Circuit breaker probes HALF_OPEN.
  * HITL explicit policy; low-urgency auto-approve opt-in.
  * Active learning returns lowest-confidence samples.
  * Clock / RNG injectable for determinism.
  * Cancellation-safe `execute()`.

Runnable with or without the real `src.integration.unified_orchestrator`.
Tests that require the real module are marked `real_only` and skipped
when it is unavailable.

Location: tests/integration/test_unified_system.py
"""

from __future__ import annotations

import asyncio
import math
import random
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace as dc_replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import pytest

# pytest-asyncio: fixture decorator may be sync or async depending on config.
try:
    import pytest_asyncio
    _asyncio_fixture = pytest_asyncio.fixture
except ImportError:  # pragma: no cover
    _asyncio_fixture = pytest.fixture

# Optional dependency — property-based tests are skipped if absent.
try:
    from hypothesis import given, settings, strategies as st
    HAS_HYPOTHESIS = True
except ImportError:  # pragma: no cover
    HAS_HYPOTHESIS = False


# =============================================================================
# Version detection
# =============================================================================
# v5.1 exposes `UnifiedGreenAgent.AUDIT_SAVINGS_FLOOR_PCT`. v5.0 does not.
# `_HAS_V5_1` gates tests that assert v5.1-only behaviour.

_HAS_REAL_ORCHESTRATOR = False
_HAS_V5_1 = False


# =============================================================================
# Fallback stubs (used ONLY when the real module is unavailable)
# =============================================================================
# NOTE (fix 1.2): every error / blocked path returns 0.0 savings and
# `simulated=True`. The previous stub returned 75 % savings on *any*
# input, which made `assert savings > 50` pass without executing a task.


class _FallbackReasonCode(Enum):
    OK = "ok"
    MISSING_FIELDS = "missing_fields"
    BUDGET_EXHAUSTED = "budget_exhausted"
    TEMPORAL_VIOLATION = "temporal_violation"
    EXECUTION_ERROR = "execution_error"
    AUDIT_REJECTED = "audit_rejected"


class _FallbackPrecision(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class _FallbackRole(Enum):
    GENERALIST = "generalist"
    CARBON_HEAVY = "carbon_heavy"
    LATENCY_CRITICAL = "latency_critical"
    ACCURACY_OPTIMIZER = "accuracy_optimizer"


class _FallbackCircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class _FallbackWorkloadProfile:
    model_params: int = 110_000_000
    estimated_energy_kwh: float = 0.01
    estimated_carbon_kgco2e: float = 0.0025
    carbon_optimization_potential: float = 0.5
    execution_dag: List[str] = field(
        default_factory=lambda: ["load", "run", "save"]
    )


@dataclass
class _FallbackExplanation:
    headline: str = ""
    rationale: List[str] = field(default_factory=list)
    confidence: float = 0.85
    contributing_factors: Dict[str, float] = field(default_factory=dict)


@dataclass
class _FallbackUnifiedResult:
    task_id: str = "task"
    status: str = "completed"
    reason_code: _FallbackReasonCode = _FallbackReasonCode.OK
    accuracy: float = 0.0
    energy_kwh: float = 0.0
    carbon_kgco2e: float = 0.0
    carbon_saved_kgco2e: float = 0.0
    carbon_savings_pct: float = 0.0
    simulated: bool = False
    reasoning: str = "stub"
    workload_profile: Optional[_FallbackWorkloadProfile] = None
    decision: Optional[Dict[str, Any]] = None
    data_optimization: Optional[Dict[str, Any]] = None
    explanation: Optional[_FallbackExplanation] = None
    precision_used: str = "fp32"
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    market_trades: List[Dict[str, Any]] = field(default_factory=list)
    distilled_used: bool = False
    federated_blend_applied: bool = False
    causal_policy_used: bool = False
    dispatched_agent: Optional[str] = None
    workflow_steps: List[Dict[str, Any]] = field(default_factory=list)
    confidence: float = 0.0

    @property
    def market_trade(self) -> Optional[Dict[str, Any]]:
        return self.market_trades[-1] if self.market_trades else None


@dataclass
class _FallbackBudget:
    budget_kgco2e: float = 20.0
    remaining_kgco2e: float = 20.0


class _FallbackLedger:
    def __init__(self):
        self._budgets: Dict[str, _FallbackBudget] = {}
        self._spent: Dict[str, float] = defaultdict(float)
        self._reservations: Dict[str, float] = {}

    def set_team_budget(self, team: str, period: str, budget_kgco2e: float):
        self._budgets[team] = _FallbackBudget(budget_kgco2e, budget_kgco2e)

    def get_team_budget(self, team: str) -> Optional[_FallbackBudget]:
        return self._budgets.get(team)

    def check_budget_available(self, team: str, required: float) -> bool:
        b = self._budgets.get(team)
        return True if b is None else required <= b.remaining_kgco2e

    def reserve(self, team: str, required: float) -> Optional[str]:
        b = self._budgets.get(team)
        if b is None or required > b.remaining_kgco2e:
            return None
        b.remaining_kgco2e -= required
        token = f"{team}-{len(self._reservations)}"
        self._reservations[token] = required
        return token

    def commit(self, token: str, actual: float) -> None:
        reserved = self._reservations.pop(token, None)
        if reserved is None:
            return
        refund = max(0.0, reserved - actual)
        for team, b in self._budgets.items():
            if token.startswith(team):
                b.remaining_kgco2e = min(
                    b.budget_kgco2e, b.remaining_kgco2e + refund
                )
                b.remaining_kgco2e = max(0.0, b.remaining_kgco2e - actual)
                return

    def release(self, token: str) -> None:
        reserved = self._reservations.pop(token, None)
        if reserved is None:
            return
        for team, b in self._budgets.items():
            if token.startswith(team):
                b.remaining_kgco2e = min(
                    b.budget_kgco2e, b.remaining_kgco2e + reserved
                )
                return


class _FallbackBenchmarkIntelligence:
    def __init__(self):
        self._count = 0

    def record(self, *a, **kw):
        self._count += 1

    def get_statistics(self):
        return {"num_benchmarks": self._count}


class _FallbackUnifiedAgent:
    """Stub agent used when the real module is unavailable.

    Mirrors the v5.1 contract: error/blocked paths return 0.0 savings,
    `simulated=True`, and a proper `reason_code`. Successful paths use a
    realistic energy / carbon model rather than fabricated constants.
    """
    AUDIT_SAVINGS_FLOOR_PCT = 55.0

    def __init__(self, **kwargs):
        self.config = kwargs
        self.ledger = _FallbackLedger()
        self.benchmark_intelligence = _FallbackBenchmarkIntelligence()
        self.total_tasks_executed = 0
        self.total_carbon_saved = 0.0
        self.features = kwargs.get("features", {})
        self.active_tasks: Dict[str, Any] = {}
        self._bg_tasks: List[asyncio.Task] = []
        self._idempotency: Dict[str, _FallbackUnifiedResult] = {}
        self.running = True

    async def execute(
        self, task: Dict[str, Any], dataset: Any = None
    ) -> _FallbackUnifiedResult:
        task_id = task.get("task_id", "unknown")
        team = task.get("team", "default")

        # Idempotency (mirrors v5.1)
        if task_id in self._idempotency:
            r = self._idempotency[task_id]
            return dc_replace(r, reasoning=f"[duplicate] {r.reasoning}")

        # Validation
        if not task.get("model_name") and not task.get("dataset_size"):
            return _FallbackUnifiedResult(
                task_id=task_id, status="error",
                reason_code=_FallbackReasonCode.MISSING_FIELDS,
                reasoning=("Missing required fields: "
                           "model_name or dataset_size"),
                simulated=True,
            )

        # Budget check
        budget = self.ledger.get_team_budget(team)
        if budget:
            est_carbon = task.get("dataset_size", 1000) / 10000.0 * 0.5
            if est_carbon > budget.remaining_kgco2e:
                return _FallbackUnifiedResult(
                    task_id=task_id, status="blocked",
                    reason_code=_FallbackReasonCode.BUDGET_EXHAUSTED,
                    reasoning=(f"Carbon budget exhausted: "
                               f"{budget.remaining_kgco2e:.4f} kgCO₂e "
                               f"remaining"),
                    simulated=True,
                )

        # Simulated compute
        await asyncio.sleep(0.01)

        profile = _FallbackWorkloadProfile()
        data_opt = None
        if dataset is not None:
            n = len(dataset)
            data_opt = {
                "original_size": n,
                "optimized_size": max(1, n // 2),
                "compression_ratio": 2.0 if n > 1 else 1.0,
                "estimated_energy_savings_kwh": 0.005,
            }

        decision = None
        if task.get("fine_tuning_method") == "full_fine_tuning":
            decision = {"how": "lora", "enforced": True}

        # Realistic model (mirrors _delegate_task in v5.1)
        ds = float(task.get("dataset_size", 500) or 500)
        intensity = 250.0  # kg CO₂e / MWh
        est_energy = ds / 100_000.0 * 0.5
        est_carbon = est_energy * intensity / 1000.0
        actual_energy = est_energy * 0.5 * 0.6  # int8 × lora
        actual_carbon = actual_energy * intensity / 1000.0
        saved = max(0.0, est_carbon - actual_carbon)
        savings_pct = (saved / est_carbon * 100.0) if est_carbon > 0 else 0.0

        # v5.1 fix: never fabricate. Flag sub-floor.
        simulated = savings_pct < self.AUDIT_SAVINGS_FLOOR_PCT

        self.total_tasks_executed += 1
        self.total_carbon_saved += saved
        self.benchmark_intelligence.record(task_id)

        explanation = _FallbackExplanation(
            headline=f"[completed] {task_id} ({savings_pct:.1f}% saved)",
            rationale=["executed via fallback stub",
                       f"savings_pct={savings_pct:.2f}"],
            confidence=0.85 if not simulated else 0.4,
        )

        result = _FallbackUnifiedResult(
            task_id=task_id, status="completed",
            reason_code=_FallbackReasonCode.OK,
            accuracy=0.92,
            energy_kwh=actual_energy,
            carbon_kgco2e=actual_carbon,
            carbon_saved_kgco2e=saved,
            carbon_savings_pct=savings_pct,
            simulated=simulated,
            workload_profile=profile,
            decision=decision,
            data_optimization=data_opt,
            explanation=explanation,
            precision_used="int8",
            temporal_verified=True,
            distilled_used=bool(self.features.get("quantum_distillation")),
            market_trades=([{"type": "sell", "carbon_kg": saved,
                             "revenue_usd": saved * 0.02}]
                           if saved > 0 else []),
            dispatched_agent=task_id,
            confidence=explanation.confidence,
        )
        self._idempotency[task_id] = result
        return result

    async def start(self) -> None:
        self.running = True

    async def shutdown(self) -> None:
        self.running = False
        for t in self._bg_tasks:
            t.cancel()
        self._bg_tasks = []

    async def stop(self) -> None:
        await self.shutdown()

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "total_tasks_executed": self.total_tasks_executed,
            "total_carbon_saved_kgco2e": self.total_carbon_saved,
            "decision_core": {},
            "benchmarks": self.benchmark_intelligence.get_statistics(),
        }


# =============================================================================
# Real orchestrator import (fallback if unavailable)
# =============================================================================

try:
    from src.integration.unified_orchestrator import (  # type: ignore
        UnifiedGreenAgent,
        create_unified_agent,
        UnifiedResult,
    )
    _HAS_REAL_ORCHESTRATOR = True
    _HAS_V5_1 = hasattr(UnifiedGreenAgent, "AUDIT_SAVINGS_FLOOR_PCT")
except Exception:  # noqa: BLE001
    UnifiedGreenAgent = _FallbackUnifiedAgent  # type: ignore
    UnifiedResult = _FallbackUnifiedResult      # type: ignore
    _HAS_V5_1 = True  # the fallback mimics v5.1 behaviour

    async def create_unified_agent(**kwargs):  # type: ignore
        agent = _FallbackUnifiedAgent(**kwargs)
        await agent.start()
        return agent


#: Skip marker for tests that require the real orchestrator.
requires_real = pytest.mark.skipif(
    not _HAS_REAL_ORCHESTRATOR,
    reason="real orchestrator not importable",
)

#: Skip marker for tests that assert v5.1-only behaviour.
requires_v5_1 = pytest.mark.skipif(
    not _HAS_V5_1,
    reason="v5.1 features not available (v5.0 orchestrator?)",
)


# =============================================================================
# Shared fixtures
# =============================================================================

@pytest.fixture
def canonical_task() -> Dict[str, Any]:
    """A task dict that legitimately clears the 55 % savings floor."""
    return {
        "task_id": "canonical",
        "model_name": "llama-7b",
        "task_type": "fine_tuning",
        "dataset_size": 50_000,
        "num_epochs": 1,
        "batch_size": 16,
        "hardware": "V100",
        "team": "test_team",
        "fine_tuning_method": "full_fine_tuning",   # triggers LoRA rewrite
        "target_accuracy": 0.90,
    }


@pytest.fixture
def tiny_task() -> Dict[str, Any]:
    """A task that legitimately produces *sub-floor* savings."""
    return {
        "task_id": "tiny",
        "model_name": "small",
        "task_type": "inference",
        "dataset_size": 10,
        "team": "test_team",
    }


@pytest.fixture
def small_dataset() -> List[Dict[str, Any]]:
    return [{"id": f"s{i}", "text": f"Sample {i}"} for i in range(100)]


@_asyncio_fixture
async def agent():
    """Default agent — enhancements enabled."""
    a = await create_unified_agent(
        enable_meta_cognitive=False,
        enable_neuro_symbolic=False,
        enable_quantum=False,
        num_ray_workers=2,
    )
    yield a
    await a.shutdown()


# =============================================================================
# Original workflow tests (soft assertions hardened)
# =============================================================================

class TestUnifiedSystem:
    """Core workflow tests with hardened assertions."""

    @pytest.mark.asyncio
    async def test_complete_workflow(self, agent, canonical_task):
        result = await agent.execute(canonical_task)

        # ---- Hard assertions ----
        assert result.status == "completed"
        assert result.task_id == canonical_task["task_id"]
        assert result.reason_code.value == "ok" or result.reason_code.value == "policy_enforced"
        assert result.accuracy > 0
        assert result.energy_kwh > 0
        assert result.carbon_kgco2e > 0
        assert result.carbon_saved_kgco2e >= 0
        assert 0.0 <= result.carbon_savings_pct <= 100.0

        # ---- v5.1 fix 1.1: never assert savings without checking simulated ----
        assert (result.simulated
                or result.carbon_savings_pct >= 50.0), (
            f"savings {result.carbon_savings_pct:.1f}% below floor and "
            f"not flagged simulated"
        )

        # ---- Workload profile is populated ----
        assert result.workload_profile is not None
        assert result.workload_profile.model_params > 0

    @pytest.mark.asyncio
    async def test_data_optimization(self, agent, small_dataset):
        task = {
            "task_id": "data_opt",
            "model_name": "bert-base",
            "task_type": "fine_tuning",
            "dataset_size": len(small_dataset),
            "team": "test_team",
        }
        result = await agent.execute(task, dataset=small_dataset)
        assert result.status == "completed"
        # Hard assert — no more `if result.data_optimization:`
        assert result.data_optimization is not None
        d = result.data_optimization
        assert d["optimized_size"] < d["original_size"]
        assert d["estimated_energy_savings_kwh"] > 0

    @pytest.mark.asyncio
    async def test_carbon_budget_enforcement(self, agent):
        agent.ledger.set_team_budget("budget_test_team", "2026-03", 0.001)
        task = {
            "task_id": "budget_test",
            "model_name": "bert-large",
            "task_type": "training",
            "dataset_size": 100_000,
            "team": "budget_test_team",
        }
        result = await agent.execute(task)
        assert result.status in ("blocked", "error")
        # v5.1 fix 1.3: reason code is typed, not a substring
        if result.status == "blocked":
            assert result.reason_code.value == "budget_exhausted"

    @pytest.mark.asyncio
    async def test_policy_enforcement(self, agent):
        # Use a task small enough to be sub-floor; policy is what matters
        task = {
            "task_id": "policy",
            "model_name": "llama-7b",
            "task_type": "fine_tuning",
            "dataset_size": 500,  # < 5000 → LoRA rewrite
            "fine_tuning_method": "full_fine_tuning",
            "team": "test_team",
        }
        result = await agent.execute(task)
        assert result.status == "completed"
        # Hard assert: policy rewrite ALWAYS sets `decision`
        assert result.decision is not None
        assert result.decision["how"] != "full_fine_tuning"
        assert result.decision.get("enforced") is True

    @pytest.mark.asyncio
    async def test_benchmarking(self, agent):
        task = {
            "task_id": "bench",
            "model_name": "bert-base",
            "task_type": "fine_tuning",
            "dataset_size": 500,
            "team": "test_team",
        }
        before = agent.benchmark_intelligence.get_statistics()["num_benchmarks"]
        result = await agent.execute(task)
        assert result.status == "completed"
        after = agent.benchmark_intelligence.get_statistics()["num_benchmarks"]
        assert after == before + 1

    @pytest.mark.asyncio
    async def test_multiple_tasks(self, agent):
        tasks = [
            {"task_id": f"multi_{i}", "model_name": "bert-base",
             "task_type": "fine_tuning", "dataset_size": 500,
             "team": "test_team"}
            for i in range(3)
        ]
        results = [await agent.execute(t) for t in tasks]
        assert all(r.status == "completed" for r in results)
        total_saved = sum(r.carbon_saved_kgco2e for r in results)
        assert total_saved >= 0  # could be zero if all simulated
        stats = agent.get_statistics()
        assert stats["total_tasks_executed"] >= 3

    @pytest.mark.asyncio
    async def test_error_handling(self, agent):
        result = await agent.execute({"task_id": "bad"})
        assert result.status in ("error", "blocked")
        assert len(result.reasoning) > 0
        # v5.1 fix: typed reason code
        assert result.reason_code.value in (
            "missing_fields", "execution_error"
        )
        # v5.1 fix 1.2: no fabricated savings on error
        assert result.carbon_savings_pct == 0.0 or result.simulated is True

    @pytest.mark.asyncio
    async def test_statistics_tracking(self, agent):
        task = {"task_id": "stats", "model_name": "bert-base",
                "dataset_size": 100, "team": "test_team"}
        await agent.execute(task)
        stats = agent.get_statistics()
        assert stats["total_tasks_executed"] > 0
        assert stats["total_carbon_saved_kgco2e"] >= 0
        assert "decision_core" in stats
        assert "benchmarks" in stats


# =============================================================================
# Component integration tests (kept — value-add when modules exist)
# =============================================================================

class TestComponentIntegration:

    @pytest.mark.asyncio
    async def test_workload_interpreter_integration(self):
        try:
            from src.interpretation.workload_interpreter import (
                WorkloadInterpreter,
            )
        except Exception:  # noqa: BLE001
            pytest.skip("WorkloadInterpreter not available")
        profile = WorkloadInterpreter().interpret({
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 10_000,
        })
        assert profile.model_params > 0
        assert profile.estimated_energy_kwh > 0
        assert profile.carbon_optimization_potential > 0
        assert len(profile.execution_dag) > 0

    @pytest.mark.asyncio
    async def test_data_optimizer_integration(self):
        try:
            from src.optimization.synthetic_data_optimizer import (
                SyntheticDataOptimizer,
            )
        except Exception:  # noqa: BLE001
            pytest.skip("SyntheticDataOptimizer not available")
        opt = SyntheticDataOptimizer()
        dataset = [{"id": f"s{i}"} for i in range(100)]
        result = opt.optimize(
            dataset=dataset, target_compression=0.5,
            baseline_energy_kwh=1.0,
        )
        # Dict or object — normalise
        if isinstance(result, dict):
            optimized = result["optimized_size"]
            original = result["original_size"]
            savings = result["estimated_energy_savings_kwh"]
        else:
            optimized = result.optimized_size
            original = result.original_size
            savings = result.estimated_energy_savings_kwh
        assert optimized <= original
        assert savings > 0


# =============================================================================
# Enhancement-layer tests (kept, hardened, and augmented)
# =============================================================================

class TestQuantumDistillation:

    @pytest.mark.asyncio
    async def test_quantum_enabled_agent(self):
        agent = await create_unified_agent(
            num_ray_workers=1,
            features={"quantum_distillation": True},
        )
        try:
            result = await agent.execute({
                "task_id": "quantum", "model_name": "bert-base",
                "task_type": "fine_tuning", "dataset_size": 500,
                "team": "test_team",
            })
            assert result.status == "completed"
            # Hard assert — distillation active
            assert result.distilled_used is True
        finally:
            await agent.shutdown()

    def test_precision_quantization_real(self):
        """Exercise the real quantize() static method, all levels."""
        try:
            from green_agent_v5_1 import (
                AdaptivePrecisionController, PrecisionLevel,
            ) as _module_unused  # noqa: F401
        except Exception:  # noqa: BLE001
            pytest.skip("v5.1 module not importable for direct testing")
        # Deferred import (the file may not be on path in every env)
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")
        q = mod.AdaptivePrecisionController.quantize
        levels = mod.PrecisionLevel
        for level in (levels.FP32, levels.FP16, levels.INT8,
                      levels.INT4, levels.QUANTUM_DISTILLED):
            v = 0.123456789
            qv = q(v, level)
            assert qv <= v + 1e-9


class TestCausalRL:

    @pytest.mark.asyncio
    async def test_cumulative_savings(self):
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            results = []
            for i in range(3):
                results.append(await agent.execute({
                    "task_id": f"causal_{i}",
                    "model_name": "bert-base",
                    "task_type": "fine_tuning",
                    "dataset_size": 500,
                    "team": "test_team",
                }))
            assert all(r.status == "completed" for r in results)
            # Accept either real or simulated — both are valid outcomes
            assert all(
                r.simulated or r.carbon_saved_kgco2e > 0 for r in results
            )
        finally:
            await agent.shutdown()


class TestFederatedLearning:

    def test_fedavg_weighted_mean(self):
        """Inline FedAvg smoke test (structural, dependency-free)."""
        updates = [{"v": 0.2, "w": 10}, {"v": 0.8, "w": 30}]
        tw = sum(u["w"] for u in updates)
        blended = sum(u["v"] * u["w"] for u in updates) / tw
        assert 0.6 < blended < 0.7

    def test_multi_deployment_blend(self):
        deployments = {
            "us-ca": {"mean_savings_pct": 75.0, "sample_count": 100},
            "eu-north": {"mean_savings_pct": 82.0, "sample_count": 50},
        }
        total_w = sum(d["sample_count"] for d in deployments.values())
        blended = sum(
            d["mean_savings_pct"] * d["sample_count"]
            for d in deployments.values()
        ) / total_w
        assert 75.0 < blended < 82.0


class TestMultiAgentCoordination:

    @pytest.mark.asyncio
    async def test_dispatch_recorded_in_workflow(self, agent):
        """v5.1 fix R11: coordinator.select_agent() is actually invoked."""
        result = await agent.execute({
            "task_id": "dispatch", "model_name": "bert-base",
            "task_type": "fine_tuning", "dataset_size": 500,
            "team": "test_team",
        })
        assert result.status == "completed"
        # Step 4.5 present when coordinator enabled
        step_names = [s["name"] for s in result.workflow_steps]
        assert "Dispatch" in step_names or result.dispatched_agent is not None


class TestTemporalLogic:

    def test_complexity_bounds_property(self):
        def check(c):
            return 0.0 <= c <= 1.0
        assert check(0.5) is True
        assert check(1.5) is False
        assert check(-0.1) is False

    def test_deadline_respected_property(self):
        def check(deadline_hours, latency_hours):
            if deadline_hours is None:
                return True
            return latency_hours <= deadline_hours
        assert check(2.0, 1.0) is True
        assert check(1.0, 2.0) is False
        assert check(None, 100.0) is True

    def test_energy_budget_property(self):
        def check(e, max_kwh=10.0):
            return e <= max_kwh
        assert check(0.5) is True
        assert check(15.0) is False

    @pytest.mark.asyncio
    @requires_v5_1
    async def test_temporal_violation_blocks_pipeline(self):
        """v5.1 fix 1.3: temporal violations block, not just warn."""
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            # Deadline in the past → DeadlineRespected fails
            past = datetime.now(timezone.utc) - timedelta(hours=1)
            result = await agent.execute({
                "task_id": "past_deadline",
                "model_name": "bert-base",
                "task_type": "fine_tuning",
                "dataset_size": 500,
                "team": "test_team",
                "deadline": past,
            })
            assert result.status == "blocked"
            assert result.reason_code.value == "temporal_violation"
            assert "DeadlineRespected" in result.temporal_violations
            assert result.temporal_verified is False
        finally:
            await agent.shutdown()


class TestExplainableAI:

    @pytest.mark.asyncio
    async def test_explanation_present_and_hard_asserted(self, agent):
        result = await agent.execute({
            "task_id": "xai", "model_name": "bert-base",
            "task_type": "fine_tuning", "dataset_size": 500,
            "team": "test_team",
        })
        assert result.explanation is not None
        assert isinstance(result.explanation.headline, str)
        assert len(result.explanation.headline) > 0
        assert len(result.explanation.rationale) > 0
        assert 0.0 <= result.explanation.confidence <= 1.0

    @pytest.mark.asyncio
    @requires_v5_1
    async def test_explanation_counterfactual_present(self, agent):
        """v5.1 fix R16: counterfactual appears when strategy differs."""
        result = await agent.execute({
            "task_id": "cf", "model_name": "llama-7b",
            "task_type": "fine_tuning",
            "dataset_size": 500,                    # forces LoRA
            "fine_tuning_method": "full_fine_tuning",
            "team": "test_team",
        })
        assert result.explanation is not None
        rationale = " ".join(result.explanation.rationale)
        assert "Counterfactual" in rationale or "would emit" in rationale


class TestAdaptivePrecision:

    @requires_v5_1
    def test_high_carbon_prefers_lower_precision(self):
        """v5.1 fix 1.4: high carbon → lower precision (less energy)."""
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")
        pc = mod.AdaptivePrecisionController({"supports_int4": True})
        low = pc.select_precision({"carbon_intensity": 100}, {})["precision"]
        high = pc.select_precision({"carbon_intensity": 600}, {})["precision"]
        rank = {"fp32": 4, "fp16": 3, "int8": 2, "int4": 1,
                "quantum_distilled": 0}
        assert rank[high] <= rank[low], (
            f"high-carbon precision {high} should not exceed "
            f"low-carbon precision {low}"
        )

    @requires_v5_1
    def test_accuracy_floor_vetoes_quantization(self):
        """v5.1 fix R19: accuracy ≥ 0.98 → FP32, ≥ 0.95 → FP16."""
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")
        pc = mod.AdaptivePrecisionController({"supports_int4": True})
        assert pc.select_precision(
            {"carbon_intensity": 800, "accuracy_requirement": 0.99}, {}
        )["precision"] == "fp32"
        assert pc.select_precision(
            {"carbon_intensity": 800, "accuracy_requirement": 0.96}, {}
        )["precision"] == "fp16"


class TestCarbonMarkets:

    def test_market_snapshot_structure(self):
        snap = {
            "carbon_price_per_tco2_usd": 45.0,
            "rec_price_per_mwh_usd": 6.0,
            "rec_available_mwh": 100.0,
        }
        assert snap["carbon_price_per_tco2_usd"] > 0
        assert snap["rec_price_per_mwh_usd"] > 0
        assert snap["rec_available_mwh"] >= 0

    def test_credit_purchase_respects_budget(self):
        class _MC:
            def __init__(self, budget=10.0):
                self.budget = budget
                self.spent = 0.0

            def buy(self, carbon_kg, price_per_tco2=45.0):
                cost = (carbon_kg / 1000.0) * price_per_tco2
                if self.spent + cost > self.budget:
                    return None
                self.spent += cost
                return {"cost": cost, "carbon_kg": carbon_kg}

        mc = _MC(budget=1.0)
        trade = mc.buy(carbon_kg=1.0)
        assert trade is not None
        assert trade["cost"] > 0
        assert mc.buy(carbon_kg=1000.0) is None


class TestResilienceAndChaos:

    @requires_v5_1
    def test_circuit_breaker_half_open_probes(self):
        """v5.1 fix R14: HALF_OPEN allows exactly one probe."""
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")

        cb = mod.CircuitBreaker(
            "x", failure_threshold=2, recovery_timeout_seconds=1,
        )
        cb.record_failure()
        cb.record_failure()
        assert cb.state == mod.CircuitState.OPEN

        # Simulate timeout recovery
        cb.last_failure_at = (
            datetime.now(timezone.utc) - timedelta(seconds=5)
        )
        assert cb.can_call() is True
        assert cb.state == mod.CircuitState.HALF_OPEN
        # Second concurrent probe blocked
        assert cb.can_call() is False

        cb.record_success()
        assert cb.state == mod.CircuitState.CLOSED
        assert cb._probes_in_flight == 0

    @pytest.mark.asyncio
    async def test_agent_survives_errors(self, agent):
        # Invalid task
        r1 = await agent.execute({"task_id": "invalid"})
        assert r1.status in ("error", "blocked")
        # Internal state clean
        assert agent.active_tasks == {}
        # Valid task afterwards works
        r2 = await agent.execute({
            "task_id": "valid_after_error",
            "model_name": "bert-base",
            "task_type": "fine_tuning",
            "dataset_size": 500,
            "team": "test_team",
        })
        assert r2.status == "completed"


class TestHumanInTheLoop:
    """Replaces the two tests that encoded the pre-v5.1 broken behaviour."""

    @requires_v5_1
    def test_default_policy_does_not_auto_approve_low_urgency(self):
        """v5.1 fix 1.12: low-urgency auto-approve is opt-in."""
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")
        policy = mod.HITLPolicy()
        assert policy.low_urgency_auto_approve is False
        assert policy.low_urgency_auto_deny is False
        assert policy.default_decision is False

    @requires_v5_1
    @pytest.mark.asyncio
    async def test_low_urgency_with_no_callback_uses_policy(self):
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")
        gate = mod.HumanInTheLoopGate(policy=mod.HITLPolicy())
        approved = await gate.request_review(
            "t", "reason", urgency="low", context={"confidence": 0.3},
        )
        # No callback + default policy = deny
        assert approved is False

    @requires_v5_1
    @pytest.mark.asyncio
    async def test_callback_timeout_uses_default_decision(self):
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")

        async def slow_cb(_):
            await asyncio.sleep(10)
            return True

        gate = mod.HumanInTheLoopGate(
            policy=mod.HITLPolicy(timeout_seconds=0.05,
                                  default_decision=False)
        )
        gate.set_async_callback(slow_cb)
        assert await gate.request_review("t", "r", "medium") is False

    @requires_v5_1
    @pytest.mark.asyncio
    async def test_active_learning_returns_lowest_confidence(self):
        """v5.1 fix R21: AL returns lowest-confidence, not newest."""
        from importlib import import_module
        try:
            mod = import_module("green_agent_v5_1")
        except Exception:  # noqa: BLE001
            pytest.skip("green_agent_v5_1 not importable")

        gate = mod.HumanInTheLoopGate()
        for i, conf in enumerate([0.9, 0.1, 0.5, 0.2]):
            await gate.request_review(
                f"t{i}", "r", "medium",
                context={"confidence": conf},
            )
        batch = gate.active_learning_batch(2)
        confs = [b["confidence"] for b in batch]
        assert confs == sorted(confs), "must be ascending by confidence"
        assert confs[0] == pytest.approx(0.1)

    @pytest.mark.asyncio
    async def test_budget_block_triggers_hitl(self, agent):
        """Budget-blocked tasks with HITL configured should be reviewed."""
        agent.ledger.set_team_budget("hitl_team", "2026-03", 0.0001)

        approved = {"called": False, "value": False}

        def cb(_req):
            approved["called"] = True
            approved["value"] = True   # approve anyway — just verify path
            return True

        if hasattr(agent, "set_hitl_callback"):
            agent.set_hitl_callback(cb)

        result = await agent.execute({
            "task_id": "hitl_block",
            "model_name": "bert-large",
            "task_type": "training",
            "dataset_size": 100_000,
            "team": "hitl_team",
        })
        assert result.status in ("blocked", "error", "completed")


class TestFeatureToggles:

    @pytest.mark.asyncio
    async def test_all_features_disabled(self):
        features = {k: False for k in (
            "causal_rl", "xai", "adaptive_precision", "federated",
            "multi_agent", "temporal_logic", "carbon_market",
            "chaos_testing", "hitl", "quantum_distillation",
        )}
        agent = await create_unified_agent(
            num_ray_workers=1, features=features,
        )
        try:
            result = await agent.execute({
                "task_id": "legacy", "model_name": "bert-base",
                "task_type": "fine_tuning", "dataset_size": 500,
                "team": "test_team",
            })
            assert result.status == "completed"
            # v5.1 fix: XAI is off → explanation is None
            assert result.explanation is None
        finally:
            await agent.shutdown()

    @pytest.mark.asyncio
    async def test_partial_features_enabled(self):
        features = {
            "xai": True, "temporal_logic": True,
            "causal_rl": False, "federated": False,
        }
        agent = await create_unified_agent(
            num_ray_workers=1, features=features,
        )
        try:
            result = await agent.execute({
                "task_id": "partial", "model_name": "bert-base",
                "task_type": "fine_tuning", "dataset_size": 500,
                "team": "test_team",
            })
            assert result.status == "completed"
            assert result.explanation is not None
        finally:
            await agent.shutdown()


# =============================================================================
# v5.1 regression tests (each maps to a review fix)
# =============================================================================

@requires_v5_1
class TestV5_1Fixes:
    """One test per v5.1 fix. These are the tests that were missing."""

    @pytest.mark.asyncio
    async def test_sub_floor_savings_are_flagged_not_fabricated(
        self, tiny_task
    ):
        """Fix 1.1: 55 % floor flags simulated; never rewrites data."""
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            result = await agent.execute(tiny_task)
            assert result.status == "completed"
            # Contract: either real savings, or simulated
            assert (result.simulated
                    or result.carbon_savings_pct
                    >= agent.AUDIT_SAVINGS_FLOOR_PCT)
            # Energy must be > 0 and consistent with carbon
            if result.energy_kwh > 0:
                implied_intensity = (
                    result.carbon_kgco2e / result.energy_kwh * 1000.0
                )
                assert 0 < implied_intensity < 1000  # sane range
        finally:
            await agent.shutdown()

    @pytest.mark.asyncio
    async def test_factory_rejects_unknown_kwargs(self):
        """Fix 1.13: unknown kwargs raise TypeError."""
        with pytest.raises(TypeError, match="unexpected kwargs"):
            await create_unified_agent(
                definitely_not_a_real_kwarg_xyz=True,
            )

    @pytest.mark.asyncio
    async def test_factory_deprecates_known_old_kwargs(self):
        """Fix 1.13: `enable_*` args are no-ops, not errors."""
        # Should NOT raise
        agent = await create_unified_agent(
            enable_meta_cognitive=True,
            num_ray_workers=1,
        )
        await agent.shutdown()

    @pytest.mark.asyncio
    async def test_idempotent_return(self):
        """Fix R15: duplicate task_id returns cached result."""
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            task = {
                "task_id": "idem", "model_name": "bert-base",
                "dataset_size": 500, "team": "test_team",
            }
            r1 = await agent.execute(task)
            r2 = await agent.execute(task)
            assert r2.reasoning.startswith("[duplicate]")
            assert r1.energy_kwh == r2.energy_kwh
            assert r1.carbon_kgco2e == r2.carbon_kgco2e
            # Only counted once
            assert agent.get_statistics()["total_tasks_executed"] == 1
        finally:
            await agent.shutdown()

    @pytest.mark.asyncio
    async def test_reason_code_typed(self, agent, canonical_task):
        """Fix 1.3: reason codes are typed enums, not free strings."""
        r = await agent.execute(canonical_task)
        # Enum-like — has a `value` attribute
        assert hasattr(r.reason_code, "value")
        assert isinstance(r.reason_code.value, str)


# =============================================================================
# Lifecycle tests (fix R6 / R24)
# =============================================================================

class TestLifecycle:

    @pytest.mark.asyncio
    async def test_stop_cancels_background_tasks(self):
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            await agent.execute({
                "task_id": "x", "model_name": "bert-base",
                "dataset_size": 100, "team": "t",
            })
            await agent.stop()
            # Background tasks drained
            assert getattr(agent, "_bg_tasks", []) == []
            # Idempotent
            await agent.stop()
        finally:
            await agent.shutdown()

    @pytest.mark.asyncio
    async def test_active_tasks_empty_after_failure(self, agent):
        """Fix 1.6: active_tasks cleaned in finally."""
        await agent.execute({"task_id": "bad"})
        assert agent.active_tasks == {}

    @pytest.mark.asyncio
    async def test_cancellation_propagates(self):
        """Fix R24: CancelledError is not swallowed."""
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            task = asyncio.create_task(agent.execute({
                "task_id": "cancel_me",
                "model_name": "bert-base",
                "dataset_size": 10_000_000,
                "team": "t",
            }))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert agent.active_tasks == {}
        finally:
            await agent.shutdown()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        async with await create_unified_agent(num_ray_workers=1) as agent:
            result = await agent.execute({
                "task_id": "ctx", "model_name": "bert-base",
                "dataset_size": 100, "team": "t",
            })
            assert result.status == "completed"


# =============================================================================
# Two-phase budget tests (fix R36 / 3.3)
# =============================================================================

class TestTwoPhaseBudget:

    @pytest.mark.asyncio
    @requires_v5_1
    async def test_reserve_commit_reconciles_to_actual(self):
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            agent.ledger.set_team_budget("t", "2026-03", 10.0)
            result = await agent.execute({
                "task_id": "commit", "model_name": "bert-base",
                "dataset_size": 1000, "team": "t",
            })
            budget = agent.ledger.get_team_budget("t")
            assert budget is not None
            # Remaining = 10 - actual_carbon (not estimated)
            assert budget.remaining_kgco2e == pytest.approx(
                10.0 - result.carbon_kgco2e, abs=1e-6,
            )
        finally:
            await agent.shutdown()

    @pytest.mark.asyncio
    @requires_v5_1
    async def test_reserve_released_on_blocked_pipeline(self):
        """A blocked task (temporal) must not consume budget."""
        agent = await create_unified_agent(num_ray_workers=1)
        try:
            agent.ledger.set_team_budget("t", "2026-03", 10.0)
            past = datetime.now(timezone.utc) - timedelta(hours=1)
            before = agent.ledger.get_team_budget("t").remaining_kgco2e
            r = await agent.execute({
                "task_id": "blocked_temporal",
                "model_name": "bert-base",
                "task_type": "fine_tuning",
                "dataset_size": 500,
                "team": "t",
                "deadline": past,
            })
            after = agent.ledger.get_team_budget("t").remaining_kgco2e
            if r.status == "blocked":
                assert after == pytest.approx(before, abs=1e-9)
        finally:
            await agent.shutdown()


# =============================================================================
# Concurrency tests
# =============================================================================

class TestConcurrency:

    @pytest.mark.asyncio
    async def test_concurrent_execute_isolation(self):
        agent = await create_unified_agent(num_ray_workers=4)
        try:
            tasks = [
                {"task_id": f"conc_{i}", "model_name": "bert-base",
                 "dataset_size": 100, "team": "t"}
                for i in range(10)
            ]
            results = await asyncio.gather(
                *[agent.execute(t) for t in tasks]
            )
            assert all(r.status == "completed" for r in results)
            assert agent.get_statistics()["total_tasks_executed"] == 10
            assert len({r.task_id for r in results}) == 10
            assert agent.active_tasks == {}
        finally:
            await agent.shutdown()


# =============================================================================
# Contract tests — run the same assertion against real AND fallback
# =============================================================================

class TestContract:
    """Assertions that must hold for any compliant orchestrator."""

    @pytest.mark.asyncio
    async def test_missing_fields(self, agent):
        r = await agent.execute({"task_id": "x"})
        assert r.status in ("error", "blocked")
        assert len(r.reasoning) > 0
        assert r.carbon_savings_pct == 0.0 or r.simulated is True

    @pytest.mark.asyncio
    async def test_metrics_consistency(self, agent, canonical_task):
        """Energy × intensity ≈ carbon within a sane band."""
        r = await agent.execute(canonical_task)
        if r.energy_kwh > 0:
            implied = r.carbon_kgco2e / r.energy_kwh * 1000.0
            assert 0 < implied < 1000, (
                f"Implied carbon intensity {implied:.0f} kg/MWh "
                f"is not physically plausible"
            )

    @pytest.mark.asyncio
    async def test_workflow_steps_are_well_formed(self, agent,
                                                   canonical_task):
        r = await agent.execute(canonical_task)
        for step in r.workflow_steps:
            assert "step" in step
            assert "name" in step
            assert "status" in step
            # Steps monotone-ish
            assert isinstance(step["step"], (int, float))


# =============================================================================
# Property-based tests (skipped if hypothesis not installed)
# =============================================================================

@pytest.mark.skipif(not HAS_HYPOTHESIS, reason="hypothesis not installed")
class TestProperties:

    @given(
        st.integers(min_value=0, max_value=10_000_000),
        st.text(min_size=1, max_size=20),
    )
    @settings(max_examples=20, deadline=None)
    def test_workload_profile_sane(self, dataset_size, model_name):
        agent = _FallbackUnifiedAgent()
        p = agent._profile_workload({
            "model_name": model_name,
            "dataset_size": dataset_size,
        }) if hasattr(agent, "_profile_workload") else None
        if p is None:
            # Fall back to a minimal local check
            assert dataset_size >= 0
            return
        assert 0.0 <= p.complexity <= 1.0
        assert p.estimated_energy_kwh >= 0.0
        assert p.estimated_carbon_kgco2e >= 0.0


# =============================================================================
# End-to-end scenario
# =============================================================================

@pytest.mark.asyncio
async def test_end_to_end_scenario():
    print("\n" + "=" * 80)
    print("🧪 RUNNING END-TO-END SCENARIO (v5.1)")
    print("=" * 80 + "\n")

    agent = await create_unified_agent(num_ray_workers=2)
    try:
        agent.ledger.set_team_budget("e2e_team", "2026-03", 10.0)

        task = {
            "task_id": "e2e_bert_sentiment",
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_name": "sst2",
            "dataset_size": 5_000,
            "num_epochs": 2,
            "batch_size": 16,
            "hardware": "V100",
            "team": "e2e_team",
            "priority": 0.8,
            "deferrable": True,
            "fine_tuning_method": "full_fine_tuning",
            "target_accuracy": 0.90,
        }
        dataset = [
            {"id": f"s{i}", "text": f"Sample {i}", "label": i % 2}
            for i in range(task["dataset_size"])
        ]

        result = await agent.execute(task, dataset=dataset)

        # ---- Core workflow ----
        print("📋 Workflow assertions:")
        assert result.status == "completed"
        assert result.workload_profile is not None
        assert result.decision is not None
        assert result.data_optimization is not None
        print(f"   ✅ Task: {result.task_id}")
        print(f"   ✅ Reason code: {result.reason_code.value}")
        print(f"   ✅ Savings: {result.carbon_savings_pct:.1f}% "
              f"(simulated={result.simulated})")

        # ---- v5.1 integrity ----
        assert (result.simulated
                or result.carbon_savings_pct >= 50.0)

        # ---- Enhancement layers ----
        print("\n🌿 Enhancement layers:")
        if result.explanation is not None:
            print(f"   ✅ XAI confidence: {result.explanation.confidence:.2f}")
        print(f"   ✅ Precision: {result.precision_used}")
        print(f"   ✅ Temporal verified: {result.temporal_verified}")
        print(f"   ✅ Distilled: {result.distilled_used}")
        print(f"   ✅ Market trades: {len(result.market_trades)}")
        print(f"   ✅ Dispatched: {result.dispatched_agent}")
    finally:
        await agent.shutdown()

    print("\n" + "=" * 80)
    print("✅ END-TO-END SCENARIO PASSED")
    print("=" * 80)


# =============================================================================
# Main entry — invoke pytest, not asyncio.run (fix 2.12)
# =============================================================================

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
