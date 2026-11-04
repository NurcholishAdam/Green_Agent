# -*- coding: utf-8 -*-
"""
Comprehensive Demo: All Green_Agent Extensions (Enhanced, v2)
=============================================================

Demonstrates:
  1. Task Complexity Normalization
  2. Budget Constraints
  3. RLHF Reward Shaping
  4. Multi-Layer Reporting
  5. Policy Evaluation
  6. Cinebench Integration (all modules combined)

Demo 7 (enabled with --enhanced) integrates LIMIT Graph metrics, MODP,
RLHF, Multi-Teacher MoE gating, and bio-inspired weight tuning — all
layered ON TOP OF the file's own production modules, not reimplemented
from scratch.

What Demo 7 does
----------------
  1. Runs NormalizedEfficiencyCalculator, RewardShaper (ECO mode), and
     LayeredReporter on the same 4-agent batch that demos 1-6 use.
  2. Uses the module outputs as the three "teacher" signals:
        rule_teacher      <- normalized efficiency
        rlhf_teacher      <- RewardShaper reward
        historical_teacher<- LayeredReporter weighted score
  3. Computes per-agent MoE scores via a softmax gate whose logits depend
     on the agent's own feature vector — different agents get different
     gate weights.
  4. Evolves 4D MODP weights with a tournament-selection GA whose fitness
     is the maximin of (0.6*MODP + 0.4*MoE) over the PARETO FRONTIER.
     The GA objective is the SAME as the final ranking objective.
  5. Prints a baseline comparison: the highest-accuracy agent vs. the
     GA's choice, so the reader can see whether the enhancement changed
     the recommendation.

What Demo 7 does NOT claim to do
--------------------------------
  - On-policy distillation (there is no distillation, only aggregation).
  - Learned MoE gating over time (the gate is per-agent at inference).
  - Learning the historical teacher from real history.

Fixes applied vs. v1
--------------------
  P0  Demo 7 used none of the file's modules      -> uses 3 of them
  P0  Input data collapsed 4D -> 3D               -> 4 distinct C/E ratios
  P0  Linear fitness -> one-hot weights           -> maximin over frontier
  P0  GA had no selection pressure                -> tournament (k=3)
  P0  Unseeded RNG                                -> --seed flag
  P0  2/3 teachers identical                      -> 3 distinct signals
  P0  MoE gate was a fixed array                  -> per-agent softmax
  P0  best_weights off-by-one + aliasing          -> global best + copy
  P0  input() x5 blocked CI                       -> --no-pause flag
  P1  Dead BudgetManager import                   -> removed
  P1  No ENHANCED_AVAILABLE guard                 -> added
  P1  No --seed / --pop / --gens flags            -> full CLI surface
  P1  main() had no argv / no exit                -> both added
  P1  Triplicate normalization                    -> AgentNormalizer
  P1  No baseline comparison                      -> printed side-by-side
  P2  Demos returned None                         -> typed returns
  P2  Duplicate print_section logic               -> helper
  P2  No return from Demo 7                       -> returns dict
  P2  Schema drift across demo 6/7                -> unified schema

Run with:
    python examples/demo_all_extensions.py                   # demos 1-6
    python examples/demo_all_extensions.py --enhanced        # + demo 7
    python examples/demo_all_extensions.py --only-enhanced --seed 42

Preserved public surface
------------------------
  demo_complexity_normalization, demo_budget_constraints,
  demo_rlhf_reward_shaping, demo_policy_evaluation,
  demo_multi_layer_reporting, demo_cinebench_integration,
  demo_advanced_techniques, main
"""

from __future__ import annotations

import argparse
import asyncio
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Add src to path (preserved for parity with v1).
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# ---- Optional: numpy (only needed for Demo 7) ------------------------------
try:
    import numpy as np
    ENHANCED_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCED_AVAILABLE = False

# ---- Production modules used by demos 1-6 ---------------------------------
from metrics.efficiency_calculator import NormalizedEfficiencyCalculator
from constraints.budget_manager import Budget                       # noqa: F401
from constraints.budget_enforcer import BudgetEnforcer
from rlhf.reward_shaper import ExecutionMode, RewardShaper
from rlhf.policy_evaluator import PolicyEvaluationEnvironment
from reporting.layered_reporter import LayeredReporter
from reporting.report_generator import ReportGenerator


# ============================================================================
# SECTION 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _bounded_inverse(q: float, scale: float) -> float:
    """1 / (1 + q/scale) — bounded on (0, 1], scale-invariant."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def print_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


def _pause(message: str, no_pause: bool) -> None:
    """Interactive prompt that degrades gracefully in CI."""
    if no_pause:
        return
    try:
        input(message)
    except EOFError:
        pass


# ============================================================================
# SECTION 2 — Agent batch (4 agents; carbon/energy ratios all differ)
# ============================================================================

def _demo_7_agents() -> List[Dict[str, Any]]:
    """
    The Demo 7 agent batch.

    IMPORTANT: carbon_kg / energy_kwh is NOT constant across agents, so the
    4D weight vector has 4 independent dimensions. (v1 used the Cinebench
    triple where carbon = 0.2 * energy for every agent, collapsing MODP to
    3 dimensions.)

    `OldModel` is dominated by `ResNet50` (lower accuracy, higher energy,
    higher carbon, higher latency) so the GA's maximin fitness has a
    genuinely non-frontier reference.
    """
    def trace(n_steps: int, tools: int, tokens: int, ms: int) -> Dict[str, Any]:
        return {
            "prompt": "Cinebench classification",
            "reasoning": [f"Step {i}" for i in range(n_steps)],
            "tool_calls": [{"tool": f"t{i}"} for i in range(tools)],
            "execution_time_ms": ms,
            "context_tokens": tokens,
        }

    return [
        {
            "agent_id": "ResNet50", "task_id": "t_resnet",
            "accuracy": 0.94, "task_success": 0.94,
            "energy_kwh": 0.008, "carbon_kg": 0.0016, "latency_ms": 350,
            "trace": trace(5, 1, 300, 350),
        },
        {
            "agent_id": "EfficientNet", "task_id": "t_efficientnet",
            "accuracy": 0.92, "task_success": 0.92,
            "energy_kwh": 0.003, "carbon_kg": 0.0004, "latency_ms": 180,
            "trace": trace(3, 1, 200, 180),
        },
        {
            "agent_id": "GreenMobile", "task_id": "t_greenmobile",
            "accuracy": 0.86, "task_success": 0.86,
            "energy_kwh": 0.001, "carbon_kg": 0.0001, "latency_ms": 80,
            "trace": trace(1, 0, 100, 80),
        },
        {
            "agent_id": "OldModel", "task_id": "t_oldmodel",
            "accuracy": 0.80, "task_success": 0.80,
            "energy_kwh": 0.005, "carbon_kg": 0.00125, "latency_ms": 500,
            "trace": trace(5, 0, 300, 500),
        },
    ]


# ============================================================================
# SECTION 3 — Agent normalizer (4D)
# ============================================================================

class AgentNormalizer:
    """
    Converts a Demo 7 agent dict to a 4-vector where higher = better.
    Scale-invariant normalization replaces v1's hard-coded ceilings
    (10 Wh, 2 g, 500 ms) which saturated for realistic values.
    """

    FEATURE_NAMES: Tuple[str, ...] = ("accuracy", "energy", "carbon", "latency")

    def __init__(self, scales: Optional[Dict[str, float]] = None) -> None:
        self.scales: Dict[str, float] = {
            "energy_wh": 10.0,
            "carbon_g": 2.0,
            "latency_ms": 500.0,
        }
        if scales:
            self.scales.update({k: float(v) for k, v in scales.items()})

    def to_vector(self, agent: Dict[str, Any]) -> "np.ndarray":
        energy_wh = max(float(agent.get("energy_kwh", 0.0)), 0.0) * 1000.0
        carbon_g = max(float(agent.get("carbon_kg", 0.0)), 0.0) * 1000.0
        return np.array(
            [
                _clamp(agent.get("accuracy", 0.0)),
                _bounded_inverse(energy_wh, self.scales["energy_wh"]),
                _bounded_inverse(carbon_g, self.scales["carbon_g"]),
                _bounded_inverse(agent.get("latency_ms", 0.0), self.scales["latency_ms"]),
            ],
            dtype=np.float64,
        )


def _compute_frontier_ids(
    agents: Sequence[Dict[str, Any]],
    normalizer: AgentNormalizer,
) -> Tuple[set, Dict[str, "np.ndarray"]]:
    """Return (frontier_ids, vecs) using 4D dominance."""
    vecs = {a["agent_id"]: normalizer.to_vector(a) for a in agents}
    frontier_ids: set = set()
    for aid, vec in vecs.items():
        dominated = False
        for other_id, other_vec in vecs.items():
            if other_id == aid:
                continue
            if (other_vec >= vec).all() and (other_vec > vec).any():
                dominated = True
                break
        if not dominated:
            frontier_ids.add(aid)
    return frontier_ids, vecs


# ============================================================================
# SECTION 4 — Multi-teacher MoE (per-agent softmax over 3 DISTINCT signals)
# ============================================================================

class MultiTeacherMoE:
    """
    Three DISTINCT teachers driven by the file's own production modules:

      rule_teacher       = NormalizedEfficiencyCalculator composite score
      rlhf_teacher       = RewardShaper reward (ECO mode)
      historical_teacher = LayeredReporter weighted score

    The gate logits depend on the agent's own vector so different agents
    receive different gate weights. v1 used a fixed [0.4, 0.4, 0.2] array
    and two of its three teachers returned identical values.
    """

    N_TEACHERS = 3

    def __init__(
        self,
        teacher_scores: Dict[str, Tuple[float, float, float]],
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> None:
        self.teacher_scores = teacher_scores
        self.centrality = _clamp(graph_metrics.get("centrality", 0.5))
        self.connectivity = _clamp(graph_metrics.get("connectivity", 0.5))
        self.human_feedback = _clamp(human_feedback)

    def _gate_logits(self, vec: "np.ndarray") -> "np.ndarray":
        # Agent-vector-dependent gate: different agents -> different weights.
        h = self.human_feedback
        return np.array(
            [
                vec[0] * 1.5 + self.centrality,        # rule: centrality-weighted
                vec[0] * h + vec[1] * (1.0 - h),       # RLHF: preference-weighted
                1.0,                                    # historical: neutral
            ],
            dtype=np.float64,
        )

    def score(self, agent_id: str, vec: "np.ndarray") -> Tuple[float, "np.ndarray", "np.ndarray"]:
        t = self.teacher_scores.get(agent_id, (0.0, 0.0, 0.0))
        teachers = np.array(t, dtype=np.float64)
        logits = self._gate_logits(vec)
        z = logits - logits.max()
        gate = np.exp(z) / np.exp(z).sum()
        return float(gate @ teachers), teachers, gate


# ============================================================================
# SECTION 5 — Evolutionary weight optimizer (real GA, maximin fitness)
# ============================================================================

class EvolutionaryWeightOptimizer:
    """
    Tournament selection + Gaussian mutation + elitism.

    Fitness = maximin of (0.6*MODP + 0.4*MoE) over the Pareto frontier.
    Because the MoE scores are precomputed and fixed, the fitness is a
    non-linear function of w — the maximum is NOT at a simplex vertex, so
    the GA produces a genuine blend. (v1's mean-of-all fitness was linear
    in w, so the maximum was always a one-hot vertex.)
    """

    def __init__(
        self,
        n_objectives: int,
        rng: "np.random.Generator",
        population_size: int = 20,
        generations: int = 10,
        mutation_sigma: float = 0.05,
        crossover_rate: float = 0.7,
        elitism: int = 2,
        modp_weight: float = 0.6,
        moe_weight: float = 0.4,
    ) -> None:
        if n_objectives < 1:
            raise ValueError("n_objectives must be >= 1")
        if population_size < 2:
            raise ValueError("population_size must be >= 2")
        if not 0 <= elitism < population_size:
            raise ValueError("elitism must be in [0, population_size)")
        if mutation_sigma <= 0:
            raise ValueError("mutation_sigma must be > 0")
        if not 0.0 <= crossover_rate <= 1.0:
            raise ValueError("crossover_rate must be in [0,1]")

        self.n = int(n_objectives)
        self.rng = rng
        self.population_size = int(population_size)
        self.generations = int(generations)
        self.mutation_sigma = float(mutation_sigma)
        self.crossover_rate = float(crossover_rate)
        self.elitism = int(elitism)
        self.modp_weight = float(modp_weight)
        self.moe_weight = float(moe_weight)

    def _composite(self, w: "np.ndarray", vec: "np.ndarray", moe: float) -> float:
        return self.modp_weight * float(w @ vec) + self.moe_weight * float(moe)

    def _fitness(
        self,
        w: "np.ndarray",
        frontier_pairs: Sequence[Tuple["np.ndarray", float]],
    ) -> float:
        if not frontier_pairs:
            return 0.0
        return float(min(self._composite(w, vec, moe) for vec, moe in frontier_pairs))

    def _tournament(
        self,
        population: Sequence["np.ndarray"],
        scores: Sequence[float],
        k: int = 3,
    ) -> "np.ndarray":
        size = min(k, len(population))
        idxs = self.rng.choice(len(population), size=size, replace=False)
        best = max((int(i) for i in idxs), key=lambda i: scores[i])
        return population[best].copy()

    def _crossover(self, a: "np.ndarray", b: "np.ndarray") -> "np.ndarray":
        if self.rng.random() < self.crossover_rate:
            alpha = float(self.rng.random())
            return alpha * a + (1.0 - alpha) * b
        return a.copy()

    def _mutate(self, w: "np.ndarray") -> "np.ndarray":
        w = w + self.rng.normal(0.0, self.mutation_sigma, self.n)
        w = np.clip(w, 1e-6, None)
        return w / w.sum()

    def evolve(
        self,
        frontier_pairs: Sequence[Tuple["np.ndarray", float]],
        initial_population: Optional[Sequence["np.ndarray"]] = None,
        *,
        verbose: bool = True,
    ) -> Tuple["np.ndarray", float]:
        population: List["np.ndarray"] = []
        if initial_population:
            for w in initial_population:
                w = np.asarray(w, dtype=np.float64)
                if w.shape == (self.n,):
                    w = np.clip(w, 1e-6, None)
                    population.append(w / w.sum())
                    if len(population) >= self.population_size:
                        break
        while len(population) < self.population_size:
            population.append(self.rng.dirichlet(np.ones(self.n)))

        # Global best across ALL generations (fixes v1's off-by-one).
        best_w = population[0].copy()
        best_f = -math.inf

        for gen in range(self.generations):
            scores = [self._fitness(w, frontier_pairs) for w in population]
            for w, s in zip(population, scores):
                if s > best_f:
                    best_f = s
                    best_w = w.copy()

            if verbose and (gen % 3 == 0 or gen == self.generations - 1):
                print(f"   gen {gen:02d}: best_fitness = {best_f:.4f}")

            order = np.argsort(scores)[::-1]
            elite = [population[int(i)].copy() for i in order[: self.elitism]]
            new_pop: List["np.ndarray"] = list(elite)
            while len(new_pop) < self.population_size:
                p1 = self._tournament(population, scores)
                p2 = self._tournament(population, scores)
                child = self._crossover(p1, p2)
                child = self._mutate(child)
                new_pop.append(child)
            population = new_pop

        return best_w, best_f


# ============================================================================
# SECTION 6 — Demo 1: Task Complexity Normalization (preserved)
# ============================================================================

def demo_complexity_normalization() -> Dict[str, Any]:
    print_section("DEMO 1: Task Complexity Normalization")

    print("Scenario: Compare Cinebench classifiers on different task complexities\n")

    results = [
        {
            "agent_id": "ResNet50",
            "task_id": "simple_classification",
            "accuracy": 0.90,
            "energy_kwh": 0.002,
            "carbon_kg": 0.0004,
            "latency_ms": 100,
            "trace": {
                "prompt": "Classify this simple Cinebench score",
                "reasoning": ["Load model", "Classify"],
                "tool_calls": [],
                "execution_time_ms": 100,
                "context_tokens": 50,
            },
        },
        {
            "agent_id": "ResNet50",
            "task_id": "complex_classification",
            "accuracy": 0.95,
            "energy_kwh": 0.010,
            "carbon_kg": 0.0020,
            "latency_ms": 500,
            "trace": {
                "prompt": "Analyze this complex Cinebench benchmark with multiple metrics...",
                "reasoning": [f"Step {i}" for i in range(10)],
                "tool_calls": [{"tool": "benchmark_db"}, {"tool": "specs_api"}],
                "execution_time_ms": 500,
                "context_tokens": 500,
            },
        },
    ]

    print("❌ WITHOUT Complexity Normalization:")
    print(f"  Simple task: {results[0]['energy_kwh']:.4f} kWh - looks 'efficient'")
    print(f"  Complex task: {results[1]['energy_kwh']:.4f} kWh - looks 'wasteful'")
    print(f"  Conclusion: Simple task seems 5x better\n")

    calculator = NormalizedEfficiencyCalculator()
    comparison = calculator.compare_across_complexities(results)

    print("✅ WITH Complexity Normalization:")
    for ranking in comparison["rankings"]:
        print(f"\n  Task: {ranking['task_id']}")
        print(f"    Raw Energy: {ranking['energy_kwh']:.4f} kWh")
        print(f"    Task Complexity: {ranking['task_complexity']:.2f}")
        print(f"    Energy Efficiency: {ranking['energy_efficiency']:.6f}")
        print(f"    → Fair comparison enabled!")

    print("\n📊 Summary:")
    print(f"  Avg Energy Efficiency: {comparison['summary']['avg_energy_efficiency']:.6f}")
    print(f"  Avg Accuracy/Watt: {comparison['summary']['avg_accuracy_per_watt']:.2f}")

    return comparison


# ============================================================================
# SECTION 7 — Demo 2: Budget Constraints (preserved)
# ============================================================================

async def demo_budget_constraints() -> Dict[str, Any]:
    print_section("DEMO 2: Budget Constraints")

    print("Scenario: Deploy Cinebench classifier with strict energy budget\n")

    budget = Budget.eco_budget()
    print(f"Budget: {budget.name}")
    print(f"  Max Energy: {budget.max_energy_wh} Wh")
    print(f"  Max Carbon: {budget.max_carbon_g} g CO₂")
    print(f"  Max Latency: {budget.max_latency_ms} ms\n")

    enforcer = BudgetEnforcer(budget)

    async def classifier_agent(task):
        return {
            "output": "classification_result",
            "accuracy": 0.93,
            "metrics": {
                "energy_kwh": 0.003,
                "carbon_kg": 0.0006,
                "latency_ms": 300,
            },
        }

    print("Executing classifier within budget...\n")

    task = {"input": "benchmark_data"}
    result = await enforcer.execute_with_budget(
        agent_fn=classifier_agent,
        task=task,
        estimated_consumption={
            "energy_wh": 3.0,
            "carbon_g": 0.6,
            "latency_ms": 300,
        },
    )

    if result["success"]:
        print("✅ Execution SUCCESS!")
        print(f"  Actual Energy: {result['actual_consumption']['energy_wh']:.2f} Wh")
        print(f"  Remaining Budget: {result['remaining_budget']['energy_wh']:.2f} Wh")
    else:
        print("❌ Execution BLOCKED!")
        print(f"  Violations: {result['violations']}")

    print("\n📊 Budget Report:")
    report = enforcer.get_budget_report()
    util = report["utilization"]
    print(f"  Energy Used: {util['energy_wh']:.1%}")
    print(f"  Carbon Used: {util['carbon_g']:.1%}")

    return {"result": result, "report": report}


# ============================================================================
# SECTION 8 — Demo 3: RLHF Reward Shaping (preserved)
# ============================================================================

def demo_rlhf_reward_shaping() -> Dict[str, Dict[str, Any]]:
    print_section("DEMO 3: RLHF Reward Shaping")

    print("Scenario: Compare agents across different execution modes\n")

    agent_results = [
        {"agent_id": "HighAccuracy_Agent", "task_success": 0.95,
         "energy_kwh": 0.010, "carbon_kg": 0.0020, "latency_ms": 500},
        {"agent_id": "Efficient_Agent", "task_success": 0.88,
         "energy_kwh": 0.002, "carbon_kg": 0.0004, "latency_ms": 150},
        {"agent_id": "Fast_Agent", "task_success": 0.90,
         "energy_kwh": 0.005, "carbon_kg": 0.0010, "latency_ms": 80},
    ]

    modes = [ExecutionMode.ECO_MODE, ExecutionMode.FAST_MODE, ExecutionMode.ACCURACY_MODE]
    results: Dict[str, Any] = {}

    for mode in modes:
        shaper = RewardShaper(mode)
        comparison = shaper.compare_policies(agent_results)
        results[mode.value] = comparison

        print(f"\n🎯 {mode.value.upper()} MODE:")
        print(f"   Best Agent: {comparison['best_agent']}")

        for rank in comparison["rankings"][:2]:
            print(f"\n   #{rank['rank']} {rank['agent_id']}")
            print(f"      Reward: {rank['reward']:.3f}")
            print(f"      Success: {rank['raw_metrics']['task_success']:.2%}")
            print(f"      Energy: {rank['raw_metrics']['energy_kwh']:.4f} kWh")

    return results


# ============================================================================
# SECTION 9 — Demo 4: Policy Evaluation Environment (preserved)
# ============================================================================

def demo_policy_evaluation() -> Dict[str, Any]:
    print_section("DEMO 4: Policy Evaluation Environment")

    print("Scenario: Evaluate agent policy across all modes\n")

    def my_classifier(task):
        return {
            "accuracy": 0.92,
            "energy_kwh": 0.003,
            "carbon_kg": 0.0006,
            "latency_ms": 200,
        }

    tasks = [{"task_id": f"task_{i}"} for i in range(10)]

    env = PolicyEvaluationEnvironment()
    results = env.multi_mode_evaluation(my_classifier, tasks, verbose=False)

    print("Results across all execution modes:\n")

    for mode, eval_result in results["evaluations"].items():
        print(f"  {mode.upper()}:")
        print(f"    Avg Reward: {eval_result['avg_reward']:.3f}")
        print(f"    Avg Success: {eval_result['avg_task_success']:.2%}")
        print(f"    Total Energy: {eval_result['summary']['total_energy_kwh']:.4f} kWh")

    print(f"\n🏆 Best Mode: {results['best_mode']}")
    print(f"\n💡 Recommendations:")
    for mode, rec in results["recommendations"].items():
        print(f"  {mode}: {rec}")

    return results


# ============================================================================
# SECTION 10 — Demo 5: Multi-Layer Reporting (preserved)
# ============================================================================

def demo_multi_layer_reporting() -> Dict[str, Any]:
    print_section("DEMO 5: Multi-Layer Reporting")

    print("Scenario: Generate transparent three-layer reports\n")

    results = [
        {
            "agent_id": "Agent_A", "task_id": "task_1",
            "accuracy": 0.95, "energy_kwh": 0.005, "carbon_kg": 0.0010,
            "latency_ms": 300,
            "trace": {
                "prompt": "Classify benchmark",
                "reasoning": [f"Step {i}" for i in range(5)],
                "tool_calls": [{"tool": "db"}],
                "execution_time_ms": 300,
                "context_tokens": 200,
            },
        },
        {
            "agent_id": "Agent_B", "task_id": "task_2",
            "accuracy": 0.88, "energy_kwh": 0.002, "carbon_kg": 0.0004,
            "latency_ms": 150,
            "trace": {
                "prompt": "Classify benchmark",
                "reasoning": ["Step 1", "Step 2"],
                "tool_calls": [],
                "execution_time_ms": 150,
                "context_tokens": 100,
            },
        },
    ]

    reporter = LayeredReporter()
    full_report = reporter.generate_full_report(results, scenario="production")

    print("Three-Layer Report Generated:\n")

    for agent_report in full_report["reports"]:
        print(f"Agent: {agent_report['agent_id']}")
        print(
            f"  Layer 1 (Raw): Accuracy={agent_report['layer1_raw']['accuracy']:.2%}, "
            f"Energy={agent_report['layer1_raw']['energy_wh']:.2f} Wh"
        )
        print(
            f"  Layer 2 (Normalized): "
            f"Energy/Task={agent_report['layer2_normalized']['energy_per_task']:.6f}"
        )
        print(
            f"  Layer 3 (Scenario): "
            f"Score={agent_report['layer3_scenario']['weighted_score']:.3f}, "
            f"Rank=#{agent_report['layer3_scenario']['rank']}\n"
        )

    print("\n📄 Generating formatted reports...\n")

    report_gen = ReportGenerator()

    exec_summary = report_gen.generate_executive_summary(full_report)
    print("Executive Summary:")
    print(exec_summary[:500] + "...\n")

    print("✅ Full technical and research reports also available")

    return full_report


# ============================================================================
# SECTION 11 — Demo 6: Cinebench Integration (preserved)
# ============================================================================

async def demo_cinebench_integration() -> Dict[str, Any]:
    print_section("DEMO 6: Cinebench Integration (All Modules Combined)")

    print("Scenario: Complete Cinebench classifier evaluation workflow\n")

    budget = Budget(
        max_energy_wh=50.0,
        max_carbon_g=10.0,
        max_latency_ms=5000,
        name="Cinebench Production Budget",
    )

    classifiers_results = [
        {
            "agent_id": "ResNet50", "accuracy": 0.94, "energy_kwh": 0.008,
            "carbon_kg": 0.0016, "latency_ms": 350, "task_success": 0.94,
            "trace": {
                "prompt": "Cinebench classification",
                "reasoning": [f"Step {i}" for i in range(5)],
                "tool_calls": [{"tool": "vision"}],
                "execution_time_ms": 350, "context_tokens": 300,
            },
        },
        {
            "agent_id": "EfficientNet", "accuracy": 0.92, "energy_kwh": 0.003,
            "carbon_kg": 0.0006, "latency_ms": 180, "task_success": 0.92,
            "trace": {
                "prompt": "Cinebench classification",
                "reasoning": [f"Step {i}" for i in range(3)],
                "tool_calls": [{"tool": "vision"}],
                "execution_time_ms": 180, "context_tokens": 200,
            },
        },
        {
            "agent_id": "MobileNet", "accuracy": 0.86, "energy_kwh": 0.001,
            "carbon_kg": 0.0002, "latency_ms": 80, "task_success": 0.86,
            "trace": {
                "prompt": "Cinebench classification",
                "reasoning": ["Quick check"],
                "tool_calls": [],
                "execution_time_ms": 80, "context_tokens": 100,
            },
        },
    ]

    print("Step 1: Normalize by Complexity")
    print("-" * 40)
    calculator = NormalizedEfficiencyCalculator()
    comparison = calculator.compare_across_complexities(classifiers_results)

    for rank in comparison["rankings"]:
        print(
            f"  {rank['agent_id']}: "
            f"Accuracy/Watt={rank['accuracy_per_watt']:.2f}, "
            f"Efficiency={rank['composite_efficiency']:.3f}"
        )

    print(f"\n  Best Normalized: {comparison['summary']['best_agent']}")

    print("\nStep 2: Check Budget Compliance")
    print("-" * 40)

    for result in classifiers_results:
        energy_wh = result["energy_kwh"] * 1000
        carbon_g = result["carbon_kg"] * 1000
        fits_budget = (
            energy_wh <= budget.max_energy_wh
            and carbon_g <= budget.max_carbon_g
            and result["latency_ms"] <= budget.max_latency_ms
        )
        status = "✅" if fits_budget else "❌"
        print(
            f"  {status} {result['agent_id']}: "
            f"E={energy_wh:.1f}Wh, C={carbon_g:.1f}g, "
            f"L={result['latency_ms']:.0f}ms"
        )

    print("\nStep 3: RLHF Mode Selection")
    print("-" * 40)

    eco_shaper = RewardShaper(ExecutionMode.ECO_MODE)
    eco_comparison = eco_shaper.compare_policies(classifiers_results)

    fast_shaper = RewardShaper(ExecutionMode.FAST_MODE)
    fast_comparison = fast_shaper.compare_policies(classifiers_results)

    print(f"  Eco Mode Best: {eco_comparison['best_agent']}")
    print(f"  Fast Mode Best: {fast_comparison['best_agent']}")

    print("\nStep 4: Generate Multi-Layer Report")
    print("-" * 40)

    reporter = LayeredReporter()
    full_report = reporter.generate_full_report(classifiers_results, "production")

    print(f"  Top Ranked: {full_report['reports'][0]['agent_id']}")
    print(
        f"  L3 Score: "
        f"{full_report['reports'][0]['layer3_scenario']['weighted_score']:.3f}"
    )

    print("\n🎯 FINAL RECOMMENDATION:")
    print("-" * 40)
    print(f"  Production Deployment: {full_report['reports'][0]['agent_id']}")
    print(f"  Reason: Best balance of accuracy, efficiency, and budget compliance")
    print(f"  Accuracy: {full_report['reports'][0]['layer1_raw']['accuracy']:.1%}")
    print(f"  Energy: {full_report['reports'][0]['layer1_raw']['energy_wh']:.2f} Wh")
    print(f"  Within Budget: ✅")

    return {"comparison": comparison, "report": full_report}


# ============================================================================
# SECTION 12 — Demo 7: Advanced Techniques (rewritten, uses production modules)
# ============================================================================

def demo_advanced_techniques(
    *,
    rng: "np.random.Generator",
    human_feedback: float = 0.8,
    graph_metrics: Optional[Dict[str, float]] = None,
    initial_modp_weights: Optional[Sequence[float]] = None,
    population_size: int = 20,
    generations: int = 10,
    mutation_sigma: float = 0.05,
    crossover_rate: float = 0.7,
    elitism: int = 2,
) -> Dict[str, Any]:
    """
    Advanced Pareto demo built on the file's own production modules.

    Layer 1 (modules):  NormalizedEfficiencyCalculator, RewardShaper, LayeredReporter.
    Layer 2 (learned):  per-agent MoE gate + evolved MODP weights.
    Layer 3 (report):   ranking aligned with the GA's fitness + baseline comparison.
    """
    print_section("DEMO 7: Advanced Techniques Integration")

    if not ENHANCED_AVAILABLE:
        print("Advanced demo requires numpy. Skipping.")
        return {"skipped": True, "reason": "numpy_unavailable"}

    gm = dict(graph_metrics or {"centrality": 0.7, "connectivity": 0.6, "density": 0.5})

    print("🌐 LIMIT Graph Metrics:")
    for k, v in gm.items():
        print(f"   {k}: {v}")
    print(f"\n👤 RLHF Feedback: {human_feedback} (prefers accuracy)")

    # ---- 1. Run the file's own production modules --------------------
    agents = _demo_7_agents()

    calculator = NormalizedEfficiencyCalculator()
    norm_resp = calculator.compare_across_complexities(agents)

    eco_shaper = RewardShaper(ExecutionMode.ECO_MODE)
    eco_resp = eco_shaper.compare_policies(agents)

    reporter = LayeredReporter()
    report_resp = reporter.generate_full_report(agents, "production")

    # ---- 2. Extract per-agent teacher signals from module outputs -----
    def _lookup(entries, agent_id, *keys):
        for entry in entries:
            if entry.get("agent_id") == agent_id:
                for key in keys:
                    value = entry.get(key)
                    if isinstance(value, (int, float)):
                        return _clamp(float(value))
                return 0.0
        return 0.0

    def _norm_signal(agent_id):
        return _lookup(
            norm_resp.get("rankings", []),
            agent_id,
            "composite_efficiency", "energy_efficiency", "accuracy_per_watt",
        )

    def _eco_signal(agent_id):
        return _lookup(eco_resp.get("rankings", []), agent_id, "reward")

    def _layered_signal(agent_id):
        for entry in report_resp.get("reports", []):
            if entry.get("agent_id") == agent_id:
                value = entry.get("layer3_scenario", {}).get("weighted_score")
                if isinstance(value, (int, float)):
                    return _clamp(float(value))
        return 0.0

    teacher_scores: Dict[str, Tuple[float, float, float]] = {}
    for a in agents:
        aid = a["agent_id"]
        teacher_scores[aid] = (
            _norm_signal(aid),
            _eco_signal(aid),
            _layered_signal(aid),
        )

    print("\n🎓 Module-derived teacher signals:")
    print(f"   {'agent':<16} {'normalized':>10} {'eco_reward':>10} {'layered':>10}")
    for aid, (a, b, c) in teacher_scores.items():
        print(f"   {aid:<16} {a:>10.3f} {b:>10.3f} {c:>10.3f}")

    # ---- 3. Per-agent MoE gate ---------------------------------------
    moe = MultiTeacherMoE(teacher_scores, gm, human_feedback)

    normalizer = AgentNormalizer()
    frontier_ids, vecs = _compute_frontier_ids(agents, normalizer)
    print(f"\n📐 Frontier: {sorted(frontier_ids)}")

    moe_scores: Dict[str, float] = {}
    for a in agents:
        aid = a["agent_id"]
        moe_scores[aid] = moe.score(aid, vecs[aid])[0]

    frontier_pairs: List[Tuple["np.ndarray", float]] = [
        (vecs[aid], moe_scores[aid]) for aid in frontier_ids
    ]

    # ---- 4. Evolve 4D MODP weights -----------------------------------
    if initial_modp_weights is None:
        initial_modp_weights = (0.4, 0.25, 0.2, 0.15)
    init_w = np.asarray(initial_modp_weights, dtype=np.float64)
    if init_w.shape != (4,):
        raise ValueError("initial_modp_weights must have 4 components")
    if (init_w < 0).any() or init_w.sum() <= 0:
        raise ValueError("initial_modp_weights must be non-negative, sum > 0")
    init_w = init_w / init_w.sum()

    print(f"\n📎 Initial MODP weights (accuracy, energy, carbon, latency):")
    print(f"   {np.round(init_w, 3)}")

    print(
        f"\n🧬 Evolving MODP weights "
        f"(population={population_size}, generations={generations})..."
    )
    optimizer = EvolutionaryWeightOptimizer(
        n_objectives=4,
        rng=rng,
        population_size=population_size,
        generations=generations,
        mutation_sigma=mutation_sigma,
        crossover_rate=crossover_rate,
        elitism=elitism,
    )

    seed_population: List["np.ndarray"] = [init_w.copy()]
    while len(seed_population) < population_size:
        child = init_w + rng.normal(0.0, 0.1, 4)
        child = np.clip(child, 1e-6, None)
        seed_population.append(child / child.sum())

    best_w, best_f = optimizer.evolve(
        frontier_pairs, initial_population=seed_population, verbose=True
    )
    print(f"\n🧬 Evolved MODP weights:")
    print(f"   {np.round(best_w, 3)}")
    print(f"   Δ vs initial: {np.round(best_w - init_w, 3)}")
    print(f"   Best fitness (maximin composite over frontier): {best_f:.4f}")

    # ---- 5. Rank with the SAME objective the GA optimised -----------
    rows: List[Dict[str, Any]] = []
    for a in agents:
        aid = a["agent_id"]
        modp = float(best_w @ vecs[aid])
        moe_s = moe_scores[aid]
        composite = 0.6 * modp + 0.4 * moe_s
        rows.append({
            "agent_id": aid,
            "modp": modp,
            "moe": moe_s,
            "composite": composite,
            "on_frontier": aid in frontier_ids,
        })
    rows.sort(key=lambda r: r["composite"], reverse=True)

    print("\n📊 Final Enhanced Ranking:")
    print(f"   {'agent':<16} {'MODP':>7} {'MoE':>7} {'Final':>7}  frontier")
    for r in rows:
        marker = "✓" if r["on_frontier"] else " "
        print(
            f"   {r['agent_id']:<16} {r['modp']:>7.3f} "
            f"{r['moe']:>7.3f} {r['composite']:>7.3f}  {marker}"
        )

    best_agent = rows[0]["agent_id"]
    on_frontier = best_agent in frontier_ids
    print(
        f"\n🏆 Best Agent with Advanced Techniques: {best_agent} "
        f"(on Pareto frontier: {'yes' if on_frontier else 'no'})"
    )

    # ---- 6. Baseline comparison -------------------------------------
    frontier_agents = [a for a in agents if a["agent_id"] in frontier_ids]
    baseline = (
        max(frontier_agents, key=lambda a: a["accuracy"])["agent_id"]
        if frontier_agents else None
    )
    print(f"\n📎 Baseline (highest-accuracy frontier agent): {baseline}")
    if baseline is None:
        print("   → Frontier is empty; no baseline.")
    elif baseline == best_agent:
        print("   → Enhanced ranking agrees with the baseline on this batch.")
    else:
        print("   → Enhanced ranking chose a different agent than the baseline.")

    return {
        "best_agent_id": best_agent,
        "baseline_agent_id": baseline,
        "on_frontier": on_frontier,
        "rows": rows,
        "evolved_weights": best_w.tolist(),
        "initial_modp_weights": init_w.tolist(),
        "feature_names": list(normalizer.FEATURE_NAMES),
        "best_fitness": best_f,
        "frontier_ids": sorted(frontier_ids),
        "teacher_scores": {k: list(v) for k, v in teacher_scores.items()},
    }


# ============================================================================
# SECTION 13 — CLI
# ============================================================================

def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Green_Agent Extensions Demo")
    parser.add_argument(
        "--enhanced", action="store_true",
        help="Include advanced techniques demo (DEMO 7).",
    )
    parser.add_argument(
        "--only-enhanced", action="store_true",
        help="Skip demos 1-6 and run only the advanced demo.",
    )
    parser.add_argument(
        "--no-pause", action="store_true",
        help="Never block on Enter (for CI / headless runs).",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="RNG seed for the GA (default: system entropy).",
    )
    parser.add_argument(
        "--population-size", type=int, default=20,
        help="GA population size.",
    )
    parser.add_argument(
        "--generations", type=int, default=10,
        help="GA generations.",
    )
    parser.add_argument(
        "--mutation-sigma", type=float, default=0.05,
        help="Standard deviation of the Gaussian mutation.",
    )
    parser.add_argument(
        "--crossover-rate", type=float, default=0.7,
        help="Probability that a child is a crossover of two parents.",
    )
    parser.add_argument(
        "--elitism", type=int, default=2,
        help="Number of top individuals copied unchanged each generation.",
    )
    parser.add_argument(
        "--human-feedback", type=float, default=0.8,
        help="RLHF score in [0,1]; higher = prefers accuracy.",
    )
    parser.add_argument(
        "--centrality", type=float, default=0.7,
        help="Graph centrality metric in [0,1].",
    )
    parser.add_argument(
        "--connectivity", type=float, default=0.6,
        help="Graph connectivity metric in [0,1].",
    )
    parser.add_argument(
        "--density", type=float, default=0.5,
        help="Graph density metric in [0,1].",
    )
    parser.add_argument(
        "--initial-modp-weights", type=str, default="0.4,0.25,0.2,0.15",
        help="Comma-separated 4 values (accuracy, energy, carbon, latency).",
    )
    return parser.parse_args(argv)


def _parse_modp_weights(s: str) -> List[float]:
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) != 4:
        raise SystemExit(
            f"--initial-modp-weights must have exactly 4 components, got {len(parts)}"
        )
    try:
        values = [float(p) for p in parts]
    except ValueError as exc:
        raise SystemExit(f"--initial-modp-weights: invalid number: {exc}") from exc
    if any(v < 0 for v in values) or sum(values) <= 0:
        raise SystemExit("--initial-modp-weights must be non-negative, sum > 0")
    return values


def _validate_args(args: argparse.Namespace) -> None:
    if not 0.0 <= args.human_feedback <= 1.0:
        raise SystemExit("--human-feedback must be in [0,1]")
    for name in ("centrality", "connectivity", "density"):
        if not 0.0 <= getattr(args, name) <= 1.0:
            raise SystemExit(f"--{name} must be in [0,1]")
    if args.population_size < 2:
        raise SystemExit("--population-size must be >= 2")
    if args.generations < 1:
        raise SystemExit("--generations must be >= 1")
    if args.mutation_sigma <= 0:
        raise SystemExit("--mutation-sigma must be > 0")
    if not 0.0 <= args.crossover_rate <= 1.0:
        raise SystemExit("--crossover-rate must be in [0,1]")
    if not 0 <= args.elitism < args.population_size:
        raise SystemExit("--elitism must be in [0, population_size)")


# ============================================================================
# SECTION 14 — main
# ============================================================================

async def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _validate_args(args)
    initial_modp_weights = _parse_modp_weights(args.initial_modp_weights)

    rng = np.random.default_rng(args.seed) if ENHANCED_AVAILABLE else None
    seed_display = args.seed if args.seed is not None else "entropy"

    print("\n" + "🌟" * 35)
    print("Green_Agent Extensions - Complete Demo")
    print("🌟" * 35)
    print(f"Seed: {seed_display}\n")

    print("This demo showcases all 4 extension modules:")
    print("  1. Task Complexity Normalization")
    print("  2. Budget Constraints")
    print("  3. RLHF Reward Shaping")
    print("  4. Multi-Layer Reporting")
    print("  5. Policy Evaluation")
    print("  6. Complete Cinebench Integration")
    if args.enhanced or args.only_enhanced:
        print(
            "  7. Advanced Techniques (LIMIT Graph, MODP, RLHF, "
            "Multi-Teacher MoE, Bio-inspired)"
        )

    _pause("Press Enter to start demos...", args.no_pause)

    if not args.only_enhanced:
        demo_complexity_normalization()
        _pause("\nPress Enter for next demo...", args.no_pause)

        await demo_budget_constraints()
        _pause("\nPress Enter for next demo...", args.no_pause)

        demo_rlhf_reward_shaping()
        _pause("\nPress Enter for next demo...", args.no_pause)

        demo_policy_evaluation()
        _pause("\nPress Enter for next demo...", args.no_pause)

        demo_multi_layer_reporting()
        _pause("\nPress Enter for next demo...", args.no_pause)

        await demo_cinebench_integration()

    if args.enhanced or args.only_enhanced:
        _pause("\nPress Enter for advanced demo...", args.no_pause)
        demo_advanced_techniques(
            rng=rng,
            human_feedback=args.human_feedback,
            graph_metrics={
                "centrality": args.centrality,
                "connectivity": args.connectivity,
                "density": args.density,
            },
            initial_modp_weights=initial_modp_weights,
            population_size=args.population_size,
            generations=args.generations,
            mutation_sigma=args.mutation_sigma,
            crossover_rate=args.crossover_rate,
            elitism=args.elitism,
        )

    print("\n" + "=" * 70)
    print("✅ All Demos Complete!")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Run tests: pytest tests/")
    print("  2. Integrate with your Cinebench pipeline")
    print("  3. Deploy to AgentBeats\n")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
