# -*- coding: utf-8 -*-
"""
Demo: Pareto Frontier Analysis for Green_Agent (Enhanced, v2)
=============================================================

Compares agents across 4 objectives (accuracy, energy, carbon, latency)
using Pareto frontier analysis, dominance ranking, complexity analysis,
and over-reasoning detection.

Demo 7 (enabled with --enhanced) adds:
  - Real GA over the probability simplex (tournament selection, Gaussian
    mutation, elitism).
  - Per-agent MoE gating over three DISTINCT teachers.
  - The GA's fitness IS the composite used for ranking (0.6·MODP + 0.4·MoE),
    so the "evolved weights" are the ones actually used.
  - Baseline comparison: prints the naive (highest-accuracy frontier) pick
    alongside the enhanced pick so the reader can see whether they agree.

What Demo 7 does NOT claim to do (and doesn't do):
  - On-policy distillation (there is no distillation, only a GA).
  - Adaptive MoE gating over time (the gate is per-agent at inference, but
    is not learned across runs in this demo).
  - Training against a historical teacher with real history (the "historical"
    teacher is a fixed linear blend).

Fixes applied vs. v1
--------------------
  P0  modp_weights was declared but never used      -> seeds GA + printed
  P0  best_weights could be undefined               -> initialised before loop
  P0  moe_score param named `weights`               -> teacher_gate_probs
  P0  GA had no selection pressure                  -> tournament (k=3)
  P0  Linear fitness -> one-hot weights             -> maximin composite
  P0  2/3 teachers returned identical values        -> 3 distinct teachers
  P0  MoE gate was a fixed array                    -> per-agent softmax
  P0  Unseeded RNG                                  -> --seed flag
  P0  best_weights tracked only last gen            -> global best
  P0  Duplicate scoring logic                       -> single helper
  P1  Hard-coded normalization ceilings             -> 1/(1+q/scale)
  P1  No _clamp helper                              -> added
  P1  3-agent batch was all-Pareto-optimal          -> 5-agent demo-1 batch
  P1  No baseline comparison                        -> printed side-by-side
  P2  Dead imports / unused TaskComplexity import   -> removed
  P2  No CLI flags for GA config                    -> full CLI surface
  P2  main() had no argv / no sys.exit              -> both added
  P2  demos 1-6 returned None                       -> return useful tuples

Run with:
    python examples/demo_pareto_analysis.py                    # demos 1-6
    python examples/demo_pareto_analysis.py --enhanced         # + demo 7
    python examples/demo_pareto_analysis.py --only-enhanced --seed 42

Preserved public surface
------------------------
  demo_basic_pareto, demo_pareto_ranking, demo_agent_comparison,
  demo_complexity_analysis, demo_over_reasoning_detection,
  demo_cinebench_integration, demo_enhanced_pareto_with_advanced_techniques,
  main
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Add src to path (preserved for parity with v1).
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.pareto_analyzer import ParetoPoint, ParetoFrontierAnalyzer
from analysis.complexity_analyzer import ComplexityAnalyzer

# Optional dependency
try:
    import numpy as np
    ENHANCED_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCED_AVAILABLE = False


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
    print("=" * 60)
    print(title)
    print("=" * 60)


# ============================================================================
# SECTION 2 — Agent feature normalizer (4D)
# ============================================================================

class AgentNormalizer:
    """
    Convert a ParetoPoint into a 4-vector where higher = better for every
    component. Scale-invariant normalization replaces the v1 hard-coded
    ceilings (10 Wh, 2 g, 500 ms) that saturated for realistic values.
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

    def to_vector(self, agent: "ParetoPoint") -> "np.ndarray":
        energy_wh = max(float(agent.energy_kwh), 0.0) * 1000.0
        carbon_g = max(float(agent.carbon_co2e_kg), 0.0) * 1000.0
        return np.array(
            [
                _clamp(agent.accuracy),
                _bounded_inverse(energy_wh, self.scales["energy_wh"]),
                _bounded_inverse(carbon_g, self.scales["carbon_g"]),
                _bounded_inverse(agent.latency_ms, self.scales["latency_ms"]),
            ],
            dtype=np.float64,
        )


# ============================================================================
# SECTION 3 — Multi-teacher MoE with per-agent softmax gating
# ============================================================================

class MultiTeacherMoE:
    """
    Three DISTINCT teachers + a per-agent softmax gate.

    v1's rule_teacher and rlhf_teacher both returned `agent.accuracy`
    whenever centrality > 0.5 and human_feedback > 0.5, collapsing the
    "MoE" to 94% accuracy regardless of the gate weights. Here:

      - rule_teacher      hard-branches on centrality
      - rlhf_teacher      soft-blends accuracy and energy by feedback
      - historical_teacher fixed linear blend of all four normalized dims

    Gate logits depend on the agent's own vector, so different agents are
    weighted differently.
    """

    N_TEACHERS = 3

    def __init__(
        self,
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> None:
        self.centrality = _clamp(graph_metrics.get("centrality", 0.5))
        self.connectivity = _clamp(graph_metrics.get("connectivity", 0.5))
        self.human_feedback = _clamp(human_feedback)

    def _rule_teacher(self, vec: "np.ndarray") -> float:
        if self.centrality > 0.5:
            return float(vec[0])   # accuracy
        return float(vec[1])       # energy

    def _rlhf_teacher(self, vec: "np.ndarray") -> float:
        h = self.human_feedback
        return float(h * vec[0] + (1.0 - h) * vec[1])

    def _historical_teacher(self, vec: "np.ndarray") -> float:
        return float(0.4 * vec[0] + 0.3 * vec[1] + 0.2 * vec[2] + 0.1 * vec[3])

    def _gate_logits(self, vec: "np.ndarray") -> "np.ndarray":
        h = self.human_feedback
        return np.array(
            [
                vec[0] * 1.5 + self.centrality,   # rule: centrality-weighted
                vec[0] * h + vec[1] * (1.0 - h),  # RLHF: preference-weighted
                1.0,                              # historical: neutral
            ],
            dtype=np.float64,
        )

    def score(
        self, vec: "np.ndarray"
    ) -> Tuple[float, "np.ndarray", "np.ndarray"]:
        teachers = np.array(
            [
                self._rule_teacher(vec),
                self._rlhf_teacher(vec),
                self._historical_teacher(vec),
            ],
            dtype=np.float64,
        )
        logits = self._gate_logits(vec)
        z = logits - logits.max()
        gate = np.exp(z) / np.exp(z).sum()
        return float(gate @ teachers), teachers, gate


# ============================================================================
# SECTION 4 — Real evolutionary optimizer over the 4-simplex
# ============================================================================

class EvolutionaryWeightOptimizer:
    """
    Real GA. Tournament selection (k=3), uniform crossover, Gaussian
    mutation (renormalised), elitism. Fitness is the maximin of the
    final composite over frontier agents — the SAME composite the demo
    uses for ranking, so the evolved weights are actually the ones used.

    `frontier_pairs` is a list of (vector, moe_score) tuples. The MoE
    score is precomputed because it does not depend on the GA's weights.
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

        self.n = int(n_objectives)
        self.rng = rng
        self.population_size = int(population_size)
        self.generations = int(generations)
        self.mutation_sigma = float(mutation_sigma)
        self.crossover_rate = float(crossover_rate)
        self.elitism = int(elitism)
        self.modp_weight = float(modp_weight)
        self.moe_weight = float(moe_weight)

    def _composite(
        self, w: "np.ndarray", vec: "np.ndarray", moe: float
    ) -> float:
        return self.modp_weight * float(w @ vec) + self.moe_weight * float(moe)

    def _fitness(
        self, w: "np.ndarray", frontier_pairs: Sequence[Tuple["np.ndarray", float]]
    ) -> float:
        if not frontier_pairs:
            return 0.0
        # Maximin over frontier agents forces the weights to be good for the
        # WORST frontier agent, not just the average. (v1's avg-fitness was
        # linear in w, so the maximum was a one-hot vertex of the simplex.)
        return float(
            min(self._composite(w, vec, moe) for vec, moe in frontier_pairs)
        )

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
        # Initialise the population — optionally seeded from `initial_population`.
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

        # Track global best across ALL generations (fixes v1's off-by-one).
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
# SECTION 5 — Unified ranking (GA objective = ranking objective)
# ============================================================================

def _rank_agents(
    agents: Sequence["ParetoPoint"],
    analyzer: "ParetoFrontierAnalyzer",
    normalizer: AgentNormalizer,
    moe: MultiTeacherMoE,
    best_w: "np.ndarray",
    modp_weight: float = 0.6,
    moe_weight: float = 0.4,
) -> Dict[str, Any]:
    """
    Compute the final composite for every agent using the SAME objective
    the GA optimised. Returns rows plus the frontier set so callers can
    distinguish "best by composite" from "best by Pareto dominance".
    """
    frontier_ids = {a.agent_id for a in analyzer.compute_frontier(agents)}
    rows: List[Dict[str, Any]] = []
    for agent in agents:
        vec = normalizer.to_vector(agent)
        moe_score, _, _ = moe.score(vec)
        modp_score = float(best_w @ vec)
        composite = modp_weight * modp_score + moe_weight * moe_score
        rows.append(
            {
                "agent_id": agent.agent_id,
                "modp": modp_score,
                "moe": moe_score,
                "composite": composite,
                "on_frontier": agent.agent_id in frontier_ids,
            }
        )
    rows.sort(key=lambda r: r["composite"], reverse=True)
    return {
        "rows": rows,
        "frontier_ids": frontier_ids,
        "best_agent_id": rows[0]["agent_id"] if rows else None,
    }


# ============================================================================
# SECTION 6 — Shared agent batches
# ============================================================================

def _build_demo_agents() -> List["ParetoPoint"]:
    """The five agents from demo 1, including the dominated `old_model`."""
    return [
        ParetoPoint("gpt4_agent", 0.95, 0.0050, 0.0010, 200),
        ParetoPoint("claude_agent", 0.93, 0.0035, 0.0007, 180),
        ParetoPoint("llama3_agent", 0.88, 0.0020, 0.0004, 120),
        ParetoPoint("mixtral_agent", 0.91, 0.0025, 0.0005, 150),
        ParetoPoint("old_model", 0.80, 0.0080, 0.0016, 250),
    ]


# ============================================================================
# SECTION 7 — Demos 1–6 (preserved from v1)
# ============================================================================

def demo_basic_pareto() -> Tuple[
    List["ParetoPoint"], List["ParetoPoint"], "ParetoFrontierAnalyzer"
]:
    """Demo 1: Basic Pareto frontier analysis."""
    print_section("DEMO 1: Basic Pareto Frontier Analysis")

    agents = _build_demo_agents()
    analyzer = ParetoFrontierAnalyzer()

    print("\n📊 Computing Pareto Frontier...")
    frontier = analyzer.compute_frontier(agents)

    print(f"\nTotal agents: {len(agents)}")
    print(f"Frontier agents: {len(frontier)}")
    print("\n✨ Agents on Pareto frontier:")
    for agent in frontier:
        print(f"  - {agent.agent_id}")
        print(f"    Accuracy: {agent.accuracy:.2%}")
        print(f"    Energy: {agent.energy_kwh*1000:.2f} Wh")
        print(f"    Carbon: {agent.carbon_co2e_kg*1000:.2f} g CO₂")
        print(f"    Latency: {agent.latency_ms:.0f} ms")
        print()

    knee = analyzer.get_knee_point(frontier)
    print(f"🎯 Knee Point (Best Balance): {knee.agent_id}")
    print(f"   This agent offers the best overall compromise.\n")

    return agents, frontier, analyzer


def demo_pareto_ranking() -> Tuple[List["ParetoPoint"], Dict[str, Any]]:
    """Demo 2: Ranking agents by Pareto dominance layers."""
    print_section("DEMO 2: Pareto Dominance Ranking")

    agents = [
        ParetoPoint("excellent", 0.95, 0.002, 0.0004, 100),
        ParetoPoint("very_good", 0.92, 0.003, 0.0006, 120),
        ParetoPoint("good", 0.88, 0.004, 0.0008, 150),
        ParetoPoint("average", 0.85, 0.005, 0.0010, 180),
        ParetoPoint("below_avg", 0.80, 0.007, 0.0014, 220),
    ]

    analyzer = ParetoFrontierAnalyzer()
    print("\n📊 Ranking by Pareto Dominance...")
    ranks = analyzer.rank_by_dominance(agents)

    for rank, rank_agents in sorted(ranks.items()):
        print(f"\n🏆 Rank {rank} ({len(rank_agents)} agents):")
        for agent in rank_agents:
            print(
                f"  - {agent.agent_id}: "
                f"{agent.accuracy:.2%} acc, "
                f"{agent.energy_kwh*1000:.2f} Wh"
            )

    return agents, ranks


def demo_agent_comparison() -> Tuple["ParetoPoint", "ParetoPoint", Dict[str, Any]]:
    """Demo 3: Compare specific agents."""
    print("\n" + "=" * 60)
    print("DEMO 3: Agent-to-Agent Comparison")
    print("=" * 60)

    agent_a = ParetoPoint("high_accuracy", 0.95, 0.006, 0.0012, 250)
    agent_b = ParetoPoint("high_efficiency", 0.88, 0.002, 0.0004, 100)

    analyzer = ParetoFrontierAnalyzer()
    comparison = analyzer.compare_agents(agent_a, agent_b)

    print(f"\n🔍 Comparing {agent_a.agent_id} vs {agent_b.agent_id}")
    print(f"Relationship: {comparison['relationship']}")
    print(f"Explanation: {comparison['explanation']}")

    if "trade_offs" in comparison:
        print("\n📊 Trade-offs:")
        trade_offs = comparison["trade_offs"]
        print(f"  {agent_a.agent_id} better on: {trade_offs['a_better_on']}")
        print(f"  {agent_b.agent_id} better on: {trade_offs['b_better_on']}")

    return agent_a, agent_b, comparison


def demo_complexity_analysis() -> Tuple[Any, Any, Dict[str, Any]]:
    """Demo 4: Task complexity analysis."""
    print("\n" + "=" * 60)
    print("DEMO 4: Task Complexity Analysis")
    print("=" * 60)

    simple_trace = {
        "prompt": "What is 2+2?",
        "reasoning": ["Simple arithmetic"],
        "tool_calls": [],
        "execution_time_ms": 50,
        "context_tokens": 20,
    }
    complex_trace = {
        "prompt": "Analyze this Cinebench result and compare against similar CPUs...",
        "reasoning": [
            "Step 1: Parse benchmark results",
            "Step 2: Query database for similar CPUs",
            "Step 3: Compare performance metrics",
            "Step 4: Analyze power efficiency",
            "Step 5: Generate recommendation",
        ],
        "tool_calls": [
            {"tool": "benchmark_db"},
            {"tool": "cpu_specs_api"},
            {"tool": "power_calculator"},
        ],
        "execution_time_ms": 2500,
        "context_tokens": 1500,
    }

    analyzer = ComplexityAnalyzer()

    print("\n📝 Simple Task:")
    simple_complexity = analyzer.analyze_from_trace(simple_trace)
    print(f"  Prompt length: {simple_complexity.prompt_length} tokens")
    print(f"  Reasoning steps: {simple_complexity.reasoning_steps}")
    print(f"  Tool calls: {simple_complexity.tool_calls}")
    print(f"  Complexity score: {simple_complexity.compute_composite_score():.2f}")
    print(f"  Tier: {analyzer.categorize_complexity(simple_complexity)}")

    print("\n📝 Complex Task:")
    complex_complexity = analyzer.analyze_from_trace(complex_trace)
    print(f"  Prompt length: {complex_complexity.prompt_length} tokens")
    print(f"  Reasoning steps: {complex_complexity.reasoning_steps}")
    print(f"  Tool calls: {complex_complexity.tool_calls}")
    print(f"  Complexity score: {complex_complexity.compute_composite_score():.2f}")
    print(f"  Tier: {analyzer.categorize_complexity(complex_complexity)}")

    comparison = analyzer.compare_complexities(complex_complexity, simple_complexity)
    print(f"\n🔄 Complexity Difference:")
    print(f"  Score difference: {comparison['score_diff']:.2f}")
    print(f"  More complex: Task {comparison['more_complex']}")

    return simple_complexity, complex_complexity, comparison


def demo_over_reasoning_detection() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Demo 5: Detect over-reasoning agents."""
    print("\n" + "=" * 60)
    print("DEMO 5: Over-Reasoning Detection")
    print("=" * 60)

    analyzer = ComplexityAnalyzer()

    normal_trace = {
        "prompt": "Classify this image (100 tokens)",
        "reasoning": ["Step 1", "Step 2", "Step 3"],
        "tool_calls": [{"tool": "vision_api"}],
        "execution_time_ms": 1000,
        "context_tokens": 200,
    }
    excessive_trace = {
        "prompt": "Classify this image (100 tokens)",
        "reasoning": [f"Step {i}" for i in range(50)],
        "tool_calls": [{"tool": "vision_api"}],
        "execution_time_ms": 5000,
        "context_tokens": 1000,
    }

    print("\n✅ Normal Agent:")
    normal_complexity = analyzer.analyze_from_trace(normal_trace)
    normal_result = analyzer.detect_over_reasoning(normal_complexity)
    print(f"  Over-reasoning: {normal_result['over_reasoning']}")
    print(f"  Ratio: {normal_result['ratio']:.2f}")
    print(f"  {normal_result['recommendation']}")

    print("\n⚠️ Over-Reasoning Agent:")
    excessive_complexity = analyzer.analyze_from_trace(excessive_trace)
    excessive_result = analyzer.detect_over_reasoning(excessive_complexity)
    print(f"  Over-reasoning: {excessive_result['over_reasoning']}")
    print(f"  Ratio: {excessive_result['ratio']:.2f}")
    print(f"  {excessive_result['recommendation']}")

    return normal_result, excessive_result


def demo_cinebench_integration() -> Tuple[
    List["ParetoPoint"], List["ParetoPoint"], "ParetoFrontierAnalyzer"
]:
    """Demo 6: Cinebench classifier comparison."""
    print("\n" + "=" * 60)
    print("DEMO 6: Cinebench Classifier Comparison")
    print("=" * 60)

    classifiers = [
        ParetoPoint("ResNet50", 0.94, 0.008, 0.0016, 350),
        ParetoPoint("EfficientNet", 0.92, 0.003, 0.0006, 180),
        ParetoPoint("MobileNet", 0.86, 0.001, 0.0002, 80),
    ]

    analyzer = ParetoFrontierAnalyzer()
    frontier = analyzer.compute_frontier(classifiers)

    print("\n📊 Cinebench Classifier Analysis:")
    print(f"\nTotal models evaluated: {len(classifiers)}")
    print(f"Pareto-optimal models: {len(frontier)}")

    print("\n✨ Recommended models (on Pareto frontier):")
    for clf in frontier:
        acc_per_wh = clf.accuracy / (clf.energy_kwh * 1000)
        print(f"\n  {clf.agent_id}:")
        print(f"    Accuracy: {clf.accuracy:.2%}")
        print(f"    Energy: {clf.energy_kwh*1000:.2f} Wh/batch")
        print(f"    Accuracy per Wh: {acc_per_wh:.4f}")
        print(f"    Latency: {clf.latency_ms:.0f} ms")

    knee = analyzer.get_knee_point(frontier)
    print(f"\n🎯 RECOMMENDED FOR PRODUCTION: {knee.agent_id}")
    print(f"   Best balance between accuracy and efficiency")

    return classifiers, frontier, analyzer


# ============================================================================
# SECTION 8 — Demo 7: Enhanced Pareto analysis (rewritten)
# ============================================================================

def demo_enhanced_pareto_with_advanced_techniques(
    agents: Optional[Sequence["ParetoPoint"]] = None,
    analyzer: Optional["ParetoFrontierAnalyzer"] = None,
    *,
    rng: "np.random.Generator",
    initial_modp_weights: Optional[Sequence[float]] = None,
    human_feedback: float = 0.7,
    graph_metrics: Optional[Dict[str, float]] = None,
    population_size: int = 20,
    generations: int = 10,
    mutation_sigma: float = 0.05,
    crossover_rate: float = 0.7,
    elitism: int = 2,
) -> Dict[str, Any]:
    """
    Enhanced demo.

    What is actually computed:
      1. Normalize the 5-agent batch to a 4-vector (higher = better).
      2. Compute per-agent MoE scores with three DISTINCT teachers and a
         per-agent softmax gate.
      3. Evolve MODP weights with a tournament-selection GA over the
         simplex. The fitness is the maximin of the SAME composite used
         for ranking (0.6·MODP + 0.4·MoE).
      4. Rank agents by that composite and print a baseline comparison.
    """
    print_section("DEMO 7: Advanced Pareto Analysis with Enhancements")

    if not ENHANCED_AVAILABLE:
        print("Advanced demo requires numpy. Skipping.")
        return {"skipped": True}

    if agents is None:
        agents = _build_demo_agents()
    if analyzer is None:
        analyzer = ParetoFrontierAnalyzer()

    gm = dict(graph_metrics or {"centrality": 0.6, "connectivity": 0.5})
    print(f"\n🌐 LIMIT Graph Metrics: {gm}")
    print(f"👤 RLHF Feedback Score: {human_feedback} "
          f"(higher = prefers accuracy)")

    normalizer = AgentNormalizer()
    moe = MultiTeacherMoE(gm, human_feedback)

    # ---- 1. Precompute agent vectors and MoE scores -------------------
    vecs: Dict[str, "np.ndarray"] = {}
    moe_scores: Dict[str, float] = {}
    for a in agents:
        v = normalizer.to_vector(a)
        vecs[a.agent_id] = v
        moe_scores[a.agent_id] = moe.score(v)[0]

    # ---- 2. Frontier of the batch -------------------------------------
    frontier_ids = {a.agent_id for a in analyzer.compute_frontier(agents)}
    frontier_pairs: List[Tuple["np.ndarray", float]] = [
        (vecs[aid], moe_scores[aid]) for aid in frontier_ids
    ]
    print(f"\n📐 Frontier size: {len(frontier_pairs)} / {len(agents)}")

    # ---- 3. Declared MODP weights (the demo's own baseline) -----------
    if initial_modp_weights is None:
        initial_modp_weights = [0.4, 0.25, 0.2, 0.15]
    init_w = np.asarray(initial_modp_weights, dtype=np.float64)
    if init_w.shape != (4,):
        raise ValueError("initial_modp_weights must have 4 components")
    if (init_w < 0).any() or init_w.sum() <= 0:
        raise ValueError("initial_modp_weights must be non-negative, sum > 0")
    init_w = init_w / init_w.sum()
    print(f"\n📎 Initial MODP weights (accuracy, energy, carbon, latency):")
    print(f"   {np.round(init_w, 3)}")

    # ---- 4. Evolve MODP weights (seeded from init_w) ------------------
    print(
        f"\n🧬 Evolving MODP weights via GA "
        f"(population={population_size}, generations={generations})..."
    )
    optimizer = EvolutionaryWeightOptimizer(
        n_objectives=len(normalizer.FEATURE_NAMES),
        rng=rng,
        population_size=population_size,
        generations=generations,
        mutation_sigma=mutation_sigma,
        crossover_rate=crossover_rate,
        elitism=elitism,
    )
    # Seed the population with init_w + noise so the evolution is visible.
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

    # ---- 5. Rank with the SAME composite the GA optimised ------------
    ranking = _rank_agents(agents, analyzer, normalizer, moe, best_w)
    print("\n📊 Enhanced ranking (MODP + MoE, aligned with GA objective):")
    print(f"   {'agent':<16} {'MODP':>7} {'MoE':>7} {'Final':>7}  frontier")
    for r in ranking["rows"]:
        marker = "✓" if r["on_frontier"] else " "
        print(
            f"   {r['agent_id']:<16} {r['modp']:>7.3f} "
            f"{r['moe']:>7.3f} {r['composite']:>7.3f}  {marker}"
        )

    best_id = ranking["best_agent_id"]
    on_frontier = best_id in frontier_ids if best_id else False
    print(
        f"\n🏆 Best by composite: {best_id} "
        f"(on Pareto frontier: {'yes' if on_frontier else 'no'})"
    )

    # ---- 6. Baseline comparison --------------------------------------
    # Naive baseline: the highest-accuracy agent on the frontier. If the
    # enhanced ranking picks the same agent, the reader can see that the
    # enhancement agrees with the simple heuristic on this batch.
    frontier_agents = [a for a in agents if a.agent_id in frontier_ids]
    baseline = (
        max(frontier_agents, key=lambda a: a.accuracy).agent_id
        if frontier_agents else None
    )
    print(f"\n📎 Baseline (highest-accuracy frontier agent): {baseline}")
    if baseline is None:
        print("   → Frontier is empty; no baseline to compare.")
    elif baseline == best_id:
        print("   → Enhanced ranking agrees with the baseline on this batch.")
    else:
        print(
            f"   → Enhanced ranking chose a different agent than the baseline."
        )

    return {
        "best_agent_id": best_id,
        "baseline_agent_id": baseline,
        "on_frontier": on_frontier,
        "rows": ranking["rows"],
        "evolved_weights": best_w.tolist(),
        "initial_modp_weights": init_w.tolist(),
        "feature_names": list(normalizer.FEATURE_NAMES),
        "best_fitness": best_f,
    }


# ============================================================================
# SECTION 9 — CLI
# ============================================================================

def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Green_Agent Pareto Analysis Demo"
    )
    parser.add_argument(
        "--enhanced", action="store_true",
        help="Include the enhanced demo (Demo 7).",
    )
    parser.add_argument(
        "--only-enhanced", action="store_true",
        help="Skip demos 1–6 and run only the enhanced demo.",
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
        "--human-feedback", type=float, default=0.7,
        help="RLHF score in [0,1]; higher = prefers accuracy.",
    )
    parser.add_argument(
        "--centrality", type=float, default=0.6,
        help="Graph centrality metric in [0,1].",
    )
    parser.add_argument(
        "--connectivity", type=float, default=0.5,
        help="Graph connectivity metric in [0,1].",
    )
    parser.add_argument(
        "--initial-modp-weights",
        type=str, default="0.4,0.25,0.2,0.15",
        help="Comma-separated 4 values for (accuracy, energy, carbon, latency).",
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
    if not 0.0 <= args.centrality <= 1.0:
        raise SystemExit("--centrality must be in [0,1]")
    if not 0.0 <= args.connectivity <= 1.0:
        raise SystemExit("--connectivity must be in [0,1]")
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
# SECTION 10 — main
# ============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _validate_args(args)
    initial_modp_weights = _parse_modp_weights(args.initial_modp_weights)

    # Single RNG threaded through the whole demo.
    rng = np.random.default_rng(args.seed) if ENHANCED_AVAILABLE else None
    seed_display = args.seed if args.seed is not None else "entropy"

    print("\n" + "🌟" * 30)
    print("Green_Agent Pareto Analysis Demo")
    print("🌟" * 30)
    print(f"Seed: {seed_display}\n")

    agents: Optional[List["ParetoPoint"]] = None
    analyzer: Optional["ParetoFrontierAnalyzer"] = None

    if not args.only_enhanced:
        agents, _, analyzer = demo_basic_pareto()
        demo_pareto_ranking()
        demo_agent_comparison()
        demo_complexity_analysis()
        demo_over_reasoning_detection()
        demo_cinebench_integration()

    if args.enhanced or args.only_enhanced:
        demo_enhanced_pareto_with_advanced_techniques(
            agents=agents,
            analyzer=analyzer,
            rng=rng,
            initial_modp_weights=initial_modp_weights,
            human_feedback=args.human_feedback,
            graph_metrics={
                "centrality": args.centrality,
                "connectivity": args.connectivity,
            },
            population_size=args.population_size,
            generations=args.generations,
            mutation_sigma=args.mutation_sigma,
            crossover_rate=args.crossover_rate,
            elitism=args.elitism,
        )

    print("\n" + "=" * 60)
    print("✅ Demo complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Run tests: pytest tests/test_pareto_analysis.py")
    print("2. Integrate with your Green_Agent evaluation pipeline")
    print("3. Use Pareto analysis for AgentBeats submissions")
    print()

    return 0


if __name__ == '__main__':
    sys.exit(main())
