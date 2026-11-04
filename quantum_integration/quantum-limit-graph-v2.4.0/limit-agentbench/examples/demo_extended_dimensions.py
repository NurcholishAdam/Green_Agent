# -*- coding: utf-8 -*-
"""
Demo: Extended 7D Pareto Analysis with Memory, Circuit Depth, Variance
=====================================================================

Showcases:
1. Memory footprint analysis (edge deployment constraints)
2. Quantum circuit depth (scalability / fragility)
3. Inference variance (stability under repeated execution)
4. Three specialized 2D plots (policy-oriented visualizations)

Enhanced Demo 7 (enabled with --enhanced):
  - LIMIT Graph metrics (centrality, connectivity, density)
  - MODP (multi-objective weighted linear aggregate)
  - RLHF (human-feedback-driven weighting)
  - Multi-Teacher Distillation + per-agent MoE gating
  - Bio-inspired optimisation (tournament-selection GA over the simplex)

What Demo 7 actually does (and does NOT claim to do):
  - The GA optimises the SAME composite used for the final ranking
    (a maximin over frontier agents of 0.6*MODP + 0.4*MoE). The v1 demo
    optimised MODP alone but ranked by MODP+MoE — they were disconnected.
  - The MoE gate is a per-agent softmax over three distinct teachers.
    v1 used a fixed [0.4, 0.4, 0.2] vector and 2/3 of its "teachers"
    returned the same value for every agent.
  - Classical agents receive a NEUTRAL depth score (0.5), not the
    maximum (1.0). v1 penalised quantum agents for being quantum.

Fixes applied vs. v1
--------------------
  P0  GA had no selection pressure (uniform parents)      -> tournament (k=3)
  P0  Fitness had no gradient (fixed frontier)            -> maximin over frontier
  P0  Classical agents got perfect depth (0 -> 1.0)       -> 0 -> neutral (0.5)
  P0  MoE gate was a fixed array                          -> per-agent softmax
  P0  2/3 teachers returned identical values              -> distinct per agent
  P0  GA objective != ranking objective                   -> aligned
  P0  Module-level ParetoPlotter import                   -> lazy import
  P0  input() blocked CI                                  -> --no-pause flag
  P0  Unseeded RNG                                        -> --seed flag
  P1  Hard-coded normalization ceilings                   -> 1/(1+q/scale)
  P1  variance_score not clipped                          -> _clamp
  P1  Off-by-one best_weights tracking                    -> global best
  P1  No CLI config for population / generations          -> flags
  P2  Dead CLI surface                                    -> all flags wired

Run with:
    python examples/demo_extended_dimensions.py                # demos 1-6
    python examples/demo_extended_dimensions.py --enhanced     # + demo 7
    python examples/demo_extended_dimensions.py --enhanced --no-pause --seed 42

Preserved public surface
------------------------
  print_section, demo_7d_pareto_analysis, demo_memory_analysis,
  demo_circuit_depth_analysis, demo_variance_stability,
  demo_comprehensive_analysis, demo_specialized_plots,
  demo_enhanced_analysis_with_advanced_techniques, main
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

# Add src to path (preserved for parity with v1).
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.extended_pareto_analyzer import ExtendedParetoPoint, ExtendedParetoAnalyzer


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
    """1/(1 + q/scale) — bounded on (0, 1], scale-invariant."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _pause(message: str, no_pause: bool) -> None:
    """Interactive prompt that degrades gracefully in CI."""
    if no_pause:
        return
    try:
        input(message)
    except EOFError:
        # Non-interactive stdin — silently continue.
        pass


def print_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


# ============================================================================
# SECTION 2 — Feature normalizer (single source of truth)
# ============================================================================

class AgentNormalizer:
    """
    Converts an ExtendedParetoPoint into a 7-vector where higher = better
    for every component. Used by both the GA and the MoE so that the
    fitness and the reported ranking are computed on the same features.

    Classical agents (circuit_depth == 0) receive NEUTRAL_DEPTH (0.5),
    not the maximum. v1 mapped 0 -> 1.0, systematically penalising
    quantum agents.
    """

    FEATURE_NAMES: Tuple[str, ...] = (
        "accuracy", "energy", "carbon", "latency", "memory", "depth", "variance",
    )
    NEUTRAL_DEPTH: float = 0.5

    def __init__(self, scales: Optional[Mapping[str, float]] = None) -> None:
        self.scales: Dict[str, float] = {
            "energy_wh": 10.0,
            "carbon_g": 2.0,
            "latency_ms": 500.0,
            "memory_mb": 2048.0,
            "depth": 50.0,
        }
        if scales:
            self.scales.update({k: float(v) for k, v in scales.items()})

    def to_vector(self, agent: "ExtendedParetoPoint") -> np.ndarray:
        energy_wh = max(float(agent.energy_kwh), 0.0) * 1000.0
        carbon_g = max(float(agent.carbon_co2e_kg), 0.0) * 1000.0

        if getattr(agent, "circuit_depth", 0) == 0:
            depth_score = self.NEUTRAL_DEPTH
        else:
            depth_score = _bounded_inverse(
                agent.circuit_depth, self.scales["depth"]
            )

        return np.array(
            [
                _clamp(agent.accuracy),
                _bounded_inverse(energy_wh, self.scales["energy_wh"]),
                _bounded_inverse(carbon_g, self.scales["carbon_g"]),
                _bounded_inverse(agent.latency_ms, self.scales["latency_ms"]),
                _bounded_inverse(agent.memory_mb, self.scales["memory_mb"]),
                depth_score,
                1.0 - _clamp(agent.variance_score),
            ],
            dtype=np.float64,
        )


# ============================================================================
# SECTION 3 — Multi-Teacher MoE with per-agent softmax gating
# ============================================================================

class MultiTeacherMoE:
    """
    Per-agent gating over three teachers. Each teacher produces a DIFFERENT
    value for each agent (v1's rule and RLHF teachers both returned
    agent.accuracy for every agent, collapsing the "MoE" to 94% accuracy).

    Teachers:
      - Rule:       accuracy if centrality > 0.5, else efficiency.
      - RLHF:       human_feedback * accuracy + (1 - h) * efficiency.
      - Historical: fixed linear blend of normalized features.

    Gate: softmax over logits derived from the agent's own feature vector,
    so different agents are weighted differently.
    """

    N_TEACHERS = 3

    def __init__(
        self,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        self.centrality = _clamp(graph_metrics.get("centrality", 0.5))
        self.connectivity = _clamp(graph_metrics.get("connectivity", 0.5))
        self.human_feedback = _clamp(human_feedback)

    def _rule_teacher(self, vec: np.ndarray) -> float:
        if self.centrality > 0.5:
            return float(vec[0])            # accuracy-heavy
        return float(vec[1])                # efficiency-heavy

    def _rlhf_teacher(self, vec: np.ndarray) -> float:
        h = self.human_feedback
        return float(h * vec[0] + (1.0 - h) * vec[1])

    def _historical_teacher(self, vec: np.ndarray) -> float:
        return float(0.7 * vec[0] + 0.3 * vec[1])

    def _gate_logits(self, vec: np.ndarray) -> np.ndarray:
        h = self.human_feedback
        return np.array(
            [
                vec[0] * 1.5 + self.centrality,   # rule teacher: centrality bias
                vec[0] * h + vec[1] * (1.0 - h),  # RLHF: preference bias
                1.0,                              # historical: neutral
            ],
            dtype=np.float64,
        )

    def score(self, vec: np.ndarray) -> Tuple[float, np.ndarray, np.ndarray]:
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
# SECTION 4 — Evolutionary weight optimizer (real GA)
# ============================================================================

class EvolutionaryWeightOptimizer:
    """
    Real GA over the probability simplex.

    Selection:   tournament (k=3) with elitism (top-`elitism` retained).
    Crossover:   uniform random interpolation: alpha*p1 + (1-alpha)*p2.
    Mutation:    Gaussian noise, renormalised. NOT Dirichlet (a uniform
                 simplex sample is not a small perturbation).
    Fitness:     maximin over frontier agents of the FINAL composite
                 (0.6 * MODP + 0.4 * MoE). This aligns the GA objective
                 with what the demo actually ranks by.

    `frontier_pairs` is a sequence of (vec, moe_score) tuples for the
    frontier agents. MoE is precomputed because it does not depend on
    the GA's weights.
    """

    def __init__(
        self,
        n_objectives: int,
        rng: np.random.Generator,
        population_size: int = 20,
        generations: int = 15,
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

    def _composite(self, w: np.ndarray, vec: np.ndarray, moe: float) -> float:
        return self.modp_weight * float(w @ vec) + self.moe_weight * float(moe)

    def _fitness(
        self, w: np.ndarray, frontier_pairs: Sequence[Tuple[np.ndarray, float]]
    ) -> float:
        if not frontier_pairs:
            return 0.0
        # Maximin: reward the WORST frontier agent's composite. This forces
        # the evolved weights to be good for the whole frontier, not just
        # the average. (v1's fitness was linear in w, so the maximum was a
        # one-hot vertex of the simplex.)
        return float(min(self._composite(w, vec, moe) for vec, moe in frontier_pairs))

    def _tournament(
        self,
        population: Sequence[np.ndarray],
        scores: Sequence[float],
        k: int = 3,
    ) -> np.ndarray:
        size = min(k, len(population))
        idxs = self.rng.choice(len(population), size=size, replace=False)
        best = max((int(i) for i in idxs), key=lambda i: scores[i])
        return population[best].copy()

    def _crossover(self, a: np.ndarray, b: np.ndarray) -> np.ndarray:
        if self.rng.random() < self.crossover_rate:
            alpha = float(self.rng.random())
            return alpha * a + (1.0 - alpha) * b
        return a.copy()

    def _mutate(self, w: np.ndarray) -> np.ndarray:
        w = w + self.rng.normal(0.0, self.mutation_sigma, self.n)
        w = np.clip(w, 1e-6, None)
        return w / w.sum()

    def evolve(
        self,
        frontier_pairs: Sequence[Tuple[np.ndarray, float]],
        *,
        verbose: bool = True,
    ) -> Tuple[np.ndarray, float]:
        population: List[np.ndarray] = [
            self.rng.dirichlet(np.ones(self.n))
            for _ in range(self.population_size)
        ]
        best_w = population[0].copy()
        best_f = -math.inf

        for gen in range(self.generations):
            scores = [self._fitness(w, frontier_pairs) for w in population]

            # Track the global best across ALL generations (fixes v1's
            # off-by-one where the previous generation's best was reported).
            for w, s in zip(population, scores):
                if s > best_f:
                    best_f = s
                    best_w = w.copy()

            if verbose and (gen % 5 == 0 or gen == self.generations - 1):
                print(f"   gen {gen:02d}: best_fitness = {best_f:.4f}")

            order = np.argsort(scores)[::-1]
            elite = [population[int(i)].copy() for i in order[: self.elitism]]
            new_pop: List[np.ndarray] = list(elite)
            while len(new_pop) < self.population_size:
                p1 = self._tournament(population, scores)
                p2 = self._tournament(population, scores)
                child = self._crossover(p1, p2)
                child = self._mutate(child)
                new_pop.append(child)
            population = new_pop

        return best_w, best_f


# ============================================================================
# SECTION 5 — Enhanced ranker (aligned with GA objective)
# ============================================================================

def _enhanced_ranking(
    agents: Sequence["ExtendedParetoPoint"],
    analyzer: "ExtendedParetoAnalyzer",
    normalizer: AgentNormalizer,
    moe: MultiTeacherMoE,
    best_w: np.ndarray,
    modp_weight: float = 0.6,
    moe_weight: float = 0.4,
) -> Dict[str, Any]:
    """
    Compute the final composite for every agent using the SAME objective
    the GA optimised. Returns a dict with per-agent scores and the ranked
    list. The frontier intersection is reported explicitly so callers can
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
# SECTION 6 — Demo 7: Enhanced analysis (rewritten)
# ============================================================================

def demo_enhanced_analysis_with_advanced_techniques(
    agents: List["ExtendedParetoPoint"],
    analyzer: "ExtendedParetoAnalyzer",
    *,
    rng: np.random.Generator,
    human_feedback: float = 0.8,
    graph_metrics: Optional[Dict[str, float]] = None,
    population_size: int = 20,
    generations: int = 15,
    mutation_sigma: float = 0.05,
    crossover_rate: float = 0.7,
    elitism: int = 2,
) -> Dict[str, Any]:
    """
    Enhanced demo.

    What's actually computed:
      1. Normalize agents to a 7-vector (higher = better).
      2. Compute per-agent MoE scores (per-agent softmax over 3 distinct
         teachers).
      3. Evolve MODP weights with a tournament-selection GA whose fitness
         is the maximin of the SAME composite used for ranking.
      4. Rank agents by that composite; report frontier intersection.
    """
    print_section(
        "DEMO 7: Enhanced Analysis with LIMIT Graph, MODP, RLHF, "
        "Distillation, MoE, Bio-inspired"
    )

    if not agents:
        print("⚠️  No agents supplied — nothing to rank.")
        return {"best_agent_id": None, "rows": []}

    gm = dict(graph_metrics or {"centrality": 0.7, "connectivity": 0.6, "density": 0.5})
    print(f"🌐 LIMIT Graph Metrics: {gm}")
    print(f"👤 RLHF Feedback Score: {human_feedback} "
          f"(higher = prefers accuracy)")

    normalizer = AgentNormalizer()
    moe = MultiTeacherMoE(gm, human_feedback)

    # ---- 1. Precompute vectors and MoE scores -------------------------
    vecs: Dict[str, np.ndarray] = {}
    moe_scores: Dict[str, float] = {}
    for a in agents:
        v = normalizer.to_vector(a)
        vecs[a.agent_id] = v
        moe_scores[a.agent_id] = moe.score(v)[0]

    # ---- 2. Frontier (fixed for this batch; depends only on the data) --
    frontier_ids = {a.agent_id for a in analyzer.compute_frontier(agents)}
    frontier_pairs: List[Tuple[np.ndarray, float]] = [
        (vecs[aid], moe_scores[aid]) for aid in frontier_ids
    ]
    print(f"\n📐 Frontier size: {len(frontier_pairs)} / {len(agents)}")

    # ---- 3. Evolve MODP weights ---------------------------------------
    print("\n🧬 Evolving MODP weights via GA "
          f"(population={population_size}, generations={generations})...")
    optimizer = EvolutionaryWeightOptimizer(
        n_objectives=AgentNormalizer.__dict__ and len(normalizer.FEATURE_NAMES),
        rng=rng,
        population_size=population_size,
        generations=generations,
        mutation_sigma=mutation_sigma,
        crossover_rate=crossover_rate,
        elitism=elitism,
    )
    best_w, best_f = optimizer.evolve(frontier_pairs, verbose=True)
    print(f"\n   Evolved weights {normalizer.FEATURE_NAMES}:")
    print(f"   {np.round(best_w, 3)}")
    print(f"   Best fitness (maximin composite over frontier): {best_f:.4f}")

    # ---- 4. Rank agents with the SAME objective -----------------------
    ranking = _enhanced_ranking(agents, analyzer, normalizer, moe, best_w)
    print("\n📊 Enhanced ranking (MODP + MoE, aligned with GA objective):")
    print(f"   {'agent':<18} {'MODP':>7} {'MoE':>7} {'Final':>7}  frontier")
    for r in ranking["rows"]:
        marker = "✓" if r["on_frontier"] else " "
        print(
            f"   {r['agent_id']:<18} {r['modp']:>7.3f} "
            f"{r['moe']:>7.3f} {r['composite']:>7.3f}  {marker}"
        )

    best = ranking["best_agent_id"]
    on_frontier = best in frontier_ids if best else False
    print(
        f"\n🏆 Best by composite: {best} "
        f"(on Pareto frontier: {'yes' if on_frontier else 'no'})"
    )

    # ---- 5. Baseline comparison --------------------------------------
    # Show what the plain Pareto frontier would recommend vs. the enhanced
    # composite. If they agree, the enhancement didn't change anything on
    # this batch (which is a legitimate, informative finding).
    baseline_best = None
    if frontier_ids:
        # v1-style baseline: pick the highest-accuracy frontier agent.
        frontier_agents = [a for a in agents if a.agent_id in frontier_ids]
        baseline_best = max(frontier_agents, key=lambda a: a.accuracy).agent_id
        print(
            f"\n📎 Baseline (highest-accuracy frontier agent): {baseline_best}"
        )
        if baseline_best == best:
            print("   → Enhanced ranking agrees with the baseline on this batch.")
        else:
            print("   → Enhanced ranking chose a different agent than the baseline.")

    return {
        "best_agent_id": best,
        "baseline_agent_id": baseline_best,
        "on_frontier": on_frontier,
        "rows": ranking["rows"],
        "evolved_weights": best_w.tolist(),
        "feature_names": list(normalizer.FEATURE_NAMES),
        "best_fitness": best_f,
    }


# ============================================================================
# SECTION 7 — Base demos (1–6, preserved from v1)
# ============================================================================

def _build_agents() -> List["ExtendedParetoPoint"]:
    """The canonical agent batch used by demos 1–6."""
    return [
        ExtendedParetoPoint(
            agent_id='ResNet50', accuracy=0.94, energy_kwh=0.008,
            carbon_co2e_kg=0.0016, latency_ms=350, memory_mb=512,
            circuit_depth=0, variance_score=0.05,
        ),
        ExtendedParetoPoint(
            agent_id='EfficientNet', accuracy=0.92, energy_kwh=0.003,
            carbon_co2e_kg=0.0006, latency_ms=180, memory_mb=256,
            circuit_depth=0, variance_score=0.08,
        ),
        ExtendedParetoPoint(
            agent_id='MobileNet', accuracy=0.86, energy_kwh=0.001,
            carbon_co2e_kg=0.0002, latency_ms=80, memory_mb=128,
            circuit_depth=0, variance_score=0.12,
        ),
        ExtendedParetoPoint(
            agent_id='QuantumHybrid', accuracy=0.91, energy_kwh=0.004,
            carbon_co2e_kg=0.0008, latency_ms=500, memory_mb=384,
            circuit_depth=25, variance_score=0.25,
        ),
        ExtendedParetoPoint(
            agent_id='FastButUnstable', accuracy=0.89, energy_kwh=0.005,
            carbon_co2e_kg=0.0010, latency_ms=100, memory_mb=300,
            circuit_depth=0, variance_score=0.35,
        ),
        ExtendedParetoPoint(
            agent_id='MemoryHog', accuracy=0.96, energy_kwh=0.010,
            carbon_co2e_kg=0.0020, latency_ms=400, memory_mb=2048,
            circuit_depth=0, variance_score=0.06,
        ),
    ]


def demo_7d_pareto_analysis() -> Tuple[
    List["ExtendedParetoPoint"],
    List["ExtendedParetoPoint"],
    "ExtendedParetoAnalyzer",
]:
    """Demo 1: 7-dimensional Pareto analysis."""
    print_section("DEMO 1: 7D Pareto Analysis")
    print("Scenario: Compare Cinebench classifiers with ALL dimensions\n")

    agents = _build_agents()
    analyzer = ExtendedParetoAnalyzer()
    frontier = analyzer.compute_frontier(agents)

    print("📊 7D Pareto Frontier Results:")
    print(f"   Total agents: {len(agents)}")
    print(f"   Frontier size: {len(frontier)}")
    print("\n✨ Agents on 7D frontier:")
    for agent in frontier:
        print(f"      • {agent.agent_id}")

    print("\n❌ Dominated agents:")
    frontier_ids = {a.agent_id for a in frontier}
    dominated = [a for a in agents if a.agent_id not in frontier_ids]
    for agent in dominated:
        print(f"      • {agent.agent_id}")

    return agents, frontier, analyzer


def demo_memory_analysis(
    agents: List["ExtendedParetoPoint"], analyzer: "ExtendedParetoAnalyzer"
) -> None:
    """Demo 2: Memory constraint analysis."""
    print_section("DEMO 2: Memory Footprint Analysis")
    print("Scenario: Edge device with 512 MB RAM limit\n")

    memory_analysis = analyzer.analyze_memory_constraint(agents, max_memory_mb=512)
    print(f"Memory Constraint: {memory_analysis['max_memory_mb']} MB\n")

    print(f"✅ Feasible agents ({memory_analysis['feasible_count']}):")
    for agent in memory_analysis['feasible']:
        eff = memory_analysis['memory_efficiency'][agent.agent_id]
        print(f"   • {agent.agent_id}: {agent.memory_mb:.0f} MB "
              f"(efficiency: {eff:.4f} accuracy/MB)")

    print(f"\n❌ Infeasible agents ({memory_analysis['infeasible_count']}):")
    for agent in memory_analysis['infeasible']:
        print(f"   • {agent.agent_id}: {agent.memory_mb:.0f} MB - TOO LARGE FOR EDGE!")

    print(f"\n🏆 Most memory-efficient: {memory_analysis['best_memory_efficient']}")
    print(f"\n📊 Frontier of feasible agents: "
          f"{len(memory_analysis['frontier_feasible'])}")
    for agent in memory_analysis['frontier_feasible']:
        print(f"   • {agent.agent_id} - Deployable on edge with "
              f"{agent.accuracy:.1%} accuracy")

    print("\n💡 Key Insight:")
    print("   Memory is a HARD constraint on edge devices.")
    print("   High-accuracy agents may be useless if they don't fit in RAM!")


def demo_circuit_depth_analysis(
    agents: List["ExtendedParetoPoint"], analyzer: "ExtendedParetoAnalyzer"
) -> None:
    """Demo 3: Quantum circuit depth analysis."""
    print_section("DEMO 3: Quantum Circuit Depth Analysis")
    print("Scenario: Analyze quantum/hybrid agent scalability\n")

    circuit_analysis = analyzer.analyze_circuit_depth_scalability(agents)

    if circuit_analysis.get('quantum_agents_count', 0) == 0:
        print("⚠️  No quantum agents in this batch (all classical)")
        return

    print(f"🔬 Quantum Agents: {circuit_analysis['quantum_agents_count']}")
    print("\n📊 Circuit Depth Statistics:")
    stats = circuit_analysis['depth_stats']
    print(f"   Mean: {stats['mean']:.1f}")
    print(f"   Median: {stats['median']:.1f}")
    print(f"   Range: {stats['min']:.0f} - {stats['max']:.0f}")
    print(f"   Std Dev: {stats['std']:.1f}")

    print("\n📈 Correlations:")
    corr = circuit_analysis['correlations']
    print(f"   Accuracy vs Depth: {corr['accuracy_vs_depth']:.3f}")
    print(f"   Energy vs Depth: {corr['energy_vs_depth']:.3f}")

    print("\n🌟 Shallow Circuit Agents (depth < median):")
    for agent_id in circuit_analysis['shallow_circuit_agents']:
        print(f"   • {agent_id} - Better scalability")

    print("\n⚠️  Fragility Scores (depth/accuracy - lower is better):")
    fragility = circuit_analysis['fragility_scores']
    for agent_id, score in sorted(fragility.items(), key=lambda x: x[1]):
        print(f"   • {agent_id}: {score:.2f}")

    print(f"\n🏆 Most robust: {circuit_analysis['most_robust']}")
    print(f"⚠️  Most fragile: {circuit_analysis['most_fragile']}")

    print("\n💡 Key Insight:")
    print("   Circuit depth predicts quantum noise and decoherence.")
    print("   Shallow circuits are more robust and scalable!")


def demo_variance_stability(
    agents: List["ExtendedParetoPoint"], analyzer: "ExtendedParetoAnalyzer"
) -> None:
    """Demo 4: Inference variance analysis."""
    print_section("DEMO 4: Inference Variance & Stability")
    print("Scenario: Production deployment requires predictability\n")

    variance_analysis = analyzer.analyze_variance_stability(
        agents, stability_threshold=0.2
    )
    print(f"Stability Threshold: {variance_analysis['stability_threshold']}\n")

    print(f"✅ Stable agents ({variance_analysis['stable_count']}):")
    for agent in variance_analysis['stable']:
        cost = variance_analysis['variance_cost'][agent.agent_id]
        print(f"   • {agent.agent_id}: σ={agent.variance_score:.3f} "
              f"(P95 energy cost: +{cost*1000:.2f} Wh)")

    print(f"\n⚠️  Unstable agents ({variance_analysis['unstable_count']}):")
    for agent in variance_analysis['unstable']:
        cost = variance_analysis['variance_cost'][agent.agent_id]
        print(f"   • {agent.agent_id}: σ={agent.variance_score:.3f} - RISKY! "
              f"(P95 energy cost: +{cost*1000:.2f} Wh)")

    print("\n📊 Stability Ranking (most → least stable):")
    for i, agent_id in enumerate(variance_analysis['stability_ranking'][:3]):
        print(f"   {i+1}. {agent_id}")

    print(f"\n🏆 Most stable: {variance_analysis['most_stable']}")
    print(f"⚠️  Least stable: {variance_analysis['least_stable']}")

    print("\n💡 Key Insight:")
    print("   High variance = unpredictable energy spikes")
    print("   → Breaks SLAs, violates carbon caps intermittently")
    print("   → Less green in practice!")


def demo_comprehensive_analysis(
    agents: List["ExtendedParetoPoint"], analyzer: "ExtendedParetoAnalyzer"
) -> None:
    """Demo 5: Comprehensive analysis with all constraints."""
    print_section("DEMO 5: Comprehensive 7D Analysis with Constraints")
    print("Scenario: Production deployment with ALL constraints\n")

    constraints = {
        'max_memory_mb': 512,
        'max_circuit_depth': 50,
        'max_variance': 0.2,
    }
    print("🎯 Deployment Constraints:")
    for key, value in constraints.items():
        print(f"   • {key}: {value}")

    comprehensive = analyzer.comprehensive_analysis(agents, constraints)

    print("\n📊 Analysis Results:")
    print(f"   Total agents: {comprehensive['total_agents']}")
    print(f"   7D frontier: {comprehensive['frontier_7d_count']} agents")
    print(f"   Fully compliant: {comprehensive['fully_compliant_count']} agents")
    print(f"   Compliant frontier: {comprehensive['frontier_compliant_count']} agents")

    if comprehensive['recommendation']:
        print("\n🏆 RECOMMENDED FOR PRODUCTION:")
        print(f"   {comprehensive['recommendation']}")
        print("   → Satisfies ALL constraints")
        print("   → On Pareto frontier")
        print("   → Production-ready!")
    else:
        print("\n❌ NO AGENT SATISFIES ALL CONSTRAINTS!")
        print("   → Relax constraints or redesign agents")

    print("\n💡 Key Insight:")
    print("   Real deployment has MULTIPLE hard constraints.")
    print("   7D Pareto analysis finds agents that satisfy them all!")


def demo_specialized_plots(
    agents: List["ExtendedParetoPoint"],
    frontier: List["ExtendedParetoPoint"],
    output_dir: Path = Path("."),
) -> None:
    """Demo 6: Three specialized 2D plots.

    ParetoPlotter is imported lazily so this demo runs even when plotly
    isn't installed.
    """
    print_section("DEMO 6: Specialized 2D Policy Plots")
    print("Why multiple 2D plots instead of one 7D plot?\n")
    print("   • Humans cannot reason in 7D")
    print("   • Each 2D plot answers a different policy question")
    print("   • Projections reveal different trade-offs\n")

    try:
        from visualization.pareto_plotter import ParetoPlotter  # noqa: WPS433
    except ImportError as exc:
        print("⚠️  Visualization requires plotly:")
        print("   pip install plotly")
        print(f"\n   Error: {exc}")
        return

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        plotter = ParetoPlotter(backend='plotly')
        print("📊 Generating three specialized plots...\n")

        print("1️⃣  Accuracy vs Carbon")
        print("   Question: 'What performance per unit environmental cost?'")
        print("   Users: Sustainability reviewers, ESG officers")
        plotter.plot_accuracy_vs_carbon(
            agents, frontier,
            save_path=str(output_dir / 'accuracy_vs_carbon.html'),
        )
        print(f"   ✅ Saved to: {output_dir / 'accuracy_vs_carbon.html'}\n")

        print("2️⃣  Latency vs Energy")
        print("   Question: 'Are fast agents inherently wasteful?'")
        print("   Users: Systems engineers, edge teams")
        plotter.plot_latency_vs_energy(
            agents, frontier,
            save_path=str(output_dir / 'latency_vs_energy.html'),
        )
        print(f"   ✅ Saved to: {output_dir / 'latency_vs_energy.html'}\n")

        print("3️⃣  Carbon vs Energy (Pure Green!)")
        print("   Question: 'Which agents are environmentally efficient?'")
        print("   Users: Green AI researchers, carbon planners")
        plotter.plot_carbon_vs_energy(
            agents, frontier,
            save_path=str(output_dir / 'carbon_vs_energy.html'),
        )
        print(f"   ✅ Saved to: {output_dir / 'carbon_vs_energy.html'}\n")

        print("🎨 All plots saved! Open in browser to interact.\n")
        print("💡 Key Insight:")
        print("   Each plot shows a different 'face' of the Pareto frontier.")
        print("   An agent can look excellent in one projection,")
        print("   but poor in another - that's not a bug, it's insight!")
    except ImportError as exc:
        print("⚠️  Visualization requires plotly:")
        print("   pip install plotly")
        print(f"\n   Error: {exc}")


# ============================================================================
# SECTION 8 — CLI
# ============================================================================

def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extended 7D Pareto Analysis Demo"
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
        "--no-pause", action="store_true",
        help="Never block on Enter (for CI / headless runs).",
    )
    parser.add_argument(
        "--no-plots", action="store_true",
        help="Skip Demo 6 (useful when plotly is unavailable).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("."),
        help="Directory to write plot HTML files (default: .).",
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
        "--generations", type=int, default=15,
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
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> None:
    if not 0.0 <= args.human_feedback <= 1.0:
        raise SystemExit("--human-feedback must be in [0,1]")
    for name in ("centrality", "connectivity", "density"):
        v = getattr(args, name)
        if not 0.0 <= v <= 1.0:
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
# SECTION 9 — main
# ============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _validate_args(args)

    # Single RNG threaded through the whole demo.
    rng = np.random.default_rng(args.seed)
    seed_display = args.seed if args.seed is not None else "entropy"
    print("\n" + "🌟" * 35)
    print("Green_Agent: Extended 7D Pareto Analysis")
    print("🌟" * 35)
    print(f"Seed: {seed_display}")

    print("\nNew Dimensions:")
    print("  1. Memory Footprint (MB) - Edge deployment constraints")
    print("  2. Circuit Depth - Quantum scalability/fragility")
    print("  3. Variance Score - Stability/predictability")
    print("\nNew Plots:")
    print("  1. Accuracy vs Carbon - Sustainability view")
    print("  2. Latency vs Energy - Systems engineering view")
    print("  3. Carbon vs Energy - Pure green efficiency\n")

    _pause("Press Enter to start demos...", args.no_pause)

    agents, frontier, analyzer = demo_7d_pareto_analysis()

    if not args.only_enhanced:
        _pause("\nPress Enter for next demo...", args.no_pause)
        demo_memory_analysis(agents, analyzer)

        _pause("\nPress Enter for next demo...", args.no_pause)
        demo_circuit_depth_analysis(agents, analyzer)

        _pause("\nPress Enter for next demo...", args.no_pause)
        demo_variance_stability(agents, analyzer)

        _pause("\nPress Enter for next demo...", args.no_pause)
        demo_comprehensive_analysis(agents, analyzer)

        if not args.no_plots:
            _pause("\nPress Enter for final demo...", args.no_pause)
            demo_specialized_plots(agents, frontier, output_dir=args.output_dir)

    if args.enhanced:
        _pause("\nPress Enter for enhanced demo...", args.no_pause)
        demo_enhanced_analysis_with_advanced_techniques(
            agents, analyzer,
            rng=rng,
            human_feedback=args.human_feedback,
            graph_metrics={
                "centrality": args.centrality,
                "connectivity": args.connectivity,
                "density": args.density,
            },
            population_size=args.population_size,
            generations=args.generations,
            mutation_sigma=args.mutation_sigma,
            crossover_rate=args.crossover_rate,
            elitism=args.elitism,
        )

    print("\n" + "=" * 70)
    print("✅ All Demos Complete!")
    print("=" * 70)

    print("\n📈 Key Takeaways:")
    print("   1. Memory is a HARD constraint for edge deployment")
    print("   2. Circuit depth predicts quantum fragility")
    print("   3. Variance matters - unpredictable = less green")
    print("   4. Multiple 2D plots > one 7D plot for policy decisions")
    print("   5. Each dimension captures different failure modes\n")

    if args.enhanced:
        print("   6. Demo 7 evolves MODP weights and ranks by the SAME")
        print("      objective it optimises. Classical agents get a")
        print("      neutral depth (0.5), not the maximum.\n")

    print("Next steps:")
    print("   • Open the HTML plots in your browser")
    print("   • Integrate with your Cinebench pipeline")
    print("   • Test with real quantum/hybrid agents")
    print("   • Use for AgentBeats submission\n")

    return 0


if __name__ == '__main__':
    sys.exit(main())
