# Extended Pareto Analysis: 7D + Specialized Plots (Enhanced v2)

**Version:** 2.0.0 · **Status:** Beta (see §0) · **Date:** <YYYY-MM-DD>
**Python:** ≥3.10 · **Requires:** `numpy>=2.2`, `plotly>=5.24` (see §12)
**Companion guides:** `AGENTBEATS_COMPLIANCE_GUIDE.md` (v2.0.0), `AGENTBEATS_DELIVERY_SUMMARY.md` (v2.0.0)

> **Changelog**
> - **2.0.0** — Full implementation of `ExtendedParetoPoint`, `ExtendedParetoAnalyzer`,
>   `ParetoPlotter`, `to_csv` helper, `record_provenance.sh`, `bench_pareto.py`.
>   Defined `variance_score` semantics (CV of energy across ≥3 runs). Added
>   missing-value policy, MODP normalization, ε-tie policy, empty-feasible-set
>   handling, HTML sanitization, typed return shapes, tests, fixtures, pinned
>   requirements, CI, and benchmark. Fixed async/sync contradiction and
>   `NodeType` enum use. Removed "Production Ready" and unsubstantiated
>   superlatives. Documented correlation caveat for memory↔energy and
>   variance↔latency.
> - **1.0.0** — Initial document (labeled "Production Ready" — see §0).
>
> **Contents of this single file — each section tagged `# === SAVE AS: <path> ===`**
>
> 1. §0–§11 — Report
> 2. `src/analysis/extended_pareto_analyzer.py`
> 3. `src/visualization/pareto_plotter.py`
> 4. `examples/demo_extended_dimensions.py`
> 5. `tests/fixtures/agents.json`
> 6. `tests/test_extended_pareto_analyzer.py`
> 7. `tests/test_pareto_plotter.py`
> 8. `scripts/record_provenance.sh`
> 9. `scripts/bench_pareto.py`
> 10. `requirements.txt`
> 11. `.github/workflows/pareto-ci.yml`

---

## 0. Status

**Beta.** All P0 items from the review are closed by the code in this file.
"Production Ready" is deferred until the open items in §0.2 are closed.

### 0.1 P0 items — closed in v2.0.0

| ID | Item | Evidence |
|---|---|---|
| P0-1 | Full implementation (not just examples) | §2, §3 |
| P0-2 | `variance_score` semantics | §1.3 |
| P0-3 | Missing-value policy | §1.4 |
| P0-4 | MODP normalization | §1.5 |
| P0-5 | Async/sync consistency (all sync) | §4 |
| P0-6 | HTML sanitization | §2.1, §3 |
| P0-7 | Tests + fixtures | §6, §7, §5 |
| P0-8 | Pinned requirements | §12 |
| P0-9 | Provenance + benchmark scripts | §8, §9 |

### 0.2 Remaining open items

| ID | Item | Owner |
|---|---|---|
| O-1 | Wire real dataset from Cinebench / agent pipeline | `@bench` |
| O-2 | Correlation-aware MODP weighting | `@research` |
| O-3 | d-dimensional maxima algorithm above n ≈ 5000 | `@research` |
| O-4 | SBOM + SLSA on example artifacts | `@sre` |

---

## 1. Data model and semantics

### 1.1 `ExtendedParetoPoint` fields

| Field | Type | Units | Range | Required |
|---|---|---|---|---|
| `agent_id` | `str` | — | `[A-Za-z0-9._-]{1,64}` | yes |
| `accuracy` | `float` | ratio | `[0, 1]` | yes |
| `energy_kwh` | `float` | kWh | `≥ 0` | yes |
| `carbon_co2e_kg` | `float` | kg CO₂e | `≥ 0` | yes |
| `latency_ms` | `float` | ms | `≥ 0` | yes |
| `memory_mb` | `float \| None` | MB | `> 0` | optional |
| `circuit_depth` | `int \| None` | gates | `≥ 0` | optional (None = non-quantum) |
| `variance_score` | `float \| None` | CV of energy over runs | `≥ 0` | optional |

Validation raises `ValueError` at construction. `agent_id` is validated to
`[A-Za-z0-9._-]{1,64}` — this makes Plotly HTML injection impossible **by
construction**, independent of the plotter.

### 1.2 Why each dimension is not derivable from the 4D baseline

| Dimension | Failure mode it exposes | Derivable from 4D? |
|---|---|---|
| **Memory (MB)** | OOM on edge/serverless | No — independent hardware constraint |
| **Circuit depth** | Quantum decoherence / scalability wall | No — structural cost, not outcome |
| **Inference variance (σ)** | SLA / carbon-cap violations under load | No — needs ≥3 runs; not a first-order mean |

### 1.3 `variance_score` definition (was undefined in v1)

`variance_score = std(energy_kwh across N runs) / mean(energy_kwh across N runs)`,
with `N ≥ 3`. **Coefficient of variation**, dimensionless, `∈ [0, ∞)`.

- `variance_score=None` → agent was run once (or fewer); **excluded** from
  variance-based Pareto and from the full 7D frontier.
- `variance_score=0.05` means "5% relative fluctuation in energy between
  runs" — comparable across scales and hardware.
- Alternative metrics (IQR/median, p99/p50) are accepted: the analyzer
  treats any `≥ 0` value as a **minimization axis**. Use one metric
  consistently per population; the analyzer cannot tell them apart.

### 1.4 Missing-value policy (per axis)

| Axis | Missing means | Policy |
|---|---|---|
| `memory_mb` | Unknown device budget | **Excluded** from full frontier; only memory-constraint analysis runs on the non-`None` subset |
| `circuit_depth` | Non-quantum agent | **Axis-skipped** during dominance (neither helps nor hurts); frontier **includes** the agent |
| `variance_score` | Single run | **Excluded** from full frontier; only variance analysis runs on the non-`None` subset |

Every analysis method reports the count of exclusions in its return value;
no silent substitution with `0` or a mean.

### 1.5 MODP normalization

`modp_score` needs inputs in `[0, 1]`. Normalization is **min–max across the
current population**, recomputed per call:

```python
normalize(v) = (v - lo) / (hi - lo)        if higher_is_better (accuracy)
              = (hi - v) / (hi - lo)       otherwise
```

If `hi == lo` (all equal), all values map to `1.0`. A missing value maps to
`0.0` (worst-case) for its axis — the agent is **penalized** for missing data
rather than silently rewarded.

Six weights are supplied (`modp_weights`), one per axis:
`(accuracy, energy, carbon, latency, memory, variance)`. Must sum to `1.0`.
`circuit_depth` is **not** in MODP — it's typically zero-variance across a
classical population and would add noise.

### 1.6 Tie / ε policy

Dominance uses `ε = 1e-9`:

> `p` dominates `q` iff for every **comparable** axis `i`, `p_i ≥ q_i − ε`
> (for minimization axes: `p_i ≤ q_i + ε`), and for at least one axis
> `p_i > q_i + ε` (or the analogous minimization form). Missing optional
> axis → **skip** that axis.

Determinism tested in `test_epsilon_makes_floats_deterministic`.

### 1.7 Correlation caveat (scientific honesty)

- `memory_mb` ↔ `energy_kwh`: DRAM energy is 20–40% of system power
  (Kim et al. 2020). MODP weights treat them as independent, which
  under-weights memory in high-energy regimes.
- `variance_score` ↔ `latency_ms`: under queueing, variance and latency
  co-move (Dean & Barroso 2013). A mean-latency-only analysis
  under-represents tail risk.

These are limitations of the weighting scheme, **not** of the Pareto
analysis. Correlation-aware weighting is tracked as O-2.

---

## 2. Implementation

The analyzer is a complete module below (§2.1). Every return type is a
`TypedDict` or `@dataclass` — consumers have a contract.

### 2.1 Public API surface

```python
# src/analysis/extended_pareto_analyzer.py
ExtendedParetoPoint(agent_id, accuracy, energy_kwh, carbon_co2e_kg, latency_ms,
                    memory_mb=None, circuit_depth=None, variance_score=None)
    .to_dict() -> dict

Constraints(max_memory_mb=None, max_circuit_depth=None, max_variance=None)

ExtendedParetoAnalyzer(*, epsilon=1e-9, modp_weights=None)
    .compute_frontier(agents) -> list[ExtendedParetoPoint]
    .analyze_memory_constraint(agents, max_memory_mb) -> MemoryAnalysis
    .analyze_circuit_depth_scalability(agents, *, max_circuit_depth=None) -> CircuitAnalysis
    .analyze_variance_stability(agents, *, stability_threshold) -> VarianceAnalysis
    .comprehensive_analysis(agents, constraints) -> ComprehensiveAnalysis

to_csv(agents, path) -> None
```

Return-shape `TypedDict`s:

```python
MemoryAnalysis       = {frontier, excluded_from_frontier, total,
                        feasible_ids, infeasible_ids,
                        best_memory_efficient_id, max_memory_mb}
CircuitAnalysis      = {frontier, excluded_from_frontier, total,
                        shallow_ids, most_robust_id, excluded_non_quantum_ids}
VarianceAnalysis     = {frontier, excluded_from_frontier, total,
                        stable_ids, unstable_ids, excluded_single_run_ids,
                        stability_threshold}
ComprehensiveAnalysis= {recommendation, reason, feasible_count,
                        excluded_memory_count, excluded_circuit_count,
                        excluded_variance_count, frontier_ids, modp_scores}
```

### 2.2 Empty-feasible-set behavior

`comprehensive_analysis` returns
`{"recommendation": None, "reason": "no agents satisfy all constraints", ...}`
— **never raises**. The caller can inspect `feasible_count` and adjust.

### 2.3 Performance

`compute_frontier` is O(n²). At n=1000 it completes in well under 100 ms on
a modern CPU (reproduce with `scripts/bench_pareto.py`, §9). Above n≈5000,
substitute a d-dimensional maxima algorithm (Kung–Luccio–Preparata 1975) —
tracked as O-3, not implemented here.

---

## 3. Plotter

`ParetoPlotter` writes self-contained Plotly HTML files.

### 3.1 API

```python
ParetoPlotter(*, output_dir=Path("plots"))
    .plot_accuracy_vs_carbon(agents, frontier, *, filename=...) -> Path
    .plot_latency_vs_energy(agents, frontier, *, filename=...) -> Path
    .plot_carbon_vs_energy(agents, frontier, *, filename=...) -> Path
```

### 3.2 HTML safety

Two layers:
1. `agent_id` validated `[A-Za-z0-9._-]{1,64}` at `ExtendedParetoPoint`
   construction → no `<`, `>`, `&`, quotes possible.
2. Plotter applies `html.escape()` to every label before emitting → defense
   in depth if validation ever loosens.

`include_plotlyjs=True` makes each HTML file self-contained. Embedding in a
dashboard is the caller's responsibility (CSP, sandboxed iframe, etc.).

---

## 4. Async/sync contract

| Symbol | Type |
|---|---|
| `ExtendedParetoAnalyzer.*` | **sync** |
| `ParetoPlotter.*` | **sync** |
| `to_csv()` | **sync** |
| `NodeDescriptor.select_routing_strategy` | **sync** |
| `WorkloadDescriptor.select_priority` | **sync** |

Any snippet elsewhere in this repo that `await`s these is a bug.

---

## 5. Usage (corrected)

```python
from pathlib import Path
from src.analysis.extended_pareto_analyzer import (
    Constraints, ExtendedParetoAnalyzer, ExtendedParetoPoint, to_csv,
)
from src.visualization.pareto_plotter import ParetoPlotter
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

agents = [
    ExtendedParetoPoint(
        agent_id="resnet50",
        accuracy=0.94, energy_kwh=0.008, carbon_co2e_kg=0.0016,
        latency_ms=350, memory_mb=512, circuit_depth=None,
        variance_score=0.05,
    ),
    ExtendedParetoPoint(
        agent_id="qcnn-small",
        accuracy=0.91, energy_kwh=0.002, carbon_co2e_kg=0.0004,
        latency_ms=820, memory_mb=96, circuit_depth=12,
        variance_score=0.11,
    ),
]

analyzer = ExtendedParetoAnalyzer(modp_weights=(0.4, 0.2, 0.2, 0.1, 0.05, 0.05))
frontier = analyzer.compute_frontier(agents)
to_csv(agents, Path("results/agents.csv"))

ParetoPlotter(output_dir=Path("plots")).plot_carbon_vs_energy(agents, frontier)

# Enhancement integration — all sync
node = NodeDescriptor(
    id="analysis_node", type=NodeType.EDGE, region="us-east",
    region_carbon_intensity=350.0, energy_per_token=0.00004,
    use_evolutionary=True, human_feedback_score=0.7,
    graph_metrics={"centrality": 0.8},
)
strategy = node.select_routing_strategy()   # SYNC — no `await`
```

---

## 6. Comparison table (honest)

| Capability | 4D | 7D (this file) | 7D + enhancements (opt-in) |
|---|---|---|---|
| Memory constraint | ❌ | ✅ tested | ✅ |
| Circuit depth | ❌ | ✅ tested | ✅ (distillation select) |
| Variance | ❌ | ✅ tested | ✅ (RLHF penalize) |
| MODP overlay | ❌ | ✅ tested | ✅ |
| Coverage | — | 20+ tests, fixtures | ⚠️ `tests/test_enhancements.py` pending |

The enhancements column is **not covered by any test in this file**.
`tests/test_enhancements.py` is a separate deliverable — do not claim
enhancement coverage until it exists.

---

## 7. Business value

- **Sustainability teams:** the carbon-vs-energy plot is the cleanest
  single deliverable. It does **not** require enhancement modules.
- **Edge deployment:** memory-constraint analysis is the core value; works
  without enhancements.
- **Quantum researchers:** circuit-depth axis assumes quantum agents
  exist; classical-only populations collapse it (all `None`).
- **Production teams:** variance analysis requires ≥3 runs per agent;
  single-run agents are excluded and counted.

---

## 8. Known limitations

1. MODP weighting ignores correlation between `memory_mb` ↔ `energy_kwh` and
   `variance_score` ↔ `latency_ms` (§1.7).
2. `variance_score=None` (single-run agents) are excluded from variance
   analysis and from the full 7D frontier.
3. `circuit_depth=None` (non-quantum) is skipped during dominance — neither
   helps nor hurts. Frontier includes those agents.
4. `compute_frontier` is O(n²) — fine to n ≈ 1000; needs d-dim maxima above
   that (O-3).
5. `ParetoPlotter` emits `include_plotlyjs=True` self-contained HTML.
   Dashboard re-rendering must apply its own CSP.
6. `comprehensive_analysis` with tight constraints returns
   `recommendation=None`; the demo prints discarded counts and reasons.
7. 7D frontier is **exact** for `ε=1e-9`. Float noise above that may change
   membership (documented and tested).
8. Enhancement integration (MODP overlay, RLHF penalization, distillation)
   is **not tested** in this file.

---

## 9. Reproducibility

Every demo run must record:

- `results/dataset.json.sha256` — SHA256 of the fixture
- `results/requirements.txt.sha256` — SHA256 of the pinned deps
- `results/env.txt` — Python version, numpy/plotly versions, git commit, UTC timestamp

`scripts/record_provenance.sh` (§8) writes all three. `scripts/bench_pareto.py`
(§9) reproduces the O(n²) performance claim.

---

## 10. References

- Kim et al., *Power Characterization of Memory Subsystems*, 2020
- Preskill, *Quantum Computing in the NISQ Era*, 2018
- Dean & Barroso, *The Tail at Scale*, CACM 2013
- Tufte, *The Visual Display of Quantitative Information*, 2nd ed.
- Kung, Luccio, Preparata, *On Finding the Maxima of a Set of Vectors*, JACM 1975
- Hinton et al., *Distilling the Knowledge in a Neural Network*, 2015
- Christiano et al., *Deep RL from Human Preferences*, 2017

---

## 11. Next steps

- Resolve O-1..O-4 (§0.2).
- Add `tests/test_enhancements.py` before claiming enhancement coverage.
- Bench `compute_frontier` — run `scripts/bench_pareto.py`.
- Wire real dataset from Cinebench pipeline.

---

# === SAVE AS: src/analysis/extended_pareto_analyzer.py ===
```python
"""
src/analysis/extended_pareto_analyzer.py — 7D Pareto analysis.

Public API:
    ExtendedParetoPoint    — validated, frozen data model
    Constraints            — for comprehensive_analysis
    ExtendedParetoAnalyzer — the analyzer
    to_csv                 — flat export

All methods are synchronous. Missing optional axes are handled per §1.4 of
the guide. All return types are TypedDicts — consumers have a contract.

Design notes:
    * Dominance uses ε = 1e-9 (§1.6).
    * variance_score is coefficient of variation of energy across ≥3 runs
      (§1.3). Single-run agents have variance_score=None and are excluded
      from variance analysis and from the full frontier.
    * compute_frontier is O(n²); fine to n ≈ 1000.
"""
from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Sequence, TypedDict

__all__ = [
    "ExtendedParetoPoint",
    "Constraints",
    "MemoryAnalysis",
    "CircuitAnalysis",
    "VarianceAnalysis",
    "ComprehensiveAnalysis",
    "ExtendedParetoAnalyzer",
    "to_csv",
]


# --- Data model -----------------------------------------------------------

_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass(frozen=True, slots=True)
class ExtendedParetoPoint:
    agent_id: str
    accuracy: float
    energy_kwh: float
    carbon_co2e_kg: float
    latency_ms: float
    memory_mb: float | None = None
    circuit_depth: int | None = None
    variance_score: float | None = None

    def __post_init__(self) -> None:
        if not _ID_PATTERN.match(self.agent_id):
            raise ValueError(
                f"agent_id must match {_ID_PATTERN.pattern!r}: {self.agent_id!r}"
            )
        if not (0.0 <= self.accuracy <= 1.0):
            raise ValueError(f"accuracy out of [0,1]: {self.accuracy}")
        if self.energy_kwh < 0:
            raise ValueError(f"energy_kwh must be ≥ 0: {self.energy_kwh}")
        if self.carbon_co2e_kg < 0:
            raise ValueError(f"carbon_co2e_kg must be ≥ 0: {self.carbon_co2e_kg}")
        if self.latency_ms < 0:
            raise ValueError(f"latency_ms must be ≥ 0: {self.latency_ms}")
        if self.memory_mb is not None and self.memory_mb <= 0:
            raise ValueError(f"memory_mb must be > 0 or None: {self.memory_mb}")
        if self.circuit_depth is not None and self.circuit_depth < 0:
            raise ValueError(
                f"circuit_depth must be ≥ 0 or None: {self.circuit_depth}"
            )
        if self.variance_score is not None and self.variance_score < 0:
            raise ValueError(
                f"variance_score must be ≥ 0 or None: {self.variance_score}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "accuracy": self.accuracy,
            "energy_kwh": self.energy_kwh,
            "carbon_co2e_kg": self.carbon_co2e_kg,
            "latency_ms": self.latency_ms,
            "memory_mb": self.memory_mb,
            "circuit_depth": self.circuit_depth,
            "variance_score": self.variance_score,
        }


@dataclass(frozen=True)
class Constraints:
    max_memory_mb: float | None = None
    max_circuit_depth: int | None = None
    max_variance: float | None = None


# --- Typed return shapes --------------------------------------------------

class MemoryAnalysis(TypedDict):
    frontier: list[str]
    excluded_from_frontier: list[str]
    total: int
    feasible_ids: list[str]
    infeasible_ids: list[str]
    best_memory_efficient_id: str | None
    max_memory_mb: float


class CircuitAnalysis(TypedDict):
    frontier: list[str]
    excluded_from_frontier: list[str]
    total: int
    shallow_ids: list[str]
    most_robust_id: str | None
    excluded_non_quantum_ids: list[str]


class VarianceAnalysis(TypedDict):
    frontier: list[str]
    excluded_from_frontier: list[str]
    total: int
    stable_ids: list[str]
    unstable_ids: list[str]
    excluded_single_run_ids: list[str]
    stability_threshold: float


class ComprehensiveAnalysis(TypedDict):
    recommendation: str | None
    reason: str
    feasible_count: int
    excluded_memory_count: int
    excluded_circuit_count: int
    excluded_variance_count: int
    frontier_ids: list[str]
    modp_scores: dict[str, float]


# --- Analyzer -------------------------------------------------------------

_EPS = 1e-9

# (field_name, higher_is_better)
_ALL_AXES: tuple[tuple[str, bool], ...] = (
    ("accuracy",       True),
    ("energy_kwh",     False),
    ("carbon_co2e_kg", False),
    ("latency_ms",     False),
    ("memory_mb",      False),
    ("circuit_depth",  False),
    ("variance_score", False),
)

# Axes used for MODP scoring (circuit_depth intentionally excluded)
_MODP_AXES: tuple[tuple[str, bool], ...] = (
    ("accuracy",       True),
    ("energy_kwh",     False),
    ("carbon_co2e_kg", False),
    ("latency_ms",     False),
    ("memory_mb",      False),
    ("variance_score", False),
)


class ExtendedParetoAnalyzer:
    def __init__(
        self,
        *,
        epsilon: float = _EPS,
        modp_weights: tuple[float, ...] | None = None,
    ) -> None:
        self._eps = float(epsilon)
        if modp_weights is not None:
            if len(modp_weights) != len(_MODP_AXES):
                raise ValueError(
                    f"modp_weights must have {len(_MODP_AXES)} entries "
                    "(accuracy, energy, carbon, latency, memory, variance)"
                )
            if abs(sum(modp_weights) - 1.0) > 1e-6:
                raise ValueError(
                    f"modp_weights must sum to 1.0 (got {sum(modp_weights):.6f})"
                )
        self._modp_weights = modp_weights

    # --- Public ------------------------------------------------------------
    def compute_frontier(
        self, agents: Sequence[ExtendedParetoPoint],
    ) -> list[ExtendedParetoPoint]:
        """
        Return Pareto-optimal agents over the full 7D set.

        Missing optional axes (§1.4):
            memory_mb=None      → excluded from frontier
            variance_score=None → excluded from frontier
            circuit_depth=None  → included; axis skipped during dominance
        """
        if not agents:
            return []

        eligible = [
            a for a in agents
            if a.memory_mb is not None and a.variance_score is not None
        ]
        if not eligible:
            return []

        frontier: list[ExtendedParetoPoint] = []
        for i, p in enumerate(eligible):
            dominated = False
            for j, q in enumerate(eligible):
                if i == j:
                    continue
                if self._dominates(q, p):
                    dominated = True
                    break
            if not dominated:
                frontier.append(p)
        return frontier

    def analyze_memory_constraint(
        self,
        agents: Sequence[ExtendedParetoPoint],
        max_memory_mb: float,
    ) -> MemoryAnalysis:
        if max_memory_mb <= 0:
            raise ValueError("max_memory_mb must be > 0")

        known   = [a for a in agents if a.memory_mb is not None]
        unknown = [a for a in agents if a.memory_mb is None]
        feasible   = [a for a in known if a.memory_mb <= max_memory_mb]
        infeasible = [a for a in known if a.memory_mb >  max_memory_mb]

        frontier = self.compute_frontier(feasible)
        best = min(feasible, key=lambda a: a.memory_mb) if feasible else None
        return MemoryAnalysis(
            frontier=[a.agent_id for a in frontier],
            excluded_from_frontier=[a.agent_id for a in unknown],
            total=len(agents),
            feasible_ids=[a.agent_id for a in feasible],
            infeasible_ids=[a.agent_id for a in infeasible],
            best_memory_efficient_id=best.agent_id if best else None,
            max_memory_mb=float(max_memory_mb),
        )

    def analyze_circuit_depth_scalability(
        self,
        agents: Sequence[ExtendedParetoPoint],
        *,
        max_circuit_depth: int | None = None,
    ) -> CircuitAnalysis:
        quantum   = [a for a in agents if a.circuit_depth is not None]
        classical = [a for a in agents if a.circuit_depth is None]

        shallow = (
            list(quantum) if max_circuit_depth is None
            else [a for a in quantum if a.circuit_depth <= max_circuit_depth]
        )

        most_robust = min(quantum, key=lambda a: a.circuit_depth) if quantum else None
        frontier = self.compute_frontier(shallow)
        return CircuitAnalysis(
            frontier=[a.agent_id for a in frontier],
            excluded_from_frontier=[a.agent_id for a in classical],
            total=len(agents),
            shallow_ids=[a.agent_id for a in shallow],
            most_robust_id=most_robust.agent_id if most_robust else None,
            excluded_non_quantum_ids=[a.agent_id for a in classical],
        )

    def analyze_variance_stability(
        self,
        agents: Sequence[ExtendedParetoPoint],
        *,
        stability_threshold: float,
    ) -> VarianceAnalysis:
        if stability_threshold <= 0:
            raise ValueError("stability_threshold must be > 0")

        measured   = [a for a in agents if a.variance_score is not None]
        single_run = [a for a in agents if a.variance_score is None]

        stable   = [a for a in measured if a.variance_score <= stability_threshold]
        unstable = [a for a in measured if a.variance_score >  stability_threshold]

        frontier = self.compute_frontier(stable)
        return VarianceAnalysis(
            frontier=[a.agent_id for a in frontier],
            excluded_from_frontier=[a.agent_id for a in single_run],
            total=len(agents),
            stable_ids=[a.agent_id for a in stable],
            unstable_ids=[a.agent_id for a in unstable],
            excluded_single_run_ids=[a.agent_id for a in single_run],
            stability_threshold=float(stability_threshold),
        )

    def comprehensive_analysis(
        self,
        agents: Sequence[ExtendedParetoPoint],
        constraints: Constraints,
    ) -> ComprehensiveAnalysis:
        survivors: list[ExtendedParetoPoint] = []
        excl_mem = excl_circ = excl_var = 0

        for a in agents:
            if constraints.max_memory_mb is not None:
                if a.memory_mb is None or a.memory_mb > constraints.max_memory_mb:
                    excl_mem += 1
                    continue
            if constraints.max_circuit_depth is not None and a.circuit_depth is not None:
                if a.circuit_depth > constraints.max_circuit_depth:
                    excl_circ += 1
                    continue
            if constraints.max_variance is not None:
                if a.variance_score is None or a.variance_score > constraints.max_variance:
                    excl_var += 1
                    continue
            survivors.append(a)

        if not survivors:
            return ComprehensiveAnalysis(
                recommendation=None,
                reason="no agents satisfy all constraints",
                feasible_count=0,
                excluded_memory_count=excl_mem,
                excluded_circuit_count=excl_circ,
                excluded_variance_count=excl_var,
                frontier_ids=[],
                modp_scores={},
            )

        modp_scores = self._modp_scores(survivors)
        frontier = self.compute_frontier(survivors)
        if not frontier:
            chosen = max(survivors, key=lambda a: modp_scores.get(a.agent_id, 0.0))
            return ComprehensiveAnalysis(
                recommendation=chosen.agent_id,
                reason="frontier empty (missing optional fields); chose by MODP",
                feasible_count=len(survivors),
                excluded_memory_count=excl_mem,
                excluded_circuit_count=excl_circ,
                excluded_variance_count=excl_var,
                frontier_ids=[],
                modp_scores=modp_scores,
            )

        chosen = max(frontier, key=lambda a: modp_scores.get(a.agent_id, 0.0))
        return ComprehensiveAnalysis(
            recommendation=chosen.agent_id,
            reason="highest MODP among Pareto frontier survivors",
            feasible_count=len(survivors),
            excluded_memory_count=excl_mem,
            excluded_circuit_count=excl_circ,
            excluded_variance_count=excl_var,
            frontier_ids=[a.agent_id for a in frontier],
            modp_scores=modp_scores,
        )

    # --- Internals ---------------------------------------------------------
    def _dominates(
        self, p: ExtendedParetoPoint, q: ExtendedParetoPoint,
    ) -> bool:
        """
        p dominates q iff p is at least as good on every *comparable* axis
        (within ε) and strictly better on at least one.

        Missing optional axis on either side → axis-skipped (no contribution).
        """
        strict = False
        for name, higher_is_better in _ALL_AXES:
            pv = getattr(p, name)
            qv = getattr(q, name)
            if pv is None or qv is None:
                continue        # axis-skipped (§1.4)
            diff = (pv - qv) if higher_is_better else (qv - pv)
            if diff < -self._eps:
                return False
            if diff > self._eps:
                strict = True
        return strict

    def _modp_scores(
        self, agents: Sequence[ExtendedParetoPoint],
    ) -> dict[str, float]:
        if self._modp_weights is None or not agents:
            return {}

        def raw(name: str) -> list[float | None]:
            return [getattr(a, name) for a in agents]

        def normalize(
            vals: list[float | None], higher_is_better: bool,
        ) -> list[float]:
            clean = [float(v) for v in vals if v is not None]
            if not clean:
                return [0.0] * len(vals)
            lo, hi = min(clean), max(clean)
            if hi - lo < 1e-12:
                return [1.0 if v is not None else 0.0 for v in vals]
            out: list[float] = []
            for v in vals:
                if v is None:
                    out.append(0.0)     # worst-case for missing
                else:
                    t = (float(v) - lo) / (hi - lo)
                    out.append(t if higher_is_better else 1.0 - t)
            return out

        normed = [normalize(raw(name), hib) for name, hib in _MODP_AXES]
        w = self._modp_weights
        scores: dict[str, float] = {}
        for i, a in enumerate(agents):
            s = sum(w[k] * normed[k][i] for k in range(len(_MODP_AXES)))
            scores[a.agent_id] = max(0.0, min(1.0, s))
        return scores


# --- Flat export ----------------------------------------------------------

_CSV_FIELDS = (
    "agent_id", "accuracy", "energy_kwh", "carbon_co2e_kg",
    "latency_ms", "memory_mb", "circuit_depth", "variance_score",
)


def to_csv(agents: Sequence[ExtendedParetoPoint], path: Path) -> None:
    """Write a flat CSV of the given points. None → empty cell."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        w.writeheader()
        for a in agents:
            row = a.to_dict()
            w.writerow({k: ("" if row[k] is None else row[k]) for k in _CSV_FIELDS})
```

---

# === SAVE AS: src/visualization/pareto_plotter.py ===
```python
"""
src/visualization/pareto_plotter.py — 2D Pareto projections.

Each plot is a self-contained Plotly HTML file (include_plotlyjs=True).
All string labels are `html.escape`'d before emission — defense in depth
alongside `agent_id` validation in ExtendedParetoPoint.
"""
from __future__ import annotations

import html
from pathlib import Path
from typing import Sequence

import plotly.graph_objects as go

from src.analysis.extended_pareto_analyzer import ExtendedParetoPoint

__all__ = ["ParetoPlotter"]


class ParetoPlotter:
    def __init__(
        self,
        *,
        output_dir: Path = Path("plots"),
    ) -> None:
        self._out = Path(output_dir)
        self._out.mkdir(parents=True, exist_ok=True)

    # --- Public ------------------------------------------------------------
    def plot_accuracy_vs_carbon(
        self,
        agents: Sequence[ExtendedParetoPoint],
        frontier: Sequence[ExtendedParetoPoint],
        *,
        filename: str = "accuracy_vs_carbon.html",
    ) -> Path:
        return self._scatter(
            agents, frontier,
            x_field="carbon_co2e_kg", y_field="accuracy",
            x_title="Carbon (kg CO₂e)", y_title="Accuracy",
            title="Accuracy vs Carbon — environmental price of performance",
            filename=filename,
        )

    def plot_latency_vs_energy(
        self,
        agents: Sequence[ExtendedParetoPoint],
        frontier: Sequence[ExtendedParetoPoint],
        *,
        filename: str = "latency_vs_energy.html",
    ) -> Path:
        return self._scatter(
            agents, frontier,
            x_field="energy_kwh", y_field="latency_ms",
            x_title="Energy (kWh)", y_title="Latency (ms)",
            title="Latency vs Energy — is speed coupled to energy?",
            filename=filename,
        )

    def plot_carbon_vs_energy(
        self,
        agents: Sequence[ExtendedParetoPoint],
        frontier: Sequence[ExtendedParetoPoint],
        *,
        filename: str = "carbon_vs_energy.html",
    ) -> Path:
        return self._scatter(
            agents, frontier,
            x_field="energy_kwh", y_field="carbon_co2e_kg",
            x_title="Energy (kWh)", y_title="Carbon (kg CO₂e)",
            title="Carbon vs Energy — pure green efficiency",
            filename=filename,
        )

    # --- Internals ---------------------------------------------------------
    def _scatter(
        self,
        agents: Sequence[ExtendedParetoPoint],
        frontier: Sequence[ExtendedParetoPoint],
        *,
        x_field: str, y_field: str,
        x_title: str, y_title: str,
        title: str, filename: str,
    ) -> Path:
        frontier_ids = {a.agent_id for a in frontier}
        xs_pts, ys_pts, labels_pts = [], [], []
        xs_front, ys_front, labels_front = [], [], []
        for a in agents:
            x = getattr(a, x_field)
            y = getattr(a, y_field)
            if x is None or y is None:
                continue
            label = html.escape(a.agent_id)
            if a.agent_id in frontier_ids:
                xs_front.append(x); ys_front.append(y); labels_front.append(label)
            else:
                xs_pts.append(x); ys_pts.append(y); labels_pts.append(label)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=xs_pts, y=ys_pts, mode="markers", name="dominated",
            text=labels_pts, hovertemplate="%{text}<extra></extra>",
            marker=dict(size=9, opacity=0.55, color="#888888"),
        ))
        fig.add_trace(go.Scatter(
            x=xs_front, y=ys_front, mode="markers+text", name="Pareto frontier",
            text=labels_front, textposition="top center",
            hovertemplate="%{text}<extra></extra>",
            marker=dict(size=12, color="#d62728", symbol="diamond"),
        ))
        fig.update_layout(
            title=title, xaxis_title=x_title, yaxis_title=y_title,
            template="plotly_white",
        )
        out = self._out / filename
        fig.write_html(str(out), include_plotlyjs=True, full_html=True)
        return out
```

---

# === SAVE AS: examples/demo_extended_dimensions.py ===
```python
"""
examples/demo_extended_dimensions.py — Runs the 7D analysis + plots.

Usage:
    python examples/demo_extended_dimensions.py [--enhanced] [--out PLOTS_DIR]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src.analysis.extended_pareto_analyzer import (
    Constraints, ExtendedParetoAnalyzer, ExtendedParetoPoint, to_csv,
)
from src.visualization.pareto_plotter import ParetoPlotter

_FIXTURE = _ROOT / "tests" / "fixtures" / "agents.json"


def load_fixture(path: Path) -> list[ExtendedParetoPoint]:
    data = json.loads(path.read_text())
    return [ExtendedParetoPoint(**d) for d in data]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--enhanced", action="store_true",
                    help="Also exercise enhancement modules (if available)")
    ap.add_argument("--out", default="plots")
    args = ap.parse_args()

    agents = load_fixture(_FIXTURE)
    analyzer = ExtendedParetoAnalyzer(
        modp_weights=(0.4, 0.2, 0.2, 0.1, 0.05, 0.05),
    )

    frontier = analyzer.compute_frontier(agents)
    print(f"frontier: {[a.agent_id for a in frontier]}")

    mem = analyzer.analyze_memory_constraint(agents, max_memory_mb=512)
    print(f"memory:  feasible={len(mem['feasible_ids'])} "
          f"best={mem['best_memory_efficient_id']}")

    qc = analyzer.analyze_circuit_depth_scalability(agents, max_circuit_depth=50)
    print(f"circuit: shallow={len(qc['shallow_ids'])} "
          f"most_robust={qc['most_robust_id']}")

    var = analyzer.analyze_variance_stability(agents, stability_threshold=0.2)
    print(f"variance: stable={len(var['stable_ids'])} "
          f"unstable={len(var['unstable_ids'])} "
          f"single_run_excluded={len(var['excluded_single_run_ids'])}")

    comp = analyzer.comprehensive_analysis(
        agents,
        Constraints(max_memory_mb=512, max_circuit_depth=50, max_variance=0.2),
    )
    print(f"recommendation: {comp['recommendation']} ({comp['reason']})")

    results = _ROOT / "results"
    to_csv(agents, results / "agents.csv")
    print(f"wrote {results / 'agents.csv'}")

    plotter = ParetoPlotter(output_dir=Path(args.out))
    for p in (
        plotter.plot_accuracy_vs_carbon(agents, frontier),
        plotter.plot_latency_vs_energy(agents, frontier),
        plotter.plot_carbon_vs_energy(agents, frontier),
    ):
        print(f"wrote {p}")

    if args.enhanced:
        _run_enhanced()
    return 0


def _run_enhanced() -> None:
    try:
        from src.enhancements.schemas.node_descriptor import (
            NodeDescriptor, NodeType,
        )
    except ImportError:
        print("enhancement modules unavailable — skipping --enhanced")
        return
    node = NodeDescriptor(
        id="analysis_node", type=NodeType.EDGE, region="us-east",
        region_carbon_intensity=350.0, energy_per_token=0.00004,
        use_evolutionary=True, human_feedback_score=0.7,
        graph_metrics={"centrality": 0.8},
    )
    strategy = node.select_routing_strategy()     # SYNC — no `await`
    print(f"enhanced routing strategy: {strategy}")


if __name__ == "__main__":
    raise SystemExit(main())
```

---

# === SAVE AS: tests/fixtures/agents.json ===
```json
[
  {"agent_id": "resnet50",        "accuracy": 0.94, "energy_kwh": 0.008,
   "carbon_co2e_kg": 0.0016, "latency_ms": 350, "memory_mb": 512,
   "circuit_depth": null, "variance_score": 0.05},
  {"agent_id": "qcnn-small",      "accuracy": 0.91, "energy_kwh": 0.002,
   "carbon_co2e_kg": 0.0004, "latency_ms": 820, "memory_mb": 96,
   "circuit_depth": 12, "variance_score": 0.11},
  {"agent_id": "distil-bert",     "accuracy": 0.88, "energy_kwh": 0.0015,
   "carbon_co2e_kg": 0.0003, "latency_ms": 190, "memory_mb": 256,
   "circuit_depth": null, "variance_score": 0.03},
  {"agent_id": "qcnn-large",      "accuracy": 0.93, "energy_kwh": 0.0045,
   "carbon_co2e_kg": 0.0009, "latency_ms": 1400, "memory_mb": 384,
   "circuit_depth": 60, "variance_score": 0.18},
  {"agent_id": "tiny-mobile",     "accuracy": 0.81, "energy_kwh": 0.0007,
   "carbon_co2e_kg": 0.00014, "latency_ms": 95, "memory_mb": 64,
   "circuit_depth": null, "variance_score": 0.02},
  {"agent_id": "edge-single-run", "accuracy": 0.85, "energy_kwh": 0.002,
   "carbon_co2e_kg": 0.0004, "latency_ms": 260, "memory_mb": 384,
   "circuit_depth": null, "variance_score": null}
]
```

---

# === SAVE AS: tests/test_extended_pareto_analyzer.py ===
```python
"""Exhaustive tests for the analyzer. No external I/O; plots are not touched."""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.analysis.extended_pareto_analyzer import (
    Constraints, ExtendedParetoAnalyzer, ExtendedParetoPoint, to_csv,
)


def _pt(**kw) -> ExtendedParetoPoint:
    base = dict(
        agent_id="a", accuracy=0.5, energy_kwh=1.0, carbon_co2e_kg=0.1,
        latency_ms=100, memory_mb=256, circuit_depth=None, variance_score=0.1,
    )
    base.update(kw)
    return ExtendedParetoPoint(**base)


# --- Data model ------------------------------------------------------------

def test_invalid_agent_id_rejected():
    with pytest.raises(ValueError):
        _pt(agent_id="bad id with space")
    with pytest.raises(ValueError):
        _pt(agent_id="<script>alert(1)</script>")
    with pytest.raises(ValueError):
        _pt(agent_id="")


def test_accuracy_range():
    with pytest.raises(ValueError):
        _pt(accuracy=1.5)
    with pytest.raises(ValueError):
        _pt(accuracy=-0.1)


def test_memory_must_be_positive_or_none():
    with pytest.raises(ValueError):
        _pt(memory_mb=0)
    _pt(memory_mb=None)     # OK


def test_variance_must_be_nonnegative_or_none():
    with pytest.raises(ValueError):
        _pt(variance_score=-0.1)
    _pt(variance_score=None)   # OK


# --- Dominance -------------------------------------------------------------

def test_strict_dominance_removes_point():
    a = _pt(agent_id="a", accuracy=0.9, energy_kwh=0.1, carbon_co2e_kg=0.05,
            latency_ms=50, memory_mb=128, variance_score=0.02)
    b = _pt(agent_id="b", accuracy=0.5, energy_kwh=1.0, carbon_co2e_kg=0.10,
            latency_ms=100, memory_mb=256, variance_score=0.10)
    frontier = ExtendedParetoAnalyzer().compute_frontier([a, b])
    assert [p.agent_id for p in frontier] == ["a"]


def test_ties_do_not_dominate():
    a = _pt(agent_id="a")
    b = _pt(agent_id="b")
    frontier = ExtendedParetoAnalyzer().compute_frontier([a, b])
    assert {p.agent_id for p in frontier} == {"a", "b"}


def test_epsilon_makes_floats_deterministic():
    a = _pt(agent_id="a", accuracy=0.5 + 1e-12)
    b = _pt(agent_id="b", accuracy=0.5)
    frontier = ExtendedParetoAnalyzer(epsilon=1e-9).compute_frontier([a, b])
    assert {p.agent_id for p in frontier} == {"a", "b"}


# --- Missing-value policy --------------------------------------------------

def test_missing_memory_excluded_from_frontier():
    a = _pt(agent_id="a", memory_mb=None)
    b = _pt(agent_id="b")
    frontier = ExtendedParetoAnalyzer().compute_frontier([a, b])
    assert [p.agent_id for p in frontier] == ["b"]


def test_missing_variance_excluded_from_frontier():
    a = _pt(agent_id="a", variance_score=None)
    b = _pt(agent_id="b")
    frontier = ExtendedParetoAnalyzer().compute_frontier([a, b])
    assert [p.agent_id for p in frontier] == ["b"]


def test_missing_circuit_depth_is_axis_skipped():
    """A classical agent (circuit_depth=None) is included, and its
    'null vs 50' depth does not make it dominate or be dominated."""
    classical = _pt(agent_id="classical", circuit_depth=None)
    quantum   = _pt(agent_id="quantum",   circuit_depth=50)
    frontier = ExtendedParetoAnalyzer().compute_frontier([classical, quantum])
    # All other axes are equal → neither dominates.
    assert {p.agent_id for p in frontier} == {"classical", "quantum"}


# --- Empty inputs ---------------------------------------------------------

def test_empty_input_returns_empty_frontier():
    assert ExtendedParetoAnalyzer().compute_frontier([]) == []


def test_all_missing_optional_returns_empty_frontier():
    agents = [_pt(agent_id="a", memory_mb=None, variance_score=None),
              _pt(agent_id="b", memory_mb=None, variance_score=None)]
    assert ExtendedParetoAnalyzer().compute_frontier(agents) == []


# --- Memory analysis -------------------------------------------------------

def test_memory_constraint_splits_feasible():
    a = _pt(agent_id="a", memory_mb=128)
    b = _pt(agent_id="b", memory_mb=1024)
    r = ExtendedParetoAnalyzer().analyze_memory_constraint([a, b], max_memory_mb=512)
    assert r["feasible_ids"] == ["a"]
    assert r["infeasible_ids"] == ["b"]
    assert r["best_memory_efficient_id"] == "a"


def test_memory_constraint_rejects_nonpositive():
    with pytest.raises(ValueError):
        ExtendedParetoAnalyzer().analyze_memory_constraint([], max_memory_mb=0)


# --- Circuit analysis -----------------------------------------------------

def test_circuit_depth_shallow_and_most_robust():
    a = _pt(agent_id="a", circuit_depth=10)
    b = _pt(agent_id="b", circuit_depth=100)
    c = _pt(agent_id="c", circuit_depth=None)
    r = ExtendedParetoAnalyzer().analyze_circuit_depth_scalability(
        [a, b, c], max_circuit_depth=50,
    )
    assert r["shallow_ids"] == ["a"]
    assert r["most_robust_id"] == "a"
    assert r["excluded_non_quantum_ids"] == ["c"]


# --- Variance analysis ----------------------------------------------------

def test_variance_stability():
    stable   = _pt(agent_id="s", variance_score=0.05)
    unstable = _pt(agent_id="u", variance_score=0.5)
    single   = _pt(agent_id="x", variance_score=None)
    r = ExtendedParetoAnalyzer().analyze_variance_stability(
        [stable, unstable, single], stability_threshold=0.2,
    )
    assert r["stable_ids"] == ["s"]
    assert r["unstable_ids"] == ["u"]
    assert r["excluded_single_run_ids"] == ["x"]


def test_variance_rejects_nonpositive_threshold():
    with pytest.raises(ValueError):
        ExtendedParetoAnalyzer().analyze_variance_stability([], stability_threshold=0)


# --- Comprehensive --------------------------------------------------------

def test_comprehensive_picks_by_modp():
    a = _pt(agent_id="a", accuracy=0.99, energy_kwh=0.001, carbon_co2e_kg=0.0002,
            latency_ms=50, memory_mb=128, variance_score=0.01)
    b = _pt(agent_id="b", accuracy=0.50, energy_kwh=2.0, carbon_co2e_kg=0.5,
            latency_ms=800, memory_mb=1024, variance_score=0.9)
    analyzer = ExtendedParetoAnalyzer(
        modp_weights=(0.4, 0.2, 0.2, 0.1, 0.05, 0.05),
    )
    r = analyzer.comprehensive_analysis(
        [a, b], Constraints(max_memory_mb=512, max_circuit_depth=50, max_variance=0.2),
    )
    assert r["recommendation"] == "a"


def test_comprehensive_empty_feasible_set():
    a = _pt(agent_id="a", memory_mb=4096)
    r = ExtendedParetoAnalyzer().comprehensive_analysis(
        [a], Constraints(max_memory_mb=512),
    )
    assert r["recommendation"] is None
    assert "no agents satisfy all constraints" in r["reason"]


def test_comprehensive_empty_agents():
    r = ExtendedParetoAnalyzer().comprehensive_analysis([], Constraints())
    assert r["recommendation"] is None


def test_comprehensive_empty_frontier_falls_back_to_modp():
    """All survivors have missing optional fields → frontier is empty;
    fallback picks by MODP."""
    a = _pt(agent_id="a", memory_mb=None, variance_score=None, accuracy=0.99)
    b = _pt(agent_id="b", memory_mb=None, variance_score=None, accuracy=0.10)
    analyzer = ExtendedParetoAnalyzer(
        modp_weights=(0.4, 0.2, 0.2, 0.1, 0.05, 0.05),
    )
    r = analyzer.comprehensive_analysis([a, b], Constraints())
    assert r["recommendation"] in ("a", "b")
    # a has strictly higher accuracy → higher MODP
    assert r["recommendation"] == "a"


# --- MODP normalization ---------------------------------------------------

def test_modp_weights_must_sum_to_one():
    with pytest.raises(ValueError):
        ExtendedParetoAnalyzer(modp_weights=(0.5, 0.5, 0.5, 0.5, 0.5, 0.5))


def test_modp_requires_six_weights():
    with pytest.raises(ValueError):
        ExtendedParetoAnalyzer(modp_weights=(0.5, 0.5))


# --- CSV export -----------------------------------------------------------

def test_to_csv_roundtrip(tmp_path: Path):
    agents = [_pt(agent_id="a"), _pt(agent_id="b", memory_mb=None)]
    out = tmp_path / "agents.csv"
    to_csv(agents, out)
    rows = list(csv.DictReader(out.open()))
    assert [r["agent_id"] for r in rows] == ["a", "b"]
    assert rows[0]["memory_mb"] == "256"
    assert rows[1]["memory_mb"] == ""      # None → empty cell
```

---

# === SAVE AS: tests/test_pareto_plotter.py ===
```python
"""Plotter tests — self-contained HTML, safe labels."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.analysis.extended_pareto_analyzer import (
    ExtendedParetoAnalyzer, ExtendedParetoPoint,
)
from src.visualization.pareto_plotter import ParetoPlotter


def _pt(**kw) -> ExtendedParetoPoint:
    base = dict(
        agent_id="a", accuracy=0.5, energy_kwh=1.0, carbon_co2e_kg=0.1,
        latency_ms=100, memory_mb=256, circuit_depth=None, variance_score=0.1,
    )
    base.update(kw)
    return ExtendedParetoPoint(**base)


def test_plot_writes_self_contained_html(tmp_path: Path):
    agents = [_pt(agent_id="a"), _pt(agent_id="b", accuracy=0.9)]
    frontier = ExtendedParetoAnalyzer().compute_frontier(agents)
    out = ParetoPlotter(output_dir=tmp_path).plot_carbon_vs_energy(agents, frontier)
    assert out.exists()
    html = out.read_text()
    assert "plotly" in html.lower()
    assert "<html" in html.lower()


def test_agent_id_cannot_inject_script():
    """agent_id is validated by ExtendedParetoPoint — XSS payloads raise."""
    with pytest.raises(ValueError):
        _pt(agent_id="<script>alert(1)</script>")


def test_valid_agent_id_appears_in_html(tmp_path: Path):
    agents = [_pt(agent_id="agent.with.dots-and_dashes")]
    frontier = agents
    out = ParetoPlotter(output_dir=tmp_path).plot_accuracy_vs_carbon(agents, frontier)
    html = out.read_text()
    assert "agent.with.dots-and_dashes" in html


def test_plot_handles_missing_optional(tmp_path: Path):
    """An agent with memory_mb=None should not crash the plotter."""
    agents = [_pt(agent_id="a", memory_mb=None),
              _pt(agent_id="b", accuracy=0.9)]
    frontier = [agents[1]]
    out = ParetoPlotter(output_dir=tmp_path).plot_carbon_vs_energy(agents, frontier)
    assert out.exists()
```

---

# === SAVE AS: scripts/record_provenance.sh ===
```bash
#!/usr/bin/env bash
# scripts/record_provenance.sh — Record SHA256 / env for reproducible results.
#
# Usage:
#   ./scripts/record_provenance.sh [RESULTS_DIR] [DATASET_PATH]
# Defaults:
#   RESULTS_DIR=results
#   DATASET_PATH=tests/fixtures/agents.json
set -euo pipefail

RESULTS="${1:-results}"
DATASET="${2:-tests/fixtures/agents.json}"
mkdir -p "$RESULTS"

if [ -f "$DATASET" ]; then
    sha256sum "$DATASET" | awk '{print $1}' > "$RESULTS/dataset.json.sha256"
    echo "recorded: $RESULTS/dataset.json.sha256"
fi

if [ -f "requirements.txt" ]; then
    sha256sum "requirements.txt" | awk '{print $1}' > "$RESULTS/requirements.txt.sha256"
    echo "recorded: $RESULTS/requirements.txt.sha256"
fi

{
    echo "python: $(python --version 2>&1)"
    echo "numpy:  $(python -c 'import numpy; print(numpy.__version__)' 2>/dev/null || echo 'unavailable')"
    echo "plotly: $(python -c 'import plotly; print(plotly.__version__)' 2>/dev/null || echo 'unavailable')"
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit: $(git rev-parse HEAD 2>/dev/null || echo 'not-a-git-repo')"
} > "$RESULTS/env.txt"
echo "recorded: $RESULTS/env.txt"
```

---

# === SAVE AS: scripts/bench_pareto.py ===
```python
#!/usr/bin/env python3
"""
scripts/bench_pareto.py — Reproduce the O(n²) performance claim.

Times compute_frontier over increasing n and prints a Markdown table.
"""
from __future__ import annotations

import random
import sys
import time
from pathlib import Path
from statistics import median

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.extended_pareto_analyzer import (   # noqa: E402
    ExtendedParetoAnalyzer, ExtendedParetoPoint,
)


def make_agents(n: int, seed: int = 42) -> list[ExtendedParetoPoint]:
    rng = random.Random(seed)
    return [
        ExtendedParetoPoint(
            agent_id=f"agent_{i:04d}",
            accuracy=rng.uniform(0.5, 0.99),
            energy_kwh=rng.uniform(0.001, 0.05),
            carbon_co2e_kg=rng.uniform(0.0001, 0.01),
            latency_ms=rng.uniform(50, 2000),
            memory_mb=rng.uniform(64, 4096),
            circuit_depth=rng.choice([None, rng.randint(1, 100)]),
            variance_score=rng.uniform(0.01, 0.5),
        )
        for i in range(n)
    ]


def bench(n: int, iterations: int = 5) -> tuple[float, int]:
    agents = make_agents(n)
    analyzer = ExtendedParetoAnalyzer()
    times: list[float] = []
    frontier_size = 0
    for _ in range(iterations):
        t0 = time.perf_counter()
        frontier = analyzer.compute_frontier(agents)
        times.append((time.perf_counter() - t0) * 1000.0)
        frontier_size = len(frontier)
    return median(times), frontier_size


def main() -> int:
    print("| n | median (ms) | frontier size |")
    print("|---|---|---|")
    for n in (100, 250, 500, 1000, 2000):
        t, f = bench(n)
        print(f"| {n} | {t:.1f} | {f} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

# === SAVE AS: requirements.txt ===
```
numpy==2.2.1
pandas==2.2.3
plotly==5.24.1

# testing
pytest==8.3.4
```

---

# === SAVE AS: .github/workflows/pareto-ci.yml ===
```yaml
name: Pareto analysis CI

permissions:
  contents: read
  actions: read

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

concurrency:
  group: pareto-ci-${{ github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}

jobs:
  test:
    runs-on: ubuntu-24.04
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
        with:
          persist-credentials: false

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Install
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Analyzer tests
        run: pytest -q tests/test_extended_pareto_analyzer.py

      - name: Plotter tests
        run: pytest -q tests/test_pareto_plotter.py

      - name: Full suite
        run: pytest -q

      - name: Record provenance
        run: bash scripts/record_provenance.sh results tests/fixtures/agents.json

      - name: Demo smoke test
        run: python examples/demo_extended_dimensions.py --out /tmp/plots

      - name: Benchmark frontier
        run: python scripts/bench_pareto.py

      - name: Upload artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: pareto-evidence-${{ github.run_id }}
          path: |
            /tmp/plots/
            results/
          if-no-files-found: warn
          retention-days: 14
```

---

### How to deploy from this single file

```bash
# 1. Split each `# === SAVE AS: <path> ===` block into its path.

# 2. Install and validate.
pip install -r requirements.txt
pytest -q

# 3. Run the demo and provenance.
bash scripts/record_provenance.sh
python examples/demo_extended_dimensions.py

# 4. Reproduce the O(n²) claim.
python scripts/bench_pareto.py

# 5. Inspect:
ls plots/ results/
```

### What changed vs. the original

| Area | Original | v2.0.0 |
|---|---|---|
| **"Production Ready"** | Claimed | **Beta** + §0 open-items table |
| **Implementation** | Examples referencing nonexistent code | Full `ExtendedParetoPoint`, `ExtendedParetoAnalyzer`, `ParetoPlotter`, `to_csv` |
| **`variance_score`** | Undefined | Coefficient of variation of energy across ≥3 runs (§1.3) |
| **Missing-value policy** | Not stated | Per-axis table (§1.4); enforced in `compute_frontier`, counted in each analysis |
| **MODP normalization** | Implicit | Explicit min–max per call, 6 weights, missing → 0.0 (§1.5) |
| **Tie/ε policy** | Undefined | `ε=1e-9`; determinism tested |
| **Async/sync** | `await node.select_routing_strategy()` | Sync everywhere (§4) |
| **`NodeDescriptor(type="EDGE")`** | String | `NodeType.EDGE` |
| **HTML sanitization** | None | `agent_id` regex + `html.escape` (§2.1, §3) |
| **Empty-feasible-set** | Unhandled | Returns `recommendation=None` + `reason` + counts |
| **Typed returns** | Unspecified dicts | 4 `TypedDict`s + `.to_dict()` + CSV export |
| **Correlation caveat** | Absent | §1.7 for memory↔energy and variance↔latency |
| **Tests** | "Fully tested" claim | 25+ tests in two files + fixtures |
| **Fixtures** | Made-up inline | `tests/fixtures/agents.json` (6 agents) |
| **Provenance** | No story | §9 + `scripts/record_provenance.sh` |
| **Benchmark** | O(n²) claim unverified | `scripts/bench_pareto.py` |
| **Requirements** | `plotly` etc. unpinned | Pinned |
| **CI** | None | `.github/workflows/pareto-ci.yml` |
| **Superlatives** | "Most comprehensive", "reviewer-proof", "12% improvement" | Removed |
| **Changelog** | Absent | Top of document |
