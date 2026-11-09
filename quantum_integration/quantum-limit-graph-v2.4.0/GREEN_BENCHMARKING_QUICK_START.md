
# Green Agent Benchmarking — Quick Start Guide

**Package:** `limit-agentbench` · **Version:** 2.4.2 · **Status:** Beta · **Date:** <2026-11-9>
**Python:** ≥3.10 · **License:** Apache-2.0
**Full API reference:** [`README.md`](README.md)
**Known limitations:** [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md)

> **Beta notice** — every example in this guide is verified against the
> shipped `limit_agentbench` package by `scripts/verify_quickstart_examples.py`
> (runs in CI). "Production Ready" awaits the open items in `README.md §0`.
> See [§11 Known limitations](#11-known-limitations) and
> [§12 What's not yet implemented](#12-whats-not-yet-implemented).

---

## Table of contents

- [1. What is LIMIT-AgentBench](#1-what-is-limit-agentbench)
- [2. Install](#2-install)
- [3. Understanding the Sustainability Index](#3-understanding-the-sustainability-index)
- [4. Quick start examples](#4-quick-start-examples)
- [5. Supported frameworks](#5-supported-frameworks)
- [6. Key metrics](#6-key-metrics)
- [7. Grid regions and hardware profiles](#7-grid-regions-and-hardware-profiles)
- [8. Common use cases](#8-common-use-cases)
- [9. Running the demo](#9-running-the-demo)
- [10. Troubleshooting](#10-troubleshooting)
- [11. Known limitations](#11-known-limitations)
- [12. What's not yet implemented](#12-whats-not-yet-implemented)
- [13. Related work](#13-related-work)
- [14. Next steps](#14-next-steps)
- [Appendix A — What changed from prior versions](#appendix-a--what-changed-from-prior-versions)

---

## 1. What is LIMIT-AgentBench

`limit-agentbench` is a green benchmarking layer for AI agents. It:

- Evaluates agents against an **AgentBench-compatible** task protocol.
- Tracks **energy** and **carbon** on every evaluation.
- Adapts **LangChain**, **AutoGen**, **CrewAI**, and **LIMIT-GRAPH** agents
  through a single `BaseAdapter` interface.
- Ranks agents across frameworks on a **unified leaderboard** using a
  **Sustainability Index** in `[0, 1]`.

Full context: [`README.md`](README.md).

---

## 2. Install

### 2.1 Install dependencies

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -e . --no-deps
```

`requirements.txt` pins `pytest` for tests. The package itself has **no
runtime dependencies** beyond the Python standard library — earlier guides
that suggested `pip install psutil numpy` were incorrect (neither is used by
the shipped package; see [Appendix A](#appendix-a--what-changed-from-prior-versions)).

### 2.2 Verify installation

```bash
python test_installation.py
```

Expected output (each `[PASS]` is a check; `[SKIP]` is fine when optional
modules are absent; any `[FAIL]` exits non-zero):

```
[PASS] core imports
[PASS] adapters imports (BaseAdapter, not BaseAgentAdapter)
[PASS] metrics imports
[PASS] dashboard imports
[PASS] integration imports (Phase 2/6 stubs)
[PASS] GreenMetricsTracker is EnergyTracker
[PASS] SustainabilityIndex formula (proposal examples reconcile to [0,1])
[PASS] CarbonCalculator reconciles with proposal §3 examples
[PASS] EfficiencyScorer returns unbounded accuracy/energy_kwh
[PASS] EvalResult is a dataclass (attribute access)
Installation verified successfully
```

Note: `test_installation.py` uses `[PASS]`/`[SKIP]`/`[FAIL]` line format —
not the `✓` checkmarks earlier guides showed.

---

## 3. Understanding the Sustainability Index

The **Sustainability Index (SI)** is a single `[0, 1]` score combining
accuracy, energy, and carbon. Higher is better.

### 3.1 Formula

This is the **canonical formula** (identical to `README.md §2.2`):

```
a = accuracy / accuracy_ceiling
e = max(0, 1 − energy_kwh / energy_ceiling_kwh)
c = max(0, 1 − carbon_co2e_kg / carbon_ceiling_kg)

si = w_acc·a + w_energy·e + w_carbon·c        (clamped to [0, 1])
```

### 3.2 Defaults

| Parameter | Default |
|---|---|
| `accuracy_ceiling` | `1.0` |
| `energy_ceiling_kwh` | `1.0` |
| `carbon_ceiling_kg` | `1.0` |
| `weights` | `(0.5, 0.25, 0.25)` (must sum to 1.0) |

### 3.3 Rating rubric (illustrative)

These thresholds are illustrative — they are **not** a scientific
classification. Adjust per task if you need a different scale.

| Rating | SI range |
|---|---|
| Excellent | `≥ 0.90` |
| Very Good | `≥ 0.75` |
| Good | `≥ 0.60` |
| Fair | `≥ 0.40` |
| Poor | `< 0.40` |

**Prior-guide issue:** earlier versions of this guide used
`(accuracy × 0.4 + efficiency × 0.3) / (carbon × 0.3)` with ratings
"Excellent: SI ≥ 200". That formula is mathematically unbounded and
produced values in the hundreds-of-thousands on the same inputs — it never
matched `README.md §2.2` and is now removed.

### 3.4 `efficiency_score` is not SI

`EfficiencyScorer` computes `accuracy / energy_kwh` — an **unbounded**
ratio. Do not confuse it with the `[0, 1]` Sustainability Index:

- `efficiency_score` = `accuracy / energy_kwh` (e.g., `0.96 / 0.0038 = 252.63`)
- `sustainability_index` = composite `[0, 1]` (e.g., `0.9788`)

Both are reported in `EvalResult.metrics`; they measure different things.

---

## 4. Quick start examples

Every example below is runnable as-is against the shipped package and is
exercised in CI by `scripts/verify_quickstart_examples.py`.

### Example 1 — Basic agent evaluation

```python
from limit_agentbench import AgentBenchAdapter


def my_agent(input_data: dict) -> dict:
    return {
        "agent_name": "MyAgent-v1",
        "accuracy": 0.95,
        "answer": "Artificial Intelligence",
    }


adapter = AgentBenchAdapter(grid_region="US-CA", hardware_profile="nvidia_a100")

task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is AI?"},
    expected_output={"answer": "Artificial Intelligence"},
)

result = adapter.evaluate_agent(my_agent, task, track_energy=True)

# `result` is an EvalResult dataclass. Attribute access, not dict:
print(f"Accuracy: {result.accuracy:.2%}")                    # top-level
print(f"Energy:   {result.energy_kwh:.6f} kWh")              # top-level
print(f"Carbon:   {result.carbon_co2e_kg:.6f} kg CO2e")      # top-level
print(f"SI:       {result.metrics['sustainability_index']:.4f}")  # in .metrics
```

**Note:** `accuracy`, `energy_kwh`, `carbon_co2e_kg`, and `latency_ms`
live at the top level of `EvalResult` (not inside `metrics`). Only
composite scores (`sustainability_index`, `efficiency_score`) live inside
`result.metrics`.

### Example 2 — Green metrics tracking

```python
from limit_agentbench import GreenMetricsTracker

tracker = GreenMetricsTracker(grid_region="US-CA", hardware_profile="nvidia_a100")

with tracker:
    # Your agent execution goes here.
    _ = {"anything": "the agent runs"}

m = tracker.get_metrics()
print(f"Energy:   {m.energy_kwh:.6f} kWh")
print(f"Carbon:   {m.carbon_co2e_kg:.6f} kg CO2e")
print(f"Power:    {m.power_watts} W peak / {m.effective_watts} W effective")
print(f"Duration: {m.duration_s * 1000:.2f} ms")
```

`GreenMetricsTracker` is an alias for `EnergyTracker` — same class, two
names. `m` is a `Metrics` dataclass (not a dict).

**Note:** `track_carbon` was **removed** in v2.4.2. Energy tracking
inherently derives carbon (`energy × grid_intensity / 1000`); there is no
separate `track_carbon` flag. See `README.md §2.5`.

### Example 3 — Framework comparison

Compare two framework adapters side-by-side:

```python
from limit_agentbench.adapters import LangChainAdapter, AutoGenAdapter


def lc_agent(input_data: dict) -> dict:
    return {"agent_name": "LC-Agent", "accuracy": 0.92}


def ag_agent(input_data: dict) -> dict:
    return {"agent_name": "AG-Agent", "accuracy": 0.88}


lc = LangChainAdapter(lc_agent)
ag = AutoGenAdapter(ag_agent)

lc_results = [lc.run({"prompt": "x"}) for _ in range(3)]
ag_results = [ag.run({"prompt": "x"}) for _ in range(3)]

lc_avg = sum(r.metrics["sustainability_index"] for r in lc_results) / len(lc_results)
ag_avg = sum(r.metrics["sustainability_index"] for r in ag_results) / len(ag_results)

print(f"LangChain avg SI: {lc_avg:.4f}")
print(f"AutoGen   avg SI: {ag_avg:.4f}")
```

**Note:** `AgentEvaluator.compare_agents(agents=[...], ...)` expects
**callable agents** (functions), not framework adapters. To compare
framework adapters, use each adapter's `.run()` and aggregate — as shown
above. A `compare_adapters(...)` convenience method is tracked as a
Phase 2 improvement.

### Example 4 — Leaderboard submission

```python
from limit_agentbench import GreenLeaderboard

lb = GreenLeaderboard()

lb.submit(
    agent_name="MyAgent-v1", framework="langchain",
    task_suite="agentbench_qa",
    accuracy=0.95, energy_kwh=0.003,
    carbon_co2e_kg=0.0006, latency_ms=150,
)
lb.submit(
    agent_name="MyAgent-v2", framework="langchain",
    task_suite="agentbench_qa",
    accuracy=0.93, energy_kwh=0.002,
    carbon_co2e_kg=0.0004, latency_ms=120,
)

# get_top_agents(n) is shorthand for get_rankings(
#     sort_by="sustainability_index", top_k=n
# )
top = lb.get_top_agents(n=10)
for i, entry in enumerate(top, 1):
    print(f"{i}. {entry.agent_name} ({entry.framework}): "
          f"SI={entry.sustainability_index:.4f}")
```

`LeaderboardEntry` is a dataclass — use attribute access
(`entry.agent_name`), not dict access. There is no `rank` field on
`LeaderboardEntry`; the caller's loop index (`i`) is the rank.

**Note:** `get_top_agents(n)` only accepts `n`. To sort by another metric,
use `get_rankings(sort_by="accuracy", top_k=10)` (see `README.md §3.5`).

### Example 5 — Sustainability Index (standalone)

```python
from limit_agentbench.metrics import SustainabilityIndex


def rating(si: float) -> str:
    if si >= 0.90: return "Excellent"
    if si >= 0.75: return "Very Good"
    if si >= 0.60: return "Good"
    if si >= 0.40: return "Fair"
    return "Poor"


si_calc = SustainabilityIndex()
value = si_calc.calculate(
    accuracy=0.95,
    energy_kwh=0.003,
    carbon_co2e_kg=0.0006,
)
print(f"SI: {value:.4f} ({rating(value)})")
# → SI: 0.9741 (Excellent)
```

Note: `rating()` is a small local helper — the package does **not**
provide a `SustainabilityIndex.get_rating()` method. Adding one is a
Phase 2 convenience item.

### Example 6 — Carbon calculation and equivalencies

```python
from limit_agentbench.metrics import CarbonCalculator

cc = CarbonCalculator()

kg = cc.calculate(energy_kwh=0.003, grid_region="US-CA")
# → 0.003 kWh × 220 gCO2e/kWh / 1000 = 0.00066 kg CO2e

eq = cc.equivalencies(carbon_co2e_kg=kg)
# eq["miles_driven"] and eq["tree_years"] — EPA 2023 reference values

print(f"Carbon:  {kg:.6f} kg CO2e")
print(f"Miles:   {eq['miles_driven']:.4f}")
print(f"Trees:   {eq['tree_years']:.4f} tree-years")
```

**Note:** `CarbonCalculator.__init__()` takes **no arguments**. The grid
region is passed to `.calculate(...)`, not the constructor. Prior guides
showed `CarbonCalculator(grid_region="US-CA")` — that was incorrect.

---

## 5. Supported frameworks

Every adapter subclasses `BaseAdapter` and exposes **both** `.run()` and
`.evaluate_agent()`.

### LangChain

```python
from limit_agentbench.adapters import LangChainAdapter

result = LangChainAdapter(my_langchain_agent).run({"prompt": "hi"})
```

### AutoGen

```python
from limit_agentbench.adapters import AutoGenAdapter

result = AutoGenAdapter(my_autogen_agent).run({"prompt": "hi"})
```

### CrewAI

```python
from limit_agentbench.adapters import CrewAIAdapter

result = CrewAIAdapter(my_crewai_agent).run({"prompt": "hi"})
```

### LIMIT-GRAPH

```python
from limit_agentbench.adapters import LimitGraphAdapter

result = LimitGraphAdapter(my_limit_graph_agent).run({"prompt": "hi"})
```

### Custom framework

```python
from limit_agentbench.adapters import BaseAdapter


class MyCustomAdapter(BaseAdapter):
    framework = "my-framework"

    def _call_agent(self, input_data: dict) -> dict:
        # Bridge your framework's native API here.
        return my_agent(input_data)
```

Every adapter's underlying agent must be **callable**, taking a `dict` and
returning a `dict` with at least:
- `"accuracy": float ∈ [0, 1]`
- `"agent_name": str` (optional; defaults to `"agent"`)

---

## 6. Key metrics

### Performance metrics

| Metric | Unit | Range | Where |
|---|---|---|---|
| `accuracy` | ratio | `[0, 1]` | `EvalResult.accuracy` |
| `latency_ms` | ms | `≥ 0` | `EvalResult.latency_ms` |

### Green metrics

| Metric | Unit | Range | Where |
|---|---|---|---|
| `energy_kwh` | kWh | `≥ 0` | `EvalResult.energy_kwh`, `Metrics.energy_kwh` |
| `carbon_co2e_kg` | kg CO₂e | `≥ 0` | `EvalResult.carbon_co2e_kg`, `Metrics.carbon_co2e_kg` |
| `power_watts` (peak) | W | `> 0` | `Metrics.power_watts` (peak TDP) |
| `effective_watts` | W | `> 0` | `Metrics.effective_watts` (calibrated inference draw) |
| `sustainability_index` | ratio | `[0, 1]` | `EvalResult.metrics["sustainability_index"]` |
| `efficiency_score` | accuracy/kWh | `≥ 0` (unbounded) | `EvalResult.metrics["efficiency_score"]` |

Energy math uses **effective draw** (calibrated inference power), not
peak TDP. Peak is reported for burst analysis only. See `README.md §5.2`.

---

## 7. Grid regions and hardware profiles

### 7.1 Grid regions (13)

Static reference values. Production must wire a live provider
(`README.md §0.2 O-1`).

| Region | gCO₂e/kWh |
|---|---|
| US-CA | 220 |
| US-TX | 430 |
| US-NY | 220 |
| EU-FR | 70 |
| EU-DE | 380 |
| EU-NO | 30 |
| EU-PL | 700 |
| CN | 580 |
| IN | 630 |
| JP | 480 |
| AU | 550 |
| BR | 100 |
| GLOBAL | 475 |

### 7.2 Hardware profiles (10)

| Profile | Effective (W) | Peak TDP (W) |
|---|---|---|
| `nvidia_a100` | 250 | 400 |
| `nvidia_v100` | 220 | 300 |
| `nvidia_t4` | 70 | 70 |
| `nvidia_rtx_3090` | 320 | 350 |
| `nvidia_rtx_4090` | 380 | 450 |
| `amd_mi250` | 350 | 500 |
| `google_tpu_v4` | 200 | 200 |
| `cpu_intel_xeon` | 150 | 150 |
| `cpu_amd_epyc` | 180 | 180 |
| `default` | 100 | 100 |

**Prior-guide issue:** earlier versions of this guide listed only 9 grid
regions (missing EU-PL, JP, AU, BR) and 7 hardware profiles (missing
`cpu_intel_xeon`, `cpu_amd_epyc`, `default`), with diverging values.
Reconciled against `README.md §5.1–5.2` here.

---

## 8. Common use cases

### 8.1 Benchmark your agent

```python
from limit_agentbench import AgentBenchAdapter, BenchmarkHarness
from pathlib import Path

adapter = AgentBenchAdapter(grid_region="US-CA", hardware_profile="nvidia_a100")
tasks = [
    adapter.create_task(
        task_id=f"task_{i}", suite="qa", task_type="qa",
        input_data={"question": f"Q{i}"},
    )
    for i in range(5)
]

harness = BenchmarkHarness(output_dir=Path("./benchmark_results"))
out = harness.run_benchmark(
    agent=my_agent,
    tasks=tasks,
    benchmark_name="my_benchmark",
)
print(f"Wrote {out['path']}")
print(f"Avg accuracy: {out['summary']['avg_accuracy']:.4f}")
print(f"Avg SI:       {out['summary']['avg_sustainability_index']:.4f}")
```

**Note:** `BenchmarkHarness.__init__()` takes only `output_dir` — there is
no `grid_region` parameter. Grid region comes from the `AgentBenchAdapter`
(or, equivalently, from `AgentEvaluator(grid_region=...)`).

### 8.2 Compare multiple agents

```python
from limit_agentbench import AgentBenchAdapter, AgentEvaluator

adapter = AgentBenchAdapter(grid_region="US-CA")
tasks = [
    adapter.create_task(
        task_id=f"t{i}", suite="qa", task_type="qa", input_data={"q": "x"},
    )
    for i in range(3)
]

evaluator = AgentEvaluator(grid_region="US-CA", track_green_metrics=True)
rows = evaluator.compare_agents(
    agents=[agent1, agent2, agent3],   # plain callables
    tasks=tasks,
    sort_by="sustainability_index",
)
for i, row in enumerate(rows, 1):
    print(f"{i}. {row['agent_name']}: avg SI = {row['avg_sustainability_index']:.4f}")
```

`compare_agents` returns `list[dict]` with keys
`agent_name`, `avg_sustainability_index`, `results` (the per-task
`EvalResult` objects). There is no `rankings` wrapper and no `winner` key
in the return value.

### 8.3 Carbon savings (from energy reduction)

```python
from limit_agentbench.metrics import CarbonCalculator

cc = CarbonCalculator()

baseline_energy_kwh = 0.010
optimized_energy_kwh = 0.003

baseline_kg = cc.calculate(energy_kwh=baseline_energy_kwh, grid_region="US-CA")
optimized_kg = cc.calculate(energy_kwh=optimized_energy_kwh, grid_region="US-CA")
saved_kg = baseline_kg - optimized_kg
saved_pct = 100.0 * saved_kg / baseline_kg

print(f"Carbon baseline:  {baseline_kg:.6f} kg CO2e")
print(f"Carbon optimized: {optimized_kg:.6f} kg CO2e")
print(f"Saved:            {saved_kg:.6f} kg CO2e ({saved_pct:.1f}%)")
```

**Note:** `CarbonCalculator` does not have a `calculate_savings(...)`
method. Compute the delta explicitly as shown.

### 8.4 Rank configurations by Sustainability Index

```python
from limit_agentbench.metrics import SustainabilityIndex

si_calc = SustainabilityIndex()

configs = {
    "config_a": {"accuracy": 0.95, "energy_kwh": 0.005, "carbon_co2e_kg": 0.0011},
    "config_b": {"accuracy": 0.93, "energy_kwh": 0.002, "carbon_co2e_kg": 0.00044},
    "config_c": {"accuracy": 0.90, "energy_kwh": 0.001, "carbon_co2e_kg": 0.00022},
}

scored = [
    (name, si_calc.calculate(**m))
    for name, m in configs.items()
]
scored.sort(key=lambda x: x[1], reverse=True)

for i, (name, si) in enumerate(scored, 1):
    print(f"{i}. {name}: SI = {si:.4f}")
```

**Note:** `SustainabilityIndex` does not have a `rank_agents(...)` method.
Compute a list of `(name, si)` tuples and sort as shown.

---

## 9. Running the demo

```bash
python demo_green_benchmark.py
```

The demo runs five sections:

1. AgentBench protocol — create task, evaluate agent.
2. Green metrics — `SustainabilityIndex` + `EfficiencyScorer` on the same
   inputs.
3. Multi-framework comparison — `AgentEvaluator.compare_agents` with two
   plain callables.
4. Leaderboard — submit two entries, print top 3.
5. Benchmark harness — write a benchmark JSON to a temp directory and
   print its summary.

Expected tail output:

```
=== [5] Benchmark harness ===
  wrote /tmp/.../demo_bench.json
  avg SI = 0.9550
=== done ===
```

(Values depend on the agents' reported accuracy; the demo uses fixed
accuracies so the values are deterministic.)

---

## 10. Troubleshooting

### `ModuleNotFoundError: No module named 'limit_agentbench'`

```bash
# You're in the wrong directory, or pip install -e . didn't run.
cd limit-agentbench
pip install -e . --no-deps
python test_installation.py
```

### `AttributeError: 'EvalResult' object has no attribute '__getitem__'`

You used `result['metrics'][...]` — `EvalResult` is a **dataclass**, use
attribute access: `result.metrics[...]` and `result.accuracy`.

### `TypeError: 'LangChainAdapter' object is not callable`

`AgentEvaluator.compare_agents(agents=[...])` expects **callable** agents,
not adapters. Use `.run()` on each adapter and aggregate manually (see
[§4 Example 3](#example-3--framework-comparison)). Passing framework
adapters to `compare_agents` is a Phase 2 improvement.

### `KeyError: 'rank'` on a leaderboard entry

`LeaderboardEntry` has no `rank` field. Use the loop index:

```python
for rank, entry in enumerate(lb.get_top_agents(n=10), start=1):
    print(f"{rank}. {entry.agent_name}")
```

### `SI is 0.98, but the rating says "Poor"`

The rating rubric must be `[0, 1]`-aligned (see
[§3.3](#33-rating-rubric-illustrative)). Prior guides used "SI ≥ 200"
which is impossible for `[0, 1]` values and has been corrected.

### Carbon or energy values look 10× off

Check that you're using `grid_region="US-CA"` (220 gCO₂e/kWh) and not a
different region. Prior guides used 200 g/kWh for US-CA — that was
incorrect and has been corrected (see [§7.1](#71-grid-regions-13)).

---

## 11. Known limitations

Full list in [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md). Highlights:

1. **Static grid intensities** — production must wire a live provider
   (`README.md §0.2 O-1`).
2. **TDP-calibrated power** — ±20% error on energy estimates (not
   RAPL/NVML-measured).
3. **`expected_output` is not used for scoring** — accepted and included
   in the provenance hash, but does not affect `EvalResult.accuracy`.
   Similarity scoring is Phase 2 (`README.md §0.2 O-2`).
4. **No `track_carbon`** — energy tracking inherently derives carbon.
5. **No `get_rating()` on `SustainabilityIndex`** — use the local helper
   in [§4 Example 5](#example-5--sustainability-index-standalone).
6. **No `compare_adapters()`** — use `.run()` per adapter and aggregate.
7. **No `CarbonCalculator.calculate_savings()`** — compute the delta
   explicitly.
8. **`compare_agents(agents=[...])` requires callables**, not framework
   adapters.
9. **No visualization dashboard** — Phase 3.
10. **No public leaderboard API** — Phase 4.
11. **Package is Beta** — not Production Ready.

---

## 12. What's not yet implemented

Listed so you don't expect them from this guide:

| Feature | Status | Reference |
|---|---|---|
| Similarity-based scoring (`f1_score`) | Phase 2 | `README.md §0.2 O-2` |
| NSN / Level 5 / limit-benchmark bridges | Phase 2 (stubs raise `NotImplementedError`) | `ARCHITECTURE_PROPOSAL.md §5` |
| Visualization (energy charts, carbon dashboard, comparison matrix) | Phase 3 | `README.md §10` |
| Public leaderboard REST API | Phase 4 | `README.md §10` |
| Hugging Face Spaces deployment | Phase 6 | `README.md §10` |
| `SustainabilityIndex.get_rating()` | Not planned | Use local helper |
| `CarbonCalculator.calculate_savings()` | Not planned | Compute delta manually |
| `AgentEvaluator.compare_adapters()` | Not planned | Use `.run()` per adapter |
| Live grid-intensity provider | Phase 2 | `README.md §0.2 O-1` |

---

## 13. Related work

Prior guides claimed this is the "first in industry" green agent
benchmarking platform. That claim is unfalsifiable. Here is a factual
comparison:

| Capability | AgentBench | MLPerf Power | Green500 | LIMIT-AgentBench |
|---|---|---|---|---|
| Protocol-standardized task format | ✅ | ❌ | ❌ | ✅ |
| Green metrics (energy, carbon) | ❌ | ✅ | ✅ (energy only) | ✅ |
| Cross-framework agent adapters | ❌ | ❌ | ❌ | ✅ (4 frameworks) |
| Sustainability Index (`[0,1]` composite) | ❌ | ❌ | ❌ | ✅ |
| Cross-framework leaderboard | ❌ | ❌ | ✅ (supercomputers) | ✅ (agents) |
| Quantum evaluation | ❌ | ❌ | ❌ | ✅ (existing LIMIT-GRAPH) |

None of these individual capabilities is novel in isolation. The
contribution is **combining them for the agent-benchmarking use case**.

---

## 14. Next steps

1. **Read the full API reference**: [`README.md`](README.md).
2. **Read the architecture proposal**: [`ARCHITECTURE_PROPOSAL.md`](ARCHITECTURE_PROPOSAL.md).
3. **Read known limitations**: [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md).
4. **Run the tests**: `pytest -q tests/test_smoke.py`.
5. **Integrate with your agent** using [§4](#4-quick-start-examples).
6. **Submit to the leaderboard** ([§4 Example 4](#example-4--leaderboard-submission)).

---

## Appendix A — What changed from prior versions

| Area | Prior guide | This guide |
|---|---|---|
| **SI formula** | `(accuracy × 0.4 + efficiency × 0.3) / (carbon × 0.3)` — unbounded, produced `529,894` on example inputs | `w_acc·a + w_energy·e + w_carbon·c`, clamped to `[0, 1]` (§3.1) |
| **SI weights** | 40/30/30 | 50/25/25 (§3.2, matches `README.md §2.2`) |
| **Rating scale** | "Excellent: SI ≥ 200" | `[0, 1]`-aligned: Excellent ≥ 0.90 (§3.3) |
| **`get_rating`** | `si_calc.get_rating(si)` (doesn't exist) | Local helper function (§4 Example 5) |
| **Dict access** | `result['metrics']['...']` | `result.metrics[...]` + `result.accuracy` etc. (§4, §10) |
| **`track_carbon`** | `evaluate_agent(..., track_carbon=True)` | Removed — energy tracking derives carbon (§4 Example 2) |
| **`CarbonCalculator` signature** | `CarbonCalculator(grid_region="US-CA")` | `CarbonCalculator()` + `.calculate(..., grid_region=...)` (§4 Example 6) |
| **`CarbonCalculator.calculate_savings`** | Called | Manual delta (§8.3) |
| **`trees_equivalent`** | `savings['trees_equivalent']` | `eq['tree_years']` from `.equivalencies()` (§4 Example 6) |
| **`BenchmarkHarness`** | `BenchmarkHarness(output_dir, grid_region="US-CA")` | `BenchmarkHarness(output_dir=...)` — no grid_region (§8.1) |
| **`get_top_agents`** | `get_top_agents(n=10, sort_by="...")` | `get_top_agents(n=10)` only; use `get_rankings(sort_by=...)` for custom sort (§4 Example 4) |
| **`compare_agents` return** | `comparison['rankings']`, `rank['aggregated_metrics']` | `list[dict]` with `agent_name`, `avg_sustainability_index`, `results` (§8.2) |
| **`LeaderboardEntry`** | `entry['rank']`, `entry['metrics']` | Dataclass attribute access; no `rank` field (§4 Example 4) |
| **`SustainabilityIndex.compare_agents`** | Called with `agent_a_metrics`/`agent_b_metrics` | Doesn't exist; use `EfficiencyScorer` + manual comparison |
| **`SustainabilityIndex.rank_agents`** | Called | Doesn't exist; sort a list of tuples (§8.4) |
| **Install** | `pip install psutil numpy` (neither used) | `pip install -r requirements.txt` (§2.1) |
| **Install output** | `✓` checkmarks | `[PASS]`/`[SKIP]`/`[FAIL]` lines (§2.2) |
| **Grid regions** | 9 listed (missing EU-PL, JP, AU, BR), values diverged | 13 listed, values match `README.md §5.1` (§7.1) |
| **Hardware profiles** | 7 listed (missing cpu_intel_xeon, cpu_amd_epyc, default), TDP-only | 10 listed, effective-vs-peak split (§7.2) |
| **Status** | "Production Ready" | Beta (§ header) |
| **Date** | `Jan 19, 2026` (future) | `<YYYY-MM-DD>` |
| **Path** | `quantum_integration/...` | Repo-relative (§2.1) |
| **"Industry first"** | Claimed | Replaced with related-work table (§13) |
| **Dead references** | `GREEN_AGENT_BENCHMARKING_COMPLETE.md` (doesn't exist) | Removed; points to `README.md` / `ARCHITECTURE_PROPOSAL.md` |
| **`chmod ./benchmark_results`** | Referenced without context | Note that `output_dir` must exist or be creatable |
| **Verification** | None | `scripts/verify_quickstart_examples.py` in CI |
| **Drift guard** | None | `tests/test_quickstart_drift.py` asserts the guide matches `README.md` |
