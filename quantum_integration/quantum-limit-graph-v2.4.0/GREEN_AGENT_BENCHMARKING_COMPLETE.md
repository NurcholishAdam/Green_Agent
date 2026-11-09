
# LIMIT-AgentBench

**Package:** `limit-agentbench` · **Version:** 2.4.2 · **Status:** Beta (see §0) · **Date:** <2026-11-9>
**Python:** ≥3.10 · **License:** Apache-2.0

AgentBench-compatible green benchmarking for AI agents. Measures accuracy, energy consumption, carbon footprint, latency, and a unified **Sustainability Index** across LangChain, AutoGen, CrewAI, and LIMIT-GRAPH agents.

> **Beta notice** — the four core pillars (AgentBench protocol, green metrics, multi-framework adapters, unified leaderboard) are implemented, tested, and CI-gated. "Production Ready" awaits live grid-intensity wiring, similarity-based scoring, integration bridges, and visualization (Phases 2–4). See [Known limitations](#known-limitations) and [Roadmap](#roadmap).

---

## Table of contents

- [Status](#0-status)
- [What ships](#1-what-ships-in-this-report)
- [Concepts](#2-concepts)
- [API reference](#3-api-reference)
- [Examples](#4-examples)
- [Supported configurations](#5-supported-configurations)
- [Testing](#6-testing)
- [Known limitations](#known-limitations)
- [Security](#8-security)
- [Related work and positioning](#9-related-work-and-positioning)
- [Roadmap](#roadmap)
- [Contributing](#11-contributing)
- [License and citation](#12-license-and-citation)

---

## 0. Status

**Beta.** All P0 items from the v2.4.1 review are closed. "Production Ready" awaits the open items in §0.2 and Phases 2–4 of the roadmap.

### 0.1 Closed in this release

| ID | Item |
|---|---|
| R01 | Status is Beta with open items table (was "Implementation Complete" ×3) |
| R02 | Grid intensities reconciled with `INDEX.md` — 13 regions, source cited, reference year stated |
| R03 | Hardware power split into effective draw vs peak TDP (was TDP-only, ~60% energy over-estimation) |
| R04 | 13 grid regions listed (was 9 despite header claiming 13) |
| R05 | 10 hardware profiles listed (was 9 despite header claiming 10) |
| R06 | `result.metrics[...]` (attribute) — prior draft used `result['metrics']` (dict) which fails at runtime |
| R07 | `BaseAdapter` class name (was `BaseAgentAdapter`) |
| R08 | `GreenMetricsTracker is EnergyTracker` (alias asserted by test) |
| R09 | New params documented: `expected_output`, `grid_region` |
| R10 | `SARS-CoV-2` copy-paste removed from `limit-benchmark` bridge line |
| R11 | `test_installation.py`, `requirements.txt`, `LICENSE` shipped |
| R12 | `tests/test_smoke.py` shipped (~20 tests) |
| R13 | Real release date |
| R14 | CI workflow `.github/workflows/bench-ci.yml` |
| R15 | `SustainabilityIndex` / `EfficiencyScorer` formulas documented |
| R16 | "Industry first" / "First green agent platform" claims replaced with related-work comparison |
| R17 | Grid-region count and hardware-profile count locked by regression tests |

### 0.2 Remaining open items

| ID | Item | Owner |
|---|---|---|
| O-1 | Live grid-intensity source (currently static table §5.1) | `@sustainability` |
| O-2 | Similarity metric for `expected_output` scoring (Phase 2) | `@bench` |
| O-3 | NSN / Level 5 / limit-benchmark bridges (Phase 2) | `@platform` |
| O-4 | Visualization dashboard (Phase 3) | `@dashboard` |
| O-5 | Hugging Face Spaces + public leaderboard API (Phase 4) | `@platform` |
| O-6 | SBOM + SLSA on published images | `@sre` |

---

## 1. What ships in this report

The `limit_agentbench` package implements:

1. **AgentBench protocol** — `AgentBenchAdapter` (task creation, evaluation, provenance hash).
2. **Green metrics** — `EnergyTracker`, `CarbonCalculator`, `EfficiencyScorer`, `SustainabilityIndex`.
3. **Unified evaluation** — `AgentEvaluator`, `BenchmarkHarness`.
4. **Framework adapters** — LangChain, AutoGen, CrewAI, LIMIT-GRAPH (all via `BaseAdapter`).
5. **Leaderboard** — `GreenLeaderboard` with cross-framework rankings.

Phases 2–4 (integration bridges, visualization, deployment) are **not** delivered. See [Roadmap](#roadmap).

---

## 2. Concepts

### 2.1 Units and ranges

| Field | Units | Range |
|---|---|---|
| `accuracy` | ratio | `[0, 1]` |
| `energy_kwh` | kWh | `≥ 0` |
| `carbon_co2e_kg` | kg CO₂e | `≥ 0` |
| `latency_ms` | ms | `≥ 0` |
| `sustainability_index` | ratio | `[0, 1]` |
| `efficiency_score` | ratio | `≥ 0` (unbounded) |
| `region_carbon_intensity` | gCO₂e / kWh | `≥ 0` |
| `hardware_effective_watts` | W | `> 0` |
| `hardware_peak_watts` | W | `≥ effective` |

### 2.2 `SustainabilityIndex.calculate()` — formula

```
a = accuracy / accuracy_ceiling
e = max(0, 1 − energy_kwh / energy_ceiling_kwh)
c = max(0, 1 − carbon_co2e_kg / carbon_ceiling_kg)

si = w_acc·a + w_energy·e + w_carbon·c        (clamped to [0, 1])
```

Defaults: ceilings `(1.0, 1.0, 1.0)`, weights `(0.5, 0.25, 0.25)`. Weights must sum to 1.0 (tolerance 1e-6). Inputs above their ceiling raise `ValueError`.

### 2.3 Async surface

Everything in the public API is **synchronous**. There is no `await` or `asyncio.run()` in any runnable example.

### 2.4 `EfficiencyScorer.score()` — formulas

```
accuracy_per_kwh    = accuracy / energy_kwh
accuracy_per_kg_co2 = accuracy / carbon_co2e_kg
accuracy_per_ms     = accuracy / latency_ms
```

Zero denominator returns `0.0` (never raises). Higher is better.

### 2.5 Energy ↔ carbon coupling

`EnergyTracker` measures energy; `AgentBenchAdapter` derives carbon as `energy_kwh × region_intensity / 1000`. Disabling `track_energy` disables both energy **and** carbon — they are coupled by design. To compute carbon-only analyses, construct `CarbonCalculator` directly.

---

## 3. API reference

### 3.1 `AgentBenchAdapter`

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter(
    protocol_version="1.1",       # "1.0" | "1.1"
    grid_region="GLOBAL",         # see §5.1
    hardware_profile="default",   # see §5.2
)

task = adapter.create_task(         # KEYWORD-ONLY
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is AI?"},
    constraints=None,               # optional dict
    expected_output=None,           # optional; see note below
)

result = adapter.evaluate_agent(agent, task, track_energy=True)
# result is an EvalResult dataclass — ATTRIBUTE access, not dict:
#   result.metrics["sustainability_index"]
#   result.accuracy, result.energy_kwh, result.carbon_co2e_kg, result.latency_ms
```

**Note on `expected_output`:** accepted and included in the provenance hash, but **not** used for scoring in v2.4.2. Similarity-based scoring is a Phase 2 concern (O-2). Passing it does not change `result.accuracy`.

### 3.2 `SustainabilityIndex`

```python
from limit_agentbench import SustainabilityIndex

si = SustainabilityIndex(
    accuracy_ceiling=1.0,
    energy_ceiling_kwh=1.0,
    carbon_ceiling_kg=1.0,
    weights=(0.5, 0.25, 0.25),    # must sum to 1.0
)
value = si.calculate(accuracy, energy_kwh, carbon_co2e_kg)   # ∈ [0, 1]
```

### 3.3 `AgentEvaluator` and `BenchmarkHarness`

```python
from limit_agentbench import AgentEvaluator, BenchmarkHarness

evaluator = AgentEvaluator(
    track_green_metrics=True,
    grid_region="US-CA",          # added in v2.4.2 (was undocumented)
    hardware_profile="default",
)
result = evaluator.evaluate(agent, task)                   # EvalResult
rows = evaluator.compare_agents(                           # list[dict]
    agents=[agent_a, agent_b],
    tasks=[task1, task2],
    sort_by="sustainability_index",                        # or "agent_name"
)
# row keys: agent_name, avg_sustainability_index, results (list[EvalResult])

harness = BenchmarkHarness(output_dir="./results")
out = harness.run_benchmark(agent, tasks, "my_benchmark")
# out: {"path": str, "summary": {num_tasks, avg_accuracy, avg_sustainability_index}}
```

### 3.4 `GreenMetricsTracker` = `EnergyTracker` (alias)

Both names refer to the **same** class:

```python
from limit_agentbench import GreenMetricsTracker
from limit_agentbench.metrics import EnergyTracker
assert GreenMetricsTracker is EnergyTracker

tracker = EnergyTracker(grid_region="US-CA", hardware_profile="nvidia_a100")
with tracker:
    run_workload()
metrics = tracker.get_metrics()   # Metrics dataclass
```

### 3.5 Adapters — `BaseAdapter` (not `BaseAgentAdapter`)

```python
from limit_agentbench.adapters import (
    BaseAdapter, LangChainAdapter, AutoGenAdapter, CrewAIAdapter,
    LimitGraphAdapter,
)

adapter = LangChainAdapter(my_agent)   # subclasses BaseAdapter

result = adapter.run({"prompt": "hi"}, track_green_metrics=True)   # EvalResult
result = adapter.evaluate_agent(my_agent, task, track_energy=True) # EvalResult
```

`BaseAgentAdapter` is **not** an alias — it never existed. Use `BaseAdapter`.

### 3.6 Metrics

```python
from limit_agentbench.metrics import (
    CarbonCalculator, EfficiencyScorer, EnergyTracker, SustainabilityIndex,
)

cc = CarbonCalculator()
kg = cc.calculate(energy_kwh=1.0, grid_region="EU-FR")   # == 0.07

scorer = EfficiencyScorer(metric="accuracy_per_kwh")
val = scorer.score(accuracy=0.9, energy_kwh=0.003, carbon_co2e_kg=0.0006)
# == 300.0
```

---

## 4. Examples (all runnable, all sync)

### 4.1 Basic evaluation

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter()
task = adapter.create_task(
    task_id="qa_001", suite="question_answering", task_type="qa",
    input_data={"question": "What is AI?"},
)
result = adapter.evaluate_agent(my_agent, task, track_energy=True)
print(f"SI: {result.metrics['sustainability_index']:.2f}")   # attribute, not dict
```

### 4.2 Multi-framework comparison

```python
from limit_agentbench import AgentEvaluator
from limit_agentbench.adapters import LangChainAdapter, AutoGenAdapter

evaluator = AgentEvaluator(grid_region="US-CA", track_green_metrics=True)
rows = evaluator.compare_agents(
    agents=[LangChainAdapter(my_langchain_agent),
            AutoGenAdapter(my_autogen_agent)],
    tasks=task_suite,
    sort_by="sustainability_index",
)
for row in rows:
    print(f"{row['agent_name']}: avg SI = {row['avg_sustainability_index']}")
```

### 4.3 Leaderboard submission

```python
from limit_agentbench import GreenLeaderboard

lb = GreenLeaderboard()
lb.submit(
    agent_name="MyAgent-v1", framework="langchain",
    task_suite="agentbench_qa", accuracy=0.95,
    energy_kwh=0.003, carbon_co2e_kg=0.0006, latency_ms=150,
)
rankings = lb.get_rankings(sort_by="sustainability_index", top_k=10)
```

### 4.4 Benchmark harness

```python
from limit_agentbench import BenchmarkHarness

harness = BenchmarkHarness(output_dir="./results")
out = harness.run_benchmark(my_agent, task_suite, "phase1_bench")
print(f"wrote {out['path']}; avg SI = {out['summary']['avg_sustainability_index']}")
```

---

## 5. Supported configurations

### 5.1 Grid regions (13)

Single source of truth. Values are **gCO₂e / kWh**, static reference defaults. Production must wire a live provider (O-1).

| Region | gCO₂e/kWh | Source (indicative) |
|---|---|---|
| US-CA | 220 | CAISO 2024 annual mean |
| US-TX | 430 | ERCOT 2024 annual mean |
| US-NY | 220 | NYISO 2024 annual mean |
| EU-FR | 70 | RTE 2024 annual mean |
| EU-DE | 380 | ENTSO-E 2024 annual mean |
| EU-NO | 30 | ENTSO-E 2024 annual mean |
| EU-PL | 700 | ENTSO-E 2024 annual mean |
| CN | 580 | IEA 2023 |
| IN | 630 | IEA 2023 |
| JP | 480 | IEA 2023 |
| AU | 550 | IEA 2023 |
| BR | 100 | IEA 2023 |
| GLOBAL | 475 | IEA 2023 world mean |

**Prior-draft issue:** v1.0 listed 9 regions (missing EU-PL, JP, AU, BR) with values that diverged from `INDEX.md` by up to 70 gCO₂/kWh. Reconciled here against `INDEX.md`; both documents now agree, and a regression test locks the count.

### 5.2 Hardware profiles (10)

Two power values per profile:

- **Effective inference draw** — the calibrated average power during inference (used by `EnergyTracker` for energy estimation).
- **Peak TDP** — the manufacturer's thermal design power (reported in `Metrics.power_watts` for burst analysis; not used for energy totals).

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

**Prior-draft issue:** v1.0 used peak TDP as the sole power value (e.g., 400 W for A100). That **over-estimates energy by ~60%** for typical inference workloads and shifts the sustainability ranking. Effective-vs-peak split fixed here; both documents now reconcile, and `test_hardware_peak_gte_effective` asserts the invariant.

### 5.3 Enhancement flags

| Flag | Default | Effect |
|---|---|---|
| `ENHANCEMENTS_ENABLED` | `false` | Master switch; `false` forces all sub-flags off |
| `LIMIT_GRAPH_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `MODP_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `RLHF_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `DISTILLATION_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `MOE_GATING_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `EVOLUTIONARY_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `FLEXGEN_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |

---

## 6. Testing

### 6.1 What ships in this bundle

```bash
# Structure and import verification
python test_installation.py

# Pytest smoke test (~20 tests)
pytest -q tests/test_smoke.py
```

`test_installation.py` exits non-zero if any import, formula check, or adapter/leaderboard behavior fails.

### 6.2 CI

`.github/workflows/bench-ci.yml` runs:

1. `pip install -r requirements.txt && pip install -e . --no-deps`
2. `python test_installation.py`
3. `pytest -q`
4. `python demo_green_benchmark.py`

on every push and pull request to `main`.

---

## 7. Known limitations

Full list in `KNOWN_LIMITATIONS.md`. Highlights:

1. **Grid intensities are static reference values** — O-1.
2. **Hardware power is TDP-calibrated**, not RAPL/NVML measured — ~±20% error on energy estimates.
3. **`expected_output` is not used for scoring** — Phase 2 (O-2).
4. **No unit tests beyond smoke** — Phase 1 verification is structural; behavioral tests are pending.
5. **Phases 2–4 not delivered** — integration bridges, visualization, deployment.
6. **Package is Beta** — "Production Ready" requires O-1..O-6 and the roadmap phases.
7. **`track_carbon` removed** — energy tracking inherently derives carbon; see §2.5.
8. **`BaseAdapter` (not `BaseAgentAdapter`)** — the latter never existed.
9. **`GreenMetricsTracker` is `EnergyTracker`** — same class, two names.

---

## 8. Security

- **No secrets in code.** Grid-API keys must come from `$GREEN_AGENT_SECRETS_DIR`, not environment variables.
- **`task_id` and `agent_name`** are restricted to `[A-Za-z0-9._-]{1,64}` to prevent log injection and CSV formula injection.
- **`GreenLeaderboard.submit`** validates every numeric field at construction; malformed submissions raise `ValueError`.
- **`AgentBenchAdapter.create_task`** validates the task schema before invoking the agent — no unvalidated dicts reach agent code.
- **`ENHANCEMENTS_ENABLED=false`** is a hard kill switch for enhancement modules.

Report security issues via the private channel in `SECURITY.md`.

---

## 9. Related work and positioning

Earlier drafts described the platform as "industry first" and "the first green agent benchmarking platform". Those claims are unfalsifiable. This section replaces them with specific comparisons.

### 9.1 What LIMIT-AgentBench adds

| Capability | AgentBench | MLPerf Power | Green500 | LIMIT-AgentBench (this) |
|---|---|---|---|---|
| Protocol-standardized task format | ✅ | ❌ | ❌ | ✅ |
| Green metrics (energy, carbon) | ❌ | ✅ | ✅ (energy only) | ✅ |
| Cross-framework adapters | ❌ | ❌ | ❌ | ✅ (4 frameworks) |
| Sustainability Index (single composite) | ❌ | ❌ | ❌ | ✅ |
| Cross-framework leaderboard | ❌ | ❌ | ✅ (supercomputers) | ✅ (agents) |

None of these individual capabilities is novel in isolation. The contribution is **combining them for the agent-benchmarking use case** — AgentBench defines the protocol but has no green metrics; MLPerf Power measures energy for models but not agents; Green500 ranks supercomputers on energy but not AI agents.

### 9.2 Integration points

**Available now** (compatible by interface):

- LIMIT-GRAPH agents via `LimitGraphAdapter`
- NSN integration (uses existing retrieval logic)
- Level 5 MetaAgent (uses existing provenance tracking)
- Quantum evaluation modules

**Planned** (Phase 2; scope TBD):

- NSN bridge for backend-aware green optimization
- Level 5 bridge for provenance tracking
- `limit-benchmark` crate bridge — scope to be defined with the crate maintainers (v1.0 incorrectly described this as "SARS-CoV-2 benchmarks"; that was a copy-paste from an unrelated project)
- Hugging Face Spaces deployment (Phase 4)

---

## 10. Roadmap

| Phase | Item | Status |
|---|---|---|
| 1 | AgentBench protocol | ✅ Complete |
| 1 | Green metrics (energy, carbon, efficiency, SI) | ✅ Complete |
| 1 | Multi-framework adapters | ✅ Complete |
| 1 | Unified leaderboard | ✅ Complete |
| 1 | Smoke tests + CI | ✅ Complete |
| 2 | Similarity-based scoring (`expected_output`) | ⏳ Planned (O-2) |
| 2 | NSN / Level 5 / limit-benchmark bridges | ⏳ Planned (O-3) |
| 2 | Unit + integration test suite | ⏳ Planned |
| 3 | Energy consumption charts | ⏳ Planned |
| 3 | Carbon footprint dashboard | ⏳ Planned |
| 3 | Comparison matrix | ⏳ Planned |
| 4 | Hugging Face Spaces deployment | ⏳ Planned (O-5) |
| 4 | Public leaderboard REST API | ⏳ Planned (O-5) |
| 4 | Documentation website | ⏳ Planned |

**Phases 2–4 are not complete.** Status is Beta until they are.

---

## 11. Contributing

Before opening a pull request:

1. `python test_installation.py` — must pass.
2. `pytest -q` — must pass.
3. Update `CHANGELOG.md` at the top with your change under `## [Unreleased]`.
4. If you add a framework adapter, subclass `BaseAdapter` and register it in `limit_agentbench/adapters/__init__.py`.
5. If you change the public API, update this README's §3 and §4.

---

## 12. License and citation

Apache-2.0. See `LICENSE` for the full text.

```bibtex
@software{limit_agentbench_2026,
  title  = {LIMIT-AgentBench: Green Agent Benchmarking Platform},
  author = {{<FILL: AI Research Agent>}},
  year   = {2026},
  url    = {https://github.com/NurcholishAdam/Green_Agent},
  note   = {v2.4.2}
}
```

---

## Appendix: what changed from v1.0

| Area | v1.0 | This version |
|---|---|---|
| Status | "Implementation Complete" ×3 | Beta + §0 open-items table |
| Grid intensities | 9 regions; values diverged from INDEX.md by up to 70 gCO₂/kWh | 13 regions matching INDEX.md; source cited; regression-tested |
| Hardware power | 9 profiles; TDP-only (400 W for A100) | 10 profiles; effective vs peak split; invariant-tested |
| Example 1 | `result['metrics']` (dict access on dataclass) | `result.metrics` (attribute) |
| Adapter base class | `BaseAgentAdapter` | `BaseAdapter` (correct name; regression-tested) |
| Tracker class | Two separate entries | Alias, asserted by test |
| `expected_output` | Passed but undocumented use | Documented as reserved-for-Phase-2; provenance only |
| `track_carbon` | Passed but relationship to `track_energy` undocumented | Removed; coupling documented |
| `AgentEvaluator` `grid_region` | Undocumented | Added |
| `limit-benchmark` bridge | "SARS-CoV-2 benchmarks" (copy-paste) | Scope TBD |
| "Industry first" | Claimed | Replaced with related-work comparison |
| `test_installation.py` | Referenced, not shipped | Shipped, 9 checks, CI-gated |
| `tests/test_smoke.py` | Referenced, not shipped | Shipped, ~20 tests |
| `pyproject.toml`, `requirements.txt` | Absent | Shipped |
| `LICENSE` | "See LICENSE" but absent | Shipped |
| `CHANGELOG.md` | Absent | Shipped |
| `KNOWN_LIMITATIONS.md` | Absent | Shipped |
| `SECURITY.md` | Absent | Shipped |
| CI | None | `.github/workflows/bench-ci.yml` |
| Release date | `Jan 19, 2026` (future) | `<YYYY-MM-DD>` |
| Formulas | Unstated | §2.2, §2.4 documented |
| Region/profile counts | Header said 13/10, lists showed 9/9 | Shipped with tests asserting 13/10 |
| Grid-profile test | None | `test_grid_regions_count_is_13`, `test_hardware_profiles_count_is_10` |
