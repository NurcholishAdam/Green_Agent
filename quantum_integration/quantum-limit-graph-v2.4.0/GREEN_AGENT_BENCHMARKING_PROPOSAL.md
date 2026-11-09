
# Green Agent Benchmarking Platform — Architecture Proposal

**Package:** `limit-agentbench` · **Version:** 2.4.2 · **Status:** Proposal (awaiting approval) · **Date:** <2026-11-9>
**Python:** ≥3.10 · **License:** Apache-2.0
**Companion document:** `README.md` (current v2.4.2 implementation — Beta)
**Reviewer:** This document reconciles with the current implementation; see §1 for the explicit delta.

---

## Table of contents

- [0. Status](#0-status)
- [1. Current vs Proposed — explicit delta](#1-current-vs-proposed--explicit-delta)
- [2. Canonical contracts](#2-canonical-contracts)
- [3. Corrected examples](#3-corrected-examples)
- [4. Proposed architecture](#4-proposed-architecture)
- [5. Integration bridges](#5-integration-bridges)
- [6. Implementation roadmap](#6-implementation-roadmap)
- [7. Success metrics](#7-success-metrics)
- [8. Security](#8-security)
- [9. Related work and positioning](#9-related-work-and-positioning)
- [10. Known limitations](#10-known-limitations)
- [11. Next steps](#11-next-steps)
- [12. Contributing](#12-contributing)
- [13. License and citation](#13-license-and-citation)
- [Appendix A — Prior-draft corrections](#appendix-a--prior-draft-corrections)
- [Appendix B — Infrastructure files](#appendix-b--infrastructure-files)

---

## 0. Status

**Proposal — awaiting approval.** This document proposes changes to the
`limit-agentbench` module documented in `README.md`. It is **not** an
implementation report.

### 0.1 What this document is

- A design proposal for extending the `limit_agentbench` package.
- A canonicalization of the contracts (`Task`, `EvalResult`, public API
  surface).
- A 6-phase roadmap with **testable** success metrics.
- Infrastructure files (`pyproject.toml`, `LICENSE`, `KNOWN_LIMITATIONS.md`,
  `SECURITY.md`, `CHANGELOG.md`, `integration/__init__.py`,
  `test_installation.py`) that the proposal assumes exist — see Appendix B.

### 0.2 What this document is **not**

- Not an "Implementation Complete" report — see `README.md` for what ships
  today.
- Not "Production Ready" — see `README.md §7` for the Beta status.
- Not "First in industry" — see §9 for related-work positioning.

### 0.3 Decisions required before Phase 1 can start

| ID | Decision | Owner |
|---|---|---|
| D-1 | Approve `EvalResult.framework` field addition (backward-compatible: default `"unknown"`) | `@platform` |
| D-2 | Approve split of `provenance_hash` into flat (kept) + nested `provenance` object (new, optional) | `@platform` |
| D-3 | Confirm `efficiency_score = accuracy / energy_kwh` (unbounded) — matches `README.md §2.4` | `@bench` |
| D-4 | Confirm `f1_score` is computed by the harness in Phase 2, not by the agent | `@bench` |
| D-5 | Approve retroactive documentation of `grid_region` on `AgentEvaluator` (already shipped, was undocumented) | `@platform` |

Until D-1..D-5 are decided, Phase 1 cannot land cleanly.

---

## 1. Current vs Proposed — explicit delta

The single most important table in this document. It reconciles this
proposal with `README.md` (the current v2.4.2 implementation) so a
reviewer can see exactly what changes.

| Contract | Current (`README.md` v2.4.2) | Proposed (this doc) | Breaking? |
|---|---|---|---|
| Task fields | `task_id, suite, task_type, input_data, constraints, expected_output, protocol_version` | Same | ❌ No |
| Task creation | `create_task(..., constraints=None, expected_output=None)` | Same | ❌ No |
| `EvalResult` top-level fields | `task_id, agent_name, accuracy, energy_kwh, carbon_co2e_kg, latency_ms, metrics, output, provenance_hash` | Same **+ `framework: str = "unknown"`** | ✅ Additive (safe) |
| `EvalResult.metrics` contents | `sustainability_index` only | `sustainability_index, efficiency_score` (Phase 1), `f1_score` (Phase 2) | ✅ Additive |
| `provenance` | Flat `provenance_hash: str` | Flat `provenance_hash` **kept**; **+ optional `provenance: {backend, grid_region, timestamp}`** | ✅ Additive |
| Tracker context | `with tracker:` | Same | ❌ No |
| `tracker.get_metrics()` return | `Metrics` dataclass | Same | ❌ No |
| `compare_agents` | `AgentEvaluator.compare_agents(agents, tasks, sort_by)` (method) | Same | ❌ No |
| `adapter.evaluate_agent` | `(agent, task, track_energy=True)` | Same | ❌ No |
| Tracker naming | `GreenMetricsTracker is EnergyTracker` | Same | ❌ No |
| Framework label | `"langchain" \| "autogen" \| "crewai" \| "limit-graph"` | Same (hyphen, not underscore) | ❌ No |
| Grid region on `AgentEvaluator` | Shipped, undocumented | Documented in §2.5 | ❌ No (doc-only) |

**Summary:** 0 breaking changes, 2 additive (`framework` field,
`provenance` object), 3 doc-only additions.

**Why this table matters:** prior drafts of this proposal introduced
divergent contracts (three task schemas, `result['metrics']` dict access,
`BaseAgentAdapter` naming, `"limit_graph"` underscore, `ibm_washington`
as a grid region) without acknowledging the delta from `README.md`. This
table makes every change reviewable.

---

## 2. Canonical contracts

### 2.1 Units and ranges

| Field | Units | Range |
|---|---|---|
| `accuracy` | ratio | `[0, 1]` |
| `energy_kwh` | kWh | `≥ 0` |
| `carbon_co2e_kg` | kg CO₂e | `≥ 0` |
| `latency_ms` | ms | `≥ 0` |
| `sustainability_index` | ratio | `[0, 1]` |
| `efficiency_score` | accuracy/kWh | `≥ 0` (unbounded) |
| `f1_score` | ratio | `[0, 1]` (Phase 2) |
| `grid_intensity` | gCO₂e/kWh | `≥ 0` |

### 2.2 `SustainabilityIndex.calculate()` — canonical formula

```
a = accuracy / accuracy_ceiling
e = max(0, 1 − energy_kwh / energy_ceiling_kwh)
c = max(0, 1 − carbon_co2e_kg / carbon_ceiling_kg)

si = w_acc·a + w_energy·e + w_carbon·c        (clamped to [0, 1])
```

Defaults: ceilings `(1.0, 1.0, 1.0)`, weights `(0.5, 0.25, 0.25)`.

**This is identical to `README.md §2.2`.** Prior drafts of this proposal
showed `sustainability_index: 226.19` and `252.63` — those were
`accuracy / energy_kwh` (the `efficiency_score`) mislabeled. Corrected
values are in §3.

**Do not use** the "composite score (accuracy × efficiency / carbon)"
formula from earlier drafts — it produced values ~1000× larger and was
never reconciled with the `[0, 1]` contract.

### 2.3 Canonical `Task` schema

**One schema. JSON and Python align.**

```json
{
  "task_id": "qa_001",
  "suite": "question_answering",
  "task_type": "qa",
  "input_data": {"question": "What is the capital of France?"},
  "constraints": {"max_tokens": 500, "timeout_seconds": 30},
  "expected_output": {"answer": "Paris"},
  "protocol_version": "1.1"
}
```

```python
task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is the capital of France?"},
    constraints={"max_tokens": 500, "timeout_seconds": 30},
    expected_output={"answer": "Paris"},
)
```

**Prior-draft issue:** earlier drafts showed three incompatible shapes —
`{task_id, type, input, expected_output, evaluation_metrics}` (one
section), `{task_id, suite, difficulty, input, expected_output,
evaluation}` (another section), and `create_task(task_id, suite,
task_type, input_data, ...)` (README). All reconciled to the shape above.

### 2.4 Canonical `EvalResult` schema

```json
{
  "task_id": "qa_001",
  "agent_name": "MyAgent-v1",
  "framework": "limit-graph",
  "accuracy": 0.96,
  "energy_kwh": 0.0038,
  "carbon_co2e_kg": 0.000836,
  "latency_ms": 145,
  "metrics": {
    "sustainability_index": 0.9788,
    "efficiency_score": 252.63,
    "f1_score": 0.94
  },
  "output": {"answer": "Paris", "confidence": 0.96},
  "provenance_hash": "a3f9c8e1b2d4f567",
  "provenance": {
    "backend": "ibm_washington",
    "grid_region": "US-CA",
    "timestamp": "<YYYY-MM-DDTHH:MM:SSZ>"
  }
}
```

**Field-by-field reconciliation vs prior drafts:**

| Field | Prior draft | This doc |
|---|---|---|
| `framework` | `"limit_graph"` (underscore) | `"limit-graph"` (hyphen, matches §2.1) |
| `accuracy` | inside `metrics` | **top-level** (matches `README.md`) |
| `energy_kwh` | inside `metrics` | **top-level** |
| `carbon_co2e_kg` | inside `metrics` | **top-level** |
| `latency_ms` | inside `metrics` | **top-level** |
| `sustainability_index` | `252.63` (wrong) | `0.9788` (correct, §3) |
| `efficiency_score` | `0.97` (wrong) | `252.63` (= `accuracy / energy_kwh`) |
| `f1_score` | inline, undefined | Documented, computed by harness (Phase 2) |
| `provenance` | nested object only | **flat `provenance_hash` kept**; nested `provenance` optional and split `backend` (quantum) from `grid_region` (carbon math) |
| `ibm_washington` | used as grid region | correctly identified as quantum backend; separate `grid_region: "US-CA"` added |

### 2.5 Public API surface — one choice per symbol

| Symbol | Signature (canonical) |
|---|---|
| `AgentBenchAdapter.create_task` | `(task_id=..., suite=..., task_type=..., input_data=..., constraints=None, expected_output=None) -> Task` |
| `AgentBenchAdapter.evaluate_agent` | `(agent, task, *, track_energy=True) -> EvalResult` |
| `AgentEvaluator.__init__` | `(*, track_green_metrics=True, grid_region="GLOBAL", hardware_profile="default")` |
| `AgentEvaluator.compare_agents` | `(*, agents, tasks, sort_by="sustainability_index") -> list[dict]` |
| `EnergyTracker.__init__` | `(*, grid_region="GLOBAL", hardware_profile="default")` |
| Tracker context | `with tracker:` (not `with tracker.track():`) |
| `EnergyTracker.get_metrics` | `() -> Metrics` (dataclass, not dict) |
| `SustainabilityIndex.calculate` | `(accuracy, energy_kwh, carbon_co2e_kg) -> float` |
| `EfficiencyScorer.score` | `(*, accuracy, energy_kwh, carbon_co2e_kg, latency_ms=0.0) -> float` |
| `GreenLeaderboard.submit` | `(*, agent_name, framework, task_suite, accuracy, energy_kwh, carbon_co2e_kg, latency_ms) -> LeaderboardEntry` |
| `GreenLeaderboard.get_rankings` | `(*, sort_by="sustainability_index", top_k=None) -> list[LeaderboardEntry]` |
| `GreenLeaderboard.get_top_agents` | `(n=10) -> list[LeaderboardEntry]` |
| `BaseAdapter.run` | `(task_input, *, track_green_metrics=True) -> EvalResult` |
| `BaseAdapter.evaluate_agent` | `(agent, task, *, track_energy=True) -> EvalResult` |

**Prior-draft issues fixed:**

| # | Prior draft | This doc |
|---|---|---|
| 1 | `with tracker.track():` | `with tracker:` |
| 2 | `tracker.get_metrics()` returns dict | Returns `Metrics` dataclass |
| 3 | `compare_agents([r1, r2])` free function | `AgentEvaluator().compare_agents(agents=..., tasks=..., sort_by=...)` |
| 4 | `adapter.evaluate_agent(task={...})` (dict) | `adapter.evaluate_agent(agent, Task(...))` |
| 5 | `track_green_metrics` and `track_energy` mixed | `evaluate_agent(track_energy=...)`, `BaseAdapter.run(track_green_metrics=...)` — documented per symbol |

---

## 3. Corrected examples

### 3.1 Green metrics example (arithmetic reconciled)

```python
from limit_agentbench.metrics import (
    CarbonCalculator, EfficiencyScorer, EnergyTracker, SustainabilityIndex,
)

tracker = EnergyTracker(grid_region="US-CA", hardware_profile="nvidia_a100")
with tracker:
    result = agent.run(task)

m = tracker.get_metrics()
# m.energy_kwh      = 0.0042
# m.carbon_co2e_kg  = 0.0042 × 220 / 1000 = 0.000924  (US-CA = 220 gCO2/kWh)
# m.effective_watts = 250
# m.power_watts     = 400 (peak TDP — reported, not used for energy totals)

si = SustainabilityIndex().calculate(
    accuracy=0.92,
    energy_kwh=m.energy_kwh,
    carbon_co2e_kg=m.carbon_co2e_kg,
)
# si = 0.5·0.92 + 0.25·(1−0.0042) + 0.25·(1−0.000924)
#    = 0.46 + 0.24895 + 0.249769
#    = 0.9587

eff = EfficiencyScorer(metric="accuracy_per_kwh").score(
    accuracy=0.92, energy_kwh=m.energy_kwh, carbon_co2e_kg=m.carbon_co2e_kg,
)
# eff = 0.92 / 0.0042 = 219.05
```

**Prior-draft issues:**

| Metric | Prior draft | Corrected | Why |
|---|---|---|---|
| `sustainability_index` | `226.19` | `0.9587` | Prior value was `accuracy/energy`, not SI |
| `carbon_co2e_kg` | `0.00084` | `0.000924` | Prior used 200 g/kWh; US-CA is 220 |
| `efficiency_score` | `0.95` | `219.05` | Prior value was a ratio, not the actual metric |

### 3.2 Full result example (all fields reconciling)

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter(grid_region="US-CA", hardware_profile="nvidia_a100")

task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is the capital of France?"},
    expected_output={"answer": "Paris"},
)

result = adapter.evaluate_agent(my_agent, task, track_energy=True)

# result is an EvalResult dataclass. Attribute access:
result.task_id            # "qa_001"
result.agent_name         # "MyAgent-v1"
result.accuracy           # 0.96
result.energy_kwh         # 0.0038
result.carbon_co2e_kg     # 0.0038 × 220 / 1000 = 0.000836
result.latency_ms         # 145
result.metrics["sustainability_index"]  # 0.9788
result.metrics["efficiency_score"]      # 0.96 / 0.0038 = 252.63
result.provenance_hash    # "a3f9c8e1b2d4f567"
```

**Prior-draft issues fixed:**

- `result['metrics']` (dict access) → `result.metrics` (attribute access on
  the `EvalResult` dataclass)
- `framework: "limit_graph"` → `framework: "limit-graph"` (matches
  `README.md §3.5`)
- `ibm_washington` as grid region → correctly modeled as
  `provenance.backend` (quantum) with separate `provenance.grid_region`
- `sustainability_index: 252.63` → correctly `0.9788`; `252.63` is now
  labeled `efficiency_score` (which is what it actually was)
- `carbon = 0.00076` → `0.000836` (correct US-CA math)

---

## 4. Proposed architecture

Files marked with a phase. **Phase 1 files are already shipped** in
`README.md`. Phases 2–6 are the proposal's scope.

```
quantum-limit-graph-v2.4.0/
├── limit-agentbench/
│   ├── __init__.py                                    # Phase 1 ✅
│   ├── types.py                                       # Phase 1 ✅
│   ├── core/
│   │   ├── __init__.py                                # Phase 1 ✅
│   │   ├── agentbench_adapter.py                      # Phase 1 ✅
│   │   ├── green_metrics.py                           # Phase 1 ✅
│   │   ├── agent_evaluator.py                         # Phase 1 ✅
│   │   └── benchmark_harness.py                       # Phase 1 ✅
│   ├── adapters/
│   │   ├── __init__.py                                # Phase 1 ✅
│   │   ├── base_adapter.py                            # Phase 1 ✅
│   │   ├── langchain_adapter.py                       # Phase 1 ✅
│   │   ├── autogen_adapter.py                         # Phase 1 ✅
│   │   ├── crewai_adapter.py                          # Phase 1 ✅
│   │   └── limit_graph_adapter.py                     # Phase 1 ✅
│   ├── metrics/
│   │   ├── __init__.py                                # Phase 1 ✅
│   │   ├── energy_tracker.py                          # Phase 1 ✅
│   │   ├── carbon_calculator.py                       # Phase 1 ✅
│   │   ├── efficiency_scorer.py                       # Phase 1 ✅
│   │   └── sustainability_index.py                    # Phase 1 ✅
│   ├── dashboard/
│   │   ├── __init__.py                                # Phase 1 ✅
│   │   └── green_leaderboard.py                       # Phase 1 ✅
│   ├── integration/                                   # Phase 2 ⏳
│   │   ├── __init__.py                                # Phase 1 ✅ (stubs)
│   │   ├── nsn_bridge.py                              # Phase 2 ⏳
│   │   ├── level5_bridge.py                           # Phase 2 ⏳
│   │   ├── limit_benchmark_bridge.py                  # Phase 2 ⏳
│   │   └── huggingface_exporter.py                    # Phase 6 ⏳
│   └── dashboard/                                     # Phase 3 ⏳
│       ├── energy_visualizer.py                       # Phase 3 ⏳
│       ├── carbon_dashboard.py                        # Phase 3 ⏳
│       └── comparison_matrix.py                       # Phase 3 ⏳
├── pyproject.toml                                     # Phase 1 ✅
├── requirements.txt                                   # Phase 1 ✅
├── LICENSE                                            # Phase 1 ✅
├── KNOWN_LIMITATIONS.md                               # Phase 1 ✅
├── SECURITY.md                                        # Phase 1 ✅
├── CHANGELOG.md                                       # Phase 1 ✅
├── test_installation.py                               # Phase 1 ✅
├── demo_green_benchmark.py                            # Phase 1 ✅
└── tests/
    ├── __init__.py                                    # Phase 1 ✅
    ├── test_smoke.py                                  # Phase 1 ✅
    ├── test_agentbench_protocol.py                    # Phase 2 ⏳
    ├── test_green_metrics.py                          # Phase 2 ⏳
    ├── test_adapters.py                               # Phase 2 ⏳
    └── test_integration.py                            # Phase 5 ⏳

# Unchanged (do not modify):
├── rust/egg/crates/limit-benchmark/                   # existing
├── nsn_integration/                                   # existing
└── level_5_agent.py                                   # existing
```

**Prior-draft issue:** earlier drafts presented the full tree as if all
files existed. This table labels every file with its phase.

---

## 5. Integration bridges

### 5.1 NSN bridge (Phase 2)

```python
from limit_agentbench.integration import NSNBridge  # Phase 2

bridge = NSNBridge()

# Select a backend under green constraints, then evaluate.
selection = bridge.select_backend_with_green_metrics(
    target_accuracy=0.90,
    max_energy_kwh=0.01,
    max_carbon_kg=0.002,
)
result = bridge.evaluate_with_nsn(
    agent=my_agent,
    task=task,
    backend=selection["backend"],
    rank=selection["rank"],
)
```

### 5.2 Level 5 bridge (Phase 2)

```python
from limit_agentbench.integration import Level5Bridge  # Phase 2

bridge = Level5Bridge()
provenance = bridge.log_to_level5(
    agent_result=result,
    green_metrics=result.metrics,
    contributor_id="contributor_001",
)
bridge.add_to_level5_leaderboard(provenance)
```

### 5.3 `limit-benchmark` crate bridge (Phase 2)

```python
from limit_agentbench.integration import LimitBenchmarkBridge  # Phase 2

bridge = LimitBenchmarkBridge()

# Runs the existing multi-intent query benchmarks from the limit-benchmark
# crate, with green-metrics tracking layered on.
result = bridge.run_limit_benchmark(
    agent=my_agent,
    track_green_metrics=True,
)
```

**Prior-draft issue:** earlier drafts called this `run_sarscov2_benchmark`
— a copy-paste from an unrelated project. The `limit-benchmark` crate is
described in §"Current State" as "multi-intent query benchmarking"; the
method name is corrected to `run_limit_benchmark`, and the scope is
"multi-intent query benchmarks".

### 5.4 Hugging Face exporter (Phase 6)

```python
import os
from pathlib import Path

from limit_agentbench.integration import HuggingFaceExporter  # Phase 6

secrets_dir = Path(os.environ["GREEN_AGENT_SECRETS_DIR"])
hf_token = (secrets_dir / "hf_token").read_text().strip()

exporter = HuggingFaceExporter(token=hf_token)
exporter.deploy_dashboard(
    space_name="quantum-limit-graph-green-benchmark",
    include_leaderboard=True,
    include_visualizations=True,
)
```

**Security note:** `HF_TOKEN` must be loaded from
`$GREEN_AGENT_SECRETS_DIR`, not the environment (§8). `auto_update=True`
from earlier drafts is removed — an auto-pulling service would need
authentication and rate-limit design, which are Phase 6 decisions.

---

## 6. Implementation roadmap (honest)

| Phase | Weeks | Scope | Status |
|---|---|---|---|
| **1** | Week 1–2 | Core infrastructure, metrics, adapters, leaderboard, smoke tests | ✅ **Shipped** (see `README.md`) |
| **2** | Week 3–4 | Integration bridges (NSN, Level 5, limit-benchmark), similarity scoring (`expected_output` → `f1_score`), unit + integration tests | ⏳ Planned |
| **3** | Week 5 | Visualization (energy charts, carbon dashboard, comparison matrix) | ⏳ Planned |
| **4** | Week 6 | Dashboard REST API | ⏳ Planned |
| **5** | Week 7 | Full test suite + docs | ⏳ Planned |
| **6** | Week 8 | Hugging Face Spaces deployment, public leaderboard | ⏳ Planned |

**Prior-draft issue:** earlier drafts showed Week 1–2 as "Create
`limit-agentbench` module structure" — but Phase 1 was already shipped per
`README.md`. This table's Phase 1 now says "Shipped" to match reality.

---

## 7. Success metrics (testable)

Prior drafts' success metrics were unverifiable. Replacements:

| Metric | Prior draft | This doc |
|---|---|---|
| AgentBench compliance | "100%" | **CI test**: `tests/test_agentbench_protocol.py` covers every status in the failure taxonomy (success, partial, timeout, invalid_output, oom, crash, failure). 100% pass = compliant. |
| Framework support | "4+" | **CI test**: `pytest -q tests/test_adapters.py` parametrized over {LangChain, AutoGen, CrewAI, LimitGraph}. Pass count ≥ 4. |
| Green metrics accuracy | "±5%" | **Benchmark**: `scripts/bench_green_accuracy.py` compares `EnergyTracker` estimates against `pyRAPL` measurements on `nvidia_a100`. Target: ±20% (realistic for TDP-based estimation). Measurement script ships with Phase 2. |
| Dashboard uptime | "99.9%" | **SLO definition**: read-path `p99 < 1s`, error rate `< 1%` over a rolling 7-day window. Requires deployment + monitoring — Phase 4. |
| Community adoption | "100+ contributors in 6 months" | **Removed** — not a technical metric. |

---

## 8. Security

This proposal introduces network-facing components and external
credentials. Minimum posture:

### 8.1 Secrets

- **Grid-API keys** (`WATTTIME_API_KEY`, `ELECTRICITYMAP_API_KEY`) must be
  loaded from `$GREEN_AGENT_SECRETS_DIR`, never environment variables.
- **Hugging Face token** (`HF_TOKEN`) likewise — required for Phase 6.
- **PostgreSQL / Redis** credentials from files, not env.
- Never echo secret values in logs.

### 8.2 Dashboard (Phase 3–4)

- The dashboard read-path is public. The write-path (leaderboard
  submission) requires authentication. Proposed scheme:
  bearer-token-per-contributor, managed in the same secrets dir.
- **CORS** must be an explicit allowlist, not `*`.
- **Rate limit** public endpoints at 60 req/min per IP.
- **No PII** in leaderboard entries — only `agent_name`, `framework`,
  metrics.

### 8.3 Hugging Face exporter (Phase 6)

- `HF_TOKEN` from secrets dir, not env.
- `auto_update` is **not implemented**. Any auto-pull service would need
  auth and rate-limit design; deferred to Phase 6 review.
- The exporter is **write-only** — it pushes data to HF, it does not
  receive callbacks.

### 8.4 Ray / distributed

Not in scope for this proposal. See the separate `MANIFEST.md`
(`Green Agent Advanced Modules`).

### 8.5 Input validation

- `task_id` and `agent_name` restricted to `[A-Za-z0-9._-]{1,64}` —
  prevents log injection and CSV formula injection.
- `GreenLeaderboard.submit` validates every numeric field at construction.
- `AgentBenchAdapter.create_task` validates the task schema before
  invoking the agent — no unvalidated dicts reach agent code.

---

## 9. Related work and positioning

Earlier drafts claimed "First in industry" and "First green agent
benchmarking platform". Both are unfalsifiable. Related-work comparison:

| Capability | AgentBench | MLPerf Power | Green500 | LIMIT-AgentBench |
|---|---|---|---|---|
| Protocol-standardized task format | ✅ | ❌ | ❌ | ✅ |
| Green metrics (energy, carbon) | ❌ | ✅ | ✅ (energy only) | ✅ |
| Cross-framework agent adapters | ❌ | ❌ | ❌ | ✅ (4 frameworks) |
| Sustainability Index (single `[0,1]` composite) | ❌ | ❌ | ❌ | ✅ |
| Cross-framework leaderboard | ❌ | ❌ | ✅ (supercomputers) | ✅ (agents) |
| Quantum evaluation | ❌ | ❌ | ❌ | ✅ (existing LIMIT-GRAPH) |

None of these individual capabilities is novel in isolation. The
contribution is **combining them for the agent-benchmarking use case** —
AgentBench defines the protocol but has no green metrics; MLPerf Power
measures energy for models but not agents; Green500 ranks supercomputers
on energy but not AI agents.

### 9.1 Comparison with prior delivery state

Prior drafts' comparison table claimed `LIMIT-GRAPH (Current):
Multi-framework ❌`. This **contradicts `README.md`**, which shipped four
framework adapters. Corrected:

| Feature | AgentBench | LIMIT-GRAPH (Current — `README.md`) | LIMIT-GRAPH (Proposed) |
|---|---|---|---|
| Multi-framework | ✅ | ✅ (4 adapters) | ✅ |
| Standardized protocol | ✅ | ✅ | ✅ |
| Energy metrics | ❌ | ✅ | ✅ |
| Carbon tracking | ❌ | ✅ | ✅ |
| Quantum evaluation | ❌ | ✅ (existing) | ✅ |
| Multilingual | Partial | ✅ (existing) | ✅ |
| Provenance tracking | ❌ | ✅ | ✅ |
| Public leaderboard API | ✅ | ❌ (Phase 4) | ⏳ Phase 4 |
| Similarity-based scoring | Partial | ❌ (Phase 2) | ⏳ Phase 2 |
| Visualization dashboard | Partial | ❌ (Phase 3) | ⏳ Phase 3 |

---

## 10. Known limitations

Full list in `KNOWN_LIMITATIONS.md`. Highlights relevant to this
proposal:

1. **Grid intensities are static reference values** (`README.md §5.1`).
   Live provider (WattTime, ElectricityMap) is Phase 2 (O-1).
2. **`expected_output` is not used for scoring today.** Phase 2 (O-2)
   adds similarity-based scoring; the `f1_score` field is documented but
   not populated until then.
3. **`efficiency_score` is `accuracy / energy_kwh`** — unbounded. Prior
   drafts of this proposal confused this with SI.
4. **`provenance.backend` is a quantum backend name**, not a grid region.
   `provenance.grid_region` is separate.
5. **`framework` naming is hyphenated** (`"limit-graph"`) — not
   underscore. Prior drafts used underscore in one place.
6. **No visualization dashboard yet** — Phase 3.
7. **No public leaderboard API yet** — Phase 4.
8. **Package status is Beta**, not "Production Ready". See `README.md §0`.
9. **`--enhanced` mode** (from prior docs) is not in scope for this
   proposal — that is `Green Agent Advanced Modules` scope.

---

## 11. Next steps

### 11.1 Immediate (this week)

1. **Approve this proposal** (§0.3 decisions D-1..D-5).
2. **Confirm delta table** (§1) — reviewer signs off that no contract
   changes are breaking.
3. **Confirm `EvalResult.framework`** additive change is acceptable.

### 11.2 Phase 2 kickoff

1. Implement `integration/` bridges (§5.1–§5.3).
2. Add `f1_score` computation to `AgentEvaluator` (O-2).
3. Add `scripts/bench_green_accuracy.py` against `pyRAPL`.

### 11.3 Phase 3–6

Per roadmap (§6).

---

## 12. Contributing

Before opening a PR against this proposal:

1. Read §1 (delta table) and confirm your change does not break an
   existing contract.
2. Add a test to `tests/` that exercises your change.
3. Update `CHANGELOG.md` at the top.
4. If your change touches §2 (contracts), update the corresponding
   section and the delta table.

---

## 13. License and citation

Apache-2.0. See `LICENSE` for the full text.

```bibtex
@software{limit_agentbench_proposal_2026,
  title  = {Green Agent Benchmarking Platform: Architecture Proposal},
  author = {{AI Research Agent}},
  year   = {2026},
  url    = {https://github.com/NurcholishAdam/Green_Agent},
  note   = {v2.4.2 proposal}
}
```

---

## Appendix A — Prior-draft corrections

This appendix exists so reviewers who saw earlier drafts can quickly
confirm the specific issues that were fixed.

| Area | Prior draft | This version |
|---|---|---|
| SI formula | Never stated; examples showed `226.19`, `252.63` | Published in §2.2; both values correctly identified as `efficiency_score`; correct SI values `0.9587`, `0.9788` |
| Task schema | Three incompatible shapes | One canonical schema (§2.3); delta documented in §1 |
| Result schema | `accuracy`/`energy_kwh`/`carbon_co2e_kg` inside `metrics`; `ibm_washington` as grid | Top-level per `README.md`; `provenance.backend` (quantum) split from `provenance.grid_region` |
| Framework label | `"limit_graph"` (underscore) | `"limit-graph"` (hyphen, matches `README.md §3.5`) |
| SARS-CoV-2 | `run_sarscov2_benchmark` | Removed → `run_limit_benchmark`; task example is generic QA |
| `tracker.track()` vs `with tracker:` | Mixed | `with tracker:` (matches `README.md`) |
| `get_metrics()` return | Dict in example | `Metrics` dataclass |
| `compare_agents` | Free function | `AgentEvaluator().compare_agents(...)` |
| `evaluate_agent(task=dict)` | Dict | `Task` dataclass |
| Energy/carbon math | Off by 10% (used 200 g/kWh for US-CA) | Reconciled at 220 g/kWh (§3) |
| Comparison table | `LIMIT-GRAPH (Current): Multi-framework ❌` | Corrected: `✅ (4 adapters)` |
| Roadmap | Phase 1 = "Week 1–2" | Phase 1 = "✅ Shipped" |
| Success metrics | "100%", "±5%", "99.9% uptime", "100+ contributors" | Testable metrics (§7) |
| "First in industry" | Claimed | Replaced with related-work comparison (§9) |
| Planned files in tree | All presented as deliverables | Phase-labeled (§4) |
| Status | "Awaiting approval" + "Production Ready" + "First in industry" | "Proposal awaiting approval"; other claims removed |
| Date | `Jan 19, 2026` (×2) | `<YYYY-MM-DD>` |
| Security section | Absent | §8 |
| Decisions required | Not listed | §0.3 (D-1..D-5) |

---

## Appendix B — Infrastructure files

This proposal assumes the following files exist. They are **not** part of
this document but are required for the proposal's examples to run.

| File | Purpose | Delivered by |
|---|---|---|
| `pyproject.toml` | Build config; `pip install -e .` | `README.md` (v2.4.2) |
| `requirements.txt` | Pinned deps; `pip install -r` | `README.md` (v2.4.2) |
| `LICENSE` | Apache-2.0 text | `README.md` (v2.4.2) |
| `KNOWN_LIMITATIONS.md` | Full limitations list | `README.md` (v2.4.2) |
| `SECURITY.md` | Security policy | `README.md` (v2.4.2) |
| `CHANGELOG.md` | Version history | `README.md` (v2.4.2) |
| `test_installation.py` | Runnable verification | `README.md` (v2.4.2) |
| `limit_agentbench/integration/__init__.py` | Stub classes raising `NotImplementedError` | this proposal (Phase 1) |

**Why `integration/__init__.py` is part of this proposal:** the §5
examples import `NSNBridge`, `Level5Bridge`, `LimitBenchmarkBridge`, and
`HuggingFaceExporter` from `limit_agentbench.integration`. Without a stub
module, those imports fail. The stub class raises `NotImplementedError`
in `__init__` so that:
- Imports succeed today (no `ModuleNotFoundError`).
- Instantiation fails loudly with a message pointing at the phase that
  will implement it.

This lets proposal code and `README.md` code coexist without import
errors, while making the "not yet implemented" state explicit at runtime.
