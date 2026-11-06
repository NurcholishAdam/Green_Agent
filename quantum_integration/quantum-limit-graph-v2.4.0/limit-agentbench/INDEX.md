
# LIMIT-AgentBench

**Version:** 2.4.2 · **Status:** Beta · **Python:** ≥3.10 · **License:** Apache-2.0

AgentBench-compatible green benchmarking for AI agents. Measures accuracy, energy consumption, carbon footprint, latency, and a unified Sustainability Index across LangChain, AutoGen, CrewAI, and LIMIT-GRAPH agents. Optional advanced modules add LIMIT Graph, MODP, RLHF, Multi-Teacher Distillation, MoE expert gating, evolutionary optimisation, and FlexGen integration.

> **Beta notice** — the four core pillars and all six enhancement modules are implemented. "Production Ready" awaits live grid-intensity wiring, CI coverage of enhancement modules, NSN integration, and the public leaderboard API. See [Known Limitations](#known-limitations) and [Roadmap](#roadmap).

---

## Table of contents

- [Install](#install)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [API reference](#api-reference)
- [Examples](#examples)
- [Enhancement modules](#enhancement-modules)
- [Testing](#testing)
- [Supported configurations](#supported-configurations)
- [Known limitations](#known-limitations)
- [Security](#security)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Install

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -e . --no-deps
```

`requirements.txt` is pinned for reproducible CI. The package installs as
`limit-agentbench` and exposes the top-level module `limit_agentbench`.

Verify the installation:

```bash
python test_installation.py
```

Expected output (any `[FAIL]` line exits non-zero; `[SKIP]` lines are fine
when the optional `src/enhancements/` folder is not present):

```
[PASS] core imports
[PASS] adapters imports
[PASS] metrics imports
[PASS] dashboard imports
[SKIP] enhancements imports (optional) — No module named 'src.enhancements'
[PASS] GreenMetricsTracker is EnergyTracker alias
[PASS] sustainability index smoke test
Installation verified successfully
```

---

## Quick start

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter(grid_region="EU-FR", hardware_profile="nvidia_a100")

task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is sustainable AI?"},
)

result = adapter.evaluate_agent(my_agent, task, track_energy=True)

print(f"SI: {result.metrics['sustainability_index']:.3f}")
print(f"Energy: {result.energy_kwh:.6f} kWh")
print(f"Carbon: {result.carbon_co2e_kg:.6f} kg CO2e")
```

See [`INDEX.md`](INDEX.md) for the full documentation index, and run
`python demo_green_benchmark.py` for a runnable end-to-end demo.

---

## Concepts

### Sustainability Index

A single score in `[0, 1]` combining accuracy, energy, and carbon. Higher is
better:

```
a = accuracy / accuracy_ceiling
e = max(0, 1 − energy_kwh / energy_ceiling_kwh)
c = max(0, 1 − carbon_co2e_kg / carbon_ceiling_kg)

si = w_acc·a + w_energy·e + w_carbon·c        (clamped to [0, 1])
```

Defaults: ceilings `(1.0, 1.0, 1.0)`, weights `(0.5, 0.25, 0.25)`. Override
by constructing `SustainabilityIndex(...)` with your own ceilings and
weights (weights must sum to 1.0, tolerance 1e-6).

### Async surface

Everything in the public API is **synchronous**. If a future method becomes a
coroutine, it must be documented here first. There is no `await` or
`asyncio.run` in any runnable example.

### Units and ranges

| Field | Units | Range |
|---|---|---|
| `accuracy` | ratio | `[0, 1]` |
| `energy_kwh` | kWh | `≥ 0` |
| `carbon_co2e_kg` | kg CO₂e | `≥ 0` |
| `latency_ms` | ms | `≥ 0` |
| `sustainability_index` | ratio | `[0, 1]` |
| `region_carbon_intensity` | gCO₂e/kWh | `≥ 0` |
| `energy_per_token` | kWh/token | `≥ 0` |
| `human_feedback_score` | ratio | `[0, 1]` |
| `graph_metrics.centrality` | ratio | `[0, 1]` |
| `graph_metrics.connectivity` | ratio | `[0, 1]` |
| `metadata.*_weight` | ratio | sum to 1.0 |

---

## API reference

### `AgentBenchAdapter` — AgentBench protocol adapter

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter(
    protocol_version="1.1",       # "1.0" | "1.1"
    grid_region="GLOBAL",         # see Supported configurations
    hardware_profile="default",   # see Supported configurations
)

task = adapter.create_task(
    task_id="qa_001",             # [A-Za-z0-9._-]{1,64}
    suite="question_answering",
    task_type="qa",
    input_data={"question": "..."},
    constraints=None,             # optional dict
)

result = adapter.evaluate_agent(agent, task, track_energy=True)
# agent is Callable[dict, dict] returning at minimum {"accuracy": float}
# result: EvalResult
```

### `SustainabilityIndex` — the SI metric

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

Invalid inputs (negative energy/carbon, out-of-range accuracy) raise
`ValueError` rather than silently producing a number.

### `AgentEvaluator` — multi-agent comparison

```python
from limit_agentbench import AgentEvaluator

evaluator = AgentEvaluator(track_green_metrics=True)

result = evaluator.evaluate(agent, task)                      # EvalResult
comparison = evaluator.compare_agents(                        # list[dict]
    agents=[agent_a, agent_b],
    tasks=task_suite,
    sort_by="sustainability_index",   # or "agent_name"
)
```

### `BenchmarkHarness` — file-backed benchmark runner

```python
from limit_agentbench import BenchmarkHarness

harness = BenchmarkHarness(output_dir="./results")
result = harness.run_benchmark(agent, task_suite, "my_benchmark")
# Writes ./results/my_benchmark.json and returns:
# {"path": "...", "summary": {"num_tasks": ..., "avg_accuracy": ..., "avg_sustainability_index": ...}}
```

### `GreenLeaderboard` — in-memory leaderboard

```python
from limit_agentbench import GreenLeaderboard

lb = GreenLeaderboard()
lb.submit(
    agent_name="MyAgent",
    framework="langchain",
    task_suite="benchmark",
    accuracy=0.95,
    energy_kwh=0.003,
    carbon_co2e_kg=0.0006,
    latency_ms=150,
)

rankings = lb.get_rankings(sort_by="sustainability_index", top_k=10)
top = lb.get_top_agents(n=10)   # == get_rankings(sort_by="sustainability_index", top_k=n)
```

`sort_by` accepts `"sustainability_index"`, `"accuracy"`, `"energy_kwh"`,
`"carbon_co2e_kg"`, `"latency_ms"`.

### Framework adapters

Every adapter exposes **both** `.run()` and `.evaluate_agent()`:

```python
from limit_agentbench.adapters import (
    LangChainAdapter, AutoGenAdapter, CrewAIAdapter, LimitGraphAdapter,
)

adapter = LangChainAdapter(my_agent)

# Framework-native input
result = adapter.run({"prompt": "hi"}, track_green_metrics=True)

# AgentBench-protocol input
result = adapter.evaluate_agent(my_agent, task, track_energy=True)
```

### Metrics

```python
from limit_agentbench.metrics import (
    EnergyTracker, CarbonCalculator, EfficiencyScorer, SustainabilityIndex,
)

# Context-manager energy tracking
tracker = EnergyTracker(grid_region="EU-FR", hardware_profile="nvidia_a100")
with tracker:
    run_workload()
metrics = tracker.get_metrics()     # Metrics dataclass

# Standalone carbon conversion
cc = CarbonCalculator()
kg = cc.calculate(energy_kwh=1.0, grid_region="EU-FR")   # == 0.07

# Efficiency metrics
scorer = EfficiencyScorer(metric="accuracy_per_kwh")
score = scorer.score(accuracy=0.9, energy_kwh=0.003, carbon_co2e_kg=0.0006)
```

**`GreenMetricsTracker` is an alias for `EnergyTracker`.** Both names work:

```python
from limit_agentbench import GreenMetricsTracker
from limit_agentbench.metrics import EnergyTracker
assert GreenMetricsTracker is EnergyTracker
```

### Dataclasses

```python
from limit_agentbench.types import Task, EvalResult, LeaderboardEntry
```

All three validate their fields in `__post_init__` and expose `.to_dict()`
for JSON serialization. `task_id` and `agent_name` are restricted to
`[A-Za-z0-9._-]{1,64}` to prevent log / CSV formula injection.

---

## Examples

### Basic evaluation

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter()
task = adapter.create_task(
    task_id="qa_001", suite="question_answering", task_type="qa",
    input_data={"question": "What is AI?"},
)
result = adapter.evaluate_agent(my_agent, task, track_energy=True)
print(f"SI: {result.metrics['sustainability_index']:.2f}")
```

### Framework comparison

```python
from limit_agentbench import AgentEvaluator
from limit_agentbench.adapters import LangChainAdapter, AutoGenAdapter

evaluator = AgentEvaluator(track_green_metrics=True)
comparison = evaluator.compare_agents(
    agents=[LangChainAdapter(agent1), AutoGenAdapter(agent2)],
    tasks=task_suite,
    sort_by="sustainability_index",
)
```

### Leaderboard

```python
from limit_agentbench import GreenLeaderboard

lb = GreenLeaderboard()
lb.submit(agent_name="MyAgent", framework="langchain", task_suite="benchmark",
          accuracy=0.95, energy_kwh=0.003, carbon_co2e_kg=0.0006,
          latency_ms=150)
top = lb.get_top_agents(n=10)
```

### Enhanced routing (`src/enhancements` required — sync, no `await`)

```python
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

node = NodeDescriptor(
    id="node1", type=NodeType.EDGE, region="us-east",
    region_carbon_intensity=350.0,        # gCO₂e / kWh
    energy_per_token=0.00004,             # kWh / token
    use_evolutionary=True,
    human_feedback_score=0.7,             # [0, 1]
    graph_metrics={"centrality": 0.8},    # [0, 1]
)
strategy = node.select_routing_strategy()   # SYNC
print(f"Selected strategy: {strategy}")
```

### Enhanced priority selection

```python
from src.enhancements.schemas.workload_descriptor import (
    WorkloadDescriptor, TaskType, Urgency,
)

wl = WorkloadDescriptor(
    task_id="task1", task_type=TaskType.INFERENCE, tokens=1000,
    latency_target=300.0, urgency=Urgency.MEDIUM,
    use_evolutionary=True, human_feedback_score=0.6,
    graph_metrics={"centrality": 0.7},
    metadata={
        # weights must sum to 1.0 — validated in constructor
        "latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2,
    },
)
priority = wl.select_priority()   # SYNC
print(f"Priority: {priority}")
```

### Feedback event

```python
from src.enhancements.schemas.feedback_event import FeedbackEvent

event = FeedbackEvent(
    source="my_agent", feedback_type="routing", task_id="t1",
    action={"selected_action": "execute"},
    performance={"quality_score": 0.9, "latency_ms": 100, "energy_kwh": 0.0003},
    adaptive_cost_value=0.85,       # [0, 1]
    graph_metrics={"centrality": 0.7},
    human_feedback_score=0.8,
    modp_score=0.75,
)
```

`feedback_type` must be one of `routing` | `priority` | `security` | `other`.

---

## Enhancement modules

Optional modules live in `src/enhancements/` (repo only — not shipped with
`pip install limit-agentbench`). Enable with:

```bash
export ENHANCEMENTS_ENABLED=true
```

| Module | Purpose |
|---|---|
| **LIMIT Graph** | Topology-aware metrics (centrality, connectivity) influence routing |
| **MODP** | Multi-objective trade-offs over accuracy, energy, latency, carbon |
| **RLHF** | Human feedback score integrated into reward shaping |
| **Multi-Teacher On-Policy Distillation** | Lightweight student learned from rule-based, ML, Q-learning, RLHF teachers |
| **MoE Expert Gating** | Dynamic blending of teacher outputs |
| **Bio-inspired Optimisation** | Evolutionary tuning of weights and hyperparameters |
| **FlexGen Integration** | Optional high-throughput LLM execution with adaptive precision |

### Enhancement flags

| Flag | Default | Effect |
|---|---|---|
| `ENHANCEMENTS_ENABLED` | `false` | Master switch — `false` forces all sub-flags off |
| `LIMIT_GRAPH_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `MODP_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `RLHF_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `DISTILLATION_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `MOE_GATING_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `EVOLUTIONARY_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `FLEXGEN_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |

`ENHANCEMENTS_ENABLED=false` is a **hard override** — sub-flags are ignored,
not errored.

---

## Testing

```bash
# Installation check
python test_installation.py

# Smoke tests
pytest -q

# Demo (core only)
python demo_green_benchmark.py

# Demo with enhancements (requires repo present + modules installed)
python demo_green_benchmark.py --enhanced

# Link checker (runs in CI)
python scripts/verify_links.py INDEX.md
```

CI runs all of the above on every push/PR to `main`.

---

## Supported configurations

### Grid regions (13)

`US-CA`, `US-TX`, `US-NY`, `EU-FR`, `EU-DE`, `EU-NO`, `EU-PL`, `CN`, `IN`,
`JP`, `AU`, `BR`, `GLOBAL`.

Static reference intensities:

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

These are **static defaults** — production deployments must wire a live
grid-intensity provider.

### Hardware profiles (10)

`nvidia_a100`, `nvidia_v100`, `nvidia_t4`, `nvidia_rtx_3090`,
`nvidia_rtx_4090`, `amd_mi250`, `google_tpu_v4`, `cpu_intel_xeon`,
`cpu_amd_epyc`, `default`.

Each maps to a TDP×utilization model in `metrics/energy_tracker.py`.

---

## Known limitations

1. **Grid intensities are static reference values.** Production must wire a
   live provider.
2. **`--enhanced` requires the repo present** — a pip-installed
   `limit-agentbench` cannot import `src.enhancements.*`. Enhancements are
   repo-only.
3. **`SustainabilityIndex` ceilings are fixed** (1.0 / 1.0 / 1.0). Workloads
   outside this range must construct their own instance.
4. **Enhancement modules are not tested in CI.** The roadmap marks Phase 5
   "Complete" for implementation, not test coverage.
5. **Public leaderboard REST API not delivered** (Roadmap Phase 4).
6. **Phases 2–4 of the roadmap are not complete.** Status is Beta.
7. **`ENHANCEMENTS_ENABLED=false` is a hard override** — sub-flags are
   ignored, not errored.
8. **`EnergyTracker` uses TDP × wall-clock**, not hardware counters. Real
   measurements require RAPL/NVML integration.
9. **`LeaderboardEntry` schema is frozen** — adding fields is breaking.
10. **`GreenMetricsTracker` is an alias for `EnergyTracker`** — do not
    instantiate both expecting separate state.

---

## Security

- **No secrets in code.** Grid-API keys and LLM API keys must come from
  `$GREEN_AGENT_SECRETS_DIR`, not environment variables.
- **`ENHANCEMENTS_ENABLED=false` is a hard kill switch** — no enhancement
  code runs, no network calls to FlexGen, no adaptive auth.
- **`FeedbackEvent` never logs raw user text** — only scores and metrics.
- **`task_id` and `agent_name`** are restricted to `[A-Za-z0-9._-]{1,64}` to
  prevent log injection and CSV formula injection.
- **`GreenLeaderboard.submit`** validates every numeric field at
  construction; malformed submissions raise `ValueError`.
- **`AgentBenchAdapter.create_task`** validates the task schema before
  invoking the agent — no unvalidated dicts reach agent code.

Report security issues to the maintainers via private disclosure, not the
public issue tracker.

---

## Roadmap

| Phase | Item | Status |
|---|---|---|
| 1 | AgentBench protocol | ✅ Complete |
| 1 | Green metrics tracking | ✅ Complete |
| 1 | Multi-framework adapters | ✅ Complete |
| 1 | Unified leaderboard | ✅ Complete |
| 2 | NSN integration bridge | ⏳ Planned |
| 2 | Level 5 MetaAgent bridge | ⏳ Planned |
| 2 | limit-benchmark crate bridge | ⏳ Planned |
| 3 | Energy consumption charts | ⏳ Planned |
| 3 | Carbon footprint dashboard | ⏳ Planned |
| 3 | Interactive comparison matrix | ⏳ Planned |
| 4 | Hugging Face Spaces dashboard | ⏳ Planned |
| 4 | Public leaderboard REST API | ⏳ Planned |
| 5 | LIMIT Graph metrics integration | ✅ Complete (repo-only) |
| 5 | MODP multi-objective scoring | ✅ Complete |
| 5 | RLHF human feedback | ✅ Complete |
| 5 | Multi-teacher distillation + MoE gating | ✅ Complete |
| 5 | Bio-inspired evolutionary optimisation | ✅ Complete |
| 5 | FlexGen execution backend hooks | ✅ Complete |

Phases 2–4 are **not complete**. Status is Beta until they are.

---

## Contributing

Contributions are welcome, especially:

- Additional framework adapters (e.g., Semantic Kernel, Haystack)
- Visualization components for the dashboard
- Integration bridges (NSN, Level 5 MetaAgent, limit-benchmark crate)
- Documentation improvements and examples
- Bug fixes and performance optimisations
- New teachers for the distillation ensemble
- Enhancements to MODP / RLHF / evolutionary modules

Before opening a PR:

1. Run `python scripts/verify_links.py INDEX.md` (must pass)
2. Run `python test_installation.py` (must pass)
3. Run `pytest -q` (must pass)
4. Update `INDEX.md` if you add or change public API
5. Update the changelog at the top of `INDEX.md`

---

## License

Apache-2.0. See [LICENSE](LICENSE) for the full text.

