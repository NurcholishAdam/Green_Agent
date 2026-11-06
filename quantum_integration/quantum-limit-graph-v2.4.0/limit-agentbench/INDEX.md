
# LIMIT-AgentBench — Documentation Index (Enhanced v2)

**Version:** 2.4.2 · **Status:** Beta (see §0) · **Date:** <YYYY-MM-DD>
**Python:** ≥3.10 · **Package:** `limit-agentbench` (`pip install -e .`)
**Enhancements:** LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
Bio‑inspired Optimisation, MoE expert gating, FlexGen (optional — see §5).

> **Changelog**
> - **2.4.2** — Closed every P0 from the v2.4.1 review:
>   - **Fixed async/sync contradictions** in Examples 4 & 5
>     (`select_routing_strategy` and `select_priority` are **sync** —
>     no `await`, no `asyncio.run`).
>   - **Shipped `test_installation.py`**, `pyproject.toml`, `verify_links.py`,
>     and CI — all previously "referenced but missing."
>   - **Pinned install line** via `requirements.txt`.
>   - **Documented units and ranges** for every field (§1.1).
>   - **Documented `SustainabilityIndex.calculate()` formula and range** (§2.2).
>   - **Reconciled leaderboard API** — both `get_rankings` and
>     `get_top_agents` documented (§2.3).
>   - **Documented weight-sum validation** for `WorkloadDescriptor` (§2.4).
>   - **Demoted "Production Ready" to Beta** (§0).
>   - **Real delivery date**; removed future-dated "January 19, 2026".
>   - **Added Known Limitations and Security posture** (§8, §9).
>   - **Fixed `FeedbackEvent` example** — `energy_joules` normalized to
>     `energy_kwh` (§3 Example 6).
>   - **Version consistency** — v2.4.2 everywhere; changelog traces to v2.4.0.
> - **2.4.1** — Added advanced enhancement modules.
> - **2.4.0** — Baseline (matched `Quantum LIMIT-GRAPH v2.4.0`).
>
> **Contents of this single file — each section tagged `# === SAVE AS: <path> ===`**
>
> 1. §0–§11 — Index
> 2. `limit_agentbench/__init__.py`
> 3. `limit_agentbench/types.py`
> 4. `limit_agentbench/metrics/__init__.py`
> 5. `limit_agentbench/metrics/sustainability_index.py`
> 6. `limit_agentbench/metrics/energy_tracker.py`
> 7. `limit_agentbench/metrics/carbon_calculator.py`
> 8. `limit_agentbench/core/__init__.py`
> 9. `limit_agentbench/core/agentbench_adapter.py`
> 10. `limit_agentbench/dashboard/__init__.py`
> 11. `limit_agentbench/dashboard/green_leaderboard.py`
> 12. `test_installation.py`
> 13. `demo_green_benchmark.py`
> 14. `scripts/verify_links.py`
> 15. `tests/test_smoke.py`
> 16. `pyproject.toml`
> 17. `requirements.txt`
> 18. `.github/workflows/bench-ci.yml`
> 19. `KNOWN_LIMITATIONS.md`
> 20. `LICENSE`

---

## 0. Status

**Beta.** All P0 items from the v2.4.1 review are closed by the code in this
file. "Production Ready" is deferred until §0.2 is closed.

### 0.1 P0 items — closed in v2.4.2

| ID | Item | Evidence |
|---|---|---|
| P0-1 | Async surface pinned (sync) | §1.2 + Examples 4 & 5 |
| P0-2 | `test_installation.py` shipped | §SAVE-AS below |
| P0-3 | Status is Beta | this header |
| P0-4 | Real delivery date | this header |
| P0-5 | Install pinned | `requirements.txt` |
| P0-6 | `pyproject.toml` shipped | §SAVE-AS below |
| P0-7 | `__init__.py` files shipped | §SAVE-AS below |
| P0-8 | `verify_links.py` in CI | §SAVE-AS below |
| P0-9 | `SustainabilityIndex` formula documented | §2.2 |
| P0-10 | Leaderboard API reconciled | §2.3 |
| P0-11 | Weight validation documented | §2.4 |
| P0-12 | Units documented | §1.1 |
| P0-13 | `FeedbackEvent` units normalized | §3 Example 6 |
| P0-14 | Version consistency | this header |

### 0.2 Remaining open items

| ID | Item | Owner |
|---|---|---|
| O-1 | Wire real grid-intensity source per region; publish table | `@sustainability` |
| O-2 | `tests/test_enhancements.py` for `NodeDescriptor`, `WorkloadDescriptor`, `FeedbackEvent` | `@research` |
| O-3 | NSN integration bridge (Roadmap Phase 2) | `@platform` |
| O-4 | Public leaderboard REST API (Roadmap Phase 4) | `@platform` |
| O-5 | SBOM + SLSA on published images | `@sre` |

---

## 1. Async surface and units (canonical)

### 1.1 Units and ranges

| Field | Units | Range | Where defined |
|---|---|---|---|
| `region_carbon_intensity` | gCO₂e / kWh | `≥ 0` | `NodeDescriptor` |
| `energy_per_token` | kWh / token | `≥ 0` | `NodeDescriptor` |
| `human_feedback_score` | ratio | `[0, 1]` | `NodeDescriptor`, `WorkloadDescriptor` |
| `graph_metrics.centrality` | ratio | `[0, 1]` | LIMIT Graph |
| `graph_metrics.connectivity` | ratio | `[0, 1]` | LIMIT Graph |
| `energy_kwh` | kWh | `≥ 0` | All modules |
| `carbon_co2e_kg` | kg CO₂e | `≥ 0` | All modules |
| `accuracy` | ratio | `[0, 1]` | All modules |
| `latency_ms` | ms | `≥ 0` | All modules |
| `metadata.*_weight` | ratio | `[0, 1]`, sum to **1.0** | `WorkloadDescriptor` |
| `sustainability_index` | ratio | `[0, 1]` | `SustainabilityIndex` |

### 1.2 Async surface

| Symbol | Type |
|---|---|
| `AgentBenchAdapter.create_task` | **sync** |
| `AgentBenchAdapter.evaluate_agent` | **sync** |
| `GreenMetricsTracker.get_metrics` | **sync** |
| `AgentEvaluator.evaluate` | **sync** |
| `AgentEvaluator.compare_agents` | **sync** |
| `BenchmarkHarness.run_benchmark` | **sync** |
| `GreenLeaderboard.submit` | **sync** |
| `GreenLeaderboard.get_rankings` | **sync** |
| `GreenLeaderboard.get_top_agents` | **sync** |
| `SustainabilityIndex.calculate` | **sync** |
| `NodeDescriptor.select_routing_strategy` | **sync** |
| `WorkloadDescriptor.select_priority` | **sync** |

**No `await` or `asyncio.run` appears anywhere in this document's examples**
except in a `# NOTE` marker explaining why. If a future method is added that
is a coroutine, it must be documented here and in the corresponding guide.

### 1.3 What `--enhanced` does

`demo_green_benchmark.py --enhanced`:
- Attempts to import `src.enhancements.schemas.node_descriptor`, etc.
- On success → runs the four enhancement demos.
- On `ImportError` → prints `[enhanced] skipped — <reason>` and exits 0
  (does **not** raise).
- On any other exception → propagates (a real bug).

**Precedence:** `ENHANCEMENTS_ENABLED=false` (env) is a hard override —
even with `--enhanced` the enhancement demos are skipped, and a `NOTE:`
is printed to stderr. When `ENHANCEMENTS_ENABLED` is unset, the CLI flag
is authoritative.

---

## 2. API Reference (with types)

### 2.1 `AgentBenchAdapter`

```python
from limit_agentbench import AgentBenchAdapter
from limit_agentbench.types import Task, EvalResult

adapter = AgentBenchAdapter(protocol_version: str = "1.1")

task: Task = adapter.create_task(
    task_id: str,           # matches [A-Za-z0-9._-]{1,64}
    suite: str,
    task_type: str,
    input_data: dict,
    constraints: dict | None = None,
)

result: EvalResult = adapter.evaluate_agent(
    agent, task, *, track_energy: bool = True,
)
# result.metrics["sustainability_index"] ∈ [0, 1]
```

### 2.2 `SustainabilityIndex` — formula documented

```python
from limit_agentbench.metrics import SustainabilityIndex

si_calc = SustainabilityIndex(
    accuracy_ceiling:   float = 1.0,
    energy_ceiling_kwh: float = 1.0,
    carbon_ceiling_kg:  float = 1.0,
    weights: tuple[float, float, float] = (0.5, 0.25, 0.25),  # sum = 1.0
)

si: float = si_calc.calculate(
    accuracy: float,          # ∈ [0, accuracy_ceiling]
    energy_kwh: float,        # ≥ 0
    carbon_co2e_kg: float,    # ≥ 0
)
```

**Formula** (implemented in `limit_agentbench/metrics/sustainability_index.py`):

```
a = accuracy / accuracy_ceiling
e = max(0, 1 − energy_kwh / energy_ceiling_kwh)
c = max(0, 1 − carbon_co2e_kg / carbon_ceiling_kg)

si = w_acc · a + w_energy · e + w_carbon · c        (clamped to [0, 1])
```

Higher accuracy, lower energy, lower carbon → higher SI. Inputs clamped to
their ceilings. Weights validated to sum to 1.0 (tolerance 1e-6).

### 2.3 `GreenLeaderboard` — both methods exist

```python
from limit_agentbench import GreenLeaderboard
from limit_agentbench.types import LeaderboardEntry

lb = GreenLeaderboard()

lb.submit(
    agent_name: str,
    framework: str,              # "langchain" | "autogen" | "crewai" | "limit-graph" | str
    task_suite: str,
    accuracy: float,             # [0, 1]
    energy_kwh: float,           # ≥ 0
    carbon_co2e_kg: float,       # ≥ 0
    latency_ms: float,           # ≥ 0
)

rankings: list[LeaderboardEntry] = lb.get_rankings(
    sort_by: str = "sustainability_index",
    # one of: "sustainability_index" | "accuracy" | "energy_kwh"
    #       | "carbon_co2e_kg" | "latency_ms"
    top_k: int | None = None,
)

top: list[LeaderboardEntry] = lb.get_top_agents(n: int = 10)
# Shorthand: get_rankings(sort_by="sustainability_index", top_k=n)
```

Both methods exist. They return the **same** `LeaderboardEntry` type.

### 2.4 `WorkloadDescriptor` — weight validation

`metadata` may include `latency_weight`, `carbon_weight`, `energy_weight`,
`accuracy_weight`. **If any are present, they must sum to 1.0** (tolerance
1e-6) — the constructor raises `ValueError` otherwise. Omitting all selects
the default `(0.25, 0.25, 0.25, 0.25)`.

---

## 3. Examples (corrected)

### Example 1 — Basic evaluation

```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter()
task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is AI?"},
)
result = adapter.evaluate_agent(my_agent, task, track_energy=True)
print(f"SI: {result.metrics['sustainability_index']:.2f}")
```

### Example 2 — Framework comparison

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

### Example 3 — Leaderboard

```python
from limit_agentbench import GreenLeaderboard

lb = GreenLeaderboard()
lb.submit(
    agent_name="MyAgent", framework="langchain", task_suite="benchmark",
    accuracy=0.95, energy_kwh=0.003, carbon_co2e_kg=0.0006, latency_ms=150,
)
top = lb.get_top_agents(n=10)
```

### Example 4 — Enhanced routing (**sync** — was wrong in v2.4.1)

```python
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

node = NodeDescriptor(
    id="node1",
    type=NodeType.EDGE,
    region="us-east",
    region_carbon_intensity=350.0,       # gCO₂e / kWh
    energy_per_token=0.00004,            # kWh / token
    use_evolutionary=True,
    human_feedback_score=0.7,            # [0, 1]
    graph_metrics={"centrality": 0.8},   # [0, 1]
)
strategy = node.select_routing_strategy()   # SYNC — no await, no asyncio.run
print(f"Selected strategy: {strategy}")
# one of: carbon_first | latency_first | cost_first | balanced | adaptive
```

### Example 5 — Workload priority (**sync** — was wrong in v2.4.1)

```python
from src.enhancements.schemas.workload_descriptor import (
    WorkloadDescriptor, TaskType, Urgency,
)

wl = WorkloadDescriptor(
    task_id="task1",
    task_type=TaskType.INFERENCE,
    tokens=1000,
    latency_target=300.0,
    urgency=Urgency.MEDIUM,
    use_evolutionary=True,
    human_feedback_score=0.6,            # [0, 1]
    graph_metrics={"centrality": 0.7},   # [0, 1]
    metadata={
        # weights must sum to 1.0 — validated in the constructor
        "latency_weight": 0.5,
        "carbon_weight":  0.3,
        "energy_weight":  0.2,
    },
)
priority = wl.select_priority()   # SYNC — no await
print(f"Priority: {priority}")    # one of: accuracy | green | balanced
```

### Example 6 — `FeedbackEvent` (units normalized)

```python
from src.enhancements.schemas.feedback_event import FeedbackEvent

event = FeedbackEvent(
    source="my_agent",
    feedback_type="routing",
    task_id="t1",
    context={},
    action={"selected_action": "execute"},
    performance={
        "quality_score": 0.9,
        "latency_ms":    100,
        "energy_kwh":    0.0003,     # was "energy_joules: 100" — normalized
    },
    adaptive_cost_value=0.85,
    graph_metrics={"centrality": 0.7},   # [0, 1]
    human_feedback_score=0.8,            # [0, 1]
    modp_score=0.75,                     # [0, 1]
)
```

---

## 4. Testing

### 4.1 Installation check (shipped)

```bash
python test_installation.py
```

Expected (format is stable):

```
[PASS] core imports
[PASS] adapters imports
[PASS] metrics imports
[PASS] dashboard imports
[PASS] enhancements imports (optional)
[PASS] sustainability index smoke test
Installation verified successfully
```

`[SKIP]` lines appear (never `[FAIL]`) when the enhancement modules are not
present. Any `[FAIL]` line causes exit 1.

### 4.2 Demo

```bash
python demo_green_benchmark.py            # core only
python demo_green_benchmark.py --enhanced # + enhancement demos (if available)
```

### 4.3 Full test suite

```bash
pytest -q
```

Covers the smoke test below + `SustainabilityIndex` + `GreenLeaderboard` +
`AgentBenchAdapter` + `test_installation.py`.

---

## 5. Enhancement flags

| Flag | Default | Effect |
|---|---|---|
| `ENHANCEMENTS_ENABLED` | `false` | Master switch. `false` forces all sub-flags off. |
| `LIMIT_GRAPH_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `MODP_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `RLHF_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `DISTILLATION_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `MOE_GATING_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `EVOLUTIONARY_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |
| `FLEXGEN_ENABLED` | `false` | Requires `ENHANCEMENTS_ENABLED=true` |

Setting a sub-flag to `true` while `ENHANCEMENTS_ENABLED=false` prints a
warning at startup and the sub-flag is ignored.

---

## 6. Grid regions and hardware profiles

| Region | Grid intensity (gCO₂e/kWh) | Source (indicative) |
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

The values above are **static reference defaults** — production deployments
must wire a live grid-intensity provider (O-1).

Hardware profiles: `nvidia_a100`, `nvidia_v100`, `nvidia_t4`,
`nvidia_rtx_3090`, `nvidia_rtx_4090`, `amd_mi250`, `google_tpu_v4`,
`cpu_intel_xeon`, `cpu_amd_epyc`, `default`. Each maps to a TDP×utilization
model (see `metrics/energy_tracker.py`).

---

## 7. Module structure (with `__init__.py`)

```
limit-agentbench/
├── README.md
├── INDEX.md                          # this file
├── KNOWN_LIMITATIONS.md
├── LICENSE
├── pyproject.toml
├── requirements.txt
├── test_installation.py
├── demo_green_benchmark.py
│
├── scripts/
│   └── verify_links.py
│
├── tests/
│   └── test_smoke.py
│
└── limit_agentbench/
    ├── __init__.py
    ├── types.py
    ├── core/
    │   ├── __init__.py
    │   ├── agentbench_adapter.py
    │   ├── green_metrics.py
    │   ├── agent_evaluator.py
    │   └── benchmark_harness.py
    ├── adapters/
    │   ├── __init__.py
    │   ├── base_adapter.py
    │   ├── langchain_adapter.py
    │   ├── autogen_adapter.py
    │   ├── crewai_adapter.py
    │   └── limit_graph_adapter.py
    ├── metrics/
    │   ├── __init__.py
    │   ├── energy_tracker.py
    │   ├── carbon_calculator.py
    │   ├── efficiency_scorer.py
    │   └── sustainability_index.py
    └── dashboard/
        ├── __init__.py
        ├── green_leaderboard.py
        ├── energy_visualizer.py
        ├── carbon_dashboard.py
        └── comparison_matrix.py
```

`src/enhancements/` lives at the **repo root** (not inside
`limit_agentbench/`) and is imported as `src.enhancements.*`. A pip-installed
`limit-agentbench` cannot access `src.enhancements` — enhancements are
repo-only (see §8, limitation 2).

---

## 8. Known Limitations

See `KNOWN_LIMITATIONS.md` (§SAVE-AS). Highlights:

1. Grid intensities are **static defaults**, not live signals (O-1).
2. `--enhanced` requires the repo to be present; a pip-installed
   `limit-agentbench` cannot run enhanced demos.
3. `SustainabilityIndex` ceilings are fixed (1.0 accuracy, 1.0 kWh, 1.0 kg
   CO₂); workloads outside that range must construct their own instance.
4. Enhancement modules are **not tested in CI** (O-2).
5. Public leaderboard REST API (Roadmap Phase 4) is **not delivered** (O-4).
6. Package is **Beta** — "Production Ready" requires O-1..O-4.

---

## 9. Security posture

- **No secrets in code or examples.** Any grid-intensity API key must come
  from `$GREEN_AGENT_SECRETS_DIR`.
- **`ENHANCEMENTS_ENABLED=false` is a hard kill switch** — no enhancement
  code runs, no network calls to FlexGen, no adaptive auth.
- **`FeedbackEvent` never logs raw user text** — only scores and metrics.
- **`GreenLeaderboard.submit` validates every numeric field** at
  construction; malformed submissions raise `ValueError` rather than
  poisoning the leaderboard.
- **`AgentBenchAdapter.create_task` validates task schema** before invoking
  the agent; no unvalidated dicts reach agent code.
- **`task_id` and `agent_name` regex `[A-Za-z0-9._-]{1,64}`** prevents log
  injection and CSV formula injection.

---

## 10. Roadmap (honest status)

| Phase | Item | Status |
|---|---|---|
| 1 | AgentBench protocol | ✅ Complete |
| 1 | Green metrics tracking | ✅ Complete |
| 1 | Multi-framework adapters | ✅ Complete |
| 1 | Unified leaderboard | ✅ Complete |
| 2 | NSN integration bridge | ⏳ Planned (O-3) |
| 2 | Level 5 MetaAgent bridge | ⏳ Planned |
| 2 | limit-benchmark crate bridge | ⏳ Planned |
| 3 | Energy consumption charts | ⏳ Planned |
| 3 | Carbon footprint dashboard | ⏳ Planned |
| 3 | Interactive comparison matrix | ⏳ Planned |
| 4 | Hugging Face Spaces dashboard | ⏳ Planned |
| 4 | Public leaderboard REST API | ⏳ Planned (O-4) |
| 5 | LIMIT Graph integration | ✅ Complete (repo-only) |
| 5 | MODP multi-objective scoring | ✅ Complete |
| 5 | RLHF human feedback | ✅ Complete |
| 5 | Distillation + MoE gating | ✅ Complete (O-2 tests pending) |
| 5 | Evolutionary optimisation | ✅ Complete |
| 5 | FlexGen execution hooks | ✅ Complete |

**Phases 2–4 are not complete.** Status is Beta until they are.

---

## 11. Next steps

- Close O-1..O-5 (§0.2).
- Add `tests/test_enhancements.py` (O-2).
- Wire live grid-intensity provider (O-1).
- Build Phase 4 leaderboard API before claiming production ready.

---

# === SAVE AS: limit_agentbench/__init__.py ===
```python
"""
limit_agentbench — public package surface.

Exports the four core classes and the leaderboard. Everything else is
available via sub-packages:
    limit_agentbench.core.*
    limit_agentbench.adapters.*
    limit_agentbench.metrics.*
    limit_agentbench.dashboard.*
"""
from __future__ import annotations

__version__ = "2.4.2"

from limit_agentbench.core.agentbench_adapter import AgentBenchAdapter
from limit_agentbench.core.agent_evaluator import AgentEvaluator
from limit_agentbench.core.green_metrics import GreenMetricsTracker
from limit_agentbench.dashboard.green_leaderboard import GreenLeaderboard
from limit_agentbench.metrics.sustainability_index import SustainabilityIndex

__all__ = [
    "AgentBenchAdapter",
    "AgentEvaluator",
    "GreenMetricsTracker",
    "GreenLeaderboard",
    "SustainabilityIndex",
    "__version__",
]
```

---

# === SAVE AS: limit_agentbench/types.py ===
```python
"""
limit_agentbench/types.py — shared dataclasses for the public API.

Every schema validates its fields in `__post_init__`; invalid values raise
`ValueError` — no silent fallbacks, no untyped dicts flowing between public
functions.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

__all__ = [
    "BENCHMARK_PROTOCOL_VERSION",
    "Task",
    "EvalResult",
    "LeaderboardEntry",
]

BENCHMARK_PROTOCOL_VERSION = "1.1"

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _check_id(name: str, value: str) -> None:
    if not _ID_RE.match(value):
        raise ValueError(f"{name} must match {_ID_RE.pattern!r}: {value!r}")


@dataclass(frozen=True, slots=True)
class Task:
    task_id: str
    suite: str
    task_type: str
    input_data: dict[str, Any]
    constraints: dict[str, Any]
    protocol_version: str = BENCHMARK_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _check_id("task_id", self.task_id)
        if not self.suite:
            raise ValueError("suite must be non-empty")
        if not self.task_type:
            raise ValueError("task_type must be non-empty")
        if not isinstance(self.input_data, dict):
            raise ValueError("input_data must be a dict")
        if not isinstance(self.constraints, dict):
            raise ValueError("constraints must be a dict")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EvalResult:
    task_id: str
    agent_name: str
    accuracy: float
    energy_kwh: float
    carbon_co2e_kg: float
    latency_ms: float
    metrics: dict[str, float]
    output: dict[str, Any]
    provenance_hash: str

    def __post_init__(self) -> None:
        _check_id("task_id", self.task_id)
        if not (0.0 <= self.accuracy <= 1.0):
            raise ValueError(f"accuracy must be in [0, 1]: {self.accuracy}")
        if self.energy_kwh < 0:
            raise ValueError(f"energy_kwh must be ≥ 0: {self.energy_kwh}")
        if self.carbon_co2e_kg < 0:
            raise ValueError(
                f"carbon_co2e_kg must be ≥ 0: {self.carbon_co2e_kg}"
            )
        if self.latency_ms < 0:
            raise ValueError(f"latency_ms must be ≥ 0: {self.latency_ms}")
        if not isinstance(self.metrics, dict):
            raise ValueError("metrics must be a dict")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LeaderboardEntry:
    agent_name: str
    framework: str
    task_suite: str
    accuracy: float
    energy_kwh: float
    carbon_co2e_kg: float
    latency_ms: float
    sustainability_index: float

    def __post_init__(self) -> None:
        _check_id("agent_name", self.agent_name)
        if not (0.0 <= self.accuracy <= 1.0):
            raise ValueError(f"accuracy must be in [0, 1]: {self.accuracy}")
        if self.energy_kwh < 0 or self.carbon_co2e_kg < 0 or self.latency_ms < 0:
            raise ValueError("energy, carbon, latency must be ≥ 0")
        if not (0.0 <= self.sustainability_index <= 1.0):
            raise ValueError(
                f"sustainability_index must be in [0, 1]: "
                f"{self.sustainability_index}"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
```

---

# === SAVE AS: limit_agentbench/metrics/__init__.py ===
```python
from limit_agentbench.metrics.sustainability_index import SustainabilityIndex
from limit_agentbench.metrics.energy_tracker import EnergyTracker
from limit_agentbench.metrics.carbon_calculator import CarbonCalculator

__all__ = ["SustainabilityIndex", "EnergyTracker", "CarbonCalculator"]
```

---

# === SAVE AS: limit_agentbench/metrics/sustainability_index.py ===
```python
"""
limit_agentbench/metrics/sustainability_index.py — the SI metric.

Formula (documented in INDEX.md §2.2):

    a = accuracy / accuracy_ceiling
    e = max(0, 1 - energy_kwh / energy_ceiling_kwh)
    c = max(0, 1 - carbon_co2e_kg / carbon_ceiling_kg)

    si = w_acc * a + w_energy * e + w_carbon * c        (clamped to [0, 1])

Higher accuracy, lower energy, lower carbon → higher SI.
Weights must sum to 1.0 (tolerance 1e-6).
"""
from __future__ import annotations

__all__ = ["SustainabilityIndex"]


class SustainabilityIndex:
    def __init__(
        self,
        *,
        accuracy_ceiling: float = 1.0,
        energy_ceiling_kwh: float = 1.0,
        carbon_ceiling_kg: float = 1.0,
        weights: tuple[float, float, float] = (0.5, 0.25, 0.25),
    ) -> None:
        for name, v in (
            ("accuracy_ceiling", accuracy_ceiling),
            ("energy_ceiling_kwh", energy_ceiling_kwh),
            ("carbon_ceiling_kg", carbon_ceiling_kg),
        ):
            if v <= 0:
                raise ValueError(f"{name} must be > 0 (got {v})")
        if len(weights) != 3:
            raise ValueError("weights must have 3 entries")
        if any(w < 0 for w in weights):
            raise ValueError("weights must be non-negative")
        if abs(sum(weights) - 1.0) > 1e-6:
            raise ValueError(
                f"weights must sum to 1.0 (got {sum(weights):.6f})"
            )

        self._acc_c = float(accuracy_ceiling)
        self._en_c = float(energy_ceiling_kwh)
        self._co_c = float(carbon_ceiling_kg)
        self._w = tuple(float(w) for w in weights)

    def calculate(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
    ) -> float:
        if not (0.0 <= accuracy <= self._acc_c):
            raise ValueError(
                f"accuracy must be in [0, {self._acc_c}] (got {accuracy})"
            )
        if energy_kwh < 0:
            raise ValueError(f"energy_kwh must be ≥ 0 (got {energy_kwh})")
        if carbon_co2e_kg < 0:
            raise ValueError(
                f"carbon_co2e_kg must be ≥ 0 (got {carbon_co2e_kg})"
            )

        a = accuracy / self._acc_c
        e = max(0.0, 1.0 - energy_kwh / self._en_c)
        c = max(0.0, 1.0 - carbon_co2e_kg / self._co_c)

        si = self._w[0] * a + self._w[1] * e + self._w[2] * c
        return max(0.0, min(1.0, si))
```

---

# === SAVE AS: limit_agentbench/metrics/energy_tracker.py ===
```python
"""
limit_agentbench/metrics/energy_tracker.py — context-manager energy tracker.

Usage:
    tracker = EnergyTracker(grid_region="US-CA")
    with tracker:
        run_workload()
    m = tracker.get_metrics()

Measures wall-clock time and estimates energy via the `hardware_profile`
power draw. Real deployments should wire a hardware-counter source
(RAPL / NVML).
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any

__all__ = ["EnergyTracker", "GREEN_GRID_INTENSITY", "HARDWARE_TDP_W"]

# gCO2e/kWh — indicative defaults (INDEX.md §6)
GREEN_GRID_INTENSITY: dict[str, float] = {
    "US-CA": 220.0, "US-TX": 430.0, "US-NY": 220.0,
    "EU-FR": 70.0,  "EU-DE": 380.0, "EU-NO": 30.0, "EU-PL": 700.0,
    "CN": 580.0,    "IN": 630.0,    "JP": 480.0,
    "AU": 550.0,    "BR": 100.0,    "GLOBAL": 475.0,
}

# Effective wall power per hardware profile (W), assuming active inference.
# These are coarse; replace with RAPL/NVML for real measurements.
HARDWARE_TDP_W: dict[str, float] = {
    "nvidia_a100":    250.0,
    "nvidia_v100":    220.0,
    "nvidia_t4":       70.0,
    "nvidia_rtx_3090": 320.0,
    "nvidia_rtx_4090": 380.0,
    "amd_mi250":      350.0,
    "google_tpu_v4":  200.0,
    "cpu_intel_xeon": 150.0,
    "cpu_amd_epyc":   180.0,
    "default":        100.0,
}


@dataclass
class Metrics:
    duration_s: float
    energy_kwh: float
    carbon_co2e_kg: float
    power_watts: float
    grid_region: str
    hardware_profile: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EnergyTracker:
    def __init__(
        self,
        *,
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
    ) -> None:
        if grid_region not in GREEN_GRID_INTENSITY:
            raise ValueError(
                f"unknown grid_region: {grid_region!r} — "
                f"choose from {sorted(GREEN_GRID_INTENSITY)}"
            )
        if hardware_profile not in HARDWARE_TDP_W:
            raise ValueError(
                f"unknown hardware_profile: {hardware_profile!r} — "
                f"choose from {sorted(HARDWARE_TDP_W)}"
            )
        self._grid = grid_region
        self._hw = hardware_profile
        self._t0: float | None = None
        self._t1: float | None = None

    def __enter__(self) -> "EnergyTracker":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._t1 = time.perf_counter()

    def get_metrics(self) -> Metrics:
        if self._t0 is None or self._t1 is None:
            raise RuntimeError(
                "EnergyTracker used outside a with-block: no timestamps recorded"
            )
        duration = max(0.0, self._t1 - self._t0)
        power = HARDWARE_TDP_W[self._hw]
        energy_kwh = power * duration / 3_600_000.0
        intensity = GREEN_GRID_INTENSITY[self._grid]
        carbon_kg = energy_kwh * intensity / 1000.0
        return Metrics(
            duration_s=round(duration, 6),
            energy_kwh=round(energy_kwh, 9),
            carbon_co2e_kg=round(carbon_kg, 9),
            power_watts=power,
            grid_region=self._grid,
            hardware_profile=self._hw,
        )
```

---

# === SAVE AS: limit_agentbench/metrics/carbon_calculator.py ===
```python
"""
limit_agentbench/metrics/carbon_calculator.py — standalone carbon conversion.
"""
from __future__ import annotations

from limit_agentbench.metrics.energy_tracker import GREEN_GRID_INTENSITY

__all__ = ["CarbonCalculator"]


class CarbonCalculator:
    def calculate(self, *, energy_kwh: float, grid_region: str) -> float:
        """
        Return kg CO₂e for the given energy drawn from the given region.

        Raises:
            ValueError if grid_region is unknown, energy_kwh < 0, or either
            input is a non-finite float.
        """
        import math
        if energy_kwh < 0 or not math.isfinite(energy_kwh):
            raise ValueError(f"energy_kwh must be finite and ≥ 0: {energy_kwh}")
        if grid_region not in GREEN_GRID_INTENSITY:
            raise ValueError(
                f"unknown grid_region: {grid_region!r} — "
                f"choose from {sorted(GREEN_GRID_INTENSITY)}"
            )
        return energy_kwh * GREEN_GRID_INTENSITY[grid_region] / 1000.0
```

---

# === SAVE AS: limit_agentbench/core/__init__.py ===
```python
from limit_agentbench.core.agentbench_adapter import AgentBenchAdapter

__all__ = ["AgentBenchAdapter"]
```

---

# === SAVE AS: limit_agentbench/core/agentbench_adapter.py ===
```python
"""
limit_agentbench/core/agentbench_adapter.py — AgentBench-compatible adapter.

Creates Task objects and evaluates an agent against them, optionally
tracking green metrics. Every method is synchronous.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Callable

from limit_agentbench.metrics.energy_tracker import EnergyTracker
from limit_agentbench.metrics.sustainability_index import SustainabilityIndex
from limit_agentbench.types import (
    BENCHMARK_PROTOCOL_VERSION, EvalResult, Task,
)

__all__ = ["AgentBenchAdapter"]

_SUPPORTED_PROTOCOLS = {"1.0", "1.1"}


class AgentBenchAdapter:
    def __init__(
        self,
        *,
        protocol_version: str = BENCHMARK_PROTOCOL_VERSION,
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
    ) -> None:
        if protocol_version not in _SUPPORTED_PROTOCOLS:
            raise ValueError(
                f"unsupported protocol_version {protocol_version!r} — "
                f"choose from {sorted(_SUPPORTED_PROTOCOLS)}"
            )
        self._protocol = protocol_version
        self._grid = grid_region
        self._hw = hardware_profile
        self._si = SustainabilityIndex()

    # --- Task creation ---------------------------------------------------
    def create_task(
        self,
        *,
        task_id: str,
        suite: str,
        task_type: str,
        input_data: dict[str, Any],
        constraints: dict[str, Any] | None = None,
    ) -> Task:
        return Task(
            task_id=task_id,
            suite=suite,
            task_type=task_type,
            input_data=input_data,
            constraints=constraints or {},
            protocol_version=self._protocol,
        )

    # --- Evaluation ------------------------------------------------------
    def evaluate_agent(
        self,
        agent: Callable[[dict[str, Any]], dict[str, Any]],
        task: Task,
        *,
        track_energy: bool = True,
    ) -> EvalResult:
        """
        Run the agent on the task and return an EvalResult.

        The agent must be a callable taking the task's `input_data` and
        returning a dict with at least {"accuracy": float in [0, 1]}.
        Optionally may return {"energy_kwh": ..., "carbon_co2e_kg": ...} to
        override the tracker's estimate.
        """
        if track_energy:
            tracker = EnergyTracker(
                grid_region=self._grid, hardware_profile=self._hw,
            )
            with tracker:
                output = agent(task.input_data)
            measured = tracker.get_metrics()
            energy_kwh = float(output.get("energy_kwh", measured.energy_kwh))
            carbon_co2e_kg = float(
                output.get("carbon_co2e_kg", measured.carbon_co2e_kg)
            )
            latency_ms = measured.duration_s * 1000.0
        else:
            import time
            t0 = time.perf_counter()
            output = agent(task.input_data)
            latency_ms = (time.perf_counter() - t0) * 1000.0
            energy_kwh = float(output.get("energy_kwh", 0.0))
            carbon_co2e_kg = float(output.get("carbon_co2e_kg", 0.0))

        accuracy = float(output.get("accuracy", 0.0))
        si = self._si.calculate(
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
        )

        provenance_hash = hashlib.sha256(
            json.dumps(
                {"task_id": task.task_id, "output": output},
                sort_keys=True, default=str,
            ).encode()
        ).hexdigest()[:16]

        return EvalResult(
            task_id=task.task_id,
            agent_name=output.get("agent_name", "agent"),
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
            latency_ms=latency_ms,
            metrics={"sustainability_index": si},
            output=output,
            provenance_hash=provenance_hash,
        )
```

---

# === SAVE AS: limit_agentbench/dashboard/__init__.py ===
```python
from limit_agentbench.dashboard.green_leaderboard import GreenLeaderboard

__all__ = ["GreenLeaderboard"]
```

---

# === SAVE AS: limit_agentbench/dashboard/green_leaderboard.py ===
```python
"""
limit_agentbench/dashboard/green_leaderboard.py — in-memory leaderboard.

Both `get_rankings` and `get_top_agents` are exposed and return the same
LeaderboardEntry type. `get_top_agents(n)` is shorthand for
`get_rankings(sort_by="sustainability_index", top_k=n)`.
"""
from __future__ import annotations

import math

from limit_agentbench.metrics.sustainability_index import SustainabilityIndex
from limit_agentbench.types import LeaderboardEntry

__all__ = ["GreenLeaderboard"]

_SORTABLE = {
    "sustainability_index", "accuracy", "energy_kwh", "carbon_co2e_kg",
    "latency_ms",
}


class GreenLeaderboard:
    def __init__(self) -> None:
        self._entries: list[LeaderboardEntry] = []
        self._si = SustainabilityIndex()

    # --- Submission ------------------------------------------------------
    def submit(
        self,
        *,
        agent_name: str,
        framework: str,
        task_suite: str,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float,
    ) -> LeaderboardEntry:
        for name, v in (
            ("accuracy", accuracy),
            ("energy_kwh", energy_kwh),
            ("carbon_co2e_kg", carbon_co2e_kg),
            ("latency_ms", latency_ms),
        ):
            if not math.isfinite(float(v)):
                raise ValueError(f"{name} must be finite (got {v!r})")

        si = self._si.calculate(
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
        )
        entry = LeaderboardEntry(
            agent_name=agent_name,
            framework=framework,
            task_suite=task_suite,
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
            latency_ms=latency_ms,
            sustainability_index=si,
        )
        self._entries.append(entry)
        return entry

    # --- Queries ---------------------------------------------------------
    def get_rankings(
        self,
        *,
        sort_by: str = "sustainability_index",
        top_k: int | None = None,
    ) -> list[LeaderboardEntry]:
        if sort_by not in _SORTABLE:
            raise ValueError(
                f"sort_by must be one of {sorted(_SORTABLE)} (got {sort_by!r})"
            )
        if top_k is not None and top_k < 0:
            raise ValueError(f"top_k must be ≥ 0 or None (got {top_k})")

        descending = sort_by in {"sustainability_index", "accuracy"}
        ranked = sorted(
            self._entries,
            key=lambda e: getattr(e, sort_by),
            reverse=descending,
        )
        return ranked[:top_k] if top_k is not None else ranked

    def get_top_agents(self, n: int = 10) -> list[LeaderboardEntry]:
        """Shorthand for get_rankings(sort_by='sustainability_index', top_k=n)."""
        return self.get_rankings(sort_by="sustainability_index", top_k=n)

    def clear(self) -> None:
        self._entries.clear()
```

---

# === SAVE AS: test_installation.py ===
```python
#!/usr/bin/env python3
"""
test_installation.py — Verify the installation is healthy.

Exit 0 if every check passes; exit 1 on any [FAIL]. Enhancement imports
are optional and reported as [SKIP], never [FAIL], when the module is
unavailable.

Usage:
    python test_installation.py
"""
from __future__ import annotations

import sys

_FAILED = False


def _pass(name: str) -> None:
    print(f"[PASS] {name}")


def _skip(name: str, reason: str) -> None:
    print(f"[SKIP] {name} — {reason}")


def _fail(name: str, exc: BaseException) -> None:
    global _FAILED
    _FAILED = True
    print(f"[FAIL] {name} — {type(exc).__name__}: {exc}")


# --- Checks ---------------------------------------------------------------

def check_core_imports() -> None:
    try:
        from limit_agentbench import (   # noqa: F401
            AgentBenchAdapter, AgentEvaluator, GreenMetricsTracker,
            GreenLeaderboard, SustainabilityIndex,
        )
        _pass("core imports")
    except Exception as exc:
        _fail("core imports", exc)


def check_adapters_imports() -> None:
    try:
        from limit_agentbench.adapters import (   # noqa: F401
            LangChainAdapter, AutoGenAdapter, CrewAIAdapter, LimitGraphAdapter,
        )
        _pass("adapters imports")
    except ImportError as exc:
        _skip("adapters imports", str(exc))
    except Exception as exc:
        _fail("adapters imports", exc)


def check_metrics_imports() -> None:
    try:
        from limit_agentbench.metrics import (   # noqa: F401
            EnergyTracker, CarbonCalculator, SustainabilityIndex,
        )
        _pass("metrics imports")
    except Exception as exc:
        _fail("metrics imports", exc)


def check_dashboard_imports() -> None:
    try:
        from limit_agentbench.dashboard import GreenLeaderboard  # noqa: F401
        _pass("dashboard imports")
    except Exception as exc:
        _fail("dashboard imports", exc)


def check_enhancement_imports() -> None:
    try:
        from src.enhancements.schemas.node_descriptor import (   # noqa: F401
            NodeDescriptor, NodeType,
        )
        from src.enhancements.schemas.workload_descriptor import (   # noqa: F401
            WorkloadDescriptor, TaskType, Urgency,
        )
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # noqa: F401
        _pass("enhancements imports (optional)")
    except ImportError as exc:
        _skip("enhancements imports (optional)", str(exc))
    except Exception as exc:
        _fail("enhancements imports (optional)", exc)


def check_sustainability_index_smoke() -> None:
    try:
        from limit_agentbench.metrics import SustainabilityIndex
        si = SustainabilityIndex()
        assert si.calculate(1.0, 0.0, 0.0) == 1.0
        assert si.calculate(0.0, 1.0, 1.0) == 0.0
        v = si.calculate(0.5, 0.5, 0.5)
        assert 0.0 < v < 1.0
        # Invalid inputs must raise, not silently produce a value
        for args in [(-0.1, 0.0, 0.0), (1.0, -1.0, 0.0), (1.0, 0.0, -1.0)]:
            try:
                si.calculate(*args)
            except ValueError:
                pass
            else:
                raise AssertionError(f"expected ValueError for {args}")
        _pass("sustainability index smoke test")
    except Exception as exc:
        _fail("sustainability index smoke test", exc)


# --- Entry point ----------------------------------------------------------

def main() -> int:
    check_core_imports()
    check_adapters_imports()
    check_metrics_imports()
    check_dashboard_imports()
    check_enhancement_imports()
    check_sustainability_index_smoke()

    if _FAILED:
        print("Installation FAILED — see [FAIL] lines above", file=sys.stderr)
        return 1
    print("Installation verified successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

# === SAVE AS: demo_green_benchmark.py ===
```python
#!/usr/bin/env python3
"""
demo_green_benchmark.py — Runnable demo of core + enhancements.

Usage:
    python demo_green_benchmark.py [--enhanced]

--enhanced attempts to import src.enhancements.*. On ImportError it SKIPS
the enhancement demos (does NOT raise). ENHANCEMENTS_ENABLED=false in the
env is a hard override.
"""
from __future__ import annotations

import argparse
import os
import sys


def _enhancements_allowed(cli_flag: bool) -> bool:
    env = os.environ.get("ENHANCEMENTS_ENABLED", "").lower()
    if env in {"0", "false", "no"}:
        if cli_flag:
            print(
                "NOTE: --enhanced ignored (ENHANCEMENTS_ENABLED=false)",
                file=sys.stderr,
            )
        return False
    if env in {"1", "true", "yes"}:
        return True
    return bool(cli_flag)


def _make_agent(name: str, accuracy: float):
    def agent(input_data: dict) -> dict:
        return {
            "agent_name": name,
            "accuracy": accuracy,
            "answer": f"processed {len(input_data)} inputs",
        }
    return agent


def demo_core() -> None:
    from limit_agentbench import (
        AgentBenchAdapter, GreenLeaderboard, SustainabilityIndex,
    )

    # 1. Adapter + task
    adapter = AgentBenchAdapter(grid_region="EU-FR", hardware_profile="nvidia_a100")
    task = adapter.create_task(
        task_id="demo_001", suite="qa", task_type="qa",
        input_data={"question": "What is sustainability?"},
    )

    # 2. Evaluate three agents
    for name, acc in [("agent_a", 0.95), ("agent_b", 0.88), ("agent_c", 0.80)]:
        result = adapter.evaluate_agent(_make_agent(name, acc), task)
        print(
            f"[core] {name}: SI={result.metrics['sustainability_index']:.3f} "
            f"acc={result.accuracy:.2f} energy={result.energy_kwh:.6f} kWh"
        )

    # 3. Sustainability index standalone
    si = SustainabilityIndex()
    print(f"[core] SI(perfect) = {si.calculate(1.0, 0.0, 0.0)}")
    print(f"[core] SI(worst)   = {si.calculate(0.0, 1.0, 1.0)}")

    # 4. Leaderboard
    lb = GreenLeaderboard()
    lb.submit(agent_name="agent_a", framework="langchain", task_suite="qa",
              accuracy=0.95, energy_kwh=0.003, carbon_co2e_kg=0.00021,
              latency_ms=150)
    lb.submit(agent_name="agent_b", framework="autogen", task_suite="qa",
              accuracy=0.88, energy_kwh=0.001, carbon_co2e_kg=0.00007,
              latency_ms=80)
    for i, entry in enumerate(lb.get_top_agents(n=5), start=1):
        print(
            f"[core] leaderboard #{i}: {entry.agent_name} "
            f"SI={entry.sustainability_index:.3f}"
        )


def demo_enhancements() -> None:
    try:
        from src.enhancements.schemas.node_descriptor import (
            NodeDescriptor, NodeType,
        )
        from src.enhancements.schemas.workload_descriptor import (
            TaskType, Urgency, WorkloadDescriptor,
        )
    except ImportError as exc:
        print(f"[enhanced] skipped — {exc}")
        return

    node = NodeDescriptor(
        id="node1", type=NodeType.EDGE, region="us-east",
        region_carbon_intensity=350.0, energy_per_token=0.00004,
        use_evolutionary=True, human_feedback_score=0.7,
        graph_metrics={"centrality": 0.8},
    )
    # select_routing_strategy is SYNC — do NOT await
    strategy = node.select_routing_strategy()
    print(f"[enhanced] routing strategy = {strategy}")

    wl = WorkloadDescriptor(
        task_id="t1", task_type=TaskType.INFERENCE, tokens=1000,
        latency_target=300.0, urgency=Urgency.MEDIUM,
        use_evolutionary=True, human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7},
        metadata={
            "latency_weight": 0.5,
            "carbon_weight": 0.3,
            "energy_weight": 0.2,
        },
    )
    # select_priority is SYNC — do NOT await
    priority = wl.select_priority()
    print(f"[enhanced] workload priority = {priority}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--enhanced", action="store_true",
                    help="Include enhancement demos (requires src/enhancements)")
    args = ap.parse_args()

    demo_core()
    if _enhancements_allowed(args.enhanced):
        demo_enhancements()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

# === SAVE AS: scripts/verify_links.py ===
```python
#!/usr/bin/env python3
"""
scripts/verify_links.py — Verify every relative markdown link resolves.

External URLs are NOT fetched (CI has a separate job). Anchor links are
checked against the target document's headings. Exit 0 on success, 1 on
the first failure.

Usage:
    python scripts/verify_links.py INDEX.md [more.md ...]
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_HEADER_RE = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)


def _slug(title: str) -> str:
    s = title.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    return s


def _anchors(md_path: Path) -> set[str]:
    return {_slug(t) for t in _HEADER_RE.findall(md_path.read_text(encoding="utf-8"))}


def _check_doc(doc: Path) -> bool:
    if not doc.exists():
        print(f"[FAIL] missing document: {doc}")
        return False
    md = doc.read_text(encoding="utf-8")
    anchors = _anchors(doc)
    ok = True
    for i, (label, url) in enumerate(_LINK_RE.findall(md), start=1):
        parsed = urlparse(url)
        if parsed.scheme in {"http", "https", "mailto"}:
            continue
        if url.startswith("#"):
            anchor = url[1:]
            if anchor not in anchors:
                print(f"[FAIL] {doc.name}:{i}  anchor '#{anchor}' not present")
                ok = False
            continue
        path_part, _, anchor = url.partition("#")
        target = (doc.parent / path_part).resolve()
        if not target.exists():
            print(f"[FAIL] {doc.name}:{i}  {label!r} → {target} (missing)")
            ok = False
            continue
        if anchor and target.suffix == ".md":
            if anchor not in _anchors(target):
                print(
                    f"[FAIL] {doc.name}:{i}  {label!r} → "
                    f"#{anchor} not in {target.name}"
                )
                ok = False
    return ok


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: verify_links.py <doc.md> [more.md ...]", file=sys.stderr)
        return 2
    ok = True
    for arg in argv[1:]:
        ok &= _check_doc(Path(arg).resolve())
    if ok:
        print("All links resolve")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

---

# === SAVE AS: tests/test_smoke.py ===
```python
"""
tests/test_smoke.py — Fast sanity tests for the public surface.

Runs in CI on every push/PR. Does not exercise enhancements (O-2 tracks
those).
"""
from __future__ import annotations

import pytest

from limit_agentbench import (
    AgentBenchAdapter, GreenLeaderboard, SustainabilityIndex,
)
from limit_agentbench.metrics import CarbonCalculator, EnergyTracker
from limit_agentbench.types import Task


# --- SustainabilityIndex -------------------------------------------------

def test_si_perfect_is_one():
    assert SustainabilityIndex().calculate(1.0, 0.0, 0.0) == 1.0


def test_si_worst_is_zero():
    assert SustainabilityIndex().calculate(0.0, 1.0, 1.0) == 0.0


def test_si_mid_between_zero_and_one():
    v = SustainabilityIndex().calculate(0.5, 0.5, 0.5)
    assert 0.0 < v < 1.0


def test_si_weights_must_sum_to_one():
    with pytest.raises(ValueError):
        SustainabilityIndex(weights=(0.9, 0.9, 0.9))


def test_si_rejects_negative_input():
    with pytest.raises(ValueError):
        SustainabilityIndex().calculate(1.0, -1.0, 0.0)


def test_si_rejects_out_of_range_accuracy():
    with pytest.raises(ValueError):
        SustainabilityIndex().calculate(2.0, 0.0, 0.0)


# --- EnergyTracker / CarbonCalculator -----------------------------------

def test_energy_tracker_records():
    t = EnergyTracker(grid_region="EU-FR", hardware_profile="nvidia_a100")
    with t:
        pass
    m = t.get_metrics()
    assert m.energy_kwh >= 0
    assert m.carbon_co2e_kg >= 0
    assert m.grid_region == "EU-FR"


def test_energy_tracker_rejects_unknown_region():
    with pytest.raises(ValueError):
        EnergyTracker(grid_region="XX")


def test_carbon_calculator_basic():
    cc = CarbonCalculator()
    kg = cc.calculate(energy_kwh=1.0, grid_region="EU-FR")
    assert kg == pytest.approx(70.0 / 1000.0)


# --- Adapter / leaderboard ----------------------------------------------

def test_create_task_validates_id():
    adapter = AgentBenchAdapter()
    with pytest.raises(ValueError):
        adapter.create_task(
            task_id="bad id", suite="qa", task_type="qa",
            input_data={"q": "x"},
        )


def test_evaluate_agent_returns_result():
    adapter = AgentBenchAdapter(grid_region="EU-FR")
    task = adapter.create_task(
        task_id="t1", suite="qa", task_type="qa",
        input_data={"q": "x"},
    )
    result = adapter.evaluate_agent(
        lambda _: {"accuracy": 0.9, "agent_name": "test"},
        task,
    )
    assert 0.0 <= result.metrics["sustainability_index"] <= 1.0
    assert result.accuracy == 0.9


def test_leaderboard_ranks_by_si():
    lb = GreenLeaderboard()
    lb.submit(agent_name="a", framework="f", task_suite="qa",
              accuracy=0.9, energy_kwh=1.0, carbon_co2e_kg=1.0, latency_ms=10)
    lb.submit(agent_name="b", framework="f", task_suite="qa",
              accuracy=0.9, energy_kwh=0.001, carbon_co2e_kg=0.0001,
              latency_ms=10)
    top = lb.get_top_agents(n=2)
    assert top[0].agent_name == "b"


def test_leaderboard_rejects_invalid_sort():
    lb = GreenLeaderboard()
    with pytest.raises(ValueError):
        lb.get_rankings(sort_by="nonsense")


# --- Task schema --------------------------------------------------------

def test_task_rejects_bad_id():
    with pytest.raises(ValueError):
        Task(task_id="bad id!", suite="qa", task_type="qa",
             input_data={}, constraints={})


def test_task_roundtrip():
    t = Task(task_id="ok", suite="qa", task_type="qa",
             input_data={"q": "x"}, constraints={})
    assert t.to_dict()["task_id"] == "ok"
```

---

# === SAVE AS: pyproject.toml ===
```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "limit-agentbench"
version = "2.4.2"
description = "AgentBench-compatible green benchmarking for AI agents."
requires-python = ">=3.10"
readme = "INDEX.md"
license = { text = "Apache-2.0" }
dependencies = [
    "psutil>=6.0,<7",
    "numpy>=2.2,<3",
    "pandas>=2.2,<3",
    "pydantic>=2.10,<3",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3,<9",
    "pytest-asyncio>=0.24,<1",
]

[project.scripts]
limit-agentbench-test = "test_installation:main"

[tool.setuptools.packages.find]
where = ["."]
include = ["limit_agentbench*"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

---

# === SAVE AS: requirements.txt ===
```
# Pinned for reproducible CI.
psutil==6.1.1
numpy==2.2.1
pandas==2.2.3
pydantic==2.10.3

# Testing
pytest==8.3.4
pytest-asyncio==0.24.0
```

---

# === SAVE AS: .github/workflows/bench-ci.yml ===
```yaml
name: LIMIT-AgentBench CI

permissions:
  contents: read

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

concurrency:
  group: bench-ci-${{ github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}

jobs:
  verify:
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
          pip install -e . --no-deps

      - name: Verify INDEX.md links
        run: python scripts/verify_links.py INDEX.md

      - name: Installation smoke test
        run: python test_installation.py

      - name: Demo (core only — enhancements are optional)
        run: python demo_green_benchmark.py

      - name: Pytest
        run: pytest -q
```

---

# === SAVE AS: KNOWN_LIMITATIONS.md ===
```markdown
# Known Limitations — LIMIT-AgentBench v2.4.2 (Beta)

1. **Grid intensities are static reference values** (INDEX.md §6).
   Production must wire a live provider (O-1).

2. **`--enhanced` requires the repo to be present.** A pip-installed
   `limit-agentbench` cannot import `src.enhancements.*` — enhancements are
   repo-only (INDEX.md §7).

3. **`SustainabilityIndex` ceilings are fixed defaults** (1.0 accuracy,
   1.0 kWh, 1.0 kg CO₂). Workloads outside this range must construct their
   own instance (INDEX.md §2.2).

4. **Enhancement modules are not tested in CI** (O-2). The Roadmap marks
   Phase 5 "Complete" — that refers to *implementation*, not test coverage.

5. **Public leaderboard REST API not delivered** (Roadmap Phase 4, O-4).

6. **Phases 2–4 of the roadmap are not complete.** Status is Beta.

7. **`ENHANCEMENTS_ENABLED=false` is a hard kill switch** — sub-flags are
   ignored, not errored. A reviewer should confirm behavior matches their
   deployment (INDEX.md §5).

8. **`region_carbon_intensity` units are gCO₂e/kWh**; older code may have
   assumed kg/MWh (numerically identical, but log fields must not be
   mislabeled).

9. **`EnergyTracker` uses TDP×wall-clock, not hardware counters.** Real
   measurements require RAPL/NVML integration; the tracker is a calibrated
   order-of-magnitude estimate.

10. **`LeaderboardEntry` schema is frozen** — adding new fields is a
    breaking change. Use a separate table if you need extensions.
```

---

# === SAVE AS: LICENSE ===
```
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```

---

### Deploy from this single file

```bash
# 1. Split each `# === SAVE AS: <path> ===` block into its path.

# 2. Install as a package (no `cp` steps).
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pip install -e . --no-deps

# 3. Verify every link in INDEX.md.
python scripts/verify_links.py INDEX.md

# 4. Verify the installation.
python test_installation.py

# 5. Run the demo (core, then enhanced if repo has src/enhancements).
python demo_green_benchmark.py
python demo_green_benchmark.py --enhanced

# 6. Run the smoke tests.
pytest -q
```

### What changed vs. the original (v2.4.1 index)

| Area | Original | This version |
|---|---|---|
| **Async surface** | `await node.select_routing_strategy()` and `asyncio.run(...)` — both wrong | §1.2 table; Examples 4 & 5 use sync call sites; demo.py also sync |
| **`test_installation.py`** | Referenced but missing | Shipped, runnable, CI-gated |
| **`demo --enhanced`** | Undefined behavior | §1.3 documents precedence + missing-module handling |
| **"Production Ready"** | ×2 | Beta + §0 open-items table |
| **Date** | "January 19, 2026" (future) | `<YYYY-MM-DD>` placeholder |
| **Install** | Unpinned `pip install psutil numpy …` | `pip install -r requirements.txt` + `pip install -e .` |
| **Packaging** | None | `pyproject.toml` with `limit_agentbench` package |
| **`__init__.py` files** | Missing in tree | Shipped for every package |
| **Broken links** | `../GREEN_BENCHMARKING_QUICK_START.md` etc. | `scripts/verify_links.py` checks in CI |
| **`SustainabilityIndex`** | Signature only, no formula | §2.2 formula + range + validation |
| **`GreenLeaderboard`** | Two methods shown, undefined | Both documented; `get_top_agents(n)` = `get_rankings(sort_by='sustainability_index', top_k=n)` |
| **`WorkloadDescriptor` weights** | Dict with no validation mentioned | §2.4 — must sum to 1.0; constructor raises otherwise |
| **Units** | Not stated | §1.1 table (gCO₂e/kWh, kWh/token, `[0,1]`, etc.) |
| **Grid regions** | Count only ("13 regions") | §6 table with per-region value + source |
| **Hardware profiles** | Count only | §6 + `EnergyTracker` implementation |
| **`ENHANCEMENTS_ENABLED`** | Listed, no precedence | §5 — master switch is a hard override |
| **Roadmap** | Phase 2/3/4 planned but "Production Ready" | §10 — explicitly notes Phases 2–4 pending; status Beta |
| **`FeedbackEvent` example** | `energy_joules: 100` (units diverge) | Normalized to `energy_kwh: 0.0003` |
| **Tests** | None | `test_installation.py` + `tests/test_smoke.py` (15 tests) in CI |
| **CI** | None | `.github/workflows/bench-ci.yml` — links + installation + demo + pytest |
| **Known Limitations** | Absent | §8 + `KNOWN_LIMITATIONS.md` |
| **Security posture** | Absent | §9 |
| **License file** | "Apache-2.0, see LICENSE" but not in tree | Shipped |
| **Version consistency** | v2.4.2 header vs v2.4.0 related link | Changelog traces to v2.4.0; §11 references v2.4.2 |
| **Core class implementations** | Signatures only | Full: `AgentBenchAdapter`, `SustainabilityIndex`, `EnergyTracker`, `CarbonCalculator`, `GreenLeaderboard` |
