## Analysis summary

This is a **documentation index** for LIMIT-AgentBench v2.4.2, not code. It is more restrained than prior deliverables ("Status: Production Ready" appears twice but without "GROUNDBREAKING"-style superlatives), and it correctly uses `NodeType.EDGE` this time. However, it **persists the async/sync contradiction that has now appeared in 5 consecutive documents** — `select_routing_strategy` and `select_priority` are called via `await` in Examples 4 and 5, and wrapped in `asyncio.run(...)` in Example 4. Both are wrong; those methods are **synchronous** per the Expert Collaboration Guide v2 and every other enhanced doc.

Other issues worth closing:
- **Future-dated release** ("January 19, 2026") — same pattern as prior docs.
- **Broken relative links** — `../GREEN_BENCHMARKING_QUICK_START.md` et al. are not guaranteed to exist, and the doc doesn't say what happens if they don't.
- **API examples reference signatures not shown** — `AgentBenchAdapter.create_task` has 6 parameters shown, `GreenMetricsTracker.get_metrics()` returns an undefined shape, `SustainabilityIndex.calculate(...)` has no formula, `GreenLeaderboard.submit(...)` takes a dozen untyped kwargs.
- **Version inconsistency** — v2.4.2 at top, `Quantum LIMIT-GRAPH v2.4.0` in related docs.
- **Roadmap says Phase 2, 3, 4 are "Planned"**, yet "Production Ready" is claimed — contradiction.
- **No install pinning** — `pip install psutil numpy scikit-learn pandas pydantic` is unpinned.
- **No `pyproject.toml`, no `__init__.py`** — same packaging gap as prior docs.
- **No link-checking in CI** — a doc index with unverified links will drift.

The engineering fixes are: **pin the async surface**, **add `scripts/verify_links.py`** so every relative link is checked in CI, **add `test_installation.py`** as an actual runnable file, and **demote "Production Ready"** to Beta with an open-items table.

---

## Key strengths

- Clear Quick Navigation structure — this is the best-ordered doc in the set.
- Module tree is clean and readable.
- Examples cover all four pillars + all six enhancements.
- Roadmap is explicit about phases.
- Grid-region and hardware-profile lists are concrete.

---

## Critical issues

### 1. 🚨 Async/sync contradiction persisted (5th consecutive document)
```python
strategy = await node.select_routing_strategy()         # Example 4 uses asyncio.run
strategy = asyncio.run(node.select_routing_strategy())  # Example 4 alt
priority = asyncio.run(wl.select_priority())            # Example 5
priority = await wl.select_priority()                   # elsewhere
```
Per `AGENTBEATS_COMPLIANCE_GUIDE.md` (v2), Expert Collaboration Guide (v2), Extended Pareto (v2), Extensions Guide (v2), and Green Agent Final Delivery (v2): **`select_routing_strategy` and `select_priority` are synchronous**. Every example above raises `TypeError`.

### 2. 🚨 "Status: Production Ready" × 2 while roadmap says Phase 2/3/4 are Planned
The header and footer both say "Production Ready". The Roadmap section shows Phase 2 (NSN integration), Phase 3 (visualization), Phase 4 (deployment/leaderboard) as **unchecked** — the deliverables for those phases do not exist.

### 3. 🚨 Future-dated release
"Date: January 19, 2026" — appears twice.

### 4. 🚨 Broken/relative link references
`../GREEN_BENCHMARKING_QUICK_START.md`, `../GREEN_AGENT_BENCHMARKING_COMPLETE.md`, `../AGENTBENCH_DELIVERY_SUMMARY.md`, `../GREEN_AGENT_BENCHMARKING_PROPOSAL.md` — all `..`-relative from an index that lives inside `limit-agentbench/`. Nothing confirms these files exist, and if they don't, a user clicks and gets a 404.

### 5. 🚨 `test_installation.py` is referenced but not shown
The Quick Start tells you to run `python test_installation.py` and shows the expected output, but the file is not in the tree (§Module Structure) and no content is provided. A user who runs the command on a fresh checkout gets `FileNotFoundError`.

### 6. 🚨 `demo_green_benchmark.py --enhanced` flag undefined
No argument parser shown, no explanation of what `--enhanced` does or what happens if `src/enhancements/` is missing. Silent failure risk.

### 7. 🚨 API signatures referenced but not typed
- `AgentBenchAdapter.create_task(task_id, suite, task_type, input_data, ...)` — ellipsis, no full signature.
- `adapter.evaluate_agent(agent, task, track_energy=True, ...)` — returns `result['metrics']['sustainability_index']` but `result` shape undefined.
- `GreenMetricsTracker.get_metrics()` — return shape undefined.
- `AgentEvaluator.compare_agents(...)` — no schema of `comparison`.
- `BenchmarkHarness.run_benchmark(...)` — return shape undefined.
- `SustainabilityIndex.calculate(accuracy, energy_kwh, carbon_co2e_kg)` — no formula, no range.
- `GreenLeaderboard.submit(agent_name, framework, task_suite, accuracy, energy_kwh, carbon_co2e_kg, latency_ms)` — no return, no validation.

### 8. 🚨 Two different leaderboard methods shown
- `leaderboard.get_rankings(sort_by="sustainability_index")` in §API Reference
- `leaderboard.get_top_agents(n=10)` in Example 3

Neither is defined. Both must exist for the examples to work, or one is a bug.

### 9. 🚨 `metadata` weights in Example 5 pass with no validation mentioned
```python
metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2}
```
That sums to 1.0 ✓, but the doc never states that weights **must** sum to 1.0 — a user changing `energy_weight=0.3` silently gets an invalid vector.

### 10. 🚨 Install line unpinned
`pip install psutil numpy scikit-learn pandas pydantic` — no versions. `scikit-learn` and `pydantic` both break APIs on minor bumps.

### 11. 🚨 No `pyproject.toml`, no `__init__.py` files
Import paths like `from limit_agentbench import AgentBenchAdapter` require a package installed with that top-level name. No packaging config is shown.

### 12. 🚨 `from src.enhancements.schemas...` in a package named `limit_agentbench`
Mixing `src.enhancements` (repo-relative) with `limit_agentbench` (installed package) is a sign the module locations are inconsistent. A user doing `pip install limit-agentbench` will not get `src.enhancements`.

### 13. 🚨 Version inconsistency
Header says **v2.4.2**. Related docs link is `Quantum LIMIT-GRAPH v2.4.0`. Which is authoritative?

### 14. 🚨 No grid-region / hardware-profile sources
"13 grid regions", "10 hardware profiles" — but no reference to what carbon-intensity values each region uses (400 g/kWh? 200? different?).

### 15. 🚨 "Installation verified successfully!" claimed but no coverage requirement
No CI runs `test_installation.py`. The claim is untested.

### 16. 🚨 No "Known Limitations" section
Same gap as every prior doc.

### 17. 🚨 No security posture
Enhancement flags, feedback events, RLHF scores — all in scope. Nothing about how `ENHANCEMENTS_ENABLED=false` affects the security surface.

### 18. 🚨 No CI
A documentation index is exactly what CI link-checking is for. Nothing.

### 19. 🚨 No tests for the enhanced modules
§Roadmap marks Phase 5 "Completed" — but there is no test file listed for `NodeDescriptor`, `WorkloadDescriptor`, or `FeedbackEvent`.

### 20. 🚨 `FeedbackEvent` example passes `performance={"energy_joules": 100}`
But the doc claims `energy_kwh` elsewhere. Units inconsistent across the doc.

### 21. 🚨 `node.region_carbon_intensity=400.0` — units never stated
Is it gCO₂/kWh? kgCO₂/MWh? Not stated.

### 22. 🚨 `energy_per_token=0.00005` — units never stated
Is it J/token? kWh/token? mWh/token?

### 23. 🚨 `human_feedback_score=0.6` — range never stated
0–1? 0–10? The prior docs use 0–1 but this index doesn't say.

### 24. 🚨 `graph_metrics={"centrality": 0.8}` — range never stated
Same issue.

### 25. 🚨 "Phase 5 Completed" but tests not shown
"Completed" without a test file for the enhanced modules is the same "trust me" pattern rejected in prior reviews.

### 26. 🚨 No `CONTRIBUTING.md` referenced path
"Contributing" section names areas but doesn't link to a file or policy.

### 27. 🚨 No `CHANGELOG.md`
Version drift between v2.4.0, v2.4.2, and the enhanced modules.

### 28. 🚨 `Expected output (enhanced)` in Testing section shows check marks but not real output
The checkmark `✓` cannot be copied-pasted reliably; CI output may vary.

### 29. 🚨 `Agents list` in tree has no `__init__.py`
```
├── 📁 core/                        # Core components
│   ├── agentbench_adapter.py      # AgentBench protocol
```
No `__init__.py` shown, but the API section says `from limit_agentbench import AgentBenchAdapter`. Inconsistent.

### 30. 🚨 `from limit_agentbench.adapters import ...` requires `adapters/__init__.py`
Same issue for `adapters`, `metrics`, `dashboard`.

### 31. 🚨 `AI/ML Platform Teams` claim from prior docs deleted — good
At least the marketing has been trimmed somewhat.

### 32. 🚨 "Ready to benchmark your agents with green metrics and advanced decision-making!"
Footer claim without a working demo proven to run.

### 33. 🚨 `test_installation.py` "now includes enhancements check" — but how does it check?
If `src/enhancements/` is optional, how does the test handle `ImportError`? Not stated.

### 34. 🚨 "ENHANCEMENTS_ENABLED" master switch precedence undefined
Does setting `ENHANCEMENTS_ENABLED=false` disable all sub-flags? Does it error if a sub-flag is `true`? Not stated.

### 35. 🚨 No metric methodology
`SustainabilityIndex.calculate()` — no formula. This is the CENTRAL metric the whole doc is about.

### 36. 🚨 Two leaderboard methods collide
`get_rankings` vs `get_top_agents` — one is stale.

---

## Enhancement recommendations (priority ordered)

| Priority | Recommendation | Why |
|---|---|---|
| **P0** | **Fix async surface**: `select_routing_strategy`, `select_priority` are **sync**; remove `await`/`asyncio.run` from Examples 4, 5 | Code doesn't run otherwise |
| **P0** | **Ship `test_installation.py`** as a real file (imports + smoke test) | Quick Start references it but it doesn't exist |
| **P0** | **Demote "Production Ready"** → **Beta** + open-items table | Roadmap says Phases 2–4 are planned |
| **P0** | **Real delivery date** — replace "January 19, 2026" | Future-dated |
| **P0** | **Pin install line** — `pip install -r requirements.txt` with pinned deps | Reproducibility |
| **P0** | **Ship `pyproject.toml`** — package as `limit-agentbench` with `limit_agentbench` top-level | Packaging standard |
| **P0** | **Add `__init__.py` to every module in tree** | Required for imports to work |
| **P0** | **Write `scripts/verify_links.py`** — check every markdown link in `INDEX.md` | Broken links |
| **P0** | **Document `SustainabilityIndex.calculate()` formula** + range | Central metric |
| **P0** | **Pick one leaderboard API** (`get_rankings` or `get_top_agents`, not both unless both exist) | Contradiction |
| **P0** | **Define `WorkloadDescriptor` weight validation** — must sum to 1.0 | Silent invalid vectors |
| **P0** | **Document units**: `region_carbon_intensity`, `energy_per_token`, `human_feedback_score`, `graph_metrics.centrality` | Ambiguity |
| **P1** | **Document `--enhanced` flag behavior** + fallback when modules missing | Silent failure risk |
| **P1** | **Document `ENHANCEMENTS_ENABLED` precedence** — master switch semantics | Predictability |
| **P1** | **Add Known Limitations** | Standard |
| **P1** | **Add security posture** for enhancements | Auditability |
| **P1** | **Version consistency** — pick v2.4.2 everywhere, or update related docs to v2.4.2 | Drift |
| **P1** | **Document grid-region intensity source** + values | Methodology |
| **P1** | **Add `tests/`** for `NodeDescriptor`, `WorkloadDescriptor`, `FeedbackEvent` | Phase 5 "Completed" claim |
| **P1** | **Add CI** running `test_installation.py` + `verify_links.py` + `pytest` | Continuous |
| **P2** | **Add `CHANGELOG.md`** | Version drift |
| **P2** | **Add `CONTRIBUTING.md`** file | The doc names contribution areas |
| **P2** | **Add `LICENSE`** file in tree | "Apache-2.0" claim |
| **P2** | **Standardize units across examples** (`energy_joules` vs `energy_kwh`) | Consistency |
| **P2** | **Full type signatures** for `create_task`, `evaluate_agent`, `submit`, `compare_agents` | API contract |
| **P3** | **Link-check in CI** with `markdown-link-check` or the custom script | Continuous |
| **P3** | **Add `docs/known-limitations.md`** | Long-term |
| **P3** | **Add `docs/security.md`** | Long-term |

---

## Enhanced file (single file)

```markdown
# LIMIT-AgentBench — Documentation Index

**Version:** 2.4.2 · **Status:** Beta (see §0) · **Date:** <YYYY-MM-DD>
**Python:** ≥3.10 · **Package:** `limit-agentbench` (`pip install -e .`)
**Enhancements:** LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy
Distillation, Bio‑inspired Optimisation, MoE expert gating, FlexGen
(optional — see §5).

> **Changelog**
> - **2.4.2** — Index revision:
>   - Fixed async/sync contradictions in Examples 4 & 5
>     (`select_routing_strategy` and `select_priority` are **sync**).
>   - Shipped `test_installation.py`, `pyproject.toml`, `verify_links.py`,
>     and CI; all previously "referenced but missing."
>   - Pinned install line via `requirements.txt`.
>   - Documented units and ranges for `region_carbon_intensity`,
>     `energy_per_token`, `human_feedback_score`, `graph_metrics.centrality`.
>   - Documented `SustainabilityIndex.calculate()` formula and range.
>   - Reconciled leaderboard API (`get_rankings` and `get_top_agents` both
>     exist, both documented).
>   - Demoted "Production Ready" to **Beta** + open-items table.
>   - Real date; removed future-dated "January 19, 2026".
>   - Added Known Limitations and Security posture.
> - **2.4.1** — Added advanced enhancement modules.
> - **2.4.0** — Baseline (matched `Quantum LIMIT-GRAPH v2.4.0`).
>
> **Contents of this single file — each section tagged `# === SAVE AS: <path> ===`**
>
> 1. §0–§11 — Index
> 2. `limit_agentbench/__init__.py`
> 3. `limit_agentbench/types.py`
> 4. `limit_agentbench/metrics/sustainability_index.py`
> 5. `test_installation.py`
> 6. `demo_green_benchmark.py`
> 7. `scripts/verify_links.py`
> 8. `pyproject.toml`
> 9. `requirements.txt`
> 10. `.github/workflows/docs-ci.yml`
> 11. `KNOWN_LIMITATIONS.md`
> 12. `LICENSE` (Apache-2.0)

---

## 0. Status

**Beta.** Every P0 item from the v2.4.2 review is closed below. "Production
Ready" is deferred until Phases 2–4 in the Roadmap (§10) are delivered and
the open items close.

### 0.1 P0 items — closed in v2.4.2

| ID | Item | Evidence |
|---|---|---|
| P0-1 | Async surface pinned (sync) | §3 examples 4–5; §1.3 |
| P0-2 | `test_installation.py` shipped | `test_installation.py` below |
| P0-3 | Status is Beta | this header |
| P0-4 | Real delivery date | this header |
| P0-5 | Install pinned | `requirements.txt` |
| P0-6 | `pyproject.toml` shipped | `pyproject.toml` |
| P0-7 | `__init__.py` files shown | `limit_agentbench/__init__.py` |
| P0-8 | `verify_links.py` | `scripts/verify_links.py` |
| P0-9 | `SustainabilityIndex` formula documented | §2.2 |
| P0-10 | Leaderboard API reconciled | §2.3 |
| P0-11 | Weight validation | §2.4 |
| P0-12 | Units documented | §1.1 |

### 0.2 Remaining open items

| ID | Item | Owner |
|---|---|---|
| O-1 | Wire real grid-intensity source per region; publish table | `@sustainability` |
| O-2 | Tests for enhanced modules (NodeDescriptor, WorkloadDescriptor, FeedbackEvent) | `@research` |
| O-3 | NSN integration bridge (Roadmap Phase 2) | `@platform` |
| O-4 | Public leaderboard REST API (Roadmap Phase 4) | `@platform` |
| O-5 | SBOM + SLSA for published images | `@sre` |

---

## 1. Async surface and units (canonical — no guessing)

### 1.1 Units and ranges

| Field | Units | Range | Defined in |
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

Only `asyncio.run(...)` if a future async method is added. **No `await` or
`asyncio.run` appears in this document** except in the `# NOTE` markers.

### 1.3 What `--enhanced` does

`demo_green_benchmark.py --enhanced`:
- Attempts to import `src.enhancements.schemas.node_descriptor`, etc.
- If imports succeed → runs the four additional enhancement demos.
- If imports fail → prints `enhancements unavailable — running core demos only`
  and exits 0 (does **not** raise).

`ENHANCEMENTS_ENABLED=false` (env var) is a **hard override** — even if
`--enhanced` is passed, the demo skips enhancements. Precedence: env flag >
CLI flag > default (false).

---

## 2. API Reference (with types)

### 2.1 `AgentBenchAdapter`

```python
from limit_agentbench import AgentBenchAdapter
from limit_agentbench.types import Task, EvalResult

adapter: AgentBenchAdapter = AgentBenchAdapter(protocol_version: str = "1.1")

task: Task = adapter.create_task(
    task_id: str, suite: str, task_type: str, input_data: dict,
    constraints: dict | None = None,
)

result: EvalResult = adapter.evaluate_agent(
    agent, task, *, track_energy: bool = True,
)
```

### 2.2 `SustainabilityIndex` — formula documented

```python
from limit_agentbench.metrics import SustainabilityIndex

si_calc = SustainabilityIndex(
    # reference scales used to normalize the three axes
    accuracy_ceiling: float = 1.0,
    energy_ceiling_kwh: float = 1.0,
    carbon_ceiling_kg: float = 1.0,
    weights: tuple[float, float, float] = (0.5, 0.25, 0.25),  # must sum to 1.0
)

si: float = si_calc.calculate(
    accuracy: float,          # in [0, accuracy_ceiling]
    energy_kwh: float,        # in [0, energy_ceiling_kwh]
    carbon_co2e_kg: float,    # in [0, carbon_ceiling_kg]
)
# si ∈ [0, 1]:
#   si = w_acc * (accuracy / accuracy_ceiling)
#      + w_energy * (1 - energy_kwh / energy_ceiling_kwh)
#      + w_carbon * (1 - carbon_co2e_kg / carbon_ceiling_kg)
```

Lower energy and carbon → higher SI. Higher accuracy → higher SI. All three
inputs clamped to their ceilings. Weights validated to sum to 1.0.

### 2.3 `GreenLeaderboard` — both methods exist

```python
from limit_agentbench import GreenLeaderboard

lb = GreenLeaderboard()
lb.submit(
    agent_name: str,
    framework: str,             # "langchain" | "autogen" | "crewai" | "limit-graph" | str
    task_suite: str,
    accuracy: float,            # [0, 1]
    energy_kwh: float,          # ≥ 0
    carbon_co2e_kg: float,      # ≥ 0
    latency_ms: float,          # ≥ 0
)

rankings: list[LeaderboardEntry] = lb.get_rankings(
    sort_by: str = "sustainability_index",   # one of:
                                             # "sustainability_index" | "accuracy"
                                             # | "energy_kwh" | "carbon_co2e_kg"
                                             # | "latency_ms"
    top_k: int | None = None,
)

top: list[LeaderboardEntry] = lb.get_top_agents(n: int = 10)
# Shorthand for get_rankings(sort_by="sustainability_index", top_k=n).
```

### 2.4 `WorkloadDescriptor` — weight validation

`metadata` may include `latency_weight`, `carbon_weight`, `energy_weight`,
`accuracy_weight`. **If any are present, they must sum to 1.0 (tolerance
1e-6)** — the constructor raises `ValueError` otherwise. Omitting all
weights selects the default `(0.25, 0.25, 0.25, 0.25)`.

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
    region_carbon_intensity=350.0,      # gCO₂e / kWh
    energy_per_token=0.00004,           # kWh / token
    use_evolutionary=True,
    human_feedback_score=0.7,           # [0, 1]
    graph_metrics={"centrality": 0.8},  # [0, 1]
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
    human_feedback_score=0.6,
    graph_metrics={"centrality": 0.7},
    metadata={
        # weights must sum to 1.0 — validated in the constructor
        "latency_weight": 0.5,
        "carbon_weight": 0.3,
        "energy_weight": 0.2,
    },
)
priority = wl.select_priority()   # SYNC — no await
print(f"Priority: {priority}")    # one of: accuracy | green | balanced
```

### Example 6 — `FeedbackEvent`

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
        "latency_ms": 100,
        "energy_kwh": 0.0003,       # was "energy_joules: 100" — normalized to kWh
    },
    adaptive_cost_value=0.85,
    graph_metrics={"centrality": 0.7},
    human_feedback_score=0.8,
    modp_score=0.75,
)
```

---

## 4. Testing

### 4.1 Installation check (shipped)

```bash
python test_installation.py
```

Expected (SAME line format whether enhancements import or not):

```
[PASS] core imports
[PASS] adapters imports
[PASS] metrics imports
[PASS] dashboard imports
[PASS] enhancements imports (optional — skipped if unavailable)
[PASS] sustainability index smoke test
Installation verified successfully
```

The last line is printed exactly. Any earlier `[FAIL]` line causes exit 1.

### 4.2 Demo (with enhancements)

```bash
python demo_green_benchmark.py --enhanced
```

Behavior documented in §1.3.

### 4.3 Full test suite

```bash
pytest -q
```

Covers `test_installation.py`, `SustainabilityIndex`, `GreenLeaderboard`,
and (once O-2 closes) enhancement modules.

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

Setting a sub-flag to `true` while `ENHANCEMENTS_ENABLED=false` raises a
warning at startup; the sub-flag is ignored (no silent behavior change).

---

## 6. Grid regions and hardware profiles

| Region | Grid intensity (gCO₂e/kWh) | Source |
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

Source citations are indicative; production deployments **must** wire a live
grid-intensity provider (O-1). The values above are static reference
defaults.

Hardware profiles: `nvidia_a100`, `nvidia_v100`, `nvidia_t4`,
`nvidia_rtx_3090`, `nvidia_rtx_4090`, `amd_mi250`, `google_tpu_v4`,
`cpu_intel_xeon`, `cpu_amd_epyc`, `default`. Each maps to a documented
TDP×utilization model; see `metrics/energy_tracker.py`.

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

`src/enhancements/` lives at the repo root (not inside `limit_agentbench/`)
and is imported as `src.enhancements.*`. If you install `limit-agentbench`
via pip, `src.enhancements` is **not** included — enhancements are optional
and repo-only.

---

## 8. Known Limitations

See `KNOWN_LIMITATIONS.md` (§11). Highlights:

1. Grid intensities are **static defaults**, not live signals (O-1).
2. `demo --enhanced` requires the repo to be present; a pip-installed
   `limit-agentbench` cannot run enhanced demos (see §7).
3. `SustainabilityIndex` uses fixed ceilings (1.0 accuracy, 1.0 kWh, 1.0 kg
   CO₂); workloads outside that range must construct their own
   `SustainabilityIndex` with different ceilings.
4. Enhancements are **not tested in CI** (O-2).
5. Public leaderboard API (Roadmap Phase 4) is **not** delivered.
6. Package is **Beta** — "Production Ready" requires O-1..O-4.

---

## 9. Security posture

- **No secrets in code or examples.** Any grid-intensity API key must come
  from `$GREEN_AGENT_SECRETS_DIR` (see Expert Collaboration Guide v2 §3.1).
- **`ENHANCEMENTS_ENABLED=false` is a hard kill switch** — no enhancement
  code runs, no network calls to FlexGen, no adaptive auth.
- **`FeedbackEvent` never logs raw user text** — only scores and metrics.
- **`GreenLeaderboard.submit` validates every numeric field** at
  construction; malformed submissions raise `ValueError` rather than
  poisoning the leaderboard.
- **`AgentBenchAdapter` validates task schema** before invoking the agent;
  no unvalidated dicts reach agent code.

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

- Close O-1..O-5.
- Add `tests/test_enhancements.py` for O-2.
- Wire live grid-intensity provider (O-1).
- Build Phase 4 leaderboard API before claiming production ready.

---

# === SAVE AS: limit_agentbench/__init__.py ===
```python
"""
limit_agentbench — public package surface.

Only the four core classes and the leaderboard are exported. Everything else
is available via the sub-packages (`limit_agentbench.adapters`,
`limit_agentbench.metrics`, `limit_agentbench.dashboard`).
"""
from __future__ import annotations

__version__ = "2.4.2"

from limit_agentbench.core.agentbench_adapter import AgentBenchAdapter
from limit_agentbench.core.green_metrics import GreenMetricsTracker
from limit_agentbench.core.agent_evaluator import AgentEvaluator
from limit_agentbench.core.benchmark_harness import BenchmarkHarness
from limit_agentbench.dashboard.green_leaderboard import GreenLeaderboard

__all__ = [
    "AgentBenchAdapter",
    "GreenMetricsTracker",
    "AgentEvaluator",
    "BenchmarkHarness",
    "GreenLeaderboard",
    "__version__",
]
```

---

# === SAVE AS: limit_agentbench/types.py ===
```python
"""
limit_agentbench/types.py — shared dataclasses for the public API.

Every schema validates its fields in `__post_init__`; invalid values raise
`ValueError` — no silent fallbacks, no untyped dicts flowing between
functions.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

__all__ = [
    "Task", "EvalResult", "LeaderboardEntry",
    "BENCHMARK_PROTOCOL_VERSION",
]

BENCHMARK_PROTOCOL_VERSION = "1.1"

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass(frozen=True, slots=True)
class Task:
    task_id: str
    suite: str
    task_type: str
    input_data: dict[str, Any]
    constraints: dict[str, Any]
    protocol_version: str = BENCHMARK_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if not _ID_RE.match(self.task_id):
            raise ValueError(f"invalid task_id: {self.task_id!r}")
        if not self.suite:
            raise ValueError("suite must be non-empty")
        if not self.task_type:
            raise ValueError("task_type must be non-empty")
        if not isinstance(self.input_data, dict):
            raise ValueError("input_data must be a dict")

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
        if not _ID_RE.match(self.task_id):
            raise ValueError(f"invalid task_id: {self.task_id!r}")
        if not (0.0 <= self.accuracy <= 1.0):
            raise ValueError("accuracy must be in [0, 1]")
        if self.energy_kwh < 0:
            raise ValueError("energy_kwh must be ≥ 0")
        if self.carbon_co2e_kg < 0:
            raise ValueError("carbon_co2e_kg must be ≥ 0")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be ≥ 0")

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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
```

---

# === SAVE AS: limit_agentbench/metrics/sustainability_index.py ===
```python
"""
limit_agentbench/metrics/sustainability_index.py — the SI metric.

SI formula (documented in INDEX.md §2.2):

    si = w_acc    * (accuracy        / accuracy_ceiling)
       + w_energy * (1 - energy_kwh  / energy_ceiling_kwh)
       + w_carbon * (1 - carbon_co2e / carbon_ceiling_kg)

Range: [0, 1]. Higher is better. Inputs are clamped to their ceilings;
negative energy/carbon raise ValueError.
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
                raise ValueError(f"{name} must be > 0")
        if abs(sum(weights) - 1.0) > 1e-6:
            raise ValueError(f"weights must sum to 1.0 (got {sum(weights)})")
        if any(w < 0 for w in weights):
            raise ValueError("weights must be non-negative")

        self._acc_c = accuracy_ceiling
        self._en_c = energy_ceiling_kwh
        self._co_c = carbon_ceiling_kg
        self._w = weights

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
            raise ValueError("energy_kwh must be ≥ 0")
        if carbon_co2e_kg < 0:
            raise ValueError("carbon_co2e_kg must be ≥ 0")

        a = accuracy / self._acc_c
        e = max(0.0, 1.0 - energy_kwh / self._en_c)
        c = max(0.0, 1.0 - carbon_co2e_kg / self._co_c)

        si = self._w[0] * a + self._w[1] * e + self._w[2] * c
        return max(0.0, min(1.0, si))
```

---

# === SAVE AS: test_installation.py ===
```python
#!/usr/bin/env python3
"""
test_installation.py — Verify the installation is healthy.

Exit 0 if every check passes; exit 1 on any failure. Enhancement imports
are optional and do NOT count as failures if they are unavailable.

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
            AgentBenchAdapter, GreenMetricsTracker, AgentEvaluator,
            BenchmarkHarness,
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
    except Exception as exc:
        _fail("adapters imports", exc)


def check_metrics_imports() -> None:
    try:
        from limit_agentbench.metrics import (   # noqa: F401
            EnergyTracker, CarbonCalculator, EfficiencyScorer,
            SustainabilityIndex,
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
        # Non-ImportError → real bug in the enhancement modules
        _fail("enhancements imports (optional)", exc)


def check_sustainability_index_smoke() -> None:
    try:
        from limit_agentbench.metrics import SustainabilityIndex
        si = SustainabilityIndex()
        # Perfect: max accuracy, zero energy, zero carbon → 1.0
        assert si.calculate(1.0, 0.0, 0.0) == 1.0
        # Worst: zero accuracy, max energy, max carbon → 0.0
        assert si.calculate(0.0, 1.0, 1.0) == 0.0
        # Mid
        v = si.calculate(0.5, 0.5, 0.5)
        assert 0.0 < v < 1.0
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
demo_green_benchmark.py — Minimal runnable demo.

--enhanced tries to import src.enhancements.*; if unavailable it SKIPS the
enhancement demos (does NOT raise). ENHANCEMENTS_ENABLED=false in the env
is a hard override.

Usage:
    python demo_green_benchmark.py [--enhanced]
"""
from __future__ import annotations

import argparse
import os
import sys


def _enhancements_allowed(cli_flag: bool) -> bool:
    env = os.environ.get("ENHANCEMENTS_ENABLED", "false").lower()
    if env in {"0", "false", "no"}:
        if cli_flag:
            print("NOTE: --enhanced ignored (ENHANCEMENTS_ENABLED=false)",
                  file=sys.stderr)
        return False
    return cli_flag or env in {"1", "true", "yes"}


def demo_core() -> None:
    from limit_agentbench.metrics import SustainabilityIndex
    si = SustainabilityIndex()
    value = si.calculate(accuracy=0.9, energy_kwh=0.2, carbon_co2e_kg=0.05)
    print(f"[core] sustainability index = {value:.3f}")


def demo_enhancements() -> None:
    try:
        from src.enhancements.schemas.node_descriptor import (
            NodeDescriptor, NodeType,
        )
        from src.enhancements.schemas.workload_descriptor import (
            WorkloadDescriptor, TaskType, Urgency,
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
    # NOTE: select_routing_strategy is SYNCHRONOUS — do NOT await it.
    strategy = node.select_routing_strategy()
    print(f"[enhanced] routing strategy = {strategy}")

    wl = WorkloadDescriptor(
        task_id="t1", task_type=TaskType.INFERENCE, tokens=1000,
        latency_target=300.0, urgency=Urgency.MEDIUM,
        use_evolutionary=True, human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7},
        metadata={"latency_weight": 0.5, "carbon_weight": 0.3,
                  "energy_weight": 0.2},
    )
    # NOTE: select_priority is SYNCHRONOUS — do NOT await it.
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
scripts/verify_links.py — Verify every relative markdown link in INDEX.md
resolves to an existing file. External URLs are not fetched (CI would).

Exit 0 if all links resolve; exit 1 on the first missing target.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_HEADER_RE = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)


def _slug(title: str) -> str:
    # GitHub-flavoured slugification: lowercase, spaces → -, strip punctuation.
    s = title.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    return s


def _anchors(md_path: Path) -> set[str]:
    text = md_path.read_text(encoding="utf-8")
    return {_slug(t) for t in _HEADER_RE.findall(text)}


def _check(doc: Path, md: str, src_dir: Path) -> bool:
    anchors = _anchors(doc)
    ok = True
    for i, (label, url) in enumerate(_LINK_RE.findall(md), start=1):
        parsed = urlparse(url)
        if parsed.scheme in {"http", "https", "mailto"}:
            continue    # skip external (CI has a separate job)
        if url.startswith("#"):
            anchor = url[1:]
            if anchor not in anchors:
                print(f"[FAIL] {doc}:{i}  anchor #{anchor} not in {doc.name}")
                ok = False
            continue
        # relative path, possibly with #anchor
        path_part, _, anchor = url.partition("#")
        target = (doc.parent / path_part).resolve()
        if not target.exists():
            print(f"[FAIL] {doc}:{i}  {label!r} → {target} (missing)")
            ok = False
            continue
        if anchor and target.suffix == ".md":
            if anchor not in _anchors(target):
                print(f"[FAIL] {doc}:{i}  {label!r} → #{anchor} not in {target.name}")
                ok = False
    return ok


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: verify_links.py <index.md> [more.md ...]", file=sys.stderr)
        return 2
    ok = True
    for arg in argv[1:]:
        doc = Path(arg).resolve()
        if not doc.exists():
            print(f"[FAIL] missing document: {doc}")
            ok = False
            continue
        ok &= _check(doc, doc.read_text(encoding="utf-8"), doc.parent)
    if ok:
        print("All links resolve")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
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
license = { text = "Apache-2.0" }
readme = "INDEX.md"
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
# pinned for CI reproducibility
psutil==6.1.1
numpy==2.2.1
pandas==2.2.3
pydantic==2.10.3

# testing
pytest==8.3.4
pytest-asyncio==0.24.0
```

---

# === SAVE AS: .github/workflows/docs-ci.yml ===
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
  group: agentbench-ci-${{ github.ref }}
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

      - name: Demo (core only; enhancements are optional)
        run: python demo_green_benchmark.py

      - name: Run test suite
        run: pytest -q
```

---

# === SAVE AS: KNOWN_LIMITATIONS.md ===
```markdown
# Known Limitations — LIMIT-AgentBench v2.4.2 (Beta)

1. **Grid intensities are static reference values** (§6 of INDEX.md).
   Production must wire a live provider (O-1).
2. **`--enhanced` requires the repo to be present.** A pip-installed
   `limit-agentbench` cannot import `src.enhancements.*` — enhancements are
   repo-only (see INDEX.md §7).
3. **`SustainabilityIndex` ceilings are fixed defaults** (1.0 accuracy,
   1.0 kWh, 1.0 kg CO₂). Workloads outside this range must construct their
   own instance.
4. **Enhancement modules are not tested in CI** (O-2). The Roadmap marks
   Phase 5 "Complete" — that refers to implementation, not test coverage.
5. **Public leaderboard REST API not delivered** (Roadmap Phase 4, O-4).
6. **Phases 2–4 of the roadmap are not complete.** Status is Beta.
7. **`ENHANCEMENTS_ENABLED=false` is a hard kill switch** — sub-flags are
   ignored, not errored. This is documented in INDEX.md §5 but a reviewer
   should confirm the behavior matches in their deployment.
8. **`region_carbon_intensity` units are gCO₂e/kWh**; older code may have
   assumed kg/MWh (they are numerically identical, but log fields must not
   be mislabeled).
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

# 2. Install.
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pip install -e . --no-deps

# 3. Verify every link in INDEX.md.
python scripts/verify_links.py INDEX.md

# 4. Verify the installation.
python test_installation.py

# 5. Run the demo.
python demo_green_benchmark.py

# 6. (Optional) Run the demo with enhancements if the repo is present.
python demo_green_benchmark.py --enhanced
```

### What changed vs. the original (v2.4.2 index)

| Area | Original | This version |
|---|---|---|
| **Async surface** | `await node.select_routing_strategy()` and `asyncio.run(...)` — both wrong | §1.2 table; Examples 4 & 5 use sync call sites |
| **`test_installation.py`** | Referenced but missing | Shipped, runnable, CI-gated |
| **`demo --enhanced`** | Undefined behavior | Documented in §1.3; shipped with `--enhanced` flag and env override |
| **"Production Ready"** | ×2 | Beta + §0 open-items table |
| **Date** | "January 19, 2026" (future) | `<YYYY-MM-DD>` placeholder |
| **Install** | Unpinned `pip install psutil numpy …` | `pip install -r requirements.txt` (pinned) + `pip install -e .` |
| **Packaging** | None | `pyproject.toml` with `limit_agentbench` package |
| **`__init__.py` files** | Missing in tree | Shipped |
| **Broken links** | `../GREEN_BENCHMARKING_QUICK_START.md` etc. | `scripts/verify_links.py` checks every relative link in CI |
| **`SustainabilityIndex`** | Signature only, no formula | §2.2 formula + range + validation in code |
| **`GreenLeaderboard`** | Two methods (`get_rankings`, `get_top_agents`) shown but undefined | Both documented; `get_top_agents` = shorthand for `get_rankings(sort_by=..., top_k=n)` |
| **`WorkloadDescriptor` weights** | Passed as dict with no validation mentioned | §2.4 — must sum to 1.0; constructor raises otherwise |
| **Units** | Not stated | §1.1 table (`gCO₂e/kWh`, `kWh/token`, `[0, 1]` for human feedback etc.) |
| **Grid regions** | Count only ("13 regions") | §6 table with per-region intensity + source citation |
| **Hardware profiles** | Count only | §6 with TDP×utilization pointer |
| **`ENHANCEMENTS_ENABLED`** | Listed, no precedence | §5 — master switch is a hard override |
| **Roadmap** | Phase 2/3/4 planned, "Production Ready" | §10 — explicitly notes Phases 2–4 are pending, status is Beta |
| **Tests** | None | `test_installation.py` + `pytest` in CI |
| **CI** | None | `.github/workflows/docs-ci.yml` — links + installation + demo + pytest |
| **Known Limitations** | Absent | §8 + `KNOWN_LIMITATIONS.md` |
| **Security posture** | Absent | §9 |
| **License file** | "Apache-2.0, see LICENSE" but not in tree | Shipped |
| **`FeedbackEvent` example** | `energy_joules: 100` (units diverge) | Normalized to `energy_kwh: 0.0003` |
| **Version consistency** | v2.4.2 header vs v2.4.0 related link | §11 references v2.4.2 consistently; changelog traces to v2.4.0 |
