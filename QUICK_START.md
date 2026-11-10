
# Quick Start Guide: Meta-Cognitive Green Agent

| Field | Value |
|---|---|
| **Version** | 1.2.0 |
| **Last Updated** | 2026-11-10 |
| **Status** | Production Ready |
| **Audience** | New users, operators, integrators |
| **Companion docs** | `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0) · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `.cursorrules` |

---

## Table of Contents

1. [Installation](#installation)
2. [Basic Usage](#basic-usage)
3. [Configuration](#configuration)
4. [Understanding the Output](#understanding-the-output)
5. [Key Features](#key-features)
6. [Advanced Usage](#advanced-usage)
7. [Metric Reference](#metric-reference)
8. [Severity Taxonomy](#severity-taxonomy)
9. [Security](#security)
10. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
11. [Reproduce with Docker](#reproduce-with-docker)
12. [Migration Guide (v1.0 → v1.2)](#migration-guide-v10--v12)
13. [What's Not in Scope](#whats-not-in-scope)
14. [Troubleshooting](#troubleshooting)
15. [Next Steps](#next-steps)
16. [Changelog](#changelog)
17. [Resources](#resources)
18. [Support](#support)

---

## Installation

```bash
# Clone the repository
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent

# Install core dependencies
pip install -r requirements.txt

# Or, using the src layout (recommended for development):
pip install -e .

# (Optional) Install dependencies for advanced enhancements
pip install scikit-learn pandas pydantic pydantic-settings cryptography pyjwt prometheus-client tenacity aiofiles networkx
```

> **Note:** `pip install -e .` uses `pyproject.toml` to register the `src/` layout and
> makes `src.enhancements.*`, `src.symbolic.*`, `src.dashboard.*`, `src.policy.*`
> importable without `PYTHONPATH` hacks. See `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0)
> for details. `MANIFEST.sha256` can be verified with `scripts/verify.sh`.

---

## Basic Usage

### 1. Run the Meta-Cognitive Demo

The easiest way to see the meta-cognitive architecture in action:

```bash
python demo_meta_cognitive.py
```

This will:
- Simulate 15 execution steps
- Trigger reflection checkpoints every 5 steps
- Generate self-explanations and decisions
- Compare with historical runs
- Identify patterns
- Generate meta-policies
- Create an interactive dashboard

**Output Files**:
- `demo_dashboard.html` — Interactive visualization
- `demo_metrics_history.json` — Detailed metrics timeline
- `demo_reflections.json` — All reflection checkpoints
- `demo_reasoning_insights.json` — Long-context analysis

### 2. Run the Symbolic Oversight Demo

To see the neuro-symbolic 4th layer (rule evaluation + violation traces):

```bash
python demo_symbolic_oversight.py
```

This produces:
- `symbolic_violations.json` — Detailed violation traces
- `symbolic_violation_report.json` — Summary report
- `dashboard.html` — Dashboard with symbolic overlays

### 3. Run Tests

```bash
# Meta-cognitive suite
python test_meta_cognitive.py

# Symbolic oversight suite — 19 tests (13 core + 6 enhancement)
python test_symbolic_oversight.py

# Full suite via pytest
python -m pytest tests/unit tests/integration -q
```

### 4. Run with Custom Configuration

```bash
python run_agent.py \
  --config example_config.json \
  --policy green_policy.yaml \
  --output results.json \
  --dashboard dashboard.html
```

**Optionally enable advanced enhancements:**

```bash
python run_agent.py --config example_config.json --policy green_policy.yaml --enhancements
```

**To baseline (disable enhancements):**

```bash
ENHANCEMENTS_ENABLED=false python run_agent.py --config example_config.json --policy green_policy.yaml
# or: python run_agent.py ... --no-enhancements
```

**To enable symbolic oversight**, ship a `symbolic_policy.yaml` alongside your policy:

```bash
python run_agent.py \
  --config example_config.json \
  --policy green_policy.yaml \
  --policy symbolic_policy.yaml \
  --enhancements
```

> `--enhancements` is **sugar** for `ENHANCEMENTS_ENABLED=true`. Per-module flags
> (e.g. `FLEXGEN_ENABLED=true`) still apply. Symbolic oversight is always-on when
> `symbolic_policy.yaml` exists.

---

## Configuration

### Policy Configuration (`green_policy.yaml`)

Key meta-cognitive settings:

```yaml
meta_cognitive:
  # Reflection frequency (every N steps)
  reflection_frequency: 5

  # Self-adjustment thresholds (percentage of budget)
  self_adjustment_thresholds:
    energy_threshold_pct: 80
    carbon_threshold_pct: 80
    latency_threshold_pct: 80
    memory_threshold_pct: 80

  # Enable adaptive policy adjustments
  adaptive_policy_enabled: true

  # Long-context reasoning
  long_context:
    enabled: true
    history_window: 10  # Consider last 10 runs
```

### Execution Configuration (`config.json`)

```json
{
  "framework": "langchain",
  "grid_intensity": 385.0,
  "pue_factor": 1.2,
  "queries": [
    {
      "id": "query-1",
      "task": "Your task description"
    }
  ],
  "enhancements": {
    "enabled": true,
    "limit_graph": {
      "enabled": true,
      "graph_metrics": { "centrality": 0.7, "connectivity": 0.6 }
    },
    "modp": {
      "enabled": true,
      "objective_weights": [0.4, 0.3, 0.2, 0.1]
    },
    "rlhf": {
      "enabled": true,
      "human_feedback_score": 0.6
    },
    "distillation": {
      "enabled": true,
      "use_moe_gating": true
    },
    "evolutionary": {
      "enabled": true,
      "population_size": 20,
      "mutation_rate": 0.1
    },
    "moe": {
      "enabled": true,
      "n_experts": 4
    },
    "flexgen": {
      "enabled": false,
      "model_name": "facebook/opt-6.7b",
      "default_precision": "fp16",
      "delegation_policy": "adaptive"
    },
    "zero_trust": {
      "enabled": false
    },
    "privacy": {
      "enabled": false
    },
    "training": {
      "enabled": false,
      "checkpoint_dir": "checkpoints"
    }
  }
}
```

> **Config reconciliation (v1.2):**
> - `enhancements.enabled` ↔ env `ENHANCEMENTS_ENABLED` (env wins on conflict).
> - `modp.objective_weights` = **`[accuracy, energy, latency, carbon]`**.
> - `evolutionary` was called `bio_inspired` in v1.0/1.1 (alias retained).
> - `moe` was called `moe_expert` in v1.0/1.1 (alias retained).
> - `zero_trust`, `privacy`, `training` are new in this guide (matching Dockerfile v2.0).

### Symbolic policy (`symbolic_policy.yaml`)

Symbolic oversight is enabled automatically when the file is present. Minimum viable example:

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon footprint combined with high latency"

  - id: "ENH-MODP-001"
    name: "Low MODP Score Alert"
    category: "enhanced"
    priority: "medium"          # NOT "warning" — warning is a severity
    condition: "modp_score < 0.4"
    action: "trigger_policy_review"
    explanation: "Overall multi-objective performance is poor"
```

---

## Understanding the Output

### Reflection Checkpoint Example

```
🤔 REFLECTION CHECKPOINT
------------------------------------------------------------
💭 Self-Explanation:
   ⚡ I'm approaching budget limits with 1 warning(s).
   Energy usage is high at 82.3% of budget.
   I'm trading speed for energy efficiency.

🎯 Decision: reduce_tool_calls
📈 Confidence: 0.75
⚡ Warnings: ['Energy high: 82.3%']
🔍 Symbolic violations: 1 (ENH-MODP-001, severity=high)
```

### Dashboard Metrics

The HTML dashboard shows:
- **Leaderboard**: Rankings by efficiency, interpretability, sustainability
- **Agent Comparisons**: Side-by-side with reasoning paths
- **Pareto Positions**: Optimal vs. dominated agents
- **Insights**: Patterns and recommendations
- **Symbolic Overlay**: Violation timeline + severity heatmap + enhancement metrics

### Artifacts Generated (11 total)

| # | File | Layer |
|---|---|---|
| 1 | `results.json` | Complete execution results (metrics per step, reflections, insights, summary) |
| 2 | `dashboard.html` | Interactive visualization (leaderboard, reasoning paths, comparative insights) |
| 3 | `metrics_history.json` | Detailed timeline (per-step snapshots, cumulative metrics) |
| 4 | `reflections.json` | All reflections (self-explanations, decisions, confidence, budget status, symbolic violations) |
| 5 | `reasoning_insights.json` | Long-context analysis (comparisons, patterns, adaptations) |
| 6 | `pareto_analysis.json` | Multi-objective evaluation |
| 7 | `run_memory.json` | Persistent history (all runs, meta-policies) |
| 8 | `dashboard_data.json` | Dashboard data export |
| 9 | `symbolic_violations.json` | **Symbolic layer — detailed traces** |
| 10 | `symbolic_violation_report.json` | **Symbolic layer — summary report** |
| 11 | Enhanced fields in `results.json` + `reflections.json` | Advanced enhancements (`modp_score`, `graph_centrality`, `human_feedback_score`, `moe_gate_stddev`, `evolutionary_best_fitness`, `flexgen_energy_kwh`) |

---

## Key Features

### 1. Real-Time Monitoring

Metrics are collected continuously and accessible mid-execution:

```python
from src.monitoring.metrics_collector import MetricsCollector

collector = MetricsCollector()
collector.start_step()
# ... do work ...
current_metrics = collector.get_current_metrics()
```

### 2. Reflection Checkpoints

Agent pauses periodically to self-reflect:

```python
from src.reflection.reflection_engine import ReflectionEngine

engine = ReflectionEngine(reflection_frequency=5)
if engine.should_reflect(step):
    reflection = engine.generate_reflection(step, metrics, timestamp)
    print(reflection.self_explanation)
```

### 3. Long-Context Reasoning

Compare current run with historical data:

```python
from src.reflection.long_context_reasoner import LongContextReasoner

reasoner = LongContextReasoner()
insights = reasoner.compare_with_past_runs(current_metrics)
for insight in insights:
    print(f"{insight.description} (confidence: {insight.confidence})")
```

### 4. Adaptive Policy

Policies adjust dynamically based on reflections:

```python
from src.policy.policy_engine import PolicyEngine

policy = PolicyEngine(policy_file="green_policy.yaml")
if policy.should_self_adjust(metrics):
    adjustment = policy.apply_adaptive_adjustment(decision)
```

### 5. Multi-Layer Feedback

Combines objective metrics, subjective reasoning, **symbolic violations**, and **advanced enhancements**:

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()

# v1.2 canonical API — 4 layers
multi_feedback = feedback.generate_multi_layer_feedback(
    pareto_analysis=pareto_position,       # 1. objective
    reflections=agent_reflections,         # 2. subjective
    metrics=metrics,
    symbolic_violations=violations,        # 3. symbolic
    enhanced_metrics=enhanced_metrics,     # 4. advanced enhancements (optional)
)
print(multi_feedback['synthesis']['synthesis_text'])

# Deprecated alias (removed in v3.0) — do not use in new code
# feedback.generate_dual_layer_feedback(...)
```

### 6. Neuro-Symbolic Oversight

Rule-based violation detection feeding the 3rd feedback layer:

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
violations = engine.evaluate_rules(metrics, step=step, domain=None)

visualizer = SymbolicVisualizer()
visualizer.add_violations([v.to_dict() for v in violations])
html = visualizer.generate_dashboard_section()
```

> Symbolic oversight is always-on when `symbolic_policy.yaml` exists. Full docs:
> `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0).

---

## Advanced Usage

### Custom Reflection Frequency

Adjust how often the agent reflects:

```yaml
meta_cognitive:
  reflection_frequency: 3  # Reflect every 3 steps
```

### Custom Thresholds

Set when self-adjustment triggers:

```yaml
meta_cognitive:
  self_adjustment_thresholds:
    energy_threshold_pct: 70  # More aggressive
    carbon_threshold_pct: 90  # More lenient
```

### Disable Adaptive Policy (Baseline)

For baseline comparison:

```yaml
meta_cognitive:
  adaptive_policy_enabled: false
```

Disable enhancements entirely:

```bash
ENHANCEMENTS_ENABLED=false python run_agent.py --config example_config.json
```

### Historical Window Size

Control how many past runs to consider:

```yaml
meta_cognitive:
  long_context:
    history_window: 20  # Consider last 20 runs
```

### Integrating LIMIT Graph, MODP, RLHF, Distillation, MoE, Evolutionary, FlexGen

The enhancements folder (`src/enhancements`) provides additional modules that can be enabled to improve decision-making and sustainability. When enabled, the agent can:

- **Select routing strategies** using a learned distillation policy with MoE gating.
- **Choose task priority** based on MODP (Multi-Objective Decision Process) weights.
- **Incorporate human feedback** (RLHF) into decisions.
- **Use LIMIT Graph metrics** (centrality, connectivity) for context awareness.
- **Evolve parameters** via bio-inspired optimisation (evolutionary).
- **Delegate LLM inference** to FlexGen with adaptive precision.
- **Enforce zero-trust actions** (e.g. `block_request`) via `ZERO_TRUST_ENABLED`.
- **Track privacy budget** via `DIFFERENTIAL_PRIVACY_ENABLED`.
- **Track model lifecycle** via `TRAINING_ENABLED`.

#### Enabling Enhancements

Set `ENHANCEMENTS_ENABLED=true` in your environment or include the `enhancements` block in `config.json`. Env vars win over YAML on conflict. The agent will automatically initialize the advanced modules.

#### Example: Using NodeDescriptor and WorkloadDescriptor

```python
import asyncio
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency

async def main():
    node = NodeDescriptor(
        id="node1",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=350.0,
        energy_per_token=0.00004,
        use_evolutionary=True,
        human_feedback_score=0.7,
        graph_metrics={"centrality": 0.8, "connectivity": 0.6},
    )
    strategy = await node.select_routing_strategy()
    print(f"Selected routing strategy: {strategy}")

    workload = WorkloadDescriptor(
        task_id="task1",
        task_type=TaskType.INFERENCE,
        tokens=1000,
        latency_target=300.0,
        urgency=Urgency.MEDIUM,
        use_evolutionary=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7},
        metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2},
    )
    priority = await workload.select_priority()
    print(f"Selected priority: {priority}")

asyncio.run(main())
```

#### FlexGen Integration

If FlexGen is available and enabled, the agent can delegate LLM tasks to FlexGen for high-throughput inference. Set `enhancements.flexgen.enabled` to `true` and adjust `delegation_policy`. The decision to delegate is made by the distillation policy based on context (carbon intensity, task complexity, etc.).

```json
{
  "enhancements": {
    "flexgen": {
      "enabled": true,
      "model_name": "facebook/opt-6.7b",
      "batch_size": 16,
      "default_precision": "fp16",
      "delegation_policy": "adaptive"
    }
  }
}
```

> **Unit note (v1.2):** the FlexGen energy metric is **`flexgen_energy_kwh`** (kWh).
> The earlier `flexgen_energy_joules` name is **deprecated**.

---

## Metric Reference

All variables usable in symbolic rules or reflection thresholds. Units are canonical
— the engine does **not** silently convert.

### Core metrics

| Variable | Unit | Range |
|---|---|---|
| `energy` | Wh | ≥ 0 |
| `carbon` | g | ≥ 0 |
| `latency` | ms | ≥ 0 |
| `memory` | MB | ≥ 0 |
| `tool_calls` | count | ≥ 0 |
| `cpu_percent` | % | 0–100 |

### Context metrics

| Variable | Type | Values |
|---|---|---|
| `query_type` | enum | `research`, `production`, `debug`, `batch` |
| `environment` | enum | `dev`, `staging`, `production` |

### Enhancement metrics

| Variable | Unit | Range | Enable flag |
|---|---|---|---|
| `modp_score` | — | 0–1, higher better | `MODP_ENABLED` |
| `graph_centrality` | — | 0–1 | `LIMIT_GRAPH_ENABLED` |
| `graph_connectivity` | — | 0–1 | `LIMIT_GRAPH_ENABLED` |
| `human_feedback_score` | — | 0–1, higher better | `RLHF_ENABLED` |
| `distillation_update_rate` | ΔL/step | ≥ 0 | `DISTILLATION_ENABLED` |
| `moe_gate_stddev` | — | 0–1 | `MOE_ENABLED` |
| `evolutionary_best_fitness` | — | 0–1, higher better | `EVOLUTIONARY_ENABLED` |
| `flexgen_energy_kwh` | kWh | ≥ 0 | `FLEXGEN_ENABLED` |

### Derived / temporal metrics

| Variable | Meaning |
|---|---|
| `fitness_delta_1h` | `evolutionary_best_fitness` now − 1h ago |
| `carbon_trend` | linear slope of `carbon` |
| `energy_variance` | variance of `energy` over last N steps |

---

## Severity Taxonomy

`priority` maps to a numeric multiplier; severity is derived from the score:

| Severity | Score range | Typical trigger |
|---|---|---|
| `critical` | ≥ 1.5 × threshold | hard budget exceeded |
| `high` | 1.2 – 1.5 × threshold | near-budget |
| `medium` | 1.0 – 1.2 × threshold | at threshold |
| `low` | < 1.0 × threshold | soft warning |

> **Do not use `warning` as a `priority` value.** `warning` is a severity only.

---

## Security

| Concern | Guarantee |
|---|---|
| Symbolic rule evaluation | **Whitelist parser** — no `eval`/`exec`, no attribute access, no function calls |
| Allowed tokens | identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| YAML parsing | `yaml.safe_load` only |
| `block_request` | Requires `ZERO_TRUST_ENABLED=true`; otherwise logged and ignored |
| Metric trust | Metrics may come from an untrusted executor; symbolic rules + numeric budgets are the mitigation |

Full contract: Symbolic README v1.2 §Security.

---

## Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available (see `.cursorrules`):

```python
ctx_read("src/enhancements/", mode="digest")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")
ctx_read("src/enhancements/core/safety.py", mode="safety")
ctx_read("src/enhancements/core/privacy.py", mode="privacy")
```

Token-budget guidance and the full mode taxonomy live in `.cursorrules`.

---

## Reproduce with Docker

```bash
# Full image (all enhancements on)
docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .

# Lean image (minimal footprint)
docker build \
  --build-arg ENHANCEMENTS_ENABLED=false \
  --build-arg MODP_ENABLED=false \
  --build-arg RLHF_ENABLED=false \
  -t green-agent:lean .

# Run demo
docker run --rm -v "$PWD:/out" green-agent:full python demo_meta_cognitive.py

# Run with meta-cognitive + symbolic + enhancements
docker run --rm -p 8000:8000 \
  -v "$PWD:/out" \
  -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
  green-agent:full \
  python run_agent.py --config example_config.json --policy green_policy.yaml
```

---

## Migration Guide (v1.0 → v1.2)

| Before (v1.0) | After (v1.2) | Notes |
|---|---|---|
| "Dual-Layer" / "Triple-Layer" | "Multi-Layer" (4 layers) | Terminology only |
| `generate_dual_layer_feedback()` | `generate_multi_layer_feedback()` | Deprecated alias kept; removed in v3.0 |
| `moe_expert.enabled` | `moe.enabled` / `MOE_ENABLED` | Flag rename |
| `bio_inspired.enabled` | `evolutionary.enabled` / `EVOLUTIONARY_ENABLED` | Flag rename |
| `flexgen` at JSON top-level | `enhancements.flexgen` | Config location |
| `flexgen_energy_joules` | `flexgen_energy_kwh` | Unit fix |
| `priority: "warning"` | `priority: "medium"` (or a severity field) | `warning` is a severity |
| `use_enhancements=False` | `ENHANCEMENTS_ENABLED=false` (env) | Deprecated alias kept |
| 6 output artifacts | 11 output artifacts | Added symbolic + dashboard-data |

---

## What's Not in Scope

- **Z3 / SMT solving** — whitelist rule evaluator, not a theorem prover.
- **Temporal logic operators** — use derived metrics (`fitness_delta_1h`, `carbon_trend`).
- **Arbitrary Python in rule conditions** — by design (see §Security).
- **Cross-agent oversight** — planned.

---

## Troubleshooting

### Issue: No reflections generated

**Solution**: Check reflection frequency. If steps < frequency, no reflections occur.

```yaml
meta_cognitive:
  reflection_frequency: 5  # Ensure steps >= 5
```

### Issue: Memory file not found

**Solution**: The run creates the memory file automatically on first execution.

### Issue: Dashboard not showing data

**Solution**: Ensure at least one complete run has finished.

### Issue: Enhancements not activating

**Solution**: Verify:
- The `src/enhancements` folder exists.
- Required dependencies installed (see Installation).
- `enhancements.enabled` is `true` in config **or** `ENHANCEMENTS_ENABLED=true` env var set (env wins).
- Per-module flag set (e.g. `MODP_ENABLED=true`).
- The modules are importable: `python -c "from src.enhancements.schemas.node_descriptor import NodeDescriptor"`.

### Issue: Symbolic rules not triggering

**Solution**: Check units against [Metric Reference](#metric-reference) — carbon is **grams**, latency is **ms**, `flexgen_energy_kwh` is **kWh**. Verify `symbolic_policy.yaml` exists and uses valid priorities (`critical`/`high`/`medium`/`low`, not `warning`).

### Issue: `src.*` imports fail

**Solution**: Install the package editable:

```bash
pip install -e .
```

Or set `PYTHONPATH`:

```bash
export PYTHONPATH="$PWD/src:$PYTHONPATH"
```

---

## Next Steps

1. **Run Baseline**: Execute with `adaptive_policy_enabled: false` and `ENHANCEMENTS_ENABLED=false`.
2. **Run Enhanced**: Execute with all features enabled (`--enhancements` + `symbolic_policy.yaml`).
3. **Compare**: Use `pareto_analysis.json` to compare performance.
4. **Iterate**: Adjust thresholds and frequencies based on results.
5. **Enable Advanced Modules**: Turn on LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen for improved sustainability and decision quality.
6. **Enable Symbolic Oversight**: Author `symbolic_policy.yaml` and verify `symbolic_violations.json` appears.
7. **Review with lean-ctx**: `ctx_read("src/enhancements/", mode="digest")` for a cheap state snapshot.

---

## Changelog

### 1.2.0 (2026-04-10)

**Added**
- Metadata block + TOC
- Symbolic oversight: demo, tests, config block, feature §6
- 11 output artifacts (was 6) — added `dashboard_data.json`, `symbolic_violations.json`, `symbolic_violation_report.json`, enhanced-fields note
- `zero_trust`, `privacy`, `training` config blocks
- §Metric Reference, §Severity Taxonomy, §Security
- §lean-ctx inspection, §Docker reproduction
- §Migration Guide, §What's Not in Scope, §Changelog
- `--no-enhancements` flag documentation
- Companion-doc cross-links in header
- `pip install -e .` install path
- `test_symbolic_oversight.py` (19 tests)

**Changed**
- "Dual-Layer" → **"Multi-Layer"** (4 layers)
- `generate_dual_layer_feedback` → `generate_multi_layer_feedback` in code sample
- `moe_expert` → `moe`; `bio_inspired` → `evolutionary`
- `flexgen` JSON location → `enhancements.flexgen`
- `objective_weights` labeled `[accuracy, energy, latency, carbon]`
- `flexgen_energy_joules` → `flexgen_energy_kwh`
- `priority: "warning"` → `"medium"` (with severity clarification)
- `META_COGNITIVE_ARCHITECTURE.md` → `Meta-Cognitive Architecture for Green Agent.md`
- Personal email → GitHub Issues link

**Fixed**
- `use_enhancements` removed as canonical (env `ENHANCEMENTS_ENABLED` now canonical)
- Dangling `src/enhancements/README.md` reference removed
- File name drift in §Resources

**Deprecated**
- `generate_dual_layer_feedback()` → `generate_multi_layer_feedback()`
- `moe_expert` / `bio_inspired` config keys → `moe` / `evolutionary`
- `flexgen_energy_joules` → `flexgen_energy_kwh`

### 1.0.0

- Initial quick start.

---

## Resources

- **Architecture**: [`Meta-Cognitive Architecture for Green Agent.md`](Meta-Cognitive%20Architecture%20for%20Green%20Agent.md) (v1.2.0)
- **Symbolic oversight**: [`SYMBOLIC_OVERSIGHT_README.md`](SYMBOLIC_OVERSIGHT_README.md) (v1.2.0)
- **Symbolic deep-dive**: `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`
- **Symbolic completion summary**: `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0)
- **Meta-cognitive summary**: `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0)
- **Repository layout**: `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0)
- **lean-ctx read modes**: `.cursorrules`
- **Repository**: https://github.com/NurcholishAdam/Green_Agent
- **AgentBeats Platform**: https://agentbeats.ai

---

## Support

For issues or questions:

- GitHub Issues: https://github.com/NurcholishAdam/Green_Agent/issues
- Maintainers: open an issue and tag `@NurcholishAdam`

> Personal email has been removed from shipped documentation. Use the GitHub
> issue tracker for all questions and bug reports.

**Last Updated**: 2026-11-10  
**Version**: 1.2.0  
**Test Coverage**: 19 symbolic tests + meta-cognitive suite  
**Breaking Changes**: None (all v1.0 config keys accepted as aliases)
```
