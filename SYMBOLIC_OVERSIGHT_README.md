
# Neuro-Symbolic Oversight for Green Agent

**Version:** 1.2.0  
**Last Updated:** 2026-04-10  
**Status:** Production Ready  
**Audience:** Operators, integrators, rule authors, and enhancement maintainers

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Requirements](#requirements)
3. [Architecture Overview](#architecture-overview)
4. [Key Features](#key-features)
5. [Metric Reference](#metric-reference)
6. [Rule Categories](#rule-categories)
7. [Rule Action Vocabulary](#rule-action-vocabulary)
8. [Severity Taxonomy](#severity-taxonomy)
9. [Customization](#customization)
10. [Rule Precedence & Lifecycle](#rule-precedence--lifecycle)
11. [Deduplication & Rate Limiting](#deduplication--rate-limiting)
12. [Security](#security)
13. [Integration with Advanced Enhancements](#integration-with-advanced-enhancements)
14. [API Reference](#api-reference)
15. [Testing](#testing)
16. [Performance](#performance)
17. [Integration with Green_Agent](#integration-with-green_agent)
18. [Reproduce with Docker](#reproduce-with-docker)
19. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
20. [Troubleshooting](#troubleshooting)
21. [Examples](#examples)
22. [FAQ](#faq)
23. [Migration Guide](#migration-guide)
24. [Changelog](#changelog)
25. [What's Not in Scope](#whats-not-in-scope)
26. [Contributing](#contributing)
27. [License](#license)
28. [References](#references)
29. [Support](#support)

---

## Quick Start

### Installation

No additional dependencies required beyond the base Green Agent installation.  
*(Optional)* To enable advanced enhancements (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen), ensure the `src/enhancements` modules are available and dependencies listed in the main `requirements.txt` are installed.

### Basic Usage

1. **Run the demo:**
```bash
python demo_symbolic_oversight.py
```

2. **Run with symbolic oversight:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml
```

3. **Run with advanced enhancements:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml --enhancements
```
   > `--enhancements` is sugar for `ENHANCEMENTS_ENABLED=true`. Per-module flags
   > (e.g. `MODP_ENABLED`, `FLEXGEN_ENABLED`) still apply. See
   > [Configuration Matrix](#integration-with-advanced-enhancements).

4. **Check results:**
- `symbolic_violations.json` - Detailed violation traces
- `dashboard.html` - Visual dashboard with violations
- `symbolic_violation_report.json` - Summary report

### Example Output

> Captured from `python demo_symbolic_oversight.py` on a 4 vCPU / 16 GB host,
> Python 3.11.4, 15 default rules loaded.

```
🌱 Green Agent - Meta-Cognitive Architecture v2.0
============================================================
📋 Policy loaded: {'max_energy_per_task_wh': 5.0, ...}
🔍 Symbolic reasoning engine loaded with 15 rules
🧠 Advanced enhancements enabled (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen)

🔄 Step 1: Processing query 'query_1'
  ⚠️  2 symbolic rule violation(s) detected
     - Energy Budget Exceeded [critical]
     - Memory Overflow Risk [critical]
  🧠 Enhanced decision: strategy=carbon_first, priority=green, MODP score=0.72 (higher is better; scale 0–1)
  🤔 Reflection checkpoint at step 1
  💭 Self-explanation: High resource usage detected
  
============================================================
✅ Green_Agent execution complete with neuro-symbolic oversight
📈 Total reflections: 3
🎯 Pareto frontier size: 2
🧠 Historical runs: 5
🔍 Symbolic violations: 8
⚠️  CRITICAL: 2 critical rule violation(s) detected!
   - Energy Budget Exceeded at step 1
   - Memory Overflow Risk at step 3
```

---

## Requirements

| Item | Version | Notes |
|---|---|---|
| Python | ≥ 3.10 | 3.11 tested |
| PyYAML | ≥ 6.0 | `symbolic_policy.yaml` parsing |
| `requirements.txt` | as shipped | base Green Agent |
| Enhancements deps | optional | required only when `ENHANCEMENTS_ENABLED=true` |
| Docker | optional | see [Reproduce with Docker](#reproduce-with-docker) |

---

## Architecture Overview

Feedback is composed of **four** layers. Numeric budgets + symbolic rules are
consumed by the feedback aggregator; advanced enhancements are an optional
side-channel that both *feeds metrics into rules* and *receives actions from
rules*.

```
┌─────────────────────────────────────────┐
│         Green Agent Core                │
│  • Metrics Collection                   │
│  • Reflection Engine                    │
│  • Pareto Analysis                      │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│    Symbolic Reasoning Layer (NEW)       │
│  • Rule Evaluation                      │
│  • Violation Detection                  │
│  • Trace Generation                     │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│    Multi-Layer Feedback System          │
│  1. Objective (Pareto)                  │
│  2. Subjective (Reflections)            │
│  3. Symbolic (Violations) ← NEW         │
│  4. Advanced Enhancements ← OPTIONAL    │
│     (MODP, RLHF, LIMIT Graph,           │
│      Distillation, MoE, Evolutionary,   │
│      FlexGen)                           │
└─────────────────────────────────────────┘
```

### Rule Evaluation Loop

```
   metrics ─► normalize ─► evaluate ─► trace ─► feedback ─► dashboard
                 │            │          │         │
                 │            ├─ unknown metric → log + skip
                 │            ├─ severity score
                 │            └─ action dispatch (optional enhancement call)
                 └─ unit coercion (g vs kg, ms vs s, J vs kWh)
```

---

## Key Features

### 1. Explicit Symbolic Rules

Define rules in `symbolic_policy.yaml`:

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon with high latency"
```

### 2. Automatic Violation Detection

Rules are evaluated automatically at:
- Each step completion
- Reflection checkpoints
- Policy violations
- Manual triggers (`engine.trigger_evaluation(metrics, step, domain=None)`)

### 3. Formal Violation Traces

```
Rule: Carbon Threshold Violation (SUST-001)
Condition: carbon > 60 AND latency > 2000
Observations:
  carbon = 72.0
  latency = 2500
Violation: Rule triggered → flag_inefficiency
```

### 4. Dashboard Integration

Violations appear in the dashboard with:
- Severity indicators (critical, high, medium, low) — see [Severity Taxonomy](#severity-taxonomy)
- Category grouping
- Timeline view
- Detailed trace explanations

### 5. Multi-Layer Feedback (formerly "Triple-Layer")

Combines four perspectives:
1. **Objective**: Pareto frontier analysis
2. **Subjective**: Agent self-reflections
3. **Symbolic**: Rule violations (NEW)
4. **Advanced Enhancements**: MODP / RLHF / Graph / Distillation / MoE / Evolutionary / FlexGen (optional)

### 6. Advanced Enhancements Integration

When enabled, the symbolic reasoning layer can also incorporate:
- **LIMIT Graph metrics** (centrality, connectivity) into rule conditions.
- **MODP composite score** to influence decision prioritisation.
- **RLHF human feedback** to adjust rule thresholds dynamically.
- **Multi‑Teacher On‑Policy Distillation + MoE gating** to select optimal mitigation actions.
- **Bio‑inspired Optimisation** to evolve rule weights or thresholds.
- **FlexGen integration** for high‑throughput LLM tasks with adaptive precision selection.

---

## Metric Reference

All variables usable in `condition` expressions. Units are canonical — the
engine does **not** silently convert; a rule using `carbon > 0.06` against a
grams-valued metric will **not** fire. Use the exact unit below.

### Core metrics (always available)

| Variable | Unit | Range | Source | Notes |
|---|---|---|---|---|
| `energy` | Wh | ≥ 0 | step metrics | per-step energy |
| `carbon` | g | ≥ 0 | step metrics | per-step grams CO₂ |
| `latency` | ms | ≥ 0 | step metrics | per-step latency |
| `memory` | MB | ≥ 0 | step metrics | peak RSS |
| `tool_calls` | count | ≥ 0 | step metrics | per-step |
| `cpu_percent` | % | 0–100 | step metrics | host CPU% |
| `cumulative.total_energy_wh` | Wh | ≥ 0 | cumulative | running total |
| `cumulative.total_carbon_kg` | kg | ≥ 0 | cumulative | running total |
| `cumulative.total_latency_ms` | ms | ≥ 0 | cumulative | running total |
| `cumulative.max_memory_mb` | MB | ≥ 0 | cumulative | max seen |
| `cumulative.total_tool_calls` | count | ≥ 0 | cumulative | running total |
| `cumulative.step_count` | count | ≥ 0 | cumulative | steps elapsed |

### Context metrics (request-scoped)

| Variable | Type | Values | Source |
|---|---|---|---|
| `query_type` | enum | `research`, `production`, `debug`, `batch` | request context |
| `environment` | enum | `dev`, `staging`, `production` | request context |

### Enhancement metrics (available when module enabled)

| Variable | Unit | Range | Source module | Enable flag |
|---|---|---|---|---|
| `modp_score` | — | 0–1, **higher = better** | `NodeDescriptor` | `MODP_ENABLED` |
| `graph_centrality` | — | 0–1 | `GraphRegistry` | `LIMIT_GRAPH_ENABLED` |
| `graph_connectivity` | — | 0–1 | `GraphRegistry` | `LIMIT_GRAPH_ENABLED` |
| `human_feedback_score` | — | 0–1, higher = better | `FeedbackEvent` | `RLHF_ENABLED` |
| `distillation_update_rate` | ΔL/step | ≥ 0 | distillation optimizer | `DISTILLATION_ENABLED` |
| `moe_gate_stddev` | — | 0–1 | MoE gate | `MOE_ENABLED` |
| `evolutionary_best_fitness` | — | 0–1, higher = better | evolutionary optimizer | `EVOLUTIONARY_ENABLED` |
| `flexgen_energy_kwh` | kWh | ≥ 0 | FlexGen runtime | `FLEXGEN_ENABLED` |

### Derived / temporal metrics (opt-in)

Computed by the engine when the corresponding config is present. Use these
instead of ad-hoc temporal operators.

| Variable | Unit | Meaning |
|---|---|---|
| `energy_variance` | Wh² | variance of `energy` over last N steps |
| `carbon_trend` | g/step | linear slope of `carbon` |
| `fitness_delta_1h` | — | `evolutionary_best_fitness` now − 1 h ago |
| `human_feedback_delta_1h` | — | `human_feedback_score` now − 1 h ago |

> ⚠️ **Deprecated**: `evolutionary_best_fitness offset 1h` is **not valid syntax**.
> Use `fitness_delta_1h < 0.01` instead.

---

## Rule Categories

### Sustainability Rules
- Carbon emissions
- Energy efficiency
- Resource optimization

### Resource Management Rules
- Memory usage
- CPU utilization
- Latency constraints

### Fairness Rules
- Resource distribution
- Query equity
- Load balancing

### Safety Rules
- Error rates
- System stability
- Cascading failures
- (Enhancement) Guardrail state via `ZERO_TRUST_ENABLED`

### Compliance Rules
- ESG standards
- Audit requirements
- Regulatory constraints
- (Enhancement) Privacy budget via `DIFFERENTIAL_PRIVACY_ENABLED`

### Enhanced Rules (optional)
Rules may reference advanced metrics:

```yaml
- id: "ENH-MODP-001"
  condition: "modp_score < 0.4"
  action: "flag_inefficiency"
  explanation: "MODP composite below 0.4 — trade-off regression"

- id: "ENH-GRAPH-001"
  condition: "graph_centrality < 0.3"
  action: "check_topology"
  explanation: "Node is topologically peripheral"

- id: "ENH-RLHF-001"
  condition: "human_feedback_score < 0.3"
  action: "collect_more_feedback"
  explanation: "Human feedback is weak"

- id: "ENH-FLEX-001"
  condition: "flexgen_energy_kwh > 1.0"
  action: "switch_precision"
  explanation: "FlexGen energy above 1 kWh — drop to fp16"
```

---

## Rule Action Vocabulary

The `action` field is dispatched to a **fixed** registry. Unknown actions are
**logged as warnings and ignored** (they do not raise). Use exactly one of the
following strings.

| Action | Effect | Requires module |
|---|---|---|
| `flag_inefficiency` | Annotate trace with `inefficiency` tag; no side-effect | — |
| `optimize_strategy` | Call `NodeDescriptor.select_routing_strategy()` | enhancements |
| `check_topology` | Emit topology warning in dashboard | `LIMIT_GRAPH_ENABLED` |
| `collect_more_feedback` | Enqueue `FeedbackEvent` request | `RLHF_ENABLED` |
| `restart_evolution` | Reset evolutionary population | `EVOLUTIONARY_ENABLED` |
| `investigate_expert_routing` | Emit MoE stability alert | `MOE_ENABLED` |
| `switch_precision` | FlexGen precision switch (fp16 → int8 etc.) | `FLEXGEN_ENABLED` |
| `optimize_research_strategy` | Domain-specific research tuning | — |
| `strict_carbon_enforcement` | Tighten carbon budget for remainder of run | — |
| `trigger_policy_review` | Raise to human review queue | — |
| `block_request` | Halt current request | `ZERO_TRUST_ENABLED` |

> Adding a new action requires editing `ACTION_REGISTRY` in
> `src/symbolic/symbolic_reasoning_engine.py` — do **not** rely on typos
> propagating silently; the engine logs `[symbolic] unknown action=<x>`.

---

## Severity Taxonomy

`priority` in the YAML maps to a numeric multiplier applied to the violation
score. The dashboard severity is derived from the **score**, not the raw
priority string.

| Severity | Score range | Typical trigger |
|---|---|---|
| `critical` | ≥ 1.5 × threshold | hard budget exceeded |
| `high` | 1.2 – 1.5 × threshold | near-budget |
| `medium` | 1.0 – 1.2 × threshold | at threshold |
| `low` | < 1.0 × threshold | soft warning |

Priority multipliers (applied before severity bucketing):

| `priority` | Multiplier |
|---|---|
| `critical` | 2.0 |
| `high` | 1.5 |
| `medium` | 1.0 |
| `low` | 0.5 |

---

## Customization

### Adding Custom Rules

Edit `symbolic_policy.yaml`:

```yaml
symbolic_rules:
  - id: "CUSTOM-001"
    name: "My Custom Rule"
    category: "custom"
    priority: "medium"
    condition: "energy > 3.0 AND tool_calls > 20"
    action: "optimize_strategy"
    explanation: "Custom constraint"
```

### Supported Operators

- **Comparison**: `>`, `<`, `>=`, `<=`, `==`, `!=`
- **Logical**: `AND`, `OR`, `NOT`
- **Grouping**: `(`, `)`
- **Variables**: see [Metric Reference](#metric-reference)

> The evaluator is a **whitelist parser**, not `eval()`. See [Security](#security).

### Domain-Specific Rules

```yaml
domain_extensions:
  research:
    - id: "RES-DOMAIN-001"
      condition: "query_type == 'research' AND tool_calls > 100"
      action: "optimize_research_strategy"
      explanation: "Research-specific constraint"
```

Activate with:
```python
violations = engine.evaluate_rules(metrics, step, domain="research")
```

---

## Rule Precedence & Lifecycle

### Precedence

When multiple rules fire in the same step, ordering is:

1. By `priority` (critical > high > medium > low)
2. Tie-break by `id` lexicographically (`ENH-*` before `SUST-*` before `RES-*`)

Precedence affects dashboard ordering and action dispatch order only; **all**
violations are recorded.

### Lifecycle fields

```yaml
symbolic_rules:
  - id: "SUST-001"
    enabled: true          # default true; false = skip evaluation
    deprecated: false      # true = still evaluated but emits deprecation warning
    supersedes: "SUST-000" # informational; old ID auto-marked deprecated
    ...
```

| Field | Effect |
|---|---|
| `enabled: false` | Rule skipped; appears in report as `skipped` |
| `deprecated: true` | Rule evaluated; warning emitted; dashboard badge |
| `supersedes: <id>` | Marks `<id>` deprecated in the same load cycle |

### Retiring a rule

1. Add `deprecated: true` for one minor release.
2. Update dashboards to reference the new `id`.
3. Remove the block in the next major release.

---

## Deduplication & Rate Limiting

Repeated violations of the same rule are **coalesced per step window** by
default to prevent dashboard spam.

```yaml
evaluation_config:
  dedup:
    window_steps: 5           # coalesce within 5-step window
    key: ["rule_id", "step_window"]
    keep: "first"             # or "last" or "worst"
  rate_limit:
    max_violations_per_step: 50
    on_exceed: "sample"       # or "drop" or "fail"
```

| Option | Behavior |
|---|---|
| `keep: first` | First occurrence kept; later ones counted |
| `keep: last` | Latest occurrence replaces earlier |
| `keep: worst` | Highest-severity occurrence kept |
| `on_exceed: sample` | Keep first N, mark rest as `sampled_out` |

Raw un-deduplicated events remain in `symbolic_violations.json`.

---

## Security

The rule evaluator is **not** `eval()`-based. Conditions are parsed by a
whitelist parser and executed against a sandboxed namespace.

| Concern | Guarantee |
|---|---|
| Arbitrary code execution | Not possible — no `eval`/`exec`/`import` |
| Attribute access | Not allowed — identifiers must appear in Metric Reference |
| Function calls | Not allowed |
| Allowed tokens | Identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| Unknown identifier | Logged as `UnknownMetric`, rule skipped (non-fatal) |
| YAML injection | Standard `yaml.safe_load` only |

If you need Z3 / SMT support, see [What's Not in Scope](#whats-not-in-scope).

---

## Integration with Advanced Enhancements

The neuro‑symbolic engine can be combined with the advanced modules in `src/enhancements` to achieve context‑aware, learned decision‑making. Below are the primary integration points.

### Configuration Matrix

| Module | Enable flag | Feeds metric |
|---|---|---|
| all enhancements | `ENHANCEMENTS_ENABLED=true` | — |
| LIMIT Graph | `LIMIT_GRAPH_ENABLED=true` | `graph_centrality`, `graph_connectivity` |
| MODP | `MODP_ENABLED=true` | `modp_score` |
| RLHF | `RLHF_ENABLED=true` | `human_feedback_score` |
| Distillation | `DISTILLATION_ENABLED=true` | `distillation_update_rate` |
| MoE | `MOE_ENABLED=true` (alias: `MOE_GATING_ENABLED`) | `moe_gate_stddev` |
| Evolutionary | `EVOLUTIONARY_ENABLED=true` | `evolutionary_best_fitness` |
| FlexGen | `FLEXGEN_ENABLED=true` | `flexgen_energy_kwh` |
| Zero Trust / Safety | `ZERO_TRUST_ENABLED=true` | trust state (via rules) |
| Privacy | `DIFFERENTIAL_PRIVACY_ENABLED=true` | DP budget (via rules) |
| Training / Checkpoint | `TRAINING_ENABLED=true` | checkpoint state |

### LIMIT Graph

- **What**: Graph metrics (centrality, connectivity) indicate the system's topological importance.
- **How**: The `GraphRegistry` and `CausalGraph` provide these metrics. They can be fed into symbolic rules or used to influence the selection of experts.
- **Example**:
  ```yaml
  symbolic_rules:
    - id: "ENH-GRAPH-001"
      condition: "graph_centrality < 0.3"
      action: "check_topology"
  ```

### MODP (Multi-Objective Decision Process)

- **What**: Configurable weights for **accuracy, energy, latency, carbon** — in that order.
- **How**: The `NodeDescriptor` and `WorkloadDescriptor` compute a composite score using these weights. The score can be included in rule conditions or used to adjust action priorities.
- **Scale**: `modp_score ∈ [0, 1]`, higher = better.
- **Example**:
  ```python
  # weights = [accuracy, energy, latency, carbon]
  node = NodeDescriptor(..., modp_weights=[0.4, 0.3, 0.2, 0.1])
  strategy = await node.select_routing_strategy()
  ```

### RLHF (Reinforcement Learning from Human Feedback)

- **What**: Human feedback score (0–1) influences policies.
- **How**: The feedback is part of the state in distillation teachers. Rules can check `human_feedback_score` to decide whether to collect more feedback or adjust behavior.
- **Example**:
  ```yaml
  - id: "ENH-RLHF-001"
    condition: "human_feedback_score < 0.3"
    action: "collect_more_feedback"
  ```

### Multi-Teacher On-Policy Distillation with MoE Gating

- **What**: A lightweight student learns from rule-based, historical ML, Q-learning, and RLHF teachers, blended by a gating network.
- **How**: The `NodeDescriptor.select_routing_strategy()` and `WorkloadDescriptor.select_priority()` internally use distillation + MoE. The symbolic engine can call these methods when a rule triggers an action like `"optimize_strategy"`.
- **Example**:
  ```python
  if any(v.action_triggered == "optimize_strategy" for v in violations):
      strategy = await node.select_routing_strategy()
      priority = await workload.select_priority()
  ```

### Bio-inspired Optimisation (Evolutionary)

- **What**: Genetic algorithms tune weights and hyperparameters.
- **How**: Set `use_evolutionary=True` in descriptors or Zero Trust config. The evolutionary optimizer updates MODP weights over time; these changes can be logged in `FeedbackEvent` and referenced in rules via `evolutionary_best_fitness` or the derived `fitness_delta_1h`.
- **Example**:
  ```yaml
  - id: "ENH-EVOL-001"
    condition: "fitness_delta_1h < 0.01"
    action: "restart_evolution"
  ```

### MoE Expert Gating

- **What**: Dynamic weighting of experts.
- **How**: The gating network is trained together with the student. Its stability (`moe_gate_stddev`) can be monitored via rules.
- **Example**:
  ```yaml
  - id: "ENH-MOE-001"
    condition: "moe_gate_stddev > 0.2"
    action: "investigate_expert_routing"
  ```

### FlexGen Integration

- **What**: High-throughput LLM inference with adaptive precision.
- **How**: The system can delegate to FlexGen when `delegation_policy` is `"adaptive"`. Symbolic rules can trigger precision switches based on energy or carbon.
- **Example**:
  ```yaml
  - id: "ENH-FLEX-001"
    condition: "flexgen_energy_kwh > 1.0"
    action: "switch_precision"
  ```

---

## API Reference

### SymbolicReasoningEngine

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

# Evaluate rules for a step
violations = engine.evaluate_rules(metrics, step=1, domain=None)

# Manual trigger (same as evaluate_rules but bypasses trigger config)
engine.trigger_evaluation(metrics, step=1, domain=None)

# Summary + export
summary = engine.get_violation_summary()
engine.export_violations("violations.json")
```

### SymbolicVisualizer

```python
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

visualizer = SymbolicVisualizer()
visualizer.add_violations([v.to_dict() for v in violations])
html = visualizer.generate_dashboard_section()
visualizer.export_violation_report("report.json")
```

### PolicyFeedback (enhanced)

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()

# v1.2+ : multi-layer feedback
result = feedback.generate_multi_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations,       # symbolic layer
    enhanced_metrics=enhanced_metrics,    # optional enhancement layer
)

# Backward-compatible alias (deprecated, will be removed in v2.0)
result = feedback.generate_dual_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations,
)
```

---

## Testing

Run the test suite:

```bash
python test_symbolic_oversight.py
```

Expected output:

```text
test_category_filtering ... ok
test_composite_rule ... ok
test_energy_violation ... ok
test_html_generation ... ok
test_memory_violation ... ok
test_no_violations ... ok
test_rule_loading ... ok
test_severity_summary ... ok
test_symbolic_recommendations ... ok
test_timeline_generation ... ok
test_triple_layer_feedback ... ok
test_violation_summary ... ok
test_violation_trace_structure ... ok
# Advanced enhancements tests (if modules available)
test_node_descriptor_routing ... ok              # src/enhancements/schemas/node_descriptor.py
test_workload_descriptor_priority ... ok         # src/enhancements/schemas/workload_descriptor.py
test_feedback_event_enhanced_fields ... ok       # src/enhancements/schemas/feedback_event.py
test_zero_trust_enhanced_init ... ok             # src/enhancements/zero_trust_architecture.py
test_graph_registry_and_causal ... ok            # src/enhancements/core/graph_registry.py
test_dag_carbon_ledger_backpropagation ... ok    # src/enhancements/core/dag_carbon_ledger.py

============================================================
Test Summary
============================================================
Tests run: 19
Successes: 19
Failures: 0
Errors: 0
```

> Enhancement tests are guarded with `@unittest.skipUnless(ENHANCEMENTS_ENABLED)`
> and will be reported as `skipped` if the modules are absent.

---

## Performance

### Computational Overhead

Measured on a 4 vCPU / 16 GB host, Python 3.11.4:

- **Rule evaluation**: ~5-10 ms per step (15 rules)
- **Memory footprint**: ~1-2 MB for violation history
- **Dashboard generation**: ~50-100 ms
- **Enhanced modules**: distillation inference < 5 ms per decision; evolutionary update negligible in frequency (once per N steps)

### Optimization Tips

1. **Reduce rule complexity**: Keep conditions simple
2. **Limit evaluation frequency**: Only at reflection checkpoints
3. **Prune history**: Archive old violations periodically
4. **Use priority ordering**: Critical rules first
5. **Disable enhancements when not needed**: Set `ENHANCEMENTS_ENABLED=false` (or use `--no-enhancements` CLI flag)

---

## Integration with Green_Agent

This implementation integrates seamlessly with [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent):

### File Structure
```
green_agent_repo/
├── symbolic_policy.yaml              # NEW
├── src/
│   ├── symbolic/                     # NEW
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py
│   ├── dashboard/
│   │   └── symbolic_visualizer.py    # NEW
│   ├── policy/
│   │   └── policy_feedback.py        # ENHANCED
│   ├── enhancements/                 # ADDITIONAL ADVANCED MODULES
│   │   ├── schemas/
│   │   │   ├── node_descriptor.py
│   │   │   ├── workload_descriptor.py
│   │   │   └── feedback_event.py
│   │   ├── zero_trust_architecture.py
│   │   └── core/
│   │       ├── graph_registry.py
│   │       ├── dag_carbon_ledger.py
│   │       └── ...
├── run_agent.py                      # ENHANCED
├── demo_symbolic_oversight.py        # NEW
├── test_symbolic_oversight.py        # NEW
├── NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md # NEW (deep-dive; see §Support)
├── SYMBOLIC_OVERSIGHT_README.md      # NEW (this file)
├── Dockerfile                        # multi-stage, non-root, healthcheck
└── .cursorrules                      # lean-ctx read modes
```

### Compatibility

✅ No breaking changes to existing functionality  
✅ Optional activation (works with/without `symbolic_policy.yaml`)  
✅ Backward compatible with existing policy files  
✅ Modular design (can be disabled)  
✅ Advanced enhancements are optional and gracefully skipped if modules missing  

---

## Reproduce with Docker

The repo ships a multi-stage `Dockerfile` with non-root user, healthcheck,
and `ARG`-driven enhancement toggles.

```bash
# Full image (all enhancements on)
docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .

# Lean image (minimal footprint)
docker build \
  --build-arg ENHANCEMENTS_ENABLED=false \
  --build-arg MODP_ENABLED=false \
  --build-arg RLHF_ENABLED=false \
  -t green-agent:lean .

# Run with symbolic oversight
docker run --rm -p 8000:8000 \
  -v "$PWD:/out" \
  -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
  green-agent:full \
  python run_agent.py --config example_config.json --policy green_policy.yaml

# Run demo
docker run --rm -v "$PWD:/out" green-agent:full \
  python demo_symbolic_oversight.py
```

---

## Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available, prefer enhanced read modes over
reading entire files. This saves tokens and keeps context focused.

```python
# One-shot snapshot of every enabled enhancement
ctx_read("src/enhancements/", mode="digest")

# MODP score + MoE gate weights in a single round-trip
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")

# Topology + next-hop probabilities (explain routing decisions)
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")

# Cost / energy slice for carbon debugging
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost+energy")

# Safety / privacy posture
ctx_read("src/enhancements/core/safety.py", mode="safety")
ctx_read("src/enhancements/core/privacy.py", mode="privacy")
```

See `.cursorrules` → **Enhanced Read Modes** for the full taxonomy,
token-budget table, and composite-mode contract.

---

## Troubleshooting

### Issue: Rules not triggering

**Cause**: Metric units mismatch.  
**Solution**: Verify against [Metric Reference](#metric-reference):
- Carbon: **grams** (not kg) in per-step rules; `cumulative.total_carbon_kg` is kg
- Latency: **milliseconds**
- Memory: **MB**
- FlexGen energy: **kWh** (not joules)

### Issue: False positives

**Cause**: Thresholds too strict.  
**Solution**: Adjust in `symbolic_policy.yaml`:
```yaml
condition: "carbon > 70"  # Increase threshold
```

### Issue: Rule never fires for `query_type` / `environment`

**Cause**: These context metrics require `domain=` passed to `evaluate_rules`
(or included in `metrics` dict explicitly).  
**Solution**:
```python
violations = engine.evaluate_rules(metrics, step=1, domain="production")
# or
metrics["environment"] = "production"
```

### Issue: `evolutionary_best_fitness offset 1h` never fires

**Cause**: `offset` is **not** a valid operator.  
**Solution**: Use the derived metric:
```yaml
condition: "fitness_delta_1h < 0.01"
```

### Issue: Performance degradation

**Cause**: Too many rules or evaluations.  
**Solution**: Reduce evaluation frequency:
```yaml
evaluation_config:
  evaluation_triggers:
    - "reflection_checkpoint"  # Only at checkpoints
```

### Issue: Missing `symbolic_policy.yaml`

**Cause**: File not found.  
**Solution**: Engine loads default rules automatically. Create `symbolic_policy.yaml` for custom rules.

### Issue: Enhancements not activating

**Solution**: Check that:
- `src/enhancements` folder exists
- Required dependencies are installed (`pip install -r requirements.txt`)
- `ENHANCEMENTS_ENABLED=true` in environment **or** `--enhancements` CLI flag
- Per-module flag set (e.g. `MODP_ENABLED=true`); see [Configuration Matrix](#configuration-matrix)
- MoE: flag is `MOE_ENABLED` (alias `MOE_GATING_ENABLED` accepted)

### Issue: Unknown action warning

**Cause**: Typo in `action:` field.  
**Solution**: Check [Rule Action Vocabulary](#rule-action-vocabulary). The engine logs:
```
[symbolic] unknown action=<x> rule_id=<id> — ignored
```

---

## Examples

### Example 1: Basic Violation Detection

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine()

metrics = {
    "energy": 6.0,  # Exceeds 5.0 limit
    "carbon": 70,   # Exceeds 60 g limit
    "latency": 2500,
    "memory": 300,
    "tool_calls": 25,
    "cumulative": {
        "total_energy_wh": 6.0,
        "total_carbon_kg": 0.07,
        "total_latency_ms": 2500,
        "max_memory_mb": 300,
        "total_tool_calls": 25,
        "step_count": 5,
    },
}

violations = engine.evaluate_rules(metrics, step=5)

for v in violations:
    print(f"{v.rule_name}: {v.explanation}")
```

### Example 2: Dashboard Integration

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

engine = SymbolicReasoningEngine()
visualizer = SymbolicVisualizer()

violations = engine.evaluate_rules(metrics, step=5)
visualizer.add_violations([v.to_dict() for v in violations])

html = visualizer.generate_dashboard_section()
with open("violations_dashboard.html", "w") as f:
    f.write(html)
```

### Example 3: Custom Domain Rules

```yaml
# In symbolic_policy.yaml
domain_extensions:
  production:
    - id: "PROD-001"
      condition: "environment == 'production' AND carbon > 50"
      action: "strict_carbon_enforcement"
      explanation: "Production requires stricter limits"
```

```python
violations = engine.evaluate_rules(
    metrics,
    step=1,
    domain="production",
)
```

### Example 4: Combined with Advanced Enhancements

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
import asyncio

engine = SymbolicReasoningEngine()
node = NodeDescriptor(...)

# ... gather metrics, evaluate rules
violations = engine.evaluate_rules(metrics, step=1)

# If any violation triggers optimization action, use enhanced routing
if any(v.action_triggered == "optimize_strategy" for v in violations):
    strategy = asyncio.run(node.select_routing_strategy())
    print(f"Selected enhanced strategy: {strategy}")
```

---

## FAQ

**Q: Does this replace the existing policy engine?**  
A: No, it complements it. Numeric budgets (`green_policy.yaml`) + symbolic rules (`symbolic_policy.yaml`) work together.

**Q: Can I disable symbolic oversight?**  
A: Yes — don't create `symbolic_policy.yaml`. The engine uses minimal default rules.

**Q: How do I add Z3 solver support?**  
A: Not in scope for v1.x. See [What's Not in Scope](#whats-not-in-scope). The current engine is a **whitelist rule evaluator**, not an SMT solver.

**Q: Can rules reference historical data?**  
A: Yes, via **derived metrics** (see [Metric Reference](#derived--temporal-metrics-opt-in)): `fitness_delta_1h`, `carbon_trend`, `energy_variance`, etc. Ad-hoc temporal syntax like `offset 1h` is **not** supported.

**Q: How do I export violations for external analysis?**  
A: `engine.export_violations("violations.json")` or `visualizer.export_violation_report("report.json")`.

**Q: How do the advanced enhancements integrate with symbolic rules?**  
A: Enhancements supply additional metrics (MODP score, graph centrality, human feedback, etc.) usable in rule conditions. When a rule triggers an action, the system can call the enhanced modules (e.g. `select_routing_strategy()`) to adapt behavior.

**Q: Why "multi-layer" instead of "triple-layer"?**  
A: v1.1 added a fourth, optional layer (advanced enhancements). The old name is retained as a backward-compat alias.

**Q: Are my rules executed with `eval()`?**  
A: No. See [Security](#security).

---

## Migration Guide

### From v1.0 (numeric-only) to v1.2

| Before | After |
|---|---|
| Only `green_policy.yaml` | `green_policy.yaml` **+** `symbolic_policy.yaml` |
| `feedback.generate_dual_layer_feedback(pareto, reflections, metrics)` | `feedback.generate_multi_layer_feedback(pareto, reflections, metrics, symbolic_violations=..., enhanced_metrics=...)` |
| `use_enhancements=True` in config | `ENHANCEMENTS_ENABLED=true` env (or `--enhancements` CLI) |
| `evolutionary_best_fitness offset 1h` | `fitness_delta_1h` |
| `flexgen_energy_joules` | `flexgen_energy_kwh` |

Backward-compatible shims are provided for `generate_dual_layer_feedback`,
`MOE_GATING_ENABLED`, and `flexgen_energy_joules`. These will be removed in v2.0.

### From v1.1 to v1.2

- Metrics table expanded (core, context, enhancement, derived).
- Severity taxonomy formalized — dashboards may re-bucket some violations.
- Added action vocabulary — unknown actions now warn instead of silently no-op.
- Added dedup config — default `window_steps: 5`.

---

## Changelog

### 1.2.0 (2026-04-10)

**Added**
- Metric Reference table with units, ranges, and source.
- Rule Action Vocabulary table.
- Severity Taxonomy with numeric triggers.
- Rule Precedence & Lifecycle section.
- Deduplication & Rate Limiting section.
- Security section (whitelist parser guarantees).
- Reproduce with Docker section.
- Inspecting State with lean-ctx section.
- Configuration Matrix for enhancements.
- Migration Guide.
- Derived metrics (`fitness_delta_1h`, `carbon_trend`, `energy_variance`).
- `generate_multi_layer_feedback()` API.

**Changed**
- "Triple-Layer" → "Multi-Layer" (4 layers).
- FlexGen energy metric `flexgen_energy_joules` → `flexgen_energy_kwh`.
- `use_enhancements` config key → `ENHANCEMENTS_ENABLED` env (config key still accepted as alias).
- `MOE_GATING_ENABLED` → `MOE_ENABLED` (old name kept as alias).

**Fixed**
- `evolutionary_best_fitness offset 1h` → `fitness_delta_1h`.
- Documented `query_type` and `environment` as context metrics.
- Corrected unit-mismatch examples (`carbon > 60` is grams).
- Real demo output captured on a defined host.

**Deprecated**
- `generate_dual_layer_feedback()` → use `generate_multi_layer_feedback()`.
- `flexgen_energy_joules` → use `flexgen_energy_kwh`.
- `use_enhancements` config key → use `ENHANCEMENTS_ENABLED`.

### 1.1.0

- Added advanced enhancement metrics to rule conditions.
- Added enhancement integration sections.

### 1.0.0

- Initial symbolic oversight layer.

---

## What's Not in Scope

- **Z3 / SMT solving** — the engine is a rule evaluator, not a solver.
- **Temporal logic operators** (`always`, `eventually`, `until`) — use derived metrics instead.
- **Cross-run state** — rules see current run only; persistence is left to the operator.
- **Arbitrary Python in conditions** — by design (see [Security](#security)).

---

## Contributing

To contribute enhancements:

1. Fork the Green_Agent repository
2. Add/modify rules in `symbolic_policy.yaml`
3. Extend `SymbolicReasoningEngine` if needed
4. Add tests in `test_symbolic_oversight.py`
5. Update documentation (this file + `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`)
6. Submit PR

### Adding a new action

1. Register it in `ACTION_REGISTRY` inside `symbolic_reasoning_engine.py`.
2. Add a row to [Rule Action Vocabulary](#rule-action-vocabulary).
3. Add a test in `test_symbolic_oversight.py`.

### Adding a new metric

1. Add it to `_normalize_metrics()`.
2. Add a row to [Metric Reference](#metric-reference) with **unit** and **range**.
3. If enhancement-gated, add the enable flag to [Configuration Matrix](#configuration-matrix).

---

## License

Follows the same license as Green_Agent repository.

---

## References

- **FormalJudge**: Neuro-symbolic oversight paradigm
- **Green_Agent**: [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent)
- **Symbolic AI**: Rule-based reasoning systems
- **Multi‑Teacher Distillation**: Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE**: Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF**: Christiano et al., "Deep Reinforcement Learning from Human Preferences"

---

## Support

For issues or questions:

1. Check this README and `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` (deep dive into the
   reasoning loop, rule parser internals, and enhancement plumbing).
2. Run `python demo_symbolic_oversight.py` for examples.
3. Run `python test_symbolic_oversight.py` to verify installation.
4. Open issue on Green_Agent repository.

---

**Version**: 1.2.0  
**Last Updated**: 2026-04-10  
**Status**: Production Ready
```
# Neuro-Symbolic Oversight for Green Agent

**Version:** 1.2.0  
**Last Updated:** 2026-04-10  
**Status:** Production Ready  
**Audience:** Operators, integrators, rule authors, and enhancement maintainers

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Requirements](#requirements)
3. [Architecture Overview](#architecture-overview)
4. [Key Features](#key-features)
5. [Metric Reference](#metric-reference)
6. [Rule Categories](#rule-categories)
7. [Rule Action Vocabulary](#rule-action-vocabulary)
8. [Severity Taxonomy](#severity-taxonomy)
9. [Customization](#customization)
10. [Rule Precedence & Lifecycle](#rule-precedence--lifecycle)
11. [Deduplication & Rate Limiting](#deduplication--rate-limiting)
12. [Security](#security)
13. [Integration with Advanced Enhancements](#integration-with-advanced-enhancements)
14. [API Reference](#api-reference)
15. [Testing](#testing)
16. [Performance](#performance)
17. [Integration with Green_Agent](#integration-with-green_agent)
18. [Reproduce with Docker](#reproduce-with-docker)
19. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
20. [Troubleshooting](#troubleshooting)
21. [Examples](#examples)
22. [FAQ](#faq)
23. [Migration Guide](#migration-guide)
24. [Changelog](#changelog)
25. [What's Not in Scope](#whats-not-in-scope)
26. [Contributing](#contributing)
27. [License](#license)
28. [References](#references)
29. [Support](#support)

---

## Quick Start

### Installation

No additional dependencies required beyond the base Green Agent installation.  
*(Optional)* To enable advanced enhancements (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen), ensure the `src/enhancements` modules are available and dependencies listed in the main `requirements.txt` are installed.

### Basic Usage

1. **Run the demo:**
```bash
python demo_symbolic_oversight.py
```

2. **Run with symbolic oversight:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml
```

3. **Run with advanced enhancements:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml --enhancements
```
   > `--enhancements` is sugar for `ENHANCEMENTS_ENABLED=true`. Per-module flags
   > (e.g. `MODP_ENABLED`, `FLEXGEN_ENABLED`) still apply. See
   > [Configuration Matrix](#integration-with-advanced-enhancements).

4. **Check results:**
- `symbolic_violations.json` - Detailed violation traces
- `dashboard.html` - Visual dashboard with violations
- `symbolic_violation_report.json` - Summary report

### Example Output

> Captured from `python demo_symbolic_oversight.py` on a 4 vCPU / 16 GB host,
> Python 3.11.4, 15 default rules loaded.

```
🌱 Green Agent - Meta-Cognitive Architecture v2.0
============================================================
📋 Policy loaded: {'max_energy_per_task_wh': 5.0, ...}
🔍 Symbolic reasoning engine loaded with 15 rules
🧠 Advanced enhancements enabled (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen)

🔄 Step 1: Processing query 'query_1'
  ⚠️  2 symbolic rule violation(s) detected
     - Energy Budget Exceeded [critical]
     - Memory Overflow Risk [critical]
  🧠 Enhanced decision: strategy=carbon_first, priority=green, MODP score=0.72 (higher is better; scale 0–1)
  🤔 Reflection checkpoint at step 1
  💭 Self-explanation: High resource usage detected
  
============================================================
✅ Green_Agent execution complete with neuro-symbolic oversight
📈 Total reflections: 3
🎯 Pareto frontier size: 2
🧠 Historical runs: 5
🔍 Symbolic violations: 8
⚠️  CRITICAL: 2 critical rule violation(s) detected!
   - Energy Budget Exceeded at step 1
   - Memory Overflow Risk at step 3
```

---

## Requirements

| Item | Version | Notes |
|---|---|---|
| Python | ≥ 3.10 | 3.11 tested |
| PyYAML | ≥ 6.0 | `symbolic_policy.yaml` parsing |
| `requirements.txt` | as shipped | base Green Agent |
| Enhancements deps | optional | required only when `ENHANCEMENTS_ENABLED=true` |
| Docker | optional | see [Reproduce with Docker](#reproduce-with-docker) |

---

## Architecture Overview

Feedback is composed of **four** layers. Numeric budgets + symbolic rules are
consumed by the feedback aggregator; advanced enhancements are an optional
side-channel that both *feeds metrics into rules* and *receives actions from
rules*.

```
┌─────────────────────────────────────────┐
│         Green Agent Core                │
│  • Metrics Collection                   │
│  • Reflection Engine                    │
│  • Pareto Analysis                      │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│    Symbolic Reasoning Layer (NEW)       │
│  • Rule Evaluation                      │
│  • Violation Detection                  │
│  • Trace Generation                     │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│    Multi-Layer Feedback System          │
│  1. Objective (Pareto)                  │
│  2. Subjective (Reflections)            │
│  3. Symbolic (Violations) ← NEW         │
│  4. Advanced Enhancements ← OPTIONAL    │
│     (MODP, RLHF, LIMIT Graph,           │
│      Distillation, MoE, Evolutionary,   │
│      FlexGen)                           │
└─────────────────────────────────────────┘
```

### Rule Evaluation Loop

```
   metrics ─► normalize ─► evaluate ─► trace ─► feedback ─► dashboard
                 │            │          │         │
                 │            ├─ unknown metric → log + skip
                 │            ├─ severity score
                 │            └─ action dispatch (optional enhancement call)
                 └─ unit coercion (g vs kg, ms vs s, J vs kWh)
```

---

## Key Features

### 1. Explicit Symbolic Rules

Define rules in `symbolic_policy.yaml`:

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon with high latency"
```

### 2. Automatic Violation Detection

Rules are evaluated automatically at:
- Each step completion
- Reflection checkpoints
- Policy violations
- Manual triggers (`engine.trigger_evaluation(metrics, step, domain=None)`)

### 3. Formal Violation Traces

```
Rule: Carbon Threshold Violation (SUST-001)
Condition: carbon > 60 AND latency > 2000
Observations:
  carbon = 72.0
  latency = 2500
Violation: Rule triggered → flag_inefficiency
```

### 4. Dashboard Integration

Violations appear in the dashboard with:
- Severity indicators (critical, high, medium, low) — see [Severity Taxonomy](#severity-taxonomy)
- Category grouping
- Timeline view
- Detailed trace explanations

### 5. Multi-Layer Feedback (formerly "Triple-Layer")

Combines four perspectives:
1. **Objective**: Pareto frontier analysis
2. **Subjective**: Agent self-reflections
3. **Symbolic**: Rule violations (NEW)
4. **Advanced Enhancements**: MODP / RLHF / Graph / Distillation / MoE / Evolutionary / FlexGen (optional)

### 6. Advanced Enhancements Integration

When enabled, the symbolic reasoning layer can also incorporate:
- **LIMIT Graph metrics** (centrality, connectivity) into rule conditions.
- **MODP composite score** to influence decision prioritisation.
- **RLHF human feedback** to adjust rule thresholds dynamically.
- **Multi‑Teacher On‑Policy Distillation + MoE gating** to select optimal mitigation actions.
- **Bio‑inspired Optimisation** to evolve rule weights or thresholds.
- **FlexGen integration** for high‑throughput LLM tasks with adaptive precision selection.

---

## Metric Reference

All variables usable in `condition` expressions. Units are canonical — the
engine does **not** silently convert; a rule using `carbon > 0.06` against a
grams-valued metric will **not** fire. Use the exact unit below.

### Core metrics (always available)

| Variable | Unit | Range | Source | Notes |
|---|---|---|---|---|
| `energy` | Wh | ≥ 0 | step metrics | per-step energy |
| `carbon` | g | ≥ 0 | step metrics | per-step grams CO₂ |
| `latency` | ms | ≥ 0 | step metrics | per-step latency |
| `memory` | MB | ≥ 0 | step metrics | peak RSS |
| `tool_calls` | count | ≥ 0 | step metrics | per-step |
| `cpu_percent` | % | 0–100 | step metrics | host CPU% |
| `cumulative.total_energy_wh` | Wh | ≥ 0 | cumulative | running total |
| `cumulative.total_carbon_kg` | kg | ≥ 0 | cumulative | running total |
| `cumulative.total_latency_ms` | ms | ≥ 0 | cumulative | running total |
| `cumulative.max_memory_mb` | MB | ≥ 0 | cumulative | max seen |
| `cumulative.total_tool_calls` | count | ≥ 0 | cumulative | running total |
| `cumulative.step_count` | count | ≥ 0 | cumulative | steps elapsed |

### Context metrics (request-scoped)

| Variable | Type | Values | Source |
|---|---|---|---|
| `query_type` | enum | `research`, `production`, `debug`, `batch` | request context |
| `environment` | enum | `dev`, `staging`, `production` | request context |

### Enhancement metrics (available when module enabled)

| Variable | Unit | Range | Source module | Enable flag |
|---|---|---|---|---|
| `modp_score` | — | 0–1, **higher = better** | `NodeDescriptor` | `MODP_ENABLED` |
| `graph_centrality` | — | 0–1 | `GraphRegistry` | `LIMIT_GRAPH_ENABLED` |
| `graph_connectivity` | — | 0–1 | `GraphRegistry` | `LIMIT_GRAPH_ENABLED` |
| `human_feedback_score` | — | 0–1, higher = better | `FeedbackEvent` | `RLHF_ENABLED` |
| `distillation_update_rate` | ΔL/step | ≥ 0 | distillation optimizer | `DISTILLATION_ENABLED` |
| `moe_gate_stddev` | — | 0–1 | MoE gate | `MOE_ENABLED` |
| `evolutionary_best_fitness` | — | 0–1, higher = better | evolutionary optimizer | `EVOLUTIONARY_ENABLED` |
| `flexgen_energy_kwh` | kWh | ≥ 0 | FlexGen runtime | `FLEXGEN_ENABLED` |

### Derived / temporal metrics (opt-in)

Computed by the engine when the corresponding config is present. Use these
instead of ad-hoc temporal operators.

| Variable | Unit | Meaning |
|---|---|---|
| `energy_variance` | Wh² | variance of `energy` over last N steps |
| `carbon_trend` | g/step | linear slope of `carbon` |
| `fitness_delta_1h` | — | `evolutionary_best_fitness` now − 1 h ago |
| `human_feedback_delta_1h` | — | `human_feedback_score` now − 1 h ago |

> ⚠️ **Deprecated**: `evolutionary_best_fitness offset 1h` is **not valid syntax**.
> Use `fitness_delta_1h < 0.01` instead.

---

## Rule Categories

### Sustainability Rules
- Carbon emissions
- Energy efficiency
- Resource optimization

### Resource Management Rules
- Memory usage
- CPU utilization
- Latency constraints

### Fairness Rules
- Resource distribution
- Query equity
- Load balancing

### Safety Rules
- Error rates
- System stability
- Cascading failures
- (Enhancement) Guardrail state via `ZERO_TRUST_ENABLED`

### Compliance Rules
- ESG standards
- Audit requirements
- Regulatory constraints
- (Enhancement) Privacy budget via `DIFFERENTIAL_PRIVACY_ENABLED`

### Enhanced Rules (optional)
Rules may reference advanced metrics:

```yaml
- id: "ENH-MODP-001"
  condition: "modp_score < 0.4"
  action: "flag_inefficiency"
  explanation: "MODP composite below 0.4 — trade-off regression"

- id: "ENH-GRAPH-001"
  condition: "graph_centrality < 0.3"
  action: "check_topology"
  explanation: "Node is topologically peripheral"

- id: "ENH-RLHF-001"
  condition: "human_feedback_score < 0.3"
  action: "collect_more_feedback"
  explanation: "Human feedback is weak"

- id: "ENH-FLEX-001"
  condition: "flexgen_energy_kwh > 1.0"
  action: "switch_precision"
  explanation: "FlexGen energy above 1 kWh — drop to fp16"
```

---

## Rule Action Vocabulary

The `action` field is dispatched to a **fixed** registry. Unknown actions are
**logged as warnings and ignored** (they do not raise). Use exactly one of the
following strings.

| Action | Effect | Requires module |
|---|---|---|
| `flag_inefficiency` | Annotate trace with `inefficiency` tag; no side-effect | — |
| `optimize_strategy` | Call `NodeDescriptor.select_routing_strategy()` | enhancements |
| `check_topology` | Emit topology warning in dashboard | `LIMIT_GRAPH_ENABLED` |
| `collect_more_feedback` | Enqueue `FeedbackEvent` request | `RLHF_ENABLED` |
| `restart_evolution` | Reset evolutionary population | `EVOLUTIONARY_ENABLED` |
| `investigate_expert_routing` | Emit MoE stability alert | `MOE_ENABLED` |
| `switch_precision` | FlexGen precision switch (fp16 → int8 etc.) | `FLEXGEN_ENABLED` |
| `optimize_research_strategy` | Domain-specific research tuning | — |
| `strict_carbon_enforcement` | Tighten carbon budget for remainder of run | — |
| `trigger_policy_review` | Raise to human review queue | — |
| `block_request` | Halt current request | `ZERO_TRUST_ENABLED` |

> Adding a new action requires editing `ACTION_REGISTRY` in
> `src/symbolic/symbolic_reasoning_engine.py` — do **not** rely on typos
> propagating silently; the engine logs `[symbolic] unknown action=<x>`.

---

## Severity Taxonomy

`priority` in the YAML maps to a numeric multiplier applied to the violation
score. The dashboard severity is derived from the **score**, not the raw
priority string.

| Severity | Score range | Typical trigger |
|---|---|---|
| `critical` | ≥ 1.5 × threshold | hard budget exceeded |
| `high` | 1.2 – 1.5 × threshold | near-budget |
| `medium` | 1.0 – 1.2 × threshold | at threshold |
| `low` | < 1.0 × threshold | soft warning |

Priority multipliers (applied before severity bucketing):

| `priority` | Multiplier |
|---|---|
| `critical` | 2.0 |
| `high` | 1.5 |
| `medium` | 1.0 |
| `low` | 0.5 |

---

## Customization

### Adding Custom Rules

Edit `symbolic_policy.yaml`:

```yaml
symbolic_rules:
  - id: "CUSTOM-001"
    name: "My Custom Rule"
    category: "custom"
    priority: "medium"
    condition: "energy > 3.0 AND tool_calls > 20"
    action: "optimize_strategy"
    explanation: "Custom constraint"
```

### Supported Operators

- **Comparison**: `>`, `<`, `>=`, `<=`, `==`, `!=`
- **Logical**: `AND`, `OR`, `NOT`
- **Grouping**: `(`, `)`
- **Variables**: see [Metric Reference](#metric-reference)

> The evaluator is a **whitelist parser**, not `eval()`. See [Security](#security).

### Domain-Specific Rules

```yaml
domain_extensions:
  research:
    - id: "RES-DOMAIN-001"
      condition: "query_type == 'research' AND tool_calls > 100"
      action: "optimize_research_strategy"
      explanation: "Research-specific constraint"
```

Activate with:
```python
violations = engine.evaluate_rules(metrics, step, domain="research")
```

---

## Rule Precedence & Lifecycle

### Precedence

When multiple rules fire in the same step, ordering is:

1. By `priority` (critical > high > medium > low)
2. Tie-break by `id` lexicographically (`ENH-*` before `SUST-*` before `RES-*`)

Precedence affects dashboard ordering and action dispatch order only; **all**
violations are recorded.

### Lifecycle fields

```yaml
symbolic_rules:
  - id: "SUST-001"
    enabled: true          # default true; false = skip evaluation
    deprecated: false      # true = still evaluated but emits deprecation warning
    supersedes: "SUST-000" # informational; old ID auto-marked deprecated
    ...
```

| Field | Effect |
|---|---|
| `enabled: false` | Rule skipped; appears in report as `skipped` |
| `deprecated: true` | Rule evaluated; warning emitted; dashboard badge |
| `supersedes: <id>` | Marks `<id>` deprecated in the same load cycle |

### Retiring a rule

1. Add `deprecated: true` for one minor release.
2. Update dashboards to reference the new `id`.
3. Remove the block in the next major release.

---

## Deduplication & Rate Limiting

Repeated violations of the same rule are **coalesced per step window** by
default to prevent dashboard spam.

```yaml
evaluation_config:
  dedup:
    window_steps: 5           # coalesce within 5-step window
    key: ["rule_id", "step_window"]
    keep: "first"             # or "last" or "worst"
  rate_limit:
    max_violations_per_step: 50
    on_exceed: "sample"       # or "drop" or "fail"
```

| Option | Behavior |
|---|---|
| `keep: first` | First occurrence kept; later ones counted |
| `keep: last` | Latest occurrence replaces earlier |
| `keep: worst` | Highest-severity occurrence kept |
| `on_exceed: sample` | Keep first N, mark rest as `sampled_out` |

Raw un-deduplicated events remain in `symbolic_violations.json`.

---

## Security

The rule evaluator is **not** `eval()`-based. Conditions are parsed by a
whitelist parser and executed against a sandboxed namespace.

| Concern | Guarantee |
|---|---|
| Arbitrary code execution | Not possible — no `eval`/`exec`/`import` |
| Attribute access | Not allowed — identifiers must appear in Metric Reference |
| Function calls | Not allowed |
| Allowed tokens | Identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| Unknown identifier | Logged as `UnknownMetric`, rule skipped (non-fatal) |
| YAML injection | Standard `yaml.safe_load` only |

If you need Z3 / SMT support, see [What's Not in Scope](#whats-not-in-scope).

---

## Integration with Advanced Enhancements

The neuro‑symbolic engine can be combined with the advanced modules in `src/enhancements` to achieve context‑aware, learned decision‑making. Below are the primary integration points.

### Configuration Matrix

| Module | Enable flag | Feeds metric |
|---|---|---|
| all enhancements | `ENHANCEMENTS_ENABLED=true` | — |
| LIMIT Graph | `LIMIT_GRAPH_ENABLED=true` | `graph_centrality`, `graph_connectivity` |
| MODP | `MODP_ENABLED=true` | `modp_score` |
| RLHF | `RLHF_ENABLED=true` | `human_feedback_score` |
| Distillation | `DISTILLATION_ENABLED=true` | `distillation_update_rate` |
| MoE | `MOE_ENABLED=true` (alias: `MOE_GATING_ENABLED`) | `moe_gate_stddev` |
| Evolutionary | `EVOLUTIONARY_ENABLED=true` | `evolutionary_best_fitness` |
| FlexGen | `FLEXGEN_ENABLED=true` | `flexgen_energy_kwh` |
| Zero Trust / Safety | `ZERO_TRUST_ENABLED=true` | trust state (via rules) |
| Privacy | `DIFFERENTIAL_PRIVACY_ENABLED=true` | DP budget (via rules) |
| Training / Checkpoint | `TRAINING_ENABLED=true` | checkpoint state |

### LIMIT Graph

- **What**: Graph metrics (centrality, connectivity) indicate the system's topological importance.
- **How**: The `GraphRegistry` and `CausalGraph` provide these metrics. They can be fed into symbolic rules or used to influence the selection of experts.
- **Example**:
  ```yaml
  symbolic_rules:
    - id: "ENH-GRAPH-001"
      condition: "graph_centrality < 0.3"
      action: "check_topology"
  ```

### MODP (Multi-Objective Decision Process)

- **What**: Configurable weights for **accuracy, energy, latency, carbon** — in that order.
- **How**: The `NodeDescriptor` and `WorkloadDescriptor` compute a composite score using these weights. The score can be included in rule conditions or used to adjust action priorities.
- **Scale**: `modp_score ∈ [0, 1]`, higher = better.
- **Example**:
  ```python
  # weights = [accuracy, energy, latency, carbon]
  node = NodeDescriptor(..., modp_weights=[0.4, 0.3, 0.2, 0.1])
  strategy = await node.select_routing_strategy()
  ```

### RLHF (Reinforcement Learning from Human Feedback)

- **What**: Human feedback score (0–1) influences policies.
- **How**: The feedback is part of the state in distillation teachers. Rules can check `human_feedback_score` to decide whether to collect more feedback or adjust behavior.
- **Example**:
  ```yaml
  - id: "ENH-RLHF-001"
    condition: "human_feedback_score < 0.3"
    action: "collect_more_feedback"
  ```

### Multi-Teacher On-Policy Distillation with MoE Gating

- **What**: A lightweight student learns from rule-based, historical ML, Q-learning, and RLHF teachers, blended by a gating network.
- **How**: The `NodeDescriptor.select_routing_strategy()` and `WorkloadDescriptor.select_priority()` internally use distillation + MoE. The symbolic engine can call these methods when a rule triggers an action like `"optimize_strategy"`.
- **Example**:
  ```python
  if any(v.action_triggered == "optimize_strategy" for v in violations):
      strategy = await node.select_routing_strategy()
      priority = await workload.select_priority()
  ```

### Bio-inspired Optimisation (Evolutionary)

- **What**: Genetic algorithms tune weights and hyperparameters.
- **How**: Set `use_evolutionary=True` in descriptors or Zero Trust config. The evolutionary optimizer updates MODP weights over time; these changes can be logged in `FeedbackEvent` and referenced in rules via `evolutionary_best_fitness` or the derived `fitness_delta_1h`.
- **Example**:
  ```yaml
  - id: "ENH-EVOL-001"
    condition: "fitness_delta_1h < 0.01"
    action: "restart_evolution"
  ```

### MoE Expert Gating

- **What**: Dynamic weighting of experts.
- **How**: The gating network is trained together with the student. Its stability (`moe_gate_stddev`) can be monitored via rules.
- **Example**:
  ```yaml
  - id: "ENH-MOE-001"
    condition: "moe_gate_stddev > 0.2"
    action: "investigate_expert_routing"
  ```

### FlexGen Integration

- **What**: High-throughput LLM inference with adaptive precision.
- **How**: The system can delegate to FlexGen when `delegation_policy` is `"adaptive"`. Symbolic rules can trigger precision switches based on energy or carbon.
- **Example**:
  ```yaml
  - id: "ENH-FLEX-001"
    condition: "flexgen_energy_kwh > 1.0"
    action: "switch_precision"
  ```

---

## API Reference

### SymbolicReasoningEngine

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

# Evaluate rules for a step
violations = engine.evaluate_rules(metrics, step=1, domain=None)

# Manual trigger (same as evaluate_rules but bypasses trigger config)
engine.trigger_evaluation(metrics, step=1, domain=None)

# Summary + export
summary = engine.get_violation_summary()
engine.export_violations("violations.json")
```

### SymbolicVisualizer

```python
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

visualizer = SymbolicVisualizer()
visualizer.add_violations([v.to_dict() for v in violations])
html = visualizer.generate_dashboard_section()
visualizer.export_violation_report("report.json")
```

### PolicyFeedback (enhanced)

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()

# v1.2+ : multi-layer feedback
result = feedback.generate_multi_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations,       # symbolic layer
    enhanced_metrics=enhanced_metrics,    # optional enhancement layer
)

# Backward-compatible alias (deprecated, will be removed in v2.0)
result = feedback.generate_dual_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations,
)
```

---

## Testing

Run the test suite:

```bash
python test_symbolic_oversight.py
```

Expected output:

```text
test_category_filtering ... ok
test_composite_rule ... ok
test_energy_violation ... ok
test_html_generation ... ok
test_memory_violation ... ok
test_no_violations ... ok
test_rule_loading ... ok
test_severity_summary ... ok
test_symbolic_recommendations ... ok
test_timeline_generation ... ok
test_triple_layer_feedback ... ok
test_violation_summary ... ok
test_violation_trace_structure ... ok
# Advanced enhancements tests (if modules available)
test_node_descriptor_routing ... ok              # src/enhancements/schemas/node_descriptor.py
test_workload_descriptor_priority ... ok         # src/enhancements/schemas/workload_descriptor.py
test_feedback_event_enhanced_fields ... ok       # src/enhancements/schemas/feedback_event.py
test_zero_trust_enhanced_init ... ok             # src/enhancements/zero_trust_architecture.py
test_graph_registry_and_causal ... ok            # src/enhancements/core/graph_registry.py
test_dag_carbon_ledger_backpropagation ... ok    # src/enhancements/core/dag_carbon_ledger.py

============================================================
Test Summary
============================================================
Tests run: 19
Successes: 19
Failures: 0
Errors: 0
```

> Enhancement tests are guarded with `@unittest.skipUnless(ENHANCEMENTS_ENABLED)`
> and will be reported as `skipped` if the modules are absent.

---

## Performance

### Computational Overhead

Measured on a 4 vCPU / 16 GB host, Python 3.11.4:

- **Rule evaluation**: ~5-10 ms per step (15 rules)
- **Memory footprint**: ~1-2 MB for violation history
- **Dashboard generation**: ~50-100 ms
- **Enhanced modules**: distillation inference < 5 ms per decision; evolutionary update negligible in frequency (once per N steps)

### Optimization Tips

1. **Reduce rule complexity**: Keep conditions simple
2. **Limit evaluation frequency**: Only at reflection checkpoints
3. **Prune history**: Archive old violations periodically
4. **Use priority ordering**: Critical rules first
5. **Disable enhancements when not needed**: Set `ENHANCEMENTS_ENABLED=false` (or use `--no-enhancements` CLI flag)

---

## Integration with Green_Agent

This implementation integrates seamlessly with [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent):

### File Structure
```
green_agent_repo/
├── symbolic_policy.yaml              # NEW
├── src/
│   ├── symbolic/                     # NEW
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py
│   ├── dashboard/
│   │   └── symbolic_visualizer.py    # NEW
│   ├── policy/
│   │   └── policy_feedback.py        # ENHANCED
│   ├── enhancements/                 # ADDITIONAL ADVANCED MODULES
│   │   ├── schemas/
│   │   │   ├── node_descriptor.py
│   │   │   ├── workload_descriptor.py
│   │   │   └── feedback_event.py
│   │   ├── zero_trust_architecture.py
│   │   └── core/
│   │       ├── graph_registry.py
│   │       ├── dag_carbon_ledger.py
│   │       └── ...
├── run_agent.py                      # ENHANCED
├── demo_symbolic_oversight.py        # NEW
├── test_symbolic_oversight.py        # NEW
├── NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md # NEW (deep-dive; see §Support)
├── SYMBOLIC_OVERSIGHT_README.md      # NEW (this file)
├── Dockerfile                        # multi-stage, non-root, healthcheck
└── .cursorrules                      # lean-ctx read modes
```

### Compatibility

✅ No breaking changes to existing functionality  
✅ Optional activation (works with/without `symbolic_policy.yaml`)  
✅ Backward compatible with existing policy files  
✅ Modular design (can be disabled)  
✅ Advanced enhancements are optional and gracefully skipped if modules missing  

---

## Reproduce with Docker

The repo ships a multi-stage `Dockerfile` with non-root user, healthcheck,
and `ARG`-driven enhancement toggles.

```bash
# Full image (all enhancements on)
docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .

# Lean image (minimal footprint)
docker build \
  --build-arg ENHANCEMENTS_ENABLED=false \
  --build-arg MODP_ENABLED=false \
  --build-arg RLHF_ENABLED=false \
  -t green-agent:lean .

# Run with symbolic oversight
docker run --rm -p 8000:8000 \
  -v "$PWD:/out" \
  -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
  green-agent:full \
  python run_agent.py --config example_config.json --policy green_policy.yaml

# Run demo
docker run --rm -v "$PWD:/out" green-agent:full \
  python demo_symbolic_oversight.py
```

---

## Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available, prefer enhanced read modes over
reading entire files. This saves tokens and keeps context focused.

```python
# One-shot snapshot of every enabled enhancement
ctx_read("src/enhancements/", mode="digest")

# MODP score + MoE gate weights in a single round-trip
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")

# Topology + next-hop probabilities (explain routing decisions)
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")

# Cost / energy slice for carbon debugging
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost+energy")

# Safety / privacy posture
ctx_read("src/enhancements/core/safety.py", mode="safety")
ctx_read("src/enhancements/core/privacy.py", mode="privacy")
```

See `.cursorrules` → **Enhanced Read Modes** for the full taxonomy,
token-budget table, and composite-mode contract.

---

## Troubleshooting

### Issue: Rules not triggering

**Cause**: Metric units mismatch.  
**Solution**: Verify against [Metric Reference](#metric-reference):
- Carbon: **grams** (not kg) in per-step rules; `cumulative.total_carbon_kg` is kg
- Latency: **milliseconds**
- Memory: **MB**
- FlexGen energy: **kWh** (not joules)

### Issue: False positives

**Cause**: Thresholds too strict.  
**Solution**: Adjust in `symbolic_policy.yaml`:
```yaml
condition: "carbon > 70"  # Increase threshold
```

### Issue: Rule never fires for `query_type` / `environment`

**Cause**: These context metrics require `domain=` passed to `evaluate_rules`
(or included in `metrics` dict explicitly).  
**Solution**:
```python
violations = engine.evaluate_rules(metrics, step=1, domain="production")
# or
metrics["environment"] = "production"
```

### Issue: `evolutionary_best_fitness offset 1h` never fires

**Cause**: `offset` is **not** a valid operator.  
**Solution**: Use the derived metric:
```yaml
condition: "fitness_delta_1h < 0.01"
```

### Issue: Performance degradation

**Cause**: Too many rules or evaluations.  
**Solution**: Reduce evaluation frequency:
```yaml
evaluation_config:
  evaluation_triggers:
    - "reflection_checkpoint"  # Only at checkpoints
```

### Issue: Missing `symbolic_policy.yaml`

**Cause**: File not found.  
**Solution**: Engine loads default rules automatically. Create `symbolic_policy.yaml` for custom rules.

### Issue: Enhancements not activating

**Solution**: Check that:
- `src/enhancements` folder exists
- Required dependencies are installed (`pip install -r requirements.txt`)
- `ENHANCEMENTS_ENABLED=true` in environment **or** `--enhancements` CLI flag
- Per-module flag set (e.g. `MODP_ENABLED=true`); see [Configuration Matrix](#configuration-matrix)
- MoE: flag is `MOE_ENABLED` (alias `MOE_GATING_ENABLED` accepted)

### Issue: Unknown action warning

**Cause**: Typo in `action:` field.  
**Solution**: Check [Rule Action Vocabulary](#rule-action-vocabulary). The engine logs:
```
[symbolic] unknown action=<x> rule_id=<id> — ignored
```

---

## Examples

### Example 1: Basic Violation Detection

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine()

metrics = {
    "energy": 6.0,  # Exceeds 5.0 limit
    "carbon": 70,   # Exceeds 60 g limit
    "latency": 2500,
    "memory": 300,
    "tool_calls": 25,
    "cumulative": {
        "total_energy_wh": 6.0,
        "total_carbon_kg": 0.07,
        "total_latency_ms": 2500,
        "max_memory_mb": 300,
        "total_tool_calls": 25,
        "step_count": 5,
    },
}

violations = engine.evaluate_rules(metrics, step=5)

for v in violations:
    print(f"{v.rule_name}: {v.explanation}")
```

### Example 2: Dashboard Integration

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

engine = SymbolicReasoningEngine()
visualizer = SymbolicVisualizer()

violations = engine.evaluate_rules(metrics, step=5)
visualizer.add_violations([v.to_dict() for v in violations])

html = visualizer.generate_dashboard_section()
with open("violations_dashboard.html", "w") as f:
    f.write(html)
```

### Example 3: Custom Domain Rules

```yaml
# In symbolic_policy.yaml
domain_extensions:
  production:
    - id: "PROD-001"
      condition: "environment == 'production' AND carbon > 50"
      action: "strict_carbon_enforcement"
      explanation: "Production requires stricter limits"
```

```python
violations = engine.evaluate_rules(
    metrics,
    step=1,
    domain="production",
)
```

### Example 4: Combined with Advanced Enhancements

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
import asyncio

engine = SymbolicReasoningEngine()
node = NodeDescriptor(...)

# ... gather metrics, evaluate rules
violations = engine.evaluate_rules(metrics, step=1)

# If any violation triggers optimization action, use enhanced routing
if any(v.action_triggered == "optimize_strategy" for v in violations):
    strategy = asyncio.run(node.select_routing_strategy())
    print(f"Selected enhanced strategy: {strategy}")
```

---

## FAQ

**Q: Does this replace the existing policy engine?**  
A: No, it complements it. Numeric budgets (`green_policy.yaml`) + symbolic rules (`symbolic_policy.yaml`) work together.

**Q: Can I disable symbolic oversight?**  
A: Yes — don't create `symbolic_policy.yaml`. The engine uses minimal default rules.

**Q: How do I add Z3 solver support?**  
A: Not in scope for v1.x. See [What's Not in Scope](#whats-not-in-scope). The current engine is a **whitelist rule evaluator**, not an SMT solver.

**Q: Can rules reference historical data?**  
A: Yes, via **derived metrics** (see [Metric Reference](#derived--temporal-metrics-opt-in)): `fitness_delta_1h`, `carbon_trend`, `energy_variance`, etc. Ad-hoc temporal syntax like `offset 1h` is **not** supported.

**Q: How do I export violations for external analysis?**  
A: `engine.export_violations("violations.json")` or `visualizer.export_violation_report("report.json")`.

**Q: How do the advanced enhancements integrate with symbolic rules?**  
A: Enhancements supply additional metrics (MODP score, graph centrality, human feedback, etc.) usable in rule conditions. When a rule triggers an action, the system can call the enhanced modules (e.g. `select_routing_strategy()`) to adapt behavior.

**Q: Why "multi-layer" instead of "triple-layer"?**  
A: v1.1 added a fourth, optional layer (advanced enhancements). The old name is retained as a backward-compat alias.

**Q: Are my rules executed with `eval()`?**  
A: No. See [Security](#security).

---

## Migration Guide

### From v1.0 (numeric-only) to v1.2

| Before | After |
|---|---|
| Only `green_policy.yaml` | `green_policy.yaml` **+** `symbolic_policy.yaml` |
| `feedback.generate_dual_layer_feedback(pareto, reflections, metrics)` | `feedback.generate_multi_layer_feedback(pareto, reflections, metrics, symbolic_violations=..., enhanced_metrics=...)` |
| `use_enhancements=True` in config | `ENHANCEMENTS_ENABLED=true` env (or `--enhancements` CLI) |
| `evolutionary_best_fitness offset 1h` | `fitness_delta_1h` |
| `flexgen_energy_joules` | `flexgen_energy_kwh` |

Backward-compatible shims are provided for `generate_dual_layer_feedback`,
`MOE_GATING_ENABLED`, and `flexgen_energy_joules`. These will be removed in v2.0.

### From v1.1 to v1.2

- Metrics table expanded (core, context, enhancement, derived).
- Severity taxonomy formalized — dashboards may re-bucket some violations.
- Added action vocabulary — unknown actions now warn instead of silently no-op.
- Added dedup config — default `window_steps: 5`.

---

## Changelog

### 1.2.0 (2026-04-10)

**Added**
- Metric Reference table with units, ranges, and source.
- Rule Action Vocabulary table.
- Severity Taxonomy with numeric triggers.
- Rule Precedence & Lifecycle section.
- Deduplication & Rate Limiting section.
- Security section (whitelist parser guarantees).
- Reproduce with Docker section.
- Inspecting State with lean-ctx section.
- Configuration Matrix for enhancements.
- Migration Guide.
- Derived metrics (`fitness_delta_1h`, `carbon_trend`, `energy_variance`).
- `generate_multi_layer_feedback()` API.

**Changed**
- "Triple-Layer" → "Multi-Layer" (4 layers).
- FlexGen energy metric `flexgen_energy_joules` → `flexgen_energy_kwh`.
- `use_enhancements` config key → `ENHANCEMENTS_ENABLED` env (config key still accepted as alias).
- `MOE_GATING_ENABLED` → `MOE_ENABLED` (old name kept as alias).

**Fixed**
- `evolutionary_best_fitness offset 1h` → `fitness_delta_1h`.
- Documented `query_type` and `environment` as context metrics.
- Corrected unit-mismatch examples (`carbon > 60` is grams).
- Real demo output captured on a defined host.

**Deprecated**
- `generate_dual_layer_feedback()` → use `generate_multi_layer_feedback()`.
- `flexgen_energy_joules` → use `flexgen_energy_kwh`.
- `use_enhancements` config key → use `ENHANCEMENTS_ENABLED`.

### 1.1.0

- Added advanced enhancement metrics to rule conditions.
- Added enhancement integration sections.

### 1.0.0

- Initial symbolic oversight layer.

---

## What's Not in Scope

- **Z3 / SMT solving** — the engine is a rule evaluator, not a solver.
- **Temporal logic operators** (`always`, `eventually`, `until`) — use derived metrics instead.
- **Cross-run state** — rules see current run only; persistence is left to the operator.
- **Arbitrary Python in conditions** — by design (see [Security](#security)).

---

## Contributing

To contribute enhancements:

1. Fork the Green_Agent repository
2. Add/modify rules in `symbolic_policy.yaml`
3. Extend `SymbolicReasoningEngine` if needed
4. Add tests in `test_symbolic_oversight.py`
5. Update documentation (this file + `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`)
6. Submit PR

### Adding a new action

1. Register it in `ACTION_REGISTRY` inside `symbolic_reasoning_engine.py`.
2. Add a row to [Rule Action Vocabulary](#rule-action-vocabulary).
3. Add a test in `test_symbolic_oversight.py`.

### Adding a new metric

1. Add it to `_normalize_metrics()`.
2. Add a row to [Metric Reference](#metric-reference) with **unit** and **range**.
3. If enhancement-gated, add the enable flag to [Configuration Matrix](#configuration-matrix).

---

## License

Follows the same license as Green_Agent repository.

---

## References

- **FormalJudge**: Neuro-symbolic oversight paradigm
- **Green_Agent**: [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent)
- **Symbolic AI**: Rule-based reasoning systems
- **Multi‑Teacher Distillation**: Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE**: Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF**: Christiano et al., "Deep Reinforcement Learning from Human Preferences"

---

## Support

For issues or questions:

1. Check this README and `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` (deep dive into the
   reasoning loop, rule parser internals, and enhancement plumbing).
2. Run `python demo_symbolic_oversight.py` for examples.
3. Run `python test_symbolic_oversight.py` to verify installation.
4. Open issue on Green_Agent repository.

---

**Version**: 1.2.0  
**Last Updated**: 2026-11-10  
**Status**: Production Ready
