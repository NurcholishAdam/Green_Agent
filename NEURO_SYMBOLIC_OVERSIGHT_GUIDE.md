
# Neuro-Symbolic Oversight for Green Agent

| Field | Value |
|---|---|
| **Version** | 1.2.0 |
| **Last Updated** | 2026-11-10 |
| **Status** | Production Ready |
| **Audience** | Operators, integrators, rule authors |
| **Companion docs** | `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0) · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `.cursorrules` |

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Key Features](#key-features)
4. [Advanced Enhancements Integration](#advanced-enhancements-integration)
5. [Metric Reference](#metric-reference)
6. [Rule Schema](#rule-schema)
7. [Rule Action Vocabulary](#rule-action-vocabulary)
8. [Severity Taxonomy](#severity-taxonomy)
9. [Configuration Matrix](#configuration-matrix)
10. [Output Artifacts](#output-artifacts)
11. [Usage Guide](#usage-guide)
12. [Integration with Green_Agent Repository](#integration-with-green_agent-repository)
13. [Security](#security)
14. [Performance Considerations](#performance-considerations)
15. [Comparison with FormalJudge](#comparison-with-formaljudge)
16. [Examples](#examples)
17. [Troubleshooting](#troubleshooting)
18. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
19. [Reproduce with Docker](#reproduce-with-docker)
20. [Migration Guide (v1.0 → v1.2)](#migration-guide-v10--v12)
21. [What's Not in Scope](#whats-not-in-scope)
22. [Future Enhancements](#future-enhancements)
23. [Changelog](#changelog)
24. [Reviewer Checklist](#reviewer-checklist)
25. [References](#references)

---

## Overview

This enhancement extends the Green Agent architecture with a neuro-symbolic
oversight paradigm inspired by FormalJudge. It combines neural agent reasoning
with explicit symbolic constraints to provide interpretable, verifiable
governance over agent behavior.

**Delivery scope (v1.2.0)**:
- **Symbolic layer**: whitelist rule engine + 15 default rules + 7 enhanced rules (21 total)
- **Visualizer**: timeline + heatmap + enhancement overlay
- **Multi-layer feedback** (4 layers: objective, subjective, symbolic, advanced enhancements)
- **Advanced enhancement integration**: LIMIT Graph, MODP, RLHF, Multi-Teacher
  On-Policy Distillation, Bio-inspired Optimisation, MoE gating, FlexGen
- **Optional modules documented**: Zero Trust (`ZERO_TRUST_ENABLED`),
  Privacy (`DIFFERENTIAL_PRIVACY_ENABLED`), Training (`TRAINING_ENABLED`)

> **Version note (v1.2.0):** feedback is now **multi-layer** (4 layers).
> "Triple-layer" and "dual-layer" are retained only as backward-compat aliases —
> see [Migration Guide](#migration-guide-v10--v12).

---

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────┐
│                    Green Agent Core                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Metrics    │  │  Reflection  │  │    Pareto    │ │
│  │  Collector   │  │    Engine    │  │   Analyzer   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│            Symbolic Reasoning Layer                      │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Symbolic Reasoning Engine                       │  │
│  │  - Rule Parser & Evaluator (whitelist)           │  │
│  │  - Violation Trace Generator                     │  │
│  │  - Category-based Filtering                      │  │
│  │  - Domain-specific Rules                         │  │
│  │  - Enhanced Metric Evaluation                    │  │
│  └──────────────────────────────────────────────────┘  │
│                          ↓                               │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Symbolic Policy (symbolic_policy.yaml)          │  │
│  │  - Sustainability Rules                          │  │
│  │  - Resource Management Rules                     │  │
│  │  - Fairness Rules                                │  │
│  │  - Safety Rules                                  │  │
│  │  - Compliance Rules                              │  │
│  │  - Enhanced Rules (MODP, RLHF, Graph, FlexGen)   │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│              Enhanced Feedback System (v1.2)             │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Multi-Layer Feedback                            │  │
│  │  1. Objective (Pareto Analysis)                  │  │
│  │  2. Subjective (Agent Reflections)               │  │
│  │  3. Symbolic (Rule Violations)                   │  │
│  │  4. Advanced Decision Metrics (MODP, RLHF, …)    │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│              Dashboard & Visualization                   │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Symbolic Visualizer                             │  │
│  │  - Violation Timeline                            │  │
│  │  - Category Breakdown                            │  │
│  │  - Severity Heatmap                              │  │
│  │  - Trace Explanations                            │  │
│  │  - Enhanced Metrics Overlay (MODP, Graph)        │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## Key Features

### 1. Symbolic Rule Definition

Rules are defined in `symbolic_policy.yaml` with explicit logical constraints:

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon footprint combined with high latency"
```

**Rule Categories:**

| Category | Focus | Default count |
|---|---|---|
| **Sustainability** | Carbon, energy efficiency | 3 |
| **Resource** | Memory, CPU, latency | 3 |
| **Fairness** | Resource distribution, query equity | 2 |
| **Safety** | Error rates, system stability | 2 |
| **Compliance** | ESG standards, audit requirements | 2 |
| **Composite** | Multi-metric patterns | 2 |
| **Enhanced** | MODP score, RLHF feedback, LIMIT Graph metrics, FlexGen energy | 7 |
| **Total** | | **21** |

### 2. Symbolic Reasoning Engine

The engine evaluates rules against collected metrics:

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
violations = engine.evaluate_rules(metrics, step=current_step, domain=None)
```

**Features:**
- Lightweight rule parser (no Z3 / pyDatalog — pure Python)
- **Whitelist evaluator** (no `eval`/`exec`; see [§Security](#security))
- Modular rule loading
- Domain-specific rule support
- Support for enhanced metrics (`modp_score`, `graph_centrality`, …)

### 3. Formal Violation Traces

When rules are violated, formal traces are generated:

```
Rule: Carbon Threshold Violation (SUST-001)
Condition: carbon > 60 AND latency > 2000
Observations:
  carbon = 72.0
  latency = 2500
Violation: Rule triggered → flag_inefficiency
```

**Trace Components:**
- Rule identification
- Condition specification
- Observed metric values
- Action triggered
- Human-readable explanation

### 4. Integration with Existing Systems

#### Metrics Collection
```python
metrics = metrics_collector.get_metrics_for_reflection()
# If enhancements enabled, metrics includes:
#   modp_score, graph_centrality, human_feedback_score,
#   distillation_update_rate, moe_gate_stddev,
#   evolutionary_best_fitness, flexgen_energy_kwh
violations = symbolic_engine.evaluate_rules(metrics, step)
```

#### Multi-Layer Feedback (canonical)
```python
multi_feedback = feedback_system.generate_multi_layer_feedback(
    pareto_analysis=pareto_position,       # 1. objective
    reflections=agent_reflections,         # 2. subjective
    metrics=metrics,
    symbolic_violations=violations,        # 3. symbolic
    enhanced_metrics=enhanced_metrics,     # 4. advanced enhancements (optional)
)

# Deprecated alias (removed in v3.0)
# feedback_system.generate_dual_layer_feedback(...)
```

#### Dashboard Visualization
```python
symbolic_visualizer.add_violations([v.to_dict() for v in violations])
html_section = symbolic_visualizer.generate_dashboard_section()
symbolic_visualizer.export_violation_report("symbolic_violation_report.json")
```

### 5. Reflection Checkpoints

Symbolic rules are evaluated at:
- **Step completion**: After each query execution
- **Reflection checkpoints**: Aligned with agent self-reflection
- **Policy violations**: When numeric budgets are exceeded
- **Manual triggers**: On-demand evaluation via `engine.trigger_evaluation(...)`

### 6. Sustained Reflection

The system tracks violation patterns across runs:

```python
summary = engine.get_violation_summary()
# {
#   "total_violations": 15,
#   "by_category": {"sustainability": 8, "resource": 7},
#   "by_severity": {"critical": 3, "high": 5, "medium": 7},
#   "violation_rate": 0.75
# }
```

**Meta-insights generated:**
- "Agent consistently violates latency when conserving carbon"
- "Memory violations increase after step 5"
- "Fairness issues emerge with heterogeneous queries"

---

## Advanced Enhancements Integration

The neuro-symbolic oversight layer can use metrics and decisions from the
advanced enhancements folder (`src/enhancements`). When those modules are enabled,
the following integrations become available:

| Enhancement | Metric variable | Unit / Range | Enable flag | Integration with Symbolic Oversight |
|---|---|---|---|---|
| **LIMIT Graph** | `graph_centrality` | — / 0–1 | `LIMIT_GRAPH_ENABLED` | Low centrality may trigger topology review |
| **LIMIT Graph** | `graph_connectivity` | — / 0–1 | `LIMIT_GRAPH_ENABLED` | Connectivity drops trigger topology review |
| **MODP** | `modp_score` | — / 0–1 (higher better) | `MODP_ENABLED` | Score below threshold triggers policy review |
| **RLHF** | `human_feedback_score` | — / 0–1 (higher better) | `RLHF_ENABLED` | Weak feedback triggers feedback-collection action |
| **Multi-Teacher Distillation** | `distillation_update_rate` | ΔL/step / ≥ 0 | `DISTILLATION_ENABLED` | Stalled learning raises an alert |
| **Bio-inspired Optimisation** | `evolutionary_best_fitness` | — / 0–1 (higher better) | `EVOLUTIONARY_ENABLED` | Stagnation triggers restart or parameter tuning |
| **MoE Expert Gating** | `moe_gate_stddev` | — / 0–1 | `MOE_ENABLED` | High variance triggers routing investigation |
| **FlexGen** | `flexgen_energy_kwh` | kWh / ≥ 0 | `FLEXGEN_ENABLED` | Energy spike triggers precision switch |
| **Zero Trust** | trust state (via rules) | enum | `ZERO_TRUST_ENABLED` | Enables `block_request` action |
| **Privacy** | DP ε/δ budget | float | `DIFFERENTIAL_PRIVACY_ENABLED` | Budget exhaustion triggers compliance rule |
| **Training / Checkpoint** | checkpoint state | dict | `TRAINING_ENABLED` | Stale checkpoint triggers warning |

> **Unit note (v1.2):** the FlexGen energy metric is **`flexgen_energy_kwh`** (kWh).
> The earlier `flexgen_energy_joules` name is **deprecated** — see [Migration Guide](#migration-guide-v10--v12).

### Example Enhanced Rule

```yaml
symbolic_rules:
  - id: "ENH-MODP-001"
    name: "Low MODP Score Alert"
    category: "enhanced"
    priority: "medium"                   # NOT "warning"
    condition: "modp_score < 0.4"
    action: "trigger_policy_review"
    explanation: "Overall multi-objective performance is poor."
```

---

## Metric Reference

All variables usable in rule `condition` expressions. Units are canonical — the
engine does **not** silently convert.

### Core metrics

| Variable | Unit | Range |
|---|---|---|
| `energy` | Wh | ≥ 0 |
| `carbon` | g | ≥ 0 |
| `latency` | ms | ≥ 0 |
| `memory` | MB | ≥ 0 |
| `tool_calls` | count | ≥ 0 |
| `cpu_percent` | % | 0–100 |
| `cumulative.total_energy_wh` | Wh | ≥ 0 |
| `cumulative.total_carbon_kg` | kg | ≥ 0 |
| `cumulative.total_latency_ms` | ms | ≥ 0 |
| `cumulative.max_memory_mb` | MB | ≥ 0 |
| `cumulative.total_tool_calls` | count | ≥ 0 |
| `cumulative.step_count` | count | ≥ 0 |

### Context metrics

| Variable | Type | Values |
|---|---|---|
| `query_type` | enum | `research`, `production`, `debug`, `batch` |
| `environment` | enum | `dev`, `staging`, `production` |

### Enhancement metrics

See [Advanced Enhancements Integration](#advanced-enhancements-integration) table.

### Derived / temporal metrics

| Variable | Meaning |
|---|---|
| `fitness_delta_1h` | `evolutionary_best_fitness` now − 1h ago |
| `carbon_trend` | linear slope of `carbon` |
| `energy_variance` | variance of `energy` over last N steps |

---

## Rule Schema

Every rule must include these fields:

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | str | ✅ | Unique; prefix by category (`SUST-`, `RES-`, `ENH-`, …) |
| `name` | str | ✅ | Human-readable |
| `category` | enum | ✅ | One of the 7 categories + `custom` |
| `priority` | enum | ✅ | `critical` / `high` / `medium` / `low` |
| `condition` | str | ✅ | Whitelist expression (see [§Security](#security)) |
| `action` | enum | ✅ | See [Rule Action Vocabulary](#rule-action-vocabulary) |
| `explanation` | str | ✅ | Shown in trace + dashboard |
| `enabled` | bool | ❌ | Default `true`; `false` = skipped |
| `deprecated` | bool | ❌ | Default `false`; `true` = warn on evaluation |
| `supersedes` | str | ❌ | Old rule id marked deprecated |

---

## Rule Action Vocabulary

`action` must be one of these; unknown actions are logged and ignored.

| Action | Effect | Requires module |
|---|---|---|
| `flag_inefficiency` | Annotate trace | — |
| `optimize_strategy` | Call `NodeDescriptor.select_routing_strategy()` | enhancements |
| `check_topology` | Emit topology warning | `LIMIT_GRAPH_ENABLED` |
| `collect_more_feedback` | Enqueue feedback request | `RLHF_ENABLED` |
| `restart_evolution` | Reset evolutionary population | `EVOLUTIONARY_ENABLED` |
| `investigate_expert_routing` | Emit MoE stability alert | `MOE_ENABLED` |
| `switch_precision` | FlexGen precision switch | `FLEXGEN_ENABLED` |
| `optimize_research_strategy` | Domain tuning | — |
| `strict_carbon_enforcement` | Tighten carbon budget | — |
| `trigger_policy_review` | Escalate to review queue | — |
| `block_request` | Halt current request | `ZERO_TRUST_ENABLED` |

---

## Severity Taxonomy

`priority` maps to a numeric multiplier; the severity is derived from the
resulting score:

| Severity | Score range | Typical trigger |
|---|---|---|
| `critical` | ≥ 1.5 × threshold | hard budget exceeded |
| `high` | 1.2 – 1.5 × threshold | near-budget |
| `medium` | 1.0 – 1.2 × threshold | at threshold |
| `low` | < 1.0 × threshold | soft warning |

Priority multipliers: `critical=2.0`, `high=1.5`, `medium=1.0`, `low=0.5`.

> **Do not use `warning` as a `priority` value.** `warning` is a severity bucket only.

---

## Configuration Matrix

Env vars are **canonical**; YAML keys map onto them (env wins on conflict).

| Module | Env flag | YAML key | Metric |
|---|---|---|---|
| Symbolic | always on with `symbolic_policy.yaml` | — | violation traces |
| LIMIT Graph | `LIMIT_GRAPH_ENABLED` | `enhancements.limit_graph.enabled` | `graph_centrality`, `graph_connectivity` |
| MODP | `MODP_ENABLED` | `enhancements.modp.enabled` | `modp_score` |
| RLHF | `RLHF_ENABLED` | `enhancements.rlhf.enabled` | `human_feedback_score` |
| Distillation | `DISTILLATION_ENABLED` | `enhancements.distillation.enabled` | `distillation_update_rate` |
| MoE | `MOE_ENABLED` (alias `MOE_GATING_ENABLED`) | `enhancements.moe.enabled` | `moe_gate_stddev` |
| Evolutionary | `EVOLUTIONARY_ENABLED` | `enhancements.evolutionary.enabled` | `evolutionary_best_fitness` |
| FlexGen | `FLEXGEN_ENABLED` | `enhancements.flexgen.enabled` | `flexgen_energy_kwh` |
| Zero Trust | `ZERO_TRUST_ENABLED` | `enhancements.zero_trust.enabled` | trust state |
| Privacy | `DIFFERENTIAL_PRIVACY_ENABLED` | `enhancements.privacy.enabled` | DP ε/δ budget |
| Training | `TRAINING_ENABLED` | `enhancements.training.enabled` | checkpoint state |

---

## Output Artifacts

| # | File | Layer |
|---|---|---|
| 1 | `results.json` | Complete execution |
| 2 | `dashboard.html` | Visual |
| 3 | `metrics_history.json` | Timeline |
| 4 | `reflections.json` | Checkpoints |
| 5 | `reasoning_insights.json` | Long-context |
| 6 | `pareto_analysis.json` | Multi-objective |
| 7 | `run_memory.json` | Persistent |
| 8 | `dashboard_data.json` | Dashboard export |
| 9 | `symbolic_violations.json` | Symbolic — traces |
| 10 | `symbolic_violation_report.json` | Symbolic — summary |
| 11 | Enhanced fields in 1 + 4 | Advanced enhancements |

**`symbolic_violations.json` schema** (per entry):

| Field | Type | Description |
|---|---|---|
| `rule_id` | str | Rule id |
| `rule_name` | str | Human name |
| `category` | enum | Category |
| `severity` | enum | Derived severity |
| `priority` | enum | Configured priority |
| `condition` | str | Rule condition |
| `observations` | dict | Metric values at trigger time |
| `action_triggered` | enum | Action dispatched |
| `explanation` | str | Human explanation |
| `step` | int | Step number |
| `timestamp` | ISO 8601 | Trigger time |

---

## Usage Guide

### Basic Usage

1. **Define symbolic rules** in `symbolic_policy.yaml`
2. **Run agent** with symbolic oversight enabled:

```bash
python run_agent.py --config example_config.json --policy green_policy.yaml
```

3. **Review violations** in outputs:
   - `symbolic_violations.json` — Detailed traces
   - `symbolic_violation_report.json` — Summary report
   - `dashboard.html` — Visual dashboard

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
    explanation: "Custom constraint for my use case"
```

**Supported Operators:**
- Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
- Logical: `AND`, `OR`, `NOT`
- Grouping: `(`, `)`
- **Variables**: see [Metric Reference](#metric-reference) — core, context, enhancement, derived

> **Context metrics** (`query_type`, `environment`) require either
> `engine.evaluate_rules(..., domain=...)` or explicit inclusion in `metrics`.

### Domain-Specific Rules

Add domain extensions:

```yaml
domain_extensions:
  research:
    - id: "RES-DOMAIN-001"
      condition: "query_type == 'research' AND tool_calls > 100"
      action: "optimize_research_strategy"
      explanation: "Research queries should use efficient strategies"
```

Activate with:
```python
violations = engine.evaluate_rules(metrics, step, domain="research")
```

### Filtering Violations

```python
# By category — via summary
summary = engine.get_violation_summary()
sustainability_count = summary["by_category"].get("sustainability", 0)

# By severity — client-side filter over traces
critical = [v for v in violations if v.severity == "critical"]

# Raw traces
traces = [v.to_dict() for v in violations]
```

> `filter_by_rule_type()` and `get_violations_by_category()` are **not** part of
> the v1.2 public API. Use `get_violation_summary()` or client-side filtering.

### Disabling Enhancements

To skip enhancement modules entirely:

```bash
ENHANCEMENTS_ENABLED=false python run_agent.py --config example_config.json
```

> The deprecated `use_enhancements=False` config key is accepted for one minor
> release — see [Migration Guide](#migration-guide-v10--v12).

---

## Integration with Green_Agent Repository

This implementation is designed to integrate seamlessly with the
[NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent) repository:

### File Structure

```
green_agent_repo/
├── symbolic_policy.yaml              # default + enhanced rules
├── config/
│   └── symbolic_policy.yaml          # (optional) config-map mirror
├── src/
│   ├── symbolic/                     # 4th layer
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py
│   ├── dashboard/
│   │   ├── green_dashboard.py
│   │   └── symbolic_visualizer.py
│   ├── policy/
│   │   ├── policy_engine.py
│   │   └── policy_feedback.py        # multi-layer feedback
│   ├── monitoring/
│   │   └── metrics_collector.py
│   ├── reflection/
│   │   ├── reflection_engine.py
│   │   └── long_context_reasoner.py
│   ├── memory/
│   │   └── run_memory.py
│   ├── analysis/
│   │   └── pareto_analyzer.py
│   └── enhancements/
│       ├── schemas/
│       │   ├── node_descriptor.py
│       │   ├── workload_descriptor.py
│       │   └── feedback_event.py
│       ├── zero_trust_architecture.py
│       └── core/
│           ├── graph_registry.py
│           ├── causal_graph.py
│           ├── dag_carbon_ledger.py
│           └── anomaly_detector.py
├── scripts/
│   ├── verify.sh
│   └── migrate.sh
├── tests/
│   ├── unit/
│   └── integration/
├── run_agent.py
├── demo_symbolic_oversight.py
├── test_symbolic_oversight.py        # 19 tests
├── Dockerfile
├── pyproject.toml
├── MANIFEST.sha256
├── .cursorrules
├── SYMBOLIC_OVERSIGHT_README.md
├── NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md
├── Meta-Cognitive Architecture for Green Agent.md
├── META_COGNITIVE_ENHANCEMENT_COMPLETE.md
├── NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md
└── REPOSITORY_REFACTORING_GUIDE.md
```

### Compatibility

- **No breaking changes** to existing Green_Agent functionality
- **Optional activation** — works with or without symbolic oversight
- **Backward compatible** with existing policy files
- **Modular design** — can be disabled by not shipping `symbolic_policy.yaml`
- **Advanced enhancements** are optional and gracefully skipped if modules are missing

### Test coverage

`test_symbolic_oversight.py` — **19 tests** (13 core + 6 enhancement,
`@unittest.skipUnless(ENHANCEMENTS_ENABLED)` for the enhancement block).

---

## Security

| Concern | Guarantee |
|---|---|
| Rule evaluation | **Whitelist parser** — no `eval`/`exec`, no attribute access, no function calls |
| Allowed tokens | identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| Unknown identifier | Logged as `UnknownMetric`, rule skipped (non-fatal) |
| YAML parsing | `yaml.safe_load` only |
| Action dispatch | Fixed vocabulary; unknown actions logged and ignored |
| `block_request` | Requires `ZERO_TRUST_ENABLED=true`; otherwise logged and ignored |
| Metric trust | Metrics may come from an untrusted executor; symbolic rules + numeric budgets are the mitigation |

---

## Performance Considerations

### Computational Overhead

Measured on a 4 vCPU / 16 GB host, Python 3.11.4:

| Operation | Time | Memory |
|---|---|---|
| Rule loading (15 rules) | ~5 ms | ~500 KB |
| Single evaluation (base, 15 rules) | ~8 ms | ~100 KB |
| **Enhanced evaluation (incl. MODP/RLHF/Graph)** | **~12 ms** | ~150 KB |
| Violation trace generation | ~2 ms | ~50 KB |
| Dashboard HTML generation (amortized) | ~50 ms | ~1 MB |
| Distillation inference (per decision) | <5 ms | <20 KB |
| **Per-step overhead (eval + trace)** | **~14 ms** | **~150 KB** |

> Enhanced evaluation is **+50 % vs base** (8 → 12 ms). "Typical rule sets" stay
> under **15 ms** per step even with all enhancement metrics enabled.

### Optimization Tips

1. **Limit rule complexity** — keep conditions simple
2. **Use priority ordering** — critical rules evaluated first
3. **Batch evaluations** — evaluate at reflection checkpoints only
4. **Prune history** — archive old violations periodically
5. **Disable enhancements when not needed** — set `ENHANCEMENTS_ENABLED=false` in env

---

## Comparison with FormalJudge

| Aspect | FormalJudge | This Implementation |
|---|---|---|
| **Symbolic Logic** | Theorem prover (Z3) | Whitelist evaluator |
| **Rule Language** | Formal logic syntax | YAML + whitelist expressions |
| **Integration** | Standalone system | Embedded in agent |
| **Overhead** | Higher (~100 ms+) | ~12 ms per eval |
| **Flexibility** | Formal proofs | Pragmatic constraints |
| **Advanced Metrics** | Not applicable | MODP, RLHF, LIMIT Graph, etc. |
| **Use Case** | Safety-critical systems | Sustainable AI agents |
| **Dependencies** | Z3, heavy libraries | Pure Python + optional enhancement modules |

---

## Examples

### Example 1: Detecting Inefficiency

```python
# Rule SUST-001: IF carbon > 60g AND latency > 2s THEN flag_inefficiency
metrics = {
    "carbon": 72,      # grams
    "latency": 2500,   # ms
    # ... other metrics
}

violations = engine.evaluate_rules(metrics, step=5)
# Result: SUST-001 violation detected
```

### Example 2: Memory Alert

```python
# Rule RES-001: IF memory > 500MB THEN trigger_resource_alert
metrics = {
    "memory": 550,     # MB
    # ... other metrics
}

violations = engine.evaluate_rules(metrics, step=3)
# Result: RES-001 violation detected
```

### Example 3: Composite Rule

```python
# Rule COMPOSITE-001: IF (carbon > 1.5g OR energy > 4.0Wh) AND latency > 100s AND tool_calls > 30
metrics = {
    "carbon": 1.8,       # g
    "energy": 4.5,       # Wh
    "latency": 110000,   # ms
    "tool_calls": 35,
}

violations = engine.evaluate_rules(metrics, step=8)
# Result: COMPOSITE-001 violation detected
```

### Example 4: Enhanced Rule with MODP

```python
# Rule ENH-MODP-001: IF modp_score < 0.4 THEN trigger_policy_review
metrics = {
    "modp_score": 0.35,   # requires MODP_ENABLED=true
    # ... other metrics
}

violations = engine.evaluate_rules(metrics, step=10)
# Result: ENH-MODP-001 violation detected
```

### Example 5: Adding a rule (3 steps)

1. Add a block to `symbolic_policy.yaml` under `symbolic_rules:`.
2. Use a variable from [Metric Reference](#metric-reference) in `condition`.
3. Use an action from [Rule Action Vocabulary](#rule-action-vocabulary) in `action`.

---

## Troubleshooting

### Issue: Rules not triggering

**Solution:** Check units against [Metric Reference](#metric-reference):
- Carbon: **grams** (not kg) — but `cumulative.total_carbon_kg` is in **kg**
- Latency: **milliseconds**
- Memory: **MB**
- Enhanced scores (`modp_score`, `human_feedback_score`,
  `evolutionary_best_fitness`): **0–1, higher better**
- `graph_centrality` / `graph_connectivity`: **0–1**
- `flexgen_energy_kwh`: **kWh** (not joules)

### Issue: False positives

**Solution:** Adjust thresholds in `symbolic_policy.yaml`:
```yaml
condition: "carbon > 70"
```

### Issue: `priority: "warning"` is rejected

**Solution:** Use a valid priority — `critical` / `high` / `medium` / `low`.
`warning` is a severity, not a priority.

### Issue: Performance degradation

**Solution:** Reduce evaluation frequency:
```yaml
evaluation_config:
  evaluation_triggers:
    - "reflection_checkpoint"
```

### Issue: Enhanced metrics not available

**Solution:** Verify:
- `src/enhancements` exists
- `pip install -r requirements.txt` done
- `ENHANCEMENTS_ENABLED=true` and per-module flag set (e.g. `MODP_ENABLED=true`)
- Rule not disabled (`enabled: false`)

### Issue: `filter_by_rule_type` / `get_violations_by_category` do not exist

**Solution:** Use `get_violation_summary()` for aggregated counts, or filter
`violations` client-side.

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
docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .

docker run --rm -p 8000:8000 \
  -v "$PWD:/out" \
  -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
  green-agent:full \
  python demo_symbolic_oversight.py

docker run --rm -v "$PWD:/out" green-agent:full \
  python test_symbolic_oversight.py
```

---

## Migration Guide (v1.0 → v1.2)

| Before (v1.0) | After (v1.2) | Notes |
|---|---|---|
| "Triple-Layer" / "Dual-Layer" (docs) | "Multi-Layer" (4 layers) | Terminology only |
| `generate_dual_layer_feedback()` | `generate_multi_layer_feedback()` | Old name works (DeprecationWarning); removed in v3.0 |
| `flexgen_energy_joules` | `flexgen_energy_kwh` | Unit fix |
| `priority: "warning"` | `priority: "medium"` (or a severity field) | `warning` is a severity |
| `use_enhancements=False` | `ENHANCEMENTS_ENABLED=false` (env) | Deprecated alias kept |
| `filter_by_rule_type("fairness")` | `get_violation_summary()` + client filter | API fix |
| `get_violations_by_category(...)` | `get_violation_summary()["by_category"][...]` | API fix |
| `evolutionary_best_fitness offset 1h` (earlier draft) | `fitness_delta_1h` | Derived metric |

---

## What's Not in Scope

- **Z3 / SMT solving** — the engine is a whitelist rule evaluator, not a theorem
  prover. This is a **v1.x design constraint** (per Symbolic README v1.2 FAQ).
- **Temporal logic operators** (`always`, `eventually`, `until`) — use derived
  metrics (`fitness_delta_1h`, `carbon_trend`, `energy_variance`).
- **Arbitrary Python in conditions** — by design (see [§Security](#security)).
- **Cross-agent oversight** — planned (see Future #5).
- **Real-time human-in-the-loop** — planned.

---

## Future Enhancements

1. **Temporal Operators** — `ALWAYS`, `EVENTUALLY`, `UNTIL`; pattern detection
2. **Probabilistic Rules** — uncertainty handling; Bayesian reasoning
3. **Auto-tuning** — learn thresholds (evolutionary); adaptive rule weights
4. **Community rule packs** — shared domain rule sets
5. **Multi-agent Coordination** — cross-agent enforcement (MoE gating); distributed oversight

> Z3 integration is **not** on this list — see [What's Not in Scope](#whats-not-in-scope).

---

## Changelog

### 1.2.0 (2026-04-10)

**Added**
- Metadata block + Table of Contents
- §Metric Reference, §Rule Schema, §Rule Action Vocabulary, §Severity Taxonomy
- §Configuration Matrix, §Output Artifacts (11 items) with `symbolic_violations.json` schema
- §Security, §What's Not in Scope, §Migration Guide (v1.0 → v1.2)
- §lean-ctx inspection examples, §Docker reproduction block
- §Reviewer Checklist, §Changelog
- `ZERO_TRUST_ENABLED`, `DIFFERENTIAL_PRIVACY_ENABLED`, `TRAINING_ENABLED` to enhancement table
- `dag_carbon_ledger.py`, `causal_graph.py`, `anomaly_detector.py`, `pyproject.toml`, `MANIFEST.sha256`, `test_symbolic_oversight.py` to file tree
- Companion-doc cross-links in header

**Changed**
- "Triple-Layer" → **"Multi-Layer"** (4 layers)
- `generate_dual_layer_feedback` → `generate_multi_layer_feedback`
- `flexgen_energy_joules` → **`flexgen_energy_kwh`**
- `priority: "warning"` → `priority: "medium"`
- `use_enhancements` → `ENHANCEMENTS_ENABLED`
- `filter_by_rule_type` / `get_violations_by_category` → `get_violation_summary()`
  + client-side filtering
- Performance "<10 ms including enhanced" → "~12 ms enhanced eval; ~14 ms
  eval + trace"
- Rule examples now reference defined IDs (`COMPOSITE-001`, `RES-001`)
- File tree reconciled with Refactor Guide v0.2
- Date format standardized to ISO 8601

**Fixed**
- Invalid `priority: "warning"` rejected in schema
- `query_type` / `environment` now documented under Metric Reference
- Z3 contradiction with Symbolic README v1.2 resolved (removed from Future)

**Deprecated**
- `generate_dual_layer_feedback()` → use `generate_multi_layer_feedback()`
- `use_enhancements` → use `ENHANCEMENTS_ENABLED`
- `flexgen_energy_joules` → use `flexgen_energy_kwh`

### 1.0.0

- Initial symbolic oversight layer.

---

## Reviewer Checklist

- [ ] §Architecture diagram label says **Multi-Layer** and lists 4 layers.
- [ ] `generate_multi_layer_feedback()` used in §Key Features #4; deprecated alias only in Migration row.
- [ ] `flexgen_energy_kwh` used everywhere; no `flexgen_energy_joules` remains except Migration.
- [ ] No `priority: "warning"` remains; §Severity Taxonomy clarifies the distinction.
- [ ] `ENHANCEMENTS_ENABLED` used in §Optimization Tips #5 and §Disabling Enhancements.
- [ ] All rule IDs referenced in §Examples (`SUST-001`, `RES-001`, `COMPOSITE-001`, `ENH-MODP-001`) are defined in §Rule Categories or examples.
- [ ] `symbolic_violations.json` + `symbolic_violation_report.json` produced on full run.
- [ ] `test_symbolic_oversight.py` passes 19/19 in CI (ENHANCEMENTS_ENABLED=true).
- [ ] File tree matches `git ls-files`.
- [ ] Cross-links to companion docs resolve.
- [ ] `ZERO_TRUST_ENABLED`, `DIFFERENTIAL_PRIVACY_ENABLED`, `TRAINING_ENABLED` present in Dockerfile + §Configuration Matrix.
- [ ] No Z3 "planned" claim — matches "not in scope for v1.x" in Symbolic README v1.2.
- [ ] `filter_by_rule_type` / `get_violations_by_category` no longer documented as if they exist.
- [ ] Date `2026-04-10` matches companion docs.

---

## References

- FormalJudge — neuro-symbolic oversight paradigm
- Green_Agent — [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent)
- Symbolic AI — rule-based reasoning systems
- Interpretable AI — explainable decision-making
- **Multi-Teacher Distillation** — Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE** — Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF** — Christiano et al., "Deep Reinforcement Learning from Human Preferences"

**Companion docs:**
- `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0)
- `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`
- `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0)
- `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0)
- `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0)
- `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0)
- `.cursorrules` — lean-ctx read modes

---

## License

Follows the same license as the Green_Agent repository.

## Contributing

To add new rule categories or enhance the symbolic engine:

1. Define rules in `symbolic_policy.yaml`
2. Extend `SymbolicReasoningEngine` if needed
3. Add visualization support in `SymbolicVisualizer`
4. Update this documentation (and companion docs)
5. Submit PR to Green_Agent repository

---

**Note:** This implementation is inspired by FormalJudge but adapted for
practical integration with Green_Agent. It prioritizes pragmatic oversight over
formal verification, making it suitable for sustainable AI agent development.

**Last Updated**: 2026-04-10  
**Version**: 1.2.0  
**Test Coverage**: 19/19 (13 core + 6 enhancement, `skipUnless` when modules absent)  
**Performance Impact**: ~14 ms per step (eval + trace; dashboard amortized)  
**Breaking Changes**: None
