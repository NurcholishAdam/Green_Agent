
# Meta-Cognitive Architecture for Green Agent

| Field | Value |
|---|---|
| **Version** | 1.2.0 |
| **Last Updated** | 2026-04-10 |
| **Status** | Production Ready |
| **Audience** | Operators, integrators, rule authors, enhancement maintainers |
| **Related docs** | `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `REPOSITORY_REFACTORING_GUIDE.md` · `.cursorrules` |

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Enhancements](#architecture-enhancements)
   - 1. [Strengthened Metric Collection](#1-strengthened-metric-collection)
   - 2. [Reflection Checkpoints](#2-reflection-checkpoints)
   - 3. [Long-Context Reasoning Backend](#3-long-context-reasoning-backend)
   - 4. [Multi-Layer Self-Reflection Narratives](#4-multi-layer-self-reflection-narratives)
   - 5. [Adaptive Policy Integration](#5-adaptive-policy-integration)
   - 6. [Dashboard / Leaderboard Upgrade](#6-dashboard--leaderboard-upgrade)
   - 7. [Sustained Reflection Across Runs](#7-sustained-reflection-across-runs)
   - 8. [Iterative Testing Framework](#8-iterative-testing-framework)
3. [Neuro-Symbolic Oversight (4th Layer)](#3-neuro-symbolic-oversight-4th-layer)
4. [Advanced Enhancement Modules Integration](#4-advanced-enhancement-modules-integration)
5. [Zero Trust & DAG Carbon Ledger](#5-zero-trust--dag-carbon-ledger)
6. [Data Flow](#6-data-flow)
7. [Metric Reference](#7-metric-reference)
8. [Severity Taxonomy (Reflection Decisions)](#8-severity-taxonomy-reflection-decisions)
9. [Interpretability Score](#9-interpretability-score)
10. [Enhancement Compatibility Matrix](#10-enhancement-compatibility-matrix)
11. [Configuration](#11-configuration)
12. [Usage](#12-usage)
13. [Benefits](#13-benefits)
14. [Security](#14-security)
15. [Inspecting State with lean-ctx](#15-inspecting-state-with-lean-ctx)
16. [Reproduce with Docker](#16-reproduce-with-docker)
17. [Migration Guide](#17-migration-guide)
18. [What's Not in Scope](#18-whats-not-in-scope)
19. [Future Extensions](#19-future-extensions)
20. [Changelog](#20-changelog)
21. [References](#21-references)

---

## Overview

This document describes the enhanced meta-cognitive architecture that adds sustained
reflection and interpretability to the Green Agent system.

The architecture integrates advanced enhancement modules from `src/enhancements/` —
**LIMIT Graph**, **MODP (Multi-Objective Decision Process)**, **RLHF (Reinforcement
Learning from Human Feedback)**, **Multi-Teacher On-Policy Distillation**,
**Bio-inspired Optimisation**, **MoE expert gating**, and **FlexGen execution
backend** — alongside the **Neuro-Symbolic Oversight** layer. Together they
strengthen decision-making, sustainability, and security, and can be used with or
without the meta-cognitive features.

> **Version note (v1.2.0):** feedback is now **multi-layer** (4 layers:
> Objective, Subjective, Symbolic, Advanced Enhancements). The previous
> "dual-layer" terminology is retained only as a backward-compat alias — see
> [Migration Guide](#17-migration-guide).

---

## Architecture Enhancements

### 1. Strengthened Metric Collection

**Module**: `src/monitoring/metrics_collector.py`

**Features**:
- Consistent logging of all resource metrics (latency, energy, carbon, memory, tool calls)
- Real-time monitoring hooks for mid-execution access
- Cumulative metrics tracking across execution steps
- Enhanced metrics pass-through (`modp_score`, `graph_centrality`,
  `human_feedback_score`, distillation stats, `moe_gate_stddev`,
  `evolutionary_best_fitness`, `flexgen_energy_kwh`) — see [Metric Reference](#7-metric-reference)
- Export capabilities for post-execution analysis

**Key Methods**:
- `start_step()`: Mark beginning of execution step
- `record_tool_call()`: Track tool usage
- `collect_snapshot()`: Capture point-in-time metrics
- `get_current_metrics()`: Real-time metric access
- `get_metrics_for_reflection()`: Formatted metrics for reflection

### 2. Reflection Checkpoints

**Module**: `src/reflection/reflection_engine.py`

**Features**:
- Periodic reflection checkpoints (configurable frequency)
- Budget status analysis against policy constraints
- Self-explanation generation in natural language
- Decision determination based on current state
- Confidence scoring for agent strategies
- Pattern identification across reflections
- Symbolic rule evaluation (delegates to `SymbolicReasoningEngine`)

**Reflection Process**:
1. Agent pauses at checkpoint (every N steps)
2. Reviews current metrics vs. budget
3. Generates self-explanation: "I'm exceeding latency but conserving energy"
4. Determines adaptive decision (see [Severity Taxonomy](#8-severity-taxonomy-reflection-decisions))
5. Evaluates symbolic rules (4th layer)
6. Stores structured reflection log

**Example Reflection**:
```json
{
  "step": 5,
  "self_explanation": "⚡ I'm approaching budget limits with 1 warning(s). Energy usage is high at 82.3% of budget. I'm trading speed for energy efficiency.",
  "decision": "reduce_tool_calls",
  "confidence": 0.75,
  "budget_status": {
    "violations": [],
    "warnings": ["Energy high: 82.3%"],
    "utilization": {"energy": 82.3, "carbon": 65.1, "latency": 45.2}
  },
  "symbolic_violations": [
    {
      "rule_id": "ENH-MODP-001",
      "severity": "high",
      "action_triggered": "trigger_policy_review",
      "explanation": "MODP composite 0.36 < 0.4 threshold"
    }
  ]
}
```

### 3. Long-Context Reasoning Backend

**Module**: `src/reflection/long_context_reasoner.py`

**Features**:
- Compare current run with past runs
- Identify patterns: "This strategy consistently trades off carbon for speed"
- Suggest adaptive strategies mid-execution
- Track performance trends over time
- Generate insights with confidence scores

**Reasoning Insights**:
- **Comparison**: Energy efficiency vs. historical average
- **Pattern**: Consistent trade-off preferences
- **Trend**: Performance improvement or degradation
- **Suggestion**: Adaptive actions based on analysis

**Example Insight**:
```json
{
  "insight_type": "pattern",
  "description": "Agent consistently prioritizes energy conservation over speed (80% of runs)",
  "confidence": 0.88,
  "suggested_action": "continue"
}
```

### 4. Multi-Layer Self-Reflection Narratives

> **Renamed from "Dual-Layer" in v1.2.0.** Feedback now includes four layers.

**Module**: `src/policy/policy_feedback.py`

**Layers**:
1. **Objective**: Pareto analysis, efficiency scores
2. **Subjective**: Agent self-reflections, confidence trends
3. **Symbolic**: Rule violations from `SymbolicReasoningEngine` (see [§3](#3-neuro-symbolic-oversight-4th-layer))
4. **Advanced Enhancements**: MODP, RLHF, graph, distillation, MoE, evolutionary, FlexGen (optional)

**Synthesis**: alignment scoring across all four layers + actionable recommendations.

**Multi-Layer Feedback Structure**:
```json
{
  "objective_layer": {
    "pareto_position": "frontier",
    "efficiency_score": 0.85,
    "interpretation": "✅ This agent is on the Pareto frontier"
  },
  "subjective_layer": {
    "narrative": "✅ I'm operating within all budget constraints...",
    "avg_confidence": 0.82,
    "decision_pattern": "continue"
  },
  "symbolic_layer": {
    "violation_count": 0,
    "critical_count": 0,
    "top_rule": null
  },
  "enhanced_layer": {
    "modp_score": 0.72,
    "graph_centrality": 0.61,
    "human_feedback_score": 0.58,
    "moe_gate_stddev": 0.08
  },
  "synthesis": {
    "alignment": "strongly_aligned",
    "synthesis_text": "Agent's self-assessment aligns with objective performance and symbolic rules",
    "recommendations": ["Continue current strategy"]
  }
}
```

**API**:
```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()
result = feedback.generate_multi_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations,     # 3rd layer
    enhanced_metrics=enhanced_metrics,  # 4th layer (optional)
)

# Deprecated alias (removed in v3.0) — do not use in new code
result = feedback.generate_dual_layer_feedback(...)  # emits DeprecationWarning
```

### 5. Adaptive Policy Integration

**Module**: `src/policy/policy_engine.py`

**Features**:
- Meta-cognitive rules in policy configuration
- Reflection frequency configuration
- Self-adjustment thresholds
- Dynamic policy adjustments based on reflection outcomes
- Policy enforcement with violation tracking

**Adaptive Adjustments** (full vocabulary — see [Severity Taxonomy](#8-severity-taxonomy-reflection-decisions)):

| Decision | Effect |
|---|---|
| `continue` | No change |
| `reduce_tool_calls` | Cap tool usage per step (default × 0.8) |
| `reduce_energy_usage` | Tighten energy budget × 0.9 |
| `optimize_speed` | Relax latency budget × 1.1 |
| `reduce_memory_usage` | Tighten memory budget × 0.9 |
| `trigger_policy_review` | Escalate to human review queue |
| `block_request` | Halt the current request (requires `ZERO_TRUST_ENABLED`) |

### 6. Dashboard / Leaderboard Upgrade

**Module**: `src/dashboard/green_dashboard.py`

**Features**:
- Visualize reflective insights alongside metrics
- Show "why the agent chose this path"
- Compare agents on interpretability AND efficiency
- Interpretability scoring based on reflection quality (see [§9](#9-interpretability-score))
- HTML report generation with reasoning paths
- Symbolic violation overlays (via `SymbolicVisualizer`)

**Interpretability Score Components** (exact weights):

| Component | Weight | Definition |
|---|---|---|
| Reflection frequency | 30% | `min(1, observed / expected)` |
| Average confidence | 30% | `mean(confidence)` |
| Decision consistency | 20% | `1 − variance(decisions) / max_variance` |
| Explanation quality | 20% | `0.5 · length_norm + 0.5 · keyword_coverage` |

Full formula: see [§9](#9-interpretability-score).

**Leaderboard Rankings**:
- **Efficiency**: Pareto position + energy usage
- **Interpretability**: Reflection quality score
- **Sustainability**: Combined energy + carbon footprint

### 7. Sustained Reflection Across Runs

**Module**: `src/memory/run_memory.py`

**Features**:
- Persistent memory across multiple runs
- Performance trend analysis
- Meta-policy generation from historical data
- Long-term pattern identification
- Historical summary statistics

**Meta-Policy Generation**:
```json
{
  "generated_at": "2025-02-06T10:30:00",
  "based_on_runs": 10,
  "recommendations": [
    {
      "metric": "energy",
      "action": "tighten_energy_budget",
      "reason": "Energy usage is increasing over time"
    },
    {
      "metric": "overall",
      "action": "continue_current_strategy",
      "reason": "Overall performance is improving"
    }
  ]
}
```

### 8. Iterative Testing Framework

**Test file**: `tests/integration/test_meta_cognitive.py` (and `test_symbolic_oversight.py`
for the symbolic layer).

**Approach**:
- Compare baseline (no reflection) vs. meta-cognitive agents
- Measure improvements in:
  - Sustainability (energy, carbon reduction)
  - Interpretability (reflection quality, confidence)
  - Adaptability (policy adjustments, pattern learning)
- Use Pareto analysis to show reflection moves agents toward optimal trade-offs

**Run**:
```bash
python -m pytest tests/integration/test_meta_cognitive.py -v
python test_symbolic_oversight.py   # 19 tests (13 symbolic + 6 enhancement)
```

**Baseline-vs-enhanced harness**:
```python
# tests/integration/test_meta_cognitive.py
import pytest
from src.reflection.reflection_engine import ReflectionEngine
from src.policy.policy_engine import PolicyEngine

@pytest.mark.parametrize("reflection_frequency", [None, 5])
def test_sustainability_improvement(reflection_frequency):
    engine = ReflectionEngine(frequency=reflection_frequency)
    policy = PolicyEngine()
    result = run_agent(engine=engine, policy=policy, steps=30)
    assert result.energy_wh <= BASELINE_ENERGY_WH  # reflection must not regress
```

---

## 3. Neuro-Symbolic Oversight (4th Layer)

> **Previously missing from this document.** The symbolic layer is the third
> feedback layer between Subjective and Advanced Enhancements.

**Module**: `src/symbolic/symbolic_reasoning_engine.py`  
**Visualizer**: `src/dashboard/symbolic_visualizer.py`  
**Config**: `symbolic_policy.yaml`  
**Full docs**: `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0), `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`

**What it provides**: rule-based violation detection over the metrics emitted by
the collector. Rules are evaluated at every reflection checkpoint and at manual
triggers. Results flow into `PolicyFeedback.generate_multi_layer_feedback()` as
the `symbolic_violations` argument.

**Rule example**:
```yaml
symbolic_rules:
  - id: "ENH-GRAPH-001"
    name: "Topologically Peripheral Node"
    category: "topology"
    priority: "medium"
    condition: "graph_centrality < 0.3"
    action: "check_topology"
    explanation: "Node is topologically peripheral — may receive stale state"
```

**Rule categories supported** (see Symbolic README for full list):
sustainability, resource, fairness, safety, compliance, **enhanced** (MODP, graph,
RLHF, distillation, MoE, evolutionary, FlexGen, zero-trust, privacy, training).

**Security**: conditions are parsed by a whitelist evaluator — **no `eval`/`exec`**.
See Symbolic README §Security.

**Enable flag**: symbolic oversight is always on when `symbolic_policy.yaml` exists.
Related enhancement flags: `ZERO_TRUST_ENABLED`, `DIFFERENTIAL_PRIVACY_ENABLED`,
`TRAINING_ENABLED`.

---

## 4. Advanced Enhancement Modules Integration

The following advanced modules are integrated into the meta-cognitive architecture,
providing context-aware, learned decision-making and enhanced sustainability
enforcement.

### LIMIT Graph

**What it provides**: Topology-aware metrics (centrality, connectivity) that
indicate the system's importance within the overall graph. These metrics are used
to influence routing and priority decisions.

**Integration point**: `src/enhancements/core/graph_registry.py` and
`src/enhancements/core/causal_graph.py` maintain the graph state. The metrics
are made available to the reflection engine and policy feedback, enabling rules
like:

```yaml
- id: "ENH-GRAPH-001"
  condition: "graph_centrality < 0.3"
  action: "check_topology"
```

**Scale**: `graph_centrality ∈ [0, 1]`, `graph_connectivity ∈ [0, 1]`.

### MODP (Multi-Objective Decision Process)

**What it provides**: Configurable weights for **accuracy, energy, latency, and
carbon** (in that order). The composite `modp_score` evaluates overall performance
and triggers policy reviews when low.

**Integration point**: `NodeDescriptor` and `WorkloadDescriptor` compute the MODP
score using distillation. The score is included in the metrics collected at each
step, so reflection checkpoints can reference it:

```yaml
- id: "ENH-MODP-001"
  condition: "modp_score < 0.4"
  action: "trigger_policy_review"
```

**Scale**: `modp_score ∈ [0, 1]`, **higher = better**.

**Weights**:
```yaml
objective_weights: [0.4, 0.3, 0.2, 0.1]
# → [accuracy, energy, latency, carbon]
```

### RLHF (Reinforcement Learning from Human Feedback)

**What it provides**: Human feedback score (0–1) that influences teacher
predictions and reward shaping. This feedback can be used to adapt policies based
on user satisfaction.

**Integration point**: `RLHFTeacher` in descriptors; the `human_feedback_score`
is stored in `FeedbackEvent` and made available for symbolic rules.

**Scale**: `human_feedback_score ∈ [0, 1]`, **higher = better**.

### Multi-Teacher On-Policy Distillation with MoE Gating

**What it provides**: A lightweight student policy trained online from rule-based,
historical ML, Q-learning, and RLHF teachers, blended via a gating network.

**Integration point**: `DistillationRoutingOptimizer` and
`DistillationPriorityOptimizer` are used by `NodeDescriptor` and
`WorkloadDescriptor` respectively. During reflection checkpoints, if the current
strategy is suboptimal, the agent can call these optimizers to select a better
routing strategy or priority.

**Metric**: `distillation_update_rate` (ΔL/step, ≥ 0).

### Bio-inspired Optimisation (Evolutionary)

**What it provides**: Genetic algorithms that tune MODP weights and other
hyperparameters over time.

**Integration point**: `EvolutionaryOptimizer` classes in descriptors. When
enabled, the best fitness is tracked and can be used in rules to detect
stagnation.

**Scale**: `evolutionary_best_fitness ∈ [0, 1]`, **higher = better**.

**Derived metric** (use this, not ad-hoc temporal syntax):
`fitness_delta_1h = evolutionary_best_fitness(now) − evolutionary_best_fitness(1h ago)`.

```yaml
- id: "ENH-EVOL-001"
  condition: "fitness_delta_1h < 0.01"
  action: "restart_evolution"
```

> ⚠️ `evolutionary_best_fitness offset 1h` is **not valid syntax** — it was in
> v1.0/1.1 of this doc. Use `fitness_delta_1h` instead (Symbolic README v1.2).

### MoE Expert Gating

**What it provides**: Dynamic weighting of expert predictions; its stability
(`moe_gate_stddev`) is monitored.

**Integration point**: The gating network is part of the distillation optimizers.
The `moe_gate_stddev` metric is exported and can trigger alerts if the routing
becomes unstable.

**Scale**: `moe_gate_stddev ∈ [0, 1]` (0 = perfectly stable routing).

```yaml
- id: "ENH-MOE-001"
  condition: "moe_gate_stddev > 0.2"
  action: "investigate_expert_routing"
```

**Flag**: `MOE_ENABLED=true` (alias: `MOE_GATING_ENABLED`).

### FlexGen Integration

**What it provides**: High-throughput LLM inference with adaptive precision
(fp32/fp16/int8) for efficient execution.

**Integration point**: When `FLEXGEN_ENABLED=true`, the agent may delegate tasks
to FlexGen. The decision is made by the distillation policy based on context
(carbon intensity, task complexity). FlexGen energy consumption is tracked as
**`flexgen_energy_kwh`** (kWh) and can be used in rules:

```yaml
- id: "ENH-FLEX-001"
  condition: "flexgen_energy_kwh > 1.0"
  action: "switch_precision"
```

**Scale**: `flexgen_energy_kwh ≥ 0` (kWh, not joules).

> ⚠️ v1.0/1.1 of this doc used `flexgen_energy_joules > 1000` — that unit is
> **deprecated**. Use kWh (Symbolic README v1.2).

**Default**: `FLEXGEN_ENABLED=false` (opt-in).

---

## 5. Zero Trust & DAG Carbon Ledger

> **Previously missing.** The §Accountability benefit claims these; this section
> documents them.

**Modules**:
- `src/enhancements/zero_trust_architecture.py`
- `src/enhancements/core/dag_carbon_ledger.py`

**Enable flags**: `ZERO_TRUST_ENABLED=true`, `TRAINING_ENABLED=true` (for ledger
checkpointing).

**What they provide**:
- **Zero Trust**: trust score, attestation state, policy decisions (fed into
  symbolic rules; `block_request` action requires this module).
- **DAG Carbon Ledger**: immutable provenance of every carbon-affecting decision;
  backpropagates carbon attribution across the DAG.

**Metrics exposed**:
- Zero Trust: trust state via symbolic rules (no numeric metric in the base
  collector; use `action_triggered` in violation traces).
- DAG ledger: `cumulative.total_carbon_kg` cross-checksummed at each checkpoint.

**Related**: DIFFERENTIAL_PRIVACY (`DIFFERENTIAL_PRIVACY_ENABLED=true`) exposes
DP ε/δ budget consumption to rules under the compliance category.

---

## 6. Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                    Agent Execution                       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Metrics Collector (Real-time)               │
│  • Latency, Energy, Carbon, Memory, Tool Calls          │
│  • Cumulative tracking                                   │
│  • Mid-execution access                                  │
│  • Enhanced metrics: modp_score, graph_centrality,      │
│    human_feedback_score, distillation_update_rate,      │
│    moe_gate_stddev, evolutionary_best_fitness,          │
│    flexgen_energy_kwh                                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Reflection Checkpoint (Every N steps)          │
│  • Budget status analysis                                │
│  • Self-explanation generation                           │
│  • Decision determination                                │
│  • Confidence scoring                                    │
└────────┬────────────────────────────┬───────────────────┘
         │                            │
         ▼                            ▼
┌──────────────────────┐   ┌────────────────────────────────┐
│ Symbolic Reasoning   │   │  Policy Engine                 │
│ (4th layer)          │   │  • Check thresholds            │
│  • Rule evaluation   │   │  • Apply adjustments           │
│  • Violation traces  │   │  • Update budgets              │
│  • Action dispatch   │   │  • block_request (zero trust)  │
└──────────┬───────────┘   └────────────┬───────────────────┘
           │                            │
           ├────────────────────────────┤
           ▼                            ▼
┌─────────────────────────────────────────────────────────┐
│   Long-Context Reasoner                                 │
│  • Compare with past runs • Identify patterns           │
│  • Suggest adaptations                                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Advanced Enhancement Modules (optional)       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ LIMIT Graph  │  │     MODP     │  │     RLHF     │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Distillation │  │ Bio-inspired │  │     MoE      │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   FlexGen    │  │ Zero Trust   │  │ DAG Ledger   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  Run Memory (Persistent)                 │
│  • Store complete run history                            │
│  • Track performance trends                              │
│  • Generate meta-policies                                │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Pareto Analysis + Multi-Layer Feedback         │
│  1. Objective   (Pareto position)                        │
│  2. Subjective  (reflections)                            │
│  3. Symbolic    (violations)                             │
│  4. Enhancements(MODP, RLHF, graph, …)                  │
│  + Synthesis: alignment + recommendations                │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                    Dashboard Output                      │
│  • Leaderboard (efficiency + interpretability)           │
│  • Reasoning paths visualization                         │
│  • Comparative insights                                  │
│  • Enhanced metrics + symbolic violations overlay        │
└─────────────────────────────────────────────────────────┘
```

---

## 7. Metric Reference

Every metric usable in symbolic rules or referenced by reflection checkpoints.
Units are canonical — the engine does **not** silently convert.

### Core metrics (always available)

| Variable | Unit | Range | Source |
|---|---|---|---|
| `energy` | Wh | ≥ 0 | step metrics |
| `carbon` | g | ≥ 0 | step metrics |
| `latency` | ms | ≥ 0 | step metrics |
| `memory` | MB | ≥ 0 | step metrics |
| `tool_calls` | count | ≥ 0 | step metrics |
| `cpu_percent` | % | 0–100 | step metrics |
| `cumulative.total_energy_wh` | Wh | ≥ 0 | cumulative |
| `cumulative.total_carbon_kg` | kg | ≥ 0 | cumulative |
| `cumulative.total_latency_ms` | ms | ≥ 0 | cumulative |
| `cumulative.max_memory_mb` | MB | ≥ 0 | cumulative |
| `cumulative.total_tool_calls` | count | ≥ 0 | cumulative |
| `cumulative.step_count` | count | ≥ 0 | cumulative |

### Context metrics (request-scoped)

| Variable | Type | Values |
|---|---|---|
| `query_type` | enum | `research`, `production`, `debug`, `batch` |
| `environment` | enum | `dev`, `staging`, `production` |

### Enhancement metrics (available when module enabled)

| Variable | Unit | Range | Source module | Flag |
|---|---|---|---|---|
| `modp_score` | — | 0–1, **higher better** | `NodeDescriptor` | `MODP_ENABLED` |
| `graph_centrality` | — | 0–1 | `GraphRegistry` | `LIMIT_GRAPH_ENABLED` |
| `graph_connectivity` | — | 0–1 | `GraphRegistry` | `LIMIT_GRAPH_ENABLED` |
| `human_feedback_score` | — | 0–1, **higher better** | `FeedbackEvent` | `RLHF_ENABLED` |
| `distillation_update_rate` | ΔL/step | ≥ 0 | distillation optimizer | `DISTILLATION_ENABLED` |
| `moe_gate_stddev` | — | 0–1 (0 = stable) | MoE gate | `MOE_ENABLED` |
| `evolutionary_best_fitness` | — | 0–1, **higher better** | evolutionary optimizer | `EVOLUTIONARY_ENABLED` |
| `flexgen_energy_kwh` | kWh | ≥ 0 | FlexGen runtime | `FLEXGEN_ENABLED` |

### Derived / temporal metrics (opt-in)

| Variable | Unit | Meaning |
|---|---|---|
| `energy_variance` | Wh² | variance of `energy` over last N steps |
| `carbon_trend` | g/step | linear slope of `carbon` |
| `fitness_delta_1h` | — | `evolutionary_best_fitness` now − 1h ago |
| `human_feedback_delta_1h` | — | `human_feedback_score` now − 1h ago |

---

## 8. Severity Taxonomy (Reflection Decisions)

Reflection checkpoints select exactly one decision per checkpoint.

| Decision | Trigger (default) | Effect |
|---|---|---|
| `continue` | all metrics < 80% budget, no critical violations | no change |
| `reduce_tool_calls` | `tool_calls > 20` AND `energy > 70%` | cap tool usage × 0.8 |
| `reduce_energy_usage` | `energy > 80%` | tighten energy budget × 0.9 |
| `optimize_speed` | `latency > 80%` | relax latency budget × 1.1 |
| `reduce_memory_usage` | `memory > 80%` | tighten memory budget × 0.9 |
| `trigger_policy_review` | any `high` symbolic violation OR `modp_score < 0.4` | escalate to human review |
| `block_request` | any `critical` symbolic violation AND `ZERO_TRUST_ENABLED` | halt current request |

**Precedence**: `block_request` > `trigger_policy_review` > resource-specific >
`continue`.

---

## 9. Interpretability Score

The dashboard interpretability score is a weighted sum:

```
interpretability = 0.30·freq_norm
                 + 0.30·avg_conf
                 + 0.20·consistency
                 + 0.20·explanation_quality
```

Where:

| Term | Definition | Range |
|---|---|---|
| `freq_norm` | `min(1, observed_reflections / expected_reflections)`; `expected_reflections = total_steps / reflection_frequency` | [0, 1] |
| `avg_conf` | `mean(confidence across reflections)` | [0, 1] |
| `consistency` | `1 − variance(decisions) / max_variance` | [0, 1] |
| `explanation_quality` | `0.5 · length_norm + 0.5 · keyword_coverage`; `length_norm = min(1, len(explanation) / 200)`, `keyword_coverage = matched_domain_keywords / total_domain_keywords` | [0, 1] |

**Example** (from the sample reflection above):
```
freq_norm         = min(1, 3/3) = 1.00
avg_conf          = 0.75
consistency       = 0.90
explanation_quality = 0.68
interpretability = 0.30·1.00 + 0.30·0.75 + 0.20·0.90 + 0.20·0.68
                 = 0.30 + 0.225 + 0.18 + 0.136 = 0.841
```

---

## 10. Enhancement Compatibility Matrix

Env vars are **canonical**; YAML keys map onto them (env wins on conflict).

| Module | Env flag (canonical) | YAML key | Metric exposed | Default |
|---|---|---|---|---|
| LIMIT Graph | `LIMIT_GRAPH_ENABLED` | `enhancements.limit_graph.enabled` | `graph_centrality`, `graph_connectivity` | `true` |
| MODP | `MODP_ENABLED` | `enhancements.modp.enabled` | `modp_score` | `true` |
| RLHF | `RLHF_ENABLED` | `enhancements.rlhf.enabled` | `human_feedback_score` | `true` |
| Distillation | `DISTILLATION_ENABLED` | `enhancements.distillation.enabled` | `distillation_update_rate` | `true` |
| MoE | `MOE_ENABLED` (alias `MOE_GATING_ENABLED`) | `enhancements.moe.enabled` | `moe_gate_stddev` | `true` |
| Evolutionary | `EVOLUTIONARY_ENABLED` | `enhancements.evolutionary.enabled` | `evolutionary_best_fitness` | `true` |
| FlexGen | `FLEXGEN_ENABLED` | `enhancements.flexgen.enabled` | `flexgen_energy_kwh` | `false` |
| Zero Trust | `ZERO_TRUST_ENABLED` | `enhancements.zero_trust.enabled` | trust state (via rules) | `false` |
| Privacy | `DIFFERENTIAL_PRIVACY_ENABLED` | `enhancements.privacy.enabled` | DP ε/δ budget | `false` |
| Training / Checkpoint | `TRAINING_ENABLED` | `enhancements.training.enabled` | checkpoint state | `false` |
| Symbolic | always on with `symbolic_policy.yaml` | — | violation traces | `true` if file present |

> **Precedence:** env wins over YAML. This matches the Dockerfile behavior.

---

## 11. Configuration

### Policy Configuration (`green_policy.yaml`)

```yaml
meta_cognitive:
  reflection_frequency: 5  # Reflect every 5 steps

  self_adjustment_thresholds:
    energy_threshold_pct: 80
    carbon_threshold_pct: 80
    latency_threshold_pct: 80
    memory_threshold_pct: 80

  adaptive_policy_enabled: true

  long_context:
    enabled: true
    history_window: 10
    min_confidence_threshold: 0.7

  sustained_reflection:
    enabled: true
    memory_persistence: true
    meta_policy_generation: true
```

### Enhanced Configuration (`green_policy.yaml` additions)

```yaml
enhancements:
  enabled: true                    # ↔ ENHANCEMENTS_ENABLED

  limit_graph:
    enabled: true                  # ↔ LIMIT_GRAPH_ENABLED
    graph_metrics:
      centrality: 0.7
      connectivity: 0.6

  modp:
    enabled: true                  # ↔ MODP_ENABLED
    objective_weights: [0.4, 0.3, 0.2, 0.1]  # [accuracy, energy, latency, carbon]

  rlhf:
    enabled: true                  # ↔ RLHF_ENABLED
    human_feedback_score: 0.6      # 0–1, higher better

  distillation:
    enabled: true                  # ↔ DISTILLATION_ENABLED
    use_moe_gating: true

  moe:
    enabled: true                  # ↔ MOE_ENABLED (NOT moe_expert)
    n_experts: 4

  evolutionary:
    enabled: true                  # ↔ EVOLUTIONARY_ENABLED (NOT bio_inspired)
    population_size: 20
    mutation_rate: 0.1

  flexgen:
    enabled: false                 # ↔ FLEXGEN_ENABLED (default off)
    model_name: "facebook/opt-6.7b"
    default_precision: "fp16"
    delegation_policy: "adaptive"

  zero_trust:
    enabled: false                 # ↔ ZERO_TRUST_ENABLED

  privacy:
    enabled: false                 # ↔ DIFFERENTIAL_PRIVACY_ENABLED

  training:
    enabled: false                 # ↔ TRAINING_ENABLED
    checkpoint_dir: "checkpoints"
```

---

## 12. Usage

### Basic Execution

```bash
python run_agent.py \
  --config config.json \
  --policy green_policy.yaml \
  --output results.json \
  --dashboard dashboard.html
```

### Execution with Advanced Enhancements

```bash
python run_agent.py --config config.json --policy green_policy.yaml --enhancements
```

> `--enhancements` is **sugar** for `ENHANCEMENTS_ENABLED=true`. Per-module flags
> still apply (e.g. `FLEXGEN_ENABLED=true` must be set separately if you want
> FlexGen). See [§10](#10-enhancement-compatibility-matrix).

### With Symbolic Oversight

```bash
python run_agent.py \
  --config config.json \
  --policy green_policy.yaml \
  --policy symbolic_policy.yaml \
  --enhancements
```

### Output Artifacts

| File | Contents |
|---|---|
| `results.json` | Complete execution results with reflections |
| `dashboard.html` | Interactive visualization |
| `metrics_history.json` | Detailed metrics timeline |
| `reflections.json` | All reflection checkpoints (see schema below) |
| `reasoning_insights.json` | Long-context analysis |
| `pareto_analysis.json` | Multi-objective evaluation |
| `run_memory.json` | Persistent historical data |
| `symbolic_violations.json` | Rule violation traces (3rd layer) |
| `symbolic_violation_report.json` | Summary report |
| Enhanced fields | If enhancements enabled, `results.json` and `reflections.json` gain `modp_score`, `graph_centrality`, `human_feedback_score`, `moe_gate_stddev`, `evolutionary_best_fitness`, `flexgen_energy_kwh` |

### `reflections.json` field reference

| Field | Type | Description |
|---|---|---|
| `step` | int | Step number of the checkpoint |
| `self_explanation` | str | Natural-language reflection |
| `decision` | enum | One of the decisions in [§8](#8-severity-taxonomy-reflection-decisions) |
| `confidence` | float ∈ [0,1] | Confidence in the decision |
| `budget_status.violations` | list | Budget violations |
| `budget_status.warnings` | list | Budget warnings |
| `budget_status.utilization` | dict[str, float] | Per-metric % of budget |
| `symbolic_violations` | list | Rule violations (3rd layer) |
| `enhanced_metrics` | dict | Enhancement metrics (4th layer, optional) |

---

## 13. Benefits

### 1. Sustainability
- Real-time awareness of resource usage
- Proactive budget management
- Adaptive optimization strategies
- Additional gains from MODP-driven multi-objective optimisation

### 2. Interpretability
- Clear reasoning paths
- Self-explanations in natural language
- Confidence scoring for transparency
- Enhanced by distillation/MoE providing traceable expert decisions

### 3. Adaptability
- Dynamic policy adjustments
- Learning from historical patterns
- Meta-policy generation
- RLHF and evolutionary optimisation allow continuous improvement

### 4. Accountability
- Multi-layer feedback (objective + subjective + symbolic + enhancements)
- Alignment verification across all four layers
- Comprehensive audit trails
- **DAG carbon ledger** provides immutable carbon provenance ([§5](#5-zero-trust--dag-carbon-ledger))
- **Zero Trust** provides attestation state and `block_request` enforcement ([§5](#5-zero-trust--dag-carbon-ledger))

---

## 14. Security

| Concern | Guarantee |
|---|---|
| Symbolic rule evaluation | Whitelist parser — **no `eval`/`exec`** (see Symbolic README §Security) |
| Metric trust boundary | Metrics may come from an untrusted executor; symbolic rules and numeric budgets are the mitigation |
| `block_request` action | Requires `ZERO_TRUST_ENABLED=true`; otherwise the action is logged and ignored |
| Reflection explanations | Generated locally; not sent to external services |
| Policy YAML | Loaded via `yaml.safe_load` only |
| Enhancements | Disabled by default except core (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary); FlexGen and privacy/zero-trust are opt-in |

---

## 15. Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available (see `.cursorrules`), prefer enhanced
read modes over full-file reads:

```python
# One-shot snapshot of the whole enhancement state
ctx_read("src/enhancements/", mode="digest")

# MODP score + MoE gate weights in a single round-trip
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")

# Topology + next-hop probabilities (explain routing)
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")

# Cost / energy slice
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost+energy")

# Safety / privacy posture
ctx_read("src/enhancements/core/safety.py", mode="safety")
ctx_read("src/enhancements/core/privacy.py", mode="privacy")
```

Token budget guidance and the full mode taxonomy live in `.cursorrules`.

---

## 16. Reproduce with Docker

The repo ships a multi-stage `Dockerfile` with non-root user, healthcheck, and
`ARG`-driven enhancement toggles.

```bash
# Full image (all enhancements on)
docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .

# Lean image (minimal footprint)
docker build \
  --build-arg ENHANCEMENTS_ENABLED=false \
  --build-arg MODP_ENABLED=false \
  --build-arg RLHF_ENABLED=false \
  -t green-agent:lean .

# Run with meta-cognitive + enhancements
docker run --rm -p 8000:8000 \
  -v "$PWD:/out" \
  -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
  green-agent:full \
  python run_agent.py --config example_config.json --policy green_policy.yaml
```

---

## 17. Migration Guide

### v1.0 → v1.2

| Before | After | Notes |
|---|---|---|
| `generate_dual_layer_feedback()` | `generate_multi_layer_feedback()` | Old name still works (DeprecationWarning); removed in v3.0 |
| "Triple-Layer" / "Dual-Layer" (docs) | "Multi-Layer" (4 layers) | Terminology change only |
| `flexgen_energy_joules > 1000` | `flexgen_energy_kwh > 1.0` | Unit correction (kWh) |
| `evolutionary_best_fitness offset 1h` | `fitness_delta_1h` | Invalid syntax → derived metric |
| `moe_expert.enabled` (YAML) | `MOE_ENABLED` (env) / `enhancements.moe.enabled` (YAML) | Flag rename |
| `bio_inspired.enabled` (YAML) | `EVOLUTIONARY_ENABLED` (env) / `enhancements.evolutionary.enabled` (YAML) | Flag rename |
| No symbolic layer section | [§3 Neuro-Symbolic Oversight](#3-neuro-symbolic-oversight-4th-layer) | New section |
| No zero trust / DAG ledger section | [§5 Zero Trust & DAG Carbon Ledger](#5-zero-trust--dag-carbon-ledger) | New section |
| `objective_weights` unlabeled | `[accuracy, energy, latency, carbon]` | Documented |

### Config key mapping

```python
# Legacy keys are accepted as aliases for one minor release
ALIASES = {
    "moe_expert.enabled":       "enhancements.moe.enabled",
    "bio_inspired.enabled":     "enhancements.evolutionary.enabled",
    "use_enhancements":         "enhancements.enabled",
    "flexgen_energy_joules":    "flexgen_energy_kwh",  # value / 3.6e6
}
```

---

## 18. What's Not in Scope

- **Z3 / SMT solving** — the symbolic layer is a rule evaluator, not a solver.
- **Temporal logic operators** (`always`, `eventually`, `until`) — use derived
  metrics (`fitness_delta_1h`, `carbon_trend`, `energy_variance`) instead.
- **Cross-agent reflection** — see [Future Extensions](#19-future-extensions) #1.
- **Arbitrary Python in rule conditions** — by design (see [§14 Security](#14-security)).
- **Real-time human-in-the-loop** — see [Future Extensions](#19-future-extensions) #5.

---

## 19. Future Extensions

1. **Multi-Agent Reflection**: Collaborative reflection across agent teams.
2. **Causal Analysis**: Identify causal relationships between decisions and outcomes.
3. **Counterfactual Reasoning**: "What if" analysis for alternative strategies.
4. **Federated Learning**: Share meta-policies across agent populations.
5. **Human-in-the-Loop**: Interactive reflection with human feedback.
6. **Adaptive tuning of reflection thresholds**: Use MODP to automatically tune
   `self_adjustment_thresholds`; use LIMIT Graph to prioritize critical nodes.

---

## 20. Changelog

### 1.2.0 (2026-04-10)

**Added**
- Metadata block and Table of Contents.
- **§3 Neuro-Symbolic Oversight** (previously missing 4th layer).
- **§5 Zero Trust & DAG Carbon Ledger** (previously claimed but undocumented).
- **§7 Metric Reference** (units, ranges, sources).
- **§8 Severity Taxonomy** (full reflection decision vocabulary).
- **§9 Interpretability Score** (exact formula).
- **§10 Enhancement Compatibility Matrix** (env ↔ YAML ↔ metric ↔ default).
- **§14 Security** section.
- **§15 lean-ctx** inspection examples.
- **§16 Docker** reproduction block.
- **§17 Migration Guide** (v1.0 → v1.2).
- **§18 What's Not in Scope**.
- `reflections.json` field reference.

**Changed**
- "Dual-Layer" → **"Multi-Layer"** (4 layers).
- `flexgen_energy_joules` → **`flexgen_energy_kwh`** (unit fix).
- `evolutionary_best_fitness offset 1h` → **`fitness_delta_1h`** (invalid syntax → derived metric).
- YAML keys reconciled with env vars: `moe_expert` → `moe`, `bio_inspired` → `evolutionary`.
- `objective_weights` now labeled `[accuracy, energy, latency, carbon]`.
- Enhancement scales documented (`modp_score` 0–1 higher better, etc.).
- Data-flow diagram updated with symbolic layer + zero trust + DAG ledger.
- §8 renamed "Iterative Testing Framework" now names `tests/integration/test_meta_cognitive.py`.
- "Future Extensions #6" reframed as adaptive tuning (was already listed).

**Fixed**
- Example "3 reflections" now consistent with `reflection_frequency: 5` (sample
  reflection body updated to include symbolic violations).
- §Benefits #4 no longer claims undocumented features — §5 now documents them.

### 1.1.0

- Added advanced enhancement module integration (LIMIT Graph, MODP, RLHF,
  distillation, bio-inspired, MoE, FlexGen).
- Added enhancements config block.

### 1.0.0

- Initial meta-cognitive architecture: metrics collector, reflection checkpoints,
  long-context reasoning, dual-layer feedback, adaptive policy, dashboard,
  run memory, testing framework.

---

## 21. References

- Green Agent Repository: https://github.com/NurcholishAdam/Green_Agent
- AgentBeats Platform: https://agentbeats.ai
- Pareto Optimization: Multi-objective decision making
- Meta-Cognitive AI: Self-aware agent architectures
- **Multi-Teacher Distillation**: Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE**: Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF**: Christiano et al., "Deep Reinforcement Learning from Human Preferences"

**Companion documents:**
- `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) — neuro-symbolic layer
- `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` — deep dive into reasoning loop and parser internals
- `REPOSITORY_REFACTORING_GUIDE.md` — src layout and import migration
- `.cursorrules` — lean-ctx enhanced read modes
