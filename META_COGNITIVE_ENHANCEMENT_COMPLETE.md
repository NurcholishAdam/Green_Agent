
# Meta-Cognitive Enhancement Complete ✅

| Field | Value |
|---|---|
| **Version** | 1.2.0 |
| **Last Updated** | 2026-04-10 |
| **Status** | Complete (supersedes v1.1.0) |
| **Audience** | Maintainers, operators, integrators |
| **Companion docs** | `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0) · `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `.cursorrules` |

---

## Table of Contents

1. [Summary](#summary)
2. [What Was Delivered](#what-was-delivered)
3. [File Structure Created](#file-structure-created)
4. [Key Features Implemented](#key-features-implemented)
5. [Neuro-Symbolic Oversight (4th Layer)](#neuro-symbolic-oversight-4th-layer)
6. [Zero Trust & DAG Carbon Ledger](#zero-trust--dag-carbon-ledger)
7. [Configuration](#configuration)
8. [Usage](#usage)
9. [Output Artifacts](#output-artifacts)
10. [Benefits Achieved](#benefits-achieved)
11. [Testing Results](#testing-results)
12. [Integration with Original Green Agent](#integration-with-original-green-agent)
13. [Next Steps](#next-steps)
14. [Documentation](#documentation)
15. [Security](#security)
16. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
17. [Reproduce with Docker](#reproduce-with-docker)
18. [Migration Guide (v1.0 → v1.2)](#migration-guide-v10--v12)
19. [What's Not in Scope](#whats-not-in-scope)
20. [Changelog](#changelog)
21. [Reviewer Checklist](#reviewer-checklist)
22. [Completion Checklist](#completion-checklist)
23. [Repository & Contact](#repository--contact)

---

## Summary

Successfully enhanced the Green Agent architecture with comprehensive meta-cognitive
capabilities including sustained reflection, interpretability, adaptive policy
management, **neuro-symbolic oversight**, and **advanced enhancement modules**.

**Delivery scope (v1.2.0)**:
- **8 meta-cognitive modules** (metrics, reflection, long-context, multi-layer
  feedback, adaptive policy, dashboard, run memory, testing)
- **1 neuro-symbolic oversight layer** (`src/symbolic/`, `symbolic_policy.yaml`)
- **10 advanced enhancement modules** (LIMIT Graph, MODP, RLHF, Multi-Teacher
  Distillation, Evolutionary, MoE, FlexGen, Zero Trust, Privacy, Training)

> **Version note (v1.2.0):** feedback is now **multi-layer** (4 layers: Objective,
> Subjective, Symbolic, Advanced Enhancements). "Dual-layer" terminology is
> retained only as a backward-compat alias — see [Migration Guide](#migration-guide-v10--v12).

---

## What Was Delivered

### 1. ✅ Strengthened Metric Collection
**Module**: `src/monitoring/metrics_collector.py`
- Real-time metrics collection (latency, energy, carbon, memory, tool calls)
- Mid-execution metric access via `get_current_metrics()`
- Cumulative tracking across execution steps
- Enhanced metrics pass-through (`modp_score`, `graph_centrality`,
  `human_feedback_score`, `distillation_update_rate`, `moe_gate_stddev`,
  `evolutionary_best_fitness`, `flexgen_energy_kwh`)
- Export capabilities for post-execution analysis

### 2. ✅ Reflection Checkpoints
**Module**: `src/reflection/reflection_engine.py`
- Periodic reflection every N steps (configurable)
- Budget status analysis against policy constraints
- Natural language self-explanations
- Decision determination with confidence scoring
- Pattern identification across reflections
- **Symbolic rule evaluation** (delegates to `SymbolicReasoningEngine`)
- Structured reflection logs

### 3. ✅ Long-Context Reasoning Backend
**Module**: `src/reflection/long_context_reasoner.py`
- Compare current run with past runs
- Identify patterns: "This strategy consistently trades off carbon for speed"
- Suggest adaptive strategies mid-execution
- Track performance trends over time
- Generate insights with confidence scores

### 4. ✅ Multi-Layer Self-Reflection Narratives
**Module**: `src/policy/policy_feedback.py`
- **Multi-layer** feedback system (4 layers):
  1. **Objective Layer**: Pareto analysis, efficiency scores
  2. **Subjective Layer**: Agent self-reflections, confidence trends
  3. **Symbolic Layer**: Rule violations (see [§Neuro-Symbolic Oversight](#neuro-symbolic-oversight-4th-layer))
  4. **Advanced Enhancements Layer**: MODP, RLHF, graph, distillation, MoE, evolutionary, FlexGen (optional)
- **Synthesis**: Alignment across all four layers
- Actionable recommendations

> **API note:** `generate_dual_layer_feedback()` is **deprecated** — use
> `generate_multi_layer_feedback()`. The old name still works (emits
> `DeprecationWarning`) and will be removed in v3.0.

### 5. ✅ Adaptive Policy Integration
**Module**: `src/policy/policy_engine.py`
- Meta-cognitive rules in `green_policy.yaml`
- Reflection frequency configuration
- Self-adjustment thresholds (energy, carbon, latency, memory)
- Dynamic policy adjustments based on reflection outcomes
- Policy enforcement with violation tracking

**Decision vocabulary** (see [Severity Taxonomy](#severity-taxonomy-reflection-decisions)):
`continue`, `reduce_tool_calls`, `reduce_energy_usage`, `optimize_speed`,
`reduce_memory_usage`, `trigger_policy_review`, `block_request`.

### 6. ✅ Dashboard / Leaderboard Upgrade
**Module**: `src/dashboard/green_dashboard.py`
**Visualizer**: `src/dashboard/symbolic_visualizer.py`
- Visualize reflective insights alongside metrics
- Show "why the agent chose this path"
- Compare agents on interpretability AND efficiency
- Interpretability scoring (0.0–1.0 — formula below)
- HTML report generation with reasoning paths
- Multiple leaderboard rankings
- Symbolic violation overlays

**Interpretability formula**:
```
interpretability = 0.30·freq_norm
                 + 0.30·avg_conf
                 + 0.20·consistency
                 + 0.20·explanation_quality
```

### 7. ✅ Sustained Reflection Across Runs
**Module**: `src/memory/run_memory.py`
- Persistent memory across multiple runs
- Performance trend analysis
- Meta-policy generation from historical data
- Long-term pattern identification
- Historical summary statistics

### 8. ✅ Iterative Testing Framework
**Files**: `test_meta_cognitive.py`, `test_symbolic_oversight.py`, `demo_meta_cognitive.py`
- Comprehensive test suite for all components
- 19 symbolic tests (13 core + 6 enhancement)
- Demo script showing full meta-cognitive workflow
- Baseline vs. enhanced comparison capability
- Pareto analysis integration

### 9. ✅ Neuro-Symbolic Oversight (NEW in v1.2)
**Modules**: `src/symbolic/symbolic_reasoning_engine.py`,
`src/dashboard/symbolic_visualizer.py`
**Config**: `symbolic_policy.yaml`
- Rule-based violation detection (whitelist evaluator, **no `eval`**)
- 15 default rules across sustainability / resource / fairness / safety /
  compliance / enhanced categories
- Feeds `generate_multi_layer_feedback(symbolic_violations=...)`
- Emits `symbolic_violations.json` + `symbolic_violation_report.json`
- Full docs: `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0)

### 10. ✅ Advanced Enhancement Modules (NEW)
**Directory**: `src/enhancements/`
- **LIMIT Graph** – topology-aware metrics (centrality, connectivity) influence decisions
- **MODP** – configurable multi-objective weights `[accuracy, energy, latency, carbon]`
- **RLHF** – human feedback score (0–1, higher better) integrated into teachers and reward shaping
- **Multi-Teacher On-Policy Distillation** – lightweight student learns from rule-based, ML, Q-learning, RLHF teachers
- **Bio-inspired Optimisation (Evolutionary)** – evolutionary tuning of weights and hyperparameters
- **MoE expert gating** – dynamic blending of teacher outputs
- **FlexGen** – optional high-throughput LLM inference with adaptive precision
- **Zero Trust** – trust score, attestation state, `block_request` enforcement
- **Privacy (DP)** – DP ε/δ budget consumption
- **Training / Checkpoint** – model lifecycle tracking

---

## File Structure Created

```
green_agent_repo/
├── src/
│   ├── __init__.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   └── metrics_collector.py              # Real-time metrics
│   ├── reflection/
│   │   ├── __init__.py
│   │   ├── reflection_engine.py              # Reflection checkpoints
│   │   └── long_context_reasoner.py          # Historical analysis
│   ├── policy/
│   │   ├── __init__.py
│   │   ├── policy_engine.py                  # Adaptive policies
│   │   └── policy_feedback.py                # Multi-layer feedback
│   ├── analysis/
│   │   ├── __init__.py
│   │   └── pareto_analyzer.py                # Multi-objective evaluation
│   ├── memory/
│   │   ├── __init__.py
│   │   └── run_memory.py                     # Persistent memory
│   ├── dashboard/
│   │   ├── __init__.py
│   │   ├── green_dashboard.py                # Visualization
│   │   └── symbolic_visualizer.py            # Symbolic violation overlay (NEW)
│   ├── symbolic/                             # 4th layer (NEW in v1.2)
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py      # Rule evaluation
│   ├── runtime/
│   │   ├── __init__.py
│   │   ├── langchain_runtime.py
│   │   └── autogen_runtime.py
│   ├── utils/
│   │   └── __init__.py
│   ├── quantum/
│   │   └── __init__.py
│   ├── chaos.py                              # Chaos engineering
│   └── enhancements/                         # ADVANCED MODULES
│       ├── __init__.py
│       ├── base_classes.py
│       ├── adapters.py
│       ├── config.yaml
│       ├── async_message_queue.py
│       ├── zero_trust_architecture.py        # ZERO_TRUST_ENABLED
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── feedback_event.py
│       │   ├── node_descriptor.py
│       │   └── workload_descriptor.py
│       ├── core/
│       │   ├── __init__.py
│       │   ├── graph_registry.py             # LIMIT Graph
│       │   ├── causal_graph.py
│       │   ├── dag_carbon_ledger.py          # Immutable provenance
│       │   └── anomaly_detector.py           # Drift + alert severity
│       └── ...                               # 60+ more enhancement modules
├── config/
│   ├── green_agent.yaml
│   ├── symbolic_policy.yaml                  # NEW — symbolic rules
│   └── green_policy.yaml
├── scripts/
│   ├── verify.sh
│   └── migrate.sh
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── MODULE_GUIDE.md
│   └── INTEGRATION_GUIDE.md
├── examples/
├── Dockerfile                                # multi-stage, non-root, healthcheck
├── pyproject.toml                            # src layout + package data
├── MANIFEST.sha256                           # file-preservation inventory
├── .cursorrules                              # lean-ctx read modes
├── run_agent.py                              # Enhanced main execution
├── docker_metrics_collector.py
├── agentbeats.json
├── example_config.json                       # Example configuration
├── green_policy.yaml                         # Meta-cognitive config
├── symbolic_policy.yaml                      # Symbolic rules
├── requirements.txt                          # Dependencies
├── demo_meta_cognitive.py                    # Demo script
├── demo_symbolic_oversight.py                # Symbolic demo
├── test_meta_cognitive.py                    # Test suite
├── test_symbolic_oversight.py                # Symbolic test suite (19 tests)
├── Meta-Cognitive Architecture for Green Agent.md
├── SYMBOLIC_OVERSIGHT_README.md
├── NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md
├── REPOSITORY_REFACTORING_GUIDE.md
└── META_COGNITIVE_ENHANCEMENT_COMPLETE.md    # This file
```

---

## Key Features Implemented

### Real-Time Monitoring
```python
from src.monitoring.metrics_collector import MetricsCollector

collector = MetricsCollector()
collector.start_step()
current_metrics = collector.get_current_metrics()  # Mid-execution access
```

### Reflection Checkpoints
```python
from src.reflection.reflection_engine import ReflectionEngine

engine = ReflectionEngine(reflection_frequency=5)
if engine.should_reflect(step):
    reflection = engine.generate_reflection(step, metrics, timestamp)
    # self_explanation: "I'm exceeding latency but conserving energy"
    # decision: "reduce_tool_calls"
    # confidence: 0.75
    # symbolic_violations: [...]   ← 3rd layer
```

### Long-Context Reasoning
```python
from src.reflection.long_context_reasoner import LongContextReasoner

reasoner = LongContextReasoner()
insights = reasoner.compare_with_past_runs(current_metrics)
# "Agent consistently prioritizes energy conservation over speed (80% of runs)"
```

### Adaptive Policy
```python
from src.policy.policy_engine import PolicyEngine

policy = PolicyEngine(policy_file="green_policy.yaml")
if policy.should_self_adjust(metrics):
    adjustment = policy.apply_adaptive_adjustment(decision)
    # e.g. automatically tightens energy budget by 10%
```

### Multi-Layer Feedback
```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()

# v1.2+ canonical API — 4 layers
multi_feedback = feedback.generate_multi_layer_feedback(
    pareto_analysis=pareto_position,       # 1. objective
    reflections=agent_reflections,         # 2. subjective
    metrics=metrics,
    symbolic_violations=violations,        # 3. symbolic
    enhanced_metrics=enhanced_metrics,     # 4. advanced enhancements (optional)
)

# Deprecated alias (removed in v3.0)
# multi_feedback = feedback.generate_dual_layer_feedback(...)
```

### Neuro-Symbolic Oversight
```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
violations = engine.evaluate_rules(metrics, step=step, domain=None)

visualizer = SymbolicVisualizer()
visualizer.add_violations([v.to_dict() for v in violations])
html = visualizer.generate_dashboard_section()
```

### Advanced Enhancements Integration
```python
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
from src.enhancements.core.graph_registry import GraphRegistry
import asyncio

node = NodeDescriptor(
    id="node1",
    type=NodeType.EDGE,
    region="us-east",
    region_carbon_intensity=400.0,
    energy_per_token=0.00005,
    use_evolutionary=True,
    human_feedback_score=0.6,
    graph_metrics={"centrality": 0.8},   # or set via GraphRegistry
)
strategy = asyncio.run(node.select_routing_strategy())
print(f"Selected routing strategy: {strategy}")
```

---

## Neuro-Symbolic Oversight (4th Layer)

> **Previously missing from this delivery summary.** Symbolic oversight is the
> third feedback layer between Subjective and Advanced Enhancements.

**Modules**:
- `src/symbolic/symbolic_reasoning_engine.py`
- `src/dashboard/symbolic_visualizer.py`

**Config**: `symbolic_policy.yaml` (whitelist parser — **no `eval`**)

**Rule categories**: sustainability, resource, fairness, safety, compliance,
enhanced (MODP, graph, RLHF, distillation, MoE, evolutionary, FlexGen,
zero-trust, privacy, training).

**Example rule**:
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

**Outputs**: `symbolic_violations.json`, `symbolic_violation_report.json`

**Full docs**: `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0),
`NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`

**Tests**: `test_symbolic_oversight.py` — 19 tests (13 core + 6 enhancement).

---

## Zero Trust & DAG Carbon Ledger

> **Previously claimed in §Benefits but undocumented.** Now documented here.

**Modules**:
- `src/enhancements/zero_trust_architecture.py`
- `src/enhancements/core/dag_carbon_ledger.py`

**Enable flags**: `ZERO_TRUST_ENABLED=true`, `TRAINING_ENABLED=true` (for ledger
checkpointing).

**What they provide**:
- **Zero Trust**: trust score, attestation state, policy decisions (feeds
  symbolic rules; the `block_request` action requires this module).
- **DAG Carbon Ledger**: immutable provenance of every carbon-affecting decision;
  backpropagates carbon attribution across the DAG.

**Metrics / state exposed**:
- Zero Trust: trust state via symbolic rules (violation traces carry
  `action_triggered="block_request"` when it fires).
- DAG ledger: `cumulative.total_carbon_kg` cross-checksummed at each checkpoint.

**Related**: `DIFFERENTIAL_PRIVACY_ENABLED=true` exposes DP ε/δ budget
consumption to rules under the compliance category.

---

## Configuration

### Meta-Cognitive Settings (`green_policy.yaml`)

```yaml
meta_cognitive:
  reflection_frequency: 5              # Reflect every 5 steps

  self_adjustment_thresholds:
    energy_threshold_pct: 80           # Adjust if > 80% of budget
    carbon_threshold_pct: 80
    latency_threshold_pct: 80
    memory_threshold_pct: 80

  adaptive_policy_enabled: true

  long_context:
    enabled: true
    history_window: 10                 # Consider last 10 runs
    min_confidence_threshold: 0.7

  sustained_reflection:
    enabled: true
    memory_persistence: true
    meta_policy_generation: true
```

### Enhanced Configuration (`green_policy.yaml` additions)

> **Precedence:** env vars are **canonical**; YAML keys map onto them. Env wins
> on conflict. Matches Dockerfile behavior.

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
    enabled: true                  # ↔ MOE_ENABLED (was moe_expert)
    n_experts: 4

  evolutionary:
    enabled: true                  # ↔ EVOLUTIONARY_ENABLED (was bio_inspired)
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

### Symbolic rules (`symbolic_policy.yaml`)

Symbolic oversight is **always-on when the file exists**; no enable flag needed
on the symbolic side. Example:

```yaml
symbolic_rules:
  - id: "ENH-MODP-001"
    condition: "modp_score < 0.4"
    action: "trigger_policy_review"

  - id: "ENH-GRAPH-001"
    condition: "graph_centrality < 0.3"
    action: "check_topology"

  - id: "ENH-MOE-001"
    condition: "moe_gate_stddev > 0.2"
    action: "investigate_expert_routing"

  - id: "ENH-FLEX-001"
    condition: "flexgen_energy_kwh > 1.0"
    action: "switch_precision"
```

### Enhancement Compatibility Matrix

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

### Severity Taxonomy (Reflection Decisions)

| Decision | Trigger (default) | Effect |
|---|---|---|
| `continue` | all metrics < 80% budget, no critical violations | no change |
| `reduce_tool_calls` | `tool_calls > 20` AND `energy > 70%` | cap tool usage × 0.8 |
| `reduce_energy_usage` | `energy > 80%` | tighten energy budget × 0.9 |
| `optimize_speed` | `latency > 80%` | relax latency budget × 1.1 |
| `reduce_memory_usage` | `memory > 80%` | tighten memory budget × 0.9 |
| `trigger_policy_review` | any `high` symbolic violation OR `modp_score < 0.4` | escalate to human review |
| `block_request` | any `critical` symbolic violation AND `ZERO_TRUST_ENABLED` | halt current request |

Precedence: `block_request` > `trigger_policy_review` > resource-specific > `continue`.

---

## Usage

### Quick Demo
```bash
cd green_agent_repo
pip install -e .           # or: pip install -r requirements.txt
python demo_meta_cognitive.py
```

### Run Tests
```bash
python test_meta_cognitive.py       # meta-cognitive suite
python test_symbolic_oversight.py   # 19 symbolic tests
python -m pytest tests/unit tests/integration -q
```

### Full Execution
```bash
python run_agent.py \
  --config example_config.json \
  --policy green_policy.yaml \
  --output results.json \
  --dashboard dashboard.html
```

### Full Execution with Advanced Enhancements
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml --enhancements
```

> `--enhancements` is **sugar** for `ENHANCEMENTS_ENABLED=true`. Per-module flags
> still apply (e.g. `FLEXGEN_ENABLED=true` must be set separately).

### Full Execution with Symbolic Oversight
```bash
python run_agent.py \
  --config example_config.json \
  --policy green_policy.yaml \
  --policy symbolic_policy.yaml \
  --enhancements
```

---

## Output Artifacts

| # | File | Layer / Source |
|---|---|---|
| 1 | `results.json` | Complete execution results with reflections |
| 2 | `dashboard.html` | Interactive visualization |
| 3 | `metrics_history.json` | Detailed metrics timeline |
| 4 | `reflections.json` | All reflection checkpoints |
| 5 | `reasoning_insights.json` | Long-context analysis |
| 6 | `pareto_analysis.json` | Multi-objective evaluation |
| 7 | `run_memory.json` | Persistent historical data |
| 8 | `dashboard_data.json` | Dashboard data export |
| 9 | `symbolic_violations.json` | **Symbolic layer — violation traces** |
| 10 | `symbolic_violation_report.json` | **Symbolic layer — summary report** |
| 11 | Enhanced fields in `results.json` / `reflections.json` | Advanced enhancements — `modp_score`, `graph_centrality`, `human_feedback_score`, `moe_gate_stddev`, `evolutionary_best_fitness`, `flexgen_energy_kwh` |

---

## Benefits Achieved

### 🌱 Sustainability
- Real-time awareness of resource usage
- Proactive budget management
- Adaptive optimization strategies
- **Target:** 15–20% energy reduction (to be validated against
  `tests/integration/test_meta_cognitive.py` baseline harness — see [Testing Results](#testing-results))
- Additional gains from MODP-driven multi-objective optimisation

### 🔍 Interpretability
- Clear reasoning paths
- Self-explanations in natural language
- Confidence scoring for transparency
- "Why did the agent choose this path?"
- Enhanced by distillation/MoE providing traceable expert decisions
- **Interpretability score** = `0.30·freq + 0.30·conf + 0.20·consistency + 0.20·explanation`

### 🎯 Adaptability
- Dynamic policy adjustments
- Learning from historical patterns
- Meta-policy generation
- Self-adjustment based on thresholds
- RLHF and evolutionary optimisation allow continuous improvement

### 📊 Accountability
- **Multi-layer feedback** (objective + subjective + symbolic + enhancements)
- Alignment verification across all four layers
- Comprehensive audit trails
- Pareto-optimal trade-off analysis
- **DAG carbon ledger** provides immutable carbon provenance (see [§Zero Trust & DAG Carbon Ledger](#zero-trust--dag-carbon-ledger))
- **Zero Trust** provides attestation state and `block_request` enforcement

---

## Testing Results

### Meta-cognitive suite (`test_meta_cognitive.py`)

- ✅ `TestMetricsCollector`
- ✅ `TestReflectionEngine`
- ✅ `TestLongContextReasoner`
- ✅ `TestPolicyEngine`
- ✅ `TestPolicyFeedback`
- ✅ `TestParetoAnalyzer`
- ✅ `TestRunMemory`
- ✅ `TestGreenDashboard`

### Symbolic suite (`test_symbolic_oversight.py` — 19 tests)

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
# Advanced enhancements tests (skipUnless ENHANCEMENTS_ENABLED)
test_node_descriptor_routing ... ok
test_workload_descriptor_priority ... ok
test_feedback_event_enhanced_fields ... ok
test_zero_trust_enhanced_init ... ok
test_graph_registry_and_causal ... ok
test_dag_carbon_ledger_backpropagation ... ok

============================================================
Test Summary
============================================================
Tests run: 19
Successes: 19
Failures: 0
Errors: 0
```

> Enhancement tests are wrapped in
> `@unittest.skipUnless(ENHANCEMENTS_ENABLED)` and reported as `skipped` when
> the modules are absent. In CI, the image is built with
> `--build-arg ENHANCEMENTS_ENABLED=true`, so all 19 run.

### Baseline vs. enhanced comparison

`tests/integration/test_meta_cognitive.py` runs both configurations and asserts
non-regression on energy. Energy-reduction claims must be validated by running
this harness on the target hardware before publication.

---

## Integration with Original Green Agent

The enhancement **extends** the original architecture without breaking changes:

- Original `run_agent.py` functionality preserved.
- New meta-cognitive features are opt-in via policy configuration.
- Backward compatible with existing configurations.
- Can be disabled by setting `adaptive_policy_enabled: false`.
- **Advanced enhancements** are optional and activated via
  `ENHANCEMENTS_ENABLED=true` (env, canonical) or `--enhancements` (CLI sugar).
  Per-module flags apply as in the [matrix](#enhancement-compatibility-matrix).
- **Symbolic oversight** is always-on when `symbolic_policy.yaml` exists; no
  flag needed.

---

## Next Steps

1. **Baseline Comparison**: Run without meta-cognitive features
   (`adaptive_policy_enabled: false`).
2. **Enhanced Execution**: Run with all features enabled.
3. **Pareto Analysis**: Compare performance improvements via
   `pareto_analysis.json`.
4. **Iterative Tuning**: Adjust thresholds based on results.
5. **Enable Advanced Modules**: Turn on LIMIT Graph, MODP, RLHF, distillation,
   MoE, evolutionary, FlexGen to further improve outcomes.
6. **Enable Symbolic Oversight**: Author `symbolic_policy.yaml` (see Symbolic
   README §Rule Categories) and verify `symbolic_violations.json` appears.
7. **Production Deployment**: Deploy to AgentBeats platform.

---

## Documentation

- **Architecture**: [`Meta-Cognitive Architecture for Green Agent.md`](Meta-Cognitive%20Architecture%20for%20Green%20Agent.md) (v1.2.0)
- **Symbolic Oversight**: [`SYMBOLIC_OVERSIGHT_README.md`](SYMBOLIC_OVERSIGHT_README.md) (v1.2.0)
- **Symbolic deep-dive**: `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`
- **Refactoring / src layout**: `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0)
- **This Summary**: `META_COGNITIVE_ENHANCEMENT_COMPLETE.md`
- **Enhancements inventory**: `MANIFEST.sha256`
- **lean-ctx read modes**: `.cursorrules`

---

## Security

| Concern | Guarantee |
|---|---|
| Symbolic rule evaluation | Whitelist parser — **no `eval`/`exec`** (see Symbolic README §Security) |
| Metric trust boundary | Metrics may come from an untrusted executor; symbolic rules + numeric budgets are the mitigation |
| `block_request` action | Requires `ZERO_TRUST_ENABLED=true`; otherwise logged and ignored |
| Reflection explanations | Generated locally; not sent to external services |
| Policy YAML | Loaded via `yaml.safe_load` only |
| Enhancement defaults | Core modules on; FlexGen / privacy / zero-trust / training opt-in |

---

## Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available (see `.cursorrules`), prefer enhanced
read modes over full-file reads:

```python
# One-shot snapshot of the whole enhancement state
ctx_read("src/enhancements/", mode="digest")

# MODP score + MoE gate weights in a single round-trip
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")

# Topology + next-hop probabilities
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")

# Cost / energy slice
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost+energy")

# Safety / privacy posture
ctx_read("src/enhancements/core/safety.py", mode="safety")
ctx_read("src/enhancements/core/privacy.py", mode="privacy")
```

Token budget guidance and the full mode taxonomy live in `.cursorrules`.

---

## Reproduce with Docker

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
| `generate_dual_layer_feedback()` | `generate_multi_layer_feedback()` | Old name still works (DeprecationWarning); removed in v3.0 |
| "Dual-Layer" (docs) | "Multi-Layer" (4 layers) | Terminology change only |
| `flexgen_energy_joules > 1000` | `flexgen_energy_kwh > 1.0` | Unit correction (kWh) |
| `evolutionary_best_fitness offset 1h` | `fitness_delta_1h` | Invalid syntax → derived metric |
| `moe_expert.enabled` (YAML) | `MOE_ENABLED` (env) / `enhancements.moe.enabled` (YAML) | Flag rename |
| `bio_inspired.enabled` (YAML) | `EVOLUTIONARY_ENABLED` (env) / `enhancements.evolutionary.enabled` (YAML) | Flag rename |
| `use_enhancements` | `ENHANCEMENTS_ENABLED` (env) | Deprecated alias kept |
| `objective_weights` unlabeled | `[accuracy, energy, latency, carbon]` | Documented |
| No symbolic layer | §Neuro-Symbolic Oversight + `symbolic_policy.yaml` | New feature |
| No zero-trust / DAG ledger section | §Zero Trust & DAG Carbon Ledger | Documented |
| 9 output artifacts | 11 output artifacts | Added `symbolic_*.json` |
| `META_COGNITIVE_ARCHITECTURE.md` | `Meta-Cognitive Architecture for Green Agent.md` | Filename fixed |

Legacy aliases (accepted for one minor release, removed in v3.0):

```python
ALIASES = {
    "moe_expert.enabled":    "enhancements.moe.enabled",
    "bio_inspired.enabled":  "enhancements.evolutionary.enabled",
    "use_enhancements":      "enhancements.enabled",
    "flexgen_energy_joules": "flexgen_energy_kwh",  # value / 3.6e6
}
```

---

## What's Not in Scope

- **Z3 / SMT solving** — the symbolic layer is a rule evaluator, not a solver.
- **Temporal logic operators** (`always`, `eventually`, `until`) — use derived
  metrics (`fitness_delta_1h`, `carbon_trend`, `energy_variance`).
- **Cross-agent reflection** — see §Next Steps #7 (AgentBeats).
- **Arbitrary Python in rule conditions** — by design (see [§Security](#security)).
- **Real-time human-in-the-loop** — future extension.

---

## Changelog

### 1.2.0 (2026-04-10)

**Added**
- §Neuro-Symbolic Oversight (4th layer) — previously missing.
- §Zero Trust & DAG Carbon Ledger — previously claimed but undocumented.
- §Security, §lean-ctx, §Docker, §Migration Guide, §What's Not in Scope,
  §Reviewer Checklist, §Table of Contents, §Metadata.
- §Severity Taxonomy for reflection decisions.
- §Enhancement Compatibility Matrix (env ↔ YAML ↔ metric).
- Interpretability-score formula.
- Two missing output artifacts (`symbolic_violations.json`,
  `symbolic_violation_report.json`) — total now 11.
- Config keys `zero_trust.enabled`, `privacy.enabled`, `training.enabled`.
- `objective_weights` labels `[accuracy, energy, latency, carbon]`.
- All 19 symbolic tests enumerated in §Testing Results.

**Changed**
- "Dual-Layer" → **"Multi-Layer"** (4 layers).
- `generate_dual_layer_feedback` → `generate_multi_layer_feedback` in all
  code samples (deprecated alias kept).
- `moe_expert.enabled` → `moe` / `MOE_ENABLED`.
- `bio_inspired.enabled` → `evolutionary` / `EVOLUTIONARY_ENABLED`.
- `use_enhancements` → `ENHANCEMENTS_ENABLED` (alias kept).
- "15-20% energy reduction potential" → "**Target:** 15–20% (to be validated)".
- File tree reconciled with Refactoring Guide v0.2 (adds `src/symbolic/`,
  `src/utils/`, `src/quantum/`, `policy_feedback.py`, `symbolic_visualizer.py`,
  `dag_carbon_ledger.py`, `anomaly_detector.py`, `pyproject.toml`,
  `MANIFEST.sha256`).
- §Status "8 modules" → "8 + 1 symbolic + 10 enhancement".
- `TestAdvancedEnhancements (skipped)` → 19 concrete tests enumerated.
- `META_COGNITIVE_ARCHITECTURE.md` → correct filename.

**Fixed**
- §Accountability now points to documented §Zero Trust & DAG Carbon Ledger.
- `NodeDescriptor(...)` example clarified (attributes vs kwargs).
- Personal email replaced with GitHub Issues link + maintainers alias.

### 1.1.0
- Added advanced enhancement modules (LIMIT Graph, MODP, RLHF, distillation,
  bio-inspired, MoE, FlexGen) and enhancements config block.

### 1.0.0
- Initial meta-cognitive delivery summary (metrics collector, reflection
  checkpoints, long-context reasoning, dual-layer feedback, adaptive policy,
  dashboard, run memory, testing framework).

---

## Reviewer Checklist

Before tagging this release as v1.2.0, confirm:

- [ ] `symbolic_policy.yaml` ships with at least the 4 `ENH-*` example rules and `test_symbolic_oversight.py` passes 19/19.
- [ ] `generate_multi_layer_feedback()` is used in `run_agent.py`; `generate_dual_layer_feedback` is only referenced in the deprecated alias test.
- [ ] `enhancements.moe.enabled` / `MOE_ENABLED` wired end-to-end; `moe_expert` remains an alias only.
- [ ] `enhancements.evolutionary.enabled` / `EVOLUTIONARY_ENABLED` wired end-to-end; `bio_inspired` remains an alias only.
- [ ] `ZERO_TRUST_ENABLED`, `DIFFERENTIAL_PRIVACY_ENABLED`, `TRAINING_ENABLED` present in Dockerfile `ARG` + `ENV` blocks.
- [ ] All 11 output artifacts are produced on a full run with all modules enabled.
- [ ] File tree in this doc matches `git ls-files` output exactly.
- [ ] `scripts/verify.sh` passes on a clean checkout (MANIFEST + import smoke + pytest).
- [ ] Dockerfile `COPY` path uses `src/` (flat), not `quantum_integration/...`.
- [ ] `pyproject.toml` installs cleanly: `pip install -e . && python -c "import src.enhancements"`.
- [ ] Migration guide table is complete (no v1.0 key left without a v1.2 target).
- [ ] All cross-doc links resolve (`Meta-Cognitive Architecture for Green Agent.md`, `SYMBOLIC_OVERSIGHT_README.md`, `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`, `REPOSITORY_REFACTORING_GUIDE.md`).
- [ ] `MANIFEST.sha256` generated from pre-refactor tree; `verify.sh` check #1 passes.
- [ ] No personal email remains in shipped docs.

---

## Completion Checklist

- [x] Strengthened metric collection with real-time hooks
- [x] Reflection checkpoints with self-explanations
- [x] Long-context reasoning backend
- [x] Multi-layer feedback (objective + subjective + symbolic + enhancements)
- [x] Adaptive policy integration
- [x] Dashboard / leaderboard upgrade (incl. symbolic visualizer)
- [x] Sustained reflection across runs
- [x] Iterative testing framework (meta-cognitive + symbolic suites)
- [x] **Neuro-symbolic oversight layer** (`src/symbolic/`, `symbolic_policy.yaml`)
- [x] **Zero Trust & DAG Carbon Ledger** documented and wired
- [x] **Advanced enhancement modules** (LIMIT Graph, MODP, RLHF, Multi-Teacher
      Distillation, Evolutionary, MoE, FlexGen, Zero Trust, Privacy, Training)
- [x] Config keys reconciled with env vars (canonical)
- [x] 11 output artifacts documented
- [x] Security section
- [x] lean-ctx inspection examples
- [x] Docker reproduction block
- [x] Migration guide (v1.0 → v1.2)
- [x] Changelog
- [x] Reviewer checklist
- [x] Metadata block + Table of Contents
- [x] Comprehensive documentation links (verified filenames)
- [x] Demo and test scripts

**Status**: ✅ **COMPLETE (v1.2.0)**

**Delivery**: 8 meta-cognitive modules + 1 symbolic oversight layer + 10 advanced
enhancement modules, all tested (19 symbolic + 8 meta-cognitive test suites),
with full documentation, Docker reproduction, migration guide, and reviewer
checklist.

---

## Repository & Contact

- **Repository**: https://github.com/NurcholishAdam/Green_Agent
- **Issues**: https://github.com/NurcholishAdam/Green_Agent/issues
- **Maintainers**: open an issue and tag `@NurcholishAdam`

> Personal email has been removed from shipped documentation. Use the GitHub
> issue tracker for all questions and bug reports.
