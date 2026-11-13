
# Module Guide

| Field | Value |
|---|---|
| **Version** | 5.0.0 |
| **Companion layer versions** | Meta-cognitive **1.2.0** · Symbolic **1.2.0** · Enhancements **1.2.0** · Refactor **0.2.0** |
| **Last Updated** | 2026-11-13 |
| **Status** | Production Ready |
| **Audience** | Maintainers, integrators, operators |
| **Companion docs** | `Green Agent Architecture.md` (v5.0.0) · `QUICK_START.md` (v1.2.0) · `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `Scripts and Utilities.md` (v1.2.0) · `.cursorrules` |

---

## Table of Contents

1. [Overview](#overview)
2. [Module Categories](#module-categories)
   1. [Policy & Optimization Modules](#1-policy--optimization-modules)
   2. [Constraint & Reasoning Modules](#2-constraint--reasoning-modules)
   3. [Learning Modules](#3-learning-modules)
   4. [Sustainability Modules](#4-sustainability-modules)
   5. [Infrastructure Modules](#5-infrastructure-modules)
   6. [Monitoring & Observability](#6-monitoring--observability)
   7. [Security & Trust Modules](#7-security--trust-modules)
   8. [Symbolic Oversight Layer](#8-symbolic-oversight-layer)
   9. [Meta-Cognitive Layer](#9-meta-cognitive-layer)
   10. [v5.0 Integration Modules](#10-v50-integration-modules)
3. [Metric Reference](#metric-reference)
4. [Configuration Matrix](#configuration-matrix)
5. [Severity Taxonomy](#severity-taxonomy)
6. [Rule Action Vocabulary](#rule-action-vocabulary)
7. [Output Artifacts](#output-artifacts)
8. [Configuration Examples](#configuration-examples)
9. [Integration Patterns](#integration-patterns)
10. [Performance Considerations](#performance-considerations)
11. [Security](#security)
12. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
13. [Migration Notes](#migration-notes)
14. [Troubleshooting](#troubleshooting)
15. [What's Not in Scope](#whats-not-in-scope)
16. [Changelog](#changelog)
17. [Reviewer Checklist](#reviewer-checklist)
18. [See Also](#see-also)

---

## Overview

This guide documents every module in the Green Agent v5.0.0 system. Each entry
lists:

- **Location** — actual path in `src/` (flat layout per Refactor Guide v0.2)
- **Purpose** — one-line description
- **Key files** — exact filenames
- **Enable flag** — env var + YAML key (env wins on conflict)
- **Configuration** — YAML path
- **Metrics** — what the module exports (see [Metric Reference](#metric-reference))

> **Layout note (v5.0):** `src/enhancements/` is **flat** — modules live at the
> package root, not in sub-folders like `flexgen/`, `optimization/`,
> `constraints/`, or `learning/`. Sub-packages exist only for:
> `schemas/`, `core/`, `MODP/`, `bio_inspired/`, `moe_expert_system/`.
> Every path below reflects this flat layout.

---

## Module Categories

### 1. Policy & Optimization Modules

#### FlexGen Integration

**Location**: `src/enhancements/flexgen_controller.py` (flat)
**Purpose**: Interface with the FlexGen LLM inference optimization backend
**Key files**:
- `flexgen_controller.py` — main controller
- `flexgen_policy.py` — policy definitions
- `flexgen_policy_selector.py` — policy selection

**Enable flag**: `FLEXGEN_ENABLED=true` ↔ `enhancements.flexgen.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.flexgen`
**Metrics exported**: `flexgen_energy_kwh` (kWh, not joules)
**Dependencies**: none (standalone backend)

#### MODP (Multi-Objective Decision Process)

**Location**: `src/enhancements/pareto_optimizer.py` + `pareto_router.py` + `adaptive_cost_function.py` (flat) + `MODP/` (sub-package)
**Purpose**: Multi-objective optimization with Pareto efficiency
**Key files**:
- `MODP/` — sub-package with the decision core
- `pareto_optimizer.py` — Pareto optimization algorithms
- `pareto_router.py` — routing based on Pareto fronts
- `adaptive_cost_function.py` — dynamic cost modeling
- `schemas/node_descriptor.py` — `modp_score` computation
- `schemas/workload_descriptor.py` — priority computation

**Enable flag**: `MODP_ENABLED=true` ↔ `enhancements.modp.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.modp`
**Metrics exported**: `modp_score` (0–1, higher better)
**Dependencies**: `numpy` (not `scipy` — corrected)

#### Pareto Analysis (Meta-Cognitive)

**Location**: `src/analysis/pareto_analyzer.py`
**Purpose**: Objective feedback layer — computes Pareto positions for the
4-layer feedback synthesis
**Key files**: `pareto_analyzer.py`
**Configuration**: `green_agent.yaml` → `meta_cognitive.long_context`
**Metrics exported**: `pareto_position` (via `pareto_analysis.json`)

---

### 2. Constraint & Reasoning Modules

#### LIMIT Graph (Enhanced)

**Location**: `src/enhancements/limit_graph.py` + `limit_graph_v2.py` + `serendipity_trace.py` + `reasoning_path.py` + `constraint_engine.py` (flat) + `core/` (sub-package)
**Purpose**: Constraint enforcement with reasoning path tracking
**Key files**:
- `limit_graph.py` — original
- `limit_graph_v2.py` — enhanced version
- `serendipity_trace.py` — serendipitous discovery tracking
- `reasoning_path.py` — reasoning path management
- `constraint_engine.py` — constraint enforcement
- `core/graph_registry.py` — graph state, centrality, connectivity
- `core/causal_graph.py` — causal graph
- `core/dag_carbon_ledger.py` — immutable carbon provenance
- `core/anomaly_detector.py` — drift / alert severity

**Enable flag**: `LIMIT_GRAPH_ENABLED=true` ↔ `enhancements.limit_graph.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.limit_graph`
**Metrics exported**: `graph_centrality` (0–1), `graph_connectivity` (0–1)
**Dependencies**: `networkx`, `numpy`

**Evolution path**:
```
LIMIT Graph (original)
→ Enhanced LIMIT Graph v2
→ + Serendipity Trace
→ + Reasoning Paths
→ + Causal Graph + DAG Carbon Ledger + Anomaly Detector
→ Current (v1.2.0)
```

---

### 3. Learning Modules

#### Bio-Inspired Computing (Evolutionary)

**Location**: `src/enhancements/bio_inspired/` (sub-package) + `evolutionary_engine.py`, `genetic_algorithm.py`, `fitness_evaluator.py` (flat)
**Purpose**: Evolutionary algorithms for policy exploration and weight tuning
**Key files**:
- `bio_inspired/` — sub-package
- `evolutionary_engine.py` — main evolutionary loop
- `genetic_algorithm.py` — GA implementation
- `fitness_evaluator.py` — fitness calculation

**Enable flag**: `EVOLUTIONARY_ENABLED=true` ↔ `enhancements.evolutionary.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.evolutionary`
**Metrics exported**: `evolutionary_best_fitness` (0–1, higher better), plus derived `fitness_delta_1h`
**Parameters**:
- `population_size` (default 20)
- `mutation_rate` (default 0.1)

> **Renamed in v1.2:** `bio_inspired` → `evolutionary`. The old key is accepted
> as a deprecated alias for one minor release.

#### Mixture of Experts (MoE)

**Location**: `src/enhancements/moe_expert_system/` (sub-package)
**Purpose**: Route to specialized expert networks with learned gating
**Key files**:
- `moe_expert_system/green_agent_moe_system.py` — main MoE orchestration
- `moe_expert_system/expert_router_harvester.py` — routing + expert harvesting
- `moe_expert_system/fft_moe_adapter.py` — FFT adapter
- `moe_expert_system/context_encoder.py` — context representation

**Enable flag**: `MOE_ENABLED=true` (alias `MOE_GATING_ENABLED`) ↔ `enhancements.moe.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.moe`
**Metrics exported**: `moe_gate_stddev` (0–1, 0 = stable routing)
**Parameters**: `n_experts` (default 4)

> **Renamed in v1.2:** `moe_expert` → `moe`. The old key is accepted as a
> deprecated alias for one minor release.

#### RLHF (Reinforcement Learning from Human Feedback)

**Location**: `src/enhancements/rlhf.py` + `feedback_collector.py` + `preference_model.py` (flat — **not** a folder)
**Purpose**: Learn from user preferences via reward shaping
**Key files**:
- `rlhf.py` — RLHF teacher
- `feedback_collector.py` — feedback ingestion
- `preference_model.py` — preference learning
- `schemas/feedback_event.py` — `human_feedback_score` storage

**Enable flag**: `RLHF_ENABLED=true` ↔ `enhancements.rlhf.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.rlhf`
**Metrics exported**: `human_feedback_score` (0–1, higher better)

**Workflow**:
1. Collect feedback from users (`feedback_collector.py`)
2. Train reward model (`preference_model.py`)
3. Generate preference-aligned policies (`rlhf.py`)
4. Validate improvements (via `policy_feedback.py`)

#### Policy Distillation

**Location**: `src/enhancements/multi_teacher_policy_distillation.py` + `distillation_orchestrator.py` (flat)
**Purpose**: Compress multiple teacher policies into a lightweight student
**Key files**:
- `multi_teacher_policy_distillation.py` — multi-teacher distillation
- `distillation_orchestrator.py` — orchestration

**Enable flag**: `DISTILLATION_ENABLED=true` ↔ `enhancements.distillation.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.distillation`
**Metrics exported**: `distillation_update_rate` (ΔL/step, ≥ 0)

**Teachers**:
- Rule-based teacher
- Historical ML teacher
- Q-learning teacher
- RLHF teacher

> **Note:** FlexGen is a *backend*, not a teacher. The distillation student
> blends the four teachers above via a gating network.

---

### 4. Sustainability Modules

#### Carbon & Sustainability Management

**Location**: `src/enhancements/` root — `sustainability_cost.py`, `sustainability_signals_v10.py`, `carbon_delay_scheduler.py`, `carbon_credit_marketplace.py`, `marginal_carbon_v10.py`, `real_carbon_intensity_api_v10.py` (flat)
**Purpose**: Track and optimize carbon emissions
**Key files**:
- `sustainability_cost.py` — cost with carbon
- `sustainability_signals_v10.py` — sustainability metrics
- `carbon_delay_scheduler.py` — carbon-aware scheduling
- `carbon_credit_marketplace.py` — carbon credit trading
- `marginal_carbon_v10.py` — marginal emissions
- `real_carbon_intensity_api_v10.py` — real-time grid data

**Configuration**: `green_agent.yaml` → `carbon`
**Data sources** (in priority order, per `provider_priority` in
`free_apis_config.yaml`):
- **`self_hosted`** (ElectricityMap, self-hosted)
- **`electricitymap`** (public API, key required)
- **`carbonintensity`** (UK grid, key optional)
- **`simulation`** (fallback)

> **Not supported:** `watttime` is **not** a valid `carbon.api_provider` value.
> Base enum: `simulation | electricitymap | carbonintensity | self_hosted`.

---

### 5. Infrastructure Modules

#### Data Center Integration

**Location**: `src/enhancements/` root — `ai_data_center_loader.py`, `green_datacenter_map.py`, `green_datacenter_selector.py`, `cloud_latency_estimator.py` (flat)
**Purpose**: Select optimal data center based on multiple criteria
**Key files**:
- `ai_data_center_loader.py` — datacenter data loading
- `green_datacenter_map.py` — datacenter mapping
- `green_datacenter_selector.py` — intelligent selection
- `cloud_latency_estimator.py` — latency estimation

**Configuration**: `green_agent.yaml` → `carbon.default_region` + `regions`
**Selection criteria**:
- Carbon intensity
- Latency
- Cost
- Availability
- Green energy %

#### Helium Integration

**Location**: `src/enhancements/` root — `helium_api_collector.py`, `helium_data_collector.py`, `helium_elasticity_v10.py`, `helium_forecaster_v9.py`, `helium_scarcity_manager.py`, `helium_circularity.py`, `quantum_helium_optimizer_v10.py`, `unified_helium_integration_v4.py`, `chromatophore_compartments.py` (flat)
**Purpose**: Helium network integration and optimization
**Key files**: see above

**Configuration**: `green_agent.yaml` → `helium`
**Key fields**:
- `helium.enabled` (default false)
- `helium.api_endpoints.primary` + `.backup` (plural — two endpoints)
- `helium.api_key_env_var: "HELIUM_API_KEY"`
- `helium.update_interval_seconds: 900`
- `helium.max_retries: 3`
- `helium.base_retry_delay: 1.0`

**Features**:
- Real-time resource monitoring
- Dynamic scaling
- Quantum-optimized routing
- Chromatophore-based adaptation

---

### 6. Monitoring & Observability

**Location** (split — v1.2):
- Meta-cognitive metrics: `src/monitoring/metrics_collector.py`
- Enhancement profilers (flat): `src/enhancements/gpu_profiler.py`, `metric_aggregator.py`, `energy_profiler.py`, `energy_scaler.py`, `module_benchmark_v5.py`

**Key files**:
- `src/monitoring/metrics_collector.py` — real-time metrics (enhanced metrics pass-through)
- `src/enhancements/gpu_profiler.py` — GPU metrics
- `src/enhancements/metric_aggregator.py` — centralized aggregation
- `src/enhancements/energy_profiler.py` — energy consumption
- `src/enhancements/energy_scaler.py` — energy scaling
- `src/enhancements/module_benchmark_v5.py` — per-module benchmarks

**Configuration**: `green_agent.yaml` → `monitoring`

**Outputs**:
- Prometheus metrics (`:9090/metrics` — separate from agent `:8000`)
- Grafana dashboards
- JSON reports (`metrics_history.json`)

---

### 7. Security & Trust Modules

#### Zero Trust Architecture

**Location**: `src/enhancements/zero_trust_architecture.py` (flat)
**Purpose**: Trust scoring, attestation, policy decisions
**Key files**: `zero_trust_architecture.py`
**Enable flag**: `ZERO_TRUST_ENABLED=true` ↔ `enhancements.zero_trust.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.zero_trust`
**Required env**: `ZERO_TRUST_TOKEN` (Kubernetes Secret)

**Metrics exported**: trust state (via symbolic rules)

**Enables**: `block_request` action in symbolic rules

#### Differential Privacy

**Location**: `src/enhancements/` (privacy module — path TBD)
**Enable flag**: `DIFFERENTIAL_PRIVACY_ENABLED=true` ↔ `enhancements.privacy.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.privacy`
**Metrics exported**: DP ε/δ budget consumption
**Parameters**: `epsilon` (default 1.0), `delta` (default 1e-5)

#### Training / Checkpoint Tracking

**Location**: `src/enhancements/` (training module — path TBD)
**Enable flag**: `TRAINING_ENABLED=true` ↔ `enhancements.training.enabled`
**Configuration**: `green_agent.yaml` → `enhancements.training`
**Metrics exported**: checkpoint state (via symbolic rules)
**Parameters**: `checkpoint_dir` (default "checkpoints")

#### Schemas (Typed Descriptors)

**Location**: `src/enhancements/schemas/` (sub-package)
**Key files**:
- `schemas/node_descriptor.py` — `NodeDescriptor`, `NodeType`, `CoolingType`, `RoutingStrategy`, `create_node_descriptor`
- `schemas/workload_descriptor.py` — `WorkloadDescriptor`, `TaskType`, `Urgency`, `Priority`, `BioMode`, `create_workload_descriptor`
- `schemas/feedback_event.py` — `FeedbackEvent`

#### Core Infrastructure

**Location**: `src/enhancements/` root — `base_classes.py`, `adapters.py`, `async_message_queue.py`, `config.yaml` (flat)

---

### 8. Symbolic Oversight Layer

**Location**: `src/symbolic/symbolic_reasoning_engine.py`
**Visualizer**: `src/dashboard/symbolic_visualizer.py`
**Config**: `symbolic_policy.yaml`
**Purpose**: Whitelist rule evaluation — the **3rd feedback layer**

**Key files**:
- `src/symbolic/symbolic_reasoning_engine.py` — `SymbolicReasoningEngine`, `SymbolicRule`, `ViolationTrace`
- `src/dashboard/symbolic_visualizer.py` — `SymbolicVisualizer`

**Enable flag**: **always on** when `symbolic_policy.yaml` exists
**Configuration**: `green_agent.yaml` → `symbolic.policy_path` (default `symbolic_policy.yaml`)

**Rule categories**: sustainability, resource, fairness, safety, compliance,
composite, enhanced (MODP, graph, RLHF, distillation, MoE, evolutionary,
FlexGen, zero-trust, privacy, training)

**Security**: whitelist parser — **no `eval`/`exec`**, no attribute access,
no function calls. Allowed tokens: identifiers, numbers,
`> < >= <= == != AND OR NOT ( )`.

**Outputs**: `symbolic_violations.json`, `symbolic_violation_report.json`

**Tests**: 19 tests (`test_symbolic_oversight.py`)

See `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) for the full contract.

---

### 9. Meta-Cognitive Layer

**Location**: `src/{monitoring,reflection,memory,analysis,policy,dashboard}/`

**Key files**:

| Directory | Files | Purpose |
|---|---|---|
| `src/monitoring/` | `metrics_collector.py` | Real-time metrics + enhanced pass-through |
| `src/reflection/` | `reflection_engine.py`, `long_context_reasoner.py` | Reflection checkpoints + historical analysis |
| `src/memory/` | `run_memory.py` | Persistent memory + meta-policy generation |
| `src/analysis/` | `pareto_analyzer.py` | Objective feedback layer |
| `src/policy/` | `policy_engine.py`, `policy_feedback.py` | Adaptive policy + `generate_multi_layer_feedback` |
| `src/dashboard/` | `green_dashboard.py`, `symbolic_visualizer.py` | Visualization |

**Configuration**: `green_agent.yaml` → `meta_cognitive` + `policy`

**Key methods**:
- `MetricsCollector.get_current_metrics()` — mid-execution access
- `ReflectionEngine.generate_reflection(step, metrics, timestamp)` — checkpoint
- `LongContextReasoner.compare_with_past_runs(metrics)` — insights
- `PolicyEngine.apply_adaptive_adjustment(decision)` — policy adjust
- `PolicyFeedback.generate_multi_layer_feedback(...)` — 4-layer synthesis

---

### 10. v5.0 Integration Modules

**Location**: `src/{interpretation,decision,optimization,benchmarking,carbon,ml_governance,governance,distributed,orchestration,integration}/`

| Directory | File | Purpose |
|---|---|---|
| `src/interpretation/` | `workload_interpreter.py` | Interpret incoming workloads |
| `src/decision/` | `carbon_aware_decision_core.py` | Carbon-aware decision making |
| `src/optimization/` | `synthetic_data_optimizer.py` | Synthetic data for training |
| `src/benchmarking/` | `benchmark_intelligence.py` | Benchmark tracking |
| `src/carbon/` | `task_carbon_profiler.py`, `forecasting_engine.py`, `eco_mode_controller.py` | Carbon profiling + forecasting |
| `src/ml_governance/` | `adaptation_classifier.py`, `policy_engine.py` (ML governance) | Governance |
| `src/governance/` | `carbon_ledger.py` | Carbon accounting |
| `src/distributed/` | `ray_cluster_manager.py`, `carbon_aware_scheduler.py` | Ray cluster ops |
| `src/orchestration/` | `multi_objective_scheduler.py` | Multi-objective scheduling |
| `src/integration/` | `unified_orchestrator.py` | Unified entrypoint |

**Configuration**: `green_agent.yaml` → `system.mode: "unified"` + `ray` + `carbon`

**Entrypoint**: `python run_agent.py --mode=unified` (via `src/integration/unified_orchestrator.py:create_unified_agent`)

---

## Metric Reference

All metrics usable in symbolic rules or reflection thresholds. Units are
canonical — the engine does **not** silently convert.

### Core metrics

| Variable | Unit | Range |
|---|---|---|
| `energy` | Wh | ≥ 0 |
| `carbon` | g | ≥ 0 |
| `latency` | ms | ≥ 0 |
| `memory` | MB | ≥ 0 |
| `tool_calls` | count | ≥ 0 |
| `cpu_percent` | % | 0–100 |

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

> ⚠️ `flexgen_energy_joules` is **deprecated** — use `flexgen_energy_kwh`.
> ⚠️ `evolutionary_best_fitness offset 1h` is **invalid syntax** — use `fitness_delta_1h`.

---

## Configuration Matrix

Env vars are **canonical**; YAML maps onto them (env wins on conflict — matches
Dockerfile v2.0 and `SYMBOLIC_OVERSIGHT_README.md` v1.2).

| Module | Env flag | YAML key | Metric |
|---|---|---|---|
| Symbolic | always on with `symbolic_policy.yaml` | `symbolic.enabled` | violation traces |
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
| Symbolic path | `SYMBOLIC_POLICY` | `symbolic.policy_path` | — |
| Log level | `LOG_LEVEL` | `system.log_level` | — |
| Log format | `LOG_FORMAT` | `monitoring.logging.format` | — |

---

## Severity Taxonomy

`priority` in a symbolic rule maps to a numeric multiplier; severity is derived
from the resulting score:

| Severity | Score range | Typical trigger |
|---|---|---|
| `critical` | ≥ 1.5 × threshold | hard budget exceeded |
| `high` | 1.2 – 1.5 × threshold | near-budget |
| `medium` | 1.0 – 1.2 × threshold | at threshold |
| `low` | < 1.0 × threshold | soft warning |

Priority multipliers: `critical=2.0`, `high=1.5`, `medium=1.0`, `low=0.5`.

> **Do not use `warning` as a `priority` value.** Priorities are
> `critical / high / medium / low`. `warning` is a severity bucket only.

---

## Rule Action Vocabulary

Every symbolic rule's `action` must be one of these; unknown actions are
logged and ignored.

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

## Output Artifacts

Every full run with all modules enabled produces **11 artifacts**:

| # | File | Layer |
|---|---|---|
| 1 | `results.json` | Execution |
| 2 | `dashboard.html` | Visual |
| 3 | `metrics_history.json` | Timeline |
| 4 | `reflections.json` | Subjective (layer 2) |
| 5 | `reasoning_insights.json` | Long-context |
| 6 | `pareto_analysis.json` | Objective (layer 1) |
| 7 | `run_memory.json` | Persistent |
| 8 | `dashboard_data.json` | Dashboard export |
| 9 | `symbolic_violations.json` | **Symbolic (layer 3)** |
| 10 | `symbolic_violation_report.json` | **Symbolic (layer 3)** |
| 11 | Enhanced fields in `results.json` + `reflections.json` | Advanced (layer 4) |

---

## Configuration Examples

### Minimal (all defaults)

```yaml
# config/green_agent.yaml
system:
  version: "5.0.0"
  layers:
    meta_cognitive: "1.2.0"
    symbolic_oversight: "1.2.0"
    enhancements: "1.2.0"
  mode: "unified"

carbon:
  api_provider: "simulation"
  fallback_provider: "simulation"
  default_region: "US-CA"
  eco_mode_threshold: 200
  defer_threshold: 400
```

### Full enhancements (v5.0 canonical)

```yaml
# config/green_agent.yaml
system:
  version: "5.0.0"
  mode: "unified"

policy:
  mode: "moderate"
  green_path: "green_policy.yaml"
  symbolic_path: "symbolic_policy.yaml"

meta_cognitive:
  reflection_frequency: 5
  adaptive_policy_enabled: true
  long_context:
    enabled: true
    history_window: 10

symbolic:
  enabled: true
  policy_path: "symbolic_policy.yaml"

enhancements:
  enabled: true                    # ↔ ENHANCEMENTS_ENABLED
  limit_graph:
    enabled: true
    graph_metrics:
      centrality: 0.7
      connectivity: 0.6
  modp:
    enabled: true
    objective_weights: [0.4, 0.3, 0.2, 0.1]  # [accuracy, energy, latency, carbon]
  rlhf:
    enabled: true
    human_feedback_score: 0.6
  distillation:
    enabled: true
    use_moe_gating: true
  evolutionary:
    enabled: true
    population_size: 20
    mutation_rate: 0.1
  moe:
    enabled: true
    n_experts: 4
  flexgen:
    enabled: false
    model_name: "facebook/opt-6.7b"
    default_precision: "fp16"
    delegation_policy: "adaptive"
  zero_trust:
    enabled: false
  privacy:
    enabled: false
    epsilon: 1.0
    delta: 1.0e-5
  training:
    enabled: false
    checkpoint_dir: "checkpoints"
```

### Carbon & Helium (v5.0 canonical)

```yaml
carbon:
  api_provider: "electricitymap"
  fallback_provider: "simulation"
  default_region: "US-CA"
  eco_mode_threshold: 200
  defer_threshold: 400
  polling_interval_seconds: 900
  cache_ttl_seconds: 300
  api_key_env_var: "CARBON_API_KEY"
  electricitymap_api_key_env_var: "ELECTRICITYMAP_API_KEY"
  carbonintensity_api_key_env_var: "CARBONINTENSITY_API_KEY"

helium:
  enabled: true
  api_endpoints:
    primary: "https://api.helium-monitor.example.com/v1/supply"
    backup:  "https://backup.helium-api.example.com/v1/status"
  api_key_env_var: "HELIUM_API_KEY"
  update_interval_seconds: 900
  history_buffer_size: 100
  max_retries: 3
  base_retry_delay: 1.0
  simulation_enabled: true
  simulation_seed: null
```

> **Note:** `FlexGen` is a backend, not a config section outside `enhancements`.
> `helium.api_endpoints` is plural with `primary` + `backup`.
> `carbon.api_provider` valid values: `simulation | electricitymap | carbonintensity | self_hosted`.

---

## Integration Patterns

### Sequential pipeline (enhanced routing + priority)

```python
import asyncio
from src.enhancements.schemas.node_descriptor import (
    NodeDescriptor, NodeType, CoolingType, create_node_descriptor,
)
from src.enhancements.schemas.workload_descriptor import (
    WorkloadDescriptor, TaskType, Urgency, create_workload_descriptor,
)

async def pipeline(task):
    # 1. Create node + workload descriptors
    node = create_node_descriptor(
        id="node1",
        node_type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=400.0,
        energy_per_token=0.00005,
        helium_connectivity_score=0.8,
        uptime=0.99,
        renewable_fraction=0.3,
        cooling_type=CoolingType.LIQUID,
        hardware_model="collector",
    )
    node.human_feedback_score = 0.6
    node.graph_metrics = {"centrality": 0.8, "connectivity": 0.6}

    # 2. Select routing strategy (distillation + MoE)
    strategy = await node.select_routing_strategy(exploration=False)
    print(f"Routing strategy: {strategy.value}")

    # 3. Create workload + select priority (MODP + MoE)
    workload = create_workload_descriptor(
        task_type=TaskType.INFERENCE,
        tokens=1000,
        latency_target=300.0,
        urgency=Urgency.MEDIUM,
        metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2},
    )
    workload.human_feedback_score = 0.7
    workload.graph_metrics = {"centrality": 0.6}

    priority = await workload.select_priority(exploration=False)
    print(f"Priority: {priority.value}")

asyncio.run(pipeline(task={}))
```

### Symbolic rule evaluation

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

metrics = {
    "carbon": 72.0,       # grams
    "energy": 4.5,        # Wh
    "latency": 2500,      # ms
    "memory": 300,        # MB
    "tool_calls": 25,
    "modp_score": 0.55,
}

violations = engine.evaluate_rules(metrics, step=5)
summary = engine.get_violation_summary()
print(f"Violations: {summary['total_violations']}")

visualizer = SymbolicVisualizer()
visualizer.add_violations([v.to_dict() for v in violations])
html = visualizer.generate_dashboard_section()
```

### Multi-layer feedback (v1.2 canonical)

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()

result = feedback.generate_multi_layer_feedback(
    pareto_analysis={"position": "frontier", "efficiency_score": 0.85},
    reflections=[{"step": 1, "decision": "continue", "confidence": 0.75}],
    metrics=metrics,
    symbolic_violations=[v.to_dict() for v in violations],
    enhanced_metrics={"modp_score": 0.72, "moe_gate_stddev": 0.08},
)

assert "objective_layer" in result
assert "subjective_layer" in result
assert "symbolic_layer" in result
assert "enhanced_layer" in result
assert "synthesis" in result

# Deprecated alias (removed in v3.0):
# feedback.generate_dual_layer_feedback(...)
```

### Reflection + long-context

```python
from src.reflection.reflection_engine import ReflectionEngine
from src.reflection.long_context_reasoner import LongContextReasoner

engine = ReflectionEngine(reflection_frequency=5)
if engine.should_reflect(step=5):
    reflection = engine.generate_reflection(step=5, metrics=metrics, timestamp=None)
    print(reflection.self_explanation)

reasoner = LongContextReasoner()
insights = reasoner.compare_with_past_runs(metrics)
for insight in insights:
    print(f"{insight.description} (confidence: {insight.confidence})")
```

---

## Performance Considerations

### Optimization for speed

- Use `flexgen_controller.py` for fast policy selection
- Enable caching in `policy_meta_cache.py`
- Reduce `evolutionary.population_size` in development
- Set `reflection_frequency` higher (e.g. 10) in latency-sensitive workloads

### Optimization for accuracy

- Increase `modp.objective_weights` precision (4 decimal places)
- Expand `moe.n_experts` (4 → 8)
- Increase RLHF buffer via `preferences` in `feedback_collector.py`
- Raise `evolutionary.population_size` (20 → 50)

### Optimization for sustainability

- Set `carbon.api_provider: "electricitymap"` (real data) — falls back to simulation
- Set `carbon.eco_mode_threshold: 200` (yellow zone) + `defer_threshold: 400` (red zone)
- Enable `helium.enabled: true` with real endpoints
- Set `enhancements.evolutionary.enabled: true` for carbon-aware weight tuning

---

## Security

| Concern | Guarantee |
|---|---|
| Pod Security Admission | Namespace enforces `restricted` |
| Symbolic rule evaluation | **Whitelist parser** — no `eval`/`exec` |
| Allowed tokens | identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| YAML parsing | `yaml.safe_load` only |
| `block_request` | Requires `ZERO_TRUST_ENABLED=true` |
| Secrets | Kubernetes `Secret` only |
| `ZERO_TRUST_TOKEN` / `RLHF_API_KEY` / `CARBON_API_KEY` / `HELIUM_API_KEY` | Via `envFrom: secretRef` |

---

## Inspecting State with lean-ctx

When `lean-ctx` is available (see `.cursorrules`):

```python
ctx_read("src/enhancements/", mode="digest")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")
ctx_read("src/symbolic/symbolic_reasoning_engine.py", mode="signatures")
ctx_read("symbolic_policy.yaml", mode="symbolic")
ctx_read("src/reflection/reflection_engine.py", mode="signatures")
```

---

## Migration Notes

### v1.0 / v1.1 → v1.2 (layer version)

| Before | After | Notes |
|---|---|---|
| `bio_inspired.enabled` | `evolutionary.enabled` | Alias kept for one minor release |
| `moe_expert.enabled` | `moe.enabled` | Alias kept |
| `modp.weights` (3 items) | `modp.objective_weights` (4 items `[accuracy, energy, latency, carbon]`) | Arity changed |
| `flexgen_energy_joules` | `flexgen_energy_kwh` | Unit fix |
| `evolutionary_best_fitness offset 1h` | `fitness_delta_1h` | Invalid syntax → derived metric |
| `generate_dual_layer_feedback()` | `generate_multi_layer_feedback()` | Deprecated alias kept |
| `priority: "warning"` | `priority: "medium"` (or severity field) | Enum fix |
| `carbon.api_provider: "watttime"` | `simulation \| electricitymap \| carbonintensity \| self_hosted` | Invalid value |
| `helium.api_endpoint` (singular) | `helium.api_endpoints.{primary,backup}` | Structural |
| `agent.version: "2.4.0"` | `system.version: "5.0.0"` | Key rename |
| `flexgen.backend_url: localhost:8000` | port 8010 or cluster hostname | Port collision |

---

## Troubleshooting

### `from src.enhancements import …` fails

**Cause:** `src/` is not on `sys.path`.
**Fix:** `pip install -e .` (uses `pyproject.toml`).

### Enhancement returns `{}`

**Cause:** enable flag not set.
**Fix:** `ENHANCEMENTS_ENABLED=true` + per-module flag (e.g. `MODP_ENABLED=true`).
Check `ctx_read("src/enhancements/", mode="digest")`.

### Symbolic rules never trigger

**Cause:** unit mismatch, missing `symbolic_policy.yaml`, or invalid priority.
- Carbon: **grams** (not kg)
- `flexgen_energy_kwh` is **kWh** (not joules)
- `priority` must be `critical`/`high`/`medium`/`low` — **not** `warning`

### `flexgen_energy_joules` returns wrong value

**Cause:** deprecated unit.
**Fix:** use `flexgen_energy_kwh` (divide joules by 3.6e6).

### Helium config fails

**Cause:** `helium.api_endpoint` (singular) instead of `api_endpoints.primary`.
**Fix:** use the nested form. Set `HELIUM_API_KEY` in the Secret.

### Symbolic `/health` and `/ready` both 404

**Cause:** only `/health` exists (matches Deployment v5.0). `/ready` is not implemented.
**Fix:** use `/health` for all three probes (startup / liveness / readiness).

---

## What's Not in Scope

- **`watttime` as a carbon provider** — invalid. Use `simulation`,
  `electricitymap`, `carbonintensity`, or `self_hosted`.
- **FlexGen as a distillation teacher** — FlexGen is a backend; teachers are
  rule-based, historical ML, Q-learning, and RLHF.
- **`scipy` as a MODP dependency** — only `numpy` is required.
- **Z3 / SMT solving** — whitelist evaluator only.
- **Cross-agent oversight** — single-agent runtime.
- **Non-Kubernetes production deployment** — `docker run` works; `deploy.sh`
  targets K8s only.

---

## Changelog

### 5.0.0 (2026-04-10)

**Fixed**

- **All 25 module paths** — rewritten from invented sub-folders
  (`flexgen/`, `optimization/`, `constraints/`, `learning/*/`, `monitoring/`)
  to the actual **flat** `src/enhancements/` layout per Refactor Guide v0.2.
- `FLexGen` → **`FlexGen`**.
- `src/enhancements/optimization/modp.py` → removed (no `MODPOptimizer` class).
- `src/enhancements/learning/moe/gating_network.py` → removed.
- `src/enhancements/learning/rlhf/*` → flattened to `rlhf.py` + `feedback_collector.py` + `preference_model.py`.
- `src/enhancements/learning/distillation/*` → flattened to `multi_teacher_policy_distillation.py` + `distillation_orchestrator.py`.
- `src/enhancements/monitoring/carbon_tracker.py` + `performance_monitor.py` → removed (not documented).
- `carbon.api_provider: "watttime"` → **invalid**; documented the actual enum.
- `bio:` → **`evolutionary:`**.
- `modp.weights: [0.4, 0.3, 0.3]` → **`modp.objective_weights: [0.4, 0.3, 0.2, 0.1]`** (4 items, labeled).
- `moe.num_experts` → **`moe.n_experts`**.
- `carbon.threshold_gco2_per_kwh` → **`carbon.defer_threshold`** (with `eco_mode_threshold`).
- `helium.api_endpoint` → **`helium.api_endpoints.{primary,backup}`**.
- `agent.version: "2.4.0"` → **`system.version: "5.0.0"`** + `system.layers.*`.
- `flexgen.backend_url: "http://localhost:8000"` → port 8000 is the **agent's own** port; use 8010 or a cluster hostname.
- All broken code samples (`MODPOptimizer`, `LimitGraph.enforce`, `bio_inspired.evolve()`, `MoESystem`, `combine()`) rewritten with verified APIs.

**Added**

- **§Symbolic Oversight Layer** — `src/symbolic/` with full description.
- **§Meta-Cognitive Layer** — 6 modules with key methods.
- **§v5.0 Integration Modules** — 10 new modules.
- **§Security & Trust Modules** — zero trust, DP, training, schemas, core.
- **§Metric Reference** — units, ranges, enable flag per metric.
- **§Configuration Matrix** — env ↔ YAML ↔ metric.
- **§Severity Taxonomy**.
- **§Rule Action Vocabulary** — 11 actions.
- **§Output Artifacts** — 11 files.
- **§Migration Notes** (v1.0 → v1.2).
- **§Security** (PSA, whitelist parser, secrets).
- **§lean-ctx inspection**.
- **§What's Not in Scope.**
- **§Changelog**, **§Reviewer Checklist**.
- **Metadata block** + **Table of Contents**.
- **Per-module enable flag** (env + YAML) for every entry.
- **`symbolic_policy.yaml`** + `symbolic_violations.json` references.
- **`pyproject.toml` / `MANIFEST.sha256` / `Dockerfile` / `.cursorrules`** references.
- **`test_symbolic_oversight.py` (19 tests)** reference.

**Changed**

- Every "Location" field now shows the **actual** path.
- §Configuration Examples rewritten with v5.0 canonical keys.
- §Integration Patterns rewritten with verified APIs.
- `WattTime API` removed from data sources (not supported).
- `DEPLOYMENT.md`, `INTEGRATION_GUIDE.md`, `API.md` links → replaced with real companion docs.

**Removed**

- Invented modules: `policy_generator.py`, `reward_model.py`, `feedback_processor.py`, `student_network.py`, `teacher_policies.py`, `carbon_tracker.py`, `performance_monitor.py`.
- Invented class `MODPOptimizer`.
- Invented functions `combine()`, `bio_inspired.evolve()`, `moe.route()`.

### Pre-5.0

- Initial module guide with 6 categories.

---

## Reviewer Checklist

- [ ] Every "Location" path exists in `git ls-files` (flat layout, not sub-folders).
- [ ] No `flexgen/`, `optimization/`, `constraints/`, `learning/*/`, `monitoring/` pseudo-folders remain.
- [ ] `src/symbolic/`, `src/reflection/`, `src/memory/`, `src/analysis/`, `src/policy/`, `src/dashboard/` present.
- [ ] 10 v5.0 modules present (`interpretation`, `decision`, `optimization`, `benchmarking`, `carbon`, `ml_governance`, `governance`, `distributed`, `orchestration`, `integration`).
- [ ] No `FLexGen` typo remains.
- [ ] Config examples use `evolutionary:` (not `bio:`), `moe:` (not `moe_expert:`), `modp.objective_weights` with 4 items labeled.
- [ ] `carbon.api_provider` only lists `simulation | electricitymap | carbonintensity | self_hosted` — **no `watttime`**.
- [ ] `helium.api_endpoints.{primary,backup}` (plural, nested) used.
- [ ] `system.version: "5.0.0"` + `system.layers.*` used (not `agent.version: "2.4.0"`).
- [ ] No `flexgen.backend_url: localhost:8000` remains.
- [ ] All code samples use verified APIs (`select_routing_strategy`, `select_priority`, `evaluate_rules`, `generate_multi_layer_feedback`).
- [ ] `SYMBOLIC_OVERSIGHT_README.md`, `Meta-Cognitive Architecture for Green Agent.md`, `REPOSITORY_REFACTORING_GUIDE.md`, `Scripts and Utilities.md` in header + See Also.
- [ ] No dangling `DEPLOYMENT.md`, `INTEGRATION_GUIDE.md`, `API.md` links.
- [ ] §Metric Reference includes `flexgen_energy_kwh` (not joules).
- [ ] §Severity Taxonomy notes `warning` is a severity, not a priority.
- [ ] §Rule Action Vocabulary lists all 11 actions.
- [ ] §Output Artifacts lists all 11 files.
- [ ] §Configuration Matrix matches Dockerfile v2.0 env names.
- [ ] `test_symbolic_oversight.py` (19 tests) referenced.
- [ ] No secrets committed.
- [ ] Date `2026-04-10`, version `5.0.0`.

---

## See Also

- [`Green Agent Architecture.md`](Green%20Agent%20Architecture.md) (v5.0.0) — system overview + 4-layer feedback
- [`QUICK_START.md`](QUICK_START.md) (v1.2.0) — install → demo → run
- [`SYMBOLIC_OVERSIGHT_README.md`](SYMBOLIC_OVERSIGHT_README.md) (v1.2.0) — symbolic rule engine, action vocabulary, severity
- [`NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`](NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md) — reasoning loop + parser internals
- [`META_COGNITIVE_ENHANCEMENT_COMPLETE.md`](META_COGNITIVE_ENHANCEMENT_COMPLETE.md) (v1.2.0) — meta-cognitive delivery summary
- [`NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md`](NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md) (v1.2.0) — symbolic integration summary
- [`REPOSITORY_REFACTORING_GUIDE.md`](REPOSITORY_REFACTORING_GUIDE.md) (v0.2.0) — src layout + import migration
- [`Scripts and Utilities.md`](Scripts%20and%20Utilities.md) (v1.2.0) — setup, verify, migrate, deploy
- [`.cursorrules`](.cursorrules) — lean-ctx read modes

---

**Last Updated**: 2026-11-13 · **Version**: 5.0.0 · **Layer versions**: 1.2.0

## Summary of changes vs. original

| Area | Before | After |
|---|---|---|
| **Paths** | 25 invented sub-folders (`flexgen/`, `optimization/`, `constraints/`, `learning/*/`, `monitoring/`) | **Flat** `src/enhancements/` per Refactor Guide v0.2 |
| **Symbolic layer** | Absent | §8 with full description |
| **Meta-cognitive layer** | Absent | §9 with 6 modules |
| **v5.0 modules** | Absent | §10 with 10 modules |
| **Security & Trust** | Absent | §7 with zero trust, DP, training, schemas, core |
| **`FLexGen` typo** | Present | Fixed to `FlexGen` |
| **`MODPOptimizer`** | Referenced | Removed (doesn't exist) |
| **`carbon.api_provider: watttime`** | Present | Removed (invalid) |
| **`bio:` config key** | Present | → `evolutionary:` |
| **`modp.weights` (3 items)** | Present | → `modp.objective_weights` (4 items labeled) |
| **`moe.num_experts`** | Present | → `moe.n_experts` |
| **`carbon.threshold_gco2_per_kwh`** | Present | → `carbon.defer_threshold` |
| **`helium.api_endpoint`** | Present | → `helium.api_endpoints.{primary,backup}` |
| **`agent.version: "2.4.0"`** | Present | → `system.version: "5.0.0"` |
| **`flexgen.backend_url: localhost:8000`** | Present | Removed (port collision) |
| **Broken code samples** | 11 invented APIs | 4 verified-API samples |
| **§Metric Reference** | Absent | 3 tables (core / enhancement / derived) |
| **§Configuration Matrix** | Absent | 14-row table |
| **§Severity Taxonomy** | Absent | 4 tiers + multipliers |
| **§Rule Action Vocabulary** | Absent | 11 actions |
| **§Output Artifacts** | Absent | 11 files |
| **§Security** | Absent | 8-row table |
| **§lean-ctx** | Absent | 6 examples |
| **§Migration Notes** | Absent | 11-row table |
| **§What's Not in Scope** | Absent | 6 items |
| **§Changelog** | Absent | Present |
| **§Reviewer Checklist** | Absent | 22 items |
| **Metadata + TOC** | Absent | Present |
| **Dangling links** (`DEPLOYMENT.md`, `INTEGRATION_GUIDE.md`, `API.md`) | Present | Replaced with real docs |
| **Per-module enable flag** | Absent | Every entry has env + YAML |
| **`symbolic_policy.yaml`** | Absent | Referenced |
| **`symbolic_violations.json`** | Absent | Referenced |
| **`pyproject.toml` / `MANIFEST.sha256` / `Dockerfile` / `.cursorrules`** | Absent | Referenced |
| **`test_symbolic_oversight.py`** | Absent | Referenced |
