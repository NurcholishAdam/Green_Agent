
# Green Agent Architecture

| Field | Value |
|---|---|
| **Version** | 5.0.0 |
| **Companion layer versions** | Meta-cognitive **1.2.0** · Symbolic **1.2.0** · Enhancements **1.2.0** · Refactor **0.2.0** |
| **Last Updated** | 2026-11-13 |
| **Status** | Production Ready |
| **Audience** | Architects, maintainers, integrators |
| **Companion docs** | `QUICK_START.md` (v1.2.0) · `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `Scripts and Utilities.md` (v1.2.0) · `.cursorrules` |

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Data Flow](#data-flow)
3. [Four-Layer Feedback](#four-layer-feedback)
4. [Module Integration Points](#module-integration-points)
5. [Key Design Principles](#key-design-principles)
6. [Deployment Architecture](#deployment-architecture)
7. [Security](#security)
8. [Configuration Matrix](#configuration-matrix)
9. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
10. [What's Not in Scope](#whats-not-in-scope)
11. [Changelog](#changelog)
12. [See Also](#see-also)

---

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│         Green Agent System Architecture (v5.0.0)        │
└─────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Configuration Layer                                     │
│  ├─ config/green_agent.yaml          (canonical app cfg) │
│  ├─ config/free_apis_config.yaml     (providers)         │
│  ├─ config/environments/.env.[env]   (secrets + env)     │
│  ├─ config/environments/.env.example (template)          │
│  ├─ green_policy.yaml                (meta-cognitive)    │
│  ├─ symbolic_policy.yaml             (4th layer rules)   │
│  └─ src/enhancements/config.yaml     (module overrides)  │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Core Agent (src/core/)                                  │
│  ├─ agent.py (Main orchestrator)                        │
│  ├─ orchestrator.py (Task management)                   │
│  └─ interfaces.py (Base interfaces)                      │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  v5.0 Integration Modules                                │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  src/interpretation/    workload_interpreter.py         │
│  src/decision/          carbon_aware_decision_core.py    │
│  src/optimization/      synthetic_data_optimizer.py      │
│  src/benchmarking/      benchmark_intelligence.py        │
│  src/carbon/            task_carbon_profiler.py          │
│                         forecasting_engine.py            │
│                         eco_mode_controller.py           │
│  src/ml_governance/     adaptation_classifier.py         │
│                         policy_engine.py                 │
│  src/governance/        carbon_ledger.py                 │
│  src/distributed/       ray_cluster_manager.py           │
│                         carbon_aware_scheduler.py        │
│  src/orchestration/     multi_objective_scheduler.py     │
│  src/integration/       unified_orchestrator.py          │
│                                                          │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Meta-Cognitive Layer (v1.2.0)                           │
├──────────────────────────────────────────────────────────┤
│  src/monitoring/   metrics_collector.py                  │
│  src/reflection/   reflection_engine.py                  │
│                    long_context_reasoner.py              │
│  src/memory/       run_memory.py                         │
│  src/analysis/     pareto_analyzer.py                    │
│  src/policy/       policy_engine.py                      │
│                    policy_feedback.py                    │
│                    (generate_multi_layer_feedback)       │
│  src/dashboard/    green_dashboard.py                    │
│                    symbolic_visualizer.py                │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Symbolic Oversight Layer (v1.2.0 — 4th feedback layer)  │
├──────────────────────────────────────────────────────────┤
│  src/symbolic/     symbolic_reasoning_engine.py          │
│                                                          │
│  • Whitelist rule evaluator (no eval/exec)               │
│  • 15 default rules + 7 enhanced rules (21 total)        │
│  • Action vocabulary: flag_inefficiency,                 │
│    optimize_strategy, block_request, …                   │
│  • Outputs: symbolic_violations.json,                    │
│    symbolic_violation_report.json                        │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Enhancement Modules (src/enhancements/)                 │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ FlexGen Integration                              │   │
│  │ ├─ flexgen_controller.py                         │   │
│  │ ├─ flexgen_policy.py                             │   │
│  │ └─ flexgen_policy_selector.py                    │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ MODP Optimizer                                   │   │
│  │ ├─ pareto_optimizer.py                           │   │
│  │ ├─ pareto_router.py                              │   │
│  │ └─ adaptive_cost_function.py                     │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ LIMIT Graph (Enhanced)                           │   │
│  │ ├─ limit_graph.py                                │   │
│  │ ├─ limit_graph_v2.py                             │   │
│  │ ├─ serendipity_trace.py                          │   │
│  │ ├─ reasoning_path.py                             │   │
│  │ ├─ core/graph_registry.py                        │   │
│  │ ├─ core/causal_graph.py                          │   │
│  │ ├─ core/dag_carbon_ledger.py                     │   │
│  │ └─ core/anomaly_detector.py                      │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Learning Modules                                 │   │
│  │ ├─ bio_inspired/ (Genetic algorithms)            │   │
│  │ ├─ moe_expert_system/ (Mixture of Experts)       │   │
│  │ ├─ rlhf/ (Preference learning)                   │   │
│  │ ├─ multi_teacher_policy_distillation.py          │   │
│  │ └─ distillation_orchestrator.py                  │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Sustainability & Carbon Management               │   │
│  │ ├─ carbon_delay_scheduler.py                     │   │
│  │ ├─ sustainability_cost.py                        │   │
│  │ └─ real_carbon_intensity_api_v10.py              │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Data Center Integration                          │   │
│  │ ├─ ai_data_center_loader.py                      │   │
│  │ ├─ green_datacenter_selector.py                  │   │
│  │ └─ cloud_latency_estimator.py                    │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Helium Integration                               │   │
│  │ ├─ helium_api_collector.py                       │   │
│  │ ├─ helium_elasticity_v10.py                      │   │
│  │ ├─ helium_forecaster_v9.py                       │   │
│  │ ├─ quantum_helium_optimizer_v10.py               │   │
│  │ └─ chromatophore_compartments.py                 │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Monitoring & Metrics                             │   │
│  │ ├─ gpu_profiler.py                               │   │
│  │ ├─ metric_aggregator.py                          │   │
│  │ └─ energy_profiler.py                            │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Security & Trust                                 │   │
│  │ ├─ zero_trust_architecture.py                    │   │
│  │ ├─ schemas/                                      │   │
│  │ │   ├─ node_descriptor.py                        │   │
│  │ │   ├─ workload_descriptor.py                    │   │
│  │ │   └─ feedback_event.py                         │   │
│  │ ├─ async_message_queue.py                        │   │
│  │ ├─ base_classes.py                               │   │
│  │ ├─ adapters.py                                   │   │
│  │ └─ feedback_collector.py                         │   │
│  └─────────────────────────────────────────────────┘   │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Quantum Integration (src/quantum/)                      │
│  (simulator | ibm | rigetti | ionq)                      │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Utilities (src/utils/)                                  │
│  ├─ logger.py           (structured logging)             │
│  ├─ config_loader.py    (YAML + env precedence)          │
│  ├─ validators.py       (schema + unit validation)       │
│  └─ exceptions.py       (typed errors)                   │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Build & Deploy Artifacts                                │
│  ├─ Dockerfile                                           │
│  ├─ pyproject.toml                                       │
│  ├─ MANIFEST.sha256                                      │
│  ├─ .cursorrules                                         │
│  ├─ scripts/                                             │
│  │   ├─ setup.sh · verify.sh · migrate.sh                │
│  │   ├─ cleanup.sh · deploy.sh                           │
│  │   ├─ community_data_collector.py                      │
│  │   └─ monitoring/                                      │
│  └─ config/base/ + config/overlays/[env]/                │
└──────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Request Processing

```
 1. Incoming Task Request
    ↓
 2. Zero-Trust Auth check (ZERO_TRUST_ENABLED? → authenticate_request)
    ↓
 3. Core Agent (agent.py) receives task
    ↓
 4. Orchestrator routes to appropriate modules
    ↓
 5. FlexGen Controller evaluates policies
    ↓
 6. MODP optimizes multi-objectives [accuracy, energy, latency, carbon]
    ↓
 7. LIMIT Graph enforces constraints (graph_centrality, connectivity)
    ↓
 8. Bio-Inspired explores solution space
    ↓
 9. MoE routes to specialized experts
    ↓
10. Policy Distillation refines solution
    ↓
11. Sustainability check (carbon, energy)
    ↓
12. Data center selection
    ↓
13. Symbolic Rule Evaluation (symbolic_reasoning_engine.py)
    │    • 21 rules evaluated against current metrics
    │    • Violations emit symbolic_violations.json
    │    • block_request action halts if ZERO_TRUST_ENABLED
    ↓
14. Multi-Layer Feedback Synthesis
    │    generate_multi_layer_feedback(
    │        pareto_analysis=…,      # 1. objective
    │        reflections=…,          # 2. subjective
    │        symbolic_violations=…,  # 3. symbolic
    │        enhanced_metrics=…,     # 4. enhancements
    │    )
    ↓
15. DAG Carbon Ledger write (immutable provenance)
    ↓
16. Execution with monitoring
    ↓
17. Results returned (+ 11 output artifacts)
```

---

## Four-Layer Feedback

The v1.2.0 architecture combines **four feedback layers** (was 3-layer / dual-layer in v1.0):

```
┌────────────────────────────────────────────────────────┐
│  1. OBJECTIVE                                          │
│     Pareto analysis · efficiency scores                │
│     (src/analysis/pareto_analyzer.py)                  │
└────────────────────┬───────────────────────────────────┘
                     │
┌────────────────────▼───────────────────────────────────┐
│  2. SUBJECTIVE                                         │
│     Agent self-reflections · confidence trends         │
│     (src/reflection/reflection_engine.py +             │
│      src/reflection/long_context_reasoner.py)          │
└────────────────────┬───────────────────────────────────┘
                     │
┌────────────────────▼───────────────────────────────────┐
│  3. SYMBOLIC                                           │
│     Rule violations (whitelist parser, no eval)        │
│     (src/symbolic/symbolic_reasoning_engine.py)        │
│     Outputs: symbolic_violations.json                  │
│              symbolic_violation_report.json            │
└────────────────────┬───────────────────────────────────┘
                     │
┌────────────────────▼───────────────────────────────────┐
│  4. ADVANCED ENHANCEMENTS                              │
│     MODP score · RLHF · graph · distillation · MoE ·   │
│     evolutionary · FlexGen · zero-trust · privacy ·    │
│     training (all in src/enhancements/)                │
└────────────────────┬───────────────────────────────────┘
                     │
                     ▼
          (src/policy/policy_feedback.py)
          generate_multi_layer_feedback(...)
```

---

## Module Integration Points

### Configuration Flow

```
Environment Variables                          (highest precedence)
    ↓
config/environments/.env.[env]                 (secrets + env)
    ↓
config/green_agent.yaml                        (app config)
    ↓
config/free_apis_config.yaml                   (providers)
    ↓
src/enhancements/config.yaml                   (module overrides)
    ↓
symbolic_policy.yaml                           (4th-layer rules)
    ↓
Module-specific configs                        (lowest precedence)
```

Key env vars (canonical — env wins over YAML):
`ENHANCEMENTS_ENABLED`, `LIMIT_GRAPH_ENABLED`, `MODP_ENABLED`, `RLHF_ENABLED`,
`DISTILLATION_ENABLED`, `MOE_ENABLED` (alias `MOE_GATING_ENABLED`),
`EVOLUTIONARY_ENABLED`, `FLEXGEN_ENABLED`, `ZERO_TRUST_ENABLED`,
`DIFFERENTIAL_PRIVACY_ENABLED`, `TRAINING_ENABLED`, `SYMBOLIC_POLICY`,
`LOG_LEVEL`, `LOG_FORMAT`.

See [Configuration Matrix](#configuration-matrix).

### Feedback & Learning

```
Execution Results
    ↓
Metric Aggregation (src/enhancements/metric_aggregator.py)
    ↓
Reflection Checkpoints (src/reflection/reflection_engine.py)
    ↓
Symbolic Rule Evaluation (src/symbolic/symbolic_reasoning_engine.py)
    ↓
Feedback Collection (src/enhancements/feedback_collector.py)
    ↓
RLHF Learning (src/enhancements/rlhf/)
    ↓
Policy Distillation (src/enhancements/multi_teacher_policy_distillation.py)
    ↓
Multi-Layer Synthesis (src/policy/policy_feedback.py)
    ↓
Next iteration improvements
```

### Monitoring & Outputs

```
All Modules
    ↓
GPU Profiler (src/enhancements/gpu_profiler.py)
    ↓
Energy Profiler (src/enhancements/energy_profiler.py)
    ↓
Metrics Aggregation (src/enhancements/metric_aggregator.py)
    ↓
Prometheus (:9090) / Grafana
```

**Output artifacts (11):**

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
| 11 | Enhanced fields in 1 + 4 | Advanced (layer 4) |

---

## Key Design Principles

1. **Modularity**: Each enhancement is independent but can be integrated
2. **Composability**: Modules can be combined in different ways
3. **Configurability**: Everything configurable via YAML/env (env wins)
4. **Observability**: Comprehensive logging, metrics, and 11 output artifacts
5. **Sustainability**: Carbon and energy tracking throughout; DAG ledger for provenance
6. **Scalability**: Kubernetes-ready (`HPA` + `RayCluster` autoscaler; Ray workers
   scale independently from HTTP head pods)
7. **Security**: PSA `restricted`, whitelist rule parser, `block_request` gated by
   `ZERO_TRUST_ENABLED`
8. **Interpretability**: 4-layer feedback, self-explanations, confidence scores,
   reasoning paths

---

## Deployment Architecture

### Local Development

```
scripts/setup.sh
    ↓
source config/environments/.env.development
    ↓
Virtual Environment + Dependencies  (pip install -e .)
    ↓
Python Application                  (python run_agent.py --mode=unified)
```

### Kubernetes (Production)

```
scripts/deploy.sh production
    ↓
config/overlays/production/         (Kustomize overlay)
    ↓
Kubernetes resources (applied in order):
    1. Namespace              green-agent          (PSA restricted)
    2. ServiceAccount         green-agent          (no token automount)
    3. ConfigMap              green-agent-config   (green_agent.yaml + policies)
    4. Secret                 green-agent-secrets  (ZERO_TRUST_TOKEN, …)
    5. Deployment             green-agent          (HTTP head, 3 replicas)
    6. Service                green-agent          (ClusterIP, port 8000)
    7. RayCluster             green-agent-cluster  (head + workers)
    8. HorizontalPodAutoscaler green-agent         (targets Deployment)
    9. PodDisruptionBudget    green-agent          (minAvailable: 1)
   10. NetworkPolicy          green-agent-network-policy
    ↓
Prometheus / Grafana               (scrapes /metrics on port 8000)
```

**Key K8s details:**
- **HPA targets Deployment, not RayCluster** — RayCluster has no `/scale` subresource.
- **Ray workers scale via Ray's in-tree autoscaler** (`enableInTreeAutoscaling`
  + `autoscalerOptions`), not via HPA.
- **Probes** live on the Deployment/RayCluster pod spec — never on the ConfigMap.
- **PSA `restricted`** is enforced at the Namespace level.
- **Custom metrics** (carbon intensity, queue depth) come from Prometheus Adapter.

---

## Security

| Concern | Guarantee |
|---|---|
| Pod Security Admission | Namespace enforces `restricted` (`runAsNonRoot`, `seccompProfile: RuntimeDefault`, `capabilities.drop: [ALL]`) |
| Symbolic rule evaluation | **Whitelist parser** — no `eval`/`exec`, no attribute access, no function calls |
| Allowed rule tokens | identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| YAML parsing | `yaml.safe_load` only |
| `block_request` action | Requires `ZERO_TRUST_ENABLED=true`; otherwise logged and ignored |
| Secrets | Kubernetes `Secret` only — never in ConfigMaps or committed files |
| `ZERO_TRUST_TOKEN` / `RLHF_API_KEY` / `CARBON_API_KEY` / `HELIUM_API_KEY` | Injected via `envFrom: secretRef` |
| ConfigMap contents | Treated as namespace-public — never contains secrets |
| NetworkPolicy | Default-deny ingress + egress; explicit allow-list only |

---

## Configuration Matrix

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

Env vars are canonical; YAML maps onto them (env wins on conflict — matches
Dockerfile v2.0 and Symbolic README v1.2).

---

## Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available (see `.cursorrules`):

```python
ctx_read("src/enhancements/", mode="digest")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")
ctx_read("src/symbolic/symbolic_reasoning_engine.py", mode="signatures")
ctx_read("symbolic_policy.yaml", mode="symbolic")
ctx_read("src/reflection/reflection_engine.py", mode="signatures")
```

---

## What's Not in Scope

- **Z3 / SMT solving** — the symbolic layer is a whitelist rule evaluator, not a
  theorem prover.
- **Temporal logic operators** (`always`, `eventually`, `until`) — use derived
  metrics (`fitness_delta_1h`, `carbon_trend`, `energy_variance`).
- **Cross-agent oversight** — single-agent runtime; federation is future work.
- **Non-Kubernetes production deployment** — `docker run` works, but
  `deploy.sh` targets K8s only.
- **Windows-native execution** — use WSL2 or Docker.

---

## Changelog

### 5.0.0 (2026-04-10)

**Added**

- **`src/symbolic/`** — the 4th feedback layer (symbolic oversight) was
  completely absent; now a first-class block in the diagram.
- **Meta-cognitive layer** — `src/monitoring/`, `src/reflection/`,
  `src/memory/`, `src/analysis/`, `src/policy/`, `src/dashboard/`.
- **12-layer v5.0 modules** — `src/interpretation/`, `src/decision/`,
  `src/optimization/`, `src/benchmarking/`, `src/carbon/`,
  `src/ml_governance/`, `src/governance/`, `src/distributed/`,
  `src/orchestration/`, `src/integration/`.
- **`src/quantum/`** block.
- **`zero_trust_architecture.py`** + `schemas/` + `core/` subpackages in
  the enhancement tree.
- **`symbolic_policy.yaml`** + `free_apis_config.yaml` + `.env.example` in
  the Configuration Layer.
- **`pyproject.toml`, `MANIFEST.sha256`, `Dockerfile`, `.cursorrules`** as
  root build artifacts.
- **Missing scripts** — `verify.sh`, `migrate.sh`, `cleanup.sh`,
  `community_data_collector.py`, `monitoring/`.
- **Four-Layer Feedback** section with diagram.
- **Configuration Matrix** (env ↔ YAML ↔ metric).
- **11 output artifacts** table.
- **Security** section (PSA, whitelist parser, `block_request`, secrets).
- **lean-ctx** inspection section.
- **"What's Not in Scope"**, **Changelog**, **Reviewer Checklist**.
- **Metadata block** + **Table of Contents**.
- **Symbolic evaluation step** and **DAG carbon ledger step** to Data Flow.
- **Zero-trust auth step** to Data Flow.
- **Symbolic layer** to Feedback & Learning loop.
- **`symbolic_violations.json` + `symbolic_violation_report.json`** to
  Monitoring outputs.

**Changed**

- `FLexGen` → **`FlexGen`**.
- `src/core/base_classes.py` → **`src/enhancements/base_classes.py`**.
- `main_integration.py` → **`src/integration/unified_orchestrator.py`**.
- `moe/` → **`moe_expert_system/`**.
- `feedback_collector.py` moved from Integration Layer to enhancements block.
- Deployment section expanded from "Docker Container → Kubernetes Cluster" to
  a 10-resource K8s pipeline with **HPA vs RayCluster** clarification and
  **PSA `restricted`**.
- Configuration Flow now includes `symbolic_policy.yaml` and
  `free_apis_config.yaml`; env vars listed explicitly.
- `src/utils/` interfaces renamed `base_classes.py` → `interfaces.py` (to
  avoid clashing with the enhancements `base_classes.py`).

**Fixed**

- Diagram now shows the 4-layer feedback architecture that all v1.2 docs
  describe.
- Data flow no longer skips symbolic evaluation, zero-trust, or DAG ledger.
- 3-layer terminology (dual/triple) replaced with **4-layer**.

**Removed**

- Unverified `MODULE_GUIDE.md`, `INTEGRATION_GUIDE.md`, `DEPLOYMENT.md`
  links (not in Refactor Guide v0.2). Replaced with actual companion docs.

### Pre-5.0

- Initial architecture with 3-layer feedback and enhancement modules.

---

## Reviewer Checklist

- [ ] `src/symbolic/` appears in the diagram with `symbolic_reasoning_engine.py`.
- [ ] Meta-cognitive modules (`monitoring`, `reflection`, `memory`, `analysis`,
      `policy`, `dashboard`) all appear.
- [ ] v5.0 12-layer modules (`interpretation`, `decision`, `optimization`,
      `benchmarking`, `carbon`, `ml_governance`, `governance`, `distributed`,
      `orchestration`, `integration`) all appear.
- [ ] `src/quantum/` appears.
- [ ] `zero_trust_architecture.py`, `schemas/`, `core/` present in
      enhancements block.
- [ ] No `FLexGen` typo remains.
- [ ] `src/enhancements/base_classes.py` (not `src/core/`).
- [ ] `src/integration/unified_orchestrator.py` (not `main_integration.py`).
- [ ] `moe_expert_system/` (not `moe/`).
- [ ] `symbolic_policy.yaml` and `free_apis_config.yaml` in Configuration Layer.
- [ ] `.env.example` referenced.
- [ ] `pyproject.toml`, `MANIFEST.sha256`, `Dockerfile`, `.cursorrules` in
      build artifacts.
- [ ] `scripts/` includes `verify.sh`, `migrate.sh`, `cleanup.sh`,
      `community_data_collector.py`.
- [ ] Data Flow includes symbolic evaluation, zero-trust, DAG ledger steps.
- [ ] Feedback loop includes symbolic layer.
- [ ] Output artifacts table lists all 11 (including `symbolic_violations.json`).
- [ ] Deployment section shows 10 K8s resources + HPA-vs-RayCluster note +
      PSA `restricted`.
- [ ] §Configuration Matrix matches Dockerfile v2.0 env names.
- [ ] §Security covers whitelist parser, `block_request`, secrets.
- [ ] Companion doc links in header + See Also resolve.
- [ ] Version `5.0.0`, date `2026-04-10`, metadata block present.

---

## See Also

- [`QUICK_START.md`](QUICK_START.md) (v1.2.0) — install → demo → run
- [`SYMBOLIC_OVERSIGHT_README.md`](SYMBOLIC_OVERSIGHT_README.md) (v1.2.0) —
  symbolic rule engine, action vocabulary, severity taxonomy
- [`NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`](NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md) —
  reasoning loop and parser internals
- [`META_COGNITIVE_ENHANCEMENT_COMPLETE.md`](META_COGNITIVE_ENHANCEMENT_COMPLETE.md)
  (v1.2.0) — meta-cognitive delivery summary
- [`NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md`](NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md)
  (v1.2.0) — symbolic integration summary
- [`REPOSITORY_REFACTORING_GUIDE.md`](REPOSITORY_REFACTORING_GUIDE.md) (v0.2.0) —
  src layout and import migration
- [`Scripts and Utilities.md`](Scripts%20and%20Utilities.md) (v1.2.0) — setup,
  verify, migrate, deploy, cleanup
- [`.cursorrules`](.cursorrules) — lean-ctx read modes

---

**Last Updated**: 2026-11-13 · **Version**: 5.0.0 · **Layer versions**: 1.2.0


## Summary of changes vs. original

| Area | Before | After |
|---|---|---|
| **Symbolic layer** | Absent | `src/symbolic/` block + step in Data Flow + artifacts |
| **Meta-cognitive layer** | Absent | `src/{monitoring,reflection,memory,analysis,policy,dashboard}/` blocks |
| **v5.0 modules** | Absent | 10 modules from Guide v5.0 |
| **`zero_trust_architecture.py`** | Absent | First-class in Security & Trust block |
| **`schemas/`, `core/`** | Absent | Sub-packages under enhancements |
| **`src/quantum/`** | Absent | Dedicated block |
| **Config files** | 3 (`green_agent.yaml`, `environments/`, `enhancements/config.yaml`) | 7 (+ `free_apis_config.yaml`, `green_policy.yaml`, `symbolic_policy.yaml`, `.env.example`) |
| **Build artifacts** | Absent | `Dockerfile`, `pyproject.toml`, `MANIFEST.sha256`, `.cursorrules` |
| **Scripts** | 1 (`deploy.sh`) + `monitoring/` | 5 (`setup.sh`, `verify.sh`, `migrate.sh`, `cleanup.sh`, `deploy.sh`) + `community_data_collector.py` + `monitoring/` |
| **Data flow** | 13 steps | 17 steps (adds zero-trust, symbolic, DAG ledger) |
| **Feedback layers** | 3 (implied) | **4** (explicit §Four-Layer Feedback) |
| **Output artifacts** | 0 listed | 11 listed |
| **Deployment K8s resources** | "Kubernetes Cluster" | 10 resources listed in order |
| **Env vars** | 0 | 13 canonical env vars listed |
| **`FLexGen` typo** | Present | Fixed to `FlexGen` |
| **`src/core/base_classes.py`** | Present | Moved to `src/enhancements/base_classes.py` |
| **`main_integration.py`** | Present | → `src/integration/unified_orchestrator.py` |
| **`moe/`** | Present | → `moe_expert_system/` |
| **`feedback_collector.py` location** | Integration Layer | Enhancements block |
| **Security section** | Absent | 10-row table |
| **Configuration Matrix** | Absent | 11-row table |
| **lean-ctx section** | Absent | 6 examples |
| **Metadata / TOC / Changelog** | Absent | Present |
| **What's Not in Scope** | Absent | Present |
| **Reviewer Checklist** | Absent | Present |
| **Dangling links** | `MODULE_GUIDE.md`, `INTEGRATION_GUIDE.md`, `DEPLOYMENT.md` | Replaced with actual v1.2 docs |
