
# 🔗 Green Agent v5.0 Integration Guide

| Field | Value |
|---|---|
| **Guide Version** | 5.0.0 |
| **Companion layer versions** | Meta-cognitive **1.2.0** · Advanced enhancements **1.2.0** · Refactor **0.2.0** |
| **Last Updated** | 2026-11-13 |
| **Status** | Production Ready |
| **Audience** | Integrators upgrading a v4.x repo to v5.0 |
| **Companion docs** | `QUICK_START.md` (v1.2.0) · `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` · `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0) · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `Scripts and Utilities.md` (v1.2.0) · `.cursorrules` |

> **Versioning note:** the "v5.0" label refers to the *integration guide* release.
> The underlying layers — meta-cognitive, symbolic oversight, advanced
> enhancements — are all at **v1.2.0**. Use the v1.2.0 docs for layer-specific
> reference, and this guide for the one-time integration steps.

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Integration](#step-by-step-integration)
4. [File Placement](#file-placement)
5. [Configuration](#configuration)
6. [Metric Reference](#metric-reference)
7. [Testing Integration](#testing-integration)
8. [Security](#security)
9. [Docker Integration](#docker-integration)
10. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
11. [Troubleshooting](#troubleshooting)
12. [Migration from v4 to v5](#migration-from-v4-to-v5)
13. [Advanced Enhancements Integration](#advanced-enhancements-integration)
14. [What's Not in Scope](#whats-not-in-scope)
15. [Changelog](#changelog)
16. [Reviewer Checklist](#reviewer-checklist)

---

## 🎯 Overview

This guide walks you through integrating:

- **~15 new v5.0 modules** (workload interpreter, decision core, synthetic data optimizer, benchmark intelligence, task carbon profiler, multi-objective scheduler, adaptation classifier, ML governance policy engine, carbon ledger, Ray cluster manager, forecasting engine, eco-mode controller, plus supporting modules)
- **8 meta-cognitive modules** — `src/monitoring/`, `src/reflection/`, `src/memory/`, `src/analysis/`, `src/policy/`, `src/dashboard/` (v1.2.0)
- **The neuro-symbolic oversight layer** — `src/symbolic/`, `src/dashboard/symbolic_visualizer.py`, `symbolic_policy.yaml` (v1.2.0)
- **10 advanced enhancement modules** — LIMIT Graph, MODP, RLHF, Multi-Teacher On-Policy Distillation, Evolutionary, MoE, FlexGen, Zero Trust, Privacy, Training (v1.2.0)

The resulting system is a **4-layer feedback architecture**:

1. **Objective** — Pareto analysis
2. **Subjective** — agent self-reflections
3. **Symbolic** — rule violations (whitelist parser)
4. **Advanced enhancements** — MODP, RLHF, graph, distillation, MoE, evolutionary, FlexGen, zero trust, privacy, training

### What you're integrating

**v5.0 modules (new):**
- `workload_interpreter.py`, `carbon_aware_decision_core.py`, `synthetic_data_optimizer.py`
- `benchmark_intelligence.py`, `adaptation_classifier.py`, `policy_engine.py` (ML governance)
- `carbon_ledger.py`, `task_carbon_profiler.py`, `forecasting_engine.py`, `eco_mode_controller.py`
- `ray_cluster_manager.py`, `carbon_aware_scheduler.py`, `multi_objective_scheduler.py`
- `unified_orchestrator.py`
- plus 2 supporting modules (`async_message_queue.py`, `dag_carbon_ledger.py` under `core/`)

**Advanced enhancement modules (v1.2.0):**
- `schemas/node_descriptor.py`, `schemas/workload_descriptor.py`, `schemas/feedback_event.py`
- `zero_trust_architecture.py`
- `core/graph_registry.py`, `core/causal_graph.py`, `core/dag_carbon_ledger.py`, `core/anomaly_detector.py`
- FlexGen integration hooks (config + delegation policy)

**Meta-cognitive modules (v1.2.0):**
- `monitoring/metrics_collector.py`
- `reflection/reflection_engine.py`, `reflection/long_context_reasoner.py`
- `memory/run_memory.py`, `analysis/pareto_analyzer.py`
- `policy/policy_engine.py`, `policy/policy_feedback.py`
- `dashboard/green_dashboard.py`, `dashboard/symbolic_visualizer.py`

**Symbolic layer (v1.2.0):**
- `symbolic/symbolic_reasoning_engine.py`

**Existing modules (unchanged):**
- PPO + Q-Table Hybrid, Carbon Forecast, Temporal Shifter, Negawatt Reward, Green Leaderboard, Carbon Credit Simulator, Quantum LIMIT-Graph

---

## 📦 Prerequisites

### 1. System Requirements

```text
- Python 3.10+ (3.12.3 tested; matches Dockerfile v2.0)
- 16 GB RAM (for Ray cluster)
- 20 GB disk space
- git ≥ 2.30
- Optional: docker ≥ 24, kubectl ≥ 1.28 (for deploy)
```

**Pre-flight check:**

```bash
python3 -c "import sys; assert sys.version_info >= (3,10), f'Python 3.10+ required, got {sys.version}'; print('✓ Python OK')"
command -v git >/dev/null || { echo "git required"; exit 1; }
df -h . | awk 'NR==2 { if ($4+0 < 20*1024*1024) { print "✗ <20GB free"; exit 1 } }'
```

### 2. Install Base Dependencies

```bash
cd Green_Agent

# Upgrade pip + install base requirements
pip install --upgrade pip
pip install -r requirements.txt

# Register the src/ layout (REQUIRED — makes `from src.* import …` work)
pip install -e .

# Fail on dependency conflicts
pip check
```

> **Why `pip install -e .`?** `pyproject.toml` declares the `src/` layout; without
> the editable install, `from src.enhancements import …` fails unless you set
> `PYTHONPATH=$(pwd)/src`. See `REPOSITORY_REFACTORING_GUIDE.md` v0.2.0 §Packaging.

### 3. Install v5.0 Module Dependencies

```bash
pip install 'ray[default]>=2.9.0' 'prophet>=1.1.0' 'sentence-transformers>=2.2.0' \
            'fastapi>=0.109.0' 'plotly>=5.18.0'
```

> **Size warning:** `sentence-transformers` pulls PyTorch (~500 MB+) and
> downloads a model at first use; `prophet` pulls Stan (~200 MB). Skip these if
> you are not using embedding-based reasoning or forecasting respectively.

### 4. Install Advanced Enhancements Dependencies

```bash
pip install 'scikit-learn>=1.3.0' 'pandas>=2.0.0' 'pydantic>=2.0.0' 'pydantic-settings>=2.0.0' \
            'cryptography>=41.0.0' 'pyjwt>=2.8.0' 'prometheus-client>=0.17.0' \
            'tenacity>=8.2.0' 'aiofiles>=23.0.0' 'aiohttp>=3.8.0' 'networkx>=3.1' 'joblib>=1.3.0'
pip check
```

### 5. Verify Existing Components

```bash
python -c "from rewards.negawatt_reward import NegawattReward; print('✓ NegawattReward OK')"
python -c "from src.reflection.reflection_engine import ReflectionEngine; print('✓ Meta-cognition OK')"
```

> **Note:** the old path `core/meta_cognition.py` does **not** exist post-refactor.
> The meta-cognitive engine lives at `src/reflection/reflection_engine.py`.

---

## 🔧 Step-by-Step Integration

### STEP 1 — Create directory structure

```bash
cd Green_Agent

# v5.0 modules
mkdir -p src/{interpretation,decision,optimization,benchmarking,carbon,orchestration,ml_governance,governance,distributed,integration}

# Meta-cognitive + symbolic + enhancements (v1.2.0 layout)
mkdir -p src/{monitoring,reflection,memory,analysis,policy,dashboard,symbolic,utils,quantum}
mkdir -p src/enhancements/{schemas,core}
mkdir -p config scripts tests/{unit,integration,scripts} docs examples
```

### STEP 2 — Copy v5.0 modules

```bash
DOWNLOAD_DIR="path/to/downloaded/modules"

cp "$DOWNLOAD_DIR/workload_interpreter.py"        src/interpretation/
cp "$DOWNLOAD_DIR/carbon_aware_decision_core.py"  src/decision/
cp "$DOWNLOAD_DIR/synthetic_data_optimizer.py"    src/optimization/
cp "$DOWNLOAD_DIR/benchmark_intelligence.py"      src/benchmarking/
cp "$DOWNLOAD_DIR/adaptation_classifier.py"       src/ml_governance/
cp "$DOWNLOAD_DIR/policy_engine.py"               src/ml_governance/
cp "$DOWNLOAD_DIR/carbon_ledger.py"               src/governance/
cp "$DOWNLOAD_DIR/task_carbon_profiler.py"        src/carbon/
cp "$DOWNLOAD_DIR/forecasting_engine.py"          src/carbon/
cp "$DOWNLOAD_DIR/eco_mode_controller.py"         src/carbon/
cp "$DOWNLOAD_DIR/ray_cluster_manager.py"         src/distributed/
cp "$DOWNLOAD_DIR/carbon_aware_scheduler.py"      src/distributed/
cp "$DOWNLOAD_DIR/multi_objective_scheduler.py"   src/orchestration/
cp "$DOWNLOAD_DIR/unified_orchestrator.py"        src/integration/
```

### STEP 3 — Copy advanced enhancements (v1.2.0)

> **Canonical source path is `src/enhancements/`.** The legacy path
> `quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements`
> is **not a valid Python package** (hyphens/dots) and has been replaced by the
> refactor — see `REPOSITORY_REFACTORING_GUIDE.md` v0.2.0. **Do not** create
> symlinks; they break on Windows checkouts and inside Docker `COPY`.

```bash
# If you already have src/enhancements/ in the repo (post-refactor):
git mv src/enhancements ./   # no-op if already there

# If you are copying from a separate bundle:
cp -r /path/to/enhancements_bundle/* src/enhancements/

# Layout should match:
# src/enhancements/
#   __init__.py
#   base_classes.py
#   adapters.py
#   config.yaml
#   async_message_queue.py
#   zero_trust_architecture.py
#   schemas/{__init__.py, node_descriptor.py, workload_descriptor.py, feedback_event.py}
#   core/{__init__.py, graph_registry.py, causal_graph.py, dag_carbon_ledger.py, anomaly_detector.py}
```

Also copy the symbolic layer + meta-cognitive modules:

```bash
cp "$DOWNLOAD_DIR/symbolic_reasoning_engine.py"   src/symbolic/
cp "$DOWNLOAD_DIR/symbolic_visualizer.py"         src/dashboard/
cp "$DOWNLOAD_DIR/metrics_collector.py"           src/monitoring/
cp "$DOWNLOAD_DIR/reflection_engine.py"           src/reflection/
cp "$DOWNLOAD_DIR/long_context_reasoner.py"       src/reflection/
cp "$DOWNLOAD_DIR/run_memory.py"                  src/memory/
cp "$DOWNLOAD_DIR/pareto_analyzer.py"             src/analysis/
cp "$DOWNLOAD_DIR/policy_engine.py"               src/policy/
cp "$DOWNLOAD_DIR/policy_feedback.py"             src/policy/
cp "$DOWNLOAD_DIR/green_dashboard.py"             src/dashboard/
```

### STEP 4 — Create `__init__.py` files

```bash
# Root + subpackages
touch src/__init__.py
for d in interpretation decision optimization benchmarking carbon orchestration \
         ml_governance governance distributed integration monitoring reflection \
         memory analysis policy dashboard symbolic utils quantum; do
  touch "src/$d/__init__.py"
done

# Enhancements + its subpackages (REQUIRED — subpackages are not implicitly importable)
touch src/enhancements/__init__.py
touch src/enhancements/schemas/__init__.py
touch src/enhancements/core/__init__.py
```

**`src/integration/__init__.py`:**

```python
from .unified_orchestrator import (
    UnifiedGreenAgent,
    create_unified_agent,
    UnifiedResult,
)

__all__ = ["UnifiedGreenAgent", "create_unified_agent", "UnifiedResult"]
```

**`src/enhancements/__init__.py` (v1.2.0 — matches `community_data_collector.py` imports):**

```python
from .schemas.node_descriptor import (
    NodeDescriptor, NodeType, CoolingType, MaintenanceStatus,
    RoutingStrategy, create_node_descriptor,
)
from .schemas.workload_descriptor import (
    WorkloadDescriptor, TaskType, Urgency, Priority, BioMode,
    create_workload_descriptor,
)
from .schemas.feedback_event import FeedbackEvent
from .zero_trust_architecture import (
    ZeroTrustArchitecture, ZeroTrustConfig, SecurityContext, SecurityException,
)
from .core.graph_registry import GraphRegistry, GraphType
from .core.causal_graph import CausalGraph
from .core.dag_carbon_ledger import DAGCarbonLedger

__all__ = [
    "NodeDescriptor", "NodeType", "CoolingType", "MaintenanceStatus",
    "RoutingStrategy", "create_node_descriptor",
    "WorkloadDescriptor", "TaskType", "Urgency", "Priority", "BioMode",
    "create_workload_descriptor",
    "FeedbackEvent",
    "ZeroTrustArchitecture", "ZeroTrustConfig", "SecurityContext", "SecurityException",
    "GraphRegistry", "GraphType", "CausalGraph", "DAGCarbonLedger",
]
```

**`src/symbolic/__init__.py`:**

```python
from .symbolic_reasoning_engine import SymbolicReasoningEngine

__all__ = ["SymbolicReasoningEngine"]
```

**`src/policy/__init__.py`:**

```python
from .policy_engine import PolicyEngine
from .policy_feedback import PolicyFeedback

__all__ = ["PolicyEngine", "PolicyFeedback"]
```

### STEP 5 — Update `run_agent.py`

```python
# run_agent.py (v5.0 argparse surface)
import argparse
import asyncio
import os
import sys

# v5.0 unified orchestrator
try:
    from src.integration.unified_orchestrator import create_unified_agent
    UNIFIED_AVAILABLE = True
except ImportError:
    UNIFIED_AVAILABLE = False

# Enhanced mode
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor  # noqa: F401
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

# Symbolic layer
try:
    from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine  # noqa: F401
    SYMBOLIC_AVAILABLE = True
except ImportError:
    SYMBOLIC_AVAILABLE = False


async def run_unified(task, region, symbolic_policy):
    agent = await create_unified_agent(
        enhancements=os.environ.get("ENHANCEMENTS_ENABLED", "false").lower() == "true",
        symbolic_policy=symbolic_policy,
    )
    try:
        result = await agent.execute(task, region=region)
        print(result)
    finally:
        await agent.shutdown()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Green Agent v5.0")
    parser.add_argument("--mode", choices=["legacy", "unified", "compare"], default="unified",
                        help="Execution mode (compare runs both legacy and unified)")
    parser.add_argument("--enhancements", action="store_true",
                        help="Enable advanced enhancements (sugar for ENHANCEMENTS_ENABLED=true)")
    parser.add_argument("--no-enhancements", action="store_true",
                        help="Disable enhancements (overrides ENHANCEMENTS_ENABLED)")
    parser.add_argument("--region", default="US-CA", help="Carbon region (default: US-CA)")
    parser.add_argument("--policy", default="green_policy.yaml", help="Policy file")
    parser.add_argument("--symbolic-policy", default="symbolic_policy.yaml",
                        help="Symbolic rule file (oversight is always-on when present)")
    parser.add_argument("--num-workers", type=int, default=8, help="Ray worker count")
    parser.add_argument("--dry-run", action="store_true", help="Do not write outputs")
    return parser


def _resolve_enhanced(args) -> bool:
    if args.no_enhancements:
        return False
    if args.enhancements:
        return True
    return os.environ.get("ENHANCEMENTS_ENABLED", "false").lower() == "true"


def main() -> int:
    args = build_parser().parse_args()

    enhanced = _resolve_enhanced(args)
    if enhanced and not ENHANCEMENTS_AVAILABLE:
        print("⚠️  Enhancements requested but modules unavailable", file=sys.stderr)
        enhanced = False

    if args.symbolic_policy and not os.path.exists(args.symbolic_policy):
        print(f"ℹ️  No symbolic_policy.yaml at {args.symbolic_policy}; symbolic layer skipped.")

    task = {
        "region": args.region,
        "policy": args.policy,
        "dry_run": args.dry_run,
    }

    try:
        if args.mode == "unified" and UNIFIED_AVAILABLE:
            asyncio.run(run_unified(task, args.region, args.symbolic_policy))
        elif args.mode == "compare":
            asyncio.run(run_unified(task, args.region, args.symbolic_policy))
            # legacy path via existing run() in your repo
            run()  # noqa: F821
        else:
            run()  # noqa: F821  (existing v4 entrypoint)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

> `--enhancements` is **sugar** for `ENHANCEMENTS_ENABLED=true`. Per-module flags
> (e.g. `FLEXGEN_ENABLED=true`) still apply. Env wins over YAML on conflict.

### STEP 6 — Move existing components (optional)

```bash
git mv core/meta_cognition.py src/reflection/reflection_engine.py       # if still at old path
git mv core/meta_cognition_v2.py src/reflection/long_context_reasoner.py
git mv dashboard/green_dashboard.py src/dashboard/
git mv policy/policy_engine.py src/policy/
git mv policy/policy_feedback.py src/policy/
```

Use `git mv` (not `mv`) so `git log --follow` preserves history — see
`REPOSITORY_REFACTORING_GUIDE.md` v0.2.0 §Gradual Migration.

### STEP 7 — Configure system

**Create `config/green_agent.yaml`** (note: no `_config` suffix — matches v1.2.0 docs):

```yaml
system:
  version: "5.0.0"
  mode: "unified"

ray:
  num_workers: 8
  dashboard_port: 8265

carbon:
  default_region: "US-CA"

policy:
  mode: "moderate"

# ---- Advanced enhancements (v1.2.0) ----
# Env vars are canonical (ENHANCEMENTS_ENABLED etc.); YAML maps onto them.
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
  evolutionary:                    # renamed from `bio_inspired` in v1.2
    enabled: true                  # ↔ EVOLUTIONARY_ENABLED
    population_size: 20
    mutation_rate: 0.1
  moe:                             # renamed from `moe_expert` in v1.2
    enabled: true                  # ↔ MOE_ENABLED (alias MOE_GATING_ENABLED)
    n_experts: 4
  flexgen:
    enabled: false                 # ↔ FLEXGEN_ENABLED (opt-in)
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

**Create `symbolic_policy.yaml`** (4th layer — always-on when present):

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon combined with high latency"

  - id: "ENH-MODP-001"
    name: "Low MODP Score Alert"
    category: "enhanced"
    priority: "medium"           # NOT "warning"
    condition: "modp_score < 0.4"
    action: "trigger_policy_review"
    explanation: "Overall multi-objective performance is poor"

  - id: "ENH-FLEX-001"
    name: "FlexGen Energy Spike"
    category: "enhanced"
    priority: "medium"
    condition: "flexgen_energy_kwh > 1.0"
    action: "switch_precision"
    explanation: "FlexGen energy above 1 kWh — drop precision"
```

**Secrets** — never commit `.env.*`; use `.env.example` as the template:

```bash
# .env.example (checked in)
ENHANCEMENTS_ENABLED=true
MODP_ENABLED=true
RLHF_ENABLED=true
FLEXGEN_ENABLED=false
ZERO_TRUST_ENABLED=false
ZERO_TRUST_TOKEN=            # required when ZERO_TRUST_ENABLED=true
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### STEP 8 — Update `requirements.txt`

```text
# Core (already present)
-r requirements-base.txt

# v5.0 additions
ray[default]>=2.9.0
prophet>=1.1.0
sentence-transformers>=2.2.0
fastapi>=0.109.0
plotly>=5.18.0

# Advanced enhancements (v1.2.0)
scikit-learn>=1.3.0
pandas>=2.0.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
cryptography>=41.0.0
pyjwt>=2.8.0
prometheus-client>=0.17.0
tenacity>=8.2.0
aiofiles>=23.0.0
aiohttp>=3.8.0
networkx>=3.1
joblib>=1.3.0
```

Then:

```bash
pip install -r requirements.txt
pip check
```

### STEP 9 — Initialize Ray

```bash
ray start --head --dashboard-port=8265
ray status
```

Stop cleanly:

```bash
ray stop
```

---

## 📂 Final File Placement

```
Green_Agent/
├── src/
│   ├── interpretation/
│   ├── decision/
│   ├── optimization/
│   ├── benchmarking/
│   ├── carbon/
│   ├── ml_governance/
│   ├── governance/
│   ├── distributed/
│   ├── orchestration/
│   ├── integration/
│   ├── monitoring/                 # v1.2.0 meta-cognitive
│   ├── reflection/                 # v1.2.0 meta-cognitive
│   ├── memory/                     # v1.2.0 meta-cognitive
│   ├── analysis/                   # v1.2.0 meta-cognitive
│   ├── policy/                     # v1.2.0 meta-cognitive
│   │   ├── policy_engine.py
│   │   └── policy_feedback.py      # multi-layer feedback
│   ├── dashboard/                  # v1.2.0
│   │   ├── green_dashboard.py
│   │   └── symbolic_visualizer.py
│   ├── symbolic/                   # v1.2.0 (4th layer)
│   │   └── symbolic_reasoning_engine.py
│   ├── utils/
│   ├── quantum/
│   └── enhancements/               # v1.2.0
│       ├── __init__.py
│       ├── base_classes.py
│       ├── adapters.py
│       ├── config.yaml
│       ├── async_message_queue.py
│       ├── zero_trust_architecture.py
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── node_descriptor.py
│       │   ├── workload_descriptor.py
│       │   └── feedback_event.py
│       └── core/
│           ├── __init__.py
│           ├── graph_registry.py
│           ├── causal_graph.py
│           ├── dag_carbon_ledger.py      # moved from metrics/ in v1.2
│           └── anomaly_detector.py
├── config/
│   ├── green_agent.yaml            # note: no `_config` suffix (v1.2)
│   ├── green_policy.yaml
│   └── symbolic_policy.yaml        # 4th layer
├── scripts/                        # see Scripts and Utilities.md v1.2.0
│   ├── setup.sh
│   ├── verify.sh
│   ├── migrate.sh
│   ├── cleanup.sh
│   ├── deploy.sh
│   ├── community_data_collector.py
│   └── monitoring/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── benchmarks/
├── examples/
├── docs/
├── Dockerfile                      # v2.0, multi-stage, non-root, healthcheck
├── pyproject.toml                  # src layout + package data
├── MANIFEST.sha256                 # file-preservation inventory
├── .cursorrules                    # lean-ctx read modes
├── run_agent.py                    # v5.0 argparse surface (see STEP 5)
├── demo_meta_cognitive.py
├── demo_symbolic_oversight.py
├── test_meta_cognitive.py
├── test_symbolic_oversight.py      # 19 tests
├── requirements.txt
└── README.md
```

---

## ⚙️ Configuration

### Env var ↔ YAML mapping (env wins)

| Env | YAML key | Default | Metric |
|---|---|---|---|
| `ENHANCEMENTS_ENABLED` | `enhancements.enabled` | `false` | — |
| `LIMIT_GRAPH_ENABLED` | `enhancements.limit_graph.enabled` | `true` | `graph_centrality`, `graph_connectivity` |
| `MODP_ENABLED` | `enhancements.modp.enabled` | `true` | `modp_score` |
| `RLHF_ENABLED` | `enhancements.rlhf.enabled` | `true` | `human_feedback_score` |
| `DISTILLATION_ENABLED` | `enhancements.distillation.enabled` | `true` | `distillation_update_rate` |
| `MOE_ENABLED` (alias `MOE_GATING_ENABLED`) | `enhancements.moe.enabled` | `true` | `moe_gate_stddev` |
| `EVOLUTIONARY_ENABLED` | `enhancements.evolutionary.enabled` | `true` | `evolutionary_best_fitness` |
| `FLEXGEN_ENABLED` | `enhancements.flexgen.enabled` | `false` | `flexgen_energy_kwh` |
| `ZERO_TRUST_ENABLED` | `enhancements.zero_trust.enabled` | `false` | trust state (via rules) |
| `DIFFERENTIAL_PRIVACY_ENABLED` | `enhancements.privacy.enabled` | `false` | DP ε/δ budget |
| `TRAINING_ENABLED` | `enhancements.training.enabled` | `false` | checkpoint state |
| `SYMBOLIC_POLICY` | — (file path) | `symbolic_policy.yaml` | violation traces |

### Symbolic layer

Symbolic oversight is **always-on when `symbolic_policy.yaml` exists**. No enable
flag needed. Actions are dispatched from the fixed vocabulary in
`SYMBOLIC_OVERSIGHT_README.md` v1.2.0 §Rule Action Vocabulary.

---

## 📐 Metric Reference

| Variable | Unit | Range | Source |
|---|---|---|---|
| `energy` | Wh | ≥ 0 | step metrics |
| `carbon` | g | ≥ 0 | step metrics |
| `latency` | ms | ≥ 0 | step metrics |
| `memory` | MB | ≥ 0 | step metrics |
| `tool_calls` | count | ≥ 0 | step metrics |
| `modp_score` | — | 0–1, higher better | `MODP_ENABLED` |
| `graph_centrality` | — | 0–1 | `LIMIT_GRAPH_ENABLED` |
| `human_feedback_score` | — | 0–1, higher better | `RLHF_ENABLED` |
| `moe_gate_stddev` | — | 0–1 | `MOE_ENABLED` |
| `evolutionary_best_fitness` | — | 0–1, higher better | `EVOLUTIONARY_ENABLED` |
| `flexgen_energy_kwh` | kWh | ≥ 0 | `FLEXGEN_ENABLED` |

Full table: `SYMBOLIC_OVERSIGHT_README.md` v1.2.0 §Metric Reference.

---

## 🧪 Testing Integration

### 1. Test imports

```bash
python -c "from src.integration.unified_orchestrator import create_unified_agent; print('✓ orchestrator OK')"
python -c "from src.enhancements import NodeDescriptor, WorkloadDescriptor; print('✓ enhancements OK')"
python -c "from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine; print('✓ symbolic OK')"
python -c "from src.policy import PolicyEngine, PolicyFeedback; print('✓ policy OK')"
```

### 2. Run `verify.sh` (v1.2 5-check contract)

```bash
./scripts/verify.sh           # MANIFEST + old imports + CI paths + smoke + pytest
./scripts/verify.sh --strict  # adds docker build + shellcheck
```

### 3. Unit tests

```bash
pytest tests/unit/ -v
```

### 4. Integration tests

```bash
pytest tests/integration/test_unified_system.py -v
```

### 5. Symbolic suite (19 tests)

```bash
python test_symbolic_oversight.py
# or:
python -m pytest test_symbolic_oversight.py -v
```

### 6. Run modes

```bash
# Legacy (v4 entrypoint preserved)
python run_agent.py --mode=legacy

# Unified (4-layer feedback)
python run_agent.py --mode=unified

# Unified + enhancements
python run_agent.py --mode=unified --enhancements --region=US-CA

# Unified + enhancements + symbolic
python run_agent.py --mode=unified --enhancements --symbolic-policy=symbolic_policy.yaml

# Baseline (enhancements off) — for comparison
ENHANCEMENTS_ENABLED=false python run_agent.py --mode=unified --no-enhancements

# Comparison
python run_agent.py --mode=compare

# Dry-run (no output files)
python run_agent.py --mode=unified --dry-run
```

### 7. Expected output (with enhancements + symbolic)

```text
╔═══════════════════════════════════════════════════════════════════╗
║         🌱 GREEN AGENT v5.0 - Sustainable AI Runtime 🌱          ║
╚═══════════════════════════════════════════════════════════════════╝

🔧 Initializing Unified Green Agent (4-layer feedback)...
✅ Layer 0: Workload Interpreter initialized
✅ Layer 3: Decision Core initialized
...
✅ Advanced enhancements initialized (NodeDescriptor, WorkloadDescriptor, …)
✅ Symbolic reasoning engine loaded (15 rules)
🧠 Selected routing strategy: carbon_first
🧠 Selected priority: green
🔍 Symbolic violations: 0 (or listed)
...
✅ EXECUTION COMPLETE
Carbon Saved: <value> kgCO2e (<pct>%)

Output artifacts:
  results.json · dashboard.html · reflections.json
  pareto_analysis.json · run_memory.json · dashboard_data.json
  symbolic_violations.json · symbolic_violation_report.json
```

> **Benchmark claim:** the "83.3% savings" figure in the previous version had
> no methodology. Record yours via `python tests/benchmarks/run_benchmark.py`
> and cite the hardware, workload, and baseline.

---

## 🔐 Security

| Concern | Guarantee |
|---|---|
| Symbolic rule evaluation | **Whitelist parser** — no `eval`/`exec`, no attribute access, no function calls |
| Allowed tokens | identifiers, numbers, `> < >= <= == != AND OR NOT ( )` |
| YAML parsing | `yaml.safe_load` only |
| `block_request` | Requires `ZERO_TRUST_ENABLED=true`; otherwise logged and ignored |
| `ZERO_TRUST_TOKEN` | Required when `ZERO_TRUST_ENABLED=true`; never committed (K8s Secret) |
| `RLHF_API_KEY` | Required when remote RLHF is used; never committed |
| `.env.*` | Gitignored; `.env.example` is the only committed file |
| `cleanup.sh` | Dry-run by default; `--force` required to delete |
| `migrate.sh` | Refuses to run unless `git status --porcelain` is clean |

Full contract: `SYMBOLIC_OVERSIGHT_README.md` v1.2.0 §Security.

---

## 🐳 Docker Integration

```bash
# Full image (all enhancements + symbolic layer)
docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .

# Lean image (baseline)
docker build --build-arg ENHANCEMENTS_ENABLED=false -t green-agent:lean .

# Run the 19-test symbolic suite inside the image
docker run --rm green-agent:full python test_symbolic_oversight.py
```

> Per-module flags (`MODP_ENABLED`, `RLHF_ENABLED`, `FLEXGEN_ENABLED`, …) are
> passed the same way. See `Dockerfile` v2.0 §Advanced Enhancements.

---

## 🧠 Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available (see `.cursorrules`):

```python
ctx_read("src/enhancements/", mode="digest")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")
ctx_read("src/enhancements/core/dag_carbon_ledger.py", mode="digest")
```

---

## 🔧 Troubleshooting

### Issue 1: `from src.enhancements import …` fails

**Cause:** `src/` is not on `sys.path`.
**Fix:** run `pip install -e .` (uses `pyproject.toml`). As a fallback:

```bash
export PYTHONPATH="$(pwd)/src:${PYTHONPATH}"
```

> ⚠️ Do **not** add just `$(pwd)` — the `src` layout requires `$(pwd)/src`.

### Issue 2: Ray won't start

**Cause:** stale Ray session, port conflict, or OOM.

```bash
ray stop --force
rm -rf /tmp/ray
ray start --head --dashboard-port=8265 --num-cpus=$(nproc)
```

### Issue 3: Enhancements not activating

Verify:

- `src/enhancements/` exists with `schemas/__init__.py`, `core/__init__.py`.
- `pip install -e .` was run.
- `ENHANCEMENTS_ENABLED=true` (env) **or** `--enhancements` (CLI sugar).
- Per-module flag set (e.g. `MODP_ENABLED=true`).
- `python -c "from src.enhancements import NodeDescriptor"` succeeds.

### Issue 4: `--mode=compare` fails

**Cause:** older `argparse` snippet didn't include `compare` in `choices`.
**Fix:** use the STEP 5 snippet — `choices=["legacy", "unified", "compare"]`.

### Issue 5: Symbolic rules never trigger

**Cause:** unit mismatch, missing `symbolic_policy.yaml`, or invalid priority.

- Carbon: **grams** (not kg); `cumulative.total_carbon_kg` is kg
- `flexgen_energy_kwh` is **kWh** (not joules)
- `priority` must be `critical` / `high` / `medium` / `low` — **not** `warning`

### Issue 6: `verify.sh` fails at check 1 (MANIFEST)

```bash
git status                        # see what changed
git ls-files | xargs sha256sum > MANIFEST.sha256   # regenerate if intentional
```

### Issue 7: Docker build fails with `invalid reference format`

**Cause:** `PYTHON_IMAGE` ARG contains `REPLACE_ME`.
**Fix:** pin a real digest — see `Dockerfile` v2.0 §Base image.

---

## 🔄 Migration from v4 to v5

### Backward compatibility

- v4 entrypoints (`run()`) still work via `--mode=legacy`.
- Policy files remain compatible; `bio_inspired` and `moe_expert` are accepted as aliases for one minor release.
- No public class renamed.

### Migration table

| Before (v4) | After (v5 / v1.2.0) | Notes |
|---|---|---|
| `core/meta_cognition.py` | `src/reflection/reflection_engine.py` + `long_context_reasoner.py` | Paths |
| `dashboard/green_dashboard.py` | `src/dashboard/green_dashboard.py` | Paths |
| `policy/policy_engine.py` | `src/policy/policy_engine.py` | Paths |
| `src/enhancements/metrics/dag_carbon_ledger.py` | `src/enhancements/core/dag_carbon_ledger.py` | Paths |
| `bio_inspired.enabled` | `evolutionary.enabled` / `EVOLUTIONARY_ENABLED` | Flag rename |
| `moe_expert.enabled` | `moe.enabled` / `MOE_ENABLED` | Flag rename |
| `flexgen_energy_joules` | `flexgen_energy_kwh` | Unit fix |
| `priority: "warning"` | `priority: "medium"` | Enum |
| `generate_dual_layer_feedback()` | `generate_multi_layer_feedback()` | Deprecated alias kept |
| `config/green_agent_config.yaml` | `config/green_agent.yaml` | Filename |
| `pip install -r requirements.txt` | `pip install -e .` (after `pip install -r requirements.txt`) | Install |
| `quantum_integration/...` imports | `src.enhancements.*` | Valid package names |
| Symlinks for enhancements | `git mv` + `pyproject.toml` | Forbidden pattern removed |

---

## 📊 Advanced Enhancements Integration

### Files to integrate

```
src/enhancements/
  __init__.py                     # re-exports (see STEP 4)
  schemas/
    __init__.py
    node_descriptor.py
    workload_descriptor.py
    feedback_event.py
  zero_trust_architecture.py
  async_message_queue.py
  core/
    __init__.py
    graph_registry.py
    causal_graph.py
    dag_carbon_ledger.py
    anomaly_detector.py
```

### Enabling

Env (canonical) or YAML (see §Configuration). Symbolic layer is always-on with
`symbolic_policy.yaml`.

### Feature mapping

| Feature | Module | Enable flag | Metric |
|---|---|---|---|
| LIMIT Graph | `core/graph_registry.py` | `LIMIT_GRAPH_ENABLED` | `graph_centrality`, `graph_connectivity` |
| MODP | `schemas/node_descriptor.py` | `MODP_ENABLED` | `modp_score` |
| RLHF | `schemas/feedback_event.py` | `RLHF_ENABLED` | `human_feedback_score` |
| Distillation | `schemas/node_descriptor.py` | `DISTILLATION_ENABLED` | `distillation_update_rate` |
| MoE | `schemas/node_descriptor.py` | `MOE_ENABLED` | `moe_gate_stddev` |
| Evolutionary | `schemas/node_descriptor.py` | `EVOLUTIONARY_ENABLED` | `evolutionary_best_fitness` |
| FlexGen | `config/green_agent.yaml` + hooks | `FLEXGEN_ENABLED` | `flexgen_energy_kwh` |
| Zero Trust | `zero_trust_architecture.py` | `ZERO_TRUST_ENABLED` | trust state |
| Privacy (DP) | `core/` (if present) | `DIFFERENTIAL_PRIVACY_ENABLED` | DP ε/δ budget |
| Training | `core/` (if present) | `TRAINING_ENABLED` | checkpoint state |
| Symbolic | `src/symbolic/` | always-on with `symbolic_policy.yaml` | violation traces |

---

## 🚫 What's Not in Scope

- **Windows native execution** — use WSL2 or Docker.
- **Non-Kubernetes production deployment** — `docker run` works, `deploy.sh` is K8s-only.
- **Z3 / SMT solving** — symbolic layer is a whitelist rule evaluator, not a theorem prover.
- **Cross-agent oversight** — planned.
- **Automatic secret rotation** — provide K8s Secret templates, not rotation tooling.

---

## 📝 Changelog

### 5.0.0 (2026-11-13)

**Added**

- Metadata block with companion layer versions.
- Symbolic oversight layer (`src/symbolic/`, `src/dashboard/symbolic_visualizer.py`, `symbolic_policy.yaml`).
- Meta-cognitive modules (`src/monitoring/`, `src/reflection/`, `src/memory/`, `src/analysis/`).
- `zero_trust.enabled`, `privacy.enabled`, `training.enabled` config blocks.
- `ZERO_TRUST_TOKEN` / `RLHF_API_KEY` secrets section.
- `pyproject.toml` + `MANIFEST.sha256` + `scripts/` + `Dockerfile` + `.cursorrules` to file tree.
- Subpackage `__init__.py` files (`schemas/`, `core/`).
- Full `src/enhancements/__init__.py` re-export covering `RoutingStrategy`, `create_node_descriptor`, `create_workload_descriptor`, `Priority`, `BioMode`, `MaintenanceStatus`.
- `--no-enhancements`, `--region`, `--policy`, `--symbolic-policy`, `--num-workers`, `--dry-run` flags.
- `--mode=compare` in `argparse.choices`.
- §Metric Reference, §Security, §Docker Integration, §lean-ctx inspection.
- §Migration table (v4 → v5).
- §What's Not in Scope, §Changelog, §Reviewer Checklist.
- `verify.sh` 5-check contract reference.
- `test_symbolic_oversight.py` (19 tests) to §Testing.

**Changed**

- "12-layer" → **4-layer feedback** taxonomy with explicit module inventory.
- Python floor `3.9+` → **`3.10+`**.
- Install flow adds **`pip install -e .`** + **`pip check`** + `pip install --upgrade pip`.
- `quantum_integration/...` source path removed; `src/enhancements/` canonical.
- Symlink recommendation removed (Refactor Guide v0.2 forbids).
- `metrics/dag_carbon_ledger.py` → **`core/dag_carbon_ledger.py`**.
- `core/meta_cognition.py` → **`src/reflection/reflection_engine.py` + `long_context_reasoner.py`**.
- `bio_inspired.enabled` → **`evolutionary.enabled`**.
- `moe_expert.enabled` → **`moe.enabled`**.
- `objective_weights` labeled `[accuracy, energy, latency, carbon]`.
- `config/green_agent_config.yaml` → **`config/green_agent.yaml`**.
- `PYTHONPATH=$(pwd)` → **`$(pwd)/src`** (with `pip install -e .` as preferred).
- "15 new modules" enumerated explicitly.
- "83.3% savings" claim marked as needing methodology.

**Fixed**

- `--mode=compare` runtime error (missing from `choices`).
- Missing `__init__.py` for subpackages (silent `ImportError`).
- Missing `symbolic_policy.yaml` example.
- Missing `symbolic_violations.json` in expected output.
- Missing `ENHANCEMENTS_ENABLED` in §Config (was only in troubleshooting).

**Deprecated**

- `bio_inspired` / `moe_expert` config keys (aliases kept for one minor release).
- `metrics/dag_carbon_ledger.py` path (moved to `core/`).

### 5.0.0-rc1

- Initial v5.0 integration guide with advanced enhancements.

---

## ✅ Reviewer Checklist

- [ ] Python floor is **3.10+** in §Prerequisites.
- [ ] Install flow includes `pip install -e .` + `pip check`.
- [ ] No `quantum_integration/...` path recommendation remains.
- [ ] No symlink recommendation remains.
- [ ] `core/dag_carbon_ledger.py` (not `metrics/`).
- [ ] `src/reflection/reflection_engine.py` (not `core/meta_cognition.py`).
- [ ] `bio_inspired` → `evolutionary`; `moe_expert` → `moe`.
- [ ] `objective_weights` labeled.
- [ ] `zero_trust`, `privacy`, `training` blocks present.
- [ ] Symbolic layer present: `src/symbolic/`, `src/dashboard/symbolic_visualizer.py`, `symbolic_policy.yaml`, `symbolic_violations.json`.
- [ ] `src/monitoring/`, `src/reflection/`, `src/memory/`, `src/analysis/`, `src/policy/` in file tree.
- [ ] `schemas/__init__.py` + `core/__init__.py` in STEP 4.
- [ ] `src/enhancements/__init__.py` re-exports full v1.2 API.
- [ ] `config/green_agent.yaml` (not `_config`).
- [ ] `--mode=compare` in `argparse.choices`.
- [ ] `--no-enhancements`, `--region`, `--policy`, `--symbolic-policy`, `--num-workers`, `--dry-run` in argparse.
- [ ] `pyproject.toml`, `MANIFEST.sha256`, `scripts/`, `Dockerfile`, `.cursorrules` in file tree.
- [ ] `test_symbolic_oversight.py` (19 tests) in §Testing.
- [ ] `ZERO_TRUST_TOKEN` / `RLHF_API_KEY` documented.
- [ ] Migration table includes all v1.2 renames.
- [ ] `verify.sh` 5-check contract referenced.
- [ ] Companion docs cross-linked.
- [ ] No unsubstantiated benchmark claims.
- [ ] No secrets committed.

---

**Integration complete!** You now have Green Agent v5.0 with a **4-layer feedback architecture** (objective + subjective + symbolic + advanced enhancements), aligned to the v1.2.0 companion docs. 🚀🌱

**Last Updated**: 2026-11-13 · **Guide Version**: 5.0.0 · **Companion layer versions**: 1.2.0 (meta-cognitive, symbolic, enhancements) · **Breaking Changes**: none for v4 users (`--mode=legacy` preserved)
