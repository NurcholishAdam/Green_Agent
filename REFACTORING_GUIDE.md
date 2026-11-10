
# Repository Refactoring Guide

| Field | Value |
|---|---|
| **Version** | 0.2.0 |
| **Last Updated** | 2026-11-10 |
| **Status** | Proposed |
| **Branch** | `refactor/simplified-structure` |
| **Target Release** | v2.0.0 |
| **Deprecation Window** | One minor release (v2.x → v3.0) for old import paths |

---

## Table of Contents

1. [Overview](#overview)
2. [Goals & Non-Goals](#goals--non-goals)
3. [What Changed](#what-changed)
4. [What Is NOT Moved](#what-is-not-moved)
5. [File Preservation](#file-preservation)
6. [Migration Path](#migration-path)
7. [Import Updates](#import-updates)
8. [Backward-Compatibility Shim](#backward-compatibility-shim)
9. [Import Audit](#import-audit)
10. [Configuration Migration](#configuration-migration)
11. [Packaging (`pyproject.toml`)](#packaging-pyprojecttoml)
12. [Testing the New Structure](#testing-the-new-structure)
13. [CI/CD Updates](#cicd-updates)
14. [Compatibility Matrix](#compatibility-matrix)
15. [Rollback Plan](#rollback-plan)
16. [Documentation Updates Needed](#documentation-updates-needed)
17. [Reviewer Checklist](#reviewer-checklist)
18. [Benefits of New Structure](#benefits-of-new-structure)
19. [Compatibility Statement](#compatibility-statement)
20. [Changelog](#changelog)
21. [Questions?](#questions)

---

## Overview

This document explains the refactoring of the Green Agent repository to use a
simplified, flat structure while preserving **all** existing enhancement
modules — including the neuro-symbolic oversight layer, the lean-ctx tooling,
and the Docker build.

> **Read this first:** the refactor **does change import paths**. The old
> nested path (`quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/`)
> is **not** a valid Python package (hyphens and dots), so today the codebase
> relies on `sys.path` hacks or namespace-package luck. The refactor fixes that
> root cause. A [backward-compatibility shim](#backward-compatibility-shim) keeps
> old imports working for one minor release.

---

## Goals & Non-Goals

### Goals

1. Reduce directory depth from **6+ levels to 3 levels** (`src/<area>/<file>.py`).
2. Make every import path a **valid Python identifier** (no hyphens, no dots in package names).
3. Preserve **every** existing module — including the 70+ enhancement files,
   the symbolic reasoning layer, dashboards, and the Dockerfile.
4. Keep **old imports working** for one minor release via a shim layer.
5. Update the Dockerfile, CI, and docs so nothing silently breaks.

### Non-Goals

- ❌ Renaming public classes (`FlexGenController`, `ParetoOptimizer`, `LimitGraph` stay the same).
- ❌ Moving `Dockerfile`, `requirements.txt`, `.cursorrules`, `run_agent.py`, or policy files.
- ❌ Changing the **runtime** behavior of any module.
- ❌ Changing the config **schema** (env vars stay canonical; see [Configuration Migration](#configuration-migration)).
- ❌ Introducing a new namespace like `green_agent/` (future ADR if desired).

---

## What Changed

### Directory Structure

**Before** — nested, invalid as a Python package, 6+ levels deep:

```
Green_Agent/
└── quantum_integration/                     ← hyphen? no, but subdir is
    └── quantum-limit-graph-v2.4.0/          ← INVALID Python identifier
        └── limit-agentbench/                ← INVALID Python identifier
            └── src/
                ├── enhancements/   (70+ files, flat + a few subfolders)
                ├── symbolic/       (symbolic_reasoning_engine.py)
                ├── dashboard/      (symbolic_visualizer.py)
                ├── policy/         (policy_feedback.py)
                └── scripts/
```

**After** — flat, valid Python packages, 3 levels:

```
Green_Agent/
├── src/
│   ├── __init__.py
│   ├── core/                     # core agent logic
│   │   └── __init__.py
│   ├── enhancements/             # 70+ modules preserved (see §File Preservation)
│   │   ├── __init__.py           # re-exports public API + shim entrypoint
│   │   ├── base_classes.py
│   │   ├── adapters.py
│   │   ├── config.yaml
│   │   ├── flexgen_controller.py
│   │   ├── flexgen_policy.py
│   │   ├── flexgen_policy_selector.py
│   │   ├── pareto_optimizer.py
│   │   ├── pareto_router.py
│   │   ├── adaptive_cost_function.py
│   │   ├── limit_graph.py
│   │   ├── limit_graph_v2.py
│   │   ├── serendipity_trace.py
│   │   ├── reasoning_path.py
│   │   ├── constraint_engine.py
│   │   ├── rlhf.py
│   │   ├── feedback_collector.py
│   │   ├── preference_model.py
│   │   ├── multi_teacher_policy_distillation.py
│   │   ├── distillation_orchestrator.py
│   │   ├── sustainability_cost.py
│   │   ├── sustainability_signals_v10.py
│   │   ├── carbon_delay_scheduler.py
│   │   ├── carbon_credit_marketplace.py
│   │   ├── marginal_carbon_v10.py
│   │   ├── real_carbon_intensity_api_v10.py
│   │   ├── ai_data_center_loader.py
│   │   ├── green_datacenter_map.py
│   │   ├── green_datacenter_selector.py
│   │   ├── cloud_latency_estimator.py
│   │   ├── helium_api_collector.py
│   │   ├── helium_data_collector.py
│   │   ├── helium_elasticity_v10.py
│   │   ├── helium_forecaster_v9.py
│   │   ├── helium_scarcity_manager.py
│   │   ├── helium_circularity.py
│   │   ├── quantum_helium_optimizer_v10.py
│   │   ├── unified_helium_integration_v4.py
│   │   ├── chromatophore_compartments.py
│   │   ├── gpu_profiler.py
│   │   ├── metric_aggregator.py
│   │   ├── energy_profiler.py
│   │   ├── energy_scaler.py
│   │   ├── module_benchmark_v5.py
│   │   ├── control_system.py
│   │   ├── contextual_bandit.py
│   │   ├── policy_meta_cache.py
│   │   ├── node_registry.py
│   │   ├── mixed_precision.py
│   │   ├── gpu_acceleration.py
│   │   ├── thermal_optimizer.py
│   │   ├── tokenization_optimizer.py
│   │   ├── fallback_manager.py
│   │   ├── federated_learning.py
│   │   ├── zero_trust_architecture.py
│   │   ├── core/                 # LIMIT graph + causal graph + carbon ledger
│   │   │   ├── __init__.py
│   │   │   ├── graph_registry.py
│   │   │   ├── causal_graph.py
│   │   │   ├── dag_carbon_ledger.py
│   │   │   └── anomaly_detector.py
│   │   ├── schemas/              # typed descriptors
│   │   │   ├── __init__.py
│   │   │   ├── node_descriptor.py
│   │   │   ├── workload_descriptor.py
│   │   │   └── feedback_event.py
│   │   ├── MODP/
│   │   ├── bio_inspired/
│   │   └── moe_expert_system/
│   ├── symbolic/                 # neuro-symbolic layer
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py
│   ├── dashboard/
│   │   ├── __init__.py
│   │   └── symbolic_visualizer.py
│   ├── policy/
│   │   ├── __init__.py
│   │   └── policy_feedback.py
│   ├── utils/
│   │   └── __init__.py
│   └── quantum/                  # quantum integration
│       └── __init__.py
├── config/
│   ├── green_agent.yaml
│   ├── symbolic_policy.yaml
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
├── Dockerfile                    # stays at root (not moved)
├── requirements.txt              # stays at root (not moved)
├── pyproject.toml                # NEW — declares src layout
├── MANIFEST.sha256               # NEW — inventory for verify.sh
├── .cursorrules                  # stays at root (not moved)
├── run_agent.py                  # stays at root (not moved)
├── agentbeats.json               # stays at root (not moved)
├── docker_metrics_collector.py   # stays at root (not moved)
├── demo_symbolic_oversight.py    # stays at root (not moved)
├── test_symbolic_oversight.py    # stays at root (not moved)
├── example_config.json           # stays at root (not moved)
├── green_policy.yaml             # stays at root (not moved)
├── symbolic_policy.yaml          # stays at root (not moved)
├── NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md
└── SYMBOLIC_OVERSIGHT_README.md
```

### Key Improvements (measured, not hand-waved)

| Metric | Before | After |
|---|---|---|
| Max directory depth to a module | 6 | 3 |
| Python-invalid package names | 2 (`quantum-limit-graph-v2.4.0`, `limit-agentbench`) | 0 |
| `sys.path` hacks required | Yes (implicit) | No (`pyproject.toml`) |
| Files moved | — | N (see `git diff --stat main...HEAD`) |
| Public API classes renamed | — | 0 |
| Files deleted | — | 0 |

---

## What Is NOT Moved

These files **stay at the repository root** — do not move them in the refactor:

| File / dir | Reason |
|---|---|
| `Dockerfile` | CI builds from root context; `.dockerignore` assumed |
| `requirements.txt` | Standard location; pip resolution |
| `.cursorrules` | lean-ctx config reads from root |
| `run_agent.py` | Docker `CMD` invokes `python run_agent.py` |
| `agentbeats.json` | Docker `COPY` + runtime reference |
| `docker_metrics_collector.py` | Docker `COPY` |
| `example_config.json` | User-facing CLI arg default |
| `green_policy.yaml` | Docker commented `COPY`; root reference |
| `symbolic_policy.yaml` | Root reference; engine default lookup |
| `demo_symbolic_oversight.py` | README quick-start path |
| `test_symbolic_oversight.py` | README test path |
| `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` | README cross-ref |
| `SYMBOLIC_OVERSIGHT_README.md` | README cross-ref |
| `REPOSITORY_REFACTORING_GUIDE.md` | this file |

Moving any of these requires a **separate** PR that also updates the
Dockerfile, CI, and every doc that references them.

---

## File Preservation

All existing modules are preserved. The refactor uses **`git mv`** (not `mv`)
so `git log --follow` works on every moved file.

> **Verification:** `MANIFEST.sha256` (generated before the refactor) lists every
> original path + SHA-256. `scripts/verify.sh` checks every entry exists at its
> new path with an identical hash.

### Core Infrastructure
- ✅ `__init__.py`
- ✅ `base_classes.py`
- ✅ `adapters.py`
- ✅ `config.yaml`

### FlexGen Integration (3 files)
- ✅ `flexgen_controller.py`
- ✅ `flexgen_policy.py`
- ✅ `flexgen_policy_selector.py`

### MODP (Multi-Objective Decision-Making)
- ✅ `MODP/` folder
- ✅ `pareto_optimizer.py`
- ✅ `pareto_router.py`
- ✅ `adaptive_cost_function.py`

### LIMIT Graph (Enhanced)
- ✅ `limit_graph.py`
- ✅ `limit_graph_v2.py`
- ✅ `serendipity_trace.py`
- ✅ `reasoning_path.py`
- ✅ `constraint_engine.py`
- ✅ `core/graph_registry.py`
- ✅ `core/causal_graph.py`
- ✅ `core/dag_carbon_ledger.py`
- ✅ `core/anomaly_detector.py`

### Bio-Inspired Learning
- ✅ `bio_inspired/` folder
- ✅ `evolutionary_engine.py`
- ✅ `genetic_algorithm.py`
- ✅ `fitness_evaluator.py`

### Mixture of Experts (MoE)
- ✅ `moe_expert_system/` folder
- ✅ `green_agent_moe_system.py` ⭐
- ✅ `expert_router_harvester.py`
- ✅ `fft_moe_adapter.py`
- ✅ `context_encoder.py`

### RLHF (Reinforcement Learning from Human Feedback)
- ✅ `rlhf.py`
- ✅ `feedback_collector.py`
- ✅ `preference_model.py`

### Policy Distillation
- ✅ `multi_teacher_policy_distillation.py`
- ✅ `distillation_orchestrator.py`

### Sustainability & Carbon
- ✅ `sustainability_cost.py` ⭐
- ✅ `sustainability_signals_v10.py`
- ✅ `carbon_delay_scheduler.py`
- ✅ `carbon_credit_marketplace.py`
- ✅ `marginal_carbon_v10.py`
- ✅ `real_carbon_intensity_api_v10.py`

### Data Center Integration
- ✅ `ai_data_center_loader.py` ⭐
- ✅ `green_datacenter_map.py`
- ✅ `green_datacenter_selector.py`
- ✅ `cloud_latency_estimator.py`

### Helium Integration
- ✅ `helium_api_collector.py`
- ✅ `helium_data_collector.py`
- ✅ `helium_elasticity_v10.py`
- ✅ `helium_forecaster_v9.py`
- ✅ `helium_scarcity_manager.py`
- ✅ `helium_circularity.py`
- ✅ `quantum_helium_optimizer_v10.py`
- ✅ `unified_helium_integration_v4.py`
- ✅ `chromatophore_compartments.py` ⭐

### Monitoring & Profiling
- ✅ `gpu_profiler.py`
- ✅ `metric_aggregator.py`
- ✅ `energy_profiler.py`
- ✅ `energy_scaler.py`
- ✅ `module_benchmark_v5.py`

### Advanced Features
- ✅ `control_system.py`
- ✅ `contextual_bandit.py`
- ✅ `policy_meta_cache.py`
- ✅ `node_registry.py`
- ✅ `mixed_precision.py`
- ✅ `gpu_acceleration.py`
- ✅ `thermal_optimizer.py`
- ✅ `tokenization_optimizer.py`
- ✅ `fallback_manager.py`
- ✅ `federated_learning.py`
- ✅ And **40+ more files** — see `MANIFEST.sha256` for the authoritative list.

### Neuro-Symbolic Oversight (previously missing from this list)
- ✅ `symbolic/symbolic_reasoning_engine.py`
- ✅ `dashboard/symbolic_visualizer.py`
- ✅ `policy/policy_feedback.py`

### Schemas (previously missing)
- ✅ `schemas/node_descriptor.py`
- ✅ `schemas/workload_descriptor.py`
- ✅ `schemas/feedback_event.py`

### Security & Privacy (previously missing)
- ✅ `zero_trust_architecture.py`
- ✅ (privacy module if present under enhancements/)

### Root-level files preserved (previously missing)
- ✅ `Dockerfile`
- ✅ `.cursorrules`
- ✅ `run_agent.py`
- ✅ `agentbeats.json`
- ✅ `docker_metrics_collector.py`
- ✅ `demo_symbolic_oversight.py`
- ✅ `test_symbolic_oversight.py`
- ✅ `example_config.json`
- ✅ `green_policy.yaml`
- ✅ `symbolic_policy.yaml`
- ✅ `requirements.txt`

---

## Migration Path

### Option 1: Direct Refactoring (recommended)

```bash
# 1. Fetch the refactor branch
git fetch origin refactor/simplified-structure

# 2. Switch to it (this IS the new structure)
git checkout refactor/simplified-structure

# 3. Verify what actually changed against main
git diff --stat main...HEAD

# 4. Confirm no old paths remain outside the shim
scripts/verify.sh
```

> ❌ **Do not** run `git merge main` on the refactor branch — that merges
> *main into the refactor*, which is the opposite of what you want. Merge the
> refactor into `main` via PR instead.

### Option 2: Gradual Migration

Use this only if you need both layouts side-by-side during a transition.

```bash
# 1. Create new tree alongside old (do NOT delete old yet)
mkdir -p src/{core,enhancements,utils,quantum,symbolic,dashboard,policy}

# 2. git mv files (preserves history)
git mv quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/* src/enhancements/

# 3. Install the shim (see §Backward-Compatibility Shim)
# 4. Update imports via scripts/migrate.sh
# 5. Run smoke + unit + integration tests
# 6. Retire old tree when all imports are rewritten
```

> ⚠️ **Do not use symlinks** for migration — they break under Docker `COPY`
> and on Windows checkouts.

---

## Import Updates

### Package Name Normalization

The old path is **not a valid Python package** — hyphens and dots are illegal
in identifiers. The refactor's first job is to fix this.

| Old (invalid) | New (valid) |
|---|---|
| `quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/` | `src/enhancements/` |
| `quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/symbolic/` | `src/symbolic/` |
| `quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/dashboard/` | `src/dashboard/` |
| `quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/policy/` | `src/policy/` |

### Old Imports (broken by design)

```python
# ❌ This never worked as a real import — the path contains a hyphen
# from quantum_integration.quantum-limit-graph-v2.4.0.limit-agentbench.src.enhancements import (
#     FlexGenController,
#     ParetoOptimizer,
#     LimitGraph,
# )
```

### New Imports (valid, match the After tree exactly)

```python
# ✅ Public API — via the re-exporting __init__.py
from src.enhancements import (
    FlexGenController,
    ParetoOptimizer,
    LimitGraph,
)

# ✅ Or via concrete modules (recommended when you want IDE "jump to definition")
from src.enhancements.flexgen_controller import FlexGenController
from src.enhancements.pareto_optimizer import ParetoOptimizer
from src.enhancements.limit_graph import LimitGraph

# ✅ Enhancement sub-packages
from src.enhancements.core.graph_registry import GraphRegistry
from src.enhancements.core.causal_graph import CausalGraph
from src.enhancements.core.dag_carbon_ledger import DAGCarbonLedger
from src.enhancements.schemas.node_descriptor import NodeDescriptor
from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor
from src.enhancements.schemas.feedback_event import FeedbackEvent

# ✅ Neuro-symbolic layer
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer
from src.policy.policy_feedback import PolicyFeedback
```

> **Note:** the earlier draft of this guide used invented sub-package paths
> (`src.enhancements.flexgen`, `src.enhancements.optimization`,
> `src.enhancements.constraints`). Those do **not** exist — the modules live
> directly under `src/enhancements/`. The paths above are the canonical ones.

---

## Backward-Compatibility Shim

To honor the "old imports still work" promise **for one minor release**, ship
this shim at the *old* path. It forwards every symbol to the new module and
emits a `DeprecationWarning` on first import.

```python
# quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/__init__.py
#
# ⚠️ DEPRECATED — REMOVED IN v3.0
# This module exists only so that old import paths keep working.
# New code must import from `src.enhancements` directly.
import importlib
import sys
import warnings

_warned = False

def _shim():
    global _warned
    if not _warned:
        warnings.warn(
            "Importing from quantum_integration...enhancements is deprecated "
            "and will be removed in v3.0. Use `src.enhancements` instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        _warned = True

_new = importlib.import_module("src.enhancements")
_shim()

# Replace this module in sys.modules so subsequent attribute lookups go to _new.
sys.modules[__name__] = _new
```

**Repeat the same 10-line shim** for `symbolic/`, `dashboard/`, `policy/` under
their old paths. The whole shim directory is deleted in the PR that bumps the
version to `v3.0`.

**How to verify the shim works:**

```python
import warnings
warnings.simplefilter("always", DeprecationWarning)

# Old path — must import AND warn
from quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench.src.enhancements \
    import FlexGenController  # noqa: F401

# New path — no warning
from src.enhancements import FlexGenController as FC2  # noqa: F401

assert FlexGenController is FC2
```

---

## Import Audit

Before merging, count how many sites need rewriting:

```bash
# How many old import sites remain?
grep -rn 'from quantum_integration' --include='*.py' | wc -l

# Where exactly?
grep -rln 'from quantum_integration' --include='*.py'

# CI workflows still referencing old paths?
grep -rn 'quantum-limit-graph\|limit-agentbench' .github/workflows/ || true

# Dockerfile still using the old COPY path?
grep -n 'quantum-limit-graph\|limit-agentbench' Dockerfile || true

# Docs still pointing at old structure?
grep -rn 'quantum-limit-graph\|limit-agentbench' docs/ *.md || true
```

The refactor PR must land **with all four greps returning zero** (outside the
shim directory).

---

## Configuration Migration

### Canonical model: environment variables

The Dockerfile, `.cursorrules`, and this repo treat **env vars** as the
canonical activation surface. `config/*.yaml` **maps onto** those env vars; it
does not replace them.

| Concern | Canonical (env) | YAML loader reads |
|---|---|---|
| Master toggle | `ENHANCEMENTS_ENABLED=true` | `enhancements.enabled` |
| FlexGen | `FLEXGEN_ENABLED=true` | `enhancements.flexgen.enabled` |
| MODP | `MODP_ENABLED=true` | `enhancements.modp.enabled` |
| LIMIT Graph | `LIMIT_GRAPH_ENABLED=true` | `enhancements.limit_graph.enabled` |
| RLHF | `RLHF_ENABLED=true` | `enhancements.rlhf.enabled` |
| Distillation | `DISTILLATION_ENABLED=true` | `enhancements.distillation.enabled` |
| MoE | `MOE_ENABLED=true` (alias: `MOE_GATING_ENABLED`) | `enhancements.moe.enabled` |
| Evolutionary | `EVOLUTIONARY_ENABLED=true` | `enhancements.evolutionary.enabled` |
| Zero Trust | `ZERO_TRUST_ENABLED=true` | `enhancements.zero_trust.enabled` |
| Privacy | `DIFFERENTIAL_PRIVACY_ENABLED=true` | `enhancements.privacy.enabled` |
| Training | `TRAINING_ENABLED=true` | `enhancements.training.enabled` |

### Old config (nested YAML — invalid keys)

```yaml
# ❌ Not a real schema — illustrative of the old nested path
quantum_integration:
  quantum-limit-graph-v2.4.0:
    limit-agentbench:
      src:
        enhancements:
          config: path/to/config.yaml
```

### New config (`config/green_agent.yaml`)

```yaml
# ✅ Canonical — mirrors env vars 1:1
enhancements:
  enabled: true                    # ← ENHANCEMENTS_ENABLED
  config: config/green_agent.yaml

  flexgen:
    enabled: true                  # ← FLEXGEN_ENABLED
    model_name: facebook/opt-6.7b
    default_precision: fp16
    delegation_policy: adaptive

  modp:
    enabled: true                  # ← MODP_ENABLED
    weights: [0.4, 0.3, 0.2, 0.1]  # accuracy, energy, latency, carbon

  limit_graph:
    enabled: true                  # ← LIMIT_GRAPH_ENABLED
    centrality: 0.7
    connectivity: 0.6

  rlhf:
    enabled: true                  # ← RLHF_ENABLED
    human_feedback_score: 0.6

  distillation:
    enabled: true                  # ← DISTILLATION_ENABLED

  moe:
    enabled: true                  # ← MOE_ENABLED

  evolutionary:
    enabled: true                  # ← EVOLUTIONARY_ENABLED
    population_size: 20
    mutation_rate: 0.1

  zero_trust:
    enabled: false                 # ← ZERO_TRUST_ENABLED

  privacy:
    enabled: false                 # ← DIFFERENTIAL_PRIVACY_ENABLED

  training:
    enabled: false                 # ← TRAINING_ENABLED
```

> ⚠️ **Precedence rule:** if both env and YAML are set, **env wins**. This
> matches the Dockerfile behavior. Document this in `docs/ARCHITECTURE.md`.

---

## Packaging (`pyproject.toml`)

Without a package declaration, `from src.enhancements import …` will fail
unless `PYTHONPATH` includes the repo root. The refactor ships this file:

```toml
# pyproject.toml (NEW — required for the src layout to be importable)
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "green-agent"
version = "2.0.0"
requires-python = ">=3.10"
description = "Green Agent with advanced enhancements"
license = { text = "Apache-2.0" }

[tool.setuptools]
package-dir = { "" = "src" }

[tool.setuptools.packages.find]
where = ["src"]
include = ["core*", "enhancements*", "symbolic*", "dashboard*", "policy*", "utils*", "quantum*"]

[tool.setuptools.package-data]
enhancements = ["config.yaml"]
"enhancements.core" = ["*.json"]
```

### Install for development

```bash
# Editable install — makes `src.*` importable without PYTHONPATH
pip install -e .

# Or, if you cannot install:
export PYTHONPATH="$PWD/src:$PYTHONPATH"
```

> **Dockerfile impact:** the runtime stage already sets `PYTHONPATH=/app`, and
> the Docker `COPY` places `src/` at `/app/src`. Either the Dockerfile is
> updated to use the **flat** path (`COPY .../src ./src`), or `pyproject.toml`
> is added and `pip install -e .` is run in the image. **Pick one** and record
> it in `docs/ARCHITECTURE.md`.

---

## Testing the New Structure

### 1. Validate File Integrity

`scripts/verify.sh` performs five checks:

```bash
#!/usr/bin/env bash
# scripts/verify.sh
set -euo pipefail

echo "▶ [1/5] Checking MANIFEST.sha256 (every old path exists at new path)"
sha256sum --quiet -c MANIFEST.sha256

echo "▶ [2/5] Checking no old imports remain outside the shim"
if grep -rn 'from quantum_integration' --include='*.py' \
     --exclude-dir='quantum_integration' . ; then
  echo "✗ old imports still present"; exit 1
fi

echo "▶ [3/5] Checking CI references no old paths"
if grep -rn 'quantum-limit-graph\|limit-agentbench' .github/workflows/ ; then
  echo "✗ CI still references old paths"; exit 1
fi

echo "▶ [4/5] Import smoke test"
python -c "from src.enhancements import *; print('✓ imports OK')"

echo "▶ [5/5] pytest"
python -m pytest tests/unit tests/integration -q

echo "✅ all checks passed"
```

### 2. Run Unit Tests

```bash
python -m pytest tests/unit/ -v
```

### 3. Run Integration Tests

```bash
python -m pytest tests/integration/ -v
```

### 4. Test Imports (smoke)

```bash
python -c "from src.enhancements import *; print('✓ All imports successful')"
python -c "from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine; print('✓ symbolic OK')"
python -c "from src.enhancements.core.graph_registry import GraphRegistry; print('✓ graph OK')"
```

### 5. Test the Shim

```bash
python -W error::DeprecationWarning -c "
import warnings
warnings.simplefilter('error', DeprecationWarning)
try:
    from quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench.src.enhancements import FlexGenController
except DeprecationWarning:
    print('✓ shim warns as expected')
"
```

---

## CI/CD Updates

Every workflow under `.github/workflows/` that references the old path must be
updated **in the same PR** as the refactor.

### Audit

```bash
grep -rn 'quantum-limit-graph\|limit-agentbench' .github/workflows/ || echo "✓ CI clean"
```

### Common replacements

| Before | After |
|---|---|
| `pip install -e quantum_integration/.../src` | `pip install -e .` |
| `PYTHONPATH=quantum_integration/.../src pytest` | `pytest tests/` |
| `COPY quantum_integration/.../src ./src` (Dockerfile) | `COPY src ./src` |
| `cd quantum_integration/.../src && …` | `cd "$GITHUB_WORKSPACE" && …` |

### Suggested CI matrix

```yaml
# .github/workflows/ci.yml
strategy:
  matrix:
    python: ["3.10", "3.11", "3.12"]
    os: [ubuntu-latest, macos-latest]
steps:
  - uses: actions/checkout@v4
  - uses: actions/setup-python@v5
    with: { python-version: "${{ matrix.python }}" }
  - run: pip install -e . && pip check
  - run: scripts/verify.sh
```

---

## Compatibility Matrix

| Python | Linux | macOS | Windows | Docker |
|---|---|---|---|---|
| 3.10 | ✅ | ✅ | ⚠️ (no CI) | ✅ |
| 3.11 | ✅ | ✅ | ⚠️ (no CI) | ✅ |
| 3.12 | ⚠️ (tested locally) | ⚠️ | ⚠️ | ⚠️ |

Legend: ✅ tested in CI · ⚠️ best-effort · ❌ unsupported.

Windows is not in CI because `scripts/verify.sh` is bash-only. A PowerShell
equivalent is a follow-up.

---

## Rollback Plan

If the refactor breaks production:

```bash
# 1. Return to main — old structure is intact on main
git checkout main

# 2. Re-run the same smoke test the refactor ran
python -c "from src.enhancements import *"   # may fail on main — expected
scripts/verify.sh                            # may fail on main — expected

# 3. Deploy from the last known-good tag (not from the branch head)
git checkout v1.4.2
```

The old structure **remains on `main`** until the refactor PR is merged.

---

## Documentation Updates Needed

Every doc that references the old path must be updated in the refactor PR:

- [ ] `README.md` — new structure tree
- [ ] `docs/ARCHITECTURE.md` — new layout + config precedence rule
- [ ] `docs/MODULE_GUIDE.md` — updated import examples
- [ ] `docs/INTEGRATION_GUIDE.md` — updated paths
- [ ] `SYMBOLIC_OVERSIGHT_README.md` — file structure block
- [ ] `NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md` — import examples
- [ ] `.cursorrules` — enhanced read modes point at `src/enhancements/…`
- [ ] `Dockerfile` — `COPY` path from `src` (flat) or update to `pyproject.toml` + `pip install -e .`
- [ ] `CONTRIBUTING.md` — dev setup (`pip install -e .`)
- [ ] CI workflows — see [CI/CD Updates](#cicd-updates)
- [ ] Deployment scripts — `scripts/*.sh`, `k8s/*.yaml`
- [ ] `pyproject.toml` — new file (see [Packaging](#packaging-pyprojecttoml))
- [ ] `MANIFEST.sha256` — new file

---

## Reviewer Checklist

Reviewers should verify each of these before approving the refactor PR:

- [ ] **New imports in this guide match the After tree** — `git ls-files src/ | head -50` confirms.
- [ ] **Dockerfile `COPY` path updated** — `grep quantum-limit-graph Dockerfile` returns nothing.
- [ ] **CI workflow paths updated** — `grep -r quantum-limit-graph .github/` returns nothing.
- [ ] **Backward-compatibility shim is present and tested** — the shim import test in §Testing passes.
- [ ] **`git log --follow` shows history preserved for each moved file** — spot-check 3 random files.
- [ ] **`MANIFEST.sha256` generated from the pre-refactor tree** — timestamp is before the first `git mv`.
- [ ] **`scripts/verify.sh` passes** on a fresh checkout of the refactor branch.
- [ ] **`pyproject.toml` installs cleanly** — `pip install -e . && python -c "import src.enhancements"`.
- [ ] **Docs updated** — the checklist above is fully ticked.
- [ ] **No public class renamed** — `git diff main...HEAD -- '*.py' | grep '^[-+]class '` is empty.

---

## Benefits of New Structure

| # | Benefit | How it's measured |
|---|---|---|
| 1 | Shallower nesting | 6 → 3 levels (measurable) |
| 2 | Valid Python package names | 2 → 0 invalid identifiers |
| 3 | No `sys.path` hacks | `pyproject.toml` replaces implicit path manipulation |
| 4 | `git log --follow` works | Every file moved with `git mv` |
| 5 | Simpler Dockerfile | `COPY src ./src` instead of a 4-level path |
| 6 | Simpler Kubernetes config | `configMap` volume at one depth |
| 7 | Easier onboarding | `docs/ARCHITECTURE.md` reflects reality |

> Assertions like "faster navigation" or "easier maintenance" are **removed**
> from this list because they are not measurable. If you want to justify them,
> add a metric (e.g. "time-to-first-import in `demo_symbolic_oversight.py` is
> X ms before, Y ms after").

---

## Compatibility Statement

What is **true** after this refactor:

- ✅ **New import paths** (`src.enhancements.*`, `src.symbolic.*`, `src.dashboard.*`, `src.policy.*`) work.
- ✅ **Old import paths** work for **one minor release** via the shim in §Backward-Compatibility Shim.
- ✅ **Public classes are unchanged** — `FlexGenController`, `ParetoOptimizer`, `LimitGraph`, `SymbolicReasoningEngine`, `SymbolicVisualizer`, `PolicyFeedback`.
- ✅ **Runtime behavior is unchanged** — same modules, same code, different path.
- ✅ **Env-var activation is unchanged** — `ENHANCEMENTS_ENABLED`, `MODP_ENABLED`, etc.

What is **not** true (do not claim these):

- ❌ "All existing code continues to work" — old imports emit `DeprecationWarning` and will be removed in v3.0.
- ❌ "Dual support: can use either old or new structure" — the old tree is a **shim**, not a parallel layout.
- ❌ "Zero changes required" — imports must migrate before v3.0.

---

## Changelog

### 0.2.0 (2026-04-10)

**Added**
- Metadata block (version, status, branch, release).
- Table of Contents.
- Goals & Non-Goals section.
- What Is NOT Moved section.
- Backward-Compatibility Shim section (with working code).
- Import Audit section (with greps).
- Packaging (`pyproject.toml`) section.
- CI/CD Updates section (with audit + matrix).
- Compatibility Matrix.
- Reviewer Checklist.
- This Changelog.

**Fixed**
- Old import examples no longer use invalid Python identifiers without explanation.
- New import examples now match the After tree (removed invented `flexgen/`, `optimization/`, `constraints/` sub-packages).
- `git checkout refactor && git merge main` corrected — that sequence was backwards.
- Config schema reconciled with Dockerfile + `.cursorrules` (env vars canonical).
- Preservation checklist now includes symbolic, dashboard, policy, schemas, security, and root files.
- `FLexGen` typo fixed.
- "Backward compatible" claim now accurate (shim + deprecation window).

**Removed**
- Unsupported benefits ("faster navigation", "easier maintenance").
- Dangling `path/to/config.yaml` placeholder.

### 0.1.0 (initial draft)

- First pass: before/after tree, migration paths, import examples, testing checklist.

---

## Questions?

Refer to:

- `docs/ARCHITECTURE.md` — high-level design + config precedence rule.
- `docs/MODULE_GUIDE.md` — per-module reference.
- `docs/INTEGRATION_GUIDE.md` — end-to-end integration.
- `SYMBOLIC_OVERSIGHT_README.md` — neuro-symbolic layer (v1.2.0).
- `.cursorrules` — lean-ctx enhanced read modes for cheap state inspection.

Open an issue on the Green_Agent repository for anything not covered above.
