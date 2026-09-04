
# 🔗 Green Agent v5.0 Integration Guide

## 📋 Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Integration](#step-by-step-integration)
4. [File Placement](#file-placement)
5. [Configuration](#configuration)
6. [Testing Integration](#testing-integration)
7. [Troubleshooting](#troubleshooting)
8. [Migration from v4 to v5](#migration)
9. [Advanced Enhancements Integration](#advanced-enhancements-integration)   <!-- NEW -->

---

## 🎯 Overview

This guide walks you through integrating **15 new modules** with your existing Green_Agent repository to create the **complete 12-layer v5.0 system**.

Additionally, it now covers integration of the **advanced enhancement modules** from `src/enhancements/` that provide:

- **LIMIT Graph** – topology‑aware metrics (centrality, connectivity).
- **MODP (Multi‑Objective Decision Process)** – configurable objective weights.
- **RLHF** – human feedback integrated into decision‑making.
- **Multi‑Teacher On‑Policy Distillation** – lightweight student policy with MoE gating.
- **Bio‑inspired Optimisation** – evolutionary tuning of weights.
- **MoE expert gating** – dynamic blending of teacher outputs.
- **FlexGen** – optional high‑throughput LLM execution with adaptive precision.

These enhancements are optional and can be enabled or disabled via configuration.

### What You're Integrating:

**NEW Modules (from me):**
- Workload Interpreter
- Carbon-Aware Decision Core
- Synthetic Data Optimizer
- Benchmark Intelligence
- Task Carbon Profiler
- Multi-Objective Scheduler
- Adaptation Classifier
- Policy Engine (ML Governance)
- Carbon Ledger
- Ray Cluster Manager
- Forecasting Engine
- Eco-Mode Controller
- (+ 3 more supporting modules)

**ADVANCED ENHANCEMENT MODULES (from `src/enhancements/`):**  <!-- NEW -->
- Node Descriptor (`schemas/node_descriptor.py`)
- Workload Descriptor (`schemas/workload_descriptor.py`)
- Feedback Event (`schemas/feedback_event.py`)
- Zero Trust Architecture (`zero_trust_architecture.py`)
- Graph Registry (`core/graph_registry.py`)
- Causal Graph (`core/causal_graph.py`)
- Meta-Cognition Layer (`core/meta_cognition.py`)
- DAG Carbon Ledger (`metrics/dag_carbon_ledger.py`)
- FlexGen integration hooks (config and delegation policy)

**EXISTING Modules (in your repo):**
- Meta-Cognitive Layer
- Neuro-Symbolic Reasoner
- PPO + Q-Table Hybrid
- Carbon Forecast
- Temporal Shifter
- Pareto Analyzer
- Negawatt Reward
- Green Leaderboard
- Carbon Credit Simulator
- Quantum LIMIT-Graph

---

## 📦 Prerequisites

### 1. System Requirements
```bash
- Python 3.9+
- 16GB RAM (for Ray cluster)
- 20GB disk space
- Git
```

### 2. Install Dependencies
```bash
cd Green_Agent

# Install base requirements
pip install -r requirements.txt

# Install new dependencies
pip install ray[default]>=2.9.0
pip install prophet>=1.1.0
pip install sentence-transformers>=2.2.0
pip install fastapi>=0.109.0
pip install plotly>=5.18.0
```

### 3. Install Advanced Enhancements Dependencies (NEW)
```bash
pip install scikit-learn>=1.3.0 pandas>=2.0.0 pydantic>=2.0.0 pydantic-settings>=2.0.0
pip install cryptography>=41.0.0 pyjwt>=2.8.0 prometheus-client>=0.17.0
pip install tenacity>=8.2.0 aiofiles>=23.0.0 aiohttp>=3.8.0 networkx>=3.1 joblib>=1.3.0
```

### 4. Verify Existing Components
```bash
python -c "from rewards.negawatt_reward import NegawattReward; print('✅ Existing components OK')"
python -c "from core.meta_cognition import MetaCognitiveLayer; print('✅ Meta-cognition OK')"
```

---

## 🔧 Step-by-Step Integration

### **STEP 1: Create Directory Structure**

```bash
cd Green_Agent

mkdir -p src/{interpretation,decision,optimization,benchmarking,carbon,orchestration,ml_governance,governance,distributed,integration}
mkdir -p tests/{unit,integration,benchmarks}
mkdir -p docs examples
```

### **STEP 2: Copy New Modules**

```bash
DOWNLOAD_DIR="path/to/downloaded/modules"

cp $DOWNLOAD_DIR/workload_interpreter.py src/interpretation/
cp $DOWNLOAD_DIR/carbon_aware_decision_core.py src/decision/
cp $DOWNLOAD_DIR/synthetic_data_optimizer.py src/optimization/
cp $DOWNLOAD_DIR/benchmark_intelligence.py src/benchmarking/
cp $DOWNLOAD_DIR/adaptation_classifier.py src/ml_governance/
cp $DOWNLOAD_DIR/policy_engine.py src/ml_governance/
cp $DOWNLOAD_DIR/carbon_ledger.py src/governance/
cp $DOWNLOAD_DIR/task_carbon_profiler.py src/carbon/
cp $DOWNLOAD_DIR/forecasting_engine.py src/carbon/
cp $DOWNLOAD_DIR/eco_mode_controller.py src/carbon/
cp $DOWNLOAD_DIR/ray_cluster_manager.py src/distributed/
cp $DOWNLOAD_DIR/carbon_aware_scheduler.py src/distributed/
cp $DOWNLOAD_DIR/multi_objective_scheduler.py src/orchestration/
cp $DOWNLOAD_DIR/unified_orchestrator.py src/integration/
```

### **STEP 3: Copy Advanced Enhancements (NEW)**

If you have the enhanced modules in a separate location (e.g., from the `src/enhancements/` folder in the repository), copy them into your project:

```bash
# Assuming enhancements source is at /path/to/enhancements
cp -r /path/to/enhancements src/enhancements

# Or if they are already in your repo under quantum_integration/.../src/enhancements,
# you can symlink or copy:
cp -r quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements src/
```

### **STEP 4: Create `__init__.py` Files**

```bash
touch src/__init__.py src/interpretation/__init__.py src/decision/__init__.py
touch src/optimization/__init__.py src/benchmarking/__init__.py src/carbon/__init__.py
touch src/orchestration/__init__.py src/ml_governance/__init__.py src/governance/__init__.py
touch src/distributed/__init__.py src/integration/__init__.py
```

**Add exports to key `__init__.py` files:**

**`src/integration/__init__.py`:**
```python
from .unified_orchestrator import (
    UnifiedGreenAgent,
    create_unified_agent,
    UnifiedResult
)

__all__ = ['UnifiedGreenAgent', 'create_unified_agent', 'UnifiedResult']
```

**`src/enhancements/__init__.py` (NEW):**
```python
from .schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
from .schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
from .schemas.feedback_event import FeedbackEvent
from .zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
from .core.graph_registry import GraphRegistry, GraphType
from .core.causal_graph import CausalGraph
from .core.meta_cognition import MetaCognitionLayer
from .metrics.dag_carbon_ledger import DAGCarbonLedger

__all__ = [
    'NodeDescriptor', 'NodeType', 'CoolingType',
    'WorkloadDescriptor', 'TaskType', 'Urgency',
    'FeedbackEvent',
    'ZeroTrustArchitecture', 'ZeroTrustConfig',
    'GraphRegistry', 'GraphType', 'CausalGraph', 'MetaCognitionLayer',
    'DAGCarbonLedger'
]
```

### **STEP 5: Update `run_agent.py`**

**Option A: Replace completely**
```bash
cp run_agent.py run_agent_v4_backup.py
cp $DOWNLOAD_DIR/run_agent_v5.py run_agent.py
```

**Option B: Add unified mode to existing**
```python
# Add import for unified and enhancements
try:
    from src.integration.unified_orchestrator import create_unified_agent
    UNIFIED_AVAILABLE = True
except ImportError:
    UNIFIED_AVAILABLE = False

try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

async def run_unified():
    agent = await create_unified_agent()
    task = {...}
    result = await agent.execute(task)
    print(result)
    await agent.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['legacy', 'unified'], default='unified')
    parser.add_argument('--enhancements', action='store_true', help='Enable advanced enhancements')
    args = parser.parse_args()

    if args.mode == 'unified' and UNIFIED_AVAILABLE:
        import asyncio
        asyncio.run(run_unified())
    else:
        run()
```

### **STEP 6: Move Existing Components (Optional)**

```bash
mkdir -p src/meta_cognitive src/neuro_symbolic src/quantum src/existing_carbon
mv core/meta_cognition.py src/meta_cognitive/
# etc.
```

### **STEP 7: Configure System**

**Create `config/green_agent_config.yaml`:**
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

# Advanced enhancements section (NEW)
enhancements:
  enabled: true
  limit_graph:
    enabled: true
    graph_metrics:
      centrality: 0.7
      connectivity: 0.6
  modp:
    enabled: true
    objective_weights: [0.4, 0.3, 0.2, 0.1]
  rlhf:
    enabled: true
    human_feedback_score: 0.6
  distillation:
    enabled: true
    use_moe_gating: true
  bio_inspired:
    enabled: true
    use_evolutionary: true
  moe_expert:
    enabled: true
    n_experts: 4
  flexgen:
    enabled: false       # set true to use FlexGen
    model_name: "facebook/opt-6.7b"
    default_precision: "fp16"
    delegation_policy: "adaptive"
```

### **STEP 8: Update Requirements**

**Merge into `requirements.txt`:**
```txt
# Existing plus new:
# Advanced enhancements dependencies
scikit-learn>=1.3.0
pandas>=2.0.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
cryptography>=41.0.0
pyjwt>=2.8.0
prometheus-client>=0.17.0
tenacity>=8.2.0
aiofiles>=23.0.0
networkx>=3.1
joblib>=1.3.0
```

### **STEP 9: Initialize Ray Cluster**

```bash
ray start --head --dashboard-port=8265
ray status
```

---

## 📂 Final File Placement

After integration, your repository should look like:

```
Green_Agent/
├── src/
│   ├── interpretation/ ...
│   ├── decision/ ...
│   ├── optimization/ ...
│   ├── benchmarking/ ...
│   ├── carbon/ ...
│   ├── ml_governance/ ...
│   ├── governance/ ...
│   ├── distributed/ ...
│   ├── orchestration/ ...
│   ├── integration/ ...
│   └── enhancements/                    # NEW
│       ├── __init__.py
│       ├── schemas/
│       │   ├── node_descriptor.py
│       │   ├── workload_descriptor.py
│       │   └── feedback_event.py
│       ├── zero_trust_architecture.py
│       ├── async_message_queue.py
│       └── core/
│           ├── graph_registry.py
│           ├── causal_graph.py
│           └── meta_cognition.py
│       └── metrics/
│           └── dag_carbon_ledger.py
│
├── tests/
│   ├── unit/ ...
│   └── integration/ ...
├── examples/
├── docs/
├── config/
│   └── green_agent_config.yaml
├── quantum_integration/ ...
├── run_agent.py
├── requirements.txt
└── README.md
```

---

## 🧪 Testing Integration

### **1. Test Imports**

```bash
python -c "from src.integration.unified_orchestrator import create_unified_agent; print('✅ Unified orchestrator OK')"
python -c "from src.enhancements.schemas.node_descriptor import NodeDescriptor; print('✅ Enhancements OK')"
```

### **2. Run Unit Tests**

```bash
pytest tests/unit/ -v
```

### **3. Run Integration Tests**

```bash
pytest tests/integration/test_unified_system.py -v
```

### **4. Run Enhanced Module Tests (if present)**

```bash
pytest tests/unit/test_enhancements.py -v
```

### **5. Run Complete System**

```bash
# Legacy mode
python run_agent.py --mode=legacy

# Unified mode (12 layers)
python run_agent.py --mode=unified

# Unified with advanced enhancements
python run_agent.py --mode=unified --enhancements

# Comparison mode
python run_agent.py --mode=compare
```

### **6. Expected Output (with enhancements)**

```
╔═══════════════════════════════════════════════════════════════════╗
║         🌱 GREEN AGENT v5.0 - Sustainable AI Runtime 🌱          ║
╚═══════════════════════════════════════════════════════════════════╝

🔧 Initializing Unified Green Agent (12 layers)...
✅ Layer 0: Workload Interpreter initialized
✅ Layer 3: Decision Core initialized
...
✅ Advanced enhancements initialized (NodeDescriptor, WorkloadDescriptor, etc.)
🧠 Selected routing strategy: carbon_first
🧠 Selected priority: green
...
✅ EXECUTION COMPLETE
Carbon Saved: 0.2468 kgCO2e (83.3%)
```

---

## 🔧 Troubleshooting

### Issue 1: Import Errors

**Solution:** Add `src` to `PYTHONPATH`:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Issue 2: Ray Won't Start

**Solution:** Restart Ray cleanly.

### Issue 3: Enhancements not activating

**Solution:** Verify:
- `src/enhancements/` folder exists.
- Dependencies installed.
- `enhancements.enabled` is `true` in config or `ENHANCEMENTS_ENABLED=true` env var.
- The modules import successfully.

---

## 🔄 Migration from v4 to v5

### Backward Compatibility

Green Agent v5.0 is **100% backward compatible** with v4.x. Enhancements are optional and can be toggled without affecting existing functionality.

---

## 📊 Validation Checklist

After integration, verify:

- [ ] All 15 new modules copied to correct locations
- [ ] Advanced enhancements folder (`src/enhancements/`) present
- [ ] All `__init__.py` files created
- [ ] Dependencies installed (including enhancement libs)
- [ ] Ray cluster running
- [ ] Imports work (`python -c "from src.integration..."`)
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Legacy mode works
- [ ] Unified mode works
- [ ] Unified + enhancements works (if enabled)
- [ ] Carbon savings >70% in unified mode
- [ ] Benchmarks recorded correctly
- [ ] Dashboard accessible (if enabled)

---

## 📞 Next Steps

1. **Read Architecture Documentation**: `docs/ARCHITECTURE_V5.md`
2. **Try Examples**: `python examples/complete_workflow_demo.py`
3. **Run Benchmarks**: `python tests/benchmarks/run_benchmark.py`
4. **Configure for Production**: Edit `config/green_agent_config.yaml`
5. **Deploy on Kubernetes**: See `k8s/ray-cluster.yaml`
6. **Explore Advanced Enhancements**: See `src/enhancements/README.md`

---

## 💡 Tips

- Start with **legacy mode** to ensure existing components work.
- Test **unified mode** with small tasks first.
- Enable **enhancements** only after core functionality is verified.
- Monitor Ray dashboard at `http://localhost:8265`.
- Check logs in `/tmp/ray/session_latest/logs/`.
- Use `--num-workers=2` for testing on laptops.

---

**Integration complete! You now have Green Agent v5.0 with 12 layers plus advanced enhancements! 🚀🌱**
