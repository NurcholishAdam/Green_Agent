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

---

## 🎯 Overview

This guide walks you through integrating **15 new modules** with your existing Green_Agent repository to create the **complete 12-layer v5.0 system**.

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
# Navigate to Green_Agent repository
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

### 3. Verify Existing Components
```bash
# Test existing components are accessible
python -c "from rewards.negawatt_reward import NegawattReward; print('✅ Existing components OK')"
python -c "from core.meta_cognition import MetaCognitiveLayer; print('✅ Meta-cognition OK')"
```

---

## 🔧 Step-by-Step Integration

### **STEP 1: Create Directory Structure**

```bash
cd Green_Agent

# Create new directories for organized code
mkdir -p src/{interpretation,decision,optimization,benchmarking,carbon,orchestration,ml_governance,governance,distributed,integration}

# Create test directories
mkdir -p tests/{unit,integration,benchmarks}

# Create docs directory
mkdir -p docs

# Create examples directory
mkdir -p examples
```

### **STEP 2: Copy New Modules**

```bash
# Assuming you have the modules in a download folder
DOWNLOAD_DIR="path/to/downloaded/modules"

# Copy Layer 0: Workload Interpreter
cp $DOWNLOAD_DIR/workload_interpreter.py src/interpretation/

# Copy Layer 3: Decision Core
cp $DOWNLOAD_DIR/carbon_aware_decision_core.py src/decision/

# Copy Layer 5: Data Optimization
cp $DOWNLOAD_DIR/synthetic_data_optimizer.py src/optimization/

# Copy Layer 9: Benchmarking
cp $DOWNLOAD_DIR/benchmark_intelligence.py src/benchmarking/

# Copy Layer 4: ML Governance
cp $DOWNLOAD_DIR/adaptation_classifier.py src/ml_governance/
cp $DOWNLOAD_DIR/policy_engine.py src/ml_governance/

# Copy Layer 6: Governance
cp $DOWNLOAD_DIR/carbon_ledger.py src/governance/

# Copy Layer 7: Carbon Monitoring
cp $DOWNLOAD_DIR/task_carbon_profiler.py src/carbon/
cp $DOWNLOAD_DIR/forecasting_engine.py src/carbon/
cp $DOWNLOAD_DIR/eco_mode_controller.py src/carbon/

# Copy Layer 8: Distributed
cp $DOWNLOAD_DIR/ray_cluster_manager.py src/distributed/
cp $DOWNLOAD_DIR/carbon_aware_scheduler.py src/distributed/

# Copy Orchestration
cp $DOWNLOAD_DIR/multi_objective_scheduler.py src/orchestration/

# Copy Integration Layer (CRITICAL)
cp $DOWNLOAD_DIR/unified_orchestrator.py src/integration/
```

### **STEP 3: Create `__init__.py` Files**

```bash
# Make directories Python packages
touch src/__init__.py
touch src/interpretation/__init__.py
touch src/decision/__init__.py
touch src/optimization/__init__.py
touch src/benchmarking/__init__.py
touch src/carbon/__init__.py
touch src/orchestration/__init__.py
touch src/ml_governance/__init__.py
touch src/governance/__init__.py
touch src/distributed/__init__.py
touch src/integration/__init__.py
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

### **STEP 4: Update `run_agent.py`**

**Option A: Replace completely**
```bash
# Backup original
cp run_agent.py run_agent_v4_backup.py

# Replace with new version
cp $DOWNLOAD_DIR/run_agent_v5.py run_agent.py
```

**Option B: Add unified mode to existing**
```python
# Edit run_agent.py and add at the top:
import sys
import argparse

# Add import for unified
try:
    from src.integration.unified_orchestrator import create_unified_agent
    UNIFIED_AVAILABLE = True
except ImportError:
    UNIFIED_AVAILABLE = False

# Add new function:
async def run_unified():
    """Run unified 12-layer mode"""
    agent = await create_unified_agent()
    task = {...}  # Your task
    result = await agent.execute(task)
    print(result)
    await agent.shutdown()

# Update main():
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['legacy', 'unified'], default='unified')
    args = parser.parse_args()
    
    if args.mode == 'unified' and UNIFIED_AVAILABLE:
        import asyncio
        asyncio.run(run_unified())
    else:
        run()  # Original function
```

### **STEP 5: Move Existing Components (Optional)**

Organize existing code into `src/` for consistency:

```bash
# Move existing components
mkdir -p src/meta_cognitive
mkdir -p src/neuro_symbolic
mkdir -p src/quantum
mkdir -p src/existing_carbon

# Move files (adjust paths as needed)
mv core/meta_cognition.py src/meta_cognitive/
mv rewards/ src/existing_rewards/
mv leaderboard/ src/existing_leaderboard/
mv carbon/ src/existing_carbon/
mv analysis/ src/existing_analysis/
mv policy/ src/existing_policy/
```

**Update imports in moved files** (use find-replace):
```
from rewards. → from src.existing_rewards.
from carbon. → from src.existing_carbon.
# etc.
```

### **STEP 6: Configure System**

**Create `config/green_agent_config.yaml`:**
```yaml
# Green Agent v5.0 Configuration

system:
  version: "5.0.0"
  mode: "unified"  # "legacy" or "unified"
  
ray:
  num_workers: 8
  dashboard_port: 8265
  object_store_memory: 10737418240  # 10GB
  
carbon:
  default_region: "US-CA"
  forecast_days: 30
  update_interval_hours: 1
  
policy:
  mode: "moderate"  # "soft", "moderate", "strict"
  enforce_lora_above_1b_params: true
  block_if_budget_exceeded: true
  
optimization:
  enable_data_compression: true
  target_compression_ratio: 0.3
  enable_synthetic_generation: true
  synthetic_ratio: 0.2
  
benchmarking:
  enable: true
  record_all_tasks: true
  generate_leaderboards: true
  
teams:
  - name: "nlp_research"
    carbon_budget_kgco2e: 20.0
  - name: "cv_team"
    carbon_budget_kgco2e: 30.0
```

### **STEP 7: Update Requirements**

**Merge into `requirements.txt`:**
```txt
# Existing requirements
# (keep all existing)

# NEW: Distributed Computing
ray[default]>=2.9.0
ray[serve]>=2.9.0

# NEW: Carbon Forecasting
prophet>=1.1.0
holidays>=0.35

# NEW: ML Frameworks
sentence-transformers>=2.2.0
torch>=2.0.0

# NEW: Dashboard
fastapi>=0.109.0
uvicorn[standard]>=0.27.0
plotly>=5.18.0
dash>=2.14.0

# NEW: Data Processing
numpy>=1.24.0
pandas>=2.0.0
scipy>=1.10.0

# NEW: Utilities
python-json-logger>=2.0.7
aiohttp>=3.8.0
tqdm>=4.65.0
```

### **STEP 8: Initialize Ray Cluster**

```bash
# Start Ray head node
ray start --head --dashboard-port=8265

# Verify Ray is running
ray status

# You should see:
# ======== Cluster Resources ========
# CPU: 8.00
# Memory: 16.00 GiB
```

---

## 📂 Final File Placement

After integration, your repository should look like:

```
Green_Agent/
├── .github/
│   └── workflows/
├── src/                                    # NEW: Organized code
│   ├── interpretation/
│   │   ├── __init__.py
│   │   └── workload_interpreter.py        # ✅ NEW
│   ├── decision/
│   │   ├── __init__.py
│   │   └── carbon_aware_decision_core.py  # ✅ NEW
│   ├── optimization/
│   │   ├── __init__.py
│   │   └── synthetic_data_optimizer.py    # ✅ NEW
│   ├── benchmarking/
│   │   ├── __init__.py
│   │   └── benchmark_intelligence.py      # ✅ NEW
│   ├── carbon/
│   │   ├── __init__.py
│   │   ├── task_carbon_profiler.py        # ✅ NEW
│   │   ├── forecasting_engine.py          # ✅ NEW
│   │   └── eco_mode_controller.py         # ✅ NEW
│   ├── ml_governance/
│   │   ├── __init__.py
│   │   ├── adaptation_classifier.py       # ✅ NEW
│   │   └── policy_engine.py               # ✅ NEW
│   ├── governance/
│   │   ├── __init__.py
│   │   └── carbon_ledger.py               # ✅ NEW
│   ├── distributed/
│   │   ├── __init__.py
│   │   ├── ray_cluster_manager.py         # ✅ NEW
│   │   └── carbon_aware_scheduler.py      # ✅ NEW
│   ├── orchestration/
│   │   ├── __init__.py
│   │   └── multi_objective_scheduler.py   # ✅ NEW
│   ├── integration/
│   │   ├── __init__.py
│   │   └── unified_orchestrator.py        # ✅ NEW (CRITICAL!)
│   ├── meta_cognitive/                     # EXISTING (moved)
│   ├── neuro_symbolic/                     # EXISTING (moved)
│   └── quantum/                            # EXISTING (moved)
│
├── tests/                                  # NEW: Comprehensive tests
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_workload_interpreter.py
│   │   ├── test_data_optimizer.py
│   │   └── ...
│   └── integration/
│       └── test_unified_system.py          # ✅ NEW
│
├── examples/                               # NEW: Usage examples
│   ├── basic_usage.py
│   ├── advanced_usage.py
│   └── complete_workflow_demo.py
│
├── docs/
│   ├── INTEGRATION_GUIDE.md               # ✅ THIS FILE
│   ├── ARCHITECTURE_V5.md                 # ✅ NEW
│   └── API_REFERENCE.md
│
├── config/
│   └── green_agent_config.yaml            # ✅ NEW
│
├── quantum_integration/                    # EXISTING
├── runtime/                                # EXISTING
├── rewards/                                # EXISTING (or moved to src/)
├── carbon/                                 # EXISTING (or moved to src/)
├── run_agent.py                            # UPDATED
├── requirements.txt                        # UPDATED
└── README.md                               # UPDATED
```

---

## 🧪 Testing Integration

### **1. Test Imports**

```bash
python -c "from src.integration.unified_orchestrator import create_unified_agent; print('✅ Unified orchestrator OK')"
python -c "from src.interpretation.workload_interpreter import WorkloadInterpreter; print('✅ Workload interpreter OK')"
```

### **2. Run Unit Tests**

```bash
pytest tests/unit/ -v
```

### **3. Run Integration Tests**

```bash
pytest tests/integration/test_unified_system.py -v
```

### **4. Run Complete System**

```bash
# Legacy mode (existing components only)
python run_agent.py --mode=legacy

# Unified mode (all 12 layers)
python run_agent.py --mode=unified

# Comparison mode (both)
python run_agent.py --mode=compare
```

### **5. Expected Output**

```
╔═══════════════════════════════════════════════════════════════════╗
║         🌱 GREEN AGENT v5.0 - Sustainable AI Runtime 🌱          ║
╚═══════════════════════════════════════════════════════════════════╝

🔧 Initializing Unified Green Agent (12 layers)...
✅ Layer 0: Workload Interpreter initialized
✅ Layer 3: Decision Core initialized
✅ Layer 5: Data Optimizer initialized
...
🚀 All async components initialized

📊 Creating task...
🚀 Executing complete 12-layer workflow...

✅ EXECUTION COMPLETE
================================================================================
Task ID: demo_bert_sentiment
Status: completed
Accuracy: 91.0%
Energy: 0.1234 kWh
Carbon: 0.0494 kgCO2e
Carbon Saved: 0.2468 kgCO2e (83.3%)
```

---

## 🔧 Troubleshooting

### Issue 1: Import Errors

**Error:** `ModuleNotFoundError: No module named 'src'`

**Solution:**
```bash
# Add src to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Or run with python -m
python -m src.integration.unified_orchestrator
```

### Issue 2: Ray Won't Start

**Error:** `ConnectionError: Ray has not been started`

**Solution:**
```bash
# Stop any existing Ray instances
ray stop

# Clean temporary files
rm -rf /tmp/ray

# Restart Ray
ray start --head --dashboard-port=8265

# Verify
ray status
```

### Issue 3: Prophet Installation Fails

**Error:** `ERROR: Failed building wheel for prophet`

**Solution:**
```bash
# Install system dependencies
# macOS:
brew install cmake

# Ubuntu/Debian:
sudo apt-get install build-essential cmake

# Then retry
pip install prophet
```

### Issue 4: Out of Memory

**Error:** `OutOfMemoryError in Ray`

**Solution:**
```bash
# Reduce Ray workers
python run_agent.py --mode=unified  # Uses 4 workers by default

# Or edit unified_orchestrator.py:
# num_ray_workers=2  # Reduce to 2

# Or increase object store memory
ray start --head --object-store-memory=5000000000  # 5GB
```

---

## 🔄 Migration from v4 to v5

### Backward Compatibility

Green Agent v5.0 is **100% backward compatible** with v4.x:

```python
# v4.x code still works
from rewards.negawatt_reward import NegawattReward
from carbon.carbon_forecast import CarbonForecast

negawatt = NegawattReward()
forecast = CarbonForecast()
# ... existing code unchanged
```

### Gradual Migration Path

**Phase 1: Install v5 alongside v4**
- Keep all v4 code
- Add new modules in `src/`
- Run in `legacy` mode

**Phase 2: Test unified mode**
- Run `python run_agent.py --mode=compare`
- Verify both modes work
- Compare results

**Phase 3: Migrate to unified**
- Switch default to `unified` mode
- Deprecate direct calls to old components
- Use `UnifiedGreenAgent` for all new code

**Phase 4: Clean up (optional)**
- Reorganize old code into `src/`
- Update all imports
- Remove redundant code

---

## 📊 Validation Checklist

After integration, verify:

- [ ] All 15 new modules copied to correct locations
- [ ] All `__init__.py` files created
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Ray cluster running (`ray status` shows healthy)
- [ ] Imports work (`python -c "from src.integration..."`)
- [ ] Unit tests pass (`pytest tests/unit/`)
- [ ] Integration tests pass (`pytest tests/integration/`)
- [ ] Legacy mode works (`python run_agent.py --mode=legacy`)
- [ ] Unified mode works (`python run_agent.py --mode=unified`)
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

---

## 💡 Tips

- Start with **legacy mode** to ensure existing components work
- Test **unified mode** with small tasks first
- Monitor Ray dashboard at `http://localhost:8265`
- Check logs in `/tmp/ray/session_latest/logs/`
- Use `--num-workers=2` for testing on laptops

---

**Integration complete! You now have Green Agent v5.0 with 12 layers! 🚀🌱**
