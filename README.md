# Green Agent — Pareto-Optimized Green Benchmarking

This repository implements **Green Agent**, a research-grade, green-first agent benchmarking system designed for **AgentBeats**. The architecture emphasizes **multi-objective evaluation** (accuracy, energy, carbon, latency, memory) using **Pareto optimization**, **budget-aware execution**, and **container-native measurement**.

> **Core idea**: *Do not collapse green metrics too early.* We preserve full multi-dimensional measurements and only aggregate via Pareto frontiers or optional scalar scores.

---

## 🧭 High-Level Architecture

```
Docker Container (single-shot)
│
├── run_agent.py               # Entry point (AgentBeats-compatible)
├── docker_metrics_collector.py
│
└── src/
    ├── analysis/              # Pareto + scoring logic
    ├── constraints/           # Energy / carbon budgets
    ├── feedback/              # Human-readable green feedback
    ├── reporting/             # AgentBeats artifacts (offline)
    ├── visualization/         # Leaderboard & Pareto plots (offline)
    └── rlhf/                   # Optional green-aware reward shaping
```

**Design principles**:

* Single container run = single benchmark datapoint
* All metrics measured *inside Docker*
* Pareto-first, scalar scores optional
* AgentBeats orchestrates queries, not the agent

---

## ⚙️ Runtime Flow (Single Query)

1. **AgentBeats launches container** with environment variables
2. `run_agent.py` executes exactly once
3. Agent inference runs under measurement
4. Metrics collected:

   * Accuracy
   * Latency + variance
   * CPU-based energy (Wh)
   * Carbon (kg CO₂)
   * Peak memory (MB)
5. Energy / carbon constraints applied
6. JSON emitted to STDOUT

---

## 📦 Key Modules (What Was Added / Extended)

### 1️⃣ `docker_metrics_collector.py`

Collects green metrics from inside Docker:

* cgroup v1/v2 memory
* process CPU time
* energy estimation via CPU TDP
* carbon via configurable intensity

This ensures **reproducible, container-native measurements**.

---

### 2️⃣ Constraints (`src/constraints/energy_budget.py`)

Applies **hard budgets**:

* `MAX_ENERGY_WH`
* `MAX_CARBON_KG`

If violated, the run is marked as rejected — no silent failures.

---

### 3️⃣ Analysis Layer

#### • Pareto Optimization (`src/analysis/pareto.py`)

* Multi-objective dominance checking
* Supports accuracy ↑, energy ↓, latency ↓, carbon ↓
* Used **offline** across AgentBeats outputs

#### • Optional Scalar Score (`src/analysis/green_score.py`)

* Weighted combination for convenience
* Never replaces Pareto frontiers

---

### 4️⃣ Feedback (`src/feedback/energy_feedback.py`)

Generates **human-readable explanations**:

* High energy usage
* Latency risks
* Memory pressure

Useful for audits, papers, and debugging.

---

### 5️⃣ RLHF Extension (`src/rlhf/green_reward.py`)

Optional module for **green-aware reward shaping**:

* Penalizes energy & carbon during training
* Not used during benchmarking

---

### 6️⃣ Visualization (`src/visualization/`)

Offline scripts for:

* Accuracy vs Energy (Pareto plot)
* Latency vs Energy
* Carbon vs Energy (pure green plot)

These are **not executed in CI** and are reviewer-friendly.

---

## 🚀 `run_agent.py` (Upgraded Entry Point)

The upgraded `run_agent.py`:

* Is **single-shot** (AgentBeats-safe)
* Reads configuration from environment variables
* Executes exactly one operating mode per container
* Emits schema-stable JSON

### Supported modes

* `low_energy`
* `balanced`
* `high_accuracy`

Selected via:

```bash
QUERY_MODE=balanced
```

---

## 📊 Multi-Query AgentBeats Submission

AgentBeats requires **queries to be an array**. Each query launches the same image with different budgets.

```json
{
  "image": "ghcr.io/nurcholishadam/green-agent:latest",
  "queries": [
    {
      "id": "low-energy",
      "command": ["python", "run_agent.py"],
      "environment": {
        "QUERY_MODE": "low_energy",
        "MAX_ENERGY_WH": "0.03"
      }
    },
    {
      "id": "balanced",
      "command": ["python", "run_agent.py"],
      "environment": {
        "QUERY_MODE": "balanced",
        "MAX_ENERGY_WH": "0.06"
      }
    },
    {
      "id": "high-accuracy",
      "command": ["python", "run_agent.py"],
      "environment": {
        "QUERY_MODE": "high_accuracy"
      }
    }
  ]
}
```

Each query → one datapoint → Pareto aggregation offline.

---

## 🐳 Docker Integration

* Python 3.11 slim base
* cgroup access enabled for metrics
* No background servers
* Deterministic, CI-safe execution

Docker image is published to **GHCR** and referenced by AgentBeats.

---

## 🧪 Offline Analysis Workflow

After AgentBeats runs:

1. Collect JSON outputs
2. Aggregate with `pareto_front()`
3. Rank with `leaderboard.py`
4. Visualize using `visualization/leaderboard_plots.py`

This separation keeps benchmarking **clean and auditable**.

---

## 🟢 Why This Architecture Is Correct

* ✅ Pareto-first (no metric hiding)
* ✅ Budget-aware
* ✅ Container-native metrics
* ✅ AgentBeats-compliant
* ✅ Extensible to quantum / RLHF settings

This design is suitable for **leaderboards, papers, and long-term green AI research**.

---

## 📌 Next Possible Extensions

* Cross-agent Pareto comparison
* Region-aware carbon intensity
* Memory-constrained queries
* CSV / Parquet leaderboard export

---

License

This project is licensed under the MIT License - see the LICENSE file for details.

👤 Author
Nurcholis Adam

- GitHub: @GreenAgent
- Email: nurcholisadam@gmail.com

🙏 Acknowledgments

- AgentBeats Team - Platform and A2A protocol
- THUDM - AgentBench framework
- Qiskit Team - Quantum computing toolkit
- RDI Foundation - Green agent template
- Quantum ML Community - QGNN research and implementations

**Green Agent** is not just a benchmark runner — it is a **green evaluation framework**.

🌱
