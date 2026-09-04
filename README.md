
# 🌱 Green Agent with Advanced Sustainability Orchestration

Green Agent is an open‑source framework for sustainable, intelligent orchestration of computing resources, built on a high‑throughput generation engine for large language models. It extends the core engine with carbon‑aware scheduling, helium tracking, predictive analytics, and a suite of reinforcement learning techniques to minimize environmental impact while maintaining performance and security.

This repository includes an **Enhancements Folder** (`quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements`) that integrates the following advanced methods:

- **LIMIT Graph** – topology‑aware decision making using graph representations of nodes and workloads.
- **Multi‑Objective Decision Process (MODP)** – tunable trade‑offs between carbon, latency, cost, and energy.
- **Reinforcement Learning from Human Feedback (RLHF)** – incorporating human preferences into routing, authentication, and priority selection.
- **Multi‑Teacher On‑Policy Distillation** – distilling knowledge from multiple expert policies (rule‑based, historical ML, Q‑learning, RLHF) into a lightweight student model.
- **Bio‑inspired Optimisation** – evolutionary algorithms for exploration and policy blending.
- **Mixture‑of‑Experts (MoE)** – learnable gating networks that dynamically weight teacher contributions.

All enhancements are designed to work seamlessly with the underlying generation engine, providing carbon‑aware, security‑conscious, and adaptive orchestration.

---

## 📁 Repository Structure

```
quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/
├── src/
│   ├── enhancements/
│   │   ├── feedback_event.py          # Canonical event schema (v2.2)
│   │   ├── node_descriptor.py         # Adaptive node routing (distillation + MoE + RLHF)
│   │   ├── workload_descriptor.py     # Adaptive workload priority (distillation + MoE + RLHF)
│   │   ├── zero_trust_architecture.py # Zero Trust security with distillation and ledger
│   │   └── async_message_queue.py     # Cross‑module communication
│   └── integration/
│       └── free_apis.py               # Community data hub and free API manager
├── scripts/
│   └── community_data_collector.py    # Enhanced data collection script
└── README.md
```

---

## 🚀 Quick Start

### Installation

```bash
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent
pip install -r requirements.txt
```

### Running the Community Data Collector

```bash
# Legacy mode (no advanced enhancements)
python scripts/community_data_collector.py --contribute

# Enhanced mode (Zero Trust, distillation, graph integration)
python scripts/community_data_collector.py --contribute --enhanced
```

The enhanced mode activates the full pipeline:

1. Authenticate using Zero Trust.
2. Use `NodeDescriptor` to select the best region for carbon observation.
3. Use `WorkloadDescriptor` to decide whether to contribute and with what priority.
4. Record the action in the immutable ledger.

---

## 🧠 How Enhancements Integrate with the Core Engine

The table below shows how each enhancement maps to the core engine's components and the requested technologies.

| Enhancement Component       | Core Engine Integration Point | LIMIT Graph | MODP | RLHF | Distillation | Bio‑inspired | MoE |
|-----------------------------|-------------------------------|-------------|------|------|--------------|--------------|-----|
| `node_descriptor.py`        | Resource allocator / router    | ✔️          | ✔️   | ✔️   | ✔️           | ✔️           | ✔️  |
| `workload_descriptor.py`    | Job scheduler                 | ✔️          | ✔️   | ✔️   | ✔️           | ✔️           | ✔️  |
| `zero_trust_architecture.py`| Security layer                | ✔️          | ✔️   | ✔️   | ✔️           | ✔️           | ✔️  |
| `feedback_event.py`         | Logging / audit trail         | ✔️          | ✔️   | ✔️   | –            | –            | –   |
| `community_data_collector.py`| Data collection / sharing    | ✔️          | ✔️   | ✔️   | ✔️           | ✔️           | ✔️  |

### Detailed Correlations

- **LIMIT Graph**  
  - `node_descriptor.py` includes `graph_id`, `graph_embedding`, and `graph_metrics`. The graph’s `centrality` influences the node’s health score and, consequently, the routing state.  
  - `workload_descriptor.py` also accepts graph fields and uses them to adjust historical performance estimates.  
  - `zero_trust_architecture.py` can hold graph metadata and passes it to the carbon‑aware authenticator as `graph_health`.

- **MODP**  
  - In all decision‑making modules, objective weights (`carbon_weight`, `latency_weight`, `cost_weight`, `energy_weight`, `security_weight`, etc.) are configurable via `metadata` or `ZeroTrustConfig`.  
  - Rewards are calculated as a weighted sum of normalized objectives, making them truly multi‑objective.

- **RLHF**  
  - A dedicated `RLHFTeacher` (or `RLHFPriorityTeacher`, `RLHFAuthTeacher`) is added to the teacher ensemble.  
  - `human_feedback_score` is a field in state vectors; it biases the teacher’s predictions and contributes to the reward (typically 10% weight).

- **Multi‑Teacher On‑Policy Distillation**  
  - Each module maintains a `DistillationStudent` and a set of teachers (rule‑based, historical ML, Q‑learning, RLHF).  
  - The student is updated online using a combination of distillation loss (KL‑like) and policy gradient (REINFORCE).  
  - Historical ML teachers can be trained offline from logs.

- **Bio‑inspired Optimisation**  
  - `EvolutionaryOptimizer` classes implement a simple genetic algorithm.  
  - When enabled (`use_evolutionary = True`), the evolutionary policy is blended with the distillation output (e.g., 70% distillation, 30% evolutionary).  
  - The evolutionary optimizer’s fitness is updated with the same reward signal.

- **MoE Expert Gating**  
  - Instead of fixed confidence‑based teacher blending, a `MoEGatingNetwork` learns to weight each teacher based on the current state.  
  - The gating network is updated together with the student using the same reward signal.  
  - The number of experts equals the number of teachers (typically 4).

---

## 📚 Detailed Module Descriptions

### 1. `feedback_event.py`

Canonical event schema for all feedback collectors. Supports:

- Multi‑objective vectors.
- Sub‑models for RLHF (`RLHFInfo`) and LIMIT Graph (`GraphInfo`).
- Extended learning metrics (multi‑teacher loss).
- Flexible metadata and tags.
- Pydantic validation and JSON serialisation.
- Database‑friendly serialisation (`to_db_dict`, `from_db_dict`).

**Version:** 2.2

### 2. `node_descriptor.py`

Adaptive routing for compute nodes. Features:

- **Distillation agent** with **MoE gating** over four teachers:
  - Rule‑based (carbon/helium heuristics).
  - Historical ML (RandomForest trained on logs).
  - Stateful Q‑learning.
  - RLHF (human feedback).
- **Evolutionary optimizer** (bio‑inspired) can be blended with distillation.
- **LIMIT Graph fields** (`graph_id`, `graph_embedding`, `graph_metrics`) influence state.
- **MODP** via configurable reward weights (`carbon_weight`, `latency_weight`, `cost_weight`).
- **Asynchronous persistence** of interaction logs and Q‑weights.

**Example:**

```python
from enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

node = NodeDescriptor(
    id="node-001",
    type=NodeType.EDGE,
    region="us-east",
    region_carbon_intensity=0.42,
    energy_per_token=0.00005,
    use_evolutionary=True,               # Enable bio‑inspired optimisation
    evolutionary_population_size=20,
    human_feedback_score=0.6,            # RLHF input
    graph_id="graph-node-001",           # LIMIT Graph integration
    graph_metrics={"centrality": 0.75},
    metadata={
        "distillation_epsilon": 0.1,
        "gating_learning_rate": 0.005,   # MoE learning rate
        "rlhf_feedback_weight": 0.3,
        "carbon_weight": 0.5,            # MODP objective weights
        "latency_weight": 0.3,
        "cost_weight": 0.2,
    }
)

strategy = await node.select_routing_strategy()
```

### 3. `workload_descriptor.py`

Adaptive priority selection for workloads. Features:

- **Distillation agent** with **MoE gating** over four teachers.
- **Evolutionary optimizer** for priority exploration.
- **RLHF** integration via human feedback score.
- **MODP** with metadata‑tunable weights (`latency_weight`, `carbon_weight`, `energy_weight`).
- **LIMIT Graph integration** for topology awareness.

**Example:**

```python
from enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType

wl = WorkloadDescriptor(
    task_id="task-001",
    task_type=TaskType.INFERENCE,
    tokens=1024,
    latency_target=150.0,
    urgency=Urgency.HIGH,
    estimated_energy_joules=0.1,
    estimated_carbon_kg=0.0005,
    use_evolutionary=True,
    human_feedback_score=0.6,
    graph_id="graph-task-001",
    graph_metrics={"centrality": 0.7},
    metadata={
        "latency_weight": 0.5,
        "carbon_weight": 0.3,
        "energy_weight": 0.2,
        "gating_learning_rate": 0.005,
    }
)

priority = await wl.select_priority()
```

### 4. `zero_trust_architecture.py`

Zero Trust security v4.2.0. Features:

- **Adaptive authentication level** (light/standard/enhanced) selected via distillation.
- **MoE gating** over teachers including RLHF.
- **Evolutionary optimizer** for auth level selection.
- **Immutable ledger** for all security events.
- **Carbon & helium tracking** for sustainability‑aware security decisions.
- **Predictive analytics** using online SGDRegressor.
- **Integration** with `FeedbackEvent` and async message queue.

**Example:**

```python
from enhancements.zero_trust_architecture import ZeroTrustArchitecture

zta = ZeroTrustArchitecture()
context = await zta.authenticate_request(
    {"data_classification": "internal"},
    {"identity": "alice", "authentication_method": "token", "token": "..."}
)
```

### 5. `community_data_collector.py`

Enhanced script for community data sharing. Supports:

- Optional `--enhanced` flag to activate Zero Trust, distillation, and graph integration.
- Region selection based on learned routing strategy.
- Priority‑based contribution decisions.
- Falls back to legacy behaviour when advanced modules are not available.

---

## 🧪 Advanced Usage

### Training Historical Models

Historical ML teachers can be trained offline from interaction logs that contain state vectors and chosen actions.

```python
from enhancements.schemas.node_descriptor import train_historical_model
from pathlib import Path

train_historical_model(
    log_paths=[Path("./node_logs/*.csv")],
    model_path=Path("./routing_historical_model.pkl")
)
```

### Enabling MoE / RLHF / Evolutionary via Configuration

All modules accept parameters via their `metadata` dictionary or dedicated constructor arguments. For example, to enable evolutionary blending with a higher RLHF influence:

```python
metadata = {
    "use_evolutionary": True,
    "evolutionary_population_size": 30,
    "rlhf_feedback_weight": 0.5,
    "gating_learning_rate": 0.01,
}
```

### Setting LIMIT Graph Data

Graph data can be provided either at construction time or by directly assigning fields:

```python
node.graph_id = "graph-123"
node.graph_embedding = [0.2, 0.5, 0.1]
node.graph_metrics = {"centrality": 0.8, "connectivity": 0.9}
```

---

## 📖 Testing

Unit tests for the distillation components (teachers, student, replay buffer, gating, evolutionary) are included in each module. Run them with:

```bash
cd path/to/enhancements
python -m unittest discover -s schemas -p "*_test.py"
```

---

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.


## 📄 License

This project is licensed under the MIT License – see [LICENSE](LICENSE) for details.
