# 🌱 Green Agent: A Green Distributed AI Runtime

**Energy-Aware • Carbon-Adaptive • Multi-Agent • Reinforcement-Learned**

A fully distributed, sustainability-first AI agent runtime that integrates energy-aware policy enforcement, hybrid reinforcement learning, distributed multi-agent coordination, real-time Pareto Frontier visualization, carbon-grid adaptive throttling, carbon-credit market simulation, negawatt-based sustainability ranking, and Kubernetes autoscaling.

---

## 🚀 Why This Project Exists

Most AI agent frameworks optimize for:
- **Accuracy**
- **Latency**
- **Throughput**

**This architecture optimizes for:**
- **Accuracy per Watt per Carbon Unit**

We treat energy as a first-class optimization objective, redefining what it means to build intelligent systems in the age of climate awareness.

---

## 🧠 Core Philosophy

We extend classical AI benchmarking into four key dimensions:

### 1️⃣ Sustainability Pareto Frontier
Visualizing the trade-off between **Energy (Joules)** and **Accuracy (%)** to identify optimal efficiency points.

### 2️⃣ Negawatt Reward System
Agents are rewarded for **energy saved**, not just accuracy achieved. This inverts traditional optimization objectives.

### 3️⃣ Carbon-Adaptive Intelligence
Agents throttle themselves when the power grid is "dirty," automatically reducing computational intensity during high-carbon periods.

### 4️⃣ Quantum-Inspired Energy Efficiency Metric
```
E_eff = Task Completion Ratio / Σ(Quantum-Inspired Energy Loops)
```
A novel metric that captures energy efficiency across complex, multi-stage agent tasks.

---

## 🏗 System Architecture

```
User
  │
  ▼
FastAPI (Dashboard API)
  │
  ▼
Ray Distributed Cluster (Kubernetes Autoscaled)
  │
  ├── Multi-Agent Workers
  ├── PPO Trainer
  ├── Q-table Store (Persistent)
  ├── Carbon Grid Forecaster
  ├── Pareto Analyzer
  └── Carbon Market Simulator
```

---

## 📂 Repository Structure

```
green_ai_cluster/
│
├── runtime/
│   ├── run_agent.py
│   ├── distributed_runtime.py
│   └── stress_test_harness.py
│
├── policy/
│   ├── policy_engine.py
│   ├── ppo_policy.py
│   └── q_table_store.py
│
├── rewards/
│   └── negawatt_reward.py
│
├── carbon/
│   ├── carbon_forecast.py
│   ├── carbon_throttler.py
│   └── carbon_credit_simulator.py
│
├── distillation/
│   └── knowledge_distiller.py
│
├── analytics/
│   └── pareto_analyzer.py
│
├── dashboard/
│   ├── api_server.py
│   └── plotly_dashboard.py
│
├── k8s/
│   └── ray-cluster.yaml
│
└── README.md
```

---

## 🔥 Features Breakdown

### 1️⃣ Ray-Based Distributed Multi-Agent Runtime
- Powered by **Ray**
- Actor-based multi-agent execution
- Autoscaling via Kubernetes
- Fault-tolerant distributed state
- Async task execution

### 2️⃣ PPO + Persistent Q-Table Hybrid Learning
- On-policy PPO fine-tuning
- Persistent Q-table for fast bootstrapping
- Episodic memory across executions
- Policy adaptation based on:
  - Energy usage
  - Accuracy
  - Carbon intensity

### 3️⃣ Negawatt Reward Module
Instead of traditional reward functions:
```
Reward = Accuracy
```

We use:
```
Reward = Accuracy + Energy_Saved_Bonus
```

Agents solving the same task with lower energy consumption get ranked higher on the **Green Leaderboard**.

### 4️⃣ Sustainability Pareto Visualization
Dashboard displays:
- **X-axis:** Energy (Joules)
- **Y-axis:** Accuracy (%)
- Pareto frontier analysis
- Dominated vs non-dominated agents

Built using:
- FastAPI
- Plotly

### 5️⃣ Carbon-Grid Adaptive Throttling
The system queries regional carbon intensity:
- If grid is "dirty" → activate **Eco Mode**
  - Reduce token output
  - Increase pruning
  - Switch to lightweight policies

### 6️⃣ Temporal Carbon Shifting
For non-urgent tasks:
- Suggest delay window
- Show estimated CO₂ savings
- "Delay for Green" option in dashboard

### 7️⃣ Carbon Credit Simulation
Each agent execution:
- Calculates carbon footprint
- Converts CO₂ to simulated carbon credits
- Tracks ESG-style sustainability index

### 8️⃣ Cross-Agent Knowledge Distillation
Agents periodically:
- Share compressed policy representations
- Align value functions
- Improve cluster-wide efficiency

This reduces:
- Redundant exploration
- Energy waste
- Training instability

### 9️⃣ Kubernetes Autoscaling
Ray cluster deployed with:
- Horizontal autoscaler
- Dynamic worker scaling
- Load-aware resource provisioning

---

## 📊 Metrics

| Metric | Description |
|--------|-------------|
| **Accuracy** | Task success ratio |
| **Energy (J)** | Total Joules consumed |
| **gCO₂** | Carbon footprint |
| **Accuracy-per-Watt** | Sustainability efficiency |
| **E_eff** | Quantum-inspired efficiency |
| **Sustainability Rank** | Composite score |

---

## 🧪 Stress Testing

The `stress_test_harness.py` simulates:
- Node crashes
- Energy spikes
- Carbon intensity shifts
- PPO instability
- Network delays

Automatic recovery includes:
- Actor restart
- State rehydration
- Q-table reload
- Policy fallback

---

## ⚙️ Installation & Quick Start

### Prerequisites
- Python 3.8+
- Kubernetes cluster
- Ray 2.0+

### Installation
```bash
pip install -r requirements.txt
```

### Start Ray Cluster (Kubernetes)
```bash
kubectl apply -f k8s/ray-cluster.yaml
```

### Run Distributed Runtime
```bash
python runtime/run_agent.py
```

### Launch Dashboard
```bash
uvicorn dashboard.api_server:app --reload
```

Access the dashboard at `http://localhost:8000`

---

## 🏆 Research Contributions

This framework contributes:
- **Sustainability-first agent benchmarking**
- **Accuracy-per-Watt optimization**
- **Carbon-reactive policy switching**
- **Distributed green multi-agent RL**
- **Quantum-inspired efficiency metrics**

---

## 🌍 Environmental Impact

By prioritizing energy efficiency and carbon awareness, Green Agent demonstrates that high-performance AI systems can be built with environmental responsibility at their core. Every joule saved is a step toward sustainable artificial intelligence.

---

## 📄 License

See [LICENSE](../../LICENSE) for details.

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📧 Contact

For questions or collaboration opportunities, please reach out via [your contact information].

---

**Built with 💚 for a sustainable AI future**

