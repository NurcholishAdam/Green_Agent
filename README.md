# 🌱 Green Agent

**A sustainability-first, carbon-aware orchestration framework for AI workloads.**

Green Agent is an open-source agentic AI runtime that decides *when, where, and how* to execute AI computation — continuously optimizing for energy, carbon, latency, cost, and quality, then **proving** what it achieved. It sits between AI workloads and the infrastructure that runs them: observing live signals, consulting specialized expert engines, learning from verified operational history, and recording every decision on an auditable governance trail.

Traditional green-software tools measure emissions *after* the job. Green Agent acts *before and during* execution — and grows measurably better at being green with every run.

---

## ✨ Highlights

- 🌍 **Carbon-aware routing** — live grid-intensity forecasting, eco-mode control, temporal shifting to greener windows, and Pareto-optimal route selection across edge INT8, cloud low-carbon regions, and deferred execution.
- 🧠 **Learning decision core** — LIMIT Graph reasoning, Multi-Objective Decision Processes (MODP), Multi-Teacher On-Policy Distillation (MOPD), Mixture-of-Experts gating, RLHF, and bio-inspired optimization — fused into one control loop.
- 🗂️ **Governed memory layer** — a 13-module, Supermemory-backed memory & context layer: recall-before-route, structured decision records, truth levels, bounded recall, write governance, feedback-loop protection, and MCP-exposed tools.
- 🛡️ **Proof-of-Green governance** — event-sourced decision log, append-only carbon ledger, human-review approvals, policy versioning, enforcement, verification, and accountability subsystems — audit-ready by construction.
- 📊 **Measurement & transparency** — efficiency accounting, metric governance, reproducibility rules, sustainable benchmarking, and a green leaderboard.
- 🔌 **Open interfaces** — FastAPI server, Streamlit dashboards, telemetry loader, and Model Context Protocol (MCP) tools.

---

## 🆕 Recent upgrades

| When | What landed |
|---|---|
| **Nov 10, 2026** | Ops hardening: `QUICK_START.md`, containerized benchmarking (`Dockerfile.benchmark`), dependency automation (Renovate / Digestabot), GPG-signed commits across the tree |
| **Oct 28–30, 2026** | Governance & measurement sprint — `workload_interpreter`, `efficiency_calculator`, `metric_governance`, `reproducibility_rules`, `policy_decision`, `governance_event`, `human_review`, `green_leaderboard`, `helium_profile` |
| **Oct 5, 2026** | Runtime expanded to 30+ module domains — orchestration, policy, scoring, reporting, runtime, retrieval, distributed execution, monitoring |
| **Sep 19, 2026** | **Memory layer shipped** — 13 modules (~718 KB): Supermemory adapter v6.0.0, schemas, bounded recall, write governor, feedback-loop guard, memory tiers, policy memory, MCP tools, self-audit benchmark |
| **Sep 17, 2026** | Lightweight `sustainability` package (carbon forecast → intensity provider → eco-mode controller → Pareto frontier → temporal shifter) + symbolic reasoning + dashboard revamp |

---

## 🧭 How it works

Green Agent runs a continuous six-step loop on every routing event:

    1. OBSERVE   workload intent · device capacity · Arm Helium telemetry · grid carbon · latency targets
    2. RECALL    3–5 comparable historical runs retrieved as cited evidence (bounded top-k recall)
    3. PLAN      MoE expert panel + Pareto frontier compare routes: edge INT8 · cloud low-carbon · deferral · quantization
    4. ACT       execute the policy-compliant route; human approval gates costly / irreversible actions
    5. EVALUATE  predicted vs measured energy, CO2e, latency, cost, quality
    6. REMEMBER  validated results promoted to durable memory — the system learns what actually worked

Recalled evidence is *re-checked* against live telemetry before it can steer a decision — remembered recommendations are evidence, never ground truth.

---

## 🏗️ Architecture

    ┌──────────────────────────────────────────────────────────────────────┐
    │ INTERFACE      FastAPI server · Streamlit dashboards · MCP tools       │
    ├──────────────────────────────────────────────────────────────────────┤
    │ GOVERNANCE     governance/ — carbon ledger · approvals · enforcement · │
    │                verification · accountability · policy versions ·       │
    │                human review                                            │
    ├──────────────────────────────────────────────────────────────────────┤
    │ MEMORY         memory/ — supermemory adapter v6.0.0 · schemas ·        │
    │                bounded recall · write governor · feedback guard ·      │
    │                memory tiers · policy memory · benchmark (13 modules)   │
    ├──────────────────────────────────────────────────────────────────────┤
    │ INTELLIGENCE   enhancements/ — LIMIT Graph · MODP · MOPD              │
    │                distillation · MoE gating · RLHF · bio-inspired         │
    ├──────────────────────────────────────────────────────────────────────┤
    │ CARBON CORE    sustainability/ — carbon forecast · intensity         │
    │                provider · eco-mode controller · Pareto frontier ·      │
    │                temporal shifter        (+ legacy carbon/ module)         │
    ├──────────────────────────────────────────────────────────────────────┤
    │ DATA           telemetry · Arm Helium · grid carbon APIs ·              │
    │                cloud / instance signals · device & helium profiles     │
    └──────────────────────────────────────────────────────────────────────┘

### Repository layout

    quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/
    ├── enhancements/      # Learning core: LIMIT Graph, MODP, MOPD distillation, MoE, RLHF
    ├── sustainability/    # Lightweight carbon-aware stack (the primary routing API surface)
    ├── memory/            # 13-module governed memory layer (Supermemory-backed, offline fallback)
    ├── governance/        # Proof-of-Green: carbon ledger, approvals, enforcement,
    │                      # verification, accountability, contracts, benchmarks, policies
    ├── decision/          # carbon_aware_decision_core · decision_request · policy_decision
    ├── carbon/            # Legacy carbon module: telemetry, helium monitor, task profiling
    ├── dashboard/         # FastAPI server · green + Streamlit dashboards · telemetry loader
    ├── optimization/      # ML optimizers · synthetic-data optimizers
    ├── symbolic/          # Symbolic reasoning engine + visualizer
    ├── benchmarking/      # Sustainable benchmarking · efficiency accounting
    ├── leaderboard/       # Green leaderboard
    ├── ml_governance/     # Metric governance
    ├── core/ · r1/ · rlhf/ · cognition/ · rewards/ · agentbeats/ · dpq/ · scripts/ · ...
    └── ...                # 30+ module domains: orchestration, policy, scoring, reporting,
                           # runtime, retrieval, distributed, monitoring, metrics,
                           # instrumentation, integration, interpretation, visualization

---

## 🚀 Quick start

### Install

    git clone https://github.com/NurcholishAdam/Green_Agent.git
    cd Green_Agent
    pip install -r requirements.txt

The Supermemory SDK is **optional** — the memory layer automatically falls back to a local mirror when the SDK is absent. Point it at a local Supermemory instance (`:6767`, embedded graph engine, data stays on-device under `./.supermemory`) or a cloud endpoint via `supermemory_config`.

### Run a module smoke test

Every module ships an executable smoke test:

    cd quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src
    python -m memory.supermemory_adapter        # bridge + offline mirror
    python -m memory.memory_schemas             # record schema round-trips
    python -m governance.carbon_ledger          # append-only carbon ledger

### Launch the control plane & dashboards

    python src/dashboard/api_server.py             # FastAPI server
    streamlit run src/dashboard/streamlit_dashboard.py

### Containerized benchmarking

    docker build -f Dockerfile.benchmark -t green-agent-benchmark .

---

## ⚙️ Configuration

Key knobs live in `src/config.py`, `src/user_preferences.py`, and `src/memory/supermemory_config.py`:

    from memory.supermemory_config import SupermemoryConfig

    SupermemoryConfig(
        mode="local",                        # or "cloud"
        default_container_tag="org:green-agent",
        recall_top_k=5,                      # bounded recall per decision
        ttl_seconds={"grid_forecast": 3600}, # TTL per record kind; policies persist
        write_retries=3,
    )

Decision objective weights (MODP) are tunable per workload via metadata — `carbon_weight`, `latency_weight`, `cost_weight`, `energy_weight`, `security_weight` — and the MoE gating network learns which expert to trust under each state.

---

## 🧪 Testing

    cd quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench
    python -m unittest discover -s src -p "*_test.py"

Or run any module directly — each validates its own happy paths, error handling, and edge cases:

    python -m enhancements.schemas.node_descriptor

---

## 🗺️ Roadmap

- [ ] Orchestrator recall-before-route GA
- [ ] Public benchmark results (memory-on vs baseline)
- [ ] Enterprise audit pack (governance export, ESG reporting templates)
- [ ] ASEAN data-centre & telco pilots

---

## 🤝 Contributing

Contributions are welcome — from new domain engines to governance adapters and memory tooling. Please open an issue or pull request; commits should be GPG-signed.

## 📄 License

MIT — see [LICENSE](LICENSE).

## 🙏 Credits

**Green Agent** is founded and led by **Nurcholish Adam** — *AI Research Agent* team.
Co-created with the international open-source and research community.
