# Green Agent

**A sustainability-first, carbon-aware orchestration runtime for AI workloads.**

![Status: alpha](https://img.shields.io/badge/status-alpha-orange)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

Green Agent decides *when, where and how* AI computation should run. It balances energy, carbon, helium dependency, latency, cost and quality, then records what it did so the decision can be audited afterwards.

Most green-software tools report emissions after a job has finished. Green Agent acts before and during execution: it reads live signals, consults specialist experts, picks a policy-compliant route, and learns from the measured outcome.

> **Status:** alpha pre-release (`v0.1.0-alpha.1`). The platform is a working system and an experimental testbed. Public benchmark results are on the [roadmap](#roadmap) and are not yet published.

---

## Contents

- [Key capabilities](#key-capabilities)
- [How it works](#how-it-works)
- [Architecture](#architecture)
- [Sustainability signals](#sustainability-signals)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Testing](#testing)
- [Deployment and observability](#deployment-and-observability)
- [Repository layout](#repository-layout)
- [Documentation](#documentation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Key capabilities

| Area | What it provides |
|---|---|
| **Carbon-aware routing** | Grid-intensity forecasting, an eco-mode controller, temporal shifting to greener time windows, and Pareto-optimal route selection |
| **Learning decision core** | LIMIT Graph reasoning, Multi-Objective Decision Processes (MODP), Multi-Teacher On-Policy Distillation (MOPD), Mixture-of-Experts (MoE) gating, RLHF and bio-inspired optimization in one control loop |
| **Efficient execution** | Adaptive numeric precision and optional FlexGen offloaded inference, chosen by the decision core |
| **Governed memory** | A Supermemory-backed memory layer with bounded recall, write governance, truth levels and feedback-loop protection. It falls back to a local mirror when the SDK is absent |
| **Proof-of-Green governance** | Event-sourced decision log, append-only carbon ledger, human-review approvals, policy versioning, enforcement and verification |
| **Measurement** | Efficiency accounting, metric governance, reproducibility rules, sustainable benchmarking and a green leaderboard |
| **Open interfaces** | FastAPI server, Streamlit dashboards, Prometheus metrics and Model Context Protocol (MCP) tools |

---

## How it works

Green Agent runs a six-step loop on every routing event:

1. **Observe** the workload intent, device capacity, helium dependency, grid carbon and latency targets.
2. **Recall** three to five comparable historical runs as cited evidence (bounded top-k recall).
3. **Plan** by comparing routes with an MoE expert panel and a Pareto frontier: edge INT8, low-carbon cloud, deferral or quantization.
4. **Act** by executing the policy-compliant route. Human approval gates costly or irreversible actions.
5. **Evaluate** predicted against measured energy, CO2e, latency, cost and quality.
6. **Remember** by promoting validated results to durable memory.

Recalled evidence is re-checked against live telemetry before it can influence a decision. A remembered recommendation is evidence, never ground truth.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ INTERFACE     FastAPI server · Streamlit dashboards · MCP tools      │
├──────────────────────────────────────────────────────────────────────┤
│ GOVERNANCE    carbon ledger · approvals · enforcement · verification │
│               accountability · policy versions · human review        │
├──────────────────────────────────────────────────────────────────────┤
│ MEMORY        Supermemory adapter · schemas · bounded recall ·       │
│               write governor · feedback guard · tiers · policy memory │
├──────────────────────────────────────────────────────────────────────┤
│ INTELLIGENCE  LIMIT Graph · MODP · MOPD · MoE gating · FlexGen ·     │
│               RLHF · bio-inspired optimization                       │
├──────────────────────────────────────────────────────────────────────┤
│ CARBON CORE   carbon forecast · intensity provider · eco-mode ·      │
│               Pareto frontier · temporal shifter                     │
├──────────────────────────────────────────────────────────────────────┤
│ DATA          telemetry · grid carbon APIs · cloud and instance      │
│               signals · device and helium profiles                   │
└──────────────────────────────────────────────────────────────────────┘
```

The intelligence layer is partitioned by when each decision is made:

| Component | Decision time | Role |
|---|---|---|
| **MODP** | Run time | Weighted multi-objective scoring with per-workload `carbon_weight`, `latency_weight`, `cost_weight`, `energy_weight` and `security_weight` |
| **MoE gating** | Run time | A gating network routes each workload to specialist experts (energy, data, IoT, federated-learning energy, helium-IoT, sustainability) |
| **FlexGen** | Execution time | Optional high-throughput offloaded inference. Disabled by default |
| **MOPD** | Training time | Multi-teacher on-policy distillation orchestrator |
| **Memory** | Across all stages | Recall before routing, remember after evaluation |

---

## Sustainability signals

- **Carbon:** live grid intensity and forecasts drive scheduling, deferral and region choice.
- **Energy:** adaptive precision, sparse expert activation and energy-budget enforcement reduce consumption per task.
- **Helium:** the repository models helium as a constrained physical resource. It classifies hardware by helium dependency, monitors supply conditions, and maps scarcity levels (normal, caution, critical, severe) to actions such as throttling, deferral, redirecting to CPU or blocking a job.

---

## Quick start

### Install

```bash
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent
pip install -r requirements.txt     # or: pip install -e .   (Python 3.10+)
```

The Supermemory SDK is optional. Without it, the memory layer uses a local mirror.

### Run

```bash
python demo_meta_cognitive.py       # guided demo of the meta-cognitive architecture
python run_agent.py                 # main entry point: runs one task and exposes metrics
```

See [`QUICK_START.md`](QUICK_START.md) for output interpretation, advanced usage and troubleshooting.

### Run with Docker

```bash
docker build -t green-agent:full .                                  # all enhancements
docker build --build-arg ENHANCEMENTS_ENABLED=false -t green-agent:lean .
docker build -f Dockerfile.benchmark -t green-agent-benchmark .     # containerized benchmarking
```

The image runs as a non-root user and listens on port 8000 (`AGENT_PORT`).

### Dashboards and module smoke tests

```bash
cd quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src

python dashboard/api_server.py                      # FastAPI server (GREEN_AGENT_DASHBOARD_PORT, default 8000)
streamlit run dashboard/streamlit_dashboard.py      # Streamlit dashboard

python -m memory.supermemory_adapter                # memory bridge and offline mirror
python -m memory.memory_schemas                     # record schema round-trips
python -m governance.carbon_ledger                  # append-only carbon ledger
```

---

## Configuration

| File | Purpose |
|---|---|
| `green_policy.yaml` | Sustainability policy |
| `symbolic_policy.yaml` | Symbolic and neuro-symbolic oversight rules |
| `moe_config.json` | MoE gating and expert settings |
| `example_config.json` | Example runtime configuration |
| `config/base`, `config/overlays/*` | Kubernetes Kustomize base and development, staging and production overlays |

Feature flags are build arguments and environment variables in the `Dockerfile`: `ENHANCEMENTS_ENABLED`, `LIMIT_GRAPH_ENABLED`, `MODP_ENABLED`, `RLHF_ENABLED`, `DISTILLATION_ENABLED`, `MOE_ENABLED`, `EVOLUTIONARY_ENABLED`, `FLEXGEN_ENABLED` (default off), `ZERO_TRUST_ENABLED` (default off), `DIFFERENTIAL_PRIVACY_ENABLED` (default off) and `TRAINING_ENABLED` (default off).

Memory settings live in `src/memory/supermemory_config.py`:

```python
from memory.supermemory_config import SupermemoryConfig

SupermemoryConfig(
    mode="local",                         # or "cloud"
    default_container_tag="org:green-agent",
    recall_top_k=5,                       # bounded recall per decision
    ttl_seconds={"grid_forecast": 3600},  # TTL per record kind; policies persist
    write_retries=3,
)
```

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest tests/unit
```

Most modules also include an executable smoke test, for example `python -m enhancements.schemas.node_descriptor`.

---

## Deployment and observability

- **Containers:** multi-stage `Dockerfile` with a pinned base image, a non-root runtime and build-time feature flags.
- **Kubernetes:** Kustomize base and overlays, with an autoscaler, network policy and service account. CI runs the Kubernetes tests (`.github/workflows/k8s-tests.yml`).
- **Metrics and alerts:** `prometheus.yml`, `green_agent_alerts.yml` and `alertmanager_templates/`.
- **Supply chain:** Renovate and Digestabot for dependency updates, and GPG-signed commits.

---

## Repository layout

The repository has two source trees. `src/` holds the core runtime used by `run_agent.py`. The nested `limit-agentbench/src/` holds the full set of module domains.

```
Green_Agent/
├── run_agent.py · run_enhanced_agent.py · run_enhanced_agent_with_moe.py
├── app.py                     # status service: /healthz, /readyz, /metrics, /version
├── src/                       # core runtime: carbon, core, enhancements, integration, monitoring
├── config/                    # Kubernetes base and overlays
├── docs/                      # ARCHITECTURE.md, MODULE_GUIDE.md
├── tests/                     # unit, e2e, k8s
└── quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/
    ├── enhancements/          # LIMIT Graph, MODP, MOPD, MoE, FlexGen, RLHF, bio-inspired
    ├── sustainability/        # carbon forecast, eco-mode, Pareto frontier, temporal shifter
    ├── memory/                # governed Supermemory layer
    ├── governance/            # carbon ledger, approvals, enforcement, verification
    ├── decision/              # carbon-aware decision core, policy decision
    ├── carbon/                # telemetry, helium monitor, task profiling
    ├── dashboard/             # FastAPI server, Streamlit dashboards, telemetry loader
    ├── benchmarking/ · leaderboard/ · ml_governance/ · rewards/ · symbolic/
    └── ...                    # 30+ module domains in total
```

---

## Documentation

| Document | Contents |
|---|---|
| [`QUICK_START.md`](QUICK_START.md) | Installation, basic usage, metrics reference, troubleshooting |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | System architecture |
| [`docs/MODULE_GUIDE.md`](docs/MODULE_GUIDE.md) | Module reference |
| [`INTEGRATION_GUIDE.md`](INTEGRATION_GUIDE.md) | Integrating the v5.0 modules |
| [`META_COGNITIVE_ARCHITECTURE.md`](META_COGNITIVE_ARCHITECTURE.md) | Meta-cognitive layer |
| [`SYMBOLIC_OVERSIGHT_README.md`](SYMBOLIC_OVERSIGHT_README.md), [`NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md`](NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md) | Symbolic and neuro-symbolic oversight |

---

## Roadmap

- [ ] Public, reproducible benchmark results (energy and accuracy against a baseline)
- [ ] Orchestrator recall-before-route general availability
- [ ] Helium-aware cost in FlexGen policy selection and in the MOPD objective
- [ ] Consolidate duplicated enhancement code into one shared package
- [ ] Enterprise audit pack (governance export and ESG reporting templates)

---

## Contributing

Contributions are welcome, from new domain engines to governance adapters and memory tooling. Please open an issue or pull request. Commits should be GPG-signed.

## License

MIT. See [LICENSE](LICENSE).

## Credits

Green Agent is founded and led by **Nurcholish Adam**.
