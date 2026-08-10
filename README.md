# Green Agent: Sustainable AI Orchestration Platform with Carbon & Helium-Aware Resource Management

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000)](https://github.com/psf/black)
[![DOI](https://img.shields.io/badge/DOI-10.xxxx/xxxxx-blue)](https://doi.org/...)

---

## Table of Contents

- [Overview](#overview)
- [Core Philosophy](#core-philosophy)
- [Architecture](#architecture)
  - [Orchestration Gateway](#orchestration-gateway)
  - [Domain Engines](#domain-engines)
  - [Decision-Making Core](#decision-making-core)
- [Key Features](#key-features)
  - [Bio-Inspired Optimisation](#bio-inspired-optimisation)
  - [Mixture-of-Experts (MoE) System](#mixture-of-experts-moe-system)
  - [Multi-Teacher On-Policy Distillation (MOPD)](#multi-teacher-on-policy-distillation-mopd)
  - [Carbon & Helium Awareness](#carbon--helium-awareness)
  - [Quantum-Resilient Security](#quantum-resilient-security)
  - [Blockchain Verification](#blockchain-verification)
  - [Multi-Cloud Distribution & Resiliency](#multi-cloud-distribution--resiliency)
  - [Observability & Telemetry](#observability--telemetry)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Quick Example](#quick-example)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

**Green Agent** is a next‑generation autonomous orchestration platform designed to minimise the environmental footprint of AI workloads while maximising performance, cost‑efficiency, and operational resilience. It integrates **bio‑inspired optimisation**, **mixture‑of‑experts (MoE) decision fusion**, and **multi‑teacher on‑policy distillation (MOPD)** to dynamically adapt to changing grid carbon intensity, helium (green hydrogen) pricing, cloud spot prices, and workload characteristics.

At its heart, the Green Agent orchestrates a suite of **scientific domain engines** – thermal optimisers, phase‑aware energy models, carbon‑aware NAS, marginal carbon forecasters, helium price elasticity models, and federated learning aggregators – through a centralised, async‑aware **enhancements gateway**. This gateway provides persistent storage, quantum‑resilient security, blockchain audit trails, multi‑cloud distribution, and real‑time observability.

The platform is **carbon‑aware** and **helium‑aware**, meaning it actively tracks and optimises not only direct energy consumption but also the embodied carbon and the cost/availability of green hydrogen fuel cells used in data centre backup power. This dual focus places Green Agent at the forefront of **sustainable AI** and **circular economy** practices.

---

## Core Philosophy

- **Autonomous Adaptation**: Continuously learn and improve from live operation without human intervention.
- **Multi‑Objective Optimisation**: Balance carbon emissions, latency, cost, and quality.
- **Resilience by Design**: Circuit breakers, retries, and fallback chains ensure high availability.
- **Security First**: Post‑quantum cryptography and hardware‑grade key management.
- **Observable by Default**: Structured logging and Prometheus metrics for full transparency.

---

## Architecture

Green Agent is built as a layered, modular system. The core components reside in the `enhancements` module (located at `quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/`), which acts as the **orchestration gateway** for all domain engines.

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Green Agent Orchestration Gateway               │
├─────────────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────────┐ │
│  │   Config  │  │   Storage   │  │  Security   │  │  Blockchain   │ │
│  │ (Pydantic)│  │  (SQLite)   │  │ (PQC+AES)   │  │   (Web3)     │ │
│  └───────────┘  └─────────────┘  └─────────────┘  └───────────────┘ │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                   Decision-Making Core                          ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐     ││
│  │  │ Bio-Inspired │  │ MoE Gating   │  │ MOPD Student      │     ││
│  │  │ Optimisers   │  │ Network      │  │ Policy (RL+Dist.)│     ││
│  │  └──────────────┘  └──────────────┘  └──────────────────┘     ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                   Domain Engines (Teachers)                     ││
│  │  Thermal  │ PhaseEnergy │ EnergyScaler │ CarbonForecaster      ││
│  │  DualCarbon│ CarbonNAS  │ HeliumElastic│ MaterialSubstitution  ││
│  │  HeliumCircularity │ RegretOptimizer │ FederatedLearning      ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │              Multi-Cloud & Infrastructure Layer                  ││
│  │  AWS (boto3)  │ Azure (blob) │ GCP (storage) │ Fallback Sims   ││
│  └─────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│  Prometheus Metrics │ Structured Logging (structlog)                │
└─────────────────────────────────────────────────────────────────────┘
```

### Orchestration Gateway

- **Configuration**: Pydantic `BaseSettings` with environment variable support and strict validation.
- **Storage**: SQLite with WAL mode, indexes, and connection pooling; stores encrypted keys, blockchain records, optimisation history, and model weights.
- **Security**: AES‑256‑GCM authenticated encryption for private keys; post‑quantum signature algorithms (Dilithium, Falcon, SPHINCS) with ECDSA fallback; master key retrieved from HashiCorp Vault (no plaintext on disk).
- **Blockchain**: Ethereum smart contract integration for immutable audit trails; nonce caching, dynamic gas pricing, and circuit‑breaker protection.
- **Multi‑Cloud**: Real SDKs for AWS, Azure, GCP with retries, fallback chains, and enhanced circuit breakers.

### Domain Engines

The platform integrates a rich ecosystem of specialised engines, each acting as a **teacher** in the MOPD framework. They are located in the same `enhancements` directory:

- `thermal_optimizer` – Optimises cooling and thermal distribution.
- `phase_energy_model` – Predicts energy phase shifts.
- `energy_scaler` – Scales resources proportionally to workload.
- `marginal_carbon` – Forecasts short‑term grid carbon intensity.
- `dual_accountant` – Tracks and offsets carbon emissions (scope 1, 2, 3).
- `carbon_nas` – Performs neural architecture search with carbon constraints.
- `helium_elasticity` – Models price elasticity of green hydrogen (helium).
- `material_substitution` – Suggests sustainable material alternatives.
- `helium_circularity` – Monitors circularity metrics for helium‑based energy.
- `regret_optimizer` – Minimises long‑term regret in sequential decisions.
- `federated_learning` – Aggregates policies across distributed edge nodes.

Each engine exposes a consistent interface, enabling plug‑and‑play integration.

### Decision‑Making Core

The decision‑making core is the brain of Green Agent, composed of three interleaved layers:

1. **Bio‑Inspired Solvers** – Provide exploratory, evolutionary solutions.
2. **Mixture‑of‑Experts (MoE) Gating** – Dynamically weights and combines expert outputs.
3. **Multi‑Teacher On‑Policy Distillation (MOPD)** – Distils the combined wisdom into a lightweight student policy that is continuously updated via reinforcement learning.

This tri‑layer architecture ensures robustness, adaptability, and scalability.

---

## Key Features

### Bio‑Inspired Optimisation

Green Agent employs a family of bio‑inspired algorithms that operate at multiple timescales:

- **Genetic Algorithms (GA)** – For neural architecture search and hyperparameter tuning; uses crossover, mutation, and elitism to evolve optimal configurations.
- **Particle Swarm Optimisation (PSO)** – For continuous optimisation of scaling factors, resource allocation, and cloud routing.
- **Ant Colony Optimisation (ACO)** – For dynamic workload routing across cloud providers and data centres, minimising latency and energy.
- **Simulated Annealing** – For global exploration of the configuration space during cold‑start or major environmental shifts.

These solvers are used both as **standalone optimisers** (for periodic re‑optimisation) and as **teachers** that provide candidate actions to the MOPD student.

### Mixture‑of‑Experts (MoE) System

The MoE layer dynamically selects or blends the recommendations from all domain engines. A **gating network** – itself a small neural network – takes the current state (carbon intensity, spot prices, workload, time of day, etc.) and outputs a probability distribution over experts. The final decision is either the top‑weighted expert's action or a weighted average (soft‑blending). The gating network is trained jointly with the student policy, ensuring that the system learns **when** to trust which expert.

This modular design allows the platform to:
- **Exploit** specialised knowledge from each engine.
- **Explore** novel combinations when uncertainty is high.
- **Adapt** to changing environments by adjusting expert weights.

### Multi‑Teacher On‑Policy Distillation (MOPD)

MOPD is the heart of Green Agent's learning capability. It replaces traditional rule‑based or simple bandit controllers with a **distilled student policy** that:

- **Learns from the ensemble of teachers** – all domain experts and bio‑inspired solvers – via a distillation loss (KL divergence). The teachers’ output probabilities are averaged to form a “soft target” for the student.
- **Simultaneously optimises for real‑world rewards** – carbon saved, latency reduction, and cost savings – through a policy gradient (REINFORCE) loss.
- **Operates on‑policy**, meaning it continuously collects experience during live execution and updates its weights in a background training loop.

**Training Pipeline:**

1. **Collect** a trajectory: `(state, action, reward, next_state)` during normal operation.
2. **Compute teacher ensemble** probabilities by averaging each teacher's output.
3. **Push** to a replay buffer.
4. **Every** `train_interval` steps, sample a batch and compute:
   - **Policy loss**: `L_policy = -∑ log π(a|s) * reward`
   - **Distillation loss**: `L_distill = KL(teacher_probs || student_probs)`
   - **Total loss**: `L_total = L_policy + β * L_distill`
5. **Backpropagate** and update the student (and optionally the gating network).
6. **Periodically persist** the model weights to SQLite.

The result is a **lightweight, fast‑inference student** that generalises beyond any single teacher, adapts to evolving conditions, and requires minimal compute overhead – ideal for real‑time edge deployment.

### Carbon & Helium Awareness

Green Agent is uniquely designed to account for both **carbon intensity** (gCO₂/kWh) and **helium (green hydrogen) price elasticity**. This dual awareness enables:

- **Carbon‑Aware Scheduling**: Shift workloads to times/locations with lower grid carbon intensity, leveraging the `MarginalCarbonIntensityForecaster`.
- **Helium‑Aware Resource Allocation**: Model the cost and availability of helium fuel cells used for backup power; adjust workload placement to minimise reliance on expensive helium during peak demand, using the `HeliumPriceElasticityModel`.
- **Circularity Tracking**: The `HeliumCircularityTracker` monitors the reuse and recycling of helium, ensuring alignment with circular economy principles.
- **Dual Carbon Accounting**: The `DualCarbonAccountant` tracks both direct (scope 1) and indirect (scope 2 & 3) emissions, enabling comprehensive offset strategies.

These features make Green Agent a **first‑of‑its‑kind** platform for sustainable AI operations.

### Quantum‑Resilient Security

- **Post‑Quantum Cryptography**: Supports Dilithium, Falcon, and SPHINCS for digital signatures, with an ECDSA fallback.
- **AES‑256‑GCM**: All private keys are encrypted with authenticated encryption and stored in SQLite.
- **Master Key Protection**: The master key is never stored on disk; it is retrieved from **HashiCorp Vault** or an environment variable (as a strict fallback).
- **Automatic Key Rotation**: Keys are rotated every `KEY_ROTATION_DAYS`; a separate master key rotation can be triggered manually.

### Blockchain Verification

- **Ethereum Integration**: Uses `web3.py` to interact with any EVM‑compatible chain.
- **Nonce Caching**: Minimises RPC calls by caching transaction nonces.
- **Dynamic Gas Pricing**: Automatically adjusts gas price based on network conditions.
- **Audit Trail**: All transactions are recorded in SQLite, including block number and status.
- **Circuit Breaker**: Protects against RPC failures with configurable thresholds.

### Multi‑Cloud Distribution & Resiliency

- **Real SDKs**: AWS (`boto3`), Azure (`azure‑storage‑blob`), GCP (`google‑cloud‑storage`).
- **Retry & Exponential Backoff**: Using `tenacity` – up to 3 attempts.
- **Enhanced Circuit Breaker**: With timeout and recovery settings.
- **Fallback Chains**: Automatically tries providers in a configurable order; falls back to simulation if all real clouds fail.

### Observability & Telemetry

- **Prometheus Metrics**: Exposes counters, gauges, and histograms on a configurable port.
  - `green_agent_carbon_saved_total_g`
  - `green_agent_optimizer_decisions_total`
  - `green_agent_operation_latency_seconds`
  - `green_agent_circuit_breaker_state`
  - `green_agent_cloud_dispatches_total`
- **Structured Logging**: JSON logs with `structlog`, including correlation IDs for end‑to‑end tracing.

---

## Getting Started

### Prerequisites

- Python 3.9+
- PyTorch (for MOPD)
- HashiCorp Vault (recommended)
- Ethereum node or Infura endpoint (for blockchain)
- Cloud provider credentials (optional, for real cloud dispatch)

### Installation

```bash
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent
pip install -r requirements.txt
# Additional dependencies (if not in requirements)
pip install torch hvac prometheus-client tenacity
```

### Configuration

Create a `.env` file (or set environment variables) with the following key settings:

```ini
# Core
GREEN_AGENT_DB_PATH=green_agent.db
LOG_LEVEL=INFO
PROMETHEUS_PORT=8000

# Vault (recommended)
VAULT_ADDR=http://127.0.0.1:8200
VAULT_TOKEN=hvs.xxxx
VAULT_SECRET_PATH=green_agent/master_key

# Blockchain
ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/your_project_id
BLOCKCHAIN_PRIVATE_KEY=your_private_key_hex
BLOCKCHAIN_CONTRACT_ADDRESS=0x...
GAS_MULTIPLIER=1.2

# Cloud
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AZURE_STORAGE_CONNECTION_STRING=...
GOOGLE_APPLICATION_CREDENTIALS=path/to/gcp_key.json
DEFAULT_CLOUD_REGION=us-east-1

# MOPD Hyperparameters
MTPD_STATE_DIM=8
MTPD_ACTION_DIM=5
MTPD_HIDDEN_SIZE=128
MTPD_LR=0.001
MTPD_BETA=0.5
MTPD_GAMMA=0.99
MTPD_BUFFER_SIZE=10000
MTPD_TRAIN_INTERVAL=10
MTPD_BATCH_SIZE=32
```

### Quick Example

```python
import asyncio
from enhancements import LifecycleManager, StrategyMetrics

async def main():
    manager = LifecycleManager()
    await manager.startup()

    # Build a state vector (features: carbon intensity, spot price, workload size, time, etc.)
    state = {
        "carbon_intensity": 0.3,
        "spot_price": 0.02,
        "workload_size": 0.8,
        "latency_ms": 120,
        "cost_usd": 0.5,
    }

    # Define candidate strategies (these would normally be generated by domain engines)
    candidates = [
        StrategyMetrics("thermal_opt", 50, 0.1, 0.01, 0.9),
        StrategyMetrics("carbon_forecast", 80, 0.05, 0.02, 0.85),
        StrategyMetrics("nas_search", 200, 0.2, 0.1, 0.95),
    ]

    # Let MOPD select the best strategy
    chosen = manager.optimizer.select_strategy(state, candidates)

    # Execute the strategy (e.g., dispatch workload to cloud)
    result = await manager.cloud.dispatch_workload("aws", {"strategy": chosen.strategy_name})

    # Compute reward (e.g., carbon saved)
    reward = manager.optimizer.compute_reward(chosen, preference="carbon")

    # Update MOPD with the experience
    await manager.optimizer.update(state, chosen, reward)

    await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## API Reference

The complete API documentation is available in the `docs/` directory (generated with Sphinx). Core classes:

- `Config` – Pydantic settings.
- `Storage` – SQLite persistence.
- `QuantumResilientEnhancementsSecurity` – PQC + AES‑256‑GCM.
- `BlockchainEnhancementsVerification` – Ethereum integration.
- `MTPDOptimizer` – Multi‑Teacher On‑Policy Distillation.
- `MultiCloudDistributor` – Cloud dispatch with fallback.
- `LifecycleManager` – Async lifecycle and health checks.
- `MetricsRegistry` – Prometheus export.

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines. Key areas for involvement:

- Adding new domain experts (teachers) for emerging sustainability metrics.
- Enhancing the MOPD with advanced RL algorithms (PPO, SAC) or transformer‑based policies.
- Integrating additional cloud providers (IBM Cloud, Oracle, etc.).
- Expanding the bio‑inspired solver library.
- Improving test coverage and performance benchmarks.

---

## License

This project is licensed under the Apache License 2.0 – see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Green Agent builds upon open‑source libraries including `cryptography`, `web3.py`, `tenacity`, `structlog`, `PyTorch`, and `prometheus_client`. Special thanks to the research community advancing sustainable AI and post‑quantum cryptography.

---

**Green Agent: Orchestrating a Greener, Smarter AI Future.** 🌱💻
