# Green Agent Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│         Green Agent System Architecture                 │
└─────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Configuration Layer                                     │
│  ├─ config/green_agent.yaml                             │
│  ├─ config/environments/                                │
│  └─ src/enhancements/config.yaml                        │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Core Agent (src/core/)                                  │
│  ├─ agent.py (Main orchestrator)                        │
│  ├─ orchestrator.py (Task management)                   │
│  └─ base_classes.py (Interfaces)                        │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Enhancement Modules (src/enhancements/)                 │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ FLexGen Integration                              │   │
│  │ ├─ flexgen_controller.py                         │   │
│  │ ├─ flexgen_policy.py                            │   │
│  │ └─ flexgen_policy_selector.py                   │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ MODP Optimizer                                   │   │
│  │ ├─ pareto_optimizer.py                           │   │
│  │ ├─ pareto_router.py                              │   │
│  │ └─ adaptive_cost_function.py                     │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ LIMIT Graph (Enhanced)                           │   │
│  │ ├─ limit_graph.py                                │   │
│  │ ├─ limit_graph_v2.py                             │   │
│  │ ├─ serendipity_trace.py                          │   │
│  │ └─ reasoning_path.py                             │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Learning Modules                                 │   │
│  │ ├─ bio_inspired/ (Genetic algorithms)            │   │
│  │ ├─ moe/ (Mixture of Experts)                     │   │
│  │ ├─ rlhf/ (Preference learning)                   │   │
│  │ └─ multi_teacher_policy_distillation.py          │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Sustainability & Carbon Management               │   │
│  │ ├─ carbon_delay_scheduler.py                     │   │
│  │ ├─ sustainability_cost.py                        │   │
│  │ └─ real_carbon_intensity_api_v10.py              │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Data Center Integration                          │   │
│  │ ├─ ai_data_center_loader.py                      │   │
│  │ ├─ green_datacenter_selector.py                  │   │
│  │ └─ cloud_latency_estimator.py                    │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Helium Integration                               │   │
│  │ ├─ helium_api_collector.py                       │   │
│  │ ├─ helium_elasticity_v10.py                      │   │
│  │ ├─ helium_forecaster_v9.py                       │   │
│  │ ├─ quantum_helium_optimizer_v10.py               │   │
│  │ └─ chromatophore_compartments.py                 │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Monitoring & Metrics                             │   │
│  │ ├─ gpu_profiler.py                               │   │
│  │ ├─ metric_aggregator.py                          │   │
│  │ └─ energy_profiler.py                            │   │
│  └─────────────────┬───────────────────────────────┘   │
│                    ↓                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Integration Layer                                │   │
│  │ ├─ main_integration.py                           │   │
│  │ ├─ adapters.py                                   │   │
│  │ └─ feedback_collector.py                         │   │
│  └─────────────────────────────────────────────────┘   │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Utilities (src/utils/)                                  │
│  ├─ logger.py                                           │
│  ├─ config_loader.py                                    │
│  ├─ validators.py                                       │
│  └─ exceptions.py                                       │
└──────────────┬───────────────────────────────────────────┘
               ↓
┌──────────────────────────────────────────────────────────┐
│  Deployment & Monitoring (scripts/, config/k8s/)         │
│  ├─ scripts/deploy.sh                                   │
│  ├─ config/k8s/overlays/[environment]/                  │
│  └─ scripts/monitoring/                                 │
└──────────────────────────────────────────────────────────┘
```

## Data Flow

### Request Processing

```
1. Incoming Task Request
   ↓
2. Core Agent (agent.py) receives task
   ↓
3. Orchestrator routes to appropriate modules
   ↓
4. FLexGen Controller evaluates policies
   ↓
5. MODP optimizes multi-objectives
   ↓
6. LIMIT Graph enforces constraints
   ↓
7. Bio-Inspired explores solution space
   ↓
8. MoE routes to specialized experts
   ↓
9. Policy Distillation refines solution
   ↓
10. Sustainability check (carbon, energy)
    ↓
11. Data center selection
    ↓
12. Execution with monitoring
    ↓
13. Results returned
```

## Module Integration Points

### Configuration Flow
```
Environment Variables
    ↓
config/environments/.env.[env]
    ↓
config/green_agent.yaml
    ↓
src/enhancements/config.yaml
    ↓
Module-specific configs
```

### Feedback & Learning
```
Execution Results
    ↓
Metric Aggregation (metric_aggregator.py)
    ↓
Feedback Collection (feedback_collector.py)
    ↓
RLHF Learning (rlhf/)
    ↓
Policy Distillation
    ↓
Next iteration improvements
```

### Monitoring
```
All Modules
    ↓
GPU Profiler (gpu_profiler.py)
    ↓
Energy Profiler (energy_profiler.py)
    ↓
Metrics Aggregation
    ↓
Prometheus/Grafana
```

## Key Design Principles

1. **Modularity**: Each enhancement is independent but can be integrated
2. **Composability**: Modules can be combined in different ways
3. **Configurability**: Everything configurable via YAML/environment
4. **Observability**: Comprehensive logging and metrics
5. **Sustainability**: Carbon and energy tracking throughout
6. **Scalability**: Kubernetes-ready deployment

## Deployment Architecture

```
Local Development
    ↓ (scripts/setup.sh)
    ↓ (source config/environments/.env.development)
    ↓
Virtual Environment + Dependencies
    ↓
Python Application

Production
    ↓ (scripts/deploy.sh production)
    ↓ (config/k8s/overlays/production/)
    ↓
Docker Container
    ↓
Kubernetes Cluster
    ↓
Multiple Replicas
    ↓
Prometheus/Grafana Monitoring
```

## See Also

- [Module Guide](MODULE_GUIDE.md) - Detailed module documentation
- [Integration Guide](INTEGRATION_GUIDE.md) - Integration patterns
- [Deployment Guide](DEPLOYMENT.md) - Deployment procedures
