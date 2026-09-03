# Module Guide

## Overview

This guide provides detailed documentation for each enhancement module in the Green Agent system.

## Module Categories

### 1. Policy & Optimization Modules

#### FLexGen Integration
**Location**: `src/enhancements/flexgen/`
**Purpose**: Interface with FLexGen LLM inference optimization backend
**Key Files**:
- `controller.py` - Main FLexGen controller
- `policy.py` - Policy definitions
- `policy_selector.py` - Policy selection algorithms

**Configuration**: `config/green_agent.yaml` → `flexgen` section
**Dependencies**: None (standalone backend)

#### MODP (Multi-Objective Decision-Making)
**Location**: `src/enhancements/optimization/`
**Purpose**: Multi-objective optimization with Pareto efficiency
**Key Files**:
- `modp.py` - Core MODP implementation
- `pareto_optimizer.py` - Pareto optimization algorithms
- `pareto_router.py` - Routing based on Pareto fronts
- `adaptive_cost_function.py` - Dynamic cost modeling

**Configuration**: `config/green_agent.yaml` → `modp` section
**Dependencies**: scipy, numpy

### 2. Constraint & Reasoning Modules

#### LIMIT Graph (Enhanced)
**Location**: `src/enhancements/constraints/`
**Purpose**: Constraint enforcement with reasoning path tracking
**Key Files**:
- `limit_graph.py` - Core implementation
- `limit_graph_v2.py` - Enhanced version
- `serendipity_trace.py` - Serendipitous discovery tracking
- `reasoning_path.py` - Reasoning path management
- `constraint_engine.py` - Constraint enforcement

**Configuration**: `config/green_agent.yaml` → `limit_graph` section
**Dependencies**: networkx, numpy
**Evolution Path**:
  - LIMIT Graph (original)
  - → Enhanced LIMIT Graph v2
  - → + Serendipity Trace
  - → + Reasoning Paths
  - → Current (v2.4.0+)

### 3. Learning Modules

#### Bio-Inspired Computing
**Location**: `src/enhancements/learning/bio_inspired/`
**Purpose**: Evolutionary algorithms for policy exploration
**Key Files**:
- `genetic_algorithm.py` - GA implementation
- `fitness_evaluator.py` - Fitness calculation
- `policy_generator.py` - Policy generation
- `evolutionary_engine.py` - Main evolutionary loop

**Configuration**: `config/green_agent.yaml` → `bio` section
**Parameters**:
  - `population_size`: GA population size
  - `generations`: Number of generations
  - `mutation_rate`: Mutation probability
  - `crossover_rate`: Crossover probability

#### Mixture of Experts (MoE)
**Location**: `src/enhancements/learning/moe/`
**Purpose**: Route to specialized expert networks
**Key Files**:
- `expert_router.py` - Main routing logic
- `expert_harvester.py` - Expert resource harvesting
- `context_encoder.py` - Context representation
- `gating_network.py` - Gating mechanism
- `moe_system.py` - System orchestration
- `green_agent_moe_system.py` - Green Agent-specific MoE

**Configuration**: `config/green_agent.yaml` → `moe` section
**Features**:
  - Load balancing across experts
  - Dynamic expert discovery
  - Context-aware routing
  - Expert specialization

#### RLHF (Reinforcement Learning from Human Feedback)
**Location**: `src/enhancements/learning/rlhf/`
**Purpose**: Learn from user preferences
**Key Files**:
- `optimizer.py` - RLHF optimizer
- `preference_model.py` - Preference learning
- `reward_model.py` - Reward modeling
- `feedback_processor.py` - Feedback handling

**Configuration**: `config/green_agent.yaml` → `rlhf` section
**Workflow**:
  1. Collect feedback from users
  2. Train reward model
  3. Generate preference-aligned policies
  4. Validate improvements

#### Policy Distillation
**Location**: `src/enhancements/learning/distillation/`
**Purpose**: Compress multiple teacher policies into student
**Key Files**:
- `multi_teacher.py` - Multi-teacher distillation
- `distillation_engine.py` - Distillation algorithms
- `teacher_policies.py` - Teacher definitions
- `student_network.py` - Student model

**Configuration**: `config/green_agent.yaml` → `distillation` section
**Teachers**:
  - FLexGen optimal policy
  - MODP-optimized policy
  - Bio-inspired policy
  - MoE-aggregated policy

### 4. Sustainability Modules

#### Carbon & Sustainability Management
**Location**: `src/enhancements/` (root level)
**Purpose**: Track and optimize carbon emissions
**Key Files**:
- `sustainability_cost.py` - Cost calculation with carbon
- `sustainability_signals_v10.py` - Sustainability metrics
- `carbon_delay_scheduler.py` - Carbon-aware scheduling
- `carbon_credit_marketplace.py` - Carbon credit trading
- `marginal_carbon_v10.py` - Marginal emissions
- `real_carbon_intensity_api_v10.py` - Real-time grid data

**Configuration**: `config/green_agent.yaml` → `carbon` section
**Data Sources**:
  - WattTime API
  - ElectricityMap API
  - National grid operators
  - Simulated data (fallback)

### 5. Infrastructure Modules

#### Data Center Integration
**Location**: `src/enhancements/` (root level)
**Purpose**: Select optimal data center based on criteria
**Key Files**:
- `ai_data_center_loader.py` - AI datacenter data loading
- `green_datacenter_map.py` - Datacenter mapping
- `green_datacenter_selector.py` - Intelligent selection
- `cloud_latency_estimator.py` - Latency estimation

**Configuration**: `config/green_agent.yaml` → `datacenters` section
**Selection Criteria**:
  - Carbon intensity
  - Latency
  - Cost
  - Availability
  - Green energy %

#### Helium Integration
**Location**: `src/enhancements/` (root level)
**Purpose**: Helium network integration and optimization
**Key Files**:
- `helium_api_collector.py` - Helium API
- `helium_data_collector.py` - Data collection
- `helium_elasticity_v10.py` - Resource elasticity
- `helium_forecaster_v9.py` - Forecasting
- `helium_scarcity_manager.py` - Scarcity handling
- `helium_circularity.py` - Circular economy integration
- `quantum_helium_optimizer_v10.py` - Quantum optimization
- `unified_helium_integration_v4.py` - Unified system
- `chromatophore_compartments.py` - Dynamic compartmentalization

**Configuration**: `config/green_agent.yaml` → `helium` section
**Features**:
  - Real-time resource monitoring
  - Dynamic scaling
  - Quantum-optimized routing
  - Chromatophore-based adaptation

### 6. Monitoring & Observability

**Location**: `src/enhancements/monitoring/`
**Key Files**:
- `gpu_profiler.py` - GPU metrics
- `metric_aggregator.py` - Centralized metrics
- `energy_profiler.py` - Energy consumption
- `carbon_tracker.py` - Carbon tracking
- `performance_monitor.py` - Performance metrics

**Outputs**:
- Prometheus metrics
- Grafana dashboards
- CSV logs
- JSON reports

## Configuration Examples

### Basic Setup
```yaml
# config/green_agent.yaml
agent:
  name: "green_agent"
  version: "2.4.0"
  
flexgen:
  enabled: true
  backend_url: "http://localhost:8000"
  
modp:
  enabled: true
  method: "pareto"
  weights: [0.4, 0.3, 0.3]
  
limit_graph:
  enabled: true
  max_nodes: 100
  enable_serendipity: true
  enable_reasoning_paths: true
  
bio:
  enabled: true
  population_size: 50
  generations: 20
  
moe:
  enabled: true
  num_experts: 4
  
rlhf:
  enabled: true
  buffer_size: 1000
  
distillation:
  enabled: true
  update_interval: 600
```

### Carbon Configuration
```yaml
carbon:
  enabled: true
  api_provider: "watttime"  # or "electricitymap"
  threshold_gco2_per_kwh: 400
  max_delay_seconds: 300
  
helium:
  enabled: true
  api_endpoint: "https://helium.api.example.com"
  use_quantum_optimizer: true
  enable_chromatophore: true
```

## Integration Patterns

### Sequential Pipeline
```python
from src.enhancements import FlexGenController, MODPOptimizer, LimitGraph

# Request flows through each module
policy = flexgen.select_policy(task)
policy = modp.optimize(policy, weights)
policy = limit_graph.enforce(policy)
```

### Parallel Exploration
```python
from src.enhancements import BioInspired, MoESystem

# Run in parallel
policies_bio = bio_inspired.evolve()
policies_moe = moe.route()
policies = combine([policies_bio, policies_moe])
```

### Feedback Loop
```python
from src.enhancements import RLHF, Distillation

# Collect feedback and improve
feedback = collector.gather(execution)
rlhf.update(feedback)
distillation.train(teachers=[...])
```

## Performance Considerations

### Optimization for Speed
- Use `flexgen_controller.py` for fast policy selection
- Enable caching in `policy_meta_cache.py`
- Reduce GA generations in development

### Optimization for Accuracy
- Increase MODP weights precision
- Expand MoE expert pool
- Increase RLHF buffer size and training iterations

### Optimization for Sustainability
- Enable carbon-aware scheduling
- Use real carbon intensity data
- Leverage Helium optimization

## Troubleshooting

See [Deployment Guide](DEPLOYMENT.md) for troubleshooting specific modules.

## See Also

- [Integration Guide](INTEGRATION_GUIDE.md) - Module integration patterns
- [API Reference](API.md) - API documentation
