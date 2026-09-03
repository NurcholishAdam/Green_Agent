# Enhancement Modules

This directory contains all enhancement modules for the Green Agent system.

## Module Organization

### 🔧 Core Infrastructure
- `__init__.py` - Module initialization and exports
- `base_classes.py` - Base abstractions for all enhancements
- `adapters.py` - Integration adapters between modules
- `config.yaml` - Enhancement module configuration

### 📊 FLexGen Integration
- `flexgen_controller.py` - FLexGen backend interface
- `flexgen_policy.py` - Policy definitions
- `flexgen_policy_selector.py` - Policy selection logic

### 🎯 MODP (Multi-Objective Decision-Making)
- `MODP/` - MODP implementation folder
- `pareto_optimizer.py` - Pareto optimization
- `pareto_router.py` - Pareto-based routing
- `adaptive_cost_function.py` - Adaptive cost modeling

### 🛑 LIMIT Graph (Enhanced Constraint Engine)
- `limit_graph.py` - Core LIMIT Graph implementation
- `limit_graph_v2.py` - Enhanced version with reasoning paths
- `serendipity_trace.py` - Serendipity tracing and discovery
- `reasoning_path.py` - Reasoning path tracking
- `constraint_engine.py` - Constraint enforcement

### 🧬 Bio-Inspired Learning
- `bio_inspired/` - Bio-inspired algorithms folder
- `evolutionary_engine.py` - Evolutionary computation
- `genetic_algorithm.py` - Genetic algorithm implementation
- `fitness_evaluator.py` - Fitness evaluation

### 🌐 Mixture of Experts (MoE)
- `moe_expert_system/` - MoE system folder
- `green_agent_moe_system.py` - Main MoE orchestrator
- `expert_router_harvester.py` - Expert routing and harvesting
- `fft_moe_adapter.py` - FFT-based MoE adapter
- `context_encoder.py` - Context encoding for MoE

### 🔄 RLHF (Reinforcement Learning from Human Feedback)
- `rlhf/` - RLHF implementation folder
- `rlhf.py` - Core RLHF optimizer
- `feedback_collector.py` - Feedback collection and processing
- `preference_model.py` - Preference learning

### 🎓 Policy Distillation
- `multi_teacher_policy_distillation.py` - Multi-teacher distillation
- `distillation_orchestrator.py` - Distillation orchestration

### 🌍 Sustainability & Carbon Management
- `sustainability_cost.py` - Sustainability cost calculation
- `sustainability_signals_v10.py` - Sustainability signals
- `carbon_delay_scheduler.py` - Carbon-aware scheduling
- `carbon_credit_marketplace.py` - Carbon credit trading
- `marginal_carbon_v10.py` - Marginal carbon intensity
- `real_carbon_intensity_api_v10.py` - Real-time carbon data

### 🔬 Data Center Integration
- `ai_data_center_loader.py` - AI data center data loading
- `green_datacenter_map.py` - Data center mapping
- `green_datacenter_selector.py` - Intelligent datacenter selection
- `cloud_latency_estimator.py` - Latency estimation

### 📡 Helium Integration
- `helium_api_collector.py` - Helium API integration
- `helium_data_collector.py` - Helium data collection
- `helium_data_collector_enhanced.py` - Enhanced collection
- `helium_elasticity_v10.py` - Elasticity management
- `helium_forecaster_v9.py` - Forecasting
- `helium_scarcity_manager.py` - Resource scarcity management
- `helium_circularity.py` - Circularity tracking
- `quantum_helium_optimizer_v10.py` - Quantum-optimized Helium
- `unified_helium_integration_v4.py` - Unified Helium integration
- `chromatophore_compartments.py` - Chromatophore-based optimization

### 📈 Monitoring & Profiling
- `gpu_profiler.py` - GPU performance profiling
- `metric_aggregator.py` - Metric aggregation
- `energy_profiler.py` - Energy consumption profiling
- `energy_scaler.py` - Energy-aware scaling
- `module_benchmark_v5.py` - Module benchmarking
- `performance.py` - Performance tracking

### 🔐 Blockchain & Security
- `blockchain_helium_rights.py` - Blockchain-based rights management
- `blockchain_helium_verification.py` - Helium verification
- `dual_accountant.py` - Dual accounting system

### 🚀 Advanced Features
- `control_system.py` - Control system management
- `contextual_bandit.py` - Contextual bandit algorithms
- `policy_meta_cache.py` - Policy caching and metadata
- `node_registry.py` - Node registration system
- `mixed_precision.py` - Mixed precision computation
- `gpu_acceleration.py` - GPU acceleration
- `thermal_optimizer.py` - Thermal management
- `tokenization_optimizer.py` - Token optimization

### 🔀 Data & Synthesis
- `synthetic_data_generator.py` - Synthetic data generation
- `synthetic_data_manager_v9.py` - Synthetic data management
- `data_integration.py` - Data integration utilities

### 🎯 Optimization & Execution
- `phase_energy_model_v10.py` - Phase-based energy modeling
- `regret_optimizer_v9.py` - Regret minimization
- `system_enhancement_simulator_v4.py` - Enhancement simulation
- `green_agent_integration.py` - Unified green agent integration
- `green_agent_policy_router.py` - Policy routing
- `main_integration.py` - Main integration entry point
- `run_enhanced_agent.py` - Agent execution runner

### 🔗 Integration & Management
- `integration.py` - Cross-module integration
- `fallback_manager.py` - Fallback strategies
- `federated_learning.py` - Federated learning
- `explainable_ui.py` - Explainable UI components
- `reward_calculator.py` - Reward calculation
- `reasoning_engine.py` - Reasoning engine
- `llm_client.py` - LLM client interface
- `storage.py` - Storage management
- `carbon_api_stub.py` - Carbon API stubs

### 📤 Data Export
- `export_ai_datacenter_data.py` - AI datacenter data export
- `export_perplexity_datacenter_data.py` - Perplexity datacenter export

### 🧪 Testing
- `test_helium_integration_v10.py` - Helium integration tests
- `tests/` - Comprehensive test suite

## Configuration

Each module can be configured via:
- `green_agent_config.yaml` - Main configuration file
- Environment variables (prefixed with module name)
- Runtime parameters

## Usage Examples

See `../../examples/` for comprehensive usage examples.

## Integration Flow

```
FLexGen Controller
    ↓
MODP Optimizer (Multi-objective)
    ↓
LIMIT Graph (Constraints)
    ↓
Bio-Inspired Engine (Exploration)
    ↓
MoE System (Expert Routing)
    ↓
RLHF (Feedback Learning)
    ↓
Policy Distillation (Teacher Aggregation)
    ↓
Execution with Monitoring
    ↓
Carbon/Sustainability Tracking
```

## Key Integration Points

### With Config/Scripts
- `config/green_agent.yaml` → loads all enhancement configs
- `scripts/deploy.sh` → deploys with enhancements enabled
- `config/k8s/` → Kubernetes manifests for enhancement services

### Module Dependencies
- All modules import from `base_classes.py` for interfaces
- `adapters.py` provides cross-module communication
- `feedback_collector.py` aggregates feedback across all modules
- `metric_aggregator.py` collects metrics from all modules

## Adding New Enhancements

1. Create new module in appropriate subdirectory
2. Inherit from base classes in `base_classes.py`
3. Implement adapter in `adapters.py` if cross-module integration needed
4. Add configuration to `config.yaml`
5. Add tests to `../../tests/`
6. Document in this README
