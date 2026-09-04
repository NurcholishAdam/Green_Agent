Here is the enhanced version of the documentation index, now updated to include the advanced enhancement modules (LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating, and FlexGen integration) along with the original core documentation.


# LIMIT-AgentBench - Documentation Index

**Version**: 2.4.2  
**Status**: Production Ready  
**Date**: January 19, 2026  
**Enhanced**: Advanced modules for LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating, and FlexGen integration.

---

## 📚 Quick Navigation

### 🚀 Getting Started
1. **[Quick Start Guide](../GREEN_BENCHMARKING_QUICK_START.md)** - Start here!
   - Installation instructions
   - Basic usage examples
   - Common use cases

2. **[Installation Test](test_installation.py)** - Verify your setup (now includes enhancements check)
   ```bash
   python test_installation.py
   ```

3. **[Demo Suite](demo_green_benchmark.py)** - See it in action
   ```bash
   python demo_green_benchmark.py --enhanced   # includes advanced demos
   ```

### 📖 Documentation

#### Core Documentation
- **[README.md](README.md)** - Complete API reference
- **[Implementation Guide](../GREEN_AGENT_BENCHMARKING_COMPLETE.md)** - Full details
- **[Delivery Summary](../AGENTBENCH_DELIVERY_SUMMARY.md)** - What was delivered
- **[Original Proposal](../GREEN_AGENT_BENCHMARKING_PROPOSAL.md)** - Design rationale
- **[Enhancements Guide](src/enhancements/README.md)** - Advanced modules documentation (NEW)

#### Technical Reference
- **[Requirements](requirements.txt)** - Dependencies (updated for enhancements)
- **[Module Structure](#module-structure)** - Code organization
- **[API Reference](#api-reference)** - Class and method docs

### 🎯 Use Cases

#### For Researchers
- Benchmark your AI agents with standardized protocol
- Track energy consumption and carbon footprint
- Compare agents across different frameworks
- Submit results to unified leaderboard
- **Utilize MODP for multi‑objective trade‑offs and RLHF for human preference integration**

#### For Developers
- Integrate green metrics into your agent
- Use adapters for LangChain, AutoGen, CrewAI
- Build custom adapters for new frameworks
- Extend with custom metrics
- **Leverage NodeDescriptor/WorkloadDescriptor for adaptive routing and priority selection**

#### For Organizations
- Measure environmental impact of AI systems
- Optimize for sustainability
- Report carbon footprint
- Track efficiency improvements
- **Employ Zero Trust security with adaptive authentication and audit trails**

---

## 📁 Module Structure

```
limit-agentbench/
│
├── 📄 README.md                    # API documentation
├── 📄 INDEX.md                     # This file
├── 📄 requirements.txt             # Dependencies
├── 🐍 demo_green_benchmark.py     # Demo suite
├── 🐍 test_installation.py        # Installation test
│
├── 📁 core/                        # Core components
│   ├── agentbench_adapter.py      # AgentBench protocol
│   ├── green_metrics.py           # Energy & carbon tracking
│   ├── agent_evaluator.py         # Unified evaluation
│   └── benchmark_harness.py       # Benchmark orchestration
│
├── 📁 adapters/                    # Framework adapters
│   ├── base_adapter.py            # Abstract base
│   ├── langchain_adapter.py       # LangChain support
│   ├── autogen_adapter.py         # AutoGen support
│   ├── crewai_adapter.py          # CrewAI support
│   └── limit_graph_adapter.py     # LIMIT-GRAPH support
│
├── 📁 metrics/                     # Green metrics
│   ├── energy_tracker.py          # Energy monitoring
│   ├── carbon_calculator.py       # Carbon calculation
│   ├── efficiency_scorer.py       # Efficiency metrics
│   └── sustainability_index.py    # Sustainability scoring
│
├── 📁 dashboard/                   # Visualization
│   ├── green_leaderboard.py       # Unified leaderboard
│   ├── energy_visualizer.py       # Energy charts (enhanced)
│   ├── carbon_dashboard.py        # Carbon dashboard (enhanced)
│   └── comparison_matrix.py       # Comparison matrix (enhanced)
│
└── 📁 src/enhancements/            # ADVANCED MODULES (NEW)
    ├── schemas/
    │   ├── feedback_event.py      # Canonical event schema (v2.2)
    │   ├── node_descriptor.py     # Adaptive routing with distillation + MoE
    │   └── workload_descriptor.py # Adaptive priority selection
    ├── zero_trust_architecture.py # Security with adaptive authentication
    ├── async_message_queue.py     # Cross-module messaging
    ├── core/
    │   ├── graph_registry.py      # LIMIT Graph lifecycle manager
    │   ├── causal_graph.py        # Root-cause attribution
    │   ├── meta_cognition.py      # Anomaly diagnosis
    │   └── policy_graph.py        # Multi-hop decision engine
    ├── metrics/
    │   └── dag_carbon_ledger.py   # Carbon backpropagation
    └── ...
```

---

## 🔧 API Reference

### Core Classes

#### AgentBenchAdapter
```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter()
task = adapter.create_task(task_id, suite, task_type, input_data, ...)
result = adapter.evaluate_agent(agent, task, track_energy=True, ...)
```

#### GreenMetricsTracker
```python
from limit_agentbench import GreenMetricsTracker

tracker = GreenMetricsTracker(grid_region="US-CA", hardware_profile="nvidia_a100")
with tracker:
    # Your code here
    pass
metrics = tracker.get_metrics()
```

#### AgentEvaluator
```python
from limit_agentbench import AgentEvaluator

evaluator = AgentEvaluator(track_green_metrics=True)
result = evaluator.evaluate(agent, task)
comparison = evaluator.compare_agents(agents, tasks)
```

#### BenchmarkHarness
```python
from limit_agentbench import BenchmarkHarness

harness = BenchmarkHarness(output_dir="./results")
result = harness.run_benchmark(agent, task_suite, benchmark_name)
```

### Adapter Classes

```python
from limit_agentbench.adapters import (
    LangChainAdapter,
    AutoGenAdapter,
    CrewAIAdapter,
    LimitGraphAdapter
)

# Wrap your agent
adapter = LangChainAdapter(my_agent)
result = adapter.run(task_input, track_green_metrics=True)
```

### Metrics Classes

```python
from limit_agentbench.metrics import (
    EnergyTracker,
    CarbonCalculator,
    EfficiencyScorer,
    SustainabilityIndex
)

# Calculate sustainability
si_calc = SustainabilityIndex()
si = si_calc.calculate(accuracy, energy_kwh, carbon_co2e_kg)
```

### Dashboard Classes

```python
from limit_agentbench import GreenLeaderboard

leaderboard = GreenLeaderboard()
leaderboard.submit(agent_name, framework, task_suite, accuracy, ...)
rankings = leaderboard.get_rankings(sort_by="sustainability_index")
```

### Enhanced Classes (NEW)

```python
# NodeDescriptor – adaptive routing with distillation, MoE, RLHF, LIMIT Graph
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType

node = NodeDescriptor(
    id="node1",
    type=NodeType.EDGE,
    region="us-east",
    region_carbon_intensity=400.0,
    energy_per_token=0.00005,
    use_evolutionary=True,
    human_feedback_score=0.6,
    graph_metrics={"centrality": 0.8}
)
strategy = await node.select_routing_strategy()   # returns optimal strategy

# WorkloadDescriptor – adaptive priority selection
from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency

wl = WorkloadDescriptor(
    task_id="task1",
    task_type=TaskType.INFERENCE,
    tokens=1000,
    latency_target=500.0,
    urgency=Urgency.MEDIUM,
    use_evolutionary=True,
    human_feedback_score=0.7,
    graph_metrics={"centrality": 0.6}
)
priority = await wl.select_priority()   # returns accuracy/green/balanced

# FeedbackEvent – canonical event with MODP/RLHF fields
from src.enhancements.schemas.feedback_event import FeedbackEvent

event = FeedbackEvent(
    source="my_agent",
    feedback_type="routing",
    task_id="t1",
    context={},
    action={"selected_action": "execute"},
    performance={"quality_score": 0.9, "latency_ms": 100, "energy_joules": 100},
    adaptive_cost_value=0.85,
    graph_metrics={"centrality": 0.7},
    human_feedback_score=0.8,
    modp_score=0.75
)
```

---

## 🌟 Key Features

### ✅ AgentBench Compatible
- Standardized task and result formats
- Protocol version tracking
- Provenance hash generation

### 🌱 Green Metrics
- Energy consumption (kWh)
- Carbon emissions (CO2e kg)
- Power usage (Watts)
- Sustainability index
- **MODP composite score** (multi‑objective trade‑off)

### 🔌 Multi-Framework
- LangChain/LangGraph
- Microsoft AutoGen
- CrewAI
- LIMIT-GRAPH

### 📊 Unified Leaderboard
- Cross-framework rankings
- Framework statistics
- Agent history tracking

### 🧠 Advanced Enhancements (NEW)
- **LIMIT Graph** – topology-aware metrics (centrality, connectivity) influence decisions
- **MODP** – configurable objective weights (accuracy, energy, latency, carbon)
- **RLHF** – human feedback score integrated into teachers and reward shaping
- **Multi‑Teacher On‑Policy Distillation** – lightweight student learns from rule‑based, ML, Q‑learning, RLHF teachers
- **MoE Expert Gating** – dynamic blending of teacher outputs
- **Bio‑inspired Optimisation** – evolutionary tuning of weights and hyperparameters
- **FlexGen Integration** – optional high‑throughput LLM execution with adaptive precision

---

## 📊 Supported Configurations

### Grid Regions (13)
US-CA, US-TX, US-NY, EU-FR, EU-DE, EU-NO, EU-PL, CN, IN, JP, AU, BR, GLOBAL

### Hardware Profiles (10)
nvidia_a100, nvidia_v100, nvidia_t4, nvidia_rtx_3090, nvidia_rtx_4090, amd_mi250, google_tpu_v4, cpu_intel_xeon, cpu_amd_epyc, default

### Enhancement Flags (NEW)
- `ENHANCEMENTS_ENABLED` – master switch
- `LIMIT_GRAPH_ENABLED`
- `MODP_ENABLED`
- `RLHF_ENABLED`
- `DISTILLATION_ENABLED`
- `MOE_GATING_ENABLED`
- `EVOLUTIONARY_ENABLED`
- `FLEXGEN_ENABLED`

---

## 🎓 Examples

### Example 1: Basic Evaluation
```python
from limit_agentbench import AgentBenchAdapter

adapter = AgentBenchAdapter()
task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is AI?"}
)

result = adapter.evaluate_agent(my_agent, task, track_energy=True)
print(f"SI: {result['metrics']['sustainability_index']:.2f}")
```

### Example 2: Framework Comparison
```python
from limit_agentbench import AgentEvaluator
from limit_agentbench.adapters import LangChainAdapter, AutoGenAdapter

evaluator = AgentEvaluator(track_green_metrics=True)
comparison = evaluator.compare_agents(
    agents=[LangChainAdapter(agent1), AutoGenAdapter(agent2)],
    tasks=task_suite,
    sort_by="sustainability_index"
)
```

### Example 3: Leaderboard
```python
from limit_agentbench import GreenLeaderboard

leaderboard = GreenLeaderboard()
leaderboard.submit(
    agent_name="MyAgent",
    framework="langchain",
    task_suite="benchmark",
    accuracy=0.95,
    energy_kwh=0.003,
    carbon_co2e_kg=0.0006,
    latency_ms=150
)

top_agents = leaderboard.get_top_agents(n=10)
```

### Example 4: Enhanced Routing with Distillation + MoE
```python
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
import asyncio

node = NodeDescriptor(
    id="node1",
    type=NodeType.EDGE,
    region="us-east",
    region_carbon_intensity=350.0,
    energy_per_token=0.00004,
    use_evolutionary=True,
    human_feedback_score=0.7,
    graph_metrics={"centrality": 0.8}
)

strategy = asyncio.run(node.select_routing_strategy())
print(f"Selected strategy: {strategy}")   # carbon_first, latency_first, cost_first, balanced, adaptive
```

### Example 5: MODP + RLHF in Workload Priority
```python
from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType
import asyncio

wl = WorkloadDescriptor(
    task_id="task1",
    task_type=TaskType.INFERENCE,
    tokens=1000,
    latency_target=300.0,
    use_evolutionary=True,
    human_feedback_score=0.6,
    graph_metrics={"centrality": 0.7},
    metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2}
)

priority = asyncio.run(wl.select_priority())
print(f"Priority: {priority}")   # accuracy, green, balanced
```

---

## 🧪 Testing

### Run Installation Test
```bash
python test_installation.py
```

Expected output (enhanced):
```
✓ Core modules imported successfully
✓ Adapter modules imported successfully
✓ Metrics modules imported successfully
✓ Dashboard modules imported successfully
✓ Enhanced modules imported successfully (optional)
✓ Installation verified successfully!
```

### Run Demo Suite (with enhancements)
```bash
python demo_green_benchmark.py --enhanced
```

Demonstrates:
1. AgentBench protocol
2. Green metrics tracking
3. Multi-framework adapters
4. Sustainability index
5. Green leaderboard
6. Benchmark harness
7. **Advanced enhancements (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary)**

---

## 🔗 Related Documentation

### In This Repository
- [Quantum LIMIT-GRAPH v2.4.0](../README.md)
- [Level 3 Maturity](../LEVEL_3_MATURITY_COMPLETE.md)
- [Level 5 MetaAgent](../LEVEL_5_COMPLETE.md)
- [NSN Integration](../../nsn_integration/README.md)
- [Enhancements README](src/enhancements/README.md) (NEW)

### External Resources
- [AgentBench](https://github.com/THUDM/AgentBench) - Original protocol
- [LangChain](https://python.langchain.com/) - Framework docs
- [AutoGen](https://microsoft.github.io/autogen/) - Framework docs
- [CrewAI](https://www.crewai.io/) - Framework docs

---

## 🤝 Contributing

We welcome contributions! Areas for contribution:
- Additional framework adapters
- Visualization components
- Integration bridges
- Documentation improvements
- Bug fixes and optimizations
- New teachers for distillation
- Enhancements to MODP / RLHF / evolutionary modules

---

## 📞 Support

### Documentation
- **Quick Start**: [GREEN_BENCHMARKING_QUICK_START.md](../GREEN_BENCHMARKING_QUICK_START.md)
- **API Reference**: [README.md](README.md)
- **Implementation**: [GREEN_AGENT_BENCHMARKING_COMPLETE.md](../GREEN_AGENT_BENCHMARKING_COMPLETE.md)
- **Enhancements**: [src/enhancements/README.md](src/enhancements/README.md)

### Testing
- **Installation**: `python test_installation.py`
- **Demo**: `python demo_green_benchmark.py --enhanced`

### Issues
- Report bugs and request features on GitHub
- Check existing documentation first
- Provide minimal reproducible examples

---

## 📈 Roadmap

### Phase 1: Core Implementation ✅
- [x] AgentBench protocol
- [x] Green metrics tracking
- [x] Multi-framework adapters
- [x] Unified leaderboard

### Phase 2: Integration (Next)
- [ ] NSN integration bridge
- [ ] Level 5 MetaAgent bridge
- [ ] limit-benchmark crate bridge

### Phase 3: Visualization (Planned)
- [ ] Energy consumption charts
- [ ] Carbon footprint dashboard
- [ ] Interactive comparison matrix

### Phase 4: Deployment (Planned)
- [ ] Hugging Face Spaces dashboard
- [ ] Public leaderboard
- [ ] REST API endpoints

### Phase 5: Advanced Enhancements (Completed)
- [x] LIMIT Graph metrics integration
- [x] MODP multi-objective scoring
- [x] RLHF human feedback
- [x] Multi-teacher distillation with MoE gating
- [x] Bio-inspired evolutionary optimisation
- [x] FlexGen execution backend hooks

---

## 📄 License

Apache-2.0 License - See LICENSE file for details

---

## 🎉 Quick Start

1. **Install**: `pip install psutil numpy scikit-learn pandas pydantic`
2. **Test**: `python test_installation.py`
3. **Demo**: `python demo_green_benchmark.py --enhanced`
4. **Read**: [Quick Start Guide](../GREEN_BENCHMARKING_QUICK_START.md)
5. **Use**: Start benchmarking your agents with green metrics and advanced enhancements!

---

**Version**: 2.4.2  
**Status**: Production Ready  
**Date**: January 19, 2026

**🚀 Ready to benchmark your agents with green metrics and advanced decision-making!**
