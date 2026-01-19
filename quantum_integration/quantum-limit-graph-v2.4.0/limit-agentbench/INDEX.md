# LIMIT-AgentBench - Documentation Index

**Version**: 2.4.2  
**Status**: Production Ready  
**Date**: January 19, 2026

---

## 📚 Quick Navigation

### 🚀 Getting Started
1. **[Quick Start Guide](../GREEN_BENCHMARKING_QUICK_START.md)** - Start here!
   - Installation instructions
   - Basic usage examples
   - Common use cases

2. **[Installation Test](test_installation.py)** - Verify your setup
   ```bash
   python test_installation.py
   ```

3. **[Demo Suite](demo_green_benchmark.py)** - See it in action
   ```bash
   python demo_green_benchmark.py
   ```

### 📖 Documentation

#### Core Documentation
- **[README.md](README.md)** - Complete API reference
- **[Implementation Guide](../GREEN_AGENT_BENCHMARKING_COMPLETE.md)** - Full details
- **[Delivery Summary](../AGENTBENCH_DELIVERY_SUMMARY.md)** - What was delivered
- **[Original Proposal](../GREEN_AGENT_BENCHMARKING_PROPOSAL.md)** - Design rationale

#### Technical Reference
- **[Requirements](requirements.txt)** - Dependencies
- **[Module Structure](#module-structure)** - Code organization
- **[API Reference](#api-reference)** - Class and method docs

### 🎯 Use Cases

#### For Researchers
- Benchmark your AI agents with standardized protocol
- Track energy consumption and carbon footprint
- Compare agents across different frameworks
- Submit results to unified leaderboard

#### For Developers
- Integrate green metrics into your agent
- Use adapters for LangChain, AutoGen, CrewAI
- Build custom adapters for new frameworks
- Extend with custom metrics

#### For Organizations
- Measure environmental impact of AI systems
- Optimize for sustainability
- Report carbon footprint
- Track efficiency improvements

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
└── 📁 dashboard/                   # Visualization
    ├── green_leaderboard.py       # Unified leaderboard
    ├── energy_visualizer.py       # Energy charts (placeholder)
    ├── carbon_dashboard.py        # Carbon dashboard (placeholder)
    └── comparison_matrix.py       # Comparison matrix (placeholder)
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

### 🔌 Multi-Framework
- LangChain/LangGraph
- Microsoft AutoGen
- CrewAI
- LIMIT-GRAPH

### 📊 Unified Leaderboard
- Cross-framework rankings
- Framework statistics
- Agent history tracking

---

## 📊 Supported Configurations

### Grid Regions (13)
US-CA, US-TX, US-NY, EU-FR, EU-DE, EU-NO, EU-PL, CN, IN, JP, AU, BR, GLOBAL

### Hardware Profiles (10)
nvidia_a100, nvidia_v100, nvidia_t4, nvidia_rtx_3090, nvidia_rtx_4090, amd_mi250, google_tpu_v4, cpu_intel_xeon, cpu_amd_epyc, default

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

---

## 🧪 Testing

### Run Installation Test
```bash
python test_installation.py
```

Expected output:
```
✓ Core modules imported successfully
✓ Adapter modules imported successfully
✓ Metrics modules imported successfully
✓ Dashboard modules imported successfully
✓ Installation verified successfully!
```

### Run Demo Suite
```bash
python demo_green_benchmark.py
```

Demonstrates:
1. AgentBench protocol
2. Green metrics tracking
3. Multi-framework adapters
4. Sustainability index
5. Green leaderboard
6. Benchmark harness

---

## 🔗 Related Documentation

### In This Repository
- [Quantum LIMIT-GRAPH v2.4.0](../README.md)
- [Level 3 Maturity](../LEVEL_3_MATURITY_COMPLETE.md)
- [Level 5 MetaAgent](../LEVEL_5_COMPLETE.md)
- [NSN Integration](../../nsn_integration/README.md)

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

---

## 📞 Support

### Documentation
- **Quick Start**: [GREEN_BENCHMARKING_QUICK_START.md](../GREEN_BENCHMARKING_QUICK_START.md)
- **API Reference**: [README.md](README.md)
- **Implementation**: [GREEN_AGENT_BENCHMARKING_COMPLETE.md](../GREEN_AGENT_BENCHMARKING_COMPLETE.md)

### Testing
- **Installation**: `python test_installation.py`
- **Demo**: `python demo_green_benchmark.py`

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

---

## 📄 License

Apache-2.0 License - See LICENSE file for details

---

## 🎉 Quick Start

1. **Install**: `pip install psutil numpy`
2. **Test**: `python test_installation.py`
3. **Demo**: `python demo_green_benchmark.py`
4. **Read**: [Quick Start Guide](../GREEN_BENCHMARKING_QUICK_START.md)
5. **Use**: Start benchmarking your agents!

---

**Version**: 2.4.2  
**Status**: Production Ready  
**Date**: January 19, 2026

**🚀 Ready to benchmark your agents with green metrics!**
