# LIMIT-AgentBench: Green Agent Benchmarking Platform

**Version**: 2.4.2  
**Status**: Production Ready  
**License**: Apache-2.0

## Overview

LIMIT-AgentBench is a comprehensive benchmarking platform for AI agents that combines:
- **AgentBench Protocol**: Standardized evaluation framework
- **Green Metrics**: Energy consumption and carbon footprint tracking
- **Multi-Framework Support**: LangChain, AutoGen, CrewAI, LIMIT-GRAPH
- **Unified Leaderboard**: Cross-framework rankings with sustainability focus

## Key Features

### 🌟 AgentBench Compatible
- Standardized task and result formats
- Protocol version tracking
- Provenance hash generation
- JSON import/export

### 🌱 Green Metrics
- Energy consumption (kWh)
- Carbon emissions (CO2e kg)
- Power usage (Watts)
- Efficiency scores
- Sustainability index

### 🔌 Multi-Framework
- LangChain/LangGraph agents
- Microsoft AutoGen agents
- CrewAI agents
- LIMIT-GRAPH quantum agents
- Extensible adapter system

### 📊 Unified Leaderboard
- Cross-framework rankings
- Multiple sort criteria
- Framework statistics
- Agent history tracking

## Quick Start

### Installation

```bash
# Install dependencies
pip install psutil numpy

# Navigate to module
cd quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench
```

### Basic Usage

```python
from limit_agentbench import AgentBenchAdapter, GreenMetricsTracker

# 1. Create adapter
adapter = AgentBenchAdapter()

# 2. Create task
task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is the capital of France?"},
    expected_output={"answer": "Paris"}
)

# 3. Evaluate agent with green metrics
result = adapter.evaluate_agent(
    agent=my_agent,
    task=task,
    track_energy=True,
    track_carbon=True
)

# 4. View results
print(f"Accuracy: {result['metrics']['accuracy']:.2%}")
print(f"Energy: {result['metrics']['energy_kwh']:.6f} kWh")
print(f"Carbon: {result['metrics']['carbon_co2e_kg']:.6f} kg CO2e")
print(f"Sustainability: {result['metrics']['sustainability_index']:.2f}")
```

### Multi-Framework Comparison

```python
from limit_agentbench import AgentEvaluator
from limit_agentbench.adapters import LangChainAdapter, AutoGenAdapter

# Create evaluator
evaluator = AgentEvaluator(
    grid_region="US-CA",
    track_green_metrics=True
)

# Wrap agents from different frameworks
agents = [
    LangChainAdapter(my_langchain_agent),
    AutoGenAdapter(my_autogen_agent)
]

# Compare agents
comparison = evaluator.compare_agents(
    agents=agents,
    tasks=task_suite,
    sort_by="sustainability_index"
)

# View rankings
for rank in comparison['rankings']:
    print(f"{rank['agent_name']}: SI={rank['aggregated_metrics']['avg_sustainability_index']:.2f}")
```

### Leaderboard Submission

```python
from limit_agentbench import GreenLeaderboard

# Initialize leaderboard
leaderboard = GreenLeaderboard()

# Submit result
leaderboard.submit(
    agent_name="MyAgent-v1",
    framework="langchain",
    task_suite="agentbench_qa",
    accuracy=0.95,
    energy_kwh=0.003,
    carbon_co2e_kg=0.0006,
    latency_ms=150
)

# Get top agents
top_agents = leaderboard.get_top_agents(n=10, sort_by="sustainability_index")
```

## Running the Demo

```bash
python demo_green_benchmark.py
```

The demo includes:
1. AgentBench protocol demonstration
2. Green metrics tracking
3. Multi-framework adapters
4. Sustainability index calculation
5. Green leaderboard
6. Benchmark harness

## Module Structure

```
limit-agentbench/
├── core/                    # Core components
│   ├── agentbench_adapter.py
│   ├── green_metrics.py
│   ├── agent_evaluator.py
│   └── benchmark_harness.py
├── adapters/                # Framework adapters
│   ├── base_adapter.py
│   ├── langchain_adapter.py
│   ├── autogen_adapter.py
│   ├── crewai_adapter.py
│   └── limit_graph_adapter.py
├── metrics/                 # Green metrics
│   ├── energy_tracker.py
│   ├── carbon_calculator.py
│   ├── efficiency_scorer.py
│   └── sustainability_index.py
└── dashboard/               # Visualization
    └── green_leaderboard.py
```

## Supported Frameworks

### LangChain
```python
from limit_agentbench.adapters import LangChainAdapter

adapter = LangChainAdapter(my_langchain_agent)
result = adapter.run(task_input, track_green_metrics=True)
```

### AutoGen
```python
from limit_agentbench.adapters import AutoGenAdapter

adapter = AutoGenAdapter(my_autogen_agent)
result = adapter.run(task_input, track_green_metrics=True)
```

### CrewAI
```python
from limit_agentbench.adapters import CrewAIAdapter

adapter = CrewAIAdapter(my_crewai_agent)
result = adapter.run(task_input, track_green_metrics=True)
```

### LIMIT-GRAPH
```python
from limit_agentbench.adapters import LimitGraphAdapter

adapter = LimitGraphAdapter(my_limit_graph_agent)
result = adapter.run(task_input, track_green_metrics=True)
```

## Green Metrics

### Energy Tracking
- Real-time power monitoring
- Hardware-specific power profiles
- Energy per task calculation
- Peak power detection

### Carbon Calculation
- Regional carbon intensity
- CO2e emissions calculation
- Carbon savings estimation
- Equivalent metrics (trees, miles)

### Efficiency Scoring
- Performance per watt
- Cost efficiency
- Throughput efficiency
- Cross-agent comparison

### Sustainability Index
- Composite green score
- Weighted metric combination
- Agent ranking
- Qualitative ratings

## Grid Regions

Supported regions with carbon intensity (kg CO2e/kWh):
- **US-CA** (California): 0.2
- **US-TX** (Texas): 0.4
- **EU-FR** (France): 0.05
- **EU-DE** (Germany): 0.35
- **CN** (China): 0.6
- **IN** (India): 0.7
- **GLOBAL** (Average): 0.475

## Hardware Profiles

Supported hardware with power consumption (Watts):
- **nvidia_a100**: 400W
- **nvidia_v100**: 300W
- **nvidia_t4**: 70W
- **nvidia_rtx_3090**: 350W
- **nvidia_rtx_4090**: 450W
- **amd_mi250**: 500W
- **google_tpu_v4**: 200W

## API Reference

### Core Classes

#### AgentBenchAdapter
```python
adapter = AgentBenchAdapter(protocol_version="1.0")
task = adapter.create_task(task_id, suite, task_type, input_data, ...)
result = adapter.evaluate_agent(agent, task, track_energy=True, ...)
```

#### GreenMetricsTracker
```python
tracker = GreenMetricsTracker(grid_region="US-CA", hardware_profile="nvidia_a100")
tracker.start()
# ... agent execution ...
tracker.stop()
metrics = tracker.get_metrics()
```

#### AgentEvaluator
```python
evaluator = AgentEvaluator(grid_region="US-CA", track_green_metrics=True)
result = evaluator.evaluate(agent, task)
suite_result = evaluator.evaluate_suite(agent, tasks)
comparison = evaluator.compare_agents(agents, tasks)
```

#### BenchmarkHarness
```python
harness = BenchmarkHarness(output_dir="./results", grid_region="US-CA")
result = harness.run_benchmark(agent, task_suite, benchmark_name)
multi_result = harness.run_multi_agent_benchmark(agents, task_suite, benchmark_name)
```

### Adapter Classes

All adapters inherit from `BaseAgentAdapter` and provide:
- `run(task_input, track_green_metrics=True)`: Execute agent
- `get_metadata()`: Get agent metadata

### Metrics Classes

#### EnergyTracker
```python
tracker = EnergyTracker(hardware_power_watts=400, sampling_interval=0.1)
tracker.start()
tracker.sample()  # Take power sample
tracker.stop()
metrics = tracker.get_metrics()
```

#### CarbonCalculator
```python
calculator = CarbonCalculator(grid_region="US-CA")
emissions = calculator.calculate_emissions(energy_kwh)
savings = calculator.calculate_savings(baseline_kwh, optimized_kwh)
```

#### EfficiencyScorer
```python
scorer = EfficiencyScorer(grid_region="US-CA")
efficiency = scorer.calculate_efficiency_score(accuracy, energy_kwh)
cost_eff = scorer.calculate_cost_efficiency(accuracy, energy_kwh)
```

#### SustainabilityIndex
```python
si_calc = SustainabilityIndex()
si = si_calc.calculate(accuracy, energy_kwh, carbon_co2e_kg)
rankings = si_calc.rank_agents(agent_metrics)
```

### Dashboard Classes

#### GreenLeaderboard
```python
leaderboard = GreenLeaderboard(storage_path="./leaderboard_data")
leaderboard.submit(agent_name, framework, task_suite, accuracy, energy_kwh, ...)
rankings = leaderboard.get_rankings(sort_by="sustainability_index")
top_agents = leaderboard.get_top_agents(n=10)
```

## Contributing

We welcome contributions! Areas for contribution:
- Additional framework adapters
- Visualization components
- Integration bridges
- Documentation improvements
- Bug fixes and optimizations

## License

MIT License - See LICENSE file for details

## Citation

If you use LIMIT-AgentBench in your research, please cite:

```bibtex
@software{limit_agentbench_2026,
  title={Green-Agent: Green Agent Benchmarking Platform},
  author={AI Research Agent Team},
  year={2026},
  version={2.4.2},
  url={https://github.com/NurcholishAdam/quantum-limit-graph}
}
```

## Support

- **Documentation**: See GREEN_AGENT_BENCHMARKING_COMPLETE.md
- **Demo**: Run `python demo_green_benchmark.py`
- **Issues**: Report on GitHub
- **Questions**: Open a discussion

## Roadmap

### Phase 2: Integration
- NSN integration bridge
- Level 5 MetaAgent bridge
- limit-benchmark crate bridge

### Phase 3: Visualization
- Energy consumption charts
- Carbon footprint dashboard
- Comparison matrix

### Phase 4: Deployment
- Hugging Face Spaces dashboard
- Public leaderboard
- API endpoints

---

**Version**: 2.4.2  
**Status**: Production Ready  
**Date**: January 19, 2026

