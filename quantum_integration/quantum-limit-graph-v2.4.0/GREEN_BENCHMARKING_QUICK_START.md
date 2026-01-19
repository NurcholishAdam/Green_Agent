# Green Agent Benchmarking - Quick Start Guide

**Version**: 2.4.2  
**Date**: January 19, 2026  
**Module**: limit-agentbench

## What is LIMIT-AgentBench?

LIMIT-AgentBench transforms Quantum LIMIT-GRAPH into a comprehensive **green agent benchmarking platform** that:

- ✅ Evaluates AI agents with **AgentBench protocol** compatibility
- 🌱 Tracks **energy consumption** and **carbon footprint**
- 🔌 Supports **multiple frameworks** (LangChain, AutoGen, CrewAI, LIMIT-GRAPH)
- 📊 Provides **unified green leaderboard** with sustainability rankings
- 🎯 Calculates **sustainability index** combining performance and environmental impact

## Installation

### 1. Navigate to Module

```bash
cd quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench
```

### 2. Install Dependencies

```bash
pip install psutil numpy
```

### 3. Verify Installation

```bash
python test_installation.py
```

Expected output:
```
✓ Core modules imported successfully
✓ Adapter modules imported successfully
✓ Metrics modules imported successfully
✓ Dashboard modules imported successfully
✓ Main package imported (version: 2.4.2)
✓ Installation verified successfully!
```

## Quick Start Examples

### Example 1: Basic Agent Evaluation

```python
from limit_agentbench import AgentBenchAdapter

# Create adapter
adapter = AgentBenchAdapter()

# Create task
task = adapter.create_task(
    task_id="qa_001",
    suite="question_answering",
    task_type="qa",
    input_data={"question": "What is AI?"},
    expected_output={"answer": "Artificial Intelligence"}
)

# Evaluate your agent
result = adapter.evaluate_agent(
    agent=my_agent,
    task=task,
    track_energy=True,
    track_carbon=True
)

# View results
print(f"Accuracy: {result['metrics']['accuracy']:.2%}")
print(f"Energy: {result['metrics']['energy_kwh']:.6f} kWh")
print(f"Carbon: {result['metrics']['carbon_co2e_kg']:.6f} kg CO2e")
print(f"Sustainability Index: {result['metrics']['sustainability_index']:.2f}")
```

### Example 2: Green Metrics Tracking

```python
from limit_agentbench import GreenMetricsTracker

# Initialize tracker
tracker = GreenMetricsTracker(
    grid_region="US-CA",           # California grid
    hardware_profile="nvidia_a100", # A100 GPU
    track_energy=True,
    track_carbon=True
)

# Track execution
with tracker:
    # Your agent execution here
    result = my_agent.run(task)

# Get metrics
metrics = tracker.get_metrics()
print(f"Energy consumed: {metrics['energy_kwh']:.6f} kWh")
print(f"Carbon emitted: {metrics['carbon_co2e_kg']:.6f} kg CO2e")
```

### Example 3: Multi-Framework Comparison

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
for i, rank in enumerate(comparison['rankings'], 1):
    print(f"{i}. {rank['agent_name']}: SI={rank['aggregated_metrics']['avg_sustainability_index']:.2f}")
```

### Example 4: Leaderboard Submission

```python
from limit_agentbench import GreenLeaderboard

# Initialize leaderboard
leaderboard = GreenLeaderboard()

# Submit your result
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

for entry in top_agents:
    print(f"{entry['rank']}. {entry['agent_name']} - SI: {entry['metrics']['sustainability_index']:.2f}")
```

### Example 5: Sustainability Index

```python
from limit_agentbench.metrics import SustainabilityIndex

# Initialize calculator
si_calc = SustainabilityIndex()

# Calculate for your agent
si = si_calc.calculate(
    accuracy=0.95,
    energy_kwh=0.003,
    carbon_co2e_kg=0.0006
)

rating = SustainabilityIndex.get_rating(si)
print(f"Sustainability Index: {si:.2f} ({rating})")

# Compare two agents
comparison = si_calc.compare_agents(
    agent_a_metrics={"accuracy": 0.95, "energy_kwh": 0.003, "carbon_co2e_kg": 0.0006},
    agent_b_metrics={"accuracy": 0.92, "energy_kwh": 0.002, "carbon_co2e_kg": 0.0004}
)

print(f"Winner: {comparison['winner']}")
print(f"Improvement: {comparison['improvement_percent']:.2f}%")
```

## Running the Demo

```bash
python demo_green_benchmark.py
```

The demo demonstrates:
1. **AgentBench Protocol**: Task creation and evaluation
2. **Green Metrics**: Energy and carbon tracking
3. **Multi-Framework**: Adapter usage
4. **Sustainability Index**: Ranking calculation
5. **Green Leaderboard**: Submission and rankings
6. **Benchmark Harness**: Full benchmark execution

## Supported Frameworks

### LangChain
```python
from limit_agentbench.adapters import LangChainAdapter

adapter = LangChainAdapter(my_langchain_agent)
result = adapter.run(task_input)
```

### AutoGen
```python
from limit_agentbench.adapters import AutoGenAdapter

adapter = AutoGenAdapter(my_autogen_agent)
result = adapter.run(task_input)
```

### CrewAI
```python
from limit_agentbench.adapters import CrewAIAdapter

adapter = CrewAIAdapter(my_crewai_agent)
result = adapter.run(task_input)
```

### LIMIT-GRAPH
```python
from limit_agentbench.adapters import LimitGraphAdapter

adapter = LimitGraphAdapter(my_limit_graph_agent)
result = adapter.run(task_input)
```

## Key Metrics

### Performance Metrics
- **Accuracy**: Task performance (0-1)
- **Latency**: Execution time (ms)
- **Success Rate**: Percentage of successful executions

### Green Metrics
- **Energy**: Consumption in kWh
- **Power**: Average and peak watts
- **Carbon**: CO2e emissions in kg
- **Efficiency**: Performance per watt
- **Sustainability Index**: Composite green score (higher is better)

## Grid Regions

Choose your grid region for accurate carbon calculations:

| Region | Code | Carbon Intensity (kg CO2e/kWh) |
|--------|------|-------------------------------|
| California | US-CA | 0.2 |
| Texas | US-TX | 0.4 |
| New York | US-NY | 0.25 |
| France | EU-FR | 0.05 |
| Germany | EU-DE | 0.35 |
| Norway | EU-NO | 0.02 |
| China | CN | 0.6 |
| India | IN | 0.7 |
| Global Average | GLOBAL | 0.475 |

## Hardware Profiles

Choose your hardware profile for accurate power estimation:

| Hardware | Code | Power (Watts) |
|----------|------|---------------|
| NVIDIA A100 | nvidia_a100 | 400 |
| NVIDIA V100 | nvidia_v100 | 300 |
| NVIDIA T4 | nvidia_t4 | 70 |
| NVIDIA RTX 3090 | nvidia_rtx_3090 | 350 |
| NVIDIA RTX 4090 | nvidia_rtx_4090 | 450 |
| AMD MI250 | amd_mi250 | 500 |
| Google TPU v4 | google_tpu_v4 | 200 |

## Understanding Sustainability Index

The **Sustainability Index** combines:
- **Accuracy** (40% weight): How well the agent performs
- **Efficiency** (30% weight): Performance per unit of energy
- **Carbon** (30% weight): Environmental impact

**Formula**: `(accuracy × 0.4 + efficiency × 0.3) / (carbon × 0.3)`

**Ratings**:
- **Excellent**: SI ≥ 200
- **Very Good**: SI ≥ 150
- **Good**: SI ≥ 100
- **Fair**: SI ≥ 50
- **Poor**: SI < 50

## Common Use Cases

### 1. Benchmark Your Agent
```python
from limit_agentbench import BenchmarkHarness

harness = BenchmarkHarness(
    output_dir="./my_results",
    grid_region="US-CA"
)

result = harness.run_benchmark(
    agent=my_agent,
    task_suite=tasks,
    benchmark_name="my_benchmark"
)
```

### 2. Compare Multiple Agents
```python
from limit_agentbench import AgentEvaluator

evaluator = AgentEvaluator(track_green_metrics=True)

comparison = evaluator.compare_agents(
    agents=[agent1, agent2, agent3],
    tasks=task_suite,
    sort_by="sustainability_index"
)
```

### 3. Track Carbon Savings
```python
from limit_agentbench.metrics import CarbonCalculator

calculator = CarbonCalculator(grid_region="US-CA")

savings = calculator.calculate_savings(
    baseline_energy_kwh=0.010,
    optimized_energy_kwh=0.003
)

print(f"Carbon saved: {savings['carbon_saved_kg']:.6f} kg CO2e")
print(f"Reduction: {savings['reduction_percent']:.2f}%")
print(f"Equivalent to {savings['trees_equivalent']:.2f} trees")
```

### 4. Optimize for Sustainability
```python
from limit_agentbench.metrics import SustainabilityIndex

si_calc = SustainabilityIndex()

# Test different configurations
configs = {
    "config_a": {"accuracy": 0.95, "energy_kwh": 0.005, "carbon_co2e_kg": 0.001},
    "config_b": {"accuracy": 0.93, "energy_kwh": 0.002, "carbon_co2e_kg": 0.0004},
}

rankings = si_calc.rank_agents(configs)
best_config = rankings[0][0]
print(f"Best configuration: {best_config}")
```

## Next Steps

1. **Read Full Documentation**: `cat GREEN_AGENT_BENCHMARKING_COMPLETE.md`
2. **Explore API**: `cat README.md`
3. **Run Demo**: `python demo_green_benchmark.py`
4. **Integrate with Your Agent**: Use the examples above
5. **Submit to Leaderboard**: Share your results

## Troubleshooting

### Import Errors
```bash
# Verify installation
python test_installation.py

# Install missing dependencies
pip install psutil numpy
```

### Permission Errors
```bash
# Ensure write permissions for output directories
chmod -R 755 ./benchmark_results
chmod -R 755 ./leaderboard_data
```

### Accuracy Issues
- Ensure `expected_output` matches your agent's output format
- Check that your agent returns the expected data structure
- Use custom accuracy calculation if needed

## Support

- **Documentation**: See `GREEN_AGENT_BENCHMARKING_COMPLETE.md`
- **API Reference**: See `README.md`
- **Demo**: Run `python demo_green_benchmark.py`
- **Test**: Run `python test_installation.py`

## What Makes This Unique?

### vs. AgentBench
- ✅ **Green metrics** (energy, carbon) - Industry first
- ✅ **Quantum evaluation** - Unique capability
- ✅ **Provenance tracking** - Full audit trail

### vs. Other Platforms
- ✅ **Multi-framework** - LangChain, AutoGen, CrewAI, LIMIT-GRAPH
- ✅ **Sustainability focus** - Environmental impact tracking
- ✅ **Unified leaderboard** - Cross-framework rankings

---

**Ready to benchmark your agent with green metrics?**

Start with: `python demo_green_benchmark.py`

---

**Version**: 2.4.2  
**Date**: January 19, 2026  
**Status**: Production Ready
