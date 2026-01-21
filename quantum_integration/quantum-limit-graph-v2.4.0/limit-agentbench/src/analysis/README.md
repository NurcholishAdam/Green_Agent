# Analysis Module

Multi-objective analysis tools for Green_Agent, including Pareto frontier analysis and task complexity normalization.

## 📦 Installation

```bash
# No additional dependencies beyond Green_Agent core
# Already included: numpy, logging
```

## 🚀 Quick Start

### Basic Pareto Analysis

```python
from analysis import ParetoPoint, ParetoFrontierAnalyzer

# Create agents with different trade-offs
agents = [
    ParetoPoint('gpt4', accuracy=0.95, energy_kwh=0.005, carbon_co2e_kg=0.001, latency_ms=200),
    ParetoPoint('llama3', accuracy=0.88, energy_kwh=0.002, carbon_co2e_kg=0.0004, latency_ms=120)
]

# Compute Pareto frontier
analyzer = ParetoFrontierAnalyzer()
frontier = analyzer.compute_frontier(agents)

# Find best balanced agent
knee = analyzer.get_knee_point(frontier)
print(f"Recommended: {knee.agent_id}")
```

### Complexity Analysis

```python
from analysis import ComplexityAnalyzer

# Analyze execution trace
trace = {
    'prompt': "Classify this image...",
    'reasoning': ["Step 1", "Step 2", "Step 3"],
    'tool_calls': [{'tool': 'vision_api'}],
    'execution_time_ms': 1500,
    'context_tokens': 512
}

analyzer = ComplexityAnalyzer()
complexity = analyzer.analyze_from_trace(trace)

print(f"Complexity tier: {analyzer.categorize_complexity(complexity)}")
print(f"Complexity score: {complexity.compute_composite_score():.2f}")
```

## 📚 API Reference

### ParetoPoint

Represents an agent's performance across multiple objectives.

**Attributes:**
- `agent_id` (str): Unique agent identifier
- `accuracy` (float): Task accuracy [0.0, 1.0]
- `energy_kwh` (float): Energy consumed (kWh)
- `carbon_co2e_kg` (float): Carbon emissions (kg CO₂e)
- `latency_ms` (float): Task latency (milliseconds)

**Methods:**
- `dominates(other)`: Check if this agent Pareto-dominates another
- `to_dict()`: Serialize to dictionary
- `from_dict(data)`: Deserialize from dictionary

### ParetoFrontierAnalyzer

Analyzes agents using Pareto optimality.

**Methods:**

#### `compute_frontier(agents: List[ParetoPoint]) -> List[ParetoPoint]`
Compute the Pareto frontier (non-dominated solutions).

**Returns:** List of agents on the frontier

**Example:**
```python
frontier = analyzer.compute_frontier(agents)
print(f"Found {len(frontier)} Pareto-optimal agents")
```

#### `rank_by_dominance(agents: List[ParetoPoint]) -> Dict[int, List[ParetoPoint]]`
Rank agents by dominance layers.

**Returns:** Dictionary mapping rank → agents
- Rank 0: Pareto frontier
- Rank 1: Dominated only by rank 0
- Rank 2: Dominated by ranks 0-1, etc.

**Example:**
```python
ranks = analyzer.rank_by_dominance(agents)
for rank, rank_agents in ranks.items():
    print(f"Rank {rank}: {[a.agent_id for a in rank_agents]}")
```

#### `get_knee_point(frontier: List[ParetoPoint]) -> ParetoPoint`
Find the "knee point" - best overall balance.

**Returns:** ParetoPoint with best compromise

**Example:**
```python
knee = analyzer.get_knee_point(frontier)
print(f"Best balanced agent: {knee.agent_id}")
```

#### `compare_agents(a: ParetoPoint, b: ParetoPoint) -> Dict`
Compare two agents and explain relationship.

**Returns:** Dictionary with:
- `relationship`: 'dominates', 'dominated_by', or 'non_comparable'
- `explanation`: Human-readable explanation
- `trade_offs`: Detailed trade-off analysis (if non-comparable)

**Example:**
```python
comparison = analyzer.compare_agents(agent_a, agent_b)
print(comparison['explanation'])
```

### TaskComplexity

Multi-dimensional task complexity measurement.

**Attributes:**
- `prompt_length` (int): Input tokens
- `reasoning_steps` (int): Number of reasoning steps
- `tool_calls` (int): External tool invocations
- `wall_clock_ms` (float): Execution time
- `context_size` (int): Total context window

**Methods:**
- `compute_composite_score(weights)`: Calculate weighted complexity score

### ComplexityAnalyzer

Analyzes task complexity from execution traces.

**Methods:**

#### `analyze_from_trace(trace: Dict) -> TaskComplexity`
Extract complexity metrics from execution trace.

**Trace Format:**
```python
{
    'prompt': str,
    'reasoning': List[str],
    'tool_calls': List[Dict],
    'execution_time_ms': float,
    'context_tokens': int
}
```

**Example:**
```python
complexity = analyzer.analyze_from_trace(trace)
print(f"Reasoning steps: {complexity.reasoning_steps}")
```

#### `categorize_complexity(complexity: TaskComplexity) -> str`
Categorize task into tiers: 'trivial', 'simple', 'moderate', 'complex', 'extreme'

**Example:**
```python
tier = analyzer.categorize_complexity(complexity)
print(f"Task difficulty: {tier}")
```

#### `detect_over_reasoning(complexity: TaskComplexity, threshold: float) -> Dict`
Detect if agent uses excessive reasoning steps.

**Returns:**
```python
{
    'over_reasoning': bool,
    'ratio': float,
    'recommendation': str
}
```

**Example:**
```python
result = analyzer.detect_over_reasoning(complexity)
if result['over_reasoning']:
    print(f"⚠️ {result['recommendation']}")
```

#### `suggest_optimization(complexity: TaskComplexity) -> List[str]`
Generate optimization suggestions.

**Returns:** List of actionable recommendations

**Example:**
```python
suggestions = analyzer.suggest_optimization(complexity)
for suggestion in suggestions:
    print(f"💡 {suggestion}")
```

## 🎯 Use Cases

### 1. Multi-Objective Agent Comparison

Replace single sustainability scores with Pareto analysis:

```python
# Traditional approach (single score)
agents_ranked = sorted(agents, key=lambda a: a.sustainability_index)

# Pareto approach (multi-objective)
analyzer = ParetoFrontierAnalyzer()
frontier = analyzer.compute_frontier(agents)
# All frontier agents are equally valid choices with different trade-offs
```

### 2. Fair Cross-Complexity Comparison

Normalize metrics by task complexity:

```python
# Without normalization: simple tasks look "efficient", complex tasks look "wasteful"
# With normalization: fair comparison

complexity_a = analyzer.analyze_from_trace(trace_a)
complexity_b = analyzer.analyze_from_trace(trace_b)

energy_eff_a = agent_a.energy_kwh / complexity_a.compute_composite_score()
energy_eff_b = agent_b.energy_kwh / complexity_b.compute_composite_score()

# Now fairly comparable even if tasks have different complexities
```

### 3. Over-Reasoning Detection

Identify agents wasting computation:

```python
for agent, trace in agent_traces.items():
    complexity = analyzer.analyze_from_trace(trace)
    result = analyzer.detect_over_reasoning(complexity)
    
    if result['over_reasoning']:
        print(f"⚠️ {agent}: {result['recommendation']}")
```

### 4. Cinebench Classifier Evaluation

Compare ML models for Cinebench classification:

```python
# Evaluate ResNet, EfficientNet, MobileNet
classifiers = [
    ParetoPoint('ResNet50', 0.94, 0.008, 0.0016, 350),
    ParetoPoint('EfficientNet', 0.92, 0.003, 0.0006, 180),
    ParetoPoint('MobileNet', 0.86, 0.001, 0.0002, 80)
]

frontier = analyzer.compute_frontier(classifiers)
knee = analyzer.get_knee_point(frontier)

print(f"Deploy: {knee.agent_id} (best accuracy/energy balance)")
```

## 📊 Visualization

The Pareto module is designed to work with visualization libraries:

```python
import matplotlib.pyplot as plt

# 2D Pareto plot
frontier_ids = [p.agent_id for p in frontier]
for agent in agents:
    color = 'green' if agent.agent_id in frontier_ids else 'gray'
    plt.scatter(agent.energy_kwh, agent.accuracy, c=color, label=agent.agent_id)

plt.xlabel('Energy (kWh)')
plt.ylabel('Accuracy')
plt.title('Pareto Frontier: Accuracy vs Energy')
plt.legend()
plt.show()
```

## 🧪 Testing

Run the test suite:

```bash
# All tests
pytest tests/test_pareto_analysis.py -v

# Specific test class
pytest tests/test_pareto_analysis.py::TestParetoFrontierAnalyzer -v

# With coverage
pytest tests/test_pareto_analysis.py --cov=analysis
```

## 📖 Examples

See `examples/demo_pareto_analysis.py` for comprehensive examples:

```bash
python examples/demo_pareto_analysis.py
```

This runs 6 demos:
1. Basic Pareto frontier
2. Dominance ranking
3. Agent-to-agent comparison
4. Complexity analysis
5. Over-reasoning detection
6. Cinebench integration

## 🔗 Integration with Green_Agent

### With Existing Green Metrics

```python
from core.green_metrics import GreenMetricsTracker
from analysis import ParetoPoint, ParetoFrontierAnalyzer

# Collect metrics
tracker = GreenMetricsTracker()
# ... execute agent ...
metrics = tracker.get_metrics()

# Create Pareto point
point = ParetoPoint(
    agent_id=agent.name,
    accuracy=metrics['accuracy'],
    energy_kwh=metrics['energy_kwh'],
    carbon_co2e_kg=metrics['carbon_co2e_kg'],
    latency_ms=metrics['latency_ms']
)

# Add to frontier analysis
analyzer = ParetoFrontierAnalyzer()
frontier = analyzer.compute_frontier([point] + existing_points)
```

### With AgentBeats A2A

```python
# In your green agent's evaluation pipeline
async def evaluate_purple_agents(purple_agents):
    pareto_points = []
    
    for agent_url in purple_agents:
        # Send task via A2A
        result = await a2a_handler.send_task(agent_url, task)
        
        # Create Pareto point from result
        point = ParetoPoint(
            agent_id=result['agent_id'],
            accuracy=result['metrics']['accuracy'],
            energy_kwh=result['metrics']['energy_kwh'],
            carbon_co2e_kg=result['metrics']['carbon_co2e_kg'],
            latency_ms=result['metrics']['latency_ms']
        )
        pareto_points.append(point)
    
    # Compute Pareto frontier
    analyzer = ParetoFrontierAnalyzer()
    frontier = analyzer.compute_frontier(pareto_points)
    
    return {
        'frontier': [p.to_dict() for p in frontier],
        'knee_point': analyzer.get_knee_point(frontier).to_dict(),
        'rankings': analyzer.rank_by_dominance(pareto_points)
    }
```

## 💡 Best Practices

### 1. Always Use Pareto for Multi-Objective Decisions

❌ **Don't:**
```python
# Single weighted score hides trade-offs
score = 0.4*accuracy + 0.3*energy + 0.3*carbon
best = max(agents, key=lambda a: score)
```

✅ **Do:**
```python
# Pareto reveals all valid options
frontier = analyzer.compute_frontier(agents)
knee = analyzer.get_knee_point(frontier)  # If you need single choice
```

### 2. Normalize Before Comparing

❌ **Don't:**
```python
# Unfair: compares simple and complex tasks directly
best_energy = min(agents, key=lambda a: a.energy_kwh)
```

✅ **Do:**
```python
# Fair: normalize by complexity
complexities = {a: analyzer.analyze_from_trace(a.trace) for a in agents}
best_eff = min(agents, key=lambda a: 
    a.energy_kwh / complexities[a].compute_composite_score()
)
```

### 3. Check for Over-Reasoning

```python
# Always check if agents are wasting computation
for agent in agents:
    complexity = analyzer.analyze_from_trace(agent.trace)
    result = analyzer.detect_over_reasoning(complexity)
    
    if result['over_reasoning']:
        logger.warning(f"{agent.id}: {result['recommendation']}")
```

## 🐛 Troubleshooting

**Issue:** Empty Pareto frontier
```python
# Check if all agents dominate each other (impossible) or data is invalid
if not frontier:
    logger.error("No agents on frontier - check data validity")
```

**Issue:** All agents on frontier
```python
# This happens when no agent dominates any other
# It's valid but suggests agents are very different or data is sparse
if len(frontier) == len(agents):
    logger.info("All agents non-comparable - significant trade-offs exist")
```

**Issue:** Complexity score too low/high
```python
# Adjust weights for your domain
custom_weights = {
    'prompt_length': 0.3,  # Emphasize input size
    'reasoning_steps': 0.4,  # Emphasize computation
    'tool_calls': 0.1,
    'wall_clock_ms': 0.1,
    'context_size': 0.1
}
score = complexity.compute_composite_score(custom_weights)
```

## 📄 License

Part of Green_Agent - Apache 2.0 License

## 🤝 Contributing

See main Green_Agent CONTRIBUTING.md

## 📧 Support

- Issues: GitHub Issues
- Docs: This README + inline docstrings
- Examples: `examples/demo_pareto_analysis.py`
