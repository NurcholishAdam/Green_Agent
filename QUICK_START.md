
# Quick Start Guide: Meta-Cognitive Green Agent

## Installation

```bash
# Clone the repository
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent

# Install core dependencies
pip install -r requirements.txt

# (Optional) Install dependencies for advanced enhancements
pip install scikit-learn pandas pydantic pydantic-settings cryptography pyjwt prometheus-client tenacity aiofiles networkx
```

## Basic Usage

### 1. Run Demo

The easiest way to see the meta-cognitive architecture in action:

```bash
python demo_meta_cognitive.py
```

This will:
- Simulate 15 execution steps
- Trigger reflection checkpoints every 5 steps
- Generate self-explanations and decisions
- Compare with historical runs
- Identify patterns
- Generate meta-policies
- Create an interactive dashboard

**Output Files**:
- `demo_dashboard.html` - Interactive visualization
- `demo_metrics_history.json` - Detailed metrics timeline
- `demo_reflections.json` - All reflection checkpoints
- `demo_reasoning_insights.json` - Long-context analysis

### 2. Run Tests

Verify all components are working:

```bash
python test_meta_cognitive.py
```

### 3. Run with Custom Configuration

```bash
python run_agent.py \
  --config example_config.json \
  --policy green_policy.yaml \
  --output results.json \
  --dashboard dashboard.html
```

**Optionally enable advanced enhancements:**

```bash
python run_agent.py --config example_config.json --policy green_policy.yaml --enhancements
```

## Configuration

### Policy Configuration (`green_policy.yaml`)

Key meta-cognitive settings:

```yaml
meta_cognitive:
  # Reflection frequency (every N steps)
  reflection_frequency: 5
  
  # Self-adjustment thresholds (percentage of budget)
  self_adjustment_thresholds:
    energy_threshold_pct: 80
    carbon_threshold_pct: 80
    latency_threshold_pct: 80
    memory_threshold_pct: 80
  
  # Enable adaptive policy adjustments
  adaptive_policy_enabled: true
  
  # Long-context reasoning
  long_context:
    enabled: true
    history_window: 10  # Consider last 10 runs
```

### Execution Configuration (`config.json`)

```json
{
  "framework": "langchain",
  "grid_intensity": 385.0,
  "pue_factor": 1.2,
  "queries": [
    {
      "id": "query-1",
      "task": "Your task description"
    }
  ],
  "enhancements": {
    "enabled": true,
    "limit_graph": {
      "enabled": true,
      "graph_metrics": { "centrality": 0.7, "connectivity": 0.6 }
    },
    "modp": {
      "enabled": true,
      "objective_weights": [0.4, 0.3, 0.2, 0.1]
    },
    "rlhf": {
      "enabled": true,
      "human_feedback_score": 0.6
    },
    "distillation": {
      "enabled": true,
      "use_moe_gating": true
    },
    "bio_inspired": {
      "enabled": true,
      "use_evolutionary": true
    },
    "moe_expert": {
      "enabled": true,
      "n_experts": 4
    },
    "flexgen": {
      "enabled": false,
      "model_name": "facebook/opt-6.7b",
      "delegation_policy": "adaptive"
    }
  }
}
```

## Understanding the Output

### Reflection Checkpoint Example

```
🤔 REFLECTION CHECKPOINT
------------------------------------------------------------
💭 Self-Explanation:
   ⚡ I'm approaching budget limits with 1 warning(s). 
   Energy usage is high at 82.3% of budget. 
   I'm trading speed for energy efficiency.

🎯 Decision: reduce_tool_calls
📈 Confidence: 0.75
⚡ Warnings: ['Energy high: 82.3%']
```

### Dashboard Metrics

The HTML dashboard shows:
- **Leaderboard**: Rankings by efficiency, interpretability, sustainability
- **Agent Comparisons**: Side-by-side with reasoning paths
- **Pareto Positions**: Optimal vs. dominated agents
- **Insights**: Patterns and recommendations

### Artifacts Generated

1. **results.json**: Complete execution results
   - All metrics per step
   - Reflection checkpoints
   - Long-context insights
   - Meta-cognitive summary

2. **dashboard.html**: Interactive visualization
   - Leaderboard with multiple rankings
   - Reasoning path visualization
   - Comparative insights

3. **metrics_history.json**: Detailed timeline
   - Per-step snapshots
   - Cumulative metrics

4. **reflections.json**: All reflections
   - Self-explanations
   - Decisions and confidence
   - Budget status

5. **reasoning_insights.json**: Long-context analysis
   - Comparisons with past runs
   - Pattern identification
   - Suggested adaptations

6. **run_memory.json**: Persistent history
   - All historical runs
   - Meta-policies generated

## Key Features

### 1. Real-Time Monitoring

Metrics are collected continuously and accessible mid-execution:

```python
from src.monitoring.metrics_collector import MetricsCollector

collector = MetricsCollector()
collector.start_step()
# ... do work ...
current_metrics = collector.get_current_metrics()
```

### 2. Reflection Checkpoints

Agent pauses periodically to self-reflect:

```python
from src.reflection.reflection_engine import ReflectionEngine

engine = ReflectionEngine(reflection_frequency=5)
if engine.should_reflect(step):
    reflection = engine.generate_reflection(step, metrics, timestamp)
    print(reflection.self_explanation)
```

### 3. Long-Context Reasoning

Compare current run with historical data:

```python
from src.reflection.long_context_reasoner import LongContextReasoner

reasoner = LongContextReasoner()
insights = reasoner.compare_with_past_runs(current_metrics)
for insight in insights:
    print(f"{insight.description} (confidence: {insight.confidence})")
```

### 4. Adaptive Policy

Policies adjust dynamically based on reflections:

```python
from src.policy.policy_engine import PolicyEngine

policy = PolicyEngine(policy_file="green_policy.yaml")
if policy.should_self_adjust(metrics):
    adjustment = policy.apply_adaptive_adjustment(decision)
```

### 5. Dual-Layer Feedback

Combines objective metrics with subjective reasoning:

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()
dual_feedback = feedback.generate_dual_layer_feedback(
    pareto_analysis, reflections, metrics
)
print(dual_feedback['synthesis']['synthesis_text'])
```

## Advanced Usage

### Custom Reflection Frequency

Adjust how often the agent reflects:

```yaml
meta_cognitive:
  reflection_frequency: 3  # Reflect every 3 steps
```

### Custom Thresholds

Set when self-adjustment triggers:

```yaml
meta_cognitive:
  self_adjustment_thresholds:
    energy_threshold_pct: 70  # More aggressive
    carbon_threshold_pct: 90  # More lenient
```

### Disable Adaptive Policy

For baseline comparison:

```yaml
meta_cognitive:
  adaptive_policy_enabled: false
```

### Historical Window Size

Control how many past runs to consider:

```yaml
meta_cognitive:
  long_context:
    history_window: 20  # Consider last 20 runs
```

### Integrating LIMIT Graph, MODP, RLHF, Distillation, MoE, Evolutionary, FlexGen

The enhancements folder (`src/enhancements`) provides additional modules that can be enabled to improve decision‑making and sustainability. When enabled, the agent can:

- **Select routing strategies** using a learned distillation policy with MoE gating.
- **Choose task priority** based on MODP (Multi‑Objective Decision Process) weights.
- **Incorporate human feedback** (RLHF) into decisions.
- **Use LIMIT Graph metrics** (centrality, connectivity) for context awareness.
- **Evolve parameters** via bio‑inspired optimisation.
- **Delegate LLM inference** to FlexGen with adaptive precision.

#### Enabling Enhancements

Set `ENHANCEMENTS_ENABLED=true` in your environment or include the `enhancements` block in `config.json`. The agent will automatically initialize the advanced modules.

#### Example: Using NodeDescriptor and WorkloadDescriptor

```python
import asyncio
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency

async def main():
    node = NodeDescriptor(
        id="node1",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=350.0,
        energy_per_token=0.00004,
        use_evolutionary=True,
        human_feedback_score=0.7,
        graph_metrics={"centrality": 0.8, "connectivity": 0.6},
    )
    strategy = await node.select_routing_strategy()
    print(f"Selected routing strategy: {strategy}")

    workload = WorkloadDescriptor(
        task_id="task1",
        task_type=TaskType.INFERENCE,
        tokens=1000,
        latency_target=300.0,
        urgency=Urgency.MEDIUM,
        use_evolutionary=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7},
        metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2}
    )
    priority = await workload.select_priority()
    print(f"Selected priority: {priority}")

asyncio.run(main())
```

#### FlexGen Integration

If FlexGen is available and enabled, the agent can delegate LLM tasks to FlexGen for high‑throughput inference. Set `flexgen.enabled` to `true` and adjust `delegation_policy`. The decision to delegate is made by the distillation policy based on context (carbon intensity, task complexity, etc.).

```yaml
# In config.json
flexgen:
  enabled: true
  model_name: "facebook/opt-6.7b"
  batch_size: 16
  default_precision: "fp16"
  delegation_policy: "adaptive"   # adaptive | always | never
```

## Troubleshooting

### Issue: No reflections generated

**Solution**: Check reflection frequency. If steps < frequency, no reflections occur.

```yaml
meta_cognitive:
  reflection_frequency: 5  # Ensure steps >= 5
```

### Issue: Memory file not found

**Solution**: Run will create memory file automatically on first execution.

### Issue: Dashboard not showing data

**Solution**: Ensure at least one complete run has finished.

### Issue: Enhancements not activating

**Solution**: Verify:
- The `src/enhancements` folder exists.
- Required dependencies installed (see Installation).
- `enhancements.enabled` is `true` in config or `ENHANCEMENTS_ENABLED=true` env var set.
- The modules are importable (run `python -c "from src.enhancements.schemas.node_descriptor import NodeDescriptor"`).

## Next Steps

1. **Run Baseline**: Execute without meta-cognitive features
2. **Run Enhanced**: Execute with all features enabled
3. **Compare**: Use Pareto analysis to compare performance
4. **Iterate**: Adjust thresholds and frequencies based on results
5. **Enable Advanced Modules**: Turn on LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen for improved sustainability and decision quality.

## Resources

- **Architecture Documentation**: `META_COGNITIVE_ARCHITECTURE.md`
- **Repository**: https://github.com/NurcholishAdam/Green_Agent
- **AgentBeats Platform**: https://agentbeats.ai
- **Enhancements Documentation**: `src/enhancements/README.md`

## Support

For issues or questions:
- GitHub Issues: https://github.com/NurcholishAdam/Green_Agent/issues
- Email: nurcholishadam@gmail.com
```
