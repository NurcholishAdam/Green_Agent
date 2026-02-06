# Quick Start Guide: Meta-Cognitive Green Agent

## Installation

```bash
# Clone the repository
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent

# Install dependencies
pip install -r requirements.txt
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
  ]
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

## Next Steps

1. **Run Baseline**: Execute without meta-cognitive features
2. **Run Enhanced**: Execute with all features enabled
3. **Compare**: Use Pareto analysis to compare performance
4. **Iterate**: Adjust thresholds and frequencies based on results

## Resources

- **Architecture Documentation**: `META_COGNITIVE_ARCHITECTURE.md`
- **Repository**: https://github.com/NurcholishAdam/Green_Agent
- **AgentBeats Platform**: https://agentbeats.ai

## Support

For issues or questions:
- GitHub Issues: https://github.com/NurcholishAdam/Green_Agent/issues
- Email: nurcholishadam@gmail.com
