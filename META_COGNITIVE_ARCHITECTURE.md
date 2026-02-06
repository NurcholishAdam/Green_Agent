# Meta-Cognitive Architecture for Green Agent

## Overview

This document describes the enhanced meta-cognitive architecture that adds sustained reflection and interpretability to the Green Agent system.

## Architecture Enhancements

### 1. Strengthened Metric Collection

**Module**: `src/monitoring/metrics_collector.py`

**Features**:
- Consistent logging of all resource metrics (latency, energy, carbon, memory, tool calls)
- Real-time monitoring hooks for mid-execution access
- Cumulative metrics tracking across execution steps
- Export capabilities for post-execution analysis

**Key Methods**:
- `start_step()`: Mark beginning of execution step
- `record_tool_call()`: Track tool usage
- `collect_snapshot()`: Capture point-in-time metrics
- `get_current_metrics()`: Real-time metric access
- `get_metrics_for_reflection()`: Formatted metrics for reflection

### 2. Reflection Checkpoints

**Module**: `src/reflection/reflection_engine.py`

**Features**:
- Periodic reflection checkpoints (configurable frequency)
- Budget status analysis against policy constraints
- Self-explanation generation in natural language
- Decision determination based on current state
- Confidence scoring for agent strategies
- Pattern identification across reflections

**Reflection Process**:
1. Agent pauses at checkpoint (every N steps)
2. Reviews current metrics vs. budget
3. Generates self-explanation: "I'm exceeding latency but conserving energy"
4. Determines adaptive decision
5. Stores structured reflection log

**Example Reflection**:
```json
{
  "step": 5,
  "self_explanation": "⚡ I'm approaching budget limits with 1 warning(s). Energy usage is high at 82.3% of budget. I'm trading speed for energy efficiency.",
  "decision": "reduce_tool_calls",
  "confidence": 0.75,
  "budget_status": {
    "violations": [],
    "warnings": ["Energy high: 82.3%"],
    "utilization": {"energy": 82.3, "carbon": 65.1, "latency": 45.2}
  }
}
```

### 3. Long-Context Reasoning Backend

**Module**: `src/reflection/long_context_reasoner.py`

**Features**:
- Compare current run with past runs
- Identify patterns: "This strategy consistently trades off carbon for speed"
- Suggest adaptive strategies mid-execution
- Track performance trends over time
- Generate insights with confidence scores

**Reasoning Insights**:
- **Comparison**: Energy efficiency vs. historical average
- **Pattern**: Consistent trade-off preferences
- **Trend**: Performance improvement or degradation
- **Suggestion**: Adaptive actions based on analysis

**Example Insight**:
```json
{
  "insight_type": "pattern",
  "description": "Agent consistently prioritizes energy conservation over speed (80% of runs)",
  "confidence": 0.88,
  "suggested_action": "continue"
}
```

### 4. Self-Reflection Narratives

**Module**: `src/policy/policy_feedback.py`

**Features**:
- Dual-layer feedback system
- **Objective Layer**: Pareto analysis, efficiency scores
- **Subjective Layer**: Agent self-reflections, confidence trends
- **Synthesis**: Alignment between objective and subjective assessments
- Actionable recommendations

**Dual-Layer Feedback Structure**:
```json
{
  "objective_layer": {
    "pareto_position": "frontier",
    "efficiency_score": 0.85,
    "interpretation": "✅ This agent is on the Pareto frontier"
  },
  "subjective_layer": {
    "narrative": "✅ I'm operating within all budget constraints...",
    "avg_confidence": 0.82,
    "decision_pattern": "continue"
  },
  "synthesis": {
    "alignment": "strongly_aligned",
    "synthesis_text": "Agent's self-assessment aligns with objective performance",
    "recommendations": ["Continue current strategy"]
  }
}
```

### 5. Adaptive Policy Integration

**Module**: `src/policy/policy_engine.py`

**Features**:
- Meta-cognitive rules in policy configuration
- Reflection frequency configuration
- Self-adjustment thresholds
- Dynamic policy adjustments based on reflection outcomes
- Policy enforcement with violation tracking

**Adaptive Adjustments**:
- `reduce_tool_calls`: Decrease tool usage frequency
- `reduce_energy_usage`: Tighten energy constraints by 10%
- `optimize_speed`: Relax latency constraints by 10%
- `reduce_memory_usage`: Tighten memory constraints by 10%

### 6. Dashboard / Leaderboard Upgrade

**Module**: `src/dashboard/green_dashboard.py`

**Features**:
- Visualize reflective insights alongside metrics
- Show "why the agent chose this path"
- Compare agents on interpretability AND efficiency
- Interpretability scoring based on reflection quality
- HTML report generation with reasoning paths

**Interpretability Score Components**:
- Reflection frequency (30%)
- Average confidence (30%)
- Decision consistency (20%)
- Explanation quality (20%)

**Leaderboard Rankings**:
- **Efficiency**: Pareto position + energy usage
- **Interpretability**: Reflection quality score
- **Sustainability**: Combined energy + carbon footprint

### 7. Sustained Reflection Across Runs

**Module**: `src/memory/run_memory.py`

**Features**:
- Persistent memory across multiple runs
- Performance trend analysis
- Meta-policy generation from historical data
- Long-term pattern identification
- Historical summary statistics

**Meta-Policy Generation**:
```json
{
  "generated_at": "2025-02-06T10:30:00",
  "based_on_runs": 10,
  "recommendations": [
    {
      "metric": "energy",
      "action": "tighten_energy_budget",
      "reason": "Energy usage is increasing over time"
    },
    {
      "metric": "overall",
      "action": "continue_current_strategy",
      "reason": "Overall performance is improving"
    }
  ]
}
```

### 8. Iterative Testing Framework

**Testing Approach**:
- Compare baseline (no reflection) vs. meta-cognitive agents
- Measure improvements in:
  - Sustainability (energy, carbon reduction)
  - Interpretability (reflection quality, confidence)
  - Adaptability (policy adjustments, pattern learning)
- Use Pareto analysis to show reflection moves agents toward optimal trade-offs

## Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                    Agent Execution                       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Metrics Collector (Real-time)               │
│  • Latency, Energy, Carbon, Memory, Tool Calls          │
│  • Cumulative tracking                                   │
│  • Mid-execution access                                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Reflection Checkpoint (Every N steps)          │
│  • Budget status analysis                                │
│  • Self-explanation generation                           │
│  • Decision determination                                │
│  • Confidence scoring                                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ├──────────────────────────────────┐
                     ▼                                  ▼
┌──────────────────────────────────┐  ┌──────────────────────────────┐
│   Long-Context Reasoner          │  │   Policy Engine              │
│  • Compare with past runs        │  │  • Check thresholds          │
│  • Identify patterns             │  │  • Apply adjustments         │
│  • Suggest adaptations           │  │  • Update budgets            │
└──────────────────┬───────────────┘  └──────────────┬───────────────┘
                   │                                  │
                   └──────────────┬───────────────────┘
                                  ▼
┌─────────────────────────────────────────────────────────┐
│                  Run Memory (Persistent)                 │
│  • Store complete run history                            │
│  • Track performance trends                              │
│  • Generate meta-policies                                │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Pareto Analysis + Feedback                  │
│  • Objective metrics (Pareto position)                   │
│  • Subjective narratives (reflections)                   │
│  • Dual-layer synthesis                                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                    Dashboard Output                      │
│  • Leaderboard (efficiency + interpretability)           │
│  • Reasoning paths visualization                         │
│  • Comparative insights                                  │
└─────────────────────────────────────────────────────────┘
```

## Configuration

### Policy Configuration (`green_policy.yaml`)

```yaml
meta_cognitive:
  reflection_frequency: 5  # Reflect every 5 steps
  
  self_adjustment_thresholds:
    energy_threshold_pct: 80
    carbon_threshold_pct: 80
    latency_threshold_pct: 80
    memory_threshold_pct: 80
  
  adaptive_policy_enabled: true
  
  long_context:
    enabled: true
    history_window: 10
    min_confidence_threshold: 0.7
  
  sustained_reflection:
    enabled: true
    memory_persistence: true
    meta_policy_generation: true
```

## Usage

### Basic Execution

```bash
python run_agent.py \
  --config config.json \
  --policy green_policy.yaml \
  --output results.json \
  --dashboard dashboard.html
```

### Output Artifacts

1. **results.json**: Complete execution results with reflections
2. **dashboard.html**: Interactive visualization
3. **metrics_history.json**: Detailed metrics timeline
4. **reflections.json**: All reflection checkpoints
5. **reasoning_insights.json**: Long-context analysis
6. **pareto_analysis.json**: Multi-objective evaluation
7. **run_memory.json**: Persistent historical data

## Benefits

### 1. Sustainability
- Real-time awareness of resource usage
- Proactive budget management
- Adaptive optimization strategies

### 2. Interpretability
- Clear reasoning paths
- Self-explanations in natural language
- Confidence scoring for transparency

### 3. Adaptability
- Dynamic policy adjustments
- Learning from historical patterns
- Meta-policy generation

### 4. Accountability
- Dual-layer feedback (objective + subjective)
- Alignment verification
- Comprehensive audit trails

## Future Extensions

1. **Multi-Agent Reflection**: Collaborative reflection across agent teams
2. **Causal Analysis**: Identify causal relationships between decisions and outcomes
3. **Counterfactual Reasoning**: "What if" analysis for alternative strategies
4. **Federated Learning**: Share meta-policies across agent populations
5. **Human-in-the-Loop**: Interactive reflection with human feedback

## References

- Green Agent Repository: https://github.com/NurcholishAdam/Green_Agent
- AgentBeats Platform: https://agentbeats.ai
- Pareto Optimization: Multi-objective decision making
- Meta-Cognitive AI: Self-aware agent architectures
