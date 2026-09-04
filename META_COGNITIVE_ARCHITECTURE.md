
# Meta-Cognitive Architecture for Green Agent

## Overview

This document describes the enhanced meta-cognitive architecture that adds sustained reflection and interpretability to the Green Agent system.

**New in this version:** The architecture now also integrates advanced enhancement modules from `src/enhancements/` – **LIMIT Graph**, **MODP (Multi‑Objective Decision Process)**, **RLHF (Reinforcement Learning from Human Feedback)**, **Multi‑Teacher On‑Policy Distillation**, **Bio‑inspired Optimisation**, **MoE expert gating**, and **FlexGen execution backend**. These modules further strengthen decision‑making, sustainability, and security, and can be used alongside the meta‑cognitive features.

---

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

---

## Advanced Enhancement Modules Integration (NEW)

The following advanced modules are now integrated into the meta‑cognitive architecture, providing context‑aware, learned decision‑making and enhanced sustainability enforcement.

### LIMIT Graph

**What it provides**: Topology‑aware metrics (centrality, connectivity) that indicate the system’s importance within the overall graph. These metrics are used to influence routing and priority decisions.

**Integration point**: `src/enhancements/core/graph_registry.py` and `causal_graph.py` maintain the graph state. The metrics are made available to the reflection engine and policy feedback, enabling rules like:

```yaml
- id: "ENH-GRAPH-001"
  condition: "graph_centrality < 0.3"
  action: "check_topology"
```

### MODP (Multi‑Objective Decision Process)

**What it provides**: Configurable weights for accuracy, energy, latency, and carbon. The composite `modp_score` is used to evaluate overall performance and trigger policy reviews when low.

**Integration point**: `NodeDescriptor` and `WorkloadDescriptor` compute the MODP score using distillation. The score is included in the metrics collected at each step, so reflection checkpoints can reference it:

```yaml
- id: "ENH-MODP-001"
  condition: "modp_score < 0.4"
  action: "trigger_policy_review"
```

### RLHF (Reinforcement Learning from Human Feedback)

**What it provides**: Human feedback score (0‑1) that influences teacher predictions and reward shaping. This feedback can be used to adapt policies based on user satisfaction.

**Integration point**: `RLHFTeacher` in descriptors; the `human_feedback_score` is stored in `FeedbackEvent` and made available for symbolic rules.

### Multi‑Teacher On‑Policy Distillation with MoE Gating

**What it provides**: A lightweight student policy trained online from rule‑based, historical ML, Q‑learning, and RLHF teachers, blended via a gating network.

**Integration point**: `DistillationRoutingOptimizer` and `DistillationPriorityOptimizer` are used by `NodeDescriptor` and `WorkloadDescriptor` respectively. During reflection checkpoints, if the current strategy is suboptimal, the agent can call these optimizers to select a better routing strategy or priority.

### Bio‑inspired Optimisation (Evolutionary)

**What it provides**: Genetic algorithms that tune MODP weights and other hyperparameters over time.

**Integration point**: `EvolutionaryOptimizer` classes in descriptors. When enabled, the best fitness is tracked and can be used in rules to detect stagnation:

```yaml
- id: "ENH-EVOL-001"
  condition: "evolutionary_best_fitness - evolutionary_best_fitness offset 1h < 0.01"
  action: "restart_evolution"
```

### MoE Expert Gating

**What it provides**: Dynamic weighting of expert predictions; its stability (`moe_gate_stddev`) is monitored.

**Integration point**: The gating network is part of the distillation optimizers. The `moe_gate_stddev` metric is exported and can trigger alerts if the routing becomes unstable.

### FlexGen Integration

**What it provides**: High‑throughput LLM inference with adaptive precision (fp32/fp16/int8) for efficient execution.

**Integration point**: When `flexgen.enabled` is `true`, the agent may delegate tasks to FlexGen. The decision is made by the distillation policy based on context (carbon intensity, task complexity). FlexGen energy consumption is tracked and can be used in rules:

```yaml
- id: "ENH-FLEX-001"
  condition: "flexgen_energy_joules > 1000"
  action: "switch_precision"
```

---

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
│  • Enhanced metrics: modp_score, graph_centrality,      │
│    human_feedback_score, distillation stats, etc.       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Reflection Checkpoint (Every N steps)          │
│  • Budget status analysis                                │
│  • Self-explanation generation                           │
│  • Decision determination                                │
│  • Confidence scoring                                    │
│  • Evaluate enhanced symbolic rules                      │
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
│           Advanced Enhancement Modules (optional)       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ LIMIT Graph  │  │     MODP     │  │     RLHF     │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Distillation │  │ Bio‑inspired │  │     MoE      │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐                                      │
│  │   FlexGen    │                                      │
│  └──────────────┘                                      │
└────────────────────┬────────────────────────────────────┘
                     │
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
│  • Advanced metrics (MODP, RLHF) incorporated           │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                    Dashboard Output                      │
│  • Leaderboard (efficiency + interpretability)           │
│  • Reasoning paths visualization                         │
│  • Comparative insights                                  │
│  • Enhanced metrics overlay                              │
└─────────────────────────────────────────────────────────┘
```

---

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

### Enhanced Configuration (`config.json` or `green_policy.yaml` additions)

```yaml
enhancements:
  enabled: true                # Master switch
  limit_graph:
    enabled: true
    graph_metrics:
      centrality: 0.7
      connectivity: 0.6
  modp:
    enabled: true
    objective_weights: [0.4, 0.3, 0.2, 0.1]
  rlhf:
    enabled: true
    human_feedback_score: 0.6
  distillation:
    enabled: true
    use_moe_gating: true
  bio_inspired:
    enabled: true
    use_evolutionary: true
  moe_expert:
    enabled: true
    n_experts: 4
  flexgen:
    enabled: false   # set true to use FlexGen
    model_name: "facebook/opt-6.7b"
    default_precision: "fp16"
```

---

## Usage

### Basic Execution

```bash
python run_agent.py \
  --config config.json \
  --policy green_policy.yaml \
  --output results.json \
  --dashboard dashboard.html
```

### Execution with Advanced Enhancements

```bash
python run_agent.py --config config.json --policy green_policy.yaml --enhancements
```

### Output Artifacts

1. **results.json**: Complete execution results with reflections
2. **dashboard.html**: Interactive visualization
3. **metrics_history.json**: Detailed metrics timeline
4. **reflections.json**: All reflection checkpoints
5. **reasoning_insights.json**: Long-context analysis
6. **pareto_analysis.json**: Multi-objective evaluation
7. **run_memory.json**: Persistent historical data
8. **Enhanced outputs** – if enhancements enabled, additional fields (MODP score, graph metrics, RLHF feedback) are included in these files.

---

## Benefits

### 1. Sustainability
- Real-time awareness of resource usage
- Proactive budget management
- Adaptive optimization strategies
- Additional gains from MODP‑driven multi‑objective optimisation

### 2. Interpretability
- Clear reasoning paths
- Self-explanations in natural language
- Confidence scoring for transparency
- Enhanced by distillation/MoE providing traceable expert decisions

### 3. Adaptability
- Dynamic policy adjustments
- Learning from historical patterns
- Meta-policy generation
- RLHF and evolutionary optimisation allow continuous improvement

### 4. Accountability
- Dual-layer feedback (objective + subjective)
- Alignment verification
- Comprehensive audit trails
- DAG carbon ledger and Zero Trust provide immutable provenance

---

## Future Extensions

1. **Multi-Agent Reflection**: Collaborative reflection across agent teams
2. **Causal Analysis**: Identify causal relationships between decisions and outcomes
3. **Counterfactual Reasoning**: "What if" analysis for alternative strategies
4. **Federated Learning**: Share meta-policies across agent populations
5. **Human-in-the-Loop**: Interactive reflection with human feedback
6. **Tighter integration of advanced enhancements** – e.g., using MODP to automatically tune reflection thresholds, or using LIMIT Graph to prioritize critical nodes.

## References

- Green Agent Repository: https://github.com/NurcholishAdam/Green_Agent
- AgentBeats Platform: https://agentbeats.ai
- Pareto Optimization: Multi-objective decision making
- Meta-Cognitive AI: Self-aware agent architectures
- **Multi‑Teacher Distillation**: Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE**: Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF**: Christiano et al., "Deep Reinforcement Learning from Human Preferences"
