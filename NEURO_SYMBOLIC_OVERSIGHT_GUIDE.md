
# Neuro-Symbolic Oversight for Green Agent

## Overview

This enhancement extends the Green Agent architecture with a neuro-symbolic oversight paradigm inspired by FormalJudge. It combines neural agent reasoning with explicit symbolic constraints to provide interpretable, verifiable governance over agent behavior.

**New in this version:** Integration with advanced enhancement modules (LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating, and FlexGen) is now supported. These modules enrich the symbolic rules with contextual metrics and learned decision-making, further strengthening the oversight layer.

---

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────┐
│                    Green Agent Core                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Metrics    │  │  Reflection  │  │    Pareto    │ │
│  │  Collector   │  │    Engine    │  │   Analyzer   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│            Symbolic Reasoning Layer (NEW)                │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Symbolic Reasoning Engine                       │  │
│  │  - Rule Parser & Evaluator                       │  │
│  │  - Violation Trace Generator                     │  │
│  │  - Category-based Filtering                      │  │
│  └──────────────────────────────────────────────────┘  │
│                          ↓                               │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Symbolic Policy (symbolic_policy.yaml)          │  │
│  │  - Sustainability Rules                          │  │
│  │  - Resource Management Rules                     │  │
│  │  - Fairness Rules                                │  │
│  │  - Safety Rules                                  │  │
│  │  - Compliance Rules                              │  │
│  │  - Enhanced Rules (MODP, RLHF, Graph, FlexGen)   │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│              Enhanced Feedback System                    │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Triple-Layer Feedback                           │  │
│  │  1. Objective (Pareto Analysis)                  │  │
│  │  2. Subjective (Agent Reflections)               │  │
│  │  3. Symbolic (Rule Violations) ← NEW             │  │
│  │  4. Advanced Decision Metrics (MODP, RLHF, etc.) │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│              Dashboard & Visualization                   │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Symbolic Visualizer (NEW)                       │  │
│  │  - Violation Timeline                            │  │
│  │  - Category Breakdown                            │  │
│  │  - Severity Heatmap                              │  │
│  │  - Trace Explanations                            │  │
│  │  - Enhanced Metrics Overlay (MODP, Graph)        │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## Key Features

### 1. Symbolic Rule Definition

Rules are defined in `symbolic_policy.yaml` with explicit logical constraints:

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon footprint combined with high latency"
```

**Rule Categories:**
- **Sustainability**: Carbon, energy efficiency
- **Resource**: Memory, CPU, latency
- **Fairness**: Resource distribution, query equity
- **Safety**: Error rates, system stability
- **Compliance**: ESG standards, audit requirements
- **Enhanced**: MODP score, RLHF feedback, LIMIT Graph metrics, FlexGen energy

### 2. Symbolic Reasoning Engine

The engine evaluates rules against collected metrics:

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
violations = engine.evaluate_rules(metrics, step=current_step)
```

**Features:**
- Lightweight rule parser (no heavy dependencies)
- Safe evaluation context
- Modular rule loading
- Domain-specific rule support
- **Support for enhanced metrics** (modp_score, graph_centrality, etc.)

### 3. Formal Violation Traces

When rules are violated, formal traces are generated:

```
Rule: Carbon Threshold Violation (SUST-001)
Condition: carbon > 60 AND latency > 2000
Observations:
  carbon = 72.0
  latency = 2500
Violation: Rule triggered → flag_inefficiency
```

**Trace Components:**
- Rule identification
- Condition specification
- Observed metric values
- Action triggered
- Human-readable explanation

### 4. Integration with Existing Systems

#### Metrics Collection
```python
# Metrics automatically fed to symbolic engine
metrics = metrics_collector.get_metrics_for_reflection()
violations = symbolic_engine.evaluate_rules(metrics, step)
```

#### Policy Feedback
```python
# Symbolic violations merged with Pareto + reflections
dual_feedback = feedback_system.generate_dual_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations  # NEW
)
```

#### Dashboard Visualization
```python
# Violations visualized in dashboard
symbolic_visualizer.add_violations(violations)
html_section = symbolic_visualizer.generate_dashboard_section()
```

### 5. Reflection Checkpoints

Symbolic rules are evaluated at:
- **Step completion**: After each query execution
- **Reflection checkpoints**: Aligned with agent self-reflection
- **Policy violations**: When numeric budgets are exceeded
- **Manual triggers**: On-demand evaluation

### 6. Sustained Reflection

The system tracks violation patterns across runs:

```python
# Pattern analysis
summary = engine.get_violation_summary()
# {
#   "total_violations": 15,
#   "by_category": {"sustainability": 8, "resource": 7},
#   "by_severity": {"critical": 3, "high": 5, "medium": 7},
#   "violation_rate": 0.75
# }
```

**Meta-insights generated:**
- "Agent consistently violates latency when conserving carbon"
- "Memory violations increase after step 5"
- "Fairness issues emerge with heterogeneous queries"

---

## Advanced Enhancements Integration

The neuro‑symbolic oversight layer can now use metrics and decisions from the advanced enhancements folder (`src/enhancements`). When those modules are enabled, the following integrations become available:

| Enhancement | Integration with Symbolic Oversight |
|-------------|---------------------------------------|
| **LIMIT Graph** | Graph centrality and connectivity are available as rule variables (`graph_centrality`, `graph_connectivity`). Low centrality may trigger topology review. |
| **MODP** | The composite score (`modp_score`) can be used in rules to trigger policy reviews when below a threshold. |
| **RLHF** | Human feedback (`human_feedback_score`) influences rule thresholds and can trigger feedback collection actions. |
| **Multi‑Teacher Distillation** | The distillation update rate (`distillation_update_rate`) is monitored; stalled learning may raise an alert. |
| **Bio‑inspired Optimisation** | Evolutionary best fitness (`evolutionary_best_fitness`) is tracked; stagnation can trigger restart or parameter tuning. |
| **MoE Expert Gating** | Gate weight standard deviation (`moe_gate_stddev`) indicates stability; high variance may indicate expert routing issues. |
| **FlexGen** | FlexGen energy consumption (`flexgen_energy_joules`) can be used to trigger precision switches or delegation policy changes. |

These enhanced metrics are automatically included in the `metrics` dict when the corresponding modules are active, and can be referenced in `symbolic_policy.yaml` rules just like any other metric.

### Example Enhanced Rule

```yaml
symbolic_rules:
  - id: "ENH-MODP-001"
    name: "Low MODP Score Alert"
    category: "enhanced"
    priority: "warning"
    condition: "modp_score < 0.4"
    action: "trigger_policy_review"
    explanation: "Overall multi‑objective performance is poor."
```

---

## Usage Guide

### Basic Usage

1. **Define symbolic rules** in `symbolic_policy.yaml`
2. **Run agent** with symbolic oversight enabled:

```bash
python run_agent.py --config example_config.json --policy green_policy.yaml
```

3. **Review violations** in outputs:
   - `symbolic_violations.json` - Detailed traces
   - `symbolic_violation_report.json` - Summary report
   - `dashboard.html` - Visual dashboard

### Adding Custom Rules

Edit `symbolic_policy.yaml`:

```yaml
symbolic_rules:
  - id: "CUSTOM-001"
    name: "My Custom Rule"
    category: "custom"
    priority: "medium"
    condition: "energy > 3.0 AND tool_calls > 20"
    action: "optimize_strategy"
    explanation: "Custom constraint for my use case"
```

**Supported Operators:**
- Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
- Logical: `AND`, `OR`, `NOT`
- Variables: `energy`, `carbon`, `latency`, `memory`, `tool_calls`, `cpu_percent`
- **Enhanced variables**: `modp_score`, `graph_centrality`, `graph_connectivity`, `human_feedback_score`, `distillation_update_rate`, `moe_gate_stddev`, `evolutionary_best_fitness`, `flexgen_energy_joules`

### Domain-Specific Rules

Add domain extensions:

```yaml
domain_extensions:
  research:
    - id: "RES-DOMAIN-001"
      condition: "query_type == 'research' AND tool_calls > 100"
      action: "optimize_research_strategy"
      explanation: "Research queries should use efficient strategies"
```

Activate with:
```python
violations = engine.evaluate_rules(metrics, step, domain="research")
```

### Filtering Violations

```python
# By category
sustainability_violations = engine.get_violations_by_category("sustainability")

# By severity
critical = [v for v in violations if v.severity == "critical"]

# Using visualizer
filtered = visualizer.filter_by_rule_type("fairness")
```

---

## Integration with Green_Agent Repository

This implementation is designed to integrate seamlessly with the [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent) repository:

### File Structure
```
green_agent_repo/
├── symbolic_policy.yaml              # NEW: Symbolic rule definitions
├── src/
│   ├── symbolic/                     # NEW: Symbolic reasoning module
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py
│   ├── dashboard/
│   │   └── symbolic_visualizer.py   # NEW: Violation visualization
│   ├── policy/
│   │   └── policy_feedback.py       # ENHANCED: Triple-layer feedback
│   └── enhancements/                # ADVANCED MODULES (LIMIT Graph, MODP, RLHF, etc.)
│       ├── schemas/
│       │   ├── node_descriptor.py
│       │   ├── workload_descriptor.py
│       │   └── feedback_event.py
│       ├── zero_trust_architecture.py
│       └── core/
│           ├── graph_registry.py
│           └── ...
├── run_agent.py                      # ENHANCED: Symbolic + advanced integration
└── demo_symbolic_oversight.py        # NEW: Demo script
```

### Compatibility

- **No breaking changes** to existing Green_Agent functionality
- **Optional activation** - works with or without symbolic oversight
- **Backward compatible** with existing policy files
- **Modular design** - can be disabled by not loading symbolic_policy.yaml
- **Advanced enhancements** are optional and gracefully skipped if modules missing

---

## Performance Considerations

### Computational Overhead

- **Rule evaluation**: O(n) where n = number of rules (~10-50 rules typical)
- **Per-step overhead**: < 10ms for typical rule sets (including enhanced metrics)
- **Memory footprint**: Minimal (~1-2MB for violation history)
- **Distillation inference**: <5ms per decision (if enhancements enabled)

### Optimization Tips

1. **Limit rule complexity**: Keep conditions simple
2. **Use priority ordering**: Critical rules evaluated first
3. **Batch evaluations**: Evaluate at reflection checkpoints only
4. **Prune history**: Archive old violations periodically
5. **Disable enhancements when not needed**: Set `use_enhancements=False` in config.

---

## Comparison with FormalJudge

| Aspect | FormalJudge | This Implementation |
|--------|-------------|---------------------|
| **Symbolic Logic** | Full theorem prover | Lightweight evaluator |
| **Rule Language** | Formal logic syntax | YAML + Python expressions |
| **Integration** | Standalone system | Embedded in agent |
| **Overhead** | Higher | Minimal |
| **Flexibility** | Rigid formal proofs | Pragmatic constraints |
| **Advanced Metrics** | Not applicable | MODP, RLHF, LIMIT Graph, etc. |
| **Use Case** | Safety-critical systems | Sustainable AI agents |

---

## Examples

### Example 1: Detecting Inefficiency

```python
# Rule: IF carbon > 60g AND latency > 2s THEN flag_inefficiency
metrics = {
    "carbon": 72,  # grams
    "latency": 2500,  # ms
    # ... other metrics
}

violations = engine.evaluate_rules(metrics, step=5)
# Result: SUST-001 violation detected
```

### Example 2: Memory Alert

```python
# Rule: IF memory > 500MB THEN trigger_resource_alert
metrics = {
    "memory": 550,  # MB
    # ... other metrics
}

violations = engine.evaluate_rules(metrics, step=3)
# Result: RES-001 violation detected
```

### Example 3: Composite Rule

```python
# Rule: IF (carbon > 1.5g OR energy > 4.0Wh) AND latency > 100s AND tool_calls > 30
metrics = {
    "carbon": 1.8,
    "energy": 4.5,
    "latency": 110000,
    "tool_calls": 35
}

violations = engine.evaluate_rules(metrics, step=8)
# Result: COMP-SUST-001 violation detected
```

### Example 4: Enhanced Rule with MODP

```python
# Rule: IF modp_score < 0.4 THEN trigger_policy_review
metrics = {
    "modp_score": 0.35,
    # ... other metrics (if enhancements enabled, this is automatically available)
}

violations = engine.evaluate_rules(metrics, step=10)
# Result: ENH-MODP-001 violation detected
```

---

## Troubleshooting

### Issue: Rules not triggering

**Solution:** Check metric normalization in `_normalize_metrics()`. Ensure units match:
- Carbon: grams (not kg)
- Latency: milliseconds
- Memory: MB
- For enhanced metrics: scores are typically 0-1, centrality 0-1, energy in Joules.

### Issue: False positives

**Solution:** Adjust thresholds in `symbolic_policy.yaml`:
```yaml
condition: "carbon > 70"  # Increase threshold
```

### Issue: Performance degradation

**Solution:** Reduce evaluation frequency:
```yaml
evaluation_config:
  evaluation_triggers:
    - "reflection_checkpoint"  # Only at checkpoints
```

### Issue: Enhanced metrics not available

**Solution:** Verify that the `src/enhancements` folder exists and dependencies are installed. When enhancements are disabled, those metrics will not be present in the `metrics` dict, and rules referencing them may not trigger. To include them, enable enhancements in your config or environment.

---

## Future Enhancements

1. **Z3 Integration**: Use Z3 SMT solver for complex constraints
2. **Temporal Logic**: Add temporal operators (ALWAYS, EVENTUALLY)
3. **Probabilistic Rules**: Support uncertainty in conditions
4. **Auto-tuning**: Learn optimal thresholds from historical data (can use evolutionary optimisation)
5. **Multi-agent Coordination**: Cross-agent rule enforcement (can use MoE gating for expert selection)

---

## References

- FormalJudge: Neuro-symbolic oversight paradigm
- Green_Agent: [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent)
- Symbolic AI: Rule-based reasoning systems
- Interpretable AI: Explainable decision-making
- **Multi‑Teacher Distillation**: Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE**: Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF**: Christiano et al., "Deep Reinforcement Learning from Human Preferences"

---

## License

Follows the same license as the Green_Agent repository.

## Contributing

To add new rule categories or enhance the symbolic engine:

1. Define rules in `symbolic_policy.yaml`
2. Extend `SymbolicReasoningEngine` if needed
3. Add visualization support in `SymbolicVisualizer`
4. Update this documentation
5. Submit PR to Green_Agent repository

---

**Note:** This implementation is inspired by FormalJudge but adapted for practical integration with Green_Agent. It prioritizes pragmatic oversight over formal verification, making it suitable for sustainable AI agent development.
