
# Neuro-Symbolic Oversight for Green Agent

## Quick Start

### Installation

No additional dependencies required beyond the base Green Agent installation.  
*(Optional)* To enable advanced enhancements (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen), ensure the `src/enhancements` modules are available and dependencies listed in the main `requirements.txt` are installed.

### Basic Usage

1. **Run the demo:**
```bash
python demo_symbolic_oversight.py
```

2. **Run with symbolic oversight:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml
```

3. **Run with advanced enhancements:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml --enhancements
```

4. **Check results:**
- `symbolic_violations.json` - Detailed violation traces
- `dashboard.html` - Visual dashboard with violations
- `symbolic_violation_report.json` - Summary report

### Example Output

```
🌱 Green Agent - Meta-Cognitive Architecture v2.0
============================================================
📋 Policy loaded: {'max_energy_per_task_wh': 5.0, ...}
🔍 Symbolic reasoning engine loaded with 15 rules
🧠 Advanced enhancements enabled (LIMIT Graph, MODP, RLHF, distillation, MoE, evolutionary, FlexGen)

🔄 Step 1: Processing query 'query_1'
  ⚠️  2 symbolic rule violation(s) detected
     - Energy Budget Exceeded [critical]
     - Memory Overflow Risk [critical]
  🧠 Enhanced decision: strategy=carbon_first, priority=green, MODP score=0.72
  🤔 Reflection checkpoint at step 1
  💭 Self-explanation: High resource usage detected
  
============================================================
✅ Green_Agent execution complete with neuro-symbolic oversight
📈 Total reflections: 3
🎯 Pareto frontier size: 2
🧠 Historical runs: 5
🔍 Symbolic violations: 8
⚠️  CRITICAL: 2 critical rule violation(s) detected!
   - Energy Budget Exceeded at step 1
   - Memory Overflow Risk at step 3
```

---

## Architecture Overview

```
┌─────────────────────────────────────────┐
│         Green Agent Core                │
│  • Metrics Collection                   │
│  • Reflection Engine                    │
│  • Pareto Analysis                      │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│    Symbolic Reasoning Layer (NEW)       │
│  • Rule Evaluation                      │
│  • Violation Detection                  │
│  • Trace Generation                     │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│    Enhanced Feedback System             │
│  • Objective (Pareto)                   │
│  • Subjective (Reflections)             │
│  • Symbolic (Violations) ← NEW          │
│  • Advanced Enhancements (MODP, RLHF,  │
│    LIMIT Graph, Distillation, MoE,     │
│    Evolutionary, FlexGen)               │
└─────────────────────────────────────────┘
```

---

## Key Features

### 1. Explicit Symbolic Rules

Define rules in `symbolic_policy.yaml`:

```yaml
symbolic_rules:
  - id: "SUST-001"
    name: "Carbon Threshold Violation"
    category: "sustainability"
    priority: "high"
    condition: "carbon > 60 AND latency > 2000"
    action: "flag_inefficiency"
    explanation: "High carbon with high latency"
```

### 2. Automatic Violation Detection

Rules are evaluated automatically at:
- Each step completion
- Reflection checkpoints
- Policy violations
- Manual triggers

### 3. Formal Violation Traces

```
Rule: Carbon Threshold Violation (SUST-001)
Condition: carbon > 60 AND latency > 2000
Observations:
  carbon = 72.0
  latency = 2500
Violation: Rule triggered → flag_inefficiency
```

### 4. Dashboard Integration

Violations appear in the dashboard with:
- Severity indicators (critical, high, medium, low)
- Category grouping
- Timeline view
- Detailed trace explanations

### 5. Triple-Layer Feedback

Combines three perspectives:
1. **Objective**: Pareto frontier analysis
2. **Subjective**: Agent self-reflections
3. **Symbolic**: Rule violations (NEW)

### 6. Advanced Enhancements Integration

When enabled, the symbolic reasoning layer can also incorporate:
- **LIMIT Graph metrics** (centrality, connectivity) into rule conditions.
- **MODP composite score** to influence decision prioritisation.
- **RLHF human feedback** to adjust rule thresholds dynamically.
- **Multi‑Teacher On‑Policy Distillation + MoE gating** to select optimal mitigation actions.
- **Bio‑inspired Optimisation** to evolve rule weights or thresholds.
- **FlexGen integration** for high‑throughput LLM tasks with adaptive precision selection.

---

## Rule Categories

### Sustainability Rules
- Carbon emissions
- Energy efficiency
- Resource optimization

### Resource Management Rules
- Memory usage
- CPU utilization
- Latency constraints

### Fairness Rules
- Resource distribution
- Query equity
- Load balancing

### Safety Rules
- Error rates
- System stability
- Cascading failures

### Compliance Rules
- ESG standards
- Audit requirements
- Regulatory constraints

### Enhanced Rules (optional)
Rules may reference advanced metrics:
- `modp_score < 0.4` → trigger policy review
- `graph_centrality < 0.3` → check topology
- `human_feedback_score < 0.3` → collect more feedback
- `flexgen_energy_joules > 1000` → switch to lower precision

---

## Customization

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
    explanation: "Custom constraint"
```

### Supported Operators

- **Comparison**: `>`, `<`, `>=`, `<=`, `==`, `!=`
- **Logical**: `AND`, `OR`, `NOT`
- **Variables**: 
  - `energy` (Wh)
  - `carbon` (grams)
  - `latency` (milliseconds)
  - `memory` (MB)
  - `tool_calls` (count)
  - `cpu_percent` (0-100)
  - Advanced: `modp_score`, `graph_centrality`, `graph_connectivity`, `human_feedback_score`, `distillation_update_rate`, `moe_gate_stddev`, `evolutionary_best_fitness`, `flexgen_energy_joules` (if enhancements enabled)

### Domain-Specific Rules

```yaml
domain_extensions:
  research:
    - id: "RES-DOMAIN-001"
      condition: "query_type == 'research' AND tool_calls > 100"
      action: "optimize_research_strategy"
      explanation: "Research-specific constraint"
```

Activate with:
```python
violations = engine.evaluate_rules(metrics, step, domain="research")
```

---

## Integration with Advanced Enhancements

The neuro‑symbolic engine can be combined with the advanced modules in `src/enhancements` to achieve context‑aware, learned decision‑making. Below are the primary integration points.

### LIMIT Graph

- **What**: Graph metrics (centrality, connectivity) indicate the system’s topological importance.
- **How**: The `GraphRegistry` and `CausalGraph` provide these metrics. They can be fed into symbolic rules or used to influence the selection of experts.
- **Example**:
  ```yaml
  symbolic_rules:
    - id: "ENH-GRAPH-001"
      condition: "graph_centrality < 0.3"
      action: "check_topology"
  ```

### MODP (Multi‑Objective Decision Process)

- **What**: Configurable weights for accuracy, energy, latency, and carbon.
- **How**: The `NodeDescriptor` and `WorkloadDescriptor` compute a composite score using these weights. The score can be included in rule conditions or used to adjust action priorities.
- **Example**:
  ```python
  node = NodeDescriptor(..., modp_weights=[0.4, 0.3, 0.2, 0.1])
  strategy = await node.select_routing_strategy()
  ```

### RLHF (Reinforcement Learning from Human Feedback)

- **What**: Human feedback score (0‑1) influences policies.
- **How**: The feedback is part of the state in distillation teachers. Rules can check `human_feedback_score` to decide whether to collect more feedback or adjust behavior.
- **Example**:
  ```yaml
  - id: "ENH-RLHF-001"
    condition: "human_feedback_score < 0.3"
    action: "collect_more_feedback"
  ```

### Multi‑Teacher On‑Policy Distillation with MoE Gating

- **What**: A lightweight student learns from rule‑based, historical ML, Q‑learning, and RLHF teachers, blended by a gating network.
- **How**: The `NodeDescriptor.select_routing_strategy()` and `WorkloadDescriptor.select_priority()` internally use distillation + MoE. The symbolic engine can call these methods when a rule triggers an action like `"optimize_strategy"`.
- **Example**:
  ```python
  if "optimize_strategy" in violations:
      strategy = await node.select_routing_strategy()
      priority = await workload.select_priority()
  ```

### Bio‑inspired Optimisation (Evolutionary)

- **What**: Genetic algorithms tune weights and hyperparameters.
- **How**: Set `use_evolutionary=True` in descriptors or Zero Trust config. The evolutionary optimizer updates MODP weights over time; these changes can be logged in `FeedbackEvent` and referenced in rules.
- **Example**:
  ```yaml
  - id: "ENH-EVOL-001"
    condition: "evolutionary_best_fitness - evolutionary_best_fitness offset 1h < 0.01"
    action: "restart_evolution"
  ```

### MoE Expert Gating

- **What**: Dynamic weighting of experts.
- **How**: The gating network is trained together with the student. Its stability (`moe_gate_stddev`) can be monitored via rules.
- **Example**:
  ```yaml
  - id: "ENH-MOE-001"
    condition: "moe_gate_stddev > 0.2"
    action: "investigate_expert_routing"
  ```

### FlexGen Integration

- **What**: High‑throughput LLM inference with adaptive precision.
- **How**: The system can delegate to FlexGen when `delegation_policy` is `"adaptive"`. Symbolic rules can trigger precision switches based on energy or carbon.
- **Example**:
  ```yaml
  - id: "ENH-FLEX-001"
    condition: "flexgen_energy_joules > 1000"
    action: "switch_precision"
  ```

---

## API Reference

### SymbolicReasoningEngine

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
violations = engine.evaluate_rules(metrics, step=1, domain=None)
summary = engine.get_violation_summary()
```

### SymbolicVisualizer

```python
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

visualizer = SymbolicVisualizer()
visualizer.add_violations([v.to_dict() for v in violations])
html = visualizer.generate_dashboard_section()
```

### PolicyFeedback (Enhanced)

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()
result = feedback.generate_dual_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations  # NEW
)
```

---

## Testing

Run the test suite:

```bash
python test_symbolic_oversight.py
```

Expected output:
```
test_category_filtering ... ok
test_composite_rule ... ok
test_energy_violation ... ok
test_html_generation ... ok
test_memory_violation ... ok
test_no_violations ... ok
test_rule_loading ... ok
test_severity_summary ... ok
test_symbolic_recommendations ... ok
test_timeline_generation ... ok
test_triple_layer_feedback ... ok
test_violation_summary ... ok
test_violation_trace_structure ... ok
# Advanced enhancements tests (if modules available)
test_node_descriptor_routing ... ok
test_workload_descriptor_priority ... ok
test_feedback_event_enhanced_fields ... ok
test_zero_trust_enhanced_init ... ok
test_graph_registry_and_causal ... ok
test_dag_carbon_ledger_backpropagation ... ok

============================================================
Test Summary
============================================================
Tests run: 19
Successes: 19
Failures: 0
Errors: 0
```

---

## Performance

### Computational Overhead

- **Rule evaluation**: ~5-10ms per step (15 rules)
- **Memory footprint**: ~1-2MB for violation history
- **Dashboard generation**: ~50-100ms
- **Enhanced modules**: distillation inference <5ms per decision; evolutionary update negligible in frequency

### Optimization Tips

1. **Reduce rule complexity**: Keep conditions simple
2. **Limit evaluation frequency**: Only at reflection checkpoints
3. **Prune history**: Archive old violations periodically
4. **Use priority ordering**: Critical rules first
5. **Disable enhancements when not needed**: Set `use_enhancements=False` in config.

---

## Integration with Green_Agent

This implementation integrates seamlessly with [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent):

### File Structure
```
green_agent_repo/
├── symbolic_policy.yaml              # NEW
├── src/
│   ├── symbolic/                     # NEW
│   │   ├── __init__.py
│   │   └── symbolic_reasoning_engine.py
│   ├── dashboard/
│   │   └── symbolic_visualizer.py   # NEW
│   ├── policy/
│   │   └── policy_feedback.py       # ENHANCED
│   ├── enhancements/                # ADDITIONAL ADVANCED MODULES
│   │   ├── schemas/
│   │   │   ├── node_descriptor.py
│   │   │   ├── workload_descriptor.py
│   │   │   └── feedback_event.py
│   │   ├── zero_trust_architecture.py
│   │   └── core/
│   │       ├── graph_registry.py
│   │       └── ...
├── run_agent.py                      # ENHANCED
├── demo_symbolic_oversight.py        # NEW
├── test_symbolic_oversight.py        # NEW
├── NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md # NEW
└── SYMBOLIC_OVERSIGHT_README.md      # NEW (this file)
```

### Compatibility

✅ No breaking changes to existing functionality  
✅ Optional activation (works with/without symbolic_policy.yaml)  
✅ Backward compatible with existing policy files  
✅ Modular design (can be disabled)  
✅ Advanced enhancements are optional and gracefully skipped if modules missing  

---

## Troubleshooting

### Issue: Rules not triggering

**Cause**: Metric units mismatch  
**Solution**: Check normalization in `_normalize_metrics()`:
- Carbon: grams (not kg)
- Latency: milliseconds
- Memory: MB

### Issue: False positives

**Cause**: Thresholds too strict  
**Solution**: Adjust in `symbolic_policy.yaml`:
```yaml
condition: "carbon > 70"  # Increase threshold
```

### Issue: Performance degradation

**Cause**: Too many rules or evaluations  
**Solution**: Reduce evaluation frequency:
```yaml
evaluation_config:
  evaluation_triggers:
    - "reflection_checkpoint"  # Only at checkpoints
```

### Issue: Missing symbolic_policy.yaml

**Cause**: File not found  
**Solution**: Engine loads default rules automatically. Create `symbolic_policy.yaml` for custom rules.

### Issue: Enhancements not activating

**Solution**: Check that:
- `src/enhancements` folder exists
- Required dependencies are installed (`pip install -r requirements.txt`)
- `use_enhancements=True` in config or environment variables set

---

## Examples

### Example 1: Basic Violation Detection

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

engine = SymbolicReasoningEngine()

metrics = {
    "energy": 6.0,  # Exceeds 5.0 limit
    "carbon": 70,   # Exceeds 60g limit
    "latency": 2500,
    "memory": 300,
    "tool_calls": 25,
    "cumulative": {
        "total_energy_wh": 6.0,
        "total_carbon_kg": 0.07,
        "total_latency_ms": 2500,
        "max_memory_mb": 300,
        "total_tool_calls": 25,
        "step_count": 5
    }
}

violations = engine.evaluate_rules(metrics, step=5)

for v in violations:
    print(f"{v.rule_name}: {v.explanation}")
```

### Example 2: Dashboard Integration

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

engine = SymbolicReasoningEngine()
visualizer = SymbolicVisualizer()

violations = engine.evaluate_rules(metrics, step=5)
visualizer.add_violations([v.to_dict() for v in violations])

html = visualizer.generate_dashboard_section()
with open("violations_dashboard.html", "w") as f:
    f.write(html)
```

### Example 3: Custom Domain Rules

```yaml
# In symbolic_policy.yaml
domain_extensions:
  production:
    - id: "PROD-001"
      condition: "environment == 'production' AND carbon > 50"
      action: "strict_carbon_enforcement"
      explanation: "Production requires stricter limits"
```

```python
violations = engine.evaluate_rules(
    metrics, 
    step=1, 
    domain="production"
)
```

### Example 4: Combined with Advanced Enhancements

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
import asyncio

engine = SymbolicReasoningEngine()
node = NodeDescriptor(...)

# ... gather metrics, evaluate rules
violations = engine.evaluate_rules(metrics, step=1)

# If any violation triggers optimization action, use enhanced routing
if any(v.action_triggered == "optimize_strategy" for v in violations):
    strategy = asyncio.run(node.select_routing_strategy())
    print(f"Selected enhanced strategy: {strategy}")
```

---

## FAQ

**Q: Does this replace the existing policy engine?**  
A: No, it complements it. Numeric budgets (green_policy.yaml) + symbolic rules (symbolic_policy.yaml) work together.

**Q: Can I disable symbolic oversight?**  
A: Yes, simply don't create `symbolic_policy.yaml`. The engine will use minimal default rules.

**Q: How do I add Z3 solver support?**  
A: Future enhancement. Current implementation uses lightweight evaluation for performance.

**Q: Can rules reference historical data?**  
A: Not directly, but you can add derived metrics (e.g., `energy_variance`) to the metrics dict.

**Q: How do I export violations for external analysis?**  
A: Use `engine.export_violations("violations.json")` or `visualizer.export_violation_report("report.json")`.

**Q: How do the advanced enhancements integrate with symbolic rules?**  
A: The enhancements provide additional metrics (MODP score, graph centrality, human feedback, etc.) that can be used in rule conditions. When a rule triggers an action, the system can call the enhanced modules (e.g., `select_routing_strategy`) to adapt behavior.

---

## Contributing

To contribute enhancements:

1. Fork the Green_Agent repository
2. Add/modify rules in `symbolic_policy.yaml`
3. Extend `SymbolicReasoningEngine` if needed
4. Add tests in `test_symbolic_oversight.py`
5. Update documentation
6. Submit PR

---

## License

Follows the same license as Green_Agent repository.

---

## References

- **FormalJudge**: Neuro-symbolic oversight paradigm
- **Green_Agent**: [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent)
- **Symbolic AI**: Rule-based reasoning systems
- **Multi‑Teacher Distillation**: Hinton et al., "Distilling the Knowledge in a Neural Network"
- **MoE**: Shazeer et al., "Outrageously Large Neural Networks: The Sparsely-Gated Mixture-of-Experts Layer"
- **RLHF**: Christiano et al., "Deep Reinforcement Learning from Human Preferences"

---

## Support

For issues or questions:
1. Check this README and NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md
2. Run `python demo_symbolic_oversight.py` for examples
3. Run `python test_symbolic_oversight.py` to verify installation
4. Open issue on Green_Agent repository

---

**Version**: 1.1.0  
**Last Updated**: 2026-04-09  
**Status**: Production Ready
