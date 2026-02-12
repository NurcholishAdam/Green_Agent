# Neuro-Symbolic Oversight for Green Agent

## Quick Start

### Installation

No additional dependencies required beyond the base Green Agent installation.

### Basic Usage

1. **Run the demo:**
```bash
python demo_symbolic_oversight.py
```

2. **Run with symbolic oversight:**
```bash
python run_agent.py --config example_config.json --policy green_policy.yaml
```

3. **Check results:**
- `symbolic_violations.json` - Detailed violation traces
- `dashboard.html` - Visual dashboard with violations
- `symbolic_violation_report.json` - Summary report

### Example Output

```
🌱 Green Agent - Meta-Cognitive Architecture v2.0
============================================================
📋 Policy loaded: {'max_energy_per_task_wh': 5.0, ...}
🔍 Symbolic reasoning engine loaded with 15 rules

🔄 Step 1: Processing query 'query_1'
  ⚠️  2 symbolic rule violation(s) detected
     - Energy Budget Exceeded [critical]
     - Memory Overflow Risk [critical]
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
└─────────────────────────────────────────┘
```

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

## API Reference

### SymbolicReasoningEngine

```python
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine

# Initialize
engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")

# Evaluate rules
violations = engine.evaluate_rules(metrics, step=1, domain=None)

# Get summary
summary = engine.get_violation_summary()

# Filter by category
sustainability = engine.get_violations_by_category("sustainability")

# Export violations
engine.export_violations("violations.json")
```

### SymbolicVisualizer

```python
from src.dashboard.symbolic_visualizer import SymbolicVisualizer

# Initialize
visualizer = SymbolicVisualizer()

# Add violations
visualizer.add_violations([v.to_dict() for v in violations])

# Generate views
timeline = visualizer.generate_violation_timeline()
category_view = visualizer.generate_category_view()
severity_summary = visualizer.generate_severity_summary()

# Generate HTML
html = visualizer.generate_dashboard_section()

# Export report
visualizer.export_violation_report("report.json")
```

### PolicyFeedback (Enhanced)

```python
from src.policy.policy_feedback import PolicyFeedback

feedback = PolicyFeedback()

# Generate triple-layer feedback
result = feedback.generate_dual_layer_feedback(
    pareto_analysis=pareto_position,
    reflections=agent_reflections,
    metrics=metrics,
    symbolic_violations=violations  # NEW parameter
)
```

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

============================================================
Test Summary
============================================================
Tests run: 13
Successes: 13
Failures: 0
Errors: 0
```

## Performance

### Computational Overhead

- **Rule evaluation**: ~5-10ms per step (15 rules)
- **Memory footprint**: ~1-2MB for violation history
- **Dashboard generation**: ~50-100ms

### Optimization Tips

1. **Reduce rule complexity**: Keep conditions simple
2. **Limit evaluation frequency**: Only at reflection checkpoints
3. **Prune history**: Archive old violations periodically
4. **Use priority ordering**: Critical rules first

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
│   └── policy/
│       └── policy_feedback.py       # ENHANCED
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

# Evaluate and visualize
violations = engine.evaluate_rules(metrics, step=5)
visualizer.add_violations([v.to_dict() for v in violations])

# Generate dashboard
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
# In code
violations = engine.evaluate_rules(
    metrics, 
    step=1, 
    domain="production"
)
```

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

## Contributing

To contribute enhancements:

1. Fork the Green_Agent repository
2. Add/modify rules in `symbolic_policy.yaml`
3. Extend `SymbolicReasoningEngine` if needed
4. Add tests in `test_symbolic_oversight.py`
5. Update documentation
6. Submit PR

## License

Follows the same license as Green_Agent repository.

## References

- **FormalJudge**: Neuro-symbolic oversight paradigm
- **Green_Agent**: [NurcholishAdam/Green_Agent](https://github.com/NurcholishAdam/Green_Agent)
- **Symbolic AI**: Rule-based reasoning systems

## Support

For issues or questions:
1. Check this README and NEURO_SYMBOLIC_OVERSIGHT_GUIDE.md
2. Run `python demo_symbolic_oversight.py` for examples
3. Run `python test_symbolic_oversight.py` to verify installation
4. Open issue on Green_Agent repository

---

**Version**: 1.0.0  
**Last Updated**: 2026-02-12  
**Status**: Production Ready
