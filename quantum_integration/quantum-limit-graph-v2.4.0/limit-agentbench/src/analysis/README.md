# `analysis/README.md`

```markdown
# Analysis Module

**Version**: 5.0.0
**Python**: 3.10+
**License**: Apache 2.0 (part of Green_Agent)

Multi-objective analysis tools for Green Agent: Pareto frontier analysis,
task complexity normalization, real-time telemetry, decision explanation,
and structured evaluation evidence.

The `analysis` folder is the **decision-intelligence and evidence layer**
for Green Agent. Its responsibility is to turn execution telemetry,
policies, and outcomes into trustworthy recommendations, explanations,
evaluation reports, and learning signals.

It does **not** implement federated training, policy execution, or
quantum execution — those live in `federation/`, `policy/`, and
`quantum/`. `analysis` evaluates, compares, audits, and explains their
outcomes.

---

## Table of Contents

1. [Installation](#-installation)
2. [Quick Start](#-quick-start)
3. [Shared Contracts](#-shared-contracts)
4. [The Ten Enhancement Layers](#-the-ten-enhancement-layers)
5. [The Telemetry Stack](#-the-telemetry-stack)
6. [API Reference](#-api-reference)
7. [Use Cases](#-use-cases)
8. [Visualization](#-visualization)
9. [Testing](#-testing)
10. [Migration from Legacy to Enhanced](#-migration-from-legacy-to-enhanced)
11. [Feature Toggles](#-feature-toggles)
12. [Priority Mapping](#-priority-mapping)
13. [Best Practices](#-best-practices)
14. [Troubleshooting](#-troubleshooting)
15. [Further Reading](#-further-reading)
16. [License](#-license)
17. [Contributing](#-contributing)
18. [Support](#-support)

---

## 📦 Installation

### Recommended

```bash
# Core analysis (required)
pip install "numpy>=1.24,<3.0" "psutil>=5.9,<7.0" "PyYAML>=6.0,<7.0"

# Optional: visualization
pip install "plotly>=5.14,<7.0" "pandas>=2.0,<3.0"

# Optional: real energy measurement
pip install "codecarbon>=2.3,<3.0"

# Test dependencies
pip install "pytest>=7.4" "pytest-asyncio>=0.21" "pytest-cov>=4.1" \
            "requests>=2.31" "prometheus_client>=0.19"

# Or install everything from the requirements file
pip install -r requirements_analysis.txt
```

### Dependency Reference

| Dependency | Required by | Optional? |
|---|---|---|
| `numpy` | Legacy Pareto modules | ✅ Enhanced modules use stdlib |
| `psutil` | `self_monitor.py`, `metrics_collector.py` | ❌ Required for memory/CPU metrics |
| `PyYAML` | `policy_loader.py` | ❌ Required for policy loading |
| `plotly`, `pandas` | `pareto_visualizer.py` | ✅ Only for visualization |
| `codecarbon` | `energy_meter.py` | ✅ Falls back to a configurable constant |

> **Note**: `logging`, `asyncio`, `json`, `dataclasses`, and `typing` are
> part of the Python standard library. They are **not** pip packages and
> must not appear in `requirements.txt`.

---

## 🚀 Quick Start

### Pareto Analysis (legacy API)

```python
from analysis import ParetoPoint, ParetoFrontierAnalyzer

agents = [
    ParetoPoint('gpt4',   accuracy=0.95, energy_kwh=0.005,
                carbon_co2e_kg=0.001,   latency_ms=200),
    ParetoPoint('llama3', accuracy=0.88, energy_kwh=0.002,
                carbon_co2e_kg=0.0004,  latency_ms=120),
]

analyzer = ParetoFrontierAnalyzer()
frontier = analyzer.compute_frontier(agents)
knee = analyzer.get_knee_point(frontier)
print(f"Recommended: {knee.agent_id}")
```

### Pareto Analysis (enhanced API with XAI + provenance)

```python
from analysis.pareto_analyzer import ParetoAnalyzer

analyzer = ParetoAnalyzer(
    run_id="run-001",
    deployment_id="us-ca-prod-01",
    precision="int8",
    features={"carbon_in_frontier": True},  # carbon as 3rd objective
)

analyzer.add_record(100.0, 0.90, 50.0, "A", source="measured")
analyzer.add_record(120.0, 0.85, 45.0, "B", source="measured")
analyzer.add_record(90.0,  0.80, 60.0, "C", source="estimated")

result = analyzer.compute_frontier_detailed()
print(f"Frontier: {[p.label for p in result.points]}")
for p in result.points:
    print(f"  {p.label}: {result.rationale_of(p.label)}")
```

### Complexity Analysis

```python
from analysis import ComplexityAnalyzer

trace = {
    'prompt': "Classify this image...",
    'reasoning': ["Step 1", "Step 2", "Step 3"],
    'tool_calls': [{'tool': 'vision_api'}],
    'execution_time_ms': 1500,
    'context_tokens': 512,
}

analyzer = ComplexityAnalyzer()
complexity = analyzer.analyze_from_trace(trace)
print(f"Tier:  {analyzer.categorize_complexity(complexity)}")
print(f"Score: {complexity.compute_composite_score():.2f}")
```

### Energy and Carbon Estimation

```python
from analysis.energy_meter import EnergyMeter
from analysis.carbon_estimator import CarbonEstimator

meter = EnergyMeter(
    fallback_power_watts=EnergyMeter.REALISTIC_GPU_TDP_WATTS,
    agent_id="worker-A",
    hardware_profile="V100",
    region="US-CA",
)
meter.start()
# ... run the task ...
meter.stop()

record = meter.measure_detailed()
print(f"Energy:    {record.energy_joules:.2f} J ({record.source})")
print(f"Simulated: {record.simulated}")

estimator = CarbonEstimator(
    grid_intensity_g_kwh=385.0,
    pue=1.2,
    region="US-CA",
)
estimate = estimator.estimate_detailed(record.energy_joules / 3.6e6)
print(f"Carbon: {estimate.carbon_operational_kg:.6f} kgCO₂e")
```

### Decision Record

```python
from analysis import create_decision_record

record = create_decision_record(
    run_id="run-001",
    task_id="classification-42",
    selected_action="route_to_mobilenet",
    alternatives=["resnet50", "efficientnet"],
    quality_score=0.86,
    latency_ms=80,
    energy_kwh=0.001,
    carbon_operational_kg=0.0002,
    provenance={"source": "measured", "simulated": False},
)
```

---

## 📚 Shared Contracts

Every enhanced module speaks a common language. When you write a new
analysis module, produce these shapes so downstream code can consume your
output without translation.

### `DecisionRecord`

The single versioned event every layer writes.

```python
from analysis import DecisionRecord
from datetime import datetime

record = DecisionRecord(
    run_id="run-001",
    timestamp=datetime.now(),
    task_id="task-1",
    selected_action="route_to_lora",
    alternatives=["full_finetuning", "qlora"],
    policy_version="v5.0.1",
    model_or_agent="agent-A",
    hardware_profile="V100",
    precision="int8",
    quality_score=0.92,
    latency_ms=120.0,
    energy_kwh=0.045,
    carbon_operational_kg=0.018,     # physical emissions
    carbon_contractual_kg=0.005,     # RECs — kept separate
    helium_units=0.003,
    uncertainty=0.02,
    explanation={"rationale": "lowest carbon route"},
    safety_verdict="passed",
    provenance={"source": "langchain_runtime", "simulated": False},
)
```

> ⚠️ **Important**: `carbon_operational_kg` and `carbon_contractual_kg`
> are **never** summed. Reports show both separately. Buying RECs does
> not cancel physical emissions.

### `EvaluationVerdict`

The structured pass/fail every evaluator emits.

```python
from analysis import EvaluationVerdict

verdict = EvaluationVerdict(
    record_id="rec-001",
    passed_hard_constraints=True,
    hard_constraint_violations=[],
    pareto_rank=0,
    is_pareto_efficient=True,
    explanation_confidence=0.92,
    safety_verdict="passed",
)
```

### Other Contracts

| Contract | Emitted by | Consumed by |
|---|---|---|
| `AgentObservation` | runtime adapters | `MultiAgentRoleAnalyzer` |
| `StepRecord` / `CallbackStep` | callbacks | `ExecutionTrace` |
| `TrendResult` | `SelfMonitor` | `AdaptiveController` |
| `RuntimeResult` | runtime adapters | all downstream analyzers |
| `MeasurementRecord` | `EnergyMeter` | `CarbonEstimator`, reports |
| `CarbonEstimate` | `CarbonEstimator` | reports, `DecisionRecord` |
| `MetricsSnapshot` | `MetricsCollector` | `metric_provenance` |
| `FrontierExplanation` | `pareto_analyzer` | decision dashboards |
| `DominanceVerdict` | `dominance_checker` | frontier algorithms |
| `OverheadExplanation` | `overhead_analyzer` | cost attribution |
| `GreenScoreResult` | `green_score` | leaderboards |
| `ResilienceEvent` | `resilience_analyzer` | chaos reports |
| `HITLRequest` | HITL gate | human review |
| `ChaosEvent` | `chaos` | `resilience_analyzer` |
| `MetricProvenance` | `metric_provenance` | all reports |

---

## 🏗️ The Ten Enhancement Layers

Every module supports the ten cross-cutting enhancements. Each layer is
independently toggleable via `features={...}`.

### 1. Quantum-Distillation

Compares classical, quantum, and distilled routes on quality, energy,
carbon, latency, error rate, shots, and queue time.

- `analysis.quantum_tradeoff_analyzer.QuantumTradeoffAnalyzer`
- `analysis.pareto_analyzer.QuantumDistillationBridge`
- `analysis.energy_meter.PrecisionLevel.QUANTUM_DISTILLED`

### 2. Causal Reinforcement Learning

Estimates whether a policy change *caused* improvement rather than
correlating with it.

- `analysis.causal_policy_evaluator.CausalPolicyEvaluator`
- `analysis.metrics_collector.CausalPenaltyLearner`
- `analysis.overhead_analyzer.CausalBudgetLearner`
- `analysis.complexity_analyzer.CausalNormalizationLearner`

### 3. Federated Green Learning

Aggregates site-level metrics without centralizing raw telemetry.

- `analysis.federated_analytics.FederatedAnalytics`
- `analysis.metric_provenance.FederatedGridAggregator`
- `analysis.overhead_analyzer.FederatedOverheadAggregator`
- `analysis.pareto_analyzer.FederatedFrontierAggregator`

### 4. Multi-Agent Coordination

Detects roles, collaboration patterns, redundancy, delegation quality,
and coordination overhead.

- `analysis.multiagent_role_analyzer.MultiAgentRoleAnalyzer`
- `analysis.adapters.langchain_runtime.LangChainRuntime.observations()`
- `analysis.adapters.autogen_conversation_graph.AutoGenConversationGraph.observations()`

### 5. Temporal Logic & Formal Verification

Stores verification evidence, coverage, counterexamples, and policy-safety
status. The verifier itself lives in `symbolic/`.

- `analysis.verification_evidence.VerificationEvidenceStore`
- `analysis.temporal_logic_monitor.TemporalLogicMonitor`
- `analysis.self_monitor.TrendResult.classification`

### 6. Explainable AI

Produces a traceable "why this action?" record with rationale,
alternatives rejected, trade-offs, and confidence.

- `analysis.decision_explainer.DecisionExplainer`
- `analysis.pareto_analyzer.FrontierExplainer`
- `analysis.green_score.GreenScoreExplainer`
- `analysis.overhead_analyzer.OverheadExplainer`
- `analysis.self_monitor.TrendExplainer`
- `analysis.leaderboard.RankExplainer`

### 7. Adaptive Precision

Compares FP32, FP16, BF16, INT8, INT4, and quantum-distilled routes on
quality, power, memory, thermal state, and device capability.

- `analysis.precision_hardware_analyzer.PrecisionHardwareAnalyzer`
- `analysis.energy_meter.PrecisionEnergyMultiplier`
- `analysis.green_score.tolerance_for_precision`

### 8. Carbon Markets & RECs

Separates operational emissions from contractual instruments. Maintains
certificate provenance, matching period, and residual emissions.

- `analysis.carbon_instruments_ledger.CarbonInstrumentsLedger`
- `analysis.carbon_estimator.CarbonEstimate.carbon_contractual_kg`

### 9. Resilience & Chaos

Measures fault-injection scenarios, recovery time, degraded-mode
behavior, safety-policy violations, and carbon cost of recovery.

- `analysis.chaos.ChaosInjector`, `ChaosEvent`
- `analysis.resilience_analyzer.ResilienceAnalyzer`
- `analysis.metrics_collector.CircuitBreaker`

### 10. Human-in-the-Loop

Tracks overrides, escalation reasons, reviewer agreement, decision impact,
and uncertainty-driven labeling value.

- `analysis.human_feedback_analyzer.HumanFeedbackAnalyzer`
- HITL gates exposed by runtime adapters and policy modules

---

## 📡 The Telemetry Stack

Nine modules produce the reliable telemetry the proposal requires
(Priority #1). Each is documented with its output shape.

| Module | Output | Purpose |
|---|---|---|
| `energy_meter.py` | `MeasurementRecord` | Real energy measurement with codecarbon fallback |
| `carbon_estimator.py` | `CarbonEstimate` | Carbon from energy + grid intensity + PUE |
| `metrics_collector.py` | `MetricsSnapshot` | Cross-cutting counter aggregation |
| `execution_trace.py` | `StepRecord` | Ordered event recording |
| `framework_overhead.py` | baseline | Per-framework latency/energy baseline |
| `overhead_analyzer.py` | `OverheadExplanation` | Per-tool-call cost estimation |
| `metric_provenance.py` | `MetricProvenance` | Source, timestamp, uncertainty per metric |
| `self_monitor.py` | `TrendResult` | Resource trends (CPU, memory) |
| `streaming.py` | `EmitStatus` | Real-time heartbeat emission |

### Provenance Bridge

Every metric is traceable back to its source:

```python
from analysis.metric_provenance import attach_provenance, get_provenance

metrics = {"energy": 0.05, "energy_source": "codecarbon"}
attach_provenance(metrics)

prov = get_provenance(metrics, "energy")
print(f"Source: {prov.source_kind} ({prov.trust_level()})")
# → Source: measured (high)
```

### Simulation Flag

Every record carries a `simulated: bool` flag. **Always check it** before
including a value in a production report:

```python
if record.simulated:
    logger.warning(f"Excluding simulated value: {record}")
    return
```

The proposal warns explicitly against "claiming advantage from isolated
results." The `simulated` flag is the safeguard.

---

## 📖 API Reference

### Legacy API (preserved, stable)

**`ParetoFrontierAnalyzer.compute_frontier(agents) -> List[ParetoPoint]`**
Compute the Pareto frontier (non-dominated solutions).

**`ParetoFrontierAnalyzer.rank_by_dominance(agents) -> Dict[int, List[ParetoPoint]]`**
Rank agents by dominance layers.

**`ParetoFrontierAnalyzer.get_knee_point(frontier) -> ParetoPoint`**
Best overall balance.

**`ParetoFrontierAnalyzer.compare_agents(a, b) -> Dict`**
Relationship + trade-offs.

**`ComplexityAnalyzer.analyze_from_trace(trace) -> TaskComplexity`**
Extract complexity from an execution trace.

**`ComplexityAnalyzer.categorize_complexity(complexity) -> str`**
Tier: `'trivial'`, `'simple'`, `'moderate'`, `'complex'`, `'extreme'`.

**`ComplexityAnalyzer.detect_over_reasoning(complexity) -> Dict`**
Returns `{over_reasoning, ratio, recommendation}`.

**`ComplexityAnalyzer.suggest_optimization(complexity) -> List[str]`**
Actionable recommendations.

### Enhanced API (additive, opt-in)

**`dominance_verdict(a, b, objectives, maximize, tolerance) -> DominanceVerdict`**
Structured dominance comparison with per-objective breakdown and XAI.

```python
from analysis.dominance_checker import dominance_verdict

v = dominance_verdict(
    {"energy": 5.0, "quality": 0.9},
    {"energy": 6.0, "quality": 0.85},
    objectives=("energy", "quality"),
    maximize={"quality"},
)
print(v.kind)        # "a_dominates_b"
print(v.rationale)   # ["A strictly better on 2 objective(s): ..."]
```

**`ParetoAnalyzer.compute_frontier_detailed() -> ParetoFrontierResult`**
Rich frontier with rationale, dominators, and HITL flag.

**`ExtendedParetoAnalyzer`**
7-dimensional Pareto with memory, circuit depth, and inference variance
as additional objectives.

**`GreenScoreCalculator.compute(...) -> GreenScoreResult`**
Green score with provenance, uncertainty, and XAI. Supports carbon and
helium terms (opt-in) and normalization.

**`ParetoVisualizer(x=..., y=..., color=...).render(df, frontier_labels=...)`**
Plotly visualization with frontier highlighting, column aliases, and
stats-in-title.

**`PolicyLoader.load(path) -> GreenPolicy`**
Load, validate, and version YAML policies with env var interpolation and
`extends` support.

**`RuntimeAdapter` / `AgentRuntime`**
The contract every runtime adapter satisfies so the analysis layer can
consume it uniformly.

### Statistics and Diagnostics

Every enhanced module exposes `get_statistics()`:

```python
analyzer = ParetoAnalyzer()
# ... use the analyzer ...
print(analyzer.get_statistics())
# {
#   "points_stored": 15,
#   "invalid_stored": 2,
#   "duplicates_skipped": 3,
#   "frontiers_computed": 4,
#   "carbon_in_frontier": True,
# }
```

---

## 🎯 Use Cases

### 1. Multi-Objective Agent Comparison

```python
analyzer = ParetoAnalyzer()
for agent_result in agent_results:
    analyzer.add_record(
        energy_joules=agent_result.energy_j,
        accuracy=agent_result.accuracy,
        carbon_grams=agent_result.carbon_g,
        label=agent_result.agent_id,
        source=agent_result.source,
    )

result = analyzer.compute_frontier_detailed()
```

### 2. Fair Cross-Complexity Comparison

```python
complexity_a = analyzer.analyze_from_trace(trace_a)
complexity_b = analyzer.analyze_from_trace(trace_b)

energy_eff_a = agent_a.energy_kwh / complexity_a.compute_composite_score()
energy_eff_b = agent_b.energy_kwh / complexity_b.compute_composite_score()
```

### 3. Over-Reasoning Detection

```python
for agent, trace in agent_traces.items():
    complexity = analyzer.analyze_from_trace(trace)
    result = analyzer.detect_over_reasoning(complexity)
    if result['over_reasoning']:
        print(f"⚠️ {agent}: {result['recommendation']}")
```

### 4. Decision Traceability

```python
from analysis import create_decision_record

record = create_decision_record(
    run_id="run-001",
    task_id="classification-42",
    selected_action="route_to_mobilenet",
    alternatives=["resnet50", "efficientnet"],
    quality_score=0.86,
    latency_ms=80,
    energy_kwh=0.001,
    carbon_operational_kg=0.0002,
    provenance={"source": "measured", "simulated": False},
)
```

### 5. Cinebench Classifier Evaluation

```python
classifiers = [
    ParetoPoint('ResNet50',     0.94, 0.008, 0.0016, 350),
    ParetoPoint('EfficientNet', 0.92, 0.003, 0.0006, 180),
    ParetoPoint('MobileNet',    0.86, 0.001, 0.0002, 80),
]

frontier = analyzer.compute_frontier(classifiers)
knee = analyzer.get_knee_point(frontier)
print(f"Deploy: {knee.agent_id} (best accuracy/energy balance)")
```

---

## 📊 Visualization

The enhanced `pareto_visualizer.py` uses Plotly and supports frontier
highlighting, column aliases, and stats-in-title.

```python
import pandas as pd
from analysis.pareto_visualizer import ParetoVisualizer

df = pd.DataFrame({
    "label": ["A", "B", "C", "D"],
    "latency": [1.0, 1.2, 0.8, 1.5],
    "energy_kwh": [0.05, 0.06, 0.10, 0.04],
    "carbon_kg": [0.02, 0.025, 0.04, 0.018],
})

viz = ParetoVisualizer(
    x="latency", y="energy_kwh", color="label",
    frontier_highlight=True,
)
fig = viz.render(df, frontier_labels=["A", "D"])
viz.save(fig, "pareto.html")
```

### Multiple 2D Projections

For 7-dimensional frontiers, use `plot_multi_projection()`:

```python
from analysis.pareto_visualizer import plot_multi_projection

figs = plot_multi_projection(
    df,
    projections=[
        ("latency", "energy_kwh"),
        ("latency", "carbon_kg"),
        ("energy_kwh", "carbon_kg"),
    ],
)
```

Each projection reveals a different trade-off.

---

## 🧪 Testing

```bash
# All tests
pytest tests/ -v

# Specific test modules
pytest tests/test_graph_metrics_exporter.py -v
pytest tests/test_graph_metrics_exporter_helium.py -v

# With coverage
pytest tests/ --cov=analysis --cov-report=html

# Async tests require pytest-asyncio
pytest tests/ -v --asyncio-mode=auto
```

---

## 🔄 Migration from Legacy to Enhanced

| Legacy | Enhanced | Change |
|---|---|---|
| `ParetoFrontierAnalyzer().compute_frontier(agents)` | `ParetoAnalyzer().compute_frontier_detailed()` | Returns `ParetoFrontierResult` instead of `List[ParetoPoint]` |
| `pareto_front(results)` — crashes on NaN | `pareto_front(results)` — skips NaN, flags as invalid | Strict improvement |
| `dominates(a, b, objectives)` — `KeyError` on missing keys | `dominates(a, b, objectives)` — returns `False` | Strict improvement |
| `GreenScoreCalculator().compute()` | `GreenScoreCalculator().compute()` returns `GreenScoreResult` | New |
| `ParetoVisualizer.plot(df)` — matplotlib workaround | `ParetoVisualizer().render(df)` — Plotly with frontier line | New |
| `SelfMonitor.last_trend()` — 2 keys | `last_trend()` — 5 deltas + 4 rates + classification | Additive |
| `MetricsStreamer.heartbeat(payload)` — silent | `heartbeat(payload)` — returns `bool` | Additive |

**Backward compatibility**: All legacy APIs are preserved. Enhanced
methods are additive. To restore the exact legacy behavior:

```python
analyzer = ParetoAnalyzer(
    tolerance=0.0,
    features={
        "validation": False,
        "carbon_in_frontier": False,
        "deduplication": False,
        "xai": False,
        "hitl": False,
    },
)
```

---

## 🎛️ Feature Toggles

Every enhanced module supports `features={...}`:

```python
analyzer = ParetoAnalyzer(
    features={
        "validation": True,
        "carbon_in_frontier": True,
        "provenance": True,
        "xai": True,
        "hitl": True,
        "statistics": True,
        "deduplication": True,
        "tolerance": True,
    },
)
```

To disable all enhancements and match legacy behavior:

```python
analyzer = ParetoAnalyzer(
    features={k: False for k in ParetoAnalyzer.DEFAULT_FEATURES},
)
```

---

## 📋 Priority Mapping

Modules are prioritized per the design proposal.

### Priority #1 — Reliable Telemetry and Provenance ✅ Stable

The nine telemetry modules. All stable. Recommended for production.

- `energy_meter`, `carbon_estimator`, `metrics_collector`,
  `execution_trace`, `framework_overhead`, `overhead_analyzer`,
  `metric_provenance`, `self_monitor`, `streaming`

### Priority #2 — Decision Explanations + Safety Evidence ✅ Stable

The XAI explainers and verification evidence store.

- `decision_explainer`, `pareto_analyzer.FrontierExplainer`,
  `verification_evidence`, `leaderboard`

### Priority #3 — Hardware-Aware Adaptive Precision ✅ Stable

- `precision_hardware_analyzer`, `energy_meter.PrecisionLevel`

### Priority #4 — Resilience/Chaos Evaluation ✅ Stable

- `chaos`, `resilience_analyzer`

### Priority #5 — Multi-Agent Coordination Analytics ✅ Stable

- `multiagent_role_analyzer`

### Priority #6 — Causal Policy Evaluation 🟡 Experimental

- `causal_policy_evaluator` — needs historical intervention data

### Priority #7 — Federated / Quantum / Carbon Markets 🟡 Experimental

- `federated_analytics`, `quantum_tradeoff_analyzer`,
  `carbon_instruments_ledger`

**Legend**: ✅ Stable — safe to depend on. 🟡 Experimental — API may change.

---

## 💡 Best Practices

### 1. Always use Pareto for multi-objective decisions

❌ **Don't** collapse to a single weighted score:
```python
score = 0.4 * accuracy + 0.3 * energy + 0.3 * carbon
best = max(agents, key=lambda a: score)
```

✅ **Do** reveal all valid options:
```python
result = analyzer.compute_frontier_detailed()
```

### 2. Check the `simulated` flag

```python
if record.simulated:
    logger.warning("Excluding simulated value from report")
    return
```

### 3. Keep operational and contractual carbon separate

```python
print(f"Operational: {record.carbon_operational_kg} kg")   # physical
print(f"Contractual: {record.carbon_contractual_kg} kg")   # RECs
# Never sum these
```

### 4. Use feature toggles during debugging

```python
analyzer = ParetoAnalyzer(
    features={"validation": False, "xai": False},  # isolate the issue
)
```

### 5. Emit `DecisionRecord`s for traceability

Every module can emit a `DecisionRecord`. This is the shared audit trail.

### 6. Persist and diff policies

```python
from analysis.policy_loader import PolicyLoader, PolicyDiff

loader = PolicyLoader()
policy = loader.load("policy.yaml")

# Later, on reload:
new_policy = loader.load("policy.yaml", force_reload=True)
diff = PolicyDiff.compare(policy, new_policy)
if diff.to_dict()["has_changes"]:
    logger.info(f"Policy changed: {diff.changed}")
```

### 7. Route critical events to HITL

```python
def approve(payload):
    # decide based on the payload
    return True

streamer.set_hitl_callback(approve)
# critical/emergency payloads now reach `approve`
```

---

## 🐛 Troubleshooting

### `ImportError: No module named 'psutil'`

```bash
pip install "psutil>=5.9,<7.0"
```

### `ImportError: No module named 'yaml'`

```bash
pip install "PyYAML>=6.0,<7.0"
```

### Empty Pareto frontier

```python
if not result.points:
    logger.error("No agents on frontier — check data validity")
    for error in result.invalid_count:
        logger.error(f"  {error}")
```

### All agents on frontier

```python
if len(result.points) == len(analyzer.points):
    logger.info("All agents non-comparable — significant trade-offs exist")
```

### `TrendResult.needs_review` is True

```python
if trend.needs_review:
    logger.warning(f"Trend flagged: {trend.review_reason}")
    # Optionally route to HITL
```

### Circuit breaker open

```python
if analyzer._circuit.state.value == "open":
    logger.warning("Circuit open — using degraded mode")
    # Wait for recovery timeout, or reset manually if safe
```

### `MetricsStreamer` sink raises

```python
stats = streamer.get_statistics()
if stats["sink_errors"] > 0:
    logger.error(f"Sink errors: {stats['sink_errors']}")
    # Sink errors are contained by default; replace the sink:
    streamer.set_sink(my_new_sink)
```

### Provenance verification fails

```python
prov = get_provenance(metrics, "energy")
if prov is None or prov.source_kind == "unknown":
    logger.warning("Energy metric has unknown provenance")
```

### `KeyError` on missing metric

Enhanced modules handle missing metrics gracefully:
- `dominates()` returns `False`
- `pareto_front()` skips and counts as invalid
- `attach_provenance()` labels the metric `"unknown"`

If you are seeing a `KeyError`, you are likely using a legacy module
directly. Migrate to the enhanced API.

---

## 📚 Further Reading

- **Design proposal** — the reference architecture for this folder
- **`requirements_analysis.txt`** — complete dependency list
- **Inline docstrings** — every module documents its own feature flags
  and contracts
- **`examples/`** — runnable demonstrations

---

## 📄 License

Part of Green_Agent — Apache 2.0 License.

---

## 🤝 Contributing

See the main Green_Agent `CONTRIBUTING.md`.

Guidelines for adding a new analysis module:

1. **Emit a shared contract.** If your module produces a result, wrap it
   in a `dataclass` that is documented in the [Shared Contracts](#-shared-contracts)
   section above.
2. **Support feature toggles.** Declare `DEFAULT_FEATURES` and honour
   `features={...}`.
3. **Attach provenance.** Include `source`, `simulated`, and (where
   applicable) `uncertainty` on every result.
4. **Provide `get_statistics()`.** Cumulative counters for diagnostics.
5. **Never raise on malformed input.** Return a degraded result with a
   clear `review_reason`.
6. **Test with the enhanced fixtures.** All enhanced modules are
   covered by `tests/`; add your tests there.

---

## 📧 Support

- **Issues**: GitHub Issues
- **Docs**: This README + inline docstrings
- **Examples**: `examples/`
- **Tests**: `tests/`


This is the complete `README.md` file for the `analysis/` folder. It can be saved directly as `analysis/README.md`. Every section from the original README is preserved, and the enhanced architecture — shared contracts, the ten enhancement layers, the nine-module telemetry stack, feature toggles, migration guidance, and the priority map — is fully documented.
