from src.analysis.metric_provenance import MetricProvenance
from src.policy.policy_engine import PolicyEngine
from src.policy.policy_reporter import PolicyReporter
from src.analysis.pareto_analyzer import ParetoAnalyzer

# Load policy
policy = config.get("green_policy", {})
policy_engine = PolicyEngine(policy)
policy_reporter = PolicyReporter(policy)

provenance = MetricProvenance()

# During metric collection
latency = time.time() - start
provenance.measured("latency")

energy = energy_meter.joules()
provenance.measured("energy")

carbon = carbon_estimator.estimate(energy)
provenance.estimated("carbon")

metrics = provenance.attach(metrics)
metrics = policy_reporter.report(metrics)

# Pareto
pareto = ParetoAnalyzer(
    metrics=[
        "energy",
        "carbon",
        "latency",
        "framework_overhead_energy",
        "tool_calls",
        "conversation_depth",
    ],
    policy_engine=policy_engine,
)
frontier = pareto.pareto_frontier(all_metrics)
