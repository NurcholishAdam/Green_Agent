from prometheus_client import Counter, Histogram, Gauge

expert_invocations = Counter(
    'expert_invocations_total',
    'Total expert invocations',
    ['expert_type', 'domain']
)

expert_energy = Histogram(
    'expert_energy_wh',
    'Energy consumed by experts',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0]
)

# Enhanced metrics
modp_score_gauge = Gauge('green_agent_modp_score', 'MODP composite score')
rlhf_feedback_gauge = Gauge('green_agent_rlhf_feedback', 'RLHF human feedback score')
graph_centrality_gauge = Gauge('green_agent_graph_centrality', 'LIMIT Graph centrality')
