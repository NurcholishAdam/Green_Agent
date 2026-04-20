"""
Integration Demo — All 5 Enhancement Phases
=============================================
Demonstrates every enhancement running in the correct implementation order:

  Phase 1  →  Graph Similarity Store + Prior retrieval
  Phase 2  →  Causal Graph + Meta-Cognition Layer
  Phase 3  →  DAG Carbon Ledger + Backpropagation
  Phase 4  →  Policy Graph decision engine
  Phase 5  →  Dual-Graph Evaluator + XAI output

Run:
    cd green_agent_enhanced
    python demo_enhanced_green_agent.py

Dependencies: stdlib only (no pip installs required).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metrics.graph_similarity_store import GraphSimilarityStore
from core.similarity_benchmark import build_execution_subgraph, get_benchmark_prior
from core.causal_graph import CausalGraph
from core.meta_cognition import MetaCognitionLayer
from metrics.dag_carbon_ledger import DAGCarbonLedger
from core.policy_graph import PolicyGraph
from core.dual_graph_evaluator import (
    DualGraphEvaluator, DecisionGraph, DecisionNode
)

DIVIDER = "─" * 70


def section(title: str):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def pretty(obj, indent: int = 2):
    import json
    print(json.dumps(obj, indent=indent, default=str))


# ===========================================================================
# PHASE 1 — Graph Similarity Store
# ===========================================================================

def phase_1_demo():
    section("PHASE 1 — Graph Similarity Store (context-aware prior)")

    store = GraphSimilarityStore(storage_path="/tmp/green_demo/graph_store")

    # --- Seed with 3 historical executions ---
    historical = [
        {
            "task": {"task_type": "question_answering", "suite": "agentbench_qa"},
            "hw": "nvidia_a100", "grid": "US-CA", "fw": "langchain",
            "metrics": {"accuracy": 0.91, "energy_kwh": 0.0032,
                        "carbon_co2e_kg": 0.00064, "sustainability_index": 0.78},
        },
        {
            "task": {"task_type": "question_answering", "suite": "agentbench_qa"},
            "hw": "nvidia_a100", "grid": "US-CA", "fw": "autogen",
            "metrics": {"accuracy": 0.88, "energy_kwh": 0.0041,
                        "carbon_co2e_kg": 0.00082, "sustainability_index": 0.71},
        },
        {
            "task": {"task_type": "code_generation", "suite": "agentbench_code"},
            "hw": "nvidia_rtx_4090", "grid": "EU-DE", "fw": "langchain",
            "metrics": {"accuracy": 0.76, "energy_kwh": 0.0065,
                        "carbon_co2e_kg": 0.00228, "sustainability_index": 0.61},
        },
    ]
    for h in historical:
        sg = build_execution_subgraph(h["task"], h["hw"], h["grid"], h["fw"], h["metrics"])
        store.store_execution(sg)

    print("Seeded 3 historical executions into the graph store.")

    # --- Query prior for a new incoming task ---
    new_task = {"task_type": "question_answering", "suite": "agentbench_qa"}
    result = get_benchmark_prior(store, new_task, "nvidia_a100", "US-CA", "crewai")

    print("\nPrior for new task (question_answering / nvidia_a100 / US-CA / crewai):")
    pretty(result["prior"])
    print("\nTop similar historical executions:")
    for sim in result["similar_executions"]:
        print(f"  similarity={sim['similarity_score']:.3f}  "
              f"SI={sim['outcome_metrics']['sustainability_index']:.2f}  "
              f"fw={sim['framework']}")


# ===========================================================================
# PHASE 2 — Causal Graph + Meta-Cognition
# ===========================================================================

def phase_2_demo():
    section("PHASE 2 — Causal Graph Meta-Cognition (root-cause attribution)")

    causal_graph = CausalGraph()
    meta = MetaCognitionLayer(causal_graph=causal_graph)

    # Simulate a runtime state where carbon is spiking
    snapshot = {
        "WeatherEvent": 1.0,            # storm detected (binary signal)
        "WeatherEvent_high": 0.5,
        "RenewableShortfall": 0.85,
        "RenewableShortfall_high": 0.70,
        "GridStrain": 0.91,
        "GridStrain_high": 0.80,
        "CarbonIntensity": 430.0,
        "CarbonIntensity_high": 400.0,
        "BatteryLevel": 0.18,
        "BatteryLevel_low": 0.25,
        "TaskPriority": 0.9,            # high-priority task — should it still defer?
    }

    meta.observe_snapshot(snapshot)
    report = meta.diagnose()

    print(f"\nStatus: {report['status']}")
    print(f"Anomalies detected: {report['anomalies']}")
    print(f"Recommended action: {report['recommended_action']}")
    print("\nTop causal chain:")
    if report["root_causes"]:
        top = report["root_causes"][0]
        print(f"  Root cause: {top['root_cause']}")
        print(f"  Path: {' → '.join(top['path'])}")
        print(f"  Labels: {' → '.join(top['path_labels'])}")
        print(f"  Cumulative weight: {top['cumulative_weight']}")

    # Simulate feedback: the recommended defer was actually WRONG (false positive)
    meta.feedback(decision_was_correct=False)
    print("\nFeedback applied (defer was wrong — grid strain was false positive).")
    print("Edge GridStrain → DeferralSignal weight reduced.")

    # Show updated edge weight
    for edge in causal_graph.edges:
        if edge.source_id == "GridStrain" and edge.target_id == "DeferralSignal":
            print(f"  Updated weight: {edge.weight:.4f} (was 0.65)")


# ===========================================================================
# PHASE 3 — DAG Carbon Ledger + Backpropagation
# ===========================================================================

def phase_3_demo():
    section("PHASE 3 — DAG Carbon Ledger (carbon backpropagation)")

    ledger = DAGCarbonLedger(storage_path="/tmp/green_demo/dag_ledger")

    # Task A — root task, triggered a model reload
    node_a = ledger.add_execution(
        task_id="task_A_model_reload",
        framework="langchain",
        energy_kwh=0.012,
        carbon_co2e_kg=0.0024,
        accuracy=0.92,
        sustainability_index=0.65,
        metadata={"note": "triggered full model reload"},
    )

    # Task B — model_state dependency on A (inherits non-quantized state)
    node_b = ledger.add_execution(
        task_id="task_B_non_quantized",
        framework="langchain",
        energy_kwh=0.009,
        carbon_co2e_kg=0.0018,
        accuracy=0.89,
        sustainability_index=0.70,
        parent_task_ids=[node_a],
        dependency_type="model_state",
    )

    # Task C — sequential dependency on B
    node_c = ledger.add_execution(
        task_id="task_C_downstream",
        framework="langchain",
        energy_kwh=0.005,
        carbon_co2e_kg=0.0010,
        accuracy=0.94,
        sustainability_index=0.82,
        parent_task_ids=[node_b],
        dependency_type="sequential",
    )

    print(f"DAG created: A → B → C")
    print(f"  node_A id (root): {node_a}")
    print(f"  node_B id:        {node_b}")
    print(f"  node_C id (leaf): {node_c}")

    # Run backpropagation from C upstream
    attributed = ledger.backpropagate_carbon(node_c, transfer_rate=0.30)
    print("\nCarbon backpropagation from Task C (transfer_rate=0.30):")
    for nid, carbon in attributed.items():
        node = ledger.nodes[nid]
        print(f"  {node.task_id}: attributed {carbon:.8f} kg CO2e")

    print("\nFull lineage of Task C (root → leaf):")
    for entry in ledger.get_lineage(node_c):
        print(f"  {entry['task_id']:30s}  "
              f"direct={entry['direct_carbon_co2e_kg']:.6f}  "
              f"inherited={entry['inherited_carbon_debt']:.6f}  "
              f"total={entry['total_attributed_carbon']:.6f}")

    print("\nLedger summary:")
    pretty(ledger.get_summary())


# ===========================================================================
# PHASE 4 — Policy Graph Decision Engine
# ===========================================================================

def phase_4_demo():
    section("PHASE 4 — Policy Graph Engine (multi-hop fuzzy decision)")

    pg = PolicyGraph()

    # Scenario A: carbon=390, battery=HIGH, priority=LOW → should still Execute
    ctx_a = {"carbon_g_per_kwh": 390, "battery_pct": 0.85,
              "task_priority": 0.2, "zone": "yellow", "queue_depth": 15}
    result_a = pg.decide(ctx_a)
    print(f"\nScenario A (carbon=390, battery=85%, priority=low, zone=yellow)")
    print(f"  Decision: {result_a['decision'].upper()}")
    print(f"  Score:    {result_a['score']}")
    print(f"  All scores: {result_a['all_scores']}")
    print(f"  Reasoning: {result_a['reasoning']}")

    # Scenario B: same carbon, but battery=LOW → should Defer
    ctx_b = {"carbon_g_per_kwh": 390, "battery_pct": 0.15,
              "task_priority": 0.2, "zone": "yellow", "queue_depth": 90}
    result_b = pg.decide(ctx_b)
    print(f"\nScenario B (carbon=390, battery=15%, priority=low, zone=yellow, queue=90)")
    print(f"  Decision: {result_b['decision'].upper()}")
    print(f"  Score:    {result_b['score']}")
    print(f"  All scores: {result_b['all_scores']}")
    print(f"  Reasoning: {result_b['reasoning']}")

    # Scenario C: carbon=500+, critical priority → should Execute despite high carbon
    ctx_c = {"carbon_g_per_kwh": 520, "battery_pct": 0.90,
              "task_priority": 0.95, "zone": "red", "queue_depth": 5}
    result_c = pg.decide(ctx_c)
    print(f"\nScenario C (carbon=520, battery=90%, priority=CRITICAL, zone=red)")
    print(f"  Decision: {result_c['decision'].upper()}")
    print(f"  Score:    {result_c['score']}")
    print(f"  All scores: {result_c['all_scores']}")
    print(f"  Reasoning: {result_c['reasoning']}")

    # Online learning: scenario A outcome was correct (execute was right)
    if result_a["winning_path"]:
        path = result_a["winning_path"]
        for i in range(len(path) - 1):
            pg.update_edge_weight(path[i], path[i + 1],
                                  decision_was_correct=True)
    print("\nOnline learning applied for Scenario A (execute was correct).")


# ===========================================================================
# PHASE 5 — Dual-Graph AI Evaluator
# ===========================================================================

def phase_5_demo():
    section("PHASE 5 — Dual-Graph Evaluator (GED + XAI)")

    evaluator = DualGraphEvaluator()

    # Register the ideal path for "question_answering" tasks
    evaluator.build_ideal_from_sequence(
        task_type="question_answering",
        action_sequence=["quantize", "execute"],  # optimal: quantize then run
    )

    # Graph A: what the agent actually did (wrong — deferred unnecessarily)
    graph_a = DecisionGraph(graph_id="run_001", graph_type="executor")
    step1 = DecisionNode(node_id="s1", action="quantize",
                         context_snapshot={"carbon": 390, "battery": 0.85})
    step2 = DecisionNode(node_id="s2", action="defer",        # WRONG: should execute
                         context_snapshot={"carbon": 390, "battery": 0.85},
                         outcome_delta_si=-0.12)
    graph_a.add_step(step1)
    graph_a.add_step(step2, after_id="s1")

    result = evaluator.evaluate(graph_a, task_type="question_answering")

    print(f"\nVerdict: {result['verdict'].upper()}")
    print(f"GED score: {result['ged_score']}  (normalised: {result['normalized_ged']})")
    print(f"\nXAI Explanation:")
    print(result["xai_explanation"])
    print(f"\nCorrective actions:")
    for ca in result["corrective_actions"]:
        print(f"  • {ca}")

    # ---- Second scenario: fully optimal path ----
    graph_optimal = DecisionGraph(graph_id="run_002", graph_type="executor")
    graph_optimal.add_step(DecisionNode(node_id="o1", action="quantize"))
    graph_optimal.add_step(DecisionNode(node_id="o2", action="execute"), after_id="o1")

    result2 = evaluator.evaluate(graph_optimal, task_type="question_answering")
    print(f"\n--- Optimal agent run ---")
    print(f"Verdict: {result2['verdict'].upper()}")
    print(f"XAI: {result2['xai_explanation']}")


# ===========================================================================
# Main
# ===========================================================================

if __name__ == "__main__":
    import tempfile
    os.makedirs("/tmp/green_demo", exist_ok=True)

    phase_1_demo()
    phase_2_demo()
    phase_3_demo()
    phase_4_demo()
    phase_5_demo()

    print(f"\n{DIVIDER}")
    print("  All 5 phases completed successfully.")
    print(DIVIDER)
