"""End-to-end deployment tests (Enhanced)

Includes placeholders for actual deployment flow, plus optional tests for
advanced enhancement modules: LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy
Distillation, Bio‑inspired Optimisation, MoE expert gating, and FlexGen.
"""

import pytest
import asyncio
import os
import importlib

# ------------------------------------------------------------------------------
# Original placeholder tests
# ------------------------------------------------------------------------------

@pytest.mark.e2e
def test_deployment_e2e():
    """End-to-end deployment test"""
    # This is a placeholder for actual e2e tests
    # In a real scenario, this would test the entire deployment flow
    print("✅ E2E test placeholder - deployment flow")
    assert True

@pytest.mark.e2e
def test_service_endpoints():
    """Test that service endpoints are accessible"""
    # Placeholder for service endpoint tests
    print("✅ E2E test placeholder - service endpoints")
    assert True

@pytest.mark.e2e
def test_carbon_tracking():
    """Test that carbon tracking is working"""
    # Placeholder for carbon tracking tests
    print("✅ E2E test placeholder - carbon tracking")
    assert True


# ------------------------------------------------------------------------------
# Enhanced tests for advanced modules (optional)
# ------------------------------------------------------------------------------

ENHANCEMENTS_PATH = "src.enhancements"


def _enhancement_module_available(module_path: str, class_name: str = None) -> bool:
    """Check if a specific module/class from enhancements is importable."""
    try:
        module = importlib.import_module(module_path)
        if class_name and not hasattr(module, class_name):
            return False
        return True
    except ImportError:
        return False


@pytest.mark.e2e
@pytest.mark.skipif(
    not _enhancement_module_available("src.enhancements.schemas.node_descriptor", "NodeDescriptor"),
    reason="NodeDescriptor not available"
)
def test_node_descriptor_routing_e2e():
    """End-to-end: NodeDescriptor selects routing strategy using distillation."""
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType

    node = NodeDescriptor(
        id="e2e_node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=350.0,
        energy_per_token=0.00004,
        use_evolutionary=True,
        human_feedback_score=0.7,
        graph_metrics={"centrality": 0.8}
    )
    strategy = asyncio.run(node.select_routing_strategy(exploration=False))
    assert strategy in ["carbon_first", "latency_first", "cost_first", "balanced", "adaptive"]
    print(f"✅ E2E NodeDescriptor routing: {strategy}")


@pytest.mark.e2e
@pytest.mark.skipif(
    not _enhancement_module_available("src.enhancements.schemas.workload_descriptor", "WorkloadDescriptor"),
    reason="WorkloadDescriptor not available"
)
def test_workload_descriptor_priority_e2e():
    """End-to-end: WorkloadDescriptor selects priority using MODP + MoE."""
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency

    wl = WorkloadDescriptor(
        task_id="e2e_task",
        task_type=TaskType.INFERENCE,
        tokens=1000,
        latency_target=300.0,
        urgency=Urgency.MEDIUM,
        use_evolutionary=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7}
    )
    priority = asyncio.run(wl.select_priority(exploration=False))
    assert priority in ["accuracy", "green", "balanced"]
    print(f"✅ E2E WorkloadDescriptor priority: {priority}")


@pytest.mark.e2e
@pytest.mark.skipif(
    not _enhancement_module_available("src.enhancements.schemas.feedback_event", "FeedbackEvent"),
    reason="FeedbackEvent not available"
)
def test_feedback_event_enhanced_fields_e2e():
    """End-to-end: FeedbackEvent retains MODP, RLHF, and graph metrics."""
    from src.enhancements.schemas.feedback_event import FeedbackEvent

    event = FeedbackEvent(
        source="e2e",
        feedback_type="routing",
        task_id="task_e2e",
        context={},
        action={"selected_action": "execute"},
        performance={"quality_score": 0.9, "latency_ms": 100, "energy_joules": 100},
        adaptive_cost_value=0.85,
        graph_metrics={"centrality": 0.7},
        human_feedback_score=0.8,
        modp_score=0.75,
        distillation_stats={"student_counter": 5}
    )
    json_str = event.to_json()
    restored = FeedbackEvent.from_json(json_str)
    assert restored.graph_metrics["centrality"] == 0.7
    assert restored.human_feedback_score == 0.8
    assert restored.modp_score == 0.75
    print("✅ E2E FeedbackEvent with enhanced fields")


@pytest.mark.e2e
@pytest.mark.skipif(
    not _enhancement_module_available("src.enhancements.zero_trust_architecture", "ZeroTrustArchitecture"),
    reason="ZeroTrustArchitecture not available"
)
def test_zero_trust_init_e2e():
    """End-to-end: ZeroTrustArchitecture initialises with enhanced flags."""
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig

    config = ZeroTrustConfig(
        use_enhancements=True,
        use_distillation=True,
        use_evolutionary=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.5}
    )
    zta = ZeroTrustArchitecture(config=config)
    assert zta.use_enhancements is True
    assert hasattr(zta, 'carbon_authenticator') or hasattr(zta, 'distillation_optimizer')
    print("✅ E2E ZeroTrustArchitecture with enhancements")


@pytest.mark.e2e
@pytest.mark.skipif(
    not _enhancement_module_available("src.enhancements.core.graph_registry", "GraphRegistry"),
    reason="GraphRegistry not available"
)
def test_graph_registry_and_causal_e2e():
    """End-to-end: LIMIT Graph operations and root-cause diagnosis."""
    from src.enhancements.core.graph_registry import GraphRegistry, GraphType
    from src.enhancements.core.causal_graph import CausalGraph
    from src.enhancements.core.meta_cognition import MetaCognitionLayer

    registry = GraphRegistry()
    causal_graph = registry.get_or_create(GraphType.CAUSAL)
    assert isinstance(causal_graph, CausalGraph)

    meta = MetaCognitionLayer(causal_graph=causal_graph)
    meta.observe_snapshot({
        "CarbonIntensity": 430.0,
        "CarbonIntensity_high": 400.0,
        "GridStrain": 0.91,
    })
    report = meta.diagnose()
    assert report["status"] == "anomaly_detected"
    print("✅ E2E LIMIT Graph + MetaCognition")


@pytest.mark.e2e
@pytest.mark.skipif(
    not _enhancement_module_available("src.enhancements.metrics.dag_carbon_ledger", "DAGCarbonLedger"),
    reason="DAGCarbonLedger not available"
)
def test_dag_carbon_ledger_backpropagation_e2e():
    """End-to-end: DAG carbon ledger backpropagation."""
    from src.enhancements.metrics.dag_carbon_ledger import DAGCarbonLedger

    ledger = DAGCarbonLedger(storage_path="/tmp/e2e_ledger")
    node_a = ledger.add_execution(
        task_id="A", framework="langchain", energy_kwh=0.01,
        carbon_co2e_kg=0.002, accuracy=0.9, sustainability_index=0.8
    )
    node_b = ledger.add_execution(
        task_id="B", framework="langchain", energy_kwh=0.008,
        carbon_co2e_kg=0.0016, accuracy=0.88, sustainability_index=0.75,
        parent_task_ids=[node_a], dependency_type="model_state"
    )
    attributed = ledger.backpropagate_carbon(node_b, transfer_rate=0.3)
    assert node_a in attributed
    assert attributed[node_a] > 0
    print("✅ E2E DAG Carbon Ledger backpropagation")


@pytest.mark.e2e
def test_flexgen_config_from_env_e2e():
    """End-to-end: FlexGen settings can be read from environment or default."""
    # We don't require FlexGen to be enabled; just check that config can be parsed.
    flexgen_enabled = os.getenv("FLEXGEN_ENABLED", "false").lower() == "true"
    model_name = os.getenv("FLEXGEN_MODEL_NAME", "facebook/opt-6.7b")
    precision = os.getenv("FLEXGEN_DEFAULT_PRECISION", "fp16")
    assert model_name.startswith("facebook/") or model_name.startswith("meta-llama/")
    assert precision in ["fp32", "fp16", "int8"]
    print(f"✅ E2E FlexGen config (enabled={flexgen_enabled}, model={model_name}, precision={precision})")
