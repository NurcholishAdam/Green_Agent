"""Basic test to verify setup, including advanced enhancement modules"""

import importlib
import asyncio
import pytest

def test_basic():
    """Verify testing works"""
    assert True
    print("✅ Tests are working!")


# ------------------------------------------------------------------------------
# Optional tests for advanced enhancement modules
# ------------------------------------------------------------------------------

def _module_available(module_path: str, class_name: str = None) -> bool:
    """Check if a module from src/enhancements is importable."""
    try:
        module = importlib.import_module(module_path)
        if class_name and not hasattr(module, class_name):
            return False
        return True
    except ImportError:
        return False


def test_enhanced_schemas_exist():
    """Verify that advanced schema files are present in the enhancements folder."""
    import os
    enhancements_dir = os.path.join(
        "quantum_integration", "quantum-limit-graph-v2.4.0",
        "limit-agentbench", "src", "enhancements"
    )
    if not os.path.isdir(enhancements_dir):
        pytest.skip("Enhancements folder not found; skipping advanced module check.")
    required_files = [
        "schemas/feedback_event.py",
        "schemas/node_descriptor.py",
        "schemas/workload_descriptor.py",
        "zero_trust_architecture.py",
        "core/graph_registry.py",
        "core/causal_graph.py",
        "core/meta_cognition.py",
        "metrics/dag_carbon_ledger.py",
    ]
    for rel_path in required_files:
        full_path = os.path.join(enhancements_dir, rel_path)
        assert os.path.exists(full_path), f"Missing required enhancement file: {rel_path}"
    print("✅ All required enhancement files are present.")


@pytest.mark.skipif(
    not _module_available("src.enhancements.schemas.node_descriptor", "NodeDescriptor"),
    reason="NodeDescriptor not available"
)
def test_node_descriptor_routing():
    """NodeDescriptor should select a routing strategy using distillation."""
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType

    node = NodeDescriptor(
        id="test_node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=400.0,
        energy_per_token=0.00005,
        use_evolutionary=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.8}
    )
    strategy = asyncio.run(node.select_routing_strategy(exploration=False))
    assert strategy in ["carbon_first", "latency_first", "cost_first", "balanced", "adaptive"]
    print(f"✅ Routing strategy: {strategy}")


@pytest.mark.skipif(
    not _module_available("src.enhancements.schemas.workload_descriptor", "WorkloadDescriptor"),
    reason="WorkloadDescriptor not available"
)
def test_workload_descriptor_priority():
    """WorkloadDescriptor should select priority using MODP + MoE."""
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency

    wl = WorkloadDescriptor(
        task_id="test_task",
        task_type=TaskType.INFERENCE,
        tokens=1000,
        latency_target=300.0,
        urgency=Urgency.MEDIUM,
        use_evolutionary=True,
        human_feedback_score=0.7,
        graph_metrics={"centrality": 0.6}
    )
    priority = asyncio.run(wl.select_priority(exploration=False))
    assert priority in ["accuracy", "green", "balanced"]
    print(f"✅ Priority: {priority}")


@pytest.mark.skipif(
    not _module_available("src.enhancements.schemas.feedback_event", "FeedbackEvent"),
    reason="FeedbackEvent not available"
)
def test_feedback_event_enhanced_fields():
    """FeedbackEvent should support MODP, RLHF, and LIMIT Graph fields."""
    from src.enhancements.schemas.feedback_event import FeedbackEvent

    event = FeedbackEvent(
        source="test",
        feedback_type="routing",
        task_id="t1",
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
    event2 = FeedbackEvent.from_json(json_str)
    assert event2.graph_metrics["centrality"] == 0.7
    assert event2.human_feedback_score == 0.8
    assert event2.modp_score == 0.75
    print("✅ FeedbackEvent with enhanced fields works.")


@pytest.mark.skipif(
    not _module_available("src.enhancements.zero_trust_architecture", "ZeroTrustArchitecture"),
    reason="ZeroTrustArchitecture not available"
)
def test_zero_trust_enhanced_init():
    """ZeroTrustArchitecture should initialize with enhanced flags."""
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
    print("✅ ZeroTrustArchitecture initialized with enhancements.")


@pytest.mark.skipif(
    not _module_available("src.enhancements.core.graph_registry", "GraphRegistry"),
    reason="GraphRegistry not available"
)
def test_graph_registry_and_causal_graph():
    """GraphRegistry and CausalGraph should support LIMIT Graph operations."""
    from src.enhancements.core.graph_registry import GraphRegistry, GraphType
    from src.enhancements.core.causal_graph import CausalGraph

    registry = GraphRegistry()
    causal_graph = registry.get_or_create(GraphType.CAUSAL)
    assert isinstance(causal_graph, CausalGraph)

    from src.enhancements.core.meta_cognition import MetaCognitionLayer
    meta = MetaCognitionLayer(causal_graph=causal_graph)
    meta.observe_snapshot({
        "CarbonIntensity": 430.0,
        "CarbonIntensity_high": 400.0,
        "GridStrain": 0.91,
    })
    report = meta.diagnose()
    assert report["status"] == "anomaly_detected"
    print("✅ LIMIT Graph, CausalGraph, and MetaCognition work.")


@pytest.mark.skipif(
    not _module_available("src.enhancements.metrics.dag_carbon_ledger", "DAGCarbonLedger"),
    reason="DAGCarbonLedger not available"
)
def test_dag_carbon_ledger_backpropagation():
    """DAGCarbonLedger should support carbon debt backpropagation."""
    from src.enhancements.metrics.dag_carbon_ledger import DAGCarbonLedger

    ledger = DAGCarbonLedger(storage_path="/tmp/test_ledger")
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
    print("✅ DAG Carbon Ledger backpropagation works.")


@pytest.mark.skipif(
    not _module_available("src.enhancements.schemas.node_descriptor", "NodeDescriptor"),
    reason="NodeDescriptor not available for FlexGen config check"
)
def test_flexgen_config_presence():
    """FlexGen integration settings should be present in enhancement config or defaults."""
    # This is a placeholder; we can verify that FlexGen-related attributes exist
    # if the enhanced config includes them. We'll just check that the NodeDescriptor
    # can accept metadata for FlexGen.
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

    node = NodeDescriptor(
        id="flexgen_node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=400.0,
        energy_per_token=0.00005,
        metadata={"flexgen_enabled": True, "flexgen_model": "facebook/opt-6.7b"}
    )
    assert node.metadata.get("flexgen_enabled") is True
    print("✅ FlexGen configuration placeholder works.")
