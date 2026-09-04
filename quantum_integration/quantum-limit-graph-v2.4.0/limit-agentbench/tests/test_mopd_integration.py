import pytest
import torch
from unittest.mock import AsyncMock, patch, MagicMock
from torch.utils.data import DataLoader

from quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench.src.enhancements.distillation_orchestrator import DistillationOrchestrator

@pytest.mark.asyncio
async def test_orchestrator_reports_to_adaptive_and_enhancements():
    """
    Integration test: DistillationOrchestrator should call adaptive.record_feedback in-process,
    and (if enhancements are enabled) expose MODP, MoE, RLHF, and LIMIT Graph metrics.
    """
    # Small student and teacher models
    student = torch.nn.Linear(10, 5)
    t1 = torch.nn.Linear(10, 5)
    t2 = torch.nn.Linear(10, 5)
    teachers = {'t1': t1, 't2': t2}

    # Minimal config: single epoch to keep test fast.
    # Enable enhanced features to exercise additional code paths.
    config = {
        'num_epochs': 1,
        'batch_size': 2,
        'mixed_precision': False,
        # Enhancement flags
        'use_enhancements': True,
        'use_moe_gating': True,
        'use_rlhf': True,
        'graph_metrics': {'centrality': 0.7, 'connectivity': 0.6},
        'human_feedback_score': 0.8,
        'modp_weights': [0.4, 0.3, 0.2, 0.1],
        'flexgen': {'enabled': False},  # FLexGen not used in this test
    }

    # Mock adaptive function with AsyncMock.record_feedback
    adaptive = AsyncMock()
    adaptive.record_feedback = AsyncMock()

    # Create a tiny dataloader that yields (inputs, labels, domain)
    X = torch.randn(4, 10)
    y = torch.randint(0, 5, (4,))
    domains = ['general'] * 4
    dataset = [(X[i], y[i], domains[i]) for i in range(4)]
    loader = DataLoader(dataset, batch_size=2)

    orchestrator = DistillationOrchestrator(
        student,
        teachers,
        config,
        adaptive_function_instance=adaptive
    )

    # Run distillation
    result = await orchestrator.distill(loader)

    # --- Core assertion: record_feedback was called at least twice (one per teacher) ---
    assert adaptive.record_feedback.call_count >= 2

    # --- Enhanced assertions (if the orchestrator returns these) ---
    if isinstance(result, dict):
        # MODP score should be present and within [0,1]
        if 'modp_score' in result:
            modp = result['modp_score']
            assert 0.0 <= modp <= 1.0, f"MODP score {modp} out of range"

        # MoE gating weights should be a list/array of positive numbers summing to 1 (per expert)
        if 'moe_gate_weights' in result:
            gate_weights = result['moe_gate_weights']
            assert isinstance(gate_weights, (list, tuple, torch.Tensor)), "MoE weights not a sequence"
            # Sum over experts for a single instance (assume shape [n_experts])
            if hasattr(gate_weights, 'sum'):
                total = float(gate_weights.sum())
            else:
                total = sum(gate_weights)
            assert abs(total - 1.0) < 1e-4, f"Gate weights sum to {total}, expected 1.0"

        # Graph metrics should be echoed back if used
        if 'graph_metrics' in result:
            assert result['graph_metrics'] == config['graph_metrics']

        # Human feedback should be reflected in the result or in feedback calls
        if 'human_feedback_score' in result:
            assert result['human_feedback_score'] == config['human_feedback_score']

    # Optionally inspect that feedback calls contain enhancement context
    # (This depends on the orchestrator's design; we make a soft check)
    for call in adaptive.record_feedback.call_args_list:
        args, kwargs = call
        # If the orchestrator passes kwargs like human_feedback_score and graph_metrics, validate them
        if 'human_feedback_score' in kwargs:
            assert kwargs['human_feedback_score'] == config['human_feedback_score']
        if 'graph_metrics' in kwargs:
            assert kwargs['graph_metrics'] == config['graph_metrics']
        if 'teacher_id' in kwargs:
            assert isinstance(kwargs['teacher_id'], str)
        # The first positional arg might be a dict containing enhancement info; not checked here
