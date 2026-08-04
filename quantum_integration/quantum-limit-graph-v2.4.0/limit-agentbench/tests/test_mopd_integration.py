import pytest
import torch
from unittest.mock import AsyncMock
from torch.utils.data import DataLoader

from quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench.src.enhancements.distillation_orchestrator import DistillationOrchestrator

@pytest.mark.asyncio
async def test_orchestrator_reports_to_adaptive():
    """Integration test: DistillationOrchestrator should call adaptive.record_feedback in-process."""
    # Small student and teacher models
    student = torch.nn.Linear(10, 5)
    t1 = torch.nn.Linear(10, 5)
    t2 = torch.nn.Linear(10, 5)
    teachers = {'t1': t1, 't2': t2}

    # Minimal config: single epoch to keep test fast
    config = {'num_epochs': 1, 'batch_size': 2, 'mixed_precision': False}

    # Mock adaptive function with AsyncMock.record_feedback
    adaptive = AsyncMock()
    adaptive.record_feedback = AsyncMock()

    # Create a tiny dataloader that yields (inputs, labels, domain)
    X = torch.randn(4, 10)
    y = torch.randint(0, 5, (4,))
    domains = ['general'] * 4
    dataset = [(X[i], y[i], domains[i]) for i in range(4)]
    loader = DataLoader(dataset, batch_size=2)

    orchestrator = DistillationOrchestrator(student, teachers, config, adaptive_function_instance=adaptive)

    # Run distillation (should invoke adaptive.record_feedback once per teacher used)
    result = await orchestrator.distill(loader)

    # We expect record_feedback to have been called for each teacher used in the epoch
    # Default selection returns the first two teachers; assert at least 2 calls
    assert adaptive.record_feedback.call_count >= 2
    # Optionally, inspect the arguments of the first call
    first_call = adaptive.record_feedback.call_args_list[0]
    assert 'teacher_id' in first_call.kwargs or len(first_call.args) >= 3
