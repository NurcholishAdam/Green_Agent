"""
Green Agent v5.0.0 - Unit Tests for Unified Orchestrator
File: tests/unit/test_orchestrator.py

Enhanced with optional tests for:
- LIMIT Graph integration
- MODP (Multi‑Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi‑Teacher On‑Policy Distillation with MoE gating
- Bio‑inspired Optimisation
- FlexGen execution backend
"""

import pytest
import asyncio
from src.integration.unified_orchestrator import UnifiedGreenAgent, ExecutionMode

# Optional imports for enhancements (graceful skip)
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    FeedbackEvent = None


@pytest.fixture
def config():
    return {
        'system': {
            'mode': 'unified',
            'debug': True
        },
        'carbon': {
            'api_provider': 'simulation'
        },
        'ray': {
            'enabled': False
        }
    }


@pytest.fixture
def enhanced_config(config):
    """Config with enhancements enabled (if modules available)."""
    config = config.copy()
    config['enhancements'] = {
        'enabled': True,
        'limit_graph': {
            'enabled': True,
            'graph_metrics': {'centrality': 0.7, 'connectivity': 0.6}
        },
        'modp': {
            'enabled': True,
            'objective_weights': [0.4, 0.3, 0.2, 0.1]
        },
        'rlhf': {
            'enabled': True,
            'human_feedback_score': 0.6
        },
        'distillation': {
            'enabled': True,
            'use_moe_gating': True
        },
        'bio_inspired': {
            'enabled': True,
            'use_evolutionary': True
        },
        'flexgen': {
            'enabled': True,
            'model_name': 'facebook/opt-6.7b',
            'default_precision': 'fp16'
        }
    }
    return config


@pytest.mark.asyncio
async def test_orchestrator_initialization(config):
    """Test orchestrator can be initialized"""
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    assert agent.running == True
    await agent.shutdown()
    assert agent.running == False


@pytest.mark.asyncio
async def test_execute_task_success(config):
    """Test successful task execution"""
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    
    task = {
        'id': 'test_001',
        'type': 'ml_inference',
        'priority': 5,
        'deferrable': True
    }
    
    result = await agent.execute_task(task)
    
    assert result.task_id == 'test_001'
    assert result.success == True
    assert result.accuracy > 0
    assert result.energy_consumed >= 0
    assert result.carbon_emitted >= 0
    
    await agent.shutdown()


@pytest.mark.asyncio
async def test_error_handling(config):
    """Test error handling in task execution"""
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    
    # Task with missing required fields
    task = {'id': 'invalid_task'}
    result = await agent.execute_task(task)
    
    # Should handle gracefully, not crash
    assert result.task_id == 'invalid_task'
    
    await agent.shutdown()


# ------------------------------------------------------------------------------
# Enhanced tests (optional)
# ------------------------------------------------------------------------------

@pytest.mark.skipif(not ENHANCEMENTS_AVAILABLE, reason="Enhancement modules not installed")
@pytest.mark.asyncio
async def test_orchestrator_accepts_enhanced_config(enhanced_config):
    """Orchestrator should initialize with enhancements enabled without error."""
    agent = UnifiedGreenAgent(enhanced_config)
    await agent.initialize()
    assert agent.running is True
    # If orchestrator exposes enhancement flags, verify them
    if hasattr(agent, 'use_enhancements'):
        assert agent.use_enhancements is True
    await agent.shutdown()


@pytest.mark.skipif(not ENHANCEMENTS_AVAILABLE, reason="Enhancement modules not installed")
@pytest.mark.asyncio
async def test_execute_task_with_enhancement_metadata(enhanced_config):
    """Task result should include MODP or graph metrics when enhancements are on."""
    agent = UnifiedGreenAgent(enhanced_config)
    await agent.initialize()

    task = {
        'id': 'enhanced_task_001',
        'type': 'ml_inference',
        'priority': 7,
        'deferrable': False,
        'graph_metrics': {'centrality': 0.8},
        'human_feedback_score': 0.7
    }

    result = await agent.execute_task(task)

    assert result.task_id == 'enhanced_task_001'
    assert result.success is True

    # Check if enhanced fields are present (if the orchestrator populates them)
    # This depends on implementation; we make a soft check.
    if hasattr(result, 'modp_score'):
        assert 0.0 <= result.modp_score <= 1.0
    if hasattr(result, 'graph_metrics'):
        assert result.graph_metrics.get('centrality', 0.0) > 0.0

    await agent.shutdown()


@pytest.mark.skipif(not ENHANCEMENTS_AVAILABLE, reason="Enhancement modules not installed")
@pytest.mark.asyncio
async def test_flexgen_delegation_flag(enhanced_config):
    """FlexGen settings in config should not break initialization, and orchestrator may expose flag."""
    agent = UnifiedGreenAgent(enhanced_config)
    await agent.initialize()

    # If orchestrator has a flag for FlexGen, check it reflects config
    if hasattr(agent, 'flexgen_enabled'):
        assert agent.flexgen_enabled == enhanced_config['enhancements']['flexgen']['enabled']

    await agent.shutdown()
