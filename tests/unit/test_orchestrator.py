"""
Green Agent v5.0.0 - Unit Tests for Unified Orchestrator
File: tests/unit/test_orchestrator.py
"""

import pytest
import asyncio
from src.integration.unified_orchestrator import UnifiedGreenAgent, ExecutionMode


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
