"""Pytest configuration for Green Agent tests (Enhanced)"""
import pytest

def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    # Added markers for advanced enhancements
    config.addinivalue_line(
        "markers", "enhancements: marks tests for advanced enhancement modules"
    )
    config.addinivalue_line(
        "markers", "limit_graph: marks tests for LIMIT Graph integration"
    )
    config.addinivalue_line(
        "markers", "modp: marks tests for Multi-Objective Decision Process (MODP)"
    )
    config.addinivalue_line(
        "markers", "rlhf: marks tests for Reinforcement Learning from Human Feedback (RLHF)"
    )
    config.addinivalue_line(
        "markers", "distillation: marks tests for Multi-Teacher On-Policy Distillation"
    )
    config.addinivalue_line(
        "markers", "moe: marks tests for Mixture-of-Experts (MoE) gating"
    )
    config.addinivalue_line(
        "markers", "bio_inspired: marks tests for Bio-inspired Optimisation"
    )
    config.addinivalue_line(
        "markers", "flexgen: marks tests for FlexGen integration"
    )

    # Set asyncio mode for easier async tests
    config.option.asyncio_mode = "auto"
