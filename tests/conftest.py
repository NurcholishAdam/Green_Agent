"""Pytest configuration for Green Agent tests"""
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
