"""
tests/test_mopd_orchestrator_v17.py
"""
import asyncio
import pytest

from green_agent_enhancements_v17_0_0 import (
    Storage, MOPDOrchestratorV17, _ConfigDict, _DEFAULTS,
)


@pytest.mark.asyncio
async def test_orchestrator_select_strategy(tmp_path):
    cfg = _ConfigDict(_DEFAULTS)
    cfg["DB_PATH"] = str(tmp_path / "test.db")
    storage = Storage(cfg["DB_PATH"])
    orch = MOPDOrchestratorV17(storage=storage, config=cfg)
    await orch.start()

    state = {
        "quality": 0.9,
        "carbon_intensity": 350,
        "cost": 0.4,
        "latency_ms": 120,
    }
    result = await orch.select_strategy(state)
    assert "strategy" in result
    assert result["strategy"] in ["performance", "carbon", "cost",
                                   "hybrid", "adaptive"]
    assert "precision" in result
    assert "carbon_decision" in result

    hc = await orch.health_check()
    assert hc["running"] is True

    await orch.shutdown()
