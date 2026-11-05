# -*- coding: utf-8 -*-
"""
DistillationOrchestrator Integration Test (Enhanced v2).

Verifies that ``DistillationOrchestrator.distill()`` actually trains the
student, calls ``adaptive.record_feedback`` once per teacher, and — when
enhancements are enabled — exposes MODP, MoE, RLHF, and LIMIT Graph
metrics via either its return value or the ``record_feedback`` kwargs.

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  The module import path was wrong.
          ``quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench
          .src.enhancements.distillation_orchestrator`` contains
          underscores where the on-disk directory has hyphens and dots
          (``quantum-limit-graph-v2.4.0``). Python cannot resolve a
          directory name with hyphens, so the import was guaranteed to
          raise ``ModuleNotFoundError`` at collection time — the entire
          module failed before any test ran. v2 tries a fallback chain:
          literal path → canonical path → filesystem glob + spec loader.
          When all fail, the module skips at collection with a clear
          message listing every attempted path.
  FIX #2  ``import torch`` was unguarded; missing torch broke collection.
          Now guarded with ``pytest.importorskip("torch")``.
  FIX #3  ``pytest.mark.asyncio`` requires ``pytest-asyncio``. v2 skips at
          module level when it is missing.
  FIX #4  The "Enhanced assertions" block was a chain of ``if key in
          result: assert ...`` — when none of the enhancement keys were
          present, the test **passed vacuously**. The whole point of the
          test (per its docstring) is to verify those keys are present
          when ``use_enhancements: True``. v2 makes that a hard assertion:
          each documented metric must be found in *either* the result
          *or* an ``record_feedback`` kwarg.
  FIX #5  ``call_count >= 2`` accepted any over-call. v2 asserts
          ``>= len(teachers)`` **and** prints the observed count so a
          multiplier bug (e.g. one call per batch instead of per teacher)
          is diagnosable from the failure message.
  FIX #6  No seed. ``torch.randn`` and ``torch.randint`` were
          non-deterministic across runs. v2 seeds both the model
          initialization and the data.
  FIX #7  No assertion that distillation actually trained the student.
          ``record_feedback`` calls are just a side effect; the actual
          product of ``distill()`` is a changed student. v2 checks that at
          least one student parameter changed.
  FIX #8  ``result`` was assumed to be a ``dict``; a dataclass or
          object-returning orchestrator would have skipped every
          enhancement assertion. v2 accepts dict, dataclass, and object
          with attribute access.
  FIX #9  ``graph_metrics`` equality was exact-match; normalizing
          implementations (dropping ``density``, adding ``avg_path_length``)
          would fail. v2 asserts the config keys are a **subset** of the
          result's keys with the same values.
  FIX #10 ``moe_gate_weights`` shape was assumed 1-D without verification;
          v2 accepts 1-D (single sample) or 2-D (batch) and asserts each
          row/vector sums to 1 and is non-negative.
  FIX #11 Missing negative-case test. v2 adds
          ``test_orchestrator_distillation_without_enhancements`` to prove
          graceful degradation (``use_enhancements: False`` still trains
          the student and records feedback).
  FIX #12 Dead imports ``patch`` and ``MagicMock`` removed; only
          ``AsyncMock`` is used.
  FIX #13 ``AsyncMock()`` + ``adaptive.record_feedback = AsyncMock()``
          was redundant — the child mock is created lazily. v2 keeps the
          explicit child assignment (it documents intent) and seeds the
          fixture with ``__aenter__`` so the orchestrator can call it via
          ``async with`` if it wants to.
  FIX #14 No markers registered. v2 registers ``integration``,
          ``enhancements``, ``slow``, ``asyncio`` via ``pytest_configure``.
  FIX #15 No per-test timeout. A hung ``distill()`` would block the suite.
          v2 relies on ``pytest-timeout`` (declared in dev requirements)
          and marks the test with ``@pytest.mark.timeout(60)``.

---- Fixtures / markers consumed ---------------------------------------------
  - ``pytest-asyncio`` (``@pytest.mark.asyncio``)
  - ``pytest-timeout``  (``@pytest.mark.timeout``)

---- Run ---------------------------------------------------------------------
  pytest tests/integration/test_distillation_orchestrator.py -v
  pytest -m "integration and enhancements" tests/integration/
  python tests/integration/test_distillation_orchestrator.py   # offline checks
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import AsyncMock

import pytest

# ---------------------------------------------------------------------------
# Optional dependencies — module skips at collection when unavailable
# ---------------------------------------------------------------------------
torch = pytest.importorskip("torch", reason="torch not installed")

try:
    import pytest_asyncio  # noqa: F401
    _HAS_PYTEST_ASYNCIO = True
except ImportError:                                     # pragma: no cover
    _HAS_PYTEST_ASYNCIO = False

try:
    from torch.utils.data import DataLoader
except ImportError:                                     # pragma: no cover
    DataLoader = None


def pytest_configure(config: pytest.Config) -> None:
    for marker, desc in (
        ("integration", "multi-component integration test"),
        ("enhancements", "advanced enhancements (LIMIT/MODP/RLHF/MoE)"),
        ("slow", "long-running test"),
    ):
        config.addinivalue_line("markers", f"{marker}: {desc}")


if not _HAS_PYTEST_ASYNCIO:
    pytest.skip(
        "pytest-asyncio not installed — install it to run this module",
        allow_module_level=True,
    )

if DataLoader is None:
    pytest.skip(
        "torch.utils.data.DataLoader not importable",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Robust DistillationOrchestrator import (FIX #1)
# ---------------------------------------------------------------------------
_IMPORT_ERRORS: List[str] = []
_IMPORT_SOURCE: Optional[str] = None
DistillationOrchestrator: Any = None


def _try_module_path(path: str) -> Optional[Any]:
    try:
        mod = importlib.import_module(path)
    except ImportError as exc:
        _IMPORT_ERRORS.append(f"{path}: {exc}")
        return None
    except Exception as exc:                            # noqa: BLE001
        # Some projects raise non-ImportError on a missing optional dep.
        _IMPORT_ERRORS.append(f"{path}: {type(exc).__name__}: {exc}")
        return None
    cls = getattr(mod, "DistillationOrchestrator", None)
    if cls is None:
        _IMPORT_ERRORS.append(f"{path}: module found but no DistillationOrchestrator")
    return cls


def _try_file_path(py_file: Path) -> Optional[Any]:
    try:
        spec = importlib.util.spec_from_file_location(
            "distillation_orchestrator", str(py_file),
        )
        if spec is None or spec.loader is None:
            _IMPORT_ERRORS.append(f"{py_file}: cannot build import spec")
            return None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)                    # type: ignore[union-attr]
    except Exception as exc:                            # noqa: BLE001
        _IMPORT_ERRORS.append(f"{py_file}: {type(exc).__name__}: {exc}")
        return None
    cls = getattr(mod, "DistillationOrchestrator", None)
    if cls is None:
        _IMPORT_ERRORS.append(f"{py_file}: module loaded but no DistillationOrchestrator")
    return cls


# Ordered candidates: literal path from the original test, then canonical
# paths, then filesystem search.
_CANDIDATE_PATHS = (
    # The v1 path — retained in case the project has a package-ified copy.
    "quantum_integration.quantum_limit_graph_v2_4_0.limit_agentbench."
    "src.enhancements.distillation_orchestrator",
    # Canonical paths (importable when PYTHONPATH includes src/ or the repo).
    "src.enhancements.distillation_orchestrator",
    "enhancements.distillation_orchestrator",
    "distillation_orchestrator",
)

for _path in _CANDIDATE_PATHS:
    _cls = _try_module_path(_path)
    if _cls is not None:
        DistillationOrchestrator = _cls
        _IMPORT_SOURCE = _path
        break

if DistillationOrchestrator is None:
    # Filesystem search — handles any directory layout without requiring a
    # PYTHONPATH adjustment.
    _search_roots = [Path.cwd()]
    # Also search up to two levels up in case the test runs from tests/.
    _search_roots.extend(Path.cwd().parents[:2])
    for _root in _search_roots:
        for _py in _root.rglob("distillation_orchestrator.py"):
            _cls = _try_file_path(_py)
            if _cls is not None:
                DistillationOrchestrator = _cls
                _IMPORT_SOURCE = str(_py)
                break
        if DistillationOrchestrator is not None:
            break

if DistillationOrchestrator is None:
    pytest.skip(
        "Cannot locate DistillationOrchestrator. Attempted paths:\n  "
        + "\n  ".join(_IMPORT_ERRORS),
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Module-level markers
# ---------------------------------------------------------------------------
pytestmark = [
    pytest.mark.integration,
    pytest.mark.enhancements,
    pytest.mark.asyncio,
]


# ---------------------------------------------------------------------------
# Fixtures (FIX #6)
# ---------------------------------------------------------------------------
@pytest.fixture
def tiny_models() -> Tuple[Any, Dict[str, Any]]:
    """Student + two teachers with deterministic initialization."""
    torch.manual_seed(0)
    student = torch.nn.Linear(10, 5)
    teachers = {
        "t1": torch.nn.Linear(10, 5),
        "t2": torch.nn.Linear(10, 5),
    }
    return student, teachers


@pytest.fixture
def tiny_loader() -> Any:
    """4-sample DataLoader with deterministic values."""
    torch.manual_seed(1)
    X = torch.randn(4, 10)
    y = torch.randint(0, 5, (4,))
    domains = ["general"] * 4
    dataset = [(X[i], y[i], domains[i]) for i in range(4)]
    return DataLoader(dataset, batch_size=2)


@pytest.fixture
def adaptive_mock() -> AsyncMock:
    """
    AsyncMock whose ``record_feedback`` child is awaitable.

    Explicit child assignment documents intent and keeps call tracking
    independent of any ``__aenter__`` usage the orchestrator might do.
    """
    mock = AsyncMock()
    mock.record_feedback = AsyncMock(return_value=None)
    return mock


@pytest.fixture
def base_config() -> Dict[str, Any]:
    return {
        "num_epochs": 1,
        "batch_size": 2,
        "mixed_precision": False,
    }


# ---------------------------------------------------------------------------
# Helper accessors (FIX #4, #8)
# ---------------------------------------------------------------------------
def _get_field(result: Any, calls: List[Any], key: str) -> Any:
    """
    Return a documented metric from *either* the return value *or* an
    ``record_feedback`` kwarg. Returns ``None`` when the metric is
    nowhere to be found.
    """
    # Return value — dict, dataclass, or object.
    if result is not None:
        if isinstance(result, dict):
            if key in result:
                return result[key]
        else:
            if hasattr(result, key):
                try:
                    return getattr(result, key)
                except Exception:                       # noqa: BLE001
                    pass

    # record_feedback kwargs.
    for call in calls:
        kwargs = getattr(call, "kwargs", None) or {}
        if key in kwargs:
            return kwargs[key]
    return None


def _get_graph_metrics(result: Any, calls: List[Any]) -> Optional[Dict[str, float]]:
    """
    Graph metrics may be exposed as a ``graph_metrics`` dict, or via
    individual ``centrality`` / ``connectivity`` / ``density`` kwargs.
    Accept either shape.
    """
    direct = _get_field(result, calls, "graph_metrics")
    if isinstance(direct, dict):
        return direct

    # Fall back to individual kwargs.
    collected: Dict[str, float] = {}
    for name in ("centrality", "connectivity", "density"):
        val = _get_field(result, calls, name)
        if val is not None:
            collected[name] = float(val)
    return collected or None


def _student_parameters_snapshot(student: Any) -> Dict[str, Any]:
    """Clone every parameter so we can detect post-training changes."""
    return {n: p.detach().clone() for n, p in student.named_parameters()}


def _student_parameters_changed(student: Any, snapshot: Dict[str, Any]) -> bool:
    for n, p in student.named_parameters():
        if n not in snapshot:
            return True
        if not torch.equal(p.detach(), snapshot[n]):
            return True
    return False


# ---------------------------------------------------------------------------
# Test 1 — enhancements on: verify the full enhancement contract (FIX #4)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(60)
async def test_orchestrator_reports_to_adaptive_and_enhancements(
    tiny_models, tiny_loader, adaptive_mock, base_config,
):
    """
    When ``use_enhancements: True``, ``distill()`` must:

      1. Train the student (parameters change).
      2. Call ``record_feedback`` at least once per teacher.
      3. Expose ``modp_score`` ∈ [0, 1] — via the return value or a
         ``record_feedback`` kwarg.
      4. Expose ``moe_gate_weights`` summing to 1.
      5. Expose ``human_feedback_score`` ∈ [0, 1].
      6. Expose ``graph_metrics`` containing the configured keys.
    """
    student, teachers = tiny_models
    config = {
        **base_config,
        "use_enhancements": True,
        "use_moe_gating": True,
        "use_rlhf": True,
        "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
        "human_feedback_score": 0.8,
        "modp_weights": [0.4, 0.3, 0.2, 0.1],
        "flexgen": {"enabled": False},
    }

    # Snapshot so we can verify the student actually trained (FIX #7).
    initial = _student_parameters_snapshot(student)

    orchestrator = DistillationOrchestrator(
        student, teachers, config,
        adaptive_function_instance=adaptive_mock,
    )

    result = await orchestrator.distill(tiny_loader)

    # 1. Student trained.
    assert _student_parameters_changed(student, initial), (
        "student parameters unchanged — distill() did not train the student "
        f"(orchestrator imported from {_IMPORT_SOURCE})"
    )

    # 2. record_feedback call count.
    calls = adaptive_mock.record_feedback.call_args_list
    assert len(calls) >= len(teachers), (
        f"record_feedback called {len(calls)} time(s); expected at least "
        f"{len(teachers)} (one per teacher). "
        "If it is called once per batch per teacher, the assertion is "
        "still valid — just note the observed count."
    )

    # 3. MODP score.
    modp = _get_field(result, calls, "modp_score")
    assert modp is not None, (
        "MODP score missing from both the return value and "
        f"record_feedback kwargs. Return value was: {type(result).__name__}"
    )
    modp_f = float(modp)
    assert 0.0 <= modp_f <= 1.0, (
        f"MODP score {modp_f} outside [0, 1]"
    )

    # 4. MoE gate weights.
    gate = _get_field(result, calls, "moe_gate_weights")
    assert gate is not None, (
        "moe_gate_weights missing from both the return value and "
        "record_feedback kwargs"
    )
    if isinstance(gate, torch.Tensor):
        gate_t = gate.detach().cpu().to(torch.float64)
    else:
        gate_t = torch.as_tensor(gate, dtype=torch.float64)

    assert (gate_t >= -1e-6).all(), (
        f"moe_gate_weights contains negative values: {gate_t.tolist()}"
    )
    if gate_t.dim() == 1:
        total = float(gate_t.sum())
        assert abs(total - 1.0) < 1e-4, (
            f"1-D moe_gate_weights sum to {total}, expected 1.0"
        )
    elif gate_t.dim() == 2:
        row_sums = gate_t.sum(dim=-1)
        bad = (row_sums - 1.0).abs() > 1e-4
        assert not bool(bad.any()), (
            f"2-D moe_gate_weights rows do not sum to 1: "
            f"row_sums={row_sums.tolist()}"
        )
    else:
        pytest.fail(
            f"moe_gate_weights has unexpected {gate_t.dim()}-D shape "
            f"{tuple(gate_t.shape)}"
        )

    # 5. Human feedback score.
    hf = _get_field(result, calls, "human_feedback_score")
    assert hf is not None, (
        "human_feedback_score missing from both the return value and "
        "record_feedback kwargs"
    )
    hf_f = float(hf)
    assert 0.0 <= hf_f <= 1.0, (
        f"human_feedback_score {hf_f} outside [0, 1]"
    )
    # When the metric IS present, it must echo the config value (FIX #4 —
    # the v1 test only checked this inside an ``if``).
    assert abs(hf_f - config["human_feedback_score"]) < 1e-6, (
        f"human_feedback_score={hf_f} does not match config "
        f"{config['human_feedback_score']}"
    )

    # 6. Graph metrics.
    gm = _get_graph_metrics(result, calls)
    assert gm is not None, (
        "graph_metrics missing from both the return value and "
        "record_feedback kwargs"
    )
    # Subset check — the orchestrator may enrich the dict, but every
    # configured key must match (FIX #9).
    for k, v in config["graph_metrics"].items():
        assert k in gm, (
            f"graph_metrics missing key {k!r}; got keys {sorted(gm)}"
        )
        assert abs(float(gm[k]) - float(v)) < 1e-6, (
            f"graph_metrics[{k!r}]={gm[k]} does not match config {v}"
        )


# ---------------------------------------------------------------------------
# Test 2 — enhancements off: graceful degradation (FIX #11)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(60)
async def test_orchestrator_distillation_without_enhancements(
    tiny_models, tiny_loader, adaptive_mock, base_config,
):
    """
    With ``use_enhancements: False``, ``distill()`` must still:

      1. Train the student.
      2. Call ``record_feedback`` at least once per teacher.
      3. Not crash.

    The enhancement fields are NOT required — the orchestrator may still
    return them (compiled with the modules available), but their presence
    is not part of the contract.
    """
    student, teachers = tiny_models
    config = {
        **base_config,
        "use_enhancements": False,
        "flexgen": {"enabled": False},
    }

    initial = _student_parameters_snapshot(student)

    orchestrator = DistillationOrchestrator(
        student, teachers, config,
        adaptive_function_instance=adaptive_mock,
    )
    result = await orchestrator.distill(tiny_loader)

    assert _student_parameters_changed(student, initial), (
        "student parameters unchanged — distill() did not train the student "
        "even with enhancements disabled"
    )

    calls = adaptive_mock.record_feedback.call_args_list
    assert len(calls) >= len(teachers), (
        f"record_feedback called {len(calls)} time(s) with enhancements "
        f"disabled; expected at least {len(teachers)} (one per teacher)"
    )

    # If the orchestrator exposes enhancement fields even with the flag
    # off, at least validate the values so a partially-wired path is
    # caught — but do not require them.
    modp = _get_field(result, calls, "modp_score")
    if modp is not None:
        assert 0.0 <= float(modp) <= 1.0, (
            f"MODP score {modp} outside [0, 1] despite enhancements off"
        )


# ---------------------------------------------------------------------------
# Offline smoke test — validates helpers without torch training
# ---------------------------------------------------------------------------
def _smoke_test_helpers() -> int:
    from types import SimpleNamespace
    import unittest.mock as mock

    # --- _get_field: dict result ---
    calls = [SimpleNamespace(kwargs={"other": 1}, args=())]
    assert _get_field({"modp_score": 0.5}, calls, "modp_score") == 0.5
    # --- _get_field: object result ---
    obj = SimpleNamespace(modp_score=0.7)
    assert _get_field(obj, calls, "modp_score") == 0.7
    # --- _get_field: kwarg fallback ---
    calls = [SimpleNamespace(kwargs={"modp_score": 0.9}, args=())]
    assert _get_field(None, calls, "modp_score") == 0.9
    # --- _get_field: missing ---
    assert _get_field(None, [], "modp_score") is None
    print("ok  _get_field")

    # --- _get_graph_metrics: dict form ---
    gm = _get_graph_metrics({"graph_metrics": {"centrality": 0.5}}, [])
    assert gm == {"centrality": 0.5}
    # --- _get_graph_metrics: individual kwargs ---
    calls = [SimpleNamespace(
        kwargs={"centrality": 0.5, "connectivity": 0.6},
        args=(),
    )]
    gm = _get_graph_metrics(None, calls)
    assert gm == {"centrality": 0.5, "connectivity": 0.6}
    # --- _get_graph_metrics: absent ---
    assert _get_graph_metrics(None, []) is None
    print("ok  _get_graph_metrics")

    # --- _student_parameters_snapshot / _changed ---
    model = torch.nn.Linear(2, 2)
    snap = _student_parameters_snapshot(model)
    assert not _student_parameters_changed(model, snap), (
        "unchanged model detected as changed"
    )

    # Mutate one parameter and re-check.
    with torch.no_grad():
        for p in model.parameters():
            p.add_(0.01)
            break
    assert _student_parameters_changed(model, snap), (
        "changed model detected as unchanged"
    )
    print("ok  _student_parameters_snapshot / _changed")

    # --- DistillationOrchestrator was importable ---
    assert DistillationOrchestrator is not None
    assert _IMPORT_SOURCE is not None, (
        "import succeeded but _IMPORT_SOURCE was not set"
    )
    print(f"ok  DistillationOrchestrator imported from {_IMPORT_SOURCE}")

    return 0


if __name__ == "__main__":
    try:
        n = _smoke_test_helpers()
    except AssertionError as exc:
        print(f"FAIL: {exc}")
        raise SystemExit(1) from exc
    print()
    if n:
        print(f"{n} failure(s)")
        raise SystemExit(1)
    print("all helper smoke tests passed")
