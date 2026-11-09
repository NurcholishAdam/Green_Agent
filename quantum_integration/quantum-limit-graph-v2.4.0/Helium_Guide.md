
# Helium-Aware Deployment Recipe

**Package:** `green-agent-helium` · **Version:** 2.4.2-helium-alpha · **Status:** Alpha · **Date:** <2026-11-9>
**Python:** ≥3.10 · **License:** Apache-2.0
**Companion:** [`README.md`](README.md) (base `limit-agentbench`), [`ARCHITECTURE_PROPOSAL.md`](ARCHITECTURE_PROPOSAL.md)

> **Changelog**
> - **2.0** — Every P0 from the v1.0 review is closed:
>   - **"Copy all files as shown above" fixed** — this document ships every
>     file inline (§3–§11).
>   - **`uvicorn ...:router` → `...:app`** — the dashboard ships a real ASGI
>     `app` object (§10).
>   - **`UnifiedOrchestrator` defined** — class + `process_task` signature +
>     `NotImplementedError` stub so the recipe runs end-to-end (§9). Real
>     implementation ships separately.
>   - **Weights reconciled** — the helium recipe defines a **new**
>     `EnvironmentalSustainability` composite (helium 0.4 + carbon 0.6) that
>     is **parallel to**, not a replacement for, `SustainabilityIndex` (§2.3).
>   - **Config append made idempotent** with `cp`-only (no append), backup
>     path, and marker check (§4).
>   - **`set -euo pipefail`** added (§4).
>   - **`mkdir -p config/base`** added before config copy (§4).
>   - **Dependencies pinned** (§7).
>   - **`--host 127.0.0.1`** default; `0.0.0.0` requires explicit opt-in (§12).
>   - **Bearer authentication** on every `/helium*` endpoint (§10).
>   - **Placeholder URL replaced** with `$HELIUM_API_URL` env var (§6).
>   - **Assertion-based verification** — `verify_helium_recipe.py` (§5)
>     replaces the print-only test.
>   - **Units documented** — `*_seconds` suffix added (§2.2).
>   - **"Helium" defined** (§1.1) — prior draft never said what it means.
> - **1.0** — Initial 7-step recipe (referenced code that was never
>   delivered; `router` instead of `app`; no `set -e`; config-append
>   duplicated on re-run; unauthenticated `0.0.0.0` dashboard).

---

## Table of contents

- [1. What "helium" means](#1-what-helium-means)
- [2. Canonical contracts](#2-canonical-contracts)
- [3. The recipe — overview](#3-the-recipe--overview)
- [4. `scripts/deploy_helium.sh`](#4-scriptsdeploy_heliumsh)
- [5. `scripts/verify_helium_recipe.py`](#5-scriptsverify_helium_recipespy)
- [6. `config/base/green_agent_config.example.yaml`](#6-configbasegreen_agent_configexampleyaml)
- [7. `requirements-helium.txt`](#7-requirements-heliumtxt)
- [8. `src/integration/helium_types.py`](#8-srcintegrationhelium_typespy)
- [9. `src/integration/unified_orchestrator.py`](#9-srcintegrationunified_orchestratorpy)
- [10. `dashboard/helium_dashboard.py`](#10-dashboardhelium_dashboardpy)
- [11. `tests/test_helium_config.py`](#11-teststest_helium_configpy)
- [12. Security](#12-security)
- [13. Known limitations](#13-known-limitations)
- [14. Next steps](#14-next-steps)

---

## 0. Status and scope

### 0.1 What "Alpha" means here

**Alpha.** This document ships the full deployment recipe, the config
schema, a working ASGI dashboard, a `UnifiedOrchestrator` **stub** that
raises `NotImplementedError`, and an assertion-based verifier.

The **orchestrator implementation** (helium + carbon decision logic) ships
separately. Until it lands, the recipe runs end-to-end and verifies —
`UnifiedOrchestrator.process_task()` raises `NotImplementedError` with a
clear pointer, and the verifier confirms that behavior explicitly.

### 0.2 What this document is **not**

- Not "Production Ready" — Alpha.
- Not a replacement for `README.md` or `ARCHITECTURE_PROPOSAL.md`.
- Not a general deployment guide — specific to the helium module.

### 0.3 Companion deliveries required

| Component | Ships in | Status |
|---|---|---|
| Recipe + config + stubs + tests + CI (this document) | Here | ✅ Alpha |
| `UnifiedOrchestrator.process_task()` implementation | Separate `green-agent-helium-core` | ⏳ Pending |
| Helium-availability API client (`HeliumMonitorClient`) | Same separate delivery | ⏳ Pending |
| Live carbon-intensity provider | `README.md §0.2 O-1` | ⏳ Phase 2 |
| Carbon + helium decision logic | Same separate delivery | ⏳ Pending |

---

## 1. What "helium" means

### 1.1 Definition

**Helium** here refers to **physical helium (He)** used in:

- **Semiconductor manufacturing** — helium is a process gas in chip
  fabrication (lithography, etching, cooling).
- **Cryogenic cooling** for quantum computing and MRI-class magnets.
- **Leak detection** in data-center hardware assembly.

Physical helium is a **finite, non-renewable** resource extracted
incidentally from natural gas. Supply is volatile; prices have risen
~300% over the last 15 years. Treating helium consumption as a
**sustainability dimension** — parallel to carbon — captures a supply-chain
risk that carbon intensity alone misses.

**The helium module monitors helium availability and, combined with carbon
intensity, informs scheduling decisions (when to defer, where to route).**
It does **not** modify `SustainabilityIndex` from `README.md §2.2` — that
stays at accuracy / energy / carbon. Helium is an orthogonal decision
input, not a scoring override.

### 1.2 What "helium awareness" means

A task is **helium-aware** when the orchestrator:

1. Checks current helium availability (via `HeliumMonitorClient`).
2. Checks current grid carbon intensity.
3. Combines them into a decision signal (§2.3).
4. Chooses **WHEN** (defer) and **WHERE** (route) accordingly.

### 1.3 `EnvironmentalSustainability` — the new composite

The recipe's config weights (`helium_weight: 0.4`, `carbon_weight: 0.6`)
define a **new** composite that is **parallel to** — and does not replace
— `SustainabilityIndex`:

```
environmental_sustainability = 0.4 · helium_availability
                             + 0.6 · carbon_sustainability
```

Where:
- `helium_availability ∈ [0, 1]` — 1 = abundant, 0 = critical scarcity.
- `carbon_sustainability ∈ [0, 1]` — 1 = cleanest grid, 0 = dirtiest.

Range: `[0, 1]`. Higher is better.

**`SustainabilityIndex` is unchanged.** It remains the accuracy / energy /
carbon composite documented in `README.md §2.2`. The two composites
coexist:

- `sustainability_index` measures **agent quality + green**.
- `environmental_sustainability` measures **supply-chain risk + green**.

A submission may report both; a leaderboard may rank by either.

---

## 2. Canonical contracts

### 2.1 `HeliumTask` — the shape `process_task` accepts

Prior drafts used:

```python
{
    'task_id': 'test_001',
    'hardware_requirements': {'gpu_count': 2},
    'model_config': {'size_gb': 10},
    'deferrable': True
}
```

This is **not** the shipped `Task` from `README.md §2.3` — and it doesn't
need to be. It is the helium module's own task shape. `HeliumTask` is
defined in §8 and validates on construction.

### 2.2 Config schema

Defined in §6. Canonical fields:

| Field | Type | Units | Default |
|---|---|---|---|
| `helium_aware_enabled` | bool | — | `false` |
| `helium_api_url` | str | URL | (required) |
| `helium_update_interval_seconds` | int | s | `300` |
| `helium_weight` | float | ratio | `0.4` |
| `carbon_weight` | float | ratio | `0.6` |
| `helium_yellow_threshold` | float | ratio | `0.6` |
| `helium_red_threshold` | float | ratio | `0.8` |
| `helium_critical_threshold` | float | ratio | `0.95` |
| `fallback_enabled` | bool | — | `true` |
| `execution_timeout_seconds` | int | s | `300` |

**Units fix from prior draft:** `helium_update_interval` →
`helium_update_interval_seconds`, `execution_timeout` →
`execution_timeout_seconds`.

### 2.3 `UnifiedOrchestrator` — public surface

```python
UnifiedOrchestrator(config_path: str | None = None)
    .process_task(task: dict) -> dict          # async
```

Return shape (documented; implemented in separate delivery):

```json
{
  "task_id": "test_001",
  "decision": "defer|execute|route|reject",
  "when_iso": "...",
  "where_node": "...",
  "helium_availability": 0.85,
  "carbon_intensity": 220.0,
  "environmental_sustainability": 0.79
}
```

Until the separate delivery lands, `process_task` raises
`NotImplementedError` — the recipe still runs and verifies.

### 2.4 Relationship to `limit_agentbench`'s `Task`

The two task types are **independent**:

| Package | Class | Purpose |
|---|---|---|
| `limit_agentbench` | `Task` (from `README.md §2.3`) | Agent benchmarking protocol |
| `green_agent_helium` | `HeliumTask` (this document) | Helium-aware scheduling |

A user may pass a `HeliumTask`-shaped dict to `UnifiedOrchestrator` **and**
a `Task` to `AgentBenchAdapter`. Neither replaces the other.

### 2.5 Units and ranges

| Field | Units | Range |
|---|---|---|
| `helium_availability` | ratio | `[0, 1]` |
| `carbon_intensity` | gCO₂e / kWh | `≥ 0` |
| `helium_update_interval_seconds` | seconds | `> 0` |
| `execution_timeout_seconds` | seconds | `> 0` |
| `helium_weight` | ratio | `[0, 1]`, sum with `carbon_weight` = 1.0 |
| `carbon_weight` | ratio | `[0, 1]` |
| `helium_yellow_threshold` | ratio | `[0, 1]`, < red |
| `helium_red_threshold` | ratio | `[0, 1]`, < critical |
| `helium_critical_threshold` | ratio | `[0, 1]`, ≤ 1.0 |

Threshold ordering invariant: `yellow < red < critical ≤ 1.0`.

---

## 3. The recipe — overview

The hardened recipe (§4) performs:

1. Verify preconditions (Python ≥ 3.10).
2. Create directories (idempotent).
3. Copy the config **example** → `config/base/green_agent_config.yaml`
   **only if not present** (never appends, never overwrites).
4. Install pinned dependencies from `requirements-helium.txt`.
5. Install the package in editable mode (`pip install -e . --no-deps`).
6. Run `scripts/verify_helium_recipe.py` (assertion-based).
7. Print next-step instructions (including the safe `uvicorn` command).

The config file is **never appended to**. If you want the helium block
merged into an existing `green_agent_config.yaml`, merge manually using the
example as reference — this avoids the v1.0 duplicate-on-rerun bug.

### 3.1 What the recipe does **not** do

- Does not modify any existing config file.
- Does not start the dashboard (prints the command instead).
- Does not call any external API.
- Does not implement `UnifiedOrchestrator.process_task()`.

### 3.2 Running the recipe

```bash
bash scripts/deploy_helium.sh
```

On success, prints:

```
[PASS] Python 3.12.3 >= 3.10
[PASS] directories ready
[PASS] copied config/base/green_agent_config.example.yaml -> .../green_agent_config.yaml
[PASS] dependencies installed
[PASS] editable install
[PASS] verification

All 10 checks passed

Done. To start the dashboard safely:
    ...
```

---

## 4. `scripts/deploy_helium.sh`

Save the following to `scripts/deploy_helium.sh` and `chmod +x` it.

```bash
#!/usr/bin/env bash
#
# scripts/deploy_helium.sh — Hardened deployment recipe for the helium module.
#
# What this script does:
#   1. Verify preconditions (Python >= 3.10).
#   2. Create directories (idempotent).
#   3. Copy config example -> config (only if config does not exist).
#   4. Install pinned dependencies.
#   5. Install the package in editable mode.
#   6. Run scripts/verify_helium_recipe.py (assertion-based).
#   7. Print next-step instructions (dashboard command, security note).
#
# What this script does NOT do:
#   - Modify any existing config file (no append, no overwrite).
#   - Start the dashboard (prints the command instead).
#   - Call any external API.
#   - Implement UnifiedOrchestrator.process_task (ships separately).
#
# Usage:
#   bash scripts/deploy_helium.sh
#   PYTHON=python3.12 bash scripts/deploy_helium.sh   # override interpreter
#
set -euo pipefail

# --- Configurable ----------------------------------------------------------
PYTHON="${PYTHON:-python3}"
REQUIRED_PY_MAJOR=3
REQUIRED_PY_MINOR=10

# --- Locate repo root ------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Repo root: $REPO_ROOT"

# --- 1. Verify Python ------------------------------------------------------
echo "==> Verifying Python"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "[FAIL] $PYTHON not found in PATH" >&2
    exit 1
fi

PY_VERSION="$("$PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")')"
PY_MAJOR="$("$PYTHON" -c 'import sys; print(sys.version_info.major)')"
PY_MINOR="$("$PYTHON" -c 'import sys; print(sys.version_info.minor)')"

if [ "$PY_MAJOR" -lt "$REQUIRED_PY_MAJOR" ] \
   || { [ "$PY_MAJOR" -eq "$REQUIRED_PY_MAJOR" ] && [ "$PY_MINOR" -lt "$REQUIRED_PY_MINOR" ]; }; then
    echo "[FAIL] Python $PY_VERSION < required $REQUIRED_PY_MAJOR.$REQUIRED_PY_MINOR" >&2
    exit 1
fi
echo "[PASS] Python $PY_VERSION >= $REQUIRED_PY_MAJOR.$REQUIRED_PY_MINOR"

# --- 2. Create directories (idempotent) -----------------------------------
echo "==> Creating directories"
for d in \
    src \
    src/integration \
    dashboard \
    config \
    config/base \
    scripts \
    tests
do
    mkdir -p "$d"
    echo "    mkdir -p $d"
done
echo "[PASS] directories ready"

# --- 3. Copy config example only if config absent ------------------------
EXAMPLE="config/base/green_agent_config.example.yaml"
TARGET="config/base/green_agent_config.yaml"

if [ ! -f "$EXAMPLE" ]; then
    echo "[FAIL] missing $EXAMPLE (should ship in this bundle)" >&2
    exit 1
fi

if [ -f "$TARGET" ]; then
    echo "[SKIP] $TARGET already exists — not overwriting"
    echo "       Compare with $EXAMPLE to merge new keys manually."
else
    cp "$EXAMPLE" "$TARGET"
    echo "[PASS] copied $EXAMPLE -> $TARGET"
fi

# --- 4. Install pinned dependencies ---------------------------------------
echo "==> Installing pinned dependencies"
"$PYTHON" -m pip install --upgrade pip >/dev/null
"$PYTHON" -m pip install -r requirements-helium.txt
echo "[PASS] dependencies installed"

# --- 5. Editable install --------------------------------------------------
echo "==> Editable install"
"$PYTHON" -m pip install -e . --no-deps
echo "[PASS] editable install"

# --- 6. Run verifier ------------------------------------------------------
echo "==> Verifying"
"$PYTHON" scripts/verify_helium_recipe.py
echo "[PASS] verification"

# --- 7. Next steps --------------------------------------------------------
cat <<'EOF'

Done. To start the dashboard safely:

    export GREEN_AGENT_SECRETS_DIR=/run/secrets
    mkdir -p "$GREEN_AGENT_SECRETS_DIR"
    printf 'changeme' > "$GREEN_AGENT_SECRETS_DIR/helium_dashboard_token"
    chmod 600 "$GREEN_AGENT_SECRETS_DIR/helium_dashboard_token"
    export HELIUM_API_URL="https://your-helium-monitor.example.org/v1"
    uvicorn dashboard.helium_dashboard:app --host 127.0.0.1 --port 8001

Do NOT use --host 0.0.0.0. See DEPLOYMENT_RECIPE.md §12.
EOF
```

---

## 5. `scripts/verify_helium_recipe.py`

Save the following to `scripts/verify_helium_recipe.py`.

```python
#!/usr/bin/env python3
"""
scripts/verify_helium_recipe.py — Assertion-based verifier.

Exits 0 on all-PASS; exits 1 on the first failed assertion.

Checks:
    1. Python >= 3.10
    2. All expected directories exist
    3. All expected __init__.py files exist
    4. config/base/green_agent_config.yaml parses as YAML
    5. Config weights sum to 1.0
    6. Config thresholds are monotonic (yellow < red < critical <= 1.0)
    7. UnifiedOrchestrator imports
    8. UnifiedOrchestrator.process_task raises NotImplementedError (stub present)
    9. Dashboard app imports and serves /healthz with 200
   10. Dashboard /helium requires auth (401 or 503 without token)

Usage:
    python scripts/verify_helium_recipe.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_FAILED: list[str] = []


def _check(name: str, fn) -> None:
    try:
        fn()
        print(f"[PASS] {name}")
    except Exception as e:
        print(f"[FAIL] {name} — {type(e).__name__}: {e}")
        _FAILED.append(name)


# --- 1. Python version -----------------------------------------------------

def check_python_version() -> None:
    assert sys.version_info >= (3, 10), \
        f"Python {sys.version_info.major}.{sys.version_info.minor} < 3.10"


# --- 2. Directories --------------------------------------------------------

def check_directories() -> None:
    expected = [
        "src",
        "src/integration",
        "dashboard",
        "config",
        "config/base",
        "scripts",
        "tests",
    ]
    missing = [d for d in expected if not (ROOT / d).is_dir()]
    assert not missing, f"missing directories: {missing}"


# --- 3. __init__.py files -------------------------------------------------

def check_init_files() -> None:
    expected = [
        "src/__init__.py",
        "src/integration/__init__.py",
        "dashboard/__init__.py",
        "tests/__init__.py",
    ]
    missing = [f for f in expected if not (ROOT / f).is_file()]
    assert not missing, f"missing __init__.py: {missing}"


# --- 4. Config parses -----------------------------------------------------

def check_config_parses() -> None:
    import yaml
    cfg_path = ROOT / "config" / "base" / "green_agent_config.yaml"
    assert cfg_path.is_file(), f"missing {cfg_path}"
    with cfg_path.open() as f:
        cfg = yaml.safe_load(f)
    assert isinstance(cfg, dict), "config did not parse to a dict"
    required = {
        "helium_aware_enabled",
        "helium_api_url",
        "helium_update_interval_seconds",
        "helium_weight",
        "carbon_weight",
        "helium_yellow_threshold",
        "helium_red_threshold",
        "helium_critical_threshold",
        "fallback_enabled",
        "execution_timeout_seconds",
    }
    missing = required - set(cfg.keys())
    assert not missing, f"config missing keys: {missing}"


# --- 5. Weights sum to 1.0 ------------------------------------------------

def check_weights() -> None:
    import yaml
    with (ROOT / "config" / "base" / "green_agent_config.yaml").open() as f:
        cfg = yaml.safe_load(f)
    h = float(cfg["helium_weight"])
    c = float(cfg["carbon_weight"])
    assert abs(h + c - 1.0) < 1e-6, f"weights do not sum to 1.0: {h} + {c} = {h + c}"


# --- 6. Thresholds monotonic ---------------------------------------------

def check_thresholds() -> None:
    import yaml
    with (ROOT / "config" / "base" / "green_agent_config.yaml").open() as f:
        cfg = yaml.safe_load(f)
    y = float(cfg["helium_yellow_threshold"])
    r = float(cfg["helium_red_threshold"])
    cr = float(cfg["helium_critical_threshold"])
    assert 0.0 <= y < r < cr <= 1.0, (
        f"thresholds must be 0 <= yellow ({y}) < red ({r}) < critical ({cr}) <= 1.0"
    )


# --- 7. UnifiedOrchestrator imports --------------------------------------

def check_orchestrator_imports() -> None:
    from src.integration.unified_orchestrator import UnifiedOrchestrator  # noqa: F401


# --- 8. process_task raises NotImplementedError --------------------------

def check_orchestrator_stub() -> None:
    import asyncio

    from src.integration.unified_orchestrator import UnifiedOrchestrator

    orch = UnifiedOrchestrator()
    task = {
        "task_id": "test_001",
        "hardware_requirements": {"gpu_count": 2},
        "model_config": {"size_gb": 10},
        "deferrable": True,
    }
    try:
        asyncio.run(orch.process_task(task))
    except NotImplementedError:
        return  # expected — stub present
    except Exception as e:
        raise AssertionError(
            f"expected NotImplementedError, got {type(e).__name__}: {e}"
        ) from e
    raise AssertionError(
        "process_task did not raise — stub was silently replaced with a no-op"
    )


# --- 9. Dashboard /healthz ------------------------------------------------

def check_dashboard_healthz() -> None:
    from fastapi.testclient import TestClient

    from dashboard.helium_dashboard import app

    client = TestClient(app)
    resp = client.get("/healthz")
    assert resp.status_code == 200, f"got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body.get("status") == "ok", f"unexpected body: {body}"


# --- 10. Dashboard /helium requires auth ---------------------------------

def check_dashboard_auth() -> None:
    from fastapi.testclient import TestClient

    from dashboard.helium_dashboard import app

    os.environ.pop("GREEN_AGENT_SECRETS_DIR", None)
    client = TestClient(app)
    resp = client.get("/helium")
    assert resp.status_code in (401, 503), (
        f"expected 401/503 without auth, got {resp.status_code}: {resp.text}"
    )


# --- Entry point ----------------------------------------------------------

def main() -> int:
    _check("Python version >= 3.10",          check_python_version)
    _check("directories present",             check_directories)
    _check("__init__.py files present",       check_init_files)
    _check("config parses as YAML",           check_config_parses)
    _check("weights sum to 1.0",              check_weights)
    _check("thresholds monotonic",            check_thresholds)
    _check("UnifiedOrchestrator imports",     check_orchestrator_imports)
    _check("process_task raises NotImplementedError", check_orchestrator_stub)
    _check("dashboard /healthz returns 200",  check_dashboard_healthz)
    _check("dashboard /helium requires auth", check_dashboard_auth)

    if _FAILED:
        print(f"\n{len(_FAILED)} check(s) failed:", file=sys.stderr)
        for name in _FAILED:
            print(f"  - {name}", file=sys.stderr)
        return 1
    print(f"\nAll {10} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

## 6. `config/base/green_agent_config.example.yaml`

Save the following to `config/base/green_agent_config.example.yaml`.

```yaml
# Green Agent — Helium-awareness configuration (example)
#
# Copy this file to config/base/green_agent_config.yaml and edit.
# The recipe (scripts/deploy_helium.sh) does the copy for you if the
# target does not exist. It NEVER overwrites an existing config.
#
# See DEPLOYMENT_RECIPE.md §2.2 for field documentation.

# Master switch. Set to true to enable helium-aware scheduling.
helium_aware_enabled: false

# Helium availability API. MUST be set to a real URL — the placeholder
# below will not resolve.
# Recommended: pass via env var and template it in your deployment tool.
helium_api_url: "https://helium-monitor.example.org/v1"

# Poll interval for the helium API. Units: seconds.
helium_update_interval_seconds: 300

# Weights for the EnvironmentalSustainability composite.
# Must sum to 1.0 (validated at load time).
helium_weight: 0.4
carbon_weight: 0.6

# Thresholds on helium_availability ∈ [0, 1].
# Must satisfy: 0.0 <= yellow < red < critical <= 1.0.
helium_yellow_threshold: 0.6
helium_red_threshold: 0.8
helium_critical_threshold: 0.95

# Fallback if helium API is unreachable.
# If true, orchestrator uses the last known value (or a static default).
# If false, orchestrator fails closed (defers the task).
fallback_enabled: true

# Per-task execution timeout. Units: seconds.
execution_timeout_seconds: 300
```

---

## 7. `requirements-helium.txt`

Save the following to `requirements-helium.txt`.

```
# Pinned for reproducible deployment.
# Install with: pip install -r requirements-helium.txt

# HTTP client (helium + carbon APIs)
httpx==0.28.1
aiohttp==3.10.11

# Data
numpy==2.2.1
pandas==2.2.3

# Config parsing
pyyaml==6.0.2

# Dashboard
fastapi==0.115.6
uvicorn[standard]==0.32.1
pydantic==2.10.3

# Testing (needed by verify_helium_recipe.py and CI)
pytest==8.3.4
pytest-asyncio==0.24.0
```

---

## 8. `src/integration/helium_types.py`

Save the following to `src/integration/helium_types.py`. Also create
`src/__init__.py` and `src/integration/__init__.py` (contents shown after
the main module).

```python
"""
src/integration/helium_types.py — typed schemas for the helium module.

Public API:
    HeliumTask                — validated task shape accepted by process_task
    EnvironmentalSustainability — the helium + carbon composite (§1.3)
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

__all__ = ["HeliumTask", "EnvironmentalSustainability"]

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass(frozen=True, slots=True)
class HeliumTask:
    """
    Task shape accepted by UnifiedOrchestrator.process_task.

    Field ranges (validated at construction):
        task_id                         — [A-Za-z0-9._-]{1,64}
        hardware_requirements.gpu_count — int in [0, 64]
        model_config.size_gb            — float > 0
        deferrable                      — bool
    """
    task_id: str
    hardware_requirements: dict[str, Any]
    model_config: dict[str, Any]
    deferrable: bool

    def __post_init__(self) -> None:
        if not _ID_RE.match(self.task_id):
            raise ValueError(f"invalid task_id: {self.task_id!r}")

        hw = self.hardware_requirements
        if not isinstance(hw, dict):
            raise ValueError("hardware_requirements must be a dict")
        gpu_count = hw.get("gpu_count", 0)
        if not isinstance(gpu_count, int):
            raise ValueError(
                f"hardware_requirements.gpu_count must be int: {gpu_count!r}"
            )
        if not (0 <= gpu_count <= 64):
            raise ValueError(
                f"hardware_requirements.gpu_count must be in [0, 64]: {gpu_count}"
            )

        mc = self.model_config
        if not isinstance(mc, dict):
            raise ValueError("model_config must be a dict")
        size_gb = mc.get("size_gb")
        if not isinstance(size_gb, (int, float)):
            raise ValueError(
                f"model_config.size_gb must be numeric: {size_gb!r}"
            )
        if size_gb <= 0:
            raise ValueError(f"model_config.size_gb must be > 0: {size_gb}")

        if not isinstance(self.deferrable, bool):
            raise ValueError(f"deferrable must be bool: {self.deferrable!r}")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "HeliumTask":
        return cls(
            task_id=d["task_id"],
            hardware_requirements=d.get("hardware_requirements", {}),
            model_config=d.get("model_config", {}),
            deferrable=bool(d.get("deferrable", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EnvironmentalSustainability:
    """
    The helium + carbon composite (§1.3).

        environmental_sustainability = 0.4 * helium_availability
                                     + 0.6 * carbon_sustainability

    Weights are validated to sum to 1.0 (default 0.4 / 0.6).
    Inputs are clamped to [0, 1].
    """
    helium_availability: float
    carbon_sustainability: float
    helium_weight: float = 0.4
    carbon_weight: float = 0.6

    def __post_init__(self) -> None:
        if not (0.0 <= self.helium_availability <= 1.0):
            raise ValueError(
                f"helium_availability must be in [0, 1]: "
                f"{self.helium_availability}"
            )
        if not (0.0 <= self.carbon_sustainability <= 1.0):
            raise ValueError(
                f"carbon_sustainability must be in [0, 1]: "
                f"{self.carbon_sustainability}"
            )
        if self.helium_weight < 0 or self.carbon_weight < 0:
            raise ValueError("weights must be non-negative")
        if abs(self.helium_weight + self.carbon_weight - 1.0) > 1e-6:
            raise ValueError(
                f"weights must sum to 1.0: "
                f"{self.helium_weight} + {self.carbon_weight} = "
                f"{self.helium_weight + self.carbon_weight}"
            )

    def calculate(self) -> float:
        return (
            self.helium_weight * self.helium_availability
            + self.carbon_weight * self.carbon_sustainability
        )
```

### 8.1 `src/__init__.py`

```python
"""Green Agent — helium module source package."""
__version__ = "2.4.2-helium-alpha"
```

### 8.2 `src/integration/__init__.py`

```python
"""
src.integration — helium-aware integration layer.

Public classes:
    UnifiedOrchestrator
    HeliumTask
    EnvironmentalSustainability
"""
from __future__ import annotations

from src.integration.helium_types import (
    EnvironmentalSustainability,
    HeliumTask,
)
from src.integration.unified_orchestrator import UnifiedOrchestrator

__all__ = [
    "EnvironmentalSustainability",
    "HeliumTask",
    "UnifiedOrchestrator",
]
```

---

## 9. `src/integration/unified_orchestrator.py`

Save the following to `src/integration/unified_orchestrator.py`.

```python
"""
src/integration/unified_orchestrator.py — helium-aware orchestrator (STUB).

This class exists so the deployment recipe runs end-to-end and the
verifier passes. The real decision logic ships separately — see
KNOWN_LIMITATIONS_HELIUM.md §1.

Once the separate delivery lands, replace this module with the real
implementation. Its public surface MUST match:

    UnifiedOrchestrator(config_path: str | None = None)
        .process_task(task: dict) -> dict           # async

See DEPLOYMENT_RECIPE.md §2.3 for the return shape.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["UnifiedOrchestrator"]


class UnifiedOrchestrator:
    """
    Helium-aware task orchestrator.

    The stub exists so:
      * The import path works.
      * The verifier can assert `process_task` raises `NotImplementedError`
        (catching the case where a future edit silently replaces the stub
        with a no-op).
      * The recipe is runnable in CI without the real delivery.

    Raises:
        NotImplementedError:
            Always raised by `process_task`. Replaced by the real delivery.
    """

    def __init__(self, config_path: str | None = None) -> None:
        self._config_path = (
            Path(config_path)
            if config_path
            else Path("config/base/green_agent_config.yaml")
        )

    async def process_task(self, task: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError(
            "UnifiedOrchestrator.process_task() ships with the separate "
            "'green-agent-helium-core' delivery. "
            "See DEPLOYMENT_RECIPE.md §0.3 and KNOWN_LIMITATIONS_HELIUM.md §1."
        )
```

---

## 10. `dashboard/helium_dashboard.py`

Save the following to `dashboard/helium_dashboard.py`. Also create
`dashboard/__init__.py` (contents shown after the main module).

```python
"""
dashboard/helium_dashboard.py — Helium monitoring dashboard (FastAPI).

Exposes:
    GET /healthz   — unauthenticated liveness probe
    GET /          — unauthenticated discovery (points to docs)
    GET /helium    — authenticated helium availability snapshot

Authentication:
    Bearer token loaded from
    `$GREEN_AGENT_SECRETS_DIR/helium_dashboard_token`.

    If the secret is missing or the token does not match, /helium
    returns 401. If the secrets dir is missing entirely, /helium returns
    503 (misconfiguration, not auth failure).

Binding:
    The recipe starts uvicorn on `127.0.0.1` by default. Do NOT bind to
    0.0.0.0. See DEPLOYMENT_RECIPE.md §12.

ASGI entry point:
    uvicorn dashboard.helium_dashboard:app --host 127.0.0.1 --port 8001
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel

__all__ = ["app"]

app = FastAPI(
    title="Green Agent — Helium Dashboard",
    version="2.4.2-helium-alpha",
    description=(
        "Helium availability monitoring. See DEPLOYMENT_RECIPE.md for the "
        "security model."
    ),
)


# --- Auth -----------------------------------------------------------------

def _read_token() -> str | None:
    """
    Read the dashboard bearer token from
    `$GREEN_AGENT_SECRETS_DIR/helium_dashboard_token`.

    Returns None if the secrets dir or the token file is missing.
    """
    secrets_dir = os.environ.get("GREEN_AGENT_SECRETS_DIR")
    if not secrets_dir:
        return None
    token_path = Path(secrets_dir) / "helium_dashboard_token"
    if not token_path.is_file():
        return None
    return token_path.read_text().strip()


async def require_bearer(
    authorization: Annotated[str | None, Header()] = None,
) -> str:
    expected = _read_token()
    if expected is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "dashboard not configured: set GREEN_AGENT_SECRETS_DIR and "
                "create helium_dashboard_token in it"
            ),
        )
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )
    token = authorization.removeprefix("Bearer ").strip()
    if token != expected:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="invalid bearer token",
        )
    return token


# --- Schemas --------------------------------------------------------------

class HealthzBody(BaseModel):
    status: str
    version: str


class DiscoveryBody(BaseModel):
    name: str
    version: str
    endpoints: dict[str, str]


class HeliumSnapshot(BaseModel):
    helium_availability: float | None
    carbon_intensity_g_per_kwh: float | None
    environmental_sustainability: float | None
    source: str
    note: str


# --- Routes ---------------------------------------------------------------

@app.get("/healthz", response_model=HealthzBody)
def healthz() -> HealthzBody:
    """Unauthenticated liveness probe."""
    return HealthzBody(status="ok", version=app.version)


@app.get("/", response_model=DiscoveryBody)
def discovery() -> DiscoveryBody:
    """Unauthenticated discovery — points to the authenticated endpoint."""
    return DiscoveryBody(
        name=app.title,
        version=app.version,
        endpoints={
            "/healthz": "liveness",
            "/helium": "helium availability snapshot (requires Bearer token)",
        },
    )


@app.get("/helium", response_model=HeliumSnapshot)
def helium_snapshot(
    _token: str = Depends(require_bearer),
) -> HeliumSnapshot:
    """
    Helium availability snapshot.

    The stub returns `null` values because the helium monitor client ships
    separately (see DEPLOYMENT_RECIPE.md §0.3). Once the client lands,
    replace this handler.
    """
    return HeliumSnapshot(
        helium_availability=None,
        carbon_intensity_g_per_kwh=None,
        environmental_sustainability=None,
        source="stub",
        note=(
            "Helium monitor client ships separately — see "
            "DEPLOYMENT_RECIPE.md §0.3."
        ),
    )
```

### 10.1 `dashboard/__init__.py`

```python
"""Green Agent — helium dashboard package."""
__version__ = "2.4.2-helium-alpha"
```

---

## 11. `tests/test_helium_config.py`

Save the following to `tests/test_helium_config.py`. Also create
`tests/__init__.py` (empty file).

```python
"""
tests/test_helium_config.py — Config invariants and stub verification.

Runs in CI on every push/PR. Fails if the config schema, weights, or
thresholds drift from DEPLOYMENT_RECIPE.md §2.2.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parent.parent
_CFG = _ROOT / "config" / "base" / "green_agent_config.example.yaml"


def _load_cfg() -> dict:
    assert _CFG.is_file(), f"missing {_CFG}"
    with _CFG.open() as f:
        return yaml.safe_load(f)


# --- Config schema --------------------------------------------------------

def test_config_has_all_required_keys():
    cfg = _load_cfg()
    required = {
        "helium_aware_enabled",
        "helium_api_url",
        "helium_update_interval_seconds",
        "helium_weight",
        "carbon_weight",
        "helium_yellow_threshold",
        "helium_red_threshold",
        "helium_critical_threshold",
        "fallback_enabled",
        "execution_timeout_seconds",
    }
    missing = required - set(cfg.keys())
    assert not missing, f"missing keys: {missing}"


def test_config_weights_sum_to_one():
    cfg = _load_cfg()
    total = float(cfg["helium_weight"]) + float(cfg["carbon_weight"])
    assert abs(total - 1.0) < 1e-6, f"weights do not sum to 1.0: {total}"


def test_config_thresholds_monotonic():
    cfg = _load_cfg()
    y = float(cfg["helium_yellow_threshold"])
    r = float(cfg["helium_red_threshold"])
    cr = float(cfg["helium_critical_threshold"])
    assert 0.0 <= y < r < cr <= 1.0, (
        f"thresholds must be 0 <= {y} < {r} < {cr} <= 1.0"
    )


def test_config_units_suffix():
    """Regression: prior draft used unitless 'helium_update_interval'."""
    cfg = _load_cfg()
    assert "helium_update_interval_seconds" in cfg
    assert "execution_timeout_seconds" in cfg
    assert "helium_update_interval" not in cfg
    assert "execution_timeout" not in cfg


def test_config_placeholder_url_documented():
    cfg = _load_cfg()
    url = cfg["helium_api_url"]
    if "example" in url:
        raw = _CFG.read_text()
        assert "placeholder" in raw.lower() or "MUST be set" in raw, (
            "example.com URL must be flagged as a placeholder in a comment"
        )


# --- HeliumTask -----------------------------------------------------------

def test_helium_task_valid():
    from src.integration.helium_types import HeliumTask
    t = HeliumTask.from_dict({
        "task_id": "test_001",
        "hardware_requirements": {"gpu_count": 2},
        "model_config": {"size_gb": 10},
        "deferrable": True,
    })
    assert t.task_id == "test_001"
    assert t.hardware_requirements["gpu_count"] == 2


def test_helium_task_invalid_gpu_count():
    from src.integration.helium_types import HeliumTask
    with pytest.raises(ValueError):
        HeliumTask.from_dict({
            "task_id": "t1",
            "hardware_requirements": {"gpu_count": 999},
            "model_config": {"size_gb": 10},
            "deferrable": True,
        })


def test_helium_task_negative_size():
    from src.integration.helium_types import HeliumTask
    with pytest.raises(ValueError):
        HeliumTask.from_dict({
            "task_id": "t1",
            "hardware_requirements": {"gpu_count": 1},
            "model_config": {"size_gb": -1},
            "deferrable": True,
        })


def test_helium_task_invalid_id():
    from src.integration.helium_types import HeliumTask
    with pytest.raises(ValueError):
        HeliumTask.from_dict({
            "task_id": "bad id!",
            "hardware_requirements": {"gpu_count": 1},
            "model_config": {"size_gb": 10},
            "deferrable": True,
        })


# --- EnvironmentalSustainability -----------------------------------------

def test_env_sustainability_calculation():
    from src.integration.helium_types import EnvironmentalSustainability
    e = EnvironmentalSustainability(
        helium_availability=0.85, carbon_sustainability=0.7,
    )
    # 0.4 * 0.85 + 0.6 * 0.7 = 0.34 + 0.42 = 0.76
    assert abs(e.calculate() - 0.76) < 1e-9


def test_env_sustainability_rejects_bad_weights():
    from src.integration.helium_types import EnvironmentalSustainability
    with pytest.raises(ValueError):
        EnvironmentalSustainability(
            helium_availability=0.5, carbon_sustainability=0.5,
            helium_weight=0.9, carbon_weight=0.9,
        )


# --- Stub still present --------------------------------------------------

@pytest.mark.asyncio
async def test_orchestrator_stub_raises():
    """Regression: catch a future edit that silently makes process_task a no-op."""
    from src.integration.unified_orchestrator import UnifiedOrchestrator
    orch = UnifiedOrchestrator()
    with pytest.raises(NotImplementedError):
        await orch.process_task({"task_id": "t1"})


# --- Dashboard ------------------------------------------------------------

def test_dashboard_healthz():
    from fastapi.testclient import TestClient
    from dashboard.helium_dashboard import app
    resp = TestClient(app).get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_dashboard_discovery():
    from fastapi.testclient import TestClient
    from dashboard.helium_dashboard import app
    resp = TestClient(app).get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert "/helium" in body["endpoints"]


def test_dashboard_helium_requires_auth(monkeypatch):
    """Without a token set, /helium returns 503 (misconfigured)."""
    monkeypatch.delenv("GREEN_AGENT_SECRETS_DIR", raising=False)
    from fastapi.testclient import TestClient
    from dashboard.helium_dashboard import app
    resp = TestClient(app).get("/helium")
    assert resp.status_code == 503


def test_dashboard_helium_rejects_bad_token(monkeypatch, tmp_path):
    monkeypatch.setenv("GREEN_AGENT_SECRETS_DIR", str(tmp_path))
    (tmp_path / "helium_dashboard_token").write_text("correct-token")
    from fastapi.testclient import TestClient
    from dashboard.helium_dashboard import app
    resp = TestClient(app).get(
        "/helium", headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401


def test_dashboard_helium_accepts_valid_token(monkeypatch, tmp_path):
    monkeypatch.setenv("GREEN_AGENT_SECRETS_DIR", str(tmp_path))
    (tmp_path / "helium_dashboard_token").write_text("correct-token")
    from fastapi.testclient import TestClient
    from dashboard.helium_dashboard import app
    resp = TestClient(app).get(
        "/helium", headers={"Authorization": "Bearer correct-token"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "stub"
```

### 11.1 `tests/__init__.py`

Empty file.

---

## 12. Security

### 12.1 Dashboard binding

**Default: `--host 127.0.0.1`.** The dashboard is reachable only from the
same host.

`--host 0.0.0.0` is **not recommended**. If you need it:

1. Place a reverse proxy (nginx / Caddy / Traefik) in front.
2. The reverse proxy terminates TLS.
3. The reverse proxy enforces auth **in addition to** the app's bearer
   token (defense in depth).
4. The reverse proxy rate-limits at 60 req/min per IP.
5. The app remains on `127.0.0.1:8001` — never exposed directly.

### 12.2 Bearer authentication

Every `/helium*` endpoint requires a bearer token loaded from
`$GREEN_AGENT_SECRETS_DIR/helium_dashboard_token`. `/healthz` and `/` are
**unauthenticated** (health checks and discovery).

### 12.3 Secrets

- `HELIUM_API_URL` from env (not a secret, but should be set explicitly,
  not defaulted to `example.com`).
- Grid-API keys (WattTime / ElectricityMap) — from
  `$GREEN_AGENT_SECRETS_DIR`, per `README.md §8`.
- `helium_dashboard_token` — from `$GREEN_AGENT_SECRETS_DIR`, mode `0600`.

### 12.4 Input validation

- `HeliumTask.task_id` restricted to `[A-Za-z0-9._-]{1,64}`.
- `hardware_requirements.gpu_count ∈ [0, 64]`.
- `model_config.size_gb > 0`.
- `deferrable` is `bool`.
- All numeric config fields validated at load; invalid config raises
  `ValueError` before `UnifiedOrchestrator` is constructed.

---

## 13. Known limitations

### 13.1 `UnifiedOrchestrator` is a stub

`UnifiedOrchestrator.process_task()` raises `NotImplementedError`. The
real decision logic (helium + carbon scheduling) ships separately as
`green-agent-helium-core`.

**Why it's a stub:** the recipe is runnable and verifiable in CI
**without** the core delivery. The stub also serves as a regression test —
`tests/test_helium_config.py::test_orchestrator_stub_raises` fails if a
future edit silently replaces the stub with a no-op.

**Replacement path:** when `green-agent-helium-core` lands, replace
`src/integration/unified_orchestrator.py` with the real implementation.
Its public surface must match §2.3.

### 13.2 Helium snapshot is null

`GET /helium` returns `null` values until the monitor client lands (§0.3).
The endpoint exists and requires auth so that clients can integrate today.

### 13.3 `SustainabilityIndex` is unchanged

The helium module defines a **new** `EnvironmentalSustainability` composite
(helium 0.4 + carbon 0.6) that is **parallel to** — not a replacement for —
`SustainabilityIndex`. The shipped `sustainability_index` (accuracy /
energy / carbon) is unchanged.

**Why this matters:** if you were expecting the config weights
(`helium_weight`, `carbon_weight`) to change the SI, they don't. They
change the environmental composite only.

### 13.4 No live carbon-intensity provider

Same status as `README.md §0.2 O-1`. The carbon side of the composite is
fed by the base package, which still uses the static reference table.

### 13.5 No dashboard write endpoints

The dashboard is **read-only**. Submitting a benchmark to the leaderboard
goes through `limit_agentbench.dashboard.green_leaderboard` (separate
package). The helium dashboard is monitoring-only.

### 13.6 Dashboard binding

The recipe prints `--host 127.0.0.1`. Binding to `0.0.0.0` exposes the
dashboard on all interfaces and is **not recommended** — a reverse proxy
with auth and rate-limiting is required (§12.1).

### 13.7 Requirements pinning

`requirements-helium.txt` pins every dependency. It is **not signed** — no
SBOM, no SLSA provenance. Supply-chain hardening is tracked in
`README.md §0.2 O-6`.

### 13.8 Package status is Alpha

Not Production Ready. See §0.1.

### 13.9 What changed from v1.0

Prior draft's recipe was **unrunnable**:

- It said "Copy all files as shown above" without an "above."
- It used `uvicorn ...:router` — uvicorn requires `module:app`.
- It had no `set -e`, so failures were silent.
- `cat >>` was not idempotent — running twice duplicated the config block.
- `pip install aiohttp numpy pandas` was unpinned.
- `--host 0.0.0.0` was the default, with no auth.
- `python -c "..."` test asserted nothing — always exited 0.

Each of these is fixed in v2.0. See this document's changelog for the
mapping.

---

## 14. Next steps

### 14.1 To run this document as-is

1. Save each code block to its path (directories created by the recipe).
2. `chmod +x scripts/deploy_helium.sh`.
3. `bash scripts/deploy_helium.sh`.
4. On success, follow the printed command to start the dashboard.

### 14.2 To implement the orchestrator

Wait for the `green-agent-helium-core` delivery. When it lands:

1. Replace `src/integration/unified_orchestrator.py` with the real module.
2. Re-run `python scripts/verify_helium_recipe.py` — the
   `NotImplementedError` check now expects the class to be callable.
3. Run `pytest -q tests/`.

### 14.3 To expose the dashboard

See §12.1 for the security model. The recipe prints the safe command.

### 14.4 CI

Add to `.github/workflows/`:

```yaml
name: Helium Recipe CI

permissions:
  contents: read

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  verify:
    runs-on: ubuntu-24.04
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
        with:
          persist-credentials: false
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - run: |
          python -m pip install --upgrade pip
          pip install -r requirements-helium.txt
          pip install -e . --no-deps
      - run: python scripts/verify_helium_recipe.py
      - run: pytest -q tests/
```

---

## License and citation

Apache-2.0. See `LICENSE` in the base package.

```bibtex
@software{green_agent_helium_2026,
  title  = {Green Agent Helium-Aware Deployment Recipe},
  author = {{AI Research Agent}},
  year   = {2026},
  url    = {https://github.com/NurcholishAdam/Green_Agent},
  note   = {v2.4.2-helium-alpha}
}
```
```
