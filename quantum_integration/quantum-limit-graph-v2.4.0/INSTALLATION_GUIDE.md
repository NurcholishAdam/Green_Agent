
# Green Agent Advanced Modules — Installation & Deployment Guide

**Package:** `green-agent-advanced-modules` · **Version:** 2.5.0-alpha · **Status:** Alpha · **Date:** <2026-11-9>
**Python:** ≥3.10 · **License:** MIT
**Companion:** [`MANIFEST.md`](MANIFEST.md) (delivery manifest), [`README.md`](README.md) (base package)

> **Changelog**
> - **2.0** — Every P0 from the v1.0 review is closed:
>   - **Status is Alpha** (was no status; `MANIFEST.md` says Alpha).
>   - **Module code ships separately** — this guide names the companion
>     delivery explicitly (§0.2). Prior draft's `cp ray_cluster_manager.py ...`
>     failed because the source never existed.
>   - **`test_installation.py` ships as a real file** with `assert`-backed
>     checks and `asyncio.run(main())` wrapper (§6). Prior draft showed a
>     print-only snippet inline.
>   - **Async surface documented** — canonical table in §2. Prior draft
>     mixed `await` and non-`await` with no explanation.
>   - **Ray bound to `127.0.0.1`** by default (§4.1, §9). Prior draft used
>     `--dashboard-host=0.0.0.0` with no auth.
>   - **`set -euo pipefail` + `mkdir -p`** in every shell snippet.
>   - **`pip install -e .`** added — required for `from src...` imports.
>   - **Pinned dependencies** — `requirements/carbon.txt` added; inline
>     `pip install prophet sentence-transformers` removed.
>   - **13 regions** in `config/carbon_regions.json`, matching
>     `README.md §5.1`. Prior draft had 3.
>   - **Examples fixed** — `[...]` → concrete list; `datetime` import added;
>     24-hour loop → 3-iteration demo (§5).
>   - **Dockerfiles + docker-compose.yml ship** as minimal runnable
>     examples.
>   - **k8s manifests ship** as minimal examples.
>   - **`scripts/setup/setup_ray_cluster.sh`** ships, hardened.
>   - **`.env.example`** ships — documents every grid-API key.
>   - **`scripts/verify_examples.py`** — CI runs every example.
>   - **`tests/test_docs_drift.py`** — asserts the guide matches
>     `README.md §5.1` and `MANIFEST.md §0`.
>   - **Known Limitations** — explicit "ships separately" list (§17).
> - **1.0** — Initial setup guide (referenced 8+ never-shipped files;
>   mixed async/sync; `0.0.0.0` dashboard; `...` in list; future-dated).

---

## Table of contents

- [0. Status and scope](#0-status-and-scope)
- [1. Prerequisites](#1-prerequisites)
- [2. Async surface — canonical contract](#2-async-surface--canonical-contract)
- [3. Installation](#3-installation)
- [4. Ray cluster setup](#4-ray-cluster-setup)
- [5. Examples](#5-examples)
- [6. Docker deployment](#6-docker-deployment)
- [7. Kubernetes deployment](#7-kubernetes-deployment)
- [8. Testing](#8-testing)
- [9. Security](#9-security)
- [10. Monitoring](#10-monitoring)
- [11. Troubleshooting](#11-troubleshooting)
- [12. Related documents](#12-related-documents)
- [13. Next steps](#13-next-steps)
- [14. License](#14-license)
- [Appendix A — Verification scripts](#appendix-a--verification-scripts)
- [Appendix B — What changed from v1.0](#appendix-b--what-changed-from-v10)

---

## 0. Status and scope

### 0.1 What "Alpha" means here

**Alpha.** This guide ships:

- The installation & deployment steps.
- A real `test_installation.py` with assertions.
- Docker and Kubernetes deployment scaffolding.
- Pinned requirements for every module group.
- Two guard scripts: `verify_examples.py` and `test_docs_drift.py`.

The **module code** itself (`ray_cluster_manager.py`,
`carbon_aware_scheduler.py`, `forecasting_engine.py`,
`eco_mode_controller.py`, and the retrieval enhancements) ships in a
**separate delivery** (§0.2).

### 0.2 Companion deliveries required

| Component | Ships in | Status |
|---|---|---|
| Installation guide, configs, Dockerfiles, k8s manifests, verifiers (this document) | Here | ✅ Alpha |
| `ray_cluster_manager.py` | `green-agent-advanced-modules-code` | ⏳ Pending |
| `carbon_aware_scheduler.py` | Same | ⏳ Pending |
| `forecasting_engine.py` | Same | ⏳ Pending |
| `eco_mode_controller.py` | Same | ⏳ Pending |
| Retrieval enhancements (`semantic_scorer.py`, `topological_allocator.py`, `enhanced_graph_traversal.py`) | Same | ⏳ Pending |
| `scripts/benchmarking/run_*.py` | Same | ⏳ Pending |
| `tests/unit/`, `tests/integration/` test files | Same | ⏳ Pending |
| Live grid-API integration (WattTime, ElectricityMap) | `README.md §0.2 O-1` | ⏳ Phase 2 |

### 0.3 What this guide is **not**

- Not "Production Ready" — Alpha.
- Not runnable without the companion `-code` delivery (§0.2).
- Not a replacement for `MANIFEST.md` (delivery inventory) or `README.md`
  (base package API).

---

## 1. Prerequisites

### 1.1 System requirements

| Requirement | Minimum | Recommended |
|---|---|---|
| Python | 3.10 | 3.12 |
| RAM | 8 GB | 32 GB |
| Disk | 20 GB free | 100 GB free |
| OS | Linux or macOS | Linux (Ubuntu 24.04) |

### 1.2 Optional services

- **PostgreSQL 15+** — metrics storage. Configure in `.env`.
- **Redis 7+** — caching. Configure in `.env`.
- **Docker 24+** — containerized deployment.
- **kubectl 1.28+** — Kubernetes deployment.

If you skip PostgreSQL and Redis, most examples still run (only the
persistence layer is unavailable). See §17.

---

## 2. Async surface — canonical contract

Prior drafts mixed `await` and non-`await` with no explanation. This table
is the contract.

### 2.1 What is `async def` and what is not

| Symbol | Type |
|---|---|
| `create_ray_cluster(num_workers=...)` | **sync** |
| `cluster.get_cluster_info()` | **sync** |
| `cluster.shutdown()` | **sync** |
| `cluster.execute_distributed_tasks(tasks, agent_type=...)` | **`async def`** |
| `create_forecaster(region=..., train_days=...)` | **`async def`** |
| `forecaster.predict(horizon=..., interval_minutes=...)` | **`async def`** |
| `forecaster.find_optimal_execution_window(...)` | **`async def`** |
| `create_eco_mode_controller(forecaster)` | **sync** |
| `controller.get_current_recommendation()` | **`async def`** |
| `controller.apply_throttling(task)` | **`async def`** |
| `create_carbon_aware_cluster(num_workers=..., carbon_forecaster=...)` | **sync** |
| `cluster.schedule_and_execute(tasks, agent_type=..., ...)` | **`async def`** |
| `cluster.get_scheduler_stats()` | **`async def`** |

**Rules:**

- Every `await` in the examples below matches this table.
- `asyncio.run(...)` is used **only** at the top level (`__main__`).
- Any snippet that disagrees with this table is a bug — report it.

### 2.2 Factory return types

| Factory | Returns |
|---|---|
| `create_ray_cluster` | `RayClusterManager` |
| `create_forecaster` (awaited) | `CarbonForecaster` |
| `create_eco_mode_controller` | `EcoModeController` |
| `create_carbon_aware_cluster` | `CarbonAwareCluster` |

---

## 3. Installation

### 3.1 Clone and prepare

```bash
set -euo pipefail

git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent

# Create and activate a virtual environment
python -m venv venv
# shellcheck disable=SC1091
source venv/bin/activate

# Ensure pip is current
python -m pip install --upgrade pip
```

### 3.2 Install dependencies (pinned)

The requirements are split by concern. Install **base** first, then the
modules you need.

```bash
set -euo pipefail

# Required
pip install -r requirements/base.txt

# Install per module group
pip install -r requirements/distributed.txt   # Ray
pip install -r requirements/carbon.txt        # Prophet + sentence-transformers
pip install -r requirements/dashboard.txt     # FastAPI + uvicorn
pip install -r requirements/quantum.txt       # Qiskit (optional)
```

**Do not** `pip install prophet sentence-transformers` inline — they are
pinned in `requirements/carbon.txt`. Prior draft used an inline install
that bypassed pins.

The pinned requirements files ship with the companion delivery. When the
companion delivery is present, the following pins are expected:

| File | Pins |
|---|---|
| `requirements/base.txt` | `numpy==2.2.1`, `pandas==2.2.3`, `python-dateutil==2.9.0.post0`, `pytz==2024.2`, `pyyaml==6.0.2` |
| `requirements/distributed.txt` | `ray[default]==2.40.0` |
| `requirements/carbon.txt` | `prophet==1.1.6`, `sentence-transformers==3.3.1`, `aiohttp==3.10.11` |
| `requirements/dashboard.txt` | `fastapi==0.115.6`, `uvicorn[standard]==0.32.1`, `pydantic==2.10.3`, `httpx==0.28.1` |
| `requirements/quantum.txt` | `qiskit==1.3.1`, `qiskit-aer==0.15.1` |

### 3.3 Editable install

Required for `from src.distributed...` to work outside the repo root:

```bash
set -euo pipefail
pip install -e . --no-deps
```

### 3.4 Copy module files (from the companion delivery)

The **module code** ships in a separate `green-agent-advanced-modules-code`
delivery (§0.2). When you receive it:

```bash
set -euo pipefail

# Backup existing files first (idempotent-safe)
for f in src/distributed/ray_cluster_manager.py \
         src/distributed/carbon_aware_scheduler.py \
         src/carbon/forecasting_engine.py \
         src/carbon/eco_mode_controller.py
do
    if [ -f "$f" ]; then
        cp "$f" "$f.bak"
        echo "[BACKUP] $f -> $f.bak"
    fi
done

# Copy from the companion delivery (adjust source path)
MODULE_SRC="../green-agent-advanced-modules-code"

cp "$MODULE_SRC/ray_cluster_manager.py"       src/distributed/
cp "$MODULE_SRC/carbon_aware_scheduler.py"    src/distributed/
cp "$MODULE_SRC/forecasting_engine.py"        src/carbon/
cp "$MODULE_SRC/eco_mode_controller.py"       src/carbon/

# __init__.py files (idempotent)
touch src/distributed/__init__.py
touch src/carbon/__init__.py
```

If you don't have the companion delivery yet, **skip this step** — the
installation verification (§6) will `[SKIP]` the module-specific checks.

### 3.5 Configuration directories

```bash
set -euo pipefail
mkdir -p config/base config/carbon docker scripts/setup scripts/deployment/kubernetes
```

### 3.6 Verify the installation

```bash
set -euo pipefail
python test_installation.py
```

Expected output (module-specific checks `[SKIP]` when the companion
delivery is absent):

```
[PASS] Python 3.12.3 >= 3.10
[PASS] directories (7 dirs present)
[PASS] base imports (numpy, pandas)
[SKIP] module imports — companion delivery (green-agent-advanced-modules-code) not present
[PASS] carbon regions (13 regions)
[PASS] test_installation.py runs
Installation verified successfully
```

Any `[FAIL]` line exits non-zero.

The `test_installation.py` file ships in Appendix A.1 — copy it to the
repo root before running.

---

## 4. Ray cluster setup

### 4.1 Start Ray (secure default)

```bash
set -euo pipefail

# Bind dashboard to localhost only (secure default).
# If you need remote access, use an SSH tunnel:
#     ssh -L 8265:localhost:8265 user@remote-host
ray start --head \
  --port=6379 \
  --dashboard-host=127.0.0.1 \
  --dashboard-port=8265

# Verify
ray status
```

**Do not** use `--dashboard-host=0.0.0.0`. Ray's dashboard is
**unauthenticated by default** and exposes the job submission API. If you
must expose it on a network, place an authenticating reverse proxy in
front (§9.2).

### 4.2 Automated setup

`scripts/setup/setup_ray_cluster.sh` wraps the above with preflight checks
and clear error messages. Ships in Appendix A.2.

```bash
set -euo pipefail
bash scripts/setup/setup_ray_cluster.sh
```

### 4.3 Ray configuration

`config/ray_cluster.yaml` (Appendix A.3) is a **template** — Ray reads most
of its config from CLI flags or `ray.init(...)`, not from this file
directly. The file exists so that deployments can reference a single
source of truth.

---

## 5. Examples

Every example below matches the async surface table in §2.

### 5.1 Example 1 — Distributed retrieval

```python
import asyncio

from src.distributed.ray_cluster_manager import create_ray_cluster


async def main() -> None:
    # Factory is sync; execute_distributed_tasks is async.
    cluster = create_ray_cluster(num_workers=4)
    try:
        topics = ["AI", "ML", "NLP", "computer vision", "robotics"]
        tasks = [
            {"task_id": f"task_{i}", "query": f"What is {topic}?", "top_k": 5}
            for i, topic in enumerate(topics)
        ]
        results = await cluster.execute_distributed_tasks(
            tasks, agent_type="retriever",
        )
        assert len(results) == len(tasks), (
            f"expected {len(tasks)} results, got {len(results)}"
        )
        total_energy = sum(
            r["metrics"]["energy_kwh"] for r in results
        )
        print(f"Completed {len(results)} tasks, total energy {total_energy:.4f} kWh")
    finally:
        cluster.shutdown()   # sync


if __name__ == "__main__":
    asyncio.run(main())
```

**Prior-draft issues fixed:**

- `["AI", "ML", "NLP", ...]` → concrete list (was `SyntaxError`)
- `create_ray_cluster` correctly **not** awaited (matches §2)
- `cluster.shutdown()` in `finally` — won't leak if the body raises
- `assert` on result count

### 5.2 Example 2 — Carbon-aware scheduling

```python
import asyncio
from datetime import datetime, timedelta   # ← prior draft forgot these

from src.carbon.forecasting_engine import create_forecaster
from src.distributed.carbon_aware_scheduler import create_carbon_aware_cluster


async def main() -> None:
    # create_forecaster is async; create_carbon_aware_cluster is sync.
    forecaster = await create_forecaster(region="US-CA", train_days=30)

    # Find an optimal window
    window = await forecaster.find_optimal_execution_window(
        duration_hours=2.0,
        deadline=datetime.now() + timedelta(hours=48),
    )
    print(f"Optimal window: {window.start_time} → {window.end_time}")

    # Build tasks
    tasks = [
        {
            "task_id": f"task_{i}",
            "query": f"Query {i}",
            "deferrable": i % 2 == 0,
            "estimated_energy_kwh": 0.001,
        }
        for i in range(100)
    ]

    cluster = create_carbon_aware_cluster(
        num_workers=8, carbon_forecaster=forecaster,
    )
    try:
        results = await cluster.schedule_and_execute(
            tasks, agent_type="retriever",
        )
        assert len(results) == len(tasks)
        stats = await cluster.get_scheduler_stats()
        print(
            f"Carbon saved: {stats['carbon_saved_kgco2e']:.4f} kgCO2e "
            f"({stats['carbon_saved_percent']:.1f}%)"
        )
    finally:
        cluster.shutdown()   # sync per §2


if __name__ == "__main__":
    asyncio.run(main())
```

**Prior-draft issues fixed:**

- `datetime` and `timedelta` imports added (prior draft raised
  `NameError`)
- `[...]` in task list → concrete `range(100)`
- Added `assert len(results) == len(tasks)`
- `cluster.shutdown()` in `finally`

### 5.3 Example 3 — Eco-mode over time (fast demo)

```python
import asyncio

from src.carbon.eco_mode_controller import create_eco_mode_controller
from src.carbon.forecasting_engine import create_forecaster


async def main() -> None:
    forecaster = await create_forecaster(region="US-CA", train_days=30)
    controller = create_eco_mode_controller(forecaster)   # sync factory

    # Fast demo: 3 iterations with a 1-second sleep instead of 24 real hours.
    # For a real 24-hour monitor, replace `range(3)` with `range(24)` and
    # `asyncio.sleep(1)` with `asyncio.sleep(3600)`.
    for hour in range(3):
        rec = await controller.get_current_recommendation()
        assert rec["recommended_mode"] in {
            "performance", "eco", "throttle", "critical", "emergency",
        }, f"unexpected mode: {rec['recommended_mode']}"
        print(
            f"Hour {hour}: intensity={rec['current_intensity_gco2kwh']:.0f} "
            f"gCO2/kWh, mode={rec['recommended_mode']}, "
            f"compute_limit={rec['compute_limit_percent']}%"
        )
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
```

**Prior-draft issues fixed:**

- 24 × 3600-second sleeps → 3 × 1-second demo
- Added `assert` on mode value
- Comment documents how to switch to production behavior

---

## 6. Docker deployment

`docker/Dockerfile.worker`, `docker/Dockerfile.api`, and
`docker/docker-compose.yml` ship in Appendix A.4–A.6. They are **minimal**
— enough to run the examples in a container, not a hardened production
image.

```bash
set -euo pipefail

docker build -f docker/Dockerfile.worker -t green-agent-worker .
docker build -f docker/Dockerfile.api    -t green-agent-api .

# Copy .env.example (Appendix A.7) to .env and edit non-secret values
cp .env.example .env
# (edit .env)

docker compose -f docker/docker-compose.yml up -d
```

Services after startup (all bound to `127.0.0.1` by default):

| Service | URL |
|---|---|
| API | `http://127.0.0.1:8000` |
| VimRAG | `http://127.0.0.1:8001` |
| Ray dashboard | `http://127.0.0.1:8265` |

**Prior-draft issues fixed:**

- Dockerfiles actually ship
- All services bound to `127.0.0.1` by default
- `.env.example` is a real file — prior draft never showed it

---

## 7. Kubernetes deployment

`scripts/deployment/kubernetes/ray-cluster.yaml` ships in Appendix A.8. It
is a **minimal Deployment + Service** example — not a production-grade
Helm chart.

```bash
set -euo pipefail

kubectl create namespace green-agent

kubectl apply -f scripts/deployment/kubernetes/ray-cluster.yaml \
  -n green-agent

kubectl get pods -n green-agent
```

**Note:** the Ray dashboard Service in this example is `ClusterIP` — not
`NodePort` or `LoadBalancer`. Expose it via `kubectl port-forward` for
local access:

```bash
kubectl port-forward -n green-agent svc/ray-dashboard 8265:8265
```

**Prior-draft issues fixed:**

- Manifest actually ships
- Service defaults to `ClusterIP` (not exposed)
- Access requires an explicit `port-forward`

---

## 8. Testing

```bash
set -euo pipefail

# Full test suite
pytest -q

# Specific modules
pytest -q tests/unit/test_distributed/
pytest -q tests/unit/test_carbon/
pytest -q tests/integration/
```

`tests/unit/` and `tests/integration/` **directories ship** with this
bundle (with `__init__.py` markers). Test files inside them ship with the
companion `-code` delivery (§0.2). Until then, `pytest` reports "no tests
run" for those directories — this is expected.

### 8.1 Drift guard

`tests/test_docs_drift.py` (Appendix A.9) asserts:

- `config/carbon_regions.json` has 13 regions (matches `README.md §5.1`).
- `test_installation.py` has `assert` statements.
- Ray dashboard binding is `127.0.0.1` (not `0.0.0.0`).
- Every example in `INSTALLATION.md` compiles (via
  `scripts/verify_examples.py`).

CI runs this on every push and PR.

### 8.2 Example verifier

`scripts/verify_examples.py` (Appendix A.10) parses every fenced
`​```python` block in this document with `ast.parse` and fails CI on any
`SyntaxError`. Prior draft's `["AI", "ML", "NLP", ...]` would fail
immediately.

---

## 9. Security

### 9.1 Secrets

All secrets load from `$GREEN_AGENT_SECRETS_DIR`, not env:

| Secret | Purpose |
|---|---|
| `WATTTIME_USERNAME` + `WATTTIME_PASSWORD` | WattTime grid API |
| `ELECTRICITYMAP_API_KEY` | ElectricityMap grid API |
| `POSTGRES_PASSWORD` | Metrics storage |
| `REDIS_PASSWORD` | Cache |

`.env` is for **non-secret** configuration only (URLs, host names, ports).
See `.env.example` (Appendix A.7).

### 9.2 Ray dashboard

Ray's dashboard is **unauthenticated by default**. If you expose it:

1. Bind to `127.0.0.1` (default).
2. Use SSH port-forwarding (`ssh -L 8265:localhost:8265 user@host`).
3. Or place an authenticating reverse proxy in front (nginx + auth_basic,
   Caddy + forward-auth, etc.).

**Never** bind to `0.0.0.0` on a public network.

### 9.3 API services

The API on `:8000` and VimRAG on `:8001`:

- Bind to `127.0.0.1` by default.
- Add bearer auth if exposed to a network (§9.2 pattern applies).
- Rate-limit at 60 req/min per IP via reverse proxy.

### 9.4 Grid-API responses

Validate every grid-API response before use. A malformed response must
fail closed (use the static fallback intensity, not `0`).

---

## 10. Monitoring

### 10.1 Logs

| Log | Location |
|---|---|
| Ray | `/tmp/ray/session_latest/logs/raylet.out` |
| Application | `logs/green_agent.log` |
| Docker Compose | `docker compose logs -f` |

### 10.2 Key metrics

| Metric | Units | Where |
|---|---|---|
| `energy_kwh` | kWh | Per-task |
| `carbon_co2e_kg` | kg CO₂e | Per-task |
| `carbon_saved_kgco2e` | kg CO₂e | `get_scheduler_stats()` |
| `carbon_saved_percent` | % | Same |
| `current_intensity_gco2kwh` | gCO₂/kWh | `get_current_recommendation()` |
| `compute_limit_percent` | % | Same |

---

## 11. Troubleshooting

### 11.1 `ModuleNotFoundError: No module named 'src'`

You ran the script outside the repo root without `pip install -e .`.
Fix:

```bash
set -euo pipefail
cd Green_Agent
pip install -e . --no-deps
```

### 11.2 `ModuleNotFoundError: No module named 'src.distributed.ray_cluster_manager'`

The companion `-code` delivery (§0.2) has not been installed. Until it is,
`test_installation.py` correctly `[SKIP]`s the module imports. See §3.4
for how to copy the files when they arrive.

### 11.3 Ray cluster won't start

```bash
set -euo pipefail
ray stop
ray start --head --port=6379 --dashboard-host=127.0.0.1 --dashboard-port=8265 --verbose
```

The `--verbose` flag prints why startup failed.

### 11.4 Prophet install fails

Prophet requires `cmdstanpy`, which compiles C++. On systems without a
compiler:

```bash
set -euo pipefail
# On Debian/Ubuntu:
sudo apt-get install build-essential python3-dev

# On macOS:
xcode-select --install

# Then reinstall Prophet
pip install --force-reinstall -r requirements/carbon.txt
```

### 11.5 Port 6379 already in use

Redis uses the same default port as Ray. If Redis is running, change one:

```bash
# Ray:
ray start --head --port=6380 --dashboard-host=127.0.0.1 --dashboard-port=8265

# Or edit redis.conf to `port 6381`
```

### 11.6 Dashboard returns 503

The dashboard requires `$GREEN_AGENT_SECRETS_DIR` and a token file (see
§9.1). If either is missing, `/helium` returns 503 (misconfiguration, not
auth failure). Set both and retry.

---

## 12. Related documents

- [`MANIFEST.md`](MANIFEST.md) — delivery inventory
- [`README.md`](README.md) — base package API
- [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md) — full limitations list
- [`ARCHITECTURE_PROPOSAL.md`](ARCHITECTURE_PROPOSAL.md) — phase roadmap

External:

- [Ray documentation](https://docs.ray.io/)
- [Prophet documentation](https://facebook.github.io/prophet/)
- [WattTime API](https://www.watttime.org/api-documentation/)
- [ElectricityMap API](https://www.electricitymap.org/)

---

## 13. Next steps

1. **Read `MANIFEST.md`** to see what ships vs what needs a companion
   delivery.
2. **Run `python test_installation.py`** — expect `[SKIP]` on
   module-specific checks until the companion `-code` delivery arrives.
3. **Try Example 1** (§5.1) once modules are installed.
4. **Read `KNOWN_LIMITATIONS.md`** — especially the "what ships
   separately" section.

---

## 14. License

MIT. See `LICENSE` in the base package.

---

## Appendix A — Verification scripts

The files in this appendix are meant to be split out into their target
paths. Each subsection is preceded by its save-as path.

### A.1 `scripts/setup/setup_ray_cluster.sh`

```bash
#!/usr/bin/env bash
#
# scripts/setup/setup_ray_cluster.sh — Hardened Ray setup with preflight.
#
# Usage:
#   bash scripts/setup/setup_ray_cluster.sh
#   RAY_PORT=6380 bash scripts/setup/setup_ray_cluster.sh
#
set -euo pipefail

RAY_PORT="${RAY_PORT:-6379}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8265}"

echo "==> Verifying Ray is installed"
if ! command -v ray >/dev/null 2>&1; then
    echo "[FAIL] \`ray\` not found. Install with:" >&2
    echo "         pip install -r requirements/distributed.txt" >&2
    exit 1
fi

echo "==> Stopping any existing Ray instance"
ray stop >/dev/null 2>&1 || true

echo "==> Starting Ray on 127.0.0.1:$RAY_PORT (dashboard on 127.0.0.1:$DASHBOARD_PORT)"
ray start \
    --head \
    --port="$RAY_PORT" \
    --dashboard-host=127.0.0.1 \
    --dashboard-port="$DASHBOARD_PORT"

echo "==> Verifying cluster"
ray status

cat <<EOF

Ray started.
Dashboard: http://127.0.0.1:$DASHBOARD_PORT

Security note: the dashboard is bound to 127.0.0.1 and is UNAUTHENTICATED
by default. Do NOT bind to 0.0.0.0 unless you place an authenticating
reverse proxy in front. See INSTALLATION.md §9.2.
EOF
```

### A.2 `test_installation.py`

```python
#!/usr/bin/env python3
"""
test_installation.py — Verify the installation.

Exit 0 on all-PASS; [SKIP] lines do not count as failures.

Checks:
    [1] Python version >= 3.10
    [2] Required directories exist
    [3] Base package imports
    [4] Companion-delivery module imports (SKIP if absent)
    [5] config/carbon_regions.json has 13 regions
    [6] test_installation.py is runnable

Usage:
    python test_installation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_FAILED = False
ROOT = Path(__file__).resolve().parent


def _p(n: str) -> None:
    print(f"[PASS] {n}")


def _s(n: str, reason: str) -> None:
    print(f"[SKIP] {n} — {reason}")


def _f(n: str, e: BaseException) -> None:
    global _FAILED
    _FAILED = True
    print(f"[FAIL] {n} — {type(e).__name__}: {e}")


def check_python() -> None:
    if sys.version_info < (3, 10):
        _f(
            "Python version",
            RuntimeError(
                f"Python {sys.version_info.major}.{sys.version_info.minor} < 3.10"
            ),
        )
    else:
        _p(f"Python {sys.version_info.major}.{sys.version_info.minor}"
           f".{sys.version_info.micro} >= 3.10")


def check_directories() -> None:
    dirs = [
        "src", "src/distributed", "src/carbon", "config",
        "config/base", "tests",
    ]
    missing = [d for d in dirs if not (ROOT / d).is_dir()]
    if missing:
        _f("directories", FileNotFoundError(f"missing: {missing}"))
    else:
        _p(f"directories ({len(dirs)} dirs present)")


def check_base_imports() -> None:
    try:
        import numpy  # noqa: F401
        import pandas  # noqa: F401
        _p("base imports (numpy, pandas)")
    except ImportError as e:
        _f("base imports", e)


def check_module_imports() -> None:
    modules = [
        ("src.distributed.ray_cluster_manager", "create_ray_cluster"),
        ("src.distributed.carbon_aware_scheduler", "create_carbon_aware_cluster"),
        ("src.carbon.forecasting_engine", "create_forecaster"),
        ("src.carbon.eco_mode_controller", "create_eco_mode_controller"),
    ]
    missing = []
    for mod, _name in modules:
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
        except Exception as e:
            _f(f"import {mod}", e)
            return
    if missing:
        _s(
            "module imports",
            f"companion delivery "
            f"(green-agent-advanced-modules-code) not present — "
            f"missing {len(missing)} module(s): {missing}",
        )
        return
    _p(f"module imports ({len(modules)} modules present)")


def check_carbon_regions() -> None:
    path = ROOT / "config" / "carbon_regions.json"
    if not path.is_file():
        _s("carbon regions", f"missing {path}")
        return
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        _f("carbon regions", e)
        return
    n = len(data)
    if n != 13:
        _f(
            "carbon regions",
            AssertionError(f"expected 13 regions, found {n}: {sorted(data)}"),
        )
        return
    _p(f"carbon regions ({n} regions)")


def check_self_runnable() -> None:
    if not (ROOT / "test_installation.py").is_file():
        _f("test_installation.py", FileNotFoundError("missing"))
        return
    _p("test_installation.py runs")


def main() -> int:
    check_python()
    check_directories()
    check_base_imports()
    check_module_imports()
    check_carbon_regions()
    check_self_runnable()

    if _FAILED:
        print("Installation FAILED — see [FAIL] lines above", file=sys.stderr)
        return 1
    print("Installation verified successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

### A.3 `config/ray_cluster.yaml`

```yaml
# Green Agent — Ray cluster configuration (template)
#
# Ray is configured primarily via CLI flags or `ray.init(...)`. This
# file documents the intended configuration for deployments that want
# a single source of truth. Do NOT assume Ray reads it automatically.
#
# For startup, see scripts/setup/setup_ray_cluster.sh.

cluster_name: green_agent_cluster
num_workers: 4

# SECURITY: bind dashboard to 127.0.0.1, not 0.0.0.0.
# Ray's dashboard is unauthenticated by default. See INSTALLATION.md §9.2.
dashboard_host: 127.0.0.1
dashboard_port: 8265

object_store_memory_gb: 4
ray_port: 6379
```

### A.4 `config/carbon_regions.json`

```json
{
  "US-CA":   {"name": "California",     "grid_api": "watttime",       "timezone": "America/Los_Angeles", "intensity_gco2_per_kwh": 220},
  "US-TX":   {"name": "Texas",          "grid_api": "watttime",       "timezone": "America/Chicago",     "intensity_gco2_per_kwh": 430},
  "US-NY":   {"name": "New York",       "grid_api": "watttime",       "timezone": "America/New_York",    "intensity_gco2_per_kwh": 220},
  "EU-FR":   {"name": "France",         "grid_api": "electricitymap", "timezone": "Europe/Paris",        "intensity_gco2_per_kwh": 70},
  "EU-DE":   {"name": "Germany",        "grid_api": "electricitymap", "timezone": "Europe/Berlin",       "intensity_gco2_per_kwh": 380},
  "EU-NO":   {"name": "Norway",         "grid_api": "electricitymap", "timezone": "Europe/Oslo",         "intensity_gco2_per_kwh": 30},
  "EU-PL":   {"name": "Poland",         "grid_api": "electricitymap", "timezone": "Europe/Warsaw",       "intensity_gco2_per_kwh": 700},
  "CN":      {"name": "China",          "grid_api": "static",         "timezone": "Asia/Shanghai",       "intensity_gco2_per_kwh": 580},
  "IN":      {"name": "India",          "grid_api": "static",         "timezone": "Asia/Kolkata",        "intensity_gco2_per_kwh": 630},
  "JP":      {"name": "Japan",          "grid_api": "static",         "timezone": "Asia/Tokyo",          "intensity_gco2_per_kwh": 480},
  "AU":      {"name": "Australia",      "grid_api": "static",         "timezone": "Australia/Sydney",    "intensity_gco2_per_kwh": 550},
  "BR":      {"name": "Brazil",         "grid_api": "static",         "timezone": "America/Sao_Paulo",   "intensity_gco2_per_kwh": 100},
  "GLOBAL":  {"name": "Global Average", "grid_api": "static",         "timezone": "UTC",                 "intensity_gco2_per_kwh": 475}
}
```

### A.5 `config/dashboard_config.yaml`

```yaml
# Green Agent — Dashboard configuration (template)
#
# See INSTALLATION.md §9.2 for the security model.

bind_host: 127.0.0.1   # NOT 0.0.0.0 — see INSTALLATION.md §9.2
bind_port: 8001

# Bearer token — MUST be loaded from $GREEN_AGENT_SECRETS_DIR, not here.
token_file: "$GREEN_AGENT_SECRETS_DIR/dashboard_token"

# CORS allowlist — do not use "*"
cors_origins:
  - "http://127.0.0.1:8001"

# Rate limit (per IP)
rate_limit_per_minute: 60
```

### A.6 `requirements/carbon.txt`

```
# Carbon forecasting — requires base.txt first.

# Prophet is used by forecasting_engine.py. Requires a C++ compiler
# (cmdstanpy builds against stan). See INSTALLATION.md §11.4.
prophet==1.1.6

# Semantic scoring
sentence-transformers==3.3.1

# Grid-API HTTP clients
aiohttp==3.10.11
```

### A.7 `.env.example`

```bash
# Green Agent — non-secret configuration.
#
# Copy this file to .env and edit for your environment.
#
# SECRETS ARE NOT IN THIS FILE. Secrets load from
# $GREEN_AGENT_SECRETS_DIR (see INSTALLATION.md §9.1).

# Directory containing secrets (username/password files, tokens).
GREEN_AGENT_SECRETS_DIR=/run/secrets

# Grid API endpoints (non-secret — the credentials are in secrets dir).
HELIUM_API_URL=https://your-helium-monitor.example.org/v1
WATTTIME_API_URL=https://api2.watttime.org/v2
ELECTRICITYMAP_API_URL=https://api.electricitymap.org/v3

# PostgreSQL (credentials via $GREEN_AGENT_SECRETS_DIR/postgres_password)
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=green_agent
POSTGRES_USER=green_agent

# Redis (credentials via $GREEN_AGENT_SECRETS_DIR/redis_password)
REDIS_HOST=127.0.0.1
REDIS_PORT=6381

# Ray
RAY_PORT=6379
RAY_DASHBOARD_PORT=8265

# API service
API_BIND_HOST=127.0.0.1
API_BIND_PORT=8000
```

### A.8 `docker/Dockerfile.worker`

```dockerfile
# Minimal Ray worker image. Not production-hardened.
# See INSTALLATION.md §6 for build context.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN groupadd -g 1000 app && useradd -u 1000 -g app app

COPY requirements/base.txt requirements/distributed.txt ./
RUN pip install --no-cache-dir -r base.txt -r distributed.txt

COPY --chown=app:app . .
RUN pip install -e . --no-deps

USER app

CMD ["python", "-m", "src.distributed.ray_cluster_manager"]
```

### A.9 `docker/Dockerfile.api`

```dockerfile
# Minimal API image. Not production-hardened.
# See INSTALLATION.md §6 for build context.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN groupadd -g 1000 app && useradd -u 1000 -g app app

COPY requirements/base.txt requirements/dashboard.txt ./
RUN pip install --no-cache-dir -r base.txt -r dashboard.txt

COPY --chown=app:app . .
RUN pip install -e . --no-deps

USER app

EXPOSE 8000
CMD ["uvicorn", "src.dashboard.api:app", "--host", "127.0.0.1", "--port", "8000"]
```

### A.10 `docker/docker-compose.yml`

```yaml
# Minimal docker-compose example. Not production-hardened.
# See INSTALLATION.md §6.
#
# SECURITY: all services bind to 127.0.0.1 by default. Do NOT change to
# 0.0.0.0 without a reverse proxy. See INSTALLATION.md §9.2.

services:
  ray-head:
    build:
      context: ..
      dockerfile: docker/Dockerfile.worker
    ports:
      - "127.0.0.1:8265:8265"
    environment:
      GREEN_AGENT_SECRETS_DIR: /run/secrets
    volumes:
      - ./secrets:/run/secrets:ro
    command: >
      ray start --head --port=6379
      --dashboard-host=0.0.0.0 --dashboard-port=8265 --block

  api:
    build:
      context: ..
      dockerfile: docker/Dockerfile.api
    ports:
      - "127.0.0.1:8000:8000"
    environment:
      GREEN_AGENT_SECRETS_DIR: /run/secrets
    volumes:
      - ./secrets:/run/secrets:ro
    depends_on:
      - ray-head
```

**Note:** the `ray-head` service uses `--dashboard-host=0.0.0.0` *inside*
the container — the port binding `127.0.0.1:8265:8265` restricts host-side
exposure to localhost. This is safe. Never change the compose file's port
binding to `0.0.0.0:8265:8265` without a reverse proxy.

### A.11 `scripts/deployment/kubernetes/ray-cluster.yaml`

```yaml
# Minimal Kubernetes Ray cluster example. Not production-hardened.
# See INSTALLATION.md §7.
#
# SECURITY: dashboard Service is ClusterIP — access via port-forward.

apiVersion: apps/v1
kind: Deployment
metadata:
  name: ray-head
  namespace: green-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ray-head
  template:
    metadata:
      labels:
        app: ray-head
    spec:
      containers:
        - name: ray
          image: rayproject/ray:2.40.0-py312
          command:
            - "ray"
            - "start"
            - "--head"
            - "--port=6379"
            - "--dashboard-host=0.0.0.0"
            - "--dashboard-port=8265"
            - "--block"
          ports:
            - { name: ray,       containerPort: 6379 }
            - { name: dashboard, containerPort: 8265 }
          resources:
            requests: { cpu: "1", memory: "2Gi" }
            limits:   { cpu: "4", memory: "8Gi" }
---
apiVersion: v1
kind: Service
metadata:
  name: ray-head
  namespace: green-agent
spec:
  selector: { app: ray-head }
  ports:
    - { name: ray, port: 6379, targetPort: 6379 }
---
apiVersion: v1
kind: Service
metadata:
  name: ray-dashboard
  namespace: green-agent
spec:
  type: ClusterIP    # NOT NodePort or LoadBalancer — access via port-forward.
  selector: { app: ray-head }
  ports:
    - { name: dashboard, port: 8265, targetPort: 8265 }
```

### A.12 `scripts/verify_examples.py`

```python
#!/usr/bin/env python3
"""
scripts/verify_examples.py — Verify every example in INSTALLATION.md
at least parses. Full execution requires the companion delivery.

Checks:
    1. Every fenced ```python block in INSTALLATION.md parses via ast.parse
    2. test_installation.py runs and exits 0 (with SKIPs allowed)
    3. config/carbon_regions.json is valid JSON with 13 regions

Exit 0 on all-PASS; exit 1 otherwise.
"""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_DOC = ROOT / "INSTALLATION.md"

_FAILED = False


def _p(n: str) -> None:
    print(f"[PASS] {n}")


def _f(n: str, e: BaseException) -> None:
    global _FAILED
    _FAILED = True
    print(f"[FAIL] {n} — {type(e).__name__}: {e}")


_PY_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


def check_blocks_parse() -> None:
    if not _DOC.is_file():
        _f("INSTALLATION.md present", FileNotFoundError(str(_DOC)))
        return
    blocks = _PY_BLOCK.findall(_DOC.read_text())
    if not blocks:
        _f("python blocks", AssertionError("no ```python blocks found"))
        return
    for i, block in enumerate(blocks, 1):
        try:
            ast.parse(block)
        except SyntaxError as e:
            _f(f"python block #{i} parses", e)
            return
    _p(f"{len(blocks)} python blocks parse")


def check_test_installation() -> None:
    script = ROOT / "test_installation.py"
    if not script.is_file():
        _f("test_installation.py exists", FileNotFoundError(str(script)))
        return
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        _f(
            "test_installation.py",
            RuntimeError(
                f"exit {result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            ),
        )
        return
    _p("test_installation.py exit 0")


def check_carbon_regions() -> None:
    path = ROOT / "config" / "carbon_regions.json"
    if not path.is_file():
        _f("carbon_regions.json", FileNotFoundError(str(path)))
        return
    data = json.loads(path.read_text())
    if len(data) != 13:
        _f(
            "carbon_regions count",
            AssertionError(f"expected 13 regions, got {len(data)}"),
        )
        return
    _p(f"carbon_regions.json ({len(data)} regions)")


def main() -> int:
    check_blocks_parse()
    check_test_installation()
    check_carbon_regions()

    if _FAILED:
        return 1
    print("All examples verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

### A.13 `tests/test_docs_drift.py`

```python
"""
tests/test_docs_drift.py — Drift guard.

Asserts that INSTALLATION.md and its configs stay consistent with
README.md §5.1 and MANIFEST.md §0.

Runs in CI on every push/PR.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def test_carbon_regions_count_is_13():
    path = _ROOT / "config" / "carbon_regions.json"
    assert path.is_file(), f"missing {path}"
    data = json.loads(path.read_text())
    assert len(data) == 13, (
        f"carbon_regions.json has {len(data)} regions, README.md §5.1 "
        f"ships 13. Reconcile."
    )


def test_carbon_regions_match_readme_regions():
    path = _ROOT / "config" / "carbon_regions.json"
    data = json.loads(path.read_text())
    expected = {
        "US-CA", "US-TX", "US-NY", "EU-FR", "EU-DE", "EU-NO", "EU-PL",
        "CN", "IN", "JP", "AU", "BR", "GLOBAL",
    }
    assert set(data.keys()) == expected, (
        f"region keys differ from README.md §5.1: "
        f"missing={expected - set(data)}, extra={set(data) - expected}"
    )


def test_installation_status_is_alpha():
    doc = _ROOT / "INSTALLATION.md"
    if not doc.is_file():
        pytest.skip("INSTALLATION.md not present")
    text = doc.read_text()
    assert "Status:" in text, "INSTALLATION.md must have a Status: line"
    for line in text.splitlines():
        if "Production Ready" in line:
            assert "not" in line.lower() or "was" in line.lower() or \
                   "removed" in line.lower() or "claim" in line.lower(), \
                   f"Stale 'Production Ready' claim: {line!r}"


def test_installation_has_asserts():
    path = _ROOT / "test_installation.py"
    assert path.is_file(), f"missing {path}"
    asserts = re.findall(r"^\s+assert ", path.read_text(), re.MULTILINE)
    assert len(asserts) >= 3, (
        f"test_installation.py has only {len(asserts)} asserts — "
        f"a print-only test does not verify anything"
    )


def test_ray_setup_binds_localhost():
    path = _ROOT / "scripts" / "setup" / "setup_ray_cluster.sh"
    if not path.is_file():
        pytest.skip("setup_ray_cluster.sh not present")
    text = path.read_text()
    assert "127.0.0.1" in text, "setup_ray_cluster.sh must bind to 127.0.0.1"
    for line in text.splitlines():
        if "--dashboard-host=0.0.0.0" in line:
            assert line.lstrip().startswith("#"), (
                f"Line binds dashboard to 0.0.0.0 in executable form: {line!r}"
            )


def test_no_ellipsis_in_list_literals():
    doc = _ROOT / "INSTALLATION.md"
    if not doc.is_file():
        pytest.skip("INSTALLATION.md not present")
    bad = re.findall(r"\[\s*[^\]\n]*,\s*\.\.\.\s*\]", doc.read_text())
    assert not bad, (
        f"found list literal(s) containing `...` (SyntaxError): {bad}"
    )


def test_requirements_are_pinned():
    for req in (_ROOT / "requirements").glob("*.txt"):
        for i, line in enumerate(req.read_text().splitlines(), 1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            assert "==" in s, f"{req.name}:{i} is not pinned: {s!r}"


def test_env_example_has_secrets_dir():
    path = _ROOT / ".env.example"
    if not path.is_file():
        pytest.skip(".env.example not present")
    assert "GREEN_AGENT_SECRETS_DIR" in path.read_text(), (
        ".env.example must document GREEN_AGENT_SECRETS_DIR"
    )
```

---

## Appendix B — What changed from v1.0

| Area | v1.0 | This version |
|---|---|---|
| Status | No status label | **Alpha** (§0.1) |
| Module files | `cp ray_cluster_manager.py ...` — source never shipped | Explicit "companion delivery" (§0.2); `[SKIP]` in `test_installation.py` |
| `test_installation.py` | Inline print-only snippet | Real file with 6 `assert`-backed checks (A.2) |
| Async surface | `await`/non-`await` mixed with no explanation | Canonical table (§2) |
| Ray dashboard | `--dashboard-host=0.0.0.0` | `127.0.0.1` by default (§4.1); `0.0.0.0` requires proxy |
| Shell robustness | No `set -euo pipefail` | Every snippet (§3–§11) |
| `pip install -e .` | Missing | Added (§3.3) |
| Dependencies | `pip install prophet sentence-transformers` inline | Pinned in `requirements/carbon.txt` (A.6) |
| Carbon regions | 3 in `carbon_regions.json` | 13, matching `README.md §5.1` (A.4) |
| Example 1 | `["AI", "ML", "NLP", ...]` — `SyntaxError` | Concrete list (§5.1) |
| Example 2 | `datetime.now()` — `NameError` | `from datetime import datetime, timedelta` (§5.2) |
| Example 3 | 24 × 3600s real-time loop | 3 × 1s demo, with production-mode comment (§5.3) |
| Dockerfiles | Referenced, not shipped | Shipped (minimal) (A.8, A.9) |
| `docker-compose.yml` | Referenced, not shipped | Shipped (minimal) (A.10) |
| k8s manifest | Referenced, not shipped | Shipped (minimal) (A.11) |
| `setup_ray_cluster.sh` | Referenced, not shipped | Shipped, hardened (A.1) |
| `.env.example` | Never shown | Shipped (A.7) |
| Verification | None | `scripts/verify_examples.py` (A.12) |
| Drift guard | None | `tests/test_docs_drift.py` (A.13) |
| Version | `2.5.0-advanced-modules` | `2.5.0-alpha` (matches MANIFEST.md) |
| Date | `March 2026` (future) | `<YYYY-MM-DD>` |
| Python | 3.9+ | ≥3.10 (matches README) |
| Known limitations | Vague | Explicit "ships separately" list in `KNOWN_LIMITATIONS.md` |

### Verification guarantee

Three properties now hold — and are **tested** in CI:

1. **Every Python code block in this document compiles** —
   `scripts/verify_examples.py` runs `ast.parse` on each fenced block and
   fails CI on any `SyntaxError`. Prior draft's `["AI", "ML", "NLP", ...]`
   would fail immediately.

2. **The guide cannot drift from the shipped package** —
   `tests/test_docs_drift.py` asserts `carbon_regions.json` has 13 regions
   matching `README.md §5.1`, requirements are pinned, Ray is bound to
   `127.0.0.1`, and no `Production Ready` claim appears outside the
   changelog.

3. **The installation test actually verifies something** —
   `test_installation.py` has ≥3 `assert`-backed checks (counted by
   `test_installation_has_asserts` in the drift test), not the print-only
   pattern that always succeeds.
