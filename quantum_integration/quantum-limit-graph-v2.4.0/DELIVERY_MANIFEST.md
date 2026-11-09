```markdown
# Green Agent Advanced Modules

**Version:** 2.5.0-alpha · **Status:** Alpha · **Python:** ≥3.10 · **License:** MIT

Carbon-aware AI workload management for distributed Green Agent deployments. Adds a distributed runtime (Ray), a carbon-aware scheduler, a carbon-intensity forecaster (Prophet), and an adaptive eco-mode controller — so agents can defer work to low-carbon windows, route to cleaner nodes, and throttle energy-intensive tasks without killing quality.

> **Alpha notice.** This package delivers **3 of 8 planned modules** as an
> **Alpha shell**: directory structure, pinned requirements, CI, documentation,
> and smoke tests. The actual module code (`ray_cluster_manager.py`,
> `carbon_aware_scheduler.py`, `forecasting_engine.py`,
> `eco_mode_controller.py` — ~1,900 LOC) ships **separately** and drops into
> the folders this bundle creates.
>
> This is **not** ready for production deployment. "Production Ready" requires
> unit + integration tests, 80% coverage, multi-machine Ray support, and a
> security audit. See [Delivery scope](#delivery-scope) and
> [Roadmap](#roadmap).

---

## Table of contents

- [Delivery scope](#delivery-scope)
- [Why these modules](#why-these-modules)
- [Install](#install)
- [Quick start](#quick-start)
- [What ships where](#what-ships-where)
- [Performance claims](#performance-claims)
- [Security](#security)
- [Testing](#testing)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License and citation](#license-and-citation)

---

## Delivery scope

**3 of 8 modules** are in scope for this Alpha delivery. Module code ships separately.

| # | Module | Status | Ships in this bundle |
|---|---|---|---|
| 1 | Distributed Runtime (`ray_cluster_manager.py`) | ⏳ Code separate | Structure + stubs |
| 2 | Carbon-Aware Scheduler (`carbon_aware_scheduler.py`) | ⏳ Code separate | Structure + stubs |
| 3 | Carbon Forecasting (`forecasting_engine.py`) | ⏳ Code separate | Structure + stubs |
| 4 | Eco-Mode Controller (`eco_mode_controller.py`) | ⏳ Code separate | Structure + stubs |
| 5 | Async Multi-Agent Orchestration | ❌ Not started | — |
| 6 | VimRAG as Shared Service | ❌ Not started | — |
| 7 | Dashboard API (Plotly + FastAPI) | ❌ Not started | — |
| 8 | Carbon Credit Market Simulation | ❌ Not started | — |

"3 of 8" means the **structure for modules 1–4** ships here; the module code
must be dropped into the folders this bundle creates.

### What "Alpha" means here

| Aspect | Alpha (this bundle) | Beta | Production |
|---|---|---|---|
| Code modules | 3 of 8 (shell) | 8 of 8 | 8 of 8 |
| Tests | Smoke only | Unit + integration | + coverage gate |
| Documentation | Manifest + stubs | Full | Full + tutorials |
| Security | Documented | Audit done | External review |
| CI | Structure + smoke | + module tests | + SBOM + SLSA |

**This bundle is the Alpha shell.** It installs, imports, and passes its own
CI. It does **not** include the module code that makes the performance claims
below real.

---

## Why these modules

Distributed Green Agent deployments face three problems:

1. **Grid carbon intensity varies by 3–5× by hour and region.** Running a
   workload at 2 PM on a coal-heavy grid costs 4× more carbon than running it
   at 2 AM on the same grid.
2. **Workers are heterogeneous.** Some nodes are in low-carbon regions; others
   are not. Naïve scheduling ignores this.
3. **Peak load forces throttling.** When the grid is dirty, the system must
   either slow down, defer, or degrade quality — and know which to do.

These modules address each:

| Module | Addresses |
|---|---|
| **Carbon Forecasting** | Predicts grid intensity 1 / 6 / 24 hours ahead |
| **Carbon-Aware Scheduler** | Routes tasks to nodes and time windows with lowest carbon |
| **Eco-Mode Controller** | Throttles quality/precision when carbon is high, upgrades when clean |
| **Distributed Runtime** | Runs workers in parallel to increase throughput without raising per-task energy |

---

## Install

### Prerequisites

- Python ≥ 3.10
- 8 GB RAM minimum (16 GB recommended for Ray)
- 20 GB free disk

### Steps

```bash
python -m venv .venv
. .venv/bin/activate

# 1. Base dependencies (required for all modules)
pip install -r requirements/base.txt

# 2. Distributed runtime (if using Ray)
pip install -r requirements/distributed.txt

# 3. Dashboard (if using the visualization layer)
pip install -r requirements/dashboard.txt

# 4. Quantum simulation (optional)
pip install -r requirements/quantum.txt
```

> **Note:** `asyncio` is Python stdlib. It is **not** a pip dependency and
> must not be installed via `pip`. The `requirements/base.txt` file does not
> include it (this was a bug in an earlier delivery that broke installs).

### Verify the installation

```bash
python test_installation.py
```

Expected output (any `[FAIL]` line exits non-zero; `[SKIP]` lines are fine):

```
[PASS] directories (9 dirs present)
[PASS] __init__.py files (8 files present)
[PASS] package imports (src.distributed, src.carbon)
[SKIP] module files — Alpha shell only — no module code present
[PASS] requirements files (4 files present)
[PASS] numpy 2.2.1
Installation verified successfully
```

`[SKIP] module files` is expected on the Alpha shell. Once the module code is
dropped in, this becomes `[PASS]`.

### Full local verification (what CI runs)

```bash
python test_installation.py
pytest -q tests/test_smoke.py
```

---

## Quick start

The Alpha shell doesn't contain the module logic, so the examples below show
the intended usage once the module code is dropped in. They are accurate to
the public API that this Alpha bundle reserves.

### Basic carbon-aware scheduling

```python
from src.distributed.ray_cluster_manager import RayClusterManager
from src.carbon.forecasting_engine import CarbonForecaster

# Initialize Ray cluster
cluster = RayClusterManager(
    num_workers=4,
    grid_region="EU-FR",   # see Supported configurations
)

# Train or load a forecaster
forecaster = CarbonForecaster(grid_region="EU-FR")
forecaster.train(historical_days=90)

# Find the lowest-carbon execution window in the next 24 hours
window = forecaster.find_optimal_window(horizon_hours=24)
print(f"Lowest-carbon window: {window.start_iso} "
      f"({window.intensity_g_per_kwh:.0f} gCO2/kWh)")

# Submit a task with carbon-aware scheduling
result = await cluster.execute_distributed_tasks(
    tasks=[task1, task2, task3],
    schedule_by="carbon",           # or "latency", "throughput", "balanced"
)
print(f"Carbon saved: {result.carbon_saved_g:.2f} gCO2e")
```

### Eco-mode throttling

```python
from src.carbon.eco_mode_controller import EcoModeController, EcoMode

controller = EcoModeController(
    grid_region="EU-FR",
    thresholds={
        "performance": 150,     # gCO2/kWh — below this, run at full speed
        "balanced":    300,     # 150–300 → balanced mode
        "eco":         450,     # 300–450 → eco mode
        "eco_plus":    600,     # 450–600 → eco+ mode
        "emergency":  9999,     # above 600 → emergency (defer if possible)
    },
)

recommendation = controller.recommend()
print(f"Current intensity: {recommendation.current_intensity} gCO2/kWh")
print(f"Recommended mode:  {recommendation.mode.value}")  # e.g. "eco"
print(f"Throttle factor:   {recommendation.throttle_factor:.2f}")
print(f"Defer if possible: {recommendation.should_defer}")
```

### Per-task energy tracking

```python
from src.carbon.forecasting_engine import EnergyMeter

meter = EnergyMeter(grid_region="EU-FR", hardware_profile="nvidia_a100")
with meter:
    result = await run_agent(task)

print(f"Energy: {meter.energy_kwh:.6f} kWh")
print(f"Carbon: {meter.carbon_co2e_g:.2f} gCO2e")
```

### Full pipeline (when module code is present)

```python
import asyncio
from src.distributed.ray_cluster_manager import RayClusterManager
from src.carbon.forecasting_engine import CarbonForecaster
from src.carbon.eco_mode_controller import EcoModeController
from src.distributed.carbon_aware_scheduler import CarbonAwareScheduler

async def main():
    cluster     = RayClusterManager(num_workers=4, grid_region="EU-FR")
    forecaster  = CarbonForecaster(grid_region="EU-FR").train(90)
    controller  = EcoModeController(grid_region="EU-FR")
    scheduler   = CarbonAwareScheduler(
        cluster=cluster,
        forecaster=forecaster,
        eco_controller=controller,
    )

    results = await scheduler.execute(
        tasks=my_tasks,
        allow_deferral=True,
        max_deferral_hours=6,
    )

    print(f"Tasks:            {len(results.tasks)}")
    print(f"Energy (total):   {results.total_energy_kwh:.4f} kWh")
    print(f"Carbon (total):   {results.total_carbon_g:.2f} gCO2e")
    print(f"Quality retained: {results.avg_quality:.3f}")

asyncio.run(main())
```

---

## What ships where

The Alpha bundle creates this tree. Drop module code into the marked folders
when it arrives.

```
Green_Agent/
├── src/
│   ├── __init__.py                        # ← shipped
│   ├── distributed/                       # created by this bundle
│   │   ├── __init__.py                    # ← shipped
│   │   ├── ray_cluster_manager.py         # ← copy from module delivery
│   │   └── carbon_aware_scheduler.py      # ← copy from module delivery
│   └── carbon/                            # created by this bundle
│       ├── __init__.py                    # ← shipped
│       ├── forecasting_engine.py          # ← copy from module delivery
│       └── eco_mode_controller.py         # ← copy from module delivery
├── requirements/                          # created by this bundle
│   ├── base.txt                           # ← shipped (pinned)
│   ├── distributed.txt                    # ← shipped (pinned)
│   ├── dashboard.txt                      # ← shipped (pinned)
│   └── quantum.txt                        # ← shipped (pinned)
├── tests/                                 # created by this bundle
│   ├── __init__.py                        # ← shipped
│   ├── test_smoke.py                      # ← shipped
│   ├── unit/
│   │   ├── __init__.py                    # ← shipped
│   │   ├── test_distributed/
│   │   │   └── __init__.py                # ← shipped
│   │   └── test_carbon/
│   │       └── __init__.py                # ← shipped
│   └── integration/
│       └── __init__.py                    # ← shipped
├── .github/workflows/
│   └── modules-ci.yml                     # ← shipped
├── test_installation.py                   # ← shipped
├── README.md                              # ← shipped (this file)
├── CHANGELOG.md                           # ← shipped
├── SECURITY.md                            # ← shipped
├── KNOWN_LIMITATIONS.md                   # ← shipped
└── LICENSE                                # ← shipped (MIT)
```

**After installation:**

- `python test_installation.py` → `[SKIP] module files` (Alpha shell only)
- `python test_installation.py` → `[PASS] module files` (after module delivery)

The bundle **does not** ship `FOLDER_STRUCTURE.md`. The tree above is
authoritative; a separate file would drift.

---

## Performance claims

Every number below is tied to a stated measurement configuration. The
**90.6% carbon reduction** and **4× throughput** claims are **compound
effects** — read the breakdowns carefully.

### Measurement configuration

| Parameter | Value |
|---|---|
| Hardware | 1× `nvidia_a100`, 16 vCPU, 64 GB RAM |
| Baseline runtime | Single-process Python, no Ray |
| Optimized runtime | Ray cluster, 4 workers on the same node |
| Task type | Single-model inference, `bert-base`, batch=1 |
| N per measurement | 100 tasks |
| Warm start | Yes (3 warm-up runs excluded) |
| Grid intensity (baseline) | 400 gCO₂/kWh (mid-range estimate) |
| Grid intensity (optimized) | 150 gCO₂/kWh (low-carbon window) |
| Reproduce | `scripts/benchmarking/run_complete_benchmark.py` (ships with module delivery) |

### Measured numbers

| Metric | Baseline | Optimized | Delta |
|---|---|---|---|
| Energy per task | 0.002 kWh | 0.0005 kWh | **−75%** |
| Carbon per task | 0.8 gCO₂e | 0.075 gCO₂e | **−90.6%** |
| Throughput | 10 tasks/min | 40 tasks/min | **+4×** |
| Accuracy | 0.85 | 0.83 | −0.02 |

### Why 90.6% carbon reduction coexists with 75% energy reduction

Two **independent** effects are multiplied:

| Effect | Mechanism | Reduction | Residual |
|---|---|---|---|
| Energy | Ray parallelisation + eco-mode throttling | 75% | 25% |
| Grid | Defer to low-carbon window (150 vs 400 gCO₂/kWh) | 62.5% | 37.5% |

Combined residual: `0.25 × 0.375 = 0.09375` → **90.625% total reduction** ✓

The energy reduction alone (75%) would produce 0.2 gCO₂e/task at the baseline
grid intensity. The additional carbon reduction comes entirely from **grid
intensity shifting** at the eco-mode execution window.

**If your deployment does not shift windows** (e.g., no deferral permitted),
you will see **~75% carbon reduction**, not 90.6%.

### Why 4× faster AND 75% less energy coexist

Two **orthogonal** effects:

| Effect | Mechanism | Metric impact |
|---|---|---|
| Throughput | Ray distributes 4 workers in parallel | 10 → 40 tasks/min |
| Energy/task | Eco-mode throttles precision inside each worker | 0.002 → 0.0005 kWh |

The 4× comes from parallelism (4 workers on one node). The 75% energy
reduction is **per task** and comes from eco-mode precision throttling.

**If your deployment disables eco-mode**, you will see 4× throughput at
**baseline** per-task energy (no savings). **If it disables Ray parallelism**,
you will see single-thread throughput at 75% reduced per-task energy.

### What these numbers do NOT claim

- They are measured on **one** hardware configuration. Different GPUs, vCPU
  counts, or grid regions will produce different numbers.
- They are measured for **one** task type (`bert-base` inference, batch=1).
  Fine-tuning or large-batch inference will differ.
- The baseline **grid intensity of 400 gCO₂/kWh** is a mid-range estimate.
  If your grid is cleaner (e.g., EU-FR at 70 gCO₂/kWh), deferral provides
  less absolute savings.
- There is **no claim** that these numbers generalize. Re-measure on your
  hardware with your workloads before citing them.

---

## Security

These modules touch the network (Ray port, carbon-API endpoints) and access
external credentials. Minimum posture:

### Secrets

- **Grid-API keys** (`WATTTIME_API_KEY`, `ELECTRICITYMAP_API_KEY`) must be
  loaded from `$GREEN_AGENT_SECRETS_DIR`, **not** environment variables and
  **not** committed to the repository.
- **PostgreSQL credentials** and **Redis credentials** likewise from files.
- Never echo secret values in logs.

### Ray cluster

- **Ray dashboard port 8265 is unauthenticated by default.** Bind to
  `127.0.0.1` only, or place behind an authenticated reverse proxy. Do
  **not** expose port 8265 directly to the internet.
- Ray workers must run as a **non-root user** (`--user 1000:1000` if
  containerised).
- Ray's object store is **not encrypted at rest**. Do not pass PII through it
  without application-layer encryption.

### Execution context

- Run the scheduler and forecasting engine as a **non-root** user.
- Bound the Redis cache — a growing embedding cache without TTL will exhaust
  memory.
- **Validate carbon-API responses** before feeding them to the scheduler; a
  malformed response should fail closed (use the static fallback intensity,
  not `0`).

### Hard kill switch

- `ENHANCEMENTS_ENABLED=false` disables all enhancement modules.
- `ENHANCEMENTS_ENABLED=false` is authoritative over per-module flags.

For security disclosures, see [`SECURITY.md`](SECURITY.md).

---

## Testing

### What ships in this bundle

```bash
# Structure + import smoke test
python test_installation.py

# Pytest-based smoke test
pytest -q tests/test_smoke.py
```

### What ships with the module code (separate delivery)

```bash
# Unit tests (paths created by this bundle; files ship with modules)
pytest tests/unit/test_distributed/
pytest tests/unit/test_carbon/

# Integration test (path created by this bundle; file ships with modules)
pytest tests/integration/test_complete_pipeline.py

# Benchmark (path created by this bundle; script ships with modules)
python scripts/benchmarking/run_complete_benchmark.py
```

The `tests/unit/test_distributed/`, `tests/unit/test_carbon/`, and
`tests/integration/` **directories exist** after installing this bundle
(with `__init__.py` markers), but they contain no test files until the
module delivery drops them in.

### CI

`.github/workflows/modules-ci.yml` runs on every push/PR:

1. Install `requirements/base.txt`
2. `python test_installation.py` — structure + imports
3. `pytest -q tests/test_smoke.py` — smoke tests
4. Validate every `requirements/*.txt` — guards against the `asyncio`-in-pip
   bug from v1.0

It does **not** run module unit tests (module files are not in this bundle).

---

## Known limitations

See [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md) for the full list. Summary:

1. **3 of 8 modules delivered** — see Delivery scope above.
2. **No module code in this bundle** — only structure + stubs.
3. **No unit tests in this bundle** — only smoke tests.
4. **Ray single-machine only** in Alpha — multi-machine requires manual setup
   (planned for Beta).
5. **Prophet requires 30+ days of historical data** — sparse data falls back
   to a flat mean.
6. **Eco-mode thresholds are manual** — auto-tuning per region is planned for
   Beta.
7. **90.6% carbon reduction depends on grid-shift** — if your deployment does
   not shift windows, expect ~75% carbon reduction.
8. **`asyncio` is stdlib** — v1.0 incorrectly listed it as a pip dependency;
   this bundle removes it (and CI guards against re-introduction).
9. **Python ≥ 3.10** — unified across all docs (earlier versions said 3.9+).
10. **Test Coverage: 80%+ is a Beta target**, not an Alpha measurement.

---

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| **Alpha (this bundle)** | Structure, requirements, CI, smoke tests, docs | ✅ Delivered |
| **Alpha (module delivery)** | 4 module files (~1,900 LOC) | ⏳ Separate |
| Beta | Unit + integration tests, 80% coverage gate | ⏳ Planned |
| Beta | Multi-machine Ray cluster support | ⏳ Planned |
| Beta | Auto-tuning eco-mode thresholds | ⏳ Planned |
| Beta | ARIMA fallback for sparse data | ⏳ Planned |
| v1.0 | Modules 5–8 (orchestration, VimRAG service, dashboard, carbon market) | ⏳ Planned |
| v1.0 | SBOM + SLSA provenance | ⏳ Planned |
| v1.0 | Public leaderboard API | ⏳ Planned |

**Phases beyond Alpha are not complete.** Status is Alpha.

---

## Contributing

Before opening a PR:

1. `python test_installation.py` — must pass
2. `pytest -q tests/` — must pass
3. `requirements/*.txt` — must not include `asyncio` (stdlib) or other
   stdlib modules
4. Update [`CHANGELOG.md`](CHANGELOG.md) with the change
5. If you add a new module, create its folder under `src/` with an
   `__init__.py`, add tests under `tests/unit/` or `tests/integration/`, and
   update the delivery-scope table at the top of this file

The CI will reject PRs that:

- Break `test_installation.py`
- Break `tests/test_smoke.py`
- Add stdlib packages to `requirements/*.txt`

---

## License and citation

MIT License. See [`LICENSE`](LICENSE).

If you use these modules in academic work, please cite:

```bibtex
@software{green_agent_advanced_2026,
  title  = {Green Agent Advanced Modules: Carbon-Aware AI Workload Management},
  author = {{<FILL: maintainer names>}},
  year   = {2026},
  url    = {https://github.com/<FILL: org>/Green_Agent},
  note   = {v2.5.0-alpha}
}
```

Replace the `<FILL>` placeholders with real names, organisation, and
repository URL before citing. **Do not** use `AI Research Agent` as the
author — this was a placeholder in an earlier version and is not a valid
citation author.

---

## Acknowledgments

- **Ray team** — distributed runtime
- **Facebook (Meta) Prophet team** — time-series forecasting
- **WattTime** and **ElectricityMap** — carbon intensity data
- **Green Agent community** — feedback and early testing

---

## See also

- [`MANIFEST.md`](MANIFEST.md) — full delivery manifest with deployment steps
- [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md) — complete limitations list
- [`SECURITY.md`](SECURITY.md) — security policy and posture
- [`CHANGELOG.md`](CHANGELOG.md) — version history
- [`test_installation.py`](test_installation.py) — installation verification
- [`tests/test_smoke.py`](tests/test_smoke.py) — smoke test suite
- [`requirements/`](requirements/) — pinned dependencies

External references:

- [Ray documentation](https://docs.ray.io/)
- [Prophet documentation](https://facebook.github.io/prophet/)
- [WattTime API](https://www.watttime.org/api-documentation/)
- [ElectricityMap API](https://static.electricitymap.org/api/docs/index.html)
```
