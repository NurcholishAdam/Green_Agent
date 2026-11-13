
# Scripts and Utilities

| Field | Value |
|---|---|
| **Version** | 1.2.0 |
| **Last Updated** | 2026-11-13 |
| **Status** | Production Ready |
| **Audience** | Operators, integrators, maintainers |
| **Companion docs** | `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) · `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0) · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `QUICK_START.md` (v1.2.0) · `.cursorrules` |

---

## Table of Contents

1. [Directory Inventory](#directory-inventory)
2. [Core Scripts](#core-scripts)
3. [Configuration Flow](#configuration-flow)
4. [Required Environment Variables](#required-environment-variables)
5. [Monitoring Setup](#monitoring-setup)
6. [Integration with Config/Enhancements](#integration-with-configenhancements)
7. [Docker Integration](#docker-integration)
8. [Security](#security)
9. [Shell Best Practices](#shell-best-practices)
10. [Inspecting State with lean-ctx](#inspecting-state-with-lean-ctx)
11. [Adding New Scripts](#adding-new-scripts)
12. [Troubleshooting](#troubleshooting)
13. [What's Not in Scope](#whats-not-in-scope)
14. [Changelog](#changelog)
15. [Reviewer Checklist](#reviewer-checklist)

---

## Directory Inventory

```
scripts/
├── setup.sh                          # Setup & dependency installation
├── verify.sh                         # 5-check verification (see §verify.sh)
├── migrate.sh                        # Old → new src layout migration
├── cleanup.sh                        # Cleanup (dry-run default since v1.2)
├── deploy.sh                         # Kubernetes deployment
├── community_data_collector.py       # Community carbon/helium data collector (v1.2)
├── lib/
│   └── rewrite_imports.py            # Import rewriter used by migrate.sh
└── monitoring/
    ├── prometheus.yml                # Prometheus scrape config
    ├── grafana-dashboard.json        # Grafana dashboard
    ├── alertmanager.yml              # Alertmanager routing (if present)
    └── loki-config.yml               # Loki log aggregation (if present)
```

> Files marked `(if present)` are optional; `verify.sh` warns (does not fail) when missing.

---

## Core Scripts

### Setup & Installation

- `setup.sh` — idempotent initial setup and dependency installation
- `verify.sh` — 5-check verification (see below)
- `migrate.sh` — old `quantum_integration/...` → `src/` migration (see `REPOSITORY_REFACTORING_GUIDE.md` v0.2.0 Option 2)
- `cleanup.sh` — dry-run by default; requires `--force` to actually delete

### Deployment

- `deploy.sh` — deploy to Kubernetes cluster
- `scripts/monitoring/prometheus.yml` — Prometheus config
- `scripts/monitoring/grafana-dashboard.json` — Grafana dashboard

### Tooling

- `community_data_collector.py` — collect/contribute community carbon & helium observations; `--enhanced` enables Zero Trust, distillation, RLHF, MoE, symbolic oversight

---

### `setup.sh`

```bash
./scripts/setup.sh [--no-venv] [--no-enhancements] [--recreate]
```

**Pre-flight checks** (fails fast if missing):

| Tool | Required? | Bypass flag |
|---|---|---|
| Python ≥ 3.10 | ✅ | — |
| `pip` | ✅ | — |
| `git` | ✅ | — |
| `docker` | Optional | `--no-docker` |
| `kubectl` | Optional | `--no-k8s` |

**Performs:**

1. Creates `.venv` (unless `--no-venv`; overwritten only with `--recreate`)
2. `pip install --upgrade pip`
3. `pip install -r requirements.txt`
4. `pip install -e .` (registers `src/` layout via `pyproject.toml`)
5. `pip install -r requirements-enhancements.txt` (unless `--no-enhancements`)
6. `pip check` — fail on dependency conflicts
7. Generates `.env` from `.env.example` if missing
8. Runs `python -c "import src.enhancements"` smoke test

**Idempotent:** re-running is safe; only `.venv` is recreated with `--recreate`.

**Example:**

```text
▶ Pre-flight: Python 3.11.4 ✓ · pip 24.0 ✓ · git 2.43 ✓ · docker 24.0 ✓
▶ Creating .venv ... ✅
▶ pip install --upgrade pip ... ✅
▶ pip install -r requirements.txt ... ✅
▶ pip install -e . ... ✅
▶ pip install -r requirements-enhancements.txt ... ✅
▶ pip check ... ✅
▶ .env created from .env.example ✅
▶ import smoke test ... ✅
✅ setup complete — activate with: source .venv/bin/activate
```

---

### `verify.sh` — 5-check contract

```bash
./scripts/verify.sh            # full checks
./scripts/verify.sh --quick    # import smoke only
./scripts/verify.sh --strict   # full + Docker build test + shellcheck
./scripts/verify.sh --no-pytest
```

**Exit codes:** `0` pass · `1` failure · `2` usage error.

**Checks performed** (matching `REPOSITORY_REFACTORING_GUIDE.md` v0.2.0 §Testing):

| # | Check | Command | Bypass |
|---|---|---|---|
| 1 | File-preservation (MANIFEST.sha256) | `sha256sum --quiet -c MANIFEST.sha256` | — |
| 2 | No old imports outside shim | `grep -rn 'from quantum_integration' --include='*.py' --exclude-dir='quantum_integration'` | — |
| 3 | No old paths in CI | `grep -rn 'quantum-limit-graph\|limit-agentbench' .github/workflows/` | — |
| 4 | Import smoke | `python -c "from src.enhancements import *"` + `python -c "from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine"` | `--quick` runs only this |
| 5 | pytest | `python -m pytest tests/unit tests/integration -q` | `--no-pytest` |

**Strict additions:**
- `docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:verify .`
- `shellcheck scripts/*.sh`

**Example output:**

```text
▶ [1/5] Checking MANIFEST.sha256 ... ✅
▶ [2/5] Checking no old imports remain outside shim ... ✅
▶ [3/5] Checking CI references no old paths ... ✅
▶ [4/5] Import smoke test ... ✅
▶ [5/5] pytest ... 19 passed, 8 passed in 4.2s ✅
✅ all checks passed
```

---

### `migrate.sh`

```bash
./scripts/migrate.sh --dry-run         # show what would move
./scripts/migrate.sh --apply           # use git mv (preserves history)
./scripts/migrate.sh --verify          # run scripts/verify.sh after migration
```

**Behavior** (matching `REPOSITORY_REFACTORING_GUIDE.md` v0.2.0 §Option 2):

- Uses `git mv` (not `mv`) to preserve `git log --follow`
- Creates `MANIFEST.sha256` from the pre-migration tree if absent
- Rewrites imports via `scripts/lib/rewrite_imports.py`
- Installs the backward-compat shim at the old path
- **Refuses to run** unless `git status --porcelain` is clean
- Emits a `git mv` command list under `--dry-run` without touching files

**Exit codes:** `0` success · `1` dirty tree or migration failure · `2` usage error.

---

### `cleanup.sh`

```bash
./scripts/cleanup.sh                 # dry-run (prints what would be removed)
./scripts/cleanup.sh --force         # actually delete
./scripts/cleanup.sh --keep-venv     # preserve .venv
./scripts/cleanup.sh --docker-images # also remove green-agent:* images
./scripts/cleanup.sh --checkpoints   # also remove checkpoints/ (dangerous)
```

**Scope** (explicit):

| Path | Removed by default | Removed by `--force` |
|---|---|---|
| `__pycache__/`, `*.pyc` | ✅ | ✅ |
| `.pytest_cache/`, `.mypy_cache/` | ✅ | ✅ |
| `build/`, `dist/`, `*.egg-info/` | ✅ | ✅ |
| `.venv/` | ❌ | ✅ (unless `--keep-venv`) |
| `green-agent:*` Docker images | ❌ | only with `--docker-images` |
| `checkpoints/` | ❌ | only with `--checkpoints` |
| `symbolic_violations.json`, `symbolic_violation_report.json` | ❌ | ✅ (with `--force`) |

> **v1.2 behavior change:** `cleanup.sh` now **defaults to dry-run**. Explicit `--force` is required to delete anything.

---

### `deploy.sh`

```bash
./scripts/deploy.sh [environment] [--dry-run] [--build] [--no-build] [--rollback REVISION]
```

**Environments:** `development`, `staging`, `production`

**Deployment process:**

1. Validates configuration (see [Required Environment Variables](#required-environment-variables))
2. Builds Docker image (unless `--no-build`); multi-arch via `docker buildx --platform linux/amd64,linux/arm64`
3. Applies Kubernetes manifests from `config/k8s/overlays/[environment]/`
4. Waits for rollout (`kubectl rollout status`)
5. Runs post-deployment smoke tests (imports + `/health`)

**Required Kubernetes resources** (must exist in each overlay):

| Kind | Name | Contents |
|---|---|---|
| `Secret` | `green-agent-secrets` | `ZERO_TRUST_TOKEN`, `RLHF_API_KEY` (if used) |
| `ConfigMap` | `green-agent-env` | `ENHANCEMENTS_ENABLED`, per-module flags, `LOG_LEVEL`, `LOG_FORMAT`, `SYMBOLIC_POLICY` |
| `ConfigMap` | `green-agent-policies` | `green_policy.yaml`, `symbolic_policy.yaml` |

**Rollback:**

```bash
./scripts/deploy.sh production --rollback <revision>
```

**Example:**

```bash
# Dry-run
./scripts/deploy.sh production --dry-run

# Full deploy
./scripts/deploy.sh production --build

# Rollback
./scripts/deploy.sh production --rollback 42
```

---

### `community_data_collector.py`

```bash
# Contribute
python scripts/community_data_collector.py --contribute [--region eu-north] [--enhanced]

# Fetch
python scripts/community_data_collector.py --fetch [--regions us-east,eu-north] [--enhanced]

# Safe local test (no writes to community hub)
python scripts/community_data_collector.py --contribute --enhanced --dry-run

# Structured output for automation
python scripts/community_data_collector.py --fetch --output-json fetch.json

# Force legacy mode (overrides ENHANCEMENTS_ENABLED)
python scripts/community_data_collector.py --contribute --no-enhancements
```

**Flags:**

| Flag | Effect |
|---|---|
| `--contribute` | Collect and contribute observations |
| `--fetch` | Fetch and display community data |
| `--region R` | Region for contribute or single-region fetch (default: `us-east`) |
| `--regions R1,R2` | Multi-region fetch |
| `--enhanced` | Force enhanced mode (overrides `ENHANCEMENTS_ENABLED`) |
| `--no-enhancements` | Force legacy mode |
| `--dry-run` | Authenticate + evaluate but do not write to community hub |
| `--output-json PATH` | Write structured `ContributionResult` / `FetchResult` |
| `--symbolic-policy PATH` | Path to `symbolic_policy.yaml` |
| `--verbose` / `--quiet` | Log level control |

See [`scripts/community_data_collector.py`](community_data_collector.py) (v1.2.0) for the full implementation.

---

## Configuration Flow

```
scripts/setup.sh
    ↓
config/environments/.env.[environment]  ← loaded by deploy.sh / run_agent.py
    ↓
config/green_agent.yaml
    ↓
src/enhancements/config.yaml
    ↓
symbolic_policy.yaml                    ← 4th layer (NEW in v1.2)
    ↓
Application initialized
```

Load environment variables:

```bash
set -a
source config/environments/.env.development
set +a
```

> **Secrets hygiene:** `.env.*` files must **not** be committed. Use `.env.example`
> (checked in) as a template; `.env.*` is gitignored.

---

## Required Environment Variables

Env vars are **canonical**; YAML config maps onto them (env wins on conflict — matches Dockerfile v2.0 and Architecture v1.2).

### Meta-cognitive

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `LOG_FORMAT` | `text` | `text` or `json` |
| `ENVIRONMENT` | `production` | `dev` / `staging` / `production` |

### Enhancements

| Variable | Default | Description |
|---|---|---|
| `ENHANCEMENTS_ENABLED` | `false` | Master switch |
| `LIMIT_GRAPH_ENABLED` | `true` | LIMIT Graph metrics |
| `MODP_ENABLED` | `true` | MODP composite score |
| `RLHF_ENABLED` | `true` | Human feedback |
| `DISTILLATION_ENABLED` | `true` | Multi-Teacher distillation |
| `MOE_ENABLED` (alias `MOE_GATING_ENABLED`) | `true` | MoE gate metrics |
| `EVOLUTIONARY_ENABLED` | `true` | Bio-inspired optimisation |
| `FLEXGEN_ENABLED` | `false` | FlexGen delegation (opt-in) |
| `ZERO_TRUST_ENABLED` | `false` | Zero Trust (opt-in) |
| `DIFFERENTIAL_PRIVACY_ENABLED` | `false` | DP budget tracking (opt-in) |
| `TRAINING_ENABLED` | `false` | Checkpoint tracking (opt-in) |

### Secrets

| Variable | Required when | Notes |
|---|---|---|
| `ZERO_TRUST_TOKEN` | `ZERO_TRUST_ENABLED=true` | Injected via K8s `Secret`, not committed |
| `RLHF_API_KEY` | `RLHF_ENABLED=true` and remote feedback | Same |

### Symbolic layer

| Variable | Default | Description |
|---|---|---|
| `SYMBOLIC_POLICY` | `symbolic_policy.yaml` | Path to symbolic rule file; oversight is always-on when the file exists |

---

## Monitoring Setup

1. Copy monitoring configs:

   ```bash
   cp scripts/monitoring/* /path/to/monitoring/
   ```

2. Update Prometheus targets for your environment:

   ```yaml
   # scripts/monitoring/prometheus.yml
   scrape_configs:
     - job_name: green-agent
       static_configs:
         - targets: ['green-agent:8000']
   ```

3. Import Grafana dashboard:

   ```bash
   curl -X POST -H "Content-Type: application/json" \
     -d @scripts/monitoring/grafana-dashboard.json \
     http://admin:admin@localhost:3000/api/dashboards/db
   ```

4. (Optional) Enable alerting:

   ```bash
   kubectl apply -f scripts/monitoring/alertmanager.yml
   ```

---

## Integration with Config/Enhancements

Scripts automatically:

- Load all configurations from `config/`
- Apply enhancement module settings from `src/enhancements/config.yaml` **and** the `ENHANCEMENTS_ENABLED` env (env wins)
- Load `symbolic_policy.yaml` when present (symbolic oversight is always-on)
- Use K8s overlays specific to the deployed environment
- Initialize monitoring based on `scripts/monitoring/` configs
- Verify `MANIFEST.sha256` before deploy (via `verify.sh`)

---

## Docker Integration

Scripts call `docker buildx` with the canonical ARG surface (matching Dockerfile v2.0):

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg ENHANCEMENTS_ENABLED=true \
  --build-arg MODP_ENABLED=true \
  --build-arg RLHF_ENABLED=true \
  --build-arg FLEXGEN_ENABLED=false \
  -t green-agent:full .
```

| Variant | Build args |
|---|---|
| **Full** (all enhancements) | `--build-arg ENHANCEMENTS_ENABLED=true` |
| **Lean** (minimal) | `--build-arg ENHANCEMENTS_ENABLED=false` |
| **Custom** | Override individual `*_ENABLED` flags |

---

## Security

| Concern | Guarantee |
|---|---|
| Secrets in `.env.*` | `.env.example` is the only committed file; `.env.*` is gitignored |
| K8s secrets | `green-agent-secrets` `Secret`; never in `ConfigMap` |
| `ZERO_TRUST_TOKEN` | Required when `ZERO_TRUST_ENABLED=true`; `verify.sh --strict` fails if missing |
| Symbolic rule evaluation | Whitelist parser — no `eval`/`exec` (see Symbolic README v1.2 §Security) |
| `block_request` action | Requires `ZERO_TRUST_ENABLED=true`; otherwise logged and ignored |
| `cleanup.sh` | Dry-run by default; `--force` required to delete |
| `migrate.sh` | Refuses to run unless `git status --porcelain` is clean |

---

## Shell Best Practices

Every script in this directory follows:

```bash
#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
```

- **`set -e`** — exit on error
- **`set -u`** — error on unset variables
- **`set -o pipefail`** — fail on any pipeline error
- **`IFS=$'\n\t'`** — safe word splitting

**Lint** with:

```bash
shellcheck scripts/*.sh
```

Run in CI as part of `verify.sh --strict`.

---

## Inspecting State with lean-ctx

When the `lean-ctx` MCP layer is available (see `.cursorrules`):

```python
ctx_read("src/enhancements/", mode="digest")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")
```

Useful before running `cleanup.sh` to confirm what state actually exists.

---

## Adding New Scripts

1. **Create the script** in this directory with the standard shebang block:

   ```bash
   #!/usr/bin/env bash
   set -euo pipefail
   IFS=$'\n\t'
   ```

2. **Add a documentation header**:

   ```bash
   # scripts/your_script.sh
   #
   # Purpose: one-line description
   # Usage:   ./scripts/your_script.sh [--flag]
   # Exit:    0 pass · 1 failure · 2 usage
   # Requires: [list external commands: docker, kubectl, jq, …]
   ```

3. **Add shellcheck-clean code**:

   ```bash
   shellcheck scripts/your_script.sh
   ```

4. **Add a `--dry-run` mode** for any destructive operation.

5. **Make executable**:

   ```bash
   chmod +x scripts/your_script.sh
   ```

6. **Add a test** in `tests/scripts/test_your_script.py` (pytest wrappers around `subprocess.run`):

   ```python
   # tests/scripts/test_your_script.py
   import subprocess
   import pytest

   @pytest.mark.parametrize("args", [["--help"], ["--dry-run"]])
   def test_your_script_smoke(args):
       result = subprocess.run(
           ["bash", "scripts/your_script.sh", *args],
           capture_output=True, text=True,
       )
       assert result.returncode in (0, 2)

   def test_your_script_requires_force():
       result = subprocess.run(
           ["bash", "scripts/your_script.sh"], capture_output=True, text=True,
       )
       assert "dry-run" in result.stdout.lower()
   ```

7. **Test in development environment first**:

   ```bash
   source config/environments/.env.development
   ./scripts/your_script.sh --dry-run
   ```

8. **Update this README** — add the script to §Directory Inventory and §Core Scripts.

9. **Update `verify.sh`** — if the script affects the build, add a check.

---

## Troubleshooting

### `verify.sh` fails at check 1 (MANIFEST)

**Cause:** files moved or modified outside `git mv`.
**Fix:** run `git status` to see what changed; if intentional, regenerate `MANIFEST.sha256`:

```bash
git ls-files | xargs sha256sum > MANIFEST.sha256
```

### `verify.sh` fails at check 2 (old imports)

**Cause:** new code uses `from quantum_integration` directly instead of the shim.
**Fix:** use `from src.enhancements import …`; if a shim is needed, add it under `quantum_integration/`.

### `setup.sh` fails on `pip install -e .`

**Cause:** `pyproject.toml` missing or malformed.
**Fix:** confirm `pyproject.toml` exists at repo root with `[tool.setuptools.packages.find] where = ["src"]`.

### `cleanup.sh` did nothing

**Cause:** dry-run is the default since v1.2.
**Fix:** add `--force` to actually delete.

### `deploy.sh` fails on missing secret

**Cause:** `ZERO_TRUST_TOKEN` not present in `green-agent-secrets` while `ZERO_TRUST_ENABLED=true`.
**Fix:** create the secret:

```bash
kubectl create secret generic green-agent-secrets \
  --from-literal=ZERO_TRUST_TOKEN="$ZERO_TRUST_TOKEN" \
  -n green-agent
```

### `community_data_collector.py --enhanced` warns "Enhanced modules unavailable"

**Cause:** `src/enhancements` not installed (missing `pip install -e .`) or missing optional deps.
**Fix:**

```bash
pip install -e .
pip install -r requirements-enhancements.txt
```

### `shellcheck` reports issues

**Cause:** scripts not conforming to `set -euo pipefail` or using unquoted variables.
**Fix:** run `shellcheck scripts/*.sh` and address each warning.

---

## What's Not in Scope

- **Package managers other than pip** (conda, poetry, uv) — the repo ships `requirements.txt` + `pyproject.toml`.
- **Non-Kubernetes deployment** (bare-metal, docker-swarm) — covered by Docker commands, not `deploy.sh`.
- **Monitoring stack installation** (Prometheus, Grafana) — only configs are shipped.
- **Secret rotation** — the repo provides the K8s Secret template but not rotation tooling.
- **Windows PowerShell equivalents** — scripts are bash-only; a PowerShell port is a follow-up.

---

## Changelog

### 1.2.0 (2026-04-10)

**Added**

- `scripts/migrate.sh` documentation (Option 2 of Refactor Guide v0.2).
- `scripts/community_data_collector.py` documentation with all CLI flags.
- Concrete 5-check contract for `verify.sh` (MANIFEST, old imports, CI paths, smoke, pytest).
- Exit-code contract (`0`/`1`/`2`).
- `--quick` / `--strict` / `--no-pytest` flags for `verify.sh`.
- `--dry-run` / `--force` / `--keep-venv` / `--docker-images` / `--checkpoints` flags for `cleanup.sh`.
- `--dry-run` / `--build` / `--no-build` / `--rollback` flags for `deploy.sh`.
- §Required Environment Variables (meta, enhancements, secrets, symbolic).
- §Docker Integration (`--build-arg ENHANCEMENTS_ENABLED=true` + variants).
- §Security (secrets hygiene, whitelist parser, `ZERO_TRUST_TOKEN`).
- §Shell Best Practices (`set -euo pipefail`, shellcheck).
- §lean-ctx inspection.
- §Troubleshooting.
- §What's Not in Scope.
- §Changelog, §Reviewer Checklist.
- Metadata block + TOC.
- K8s Secret/ConfigMap inventory.
- `scripts/monitoring/` full inventory (with `(if present)` markers).
- `scripts/lib/rewrite_imports.py` to §Directory Inventory.

**Changed**

- `verify.sh` description rewritten from vague bullets to 5 concrete checks matching Refactor Guide v0.2 §Testing.
- `setup.sh` now documents `pip install -e .`, `pip check`, pre-flight checks.
- `cleanup.sh` now documented as **dry-run by default** (v1.2 behavioral change).
- Config flow now includes `symbolic_policy.yaml`.
- Environment setup now includes `.env.example` + gitignore guidance.

**Fixed**

- Missing `scripts/migrate.sh` reference.
- Missing `scripts/community_data_collector.py` reference.
- Missing `MANIFEST.sha256` enforcement.
- Missing `pyproject.toml` install path.
- Missing `ENHANCEMENTS_ENABLED` / per-module flags.
- Missing `ZERO_TRUST_TOKEN`.

### 1.0.0

- Initial scripts README.

---

## Reviewer Checklist

- [ ] §Directory Inventory matches `ls scripts/` output.
- [ ] §`verify.sh` 5-check contract matches `REPOSITORY_REFACTORING_GUIDE.md` v0.2 §Testing verbatim.
- [ ] `scripts/migrate.sh` exists and uses `git mv`.
- [ ] `scripts/community_data_collector.py` exists with `--contribute` / `--fetch` / `--enhanced` / `--dry-run` / `--output-json`.
- [ ] `scripts/cleanup.sh` defaults to dry-run (v1.2 behavior) — confirmed by running without `--force`.
- [ ] `scripts/setup.sh` runs `pip check` and `pip install -e .`.
- [ ] `scripts/deploy.sh` lists required K8s Secrets/ConfigMaps.
- [ ] `config/environments/.env.example` exists; `.env.*` is in `.gitignore`.
- [ ] §Required Environment Variables matches Dockerfile v2.0 ARG/ENV names.
- [ ] §Docker Integration shows `--build-arg ENHANCEMENTS_ENABLED=true`.
- [ ] `symbolic_policy.yaml` appears in §Configuration Flow.
- [ ] Every script passes `shellcheck`.
- [ ] `scripts/verify.sh --strict` passes locally.
- [ ] Cross-links to companion docs resolve.
- [ ] No secrets committed in this file or referenced paths.

---

**Last Updated**: 2026-11-13  
**Version**: 1.2.0  
**Companion docs**: see header table  
**Breaking Changes**: `cleanup.sh` now defaults to dry-run (v1.2.0); add `--force` to restore v1.0 behavior

