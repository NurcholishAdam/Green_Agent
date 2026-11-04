#!/usr/bin/env bash
#
# fix-workflow.sh — Install corrected deploy workflow (Enhanced, v2)
# ==================================================================
#
# Fixes applied vs. v1
# --------------------
#   P0  Verification grepped for `if:.*KUBECONFIG` and reported it as
#       "good" — that's the BUG, not the fix. A correct workflow would
#       fail the check.                                    -> semantic YAML
#                                                             validation
#   P0  No validation that the source `deploy.yml` actually contains the
#       fix; a user who downloaded the wrong file got a "✅ installed"
#       message                                        -> pre-install
#                                                          validation
#   P0  RayCluster patch replaced `containers` array entirely,
#       wiping image/resources/ports/volumes           -> envFrom +
#                                                          ConfigMap
#   P0  No `kustomization.yaml` in the generated overlay; `kubectl
#       apply -k` failed                               -> generated
#   P0  Enhancements directory path pointed to files that don't exist
#       in this codebase (`quantum_integration/...`)   -> verified against
#                                                          real paths
#   P0  `.env.enhancements` contained `MODP_WEIGHTS=[...]` — invalid
#       for any standard parser                       -> JSON object +
#                                                         per-key vars
#   P0  `.env.enhancements` was never read by any Python module
#       (verified across files 1–15)                   -> created with an
#                                                          explicit
#                                                          "stub only"
#                                                          note, and only
#                                                          under
#                                                          --allow-stubs
#   P1  `set -e` only                                  -> set -euo pipefail
#   P1  `if $ENHANCEMENTS` (fragile)                   -> [[ ... == true ]]
#   P1  Only `$1` was parsed                           -> case loop
#   P1  No rollback if the copy or install failed      -> trap + restore
#   P1  Backup used `$(date +%s)` (accumulated backups) -> single .backup
#                                                          file
#   P1  Repo check accepted any directory with `.github/` -> requires
#                                                          marker file
#   P1  `.env` was world-readable                      -> chmod 600
#                                                          + .gitignore
#   P1  `.env` and RayCluster patch were inconsistent   -> generated from
#                                                          one source
#   P2  Summary said "actual deployment only happens if KUBECONFIG is
#       configured" — false: `if: secrets.*` never fires -> reworded
#   P2  No --dry-run / --revert / --help               -> all three added
#
# The workflow installed by this script MUST pass semantic validation:
#
#   - It has a `deploy-to-cluster` job.
#   - That job has an `if:` gate.
#   - The gate does NOT reference `secrets.*` in `if:` (per GitHub
#     Actions semantics, that condition is always false; see
#     file 19 §2.1).
#   - The gate references `workflow_dispatch` (the correct pattern).
#
# If the source file fails any of these checks, the script restores the
# previous workflow from backup and exits non-zero.
#
# Usage:
#   ./fix-workflow.sh --help
#   ./fix-workflow.sh --dry-run
#   ./fix-workflow.sh                 # install + validate
#   ./fix-workflow.sh --revert        # restore most recent backup
#   ./fix-workflow.sh --enhancements  # also scaffold enhancement overlay
#   ./fix-workflow.sh --allow-stubs   # also write .env.enhancements stub
#
# Exit codes:
#   0  success
#   1  validation failure / install failure / missing prerequisites
#   2  invalid CLI arguments

set -euo pipefail

# ============================================================================
# Constants
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_MARKER="run_agent.py"           # presence check; adjust if renamed
WORKFLOW_DIR=".github/workflows"
WORKFLOW=".github/workflows/deploy.yml"
BACKUP=".github/workflows/deploy.yml.backup"
ENH_OVERLAY="config/overlays/enhancements"
ENV_FILE="config/environments/.env.enhancements"

# ============================================================================
# Colour support (tty only, respects NO_COLOR)
# ============================================================================

if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]] && [[ "${TERM:-}" != "dumb" ]]; then
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'
    C_YELLOW=$'\033[33m'; C_BLUE=$'\033[36m'; C_NC=$'\033[0m'
else
    C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_NC=""
fi

log()   { printf '%s[info]%s %s\n'  "$C_BLUE"   "$C_NC" "$*"; }
ok()    { printf '%s[ok]%s %s\n'    "$C_GREEN"  "$C_NC" "$*"; }
warn()  { printf '%s[warn]%s %s\n'  "$C_YELLOW" "$C_NC" "$*" >&2; }
err()   { printf '%s[error]%s %s\n' "$C_RED"    "$C_NC" "$*" >&2; }
die()   { err "$*"; exit 1; }

# ============================================================================
# Usage
# ============================================================================

usage() {
    cat <<'EOF'
Usage: fix-workflow.sh [OPTIONS]

Installs a corrected .github/workflows/deploy.yml and validates it.

The installed workflow is checked semantically (via Python + PyYAML):
  - a deploy-to-cluster job exists
  - its `if:` gate does not use secrets.* (which always evaluates false)
  - its `if:` gate references workflow_dispatch

If validation fails, the previous workflow is restored from backup.

Options:
  --dry-run           Show what would change; do not mutate the repo.
  --revert            Restore the most recent backup and remove any
                      enhancement files created by a previous run.
  --enhancements      Scaffold the enhancement kustomize overlay under
                      config/overlays/enhancements/ (ConfigMap + patch +
                      kustomization).
  --allow-stubs       Also write config/environments/.env.enhancements.
                      This file is NOT read by any Python module today
                      (see analyses of files 1–15); it is a stub for
                      future use. Requires --enhancements.
  --help, -h          Show this message.

Environment:
  NO_COLOR            Disable ANSI colour output.
  SOURCE_DEPLOY_YML   Override the source workflow path. Defaults to
                      ./deploy.yml or <script_dir>/deploy.yml.

Exit codes:
  0  success
  1  validation failure / install failure / missing prerequisites
  2  invalid CLI arguments
EOF
}

# ============================================================================
# Flag parsing
# ============================================================================

DRY_RUN=false
REVERT=false
ENHANCEMENTS=false
ALLOW_STUBS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)      DRY_RUN=true;       shift ;;
        --revert)       REVERT=true;        shift ;;
        --enhancements) ENHANCEMENTS=true;  shift ;;
        --allow-stubs)  ALLOW_STUBS=true;   shift ;;
        -h|--help)      usage; exit 0 ;;
        *)              err "Unknown flag: $1"; usage; exit 2 ;;
    esac
done

if [[ "$ALLOW_STUBS" == "true" ]] && [[ "$ENHANCEMENTS" != "true" ]]; then
    die "--allow-stubs requires --enhancements"
fi

# ============================================================================
# Repo check
# ============================================================================

# Require a repo marker AND a Python project marker. This prevents the
# script from running in an unrelated repo that happens to have .github/.
check_repo() {
    if [[ ! -f "$REPO_MARKER" ]] && [[ ! -f pyproject.toml ]] && [[ ! -f requirements.txt ]]; then
        err "Not a Green_Agent repository: no $REPO_MARKER, pyproject.toml, or requirements.txt"
        err "Current directory: $(pwd)"
        die "cd to the repository root and retry"
    fi
    # Belt-and-suspenders: look for at least one of the codebase's known
    # source directories.
    local found=0
    for d in core metrics rlhf constraints reporting analysis visualization examples; do
        [[ -d "$d" ]] && { found=1; break; }
    done
    (( found )) || die "No known source directories found; wrong repo?"
    ok "Detected Green_Agent repository at $(pwd)"
}

# ============================================================================
# Rollback trap
# ============================================================================

INSTALL_IN_PROGRESS=false
cleanup() {
    local exit_code=$?
    if [[ "$INSTALL_IN_PROGRESS" == "true" ]] && [[ -f "$BACKUP" ]]; then
        cp -f "$BACKUP" "$WORKFLOW" 2>/dev/null || true
        warn "Install failed (exit $exit_code); restored workflow from $BACKUP"
    fi
    exit $exit_code
}
trap cleanup EXIT

# ============================================================================
# Revert mode
# ============================================================================

do_revert() {
    log "Reverting installation"
    if [[ -f "$BACKUP" ]]; then
        cp -f "$BACKUP" "$WORKFLOW"
        rm -f "$BACKUP"
        ok "Restored $WORKFLOW from backup"
    else
        warn "No backup at $BACKUP; nothing to restore"
    fi
    if [[ -d "$ENH_OVERLAY" ]]; then
        rm -rf "$ENH_OVERLAY"
        ok "Removed $ENH_OVERLAY"
    fi
    if [[ -f "$ENV_FILE" ]]; then
        rm -f "$ENV_FILE"
        ok "Removed $ENV_FILE"
    fi
}

# ============================================================================
# Workflow validation (semantic, via Python + PyYAML)
# ============================================================================

validate_workflow() {
    local path="$1"
    python3 - "$path" <<'PY'
import sys

path = sys.argv[1]
try:
    import yaml
except ImportError:
    print(f"{path}: PyYAML not installed; cannot validate", file=sys.stderr)
    sys.exit(1)

try:
    with open(path) as f:
        wf = yaml.safe_load(f)
except yaml.YAMLError as exc:
    print(f"{path}: invalid YAML — {exc}", file=sys.stderr)
    sys.exit(1)

if not isinstance(wf, dict):
    print(f"{path}: not a mapping at top level", file=sys.stderr)
    sys.exit(1)

jobs = wf.get("jobs", {})
if not isinstance(jobs, dict):
    print(f"{path}: jobs is not a mapping", file=sys.stderr)
    sys.exit(1)

deploy = jobs.get("deploy-to-cluster")
if deploy is None:
    print(
        f"{path}: no 'deploy-to-cluster' job — cannot verify the fix",
        file=sys.stderr,
    )
    sys.exit(1)

cond = deploy.get("if")
cond_str = str(cond) if cond is not None else ""
if not cond_str:
    print(
        f"{path}: deploy-to-cluster has no 'if:' gate — will run on every "
        f"push/PR, not just workflow_dispatch",
        file=sys.stderr,
    )
    sys.exit(1)

if "secrets." in cond_str:
    print(
        f"{path}: deploy-to-cluster uses secrets.* in if: — this condition "
        f"always evaluates false (GitHub Actions restricts the secrets "
        f"context in jobs.<job_id>.if). Use "
        f"`if: ${{{{ github.event_name == 'workflow_dispatch' }}}}`.",
        file=sys.stderr,
    )
    sys.exit(1)

if "workflow_dispatch" not in cond_str:
    print(
        f"{path}: deploy-to-cluster is not gated on workflow_dispatch.\n"
        f"  Current gate: {cond_str}\n"
        f"  Fix: change to `if: ${{{{ github.event_name == 'workflow_dispatch' }}}}`",
        file=sys.stderr,
    )
    sys.exit(1)

print("OK: deploy-to-cluster is correctly gated on workflow_dispatch")
PY
}

# ============================================================================
# Install workflow
# ============================================================================

find_source() {
    if [[ -n "${SOURCE_DEPLOY_YML:-}" ]]; then
        [[ -f "$SOURCE_DEPLOY_YML" ]] || die "SOURCE_DEPLOY_YML=$SOURCE_DEPLOY_YML not found"
        echo "$SOURCE_DEPLOY_YML"
        return
    fi
    if [[ -f "${SCRIPT_DIR}/deploy.yml" ]]; then
        echo "${SCRIPT_DIR}/deploy.yml"
        return
    fi
    if [[ -f "deploy.yml" ]]; then
        echo "deploy.yml"
        return
    fi
    return 1
}

install_workflow() {
    local source
    if ! source="$(find_source)"; then
        die "deploy.yml not found. Provide it in one of:
  - \$SOURCE_DEPLOY_YML
  - <script_dir>/deploy.yml
  - ./deploy.yml
See file 19 v2 for a corrected workflow template."
    fi
    log "Source workflow: $source"

    # Pre-validate the source BEFORE touching the destination.
    log "Validating source workflow..."
    if ! validate_workflow "$source"; then
        die "Source workflow failed validation; aborting before making any changes"
    fi
    ok "Source workflow is valid"

    if [[ "$DRY_RUN" == "true" ]]; then
        log "(dry-run) would back up $WORKFLOW to $BACKUP (if it exists)"
        log "(dry-run) would copy $source to $WORKFLOW"
        return 0
    fi

    mkdir -p "$WORKFLOW_DIR"

    if [[ -f "$WORKFLOW" ]]; then
        cp -f "$WORKFLOW" "$BACKUP"
        log "Backed up existing workflow to $BACKUP"
    fi

    INSTALL_IN_PROGRESS=true
    cp -f "$source" "$WORKFLOW"
    log "Installed $source → $WORKFLOW"

    log "Re-validating installed workflow..."
    validate_workflow "$WORKFLOW" || die "Installed workflow failed validation"
    ok "Installed workflow passes semantic validation"
    INSTALL_IN_PROGRESS=false
}

# ============================================================================
# Enhancement overlay (ConfigMap + JSON patch + kustomization)
# ============================================================================
#
# Uses envFrom + ConfigMap rather than patching the container's env array.
# The v1 patch replaced `containers` entirely, wiping image/resources/
# ports/volumes on apply. This version preserves the existing container
# spec and only adds an envFrom reference.
#
# The JSON patch adds `envFrom` on container index 0 of the head group
# and the first worker group. If a deployment already has envFrom entries,
# they are replaced — document this in the overlay's README.

write_enhancement_overlay() {
    log "Scaffolding enhancement overlay under $ENH_OVERLAY"

    # Verify the codebase actually has enhancement code we can point at.
    # v1 pointed to `quantum_integration/quantum-limit-graph-v2.4.0/...`
    # which does not exist in this repo (verified across files 1–15).
    local found=0
    for d in core metrics rlhf constraints reporting; do
        [[ -d "$d" ]] && { found=1; break; }
    done
    if (( ! found )); then
        die "No enhancement source directories found (core/, metrics/, etc.);
this repo does not appear to contain the enhancement modules v1 referenced"
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log "(dry-run) would create $ENH_OVERLAY/{configmap,patch,kustomization}.yaml"
        return 0
    fi

    mkdir -p "$ENH_OVERLAY"

    # ConfigMap: environment variables the Python modules would read IF
    # they had env-var support. None of them do today (per file 4/7/13/15
    # analyses), but this ConfigMap is the correct K8s primitive to use
    # once that support is added.
    cat > "$ENH_OVERLAY/configmap.yaml" <<'EOF'
# Enhancement environment for green-agent pods.
#
# These env vars are exposed to the pod via envFrom (see patch.yaml).
# No Python module in this codebase reads them today. When env-var support
# is added to each module's __init__, the variable names here are the
# agreed contract.
#
# Note on MODP_WEIGHTS: the codebase uses SIX different orderings across
# files 13–15. This ConfigMap uses named keys (JSON object) rather than
# an ordered array so the semantics are unambiguous.
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-enhancements
  labels:
    app: green-agent
    app.kubernetes.io/component: enhancement-config
data:
  ENHANCEMENTS_ENABLED: "true"
  LIMIT_GRAPH_ENABLED: "true"
  LIMIT_GRAPH_CENTRALITY: "0.7"
  LIMIT_GRAPH_CONNECTIVITY: "0.6"
  MODP_ENABLED: "true"
  MODP_WEIGHTS_JSON: '{"quality":0.4,"latency":0.3,"energy":0.2,"carbon":0.1}'
  RLHF_ENABLED: "true"
  HUMAN_FEEDBACK_SCORE: "0.6"
  DISTILLATION_ENABLED: "true"
  MOE_GATING_ENABLED: "true"
  EVOLUTIONARY_ENABLED: "true"
  POPULATION_SIZE: "20"
  MUTATION_RATE: "0.1"
  FLEXGEN_ENABLED: "false"
  FLEXGEN_MODEL_NAME: "facebook/opt-6.7b"
  FLEXGEN_DEFAULT_PRECISION: "fp16"
EOF

    # JSON patch — appends an envFrom to the head and first worker group.
    # Uses `add /envFrom` (replaces if present) rather than `add /envFrom/-`
    # (fails if absent). If you already have envFrom entries and want to
    # preserve them, edit this patch to use `add /envFrom/-` and pre-seed
    # the array first.
    cat > "$ENH_OVERLAY/patch.yaml" <<'EOF'
# JSON patch (RFC 6902). Applied via kustomize to RayCluster.
#
# Adds an envFrom reference to the head container and the first worker
# group's container. Uses ConfigMapRef with `optional: true` so the patch
# is safe even if the ConfigMap hasn't been created yet.
#
# If you have multiple worker groups, duplicate the second operation
# with the appropriate index (0, 1, 2, ...).
- op: add
  path: /spec/headGroupSpec/template/spec/containers/0/envFrom
  value:
  - configMapRef:
      name: green-agent-enhancements
      optional: true
- op: add
  path: /spec/workerGroupSpecs/0/template/spec/containers/0/envFrom
  value:
  - configMapRef:
      name: green-agent-enhancements
      optional: true
EOF

    # Kustomization — applies the ConfigMap and the JSON patch. The `target`
    # block scopes the patch to the RayCluster named `green-agent-cluster`.
    cat > "$ENH_OVERLAY/kustomization.yaml" <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- configmap.yaml
patches:
- path: patch.yaml
  target:
    group: ray.io
    version: v1
    kind: RayCluster
    name: green-agent-cluster
EOF

    ok "Created $ENH_OVERLAY/{configmap,patch,kustomization}.yaml"

    if [[ "$ALLOW_STUBS" == "true" ]]; then
        write_enhancement_env_stub
    else
        log "Skipped .env.enhancements (pass --allow-stubs to create the stub)"
    fi
}

# Local-dev env stub. NOT read by any Python module today.
write_enhancement_env_stub() {
    log "Writing $ENV_FILE stub (not read by any Python module today)"
    if [[ "$DRY_RUN" == "true" ]]; then
        log "(dry-run) would create $ENV_FILE"
        return 0
    fi
    mkdir -p "$(dirname "$ENV_FILE")"
    cat > "$ENV_FILE" <<'EOF'
# Enhancement environment for LOCAL DEVELOPMENT.
#
# NOTE: No Python module in this codebase reads these variables today.
# This file is a placeholder for when env-var support is added to each
# module's __init__ (see analyses of files 4, 7, 13, 15).
#
# MODP_WEIGHTS_JSON uses a JSON object with named keys, NOT an ordered
# array — the codebase has six different orderings across files 13–15,
# so ordered arrays are ambiguous.
ENHANCEMENTS_ENABLED=true
LIMIT_GRAPH_ENABLED=true
LIMIT_GRAPH_CENTRALITY=0.7
LIMIT_GRAPH_CONNECTIVITY=0.6
MODP_ENABLED=true
MODP_WEIGHTS_JSON={"quality":0.4,"latency":0.3,"energy":0.2,"carbon":0.1}
RLHF_ENABLED=true
HUMAN_FEEDBACK_SCORE=0.6
DISTILLATION_ENABLED=true
MOE_GATING_ENABLED=true
EVOLUTIONARY_ENABLED=true
POPULATION_SIZE=20
MUTATION_RATE=0.1
FLEXGEN_ENABLED=false
FLEXGEN_MODEL_NAME=facebook/opt-6.7b
FLEXGEN_DEFAULT_PRECISION=fp16
EOF
    chmod 600 "$ENV_FILE"
    ok "Wrote $ENV_FILE (chmod 600)"

    # Auto-ignore the stub so `git add -A` doesn't commit it.
    if [[ -f .gitignore ]] && ! grep -qxF 'config/environments/.env.*' .gitignore; then
        echo 'config/environments/.env.*' >> .gitignore
        ok "Added config/environments/.env.* to .gitignore"
    fi
}

# ============================================================================
# Summary
# ============================================================================

print_summary() {
    echo ""
    echo "What changed:"
    echo "  • Workflow now gates deploy-to-cluster on workflow_dispatch"
    echo "  • Deploy job no longer references secrets.* in its if: condition"
    echo "    (that condition always evaluated false; deployment never ran)"
    echo "  • Verify gate: 'kubectl get hpa -n green-agent' should show scaling"
    echo "    metrics once the KUBECONFIG secret is set and a dispatch is run"
    if [[ "$ENHANCEMENTS" == "true" ]]; then
        echo "  • Enhancement overlay scaffolded under $ENH_OVERLAY"
        echo "    Apply with: kubectl apply -k $ENH_OVERLAY"
        echo "    NOTE: the Python modules in this repo do not yet read the"
        echo "    env vars in the ConfigMap; the overlay is a stub."
    fi
    echo ""
    echo "Next steps:"
    echo "  1. Review the changes:"
    echo "       git diff $WORKFLOW"
    [[ "$ENHANCEMENTS" == "true" ]] && echo "       git status $ENH_OVERLAY"
    [[ -f "$ENV_FILE" ]] && echo "       git status $ENV_FILE (ignored by .gitignore)"
    echo ""
    echo "  2. Commit and push:"
    echo "       git add $WORKFLOW"
    [[ "$ENHANCEMENTS" == "true" ]] && echo "       git add $ENH_OVERLAY"
    echo "       git commit -m 'fix(ci): gate deploy on workflow_dispatch'"
    echo "       git push origin main"
    echo ""
    echo "  3. Trigger a deploy via the Actions tab (Run workflow)."
    echo ""
    echo "To undo: $0 --revert"
}

# ============================================================================
# Main
# ============================================================================

main() {
    log "Green Agent — workflow installer (v2)"
    check_repo

    if [[ "$REVERT" == "true" ]]; then
        do_revert
        exit 0
    fi

    install_workflow

    if [[ "$ENHANCEMENTS" == "true" ]]; then
        write_enhancement_overlay
    fi

    print_summary
    ok "Installation complete"
}

main
