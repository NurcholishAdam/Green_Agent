#!/usr/bin/env bash
#
# Green Agent Kubernetes Deployment (Enhanced, v2)
# =================================================
#
# Deploys the complete green-agent stack: namespace, network policies,
# monitoring, Ray cluster, HPA + VPA, Prometheus adapter, PDB, quota,
# priority class, and — with --enhancements — the enhancement ConfigMap,
# kustomize overlay, and RayCluster patch.
#
# Fixes applied vs. v1
# --------------------
#   P0  Adapter ConfigMap never deployed (HPA dead)   -> deployed to monitoring
#   P0  `set -e` only                                  -> set -euo pipefail
#   P0  `if $ENHANCEMENTS; then` (fragile)             -> [[ "$X" == "true" ]]
#   P0  Substring flag matching (--no-enhancements    -> `case` loop
#       enabled enhancements)
#   P0  No prerequisite checks (silent jq/CRD fail)    -> check_prerequisites()
#   P0  No --context / no confirmation (prod risk)     -> --context + prompt
#   P0  "Verify deployment" verified nothing           -> rollout + HPA check
#   P0  Final greps targeted non-existent metrics      -> real metric names
#   P1  Duplicate `carbon-autoscaler.yaml` apply       -> applied once
#   P1  Relative paths (cd to scripts breaks)          -> $SCRIPT_DIR
#   P1  No --dry-run / --delete / --namespace          -> all three added
#   P1  Missing patch silently continued               -> fail under --enh
#   P1  jq filter ignored ENHANCEMENTS_ENABLED value   -> checks ==true
#   P1  No rollout wait                                -> rollout status
#   P2  Decorative emoji (breaks non-UTF-8)            -> ASCII only
#   P2  No logging (nothing to trace)                  -> UTC timestamped log
#
# Expected directory layout:
#
#   k8s/
#   ├── network-policy.yaml
#   ├── grafana-dashboard.yaml
#   ├── monitoring.yaml               # ServiceMonitor, Prometheus
#   ├── ray-cluster.yaml              # RayCluster CR
#   ├── carbon-autoscaler.yaml        # HPA + VPA (file 16 §)
#   ├── pdb.yaml                      # PodDisruptionBudget (file 16 §3)
#   ├── quota.yaml                    # ResourceQuota (file 16 §2)
#   ├── priorityclass.yaml            # PriorityClass (file 16 §1)
#   ├── green-agent-adapter-config.yaml   # Adapter rules (file 17 §)
#   ├── prometheus-carbon-credit.yaml     # PrometheusRule (file 17 §)
#   ├── adapter-deployment.yaml       # Adapter Deployment + Service + RBAC + APIService
#   └── enhancements/                 # OPTIONAL — only with --enhancements
#       ├── kustomization.yaml
#       └── ...
#
# Prerequisites (verified before any mutation):
#   - kubectl (v1.23+) and jq on PATH
#   - kubeconfig pointing at a reachable cluster
#   - CRDs: rayclusters.ray.io, servicemonitors.monitoring.coreos.com,
#           verticalpodautoscalers.autoscaling.k8s.io,
#           prometheusrules.monitoring.coreos.com,
#           scaledobjects.keda.sh (only for carbon-aware KEDA scaling)
#   - Image pulls permitted from registry.k8s.io
#
# Usage:
#   ./deploy.sh --help
#   ./deploy.sh --context prod-us --dry-run
#   ./deploy.sh --enhancements --context dev
#   ./deploy.sh --delete --context dev
#
# Exit codes:
#   0  success (or dry-run)
#   1  prerequisite failure / apply failure / verification failure
#   2  invalid CLI arguments

set -euo pipefail

# ============================================================================
# 1. Constants and script directory
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/k8s"
ADAPTER_NAMESPACE="monitoring"

# ============================================================================
# 2. Colour support (opt-in, tty-only, respects NO_COLOR)
# ============================================================================

if [[ -t 2 ]] && [[ -z "${NO_COLOR:-}" ]] && [[ "${TERM:-}" != "dumb" ]]; then
    C_RESET=$'\033[0m'
    C_BOLD=$'\033[1m'
    C_RED=$'\033[31m'
    C_YELLOW=$'\033[33m'
    C_GREEN=$'\033[32m'
    C_CYAN=$'\033[36m'
else
    C_RESET=""
    C_BOLD=""
    C_RED=""
    C_YELLOW=""
    C_GREEN=""
    C_CYAN=""
fi

# ============================================================================
# 3. Logging helpers
# ============================================================================

log()   { printf '%s %s[%s]%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
          "${C_CYAN}" "info" "${C_RESET}" "$*"; }
warn()  { printf '%s %s[warn]%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
          "${C_YELLOW}" "${C_RESET}" "$*" >&2; }
error() { printf '%s %s[error]%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
          "${C_RED}" "${C_RESET}" "$*" >&2; }
ok()    { printf '%s %s[ok]%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
          "${C_GREEN}" "${C_RESET}" "$*"; }
die()   { error "$*"; exit 1; }

# ============================================================================
# 4. Usage
# ============================================================================

usage() {
    cat <<'EOF'
Usage: deploy.sh [OPTIONS]

Deploys the green-agent Kubernetes stack.

Options:
  --enhancements         Enable optional enhancement modules (RayCluster patch,
                         kustomize overlay, enhancement ConfigMap).
  --no-enhancements      Explicitly disable enhancements (default).
  --namespace NAME       Target namespace (default: green-agent).
  --context NAME         kubectl context to use (default: current context).
  --dry-run              Server-side dry-run every apply; no cluster mutations.
  --delete               Tear down all previously-applied resources.
  --wait-timeout SECS    Rollout wait timeout in seconds (default: 300).
  --yes                  Skip the confirmation prompt (CI use only).
  -h, --help             Show this message and exit.

Environment:
  NO_COLOR               Disable ANSI colour output.
  NAMESPACE              Alternative to --namespace.

Examples:
  ./deploy.sh --context staging --dry-run
  ./deploy.sh --context prod-us --yes
  ./deploy.sh --enhancements --context dev
  ./deploy.sh --delete --context dev

Exit codes:
  0 success (or dry-run)
  1 prerequisite failure / apply failure / verification failure
  2 invalid CLI arguments
EOF
}

# ============================================================================
# 5. Defaults and flag parsing
# ============================================================================

NAMESPACE="${NAMESPACE:-green-agent}"
CONTEXT=""
ENHANCEMENTS=false
DRY_RUN=false
DELETE=false
ASSUME_YES=false
WAIT_TIMEOUT=300

while [[ $# -gt 0 ]]; do
    case "$1" in
        --enhancements)     ENHANCEMENTS=true;  shift ;;
        --no-enhancements)  ENHANCEMENTS=false; shift ;;
        --namespace)
            [[ $# -ge 2 ]] || die "--namespace requires a value"
            NAMESPACE="$2"; shift 2 ;;
        --context)
            [[ $# -ge 2 ]] || die "--context requires a value"
            CONTEXT="$2"; shift 2 ;;
        --dry-run)          DRY_RUN=true;       shift ;;
        --delete)           DELETE=true;        shift ;;
        --yes|-y)           ASSUME_YES=true;    shift ;;
        --wait-timeout)
            [[ $# -ge 2 ]] || die "--wait-timeout requires a value"
            WAIT_TIMEOUT="$2"; shift 2 ;;
        -h|--help)          usage; exit 0 ;;
        *)                  error "Unknown flag: $1"; usage; exit 2 ;;
    esac
done

# Validate namespace format (RFC 1123 label).
[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    || die "Invalid namespace: $NAMESPACE (must match RFC 1123)"

# Validate timeout is a positive integer.
[[ "$WAIT_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
    || die "--wait-timeout must be a positive integer"

# ============================================================================
# 6. kubectl wrapper (single place to add --context)
# ============================================================================

KC=(kubectl)
if [[ -n "$CONTEXT" ]]; then
    KC+=(--context "$CONTEXT")
fi

# Apply with optional dry-run.
apply_file() {
    local file="$1" ns="${2:-$NAMESPACE}"
    local cmd=("${KC[@]}" apply -f "$file" -n "$ns")
    [[ "$DRY_RUN" == "true" ]] && cmd+=(--dry-run=server)
    log "apply -f $(basename "$file") -> $ns"
    "${cmd[@]}" >/dev/null || die "Failed to apply $file"
}

apply_kustomize() {
    local dir="$1" ns="${2:-$NAMESPACE}"
    local cmd=("${KC[@]}" apply -k "$dir" -n "$ns")
    [[ "$DRY_RUN" == "true" ]] && cmd+=(--dry-run=server)
    log "apply -k $(basename "$dir") -> $ns"
    "${cmd[@]}" >/dev/null || die "Failed to apply $dir"
}

# Fail-closed helper for required files.
require_file() {
    [[ -f "$1" ]] || die "Required file missing: $1"
}

# ============================================================================
# 7. Prerequisites
# ============================================================================

check_command() {
    command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

check_prerequisites() {
    log "Checking prerequisites..."

    check_command kubectl
    check_command jq

    # kubectl 1.23+ for autoscaling/v2.
    local kver
    kver="$("${KC[@]}" version --client -o json 2>/dev/null \
            | jq -r '.clientVersion.major + "." + .clientVersion.minor' \
            | tr -d '+')" || die "Cannot determine kubectl version"
    local kmin="${kver#*.}"
    [[ "$kmin" =~ ^[0-9]+$ ]] || kmin=0
    (( kmin >= 23 )) || die "kubectl >= 1.23 required (detected $kver)"

    # Cluster reachable.
    "${KC[@]}" cluster-info --request-timeout=5s >/dev/null 2>&1 \
        || die "Cannot reach cluster (context: ${CONTEXT:-current})"

    # CRDs — collect missing ones, report all at once.
    local missing=()
    for crd in \
        rayclusters.ray.io \
        servicemonitors.monitoring.coreos.com \
        verticalpodautoscalers.autoscaling.k8s.io \
        prometheusrules.monitoring.coreos.com; do
        "${KC[@]}" get crd "$crd" >/dev/null 2>&1 || missing+=("$crd")
    done

    # KEDA only needed if the carbon-aware ScaledObject is deployed.
    # Check the operator's presence non-fatally; the deployment will
    # fail visibly if it's referenced but unavailable.
    if ! "${KC[@]}" get crd scaledobjects.keda.sh >/dev/null 2>&1; then
        warn "CRD scaledobjects.keda.sh not found — KEDA-based"
        warn "carbon-aware scaling will not be applied."
    fi

    if (( ${#missing[@]} > 0 )); then
        die "Missing required CRDs: ${missing[*]}"
    fi

    ok "Prerequisites satisfied"
}

# ============================================================================
# 8. Confirmation
# ============================================================================

confirm_target() {
    local current_context
    current_context="$("${KC[@]}" config current-context 2>/dev/null \
        || echo "<none>")"

    log "Target cluster:   ${current_context}"
    log "Target namespace: ${NAMESPACE}"
    log "Enhancements:     ${ENHANCEMENTS}"
    log "Action:           $( $DELETE && echo delete || echo apply )"
    [[ "$DRY_RUN" == "true" ]] && log "Mode:             dry-run (no mutations)"

    # Exit early — nothing to confirm.
    if [[ "$DRY_RUN" == "true" ]] || [[ "$ASSUME_YES" == "true" ]]; then
        return 0
    fi

    read -rp "Proceed? [y/N] " yn
    case "$yn" in
        [yY]|[yY][eE][sS]) return 0 ;;
        *) log "Aborted by user"; exit 0 ;;
    esac
}

# ============================================================================
# 9. Core deployment
# ============================================================================

deploy_core() {
    log "Deploying core resources..."

    # 9.1 Namespace (idempotent)
    if [[ "$DRY_RUN" != "true" ]]; then
        "${KC[@]}" create namespace "$NAMESPACE" \
            --dry-run=client -o yaml \
            | "${KC[@]}" apply -f - >/dev/null
        ok "Namespace $NAMESPACE ready"
    else
        log "(dry-run) would ensure namespace $NAMESPACE"
    fi

    # 9.2 Cluster-scoped resources (PriorityClass)
    require_file "${K8S_DIR}/priorityclass.yaml"
    apply_file "${K8S_DIR}/priorityclass.yaml" "$NAMESPACE"

    # 9.3 Namespace-scoped resources
    for f in network-policy.yaml pdb.yaml quota.yaml; do
        require_file "${K8S_DIR}/${f}"
        apply_file "${K8S_DIR}/${f}"
    done

    for f in grafana-dashboard.yaml monitoring.yaml ray-cluster.yaml; do
        require_file "${K8S_DIR}/${f}"
        apply_file "${K8S_DIR}/${f}"
    done

    # 9.4 Autoscalers (HPA + VPA)
    require_file "${K8S_DIR}/carbon-autoscaler.yaml"
    apply_file "${K8S_DIR}/carbon-autoscaler.yaml"

    ok "Core resources deployed"
}

# ============================================================================
# 10. Prometheus adapter (deployed to monitoring namespace)
# ============================================================================

deploy_adapter() {
    log "Deploying Prometheus adapter to ${ADAPTER_NAMESPACE}..."

    require_file "${K8S_DIR}/green-agent-adapter-config.yaml"
    apply_file "${K8S_DIR}/green-agent-adapter-config.yaml" "$ADAPTER_NAMESPACE"

    require_file "${K8S_DIR}/prometheus-carbon-credit.yaml"
    apply_file "${K8S_DIR}/prometheus-carbon-credit.yaml" "$ADAPTER_NAMESPACE"

    require_file "${K8S_DIR}/adapter-deployment.yaml"
    apply_file "${K8S_DIR}/adapter-deployment.yaml" "$ADAPTER_NAMESPACE"

    # Wait for adapter to become Ready so HPA can immediately read metrics.
    if [[ "$DRY_RUN" != "true" ]]; then
        log "Waiting for adapter rollout..."
        "${KC[@]}" -n "$ADAPTER_NAMESPACE" rollout status \
            deployment/green-agent-prometheus-adapter \
            --timeout="${WAIT_TIMEOUT}s" >/dev/null \
            || die "Adapter rollout failed"
        ok "Adapter ready"
    fi
}

# ============================================================================
# 11. Enhancements (optional)
# ============================================================================

deploy_enhancements() {
    if [[ "$ENHANCEMENTS" != "true" ]]; then
        log "Skipping enhancements (pass --enhancements to enable)"
        return 0
    fi

    log "Deploying enhancement overlay..."

    # 11.1 Kustomize overlay
    [[ -d "${K8S_DIR}/enhancements" ]] \
        || die "k8s/enhancements missing; required for --enhancements"
    require_file "${K8S_DIR}/enhancements/kustomization.yaml"
    apply_kustomize "${K8S_DIR}/enhancements"

    # 11.2 RayCluster patch
    require_file "${K8S_DIR}/ray-cluster-enhancements-patch.yaml"
    log "Patching RayCluster with enhancement env vars"
    if [[ "$DRY_RUN" == "true" ]]; then
        "${KC[@]}" -n "$NAMESPACE" patch raycluster green-agent-cluster \
            --type merge \
            --patch "$(cat "${K8S_DIR}/ray-cluster-enhancements-patch.yaml")" \
            --dry-run=server >/dev/null
    else
        "${KC[@]}" -n "$NAMESPACE" patch raycluster green-agent-cluster \
            --type merge \
            --patch "$(cat "${K8S_DIR}/ray-cluster-enhancements-patch.yaml")" \
            >/dev/null
    fi
    ok "Enhancements deployed"
}

# ============================================================================
# 12. Verification (real checks)
# ============================================================================

verify_rollout() {
    local kind="$1" name="$2" ns="$3"
    if [[ "$DRY_RUN" == "true" ]]; then
        log "(dry-run) would wait for ${kind}/${name}"
        return 0
    fi
    log "Waiting for ${kind}/${name}..."
    "${KC[@]}" -n "$ns" rollout status "${kind}/${name}" \
        --timeout="${WAIT_TIMEOUT}s" >/dev/null \
        || die "${kind}/${name} rollout failed"
}

verify_hpa_metrics() {
    if [[ "$DRY_RUN" == "true" ]]; then
        log "(dry-run) would check HPA metrics"
        return 0
    fi

    local total unknown
    total=$("${KC[@]}" -n "$NAMESPACE" get hpa -o json 2>/dev/null \
        | jq '.items | length' || echo 0)

    if [[ "$total" -eq 0 ]]; then
        warn "No HPAs found in $NAMESPACE — skipping metric check"
        return 0
    fi

    # Count HPAs whose currentMetrics has at least one <unknown> external value.
    unknown=$("${KC[@]}" -n "$NAMESPACE" get hpa -o json 2>/dev/null \
        | jq '[.items[]
               | select(.status.currentMetrics != null)
               | .status.currentMetrics[]
               | select(.external.current.averageValue == "<unknown>")
              ] | length' || echo 0)

    if [[ "$unknown" -gt 0 ]]; then
        warn "$unknown HPA metric(s) are <unknown>"
        warn "Verify adapter is scraping Prometheus and exporters are running:"
        warn "  kubectl -n $ADAPTER_NAMESPACE logs deploy/green-agent-prometheus-adapter"
        warn "  kubectl get --raw /apis/external.metrics.k8s.io/v1beta2 | jq"
        return 1
    fi

    ok "All HPA metrics resolved"
}

verify_pods() {
    if [[ "$DRY_RUN" == "true" ]]; then
        return 0
    fi

    local not_ready
    not_ready=$("${KC[@]}" -n "$NAMESPACE" get pods -o json 2>/dev/null \
        | jq '[.items[]
               | select(
                   .status.phase != "Running"
                   and .status.phase != "Succeeded"
                 )] | length' || echo 0)

    if [[ "$not_ready" -gt 0 ]]; then
        warn "$not_ready pod(s) not Running in $NAMESPACE"
        "${KC[@]}" -n "$NAMESPACE" get pods
        return 1
    fi

    ok "All pods in $NAMESPACE are Running"
}

verify_enhancement_env() {
    if [[ "$ENHANCEMENTS" != "true" ]] || [[ "$DRY_RUN" == "true" ]]; then
        return 0
    fi

    local count
    count=$("${KC[@]}" -n "$NAMESPACE" get pods -o json 2>/dev/null \
        | jq '[.items[]
               | .spec.containers[]
               | .env[]?
               | select(.name == "ENHANCEMENTS_ENABLED" and .value == "true")
              ] | length' || echo 0)

    if [[ "$count" -eq 0 ]]; then
        warn "No pods have ENHANCEMENTS_ENABLED=true"
        warn "The RayCluster patch may not have applied. Check:"
        warn "  kubectl -n $NAMESPACE get raycluster green-agent-cluster -o yaml"
        return 1
    fi

    ok "$count container(s) have ENHANCEMENTS_ENABLED=true"
}

verify_deployment() {
    log "Verifying deployment..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log "(dry-run) skipping rollout and metric verification"
        return 0
    fi

    local failures=0

    # Rollouts — collect all failures so the operator sees every problem.
    local dc
    for dc in $( "${KC[@]}" -n "$NAMESPACE" get deploy -o name 2>/dev/null \
                 | sed 's|deployment.apps/||' ); do
        "${KC[@]}" -n "$NAMESPACE" rollout status "deployment/$dc" \
            --timeout="${WAIT_TIMEOUT}s" >/dev/null \
            || { warn "deployment/$dc not ready"; failures=$((failures + 1)); }
    done

    verify_hpa_metrics   || failures=$((failures + 1))
    verify_pods          || failures=$((failures + 1))
    verify_enhancement_env || failures=$((failures + 1))

    if (( failures > 0 )); then
        die "Verification failed with $failures issue(s)"
    fi

    ok "Verification passed"
}

# ============================================================================
# 13. Teardown
# ============================================================================

teardown() {
    log "Deleting resources from $NAMESPACE (and adapter from ${ADAPTER_NAMESPACE})..."

    local del_cmd=("${KC[@]}" delete)
    [[ "$DRY_RUN" == "true" ]] && del_cmd+=(--dry-run=server)

    # Reverse order of apply.
    if [[ -f "${K8S_DIR}/adapter-deployment.yaml" ]]; then
        "${del_cmd[@]}" -f "${K8S_DIR}/adapter-deployment.yaml" \
            -n "$ADAPTER_NAMESPACE" --ignore-not-found || true
    fi
    if [[ -f "${K8S_DIR}/prometheus-carbon-credit.yaml" ]]; then
        "${del_cmd[@]}" -f "${K8S_DIR}/prometheus-carbon-credit.yaml" \
            -n "$ADAPTER_NAMESPACE" --ignore-not-found || true
    fi
    if [[ -f "${K8S_DIR}/green-agent-adapter-config.yaml" ]]; then
        "${del_cmd[@]}" -f "${K8S_DIR}/green-agent-adapter-config.yaml" \
            -n "$ADAPTER_NAMESPACE" --ignore-not-found || true
    fi

    if [[ -d "${K8S_DIR}/enhancements" ]]; then
        "${del_cmd[@]}" -k "${K8S_DIR}/enhancements" \
            -n "$NAMESPACE" --ignore-not-found || true
    fi

    for f in carbon-autoscaler.yaml ray-cluster.yaml monitoring.yaml \
             grafana-dashboard.yaml quota.yaml pdb.yaml network-policy.yaml \
             priorityclass.yaml; do
        [[ -f "${K8S_DIR}/${f}" ]] || continue
        "${del_cmd[@]}" -f "${K8S_DIR}/${f}" \
            -n "$NAMESPACE" --ignore-not-found || true
    done

    # Namespace last (cascades remaining resources).
    "${del_cmd[@]}" namespace "$NAMESPACE" --ignore-not-found || true

    ok "Teardown complete"
}

# ============================================================================
# 14. Main
# ============================================================================

main() {
    log "Green Agent deployment starting"
    check_prerequisites
    confirm_target

    if [[ "$DELETE" == "true" ]]; then
        teardown
        exit 0
    fi

    deploy_core
    deploy_adapter
    deploy_enhancements
    verify_deployment

    ok "Deployment complete"
    cat <<EOF

Next steps:
  kubectl -n ${NAMESPACE} port-forward svc/green-agent-dashboard 8000:8000
  curl http://localhost:8000/health
  curl http://localhost:8000/ready

  kubectl -n ${NAMESPACE} port-forward svc/green-agent-metrics 9090:9090
  curl -s http://localhost:9090/metrics | grep green_metrics_composite_score
  curl -s http://localhost:9090/metrics | grep rlhf_overall_score
  curl -s http://localhost:9090/metrics | grep graph_registry_executions_total

  Verify HPA metrics:
    kubectl -n ${NAMESPACE} get hpa
    kubectl get --raw /apis/external.metrics.k8s.io/v1beta2 | jq

  To inspect the adapter:
    kubectl -n ${ADAPTER_NAMESPACE} logs deploy/green-agent-prometheus-adapter

  To remove everything:
    $0 --delete --context ${CONTEXT:-current}

EOF
}

main "$@"
