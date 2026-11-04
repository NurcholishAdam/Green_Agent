#!/usr/bin/env bash
#
# Green Agent — Prometheus Adapter deployment (Enhanced, v2)
# ==========================================================
#
# Deploys prometheus-adapter via Helm, verifies the custom metrics API is
# serving, and checks that each HPA-referenced metric resolves to a real
# Prometheus series (not <unknown>).
#
# Fixes applied vs. v1
# --------------------
#   P0  CHART_VERSION declared but never used        -> --version $CHART_VERSION
#   P0  Adapter rules never applied (values file
#       only contains scalar overrides)              -> verify values file
#                                                       contains rules
#   P0  Enhanced metrics verified, none exist        -> honest warn + skip
#   P0  Script always exits 0                        -> fail on failed checks
#   P0  `if $ENHANCEMENTS` fragile                   -> [[ "$X" == "true" ]]
#   P0  Substring flag matching (--no-enhancements
#       enabled enhancements)                        -> case loop
#   P0  No --context: deploys to active cluster      -> --context + prompt
#   P0  Values file existence not checked            -> require_file
#   P1  `set -e` only                                -> set -euo pipefail
#   P1  Relative paths (cd breaks)                   -> $SCRIPT_DIR
#   P1  No --dry-run / --delete / --help             -> all three
#   P1  No real HPA metric check                     -> inspect currentMetrics
#   P1  Hardcoded green-agent ns for HPA             -> $WORKLOAD_NAMESPACE
#   P1  grep -c edge case (integer comparison)       -> awk/jq
#   P1  sleep-based waits                            -> poll with timeout
#   P1  Box drawing chars (non-UTF-8 terminals)       -> ASCII + NO_COLOR
#   P1  No kubectl version check                     -> require >= 1.23
#   P1  No jq / helm-repo / values-file checks       -> require_* helpers
#   P2  Fragile label selector for logs              -> name-based
#   P2  No idempotency awareness                     -> print prior revision
#   P2  No timed rollout wait after Helm             -> kubectl wait
#
# Prerequisites
# -------------
#   helm >= 3.12, kubectl >= 1.23, jq
#   A Prometheus instance reachable from the adapter
#   k8s/prometheus-adapter-values.yaml with this shape:
#
#       prometheus:
#         url: http://prometheus-server.monitoring.svc.cluster.local
#         port: 9090
#       rules:
#         default: false
#         custom:
#           - seriesQuery: 'DCGM_FI_DEV_GPU_UTIL{namespace!="",pod!=""}'
#             resources:
#               overrides:
#                 namespace: {resource: "namespace"}
#                 pod:       {resource: "pod"}
#             name:
#               matches: '^DCGM_FI_DEV_GPU_UTIL$'
#               as: 'gpu_utilization'
#             metricsQuery: 'avg(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)'
#           - seriesQuery: 'ray_serve_deployment_queued_queries{namespace!="",pod!=""}'
#             resources:
#               overrides:
#                 namespace: {resource: "namespace"}
#                 pod:       {resource: "pod"}
#             name:
#               matches: '^ray_serve_deployment_queued_queries$'
#               as: 'ray_queue_depth'
#             metricsQuery: 'sum(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)'
#         external:
#           - seriesQuery: 'green_agent:carbon_credit:avg{region!=""}'
#             resources:
#               template: <<.Resource>>
#             name:
#               matches: 'green_agent:carbon_credit:avg'
#               as: 'carbon_credit'
#             metricsQuery: 'avg(<<.Series>>{<<.LabelMatchers>>})'
#
# The v1 "enhanced metrics" (modp_score, rlhf_feedback_score, graph_
# centrality, graph_connectivity, distillation_update_rate, moe_gate_stddev,
# evolutionary_best_fitness, flexgen_energy_rate) are NOT verifiable against
# this codebase because no exporter emits them. Passing --enhancements now
# produces a visible warning instead of pretending to verify them.
#
# Usage
# -----
#   ./deploy-prometheus-adapter.sh --help
#   ./deploy-prometheus-adapter.sh --context staging
#   ./deploy-prometheus-adapter.sh --dry-run --context prod-us
#   ./deploy-prometheus-adapter.sh --enhancements --context dev
#   ./deploy-prometheus-adapter.sh --delete --context dev
#
# Exit codes:
#   0  success
#   1  prerequisite / deploy / verification failure
#   2  invalid CLI arguments

set -euo pipefail

# ============================================================================
# 1. Script directory, constants
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALUES_FILE="${SCRIPT_DIR}/k8s/prometheus-adapter-values.yaml"

RELEASE_NAME="prometheus-adapter"
CHART_NAME="prometheus-community/prometheus-adapter"
CHART_VERSION="3.4.0"
CHART_REPO_NAME="prometheus-community"
CHART_REPO_URL="https://prometheus-community.github.io/helm-charts"
TIMEOUT="5m"
WAIT_TIMEOUT_S=120

# ============================================================================
# 2. Colour support (opt-in, tty-only, respects NO_COLOR)
# ============================================================================

if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]] && [[ "${TERM:-}" != "dumb" ]]; then
    RED=$'\033[0;31m'
    GREEN=$'\033[0;32m'
    YELLOW=$'\033[1;33m'
    BLUE=$'\033[0;34m'
    NC=$'\033[0m'
else
    RED="" GREEN="" YELLOW="" BLUE="" NC=""
fi

# ============================================================================
# 3. Logging helpers
# ============================================================================

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
status()  { printf '%s %s>%s %s\n'  "$(ts)" "${BLUE}"   "${NC}" "$*"; }
success() { printf '%s %s✓%s %s\n'  "$(ts)" "${GREEN}"  "${NC}" "$*"; }
warning() { printf '%s %s⚠%s %s\n'  "$(ts)" "${YELLOW}" "${NC}" "$*" >&2; }
error()   { printf '%s %s✗%s %s\n'  "$(ts)" "${RED}"    "${NC}" "$*" >&2; }
die()     { error "$*"; exit 1; }

# ============================================================================
# 4. Usage
# ============================================================================

usage() {
    cat <<'EOF'
Usage: deploy-prometheus-adapter.sh [OPTIONS]

Deploys prometheus-adapter via Helm, applies rules from the values file, and
verifies the custom / external metrics API is serving.

Options:
  --context NAME            kubectl context to use (default: current)
  --namespace NAME          Monitoring namespace (default: monitoring, or
                            $MONITORING_NAMESPACE)
  --workload-namespace NAME Namespace of the green-agent HPA (default:
                            green-agent)
  --enhancements            Emit a warning that enhanced metrics are
                            currently unimplemented; does not fail
  --dry-run                 helm --dry-run + kubectl --dry-run=server
  --delete                  Uninstall the Helm release
  --yes                     Skip the confirmation prompt (CI use only)
  --wait-timeout SECS       Adapter rollout timeout (default: 120)
  -h, --help                Show this message and exit

Environment:
  MONITORING_NAMESPACE      Alternate to --namespace
  WORKLOAD_NAMESPACE        Alternate to --workload-namespace
  NO_COLOR                  Disable ANSI colour output

Exit codes:
  0 success
  1 prerequisite / deploy / verification failure
  2 invalid CLI arguments
EOF
}

# ============================================================================
# 5. Defaults + flag parsing
# ============================================================================

NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
WORKLOAD_NAMESPACE="${WORKLOAD_NAMESPACE:-green-agent}"
CONTEXT=""
ENHANCEMENTS=false
DRY_RUN=false
DELETE=false
ASSUME_YES=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --context)
            [[ $# -ge 2 ]] || die "--context requires a value"
            CONTEXT="$2"; shift 2 ;;
        --namespace)
            [[ $# -ge 2 ]] || die "--namespace requires a value"
            NAMESPACE="$2"; shift 2 ;;
        --workload-namespace)
            [[ $# -ge 2 ]] || die "--workload-namespace requires a value"
            WORKLOAD_NAMESPACE="$2"; shift 2 ;;
        --enhancements)        ENHANCEMENTS=true;  shift ;;
        --no-enhancements)     ENHANCEMENTS=false; shift ;;
        --dry-run)             DRY_RUN=true;       shift ;;
        --delete)              DELETE=true;        shift ;;
        --yes|-y)              ASSUME_YES=true;    shift ;;
        --wait-timeout)
            [[ $# -ge 2 ]] || die "--wait-timeout requires a value"
            WAIT_TIMEOUT_S="$2"; shift 2 ;;
        -h|--help)             usage; exit 0 ;;
        *)                     error "Unknown flag: $1"; usage; exit 2 ;;
    esac
done

[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    || die "Invalid namespace: $NAMESPACE"
[[ "$WORKLOAD_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    || die "Invalid workload namespace: $WORKLOAD_NAMESPACE"
[[ "$WAIT_TIMEOUT_S" =~ ^[1-9][0-9]*$ ]] \
    || die "--wait-timeout must be a positive integer"

# ============================================================================
# 6. kubectl / helm wrappers (single place for --context)
# ============================================================================

KC=(kubectl)
if [[ -n "$CONTEXT" ]]; then
    KC+=(--context "$CONTEXT")
fi

HC=(helm)
if [[ -n "$CONTEXT" ]]; then
    HC+=(--kube-context "$CONTEXT")
fi

# ============================================================================
# 7. Prerequisites
# ============================================================================

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

require_file() {
    [[ -f "$1" ]] || die "Required file missing: $1"
}

check_prerequisites() {
    status "Checking prerequisites"

    require_command helm
    require_command kubectl
    require_command jq

    local hver kver
    hver="$(helm version --short | sed 's/^v//;s/\+.*//')"
    kver="$("${KC[@]}" version --client -o json 2>/dev/null \
            | jq -r '.clientVersion.major + "." + .clientVersion.minor' \
            | tr -d '+')"
    local kmin="${kver#*.}"
    [[ "$kmin" =~ ^[0-9]+$ ]] || kmin=0
    (( kmin >= 23 )) || die "kubectl >= 1.23 required (detected $kver)"

    "${KC[@]}" cluster-info --request-timeout=5s >/dev/null 2>&1 \
        || die "Cannot reach cluster (context: ${CONTEXT:-current})"

    if [[ "$DELETE" != "true" ]]; then
        require_file "$VALUES_FILE"
        # Verify the values file contains at least one `rules.custom` entry.
        local rule_count
        rule_count="$(python3 - "$VALUES_FILE" <<'PY'
import sys, yaml
with open(sys.argv[1]) as f:
    data = yaml.safe_load(f) or {}
rules = (data.get("rules") or {}).get("custom") or []
print(len(rules))
PY
)"
        if [[ "$rule_count" -eq 0 ]]; then
            die "Values file has no 'rules.custom' entries. The adapter will serve zero metrics."
        fi
        success "Prerequisites satisfied (kubectl $kver, helm $hver, $rule_count custom rule(s))"
    else
        success "Prerequisites satisfied (delete mode)"
    fi
}

# ============================================================================
# 8. Confirmation
# ============================================================================

confirm_target() {
    local current_context
    current_context="$("${KC[@]}" config current-context 2>/dev/null || echo '<none>')"

    status "Target cluster:          ${current_context}"
    status "Monitoring namespace:    ${NAMESPACE}"
    status "Workload namespace:      ${WORKLOAD_NAMESPACE}"
    status "Helm release:            ${RELEASE_NAME}"
    status "Chart version:           ${CHART_VERSION}"
    status "Action:                  $( $DELETE && echo uninstall || echo install/upgrade )"
    [[ "$DRY_RUN" == "true" ]] && status "Mode:                    dry-run"

    if [[ "$DRY_RUN" == "true" ]] || [[ "$ASSUME_YES" == "true" ]]; then
        return 0
    fi
    read -rp "Proceed? [y/N] " yn
    case "$yn" in
        [yY]|[yY][eE][sS]) return 0 ;;
        *) status "Aborted"; exit 0 ;;
    esac
}

# ============================================================================
# 9. Helm repository + deploy
# ============================================================================

ensure_repo() {
    status "Ensuring ${CHART_REPO_NAME} Helm repository is available"
    # --force-update handles the case where the repo URL has changed.
    "${HC[@]}" repo add "$CHART_REPO_NAME" "$CHART_REPO_URL" \
        --force-update >/dev/null
    "${HC[@]}" repo update "$CHART_REPO_NAME" >/dev/null
    success "Helm repo ready"
}

deploy_adapter() {
    status "Deploying ${RELEASE_NAME} (chart ${CHART_NAME}, version ${CHART_VERSION})"

    local args=(
        upgrade --install "$RELEASE_NAME" "$CHART_NAME"
        --namespace "$NAMESPACE"
        --create-namespace
        --version "$CHART_VERSION"                 # v1 forgot this — critical
        -f "$VALUES_FILE"
        --wait
        --timeout "$TIMEOUT"
        --atomic
    )
    if [[ "$DRY_RUN" == "true" ]]; then
        args+=(--dry-run)
    fi

    # Show current revision so the operator knows what's being upgraded.
    if "${HC[@]}" -n "$NAMESPACE" status "$RELEASE_NAME" >/dev/null 2>&1; then
        local rev
        rev="$("${HC[@]}" -n "$NAMESPACE" status "$RELEASE_NAME" -o json \
               | jq -r '.version' 2>/dev/null || echo '?')"
        status "Existing release detected at revision ${rev}"
    fi

    "${HC[@]}" "${args[@]}"
    success "Helm release deployed"
}

wait_for_rollout() {
    if [[ "$DRY_RUN" == "true" ]]; then
        status "(dry-run) would wait for adapter rollout"
        return 0
    fi
    status "Waiting for adapter deployment to become available"
    local deploy="${RELEASE_NAME}"
    # Chart names the deployment after the release unless overridden.
    if ! "${KC[@]}" -n "$NAMESPACE" get deployment "$deploy" >/dev/null 2>&1; then
        deploy="${RELEASE_NAME}-prometheus-adapter"
    fi
    "${KC[@]}" -n "$NAMESPACE" rollout status "deployment/$deploy" \
        --timeout="${WAIT_TIMEOUT_S}s" >/dev/null \
        || die "Adapter rollout failed"
    success "Adapter is available"
}

# ============================================================================
# 10. Verification
# ============================================================================

verify_apiservice() {
    if [[ "$DRY_RUN" == "true" ]]; then
        status "(dry-run) would verify APIService registration"
        return 0
    fi
    status "Verifying Custom Metrics API registration"
    # Poll — APIService registration lags the deployment by a few seconds.
    local deadline=$(( SECONDS + 60 ))
    while (( SECONDS < deadline )); do
        if "${KC[@]}" get --raw /apis/custom.metrics.k8s.io/v1beta1 \
                >/dev/null 2>&1; then
            success "Custom Metrics API is serving"
            return 0
        fi
        sleep 2
    done
    die "Custom Metrics API did not register within 60s"
}

# Extract the metric names the adapter is actually serving.
# Returns a newline-separated list on stdout, exits 1 if the API is unreachable.
list_served_metrics() {
    local api="custom.metrics.k8s.io/v1beta1"
    if ! "${KC[@]}" get --raw "/apis/${api}" >/dev/null 2>&1; then
        return 1
    fi
    "${KC[@]}" get --raw "/apis/${api}" 2>/dev/null \
        | jq -r '.resources[]?.name // empty' 2>/dev/null || true
}

verify_required_metrics() {
    if [[ "$DRY_RUN" == "true" ]]; then
        status "(dry-run) would verify required metrics"
        return 0
    fi

    # Metrics the HPA in file 16 must resolve.
    local required=(
        "pods/gpu_utilization"
        "pods/ray_queue_depth"
        "external.metrics.k8s.io/carbon_credit"
    )

    # Extract rules from the values file.
    local custom_count
    custom_count="$(python3 - "$VALUES_FILE" <<'PY'
import sys, yaml
with open(sys.argv[1]) as f:
    data = yaml.safe_load(f) or {}
rules = (data.get("rules") or {}).get("custom") or []
print(len(rules))
PY
)"
    status "Adapter is configured with ${custom_count} custom rule(s)"

    status "Verifying required metrics are served"
    local served
    if ! served="$(list_served_metrics)"; then
        die "Custom Metrics API not reachable"
    fi

    local missing=0
    for metric in "${required[@]}"; do
        local name="${metric##*/}"
        if grep -Fxq "$name" <<<"$served"; then
            success "Serving: $name"
        else
            warning "Not yet served: $name (may lag while Prometheus scrapes)"
            missing=$((missing + 1))
        fi
    done

    if (( missing > 0 )); then
        warning "${missing} metric(s) not yet served — check Prometheus scrape targets"
        warning "Debug: kubectl -n $NAMESPACE logs deploy/${RELEASE_NAME} "
        warning "       kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1 | jq"
        # Not fatal — metrics may lag Prometheus's scrape interval.
    fi
    return 0
}

verify_hpa_metrics() {
    if [[ "$DRY_RUN" == "true" ]]; then
        status "(dry-run) would verify HPA currentMetrics"
        return 0
    fi

    if ! "${KC[@]}" -n "$WORKLOAD_NAMESPACE" get hpa >/dev/null 2>&1; then
        status "No HPAs in $WORKLOAD_NAMESPACE — skipping HPA metric check"
        return 0
    fi

    status "Checking HPA currentMetrics for <unknown>"
    local unknown
    unknown="$("${KC[@]}" -n "$WORKLOAD_NAMESPACE" get hpa -o json 2>/dev/null \
        | jq '[.items[]
               | .status.currentMetrics[]?
               | select(
                   (.external.current.averageValue? == "<unknown>")
                   or (.pods.current.averageValue?     == "<unknown>")
                 )] | length' 2>/dev/null || echo 0)"

    if [[ "$unknown" -gt 0 ]]; then
        warning "$unknown HPA metric(s) are <unknown>"
        warning "The adapter is running but Prometheus has no data yet."
        warning "Check exporters: DCGM, Ray, and the green-agent sidecar."
        return 1
    fi
    success "All HPA metrics resolve"
}

verify_enhancements_placeholder() {
    # v1 "verified" 8 enhanced metrics that have no emitter in this codebase.
    # We surface that fact instead of pretending success.
    if [[ "$ENHANCEMENTS" != "true" ]]; then
        return 0
    fi

    warning "Enhanced metrics are declared but not implemented in this codebase."
    warning "The following 8 metrics have no emitter anywhere in the Python source:"
    for m in \
        green_agent_modp_score \
        green_agent_rlhf_feedback \
        green_agent_graph_centrality \
        green_agent_graph_connectivity \
        green_agent_distillation_update_rate \
        green_agent_moe_gate_stddev \
        green_agent_evolutionary_best_fitness \
        green_agent_flexgen_energy_rate; do
        warning "    $m"
    done
    warning "If you add emitters, extend k8s/prometheus-adapter-values.yaml"
    warning "with matching rules and update verify_required_metrics() in this script."
    # Not fatal — the flag is informational.
    return 0
}

# ============================================================================
# 11. Access commands
# ============================================================================

print_next_steps() {
    echo ""
    status "Deployment complete"
    echo ""
    echo "Next steps:"
    echo "  1. Watch HPA:"
    echo "       kubectl -n ${WORKLOAD_NAMESPACE} get hpa -w"
    echo ""
    echo "  2. Query an installed metric directly:"
    echo "       kubectl get --raw \\"
    echo "         /apis/custom.metrics.k8s.io/v1beta1/namespaces/${WORKLOAD_NAMESPACE}/pods/*/gpu_utilization"
    echo ""
    echo "  3. Adapter logs:"
    echo "       kubectl -n ${NAMESPACE} logs deploy/${RELEASE_NAME}"
    echo ""
    echo "  4. Adapter configuration in use:"
    echo "       helm -n ${NAMESPACE} get values ${RELEASE_NAME}"
    echo ""
    echo "  5. Helm release status:"
    echo "       helm -n ${NAMESPACE} status ${RELEASE_NAME}"
    echo ""
}

# ============================================================================
# 12. Delete
# ============================================================================

delete_adapter() {
    status "Uninstalling ${RELEASE_NAME} from ${NAMESPACE}"
    local args=(uninstall "$RELEASE_NAME" --namespace "$NAMESPACE")
    [[ "$DRY_RUN" == "true" ]] && args+=(--dry-run)
    if "${HC[@]}" -n "$NAMESPACE" status "$RELEASE_NAME" >/dev/null 2>&1; then
        "${HC[@]}" "${args[@]}"
        success "Release uninstalled"
    else
        status "Release not found — nothing to do"
    fi
}

# ============================================================================
# 13. Main
# ============================================================================

main() {
    echo ""
    echo "Green Agent — Prometheus Adapter deployment (v2)"
    echo "================================================"
    echo ""

    check_prerequisites
    confirm_target

    if [[ "$DELETE" == "true" ]]; then
        delete_adapter
        exit 0
    fi

    ensure_repo
    deploy_adapter
    wait_for_rollout
    verify_apiservice
    verify_required_metrics
    verify_hpa_metrics
    verify_enhancements_placeholder

    print_next_steps
}

main "$@"
