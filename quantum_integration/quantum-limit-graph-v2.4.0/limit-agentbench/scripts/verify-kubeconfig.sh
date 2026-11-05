#!/usr/bin/env bash
# =============================================================================
# verify-kubeconfig.sh — Pre-flight check for a kubeconfig intended for
# GitHub Actions deployment of the Green Agent.
# =============================================================================
# ---- Fixes vs v1 -----------------------------------------------------------
#   FIX #1   Removed "print first 200 base64 chars" — credentials are never
#            echoed. A SHA-256 digest of the encoded blob is printed instead.
#   FIX #2   --expected-server / --expected-context verify "right cluster?".
#   FIX #3   gitignore + git-status checks refuse to proceed if the kubeconfig
#            would be committed.
#   FIX #4   Base64 round-trip uses cmp -s on temp files (portable).
#   FIX #5   Cross-platform base64 / stat / sha256 (GNU + macOS).
#   FIX #6   Node count distinguishes "0 nodes" from "kubectl failed".
#   FIX #7   Memory sum handles Ki/Mi/Gi/Ti/K/M/G; CPU handles millicores.
#   FIX #9   Warns when file mode is not 600/400.
#   FIX #11  Warns on system:masters / cluster-admin binding.
#   FIX #13  --strict turns warnings into failures.
#   FIX #15  --kubeconfig <path> override.
#   FIX #20  umask 077 + trap cleanup for temp files.
#   FIX #22  Next-steps message says "add KUBE_CONFIG to each GitHub
#            environment" (not "the repo setting").
#   FIX #29  --json for tooling integration (with proper escaping).
#   FIX #34  --check-rbac runs kubectl auth can-i for deploy verbs.
# =============================================================================

set -euo pipefail
umask 077

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
KUBECONFIG_PATH="./kubeconfig-github"
ENHANCEMENTS=false
STRICT=false
CHECK_RBAC=false
JSON=false
QUIET=false
EXPECTED_SERVER=""
EXPECTED_CONTEXT=""
TARGET_NAMESPACE=""

# ---------------------------------------------------------------------------
# State + traps
# ---------------------------------------------------------------------------
TEMP_FILES=()
WARNINGS=0
ERRORS=0
CHECKS=()       # each entry: "status|name|message"

_cleanup() {
    local f
    if [[ ${#TEMP_FILES[@]} -gt 0 ]]; then
        for f in "${TEMP_FILES[@]}"; do
            [[ -n "${f:-}" && -e "$f" ]] && rm -f "$f" || true
        done
    fi
}
trap _cleanup EXIT

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'
    BLUE=$'\033[0;34m'; NC=$'\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
fi
if [[ "${JSON:-false}" == true ]]; then RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''; fi

record() {  # $1 = status (ok|warn|fail), $2 = name, $3 = message
    local status="$1" name="$2" msg="$3"
    CHECKS+=("${status}|${name}|${msg}")
    case "$status" in
        ok)   [[ "$QUIET" == true ]] || printf '%s\n' "${GREEN}✅${NC} ${msg}" ;;
        warn) WARNINGS=$((WARNINGS + 1)); printf '%s\n' "${YELLOW}⚠️ ${NC} ${msg}" >&2 ;;
        fail) ERRORS=$((ERRORS + 1));     printf '%s\n' "${RED}❌${NC} ${msg}" >&2 ;;
    esac
}

warn_or_fail() {  # $1 = name, $2 = message — promote warnings under --strict
    if [[ "$STRICT" == true ]]; then
        record fail "$1" "$2"
    else
        record warn "$1" "$2"
    fi
}

# ---------------------------------------------------------------------------
# JSON emitter — declared before any use, escapes newlines/quotes/backslashes
# ---------------------------------------------------------------------------
json_escape() {
    # Escape a shell string for inclusion in a JSON string literal.
    local s="$1"
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/\\n}
    s=${s//$'\r'/\\r}
    s=${s//$'\t'/\\t}
    printf '%s' "$s"
}

emit_json() {
    local overall="ok"
    if [[ $ERRORS -gt 0 ]]; then
        overall="fail"
    elif [[ $WARNINGS -gt 0 ]]; then
        overall="warn"
    fi

    printf '{"status":"%s","warnings":%d,"errors":%d,"checks":[' \
        "$overall" "$WARNINGS" "$ERRORS"

    local first=true entry status name msg
    if [[ ${#CHECKS[@]} -gt 0 ]]; then
        for entry in "${CHECKS[@]}"; do
            IFS='|' read -r status name msg <<<"$entry"
            if [[ "$first" == true ]]; then first=false; else printf ','; fi
            printf '{"status":"%s","name":"%s","message":"%s"}' \
                "$(json_escape "$status")" \
                "$(json_escape "$name")" \
                "$(json_escape "$msg")"
        done
    fi

    printf ']}\n'
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
    cat <<'EOF'
Usage: verify-kubeconfig.sh [OPTIONS]

Options:
  --kubeconfig <path>       Path to kubeconfig (default: ./kubeconfig-github)
  --enhancements            Run enhanced capability checks (LIMIT Graph,
                            MODP, RLHF, Distillation, Bio-inspired, MoE)
  --check-rbac              Run kubectl auth can-i for deploy verbs
  --expected-server <url>   Fail if the server URL does not match
  --expected-context <name> Fail if the current-context does not match
  --namespace <ns>          Only check this namespace (default: all four)
  --strict                  Treat warnings as failures
  --json                    Machine-readable output
  --quiet                   Errors only
  -h, --help                Show this help
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --kubeconfig)       KUBECONFIG_PATH="${2:?--kubeconfig requires a value}"; shift ;;
        --enhancements)     ENHANCEMENTS=true ;;
        --check-rbac)       CHECK_RBAC=true ;;
        --strict)           STRICT=true ;;
        --json)             JSON=true ;;
        --quiet)            QUIET=true ;;
        --expected-server)  EXPECTED_SERVER="${2:?--expected-server requires a value}"; shift ;;
        --expected-context) EXPECTED_CONTEXT="${2:?--expected-context requires a value}"; shift ;;
        --namespace)        TARGET_NAMESPACE="${2:?--namespace requires a value}"; shift ;;
        -h|--help)          usage; exit 0 ;;
        *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

# Re-evaluate color suppression after JSON/QUIET are known
if [[ "$JSON" == true || "$QUIET" == true ]]; then
    RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
fi

if [[ "$QUIET" == false && "$JSON" == false ]]; then
    printf '%s\n' "${BLUE}🔍 Green Agent — kubeconfig verification${NC}"
    [[ "$ENHANCEMENTS" == true ]] && \
        printf '%s\n' "${BLUE}   enhanced capability checks: ON${NC}"
    [[ -n "$EXPECTED_SERVER" ]] && \
        printf '%s\n' "${BLUE}   expected server: ${EXPECTED_SERVER}${NC}"
    [[ -n "$EXPECTED_CONTEXT" ]] && \
        printf '%s\n' "${BLUE}   expected context: ${EXPECTED_CONTEXT}${NC}"
    printf '\n'
fi

# ---------------------------------------------------------------------------
# Portable helpers
# ---------------------------------------------------------------------------
have() { command -v "$1" >/dev/null 2>&1; }

b64_encode() {
    if base64 --help 2>&1 | grep -q -- '-w'; then
        base64 -w0 "$1"
    else
        base64 -i "$1"
    fi
}

b64_decode_file() {  # $1 = encoded file, stdout = decoded
    if base64 --help 2>&1 | grep -q -- '--decode'; then
        base64 --decode < "$1"
    elif base64 --help 2>&1 | grep -q -- '-D'; then
        base64 -D < "$1"
    else
        base64 -d < "$1"
    fi
}

file_mode() {  # prints octal like "600"
    if stat -c '%a' "$1" >/dev/null 2>&1; then
        stat -c '%a' "$1"
    else
        stat -f '%Lp' "$1"
    fi
}

sha256_file() {
    if have sha256sum; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

k() {
    kubectl --kubeconfig="$KUBECONFIG_PATH" --request-timeout=15s "$@"
}

# ---------------------------------------------------------------------------
# 0. Required binaries
# ---------------------------------------------------------------------------
for bin in kubectl base64 awk sed; do
    if ! have "$bin"; then
        record fail "deps.${bin}" "Required binary not found: ${bin}"
    fi
done

# ---------------------------------------------------------------------------
# 1. File presence + permissions
# ---------------------------------------------------------------------------
if [[ ! -f "$KUBECONFIG_PATH" ]]; then
    record fail "file.exists" "Missing kubeconfig: ${KUBECONFIG_PATH}"
    record fail "file.hint"   "Generate with: kubectl config view --raw > ${KUBECONFIG_PATH}"
elif [[ $ERRORS -eq 0 ]]; then
    record ok "file.exists" "Kubeconfig found: ${KUBECONFIG_PATH}"

    MODE=$(file_mode "$KUBECONFIG_PATH")
    if [[ "$MODE" != "600" && "$MODE" != "400" ]]; then
        warn_or_fail "file.mode" \
            "Kubeconfig mode is ${MODE}; recommend 600 (chmod 600 ${KUBECONFIG_PATH})"
    fi
fi

# ---------------------------------------------------------------------------
# 2. Structure — reject placeholders
# ---------------------------------------------------------------------------
if [[ $ERRORS -eq 0 ]]; then
    if ! grep -qE '^clusters:' "$KUBECONFIG_PATH" \
       || ! grep -qE '^users:'    "$KUBECONFIG_PATH" \
       || ! grep -qE 'server:'    "$KUBECONFIG_PATH"; then
        record fail "struct.fields" \
            "Kubeconfig missing required top-level fields (clusters/users/server)"
    else
        record ok "struct.fields" "Kubeconfig has required top-level fields"
    fi

    if grep -qE 'server:[[:space:]]*(CHANGE_ME|<[^>]+>|""|)$' "$KUBECONFIG_PATH"; then
        record fail "struct.placeholder" \
            "Kubeconfig contains a placeholder server URL"
    fi
fi

# ---------------------------------------------------------------------------
# 3. Git safety — do not proceed if the file would be committed
# ---------------------------------------------------------------------------
if [[ $ERRORS -eq 0 ]] && have git && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    if git check-ignore -q -- "$KUBECONFIG_PATH" 2>/dev/null; then
        record ok "git.ignored" "${KUBECONFIG_PATH} is gitignored"
    else
        record fail "git.ignored" \
            "${KUBECONFIG_PATH} is NOT gitignored. Add it before any 'git add': echo '${KUBECONFIG_PATH}' >> .gitignore"
    fi
    if git status --porcelain -- "$KUBECONFIG_PATH" 2>/dev/null | grep -q .; then
        record fail "git.status" \
            "${KUBECONFIG_PATH} appears in git status — do NOT commit it"
    fi
fi

# ---------------------------------------------------------------------------
# 4. Base64 round-trip — portable, cmp-based
# ---------------------------------------------------------------------------
if [[ $ERRORS -eq 0 ]]; then
    TMP_ENC=$(mktemp); TEMP_FILES+=("$TMP_ENC")
    TMP_DEC=$(mktemp); TEMP_FILES+=("$TMP_DEC")

    b64_encode "$KUBECONFIG_PATH" > "$TMP_ENC"
    b64_decode_file "$TMP_ENC" > "$TMP_DEC"

    if cmp -s -- "$KUBECONFIG_PATH" "$TMP_DEC"; then
        record ok "b64.roundtrip" "Base64 encoding is lossless"
    else
        record fail "b64.roundtrip" \
            "Base64 round-trip mismatch — file may contain CRLF or binary bytes"
    fi

    # GitHub's secret → file path: `echo "$secret" | base64 -d`
    # Simulate by wrapping with a newline, as the runner does.
    TMP_GHA=$(mktemp); TEMP_FILES+=("$TMP_GHA")
    { printf '%s\n' "$(cat "$TMP_ENC")"; } | b64_decode_file /dev/stdin > "$TMP_GHA" 2>/dev/null || true
    if cmp -s -- "$KUBECONFIG_PATH" "$TMP_GHA"; then
        record ok "b64.gha" "GitHub-style 'base64 -d' recovers the original"
    else
        warn_or_fail "b64.gha" \
            "GitHub-style 'base64 -d' did not recover the original (check line wrapping)"
    fi

    record ok "b64.digest" "Encoded SHA-256: $(sha256_file "$KUBECONFIG_PATH")"
fi

# ---------------------------------------------------------------------------
# 5. Connectivity + identity
# ---------------------------------------------------------------------------
if [[ $ERRORS -eq 0 ]]; then
    if ! k cluster-info >/dev/null 2>&1; then
        record fail "conn.reach" \
            "kubectl cannot reach the cluster with ${KUBECONFIG_PATH}"
    else
        record ok "conn.reach" "kubectl can reach the cluster"

        SERVER=$(k config view -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)
        CONTEXT=$(k config current-context 2>/dev/null || true)
        record ok "conn.server"  "Server:  ${SERVER:-<unknown>}"
        record ok "conn.context" "Context: ${CONTEXT:-<unknown>}"

        if [[ -n "$EXPECTED_SERVER" && "$SERVER" != "$EXPECTED_SERVER" ]]; then
            record fail "conn.expected_server" \
                "Server '${SERVER}' != expected '${EXPECTED_SERVER}'"
        fi
        if [[ -n "$EXPECTED_CONTEXT" && "$CONTEXT" != "$EXPECTED_CONTEXT" ]]; then
            record fail "conn.expected_context" \
                "Context '${CONTEXT}' != expected '${EXPECTED_CONTEXT}'"
        fi
    fi

    # Exec-plugin credentials (OIDC / IRSA / gcloud) may be short-lived
    if grep -qE '^[[:space:]]*exec:' "$KUBECONFIG_PATH"; then
        warn_or_fail "auth.exec_plugin" \
            "Kubeconfig uses an exec credential plugin (OIDC/IRSA/etc.). Encoded secrets may expire — verify in CI."
    fi

    # cluster-admin / system:masters bindings are the most common leak vector
    if grep -qE 'system:masters|cluster-admin' "$KUBECONFIG_PATH"; then
        warn_or_fail "auth.overpowered" \
            "Kubeconfig appears to grant cluster-admin / system:masters. Generate a least-privilege kubeconfig for CI."
    fi
fi

# ---------------------------------------------------------------------------
# 6. Enhanced capability checks
# ---------------------------------------------------------------------------
if [[ $ERRORS -eq 0 && "$ENHANCEMENTS" == true ]]; then
    record ok "enh.begin" "Running enhanced capability checks"

    # --- Node count (0 nodes and kubectl failure are distinguished) ---
    NODE_COUNT=0
    if NODES_JSON=$(k get nodes -o json 2>/tmp/nodes_err.$$); then
        NODE_COUNT=$(printf '%s' "$NODES_JSON" | grep -c '"kind": *"Node"' || true)
        [[ -z "$NODE_COUNT" ]] && NODE_COUNT=0
        record ok "enh.nodes" "Node count: ${NODE_COUNT}"
        if [[ "$NODE_COUNT" -lt 2 ]]; then
            warn_or_fail "enh.nodes" \
                "Fewer than 2 nodes; distributed RL / MoE / bio-inspired will be limited"
        fi
    else
        record fail "enh.nodes" \
            "Cannot list nodes: $(head -1 /tmp/nodes_err.$$ 2>/dev/null || echo 'unknown error')"
    fi
    rm -f /tmp/nodes_err.$$ || true

    # --- Allocatable CPU / memory (millicores + Ki/Mi/Gi/Ti/K/M/G) ---
    if [[ "$NODE_COUNT" -gt 0 ]]; then
        CPUS=$(k get nodes -o jsonpath='{range .items[*]}{.status.allocatable.cpu}{"\n"}{end}' 2>/dev/null \
            | awk '
                { v=$1
                  if (v ~ /m$/) { sub(/m$/,"",v); sum += v/1000 }
                  else          { sum += v }
                }
                END { printf "%.2f", sum+0 }')
        MEM_MI=$(k get nodes -o jsonpath='{range .items[*]}{.status.allocatable.memory}{"\n"}{end}' 2>/dev/null \
            | awk '
                { v=$1; mult=1
                  if      (v ~ /Ki$/) { sub(/Ki$/,"",v); mult=1/1024 }
                  else if (v ~ /Mi$/) { sub(/Mi$/,"",v); mult=1 }
                  else if (v ~ /Gi$/) { sub(/Gi$/,"",v); mult=1024 }
                  else if (v ~ /Ti$/) { sub(/Ti$/,"",v); mult=1024*1024 }
                  else if (v ~ /K$/)  { sub(/K$/,"",v);  mult=1/1024/1000 }
                  else if (v ~ /M$/)  { sub(/M$/,"",v);  mult=1/1000/1000 }
                  else if (v ~ /G$/)  { sub(/G$/,"",v);  mult=1/1000 }
                  sum += v*mult
                }
                END { printf "%.0f", sum+0 }')
        record ok "enh.cpu" "Allocatable CPU:    ${CPUS} cores"
        record ok "enh.mem" "Allocatable memory: ${MEM_MI} Mi"
        if awk -v c="$CPUS" 'BEGIN{exit !(c+0<4)}'; then
            warn_or_fail "enh.cpu" "Low CPU (<4 cores) may limit MODP / distillation training"
        fi
    fi

    # --- Prometheus Operator CRD (works for scoped SAs) ---
    if k api-resources --api-group=monitoring.coreos.com 2>/dev/null | grep -q servicemonitors; then
        record ok "enh.prom_crd" "ServiceMonitor CRD is available"
    else
        warn_or_fail "enh.prom_crd" \
            "ServiceMonitor CRD not found; MODP / RLHF metrics may not be scraped"
    fi

    # --- Namespaces + optional write checks ---
    NAMESPACES=( "green-agent" "green-agent-development" "green-agent-staging" "green-agent-production" )
    [[ -n "$TARGET_NAMESPACE" ]] && NAMESPACES=( "$TARGET_NAMESPACE" )

    for ns in "${NAMESPACES[@]}"; do
        if k get namespace "$ns" >/dev/null 2>&1; then
            record ok "enh.ns.${ns}" "Namespace '${ns}' exists"
            if [[ "$CHECK_RBAC" == true ]]; then
                for verb in get patch create delete; do
                    if ! k auth can-i "$verb" pods -n "$ns" >/dev/null 2>&1; then
                        warn_or_fail "enh.rbac.ns.${ns}.${verb}" \
                            "Cannot '${verb} pods' in namespace '${ns}'"
                    fi
                done
            fi
        fi
    done

    # --- RBAC for the actual deploy verbs (RayCluster CR) ---
    if [[ "$CHECK_RBAC" == true ]]; then
        while IFS=' ' read -r verb obj; do
            [[ -z "${verb:-}" ]] && continue
            if k auth can-i "$verb" "$obj" >/dev/null 2>&1; then
                record ok   "enh.rbac.${verb}.${obj}" "can-i ${verb} ${obj}"
            else
                warn_or_fail "enh.rbac.${verb}.${obj}" "cannot ${verb} ${obj}"
            fi
        done <<'VERBS'
get rayclusters
create rayclusters
patch rayclusters
delete rayclusters
create pods
delete pods
VERBS
    fi

    record ok "enh.end" "Enhanced capability checks complete"
fi

# ---------------------------------------------------------------------------
# 7. Emit report and exit
# ---------------------------------------------------------------------------
if [[ "$JSON" == true ]]; then
    emit_json
fi

if [[ $ERRORS -gt 0 ]]; then
    [[ "$JSON" == true ]] || {
        printf '\n%s\n' "${RED}❌ ${ERRORS} error(s), ${WARNINGS} warning(s)${NC}"
        printf '\nCommon fixes:\n'
        printf '  • kubectl config view --raw > %s       (regenerate kubeconfig)\n' "$KUBECONFIG_PATH"
        printf '  • chmod 600 %s                         (tighten permissions)\n' "$KUBECONFIG_PATH"
        printf '  • echo %s >> .gitignore                (prevent commit)\n' "$KUBECONFIG_PATH"
    }
    exit 1
fi

if [[ $WARNINGS -gt 0 && "$STRICT" != true ]]; then
    [[ "$JSON" == true ]] || {
        printf '\n%s\n' "${YELLOW}⚠️  Passed with ${WARNINGS} warning(s)${NC}"
        printf '\nNext steps:\n'
        printf '  1. Encode and store as KUBE_CONFIG in EACH GitHub environment:\n'
        printf '       GitHub → Settings → Environments → development | staging | production\n'
        printf '  2. Do NOT commit %s (it must be in .gitignore)\n' "$KUBECONFIG_PATH"
        printf '  3. Re-run with --strict in CI to promote warnings to errors\n'
    }
    exit 0
fi

[[ "$JSON" == true ]] || {
    printf '\n%s\n' "${GREEN}✅ All checks passed${NC}"
    printf '\nNext steps:\n'
    printf '  1. Encode (do NOT paste the encoded string into chat/logs):\n'
    printf '       base64 -w0 %s | pbcopy     # macOS\n' "$KUBECONFIG_PATH"
    printf '       base64 -w0 %s | xclip      # Linux\n' "$KUBECONFIG_PATH"
    printf '     Paste into GitHub → Settings → Environments → <env> → KUBE_CONFIG\n'
    printf '     Add the secret to EVERY environment (development, staging, production)\n'
    printf '  2. Confirm %s is listed in .gitignore\n' "$KUBECONFIG_PATH"
    printf '  3. Delete %s from the working tree after adding the secret\n' "$KUBECONFIG_PATH"
}
exit 0
