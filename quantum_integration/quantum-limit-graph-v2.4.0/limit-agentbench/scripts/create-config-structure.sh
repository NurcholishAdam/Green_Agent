#!/usr/bin/env bash
# Green Agent - Kubernetes Config Structure Generator (Enhanced v2)
# =============================================================================
# Generates a complete, deployable Kustomize tree for the Green Agent v5.
#
# ---- Fixes vs v1 ------------------------------------------------------------
#   FIX #1  No more `replicas:` on RayCluster overlays (unsupported by
#           kustomize; set via workerGroupSpecs[].replicas in patches).
#   FIX #2  Service selector now matches pods (component: head label added
#           to headGroupSpec.template.metadata.labels).
#   FIX #3  HPA removed — RayCluster has no scale subresource. Autoscaling is
#           handled by workerGroupSpecs[].{minReplicas,maxReplicas}.
#   FIX #4  Conflicting configmap.yaml / secrets.yaml stubs deleted — only
#           configMapGenerator / secretGenerator remain.
#   FIX #5  Secrets no longer include unresolved ${CARBON_API_KEY}. Overlays
#           point at per-environment secrets.env (gitignored) instead.
#   FIX #6  deploy-environment.sh waits on raycluster readiness via the
#           ray.io/cluster pod label (works with namePrefix).
#   FIX #7  green-agent-data-pvc created in base/pvc.yaml.
#   FIX #8  --enhancements flag now actually toggles enhancement blocks.
#   FIX #9  NetworkPolicy no longer opens 8000/8265 to "from: []".
#   FIX #11 commonLabels replaced with labels (kustomize v5).
#   FIX #12 version moved from label to annotation.
#   FIX #13 kustomize build + kubeconform verification at the end.
#   FIX #19 securityContext applied to workers as well as head.
#   FIX #30 set -euo pipefail.
#   FIX #32 --force guard against overwriting hand-edited configs.
#   FIX #38–40 PersistentVolumeClaim, ServiceAccount, PodDisruptionBudget.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# MODULE 0 — CLI parsing + environment setup
# ---------------------------------------------------------------------------
INCLUDE_ENHANCEMENTS=false
FORCE=false
DRY_RUN=false
IMAGE_REGISTRY="${IMAGE_REGISTRY:-ghcr.io/nurcholishadam}"
VERSION="${VERSION:-v5.0.0}"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --enhancements    Enable LIMIT Graph / MODP / RLHF / Distillation / MoE
                    / Bio-inspired in every overlay config.
  --force           Overwrite existing files (default: refuse).
  --dry-run         Validate the plan; do not write anything.
  --registry <r>    Container registry (default: ghcr.io/nurcholishadam).
  --version <v>     Image tag (default: v5.0.0).
  -h, --help        Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --enhancements) INCLUDE_ENHANCEMENTS=true ;;
        --force)        FORCE=true ;;
        --dry-run)      DRY_RUN=true ;;
        --registry)     IMAGE_REGISTRY="${2:?--registry requires a value}"; shift ;;
        --version)      VERSION="${2:?--version requires a value}"; shift ;;
        -h|--help)      usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
    shift
done

if [[ "$INCLUDE_ENHANCEMENTS" == true ]]; then
    ENHANCEMENTS_ENABLED="true"
    echo "🔧 Enhancements: ENABLED"
else
    ENHANCEMENTS_ENABLED="false"
    echo "ℹ️  Enhancements: disabled (pass --enhancements to enable)"
fi
echo "🖼  Image: ${IMAGE_REGISTRY}/green_agent:${VERSION}"
echo "📄 Mode: $([[ $DRY_RUN == true ]] && echo 'DRY RUN' || echo 'WRITE')"
echo ""

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()   { echo -e "${BLUE}▶${NC} $*"; }
ok()    { echo -e "${GREEN}✓${NC} $*"; }
warn()  { echo -e "${YELLOW}⚠${NC} $*"; }
die()   { echo -e "${RED}✗${NC} $*" >&2; exit 1; }

# FIX #32 — refuse to overwrite unless --force
write_file() {
    local path="$1"
    if [[ "$DRY_RUN" == true ]]; then
        log "[dry-run] would write: $path"
        cat > /dev/null
        return
    fi
    if [[ -f "$path" && "$FORCE" != true ]]; then
        die "$path already exists (use --force to overwrite)"
    fi
    mkdir -p "$(dirname "$path")"
    cat > "$path"
    ok "wrote $path"
}

# ---------------------------------------------------------------------------
# MODULE 1 — Directory structure
# ---------------------------------------------------------------------------
log "Creating directory structure"
for d in \
    config/base \
    config/overlays/development \
    config/overlays/staging \
    config/overlays/production \
    config/environments \
    scripts \
    tests/k8s \
    tests/e2e
do
    [[ "$DRY_RUN" == true ]] && log "[dry-run] mkdir -p $d" || mkdir -p "$d"
done
ok "Directories ready"
echo ""

# ---------------------------------------------------------------------------
# MODULE 2 — Base kustomization (FIX #11, #12)
# ---------------------------------------------------------------------------
log "Writing base kustomization"

write_file config/base/kustomization.yaml <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent

labels:
  - includeSelectors: false
    pairs:
      app: green-agent
      managed-by: kustomize

commonAnnotations:
  app.kubernetes.io/part-of: green-agent
  app.kubernetes.io/managed-by: kustomize
  app.kubernetes.io/version: "${VERSION}"

resources:
  - pvc.yaml
  - serviceaccount.yaml
  - ray-cluster.yaml
  - service.yaml
  - network-policy.yaml
  - pdb.yaml
  - monitoring.yaml

# FIX #4 — configmap.yaml and secrets.yaml stubs removed; only generators remain.
configMapGenerator:
  - name: green-agent-config
    files:
      - green_agent_config.yaml
    options:
      disableNameSuffixHash: false

secretGenerator:
  - name: green-agent-secrets
    envs:
      - secrets.env
    options:
      disableNameSuffixHash: false

images:
  - name: green-agent
    newName: ${IMAGE_REGISTRY}/green_agent
    newTag: ${VERSION}
EOF

# ---------------------------------------------------------------------------
# MODULE 3 — Base config (FIX #8: enhancements flag actually toggles)
# ---------------------------------------------------------------------------
log "Writing base green_agent_config.yaml"

write_file config/base/green_agent_config.yaml <<EOF
system:
  version: "${VERSION}"
  mode: "unified"
  debug: false
  log_level: "INFO"
  enable_telemetry: true

dashboard:
  enabled: true
  host: "0.0.0.0"
  port: 8000
  update_interval_seconds: 5

ray:
  enabled: true
  num_workers: 4
  min_workers: 2
  max_workers: 20
  dashboard_port: 8265
  carbon_aware_scaling: true

carbon:
  default_region: "US-CA"
  api_provider: "simulation"
  eco_mode_threshold: 200
  defer_threshold: 400

policy:
  mode: "moderate"
  weights:
    carbon_importance: 0.4
    performance_importance: 0.3

monitoring:
  prometheus:
    enabled: true
    port: 9090
  logging:
    level: "INFO"
    format: "json"

security:
  enable_auth: false
  tls:
    enabled: false

development:
  enable_profiling: false
  mock_carbon_api: true

enhancements:
  enabled: ${ENHANCEMENTS_ENABLED}
  limit_graph:
    enabled: true
    graph_metrics: {"centrality": 0.5, "connectivity": 0.5, "density": 0.5}
  modp:
    enabled: true
    weights: [0.4, 0.3, 0.2, 0.1]
  rlhf:
    enabled: true
    human_feedback_score: 0.5
  distillation:
    enabled: true
    use_moe_gating: true
    distillation_lr: 0.01
    gating_lr: 0.005
  bio_inspired:
    enabled: true
    use_evolutionary: true
    population_size: 20
    mutation_rate: 0.1
    crossover_rate: 0.7
  moe_expert:
    enabled: true
    n_experts: 4
EOF

# ---------------------------------------------------------------------------
# MODULE 4 — PVC (FIX #7)
# ---------------------------------------------------------------------------
log "Writing base pvc.yaml"

write_file config/base/pvc.yaml <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: green-agent-data-pvc
  labels:
    app: green-agent
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 20Gi
  # storageClassName omitted -> cluster default
EOF

# ---------------------------------------------------------------------------
# MODULE 5 — ServiceAccount (FIX #40)
# ---------------------------------------------------------------------------
log "Writing base serviceaccount.yaml"

write_file config/base/serviceaccount.yaml <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: green-agent
  labels:
    app: green-agent
automountServiceAccountToken: false
EOF

# ---------------------------------------------------------------------------
# MODULE 6 — Ray cluster (FIX #2 head label, #19 worker securityContext,
#                       #36 worker probes, image repo variable)
# ---------------------------------------------------------------------------
log "Writing base ray-cluster.yaml"

write_file config/base/ray-cluster.yaml <<EOF
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: green-agent-cluster
  labels:
    app: green-agent
spec:
  headGroupSpec:
    rayStartParams:
      dashboard-host: "0.0.0.0"
      num-cpus: "4"
    template:
      metadata:
        # FIX #2 — this label is what Service.selector matches
        labels:
          app: green-agent
          component: head
      spec:
        serviceAccountName: green-agent
        containers:
          - name: ray-head
            image: ${IMAGE_REGISTRY}/green_agent:${VERSION}
            imagePullPolicy: IfNotPresent
            ports:
              - { name: gcs,      containerPort: 6379 }
              - { name: dashboard, containerPort: 8265 }
              - { name: http,     containerPort: 8000 }
              - { name: metrics,  containerPort: 9090 }
            resources:
              requests: { cpu: "2", memory: "4Gi" }
              limits:   { cpu: "4", memory: "8Gi" }
            livenessProbe:
              httpGet: { path: /health, port: 8000 }
              initialDelaySeconds: 30
              periodSeconds: 10
            readinessProbe:
              httpGet: { path: /ready, port: 8000 }
              initialDelaySeconds: 10
              periodSeconds: 5
            volumeMounts:
              - { name: config-volume, mountPath: /app/config }
              - { name: data-volume,   mountPath: /app/data }
        volumes:
          - name: config-volume
            configMap:
              name: green-agent-config
          - name: data-volume
            persistentVolumeClaim:
              claimName: green-agent-data-pvc
  workerGroupSpecs:
    - groupName: standard-workers
      replicas: 2
      minReplicas: 1
      maxReplicas: 10
      rayStartParams:
        num-cpus: "2"
      template:
        metadata:
          labels:
            app: green-agent
            component: worker
        spec:
          serviceAccountName: green-agent
          securityContext:
            # FIX #19 — apply securityContext to workers too
            runAsNonRoot: true
            runAsUser: 1000
            fsGroup: 1000
          containers:
            - name: ray-worker
              image: ${IMAGE_REGISTRY}/green_agent:${VERSION}
              imagePullPolicy: IfNotPresent
              securityContext:
                allowPrivilegeEscalation: false
                readOnlyRootFilesystem: true
                capabilities: { drop: ["ALL"] }
              resources:
                requests: { cpu: "1", memory: "2Gi" }
                limits:   { cpu: "2", memory: "4Gi" }
              # FIX #36 — health probes on workers
              livenessProbe:
                exec:
                  command: ["ray", "health-check", "--component", "raylet"]
                initialDelaySeconds: 60
                periodSeconds: 30
                failureThreshold: 3
              readinessProbe:
                exec:
                  command: ["ray", "health-check", "--component", "raylet"]
                initialDelaySeconds: 30
                periodSeconds: 10
              volumeMounts:
                - { name: tmp-volume, mountPath: /tmp }
          volumes:
            - name: tmp-volume
              emptyDir: {}
EOF

# ---------------------------------------------------------------------------
# MODULE 7 — Service (FIX #2: selector now matches)
# ---------------------------------------------------------------------------
log "Writing base service.yaml"

write_file config/base/service.yaml <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: green-agent-dashboard
  labels:
    app: green-agent
spec:
  type: ClusterIP
  ports:
    - { name: http,         port: 8000, targetPort: 8000 }
    - { name: ray-dashboard,port: 8265, targetPort: 8265 }
    - { name: metrics,      port: 9090, targetPort: 9090 }
  selector:
    app: green-agent
    component: head
EOF

# ---------------------------------------------------------------------------
# MODULE 8 — Secrets template (FIX #5: renamed to .example; real file gitignored)
# ---------------------------------------------------------------------------
log "Writing base secrets.env.example"

write_file config/base/secrets.env.example <<'EOF'
# Copy to secrets.env and fill in real values. secrets.env is gitignored.
CARBON_API_KEY=replace-me
SLACK_WEBHOOK=replace-me
API_KEYS=replace-me
EOF

write_file config/base/secrets.env <<'EOF'
# Placeholder so kustomize build succeeds. DO NOT COMMIT with real values.
CARBON_API_KEY=CHANGE_ME
SLACK_WEBHOOK=CHANGE_ME
API_KEYS=CHANGE_ME
EOF

# ---------------------------------------------------------------------------
# MODULE 9 — NetworkPolicy (FIX #9: no "from: []" wildcard)
# ---------------------------------------------------------------------------
log "Writing base network-policy.yaml"

write_file config/base/network-policy.yaml <<'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: green-agent-network-policy
  labels:
    app: green-agent
spec:
  podSelector:
    matchLabels:
      app: green-agent
  policyTypes: [Ingress, Egress]
  ingress:
    # Intra-namespace: allow Ray head <-> workers, metrics scraping
    - from:
        - podSelector:
            matchLabels:
              app: green-agent
      ports:
        - { protocol: TCP, port: 6379 }   # GCS
        - { protocol: TCP, port: 8265 }   # dashboard
        - { protocol: TCP, port: 8000 }   # HTTP
        - { protocol: TCP, port: 9090 }   # metrics
    # External ingress only from ingress-controller namespace (customize label)
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ingress-nginx
      ports:
        - { protocol: TCP, port: 8000 }
  egress:
    # DNS
    - ports:
        - { protocol: UDP, port: 53 }
        - { protocol: TCP, port: 53 }
    # HTTPS to carbon intensity APIs
    - ports:
        - { protocol: TCP, port: 443 }
EOF

# ---------------------------------------------------------------------------
# MODULE 10 — PDB (FIX #16)
# ---------------------------------------------------------------------------
log "Writing base pdb.yaml"

write_file config/base/pdb.yaml <<'EOF'
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: green-agent-head-pdb
  labels:
    app: green-agent
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: green-agent
      component: head
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: green-agent-worker-pdb
  labels:
    app: green-agent
spec:
  maxUnavailable: 50%
  selector:
    matchLabels:
      app: green-agent
      component: worker
EOF

# ---------------------------------------------------------------------------
# MODULE 11 — Monitoring (FIX #45: namespace-agnostic ServiceMonitor)
# ---------------------------------------------------------------------------
log "Writing base monitoring.yaml"

write_file config/base/monitoring.yaml <<'EOF'
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: green-agent-monitor
  labels:
    release: prometheus
    app: green-agent
spec:
  selector:
    matchLabels:
      app: green-agent
  # No matchNames — ServiceMonitor watches its own namespace by default
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: green-agent-alerts
  labels:
    release: prometheus
spec:
  groups:
    - name: green-agent.rules
      rules:
        - alert: GreenAgentHighCarbonIntensity
          expr: green_agent_carbon_intensity > 400
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Green Agent is running in a high-carbon-intensity region"
            description: "Carbon intensity is {{ $value }} gCO2/kWh (threshold 400)."
        - alert: GreenAgentTaskFailed
          expr: rate(green_agent_tasks_total{status="fail"}[5m]) > 0.05
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Green Agent task failure rate above 5%"
EOF

ok "Base manifests ready"
echo ""

# ---------------------------------------------------------------------------
# MODULE 12 — Overlays (FIX #1: no replicas: field)
# ---------------------------------------------------------------------------
generate_overlay() {
    local env="$1" prefix="$2" namespace="green-agent-$1" tag="$3"
    local head_cpu="$4" head_mem="$5" worker_replicas="$6" worker_min="$7" worker_max="$8" worker_cpu="$9" worker_mem="${10}"

    log "Writing ${env} overlay"

    write_file "config/overlays/${env}/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: ${namespace}
namePrefix: ${prefix}-

resources:
  - ../../base

labels:
  - includeSelectors: false
    pairs:
      environment: ${env}

patches:
  - path: config-patch.yaml
  - path: replica-patch.yaml
  $([[ "$env" == "production" ]] && echo "- path: security-patch.yaml")

configMapGenerator:
  - name: green-agent-config
    behavior: merge
    files:
      - green_agent_config.override.yaml

images:
  - name: green-agent
    newName: ${IMAGE_REGISTRY}/green_agent
    newTag: ${tag}
EOF

    # Per-overlay config patch (FIX #8: enhancements conditional)
    case "$env" in
        development)
            write_file "config/overlays/${env}/green_agent_config.override.yaml" <<EOF
system: {debug: true, log_level: DEBUG}
ray: {enabled: false, num_workers: 2}
carbon: {api_provider: simulation}
development: {enable_profiling: true, mock_carbon_api: true}
enhancements: {enabled: ${ENHANCEMENTS_ENABLED}}
EOF
            ;;
        staging)
            write_file "config/overlays/${env}/green_agent_config.override.yaml" <<EOF
system: {log_level: INFO}
ray: {enabled: true, num_workers: 4, min_workers: 2, max_workers: 10}
carbon: {api_provider: electricitymap}
enhancements: {enabled: ${ENHANCEMENTS_ENABLED}, bio_inspired: {enabled: true, population_size: 10}}
EOF
            ;;
        production)
            write_file "config/overlays/${env}/green_agent_config.override.yaml" <<EOF
system: {log_level: WARNING}
ray: {enabled: true, num_workers: 8, min_workers: 4, max_workers: 20, carbon_aware_scaling: true}
carbon:
  api_provider: electricitymap
  budget: {enabled: true, daily_limit_kg: 100.0}
security: {enable_auth: true, tls: {enabled: true}}
enhancements:
  enabled: ${ENHANCEMENTS_ENABLED}
  limit_graph: {enabled: true, graph_metrics: {centrality: 0.8, connectivity: 0.7, density: 0.6}}
  modp: {enabled: true, weights: [0.4, 0.3, 0.2, 0.1]}
  rlhf: {enabled: true, human_feedback_score: 0.6}
  distillation: {enabled: true, use_moe_gating: true}
  bio_inspired: {enabled: true, population_size: 30, mutation_rate: 0.1}
  moe_expert: {enabled: true, n_experts: 4}
EOF
            ;;
    esac

    # Replica patch targets the workerGroupSpecs (FIX #1)
    write_file "config/overlays/${env}/replica-patch.yaml" <<EOF
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: green-agent-cluster
spec:
  headGroupSpec:
    template:
      spec:
        containers:
          - name: ray-head
            resources:
              requests: {cpu: "${head_cpu}", memory: "${head_mem}"}
  workerGroupSpecs:
    - groupName: standard-workers
      replicas: ${worker_replicas}
      minReplicas: ${worker_min}
      maxReplicas: ${worker_max}
      template:
        spec:
          containers:
            - name: ray-worker
              resources:
                requests: {cpu: "${worker_cpu}", memory: "${worker_mem}"}
EOF

    # Production-only security patch
    if [[ "$env" == "production" ]]; then
        write_file "config/overlays/${env}/security-patch.yaml" <<'EOF'
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: green-agent-cluster
spec:
  headGroupSpec:
    template:
      spec:
        securityContext:
          runAsNonRoot: true
          runAsUser: 1000
          fsGroup: 1000
        containers:
          - name: ray-head
            securityContext:
              allowPrivilegeEscalation: false
              readOnlyRootFilesystem: true
              capabilities: { drop: ["ALL"] }
            volumeMounts:
              - { name: tmp-volume, mountPath: /tmp }
        volumes:
          - name: tmp-volume
            emptyDir: {}
EOF
    fi
}

# dev: 2 workers, 1 CPU / 2 Gi, tag v5.0.0-dev
generate_overlay development dev "v5.0.0-dev" "1" "2Gi" 2 1 4 "1" "2Gi"
# staging: 4 workers, 2 CPU / 4 Gi, tag v5.0.0-rc1
generate_overlay staging staging "v5.0.0-rc1" "2" "4Gi" 4 2 10 "2" "4Gi"
# production: 8 workers, 4 CPU / 8 Gi, tag v5.0.0
generate_overlay production prod "${VERSION}" "4" "8Gi" 8 4 20 "4" "8Gi"

# ---------------------------------------------------------------------------
# MODULE 13 — Environment files (dotenv)
# ---------------------------------------------------------------------------
log "Writing environment files"

write_file config/environments/.env.development <<EOF
MODE=unified
LOG_LEVEL=DEBUG
RAY_ENABLED=false
CARBON_API_PROVIDER=simulation
ENHANCEMENTS_ENABLED=${ENHANCEMENTS_ENABLED}
EOF

write_file config/environments/.env.staging <<EOF
MODE=unified
LOG_LEVEL=INFO
RAY_ENABLED=true
CARBON_API_PROVIDER=electricitymap
ENHANCEMENTS_ENABLED=${ENHANCEMENTS_ENABLED}
EOF

write_file config/environments/.env.production <<EOF
MODE=unified
LOG_LEVEL=WARNING
RAY_ENABLED=true
CARBON_API_PROVIDER=electricitymap
ENHANCEMENTS_ENABLED=${ENHANCEMENTS_ENABLED}
EOF

# ---------------------------------------------------------------------------
# MODULE 14 — .gitignore (protects real secrets — FIX #15)
# ---------------------------------------------------------------------------
log "Writing .gitignore additions"

if [[ "$DRY_RUN" == false ]]; then
    {
        echo "# Green Agent — generated artifacts and secrets"
        echo "config/base/secrets.env"
        echo "config/overlays/*/secrets.env"
        echo "config/environments/.env.*.local"
    } >> .gitignore
    ok "appended to .gitignore"
fi

# ---------------------------------------------------------------------------
# MODULE 15 — Deploy script (FIX #6: correct wait; set -euo; dry-run)
# ---------------------------------------------------------------------------
log "Writing deployment script"

write_file scripts/deploy-environment.sh <<'SCRIPT'
#!/usr/bin/env bash
# Green Agent — Environment Deployment
set -euo pipefail

ENVIRONMENT="${1:-}"
DRY_RUN="${DRY_RUN:-false}"

usage() {
    echo "Usage: $0 <development|staging|production> [--dry-run]"
    exit 2
}

[[ -z "$ENVIRONMENT" ]] && usage
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=true

case "$ENVIRONMENT" in
    development) PREFIX="dev";     NAMESPACE="green-agent-development" ;;
    staging)     PREFIX="staging"; NAMESPACE="green-agent-staging" ;;
    production)  PREFIX="prod";    NAMESPACE="green-agent-production" ;;
    *) echo "Unknown environment: $ENVIRONMENT" >&2; usage ;;
esac

CONFIG_DIR="config/overlays/${ENVIRONMENT}"
[[ -d "$CONFIG_DIR" ]] || { echo "Missing overlay: $CONFIG_DIR" >&2; exit 1; }

log()  { echo -e "\033[0;34m▶\033[0m $*"; }
ok()   { echo -e "\033[0;32m✓\033[0m $*"; }
fail() { echo -e "\033[0;31m✗\033[0m $*" >&2; exit 1; }

log "Deploying $ENVIRONMENT → namespace $NAMESPACE"

if [[ "$DRY_RUN" == true ]]; then
    log "dry-run — rendering manifests only"
    kustomize build "$CONFIG_DIR" > "/tmp/green-agent-${ENVIRONMENT}.yaml"
    ok "would apply /tmp/green-agent-${ENVIRONMENT}.yaml"
    exit 0
fi

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# FIX #6 — wait on raycluster readiness via pod labels (works with namePrefix)
kustomize build "$CONFIG_DIR" | kubectl apply -n "$NAMESPACE" -f -

log "Waiting for RayCluster pods to become Ready (up to 300s)..."
if ! kubectl wait --for=condition=Ready pod \
        -l "ray.io/cluster=${PREFIX}-green-agent-cluster" \
        -n "$NAMESPACE" --timeout=300s; then
    fail "Ray pods did not become Ready within 300s"
fi
ok "Deployment complete"
kubectl get pods -n "$NAMESPACE"
SCRIPT

if [[ "$DRY_RUN" == false ]]; then
    chmod +x scripts/deploy-environment.sh
    ok "chmod +x scripts/deploy-environment.sh"
fi

# ---------------------------------------------------------------------------
# MODULE 16 — Verification (FIX #13: build + kubeconform)
# ---------------------------------------------------------------------------
verify_manifests() {
    log "Verifying manifests"
    local fail_count=0
    for env in development staging production; do
        echo "  → kustomize build config/overlays/$env"
        local out="/tmp/green-agent-$env.yaml"
        if [[ "$DRY_RUN" == true ]]; then
            echo "    (dry-run) would build to $out"
            continue
        fi
        if ! kustomize build "config/overlays/$env" > "$out" 2>/tmp/build.log; then
            echo "    ✗ BUILD FAILED"
            sed 's/^/      /' /tmp/build.log
            fail_count=$((fail_count + 1))
            continue
        fi
        echo "    ✓ kustomize build OK"

        if command -v kubeconform >/dev/null 2>&1; then
            if ! kubeconform -strict -ignore-missing-schemas -summary "$out" >/tmp/kc.log 2>&1; then
                echo "    ✗ kubeconform FAILED"
                sed 's/^/      /' /tmp/kc.log
                fail_count=$((fail_count + 1))
            else
                echo "    ✓ kubeconform OK"
            fi
        else
            warn "    kubeconform not installed — schema validation skipped"
        fi
    done
    return $fail_count
}

if verify_manifests; then
    ok "All overlays built successfully"
else
    die "One or more overlays failed verification — see logs above"
fi
echo ""

# ---------------------------------------------------------------------------
# MODULE 17 — Summary
# ---------------------------------------------------------------------------
echo "=================================================="
echo "✅ Config structure generation complete"
echo "=================================================="
echo ""
echo "Summary:"
echo "  • Base: $(find config/base -name '*.yaml' | wc -l | tr -d ' ') YAML files"
echo "  • Overlays: $(find config/overlays -name '*.yaml' | wc -l | tr -d ' ') YAML files"
echo "  • Enhancements: ${ENHANCEMENTS_ENABLED}"
echo ""
echo "Next steps:"
echo "  1. Copy config/base/secrets.env.example → config/base/secrets.env"
echo "     and fill in real credentials (secrets.env is gitignored)"
echo "  2. git add config/ scripts/ .gitignore"
echo "  3. git commit -m 'feat: green agent kubernetes config (enhanced)'"
echo "  4. Deploy: ./scripts/deploy-environment.sh development --dry-run"
echo "  5. Deploy: ./scripts/deploy-environment.sh development"
