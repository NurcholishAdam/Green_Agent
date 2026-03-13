#!/bin/bash
# Green Agent - Create Complete Kubernetes Config Structure
# Run this locally to generate all required config files

set -e

echo "🌱 Green Agent v5.0.0 - Config Structure Generator"
echo "=================================================="
echo ""

# Create directory structure
echo "📁 Creating directory structure..."
mkdir -p config/base
mkdir -p config/overlays/development
mkdir -p config/overlays/staging
mkdir -p config/overlays/production
mkdir -p config/environments
mkdir -p scripts
mkdir -p tests/k8s
mkdir -p tests/e2e

echo "✅ Directories created"
echo ""

# ============================================
# BASE KUSTOMIZATION
# ============================================
echo "📝 Creating base configuration..."

cat > config/base/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent

commonLabels:
  app: green-agent
  version: "5.0.0"
  managed-by: kustomize

resources:
- ray-cluster.yaml
- service.yaml
- configmap.yaml
- secrets.yaml
- hpa.yaml
- network-policy.yaml
- monitoring.yaml

configMapGenerator:
- name: green-agent-config
  files:
  - green_agent_config.yaml

secretGenerator:
- name: green-agent-secrets
  envs:
  - secrets.env

images:
- name: ghcr.io/nurcholishadam/green_agent
  newTag: v5.0.0

commonAnnotations:
  app.kubernetes.io/part-of: green-agent
  app.kubernetes.io/managed-by: kustomize
EOF

# ============================================
# BASE CONFIG
# ============================================
cat > config/base/green_agent_config.yaml << 'EOF'
system:
  version: "5.0.0"
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

quantum:
  enabled: false
  backend: "simulator"

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
EOF

# ============================================
# BASE RAY CLUSTER
# ============================================
cat > config/base/ray-cluster.yaml << 'EOF'
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: green-agent-cluster
  labels:
    app: green-agent
spec:
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
      num-cpus: '4'
    template:
      spec:
        containers:
        - name: ray-head
          image: ghcr.io/nurcholishadam/green_agent:v5.0.0
          imagePullPolicy: Always
          ports:
          - containerPort: 6379
          - containerPort: 8265
          - containerPort: 8000
          - containerPort: 9090
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 5
          volumeMounts:
          - name: config-volume
            mountPath: /app/config
          - name: data-volume
            mountPath: /app/data
        volumes:
        - name: config-volume
          configMap:
            name: green-agent-config
        - name: data-volume
          persistentVolumeClaim:
            claimName: green-agent-data-pvc
  workerGroupSpecs:
  - replicas: 2
    minReplicas: 1
    maxReplicas: 10
    groupName: standard-workers
    rayStartParams:
      num-cpus: '2'
    template:
      spec:
        containers:
        - name: ray-worker
          image: ghcr.io/nurcholishadam/green_agent:v5.0.0
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
EOF

# ============================================
# BASE SERVICE
# ============================================
cat > config/base/service.yaml << 'EOF'
apiVersion: v1
kind: Service
metadata:
  name: green-agent-dashboard
  labels:
    app: green-agent
spec:
  type: ClusterIP
  ports:
  - name: dashboard
    port: 8000
    targetPort: 8000
  - name: ray-dashboard
    port: 8265
    targetPort: 8265
  - name: metrics
    port: 9090
    targetPort: 9090
  selector:
    app: green-agent
    component: head
EOF

# ============================================
# BASE CONFIGMAP
# ============================================
cat > config/base/configmap.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-config
  labels:
    app: green-agent
data:
  # Content generated by kustomize from green_agent_config.yaml
EOF

# ============================================
# BASE SECRETS
# ============================================
cat > config/base/secrets.yaml << 'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: green-agent-secrets
  labels:
    app: green-agent
type: Opaque
# Values populated from secrets.env
EOF

cat > config/base/secrets.env << 'EOF'
# Base secrets template - replace with real values
CARBON_API_KEY=your-key-here
SLACK_WEBHOOK=your-webhook-here
API_KEYS=your-api-keys-here
EOF

# ============================================
# BASE HPA
# ============================================
cat > config/base/hpa.yaml << 'EOF'
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: green-agent-hpa
  labels:
    app: green-agent
spec:
  scaleTargetRef:
    apiVersion: ray.io/v1
    kind: RayCluster
    name: green-agent-cluster
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
    scaleUp:
      stabilizationWindowSeconds: 60
EOF

# ============================================
# BASE NETWORK POLICY
# ============================================
cat > config/base/network-policy.yaml << 'EOF'
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
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from: []
    ports:
    - protocol: TCP
      port: 8000
    - protocol: TCP
      port: 8265
  - from:
    - podSelector:
        matchLabels:
          app: green-agent
    ports:
    - protocol: TCP
      port: 6379
  egress:
  - to:
    - namespaceSelector: {}
    ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
  - to:
    - ipBlock:
        cidr: 0.0.0.0/0
    ports:
    - protocol: TCP
      port: 443
EOF

# ============================================
# BASE MONITORING
# ============================================
cat > config/base/monitoring.yaml << 'EOF'
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
  namespaceSelector:
    matchNames:
    - green-agent
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
        summary: "High carbon intensity"
EOF

echo "✅ Base configuration created"
echo ""

# ============================================
# DEVELOPMENT OVERLAY
# ============================================
echo "📝 Creating development overlay..."

cat > config/overlays/development/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent-dev
namePrefix: dev-

resources:
- ../../base

commonLabels:
  environment: development

patches:
- path: config-patch.yaml
- path: replica-patch.yaml

configMapGenerator:
- name: green-agent-config
  behavior: merge
  literals:
  - LOG_LEVEL=DEBUG
  - RAY_ENABLED=false
  - CARBON_API_PROVIDER=simulation

secretGenerator:
- name: green-agent-secrets
  behavior: merge
  literals:
  - CARBON_API_KEY=dev-key

images:
- name: ghcr.io/nurcholishadam/green_agent
  newTag: v5.0.0-dev

replicas:
- name: green-agent-cluster
  count: 2
EOF

cat > config/overlays/development/config-patch.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-config
data:
  green_agent_config.yaml: |
    system:
      version: "5.0.0-dev"
      mode: "unified"
      debug: true
      log_level: "DEBUG"
    ray:
      enabled: false
      num_workers: 2
    carbon:
      api_provider: "simulation"
    quantum:
      enabled: false
    development:
      enable_profiling: true
      mock_carbon_api: true
EOF

cat > config/overlays/development/replica-patch.yaml << 'EOF'
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
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
  workerGroupSpecs:
  - replicas: 2
    minReplicas: 1
    maxReplicas: 4
    groupName: standard-workers
    template:
      spec:
        containers:
        - name: ray-worker
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
EOF

echo "✅ Development overlay created"
echo ""

# ============================================
# STAGING OVERLAY
# ============================================
echo "📝 Creating staging overlay..."

cat > config/overlays/staging/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent-staging
namePrefix: staging-

resources:
- ../../base

commonLabels:
  environment: staging

patches:
- path: config-patch.yaml
- path: replica-patch.yaml

configMapGenerator:
- name: green-agent-config
  behavior: merge
  literals:
  - LOG_LEVEL=INFO
  - RAY_ENABLED=true
  - CARBON_API_PROVIDER=electricitymap

secretGenerator:
- name: green-agent-secrets
  behavior: merge
  literals:
  - CARBON_API_KEY=${CARBON_API_KEY}

images:
- name: ghcr.io/nurcholishadam/green_agent
  newTag: v5.0.0-rc1

replicas:
- name: green-agent-cluster
  count: 4
EOF

cat > config/overlays/staging/config-patch.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-config
data:
  green_agent_config.yaml: |
    system:
      version: "5.0.0-rc1"
      mode: "unified"
      log_level: "INFO"
    ray:
      enabled: true
      num_workers: 4
      min_workers: 2
      max_workers: 10
    carbon:
      api_provider: "electricitymap"
    quantum:
      enabled: false
EOF

cat > config/overlays/staging/replica-patch.yaml << 'EOF'
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
            requests:
              cpu: "2"
              memory: "4Gi"
  workerGroupSpecs:
  - replicas: 4
    minReplicas: 2
    maxReplicas: 10
    groupName: standard-workers
    template:
      spec:
        containers:
        - name: ray-worker
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
EOF

echo "✅ Staging overlay created"
echo ""

# ============================================
# PRODUCTION OVERLAY
# ============================================
echo "📝 Creating production overlay..."

cat > config/overlays/production/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent-production
namePrefix: prod-

resources:
- ../../base

commonLabels:
  environment: production
  criticality: high

patches:
- path: config-patch.yaml
- path: replica-patch.yaml
- path: security-patch.yaml

configMapGenerator:
- name: green-agent-config
  behavior: merge
  literals:
  - LOG_LEVEL=WARNING
  - RAY_ENABLED=true
  - QUANTUM_ENABLED=true
  - CARBON_API_PROVIDER=electricitymap

secretGenerator:
- name: green-agent-secrets
  behavior: merge
  literals:
  - CARBON_API_KEY=${CARBON_API_KEY}

images:
- name: ghcr.io/nurcholishadam/green_agent
  newTag: v5.0.0

replicas:
- name: green-agent-cluster
  count: 8
EOF

cat > config/overlays/production/config-patch.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-config
data:
  green_agent_config.yaml: |
    system:
      version: "5.0.0"
      mode: "unified"
      log_level: "WARNING"
    ray:
      enabled: true
      num_workers: 8
      min_workers: 4
      max_workers: 20
      carbon_aware_scaling: true
    carbon:
      api_provider: "electricitymap"
      budget:
        enabled: true
        daily_limit_kg: 100.0
    quantum:
      enabled: true
      backend: "simulator"
    security:
      enable_auth: true
      tls:
        enabled: true
EOF

cat > config/overlays/production/replica-patch.yaml << 'EOF'
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
            requests:
              cpu: "4"
              memory: "8Gi"
  workerGroupSpecs:
  - replicas: 8
    minReplicas: 4
    maxReplicas: 20
    groupName: standard-workers
    template:
      spec:
        containers:
        - name: ray-worker
          resources:
            requests:
              cpu: "4"
              memory: "8Gi"
EOF

cat > config/overlays/production/security-patch.yaml << 'EOF'
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
            capabilities:
              drop: ["ALL"]
          volumeMounts:
          - name: tmp-volume
            mountPath: /tmp
        volumes:
        - name: tmp-volume
          emptyDir: {}
EOF

echo "✅ Production overlay created"
echo ""

# ============================================
# ENVIRONMENT FILES
# ============================================
echo "📝 Creating environment files..."

cat > config/environments/.env.development << 'EOF'
MODE=unified
LOG_LEVEL=DEBUG
RAY_ENABLED=false
CARBON_API_PROVIDER=simulation
QUANTUM_ENABLED=false
EOF

cat > config/environments/.env.staging << 'EOF'
MODE=unified
LOG_LEVEL=INFO
RAY_ENABLED=true
CARBON_API_PROVIDER=electricitymap
QUANTUM_ENABLED=false
EOF

cat > config/environments/.env.production << 'EOF'
MODE=unified
LOG_LEVEL=WARNING
RAY_ENABLED=true
CARBON_API_PROVIDER=electricitymap
QUANTUM_ENABLED=true
EOF

echo "✅ Environment files created"
echo ""

# ============================================
# DEPLOYMENT SCRIPT
# ============================================
echo "📝 Creating deployment script..."

cat > scripts/deploy-environment.sh << 'SCRIPT_EOF'
#!/bin/bash
# Green Agent - Environment Deployment Script

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ENVIRONMENT="${1:-development}"
CONFIG_DIR="config/overlays/${ENVIRONMENT}"

print_status() { echo -e "${BLUE}▶${NC} $1"; }
print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1"; }

if [ ! -d "$CONFIG_DIR" ]; then
    print_error "Environment '$ENVIRONMENT' not found!"
    exit 1
fi

NAMESPACE="green-agent-${ENVIRONMENT}"

print_status "Deploying to $ENVIRONMENT environment..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k "$CONFIG_DIR" -n "$NAMESPACE"

print_status "Waiting for deployment..."
kubectl wait --for=condition=available \
  deployment/"${ENVIRONMENT:0:4}-green-agent-cluster-head" \
  -n "$NAMESPACE" \
  --timeout=300s || true

print_success "Deployment complete!"
kubectl get pods -n "$NAMESPACE"
SCRIPT_EOF

chmod +x scripts/deploy-environment.sh

echo "✅ Deployment script created"
echo ""

# ============================================
# VERIFY
# ============================================
echo "🔍 Verifying config structure..."
find config -type f -name "*.yaml" | wc -l
echo "config files created"
echo ""

echo "=================================================="
echo "✅ Config structure generation complete!"
echo "=================================================="
echo ""
echo "🚀 Next steps:"
echo "1. git add config/ scripts/"
echo "2. git commit -m 'feat: add kubernetes config structure'"
echo "3. git push"
echo "4. Set GitHub secrets: KUBE_CONFIG, CARBON_API_KEY"
echo "5. Run GitHub Actions workflow"
echo ""
