#!/bin/bash
# Generate all missing Kubernetes config files for Green Agent

set -e

echo "🔧 Creating all missing Kubernetes config files..."
echo ""

# Check we're in the right place
if [ ! -d ".git" ]; then
    echo "❌ Error: Must run from Green_Agent repository root"
    exit 1
fi

# Create directory structure
echo "📂 Creating directory structure..."
mkdir -p config/base
mkdir -p config/overlays/development
mkdir -p config/overlays/staging
mkdir -p config/overlays/production
echo "✅ Directories created"
echo ""

# ============================================================================
# BASE CONFIGURATION
# ============================================================================

echo "📝 Creating base configuration files..."

# 1. config/base/namespace.yaml
cat > config/base/namespace.yaml << 'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: green-agent
  labels:
    name: green-agent
EOF
echo "✅ config/base/namespace.yaml"

# 2. config/base/deployment.yaml
cat > config/base/deployment.yaml << 'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: green-agent
  namespace: green-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: green-agent
  template:
    metadata:
      labels:
        app: green-agent
    spec:
      containers:
      - name: green-agent
        image: green-agent:latest
        ports:
        - containerPort: 8000
          name: api
        - containerPort: 8265
          name: ray-dashboard
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
EOF
echo "✅ config/base/deployment.yaml"

# 3. config/base/service.yaml
cat > config/base/service.yaml << 'EOF'
apiVersion: v1
kind: Service
metadata:
  name: green-agent
  namespace: green-agent
spec:
  type: ClusterIP
  selector:
    app: green-agent
  ports:
  - name: api
    port: 8000
    targetPort: 8000
  - name: ray-dashboard
    port: 8265
    targetPort: 8265
EOF
echo "✅ config/base/service.yaml"

# 4. config/base/configmap.yaml
cat > config/base/configmap.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-config
  namespace: green-agent
data:
  VERSION: "5.0.0"
  ENVIRONMENT: "base"
  RAY_WORKERS: "4"
  CARBON_REGION: "US-CA"
EOF
echo "✅ config/base/configmap.yaml"

# 5. config/base/ray-cluster.yaml (IMPORTANT - was missing!)
cat > config/base/ray-cluster.yaml << 'EOF'
# Ray Cluster Configuration for Green Agent
# This is a placeholder - actual Ray cluster should be configured separately

apiVersion: v1
kind: ConfigMap
metadata:
  name: ray-cluster-config
  namespace: green-agent
data:
  ray-config.yaml: |
    # Ray cluster configuration
    cluster_name: green-agent-ray
    max_workers: 8
    upscaling_speed: 1.0
    idle_timeout_minutes: 5
    
    head_node_type: head.default
    available_node_types:
      head.default:
        min_workers: 0
        max_workers: 0
        resources: {}
      
      worker.default:
        min_workers: 2
        max_workers: 8
        resources:
          CPU: 4
          memory: 8000000000
EOF
echo "✅ config/base/ray-cluster.yaml"

# 6. config/base/kustomization.yaml (CRITICAL!)
cat > config/base/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent

resources:
  - namespace.yaml
  - deployment.yaml
  - service.yaml
  - configmap.yaml
  - ray-cluster.yaml

commonLabels:
  app: green-agent
  version: v5.0
EOF
echo "✅ config/base/kustomization.yaml"

echo ""

# ============================================================================
# DEVELOPMENT OVERLAY
# ============================================================================

echo "📝 Creating development overlay..."

# 7. config/overlays/development/kustomization.yaml
cat > config/overlays/development/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent-dev

bases:
  - ../../base

namePrefix: dev-

commonLabels:
  environment: development

configMapGenerator:
  - name: green-agent-config
    behavior: merge
    literals:
      - ENVIRONMENT=development
      - RAY_WORKERS=2
      - LOG_LEVEL=DEBUG

replicas:
  - name: green-agent
    count: 1
EOF
echo "✅ config/overlays/development/kustomization.yaml"

echo ""

# ============================================================================
# STAGING OVERLAY
# ============================================================================

echo "📝 Creating staging overlay..."

cat > config/overlays/staging/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent-staging

bases:
  - ../../base

namePrefix: staging-

commonLabels:
  environment: staging

configMapGenerator:
  - name: green-agent-config
    behavior: merge
    literals:
      - ENVIRONMENT=staging
      - RAY_WORKERS=4
      - LOG_LEVEL=INFO

replicas:
  - name: green-agent
    count: 2
EOF
echo "✅ config/overlays/staging/kustomization.yaml"

echo ""

# ============================================================================
# PRODUCTION OVERLAY
# ============================================================================

echo "📝 Creating production overlay..."

cat > config/overlays/production/kustomization.yaml << 'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: green-agent-production

bases:
  - ../../base

namePrefix: prod-

commonLabels:
  environment: production

configMapGenerator:
  - name: green-agent-config
    behavior: merge
    literals:
      - ENVIRONMENT=production
      - RAY_WORKERS=8
      - LOG_LEVEL=INFO

replicas:
  - name: green-agent
    count: 3
EOF
echo "✅ config/overlays/production/kustomization.yaml"

echo ""

# ============================================================================
# VERIFICATION
# ============================================================================

echo "🔍 Verifying all files created..."
echo ""

FILES=(
    "config/base/namespace.yaml"
    "config/base/deployment.yaml"
    "config/base/service.yaml"
    "config/base/configmap.yaml"
    "config/base/ray-cluster.yaml"
    "config/base/kustomization.yaml"
    "config/overlays/development/kustomization.yaml"
    "config/overlays/staging/kustomization.yaml"
    "config/overlays/production/kustomization.yaml"
)

MISSING=0
for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file"
    else
        echo "❌ $file"
        MISSING=$((MISSING + 1))
    fi
done

echo ""

if [ $MISSING -eq 0 ]; then
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║  ✅ All config files created successfully!                   ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "📋 Next steps:"
    echo ""
    echo "1. Review the files:"
    echo "   tree config/"
    echo ""
    echo "2. Test kustomize build (optional):"
    echo "   kustomize build config/overlays/development"
    echo ""
    echo "3. Commit and push:"
    echo "   git add config/"
    echo "   git commit -m 'Add Kubernetes configuration files'"
    echo "   git push origin main"
    echo ""
    echo "4. Watch GitHub Actions succeed! 🎉"
    echo ""
else
    echo "❌ $MISSING files missing - please check errors above"
    exit 1
fi
