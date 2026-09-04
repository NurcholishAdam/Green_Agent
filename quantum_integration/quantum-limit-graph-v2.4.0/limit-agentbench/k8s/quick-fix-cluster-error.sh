#!/bin/bash
# Quick Fix for Kubernetes Cluster Connection Error (Enhanced)
# This script replaces the workflow file to fix the "connection refused" error.
# Optional --enhancements flag adds advanced configuration for LIMIT Graph, MODP,
# RLHF, Multi‑Teacher Distillation, Bio‑inspired Optimisation, and MoE expert gating.

set -e

ENHANCEMENTS=false
if [[ "$1" == "--enhancements" ]]; then
  ENHANCEMENTS=true
  echo "🔧 Enhanced mode enabled (LIMIT Graph, MODP, RLHF, etc.)"
fi

echo "🔧 Green Agent - Cluster Connection Error Fix"
echo "=============================================="
echo ""

# Check if we're in the Green_Agent directory
if [ ! -f "run_agent.py" ] && [ ! -d ".github" ]; then
    echo "❌ Error: This script must be run from the Green_Agent repository root"
    echo "   Current directory: $(pwd)"
    echo "   Please cd to your Green_Agent directory first"
    exit 1
fi

echo "✅ Detected Green_Agent repository"
echo ""

# Backup existing workflow
if [ -f ".github/workflows/deploy.yml" ]; then
    BACKUP_FILE=".github/workflows/deploy.yml.backup.$(date +%s)"
    echo "📦 Backing up existing workflow to: $BACKUP_FILE"
    cp .github/workflows/deploy.yml "$BACKUP_FILE"
    echo "✅ Backup created"
else
    echo "ℹ️  No existing deploy.yml found (this is fine)"
fi

echo ""

# Get the source directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if we have the fixed workflow
if [ -f "${SCRIPT_DIR}/deploy.yml" ]; then
    echo "📥 Installing fixed workflow..."
    mkdir -p .github/workflows
    cp "${SCRIPT_DIR}/deploy.yml" .github/workflows/deploy.yml
    echo "✅ Fixed workflow installed at: .github/workflows/deploy.yml"
elif [ -f "deploy.yml" ]; then
    echo "📥 Installing fixed workflow from current directory..."
    mkdir -p .github/workflows
    cp "deploy.yml" .github/workflows/deploy.yml
    echo "✅ Fixed workflow installed"
else
    echo "❌ Error: deploy.yml not found"
    echo "   Please download it first and place it in the same directory as this script"
    exit 1
fi

echo ""
echo "🔍 Verifying installation..."

if [ -f ".github/workflows/deploy.yml" ]; then
    # Check if it has the fix
    if grep -q "validate-manifests" .github/workflows/deploy.yml; then
        echo "✅ Fixed workflow verified (contains validate-manifests job)"
    else
        echo "⚠️  Warning: Workflow may not contain the fix"
        echo "   Please verify the file manually"
    fi
    
    # Check it doesn't try to connect to cluster without KUBECONFIG
    if grep -q "if:.*KUBECONFIG" .github/workflows/deploy.yml; then
        echo "✅ Cluster deployment is conditional (good!)"
    else
        echo "⚠️  Warning: Cluster deployment may not be conditional"
    fi
else
    echo "❌ Workflow file not found after installation"
    exit 1
fi

# ---------------------------------------------------------------------------
# Optional Enhancement Checks
# ---------------------------------------------------------------------------
if $ENHANCEMENTS; then
    echo ""
    echo "🔧 Running enhancement-specific checks..."

    # Check for enhancements directory and key files
    ENHANCEMENTS_DIR="quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements"
    if [ -d "$ENHANCEMENTS_DIR" ]; then
        echo "   ✅ Enhancements directory found: $ENHANCEMENTS_DIR"
        # Verify presence of key files
        for f in feedback_event.py node_descriptor.py workload_descriptor.py zero_trust_architecture.py; do
            if [ -f "$ENHANCEMENTS_DIR/$f" ]; then
                echo "      ✅ $f"
            else
                echo "      ❌ $f MISSING"
            fi
        done
    else
        echo "   ⚠️  Enhancements directory not found; advanced features may not be available."
    fi

    # Create enhancement environment variables file if it doesn't exist
    ENV_FILE="config/environments/.env.enhancements"
    mkdir -p config/environments
    if [ ! -f "$ENV_FILE" ]; then
        cat > "$ENV_FILE" << 'EOF'
# Enhancement environment variables
ENHANCEMENTS_ENABLED=true
LIMIT_GRAPH_ENABLED=true
LIMIT_GRAPH_CENTRALITY=0.7
LIMIT_GRAPH_CONNECTIVITY=0.6
MODP_ENABLED=true
MODP_WEIGHTS=[0.4,0.3,0.2,0.1]
RLHF_ENABLED=true
HUMAN_FEEDBACK_SCORE=0.6
DISTILLATION_ENABLED=true
MOE_GATING_ENABLED=true
EVOLUTIONARY_ENABLED=true
POPULATION_SIZE=20
MUTATION_RATE=0.1
FLEXGEN_ENABLED=true
FLEXGEN_MODEL_NAME=facebook/opt-6.7b
FLEXGEN_DEFAULT_PRECISION=fp16
EOF
        echo "   ✅ Created $ENV_FILE with enhancement defaults"
    else
        echo "   ℹ️  $ENV_FILE already exists"
    fi

    # Optionally, create a ConfigMap patch for enhancements
    PATCH_DIR="config/overlays/enhancements"
    if [ ! -f "$PATCH_DIR/patch.yaml" ]; then
        mkdir -p "$PATCH_DIR"
        cat > "$PATCH_DIR/patch.yaml" << 'EOF'
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
          env:
          - name: ENHANCEMENTS_ENABLED
            value: "true"
          - name: LIMIT_GRAPH_ENABLED
            value: "true"
          - name: MODP_ENABLED
            value: "true"
          - name: RLHF_ENABLED
            value: "true"
          - name: DISTILLATION_ENABLED
            value: "true"
          - name: MOE_GATING_ENABLED
            value: "true"
          - name: EVOLUTIONARY_ENABLED
            value: "true"
  workerGroupSpecs:
  - groupName: standard-workers
    template:
      spec:
        containers:
        - name: ray-worker
          env:
          - name: ENHANCEMENTS_ENABLED
            value: "true"
          - name: LIMIT_GRAPH_ENABLED
            value: "true"
          - name: MODP_ENABLED
            value: "true"
          - name: RLHF_ENABLED
            value: "true"
          - name: DISTILLATION_ENABLED
            value: "true"
          - name: MOE_GATING_ENABLED
            value: "true"
          - name: EVOLUTIONARY_ENABLED
            value: "true"
EOF
        echo "   ✅ Created $PATCH_DIR/patch.yaml"
    else
        echo "   ℹ️  Enhancement patch already exists"
    fi

    echo "   🧠 Advanced enhancement configuration prepared."
fi

echo ""
echo "✅ Installation complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Review the changes:"
echo "      git diff .github/workflows/deploy.yml"
if $ENHANCEMENTS; then
    echo "      git diff config/environments/.env.enhancements"
    echo "      git diff config/overlays/enhancements/patch.yaml"
fi
echo ""
echo "   2. Commit and push:"
echo "      git add .github/workflows/deploy.yml"
if $ENHANCEMENTS; then
    echo "      git add config/environments/.env.enhancements"
    echo "      git add config/overlays/enhancements/patch.yaml"
fi
echo "      git commit -m 'fix: Update workflow to not require Kubernetes cluster'"
echo "      git push origin main"
echo ""
echo "   3. Watch GitHub Actions run successfully! 🎉"
echo ""
echo "🎯 What changed:"
echo "   • Workflow no longer tries to connect to Kubernetes cluster"
echo "   • Only validates manifests and uploads artifacts"
echo "   • Actual deployment only happens if KUBECONFIG is configured"
echo "   • No more 'connection refused' errors!"
if $ENHANCEMENTS; then
    echo "   • Added enhancement configuration for advanced modules (LIMIT Graph, MODP, RLHF, etc.)"
fi
echo ""
echo "📚 Read CLUSTER_CONNECTION_FIX.md for full documentation"
echo ""
