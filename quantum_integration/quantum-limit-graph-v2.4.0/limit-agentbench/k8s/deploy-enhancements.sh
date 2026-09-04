#!/bin/bash

# Green Agent Kubernetes Enhancements Deployment Script (Enhanced)
# Deploys all core enhancements plus optional advanced modules:
# LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
# Bio‑inspired Optimisation, MoE expert gating, and optional FlexGen.

set -e

# Parse optional --enhancements flag to include advanced modules
ENHANCEMENTS=false
if [[ "$*" == *"--enhancements"* ]]; then
    ENHANCEMENTS=true
fi

echo "🚀 Deploying Green Agent Kubernetes Enhancements..."
if $ENHANCEMENTS; then
    echo "🔧 Advanced enhancements enabled (LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio‑inspired)"
else
    echo "ℹ️  Deploying core components only (use --enhancements to include advanced modules)"
fi
echo "=================================================="

NAMESPACE="green-agent"

# 1. Create namespace if not exists
echo "1️⃣  Creating namespace..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# 2. Deploy Network Policies (security first)
echo "2️⃣  Deploying Network Policies..."
kubectl apply -f k8s/network-policy.yaml -n $NAMESPACE

# 3. Deploy ConfigMaps
echo "3️⃣  Deploying ConfigMaps..."
kubectl apply -f k8s/grafana-dashboard.yaml -n $NAMESPACE
kubectl apply -f k8s/carbon-autoscaler.yaml -n $NAMESPACE

# 4. Deploy Monitoring
echo "4️⃣  Deploying Monitoring Stack..."
kubectl apply -f k8s/monitoring.yaml -n $NAMESPACE

# 5. (Optional) Deploy Enhancement-specific ConfigMaps and Secrets
if $ENHANCEMENTS; then
    echo "5️⃣  Deploying enhancement ConfigMaps and Secrets..."
    # Check if enhancements directory exists
    if [ -d "k8s/enhancements" ]; then
        kubectl apply -k k8s/enhancements -n $NAMESPACE
        echo "   ✅ Enhancement overlays applied"
    else
        echo "   ⚠️  k8s/enhancements directory not found; creating default enhancement ConfigMap"
        # Create a temporary ConfigMap from a heredoc (or use existing file if present)
        cat <<EOF | kubectl apply -f - -n $NAMESPACE
apiVersion: v1
kind: ConfigMap
metadata:
  name: green-agent-enhancements
  labels:
    app: green-agent
    enhancements: enabled
data:
  green_agent_config.yaml: |
    enhancements:
      enabled: true
      limit_graph:
        enabled: true
        graph_metrics:
          centrality: 0.7
          connectivity: 0.6
      modp:
        enabled: true
        objective_weights: [0.4,0.3,0.2,0.1]
      rlhf:
        enabled: true
        human_feedback_score: 0.6
      distillation:
        enabled: true
        use_moe_gating: true
      bio_inspired:
        enabled: true
        use_evolutionary: true
        population_size: 20
      moe_expert:
        enabled: true
        n_experts: 4
      flexgen:
        enabled: true
        model_name: "facebook/opt-6.7b"
        default_precision: "fp16"
EOF
        echo "   ✅ Default enhancement ConfigMap created"
    fi
else
    echo "5️⃣  Skipping enhancement ConfigMaps/Secrets (use --enhancements to enable)"
fi

# 6. Update Ray Cluster with enhanced probes
echo "6️⃣  Updating Ray Cluster..."
kubectl apply -f k8s/ray-cluster.yaml -n $NAMESPACE

# 7. Deploy HPA
echo "7️⃣  Deploying Horizontal Pod Autoscaler..."
kubectl apply -f k8s/carbon-autoscaler.yaml -n $NAMESPACE

# 8. (Optional) Apply enhancement-specific RayCluster patches
if $ENHANCEMENTS; then
    echo "8️⃣  Applying enhancement patches to RayCluster..."
    if [ -f "k8s/ray-cluster-enhancements-patch.yaml" ]; then
        kubectl patch raycluster green-agent-cluster -n $NAMESPACE --type merge --patch "$(cat k8s/ray-cluster-enhancements-patch.yaml)"
        echo "   ✅ RayCluster patched with enhancement environment variables"
    else
        echo "   ⚠️  k8s/ray-cluster-enhancements-patch.yaml not found; you may need to manually set env vars"
    fi
fi

# 9. Verify deployment
echo ""
echo "✅ Verifying deployment..."
echo ""

echo "📊 Network Policies:"
kubectl get networkpolicy -n $NAMESPACE

echo ""
echo "📈 HPA Status:"
kubectl get hpa -n $NAMESPACE

echo ""
echo "🔍 ServiceMonitors:"
kubectl get servicemonitor -n $NAMESPACE

echo ""
echo "📊 ConfigMaps:"
kubectl get configmap -n $NAMESPACE

echo ""
echo "🚀 Pods:"
kubectl get pods -n $NAMESPACE

if $ENHANCEMENTS; then
    echo ""
    echo "🧠 Enhanced Pods (should have ENHANCEMENTS_ENABLED=true):"
    kubectl get pods -n $NAMESPACE -o json | jq -r '.items[] | select(.spec.containers[].env[]?.name=="ENHANCEMENTS_ENABLED") | .metadata.name'
fi

echo ""
echo "=================================================="
echo "✅ Deployment complete!"
echo ""
echo "📊 Access Dashboard: kubectl port-forward svc/green-agent-dashboard 8000:8000 -n $NAMESPACE"
echo "📈 Access Metrics: kubectl port-forward svc/green-agent-metrics 9090:9090 -n $NAMESPACE"
echo "❤️  Health Check: curl http://localhost:8000/health"
echo "✅ Readiness: curl http://localhost:8000/ready"
echo "📊 Prometheus: curl http://localhost:9090/metrics"
if $ENHANCEMENTS; then
    echo "🧠 MODP Score: curl http://localhost:9090/metrics | grep modp"
    echo "🧠 RLHF Feedback: curl http://localhost:9090/metrics | grep rlhf"
    echo "🧠 Graph Centrality: curl http://localhost:9090/metrics | grep graph_centrality"
fi
echo "=================================================="
