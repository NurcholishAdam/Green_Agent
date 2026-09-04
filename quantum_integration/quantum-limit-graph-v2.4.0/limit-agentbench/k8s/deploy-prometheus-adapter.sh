#!/bin/bash

# Green Agent - Prometheus Adapter Deployment Script (Enhanced)
# Enables custom metrics for carbon-aware autoscaling and advanced enhancements:
# LIMIT Graph, MODP, RLHF, Multi‑Teacher Distillation, Bio‑inspired, MoE expert gating.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
RELEASE_NAME="prometheus-adapter"
CHART_VERSION="3.4.0"
TIMEOUT="5m"

# Parse optional --enhancements flag
ENHANCEMENTS=false
if [[ "$*" == *"--enhancements"* ]]; then
    ENHANCEMENTS=true
fi

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Green Agent - Prometheus Adapter Deployment         ║${NC}"
if $ENHANCEMENTS; then
    echo -e "${BLUE}║   Enhanced Mode: LIMIT Graph, MODP, RLHF, etc.        ║${NC}"
fi
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to print status
print_status() {
    echo -e "${BLUE}▶${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Step 1: Check prerequisites
print_status "Checking prerequisites..."

if ! command -v helm &> /dev/null; then
    print_error "Helm is not installed. Please install Helm first."
    exit 1
fi
print_success "Helm is installed"

if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed. Please install kubectl first."
    exit 1
fi
print_success "kubectl is installed"

# Check cluster connectivity
if ! kubectl cluster-info &> /dev/null; then
    print_error "Cannot connect to Kubernetes cluster"
    exit 1
fi
print_success "Connected to Kubernetes cluster"

# Step 2: Create namespace if not exists
print_status "Creating namespace: $NAMESPACE..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
print_success "Namespace ready"

# Step 3: Add Helm repository
print_status "Adding Prometheus Community Helm repository..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
print_success "Helm repository added"

# Step 4: Deploy Prometheus Adapter
print_status "Deploying Prometheus Adapter..."
helm upgrade --install $RELEASE_NAME \
  prometheus-community/prometheus-adapter \
  --namespace $NAMESPACE \
  --create-namespace \
  -f k8s/prometheus-adapter-values.yaml \
  --wait \
  --timeout $TIMEOUT \
  --atomic

print_success "Prometheus Adapter deployed"

# Step 5: Wait for deployment to be ready
print_status "Waiting for Prometheus Adapter to be ready..."
kubectl wait --for=condition=available \
  deployment/$RELEASE_NAME-prometheus-adapter \
  -n $NAMESPACE \
  --timeout=120s

print_success "Prometheus Adapter is ready"

# Step 6: Verify API service
print_status "Verifying Custom Metrics API..."
sleep 5  # Allow time for API registration

if kubectl get apiservice v1beta1.custom.metrics.k8s.io &> /dev/null; then
    print_success "Custom Metrics API is registered"
else
    print_warning "Custom Metrics API not yet registered (may take 1-2 minutes)"
fi

# Step 7: Test metrics endpoint
print_status "Testing custom metrics availability..."
sleep 10  # Allow time for metrics to populate

# Check if Green Agent metrics are available
METRICS_AVAILABLE=$(kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1 2>/dev/null | grep -c "green_agent" || echo "0")

if [ "$METRICS_AVAILABLE" -gt 0 ]; then
    print_success "Green Agent custom metrics are available"
    echo ""
    echo -e "${GREEN}Available custom metrics:${NC}"
    kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1 2>/dev/null | \
      grep -o '"name":"[^"]*"' | grep "green_agent" | sed 's/"name":"//g' | sed 's/"//g' | \
      while read metric; do echo "  - $metric"; done
else
    print_warning "Custom metrics not yet available (may take 1-2 minutes)"
    print_status "Metrics will be available once Green Agent starts exporting data"
fi

# Step 8: Verify HPA can access metrics
print_status "Verifying HPA metric access..."
if kubectl get hpa -n green-agent &> /dev/null; then
    print_success "HPA resources found"
    echo ""
    echo -e "${GREEN}Current HPA status:${NC}"
    kubectl get hpa -n green-agent -o wide
else
    print_warning "No HPA resources found in green-agent namespace"
fi

# ---------------------------------------------------------------------------
# Enhanced Verification (if --enhancements flag provided)
# ---------------------------------------------------------------------------
if $ENHANCEMENTS; then
    echo ""
    print_status "Running enhanced metrics verification..."

    ENHANCED_METRICS=(
        "green_agent_modp_score"
        "green_agent_rlhf_feedback"
        "green_agent_graph_centrality"
        "green_agent_graph_connectivity"
        "green_agent_distillation_update_rate"
        "green_agent_moe_gate_stddev"
        "green_agent_evolutionary_best_fitness"
        "green_agent_flexgen_energy_rate"
    )

    for metric in "${ENHANCED_METRICS[@]}"; do
        # Check if the metric appears in the custom metrics API
        if kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1 2>/dev/null | grep -q "$metric"; then
            print_success "$metric is available"
        else
            print_warning "$metric not found yet (may appear after Green Agent starts exporting)"
        fi
    done

    # Optionally check HPA using enhanced metrics
    HPA_ENHANCED=$(kubectl get hpa -n green-agent -o json 2>/dev/null | \
        jq -r '.items[] | select(.spec.metrics[]?.pods.metric.name? // empty | startswith("green_agent_modp")) | .metadata.name' 2>/dev/null)
    if [ -n "$HPA_ENHANCED" ]; then
        print_success "HPA configured with enhanced metrics: $HPA_ENHANCED"
    else
        print_status "HPA may not yet be using enhanced metrics; you can update it manually with the metrics defined in k8s/prometheus-adapter-values.yaml"
    fi

    echo ""
    echo -e "${BLUE}Enhanced metrics next steps:${NC}"
    echo "1. Ensure Green Agent pods are exporting the enhanced metrics."
    echo "2. Update HPA to include enhanced metrics (e.g., MODP score, RLHF feedback)."
    echo "3. Verify with:"
    echo -e "   ${YELLOW}kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1/namespaces/green-agent/pods/*/modp_score${NC}"
    echo "   ${YELLOW}kubectl get hpa -n green-agent -o yaml | grep -A5 enhanced${NC}"
fi

# Step 9: Display access commands (always)
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              Deployment Complete!                      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Next Steps:${NC}"
echo ""
echo "1. Verify HPA is using custom metrics:"
echo -e "   ${YELLOW}kubectl get hpa -n green-agent --watch${NC}"
echo ""
echo "2. Test custom metrics endpoint:"
echo -e "   ${YELLOW}kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1/namespaces/green-agent/pods/*/carbon_intensity${NC}"
echo ""
echo "3. Check Prometheus Adapter logs:"
echo -e "   ${YELLOW}kubectl logs -l app.kubernetes.io/name=prometheus-adapter -n $NAMESPACE${NC}"
echo ""
echo "4. View available metrics:"
echo -e "   ${YELLOW}kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1 | jq '.resources[].name'${NC}"
echo ""
if $ENHANCEMENTS; then
    echo "5. Enhanced metrics (MODP, RLHF, Graph, etc.) can be verified similarly."
    echo "   See output above for their status."
fi
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
