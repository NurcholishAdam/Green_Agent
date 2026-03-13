#!/bin/bash

# Green Agent - Environment Deployment Script
# Deploys to development, staging, or production

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
ENVIRONMENT=${1:-development}
CONFIG_DIR="config/overlays/$ENVIRONMENT"

print_status() { echo -e "${BLUE}▶${NC} $1"; }
print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1"; }

# Validate environment
if [ ! -d "$CONFIG_DIR" ]; then
    print_error "Environment '$ENVIRONMENT' not found!"
    echo "Available environments: development, staging, production"
    exit 1
fi

print_status "Deploying Green Agent to $ENVIRONMENT environment..."
echo "========================================================"

# Create namespace
NAMESPACE="green-agent-$ENVIRONMENT"
print_status "Creating namespace: $NAMESPACE..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# Apply configuration
print_status "Applying Kustomize configuration..."
kubectl apply -k $CONFIG_DIR -n $NAMESPACE

# Wait for deployment
print_status "Waiting for deployment to complete..."
kubectl wait --for=condition=available \
  deployment/${ENVIRONMENT:0:4}-green-agent-cluster-head \
  -n $NAMESPACE \
  --timeout=300s || true

# Verify
print_status "Verifying deployment..."
kubectl get pods -n $NAMESPACE
kubectl get hpa -n $NAMESPACE
kubectl get networkpolicy -n $NAMESPACE

echo ""
print_success "Deployment to $ENVIRONMENT complete!"
echo ""
echo "📊 Access Dashboard:"
echo "   kubectl port-forward svc/${ENVIRONMENT:0:4}-green-agent-dashboard 8000:8000 -n $NAMESPACE"
echo "   open http://localhost:8000"
echo ""
