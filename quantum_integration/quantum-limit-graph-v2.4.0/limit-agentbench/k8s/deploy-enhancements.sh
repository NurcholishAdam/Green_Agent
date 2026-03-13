#!/bin/bash

# Green Agent Kubernetes Enhancements Deployment Script
# Deploys all 5 enhancements in correct order

set -e

echo "🚀 Deploying Green Agent Kubernetes Enhancements..."
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

# 5. Update Ray Cluster with enhanced probes
echo "5️⃣  Updating Ray Cluster..."
kubectl apply -f k8s/ray-cluster.yaml -n $NAMESPACE

# 6. Deploy HPA
echo "6️⃣  Deploying Horizontal Pod Autoscaler..."
kubectl apply -f k8s/carbon-autoscaler.yaml -n $NAMESPACE

# 7. Verify deployment
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

echo ""
echo "=================================================="
echo "✅ Deployment complete!"
echo ""
echo "📊 Access Dashboard: kubectl port-forward svc/green-agent-dashboard 8000:8000 -n $NAMESPACE"
echo "📈 Access Metrics: kubectl port-forward svc/green-agent-metrics 9090:9090 -n $NAMESPACE"
echo "❤️  Health Check: curl http://localhost:8000/health"
echo "✅ Readiness: curl http://localhost:8000/ready"
echo "📊 Prometheus: curl http://localhost:9090/metrics"
echo "=================================================="
