#!/bin/bash
# Verify kubeconfig setup before pushing to GitHub
# Enhanced with optional checks for advanced enhancements (LIMIT Graph, MODP, RLHF,
# Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, and MoE expert gating).

set -e

ENHANCEMENTS=false
if [[ "$1" == "--enhancements" ]]; then
  ENHANCEMENTS=true
  echo "🔧 Enhanced verification enabled (checks cluster capabilities for LIMIT Graph, MODP, RLHF, etc.)"
fi

echo "🔍 Verifying Kubernetes setup for GitHub Actions..."

# Check kubeconfig file
if [ ! -f "kubeconfig-github" ]; then
  echo "❌ Missing kubeconfig-github file"
  echo "Generate with: kubectl config view --raw > kubeconfig-github"
  exit 1
fi

# Verify structure
if ! grep -q "clusters:" kubeconfig-github || \
   ! grep -q "users:" kubeconfig-github || \
   ! grep -q "server:" kubeconfig-github; then
  echo "❌ Kubeconfig missing required fields"
  exit 1
fi

# Test locally
echo "🔍 Testing local connection..."
if kubectl --kubeconfig=./kubeconfig-github cluster-info --request-timeout=10s &>/dev/null; then
  echo "✅ Local connection works"
else
  echo "❌ Local connection failed"
  echo "Fix your kubeconfig before pushing to GitHub"
  exit 1
fi

# Verify base64 encoding
echo "🔍 Testing base64 encoding..."
ENCODED=$(base64 -w0 kubeconfig-github)
DECODED=$(echo "$ENCODED" | base64 -d)

if [ "$(md5sum < kubeconfig-github)" = "$(echo "$DECODED" | md5sum)" ]; then
  echo "✅ Base64 encoding is correct"
else
  echo "❌ Base64 encoding mismatch"
  exit 1
fi

# ------------------------------------------------------------------
# Optional Enhanced Checks (correlate with LIMIT Graph, MODP, RLHF,
# Multi‑Teacher Distillation, Bio‑inspired, MoE)
# ------------------------------------------------------------------
if $ENHANCEMENTS; then
  echo ""
  echo "🔍 Running enhanced cluster capability checks..."

  # 1. Check for node count (minimum 2 recommended for distributed RL/distillation)
  NODE_COUNT=$(kubectl --kubeconfig=./kubeconfig-github get nodes --no-headers | wc -l)
  echo "   Node count: $NODE_COUNT"
  if [ "$NODE_COUNT" -lt 2 ]; then
    echo "   ⚠️  Enhanced components (distributed RL, MoE, bio‑inspired optimisation) benefit from ≥2 nodes"
  fi

  # 2. Check total allocatable CPU and memory
  TOTAL_CPU=$(kubectl --kubeconfig=./kubeconfig-github get nodes -o jsonpath='{range .items[*]}{.status.allocatable.cpu}{"\n"}{end}' | awk '{sum += $1} END {print sum}')
  TOTAL_MEM=$(kubectl --kubeconfig=./kubeconfig-github get nodes -o jsonpath='{range .items[*]}{.status.allocatable.memory}{"\n"}{end}' | sed 's/Ki//g' | awk '{sum += $1} END {print sum}')
  echo "   Total allocatable CPU: $TOTAL_CPU cores"
  echo "   Total allocatable memory: $((TOTAL_MEM/1024)) Mi"
  if [ "$TOTAL_CPU" -lt 4 ]; then
    echo "   ⚠️  Low CPU may limit MODP and distillation training"
  fi

  # 3. Check for presence of Prometheus Operator CRDs (used for MODP metric collection)
  if kubectl --kubeconfig=./kubeconfig-github get crd servicemonitors.monitoring.coreos.com &>/dev/null; then
    echo "   ✅ Prometheus ServiceMonitor CRD found (MODP/RLHF metrics can be scraped)"
  else
    echo "   ⚠️  ServiceMonitor CRD not found; enhanced monitoring may not work"
  fi

  # 4. Check for required namespaces (if already exist)
  for ns in green-agent green-agent-dev green-agent-staging green-agent-production; do
    if kubectl --kubeconfig=./kubeconfig-github get namespace $ns &>/dev/null; then
      echo "   ✅ Namespace '$ns' exists"
    fi
  done

  echo ""
  echo "✅ Enhanced verification complete."
fi

echo ""
echo "✅ All checks passed!"
echo ""
echo "🚀 To add to GitHub:"
echo "1. Copy this encoded string:"
echo "$ENCODED" | head -c 200
echo "..."
echo ""
echo "2. Add as secret 'KUBE_CONFIG' in GitHub repo settings"
echo "3. Push your changes"
