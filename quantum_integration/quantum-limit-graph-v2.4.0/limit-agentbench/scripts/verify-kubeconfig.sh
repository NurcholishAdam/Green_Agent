#!/bin/bash
# Verify kubeconfig setup before pushing to GitHub

set -e

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
