#!/bin/bash
# Quick Fix for Kubernetes Cluster Connection Error
# This script replaces the workflow file to fix the "connection refused" error

set -e

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

echo ""
echo "✅ Installation complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Review the changes:"
echo "      git diff .github/workflows/deploy.yml"
echo ""
echo "   2. Commit and push:"
echo "      git add .github/workflows/deploy.yml"
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
echo ""
echo "📚 Read CLUSTER_CONNECTION_FIX.md for full documentation"
echo ""
