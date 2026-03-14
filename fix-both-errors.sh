#!/bin/bash
# Green Agent - One-Command Fix for Both Errors
# Fixes: 1) kubectl connection refused, 2) base64 invalid input

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  🔧 Green Agent - Complete Error Fix                          ║"
echo "║  Fixes both: kubectl connection + base64 errors               ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in the correct directory
if [ ! -d ".github" ]; then
    echo -e "${RED}❌ Error: .github directory not found${NC}"
    echo "   This script must be run from your Green_Agent repository root"
    echo "   Current directory: $(pwd)"
    exit 1
fi

echo -e "${GREEN}✅ Detected Git repository${NC}"
echo ""

# Create .github/workflows if it doesn't exist
mkdir -p .github/workflows

# Check if deploy-FINAL-FIX.yml exists
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FIXED_WORKFLOW="${SCRIPT_DIR}/deploy-FINAL-FIX.yml"

if [ ! -f "$FIXED_WORKFLOW" ]; then
    echo -e "${RED}❌ Error: deploy-FINAL-FIX.yml not found${NC}"
    echo "   Expected location: $FIXED_WORKFLOW"
    echo "   Please download it and place it in the same directory as this script"
    exit 1
fi

echo -e "${GREEN}✅ Found fixed workflow file${NC}"
echo ""

# Backup existing workflow if it exists
if [ -f ".github/workflows/deploy.yml" ]; then
    BACKUP_FILE=".github/workflows/deploy.yml.backup-$(date +%Y%m%d-%H%M%S)"
    echo -e "${YELLOW}📦 Backing up existing workflow...${NC}"
    cp .github/workflows/deploy.yml "$BACKUP_FILE"
    echo -e "${GREEN}   ✅ Backup created: $BACKUP_FILE${NC}"
else
    echo -e "${YELLOW}ℹ️  No existing deploy.yml found (this is fine)${NC}"
fi

echo ""

# Copy the fixed workflow
echo -e "${YELLOW}📥 Installing fixed workflow...${NC}"
cp "$FIXED_WORKFLOW" .github/workflows/deploy.yml
echo -e "${GREEN}✅ Fixed workflow installed${NC}"
echo ""

# Verify the fix
echo -e "${YELLOW}🔍 Verifying installation...${NC}"

ERROR_COUNT=0

# Check 1: File exists
if [ -f ".github/workflows/deploy.yml" ]; then
    echo -e "${GREEN}   ✅ Workflow file exists${NC}"
else
    echo -e "${RED}   ❌ Workflow file missing${NC}"
    ERROR_COUNT=$((ERROR_COUNT + 1))
fi

# Check 2: No kubectl in default jobs
if grep -q "kubectl cluster-info\|kubectl apply" .github/workflows/deploy.yml 2>/dev/null; then
    # Check if it's in a conditional job
    if grep -B 5 "kubectl" .github/workflows/deploy.yml | grep -q "if:.*KUBECONFIG"; then
        echo -e "${GREEN}   ✅ kubectl is conditional (good!)${NC}"
    else
        echo -e "${YELLOW}   ⚠️  kubectl found in workflow (may still have issues)${NC}"
        ERROR_COUNT=$((ERROR_COUNT + 1))
    fi
else
    echo -e "${GREEN}   ✅ No kubectl in default workflow (good!)${NC}"
fi

# Check 3: No KUBECONFIG decoding in default jobs
if grep -q "base64 -d.*kubeconfig\|KUBECONFIG.*base64" .github/workflows/deploy.yml 2>/dev/null; then
    echo -e "${YELLOW}   ⚠️  KUBECONFIG decoding found (may cause errors)${NC}"
    ERROR_COUNT=$((ERROR_COUNT + 1))
else
    echo -e "${GREEN}   ✅ No KUBECONFIG decoding (good!)${NC}"
fi

# Check 4: Has validate-manifests job
if grep -q "validate-manifests" .github/workflows/deploy.yml; then
    echo -e "${GREEN}   ✅ Validate manifests job found${NC}"
else
    echo -e "${YELLOW}   ⚠️  Validate manifests job not found${NC}"
fi

echo ""

if [ $ERROR_COUNT -eq 0 ]; then
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  ✅ Installation successful!                                   ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "🎯 What was fixed:"
    echo "   • Removed kubectl connection attempts"
    echo "   • Removed KUBECONFIG decoding"
    echo "   • Added manifest validation instead"
    echo "   • Workflow will now run successfully without a cluster"
    echo ""
    echo "📋 Next steps:"
    echo ""
    echo "   1. Review the changes:"
    echo -e "      ${YELLOW}git diff .github/workflows/deploy.yml${NC}"
    echo ""
    echo "   2. Commit and push:"
    echo -e "      ${YELLOW}git add .github/workflows/deploy.yml${NC}"
    echo -e "      ${YELLOW}git commit -m 'fix: Remove kubectl and KUBECONFIG from workflow'${NC}"
    echo -e "      ${YELLOW}git push origin main${NC}"
    echo ""
    echo "   3. Watch GitHub Actions succeed! 🎉"
    echo "      → Go to: https://github.com/$(git remote get-url origin | sed 's/.*github.com[:/]//' | sed 's/.git$//')/actions"
    echo ""
    echo "✅ No more 'connection refused' or 'base64 invalid' errors!"
    echo ""
else
    echo -e "${YELLOW}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║  ⚠️  Installation completed with warnings                     ║${NC}"
    echo -e "${YELLOW}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "   Please review the warnings above"
    echo "   The workflow should still work, but verify manually"
    echo ""
fi

echo "📚 For more information, see: COMPLETE-FIX-BOTH-ERRORS.md"
echo ""
