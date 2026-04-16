#!/bin/bash
# compare-green-agent.sh - Compare actual repo vs. v5.0.0 specifications

set -e

echo "🔍 Green Agent v5.0.0 Comparison Tool"
echo "======================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results
TOTAL=0
MATCHED=0
MISSING=0
PARTIAL=0

check_file() {
    local filepath="$1"
    local description="$2"
    local expected_content="$3"
    
    TOTAL=$((TOTAL + 1))
    
    if [ -f "$filepath" ]; then
        if [ -n "$expected_content" ]; then
            if grep -q "$expected_content" "$filepath" 2>/dev/null; then
                echo -e "${GREEN}✓${NC} $description: $filepath"
                MATCHED=$((MATCHED + 1))
            else
                echo -e "${YELLOW}⚠${NC} $description: $filepath (exists but content differs)"
                PARTIAL=$((PARTIAL + 1))
            fi
        else
            echo -e "${GREEN}✓${NC} $description: $filepath"
            MATCHED=$((MATCHED + 1))
        fi
    else
        echo -e "${RED}✗${NC} $description: $filepath (MISSING)"
        MISSING=$((MISSING + 1))
    fi
}

echo "📁 Checking Core Architecture Files..."
echo "---------------------------------------"
check_file "README.md" "Main README" "Green Agent"
check_file "runtime/run_agent.py" "Main entry point" "GreenAgent\|run_agent"
check_file "src/integration/unified_orchestrator.py" "Unified Orchestrator" "class.*Orchestrator"
check_file "src/decision/carbon_aware_decision_core.py" "Carbon Decision Core" "class.*Carbon"
check_file "src/carbon/forecasting_engine.py" "Carbon Forecaster" "class.*Forecast"
check_file "dashboard/api_server.py" "Dashboard API" "FastAPI\|api_server"

echo ""
echo "🗂️ Checking Kubernetes Configuration..."
echo "----------------------------------------"
check_file "config/base/kustomization.yaml" "Base Kustomize" "kustomize"
check_file "config/overlays/development/kustomization.yaml" "Dev Overlay" "development\|dev"
check_file "config/overlays/staging/kustomization.yaml" "Staging Overlay" "staging"
check_file "config/overlays/production/kustomization.yaml" "Prod Overlay" "production\|prod"
check_file "k8s/ray-cluster.yaml" "Ray Cluster" "ray.io"
check_file "k8s/hpa.yaml" "HPA Config" "autoscaling"
check_file "k8s/network-policy.yaml" "Network Policies" "NetworkPolicy"

echo ""
echo "🔄 Checking CI/CD Pipeline..."
echo "-----------------------------"
check_file ".github/workflows/build.yml" "Build Workflow" "workflow\|build"
check_file ".github/workflows/k8s-tests.yml" "K8s Tests Workflow" "kubernetes\|k8s"
check_file ".github/workflows/deploy.yml" "Deploy Workflow" "deploy"
check_file "Dockerfile" "Dockerfile" "FROM"

echo ""
echo "🧪 Checking Test Suite..."
echo "-------------------------"
check_file "tests/unit/" "Unit Tests Directory" ""
check_file "tests/integration/" "Integration Tests Directory" ""
check_file "tests/k8s/" "K8s Tests Directory" ""
check_file "tests/e2e/" "E2E Tests Directory" ""
check_file "tests/requirements-tests.txt" "Test Dependencies" "pytest"

echo ""
echo "📚 Checking Documentation..."
echo "----------------------------"
check_file "docs/ARCHITECTURE.md" "Architecture Docs" "architecture"
check_file "docs/DEPLOYMENT.md" "Deployment Guide" "deploy"
check_file "docs/API_REFERENCE.md" "API Reference" "api"
check_file "docs/TESTING.md" "Testing Guide" "test"

echo ""
echo "======================================"
echo "📊 Comparison Results:"
echo "  Total checks: $TOTAL"
echo -e "  ${GREEN}Matched: $MATCHED${NC}"
echo -e "  ${YELLOW}Partial: $PARTIAL${NC}"
echo -e "  ${RED}Missing: $MISSING${NC}"
echo ""

if [ $MISSING -eq 0 ] && [ $PARTIAL -eq 0 ]; then
    echo -e "${GREEN}✅ Repository matches v5.0.0 specifications!${NC}"
    exit 0
elif [ $MISSING -lt 5 ] && [ $PARTIAL -lt 5 ]; then
    echo -e "${YELLOW}⚠️  Repository is mostly aligned. Minor updates needed.${NC}"
    exit 1
else
    echo -e "${RED}❌ Repository needs significant updates to match v5.0.0.${NC}"
    exit 2
fi
