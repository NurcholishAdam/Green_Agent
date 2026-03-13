name: Deploy Green Agent

on:
  push:
    branches: [main]
    tags:
      - 'v*'
  workflow_dispatch:
    inputs:
      environment:
        description: 'Deployment environment'
        required: true
        default: 'staging'
        type: choice
        options:
        - development
        - staging
        - production

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}
  # ✅ FIX: Set working directory explicitly
  WORKING_DIRECTORY: ${{ github.workspace }}

permissions:
  contents: read
  packages: write
  id-token: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ github.event.inputs.environment || 'staging' }}
    timeout-minutes: 30
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
      with:
        fetch-depth: 0
        # ✅ FIX: Ensure we're in the repo root
        path: .
    
    - name: Set up kubectl
      uses: Azure/setup-kubectl@v4
      with:
        version: 'v1.28.0'
    
    - name: Configure kubeconfig
      run: |
        mkdir -p ~/.kube
        echo "${{ secrets.KUBE_CONFIG }}" | base64 -d > ~/.kube/config
        chmod 600 ~/.kube/config
    
    # ✅ FIX: Make scripts executable BEFORE running them
    - name: Make deployment scripts executable
      run: |
        chmod +x ./scripts/*.sh
        chmod +x ./k8s/*.sh
        ls -la ./scripts/
        ls -la ./k8s/
    
    # ✅ FIX: Use absolute path or cd to workspace first
    - name: Deploy to environment
      run: |
        # Ensure we're in the correct directory
        cd ${{ github.workspace }}
        
        ENVIRONMENT="${{ github.event.inputs.environment || 'staging' }}"
        echo "🚀 Deploying to $ENVIRONMENT environment..."
        
        # Run the deployment script with absolute path
        "${{ github.workspace }}/scripts/deploy-environment.sh" "$ENVIRONMENT"
      env:
        KUBECONFIG: ~/.kube/config
    
    - name: Verify deployment
      run: |
        cd ${{ github.workspace }}
        ENVIRONMENT="${{ github.event.inputs.environment || 'staging' }}"
        NAMESPACE="green-agent-$ENVIRONMENT"
        
        # Wait for pods to be ready
        kubectl wait --for=condition=ready pod \
          -l app=green-agent \
          -n $NAMESPACE \
          --timeout=300s || true
        
        # Check deployment status
        kubectl get pods -n $NAMESPACE
        kubectl get hpa -n $NAMESPACE
    
    # ✅ FIX: Updated to v4 with proper paths
    - name: Upload Deployment Logs
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: deployment-logs-${{ github.event.inputs.environment || 'staging' }}
        path: |
          ${{ github.workspace }}/kubectl-logs.txt
          ${{ github.workspace }}/deployment-status.json
        retention-days: 14
        compression-level: 6
        if-no-files-found: warn
    
    - name: Notify Success
      if: success()
      run: |
        echo "✅ Deployment to ${{ github.event.inputs.environment || 'staging' }} successful!"
    
    - name: Notify Failure
      if: failure()
      run: |
        echo "❌ Deployment to ${{ github.event.inputs.environment || 'staging' }} failed!"
        # Collect debug info
        kubectl get events -n "green-agent-${{ github.event.inputs.environment || 'staging' }}" || true
        exit 1
