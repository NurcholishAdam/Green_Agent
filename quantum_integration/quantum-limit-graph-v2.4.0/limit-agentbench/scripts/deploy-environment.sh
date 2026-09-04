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
      enable_enhancements:
        description: 'Enable advanced enhancements (LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio‑inspired)'
        required: false
        default: false
        type: boolean

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}
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

    - name: Make deployment scripts executable
      run: |
        chmod +x ./scripts/*.sh
        chmod +x ./k8s/*.sh
        ls -la ./scripts/
        ls -la ./k8s/

    - name: Deploy to environment
      run: |
        cd ${{ github.workspace }}
        ENVIRONMENT="${{ github.event.inputs.environment || 'staging' }}"
        echo "🚀 Deploying to $ENVIRONMENT environment..."

        # Build deploy command with optional enhancements flag
        DEPLOY_ARGS="$ENVIRONMENT"
        if [[ "${{ github.event.inputs.enable_enhancements }}" == "true" ]]; then
          echo "✨ Enhancements enabled"
          DEPLOY_ARGS="$DEPLOY_ARGS --enhancements"
        fi

        "${{ github.workspace }}/scripts/deploy-environment.sh" $DEPLOY_ARGS
      env:
        KUBECONFIG: ~/.kube/config

    - name: Verify deployment
      run: |
        cd ${{ github.workspace }}
        ENVIRONMENT="${{ github.event.inputs.environment || 'staging' }}"
        NAMESPACE="green-agent-$ENVIRONMENT"

        kubectl wait --for=condition=ready pod \
          -l app=green-agent \
          -n $NAMESPACE \
          --timeout=300s || true

        kubectl get pods -n $NAMESPACE
        kubectl get hpa -n $NAMESPACE

    - name: Upload Deployment Logs
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: deployment-logs-${{ github.event.inputs.environment || 'staging' }}${{ github.event.inputs.enable_enhancements == 'true' && '-enhanced' || '' }}
        path: |
          ${{ github.workspace }}/kubectl-logs.txt
          ${{ github.workspace }}/deployment-status.json
        retention-days: 14
        compression-level: 6
        if-no-files-found: warn

    - name: Notify Success
      if: success()
      run: |
        ENHANCEMENT_MSG=""
        if [[ "${{ github.event.inputs.enable_enhancements }}" == "true" ]]; then
          ENHANCEMENT_MSG=" with enhancements"
        fi
        echo "✅ Deployment to ${{ github.event.inputs.environment || 'staging' }}$ENHANCEMENT_MSG successful!"

    - name: Notify Failure
      if: failure()
      run: |
        ENHANCEMENT_MSG=""
        if [[ "${{ github.event.inputs.enable_enhancements }}" == "true" ]]; then
          ENHANCEMENT_MSG=" with enhancements"
        fi
        echo "❌ Deployment to ${{ github.event.inputs.environment || 'staging' }}$ENHANCEMENT_MSG failed!"
        kubectl get events -n "green-agent-${{ github.event.inputs.environment || 'staging' }}" || true
        exit 1
