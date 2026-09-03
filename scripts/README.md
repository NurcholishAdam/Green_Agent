# Scripts and Utilities

This directory contains scripts for setup, deployment, and management of the Green Agent system.

## Core Scripts

### Setup & Installation
- `setup.sh` - Initial setup and dependency installation
- `verify.sh` - Verify installation and dependencies
- `cleanup.sh` - Clean up resources and temporary files

### Deployment
- `deploy.sh` - Deploy to Kubernetes cluster
- `monitoring/prometheus.yml` - Prometheus configuration
- `monitoring/grafana-dashboard.json` - Grafana dashboard

## Features

### setup.sh
```bash
./scripts/setup.sh
```
- Creates Python virtual environment
- Installs dependencies
- Configures local development environment
- Initializes database (if needed)

### deploy.sh
```bash
./scripts/deploy.sh [environment]
```
Environments: `development`, `staging`, `production`

Deployment process:
1. Validates configuration
2. Builds Docker image (if needed)
3. Applies Kubernetes manifests from `config/k8s/overlays/[environment]/`
4. Waits for deployment to be ready
5. Runs post-deployment tests

### verify.sh
```bash
./scripts/verify.sh
```
Verifies:
- Python environment setup
- Dependencies installed
- Configuration valid
- Local services running (if applicable)

### cleanup.sh
```bash
./scripts/cleanup.sh
```
Cleans up:
- Temporary files
- Cache directories
- Virtual environment (optional)
- Docker images (optional)

## Environment-Specific Setup

Load environment variables:
```bash
source config/environments/.env.development
```

Configuration flow:
```
scripts/setup.sh
    ↓
config/environments/.env.[environment]
    ↓
config/green_agent.yaml
    ↓
src/enhancements/config.yaml
    ↓
Application initialized
```

## Monitoring Setup

1. Copy monitoring configs:
   ```bash
   cp scripts/monitoring/* /path/to/monitoring/
   ```

2. Update Prometheus targets for your environment

3. Import Grafana dashboard

## Integration with Config/Enhancements

Scripts automatically:
- Load all configurations from `config/`
- Apply enhancement module settings from `src/enhancements/config.yaml`
- Use K8s overlays specific to deployed environment
- Initialize monitoring based on `scripts/monitoring/` configs

## Adding New Scripts

1. Create script in this directory
2. Add `#!/bin/bash` shebang
3. Add documentation header
4. Make executable: `chmod +x scripts/your_script.sh`
5. Test in development environment first
