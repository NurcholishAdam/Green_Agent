# Configuration

This directory contains all configuration files for the Green Agent system.

## Structure

### Main Configuration
- `default.yaml` - Default configuration values
- `green_agent.yaml` - Main Green Agent configuration

### Environment Configurations
- `environments/.env.development` - Development environment
- `environments/.env.staging` - Staging environment
- `environments/.env.production` - Production environment

### Kubernetes Manifests
- `k8s/base/` - Base Kubernetes manifests
- `k8s/overlays/development/` - Development overlay
- `k8s/overlays/staging/` - Staging overlay
- `k8s/overlays/production/` - Production overlay

### Monitoring
- `alerts/green_agent_alerts.yml` - Prometheus alerting rules
- `alerts/grafana-dashboard.json` - Grafana dashboard

## Loading Configuration

The system automatically loads configuration in this order:
1. `default.yaml` (base values)
2. `green_agent.yaml` (overrides)
3. Environment variables (highest priority)

## Environment Variables

Prefix-based configuration:
- `GA_*` - Core agent settings
- `FLEXGEN_*` - FLexGen configuration
- `MODP_*` - MODP configuration
- `LIMIT_GRAPH_*` - LIMIT Graph configuration
- `MoE_*` - Mixture of Experts configuration
- `RLHF_*` - RLHF configuration
- etc.

## See Also

- `src/enhancements/config.yaml` - Enhancement module configuration
- `scripts/` - Configuration management scripts
