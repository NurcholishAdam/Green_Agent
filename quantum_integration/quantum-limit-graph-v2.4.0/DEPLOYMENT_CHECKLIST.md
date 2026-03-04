# Green Agent Advanced Modules - Deployment Checklist

## 📋 Pre-Deployment Checklist

### System Requirements
- [ ] Python 3.9+ installed
- [ ] pip package manager updated
- [ ] 8GB+ RAM available
- [ ] 20GB+ disk space
- [ ] Internet connection for API calls

### Dependencies
- [ ] Ray installed (`pip install ray[default]>=2.9.0`)
- [ ] Prophet installed (`pip install prophet`)
- [ ] NetworkX installed (`pip install networkx>=3.1`)
- [ ] FastAPI installed (`pip install fastapi uvicorn`)
- [ ] Plotly Dash installed (`pip install dash plotly`)

### Optional Services
- [ ] PostgreSQL (for metrics storage)
- [ ] Redis (for caching)
- [ ] Docker (for containerized deployment)

---

## 🚀 Deployment Steps

### Step 1: Clone Repository
```bash
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent
```
- [ ] Repository cloned successfully
- [ ] Current directory is Green_Agent root

### Step 2: Create Directory Structure
```bash
# Create new module directories
mkdir -p src/distributed
mkdir -p src/carbon
mkdir -p src/orchestration
mkdir -p src/services
mkdir -p src/dashboard
mkdir -p src/market
mkdir -p src/quantum
mkdir -p config
mkdir -p requirements
```
- [ ] All directories created
- [ ] No permission errors

### Step 3: Copy Module Files
```bash
# Copy distributed runtime files
cp path/to/ray_cluster_manager.py src/distributed/
cp path/to/carbon_aware_scheduler.py src/distributed/

# Copy carbon intelligence files
cp path/to/forecasting_engine.py src/carbon/
cp path/to/eco_mode_controller.py src/carbon/

# Create __init__.py files
touch src/distributed/__init__.py
touch src/carbon/__init__.py
```
- [ ] All files copied successfully
- [ ] `__init__.py` files created
- [ ] No file conflicts

### Step 4: Install Dependencies
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements/base.txt
pip install -r requirements/distributed.txt
pip install -r requirements/dashboard.txt
pip install -r requirements/quantum.txt
```
- [ ] Virtual environment activated
- [ ] All requirements installed
- [ ] No dependency conflicts

### Step 5: Initialize Ray Cluster
```bash
# Start Ray head node (local)
ray start --head --port=6379 --dashboard-host=0.0.0.0 --dashboard-port=8265

# Verify cluster
ray status
```
- [ ] Ray head node started
- [ ] Dashboard accessible at http://localhost:8265
- [ ] No errors in Ray logs

### Step 6: Test Installation
```bash
python test_installation.py
```
Expected output:
```
Ray cluster: {'status': 'active', 'num_nodes': 1, ...}
Carbon forecasts: 24 points
Eco-mode: balanced
✅ All modules working!
```
- [ ] All tests passed
- [ ] No import errors
- [ ] Metrics collected successfully

### Step 7: Configure Services

#### Ray Cluster Config (`config/ray_cluster.yaml`)
```yaml
cluster_name: green_agent_cluster
num_workers: 4
dashboard_host: 0.0.0.0
dashboard_port: 8265
object_store_memory_gb: 4
```
- [ ] Config file created
- [ ] Values customized for environment

#### Carbon Regions Config (`config/carbon_regions.json`)
```json
{
  "US-CA": {
    "name": "California",
    "grid_api": "watttime",
    "timezone": "America/Los_Angeles"
  }
}
```
- [ ] Config file created
- [ ] Region configured

### Step 8: Run Example Workflow
```bash
python examples/basic_usage/02_carbon_aware_execution.py
```
- [ ] Example runs successfully
- [ ] Carbon savings calculated
- [ ] No errors or warnings

---

## ✅ Post-Deployment Validation

### Functional Tests
- [ ] Ray cluster operational (`ray status`)
- [ ] Carbon forecaster trained (no errors)
- [ ] Eco-mode controller responsive
- [ ] Distributed tasks execute successfully
- [ ] Energy metrics collected
- [ ] Carbon metrics calculated

### Performance Tests
- [ ] Task execution time < 500ms (p95)
- [ ] Ray cluster utilization > 70%
- [ ] Carbon forecasting accuracy > 80%
- [ ] Energy reduction > 50% vs baseline
- [ ] No memory leaks (24hr stress test)

### Integration Tests
- [ ] Ray + Carbon forecaster integration
- [ ] Carbon scheduler + Eco-mode integration
- [ ] VimRAG + Distributed execution integration
- [ ] End-to-end pipeline functional

---

## 🐛 Troubleshooting

### Issue 1: Ray won't start
**Symptoms**: `ray start` fails or times out
**Solutions**:
```bash
# Kill existing processes
ray stop
pkill -9 ray

# Clean Ray directory
rm -rf /tmp/ray

# Restart with verbose logging
ray start --head --verbose --log-to-driver
```
- [ ] Ray processes killed
- [ ] Clean restart successful

### Issue 2: Import errors
**Symptoms**: `ModuleNotFoundError`
**Solutions**:
```bash
# Verify Python path
python -c "import sys; print('\n'.join(sys.path))"

# Reinstall packages
pip install --force-reinstall -r requirements/distributed.txt

# Add src to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```
- [ ] Python path correct
- [ ] Packages reinstalled
- [ ] PYTHONPATH configured

### Issue 3: Carbon forecasting fails
**Symptoms**: Prophet errors or NaN predictions
**Solutions**:
```bash
# Verify Prophet installation
python -c "from prophet import Prophet; print('Prophet OK')"

# Reinstall with dependencies
pip uninstall prophet
pip install prophet --no-cache-dir

# Check historical data
python -c "from src.carbon.forecasting_engine import CarbonForecaster; f = CarbonForecaster(); print(len(f.historical_data))"
```
- [ ] Prophet working
- [ ] Historical data loaded
- [ ] Predictions generating

### Issue 4: Out of memory
**Symptoms**: Tasks failing with OOM errors
**Solutions**:
```bash
# Reduce Ray object store
ray stop
ray start --head --object-store-memory=2000000000  # 2GB

# Reduce agent pool size
# In code: create_ray_cluster(num_workers=2)  # instead of 4

# Monitor memory usage
watch -n 1 'free -h'
```
- [ ] Ray restarted with lower memory
- [ ] Agent pool size reduced
- [ ] Memory usage stable

---

## 📊 Monitoring

### Daily Checks
- [ ] Ray dashboard accessible
- [ ] No crashed workers
- [ ] Carbon forecaster updated (retrained every 7 days)
- [ ] Eco-mode controller responsive
- [ ] Disk space > 10% free

### Weekly Checks
- [ ] Review carbon savings metrics
- [ ] Check for Ray version updates
- [ ] Analyze eco-mode distribution
- [ ] Review task failure rate
- [ ] Benchmark performance regression

### Monthly Checks
- [ ] Retrain carbon forecaster with fresh data
- [ ] Review and optimize eco-mode thresholds
- [ ] Update dependencies (`pip list --outdated`)
- [ ] Review and archive logs
- [ ] Capacity planning (scale cluster if needed)

---

## 🔄 Rollback Plan

### If Deployment Fails
1. Stop all services
```bash
ray stop
```

2. Restore previous version
```bash
git checkout <previous-commit-hash>
```

3. Reinstall dependencies
```bash
pip install -r requirements/distributed.txt
```

4. Restart services
```bash
ray start --head
```

### Backup Important Data
- [ ] Config files backed up
- [ ] Trained models backed up
- [ ] Metrics database backed up
- [ ] Logs archived

---

## 📝 Sign-Off

### Deployment Team
- [ ] Developer: _____________________ Date: _________
- [ ] Reviewer: _____________________ Date: _________
- [ ] Operations: ___________________ Date: _________

### Approvals
- [ ] All tests passed
- [ ] Documentation complete
- [ ] Monitoring configured
- [ ] Rollback plan verified
- [ ] Production ready

---

**Deployment Version**: 2.5.0-alpha  
**Deployment Date**: _______________  
**Environment**: [ ] Development [ ] Staging [ ] Production  
**Status**: [ ] Success [ ] Failed [ ] Rolled Back  

---

## 📞 Support Contacts

- **Technical Issues**: Create issue on GitHub
- **Emergency**: [Add contact info]
- **Documentation**: See INSTALLATION_GUIDE.md

---

**Last Updated**: March 2026
