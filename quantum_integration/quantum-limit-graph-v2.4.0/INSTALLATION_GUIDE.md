# Green Agent Advanced Modules - Installation & Deployment Guide

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Git
- Docker (optional, for containerized deployment)
- PostgreSQL (for metrics storage)
- Redis (for caching)

### 1. Clone Repository

```bash
git clone https://github.com/NurcholishAdam/Green_Agent.git
cd Green_Agent
```

### 2. Install Dependencies

#### Option A: Install All Modules (Recommended)
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install all requirements
pip install -r requirements/base.txt
pip install -r requirements/distributed.txt
pip install -r requirements/dashboard.txt
pip install -r requirements/quantum.txt

# Additional dependencies for carbon forecasting
pip install prophet
pip install sentence-transformers  # For semantic scoring
```

#### Option B: Install Specific Modules Only
```bash
# Just distributed runtime
pip install -r requirements/distributed.txt

# Just dashboard
pip install -r requirements/dashboard.txt

# Just quantum metrics
pip install -r requirements/quantum.txt
```

### 3. Place Module Files in Repository

Copy the advanced module files to their respective locations:

```bash
# Distributed Runtime
cp ray_cluster_manager.py src/distributed/
cp carbon_aware_scheduler.py src/distributed/

# Carbon Intelligence
cp forecasting_engine.py src/carbon/
cp eco_mode_controller.py src/carbon/

# Create __init__.py files
touch src/distributed/__init__.py
touch src/carbon/__init__.py
```

### 4. Configure Services

#### Ray Cluster Configuration
Create `config/ray_cluster.yaml`:
```yaml
cluster_name: green_agent_cluster
num_workers: 4
dashboard_host: 0.0.0.0
dashboard_port: 8265
object_store_memory_gb: 4
```

#### Carbon Regions Configuration
Create `config/carbon_regions.json`:
```json
{
  "US-CA": {
    "name": "California",
    "grid_api": "watttime",
    "timezone": "America/Los_Angeles"
  },
  "US-NY": {
    "name": "New York",
    "grid_api": "watttime",
    "timezone": "America/New_York"
  },
  "EU-DE": {
    "name": "Germany",
    "grid_api": "electricitymap",
    "timezone": "Europe/Berlin"
  }
}
```

### 5. Initialize Ray Cluster

```bash
# Start Ray head node
ray start --head --port=6379 --dashboard-host=0.0.0.0 --dashboard-port=8265

# On worker nodes (if multi-machine):
ray start --address='<head-node-ip>:6379'

# Verify cluster
ray status
```

### 6. Test Installation

```python
# test_installation.py
import asyncio
from src.distributed.ray_cluster_manager import create_ray_cluster
from src.carbon.forecasting_engine import create_forecaster
from src.carbon.eco_mode_controller import create_eco_mode_controller

async def test():
    # Test Ray cluster
    cluster = create_ray_cluster(num_workers=2)
    print(f"Ray cluster: {cluster.get_cluster_info()}")
    
    # Test carbon forecaster
    forecaster = await create_forecaster(region="US-CA", train_days=30)
    forecasts = await forecaster.predict(horizon="6h")
    print(f"Carbon forecasts: {len(forecasts)} points")
    
    # Test eco-mode controller
    controller = create_eco_mode_controller(forecaster)
    recommendation = await controller.get_current_recommendation()
    print(f"Eco-mode: {recommendation['recommended_mode']}")
    
    cluster.shutdown()
    print("✅ All modules working!")

asyncio.run(test())
```

Run test:
```bash
python test_installation.py
```

---

## 📁 Complete Repository Structure

After installation, your repository should look like:

```
Green_Agent/
├── src/
│   ├── distributed/          # ✅ NEW
│   │   ├── __init__.py
│   │   ├── ray_cluster_manager.py
│   │   ├── carbon_aware_scheduler.py
│   │   ├── distributed_retrieval.py
│   │   └── agent_pool.py
│   │
│   ├── carbon/               # ✅ NEW
│   │   ├── __init__.py
│   │   ├── forecasting_engine.py
│   │   ├── eco_mode_controller.py
│   │   ├── grid_api_clients.py
│   │   └── carbon_budget_manager.py
│   │
│   ├── retrieval/            # EXISTING + ENHANCEMENTS
│   │   ├── topological_allocator.py       # ✅ NEW
│   │   ├── semantic_scorer.py             # ✅ NEW
│   │   ├── enhanced_graph_traversal.py    # ✅ NEW
│   │   ├── vimrag_integration.py          # ✅ UPDATE
│   │   └── ...                            # Existing files
│   │
│   ├── orchestration/        # ✅ NEW
│   ├── services/             # ✅ NEW
│   ├── dashboard/            # ✅ NEW
│   ├── market/               # ✅ NEW
│   ├── quantum/              # ✅ NEW
│   └── ...
│
├── config/                   # ✅ NEW
│   ├── ray_cluster.yaml
│   ├── carbon_regions.json
│   └── dashboard_config.yaml
│
├── requirements/             # ✅ NEW
│   ├── base.txt
│   ├── distributed.txt
│   ├── dashboard.txt
│   └── quantum.txt
│
└── ...
```

---

## 🔧 Module-Specific Setup

### 1. Distributed Runtime (Ray)

**Start Ray Cluster:**
```bash
./scripts/setup/setup_ray_cluster.sh
```

**Test Ray Integration:**
```python
from src.distributed.ray_cluster_manager import create_ray_cluster

# Create cluster with 4 workers
cluster = create_ray_cluster(num_workers=4)

# Create tasks
tasks = [{"task_id": f"task_{i}", "query": f"Query {i}"} for i in range(10)]

# Execute distributed
results = await cluster.execute_distributed_tasks(tasks, agent_type="retriever")

print(f"Completed {len(results)} tasks")
cluster.shutdown()
```

**Ray Dashboard:**
Access at `http://localhost:8265`

---

### 2. Carbon Forecasting + Eco-Mode

**Train Forecaster:**
```python
from src.carbon.forecasting_engine import create_forecaster

# Train on 90 days of data
forecaster = await create_forecaster(region="US-CA", train_days=90)

# Predict next 24 hours
forecasts = await forecaster.predict(horizon="24h", interval_minutes=60)

# Find optimal execution window
optimal = await forecaster.find_optimal_execution_window(
    duration_hours=2.0,
    deadline=datetime.now() + timedelta(hours=48)
)
print(f"Optimal window: {optimal.start_time} to {optimal.end_time}")
```

**Use Eco-Mode Controller:**
```python
from src.carbon.eco_mode_controller import create_eco_mode_controller

controller = create_eco_mode_controller(forecaster)

# Get current recommendation
recommendation = await controller.get_current_recommendation()
print(f"Recommended mode: {recommendation['recommended_mode']}")

# Apply throttling to task
task = {"max_tokens": 1000, "temperature": 0.7}
decision = await controller.apply_throttling(task)
print(f"Throttled task: {decision.throttled_task}")
```

---

### 3. Carbon-Aware Ray Cluster

**Combine Ray + Carbon Intelligence:**
```python
from src.distributed.carbon_aware_scheduler import create_carbon_aware_cluster

# Create carbon-aware cluster
cluster = create_carbon_aware_cluster(
    num_workers=4,
    carbon_forecaster=forecaster
)

# Schedule and execute with carbon awareness
results = await cluster.schedule_and_execute(
    tasks=tasks,
    agent_type="retriever",
    task_energy_estimate_kwh=0.002
)

# Get statistics
stats = await cluster.get_scheduler_stats()
print(f"Carbon saved: {stats['carbon_saved_kgco2e']:.4f} kgCO2e")
```

---

## 🐳 Docker Deployment

### Build Images

```bash
# Ray worker
docker build -f docker/Dockerfile.worker -t green-agent-worker .

# Dashboard API
docker build -f docker/Dockerfile.api -t green-agent-api .

# VimRAG service
docker build -f docker/Dockerfile.vimrag -t green-agent-vimrag .
```

### Run with Docker Compose

```bash
# Start all services
docker-compose -f docker/docker-compose.yml up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

Services will be available at:
- Ray Dashboard: `http://localhost:8265`
- Green Agent API: `http://localhost:8000`
- VimRAG Service: `http://localhost:8001`

---

## ☸️ Kubernetes Deployment

### Deploy to Kubernetes

```bash
# Create namespace
kubectl create namespace green-agent

# Deploy Ray cluster
kubectl apply -f scripts/deployment/kubernetes/ray-cluster.yaml

# Deploy VimRAG service
kubectl apply -f scripts/deployment/kubernetes/vimrag-deployment.yaml

# Deploy dashboard
kubectl apply -f scripts/deployment/kubernetes/dashboard-deployment.yaml

# Check status
kubectl get pods -n green-agent
```

---

## 📊 Usage Examples

### Example 1: Simple Distributed Retrieval

```python
import asyncio
from src.distributed.ray_cluster_manager import create_ray_cluster

async def main():
    cluster = create_ray_cluster(num_workers=4)
    
    # 100 parallel retrieval tasks
    tasks = [
        {"task_id": f"task_{i}", "query": f"What is {topic}?", "top_k": 5}
        for i, topic in enumerate(["AI", "ML", "NLP", ...])
    ]
    
    results = await cluster.execute_distributed_tasks(tasks, agent_type="retriever")
    
    print(f"Completed {len(results)} tasks")
    print(f"Total energy: {sum(r['metrics']['energy_kwh'] for r in results):.4f} kWh")
    
    cluster.shutdown()

asyncio.run(main())
```

### Example 2: Carbon-Aware Workload Scheduling

```python
from src.carbon.forecasting_engine import create_forecaster
from src.distributed.carbon_aware_scheduler import create_carbon_aware_cluster

async def main():
    # Train forecaster
    forecaster = await create_forecaster(region="US-CA", train_days=30)
    
    # Create carbon-aware cluster
    cluster = create_carbon_aware_cluster(num_workers=8, carbon_forecaster=forecaster)
    
    # Tasks with deferrability
    tasks = [
        {
            "task_id": f"task_{i}",
            "query": f"Query {i}",
            "deferrable": i % 2 == 0,  # 50% deferrable
            "estimated_energy_kwh": 0.001
        }
        for i in range(100)
    ]
    
    # Schedule and execute with carbon optimization
    results = await cluster.schedule_and_execute(tasks, agent_type="retriever")
    
    # Get carbon savings
    stats = await cluster.get_scheduler_stats()
    print(f"✅ Carbon saved: {stats['carbon_saved_kgco2e']:.4f} kgCO2e")
    print(f"✅ Savings: {stats['carbon_saved_percent']:.1f}%")

asyncio.run(main())
```

### Example 3: Eco-Mode Adaptive Execution

```python
from src.carbon.eco_mode_controller import create_eco_mode_controller

async def main():
    forecaster = await create_forecaster(region="US-CA")
    controller = create_eco_mode_controller(forecaster)
    
    # Monitor eco-mode over time
    for _ in range(24):  # 24 hours
        recommendation = await controller.get_current_recommendation()
        
        print(f"Hour {_}:")
        print(f"  Carbon intensity: {recommendation['current_intensity_gco2kwh']:.0f} gCO2/kWh")
        print(f"  Recommended mode: {recommendation['recommended_mode']}")
        print(f"  Compute limit: {recommendation['compute_limit_percent']}%")
        
        # Wait 1 hour
        await asyncio.sleep(3600)

asyncio.run(main())
```

---

## 🧪 Testing

### Run Unit Tests

```bash
# Test distributed module
pytest tests/unit/test_distributed/

# Test carbon module
pytest tests/unit/test_carbon/

# Test all modules
pytest tests/
```

### Run Integration Tests

```bash
# Test complete pipeline
pytest tests/integration/test_complete_pipeline.py

# Test carbon-aware scheduling
pytest tests/integration/test_carbon_orchestrator.py
```

### Run Benchmarks

```bash
# Energy efficiency benchmark
python scripts/benchmarking/run_complete_benchmark.py

# Carbon reduction benchmark
python scripts/benchmarking/run_carbon_comparison.py
```

---

## 📈 Monitoring & Observability

### Access Ray Dashboard
```bash
# Local
http://localhost:8265

# Remote (with port forwarding)
ssh -L 8265:localhost:8265 user@remote-host
```

### View Logs

```bash
# Ray logs
tail -f /tmp/ray/session_latest/logs/raylet.out

# Application logs
tail -f logs/green_agent.log
```

### Metrics

Monitor key metrics:
- **Energy consumption** (kWh)
- **Carbon emissions** (kgCO2e)
- **Carbon savings** (% vs baseline)
- **Task throughput** (tasks/sec)
- **Eco-mode distribution** (time in each mode)

---

## 🐛 Troubleshooting

### Issue: Ray cluster won't start
```bash
# Check if Ray is running
ray status

# Kill existing Ray processes
ray stop

# Restart with verbose logging
ray start --head --verbose
```

### Issue: Import errors
```bash
# Reinstall dependencies
pip install --upgrade -r requirements/distributed.txt

# Check Python path
python -c "import sys; print(sys.path)"
```

### Issue: Carbon forecasting fails
```bash
# Verify Prophet installation
python -c "from prophet import Prophet; print('Prophet OK')"

# Reinstall Prophet
pip uninstall prophet
pip install prophet
```

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/your-feature`
3. Add tests for new functionality
4. Ensure all tests pass: `pytest tests/`
5. Submit pull request

---

## 📚 Additional Resources

- **Ray Documentation**: https://docs.ray.io/
- **Prophet Documentation**: https://facebook.github.io/prophet/
- **Green Agent Paper**: [Link to paper when published]
- **Carbon Intensity APIs**:
  - WattTime: https://www.watttime.org/api-documentation/
  - ElectricityMap: https://www.electricitymap.org/

---

## 📝 License

MIT License - See LICENSE file for details

---

## 🌟 Acknowledgments

- Ray team for distributed runtime
- Facebook for Prophet forecasting library
- WattTime and ElectricityMap for carbon intensity data
- Green Agent community contributors

---

**Last Updated**: March 2026
**Version**: 2.5.0-advanced-modules
