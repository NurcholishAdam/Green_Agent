# 1. Copy all modules to Green Agent directory
cd Green_Agent/

# 2. Create helium modules
mkdir -p src/interpretation src/governance src/decision src/optimization src/distributed src/carbon src/integration

# 3. Copy all files as shown above

# 4. Install additional dependencies
pip install aiohttp numpy pandas

# 5. Update configuration (config/base/green_agent_config.yaml)
cat >> config/base/green_agent_config.yaml << EOF
# Helium awareness configuration
helium_aware_enabled: true
helium_api_url: "https://api.helium-monitor.example.com/v1"
helium_update_interval: 300
helium_weight: 0.4
carbon_weight: 0.6

# Helium thresholds
helium_yellow_threshold: 0.6
helium_red_threshold: 0.8
helium_critical_threshold: 0.95

# Fallback configuration
fallback_enabled: true
execution_timeout: 300
EOF

# 6. Run integration test
python -c "
import asyncio
from src.integration.unified_orchestrator import UnifiedOrchestrator

async def test():
    orch = UnifiedOrchestrator()
    task = {
        'task_id': 'test_001',
        'hardware_requirements': {'gpu_count': 2},
        'model_config': {'size_gb': 10},
        'deferrable': True
    }
    result = await orch.process_task(task)
    print(result)

asyncio.run(test())
"

# 7. Start dashboard
uvicorn dashboard.helium_dashboard:router --host 0.0.0.0 --port 8001
