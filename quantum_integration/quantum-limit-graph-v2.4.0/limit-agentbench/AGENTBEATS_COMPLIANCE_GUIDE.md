# AgentBeats Compliance Guide

Complete guide for AgentBeats-ready Green_Agent architecture implementation.

## 🏆 Four Pillars of AgentBeats Compliance

### 1. A2A Protocol Compliance ✅

**Implementation:** `core/a2a_gateway.py`

The A2A Gateway ensures all agent interactions conform to the Agent-to-Agent protocol standard:

#### Features:
- **Request Validation**: Validates incoming tasks against A2A JSON schema
- **Response Transformation**: Converts agent outputs to A2A-standard format
- **Version Support**: Handles v1.0 and v1.1 with backward compatibility
- **Error Formatting**: Even failures return properly formatted A2A responses

#### Usage:

```python
from core.a2a_gateway import A2AGateway, create_a2a_task

# Initialize gateway
gateway = A2AGateway()

# Create A2A task
task = create_a2a_task(
    task_id="task_001",
    task_type="research",
    query="What are the benefits of quantum computing?",
    max_tokens=500,
    timeout_seconds=30
)

# Validate request
validated = gateway.validate_request(task)

# Execute agent (your implementation)
agent_output = your_agent.execute(validated.input_data)

# Transform to A2A response
response = gateway.transform_agent_output(
    task_id=validated.task_id,
    agent_output=agent_output,
    execution_time=2.5,
    green_metrics={"energy_kwh": 0.042},
    reasoning_trace=[...]
)

# Get A2A-compliant dictionary
a2a_response = response.to_dict()
```

#### A2A Request Schema:

```json
{
  "task_id": "unique_identifier",
  "task_type": "research|qa|summarization|...",
  "input_data": {
    "query": "User question or task description"
  },
  "constraints": {
    "max_tokens": 500,
    "timeout_seconds": 30
  },
  "version": "1.1"
}
```

#### A2A Response Schema:

```json
{
  "task_id": "unique_identifier",
  "status": "success|failure|timeout|...",
  "output": {
    "result": "Agent response"
  },
  "execution_time_seconds": 2.5,
  "green_metrics": {
    "energy_kwh": 0.042,
    "carbon_kg": 0.018,
    "sustainability_index": 0.73
  },
  "reasoning_trace": [...],
  "timestamp": "2026-01-20T10:30:00Z",
  "version": "1.1"
}
```

---

### 2. Independent Execution ✅

**Implementation:** `core/docker_orchestrator.py`

Fully automated container lifecycle management with zero manual intervention.

#### Features:
- **Docker Orchestration**: Automated container launch and cleanup
- **Resource Isolation**: CPU, memory, and GPU limits enforced
- **Self-contained**: All dependencies bundled in Docker image
- **JSON I/O**: Tasks execute from JSON input to JSON output

#### Docker Configuration:

```python
from core.docker_orchestrator import DockerOrchestrator, ContainerConfig

# Initialize orchestrator
orchestrator = DockerOrchestrator(work_dir="./work")

# Configure container
config = ContainerConfig(
    image="limit-graph-agent:latest",
    cpu_limit="2.0",
    memory_limit="4g",
    gpu_limit="1",
    timeout_seconds=300
)

# Execute task in container
response = orchestrator.execute_task(
    task_request=a2a_task,
    config=config
)
```

#### Building Agent Container:

```bash
# Create Dockerfile
python -c "from core.docker_orchestrator import create_dockerfile; create_dockerfile()"

# Build image
docker build -t limit-graph-agent:latest -f Dockerfile.agent .

# Test locally
docker run --rm \
  -v $(pwd)/input.json:/app/input.json:ro \
  -v $(pwd)/output.json:/app/output.json:rw \
  --cpus 2.0 \
  --memory 4g \
  limit-graph-agent:latest
```

#### Execution Lifecycle:

1. **Container Launch**: Orchestrator launches container from A2A task JSON
2. **Initialization**: Agent loads task and initializes components
3. **Autonomous Execution**: Agent executes with green metrics tracking
4. **Output Generation**: Results written to A2A response JSON
5. **Cleanup**: Container terminated and resources released

---

### 3. Robust Scoring ✅

**Implementation:** Integrated in `core/a2a_gateway.py` and demo

Handles all failure modes gracefully with partial credit system.

#### Features:
- **Failure Classification**: Categorizes errors (timeout, OOM, crash, invalid output)
- **Partial Credit**: Awards points based on partial completion
- **Graceful Degradation**: Scorer never crashes
- **Timeout Handling**: Evaluates partial outputs when agents exceed time limits

#### Scoring Logic:

```python
def calculate_robust_score(status, output, expected_output):
    """
    Robust scoring with failure handling
    
    Returns score between 0.0 and 1.0
    """
    if status == "success":
        # Full evaluation
        return evaluate_similarity(output, expected_output)
    
    elif status == "timeout" and output:
        # Partial credit for timeout with output
        base_score = evaluate_similarity(output, expected_output)
        completeness = estimate_completeness(output)
        return base_score * 0.8 * completeness
    
    elif status == "invalid_output" and output:
        # Small credit for attempting output
        return 0.3 * evaluate_partial_match(output, expected_output)
    
    elif status in ["oom", "crash"]:
        # No output possible
        return 0.0
    
    else:
        # Unknown failure
        return 0.0
```

#### Failure Modes Handled:

| Status | Output Present | Score Range | Behavior |
|--------|---------------|-------------|----------|
| Success | Yes | 0.8-1.0 | Full evaluation |
| Timeout | Yes | 0.5-0.8 | Partial credit |
| Timeout | No | 0.0 | No credit |
| OOM | No | 0.0 | No credit |
| Crash | No | 0.0 | No credit |
| Invalid Output | Yes | 0.2-0.4 | Minimal credit |

---

### 4. RLHF Feedback Loop ✅

**Implementation:** `core/rlhf_feedback_engine.py`

Generates detailed improvement feedback based on RLHF research.

#### Features:
- **Reasoning Trace Analysis**: Analyzes step-by-step agent reasoning
- **Quality Metrics**: Multi-dimensional assessment (reasoning, efficiency, completeness)
- **Improvement Suggestions**: Actionable recommendations
- **Historical Comparison**: Tracks performance over time

#### Usage:

```python
from core.rlhf_feedback_engine import RLHFFeedbackEngine

# Initialize engine
rlhf = RLHFFeedbackEngine()

# Analyze reasoning trace
feedback = rlhf.analyze_reasoning_trace(
    reasoning_trace=[
        {
            "action": "search",
            "thought": "Need to find information...",
            "tool": "web_search",
            "duration": 0.5
        },
        # ... more steps
    ],
    task_type="research",
    execution_time=2.9,
    success=True
)

# Access feedback
print(f"Overall Score: {feedback['overall_score']}")
print(f"Reasoning Quality: {feedback['reasoning_quality']}")
print(f"Suggestions: {feedback['improvement_suggestions']}")
```

#### Feedback Output:

```json
{
  "overall_score": 0.847,
  "reasoning_quality": "good",
  "reasoning_score": 0.85,
  "efficiency_score": 0.78,
  "completeness_score": 0.90,
  "feedback_items": [
    {
      "category": "reasoning_quality",
      "severity": "minor",
      "message": "Limited error handling observed",
      "suggestion": "Add explicit error checking and recovery strategies"
    }
  ],
  "improvement_suggestions": [
    "EFFICIENCY: Cache intermediate results to avoid redundant steps",
    "REASONING: Add explicit verification steps after synthesis"
  ],
  "metrics": {
    "total_steps": 7,
    "avg_step_duration": 0.41,
    "redundant_steps": 0,
    "tool_usage_efficiency": 0.71
  }
}
```

#### Quality Assessment Dimensions:

1. **Reasoning Quality** (40% weight)
   - Clear planning phase
   - Logical progression
   - Iterative refinement
   - Error handling

2. **Efficiency** (20% weight)
   - Step duration
   - Redundant actions
   - Tool utilization

3. **Completeness** (30% weight)
   - Information gathering
   - Analysis phase
   - Synthesis/conclusion

4. **Success** (10% weight)
   - Task completion
   - Output validity

---

## 🚀 Quick Start

### 1. Run Complete Demo

```bash
cd quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench
python demo_agentbeats_integration.py
```

This demonstrates all four pillars in action.

### 2. Integrate with Your Agent

```python
from core.a2a_gateway import A2AGateway
from core.rlhf_feedback_engine import RLHFFeedbackEngine
from core.green_metrics import GreenMetricsCollector

# Initialize components
gateway = A2AGateway()
rlhf = RLHFFeedbackEngine()
metrics = GreenMetricsCollector()

# Your agent execution
def execute_agent_task(task_request):
    # Validate A2A request
    validated = gateway.validate_request(task_request)
    
    # Start metrics
    metrics.start_collection()
    
    # Execute your agent
    result = your_agent.run(validated.input_data)
    
    # Stop metrics
    green_metrics = metrics.stop_collection()
    
    # Generate RLHF feedback
    feedback = rlhf.analyze_reasoning_trace(
        reasoning_trace=result['trace'],
        task_type=validated.task_type,
        execution_time=result['time'],
        success=result['success']
    )
    
    # Create A2A response
    response = gateway.create_success_response(
        task_id=validated.task_id,
        output=result['output'],
        execution_time=result['time'],
        green_metrics=green_metrics,
        reasoning_trace=result['trace'],
        metadata={'rlhf_feedback': feedback}
    )
    
    return response.to_dict()
```

### 3. Build Docker Container

```bash
# Generate Dockerfile and entrypoint
python -c "
from core.docker_orchestrator import create_dockerfile, create_entrypoint
create_dockerfile()
create_entrypoint()
"

# Build image
docker build -t your-agent:latest -f Dockerfile.agent .

# Test
echo '{"task_id":"test","task_type":"qa","input_data":{"query":"Test?"}}' > input.json
docker run --rm \
  -v $(pwd)/input.json:/app/input.json:ro \
  -v $(pwd)/output.json:/app/output.json:rw \
  your-agent:latest
cat output.json
```

---

## 📊 Green Metrics Integration

All responses include sustainability metrics:

```json
{
  "green_metrics": {
    "energy_kwh": 0.042,
    "carbon_kg": 0.018,
    "sustainability_index": 0.73,
    "efficiency_score": 0.85,
    "comparative_baseline": {
      "percentile": 75,
      "vs_average": "+15%"
    }
  }
}
```

See `GREEN_AGENT_BENCHMARKING_COMPLETE.md` for details.

---

## 🎯 AgentBeats Submission Checklist

- [ ] A2A protocol validation passes
- [ ] Docker container builds successfully
- [ ] Independent execution works (no manual intervention)
- [ ] All failure modes handled gracefully
- [ ] RLHF feedback generates for all tasks
- [ ] Green metrics included in responses
- [ ] Documentation complete
- [ ] Tests pass

---

## 📚 Additional Resources

- **A2A Protocol Spec**: [Link to spec]
- **AgentBeats Leaderboard**: [Link to leaderboard]
- **Green Metrics Guide**: `GREEN_AGENT_BENCHMARKING_COMPLETE.md`
- **Integration Examples**: `demo_agentbeats_integration.py`

---

## 🤝 Contributing

See `CONTRIBUTING.md` for guidelines on improving AgentBeats compliance.

---

## 📄 License

See `LICENSE` file for details.
