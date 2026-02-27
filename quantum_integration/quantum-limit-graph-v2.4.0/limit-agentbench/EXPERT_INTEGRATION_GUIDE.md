# Expert Collaboration Layer - Integration Guide

## Overview

This guide shows how to integrate the Expert Collaboration Layer into Green Agent, adding intelligent escalation to external experts while maintaining sustainability focus.

## Architecture

```
User Layer
   │
   ▼
FastAPI Dashboard API
   │
   ▼
Ray Distributed Cluster (Kubernetes Autoscaled)
   │
   ├── Multi-Agent Workers
   ├── PPO Trainer
   ├── Q-table Store
   ├── Carbon Grid Forecaster
   ├── Pareto Analyzer
   │
   ▼
──────────────────────────────────────────────
Enhancement Layer: Expert Collaboration
   │
   ├── Expert Model Gateway (expert_gateway.py)
   │     • OpenAI GPT-4 / Anthropic Claude
   │     • Sustainability tracking
   │     • Response caching
   │
   ├── Domain Connectors (expert_connectors.py)
   │     • Compiler analysis
   │     • Static analysis
   │     • Energy benchmarking
   │
   ├── Knowledge Integrator (invocation_policy.py)
   │     • Energy standards
   │     • Scientific papers
   │     • Code repositories
   │
   ├── Human Review Portal (expert_collaboration_system.py)
   │     • Critical task escalation
   │     • Reviewer assignment
   │     • Status tracking
   │
   ▼
Selective Invocation Policy (invocation_policy.py)
   │
   ├── Confidence Threshold (< 0.7 → escalate)
   ├── Sustainability Threshold (> 0.1 Wh → escalate)
   ├── Criticality Threshold (safety/security → escalate)
   │
   ▼
Feedback Integration & Audit (audit_logger.py)
   │
   ├── Expert feedback merger
   ├── Sustainability logger
   ├── Transparency reports
```

## File Structure

Add the following files to your Green Agent repository:

```
Green_Agent/
├── runtime/
│   ├── expert_gateway.py              (NEW)
│   ├── expert_connectors.py           (NEW)
│   └── run_agent.py                   (UPDATE)
│
├── policy/
│   ├── invocation_policy.py           (NEW)
│   └── policy_engine.py               (UPDATE)
│
├── analytics/
│   ├── audit_logger.py                (NEW)
│   ├── expert_collaboration_system.py (NEW)
│   └── pareto_analyzer.py             (UPDATE)
│
├── dashboard/
│   ├── api_server.py                  (UPDATE)
│   └── plotly_dashboard.py            (UPDATE)
│
└── knowledge/
    ├── energy_standards.json          (NEW)
    ├── benchmarks.json                (NEW)
    └── knowledge_integrator.py        (NEW)
```

## Installation

### Step 1: Install Dependencies

```bash
pip install openai anthropic sentence-transformers
```

### Step 2: Add Expert Collaboration Modules

Place the new files in their respective directories:

```bash
# Copy expert collaboration files
cp expert_gateway.py Green_Agent/runtime/
cp expert_connectors.py Green_Agent/runtime/
cp invocation_policy.py Green_Agent/policy/
cp audit_logger.py Green_Agent/analytics/
cp expert_collaboration_system.py Green_Agent/analytics/
```

### Step 3: Update Existing Files

#### Update `runtime/run_agent.py`

```python
from runtime.expert_gateway import create_multi_provider_gateway, ExpertDomain
from policy.invocation_policy import SelectiveInvocationPolicy
from analytics.audit_logger import AuditLogger
from analytics.expert_collaboration_system import ExpertCollaborationSystem

class EnhancedAgent:
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        
        # Initialize expert collaboration
        self.expert_system = ExpertCollaborationSystem(
            expert_gateway=create_multi_provider_gateway(
                openai_key=os.getenv("OPENAI_API_KEY"),
                anthropic_key=os.getenv("ANTHROPIC_API_KEY")
            ),
            invocation_policy=SelectiveInvocationPolicy(
                confidence_threshold=0.7,
                sustainability_threshold_wh=0.1
            ),
            audit_logger=AuditLogger(
                log_file="expert_audit.log"
            )
        )
    
    async def run_task(self, task):
        # Original agent processing
        output, confidence, energy = await self._process_task(task)
        
        # Expert collaboration
        result = await self.expert_system.process_task(
            task_id=task.id,
            agent_id=self.agent_id,
            task_description=task.description,
            agent_output=output,
            agent_confidence=confidence,
            estimated_energy_wh=energy,
            domain=task.domain
        )
        
        return result
```

#### Update `policy/policy_engine.py`

```python
from policy.invocation_policy import SelectiveInvocationPolicy, EscalationReason

class PolicyEngine:
    def __init__(self):
        # Existing initialization...
        
        # Add invocation policy
        self.invocation_policy = SelectiveInvocationPolicy(
            confidence_threshold=0.7,
            sustainability_threshold_wh=0.1,
            enable_criticality_check=True,
            enable_sustainability_check=True
        )
    
    def should_escalate(self, task, agent_state):
        """Check if task should escalate to expert."""
        decision = self.invocation_policy.decide_escalation(
            task=task.description,
            agent_confidence=agent_state.confidence,
            estimated_energy_wh=agent_state.estimated_energy,
            domain=task.domain,
            context={"agent_state": agent_state}
        )
        
        return decision.should_escalate, decision
```

#### Update `dashboard/api_server.py`

```python
from fastapi import FastAPI
from analytics.expert_collaboration_system import ExpertCollaborationSystem
from analytics.audit_logger import TransparencyReport

app = FastAPI()

@app.get("/api/expert/stats")
async def get_expert_stats():
    """Get expert collaboration statistics."""
    return expert_system.get_collaboration_stats()

@app.get("/api/transparency/report")
async def get_transparency_report(
    start_time: Optional[float] = None,
    end_time: Optional[float] = None
):
    """Generate transparency report."""
    report = audit_logger.generate_transparency_report(
        period_start=start_time,
        period_end=end_time
    )
    return asdict(report)

@app.post("/api/human/review/submit")
async def submit_review(
    request_id: str,
    reviewer_id: str,
    status: str,
    notes: str
):
    """Submit human review."""
    success = expert_system.human_portal.submit_review(
        request_id=request_id,
        reviewer_id=reviewer_id,
        status=ReviewStatus[status.upper()],
        notes=notes
    )
    return {"success": success}

@app.get("/api/human/review/pending")
async def get_pending_reviews():
    """Get pending review requests."""
    reviews = expert_system.human_portal.get_pending_reviews()
    return [asdict(r) for r in reviews]
```

## Usage Examples

### Example 1: Basic Expert Escalation

```python
from analytics.expert_collaboration_system import create_expert_collaboration_system

# Create system
expert_system = create_expert_collaboration_system(
    openai_key="your-openai-key",
    anthropic_key="your-anthropic-key",
    confidence_threshold=0.7,
    sustainability_threshold_wh=0.1
)

# Process task
result = await expert_system.process_task(
    task_id="task_001",
    agent_id="agent_alpha",
    task_description="Optimize memory allocation in critical safety system",
    agent_output="malloc(size)",
    agent_confidence=0.65,  # Low confidence
    estimated_energy_wh=0.05,
    domain="security"
)

# Result includes expert feedback
if result["escalated"]:
    print(f"Expert: {result['expert_feedback']['response_text']}")
    print(f"Energy: {result['expert_feedback']['energy_consumed_wh']} Wh")
```

### Example 2: Domain-Specific Analysis

```python
from runtime.expert_connectors import ExpertConnectorRegistry, ExpertConnectorType

# Create connector registry
registry = ExpertConnectorRegistry()

# Analyze code with compiler
result = await registry.invoke(
    connector_type=ExpertConnectorType.COMPILER,
    input_data=source_code,
    config={"optimization_level": "-O3"}
)

if result.success:
    print(f"Optimizations: {result.output['optimizations']}")
    print(f"Warnings: {result.warnings}")
    print(f"Energy: {result.energy_consumed_wh} Wh")
```

### Example 3: Transparency Reporting

```python
from analytics.audit_logger import AuditLogger

# Create audit logger
audit = AuditLogger(log_file="expert_audit.log")

# Log events...
audit.log_expert_invocation(
    task_id="task_001",
    agent_id="agent_alpha",
    expert_type="gpt-4",
    energy_wh=0.05,
    carbon_kg=0.00002
)

# Generate report
report = audit.generate_transparency_report()

print(f"Escalation Rate: {report.escalation_rate:.1%}")
print(f"Energy Saved: {report.energy_saved_wh*1000:.1f} mWh")
print(f"Transparency Score: {report.transparency_score:.2f}")

# Export as HTML
audit.export_report(report, "transparency_report.html", format="html")
```

### Example 4: Human Review Integration

```python
# Submit critical task for human review
request_id = expert_system.human_portal.submit_for_review(
    task_id="critical_task_001",
    agent_output=agent_output,
    escalation_reasons=["critical_safety", "low_confidence"],
    criticality_level="critical"
)

# Assign reviewer
expert_system.human_portal.assign_reviewer(
    request_id=request_id,
    reviewer_id="reviewer_alice"
)

# Submit review
expert_system.human_portal.submit_review(
    request_id=request_id,
    reviewer_id="reviewer_alice",
    status=ReviewStatus.APPROVED,
    notes="Verified safety properties. Approved."
)
```

## Integration with Existing Green Agent Components

### With VimRAG Retrieval System

```python
from retrieval.vimrag_integration import VimRAGIntegration
from analytics.expert_collaboration_system import ExpertCollaborationSystem

# Combine VimRAG with expert collaboration
vimrag = VimRAGIntegration(enable_semantic_scoring=True)
expert_system = create_expert_collaboration_system()

# Process retrieval task
retrieval_result = vimrag.retrieve(query="quantum computing safety")

# If low confidence, escalate
if retrieval_result.avg_similarity < 0.7:
    expert_result = await expert_system.process_task(
        task_id="retrieval_001",
        agent_id="vimrag",
        task_description=f"Verify retrieval accuracy for: {query}",
        agent_output=retrieval_result.retrieved_nodes,
        agent_confidence=retrieval_result.avg_similarity,
        estimated_energy_wh=retrieval_result.total_energy_wh,
        domain="scientific"
    )
```

### With Ray Distributed Runtime

```python
import ray
from analytics.expert_collaboration_system import ExpertCollaborationSystem

@ray.remote
class DistributedAgent:
    def __init__(self):
        self.expert_system = create_expert_collaboration_system()
    
    async def process(self, task):
        result = await self.expert_system.process_task(
            task_id=task.id,
            agent_id=ray.get_runtime_context().task_id,
            task_description=task.description,
            agent_output=await self._agent_process(task),
            agent_confidence=self._get_confidence(),
            estimated_energy_wh=self._estimate_energy(),
            domain=task.domain
        )
        return result

# Deploy to Ray cluster
agents = [DistributedAgent.remote() for _ in range(10)]
results = ray.get([agent.process.remote(task) for agent in agents])
```

## Configuration

### Environment Variables

```bash
# API Keys
export OPENAI_API_KEY="your-openai-key"
export ANTHROPIC_API_KEY="your-anthropic-key"

# Thresholds
export CONFIDENCE_THRESHOLD=0.7
export SUSTAINABILITY_THRESHOLD_WH=0.1

# Audit Logging
export AUDIT_LOG_FILE="expert_audit.log"
export ENABLE_PERSISTENCE=true
```

### Configuration File (config.yaml)

```yaml
expert_collaboration:
  expert_gateway:
    providers:
      - type: openai
        model: gpt-4-turbo
        api_key: ${OPENAI_API_KEY}
      - type: anthropic
        model: claude-3-opus-20240229
        api_key: ${ANTHROPIC_API_KEY}
    enable_caching: true
    
  invocation_policy:
    confidence_threshold: 0.7
    sustainability_threshold_wh: 0.1
    enable_criticality_check: true
    enable_sustainability_check: true
    
  audit_logger:
    log_file: expert_audit.log
    enable_persistence: true
    
  human_review:
    enable: true
    max_queue_size: 100
```

## Testing

### Unit Tests

```python
# tests/test_expert_gateway.py
import pytest
from runtime.expert_gateway import create_openai_gateway, ExpertDomain

@pytest.mark.asyncio
async def test_expert_invocation():
    gateway = create_openai_gateway(api_key="test-key")
    
    response = await gateway.invoke_expert(
        task="Test task",
        prompt="Optimize this code",
        domain=ExpertDomain.CODE_GENERATION
    )
    
    assert response.response_text
    assert response.energy_consumed_wh > 0
```

### Integration Tests

```python
# tests/test_expert_system.py
@pytest.mark.asyncio
async def test_end_to_end_escalation():
    system = create_expert_collaboration_system()
    
    result = await system.process_task(
        task_id="test_001",
        agent_id="test_agent",
        task_description="Critical security task",
        agent_output="vulnerable code",
        agent_confidence=0.5,
        estimated_energy_wh=0.05,
        domain="security"
    )
    
    assert result["escalated"]
    assert result["expert_feedback"]
```

## Monitoring & Metrics

### Dashboard Integration

Add to Plotly dashboard:

```python
import plotly.graph_objects as go

# Expert invocation timeline
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=[e.timestamp for e in audit_events],
    y=[e.energy_consumed_wh for e in audit_events],
    name="Expert Energy"
))

# Escalation rate over time
fig.add_trace(go.Scatter(
    x=timestamps,
    y=escalation_rates,
    name="Escalation Rate"
))
```

### Prometheus Metrics

```python
from prometheus_client import Counter, Histogram

expert_invocations = Counter(
    'expert_invocations_total',
    'Total expert invocations',
    ['expert_type', 'domain']
)

expert_energy = Histogram(
    'expert_energy_wh',
    'Energy consumed by experts',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0]
)
```

## Best Practices

1. **Set appropriate thresholds**: Start conservative (high confidence threshold) and adjust based on metrics

2. **Monitor energy consumption**: Track expert invocation costs vs. savings

3. **Review escalation patterns**: Regularly check which tasks escalate most

4. **Human review for critical tasks**: Always route safety/security to humans

5. **Cache expert responses**: Significant energy savings for repeated queries

6. **Transparency reporting**: Generate regular reports for stakeholders

## Troubleshooting

### Issue: High escalation rate
- **Cause**: Thresholds too strict
- **Solution**: Adjust confidence_threshold or sustainability_threshold

### Issue: Expert API timeouts
- **Cause**: Network issues or rate limiting
- **Solution**: Implement retry logic, use multiple providers

### Issue: Audit log growing too large
- **Cause**: High event volume
- **Solution**: Rotate logs, archive old events, use compression

## Next Steps

1. Deploy to staging environment
2. Run A/B test against baseline
3. Measure sustainability improvements
4. Collect user feedback on human review portal
5. Optimize thresholds based on production data

---

**Version**: 1.0  
**Date**: 2026-02-24  
**Status**: Production Ready
