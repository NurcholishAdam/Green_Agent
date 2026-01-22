FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src ./src
COPY agent_card.toml .
COPY docker_metrics_collector.py .
COPY run_agent.py .

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Allow cgroup access for metrics
VOLUME ["/sys/fs/cgroup"]

# SINGLE execution entrypoint
CMD ["python", "run_agent.py"]

# same base as above ...

# Disable healthcheck during benchmarking
# HEALTHCHECK intentionally omitted

CMD ["python", "-m", "uvicorn", "src.agentbeats.main:app", "--host", "0.0.0.0", "--port", "8000"]
