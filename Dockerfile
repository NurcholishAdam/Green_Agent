FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (includes src/enhancements/ if present)
COPY . .

# Create directories
RUN mkdir -p config data logs results

# Set environment variables (original)
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    MODE=unified \
    LOG_LEVEL=INFO \
    RAY_DISABLE_DOCKER_CPU_WARNING=1

# ---------------------------------------------------------------
# Advanced Enhancements Environment Variables
# These toggles enable/disable the modules from src/enhancements/
# Set to "true" to activate; they default to false for minimal
# footprint unless overridden at runtime.
# ---------------------------------------------------------------
ENV ENHANCEMENTS_ENABLED=true
ENV LIMIT_GRAPH_ENABLED=true
ENV LIMIT_GRAPH_CENTRALITY=0.7
ENV LIMIT_GRAPH_CONNECTIVITY=0.6
ENV MODP_ENABLED=true
ENV MODP_WEIGHTS=[0.4,0.3,0.2,0.1]
ENV RLHF_ENABLED=true
ENV HUMAN_FEEDBACK_SCORE=0.6
ENV DISTILLATION_ENABLED=true
ENV MOE_GATING_ENABLED=true
ENV EVOLUTIONARY_ENABLED=true
ENV POPULATION_SIZE=20
ENV MUTATION_RATE=0.1
ENV FLEXGEN_ENABLED=false
ENV FLEXGEN_MODEL_NAME=facebook/opt-6.7b
ENV FLEXGEN_DEFAULT_PRECISION=fp16
ENV FLEXGEN_DELEGATION_POLICY=adaptive

# Expose ports
EXPOSE 8000 8265 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Label to indicate enhanced image
LABEL org.green-agent.enhancements="true"

# Default command
CMD ["python", "runtime/run_agent.py", "--mode", "unified"]
