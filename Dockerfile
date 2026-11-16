# syntax=docker/dockerfile:1.7
#
# Green Agent v5.0.0 — Dockerfile
# ================================
#
# | Field | Value |
# |---|---|
# | Version | 5.0.0 |
# | Last Updated | 2026-11-16 |
# | Status | Production Ready |
# | Base image | python:3.12.3-slim (digest-pinned) |
# | Runtime user | appuser:10001 (non-root) |
# | PID 1 | tini |
# | Healthcheck | GET /health :8000 |
# | Signals | SIGTERM (graceful shutdown) |
#
# ── Build variants ────────────────────────────────────────────────────────
#   Full (all enhancements) :
#     docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .
#   Lean (no enhancements) :
#     docker build --build-arg ENHANCEMENTS_ENABLED=false -t green-agent:lean .
#   Custom (per-module flags) :
#     docker build \
#       --build-arg ENHANCEMENTS_ENABLED=true \
#       --build-arg FLEXGEN_ENABLED=true \
#       --build-arg ZERO_TRUST_ENABLED=true \
#       -t green-agent:custom .
#
# ── To update the base digest ─────────────────────────────────────────────
#   docker buildx imagetools inspect python:3.12.3-slim
#   → copy the `Digest: sha256:…` line into ARG PYTHON_IMAGE below.
#
# ── Verification after build ──────────────────────────────────────────────
#   docker run --rm green-agent:full python test_symbolic_oversight.py
#   docker inspect --format='{{.State.Health.Status}}' <container-id>
#
# ── Reproducibility ───────────────────────────────────────────────────────
#   All versioned flags are ARG-overridable; the defaults below produce a
#   full-enhancements image. Never commit a `REPLACE_ME` digest — the build
#   fails at the `FROM` line before any layer runs.
# =============================================================================

# ── Base image (pinned by digest for reproducibility) ────────────────────
# Last verified: 2026-11-16
# To update:     docker buildx imagetools inspect python:3.12.3-slim
ARG PYTHON_IMAGE=python:3.12.3-slim@sha256:afc139a0a640942491ec481ad8dda10f2c5b753f5c969393b12480155fe15a63


# =============================================================================
# Stage 1 — builder
#   Installs Python dependencies into /install (a self-contained prefix).
#   Leaves gcc/g++ behind — they do NOT ship in the runtime stage.
# =============================================================================
FROM ${PYTHON_IMAGE} AS builder

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Build-time system deps only. These stay in the builder stage.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy requirements first (layer cache: deps only re-install when they change)
COPY requirements.txt .
COPY requirements.lock.txt* ./
COPY pyproject.toml ./
COPY README.md* ./

# Install into /install (isolated prefix — copied verbatim in runtime)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir --upgrade pip && \
    if [ -f requirements.lock.txt ]; then \
        pip install --no-cache-dir --prefix=/install \
            --require-hashes -r requirements.lock.txt; \
    else \
        pip install --no-cache-dir --prefix=/install \
            -r requirements.txt; \
    fi && \
    pip check

# Install the src/ package in editable-equivalent mode so `from src.*` works.
# We copy the source tree now, then `pip install --prefix=/install .`.
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir --prefix=/install --no-deps . && \
    pip check


# =============================================================================
# Stage 2 — runtime
#   Minimal footprint: no compilers, no caches, non-root user, tini PID 1.
# =============================================================================
FROM ${PYTHON_IMAGE} AS runtime

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ── Build metadata (baked into image labels) ──────────────────────────────
ARG BUILD_DATE
ARG VCS_REF
ARG BUILD_VERSION=5.0.0

# ── Runtime system deps: tini only ────────────────────────────────────────
# tini = minimal init: forwards SIGTERM, reaps zombie processes.
RUN apt-get update && apt-get install -y --no-install-recommends \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10001 -g 10001 -m -s /bin/bash appuser

# ── Python dependencies from builder ──────────────────────────────────────
COPY --from=builder /install /usr/local

# ── Application source + assets ───────────────────────────────────────────
WORKDIR /app

COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser run_agent.py ./run_agent.py
COPY --chown=appuser:appuser docker_metrics_collector.py ./docker_metrics_collector.py
COPY --chown=appuser:appuser agentbeats.json ./agentbeats.json

# Config + policies (mounted or baked-in)
COPY --chown=appuser:appuser green_policy.yaml* ./green_policy.yaml
COPY --chown=appuser:appuser symbolic_policy.yaml* ./symbolic_policy.yaml

# Optional: free_apis config (mounted from ConfigMap in K8s)
COPY --chown=appuser:appuser free_apis_config.yaml* ./free_apis_config.yaml

# Test suite (kept so `python test_symbolic_oversight.py` works in-container)
COPY --chown=appuser:appuser test_symbolic_oversight.py* ./
COPY --chown=appuser:appuser test_meta_cognitive.py* ./

# Make sure appuser owns everything
RUN chown -R appuser:appuser /app && \
    mkdir -p /app/checkpoints /out && \
    chown -R appuser:appuser /app/checkpoints /out

# ── Base environment (always set) ─────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONHASHSEED=0 \
    PYTHONPATH=/app:/app/src \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json \
    LEAN_CTX_VERSION=0.1.0 \
    AGENT_PORT=8000 \
    AGENT_HEALTH_PATH=/health \
    SYMBOLIC_POLICY=symbolic_policy.yaml \
    FREE_APIS_CONFIG=free_apis_config.yaml

# ── Build-time toggles (ARG → ENV) ────────────────────────────────────────
# Every ARG is overridable per build; defaults enable the full suite.
ARG ENHANCEMENTS_ENABLED=true
ARG LIMIT_GRAPH_ENABLED=true
ARG MODP_ENABLED=true
ARG RLHF_ENABLED=true
ARG DISTILLATION_ENABLED=true
ARG MOE_ENABLED=true
ARG EVOLUTIONARY_ENABLED=true
ARG FLEXGEN_ENABLED=false
ARG ZERO_TRUST_ENABLED=false
ARG DIFFERENTIAL_PRIVACY_ENABLED=false
ARG TRAINING_ENABLED=false

# ── Advanced enhancements (runtime ENV) ───────────────────────────────────
# NOTE: MODP_WEIGHTS is CSV, NOT JSON — Docker ENV does not parse JSON.
# The entrypoint below validates format and sum() ≈ 1.0 before starting.
ENV ENHANCEMENTS_ENABLED=${ENHANCEMENTS_ENABLED} \
    LIMIT_GRAPH_ENABLED=${LIMIT_GRAPH_ENABLED} \
    LIMIT_GRAPH_CENTRALITY=0.7 \
    LIMIT_GRAPH_CONNECTIVITY=0.6 \
    MODP_ENABLED=${MODP_ENABLED} \
    MODP_WEIGHTS="0.4,0.3,0.2,0.1" \
    RLHF_ENABLED=${RLHF_ENABLED} \
    HUMAN_FEEDBACK_SCORE=0.6 \
    DISTILLATION_ENABLED=${DISTILLATION_ENABLED} \
    MOE_ENABLED=${MOE_ENABLED} \
    MOE_GATING_ENABLED=${MOE_ENABLED} \
    EVOLUTIONARY_ENABLED=${EVOLUTIONARY_ENABLED} \
    POPULATION_SIZE=20 \
    MUTATION_RATE=0.1 \
    ZERO_TRUST_ENABLED=${ZERO_TRUST_ENABLED} \
    DIFFERENTIAL_PRIVACY_ENABLED=${DIFFERENTIAL_PRIVACY_ENABLED} \
    TRAINING_ENABLED=${TRAINING_ENABLED} \
    CHECKPOINT_DIR=/app/checkpoints \
    FLEXGEN_ENABLED=${FLEXGEN_ENABLED} \
    FLEXGEN_MODEL_NAME=facebook/opt-6.7b \
    FLEXGEN_DEFAULT_PRECISION=fp16 \
    FLEXGEN_DELEGATION_POLICY=adaptive

# ── Entrypoint with validation ────────────────────────────────────────────
# Runs BEFORE CMD. Fails fast on bad config instead of silently degrading.
RUN cat > /usr/local/bin/docker-entrypoint.sh <<'ENTRYPOINT_EOF'
#!/bin/bash
set -euo pipefail

# 1. Validate MODP_WEIGHTS: list of floats summing to ~1.0
if [[ -n "${MODP_WEIGHTS:-}" ]]; then
    if ! python - <<'PY'
import os, sys
raw = os.environ["MODP_WEIGHTS"]
try:
    parts = [p.strip() for p in raw.strip("[]").split(",") if p.strip()]
    vals  = [float(p) for p in parts]
    assert vals, "empty weight vector"
    total = sum(vals)
    assert abs(total - 1.0) < 1e-6, f"weights sum to {total}, expected ~1.0"
except Exception as exc:
    print(f"[entrypoint] MODP_WEIGHTS invalid ({raw!r}): {exc}", file=sys.stderr)
    sys.exit(1)
PY
    then
        echo "[entrypoint] fatal: MODP_WEIGHTS is not a valid weight vector" >&2
        exit 1
    fi
fi

# 2. Warn on dead configuration
if [[ "${FLEXGEN_ENABLED:-false}" == "false" && -n "${FLEXGEN_MODEL_NAME:-}" ]]; then
    echo "[entrypoint] warn: FLEXGEN_ENABLED=false but FLEXGEN_* vars set (ignored)" >&2
fi

# 3. Fail fast if Zero Trust is enabled without a token
if [[ "${ZERO_TRUST_ENABLED:-false}" == "true" && -z "${ZERO_TRUST_TOKEN:-}" ]]; then
    echo "[entrypoint] fatal: ZERO_TRUST_ENABLED=true but ZERO_TRUST_TOKEN is unset" >&2
    exit 1
fi

# 4. Emit the resolved toggle matrix for operability
echo "[entrypoint] enhancement toggles:" >&2
for flag in ENHANCEMENTS_ENABLED LIMIT_GRAPH_ENABLED MODP_ENABLED RLHF_ENABLED \
            DISTILLATION_ENABLED MOE_ENABLED EVOLUTIONARY_ENABLED ZERO_TRUST_ENABLED \
            DIFFERENTIAL_PRIVACY_ENABLED TRAINING_ENABLED FLEXGEN_ENABLED; do
    printf '  %-30s = %s\n' "$flag" "${!flag:-unset}" >&2
done

exec "$@"
ENTRYPOINT_EOF

RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

# ── Metadata labels (OCI) ─────────────────────────────────────────────────
LABEL org.opencontainers.image.title="green-agent" \
      org.opencontainers.image.description="Green Agent with 4-layer feedback + advanced enhancements" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/NurcholishAdam/Green_Agent" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.green-agent.enhancements="true" \
      org.green-agent.layer-versions="meta-cognitive=1.2.0,symbolic=1.2.0,enhancements=1.2.0"

# ── Volume for cgroup metrics ─────────────────────────────────────────────
# Mount read-only at run time:
#   -v /sys/fs/cgroup:/sys/fs/cgroup:ro
VOLUME ["/sys/fs/cgroup"]

# ── Runtime ───────────────────────────────────────────────────────────────
EXPOSE 8000
STOPSIGNAL SIGTERM

USER appuser

# Healthcheck against the agent's /health endpoint (matches Deployment v5.0).
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import os,sys,urllib.request; \
        port=os.environ.get('AGENT_PORT','8000'); \
        path=os.environ.get('AGENT_HEALTH_PATH','/health'); \
        sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=3).status==200 else 1)"

# tini = PID 1 → signal forwarding + zombie reaping.
# entrypoint = validation, then exec CMD.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "run_agent.py", "--mode=unified"]
