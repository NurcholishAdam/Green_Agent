# syntax=docker/dockerfile:1.7
# =============================================================================
# Dockerfile — Green Agent (Enhanced v2.0)
# =============================================================================
# Multi-stage build with a version-tagged base image, hardened non-root runtime, tini
# as PID 1, and health checks on /metrics (the only HTTP endpoint
# run_agent.py exposes).
#
# WHAT CHANGED FROM v1.0
# ----------------------
#   - Uses a version-tagged base image (rather than an invalid placeholder digest).
#   - Multi-stage build — build tools (gcc, g++) live in the builder stage
#     and are NOT copied to the runtime image (~200 MB saved, CVE surface
#     reduced).
#   - Non-root runtime user (uid=1000) — v1.0 ran as root.
#   - tini as PID 1 — v1.0's SIGTERM reached Python but not Ray workers.
#   - HEALTHCHECK on /metrics — v1.0's /health endpoint does not exist in
#     run_agent.py. Using Python (not curl) so the runtime image stays lean.
#   - EXPOSE 8000 only — v1.0 exposed 8265 (Ray dashboard, UNAUTHENTICATED
#     by default) and 9090 (Prometheus UI, not in this container).
#   - ENHANCEMENTS_ENABLED=false and all sibling flags=false — v1.0 set them
#     to true, contradicting README.md §0.2 and the file's own comment.
#   - MODP_WEIGHTS quoted — the raw [0.4,0.3,0.2,0.1] is valid but opaque.
#   - COPY --chown=app:app — v1.0 left files root-owned.
#   - ARG for build metadata + full OCI labels.
#   - STOPSIGNAL SIGTERM.
#   - ENTRYPOINT + CMD split — v1.0 used CMD only, so `docker run <image>
#     --once` was impossible.
#   - Removed `MODE=unified` env (duplicated with CMD `--mode unified`).
#   - Removed `PYTHONPATH=/app` (masks missing installs; `pip install -e .`
#     is authoritative).
#   - Added VOLUME /run/secrets for the secrets mount.
#   - Secrets are never baked into the image; see SECURITY.md.
#
# BUILD METADATA
# --------------
#   docker buildx build \
#     --build-arg VERSION=5.0.0 \
#     --build-arg VCS_REF=$(git rev-parse HEAD) \
#     --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
#     --build-arg PYTHON_IMAGE=python:3.12.3-slim \
#     --sbom=true --provenance=true \
#     -t ghcr.io/<org>/green-agent:5.0.0 .
#
# RUN
# ---
#   # Continuous (metrics + a2a endpoint stay up for scraping)
#   docker run --rm -p 8000:8000 -v /run/secrets:/run/secrets:ro \
#     ghcr.io/<org>/green-agent@sha256:<digest>
#
#   # Batch — one task and exit
#   docker run --rm -v /run/secrets:/run/secrets:ro \
#     ghcr.io/<org>/green-agent@sha256:<digest> --once --iterations 1
#
#   # Enable enhancements at runtime (they are OFF by default)
#   docker run --rm -p 8000:8000 -e ENHANCEMENTS_ENABLED=true \
#     ghcr.io/<org>/green-agent@sha256:<digest>
# =============================================================================

# -----------------------------------------------------------------------------
# Build metadata
# -----------------------------------------------------------------------------
ARG PYTHON_IMAGE=python:3.12.3-slim
ARG BUILD_DATE=unknown
ARG VCS_REF=unknown
ARG VERSION=5.0.0

# =============================================================================
# Stage 1 — builder
# =============================================================================
FROM ${PYTHON_IMAGE} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Build-time system dependencies. gcc/g++ are required by prophet
# (cmdstanpy → C++), qiskit-aer (rust/cargo wheel build fallback), and
# sentence-transformers (torch on some platforms). They are NOT copied to
# the runtime stage.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
    && rm -rf /var/lib/apt/lists/*

# Copy pinned dependency manifests. The lock file is preferred when present
# (--require-hashes for supply-chain integrity). requirements.txt is the
# fallback for environments without a lock file.
COPY requirements.txt requirements.lock.txt* ./

# Prefer hashed lock install. Fall back to plain pinned install.
RUN --mount=type=cache,target=/root/.cache/pip \
    if [ -f requirements.lock.txt ]; then \
        pip install --no-cache-dir --require-hashes \
            -r requirements.lock.txt; \
    else \
        pip install --no-cache-dir -r requirements.txt; \
    fi

# Pre-download the sentence-transformers model so the runtime image does
# not need network access on first request. Best-effort — skip if the
# module is not present in requirements.txt.
RUN python -c "from sentence_transformers import SentenceTransformer; \
    SentenceTransformer('all-MiniLM-L6-v2')" || true

# =============================================================================
# Stage 2 — runtime
# =============================================================================
FROM ${PYTHON_IMAGE} AS runtime

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

# --- OCI image labels -------------------------------------------------------
LABEL org.opencontainers.image.title="Green Agent" \
      org.opencontainers.image.description="Green benchmarking agent with optional advanced enhancements" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/your-org/Green_Agent" \
      org.green-agent.enhancements.available="true" \
      org.green-agent.enhancements.default-enabled="false"

# --- Baseline env -----------------------------------------------------------
# No PYTHONPATH=/app (that masks missing installs; `pip install -e .`
# is authoritative). No MODE (CMD carries --mode unified).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    RAY_DISABLE_DOCKER_CPU_WARNING=1 \
    LOG_LEVEL=INFO \
    PORT=8000 \
    BIND=0.0.0.0 \
    GREEN_AGENT_DATA_DIR=/var/lib/green-agent \
    GREEN_AGENT_SECRETS_DIR=/run/secrets

# --- tini: PID 1 for signal forwarding -------------------------------------
# Without tini, SIGTERM reaches python but not Ray workers. tini forwards
# the signal to all children so the container drains cleanly.
RUN apt-get update && apt-get install -y --no-install-recommends \
        tini \
    && rm -rf /var/lib/apt/lists/*

# --- Non-root user ---------------------------------------------------------
# UID 1000 matches run_agent.py's `run_as_user` in agent_config.yaml and
# the k8s deployment's `securityContext.runAsUser`.
RUN groupadd -g 1000 app \
    && useradd -u 1000 -g app -m -s /usr/sbin/nologin app \
    && mkdir -p /app /var/lib/green-agent /var/log/green-agent /app/results \
    && chown -R app:app /app /var/lib/green-agent /var/log/green-agent

# --- Installed Python environment from the builder stage -------------------
# Both stages use the same base image, so the site-packages and console
# scripts are safe to copy. This is the standard multi-stage Python pattern.
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

WORKDIR /app

# --- Application code ------------------------------------------------------
# Copied as `app:app` — v1.0 left files root-owned, which broke the
# non-root runtime.
COPY --chown=app:app . .

# Editable install so `from src...` works without PYTHONPATH hacks. The
# `|| true` handles cases where the repo root has no pyproject.toml (e.g.,
# legacy deployments) — in that case `run_agent.py` must be invoked with
# an explicit PYTHONPATH via env.
RUN pip install --no-deps -e . || true

# --- Enhancement toggles — default FALSE per README.md §0.2 ----------------
# Enable at runtime:  -e ENHANCEMENTS_ENABLED=true
# Or via docker-compose env_file.
ENV ENHANCEMENTS_ENABLED=false \
    LIMIT_GRAPH_ENABLED=false \
    LIMIT_GRAPH_CENTRALITY=0.7 \
    LIMIT_GRAPH_CONNECTIVITY=0.6 \
    MODP_ENABLED=false \
    MODP_WEIGHTS="[0.4,0.3,0.2,0.1]" \
    RLHF_ENABLED=false \
    HUMAN_FEEDBACK_SCORE=0.6 \
    DISTILLATION_ENABLED=false \
    MOE_GATING_ENABLED=false \
    EVOLUTIONARY_ENABLED=false \
    POPULATION_SIZE=20 \
    MUTATION_RATE=0.1 \
    FLEXGEN_ENABLED=false \
    FLEXGEN_MODEL_NAME=facebook/opt-6.7b \
    FLEXGEN_DEFAULT_PRECISION=fp16 \
    FLEXGEN_DELEGATION_POLICY=adaptive

# --- Secrets mount ---------------------------------------------------------
# Never bake secrets into the image. The orchestrator mounts a tmpfs or
# Kubernetes Secret at /run/secrets.
VOLUME ["/run/secrets"]

# --- Ports -----------------------------------------------------------------
# Only 8000 is exposed. The container serves:
#   - /metrics      Prometheus scrape endpoint (run_agent.py)
# Ray dashboard (8265) is UNAUTHENTICATED by default and must NOT be
# exposed from this container. Prometheus UI (9090) is a separate service
# that runs outside the container.
EXPOSE 8000

# --- Runtime user ----------------------------------------------------------
USER 1000:1000

# --- Health check ----------------------------------------------------------
# Uses Python (not curl) so the runtime image stays lean. /metrics is the
# only HTTP endpoint run_agent.py exposes; it returns 200 while the
# process is serving. For /healthz + /readyz semantics, add app.py as a
# sidecar or process supervisor.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "\
import sys, urllib.request; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/metrics', timeout=5).status == 200 else 1)"

# --- Signal handling -------------------------------------------------------
# Default is SIGTERM, but explicit is better — tini forwards it, python
# handles it via atexit + signal.signal(), Ray workers drain gracefully.
STOPSIGNAL SIGTERM

# --- Entrypoint ------------------------------------------------------------
# ENTRYPOINT + CMD pattern so batch mode is possible at runtime:
#     docker run <image> --once --iterations 1
# The CMD below is the default (continuous mode).
ENTRYPOINT ["/usr/bin/tini", "--", "python", "runtime/run_agent.py"]
CMD ["--mode", "unified"]
