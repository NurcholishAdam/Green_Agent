# syntax=docker/dockerfile:1.7
#
# Green Agent — Dockerfile
# ========================
#
# Standalone Green Agent production image.
# No AgentBeats configuration or event integration.
#
# Build:
#   docker build --build-arg ENHANCEMENTS_ENABLED=true -t green-agent:full .
#   docker build --build-arg ENHANCEMENTS_ENABLED=false -t green-agent:lean .

ARG PYTHON_IMAGE=python:3.12.3-slim@sha256:afc139a0a640942491ec481ad8dda10f2c5b753f5c969393b12480155fe15a63


# =============================================================================
# Stage 1 — builder
# =============================================================================
FROM ${PYTHON_IMAGE} AS builder

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements*.txt ./
COPY pyproject.toml ./

# Copy the application source
COPY src/ ./src/

# Copy the enhancements source from the nested LIMIT Graph integration tree.
# This is required for `import src.enhancements.core`.
COPY quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/ \
     ./src/enhancements/

# Ensure every package directory is importable.
RUN set -e; \
    touch src/__init__.py; \
    touch src/enhancements/__init__.py; \
    if [ ! -f src/enhancements/core/__init__.py ]; then \
        echo "ERROR: src/enhancements/core/__init__.py is missing" >&2; \
        find src/enhancements -maxdepth 2 -type f -print >&2; \
        exit 1; \
    fi

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


# =============================================================================
# Stage 2 — runtime
# =============================================================================
FROM ${PYTHON_IMAGE} AS runtime

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG BUILD_DATE
ARG VCS_REF
ARG BUILD_VERSION=5.0.0

RUN apt-get update && apt-get install -y --no-install-recommends \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 appuser \
    && useradd --uid 10001 --gid appuser --create-home --shell /bin/bash appuser \
    && id appuser

COPY --from=builder /install /usr/local

WORKDIR /app

COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser run_agent.py ./run_agent.py

# Fail the image build if the required module is absent.
RUN python -c "import src.enhancements.core; print('src.enhancements.core import OK')"

# Copy optional repository files only when they exist.
RUN --mount=type=bind,source=.,target=/ctx,ro \
    set -e; \
    for file in \
        green_policy.yaml \
        symbolic_policy.yaml \
        free_apis_config.yaml \
        test_symbolic_oversight.py \
        test_meta_cognitive.py \
        docker_metrics_collector.py \
    ; do \
        if [ -f "/ctx/${file}" ]; then \
            cp "/ctx/${file}" "/app/${file}"; \
            chown appuser:appuser "/app/${file}"; \
            echo "Copied /app/${file}"; \
        else \
            echo "Skipped /app/${file} (not present in build context)"; \
        fi; \
    done

RUN mkdir -p /app/checkpoints /out \
    && chown -R appuser:appuser /app/checkpoints /out

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

RUN cat > /usr/local/bin/docker-entrypoint.sh <<'ENTRYPOINT_EOF'
#!/bin/bash
set -euo pipefail

echo "[entrypoint] Verifying required Green Agent modules..." >&2

python - <<'PY'
import importlib
import sys

required_modules = [
    "src.enhancements.core",
]

missing = []

for module_name in required_modules:
    try:
        importlib.import_module(module_name)
        print(f"[entrypoint] OK: {module_name}")
    except Exception as error:
        missing.append(module_name)
        print(f"[entrypoint] MISSING: {module_name}: {error}", file=sys.stderr)

if missing:
    print(
        "[entrypoint] Fatal: required Green Agent modules are unavailable: "
        + ", ".join(missing),
        file=sys.stderr,
    )
    sys.exit(1)
PY

if [[ -n "${MODP_WEIGHTS:-}" ]]; then
    if ! python - <<'PY'
import os
import sys

raw = os.environ["MODP_WEIGHTS"]

try:
    parts = [part.strip() for part in raw.strip("[]").split(",") if part.strip()]
    values = [float(part) for part in parts]

    assert values, "MODP_WEIGHTS is empty"
    assert abs(sum(values) - 1.0) < 1e-6, (
        f"MODP_WEIGHTS must sum to 1.0; got {sum(values)}"
    )
except Exception as error:
    print(
        f"[entrypoint] Invalid MODP_WEIGHTS ({raw!r}): {error}",
        file=sys.stderr,
    )
    sys.exit(1)
PY
    then
        echo "[entrypoint] Fatal: MODP_WEIGHTS is invalid" >&2
        exit 1
    fi
fi

if [[ "${FLEXGEN_ENABLED:-false}" == "false" && -n "${FLEXGEN_MODEL_NAME:-}" ]]; then
    echo "[entrypoint] Warning: FLEXGEN_ENABLED=false, ignoring FLEXGEN configuration" >&2
fi

if [[ "${ZERO_TRUST_ENABLED:-false}" == "true" && -z "${ZERO_TRUST_TOKEN:-}" ]]; then
    echo "[entrypoint] Fatal: ZERO_TRUST_ENABLED=true but ZERO_TRUST_TOKEN is not set" >&2
    exit 1
fi

echo "[entrypoint] Green Agent enhancement configuration:" >&2

for flag in \
    ENHANCEMENTS_ENABLED \
    LIMIT_GRAPH_ENABLED \
    MODP_ENABLED \
    RLHF_ENABLED \
    DISTILLATION_ENABLED \
    MOE_ENABLED \
    EVOLUTIONARY_ENABLED \
    ZERO_TRUST_ENABLED \
    DIFFERENTIAL_PRIVACY_ENABLED \
    TRAINING_ENABLED \
    FLEXGEN_ENABLED
do
    printf '  %-32s = %s\n' "${flag}" "${!flag:-unset}" >&2
done

exec "$@"
ENTRYPOINT_EOF

RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

LABEL org.opencontainers.image.title="green-agent" \
      org.opencontainers.image.description="Green Agent — carbon-aware advisory orchestration" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/NurcholishAdam/Green_Agent" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.green-agent.enhancements="true"

VOLUME ["/sys/fs/cgroup"]

EXPOSE 8000
STOPSIGNAL SIGTERM

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import os,sys,urllib.request; port=os.environ.get('AGENT_PORT','8000'); path=os.environ.get('AGENT_HEALTH_PATH','/health'); sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=3).status==200 else 1)"

RUN printf '#!/bin/sh\nset -e\nexec "$@"\n' > /tmp/docker-entrypoint.sh && chmod +x /tmp/docker-entrypoint.sh

ENTRYPOINT ["/tmp/docker-entrypoint.sh"]

CMD ["python", "run_agent.py"]
