FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git curl gcc g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p config data logs results

EXPOSE 8000 8265 9090

ENV PYTHONUNBUFFERED=1 \
    MODE=unified \
    LOG_LEVEL=INFO

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "runtime/run_agent.py", "--mode", "unified"]
