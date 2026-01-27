FROM python:3.11-slim

# --------------------------------------------------
# System dependencies
# --------------------------------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# Working directory
# --------------------------------------------------
WORKDIR /app

# --------------------------------------------------
# Python dependencies
# --------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------
# Copy full repo (IMPORTANT)
# --------------------------------------------------
COPY . .

# --------------------------------------------------
# Runtime environment
# --------------------------------------------------
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# --------------------------------------------------
# Default execution
# --------------------------------------------------
CMD ["python", "run_agent.py", "--config", "config.json"]
