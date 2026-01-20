# Add: Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy green agent code
COPY src/ ./src/
COPY config/ ./config/

# Expose A2A endpoint
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s \
  CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Start green agent
CMD ["python", "-m", "src.agentbeats.main"]
