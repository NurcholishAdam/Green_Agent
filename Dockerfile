FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_agent.py .
COPY agentbeats.json .

ENTRYPOINT ["python", "run_agent.py"]
