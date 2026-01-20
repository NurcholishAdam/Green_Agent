# Add: src/agentbeats/main.py

import asyncio
from fastapi import FastAPI
from .green_agent import GreenSustainabilityAgent

app = FastAPI()
agent = GreenSustainabilityAgent()

@app.post("/a2a/task")
async def receive_task(request: dict):
    """A2A endpoint for receiving assessment requests"""
    return await agent.handle_assessment_request(request)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)