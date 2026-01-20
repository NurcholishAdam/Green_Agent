from fastapi import FastAPI
from .green_agent import GreenSustainabilityAgent

app = FastAPI()
agent = GreenSustainabilityAgent()

@app.post("/a2a/task")
async def receive_task(request: dict):
    """A2A endpoint for receiving assessment requests"""
    # ✓ REQUIRED: This endpoint must exist
    return await agent.handle_assessment_request(request)

@app.get("/a2a/task/{task_id}")
async def get_task_status(task_id: str):
    """A2A endpoint for checking task status"""
    # ✓ REQUIRED: Return task status
    pass

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    # ✓ REQUIRED: Return {"status": "healthy"}
    return {"status": "healthy"}

@app.get("/metrics")
async def metrics():
    """Prometheus-style metrics (optional)"""
    # ⚪ OPTIONAL but recommended
    pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
