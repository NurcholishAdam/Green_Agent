from fastapi import FastAPI
import ray
import json

app = FastAPI()

ray.init(address="auto")

aggregator = ray.get_actor("ClusterMetricsAggregator")

@app.get("/metrics")
def get_metrics():
    summary = ray.get(aggregator.finalize.remote())
    return summary

@app.get("/pareto")
def get_pareto():
    with open("cluster_pareto.json") as f:
        return json.load(f)
