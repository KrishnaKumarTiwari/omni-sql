import time
import uuid
import os
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from prototype.engine import FederatedEngine
from prototype.utils.security import SecurityEnforcer

app = FastAPI(title="OmniSQL Prototype Gateway")
engine = FederatedEngine()

# Metrics
QUERY_COUNT = Counter('omnisql_queries_total', 'Total SQL queries processed', ['status'])
QUERY_LATENCY = Histogram('omnisql_query_latency_seconds', 'Query execution latency')

class QueryRequest(BaseModel):
    sql: str
    metadata: Optional[Dict[str, Any]] = {}

@app.get("/")
async def get_console():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

@app.get("/metrics")
async def metrics():
    return JSONResponse(content=generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)

@app.get("/metrics/dashboard")
async def metrics_dashboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "metrics.html"))

@app.post("/v1/query")
async def execute_query(request: QueryRequest, x_user_token: str = Header(None)):
    trace_id = request.metadata.get("trace_id", str(uuid.uuid4()))
    max_staleness_ms = request.metadata.get("max_staleness_ms", 0)
    
    # 1. AuthN
    user_context = SecurityEnforcer.authenticate(x_user_token)
    if user_context["role"] == "guest":
        QUERY_COUNT.labels(status="401").inc()
        raise HTTPException(status_code=401, detail="Invalid user token")

    # 2. Execute with Latency tracking
    start_time = time.time()
    try:
        result = engine.execute_query(user_context, max_staleness_ms, request.sql)
        
        # Guard for rate limits from connectors
        if "error" in result and result.get("status_code") == 429:
            QUERY_COUNT.labels(status="429").inc()
            return JSONResponse(
                status_code=429,
                headers={"Retry-After": str(result.get("retry_after", 5))},
                content={
                    "error": "RATE_LIMIT_EXHAUSTED",
                    "details": "A downstream SaaS connector budget has been exceeded.",
                    "rate_limit_status": result.get("rate_limit_status"),
                    "trace_id": trace_id
                }
            )

        duration = time.time() - start_time
        QUERY_LATENCY.observe(duration)
        QUERY_COUNT.labels(status="200").inc()

        # Add trace metadata to result
        result["trace_id"] = trace_id
        return result

    except Exception as e:
        QUERY_COUNT.labels(status="500").inc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
