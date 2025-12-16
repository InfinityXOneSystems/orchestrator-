import os
import uuid
import time
import httpx
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn

# ==============================
# ENV CONFIG (MINIMAL & SAFE)
# ==============================

CRAWLER_URL = os.getenv("CRAWLER_URL")            # e.g. https://crawler-scraper.run.app/run
RETRIEVAL_URL = os.getenv("RETRIEVAL_URL")        # e.g. https://retrieval-service.run.app/ingest
INTELLIGENCE_URL = os.getenv("INTEL_URL")         # optional downstream consumer
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))

if not CRAWLER_URL or not RETRIEVAL_URL:
    raise RuntimeError("CRAWLER_URL and RETRIEVAL_URL must be set")

# ==============================
# FASTAPI INIT
# ==============================

app = FastAPI(
    title="Infinity Universal Orchestrator",
    version="1.0.0",
    description="Stateless orchestration service for crawlers, retrieval, and intelligence pipelines"
)

# ==============================
# MODELS
# ==============================

class OrchestrationRequest(BaseModel):
    job_type: str = Field(..., example="crawl")
    domain: str = Field(..., example="real_estate")
    source: str = Field(..., example="county_records")
    params: Dict[str, Any] = Field(default_factory=dict)
    callback: Optional[str] = Field(None, description="Optional webhook URL")

class OrchestrationResult(BaseModel):
    job_id: str
    status: str
    duration_seconds: float
    records_processed: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)

# ==============================
# HEALTH CHECK
# ==============================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "orchestrator",
        "timestamp": int(time.time())
    }

# ==============================
# CORE ORCHESTRATION
# ==============================

@app.post("/orchestrate", response_model=OrchestrationResult)
async def orchestrate(request: OrchestrationRequest):
    job_id = f"job-{uuid.uuid4()}"
    start_time = time.time()

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            # ---- STEP 1: TRIGGER CRAWLER ----
            crawler_payload = {
                "job_id": job_id,
                "domain": request.domain,
                "source": request.source,
                "params": request.params
            }
            crawler_resp = await client.post(CRAWLER_URL, json=crawler_payload)
            crawler_resp.raise_for_status()
            crawler_data = crawler_resp.json()
            records = crawler_data.get("records", [])
            record_count = len(records)

            # ---- STEP 2: SEND TO RETRIEVAL ----
            retrieval_payload = {
                "job_id": job_id,
                "domain": request.domain,
                "source": request.source,
                "records": records
            }
            retrieval_resp = await client.post(RETRIEVAL_URL, json=retrieval_payload)
            retrieval_resp.raise_for_status()

            # ---- STEP 3: OPTIONAL INTELLIGENCE CALLBACK ----
            if INTELLIGENCE_URL:
                await client.post(INTELLIGENCE_URL, json={
                    "job_id": job_id,
                    "domain": request.domain,
                    "source": request.source,
                    "record_count": record_count
                })

            # ---- STEP 4: OPTIONAL WEBHOOK CALLBACK ----
            if request.callback:
                await client.post(request.callback, json={
                    "job_id": job_id,
                    "status": "completed",
                    "records": record_count
                })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    duration = round(time.time() - start_time, 2)
    return OrchestrationResult(
        job_id=job_id,
        status="completed",
        duration_seconds=duration,
        records_processed=record_count,
        metadata={
            "domain": request.domain,
            "source": request.source
        }
    )

# ==============================
# ENTRYPOINT (CLOUD RUN / DOCKER)
# ==============================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        log_level="info"
    )
