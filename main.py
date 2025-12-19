
# ===================== ROUTE ENFORCEMENT KERNEL =====================

from fastapi import FastAPI
import sys
import os
from datetime import datetime

app = FastAPI(title="Infinity XOS Orchestrator")

ROUTES_REQUIRED = [
    "/health",
    "/state",
    "/bootstrap",
    "/inventory",
    "/run/all",
    "/emit/ledger",
    "/emit/runs",
]

@app.on_event("startup")
def enforce_routes():
    existing = {route.path for route in app.routes}
    missing = [r for r in ROUTES_REQUIRED if r not in existing]
    print({
        "ts": datetime.utcnow().isoformat(),
        "event": "ROUTE_ENFORCEMENT_CHECK",
        "existing_routes": sorted(existing),
        "missing_routes": missing
    }, flush=True)
    if missing:
        # HARD FAIL — do not allow silent mismatch
        raise RuntimeError(f"CRITICAL: Missing routes: {missing}")

# ===================== REQUIRED ROUTES =====================

@app.get("/health")
def health():
    return {"status": "ok", "role": "LEADER"}

@app.get("/state")
def state():
    return {
        "service": "InfinityXOS",
        "mode": "AUTONOMOUS",
        "runtime": os.getenv("K_SERVICE", "local"),
    }

@app.post("/bootstrap")
def bootstrap():
    return {"status": "BOOTSTRAPPED", "ts": datetime.utcnow().isoformat()}

@app.get("/inventory")
def inventory():
    return {
        "github": bool(os.getenv("GITHUB_APP_ID")),
        "gcp": bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")),
        "openai": bool(os.getenv("OPENAI_API_KEY")),
    }

@app.post("/run/all")
def run_all():
    return {"status": "RUN_ALL_ACCEPTED"}

@app.get("/emit/ledger")
def emit_ledger():
    return {"ledger": "ACTIVE"}

@app.get("/emit/runs")
def emit_runs():
    return {"runs": "ACTIVE"}
