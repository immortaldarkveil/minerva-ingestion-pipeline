"""
FastAPI review-queue API.
Provides endpoints to triage rejected rows without tailing logs.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .database import (
    DEFAULT_DB_PATH,
    fetch_all_rejected,
    fetch_all_transactions,
    fetch_chart,
    fetch_journal_entries,
    fetch_rejected,
    fetch_trial_balance,
    get_connection,
    init_db,
    resolve_rejected,
)
from .pipeline import run_pipeline

# Ensure DB and chart are ready (important for Vercel's ephemeral /tmp)
try:
    init_db()
except Exception:
    pass

app = FastAPI(
    title="Ingestion API",
    description="Idempotent ingestion with double-entry ledger, categorization, and review queue",
    version="0.2.0",
)

# CORS for local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "version": app.version}


@app.post("/ingest", summary="Ingest CSV or JSON file (multipart)")
async def ingest_file(file: UploadFile = File(...), db_path: Optional[str] = None):
    # Validate extension
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".csv", ".json"):
        raise HTTPException(status_code=400, detail="Only .csv and .json supported")
    # Save to temp file so pipeline can handle path-based ingestion
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)
    try:
        result = run_pipeline(tmp_path, db_path=db_path or DEFAULT_DB_PATH)
        # Override source_file to original filename for UI clarity
        result.source_file = file.filename or tmp_path.name
        return JSONResponse(content=json.loads(result.model_dump_json()))
    finally:
        try:
            tmp_path.unlink()
        except Exception:
            pass


@app.get("/transactions", summary="List ledger (with categorization and review flags)")
def list_transactions(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    needs_review: Optional[bool] = None,
    currency: Optional[str] = None,
):
    rows = fetch_all_transactions()
    # In-memory filter for prototype (Postgres would push down)
    if needs_review is not None:
        flag = 1 if needs_review else 0
        rows = [r for r in rows if r.get("needs_review") == flag]
    if currency:
        rows = [r for r in rows if r.get("currency") == currency.upper()]
    total = len(rows)
    return {"total": total, "limit": limit, "offset": offset, "data": rows[offset : offset + limit]}


@app.get("/journal", summary="Double-entry journal entries + lines")
def list_journal():
    entries = fetch_journal_entries()
    return {"total": len(entries), "data": entries}


@app.get("/trial-balance", summary="Trial balance — must be balanced")
def trial_balance():
    tb = fetch_trial_balance()
    return tb


@app.get("/chart", summary="Chart of accounts")
def chart():
    return {"data": fetch_chart()}


@app.get("/rejected", summary="Review queue — rejected rows")
def list_rejected(status: str = Query("pending", enum=["pending", "resolved", "dismissed", "all"])):
    if status == "all":
        rows = fetch_all_rejected()
    else:
        rows = fetch_rejected(status=status)
    return {"total": len(rows), "status": status, "data": rows}


@app.get("/rejected/{row_id}")
def get_rejected(row_id: int):
    # Simple fetch all and filter for prototype
    all_rows = fetch_all_rejected()
    for r in all_rows:
        if r["id"] == row_id:
            return r
    raise HTTPException(status_code=404, detail="Rejected row not found")


@app.post("/rejected/{row_id}/resolve", summary="Mark rejected row as resolved/dismissed")
def resolve(row_id: int, note: str = Query("resolved via review queue"), status: str = Query("resolved", enum=["resolved", "dismissed"])):
    ok = resolve_rejected(row_id, note=note, new_status=status)
    if not ok:
        raise HTTPException(status_code=404, detail="Row not found or already resolved")
    return {"id": row_id, "status": status, "note": note}


@app.get("/ingestions", summary="Ingestion log history")
def ingestions():
    conn = get_connection()
    try:
        conn.row_factory = __import__("sqlite3").Row
        cur = conn.execute("SELECT * FROM ingestion_log ORDER BY created_at DESC LIMIT 50")
        rows = [dict(r) for r in cur.fetchall()]
        return {"total": len(rows), "data": rows}
    finally:
        conn.close()


@app.get("/stats", summary="Operational stats")
def stats():
    tx = fetch_all_transactions()
    rej = fetch_all_rejected()
    pending = [r for r in rej if r["status"] == "pending"]
    needs_review = [t for t in tx if t.get("needs_review") == 1]
    tb = fetch_trial_balance()
    return {
        "transactions_total": len(tx),
        "transactions_needs_review": len(needs_review),
        "rejected_total": len(rej),
        "rejected_pending": len(pending),
        "trial_balanced": tb["balanced"],
        "trial_totals": {"debit": tb["total_debit"], "credit": tb["total_credit"]},
    }


# --- Serve static frontend if it exists ---
from pathlib import Path as _P

_static_dir = _P(__file__).parent.parent / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    # Try to serve frontend index if exists
    idx = _P(__file__).parent.parent / "static" / "index.html"
    if idx.exists():
        return HTMLResponse(content=idx.read_text(encoding="utf-8"))
    return HTMLResponse(
        content="""
        <html><body style="font-family: ui-sans-serif,system-ui; max-width: 700px; margin: 40px auto; line-height:1.6">
        <h1>Ingestion API v0.2.0</h1>
        <p>API is running. Frontend not yet built — see <a href="/docs">/docs</a> for OpenAPI.</p>
        <ul>
          <li><a href="/docs">Swagger UI</a></li>
          <li><a href="/health">Health</a></li>
          <li><a href="/transactions">Transactions</a></li>
          <li><a href="/journal">Journal</a></li>
          <li><a href="/trial-balance">Trial Balance</a></li>
          <li><a href="/rejected">Review Queue</a></li>
        </ul>
        <p>Run <code>python run_demo.py</code> to seed demo data.</p>
        </body></html>
        """
    )
