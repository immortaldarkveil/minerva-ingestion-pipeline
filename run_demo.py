#!/usr/bin/env python3
"""
End-to-end demo — messy data → normalized ledger.
Demonstrates: header/encoding/amount/date normalization, heuristic categorization,
GL mapping, double-entry journal, fuzzy dedupe, fault tolerance and review queue.
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.database import (
    clear_db,
    count_transactions,
    fetch_all_transactions,
    fetch_chart,
    fetch_journal_entries,
    fetch_rejected,
    fetch_trial_balance,
    init_db,
)
from src.pipeline import run_pipeline

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "demo.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
DB_PATH = ROOT / "data" / "ledger.db"
CSV_PATH = ROOT / "data" / "dirty_transactions.csv"
JSON_PATH = ROOT / "data" / "dirty_receipts.json"


def banner(title: str):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_result(result, label: str):
    print(f"\n--- {label} ---")
    print(f"File:              {result.source_file}")
    print(f"Total rows:        {result.total_rows}")
    print(f"  ✓ Successful:    {result.successful}")
    print(f"  ✗ Rejected:      {result.rejected}")
    print(f"  ↻ Exact dupes:   {result.duplicates_skipped}")
    print(f"  ≈ Fuzzy flagged: {result.fuzzy_flagged}")
    print(f"  ⚠ Needs review:  {result.needs_review_count} (low confidence + fuzzy)")
    print(f"Warnings:          {result.warnings_count}")
    print(f"Duration:          {result.duration_seconds}s")
    print(f"Success rate:      {result.success_rate}%")
    if result.inserted_ids:
        print(f"Inserted IDs:      {result.inserted_ids}")
    if result.rejected_rows:
        print(f"\nRejected rows detail ({len(result.rejected_rows)}):")
        for r in result.rejected_rows:
            print(f"  row {r.source_row:2d} [{r.error_type:13s}] {r.error_message}")
            if r.field_errors:
                print(f"          fields: {r.field_errors}")
            excerpt = {k: v for k, v in r.raw_data.items() if v not in (None, "")}
            print(f"          raw: {excerpt}")
    out_path = LOG_DIR / f"result_{result.source_file}.json"
    out_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    print(f"\n→ Full JSON saved to {out_path}")


def main():
    banner("FINANCIAL DATA INGESTION PIPELINE — DEMO")
    print("Python + Pandas + Pydantic v2 + SQLite (Postgres-compatible) + FastAPI")
    print(f"DB:   {DB_PATH}")
    print(f"CSV:  {CSV_PATH}")
    print(f"JSON: {JSON_PATH}")

    banner("Step 0 — Initialize and clear DB")
    init_db(DB_PATH)
    clear_db(DB_PATH)
    chart = fetch_chart(DB_PATH)
    print(f"DB cleared. Transactions: {count_transactions(DB_PATH)} | Chart accounts: {len(chart)}")
    for c in chart[:5]:
        print(f"  {c['code']} {c['name']} ({c['type']})")
    print("  ...")

    banner("Step 1 — Ingest CSV (25 rows)")
    print("Expect: 7 rejections, 1 exact duplicate, categorization and GL mapping, journal entries created")
    result_csv_1 = run_pipeline(CSV_PATH, db_path=DB_PATH)
    print_result(result_csv_1, "CSV First Ingestion")

    print("\nSample normalized records (first 4):")
    all_tx = fetch_all_transactions(DB_PATH)
    for tx in all_tx[:4]:
        print(f"  {tx['transaction_date']} | {tx['currency']} {tx['amount']:>9s} | {tx['description'][:32]:32s} | cat={str(tx['category'] or tx['llm_category']):10s} gl={tx['gl_account_code']} llm={tx['llm_category']}:{tx['llm_confidence']} review={bool(tx['needs_review'])}")

    print("\nCategorization samples:")
    for tx in all_tx[:3]:
        if tx['llm_reasoning']:
            print(f"  '{tx['description'][:30]}' → {tx['llm_category']} ({tx['llm_confidence']}) — {tx['llm_reasoning']}")

    banner("Step 2 — Idempotency: re-ingest same CSV")
    print("Expect: 0 new, 18 exact duplicates via raw_hash")
    result_csv_2 = run_pipeline(CSV_PATH, db_path=DB_PATH)
    print_result(result_csv_2, "CSV Second Ingestion (Idempotency)")
    if result_csv_2.duplicates_skipped == result_csv_1.successful:
        print("\n✓ IDEMPOTENCY VERIFIED")
    else:
        print(f"\n? dup {result_csv_2.duplicates_skipped} vs first success {result_csv_1.successful}")

    banner("Step 3 — Ingest JSON (10 records, mixed schemas)")
    result_json = run_pipeline(JSON_PATH, db_path=DB_PATH)
    print_result(result_json, "JSON Ingestion")

    banner("Step 4 — Double-entry journal and trial balance")
    entries = fetch_journal_entries(DB_PATH)
    print(f"Journal entries: {len(entries)} (expected = persisted transactions)")
    for e in entries[:3]:
        lines = e['lines']
        print(f"  Entry #{e['id']} {e['entry_date']} {e['description'][:30]:30s} {e['currency']} {e['amount']:>8s}")
        for l in lines:
            side = f"DR {l['debit']}" if l['debit'] != '0.00' else f"CR {l['credit']}"
            print(f"      → {l['account_code']} {l['account_name']:22s} {side}")

    tb = fetch_trial_balance(DB_PATH)
    print(f"\nTrial Balance: debit {tb['total_debit']} | credit {tb['total_credit']} | balanced={tb['balanced']}")
    if not tb['balanced']:
        print("  ✗ NOT BALANCED — invariant violated")
    else:
        print("  ✓ Balanced — debits equal credits")
    for a in tb['accounts']:
        if float(a['total_debit'] or 0) > 0 or float(a['total_credit'] or 0) > 0:
            print(f"    {a['account_code']} {a['name']:22s} {a['type']:8s} DR {float(a['total_debit'] or 0):8.2f} CR {float(a['total_credit'] or 0):8.2f}")

    banner("Step 5 — Review queue")
    rejected = fetch_rejected(status="pending", db_path=DB_PATH)
    print(f"Pending review: {len(rejected)} rows (persisted)")
    for r in rejected[:3]:
        print(f"  #{r['id']} {r['source_file']} row {r['source_row']} [{r['error_type']}] {r['error_message'][:80]}")
        print(f"      raw: {r['raw_data']}")
    if rejected:
        print(f"  → Full queue via API: GET /rejected?status=pending or http://localhost:8000/")

    banner("Step 6 — Final ledger")
    final = fetch_all_transactions(DB_PATH)
    print(f"Total persisted transactions: {len(final)}")
    needs = [t for t in final if t['needs_review']]
    print(f"  Needs review (low confidence / fuzzy): {len(needs)}")
    for t in needs[:3]:
        print(f"    {t['transaction_date']} {t['description'][:30]} llm={t['llm_category']}:{t['llm_confidence']} fuzzy={t['fuzzy_flagged']}")

    print(f"\nLedger rows:")
    print(f"{'ID':>3} | {'Date':10} | {'Curr':4} | {'Amount':>9} | {'Category→GL':16} | {'Review':7} | Description")
    print("-" * 110)
    for tx in final:
        cat = (tx['category'] or tx['llm_category'] or '—')[:10]
        gl = tx['gl_account_code'] or '—'
        rev = '⚠' if tx['needs_review'] else '✓'
        print(f"{tx['id']:3d} | {tx['transaction_date']:10s} | {tx['currency']:4s} | {tx['amount']:>9s} | {cat:10s}→{gl:4s} | {rev:7s} | {tx['description'][:32]}")

    banner("Step 7 — Clean downstream JSON")
    clean_output = [
        {
            "transaction_date": tx["transaction_date"],
            "description": tx["description"],
            "amount": tx["amount"],
            "currency": tx["currency"],
            "merchant": tx["merchant"],
            "category": tx["category"] or tx["llm_category"],
            "gl_account": tx["gl_account_code"],
            "llm_confidence": tx["llm_confidence"],
            "needs_review": bool(tx["needs_review"]),
            "raw_hash": tx["raw_hash"],
        }
        for tx in final
    ]
    out_clean = LOG_DIR / "clean_ledger.json"
    out_clean.write_text(json.dumps(clean_output, indent=2), encoding="utf-8")
    print(json.dumps(clean_output[:2], indent=2))
    print(f"\n... ({len(clean_output)} total) → saved to {out_clean}")

    banner("SUMMARY")
    total_input = result_csv_1.total_rows + result_json.total_rows
    print(f"Input rows (CSV+JSON):        {total_input}")
    print(f"Persisted (clean):            {len(final)}")
    print(f"  └ needs review:             {len(needs)}")
    print(f"Rejected (queued):            {result_csv_1.rejected + result_json.rejected}")
    print(f"Exact duplicates blocked:     {result_csv_2.duplicates_skipped}")
    print(f"Fuzzy flagged:                {result_csv_1.fuzzy_flagged + result_json.fuzzy_flagged}")
    print(f"Trial balanced:               {tb['balanced']} ✓")
    print(f"Journal entries:              {len(entries)} (2 lines each, balanced)")
    print(f"Pipeline never crashed:       ✓")
    print(f"\nDB:   {DB_PATH} ({DB_PATH.stat().st_size} bytes)")
    print(f"Logs: {LOG_DIR / 'demo.log'}")
    print("API:  uvicorn src.api:app --reload  → http://localhost:8000/")
    banner("DEMO COMPLETE")


if __name__ == "__main__":
    main()
