# Financial Data Ingestion Pipeline

Robust backend for ingesting erratic SMB financial data — mismatched CSV headers, inconsistent date formats, currency variations, and encoding issues. Validates and normalizes per row, isolates errors without failing the batch, and persists to a relational ledger with idempotency and audit trails.

Built with **Python, Pandas, Pydantic v2, SQLite (Postgres-compatible), FastAPI**. Demo runs in under 5 seconds and maintains a balanced trial balance.

Try it without cloning — one click, no local setup. Local fallback documented below.

---

## Behavior

| Input condition | Handling |
|---|---|
| Single row with invalid date `31/02/2023` or zero amount | Row is rejected and queued; remaining rows continue to process. Example: 7 of 25 rows rejected, 18 processed. |
| No double-entry structure (single transactions table) | Each transaction posts two balanced journal lines (debit = credit). Verified via `GET /trial-balance`. See `src/database.py:68`. |
| Missing `category` column | Heuristic categorization with confidence and reasoning; low confidence flagged for review. See `src/normalization.py:508`. |
| Same file uploaded twice | Deduplicated via `raw_hash = sha256(date|amount|desc|curr)` with `UNIQUE` constraint. Second run: `0 new, 18 skipped`. See `src/models.py:142`. |
| Near-duplicate descriptions (`Starbucks` vs `Starbucks Coffee`) with same amount and date | Flagged by fuzzy matching (amount tolerance, 3-day window, 82% similarity). See `src/normalization.py:560`. |
| Downstream review of failures | Rejected rows persisted to `rejected_rows` table and exposed via `GET /rejected` and `static/index.html` — resolve or dismiss without shell access. |

---

## Architecture

```
Dirty Input (CSV/JSON)
  • " TransAction Date "  header noise
  • "CafÃ© Luna" Mojibake
  • 15/01/2023 vs 01/15/2023 vs Jan 12th
  • $1,234.56 vs (123.45) vs 1.234,56
        │
        ▼
src/ingestion.py:33  load_raw_file()
  4-encoding fallback, synonym header map, empty-row skip
        │
        ▼
src/normalization.py per-field cleaners (each returns warnings)
  fix_encoding • normalize_amount (EU) • normalize_date (dayfirst heuristic)
  normalize_currency • clean_string
        │
        ▼
src/normalization.py:508  mock_llm_categorize()
  rules: staples→office (0.96), cafe→meals (0.94), taxi→travel (0.95)…
  → LLMCategorization(category, confidence, reasoning, needs_review)
  → GL account via CATEGORY_TO_ACCOUNT (6100 Office, 6150 Meals, 6200 Travel…)
        │
        ▼
src/models.py  NormalizedTransaction (+ llm_category, gl_account_code, needs_review)
  Pydantic strict: amount !=0, currency ∈ allowed, date not far-future, etc.
        │
        ▼
src/pipeline.py:240  run_pipeline() fault-tolerant loop
  for each row: process_single_row() try/except → success or RejectedRow
  fuzzy check vs DB + batch (exact hash excluded, near-miss flagged)
        │
        ▼
src/database.py  bulk_insert_with_idempotency()  (BEGIN IMMEDIATE)
  transactions (with llm, gl, needs_review, fuzzy_flagged)
  + journal_entries / journal_lines (balanced, FK to chart_of_accounts)
  + rejected_rows (status pending/resolved)
  + ingestion_log + trial balance check
        │
        ▼
Output:  data/ledger.db + logs/clean_ledger.json + /rejected UI + /journal + /trial-balance
```

**Invariants checked every ingestion:**
- `journal_lines` per entry: `SUM(debit) == SUM(credit)` (Pydantic + SQL CHECK)
- Trial balance: `SELECT SUM(debit) == SUM(credit)` → must be `balanced: true`

---

## Project structure

```
Data Ingestion/
├── src/
│   ├── models.py          # NormalizedTransaction (+LLM/GL), JournalEntry/Line, LLMCategorization, RejectedRow
│   ├── normalization.py   # header/amount/date/currency + mock_llm_categorize + fuzzy_duplicate_score
│   ├── ingestion.py       # CSV/JSON loader, header synonym map
│   ├── database.py        # SQLite WAL + chart_of_accounts (16 seeded) + journal + rejected queue + trial balance
│   ├── pipeline.py        # per-row isolation + categorization + GL + fuzzy + queue persistence
│   └── api.py             # FastAPI: /ingest, /transactions, /journal, /trial-balance, /rejected, /stats
├── static/
│   └── index.html         # Control plane: stats, upload dropzone, Transactions / Journal / Trial Balance / Review Queue
├── data/
│   ├── dirty_transactions.csv   # 25 rows — 7 rejections, 1 exact dup, 1 fuzzy, encoding/header chaos
│   ├── dirty_receipts.json      # 10 records — mixed keys (when/what/amt/store…), Mojibake, EU amounts
│   └── ledger.db                # WAL, FK, chart seeded, created on first run
├── logs/
│   ├── demo.log
│   ├── result_*.json
│   └── clean_ledger.json   # downstream consumers (with gl_account, llm_confidence)
├── run_demo.py            # End-to-end demo: CSV → re-ingest → JSON → journal → trial balance → review queue
└── requirements.txt
```

---

## Datasets — edge cases

**`dirty_transactions.csv` (25 rows)**
- Header: ` TransAction Date `, ` AMOUNT ($) `, ` Desc. ` (whitespace + parens)
- `CafÃ© Luna` → `Café Luna` repaired, `Müller GmbH` + `€` + `1.234,56` EU, `(123.45)` parens, `£`, `$`, `Dollar` alias
- Dates: `15/01/2023` DD/MM, `01/15/2023` MM/DD, `Jan 22 2023`, `20.01.2023`, `15/02/23`, `03/04/2023` ambiguous (logs MM/DD vs DD/MM)
- Generics: empty `,,, , , ,` skipped, `$0` zero, `31/02/2023` invalid, `N/A` missing, `2099` far-future, `1890` ancient, `$15M` sanity bound, empty desc+merchant, duplicate `Dup Co` whitespace variant

Result first run: `18 success, 7 rejected, 1 exact dup skipped, 1 fuzzy flagged, 4 needs_review`

**`dirty_receipts.json` (10 records)** — keys vary per record (`date` vs `Transaction Date` vs `when` vs `booking date`…), same synonym map, plus `CafÃ©`, `€ 22,50`, `¥ 15000`, `(55.00)` refund.

Result: `7 success, 3 rejected`

---

## Try without installing

**Option A — Codespaces (1 click, no setup):**  
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/immortaldarkveil/minerva-ingestion-pipeline?quickstart=1) — Wait for `postCreate` to finish (`python run_demo.py` runs automatically), then **Ports → 8000 → Open in Browser**.

**Option B — Deployed demo:**  
If you have a hosted URL (Render/Railway/Fly), set `DEMO_URL` below. Otherwise the API runs wherever you deploy `Dockerfile` — it listens on `$PORT`:
```bash
docker build -t ingestion .
docker run -p 8000:8000 ingestion
# → http://localhost:8000/ (upload, Transactions, Journal, Trial Balance, Review Queue)
```

Deploy button (Render):
```yaml
# render.yaml is included — connect the repo to Render → New Web Service → Free
# Build: pip install -r requirements.txt
# Start: uvicorn src.api:app --host 0.0.0.0 --port $PORT
```

## Run locally

```bash
# 1. Install
pip install -r requirements.txt

# 2. Demo (clears DB, seeds chart, checks idempotency and trial balance)
python run_demo.py
# → console + logs/demo.log + logs/clean_ledger.json (with gl_account, llm_confidence, needs_review)

# 3. API + UI
uvicorn src.api:app --reload
# → http://localhost:8000/          (upload, Transactions, Journal, Trial Balance, Review Queue)
# → http://localhost:8000/docs       (OpenAPI)
# → http://localhost:8000/health     (health)
```

**API examples** (replace `http://localhost:8000` with your deployed URL if using a host):
```bash
BASE="http://localhost:8000"  # or https://your-app.onrender.com
# Upload
curl -F "file=@data/dirty_transactions.csv" $BASE/ingest | jq
# Ledger (only needs review)
curl "$BASE/transactions?needs_review=true" | jq
# Journal (double-entry)
curl $BASE/journal | jq
# Trial balance (must be balanced)
curl $BASE/trial-balance | jq
# Review queue
curl "$BASE/rejected?status=pending" | jq
curl -X POST "$BASE/rejected/1/resolve?status=resolved&note=fixed%20date"
# Stats
curl $BASE/stats | jq
```

Inspect DB directly:
```bash
sqlite3 data/ledger.db "SELECT code, name, type FROM chart_of_accounts;"
sqlite3 data/ledger.db "SELECT transaction_date, amount, currency, category, gl_account_code, llm_confidence, needs_review FROM transactions ORDER BY transaction_date LIMIT 5;"
sqlite3 data/ledger.db "SELECT entry_date, description, amount FROM journal_entries LIMIT 3;"
sqlite3 data/ledger.db "SELECT jl.account_code, ca.name, SUM(CAST(debit AS REAL)), SUM(CAST(credit AS REAL)) FROM journal_lines jl JOIN chart_of_accounts ca ON jl.account_code=ca.code GROUP BY jl.account_code;"
```

---

## Design notes

- **Ledger integrity:** `chart_of_accounts` (1010 Bank, 6100 Office, etc.) and two-line journal entries with trial balance verification prevent silent drift.
- **Categorization:** heuristic model returns `confidence` and `reasoning`; low confidence is gated to `needs_review` rather than auto-posted. Swap `mock_llm_categorize` for a function-calling LLM with the same interface.
- **Workflow:** rejections are queued with `pending/resolved` states and a UI for resolution, keeping operator work out of log files.
- **Operations:** `raw_hash` idempotency, fuzzy near-miss detection, WAL, `BEGIN IMMEDIATE`, and `ingestion_log` provide a path to Postgres, queuing, and metrics.

---

## Production considerations

- Postgres `ON CONFLICT (raw_hash) DO NOTHING`, advisory locks, queue and DLQ for `rejected_rows`, metrics
- Replace `mock_llm_categorize` with an LLM + evaluation set (precision/recall per category)
- FX: `fx_rates` table, store `base_amount`
- Auth, rate limiting, tenant `org_id` column

---

## License

MIT
