"""
Database layer — SQLite with clean schema, transactional inserts, idempotent deduplication.
Extended with double-entry accounting (chart_of_accounts, journal_entries/lines) and review queue.
In production this would be Postgres; SQLite keeps the prototype dependency-free while preserving SQL semantics.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from .models import CHART_OF_ACCOUNTS_SEED, NormalizedTransaction


DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "ledger.db"

# Schema mirrors a real accounting ledger: strict types, constraints, audit columns
DDL_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS transactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_hash        TEXT    NOT NULL UNIQUE,   -- idempotency key
    transaction_date TEXT   NOT NULL,          -- ISO date
    description     TEXT    NOT NULL,
    amount          TEXT    NOT NULL,          -- store Decimal as TEXT to preserve precision
    currency        TEXT    NOT NULL CHECK (length(currency)=3),
    merchant        TEXT,
    category        TEXT,
    reference_id    TEXT,
    source_file     TEXT    NOT NULL,
    source_row      INTEGER NOT NULL,
    warnings        TEXT,                      -- JSON array
    ingested_at     TEXT    NOT NULL,          -- ISO timestamp
    -- enrichment
    llm_category    TEXT,
    llm_confidence  REAL,
    llm_reasoning   TEXT,
    gl_account_code TEXT,
    needs_review    INTEGER DEFAULT 0,
    fuzzy_flagged   INTEGER DEFAULT 0,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_currency ON transactions(currency);
CREATE INDEX IF NOT EXISTS idx_transactions_needs_review ON transactions(needs_review);
"""

DDL_INGESTION_LOG = """
CREATE TABLE IF NOT EXISTS ingestion_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file      TEXT NOT NULL,
    total_rows       INTEGER NOT NULL,
    successful       INTEGER NOT NULL,
    rejected         INTEGER NOT NULL,
    duplicates_skipped INTEGER NOT NULL,
    fuzzy_flagged    INTEGER DEFAULT 0,
    needs_review_count INTEGER DEFAULT 0,
    duration_seconds REAL NOT NULL,
    started_at       TEXT NOT NULL,
    finished_at      TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

DDL_CHART = """
CREATE TABLE IF NOT EXISTS chart_of_accounts (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('asset','liability','equity','revenue','expense')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

DDL_JOURNAL_ENTRIES = """
CREATE TABLE IF NOT EXISTS journal_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id  INTEGER REFERENCES transactions(id) ON DELETE CASCADE,
    entry_date      TEXT NOT NULL,
    description     TEXT NOT NULL,
    reference_id    TEXT,
    source_file     TEXT NOT NULL,
    source_row      INTEGER NOT NULL,
    currency        TEXT NOT NULL,
    amount          TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_journal_entry_date ON journal_entries(entry_date);
"""

DDL_JOURNAL_LINES = """
CREATE TABLE IF NOT EXISTS journal_lines (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id        INTEGER NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
    account_code    TEXT NOT NULL REFERENCES chart_of_accounts(code),
    account_name    TEXT NOT NULL,
    debit           TEXT NOT NULL DEFAULT '0.00',
    credit          TEXT NOT NULL DEFAULT '0.00',
    currency        TEXT NOT NULL,
    description     TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    CHECK ( (debit != '0.00' AND credit = '0.00') OR (debit = '0.00' AND credit != '0.00') )
);
CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account ON journal_lines(account_code);
"""

DDL_REJECTED = """
CREATE TABLE IF NOT EXISTS rejected_rows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file     TEXT NOT NULL,
    source_row      INTEGER NOT NULL,
    raw_data        TEXT NOT NULL, -- JSON
    error_type      TEXT NOT NULL,
    error_message   TEXT NOT NULL,
    field_errors    TEXT, -- JSON
    warnings        TEXT, -- JSON
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','resolved','dismissed')),
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    resolved_at     TEXT,
    resolution_note TEXT
);
CREATE INDEX IF NOT EXISTS idx_rejected_status ON rejected_rows(status);
CREATE INDEX IF NOT EXISTS idx_rejected_source ON rejected_rows(source_file);
"""


def get_connection(db_path: Path | str | None = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Create a connection with sensible defaults (WAL, FK, typed)."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db(db_path: Path | str | None = DEFAULT_DB_PATH) -> None:
    """Idempotent schema creation + seed chart of accounts."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    conn = get_connection(db_path)
    try:
        conn.executescript(DDL_TRANSACTIONS)
        conn.executescript(DDL_INGESTION_LOG)
        conn.executescript(DDL_CHART)
        conn.executescript(DDL_JOURNAL_ENTRIES)
        conn.executescript(DDL_JOURNAL_LINES)
        conn.executescript(DDL_REJECTED)
        conn.commit()

        # Handle legacy DB without new columns (alter if missing)
        # Check transactions columns
        cur = conn.execute("PRAGMA table_info(transactions)")
        cols = {row[1] for row in cur.fetchall()}
        for col, ddl in [
            ("llm_category", "ALTER TABLE transactions ADD COLUMN llm_category TEXT"),
            ("llm_confidence", "ALTER TABLE transactions ADD COLUMN llm_confidence REAL"),
            ("llm_reasoning", "ALTER TABLE transactions ADD COLUMN llm_reasoning TEXT"),
            ("gl_account_code", "ALTER TABLE transactions ADD COLUMN gl_account_code TEXT"),
            ("needs_review", "ALTER TABLE transactions ADD COLUMN needs_review INTEGER DEFAULT 0"),
            ("fuzzy_flagged", "ALTER TABLE transactions ADD COLUMN fuzzy_flagged INTEGER DEFAULT 0"),
        ]:
            if col not in cols:
                conn.execute(ddl)
        # ingestion_log new cols
        cur = conn.execute("PRAGMA table_info(ingestion_log)")
        cols2 = {row[1] for row in cur.fetchall()}
        for col, ddl in [
            ("fuzzy_flagged", "ALTER TABLE ingestion_log ADD COLUMN fuzzy_flagged INTEGER DEFAULT 0"),
            ("needs_review_count", "ALTER TABLE ingestion_log ADD COLUMN needs_review_count INTEGER DEFAULT 0"),
        ]:
            if col not in cols2:
                conn.execute(ddl)
        conn.commit()

        # Seed chart of accounts idempotently
        for acct in CHART_OF_ACCOUNTS_SEED:
            conn.execute(
                "INSERT OR IGNORE INTO chart_of_accounts (code, name, type) VALUES (?, ?, ?)",
                (acct["code"], acct["name"], acct["type"]),
            )
        conn.commit()
    finally:
        conn.close()


def insert_transaction(conn: sqlite3.Connection, tx: NormalizedTransaction) -> tuple[Optional[int], bool]:
    """
    Insert a single transaction with idempotency.
    Returns (inserted_id, was_duplicate).
    Uses INSERT ... ON CONFLICT DO NOTHING semantics via raw_hash UNIQUE.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO transactions
                (raw_hash, transaction_date, description, amount, currency, merchant, category, reference_id, source_file, source_row, warnings, ingested_at, llm_category, llm_confidence, llm_reasoning, gl_account_code, needs_review, fuzzy_flagged)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tx.raw_hash,
                tx.transaction_date.isoformat(),
                tx.description,
                str(tx.amount),
                tx.currency,
                tx.merchant,
                tx.category,
                tx.reference_id,
                tx.source_file,
                tx.source_row,
                json.dumps(tx.warnings),
                tx.ingested_at.isoformat(),
                tx.llm_category,
                tx.llm_confidence,
                tx.llm_reasoning,
                tx.gl_account_code,
                1 if tx.needs_review else 0,
                0,  # fuzzy_flagged set later if needed
            ),
        )
        return cur.lastrowid, False
    except sqlite3.IntegrityError as e:
        # UNIQUE constraint on raw_hash → duplicate
        if "raw_hash" in str(e) or "UNIQUE" in str(e):
            return None, True
        raise


def bulk_insert_with_idempotency(
    transactions: list[NormalizedTransaction], db_path: Path | str = DEFAULT_DB_PATH
) -> tuple[list[int], int]:
    """
    Transactional bulk insert: all-or-nothing per batch, but duplicates are skipped, not failed.
    Returns (inserted_ids, duplicates_skipped).
    """
    if not transactions:
        return [], 0

    conn = get_connection(db_path)
    inserted: list[int] = []
    duplicates = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        for tx in transactions:
            row_id, is_dup = insert_transaction(conn, tx)
            if is_dup:
                duplicates += 1
            elif row_id:
                inserted.append(row_id)
                # Immediately create journal entry for this transaction
                create_journal_for_transaction(conn, tx, row_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return inserted, duplicates


def create_journal_for_transaction(conn: sqlite3.Connection, tx: NormalizedTransaction, transaction_id: int) -> int:
    """
    Create double-entry journal: debit expense (or credit revenue) vs credit Bank.
    Every entry is balanced: debit == credit.
    """
    from decimal import Decimal

    amount = tx.amount
    # Determine accounts: category GL vs Bank 1010
    gl_code = tx.gl_account_code or "6500"
    cur = conn.execute("SELECT name, type FROM chart_of_accounts WHERE code = ?", (gl_code,))
    row = cur.fetchone()
    gl_name = row[0] if row else "Uncategorized Expense"
    gl_type = row[1] if row else "expense"

    bank_code, bank_name = "1010", "Bank - Primary"

    # Amount positive = expense (debit expense, credit bank), negative = refund (debit bank, credit revenue/exp)
    abs_amt = str(abs(amount).quantize(Decimal("0.01")))

    # For prototype: expense type handling; revenue handling simplified
    if amount > 0:
        # Expense: Debit GL, Credit Bank
        lines = [
            (gl_code, gl_name, abs_amt, "0.00"),
            (bank_code, bank_name, "0.00", abs_amt),
        ]
    else:
        # Refund/negative: Debit Bank, Credit GL (or contra-revenue if refund category)
        # If GL is revenue type, keep as revenue credit; else treat as expense credit
        lines = [
            (bank_code, bank_name, abs_amt, "0.00"),
            (gl_code, gl_name, "0.00", abs_amt),
        ]

    cur = conn.execute(
        """
        INSERT INTO journal_entries (transaction_id, entry_date, description, reference_id, source_file, source_row, currency, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (transaction_id, tx.transaction_date.isoformat(), tx.description, tx.reference_id, tx.source_file, tx.source_row, tx.currency, str(amount)),
    )
    entry_id = cur.lastrowid  # type: ignore
    for acct_code, acct_name, debit, credit in lines:
        conn.execute(
            """
            INSERT INTO journal_lines (entry_id, account_code, account_name, debit, credit, currency, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (entry_id, acct_code, acct_name, debit, credit, tx.currency, tx.description[:100]),
        )
    return entry_id  # type: ignore


def fetch_all_transactions(db_path: Path | str = DEFAULT_DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM transactions ORDER BY transaction_date, id")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def fetch_journal_entries(db_path: Path | str = DEFAULT_DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT je.*, 
                   (SELECT json_group_array(json_object('account_code', jl.account_code, 'account_name', jl.account_name, 'debit', jl.debit, 'credit', jl.credit, 'currency', jl.currency))
                    FROM journal_lines jl WHERE jl.entry_id = je.id) as lines_json
            FROM journal_entries je ORDER BY je.entry_date, je.id
            """
        )
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            d["lines"] = json.loads(d["lines_json"]) if d["lines_json"] else []
            d.pop("lines_json", None)
            rows.append(d)
        return rows
    finally:
        conn.close()


def fetch_trial_balance(db_path: Path | str = DEFAULT_DB_PATH) -> dict:
    """Compute trial balance: sum debits/credits per account + overall balanced check."""
    conn = get_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT jl.account_code, ca.name, ca.type,
                   SUM(CAST(jl.debit AS REAL)) as total_debit,
                   SUM(CAST(jl.credit AS REAL)) as total_credit
            FROM journal_lines jl
            JOIN chart_of_accounts ca ON jl.account_code = ca.code
            GROUP BY jl.account_code
            ORDER BY jl.account_code
            """
        )
        accounts = [dict(r) for r in cur.fetchall()]
        cur2 = conn.execute("SELECT SUM(CAST(debit AS REAL)) as d, SUM(CAST(credit AS REAL)) as c FROM journal_lines")
        tot = cur2.fetchone()
        total_debit = tot[0] or 0
        total_credit = tot[1] or 0
        return {
            "accounts": accounts,
            "total_debit": round(total_debit, 2),
            "total_credit": round(total_credit, 2),
            "balanced": abs(total_debit - total_credit) < 0.01,
        }
    finally:
        conn.close()


def fetch_chart(db_path: Path | str = DEFAULT_DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM chart_of_accounts ORDER BY code")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def count_transactions(db_path: Path | str = DEFAULT_DB_PATH) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM transactions")
        return cur.fetchone()[0]
    finally:
        conn.close()


def insert_rejected_rows(rejected: list, db_path: Path | str = DEFAULT_DB_PATH) -> None:
    if not rejected:
        return
    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for r in rejected:
            # r is RejectedRow pydantic
            raw = r.model_dump() if hasattr(r, "model_dump") else r
            conn.execute(
                """
                INSERT INTO rejected_rows (source_file, source_row, raw_data, error_type, error_message, field_errors, warnings, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    raw.get("source_file"),
                    raw.get("source_row"),
                    json.dumps(raw.get("raw_data")),
                    raw.get("error_type"),
                    raw.get("error_message"),
                    json.dumps(raw.get("field_errors")),
                    json.dumps(raw.get("warnings")),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_rejected(status: str = "pending", db_path: Path | str = DEFAULT_DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM rejected_rows WHERE status = ? ORDER BY created_at DESC", (status,))
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            for k in ("raw_data", "field_errors", "warnings"):
                try:
                    r[k] = json.loads(r[k]) if isinstance(r[k], str) else r[k]
                except Exception:
                    pass
        return rows
    finally:
        conn.close()


def fetch_all_rejected(db_path: Path | str = DEFAULT_DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM rejected_rows ORDER BY created_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            for k in ("raw_data", "field_errors", "warnings"):
                try:
                    r[k] = json.loads(r[k]) if isinstance(r[k], str) else r[k]
                except Exception:
                    pass
        return rows
    finally:
        conn.close()


def resolve_rejected(row_id: int, note: str = "resolved", new_status: str = "resolved", db_path: Path | str = DEFAULT_DB_PATH) -> bool:
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "UPDATE rejected_rows SET status = ?, resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), resolution_note = ? WHERE id = ?",
            (new_status, note, row_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def clear_db(db_path: Path | str = DEFAULT_DB_PATH) -> None:
    """For testing/demo: wipe all data (keeps schema + chart)."""
    conn = get_connection(db_path)
    try:
        conn.execute("DELETE FROM journal_lines")
        conn.execute("DELETE FROM journal_entries")
        conn.execute("DELETE FROM transactions")
        conn.execute("DELETE FROM rejected_rows")
        conn.execute("DELETE FROM ingestion_log")
        conn.commit()
        # Reset autoincrement
        conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('transactions','journal_entries','journal_lines','rejected_rows','ingestion_log')")
        conn.commit()
        # Re-seed chart (in case it was cleared)
        for acct in CHART_OF_ACCOUNTS_SEED:
            conn.execute(
                "INSERT OR IGNORE INTO chart_of_accounts (code, name, type) VALUES (?, ?, ?)",
                (acct["code"], acct["name"], acct["type"]),
            )
        conn.commit()
    finally:
        conn.close()
