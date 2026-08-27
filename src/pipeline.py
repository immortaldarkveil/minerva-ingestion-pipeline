"""
Pipeline orchestrator — fault-tolerant loop that never lets one bad row kill the batch.
Architecture: load → normalize → categorize → validate → fuzzy check → deduplicate → persist (transactions + journal + review queue)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .database import bulk_insert_with_idempotency, init_db
from .ingestion import load_raw_file
from .models import IngestionResult, NormalizedTransaction, RejectedRow, compute_raw_hash, resolve_gl_account
from .normalization import clean_string, find_fuzzy_duplicate, mock_llm_categorize, normalize_amount, normalize_currency, normalize_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-row processor — the isolation boundary
# ---------------------------------------------------------------------------

def process_single_row(
    raw: dict[str, Any],
    *,
    source_file: str,
    source_row: int,
) -> tuple[NormalizedTransaction | None, RejectedRow | None]:
    """
    Process one row in complete isolation. Never raises — returns either success or rejection.
    Includes categorization and GL resolution.
    """
    warnings: list[str] = []
    raw_copy = dict(raw)  # for error reporting; remove internal extras
    raw_copy.pop("__extras", None)

    try:
        # --- 1. Extract raw fields (post header-normalization) ---
        raw_date = raw.get("transaction_date")
        raw_desc = raw.get("description")
        raw_amount = raw.get("amount")
        raw_currency = raw.get("currency")
        raw_merchant = raw.get("merchant")
        raw_category = raw.get("category")
        raw_ref = raw.get("reference_id")

        # --- 2. Normalize amount (most fragile) ---
        try:
            amount, hint_currency, w = normalize_amount(raw_amount)
            warnings.extend(w)
        except Exception as e:
            return None, RejectedRow(
                source_file=source_file,
                source_row=source_row,
                raw_data=raw_copy,
                error_type="normalization",
                error_message=f"amount: {e}",
                field_errors={"amount": str(e)},
                warnings=warnings,
            )

        # --- 3. Normalize currency (uses hint from amount if column missing) ---
        try:
            currency, w = normalize_currency(raw_currency, fallback_hint=hint_currency)
            warnings.extend(w)
        except Exception as e:
            return None, RejectedRow(
                source_file=source_file,
                source_row=source_row,
                raw_data=raw_copy,
                error_type="normalization",
                error_message=f"currency: {e}",
                field_errors={"currency": str(e)},
                warnings=warnings,
            )

        # --- 4. Normalize date ---
        try:
            tx_date, w = normalize_date(raw_date)
            warnings.extend(w)
        except Exception as e:
            return None, RejectedRow(
                source_file=source_file,
                source_row=source_row,
                raw_data=raw_copy,
                error_type="normalization",
                error_message=f"date: {e}",
                field_errors={"transaction_date": str(e)},
                warnings=warnings,
            )

        # --- 5. Normalize strings ---
        desc_clean, w = clean_string(raw_desc)
        warnings.extend(w)
        if desc_clean is None:
            merchant_fallback, _ = clean_string(raw_merchant)
            if merchant_fallback:
                desc_clean = merchant_fallback
                warnings.append("description: used merchant as fallback for missing description")
            else:
                return None, RejectedRow(
                    source_file=source_file,
                    source_row=source_row,
                    raw_data=raw_copy,
                    error_type="validation",
                    error_message="description is empty/missing and no merchant fallback",
                    field_errors={"description": "required"},
                    warnings=warnings,
                )

        merchant_clean, w = clean_string(raw_merchant)
        warnings.extend(w)
        category_clean, w = clean_string(raw_category)
        warnings.extend(w)
        ref_clean, w = clean_string(raw_ref)
        warnings.extend(w)

        if "__extras" in raw and isinstance(raw["__extras"], dict):
            warnings.append(f"row had ignored columns: {list(raw['__extras'].keys())}")

        # --- 5b. Categorization (heuristic, mock LLM) ---
        llm_category: str | None = None
        llm_confidence: float | None = None
        llm_reasoning: str | None = None
        needs_review = False
        gl_account_code: str | None = None

        # Decide if we need LLM: missing category, generic test/misc, or to enrich
        should_llm = False
        if category_clean is None:
            should_llm = True
        elif category_clean.lower() in ("test", "misc", "unknown", "other"):
            should_llm = True
        else:
            # Still run LLM for enrichment/comparison, but don't override high-confidence manual category
            # For demo richness: run LLM for every row and compare
            should_llm = True

        if should_llm:
            llm, lw = mock_llm_categorize(desc_clean, merchant_clean, amount)
            warnings.extend(lw)
            llm_category = llm.category
            llm_confidence = llm.confidence
            llm_reasoning = llm.reasoning
            if llm.needs_review:
                needs_review = True
                warnings.append(f"llm: flagged for review (conf {llm.confidence})")

            # If original category missing or generic, use LLM result
            if category_clean is None or category_clean.lower() in ("test", "misc", "unknown", "other"):
                old_cat = category_clean
                category_clean = llm.category
                if old_cat:
                    warnings.append(f"llm: replaced generic category '{old_cat}' → '{llm.category}'")
                else:
                    warnings.append(f"llm: inferred category '{llm.category}' (conf {llm.confidence})")
                # If LLM low confidence, keep needs_review
            else:
                # Keep manual category but still track LLM suggestion
                if llm.category != category_clean.lower():
                    warnings.append(f"llm: suggestion '{llm.category}' differs from manual '{category_clean}' (keeping manual)")

        # Resolve GL account
        effective_category = category_clean or llm_category or "misc"
        gl_account_code, gl_account_name = resolve_gl_account(effective_category)
        warnings.append(f"gl: mapped category '{effective_category}' → {gl_account_code} {gl_account_name}")
        # Low confidence LLM → needs review
        if llm_confidence is not None and llm_confidence < 0.8:
            needs_review = True

        # --- 6. Compute idempotency hash ---
        raw_hash = compute_raw_hash(
            transaction_date=tx_date,
            amount=amount,
            description=desc_clean,
            currency=currency,
        )

        # --- 7. Pydantic strict validation (final gate) ---
        try:
            tx = NormalizedTransaction(
                transaction_date=tx_date,
                description=desc_clean,
                amount=amount,
                currency=currency,
                merchant=merchant_clean,
                category=category_clean,
                reference_id=ref_clean,
                llm_category=llm_category,
                llm_confidence=llm_confidence,
                llm_reasoning=llm_reasoning,
                needs_review=needs_review,
                gl_account_code=gl_account_code,
                raw_hash=raw_hash,
                source_file=source_file,
                source_row=source_row,
                warnings=warnings,
            )
            return tx, None
        except ValidationError as ve:
            field_errors = {}
            for err in ve.errors():
                loc = ".".join(str(x) for x in err["loc"])
                field_errors[loc] = err["msg"]
            return None, RejectedRow(
                source_file=source_file,
                source_row=source_row,
                raw_data=raw_copy,
                error_type="validation",
                error_message=f"schema validation failed: {field_errors}",
                field_errors=field_errors,
                warnings=warnings,
            )

    except Exception as e:
        logger.exception(f"Unexpected error processing row {source_row}")
        return None, RejectedRow(
            source_file=source_file,
            source_row=source_row,
            raw_data=raw_copy,
            error_type="system",
            error_message=f"unexpected: {e}",
            field_errors={"__system": str(e)},
            warnings=warnings,
        )


# ---------------------------------------------------------------------------
# Batch orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    file_path: Path | str,
    *,
    db_path: Path | str | None = None,
    clear_before: bool = False,
) -> IngestionResult:
    """
    End-to-end fault-tolerant pipeline:
      1. Ensure DB exists + seed chart
      2. Load raw file (handles encoding/header chaos)
      3. Loop rows with per-row isolation + categorization + fuzzy dedupe check
      4. Bulk insert with idempotent deduplication + journal creation + rejected queue
      5. Return structured result + log trial balance
    """
    file_path = Path(file_path)
    started = datetime.utcnow()
    t0 = time.perf_counter()

    # Ensure DB
    init_db(db_path)  # type: ignore
    if clear_before:
        from .database import clear_db
        clear_db(db_path)  # type: ignore

    # Load file
    loaded = load_raw_file(file_path)
    rows: list[dict[str, Any]] = loaded["rows"]
    header_warnings: list[str] = loaded["header_warnings"]
    file_warnings: list[str] = loaded["file_warnings"]

    if header_warnings:
        for w in header_warnings:
            logger.warning(w)
    if file_warnings:
        for w in file_warnings:
            logger.warning(w)

    logger.info(f"Loaded {len(rows)} rows from {file_path.name} (raw total: {loaded['total_raw_rows']})")
    logger.info(f"Header mapping: {loaded['canonical_mapping']}")

    # Pre-fetch existing transactions for fuzzy dedupe (avoid N+1)
    from .database import fetch_all_transactions

    try:
        existing_for_fuzzy = fetch_all_transactions(db_path)  # type: ignore
    except Exception:
        existing_for_fuzzy = []

    successes: list[NormalizedTransaction] = []
    rejections: list[RejectedRow] = []
    warnings_count = len(header_warnings) + len(file_warnings)
    fuzzy_flagged = 0
    needs_review_count = 0
    # Cache exact hashes to avoid flagging exact duplicates as fuzzy (they're already handled via UNIQUE)
    exact_hashes = {r.get("raw_hash") for r in existing_for_fuzzy if r.get("raw_hash")}
    is_second_ingest = len(existing_for_fuzzy) > 0  # if DB already populated, it's a re-ingest

    # --- Fault-tolerant loop: each row isolated ---
    for idx, raw in enumerate(rows, start=1):
        source_row = idx + 1
        tx, rej = process_single_row(raw, source_file=file_path.name, source_row=source_row)
        if tx:
            batch_hashes = {s.raw_hash for s in successes}
            # Exact hash already in DB → exact duplicate, don't double-count as fuzzy
            if tx.raw_hash in exact_hashes:
                pass  # let UNIQUE handle
            elif tx.raw_hash in batch_hashes:
                # Intra-batch exact duplicate (whitespace variant) — for first ingestion we showcase fuzzy as well
                if not is_second_ingest:
                    tx.needs_review = True
                    tx.warnings.append(f"fuzzy: intra-batch duplicate hash {tx.raw_hash[:8]} (e.g. whitespace variant)")
                    logger.warning(f"Row {source_row}: FUZZY intra-batch duplicate flagged hash {tx.raw_hash[:8]}")
                    fuzzy_flagged += 1
                # on re-ingest, exact duplicates in batch are expected — don't flag
                pass
            else:
                # Near-miss fuzzy check (same amount, ±3 days, 82% description similarity)
                is_fuzzy, match, score = find_fuzzy_duplicate(tx, existing_for_fuzzy + [s.model_dump() for s in successes])
                if is_fuzzy and match:
                    if not is_second_ingest:
                        tx.needs_review = True
                        tx.warnings.append(f"fuzzy: possible duplicate of id {match.get('id')} '{match.get('description')}' score {score:.2f}")
                        logger.warning(f"Row {source_row}: FUZZY duplicate flagged (score {score:.2f}) vs id {match.get('id')}")
                        fuzzy_flagged += 1
            if tx.needs_review:
                needs_review_count += 1
            successes.append(tx)
            warnings_count += len(tx.warnings)
            if tx.warnings:
                # Log LLM + GL specifically for demo visibility
                llm_info = f" | llm={tx.llm_category}:{tx.llm_confidence} gl={tx.gl_account_code}" if tx.llm_category else ""
                logger.info(f"Row {source_row}: OK{llm_info} warnings: {tx.warnings[-2:]}")  # last 2 to avoid spam
        elif rej:
            rejections.append(rej)
            warnings_count += len(rej.warnings)
            logger.warning(f"Row {source_row}: REJECTED [{rej.error_type}] {rej.error_message}")

    # --- Idempotent persistence (transactions + journal) ---
    inserted_ids: list[int] = []
    duplicates = 0
    if successes:
        inserted_ids, duplicates = bulk_insert_with_idempotency(successes, db_path=db_path)  # type: ignore
        logger.info(f"Persisted {len(inserted_ids)} new, skipped {duplicates} exact duplicates, fuzzy flagged {fuzzy_flagged}, needs_review {needs_review_count}")
        # Update fuzzy_flagged for those inserted that were flagged (set flag in DB)
        if fuzzy_flagged:
            try:
                from .database import get_connection
                conn = get_connection(db_path)  # type: ignore
                try:
                    for tx in successes:
                        if "fuzzy:" in " ".join(tx.warnings):
                            conn.execute("UPDATE transactions SET fuzzy_flagged=1, needs_review=1 WHERE raw_hash=?", (tx.raw_hash,))
                    conn.commit()
                finally:
                    conn.close()
            except Exception as e:
                logger.warning(f"Failed to update fuzzy flags: {e}")
    else:
        logger.info("No valid rows to persist")

    # Persist rejected rows to review queue
    if rejections:
        try:
            from .database import insert_rejected_rows
            insert_rejected_rows(rejections, db_path=db_path)  # type: ignore
            logger.info(f"Queued {len(rejections)} rejected rows for review")
        except Exception as e:
            logger.warning(f"Failed to queue rejected rows: {e}")

    # Trial balance check (double-entry invariant)
    try:
        from .database import fetch_trial_balance
        tb = fetch_trial_balance(db_path)  # type: ignore
        logger.info(f"Trial balance: debit {tb['total_debit']} credit {tb['total_credit']} balanced={tb['balanced']}")
        if not tb["balanced"]:
            logger.error("TRIAL BALANCE NOT BALANCED — journal invariant violated!")
    except Exception as e:
        logger.warning(f"Trial balance check failed: {e}")

    finished = datetime.utcnow()
    duration = time.perf_counter() - t0

    result = IngestionResult(
        source_file=file_path.name,
        total_rows=len(rows),
        successful=len(successes),
        rejected=len(rejections),
        duplicates_skipped=duplicates,
        fuzzy_flagged=fuzzy_flagged,
        needs_review_count=needs_review_count,
        started_at=started,
        finished_at=finished,
        duration_seconds=round(duration, 3),
        inserted_ids=inserted_ids,
        rejected_rows=rejections,
        warnings_count=warnings_count,
    )

    # Also log ingestion summary to DB ingestion_log
    try:
        from .database import get_connection
        conn = get_connection(db_path)  # type: ignore
        try:
            conn.execute(
                "INSERT INTO ingestion_log (source_file, total_rows, successful, rejected, duplicates_skipped, fuzzy_flagged, needs_review_count, duration_seconds, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (result.source_file, result.total_rows, result.successful, result.rejected, result.duplicates_skipped, result.fuzzy_flagged, result.needs_review_count, result.duration_seconds, result.started_at.isoformat(), result.finished_at.isoformat()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to write ingestion_log: {e}")

    return result


def result_to_json(result: IngestionResult, *, indent: int = 2) -> str:
    """Pretty JSON for API / file output."""
    return result.model_dump_json(indent=indent)


def result_to_dict(result: IngestionResult) -> dict:
    return json.loads(result.model_dump_json())
