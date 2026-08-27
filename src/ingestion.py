"""
Ingestion layer — reads dirty CSV/JSON, handles encoding chaos, normalizes headers, yields raw dicts.
Fault tolerance starts here: encoding errors never crash the reader; header mismatches are mapped, not rejected.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterator

from .normalization import normalize_headers

# Try pandas for robust CSV parsing, fall back to stdlib csv
try:
    import pandas as pd  # type: ignore

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def _read_csv_with_encoding_fallback(path: Path) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    """
    Read CSV trying utf-8-sig first, then latin1, then cp1252.
    Returns (raw_headers, rows, warnings).
    Each row is a dict keyed by original header.
    """
    warnings: list[str] = []
    encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    last_err: Exception | None = None

    for enc in encodings:
        try:
            if HAS_PANDAS:
                # Use pandas for its handling of quoted commas, multiline fields, etc.
                # Keep everything as string to avoid pandas type coercion surprises
                df = pd.read_csv(
                    path,
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False,  # don't interpret "NA" as NaN — we handle it
                    na_filter=False,
                    engine="python",  # more tolerant of malformed rows
                    on_bad_lines="warn",  # skip bad lines with warning, don't crash
                    skip_blank_lines=True,
                )
                # Fix: pandas may have already stripped? Preserve original headers exactly
                raw_headers = list(df.columns)
                rows = df.to_dict(orient="records")
                # Also handle case where file is actually empty
                if not raw_headers and len(rows) == 0:
                    raise ValueError("Empty CSV or missing header row")
                if enc != "utf-8-sig":
                    warnings.append(f"ingestion: fell back to encoding '{enc}' (utf-8-sig failed)")
                return raw_headers, rows, warnings
            else:
                # Stdlib fallback
                with open(path, encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames is None:
                        raise ValueError("Missing header row")
                    raw_headers = list(reader.fieldnames)
                    rows = list(reader)
                if enc != "utf-8-sig":
                    warnings.append(f"ingestion: fell back to encoding '{enc}'")
                return raw_headers, rows, warnings

        except UnicodeDecodeError as e:
            last_err = e
            continue
        except UnicodeError as e:
            last_err = e
            continue
        except Exception as e:
            # For pandas parser errors, try next encoding only if it's decode-related
            if "codec" in str(e).lower() or "decode" in str(e).lower():
                last_err = e
                continue
            raise

    raise ValueError(f"Failed to read CSV with any encoding {encodings}: {last_err}")


def _read_json(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Read JSON array or JSONL. Returns (rows, warnings)."""
    warnings: list[str] = []
    text = path.read_text(encoding="utf-8-sig")
    # Try JSON array
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data, warnings
        if isinstance(data, dict):
            # Single object → wrap
            return [data], warnings
    except json.JSONDecodeError:
        pass

    # Try JSONL (one JSON per line)
    rows: list[dict[str, Any]] = []
    for i, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            warnings.append(f"ingestion: skipped invalid JSONL line {i}: {e}")
    if rows:
        warnings.append(f"ingestion: parsed as JSONL ({len(rows)} records)")
        return rows, warnings

    raise ValueError(f"Cannot parse JSON file {path}: not a JSON array or JSONL")


def load_raw_file(path: Path | str) -> dict[str, Any]:
    """
    Unified entry: detect file type, read, normalize headers.
    Returns dict with keys: raw_headers, canonical_mapping, rows, header_warnings, file_warnings
    Rows are list[dict] keyed by canonical field where possible.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()
    file_warnings: list[str] = []

    if suffix == ".csv":
        raw_headers, raw_rows, file_warnings = _read_csv_with_encoding_fallback(path)
        mapping, header_warnings = normalize_headers(raw_headers)

        # Remap rows to canonical keys, preserving ignored fields for audit
        canonical_rows: list[dict[str, Any]] = []
        for r in raw_rows:
            # Skip fully empty rows (common in Excel exports)
            if all(str(v).strip() == "" for v in r.values()):
                file_warnings.append("ingestion: skipped fully empty row")
                continue
            canonical: dict[str, Any] = {}
            extras: dict[str, Any] = {}
            for raw_h, canonical_h in mapping.items():
                val = r.get(raw_h)
                if canonical_h.startswith("__ignored__"):
                    extras[raw_h] = val
                else:
                    # If multiple raw headers mapped to same canonical (duplicate case), keep first non-empty
                    if canonical_h in canonical and canonical[canonical_h] not in (None, ""):
                        continue
                    canonical[canonical_h] = val
            # Stash extras for debugging but don't use for validation
            if extras:
                canonical["__extras"] = extras
            canonical_rows.append(canonical)

        return {
            "raw_headers": raw_headers,
            "canonical_mapping": mapping,
            "rows": canonical_rows,
            "header_warnings": header_warnings,
            "file_warnings": file_warnings,
            "total_raw_rows": len(raw_rows),
        }

    elif suffix == ".json":
        raw_rows, file_warnings = _read_json(path)
        # JSON: infer headers from keys of first row
        all_keys: set[str] = set()
        for r in raw_rows:
            all_keys.update(r.keys())
        mapping, header_warnings = normalize_headers(list(all_keys))

        canonical_rows = []
        for r in raw_rows:
            if not r or all(str(v).strip() == "" for v in r.values()):
                continue
            canonical: dict[str, Any] = {}
            extras: dict[str, Any] = {}
            for k, v in r.items():
                canonical_h = mapping.get(k, f"__ignored__{k}")
                if canonical_h.startswith("__ignored__"):
                    extras[k] = v
                else:
                    canonical[canonical_h] = v
            if extras:
                canonical["__extras"] = extras
            # Also apply mapping for missing keys that should be None (ensure all canonical keys present)
            for canonical_key in set(mapping.values()):
                if not canonical_key.startswith("__ignored__") and canonical_key not in canonical:
                    canonical[canonical_key] = None
            canonical_rows.append(canonical)

        return {
            "raw_headers": list(all_keys),
            "canonical_mapping": mapping,
            "rows": canonical_rows,
            "header_warnings": header_warnings,
            "file_warnings": file_warnings,
            "total_raw_rows": len(raw_rows),
        }

    else:
        raise ValueError(f"Unsupported file type '{suffix}' — expected .csv or .json")
