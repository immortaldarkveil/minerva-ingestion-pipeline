"""
Normalization engine — the heart of the pipeline.
Handles the "nightmare" of SMB financial inputs: mojibake, inconsistent headers, currency noise, date chaos.
Every function returns (cleaned_value, warnings) so the caller can audit what was fixed.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from dateutil import parser as date_parser

from .models import CURRENCY_SYMBOLS

# ---------------------------------------------------------------------------
# 1. String / encoding cleanup
# ---------------------------------------------------------------------------

MOJIBAKE_FIXES = {
    "Ã©": "é",
    "Ã¨": "è",
    "Ã¢": "â",
    "Ã´": "ô",
    "Ã§": "ç",
    "â€™": "'",
    "â€œ": '"',
    "â€\x9d": '"',
    "â€“": "–",
    "â€”": "—",
    "Â£": "£",
    "Â€": "€",
    "Â¥": "¥",
    "Ã¼": "ü",
    "Ã¶": "ö",
}


def fix_encoding(text: str) -> tuple[str, list[str]]:
    """
    Fix common mojibake / double-encoded UTF-8 artifacts seen in SMB CSV exports.
    Returns cleaned text + warnings if a fix was applied.
    """
    warnings: list[str] = []
    original = text

    # 1) Try ftfy-style latin1 → utf-8 repair for cases like "CafÃ©"
    #    Heuristic: if text contains Ã or Â followed by another char, attempt re-decode
    if any(c in text for c in ("Ã", "Â")):
        try:
            # Common double-encoding: original bytes were utf-8, mis-decoded as latin1
            repaired = text.encode("latin1").decode("utf-8")
            # Only accept if repaired looks cleaner (no Ã/Â and fewer replacement chars)
            if "Ã" not in repaired and "Â" not in repaired:
                text = repaired
                warnings.append(f"encoding: repaired mojibake '{original[:30]}' → '{text[:30]}'")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    # 2) Targeted replacements for known artifacts
    for bad, good in MOJIBAKE_FIXES.items():
        if bad in text:
            text = text.replace(bad, good)
            warnings.append(f"encoding: replaced '{bad}' → '{good}'")

    # 3) Strip zero-width / control chars (common in Excel exports)
    cleaned = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\u200b\u200c\u200d\ufeff]", "", text)
    if cleaned != text:
        warnings.append("encoding: stripped control/zero-width characters")
        text = cleaned

    # 4) Normalize whitespace: collapse multiple spaces, trim
    collapsed = re.sub(r"\s+", " ", text).strip()
    if collapsed != text:
        # Don't warn for trivial trimming — only for collapse
        if "  " in text:
            warnings.append("whitespace: collapsed multiple spaces")
        text = collapsed

    return text, warnings


def clean_string(value: Any) -> tuple[Optional[str], list[str]]:
    """Coerce to string, fix encoding, handle null-ish values."""
    warnings: list[str] = []
    if value is None:
        return None, warnings
    # Pandas NaN check
    try:
        import math

        if isinstance(value, float) and math.isnan(value):
            return None, warnings
    except Exception:
        pass

    s = str(value)
    # Treat empty-ish strings as None
    if s.strip() in ("", "-", "N/A", "n/a", "NULL", "null", "#N/A"):
        warnings.append(f"string: treated '{s}' as null")
        return None, warnings

    cleaned, w = fix_encoding(s)
    warnings.extend(w)
    # Final trim
    cleaned = cleaned.strip()
    if not cleaned:
        return None, warnings
    return cleaned, warnings


# ---------------------------------------------------------------------------
# 2. Header normalization
# ---------------------------------------------------------------------------

# Canonical field → list of fuzzy synonyms (lowercase, no punctuation)
HEADER_SYNONYMS: dict[str, list[str]] = {
    "transaction_date": [
        "date",
        "transaction date",
        "txn date",
        "trans date",
        "transacton date",  # common typo
        "invoice date",
        "receipt date",
        "posting date",
        "value date",
        "booking date",
        "dated",
        "when",
        "day",
    ],
    "description": [
        "description",
        "desc",
        "details",
        "narrative",
        "memo",
        "particulars",
        "transaction details",
        "transaction description",
        "notes",
        "info",
        "label",
        "what",
    ],
    "amount": [
        "amount",
        "amt",
        "sum",
        "total",
        "value",
        "price",
        "cost",
        "charge",
        "payment",
        "debit",
        "credit",
        "transaction amount",
        "amount usd",
        "amount gbp",
        "amount eur",
    ],
    "currency": [
        "currency",
        "curr",
        "ccy",
        "currency code",
        "curr code",
    ],
    "merchant": [
        "merchant",
        "vendor",
        "payee",
        "supplier",
        "counterparty",
        "beneficiary",
        "merchant name",
        "store",
        "shop",
    ],
    "category": [
        "category",
        "cat",
        "type",
        "account",
        "ledger",
        "class",
        "tag",
    ],
    "reference_id": [
        "reference",
        "ref",
        "id",
        "transaction id",
        "txn id",
        "invoice id",
        "receipt id",
        "number",
        "num",
        "external id",
    ],
}


def _normalize_header_token(header: str) -> str:
    """Lowercase, strip, remove currency symbols and punctuation for matching."""
    h = header.strip().lower()
    # Remove parenthetical currency hints: "Amount ($)" → "amount"
    h = re.sub(r"\s*\(.*?\)\s*", " ", h)
    h = re.sub(r"[$£€¥₹]", " ", h)
    # Remove punctuation, collapse
    h = re.sub(r"[^a-z0-9 ]", " ", h)
    h = re.sub(r"\s+", " ", h).strip()
    return h


# Build reverse lookup: normalized token → canonical
_REVERSE_HEADER_MAP: dict[str, str] = {}
for canonical, synonyms in HEADER_SYNONYMS.items():
    for syn in synonyms:
        _REVERSE_HEADER_MAP[_normalize_header_token(syn)] = canonical
    _REVERSE_HEADER_MAP[_normalize_header_token(canonical)] = canonical


def normalize_headers(raw_headers: list[str]) -> tuple[dict[str, str], list[str]]:
    """
    Map dirty headers → canonical field names.
    Returns (mapping: raw_header → canonical, warnings).
    Unknown headers are kept as-is with a warning and will be ignored downstream.
    """
    mapping: dict[str, str] = {}
    warnings: list[str] = []
    seen_canonical: dict[str, str] = {}

    for h in raw_headers:
        original = h
        cleaned, w = fix_encoding(str(h))
        # warnings for encoding already handled; but header-specific:
        token = _normalize_header_token(cleaned)
        canonical = _REVERSE_HEADER_MAP.get(token)

        # Handle exact header noise: trailing whitespace already stripped in token
        if original != original.strip():
            warnings.append(f"header: trimmed whitespace '{original}' → '{original.strip()}'")

        if canonical:
            # Detect duplicate canonical mapping (e.g., both "Amount" and "Total" → amount).
            # For CSV headers duplicates are rare and will be coalesced per-row;
            # for JSON (union of keys across rows) this is expected — keep mapping for all.
            if canonical in seen_canonical:
                warnings.append(
                    f"header: duplicate mapping '{original}' → '{canonical}' (already mapped from '{seen_canonical[canonical]}'); will coalesce per-row"
                )
            else:
                seen_canonical[canonical] = original
            mapping[original] = canonical
        else:
            warnings.append(f"header: unrecognized header '{original}' (normalized: '{token}') — will be ignored")
            mapping[original] = f"__ignored__{token}"

    # Check required fields present
    required = {"transaction_date", "description", "amount"}
    missing = required - set(mapping.values())
    if missing:
        warnings.append(f"header: missing required fields after normalization: {missing}")

    return mapping, warnings


# ---------------------------------------------------------------------------
# 3. Amount / currency normalization
# ---------------------------------------------------------------------------

_AMOUNT_RE = re.compile(r"[^0-9\.,\(\)\-]")  # keep digits, dot, comma, parens, minus


def normalize_amount(raw: Any) -> tuple[Decimal, str, list[str]]:
    """
    Parse messy amount strings into Decimal + inferred currency.
    Handles: "$1,234.56", "£ 2.500,00", "(123.45)" for negatives, trailing spaces, missing symbol.
    Returns (amount, currency_hint, warnings).
    - currency_hint is inferred from symbol if present, else "USD" default (caller may override with currency column)
    """
    warnings: list[str] = []
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        raise ValueError("Amount is empty/missing")

    # Already numeric?
    if isinstance(raw, (int, float, Decimal)):
        # Float → Decimal via string to avoid binary artifacts
        dec = Decimal(str(raw))
        return dec, "USD", warnings

    s_original = str(raw)
    s, w = fix_encoding(s_original)
    warnings.extend(w)
    s = s.strip()

    # Detect currency symbol in original
    currency_hint = "USD"  # default
    for sym, code in CURRENCY_SYMBOLS.items():
        if sym in s_original or sym in s:
            currency_hint = code
            warnings.append(f"amount: inferred currency {code} from symbol '{sym}'")
            break

    # Parentheses → negative: "(123.45)" or "($123.45)"
    is_parenthesized_negative = s.startswith("(") and s.endswith(")")
    if is_parenthesized_negative:
        s = s[1:-1].strip()
        warnings.append("amount: interpreted parenthesized value as negative")

    # Remove currency symbols & letters for numeric parse, but keep structure
    # Handle European format: "1.234,56" vs US "1,234.56"
    # Heuristic: if both . and , present, the last separator is decimal
    numeric_part = re.sub(r"[^\d\.,\-]", "", s)  # strip everything except digits/./,/-

    # Determine decimal separator
    if "." in numeric_part and "," in numeric_part:
        last_dot = numeric_part.rfind(".")
        last_comma = numeric_part.rfind(",")
        if last_comma > last_dot:
            # European: "1.234,56" → remove dots, comma→dot
            numeric_part = numeric_part.replace(".", "").replace(",", ".")
            warnings.append("amount: interpreted European format (dot thousands, comma decimal)")
        else:
            # US: "1,234.56" → remove commas
            numeric_part = numeric_part.replace(",", "")
    elif "," in numeric_part and "." not in numeric_part:
        # Ambiguous: "1,234" could be 1234 or 1.234
        # Heuristic: if comma followed by exactly 2 digits at end → decimal comma
        if re.search(r",\d{2}$", numeric_part):
            numeric_part = numeric_part.replace(".", "").replace(",", ".")
            warnings.append("amount: interpreted comma as decimal separator")
        else:
            numeric_part = numeric_part.replace(",", "")
            if "," in s:
                warnings.append("amount: stripped comma thousand-separator")

    # Now numeric_part should be like "-1234.56"
    try:
        dec = Decimal(numeric_part)
    except InvalidOperation as e:
        raise ValueError(f"Cannot parse amount '{s_original}' (cleaned: '{numeric_part}'): {e}") from e

    if is_parenthesized_negative:
        dec = -abs(dec)

    # Handle explicit minus vs accounting: ensure -0 handling
    if dec == Decimal("-0"):
        dec = Decimal("0")

    return dec, currency_hint, warnings


def normalize_currency(raw: Any, fallback_hint: str = "USD") -> tuple[str, list[str]]:
    """Normalize currency field; falls back to hint from amount column."""
    warnings: list[str] = []
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        warnings.append(f"currency: missing, defaulting to {fallback_hint}")
        return fallback_hint, warnings

    s, w = clean_string(raw)
    warnings.extend(w)
    if s is None:
        warnings.append(f"currency: empty after cleaning, defaulting to {fallback_hint}")
        return fallback_hint, warnings

    # Symbol → code
    if s in CURRENCY_SYMBOLS:
        code = CURRENCY_SYMBOLS[s]
        warnings.append(f"currency: mapped symbol '{s}' → {code}")
        return code, warnings
    for sym, code in CURRENCY_SYMBOLS.items():
        if sym in s:
            warnings.append(f"currency: extracted symbol '{sym}' → {code} from '{s}'")
            return code, warnings

    upper = s.strip().upper()
    # Already 3-letter code?
    if re.match(r"^[A-Z]{3}$", upper):
        return upper, warnings
    # Common aliases
    aliases = {"DOLLAR": "USD", "POUND": "GBP", "EURO": "EUR", "YEN": "JPY", "RUPEE": "INR"}
    if upper in aliases:
        warnings.append(f"currency: mapped alias '{s}' → {aliases[upper]}")
        return aliases[upper], warnings

    warnings.append(f"currency: unrecognized '{s}', defaulting to {fallback_hint}")
    return fallback_hint, warnings


# ---------------------------------------------------------------------------
# 4. Date normalization — handles DD/MM vs MM/DD nightmare
# ---------------------------------------------------------------------------

EXPLICIT_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d.%m.%Y",
    "%m-%d-%Y",
    "%d %b %Y",
    "%d %B %Y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%d-%b-%Y",
    "%d-%B-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%y",
    "%d/%m/%y",
    "%y-%m-%d",
]


def normalize_date(raw: Any) -> tuple[date, list[str]]:
    """
    Try multiple strategies to parse a date. Returns (date, warnings).
    Strategy:
      1. Try explicit strptime formats
      2. Try dateutil with dayfirst=False, then dayfirst=True
      3. If ambiguous (01/02/2023), log warning and pick dayfirst=True as default (international SMB bias)
    """
    warnings: list[str] = []
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        raise ValueError("Date is empty/missing")

    # Already date/datetime?
    if isinstance(raw, datetime):
        return raw.date(), warnings
    if isinstance(raw, date):
        return raw, warnings
    # Excel serial? (pandas may give float)
    if isinstance(raw, (int, float)):
        # Heuristic: Excel serial numbers are > 30000 (~1982) and < 60000 (~2064)
        if 30000 < raw < 60000:
            try:
                # Excel epoch 1899-12-30
                base = date(1899, 12, 30)
                from datetime import timedelta

                d = base + timedelta(days=int(raw))
                warnings.append(f"date: interpreted numeric {raw} as Excel serial → {d.isoformat()}")
                return d, warnings
            except Exception:
                pass
        raise ValueError(f"Cannot parse numeric date: {raw}")

    s_original = str(raw)
    s, w = fix_encoding(s_original)
    warnings.extend(w)
    s = s.strip()
    # Excel often wraps dates in single quotes: "'2023-01-15"
    s = s.lstrip("'").strip()

    # Quick cleanup: remove ordinal suffixes "1st", "2nd"
    s = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", s, flags=re.IGNORECASE)

    # 1. Try explicit formats first (deterministic)
    for fmt in EXPLICIT_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # Heuristic: two-digit year → assume 2000s if < 50 else 1900s is handled by strptime
            return dt.date(), warnings
        except ValueError:
            continue

    # 2. Try dateutil — compare dayfirst=False vs True to detect ambiguity
    try:
        d1 = date_parser.parse(s, dayfirst=False, yearfirst=False).date()
    except Exception as e1:
        d1 = None
        err1 = str(e1)
    else:
        err1 = None

    try:
        d2 = date_parser.parse(s, dayfirst=True, yearfirst=False).date()
    except Exception as e2:
        d2 = None
        err2 = str(e2)
    else:
        err2 = None

    if d1 and d2:
        if d1 != d2:
            warnings.append(
                f"date: ambiguous '{s_original}' → MM/DD={d1.isoformat()} vs DD/MM={d2.isoformat()}; chose DD/MM ({d2.isoformat()})"
            )
            return d2, warnings
        return d1, warnings
    if d1:
        return d1, warnings
    if d2:
        warnings.append(f"date: parsed with dayfirst=True after dayfirst=False failed")
        return d2, warnings

    raise ValueError(f"Cannot parse date '{s_original}': {err1 or err2}")


# ---------------------------------------------------------------------------
# 5. Heuristic categorization (mock LLM)
# ---------------------------------------------------------------------------

def mock_llm_categorize(description: str, merchant: str | None, amount=None) -> tuple["LLMCategorization", list[str]]:
    """
    Heuristic categorizer that mimics an LLM call.
    In production this would call an LLM with function calling.
    Deterministic keyword rules keep the demo reproducible without API keys.
    Returns (LLMCategorization, warnings)
    """
    from decimal import Decimal

    from .models import LLMCategorization

    warnings: list[str] = []
    text = f"{description or ''} {merchant or ''}".lower()
    # Normalize unicode for matching (café -> cafe)
    text = text.replace("é", "e").replace("ü", "u").replace("ö", "o")

    # Rule table: (keywords, category, confidence, reasoning)
    rules: list[tuple[list[str], str, float, str]] = [
        (["staples", "office supplies", "paper", "staple"], "office", 0.96, "Office supply merchant/keywords"),
        (["cafe", "starbucks", "ivy", "meal", "lunch", "dinner", "restaurant", "soho"], "meals", 0.94, "Hospitality / meals keywords"),
        (["taxi", "cab", "uber", "heathrow", "hotel", "premier inn", "shell gas", "gas"], "travel", 0.95, "Travel / transport keywords"),
        (["microsoft", "software", "license", "saas", "google", "bonus"], "software", 0.92, "Software / SaaS keywords"),
        (["acme", "consulting", "professional", "service"], "services", 0.90, "Professional services keywords"),
        (["tesco", "walmart", "grocery", "shop"], "groceries", 0.88, "Grocery / retail keywords"),
        (["muller", "gmbh", "european invoice"], "services", 0.85, "EU service provider"),
        (["amazon", "refund", "returned"], "refunds", 0.93, "Refund / contra-revenue keywords"),
        (["bank fee", "fee"], "misc", 0.80, "Bank fee pattern"),
    ]

    for keywords, cat, conf, reason in rules:
        if any(k in text for k in keywords):
            # Lower confidence if amount is unusual for category
            llm = LLMCategorization(category=cat, confidence=conf, reasoning=reason, needs_review=conf < 0.80)
            if conf < 0.80:
                warnings.append(f"llm: low confidence {conf} for '{description[:30]}' → needs review")
            return llm, warnings

    # Fallback: check amount sign
    try:
        amt = Decimal(str(amount)) if amount is not None else Decimal("0")
        if amt < 0:
            llm = LLMCategorization(category="refunds", confidence=0.75, reasoning="Negative amount suggests refund", needs_review=True)
            warnings.append("llm: negative amount fallback → refunds (needs review)")
            return llm, warnings
    except Exception:
        pass

    # True fallback — needs human review
    llm = LLMCategorization(
        category="misc",
        confidence=0.55,
        reasoning="No strong keyword match — flagged for human review",
        needs_review=True,
    )
    warnings.append(f"llm: no keyword match for '{description[:30]}' → misc (needs review)")
    return llm, warnings


# ---------------------------------------------------------------------------
# 6. Fuzzy duplicate detection (beyond exact hash)
# ---------------------------------------------------------------------------

def fuzzy_duplicate_score(a_desc: str, b_desc: str) -> float:
    """Normalized similarity 0-1 using difflib. Cheap, no extra deps."""
    import difflib

    a = a_desc.strip().lower()
    b = b_desc.strip().lower()
    return difflib.SequenceMatcher(None, a, b).ratio()


def find_fuzzy_duplicate(
    new_tx: dict | object,
    existing_rows: list[dict],
    *,
    amount_tolerance: Decimal = Decimal("0.01"),
    day_window: int = 3,
    threshold: float = 0.82,
) -> tuple[bool, dict | None, float]:
    """
    Check if new_tx looks like a fuzzy duplicate of any existing row.
    Criteria: same amount (±tolerance) AND date within ±3 days AND description similarity > 0.82
    Returns (is_fuzzy, matched_row, score)
    """
    from datetime import date as date_type
    from decimal import Decimal

    # Normalize new_tx to dict-like
    if hasattr(new_tx, "model_dump"):
        nd = new_tx.model_dump()  # type: ignore
    elif isinstance(new_tx, dict):
        nd = new_tx
    else:
        nd = dict(new_tx)  # type: ignore

    new_amount = Decimal(str(nd.get("amount") or nd.get("Amount") or 0))
    new_desc = str(nd.get("description") or nd.get("desc") or "")
    # Parse new_date
    new_date_raw = nd.get("transaction_date") or nd.get("date")
    try:
        if isinstance(new_date_raw, str):
            from dateutil import parser as dparser

            new_date = dparser.parse(new_date_raw).date()
        elif isinstance(new_date_raw, date_type):
            new_date = new_date_raw
        else:
            new_date = None
    except Exception:
        new_date = None

    best_score = 0.0
    best_match: dict | None = None

    for row in existing_rows:
        try:
            row_amount = Decimal(str(row.get("amount") or 0))
            if abs(row_amount - new_amount) > amount_tolerance:
                continue
            # Date proximity
            row_date_raw = row.get("transaction_date")
            if isinstance(row_date_raw, str):
                from dateutil import parser as dparser

                row_date = dparser.parse(row_date_raw).date()
            elif isinstance(row_date_raw, date_type):
                row_date = row_date_raw
            else:
                continue
            if new_date and row_date:
                delta = abs((new_date - row_date).days)
                if delta > day_window:
                    continue
            # Description similarity
            row_desc = str(row.get("description") or "")
            score = fuzzy_duplicate_score(new_desc, row_desc)
            if score > best_score:
                best_score = score
                best_match = row
            if score >= threshold:
                return True, row, score
        except Exception:
            continue

    if best_match and best_score >= threshold:
        return True, best_match, best_score
    return False, best_match, best_score
