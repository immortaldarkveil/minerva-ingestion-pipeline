"""
Pydantic models for strict validation and double-entry accounting.
Every row must pass this gate — but failures are isolated, never crashing the batch.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Core domain models
# ---------------------------------------------------------------------------

ALLOWED_CURRENCIES = {"USD", "GBP", "EUR", "JPY", "INR", "CAD", "AUD", "CHF", "CNY"}
CURRENCY_SYMBOLS = {"$": "USD", "£": "GBP", "€": "EUR", "¥": "JPY", "₹": "INR"}

# Standard SMB chart of accounts
CHART_OF_ACCOUNTS_SEED = [
    # Assets
    {"code": "1010", "name": "Bank - Primary", "type": "asset"},
    {"code": "1020", "name": "Cash", "type": "asset"},
    {"code": "1030", "name": "Accounts Receivable", "type": "asset"},
    # Liabilities
    {"code": "2010", "name": "Accounts Payable", "type": "liability"},
    {"code": "2020", "name": "Credit Card Payable", "type": "liability"},
    # Revenue
    {"code": "4010", "name": "Sales Revenue", "type": "revenue"},
    {"code": "4020", "name": "Service Revenue", "type": "revenue"},
    {"code": "4030", "name": "Refunds / Contra-Revenue", "type": "revenue"},
    # Expenses
    {"code": "6100", "name": "Office Supplies", "type": "expense"},
    {"code": "6150", "name": "Meals & Entertainment", "type": "expense"},
    {"code": "6200", "name": "Travel", "type": "expense"},
    {"code": "6300", "name": "Software & SaaS", "type": "expense"},
    {"code": "6050", "name": "Professional Services", "type": "expense"},
    {"code": "6400", "name": "Marketing", "type": "expense"},
    {"code": "6500", "name": "Uncategorized Expense", "type": "expense"},
    {"code": "6600", "name": "Bank Fees", "type": "expense"},
]

# Category → GL account mapping (what LLM or rule maps to)
CATEGORY_TO_ACCOUNT = {
    "office": "6100",
    "meals": "6150",
    "travel": "6200",
    "software": "6300",
    "services": "6050",
    "groceries": "6150",
    "marketing": "6400",
    "refunds": "4030",
    "bonus": "6050",
    "misc": "6500",
    "test": "6500",
    "office supplies": "6100",
    "professional services": "6050",
}


class LLMCategorization(BaseModel):
    """Result of heuristic categorization — mimics an LLM call with confidence."""

    category: str = Field(description="Inferred GL category, lowercased")
    confidence: float = Field(ge=0.0, le=1.0, description="Model confidence")
    reasoning: str = Field(description="Short human-readable why")
    model: str = Field(default="mock-llm-v1", description="Model ID for audit")
    needs_review: bool = Field(description="True if confidence < threshold")


class NormalizedTransaction(BaseModel):
    """
    Canonical transaction record — the single source of truth after ingestion.
    Strict validation ensures downstream accounting logic never sees garbage.
    Includes enrichment fields for categorization and review workflow.
    """

    # Business fields
    transaction_date: date = Field(description="ISO date, normalized from any input format")
    description: str = Field(min_length=1, max_length=500, description="Cleaned merchant/description")
    amount: Decimal = Field(description="Decimal with 2dp, negative = refund/credit, positive = debit/expense")
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    merchant: Optional[str] = Field(default=None, max_length=200)
    category: Optional[str] = Field(default=None, max_length=100)
    reference_id: Optional[str] = Field(default=None, description="External ID if provided")

    # Enrichment
    llm_category: Optional[str] = Field(default=None, description="Inferred category if category missing")
    llm_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    llm_reasoning: Optional[str] = Field(default=None)
    needs_review: bool = Field(default=False, description="True if LLM confidence low or fuzzy duplicate suspected")
    gl_account_code: Optional[str] = Field(default=None, description="Resolved GL account code")

    # Lineage / idempotency
    raw_hash: str = Field(description="SHA256 of canonical fields — used for deduplication")
    source_file: str = Field(description="Origin file for audit trail")
    source_row: int = Field(description="1-indexed row number in source")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    # Quality signals
    warnings: list[str] = Field(default_factory=list, description="Non-blocking fixes applied")

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in ALLOWED_CURRENCIES:
            raise ValueError(f"Unsupported currency '{v}'. Allowed: {sorted(ALLOWED_CURRENCIES)}")
        return v

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        # Reject zero-value transactions — likely data error
        if v == Decimal("0"):
            raise ValueError("Amount cannot be zero")
        # Quantize to 2 decimal places
        try:
            v = v.quantize(Decimal("0.01"))
        except InvalidOperation:
            raise ValueError(f"Invalid amount precision: {v}")
        # Sanity bound: flag absurd values (> $10M single transaction)
        if abs(v) > Decimal("10000000"):
            raise ValueError(f"Amount {v} exceeds sanity bound (±10M)")
        return v

    @field_validator("description", "merchant", "category", "llm_category", mode="before")
    @classmethod
    def strip_strings(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip()
            return v if v else None
        return v

    @field_validator("transaction_date")
    @classmethod
    def validate_date(cls, v: date) -> date:
        # Reject far-future dates ( > 30 days ahead) — likely MM/DD vs DD/MM swap
        today = date.today()
        if v > today:
            delta = (v - today).days
            if delta > 30:
                raise ValueError(f"Date {v} is {delta} days in future — likely format error")
        # Reject ancient dates
        if v < date(1990, 1, 1):
            raise ValueError(f"Date {v} is before 1990 — likely parsing error")
        return v

    @model_validator(mode="after")
    def check_description_present(self):
        if not self.description and not self.merchant:
            raise ValueError("At least description or merchant must be present")
        return self

    @model_validator(mode="after")
    def resolve_gl_account(self):
        # Auto-resolve GL account if not set
        if not self.gl_account_code:
            cat = (self.category or self.llm_category or "misc").lower()
            self.gl_account_code = CATEGORY_TO_ACCOUNT.get(cat, "6500")
        return self

    model_config = {"str_strip_whitespace": True}


class RejectedRow(BaseModel):
    """Structured error record for every row that failed validation — never lost."""

    source_file: str
    source_row: int
    raw_data: dict
    error_type: str  # "validation" | "normalization" | "system"
    error_message: str
    field_errors: dict = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class IngestionResult(BaseModel):
    """Batch-level summary returned to caller and persisted as JSON."""

    source_file: str
    total_rows: int
    successful: int
    rejected: int
    duplicates_skipped: int
    fuzzy_flagged: int = 0
    needs_review_count: int = 0
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    inserted_ids: list[int] = Field(default_factory=list)
    rejected_rows: list[RejectedRow] = Field(default_factory=list)
    warnings_count: int = 0

    @property
    def success_rate(self) -> float:
        return round(self.successful / self.total_rows * 100, 2) if self.total_rows else 0


# ---------------------------------------------------------------------------
# Double-entry accounting models
# ---------------------------------------------------------------------------

class Account(BaseModel):
    code: str
    name: str
    type: str  # asset, liability, equity, revenue, expense

class JournalLine(BaseModel):
    account_code: str
    account_name: str
    debit: Decimal = Field(default=Decimal("0.00"))
    credit: Decimal = Field(default=Decimal("0.00"))
    currency: str
    description: Optional[str] = None

    @model_validator(mode="after")
    def check_one_sided(self):
        # Exactly one of debit/credit must be >0, not both, not neither
        if (self.debit == Decimal("0") and self.credit == Decimal("0")) or (self.debit > 0 and self.credit > 0):
            raise ValueError("Journal line must have exactly one of debit or credit > 0")
        return self

class JournalEntry(BaseModel):
    """Double-entry: every transaction posts as balanced debit/credit pair."""

    id: Optional[int] = None
    transaction_id: Optional[int] = None
    entry_date: date
    description: str
    reference_id: Optional[str] = None
    source_file: str
    source_row: int
    lines: list[JournalLine] = Field(min_length=2, max_length=2)  # simple 2-line entry for prototype
    currency: str
    amount: Decimal
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def check_balanced(self):
        total_debit = sum(l.debit for l in self.lines)
        total_credit = sum(l.credit for l in self.lines)
        if total_debit != total_credit:
            raise ValueError(f"Journal not balanced: debit {total_debit} != credit {total_credit}")
        return self

    @property
    def is_balanced(self) -> bool:
        return sum(l.debit for l in self.lines) == sum(l.credit for l in self.lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def compute_raw_hash(*, transaction_date: date, amount: Decimal, description: str, currency: str) -> str:
    """
    Deterministic hash for idempotency.
    Same logical transaction → same hash, even if file is re-ingested.
    Normalizes: date iso, amount quantized, description lowercased+stripped.
    """
    canonical = f"{transaction_date.isoformat()}|{amount.quantize(Decimal('0.01'))}|{description.strip().lower()}|{currency.upper()}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]  # 16 hex chars = 64-bit, enough for dedup

def resolve_gl_account(category: Optional[str]) -> tuple[str, str]:
    """Map category → (account_code, account_name) via CHART_OF_ACCOUNTS_SEED."""
    cat = (category or "misc").lower()
    code = CATEGORY_TO_ACCOUNT.get(cat, "6500")
    name = next((a["name"] for a in CHART_OF_ACCOUNTS_SEED if a["code"] == code), "Uncategorized Expense")
    return code, name
