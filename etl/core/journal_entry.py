"""
Pydantic model for double-entry journal entries.
Each transaction produces a CREDIT and a DEBIT entry to maintain accounting balance.
"""
from pydantic import BaseModel, field_validator


class JournalEntry(BaseModel):
    """
    Represents a single journal entry in the double-entry accounting system.

    Fields:
        journal_id: Sequential identifier (e.g., "JRN-0001")
        txn_id: Reference to the originating transaction
        entry_type: Must be "CREDIT" or "DEBIT"
        account: Account identifier (e.g., "CUST_1001" or "MERCH_MERCHANT_A")
        amount: Transaction amount (must be positive)
        currency: Currency code (default "USD")
        txn_timestamp: Original transaction timestamp
        description: Human-readable description of the entry
    """

    journal_id: str
    txn_id: str
    entry_type: str
    account: str
    amount: float
    currency: str
    txn_timestamp: str
    description: str

    # Validate that entry_type is either CREDIT or DEBIT
    @field_validator("entry_type")
    @classmethod
    def validate_entry_type(cls, v: str) -> str:
        """Ensure entry_type is one of the allowed values for double-entry accounting."""
        allowed = {"CREDIT", "DEBIT"}
        if v not in allowed:
            raise ValueError(f"entry_type must be one of {allowed}, got '{v}'")
        return v

    # Validate that amount is always positive (debits and credits are distinguished by entry_type)
    @field_validator("amount")
    @classmethod
    def validate_amount_positive(cls, v: float) -> float:
        """Amount must be positive; direction is indicated by entry_type, not sign."""
        if v <= 0:
            raise ValueError(f"amount must be positive, got {v}")
        return v
