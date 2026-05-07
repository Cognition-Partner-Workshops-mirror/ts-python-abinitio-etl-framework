"""
Transaction processor for converting raw transactions into double-entry journal entries.
Implements the core accounting logic: every transaction generates a matching DEBIT and CREDIT
pair to maintain the fundamental accounting equation (Assets = Liabilities + Equity).
"""
import logging
from typing import List

import pandas as pd

from etl.core.journal_entry import JournalEntry

logger = logging.getLogger(__name__)


class TransactionProcessor:
    """
    Converts validated transaction records into double-entry journal entries.

    For each transaction, two journal entries are created:
      - PURCHASE (txn_type=1): Debit customer account, Credit merchant account
      - REFUND (txn_type=2): Credit customer account, Debit merchant account

    This ensures that total debits always equal total credits across the ledger.
    """

    def __init__(self, currency: str = "USD"):
        """
        Initialize the transaction processor.

        Args:
            currency: Default currency code for all journal entries (default "USD").
        """
        self.currency = currency
        # Sequential counter for generating unique journal IDs
        self._journal_counter = 0

    def _next_journal_id(self) -> str:
        """Generate the next sequential journal ID (e.g., JRN-0001, JRN-0002)."""
        self._journal_counter += 1
        return f"JRN-{self._journal_counter:04d}"

    def process_transaction(self, row: pd.Series) -> List[JournalEntry]:
        """
        Convert a single transaction row into a pair of journal entries.

        Double-entry accounting rules:
          - PURCHASE (txn_type=1): Customer is debited, Merchant is credited
          - REFUND (txn_type=2): Customer is credited, Merchant is debited

        Args:
            row: A pandas Series containing transaction fields including
                 txn_id, customer_id, txn_type, merchant_name, amount, txn_timestamp.

        Returns:
            A list of exactly 2 JournalEntry objects (one DEBIT, one CREDIT).
        """
        txn_id = str(row["txn_id"])
        customer_id = str(row["customer_id"])
        txn_type = int(row["txn_type"])
        merchant_name = str(row["merchant_name"])
        amount = float(row["amount"])
        txn_timestamp = str(row["txn_timestamp"])

        # Build account identifiers following the naming convention
        customer_account = f"CUST_{customer_id}"
        merchant_account = f"MERCH_{merchant_name}"

        entries = []

        if txn_type == 1:
            # PURCHASE: Customer pays merchant
            # Debit the customer account (money leaves the customer)
            entries.append(JournalEntry(
                journal_id=self._next_journal_id(),
                txn_id=txn_id,
                entry_type="DEBIT",
                account=customer_account,
                amount=amount,
                currency=self.currency,
                txn_timestamp=txn_timestamp,
                description=f"Purchase debit from customer {customer_id}",
            ))
            # Credit the merchant account (money enters the merchant)
            entries.append(JournalEntry(
                journal_id=self._next_journal_id(),
                txn_id=txn_id,
                entry_type="CREDIT",
                account=merchant_account,
                amount=amount,
                currency=self.currency,
                txn_timestamp=txn_timestamp,
                description=f"Purchase credit to merchant {merchant_name}",
            ))
        elif txn_type == 2:
            # REFUND: Merchant returns money to customer
            # Credit the customer account (money returns to the customer)
            entries.append(JournalEntry(
                journal_id=self._next_journal_id(),
                txn_id=txn_id,
                entry_type="CREDIT",
                account=customer_account,
                amount=amount,
                currency=self.currency,
                txn_timestamp=txn_timestamp,
                description=f"Refund credit to customer {customer_id}",
            ))
            # Debit the merchant account (money leaves the merchant)
            entries.append(JournalEntry(
                journal_id=self._next_journal_id(),
                txn_id=txn_id,
                entry_type="DEBIT",
                account=merchant_account,
                amount=amount,
                currency=self.currency,
                txn_timestamp=txn_timestamp,
                description=f"Refund debit from merchant {merchant_name}",
            ))
        else:
            logger.warning(f"Unknown txn_type={txn_type} for txn_id={txn_id}, skipping")

        return entries

    def process_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a batch of transactions and return all journal entries as a DataFrame.

        Iterates over each row in the input DataFrame, generates journal entry pairs,
        and collects them into a single output DataFrame suitable for file output.

        Args:
            df: Input DataFrame with transaction columns (txn_id, txn_type, etc.)

        Returns:
            DataFrame with journal entry columns: journal_id, txn_id, entry_type,
            account, amount, currency, txn_timestamp, description.
        """
        all_entries = []
        for _, row in df.iterrows():
            entries = self.process_transaction(row)
            for entry in entries:
                all_entries.append(entry.model_dump())

        logger.info(f"Processed {len(df)} transactions into {len(all_entries)} journal entries")
        return pd.DataFrame(all_entries)
