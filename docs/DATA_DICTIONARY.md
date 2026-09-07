# Data Dictionary

This document defines the schema for the core data structures used in the reconciliation pipeline.

## Webhooks

Webhooks represent the initial payment notification received from the payment gateway. They are ingested from Kafka into the Iceberg `webhooks` table.

| Field | Type | Description | Constraints |
| :--- | :--- | :--- | :--- |
| `transaction_id` | String | Unique identifier for the transaction. | Primary key, cannot be null. |
| `merchant_id` | String | Identifier for the merchant. | Cannot be null. |
| `amount_paise` | Integer | The transaction amount in paise (1 INR = 100 paise). | Must be > 0. |
| `currency` | String | Three-letter currency code. | e.g., 'INR'. |
| `timestamp` | Timestamp | Time the webhook was received. | |
| `payment_instrument`| String | Method of payment. | e.g., 'UPI', 'CREDIT_CARD', 'DEBIT_CARD'. |
| `status` | String | Status of the payment. | e.g., 'SUCCESS', 'FAILED'. |

## Settlements

Settlement files are batch CSVs received from the bank, detailing funds actually cleared and deposited.

| Field | Type | Description | Constraints |
| :--- | :--- | :--- | :--- |
| `transaction_id` | String | Unique identifier for the transaction. | Must exist, must be unique within the file. |
| `bank_ref_id` | String | The bank's reference number for the settlement. | Cannot be null. |
| `settled_amount_paise`| Integer | The amount deposited to the merchant's account. | Must be > 0. |
| `settlement_date` | Date | The date the funds were settled. | |

## Reconciled output

The reconciliation process merges the settlements against the webhooks and calculates the expected fees. The final output is written to the Iceberg `reconciled` table.

| Field | Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | String | Unique identifier for the transaction. |
| `status` | String | Reconciliation status. Can be `MATCHED`, `FEE_MISMATCH`, `MISSING_WEBHOOK`, `ORPHAN`, etc. |
| `webhook_amount` | Integer | The original amount from the webhook. |
| `settled_amount` | Integer | The cleared amount from the settlement file. |
| `expected_fee` | Integer | The calculated fee (MDR + GST) based on the rate card. |
| `actual_fee` | Integer | The difference between the webhook amount and the settled amount. |
| `fee_diff` | Integer | `actual_fee - expected_fee`. Should be 0 for a perfect match. |

## Data types and constraints

*   **Currency as integers:** All currency fields (`amount_paise`, `settled_amount_paise`, `expected_fee`, etc.) are stored as integers representing the smallest currency unit (paise). This prevents floating-point inaccuracies.
*   **Validation:** Settlement files are validated via Pandera before processing. Rows violating constraints (e.g., negative amounts or duplicate transaction IDs) are stripped from the dataset and placed into quarantine.
