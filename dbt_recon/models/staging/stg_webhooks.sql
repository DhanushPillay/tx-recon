with source as (
    select *
    from {{ source('nessie', 'webhooks') }}
)

select
    transaction_id,
    amount_paise,
    gateway_status,
    timestamp_utc,
    merchant_id,
    reconciliation_status,
    bank_ref_id,
    ingested_at
from source
