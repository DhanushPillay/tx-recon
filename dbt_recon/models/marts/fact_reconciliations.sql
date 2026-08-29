with webhooks as (
    select *
    from {{ ref('stg_webhooks') }}
)

select
    transaction_id,
    amount_paise,
    reconciliation_status,
    bank_ref_id,
    gateway_status,
    timestamp_utc,
    ingested_at
from webhooks
