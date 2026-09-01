with source as (
    select *
    from {{ source('nessie', 'webhooks') }}
)

select
    transaction_id,
    amount_paise,
    gateway_status,
    timestamp_utc,
    cast(timestamp_utc as date) as transaction_date,
    merchant_id,
    reconciliation_status,
    bank_ref_id,
    ingested_at,
    case
        when reconciliation_status = 'MATCHED' then true
        else false
    end as is_reconciled,
    case
        when reconciliation_status like 'EXCEPTION%' then true
        else false
    end as has_exception
from source
