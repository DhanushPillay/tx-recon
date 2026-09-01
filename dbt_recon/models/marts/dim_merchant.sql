with merchants as (
    select
        merchant_id,
        count(*) as total_transactions,
        sum(amount_paise) as total_amount_paise,
        min(timestamp_utc) as first_seen,
        max(timestamp_utc) as last_seen
    from {{ source('nessie', 'webhooks') }}
    where merchant_id is not null
    group by merchant_id
)

select
    {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} as merchant_key,
    merchant_id,
    total_transactions,
    total_amount_paise,
    cast(first_seen as timestamp) as first_seen,
    cast(last_seen as timestamp) as last_seen,
    current_timestamp() as loaded_at
from merchants
