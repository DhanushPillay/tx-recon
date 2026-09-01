with instruments as (
    select distinct
        coalesce(
            case when gateway_status in ('SUCCESS', 'FAILED', 'PENDING') then gateway_status end,
            'UNKNOWN'
        ) as instrument_status
    from {{ source('nessie', 'webhooks') }}
    where gateway_status is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['instrument_status']) }} as instrument_key,
    instrument_status,
    current_timestamp() as loaded_at
from instruments
