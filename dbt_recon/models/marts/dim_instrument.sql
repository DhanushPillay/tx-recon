with instruments as (
    select distinct
        case
            when gateway_status = 'SUCCESS' then 'SUCCESS'
            when gateway_status = 'FAILED' then 'FAILED'
            when gateway_status = 'PENDING' then 'PENDING'
            else 'UNKNOWN'
        end as instrument_status,
        gateway_status as original_status
    from {{ source('nessie', 'webhooks') }}
    where gateway_status is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['instrument_status']) }} as instrument_key,
    instrument_status,
    original_status,
    current_timestamp() as loaded_at
from instruments
