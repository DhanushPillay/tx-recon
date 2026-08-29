with webhooks as (
    select *
    from {{ ref('stg_webhooks') }}
)

select distinct
    merchant_id
from webhooks
where merchant_id is not null
