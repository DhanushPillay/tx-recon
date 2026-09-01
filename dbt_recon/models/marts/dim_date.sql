with webhooks as (
    select distinct cast(timestamp_utc as date) as date_day
    from {{ source('nessie', 'webhooks') }}
    where timestamp_utc is not null
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    case
        when extract(dow from date_day) in (0, 6) then true
        else false
    end as is_weekend,
    extract(quarter from date_day) as quarter,
    concat(cast(extract(year from date_day) as string), '-', lpad(cast(extract(month from date_day) as string), 2, '0')) as year_month
from webhooks
