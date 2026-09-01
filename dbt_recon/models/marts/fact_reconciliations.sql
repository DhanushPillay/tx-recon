with webhooks as (
    select * from {{ ref('stg_webhooks') }}
),

daily_agg as (
    select
        transaction_date,
        merchant_id,
        count(*) as total_transactions,
        sum(amount_paise) as total_amount_paise,
        sum(case when is_reconciled then 1 else 0 end) as reconciled_count,
        sum(case when has_exception then 1 else 0 end) as exception_count,
        sum(case when reconciliation_status = 'MATCHED' then amount_paise else 0 end) as reconciled_amount_paise,
        sum(case when reconciliation_status = 'EXCEPTION_FEE_MISMATCH' then amount_paise else 0 end) as fee_mismatch_amount_paise,
        sum(case when reconciliation_status = 'PENDING_SETTLEMENT' then amount_paise else 0 end) as pending_amount_paise
    from webhooks
    group by transaction_date, merchant_id
)

select
    {{ dbt_utils.generate_surrogate_key(['transaction_date', 'merchant_id']) }} as recon_key,
    transaction_date,
    merchant_id,
    total_transactions,
    total_amount_paise,
    reconciled_count,
    exception_count,
    reconciled_amount_paise,
    fee_mismatch_amount_paise,
    pending_amount_paise,
    case
        when total_transactions > 0
        then round(cast(reconciled_count as float) / cast(total_transactions as float) * 100, 2)
        else 0
    end as reconciliation_rate_pct
from daily_agg
