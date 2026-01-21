-- models/intermediate/int_financial_risk_metrics.sql
with users as (
    select * from {{ ref('stg_users') }}
),
cards as (
    select * from {{ ref('stg_cards') }}
),
transactions as (
    select * from {{ ref('stg_transactions') }}
),
card_utilization as (
    select 
        c.user_id,
        sum(t.amount) as total_spend_amount,
        max(c.credit_limit) as total_card_limit,
        safe_divide(sum(t.amount), max(c.credit_limit)) * 100 as limit_utilization_rate
    from cards c
    left join transactions t on c.card_id = t.card_id
    group by 1
),
risk_calculations as (
    select
        u.*,
        safe_divide(u.total_debt, u.yearly_income) as debt_to_income_ratio,
        case 
            when safe_divide(u.total_debt, u.yearly_income) > 0.5 then 'High Risk'
            else 'Low Risk'
        end as financial_risk_segment
    from users u
)
select 
    r.*,
    coalesce(u.limit_utilization_rate, 0) as limit_utilization_rate
from risk_calculations r
left join card_utilization u on r.user_id = u.user_id