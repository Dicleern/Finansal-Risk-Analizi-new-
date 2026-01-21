-- models/marts/fct_financial_risk_and_fraud.sql
with
    risk_metrics as (
        -- int_financial_risk_metrics modelinden gelen veriler (Burada sütun adını
        -- user_id yapmıştık)
        select * from {{ ref("int_financial_risk_metrics") }}
    ),

    fraud_signals as (
        -- int_fraud_detection_signals modelinden gelen veriler
        -- Not: stg_transactions içinde 'client_id' sütununu 'user_id' olarak
        -- isimlendirdiğimizden emin olmalıyız
        select
            user_id,
            count(*) as total_transactions,
            sum(is_potential_limit_fraud) as count_limit_fraud_attempts,
            sum(is_unusual_time_spend) as count_night_spends,
            sum(contains_transaction_error) as count_system_errors
        from {{ ref("int_fraud_detection_signals") }}
        group by 1
    )

select
    r.*,
    coalesce(f.total_transactions, 0) as total_transactions,
    coalesce(f.count_limit_fraud_attempts, 0) as count_limit_fraud_attempts,
    coalesce(f.count_night_spends, 0) as count_night_spends,
    coalesce(f.count_system_errors, 0) as count_system_errors,
    -- Finansal Sağlık Skoru: Risk segmenti ve fraud denemelerine göre kategorizasyon
    -- [cite: 8, 22]
    case
        when r.financial_risk_segment = 'High Risk' or f.count_limit_fraud_attempts > 0
        then 'Critically Vulnerable'
        when r.financial_risk_segment = 'Low Risk' and f.count_limit_fraud_attempts = 0
        then 'Healthy'
        else 'Monitor Closely'
    end as customer_vulnerability_status
from risk_metrics r
left join fraud_signals f on r.user_id = f.user_id
