-- models/intermediate/int_fraud_detection_signals.sql

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

cards as (
    select * from {{ ref('stg_cards') }}
),

fraud_signals as (
    select 
        t.*,
        c.credit_limit,
        -- 1. Limit Aşımı Analizi: İşlem tutarı kart limitine oranı [cite: 92]
        safe_divide(t.amount, c.credit_limit) as limit_usage_per_transaction,
        case 
            when safe_divide(t.amount, c.credit_limit) > 0.8 then 1 
            else 0 
        end as is_potential_limit_fraud,
        
        -- 2. Alışılmadık Zaman: Gece harcaması kontrolü (Parantezler düzeltildi) [cite: 94]
        case 
            when extract(hour from t.transaction_date) between 0 and 5 
                 and t.amount > 500 then 1 
            else 0 
        end as is_unusual_time_spend,

        -- 3. Hata Analizi: İşlem sırasında oluşan hataların varlığı [cite: 50, 96]
        case 
            when t.errors is not null and t.errors != '' then 1 
            else 0 
        end as contains_transaction_error
    from transactions t
    left join cards c on t.card_id = c.card_id
)

select * from fraud_signals