{{ config(materialized='table') }}

with rfm_data as (
    -- 1. Hazırladığımız RFM tablosu
    select * from {{ ref('fct_rfm_analysis') }}
),

behavior_metrics as (
    -- 2. Davranışsal metrikler
    select
        client_id as user_id,
        count(*) as total_tx_count,
        
        -- Kanal Tercihi Metrikleri
        count(case when use_chip = 'Online Transaction' then 1 end) as online_tx_count,
        
        -- Finansal Risk Metrikleri
        count(case when errors like '%Insufficient Balance%' then 1 end) as limit_error_count,
        count(case when errors like '%Bad PIN%' or errors like '%Bad CVV%' then 1 end) as security_error_count,
        
        -- 🚀 FIX: Amount zaten sayı olduğu için direkt AVG alıyoruz
        avg(amount) as avg_ticket_size
        
    from {{ ref('stg_transaction') }} 
    group by 1
),

behavior_segments as (
    -- 3. Davranışsal ve Risk Segmentleri
    select
        user_id,
        total_tx_count,
        avg_ticket_size,
        
        -- Kanal Segmentasyonu
        case 
            when (online_tx_count * 1.0 / nullif(total_tx_count, 0)) > 0.7 then 'Dijital Yerli (Online Odaklı)'
            when (online_tx_count * 1.0 / nullif(total_tx_count, 0)) < 0.2 then 'Gelenekselci (Mağaza Odaklı)'
            else 'Hibrit Kullanıcı'
        end as kanal_tercihi,

        -- Finansal Sağlık Segmentasyonu
        case 
            when limit_error_count > 5 then 'Limit Zorlayan (Kredi/Limit İhtiyacı)'
            when security_error_count > 3 then 'Operasyonel Risk (Güvenlik/Eğitim)'
            else 'Güvenli Profil'
        end as risk_profili
    from behavior_metrics
)

-- 4. Final Birleştirme (360 Derece Görünüm)
select
    r.*,
    b.total_tx_count,
    b.avg_ticket_size,
    coalesce(b.kanal_tercihi, 'Bilinmiyor') as kanal_tercihi,
    coalesce(b.risk_profili, 'Bilinmiyor') as risk_profili
from rfm_data r
left join behavior_segments b on r.user_id = b.user_id