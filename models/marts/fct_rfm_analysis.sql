{{ config(materialized='table') }}

with all_users as (
    select 
        id as user_id,
        gender,
        current_age,
        -- 🎯 Yeni ve Keskin Yaş Segmentasyonu
        case 
            when current_age < 25 then '18-24 (Young Adults)'
            when current_age between 25 and 34 then '25-34 (Young Professionals)'
            when current_age between 35 and 44 then '35-44 (Established Adults)'
            when current_age between 45 and 54 then '45-54 (Peak Earning Years)'
            when current_age between 55 and 64 then '55-64 (Pre-Retirement)'
            else '65+ (Senior Citizens)' 
        end as age_group,
        
        -- Looker/Power BI sıralaması için yardımcı kolon
        case 
            when current_age < 25 then 1
            when current_age between 25 and 34 then 2
            when current_age between 35 and 44 then 3
            when current_age between 45 and 54 then 4
            when current_age between 55 and 64 then 5
            else 6
        end as age_group_rank
    from {{ ref('stg_users_data') }}
),

customer_metrics as (
    select
        user_id,
        date_diff(
            (select max(transaction_at) from {{ ref('fct_financial_analysis') }}),
            max(transaction_at),
            day
        ) as recency_days,
        count(transaction_id) as frequency,
        sum(amount) as monetary
    from {{ ref('fct_financial_analysis') }}
    group by 1
),

rfm_scores as (
    select
        *,
        -- ✅ Recency: Sabit Gün Aralıkları
        case 
            when recency_days <= 7 then 5
            when recency_days <= 30 then 4
            when recency_days <= 90 then 3
            when recency_days <= 180 then 2
            else 1
        end as r_score,

        -- Frequency ve Monetary (Yüzdelik Dilimler)
        ntile(5) over (order by frequency asc) as f_score,
        ntile(5) over (order by monetary asc) as m_score
    from customer_metrics
),

final as (
    select
        u.user_id,
        u.gender,
        u.age_group,
        u.age_group_rank,
        r.recency_days,
        r.frequency,
        r.monetary,
        r.r_score,
        r.f_score,
        r.m_score,

        -- 🎯 5 Stratejik Segment
        case
            when r.user_id is null then 'Never Transacted Portfolio'

            when r_score >= 4 and f_score >= 4 and m_score >= 4 
                then 'VIP Portfolio'

            when r_score >= 3 and f_score >= 3 and m_score >= 3
                then 'Loyal Customers'

            when r_score >= 4 and f_score <= 2 and m_score <= 2
                then 'New Customers'

            else 'Low Engagement Segment'
        end as customer_segment,

        -- 🔢 Dashboard Sıralama Kolonu
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4 then 1
            when r_score >= 3 and f_score >= 3 and m_score >= 3 then 2
            when r_score >= 4 and f_score <= 2 then 3
            when r_score <= 2 and m_score >= 4 then 4
            when r.user_id is not null then 5
            else 6
        end as segment_rank

    from all_users u
    left join rfm_scores r on u.user_id = r.user_id
)

select * from final
