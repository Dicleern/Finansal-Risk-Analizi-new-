{{ config(materialized='table') }}

with user_demographics as (
    -- 1. Kullanıcı Temel Bilgileri
    select 
        id as user_id,
        gender as cinsiyet,
        current_age as yas,
        case 
            when current_age < 25 then '18-24 (Young Adults)'
            when current_age between 25 and 34 then '25-34 (Young Professionals)'
            when current_age between 35 and 44 then '35-44 (Established Adults)'
            when current_age between 45 and 54 then '45-54 (Peak Earning Years)'
            when current_age between 55 and 64 then '55-64 (Pre-Retirement)'
            else '65+ (Senior Citizens)' 
        end as yas_grubu,
        -- Sayısal temizlik (Hata almamak için)
        cast(replace(replace(cast(yearly_income as string), '$', ''), ',', '') as numeric) as yillik_gelir,
        credit_score as kredi_skoru
    from {{ ref('stg_users_data') }}
),

card_details as (
    -- 2. Kart Bilgileri: Toplam Limit, Adet ve Birincil Kart Tipi
    with card_ranking as (
        select 
            client_id as user_id,
            card_brand,
            card_type,
            cast(replace(replace(cast(credit_limit as string), '$', ''), ',', '') as numeric) as card_limit,
            -- En yüksek limitli kartı "Ana Kart" olarak belirliyoruz
            row_number() over (partition by client_id order by cast(replace(replace(cast(credit_limit as string), '$', ''), ',', '') as numeric) desc) as rank_desc
        from {{ ref('stg_card') }}
    )
    select 
        user_id,
        count(*) as toplam_kart_sayisi,
        sum(card_limit) as toplam_kredi_limiti,
        max(case when rank_desc = 1 then card_brand end) as ana_kart_markasi,
        max(case when rank_desc = 1 then card_type end) as ana_kart_tipi
    from card_ranking
    group by 1
),

spending_summary as (
    -- 3. Favori Harcama Kategorisi (Fct_financial_analysis'den hazır çekiyoruz)
    with ranked_spends as (
        select 
            user_id,
            category_group, -- 
            sum(amount) as toplam_tutar,
            row_number() over (partition by user_id order by sum(amount) desc) as rank_desc
        from {{ ref('fct_financial_analysis') }}
        group by 1, 2
    )
    select 
        user_id,
        category_group as favori_kategori
    from ranked_spends
    where rank_desc = 1
)

-- 4. FİNAL BİRLEŞTİRME
select
    d.*,
    coalesce(c.toplam_kart_sayisi, 0) as toplam_kart_sayisi,
    coalesce(c.toplam_kredi_limiti, 0) as toplam_kredi_limiti,
    coalesce(c.ana_kart_markasi, 'Kart Yok') as ana_kart_markasi,
    coalesce(c.ana_kart_tipi, 'Kart Yok') as ana_kart_tipi,
    coalesce(s.favori_kategori, 'İşlem Verisi Yok') as favori_kategori
from user_demographics d
left join card_details c on d.user_id = c.user_id
left join spending_summary s on d.user_id = s.user_id