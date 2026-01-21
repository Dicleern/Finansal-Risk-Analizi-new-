with card_info as (
    select 
        id as card_id,
        client_id,
        card_brand,
        card_type,
        last_day(parse_date('%m/%Y', expires)) as son_kullanma_tarihi,
        credit_limit
    from {{ ref('stg_raw_card_data') }}
),

transactions as (
    select
        card_id,
        amount,
        cast(date as date) as islem_tarihi
    from {{ ref('stg_raw_transaction_data') }}
),

usage_stats as (
    select
        card_id,
        count(*) as kullanim_sayisi,
        sum(amount) as toplam_tutar,
        max(islem_tarihi) as son_islem_tarihi,
        min(islem_tarihi) as ilk_islem_tarihi
    from transactions
    group by 1
)

select
    c.*,
    coalesce(u.kullanim_sayisi, 0) as kullanim_sayisi,
    coalesce(u.toplam_tutar, 0) as toplam_tutar,
    u.ilk_islem_tarihi,
    u.son_islem_tarihi
from card_info c
left join usage_stats u on c.card_id = u.card_id

-- join işlemi yaparak card_info ve transaction_data tabloları birleştirdim
-- müşterilerin card_idlere göre ilk işlem tarihleri ve son işlem tarihleri hesapladım
-- kartları kaçar kez kullandıklarını hesaplandım 
-- card_id'ye göre de toplam tutar hesapladım. Limitin üzerinde çıkması normal çünkü tüm tarihler boyunca toplam harcamasına baktım