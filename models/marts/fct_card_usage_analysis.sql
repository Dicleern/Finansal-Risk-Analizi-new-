with base_data as (
    select * from {{ ref('int_card_spending_summary_gold') }}
)

select
    *,
    rank() over (
        partition by client_id 
        order by kullanim_sayisi desc
    ) as kullanim_sirasi,
    date_diff(son_kullanma_tarihi, '2020-02-29', day) as son_islem_ve_expiry_farki_gun,
    case 
        when date_diff(son_kullanma_tarihi, '2020-02-29', day) <= 60 then 1 
        else 0 
    end as son_kullanma_kritik_mi
from base_data
order by client_id, kullanim_sirasi


-- müşteri idlerine göre kartların en çok kullanılma durumlarını sıraladım 
-- son işlem tarihi ve expires arasındaki tarih farkını hesapladım
-- çıkan tarih farkı 60 ve 60'dan az ise 0 çok ise 1 yazdırdım.
-- update ettim
-- son kullanım tarihi - son işlem tarihi yapmak yerine user data tanlosundan analizin çekildi tarihi hesapladım.
-- 29.02.2020 yılına göre yaş hesaplaması yapılmış current ageler yazılmış o tarihe göre de son kullanıma ne kadar kaldığını hesapladım.