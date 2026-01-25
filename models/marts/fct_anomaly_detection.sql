with source_transactions as (
    select * from {{ ref('stg_raw_transaction_data') }}
),

--Her kullanıcının işlem yaptığı saatleri ve frekansını hesapladım
user_hour_profile as (
    select
        client_id,
        extract(hour from cast(date as timestamp)) as txn_hour,
        count(*) as txn_count
    from source_transactions
    group by 1, 2
),

--"Normal" saatleri belirledim 
typical_hours as (
    select
        client_id,
        txn_hour
    from user_hour_profile
    where txn_count >= 2
),

--Mevcut işlemleri kullanıcının profil saatleri ile karşılaştırdım
final_analysis as (
    select
        t.id as transaction_id,
        t.client_id,
        t.date,
        t.amount,
        extract(hour from cast(t.date as timestamp)) as current_txn_hour,
        case 
            when th.txn_hour is null then true 
            else false 
        end as is_outside_normal_hours
    from source_transactions t
    left join typical_hours th 
        on t.client_id = th.client_id 
        and extract(hour from cast(t.date as timestamp)) = th.txn_hour
)

--Sadece normal saatlerin dışındaki işlemleri getirdi
select * from final_analysis 
where is_outside_normal_hours = true
order by amount desc


-- tabloda normal işlem saatleri dışında işlem yapan kullanıcılar işlem tarihleri ve saatleri ve işlem tutarları var
-- current txt hour 24 saatlik dilime göre saati gösteriyor görüntü kolaylığı olsun diye 
-- özellikle gece 00:00 ve 05:00 saatleri arasındaki işlemler daha tehlikeli