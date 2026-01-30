{{ config(
    materialized='table'
) }}

with transactions as (
    select * from {{ ref('stg_transaction') }}
),
users as (
    select * from {{ ref('stg_users_data') }} 
),
cards as (
    select * from {{ ref('stg_card') }}
),
mcc as (
    select * from {{ ref('stg_mcc_codes') }}
)

select 
    -- 🔑 Transaction Grain
    t.id as transaction_id,

    -- 💰 Transaction Amount
    cast(t.amount as numeric) as amount,
    cast(t.date as timestamp) as transaction_at,

    -- 👤 Customer
    u.id as user_id,
    u.current_age,
    u.gender,
    cast(u.yearly_income as numeric) as yearly_income,
    u.credit_score,

case 
    when u.current_age < 25 then '18-24 (Young Adults)'
    when u.current_age between 25 and 34 then '25-34 (Young Professionals)'
    when u.current_age between 35 and 44 then '35-44 (Established Adults)'
    when u.current_age between 45 and 54 then '45-54 (Peak Earning Years)'
    when u.current_age between 55 and 64 then '55-64 (Pre-Retirement)'
    else '65+ (Senior Citizens)' 
end as age_group,

    -- 💳 Card
    c.card_brand,
    c.card_type,
    cast(c.credit_limit as numeric) as credit_limit,

    -- 🏷️ MCC Category (Ham Açıklama)
    m.description as raw_category_name,

    -- 🚀 FIX: t.mcc CAST edilerek kontrol ediliyor
  case 
    when cast(t.mcc as string) in ('5812', '5814', '5813', '5816', '7996', '7832', '7922', '7995', '7801', '7802') 
        then 'Food, Dining & Entertainment'
    
    when cast(t.mcc as string) in ('5411', '5499', '5300', '5921') 
        then 'Grocery & Food Stores'
    
    when cast(t.mcc as string) in ('5541', '4784', '4121', '4111', '3722', '3771', '3775', '4112', '4131', '4214', '5533', '7531', '7538', '7542', '7549') 
        then 'Transportation & Fuel'
    
    when cast(t.mcc as string) in ('5311', '5310', '5661', '5977', '5815', '5655', '5651', '5970', '5932', '5621', '5942', '5192', '5094', '5947', '3132', '5941', '5733') 
        then 'Retail & Shopping'
    
    when cast(t.mcc as string) in ('5211', '5719', '5251', '5912', '5193', '3504', '7210', '3640', '5732', '5712', '3174', '5261', '3144', '1711', '5722', '5045', '3684') 
        then 'Home, Lifestyle & Technology'
    
    when cast(t.mcc as string) in ('8099', '8021', '8011', '8041', '8043', '8049', '8062', '7230') 
        then 'Healthcare & Personal Care'
    
    when cast(t.mcc as string) in ('4900', '4829', '4814', '4899', '3780', '9402', '7349', '6300', '7393', '8111', '8931', '7276') 
        then 'Utilities, Services & Government'
    
    when cast(t.mcc as string) in ('3390', '3596', '3730', '3509', '3389', '3393', '3001', '3395', '3058', '3387', '3405', '3359', '3260', '3256', '3006', '3007', '3075', '3066', '3005', '3000', '3008', '3009') 
        then 'Industrial, Manufacturing & Logistics'
    
    when cast(t.mcc as string) in ('4722', '7011', '4511', '4411') 
        then 'Travel & Accommodation'
    
    else 'Other / Undefined'
end as category_group


from transactions t
left join users u 
    on t.client_id = u.id
left join cards c 
    on t.card_id = c.id
left join mcc m 
    on cast(t.mcc as string) = cast(m.mcc_code as string)