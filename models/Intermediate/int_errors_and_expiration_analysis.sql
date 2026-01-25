WITH transactions AS (
    SELECT 
        id AS transaction_id,
        client_id,
        card_id,
        cast(date as date) as transaction_date,
        errors,
        use_chip
    FROM {{ ref('stg_raw_transaction_data') }}
    WHERE errors LIKE '%Bad Expiration%'
),
cards AS (
    SELECT 
        id AS card_id,
        last_day(parse_date('%m/%Y', expires)) as card_expiration_date
    FROM {{ ref('stg_raw_card_data') }}
),
joined_data AS (
    SELECT 
        t.*,
        c.card_expiration_date
    FROM transactions t
    JOIN cards c ON t.card_id = c.card_id
)
SELECT 
    *,
    COUNT(*) OVER(PARTITION BY transaction_date) as daily_error_count,
    CASE 
        WHEN transaction_date < card_expiration_date THEN 'false'
        ELSE 'true'
    END AS manual_entry_check
FROM joined_data

--transaction table da son kullanım tarihi geçmiş hatası alanları tespit ettim
--bunları card id ile eşleştirdim
--işlem tarihi ve card id tablosundaki son kullanma tarihini karşılaştırdım
--eğer işlem yaptığı tarih son kullanma tarihinden önce ise manuel giriş hatası olabilir
--yeni oluşan sütunda da manuel giriş hatası yapan müşteriler için false gerçekten kullanma tarihi geçmiş kartlar için ise true yazdırdım
-- aynı tarihte kaç kere bu hatayı yapmış bunu hesapladım
