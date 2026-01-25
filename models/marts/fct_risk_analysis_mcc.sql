WITH flat_errors AS (
    SELECT 
        mcc_id,
        use_chip,
        TRIM(flattened_error) as error_type
    FROM {{ ref('int_errors_details_analysis') }},
    UNNEST(SPLIT(error_type, ',')) as flattened_error 
    WHERE TRIM(flattened_error) != 'Insufficient Balance'
),

raw_errors AS (
    SELECT 
        mcc_id as mcc,
        use_chip,
        error_type,
        CASE 
            -- SWIPE VE CHIP İÇİN MANTIK
            WHEN use_chip IN ('Swipe Transaction', 'Chip Transaction') THEN
                CASE 
                    WHEN error_type IN ( 'Bad PIN') THEN 'User Error' --'Insufficient Balance',
                    WHEN error_type = 'Technical Glitch' 
                         OR error_type IN ('Bad Card Number', 'Bad CVV', 'Bad Expiration', 'Bad Zipcode') THEN 'System/Security Error'
                    ELSE 'Other/Combined'
                END
            
            -- ONLINE İÇİN MANTIK
            WHEN use_chip = 'Online Transaction' THEN
                CASE 
                    WHEN error_type IN ('Bad Card Number', 'Bad CVV', 'Bad Expiration', 'Bad Zipcode') THEN 'User Error' --'Insufficient Balance', 
                    WHEN error_type = 'Technical Glitch' THEN 'System/Security Error'
                    ELSE 'Other/Combined'
                END
            
            ELSE 'Other/Combined'
        END AS main_error_category
    FROM flat_errors
),

mcc_labels AS (
    SELECT mcc_code, description FROM {{ ref('stg_raw_mcc_code') }}
),

aggregated AS (
    SELECT 
        m.description as merchant_category,
        r.use_chip as use_chip,
        r.main_error_category,
        COUNT(*) as error_count
    FROM raw_errors r
    LEFT JOIN mcc_labels m ON r.mcc = m.mcc_code
    GROUP BY 1, 2, 3
)

SELECT 
    *,
    ROUND(100.0 * error_count / SUM(error_count) OVER(PARTITION BY merchant_category), 2) as category_risk_share_pct
FROM aggregated
ORDER BY merchant_category, error_count DESC

-- Her kategori için use chip tipine göre toplam hata sayılarını hesapladım
-- hataları kullanıcı ve sistem olarak ikiye ayırdım
-- hata yoğunluğunu hesapladım 
-- Insufficient Balance hatasını çıkaratarak hesaplama yaptım 
