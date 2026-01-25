WITH exploded_errors AS (
    SELECT 
        use_chip AS transaction_method,
        TRIM(split_error) AS specific_error_type
    FROM {{ ref('int_errors_details_analysis') }},
    UNNEST(SPLIT(error_type, ',')) AS split_error
),

categorized_analysis AS (
    SELECT 
        transaction_method,
        specific_error_type,
        CASE WHEN specific_error_type = 'Insufficient Balance' THEN 1 ELSE 0 END AS is_insufficient_balance,
        CASE WHEN specific_error_type = 'Bad PIN' THEN 1 ELSE 0 END AS is_bad_pin,
        CASE WHEN specific_error_type = 'Bad Card Number' THEN 1 ELSE 0 END AS is_bad_card_number,
        CASE WHEN specific_error_type = 'Bad Expiration' THEN 1 ELSE 0 END AS is_bad_expiration,
        CASE WHEN specific_error_type = 'Bad CVV' THEN 1 ELSE 0 END AS is_bad_cvv,
        CASE WHEN specific_error_type = 'Bad Zipcode' THEN 1 ELSE 0 END AS is_bad_zipcode,
        CASE WHEN specific_error_type = 'Technical Glitch' THEN 1 ELSE 0 END AS is_technical_glitch
    FROM exploded_errors
),

final_summary AS (
    SELECT 
        transaction_method,
        SUM(is_insufficient_balance) AS total_insufficient_balance,
        SUM(is_bad_pin) AS total_bad_pin,
        SUM(is_bad_card_number) AS total_bad_card_number,
        SUM(is_bad_expiration) AS total_bad_expiration,
        SUM(is_bad_cvv) AS total_bad_cvv,
        SUM(is_bad_zipcode) AS total_bad_zipcode,
        SUM(is_technical_glitch) AS total_technical_glitch,
        COUNT(*) AS total_errors_count 
    FROM categorized_analysis
    GROUP BY 1
)

SELECT 
    *,
    ROUND(100.0 * total_insufficient_balance / total_errors_count, 2) AS insufficient_balance_pct,
    ROUND(100.0 * total_bad_pin / total_errors_count, 2) AS bad_pin_pct,
    ROUND(100.0 * total_bad_card_number / total_errors_count, 2) AS bad_card_number_pct,
    ROUND(100.0 * total_bad_expiration / total_errors_count, 2) AS bad_expiration_pct,
    ROUND(100.0 * total_bad_cvv / total_errors_count, 2) AS bad_cvv_pct,
    ROUND(100.0 * total_bad_zipcode / total_errors_count, 2) AS bad_zipcode_pct,
    ROUND(100.0 * total_technical_glitch / total_errors_count, 2) AS technical_glitch_pct
FROM final_summary
ORDER BY total_errors_count DESC

-- Transaction methodlarına göre hata türlerini topladım
-- her hata türünden kaçar tane olduğunu ve dağılımını gösterdim
-- kombineli hata türleri vardı trim ile , kullanarak ayırdım hesaba öyle ekledim 
