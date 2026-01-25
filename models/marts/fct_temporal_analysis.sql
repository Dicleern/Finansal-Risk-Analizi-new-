WITH user_daily_errors AS (
    SELECT 
        client_id,
        DATE(date) AS error_date,
        use_chip AS transaction_method,
        card_id,
        TRIM(split_error) AS specific_error_type
    FROM {{ ref('int_errors_details_analysis') }},
    UNNEST(SPLIT(error_type, ',')) AS split_error
),

daily_error_frequency AS (
    SELECT 
        client_id,
        error_date,
        transaction_method,
        card_id,
        specific_error_type,
        COUNT(*) AS daily_occurrence_count
    FROM user_daily_errors
    GROUP BY 1, 2, 3, 4, 5
)


SELECT 
    client_id,
    error_date,
    transaction_method,
    card_id,
    specific_error_type,
    daily_occurrence_count
FROM daily_error_frequency
WHERE daily_occurrence_count > 2 
ORDER BY daily_occurrence_count DESC, error_date DESC

-- error typeları virgüllerden ayırdım
-- kullanıcılar aynı aynı gün içerisinde 2 den fazla yaptıysa belirttim
-- yetersiz bakiye hatasını alınmış ve denenmiş