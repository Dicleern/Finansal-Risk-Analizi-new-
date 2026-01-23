{{ config(materialized='table') }}

WITH base_users AS (
    SELECT 
        id, 
        current_age
    FROM {{ ref('stg_raw_users_data') }}
),

base_transactions AS (
    SELECT 
        client_id,
        use_chip
    FROM {{ ref('stg_raw_transaction_data') }}
),

age_segmentation AS (
    SELECT 
        u.id,
        t.use_chip,
        CASE 
            WHEN u.current_age < 18 THEN 'Çocuk / Reşit Değil'
            WHEN u.current_age BETWEEN 18 AND 24 THEN 'Genç Yetişkin (18-24)'
            WHEN u.current_age BETWEEN 25 AND 34 THEN 'Erken Kariyer (25-34)'
            WHEN u.current_age BETWEEN 35 AND 49 THEN 'Orta Kariyer (35-49)'
            WHEN u.current_age BETWEEN 50 AND 64 THEN 'Olgun Yetişkin (50-64)'
            ELSE '65+ Emekli'
        END AS age_group
    FROM base_users u
    JOIN base_transactions t ON u.id = t.client_id
)

SELECT 
    age_group,
    use_chip,
    COUNT(*) AS total_transactions
FROM age_segmentation
GROUP BY 1, 2
ORDER BY age_group, total_transactions DESC