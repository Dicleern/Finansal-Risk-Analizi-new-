WITH errors AS (
    SELECT * FROM {{ ref('stg_raw_errors_is_not_null') }}
),
mcc_codes AS (
    SELECT 
        mcc_code, 
        description AS mcc_description 
    FROM {{ ref('stg_raw_mcc_code') }}
)

SELECT
    e.id AS transaction_id,
    e.date,
    e.client_id,
    e.card_id,
    e.merchant_id,
    e.merchant_city,
    e.merchant_state,
    e.use_chip,
    e.errors AS error_type,
    e.mcc AS mcc_id,
    m.mcc_description,
    CASE WHEN e.errors like '%Technical Glitch%' THEN 1 ELSE 0 END AS is_technical_glitch
FROM errors e
LEFT JOIN mcc_codes m ON e.mcc = m.mcc_code

-- mcc kodlarını ve transaction kayıtlarını birleştirdim 
-- her tranbsactionın teknik kaynaklı mı olup olmadığını inceledim
-- teknik olanlara 1 yazdırdım 
