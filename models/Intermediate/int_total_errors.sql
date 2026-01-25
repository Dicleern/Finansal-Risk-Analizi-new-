SELECT 
    split_error as error_type, 
    COUNT(*) as islem_sayisi
FROM {{ ref('stg_raw_transaction_data') }},
UNNEST(SPLIT(errors, ',')) AS split_error
WHERE errors IS NOT NULL
GROUP BY 1
ORDER BY islem_sayisi DESC

-- her hatanın toplam kaç işlemde olduğunu buldum