SELECT
      mcc_code
    , description as category_name
FROM {{ source('finansal_analiz', 'mcc_codes') }}