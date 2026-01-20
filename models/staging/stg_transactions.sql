SELECT
      id as transaction_id
    , client_id
    , card_id
    , date as transaction_date
    , amount
    , use_chip
    , merchant_id
    , merchant_city
    , merchant_state
    , mcc
    , errors
    , zip as merchant_zip
FROM {{ source('finansal_analiz', 'transaction_data') }}