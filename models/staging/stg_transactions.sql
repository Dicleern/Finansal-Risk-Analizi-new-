select 
    id as transaction_id,
    card_id,
    client_id as user_id,  -- Bu satırın varlığından emin ol
    amount,
    date as transaction_date,
    merchant_city,
    errors
from {{ source('finansal_analiz', 'transaction_data') }}