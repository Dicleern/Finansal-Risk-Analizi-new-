select 
    id as card_id,         -- 'id' sütununu 'card_id' olarak isimlendirdik
    client_id as user_id,  -- Şemada 'client_id' olarak görünüyor
    card_type,
    credit_limit
from {{ source('finansal_analiz', 'card_data') }}