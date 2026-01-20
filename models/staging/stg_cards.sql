SELECT
      id as card_id
    , client_id
    , card_brand
    , card_type
    , has_chip            
    , num_cards_issued     
    , credit_limit
    , acct_open_date       
    , year_pin_last_changed
    , card_on_dark_web
FROM {{ source('finansal_analiz', 'card_data') }}       