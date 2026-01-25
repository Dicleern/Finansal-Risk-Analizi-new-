/*
Model: int_transaction_leftjoin
Purpose: To combine Transaction, User, Card, Sector and Coordinate data into a single 'Master Table'.
Author: Elif
Note: This table forms the basis of Geographic Analysis and Fraud Detection models. */

WITH 
-- retrieving the transaction data from the staging layer.
  transactions AS ( SELECT * FROM {{ ref('stg_transactions') }}) 

-- extracting user demographic and financial data.
, users AS ( SELECT * FROM {{ ref('int_user_location') }}) 

-- retrieving card security and limit information.
, cards AS ( SELECT * FROM {{ ref('stg_cards') }})

-- extracting the explanations of the sector codes.
, mcc AS ( SELECT * FROM {{ ref('stg_mcc') }})

, zip_codes AS ( SELECT * FROM {{ ref('stg_zip_codes') }})

, user_location AS ( SELECT * FROM {{ ref('stg_users_location')}})


SELECT
    
    -- t is an abbreviation for transactions table.
      t.transaction_id
    , t.transaction_date
    , t.amount
    , t.use_chip       
    , t.errors          
    , t.merchant_id     
    , t.merchant_city
    , t.merchant_state
    , t.merchant_zip   

    -- z is an abbreviation for zip_codes table.
    , z.latitude AS merchant_lat
    , z.longitude AS merchant_lon

    -- c is an abbreviation for cards table.
    , c.card_id
    , c.card_brand
    , c.card_type
    , c.credit_limit         
    , c.has_chip AS card_has_chip 
    , c.card_on_dark_web      
    , c.num_cards_issued    
    , c.year_pin_last_changed
    , c.acct_open_date       
    
    -- u is an abbreviation for users table.
    , u.user_id
    , u.current_age
    , u.gender                
    , u.yearly_income
    , u.total_debt            
    , u.credit_score         
    , u.per_capita_income     
    , u.latitude AS user_home_lat
    , u.longitude AS user_home_lon
    , u.users_state 
    , u.state_name AS user_state_name
    , u.address
    
    -- m is an abbreviation for  mcc table.
    , m.category_name         

/* I chose to use LEFT JOIN to avoid losing the transaction 
even if user or card information is incomplete. */
FROM transactions as t
LEFT JOIN users as u ON t.client_id = u.user_id
LEFT JOIN cards as c ON t.card_id = c.card_id
LEFT JOIN mcc as m ON t.mcc = m.mcc_code
/* I am joining the zip_codes table by standardizing the merchant_zip column. 
First, I convert the zip code to an integer and then back to a string to remove any formatting inconsistencies. 
Finally, I use LPAD to add leading zeros, 
ensuring that every zip code is exactly 5 digits long before matching it to z.zip_code.*/
LEFT JOIN zip_codes z ON LPAD(SAFE_CAST(SAFE_CAST(t.merchant_zip AS INT64) AS STRING), 5, '0') = z.zip_code
