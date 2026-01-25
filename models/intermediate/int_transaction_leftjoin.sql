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

--I retrieved the data from a BigQuery public dataset.
, zip_codes AS ( SELECT * FROM {{ ref('stg_zip_codes') }})

--I retrieved the data from a BigQuery public dataset.
, user_location AS ( SELECT * FROM {{ ref('stg_users_location')}})

-- There were 189 missing country-city combinations. 
--I queried them using BigQuery and saved the results as a CSV file. 
--Then, I used AI assistance to match these points with location information.
, location_lookup AS ( SELECT 
          merchant_city 
        , merchant_state 
        , SAFE_CAST(latitude AS FLOAT64) as latitude
        , SAFE_CAST(longitude AS FLOAT64) as longitude FROM {{ ref('merchant_locations') }})

-- I obtained this dataset from https://simplemaps.com/data/us-cities.
, us_city_lookup AS ( SELECT 
        LOWER(TRIM(city)) as join_city,
        TRIM(state_id) as join_state_code,
        SAFE_CAST(lat AS FLOAT64) as us_latitude,
        SAFE_CAST(lng AS FLOAT64) as us_longitude,
        `population` FROM {{ ref('uscities') }})

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

    -- 1st priority: The exact coordinates from the Zip Code table.
    -- 2nd priority: If that is not available, the city center coordinates from the Seed table.
    -- 3rd priority : Look for mismatched zip codes by state and city.
, COALESCE(
          SAFE_CAST(z.latitude AS FLOAT64) 
        , loc.latitude 
        , us.us_latitude) AS merchant_lat
      
    , COALESCE(
          SAFE_CAST(z.longitude AS FLOAT64)
        , loc.longitude
        , us.us_longitude) AS merchant_lon

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
    

-- I linked the tables according to city and country names.
LEFT JOIN location_lookup loc
    ON LOWER(TRIM(t.merchant_city)) = LOWER(TRIM(loc.merchant_city)) 
    AND LOWER(TRIM(t.merchant_state)) = LOWER(TRIM(loc.merchant_state))

/*I downloaded a comprehensive dataset for the states with mismatched zip codes and performed 
a city-by-state match. I managed to reduce the missing 1.7 billion data points to 12,000.*/
LEFT JOIN us_city_lookup us
    ON LOWER(TRIM(t.merchant_city)) = us.join_city
    AND TRIM(t.merchant_state) = us.join_state_code


