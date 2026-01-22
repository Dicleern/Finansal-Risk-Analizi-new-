/*  Model: fact_transactions_enriched
    Purpose: Adding distance and digitalization metrics to analyze each operation.
    Author: Elif
*/

{{ config(materialized='table') }}

WITH master_table AS ( SELECT * FROM {{ ref('int_transaction_leftjoin') }} )

SELECT
    
      transaction_id
    , user_id
    , transaction_date
    , use_chip

    , CASE 
         WHEN use_chip = 'Online Transaction' THEN 'Online'
         ELSE 'Physical Transaction'
      END AS channel_type
    
    /* I measured the distance between the user's house and the seller in meters, 
    then divided by 1000 to convert it to kilometers.*/
    , ST_DISTANCE(
        ST_GEOGPOINT(user_home_lon, user_home_lat), 
        ST_GEOGPOINT(merchant_lon, merchant_lat)) / 1000 AS distance_km

    , user_home_lon
    , user_home_lat
    , merchant_lon
    , merchant_lat
    , merchant_city
    , merchant_state
    , category_name 
    , amount
    , card_on_dark_web
    , errors

FROM master_table
