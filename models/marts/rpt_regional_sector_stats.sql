/*  Model: rpt_regional_sector_stats
    Purpose: I designed this reporting model to aggregate transaction metrics by city and sector.
    Author: Elif
*/
{{ config(materialized='table') }}
WITH enriched_data AS (
    -- I am pulling data from my enhanced fact table where I previously added distance and channel logic.
    SELECT * FROM {{ ref('fact_transactions_enriched') }})

SELECT

      merchant_state
    , merchant_city
    , category_name
    , general_group
    , users_state 
    , user_state_name

    -- I calculated the total count and volume to identify the most active sectors in each region.
    , COUNT(transaction_id) as total_transactions
    , SUM(amount) as total_volume
    , AVG(amount) as avg_ticket_size

    -- I counted online transactions specifically to measure digital adoption.
    , COUNT(CASE WHEN channel_type = 'Online' THEN 1 END) as online_transaction_count
    
    -- I calculated the digitalization rate by dividing online transactions by the total.
    -- This allows me to see which cities are shifting to e-commerce faster.
    , ROUND(COUNT(CASE WHEN channel_type = 'Online' THEN 1 END) / COUNT(*) ,2) as digitalization_rate

    -- I aggregated high-risk transactions to create a regional risk heatmap.
    , COUNT(CASE WHEN card_on_dark_web = true THEN 1 END) as fraud_risk_count

FROM enriched_data

GROUP BY   merchant_state
         , merchant_city
         , category_name
         , general_group
         , users_state 
         , user_state_name