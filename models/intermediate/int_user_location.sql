{{ config( materialized='table' ) }}

WITH users AS (
    SELECT
          id as user_id
        , current_age
        , address
        , latitude
        , longitude
        , per_capita_income
        , gender
        , yearly_income 
        , total_debt    
        , credit_score
        , ST_GEOGPOINT(longitude, latitude) as user_point_geom
    FROM {{ source('finansal_analiz', 'users_data') }}
),

states AS (
    SELECT
          `state` as users_state 
        , state_name             
        , state_geom            
    FROM {{ source('user_location', 'users_location') }}
)

SELECT
      u.user_id
    , u.current_age
    , u.address
    , u.latitude
    , u.longitude
    , u.per_capita_income
    , u.gender
    , u.yearly_income 
    , u.total_debt    
    , u.credit_score
    , s.users_state 
    , s.state_name 
FROM users u
LEFT JOIN states s
    ON ST_CONTAINS(s.state_geom, u.user_point_geom)