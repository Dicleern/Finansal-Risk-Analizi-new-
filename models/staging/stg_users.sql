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
FROM {{ source('finansal_analiz', 'users_data') }}