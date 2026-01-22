SELECT
      zipcode as zip_code
    , city
    , state_code as state
    , latitude
    , longitude
FROM {{ source('zip_code_final', 'zipcode_area') }}