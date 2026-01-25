SELECT
    `state` as users_state
    , state_name
    , state_geom
FROM {{ source('user_location', 'users_location') }}