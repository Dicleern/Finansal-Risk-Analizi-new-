-- models/staging/stg_users.sql
select
    id as user_id,         -- 'id' sütununu 'user_id' olarak isimlendirdik
    current_age as age,
    yearly_income,
    total_debt,
    credit_score
from {{ source('finansal_analiz', 'users_data') }}