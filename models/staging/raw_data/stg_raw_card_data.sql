{{ config(materialized='table') }}
select * from {{ source('Finansal_analiz_raw', 'card_data') }}
