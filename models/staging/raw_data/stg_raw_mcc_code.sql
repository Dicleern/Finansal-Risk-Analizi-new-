{{ config(materialized='view') }}
select * from {{ source('Finansal_analiz_raw', 'mcc_codes') }}