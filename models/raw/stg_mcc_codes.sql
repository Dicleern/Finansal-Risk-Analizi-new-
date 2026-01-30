select 
    mcc_code, -- Senin tablondaki gerçek isim
    description
from {{ source('finans_kaynaklari', 'mcc_codes') }}