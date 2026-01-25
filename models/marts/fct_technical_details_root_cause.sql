WITH base AS (
    SELECT * FROM {{ ref('int_errors_details_analysis') }}
)

SELECT 
    mcc_description,
    use_chip,
    COUNT(*) AS total_errors,
    SUM(is_technical_glitch) AS technical_glitch_count,
    ROUND(SUM(is_technical_glitch) * 100.0 / COUNT(*), 2) AS glitch_rate_pct
FROM base
GROUP BY 1, 2
HAVING COUNT(*) > 100
ORDER BY total_errors DESC

-- error details tablosundaki technical glitchlere göre toplam error sayısında technical olanların ağırlıkları hesaplandı
-- technical kısmı hangi use chipe göre hangi kategoride daha çok onu görmüş oluyoruz
