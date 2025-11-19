-- Check percentile boundaries for Low Discount Affinity segment
SELECT 
    CASE 
        WHEN discount_affinity_percentile >= 0.70 THEN 'Top 30% (High)'
        WHEN discount_affinity_percentile >= 0.30 THEN 'Middle 40% (Medium)'
        WHEN discount_affinity_percentile > 0 THEN 'Bottom 30% (Low)'
        ELSE 'No Usage (0)'
    END AS percentile_range,
    COUNT(*) AS customer_count,
    MIN(discount_affinity_score) AS min_score,
    MAX(discount_affinity_score) AS max_score,
    AVG(discount_affinity_score) AS avg_score,
    MIN(discount_affinity_percentile) AS min_percentile,
    MAX(discount_affinity_percentile) AS max_percentile
FROM {{ ref('dim_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
GROUP BY 1
ORDER BY 7 DESC

