-- Quick test of discount affinity distribution
SELECT 
    discount_affinity_segment,
    discount_affinity_segment_order,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_customers,
    ROUND(MIN(discount_affinity_score), 2) AS min_score,
    ROUND(MAX(discount_affinity_score), 2) AS max_score,
    ROUND(AVG(discount_affinity_score), 2) AS avg_score,
    ROUND(AVG(discount_usage_rate_pct), 2) AS avg_usage_rate_pct,
    ROUND(AVG(discount_dependency_pct), 2) AS avg_dependency_pct,
    ROUND(AVG(orders_with_discount_count), 1) AS avg_discount_orders,
    ROUND(AVG(total_discount_amount), 2) AS avg_discount_amt_aed
FROM {{ ref('dim_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
GROUP BY discount_affinity_segment, discount_affinity_segment_order
ORDER BY discount_affinity_segment_order

