-- Sample customers from each discount affinity segment
SELECT 
    discount_affinity_segment,
    unified_customer_id,
    customer_name,
    discount_affinity_score,
    discount_affinity_percentile,
    discount_usage_rate_pct,
    discount_dependency_pct,
    orders_with_discount_count,
    total_order_count,
    total_discount_amount,
    distinct_offers_used
FROM {{ ref('dim_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
    AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity', 'Low Discount Affinity')
ORDER BY 
    discount_affinity_segment_order,
    discount_affinity_score DESC

