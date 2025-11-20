-- =====================================================
-- Discount Affinity Feature - Distribution Analysis
-- =====================================================
-- Purpose: Validate the discount affinity scoring implementation
-- Date: 2025-11-19
-- Feature: High/Medium/Low Discount Affinity Segmentation

-- Part 1: Overall Distribution by Segment
SELECT 
    discount_affinity_segment,
    discount_affinity_segment_order,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_customers,
    
    -- Score ranges
    ROUND(MIN(discount_affinity_score), 2) AS min_score,
    ROUND(MAX(discount_affinity_score), 2) AS max_score,
    ROUND(AVG(discount_affinity_score), 2) AS avg_score,
    ROUND(STDDEV(discount_affinity_score), 2) AS stddev_score,
    
    -- Usage metrics
    ROUND(AVG(discount_usage_rate_pct), 2) AS avg_usage_rate_pct,
    ROUND(AVG(discount_dependency_pct), 2) AS avg_dependency_pct,
    ROUND(AVG(distinct_offers_used), 1) AS avg_offers_used,
    
    -- Business metrics
    ROUND(AVG(orders_with_discount_count), 1) AS avg_discount_orders,
    ROUND(AVG(total_discount_amount), 2) AS avg_discount_amt_aed,
    ROUND(AVG(total_sales_value), 2) AS avg_ltv_aed,
    ROUND(AVG(total_order_count), 1) AS avg_total_orders
    
FROM {{ ref('int_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
GROUP BY discount_affinity_segment, discount_affinity_segment_order
ORDER BY discount_affinity_segment_order;

-- Part 2: Percentile Breakdown
SELECT 
    CASE 
        WHEN discount_affinity_percentile >= 0.90 THEN 'Top 10%'
        WHEN discount_affinity_percentile >= 0.70 THEN '70-90%'
        WHEN discount_affinity_percentile >= 0.50 THEN '50-70%'
        WHEN discount_affinity_percentile >= 0.30 THEN '30-50%'
        WHEN discount_affinity_percentile >= 0.10 THEN '10-30%'
        WHEN discount_affinity_percentile > 0 THEN 'Bottom 10%'
        ELSE 'No Usage'
    END AS percentile_bucket,
    
    COUNT(*) AS customer_count,
    ROUND(AVG(discount_affinity_score), 2) AS avg_score,
    ROUND(AVG(discount_usage_rate_pct), 2) AS avg_usage_rate,
    ROUND(AVG(total_discount_amount), 2) AS avg_discount_amt
    
FROM {{ ref('int_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
GROUP BY 1
ORDER BY 
    CASE 
        WHEN discount_affinity_percentile >= 0.90 THEN 1
        WHEN discount_affinity_percentile >= 0.70 THEN 2
        WHEN discount_affinity_percentile >= 0.50 THEN 3
        WHEN discount_affinity_percentile >= 0.30 THEN 4
        WHEN discount_affinity_percentile >= 0.10 THEN 5
        WHEN discount_affinity_percentile > 0 THEN 6
        ELSE 7
    END;

-- Part 3: Cross-segment Analysis (Discount Affinity vs RFM)
SELECT 
    discount_affinity_segment,
    customer_rfm_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_sales_value), 2) AS avg_ltv,
    ROUND(AVG(total_discount_amount), 2) AS avg_discount_amt,
    ROUND(AVG(discount_affinity_score), 2) AS avg_affinity_score
    
FROM {{ ref('int_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
GROUP BY discount_affinity_segment, customer_rfm_segment
ORDER BY 
    CASE 
        WHEN discount_affinity_segment = 'High Discount Affinity' THEN 1
        WHEN discount_affinity_segment = 'Medium Discount Affinity' THEN 2
        WHEN discount_affinity_segment = 'Low Discount Affinity' THEN 3
        ELSE 4
    END,
    customer_count DESC;

-- Part 4: Sample of High Affinity Customers (for validation)
SELECT 
    unified_customer_id,
    customer_name,
    discount_affinity_segment,
    discount_affinity_score,
    discount_usage_rate_pct,
    discount_dependency_pct,
    orders_with_discount_count,
    total_order_count,
    total_discount_amount,
    distinct_offers_used,
    total_sales_value,
    customer_recency_segment
    
FROM {{ ref('int_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')
    AND discount_affinity_segment = 'High Discount Affinity'
ORDER BY discount_affinity_score DESC
LIMIT 20;

