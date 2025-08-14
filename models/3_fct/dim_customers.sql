-- Gold Layer: Customer Model Grouped by Phone Number
-- Maintains backward compatibility with existing dashboards while consolidating duplicate customer IDs

WITH phone_aggregated AS (
  SELECT
    -- Phone as primary identifier
   

        -- Dynamic grouping key: use source_no when phone is invalid, otherwise use phone
    CASE 
      WHEN std_phone_no_ = '000000000000' OR std_phone_no_ IS NULL THEN source_no_
      ELSE CAST(std_phone_no_ AS STRING)
    END AS std_phone_no_,

     

    
    -- Customer Identification (aggregated)
    STRING_AGG(DISTINCT source_no_, ' | ' ORDER BY source_no_) AS source_no_,
    COUNT(DISTINCT source_no_) AS duplicate_customer_count,
    STRING_AGG(DISTINCT source_no_, ' | ' ORDER BY source_no_) AS all_source_nos, -- New field for tracking
    MAX(name) AS name,
    MAX(customer_identity_status) AS customer_identity_status,
    -- Keep original field name for compatibility
    ARRAY_AGG(loyality_member_id ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS loyality_member_id,
    STRING_AGG(DISTINCT loyality_member_id, ' | ' ORDER BY loyality_member_id) AS all_loyalty_ids, -- New field
    
    -- Activity Metrics
    MAX(active_months_count) AS active_months_count,
    MAX(loyalty_enrollment_status) AS loyalty_enrollment_status,
    
    -- Date Metrics
    MIN(customer_acquisition_date) AS customer_acquisition_date,
    MIN(first_order_date) AS first_order_date,
    MAX(last_order_date) AS last_order_date,
    
    -- Channel Information
    STRING_AGG(DISTINCT stores_used, ' | ') AS stores_used,
    STRING_AGG(DISTINCT platforms_used, ' | ') AS platforms_used,
    
    -- Order Counts
    SUM(total_order_count) AS total_order_count,
    SUM(online_order_count) AS online_order_count,
    SUM(offline_order_count) AS offline_order_count,
    
    -- Sales Values
    SUM(total_sales_value) AS total_sales_value,
    SUM(online_sales_value) AS online_sales_value,
    SUM(offline_sales_value) AS offline_sales_value,
    SUM(ytd_sales) AS ytd_sales,
    SUM(mtd_sales) AS mtd_sales,
    
    -- Hyperlocal Metrics
    SUM(pre_hyperlocal_orders) AS pre_hyperlocal_orders,
    SUM(pre_hyperlocal_revenue) AS pre_hyperlocal_revenue,
    SUM(post_hyperlocal_orders) AS post_hyperlocal_orders,
    SUM(post_hyperlocal_revenue) AS post_hyperlocal_revenue,
    SUM(hyperlocal_60min_orders) AS hyperlocal_60min_orders,
    SUM(hyperlocal_60min_revenue) AS hyperlocal_60min_revenue,
    SUM(express_4hour_orders) AS express_4hour_orders,
    SUM(express_4hour_revenue) AS express_4hour_revenue,
    
    -- Order Lists
    STRING_AGG(document_ids_list, ' | ' ORDER BY first_order_date) AS document_ids_list,
    STRING_AGG(online_order_ids, ' | ' ORDER BY first_order_date) AS online_order_ids,
    STRING_AGG(offline_order_ids, ' | ' ORDER BY first_order_date) AS offline_order_ids,
    
    -- Acquisition Details (from earliest record)
    ARRAY_AGG(first_acquisition_store ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS first_acquisition_store,
    ARRAY_AGG(first_acquisition_platform ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS first_acquisition_platform,
    ARRAY_AGG(customer_acquisition_channel ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS customer_acquisition_channel,
    ARRAY_AGG(first_acquisition_paymentgateway ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS first_acquisition_paymentgateway,
    ARRAY_AGG(first_acquisition_order_type ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS first_acquisition_order_type,
    ARRAY_AGG(customer_acquisition_channel_detail ORDER BY customer_acquisition_date LIMIT 1)[OFFSET(0)] AS customer_acquisition_channel_detail,
    
    -- Aggregated fields for pattern analysis
    AVG(avg_days_between_orders) AS avg_days_between_orders_raw,
    AVG(stddev_days_between_orders) AS stddev_days_between_orders_raw,
    
    -- M1 Retention
    MAX(transacted_last_month) AS transacted_last_month,
    MAX(transacted_current_month) AS transacted_current_month,
    
    -- Original duplicate flag (maintain for compatibility)
    MAX(duplicate_flag) AS duplicate_flag,
    
    -- New duplicate tracking
    CASE 
      WHEN COUNT(DISTINCT source_no_) > 1 THEN 'Yes'
      ELSE 'No'
    END AS has_duplicate_customer_ids

  FROM {{ ref('int_customers') }}
  WHERE std_phone_no_ IS NOT NULL
  GROUP BY std_phone_no_
),

-- Calculate percentiles for RFM scoring
rfm_percentiles AS (
  SELECT
    PERCENTILE_CONT(recency_days, 0.2) OVER() AS r_20,
    PERCENTILE_CONT(recency_days, 0.4) OVER() AS r_40,
    PERCENTILE_CONT(recency_days, 0.6) OVER() AS r_60,
    PERCENTILE_CONT(recency_days, 0.8) OVER() AS r_80,
    
    PERCENTILE_CONT(frequency_orders, 0.2) OVER() AS f_20,
    PERCENTILE_CONT(frequency_orders, 0.4) OVER() AS f_40,
    PERCENTILE_CONT(frequency_orders, 0.6) OVER() AS f_60,
    PERCENTILE_CONT(frequency_orders, 0.8) OVER() AS f_80,
    
    PERCENTILE_CONT(monetary_total_value, 0.2) OVER() AS m_20,
    PERCENTILE_CONT(monetary_total_value, 0.4) OVER() AS m_40,
    PERCENTILE_CONT(monetary_total_value, 0.6) OVER() AS m_60,
    PERCENTILE_CONT(monetary_total_value, 0.8) OVER() AS m_80,
    
    PERCENTILE_CONT(monetary_total_value, 0.99) OVER() AS m_99,
    PERCENTILE_CONT(monetary_total_value, 0.80) OVER() AS m_80_value,
    PERCENTILE_CONT(monetary_total_value, 0.60) OVER() AS m_60_value,
    PERCENTILE_CONT(monetary_total_value, 0.40) OVER() AS m_40_value
  FROM (
    SELECT
      DATE_DIFF(CURRENT_DATE(), DATE(last_order_date), DAY) AS recency_days,
      total_order_count AS frequency_orders,
      total_sales_value AS monetary_total_value
    FROM phone_aggregated
  )
  LIMIT 1
),

-- Main query with all calculated fields
final_output AS (
  SELECT
    pa.*,
    
    -- Recalculated Metrics
    DATE_DIFF(CURRENT_DATE(), DATE(pa.last_order_date), DAY) AS recency_days,
    DATE_DIFF(CURRENT_DATE(), DATE(pa.customer_acquisition_date), DAY) AS customer_tenure_days,
    pa.total_order_count AS frequency_orders,
    pa.total_sales_value AS monetary_total_value,
    SAFE_DIVIDE(pa.total_sales_value, pa.total_order_count) AS monetary_avg_order_value,
    SAFE_DIVIDE(pa.total_sales_value, pa.active_months_count) AS avg_monthly_demand,
    DATE_DIFF(CURRENT_DATE(), DATE(pa.customer_acquisition_date), MONTH) AS months_since_acquisition,
    
    -- Order Pattern Analysis
    COALESCE(pa.avg_days_between_orders_raw, 
      SAFE_DIVIDE(
        DATE_DIFF(DATE(pa.last_order_date), DATE(pa.first_order_date), DAY),
        GREATEST(pa.total_order_count - 1, 1)
      )
    ) AS avg_days_between_orders,
    
    COALESCE(pa.stddev_days_between_orders_raw, 0) AS stddev_days_between_orders,
    
    -- RFM Scores
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(pa.last_order_date), DAY) <= rp.r_20 THEN 5
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(pa.last_order_date), DAY) <= rp.r_40 THEN 4
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(pa.last_order_date), DAY) <= rp.r_60 THEN 3
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(pa.last_order_date), DAY) <= rp.r_80 THEN 2
      ELSE 1
    END AS r_score,
    
    CASE
      WHEN pa.total_order_count >= rp.f_80 THEN 5
      WHEN pa.total_order_count >= rp.f_60 THEN 4
      WHEN pa.total_order_count >= rp.f_40 THEN 3
      WHEN pa.total_order_count >= rp.f_20 THEN 2
      ELSE 1
    END AS f_score,
    
    CASE
      WHEN pa.total_sales_value >= rp.m_80 THEN 5
      WHEN pa.total_sales_value >= rp.m_60 THEN 4
      WHEN pa.total_sales_value >= rp.m_40 THEN 3
      WHEN pa.total_sales_value >= rp.m_20 THEN 2
      ELSE 1
    END AS m_score,
    
    -- Customer Value Segment
    CASE
      WHEN pa.total_sales_value >= rp.m_99 THEN 'Top 1%'
      WHEN pa.total_sales_value >= rp.m_80_value THEN 'Top 20%'
      WHEN pa.total_sales_value >= rp.m_60_value THEN 'Middle 30-60%'
      ELSE 'Bottom 40%'
    END AS customer_value_segment,
    
    CASE
      WHEN pa.total_sales_value >= rp.m_99 THEN 1
      WHEN pa.total_sales_value >= rp.m_80_value THEN 2
      WHEN pa.total_sales_value >= rp.m_60_value THEN 3
      ELSE 4
    END AS customer_value_segment_order
    
  FROM phone_aggregated pa
  CROSS JOIN rfm_percentiles rp
)

SELECT
  -- Customer Identification
  source_no_,
  name,
  std_phone_no_,
  customer_identity_status,
  loyality_member_id, -- Original field maintained
  
  -- Activity Metrics
  active_months_count,
  loyalty_enrollment_status,
  
  -- Date Metrics
  customer_acquisition_date,
  first_order_date,
  last_order_date,
  
  -- Channel Information
  stores_used,
  platforms_used,
  
  -- Order Counts
  total_order_count,
  online_order_count,
  offline_order_count,
  
  -- Sales Values
  total_sales_value,
  online_sales_value,
  offline_sales_value,
  ytd_sales,
  mtd_sales,
  
  -- Hyperlocal Metrics
  pre_hyperlocal_orders,
  pre_hyperlocal_revenue,
  post_hyperlocal_orders,
  post_hyperlocal_revenue,
  hyperlocal_60min_orders,
  hyperlocal_60min_revenue,
  express_4hour_orders,
  express_4hour_revenue,
  
  -- Order Lists
  document_ids_list,
  online_order_ids,
  offline_order_ids,
  
  -- Acquisition Details
  first_acquisition_store,
  first_acquisition_platform,
  customer_acquisition_channel,
  first_acquisition_paymentgateway,
  first_acquisition_order_type,
  customer_acquisition_channel_detail,
  
  -- Calculated Metrics
  recency_days,
  customer_tenure_days,
  frequency_orders,
  monetary_total_value,
  monetary_avg_order_value,
  avg_monthly_demand,
  months_since_acquisition,
  
  -- Hyperlocal Segmentation
  CASE
    WHEN DATE(customer_acquisition_date) >= '2025-01-16' THEN 'Acquired Post-Launch'
    ELSE 'Acquired Pre-Launch'
  END AS hyperlocal_customer_segment,
  
  CASE
    WHEN hyperlocal_60min_orders > 0 THEN 'Used Hyperlocal'
    ELSE 'Never Used Hyperlocal'
  END AS hyperlocal_usage_flag,
  
  CASE
    WHEN hyperlocal_60min_orders > 0 AND express_4hour_orders > 0 THEN 'Both Express Types'
    WHEN hyperlocal_60min_orders > 0 THEN '60-Min Only'
    WHEN express_4hour_orders > 0 THEN '4-Hour Only'
    ELSE 'Standard Delivery Only'
  END AS delivery_service_preference,
  
  CASE
    WHEN DATE(customer_acquisition_date) >= '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 'Post-Launch Hyperlocal User'
    WHEN DATE(customer_acquisition_date) >= '2025-01-16' AND hyperlocal_60min_orders = 0 THEN 'Post-Launch Non-User'
    WHEN DATE(customer_acquisition_date) < '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 'Pre-Launch Adopter'
    WHEN DATE(customer_acquisition_date) < '2025-01-16' AND hyperlocal_60min_orders = 0 THEN 'Pre-Launch Non-Adopter'
    ELSE 'Unknown'
  END AS hyperlocal_customer_detailed_segment,
  
  CASE
    WHEN DATE(customer_acquisition_date) >= '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 1
    WHEN DATE(customer_acquisition_date) >= '2025-01-16' AND hyperlocal_60min_orders = 0 THEN 2
    WHEN DATE(customer_acquisition_date) < '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 3
    WHEN DATE(customer_acquisition_date) < '2025-01-16' AND hyperlocal_60min_orders = 0 THEN 4
    ELSE 5
  END AS hyperlocal_customer_detailed_segment_order,
  
  -- Purchase Behavior
  CASE
    WHEN total_order_count = 1 THEN '1 Order'
    WHEN total_order_count BETWEEN 2 AND 3 THEN '2-3 Orders'
    WHEN total_order_count BETWEEN 4 AND 6 THEN '4-6 Orders'
    WHEN total_order_count BETWEEN 7 AND 10 THEN '7-10 Orders'
    WHEN total_order_count >= 11 THEN '11+ Orders'
  END AS purchase_frequency_bucket,
  
  CASE
    WHEN total_order_count = 1 THEN 1
    WHEN total_order_count BETWEEN 2 AND 3 THEN 2
    WHEN total_order_count BETWEEN 4 AND 6 THEN 3
    WHEN total_order_count BETWEEN 7 AND 10 THEN 4
    WHEN total_order_count >= 11 THEN 5
    ELSE 6
  END AS purchase_frequency_bucket_order,
  
  -- Recency Segmentation
  CASE
    WHEN recency_days <= 30 THEN 'Active'
    WHEN recency_days <= 60 THEN 'Recent'
    WHEN recency_days <= 90 THEN 'At Risk'
    WHEN recency_days <= 180 THEN 'Churn'
    WHEN recency_days <= 365 THEN 'Inactive'
    ELSE 'Lost'
  END AS customer_recency_segment,
  
  CASE
    WHEN recency_days <= 30 THEN 1
    WHEN recency_days <= 60 THEN 2
    WHEN recency_days <= 90 THEN 3
    WHEN recency_days <= 180 THEN 4
    WHEN recency_days <= 365 THEN 5
    ELSE 6
  END AS customer_recency_segment_order,
  
  -- Tenure Segmentation
  CASE
    WHEN months_since_acquisition <= 1 THEN '1 Month'
    WHEN months_since_acquisition <= 3 THEN '3 Months'
    WHEN months_since_acquisition <= 6 THEN '6 Months'
    WHEN months_since_acquisition <= 12 THEN '1 Year'
    WHEN months_since_acquisition <= 24 THEN '2 Years'
    WHEN months_since_acquisition <= 36 THEN '3 Years'
    ELSE '4+ Years'
  END AS customer_tenure_segment,
  
  CASE
    WHEN months_since_acquisition <= 1 THEN 1
    WHEN months_since_acquisition <= 3 THEN 2
    WHEN months_since_acquisition <= 6 THEN 3
    WHEN months_since_acquisition <= 12 THEN 4
    WHEN months_since_acquisition <= 24 THEN 5
    WHEN months_since_acquisition <= 36 THEN 6
    ELSE 7
  END AS customer_tenure_segment_order,
  
  -- Customer Classification
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 'New'
    WHEN total_order_count > 1 THEN 'Repeat'
    ELSE 'One-Time'
  END AS customer_type,
  
  CASE
    WHEN online_order_count > 0 AND offline_order_count > 0 THEN 'Hybrid'
    WHEN online_order_count > 0 THEN 'Online'
    WHEN offline_order_count > 0 THEN 'Shop'
    ELSE 'Unknown'
  END AS customer_channel_distribution,
  
  -- Acquisition Cohort
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_acquisition_date), DAY) <= DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), MONTH), DAY) THEN 'MTD'
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_acquisition_date), MONTH) = 0 THEN FORMAT_DATE('%Y-%m', DATE(customer_acquisition_date))
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_acquisition_date), MONTH) <= 12 THEN FORMAT_DATE('%Y-%m', DATE(customer_acquisition_date))
    ELSE FORMAT_DATE('%Y', DATE(customer_acquisition_date))
  END AS acquisition_cohort,
  
  -- Acquisition Cohort Rank
  DENSE_RANK() OVER (ORDER BY 
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_acquisition_date), DAY) <= DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), MONTH), DAY) THEN DATE(customer_acquisition_date)
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_acquisition_date), MONTH) <= 12 THEN DATE_TRUNC(DATE(customer_acquisition_date), MONTH)
      ELSE DATE_TRUNC(DATE(customer_acquisition_date), YEAR)
    END DESC
  ) AS acquisition_cohort_rank,
  
  -- RFM Analysis
  r_score,
  f_score,
  m_score,
  CAST(r_score * 100 + f_score * 10 + m_score AS INT64) AS rfm_segment,
  customer_value_segment,
  customer_value_segment_order,
  
  -- RFM Segment Names
  CASE
    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
    WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
    WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cant Lose Them'
    WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 2 THEN 'Potential Loyalists'
    WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
    WHEN r_score <= 2 AND f_score >= 2 THEN 'At Risk'
    ELSE 'Lost'
  END AS customer_rfm_segment,
  
  CASE
    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 1
    WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 2
    WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 3
    WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 2 THEN 4
    WHEN r_score >= 4 AND f_score <= 2 THEN 5
    WHEN r_score <= 2 AND f_score >= 2 THEN 6
    ELSE 7
  END AS customer_rfm_segment_order,
  
  -- Order Pattern Analysis
  avg_days_between_orders,
  stddev_days_between_orders,
  
  CASE
    WHEN total_order_count = 1 THEN 'No Pattern'
    WHEN stddev_days_between_orders = 0 THEN 'Perfect Consistency'
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 0.1 THEN 'Highly Consistent'
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 0.25 THEN 'Consistent'
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 0.5 THEN 'Moderately Consistent'
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 1.0 THEN 'Variable'
    ELSE 'Highly Variable'
  END AS customer_pattern_type,
  
  CASE
    WHEN total_order_count = 1 THEN 0
    WHEN stddev_days_between_orders = 0 THEN 1
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 0.1 THEN 2
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 0.25 THEN 3
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 0.5 THEN 4
    WHEN stddev_days_between_orders / NULLIF(avg_days_between_orders, 0) <= 1.0 THEN 5
    ELSE 6
  END AS customer_pattern_type_order,
  
  -- Churn Risk Analysis
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 0
    WHEN recency_days <= 30 THEN LEAST(recency_days * 2, 100)
    WHEN recency_days <= 60 THEN LEAST(40 + recency_days, 100)
    WHEN recency_days <= 90 THEN LEAST(60 + recency_days / 2, 100)
    ELSE LEAST(80 + recency_days / 10, 100)
  END AS churn_risk_score,
  
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 'New Customer'
    WHEN recency_days <= 30 THEN 'Low'
    WHEN recency_days <= 60 THEN 'Medium'
    WHEN recency_days <= 90 THEN 'High'
    ELSE 'Critical'
  END AS churn_risk_level,
  
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 0
    WHEN recency_days <= 30 THEN 1
    WHEN recency_days <= 60 THEN 2
    WHEN recency_days <= 90 THEN 3
    ELSE 4
  END AS churn_risk_level_order,
  
  -- Days Until Expected Order
  CASE
    WHEN total_order_count = 1 THEN NULL
    ELSE GREATEST(0, avg_days_between_orders - recency_days)
  END AS days_until_expected_order,
  
  -- Is Overdue
  CASE
    WHEN total_order_count = 1 THEN 'No'
    WHEN recency_days > avg_days_between_orders THEN 'Yes'
    ELSE 'No'
  END AS is_overdue,
  
  -- Overdue Confidence
  CASE
    WHEN total_order_count = 1 THEN 'N/A'
    WHEN stddev_days_between_orders = 0 AND recency_days > avg_days_between_orders THEN 'High'
    WHEN recency_days > (avg_days_between_orders + 2 * stddev_days_between_orders) THEN 'High'
    WHEN recency_days > (avg_days_between_orders + stddev_days_between_orders) THEN 'Medium'
    WHEN recency_days > avg_days_between_orders THEN 'Low'
    ELSE 'Not Overdue'
  END AS overdue_confidence,
  
  -- Churn Action Required
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 'Welcome Series'
    WHEN recency_days <= 30 THEN 'Maintain Engagement'
    WHEN recency_days <= 60 THEN 'Re-engagement Campaign'
    WHEN recency_days <= 90 THEN 'Win-back Offer'
    WHEN recency_days <= 180 THEN 'Aggressive Win-back'
    ELSE 'Consider Lost'
  END AS churn_action_required,
  
  -- Purchase Frequency Type
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 'New Customer'
    WHEN total_order_count = 1 THEN 'One-Time Buyer'
    WHEN avg_days_between_orders <= 7 THEN 'Weekly Buyer'
    WHEN avg_days_between_orders <= 30 THEN 'Monthly Buyer'
    WHEN avg_days_between_orders <= 90 THEN 'Quarterly Buyer'
    WHEN avg_days_between_orders <= 365 THEN 'Annual Buyer'
    ELSE 'Inconsistent Buyer'
  END AS purchase_frequency_type,
  
  CASE
    WHEN total_order_count = 1 AND recency_days <= 30 THEN 0
    WHEN total_order_count = 1 THEN 1
    WHEN avg_days_between_orders <= 7 THEN 2
    WHEN avg_days_between_orders <= 30 THEN 3
    WHEN avg_days_between_orders <= 90 THEN 4
    WHEN avg_days_between_orders <= 365 THEN 5
    ELSE 6
  END AS purchase_frequency_type_order,
  
  -- M1 Retention Segment
  CASE
    WHEN transacted_last_month = 'Yes' AND transacted_current_month = 'Yes' THEN 'Retained'
    WHEN transacted_last_month = 'Yes' AND transacted_current_month = 'No' THEN 'Churned'
    WHEN transacted_last_month = 'No' AND transacted_current_month = 'Yes' THEN 'Reactivated'
    ELSE 'Inactive'
  END AS m1_retention_segment,
  
  transacted_last_month,
  transacted_current_month,
  
  -- Original duplicate flag (for backward compatibility)
  duplicate_flag,
  
  -- Report Metadata
  DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at,
  
  -- New fields (won't break existing dashboards)
  all_source_nos,
  duplicate_customer_count,
  all_loyalty_ids,
  has_duplicate_customer_ids,
  CURRENT_DATE() AS consolidation_date
  
FROM final_output

--where source_no_ = 'C000000004'
ORDER BY std_phone_no_