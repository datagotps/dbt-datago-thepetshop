{{ config(
    materialized='table',
    description='TRUE Order-level analysis for Hyperlocal service performance with enhanced retention analytics - Sourced from int_order_lines'
) }}


-- =====================================================
-- STEP 1: Aggregate line items to ORDER level from int_order_lines
-- =====================================================
with order_aggregation AS (
    SELECT 
        -- Primary grouping keys (only these two)
        ol.unified_order_id,
        ol.posting_date AS order_date,
        MAX(ol.document_date) AS document_date,

        -- Order identifiers (using MAX as they should be consistent per order)
        --MAX(ol.source_no_) AS source_no_,
        STRING_AGG(DISTINCT ol.source_no_, ', ') AS source_no_,
        MAX(ol.document_no_) AS document_no_,
        MAX (ol.company_source) as company_source,
        MAX(ol.web_order_id) AS web_order_id,
        MAX(ol.unified_refund_id) AS unified_refund_id,
        MAX(ol.unified_customer_id) AS unified_customer_id,
        
        -- Channel information (should be consistent per order)
        MAX(ol.sales_channel) AS sales_channel,
        MAX(ol.offline_order_channel) AS offline_order_channel,
        MAX(ol.online_order_channel) AS online_order_channel,
        MAX(ol.order_type) AS order_type,
        MAX(ol.loyality_member_id) AS loyality_member_id,
        MAX(ol.web_customer_no_) AS web_customer_no_,  -- Shopify customer ID for SuperApp linkage
        
        -- Payment information (should be consistent per order)
        MAX(ol.paymentgateway) AS paymentgateway,
        MAX(ol.paymentmethodcode) AS paymentmethodcode,
        
        -- Customer information (should be consistent per order)
        MAX(ol.customer_name) AS customer_name,
        MAX(ol.std_phone_no_) AS std_phone_no_,
        MAX(ol.raw_phone_no_) AS raw_phone_no_,
        MAX(ol.customer_identity_status) AS customer_identity_status,
        MAX(ol.duplicate_flag) AS duplicate_flag,
        
        
        -- Revenue metrics - using transaction_type from int_order_lines
        SUM(CASE 
            WHEN ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_
            ELSE 0
        END) AS order_value,
        
        SUM(CASE 
            WHEN ol.transaction_type = 'Refund' THEN ol.sales_amount__actual_
            ELSE 0
        END) AS refund_amount,
        
        SUM(ol.sales_amount__actual_) AS total_order_amount,
        
        -- Order characteristics
        MAX(ol.document_type) AS document_type,
        MAX(ol.transaction_type) AS transaction_type,
        
        -- Order metrics
        COUNT(*) AS line_items_count,
        COUNT(DISTINCT document_no_) AS document_no_count,
        COUNT(DISTINCT CASE WHEN ol.sales_amount__actual_ > 0 THEN 1 END) AS positive_line_items
        
    FROM {{ ref('int_order_lines') }} AS ol
    WHERE ol.unified_order_id IS NOT NULL
    GROUP BY 
        ol.unified_order_id, ol.posting_date

       -- ol.unified_customer_id
),

-- =====================================================
-- Customer Acquisition Info for Segmentation
-- Using unified_customer_id for proper customer tracking
-- =====================================================
customer_acquisition AS (
    SELECT 
        unified_customer_id,
        MIN(order_date) AS customer_acquisition_date,
        DATE_TRUNC(MIN(order_date), MONTH) AS acquisition_month_date,
        
        CASE 
            WHEN MIN(order_date) >= '2025-01-16' THEN 'Acquired Post-Launch' 
            WHEN MIN(order_date) < '2025-01-16' THEN 'Acquired Pre-Launch'
            ELSE 'Unknown'
        END AS hyperlocal_customer_segment
    FROM order_aggregation
    GROUP BY unified_customer_id
),

-- =====================================================
-- Customer Hyperlocal Usage for Detailed Segmentation
-- Using unified_customer_id for proper customer tracking
-- =====================================================
customer_hyperlocal_usage AS (
    SELECT 
        unified_customer_id,
        CASE 
            WHEN COUNT(CASE WHEN order_type = 'EXPRESS' AND order_date >= '2025-01-16' THEN 1 END) > 0 
            THEN 'Used Hyperlocal'
            ELSE 'Never Used Hyperlocal'
        END AS hyperlocal_usage_flag
    FROM order_aggregation
    GROUP BY unified_customer_id
),

-- =====================================================
-- Create Detailed Customer Segments
-- Using unified_customer_id for proper customer tracking
-- =====================================================
customer_segments_lookup AS (
    SELECT 
        ca.unified_customer_id,
        ca.customer_acquisition_date,
        ca.acquisition_month_date,
        ca.hyperlocal_customer_segment,
        chu.hyperlocal_usage_flag,
        
        CASE 
            WHEN ca.customer_acquisition_date >= '2025-01-16' AND chu.hyperlocal_usage_flag = 'Used Hyperlocal'
            THEN 'Post-HL Acq + HL User'
            
            WHEN ca.customer_acquisition_date < '2025-01-16' AND chu.hyperlocal_usage_flag = 'Used Hyperlocal'
            THEN 'Pre-HL Acq + HL User'
            
            WHEN ca.customer_acquisition_date >= '2025-01-16' AND chu.hyperlocal_usage_flag = 'Never Used Hyperlocal'
            THEN 'Post-HL Acq + Non-HL User'
            
            WHEN ca.customer_acquisition_date < '2025-01-16' AND chu.hyperlocal_usage_flag = 'Never Used Hyperlocal'
            THEN 'Pre-HL Acq + Non-HL User'
            
            ELSE 'UNCLASSIFIED'
        END AS hyperlocal_customer_detailed_segment,
        
        CASE 
            WHEN ca.customer_acquisition_date >= '2025-01-16' AND chu.hyperlocal_usage_flag = 'Used Hyperlocal' THEN 1
            WHEN ca.customer_acquisition_date < '2025-01-16' AND chu.hyperlocal_usage_flag = 'Used Hyperlocal' THEN 2
            WHEN ca.customer_acquisition_date >= '2025-01-16' AND chu.hyperlocal_usage_flag = 'Never Used Hyperlocal' THEN 3
            WHEN ca.customer_acquisition_date < '2025-01-16' AND chu.hyperlocal_usage_flag = 'Never Used Hyperlocal' THEN 4
            ELSE 5
        END AS hyperlocal_customer_detailed_segment_order
    FROM customer_acquisition ca
    LEFT JOIN customer_hyperlocal_usage chu ON ca.unified_customer_id = chu.unified_customer_id
),

-- =====================================================
-- STEP 2: Enrich Orders with Business Logic
-- =====================================================
order_enriched AS (
    SELECT 
        oa.*,
        csl.customer_acquisition_date,
        csl.acquisition_month_date,
        csl.hyperlocal_customer_segment,
        csl.hyperlocal_usage_flag,
        csl.hyperlocal_customer_detailed_segment,
        csl.hyperlocal_customer_detailed_segment_order,
        
        -- Time-based classifications
        DATE_TRUNC(oa.order_date, MONTH) AS order_month,
        DATE_TRUNC(oa.order_date, WEEK) AS order_week,
        EXTRACT(YEAR FROM oa.order_date) AS order_year,
        EXTRACT(MONTH FROM oa.order_date) AS order_month_num,
        FORMAT_DATE('%Y-%m', oa.order_date) AS year_month,
        
        -- Hyperlocal classifications
        CASE 
            WHEN oa.order_date >= '2025-01-16' THEN 'Post-Launch'
            WHEN oa.order_date < '2025-01-16' THEN 'Pre-Launch'
        END AS hyperlocal_period,
        
        -- Service type classification
        CASE 
            WHEN oa.order_type = 'EXPRESS' AND oa.order_date >= '2025-01-16' AND oa.transaction_type = 'Sale' THEN '60-Min Hyperlocal'
            WHEN oa.order_type = 'EXPRESS' AND oa.order_date < '2025-01-16' AND oa.transaction_type = 'Sale' THEN '4-Hour Express'
            WHEN oa.order_type = 'NORMAL' AND oa.transaction_type = 'Sale' THEN 'Standard Delivery'
            WHEN oa.order_type = 'EXCHANGE' AND oa.transaction_type = 'Sale' THEN 'Exchange Order'
            WHEN oa.transaction_type = 'Refund' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS delivery_service_type,
        
        -- Service tier
        CASE 
            WHEN oa.order_type = 'EXPRESS' AND oa.transaction_type = 'Sale' THEN 'Express Service'
            WHEN oa.transaction_type = 'Sale' THEN 'Standard Service'
            WHEN oa.transaction_type = 'Refund' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS service_tier,
        
        -- Hyperlocal order flag
        CASE 
            WHEN oa.order_type = 'EXPRESS' AND oa.order_date >= '2025-01-16' AND oa.transaction_type = 'Sale' THEN 'Hyperlocal Order'
            WHEN oa.transaction_type = 'Sale' THEN 'Non-Hyperlocal Order'
            WHEN oa.transaction_type = 'Refund' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS hyperlocal_order_flag,
        
        -- Other classifications
        DATE_DIFF(oa.order_date, DATE('2025-01-16'), DAY) AS days_since_hyperlocal_launch,
        
        CASE 
            WHEN oa.sales_channel = 'Online' THEN 'Online'
            WHEN oa.sales_channel = 'Shop' THEN 'Store'
            ELSE 'Unknown'
        END AS order_channel,

        -- Detailed order channel
        COALESCE(oa.offline_order_channel, oa.online_order_channel, 'Unknown') AS order_channel_detail,
        
        -- Payment classification
        CASE 
            WHEN UPPER(oa.paymentgateway) LIKE '%CASH%' OR UPPER(oa.paymentgateway) = 'COD' THEN 'Cash/COD'
            WHEN UPPER(oa.paymentgateway) LIKE '%CARD%' OR UPPER(oa.paymentgateway) LIKE '%CREDIT%' THEN 'Card Payment'
            WHEN UPPER(oa.paymentgateway) LIKE '%TABBY%' THEN 'BNPL (Tabby)'
            WHEN UPPER(oa.paymentgateway) LIKE '%LOYALTY%' OR UPPER(oa.paymentgateway) LIKE '%POINTS%' THEN 'Loyalty/Points'
            ELSE 'Other Payment'
        END AS payment_category,
        
        -- Order size classification
        CASE 
            WHEN oa.order_value >= 500 THEN 'Large Order (500+ AED)'
            WHEN oa.order_value >= 200 THEN 'Medium Order (200-499 AED)'
            WHEN oa.order_value >= 100 THEN 'Small Order (100-199 AED)'
            WHEN oa.order_value < 100 AND oa.order_value > 0 THEN 'Micro Order (<100 AED)'
            WHEN oa.transaction_type = 'Refund' THEN 'Refund Transaction'
            ELSE 'Unknown Size'
        END AS order_size_category,

        CASE 
            WHEN oa.order_value < 50 THEN '0–50'
            WHEN oa.order_value < 100 THEN '50–100'
            WHEN oa.order_value < 200 THEN '100–200'
            WHEN oa.order_value < 500 THEN '200–500'
            WHEN oa.order_value < 1000 THEN '500–1000'
            ELSE '1000+'
        END AS order_value_bucket,
        
        CASE 
            WHEN oa.order_value < 50 THEN 1
            WHEN oa.order_value < 100 THEN 2
            WHEN oa.order_value < 200 THEN 3
            WHEN oa.order_value < 500 THEN 4
            WHEN oa.order_value < 1000 THEN 5
            ELSE 6
        END AS order_value_bucket_sort,
        
        -- Customer tenure
        DATE_DIFF(oa.order_date, csl.customer_acquisition_date, DAY) AS customer_tenure_days_at_order,
        
        -- Customer lifecycle at order
        CASE 
            WHEN oa.transaction_type = 'Sale' AND DATE_DIFF(oa.order_date, csl.customer_acquisition_date, DAY) <= 30 THEN 'New Customer Order'
            WHEN oa.transaction_type = 'Sale' THEN 'Returning Customer Order'
            WHEN oa.transaction_type = 'Refund' THEN 'Customer Refund'
            ELSE 'Other'
        END AS customer_lifecycle_at_order
        
    FROM order_aggregation oa
    LEFT JOIN customer_segments_lookup csl ON oa.unified_customer_id = csl.unified_customer_id
),

-- =====================================================
-- STEP 3: Calculate Order Sequences (already at order level)
-- Using unified_customer_id for proper customer tracking
-- =====================================================
order_sequence AS (
    SELECT 
        *,
        -- Customer order sequence (using unified_customer_id)
        ROW_NUMBER() OVER (
            PARTITION BY unified_customer_id 
            ORDER BY order_date, unified_order_id
        ) AS customer_order_sequence,
        
        -- Channel order sequence (using unified_customer_id)
        ROW_NUMBER() OVER (
            PARTITION BY unified_customer_id, sales_channel 
            ORDER BY order_date, unified_order_id
        ) AS channel_order_sequence,
        
        -- Previous order dates (using unified_customer_id)
        LAG(order_date) OVER (
            PARTITION BY unified_customer_id 
            ORDER BY order_date, unified_order_id
        ) AS previous_order_date,
        
        LAG(order_date) OVER (
            PARTITION BY unified_customer_id, sales_channel 
            ORDER BY order_date, unified_order_id
        ) AS previous_channel_order_date,
        
        -- Lifetime totals (using unified_customer_id)
        COUNT(*) OVER (PARTITION BY unified_customer_id) AS total_lifetime_orders,
        COUNT(*) OVER (PARTITION BY unified_customer_id, sales_channel) AS total_channel_orders
        
    FROM order_enriched
),

-- =====================================================
-- STEP 4: Calculate Retention Metrics
-- =====================================================
retention_metrics AS (
    SELECT 
        *,
        -- Days since previous order
        DATE_DIFF(order_date, previous_order_date, DAY) AS days_since_last_order,
        DATE_DIFF(order_date, previous_channel_order_date, DAY) AS days_since_last_channel_order,
        
        -- Recency cohort
        CASE 
            WHEN customer_order_sequence = 1 THEN 'New Customer'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 30 THEN 'Recent Return (<1M)'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) BETWEEN 31 AND 60 THEN 'Month 1 Return'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) BETWEEN 61 AND 90 THEN 'Month 2 Return'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) BETWEEN 91 AND 120 THEN 'Month 3 Return'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) BETWEEN 121 AND 150 THEN 'Month 4 Return'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) BETWEEN 151 AND 180 THEN 'Month 5 Return'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) BETWEEN 181 AND 210 THEN 'Month 6 Return'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) > 210 THEN 'Dormant Return (>6M)'
            ELSE 'Unknown'
        END AS recency_cohort,
        
        -- Customer engagement status
        CASE 
            WHEN customer_order_sequence = 1 THEN 'New'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 30 THEN 'Active'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 60 THEN 'Recent'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 90 THEN 'At Risk'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 180 THEN 'Reactivated (Churn)'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 365 THEN 'Reactivated (Inactive)'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) > 365 THEN 'Reactivated (Lost)'
            ELSE 'Check Logic'
        END AS customer_engagement_status,

        CASE 
            WHEN customer_order_sequence = 1 THEN 1
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 30 THEN 2
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 60 THEN 3
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 90 THEN 4
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 180 THEN 5
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 365 THEN 6
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) > 365 THEN 7
            ELSE 8
        END AS customer_engagement_status_sort,

        -- Simple new/returning
        CASE 
            WHEN customer_order_sequence = 1 THEN 'New'
            ELSE 'Returning'
        END AS new_vs_returning,

        -- Transaction frequency analysis
        CASE 
            WHEN customer_order_sequence = 1 THEN '1st Purchase'
            WHEN customer_order_sequence = 2 THEN '2nd Purchase' 
            WHEN customer_order_sequence = 3 THEN '3rd Purchase'
            WHEN customer_order_sequence = 4 THEN '4th Purchase'
            WHEN customer_order_sequence = 5 THEN '5th Purchase'
            WHEN customer_order_sequence = 6 THEN '6th Purchase'
            WHEN customer_order_sequence = 7 THEN '7th Purchase'
            WHEN customer_order_sequence > 7 THEN 'Repeat Buyer (8+ Orders)'
            ELSE 'Unknown'
        END AS transaction_frequency_segment,

        -- Transaction frequency sorting
        CASE 
            WHEN customer_order_sequence = 1 THEN 1
            WHEN customer_order_sequence = 2 THEN 2
            WHEN customer_order_sequence = 3 THEN 3
            WHEN customer_order_sequence = 4 THEN 4
            WHEN customer_order_sequence = 5 THEN 5
            WHEN customer_order_sequence = 6 THEN 6
            WHEN customer_order_sequence = 7 THEN 7
            WHEN customer_order_sequence > 7 THEN 8
            ELSE 9
        END AS transaction_frequency_segment_sort,

        -- Test customer detection
        CASE 
            WHEN total_lifetime_orders >= 200 THEN TRUE
            ELSE FALSE
        END AS is_test_customer
        
    FROM order_sequence
),

-- =====================================================
-- STEP 5: Customer Acquisition Enrichment
-- Using unified_customer_id for proper customer tracking
-- =====================================================
customer_acquisition_enriched AS (
    SELECT 
        unified_customer_id,
        -- First order details
        MIN(CASE WHEN customer_order_sequence = 1 THEN sales_channel END) AS acquisition_channel,
        MIN(CASE WHEN customer_order_sequence = 1 THEN offline_order_channel END) AS acquisition_store,
        MIN(CASE WHEN customer_order_sequence = 1 THEN online_order_channel END) AS acquisition_platform,
        MIN(CASE WHEN customer_order_sequence = 1 THEN paymentgateway END) AS acquisition_payment_method,
        MIN(CASE WHEN customer_order_sequence = 1 THEN order_date END) AS acquisition_date,
        
        -- Channel usage patterns
        COUNT(DISTINCT sales_channel) AS channels_used_count,
        COUNT(DISTINCT CASE WHEN sales_channel = 'Shop' THEN offline_order_channel END) AS stores_visited_count,
        
        -- Channel preference
        CASE 
            WHEN COUNT(DISTINCT sales_channel) > 1 THEN 'Hybrid'
            WHEN MAX(sales_channel) = 'Online' THEN 'Online'
            WHEN MAX(sales_channel) = 'Shop' THEN 'Shop'
            ELSE 'Other'
        END AS channel_preference_type
        
    FROM retention_metrics
    GROUP BY unified_customer_id
),

-- =====================================================
-- STEP 6: Final Enhanced Dataset with All Cohort Fields
-- =====================================================
final_enhanced AS (
    SELECT 
        rm.*,
        cae.acquisition_channel,
        cae.acquisition_store,
        cae.acquisition_platform,
        cae.acquisition_payment_method,
        cae.channels_used_count,
        cae.stores_visited_count,
        cae.channel_preference_type,
        
        -- Cohort analysis dimensions
        CONCAT('Q', EXTRACT(QUARTER FROM rm.customer_acquisition_date), ' ', EXTRACT(YEAR FROM rm.customer_acquisition_date)) AS acquisition_quarter,
        FORMAT_DATE('%b %Y', rm.customer_acquisition_date) AS acquisition_month,
        EXTRACT(YEAR FROM rm.customer_acquisition_date) AS acquisition_year,

        -- =============================================
        -- MONTHLY COHORT FIELDS
        -- =============================================
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE FORMAT_DATE('%b %Y', 
                DATE_ADD(
                    DATE_TRUNC(rm.customer_acquisition_date, MONTH), 
                    INTERVAL DATE_DIFF(
                        DATE_TRUNC(rm.order_date, MONTH),
                        DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                        MONTH
                    ) MONTH
                )
            )
        END AS cohort_month_actual_name,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 999999
            ELSE CAST(FORMAT_DATE('%Y%m', 
                DATE_ADD(
                    DATE_TRUNC(rm.customer_acquisition_date, MONTH), 
                    INTERVAL DATE_DIFF(
                        DATE_TRUNC(rm.order_date, MONTH),
                        DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                        MONTH
                    ) MONTH
                )
            ) AS INT64)
        END AS cohort_month_actual_sort,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT('Month ', 
                DATE_DIFF(
                    DATE_TRUNC(rm.order_date, MONTH),
                    DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                    MONTH
                )
            )
        END AS cohort_month_label,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE DATE_DIFF(
                DATE_TRUNC(rm.order_date, MONTH),
                DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                MONTH
            )
        END AS cohort_month_number,

        -- =============================================
        -- QUARTERLY COHORT FIELDS
        -- =============================================
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT(
                'Q',
                EXTRACT(QUARTER FROM DATE_ADD(
                    DATE_TRUNC(rm.customer_acquisition_date, QUARTER),
                    INTERVAL (
                        (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                        (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
                    ) QUARTER
                )),
                ' ',
                EXTRACT(YEAR FROM DATE_ADD(
                    DATE_TRUNC(rm.customer_acquisition_date, QUARTER),
                    INTERVAL (
                        (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                        (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
                    ) QUARTER
                ))
            )
        END AS cohort_quarter_actual_name,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 99999
            ELSE CAST(CONCAT(
                EXTRACT(YEAR FROM DATE_ADD(
                    DATE_TRUNC(rm.customer_acquisition_date, QUARTER),
                    INTERVAL (
                        (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                        (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
                    ) QUARTER
                )),
                EXTRACT(QUARTER FROM DATE_ADD(
                    DATE_TRUNC(rm.customer_acquisition_date, QUARTER),
                    INTERVAL (
                        (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                        (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
                    ) QUARTER
                ))
            ) AS INT64)
        END AS cohort_quarter_actual_sort,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT('Q ', 
                (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
            )
        END AS cohort_quarter_label,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
        END AS cohort_quarter_number,

        -- =============================================
        -- YEARLY COHORT FIELDS
        -- =============================================
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CAST(
                EXTRACT(YEAR FROM rm.customer_acquisition_date) + 
                (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date))
            AS STRING)
        END AS cohort_year_actual_name,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 9999
            ELSE EXTRACT(YEAR FROM rm.customer_acquisition_date) + 
                (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date))
        END AS cohort_year_actual_sort,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT('Year ', EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date))
        END AS cohort_year_label,

        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)
        END AS cohort_year_number,

        -- Additional time-based fields
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE DATE_DIFF(rm.order_date, rm.customer_acquisition_date, WEEK)
        END AS weeks_since_acquisition,
        
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE FORMAT_DATE('Week %V %Y', rm.customer_acquisition_date)
        END AS acquisition_week,
        
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN FALSE
            WHEN DATE_TRUNC(rm.order_date, MONTH) = DATE_TRUNC(rm.customer_acquisition_date, MONTH) THEN TRUE
            ELSE FALSE
        END AS is_acquisition_month,
        
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) = 0 THEN 'Day 0'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 30 THEN 'Month 1'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 60 THEN 'Month 2'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 90 THEN 'Month 3'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 180 THEN 'Month 4-6'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 365 THEN 'Month 7-12'
            ELSE 'Month 13+'
        END AS cohort_age_bucket,

        -- Sort keys for Power BI
        CAST(FORMAT_DATE('%Y%Q', rm.customer_acquisition_date) AS INT64) AS acquisition_quarter_sort,
        CAST(FORMAT_DATE('%Y%m', rm.customer_acquisition_date) AS INT64) AS acquisition_month_sort

    FROM retention_metrics rm
    LEFT JOIN customer_acquisition_enriched cae ON rm.unified_customer_id = cae.unified_customer_id
)

-- =====================================================
-- Final SELECT - TRUE ORDER LEVEL (Same output as original)
-- =====================================================
SELECT 
    -- Order identifiers
    source_no_,
    unified_customer_id,
    unified_order_id,
    document_no_,
    company_source,
    web_order_id,
    loyality_member_id,
    web_customer_no_,  -- Shopify customer ID for SuperApp linkage
    
    -- Order core data (aggregated from line items)
    order_date,
    document_date,
    order_value,
    refund_amount,
    total_order_amount,
    line_items_count,
    document_no_count,
    positive_line_items,
    document_type,
    transaction_type,
    
    -- Order attributes
    sales_channel,
    CASE 
        WHEN sales_channel = 'Online' THEN 'Online'
        ELSE COALESCE(offline_order_channel, 'Unknown')
    END AS store_location,

    CASE 
        WHEN sales_channel = 'Online' THEN 1
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'DIP' THEN 2
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'FZN' THEN 3
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'REM' THEN 4
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'UMSQ' THEN 5
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'WSL' THEN 6
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'CREEK' THEN 7
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'DSO' THEN 8
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'MRI' THEN 9
        WHEN COALESCE(offline_order_channel, 'Unknown') = 'RAK' THEN 10
        ELSE 99
    END AS store_location_sort,

    online_order_channel AS platform,
    order_type,
    paymentgateway,
    paymentmethodcode,
    
    -- Customer data
    customer_name,
    std_phone_no_,
    raw_phone_no_,
    duplicate_flag,
    customer_identity_status,
    
    -- Time dimensions
    order_month,
    order_week,
    order_year,
    order_month_num,
    year_month,
    
    -- Hyperlocal analysis
    hyperlocal_period,
    delivery_service_type,
    service_tier,
    hyperlocal_order_flag,
    days_since_hyperlocal_launch,
    
    -- Customer segmentation
    hyperlocal_customer_segment,
    hyperlocal_usage_flag,
    hyperlocal_customer_detailed_segment,
    hyperlocal_customer_detailed_segment_order,
    
    -- Classifications
    order_channel,
    order_channel_detail,
    payment_category,
    order_size_category,
    customer_tenure_days_at_order,
    customer_lifecycle_at_order,
    
    -- Retention analytics (calculated at ORDER level)
    customer_order_sequence,
    channel_order_sequence,
    previous_order_date,
    previous_channel_order_date,
    total_lifetime_orders,
    total_channel_orders,
    days_since_last_order,
    days_since_last_channel_order,
    recency_cohort,
    customer_engagement_status,
    customer_engagement_status_sort,
    new_vs_returning,
    is_test_customer,

    -- Transaction frequency analysis fields
    transaction_frequency_segment,
    transaction_frequency_segment_sort,
    
    -- Acquisition metrics
    acquisition_channel,
    acquisition_store,
    acquisition_platform,
    acquisition_payment_method,
    channels_used_count,
    stores_visited_count,
    channel_preference_type,

    -- Cohort analysis
    acquisition_quarter,
    acquisition_month,
    acquisition_year,
    acquisition_quarter_sort,
    acquisition_month_sort,
    weeks_since_acquisition,
    acquisition_week,
    is_acquisition_month,
    cohort_age_bucket,
    acquisition_month_date,
    customer_acquisition_date,
    order_value_bucket,
    order_value_bucket_sort,

    -- Cohort Fields Monthly, Quarterly and Yearly
    -- Actual calendar Time Period names
    cohort_year_actual_name,
    cohort_quarter_actual_name,
    cohort_month_actual_name,

    -- Relative position labels
    cohort_year_label,
    cohort_quarter_label,
    cohort_month_label,

    cohort_year_actual_sort,
    cohort_quarter_actual_sort,
    cohort_month_actual_sort,
    
    cohort_year_number,
    cohort_quarter_number,
    cohort_month_number

FROM final_enhanced
--where unified_customer_id = 'CUST-PRE-0419'

ORDER BY order_date DESC, source_no_, unified_order_id

