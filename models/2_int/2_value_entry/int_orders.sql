{{ config(
    materialized='table',
    description='TRUE Order-level analysis for Hyperlocal service performance with enhanced retention analytics - Revenue impact, AOV trends, behavioral changes, and customer journey tracking'
) }}

-- Force dependency on dim_date to ensure it's always refreshed
{% set dummy_ref = ref('dim_date') %}

WITH base_transactions AS (
    SELECT 
        a.source_no_,
        a.document_no_,
        a.posting_date,
        a.sales_amount__actual_,
        a.sales_channel, -- Online or Shop
        a.offline_order_channel, --store location
        a.document_type_2, -- Sales Invoice or Sales Credit Memo

        b.web_order_id,
        b.online_order_channel, --website, Android, iOS, CRM, Unmapped
        b.order_type, --EXPRESS, NORMAL, EXCHANGE
        b.paymentgateway,
        b.paymentmethodcode,

        c.name,
        c.raw_phone_no_,
        c.customer_identity_status
    FROM {{ ref('stg_erp_value_entry') }} AS a
    LEFT JOIN {{ ref('stg_erp_inbound_sales_header') }} AS b 
        ON a.document_no_ = b.documentno
    LEFT JOIN {{ ref('int_erp_customer') }} AS c 
        ON a.source_no_ = c.no_
    WHERE 
        a.source_code NOT IN ('INVTADMT', 'INVTADJMT')
        AND a.item_ledger_entry_type = 'Sale' 
        AND a.sales_channel IN ('Shop','Online')
),

-- STEP 1: Aggregate line items to ORDER level first
order_aggregation AS (
    SELECT 
        -- Order identification (GROUP BY these)
        source_no_,
        CASE 
            WHEN sales_channel = 'Online' THEN web_order_id 
            ELSE document_no_ 
        END AS unified_order_id,
        document_no_,
        web_order_id,
        sales_channel,
        offline_order_channel,
        online_order_channel,
        order_type,
        paymentgateway,
        paymentmethodcode,
        name,
        raw_phone_no_,
        customer_identity_status,
        
        -- Order-level aggregations
        DATE(MIN(posting_date)) AS order_date,  -- Earliest line item date for the order
        
        -- Revenue metrics - aggregate across line items
        SUM(CASE 
            WHEN sales_channel = 'Shop' THEN sales_amount__actual_
            WHEN document_type_2 = 'Sales Invoice' THEN sales_amount__actual_
            ELSE 0
        END) AS order_value,
        
        SUM(CASE 
            WHEN document_type_2 = 'Sales Credit Memo' THEN sales_amount__actual_
            ELSE 0
        END) AS refund_amount,
        
        SUM(sales_amount__actual_) AS total_order_amount,  -- Total across all line items
        
        -- Order characteristics (take first non-null value)
        MAX(document_type_2) AS document_type_2,  -- Assuming consistent per order
        
        -- Order metrics
        COUNT(*) AS line_items_count,
        COUNT(DISTINCT CASE WHEN sales_amount__actual_ > 0 THEN 1 END) AS positive_line_items,
        
        -- Transaction type classification
        CASE 
            WHEN sales_channel = 'Shop' THEN 'Sale'
            WHEN MAX(document_type_2) = 'Sales Invoice' THEN 'Sale'
            WHEN MAX(document_type_2) = 'Sales Credit Memo' THEN 'Refund'
            ELSE 'Other'
        END AS transaction_type
        
    FROM base_transactions
    GROUP BY 
        source_no_,
        CASE WHEN sales_channel = 'Online' THEN web_order_id ELSE document_no_ END,
        document_no_,
        web_order_id,
        sales_channel,
        offline_order_channel,
        online_order_channel,
        order_type,
        paymentgateway,
        paymentmethodcode,
        name,
        raw_phone_no_,
        customer_identity_status
),

-- Get customer acquisition info for segmentation
customer_acquisition AS (
    SELECT 
        source_no_,
        MIN(order_date) AS customer_acquisition_date,
        DATE_TRUNC(MIN(order_date), MONTH) AS acquisition_month_date,
        
        CASE 
            WHEN MIN(order_date) >= '2025-01-16' THEN 'Acquired Post-Launch' 
            WHEN MIN(order_date) < '2025-01-16' THEN 'Acquired Pre-Launch'
            ELSE 'Unknown'
        END AS hyperlocal_customer_segment
    FROM order_aggregation
    GROUP BY source_no_
),

-- Get customer Hyperlocal usage for detailed segmentation
customer_hyperlocal_usage AS (
    SELECT 
        source_no_,
        CASE 
            WHEN COUNT(CASE WHEN order_type = 'EXPRESS' AND order_date >= '2025-01-16' THEN 1 END) > 0 
            THEN 'Used Hyperlocal'
            ELSE 'Never Used Hyperlocal'
        END AS hyperlocal_usage_flag
    FROM order_aggregation
    GROUP BY source_no_
),

-- Create detailed customer segments
customer_segments_lookup AS (
    SELECT 
        ca.source_no_,
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
    LEFT JOIN customer_hyperlocal_usage chu ON ca.source_no_ = chu.source_no_
),

-- STEP 2: Enrich orders with business logic
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
            WHEN oa.sales_channel = 'Shop' AND oa.order_type = 'EXPRESS' AND oa.order_date >= '2025-01-16' THEN '60-Min Hyperlocal'
            WHEN oa.sales_channel = 'Shop' AND oa.order_type = 'EXPRESS' AND oa.order_date < '2025-01-16' THEN '4-Hour Express'
            WHEN oa.sales_channel = 'Shop' AND oa.order_type = 'NORMAL' THEN 'Standard Delivery'
            WHEN oa.sales_channel = 'Shop' AND oa.order_type = 'EXCHANGE' THEN 'Exchange Order'
            WHEN oa.document_type_2 = 'Sales Invoice' AND oa.order_type = 'EXPRESS' AND oa.order_date >= '2025-01-16' THEN '60-Min Hyperlocal'
            WHEN oa.document_type_2 = 'Sales Invoice' AND oa.order_type = 'EXPRESS' AND oa.order_date < '2025-01-16' THEN '4-Hour Express'
            WHEN oa.document_type_2 = 'Sales Invoice' AND oa.order_type = 'NORMAL' THEN 'Standard Delivery'
            WHEN oa.document_type_2 = 'Sales Invoice' AND oa.order_type = 'EXCHANGE' THEN 'Exchange Order'
            WHEN oa.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS delivery_service_type,
        
        -- Service tier
        CASE 
            WHEN oa.sales_channel = 'Shop' AND oa.order_type = 'EXPRESS' THEN 'Express Service'
            WHEN oa.sales_channel = 'Shop' THEN 'Standard Service'
            WHEN oa.document_type_2 = 'Sales Invoice' AND oa.order_type = 'EXPRESS' THEN 'Express Service'
            WHEN oa.document_type_2 = 'Sales Invoice' THEN 'Standard Service'
            WHEN oa.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS service_tier,
        
        -- Hyperlocal order flag
        CASE 
            WHEN oa.sales_channel = 'Shop' AND oa.order_type = 'EXPRESS' AND oa.order_date >= '2025-01-16' THEN 'Hyperlocal Order'
            WHEN oa.sales_channel = 'Shop' THEN 'Non-Hyperlocal Order'
            WHEN oa.document_type_2 = 'Sales Invoice' AND oa.order_type = 'EXPRESS' AND oa.order_date >= '2025-01-16' THEN 'Hyperlocal Order'
            WHEN oa.document_type_2 = 'Sales Invoice' THEN 'Non-Hyperlocal Order'
            WHEN oa.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS hyperlocal_order_flag,
        
        -- Other classifications
        DATE_DIFF(oa.order_date, DATE('2025-01-16'), DAY) AS days_since_hyperlocal_launch,
        
        CASE 
            WHEN oa.sales_channel = 'Online' THEN 'Online'
            WHEN oa.sales_channel = 'Shop' THEN 'Store'
            ELSE 'Unknown'
        END AS order_channel,

        -- NEW: Detailed order channel combining main channel with specific sub-channel
        COALESCE(oa.offline_order_channel, oa.online_order_channel, 'Unknown') AS order_channel_detail,


        
        -- Payment classification
        CASE 
            WHEN UPPER(oa.paymentgateway) LIKE '%CASH%' OR UPPER(oa.paymentgateway) = 'COD' THEN 'Cash/COD'
            WHEN UPPER(oa.paymentgateway) LIKE '%CARD%' OR UPPER(oa.paymentgateway) LIKE '%CREDIT%' THEN 'Card Payment'
            WHEN UPPER(oa.paymentgateway) LIKE '%TABBY%' THEN 'BNPL (Tabby)'
            WHEN UPPER(oa.paymentgateway) LIKE '%LOYALTY%' OR UPPER(oa.paymentgateway) LIKE '%POINTS%' THEN 'Loyalty/Points'
            ELSE 'Other Payment'
        END AS payment_category,
        
        -- Order size classification (using aggregated order_value)
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
        
        -- Order value bucket sort key for proper ordering in visualizations
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
    LEFT JOIN customer_segments_lookup csl ON oa.source_no_ = csl.source_no_
),

-- STEP 3: Calculate order sequences (already at order level)
order_sequence AS (
    SELECT 
        *,
        -- Customer order sequence
        ROW_NUMBER() OVER (
            PARTITION BY source_no_ 
            ORDER BY order_date, unified_order_id
        ) as customer_order_sequence,
        
        -- Channel order sequence
        ROW_NUMBER() OVER (
            PARTITION BY source_no_, sales_channel 
            ORDER BY order_date, unified_order_id
        ) as channel_order_sequence,
        
        -- Previous order dates
        LAG(order_date) OVER (
            PARTITION BY source_no_ 
            ORDER BY order_date, unified_order_id
        ) as previous_order_date,
        
        LAG(order_date) OVER (
            PARTITION BY source_no_, sales_channel 
            ORDER BY order_date, unified_order_id
        ) as previous_channel_order_date,
        
        -- Lifetime totals
        COUNT(*) OVER (PARTITION BY source_no_) as total_lifetime_orders,
        COUNT(*) OVER (PARTITION BY source_no_, sales_channel) as total_channel_orders
        
    FROM order_enriched
),

-- STEP 4: Calculate retention metrics
retention_metrics AS (
    SELECT 
        *,
        -- Days since previous order
        DATE_DIFF(order_date, previous_order_date, DAY) as days_since_last_order,
        DATE_DIFF(order_date, previous_channel_order_date, DAY) as days_since_last_channel_order,
        
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
        END as recency_cohort,
        
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
        END as customer_engagement_status,

        CASE 
            WHEN customer_order_sequence = 1 THEN 1
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 30 THEN 2
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 60 THEN 3
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 90 THEN 4
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 180 THEN 5
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 365 THEN 6
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) > 365 THEN 7
            ELSE 8
        END as customer_engagement_status_sort,

        -- Simple new/returning
        CASE 
            WHEN customer_order_sequence = 1 THEN 'New'
            ELSE 'Returning'
        END as new_vs_returning,

        -- NEW: Transaction frequency analysis at order level
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
        END as transaction_frequency_segment,

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
        END as transaction_frequency_segment_sort,

        -- Test customer detection
        CASE 
            WHEN total_lifetime_orders >= 200 THEN TRUE
            ELSE FALSE
        END as is_test_customer
        
    FROM order_sequence
),

-- STEP 5: Customer acquisition enrichment
customer_acquisition_enriched AS (
    SELECT 
        source_no_,
        -- First order details
        MIN(CASE WHEN customer_order_sequence = 1 THEN sales_channel END) as acquisition_channel,
        MIN(CASE WHEN customer_order_sequence = 1 THEN offline_order_channel END) as acquisition_store,
        MIN(CASE WHEN customer_order_sequence = 1 THEN online_order_channel END) as acquisition_platform,
        MIN(CASE WHEN customer_order_sequence = 1 THEN paymentgateway END) as acquisition_payment_method,
        MIN(CASE WHEN customer_order_sequence = 1 THEN order_date END) as acquisition_date,
        
        -- Channel usage patterns
        COUNT(DISTINCT sales_channel) as channels_used_count,
        COUNT(DISTINCT CASE WHEN sales_channel = 'Shop' THEN offline_order_channel END) as stores_visited_count,
        
        -- Channel preference
        CASE 
            WHEN COUNT(DISTINCT sales_channel) > 1 THEN 'Hybrid'
            WHEN MAX(sales_channel) = 'Online' THEN 'Online'
            WHEN MAX(sales_channel) = 'Shop' THEN 'Shop'
            ELSE 'Other'
        END as channel_preference_type
        
    FROM retention_metrics
    GROUP BY source_no_
),

-- STEP 6: Final enhanced dataset
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
        CONCAT('Q', EXTRACT(QUARTER FROM rm.customer_acquisition_date), ' ', EXTRACT(YEAR FROM rm.customer_acquisition_date)) as acquisition_quarter,
        FORMAT_DATE('%b %Y', rm.customer_acquisition_date) as acquisition_month,
        EXTRACT(YEAR FROM rm.customer_acquisition_date) as acquisition_year,


        -- =============================================
        -- MONTHLY COHORT FIELDS
        -- =============================================

        -- MONTHLY: Actual calendar month names (Jan 2021, Feb 2021, etc.)
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
        END as cohort_month_actual_name,

        -- MONTHLY: Sort key for actual month names (YYYYMM format: 202101, 202102, etc.)
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
        END as cohort_month_actual_sort,

        -- MONTHLY: Relative position labels (Month 0, Month 1, Month 2, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT('Month ', 
                DATE_DIFF(
                    DATE_TRUNC(rm.order_date, MONTH),
                    DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                    MONTH
                )
            )
        END as cohort_month_label,

        -- MONTHLY: Numeric position only (0, 1, 2, 3, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE DATE_DIFF(
                DATE_TRUNC(rm.order_date, MONTH),
                DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                MONTH
            )
        END as cohort_month_number,

        -- =============================================
        -- QUARTERLY COHORT FIELDS
        -- =============================================

        -- QUARTERLY: Actual calendar quarter names (Q1 2021, Q2 2021, etc.)
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
        END as cohort_quarter_actual_name,

        -- QUARTERLY: Sort key for actual quarter names (YYYYQ format: 20211, 20212, etc.)
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
        END as cohort_quarter_actual_sort,

        -- QUARTERLY: Relative position labels (Q 0, Q 1, Q 2, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT('Q ', 
                (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
            )
        END as cohort_quarter_label,

        -- QUARTERLY: Numeric position only (0, 1, 2, 3, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)) * 4 + 
                (EXTRACT(QUARTER FROM rm.order_date) - EXTRACT(QUARTER FROM rm.customer_acquisition_date))
        END as cohort_quarter_number,

        -- =============================================
        -- YEARLY COHORT FIELDS
        -- =============================================

        -- YEARLY: Actual calendar year (2021, 2022, 2023, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CAST(
                EXTRACT(YEAR FROM rm.customer_acquisition_date) + 
                (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date))
            AS STRING)
        END as cohort_year_actual_name,

        -- YEARLY: Sort key for actual year (YYYY format: 2021, 2022, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 9999
            ELSE EXTRACT(YEAR FROM rm.customer_acquisition_date) + 
                (EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date))
        END as cohort_year_actual_sort,

        -- YEARLY: Relative position labels (Year 0, Year 1, Year 2, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE CONCAT('Year ', EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date))
        END as cohort_year_label,

        -- YEARLY: Numeric position only (0, 1, 2, 3, etc.)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE EXTRACT(YEAR FROM rm.order_date) - EXTRACT(YEAR FROM rm.customer_acquisition_date)
        END as cohort_year_number,



        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE DATE_DIFF(rm.order_date, rm.customer_acquisition_date, WEEK)
        END as weeks_since_acquisition,
        
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE FORMAT_DATE('Week %V %Y', rm.customer_acquisition_date)
        END as acquisition_week,
        
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN FALSE
            WHEN DATE_TRUNC(rm.order_date, MONTH) = DATE_TRUNC(rm.customer_acquisition_date, MONTH) THEN TRUE
            ELSE FALSE
        END as is_acquisition_month,
        
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) = 0 THEN 'Day 0'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 30 THEN 'Month 1'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 60 THEN 'Month 2'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 90 THEN 'Month 3'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 180 THEN 'Month 4-6'
            WHEN DATE_DIFF(rm.order_date, rm.customer_acquisition_date, DAY) <= 365 THEN 'Month 7-12'
            ELSE 'Month 13+'
        END as cohort_age_bucket,

        -- Sort keys for Power BI
        CAST(FORMAT_DATE('%Y%Q', rm.customer_acquisition_date) AS INT64) as acquisition_quarter_sort,
        CAST(FORMAT_DATE('%Y%m', rm.customer_acquisition_date) AS INT64) as acquisition_month_sort

    FROM retention_metrics rm
    LEFT JOIN customer_acquisition_enriched cae ON rm.source_no_ = cae.source_no_
)

-- Final SELECT - TRUE ORDER LEVEL
SELECT 
    -- Order identifiers
    source_no_,
    unified_order_id,
    document_no_,
    web_order_id,
    
    -- Order core data (aggregated from line items)
    order_date,
    order_value,                        -- SUM of line item amounts
    refund_amount,                      -- SUM of refund amounts
    total_order_amount,                 -- SUM of all line amounts
    line_items_count,                   -- COUNT of line items in order
    positive_line_items,                -- COUNT of positive amount lines
    document_type_2,
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
    name AS customer_name,
    raw_phone_no_,
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

    -- NEW: Transaction frequency analysis fields
    transaction_frequency_segment,           -- '1st Purchase', '2nd Purchase', etc.
    transaction_frequency_segment_sort,      -- Numeric sort order

    
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


--Cohort Fields Monthly, Quarterly and Yearly

    --Actual calendar Time Period names
    cohort_year_actual_name,  --(2021, 2022, 2023, etc.)
    cohort_quarter_actual_name, --(Q1 2021, Q2 2021, etc.)
    cohort_month_actual_name, --(Jan 2021, Feb 2021, etc.)

    --Relative position labels
    cohort_year_label, --(Year 0, Year 1, Year 2, etc.)
    cohort_quarter_label, --(Q 0, Q 1, Q 2, etc.)
    cohort_month_label, --(Month 0, Month 1, Month 2, etc.)


    cohort_year_actual_sort,
    cohort_quarter_actual_sort,
    cohort_month_actual_sort,
    
    cohort_year_number,
    cohort_quarter_number,
    cohort_month_number,




FROM final_enhanced
ORDER BY order_date DESC, source_no_, unified_order_id