{{ config(
    materialized='table',
    description='Order-level analysis for Hyperlocal service performance with enhanced retention analytics - Revenue impact, AOV trends, behavioral changes, and customer journey tracking'
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
        b.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
        b.paymentmethodcode, -- PREPAID, COD, creditCard

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
        --AND a.sales_channel IN ('Shop','Online')
 
        AND (
            a.sales_channel = 'Shop'
            OR 
            -- Valid Online records: must have web_order_id AND proper INV document format
            (
                a.sales_channel = 'Online' AND a.document_no_ LIKE 'INV%'
            )
        )


),

-- Get customer acquisition info for segmentation (NO document_type_2 filter)
customer_acquisition AS (
    SELECT 
        source_no_,
        MIN(posting_date) AS customer_acquisition_date, -- Result: 2025-01-15 (exact date)
        DATE_TRUNC(MIN(posting_date), MONTH) AS acquisition_month_date,  -- Result: 2025-01-01 (first day of month)

        
        -- Customer segment based on acquisition timing
        CASE 
            WHEN MIN(posting_date) >= '2025-01-16' THEN 'Acquired Post-Launch' 
            WHEN MIN(posting_date) < '2025-01-16' THEN 'Acquired Pre-Launch'
            ELSE 'Unknown'
        END AS hyperlocal_customer_segment
    FROM base_transactions
    -- REMOVED: WHERE document_type_2 = 'Sales Invoice' 
    GROUP BY source_no_
),

-- Get customer Hyperlocal usage for detailed segmentation (NO document_type_2 filter)
customer_hyperlocal_usage AS (
    SELECT 
        source_no_,
        -- Check if customer ever used Hyperlocal (60-min Express after launch)
        CASE 
            WHEN COUNT(CASE WHEN order_type = 'EXPRESS' AND posting_date >= '2025-01-16' THEN 1 END) > 0 
            THEN 'Used Hyperlocal'
            ELSE 'Never Used Hyperlocal'
        END AS hyperlocal_usage_flag
    FROM base_transactions
    -- REMOVED: AND document_type_2 = 'Sales Invoice'
    GROUP BY source_no_
),

-- Create detailed customer segments for order-level analysis
customer_segments_lookup AS (
    SELECT 
        ca.source_no_,
        ca.customer_acquisition_date,
        ca.acquisition_month_date,
        ca.hyperlocal_customer_segment,
        chu.hyperlocal_usage_flag,
        
        -- Detailed customer segmentation
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
        
        -- Sort order for detailed segments
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

-- Main order-level analysis with all metrics
order_level_analysis AS (
    SELECT 
        -- Order identification
        bt.source_no_,
        CASE 
            WHEN bt.sales_channel = 'Online' THEN bt.web_order_id 
            ELSE bt.document_no_ 
        END AS unified_order_id,
        bt.document_no_,
        bt.web_order_id,
        
        -- Order details with proper revenue/refund handling
        DATE(bt.posting_date) AS order_date,

        
        -- Revenue metrics (handle NULL document_type_2 for Shop transactions)
        CASE 
            WHEN bt.sales_channel = 'Shop' THEN bt.sales_amount__actual_  -- All Shop amounts as order_value
            WHEN bt.document_type_2 = 'Sales Invoice' THEN bt.sales_amount__actual_
            ELSE 0
        END AS order_value,
        
        CASE 
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN bt.sales_amount__actual_
            ELSE 0
        END AS refund_amount,
        
        -- Original order value for backward compatibility
        bt.sales_amount__actual_,
        
        -- Document type information (keep for dashboard filtering)
        bt.document_type_2,
        CASE 
            WHEN bt.sales_channel = 'Shop' THEN 'Sale'  -- Assume Shop transactions are sales
            WHEN bt.document_type_2 = 'Sales Invoice' THEN 'Sale'
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN 'Refund'
            ELSE 'Other'
        END AS transaction_type,
        
        bt.sales_channel,
        bt.offline_order_channel AS store_location,
        bt.online_order_channel AS platform,
        bt.order_type,
        bt.paymentgateway,
        bt.paymentmethodcode,
        
        -- Customer information
        bt.name AS customer_name,
        bt.raw_phone_no_,
        bt.customer_identity_status,
        
        -- Time-based classifications
        DATE_TRUNC(bt.posting_date, MONTH) AS order_month,
        DATE_TRUNC(bt.posting_date, WEEK) AS order_week,
        EXTRACT(YEAR FROM bt.posting_date) AS order_year,
        EXTRACT(MONTH FROM bt.posting_date) AS order_month_num,
        FORMAT_DATE('%Y-%m', bt.posting_date) AS year_month,
        
        -- Hyperlocal launch period classification
        CASE 
            WHEN bt.posting_date >= '2025-01-16' THEN 'Post-Launch'
            WHEN bt.posting_date < '2025-01-16' THEN 'Pre-Launch'
        END AS hyperlocal_period,
        
        -- Service type classification (handle NULL document_type_2)
        CASE 
            WHEN bt.sales_channel = 'Shop' AND bt.order_type = 'EXPRESS' AND bt.posting_date >= '2025-01-16' THEN '60-Min Hyperlocal'
            WHEN bt.sales_channel = 'Shop' AND bt.order_type = 'EXPRESS' AND bt.posting_date < '2025-01-16' THEN '4-Hour Express'
            WHEN bt.sales_channel = 'Shop' AND bt.order_type = 'NORMAL' THEN 'Standard Delivery'
            WHEN bt.sales_channel = 'Shop' AND bt.order_type = 'EXCHANGE' THEN 'Exchange Order'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.order_type = 'EXPRESS' AND bt.posting_date >= '2025-01-16' THEN '60-Min Hyperlocal'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.order_type = 'EXPRESS' AND bt.posting_date < '2025-01-16' THEN '4-Hour Express'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.order_type = 'NORMAL' THEN 'Standard Delivery'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.order_type = 'EXCHANGE' THEN 'Exchange Order'
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS delivery_service_type,
        
        -- Premium service flag (handle NULL document_type_2)
        CASE 
            WHEN bt.sales_channel = 'Shop' AND bt.order_type = 'EXPRESS' THEN 'Express Service'
            WHEN bt.sales_channel = 'Shop' THEN 'Standard Service'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.order_type = 'EXPRESS' THEN 'Express Service'
            WHEN bt.document_type_2 = 'Sales Invoice' THEN 'Standard Service'
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS service_tier,
        
        -- Hyperlocal service flag (handle NULL document_type_2)
        CASE 
            WHEN bt.sales_channel = 'Shop' AND bt.order_type = 'EXPRESS' AND bt.posting_date >= '2025-01-16' THEN 'Hyperlocal Order'
            WHEN bt.sales_channel = 'Shop' THEN 'Non-Hyperlocal Order'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.order_type = 'EXPRESS' AND bt.posting_date >= '2025-01-16' THEN 'Hyperlocal Order'
            WHEN bt.document_type_2 = 'Sales Invoice' THEN 'Non-Hyperlocal Order'
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Other'
        END AS hyperlocal_order_flag,
        
        -- Days since Hyperlocal launch
        DATE_DIFF(bt.posting_date, DATE('2025-01-16'), DAY) AS days_since_hyperlocal_launch,
        
        -- Customer segmentation (from lookup)
        csl.customer_acquisition_date,
        csl.acquisition_month_date,
        csl.hyperlocal_customer_segment,
        csl.hyperlocal_usage_flag,
        csl.hyperlocal_customer_detailed_segment,
        csl.hyperlocal_customer_detailed_segment_order,
        
        -- Channel classification
        CASE 
            WHEN bt.sales_channel = 'Online' THEN 'Online'
            WHEN bt.sales_channel = 'Shop' THEN 'Store'
            ELSE 'Unknown'
        END AS order_channel,
        
        -- Payment method classification
        CASE 
            WHEN UPPER(bt.paymentgateway) LIKE '%CASH%' OR UPPER(bt.paymentgateway) = 'COD' THEN 'Cash/COD'
            WHEN UPPER(bt.paymentgateway) LIKE '%CARD%' OR UPPER(bt.paymentgateway) LIKE '%CREDIT%' THEN 'Card Payment'
            WHEN UPPER(bt.paymentgateway) LIKE '%TABBY%' THEN 'BNPL (Tabby)'
            WHEN UPPER(bt.paymentgateway) LIKE '%LOYALTY%' OR UPPER(bt.paymentgateway) LIKE '%POINTS%' THEN 'Loyalty/Points'
            ELSE 'Other Payment'
        END AS payment_category,
        
        -- Order size classification (handle NULL document_type_2)
        CASE 
            WHEN bt.sales_channel = 'Shop' AND bt.sales_amount__actual_ >= 500 THEN 'Large Order (500+ AED)'
            WHEN bt.sales_channel = 'Shop' AND bt.sales_amount__actual_ >= 200 THEN 'Medium Order (200-499 AED)'
            WHEN bt.sales_channel = 'Shop' AND bt.sales_amount__actual_ >= 100 THEN 'Small Order (100-199 AED)'
            WHEN bt.sales_channel = 'Shop' AND bt.sales_amount__actual_ < 100 THEN 'Micro Order (<100 AED)'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.sales_amount__actual_ >= 500 THEN 'Large Order (500+ AED)'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.sales_amount__actual_ >= 200 THEN 'Medium Order (200-499 AED)'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.sales_amount__actual_ >= 100 THEN 'Small Order (100-199 AED)'
            WHEN bt.document_type_2 = 'Sales Invoice' AND bt.sales_amount__actual_ < 100 THEN 'Micro Order (<100 AED)'
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN 'Refund Transaction'
            ELSE 'Unknown Size'
        END AS order_size_category,
        
        -- Customer tenure at time of order
        DATE_DIFF(bt.posting_date, csl.customer_acquisition_date, DAY) AS customer_tenure_days_at_order,
        
        -- New customer flag (handle NULL document_type_2)
        CASE 
            WHEN bt.sales_channel = 'Shop' AND DATE_DIFF(bt.posting_date, csl.customer_acquisition_date, DAY) <= 30 THEN 'New Customer Order'
            WHEN bt.sales_channel = 'Shop' THEN 'Returning Customer Order'
            WHEN bt.document_type_2 = 'Sales Invoice' AND DATE_DIFF(bt.posting_date, csl.customer_acquisition_date, DAY) <= 30 THEN 'New Customer Order'
            WHEN bt.document_type_2 = 'Sales Invoice' THEN 'Returning Customer Order'
            WHEN bt.document_type_2 = 'Sales Credit Memo' THEN 'Customer Refund'
            ELSE 'Other'
        END AS customer_lifecycle_at_order
        
    FROM base_transactions bt
    LEFT JOIN customer_segments_lookup csl ON bt.source_no_ = csl.source_no_
),

    order_level_sequence AS (
        SELECT 
            source_no_,
            unified_order_id,
            sales_channel,
            MIN(order_date) as order_date,  -- Get order date (same for all line items)
            -- Calculate sequence at ORDER level
            ROW_NUMBER() OVER (
                PARTITION BY source_no_ 
                ORDER BY MIN(order_date), unified_order_id
            ) as customer_order_sequence,
            
            ROW_NUMBER() OVER (
                PARTITION BY source_no_, sales_channel 
                ORDER BY MIN(order_date), unified_order_id
            ) as channel_order_sequence,
            
            LAG(MIN(order_date)) OVER (
                PARTITION BY source_no_ 
                ORDER BY MIN(order_date), unified_order_id
            ) as previous_order_date,
            
            LAG(MIN(order_date)) OVER (
                PARTITION BY source_no_, sales_channel 
                ORDER BY MIN(order_date), unified_order_id
            ) as previous_channel_order_date
            
        FROM order_level_analysis
       -- WHERE transaction_type = 'Sale'
        GROUP BY source_no_, unified_order_id, sales_channel
    ),

    -- Then join back to get line-level detail with order-level sequence
    order_sequence_metrics AS (
        SELECT 
            ola.*,
            ols.customer_order_sequence,
            ols.channel_order_sequence,
            ols.previous_order_date,
            ols.previous_channel_order_date,
            
            -- Count total ORDERS (not line items)
            COUNT(DISTINCT ols.unified_order_id) OVER (PARTITION BY ola.source_no_) 
                as total_lifetime_orders,
            
            COUNT(DISTINCT ols.unified_order_id) OVER (PARTITION BY ola.source_no_, ola.sales_channel) 
                as total_channel_orders
                
        FROM order_level_analysis ola
        LEFT JOIN order_level_sequence ols 
            ON ola.source_no_ = ols.source_no_ 
            AND ola.unified_order_id = ols.unified_order_id
            AND ola.sales_channel = ols.sales_channel
       -- WHERE ola.transaction_type = 'Sale'
    ),

-- NEW: Calculate retention and recency metrics
retention_metrics AS (
    SELECT 
        *,
        -- Days since previous order
        DATE_DIFF(order_date, previous_order_date, DAY) 
            as days_since_last_order,
        
        DATE_DIFF(order_date, previous_channel_order_date, DAY) 
            as days_since_last_channel_order,
        
        -- Recency cohort classification
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
        
        -- Customer lifecycle status based on recency
        CASE 
            WHEN customer_order_sequence = 1 THEN 'New'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 30 THEN 'Active'
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 90 THEN 'Reactivated' --Re-Engaged --Returning, Customer recently inactive, now returned, Medium (e.g., 31–90 days)
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) > 90 THEN 'Recovered' --Winback , Recovered --Customer was long inactive, now returned, Long (e.g., >90 days)
            ELSE 'Cheak Logic'
        END as customer_engagement_status,

        CASE 
            WHEN customer_order_sequence = 1 THEN 1
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 30 THEN 2
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) <= 90 THEN 3
            WHEN DATE_DIFF(order_date, previous_order_date, DAY) > 90 THEN 4
            ELSE 5
        END as customer_engagement_status_sort,

        -- Simple new/returning flag for backward compatibility
        CASE 
            WHEN customer_order_sequence = 1 THEN 'New'
            ELSE 'Returning'
        END as new_vs_returning,
        
        -- Test customer detection
        CASE 
            WHEN total_lifetime_orders >= 200 THEN TRUE
            ELSE FALSE
        END as is_test_customer
        
    FROM order_sequence_metrics
),

-- NEW: Get customer acquisition details (enriching what you already have)
customer_acquisition_enriched AS (
    SELECT 
        source_no_,
        -- First order details
        MIN(CASE WHEN customer_order_sequence = 1 THEN sales_channel END) 
            as acquisition_channel,  -- Online/Shop
        
        MIN(CASE WHEN customer_order_sequence = 1 THEN store_location END) 
            as acquisition_store,
        
        MIN(CASE WHEN customer_order_sequence = 1 THEN platform END) 
            as acquisition_platform,  -- Website/iOS/Android/CRM
        
        MIN(CASE WHEN customer_order_sequence = 1 THEN paymentgateway END) 
            as acquisition_payment_method,
        
        MIN(CASE WHEN customer_order_sequence = 1 THEN order_date END) 
            as acquisition_date,
        
        -- Channel usage patterns
        COUNT(DISTINCT sales_channel) 
            as channels_used_count,
        
        COUNT(DISTINCT CASE WHEN sales_channel = 'Shop' THEN store_location END) 
            as stores_visited_count,
        
        -- Channel preference classification
        CASE 
            WHEN COUNT(DISTINCT sales_channel) > 1 THEN 'Hybrid'
            WHEN MAX(sales_channel) = 'Online' THEN 'Online'
            WHEN MAX(sales_channel) = 'Shop' THEN 'Shop'
            ELSE 'Other'
        END as channel_preference_type
        
    FROM retention_metrics
    GROUP BY source_no_
),

-- NEW: Customer transaction frequency classification
customer_transaction_frequency AS (
    SELECT 
        source_no_,
        MAX(total_lifetime_orders) as total_orders,
        
        -- Transaction frequency classification (same logic as old model)
        CASE 
            WHEN MAX(total_lifetime_orders) = 1 THEN 'Transaction_1'
            WHEN MAX(total_lifetime_orders) = 2 THEN 'Transaction_2'
            WHEN MAX(total_lifetime_orders) = 3 THEN 'Transaction_3'
            WHEN MAX(total_lifetime_orders) = 4 THEN 'Transaction_4'
            WHEN MAX(total_lifetime_orders) = 5 THEN 'Transaction_5'
            WHEN MAX(total_lifetime_orders) = 6 THEN 'Transaction_6'
            WHEN MAX(total_lifetime_orders) = 7 THEN 'Transaction_7'
            WHEN MAX(total_lifetime_orders) > 7 THEN 'Transaction_More_Than_7'
            ELSE 'Unknown'
        END as transaction_frequency_segment,
        
        -- Numeric sort order for the segments
        CASE 
            WHEN MAX(total_lifetime_orders) = 1 THEN 1
            WHEN MAX(total_lifetime_orders) = 2 THEN 2
            WHEN MAX(total_lifetime_orders) = 3 THEN 3
            WHEN MAX(total_lifetime_orders) = 4 THEN 4
            WHEN MAX(total_lifetime_orders) = 5 THEN 5
            WHEN MAX(total_lifetime_orders) = 6 THEN 6
            WHEN MAX(total_lifetime_orders) = 7 THEN 7
            WHEN MAX(total_lifetime_orders) > 7 THEN 8
            ELSE 9
        END as transaction_frequency_sort_order,
        
        -- Customer loyalty tier (broader classification)
        CASE 
            WHEN MAX(total_lifetime_orders) = 1 THEN 'One-Time Buyer'
            WHEN MAX(total_lifetime_orders) BETWEEN 2 AND 3 THEN 'Early Repeat'
            WHEN MAX(total_lifetime_orders) BETWEEN 4 AND 6 THEN 'Regular Customer'
            WHEN MAX(total_lifetime_orders) >= 7 THEN 'Loyal Customer'
            ELSE 'Unknown'
        END as customer_loyalty_tier
        
    FROM retention_metrics
    GROUP BY source_no_
),


-- NEW: Final enhanced dataset combining all metrics
enhanced_order_data AS (
    SELECT 
        rm.*,
        cae.acquisition_channel,
        cae.acquisition_store,
        cae.acquisition_platform,
        cae.acquisition_payment_method,
        cae.channels_used_count,
        cae.stores_visited_count,
        cae.channel_preference_type,

        -- NEW: Add transaction frequency fields
        ctf.transaction_frequency_segment,
        ctf.transaction_frequency_sort_order,
        ctf.customer_loyalty_tier,

                
        -- Acquisition time dimensions for cohort analysis
        CONCAT('Q', EXTRACT(QUARTER FROM rm.customer_acquisition_date), ' ', EXTRACT(YEAR FROM rm.customer_acquisition_date)) as acquisition_quarter,  -- e.g., 'Q3 2021'
        FORMAT_DATE('%b %Y', rm.customer_acquisition_date) as acquisition_month,     -- e.g., 'Jul 2021'
        EXTRACT(YEAR FROM rm.customer_acquisition_date) as acquisition_year,         -- e.g., 2021

                -- Months since acquisition (for relative month analysis)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE DATE_DIFF(
                DATE_TRUNC(rm.order_date, MONTH),
                DATE_TRUNC(rm.customer_acquisition_date, MONTH),
                MONTH
            )
        END as cohort_month_number,

        
        -- Relative month label
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
        
        -- Weeks since acquisition (for more granular analysis)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN NULL
            ELSE DATE_DIFF(rm.order_date, rm.customer_acquisition_date, WEEK)
        END as weeks_since_acquisition,
        
        -- Acquisition cohort week (for weekly cohorts)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN 'Unknown'
            ELSE FORMAT_DATE('Week %V %Y', rm.customer_acquisition_date)
        END as acquisition_week,
        
        -- Is same month as acquisition (for Month 0 identification)
        CASE 
            WHEN rm.customer_acquisition_date IS NULL THEN FALSE
            WHEN DATE_TRUNC(rm.order_date, MONTH) = DATE_TRUNC(rm.customer_acquisition_date, MONTH) THEN TRUE
            ELSE FALSE
        END as is_acquisition_month,
        
        -- Cohort age bucket (for grouping)
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

                
        -- Sort keys for proper ordering (IMPORTANT for Power BI)
        CAST(FORMAT_DATE('%Y%Q', rm.customer_acquisition_date) AS INT64) as acquisition_quarter_sort,  -- e.g., 20213
        CAST(FORMAT_DATE('%Y%m', rm.customer_acquisition_date) AS INT64) as acquisition_month_sort     -- e.g., 202107
        

    FROM retention_metrics rm
    LEFT JOIN customer_acquisition_enriched cae 
        ON rm.source_no_ = cae.source_no_
    LEFT JOIN customer_transaction_frequency ctf  -- NEW JOIN
        ON rm.source_no_ = ctf.source_no_

)

-- Final SELECT with all original and new retention columns
SELECT 
    -- Order identifiers
    source_no_,
    unified_order_id,
    document_no_,
    web_order_id,
    
    -- Order core data with revenue metrics
    order_date,
    order_value,                        -- Handles Shop + Online Sales Invoice
    refund_amount,                      -- Only Online Credit Memo
    sales_amount__actual_,              -- Original field for backward compatibility
    document_type_2,                    -- KEPT for dashboard filtering
    transaction_type,                   -- Enhanced to handle Shop transactions
    
    sales_channel,
    store_location,
    platform,
    order_type,
    paymentgateway,
    paymentmethodcode,
    
    -- Customer data
    customer_name,
    raw_phone_no_,
    customer_identity_status,
    
    
    -- Time dimensions
    order_month,
    order_week,
    order_year,
    order_month_num,
    year_month,
    
    -- Hyperlocal analysis dimensions
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
    
    -- Additional classifications
    order_channel,
    payment_category,
    order_size_category,
    customer_tenure_days_at_order,
    customer_lifecycle_at_order,
    
    -- NEW: Retention analytics columns
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
    acquisition_channel,
    acquisition_store,
    acquisition_platform,
    acquisition_payment_method,
    channels_used_count,
    stores_visited_count,
    channel_preference_type,

    acquisition_quarter,            -- e.g., 'Q3 2021'
    acquisition_month,              -- e.g., 'Jul 2021'    
    acquisition_year,               -- e.g., 2021
    acquisition_quarter_sort,
    acquisition_month_sort,    

    cohort_month_number,            -- Month-level cohort position: 0 = same month as acquisition, 1 = next month, 2 = two months later, etc.
    cohort_month_label,           -- String: "Month 0", "Month 1"... for display
    weeks_since_acquisition,        -- For weekly cohort analysis
    acquisition_week,               -- For weekly cohorts
    is_acquisition_month,           -- Boolean flag for Month 0
    cohort_age_bucket,              -- For simplified views

    acquisition_month_date,         -- Result: 2025-01-01 (first day of month)  
    customer_acquisition_date,      -- Result: 2025-01-15 (exact date)
    transaction_frequency_segment,      -- 'Transaction_1', 'Transaction_2', etc.
    transaction_frequency_sort_order,   -- 1, 2, 3, 4, 5, 6, 7, 8, 9
    customer_loyalty_tier,              -- 'One-Time Buyer', 'Early Repeat', etc.



FROM enhanced_order_data
ORDER BY order_date DESC, source_no_, unified_order_id