{{ config(
    materialized='table',
    description='Order-level analysis for Hyperlocal service performance - Revenue impact, AOV trends, and behavioral changes with Credit Memo handling'
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
        AND a.sales_channel IN ('Shop','Online')
),

-- Get customer acquisition info for segmentation (NO document_type_2 filter)
customer_acquisition AS (
    SELECT 
        source_no_,
        MIN(posting_date) AS customer_acquisition_date,
        
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
        DATE(csl.customer_acquisition_date) AS customer_acquisition_date,
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
)

SELECT 
    -- Order identifiers
    source_no_,
    unified_order_id,
    document_no_,
    web_order_id,
    
    -- Order core data with new revenue metrics
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
    customer_acquisition_date,
    
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
    customer_lifecycle_at_order

FROM order_level_analysis
ORDER BY order_date DESC, source_no_, unified_order_id