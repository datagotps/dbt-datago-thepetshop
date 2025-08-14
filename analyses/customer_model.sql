{{ config(
    materialized='table',
    description='Customer master analysis with RFM segmentation and comprehensive customer intelligence - Enhanced with sales_channel-based logic and Purchase Frequency Analysis'
) }}
WITH customer_golden_record AS (
    SELECT 
        no_,
        std_phone_no_,
        name,
        customer_identity_status,
        loyality_member_id,
        date_created,
        -- Create a master customer ID based on phone number
        FIRST_VALUE(no_) OVER (
            PARTITION BY std_phone_no_ 
            ORDER BY 
                date_created ASC,  -- Prioritize earliest created
                no_ ASC     -- Consistent tie-breaker
        ) AS master_customer_id,
        
        -- Flag duplicate records
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY std_phone_no_) > 1 
            THEN 'Duplicate'
            ELSE 'Unique'
        END AS duplicate_flag,
        
        ROW_NUMBER() OVER (
            PARTITION BY std_phone_no_ 
            ORDER BY date_created ASC, no_ ASC
        ) AS customer_instance
        
    FROM {{ ref('int_erp_customer') }}
    WHERE std_phone_no_ IS NOT NULL 
      AND std_phone_no_ != ''
      AND std_phone_no_ != 'NULL'
),

 base_transactions AS (
    SELECT 
        
        COALESCE(cgr.master_customer_id, a.source_no_) AS source_no_,  -- Use master ID
        a.source_no_ AS original_source_no_,  -- Keep original for audit
        a.document_no_,
        a.posting_date,
        a.sales_amount__actual_,
        a.sales_channel, -- Online or Shop
        a.offline_order_channel, --store location

        b.web_order_id,
        b.online_order_channel, --website, Android, iOS, CRM, Unmapped
        b.order_type, --EXPRESS, NORMAL, EXCHANGE
        b.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
        b.paymentmethodcode, -- PREPAID, COD, creditCard

        c.name,
        c.std_phone_no_,
        c.customer_identity_status,
        c.loyality_member_id,
        c.date_created,

        cgr.duplicate_flag,
        cgr.customer_instance,

        
    FROM {{ ref('stg_erp_value_entry') }} AS a
    LEFT JOIN {{ ref('stg_erp_inbound_sales_header') }} AS b 
        ON a.document_no_ = b.documentno
    LEFT JOIN {{ ref('int_erp_customer') }} AS c 
        ON a.source_no_ = c.no_

    LEFT JOIN customer_golden_record AS cgr 
        ON a.source_no_ = cgr.no_
    WHERE 
        a.source_code NOT IN ('INVTADMT', 'INVTADJMT')
        AND a.item_ledger_entry_type = 'Sale' 
        AND a.sales_channel IN ('Shop','Online')
),

-- Order aggregation helper for chronological document lists
order_chronology AS (
    SELECT 
        source_no_,
        CASE 
            WHEN sales_channel = 'Online' THEN web_order_id 
            ELSE document_no_ 
        END AS unified_order_id,
        MIN(posting_date) AS order_date,
        CASE 
            WHEN sales_channel = 'Online' THEN 'Online'
            WHEN sales_channel = 'Shop' THEN 'Offline'
            ELSE 'Unknown'
        END AS order_channel
    FROM base_transactions
    WHERE CASE 
            WHEN sales_channel = 'Online' THEN web_order_id 
            ELSE document_no_ 
        END IS NOT NULL
    GROUP BY 1, 2, 4
),

-- Chronological order lists
customer_order_lists AS (
    SELECT 
        source_no_,
        STRING_AGG(unified_order_id, ' | ' ORDER BY order_date ASC) AS document_ids_list,
        STRING_AGG(
            CASE WHEN order_channel = 'Online' THEN unified_order_id END, ' | ' 
            ORDER BY CASE WHEN order_channel = 'Online' THEN order_date END ASC
        ) AS online_order_ids,
        STRING_AGG(
            CASE WHEN order_channel = 'Offline' THEN unified_order_id END, ' | ' 
            ORDER BY CASE WHEN order_channel = 'Offline' THEN order_date END ASC
        ) AS offline_order_ids
    FROM order_chronology
    GROUP BY 1
),

-- ENHANCED: Clean acquisition analysis using sales_channel
first_transactions AS (
    SELECT 
        source_no_,
        posting_date,
        sales_channel,
        offline_order_channel,
        online_order_channel,
        paymentgateway,
        order_type,

        ROW_NUMBER() OVER (
            PARTITION BY source_no_ 
            ORDER BY 
                posting_date ASC,
                -- Prioritize rows with non-null channel information
                CASE WHEN sales_channel = 'Online' AND online_order_channel IS NOT NULL THEN 1
                     WHEN sales_channel = 'Shop' AND offline_order_channel IS NOT NULL THEN 1
                     ELSE 2 END,
                document_no_ ASC  -- Final tie-breaker for consistency
        ) AS rn
    FROM base_transactions
)
,

first_transaction_details AS (
    SELECT 
        source_no_,
        posting_date AS first_order_date,
        sales_channel,
        offline_order_channel,
        online_order_channel,
        paymentgateway,
        order_type,

        CASE 
            WHEN sales_channel = 'Online' THEN 'Online'
            WHEN sales_channel = 'Shop' THEN 'Offline'
            ELSE 'Unknown'
        END AS customer_acquisition_channel
    FROM first_transactions 
    WHERE rn = 1
),

customer_acquisition_analysis AS (
    SELECT 
        source_no_,
        first_order_date,
        customer_acquisition_channel,
        
        -- First order analysis (simplified - since we already have the first transaction)
        CASE WHEN customer_acquisition_channel = 'Online' THEN first_order_date END AS first_online_order_date,
        CASE WHEN customer_acquisition_channel = 'Offline' THEN first_order_date END AS first_offline_order_date,
        
        -- BEST PRACTICE: Conditional acquisition channels (clean separation)
        CASE 
            WHEN customer_acquisition_channel = 'Offline' THEN offline_order_channel
            ELSE NULL  -- NULL if acquired online
        END AS first_acquisition_store,
        
        CASE 
            WHEN customer_acquisition_channel = 'Online' THEN online_order_channel
            ELSE NULL  -- NULL if acquired offline
        END AS first_acquisition_platform,

        paymentgateway AS first_acquisition_paymentgateway,
        order_type AS first_acquisition_order_type
        
    FROM first_transaction_details
),

customer_base_metrics AS (
    SELECT 
        bt.source_no_,
        bt.name,
        bt.std_phone_no_,
        bt.customer_identity_status,
        MAX(bt.loyality_member_id) AS loyality_member_id,

        COUNT(DISTINCT DATE_TRUNC(bt.posting_date, MONTH)) AS active_months_count,



        
        -- Date metrics
        DATE(MIN(bt.date_created)) AS customer_acquisition_date,
        DATE(MIN(bt.posting_date)) AS first_order_date,
        DATE(MAX(bt.posting_date)) AS last_order_date,

        
        -- Channel Distribution Lists
        STRING_AGG(DISTINCT bt.offline_order_channel, ' | ') AS stores_used,
        STRING_AGG(DISTINCT bt.online_order_channel, ' | ') AS platforms_used,
        
        -- Order counts using sales_channel
        COUNT(DISTINCT 
            CASE 
                WHEN bt.sales_channel = 'Online' THEN bt.web_order_id 
                ELSE bt.document_no_ 
            END
        ) AS total_order_count,
        
        COUNT(DISTINCT CASE WHEN bt.sales_channel = 'Online' THEN bt.web_order_id END) AS online_order_count,
        COUNT(DISTINCT CASE WHEN bt.sales_channel = 'Shop' THEN bt.document_no_ END) AS offline_order_count,
        
        -- Sales values using sales_channel
        SUM(bt.sales_amount__actual_) AS total_sales_value,
        SUM(CASE WHEN bt.sales_channel = 'Online' THEN bt.sales_amount__actual_ ELSE 0 END) AS online_sales_value,
        SUM(CASE WHEN bt.sales_channel = 'Shop' THEN bt.sales_amount__actual_ ELSE 0 END) AS offline_sales_value,
        
        -- YTD Sales
        SUM(CASE 
            WHEN bt.posting_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
            AND bt.posting_date <= CURRENT_DATE()
            THEN bt.sales_amount__actual_ 
            ELSE 0 
        END) AS ytd_sales,
        
        -- MTD Sales
        SUM(CASE 
            WHEN bt.posting_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            AND bt.posting_date <= CURRENT_DATE()
            THEN bt.sales_amount__actual_ 
            ELSE 0 
        END) AS mtd_sales,
        
        -- HYPERLOCAL METRICS: Pre-Hyperlocal (before 2025-01-16)
        COUNT(DISTINCT CASE 
            WHEN bt.posting_date < '2025-01-16' 
            THEN CASE WHEN bt.sales_channel = 'Online' THEN bt.web_order_id ELSE bt.document_no_ END 
        END) AS pre_hyperlocal_orders,
        
        SUM(CASE 
            WHEN bt.posting_date < '2025-01-16' 
            THEN bt.sales_amount__actual_ 
            ELSE 0 
        END) AS pre_hyperlocal_revenue,
        
        -- HYPERLOCAL METRICS: Post-Hyperlocal (after 2025-01-16)
        COUNT(DISTINCT CASE 
            WHEN bt.posting_date >= '2025-01-16' 
            THEN CASE WHEN bt.sales_channel = 'Online' THEN bt.web_order_id ELSE bt.document_no_ END 
        END) AS post_hyperlocal_orders,
        
        SUM(CASE 
            WHEN bt.posting_date >= '2025-01-16' 
            THEN bt.sales_amount__actual_ 
            ELSE 0 
        END) AS post_hyperlocal_revenue,
        
        -- HYPERLOCAL METRICS: 60-Min Express Orders (after 2025-01-16)
        COUNT(DISTINCT CASE 
            WHEN bt.order_type = 'EXPRESS' AND bt.posting_date >= '2025-01-16'
            THEN CASE WHEN bt.sales_channel = 'Online' THEN bt.web_order_id ELSE bt.document_no_ END 
        END) AS hyperlocal_60min_orders,
        
        SUM(CASE 
            WHEN bt.order_type = 'EXPRESS' AND bt.posting_date >= '2025-01-16'
            THEN bt.sales_amount__actual_ 
            ELSE 0 
        END) AS hyperlocal_60min_revenue,
        
        -- HYPERLOCAL METRICS: 4-Hour Express Orders (before 2025-01-16)
        COUNT(DISTINCT CASE 
            WHEN bt.order_type = 'EXPRESS' AND bt.posting_date < '2025-01-16'
            THEN CASE WHEN bt.sales_channel = 'Online' THEN bt.web_order_id ELSE bt.document_no_ END 
        END) AS express_4hour_orders,
        
        SUM(CASE 
            WHEN bt.order_type = 'EXPRESS' AND bt.posting_date < '2025-01-16'
            THEN bt.sales_amount__actual_ 
            ELSE 0 
        END) AS express_4hour_revenue
        
    FROM base_transactions bt
    GROUP BY 1, 2, 3, 4
),

-- Join order lists with customer metrics AND acquisition data
customer_base_metrics_with_lists AS (
    SELECT 
        cbm.*,
        col.document_ids_list,
        col.online_order_ids,
        col.offline_order_ids,
        caa.first_acquisition_store,
        caa.first_acquisition_platform,
        caa.customer_acquisition_channel,
        caa.first_acquisition_paymentgateway,
        caa.first_acquisition_order_type,
        
        -- Combined Acquisition Channel Detail (simplified)
        COALESCE(caa.first_acquisition_platform, caa.first_acquisition_store, 'Unknown') AS customer_acquisition_channel_detail
    FROM customer_base_metrics cbm
    LEFT JOIN customer_order_lists col ON cbm.source_no_ = col.source_no_
    LEFT JOIN customer_acquisition_analysis caa ON cbm.source_no_ = caa.source_no_
),

customer_calculated_metrics AS (
    SELECT *,

        -- LOYALTY PROGRAM STATUS
        CASE 
            WHEN loyality_member_id IS NOT NULL AND loyality_member_id != '' 
            THEN 'Enrolled'
            ELSE 'Not Enrolled'
        END AS loyalty_enrollment_status,

        -- Derived metrics
        DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) AS recency_days,
        DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) AS customer_tenure_days,
        total_order_count AS frequency_orders,
        total_sales_value AS monetary_total_value,
        ROUND(total_sales_value / NULLIF(total_order_count, 0), 2) AS monetary_avg_order_value,

        -- Average Monthly Demand (total sales / active months with us)
        ROUND(
            total_sales_value / NULLIF(active_months_count, 0), 2
        ) AS avg_monthly_demand,

        -- Months since acquisition (numeric for calculations)
        DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, MONTH) AS months_since_acquisition,
        
        -- HYPERLOCAL SEGMENTATION: Customer segment based on Hyperlocal launch date (2025-01-16)
        CASE 
            WHEN customer_acquisition_date >= '2025-01-16' THEN 'Acquired Post-Launch'
            WHEN customer_acquisition_date < '2025-01-16' THEN 'Acquired Pre-Launch'
            ELSE 'Unknown'
        END AS hyperlocal_customer_segment,

        -- M1 RETENTION SEGMENT: Customers who transacted last month but haven't transacted in current month by 21st
        CASE 
            WHEN 
                -- Condition 1: Customer transacted in last month
                DATE(last_order_date) >= DATE(EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 
                                               EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 1)
                AND DATE(last_order_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
                
                -- Condition 2: Haven't transacted in current month at all
                AND DATE(last_order_date) < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
                
                -- Condition 3: We're already past the 21st of current month
                AND CURRENT_DATE() >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 21)
            THEN 'M1 Retention Target'
            ELSE 'Not M1 Target'
        END AS m1_retention_segment,

        -- Helper fields for analysis
        CASE 
            WHEN DATE(last_order_date) >= DATE(EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 
                                               EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 1)
            AND DATE(last_order_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
            THEN 'Yes'
            ELSE 'No'
        END AS transacted_last_month,

        CASE 
            WHEN DATE(last_order_date) >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            AND DATE(last_order_date) <= CURRENT_DATE()
            THEN 'Yes'
            ELSE 'No'
        END AS transacted_current_month,

        -- HYPERLOCAL USAGE: Whether customer has used 60-min Hyperlocal service
        CASE 
            WHEN hyperlocal_60min_orders > 0 THEN 'Used Hyperlocal'
            ELSE 'Never Used Hyperlocal'
        END AS hyperlocal_usage_flag,
        
        -- DELIVERY SERVICE PREFERENCE: What type of delivery services customer uses
        CASE 
            WHEN hyperlocal_60min_orders > 0 AND express_4hour_orders > 0 THEN 'Both Express Types'
            WHEN hyperlocal_60min_orders > 0 AND express_4hour_orders = 0 THEN '60-Min Only'
            WHEN hyperlocal_60min_orders = 0 AND express_4hour_orders > 0 THEN '4-Hour Only'
            ELSE 'Standard Delivery Only'
        END AS delivery_service_preference,
        
        -- HYPERLOCAL CUSTOMER SEGMENTATION: Based on first order date and HL usage
        CASE 
            -- Post-HL Acq + HL User: First order date on or after Jan 16 AND placed at least one HL order
            WHEN customer_acquisition_date >= '2025-01-16' 
                 AND hyperlocal_60min_orders > 0 
            THEN 'Post-HL Acq + HL User'
            
            -- Pre-HL Acq + HL User: First order date before Jan 16 AND placed at least one HL order
            WHEN customer_acquisition_date < '2025-01-16' 
                 AND hyperlocal_60min_orders > 0 
            THEN 'Pre-HL Acq + HL User'
            
            -- Post-HL Acq + Non-HL User: First order date on or after Jan 16 AND never placed HL order (includes 0-order customers)
            WHEN customer_acquisition_date >= '2025-01-16' 
                 AND hyperlocal_60min_orders = 0 
                 AND total_order_count >= 0
            THEN 'Post-HL Acq + Non-HL User'
            
            -- Pre-HL Acq + Non-HL User: First order date before Jan 16 AND never placed HL order (includes 0-order customers)
            WHEN customer_acquisition_date < '2025-01-16' 
                 AND hyperlocal_60min_orders = 0 
                 AND total_order_count >= 0
            THEN 'Pre-HL Acq + Non-HL User'
            
            ELSE 'UNCLASSIFIED'
        END AS hyperlocal_customer_detailed_segment,
        
        -- Sort order for the detailed hyperlocal segmentation
        CASE 
            WHEN customer_acquisition_date >= '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 1  -- Post-HL Acq + HL User
            WHEN customer_acquisition_date < '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 2   -- Pre-HL Acq + HL User
            WHEN customer_acquisition_date >= '2025-01-16' AND hyperlocal_60min_orders = 0 AND total_order_count >= 0 THEN 3  -- Post-HL Acq + Non-HL User
            WHEN customer_acquisition_date < '2025-01-16' AND hyperlocal_60min_orders = 0 AND total_order_count >= 0 THEN 4   -- Pre-HL Acq + Non-HL User
            ELSE 5  -- UNCLASSIFIED
        END AS hyperlocal_customer_detailed_segment_order,
        
        -- ADDED: Purchase Frequency Analysis Bucket
        CASE 
            WHEN total_order_count <= 1 THEN '1 Order'
            WHEN total_order_count BETWEEN 2 AND 3 THEN '2-3 Orders'
            WHEN total_order_count BETWEEN 4 AND 6 THEN '4-6 Orders'
            WHEN total_order_count BETWEEN 7 AND 10 THEN '7-10 Orders'
            WHEN total_order_count >= 11 THEN '11+ Orders'
            ELSE 'Unknown'
        END AS purchase_frequency_bucket,
        
        -- ADDED: Purchase Frequency Bucket Sort Order
        CASE 
            WHEN total_order_count <= 1 THEN 1
            WHEN total_order_count BETWEEN 2 AND 3 THEN 2
            WHEN total_order_count BETWEEN 4 AND 6 THEN 3
            WHEN total_order_count BETWEEN 7 AND 10 THEN 4
            WHEN total_order_count >= 11 THEN 5
            ELSE 6
        END AS purchase_frequency_bucket_order,

        -- Customer Recency Segmentation: Groups customers based on days since last order
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 30 THEN 'Active'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 31 AND 60 THEN 'Recent'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 61 AND 90 THEN 'At Risk'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 91 AND 180 THEN 'Churn'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 181 AND 365 THEN 'Inactive'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) > 365 THEN 'Lost'
            ELSE 'Unclassified'
        END AS customer_recency_segment,
            
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 30 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 31 AND 60 THEN 2
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 61 AND 90 THEN 3
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 91 AND 180 THEN 4
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 181 AND 365 THEN 5
            ELSE 6
        END AS customer_recency_segment_order,

        -- ADD CUSTOMER TENURE SEGMENTATION HERE:
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 0 AND 29 THEN '1 Month'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 30 AND 89 THEN '3 Months'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 90 AND 179 THEN '6 Months'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 180 AND 364 THEN '1 Year'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 365 AND 729 THEN '2 Years'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 730 AND 1094 THEN '3 Years'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) >= 1095 THEN '4+ Years'
            ELSE 'Unknown'
        END AS customer_tenure_segment,
        
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 0 AND 29 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 30 AND 89 THEN 2
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 90 AND 179 THEN 3
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 180 AND 364 THEN 4
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 365 AND 729 THEN 5
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 730 AND 1094 THEN 6
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) >= 1095 THEN 7
            ELSE 8
        END AS customer_tenure_segment_order,



        -- Customer Type: New vs Repeat
        CASE 
            -- New: Only 1 order AND acquired within last 30 days
            WHEN total_order_count <= 1 
                 AND customer_acquisition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
            THEN 'New'
            
            -- Repeat: More than 1 order in history
            WHEN total_order_count > 1 
            THEN 'Repeat'
            
            -- One Time Customer: Only 1 order BUT acquired more than 30 days ago
            WHEN total_order_count <= 1 
                 AND customer_acquisition_date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
            THEN 'One-Time'
            
            ELSE 'Unknown'
        END AS customer_type,
        
        -- Customer Channel Distribution
        CASE 
            WHEN online_order_count > 0 AND offline_order_count > 0 THEN 'Hybrid'
            WHEN online_order_count >= 0 AND offline_order_count = 0 THEN 'Online'
            WHEN online_order_count = 0 AND offline_order_count > 0 THEN 'Shop'
            ELSE 'Unknown'
        END AS customer_channel_distribution,
        
        -- Dynamic Customer Acquisition Age Segmentation
        CASE 
            -- MTD: Current month 
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1) THEN 'MTD'
            
            -- All previous months in current year (goes back to January)
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
            AND customer_acquisition_date < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN CONCAT(FORMAT_DATE('%b', customer_acquisition_date), ' ', 
                        RIGHT(CAST(EXTRACT(YEAR FROM customer_acquisition_date) AS STRING), 2))
            
            -- Previous full years (2024, 2023, 2022, etc.)
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING), 2))
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 2
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 2 AS STRING), 2))
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 3
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 3 AS STRING), 2))
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 4
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 4 AS STRING), 2))
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 5
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 5 AS STRING), 2))
            
            -- 6+ years old
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) <= EXTRACT(YEAR FROM CURRENT_DATE()) - 6
            THEN 'Year 19 & Before'
            
            ELSE 'Unknown'
        END AS acquisition_cohort,

        CASE 
            -- MTD: Current month (highest priority - sort first)
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN 1000
            
            -- Current year months (recent months get higher numbers)
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
            AND customer_acquisition_date < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN 900 + EXTRACT(MONTH FROM customer_acquisition_date)  -- 901-912 (Jan=901, Feb=902, etc.)
            
            -- Previous years (recent years get higher numbers)
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
            THEN 800  -- Year 24
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 2
            THEN 700  -- Year 23
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 3
            THEN 600  -- Year 22
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 4
            THEN 500  -- Year 21
            
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 5
            THEN 400  -- Year 20
            
            -- 6+ years old
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) <= EXTRACT(YEAR FROM CURRENT_DATE()) - 6
            THEN 100  -- Year 19 & Before
            
            ELSE 1  -- Unknown - sort last
        END AS acquisition_cohort_rank,


                -- Data Quality Indicators
        COUNT(DISTINCT original_source_no_) OVER (
            PARTITION BY source_no_
        ) AS duplicate_customer_ids_count,
        
        STRING_AGG(DISTINCT original_source_no_, ' | ') OVER (
            PARTITION BY source_no_
        ) AS duplicate_customer_ids_list,
        
        -- Flag for reporting
        CASE 
            WHEN COUNT(DISTINCT original_source_no_) OVER (
                PARTITION BY source_no_
            ) > 1 THEN 'Has Duplicates'
            ELSE 'Clean'
        END AS customer_data_quality_flag,

        
    FROM customer_base_metrics_with_lists
),

customer_rfm_scores AS (
    SELECT *,
        --business logic R + percentile F&M
        -- R Score: Recency
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END AS r_score,
        
        -- F Score: Frequency - PERCENTILE-BASED  
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.8 THEN 5  -- Top 20%
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.6 THEN 4  -- 60-80%
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.4 THEN 3  -- 40-60%
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.2 THEN 2  -- 20-40%
            ELSE 1  -- Bottom 20%
        END AS f_score,
        
        -- M Score: Monetary - PERCENTILE-BASED
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.8 THEN 5  -- Top 20%
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.6 THEN 4  -- 60-80%
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.4 THEN 3  -- 40-60%
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.2 THEN 2  -- 20-40%
            ELSE 1  -- Bottom 20%
        END AS m_score,
        
    FROM customer_calculated_metrics
),

-- First, calculate average days between orders for each customer
customer_order_patterns AS (
    SELECT 
        source_no_,
        -- Calculate days between consecutive orders
        AVG(days_between_orders) AS avg_days_between_orders,
        STDDEV(days_between_orders) AS stddev_days_between_orders,
        MAX(days_between_orders) AS max_days_between_orders
    FROM (
        SELECT 
            source_no_,
            posting_date,
            LAG(posting_date) OVER (PARTITION BY source_no_ ORDER BY posting_date) AS prev_order_date,
            DATE_DIFF(
                posting_date, 
                LAG(posting_date) OVER (PARTITION BY source_no_ ORDER BY posting_date), 
                DAY
            ) AS days_between_orders
        FROM (
            SELECT DISTINCT
                source_no_,
                posting_date,
                CASE 
                    WHEN sales_channel = 'Online' THEN web_order_id 
                    ELSE document_no_ 
                END AS order_id
            FROM base_transactions
        )
        WHERE order_id IS NOT NULL
    )
    WHERE days_between_orders IS NOT NULL
    GROUP BY source_no_
),


customer_segments AS (
    SELECT 
        cs.*,
                -- RFM Segment
        CONCAT(r_score, f_score, m_score) AS rfm_segment,
        
        -- Customer Value Tier based on spending percentiles
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.01 THEN 'Top 1%'
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.20 THEN 'Top 20%'
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.60 THEN 'Middle 30-60%'
            ELSE 'Bottom 40%'
        END AS customer_value_segment,

        -- Customer Value Tier based on spending percentiles
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.01 THEN 1
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.20 THEN 2
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.60 THEN 3
            ELSE 4
        END AS customer_value_segment_order,     

        -- Customer Segment Classification (HIERARCHICAL - No Overlaps)
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN f_score >= 4 AND m_score >= 4 THEN 'Cant Lose Them'
            WHEN r_score >= 4 AND (f_score >= 2 OR m_score >= 2) THEN 'Potential Loyalists'
            WHEN customer_tenure_days <= 90 OR r_score >= 4 THEN 'New Customers'
            WHEN f_score >= 2 AND m_score >= 2 OR m_score IN (2,3) THEN 'At Risk'
            WHEN r_score = 1 AND f_score = 1 OR m_score = 1 THEN 'Lost'
            
            -- ASSIGN REMAINING "OTHERS" TO SUITABLE BUCKETS
            WHEN m_score >= 4 THEN 'Cant Lose Them'        -- High spenders regardless of R&F
            WHEN f_score >= 3 THEN 'At Risk'                -- Frequent buyers regardless of M&R  
            WHEN r_score >= 3 THEN 'At Risk'                -- Recent buyers regardless of F&M
            WHEN f_score >= 2 OR m_score >= 2 THEN 'At Risk' -- Any moderate engagement
            
            ELSE 'Lost'  -- Bottom tier: R<=2, F<=1, M<=1
        END AS customer_rfm_segment,

        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 1
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 2
            WHEN f_score >= 4 AND m_score >= 4 THEN 3
            WHEN r_score >= 4 AND (f_score >= 2 OR m_score >= 2) THEN 4
            WHEN customer_tenure_days <= 90 OR r_score >= 4 THEN 5
            WHEN f_score >= 2 AND m_score >= 2 OR m_score IN (2,3) THEN 6
            WHEN r_score = 1 AND f_score = 1 OR m_score = 1 THEN 7
            
            -- ASSIGN REMAINING "OTHERS" TO SUITABLE BUCKETS
            WHEN m_score >= 4 THEN 3        -- High spenders regardless of R&F
            WHEN f_score >= 3 THEN 6                -- Frequent buyers regardless of M&R  
            WHEN r_score >= 3 THEN 6                -- Recent buyers regardless of F&M
            WHEN f_score >= 2 OR m_score >= 2 THEN 6 -- Any moderate engagement
            
            ELSE 7  -- Bottom tier: R<=2, F<=1, M<=1
        END AS customer_rfm_segment_order,


        cop.avg_days_between_orders,
        cop.stddev_days_between_orders,

        -- PURCHASE FREQUENCY CLASSIFICATION (Your 7 Categories Only)
        CASE 
            -- New Customer: Recent customers with limited history
            WHEN cs.customer_tenure_days <= 30 THEN 'New Customer'
            
            -- One-Time Buyer: Only one order and older than 30 days
            WHEN cs.total_order_count = 1 AND cs.customer_tenure_days > 30 THEN 'One-Time Buyer'
            
            -- Weekly Buyer: Average 7 days or less between orders
            WHEN cop.avg_days_between_orders <= 7 AND cs.total_order_count >= 4 THEN 'Weekly Buyer'
            
            -- Monthly Buyer: Average 8-35 days between orders
            WHEN cop.avg_days_between_orders BETWEEN 8 AND 35 AND cs.total_order_count >= 3 THEN 'Monthly Buyer'
            
            -- Quarterly Buyer: Average 36-120 days between orders
            WHEN cop.avg_days_between_orders BETWEEN 36 AND 120 AND cs.total_order_count >= 2 THEN 'Quarterly Buyer'
            
            -- Annual Buyer: Average 121-400 days between orders
            WHEN cop.avg_days_between_orders BETWEEN 121 AND 400 AND cs.total_order_count >= 2 THEN 'Annual Buyer'
            
            -- Inconsistent Buyer: Everything else (high variation or doesn't fit other patterns)
            ELSE 'Inconsistent Buyer'
        END AS purchase_frequency_type,

        -- SORT ORDER for Power BI
        CASE 
            WHEN cs.customer_tenure_days <= 30 THEN 1  -- New Customer
            WHEN cs.total_order_count = 1 AND cs.customer_tenure_days > 30 THEN 2  -- One-Time Buyer
            WHEN cop.avg_days_between_orders <= 7 AND cs.total_order_count >= 4 THEN 3  -- Weekly Buyer
            WHEN cop.avg_days_between_orders BETWEEN 8 AND 35 AND cs.total_order_count >= 3 THEN 4  -- Monthly Buyer
            WHEN cop.avg_days_between_orders BETWEEN 36 AND 120 AND cs.total_order_count >= 2 THEN 5  -- Quarterly Buyer
            WHEN cop.avg_days_between_orders BETWEEN 121 AND 400 AND cs.total_order_count >= 2 THEN 6  -- Annual Buyer
            ELSE 7  -- Inconsistent Buyer
        END AS purchase_frequency_type_order,


         -- CUSTOMER PATTERN TYPE based on variability
        CASE 
            WHEN cop.avg_days_between_orders IS NULL OR cop.stddev_days_between_orders IS NULL THEN 'No Pattern'
            WHEN cop.avg_days_between_orders = 0 THEN 'No Pattern'
            WHEN cop.stddev_days_between_orders = 0 THEN 'Perfect Consistency'
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 0.1 THEN 'Highly Consistent'
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 0.3 THEN 'Consistent'
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 0.5 THEN 'Moderately Consistent'
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 1.0 THEN 'Variable'
            ELSE 'Highly Variable'
        END AS customer_pattern_type,
        
        
        -- SORT ORDER for Power BI
        CASE 
            WHEN cop.avg_days_between_orders IS NULL OR cop.stddev_days_between_orders IS NULL THEN 0
            WHEN cop.avg_days_between_orders = 0 THEN 0
            WHEN cop.stddev_days_between_orders = 0 THEN 1
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 0.1 THEN 2
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 0.3 THEN 3
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 0.5 THEN 4
            WHEN cop.stddev_days_between_orders / cop.avg_days_between_orders <= 1.0 THEN 5
            ELSE 6
        END AS customer_pattern_type_order,
       
        
        
        -- ENHANCED CHURN RISK SCORE (0-100) using Standard Deviation
        CASE 
            WHEN cs.total_order_count <= 1 THEN 
                -- New/One-time customers: based on time since first order
                CASE 
                    WHEN cs.recency_days <= 30 THEN 10  -- Still in honeymoon period
                    WHEN cs.recency_days <= 60 THEN 30
                    WHEN cs.recency_days <= 90 THEN 50
                    WHEN cs.recency_days <= 180 THEN 70
                    ELSE 90
                END
            
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders > 0 THEN
                -- Repeat customers with predictable patterns: use standard deviations
                CASE 
                    -- More than 3 standard deviations overdue (99.7% confidence they should have ordered)
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                        LEAST(
                            90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                  GREATEST(cop.stddev_days_between_orders, 1)) * 3, 
                            100
                        )
                    
                    -- More than 2 standard deviations overdue (95% confidence they should have ordered)
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                        70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                              GREATEST(cop.stddev_days_between_orders, 1)) * 10
                    
                    -- More than 1 standard deviation overdue (68% confidence they should have ordered)
                    WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                        40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                              GREATEST(cop.stddev_days_between_orders, 1)) * 15
                    
                    -- Within expected range but approaching due date
                    WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                        20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                              GREATEST(cop.stddev_days_between_orders, 1)) * 10
                    
                    -- Not yet due (within normal pattern)
                    ELSE GREATEST(
                        10, 
                        10 + (cs.recency_days / cop.avg_days_between_orders) * 10
                    )
                END
                
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND (cop.stddev_days_between_orders IS NULL OR cop.stddev_days_between_orders = 0) THEN
                -- Repeat customers but with no variation (or single repeat order) - Use fixed multipliers as fallback
                CASE 
                    WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                        LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                    
                    WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                        70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                    
                    WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                        50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                    
                    WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                        30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                    
                    ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                END
                
            ELSE 
                -- Fallback: use general recency rules
                CASE 
                    WHEN cs.recency_days <= 30 THEN 20
                    WHEN cs.recency_days <= 60 THEN 40
                    WHEN cs.recency_days <= 90 THEN 60
                    WHEN cs.recency_days <= 180 THEN 80
                    ELSE 95
                END
        END AS churn_risk_score,
        
        -- ENHANCED CHURN RISK LEVEL (categorical) using standard deviation
        CASE 
            WHEN cs.total_order_count <= 1 AND cs.recency_days <= 30 THEN 'New Customer'
            WHEN CASE 
                    WHEN cs.total_order_count <= 1 THEN 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 10
                            WHEN cs.recency_days <= 60 THEN 30
                            WHEN cs.recency_days <= 90 THEN 50
                            WHEN cs.recency_days <= 180 THEN 70
                            ELSE 90
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders > 0 THEN
                        CASE 
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                                LEAST(90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 3, 100)
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                                70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                                40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 15
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            ELSE GREATEST(10, 10 + (cs.recency_days / cop.avg_days_between_orders) * 10)
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL THEN
                        CASE 
                            WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                                LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                            WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                                70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                                50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                            ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                        END
                    ELSE 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 20
                            WHEN cs.recency_days <= 60 THEN 40
                            WHEN cs.recency_days <= 90 THEN 60
                            WHEN cs.recency_days <= 180 THEN 80
                            ELSE 95
                        END
                END >= 80 THEN 'Critical'
            WHEN CASE 
                    WHEN cs.total_order_count <= 1 THEN 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 10
                            WHEN cs.recency_days <= 60 THEN 30
                            WHEN cs.recency_days <= 90 THEN 50
                            WHEN cs.recency_days <= 180 THEN 70
                            ELSE 90
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders > 0 THEN
                        CASE 
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                                LEAST(90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 3, 100)
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                                70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                                40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 15
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            ELSE GREATEST(10, 10 + (cs.recency_days / cop.avg_days_between_orders) * 10)
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL THEN
                        CASE 
                            WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                                LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                            WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                                70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                                50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                            ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                        END
                    ELSE 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 20
                            WHEN cs.recency_days <= 60 THEN 40
                            WHEN cs.recency_days <= 90 THEN 60
                            WHEN cs.recency_days <= 180 THEN 80
                            ELSE 95
                        END
                END >= 60 THEN 'High'
            WHEN CASE 
                    WHEN cs.total_order_count <= 1 THEN 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 10
                            WHEN cs.recency_days <= 60 THEN 30
                            WHEN cs.recency_days <= 90 THEN 50
                            WHEN cs.recency_days <= 180 THEN 70
                            ELSE 90
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders > 0 THEN
                        CASE 
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                                LEAST(90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 3, 100)
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                                70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                                40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 15
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            ELSE GREATEST(10, 10 + (cs.recency_days / cop.avg_days_between_orders) * 10)
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL THEN
                        CASE 
                            WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                                LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                            WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                                70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                                50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                            ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                        END
                    ELSE 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 20
                            WHEN cs.recency_days <= 60 THEN 40
                            WHEN cs.recency_days <= 90 THEN 60
                            WHEN cs.recency_days <= 180 THEN 80
                            ELSE 95
                        END
                END >= 40 THEN 'Medium'
            ELSE 'Low'
        END AS churn_risk_level,
        
        -- CHURN RISK LEVEL SORT ORDER for Power BI
        CASE 
            WHEN cs.total_order_count <= 1 AND cs.recency_days <= 30 THEN 0  -- New Customer
            WHEN CASE 
                    WHEN cs.total_order_count <= 1 THEN 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 10
                            WHEN cs.recency_days <= 60 THEN 30
                            WHEN cs.recency_days <= 90 THEN 50
                            WHEN cs.recency_days <= 180 THEN 70
                            ELSE 90
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders > 0 THEN
                        CASE 
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                                LEAST(90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 3, 100)
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                                70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                                40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 15
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            ELSE GREATEST(10, 10 + (cs.recency_days / cop.avg_days_between_orders) * 10)
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL THEN
                        CASE 
                            WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                                LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                            WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                                70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                                50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                            ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                        END
                    ELSE 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 20
                            WHEN cs.recency_days <= 60 THEN 40
                            WHEN cs.recency_days <= 90 THEN 60
                            WHEN cs.recency_days <= 180 THEN 80
                            ELSE 95
                        END
                END >= 80 THEN 4  -- Critical
            WHEN CASE 
                    WHEN cs.total_order_count <= 1 THEN 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 10
                            WHEN cs.recency_days <= 60 THEN 30
                            WHEN cs.recency_days <= 90 THEN 50
                            WHEN cs.recency_days <= 180 THEN 70
                            ELSE 90
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders > 0 THEN
                        CASE 
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                                LEAST(90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 3, 100)
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                                70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                                40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 15
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            ELSE GREATEST(10, 10 + (cs.recency_days / cop.avg_days_between_orders) * 10)
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL THEN
                        CASE 
                            WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                                LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                            WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                                70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                                50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                            ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                        END
                    ELSE 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 20
                            WHEN cs.recency_days <= 60 THEN 40
                            WHEN cs.recency_days <= 90 THEN 60
                            WHEN cs.recency_days <= 180 THEN 80
                            ELSE 95
                        END
                END >= 60 THEN 3  -- High
            WHEN CASE 
                    WHEN cs.total_order_count <= 1 THEN 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 10
                            WHEN cs.recency_days <= 60 THEN 30
                            WHEN cs.recency_days <= 90 THEN 50
                            WHEN cs.recency_days <= 180 THEN 70
                            ELSE 90
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders IS NOT NULL 
                         AND cop.stddev_days_between_orders > 0 THEN
                        CASE 
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 
                                LEAST(90 + ((cs.recency_days - (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 3, 100)
                            WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 
                                70 + ((cs.recency_days - (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 
                                40 + ((cs.recency_days - (cop.avg_days_between_orders + cop.stddev_days_between_orders)) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 15
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                20 + ((cs.recency_days - cop.avg_days_between_orders) / 
                                      GREATEST(cop.stddev_days_between_orders, 1)) * 10
                            ELSE GREATEST(10, 10 + (cs.recency_days / cop.avg_days_between_orders) * 10)
                        END
                    WHEN cop.avg_days_between_orders IS NOT NULL THEN
                        CASE 
                            WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                                LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                            WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                                70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                                50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / cop.avg_days_between_orders) * 20
                            WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                                30 + ((cs.recency_days - cop.avg_days_between_orders) / cop.avg_days_between_orders) * 20
                            ELSE GREATEST(10, (cs.recency_days / cop.avg_days_between_orders) * 30)
                        END
                    ELSE 
                        CASE 
                            WHEN cs.recency_days <= 30 THEN 20
                            WHEN cs.recency_days <= 60 THEN 40
                            WHEN cs.recency_days <= 90 THEN 60
                            WHEN cs.recency_days <= 180 THEN 80
                            ELSE 95
                        END
                END >= 40 THEN 2  -- Medium
            ELSE 1  -- Low
        END AS churn_risk_level_order,
        
        -- ENHANCED: Days until expected next order
        CASE 
            WHEN cop.avg_days_between_orders IS NOT NULL 
                AND cs.recency_days < cop.avg_days_between_orders THEN
                ROUND(cop.avg_days_between_orders - cs.recency_days, 0)
            ELSE NULL
        END AS days_until_expected_order,
        
        -- ENHANCED: Is customer overdue? (using standard deviation)
        CASE 
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders IS NOT NULL
                 AND cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 'Yes'
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders IS NULL
                 AND cs.recency_days > cop.avg_days_between_orders * 1.2 THEN 'Yes'
            ELSE 'No'
        END AS is_overdue,
        
        -- ENHANCED: Statistical confidence of being overdue
        CASE 
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders > 0 THEN
                CASE 
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN '99.7% Confidence Overdue'
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN '95% Confidence Overdue'
                    WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN '68% Confidence Overdue'
                    WHEN cs.recency_days > cop.avg_days_between_orders THEN 'Slightly Late (Within Normal Variation)'
                    ELSE 'On Schedule'
                END
            ELSE NULL
        END AS overdue_confidence,
        
        
        -- ENHANCED: Churn action required (more nuanced with standard deviation)
        CASE 
            WHEN cs.total_order_count = 1 THEN 'Single Order - Monitor'
            WHEN cs.recency_days > 365 THEN 'Dormant - Reactivation Needed'
            
            -- Use standard deviations for more accurate action triggers
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders > 0 THEN
                CASE 
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) THEN 'Severely Overdue - Immediate Action'
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) THEN 'Overdue - Urgent Action Required'
                    WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) THEN 'At Risk - Engage Soon'
                    WHEN cs.recency_days > cop.avg_days_between_orders THEN 'Monitor - Approaching Due Date'
                    ELSE 'On Track'
                END
                
            -- Fallback for customers without standard deviation data
            WHEN cop.avg_days_between_orders IS NOT NULL THEN
                CASE 
                    WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 'Severely Overdue'
                    WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 'Overdue - Action Required'
                    WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 'At Risk - Engage Soon'
                    ELSE 'On Track'
                END
                
            WHEN cs.monetary_total_value > 10000 AND cs.recency_days > 60 THEN 'High Value - At Risk'
            ELSE 'Monitor'
        END AS churn_action_required
        
    FROM customer_rfm_scores cs
    LEFT JOIN customer_order_patterns cop ON cs.source_no_ = cop.source_no_

)

SELECT * FROM customer_segments
ORDER BY total_sales_value DESC